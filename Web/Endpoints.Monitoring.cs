// ═══════════════════════════════════════════════════════════════
// Web/Endpoints.Monitoring.cs —— 监控/健康/看板 + 巡查员启动
// ═══════════════════════════════════════════════════════════════
using MessageQueueLab.Core;
using System.Diagnostics;
using System.Net.NetworkInformation;

namespace MessageQueueLab.Web;

public static class MonitoringEndpoints
{
    // 巡查员：每秒扫描全部队列的锁定区；逾期者走统一"失败善后"
    public static void StartSweeper(this WebApplication app, MessageQueueHub hub)
    {
        // 生产铁律：后台线程穿防弹衣——异常只蹲一秒不死
        _ = Task.Run(async () =>
        {
            while (true)
            {
                try
                {
                    await Task.Delay(1000);
                    // ① 锁定区巡查（业务状态变更 —— 写事件流，leader 侧产生、复制给 follower）
                    //    注：SweepAll 内部按 ClusterOptions 判分（写角色才真正回收）
                    var reclaimed = hub.SweepAll();   // SWEEP_V2_MARKER
                    if (reclaimed > 0)
                        hub.PushEvent($"⏰ 巡查员：回收了 {reclaimed} 张逾期未交差的锁定消息（消费者可能崩了）");
                    // ② 压缩巡查（纯磁盘 GC，不改任何业务状态——每个节点都各自扫各自的账本）
                    //    双门槛：已封段≥4 或（50% 脏率 +（行数≥128 / 段龄>1h））
                    hub.CompactAllChecks();
                    // ③ Delta replay 巡查：掉队 follower（ISR 剔除者）从水位处补事件
                    await hub.SweepLaggingFollowersAsync();
                }
                catch (Exception ex)
                {
                    hub.PushEvent($"‼️ 巡查员异常（本回合蹲下，下秒继续）：{ex.Message}");
                }
            }
        });
    }

    public static void MapMonitoringApi(this WebApplication app, MessageQueueHub hub)
    {
        app.MapGet("/health", () => Results.Ok(new
        {
            service = "self-made-mq",
            status = "alive",
            now = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")
        }));

        // 健康看板主页（服务自己吐网页）
        app.MapGet("/", () => Results.Content(DashboardPage.Html, "text/html; charset=utf-8"));

        // 四数字 = 全队列聚合（看板老卡兼容）
        app.MapGet("/api/stats", () =>
        {
            var (r, l, d, dl) = hub.Aggregate();
            return Results.Ok(new
            {
                readyInMemory = r,
                lockedInMemory = l,
                pendingOnDisk = d,
                deadLetters = dl,
                hint = "按队列明细看 /api/queues"
            });
        });

        app.MapGet("/api/queues", () => Results.Ok(hub.StatsPerQueue()));
        app.MapGet("/api/events", () => Results.Ok(hub.RecentEvents().Select(e => new { ts = e.ts, msg = e.msg })));

        // 本机资源快照（看板饼图用）：CPU/内存/磁盘/网卡，一次 250ms 采样同时出进程 CPU 与网卡速率。
        // 跨平台：Linux 读 /proc（容器/物理机），Windows 取不到系统级总量时返回 null（前端显示"仅进程"）。
        app.MapGet("/api/monitoring/resources", async () =>
        {
            var proc = Process.GetCurrentProcess();
            var cpu0 = proc.TotalProcessorTime;
            var sys0 = ReadProcCpu();
            var nic0 = SnapshotNicBytes();
            var sw = Stopwatch.StartNew();
            await Task.Delay(250);
            sw.Stop();
            proc.Refresh();
            var cpu1 = proc.TotalProcessorTime;

            var cores = Environment.ProcessorCount;
            double processPct = (cpu1 - cpu0).TotalMilliseconds / Math.Max(1, sw.Elapsed.TotalMilliseconds * cores) * 100;
            double? systemPct = null;
            var sys1 = ReadProcCpu();
            if (sys0.HasValue && sys1.HasValue && sys1.Value.total > sys0.Value.total)
                systemPct = (1 - (double)(sys1.Value.idle - sys0.Value.idle) / (sys1.Value.total - sys0.Value.total)) * 100;

            long memTotal = (long)GC.GetGCMemoryInfo().TotalAvailableMemoryBytes;   // cgroup 感知
            long memUsed = proc.WorkingSet64;

            long diskTotal = 0, diskFree = 0;
            string diskPath = "";
            try
            {
                diskPath = Path.GetFullPath("data");
                var drive = new DriveInfo(Path.GetPathRoot(diskPath)!);
                diskTotal = drive.TotalSize;
                diskFree = drive.AvailableFreeSpace;
            }
            catch { }

            var nic1 = SnapshotNicBytes();
            double secs = Math.Max(0.05, sw.Elapsed.TotalSeconds);
            var nics = new List<object>();
            long rxSum = 0, txSum = 0;
            long speedSum = 0;
            bool anySpeed = false;
            foreach (var ni in NicList())
            {
                nic0.TryGetValue(ni.Id, out var b0);
                nic1.TryGetValue(ni.Id, out var b1);
                long rx = (long)((b1.rx - b0.rx) / secs);
                long tx = (long)((b1.tx - b0.tx) / secs);
                if (rx < 0) rx = 0;
                if (tx < 0) tx = 0;
                rxSum += rx;
                txSum += tx;
                long? speed = ni.Speed > 0 ? ni.Speed : null;
                if (speed.HasValue) { speedSum += speed.Value; anySpeed = true; }
                nics.Add(new { name = ni.Name, speedBps = speed, rxBytesPerSec = rx, txBytesPerSec = tx });
            }

            return Results.Ok(new
            {
                node = Core.ClusterOptions.NodeName,
                cpu = new { cores, processPercent = Math.Round(Math.Clamp(processPct, 0, 100), 1), systemPercent = systemPct.HasValue ? Math.Round(Math.Clamp(systemPct.Value, 0, 100), 1) : (double?)null },
                memory = new { usedBytes = memUsed, totalBytes = memTotal, percent = memTotal > 0 ? Math.Round(Math.Clamp((double)memUsed / memTotal * 100, 0, 100), 1) : 0 },
                disk = new { path = diskPath, totalBytes = diskTotal, usedBytes = Math.Max(0, diskTotal - diskFree), percent = diskTotal > 0 ? Math.Round((double)(diskTotal - diskFree) / diskTotal * 100, 1) : 0 },
                network = new { rxBytesPerSec = rxSum, txBytesPerSec = txSum, totalSpeedBps = anySpeed ? speedSum : (long?)null, nics }
            });
        });
    }

    private static List<NetworkInterface> NicList()
    {
        try
        {
            return NetworkInterface.GetAllNetworkInterfaces()
                .Where(n => n.OperationalStatus == OperationalStatus.Up && n.NetworkInterfaceType != NetworkInterfaceType.Loopback).ToList();
        }
        catch { return new List<NetworkInterface>(); }
    }

    private static Dictionary<string, (long rx, long tx)> SnapshotNicBytes()
    {
        var d = new Dictionary<string, (long, long)>();
        foreach (var ni in NicList())
        {
            try
            {
                var s = ni.GetIPv4Statistics();
                d[ni.Id] = (s.BytesReceived, s.BytesSent);
            }
            catch { }
        }
        return d;
    }

    private static (long idle, long total)? ReadProcCpu()
    {
        try
        {
            var line = File.ReadLines("/proc/stat").FirstOrDefault(l => l.StartsWith("cpu "));
            if (line is null) return null;
            var parts = line.Split(' ', StringSplitOptions.RemoveEmptyEntries).Skip(1).Select(long.Parse).ToArray();
            if (parts.Length < 4) return null;
            long idle = parts[3] + (parts.Length > 4 ? parts[4] : 0);
            return (idle, parts.Sum());
        }
        catch { return null; }
    }
}
