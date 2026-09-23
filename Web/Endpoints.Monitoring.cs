// ═══════════════════════════════════════════════════════════════
// Web/Endpoints.Monitoring.cs —— 监控/健康/看板 + 巡查员启动
// ═══════════════════════════════════════════════════════════════
using MessageQueueLab.Core;

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
                    var reclaimed = hub.SweepAll();   // SWEEP_V2_MARKER
                    if (reclaimed > 0)
                        hub.PushEvent($"⏰ 巡查员：回收了 {reclaimed} 张逾期未交差的锁定消息（消费者可能崩了）");
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
    }
}
