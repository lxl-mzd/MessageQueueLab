// ═══════════════════════════════════════════════════════════════
// Core/MessageQueueHub.cs —— 枢纽：交换机路由 + 多队列管理 + 集群复制 + 事件流水
//
//   现成的三台分拣机：
//     orders    direct 直连型：routingKey 精确匹配
//     broadcast fanout 扇出型：广播给所有绑定队列（各得一份）
//     alerts    topic 主题型：通配符匹配（多播）
//
//   集群模式（P1/P2 生产者 Quorum 版）：
//     Leader：本地 Push → 后台复制品推给follower（acks / required 随响应回传）
//     Follower：拒绝业务写入（HTTP 503），仅收复制流（经 /api/cluster/replicate）
// ═══════════════════════════════════════════════════════════════
using System.Text.Json;
using MessageQueueLab.Models;
using MessageQueueLab.Persistence;
using MessageQueueLab.Routing;

namespace MessageQueueLab.Core;

public class MessageQueueHub
{
    private readonly Dictionary<string, QueueCore> _queues = new();
    private readonly Dictionary<string, ExchangeDef> _exchanges = new();
    private readonly object _eventLock = new();
    private readonly List<(DateTime Ts, string Msg)> _events = new();
    private readonly ClusterReplicator? _replicator;
    private readonly string _dataDir;

    public MessageQueueHub(string dataDirectory = "data", ClusterReplicator? replicator = null)
    {
        _replicator = replicator;
        _dataDir = dataDirectory;

        // ── 出厂三队列（首次启动的答案；此后磁盘目录才是真相） ──
        _queues["normal"] = new QueueCore("normal", PushEvent, dataDirectory);
        _queues["vip"]    = new QueueCore("vip", PushEvent, dataDirectory);
        _queues["error"]  = new QueueCore("error", PushEvent, dataDirectory);   // 错误专属队列（#.error 落点）

        _exchanges["orders"] = new ExchangeDef("orders", "direct", new()
        {
            new Binding("normal", "normal"),
            new Binding("vip",    "vip"),
        });

        _exchanges["broadcast"] = new ExchangeDef("broadcast", "fanout", new()
        {
            new Binding(null!, "normal"),
            new Binding(null!, "vip"),
        });

        _exchanges["alerts"] = new ExchangeDef("alerts", "topic", new()
        {
            new Binding("pay.#",    "normal"),
            new Binding("#.urgent", "vip"),
            new Binding("#.error",  "error"),      // 一切 "…error" 收尾的消息进 error 队列
        });

        // ── 死信交换机（DLX）：类型 topic，routingKey 约定 "dead.{来源队列}" ──
        //    队列判死时不再直接写自家 DLQ，而是经此交换机路由（可重绑/可审计）
        //    默认绑定回各自队列（dead.normal→normal 的 dlq 账本）...
        _exchanges["dead"] = new ExchangeDef("dead", "topic", new()
        {
            new Binding("dead.normal", "normal"),
            new Binding("dead.vip",    "vip"),
            new Binding("dead.error",  "error"),
        });

        foreach (var q in _queues.Values)
        {
            q.Stats();
            q.SetDeadSink(DeliverDead);            // ★ 死信交换机路由钩子注入
        }

        // ── 持久化装载（磁盘即真相） ──
        //    队列：扫描 data/ 下所有队列目录 → 动态/陌生队列自动认领
        //    交换机：出厂表 → data/_exchanges.json 覆盖（如果上次运行存过）
        LoadExtraQueues(dataDirectory);
        LoadExchanges(dataDirectory);
    }

    public QueueCore? Get(string queueName)
        => _queues.TryGetValue(queueName, out var q) ? q : null;

    // ═══ 队列持久化（磁盘目录 = 队列身份） ═══

    /// <summary>启动认领：data/ 下每个 QueueCore 都会在队列目录下建一个同名目录（含 .dlq 账本夹）。
    ///   首启没有目录 → 各队列出厂即建（兜底）；动态新建后的文件夹重启自动认领。</summary>
    private void LoadExtraQueues(string dataDir)
    {
        if (!Directory.Exists(dataDir)) return;
        foreach (var dir in Directory.GetDirectories(dataDir))
        {
            var name = Path.GetFileName(dir);
            if (_queues.ContainsKey(name)) continue;
            if (name.EndsWith(".dlq", StringComparison.OrdinalIgnoreCase)) continue;   // 死信账本夹（附属品）

            _queues[name] = new QueueCore(name, PushEvent, dataDir);
            _queues[name].SetDeadSink(DeliverDead);
            PushEvent($"📇 [{name}] 磁盘认领：队列目录存在，自动挂账重建");
        }
    }

    /// <summary>动态声明队列（HTTP POST /api/q/{queue} 调用）：幂等，注册即落目录盘</summary>
    public QueueCore DeclareQueue(string queueName)
    {
        if (_queues.TryGetValue(queueName, out var existing)) return existing;
        var q = new QueueCore(queueName, PushEvent, _dataDir);
        q.SetDeadSink(DeliverDead);
        _queues[queueName] = q;
        PushEvent($"🆕 队列声明：{queueName}（主账 + .dlq 死信账本双人套件就绪）");
        return q;
    }

    // ═══ 交换机持久化（data/_exchanges.json 快照） ═══

    public void Bind(string exchangeName, string pattern, string queue)
    {
        if (!_exchanges.TryGetValue(exchangeName, out var ex))
            throw new KeyNotFoundException($"不存在交换机 {exchangeName}");
        var newList = new List<Binding>(ex.Bindings) { new Binding(pattern.Length == 0 ? null : pattern, queue) };
        _exchanges[exchangeName] = new ExchangeDef(ex.Name, ex.Type, newList);
        PersistExchanges();
        PushEvent($"🔗 绑定声明：[{exchangeName}] += {pattern ?? "(fanout)"} → {queue}");
    }

    public void Unbind(string exchangeName, string pattern, string queue)
    {
        if (!_exchanges.TryGetValue(exchangeName, out var ex))
            throw new KeyNotFoundException($"不存在交换机 {exchangeName}");
        var newList = ex.Bindings.Where(b => b.Pattern != pattern || b.Queue != queue).ToList();
        _exchanges[exchangeName] = new ExchangeDef(ex.Name, ex.Type, newList);
        PersistExchanges();
        PushEvent($"✂️ 解绑声明：[{exchangeName}] -= {pattern ?? "(fanout)"} ↛ {queue}");
    }

    private void LoadExchanges(string dataDir)
    {
        var path = Path.Combine(dataDir, "_exchanges.json");
        if (!File.Exists(path)) return;      // 首启：没有快照，出厂表当真相
        try
        {
            var json = File.ReadAllText(path);
            var load = JsonSerializer.Deserialize<List<ExchangeSnapshot>>(json);
            if (load is null) return;
            foreach (var e in load)
            {
                _exchanges[e.Name] = new ExchangeDef(e.Name, e.Type,
                    e.Bindings.Select(b => new Binding(b.Pattern.Length == 0 ? null : b.Pattern, b.Queue)).ToList());
            }
            PushEvent($"📇 交换机装载：data/_exchanges.json → {load.Count} 台交换机（含绑定表）");
        }
        catch (Exception ex)
        {
            PushEvent($"‼️ 交换机快照装载失败（沿用出厂表）：{ex.Message}");
        }
    }

    public void PersistExchanges()
    {
        var path = Path.Combine(_dataDir, "_exchanges.json");
        var snapshots = _exchanges.Values.Select(e => new ExchangeSnapshot(
            e.Name, e.Type,
            e.Bindings.Select(b => new BindingSnapshot(b.Pattern ?? "", b.Queue)).ToList())).ToList();
        File.WriteAllText(path, JsonSerializer.Serialize(snapshots, new JsonSerializerOptions { WriteIndented = true }));
    }

    private sealed record ExchangeSnapshot(string Name, string Type, List<BindingSnapshot> Bindings);
    private sealed record BindingSnapshot(string Pattern, string Queue);

    /// <summary>某段上未完成的消息（Ready/InFlight）—— GC 重写名单的对外键</summary>

    public int Publish(string exchangeName, string? routingKey, string content)
    {
        if (!_exchanges.TryGetValue(exchangeName, out var ex))
            throw new KeyNotFoundException($"不存在交换机 {exchangeName}");

        var targets = ex.Type switch
        {
            "direct" => ex.Bindings.Where(b => b.Pattern == routingKey).Select(b => b.Queue).ToList(),
            "fanout" => ex.Bindings.Select(b => b.Queue).ToList(),
            "topic"  => ex.Bindings.Where(b => b.Matches(routingKey ?? "")).Select(b => b.Queue).ToList(),
            _ => throw new InvalidOperationException($"未知交换机类型 {ex.Type}"),
        };

        foreach (var target in targets)
            _queues[target].Send(content, routingKey);

        PushEvent($"📣 [交换机 {exchangeName}({ex.Type})] routingKey={routingKey ?? "-"} → {string.Join(',', targets)}（共{targets.Count}个队列）");
        return targets.Count;
    }

    public List<ExchangeDef> ListExchanges() => _exchanges.Values.ToList();

    // ── 死信交换机执行器（QueueCore.SetDeadSink 挂进来的回调）──
    //   routingKey = "dead.{来源队列}"，topic 通配匹配 → 命中的绑定队列收 DLQ 投递
    //   没有任何绑定命中 → 回退：回自家队列的死信账本（不弄丢尸体）
    public List<LogMessage> DeliverDead(MqMessage deadMsg, string fromQueue)
    {
        var events = new List<LogMessage>();
        var routingKey = $"dead.{fromQueue}";

        if (!_exchanges.TryGetValue("dead", out var dlx))
        {
            events.AddRange(_queues[fromQueue].AcceptDeadLetter(deadMsg));
            return events;
        }

        var targets = dlx.Type switch
        {
            "direct" => dlx.Bindings.Where(b => b.Pattern == routingKey).Select(b => b.Queue).ToList(),
            "fanout" => dlx.Bindings.Select(b => b.Queue).ToList(),
            "topic"  => dlx.Bindings.Where(b => b.Matches(routingKey)).Select(b => b.Queue).ToList(),
            _ => new List<string>(),
        };

        if (targets.Count == 0)
        {
            PushEvent($"🌡️ [DLX dead] routingKey={routingKey} 无绑定命中 → 回退队自家 DLQ");
            events.AddRange(_queues[fromQueue].AcceptDeadLetter(deadMsg));
            return events;
        }

        foreach (var t in targets)
        {
            if (t == fromQueue)
            {
                // 目标=来源（默认绑定）：直投其死信账本
                events.AddRange(_queues[t].AcceptDeadLetter(deadMsg));
            }
            else
            {
                // 跨队列投递：目标收尸 + 来源主账一并销账
                events.AddRange(_queues[t].AcceptDeadLetter(deadMsg));
                PushEvent($"↪️ [DLX dead({dlx.Type})] {fromQueue} → routingKey={routingKey} → {t}（跨队列安置）");
            }
        }
        PushEvent($"☠️ [DLX dead({dlx.Type})] 死信自 {fromQueue} → routingKey={routingKey} → {string.Join(',', targets)}（共 {targets.Count} 处）");
        return events;
    }

    public (int Ready, int Locked, int PendingOnDisk, int DeadLetters) Aggregate()
    {
        int ready = 0, locked = 0, disk = 0, dead = 0;
        foreach (var q in _queues.Values)
        {
            var s = q.Stats();
            ready += s.ReadyInMemory; locked += s.LockedInMemory;
            disk += s.PendingOnDisk;  dead += s.DeadLetters;
        }
        return (ready, locked, disk, dead);
    }

    public List<QueueCoreStats> StatsPerQueue() => _queues.Values.Select(q => q.Stats()).ToList();

    // 巡查员全队列回收：QueueCore 返回的账本事件 fire-and-forget 复制
    public int SweepAll()
    {
        var reclaimed = 0;
        foreach (var q in _queues.Values)
        {
            var evts = q.SweepExpiredLocks();
            if (evts.Count > 0) ReplicateFireAndForget(q.QueueName, evts);
            reclaimed += evts.Count;
        }
        return reclaimed;
    }

    // 压缩巡查：每节点自己扫自己的磁盘（WAL 压缩不涉及业务状态，无需 leader/follower 之分）
    public void CompactAllChecks()
    {
        foreach (var q in _queues.Values) q.SweepCompaction();
    }

    // ── Leader 复制钩子（P2 核心）：推送事件到 followers，Quorum 结果回传给调用方 HTTP 响应──
    public async Task<ReplicationResult?> ReplicateQueueEventAsync(string queue, LogMessage evt)
    {
        if (_replicator is null) return null;                       // 单机模式
        var res = await _replicator.ReplicateAsync(queue, evt);
        if (res.AckCount < res.Required)
            PushEvent($"⚠️ 复制 Quorum 未达：{res.AckCount}/{res.Required} acks（ackBy={string.Join(',', res.AckedBy)}）");
        return res;
    }

    // 火忘式：巡查员的批量回执（不阻塞）
    public void ReplicateFireAndForget(string queue, List<LogMessage> evts)
    {
        if (_replicator is null) return;
        _ = Task.Run(async () =>
        {
            foreach (var e in evts) await ReplicateQueueEventAsync(queue, e);
        });
    }

    public void PushEvent(string msg)
    {
        lock (_eventLock)
        {
            _events.Add((DateTime.Now, msg));
            if (_events.Count > 200) _events.RemoveAt(0);
        }
        Console.WriteLine(msg);
    }

    public List<(string ts, string msg)> RecentEvents()
    {
        lock (_eventLock)
        {
            return _events.Select(e => (ts: e.Ts.ToString("HH:mm:ss"), msg: e.Msg)).ToList();
        }
    }
}
