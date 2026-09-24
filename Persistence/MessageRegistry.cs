// ═══════════════════════════════════════════════════════════════
// Persistence/MessageRegistry.cs —— 消息注册表（全局 Map：状态 + 位置 + TTL + 幂等窗口）
//
//   注册表机制（对应 GC 的 Mark-Sweep-Compact 三件套）：
//     WAL 账本 = 堆内存真相（只追加）
//     Registry = 投影/索引 id → (State, Location)   ← 布局X：现结构不动（_ready/_locked/_dead 照旧）
//     LogCleaner    = Mark-Sweep：Acked 比率 ≥ 0.5 → GC 重写旧段
//
//   字段（RegistryEntry）：
//     State: Ready / InFlight / Acked / Dead
//     Location: (SegmentPath, LineNo) —— 消息出生行（GC 比率归属计算的记账依据）
//     AckedAtUtc: Acked 时间戳（24h 幂等窗口保留到期）
//     Msg: 全量消息（含 Crc/Key/Timestamp/ReceiveTime/RetryCount/ExpiresAtUtc）
//
//   语义：
//     - Ack (终态)         → State=Acked, AckedAtUtc=now   （24h 幂等查重白送；24h 后惰性逐出）
//     - Nack/超时失踪       → State=Ready（消息回墙重试）
//     - 判死信移送          → State=Dead   → ☠️ 死信账本（待人工 enf）
//     - 死信后被 revive     → State=Ready（回到墙，重新排队）
//     - 消息 TTL 到期       → 免死 → 直接 State=Dead ☠️（有迹可循）
// ═══════════════════════════════════════════════════════════════
using System.Collections.Concurrent;
using MessageQueueLab.Models;

namespace MessageQueueLab.Persistence;

public enum RegistryState { Ready, InFlight, Acked, Dead }

public sealed record RegistryEntry
{
    public string Queue { get; init; } = "";
    public RegistryState State { get; set; }
    public LogSegment? Segment { get; set; }
    public string SegmentPath { get; set; } = "";
    public int LineNo { get; set; }
    public DateTime AckedAtUtc { get; set; }
    public MqMessage Msg { get; init; } = new();
}

public static class MessageRegistry
{
    public static readonly TimeSpan AckWindow = TimeSpan.FromHours(24);   // 幂等窗口（可调）

    private static readonly ConcurrentDictionary<string, RegistryEntry> Entries = new();

    private static string Key(string queue, string id) => $"{queue}\u0001{id}";

    // ── 状态登记（QueueCore 各环节调用）──

    /// <summary>Ready：消息入账/回墙/复活（send / retry / revive 之后）</summary>
    public static void MarkReady(string queue, LogSegment? segment, int lineNo, MqMessage msg)
    {
        var key = Key(queue, msg.Id);
        if (Entries.TryGetValue(key, out var e) && e.State == RegistryState.Acked)
        {
            // 复活场景：原 Acked 纪录被 Ready 覆盖，operator 需注意（幂等审核覆盖即可）
            Entries[key] = e with { State = RegistryState.Ready, Segment = segment, SegmentPath = segment?.Path ?? "", LineNo = lineNo, Msg = msg };
            return;
        }
        Entries[key] = new RegistryEntry
        {
            Queue = queue,
            State = RegistryState.Ready,
            Segment = segment,
            SegmentPath = segment?.Path ?? "",
            LineNo = lineNo,
            Msg = msg,
        };
    }

    /// <summary>InFlight(执行中)：消费者领走（写 ReceiveTime —— Registry 的 InFlight 位置同款）</summary>
    public static void MarkInFlight(string queue, LogSegment? segment, int lineNo, MqMessage msg)
    {
        var key = Key(queue, msg.Id);
        if (Entries.TryGetValue(key, out var entry))
        {
            Entries[key] = entry with { State = RegistryState.InFlight, Segment = segment, SegmentPath = segment?.Path ?? "", LineNo = lineNo, Msg = msg };
        }
        else
        {
            Entries[key] = new RegistryEntry
            {
                Queue = queue,
                State = RegistryState.InFlight,
                Segment = segment,
                SegmentPath = segment?.Path ?? "",
                LineNo = lineNo,
                Msg = msg,
            };
        }
    }

        /// <summary>Ack：确认完成。同步把账本 Acked 计数计入出生段（GC 触发源）</summary>
    // ── 启动重建钩子：WAL 是这张表的持久化形态，回放时逐事件重算（非独立持久化）──
    //    Replay 按 (段序,行序) 逐行调用；e/n → Ready；d → Acked（幂等窗口自此刻起算）
    public static void ReplayHook(string queue, string type, string? id, MqMessage? msg, LogSegment? seg, int lineNo)
    {
        switch (type)
        {
            case "e":
            case "n":
                if (msg is null) return;
                Entries[Key(queue, msg.Id)] = new RegistryEntry
                {
                    Queue = queue,
                    State = RegistryState.Ready,
                    Segment = seg,
                    SegmentPath = seg?.Path ?? "",
                    LineNo = lineNo,
                    Msg = msg,
                };
                break;

            case "d":
                if (id is null) return;
                if (Entries.TryGetValue(Key(queue, id), out var e))
                {
                    e.Segment?.RecordAck();   // 出生段的销账比率记账（GC 触发依据要跨重启生效）
                    Entries[Key(queue, id)] = e with { State = RegistryState.Acked, AckedAtUtc = DateTime.UtcNow };
                }
                break;
        }
    }

    /// <summary>UnityRegister: 兼容 wrapper —— QueueCore 直接传 Log 对象</summary>
    public static void UnityRegister(Log ledger, MqMessage msg, RegistryState state)
        => MarkReady(ledger.QueueName, ledger.ActiveSegment, ledger.ActiveLineNo, msg);

    public static void MarkAcked(string queue, string id)
    {
        var key = Key(queue, id);
        if (Entries.TryGetValue(key, out var entry) && entry.State != RegistryState.Acked)
        {
            // 💰 销账计数归到“出生段”（Registry 的本职记账）
            entry.Segment?.RecordAck();
            Entries[key] = entry with { State = RegistryState.Acked, AckedAtUtc = DateTime.UtcNow };
        }
    }

    /// <summary>Dead(死信)：判死移送进 DLQ（via 死信账本 e 事件）</summary>
    public static void MarkDead(string queue, LogSegment? segment, int lineNo, MqMessage msg)
    {
        var key = Key(queue, msg.Id);
        Entries[key] = new RegistryEntry
        {
            Queue = queue,
            State = RegistryState.Dead,
            Segment = segment,
            SegmentPath = segment?.Path ?? "",
            LineNo = lineNo,
            Msg = msg,
        };
    }

    /// <summary>复活：死信送回主队列 → State 重新赋值 Ready（计数清零在 LogMessage 内容里）</summary>
    public static void MarkRevived(string queue, LogSegment? segment, int lineNo, MqMessage msg)
    {
        MarkReady(queue, segment, lineNo, msg);
    }

    // ── 查询 ──
    public static RegistryEntry? TryGet(string queue, string msgId)
        => Entries.TryGetValue(Key(queue, msgId), out var e) ? e : null;

    /// <summary>某段上未完成的消息（Ready/InFlight）—— GC 重写名单</summary>
    public static List<MqMessage> AliveIn(LogSegment segment)
    {
        var alive = new List<MqMessage>();
        foreach (var e in Entries.Values)
        {
            if (ReferenceEquals(e.Segment, segment) &&
                (e.State == RegistryState.Ready || e.State == RegistryState.InFlight))
                alive.Add(e.Msg);
        }
        return alive;
    }

    /// <summary>惰性逐出：Acked 超过 24h 幂等窗口的条目</summary>
    public static void EvictStale()
    {
        var now = DateTime.UtcNow;
        foreach (var key in Entries.Keys)
        {
            var e = Entries[key];
            if (e.State == RegistryState.Acked && now - e.AckedAtUtc > AckWindow)
                Entries.TryRemove(key, out _);
        }
    }

    /// <summary>消息级 CRC 复算校验（与 LogMessage.ComputeContentCrc 同款）</summary>
    public static bool VerifyContent(MqMessage msg)
        => string.Equals(
            LogMessage.ComputeContentCrc(msg with { Crc = null }),
            msg.Crc,
            StringComparison.OrdinalIgnoreCase);
}
