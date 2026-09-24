// ═══════════════════════════════════════════════════════════════
// Core/QueueCore.cs —— 单队列核心引擎
//
//   Quorum 写入三阶段流水线：
//     阶段 ① PrepareEvent   生成事件（内存 CAS 无写入）
//     阶段 ② Replicate+Quorum    由 Leader 分发给 Follower，只等多数派 OK
//     阶段 ③ CommitEvent    多数派已达标 → 真正写本地 WAL + 内存推进
//
//   顺序铁律：先 Quorum 后 commit —— 不行就 503 重试，而且本地不留垃圾.
// ═══════════════════════════════════════════════════════════════
using System.Collections.Concurrent;
using MessageQueueLab.Models;
using MessageQueueLab.Persistence;

namespace MessageQueueLab.Core;

public class QueueCore
{
    public const int MaxRetryCount = 3;
    public const int DlqCapacity = 500;

    private readonly string _queueName;
    private readonly Log _ledger;
    private readonly Log _dlqLedger;
    private readonly ConcurrentQueue<MqMessage> _ready = new();
    private readonly ConcurrentDictionary<string, (MqMessage Msg, DateTime ExpiresAtUtc)> _locked = new();
    private readonly List<MqMessage> _dead = new();
    private readonly object _dlqLock = new();
    private readonly Action<string> _emit;
    private Func<MqMessage, string, List<LogMessage>>? _deadSink;   // 死信交换机路由钩子（Hub 装配后注入）

    public string QueueName => _queueName;

    /// <summary>装配钩子：Hub 把 DLX 路由函数挂进来（死信不再写死直进自家账本）</summary>
    public void SetDeadSink(Func<MqMessage, string, List<LogMessage>> sink) => _deadSink = sink;

    public QueueCore(string queueName, Action<string> emit, string dataDirectory = "data")
    {
        _queueName = queueName;
        _emit = emit;

        _ledger    = new Log(queueName, dataDirectory, emit);
        _dlqLedger = new Log(queueName + ".dlq", dataDirectory, emit);
        RestoreFromDisk();
    }

    private void RestoreFromDisk()
    {
        foreach (var msg in _ledger.Snapshot())
        {
            _ready.Enqueue(msg);
        }

        _dead.AddRange(_dlqLedger.Snapshot());

        var deadIds = _dead.Select(m => m.Id).ToHashSet();
        if (deadIds.Count > 0)
        {
            var survivors = new List<MqMessage>();
            while (_ready.TryDequeue(out var m))
                if (!deadIds.Contains(m.Id)) survivors.Add(m);
            foreach (var m in survivors) _ready.Enqueue(m);
            if (survivors.Count < deadIds.Count)
                _emit($"📌 [{_queueName}] 安检：死信在册 {deadIds.Count} 条，重名的上岗申请已被驳回");
        }

        _emit($"🧾 [{_queueName}] 开店盘点：主账本 {_ready.Count} 条待处理 | 死信 {_dead.Count} 条待人工处置");
    }

    // ══ Quorum 写入三阶段 ══

    // 阶段 ① Prepare：只造消息和事件（不写盘、不入墙）
    public (MqMessage Msg, LogMessage Evt) Prepare(string content, string? key = null, int? ttlSeconds = null)
    {
        var msg = new MqMessage
        {
            Id = Guid.NewGuid().ToString("N"),
            Key = key,
            Content = content,
            Timestamp = DateTime.UtcNow,
            ReceiveTime = null,
            RetryCount = 0,
            ExpiresAtUtc = ttlSeconds is > 0
                ? DateTime.UtcNow.AddSeconds(ttlSeconds.Value)
                : null,
        };

        msg = msg with { Crc = LogMessage.ComputeContentCrc(msg) };

        var evt = _ledger.BuildEvent("e", msg, msg.Id);
        return (msg, evt);
    }

    // 阶段 ③ Quorum 达标后，真正把事件落盘+内存入墙
    public void CommitEvent(LogMessage evt)
    {
        if (evt.Message is null) return;
        _ledger.CommitEvent(evt);
        _ready.Enqueue(evt.Message);
        _emit($"📥 [{_queueName}] commit: id={evt.Message.Id[..12]}… content={evt.Message.Content}");
    }

    // 兼容旧一则（Send without Quorum 入口 —— 走同步测试/单机路径）
    public (MqMessage Msg, LogMessage Evt) SendDirect(string content, string? key = null, int? ttlSeconds = null)
    {
        var (msg, evt) = Prepare(content, key, ttlSeconds);
        CommitEvent(evt);
        return (msg, evt);
    }

    public MqMessage Send(string content, string? key = null, int? ttlSeconds = null)
    {
        var (msg, evt) = SendDirect(content, key, ttlSeconds);
        _emit($"📥 [{_queueName}] 收到消息 id={msg.Id[..12]}… content={msg.Content}");
        return msg;
    }

    // ── 长轮询 + TTL 检查 ──
    public MqMessage? Receive(int visibilitySeconds = 30, int waitMs = 0)
    {
        var deadline = DateTime.UtcNow.AddMilliseconds(waitMs);
        while (true)
        {
            if (_ready.TryDequeue(out var msg))
            {
                // ══ 幂等防线：账本已销(d 事件) → 就算消息重现也不准领 ══
                //    场景：竞态把 Acked 覆盖、或跨重启重建间隙 —— 账本说了算，同时把内存账面销平
                if (MessageRegistry.TryGet(QueueName, msg.Id) is { State: RegistryState.Acked })
                {
                    _ledger.Push("d", null, msg.Id);
                    _emit($"🧊 [{_queueName}] 幂等拦截：id={msg.Id[..12]}… 已销账的幽灵消息 → 丢弃");
                    continue;
                }

                // ══ TTL 检查：过期 → 死信（继续取下一条）═══
                if (msg.ExpiresAtUtc is not null && msg.ExpiresAtUtc.Value <= DateTime.UtcNow)
                {
                    _dlqLedger.Push("e", msg, msg.Id);
                    _dead.Add(msg);
                    _ledger.Push("d", null, msg.Id);
                    MessageRegistry.MarkDead(_queueName, _dlqLedger.ActiveSegment, _dlqLedger.ActiveLineNo, msg);
                    _emit($"☠️ [{_queueName}] TTL 过期 msg={msg.Id[..12]}…"); 
                    continue;
                }

                msg = msg with { ReceiveTime = DateTime.UtcNow };
                _locked[msg.Id] = (msg, DateTime.UtcNow.AddSeconds(visibilitySeconds));
                MessageRegistry.MarkInFlight(QueueName, _ledger.ActiveSegment, _ledger.ActiveLineNo, msg);
                return msg;
            }
            if (waitMs <= 0 || DateTime.UtcNow >= deadline) return null;
            Thread.Sleep(40);
        }
    }

    // ── Ack（三阶段：Prepare 拆锁 → 复制 Quorum → Commit 落盘销账）──

    // 阶段 ①：从锁定区取下消息 + 生成 d 事件（不写盘、不销账）
    public (bool Ok, LogMessage Evt) PrepareAck(string id)
    {
        if (!_locked.TryRemove(id, out _)) return (false, _ledger.BuildEvent("d", null, id));
        var evt = _ledger.BuildEvent("d", null, id);
        return (true, evt);
    }

    // 阶段 ③：Quorum 达标后销账落盘
    public void CommitAck(LogMessage evt)
    {
        _ledger.CommitEvent(evt);
        MessageRegistry.MarkAcked(QueueName, evt.Id!);
        _emit($"✅ [{_queueName}] 销账 id={evt.Id![..12]}…");
    }

    // 兼容旧入口：单机路径一步搞定
    public (bool Acked, List<LogMessage> Events) Ack(string id)
    {
        var (ok, evt) = PrepareAck(id);
        if (!ok) return (false, new());
        CommitAck(evt);
        return (true, new List<LogMessage> { evt });
    }

    // ══ 统一失败善后 ══
    private List<LogMessage> ProcessFailedDelivery(MqMessage failedMsg, string reason)
    {
        var retried = failedMsg with { RetryCount = failedMsg.RetryCount + 1 };
        var events = new List<LogMessage>();

        if (retried.RetryCount >= MaxRetryCount)
        {
            // 💣 主账销账（无条件——死信写入哪个 DLQ 由死信交换机决定）
            var e2 = _ledger.Push("d", null, retried.Id);
            events.Add(e2);

            if (_deadSink is not null)
            {
                // ══ 走 DLX：死 → dead.{queue} → 绑定命中的目标 DLQ ══
                events.AddRange(_deadSink(retried, _queueName));
            }
            else
            {
                // 兜底直投自家 DLQ（solo/测试构造无 Hub 钩子时）
                lock (_dlqLock)
                {
                    if (_dead.Count >= DlqCapacity)
                        return events;
                }
                var e1 = _dlqLedger.Push("e", retried, retried.Id);
                _dead.Add(retried);
                events.Add(e1);
                MessageRegistry.MarkDead(_queueName, _dlqLedger.ActiveSegment, _dlqLedger.ActiveLineNo, retried);
            }
            _emit($"☠️ [{_queueName}] 消息 {retried.Id[..12]}… 已失败 {retried.RetryCount} 次（原因：{reason}）");
            return events;
        }

        events.Add(_ledger.Push("n", retried, retried.Id));
        MessageRegistry.MarkReady(_queueName, _ledger.ActiveSegment, _ledger.ActiveLineNo, retried);
        _ready.Enqueue(retried);
        return events;
    }

    public (MqMessage? Msg, List<LogMessage> Events) Nack(string id)
    {
        if (!_locked.TryRemove(id, out var entry)) return (null, new());
        var events = ProcessFailedDelivery(entry.Msg, reason: "消费者主动拒绝重试");
        var last = events.LastOrDefault(e => e.Type is "e" or "n");
        var retried = last?.Message;
        return (retried, events);
    }

    public List<MqMessage> DeadLetters() { lock (_dlqLock) return _dead.ToList(); }

    public (bool Ok, List<LogMessage> Events) DeleteDeadLetter(string id)
    {
        lock (_dlqLock)
        {
            var removed = _dead.FirstOrDefault(m => m.Id == id);
            if (removed is null) return (false, new());
            _dead.Remove(removed);
            _dlqLedger.Push("d", null, id);
            _emit($"✅ [{_queueName}] 死信 {id[..12]}… 销账离场");
            return (true, new());
        }
    }

    public (MqMessage? Revived, List<LogMessage> Events) ReviveDeadLetter(string id)
    {
        lock (_dlqLock)
        {
            var dead = _dead.FirstOrDefault(m => m.Id == id);
            if (dead is null) return (null, new());
            _dead.Remove(dead);

            var revived = dead with { RetryCount = 0, ReceiveTime = null };
            var e1 = _ledger.Push("e", revived, revived.Id);
            _dlqLedger.Push("d", null, id);

            _ready.Enqueue(revived);
            _emit($"🔁 [{_queueName}] 死信 {id[..12]}… 被送回主队列");
            return (revived, new List<LogMessage> { e1 });
        }
    }

    public QueueCoreStats Stats()
    {
        lock (_dlqLock)
            return new QueueCoreStats(_queueName, _ready.Count, _locked.Count, _ledger.ItemCount, _dead.Count);
    }

    // 压缩巡查入口：交给所属 Wal Log 做双门槛判定（段数≥4 / 50% 脏率）
    public void SweepCompaction() => _ledger.SweepCompaction();

    /// <summary>DLX 投递目标：把外来死信收进本队列的死信账本（死信交换机绑定命中后调用）</summary>
    public List<LogMessage> AcceptDeadLetter(MqMessage msg)
    {
        var events = new List<LogMessage>();
        lock (_dlqLock)
        {
            if (_dead.Count >= DlqCapacity)
            {
                _emit($"🚧 [{_queueName}] DLQ 已满（{DlqCapacity}），外来死信被拒收（自生自灭）");
                return events;   // 满仓 → 空 events，调用方主账已自行销账
            }
        }
        var e1 = _dlqLedger.Push("e", msg, msg.Id);
        _dead.Add(msg);
        MessageRegistry.MarkDead(_queueName, _dlqLedger.ActiveSegment, _dlqLedger.ActiveLineNo, msg);
        _emit($"📥 [{_queueName}] DLX 投递：死信 {msg.Id[..12]}… 入驻本队列死信账本");
        events.Add(e1);
        return events;
    }

    public void PushExternal(LogMessage evt)
    {
        if (string.IsNullOrEmpty(evt.Ledger) || evt.Ledger == _queueName)
        {
            _ledger.PushExternal(evt);
            if (evt.Message is null) return;
            // 主账本复制事件同步挂账（Follower 王位切换后马上要靠这张表做查重/记账）
            if (evt.Type is "e" or "n")
                MessageRegistry.MarkReady(QueueName, null, 0, evt.Message);
        }
        else
        {
            _dlqLedger.PushExternal(evt);
            if (evt.Type == "e" && evt.Message is not null)
            {
                _dead.Add(evt.Message);
                MessageRegistry.MarkDead(QueueName, null, 0, evt.Message);
            }
            if (evt.Type == "d" && evt.Id is not null)
            {
                var removed = _dead.FirstOrDefault(m => m.Id == evt.Id);
                if (removed is not null) _dead.Remove(removed);
                MessageRegistry.MarkAcked(QueueName, evt.Id);
            }
        }
    }

    public List<LogMessage> SweepExpiredLocks()
    {
        var events = new List<LogMessage>();
        var now = DateTime.UtcNow;
        foreach (var kv in _locked)
        {
            if (kv.Value.ExpiresAtUtc < now && _locked.TryRemove(kv.Key, out var entry))
            {
                events.AddRange(ProcessFailedDelivery(entry.Msg, reason: "消费者失踪(锁定超时)"));
            }
        }
        return events;
    }

    public int SweepExpiredMessages()
    {
        var reclaimed = 0;
        var now = DateTime.UtcNow;
        var outOf = new List<MqMessage>();
        while (_ready.TryDequeue(out var msg))
        {
            if (msg.ExpiresAtUtc is not null && msg.ExpiresAtUtc.Value <= now)
            {
                _dlqLedger.Push("e", msg, msg.Id);
                _dead.Add(msg);
                _ledger.Push("d", null, msg.Id);
                reclaimed++;
                continue;
            }
            _ready.Enqueue(msg);
        }
        return reclaimed;
    }
}
