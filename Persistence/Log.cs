// ═══════════════════════════════════════════════════════════════
// Persistence/Log.cs —— 队列日志总管（一个队列一本账）
//
//   拆锁版锁的分工：
//     _state      ConcurrentDictionary（账本状态机，读写枚举天然安全）
//     _segments   copy-on-write 清单（**只装已封段 + 压缩标记**，换段=整表原子交换）
//     _activeSegment? 独立volatile活跃段指针（**Cleaner 视野之外**——"正在写的文件不能动"写进集合设计）
//     _activeAppendLock 只护"活跃段的追加顺序"（单锁精益，不锁天下）
//
//   Registry 集成：每个 LedgerEvent 注册到 Registry（GC 比率记账、Acked 幂等窗口、状态机查询）
// ═══════════════════════════════════════════════════════════════
using System.Collections.Concurrent;
using MessageQueueLab.Models;
using MessageQueueLab.Persistence;

namespace MessageQueueLab.Persistence;

public sealed class Log
{
    public const int RowsPerSegment = 512;
    public const int CompactAtSegments = 4;

    private readonly string _dir;
    private readonly Action<string> _emit;
    private readonly object _activeAppendLock = new();
    //记录还存活的消息
    private readonly ConcurrentDictionary<string, (long Seq, MqMessage Msg)> _state = new();
    private volatile List<LogSegment> _segments = new(); // copy-on-write（只含已封段/压缩标记）
    private volatile LogSegment? _activeSegment;
    private int _activeLines;
    private long _nextSeq;
    private long _nextSegmentIndex = 0;
    private volatile bool _compacting;

    public int ItemCount => _state.Count;
    public string QueueName { get; }
    public LogSegment? ActiveSegment => _activeSegment;    // Registry 十 MarkInFlight/MarkDead 定位用
    public int ActiveLineNo => _activeLines;

    public Log(string queueName, string dataDirectory, Action<string> emit)
    {
        QueueName = queueName;
        _dir = System.IO.Path.Combine(dataDirectory, queueName);
        _emit = emit;
        Directory.CreateDirectory(_dir);
        Replay();
    }

    private void Replay()
    {
        var segs = new List<LogSegment>();
        foreach (var path in Directory.GetFiles(_dir, "*.jsonl"))
        {
            var seg = LogSegment.TryCreateFromDisk(path);
            if (seg is not null)
            {
                segs.Add(seg);
                _nextSegmentIndex = Math.Max(_nextSegmentIndex, seg.EndIndex + 1);
            }
        }

        var topSeq = -1L;
        foreach (var seg in segs.OrderBy(s => s.StartIndex))
        {
            var lineNo = 0;
            foreach (var evt in seg.ReadAll())
            {
                // 顺带重建映射表（幂等查重/GC 记账跨重启留存的基础）
                if (evt.Seq >= topSeq)
                {
                    topSeq = evt.Seq;
                    MessageRegistry.ReplayHook(QueueName, evt.Type, evt.Id, evt.Message, seg, lineNo);
                    Apply(evt);
                }
                lineNo++;
            }
        }
        _nextSeq = topSeq + 1;

        var compactedMax = segs.Where(s => s.State == SegmentState.Compacted)
                               .Select(s => s.EndIndex).DefaultIfEmpty(0).Max();
        foreach (var seg in segs.Where(s => s.State == SegmentState.Sealed && s.EndIndex <= compactedMax))
        {
            seg.Delete();
        }
        if (_nextSegmentIndex <= compactedMax) _nextSegmentIndex = compactedMax + 1;

        _emit($"🧾 [{_dir}] 账本回放：{_state.Count} 条存留消息（{segs.Count} 个段文件）");
    }

    private void Apply(LogMessage evt)
    {
        switch (evt.Type)
        {
            case "e":
            case "n":
                if (evt.Message is not null)
                    _state[evt.Message.Id] = (evt.Seq, evt.Message);
                break;
            case "d":
                if (evt.Id is not null) _state.TryRemove(evt.Id, out _);
                break;
        }
    }

    // ================= 追加（写入端：只用 _activeAppendLock） =================

    // ── [新增] 阶段 ①：Build Event（内存，不写盘不入墙）──
    // 供 Quorum 三阶段流水线调用
    public LogMessage BuildEvent(string type, MqMessage? msg, string? id)
    {
        // Ledger 名 = 队列名
        var ledger = QueueName;
        var evt = new LogMessage(_nextSeq++, ledger, type, msg, id);
        return evt;
    }

    // ── 阶段 ③ Commit：多数派已确认，真正落盘 + 状态机推进 ──
    public void CommitEvent(LogMessage evt)
    {
        lock (_activeAppendLock)
        {
            if (_activeSegment is null) OpenNewActiveSegment();
            _activeSegment!.Append(evt);
            Apply(evt);

            // Registry 同步（e/n → Ready；d → Acked）
            if (evt.Type is "e" or "n")
                MessageRegistry.MarkReady(QueueName, _activeSegment, _activeLines, evt.Message);
            else if (evt.Type == "d")
                MessageRegistry.MarkAcked(QueueName, evt.Id);

            if (_activeSegment!.LineCount >= RowsPerSegment)
                SealActive();
        }

    }

    // ── 全量同步 Push（兼容旧接口：Build + Commit 一步） ──
    public LogMessage Push(string type, MqMessage? msg, string? id)
    {
        var evt = BuildEvent(type, msg, id);
        lock (_activeAppendLock)
        {
            if (_activeSegment is null) OpenNewActiveSegment();
            _activeSegment!.Append(evt);
            Apply(evt);

            // Registry 挂账：e/n → Ready；d → Acked
            if (evt.Type is "e" or "n")
                MessageRegistry.MarkReady(QueueName, _activeSegment, _activeLines, evt.Message);
            else if (evt.Type == "d")
                MessageRegistry.MarkAcked(QueueName, evt.Id);

            if (_activeSegment!.LineCount >= RowsPerSegment)
                SealActive();
        }
        return evt;
    }

    private void OpenNewActiveSegment()
    {
        var idx = Math.Max(_nextSegmentIndex, 1);
        var path = System.IO.Path.Combine(_dir, $"s{idx:000000}.jsonl");
        _activeSegment = LogSegment.OpenActive(path, idx);
        _activeLines = 0;
        _nextSegmentIndex = idx + 1;
    }

    private void SealActive()
    {
        _activeSegment!.MarkSealed();
        var newList = new List<LogSegment>(_segments) { _activeSegment };
        _segments = newList;
        _activeSegment = null;
        _activeLines = 0;
    }

    // ── Follower 收 Leader 复制事件（幂等：Seq 回退自动丢弃） ──
    public void PushExternal(LogMessage evt)
    {
        if (evt.Seq < _nextSeq) return;

        lock (_activeAppendLock)
        {
            if (_activeSegment is null) OpenNewActiveSegment();
            _activeSegment!.Append(evt);
            _nextSeq = Math.Max(_nextSeq, evt.Seq + 1);
            Apply(evt);

            if (_activeSegment!.LineCount >= RowsPerSegment)
                SealActive();
        }

        MessageRegistry.EvictStale();               // Follower 同样挂账
    }

    // ================= 压缩计划（LogCleaner 专用，零锁） =================

    public CompactionPlan BuildCompactionPlan()
    {
        var sealedSegs = _segments.Where(s => s.State == SegmentState.Sealed)
                                  .OrderBy(s => s.StartIndex).ToList();
        var start = sealedSegs.Any() ? sealedSegs.Min(s => s.StartIndex) : 1;
        var end = sealedSegs.Any() ? sealedSegs.Max(s => s.EndIndex) : start;

        // 活账快照：携带每条消息出生 Seq（供压缩件重编 em 运动）
        var live = _state.Values.OrderBy(x => x.Seq).Select(x => (x.Seq, x.Msg)).ToList();

        // ⚠️ MqMessage record 没有 Deconstruct → 用显式构造
        return new CompactionPlan(start, end, sealedSegs, live);
    }

    // ── [新增] 双门槛比率触发检查（50% + 行数≥128 或 年龄>1h）──
    //  返回：达到比率的已封段集合（若有）
    public List<LogSegment> TryElevateRatioTrigger()
    {
        var dirty = new List<LogSegment>();
        var now = DateTime.UtcNow;

        foreach (var seg in _segments)
        {
            if (seg.State != SegmentState.Sealed) continue;

            var ratioMet = seg.AckedRatio >= 0.5;
            var sizeMet = seg.LineCount >= 128;
            var ageMet = File.Exists(seg.Path)
                && (now - System.IO.File.GetLastWriteTimeUtc(seg.Path)) > TimeSpan.FromHours(1);

            if (ratioMet && (sizeMet || ageMet))
                dirty.Add(seg);
        }
        return dirty;
    }

    public void ReplaceSegments(string compactedPath, long start, long end, List<LogSegment> oldSegs)
    {
        var newList = new List<LogSegment>(_segments.Where(s => !oldSegs.Contains(s)))
        {
            LogSegment.OpenCompacted(compactedPath, start, end)
        };
        _segments = newList;

        foreach (var old in oldSegs) old.Delete();
        _emit($"🌲 [{_dir}] 压缩完成 → {System.IO.Path.GetFileName(compactedPath)}（覆盖段已清）");
    }

    public List<MqMessage> Snapshot()
    {
        return _state.Values.OrderBy(x => x.Seq).Select(x => x.Msg).ToList();
    }

    /// <summary>带 Seq 的活账视图（catch-up 快照用）。</summary>
    public List<(long Seq, MqMessage Msg)> SnapshotWithSeq()
        => _state.Values.OrderBy(x => x.Seq).Select(x => (x.Seq, x.Msg)).ToList();
    public long NextSeq() => _nextSeq;       // _nextSeq 是"下一个未用 Seq"，uptoSeq = NextSeq - 1

    /// <summary>
    /// delta 回溯读：从 WAL（含压缩段）里读出 Seq ≥ fromSeq 的全部事件行（e/n/d 原序）。
    /// 场景：follower 掉队（ISR 剔除）后，Leader 从该 follower 的 watermark+1 处补发；
    ///       压缩段保留的"活账终态行"也能给到（保证 catch-up 后窗口回溯读不缺行）。
    /// 注意：若 fromSeq 早于最老存在的段（段已被压缩覆盖 BIOS），会从最老的段开始返回——
    ///       那个案件的间隙由快照(catch-up)负责，这里只是尽力补。
    /// </summary>
    public List<LogMessage> ReadEventsFromSeq(long fromSeq)
    {
        var rows = new List<LogMessage>();
        lock (_activeAppendLock)
        {
            foreach (var seg in _segments)
            {
                try { foreach (var evt in seg.ReadAll()) if (evt.Seq >= fromSeq) rows.Add(evt); }
                catch { /* 段文件损坏跳过 */ }
            }
            if (_activeSegment is not null)
            {
                try { foreach (var evt in _activeSegment.ReadAll()) if (evt.Seq >= fromSeq) rows.Add(evt); }
                catch { }
            }
        }
        // 按 Seq 归序 + 同 Seq 去重（Compacted 与原始段可能重复覆盖）
        return rows.OrderBy(e => e.Seq).GroupBy(e => e.Seq).Select(g => g.First()).ToList();
    }

    /// <summary>
    /// catch-up 快照写入：清空本账本所有段文件，整段重建（只含活账 e 事件 + Seq 保真）。
    /// 场景：新节点 join 后从 Leader 拿快照落盘回放。
    /// </summary>
    public void LoadSnapshot(List<(long Seq, MqMessage Msg)> items, long uptoSeq)
    {
        lock (_activeAppendLock)
        {
            // ① 清掉既有段文件（包括压缩产物）
            foreach (var seg in _segments) seg.Delete();
            if (_activeSegment is not null) { _activeSegment.Delete(); _activeSegment = null; }
            _segments = new List<LogSegment>();
            _state.Clear();
            _activeLines = 0;
            _nextSegmentIndex = 1;

            // ② 重建一个含全量活账的段文件（用当前 Seq 数字保真）
            OpenNewActiveSegment();
            foreach (var (seq, msg) in items.OrderBy(x => x.Seq))
            {
                var evt = new LogMessage(seq, QueueName, "e", msg, msg.Id);
                _activeSegment!.Append(evt);
                _state[msg.Id] = (seq, msg);
            }
            // ③ _nextSeq 与新建/回放语义对齐：空账本从 0 起（首个事件 Seq=0 必须能进来），
            //    否则 follower 的 PushExternal 幂等 guard（evt.Seq < _nextSeq）会把 Seq 0 静默丢掉还回 200。
            _nextSeq = items.Count == 0 ? 0 : uptoSeq + 1;
        }
        _emit($"🧾 [{_dir}] 快照载入：{_state.Count} 条活账（uptoSeq={uptoSeq}）");
    }

    // 触发条件（双重门槛）：已封段≥4 或（50% 脏率 +（行数≥128/段龄>1h））
    //   v2：写入路径不再各自触发，改由后台压缩巡检线程统一定时拉起（见 Program.StartSweeper）
    public void SweepCompaction()
        => TryRequestCompaction();

    private void TryRequestCompaction()
    {
        // （1）按已封段数触发（原 Path）
        var sealedCount = _segments.Count(s => s.State == SegmentState.Sealed);
        if (!_compacting && sealedCount >= CompactAtSegments)
        {
            _compacting = true;
            _ = Task.Run(() =>
            {
                try { LogCleaner.Compact(_dir, this); }
                catch (Exception ex) { _emit($"‼️ 压缩线程异常：{ex.Message}"); }
                finally { _compacting = false; }
            });
            return;   // 已触发，直接退
        }

        // （2）按比率触发（新增分支）：AckedRatio≥0.5 且（行数≥128 或 段龄>1h）
        foreach (var seg in TryElevateRatioTrigger())
        {
            if (_compacting) return;
            _compacting = true;
            _ = Task.Run(() =>
            {
                try { LogCleaner.Compact(_dir, this); }
                catch (Exception ex) { _emit($"‼️ 比率周期触发异常：{ex.Message}"); }
                finally { _compacting = false; }
            });
            break;       // 本轮一个即可（等 sealed 清单被压缩完了再算）
        }
    }
}
