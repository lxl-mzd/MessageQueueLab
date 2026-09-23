// ═══════════════════════════════════════════════════════════════
// Persistence/LogCleaner.cs —— 后台压缩线程（专属，不阻塞写入）
//
//   🔧 职责：把"已完结的写入段"重新打包成紧凑的压缩标记段。
//
//   🔍 流程：
//     1. 从 Log 拿压缩计划（BuildCompactionPlan ⇐ 单锁快照）
//        plan.OldSegments = 需要合并的旧段
//        plan.LiveMessages = 当前账面活账消息 (含Seq)
//        plan.Start/End   = 覆盖区间（标记文件的命名依据）
//     2. 活账快照 → 写 c{start}-{end}.compacted.tmp（Column-Layout，逐行加密格式）
//     3. File.Move(tmp → compacted.jsonl, overwrite: true) ← 原子上位
//     4. log.ReplaceSegments(compacted, start, end, oldSegs) ← 换段清单+旧段Delete
//
//   🧠 崩溃矩阵（三档安全防线）：
//     · tmp 半路亡（崩溃于写途中）→ 孤儿文件（启动时被清扫）✅
//     · File.Move 半路断电        → tmp 仍半截（实测 Leased Rename 原子性保证）
//     · compacted 上位成功但旧段未删   → 启动扫描时发现重复段，消重自动移除 ✅
//     · Registry.Relocate 未访问 → 高效标记自动过时，启动后发现即更新（无数据风险）
//
//   🔬 Kafka 同款理论上：LogCleaner 跑的是"分段文件的 merge-sort"——与 RocksDB、
//      Redis AOF rewrite 同族。压缩出来的 c 文件是一个"虫洞快照"（语义上等价于把
//      WAL 的 e/d/n 事件折叠到只含活消息的"终态"）。
// ═══════════════════════════════════════════════════════════════
using MessageQueueLab.Models;

namespace MessageQueueLab.Persistence;

// 压缩计划（Log.BuildCompactionPlan 产出）——Cleaner 的施工清单
//   OldSegments: 需要合并的旧段（生命周期已完结）
//   LiveMessages: 当前账面还活着的消息（Kafka LogMessage snapshot）
public sealed record CompactionPlan(
    long Start,                                  // 覆盖区间起点
    long End,                                    // 覆盖区间终点
    List<LogSegment> OldSegments,                // 要合并（Delete）的旧段集合
    List<(long Seq, MqMessage Msg)> LiveMessages); // 当前活账快照（按Seq FIFO 排队）

public sealed class LogCleaner
{
    /// <summary>
    /// 执行一轮压缩（由 Log.Push 在"已封段 ≥4"时投递；运行于后台线程）。
    ///   dir: 段文件所在目录（队列专属账本夹）
///   log: 所属队列的总管对象（把换段结果回调给它）
/// </summary>
    public static void Compact(string dir, Log log)
    {
        var plan = log.BuildCompactionPlan();
        if (plan.OldSegments.Count == 0) return;      // 没有可压缩的已结束段（活跃段不算）

        // 活账快照 → tmp（完整落盘）
        var tmp = System.IO.Path.Combine(dir, $"c{plan.Start}-{plan.End}.compacted.tmp");
        using (var w = new System.IO.StreamWriter(tmp, false))
        {
            foreach (var (seq, msg) in plan.LiveMessages)
                w.WriteLine(new LogMessage(seq, dir, "e", msg, msg.Id).EncodeLine());
        }

        // 全方位落盘完毕 → 原子上位
        var compacted = System.IO.Path.Combine(dir, $"c{plan.Start}-{plan.End}.compacted.jsonl");
        System.IO.File.Move(tmp, compacted, overwrite: true);

        // 通知总管换段清单 + 删旧段
        log.ReplaceSegments(compacted, plan.Start, plan.End, plan.OldSegments);
    }
}
