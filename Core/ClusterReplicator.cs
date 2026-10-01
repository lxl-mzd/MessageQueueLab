// ═══════════════════════════════════════════════════════════════
// Core/ClusterReplicator.cs —— Leader 侧复制器（Quorum 写入）
//
//   🔧 职责（这是 Leader 写入端的核心部件）：
//      · Leader 每写一条主账本事件 → 并行推给所有 Follower
//      · 收集确认数 acks
//      · Quorum 多数派公式 = 1（Leader 本身）+ ⌈followers/2⌉ 个 follower 回执
//      · 例：3 节点集群（1 leader + 2 followers）：
//        → acks ≥ 1 才算达到 2/3
//
//   🔥 一致性等级：
//      · 教学模式（at-least-once）：Leader 收下生产者消息 + 本地落盘成功即回 201，
//        follower 异步追复制（悲观下 follower 可能落后）。
//      · 真生产级（CP/strong consistency）：可以在此处改为同步等待 ackCount ≥ quorum，
//        拿不到多数派回执就 503 生产者重试。（代码已预留标记）
//
//   🧠 复制语义细节：
//     · 事件内携带 Seq 全序号（全局单调递增），follower 判断是否重复投递
//     · follower 收到旧 Seq 直接丢弃，不算重复（AppendLedger 本身天然消重）
// ═══════════════════════════════════════════════════════════════
using System.Collections.Concurrent;
using System.Net.Http.Json;
using System.Text.Json;
using MessageQueueLab.Models;
using MessageQueueLab.Persistence;

namespace MessageQueueLab.Core;

/// <summary>
/// 一次"事件复制"的结果
///   AckCount  : 完成的follower 票数（Leader自身=1）
///   Required  : 集群应满足的多数派票数
///   AckedBy   : 具名确认的 follower 基址列表（网页/看板可视化用）
/// </summary>
public record ReplicationResult(int AckCount, int Required, List<string> AckedBy);

public class ClusterReplicator
{
    /// <summary>与 follower 通信的 HttpClient（BaseAddress 在每个 follower 的请求前拼接）</summary>
    private readonly HttpClient _http = new();
    /// <summary>启动期 follower 基址（Program 喂的 BootstrapTargets；运行时走 Raft 成员表，此仅 fallback）</summary>
    private readonly string[] _followers;
    /// <summary>达到多数派需要的 follower 回执票数（阈值：N/2+1）</summary>
    private readonly int _requiredAcks;          // 需要几个 follower 回执（含 leader 已=1）

    public ClusterReplicator(string[] followers)
    {
        _followers = followers;
        // ⭐ N/2+1 选择规则：followers=2 → 需1票；followers=4 → 需3票
        // 待为 Quorum 复制（数学上保证：两个 sub-quorum 附近共享至少一个 follower —— 防 split-brain)
        _requiredAcks = Math.Max(1, followers.Length / 2 + 1);
    }

    public int RequiredAcks => _requiredAcks;

    /// <summary>
    /// 把事件复制给每个 Follower，等待各自确认。
    /// 每个POST都用 Task.Run 包裹，触发可并发——不阻塞业务写其它部分的 push。
    /// </summary>
    public async Task<ReplicationResult> ReplicateAsync(string queue, LogMessage evt)
    {
        var ackedBy = new List<string>();

        // ── 动态成员表（Raft 成员驱动；无 Raft 引用时退回静态 env 配置的 _followers） ──
        RaftNode? raft = null;
        RaftNode? raftNode = ClusterOptions.Raft;
        if (raftNode != null && raftNode.Synced) raft = raftNode;
        var targets = raft is not null
                        ? raft.PublishTargets()      // Syncing 成员被 Raft 状态闸门自动过筛
                        : _followers;
        var ackRequired = raft is not null ? raft.WriteMajority()
                                                        : Math.Max(1, targets.Length / 2 + 1);
        var totalMembers = raft is not null ? raft.MemberCount : targets.Length + 1;



        // 并行传感器：每个 follower 一张“回执票”（POST；返回成功=真回执）
        var tasks = targets.Select(async f =>
        {
            try
            {
                var resp = await _http.PostAsJsonAsync(
                    $"{f}/api/cluster/replicate/{queue}",
                    evt);                                // 直接发送完整 LogMessage（Message 全形）
                if (resp.IsSuccessStatusCode) return f;
                return null;
            }
            catch { return null; }
        }).ToList();

        var done = await Task.WhenAll(tasks);
        // ── per-follower 水位推进 + ISR 剔除标记（Leader 视角） ──
        //    水位 key 必须用 Ledger（"normal"/"normal.dlq"），不能用 API queue 名：
        //    主账本与死信账本是两个独立 Seq 空间，混用一个 key 会互相污染水位。
        var ledgerKey = evt.Ledger?.Length > 0 ? evt.Ledger : queue;
        for (var i = 0; i < targets.Length; i++)
        {
            var f = targets[i];
            if (done[i] is not null)
            {
                var wm = _watermarks.GetOrAdd(f, _ => new ConcurrentDictionary<string, long>());
                wm.AddOrUpdate(ledgerKey, evt.Seq, (_, old) => Math.Max(old, evt.Seq));
                _lagging.TryRemove(f, out _);
            }
            else
            {
                _lagging[f] = 1;    // 推送失败 → 进 ISR 剔除名单（等待后台重放线程）
            }
        }
        ackedBy.AddRange(done.Where(x => x != null).Cast<string>());

        return new ReplicationResult(ackedBy.Count + 1, totalMembers, ackedBy);
    }

    // ═══════════════════════════════════════════════════════════════
    //  Raft 标准做法：**per-follower 复制水位**（Kafka 即 LEO/HW，Raft 即 nextIndex）
    //
    //    Leader 对每个 follower 记录每个队列的 lastAckSeq：
    //      · 推送成功 → watermark 前进
    //      · 推送失败 → follower 进 lagging 名单（ISR 剔除）→ 后台补发线程从水位处补
    //      · sync-ack（新节点 catch-up 完成）→ 从 Leader 侧 seed 该新节点的初始水位
    //
    //    WAL 就是唯一的"buffer" —— append-only 永远在，掉队就从落后点读回来。
    // ═══════════════════════════════════════════════════════════════

    // followerUrl → (queue → lastAckedSeq)
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, long>> _watermarks = new();
    // 掉队名单（ISR 剔除者）
    private readonly ConcurrentDictionary<string, byte> _lagging = new();

    public long? WatermarkOf(string followerUrl, string queue)
    {
        return _watermarks.TryGetValue(followerUrl, out var wm) && wm.TryGetValue(queue, out var seq)
            ? (long?)seq
            : null;
    }

    public void SeedWatermark(string followerUrl, string queue, long seq)
    {
        var wm = _watermarks.GetOrAdd(followerUrl, _ => new ConcurrentDictionary<string, long>());
        wm.AddOrUpdate(queue, seq, (_, old) => Math.Max(old, seq));
    }

    public bool IsLagging(string followerUrl) => _lagging.ContainsKey(followerUrl);

    public void MarkLagging(string followerUrl, bool lagging)
    {
        if (lagging) _lagging[followerUrl] = 1;
        else _lagging.TryRemove(followerUrl, out _);
    }

    /// <summary>所有处于掉队状态的 follower URL（供巡查员补发循环消费）。</summary>
    public List<string> LaggingFollowers() => _lagging.Keys.ToList();

    /// <summary>该 follower 的所有队列的 watermark 快照（诊断用）。</summary>
    public IReadOnlyDictionary<string, long> WatermarksFor(string followerUrl)
        => _watermarks.TryGetValue(followerUrl, out var wm) ? (IReadOnlyDictionary<string, long>)new Dictionary<string, long>(wm) : new Dictionary<string, long>();
}

// ⭐ 未来加强点（生产级可切换）:
//  · 同步等待确认（followers 同步到达才回复生产者）→ 拉低 TPS 但消灭丢失缝隙
//  · 事件 Promise-Future（生产者 send() 立即返回 future；follower 回执后 future resolve）
//  · acked 之后 fsync 强刷 + SHA256 数据签名
