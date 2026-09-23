// ═══════════════════════════════════════════════════════════════
// Core/RaftNode.cs —— Raft 状态机（Follower → Candidate → Leader 三态）
//
//   核心思想（教学简化版 Raft，仅实现 Leader 选举协议）：
//     1. Follower 超过 electionTimeout 没收到 Leader 心跳 → 变 Candidate
//     2. Candidate 向所有其他节点请求投票（Term + 1）
//     3. 收到多数派票（N/2+1）→ 当选 Leader
//     4. 收到更高 Term 的 RPC → 自动降级为 Follower
//
//   设计保证（安全性守则）：
//     · 一个 Term 内最多投一票（防脑裂）
//     · 心跳接收优先级高于投票（防频繁选举）
//     · Election timeout 随机化（2000~4500ms）避免同时全员决定 Claiming
//
//   集成方式：
//     · HTTP RPC：Web\Endpoints.Raft.cs 提供的接口
//     · Leader 就位自动挂 ClusterReplicator 推事件流
// ═══════════════════════════════════════════════════════════════
using System.Net.Http.Json;

namespace MessageQueueLab.Core;

public enum RaftRole { Follower, Candidate, Leader }

public sealed class RaftNode : IDisposable
{
    public sealed record RaftVoteResponse(long Term, string VoterId, bool VoteGranted);
    private readonly string _nodeId;
    private readonly string[] _peerUrls;       // 除自身外的其他 Raft 节点 HTTP 基址
    private readonly HttpClient _http = new();
    private readonly Random _rand = new();
    private readonly Action<string>? _emit;   // 日志回调（看板流水使用）

    // ── Raft 核心状态（在 _stateLock 保护下手动修改） ──
    private long _currentTerm;                // 当前任期号
    private string? _votedFor;                // 当前任期投给了谁（null=未投）
    private string? _leaderId_;               // Stage指定的leader id（内部state. Full name= Leader）
    private RaftRole _role = RaftRole.Follower;
    private DateTime _lastHeartbeatUtc = DateTime.UtcNow;
    private int _electionTimeoutMs;
    private volatile bool _stopping;
    private Task? _raftLoopTask;
    private readonly CancellationTokenSource _cts = new();

    public RaftRole Role => _role;
    public long CurrentTerm => _currentTerm;
    public string NodeId => _nodeId;
    public string? CurrentLeaderId => _leaderId_ ?? _nodeId;
    public IReadOnlyList<string> Peers => _peerUrls;

    public RaftNode(string nodeId, string selfUrl, string[] peerUrls, Action<string>? emit = null)
    {
        _nodeId = nodeId;
        // 剥掉自身地址：自己不给自己发心跳/拉票 —— 否则 Leader 上任第 100ms 就把自己降级了
        _peerUrls = peerUrls.Where(p => !string.Equals(p, selfUrl, StringComparison.OrdinalIgnoreCase)).ToArray();
        _emit = emit;
        // 选举超时随机化 2000~4500 ms（同时发起选主概率低）
        _electionTimeoutMs = _rand.Next(2000, 4500);
        _lastHeartbeatUtc = DateTime.UtcNow;
    }

    // ── Start：Background loop（心跳 + 选举）─
    public Task StartAsync()
    {
        var ct = CancellationToken.None;
        _raftLoopTask = Task.Run(async () =>
        {
            while (!ct.IsCancellationRequested && !_stopping)
            {
                try { await RunTickAsync(ct); }
                catch (OperationCanceledException) { break; }
                catch (Exception ex) { Console.WriteLine($"[raft:{_nodeId}] tick error: {ex.Message}"); }
            }
        }, ct);
        return _raftLoopTask;
    }

    public void Stop()
    {
        _stopping = true;
        _cts.Cancel();
    }

    public void Dispose()
    {
        Stop();
        _cts.Dispose();
        _http.Dispose();
    }

    // 主循环 tick：每 100ms检测一次（Follower election timeout/Leader heartbeat broadcast）
    private async Task RunTickAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested && !_stopping)
        {
            await Task.Delay(100, ct);
            var elapsedMs = (DateTime.UtcNow - _lastHeartbeatUtc).TotalMilliseconds;

            switch (_role)
            {
                case RaftRole.Leader:
                    await SendHeartbeatsAsync(ct);
                    break;
                case RaftRole.Follower:
                case RaftRole.Candidate:
                    // 如果距上次心跳超时，变候选议
                    if (elapsedMs >= _electionTimeoutMs)
                        await StartElectionAsync(ct);
                    break;
            }
        }
    }

    // ── Candidate：发选举（Term+1，自投 + 请其他节点投票）──
    private async Task StartElectionAsync(CancellationToken ct)
    {
        _currentTerm++;
        _votedFor = _nodeId;       // 给自己一票
        _role = RaftRole.Candidate;
        _leaderId_ = null;

        _emit?.Invoke($"🔔 [raft:{_nodeId}] term={_currentTerm} Candidate → 向 {_peerUrls.Length} 节点请求投票");

        // 并行给所有 peer 发送 RequestVote
        var tasks = _peerUrls.Select(async peer =>
        {
            try
            {
                var resp = await _http.PostAsJsonAsync($"{peer}/api/raft/vote",
                    new { term = _currentTerm, candidateId = _nodeId });
                resp.EnsureSuccessStatusCode();
                var json = await resp.Content.ReadAsStringAsync(ct);
                var obj = System.Text.Json.JsonDocument.Parse(json).RootElement;
                bool granted = obj.TryGetProperty("voteGranted", out var g) && g.GetBoolean();
                return granted;
            }
            catch { return false; }
        }).ToList();

        var votes = await Task.WhenAll(tasks);
        int grantedCount = 1;                        // 自己 + 1 票
        foreach (var v in votes) if (v) grantedCount++;

        var majority = (_peerUrls.Length + 1) / 2 + 1;     // N/2+1 (leader本人+N x n众人)
        if (grantedCount >= majority)
        {
            _role = RaftRole.Leader;
            _leaderId_ = _nodeId;
            _emit?.Invoke($"👑 [raft:{_nodeId}] 当选 Leader (term={_currentTerm}, votes={grantedCount}/{_peerUrls.Length + 1})");
            await SendHeartbeatsAsync(ct);   // 上任就立刻给所有人发心跳广播
        }
        else
        {
            _role = RaftRole.Follower;
            _electionTimeoutMs = _rand.Next(2000, 4500);
            _lastHeartbeatUtc = DateTime.UtcNow;
        }
    }

    // ── Leader 心跳广播（AppendEntries 空包：向所有 follower 通知自己存在）
    //  ⚠️ 不给自己发心跳——否则 ReceiveHeartbeat 会把自己的 Leader 降级为 Follower !
    private async Task SendHeartbeatsAsync(CancellationToken ct)
    {
        var hb = new { term = _currentTerm, leaderId = _nodeId };
        var tasks = _peerUrls.Select(async peer =>
        {
            try
            {
                await _http.PostAsJsonAsync($"{peer}/api/raft/heartbeat", hb);
            }
            catch { /* silently fail — follower might be offline */ }
        });
        await Task.WhenAll(tasks);
    }
    // ── 处理 HTTP 的 /api/raft/vote（Candidate 请求，返回投票结果）──
    private readonly object _raftLock = new();   // Raft 状态统一锁（HTTP 线程 + 后台循环共用）

    public RaftVoteResponse HandleVote(string candidateId, long candidateTerm)
    {
        lock (_raftLock)
        {
            // ① 更高任期 → 先收养（Raft 规则：Adopt new term 之前，本任期投票记录立即作废）
            //    没有这条，各节点抱着 "_votedFor=自己" 死锁，永远选不出 Leader
            if (candidateTerm > _currentTerm)
            {
                _currentTerm = candidateTerm;
                _votedFor = null;
                if (_role != RaftRole.Follower) _role = RaftRole.Follower;
            }

            // ② 旧任期拒绝
            if (candidateTerm < _currentTerm)
                return new RaftVoteResponse(_currentTerm, _nodeId, false);

            // ③ 同一任期只投一票
            if (_votedFor is not null && _votedFor != candidateId)
                return new RaftVoteResponse(_currentTerm, _nodeId, false);

            // ④ 授权投票
            _votedFor = candidateId;
            _role = RaftRole.Follower;
            _lastHeartbeatUtc = DateTime.UtcNow;  // 重置选举计时器
            return new RaftVoteResponse(candidateTerm, _nodeId, true);
        }
    }

    // ── 处理 HTTP /api/raft/heartbeat（Leader 心跳）──
    public void ReceiveHeartbeat(string leaderId, long term)
    {
        lock (_raftLock)
        {
            if (term < _currentTerm) return;    // 忽略旧 term 心跳

            // 收到更高任期心跳 → 收养 term，清掉本任期投票记录
            if (term > _currentTerm)
            {
                _currentTerm = term;
                _votedFor = null;
            }

            if (_role != RaftRole.Follower) _role = RaftRole.Follower;
            _leaderId_ = leaderId;
            _lastHeartbeatUtc = DateTime.UtcNow;
        }
    }
}
