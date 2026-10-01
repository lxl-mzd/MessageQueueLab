// ═══════════════════════════════════════════════════════════════
// Core/RaftNode.cs —— Raft 状态机 v2（Follower → Candidate → Leader 三态）
//                        + **动态成员表（join / leave）**
//
//   核心思想（教学简化版 Raft，仅实现 Leader 选举与成员变更协议）：
//     1. Follower 超过 electionTimeout 没收到 Leader 心跳 → 变 Candidate
//     2. Candidate 向所有其他成员请求投票（Term+1）
//     3. 收到多数派票 → 当选 Leader
//     4. 收到更高 Term 的 RPC → 自动降级为 Follower
//     5. **成员变更（阶段 C)**：
//        · 成员表持久化在 data/_cluster/members.json（每节点自己一份）
//        · 变更（±1）必须走 Quorum：Leader 造一条 m 记录 → 广播 → 过半同意 → 各自 Apply
//        · 投票/复制/心跳/写闸门全部动态读成员表（不再用死环境变量）
//
//   同步闸门（Syncing）：
//     · 新加入节点在「历史快照(catch-up)完成前」 → Syncing=true
//     · 期间它 **不发起选举、不服务写请求、不参与写 Quorum 票数**
//       （否则空账节点当选 = 无限脑裂候选）
//
//   下线（leave）：
//     · 只有 Leader 能处理 leave（非 Leader 一律 503）
//     · 离开节点若是 Leader 且想走 → commit 一条去掉自己的 m 事件（新表多数派 ack）
//        → 立刻退回 Follower + 停心跳，让剩余成员重新选出新王
// ═══════════════════════════════════════════════════════════════
using System.Net.Http.Json;

namespace MessageQueueLab.Core;

public enum RaftRole { Follower, Candidate, Leader }

public sealed class RaftNode : IDisposable
{
    public sealed record RaftVoteResponse(long Term, string VoterId, bool VoteGranted);

    private readonly string _nodeId;
    private readonly string _selfUrl;
    private readonly HttpClient _http = new();
    private readonly Random _rand = new();
    private readonly Action<string>? _emit;
    private readonly Persistence.MembershipStore _membership;

    // ── Raft 核心状态（_raftLock 保护） ──
    private long _currentTerm;
    private string? _votedFor;
        private string? _leaderId_;
    private RaftRole _role = RaftRole.Follower;
    private DateTime _lastHeartbeatUtc = DateTime.UtcNow;
    private int _electionTimeoutMs;
    private readonly object _raftLock = new();   // Raft 状态统一锁（HTTP 线程 + 后台循环共用）
    private volatile bool _stopping;
    private Task? _raftLoopTask;
    private readonly CancellationTokenSource _cts = new();

    // ── 同步 / 下线 闸门 ──
    private bool _synced = true;        // 新节点 catch-up 完成前为 false（不投票、不竞选、不接写）
    private bool _leaving = false;      // 本节点已宣布下线（Leave 已 Quorum）
    private int _membershipChangeInProgress = 0;   // 一次只允许 ±1 调表

    public RaftRole Role => _role;
    public long CurrentTerm => _currentTerm;
    public string NodeId => _nodeId;
    public string? CurrentLeaderId => _leaderId_;
    /// <summary>本节点认为当前哪个节点是王（currentLeaderId 别名，write-ready 的可读字段）</summary>
    public string? LeaderIdOfCluster() { lock (_raftLock) return _leaderId_ ?? _nodeId; }
    public bool Synced => _synced;

    /// <summary>成员表（含自己在内）的快照</summary>
    public IReadOnlyList<Persistence.RaftMember> Members { get { lock (_raftLock) return _membership.Table.ToList(); } }

    /// <summary>供写流量就绪探针：Leader 且已同步 → 200</summary>
    public bool WriteReady => _role == RaftRole.Leader && _synced && !_leaving;

    /// <summary>复制目标（给 ClusterReplicator 的动态表）：除自身外的**已同步**成员 URL</summary>
    public string[] PublishTargets()
    {
        lock (_raftLock)
            return _membership.Table
                .Where(m => m.Node != _nodeId && SyncFlag(m.Node))
                .Select(m => m.Url).Distinct().ToArray();
    }
    /// <summary>写 Quorum 所需票数：已同步成员（含 Leader）的多数派</summary>
    public int WriteMajority()
    {
        lock (_raftLock)
        {
            var n = _membership.Table.Count(m => m.Node == _nodeId || SyncFlag(m.Node));
            return n / 2 + 1;
        }
    }
    public int MemberCount { get { lock (_raftLock) return _membership.Table.Count; } }
    /// <summary>成员表的 seq 迷行号（snapshot 载体）</summary>
    public long MemberSeq() { lock (_raftLock) return _membership.Seq; }

    /// <summary>成员是否已同步（Syncing 闸门；bootstrap 成员默认 true）</summary>
    private bool SyncFlag(string node)
        => _syncFlags.TryGetValue(node, out var v) ? v : true;

    private static string ExtractHost(string url)
    {
        // "http://node-1:8080" → "node-1"；"http://mq-0.mq-headless.default.svc:8080" → "mq-0"
        var rest = url.Replace("http://", "").Replace("https://", "");
        var host = rest.Split('/')[0];
        if (host.Contains(':')) host = host.Split(':')[0];   // 剥掉端口（compose："node-1:8080"→"node-1"，与 MQ_NODE_NAME 对齐）
        return host.Split('.')[0];   // k8s 的 DNS "mq-0.mq-headless..." 截第一段 = "mq-0"
    }

    /// <summary>
    /// 构造：数据目录里若有旧的 members.json → 回放还原（崩溃/重启无损）；
    /// 否则用 bootstrapPeers 种表并落盘（写"第一任期成员表"）。
    /// bootstrapPeers 是 URL 列表（含自身也无所谓，内部按 &lt;node,url&gt; 对齐）。
    /// </summary>
    public RaftNode(string nodeId, string selfUrl, string[] bootstrapPeerUrls, string dataDirectory, Action<string>? emit = null)
    {
        _nodeId = nodeId;
        _selfUrl = selfUrl;
        _emit = emit;

        _membership = new Persistence.MembershipStore(dataDirectory, emit);

        var table = new List<Persistence.RaftMember>();
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        bool selfInPeers = bootstrapPeerUrls.Any(u => string.Equals(ExtractHost(u), _nodeId, StringComparison.OrdinalIgnoreCase));
        if (_membership.Table.Count > 0)
        {
            // ── 磁盘回放（kv 恢复路径）──
            table.AddRange(_membership.Table);
            _emit?.Invoke($"📇 [raft:{_nodeId}] 成员表回放（{_membership.Table.Count} 人）");
        }
        else if (bootstrapPeerUrls.Length == 0 || !selfInPeers)
        {
            // ── join 模式：MQ_PEERS 不含自己的名字（新节点不带成员表启动）→ 不自封为王，等 join 通告
            _synced = false;
            _emit?.Invoke($"🧪 [raft:{_nodeId}] join 模式：空成员表启动，等待 join 通告后 catch-up");
            _electionTimeoutMs = _rand.Next(2000, 4500);
            return;
        }
        else
        {
            // ── 出厂引导 ──
            foreach (var url in bootstrapPeerUrls.Where(u => !string.IsNullOrWhiteSpace(u)))
            {
                var host = ExtractHost(url);     // 以 URL 主机名当节点名（node-1/node-2… 或 k8s 的 mq-0）
                if (!seen.Add(host)) continue;
                table.Add(new Persistence.RaftMember(host, url));
            }
            // 自身必须入表（可能 bootstrap 里没写自己，也可能写法不一致）
            if (!seen.Contains(_nodeId)) table.Add(new Persistence.RaftMember(_nodeId, _selfUrl));
            _membership.Seed(table);
            _emit?.Invoke($"📇 [raft:{_nodeId}] 首次上牌：成员表 {table.Count} 人 → {string.Join(',', table.Select(t => t.Node))}");
        }
        _electionTimeoutMs = _rand.Next(2000, 4500);
        foreach (var m0 in _membership.Table) _syncFlags[m0.Node] = true;   // 出厂成员视为已同步
        if (_membership.Table.Count == 0) _synced = false;   // 空成员表（新节点 join 模式）→ Syncing 闸门
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

    public void Stop() { _stopping = true; _cts.Cancel(); }
    public void Dispose() { Stop(); _cts.Dispose(); _http.Dispose(); }

    private async Task RunTickAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested && !_stopping)
        {
            await Task.Delay(100, ct);
            bool synced; RaftRole role; double elapsedMs;
            lock (_raftLock)
            {
                role = _role; synced = _synced && !_leaving;
                elapsedMs = (DateTime.UtcNow - _lastHeartbeatUtc).TotalMilliseconds;
            }
            if (!synced) continue;                        // 追平前：不心跳不竞选

            switch (role)
            {
                case RaftRole.Leader:
                    await SendHeartbeatsAsync(ct);
                    break;
                default:   // Follower / Candidate
                    if (elapsedMs >= _electionTimeoutMs)
                        await StartElectionAsync(ct);
                    break;
            }
        }
    }

    // ── Candidate：发选举（Term+1，自投 + 请其他成员投票）──
    private async Task StartElectionAsync(CancellationToken ct)
    {
        string[] peers;
        int votesNeeded;
        lock (_raftLock)
        {
            if (_leaving) return;                                  // 已宣布离场的节点不再拉票
            _currentTerm++;
            _votedFor = _nodeId;
            _role = RaftRole.Candidate;
            _leaderId_ = null;
            peers = _membership.Table.Where(m => m.Node != _nodeId).Select(m => m.Url).ToArray();
            votesNeeded = _membership.Table.Count / 2 + 1;
        }
        _emit?.Invoke($"🔔 [raft:{_nodeId}] term={_currentTerm} Candidate → 向 {peers.Length} 节点请求投票");

        var tasks = peers.Select(async peer =>
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
        int grantedCount = 1;
        foreach (var v in votes) if (v) grantedCount++;

        bool won;
        lock (_raftLock) won = grantedCount >= votesNeeded;
        if (won)
        {
            lock (_raftLock)
            {
                _role = RaftRole.Leader;
                _leaderId_ = _nodeId;
            }
            _emit?.Invoke($"👑 [raft:{_nodeId}] 当选 Leader (term={_currentTerm}, votes={grantedCount}/{peers.Length + 1})");
            await SendHeartbeatsAsync(ct);
        }
        else
        {
            lock (_raftLock)
            {
                _role = RaftRole.Follower;
                _electionTimeoutMs = _rand.Next(2000, 4500);
                _lastHeartbeatUtc = DateTime.UtcNow;
            }
        }
    }

    private async Task SendHeartbeatsAsync(CancellationToken ct)
    {
        string[] peers;
        lock (_raftLock) peers = _membership.Table.Where(m => m.Node != _nodeId).Select(m => m.Url).ToArray();
        var hb = new { term = _currentTerm, leaderId = _nodeId };
        var tasks = peers.Select(async peer =>
        {
            try { await _http.PostAsJsonAsync($"{peer}/api/raft/heartbeat", hb); }
            catch { /* silently fail — follower might be offline */ }
        });
        await Task.WhenAll(tasks);
    }

    // ── 处理 HTTP 的 /api/raft/vote ──
    public RaftVoteResponse HandleVote(string candidateId, long candidateTerm)
    {
        lock (_raftLock)
        {
            if (_leaving || !_synced)
            {
                // 同步中 / 已离场节点：不参与投票（它是"账本空洞"，投出去没有保障）
                _emit?.Invoke($"🧊 [raft:{_nodeId}] 拒票（syncing={_synced == false}, leaving={_leaving}）candidate={candidateId}");
                return new RaftVoteResponse(_currentTerm, _nodeId, false);
            }

            if (candidateTerm > _currentTerm)
            {
                _currentTerm = candidateTerm;
                _votedFor = null;
                if (_role != RaftRole.Follower) _role = RaftRole.Follower;
            }
            if (candidateTerm < _currentTerm)
                return new RaftVoteResponse(_currentTerm, _nodeId, false);
            if (_votedFor is not null && _votedFor != candidateId)
                return new RaftVoteResponse(_currentTerm, _nodeId, false);

            _votedFor = candidateId;
            _role = RaftRole.Follower;
            _lastHeartbeatUtc = DateTime.UtcNow;
            return new RaftVoteResponse(candidateTerm, _nodeId, true);
        }
    }

    // ── 处理 HTTP /api/raft/heartbeat ──
    public void ReceiveHeartbeat(string leaderId, long term)
    {
        lock (_raftLock)
        {
            if (_leaving) return;
            if (term < _currentTerm) return;
            if (term > _currentTerm) { _currentTerm = term; _votedFor = null; }
            if (_role != RaftRole.Follower) _role = RaftRole.Follower;
            _leaderId_ = leaderId;
            _lastHeartbeatUtc = DateTime.UtcNow;
        }
    }

    // ════════════ 动态成员变更（Join / Leave / Apply）════════════

    /// <summary>join（Leader 专用）：把 newNode 提交为成员 + 走 Quorum（除 Leader 外的旧成员 ack）。非 Leader → 503。</summary>
    public async Task<(bool Ok, string Error)> JoinMemberAsync(string newNode, string newUrl)
    {
        List<Persistence.RaftMember> oldTable, newTable;
        lock (_raftLock)
        {
            if (_role != RaftRole.Leader) return (false, "仅 Leader 节点处理 join");
            if (_leaving) return (false, "本节点已宣布离场");
            if (System.Threading.Interlocked.Exchange(ref _membershipChangeInProgress, 1) == 1)
                return (false, "已有成员变更进行中（±1）");

            oldTable = _membership.Table.ToList();
            if (oldTable.Count >= 9) return (false, "成员数已到上限（教学版 ≤9）");
            if (_membership.Contains(newNode))
            {
                // 幂等语义：如果同名且 URL 一致（catch-up 卡住重放 join 的情况）→ 直接当作成功
                string existingUrl = "";
                _membership.TryGetUrl(newNode, out existingUrl);
                if (string.Equals(existingUrl, newUrl, StringComparison.OrdinalIgnoreCase))
                    return (true, "");
                return (false, $"节点名 {newNode} 已被占用（URL 不同请求：{existingUrl}）");
            }
            if (_membership.Table.Any(m => string.Equals(m.Url, newUrl, StringComparison.OrdinalIgnoreCase)))
                return (false, $"URL {newUrl} 已被其他节点使用");
            if (string.IsNullOrWhiteSpace(newNode) || string.IsNullOrWhiteSpace(newUrl))
                return (false, "node / url 不能为空");

            newTable = oldTable.Append(new Persistence.RaftMember(newNode, newUrl)).ToList();
        }

        try
        {
            var seq = _membership.NextSeq();
            var (ok, detail) = await CommitMembershipEventAsync(seq, oldTable, newTable,
                excludeNode: null, extraPushTargets: new[] { newUrl });
            if (!ok) return (false, detail);

            // Leader 视角：新节点进入"Syncing"（等待它 catch-up 完来 sync-ack）
            lock (_raftLock)
            {
                _syncFlags[newNode] = false;
                _emit?.Invoke($"🧪 [raft:{_nodeId}] join 已提交：{newNode}（seq={seq}）→ 等待 catch-up 后 sync-ack");
            }
            return (true, "");
        }
        finally
        {
            System.Threading.Interlocked.Exchange(ref _membershipChangeInProgress, 0);
        }
    }

    /// <summary>leave（Leader 专用）：把某节点从成员表移除并 Quorum 提交。被移除节点无需 ack。</summary>
    public async Task<(bool Ok, string Error)> LeaveMemberAsync(string leavingNode)
    {
        List<Persistence.RaftMember> oldTable, newTable;
        bool selfLeaving;
        lock (_raftLock)
        {
            if (_role != RaftRole.Leader) return (false, "仅 Leader 节点处理 leave");
            if (_leaving) return (false, "本节点已宣布离场");
            if (System.Threading.Interlocked.Exchange(ref _membershipChangeInProgress, 1) == 1)
                return (false, "已有成员变更进行中（±1）");
            oldTable = _membership.Table.ToList();
            if (!_membership.Contains(leavingNode))
                return (false, $"节点 {leavingNode} 不在成员表里");
            if (oldTable.Count <= 1)
                return (false, "桌上只剩一个人，不能自己把自己开除");
            newTable = oldTable.Where(m => m.Node != leavingNode).ToList();
            selfLeaving = string.Equals(leavingNode, _nodeId, StringComparison.OrdinalIgnoreCase);
        }

        try
        {
            var seq = _membership.NextSeq();
            var (ok, detail) = await CommitMembershipEventAsync(seq, oldTable, newTable,
                excludeNode: leavingNode, extraPushTargets: Array.Empty<string>());
            if (!ok) return (false, detail);

            // 被移除的是自己（Leader 自我离场）？→ 退回 Follower、不再心跳，让剩余成员选新王
            if (selfLeaving)
            {
                lock (_raftLock)
                {
                    _role = RaftRole.Follower;
                    _leaderId_ = null;
                    _leaving = true;
                    _lastHeartbeatUtc = DateTime.UtcNow;   // 不触发竞选
                }
                _emit?.Invoke($"🚪 [raft:{_nodeId}] 优雅下线：自我移除已 Quorum 通过，让出 Leader 给剩余成员重新选举");
            }
            return (true, "");
        }
        finally
        {
            System.Threading.Interlocked.Exchange(ref _membershipChangeInProgress, 0);
        }
    }

    /// <summary>Follower 收到 Leader 推来的 m 事件（Quorum 已在其侧通过）→ 本地 apply + 落盘</summary>
    public void ApplyMembership(long seq, List<Persistence.RaftMember> table)
    {
        lock (_raftLock)
        {
            _membership.Apply(seq, table);
            if (table.Any(m => m.Node == _nodeId)) _synced = _synced;   // 老成员：本来就同步
            // 新节点（无 WAL 历史）的 synced 由 catch-up 完成 → MarkSynced 转 true
        }
    }

    public void MarkSynced(bool value)
    {
        lock (_raftLock)
        {
            if (_synced != value)
            {
                _synced = value;
                _lastHeartbeatUtc = DateTime.UtcNow;
            }
        }
        _emit?.Invoke(value
            ? $"🧪 [raft:{_nodeId}] catch-up 完成 → 正式成为集群成员（可投票/可竞选）"
            : $"🧪 [raft:{_nodeId}] 进入 Syncing 状态（历史快照追赶中）");
    }

    /// <summary>Leader 视角：标记某个成员的同步状态（由该成员的 sync-ack RPC 触发）。</summary>
    public void SetMemberSynced(string node, bool value)
    {
        lock (_raftLock)
        {
            var old = _syncFlags.TryGetValue(node, out var v) ? v : true;
            if (old != value)
            {
                _syncFlags[node] = value;
                _emit?.Invoke(value
                    ? $"🧪 [raft:{_nodeId}] {node} catch-up 完成 → 加入 Quorum 票池"
                    : $"🧪 [raft:{_nodeId}] {node} 标记为 Syncing（退出 Quorum）");
            }
        }
    }

    /// <summary>当前处于 Syncing 状态的成员列表（Leader 视角，供 Cor 的 Delta Buffer 使用）。</summary>
    public List<(string Node, string Url)> GetSyncingMembers()
    {
        lock (_raftLock)
            return _membership.Table
                .Where(m => m.Node != _nodeId && !_syncFlags.GetValueOrDefault(m.Node, true))
                .Select(m => (m.Node, m.Url))
                .ToList();
    }

    /// <summary>本节点成员表是否为空（判"新节点 join 模式"的唯一证据）</summary>
    public bool NeedsJoinAtBoot { get { lock (_raftLock) return _membership.Table.Count == 0; } }
    /// <summary>
    /// Join-on-boot 自助流程（新空节点）：
    ///   ① 向已知 Leader 发 join（带自身 node/url）→ Leader 走 Quorum 提交成员表
    ///   ② 从 Leader 拉 /api/raft/snapshot（活账快照）→ hub.ApplySnapshot 写回自己磁盘
    ///   ③ 自身 MarkSynced(true) → 转正
    /// 全程异步；失败自动退避重试（Leader 换人/网络抖动等）。调用方 fire-and-forget。
    /// </summary>
    public async Task StartJoinAndCatchUpAsync(string leaderUrl, MessageQueueHub hub)
    {
        var attempts = 0;
        while (!_stopping && !_synced && attempts < 200)
        {
            attempts++;
            try
            {
                // ① join（Leader 侧会走 Quorum；node/url 的成员表同时广播到自己这份节点）
                //   注意：NeedsJoinAtBoot 只在成员表为空时才请求 join；
                //     若已有成员表但 synced=false（上次 catch-up 未赖完），直接走 catch-up 补发
                if (NeedsJoinAtBoot)
                {
                    var resp = await _http.PostAsJsonAsync($"{leaderUrl}/api/raft/join",
                        new { node = _nodeId, url = _selfUrl });
                    if (!resp.IsSuccessStatusCode)
                    {
                        await Task.Delay(3000);
                        continue;
                    }
                    // join 已提交：若本地表仍空（apply-membership 广播在途中丢失），
                    // 主动拉一次成员表本地 apply。只在表空时做、自增 seq 从 1 起，
                    // 不会挡掉后续真广播（真广播 seq ≥ 2）。
                    if (MemberCount == 0)
                    {
                        try
                        {
                            var memBody = await _http.GetStringAsync($"{leaderUrl}/api/raft/members");
                            using var memDoc = System.Text.Json.JsonDocument.Parse(memBody);
                            if (memDoc.RootElement.TryGetProperty("table", out var tabArr))
                            {
                                var table = new List<Persistence.RaftMember>();
                                foreach (var m in tabArr.EnumerateArray())
                                {
                                    var mn = m.TryGetProperty("node", out var nn) ? nn.GetString() ?? "" : "";
                                    var mu = m.TryGetProperty("url", out var uu) ? uu.GetString() ?? "" : "";
                                    if (mn.Length > 0 && mu.Length > 0) table.Add(new Persistence.RaftMember(mn, mu));
                                }
                                if (table.Count > 0) ApplyMembership(MemberSeq() + 1, table);
                            }
                        }
                        catch { /* 拉表失败就靠广播那条路，下轮重试 */ }
                    }
                }

                // ② 拉 Leader 的快照（可能要等 Leader 自己收敛好）
                var snapResp = await _http.GetAsync($"{leaderUrl}/api/raft/snapshot");
                if (!snapResp.IsSuccessStatusCode) { await Task.Delay(3000); continue; }
                var snapBody = await snapResp.Content.ReadAsStringAsync();
                // Leader 返回的 JSON 键是 camelCase（"queues"），反序列化需要大小写不敏感
                var snap = System.Text.Json.JsonSerializer.Deserialize<MessageQueueLab.Core.MessageQueueHub.ClusterSnapshotBody>(snapBody,
                    new System.Text.Json.JsonSerializerOptions { PropertyNameCaseInsensitive = true });
                if (snap is null || snap.Queues is null) { await Task.Delay(3000); continue; }

                // ③ 应用快照到本节点各队列（hub 内部逐个 QueueCore restore）
                hub.ApplySnapshot(snap);

                // ④ 自身追平转正（在 sync-ack 之前——确保 Leader 全量复制到来时本节点已 ready）
                MarkSynced(true);

                // ⑤ 通知 Leader 追平完成，随包携带各账本水位（空账本记 -1，保证 delta 从 Seq 0 开始读）：
                //    Leader 侧 seed 进 per-follower 水位表 → 快照点到 sync-ack 窗口的漏事件走 delta 回放补齐
                var wms = new Dictionary<string, long>();
                foreach (var qs in snap.Queues)
                    wms[qs.Queue] = qs.Events.Count == 0 ? -1 : qs.UptoSeq;
                await _http.PostAsJsonAsync($"{leaderUrl}/api/raft/sync-ack",
                    new { node = _nodeId, synced = true, watermarks = wms });

                _emit?.Invoke($"🎉 [raft:{_nodeId}] join + catch-up 完成（成员表 seq={snap.MemberSeq}）—— 正式入列");
                return;
            }
            catch (Exception ex)
            {
                _emit?.Invoke($"‼️ [raft:{_nodeId}] join-on-boot 异常（重试中）：{ex.Message}\n{ex.StackTrace}");
                await Task.Delay(4000);
            }
        }
        _emit?.Invoke($"🚧 [raft:{_nodeId}] join-on-boot 放弃（尝试 {attempts} 次仍未成功）——手动介入处理");
    }

    // ── m 事件提交引擎：Leader 用它把成员变更广播到其他成员 ──
    private Dictionary<string, bool> _syncFlags = new();   // node → 是否 catch-up 完成（Leaders 视角）

    private async Task<(bool Ok, string Detail)> CommitMembershipEventAsync(
        long seq, List<Persistence.RaftMember> oldTable, List<Persistence.RaftMember> newTable,
        string? excludeNode, string[]? extraPushTargets)
    {
        var targets = oldTable
            .Where(m => m.Node != _nodeId && (excludeNode is null || m.Node != excludeNode))
            .Select(m => m.Url);
        if (extraPushTargets is not null) targets = targets.Concat(extraPushTargets);
        targets = targets.Distinct().ToList();

        _emit?.Invoke($"📜 [raft:{_nodeId}] 提交成员变更 seq={seq}（旧 {oldTable.Count} 人 → 新 {newTable.Count} 人）→ 广播到：{string.Join(',', targets)}");

        var tasks = targets.Select(async url =>
        {
            try
            {
                var body = new { seq, table = newTable.Select(t => new { t.Node, t.Url }) };
                using var resp = await _http.PostAsJsonAsync($"{url}/api/raft/apply-membership", body);
                return resp.IsSuccessStatusCode;
            }
            catch { return false; }
        }).ToList();

        var acks = await Task.WhenAll(tasks);
        var required = oldTable.Count / 2 + 1;               // m 事件的 Quorum = 旧成员表的多数派（含 Leader 自身 1 票）
        var total = 1 + acks.Count(a => a);                  // Leader 自己算一票
        var ok2 = total >= required;
        if (ok2)
        {
            _membership.Apply(seq, newTable);
        }
        return (ok2, ok2 ? "" : $"Quorum 未达（{total}/{required}）：{string.Join(',', targets)}");
    }

    /// <summary>
    /// SIGTERM / preStop 时的优雅下线：
    ///   · 我是 Leader → LeaveMemberAsync(self)：m 事件去掉自己 → 剩余成员重选王
    ///   · 我是 Follower → 向已知 Leader 发 /api/raft/leave {node: 自己}
    ///   · 失败则静默退出（WAL 在 PVC 里，重启时回放恢复，由 ops 手动清僵尸成员）
    /// </summary>
    public async Task GracefulOfflineAsync()
    {
        lock (_raftLock)
        {
            if (_leaving || !_synced) return;   // 已经下线中 / 还不是正式成员（join 模式未追平）→ 静默退出
        }
        // 不预占 _leaving —— 让 LeaveMemberAsync 自己管理 _leaving，否则它会被自己给卡死

        try
        {
            if (_role == RaftRole.Leader)
            {
                await LeaveMemberAsync(_nodeId);

                // ── 轮询确认新王已选出（代替瞎等 3 秒）──
                //  新成员表（去掉自己后）里剩余的成员 URL 列表
                var remainingUrls = _membership.Table
                    .Where(m => m.Node != _nodeId)
                    .Select(m => m.Url)
                    .ToList();

                var confirmed = false;
                for (var poll = 0; poll < 15 && !confirmed; poll++)   // 最多 7.5 秒
                {
                    await Task.Delay(500);
                    foreach (var url in remainingUrls)
                    {
                        try
                        {
                            var body = await _http.GetStringAsync($"{url}/api/raft/status");
                            if (body.Contains("\"role\":\"Leader\""))
                            {
                                _emit?.Invoke($"🚪 [raft:{_nodeId}] 已确认新 Leader 在 {ExtractHost(url)} → 安全退出");
                                confirmed = true;
                                break;
                            }
                        }
                        catch { /* 该成员尚未就绪，轮询下一个 */ }
                    }
                }
                if (!confirmed)
                    _emit?.Invoke($"⚠️ [raft:{_nodeId}] 轮询 7.5s 内未确认新 Leader，按 Raft 协议自主收敛后仍然退出");
            }
            else
            {
                // Follower：向当前 Leader 发 leave 请求（Leader 会替我们走 Quorum）
                string? leaderUrl = null;
                lock (_raftLock) leaderUrl = _membership.Table.FirstOrDefault(m => m.Node == _leaderId_)?.Url;
                if (!string.IsNullOrEmpty(leaderUrl))
                {
                    using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
                    using var resp = await _http.PostAsJsonAsync(
                        $"{leaderUrl}/api/raft/leave", new { node = _nodeId },
                        cancellationToken: cts.Token);
                    _emit?.Invoke(resp.IsSuccessStatusCode
                        ? $"🚪 [raft:{_nodeId}] Follower 优雅下线：Leader 已处理 leave"
                        : $"🚪 [raft:{_nodeId}] Follower leave 失败 ({resp.StatusCode})，静默退出");
                }
                else
                    _emit?.Invoke($"🚪 [raft:{_nodeId}] 下线：无已知 Leader，静默退出");
            }
        }
        catch (Exception ex)
        {
            _emit?.Invoke($"🚪 [raft:{_nodeId}] 优雅下线异常（静默跳过）：{ex.Message}");
        }
    }
}
