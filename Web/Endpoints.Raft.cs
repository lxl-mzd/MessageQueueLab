// ═══════════════════════════════════════════════════════════════
// Web/Endpoints.Raft.cs —— Raft RPC 端点（投票 / 心跳 / 状态 / 动态成员）
//
//   POST /api/raft/vote            — Candidate 请求投票（term+candidateId）
//   POST /api/raft/heartbeat       — Leader 心跳广播（term+leaderId）
//   GET  /api/raft/status          — 节点 Report 请求
//
//   动态成员（阶段 C）：
//   POST /api/raft/join            — {node,url}（Leader 专用；Quorum 流转后全体 apply）
//   POST /api/raft/leave           — {node}   （Leader 专用；被移除节点无需 ack）
//   POST /api/raft/apply-membership— {seq,table} Leader 广播的成员表变更滚动应用
//   POST /api/raft/sync-ack        — {synced} 新追加节点 catch-up 完成后上报
//   GET  /api/raft/members         — 成员表视图（诊断）
//   GET  /api/raft/write-ready     — k8s mq-write Service 的 readinessProbe
//                                   （服务"Leader 是否健康可写"，非 Leader 503 → 流量自动跟王）
// ═══════════════════════════════════════════════════════════════
using System.Text.Json;
using MessageQueueLab.Core;
using MessageQueueLab.Persistence;

namespace MessageQueueLab.Web;

public static class RaftEndpoints
{
    /// <summary>RaftNode 后台线程：每 100ms 检查一次选举/心跳 - 集群 API 绑定</summary>
    public static void MapRaftApi(this WebApplication app, RaftNode? raft, MessageQueueHub hub)
    {
        if (raft is null) return;

        // ── 收到 Candidate 请求投票 ──
        app.MapPost("/api/raft/vote", (System.Text.Json.JsonElement body) =>
        {
            long term = body.TryGetProperty("term", out var t) ? t.GetInt64() : 0;
            string candidate = body.TryGetProperty("candidateId", out var c) ? c.GetString()! : "";
            if (string.IsNullOrEmpty(candidate)) return Results.BadRequest(new { error = "candidateId required" });
            var resp = raft.HandleVote(candidate, term);
            return Results.Ok(resp);
        });
        // ── 收到 Leader 心跳（leader 广播 AppendEntries） ──
        app.MapPost("/api/raft/heartbeat", (System.Text.Json.JsonElement body) =>
        {
            long term = body.TryGetProperty("term", out var t) ? t.GetInt64() : 0;
            string leaderId = body.TryGetProperty("leaderId", out var l) ? l.GetString()! : "";
            raft.ReceiveHeartbeat(leaderId, term);
            return Results.Ok(new { acked = true, term = raft.CurrentTerm });
        });

        // ── Raft 身份状态查询 ──
        app.MapGet("/api/raft/status", () =>
            Results.Ok(new
            {
                node = raft.NodeId,
                role = raft.Role.ToString(),
                term = raft.CurrentTerm,
                leaderId = raft.CurrentLeaderId,
                synced = raft.Synced,
                memberCount = raft.MemberCount,
                members = raft.Members.Select(m => new { m.Node, m.Url }).ToList(),
            }));

        // ═══ 动态成员变更 ═══

        // join：Leader 专用。集群新增一个节点（走 Quorum ±1）。非 Leader 一律 503。
        app.MapPost("/api/raft/join", async (System.Text.Json.JsonElement body) =>
        {
            try
            {
                string node = body.TryGetProperty("node", out var n) ? n.GetString()! : "";
                string url = body.TryGetProperty("url", out var u) ? u.GetString()! : "";
                if (string.IsNullOrWhiteSpace(node) || string.IsNullOrWhiteSpace(url))
                    return Results.BadRequest(new { error = "node / url required" });
                var (ok, err) = await raft.JoinMemberAsync(node, url);
                if (!ok)
                    return Results.Conflict(new { error = err, joined = false });
                return Results.Ok(new { joined = true, node, url, memberCount = raft.MemberCount });
            }
            catch (Exception ex)
            {
                return Results.BadRequest(new { error = ex.Message });
            }
        });

        // leave：Leader 专用。将某节点移出集群（Quorum 流转；被移除节点无需 ack）。
        app.MapPost("/api/raft/leave", async (System.Text.Json.JsonElement body) =>
        {
            string node = body.TryGetProperty("node", out var n) ? n.GetString()! : "";
            if (string.IsNullOrWhiteSpace(node))
                return Results.BadRequest(new { error = "node required" });
            var (ok, err) = await raft.LeaveMemberAsync(node);
            if (!ok)
                return Results.Conflict(new { error = err, left = false });
            return Results.Ok(new { left = true, node, memberCount = raft.MemberCount });
        });

        // apply-membership：Leader 广播成员变更 → 各节点本地 Apply + 落盘
        // （语义：只有 Leader 侧 Quorum 达成才广播给本节点，所以本地只管接受）
        app.MapPost("/api/raft/apply-membership", (System.Text.Json.JsonElement body) =>
        {
            long seq = body.TryGetProperty("seq", out var s) ? s.GetInt64() : 0;
            var tableJson = body.TryGetProperty("table", out var tab) ? tab : default;
            if (tableJson.ValueKind != JsonValueKind.Array || seq <= 0)
                return Results.BadRequest(new { error = "seq / table required" });

            var table = new List<RaftMember>();
            foreach (var m in tableJson.EnumerateArray())
            {
                var node = m.TryGetProperty("node", out var n) ? n.GetString()! : "";
                var url = m.TryGetProperty("url", out var u) ? u.GetString()! : "";
                if (!string.IsNullOrWhiteSpace(node) && !string.IsNullOrWhiteSpace(url))
                    table.Add(new RaftMember(node, url));
            }
            if (table.Count == 0) return Results.BadRequest(new { error = "table empty" });
            raft.ApplyMembership(seq, table);
            return Results.Ok(new { applied = true, seq });
        });

        // sync-ack：新挂成员在 catch-up 完成后上报自身状态
        //   v5：**watermark 版** —— 新节点随包携带"自己已追平到各队列 Seq 值"，Leader seed 进 per follower 水位表；
        //     然后把 sync-ack 时间点之后漏掉的事件（快照点到 sync-ack 的窗口）从 WAL 读出补发给新节点
        app.MapPost("/api/raft/sync-ack", async (System.Text.Json.JsonElement body) =>
        {
            var nodeName = body.TryGetProperty("node", out var n) ? n.GetString() ?? "" : raft.NodeId;
            bool synced = body.TryGetProperty("synced", out var s) && s.GetBoolean();

            // ① 记录成员同步状态（Leader 视角 per-member 追踪，参与 Quorum 票池）
            raft.SetMemberSynced(nodeName, synced);

            // ② seed per-follower 水位（快照追到哪了 = 每队列 uptoSeq）
            if (body.TryGetProperty("watermarks", out var wms) && wms.ValueKind == JsonValueKind.Object)
            {
                var targetMember = raft.Members.FirstOrDefault(m => m.Node == nodeName);
                var targetUrl = targetMember?.Url;
                if (targetUrl is not null)
                {
                    foreach (var q in wms.EnumerateObject())
                    {
                        if (q.Value.TryGetInt64(out var seq))
                            hub.Replicator?.SeedWatermark(targetUrl, q.Name, seq);
                    }
                    // ③ 从水位处把"快照点 → 当前 Leader 水位"的漏事件补发（异步不阻塞 ack 返回）
                    //    先标 lagging：回放中途失败则巡查员下轮重试，直到追平才解除
                    hub.Replicator?.MarkLagging(targetUrl, true);
                    _ = hub.ReplayDeltaToFollowerAsync(targetUrl);
                }
            }
            return Results.Ok(new { synced, node = nodeName });
        });

        // write-ready：k8s write Service 的 readinessProbe（非 Leader 503 → Service 端点自动只指王）
        app.MapGet("/api/raft/write-ready", () =>
            raft.WriteReady
                ? Results.Ok(new { ready = true, leaderOf = raft.LeaderIdOfCluster() })
                : Results.StatusCode(503));

        // members：诊断视图
        app.MapGet("/api/raft/members", () => Results.Ok(new
        {
            node = raft.NodeId,
            synced = raft.Synced,
            memberCount = raft.MemberCount,
            table = raft.Members.Select(m => new { m.Node, m.Url }).ToList(),
        }));

        // ═══ catch-up 快照（新节点 join 后从 Leader 拉全量活账） ═══
        app.MapGet("/api/raft/snapshot", () =>
        {
            if (raft.Role != RaftRole.Leader)
                return Results.Conflict(new { error = "仅 Leader 节点提供快照" });
            return Results.Json(hub.CaptureSnapshot());
        });

        // ═══ 运维代查（看板经王转发）：POST /api/admin/forward {node, method, path, body?} ═══
        //   看板只需要一个可达地址（5091 经 mq-write 到王），王用集群内网替浏览器问其他成员。
        //   Raft 内部协议（投票/心跳/复制流/成员应用）禁止经此透传，防环路与误操作。
        app.MapPost("/api/admin/forward", async (System.Text.Json.JsonElement req) =>
        {
            var node = req.TryGetProperty("node", out var n) ? n.GetString() ?? "" : "";
            var method = req.TryGetProperty("method", out var m) ? (m.GetString() ?? "GET").ToUpperInvariant() : "GET";
            var path = req.TryGetProperty("path", out var p) ? p.GetString() ?? "" : "";
            string? body = req.TryGetProperty("body", out var b) && b.ValueKind == JsonValueKind.String
                ? b.GetString() : null;
            if (!path.StartsWith("/")) return Results.BadRequest(new { error = "path 必须以 / 开头" });
            string[] blocked = ["/api/raft/vote", "/api/raft/heartbeat", "/api/cluster/replicate",
                                "/api/raft/apply-membership", "/api/raft/join", "/api/raft/leave"];
            if (blocked.Any(x => path.StartsWith(x, StringComparison.OrdinalIgnoreCase)))
                return Results.StatusCode(403);
            if (method != "GET" && method != "POST") return Results.BadRequest(new { error = "仅支持 GET/POST" });

            string? url = node.StartsWith("http", StringComparison.OrdinalIgnoreCase) ? node
                : raft.Members.FirstOrDefault(x => x.Node == node)?.Url;
            if (string.IsNullOrWhiteSpace(url)) return Results.NotFound(new { error = "未知成员", node });

            try
            {
                using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
                using var msg = new HttpRequestMessage(
                    method == "POST" ? HttpMethod.Post : HttpMethod.Get, url + path);
                if (method == "POST")
                    msg.Content = new StringContent(body ?? "",
                        System.Text.Encoding.UTF8, "application/json");
                using var resp = await ForwardHttp.SendAsync(msg, cts.Token);
                return Results.Ok(new
                {
                    code = (int)resp.StatusCode,
                    body = await resp.Content.ReadAsStringAsync(cts.Token),
                });
            }
            catch (Exception ex) { return Results.Ok(new { code = 0, body = "", error = ex.Message }); }
        });
    }

    private static readonly HttpClient ForwardHttp = new();
}
