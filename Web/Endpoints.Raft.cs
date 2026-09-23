// ═══════════════════════════════════════════════════════════════
// Web/Endpoints.Raft.cs —— Raft RPC 端点（投票 / 心跳 / 状态）
//
//   POST /api/raft/vote      — Candidate 请求投票（term+candidateId）
//   POST /api/raft/heartbeat — Leader 心跳广播（term+leaderId）
//   GET  /api/raft/status    — 节点 Report 请求
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
                peers = raft.Peers,
            }));
    }
}
