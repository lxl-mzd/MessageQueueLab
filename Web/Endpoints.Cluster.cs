// ═══════════════════════════════════════════════════════════════
// Web/Endpoints.Cluster.cs —— 集群层接口
//   POST /api/cluster/replicate/{queue}   Follower 收事件（LogMessage JSON 全体）
//   GET  /api/cluster/status              集群体检：身份/覆盖段/计数
// ═══════════════════════════════════════════════════════════════
using MessageQueueLab.Core;
using MessageQueueLab.Persistence;
using MessageQueueLab.Sdk;

namespace MessageQueueLab.Web;

public static class ClusterEndpoints
{
    public static void MapClusterApi(this WebApplication app, MessageQueueHub hub)
    {
        // Follower 收 Leader 复制的事件（LogMessage 全体）
        app.MapPost("/api/cluster/replicate/{queue}", async (string queue, HttpRequest req) =>
        {
            if (!ClusterOptions.IsFollower)
                return Results.Conflict(new { error = "仅 follower 节点接收复制", role = ClusterOptions.Role });

            var evt = await System.Text.Json.JsonSerializer.DeserializeAsync<LogMessage>(
                req.Body, SefMqJson.CamelOpts);
            if (evt is null) return Results.BadRequest(new { error = "事件解析失败" });

            var q = hub.Get(queue);
            if (q is null) return Results.NotFound(new { error = "不存在队列", queue });

            q.PushExternal(evt);               // 幂等：Seq ≤ 已回放位点自动跳过
            return Results.Ok(new { appliedSeq = evt.Seq });
        });

        app.MapGet("/api/cluster/status", () =>
        {
            var (ready, locked, pending, dead) = hub.Aggregate();
            return Results.Ok(new
            {
                role = ClusterOptions.Role,
                node = ClusterOptions.NodeName,
                peers = ClusterOptions.Distributed
                    ? (ClusterOptions.IsLeader ? ClusterOptions.Followers : new[] { ClusterOptions.LeaderUrl })
                    : Array.Empty<string>(),
                queues = new { ready, locked, pendingOnDisk = pending, deadLetters = dead },
            });
        });
    }
}
