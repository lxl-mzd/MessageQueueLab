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
        // v2：身份看 Raft 王位 —— 非当前王（含降级的老王）都接收复制流；
        //     只有现任王才拒收（防止自己发给自己的事件在环路上落地）
        app.MapPost("/api/cluster/replicate/{queue}", async (string queue, HttpRequest req) =>
        {
            if (ClusterOptions.Distributed && ClusterOptions.IsWriter)
                return Results.Conflict(new { error = "当前为 Raft Leader，不接收复制流", role = "Leader" });

            var evt = await System.Text.Json.JsonSerializer.DeserializeAsync<LogMessage>(
                req.Body, SefMqJson.CamelOpts);
            if (evt is null) return Results.BadRequest(new { error = "事件解析失败" });

            var q = hub.Get(queue);
            if (q is null)
            {
                // 复制流自愈：王位侧声明的新队列， follower 磁盘上还不知情 → 认领建队（幂等）
                q = hub.DeclareQueue(queue);
            }

            q.PushExternal(evt);               // 幂等：Seq ≤ 已回放位点自动跳过
            return Results.Ok(new { appliedSeq = evt.Seq });
        });

        app.MapGet("/api/cluster/status", () =>
        {
            var (ready, locked, pending, dead) = hub.Aggregate();
            var raft = ClusterOptions.Raft;
            return Results.Ok(new
            {
                // 身份看 Raft 现状（单机无 Raft 即 single），不再读任何静态 ROLE
                role = raft is null ? "single" : raft.Role.ToString(),
                node = ClusterOptions.NodeName,
                peers = raft is not null
                    ? raft.Members.Where(m => m.Node != ClusterOptions.NodeName).Select(m => m.Url).ToArray()
                    : ClusterOptions.Peers.Where(u => u.Length > 0 && u != ClusterOptions.SelfUrl).ToArray(),
                queues = new { ready, locked, pendingOnDisk = pending, deadLetters = dead },
            });
        });
    }
}
