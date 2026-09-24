// ═══════════════════════════════════════════════════════════════
// Web/Endpoints.DeadLetter.cs —— 死信审案接口注册（队列族，Leader 复制挂钩）
// ═══════════════════════════════════════════════════════════════
using MessageQueueLab.Core;

namespace MessageQueueLab.Web;

public static class DeadLetterEndpoints
{
    public static void MapDeadLetterApi(this WebApplication app, MessageQueueHub hub)
    {
        //查看指定队列的死信列表
        app.MapGet("/api/q/{queue}/dlq", (string queue) =>
        {
            var q = hub.Get(queue);
            if (q is null) return Results.NotFound(new { error = "不存在队列", queue });
            return Results.Ok(q.DeadLetters().Select(d => new { d.Id, d.Key, d.Content, d.RetryCount }));
        });
        //确认删除/清理指定死信
        app.MapPost("/api/q/{queue}/dlq/{id}/ack", async (string queue, string id) =>
        {
            if (!ClusterOptions.IsWriter)
                return Results.StatusCode(503);
            var q = hub.Get(queue);
            if (q is null) return Results.NotFound(new { error = "不存在队列", queue });

            var (ok, events) = q.DeleteDeadLetter(id);
            if (ok)
            {
                foreach (var e in events) await hub.ReplicateQueueEventAsync(queue, e);
                return Results.Ok(new { deadLetterAcked = true, id });
            }
            return Results.NotFound(new { error = "死信清单没有此id", id });
        });
        //复活指定死信（重新入队）
        app.MapPost("/api/q/{queue}/dlq/{id}/revive", async (string queue, string id) =>
        {
            if (!ClusterOptions.IsWriter)
                return Results.StatusCode(503);
            var q = hub.Get(queue);
            if (q is null) return Results.NotFound(new { error = "不存在队列", queue });

            var (revived, events) = q.ReviveDeadLetter(id);
            if (revived is null) return Results.NotFound(new { error = "死信清单没有此id", id });

            foreach (var e in events) await hub.ReplicateQueueEventAsync(queue, e);
            return Results.Ok(new { revived = true, id = revived.Id });
        });
    }
}
