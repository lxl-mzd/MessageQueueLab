// ═══════════════════════════════════════════════════════════════
// Web/Endpoints.Queue.cs —— 队列收发接口注册
//
//   Quorum 写入确认（N/2+1 多数派）：
//     发消息 → Leader 本地落盘 + 并行复制给 Follower 群
//     → 只有收到的复制 ack ≥ ⌈N/2⌉（即续上 Leader 自身 ≥ N/2+1 票），
//     才回 201 给调用方；否则 **回 503**——调方知道没多数派确认，可自行重试
//
//   单机（replicator == null）模式：Quorum 门槛自动跳过（1票 = N/2+1 for N=1）
// ═══════════════════════════════════════════════════════════════
using MessageQueueLab.Core;
using MessageQueueLab.Models;

namespace MessageQueueLab.Web;

public static class QueueEndpoints
{
    public static void MapQueueApi(this WebApplication app, MessageQueueHub hub)
    {
        // Follower 拒绝业务写（复制流才进数据）
        app.MapPost("/api/q/{queue}/messages", async (string queue, MessageBody body) =>
        {
            if (!ClusterOptions.IsWriter)
                return Results.StatusCode(503);
            if (string.IsNullOrWhiteSpace(body.Content))
                return Results.BadRequest(new { error = "content 不能为空" });

            var q = hub.Get(queue);
            if (q is null) return Results.NotFound(new { error = "不存在队列", queue });

            // ══ Quorum 写入三阶段 ══
            // ① Prepare：只造消息和事件（内存，未落盘未入墙）
            var prepared = q.Prepare(body.Content, body.Key, body.TtlSeconds);

            // ② 广播复制并等 Quorum
            var repl = await hub.ReplicateQueueEventAsync(queue, prepared.Evt);

            var quorumOk = repl is null || repl.AckCount >= repl.Required / 2 + 1;
            if (!quorumOk)
            {
                // 未达多数派：本地也不 commit，无残留事件，客户端拿到 503 直接重试
                hub.PushEvent($"⚠️ Quorum 未达: ack={repl!.AckCount}/{repl.Required} → 503 回退给调用方（消息未提交）");
                return Results.StatusCode(503);
            }

            // ③ Quorum 达标 → 真正执行本地状态机（写 WAL + 入 ready 队列）
            q.CommitEvent(prepared.Evt);

            return Results.Created($"/api/q/{queue}/messages/{prepared.Msg.Id}", new
            {
                message = prepared.Msg,
                replication = repl
            });
        });

        app.MapPost("/api/q/{queue}/receive", (string queue, int? visibilitySeconds, int? waitMs) =>
        {
            var q = hub.Get(queue);
            if (q is null) return Results.NotFound(new { error = "不存在队列", queue });
            var msg = q.Receive(visibilitySeconds ?? 30, waitMs ?? 0);
            return msg is null ? Results.NoContent() : Results.Ok(msg);
        });

        app.MapPost("/api/q/{queue}/ack/{id}", async (string queue, string id) =>
        {
            if (!ClusterOptions.IsWriter) return Results.StatusCode(503);
            var q = hub.Get(queue);
            if (q is null) return Results.NotFound(new { error = "不存在队列", queue });

            // ══ Ack 三阶段 ══
            // ① Prepare：拆锁定区 + 生成 d 事件（未落盘未销账）
            var (ok, evt) = q.PrepareAck(id);
            if (!ok) return Results.NotFound(new { error = "不在锁定区", id });

            // ② 广播复制并等 Quorum
            var repl = await hub.ReplicateQueueEventAsync(queue, evt);

            var quorumOk = repl is null || repl.AckCount >= repl.Required / 2 + 1;
            if (!quorumOk)
            {
                hub.PushEvent($"⚠️ Ack 复制 Quorum 未达 —— ack={id[..12]}…（Quorum {repl!.AckCount}/{repl.Required}）→ 503 未销账");
                return Results.StatusCode(503);
            }

            // ③ Quorum 达标 → 本地销账落盘
            q.CommitAck(evt);

            return Results.Ok(new { acked = true, id });
        });

        app.MapPost("/api/q/{queue}/nack/{id}", async (string queue, string id) =>
        {
            if (!ClusterOptions.IsWriter) return Results.StatusCode(503);
            var q = hub.Get(queue);
            if (q is null) return Results.NotFound(new { error = "不存在队列", queue });

            var (msg, events) = q.Nack(id);
            if (msg is null) return Results.NotFound(new { error = "不在锁定区", id });

            var repl = (ReplicationResult?)null;
            foreach (var evt in events)
                repl = await hub.ReplicateQueueEventAsync(queue, evt);
            var quorumOk = repl is null || repl.AckCount >= repl.Required / 2 + 1;

            if (!quorumOk)
            {
                hub.PushEvent($"⚠️ Nack 复制 Quorum 未达 —— 回 503");
                return Results.StatusCode(503);
            }

            return Results.Ok(new { message = msg });   // RetryCount 已 +1
        });

        // ── 动态声明队列（持久化：目录落盘即真相，重启自动认领）──
        app.MapPost("/api/q/{queue}", (string queue) =>
        {
            if (string.IsNullOrWhiteSpace(queue) || !System.Text.RegularExpressions.Regex.IsMatch(queue, "^[A-Za-z0-9_\\-]+$"))
                return Results.BadRequest(new { error = "队列名仅限字母/数字/_/-" });
            if (!ClusterOptions.IsWriter)
                return Results.StatusCode(503);
            var q = hub.DeclareQueue(queue);
            return Results.Created($"/api/q/{queue}", new { declared = true, queue });
        });
    }
}
