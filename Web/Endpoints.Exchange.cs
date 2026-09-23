// ═══════════════════════════════════════════════════════════════
// Web/Endpoints.Exchange.cs —— 交换机投递接口注册
//   POST /api/ex/orders/publish?routingKey=vip
//   POST /api/ex/broadcast/publish
// ═══════════════════════════════════════════════════════════════
using MessageQueueLab.Core;
using MessageQueueLab.Models;

namespace MessageQueueLab.Web;

public static class ExchangeEndpoints
{
    public static void MapExchangeApi(this WebApplication app, MessageQueueHub hub)
    {
        // Kafka 对拍：Record 的 key 兼做路由钥匙（未显式指定 routingKey 时用 Key）
        app.MapPost("/api/ex/{exchange}/publish", (string exchange, string? routingKey, MessageBody body) =>
        {
            if (string.IsNullOrWhiteSpace(body.Content))
                return Results.BadRequest(new { error = "content 不能为空" });

            // 默认路由钥匙：normal（不调 key 跳一跳，货永远有归处）
            var key = routingKey ?? body.Key;
            if (string.IsNullOrWhiteSpace(key)) key = "normal";

            try
            {
                var delivered = hub.Publish(exchange, key, body.Content);
                return Results.Ok(new { exchange, routingKey = key, deliveredTo = delivered });
            }
            catch (KeyNotFoundException)
            {
                return Results.NotFound(new { error = "不存在这个交换机", exchange });
            }
        });

        app.MapGet("/api/ex", () => Results.Ok(hub.ListExchanges().Select(e =>
            new { e.Name, e.Type, bindings = e.Bindings.Select(b => new { routingKey = b.Pattern, queue = b.Queue }) })));
    }
}
