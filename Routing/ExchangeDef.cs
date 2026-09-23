// ═══════════════════════════════════════════════════════════════
// Routing/ExchangeDef.cs —— 交换机定义（名字 + 类型 + 绑定清单）
//
//   什么是交换机（Exchange）？
//     RabbitMQ 语义的核心：**生产者不直接发消息给队列**，而是给交换机 + routing key；
//     交换机根据“绑定规则”把消息路由到一个/多个队列。
//
//   三种类型（区分类）：
//     · direct : 按字面量精确匹配（routingKey == routingKey）
//     · fanout : 广播给所有绑定队列（不看 key，各得一份）
//     · topic  : 通配符匹配（* = 恰好 1 词，# = 0..n 词；多播）
//
//   绑定规则（Binding）在 Routing/Binding.cs；路由动作的执行在 Core/MessageQueueHub.Publish()
//
//   为什么用 record 而不是 class？
//     · record 默认不可变，表达“定义即不可变”的领域语义
//     · 等价于 Kafka TopicMetadata 的简化版
// ═══════════════════════════════════════════════════════════════
namespace MessageQueueLab.Routing;

// Name = 交换机名（如 "orders" / "broadcast" / "alerts"）
// Type = 交换机类型（direct / fanout / topic）
// Bindings = 绑定规则列表（每条 Binding 由 (routingKey或pattern, queue) 组成）
public record ExchangeDef(string Name, string Type, List<Binding> Bindings);
