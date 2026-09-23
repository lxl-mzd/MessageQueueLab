// ═══════════════════════════════════════════════════════════════
// Models/MqMessage.cs —— 统一消息模型（全栈同形：内存/账本/HTTP/SDK/死信）
//
//   Id          身份证号（Guid，全栈唯一）
//   Crc         校验码（对“除 Crc 外的规范化 JSON”计算，防自指循环；出账时随消息派发）
//   Key         路由键（交换机 direct/topic 的分拣依据；队列直发时可空）
//   Content     业务内容
//   Timestamp   消息产生时间 UTC（终身不变——出生证明）
//   ReceiveTime 最近一次被消费者领取的时间（UTC；内存态，随重试事件持久化）
//   RetryCount  重试次数
//
//   唯一性：Id = 系统身份证（全球唯一）；Key = 业务标签（同 key 多条合法）
// ═══════════════════════════════════════════════════════════════

namespace MessageQueueLab.Models;

public sealed record MqMessage
{
    public string Id { get; init; } = "";
    public string? Crc { get; init; }
    public string? Key { get; init; }
    public string Content { get; init; } = "";
    public DateTime Timestamp { get; init; }          // UTC
    public DateTime? ReceiveTime { get; init; }       // UTC（最近一次领取）
    public DateTime? ExpiresAtUtc { get; init; }      // UTC（TTL 到期时间；null=永不过期）
    public int RetryCount { get; init; }
}
