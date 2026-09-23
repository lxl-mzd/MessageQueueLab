// ═══════════════════════════════════════════════════════════════
// Sdk/SefMqRecord.cs —— Record：MessageBody 的 Kafka 三件套卷装
// ═══════════════════════════════════════════════════════════════
namespace MessageQueueLab.Sdk;

public sealed record SefMqRecord(string Topic, string? Key, string Value);

/// <summary>RecordMetadata 同款：发送到哪儿了</summary>
public sealed record SefMqMetadata(string Topic, string MessageId, string? Key);
