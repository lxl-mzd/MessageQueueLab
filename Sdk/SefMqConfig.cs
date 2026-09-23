// ═══════════════════════════════════════════════════════════════
// Sdk/SefMqConfig.cs —— Properties 同款配置袋（Kafka 风格）
// ═══════════════════════════════════════════════════════════════
using System.Text.Json;

namespace MessageQueueLab.Sdk;

public sealed class SefMqConfig : Dictionary<string, string>
{
    public string Get(string key, string fallback = "")
        => TryGetValue(key, out var v) ? v : fallback;
}

/// <summary>SDK 与服务端约定的 JSON 序列化选项（camelCase 服务端 vs 大小写不敏感反序列化）</summary>
public static class SefMqJson
{
    public static readonly JsonSerializerOptions CamelOpts = new()
    {
        PropertyNameCaseInsensitive = true,
    };
}
