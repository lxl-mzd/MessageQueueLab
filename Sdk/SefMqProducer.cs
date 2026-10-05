// ═══════════════════════════════════════════════════════════════
// Sdk/SefMqProducer.cs —— KafkaProducer 姿势的生产者对象
// ═══════════════════════════════════════════════════════════════
using System.Net.Http.Json;
using System.Text.Json;
using MessageQueueLab.Models;

namespace MessageQueueLab.Sdk;

public sealed class SefMqProducer : IDisposable
{
    private readonly SefMqClusterClient _cluster;

    public SefMqProducer(SefMqConfig config)
    {
        _cluster = new SefMqClusterClient(config);
    }

    public void Dispose() => _cluster.Dispose();

    /// <summary>Kafka 同款 send(record, callback)：异步发送 + 回调（fire-and-forget）</summary>
    public void Send(SefMqRecord record, Action<SefMqMetadata?, Exception?>? callback = null)
        => _ = SendInternalAsync(record)
                  .ContinueWith(t =>
                  {
                      if (t.IsFaulted) callback?.Invoke(null, t.Exception!.GetBaseException());
                      else callback?.Invoke(t.Result, null);
                  });

    /// <summary>异步等待版（测试友好）</summary>
    public async Task<SefMqMetadata> SendAsync(SefMqRecord record)
        => await SendInternalAsync(record);

    private async Task<SefMqMetadata> SendInternalAsync(SefMqRecord record)
    {
        var resp = await _cluster.PostAsync($"/api/q/{record.Topic}/messages",
            JsonContent.Create(new { content = record.Value, key = record.Key }));
        resp.EnsureSuccessStatusCode();
        var doc = JsonSerializer.Deserialize<JsonElement>(await resp.Content.ReadAsStringAsync());
        // 集群版响应形状：{ message: MqMessage, replication: {...} }
        var msgJson = doc.GetProperty("message").GetRawText();
        var msg = JsonSerializer.Deserialize<MqMessage>(msgJson, SefMqJson.CamelOpts)!;
        var replRaw = doc.TryGetProperty("replication", out var replEl) ? replEl.GetRawText() : "{}";
        return new SefMqMetadata(msg.Id, replRaw, record.Key);
    }
}

/// <summary>Record：发什么（Topic/Key/Value 三件套，Kafka 同款卷装）</summary>
public sealed record SefMqRecord(string Topic, string? Key, string Value);

/// <summary>RecordMetadata 同款：发送到哪儿了</summary>
public sealed record SefMqMetadata(string Topic, string MessageId, string? Key);
