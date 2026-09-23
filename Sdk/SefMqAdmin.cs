// ═══════════════════════════════════════════════════════════════
// Sdk/SefMqAdmin.cs —— 管理者对象：家底盘点 + 死信审案
// ═══════════════════════════════════════════════════════════════
using System.Text.Json;
using MessageQueueLab.Models;

namespace MessageQueueLab.Sdk;

public sealed class SefMqAdmin
{
    private readonly HttpClient _http;
    private readonly string _queue;

    public SefMqAdmin(SefMqConfig config, string queue = "normal")
    {
        _http = new HttpClient { BaseAddress = new Uri(config.Get("bootstrap.url")) };
        _queue = queue;
    }

    public async Task<(int Ready, int Locked, int PendingOnDisk, int DeadLetters)> QueueStatsAsync()
    {
        var doc = JsonDocument.Parse(await _http.GetStringAsync("/api/queues"));
        foreach (var e in doc.RootElement.EnumerateArray())
        {
            if (e.GetProperty("queue").GetString() == _queue)
                return (e.GetProperty("readyInMemory").GetInt32(),
                        e.GetProperty("lockedInMemory").GetInt32(),
                        e.GetProperty("pendingOnDisk").GetInt32(),
                        e.GetProperty("deadLetters").GetInt32());
        }
        throw new InvalidOperationException($"队列 {_queue} 不存在");
    }

    public async Task<List<MqMessage>> DlqListAsync()
    {
        var doc = JsonDocument.Parse(await _http.GetStringAsync($"/api/q/{_queue}/dlq"));
        var list = new List<MqMessage>();
        foreach (var e in doc.RootElement.EnumerateArray())
            list.Add(JsonSerializer.Deserialize<MqMessage>(e.GetRawText(), SefMqJson.CamelOpts)!);
        return list;
    }

    public Task DlqAckAsync(string id)    => _http.PostAsync($"/api/q/{_queue}/dlq/{id}/ack", null);
    public Task DlqReviveAsync(string id) => _http.PostAsync($"/api/q/{_queue}/dlq/{id}/revive", null);
}
