// ═══════════════════════════════════════════════════════════════
// Sdk/SefMqAdmin.cs —— 管理者对象：家底盘点 + 死信审案
// ═══════════════════════════════════════════════════════════════
using System.Text.Json;
using MessageQueueLab.Models;

namespace MessageQueueLab.Sdk;

public sealed class SefMqAdmin : IDisposable
{
    private readonly SefMqClusterClient _cluster;
    private readonly string _queue;

    public SefMqAdmin(SefMqConfig config, string queue = "normal")
    {
        _cluster = new SefMqClusterClient(config);
        _queue = queue;
    }

    public void Dispose() => _cluster.Dispose();

    public async Task<(int Ready, int Locked, int PendingOnDisk, int DeadLetters)> QueueStatsAsync()
    {
        var resp = await _cluster.GetAsync("/api/queues");
        resp.EnsureSuccessStatusCode();
        var doc = JsonDocument.Parse(await resp.Content.ReadAsStringAsync());
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
        var resp = await _cluster.GetAsync($"/api/q/{_queue}/dlq");
        resp.EnsureSuccessStatusCode();
        var doc = JsonDocument.Parse(await resp.Content.ReadAsStringAsync());
        var list = new List<MqMessage>();
        foreach (var e in doc.RootElement.EnumerateArray())
            list.Add(JsonSerializer.Deserialize<MqMessage>(e.GetRawText(), SefMqJson.CamelOpts)!);
        return list;
    }

    public Task<HttpResponseMessage> DlqAckAsync(string id)    => _cluster.PostAsync($"/api/q/{_queue}/dlq/{id}/ack", null);
    public Task<HttpResponseMessage> DlqReviveAsync(string id) => _cluster.PostAsync($"/api/q/{_queue}/dlq/{id}/revive", null);
}
