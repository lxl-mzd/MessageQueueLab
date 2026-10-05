// ═══════════════════════════════════════════════════════════════
// Sdk/SefMqConsumer.cs —— KafkaConsumer 姿势的消费者对象
//   Subscribe(topic) → BeginConsume：内部后台线程不停 Receive→handler→断案
//   handler 正常返回 = 自动 Ack；抛异常 = 自动 Nack（enable.auto.commit 对拍）
//   handler 内也可 ctx.Ack/Nack 手动断案（已断案的自动跳过）
// ═══════════════════════════════════════════════════════════════
using System.Net.Http.Json;
using System.Text.Json;
using MessageQueueLab.Models;

namespace MessageQueueLab.Sdk;

public sealed class SefMqConsumer : IDisposable
{
    private readonly SefMqClusterClient _cluster;
    private readonly SefMqConfig _config;
    private string? _topic;

    public SefMqConsumer(SefMqConfig config)
    {
        _config = config;
        _cluster = new SefMqClusterClient(config);
    }

    public void Dispose() => _cluster.Dispose();

    public void Subscribe(string topic) => _topic = topic;

    public Task BeginConsume(
        Func<MqMessage, SefMqContext, Task> handler,
        CancellationToken? ct = null,
        int? visibilitySeconds = null)
    {
        if (_topic is null) throw new InvalidOperationException("先 Subscribe(topic) 再 BeginConsume");
        var cancel = ct ?? CancellationToken.None;
        return Task.Run(async () =>
        {
            var vis = visibilitySeconds ?? (int.TryParse(_config.Get("default.visibility", "12"), out var v) ? v : 12);
            // 长轮询：空墙时服务端最多挂 waitMs 毫秒等消息；客户端小步紧贴
            var waitMs = int.TryParse(_config.Get("long.poll.wait.ms", "500"), out var w) ? w : 500;
            var pollMs = int.TryParse(_config.Get("poll.delay.ms", "60"), out var pm) ? pm : 60;

            while (!cancel.IsCancellationRequested)
            {
                try
                {
                    var resp = await _cluster.PostAsync($"/api/q/{_topic}/receive?visibilitySeconds={vis}&waitMs={waitMs}", null, cancel);
                    if (resp.StatusCode == System.Net.HttpStatusCode.NoContent)
                    {
                        await Task.Delay(pollMs, cancel);
                        continue;
                    }
                    resp.EnsureSuccessStatusCode();
                    var json = await resp.Content.ReadAsStringAsync(cancel);
                    var msg = JsonSerializer.Deserialize<MqMessage>(json, SefMqJson.CamelOpts)!;

                    var ctx = new SefMqContext(
                        ack:  async id => await _cluster.PostAsync($"/api/q/{_topic}/ack/{id}", null, cancel),
                        nack: async id => await _cluster.PostAsync($"/api/q/{_topic}/nack/{id}", null, cancel));

                    try
                    {
                        await handler(msg, ctx);
                        if (!ctx.HandledManually)
                            await ctx.Ack(msg.Id);       // 自动断案：正常返回 = Ack
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[consumer] 处理异常自动 Nack：{ex.Message}");
                        await ctx.Nack(msg.Id);
                    }
                }
                catch (OperationCanceledException) { break; }
                catch (Exception ex) { Console.WriteLine($"[consumer] 轮询异常，稍候再试：{ex.Message}"); }
            }
        }, cancel);
    }
}

/// <summary>消费上下文：handler 里用它断案（Ack/Nack 手柄，防重复断案）</summary>
public sealed class SefMqContext
{
    private readonly Func<string, Task> _ack;
    private readonly Func<string, Task> _nack;

    /// <summary>handler 是否已手动断案（是则平台不再自动 Ack/Nack）</summary>
    internal bool HandledManually { get; private set; }

    internal SefMqContext(Func<string, Task> ack, Func<string, Task> nack)
    {
        _ack = ack; _nack = nack;
    }

    public Task Ack(string messageId)  { HandledManually = true; return _ack(messageId); }
    public Task Nack(string messageId) { HandledManually = true; return _nack(messageId); }
}
