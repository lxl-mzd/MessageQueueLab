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

public sealed class SefMqConsumer
{
    private readonly HttpClient _http;
    private readonly SefMqConfig _config;
    private string? _topic;

    public SefMqConsumer(SefMqConfig config)
    {
        _config = config;
        _http = new HttpClient { BaseAddress = new Uri(config.Get("bootstrap.url")) };
    }

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
                    var resp = await _http.PostAsync($"/api/q/{_topic}/receive?visibilitySeconds={vis}&waitMs={waitMs}", null, cancel);
                    if (resp.StatusCode == System.Net.HttpStatusCode.NoContent)
                    {
                        await Task.Delay(pollMs, cancel);
                        continue;
                    }
                    resp.EnsureSuccessStatusCode();
                    var json = await resp.Content.ReadAsStringAsync(cancel);
                    var msg = JsonSerializer.Deserialize<MqMessage>(json, SefMqJson.CamelOpts)!;

                    var ctx = new SefMqContext(
                        ack:  async id => await _http.PostAsync($"/api/q/{_topic}/ack/{id}", null, cancel),
                        nack: async id => await _http.PostAsync($"/api/q/{_topic}/nack/{id}", null, cancel));

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
