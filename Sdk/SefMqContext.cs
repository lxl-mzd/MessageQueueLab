// ═══════════════════════════════════════════════════════════════
// Sdk/SefMqContext.cs —— 消费上下文：handler 里用它断案
// ═══════════════════════════════════════════════════════════════
namespace MessageQueueLab.Sdk;

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
