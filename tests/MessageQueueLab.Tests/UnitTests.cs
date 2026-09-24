// ═══════════════════════════════════════════════════════════════
// tests/MessageQueueLab.Tests —— 单元测试
//
//   覆盖面（对齐 CI 第一道闸门）：
//     1. Routing 通配匹配引擎（`#` 回溯 / `*` 单词）
//     2. WAL 回放重建（Log.Replay + Registry.ReplayHook）
//     3. 幂等防线（Acked → PrepareAck / TryGet）
//     4. DLX 死信路由（命中目标 / 回退）
//     5. 队列 + 交换机持久化（磁盘认领 / _exchanges.json）
//     6. Quorum 票数数学
//
//   约定：每个测试用独立 temp 目录 + 唯一队列名 —— MessageRegistry 是
//   静态字典，跨测试唯一键才能免互相污染。
// ═══════════════════════════════════════════════════════════════
using MessageQueueLab.Core;
using MessageQueueLab.Models;
using MessageQueueLab.Persistence;
using MessageQueueLab.Routing;
using Xunit;

namespace MessageQueueLab.Tests;

public class RoutingTests
{
    [Theory]
    [InlineData("pay.#", "pay.urgent", true)]       // `#` 吞多词
    [InlineData("pay.#", "pay", true)]              // `#` 吞 0 词
    [InlineData("pay.#", "pay.a.b", true)]          // `#` 吞 2 词级联
    [InlineData("#.urgent", "pay.urgent", true)]
    [InlineData("#.urgent", "urgent", true)]        // `#` 吞 0 词收尾
    [InlineData("#.error", "a.b.c.error", true)]    // 深层回溯
    [InlineData("#.error", "error", true)]          // 裸 error
    [InlineData("#.error", "error.b", false)]       // error 非收尾 → 不命中
    [InlineData("pay.#", "vip.urgent", false)]      // 前缀不符
    [InlineData("*.error", "a.error", true)]        // `*` 恰好 1 词
    [InlineData("*.error", "a.b.error", false)]     // `*` 不能吞多词
    [InlineData("dead.normal", "dead.normal", true)]
    [InlineData("dead.normal", "dead.vip", false)]
    public void BindingMatchesMatrix(string pattern, string routingKey, bool expect)
    {
        var b = new Binding(pattern, "some-queue");
        Assert.Equal(expect, b.Matches(routingKey));
    }
}

public class LogReplayTests : IDisposable
{
    private readonly string _dir;

    public LogReplayTests()
    {
        _dir = Path.Combine(Path.GetTempPath(), "mqlab-tests-" + Guid.NewGuid().ToString("N")[..8]);
    }

    public void Dispose()
    {
        if (Directory.Exists(_dir)) Directory.Delete(_dir, true);
    }

    [Fact]
    public void Replay_KeepsLiveMessages_DropsAcked()
    {
        var q = "rx-" + Guid.NewGuid().ToString("N")[..8];
        var m1 = new MqMessage { Id = Guid.NewGuid().ToString("N"), Content = "live-1" };
        var m2 = new MqMessage { Id = Guid.NewGuid().ToString("N"), Content = "dead-one" };
        var log1 = new MessageQueueLab.Persistence.Log(q, _dir, _ => { });
        // CRC 注入（业务链路里由 Prepare 来做，直试账本时手动补）
        var v1 = m1 with { Crc = LogMessage.ComputeContentCrc(m1) };
        var v2 = m2 with { Crc = LogMessage.ComputeContentCrc(m2) };
        log1.Push("e", v1, v1.Id);
        log1.Push("e", v2, v2.Id);
        log1.Push("d", null, v2.Id);
        var snapshot1 = log1.Snapshot();
        Assert.Single(snapshot1);
        Assert.Equal("live-1", snapshot1[0].Content);

        // ── 重新开账：回放要重构出同等状态 ──
        var log2 = new MessageQueueLab.Persistence.Log(q, _dir, _ => { });
        var snapshot2 = log2.Snapshot();
        Assert.Single(log2.Snapshot());
        Assert.Equal("live-1", snapshot2[0].Content);

        // ── Registry 跨重启重建：Acked 状态派发与账本一致 ──
        Assert.Equal(RegistryState.Acked, MessageRegistry.TryGet(q, v2.Id)!.State);
        Assert.Equal(RegistryState.Ready, MessageRegistry.TryGet(q, v1.Id)!.State);
    }


    [Fact]
    public void Replay_RebuildsRegistryWithSegmentLocation()
    {
        var q = "seg-" + Guid.NewGuid().ToString("N")[..8];
        var log1 = new MessageQueueLab.Persistence.Log(q, _dir, _ => { });
        var m = new MqMessage { Id = Guid.NewGuid().ToString("N"), Content = "anchor" };
        m = m with { Crc = LogMessage.ComputeContentCrc(m) };
        var evt = log1.Push("e", m, m.Id);
        Assert.NotNull(evt);

        var log2 = new MessageQueueLab.Persistence.Log(q, _dir, (s) => { });
        log2 = new MessageQueueLab.Persistence.Log(q, _dir, _ => { });

        // 重建后的 Registry 应指向崭新的落点（磁盘段而非空）
        var e2 = MessageRegistry.TryGet(q, m.Id);
        Assert.NotNull(e2);
        Assert.Equal(RegistryState.Ready, e2!.State);
        Assert.False(string.IsNullOrWhiteSpace(e2.SegmentPath));
        Assert.Equal('s', Path.GetFileName(e2.SegmentPath)[0]);   // 出生段是 s 段
    }

    [Fact]
    public void AckedIsNotRedelivered_AfterRestart()
    {
        var q = "ghost-" + Guid.NewGuid().ToString("N")[..8];
        var hub1 = new MessageQueueHub(_dir);
        var q1 = hub1.Get(q) ?? hub1.DeclareQueue(q);
        var sent = q1.Send("payload", "biz-key");
        var m = q1.Receive();
        Assert.NotNull(m);
        var (acked, _) = q1.Ack(m!.Id);
        Assert.True(acked);

        // ── 冷启重启 ──
        var hub2 = new MessageQueueHub(_dir);
        var q2 = hub2.Get(q)!;
        Assert.Null(q2.Receive());          // 已销账的消息绝不复活
        Assert.Empty(q2.DeadLetters());     // 也不该落进死信
    }

}

public class QueueLifecycleTests : IDisposable
{
    private readonly string _dir;

    public QueueLifecycleTests()
    {
        _dir = Path.Combine(Path.GetTempPath(), "mqlab-tests-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_dir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_dir)) Directory.Delete(_dir, true);
    }

    [Fact]
    public void Ack_Twice_SecondFails()
    {
        var hub = new MessageQueueHub(_dir);
        var q = hub.DeclareQueue("aevt-" + Guid.NewGuid().ToString("N")[..6]);
        q.Send("x");
        var m = q.Receive();
        Assert.NotNull(m);
        Assert.True(q.Ack(m!.Id).Acked);
        var (second, _) = q.Ack(m.Id);      // 第二次预备时锁定区已空
        Assert.False(second);
    }

    [Fact]
    public void NackThreeTimes_ArrivesAtDLQ()
    {
        var hub = new MessageQueueHub(_dir);
        var q = hub.DeclareQueue("dlx-" + Guid.NewGuid().ToString("N")[..8]);
        q.Send("victim");
        for (var i = 0; i < QueueCore.MaxRetryCount; i++)
        {
            var m = q.Receive();
            Assert.NotNull(m);
            var (nacked, _) = q.Nack(m!.Id);
            Assert.NotNull(nacked);
        }
        var dead = q.DeadLetters();
        Assert.Single(dead);
        Assert.Equal(QueueCore.MaxRetryCount, dead[0].RetryCount);
    }

    [Fact]
    public void DlgRoute_MatchesRebind()
    {
        var hub = new MessageQueueHub(_dir);
        var q = hub.DeclareQueue("dlxe-" + Guid.NewGuid().ToString("N")[..8]);
        var victim = new MqMessage { Id = Guid.NewGuid().ToString("N"), Content = "x" };
        var evts = hub.DeliverDead(victim, q.QueueName);     // 直打法：跳过 3 次重试一步到位
        Assert.NotEmpty(evts);
        Assert.Contains(q.DeadLetters(), d => d.Id == victim.Id);
    }

    [Fact]
    public void TtlExpires_ReceivesSkipsIt()
    {
        var hub = new MessageQueueHub(_dir);
        var q = hub.DeclareQueue("ttl-" + Guid.NewGuid().ToString("N")[..8]);
        q.Send("ghost", null, 1);
        q.Send("survivor", null, 60);
        Thread.Sleep(1200);
        var m = q.Receive();
        Assert.NotNull(m);
        Assert.Equal("survivor", m!.Content);
        Assert.Single(q.DeadLetters());
        Assert.Equal("ghost", q.DeadLetters()[0].Content);
    }
}

public class PersistenceLayerTests : IDisposable
{
    private readonly string _dir;

    public PersistenceLayerTests()
    {
        _dir = Path.Combine(Path.GetTempPath(), "mqlab-tests-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_dir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_dir)) Directory.Delete(_dir, true);
    }

    [Fact]
    public void SealedAt512Rows()
    {
        var q = "seal-" + Guid.NewGuid().ToString("N")[..8];
        var hub = new MessageQueueHub(_dir);
        var q1 = hub.DeclareQueue(q);
        for (var i = 0; i < 512; i++) q1.Send("row-" + i);   // 512 = RowsPerSegment 恰好整封
        q1.Receive();
        // 封段逻辑在 512 行触发 → 深入文件层校验：主账目录里至少有 1 个已封段即可
        var dir = Path.Combine(_dir, q);
        Assert.True(File.Exists(Path.Combine(dir, "s000001.jsonl")));
    }

    [Fact]
    public void DynamicQueue_Declared_ThenFilePaperPersists_AfterRestart()
    {
        var hub1 = new MessageQueueHub(_dir);
        hub1.DeclareQueue("dyn-" + Guid.NewGuid().ToString("N")[..6]);
        hub1.Bind("alerts", "#.dynamic", "normal");           // 触发 PersistExchanges
        Assert.True(File.Exists(Path.Combine(_dir, "_exchanges.json")));

        var hub2 = new MessageQueueHub(_dir);     // 重新初始化 → 磁盘认领 + 快照装载
        var alerts = hub2.ListExchanges().First(e => e.Name == "alerts");
        Assert.True(alerts.Bindings.Any(b => b.Pattern == "#.dynamic"));
    }

    [Fact]
    public void QuorumMath()
    {
        Assert.Equal(2, new ClusterReplicator(new[] { "f1", "f2" }).RequiredAcks);      // (2/2)+1
        Assert.Equal(3, new ClusterReplicator(new[] { "f1", "f2", "f3", "f4" }).RequiredAcks);
    }
}
