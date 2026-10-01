// ═══════════════════════════════════════════════════════════════
// Core/ClusterOptions.cs —— 集群环境变量读取（docker-compose / K8s 注入）
//
//   环境变量清单（容器启动即生效）：
//     MQ_NODE_NAME    节点身份证（Raft node 名，看板/日志也用它标注）
//     MQ_SELF         本节点对外 URL（成员表里代表自己的那条）
//     MQ_PEERS        Raft 全员表逗号分隔（含自身；种子节点用它引导，join 节点可为空）
//     MQ_LEADER_URL   join 目标（新节点/重启自愈找王用；单机为空）
//
//   故意没有的东西：
//     · 没有 MQ_ROLE —— 静态主从时代（P4 之前）用它声明 leader/follower，
//       动态主权后谁可写只看 Raft 王位，写"leader"不加冕、写"follower"也不限权，
//       留着只会误导人，已删除。单机/分布式也不靠它区分。
//     · 没有 MQ_FOLLOWERS —— 静态复制目标，v2 复制器走 Raft 成员表，已删除。
//
//   单机 vs 分布式：看有没有"组织"（全员表或 join 目标），都不配就是单机。
// ═══════════════════════════════════════════════════════════════
namespace MessageQueueLab.Core;

public static class ClusterOptions
{
    /// <summary>节点名（默认兜底 "node"，compose/K8s 传真实名）</summary>
    public static string NodeName
        => Environment.GetEnvironmentVariable("MQ_NODE_NAME") ?? "node";

    /// <summary>join 目标：新节点/重启自愈找王用（单机为空）</summary>
    public static string LeaderUrl
        => Environment.GetEnvironmentVariable("MQ_LEADER_URL") ?? "";

    /// <summary>Raft 全体成员列表（含自身）：种子节点引导用；join 节点可为空</summary>
    public static string[] Peers
        => (Environment.GetEnvironmentVariable("MQ_PEERS") ?? "")
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

    /// <summary>本节点对外 URL（成员表里代表自己的那条）</summary>
    public static string SelfUrl
        => Environment.GetEnvironmentVariable("MQ_SELF") ?? "";

    /// <summary>是否处于分布式模式：配了全员表或 join 目标就是，反之单机</summary>
    public static bool Distributed => Peers.Length > 0 || LeaderUrl.Length > 0;

    /// <summary>
    /// 启动期复制目标（Peers 剥自己）：只给 ClusterReplicator 当"出生纸"——
    /// 运行时一律走 Raft 成员表（PublishTargets），这里只是 Raft 尚未装配前的 fallback。
    /// </summary>
    public static string[] BootstrapTargets
        => Peers.Where(u => u.Length > 0 && u != SelfUrl).ToArray();

    // ── 动态主权（Raft 王位驱动一切；环境变量只管"出生纸"） ──
    /// <summary>Raft 王座引用（Program 装配后注入；单机模式为 null）</summary>
    public static RaftNode? Raft { get; set; }

    /// <summary>本节点当前是否可受理业务写入 —— 故障转移核心：
    ///   single → 恒可写；分布式 → 看 Raft 王位（谁当选谁可写，老王回归自动降级后也要 503）
    /// </summary>
    public static bool IsWriter
        => !Distributed || Raft?.Role == RaftRole.Leader;
}
