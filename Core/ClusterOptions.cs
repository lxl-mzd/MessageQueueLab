// ═══════════════════════════════════════════════════════════════
// Core/ClusterOptions.cs —— 集群环境变量读取（docker-compose 注入）
//
//   环境变量清单（docker-compose.yml 的 environment: 注入，容器启动即生效）：
//     MQ_ROLE         single | leader | follower   （默认 single）
//                     · single  : 单机模式（默认，不加 docker-compose 也能跑）
//                     · leader  : 集群主节点——接受业务 HTTP 写入，并复制事件给 followers
//                     · follower: 副本节点——只接收复制事件（业务写请求一律 503 拒绝）
//     MQ_NODE_NAME    节点名（看板/日志标注“node-leader”等，便于运维区分）
//     MQ_FOLLOWERS    Leader 专用：逗号分隔的 follower 基址（http://host:port, http://host2:port2）
//     MQ_LEADER_URL   Follower 专用：Leader 基址
//                     ⚠️ 当前为静态主从（P4 自动选主 Raft 生效前）；将来 leader 挂了自动选举接管
//
//   设计权衡：
//     - 每个 getter 都是实时读取 Environment.GetEnvironmentVariable —— 便于运行时改配置
//     - 默认值兜底：即使忘配 env，系统也能按 single 模式正常跑
//
//   为什么用 static class + getter 而不是 Options 对象注入？
//     · 教学期简化：环境变量直接读，无需 config 文件
//     · 真实生产应换 IOptions<T> / IConfiguration 强类型注入
// ═══════════════════════════════════════════════════════════════
namespace MessageQueueLab.Core;

public static class ClusterOptions
{
    /// <summary>节点角色：single（单机兜底）/ leader（写主）/ follower（只读副本）</summary>
    public static string Role
        => Environment.GetEnvironmentVariable("MQ_ROLE") ?? "single";

    /// <summary>节点名（默认兜底 "node"，docker-compose 传 Ferrari/nodes 等别名）</summary>
    public static string NodeName
        => Environment.GetEnvironmentVariable("MQ_NODE_NAME") ?? "node";

    /// <summary>Follower 专用：Leader 基址（当前静态主从，P4 选主生效前）</summary>
    public static string LeaderUrl
        => Environment.GetEnvironmentVariable("MQ_LEADER_URL") ?? "";
    /// <summary>Follower 列表：逗号分隔的 follower 基址（空格自动剔除）</summary>
    public static string[] Followers
        => (Environment.GetEnvironmentVariable("MQ_FOLLOWERS") ?? "")
           .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

    /// <summary>Raft 全体成员列表（含自身）：Raft 选举必须所有节点互相认识，否则会出现双 Leader</summary>
    public static string[] Peers
        => (Environment.GetEnvironmentVariable("MQ_PEERS") ?? "")
           .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

    /// <summary>本节点对外 URL（MQ_PEERS 里剥自己用的身份证）</summary>
    public static string SelfUrl
        => Environment.GetEnvironmentVariable("MQ_SELF") ?? "";

    /// <summary>是不是写主节点（leader 或 single 都可以写）</summary>
    public static bool IsLeader      => Role is "leader" or "single";

    /// <summary>是不是只读的 follower（副本节点，只收复制流）</summary>
    public static bool IsFollower    => Role == "follower";

    /// <summary>是否处于分布式模式下（leader/follower 都算）</summary>
    public static bool Distributed   => Role is "leader" or "follower";
}
