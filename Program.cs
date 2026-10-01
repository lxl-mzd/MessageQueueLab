// ═══════════════════════════════════════════════════════════════
//  Program.cs —— 薄启动器（集群装配 + Raft 选主 + 路由注册）
//
//  5 步：
//    ① CreateBuilder       ASP.NET web框架
//    ② CORS 全开放         为了看板/测试
//  ③ 集群身份装配        分布式下每个节点都持复制器；单机不挂
//    ④ Raft 节点 born 并启动心跳/选举 loop
//    ⑤ 注册 6 组路由
//
//  启动方式（单机 docker）：
//      docker build -t mq-lab:13 .
//      docker run -d --name mq-server -p 5080:8080 -v mqlab-data:/app/data mq-lab:13
//  集群方式：
//      docker compose up -d --build
//      → leader(5081)/follower1(5082)/follower2(5083)，Raft 自动选举
//
//  Raft 环境变量（docker-compose 注入）：
//    MQ_NODE_ID    "node-1" / "node-2" / "node-3"（compose）或 Pod 名（K8s）
//    MQ_PEERS      全员表逗号分隔（含自身，种子节点用；join 节点可为空）
//    MQ_SELF       本节点对外 URL（成员表身份证）
//    MQ_LEADER_URL join 目标（新节点找王；单机为空）
//    身份（王/民）一律由 Raft 选举决定，没有静态角色参数
//
// ═══════════════════════════════════════════════════════════════
using MessageQueueLab.Core;
using MessageQueueLab.Persistence;
using MessageQueueLab.Web;

var builder = WebApplication.CreateBuilder(args);

// ⚙️ CORS 全开放：网页（http://localhost:5080/）能直接用 fetch 调队列接口
//    生产上线时应换成白名单：AllowAnyOrigin() 是最宽松的——真正部署要收紧到可信域
builder.Services.AddCors(o => o.AddDefaultPolicy(p =>
    p.AllowAnyOrigin().AllowAnyHeader().AllowAnyMethod()));

// ── 集群身份装配：分布式下每个节点都持复制器（运行时目标走 Raft 成员表） ──
//    单机模式不建复制器
ClusterReplicator? replicator =
    ClusterOptions.Distributed ? new ClusterReplicator(ClusterOptions.BootstrapTargets) : null;

builder.Services.AddSingleton(new MessageQueueHub("data", replicator));

var app = builder.Build();
app.UseCors();

var hub = app.Services.GetRequiredService<MessageQueueHub>();

// ── Raft 选主装配（P4）：三节点实行 Raft 选举；单机模式跳过 ──
RaftNode? raftNode = null;
if (ClusterOptions.Distributed)
{
    // Raft 全体成员表：优先用 MQ_PEERS（每个节点都要知道全部 3 个成员——含自身，
    // 否则 follower 之间互不相识 → 两个小选区各自选出 Leader → 集群分裂）
    // MQ_SELF = 本节点身份证，RaftNode 用它把"自己"从 peers 里剥掉（不投自己不干扰）
    // Raft 全体成员表：优先用 MQ_PEERS（每个节点都要知道全部 3 个成员——含自身，
    // 否则 follower 之间互不相识 → 两个小选区各自选出 Leader → 集群分裂）
    // 若 MQ_PEERS 为空：新节点（join 模式）不预设任何 peers，只靠"自我 join 上报后由 King 通知"。
    // （注意：不 fallback localhost —— 那会让 join 模式的节点误认为自己有一个 2 人小集群）
    var peerUrls = ClusterOptions.Peers.Length > 0
        ? ClusterOptions.Peers
        : Array.Empty<string>();

    raftNode = new RaftNode(
        ClusterOptions.NodeName,
        ClusterOptions.SelfUrl,
        peerUrls.Where(p => p.Length > 0).ToArray(),
        "data",     // members.json 与 WAL 数据目录同根（_cluster/ 子夹）
        hub.PushEvent);

    raftNode.StartAsync();                    // 启动 Raft loop（心跳/选举）

    // ★ 把王座接进 ClusterOptions：写闸门（IsWriter）与复制流从此跟 Raft 王位联动
    ClusterOptions.Raft = raftNode;

    // ★ F2 自愈 + 新节点自助 join-on-boot（配置了 join URL 就启动）：
    //    空表走老路 join；有表（重启/崩溃恢复，表可能是旧表）先 join 认领活表再 catch-up。
    //    必须等本节点 HTTP 服务真正监听后才启动 —— 否则 Leader 回推的 apply-membership
    //    打到还没开门的端口上丢失，本节点成员表永远是空的（join 成功但本地无表）。
    if (ClusterOptions.LeaderUrl.Length > 0)
    {
        var joinUrl = ClusterOptions.LeaderUrl;
        var lifetime = app.Services.GetRequiredService<Microsoft.Extensions.Hosting.IHostApplicationLifetime>();
        lifetime.ApplicationStarted.Register(() =>
            _ = raftNode.TryRejoinAtBootAsync(joinUrl, hub));
    }
}

// ── 步骤 ⑤：5 组路由注册 ──
app.MapQueueApi(hub);                       // 队列收发
app.MapDeadLetterApi(hub);                  // 死信审案
app.MapExchangeApi(hub);                    // 交换机投递
app.MapMonitoringApi(hub);                  // 健康/看板/统计/事件流水
app.MapClusterApi(hub);                     // 集群复制接收 + 集群状态
app.MapRaftApi(raftNode, hub);              // Raft vote/heartbeat/status
app.StartSweeper(hub);                      // 巡查员后台线程（含TTL、ratio GC）

hub.PushEvent($"🚾 集群节点就绪（role={raftNode?.Role.ToString() ?? "single"} node={ClusterOptions.NodeName}）");
hub.PushEvent("⏳ Raft 开始心跳/选举");

// ── 步骤 ⑥：优雅下线（SIGTERM → .NET ApplicationStopping → leave → 退出）──
//    K8s 缩容时发 SIGTERM，我们利用这 20 秒 grace period 处理 leave：
//      · Leader → LeaveMemberAsync(self)：m 事件去掉自己 → 剩余成员重选王
//      · Follower → 向当前 Leader 发 /api/raft/leave {node: 自己}
//    （不再依赖镜像里的 wget/curl/exec preStop —— .NET 自己管理生命周期）
var lifetime2 = app.Services.GetRequiredService<Microsoft.Extensions.Hosting.IHostApplicationLifetime>();
lifetime2.ApplicationStopping.Register(() =>
{
    try
    {
        if (raftNode == null) return;
        raftNode.GracefulOfflineAsync().GetAwaiter().GetResult();   // 同步等最多 ~5s（gracePeriod 内）
    }
    catch { /* 退出路径中的异常不再阻塞 */ }
});

app.Run();
