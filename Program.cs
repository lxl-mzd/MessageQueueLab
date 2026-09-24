// ═══════════════════════════════════════════════════════════════
//  Program.cs —— 薄启动器（集群装配 + Raft 选主 + 路由注册）
//
//  5 步：
//    ① CreateBuilder       ASP.NET web框架
//    ② CORS 全开放         为了看板/测试
//    ③ 集群身份装配        Leader 持复制器；Follower 不挂
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
//    MQ_NODE_ID    "node-leader" / "node-f1" / "node-f2"
//    MQ_PEERS      "http://mq-leader:8080,http://mq-follower-1:8080,http://mq-follower-2:8080"
//    MQ_ROLE       leader / follower （决定初始 Role，后续由 Raft 自动晋升/降级）
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

// ── 集群身份装配（v2 动态主权）：分布式下每个节点都持复制器 ——
//    王位切到谁，谁就朝另外两间发货（ReplicationTargets = MQ_PEERS 剥自己）
//    单机模式不建复制器
ClusterReplicator? replicator =
    ClusterOptions.Distributed ? new ClusterReplicator(ClusterOptions.ReplicationTargets) : null;

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
    var peerUrls = ClusterOptions.Peers.Length > 0
        ? ClusterOptions.Peers
        : new[] { "http://localhost:8080" }.Concat(ClusterOptions.Followers).ToArray();

    raftNode = new RaftNode(
        ClusterOptions.NodeName,
        ClusterOptions.SelfUrl,
        peerUrls.Where(p => p.Length > 0).ToArray(),
        hub.PushEvent);

    raftNode.StartAsync();                    // 启动 Raft loop（心跳/选举）

    // ★ 把王座接进 ClusterOptions：写闸门（IsWriter）与复制流从此跟 Raft 王位联动
    ClusterOptions.Raft = raftNode;
}

// ── 步骤 ⑤：5 组路由注册 ──
app.MapQueueApi(hub);                       // 队列收发
app.MapDeadLetterApi(hub);                  // 死信审案
app.MapExchangeApi(hub);                    // 交换机投递
app.MapMonitoringApi(hub);                  // 健康/看板/统计/事件流水
app.MapClusterApi(hub);                     // 集群复制接收 + 集群状态
app.MapRaftApi(raftNode, hub);              // Raft vote/heartbeat/status
app.StartSweeper(hub);                      // 巡查员后台线程（含TTL、ratio GC）

hub.PushEvent($"🚾 集群节点就绪（role={ClusterOptions.Role} node={ClusterOptions.NodeName}）");
hub.PushEvent("⏳ Raft 开始心跳/选举");

app.Run();
