# 自造消息队列 · 完整设计文档

> 版本：v4.0（动态主权 Raft + Quorum 三阶段写入 + DLX 死信交换机 + 幂等查重落地 + 后台压缩巡查）
> 交付：可独立运行、可打包为 dll 引入业务侧、支持 docker-compose 三节点集群部署、支持故障转移

---

## 一、总览

本项目是一个**从零实现的生产级消息队列**，支持：

| 功能 | 状态 |
|---|---|
| FIFO 队列 + 线程安全入队/出队 | ✅ |
| WAL 分段账本（Append-Only + 512 行封段 + 全序 Seq 回放） | ✅ |
| 双层 CRC 封蜡（行级 + 消息级） | ✅ |
| Ack / Nack / 重试 / 死信队列（显式 nack×3 / 锁定超时 / TTL 三通道） | ✅ |
| 死信交换机 DLX（topic 交换机 `dead`，routingKey=`dead.{queue}`，可重绑） | ✅ |
| 死信复活（`retryCount` 归零）+ 死信人工确认删除 | ✅ |
| 消息 TTL（body `ttlSeconds` → `ExpiresAtUtc`，receive 时惰性判死） | ✅ |
| Registry 全局映射表（状态机 + 位置 + Acked 24h 幂等窗口；**WAL 回放重建**） | ✅ |
| 幂等查重（领取前查表、Acked 幽灵丢弃 + follower 复制流登记） | ✅ |
| 四大交换机（direct / fanout / topic 通配符多播 + **DLX**） | ✅ |
| 长轮询 Receive（waitMs）+ 可见性超时回收 | ✅ |
| HTTP REST API + SDK（Kafka 风格） | ✅ |
| 健康看板 Dashboard + 事件流水 | ✅ |
| Quorum 写入三阶段流水线（Prepare → 多数派复制 → 本地提交） | ✅ |
| Raft 选主（vote/heartbeat RPC；term 收养 / 同任期一票 / 随机超时 / 自剥地址） | ✅ |
| **动态主权故障转移**（写闸门与复制流跟随 Raft 王位；杀王秒级接管） | ✅ |
| 后台巡查员（可见性回收 + 定时压缩触发，每节点自扫磁盘） | ✅ |
| WAL 压缩（段数≥4 或 50% 脏率 + 行数/段龄双门槛；tmp→Move→Replace 三步原子上位） | ✅ |
| docker-compose 三节点集群 + 崩溃自愈 | ✅ |
| ConsumerGroup / 分区消费 | 🔜 |
| JWT / API-Key 鉴权 | 🔮 |
| 动态队列/绑定声明 API | 🔮 |

---

## 二、目录结构

```
MessageQueueLab\
├── Program.cs                 薄启动器（复制器装配 + Raft 王座接线 + 路由注册）
├── docker-compose.yml         三节点集群（MQ_PEERS 全员表 + MQ_SELF 身份）
├── Dockerfile / .dockerignore
├── API.md / check-mq.ps1
│
├── Core\
│   ├── QueueCore.cs           单队列引擎（ready 墙/锁定区/死信区/主账/死信账）
│   ├── MessageQueueHub.cs     枢纽：队列池 + 4 交换机路由 + 事件流水 + Quorum 复制
│   ├── ClusterOptions.cs      集群配置 + 动态主权（Raft 引用 / IsWriter / ReplicationTargets）
│   ├── RaftNode.cs            Raft 三态状态机（选举/心跳）
│   └── ClusterReplicator.cs   事件复制器（多数派票数器）
│
├── Persistence\
│   ├── Log.cs                 账本总管（分段清单/回放/Registry 重建/压缩计划）
│   ├── LogSegment.cs          段文件三态（Active/Sealed/Compacted）+ Ack 计数
│   ├── LogMessage.cs          事件信封（行级 CRC + 消息级 CRC）
│   ├── LogCleaner.cs          压缩执行器（tmp → Move → Replace 三步）
│   └── MessageRegistry.cs     全局映射表（回放重建 + 状态机 + 幂等窗口）
│
├── Models\ MqMessage.cs, QueueCoreStats.cs（含 MessageBody）
├── Routing\ Binding.cs（`#` 回溯通配引擎）, ExchangeDef.cs
├── Sdk\ SefMq*（Producer/Consumer/Context/Record/Config/Admin）
└── Web\ Endpoints.*（Queue/Exchange/DeadLetter/Monitoring/Cluster/Raft）+ DashboardPage.cs
```

---

## 三、核心架构

### 3.1 单机全貌

```
HTTP ──▶ Endpoints ──▶ MessageQueueHub
                          │
       ┌──────────────────┼──────────────────────┐
       ▼                  ▼                      ▼
 QueueCore("normal") QueueCore("vip")     QueueCore("error")
   ├── _ready         就绪墙（FIFO）
   ├── _locked        锁定区（visibilityicoptical timeout）
   ├── _dead          死信清单 + _dlqLedger（{queue}.dlq 分段 WAL）
   └── _ledger        主账本压缩主题定期由检查员触发
                                 │
Hub._exchanges：orders(direct) / broadcast(fanout) / alerts(topic) / dead(topic DLX)
```

每条 WAL 行 = `crc|json` 信封（类型 e=入账 / n=回墙 / d=销账），每个队列独立目录：

```
data/{queue}/
  s000001.jsonl … s00000N.jsonl    （512 行封段，Append-Only）
  c{start}-{end}.compacted.jsonl   （后台压缩终态快照）
data/{queue}.dlq/                  （死信账本，同款 WAL 机制）
```

### 3.2 三节点集群 + 动态主权（v4 核心变化）

```
   ┌──────────────  Raft 全员表 MQ_PEERS（3 员，每节点都知道全部成员）───┐
   │   MQ_SELF = 本节点身份证（_peers 剥自己，杜绝自投心跳）           │
   ▼                                                                ▼
 RaftNode(term 收养 / 同任期一票 / 随机 2~4.5s 超时)  →  王位在谁
      │
      ├── 写入闸门：IsWriter = Raft.Role == Leader   （MQ_ROLE 只是初始声明）
      ├── 复制目的地：ReplicationTargets = MQ_PEERS - MQ_SELF  （王在谁，就朝另两间发货）
      └── 复制流接收：非现任王都收 /api/cluster/replicate

 故障转移实录：杀王容器 → ≤12s 新王当选（term++）→ 写权限自动跟王走
              被杀王容器回归 → 自动降级 Follower、复制流追平、写拒绝 503
```

### 3.3 Quorum 写入三阶段流水线

```
① Prepare            内存造 MqMessage + LogMessage（不落盘、不入 ready、Registry 不动）
② ReplicateAsync     并行 POST 给 ReplicationTargets，收集 ack
③ Quorum ≥ N/2+1     ✔ → CommitEvent：WAL 追加 + Apply + Registry 登记封段 → 201
                      ✗ → 503；全集群零残留，客户端重试即可
```

Ack 走同一流水线：`PrepareAck`（拆锁定区+造 d 事件）→ 复制过票 → `CommitAck` 销账。

### 3.4 死信链路（经 DLX）

```
nack×3 / 锁定超时 / TTL 到期
   │ 主账 d 销账（无条件）
   ▼
_deadSink → DLX dead(topic)，routingKey="dead.{来源队列}"
   ├── 命中绑定 → 目标队列 AcceptDeadLetter（dlq 账本 e 事件 + _dead + Registry MarkDead）
   ├── DLQ 满 500 → 目标拒收（尸体管控，主账仍然干净）
   └── 无绑定命中 → 🌡️ 回退自投自家 DLQ
        ▼
   死信货架：GET /dlq（清单）→ /dlq/{id}/revive（retryCount 归 0 重生）
                            → /dlq/{id}/ack（人工确认销账）
```

### 3.5 幂等设计（四层防线）

| 层 | 机制 | 状态 |
|---|---|---|
| 复制层 | `PushExternal`：`evt.Seq < _nextSeq` 回退丢弃 | ✅ |
| 账本层 | `d`（销账）事件永久在 WAL；回放只装回活消息 | ✅ |
| 接收层 | Receive 前 `Registry.TryGet(...).State==Acked → 丢弃 + 补 d 事件（🧊 幽灵拦截）` | ✅ |
| 消费层 | 业务幂等键（`MqMessage.Key` 由生产者填业务键，消费方按 Key 去重） | 约定字段 |

**Registry 不是双记账**：它是 WAL 的内存投影，启动时由 `ReplayHook` 从 `e/n/d` 事件流确定性重建 —— WAL 即持久化形态。`MarkReady` 对 Acked 的覆盖 **仅限显式 revive 管理动作**。

### 3.6 后台巡查员（StartSweeper，每 1000ms）

```
① SweepAll()         锁定区逾期回收（业务状态变更 → 事件流 → Quorum 复制）
② CompactAllChecks() 压缩巡查：每节点扫自己磁盘
     段数 ≥ 4 ／（AckedRatio≥0.5 且 (行数≥128 或 段龄>1h)）
     → LogCleaner：活账快照 → c{..}.compacted.tmp → File.Move 原子上位 → ReplaceSegments+删旧段
     崩溃矩阵：tmp 孤儿（启动清扫）/ Move 断电 / 上位后旧段未删（回放消重）/ Registry 指针过时（自愈）
```

---

## 四、HTTP API 总览

| 接口 | 方法 | 功能 |
|---|---|---|
| `/api/q/{queue}/messages` | POST | 发消息（body `ttlSeconds` 支持 TTL） |
| `/api/q/{queue}/receive` | POST | 领消息（长轮询 waitMs + visibilitySeconds） |
| `/api/q/{queue}/ack/{id}` | POST | 销账（幂等：重复 ack → 404） |
| `/api/q/{queue}/nack/{id}` | POST | 拒绝重试（retryCount+1，满 3 经 DLX 入 DLQ） |
| `/api/q/{queue}/dlq` | GET | 死信清单 |
| `/api/q/{queue}/dlq/{id}/ack` | POST | 死信人工删除 |
| `/api/q/{queue}/dlq/{id}/revive` | POST | 死信复活（计数清零） |
| `/api/ex/{exchange}/publish` | POST | 交换机投递（direct/fanout/topic 路由） |
| `/api/ex` | GET | 交换机绑定一览（4 台：orders/broadcast/alerts/dead） |
| `/api/stats` `/api/queues` | GET | 家底统计 |
| `/api/events` | GET | 事件流水（最近 200 条） |
| `/health` `/` | GET | 探活 / 健康看板 |
| `/api/cluster/status` `/api/cluster/replicate/{queue}` | GET/POST | 集群状态 / 复制接收 |
| `/api/raft/vote` `/api/raft/heartbeat` `/api/raft/status` | — | Raft 选举协议（投票/心跳/状态） |

所有业务写端点受 `IsWriter` 闸门保护（非 Raft 王位一律 503）——这是动态主权故障转移的接管点。

## 五、MqMessage 字段

| 字段 | 语义 |
|---|---|
| Id | Guid（发端重试会换新 Id → 跨重试幂等要靠业务 Key，见 3.5 第 4 层） |
| Crc | 内容 CRC（消费侧 `VerifyContent` 校验可二次验证） |
| Key | 路由键 + 业务幂等键（建议约定） |
| Content / Timestamp / ReceiveTime / ExpiresAtUtc / RetryCount | 生命周期字段 |

## 六、Road Future

- **catch-up 重同步**：follower 离线窗口内错过的事件，重启时向 leader 拉全量快照对账（目前唯一的恢复空洞）
- ConsumerGroup 分区消费 / 多 partition
- 动态绑定 API（`POST /api/ex/{ex}/bind`，DLX 重绑直达运维）
- 鉴权 / TLS
