# 自造消息队列 · 架构图

> v4.0 · 动态主权 Raft + Quorum 三阶段 + DLX + 幂等查重 + 后台巡查

---

## 1. 一图总览（单节点数据面 + 控制面）

```
┌──────────────────────────── ASP.NET 容器 (mq-lab) ─────────────────────────────┐
│                                                                                │
│   生产者                  消费者                          运维/看板              │
│     │ POST /messages        │ POST /receive                      │  GET /      │
│     │ (body/{ttlSeconds})   │ POST /ack  /nack                    │ GET /api/*  │
│     ▼            ▼                                              ▼              │
│  ┌──────────── Web/Endpoints（Queue · Exchange · DeadLetter · Monitoring）────┐ │
│  │   写闸门：ClusterOptions.IsWriter  ←─ 非 Raft 王位一律 503                │ │
│  └──────────────────────────────┬────────────────────────────────────────────┘ │
│                                 ▼                                              │
│                    Core\MessageQueueHub（枢纽）                                 │
│                    ├── _queues  队列池（磁盘目录 = 身份，启动磁盘认领）            │
│                    ├── _exchanges 交换机表（data/_exchanges.json 快照持久化）      │
│                    ├── DeliverDead() 死信路由执行器                              │
│                    ├── ReplicateQueueEventAsync() Quorum 复制                   │
│                    └── PushEvent() 事件流水（看板消费）                           │
│                                 │                                              │
│        ┌────────────────────────┼─────────────────────────┐                    │
│        ▼                        ▼                         ▼                    │
│  QueueCore("normal")     QueueCore("vip")          QueueCore("error") …        │
│  （动态 POST /api/q/{queue} 可声明，主账+死信账双人套件）                          │
│   ├── _ready   就绪墙 (FIFO ConcurrentQueue)                                    │
│   ├── _locked  锁定区（visibilitySeconds 租期）                                  │
│   ├── _dead    死信清单 + _dlqLedger（{queue}.dlq 独立分段 WAL）                  │
│   └── _ledger  主账本（见下方 WAL 家族）                                          │
│                                                                                │
│  ┌── Persistence（WAL 全家）─────────────────────────────────────────────────┐  │
│  │  LogMessage            crc|json 信封（行级 CRC + 消息级 CRC）              │  │
│  │  LogSegment            三态 Segment（Active→Sealed→Compacted）+ Ack 计数   │  │
│  │  Log                   段清单 + Replay（事件重放 + Registry 重建）          │  │
│  │    _state              活账 (id → (Seq, Msg))                             │  │
│  │  LogCleaner            压缩执行：tmp → File.Move 原子上位 → Replace        │  │
│  │  MessageRegistry       全局映射表（WAL 重建，非第二记账）                    │  │
│  └───────────────────────────────────────────────────────────────────────────┘  │
│                                                                                │
│  ┌── 后台巡查员 StartSweeper（每 1000ms，防弹衣循环）────────────────────────┐    │
│  │  ① SweepAll()            锁定区逾期回收（业务事件 → 复制）                 │    │
│  │  ② CompactAllChecks()    压缩巡查（每节点自扫）：≥4 段 / 50% 脏率            │    │
│  └───────────────────────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────────────────────┘
```

## 2. 消息生命周期（含 DLX 死信链）

```
Producer ──① Prepare(内存造事件)──▶ ② ReplicateAsync ──▶ ③ CommitEvent
                                       │ 多数派 ack          │
                                       │                     │ WAL(append)→ready
                       凑不齐 → 503 ◀──┘ (必须)─→ 全集群零残留  │             ▼
                                                     ┌────────────────────┐
Consumer GET /receive（长轮询 waitMs）                 │  幂等拦截           │
        │ TryDequeue                                  │  Registry.Acked? → 丢│
        │                                             │  TTL 过期? → ☠️ 死信 │
        ▼                                             └────────────┬───────┘
  Registry.MarkInFlight                                              │
        ▼                                                            │
 ┌── 消费者的三种命运 ──┐                                              │
 ├─ Ack   → d 事件 → 销账 ✅                            账本真相不变 ◀─┘
 ├─ Nack  → n 事件 → RetryCount+1 回墙 🔁 (满 3 ↓)
 └─ 失联  → 巡查员回收 → n 事件 🔁
        │
        ▼ RetryCount ≥ 3
 DLX「dead」topic 交换机   routingKey = "dead.{来源队列}"
        │ Binding.Matches 通配匹配
        ├── dead.normal → normal（AcceptDeadLetter 进 normal.dlq 账本）
        ├── dead.vip / dead.error …（可重绑：POST /api/ex/{ex}/bind）
        └── 没有绑定命中 → 🌡️ 回退自投自家 DLQ
        ▼
 死信货架：revive（retryCount=0 复活）/ dlq-ack（销尸）
```

## 3. 集群控制面（动态主权）

```
 ┌─────────────────── docker-compose 三节点 ──────────────────────────┐
 │                                                                    │
 │  MQ_PEERS = 全员表（3 员）      MQ_SELF = 自剥身份证                 │
 │                                                                    │
 │  node-1 (:5081)   RaftNode[term 心跳/选举]     磁盘 WAL 卷 1         │
 │  node-2 (:5082)   RaftNode[clone]             磁盘 WAL 卷 2         │
 │  node-3 (:5083)   RaftNode[现王 role=Leader]   磁盘 WAL 卷 3         │
 │                                                                    │
 │  选王协议（RaftNode.cs）：                                          │
 │    Follower 随机 2~4.5s 超时 → Candidate → 三路招票（term++）        │
 │    HandleVote：旧任期拒 / 更高任期收养+清票 / 同任期一票            │
 │    3/3 票 → 👑 Leader → 心跳广播（不自投）                          │
 │                                                                    │
 │  王位联动（ClusterOptions.IsWriter / ReplicationTargets）：          │
 │    · 写闸门：IsWriter ⇒ (Raft.Role == Leader)                       │
 │    · 复制目的地 = MQ_PEERS - MQ_SELF                                │
 │    · 复制流接收 = 非现任王（老王回归自动降级后继续接复制）            │
 │                                                                    │
 │  杀王实录：≤12s 新王 elected + 写权限跟王走；旧王回头 → Follower      │
 └────────────────────────────────────────────────────────────────────┘
```

## 4. Quorum 三阶段写（因果链铁律）

```
   Producer
      │ POST /messages
      ▼
  ┌──────────┐     ┌────────────────────┐
  │ ① Prepare │     │  事件只存在于内存    │
  │ (无落盘)  │     └──────────┬─────────┘
  └──────────┘                 │
                               │ 广播复制
                       ┌───────▼──────────┐
                       │ ② Quorum         │
                       │  ackCount ≥ 2/3  │
                       └──────┬───────────┘
              达标 ✔           │           未达标 ✗
      ┌────────────────────┐  └──────────────┐
      │ ③ CommitEvent       │          ┌──────▼──────────┐
      │   WAL(append-e)     │          │ 503 重试         │
      │   ready 入墙        │          │ 全集群零残留      │
      │   Registry 挂账     │          │ （天然无垃圾）    │
      └──────────┬──────────┘          └─────────────────┘
                 ▼
              201 Created
```


## 5. WAL 家族分段图（磁盘组成）

```
data/{queue}/                        data/{queue}.dlq/               data/
  s000001.jsonl                                                       _exchanges.json
  s000002.jsonl      ◀── 满行 512 即封段                                （绑定表快照）
  s000003.jsonl
  c1-4.compacted.jsonl      ◀── 压缩产物（只含活账快照）
  s00000N.jsonl     ← 活跃段：Append-Only，永不参与压缩
```

---

## 一图收束

```
消息（双层 CRC WAL          队列（磁盘目录              交换机（json 快照
     + DLQ 同款）              认领重建）                报表可变）
─────────────────────────────────────────────────────────────────────
 Quorum 多数派投票 王位跟 Raft 真王位（IsWriter 才写）
 压缩 = 后台巡查 GC 幂等 = WAL 回放重建映射表
```
