# 自造消息队列 · 这次到底做了什么（术语 + 大白话）

> 目标：把"用 K8s 管我们消息队列的自动扩缩容"从想法变成能跑的东西。
> 术语依据：Raft 论文 + Kubernetes StatefulSet 设计

---

## 一句话版本

> 给自己的消息队列装上了 **"Raft 动态成员变更协议"** 和 **"K8s StatefulSet 管理三套部署"**，
> 让你用一个 `kubectl scale` 就能安全地加/减节点，崩溃了还能自动站起来。

下面按"先述结论 → 术语+大白话对照 → 每一步做了什么"的结构展开。

---

## 一、发明前的痛是什么？

**拷贝/rosa 严格版本的限制**（之前的问题），

之前 3 节点 raft 的成员表是**环境变量一次性塞进容器的**：

```
MQ_PEERS="http://node-1:8080,http://node-2:8080,http://node-3:8080"
 MQ  SELF = http://node-1:8080    ← 这个特别定制的"我是谁"
```

大白话：—— 这就相当于三个人**开学第一天那张班级表就定死**：
- 想加第 4 个同学？班级表上没有他，**没有一处能录入他** — 谁都不认识他。
- 想删第 3 个同学？也不行，班级表防送他进来。
- 所以：**集群威胁额度永远锁死 3 个**，k8s scale 加/减都没有意义。

生产级 Raft 怎么解决？**"改班级表这个动作本身也要走集群 majority 投票"** ——
把班级表当作一条 WAL 日志事件，Quorum 复制给所有成员。
这就是 **Raft 论文 5.1 的成员变更（membership change）**。
这次我们把它从 zero 到 one 的实现了。

---

## 二、做了什么（每个术语都有大白话版本）

### ① MembershipStore（成员账本）

**术语**：持久化成员存储（MStore），Append-Only Log，原子写。

```
data/_cluster/members.json
{
  "uptoSeq": 3,   ← 第 3 次成员变更
  "table": [
    {"node":"mq-0","url":"http://mq-0.mq-headless.default.svc.cluster.local:8080"},
    {"node":"mq-1","url":"http://mq-1.mq-headless.default.svc.cluster.local:8080"},
    {"node":"mq-2","url":"http://mq-2.mq-headless.default.svc.cluster.local:8080"}
  ]
}
```

大白话：每个节点把自己的**班级名单**写到磁盘。谁想加人/踢人，都要走 Quorum 投票 —— 而且通过 Quorum 复制给全员。

写入是**原子的**：先写 tmp 文件 → File.Move 一把吃掉（跟 WAL 其他身份账一样的套路）。
- 崩了 → 恢复到上一张名单（跟队列 WAL 的 `Log.Replay` 同族）
- Raft Leader sees _membership 表 → Writes cluster's King's table

### ② RaftNode 重写：从"死成员表"到"账本驱动的动态成员"

| 之前 | 现在 |
|---|---|
| peers 从 env 一次性读，死了 | members.json 回放 + 动态读 followers |
| Majority = `(peers.Length/2)+1` | = `(_members.Count/2)+1`（**跟着成员表漂移**） |
|kommen 只包含 3 台 | 全部跟成员表（add/remove 自即更新） |

大白话：Node 啊，你的 **"我知道的队友名单"** 变成了一份**账本能读着走的活白纸**。

### ③ Join（加人）+ Leave（踢人）协议

这部分是" rafts 集群让新节点加入/老节点退出"的 HTTP 通道（大白话 说法："新同侪/退休者跟 Leader 报名 → Leader 上报全班）。

```
     新 Node-4 写接口 ~ Raft Leader端
      │ POST /api/raft/join    { node:"node-4", url:"http://node-4:8080" }
      ▼
  1. Leader 校验：node name 唯一？URL 不冲突？成员 ≤9？
      如 node 已存在但 URL 相同 → 200 幂等 ✔（固定）
      如 URL 不一致            → 409 冲突
      集群满员（>9）           → 409 冲突
  2. 攒 m 事件（成员变更 = newTable）→走已有的 Quorum 三阶段流水线
      ① PrepareEvent       （在内存里造 m 记录）
      ② 广播 apply-membership 给其他的老成员（老成员的 majority ≤2 票就 OK）
      ③ Quorum 达标 → 各节点本地 persist 成员表 + 应用
      → Leader._membershipChanges（成员表历史）
  3. 老节点 Apply → 写成员表到磁盘
     新节点 side：签名（不需要 apply）→ 直接收到广播。
  4. Leader 把 Syncing 成员标记为 false 状态 -> 直到 SyncingGate 转正
```

大白话：
- **join** 是"这家人想加入董事会，找 Leader 报名 → 报表挨个发给现有成员投票 → 达成共识才写进"合同"。
- **leave** 同理 —— 但被踢的人不需要表决（它只有被通知的份）。

### ④ Syncing 闸门（新节点的“血账一致后才给权限”）

Raft 的一个陷阱：**空账的新节点如果给它投票权，它有可能"当选 Leader"**（比如在 1 人集群里它自己就能凑齐 1/2+1）→ 集群会裂开。

**术语叫**：Vote Election Restriction / Readiness Gate。
**大白话**：新人在没学完业务前不给开会资格。

实现方法：给节点发一个 `_synced=false` 标志：
- safe：不竞选、不心跳、不参与 quorum 票数
- 直到它拿到集群快照、走完 catch-up 才 `MarkSynced(true)` → 获得完整成员权利。

这样我们也顺带解决了 a **“同步进程不需要 Quorum 亲眼看到我”** 的问题（否则新人没到场就触发了 quorum 计数，写请求会 503 卡死）。

### ⑤ Catch-up 快照（历史数据同步）

术语：Raft InstallSnapshot / Cluster Catch-up / Log rebuild
大白话：**"先把新人送到补习班里把账补齐"**。

为什么需要？node-4 是_empty WAL 启动的。
- Leader 可能已在 Seq=10000+ 上了
- 新节点 join 后收到的是 Seq=10001… 的新事件
- **前 1~10000 的账目全部没有！**（这就是"空洞节点"）
→ 如果它参了 Quorum 投票，就能"以空账当选 Leader" → **大坑**

流程：
```
① node-4 POST /api/raft/join          → Leader 已连入 Quorum 后
② node-4 GET  /api/raft/snapshot       ← Leader 返回 6 个账本快照
③ hub.ApplySnapshot(snap)             → 底层分：
   - Log.LoadSnapshot(items, uptoSeq)  → 清段文件、重建、写快照单段
   - QueueCore.RestoreFromSnapshot     →（_ready 清空 → 事件重撫帰集群）
④ RaftNode.MarkSynced(true)            → 闸门开启，正式入列
```

小坑修了 2 次：
- **ValueTuple 序列化**： `List<(long, MqMessage)>` 的 JSON `.NET` 变 `{"":...}`，加了一层 `SnapshotItem`（显式 record）
- **大小写**： Leader 返 camelCase "queues"，RaftNode 反序列化默认大小写敏感 → 节点拿到的全 null。修复 = `PropertyNameCaseInsensitive: true`

### ⑥ GracefulOfflineAsync（优雅下线）

术语：Raft Leave / Graceful Shutdown / Pod preStop 协调

大白话：**"你还不能现在关机！先让集群知道你要走了，交接好任务。"**

具体分成两条路：

```
Leader 下线（gracefulShutdown）:
  · 我是 mq-0 → LeaveMemberAsync("mq-0")
   → m 事件走 Quorum（delete 自己 + 剩余 2 人 quorum）
   → 提交完成 → 自我 _leaving=true，role 回 Follower，停心跳
   → 剩余成员在 2~4.5s 内选出新王
   → sleep 3s  给新王就位时间 → 退出

Follower 下线:
  · 我要知道当前 Leader URL → 请求 leader /api/raft/leave {node: myself}
   → Leader Quorum 结成 → remove 自己 → 频段回复 200
   → 退出
```

**为何用 SIGTERM，而不是 K8s preStop 钩子？**
- 我们容器的 `mq-lab:18` 里没有 wget/curl — 前 Stop 钩子 exec 命令直接 127 静默失败
- 改用 SIGTERM: `ApplicationStopping` = 容器收到 SIGTERM 自动触发，走到 .NET 的 lifecycle → 留下 leave
- K8s 给 20 second gracePeriodSeconds，够放 3s 选举 + leave + snapshot 传播

### ⑦ RaftNode.write-ready 探针

术语：Kubernetes readinessProbe
大白话：“服务器上贴了个**门牌：谁在当 leader？** → 如果非 leader 领域，时刻偷偷翻转”

```
GET /api/raft/write-ready
    200 ← 我是 Leader，可以写
    503 ← 我是 Follower，不接写
```

K8s 的 readinessProbe 用这条 → Service "mq-write" 的 Endpoints 自动获取 **谁的 200 通过**谁就当 King（每 3 秒的探针周期）
业务永远只打到 King，**换主零配置**（不用改 DNS、不用改 Ingress —— K8s 自动）。

### ⑧ K8s StatefulSet 落地

大白话：不是一个 Deployment（它的 Pod 是无序可替换、动态 IP），而是 **StatefulSet**（Pod 有稳定身份 + 独立 PVC 卷，类似 “mq-0/mq-1/mq-2 有序命名”）

```yaml
apiVersion: apps/v1
kind: StatefulSet
spec:
  podManagementPolicy: Parallel           # 所有 Pod 并行启动（不 OrderedReady 等第一个 Ready）
  serviceName: mq-headless                # 稳定 DNS 头 mq-0.mq-headless。
  replicas: 3
  volumeClaimTemplates: [wal: 1Gi]        # 各自独立账本
  template:
    containers:
      env:
        MQ_NODE_NAME: $(POD_NAME)          # Raft 的 node 名 = Pod 名（dynamic 主权）
        MQ_SELF: http://$(POD_NAME).mq-headless:8080
        MQ_PEERS: 初始 3 员表（新 Pod join 时不用）
        MQ_LEADER_URL: http://mq-write.default.svc:8080  ← 用 K8s write Service 自动指 King
```

**三个坑（认真记下）**：
| 坑 | 为什么 | 修法 |
|---|---|---|
| headless Service 只收 Ready 的 Pod | Follower 没 Ready → DNS 查不到自己兄弟 → 选主死锁 | `publishNotReadyAddresses: true` |
| podManagementPolicy: OrderedReady | 第 2/3 Pod 等 Pod1 Ready → Pod1 是 Leader 前 Ready 不 True → 谁也起不来 | `Parallel` |
| 镜像在 K8s containerd 不看 docker 镜像 | kind 的 k8s 自己一个 containerd namespace | save→cp→ctr import 手动导 |

### ⑦ Quorum 数学（扩容后怎么“算票"？）

```
members.Count = 4 写：
  WriteMajority   = (4/2) + 1 = 3     ← 要 ≥3 票（Leader 自己 1 票 + 2 follower 才够）
  ReplicationTargets = 3 个 follower 的 URL（PublishTargets）
  ackCount           = leader-self(1) + follower(3) = 4  (准就 201；不足退 503 重试)

 Quorum 算法（newTable majority）：
  join: old majority(3 members /2+1 = 2 票) + (newNode 1 票) = 3
  leave：old majority 2 票 + (排除被离去 node)  = 3 票

要素一致性： "Ring Lock"（一次只能 1 个成员变更，配 His _membershipChangeInProgress = 1）
  · __ +1 join +1 leave：每次改要等上一次 commit
  · 防止两次变更交叠 → old config 与 new config 的 quorum 集合不同 → 撞车脑裂
```

---

## 三、端到端验证（K8s live 集群里 3→4→3 全链路）

```
  ① 3 节点 K8s 集群入口：
     kubectl apply -f k8s/...
     [mq-0] Leader / [mq-1, mq-2] Follower · (readinessProbe 探到 write-ready)
  ② Quorum writing：
      POST → mq-write (K8s Service) → ack=3/3    ✓
      （K8s自动把业务打到 Leader，不是 follower）
  ③ scale 扩容 3→4：
      `kubectl scale --replicas=4 statefulset/mq`
      node mq-3 启动（join-mode 上牌）→ join RPC → 全员 Quorum merged
      → Snapshot 追赶 (6 账本) → sync-ack → ready → ack=4/4
      member table 一致性 seq=2 in `_cluster/members.json` × 4 nodes
  ④ scale 小部 4→3：
      node mq-3 的 SIGTERM → graceful leave
      → Leader 的 cmd Out: `Quorum 通过` →  剩下 3 人
      → mq-1/mq-2 raft memberCount = 3, ack=3/3
  ⑤ 大白话一句话：
      你只需要一个 `kubectl scale` 命令告诉 K8s“要 4 个节点”；
      剩下的一切（Raft 选王、join 协议、快照补账、write-readiness 跟王走、优雅下线）
      全是我们自己写的消息队列自己做的。
```

---

## 四、Commit 列表 & 收益

```
d59656e feat: Raft 动态成员 + catch-up 快照 + write-ready 探针（阶段 C 第 1 步）
2389ad4 feat: k8s 落地（StatefulSet/headless/mq-write）+ join-catchup 补丁
4275b48 feat: 优雅下线 RaftNode.GracefulOfflineAsync（SIGTERM → leave → 剩余成员重选王）
24/24 单测 green
```

**大白话总结**:
- 我们给消息队列装了第四条腿：“**动态成员变换**了”—— 以前想扩就得“删掉所有数据卷+手动销毁全部重来”；现在“**一个kubectl scale 就达成两份正统 Mahout（扩 / 缩）**的梦想。”

---

## 五、诚实清点已知 edge cases（这次没做好的）

1. **Replication 字段 `publishNotReadyAddresses`**： 不仅是 headless 主操作对，还有生产上 ingress 切可能需要新的 readiness广泛　方案。
2. **join 期间有 K8s leader 换人**（edge case）： join RPC 打到旧 leader 上 → 503 拒（Follower 不处理）→ 新节点自动跳到新 Leader 重试 —— OK but maybe 一次性
3. **缩容时 (4→3) 剩余跟随者 NotReady**：因为非 Leader `write-ready=503` — 但这是设计（所有写走 King），我们在这里用 raft来授予 acks，我确认这是by design，已经验证 3 人 Quorum 全立
4. ** Leave（优雅下线）应该在消息"拷贝五营地新Get member 下一部" 前先给 leader** 一次性"快速的：“先 ack 再 kill”（给 3s 缓冲），每一个生产 env 都建议 > 20s 的 K8s gracePeriod 建议值
5. **Join 次序限流**：一个 join 正在处理中，另一个又 join → 409 "已有成员变更进行中（±1）" → K8s 在多次 scale 时不连续加人 → 运维得一点一点做
6. **旧的 node-4 已离开但再次启动**：成员表只能从新的 join 走一（教学简化版，可以做到 remove 时自动自动)
7. **Aurora split brain 风险**： raft数据 /_cluster/members.json 为 atomic 写入。不依赖于 ZooKeeper/etcd，避免第三方依赖

---

## 六、大白话总收

```
你拿到的是：一个能让你用 `kubectl scale --replicas=N` 这一条命令
             就能**安全扩 / 缩集群**的消息队列——
哪些大类都要写 raft 的 join/leave 快照 catch-up
         但你不用管：
    · 决定谁是王（Raft 自动）
    · 消息同步给新节点（catch-up 自动）
    · 节点挂了怎么自动恢复/下线（GracefulOfflineAsync 自动）
    · 业务流量跟新王（write-ready readinessProbe 自动）

它就像你租了一间出租屋，K8s 只是物业，我们自己写的消息队列
才是"关起门做自己家务"的女主人。
```
