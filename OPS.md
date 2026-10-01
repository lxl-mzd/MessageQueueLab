# 自造消息队列 · 部署与运维手册（OPS.md）

> 适用版本：mq-lab v5+（动态成员 Raft / watermark+ISR 掉队回放 / K8s StatefulSet 部署）
> 读者：部署这套消息队列的运维 / 兼值运营 / 微服务接入同学
>
> 相关文档：`README.md`（定位）· `ARCHITECTURE.md`（架构图）· `DESIGN.md`（设计全文）· `API.md`（接口）· `K8S_SCALE.md`（K8s 扩缩容流程）

---

## 0. 三分钟速查

```bash
# 单机（有 docker 即起）
docker run -d --name mq-server -p 5000:8080 -v mqlab-data:/app/data \
    ghcr.io/lxl-mzd/messagequeue-lab:latest
curl http://localhost:5000/health

# 三节点 compose（生产形态）
git clone https://github.com/lxl-mzd/MessageQueueLab.git
cd MessageQueueLab && docker compose up -d --build

# k8s（StatefulSet 部署）
kubectl apply -f k8s/mq-headless-svc.yaml
kubectl apply -f k8s/mq-write-svc.yaml
kubectl apply -f k8s/mq-statefulset.yaml

# 日常巡检三连
docker compose ps
curl http://127.0.0.1:5081/api/raft/status
curl http://127.0.0.1:5081/api/queues
```

---

## 1. 部署（三种形态）

### 1.1 形态抉择

| 形态 | 场景 | 特征 |
|---|---|---|
| **单节点**（`MQ_ROLE=single`） | 开发 / 测试 / 业务量小 | 无 Raft |
| **docker-compose 三节点** | 正式微服务链路（小团队） | 每节点独立卷，Raft 动态成员自动起用 |
| **K8s StatefulSet** | 需要自动扩缩容 / 复杂网络/节点迁移 | 配合 `kubectl scale --replicas=N` 做动态集群 |

> K8s 部署请配合 Kafka 风格的 `mq-headless`（稳定 DNS）+ `mq-write`（readiness 过滤王）Service —— 详见 `K8S_SCALE.md`。

### 1.2 单机部署

```bash
docker run -d --name mq-server \
    -p 5000:8080 \
    -v mqlab-data:/app/data \
    -e MQ_ROLE=single \
    --restart unless-stopped \
    ghcr.io/lxl-mzd/messagequeue-lab:latest
```

- `-v` 数据卷：**含 WAL + 死信账本 + 交换机快照 + Raft 成员表**（一个都不能少）

### 1.3 docker-compose 三节点

```bash
git clone https://github.com/lxl-mzd/MessageQueueLab.git
cd MessageQueueLab && docker compose up -d --build
```

配置表（`docker-compose.yml` 中）：
| 变量 | 值 | 出错后果 |
|---|---|---|
| `MQ_PEERS` | `http://node-1:8080,http://node-2:8080,http://node-3:8080` | 少一个 → 脑裂双王 |
| `MQ_SELF` | 每台指向自身 | Leader 自投心跳自降 → 反复重选 |
| `MQ_NODE_NAME` | node-1/2/3 | 日志看板分机识别 |
| `MQ_ROLE` | leader/follower | 动态主权下只影响首写入口，王位接管后以 Raft 王为准 |

### 1.4 K8s 部署（StatefulSet）

```bash
kubectl apply -f k8s/mq-headless-svc.yaml
kubectl apply -f k8s/mq-write-svc.yaml
kubectl apply -f k8s/mq-statefulset.yaml
```

部署细节（`k8s/mq-statefulset.yaml` 内）—— 三个关键开关，缺一个就跑不起来：

1. **PVC 逐 Pod 独立挂载**（`volumeClaimTemplates`）—— Pod 重启自动绑回自己的卷
2. `podManagementPolicy: Parallel` —— 防止 OrderedReady 模式下"#
   第一个 Pod NotReady → 后面的 Pod 检查不通过永远不建"
3. `publishNotReadyAddresses: true`（headless Service 里）—— 否则 3 follower 都 NotReady（无王）时互 ping 不通

readinessProbe 指向 `/api/raft/write-ready`（`mq-write` Service 自动把流量跟王走）。

---

## 2. 运维

### 2.1 每日巡检（5 项）

```bash
for port in 5081 5082 5083; do
  curl -s http://localhost:$port/health
  curl -s http://localhost:$port/api/raft/status
  curl -s http://localhost:$port/api/queues
done
curl -s http://localhost:5081/api/events
```

判定健康：
- **恰好一个 Leader**（"role":"Leader"只出现一次）
- term 三节点相同或±1（选举无频繁抖动）
- `pendingOnDisk` 三节点差距 ≤10（复制无积压）
- `deadLetters` 不持续上升（下游没挂）
- WAL 磁盘 <80%（WAL 压缩兜不住磁盘满）

### 2.2 告警阈值（建议抄监控）

1. 任一节点 `/health` >30s 无响应 → P1
2. `leaderId` 30 秒内翻转 2 次 → P1（选主抖动）
3. WAL 磁盘 >80% → P0
4. `deadLetters` >10/分钟 → P2（下游挂了）
5. `pendingOnDisk` 各节点差 >100 → P2（复制积压）
6. `members` 表出现重复 node 名 → P1（脑裂）

### 2.3 运维动作五连

```bash
# 死信复活
curl -X POST http://<master>:5000/api/q/audit/dlq/<id>/revive     # retryCount 归 0
# 死信销账
curl -X POST http://<master>:5000/api/q/audit/dlq/<id>/ack
# 动态建队
curl -X POST http://<master>:5000/api/q/audit
# 改绑定（持久化 _exchanges.json）
curl -X POST http://<master>:5000/api/ex/dead/bind?pattern=dead.urgent&queue=error
curl -X POST http://<master>:5000/api/ex/dead/unbind?pattern=dead.urgent&queue=error
```

幂等日志说明：`🧊 幂等拦截` 是**正常防护动作**（同 id 重复出现被挡回），不是故障。

---

## 3. 备份与灾难恢复

### 3.1 每日备份

```bash
docker run --rm -v mqlab-leader:/data -v $PWD/backup:/backup alpine \
    tar czf /backup/mqlab-leader-$(date +%F).tar.gz -C /data .
```

### 3.2 空机恢复（从 TAR 还原）

```bash
mkdir -p $PWD/data
tar xzf mqlab-leader-2026-09-24.tar.gz -C $PWD/data

docker run -d --name mq-server -p 5000:8080 -v $PWD/data:/app/data \
    -e MQ_ROLE=single \
    ghcr.io/lxl-mzd/messagequeue-lab:latest
# 启动日志：📇 磁盘认领 audit / ... 队列（目录在即挂账）
```

### 3.3 数据卷结构（备份会拷走的东西）

```
/app/data/
├── _cluster/
│   └── members.json          ★ Raft 成员表持久化（动态主权）
├── _exchanges.json           ★ 交换机/绑定快照
├── normal/
│   ├── s000001.jsonl …       ★ WAL 分段
│   └── c1-4.compacted.jsonl  ★ WAL 压缩产物
├── normal.dlq/               死信账本
├── vip/ vip.dlq/
└── error/ error.dlq/
```

---

## 4. 扩容 / 缩容（K8s StatefulSet）

### 4.1 扩容（3 → 4）

```bash
kubectl scale statefulset mq --replicas=4
kubectl logs mq-3 -f    # 应出现：
#   🧪 [raft:mq-3] join 模式：空成员表启动
#   📇 [membership] seq=2 成员表更新 mq-0..mq-3
#   🧪 [snapshot] 应用集群快照：6 个账本已追赶
#   🎉 [raft:mq-3] join + catch-up 完成 —— 正式入列
```

**注意**：mq-3 在 `synced=false`（catch-up 未完成）时：
- 不参与 Raft 投票（vote RPC 返回 false）
- 不参与写 Quorum（`PublishTargets` 自动排除）
- write-ready = 503，Pod NotReady（业务流量不会打到它）

catch-up 完成 `synced=true` → Pod Ready → 正式可接请求。

### 4.2 缩容（4 → 3）

```bash
kubectl scale statefulset mq --replicas=3
# StatefulSet 杀最高序号的 Pod（mq-3）→ 发 SIGTERM
# .NET ApplicationStopping → GracefulOfflineAsync
#   Follower：向 Leader 发 leave → Leader Quorum 重写成员表（去 mq-3）→ mq-3 自退出
#   Leader（若被杀节点是自己）：m 事件去自己 → 剩余成员重选王 → 退出
```

### 4.3 扩缩容的票数数学

| 集群规模 | 写 Quorum 需要的 `ackCount` |
|---|---|
| 3 节点 | 2 票 |
| 4 节点 | 3 票 |
| 5 节点 | 3 票 |

公式：`(floor(已同步者数/2) + 1)`；未追平的成员 **不计票**（Stage: `_syncFlags[node] == false`）。

---

## 5. 故障排查速查表

| 症状 | 最可能原因 | 处理 |
|---|---|---|
| 全部节点写 503 | Raft 选不出王 | 查 `MQ_PEERS` 或网络；30~60 秒内自动收敛 |
| 发现 2 个 Leader | 脑裂 | 查 `MQ_PEERS` 是否所有人一致；重启受影响节点 |
| 复制滞后 pendingOnDisk 差 >100 | follower 掉线 delta replay 没跟上 | `/api/raft/members` 查 synced；查 MW PEERS 网络 |
| 死信连续上升 | 下游业务处理挂 | 抄 2.3 复活 / 销账；或手动 kill 下游 |
| 日志"🧊 幂等拦截" | 消费者重复/ttl 幽灵 | 正常防护动作（不是故障） |
| m event 循环 | 串行变更锁未释放 | 重启受影响节点 |

---

## 6. CI/CD 与版本

- 每次 main push：**24 项单测** + **三节点 e2e 冒烟**（Raft / Quorum / DLX / 杀王接管）
- 全绿后推 GHCR：`ghcr.io/lxl-mzd/messagequeue-lab:{latest, <sha>}`
- 升级：`docker compose pull && up -d`（或指定 sha 的镜像）

---

## 一句话总结

```
正常运行：什么都不用管
节点宕机：Raft 自动切王；回头的节点 join+catch-up 自愈
磁盘毁掉：从备份恢复（data/ 卷整体是真相）
告警：   2.2 的五条，齐了
```

欢迎提 issue。
