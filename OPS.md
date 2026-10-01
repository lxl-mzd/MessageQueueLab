# 自造消息队列 · 部署与运维手册（OPS.md v6）

> 适用版本：mq-lab v6+（SDK 自动找王 / follower 纯副本 / join 竞态修复 / K8s 常驻）
> 读者：部署运维 / 值班 / 微服务接入同学
>
> 相关文档：`README.md`（定位）· `ARCHITECTURE.md`（架构图）· `DESIGN.md`（设计）
> · `API.md`（接口）· `K8S_SCALE.md`（K8s 扩缩容流程）· `ENV.md`（环境清单）

---

## 0. 三分钟速查

```bash
# 单机（有 docker 即起）
docker run -d --name mq-server -p 5000:8080 -v mqlab-data:/app/data \
    ghcr.io/lxl-mzd/messagequeue-lab:latest
curl http://localhost:5000/health

# 三节点 compose（小团队生产形态）
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
| **单机**（`MQ_ROLE` 省略即单机） | 开发 / 测试 / 业务量小 | 无 Raft，开箱即用 |
| **docker-compose 三节点** | 正式微服务链路（小团队） | 每节点独立卷，Raft 动态成员 |
| **K8s StatefulSet**（当前常驻形态） | 需要 `kubectl scale` 扩缩容 | headless + write 双 Service |

单机最低环境：**只要有 Docker**（.NET SDK、代码仓库都不需要，运行时在镜像里），约 400MB 磁盘，一个空闲端口。

### 1.2 单机部署

```bash
docker run -d --name mq-server \
    -p 5000:8080 \
    -v mqlab-data:/app/data \
    --restart unless-stopped \
    ghcr.io/lxl-mzd/messagequeue-lab:latest
```

- 不传 `-e MQ_ROLE` 即单机（代码默认 `single`）。
- `-v` 数据卷必备：WAL + 死信账本 + 交换机快照 + Raft 成员表全在里面。

### 1.3 docker-compose 三节点

关键环境变量：

| 变量 | 说明 | 配错后果 |
|---|---|---|
| `MQ_PEERS` | Raft 全员表（每节点写全，含自身） | 少一个 → 脑裂双王 |
| `MQ_SELF` | 本节点对外地址 | Leader 自投心跳自降 → 反复重选 |
| `MQ_NODE_NAME` | 节点名（node-1/2/3） | 仅显示 |
| `MQ_ROLE` | leader/follower（初始声明） | 只影响首写入口；Raft 定身后以王为准 |

### 1.4 K8s 部署（StatefulSet）

三个关键开关（缺一个就起不来，全部真实踩过）：

1. **headless Service 必须 `publishNotReadyAddresses: true`** —— 否则 follower 全 NotReady 时互相解析不到，永远选不出王。
2. **`podManagementPolicy: Parallel`** —— 否则 OrderedReady 下第一个 Pod 不 Ready，后面 Pod 永不创建。
3. **`persistentVolumeClaimRetentionPolicy: whenScaled/whenDeleted: Delete`** —— 否则缩容残留旧 PVC，下次扩容拿到僵尸成员表自立为王（脑裂，见 §5.4）。

镜像：`mq-lab:18`（本地构建 → `ctr import` 进 K8s containerd，`imagePullPolicy: IfNotPresent`）。

---

## 2. 客户端接入：只找 Leader（SDK 全自动）

### 2.1 规则（一句话）

```
读（queues/health/看板/事件流）：任意节点都行。
收发消息（receive/send/ack/nack/死信处置/publish/bind/建队）：只打 Leader，否则 503。
    follower 是纯副本：它的锁只活在自己内存，receive 到它手里也销不了账，所以直接拒收。
```

**不要给所有节点发请求** —— 写只能打一个（王）。

### 2.2 SDK 用法（用户只管 .Send）

```csharp
var config = new SefMqConfig {
    // 种子节点表：写 1 个能活着的就行，不需要写全，更不需要随扩容改代码
    ["bootstrap.servers"] = "http://h1:5081,http://h2:5082,http://h3:5083"
};
var producer = new SefMqProducer(config);
await producer.SendAsync(new SefMqRecord("normal", "k1", "hello"));  // 不用管谁是王

var consumer = new SefMqConsumer(config);
consumer.Subscribe("normal");
await consumer.BeginConsume(async (msg, ctx) => { /* 正常返回=自动 Ack */ });
```

SDK 内部机制（`SefMqClusterClient`）：

| 机制 | 说明 |
|---|---|
| 种子轮询 + 503 换王 | 第一次按种子顺序试，503/连不上就换下一个 |
| 内存缓存 Leader | 打中谁就记住，下次直达，少一次试探 |
| 成员表自学习 | 每次绕路才找到王，就顺手向王要最新成员表（`/api/raft/members`），新节点 URL 自动并入种子 —— **扩容后 SDK 不用改配置** |
| 旧配置兼容 | `bootstrap.url` 单地址照样能用 |

### 2.3 K8s 下更省事：直接打 Service 名

```
http://mq-write:8080/api/q/normal/messages
```

`mq-write` 的 readiness 探针是 `/api/raft/write-ready`（仅 Leader 200），K8s 自动把流量只导给当王的 Pod。王漂移时 endpoint 自动切换，客户端无感。

---

## 3. 日常运维

### 3.1 巡检五项

| 检查项 | 方法 | 健康值 |
|---|---|---|
| 探活 | `GET /health` | 200 alive |
| 王位 | `GET /api/raft/status` × N | 恰好 1 个 Leader；term 一致或 ±1 |
| 复制同步 | `GET /api/queues` × N | `pendingOnDisk` 差距 ≤10 |
| 死信 | `deadLetters` | 增长 <10 条/分钟 |
| 磁盘 | data 卷 | <80% |

看板：`http://<王>:8080/`（单机 `:5000/`）。节点卡**自动发现**：每次刷新先读成员表按 `mq-N→5081+N` / `node-N→5080+N` 算端口，新节点自动出现、删除自动消失；拿不到名单时回退手动输入框。

### 3.2 告警阈值

1. 任一节点 `/health` >30s 无响应 → P1
2. `leaderId` 30s 内翻转 ≥2 次 → P1（选主抖动）
3. data 卷 >80% → P0
4. `deadLetters` >10/分钟 → P2（下游挂了）
5. `pendingOnDisk` 节点差 >100 → P2（复制积压）
6. 成员表出现重复 node 名 → P1（脑裂前兆）

### 3.3 运维动作

```bash
# 死信复活 / 删除
curl -X POST http://<王>:8080/api/q/<q>/dlq/<id>/revive     # retryCount 归 0
curl -X POST http://<王>:8080/api/q/<q>/dlq/<id>/ack
# 动态建队 / 改绑定（持久化，重启不掉）
curl -X POST http://<王>:8080/api/q/audit
curl -X POST "http://<王>:8080/api/ex/dead/bind?pattern=dead.urgent&queue=error"
```

`🧊 幂等拦截` 日志是正常防护（同 id 幽灵被挡），不是故障。

---

## 4. 扩容 / 缩容

### 4.1 docker-compose 加节点（手动一步）

```bash
docker run -d --name node-4 --network messagequeuelab_default -p 5084:8080 \
  -e MQ_NODE_NAME=node-4 -e MQ_SELF=http://node-4:8080 -e MQ_PEERS= \
  -e MQ_ROLE=leader -e MQ_LEADER_URL=http://node-1:8080 mq-lab:18
# 全自动：join → 成员表广播 → catch-up 快照 → sync-ack → 入列
```

### 4.2 K8s 扩缩容（一行命令）

```bash
kubectl scale statefulset mq --replicas=4   # 新 Pod 自动 join+追平
kubectl scale statefulset mq --replicas=3   # SIGTERM → 优雅下线 → 成员表自动去掉
```

扩容时新节点三闸门（未追平前）：不竞选、不投票、write-ready 503。追平后自动转正。

缩容时被杀节点：SIGTERM → `GracefulOfflineAsync`（Follower 向王发 leave；是王就自己走 leave 并轮询确认新王）→ 退出。剩余节点 Quorum 自动收缩（4→3 票）。

### 4.3 票数数学

| 规模 | 写 Quorum（ack 数） |
|---|---|
| 3 节点 | 2 |
| 4 节点 | 3 |
| 5 节点 | 3 |

公式 `floor(N/2)+1`，N = 已同步成员数（Syncing 中不计票）。

---

## 5. 故障排查速查表

| 症状 | 最可能原因 | 处理 |
|---|---|---|
| 全节点写 503 | 选不出王 | 查 `MQ_PEERS` / 网络；等 30~60s 收敛 |
| 发现 2 个 Leader | 脑裂 | 查成员表是否一致；重点查**僵尸 PVC**（§5.4） |
| 新节点 `synced=false` 卡死 | catch-up 拉不到快照 / join 打到非王节点 | 查 Leader 是否存活；`MQ_LEADER_URL` 指向王（K8s 用 mq-write Service 名） |
| 新节点 `memberCount=0` 但 join 显示成功 | join 广播竞态（已修）：发 join 时自身 HTTP 未监听 | 先确认镜像 ≥ 最新 tag；再查该节点 `/api/raft/members` 是否持久化 |
| 写 503 但 raft 有王 | 请求打到了非王节点 | 用 SDK 或 mq-write Service；裸调时先查王再打 |
| 三节点 `pendingOnDisk` 不一致 | follower 掉线，delta replay 没跟上 | `/api/raft/members` 查 synced；查网络连通 |
| 大量 `🧊 幂等拦截` | 客户端重复投递 | 正常防护日志，不是故障 |
| 同一节点日志每秒重复一行 | 服务起了双进程抢端口 | 杀掉重复进程 / 查端口占用 |
| `☠️ DLX dead` 频繁 | 业务消息反复 nack | 下游处理坏了，看死信清单 |

### 5.4 经典故障：僵尸成员表脑裂（已修，留档）

**现象**：缩容后再次扩容，新 Pod 自立为王（term 更大），与主集群分裂。

**根因**：StatefulSet 缩容不删 PVC → 新 Pod 挂回旧卷 → 读到几个月前的 `members.json`（含自己）→ 以为自己是合法成员开始竞选。

**修复**：`persistentVolumeClaimRetentionPolicy: whenScaled/whenDeleted: Delete`。缩容自动清卷，下次扩容拿干净卷走 join 流程。

---

## 6. 备份 / 恢复 / 升级 / CI

- **备份**：每天 tar 任一节点的 data 卷（WAL + `_cluster/members.json` + `_exchanges.json` 全在里面）。
- **恢复**：空目录解压 → 起单机（目录认领）或重建集群（成员表回放）。
- **升级**：先 follower 后王；`compose down` **永不带 `--volumes`**；K8s 改 image tag 后 `rollout restart`。
- **回滚**：GHCR 按 sha 取旧镜像（`ghcr.io/lxl-mzd/messagequeue-lab:<sha>`）。
- **CI**：push/PR → build + 24 单测 + 三节点 e2e（选主/Quorum/DLX/杀王）；main 合入 → 推 GHCR `latest` + `sha`。

## 7. 已知限制（诚实清单）

1. SDK 种子全灭则不可用（至少 1 个活种子是底线；K8s 下建议种子写 Service 名）。
2. 元数据（`_exchanges.json` 绑定表）各节点各自持久化，改绑定要在王节点改。
3. 无鉴权：生产放内网或前置反向代理做 API-Key。
4. 大 WAL 快照传输是全量（百万级消息的首次 join 会慢；后续 delta 只追增量）。

---

## 一句话总结

```
部署：单机 1 条命令；compose 1 条命令；K8s 3 个 yaml。
扩缩：compose 手动一步；K8s 一行 scale，join/catch-up/leave 全自动。
客户端：种子给 1 个活地址就行，找王+自学习 SDK 包了。
值班：看 3.1 五项 + 3.2 六阈值；出事先看 §5 表。
```

欢迎提 issue。
