# 自造消息队列 · 部署与运维手册（OPS.md）

> 适用版本：mq-lab v4+（动态主权 Raft / Quorum 三阶段 / 幂等查重 / DLX 死信交换机 / 元数据持久化）
> 读者：部署这套消息队列的工程师 / 值班运维 / 偶尔看一眼的微服务同学

---

## 0. 三分钟版（懒得看全文）

```bash
# 单机拉起（服务器上有 docker 就行，拉我们的成品镜像）
docker run -d --name mq-server -p 5000:8080 -v mqlab-data:/app/data \
    ghcr.io/lxl-mzd/messagequeue-lab:latest
# 验证
curl http://localhost:5000/health
浏览器开 http://localhost:5000/     # 健康看板

# 三节点集群（生产形态）
git clone https://github.com/lxl-mzd/MessageQueueLab.git
cd MessageQueueLab && docker compose up -d --build

# 日常巡检三连
docker compose ps                              # 三容器全 Up
curl http://127.0.0.1:5081/api/raft/status     # 恰好一个 Leader
curl http://127.0.0.1:5081/api/queues          # 各队列计数
```

读完全文你还会知道：怎么升级不中断、怎么备份恢复、哪些指标该配告警。

---

## 1. 部署

### 1.1 选形态

| 形态 | 什么时候用 | 特征 |
|---|---|---|
| **单节点** | 开发/测试环境、能容忍挂掉后人工重启 | 1 个容器，无 Raft |
| **三节点**（生产推荐） | 正式微服务链路、不能丢消息 | Raft 自动选王，任意 1 台宕机零中断 |

三节点硬性前提：**三套副本要尽量独立**。最忌讳三个容器挤同一台宿主机——宿主机断电等于三副本一起死，高可用变装饰。

### 1.2 单机部署

```bash
docker pull ghcr.io/lxl-mzd/messagequeue-lab:latest

docker run -d --name mq-server \
    -p 5000:8080 \
    -v mqlab-data:/app/data \
    -e MQ_ROLE=single \
    --restart unless-stopped \
    ghcr.io/lxl-mzd/messagequeue-lab:latest
```

- `-v mqlab-data:/app/data` **千万别少**（WAL 账本 + 死信账本 + 交换机快照全在里面）
- `--restart unless-stopped` 让 docker daemon 重启后自动拉起服务

### 1.3 三节点集群（docker-compose）

仓库里的 `docker-compose.yml` 已经写好一键三节点（宿主机端口 5081/5082/5083），关键环境变量：

| 变量 | 说明 | 出错后果 |
|---|---|---|
| `MQ_PEERS` | Raft 全员表（**每个节点都要配全 3 员，含自身**） | 少一个 → 脑裂出双王 |
| `MQ_SELF` | 本节点对外地址（Raft 用来把"自己"剥掉） | Leader 会自投心跳把自己降级 → 反复重选 |
| `MQ_NODE_NAME` | 日志看板上的节点名 | 只是显示 |
| `MQ_ROLE` | 初始角色声明 | 只影响首现写入口；Raft 定身后以王为准 |

三个要点：
1. **MQ_PEERS 三节点必须完全一致**（写错一个字就脑裂）。
2. 生产建议三台独立机器各跑 1 个节点，而不是同宿主机 3 容器。
3. 端口暴露建议只暴露**王所在的那一个**给业务方（其他两台 follower 本身不收业务写）。

### 1.4 微服务接入方式

```bash
# 发消息（强烈建议带 key 作为业务幂等键）
curl -X POST http://<leader>:5000/api/q/orders/messages \
    -H "Content-Type: application/json" \
    -d '{"content":"创建订单","key":"order-12345"}'

# 消费（长轮询 5 秒 + 可见性 30 秒）
curl -X POST "http://<node>:5000/api/q/orders/receive?waitMs=5000&visibilitySeconds=30"

# 销账
curl -X POST "http://<node>:5000/api/q/orders/ack/{id}"
```

入口选择：
- **方式 A**：DNS 指向当前王（需外部脚本做 failover 漂移）
- **方式 B（推荐，零依赖）**：客户端拿到 503 / 连接失败自动换下一个节点重试（503 = "我不是王"），SDK 内置即可

### 1.5 镜像与回滚

```bash
docker pull ghcr.io/lxl-mzd/messagequeue-lab:latest       # 最新
docker pull ghcr.io/lxl-mzd/messagequeue-lab:<git-sha>    # 时光机，回滚用
```

要固定版本，把 compose 里的 tag 写成具体 sha 再 up，不 pin `latest`。

---

## 2. 常规运维

### 2.1 健康检查清单（每天一次）

| 检查项 | 方法 | 健康值 |
|---|---|---|
| 探活 | `GET /health` ×3 | 200 且 `"status":"alive"` |
| 王位 | `GET /api/raft/status` ×3 | **恰好 1 个 role=Leader**；term 三节点一致或 ±1 |
| 复制同步 | `GET /api/queues` ×3 | follower 的 `pendingOnDisk` 与王差距不悬殊 |
| 死信 | `GET /api/queues` 的 `deadLetters` | 增长 <10 条/分钟；持续堆积需人工处理 |
| 磁盘 | data 卷容量 | <80%（WAL 压缩兜不住磁盘满） |

看板直接浏览器开 http://host:5081/ 静态浏览，等价于以上手工。

### 2.2 告警阈值建议

1. 任一节点 `/health` 超 30 秒无响应 → P1（业务写不停但副本少一份）
2. raft `leaderId` 30 秒内连续变化 → P1（选主抖动，查网络）
3. data 卷 >80% → P0
4. `deadLetters` 持续增长 → P2（下游挂了）
5. `pendingOnDisk` 三节点差 >100 → P2（复制断流）

### 2.3 运维操作（金句五连）

```bash
# 死信复活
curl -X POST http://<leader>:5000/api/q/orders/dlq/<id>/revive

# 死信删除（人工确认后）
curl -X POST http://<leader>:5000/api/q/orders/dlq/<id>/ack

# 动态建队（立即生效；重启自动认领）
curl -X POST http://<leader>:5000/api/q/audit

# 改 DLX 死信路由（持久化，重启不掉）
curl -X POST "http://<leader>:5000/api/ex/dead/bind?pattern=dead.urgent&queue=error"

# 解绑
curl -X POST "http://<leader>:5000/api/ex/dead/unbind?pattern=dead.urgent&queue=error"
```

幂等说明：服务端日志出现 `🧊 幂等拦截` 是**正常防护**（同一条消息重复被提出/重复 ack），不是故障——业务侧需要的是核对幂等键而不是报障。

---

## 3. 备份与灾难恢复

```bash
# 每天一次（任一节点）
docker run --rm -v mqlab-leader:/data -v $PWD/backup:/backup alpine \
    tar czf /backup/mqlab-leader-$(date +%F).tar.gz -C /data .
```

**灾难恢复（纯空机器）**：
```bash
mkdir -p $PWD/data
tar xzf mqlab-leader-2026-09-24.tar.gz -C $PWD/data
docker run -d -p 5000:8080 -v $PWD/data:/app/data mqlab-latest
# 启动日志应出现 📇 磁盘认领 ... 队列（目录在即认领）
```

> ⚠️ **如果三台节点的 WAL 磁盘都毁掉（且没备份）** —— 那就是数据全没。
> 所以**每天备份是必须的**。3 节点 Quorum 只保证"短窗口单机故障不丢"，不给"全清空"兜底。

---

## 4. 升级流程

**铁律：`docker compose down` 永远不带 `--volumes`**（删数据卷 = 删账本），日常用 `stop`。

```
1. 预检：/health ×3 全 200 + 做第 3 节备份 + deadLetters 不在突增
2. 先升两个 follower（stop → pull → up），逐个来，整体不中断
   （期间 production 实际约 503 一次，客户端重试即可）
3. 升级王节点：docker compose stop mq-leader → pull 新镜像 → up
   （新王 ≤12s 自动选出，老王起来自动降级回 Follower，这是设计的）
4. 冒烟：写一条消息 → 三节点 /api/queues pendingOnDisk 对齐
5. 观察 /api/events 半分钟无 Quorum 未达
```

---

## 5. 故障排查速查表

| 症状 | 最可能原因 | 处理 |
|---|---|---|
| 所有节点写 503 | Raft 还没选出王（网络不通/全员表错） | 等 30~60s；查 MQ_PEERS 与网络互通 |
| `/raft/status` 出现 2 个 Leader | MQ_PEERS 配错 → 节点互不相识（脑裂） | 修配置 + 重启受影响节点 |
| 写 503 但 raft 正常 | 业务请求打到了非王节点 | 用客户端轮转（1.4 方式 B）或 DNS 漂移 |
| 三节点 `pendingOnDisk` 不一致 | follower 离线窗口丢复制 | **catch-up 未实现**（见 Road）；短期做完整性对账，或删卷重做 follower |
| 大量 `🧊 幂等拦截` | 客户端重复 send/ack | 正常防护日志，不是故障 |
| 每秒日志一行同一句 | 服务起欢了但有端口竞争 | 杀掉重复进程 / 查占用 |
| `☠️ DLX dead` 频繁 | 业务消息反复 nack | 下游处理坏了，看 2.1 死信清单 |

---

## 5.1 (附) 已知限制（诚实清单）

1. **follower 离线丢账补不回来**：follower 挂期间 leader 收的新事件不追平（catch-up Road 未做）。建议用 quorum 三副本时业务侧"全量校验"兜底。
2. **元数据（/exchanges.json）节点间不同步**：各节点各写自盘；改绑定要在王节点或全员执行。
3. **鉴权未实现**：生产务必内网部署，或加反向代理做 API-Key。

---

## 6. 版本管理

- 镜像 tag：`latest`（最新）、`<git-sha>`（精确回滚时光机）。
- WAL 格式是 data contract，跨版本升级前必须**先做备份**（第 3 节）。
- CI 绿构则说明代码是"非烂"状态；release passing = 镜像能跑。仓库每次 push 自动升级。

---

## 一句话总纲

```
正常运行：什么也不用做（除非要改绑定或建队列）
节点崩溃：Raft 自动自愈，回头的节点自动降级复制
磁盘挂掉：用第 3 节的备份恢复（磁盘挂掉 = 冷备回填）
告警五条 = 第 2.2 节，齐了
升级四步 = 第 4 节，齐了
```

欢迎提 issue。Good luck 😊
