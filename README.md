# 自造消息队列 · MessageQueueLab

从零搭建的教学级消息队列，纯 C#，零外部依赖：

- WAL 分段账本（512 行封段 + 压缩标记）+ 双层 CRC（消息级 + 行级）
- Registry 全局状态机 Map（Ready → InFlight → Acked / Dead）
- direct / fanout / topic 三类交换机（topic 带通配符回溯匹配 `pay.#`、`#.error`）
- 死信队列（重试 3 次上限、复活、人工确认）
- TTL（`ttlSeconds` 显式过期，receive 惰性判定）
- 可见性超时回收 + 长轮询 Receive
- Quorum 写入三阶段流水线：① 内存 Prepare → ② 广播 Follower 等多数派 → ③ 本地落盘+状态机（凑不够选举人数 → 503 且全集群零残留）
- Raft 选举（vote/heartbeat RPC，term 收养 + 同任期一票 + 随机超时）
- 三节点 docker-compose 集群 + Raft 选举 + 健康看板（`/`）

## 快速开始

```bash
# 单机
dotnet run   # http://localhost:5000

# 三节点集群
docker compose up -d --build
# 看板: http://127.0.0.1:5081/ (Leader), :5082, :5083
```

文档见 `API.md`（接口）与 `DESIGN.md`（架构）。
