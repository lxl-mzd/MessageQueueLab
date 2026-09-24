# 自造消息队列 · MessageQueueLab

![CI](https://github.com/lxl-mzd/MessageQueueLab/actions/workflows/ci.yml/badge.svg)

**定位：面向微服务间的轻量消息中间件** —— 不拼吞吐，拼"不丢、看得懂、坏了自己能站起来"：
WAL 持久化、三节点 Quorum 故障转移、死信交换机、幂等查重与 TTL 全部内置，HTTP REST 一键部署。

核心能力：

- WAL 分段账本（512 行封段 + 压缩标记）+ 双层 CRC（消息级 + 行级）
- Registry 全局映射表（WAL 回放重建，跨重启幂等查重成立）
- 四类交换机（direct / fanout / topic 通配符 + **死信交换机 DLX**，路由 `dead.{queue}`，可动态 rebind）
- 死信队列（重试 3 次上限、复活、人工确认 + TTL 过期直判）
- TTL（body `ttlSeconds` 显式过期）
- 可见性超时回收 + 长轮询 Receive
- Quorum 写入三阶段流水线：① 内存 Prepare → ② 广播 Follower 等多数派 → ③ 本地落盘+状态机（凑不够多数派 → 503 且全集群零残留）
- Raft 动态主权（选举自动选王；杀王 ≤12s 接管；杀掉的节点回归自动降级 Follower）
- 三节点 docker-compose 集群 + 健康看板
- 动态声明（`POST /api/q/{queue}` 建队、`/api/ex/{ex}/bind|unbind` 改绑，全持久化）

## 快速开始

```bash
mdo 最省事（不 clone，拉成品镜像）:
docker run -d -p 5000:8080 ghcr.io/lxl-mzd/messagequeue-lab:latest

# 单机源码开发
dotnet run   # http://localhost:5000

# 三节点集群
docker compose up -d --build
# 看板: http://127.0.0.1:5081/ (King), :5082, :5083
```

文档见 `ARCHITECTURE.md`（架构图）`API.md`（接口）与 `DESIGN.md`（设计全文）。

## CI/CD

```
push / PR  →  GitHub Actions: build + 23 单测 + 三节点 e2e 冒烟（Raft/Quorum/DLX/杀王接管）
main 合入  →  自动推镜像到 ghcr.io/lxl-mzd/messagequeue-lab:{latest, sha}
```

部署升级请用 `docker compose pull && docker compose up -d`（先 follower 后 King，参考 ARCHITECTURE.md 第 3 节）。
