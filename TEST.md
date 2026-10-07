# 自造消息队列 · 测试方案与实测结果（TEST）

> 目的：一份文档说清"每项功能怎么测的、测出来什么"。
> 分四层：L1 单元测试（进程内）→ L2 SDK 端到端（真集群真 HTTP）→ L3 故障演练（杀节点/滚动升级）→ L4 运维验收（看板人工）。
> 最近一轮全量实测：compose 三节点（镜像 `mq-lab:22`）+ K8s 三节点，全部通过。

---

## 0. 测试环境与复跑命令

| 层 | 环境 | 复跑命令 |
|---|---|---|
| L1 单元 | 进程内（临时目录） | `dotnet test tests/MessageQueueLab.Tests` |
| L2 SDK E2E | compose 三节点（`:5081-5083`） | 先 `docker compose up -d` 等 45s，然后 `cd ../MQTestClient && dotnet run` |
| L2 SDK E2E（单机） | `dotnet run -- http://localhost:5000`（集群阶段自动跳过） |
| L2 SDK E2E（K8s） | `dotnet run -- http://127.0.0.1:5091`（先架 port-forward） |
| L3 故障演练 | K8s StatefulSet | 手工（本文第 3 节步骤可复现） |
| CI | GitHub Actions | push 自动：build + L1 + 三节点 e2e 冒烟（`tests/smoke.ps1`） |

---

## 1. L1 单元测试（24 项，24/24 通过）

进程内直测 Core/Persistence，不碰网络。`dotnet test` 1 秒跑完。

| 测试 | 测什么 | 结果 |
|---|---|---|
| `BindingMatchesMatrix`（13 例） | topic 通配符矩阵：`#` 吞 0/1/多词、`*` 恰 1 词、深层回溯、`dead.normal` 精确、不命中反例 | ✅ 13/13 |
| `Replay_KeepsLiveMessages_DropsAcked` | WAL 回放：活账留下、已销账丢弃 | ✅ |
| `Replay_RebuildsRegistryWithSegmentLocation` | 回放重建 Registry 映射（含段定位） | ✅ |
| `AckedIsNotRedelivered_AfterRestart` | **幂等跨重启**：已 ack 的消息重启后不再投递 | ✅ |
| `Ack_Twice_SecondFails` | 重复 ack 第二次拒绝 | ✅ |
| `NackThreeTimes_ArrivesAtDLQ` | nack 满 3 次进死信 | ✅ |
| `DlgRoute_MatchesRebind` | DLX 路由跟绑定走（可重绑） | ✅ |
| `TtlExpires_ReceivesSkipsIt` | TTL 过期：领取时跳过直判死信 | ✅ |
| `SealedAt512Rows` | WAL 满 512 行封段 | ✅ |
| `DynamicQueue_Declared_ThenFilePaperPersists_AfterRestart` | 动态建队 + 交换机绑定跨重启持久化 | ✅ |
| `QuorumMath` | 多数派票数 N/2+1（2→2、4→3） | ✅ |
| `ApplySnapshot_ShouldRestoreQueues` | 快照恢复（新节点 catch-up 的地基） | ✅ |

---

## 2. L2 SDK 端到端（MQTestClient，8 阶段 29 检查点，全 PASS）

多生产者×多消费者打真集群，退出码 0 才算过。最近一轮输出摘要：

| 阶段 | 测什么 | 怎么测 | 结果 |
|---|---|---|---|
| 0 建队 | 声明幂等 + 残留清理 | SDK `DeclareQueueAsync`×2；开工前排空旧消息（重跑不污染） | ✅ |
| 1 并发写 | 3 生产者 × 20 并发 `SendAsync` | 全部成功 | ✅ 60/60 |
| 2 并行消费 | 3 消费者 `BeginConsume` 自动 Ack | 60 条全领走、**去重 60**（无重复投递）、队列排空 | ✅ |
| 3 TTL | 过期直判死信 | SDK `.WithTtl(2)` 发 → 4s 后领不到（204）→ 死信列表可见 → 销尸 | ✅ 4/4 |
| 4 交换机 | topic 绑定→投递→解绑 | SDK `BindAsync("alerts","e2e.#")` → `PublishViaExchangeAsync`（deliveredTo=1）→ SDK 消费者领到同一条 → `UnbindAsync` 后 deliveredTo=0 | ✅ 5/5 |
| 5 死信全链 | nack×3 → revive → 销账 | 3 次"领到+nack" → 死信可见 → SDK `DlqReviveAsync` → **同一条 ID 复活可领** → 终局 ready=0 dead=0 | ✅ 6/6 |
| 6 可见性超时 | 锁定期 + 超时重现 | 领走不 ack（vis=2s）→ 锁定期内 204 → 3s 后**同一条 ID 重现** → 销账排空 | ✅ 4/4 |
| 7 集群切换 | 纯副本拒写 + SDK 找王 | 直连 Follower 写得 **503** → SDK 经种子表自动绕到王发送成功 → 消息可领 | ✅ 4/4 |
| 8 终局 | 重复建队 + 全场干净 | ready=0 locked=0 dlq=0 | ✅ 2/2 |

裸 HTTP 只用在 SDK 够不到的两处：直连跟随者探 503（SDK 会自动绕开）、精准 nack/可见性控秒（要掐秒表）。

---

## 3. L3 故障演练（K8s 实录，人工触发）

| 演练 | 怎么触发 | 预期 | 实测结果 |
|---|---|---|---|
| 扩容 3→4 | `kubectl scale --replicas=4` | 新 Pod 自动 join + catch-up 转正 | ✅ join 干净、`synced=true`、写 **ack 4/4**、看板第 4 卡自动出现（仅 5091 一个桥，经王转发） |
| 缩容 4→3 | `kubectl scale --replicas=3` | SIGTERM → 优雅 leave → 表回 3 人 | ✅ 成员表回 3、写 3/3、PVC 自动回收 |
| 杀王 failover | `kubectl delete pod <王>` | ≤12s 新王当选、旧王重启后自动归队 | ✅ 新王当选 term+1、三表一致、重启王以 Follower 回归（F2 自愈）、灾后写 3/3 |
| 滚动升级 | 逐台 `delete pod` 换镜像（:20→:21→:22） | 每台回归、全程单王 | ✅ :22 三台滚动含两次王位切换零事故 |
| 双王事故复盘 | :19 时代滚动更新触发 | —— | 事故根因：**leave 后重启的节点带旧成员表竞选 + 投票无成员校验** → 双王 503。修复 = F1（非成员拒票）+ F2（重启先 join 认领活表/连续零票回 join），修复后 :21/:22 两轮滚动零复现 |
| 跨环境污染 | compose 与 K8s 并行跑 | —— | 事故：docker 网段的 join 打进 K8s 成员表致脑裂。处置：清表重建。教训固化：两套环境不并行、端口惯例分区 |

---

## 4. L4 运维验收（看板，人工）

| 项 | 验收点 | 结果 |
|---|---|---|
| 节点自动发现 | 成员表驱动：新节点卡片自动出现、离场自动消失 | ✅（3↔4 卡片随扩缩容进出） |
| 经王转发 | 只搭 5091 一个桥，其他节点数据由王代查 | ✅（4 卡全"经王转发"取数） |
| 资源面板 | CPU/内存/磁盘/带宽饼图，集群聚合 + 节点展开 | ✅ 3/3 节点上报、聚合数正确 |
| 单机模式看板 | 无成员表时回退看自己（不再死守 compose 端口全失联） | ✅（9824563 修复后验证） |
| 看板读数 | 就绪/锁定/主账/死信四数与 API 一致 | ✅ |
| 排障路径 | `check-mq.ps1` 一键体检 | ✅ |

---

## 5. 结果汇总

| 层 | 规模 | 结果 |
|---|---|---|
| L1 单元 | 24 项 | **24/24 通过** |
| L2 SDK E2E | 8 阶段 29 检查点 | **全部 PASS，退出码 0** |
| L3 故障演练 | 6 项 | **全部符合预期**（含 2 起真实事故的根因修复与复验） |
| L4 运维验收 | 6 项 | **全部通过** |
| CI | push 即跑 | build + L1 + 三节点 e2e 冒烟全绿 |

---

## 6. 诚实清单：没测的与已知边界

| 项 | 状态 | 说明 |
|---|---|---|
| 吞吐压测 | **未做** | 定位是微服务通信中间件，不拼吞吐；要压再说 |
| 灾难恢复 3.5（死 2/3 手术） | **写了手册未实操** | `OPS.md` §3.5 有逐步命令，建议找时间在 compose 演练一遍 |
| 长稳测试（7×24） | 未做 | 目前均为分钟级会话验证 |
| 消息体 >512KB / 乱码 / 截断 | 未系统测 | CRC 校验逻辑在（双 CRC），但边界用例未补 |
| K8s 控制面掉线自愈 | 事故已遇到 | Docker Desktop 重启清了 kubeconfig，属环境问题非产品问题 |

> 原则：本表只记真测过的；"应该没问题"不写"通过"。
