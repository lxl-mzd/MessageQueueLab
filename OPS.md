# 消息队列 · 部署运维手册（给第一次接触的人）

> 你不需要懂 Raft、Quorum、WAL 是什么。跟着敲命令就行，
> 每一步都有"成功长什么样"对照，不一样就是出问题了。

---

## 第一部分：部署（别人电脑上啥都没有，从零开始）

### 准备工作：只装一个 Docker

| 电脑 | 装什么 | 去哪下 |
|---|---|---|
| Windows / Mac | Docker Desktop（安装时把 Kubernetes 勾上，如果以后想玩 K8s） | https://www.docker.com/products/docker-desktop |
| Linux 服务器 | Docker Engine | `curl -fsSL https://get.docker.com \| sh` |

装完验证（复制粘贴，看到版本号就是装好了）：

```bash
docker --version
```

**除此之外什么都不用装。** 不用装 .NET，不用下代码，不用配数据库。

---

### 方式一：单机模式（1 台机器，测试用，5 分钟搞定）

**第 1 步：拉起服务**（复制粘贴下面整段，一次回车）：

```bash
docker run -d --name mq-server \
  -p 5000:8080 \
  -v mq-data:/app/data \
  --restart unless-stopped \
  ghcr.io/lxl-mzd/messagequeue-lab:latest
```

这行命令的意思（不用背，看懂就行）：
- `docker run -d`：后台起一个容器
- `-p 5000:8080`：把容器里的 8080 端口映射到你电脑的 5000 端口（后面都用 5000 访问）
- `-v mq-data:/app/data`：数据存在一个叫 `mq-data` 的卷里。**没有这一行，容器删掉数据就全没了。**
- `--restart unless-stopped`：电脑重启后自动把消息队列拉起来

**第 2 步：确认跑起来了：**

```bash
curl http://localhost:5000/health
```

成功长这样（status 是 alive 就行）：

```json
{"service":"self-made-mq","status":"alive","now":"2026-10-01 14:20:57"}
```

不成功长这样（二选一处理）：
- `curl: (7) Failed to connect` → 容器没起来，跑 `docker logs mq-server` 看最后一行报错
- 端口被占用 → 把 `-p 5000:8080` 改成 `-p 5001:8080`，后面所有 `5000` 换成 `5001`

**第 3 步：发第一条消息试试：**

```bash
curl -X POST http://localhost:5000/api/q/test/messages \
  -H "Content-Type: application/json" \
  -d '{"content":"hello"}'

curl -X POST http://localhost:5000/api/q/test/receive
```

第二条命令返回了 `hello`，单机部署成功。

---

### 方式二：compose 三节点模式（生产用，10 分钟搞定）

**第 1 步：拿代码**（只需要里面的 `docker-compose.yml`）：

```bash
git clone https://github.com/lxl-mzd/MessageQueueLab.git
cd MessageQueueLab
```

没装 git？去 https://git-scm.com/downloads 下一个，一路下一步。

**第 2 步：拉起三个节点：**

```bash
docker compose up -d --build
```

**第 3 步：确认三个都活着，且只选出一个王：**

```bash
docker compose ps
```

成功长这样（三个都是 Up）：

```
messagequeuelab-node-1-1  Up
messagequeuelab-node-2-1  Up
messagequeuelab-node-3-1  Up
```

再确认选主（三条命令，每条看 `role` 字段）：

```bash
curl http://127.0.0.1:5081/api/raft/status
curl http://127.0.0.1:5082/api/raft/status
curl http://127.0.0.1:5083/api/raft/status
```

成功标准：**三条返回里，恰好一个 `"role":"Leader"`，另外两个是 `"role":"Follower"`**，而且三个的 `term` 数字一样。

- 0 个 Leader → 还在选，等 30 秒再查
- 2 个 Leader → 脑裂，停掉重来（见第五部分）

**第 4 步：发一条消息试试（打给王，5081/5082/5083 哪个是王就打哪个）：**

```bash
curl -X POST http://127.0.0.1:5082/api/q/normal/messages \
  -H "Content-Type: application/json" \
  -d '{"content":"cluster-hello"}'
```

成功返回里有 `"ackCount":3`（三个节点都落盘了）。

打错门（打到 follower）会返回 `503` —— 不是故障，换一个端口再打就行。嫌麻烦就用 SDK（它自动找王，见 API.md）。

---

### 方式三：K8s 模式（要扩缩容才用，20 分钟搞定）

**第 0 步：确认 Docker Desktop 里 Kubernetes 开着。**
打开 Docker Desktop → 右下角 Settings → Kubernetes → 勾选 "Enable Kubernetes" → Apply。第一次开要下载几分钟，右下角鲸鱼图标不再转圈就是好了。验证：

```bash
kubectl get nodes
```

成功长这样：

```
NAME                    STATUS   ROLES           AGE   VERSION
desktop-control-plane   Ready    control-plane   ...   v1.36.1
```

**第 1 步：部署三个文件（顺序别反）：**

```bash
kubectl apply -f k8s/mq-headless-svc.yaml
kubectl apply -f k8s/mq-write-svc.yaml
kubectl apply -f k8s/mq-statefulset.yaml
```

这三个是干嘛的（一句话版）：
- `mq-headless-svc`：给每个 Pod 一个固定名字（mq-0/mq-1/mq-2），互相能找到
- `mq-write-svc`：只把流量导给王（自动跟王走，王换了它自己换）
- `mq-statefulset`：管三个 Pod + 每人一块独立磁盘

**第 2 步：等 Pod 都起来：**

```bash
kubectl get pods
```

成功长这样（1/1 的是王，0/1 的是跟随者——**跟随者 0/1 是正常的**，只有王才标 Ready）：

```
mq-0   0/1     Running
mq-1   0/1     Running
mq-2   1/1     Running
```

如果有 Pod 一直 `Pending`：磁盘 storageclass 有问题，Docker Desktop 默认一般没事，重启 Docker Desktop 再试。

**第 3 步：验证能写：**

```bash
# 先搭一座桥（K8s 里面，外面够不着，搭桥才能访问；这个窗口关了桥就断）
kubectl port-forward svc/mq-write 5091:8080 --address 0.0.0.0

# 新开一个终端验证
curl -X POST http://127.0.0.1:5091/api/q/normal/messages \
  -H "Content-Type: application/json" \
  -d '{"content":"k8s-hello"}'
```

返回 `"ackCount":3` 就是成功。

---

## 第二部分：运维就是看监控面板

### 2.0 先搭桥（K8s 专用，compose/单机跳过）

K8s 里面的服务，外面浏览器够不着，需要搭两座桥（两个命令，各开一个终端挂着，**关了就进不去**）：

```bash
# 桥 1：看板入口（开 http://127.0.0.1:5091/ 看）
kubectl port-forward svc/mq-write 5091:8080 --address 0.0.0.0

# 桥 2~4：三个节点逐台详情（看板自动发现要用）
kubectl port-forward pod/mq-0 5081:8080 --address 0.0.0.0
kubectl port-forward pod/mq-1 5082:8080 --address 0.0.0.0
kubectl port-forward pod/mq-2 5083:8080 --address 0.0.0.0
```

compose / 单机不需要搭桥：单机直接 `http://localhost:5000/`，compose 直接 `http://127.0.0.1:5081/`。

### 2.1 打开看板

| 模式 | 地址 |
|---|---|
| 单机 | http://localhost:5000/ |
| compose | http://127.0.0.1:5081/（三个端口任意一个都行） |
| K8s | http://127.0.0.1:5091/（先搭上面的桥） |

### 2.2 每个数字是什么意思（正常长什么样）

看板从上到下分四块：

**第一排：五个大数字（全集群总数）**

| 数字 | 意思（大白话） | 正常值 | 不正常 |
|---|---|---|---|
| 全集群就绪 | 等着被领走的消息数 | 时有时无，来活就涨，消费完就掉 | 一直涨不掉 → 消费者挂了 |
| 全集群锁定区 | 被领走、还没 ack 的消息数 | 小数字，来回波动 | 一直涨 → 消费者领了不认账 |
| 全集群主账 | 磁盘上有多少条没销账 | 和"就绪+锁定"差不多 | 差很多 → 问开发 |
| 全集群死信 | 失败太多次、等人工看的消息数 | 0，最好一直是 0 | 一直涨 → 下游业务逻辑坏了 |
| 存活节点 | 格式是 `在线数/总数`，后面跟王的状态 | `3/3 在线 · Leader unique` |  anything else → 看下面 |

**第二块：服务器节点（每台一张卡）**

每张卡四要素：
- 左上角名字（mq-0 / node-1）和地址
- 右上角徽章：`Leader`（绿，王）/ `Follower`（灰，跟随者）/ `Down`（红，失联）
- `term=数字 leader=名字`：三张卡的 term 必须一样；leader 必须是同一台
- 四个小数字：这台机器自己的就绪/锁定/主账/死信

每张卡三个按钮：
- `健康检查`：弹窗显示这台机器 raw 状态，看不懂就截图发开发
- `事件流`：这台机器最近干了什么（谁选上王、谁复制了、谁压缩了）
- `冒烟测试`：在这台机器上走一遍"写→读→删"，通了就弹 OK

**第三块：队列明细 / 交换机 / 死信货架**

- 队列明细：每个队列的四个数，同第一排
- 交换机：路由规则表，不用动，除非你要改路由（先看懂再改）
- 死信货架：每条死信两个按钮——`复活`（送回队列重发一次）、`销账`（看过确认没问题，删掉）

**第四块：事件流水**

所有节点最近干的事按时间排。找关键词：
- `当选 Leader` → 刚选完王（后面应安静）
- `Quorum 未达` → 写失败了，看前后发生了什么
- `压缩完成` → 磁盘 GC，正常
- `幂等拦截` → 挡掉重复消息，正常

### 2.3 CPU 内存怎么看（看板上没有，用这两条命令）

看板只显示**消息队列自己的指标**，CPU/内存要看容器层面：

**compose / 单机：**

```bash
docker stats
```

出来一张实时表，只看两列：
- `CPU %`：长期 >80% → 机器扛不住了 → 去第 3 部分扩容
- `MEM %`：长期 >80% → 同上

**K8s：**

```bash
kubectl top pods
```

没有这条命令？先装 metrics-server（Docker Desktop 一般自带，空就跳过，用 `docker stats` 看宿主机也行）：
- 某个 mq Pod 的 CPU 一直顶满 → 去第 3 部分扩容

**先分清再动手**：如果 CPU/内存不高，但是"全集群就绪"一直涨——那不是机器小，是**消费者太慢**，加消费者，不加消息队列节点。

---

## 第三部分：扩缩容（什么时候扩 + 怎么扩 + 怎么缩）

### 3.0 什么时候需要扩

三个信号，**中一个就考虑扩**：

1. `docker stats` / `kubectl top pods` 里 CPU 或内存长期 >80%
2. 看板"全集群就绪"只涨不掉，加消费者也追不上（写远大于读）
3. `pendingOnDisk` 三节点差距 >100 且长时间不追平（复制跟不上了）

都不中？别扩，扩了浪费。

### 3.1 compose 加节点（手动两步）

以 3 台加到第 4 台为例（端口按 5081 往后递增，第 4 台用 5084）：

**第 1 步：起容器**（复制粘贴，把 `node-4` / `5084` 换成你要的）：

```bash
docker run -d --name node-4 --network messagequeuelab_default -p 5084:8080 \
  -e MQ_NODE_NAME=node-4 -e MQ_SELF=http://node-4:8080 -e MQ_PEERS= \
  -e MQ_ROLE=leader -e MQ_LEADER_URL=http://node-1:8080 mq-lab:18
```

**第 2 步：等 1 分钟，看它自己入列。** 新节点会自动：报到 → 拿成员表 → 从王那里把历史数据补齐 → 开始干活。你要做的只是等，然后验证：

```bash
curl http://127.0.0.1:5084/api/raft/status
```

成功标志（三个同时满足）：
- `"synced":true`
- `"memberCount":4`
- `members` 里有 4 个名字

再去看板刷新，应该自动多出一张 `node-4` 的卡（看板会自动发现新节点，不用改配置）。

### 3.2 compose 减节点

```bash
docker stop node-4
```

就这一条。它收到停止信号会自己先跟王说"我走了"（优雅下线），王把成员表改回 3 个。等 15 秒验证：

```bash
curl http://127.0.0.1:5081/api/raft/status
# memberCount 回到 3
```

**删容器不删数据**：`docker rm node-4` 只删容器，它的卷还在；想连数据一起扔才加 `--volumes`（一般别加）。

### 3.3 K8s 扩缩容（一行命令）

```bash
kubectl scale statefulset mq --replicas=4   # 扩到 4
kubectl scale statefulset mq --replicas=3   # 缩回 3
```

**扩容后等 1~2 分钟再验证**（Pod 启动 + 自动 join + 补数据需要时间）：

```bash
kubectl get pods          # 4 个 Running（只有王是 1/1 Ready，跟随者 0/1 是正常的）
```

**缩容是全自动优雅下线**：StatefulSet 先杀序号最大的 Pod → Pod 收到 SIGTERM → 自己向王发 leave → 王改成员表 → Pod 再退出。验证成员表：

```bash
# 随便找个 Pod 的转发端口查（接上面的桥，用 5091 经 mq-write 问王）
curl http://127.0.0.1:5091/api/raft/members
```

**注意**：缩容后旧 Pod 的磁盘卷会被自动回收（这是故意配的，防僵尸数据复活脑裂）。**缩容前确认**：`pendingOnDisk` 三节点基本一致（数据已同步完），再缩。

### 3.4 扩完怎么确认真的好了（三步）

```bash
# 1. 成员数对了（扩到几就是几）
curl http://127.0.0.1:5091/api/raft/members

# 2. 写一条，ack 数 == 成员数（4 节点就是 ack=4）
curl -X POST http://127.0.0.1:5091/api/q/normal/messages \
  -H "Content-Type: application/json" -d '{"content":"smoke"}'

# 3. 看板刷新，新卡出现且数字正常
```

---

## 第四部分：备份、恢复、升级（各就三条命令）

### 备份（每周至少一次，拷走就行）

```bash
docker run --rm -v mqlab-leader:/data -v $PWD/backup:/backup alpine \
  tar czf /backup/mqlab-$(date +%F).tar.gz -C /data .
```

K8s 的卷名用 `kubectl get pvc` 查，`v` 后面换成对的卷名，命令一样。

### 恢复

```bash
mkdir -p $PWD/data
tar xzf mqlab-2026-09-24.tar.gz -C $PWD/data
docker run -d -p 5000:8080 -v $PWD/data:/app/data \
  -e MQ_ROLE=single ghcr.io/lxl-mzd/messagequeue-lab:latest
```

起来后看日志有"磁盘认领"字样就是认回来了。

### 升级（不停服四步）

1. 先升跟随者：一次升一台，升完等它 Ready 再升下一台
2. 最后升王：停王 → 起新版 → 剩下两台 10 秒内自动选出新王
3. 每步后写一条消息验证 `ack` 数 == 当前成员数
4. 全程业务方无感（SDK 自动找新王；裸调的遇到 503 重试一次就行）

**铁律**：`docker compose down` **永远不带 `--volumes`**（带了就是删账本）。K8s 升级改 image tag 后 `kubectl rollout restart statefulset/mq`。

### 回滚

GHCR 里每个版本都带 git 短哈希 tag，打回旧版只需把镜像 tag 改回去重起：

```bash
docker pull ghcr.io/lxl-mzd/messagequeue-lab:<旧的sha>
```

---

## 第五部分：出问题先查这张表

| 你看到的现象 | 最可能的原因 | 复制粘贴这条命令看一眼 |
|---|---|---|
| 看板全 Down / 0/3 在线 | 看板问错了地址（端口转发没建，或 compose 关了） | 先 `curl` 一下对应端口的 `/health`，通不通 |
| 写返回 503 | 打到跟随者了（只有王能写） | 换个端口打，或用 SDK / mq-write Service |
| `memberCount` 三台不一致 | 有节点刚加/刚走，成员表还没收敛 | 等 1 分钟再查；还不一致就重启那个数的节点 |
| 新节点 `synced=false` 卡死 | join 打到非王节点，或快照拉不到 | 查它的 `MQ_LEADER_URL` 是否指向王；看它的容器日志 |
| 出现 2 个 Leader | 脑裂：旧数据卷复活（僵尸成员表） | 删掉问题 Pod 的卷重建（K8s 已配自动回收，compose 手动 `volume rm`） |
| `pendingOnDisk` 差 >100 | follower 掉线，delta replay 没跟上 | 查该节点连通性；巡查员每秒自动补，看事件流有没有 `delta-replay` |
| 死信一直涨 | 下游业务挂了 | 先看死信内容，再修下游；急着恢复先点复活 |
| 磁盘 >80% | WAL 涨太快，压缩跟不上 | 看事件流有没有 `压缩完成`；没有就加磁盘，消息太多就扩节点 |
| K8s Pod 一直 0/1 | 它是 follower（只有王 Ready，**正常的**） | 看 `role` 是不是 Follower，是就不用管 |
| K8s Pod 一直 Pending | 磁盘 storageclass 问题 | 重启 Docker Desktop 再试 |

---

## 一句话总结（贴显示器上）

```
部署：单机 1 条命令；compose 1 条命令；K8s 3 个 yaml。
看病：先看汇总五个大数字，再看每台卡的徽章（绿王/灰跟随/红失联）。
加人：compose 复制粘贴起容器；K8s 一行 scale。等 1 分钟，看成员数。
减人：compose docker stop；K8s 一行 scale。等 15 秒，看成员数。
备份：每周 tar 一个卷。升级：先跟随者后王。down 永不带 --volumes。
```

有问题先看第五部分的表，表里没有再提 issue。
