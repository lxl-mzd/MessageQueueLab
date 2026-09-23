# 自造消息队列 · API 文档

> 版本：mq-lab:7（Web\Endpoints.*.cs 注册，与项目结构同名同步更新）
> 默认地址：`http://localhost:5080`（docker：`docker run -d --name mq-server -p 5080:8080 -v mqlab-data:/app/data mq-lab:7`）
> 内置队列：`normal` / `vip`
> 内置交换机：`orders`(direct) / `broadcast`(fanout) / `alerts`(topic)

---

## 目录

1. [总览与语义约定](#1-总览与语义约定)
2. [监控与运维接口](#2-监控与运维接口)
3. [消息收发接口（队列族）](#3-消息收发接口队列族)
4. [交换机接口](#4-交换机接口)
5. [死信队列接口](#5-死信队列接口)
6. [C# SDK（dll 内置客户端）](#6-c-sdkdll-内置客户端)
7. [设计语义速查](#7-设计语义速查)

---

## 1. 总览与语义约定

> **统一消息模型 `MqMessage`（全栈同形）**：`id / crc / key(可空) / content / timestamp(消息产生时间UTC) / receiveTime(最近一次被领取UTC，未领取为 null) / retryCount`。HTTP 返回值即此形状（camelCase）。

### 1.1 消息生命周期

### 1.1 消息生命周期

```
Send ──▶ 就绪墙 ──领走──▶ 锁定区(租期 visibilitySeconds)
              ▲               │
              │               ├─ 做 完 ──▶ ACK   → 账本销账（终点 ✅）
              │   超时失踪      │
              ├─── 巡查员 ◀───┘
              │   (RetryCount+1)
              │               ├─ NACK ──▶ RetryCount+1
              │                              ├─ 未达上限3 → 回墙重试 🔁
              │                              └─ 达上限3   → ☠️ 死信队列
              │                                  ├─ 人工 ack → 销账离场
              │                                  └─ 人工 revive → 计数清零送回
```

### 1.2 三条核心语义

| 语义 | 说明 |
|---|---|
| **at-least-once** | 宁可重复，绝不丢失。消费者重复收到同一 id 是合法现象，业务侧请以 `msg.id` 做幂等去重 |
| **超时=一次失败交付** | `visibilitySeconds` 内不 Ack/Nack，巡查员自动收回并 RetryCount+1（与显式 Nack 同罪），累计 3 次判入死信 |
| **先立卷后除名** | 判死信时先写死信账本再从主账本除名——中断电最坏是重复出现（恢复安检兜底），绝不蒸发 |

### 1.3 通用错误格式

```
404 { "error": "不存在队列/交换机/不在锁定区…", "queue|exchange|id": "..." }
400 { "error": "content 不能为空" }
204 NoContent  = 墙上没票（正常业务状态，非错误）
```

---

## 2. 监控与运维接口

### GET /health
**探活**。容器编排的 liveness probe 用。
```json
{ "service": "self-made-mq", "status": "alive", "now": "2026-09-17 08:33:02" }
```

### GET /api/stats
**全队列聚合计数**（看板四卡数据源）。
```json
{ "readyInMemory": 0, "lockedInMemory": 0, "pendingOnDisk": 0, "deadLetters": 0, "hint": "按队列明细看 /api/queues" }
```

### GET /api/queues
**每队列明细**。
```json
[ { "queue": "normal", "readyInMemory": 2, "lockedInMemory": 0, "pendingOnDisk": 2, "deadLetters": 0 },
  { "queue": "vip",    "readyInMemory": 1, "lockedInMemory": 0, "pendingOnDisk": 1, "deadLetters": 0 } ]
```

### GET /api/events
**事件流水**（最近 200 条；终端日志与看板 feed 同源）。
```json
[ { "ts": "08:33:05", "msg": "📥 [normal] 收到消息 id=940076e4…" } ]
```

### GET /
**健康看板网页**（`text/html`）：指标卡、队列一览、交换机绑定表、死信货架按钮、事件流水。2 秒自动刷新。

---

## 3. 消息收发接口（队列族）

### 3.1 发消息

#### `POST /api/q/{queue}/messages`
| 参数 | 位置 | 必填 | 说明 |
|---|---|---|---|
| queue | 路径 | ✔ | 队列名（normal / vip，平权，normal 无特权） |
| content | body | ✔ | 消息内容 |
| key | body | ✖ | 消息 key（交换机的路由钥匙；队列直发时仅作存档字段） |

**Body**：`{"content": "hello", "key": "order-100"}`（key 可省）
**201** → `{ "queue": "normal", "id": "940076e4…", "key": "order-100", "content": "hello" }`
**400** → content 为空；**404** → 队列不存在

### 3.2 领消息（含长轮询）

#### `POST /api/q/{queue}/receive?visibilitySeconds={Ns}&waitMs={Ms}`

| 参数 | 说明 |
|---|---|
| visibilitySeconds | 领走后的锁定时长（默认 30s）：内需 Ack/Nack，否则巡查员收回并计失败 |
| **waitMs** | **长轮询**：队列空时服务端最多挂住连接 N 毫秒（40ms 步进）等消息上门；0 = 立即返回 |

**204** → 等/查无消息   **200** → `{ "queue": "normal", "id": "…", "key": "…", "content": "…", "retryCount": 0 }`

### 3.3 断案

#### `POST /api/q/{queue}/ack/{id}`
确认完成 → 消息从账本真删。**200** `{ "acked": true }`；**404** 不在锁定区（已确认过/超时回队）。

#### `POST /api/q/{queue}/nack/{id}`
拒绝重做 → RetryCount+1：
- 未达 3：回就绪队列重试，**200** `{ "requeued": true, "retryCount": n }`
- 达 3：自动转死信队列（返回仍是 200，但事件流水出现 ☠️）

---

## 4. 交换机接口

### `GET /api/ex` 交换机规则一览
```json
[ { "name": "orders",    "type": "direct", "bindings": [ { "routingKey": "normal", "queue": "normal" }, { "routingKey": "vip", "queue": "vip" } ] },
  { "name": "broadcast", "type": "fanout",  "bindings": [ { "queue": "normal" }, { "queue": "vip" } ] },
  { "name": "alerts",    "type": "topic",   "bindings": [ { "routingKey": "pay.#", "queue": "normal" }, { "routingKey": "#.urgent", "queue": "vip" } ] } ]
```

### `POST /api/ex/{exchange}/publish?routingKey={RK}`
**投递**——按交换机类型分拣：
| 类型 | 语义 |
|---|---|
| direct | routingKey 字面量精确匹配绑定 |
| fanout | 忽略 key，广播到所有绑定队列（各一份） |
| topic | 通配符匹配：`*`=恰好1词、`#`=0..n词；可多播命中多队列 |

**Body** `{ "content": "...", "key": "..." }`；**未显式提供 routingKey 且 body.key 为空 → 默认 `"normal"`**。
**200** → `{ "exchange": "alerts", "routingKey": "pay.urgent", "deliveredTo": 2 }`
**404** → 交换机不存在

---

## 5. 死信队列接口

### `GET /api/q/{queue}/dlq`
```json
[ { "id": "940076e4…", "key": "order-1", "content": "x", "retryCount": 3 } ]
```

### `POST /api/q/{queue}/dlq/{id}/ack`
**人工善后**：确认处理完/决定放弃 → 双账本+内存全清。**200** `{ "deadLetterAcked": true }`

### `POST /api/q/{queue}/dlq/{id}/revive`
**人工送回**：RetryCount 清零重新排队。**200** `{ "revived": true, "id": "…" }`

---

## 6. C# SDK（dll 内置客户端）

`ProjectReference` 引入 `MessageQueueLab.dll` 后：

### 6.1 SefMqConfig（Properties 等价）

| 键 | 默认 | 说明 |
|---|---|---|
| `bootstrap.url` | （必填） | Broker 地址，如 `http://localhost:5080` |
| `default.visibility` | `12` | 消费者 Receive 的默认锁定秒数 |
| `poll.delay.ms` | `120` | 空轮询间隔（长轮询期间的口语间距） |
| `long.poll.wait.ms` | `500` | **长轮询**：每次 receive 最多挂住毫秒数 |

### 6.2 生产者

```csharp
using MessageQueueLab.Sdk;

var config   = new SefMqConfig { ["bootstrap.url"] = "http://localhost:5080" };
var producer = new SefMqProducer(config);

// Kafka 同款：record(topic, key, value) + 异步发送回调
producer.Send(new SefMqRecord("normal", "order-100", "hello"),
    callback: (meta, ex) =>
    {
        if (ex == null) Console.WriteLine($"成功 {meta!.MessageId}");
        else            Console.WriteLine($"失败 {ex.Message}");
    });

// awaitable 版（当返回值用）
SefMqMetadata meta = await producer.SendAsync(new SefMqRecord("vip", "order-101", "hi"));

// 交换机投递（广播/主题/直连），返回送达队列数
int n = await producer.PublishAsync("broadcast", null, "全店公告");
```

### 6.3 消费者

```csharp
var consumer = new SefMqConsumer(config);   // 每个 worker 一个实例（内部自带后台消费线程）
consumer.Subscribe("normal");               // 订阅 topic（=队列名）

var task = consumer.BeginConsume(async (msg, ctx) =>
{
    // msg: QueueMessage { Id, Key, Content, RetryCount }
    if (msg.Content.Contains("必败"))
    {
        await ctx.Nack(msg.Id);   // 手动断案 → 重试/死信通道（自动断案不再介入）
        return;
    }
    await ctx.Ack(msg.Id);        // 也可以手动 Ack
    // 不手动断案 → 正常返回自动 Ack；抛异常自动 Nack
}, ct: myCancellationToken);
```

模型：**handler 正常返回 = 自动 Ack**（enable.auto.commit 对拍）；`ctx` 已手动断案则跳过自动。

### 6.4 管理者

```csharp
var admin = new SefMqAdmin(config, queue: "normal");

var s = await admin.QueueStatsAsync();   // (ready, locked, pendingOnDisk, deadLetters)
var dlq = await admin.DlqListAsync();    // List<QueueMessage> 死信清单
await admin.DlqAckAsync(dlq[0].Id);      // 善后销账
await admin.DlqReviveAsync(dlq[0].Id);   // 送回主队列（计数清零）
```

---

## 7. 设计语义速查

| 机制 | 关键词 | 一句话 |
|---|---|---|
| 持久化 | AppendLedger | WAL 只追加事件账本（e/d/n），O(1) 写入，重启回放重建，512 行自动压缩 |
| 数据防护 | CRC32 | 账本每行封蜡；恢复逐条验蜡，坏点起全弃（残卷无论中段/末尾抓现行）|
| 锁定租约 | visibilitySeconds | 借走≠拿走；逾期不吭声自动收回，且记一次失败 |
| 重试 | RetryCount ≤3 | nack 与超时失踪同罪计数；计数持久化防“死亡循环" |
| 死信 | DLQ | 判死先立卷后除名；原地保留待人工；revive 计数清零 |
| 交换机 | direct/fanout/topic | direct 字面匹配；fanout 门广播；topic 通配符多播（`*`1词、`#`0..n词）|
| 长轮询 | waitMs | 空墙最多挂 N 毫秒；期间来货即刻送达 |
| 自动断案 | HandledManually | handler 正常返回自动 Ack；抛异常自动 Nack；ctx 已手动则跳过自动 |
| 事件流水 | Emit | 最近 200 条环形日志；docker logs 与网页同源 |
