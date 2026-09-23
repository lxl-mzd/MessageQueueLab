// ═══════════════════════════════════════════════════════════════
// Models/QueueCoreStats.cs —— 统计/请求体等数据契约（零行为，纯形状）
//
//   QueueCoreStats —— 单队列四数字统计体
//     Queue           队列名（normal / vip）
//     ReadyInMemory   就绪墙：还在等消费的消息数（ConcurrentQueue 里的）
//     LockedInMemory  锁定区：被领走仍在处理中的消息数
//     PendingOnDisk   主账本现存行（分区日志，等于“未销账+未归档”的消息量）
//     DeadLetters     死信货架上的死信数
//
//   MessageBody
//     HTTP POST /api/q/{queue}/messages 的请求体 record
//       Content   业务内容（必填）
//       Key       路由键（可空；交换机 direct/topic 分拣时用的钥匙）
//
//   为什么用 `record`：纯不可变数据，无行为——防止“数据quislle误流”到业务
//   层的模型。record 自动生成 Deconstruct 和 Equals，方便测试对比。
//
//   注意：record 默认按特性名 PascalCase，序列化到 JSON 时会被 ASP.NET 隐式转
//   camelCase（`readyInMemory`, `pendingOnDisk`），所以客户端看到的键都是小写的。
// ═══════════════════════════════════════════════════════════════
namespace MessageQueueLab.Models;

// QueueCoreStats：单队列统计快照（stats 接口 + Dashboard 老卡兼容）
public record QueueCoreStats(
    string Queue,              // 队列名（normal / vip）
    int ReadyInMemory,         // 就绪墙上的票数
    int LockedInMemory,        // 锁定区工人手中的票数
    int PendingOnDisk,         // 主账本中未销账的行数
    int DeadLetters);          // 死信账本中的条数

// MessageBody：POST /api/q/{queue}/messages 的 body —— HTTP 绑定用
public record MessageBody(string Content, string? Key = null, int? TtlSeconds = null);
