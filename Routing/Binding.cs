// ═══════════════════════════════════════════════════════════════
// Routing/Binding.cs —— 绑定类 + 通配符匹配引擎（topic 交换机的大脑）
//
//   routingKey 与 pattern 都是"点分词"： pay.success / a.b.c
//     · pattern 通配符：
//         *  → 恰好匹配 1 个词
//         #  → 匹配 0 个或多个词（含 0——"以 urgent 收尾"的特例）
//
//   一条消息可以同时命中多个绑定 → 多播（各队列各得一份）
//
//   Kafka 对拍：min.cleanable.dirty.ratio 0.5（比率思想同源）
//     · Kafka 的 LogCleaner 也按比率触发 GC，而这儿的 topic 匹配是
//       "每条消息同时有多路订阅"——就是 topic 扇出表达式的学术原型
//
//   零依赖：无 LINQ、无 Regex —— 纯 Switch 逻辑，易教易测
// ═══════════════════════════════════════════════════════════════
namespace MessageQueueLab.Routing;

public class Binding
{
    public string Pattern { get; init; } = "";   // 绑定模板（含通配符；fanout 类无钥匙=空串）
    public string Queue { get; }                 // 绑到哪个队列

    // 预切割（构造时算好，Matches 时不再拆）
    private readonly string[] _patternParts;

    public Binding(string? pattern, string queue)
    {
        Pattern = pattern ?? "";
        Queue = queue;
        _patternParts = pattern?.Split('.') ?? [];   // null 绑定：fanout 登记空串（匹配永不命中）
    }

    /// <summary>public 接口：判断这条消息的 routingKey 是否命中该绑定（含通配符）</summary>
    public bool Matches(string routingKey)
    {
        var words = routingKey.Split('.');
        return MatchFrom(words, 0, _patternParts, 0);
    }

    // 逐段推进；# 需要回溯试探"吞几个词"，普通段 * 段一口气前进
    // wi = 当前 routingKey 词的下标  pi = 当前 pattern 段的下标
    private bool MatchFrom(string[] words, int wi, string[] parts, int pi)
    {
        // pattern 消费完 → key 也必须刚好用完（两边同时到“末尾”才算命中）
        if (pi == parts.Length) return wi == words.Length;

        var part = parts[pi];

        // 通配符 # 一支：它甚至可以吞 0 个词——从这个位置向后尝试所有可能的词数
        if (part == "#")
        {
            for (var skip = wi; skip <= words.Length; skip++)
            {
                if (MatchFrom(words, skip, parts, pi + 1)) return true;
            }
            return false;
        }

        // 这一步：pattern 还有段，key 已用完 → 不命中
        if (wi == words.Length) return false;

        // 普通段 /* 段：wi+1 并进（匹配一个词）
        if (part == "*" || string.Equals(part, words[wi], StringComparison.Ordinal))
            return MatchFrom(words, wi + 1, parts, pi + 1);

        return false;
    }
}
