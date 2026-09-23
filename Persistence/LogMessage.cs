// ═══════════════════════════════════════════════════════════════
// Persistence/LogMessage.cs —— 账本信封（Kafka 姿势：LogMessage 带自身封蜡）
//
//   字段：
//     Seq      世界时间线序号（追加顺序递增；回放按 Seq 全序=历史精确重现）
//     Type     e(入账： send/revive/判死移送) / d(离场: ack/善后) / n(计数刷新)
//     Message  MqMessage 全量载荷（d 事件 Message=null）
//     Id       死信凭证 id（e/n 与 Message.Id 相同）
//
//   ⚠️ 两个 CRC 的边界（守住灵魂，勿混）：
//     行级封蜡   EncodeLine/TryDecodeLine  → 账本行运输/落盘完整性
//     消息面封蜡 MqMessage.Crc（ComputeContentCrc）→ 内容端到端指纹（出生快照）
//
//   集群复制（Leader→Follower）：直接传输 LogMessage JSON（同一信封格式）——
//     Follower PushExternal(evt) 幂等：Seq ≤ 已回放位点直接丢弃
// ═══════════════════════════════════════════════════════════════
using System.Globalization;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using MessageQueueLab.Models;

namespace MessageQueueLab.Persistence;

public sealed record LogMessage(long Seq, string Ledger, string Type, MqMessage? Message, string? Id)
{
    private const char Separator = '|';

    private static readonly JsonSerializerOptions LogJsonOpts = new()
    {
        Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,   // 中文直写（账本肉眼可读）
        WriteIndented = false,
    };

    // ── 行级封蜡（信封完整性：落盘/复制传输）──
    public string EncodeLine()
    {
        var json = JsonSerializer.Serialize(this, LogJsonOpts);
        return $"{Crc32(json):x8}{Separator}{json}";
    }

    public static bool TryDecodeLine(string line, out LogMessage? evt)
    {
        evt = null;
        if (string.IsNullOrWhiteSpace(line)) return false;

        var idx = line.IndexOf(Separator);
        if (idx != 8) return false;                        // 指纹恒 8 位——位置不对必是残卷

        if (!uint.TryParse(line[..idx], NumberStyles.HexNumber, CultureInfo.InvariantCulture, out var crc))
            return false;

        var json = line[(idx + 1)..];
        if (Crc32(json) != crc) return false;              // 行内容被动过 → 拒收

        try { evt = JsonSerializer.Deserialize<LogMessage>(json, LogJsonOpts); }
        catch (JsonException) { return false; }

        return evt is not null;
    }

    // ── 消息级封蜡（内容端到端指纹）──
    //  规范化形态 = “Crc=null 的 MqMessage JSON”（字段名/形状固定 → 结果稳定），消费/审计用时可自行重算比对
    public static string ComputeContentCrc(MqMessage msg)
    {
        var canonical = JsonSerializer.Serialize(msg with { Crc = null }, LogJsonOpts);
        return $"{Crc32(canonical):x8}";
    }

    private static uint Crc32(string s)
    {
        var bytes = Encoding.UTF8.GetBytes(s);
        uint crc = 0xFFFFFFFFu;
        foreach (var b in bytes)
        {
            crc ^= b;
            for (var i = 0; i < 8; i++)
                crc = (crc >> 1) ^ (((crc & 1) != 0) ? 0xEDB88320u : 0u);
        }
        return ~crc;
    }
}
