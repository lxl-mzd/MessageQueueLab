// ═══════════════════════════════════════════════════════════════
// Persistence/LogSegment.cs —— 单个段文件对象（文件操作的家）
//
//   三态生活（类型系统保护"正在写的文件不能动"）：
//     Active    → Append 合法，Read 合法，Delete 禁止
//     Sealed    → Append 禁止，Read 合法，Delete 合法（可被合并）
//     Compacted → Append 禁止，Read 合法，Delete 仅被更深的压缩覆盖
//
//   段文件命名（命名即状态）：
//     {queueName}.s{N:000000}.jsonl                         段文件
//     {queueName}.c{start}-{end}.compacted.jsonl            已压缩标记文件
// ═══════════════════════════════════════════════════════════════
using System.Text.RegularExpressions;

namespace MessageQueueLab.Persistence;

public enum SegmentState { Active, Sealed, Compacted }

public sealed class LogSegment
{
    private static readonly Regex SegmentNameRx = new(@"^s(?<n>\d{6})$", RegexOptions.Compiled);
    private static readonly Regex CompactedNameRx = new(@"^c(?<s>\d+)-(?<e>\d+)$", RegexOptions.Compiled);

    public string Path { get; }
    public long StartIndex { get; }
    public long EndIndex { get; }
    public SegmentState State { get; private set; }
    public int LineCount { get; private set; }

    // GC 计数器（率触发器：Acked/Total ≥ 0.5 且 行数 ≥ MinGcLines）
    public int TotalMessages { get; private set; }
    public int AckedMessages { get; private set; }
    public double AckedRatio => TotalMessages == 0 ? 0 : (double)AckedMessages / TotalMessages;

    public void IncrementTotal() => TotalMessages++;
    public void RecordAck() => AckedMessages++;

    private LogSegment(string path, long start, long end, SegmentState state)
    {
        Path = path;
        StartIndex = start;
        EndIndex = end;
        State = state;
    }

    public static LogSegment? TryCreateFromDisk(string path)
    {
        var name = System.IO.Path.GetFileNameWithoutExtension(path);

        var m = SegmentNameRx.Match(name);
        if (m.Success && System.IO.File.Exists(path))
        {
            var n = long.Parse(m.Groups["n"].Value);
            var seg = new LogSegment(path, n, n, SegmentState.Sealed);
            seg.LineCount = System.IO.File.ReadAllLines(path).Count(l => l.Length > 0);
            return seg;
        }

        var m2 = CompactedNameRx.Match(name);
        if (m2.Success && System.IO.File.Exists(path))
        {
            var s = long.Parse(m2.Groups["s"].Value);
            var e = long.Parse(m2.Groups["e"].Value);
            var seg = new LogSegment(path, s, e, SegmentState.Compacted);
            seg.LineCount = System.IO.File.ReadAllLines(path).Count(l => l.Length > 0);
            return seg;
        }

        return null;      // 陌生文件：不动
    }

    public static LogSegment OpenCompacted(string path, long start, long end)
        => new(path, start, end, SegmentState.Compacted);

    public static LogSegment OpenActive(string path, long index)
        => new(path, index, index, SegmentState.Active);

    // ── 活跃段追加（唯一写入口；e 事件计 TotalMessages）──
    public void Append(LogMessage evt)
    {
        if (State != SegmentState.Active)
            throw new InvalidOperationException($"段 {System.IO.Path.GetFileName(Path)} 已封段（{State}），禁止追加");
        System.IO.File.AppendAllText(Path, evt.EncodeLine() + Environment.NewLine);
        LineCount++;
        if (evt.Type == "e") IncrementTotal();
    }

    public void MarkSealed()
    {
        if (State != SegmentState.Active)
            throw new InvalidOperationException($"段 {System.IO.Path.GetFileName(Path)} 不是 Active，无法封段");
        State = SegmentState.Sealed;
    }

    public List<LogMessage> ReadAll()
    {
        var events = new List<LogMessage>();
        if (!System.IO.File.Exists(Path)) return events;

        var lines = System.IO.File.ReadAllLines(Path);
        for (var i = 0; i < lines.Length; i++)
        {
            var line = lines[i];
            if (string.IsNullOrWhiteSpace(line)) continue;

            if (LogMessage.TryDecodeLine(line, out var evt) && evt is not null)
            {
                events.Add(evt);
                continue;
            }
            if (i == lines.Length - 1)
                Console.WriteLine($"⚠️ [{System.IO.Path.GetFileName(Path)}] 末行残损（疑似断电撕断），丢弃该行");
            else
            {
                Console.WriteLine($"‼️ [{System.IO.Path.GetFileName(Path)}] 中段损坏（CRC 验不过）！从坏点起丢弃剩余行——请检查磁盘健康");
                break;
            }
        }
        return events;
    }

    public void Delete()
    {
        if (State == SegmentState.Active)
            throw new InvalidOperationException($"段 {System.IO.Path.GetFileName(Path)} 仍是 Active，禁止删除（正在写的文件不能动）");
        try { System.IO.File.Delete(Path); } catch { }
        LineCount = 0;
    }

    // ── GC 就地重写：tmp 落盘 → 原子上位（新版本行数/计数同步刷新） ──
    public void RewriteInPlace(List<LogMessage> kept)
    {
        if (State == SegmentState.Active)
            throw new InvalidOperationException($"段 {System.IO.Path.GetFileName(Path)} 仍是 Active，禁止重写（正在写的文件不能动）");

        var tmp = Path + ".rewrite.tmp";
        using (var w = new System.IO.StreamWriter(tmp, append: false))
        {
            foreach (var evt in kept)
                w.WriteLine(evt.EncodeLine());
        }
        System.IO.File.Move(tmp, Path, overwrite: true);
        LineCount = kept.Count;
    }
}
