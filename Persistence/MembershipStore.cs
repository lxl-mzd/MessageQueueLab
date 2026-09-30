// ═══════════════════════════════════════════════════════════════
// Persistence/MembershipStore.cs —— Raft 成员表持久化（每节点自己一份）
//
//   data/_cluster/members.json:
//     { "uptoSeq": 3, "table": [ {"node":"node-1","url":"http://node-1:8080"}, ... ] }
//
//   原则（对应 步骤 C 的成员变更协议）：
//     · 内容走 Quorum 投票（m 事件），每节点 apply 后**各自原子落盘**
//     · 崩溃恢复：启动时读文件 → 成员表无损回归（与 WAL 其他身份账同类）
//   原子性：tmp 文件 + File.Move（我们就吃 File.Move 原子改名的饭，同 LogCleaner）
// ═══════════════════════════════════════════════════════════════
using System.Text.Json;

namespace MessageQueueLab.Persistence;

public sealed record RaftMember(string Node, string Url);

public sealed class MembershipStore
{
    private readonly string _file;
    private long _seq;
    private List<RaftMember> _table = new();
    private readonly object _lock = new();
    private readonly Action<string>? _emit;

    public long Seq => _seq;
    public IReadOnlyList<RaftMember> Table => _table;

    public MembershipStore(string dataDir, Action<string>? emit = null)
    {
        _emit = emit;
        var dir = System.IO.Path.Combine(dataDir, "_cluster");
        System.IO.Directory.CreateDirectory(dir);
        _file = System.IO.Path.Combine(dir, "members.json");
        Load();
    }

    public void Seed(List<RaftMember> bootstrap)
    {
        lock (_lock)
        {
            if (_table.Count > 0) return;      // 磁盘已有真相
            _table = bootstrap;
            _seq = 1;
            Persist();
        }
    }

    public bool Contains(string node) { lock (_lock) return _table.Any(m => m.Node == node); }
    public bool TryGetUrl(string node, out string url)
    {
        lock (_lock)
        {
            var m = _table.FirstOrDefault(x => x.Node == node);
            url = m?.Url ?? "";
            return m is not null;
        }
    }

    public bool ContainsUrl(string url)
        { lock (_lock) return _table.Any(m => string.Equals(m.Url, url, StringComparison.OrdinalIgnoreCase)); }

    /// <summary>Quorum 已通过后调用：**替换成员表 + 原子落盘**。每一台节点 apply 后都调它。</summary>
    public void Apply(long seq, List<RaftMember> table)
    {
        lock (_lock)
        {
            if (seq <= _seq) { _emit?.Invoke($"📎 [membership] seq={seq} 已过期丢弃（当前 {_seq}）"); return; }
            _seq = seq;
            _table = table;
            Persist();
            _emit?.Invoke($"📇 [membership] seq={seq} 成员表更新 → {string.Join(',', table.Select(t => t.Node))}");
        }
    }

    public long NextSeq() { lock (_lock) return _seq + 1; }

    private void Load()
    {
        try
        {
            if (!System.IO.File.Exists(_file)) return;
            var json = System.IO.File.ReadAllText(_file);
            var snap = JsonSerializer.Deserialize<SnapshotModel>(json);
            if (snap is null || snap.Table is null) return;
            _seq = snap.UptoSeq;
            _table = snap.Table.Select(t => new RaftMember(t.Node, t.Url)).ToList();
            _emit?.Invoke($"📇 [membership] 回放成员表（seq={_seq}，{Table.Count()} 名成员）");
        }
        catch (Exception ex)
        {
            _emit?.Invoke($"‼️ [membership] 回放失败（将沿用空表）：{ex.Message}");
        }
    }

    private void Persist()
    {
        var tmp = _file + ".tmp";
        var json = JsonSerializer.Serialize(new SnapshotModel
        {
            UptoSeq = _seq,
            Table = _table.Select(t => new SnapshotMember { Node = t.Node, Url = t.Url }).ToList(),
        });
        System.IO.File.WriteAllText(tmp, json);
        if (System.IO.File.Exists(_file)) System.IO.File.Delete(_file);
        System.IO.File.Move(tmp, _file);
    }

    internal sealed class SnapshotModel
    {
        public long UptoSeq { get; set; }
        public List<SnapshotMember> Table { get; set; } = new();
    }
    internal sealed class SnapshotMember { public string Node { get; set; } = ""; public string Url { get; set; } = ""; }
}
