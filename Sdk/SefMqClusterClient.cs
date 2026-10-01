// ═══════════════════════════════════════════════════════════════
// Sdk/SefMqClusterClient.cs —— 集群找王客户端（SDK 内部共用底座）
//
//   用户只管 .Send()/.BeginConsume()，找王的事 SDK 自己做：
//     · 种子节点表：bootstrap.servers="http://h1:5081,http://h2:5082"（兼容旧 bootstrap.url 单地址）
//     · 内存缓存 Leader：命中直接打，503/连不上就换下一个并更新缓存
//     · 单机模式：只有一个种子，行为退化为直连
// ═══════════════════════════════════════════════════════════════
using System.Text.Json;

namespace MessageQueueLab.Sdk;

public sealed class SefMqClusterClient : IDisposable
{
    private readonly HttpClient _http = new();
    private readonly List<string> _seeds;
    private readonly object _seedsLock = new();
    private volatile string? _leaderUrl;   // 内存缓存的王地址（换王时自动刷新）
    private int _roundRobin;

    public SefMqClusterClient(SefMqConfig config)
    {
        var raw = config.Get("bootstrap.servers", "");
        if (string.IsNullOrWhiteSpace(raw)) raw = config.Get("bootstrap.url", "");
        _seeds = raw.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).ToList();
        if (_seeds.Count == 0) throw new ArgumentException("缺少配置 bootstrap.servers（或旧 bootstrap.url）");
    }

    public IReadOnlyList<string> Seeds { get { lock (_seedsLock) return _seeds.ToList(); } }
    public string? CachedLeader => _leaderUrl;

    /// <summary>
    /// 向任一活节点要成员表（/api/raft/members），把新发现的 URL 并入种子。
    /// 新节点加入后，老种子照样能问到它 —— 种子表自学习，永不过期。
    /// 失败静默（单机/网络抖动时不影响主流程）。
    /// </summary>
    public async Task RefreshSeedsAsync(CancellationToken ct = default)
    {
        List<string> snapshot;
        lock (_seedsLock) snapshot = _seeds.ToList();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, timeout.Token);
        foreach (var baseUrl in snapshot)
        {
            try
            {
                var json = await _http.GetStringAsync(baseUrl + "/api/raft/members", linked.Token);
                using var doc = JsonDocument.Parse(json);
                if (!doc.RootElement.TryGetProperty("table", out var table)) continue;
                var added = 0;
                lock (_seedsLock)
                {
                    foreach (var m in table.EnumerateArray())
                    {
                        if (m.TryGetProperty("url", out var u) && u.GetString() is string url
                            && url.Length > 0 && !_seeds.Contains(url)) { _seeds.Add(url); added++; }
                    }
                }
                if (added > 0) return;   // 问到一家就够
            }
            catch { /* 下一个种子 */ }
        }
    }

    /// <summary>按“缓存的王优先 → 轮询兜底”顺序试，直到拿到非 503 响应或全部失败。</summary>
    public async Task<HttpResponseMessage> CallLeaderAsync(
        Func<string, Task<HttpResponseMessage>> fn, CancellationToken ct = default)
    {
        var order = BuildOrder();
        HttpResponseMessage? last503 = null;
        Exception? lastEx = null;
        var skipped = 0;
        foreach (var baseUrl in order)
        {
            HttpResponseMessage resp;
            try { resp = await fn(baseUrl); }
            catch (Exception ex) { lastEx = ex; skipped++; continue; }   // 连不上 → 下一个
            if ((int)resp.StatusCode == 503) { last503 = resp; skipped++; continue; }  // 非王 → 下一个
            _leaderUrl = baseUrl;                             // 命中 → 缓存王地址
            if (skipped > 0) await RefreshSeedsAsync(ct);      // 绕过路才找到王 → 顺手向王要最新成员表
            return resp;
        }
        if (last503 is not null) return last503;
        throw lastEx ?? new HttpRequestException("集群无可用节点");
    }

    public Task<HttpResponseMessage> PostAsync(string path, HttpContent? body, CancellationToken ct = default)
        => CallLeaderAsync(baseUrl => _http.PostAsync(baseUrl + path, body, ct), ct);

    public Task<HttpResponseMessage> GetAsync(string path, CancellationToken ct = default)
        => CallLeaderAsync(baseUrl => _http.GetAsync(baseUrl + path, ct), ct);

    private List<string> BuildOrder()
    {
        List<string> seeds;
        lock (_seedsLock) seeds = _seeds.ToList();
        var order = new List<string>(seeds.Count);
        // 缓存的王排第一（大概率命中，少一次 503 试探）
        if (_leaderUrl is not null && seeds.Contains(_leaderUrl)) order.Add(_leaderUrl);
        var start = Math.Abs(System.Threading.Interlocked.Increment(ref _roundRobin)) % Math.Max(1, seeds.Count);
        for (var i = 0; i < seeds.Count; i++)
        {
            var u = seeds[(start + i) % seeds.Count];
            if (!order.Contains(u)) order.Add(u);
        }
        return order;
    }

    public void Dispose() => _http.Dispose();
}
