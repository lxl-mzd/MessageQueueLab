// ═══════════════════════════════════════════════════════════════
// Sdk/SefMqClusterClient.cs —— 集群找王客户端（SDK 内部共用底座）
//
//   用户只管 .Send()/.BeginConsume()，找王的事 SDK 自己做：
//     · 种子节点表：bootstrap.servers="http://h1:5081,http://h2:5082"（兼容旧 bootstrap.url 单地址）
//     · 内存缓存 Leader：命中直接打，503/连不上就换下一个并更新缓存
//     · 单机模式：只有一个种子，行为退化为直连
// ═══════════════════════════════════════════════════════════════
namespace MessageQueueLab.Sdk;

public sealed class SefMqClusterClient : IDisposable
{
    private readonly HttpClient _http = new();
    private readonly List<string> _seeds;
    private volatile string? _leaderUrl;   // 内存缓存的王地址（换王时自动刷新）
    private int _roundRobin;

    public SefMqClusterClient(SefMqConfig config)
    {
        var raw = config.Get("bootstrap.servers", "");
        if (string.IsNullOrWhiteSpace(raw)) raw = config.Get("bootstrap.url", "");
        _seeds = raw.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).ToList();
        if (_seeds.Count == 0) throw new ArgumentException("缺少配置 bootstrap.servers（或旧 bootstrap.url）");
    }

    public IReadOnlyList<string> Seeds => _seeds;
    public string? CachedLeader => _leaderUrl;

    /// <summary>按“缓存的王优先 → 轮询兜底”顺序试，直到拿到非 503 响应或全部失败。</summary>
    public async Task<HttpResponseMessage> CallLeaderAsync(
        Func<string, Task<HttpResponseMessage>> fn, CancellationToken ct = default)
    {
        var order = BuildOrder();
        HttpResponseMessage? last503 = null;
        Exception? lastEx = null;
        foreach (var baseUrl in order)
        {
            HttpResponseMessage resp;
            try { resp = await fn(baseUrl); }
            catch (Exception ex) { lastEx = ex; continue; }   // 连不上 → 下一个
            if ((int)resp.StatusCode == 503) { last503 = resp; continue; }  // 非王 → 下一个
            _leaderUrl = baseUrl;                             // 命中 → 缓存王地址
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
        var order = new List<string>(_seeds.Count);
        // 缓存的王排第一（大概率命中，少一次 503 试探）
        if (_leaderUrl is not null && _seeds.Contains(_leaderUrl)) order.Add(_leaderUrl);
        var start = Math.Abs(System.Threading.Interlocked.Increment(ref _roundRobin)) % Math.Max(1, _seeds.Count);
        for (var i = 0; i < _seeds.Count; i++)
        {
            var u = _seeds[(start + i) % _seeds.Count];
            if (!order.Contains(u)) order.Add(u);
        }
        return order;
    }

    public void Dispose() => _http.Dispose();
}
