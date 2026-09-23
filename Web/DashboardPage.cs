// ═══════════════════════════════════════════════════════════════
// Web/DashboardPage.cs —— 健康看板网页（零依赖单文件：无CDN、无外部库）
//   访问 http://localhost:5080/
//   - 四大指标卡 + 队列一览 + 交换机一览 + 死信货架(双按钮) + 事件流水
// ═══════════════════════════════════════════════════════════════
namespace MessageQueueLab.Web;

public static class DashboardPage
{
    public const string Html = """
<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>自造消息队列 · 健康看板</title>
<style>
  * { box-sizing: border-box; margin: 0; }
  body { background:#0f172a; color:#e2e8f0; font-family:"Microsoft YaHei",system-ui,sans-serif; padding:24px; }
  h1 { font-size:22px; color:#7dd3fc; }
  .sub { color:#94a3b8; font-size:13px; margin:6px 0 20px; }
  .alive { color:#4ade80; } .down { color:#f87171; }
  .grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(180px,1fr)); gap:14px; max-width:1000px; }
  .card { border-radius:14px; padding:18px 20px; }
  .num { font-size:40px; font-weight:700; line-height:1; }
  .label { margin-top:8px; font-size:13px; opacity:.85; }
  .blue   { background:#1d4ed8; }
  .orange { background:#c2410c; }
  .green  { background:#15803d; }
  .red    { background:#b91c1c; }
  h2 { font-size:16px; margin:28px 0 10px; color:#7dd3fc; }
  table { width:100%; max-width:1000px; border-collapse:collapse; font-size:13px; }
  th,td { text-align:left; padding:8px 10px; border-bottom:1px solid #1e293b; }
  th { color:#94a3b8; font-weight:600; }
  .blink { padding:4px 10px; border:0; border-radius:8px; cursor:pointer; color:#fff; font-size:12px; margin-right:4px; }
  .b-ack { background:#dc2626; } .b-revive { background:#0891b2; }
  code { background:#1e293b; padding:2px 6px; border-radius:6px; }
  #feed { max-width:1000px; font-family:Consolas,monospace; font-size:12.5px; background:#111827;
          border:1px solid #1e293b; border-radius:10px; padding:12px; max-height:300px; overflow:auto; }
  #feed div { padding:2px 0; border-bottom:1px dashed #1f2937cc; }
  .ts { color:#64748b; margin-right:8px; }
</style>
</head>
<body>
  <h1>🚦 自造消息队列 · 健康看板</h1>
  <div class="sub" id="health">连接中…</div>

  <div class="grid">
    <div class="card blue"  ><div class="num" id="s-ready">–</div><div class="label">📥 就绪（墙上小票）</div></div>
    <div class="card orange"><div class="num" id="s-locked">–</div><div class="label">🔒 锁定区（借走待干）</div></div>
    <div class="card green" ><div class="num" id="s-disk">–</div><div class="label">📒 主账本待处理</div></div>
    <div class="card red"   ><div class="num" id="s-dead">–</div><div class="label">☠️ 死信待人工处置</div></div>
  </div>

  <h2>🧭 队列一览（各柜台明细）</h2>
  <table>
    <thead><tr><th>队列</th><th>就绪</th><th>锁定区</th><th>主账本待处理</th><th>☠️死信</th></tr></thead>
    <tbody id="queues-body"></tbody>
  </table>

  <h2>🪄 交换机（分拣机）</h2>
  <table>
    <thead><tr><th>名字</th><th>类型</th><th>绑定规则</th></tr></thead>
    <tbody id="ex-body"></tbody>
  </table>

  <h2>☠️ 死信货架 <span style="font-size:12px;color:#94a3b8">（normal 队列 · 店长审案：右侧两个按钮直接调用接口）</span></h2>
  <table>
    <thead><tr><th>身份证(前16)</th><th>内容</th><th>重试次数</th><th>店长的处置</th></tr></thead>
    <tbody id="dlq-body"></tbody>
  </table>

  <h2>📜 事件流水（最近 200 条）</h2>
  <div id="feed"></div>
  <div class="sub" style="margin-top:10px">自动刷新 2s ｜ <code>GET /api/stats</code> <code>GET /api/dlq</code> <code>GET /api/events</code></div>

<script>
function e_esc(s) { const d = document.createElement("span"); d.textContent = s; return d.innerHTML; }

async function refresh() {
  try {
    const [stats, dlq, events, health, queues, exchanges] = await Promise.all([
      fetch("/api/stats").then(r => r.json()),
      fetch("/api/q/normal/dlq").then(r => r.json()),
      fetch("/api/events").then(r => r.json()),
      fetch("/health").then(r => r.json()).catch(() => null),
      fetch("/api/queues").then(r => r.json()),
      fetch("/api/ex").then(r => r.json())
    ]);

    document.getElementById("s-ready").textContent  = stats.readyInMemory;
    document.getElementById("s-locked").textContent = stats.lockedInMemory;
    document.getElementById("s-disk").textContent   = stats.pendingOnDisk;
    document.getElementById("s-dead").textContent   = stats.deadLetters;
    document.getElementById("health").innerHTML =
      (health && health.status === "alive")
        ? '<span class="alive">● 服务存活</span>  ' + health.now
        : '<span class="down">● 服务失联</span>';

    // 队列一览
    const qbody = document.getElementById("queues-body");
    qbody.innerHTML = "";
    (Array.isArray(queues) ? queues : queues.value || []).forEach(q => {
      const tr = document.createElement("tr");
      tr.innerHTML = "<td><b>" + e_esc(q.queue) + "</b></td><td>" + q.readyInMemory +
        "</td><td>" + q.lockedInMemory + "</td><td>" + q.pendingOnDisk + "</td><td>" + q.deadLetters + "</td>";
      qbody.appendChild(tr);
    });

    // 交换机一览
    const ebody = document.getElementById("ex-body");
    ebody.innerHTML = "";
    (Array.isArray(exchanges) ? exchanges : exchanges.value || []).forEach(e => {
      const tr = document.createElement("tr");
      const tdName = document.createElement("td");
      tdName.textContent = e.name;
      const tdType = document.createElement("td");
      tdType.textContent = e.type === "direct" ? "直连 direct" : (e.type === "fanout" ? "扇出 fanout" : "主题 topic");
      const tdBind = document.createElement("td");
      tdBind.innerHTML = e.bindings && e.bindings.length
        ? e.bindings.map(b => b.routingKey ? (e_esc(b.routingKey) + " → " + e_esc(b.queue)) : ("广播→ " + e_esc(b.queue))).join("<br>")
        : "（无绑定）";
      tr.appendChild(tdName); tr.appendChild(tdType); tr.appendChild(tdBind);
      ebody.appendChild(tr);
    });

    // 死信货架（纯 DOM 编程，避免引号地狱）
    const tbody = document.getElementById("dlq-body");
    tbody.innerHTML = "";
    (Array.isArray(dlq) ? dlq : dlq.value || []).forEach(d => {
      const tr  = document.createElement("tr");

      const tdId = document.createElement("td");
      const code = document.createElement("code");
      code.textContent = d.id.slice(0,16) + "…";
      tdId.appendChild(code);
      tr.appendChild(tdId);

      const tdContent = document.createElement("td");
      tdContent.textContent = d.content;
      tr.appendChild(tdContent);

      const tdRetry = document.createElement("td");
      tdRetry.textContent = d.retryCount;
      tr.appendChild(tdRetry);

      const tdOp = document.createElement("td");
      const btnAck    = document.createElement("button");
      btnAck.className = "blink b-ack"; btnAck.textContent = "🧹 善后销账";
      btnAck.onclick   = () => dlqAction("ack", d.id);
      const btnRevive  = document.createElement("button");
      btnRevive.className = "blink b-revive"; btnRevive.textContent = "🔁 送回重试";
      btnRevive.onclick   = () => dlqAction("revive", d.id);
      tdOp.appendChild(btnAck);
      tdOp.appendChild(btnRevive);
      tr.appendChild(tdOp);

      tbody.appendChild(tr);
    });

    document.getElementById("feed").innerHTML =
      events.map(e => "<div><span class='ts'>" + e.ts + "</span>" + e.msg + "</div>").join("");

  } catch (err) {
    document.getElementById("health").innerHTML = '<span class="down">● 数据拉取失败：' + err + '</span>';
  }
}

async function dlqAction(kind, id) {
  await fetch("/api/q/normal/dlq/" + id + "/" + kind, { method: "POST" });
  refresh();
}

setInterval(refresh, 2000);
refresh();
</script>
</body>
</html>
""";
}
