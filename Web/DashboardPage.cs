// ═══════════════════════════════════════════════════════════════
// Web/DashboardPage.cs —— 运维看板 v4（单文件零依赖：无 CDN 无外部库）
//
//   ① 集群总览：三节点并排卡片（王位/term/计数）+ 全局同步判定
//   ② 单节点视图：每个节点的详细计数 + 运维按钮（健康检查/事件流/冒烟测试）
//   ③ 死信货架：replicate 到任意 node 的全局死信、复活/删除按钮
//   ④ 事件流 + 交换机绑定
//   ⑤ 自动刷新（可暂停/选频），手动刷新即拉全部节点
//
//   CORS 已全开放（Program.UseCors），跨节点 fetch 无障碍。
// ═══════════════════════════════════════════════════════════════
namespace MessageQueueLab.Web;

public static class DashboardPage
{
    public const string Html = """
<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>自造消息队列 · 运维看板</title>
<style>
  * { box-sizing:border-box; margin:0; font-family:"Segoe UI","Microsoft YaHei",system-ui,sans-serif; }
  body { background:#0b1020; color:#dbe6f3; padding:24px 20px 60px; min-height:100vh; }
  a { color:#7dd3fc; text-decoration:none; }
  h1 { font-size:24px; color:#93c5fd; display:flex; align-items:center; gap:10px; }
  h2 { font-size:15px; margin:30px 0 12px; color:#93c5fd; letter-spacing:.4px; }
  .sub { color:#8ea3bd; font-size:13px; margin:6px 0 22px; }
  .wrap { max-width:1200px; margin:0 auto; }

  .toolbar { display:flex; gap:10px; flex-wrap:wrap; align-items:center; margin-bottom:22px; }
  .toolbar input, .toolbar select { background:#131a2e; color:#dbe7f4; border:1px solid #2b3ba8; border-radius:8px; padding:7px 10px; font-size:13px; }
  .btn { border:0; border-radius:9px; padding:8px 14px; font-size:13px; color:#fff; cursor:pointer; transition:transform .06s, filter .15s; }
  .btn:active { transform:scale(.96); }
  .btn:hover { filter:brightness(1.15); }
  .btn-blue { background:#2563eb; } .btn-slate { background:#334155; } .btn-teal { background:#0d9488; }
  .btn-amber { background:#b45309; } .btn-rose { background:#be123c; } .btn-green { background:#15803d; }
  .btn[disabled] { opacity:.45; cursor:not-allowed; }

  /* 集群总览卡 */
  .nodegrid { display:grid; grid-template-columns:repeat(auto-fit,minmax(300px,1fr)); gap:16px; max-width:1200px; }
  .node { background:linear-gradient(180deg,#131c33,#101728); border:1px solid #25335c; border-radius:16px; padding:18px 20px; box-shadow:0 6px 22px #0008; }
  .node.hdr { display:flex; justify-content:space-between; align-items:center; margin-bottom:12px; }
  .node .name { font-size:16px; font-weight:700; }
  .badge { padding:3px 10px; border-radius:999px; font-size:12px; font-weight:600; }
  .b-king { background:#15803d; color:#e7ffe9; }
  .b-follower { background:#334155; color:#cbd5e1; }
  .b-candidate { background:#b45309; color:#fff7ed; }
  .b-dead  { background:#7f1d1d; color:#fee2e2; }
  .mini { font-size:12px; color:#8ea3bd; }
  .counts { display:grid; grid-template-columns:repeat(4,1fr); gap:8px; margin:12px 0; }
  .cnt { background:#0d1526; border:1px solid #1f2b52; border-radius:10px; text-align:center; padding:10px 4px; }
  .cnt .n { font-size:22px; font-weight:700; }
  .cnt .t { font-size:11px; color:#8ea3bd; margin-top:3px; }
  .btns { display:flex; gap:8px; flex-wrap:wrap; }
  .node .row2 { display:flex; gap:8px; margin-top:8px; font-size:12px; color:#8ea3bd; }

  /* 汇总带 */
  .summary { display:grid; grid-template-columns:repeat(auto-fit,minmax(160px,1fr)); gap:12px; margin-bottom:26px; }
  .scard { border-radius:14px; padding:16px 18px; text-align:center; }
  .scard .n { font-size:32px; font-weight:800; line-height:1; }
  .scard .t { margin-top:6px; font-size:12px; opacity:.8; }
  .c-blue { background:#1e40af; } .c-amber { background:#9a3412; } .c-green { background:#166534; } .c-rose { background:#9f1239; }

  table { width:100%; border-collapse:collapse; font-size:13px; background:#0d1424; border:1px solid #1e2b4e; border-radius:12px; overflow:hidden; }
  th,td { text-align:left; padding:9px 12px; border-bottom:1px solid #16213c; }
  th { color:#8ea3bd; font-weight:600; background:#101830; }
  tr:last-child td { border-bottom:0; }
  tr:hover td { background:#131c33; }

  #feed { font-family:Consolas,"Cascadia Mono",monospace; font-size:12.5px; background:#0d1424; border:1px solid #1e2b4e;
          border-radius:12px; padding:12px; max-height:320px; overflow:auto; }
  #feed div { padding:2.5px 0; border-bottom:1px dashed #16233f; }
  .ts { color:#52627e; margin-right:8px; }

  /* 弹窗 */
  #modal { position:fixed; inset:0; background:#000a; display:none; align-items:center; justify-content:center; z-index:50; }
  #modal .box { width:min(900px,92vw); max-height:86vh; overflow:auto; background:#0f172b; border:1px solid #2b3768; border-radius:16px; padding:18px; }
  #modal h3 { margin:0 0 10px; color:#93c5fd; font-size:15px; }
  #modal pre { font-size:12px; line-height:1.55; white-space:pre-wrap; word-break:break-all; background:#0a1020; border-radius:10px; padding:12px; max-height:64vh; overflow:auto; }
  .toast { position:fixed; right:18px; bottom:18px; background:#123f2b; border:1px solid #1d7a4d; color:#d1fae5; padding:10px 14px; border-radius:10px; font-size:13px; z-index:60; }
</style>
</head>
<body>
<div class="wrap">
  <h1>🚦 自造消息队列 · 运维看板</h1>
  <div class="sub">Raft 动态主权 · Quorum 三阶段 · DLX 死信 · 幂等查重 —— 全集群一屏巡检</div>

  <!-- 工具栏 -->
  <div class="toolbar">
    <span class="mini">节点端口：</span>
    <input id="nodeports" size="14" value="5081,5082,5083">
    <button class="btn btn-blue"   onclick="refresh()">🔄 全体刷新</button>
    <button class="btn btn-slate"  id="tbtn" onclick="toggleAuto()">⏸ 自动刷新 (开)</button>
    <select id="interval" onchange="resetTimer()">
      <option value="5000">5s</option><option value="15000" selected>15s</option><option value="60000">60s</option>
    </select>
    <button class="btn btn-teal" onclick="showClusterStatus()">🧮 集群速览</button>
  </div>

  <!-- 集群汇总条 -->
  <div class="summary">
    <div class="scard c-blue"><div class="n" id="ag-ready">–</div><div class="t">📥 全集群就绪</div></div>
    <div class="scard c-amber"><div class="n" id="ag-locked">–</div><div class="t">🔒 全集群锁定区</div></div>
    <div class="scard c-green"><div class="n" id="ag-disk">–</div><div class="t">📒 全集群主账 SSD</div></div>
    <div class="scard c-rose"><div class="n" id="ag-dead">–</div><div class="t">☠️ 全集群死信</div></div>
    <div class="scard" style="background:#334155"><div class="n" id="ag-alive">–</div><div class="t">🧭 存活节点 / 王位状态</div></div>
  </div>

  <h2>🖥️ 服务器节点（每台一份，可逐台操作）</h2>
  <div class="nodegrid" id="nodegrid"></div>

  <h2>🧭 全集群队列明细</h2>
  <table><thead><tr><th>队列</th><th>就绪</th><th>锁定区</th><th>主账待处理</th><th>☠️ 死信</th></tr></thead>
    <tbody id="queues-body"></tbody></table>

  <h2>🪄 交换机（data/_exchanges.json 快照）</h2>
  <table><thead><tr><th>交换机</th><th>类型</th><th>绑定规则</th></tr></thead>
    <tbody id="ex-body"></tbody></table>

  <h2>☠️ 死信货架（三队列合并）</h2>
  <table><thead><tr><th>队列</th><th>身份证</th><th>内容</th><th>重试次数</th><th>处置</th></tr></thead>
    <tbody id="dlq-body"></tbody></table>

  <h2>📜 事件流水（最近 200 条 · 来自当前王）</h2>
  <div id="feed"></div>
  <div class="sub" style="margin-top:12px">数据接口：<code>/health</code> <code>/api/raft/status</code> <code>/api/queues</code> <code>/api/events</code> <code>/api/q/{q}/dlq</code></div>
</div>

<div id="modal"><div class="box"><h2 id="m-title"></h2><pre id="m-body"></pre>
  <div style="text-align:right;margin-top:10px"><button class="btn btn-slate" onclick="closeModal()">关闭</button></div></div></div>
<div id="toast" class="toast" style="display:none"></div>

<script>
const NODES = () => document.getElementById("nodeports").value.split(",").map(s=>s.trim()).filter(Boolean);
let timer = setInterval(refresh, 15000), autoOn = true;
let toastT = null;
function e_esc(s){ const d=document.createElement("span"); d.textContent = s??""; return d.innerHTML; }
function toast(msg){ const t=document.getElementById("toast"); t.textContent=msg; t.style.display="block";
  clearTimeout(toastT); toastT=setTimeout(()=>t.style.display="none", 2600); }
function resetTimer(){ clearInterval(timer); if(autoOn) timer=setInterval(refresh, +document.getElementById("interval").value); }
function toggleAuto(){ autoOn=!autoOn; document.getElementById("tbtn").textContent = autoOn?"⏸ 自动刷新 (开)":"▶ 自动刷新 (关)";
  resetTimer(); }

async function jget(base, path){ try { const r = await fetch(base+path); return { code:r.status, body:await r.text() }; } catch(e){ return { code:0, body:"" }; } }

/* ── 集群汇总 + 每个节点卡片 ── */
async function refresh(){
  const ports = NODES();
    const rows = await Promise.all(ports.map(async p => {
      const b = "http://"+location.hostname+":"+p;
      let [health, raft, queues] = await Promise.all([
        jget(b,"/health"), jget(b,"/api/raft/status"), jget(b,"/api/queues") ]);
      if (raft.code!==200){        // 单机模式没有 raft 端点 → 用 cluster/status 兜底
        const cs = await jget(b,"/api/cluster/status");
        let cj=null; try{ cj=JSON.parse(cs.body); }catch(e){}
        if (cj) raft = { code:200, body: JSON.stringify({ node:cj.node||(":"+p), role:(cj.role==="Leader"||cj.role==="single")?"Leader":cj.role, term:"–", leaderId:cj.node }) };
      }
      let rj=null, qj=null;
    try{ rj = raft.body?JSON.parse(raft.body):null; }catch(e){}
    try{ qj = queues.body?JSON.parse(queues.body):null; }catch(e){}
    const agg = { ready:0, locked:0, disk:0, dead:0 };
    (Array.isArray(qj)?qj:[]).forEach(q=>{ agg.ready+=q.readyInMemory; agg.locked+=q.lockedInMemory; agg.disk+=q.pendingOnDisk; agg.dead+=q.deadLetters; });
    return { p, b, alive: health.code===200 && health.body.includes('"status":"alive"'),
             raft: rj, sum: agg, queues: Array.isArray(qj)?qj:[] };
  }));

  // 汇总
  let agR=0, agL=0, agD=0, agDead=0, aliveN=0, kingN=0;
  rows.forEach(r=>{ agR+=r.sum.ready; agL+=r.sum.locked; agD+=r.sum.disk; agDead+=r.sum.dead;
    if(r.alive) aliveN++; if(r.raft && r.raft.role==="Leader") kingN++; });
  document.getElementById("ag-ready").textContent=agR;
  document.getElementById("ag-locked").textContent=agL;
  document.getElementById("ag-disk").textContent=agD;
  document.getElementById("ag-dead").textContent=agDead;
  document.getElementById("ag-alive").innerHTML = '<span style="color:'+(kingN===1?'#86efac':'#fca5a5')+'">'
    + aliveN+'/'+rows.length+' 在线 · '+(kingN===1?'王位正常':'王位异常!')+'</span>';

  // 节点卡片
  const grid = document.getElementById("nodegrid"); grid.innerHTML="";
  rows.forEach(r=>{
    const role = r.raft ? (r.raft.role||"Down") : "Down";
    const cls  = role==="Leader"?"b-king":(role==="Follower"?"b-follower":(role==="Candidate"?"b-candidate":"b-dead"));
    const div=document.createElement("div"); div.className="node";
    div.innerHTML =
      '<div class="hdr" style="display:flex;justify-content:space-between;align-items:center">'
        +'<div><div class="name">'+e_esc(r.raft?r.raft.node:(":"+r.p))+'</div>'
        +'<div class="mini">http://'+location.hostname+':'+e_esc(r.p)+'</div></div>'
        +'<div style="text-align:right"><span class="badge '+cls+'">'+e_esc(badgeText(role))+'</span>'
        +'<div class="mini" style="margin-top:6px">term='+(r.raft?r.raft.term:"–")
        +' &nbsp; leader='+(r.raft?(r.raft.leaderId||"-"):"-")+'</div></div>'
      +'</div>'
      +'<div class="counts">'
        +'<div class="cnt"><div class="n">'+r.sum.ready+'</div><div class="t">📥 就绪</div></div>'
        +'<div class="cnt"><div class="n">'+r.sum.locked+'</div><div class="t">🔒 锁定</div></div>'
        +'<div class="cnt"><div class="n">'+r.sum.disk+'</div><div class="t">📒 主账</div></div>'
        +'<div class="cnt"><div class="n">'+r.sum.dead+'</div><div class="t">☠️ 死信</div></div>'
      +'</div>'
      +'<div class="row2">'+(r.alive?'<span style="color:#86efac">🩺 HTTP 正常</span>':'<span style="color:#fca5a5">🩺 HTTP 失联</span>')+'</div>'
      +'<div style="margin-top:12px;display:flex;gap:8px;flex-wrap:wrap">'
        +'<button class="btn btn-blue"  onclick="readNode(\''+r.p+'\')">🩺 健康检查</button>'
        +'<button class="btn btn-slate" onclick="eventsNode(\''+r.p+'\')">📜 事件流</button>'
        +'<button class="btn btn-teal"  onclick="probeNode(\''+r.p+'\')">🧪 冒烟测试</button>'
      +'</div>';
    grid.appendChild(div);
  });

  // 全集群队列明细 (取第一个存活节点)
  const first = rows.find(r=>r.alive);
  const qb=document.getElementById("queues-body"); qb.innerHTML="";
  if (first){ (first.queues||[]).forEach(q=>{
    const tr=document.createElement("tr");
    tr.innerHTML='<td><b>'+e_esc(q.queue)+'</b></td><td>'+q.readyInMemory+'</td><td>'+q.lockedInMemory
                +'</td><td>'+q.pendingOnDisk+'</td><td>'+q.deadLetters+'</td>';
    qb.appendChild(tr);
  });}

  // 交换机
  if (first){
    const exR = await jget("http://"+location.hostname+":"+first.p, "/api/ex");
    let ex=[]; try{ ex=JSON.parse(exR.body); }catch(e){}
    const eb=document.getElementById("ex-body"); eb.innerHTML="";
    ex.forEach(e=>{
      const tr=document.createElement("tr");
      tr.innerHTML='<td><b>'+e_esc(e.name)+'</b></td><td>'+e_esc(e.type)+'</td><td>'
        + (e.bindings&&e.bindings.length ? e.bindings.map(b=> b.routingKey? e_esc(b.routingKey)+" → "+e_esc(b.queue) : "广播→ "+e_esc(b.queue)).join("<br>") : "（无绑定）")
        +'</td>';
      eb.appendChild(tr);
    });
  }

  // 死信货架：扫 normal/vip/error
  const dlqRows=[]; const qsToScan=["normal","vip","error"];
  if (first) {
    for (const q of qsToScan){
      const r = await jget("http://"+location.hostname+":"+first.p, "/api/q/"+q+"/dlq");
      let l=[]; try{ l=JSON.parse(r.body); }catch(e){}
      l.forEach(d=> dlqRows.push({q, d}));
    }
  }
  const tb=document.getElementById("dlq-body"); tb.innerHTML="";
  dlqRows.forEach(({q,d})=>{
    const tr=document.createElement("tr");
    tr.innerHTML='<td>'+e_esc(q)+'</td><td><code>'+e_esc(d.id.slice(0,14))+'…</code></td><td>'+e_esc(d.content)
      +'</td><td>'+d.retryCount+'</td>';
    const op=document.createElement("td");
    const bA=document.createElement("button"); bA.className="btn btn-rose";  bA.textContent="🧹 善后";   bA.onclick=()=>dlqAct(q,d.id,"ack");
    const bR=document.createElement("button"); bR.className="btn btn-teal";  bR.textContent="🔁 复活";   bR.onclick=()=>dlqAct(q,d.id,"revive");
    bA.style.marginRight="6px"; op.appendChild(bA); op.appendChild(bR); tr.appendChild(op);
    tb.appendChild(tr);
  });
}
function badgeText(role){
  return role;   // 直接 Leader / Follower / Candidate / Down
}

/* ── 运维按钮 ── */
async function dlqAct(q, id, kind){
  const p = NODES()[0];
  await fetch("http://"+location.hostname+":"+p+"/api/q/"+q+"/dlq/"+id+"/"+kind, { method:"POST" });
  toast(kind==="ack" ? "死信已销账" : "死信复活, count 归 0");
  setTimeout(refresh, 400);
}
async function readNode(p){
  const [h, r] = await Promise.all([
    jget("http://"+location.hostname+":"+p, "/health"),
    jget("http://"+location.hostname+":"+p, "/api/raft/status")]);
  let lines = [];
  try {
    const hJson = JSON.parse(h.body||"{}");
    lines.push((hJson && hJson.status==="alive") ? "🩺 服务存活" : "🩺 服务无响应");
    if (hJson && hJson.now) lines.push("🕒 服务器时间：" + hJson.now);
  } catch(e){ lines.push("🩺 /health 无法解析"); }
  try {
    const rJson = JSON.parse(r.body||"{}");
    lines.push("👤 节点：" + (rJson.node||"–"));
    lines.push("🎖️ 角色：" + (rJson.role||"–"));
    lines.push("🔢 任期 term：" + (rJson.term ?? "–"));
    lines.push("🎯 跟随王：" + (rJson.leaderId||"–"));
    if (rJson.peers) lines.push("🧭 peers：" + rJson.peers.join(", "));
  } catch(e){ lines.push("raft/status 无法解析"); }
  openModal(":"+p+" · 健康检查", lines.join("\n"));
}
async function eventsNode(p){
  const r = await jget("http://"+location.hostname+":"+p, "/api/events");
  let rows=[]; try{ rows=JSON.parse(r.body); }catch(e){}
  openModal("节点 :"+p+" · 最近事件 "+rows.length+" 条",
    rows.slice(0,60).map(x=>x.ts+"  "+e_esc(x.msg)).join("\n") || "（空）");
}
async function probeNode(p){
  const b="http://"+location.hostname+":"+p, tag="probe-"+Date.now();
  const w = await fetch(b+"/api/q/normal/messages", {method:"POST", headers:{"Content-Type":"application/json"},
                                                    body:JSON.stringify({content:tag})});
  if (w.status!==201){ toast("冒烟失败：写入返回 "+w.status); return; }
  const r = await (await fetch(b+"/api/q/normal/receive?waitMs=3000")).json();
  if (r && r.content===tag){ await fetch(b+"/api/q/normal/ack/"+r.id,{method:"POST"}); toast("🧪 冒烟 OK（写入→领取→销账 全链通）"); refresh(); }
  else toast("冒烟异常：领取内容不匹配（可能是队列被长轮询占用）");
}
async function showClusterStatus(){
  const p = NODES()[0];
  const r = await jget("http://"+location.hostname+":"+p, "/api/cluster/status");
  let lines = [];
  try {
    const s = JSON.parse(r.body||"{}");
    if (s.role) lines.push("🎖️ 本机角色（静态声明）：" + s.role);
    if (s.node) lines.push("👤 节点：" + s.node);
    if (s.peers && s.peers.length) lines.push("🧭 peers：" + s.peers.join(", "));
    if (s.queues) lines.push("📊 队列：就绪 " + (s.queues.ready??0) + " / 锁定 " + (s.queues.locked??0)
                          + " / 主账 " + (s.queues.pendingOnDisk??0) + " / 死信 " + (s.queues.deadLetters??0));
    lines.push("");
    lines.push("提示：当前王位以各节点「Raft 状态」卡为准（动态主权）。");
  } catch(e){ lines.push("数据解析失败"); }
  openModal("集群聚合状态（当前王视角）", lines.join("\n"));
}

/* ── 弹窗 ── */
function openModal(title, body){ document.getElementById("m-title").textContent=title;
  document.getElementById("m-body").textContent = typeof body==="string"?body:JSON.stringify(body,null,2);
  document.getElementById("modal").style.display="flex"; }
function closeModal(){ document.getElementById("modal").style.display="none"; }

/* ── 启动 ── */
refresh();
</script>
</body>
</html>
""";
}
