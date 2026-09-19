import http from "node:http";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const dbPath = join(__dirname, "data", "core-slices.json");
const port = Number(process.env.PORT || 3025);
const statuses = ["待切割", "制片中", "待观察", "已交付"];
const taskSteps = ["取样", "切割", "研磨", "染色", "观察"];

const HOME_LOCATION = "标本库 A-01";
const QUARANTINE_LOCATION = "隔离复核区";
const DEFAULT_TEMP_MINUTES = 240;

const seed = {
  samples: [
    {
      id: "CORE-001",
      project: "东岭铜矿薄片",
      borehole: "ZK-17",
      coreBox: "BX-09",
      depth: "128.4-128.8m",
      owner: "陆川",
      status: "制片中",
      delivery: "未交付",
      slices: [
        { id: "SL-001-A", method: "茜素红染色", observation: "", status: "研磨", logs: [{ at: "2026-06-12T10:00:00.000Z", step: "取样", note: "截取含矿化条带位置" }, { at: "2026-06-13T11:20:00.000Z", step: "切割", note: "完成粗切" }] }
      ]
    }
  ]
};

let evSeq = 0;
const evId = () => `EV-${Date.now().toString(36)}-${(evSeq++).toString(36)}`;
const nowIso = () => new Date().toISOString();

// 所有写操作串行化：整个数据库是一个 JSON 文件，读写必须原子，
// 否则并发的领取/交接会互相覆盖，“只成功一次”也无法保证。
let writeChain = Promise.resolve();
function withWrite(fn) {
  const run = writeChain.then(fn, fn);
  writeChain = run.then(() => {}, () => {});
  return run;
}

async function loadDb() {
  if (!existsSync(dbPath)) {
    await mkdir(dirname(dbPath), { recursive: true });
    await writeFile(dbPath, JSON.stringify(seed, null, 2));
  }
  return JSON.parse(await readFile(dbPath, "utf8"));
}
async function saveDb(db) { await writeFile(dbPath, JSON.stringify(db, null, 2)); }
async function body(req) {
  const chunks = [];
  for await (const chunk of req) chunks.push(chunk);
  if (!chunks.length) return {};
  try {
    return JSON.parse(Buffer.concat(chunks).toString("utf8"));
  } catch {
    return Object.assign(new Error("invalid_json"), { code: "invalid_json" });
  }
}
function sendJson(res, status, data) {
  res.writeHead(status, { "Content-Type": "application/json; charset=utf-8" });
  res.end(JSON.stringify(data, null, 2));
}
function fail(res, status, code, detail) {
  return sendJson(res, status, Object.assign({ error: code }, detail ? { detail } : {}));
}
function str(value) { return typeof value === "string" ? value.trim() : ""; }

// ---------- 切片保管链模型 ----------
// custody=null 表示在库；custody 存在表示被领取且未归还，其中 handover 至多一笔（未归还交接）。
// quarantine.active 表示隔离复核中；contamination 永久保留污染原因（解除后写 resolvedAt）。
// history 只追加、不改写，保证刷新后历史位置与污染原因一致。
function newSlice(id, method, owner, note) {
  const at = nowIso();
  return {
    id,
    method: method || "未指定",
    observation: "",
    status: "取样",
    logs: [{ at, step: "取样", note }],
    homeLocation: HOME_LOCATION,
    location: HOME_LOCATION,
    holder: null,
    sealed: true,
    custody: null,
    quarantine: null,
    contamination: null,
    history: [{ id: evId(), at, kind: "制片", step: "取样", note, person: owner || "—", location: HOME_LOCATION }]
  };
}

function migrateSlice(slice, owner) {
  let changed = false;
  if (!slice.homeLocation) { slice.homeLocation = HOME_LOCATION; changed = true; }
  if (!slice.location) { slice.location = slice.homeLocation; changed = true; }
  if (slice.holder === undefined) { slice.holder = null; changed = true; }
  if (slice.sealed === undefined) { slice.sealed = true; changed = true; }
  if (slice.custody === undefined) { slice.custody = null; changed = true; }
  if (slice.quarantine === undefined) { slice.quarantine = null; changed = true; }
  if (slice.contamination === undefined) { slice.contamination = null; changed = true; }
  if (!Array.isArray(slice.history)) {
    slice.history = (Array.isArray(slice.logs) ? slice.logs : []).map(log => ({
      id: evId(), at: log.at, kind: "制片", step: log.step, note: log.note,
      person: owner || "—", location: "制片区"
    }));
    changed = true;
  }
  return changed;
}

function tempExpired(slice, at = Date.now()) {
  return Boolean(slice.custody && slice.custody.tempDueAt && at > new Date(slice.custody.tempDueAt).getTime());
}
function sliceState(slice, at = Date.now()) {
  if (slice.quarantine && slice.quarantine.active) return "隔离复核";
  if (slice.custody && slice.custody.handover) return "交接中";
  if (slice.custody) return "已领取";
  return "在库";
}
function decorate(slice) {
  return Object.assign({}, slice, { state: sliceState(slice), tempExpired: tempExpired(slice) });
}
function decorateSample(sample) {
  return Object.assign({}, sample, { slices: sample.slices.map(decorate) });
}
function pushHistory(slice, event) {
  slice.history.push(Object.assign({ id: evId(), at: nowIso() }, event));
}

// 已污染 / 温控超时 / 隔离中：一切正常流转都被拦下，只剩隔离（或解除隔离）。
function guardFlow(slice) {
  if (slice.quarantine && slice.quarantine.active) return "quarantined";
  if (slice.contamination && !slice.contamination.resolvedAt) return "contaminated";
  if (tempExpired(slice)) return "temp_timeout";
  return null;
}

function updateSampleStatus(sample) {
  const sliceStatuses = sample.slices.map(slice => slice.status);
  if (sliceStatuses.length && sliceStatuses.every(step => step === "观察")) sample.status = "待观察";
  if (sample.delivery === "已交付") sample.status = "已交付";
  else if (sliceStatuses.some(step => ["取样", "切割", "研磨", "染色"].includes(step))) sample.status = "制片中";
  else sample.status = "待切割";
}
function findSlice(db, sampleId, sliceId) {
  const sample = db.samples.find(item => item.id === sampleId);
  if (!sample) return [{ error: "sample_not_found" }, null, null];
  const slice = sample.slices.find(item => item.id === sliceId);
  if (!slice) return [{ error: "slice_not_found" }, sample, null];
  return [null, sample, slice];
}

// 在串行队列内完成一次切片状态变更：重新读盘 → 校验 → 修改 → 落盘。
async function mutateSlice(sampleId, sliceId, handler) {
  return withWrite(async () => {
    const db = await loadDb();
    const [lookup, sample, slice] = findSlice(db, sampleId, sliceId);
    if (lookup) return { status: 404, body: lookup };
    const result = await handler(slice, sample);
    if (result && result.abort) return { status: result.status || 409, body: { error: result.code } };
    updateSampleStatus(sample);
    await saveDb(db);
    return { status: 200, body: decorateSample(sample) };
  });
}

const page = `<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>切片交接与污染隔离台</title>
  <style>
    :root { --bg:#f1f3ef; --panel:#fff; --ink:#242822; --muted:#687062; --line:#d7ddd1; --accent:#526f43; --stone:#73706a; --warn:#b07a1e; --danger:#b03a2e; }
    * { box-sizing:border-box; } body { margin:0; background:var(--bg); color:var(--ink); font-family:Arial,"PingFang SC",sans-serif; }
    header { padding:22px 28px; background:#fff; border-bottom:1px solid var(--line); display:flex; justify-content:space-between; align-items:center; gap:16px; }
    h1 { margin:0; font-size:26px; } main { display:grid; grid-template-columns:390px 1fr; gap:22px; padding:22px 28px; }
    form,.panel,.card,.stat { background:#fff; border:1px solid var(--line); border-radius:8px; padding:16px; } h2 { margin:0 0 12px; font-size:18px; }
    label { display:block; margin:10px 0 5px; color:var(--muted); font-size:13px; } input,select,textarea { width:100%; border:1px solid var(--line); border-radius:6px; padding:9px; font:inherit; background:#fff; } textarea { min-height:56px; }
    button { border:0; border-radius:6px; background:var(--accent); color:#fff; padding:9px 13px; font-weight:700; cursor:pointer; }
    button.warn { background:var(--warn); } button.danger { background:var(--danger); } button.ghost { background:#eef1ea; color:var(--ink); border:1px solid var(--line); }
    .stats { display:grid; grid-template-columns:repeat(5,1fr); gap:10px; margin-bottom:14px; } .stat strong { display:block; font-size:24px; }
    .grid { display:grid; grid-template-columns:repeat(auto-fill,minmax(360px,1fr)); gap:12px; } .card { display:grid; gap:8px; }
    .meta { color:var(--muted); font-size:13px; } .pill { display:inline-block; border:1px solid var(--line); border-radius:999px; padding:3px 8px; font-size:12px; }
    .slice { border-top:1px solid var(--line); padding-top:10px; margin-top:10px; } .toolbar { display:flex; gap:10px; flex-wrap:wrap; margin-bottom:14px; }
    .state { font-weight:700; } .state-在库 { color:var(--stone); } .state-已领取 { color:var(--accent); } .state-交接中 { color:var(--warn); } .state-隔离复核 { color:var(--danger); }
    .seal-ok { color:var(--accent); font-weight:700; } .seal-bad { color:var(--danger); font-weight:700; }
    .banner { border-radius:6px; padding:8px 10px; font-size:13px; } .banner.red { background:#fbeae7; color:var(--danger); border:1px solid #e3b4ad; }
    .banner.gray { background:#f4f4f2; color:var(--muted); border:1px solid var(--line); }
    .act { border-top:1px dashed var(--line); padding-top:10px; margin-top:8px; display:grid; gap:6px; }
    .act .row { display:flex; gap:6px; flex-wrap:wrap; align-items:center; } .act input,.act select { width:auto; flex:1; min-width:90px; }
    .act label { margin:0; } .msg { font-size:13px; padding:7px 9px; border-radius:6px; }
    .msg.err { background:#fbeae7; color:var(--danger); border:1px solid #e3b4ad; } .msg.ok { background:#edf4e8; color:var(--accent); border:1px solid #c7d8bd; }
    .history { list-style:none; margin:6px 0 0; padding:0; display:grid; gap:4px; } .history li { font-size:12px; color:var(--muted); border-left:3px solid var(--line); padding-left:8px; }
    .temp.overdue { color:var(--danger); font-weight:700; }
    @media (max-width:950px){ header{display:block;padding:18px 16px;} main{grid-template-columns:1fr;padding:16px;} .stats{grid-template-columns:repeat(2,1fr);} }
  </style>
</head>
<body>
  <header><div><h1>切片交接与污染隔离台</h1><div class="meta">领取 · 制片 · 交接 · 归还 —— 全程记录位置与责任人；污染或温控超时仅可隔离复核</div></div><button id="reload">刷新</button></header>
  <main>
    <form id="form">
      <h2>创建岩芯样本</h2>
      <label>项目</label><input name="project" required>
      <label>钻孔编号</label><input name="borehole" required>
      <label>岩芯箱号</label><input name="coreBox" required>
      <label>取样深度</label><input name="depth" required>
      <label>负责人</label><input name="owner" required>
      <label>初始切片编号</label><input name="sliceId" required>
      <label>染色方法</label><input name="method" required>
      <button>保存样本</button>
    </form>
    <section>
      <div class="stats" id="stats"></div>
      <div class="grid" id="samples"></div>
    </section>
  </main>
  <script>
    const statuses = ${JSON.stringify(statuses)};
    const steps = ${JSON.stringify(taskSteps)};
    const defaultTempMinutes = ${DEFAULT_TEMP_MINUTES};
    const form = document.querySelector("#form");
    const stats = document.querySelector("#stats");
    const samplesEl = document.querySelector("#samples");
    let samples = [];
    const messages = {};
    const errText = {
      already_claimed: "该切片已被领取且未归还，重复领取被拒绝",
      not_in_custody: "切片当前在库，尚未领取",
      handover_open: "已存在一笔未归还交接，归还前不能再次交接",
      holder_mismatch: "操作人与登记的当前责任人不一致",
      seal_broken: "密封状态异常，不能归还，请转入隔离复核",
      quarantined: "切片处于隔离复核中，正常流程已暂停",
      contaminated: "切片已标记污染，只能进入隔离复核",
      temp_timeout: "已超过温控时限，只能进入隔离复核",
      already_quarantined: "该切片已在隔离复核中",
      not_quarantined: "该切片当前不在隔离复核",
      person_required: "请填写责任人",
      target_required: "请填写交接接收人",
      location_required: "请填写交接位置",
      bad_temp_minutes: "温控时限需为正整数（分钟）",
      invalid_step: "制片步骤不合法",
      invalid_json: "请求内容不是合法 JSON"
    };
    async function api(path, options) {
      const res = await fetch(path, options && options.body ? { ...options, headers:{ "Content-Type":"application/json" } } : options);
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || "请求失败");
      return data;
    }
    function allSlices(){ return samples.flatMap(sample => sample.slices.map(slice => ({ sample, slice }))); }
    function msgKey(sampleId, sliceId){ return sampleId + "|" + sliceId; }
    function esc(s){ return String(s == null ? "" : s).replace(/[&<>"]/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;","\\"":"&quot;"}[c])); }
    function fmtTime(at){ return at ? new Date(at).toLocaleString("zh-CN", { hour12:false }) : ""; }
    function fmtEvent(ev){
      const t = fmtTime(ev.at);
      if (ev.kind === "领取") return t + " 领取：" + esc(ev.person) + " 自 " + esc(ev.from) + " 领至 " + esc(ev.to) + "，温控截止 " + fmtTime(ev.tempDueAt);
      if (ev.kind === "制片") return t + " 制片：" + esc(ev.person) + " 在 " + esc(ev.location) + " 记录「" + esc(ev.step) + "」" + (ev.note ? " · " + esc(ev.note) : "");
      if (ev.kind === "交接") return t + " 交接：" + esc(ev.from) + " → " + esc(ev.to) + "（" + esc(ev.location) + "），未归还";
      if (ev.kind === "归还") return t + " 归还：" + esc(ev.person) + " 交回 " + esc(ev.location) + "，核对密封完好";
      if (ev.kind === "隔离") return t + " 隔离复核（" + esc(ev.source) + "）：" + esc(ev.by || "复核员") + " 登记 · " + esc(ev.reason);
      if (ev.kind === "解除隔离") return t + " 解除隔离：" + esc(ev.by) + " · " + esc(ev.note || "复核通过") + (ev.sealed === false ? "，密封异常" : "，密封合格") + "，回库 " + esc(ev.location);
      return t + " " + esc(ev.kind);
    }
    function msgBanner(key){
      const m = messages[key];
      if (!m) return "";
      return '<div class="msg ' + (m.type === "ok" ? "ok" : "err") + '">' + esc(m.text) + "</div>";
    }
    function sliceBlock(sample, slice){
      const key = msgKey(sample.id, slice.id);
      const state = slice.state;
      const seal = slice.sealed ? '<span class="seal-ok">密封完好</span>' : '<span class="seal-bad">密封异常</span>';
      let tempLine = "温控：在库不计时";
      if (slice.custody && slice.custody.tempDueAt) tempLine = '温控截止 <span class="temp" data-due="' + slice.custody.tempDueAt + '">' + fmtTime(slice.custody.tempDueAt) + "</span>";
      let banner = "";
      if (state === "隔离复核") banner = '<div class="banner red">隔离复核中 · 原因（' + esc(slice.quarantine.source) + "）：" + esc(slice.quarantine.reason) + "<br>位置：" + esc(slice.location) + "，复核责任人：" + esc(slice.holder) + "</div>";
      else if (slice.tempExpired) banner = '<div class="banner red overdue-banner">已超过温控时限：禁止交接与归还，只能进入隔离复核</div>';
      else if (slice.contamination && slice.contamination.resolvedAt) banner = '<div class="banner gray">历史污染（' + esc(slice.contamination.source) + "）：" + esc(slice.contamination.reason) + "，已于 " + fmtTime(slice.contamination.resolvedAt) + " 解除，记录保留</div>";
      const openHandover = slice.custody && slice.custody.handover;
      let actions = "";
      if (state === "在库") {
        actions =
          '<form class="act" data-act="claim" data-sample="' + sample.id + '" data-slice="' + slice.id + '">' +
          '<label>领取（在库方可领取，并发/重复领取仅一笔成功）</label>' +
          '<div class="row"><input name="person" placeholder="领取责任人" required><input name="station" placeholder="领取后位置，如 制片台 P-1" required></div>' +
          '<div class="row"><input name="tempMinutes" type="number" min="1" value="' + defaultTempMinutes + '" title="温控时限（分钟）"><button type="submit">领取</button></div></form>';
      } else if (state === "已领取" || state === "交接中") {
        actions =
          '<form class="act" data-act="prepare" data-sample="' + sample.id + '" data-slice="' + slice.id + '">' +
          '<label>制片记录（责任人须为当前持有人）</label>' +
          '<div class="row"><input name="person" placeholder="责任人 ' + esc(slice.holder) + '" required><select name="step">' + steps.map(s => '<option' + (s === slice.status ? ' selected' : '') + '>' + s + '</option>').join("") + '</select></div>' +
          '<div class="row"><input name="station" placeholder="制片位置" value="' + esc(slice.location) + '"><input name="note" placeholder="步骤备注 / 观察结果"></div>' +
          '<div class="row"><button type="submit" class="ghost">记录制片</button></div></form>';
        if (state === "已领取") {
          actions +=
            '<form class="act" data-act="handover" data-sample="' + sample.id + '" data-slice="' + slice.id + '">' +
            '<label>交接（同片仅允许一笔未归还交接）</label>' +
            '<div class="row"><input name="from" value="' + esc(slice.holder) + '" title="交出人（当前责任人）" readonly><input name="to" placeholder="接收责任人" required><input name="location" placeholder="交接位置" required></div>' +
            '<div class="row"><button type="submit" class="warn">交接</button></div></form>';
        } else {
          actions += '<div class="banner gray">未归还交接：' + esc(openHandover.from) + " → " + esc(openHandover.to) + "（" + esc(openHandover.location) + "），归还前不能再次交接</div>";
        }
        actions +=
          '<form class="act" data-act="return" data-sample="' + sample.id + '" data-slice="' + slice.id + '">' +
          '<label>归还（核对原责任人与密封状态）</label>' +
          '<div class="row"><input name="person" placeholder="归还人，须为 ' + esc(slice.holder) + '" required><input name="location" placeholder="归还库位（默认 ' + esc(slice.homeLocation) + '）"></div>' +
          '<div class="row"><label><input style="width:auto" type="checkbox" name="sealed" value="true" required> 密封完好，核对一致</label><button type="submit">归还入库</button></div></form>';
        actions +=
          '<form class="act" data-act="quarantine" data-sample="' + sample.id + '" data-slice="' + slice.id + '">' +
          '<label>隔离复核（污染登记 / 温控超时）</label>' +
          '<input name="reason" placeholder="污染原因或复核事由" required>' +
          '<div class="row"><input name="by" placeholder="复核责任人" required>' +
          '<button type="submit" name="source" value="污染" class="danger">污染隔离</button>' +
          '<button type="submit" name="source" value="温控超时" class="danger"' + (slice.tempExpired ? "" : " title=\"未超时时仍可由复核员确认\"") + ">温控超时隔离</button></div></form>";
      } else {
        actions =
          '<form class="act" data-act="release" data-sample="' + sample.id + '" data-slice="' + slice.id + '">' +
          '<label>解除隔离（回库后恢复领取/制片/交接/归还原流程）</label>' +
          '<div class="row"><input name="by" placeholder="复核责任人" required><input name="note" placeholder="复核结论"></div>' +
          '<div class="row"><label><input style="width:auto" type="checkbox" name="sealed" value="true" checked> 密封合格</label><button type="submit" class="ghost">解除隔离并回库</button></div></form>';
      }
      return '<div class="slice"><b>' + esc(slice.id) + '</b> <span class="pill state state-' + state + '">' + state + "</span> " + seal +
        '<div class="meta">染色：' + esc(slice.method) + " · 制片步骤：" + esc(slice.status) + "</div>" +
        '<div class="meta">当前位置：' + esc(slice.location) + " · 当前责任人：" + esc(slice.holder || "—（在库）") + "</div>" +
        '<div class="meta">' + tempLine + "</div>" +
        banner + msgBanner(key) + actions +
        '<ul class="history">' + slice.history.slice().reverse().map(ev => "<li>" + fmtEvent(ev) + "</li>").join("") + "</ul></div>";
    }
    function render() {
      const rows = allSlices().map(r => r.slice);
      const cards = [
        ["切片总数", rows.length],
        ["在库", rows.filter(s => s.state === "在库").length],
        ["领取流转中", rows.filter(s => s.state === "已领取").length],
        ["未归还交接", rows.filter(s => s.state === "交接中").length],
        ["隔离复核", rows.filter(s => s.state === "隔离复核").length]
      ];
      stats.innerHTML = cards.map(c => '<div class="stat"><span>' + c[0] + '</span><strong>' + c[1] + "</strong></div>").join("");
      samplesEl.innerHTML = samples.map(sample =>
        '<article class="card"><h3>' + esc(sample.project) + '</h3><span class="pill">' + esc(sample.status) + "</span>" +
        '<div class="meta">' + esc(sample.borehole) + " · " + esc(sample.coreBox) + " · " + esc(sample.depth) + " · " + esc(sample.owner) + " · " + esc(sample.delivery) + "</div>" +
        sample.slices.map(slice => sliceBlock(sample, slice)).join("") +
        '<form class="act" data-act="add-slice" data-sample="' + sample.id + '"><label>新增切片</label><div class="row"><input name="id" placeholder="切片编号" required><input name="method" placeholder="染色方法"></div><div class="row"><button type="submit" class="ghost">添加切片</button><button type="button" class="ghost" data-deliver="' + sample.id + '">标记交付</button></div></form>' +
        "</article>").join("");
      tick();
    }
    function tick(){
      const now = Date.now();
      document.querySelectorAll("[data-due]").forEach(node => {
        const due = new Date(node.dataset.due).getTime();
        const left = due - now;
        node.classList.toggle("overdue", left <= 0);
        node.textContent = fmtTime(node.dataset.due) + "（" + (left > 0 ? "剩余 " + Math.floor(left / 60000) + " 分 " + Math.floor(left / 1000) % 60 + " 秒" : "已超时") + "）";
      });
    }
    setInterval(tick, 1000);
    function pathFor(act, sampleId, sliceId){
      const base = "/api/samples/" + sampleId + "/slices/" + sliceId;
      if (act === "claim") return base + "/claim";
      if (act === "prepare") return base + "/prepare";
      if (act === "handover") return base + "/handover";
      if (act === "return") return base + "/return";
      if (act === "quarantine") return base + "/quarantine";
      if (act === "release") return base + "/quarantine/release";
      return null;
    }
    samplesEl.addEventListener("submit", async event => {
      const f = event.target.closest("form[data-act]");
      if (!f || !samplesEl.contains(f)) return;
      event.preventDefault();
      const act = f.dataset.act;
      const sampleId = f.dataset.sample;
      const sliceId = f.dataset.slice;
      const payload = Object.fromEntries(new FormData(f).entries());
      const key = msgKey(sampleId || "", sliceId || "");
      try {
        if (act === "add-slice") {
          await api("/api/samples/" + sampleId + "/slices", { method:"POST", body: JSON.stringify(payload) });
        } else {
          await api(pathFor(act, sampleId, sliceId), { method:"POST", body: JSON.stringify(payload) });
        }
        messages[key] = { type:"ok", text:"操作已记录" };
      } catch (error) {
        messages[key] = { type:"err", text: errText[error.message] || error.message };
      }
      await load();
    });
    samplesEl.addEventListener("click", async event => {
      const btn = event.target.closest("[data-deliver]");
      if (!btn) return;
      await api("/api/samples/" + btn.dataset.deliver + "/deliver", { method:"POST", body: JSON.stringify({}) });
      await load();
    });
    async function load(){ samples = await api("/api/samples"); render(); }
    document.querySelector("#reload").onclick = load;
    form.onsubmit = async event => {
      event.preventDefault();
      await api("/api/samples", { method:"POST", body: JSON.stringify(Object.fromEntries(new FormData(form).entries())) });
      form.reset(); await load();
    };
    load();
  </script>
</body>
</html>`;

const server = http.createServer(async (req, res) => {
  try {
    const url = new URL(req.url, `http://${req.headers.host}`);
    if (req.method === "GET" && url.pathname === "/") {
      res.writeHead(200, { "Content-Type":"text/html; charset=utf-8" });
      return res.end(page);
    }
    if (req.method === "GET" && url.pathname === "/api/samples") {
      const db = await loadDb();
      return sendJson(res, 200, db.samples.map(decorateSample));
    }
    if (req.method === "POST" && url.pathname === "/api/samples") {
      const input = await body(req);
      if (input instanceof Error) return fail(res, 400, input.code);
      return withWrite(async () => {
        const db = await loadDb();
        const sample = {
          id: `CORE-${Date.now()}`, project: input.project, borehole: input.borehole, coreBox: input.coreBox,
          depth: input.depth, owner: input.owner, status: "待切割", delivery: "未交付",
          slices: [newSlice(input.sliceId, input.method, input.owner, "创建初始切片任务")]
        };
        updateSampleStatus(sample);
        db.samples.unshift(sample);
        await saveDb(db);
        return sendJson(res, 201, decorateSample(sample));
      });
    }
    const addSlice = url.pathname.match(/^\/api\/samples\/([^/]+)\/slices$/);
    if (addSlice && req.method === "POST") {
      const input = await body(req);
      if (input instanceof Error) return fail(res, 400, input.code);
      return withWrite(async () => {
        const db = await loadDb();
        const sample = db.samples.find(item => item.id === addSlice[1]);
        if (!sample) return sendJson(res, 404, { error: "sample_not_found" });
        if (sample.slices.some(item => item.id === input.id)) return sendJson(res, 409, { error: "slice_exists" });
        sample.slices.push(newSlice(input.id, input.method, sample.owner, "新增切片任务"));
        updateSampleStatus(sample);
        await saveDb(db);
        return sendJson(res, 201, decorateSample(sample));
      });
    }
    const deliverMatch = url.pathname.match(/^\/api\/samples\/([^/]+)\/deliver$/);
    if (deliverMatch && req.method === "POST") {
      return withWrite(async () => {
        const db = await loadDb();
        const sample = db.samples.find(item => item.id === deliverMatch[1]);
        if (!sample) return sendJson(res, 404, { error: "sample_not_found" });
        sample.delivery = "已交付";
        updateSampleStatus(sample);
        await saveDb(db);
        return sendJson(res, 200, decorateSample(sample));
      });
    }

    // 兼容旧版步骤记录接口（新页面统一走 /prepare，带责任人与位置）。
    const logMatch = url.pathname.match(/^\/api\/samples\/([^/]+)\/slices\/([^/]+)\/logs$/);
    if (logMatch && req.method === "POST") {
      const input = await body(req);
      if (input instanceof Error) return fail(res, 400, input.code);
      return withWrite(async () => {
        const db = await loadDb();
        const [lookup, sample, slice] = findSlice(db, logMatch[1], logMatch[2]);
        if (lookup) return sendJson(res, 404, lookup);
        slice.status = input.step;
        if (input.step === "观察") slice.observation = input.note || slice.observation;
        slice.logs.push({ at: nowIso(), step: input.step, note: input.note || "" });
        updateSampleStatus(sample);
        await saveDb(db);
        return sendJson(res, 200, decorateSample(sample));
      });
    }

    const custodyMatch = url.pathname.match(/^\/api\/samples\/([^/]+)\/slices\/([^/]+)\/(claim|prepare|handover|return|quarantine)$/);
    if (custodyMatch && req.method === "POST") {
      const [, sampleId, sliceId, action] = custodyMatch;
      const input = await body(req);
      if (input instanceof Error) return fail(res, 400, input.code);
      const result = await mutateSlice(sampleId, sliceId, (slice) => {
        if (action === "claim") {
          const blocked = guardFlow(slice);
          if (blocked) return { abort: true, code: blocked };
          if (slice.custody) return { abort: true, code: "already_claimed" };
          const person = str(input.person);
          const station = str(input.station);
          if (!person) return { abort: true, status: 400, code: "person_required" };
          if (!station) return { abort: true, status: 400, code: "location_required" };
          const minutes = Math.floor(Number(input.tempMinutes == null || input.tempMinutes === "" ? DEFAULT_TEMP_MINUTES : input.tempMinutes));
          if (!Number.isFinite(minutes) || minutes <= 0) return { abort: true, status: 400, code: "bad_temp_minutes" };
          const at = nowIso();
          const dueAt = new Date(Date.now() + minutes * 60000).toISOString();
          slice.custody = { person, station, at, tempStartAt: at, tempLimitMinutes: minutes, tempDueAt: dueAt, handover: null };
          slice.holder = person;
          slice.location = station;
          slice.sealed = true;
          pushHistory(slice, { kind: "领取", person, from: slice.homeLocation, to: station, sealed: true, tempDueAt: dueAt });
        }
        if (action === "prepare") {
          if (!slice.custody) return { abort: true, code: "not_in_custody" };
          const blocked = guardFlow(slice);
          if (blocked) return { abort: true, code: blocked };
          const person = str(input.person);
          if (!person) return { abort: true, status: 400, code: "person_required" };
          if (person !== slice.holder) return { abort: true, code: "holder_mismatch" };
          const step = str(input.step);
          if (!taskSteps.includes(step)) return { abort: true, status: 400, code: "invalid_step" };
          const station = str(input.station) || slice.location;
          const note = str(input.note);
          slice.status = step;
          if (step === "观察") slice.observation = note || slice.observation;
          slice.logs.push({ at: nowIso(), step, note });
          slice.location = station;
          pushHistory(slice, { kind: "制片", person, step, note, location: station });
        }
        if (action === "handover") {
          if (!slice.custody) return { abort: true, code: "not_in_custody" };
          const blocked = guardFlow(slice);
          if (blocked) return { abort: true, code: blocked };
          if (slice.custody.handover) return { abort: true, code: "handover_open" };
          const from = str(input.from);
          const to = str(input.to);
          const location = str(input.location);
          if (from !== slice.holder) return { abort: true, code: "holder_mismatch" };
          if (!to) return { abort: true, status: 400, code: "target_required" };
          if (!location) return { abort: true, status: 400, code: "location_required" };
          const at = nowIso();
          slice.custody.handover = { from, to, location, at };
          slice.holder = to;
          slice.location = location;
          pushHistory(slice, { kind: "交接", from, to, location });
        }
        if (action === "return") {
          if (!slice.custody) return { abort: true, code: "not_in_custody" };
          const blocked = guardFlow(slice);
          if (blocked) return { abort: true, code: blocked };
          const person = str(input.person);
          if (!person) return { abort: true, status: 400, code: "person_required" };
          if (person !== slice.holder) return { abort: true, code: "holder_mismatch" };
          if (input.sealed !== true) return { abort: true, code: "seal_broken" };
          const destination = str(input.location) || slice.homeLocation;
          const handover = slice.custody.handover;
          pushHistory(slice, {
            kind: "归还", person, location: destination, sealed: true,
            handover: handover ? { from: handover.from, to: handover.to } : null
          });
          slice.custody = null;
          slice.holder = null;
          slice.location = destination;
          slice.sealed = true;
        }
        if (action === "quarantine") {
          if (slice.quarantine && slice.quarantine.active) return { abort: true, code: "already_quarantined" };
          const reason = str(input.reason);
          if (!reason) return { abort: true, status: 400, code: "reason_required" };
          const by = str(input.by) || "复核员";
          const source = input.source === "温控超时" ? "温控超时" : "污染";
          const at = nowIso();
          slice.quarantine = { active: true, reason, source, at, by, reviewNote: "", releasedAt: null, releasedBy: null };
          slice.contamination = { reason, source, at, by, resolvedAt: null };
          slice.location = QUARANTINE_LOCATION;
          slice.holder = by;
          pushHistory(slice, { kind: "隔离", reason, source, by, location: QUARANTINE_LOCATION });
        }
        return null;
      });
      return sendJson(res, result.status, result.body);
    }

    const releaseMatch = url.pathname.match(/^\/api\/samples\/([^/]+)\/slices\/([^/]+)\/quarantine\/release$/);
    if (releaseMatch && req.method === "POST") {
      const input = await body(req);
      if (input instanceof Error) return fail(res, 400, input.code);
      const result = await mutateSlice(releaseMatch[1], releaseMatch[2], (slice) => {
        if (!slice.quarantine || !slice.quarantine.active) return { abort: true, code: "not_quarantined" };
        const by = str(input.by);
        if (!by) return { abort: true, status: 400, code: "person_required" };
        const note = str(input.note);
        const sealed = input.sealed !== false;
        const at = nowIso();
        slice.quarantine.active = false;
        slice.quarantine.releasedAt = at;
        slice.quarantine.releasedBy = by;
        slice.quarantine.reviewNote = note;
        if (slice.contamination) slice.contamination.resolvedAt = at;
        // 解除即回库，原领取/交接链终结，温控计时清空；历史与污染原因原样保留。
        slice.custody = null;
        slice.holder = null;
        slice.location = slice.homeLocation;
        slice.sealed = sealed;
        pushHistory(slice, { kind: "解除隔离", by, note, sealed, location: slice.homeLocation });
        return null;
      });
      return sendJson(res, result.status, result.body);
    }

    sendJson(res, 404, { error: "not_found" });
  } catch (error) {
    sendJson(res, 500, { error: error.message });
  }
});

// 启动时迁移一次旧数据：补全新字段，并把旧制片日志转成带位置/责任人的历史链。
const bootDb = await loadDb();
let migrated = false;
for (const sample of bootDb.samples) {
  for (const slice of sample.slices) {
    if (migrateSlice(slice, sample.owner)) migrated = true;
  }
}
if (migrated) await saveDb(bootDb);

server.listen(port, () => console.log(`Core slice lab app listening on http://localhost:${port}`));
