import http from "node:http";
import { mkdir, readFile, writeFile, rename } from "node:fs/promises";
import { existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const dbPath = join(__dirname, "data", "core-slices.json");
const port = Number(process.env.PORT || 3025);

const statuses = ["待切割", "制片中", "待观察", "已交付"];
const taskSteps = ["取样", "切割", "研磨", "染色", "观察"];
const phases = ["待领取", "制片中", "交接中", "已归还", "隔离中"];
const quarantineReasons = {
  contaminated: "污染确认",
  temp_exceeded: "温控超时",
  seal_broken: "密封破损"
};
const DEFAULT_CLAIM_LIMIT = 240; // 领取后温控允许时长（分钟）
const DEFAULT_HANDOVER_LIMIT = 120; // 单次交接温控允许时长（分钟）
const TEMP_WARN_MS = 30 * 60 * 1000;
const STORAGE = "样本暂存柜";
const QUARANTINE_BAY = "隔离复核区";

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
        {
          id: "SL-001-A",
          method: "茜素红染色",
          observation: "",
          status: "研磨",
          chainVersion: 1,
          phase: "制片中",
          location: "制片台-1",
          custodian: "陆川",
          sealIntact: true,
          contaminated: false,
          tempDeadline: "2026-06-13T15:20:00.000Z",
          activeHandoverId: null,
          claimed: { by: "陆川", at: "2026-06-12T10:00:00.000Z", station: "制片台-1", tempLimitMinutes: DEFAULT_CLAIM_LIMIT, requestId: null },
          handovers: [],
          quarantine: null,
          quarantineRecords: [],
          logs: [
            { at: "2026-06-12T10:00:00.000Z", step: "取样", note: "截取含矿化条带位置" },
            { at: "2026-06-13T11:20:00.000Z", step: "切割", note: "完成粗切" }
          ],
          history: [
            { at: "2026-06-12T10:00:00.000Z", type: "领取", fromPerson: "陆川", fromLocation: STORAGE, toPerson: "陆川", toLocation: "制片台-1", note: "截取含矿化条带位置" },
            { at: "2026-06-12T10:00:00.000Z", type: "制片", step: "取样", fromPerson: "陆川", fromLocation: "制片台-1", toPerson: "陆川", toLocation: "制片台-1", note: "截取含矿化条带位置" },
            { at: "2026-06-13T11:20:00.000Z", type: "制片", step: "切割", fromPerson: "陆川", fromLocation: "制片台-1", toPerson: "陆川", toLocation: "制片台-1", note: "完成粗切" }
          ]
        },
        {
          id: "SL-001-B",
          method: "未指定",
          observation: "",
          status: "取样",
          chainVersion: 1,
          phase: "待领取",
          location: STORAGE,
          custodian: "陆川",
          sealIntact: true,
          contaminated: false,
          tempDeadline: null,
          activeHandoverId: null,
          claimed: null,
          handovers: [],
          quarantine: null,
          quarantineRecords: [],
          logs: [],
          history: [
            { at: "2026-06-13T12:00:00.000Z", type: "登记", fromPerson: null, fromLocation: null, toPerson: "陆川", toLocation: STORAGE, note: "切片入库待领取" }
          ]
        }
      ]
    }
  ]
};

// ---------- 存储 ----------

async function loadDb() {
  if (!existsSync(dbPath)) {
    await mkdir(dirname(dbPath), { recursive: true });
    await saveDb(seed);
    return structuredClone(seed);
  }
  const db = JSON.parse(await readFile(dbPath, "utf8"));
  let changed = false;
  for (const sample of db.samples || []) {
    for (const slice of sample.slices || []) {
      if (normalizeSlice(slice, sample.owner)) changed = true;
    }
  }
  if (changed) await saveDb(db);
  return db;
}

async function saveDb(db) {
  const tmp = `${dbPath}.tmp`;
  await writeFile(tmp, JSON.stringify(db, null, 2));
  await rename(tmp, dbPath);
}

// 旧数据幂等迁移：补齐位置/责任人/历史，只执行一次
function normalizeSlice(slice, owner) {
  if (slice.chainVersion === 1) return false;
  slice.handovers ||= [];
  slice.quarantineRecords ||= [];
  slice.history ||= [];
  slice.contaminated ||= false;
  slice.sealIntact ??= true;
  slice.activeHandoverId ??= null;
  slice.quarantine ??= null;
  if (!slice.phase) {
    if (Array.isArray(slice.logs) && slice.logs.length) {
      const first = slice.logs[0];
      const last = slice.logs[slice.logs.length - 1];
      slice.claimed = { by: owner, at: first.at, station: "制片台-1", tempLimitMinutes: DEFAULT_CLAIM_LIMIT, requestId: null };
      slice.phase = "制片中";
      slice.location = "制片台-1";
      slice.custodian = owner;
      // 历史数据缺少温控凭证：按最后一次制片记录 + 时限保守推算，通常已超时
      slice.tempDeadline = new Date(Date.parse(last.at) + DEFAULT_CLAIM_LIMIT * 60000).toISOString();
      slice.history = [
        { at: first.at, type: "领取", fromPerson: owner, fromLocation: STORAGE, toPerson: owner, toLocation: "制片台-1", note: "历史数据补登领取" },
        ...slice.logs.map(log => ({ at: log.at, type: "制片", step: log.step, fromPerson: owner, fromLocation: "制片台-1", toPerson: owner, toLocation: "制片台-1", note: log.note }))
      ];
    } else {
      slice.claimed = null;
      slice.phase = "待领取";
      slice.location = STORAGE;
      slice.custodian = owner;
      slice.tempDeadline = null;
      slice.history = [{ at: new Date().toISOString(), type: "登记", fromPerson: null, fromLocation: null, toPerson: owner, toLocation: STORAGE, note: "切片入库待领取" }];
    }
  }
  slice.chainVersion = 1;
  return true;
}

// ---------- 并发控制 ----------
// 同一样本的变更串行化：读-判-写在锁内完成，重复/并发领取只有第一次成功。
const queues = new Map();
function withLock(key, task) {
  const prev = queues.get(key) || Promise.resolve();
  let release;
  const gate = new Promise(resolve => { release = resolve; });
  const run = prev.then(() => task()).finally(() => {
    if (queues.get(key) === gate) queues.delete(key);
    release();
  });
  queues.set(key, gate);
  return run;
}

// ---------- 工具 ----------

async function body(req) {
  const chunks = [];
  for await (const chunk of req) chunks.push(chunk);
  return chunks.length ? JSON.parse(Buffer.concat(chunks).toString("utf8")) : {};
}
function sendJson(res, status, data) {
  res.writeHead(status, { "Content-Type": "application/json; charset=utf-8" });
  res.end(JSON.stringify(data, null, 2));
}
function fail(res, status, code, message) {
  return sendJson(res, status, { error: code, message });;
}
const str = v => (typeof v === "string" ? v.trim() : v == null ? "" : String(v).trim());
function limitOf(v, fallback) {
  const n = Math.floor(Number(v));
  return Number.isFinite(n) && n >= 0 ? n : fallback;
}
function nowIso() { return new Date().toISOString(); }
function newHandoverId() {
  return `HO-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 6)}`;
}
function pushHistory(slice, ev) {
  slice.history.push({ at: nowIso(), ...ev });
}
function locate(db, sampleId, sliceId) {
  const sample = db.samples.find(item => item.id === sampleId);
  if (!sample) return { errorCode: "sample_not_found", status: 404 };
  const slice = sample.slices.find(item => item.id === sliceId);
  if (!slice) return { errorCode: "slice_not_found", status: 404 };
  return { sample, slice };
}
function activeHandover(slice) {
  if (!slice.activeHandoverId) return null;
  return slice.handovers.find(h => h.id === slice.activeHandoverId && !h.returnedAt) || null;
}
function isTempOverdue(slice, nowTs = Date.now()) {
  if (slice.phase === "隔离中" || !slice.tempDeadline) return false;
  return Date.parse(slice.tempDeadline) < nowTs;
}
function decorateSlice(slice, nowTs = Date.now()) {
  const view = { ...slice };
  if (slice.phase !== "隔离中" && slice.tempDeadline) {
    view.tempRemainingMs = Date.parse(slice.tempDeadline) - nowTs;
    view.tempStatus = view.tempRemainingMs < 0 ? "超时" : view.tempRemainingMs <= TEMP_WARN_MS ? "临期" : "正常";
  } else {
    view.tempRemainingMs = null;
    view.tempStatus = null;
  }
  view.activeHandover = activeHandover(slice);
  view.flags = {
    canClaim: !["制片中", "交接中", "隔离中"].includes(slice.phase),
    canPrepare: slice.phase === "制片中",
    canHandover: slice.phase === "制片中" && !slice.contaminated && !isTempOverdue(slice, nowTs) && slice.sealIntact,
    canReturn: slice.phase === "交接中" && !isTempOverdue(slice, nowTs) && slice.sealIntact && !slice.contaminated,
    canQuarantine: slice.phase !== "隔离中",
    canRelease: slice.phase === "隔离中"
  };
  return view;
}
function updateSampleStatus(sample) {
  const sliceStatuses = sample.slices.map(slice => slice.status);
  if (sample.delivery === "已交付") sample.status = "已交付";
  else if (sliceStatuses.length && sliceStatuses.every(step => step === "观察")) sample.status = "待观察";
  else if (sliceStatuses.some(step => ["取样", "切割", "研磨", "染色"].includes(step))) sample.status = "制片中";
  else sample.status = "待切割";
}
function newSlice(id, method, owner) {
  return {
    id,
    method: method || "未指定",
    observation: "",
    status: "取样",
    chainVersion: 1,
    phase: "待领取",
    location: STORAGE,
    custodian: owner,
    sealIntact: true,
    contaminated: false,
    tempDeadline: null,
    activeHandoverId: null,
    claimed: null,
    handovers: [],
    quarantine: null,
    quarantineRecords: [],
    logs: [],
    history: [{ at: nowIso(), type: "登记", fromPerson: null, fromLocation: null, toPerson: owner, toLocation: STORAGE, note: "切片入库待领取" }]
  };
}

// ---------- 页面 ----------

const page = `<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>切片交接与污染隔离台</title>
  <style>
    :root { --bg:#f1f3ef; --panel:#fff; --ink:#242822; --muted:#687062; --line:#d7ddd1; --accent:#526f43; --stone:#73706a; --warn:#b3741f; --danger:#a83a2f; }
    * { box-sizing:border-box; } body { margin:0; background:var(--bg); color:var(--ink); font-family:Arial,"PingFang SC",sans-serif; }
    header { padding:22px 28px; background:#fff; border-bottom:1px solid var(--line); display:flex; justify-content:space-between; align-items:center; gap:16px; }
    h1 { margin:0; font-size:26px; } main { display:grid; grid-template-columns:400px 1fr; gap:22px; padding:22px 28px; }
    form,.panel,.card,.stat { background:#fff; border:1px solid var(--line); border-radius:8px; padding:16px; } h2 { margin:0 0 12px; font-size:18px; }
    label { display:block; margin:10px 0 5px; color:var(--muted); font-size:13px; } input,select,textarea { width:100%; border:1px solid var(--line); border-radius:6px; padding:9px; font:inherit; background:#fff; } textarea { min-height:54px; }
    button { border:0; border-radius:6px; background:var(--accent); color:#fff; padding:9px 12px; font-weight:700; cursor:pointer; } button:disabled { opacity:.45; cursor:not-allowed; }
    button.ghost { background:#eee9df; color:var(--ink); } button.warn { background:var(--warn); } button.danger { background:var(--danger); }
    .stats { display:grid; grid-template-columns:repeat(auto-fill,minmax(130px,1fr)); gap:10px; margin-bottom:14px; } .stat strong { display:block; font-size:24px; }
    .grid { display:grid; grid-template-columns:repeat(auto-fill,minmax(380px,1fr)); gap:12px; } .card { display:grid; gap:8px; }
    .meta { color:var(--muted); font-size:13px; } .rules li { margin:6px 0; font-size:13px; color:var(--ink); }
    .pill { display:inline-block; border:1px solid var(--line); border-radius:999px; padding:3px 8px; font-size:12px; }
    .ph-待领取 { background:#eee9df; } .ph-制片中 { background:#e3ead9; color:var(--accent); } .ph-交接中 { background:#e2e8ef; color:#34507a; }
    .ph-已归还 { background:#e8e8e4; color:var(--stone); } .ph-隔离中 { background:#f6e1dc; color:var(--danger); border-color:#e3b4ab; }
    .badge { display:inline-block; border-radius:6px; padding:2px 7px; font-size:12px; font-weight:700; }
    .t-ok { background:#e3ead9; color:var(--accent); } .t-warn { background:#f6ead7; color:var(--warn); } .t-bad { background:#f6e1dc; color:var(--danger); }
    .slice { border-top:1px solid var(--line); padding-top:10px; display:grid; gap:8px; }
    .custody { display:flex; flex-wrap:wrap; gap:6px 12px; align-items:center; font-size:13px; }
    .acts { display:grid; grid-template-columns:1fr 1fr; gap:6px; align-items:end; border:1px dashed var(--line); border-radius:8px; padding:8px; background:#fbfcf9; }
    .acts .wide { grid-column:1 / -1; } .acts label { margin:2px 0 2px; font-size:12px; }
    .check { display:flex; align-items:center; gap:6px; font-size:13px; color:var(--muted); } .check input { width:auto; }
    .banner { border-radius:8px; padding:8px 10px; font-size:13px; } .banner.bad { background:#f6e1dc; color:var(--danger); border:1px solid #e3b4ab; }
    .banner.warn { background:#f6ead7; color:var(--warn); border:1px solid #e5cfa7; }
    .qbox { border:1px solid #e3b4ab; background:#fdf3f0; border-radius:8px; padding:10px; display:grid; gap:6px; }
    .timeline { list-style:none; margin:6px 0 0; padding:0; display:grid; gap:6px; max-height:220px; overflow:auto; }
    .timeline li { border-left:3px solid var(--line); padding:2px 0 2px 10px; font-size:13px; } .timeline .t { color:var(--muted); font-size:12px; margin-right:6px; }
    .handover-box { border:1px solid #c8d4e4; background:#f4f7fb; border-radius:8px; padding:8px 10px; font-size:13px; display:grid; gap:4px; }
    #toast { position:fixed; top:14px; left:50%; transform:translateX(-50%); display:none; padding:10px 18px; border-radius:8px; font-size:14px; z-index:10; box-shadow:0 4px 14px rgba(0,0,0,.15); }
    #toast.ok { display:block; background:#e3ead9; color:var(--accent); border:1px solid #bccbb0; }
    #toast.err { display:block; background:#f6e1dc; color:var(--danger); border:1px solid #e3b4ab; }
    @media (max-width:980px){ header{display:block;padding:18px 16px;} main{grid-template-columns:1fr;padding:16px;} }
  </style>
</head>
<body>
  <header>
    <div><h1>切片交接与污染隔离台</h1><div class="meta">领取 · 制片 · 交接 · 归还 全链路位置与责任人；污染与温控超时自动转隔离复核</div></div>
    <button id="reload">刷新</button>
  </header>
  <main>
    <div style="display:grid;gap:14px;align-content:start;">
      <form id="form">
        <h2>创建岩芯样本</h2>
        <label>项目</label><input name="project" required>
        <label>钻孔编号</label><input name="borehole" required>
        <label>岩芯箱号</label><input name="coreBox" required>
        <label>取样深度</label><input name="depth" required>
        <label>负责人</label><input name="owner" required>
        <label>初始切片编号</label><input name="sliceId" required>
        <label>染色方法</label><input name="method" required>
        <button>保存样本（切片入库待领取）</button>
      </form>
      <section class="panel">
        <h2>规则</h2>
        <ul class="rules">
          <li>每片切片按 <b>领取 → 制片 → 交接 → 归还</b> 记录当前位置与责任人。</li>
          <li><b>已污染</b>或<b>超过温控时限</b>的切片不能再交接，只能进入隔离复核。</li>
          <li>同一片仅允许一笔<b>未归还交接</b>；重复或并发领取只成功一次。</li>
          <li>归还必须核对<b>原责任人</b>与<b>密封状态</b>，不符即拒绝。</li>
          <li>隔离解除后恢复原流程；历史位置与污染原因只追加、刷新后仍一致。</li>
        </ul>
      </section>
    </div>
    <section>
      <div class="stats" id="stats"></div>
      <div class="grid" id="samples"></div>
    </section>
  </main>
  <div id="toast"></div>
  <script>
    const steps = ${JSON.stringify(taskSteps)};
    const reasons = ${JSON.stringify(quarantineReasons)};
    const statsEl = document.querySelector("#stats");
    const samplesEl = document.querySelector("#samples");
    const toastEl = document.querySelector("#toast");
    let samples = [];
    let toastTimer = null;

    function esc(v) {
      return String(v == null ? "" : v).replace(/[&<>"']/g, function (c) {
        return { "&":"&amp;", "<":"&lt;", ">":"&gt;", '"':"&quot;", "'":"&#39;" }[c];
      });
    }
    function fmt(at) {
      try { return new Date(at).toLocaleString("zh-CN", { hour12:false }); } catch (e) { return esc(at); }
    }
    function remain(ms) {
      if (ms == null) return "";
      const a = Math.abs(ms), h = Math.floor(a/3600000), m = Math.floor(a%3600000/60000), s = Math.floor(a%60000/1000);
      const t = (h ? h + ":" : "") + String(m).padStart(2,"0") + ":" + String(s).padStart(2,"0");
      return ms < 0 ? "已超时 " + t : "剩余 " + t;
    }
    function uid() {
      return window.crypto && crypto.randomUUID ? crypto.randomUUID() : "r-" + Date.now() + "-" + Math.random().toString(16).slice(2);
    }
    function toast(msg, ok) {
      toastEl.textContent = msg;
      toastEl.className = ok ? "ok" : "err";
      clearTimeout(toastTimer);
      toastTimer = setTimeout(function () { toastEl.className = ""; }, 3200);
    }
    async function api(path, options) {
      const res = await fetch(path, options && options.body ? Object.assign({}, options, { headers:{ "Content-Type":"application/json" } }) : options);
      const data = await res.json();
      if (!res.ok) throw new Error(data.message || data.error || "请求失败");
      return data;
    }

    function stepsHtml(selected) {
      return steps.map(function (step) { return '<option' + (step === selected ? ' selected' : '') + '>' + step + '</option>'; }).join("");
    }
    function reasonOptions(current) {
      return Object.keys(reasons).map(function (k) {
        return '<option value="' + k + '"' + (k === current ? ' selected' : '') + '>' + reasons[k] + '</option>';
      }).join("");
    }

    function historyHtml(slice) {
      const list = slice.history.slice().reverse().map(function (ev) {
        let line = '<li><span class="t">' + fmt(ev.at) + '</span><b>' + esc(ev.type) + '</b>';
        if (ev.reasonText) line += ' <span class="badge t-bad">' + esc(ev.reasonText) + '</span>';
        line += '<div class="meta">' + (ev.fromPerson ? esc(ev.fromPerson) + ' @ ' + esc(ev.fromLocation || "—") : "—")
          + ' → ' + (ev.toPerson ? esc(ev.toPerson) : "库房") + ' @ ' + esc(ev.toLocation || "—") + '</div>';
        if (ev.step) line += '<div class="meta">制片步骤：' + esc(ev.step) + '</div>';
        if (ev.sealIntact === false) line += '<div class="meta">密封：破损</div>';
        if (ev.note) line += '<div class="meta">' + esc(ev.note) + '</div>';
        return line + '</li>';
      }).join("");
      return '<ul class="timeline">' + list + '</ul>';
    }

    function custodyHtml(sample, slice) {
      let temp = "温控：未计时";
      if (slice.tempDeadline) {
        const cls = slice.tempStatus === "超时" ? "t-bad" : slice.tempStatus === "临期" ? "t-warn" : "t-ok";
        temp = '<span>温控截止 ' + fmt(slice.tempDeadline) + '</span> <span class="badge ' + cls + '" data-deadline="' + slice.tempDeadline + '" data-remain="' + (slice.tempRemainingMs || 0) + '">' + remain(slice.tempRemainingMs) + '</span>';
      }
      const seal = '<span class="badge ' + (slice.sealIntact ? "t-ok" : "t-bad") + '">密封：' + (slice.sealIntact ? "完好" : "破损") + '</span>';
      return '<div class="custody"><span class="pill ph-' + slice.phase + '">' + slice.phase + '</span>'
        + '<span>📍 ' + esc(slice.location) + '</span>'
        + '<span>👤 ' + (slice.custodian ? esc(slice.custodian) : "库房（无责任人）") + '</span>'
        + seal + temp + '</div>';
    }

    function actionHtml(sample, slice) {
      const sid = sample.id, id = slice.id, out = [];
      const disabled = function (ok) { return ok ? "" : " disabled title='当前状态不允许，以服务端校验为准'"; };

      if (slice.phase === "隔离中" && slice.quarantine) {
        const q = slice.quarantine, locked = q.reason === "contaminated";
        out.push('<div class="qbox"><b>隔离复核中</b>'
          + '<div class="meta">原因：' + esc(reasons[q.reason] || q.reason) + ' · 复核员：' + esc(q.by) + ' · ' + fmt(q.at) + '</div>'
          + (q.detail ? '<div class="meta">说明：' + esc(q.detail) + '</div>' : "")
          + '<div class="meta">隔离前快照：' + esc(q.snapshot.phase) + ' · ' + esc(q.snapshot.location) + ' · 责任人 ' + esc(q.snapshot.custodian || "—") + (q.snapshot.activeHandoverId ? " · 未归还交接 " + esc(q.snapshot.activeHandoverId) : "") + '</div>'
          + '<div data-act-group="release" class="acts">'
          + '<label class="wide">复核员</label><input class="wide" data-field="by" placeholder="解除操作人">'
          + '<label class="wide">复核结论</label><select class="wide" data-field="outcome"' + (locked ? " disabled" : "") + '>'
          + (locked ? '<option value="confirmed_contamination">污染已确认（永久禁止交接）</option>' : '<option value="cleared">复核通过，解除隔离</option><option value="confirmed_contamination">确认污染（永久禁止交接）</option>') + '</select>'
          + '<label class="wide">备注</label><input class="wide" data-field="note" placeholder="复核记录">'
          + '<button class="wide danger" data-act="release" data-sample="' + sid + '" data-slice="' + id + '">解除隔离 · 恢复原流程</button>'
          + '</div></div>');
      } else {
        // 领取
        out.push('<div data-act-group="claim" class="acts">'
          + '<label>领取人</label><label>制片台位</label>'
          + '<input data-field="person" placeholder="责任人姓名"><input data-field="station" placeholder="如 制片台-2">'
          + '<label>温控时限(分钟)</label><label>密封核对</label>'
          + '<input type="number" min="1" data-field="tempLimitMinutes" value="240">'
          + '<div class="check"><input type="checkbox" data-field="sealIntact" checked>出库密封完好</div>'
          + '<input type="hidden" data-field="requestId" value="' + uid() + '">'
          + '<button class="wide" data-act="claim" data-sample="' + sid + '" data-slice="' + id + '"' + disabled(slice.flags.canClaim) + '>领取（进入制片）</button>'
          + '</div>');

        // 制片
        out.push('<div data-act-group="prepare" class="acts">'
          + '<label class="wide">制片步骤记录</label>'
          + '<select class="wide" data-field="step">' + stepsHtml(slice.status) + '</select>'
          + '<input class="wide" data-field="note" placeholder="步骤备注或观察结果">'
          + '<button class="wide" data-act="prepare" data-sample="' + sid + '" data-slice="' + id + '"' + disabled(slice.flags.canPrepare) + '>记录制片</button>'
          + '</div>');

        // 交接
        let hoNote = "";
        if (slice.contaminated) hoNote = '<div class="banner bad wide">已污染切片永久禁止交接，只能隔离复核。</div>';
        else if (slice.tempStatus === "超时") hoNote = '<div class="banner bad wide">温控时限已过，不能交接，请转隔离复核。</div>';
        else if (slice.activeHandover) hoNote = '<div class="banner warn wide">已有一笔未归还交接，归还前不能再次交接。</div>';
        out.push('<div data-act-group="handover" class="acts">'
          + '<label>接收责任人</label><label>交接位置</label>'
          + '<input data-field="toPerson" placeholder="如 观察员 何芮"><input data-field="toLocation" placeholder="如 观察室-1">'
          + '<label>本段温控时限(分钟)</label><label>密封核对</label>'
          + '<input type="number" min="1" data-field="tempLimitMinutes" value="120">'
          + '<div class="check"><input type="checkbox" data-field="sealIntact" checked>交出时密封完好</div>'
          + '<input class="wide" data-field="note" placeholder="交接备注（可选）">'
          + '<input type="hidden" data-field="requestId" value="' + uid() + '">'
          + hoNote
          + '<button class="wide" data-act="handover" data-sample="' + sid + '" data-slice="' + id + '"' + disabled(slice.flags.canHandover) + '>交接切片</button>'
          + '</div>');

        // 归还
        if (slice.activeHandover) {
          const h = slice.activeHandover;
          out.push('<div class="handover-box"><b>未归还交接 ' + esc(h.id) + '</b>'
            + '<div class="meta">' + esc(h.fromPerson) + ' → ' + esc(h.toPerson) + ' @ ' + esc(h.toLocation) + ' · ' + fmt(h.at) + '</div>'
            + '<div class="meta">归还人必须为原责任人 <b>' + esc(h.toPerson) + '</b>，且密封完好</div></div>');
        }
        out.push('<div data-act-group="return" class="acts">'
          + '<label>归还人（须为原责任人）</label><label>归还位置</label>'
          + '<input data-field="person" placeholder="核对责任人"><input data-field="toLocation" placeholder="默认 ' + STORAGE + '">'
          + '<label>密封核对</label><label>备注</label>'
          + '<div class="check"><input type="checkbox" data-field="sealIntact" checked>归还时密封完好</div>'
          + '<input data-field="note" placeholder="归还备注（可选）">'
          + '<button class="wide" data-act="return" data-sample="' + sid + '" data-slice="' + id + '" data-handover="' + (slice.activeHandover ? slice.activeHandover.id : "") + '"' + disabled(slice.flags.canReturn) + '>归还入库</button>'
          + '</div>');

        // 隔离
        const urgent = slice.tempStatus === "超时" || !slice.sealIntact || slice.contaminated;
        out.push('<div data-act-group="quarantine" class="acts">'
          + '<label>隔离原因</label><label>复核员</label>'
          + '<select data-field="reason">' + reasonOptions(slice.tempStatus === "超时" ? "temp_exceeded" : !slice.sealIntact ? "seal_broken" : "contaminated") + '</select>'
          + '<input data-field="by" placeholder="转入复核员">'
          + '<label class="wide">污染/异常说明</label><input class="wide" data-field="detail" placeholder="如：盖玻片碎裂、试剂污染、离温超 40 分钟">'
          + '<button class="wide ' + (urgent ? "danger" : "warn") + '" data-act="quarantine" data-sample="' + sid + '" data-slice="' + id + '"' + disabled(slice.flags.canQuarantine) + '>转入隔离复核</button>'
          + '</div>');
      }
      return out.join("");
    }

    function renderSlice(sample, slice) {
      let head = '<b>' + esc(slice.id) + '</b> <span class="pill">' + esc(slice.method) + '</span> <span class="meta">制片步骤：' + esc(slice.status) + '</span>';
      if (slice.contaminated) head += ' <span class="badge t-bad">已污染</span>';
      return '<div class="slice">' + head + custodyHtml(sample, slice) + actionHtml(sample, slice)
        + '<div class="meta"><b>位置与责任历史</b>（只追加）</div>' + historyHtml(slice) + '</div>';
    }

    function render() {
      const all = [];
      samples.forEach(function (sample) { sample.slices.forEach(function (slice) { all.push(slice); }); });
      const count = function (fn) { return all.filter(fn).length; };
      const tiles = [
        ["切片总数", all.length],
        ["待领取", count(function (x) { return x.phase === "待领取"; })],
        ["制片中", count(function (x) { return x.phase === "制片中"; })],
        ["交接中·未归还", count(function (x) { return x.phase === "交接中"; })],
        ["已归还", count(function (x) { return x.phase === "已归还"; })],
        ["隔离复核中", count(function (x) { return x.phase === "隔离中"; })],
        ["已污染", count(function (x) { return x.contaminated; })],
        ["温控超时", count(function (x) { return x.tempStatus === "超时"; })]
      ];
      statsEl.innerHTML = tiles.map(function (t) { return '<div class="stat"><span>' + t[0] + '</span><strong>' + t[1] + '</strong></div>'; }).join("");

      samplesEl.innerHTML = samples.map(function (sample) {
        return '<article class="card"><h3>' + esc(sample.project) + '</h3>'
          + '<span class="pill">' + esc(sample.status) + '</span>'
          + '<div class="meta">' + esc(sample.borehole) + ' · ' + esc(sample.coreBox) + ' · ' + esc(sample.depth) + ' · 负责人 ' + esc(sample.owner) + ' · ' + esc(sample.delivery) + '</div>'
          + '<div data-act-group="add" class="acts"><label>新增切片编号</label><label>染色方法</label>'
          + '<input data-field="id" placeholder="切片编号"><input data-field="method" placeholder="染色方法">'
          + '<button class="wide ghost" data-act="add" data-sample="' + sample.id + '">添加切片（入库待领取）</button></div>'
          + sample.slices.map(function (slice) { return renderSlice(sample, slice); }).join("")
          + '<button class="ghost" data-act="deliver" data-sample="' + sample.id + '">标记交付</button>'
          + '</article>';
      }).join("");
    }

    function readFields(btn) {
      const out = {};
      btn.closest("[data-act-group]").querySelectorAll("[data-field]").forEach(function (i) {
        out[i.dataset.field] = i.type === "checkbox" ? i.checked : i.value;
      });
      return out;
    }

    samplesEl.addEventListener("click", async function (ev) {
      const btn = ev.target.closest("button[data-act]");
      if (!btn) return;
      const act = btn.dataset.act, sid = btn.dataset.sample, id = btn.dataset.slice;
      let path = null, payload = {};
      try {
        if (act === "add") {
          payload = readFields(btn);
          if (!payload.id) throw new Error("请填写切片编号");
          path = "/api/samples/" + sid + "/slices";
          payload = { id: payload.id, method: payload.method || "未指定" };
        } else if (act === "deliver") {
          path = "/api/samples/" + sid + "/deliver";
        } else {
          payload = readFields(btn);
          path = "/api/samples/" + sid + "/slices/" + id + "/";
          if (act === "claim") { path += "claim"; }
          else if (act === "prepare") { path += "logs"; payload = { step: payload.step, note: payload.note || "步骤完成" }; }
          else if (act === "handover") { path += "handovers"; }
          else if (act === "return") {
            if (!btn.dataset.handover) throw new Error("没有未归还交接");
            path += "handovers/" + btn.dataset.handover + "/return";
          }
          else if (act === "quarantine") { path += "quarantine"; }
          else if (act === "release") { path += "quarantine/release"; }
        }
        btn.disabled = true;
        await api(path, { method: "POST", body: JSON.stringify(payload) });
        toast("操作成功", true);
        await load();
      } catch (e) {
        toast(e.message, false);
        btn.disabled = false;
      }
    });

    async function load() {
      const data = await api("/api/samples");
      samples = data;
      render();
    }
    document.querySelector("#reload").onclick = function () { load(); };
    document.querySelector("#form").onsubmit = async function (event) {
      event.preventDefault();
      const form = event.target;
      try {
        await api("/api/samples", { method: "POST", body: JSON.stringify(Object.fromEntries(new FormData(form).entries())) });
        form.reset();
        toast("样本已创建，切片入库待领取", true);
        await load();
      } catch (e) { toast(e.message, false); }
    };

    // 每秒刷新温控倒计时（不重拉数据，不影响正在填写的表单）
    setInterval(function () {
      document.querySelectorAll("[data-deadline]").forEach(function (el) {
        const ms = Date.parse(el.dataset.deadline) - Date.now();
        el.textContent = remain(ms);
        el.className = "badge " + (ms < 0 ? "t-bad" : ms <= 30 * 60000 ? "t-warn" : "t-ok");
      });
    }, 1000);
    // 每 60 秒同步一次服务端状态
    setInterval(load, 60000);

    load();
  </script>
</body>
</html>`;

// ---------- 业务处理 ----------

async function handleClaim(req, res, db, sample, slice) {
  const input = await body(req);
  const person = str(input.person);
  if (!person) return fail(res, 400, "person_required", "请填写领取责任人");
  if (slice.phase === "隔离中") return fail(res, 409, "quarantined", "切片正在隔离复核，解除前不能领取");
  if (slice.phase === "制片中" || slice.phase === "交接中") {
    // 同一 requestId 的重试：幂等返回第一次的领取结果
    if (input.requestId && slice.claimed && slice.claimed.requestId === input.requestId) {
      return sendJson(res, 200, decorateSlice(slice));
    }
    return fail(res, 409, "already_claimed", `切片已由「${slice.custodian}」领取且流程未结束，重复或并发领取只成功一次`);
  }
  if (input.sealIntact === false) return fail(res, 409, "seal_broken", "出库密封破损，不能领取，请先转隔离复核");

  const station = str(input.station) || "制片台";
  const limit = limitOf(input.tempLimitMinutes, DEFAULT_CLAIM_LIMIT);
  const at = nowIso();
  const fromLocation = slice.location || STORAGE;
  const fromPerson = slice.custodian;
  slice.claimed = { by: person, at, station, tempLimitMinutes: limit, requestId: str(input.requestId) || null };
  slice.phase = "制片中";
  slice.location = station;
  slice.custodian = person;
  slice.sealIntact = true;
  slice.tempDeadline = new Date(Date.now() + limit * 60000).toISOString();
  pushHistory(slice, { type: "领取", fromPerson, fromLocation, toPerson: person, toLocation: station, note: str(input.note) || `领取制片，温控时限 ${limit} 分钟` });
  updateSampleStatus(sample);
  await saveDb(db);
  return sendJson(res, 201, decorateSlice(slice));
}

async function handleHandover(req, res, db, sample, slice) {
  const input = await body(req);
  if (slice.phase === "隔离中") return fail(res, 409, "quarantined", "切片正在隔离复核，不能交接");
  if (slice.contaminated) return fail(res, 409, "contaminated", "已污染切片不能再交接，只能进入隔离复核");
  if (slice.phase === "待领取" || slice.phase === "已归还") return fail(res, 409, "not_claimed", "切片尚未领取，需先领取制片才能交接");
  const existing = activeHandover(slice);
  if (existing) {
    if (input.requestId && existing.requestId === input.requestId) return sendJson(res, 200, decorateSlice(slice));
    return fail(res, 409, "active_handover_exists", `已有未归还交接 ${existing.id}（责任人 ${existing.toPerson}），归还前不能再次交接`);
  }
  if (isTempOverdue(slice)) return fail(res, 409, "temp_overdue", "已超过温控时限，不能交接，请先转隔离复核");
  if (input.sealIntact === false) return fail(res, 409, "seal_broken", "交出时密封破损，不能交接，请先转隔离复核");
  const toPerson = str(input.toPerson);
  if (!toPerson) return fail(res, 400, "to_person_required", "请填写接收责任人");
  const toLocation = str(input.toLocation) || "观察室";
  const limit = limitOf(input.tempLimitMinutes, DEFAULT_HANDOVER_LIMIT);
  const at = nowIso();
  const handover = {
    id: newHandoverId(),
    requestId: str(input.requestId) || null,
    fromPerson: slice.custodian,
    toPerson,
    toLocation,
    at,
    sealIntact: true,
    tempLimitMinutes: limit,
    tempDeadline: new Date(Date.now() + limit * 60000).toISOString(),
    note: str(input.note) || "",
    returnedAt: null
  };
  slice.handovers.push(handover);
  slice.activeHandoverId = handover.id;
  const fromPerson = slice.custodian;
  const fromLocation = slice.location;
  slice.phase = "交接中";
  slice.location = toLocation;
  slice.custodian = toPerson;
  slice.tempDeadline = handover.tempDeadline;
  pushHistory(slice, { type: "交接", fromPerson, fromLocation, toPerson, toLocation, note: handover.note || `交接，本段温控时限 ${limit} 分钟` });
  await saveDb(db);
  return sendJson(res, 201, decorateSlice(slice));
}

async function handleReturn(req, res, db, sample, slice, handoverId) {
  const input = await body(req);
  if (slice.phase === "隔离中") return fail(res, 409, "quarantined", "切片正在隔离复核，不能归还");
  const handover = slice.handovers.find(h => h.id === handoverId && !h.returnedAt);
  if (!handover || slice.activeHandoverId !== handoverId) return fail(res, 409, "no_active_handover", "没有对应的未归还交接");
  const person = str(input.person);
  if (!person) return fail(res, 400, "person_required", "请核对并填写归还责任人");
  if (person !== handover.toPerson) {
    return fail(res, 409, "custodian_mismatch", `归还人必须是原责任人「${handover.toPerson}」，当前填写「${person}」，归还被拒绝`);
  }
  if (input.sealIntact === false) return fail(res, 409, "seal_broken", "归还密封破损，核对不通过，请转隔离复核");
  if (slice.contaminated) return fail(res, 409, "contaminated", "已污染切片不能正常归还，只能进入隔离复核");
  if (isTempOverdue(slice)) return fail(res, 409, "temp_overdue", "交接段已超过温控时限，不能归还入库，请转隔离复核");

  const returnLocation = str(input.toLocation) || STORAGE;
  handover.returnedAt = nowIso();
  handover.returnBy = person;
  handover.returnSealIntact = true;
  handover.returnLocation = returnLocation;
  handover.returnNote = str(input.note) || "";
  slice.activeHandoverId = null;
  slice.phase = "已归还";
  slice.location = returnLocation;
  slice.custodian = null;
  slice.tempDeadline = null;
  slice.sealIntact = true;
  pushHistory(slice, { type: "归还", fromPerson: person, fromLocation: handover.toLocation, toPerson: null, toLocation: returnLocation, note: handover.returnNote || `原责任人 ${person} 核对密封完好后归还` });
  await saveDb(db);
  return sendJson(res, 200, decorateSlice(slice));
}

async function handleQuarantine(req, res, db, sample, slice) {
  const input = await body(req);
  if (slice.phase === "隔离中") return fail(res, 409, "already_quarantined", "切片已在隔离复核中");
  const reason = str(input.reason);
  if (!quarantineReasons[reason]) return fail(res, 400, "bad_reason", "隔离原因必须是 contaminated / temp_exceeded / seal_broken");
  const by = str(input.by);
  if (!by) return fail(res, 400, "person_required", "请填写复核员");

  const snapshot = {
    phase: slice.phase,
    location: slice.location,
    custodian: slice.custodian,
    sealIntact: slice.sealIntact,
    tempDeadline: slice.tempDeadline,
    activeHandoverId: slice.activeHandoverId
  };
  const detail = str(input.detail);
  const q = { reason, reasonText: quarantineReasons[reason], detail, by, at: nowIso(), snapshot, releasedAt: null };
  slice.quarantine = q;
  if (reason === "contaminated") slice.contaminated = true;
  if (reason === "seal_broken") slice.sealIntact = false;
  const prevPerson = slice.custodian, prevLocation = slice.location;
  slice.phase = "隔离中";
  slice.location = QUARANTINE_BAY;
  slice.custodian = by;
  pushHistory(slice, {
    type: "隔离",
    reason,
    reasonText: quarantineReasons[reason],
    fromPerson: prevPerson,
    fromLocation: prevLocation,
    toPerson: by,
    toLocation: QUARANTINE_BAY,
    note: detail ? `${quarantineReasons[reason]}：${detail}` : quarantineReasons[reason]
  });
  await saveDb(db);
  return sendJson(res, 201, decorateSlice(slice));
}

async function handleRelease(req, res, db, sample, slice) {
  const input = await body(req);
  if (slice.phase !== "隔离中" || !slice.quarantine) return fail(res, 409, "not_quarantined", "切片不在隔离复核中");
  const by = str(input.by);
  if (!by) return fail(res, 400, "person_required", "请填写解除操作人");
  const confirmed = input.outcome === "confirmed_contamination" || slice.quarantine.reason === "contaminated";

  const q = slice.quarantine;
  q.releasedAt = nowIso();
  q.releaseBy = by;
  q.releaseNote = str(input.note);
  q.finding = confirmed ? "contaminated" : "cleared";
  if (confirmed) slice.contaminated = true;
  slice.quarantineRecords.push(q);
  slice.quarantine = null;

  // 恢复隔离前流程快照（位置、责任人、未归还交接都原样保留）
  const snap = q.snapshot;
  slice.phase = snap.phase;
  slice.location = snap.location;
  slice.custodian = snap.custodian;
  slice.activeHandoverId = snap.activeHandoverId;
  slice.sealIntact = confirmed ? false : true;
  // 复核解除后温控计时重新开始，避免恢复瞬间即超时；原时限为 0（已彻底耗尽）时回退默认时限
  const handover = activeHandover(slice);
  const limit = handover
    ? (handover.tempLimitMinutes > 0 ? handover.tempLimitMinutes : DEFAULT_HANDOVER_LIMIT)
    : (slice.claimed?.tempLimitMinutes > 0 ? slice.claimed.tempLimitMinutes : DEFAULT_CLAIM_LIMIT);
  slice.tempDeadline = new Date(Date.now() + limit * 60000).toISOString();
  pushHistory(slice, {
    type: "隔离解除",
    reason: q.reason,
    reasonText: q.reasonText,
    fromPerson: by,
    fromLocation: QUARANTINE_BAY,
    toPerson: slice.custodian,
    toLocation: slice.location,
    note: `恢复「${snap.phase}」流程${confirmed ? "；污染已确认，永久禁止交接" : "；复核通过"}；温控时限重置 ${limit} 分钟${q.releaseNote ? `；${q.releaseNote}` : ""}`
  });
  await saveDb(db);
  return sendJson(res, 200, decorateSlice(slice));
}

// ---------- 路由 ----------

const server = http.createServer(async (req, res) => {
  try {
    const url = new URL(req.url, `http://${req.headers.host}`);

    if (req.method === "GET" && url.pathname === "/") {
      res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
      return res.end(page);
    }

    if (req.method === "GET" && url.pathname === "/api/samples") {
      const db = await loadDb();
      const now = Date.now();
      return sendJson(res, 200, db.samples.map(sample => ({
        ...sample,
        slices: sample.slices.map(slice => decorateSlice(slice, now))
      })));
    }

    const getSampleMatch = url.pathname.match(/^\/api\/samples\/([^/]+)$/);
    if (getSampleMatch && req.method === "GET") {
      const db = await loadDb();
      const sample = db.samples.find(item => item.id === decodeURIComponent(getSampleMatch[1]));
      if (!sample) return fail(res, 404, "sample_not_found", "样本不存在");
      const now = Date.now();
      return sendJson(res, 200, { ...sample, slices: sample.slices.map(slice => decorateSlice(slice, now)) });
    }

    if (req.method === "POST" && url.pathname === "/api/samples") {
      const input = await body(req);
      return withLock(`sample:${input.project || Date.now()}`, async () => {
        const db = await loadDb();
        const sample = {
          id: `CORE-${Date.now()}`,
          project: input.project, borehole: input.borehole, coreBox: input.coreBox,
          depth: input.depth, owner: input.owner,
          status: "待切割", delivery: "未交付",
          slices: [newSlice(input.sliceId, input.method, input.owner)]
        };
        updateSampleStatus(sample);
        db.samples.unshift(sample);
        await saveDb(db);
        return sendJson(res, 201, sample);
      });
    }

    // 以下所有切片级变更均在样本锁内串行执行
    const addSliceMatch = url.pathname.match(/^\/api\/samples\/([^/]+)\/slices$/);
    const claimMatch = url.pathname.match(/^\/api\/samples\/([^/]+)\/slices\/([^/]+)\/claim$/);
    const handoverMatch = url.pathname.match(/^\/api\/samples\/([^/]+)\/slices\/([^/]+)\/handovers$/);
    const returnMatch = url.pathname.match(/^\/api\/samples\/([^/]+)\/slices\/([^/]+)\/handovers\/([^/]+)\/return$/);
    const logMatch = url.pathname.match(/^\/api\/samples\/([^/]+)\/slices\/([^/]+)\/logs$/);
    const quarantineMatch = url.pathname.match(/^\/api\/samples\/([^/]+)\/slices\/([^/]+)\/quarantine$/);
    const releaseMatch = url.pathname.match(/^\/api\/samples\/([^/]+)\/slices\/([^/]+)\/quarantine\/release$/);
    const deliverMatch = url.pathname.match(/^\/api\/samples\/([^/]+)\/deliver$/);

    const sliceRoute = claimMatch || handoverMatch || returnMatch || logMatch || quarantineMatch || releaseMatch;
    if (sliceRoute && req.method === "POST") {
      const sampleId = decodeURIComponent(sliceRoute[1]);
      const sliceId = sliceRoute[2] ? decodeURIComponent(sliceRoute[2]) : null;
      const handoverId = returnMatch ? decodeURIComponent(returnMatch[3]) : null;
      return withLock(`sample:${sampleId}`, async () => {
        const db = await loadDb();
        const found = locate(db, sampleId, sliceId);
        if (found.errorCode) return fail(res, found.status, found.errorCode, found.errorCode === "sample_not_found" ? "样本不存在" : "切片不存在");
        const { sample, slice } = found;
        if (claimMatch) return handleClaim(req, res, db, sample, slice);
        if (handoverMatch) return handleHandover(req, res, db, sample, slice);
        if (returnMatch) return handleReturn(req, res, db, sample, slice, handoverId);
        if (quarantineMatch) return handleQuarantine(req, res, db, sample, slice);
        if (releaseMatch) return handleRelease(req, res, db, sample, slice);
        if (logMatch) {
          const input = await body(req);
          if (slice.phase === "隔离中") return fail(res, 409, "quarantined", "隔离复核中不能记录制片");
          if (slice.phase !== "制片中") return fail(res, 409, "not_preparing", "只有制片中的切片才能记录制片步骤");
          slice.status = input.step;
          if (input.step === "观察") slice.observation = input.note || slice.observation;
          slice.logs.push({ at: nowIso(), step: input.step, note: input.note || "" });
          pushHistory(slice, { type: "制片", step: input.step, fromPerson: slice.custodian, fromLocation: slice.location, toPerson: slice.custodian, toLocation: slice.location, note: input.note || "" });
          updateSampleStatus(sample);
          await saveDb(db);
          return sendJson(res, 200, decorateSlice(slice));
        }
      });
    }

    if (addSliceMatch && req.method === "POST") {
      const sampleId = decodeURIComponent(addSliceMatch[1]);
      return withLock(`sample:${sampleId}`, async () => {
        const db = await loadDb();
        const sample = db.samples.find(item => item.id === sampleId);
        if (!sample) return fail(res, 404, "sample_not_found", "样本不存在");
        const input = await body(req);
        if (!str(input.id)) return fail(res, 400, "id_required", "请填写切片编号");
        if (sample.slices.some(item => item.id === input.id)) return fail(res, 409, "slice_exists", "切片编号已存在");
        const slice = newSlice(input.id, input.method, sample.owner);
        sample.slices.push(slice);
        updateSampleStatus(sample);
        await saveDb(db);
        return sendJson(res, 201, decorateSlice(slice));
      });
    }

    if (deliverMatch && req.method === "POST") {
      const sampleId = decodeURIComponent(deliverMatch[1]);
      return withLock(`sample:${sampleId}`, async () => {
        const db = await loadDb();
        const sample = db.samples.find(item => item.id === sampleId);
        if (!sample) return fail(res, 404, "sample_not_found", "样本不存在");
        sample.delivery = "已交付";
        updateSampleStatus(sample);
        await saveDb(db);
        return sendJson(res, 200, sample);
      });
    }

    sendJson(res, 404, { error: "not_found", message: "接口不存在" });
  } catch (error) {
    sendJson(res, 500, { error: "server_error", message: error.message });
  }
});

server.listen(port, () => console.log(`Slice custody & quarantine console listening on http://localhost:${port}`));
