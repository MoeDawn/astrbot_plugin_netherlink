/*
 * NetherLink 管理面板（C1：骨架 + 桥接 + 在线服务器 / 好感度两节；
 *                    C2：绑定管理 / 指令审计 / 快捷指令三节 + 迁移二次确认）。
 *
 * 🔴 三条桥接硬约束（写错都是**静默失败**，官方 plugin-pages.md 核实）：
 *   1. 本文件是**外部 module 文件**（`index.html` 里 `<script type="module" src="./app.js">`）
 *      ——内联脚本无法同步访问 `window.AstrBotPluginPage`。
 *   2. `apiGet` / `apiPost` 的 endpoint 是**插件内相对路径**（`"servers"`）：
 *      **不带插件名前缀**、**不以 `/` 开头**、**不含 query / hash / `\` / scheme /
 *      `.` / `..` 片段**。查询参数走 `apiGet` 的**第二个参数对象**，
 *      绝不拼进 endpoint 字符串——拼进去的形状（`apiGet("audit" + "?limit=1")`）
 *      不在静态交叉校验的覆盖里，是**静默 404**。Dashboard 会补成
 *      `/api/v1/plugins/extensions/<插件名>/<endpoint>`。
 *   3. **不做懒加载**（没有动态 `import()`、没有按需 fetch 的资产）：页面资产挂在
 *      有效期 **60 秒**的 JWT asset token 上（`PLUGIN_PAGE_ASSET_TOKEN_TTL_SECONDS = 60`），
 *      页面打开一分钟后再去拉新资产会 401。单页、开机一次拉齐。
 *
 * 返回体口径：bridge 把 `{"status":"ok","data":v}` 解成 `v`，把
 * `{"status":"error",...}` 与 HTTP 失败**变成 rejected promise**。
 * 所以每次调用都必须 try/catch，并把 `error.message` 显示给管理员——
 * 后端 handler 的报错文案（如好感值超出 karma_min~karma_max、qq 必须是 5~13 位
 * 纯数字）正是靠这条路到达眼前。
 *
 * ⚠️ 本文件**没有被真机执行过**：本机没有 AstrBot、没有 docker，面板打不开。
 *    静态守卫能证明的只有「endpoint 对得上注册表」「DOM id 都在」「迁移确认的
 *    结构没被绕开」这些**形状**；「点了按钮会不会动」要看用户在真机上的验收。
 */

const bridge = window.AstrBotPluginPage;

/** 审计条数的默认值，与后端 `PANEL_AUDIT_LIMIT` 一致。 */
const DEFAULT_AUDIT_LIMIT = 100;

/** 迁移对话框相关元素。抽成常量是为了让「哪颗按钮下发请求」一眼可读。 */
const MIGRATE_DIALOG_ID = "bindings-migrate-dialog";

/** 页面状态。 */
const state = {
  servers: [],
  karma: [],
  karmaQuery: "",
  bindings: [],
  bindingsQuery: "",
  audit: [],
  auditQuery: "",
  commands: [],
};

/* ------------------------------------------------------------------ */
/* DOM 小工具                                                          */
/* ------------------------------------------------------------------ */

function byId(id) {
  return document.getElementById(id);
}

/**
 * 写一行状态提示。
 * `kind` 取 "ok" / "error" / "busy"，传空串则只清空。
 */
function setMessage(node, text, kind) {
  if (!node) {
    return;
  }
  node.textContent = text || "";
  node.classList.remove("msg--ok", "msg--error", "msg--busy");
  if (kind) {
    node.classList.add("msg--" + kind);
  }
}

/** 建一个单元格，内容一律走 textContent（数据来自配置项，绝不拼 HTML）。 */
function cell(text, className) {
  const td = document.createElement("td");
  td.textContent = text === null || text === undefined ? "" : String(text);
  if (className) {
    td.className = className;
  }
  return td;
}

/** 用一行「没有数据」占满整表，比空 tbody 更能说明「是空的，不是坏了」。 */
function emptyRow(tbody, columns, text) {
  tbody.replaceChildren();
  const tr = document.createElement("tr");
  const td = document.createElement("td");
  td.colSpan = columns;
  td.className = "empty";
  td.textContent = text;
  tr.appendChild(td);
  tbody.appendChild(tr);
}

/** 请求进行中把按钮禁掉，免得管理员连点出一串并发写。 */
function setBusy(nodes, busy) {
  for (const node of nodes) {
    if (node) {
      node.disabled = !!busy;
    }
  }
}

/** 从 rejected promise 里取出给人看的一句话。 */
function errorText(error) {
  if (error && error.message) {
    return error.message;
  }
  return String(error);
}

/* ------------------------------------------------------------------ */
/* 主题                                                                */
/* ------------------------------------------------------------------ */

/**
 * 把主题写到 `<html data-theme>`。
 *
 * AstrBot 返回 HTML 时**已经预注入**过一次（减少初始闪烁），通常不需要本函数。
 * 它是兜底：万一预注入缺席，`ctx.isDark` 也还能把颜色摆正。
 * ⚠️ 不读 `prefers-color-scheme`——用户可以在 WebUI 里手动选与系统相反的主题，
 * 那时系统偏好是错的信号。
 */
function applyTheme(isDark) {
  document.documentElement.setAttribute(
    "data-theme",
    isDark ? "dark" : "light",
  );
}

/* ------------------------------------------------------------------ */
/* 在线服务器                                                          */
/* ------------------------------------------------------------------ */

async function loadServers() {
  const msg = byId("servers-msg");
  setMessage(msg, "读取中…", "busy");
  try {
    state.servers = await bridge.apiGet("servers");
  } catch (error) {
    state.servers = [];
    renderServers();
    setMessage(msg, "读取服务器列表失败：" + errorText(error), "error");
    return;
  }
  renderServers();
  const rows = state.servers || [];
  setMessage(
    msg,
    rows.length ? "共 " + rows.length + " 台在线。" : "",
    "ok",
  );
}

function renderServers() {
  const tbody = byId("servers-body");
  const rows = state.servers || [];
  if (!rows.length) {
    emptyRow(tbody, 4, "当前没有 MC 服务器连接。");
    return;
  }
  tbody.replaceChildren();
  for (const row of rows) {
    const tr = document.createElement("tr");
    tr.appendChild(cell(row.id, "mono"));
    tr.appendChild(cell(row.display));
    tr.appendChild(cell(row.port, "mono"));
    tr.appendChild(cell(row.reported_name || "—", "mono"));
    tbody.appendChild(tr);
  }
}

/* ------------------------------------------------------------------ */
/* 好感度                                                              */
/* ------------------------------------------------------------------ */

/**
 * 拉取好感记录。
 *
 * ⚠️ `quiet` = 写操作之后的**静默重载**：那一趟不该去动提示行，否则
 *    「已写入 qq:123 → 30」这类**结果**会被紧跟着的「共 N 条记录」冲掉，
 *    管理员就看不到这次到底改成了什么。失败仍然照报（错误比结果重要）。
 */
async function loadKarma(quiet) {
  const msg = byId("karma-msg");
  if (!quiet) {
    setMessage(msg, "读取中…", "busy");
  }
  const params = {};
  if (state.karmaQuery) {
    params.q = state.karmaQuery;
  }
  let rows;
  try {
    rows = await bridge.apiGet("karma", params);
  } catch (error) {
    state.karma = [];
    renderKarma();
    setMessage(msg, "读取好感度记录失败：" + errorText(error), "error");
    return;
  }
  state.karma = rows;
  renderKarma();
  if (!quiet) {
    const list = state.karma || [];
    setMessage(msg, list.length ? "共 " + list.length + " 条记录。" : "", "ok");
  }
}

function renderKarma() {
  const tbody = byId("karma-body");
  const rows = state.karma || [];
  if (!rows.length) {
    emptyRow(tbody, 3, "没有好感度记录。");
    return;
  }
  tbody.replaceChildren();
  for (const row of rows) {
    tbody.appendChild(karmaRow(row));
  }
}

function karmaRow(row) {
  const tr = document.createElement("tr");
  tr.appendChild(cell(row.key, "mono"));

  // 值原样显示：`karma_records` 允许管理员手改，手改出非数字时也要看得见
  // （panel.karma_view 刻意不转换、不四舍五入）。
  tr.appendChild(cell(row.value, "mono"));

  const actions = document.createElement("td");
  actions.className = "col-actions";

  const input = document.createElement("input");
  input.type = "text";
  input.className = "input input--num";
  input.value = row.value === null || row.value === undefined ? "" : String(row.value);
  input.setAttribute("aria-label", "好感值 " + row.key);

  const save = document.createElement("button");
  save.type = "button";
  save.className = "btn";
  save.textContent = "保存";

  const remove = document.createElement("button");
  remove.type = "button";
  remove.className = "btn btn--danger";
  remove.textContent = "删除";

  // 键用闭包带过去，不拼进 DOM 字符串——键里有引号也不会破坏结构。
  const key = row.key;
  save.addEventListener("click", () => {
    saveKarmaValue(key, input.value, [save, remove]);
  });
  remove.addEventListener("click", () => {
    deleteKarmaValue(key, [save, remove]);
  });

  actions.append(input, save, remove);
  tr.appendChild(actions);
  return tr;
}

/**
 * 写入一条好感值。
 *
 * ⚠️ 值**不在前端做范围校验**：范围由插件的 `karma_min` / `karma_max` 决定，
 * 前端写死一个 -50~100 会在管理员改了配置之后**静默判错**。
 * 形状像整数就转成数字发过去，否则**原样发**——由服务端拒绝并把话说明白，
 * 前端负责把那句话显示出来。
 */
async function saveKarmaValue(key, rawValue, buttons) {
  const msg = byId("karma-msg");
  const text = String(rawValue === null || rawValue === undefined ? "" : rawValue).trim();
  const value = /^-?\d+$/.test(text) ? Number(text) : text;

  setBusy(buttons, true);
  setMessage(msg, "写入中…", "busy");
  let ok = false;
  try {
    const result = await bridge.apiPost("karma/set", { key: key, value: value });
    // 🔴 `&&` 比 `?:` 优先级**高**。写成
    //      result && result.old === null ? "（无记录）" : result.old
    // 时，`result` 为空会让条件整体为假 → 走 else 分支 → 解引用 `result.old`
    // 抛 TypeError → 被下面的 catch 接住 → 渲染成「写入失败」，
    // **而服务端其实写成功了**。三元的分支必须自成一体（同 deleteKarmaValue）。
    const before =
      result && result.old !== null && result.old !== undefined
        ? result.old
        : "（无记录）";
    const after =
      result && result.value !== undefined ? result.value : value;
    setMessage(msg, "已写入 " + key + "：" + before + " → " + after, "ok");
    ok = true;
  } catch (error) {
    setMessage(msg, "写入失败：" + errorText(error), "error");
  } finally {
    setBusy(buttons, false);
  }
  // 失败时**不重载**：重载会把刚写上去的报错冲掉，管理员只看到「共 N 条记录」，
  // 误以为改成功了。上面那条报错留在屏幕上，让他自己决定要不要重新加载。
  if (ok) {
    await loadKarma(true);
  }
}

async function deleteKarmaValue(key, buttons) {
  const msg = byId("karma-msg");
  setBusy(buttons, true);
  setMessage(msg, "删除中…", "busy");
  let ok = false;
  try {
    const result = await bridge.apiPost("karma/delete", { key: key });
    // 删一个本来就不存在的键**不是错误**（删除是幂等的），后端给 deleted:false
    // 的正常响应——照它的话显示，别渲染成红色故障。
    const text = result && result.message ? result.message : "已处理 " + key;
    setMessage(msg, text, result && result.deleted ? "ok" : "busy");
    ok = true;
  } catch (error) {
    setMessage(msg, "删除失败：" + errorText(error), "error");
  } finally {
    setBusy(buttons, false);
  }
  if (ok) {
    await loadKarma(true);
  }
}

/* ------------------------------------------------------------------ */
/* 绑定管理                                                            */
/* ------------------------------------------------------------------ */

async function loadBindings(quiet) {
  const msg = byId("bindings-msg");
  if (!quiet) {
    setMessage(msg, "读取中…", "busy");
  }
  const params = {};
  if (state.bindingsQuery) {
    params.q = state.bindingsQuery;
  }
  let rows;
  try {
    rows = await bridge.apiGet("bindings", params);
  } catch (error) {
    state.bindings = [];
    renderBindings();
    setMessage(msg, "读取绑定表失败：" + errorText(error), "error");
    return;
  }
  state.bindings = rows;
  renderBindings();
  if (!quiet) {
    const list = state.bindings || [];
    setMessage(
      msg,
      list.length ? "共 " + list.length + " 条绑定。" : "",
      "ok",
    );
  }
}

function renderBindings() {
  const tbody = byId("bindings-body");
  const rows = state.bindings || [];
  if (!rows.length) {
    emptyRow(tbody, 7, "没有绑定记录（也可能只是被过滤条件挡住了）。");
    return;
  }
  tbody.replaceChildren();
  for (const row of rows) {
    tbody.appendChild(bindingRow(row));
  }
}

function bindingRow(row) {
  const tr = document.createElement("tr");
  tr.appendChild(cell(row.player, "mono"));
  tr.appendChild(cell(row.qq, "mono"));
  tr.appendChild(cell(row.qq_name || "—"));
  tr.appendChild(cell((row.servers || []).join("、") || "—", "mono"));
  tr.appendChild(cell(row.bound_at || "—", "mono"));
  // method 是内部枚举（code / manual），原样显示、不翻译成中文——
  // 翻译表会与服务端新增的取值静默失配。
  tr.appendChild(cell(row.method || "—", "mono"));

  const actions = document.createElement("td");
  actions.className = "col-actions";
  const remove = document.createElement("button");
  remove.type = "button";
  remove.className = "btn btn--danger";
  remove.textContent = "解绑";
  const player = row.player;
  remove.addEventListener("click", () => {
    unbindBinding(player, [remove]);
  });
  actions.appendChild(remove);
  tr.appendChild(actions);
  return tr;
}

/**
 * 改绑：把一个游戏 ID 指到另一个 QQ。
 *
 * ⚠️ 表单校验**只做「非空」**：QQ 号的位数、纯数字、昵称类型全部由服务端判，
 *    前端自己写一份会在服务端改了规则之后**静默判错**（同 `saveKarmaValue`）。
 * ⚠️ 成功后**清空表单**：不改绑成功之后还留着旧游戏 ID，管理员再点一次就会
 *    拿同一个 ID 覆盖一遍——看起来没反应。
 */
async function rebindBinding() {
  const msg = byId("bindings-msg");
  const player = byId("bindings-rebind-player").value.trim();
  const qq = byId("bindings-rebind-qq").value.trim();
  const qqName = byId("bindings-rebind-name").value.trim();
  if (!player || !qq) {
    setMessage(msg, "游戏 ID 与 QQ 号都不能为空。", "error");
    return;
  }

  const button = byId("bindings-rebind-submit");
  setBusy([button], true);
  setMessage(msg, "改绑中…", "busy");
  let ok = false;
  try {
    const result = await bridge.apiPost("bindings/rebind", {
      player: player,
      qq: qq,
      qq_name: qqName,
    });
    const text = result && result.message ? result.message : "已改绑 " + player;
    setMessage(msg, text, "ok");
    ok = true;
  } catch (error) {
    setMessage(msg, "改绑失败：" + errorText(error), "error");
  } finally {
    setBusy([button], false);
  }
  if (ok) {
    byId("bindings-rebind-player").value = "";
    byId("bindings-rebind-qq").value = "";
    byId("bindings-rebind-name").value = "";
    await loadBindings(true);
  }
}

/**
 * 解绑一个游戏 ID。
 *
 * ⚠️ 后端把「本来就没绑」当成**正常响应**（`removed: false`），照它的话显示，
 *    别渲染成红色故障——那会让管理员对着过期面板反复重试。
 */
async function unbindBinding(player, buttons) {
  const msg = byId("bindings-msg");
  setBusy(buttons, true);
  setMessage(msg, "解绑中…", "busy");
  let ok = false;
  try {
    const result = await bridge.apiPost("bindings/unbind", { player: player });
    const text = result && result.message ? result.message : "已处理 " + player;
    setMessage(msg, text, result && result.removed ? "ok" : "busy");
    ok = true;
  } catch (error) {
    setMessage(msg, "解绑失败：" + errorText(error), "error");
  } finally {
    setBusy(buttons, false);
  }
  if (ok) {
    await loadBindings(true);
  }
}

/* ---------------- 好感迁移：二次确认 ---------------- */

/**
 * 打开确认对话框。**它只负责显示，绝不发任何请求**——
 * 这是「触发器不能直接下发」这条结构的半边，另半边是 `runMigrateConfirm`
 * 只被对话框里的确认按钮引用（见 `bindMigrateControls`）。
 *
 * ⚠️ `<dialog>` 的 `showModal` 不存在时（老 WebView）退化成 `open` 属性：
 *    退化的只是**居中与遮罩**，确认这一步照样要走完，不会退化成直接执行。
 */
function openMigrateConfirm() {
  const dialog = byId(MIGRATE_DIALOG_ID);
  if (!dialog) {
    return;
  }
  setMessage(byId("bindings-migrate-result"), "", "");
  if (typeof dialog.showModal === "function") {
    dialog.showModal();
  } else {
    dialog.setAttribute("open", "");
  }
}

function closeMigrateConfirm() {
  const dialog = byId(MIGRATE_DIALOG_ID);
  if (!dialog) {
    return;
  }
  if (typeof dialog.close === "function") {
    dialog.close();
  } else {
    dialog.removeAttribute("open");
  }
}

/**
 * 🔴 **`bindings/migrate` 的唯一调用点**，而且只挂在确认对话框里的按钮上。
 *
 * 为什么是这个形状（而不是在触发器里先问一句）：
 *   后端**刻意**不接收 `confirm` 之类的形式参数（见 `_api_bindings_migrate`），
 *   那种布尔值总能被构造出来。所以拦人的责任整个在前端结构上——
 *   「触发器只开对话框」+「下发请求的函数只被对话框的确认按钮引用」，
 *   两条缺一不可。任何把 `runMigrateConfirm` 接到触发器上的改动都会让
 *   `tests/test_panel_frontend.py` 的迁移确认守卫变红（已变异验证）。
 *
 * ⚠️ 失败路径要**分清三种结局**，别一律报成失败（同「不能把失败报成成功」的
 *    反面）：迁移成功 / 无可迁移项（幂等，不是错误）/ 真失败。
 *    后端把「无可迁移项」做成**正常响应**（`migrated: false`），照它的话显示。
 */
async function runMigrateConfirm() {
  closeMigrateConfirm();
  const msg = byId("bindings-migrate-result");
  const buttons = [byId("bindings-migrate"), byId("bindings-migrate-confirm")];
  setBusy(buttons, true);
  setMessage(msg, "迁移中…", "busy");
  try {
    // ⚠️ 显式传 `{}` 而不是省略第二个实参：这个接口**没有**任何入参（后端刻意
    //    不读 body），传空对象只是「没有请求体」的稳妥写法——把 `undefined`
    //    交给 `JSON.stringify` 会得到非字符串，那是桥接 SDK 的内部行为，
    //    不值得赌。
    const result = await bridge.apiPost("bindings/migrate", {});
    const changes = (result && result.changes) || [];
    if (result && result.migrated) {
      const parts = [];
      for (const change of changes) {
        parts.push(
          change.key +
            "：" +
            change.old +
            " → " +
            change.value +
            "（并入 " +
            ((change.removed || []).join("、") || "无") +
            "）",
        );
      }
      const warnings = (result.warnings || []).join("；");
      setMessage(
        msg,
        (result.message || "迁移完成") + " " + parts.join(" | ") +
          (warnings ? " ⚠️ " + warnings : ""),
        "ok",
      );
    } else {
      // 幂等的第二次点击走这里——**不是错误**，所以用 busy 色而不是错误色。
      setMessage(msg, (result && result.message) || "没有可迁移的记录。", "busy");
    }
  } catch (error) {
    setMessage(msg, "迁移失败：" + errorText(error), "error");
  } finally {
    setBusy(buttons, false);
  }
  // 迁移改的是好感记录（不是绑定表），所以刷新好感那一节。
  await loadKarma(true);
}

/* ------------------------------------------------------------------ */
/* 指令审计                                                            */
/* ------------------------------------------------------------------ */

/**
 * 读条数输入框，**非法值一律不下发**。
 *
 * 🔴 `?limit=` 是**外部输入**：后端 `_panel_limit` 对 `<=0` 会回退默认值
 *    （`read_audit` 对 `<=0` 更是直接返回空表）。前端自己先拦一道，是为了让
 *    「填 0 想看全部」这种笔误在**本地**就得到一句人话，而不是拿到一张空表
 *    以为审计功能坏了。
 *
 * 返回 `null` 表示非法——调用方据此**放弃这一趟请求**，绝不发一个负数出去。
 */
function auditLimit() {
  const raw = byId("audit-limit").value.trim();
  if (!/^\d+$/.test(raw)) {
    return null;
  }
  const value = Number(raw);
  if (!(value > 0)) {
    return null;
  }
  return value;
}

async function loadAudit(quiet) {
  const msg = byId("audit-msg");
  const limit = auditLimit();
  if (limit === null) {
    setMessage(msg, "条数必须是正整数（后端对 0 与负数会回退默认值）。", "error");
    return;
  }
  if (!quiet) {
    setMessage(msg, "读取中…", "busy");
  }
  const params = { limit: limit };
  if (state.auditQuery) {
    params.q = state.auditQuery;
  }
  let rows;
  try {
    rows = await bridge.apiGet("audit", params);
  } catch (error) {
    state.audit = [];
    renderAudit();
    setMessage(msg, "读取审计失败：" + errorText(error), "error");
    return;
  }
  state.audit = rows;
  renderAudit();
  if (!quiet) {
    const list = state.audit || [];
    setMessage(
      msg,
      list.length ? "显示最近 " + list.length + " 条（上限 " + limit + "）。" : "",
      "ok",
    );
  }
}

function renderAudit() {
  const tbody = byId("audit-body");
  const rows = state.audit || [];
  if (!rows.length) {
    emptyRow(tbody, 8, "没有审计记录（也可能只是被过滤条件挡住了）。");
    return;
  }
  tbody.replaceChildren();
  for (const row of rows) {
    const tr = document.createElement("tr");
    tr.appendChild(cell(row.ts || "—", "mono"));
    tr.appendChild(cell(row.event || "—", "mono mono--event"));
    tr.appendChild(cell(row.initiator || "—", "mono"));
    tr.appendChild(cell(row.source || "—", "mono"));
    tr.appendChild(cell(row.server || "—", "mono"));
    // 报价与实扣分两列显示：成功了才扣（spent>0），被拒/失败时 spent 是 0
    // 而 quoted 仍在——只显示一个数会让「报了价但没扣」看起来像「没报价」。
    tr.appendChild(
      cell("报 " + auditNumber(row.quoted) + " / 扣 " + auditNumber(row.spent), "mono"),
    );
    tr.appendChild(cell(row.cmd || "—", "mono"));
    tr.appendChild(cell(row.note || ""));
    tbody.appendChild(tr);
  }
}

/** 审计里的数字字段可能缺失或不是数字，一律**原样展示**，不编造 0。 */
function auditNumber(value) {
  if (value === null || value === undefined || value === "") {
    return "—";
  }
  return String(value);
}

/* ------------------------------------------------------------------ */
/* 快捷指令                                                            */
/* ------------------------------------------------------------------ */

async function loadCommands() {
  const msg = byId("commands-msg");
  setMessage(msg, "读取中…", "busy");
  let payload;
  try {
    payload = await bridge.apiGet("commands");
  } catch (error) {
    state.commands = [];
    renderCommands();
    setMessage(msg, "读取快捷指令失败：" + errorText(error), "error");
    return;
  }
  state.commands = (payload && payload.commands) || [];
  // 🔴 安全说明**必须显示**：这些指令不经 AI、不扣好感度，而面板要登录所以等同
  //    管理员权限。后端把它放进响应体正是为了让前端没有理由漏掉它——原文只有
  //    `panel.QUICK_COMMAND_SECURITY_NOTE` 一份，前端**不自己编**（编一份就会
  //    与服务端那份漂移，而这正是安全边界的表述）。
  byId("commands-security-note").textContent =
    (payload && payload.security_note) || "";
  byId("commands-security-note").hidden = !(payload && payload.security_note);
  renderCommands();
  const list = state.commands || [];
  setMessage(
    msg,
    list.length ? "共 " + list.length + " 条快捷指令。" : "",
    "ok",
  );
}

function renderCommands() {
  const list = byId("commands-list");
  list.replaceChildren();
  const rows = state.commands || [];
  if (!rows.length) {
    const p = document.createElement("p");
    p.className = "empty";
    p.textContent =
      "没有可用的快捷指令（配置项 quick_commands 的格式是 名称|指令|服务器；" +
      "缺少名称或指令的条目会被服务端跳过）。";
    list.appendChild(p);
    return;
  }
  for (const item of rows) {
    list.appendChild(commandButton(item));
  }
}

function commandButton(item) {
  const button = document.createElement("button");
  button.type = "button";
  button.className = "btn cmd-btn";
  button.textContent = item.name;
  button.title = item.server ? item.cmd + "（服务器 " + item.server + "）" : item.cmd;
  button.addEventListener("click", () => {
    runQuickCommand(item, button);
  });
  return button;
}

/**
 * 下发一条快捷指令。
 *
 * ⚠️ `index` 用的是**服务端解析后的序号**（`item.index`），不是本数组的下标——
 *    两者今天相等，但坏条目被跳过后位置会前移，将来任何一处过滤都会让
 *    「点第 2 个按钮、跑了第 3 条指令」，且**不报错**。闭环用服务端给的那个。
 */
async function runQuickCommand(item, button) {
  const msg = byId("commands-msg");
  setBusy([button], true);
  setMessage(msg, "执行中…", "busy");
  try {
    const result = await bridge.apiPost("commands/run", { index: item.index });
    renderCommandResult(result);
    setMessage(msg, "", "");
  } catch (error) {
    // 这条是**请求本身**失败（越界、断网）——与「指令没生效」不同，红色。
    setMessage(msg, "执行失败：" + errorText(error), "error");
  } finally {
    setBusy([button], false);
  }
}

/**
 * 把 `commands/run` 的返回体判成**四个互不相同的结局**。
 *
 * 🔴 这四个结局**不能压成一个**：把「服务器明确回了失败」渲染成成功，正是本
 *    项目第一轮修的那个跨端 bug 的形状（claude.md 坑 4）。判据来自后端
 *    `_command_payload` 的两个字段：
 *
 *    | 结局 | connected | ok |
 *    |---|---|---|
 *    | 成功 | true | true |
 *    | 服务器明确报失败 | true | false |
 *    | 目标服务器不在线 → **未下发** | false | null（且 server 有值、server_id 空）|
 *    | 没有回执（未连接 / 多台在线被拒发 / 超时） | false | null |
 *
 * ⚠️ 这个函数是**纯函数**（不碰 DOM），所以 `tests/test_panel_frontend.py`
 *    能把它抠出来用 node 真跑一遍，断言四个输入得到四个不同的标签。
 *    改动它时别引入 DOM 依赖，否则那条守卫会失效。
 */
function commandOutcome(result) {
  const connected = result && result.connected === true;
  const ok = result ? result.ok : null;
  if (connected && ok === true) {
    return {
      kind: "ok",
      label: "执行成功",
      detail: "服务器已执行这条指令。",
    };
  }
  if (connected && ok === false) {
    return {
      kind: "error",
      label: "服务器报告执行失败",
      detail: "服务器明确回了失败——这条指令没有生效。",
    };
  }
  if (!connected && result && result.server && !result.server_id) {
    return {
      kind: "warn",
      label: "未下发（目标服务器不在线）",
      detail: "配置里的目标服务器当前不在线，指令没有发给任何一台服务器。",
    };
  }
  return {
    kind: "warn",
    label: "没有回执",
    detail: "MC 未连接、在线服务器不止一台（插件拒发），或等待回执超时——无法判断是否生效。",
  };
}

function renderCommandResult(result) {
  const box = byId("commands-output");
  const title = byId("commands-output-title");
  const detail = byId("commands-output-detail");
  const body = byId("commands-output-body");
  const outcome = commandOutcome(result);

  box.hidden = false;
  box.className = "cmd-output cmd-output--" + outcome.kind;
  title.textContent = outcome.label + "：" + ((result && result.name) || "（未命名）");
  const extra = [];
  if (outcome.detail) {
    extra.push(outcome.detail);
  }
  if (result && result.message) {
    extra.push(result.message);
  }
  if (result && result.server_id) {
    extra.push("实际下发到：" + result.server_id);
  }
  if (result && result.cmd) {
    extra.push("指令：" + result.cmd);
  }
  detail.textContent = extra.join(" ");
  const output = (result && result.output) || "";
  body.textContent = output || "（服务器没有返回任何输出）";
  body.hidden = !output;
}

/* ------------------------------------------------------------------ */
/* 分区切换                                                            */
/* ------------------------------------------------------------------ */

function showSection(name) {
  for (const tab of document.querySelectorAll(".tab")) {
    tab.classList.toggle("is-active", tab.dataset.target === name);
  }
  for (const panel of document.querySelectorAll(".panel")) {
    panel.hidden = panel.id !== "section-" + name;
  }
}

/* ------------------------------------------------------------------ */
/* 事件绑定与启动                                                      */
/* ------------------------------------------------------------------ */

/**
 * 迁移按钮的接线——**这一对是「确认不可绕过」的全部落点**：
 *
 *   · 触发器（`bindings-migrate`）→ `openMigrateConfirm`，它**只开对话框**；
 *   · 确认按钮（`bindings-migrate-confirm`，在 `<dialog>` 里）→ `runMigrateConfirm`，
 *     它是 `apiPost("bindings/migrate")` 的**唯一**调用点。
 *
 * 改这里之前先读 `tests/test_panel_frontend.py` 的迁移确认守卫：它按
 * 「下发请求的函数体」「addEventListener 的接线表」「确认按钮在对话框内」
 * 三条静态事实核对，把 `runMigrateConfirm` 接到触发器上会当场变红。
 */
function bindMigrateControls() {
  byId("bindings-migrate").addEventListener("click", openMigrateConfirm);
  byId("bindings-migrate-cancel").addEventListener("click", closeMigrateConfirm);
  byId("bindings-migrate-confirm").addEventListener("click", runMigrateConfirm);
}

function bindEvents() {
  for (const tab of document.querySelectorAll(".tab")) {
    if (tab.disabled) {
      continue;
    }
    tab.addEventListener("click", () => {
      showSection(tab.dataset.target);
    });
  }

  byId("refresh").addEventListener("click", () => {
    loadServers();
    loadKarma();
    loadBindings();
    loadAudit();
    loadCommands();
  });

  byId("karma-search").addEventListener("click", () => {
    state.karmaQuery = byId("karma-query").value.trim();
    loadKarma();
  });

  byId("karma-query").addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      state.karmaQuery = byId("karma-query").value.trim();
      loadKarma();
    }
  });

  byId("karma-reload").addEventListener("click", () => {
    byId("karma-query").value = "";
    state.karmaQuery = "";
    loadKarma();
  });

  byId("karma-set").addEventListener("click", () => {
    const key = byId("karma-new-key").value.trim();
    const value = byId("karma-new-value").value.trim();
    if (!key) {
      setMessage(byId("karma-msg"), "身份键不能为空。", "error");
      return;
    }
    saveKarmaValue(key, value, [byId("karma-set")]);
    byId("karma-new-key").value = "";
    byId("karma-new-value").value = "";
  });

  byId("bindings-search").addEventListener("click", () => {
    state.bindingsQuery = byId("bindings-query").value.trim();
    loadBindings();
  });

  byId("bindings-query").addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      state.bindingsQuery = byId("bindings-query").value.trim();
      loadBindings();
    }
  });

  byId("bindings-reload").addEventListener("click", () => {
    byId("bindings-query").value = "";
    state.bindingsQuery = "";
    loadBindings();
  });

  byId("bindings-rebind-submit").addEventListener("click", rebindBinding);

  byId("audit-search").addEventListener("click", () => {
    state.auditQuery = byId("audit-query").value.trim();
    loadAudit();
  });

  byId("audit-query").addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      state.auditQuery = byId("audit-query").value.trim();
      loadAudit();
    }
  });

  byId("audit-limit").addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      loadAudit();
    }
  });

  byId("audit-reload").addEventListener("click", () => {
    byId("audit-query").value = "";
    state.auditQuery = "";
    loadAudit();
  });

  bindMigrateControls();
}

async function boot() {
  if (!bridge) {
    document.body.textContent =
      "This page must be opened from the AstrBot WebUI (window.AstrBotPluginPage is missing).";
    return;
  }

  let ctx = {};
  try {
    ctx = (await bridge.ready()) || {};
  } catch (error) {
    // bridge 缺席不该让整页白掉：下面的 apiGet 会各自报错，管理员至少看得到界面。
    ctx = {};
  }

  applyTheme(!!ctx.isDark);

  const badge = byId("ctx-badge");
  if (badge) {
    badge.textContent = ctx.pluginName || "NetherLink";
    badge.title = [ctx.displayName, ctx.pageName, ctx.locale]
      .filter(Boolean)
      .join(" / ");
  }
  if (ctx.pageTitle) {
    document.title = ctx.pageTitle;
  }
  const sub = byId("page-sub");
  if (sub) {
    sub.textContent =
      "服务器 " + location.host + " · 身份与好感数据来自本插件运行期状态";
  }

  bindEvents();

  // 单页、开机一次拉齐（不做懒加载，见文件头第 3 条）。
  await Promise.all([
    loadServers(),
    loadKarma(),
    loadBindings(),
    loadAudit(),
    loadCommands(),
  ]);
}

boot();
