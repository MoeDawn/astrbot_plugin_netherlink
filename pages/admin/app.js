/*
 * NetherLink 管理面板（C1：骨架 + 桥接 + 在线服务器 / 好感度两节）。
 *
 * 🔴 三条桥接硬约束（写错都是**静默失败**，官方 plugin-pages.md 核实）：
 *   1. 本文件是**外部 module 文件**（`index.html` 里 `<script type="module" src="./app.js">`）
 *      ——内联脚本无法同步访问 `window.AstrBotPluginPage`。
 *   2. `apiGet` / `apiPost` 的 endpoint 是**插件内相对路径**（`"servers"`）：
 *      **不带插件名前缀**、**不以 `/` 开头**、**不含 query / hash / `\` / scheme /
 *      `.` / `..` 片段**。查询参数走 `apiGet` 的**第二个参数对象**，
 *      绝不拼进 endpoint 字符串。Dashboard 会补成
 *      `/api/v1/plugins/extensions/<插件名>/<endpoint>`。
 *   3. **不做懒加载**（没有动态 `import()`、没有按需 fetch 的资产）：页面资产挂在
 *      有效期 **60 秒**的 JWT asset token 上（`PLUGIN_PAGE_ASSET_TOKEN_TTL_SECONDS = 60`），
 *      页面打开一分钟后再去拉新资产会 401。单页、开机一次拉齐。
 *
 * 返回体口径：bridge 把 `{"status":"ok","data":v}` 解成 `v`，把
 * `{"status":"error",...}` 与 HTTP 失败**变成 rejected promise**。
 * 所以每次调用都必须 try/catch，并把 `error.message` 显示给管理员——
 * 后端 handler 的报错文案（如好感值超出 karma_min~karma_max）正是靠这条路到达眼前。
 */

const bridge = window.AstrBotPluginPage;

/** 页面状态。C2 的绑定/审计/指令三节各自加自己的键。 */
const state = {
  servers: [],
  karma: [],
  karmaQuery: "",
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
    const before = result && result.old === null ? "（无记录）" : result.old;
    setMessage(
      msg,
      "已写入 " + key + "：" + before + " → " + (result ? result.value : value),
      "ok",
    );
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
  await Promise.all([loadServers(), loadKarma()]);
}

boot();
