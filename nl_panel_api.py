# -*- coding: utf-8 -*-
"""管理面板域服务（2026-10-13 重构阶段二，Task 3）。

从 `main.py` 的 `NetherLinkPlugin` 抽出：15 个 `_api_*` 路由 handler
（→ `api_*`）+ `_query_players_one` + `_quick_commands`（→ `query_players_one` /
`quick_commands`），以及它们共用的入参归一函数。插件上留 `self.panel` 作为入口，
且在 `__init__` 里 **Eager 构造**（路由注册点要拿 `self.panel.api_*` 当 handler）。

与 `BindingService(self)` / `KarmaService(self)` 同模式：**持有 plugin 引用**做跨域
操作（`plugin.karma.*` / `plugin.bindings.*` / `plugin._mc_conns` /
`plugin.commands.run_console_cmd` / `plugin.actions.actions_path()` …），自己只拥有「面板」这一层：
入参校验 + 整形调用。整形逻辑全在 `panel.py`（那部分脱离 AstrBot 可单测）。

🔴 **本模块刻意不 import `astrbot.api.web`**：`json_response` / `error_response` /
`request` / `PANEL_AVAILABLE` 一律从**插件所在的模块现取**（见 `PanelApi.__init__`）。
理由：本模块一旦被加载就一直留在 `sys.modules` 里（跨测试文件缓存），import 期定值
会把**第一个世界**的结论带进后来的世界——`tests/test_panel_optional_api.py` 造的
正是一个「没有 `astrbot.api.web`」的世界，它要求 handler 在缺 helper 时**安静降级**
（HANDOFF.md 的要求）。按 import 期缓存走，那边拿到的会是上一个世界里的 helper，
降级分支永远不触发（症状：本该回错误体，却回了正常体）。

⚠️ `_panel_*` 入参归一函数也搬进本模块：它们**只**被这一批 handler 用，留在
`main.py` 就是死代码。`_command_payload` 相反——它是 `@staticmethod`，本模块经
`self.plugin._command_payload(...)` 调，仍留在插件上。

⚠️ **数据根不从本模块取**（与 `nl_binding_svc` 同一条教训）：本模块不调
`get_astrbot_plugin_data_path()`，只经 `plugin._audit_path` / `plugin.actions.actions_path()`
拿路径——测试重定向的是 **main 模块的名字绑定**，本模块自己 import 一份再调用，
在真机上会写进真实数据目录。
"""

from __future__ import annotations

import asyncio
import sys
import time
from datetime import datetime
from pathlib import Path

from astrbot.api import logger

# ⚠️ 与 main.py 里那一串同目录模块**同一个双路导入写法**（坑 14）：
# AstrBot 把插件当**包**导入时 `from . import x` 成立；某些加载路径把它当
# **顶层模块**，那时只有 `import x` 能用。写成裸导入 = 插件**整个加载失败**
# （不是「面板不可用」，而是 QQ↔MC 互通 / 好感度 / 指令执行一起没）。
try:
    from . import actions
    from . import audit
    from . import bindings
    from . import panel
except ImportError:  # 插件以顶层模块方式加载时
    import actions
    import audit
    import bindings
    import panel

# 快捷指令配置项的**唯一**解析入口（阶段一外移到 nl_config.py）——展示与下发
# 两条路由必须看到同一份、同序的列表。
try:
    from .nl_config import _quick_command_entries
except ImportError:  # 插件以顶层模块方式加载时
    from nl_config import _quick_command_entries


# 面板一次最多回多少条审计。显式写出来（而不是依赖 `read_audit` 的默认值），
# 是因为面板要**夹**这个数：`?limit=` 是外部输入，不夹就会被 `<=0` 弄成空表。
PANEL_AUDIT_LIMIT = 100


def _panel_unavailable() -> dict:
    """面板不可用时的兜底返回体。

    ⚠️ 返回**普通 dict**，不依赖缺席的 helper：AstrBot 允许 handler 直接返回
    dict，所以这段在缺 helper 时也成立（换 `error_response(...)` 会在缺 helper
    时再炸一次——那名字恰好是 None）。
    ⚠️ 收成一个函数是为了**只有一份文案**：十三条路由各抄一遍，改一句就会漂，
    而这句要同时被 `tests/test_panel_optional_api.py` 断言。
    """
    return {
        "status": "error",
        "message": "管理面板不可用：当前 AstrBot 没有 astrbot.api.web"
                   "（Plugin Pages 需要较新版本；本插件已在 4.27.4 上验证可用）",
    }


def _panel_karma_key(raw) -> str:
    """归一面板给的 key：必须是**非空字符串**（首尾空白会被去掉）。非法返回空串。

    ⚠️ **为什么去空白**：HTML 输入框里复制粘贴很容易带上首尾空格，而
    `merge_records` 不裁键——「 qq:123」于是变成一条谁也认不出来的新记录，
    面板上表现为「设成功了，但列表里多出一条怪东西」。
    ⚠️ **非字符串一律拒绝**（含 JSON 数字）：键的形态是 `qq:<QQ号>` / `mc:<游戏ID>`，
    一个数字键只能是调用方写错了，悄悄 `str()` 只会把这个错误掩盖过去。
    ⚠️ 这是**面板边界**的归一，不动既有数据：存量里若真有一条带空格的键，
    它仍然原样躺在表里，只是再也别想从面板上改到它。
    """
    if not isinstance(raw, str):
        return ""
    return raw.strip()


def _panel_karma_value(raw, lo: int, hi: int) -> int | None:
    """把面板给的 value 归一成 `[lo, hi]` 内的整数。非法返回 None。

    🔴 **`bool` 必须显式拒绝**：`isinstance(True, int)` 为真、`int(True) == 1`
    —— 不判的话 JSON 的 `true` 会被当成「设成 1」，而面板上什么都看不出来
    （这正是本仓库反复强调的静默失败）。
    ⚠️ **不接受字符串数字**：面板发的是 JSON 数字，`"50"` 说明前端没解析。
    这时回一句明确的错误比替它猜要值钱——猜错了是静默的，报错不是。
    ⚠️ **小数同样拒绝**：好感是整数语义，`50.9` 该由前端取整，
    而不是在这里被 `int()` 悄悄抹平（抹平了面板会显示一个用户没输入过的数字）。
    ⚠️ 范围用调用方给的 `lo` / `hi`（= `plugin.karma_min` / `karma_max`，可配），
    不硬编码 -50~100：那样改了配置面板就开始拒收合法值。
    """
    if isinstance(raw, bool) or not isinstance(raw, int):
        return None
    if not (lo <= raw <= hi):
        return None
    return raw


def _panel_player_name(raw) -> str:
    """归一面板给的游戏 ID：必须是**非空字符串**（去首尾空白）；非法返回空串。

    ⚠️ **大小写原样保留**：MC 上报的是规范拼写（`MoeDawn`），而绑定表以它为键。
    在这里做大小写归一等于造出一个表里不存在的键——绑完当场「没绑上」，面板却
    显示成功。（`admin_mc` 那边大小写不敏感是**匹配**语义，与这里拿它当**键**
    是两回事，别照抄。）
    ⚠️ 只去首尾空白，不碰别的：玩家名里的非 ASCII 字符是合法的。
    """
    if not isinstance(raw, str):
        return ""
    return raw.strip()


def _panel_qq_number(raw) -> str:
    """归一面板给的 QQ 号：必须**纯 ASCII 数字**、长度 5~13；非法返回空串。

    ⚠️ **为什么不满足于「非空字符串」**：QQ 号是纯数字的，一条带字母的绑定
    **永远不会**被 `qq:<QQ号>` 形式的身份键命中，也不会与群消息的 `sender_id`
    对上——它会安静地躺在表里，面板上看着像绑好了，实际谁也认不出来。
    宁可当场拒收，也不要制造一条「看起来成功」的死绑定。
    ⚠️ **长度 5~13**：QQ 号历史上 5 位起，现在最长到 13 位（`binding_flow` 的
    测试数据用的就是 13 位）。这是**面板边界**的一道粗筛，不是权威校验——
    要放宽只改这两个数字。
    ⚠️ **必须 ASCII**：`str.isdigit()` 对全角数字（`１２３４５`）也为真，而全角
    QQ 号与 `sender_id` 永远对不上（与 `_SEPARATORS` 处理全角逗号同一类坑，
    只是这里的正确处理是**拒绝**而不是归一）。
    ⚠️ **拒绝数字类型**（JSON 里发成 `12345`）：面板该发字符串——猜错了是静默
    的（「12345」与 12345 长得一模一样），报错不是。与 `_panel_karma_key` 同一口径。
    """
    if not isinstance(raw, str):
        return ""
    s = raw.strip()
    if not (5 <= len(s) <= 13):
        return ""
    if not all(c in "0123456789" for c in s):
        return ""
    return s


class PanelApi:
    """管理面板 Web API：15 条路由的 handler + 2 个面板内部解析方法。

    ⚠️ 构造顺序要求：插件的 `__init__` 要**在 `self.karma` 设好之后**构造本服务
    （面板 handler 依赖 `plugin.karma.*` / `plugin.bindings.*`）。本服务只存引用、
    不缓存那些状态，所以真正的约束是「路由注册点之前必须已经构造好」。

    ⚠️ **构造期快照（语义与旧代码不同）**：`PANEL_AVAILABLE` / `json_response` /
    `error_response` / `request` 在 `__init__` 里**取一次**就缓存到实例。旧代码是
    **调用期**读 `main` 模块的模块级名字；测试下两者等价，但这里语义收窄了：构造
    之后再 monkeypatch `main` 模块的这四个名字**将不再生效**——要改它们必须在
    构造本服务**之前**打桩。这样做是为避开跨测试文件 `sys.modules` 缓存污染
    （见模块 docstring）。
    """

    def __init__(self, plugin) -> None:
        self.plugin = plugin
        # 🔴 面板的 web 三件套与可用性**从插件所在的模块现取**，不本模块 import：
        # 见模块 docstring。⚠️ 它们是**主模块的模块级名字**（不是插件实例的属性），
        # 所以只能经 `sys.modules` 拿；`type(plugin).__module__` 是本仓库既有写法
        # （见 `tests/test_karma_integration.py` 的 `_cmd_result_cls`）。
        # 取不到时**fail-safe 到「面板不可用」**：handler 返回 `_panel_unavailable()`，
        # 而不是在真机上抛 AttributeError。
        mod = sys.modules.get(type(plugin).__module__)
        self.available = bool(getattr(mod, "PANEL_AVAILABLE", False))
        self.json_response = getattr(mod, "json_response", None)
        self.error_response = getattr(mod, "error_response", None)
        self.request = getattr(mod, "request", None)

    # ------------------------------------------------------------------
    # 入参归一（面板边界）
    # ------------------------------------------------------------------

    def _panel_query(self, name: str, default: str = "") -> str:
        """读一个面板 query 参数（字符串），**任何异常都回退默认值**。

        ⚠️ query 是**外部输入**（地址栏里手打的）。真机的 `request.query.get` 在
        类型转换失败时是**抛异常**的，所以这里整段包住——一个打错的参数不该让
        面板 500。
        ⚠️ `self.request` 在「没有 astrbot.api.web」的世界里是 None，取属性同样抛
        异常——被这里的 except 一起接住，回退默认值。
        """
        try:
            return str(self.request.query.get(name, default) or "")
        except Exception:
            return default

    def _panel_limit(self, maximum: int) -> int:
        """读 `?limit=`，**任何异常/越界都回退 `PANEL_AUDIT_LIMIT`**。

        ⚠️ 真机的 `request.query.get("limit", 100, type=int)` 对 `"abc"` **抛
        `ValueError`**（不是回退默认值）——不接住就是「地址栏打错一个字 → 面板 500」。
        ⚠️ `<= 0` 特别处理：`audit.read_audit(limit <= 0)` 会**直接返回空表**，
        照单全收会让面板「莫名其妙空了」，而这几乎总是笔误，不是「我要看 0 条」。
        ⚠️ 上限夹到调用方给的值：审计文件本身就没那么多条，更大的值没有意义。
        """
        try:
            value = self.request.query.get("limit", PANEL_AUDIT_LIMIT, type=int)
        except Exception:
            return PANEL_AUDIT_LIMIT
        if value is None or value <= 0:
            return PANEL_AUDIT_LIMIT
        return min(int(value), maximum)

    # ------------------------------------------------------------------
    # 管理面板 Web API（AstrBot Plugin Pages）
    # ------------------------------------------------------------------

    async def api_servers(self):
        """GET 在线服务器列表。

        路由注册在 `main.py` 的 `__init__` 末尾。本方法只做「取运行期状态 →
        整形 → JSON」，整形逻辑全在 `panel.servers_view` 里（那部分脱离 AstrBot 可单测）。

        ⚠️ `available` 为假时这条路由**根本不会被注册**，所以下面那段
        兜底是防御性的：handler 是普通绑定方法，别处（测试、将来的内部调用）
        仍可能直接 await 它，那时 `json_response` / `error_response` 都是 None，
        裸调会抛 'NoneType' object is not callable。返回**普通 dict**——
        AstrBot 允许 handler 直接返回 dict，不必依赖缺席的 helper。
        ⚠️ 那段文案收在 `_panel_unavailable()` 里——十五条路由共用一份，
        各抄一遍的话改一句就会漂。下面十五条 handler 的兜底都走它。
        """
        if not self.available:
            return _panel_unavailable()
        try:
            return self.json_response(panel.servers_view(
                self.plugin._mc_conns, self.plugin.transport.server_display,
                time.monotonic()
            ))
        except Exception as e:
            logger.error(f"NetherLink: 面板读取服务器列表失败: {e}")
            return self.error_response("读取服务器列表失败")

    async def api_karma(self):
        """GET 好感度记录（可选 `?q=` 子串过滤，大小写不敏感）。

        整形逻辑全在 `panel.karma_view` 里（脱离 AstrBot 可单测）。

        🔴 **降级态必须 `error_response`，绝不回空列表。** 那两个状态在 JSON
        里长得一模一样（都是"没有记录"），但事实相反：真的空是「他没有记录」，
        降级态是「他的记录我读不出来」。回空列表等于**说假话**，而管理员看到
        「一条都没有」的下一步动作是重建数据——重建的第一步正是把配置项覆写成
        内存里那几条，数据就此消失。写侧三个出口（set / delete / migrate）早已
        各自拒绝，读侧是同一条设计对偶的那一半。

        ⚠️ 前端确实会把这条当失败：框架的 `PluginPagePage.vue` 判
        `response.data?.status === "error"` 即 throw，bridge 转成 rejected
        promise，`loadKarma` 的 catch 显示错误。这是本修法成立的前提。
        ⚠️ 兜底那段的理由同 `api_servers`：禁用态下这条路由根本不会注册，
        但 handler 是普通绑定方法，别处仍可能直接 await 它。
        """
        if not self.available:
            return _panel_unavailable()
        try:
            if self.plugin.karma.degraded:
                return self.error_response(
                    "好感记录处于降级态（配置项里的 karma_records 读不出来），"
                    "无法显示记录——请到 WebUI 检查该项的 JSON 是否合法。"
                    "在你修好之前，面板上的增删改一律被拒绝，以免覆盖掉原文。"
                )
            return self.json_response(
                panel.karma_view(
                    self.plugin.karma.store.snapshot(), self._panel_query("q")
                )
            )
        except Exception as e:
            logger.error(f"NetherLink: 面板读取好感记录失败: {e}")
            return self.error_response("读取好感记录失败")

    async def api_karma_set(self):
        """POST 把某个好感键**设成**目标值。body：`{"key": ..., "value": <int>}`。

        ⚠️ body 走 `await self.request.json(default={})`——`request` 是 **ContextVar
        代理**，不是 handler 形参（handler 只收路径参数，见 plugin-pages.md）。
        ⚠️ 入参**全部在这里校验完再动手**：非法立即 `error_response`，
        绝不让 `karma.set` 去处理一个非数字的 delta，也绝不让异常冒泡给框架。
        ⚠️ 返回体的 `old` / `value` 是**真实的前后值**（`karma.set` 给的），
        不是回显请求——面板要能看出这次到底改没改。
        ⚠️ 目标值恰好等于 `karma_initial` 且原本没有记录时**不会新建记录**
        （见 `KarmaService.set`）：值本来就等于 initial，语义上没有变化。
        🔴 **降级态必须返回 `error_response`**：那一态下 `_save()`（`path=None`）
        与 `KarmaService.sync_to_config` **都不写**，照常回 `{"old": …, "value": …}`
        就是**假成功**——管理员被告知改成了 30，而重启后一个字节都不剩。
        理由与 `api_karma_delete` 的同名分支完全一样：内存表不完整，
        写回会清掉管理员手填的记录。
        ⚠️ 兜底那段的理由同 `api_servers`：禁用态下这条路由根本不会注册，
        但 handler 是普通绑定方法，别处仍可能直接 await 它。
        """
        if not self.available:
            return _panel_unavailable()
        try:
            payload = await self.request.json(default={})
            if not isinstance(payload, dict):
                return self.error_response("请求体必须是 JSON 对象")
            key = _panel_karma_key(payload.get("key"))
            if not key:
                return self.error_response("key 必须是非空字符串")
            value = _panel_karma_value(
                payload.get("value"),
                self.plugin.karma_min, self.plugin.karma_max,
            )
            if value is None:
                return self.error_response(
                    f"value 必须是 {self.plugin.karma_min}~"
                    f"{self.plugin.karma_max} 之间的整数"
                )
            outcome, old, new = await self.plugin.karma.set(key, value)
            if outcome == "degraded":
                return self.error_response(
                    "好感记录处于降级态（内存表没能从配置项播种），拒绝设置——"
                    "写回会清掉管理员手填的 karma_records"
                )
            return self.json_response({"key": key, "old": old, "value": new})
        except Exception as e:
            logger.error(f"NetherLink: 面板设置好感度失败: {e}")
            return self.error_response("设置好感度失败")

    async def api_karma_delete(self):
        """POST 删除某个好感键。body：`{"key": ...}`。

        🔴 **删除不是「设成 0」**：删掉之后 `karma.get` 返回 `karma_initial`
        （默认 20），面板上也不再有这一行。这句写进返回体的 `message`——
        前端与运维都该看到它，否则「删完显示 20」会被当成删除失败。
        ⚠️ 删一个本来就不存在的键**不是错误**（删除是幂等的），返回
        `deleted: false` 的正常响应。把它渲染成红色故障，会让管理员对着一份
        已经过期的面板反复重试。
        ⚠️ 降级态返回 `error_response`：那不是「这次没删掉」，而是**整类操作
        都被拒**（内存表不完整，写回会清掉管理员手填的记录），必须显眼。
        ⚠️ `write_failed`（内存删了、配置项没写成）同样返回 `error_response`：
        那会在重启后**复活**这条记录，报成功就是撒谎。
        """
        if not self.available:
            return _panel_unavailable()
        try:
            payload = await self.request.json(default={})
            if not isinstance(payload, dict):
                return self.error_response("请求体必须是 JSON 对象")
            key = _panel_karma_key(payload.get("key"))
            if not key:
                return self.error_response("key 必须是非空字符串")
            outcome, old = await self.plugin.karma.delete(key)
            if outcome == "degraded":
                return self.error_response(
                    "好感记录处于降级态（内存表没能从配置项播种），拒绝删除——"
                    "写回会清掉管理员手填的 karma_records"
                )
            if outcome == "write_failed":
                return self.error_response(
                    "记录已从内存删除，但写回配置项失败——重启后它会从配置项回来，"
                    "请检查数据目录的权限与磁盘空间"
                )
            if outcome == "missing":
                return self.json_response({
                    "key": key,
                    "deleted": False,
                    "old": None,
                    "message": "该键本来就没有记录，未做任何改动",
                })
            return self.json_response({
                "key": key,
                "deleted": True,
                "old": old,
                "message": (
                    "已删除：记录已从配置项与镜像中移除，此后读取回退到初始值 "
                    f"{self.plugin.karma_initial}（删除不等于设成 0）"
                ),
            })
        except Exception as e:
            logger.error(f"NetherLink: 面板删除好感度失败: {e}")
            return self.error_response("删除好感度失败")

    async def api_bindings(self):
        """GET QQ ↔ 游戏账号绑定表（可选 `?q=`）。

        ⚠️ 读的是**运行期那一份** `plugin.bindings.table`（第二轮定的：全项目只有
        这一份，绑定成功时整体替换）。不要在这里重新 `bindings.load` 一遍——
        那会造出第二个真源，面板显示的可能与实际生效的不是同一份。
        """
        if not self.available:
            return _panel_unavailable()
        try:
            return self.json_response(
                panel.bindings_view(
                    self.plugin.bindings.table, self._panel_query("q")
                )
            )
        except Exception as e:
            logger.error(f"NetherLink: 面板读取绑定表失败: {e}")
            return self.error_response("读取绑定表失败")

    async def api_bindings_rebind(self):
        """POST 改绑：把一个游戏 ID 改到另一个 QQ。body：`{player, qq, qq_name?}`。

        ⚠️ body 走 `await self.request.json(default={})`——`request` 是 **ContextVar
        代理**，不是 handler 形参（handler 只收路径参数，见 plugin-pages.md）。
        ⚠️ 入参**全部在这里校验完再动手**：非法立即 `error_response`，绝不让一个
        带字母的 QQ 号落进绑定表（那是条永远对不上的死绑定）。
        ⚠️ `method` 记成 `"manual"`：绑定表里这一栏就是「怎么绑上的」，人工改绑与
        群内发码（`"code"`）要能分辨。
        ⚠️ `server_id` 传空串：`upsert` 的 `servers` 取**并集**，传空串即保留原有的
        那串「他上过哪些服」——改绑 QQ 不该把这段既成事实抹掉。
        ⚠️ 兜底那段的理由同 `api_servers`：禁用态下这条路由根本不会注册，
        但 handler 是普通绑定方法，别处仍可能直接 await 它。
        """
        if not self.available:
            return _panel_unavailable()
        try:
            payload = await self.request.json(default={})
            if not isinstance(payload, dict):
                return self.error_response("请求体必须是 JSON 对象")
            player = _panel_player_name(payload.get("player"))
            if not player:
                return self.error_response("player 必须是非空字符串")
            qq = _panel_qq_number(payload.get("qq"))
            if not qq:
                return self.error_response("qq 必须是 5~13 位的纯数字 QQ 号")
            qq_name = payload.get("qq_name")
            if qq_name is None:
                qq_name = ""
            if not isinstance(qq_name, str):
                return self.error_response("qq_name 必须是字符串")
            qq_name = qq_name.strip()

            old_table = self.plugin.bindings.table
            new_table = bindings.upsert(
                old_table, player, qq, qq_name, "", "manual",
                datetime.now().isoformat(timespec="seconds"),
            )
            if not self.plugin.bindings.commit_table(new_table, old_table):
                return self.error_response(
                    "绑定表写入失败（数据目录不可写？），改动未生效"
                )
            return self.json_response({
                "player": player,
                "qq": qq,
                "qq_name": qq_name,
                "method": "manual",
                "message": "已改绑：该游戏 ID 现在对应 QQ %s" % qq,
            })
        except Exception as e:
            logger.error(f"NetherLink: 面板改绑失败: {e}")
            return self.error_response("改绑失败")

    async def api_bindings_unbind(self):
        """POST 解绑一个游戏 ID。body：`{player}`。

        ⚠️ 解绑按**游戏 ID** 删。一个 QQ 可以挂多个游戏 ID（规范 §3.1），删掉其中
        一个不该动到兄弟条目——这正是 `bindings.remove` 按键删的语义。
        ⚠️ 解一个本来就没绑的 ID **不是错误**（删除是幂等的），返回 `removed: false`
        的正常响应；渲染成红色故障会让管理员对着一份过期的面板反复重试。
        ⚠️ **不动好感记录**：解绑只改绑定关系，`mc:` / `qq:` 的好感键一个都不碰
        （迁移是另一个按钮的活）。解绑之后这个人的好感仍留在原来的键上，
        只是不再与那个 QQ 共享。
        """
        if not self.available:
            return _panel_unavailable()
        try:
            payload = await self.request.json(default={})
            if not isinstance(payload, dict):
                return self.error_response("请求体必须是 JSON 对象")
            player = _panel_player_name(payload.get("player"))
            if not player:
                return self.error_response("player 必须是非空字符串")

            old_table = self.plugin.bindings.table
            new_table, removed = bindings.remove(old_table, player)
            if removed is None:
                return self.json_response({
                    "player": player,
                    "removed": False,
                    "message": "该游戏 ID 本来就没有绑定，未做任何改动",
                })
            if not self.plugin.bindings.commit_table(new_table, old_table):
                return self.error_response(
                    "绑定表写入失败（数据目录不可写？），改动未生效"
                )
            qq = str(removed.get("qq") or "")
            return self.json_response({
                "player": player,
                "removed": True,
                "qq": qq,
                "message": "已解绑：该游戏 ID 不再关联 QQ %s" % qq,
            })
        except Exception as e:
            logger.error(f"NetherLink: 面板解绑失败: {e}")
            return self.error_response("解绑失败")

    async def api_bindings_migrate(self):
        """POST 触发一次性好感迁移（把已绑定玩家的 `mc:` 并入 `qq:<QQ>`）。

        🔴 这是面板上**唯一会改动玩家数据**的操作（规范 §4.3），所以：
        - 前端**必须**先做二次确认再发这个请求。本路由刻意**不收** `confirm` 之类
          的形式参数——那只是把「确认」变成一个总能被绕过的布尔值，真正该拦人的
          是对话框本身；
        - 返回体里**说清改了哪几条**（`changes`），管理员点完要能核对；
        - **幂等**：迁完 `mc:` 就没了，再点一次返回 `migrated: false` 与
          「无可迁移项」——重复点击不会把好感加两遍。
        - 请求体**根本不读**：这个操作没有参数。一个「可以传点东西进去」的入口
          只会让人以为传点什么能改行为。
        """
        if not self.available:
            return _panel_unavailable()
        try:
            outcome, changes, warnings = await self.plugin.karma.migrate()
            if outcome == "degraded":
                return self.error_response(
                    "好感记录处于降级态（内存表没能从配置项播种），拒绝迁移——"
                    "写回会清掉管理员手填的 karma_records"
                )
            if outcome == "write_failed":
                return self.error_response(
                    "迁移已改内存，但写回配置项失败——重启后会回落到迁移前的状态。"
                    "请检查数据目录的权限与磁盘空间，请勿重复点击，重启后再试"
                )
            if outcome == "nothing":
                return self.json_response({
                    "migrated": False,
                    "changes": [],
                    "message": "无可迁移项：没有已绑定玩家还留着 mc: 记录",
                })
            payload = {
                "migrated": True,
                "changes": changes,
                "message": "已迁移 %d 个 qq: 键（并入 %d 条 mc: 记录）"
                           % (len(changes),
                              sum(len(c["removed"]) for c in changes)),
            }
            if warnings:
                payload["warnings"] = warnings
            return self.json_response(payload)
        except Exception as e:
            logger.error(f"NetherLink: 面板触发好感迁移失败: {e}")
            return self.error_response("好感迁移失败")

    async def api_diagnostics(self):
        """GET 配置**解析之后**的四项：管理员名单 / 绑定群 / 端口绑定。

        这是启动日志里那条「配置解析结果」的可视化版本——排查「我配的东西到底
        生效没有」时，**先看这里**，再看 AI 收到的 system 消息。
        ⚠️ 给的是**解析结果**而不是原始配置项：中文分隔符、大小写、`list` 与
        `string` 两种形态这些坑，全都发生在解析这一步，看原始值看不出来。
        """
        if not self.available:
            return _panel_unavailable()
        try:
            return self.json_response(panel.diagnostics_view(
                self.plugin.admin_mc, self.plugin.admin_qq,
                self.plugin.group_names, self.plugin.ws_bindings,
            ))
        except Exception as e:
            logger.error(f"NetherLink: 面板读取配置诊断失败: {e}")
            return self.error_response("读取配置诊断失败")

    async def api_actions(self):
        """GET 结构化动作列表（含目录错误）。

        🔴 **目录错误必须能看见**：`actions.register_tools` 在目录有坏条目时
        **一个动作都不注册**（宁可全不注册，也不要半套规则）。只记一条 ERROR 的话，
        症状是「AI 说没这个工具」，与「压根没配」一模一样 —— 2026-10-11 真机
        就栽在这：落地的 `actions.yaml` 是旧版（缺 `apply`），新校验判它坏，
        `give_item` / `swap_items` 全没注册，而面板上什么都看不见。
        """
        if not self.available:
            return _panel_unavailable()
        try:
            acts = []
            if self.plugin.enable_actions:
                try:
                    acts = actions.load_actions(str(self.plugin.actions.actions_path()))
                except Exception as e:
                    # 面板这条路径自己读一遍：可能比注册时更糟（用户刚改坏）
                    self.plugin._actions_error = str(e)
            return self.json_response(panel.actions_view(
                acts, self.plugin.enable_actions,
                str(self.plugin.actions.actions_path()),
                self.plugin._actions_error,
            ))
        except Exception as e:
            logger.error(f"NetherLink: 面板读取动作列表失败: {e}")
            return self.error_response("读取动作列表失败")

    async def api_actions_reset(self):
        """POST 把动作目录重置为**插件自带默认**（先备份原文件）。

        🔴 **为什么要有这个按钮**：`actions.ensure_actions_file` **从不覆盖**已存在的
        文件（那是给人改的），所以升级插件后用户目录还是旧版。2026-10-11 真机
        就栽在这：旧版 `give_item` 有 `confirm` 无 `apply`，新校验判它坏 ⇒
        **一个动作都不注册**，而界面上什么都看不见。

        ⚠️ **必须先备份**：用户可能改过那份目录，直接覆盖等于删他的配置。
        备份名带 `.bak`，同目录、可手动找回。
        ⚠️ 重置后**要热重载或重启**才生效（工具注册发生在插件加载期）。
        """
        if not self.available:
            return _panel_unavailable()
        try:
            src = Path(__file__).resolve().parent / "actions.default.yaml"
            if not src.is_file():
                return self.error_response("找不到插件自带的默认动作目录")
            dst = self.plugin.actions.actions_path()
            backup = ""
            if dst.exists():
                backup = str(dst) + ".bak"
                dst.replace(backup)
            dst.parent.mkdir(parents=True, exist_ok=True)
            dst.write_bytes(src.read_bytes())
            logger.info(f"NetherLink: 动作目录已重置为默认 -> {dst}（原文件备份在 {backup or '无'}）")
            # 重置后顺手重读一次，让面板立刻反映新状态
            try:
                acts = actions.load_actions(str(dst))
                self.plugin._actions_error = ""
            except Exception as e:
                acts = []
                self.plugin._actions_error = str(e)
            return self.json_response({
                "ok": True,
                "backup": backup,
                "actions": len(acts),
                "note": "已重置为默认动作目录。需**热重载或重启插件**后才生效。",
            })
        except Exception as e:
            logger.error(f"NetherLink: 重置动作目录失败: {e}")
            return self.error_response("重置动作目录失败: %s" % e)

    async def query_players_one(self, server_id: str) -> dict:
        """对**一台**服务器下发 `list` 并整形出一行结果。

        ⚠️ **逐台问，绝不「随便挑一台」**：`transport.send_to_mc` 在多台在线且未指定目标时
        会拒发（那是刻意的），所以面板必须自己指定 server_id。
        ⚠️ **不扣好感**：`commands.run_console_cmd` 只管执行与等回执，扣费在
        `exec_command_for` 里。面板是管理员功能（规范 §5.3）。
        ⚠️ **拿不到回执不是错误**（超时 / 发送失败）：照常返回一行、`ok` 与
        解析结果都是 `None`，`message` 说明原因——用 `error_response` 会让前端
        把它渲染成红色故障，而「那台服没回话」是常见情形。
        """
        row = {
            "id": str(server_id),
            "display": self.plugin.transport.server_display(server_id),
            "ok": None,
            "count": None,
            "max": None,
            "players": None,
            "output": "",
            "message": "",
        }
        try:
            result = await self.plugin.commands.run_console_cmd(
                "list", timeout=8.0, server_id=server_id
            )
        except Exception as e:
            logger.error(f"NetherLink: 面板向 [{server_id}] 下发 list 失败: {e}")
            row["message"] = "下发 list 失败：%s" % e
            return row
        if result is None:
            row["message"] = "没有拿到回执（超时或发送失败）"
            return row
        row.update(panel.players_view(result.output))
        row["ok"] = bool(result.ok)
        row["output"] = result.output
        if not result.ok:
            row["message"] = "服务器报告这条 list 执行失败"
        return row

    async def api_players(self):
        """GET 在线玩家——对**每一台**在线服务器各下发一次 `list`，逐台列出。

        ⚠️ 本插件**不追踪在线玩家**：join/leave 事件只按模板广播出去，从不累加
        （见 `dispatcher.dispatch_mc_event`）。所以这份数据只能现场问。
        ⚠️ **并发问**（`asyncio.gather`），不是逐台串行：串行时 N 台里只要有一台
        不回话，面板就要卡满 N × 8 秒；并发下总耗时约等于最慢的那一台。
        ⚠️ **没有服务器在线时返回空列表**，不是错误——那是最常见的状态，
        前端渲染成「未连接」即可。
        ⚠️ 每台的 `count` / `max` / `players` **解析不出来就是 `None`**，且
        **原始输出一律带上**：解析只认实测见过的那一种写法（见 `panel.players_view`），
        界面要能把「看不懂的输出」原样交给管理员，而不是显示一个编出来的数字。
        """
        if not self.available:
            return _panel_unavailable()
        try:
            alive = sorted(
                sid for sid, c in self.plugin._mc_conns.items() if not c.closed
            )
            if not alive:
                return self.json_response({"servers": [], "total": None})
            rows = await asyncio.gather(
                *[self.query_players_one(sid) for sid in alive]
            )
            counts = [r["count"] for r in rows if isinstance(r["count"], int)]
            return self.json_response({
                "servers": list(rows),
                # 有任意一台没解析出人数时，合计就是 `None`——宁可不说，
                # 也不给一个只加了一半的数（那会被当事实看）。
                "total": sum(counts) if len(counts) == len(rows) else None,
            })
        except Exception as e:
            logger.error(f"NetherLink: 面板读取在线玩家失败: {e}")
            return self.error_response("读取在线玩家失败")

    async def api_audit(self):
        """GET 指令审计（最近 N 条，**新的在前**；可选 `?q=` 过滤）。

        ⚠️ `?limit=` 经 `_panel_limit` 夹过：非法值回退默认、`<=0` 回退默认
        （`read_audit` 对 `<=0` 直接返回空表，照单全收会让面板莫名清空）。
        ⚠️ 过滤在**整形层**做（`panel.audit_view`），不在这里筛——面板的筛选
        规则要能脱离 AstrBot 单测。
        """
        if not self.available:
            return _panel_unavailable()
        try:
            records = audit.read_audit(
                self.plugin._audit_path,
                limit=self._panel_limit(audit.AUDIT_MAX_ENTRIES),
            )
            return self.json_response(
                panel.audit_view(records, self._panel_query("q"))
            )
        except Exception as e:
            logger.error(f"NetherLink: 面板读取指令审计失败: {e}")
            return self.error_response("读取指令审计失败")

    # ------------------------------------------------------------ B3：快捷指令

    def quick_commands(self) -> list:
        """`quick_commands` 配置项 → 面板按钮列表。

        ⚠️ **唯一的解析入口**：`commands`（展示）与 `commands/run`（下发）必须看到
        同一份、同序的列表——`commands/run` 收到的 index 是按**解析后**的位置算的，
        两条路由各解析一次就会出现「点第 2 个按钮、跑了第 3 条指令」，且不报错。
        """
        return panel.quick_commands_view(
            _quick_command_entries(self.plugin.config.get("quick_commands"))
        )

    async def api_commands(self):
        """GET 快捷指令按钮列表 + **安全说明**。

        ⚠️ `security_note` 是响应的一部分，不是装饰：这些指令不经 AI 判断、不扣
        好感度，而面板本身要登录所以等同管理员权限——说明送到前端，前端才没有
        理由不显示它（文本只有 `panel.QUICK_COMMAND_SECURITY_NOTE` 一份）。
        ⚠️ 兜底那段的理由同 `api_servers`：禁用态下这条路由根本不会注册，
        但 handler 是普通绑定方法，别处（测试、将来的内部调用）仍可能直接 await。
        """
        if not self.available:
            return _panel_unavailable()
        try:
            return self.json_response({
                "commands": self.quick_commands(),
                "security_note": panel.QUICK_COMMAND_SECURITY_NOTE,
            })
        except Exception as e:
            logger.error(f"NetherLink: 面板读取快捷指令失败: {e}")
            return self.error_response("读取快捷指令失败")

    async def api_commands_run(self):
        """POST 执行一条快捷指令。body：`{index}`（`commands` 里那一项的序号）。

        ⚠️ index 是**外部输入**：非整数 / 负数 / 越界一律 `error_response`，且要在
        动任何东西**之前**判掉——绝不把异常冒泡给框架。
        🔴 越界检查必须对**解析后**的列表做（`self.quick_commands()`）：坏条目在
        解析时被跳过、位置前移，拿**原始配置项**的长度当上界会索引越界（被兜底
        except 接住，报成一句与病因无关的失败），或更糟——下发另一条指令。
        ⚠️ `ok` 的三种结局必须分清（既有约定，见 claude.md 坑 4）：`True` /
        `False`（服务端明确回了失败）/ `None`（没拿到回执：未连接、多台在线被拒发、
        或超时）。把 `False` 报成成功正是第一轮修的那个跨端 bug 的形状。
        ⚠️ 指令**不经 AI、不扣好感度**：面板走的是 `commands.run_console_cmd`（只管执行与
        等回执），计价的 `exec_command_for` 那条路这里一步都不碰。
        """
        if not self.available:
            return _panel_unavailable()
        try:
            body = await self.request.json(default={})
            if not isinstance(body, dict):
                return self.error_response("请求体必须是一个 JSON 对象")
            index = body.get("index")
            # ⚠️ bool 是 int 的子类：JSON 的 true 会通过 isinstance(x, int)，进而被
            #    当成「第 1 条」——与 B1 的 value 校验同一个坑。
            if isinstance(index, bool) or not isinstance(index, int):
                return self.error_response("index 必须是整数(面板按钮的序号,从 0 开始)")
            commands = self.quick_commands()
            if index < 0 or index >= len(commands):
                return self.error_response(
                    "指令序号越界:当前共 %d 条快捷指令(可用序号 0~%d)"
                    % (len(commands), len(commands) - 1)
                )
            item = commands[index]

            server_id = ""
            if item["server"]:
                server_id = self.plugin._resolve_target_server(item["server"])
                if not server_id:
                    # 🔴 配了目标服但它不在线：**不许**退化成「发给唯一在线的那台」
                    #    ——那会把指令发到另一台服务器上，而面板还报成功。
                    #    （`_resolve_target_server` 只认在线的那几台，所以这里
                    #    分不出「名字写错」与「没在线」，如实报「不在线」。）
                    return self.json_response(self.plugin._command_payload(
                        item, "", False, None, "",
                        "配置里的目标服务器 %s 不在线" % item["server"],
                    ))

            result = await self.plugin.commands.run_console_cmd(
                item["cmd"], timeout=8.0, server_id=server_id
            )
            if result is None:
                # 未连接 / 多台在线被拒发 / 超时——**不是错误**，同 `api_players`：
                # 问不到是常态，用 error_response 会让前端渲染成一片红色故障。
                return self.json_response(self.plugin._command_payload(
                    item, server_id, False, None, "",
                    "MC 服务器未连接、未回执,或在线服务器不止一台"
                    "(未指定服务器时插件会拒发)",
                ))
            return self.json_response(self.plugin._command_payload(
                item, server_id, True, bool(result.ok), result.output, "",
            ))
        except Exception as e:
            logger.error(f"NetherLink: 面板执行快捷指令失败: {e}")
            return self.error_response("执行快捷指令失败")
