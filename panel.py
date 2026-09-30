# -*- coding: utf-8 -*-
"""管理面板的**数据整形层**。

本模块只做一件事：把插件的**运行时状态**整形成能直接 `json_response` 出去的形状。
它**不 import web、不 import 插件**，所以能脱离 AstrBot 单测。

⚠️ 路由前缀必须是**插件名**：dashboard 拿注册时的 route **原样**与
`/api/v1/plugins/extensions/<插件>/<子路径>` 比对（源码是正则，不是 FastAPI）。
不带前缀的路由**永远匹配不上**，而且不报错——只表现为 404。

⚠️ **本模块不产出 `online_seconds`**：`McConn` 上只有 `ws` / `port` /
`reported_name` 三个字段，**没有连接时间戳**，算出不来。`servers_view` 的
`now` 形参因此**当前未被使用**——保留它是为了不改动既定的调用/测试签名
（面板要显示在线时长，得先在 MC 端上报连接时刻，那是另一件事）。
"""

import logging
import re


logger = logging.getLogger(__name__)


# 🔴 必须与 metadata.yaml 的 `name` 一致。
ROUTE_PREFIX = "astrbot_plugin_netherlink"


def route(sub: str) -> str:
    """拼一条 API 路由。**全项目只此一处**拼路由，免得前缀写漏。"""
    return "/%s/%s" % (ROUTE_PREFIX, str(sub).lstrip("/"))


def servers_view(conns: dict, display_of, now: float) -> list:
    """在线服务器列表。

    ⚠️ 只列**未关闭**的连接：`_mc_conns` 里可能残留已关闭的条目，
    面板按「在不在线」展示，关掉的一律不算在线。
    ⚠️ `display` 走 `_mc_server_display`（配置的显示名），**不是** MC 上报名
    ——上报名只作标识，显示名一律以插件配置为准（既有约定）。
    ⚠️ `now` 目前未使用，见模块 docstring（没有连接时间戳，算不出在线时长）。
    """
    out = []
    for sid, conn in sorted(conns.items()):
        if getattr(conn, "closed", True):
            continue
        out.append({
            "id": str(sid),
            "display": str(display_of(sid)),
            "port": getattr(conn, "port", 0),
            "reported_name": str(getattr(conn, "reported_name", "") or ""),
        })
    return out


# ---------------------------------------------------------------------------
# 只读面的整形函数
# ---------------------------------------------------------------------------
#
# 共同的约定（四个函数一致，别各写各的）：
#   · 过滤一律**大小写不敏感**的子串匹配（`query.strip().lower()` 后比）——
#     面板是给人用的，没人会照着大小写去搜；空串/纯空白 = 不过滤。
#   · 排序一律**朴素的字符串序**（`sorted(..., key=str)`），不做「数字感知」的
#     自然序。这里排的是身份键（`qq:123` / `mc:Steve`）与玩家名，没有数值语义；
#     要数字序得先定义「哪一段是数字」，那是另一种东西，不该偷偷塞进来。
#   · **坏条目逐条跳过，绝不抛异常**——面板是只读的观测工具，为一条脏数据整个
#     面白掉，比少显示一条糟糕得多。


def _matches(query: str, fields) -> bool:
    """`query` 是否命中 `fields` 里任意一项（大小写不敏感子串）。"""
    return any(query in str(f or "").lower() for f in fields)


def _norm_query(query) -> str:
    """归一 query 参数：None / 非字符串都当空串。**调用方传进来的还没 strip**。"""
    return str(query or "").strip().lower()


def karma_view(records: dict, query: str = "") -> list:
    """好感记录 → `[{"key", "value"}]`，按 key 排序。

    ⚠️ **值原样输出**（`value`），不转换、不四舍五入、不丢非数字——好感值的权威
    来源是配置项 `karma_records`，而**那一项是允许管理员在 WebUI 手改的**。
    手改出一个非数字时，面板应当把它**显示出来**：静默吞掉会让管理员以为记录
    是好的，替它编一个 0 则更糟（凭空多出一条他从未写过的数字）。
    ⚠️ key 一律 `str()`：JSON 对象的键必须是字符串，而配置项手改出来的键未必是。
    ⚠️ 排序用 `key=str` 而不是直接 `sorted(records)`：键类型不齐（手改出来的）
    时后者会抛 `TypeError`，一条脏数据就能让整个面板白掉。
    """
    q = _norm_query(query)
    out = []
    for key in sorted(records, key=str):
        k = str(key)
        if q and q not in k.lower():
            continue
        out.append({"key": k, "value": records[key]})
    return out


def bindings_view(table: dict, query: str = "") -> list:
    """绑定表 → `[{"player", "qq", "qq_name", "servers", "bound_at", "method"}]`。

    ⚠️ 字段归一化口径**与 `bindings.load` 一致**（缺字段按空值补、`servers` 不是
    列表就当空表）——运行期的 `_binding_table` 本来就是 `load` 的产物，两边口径
    不同会让「面板看到的」与「实际生效的」悄悄分叉。
    ⚠️ **非字典的值直接跳过**（`bindings.load` 同样逐条丢弃），不让一条脏数据
    把整张表打成 500。
    ⚠️ 过滤只匹配**身份字段**（player / qq / qq_name / servers）：
    `bound_at` 是时间戳、`method` 是内部枚举，拿它们搜会让「9」「auto」这类
    输入命中一大片，对管理员没有意义。
    """
    q = _norm_query(query)
    out = []
    for player in sorted(table, key=str):
        rec = table[player]
        if not isinstance(rec, dict):
            continue
        servers = rec.get("servers")
        row = {
            "player": str(player),
            "qq": str(rec.get("qq") or "").strip(),
            "qq_name": str(rec.get("qq_name") or ""),
            "servers": [str(s) for s in servers] if isinstance(servers, list) else [],
            "bound_at": str(rec.get("bound_at") or ""),
            "method": str(rec.get("method") or ""),
        }
        if q and not _matches(
            q, [row["player"], row["qq"], row["qq_name"]] + row["servers"]
        ):
            continue
        out.append(row)
    return out


def diagnostics_view(admin_mc, admin_qq, group_names, ws_bindings) -> dict:
    """配置诊断：**解析之后**的四项，用来回答「我配的东西到底被读成了什么」。

    这正是启动日志那条「配置解析结果」的可视化版本——排查配置不生效时，
    先看这里，再看 AI 收到的 system 消息。

    ⚠️ `admin_mc` / `admin_qq` 在插件里是 **set**：`json_response` 序列化不了，
    而且 set 的迭代顺序不确定，面板上会「刷新一次换个序」。这里统一成**排序好的
    列表**。
    ⚠️ `ws_bindings` 原样带过（`[(server_id, port), ...]`）——它已经是
    `_parse_ws_ports` 的产物，重排只会引入新的出错点。
    ⚠️ 只报这四项。**不顺手加**别的东西（如「面板是否可用」）：这个形状是面板与
    诊断页的接口，多出来的字段没人消费，却会被当成契约的一部分。
    """
    return {
        "admin_mc": sorted(str(x) for x in (admin_mc or ())),
        "admin_qq": sorted(str(x) for x in (admin_qq or ())),
        "group_names": {str(k): str(v) for k, v in dict(group_names or {}).items()},
        "ws_bindings": list(ws_bindings or []),
    }


def audit_view(records: list, query: str = "") -> list:
    """指令审计记录 → 过滤后的列表（**保持入参顺序**）。

    ⚠️ **不重排**：`audit.read_audit` 给的已经是「新的在前」，那是它承诺的语义；
    整形层再排一次就是第二个真源。
    ⚠️ 过滤只看 `initiator` / `cmd` / `event` 三个字段——它们回答的是管理员会问的
    问题（「谁干的」「干了什么」「成了没有」）。`note` 是自由文本，不参与。
    ⚠️ 非字典条目跳过（`read_audit` 只放 dict 进来，但本函数是公开的整形层，
    别的调用方未必守这条）。
    ⚠️ 返回的是**浅拷贝**：面板只做序列化，但调用方就地改一条不该改到审计数组。
    """
    q = _norm_query(query)
    out = []
    for rec in records or []:
        if not isinstance(rec, dict):
            continue
        if q and not _matches(
            q, [rec.get("initiator"), rec.get("cmd"), rec.get("event")]
        ):
            continue
        out.append(dict(rec))
    return out


# `list` 指令回执里那一行的判据。
#
# 🔴 这条正则的**每一段都是实测出来的**，不是照直觉写的：
#   · 串本身来自真 Paper 26.3 服务端语言文件
#     `assets/minecraft/lang/en_us.json`：
#         commands.list.players = "There are %s of a max of %s players online: %s"
#     服务端指令反馈固定走 en_us（与客户端语言无关），所以这一段是稳定的。
#   · 实测回执（真 Paper 26.3 + 真本插件，A2 取证）：
#         'There are 0 of a max of 5 players online: \n'
#     ⚠️ **结尾带一个 `\n`**（捕获侧拼出来的），所以**不能锚定 `$`**。
#   · **不匹配行尾的玩家名**：有玩家时名字跟在**同一行**（`", "` 连接）——
#     依据是 `ListPlayersCommand` 的字节码，它把整个列表交给
#     `ComponentUtils.formatList(players, DEFAULT_SEPARATOR)`（`", "`）拼成
#     **一个** Component 再套进那条翻译。所以只取「有几个 / 上限多少」，
#     名字留原始输出给管理员自己看。
#   · 用 `search` 而不是 `match`：真实回执可能带前缀/后缀（ANSI、换行）。
_LIST_LINE_RE = re.compile(r"There are (\d+) of a max of (\d+) players online")

# 控制台渲染是带 ANSI 色序列的（实测服务端日志里就有 SGR 序列）。
# ⚠️ 目前**捕获路径**（Paper / Fabric / NeoForge 三端的 command_result）拿到的
# 都是纯文本，不带 ANSI——这一段是**保险**，防的是将来某端改成直接转控制台输出。
# 只剥 SGR（`ESC [ ... m`），不碰别的转义序列（剥多了反而会改变有意义的字节）。
_ANSI_SGR_RE = re.compile(r"\x1b\[[0-9;]*m")


def players_view(output) -> dict:
    """`list` 指令的原始回执 → `{"count", "max"}`。

    🔴 **解析不出来就是 `None`，绝不猜**。在线人数是会被人当**事实**看的数字，
    猜错的代价远高于显示「无法解析」——原始输出就在同一个 payload 里，
    管理员自己能看到那一行。同理，本函数**只认**那个实测见过的模式：
    换一种写法（比如将来服务端改了措辞）会走到 `None`，而不是匹配出半个数字。

    ⚠️ `output` 为 `None`（服务器没回执）也走这条路，不抛异常。
    """
    text = _ANSI_SGR_RE.sub("", str(output or ""))
    m = _LIST_LINE_RE.search(text)
    if not m:
        return {"count": None, "max": None}
    return {"count": int(m.group(1)), "max": int(m.group(2))}


# ---------------------------------------------------------------------------
# B3：快捷指令（面板按钮）
# ---------------------------------------------------------------------------
#
# 🔴 安全边界**只此一份**：`main.py` 的 `_api_commands` 把它原样放进 GET 的返回体，
# 前端因此没有理由不显示它。
# 依据：AstrBot 的 `dashboard/api/plugins.py` 里，`/plugins/extensions/` 的**每一个**
# 端点都声明了 `auth: AuthContext = Depends(require_plugin_scope)`，而
# `require_plugin_scope = ScopeDependency("plugin")`——面板本身就要登录，
# 所以能从这里点出去的指令等同管理员权限（规范 §5.3）。
# ⚠️ 措辞必须点明它**不是**「绕过好感度的后门」：好感度约束的是 **AI 替人执行指令**
# 时的计价，而这里的按钮是管理员**本人**动手，两条路径不同。
QUICK_COMMAND_SECURITY_NOTE = (
    "安全说明：快捷指令由管理员在面板上直接下发，不经 AI 判断、也不消耗好感度。"
    "面板需要登录（AstrBot 的插件扩展接口统一要求 plugin 权限范围），"
    "所以这等同管理员权限。它不是绕过好感度的后门——"
    "好感度管的是 AI 替人执行指令时的计价，而这里的按钮是管理员本人动手，"
    "与 AI 那条路径无关。"
)


def quick_commands_view(entries) -> list:
    """`quick_commands` 配置项 → `[{"index", "name", "cmd", "server"}]`。

    ⚠️ 入参是**已经过 `NetherLinkPlugin._as_str_list` 归一**的字符串列表
    （全角逗号 / 顿号那些归它管，本模块不重复实现一份，免得两处漂移）。
    配置项形态是 `名称|指令|服务器`，**一条记录塞在一个字符串里**——AstrBot 的
    `type: "list"` 没有对象列表控件，全项目的列表项都是纯字符串（8 项无一例外）。

    🔴 **分隔符是竖线，不是冒号**：MC 指令里到处都是冒号（`minecraft:diamond`、
    NBT 的 `{id:"..."}`），照 `_parse_ws_ports` 那样 `rpartition(":")` 会把一条
    指令劈成两半。代价是**指令里不能含竖线**——这条限制如实写在 schema 的 hint
    里，不假装支持。

    ⚠️ 坏条目**跳过并记 warning，绝不抛异常**（同 `_parse_ws_ports` 的口径）：
    配置来自 WebUI 手填，一条写错不该让整个面板白掉。
    ⚠️ `index` 是**解析后**的位置，不是原始配置项里的位置——坏条目被跳过后位置
    会前移。面板按下标下发（`commands/run`），序号若沿用原始位置，跳过一条就会让
    后面**每一条按钮都指向别人的指令**。
    """
    if isinstance(entries, str):
        # 防御性：契约是列表，但单串传进来时按一条条目处理，而不是逐字符遍历
        entries = [entries]
    out = []
    for raw in entries or ():
        text = str(raw).replace("｜", "|").strip()
        if not text:
            continue
        parts = [p.strip() for p in text.split("|")]
        if len(parts) > 3:
            logger.warning(
                "NetherLink: quick_commands 条目里的竖线过多（竖线是分隔符，"
                "指令里不能含它），已跳过: %r" % (text,)
            )
            continue
        name = parts[0]
        cmd = parts[1] if len(parts) > 1 else ""
        server = parts[2] if len(parts) > 2 else ""
        if not name or not cmd:
            logger.warning(
                "NetherLink: quick_commands 条目缺少名称或指令，已跳过: %r" % (text,)
            )
            continue
        out.append({"index": len(out), "name": name, "cmd": cmd, "server": server})
    return out
