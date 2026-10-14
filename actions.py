# -*- coding: utf-8 -*-
"""结构化动作层：目录加载 + 参数校验 + 命令渲染。

设计见 `docs/superpowers/specs/2026-10-10-structured-actions-and-local-menu-design.md` §2。

**为什么存在**：AI 直接生成格式严格的成品命令时频繁出错（模型没有 MC 26.3 的
训练数据，见 CLAUDE.md 坑 13）。本模块把「命令文本的生成」从 AI 移出 ——
AI 只填参数，模板 + 校验保证格式。

⚠️ 本模块**故意不 import astrbot、不碰网络**：纯函数 + 一次文件读，
便于脱离框架单测（与 `panel.py` 同一取法）。`logger` 由调用方注入或忽略。

## 安全边界（🔴 改动前必读）

参数值会被拼进**以控制台身份执行**的命令字符串。若放任 `;`、换行等字符进入，
就是**命令注入**（等价于拿到服务器 OP）。所以：

* 每种参数类型有**自己的校验规则**，字符集按 **MC 命令解析器的未加引号字符集**
  （`StringReader.readUnquotedString`：字母数字与 `_.+-`）来定 —— 这样拼出来的
  命令**永远不需要加引号**，也就不存在「引号里逃逸」这条路径。
* ⚠️ **不用「加引号包起来」来兜底**：引号内部的转义规则是另一套（`\\` / `\"`），
  多一条路径就多一个出错的地方。宁可严格拒绝。
* 校验是 **fail-closed**：不认识的值一律抛 `ActionError`，绝不「尽量拼一个」。
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Optional

# ---------------------------------------------------------------------------
# 参数类型
# ---------------------------------------------------------------------------

# ⚠️ **按 MC 命令解析器的未加引号字符集**（`StringReader.readUnquotedString`）：
#    字母数字与 `_ . + -`。用这个集合的额外好处是**永远不必加引号**。
#    长度上限 16 来自 `StringUtil.isValidName`（`name.length() > 16` 即非法）。
#    注意它比正版账号的 `[A-Za-z0-9_]` 宽（含 `. + -`），少数离线服会用。
_RE_PLAYER = re.compile(r"^[A-Za-z0-9_.+-]{1,16}$")

# 坐标值：绝对数（含负数/小数）、相对 `~`、局部 `^` 都允许，其余一律拒绝。
# 🔴 **安全关键**：这个值会直接拼进以控制台身份执行的 `tp` 命令，
#    所以用的是**白名单**（与 `_RE_PLAYER` 同一思路）—— 分号（分隔命令）、
#    空格、换行、`@`、`$` 全被挡在外面。
#    允许 `~5` / `^1.5` / `-12.5` / `~` / `100`。
_RE_COORD = re.compile(r"^(?:[~^]-?\d{0,10}(?:\.\d{1,6})?|-?\d{1,10}(?:\.\d{1,6})?)$")


# 物品 ID：`namespace:path` 或裸 `path`（裸的按 minecraft: 归一）。
# namespace 与 path 的字符集来自 `Identifier`（`isValidNamespace` / `isValidPath`）。
_RE_NS = re.compile(r"^[a-z0-9_.-]+$")
_RE_PATH = re.compile(r"^[a-z0-9/._-]+$")

# 模板里允许出现的占位符名（防「模板写了个不存在的字段」静默不替换）。
_RE_PLACEHOLDER = re.compile(r"\{([A-Za-z_][A-Za-z0-9_]*)\}")


@dataclass
class PendingAction:
    """一条「等玩家点确认」的请求。

    MC 端渲染卡片时把 `token` **捕获进点击回调的闭包**，点击后回报
    `action_result{token}`；这里按 token 找回整条请求（含要执行的 steps）。
    """

    token: int
    action_name: str
    values: dict
    player: str
    server_id: str
    cost: int
    cmd: str
    deadline: float
    # 这次动作对好感的**带符号**变化：普通动作是 -cost，送礼（`model: param`）是
    # 玩家该加的值。`_handle_action_result` 用它，不再写死 `-cost`。
    karma_delta: int = 0


# 确认卡 token 的起点。
#
# ⚠️ 历史：早先确认卡与本地菜单**共用一个 trigger 计分板**，靠数值区间分流
# （菜单码 < 10000 / 确认卡 >= 10000），所以这里必须从 10000 起。
# 2026-10-11 起**菜单与确认卡都改走 Paper 的点击回调**（`ClickEvent.callback`，
# 真人验证可用），计分板整体退役 —— 但 token 起点**保持不变**：已有的
# `_pending_actions` 键空间与测试都按它写，没有理由动。
TOKEN_BASE = 10000


# MC 端认识的原子操作 —— **唯一真源**。
# ⚠️ 加一个 op 必须**同时**改 Java 的 `runSteps` / `applyStep`，否则真机上
#    那一步会被当成「未知的步骤类型」静默失败，而本地测试全绿。
#    守卫 `test_step_ops_match_the_mc_side` 钉住两边一致。
STEP_OPS = ("check", "clear", "give", "modify", "command")


def new_token(counter: int) -> int:
    """下一个 token。

    ⚠️ **单调递增、绝不复用**：复用（或随机）都可能让「上一张过期卡的旧 token」
    撞上「新卡」—— 玩家点旧按钮就执行了新请求。递增从根上避免这件事，
    而且可复现（好写测试）。

    起点见 `TOKEN_BASE`。
    """
    return max(int(counter), TOKEN_BASE - 1) + 1


def prune_pending(pending: dict, now: float) -> dict:
    """丢掉已过期的待确认项。返回**新**字典（不改原对象，便于调用方判断变化）。"""
    return {k: v for k, v in pending.items() if v.deadline > now}


def take_pending(pending: dict, player: str, token, now: float):
    """取走**属于该玩家**的待确认项（取走即删除，防重复点击）。

    返回 `(PendingAction | None, 可读的原因)`。

    ⚠️ **必须校验玩家**：卡是私聊发的，但 token 是个整数、可被猜到或传抄。
    只认「发起者本人」能显著缩小面（MC 端的触发词守卫是另一层，不能替代这里）。
    """
    try:
        tk = int(token)
    except (TypeError, ValueError):
        return None, "确认码无效"
    item = pending.get(tk)
    if item is None:
        return None, "确认已失效，请重新发起请求"
    if item.player != player:
        # ⚠️ 别人的确认不当自己的用；也**不删**它（那是他的，还没过期）
        return None, "这不是你的确认请求"
    if item.deadline <= now:
        pending.pop(tk, None)
        return None, "确认已过期，请重新发起请求"
    pending.pop(tk, None)
    return item, ""


class ActionError(Exception):
    """参数或目录有问题。**调用方应当把 str(e) 原样回给 AI**，让它改参数重试。"""


@dataclass
class Action:
    """一条动作目录条目。"""

    name: str                      # 工具名，如 give_item
    label: str = ""                # 给人看的名字
    desc: str = ""                 # 注入工具描述
    params: dict = field(default_factory=dict)
    template: str = ""             # 命令模板，`{参数名}` 占位
    karma: dict = field(default_factory=dict)
    confirm: bool = False          # 是否需要确认卡
    done: str = ""                 # 完成播报模板
    apply: list = field(default_factory=list)   # 展开成 steps 的规则（spec §10）
    card: dict = field(default_factory=dict)    # 卡片版式（由 MC 渲染）
    # `done` 播报里 `{items}` 从哪些参数取物品（默认只取 `item`）。
    # `swap_items` 要设成 `[gain]` —— 播报该说「获得」的那侧，不是他上交的。
    done_items_from: list = field(default_factory=lambda: ["item"])
    # 顶层 `pricing_common` 的文本(version 6 起):通用计价框架(L/I/R/超标三档/
    # 折扣),构建工具描述时拼在本动作 pricing_rules 之后 —— 改一处即全部动作生效.
    pricing_common: str = ""

    @property
    def pricing_rules(self) -> str:
        """给 AI 看的报价规则（注入工具描述）。表内物品用不到它。"""
        return str(self.karma.get("pricing_rules") or "")

    @property
    def karma_table(self) -> dict:
        t = self.karma.get("table")
        return t if isinstance(t, dict) else {}


# ---------------------------------------------------------------------------
# 目录加载
# ---------------------------------------------------------------------------

def doc_version(doc) -> int:
    """读目录的 `version`（缺失/非法按 0，代表「比任何默认都旧」）。"""
    if not isinstance(doc, dict):
        return 0
    try:
        return int(doc.get("version") or 0)
    except (TypeError, ValueError):
        return 0


def load_actions_doc(path):
    """读 YAML 并返回 `(doc, version)`。**文件不存在返回 (None, 0)**。"""
    import os
    if not path or not os.path.isfile(path):
        return None, 0
    import yaml
    try:
        with open(path, "r", encoding="utf-8") as f:
            doc = yaml.safe_load(f)
    except Exception as e:
        raise ActionError("动作目录 %s 读不出来：%s" % (path, e)) from e
    return doc, doc_version(doc)


def parse_actions(doc) -> list:
    """把已解析的 YAML 文档整形成 `[Action]`。**坏目录一律抛 `ActionError`。**

    ⚠️ **不静默跳过坏条目**：一条动作配错了却安静地不注册，症状是「AI 说没有
    这个工具」——与「压根没写这条」长得一样，无从排查。宁可启动就报。
    """
    if not isinstance(doc, dict):
        raise ActionError("动作目录的顶层必须是映射，实际是 %s" % type(doc).__name__)
    raw = doc.get("actions")
    if raw is None:
        return []
    if not isinstance(raw, list):
        raise ActionError("actions 必须是列表，实际是 %s" % type(raw).__name__)

    out = []
    # 顶层通用计价框架(version 6):所有动作共用,构建工具描述时拼接
    _common = str(doc.get("pricing_common") or "").strip()
    seen = set()
    for i, item in enumerate(raw):
        if not isinstance(item, dict):
            raise ActionError("第 %d 条动作不是映射" % (i + 1))
        name = str(item.get("name") or "").strip()
        if not name:
            raise ActionError("第 %d 条动作缺 name" % (i + 1))
        if not re.match(r"^[a-z][a-z0-9_]*$", name):
            # 工具名要能当函数名用，也避免与既有工具（mc_command/mc_karma）撞
            raise ActionError("动作名 %r 非法：只允许小写字母、数字、下划线，且以字母开头" % name)
        if name in seen:
            raise ActionError("动作名 %r 重复" % name)
        seen.add(name)

        # ⚠️ **不能用 `or {}`**：空列表/空串同样是 falsy，`params: []` 会被
        #    静默转成 `{}` 而不报错 —— 实测被守卫抓出来过。
        params = item.get("params")
        if params is None:
            params = {}
        if not isinstance(params, dict):
            raise ActionError("动作 %r 的 params 必须是映射" % name)
        # ⚠️ `template` 与 `apply` **二选一**：
        #   · 老形态：`template` + 一条命令（AstrBot 拼好整串，MC 只执行）
        #   · 新形态：`apply` 展开成多步（check/clear/give，MC 按序执行）
        #   两者都没有 ⇒ 这条动作什么也不做，属于配置错误。
        template = str(item.get("template") or "")
        # ⚠️ `apply` **显式写成空列表** 与「压根没写」含义不同：
        #    · 没写 + 没 template ⇒ 配置错误（什么都不做）
        #    · 显式 `apply: []` + 没 template ⇒ **合法**：「只改好感、不动游戏」
        #      （`refuse_gift` 就是这类 —— 拒收，物品留在玩家背包）
        apply_rules = item.get("apply")
        if not template and apply_rules is None:
            raise ActionError("动作 %r 既没有 template 也没有 apply" % name)
        apply_rules = apply_rules or []

        # 模板里的占位符必须都在 params 里（否则渲染出字面量 `{x}` 发给玩家）
        for ph in _RE_PLACEHOLDER.findall(template):
            if ph not in params:
                raise ActionError(
                    "动作 %r 的模板用了占位符 {%s}，但 params 里没有这一项" % (name, ph)
                )

        karma = item.get("karma") or {}
        if not isinstance(karma, dict):
            raise ActionError("动作 %r 的 karma 必须是映射" % name)
        # ⚠️ **不写 karma 是合法的**，等同于 `model: none`（免费）。
        #    要求每条动作都显式写 karma 只会让目录啰嗦，不增加安全感。
        model = str(karma.get("model") or "none")
        if model not in ("table_then_ai", "fixed", "none", "param"):
            raise ActionError(
                "动作 %r 的 karma.model 只支持 table_then_ai / fixed / none / param，"
                "实际 %r" % (name, model)
            )
        if model == "fixed" and not isinstance(karma.get("cost"), int):
            raise ActionError("动作 %r 的 karma.model=fixed 时必须给整数 cost" % name)
        if model == "param":
            pname = str(karma.get("param") or "")
            if pname not in params:
                raise ActionError(
                    "动作 %r 的 karma.model=param 必须指向一个已声明的参数，实际 %r"
                    % (name, pname))

        # 🔴 **apply 的引用在**解析期**就查** —— 坏目录要在启动时报，
        #    不能等玩家请求了才炸（那症状是「AI 说没这个能力」，与「压根没写」
        #    长得一样，无从排查）。与 params 的占位符校验同一口径。
        if apply_rules:
            if not isinstance(apply_rules, list):
                raise ActionError("动作 %r 的 apply 必须是列表" % name)
            for k, rule in enumerate(apply_rules):
                if not isinstance(rule, dict):
                    raise ActionError("动作 %r 的 apply 第 %d 条不是映射" % (name, k + 1))
                op = str(rule.get("op") or "")
                if op not in STEP_OPS:
                    raise ActionError(
                        "动作 %r 的 apply 第 %d 条 op 只能是 %s，实际 %r"
                        % (name, k + 1, "/".join(STEP_OPS), op))
                # 🔴 `command` 步**无法预校验**（命令成不成只有跑了才知道），
                #    所以它必须**独占** —— 与任何别的步骤共存都会在
                #    「前面扣了材料、它失败」时留下半成品。
                if op == "command" and len(apply_rules) != 1:
                    raise ActionError(
                        "动作 %r 含 command 步骤，它必须独占（不能与别的步骤共存）："
                        "command 无法预校验，混着用会留下半成品状态" % name)
                src = str(rule.get("from") or "")
                if src not in params:
                    raise ActionError(
                        "动作 %r 的 apply 第 %d 条 from=%r，但 params 里没有这一项"
                        % (name, k + 1, src))
                cf = str(rule.get("count_from") or "")
                if cf and cf not in params:
                    raise ActionError(
                        "动作 %r 的 apply 第 %d 条 count_from=%r，但 params 里没有这一项"
                        % (name, k + 1, cf))

        if item.get("confirm") and not (item.get("apply")):
            # ⚠️ 需要确认却没有 apply ⇒ 玩家点确认后无事可做（静默空转）。
            #    这类「配了但没生效」正是本项目最怕的失败模式，宁可启动就报。
            raise ActionError("动作 %r 声明了 confirm 但没有 apply（点确认后无事可做）" % name)

        out.append(Action(
            name=name,
            label=str(item.get("label") or name),
            desc=str(item.get("desc") or ""),
            params=params,
            template=template,
            karma=karma,
            confirm=bool(item.get("confirm", False)),
            done=str(item.get("done") or ""),
            apply=item.get("apply") or [],
            card=item.get("card") or {},
            done_items_from=item.get("done_items_from") or ["item"],
            pricing_common=_common,
        ))
    return out


def load_actions(path) -> list:
    """读 YAML 文件并解析。**文件不存在返回空列表**（不是错误）。

    ⚠️ 用 `yaml.safe_load`：**绝不用 `yaml.load`**（能构造任意 Python 对象，
    而这份文件是可编辑的）。
    """
    doc, _v = load_actions_doc(path)
    if doc is None:
        return []
    return parse_actions(doc)


# ---------------------------------------------------------------------------
# 参数校验
# ---------------------------------------------------------------------------

# 组件块内部的字符集。**允许**的东西：
#   字母数字 _ . / : - = , { } [ ] ( ) " ' 空格 以及中文（名字/lore 里会有）
# **禁止**：`;` 与换行 —— 它们是「第二条命令」的常见注入载体。
# 注意：这里做的是**粗筛**，真正的语法校验靠 MC 的 `createItemStack`
# （造不出来就拒绝），以及 MC 自己的组件解析器（实测拼错会**响亮报错**）。
# ⚠️ 必须是 **raw 字符串**：普通字符串里 `\[` `\]` `\-` `\uXXXX` 都是非法转义（`re` 认它们，Python 字面量不认 → DeprecationWarning）。
# ⚠ 分段拼：raw 段放制表转义，普通段放引号。
#   普通字符串里 `\[` `\]` `\-` 是非法转义（`re` 认，Python 字面量不认 → DeprecationWarning）。
# ⚠ `-` 必须在字符集**首位**，否则会被当成区间端点（`\[-A-Za-z]` 会被读成「从 [ 到 A」）。引号用普通段拼，别放进 raw 段尾。
# ⚠ 字符集里 `-` 必须在**首位**，否则会被读成区间端点。
#   逗号在列（组件的多项分隔符）；引号用普通段拼，别放在 raw 段尾。
# ⚠ **禁止集**，不是允许集。
#   允许集我反复写错（漏逗号、`-` 位置、引号收尾），而禁止集小且明确：
#   分号与换行是「第二条命令」的注入载体；C0 控制字符一律不要。
#   其余内容交给 MC 的 `createItemStack` —— 实测拼错会**响亮报错**
#   （`Unknown item component` / `Not a list` / `Failed to parse`）。
_RE_COMPONENTS_BAD = re.compile(
    "[;\r\n\x00-\x08\x0b\x0c\x0e-\x1f]"
)


def normalize_components(value) -> str:
    """校验 `components` 字符串（组件块**内部**语法，不带外层方括号）。

    真 26.3 实测两种写法都合法，但结果天差地别：

        custom_name={text:"龙鳞",color:"gold"}      ✅ 显示「龙鳞」金色
        custom_name='{"text":"X"}'                  ❌ **静默**变成字面量

    ⇒ 提示词里一律给**组件对象**形式的示例（见 docs/mc-26.3-spec.md 铁律 1）。

    ⚠️ **本函数不做语法解析** —— 组件的语法很复杂（映射/列表/组件对象嵌套）。
        真正的校验有两道：
          ① MC 的组件解析器：拼错会**响亮报错**（实测：`Unknown item component`
             / `Not a list` / `Expected '='`），**不静默**
          ② MC 端 `createItemStack` 试造一次：造不出来就拒绝
        这里只挡住**最危险的一类**：分号与换行（可能透传成第二条命令）。
    """
    s = str(value or "").strip()
    if not s:
        raise ActionError("components 不能为空（要么别传这个参数，要么给内容）")
    if len(s) > 512:
        raise ActionError("components 太长（%d > 512）" % len(s))
    m = _RE_COMPONENTS_BAD.search(s)
    if m:
        raise ActionError(
            "components 里有不允许的字符 %r —— 分号与换行一律禁止"
            "（那是命令注入的载体），控制字符也不行" % (m.group(),)
        )
    return s


def normalize_player(value) -> str:
    """校验并归一玩家名。非法则抛 `ActionError`。"""
    s = str(value or "").strip()
    if not _RE_PLAYER.match(s):
        raise ActionError(
            "玩家名 %r 非法：只允许 1-16 个字母/数字/下划线/点/加号/减号"
            "（与 MC 命令解析器一致，多了会拼出可注入的命令）" % s
        )
    return s


def normalize_coord(value) -> str:
    """校验并归一坐标分量（`x` / `y` / `z` 各调一次）。非法就抛。

    支持绝对数、`~` 相对、`^` 局部 —— 都是原版 `tp` 的合法写法。
    """
    s = str(value if value is not None else "").strip()
    if not _RE_COORD.match(s):
        raise ActionError(
            "坐标 %r 非法：只允许数字（可带小数与负号），或 `~` / `^` 开头的"
            "相对/局部坐标。别的字符一律不行 —— 它会拼进以控制台身份执行的命令"
            % s
        )
    return s


def normalize_item_id(value) -> str:
    """校验并归一物品 ID：接受 `name` 或 `ns:name`，返回**带命名空间**的形式。"""
    s = str(value or "").strip().lower()
    if not s:
        raise ActionError("物品 ID 不能为空")
    if s.count(":") > 1:
        raise ActionError("物品 ID %r 非法：冒号只允许一个" % s)
    if ":" in s:
        ns, path = s.split(":", 1)
    else:
        ns, path = "minecraft", s
    if not _RE_NS.match(ns):
        raise ActionError("物品 ID %r 的命名空间非法" % s)
    if not _RE_PATH.match(path):
        raise ActionError("物品 ID %r 的路径非法（只允许小写字母/数字/下划线/点/斜杠/减号）" % s)
    return "%s:%s" % (ns, path)


def normalize_int(value, lo: Optional[int] = None, hi: Optional[int] = None) -> int:
    """校验整数并夹范围。**不做「尽量转」**：`"3.7"` / `"abc"` 一律拒绝。"""
    if isinstance(value, bool) or value is None:
        raise ActionError("需要一个整数，实际是 %r" % (value,))
    if isinstance(value, int):
        n = value
    elif isinstance(value, str) and re.match(r"^-?\d+$", value.strip()):
        n = int(value.strip())
    else:
        raise ActionError("需要一个整数，实际是 %r" % (value,))
    if lo is not None and n < lo:
        raise ActionError("数值 %d 小于下限 %d" % (n, lo))
    if hi is not None and n > hi:
        raise ActionError("数值 %d 大于上限 %d" % (n, hi))
    return n


def validate_params(action: Action, raw: dict) -> dict:
    """按目录声明校验并归一参数，返回**可直接用于渲染**的字典。

    🔴 规则：
      · 未在 params 里声明的键**直接拒绝**（防 AI 塞多余字段绕过模板）
      · required 缺失 → 报错；optional 缺失 → 用 default，没 default 就不放进结果
      · 每种 type 走自己的归一函数

    抛 `ActionError` 时消息是**给 AI 看的**（要能据此改参数重试），所以写具体。
    """
    if not isinstance(raw, dict):
        raise ActionError("参数必须是一个对象，实际是 %s" % type(raw).__name__)
    out = {}
    unknown = [k for k in raw if k not in action.params]
    if unknown:
        raise ActionError(
            "动作 %r 不认识这些参数：%s（可用：%s）"
            % (action.name, ", ".join(sorted(unknown)), ", ".join(sorted(action.params)))
        )
    for pname, spec in action.params.items():
        if not isinstance(spec, dict):
            raise ActionError("动作 %r 的参数 %s 声明不是映射" % (action.name, pname))
        has = pname in raw and raw[pname] not in (None, "")
        if not has:
            # ⚠️ **有 default 就不算必填**：`required: true` + `default` 一起写
            #    是自相矛盾的声明（既有默认值又强制给），按 default 走，
            #    否则 `count: {required: true, default: 1}` 这种写法永远报缺参。
            if "default" in spec:
                out[pname] = spec["default"]
            elif spec.get("required"):
                raise ActionError("动作 %r 缺少必填参数 %s" % (action.name, pname))
            continue
        out[pname] = _coerce(spec, raw[pname], pname)
    return out


def _coerce(spec: dict, value, pname: str):
    """把单个参数值按类型归一。"""
    t = str(spec.get("type") or "str")
    if t == "player":
        return normalize_player(value)
    if t == "item_id":
        return normalize_item_id(value)
    if t == "int":
        return normalize_int(value, spec.get("min"), spec.get("max"))
    if t == "item_list":
        # 只做**校验**（坏条目立刻报），返回值仍是原串 —— 展开成条目在
        # `expand_item_list` 里做（那时才知道要不要带显示名）。
        expand_item_list(value)
        return str(value).strip()
    if t == "slot_list":
        # 🔴 2026-10-11：这一支原先**漏了** —— `modify_item` 的 `targets`
        #    永远过不了校验，导致该动作从上线起就 100% 不可用。
        #    真机日志里 AI 试了 7 种形状全被拒（其中一种是**对的**），
        #    因为报错说的是「类型不受支持」，它只能去猜数组/字典。
        #    只做**校验**（坏槽位立刻报，报错里列出可用槽位），
        #    返回值仍是原串 —— 展开成条目在 `expand_slot_list` 里做。
        expand_slot_list(value)
        return str(value).strip()
    if t == "coord":
        return normalize_coord(value)
    if t == "components":
        return normalize_components(value)
    if t == "str":
        s = str(value)
        mx = spec.get("max_len")
        if isinstance(mx, int) and len(s) > mx:
            raise ActionError("参数 %s 太长（%d > %d）" % (pname, len(s), mx))
        return s
    raise ActionError("参数 %s 的类型 %r 不受支持" % (pname, t))


# ---------------------------------------------------------------------------
# 展开成 MC 可执行的 steps + 卡片数据（spec §10）
# ---------------------------------------------------------------------------
#
# 🔴 **为什么由 AstrBot 展开、而不是 MC 端读模板**：
#    MC 端只认识 `check` / `clear` / `give` 三种原子操作，**不认识动作名**。
#    动作定义（这条动作要做什么）只有 AstrBot 一份 —— 加新动作**不用改 MC**，
#    也不存在「两处模板必然漂移」那个坑。
#
# 🔴 **`check` 必须排在所有 `clear` 之前**：
#    实测（Paper 26.3 + 真玩家）`clear <玩家> 钻石 5` 在玩家只有 2 个时
#    **会清光 2 个**（不是原子失败）。所以「够不够」必须**先一次性问清**，
#    全通过才开始扣 —— 否则会出现「扣了一半、后面缺料」的半成品状态。

# 允许寻址的槽位（`modify` 的 targets 与 `item_list` 的槽位寻址共用）。
# **故意不含 `contents`** 这类范围槽 —— 那是多格，语义不清；也不含末影箱等
# 需要在别的容器上操作的槽。
_SLOTS = frozenset(
    ["weapon.mainhand", "weapon.offhand", "armor.head", "armor.chest",
     "armor.legs", "armor.feet"]
    + ["hotbar.%d" % i for i in range(9)]
    + ["inventory.%d" % i for i in range(9, 36)]
)

# 供 AI 查询槽位用的原版指令（MC 26.3）。**AI 没有 26.3 的训练数据**，
# 不能让它自己编 —— 所以写进工具描述，让它照着跑。
SLOT_QUERY_HINT = (
    "想知道玩家身上/背包里到底是什么，先**查**，别猜：\n"
    "  · 手上与身上的装备：data get entity <玩家ID> equipment\n"
    "  · 背包某几格：data get entity <玩家ID> Inventory[{Slot:5b}]（只列了 Slot 5 的那格）\n"
    "  · 交互过的方块（末影箱等）：查不到，别猜\n"
    "⚠️ 26.3 的 `/data get` 读**列表/映射**时会省略元素（约 4 个以上就写成 ...），"
    "所以**逐个槽位查**，不要指望一次读出整背包。"
)


def expand_item_list(raw_value) -> list:
    """把 `id*数量=显示名; id*数量=显示名` 解析成 `[{id, count, name}]`。

    格式（用户 2026-10-10 指定）：
      `minecraft:emerald*8=绿宝石原矿; minecraft:echo_shard*2=回响碎片`

    - 条目用 `;` 分隔
    - 每条：`id [*数量] [=显示名] [|组件]`；数量省略 = 1
    - `|` 之后是**组件块内部**语法（不带外层方括号），见 `normalize_components`
    - **任一坏条目 ⇒ 整体抛错**（fail-closed，与其它参数同口径）

    ⚠️ `|` 只做「切出组件段」，组件内容仍交给 `normalize_components` 校验
    （分号与换行在那里被挡下）。
    """
    s = str(raw_value or "").strip()
    if not s:
        raise ActionError("物品列表不能为空")
    out = []
    for part in s.split(";"):
        piece = part.strip()
        if not piece:
            continue
        comps = ""
        if "|" in piece:
            piece, comps = piece.split("|", 1)
            comps = normalize_components(comps.strip())
        name = ""
        if "=" in piece:
            piece, name = piece.split("=", 1)
            name = name.strip()
            if len(name) > 24:
                raise ActionError("显示名太长（%d > 24）：%r" % (len(name), name))
        count = 1
        if "*" in piece:
            piece, cnt = piece.rsplit("*", 1)
            cnt = cnt.strip()
            if not re.match(r"^\d+$", cnt):
                raise ActionError("数量必须是整数：%r" % cnt)
            count = int(cnt)
            if count < 1:
                raise ActionError("数量必须 >= 1：%d" % count)
        # 🔴 **槽位寻址**（2026-10-12 用户要求）：条目可以写成
        #    `weapon.mainhand=旧剑` —— 表示「他身上那一格的东西」。
        #    **为什么需要**：LLM **读不到**玩家物品的组件，只能看到名字；
        #    而它凭对话猜的组件写进 `pay` 后，按组件匹配必然失败
        #    （「升级我手上这把剑」实测必挂）。用槽位寻址就由**服务器**
        #    去读那一格真实的物品，不用猜。
        key = piece.strip()
        if key in _SLOTS:
            entry = {"slot": key, "count": count, "name": name}
        else:
            entry = {
                "id": normalize_item_id(key),
                "count": count,
                "name": name,
            }
        if comps:
            entry["components"] = comps
        out.append(entry)
    if not out:
        raise ActionError("物品列表解析后是空的：%r" % s)
    return out


def expand_slot_list(raw_value) -> list:
    """把 `槽位=显示名; 槽位=显示名` 解析成 `[(slot, name)]`。

    用于 `modify_item` 的批量改（「给身上全套装备改名」）：
        `armor.head=神级头盔; armor.chest=神级胸甲`
    显示名可省（`armor.head`）。槽位必须**在已知清单里** —— fail-closed。
    """
    s = str(raw_value or "").strip()
    if not s:
        raise ActionError("槽位列表不能为空")
    out = []
    for part in s.split(";"):
        piece = part.strip()
        if not piece:
            continue
        name = ""
        if "=" in piece:
            piece, name = piece.split("=", 1)
            name = name.strip()
            if len(name) > 24:
                raise ActionError("显示名太长（%d > 24）：%r" % (len(name), name))
        slot = piece.strip()
        if slot not in _SLOTS:
            raise ActionError(
                "槽位 %r 不认识。可用：%s" % (slot, "、".join(sorted(_SLOTS))))
        out.append((slot, name))
    if not out:
        raise ActionError("槽位列表解析后是空的：%r" % s)
    return out


# 允许 modify 的槽位。**故意不含 `contents` 这类范围槽**（那是多格，
# `setItem` 语义不清），也不含末影箱等需要在别的容器上操作的槽。
def expand_steps(action: "Action", values: dict) -> list:
    """按 `action.apply` 把动作展开成 MC 可执行的步骤列表。

    每步形如 `{"op": "check"|"clear"|"give", "item": ..., "count": ...,
    "name": ...}`。`name` 只用于 MC 端「材料不足」的私聊文案。

    `apply` 里的 `from` 指向某个**参数名**，其值可以是：
      · `item_list`（解析成多条，每样一条 step）
      · `item_id`（单条）
    """
    rules = action.apply
    if not isinstance(rules, list):
        raise ActionError("动作 %r 的 apply 不是列表" % action.name)
    if not rules:
        # ⚠️ **空 apply 是合法的**：只改好感、不动游戏（`refuse_gift`）。
        #    parse_actions 已经区分过「没写」与「显式空」了。
        return []
    steps = []
    checks = []
    for i, rule in enumerate(rules):
        if not isinstance(rule, dict):
            raise ActionError("动作 %r 的第 %d 条 apply 不是映射" % (action.name, i + 1))
        op = str(rule.get("op") or "")
        if op not in STEP_OPS:
            raise ActionError(
                "动作 %r 的 apply 里 op 只能是 %s，实际 %r"
                % (action.name, "/".join(STEP_OPS), op))
        src = str(rule.get("from") or "")
        spec = action.params.get(src)
        if spec is None:
            raise ActionError("动作 %r 的 apply 指向了不存在的参数 %r" % (action.name, src))
        val = values.get(src)
        if val in (None, ""):
            continue
        typ = str(spec.get("type") or "str")

        # ---- command：不引用参数，展开成**动作自己的 template** ----
        # 🔴 **为什么必须有这个 op**：`template` 与 `confirm` 共存在旧写法下会
        #    **静默失灵** —— `apply` 默认成空列表 ⇒ steps 为空 ⇒
        #    `_offer_action_confirm` 走「空 steps 立即结算」的分支，
        #    命令永远不执行。有了这个 op，模板才走得进确认卡那条路。
        #    `from` 必须指向**某个已声明参数**（规则统一的形状），
        #    约定填 `player`；真正的文本来自 `template`。
        if op == "command":
            steps.append({"op": "command", "cmd": render_command(action, values)})
            continue

        # ---- modify：`from` 指向 slot_list（`槽位=显示名`），组件来自另一个参数 ----
        if op == "modify":
            if typ != "slot_list":
                raise ActionError(
                    "动作 %r 的 modify 步骤只能引用 slot_list 参数，实际 %r"
                    % (action.name, typ))
            comps_from = str(rule.get("components_from") or "")
            comps = ""
            if comps_from:
                if comps_from not in action.params:
                    raise ActionError(
                        "动作 %r 的 modify 里 components_from 指向了不存在的参数 %r"
                        % (action.name, comps_from))
                comps = str(values.get(comps_from) or "")
            for slot, name in expand_slot_list(val):
                st = {"op": "modify", "slot": slot, "raw": comps}
                if name:
                    st["name"] = name
                steps.append(st)
            continue
        if typ == "item_list":
            items = expand_item_list(val)
        elif typ == "item_id":
            # 数量可能来自另一个参数（`give_item` 的 item 与 count 是分开的两个参数）
            cnt_from = str(rule.get("count_from") or "")
            if cnt_from:
                if cnt_from not in action.params:
                    raise ActionError(
                        "动作 %r 的 apply 里 count_from 指向了不存在的参数 %r"
                        % (action.name, cnt_from)
                    )
                try:
                    cnt = int(values.get(cnt_from) or 1)
                except (TypeError, ValueError):
                    raise ActionError(
                        "动作 %r 的 %s 不是整数，无法当数量" % (action.name, cnt_from)
                    )
            else:
                try:
                    cnt = int(rule.get("count") or 1)
                except (TypeError, ValueError):
                    raise ActionError(
                        "动作 %r 的 apply 里 count 必须是整数" % action.name
                    )
            entry = {"id": str(val), "count": cnt, "name": ""}
            # 组件来自另一个参数（`give_item` 的 item 与 components 是分开的）
            cf2 = str(rule.get("components_from") or "")
            if cf2:
                if cf2 not in action.params:
                    raise ActionError(
                        "动作 %r 的 apply 里 components_from 指向了不存在的参数 %r"
                        % (action.name, cf2))
                comps2 = str(values.get(cf2) or "").strip()
                if comps2:
                    entry["components"] = comps2
            items = [entry]
        elif typ == "slot_list":
            # 只有 modify 用得上；别的 op 引用它是配置错误
            raise ActionError(
                "动作 %r 的 %s 步骤引用了 slot_list 参数，但只有 modify 能用它"
                % (action.name, op))
        else:
            raise ActionError(
                "动作 %r 的 apply 只能引用 item_list / item_id（或 modify 用 "
                "slot_list），实际是 %r" % (action.name, typ)
            )
        for it in items:
            if "slot" in it:
                # 槽位寻址的条目：**只给槽位**，物品由 MC 端去读
                step = {"op": op, "slot": it["slot"], "count": it["count"]}
                if it.get("name"):
                    step["name"] = it["name"]
                (checks if op == "check" else steps).append(step)
                continue
            step = {"op": op, "item": it["id"], "count": it["count"]}
            if it.get("name"):
                step["name"] = it["name"]
            if it.get("components"):
                step["components"] = it["components"]
            # ⚠️ 所有 check 收进前面的桶，保证「全查完才扣」
            (checks if op == "check" else steps).append(step)
    return checks + steps


def render_done_items(action: "Action", values: dict) -> str:
    """把动作**产出**的物品渲染成一行显示名，供 `done` 播报的 `{items}` 用。

    🔴 **为什么需要它**：原先 `done` 模板只有 `{item}`，那给的是**物品 ID**
    （`minecraft:iron_ingot`）—— 2026-10-11 用户报「公屏播报显示的是物品 id
    而不是显示名」。显示名的来源按优先级：

      1. `display_name` 参数（单物品动作才有，如 give_item）
      2. 物品条目自带的显示名（`item_list` 的 `=显示名` 段）
      3. 物品 ID（兜底）

    ⚠️ 只取**产出**那侧 —— 取哪些参数由 `Action.done_items_from` 决定
    （`swap_items` 设成 `[gain]`，因为 `pay` 是玩家上交的，
    不该出现在「获得」播报里）。
    """
    entries = render_done_item_entries(action, values)
    return " ".join("⌜%s⌝ ×%d" % (e["name"], e["count"]) for e in entries)


def render_done_item_entries(action: "Action", values: dict) -> list:
    """同上的**结构化**版本：`[{id, count, name, components?}]`。

    `render_done_items` 只是把它拼成字符串。拆出来是因为**播报要带 hover**
    （用户 2026-10-11：「同意确认卡片之后发的那条公告也把提到的物品用蓝色
    字体显示，同时添加 hover 显示 show_item 详细信息」）——
    那样 MC 端需要拿到 `id` 与 `components`，光有一行文字办不到。
    """
    out = []
    for pname in (action.done_items_from or ["item"]):
        spec = action.params.get(pname)
        if spec is None:
            continue
        val = values.get(pname)
        if val in (None, ""):
            continue
        ptype = str(spec.get("type") or "")
        if ptype == "item_list":
            shared = str(values.get("display_name") or "").strip()
            for it in expand_item_list(val):
                # ⚠️ 条目可能是**槽位寻址**（只有 `slot`，没有 `id`）——
                #    播报拿不到物品 ID（那是服务器才知道的），用显示名兜底。
                key = it.get("id") or it.get("slot") or "?"
                e = {"count": it["count"], "name": it.get("name") or shared or key}
                if it.get("id"):
                    e["id"] = it["id"]
                if it.get("slot"):
                    e["slot"] = it["slot"]
                if it.get("components"):
                    e["components"] = it["components"]
                out.append(e)
        elif ptype == "item_id":
            # 数量可能来自另一个参数（同 expand_steps 的 count_from）
            cnt = 1
            for rule in action.apply:
                if (isinstance(rule, dict) and rule.get("from") == pname
                        and rule.get("count_from")):
                    try:
                        cnt = int(values.get(rule["count_from"]) or 1)
                    except (TypeError, ValueError):
                        cnt = 1
                    break
            name = str(values.get("display_name") or "").strip() or str(val)
            e = {"id": str(val), "count": cnt, "name": name}
            comps = _components_param_for(action, pname, values)
            if comps:
                e["components"] = comps
            out.append(e)
    return out


def _components_param_for(action: "Action", pname: str, values: dict) -> str:
    """`apply` 里为这个参数声明的 `components_from` 指向哪个参数 —— 取它的值。

    卡片把组件一并交给 MC 端，它据此造**带完整组件的 hover**
    （与玩家在物品栏里悬停所见一致）。

    找不到就返回空串 —— 那种情况卡片只显示裸物品，
    由 MC 端自己想办法（`modify_item` 的 `slot_list` 就是如此：
    它从玩家真实物品上读，见 build_card 里的 `slots`）。
    """
    for rule in action.apply:
        if (isinstance(rule, dict) and rule.get("from") == pname
                and rule.get("components_from")):
            return str(values.get(rule["components_from"]) or "").strip()
    return ""


def build_card(action: "Action", values: dict, cost: int) -> dict:
    """产出给 MC 端渲染卡片的**数据**（不是 tellraw JSON）。

    MC 端用 Adventure 构造可点击组件 —— 它能把 token 捕获进闭包，
    所以**不需要计分板**（见 spec §10.5）。
    """
    spec = action.card if isinstance(action.card, dict) else {}
    title = str(spec.get("title") or action.label or action.name)
    lines = []

    # 🔴 `display_name` 是**物品的显示名**，不是一条独立信息 ——
    #    2026-10-11 真机截图：收下礼物卡片出现了
    #      `[minecraft:diamond] ×2` 紧跟一行 `display_name: 钻石`。
    #    正确做法是拿它当物品的显示名 ⇒ 卡片显示 `[钻石] ×2`。
    #    MC 端已经是这个优先级（`it["name"]` 非空就用它），这边只是没喂。
    shared_name = str(values.get("display_name") or "").strip()

    # 物品类参数：按声明顺序，每个 item_list / item_id 出一行
    for pname, pspec in action.params.items():
        val = values.get(pname)
        if val in (None, ""):
            continue
        ptype = str(pspec.get("type") or "")
        if ptype == "slot_list":
            # 改哪几个槽位（`modify_item` 的 targets）。
            # 🔴 **出两行**（用户 2026-10-12）：改东西时卡片只写一句
            #    「修改物品 + 显示名」看不出改前改后。现在：
            #      · 「当前」—— MC 端按 `slot` 去读玩家**真实**那件
            #        （改的是他身上那件，组件只有服务器知道）
            #      · 「改成」—— 同一件为基底 + LLM 给的组件
            #        （MC 端走 `buildMergedItem`，与真正执行时**同口径**）
            #    两行都是普通 `items` 条目 ⇒ 自动带 `show_item` 悬停。
            #    ⚠️ 条目的 `slot` 是新的取值路径：MC 端据此取基底物品
            #      （有 `components` 就再套上去）。
            pairs = expand_slot_list(val)
            lines.append({
                "label": str(spec.get("current_label") or "当前"),
                "items": [{"slot": s, "name": n or s} for s, n in pairs],
            })
            comps = _components_param_for(action, pname, values)
            if comps:
                lines.append({
                    "label": str(spec.get("modified_label") or "改成"),
                    "items": [{"slot": s, "components": comps,
                               "name": n or s} for s, n in pairs],
                })
            continue
        if ptype == "item_list":
            items = expand_item_list(val)
            # 条目的 `=显示名` 优先；没写的回退共用 display_name
            for it in items:
                if not it.get("name") and shared_name:
                    it["name"] = shared_name
        elif ptype == "item_id":
            # 数量参数（若 apply 里声明了 count_from）—— 卡片要显示真实数量
            cnt = 1
            for rule in action.apply:
                if (isinstance(rule, dict) and rule.get("from") == pname
                        and rule.get("count_from")):
                    try:
                        cnt = int(values.get(rule["count_from"]) or 1)
                    except (TypeError, ValueError):
                        cnt = 1
                    break
            entry = {"id": str(val), "count": cnt, "name": shared_name}
            # 🔴 组件一并给 MC 端 —— 它用 `ItemStack.asHoverEvent()` 造 hover，
            #    于是玩家悬停看到的是**带名字/附魔的那件**，不是裸物品。
            #    （改前只给了 id ⇒ hover 是普通版本，玩家只能从下面的
            #      component 文本栏看出「其实有名字」，用户 2026-10-11 报的正是这个）
            comps = _components_param_for(action, pname, values)
            if comps:
                entry["components"] = comps
            items = [entry]
        else:
            continue
        lines.append({
            # 标签可用 card.<参数名>_label 覆盖，否则用参数名
            "label": str(spec.get(pname + "_label") or pspec.get("label") or pname),
            "items": items,
        })

    # 非物品参数里显眼的也带上（如改名动作的 custom_name）
    # ⚠️ **`display_name` 除外**：它已经当物品显示名用掉了，再加一行就是重复
    # ⚠️ **`components` 也不再出行**（用户 2026-10-11）：hover 现在能看全，
    #    再拿一排 `改成: custom_name={...}` 的原始组件文本是噪声。
    # ⚠️ 标量类型都出一行（`str` / `coord` / `dimension` / `player`）——
    #    2026-10-11 加传送动作时发现：只认 `str` 会让 `coord` 与 `target`
    #    都不显示，**确认卡是空的**。`player` 排除的是主接收者（那是卡片的
    #    沉默前提，不是要确认的信息），`target` 这种**另一个**玩家要显示。
    for pname, pspec in action.params.items():
        if str(pspec.get("type") or "") not in (
                "str", "coord", "dimension", "player") or pname == "player":
            continue
        if pname in ("display_name", "components"):
            continue
        val = values.get(pname)
        if val:
            lines.append({"label": str(pspec.get("label") or pname), "text": str(val)})

    out = {"title": title, "lines": lines}
    if cost:
        out["cost_value"] = cost
    # 🔴 **按钮 hover 里的 `{cost}` 必须在这里替换掉** —— 2026-10-11 真机暴露：
    #    原样发下去，MC 端只是照字面渲染，玩家看到的是 `{cost}` 这四个字符。
    #    （MC 端不做占位符替换 —— 它不认识卡片里的模板语法。）
    for k in ("confirm_hover", "cancel_hover"):
        if spec.get(k):
            out[k] = str(spec[k]).replace("{cost}", str(cost))
    return out


# ---------------------------------------------------------------------------
# 渲染
# ---------------------------------------------------------------------------

def render_command(action: Action, values: dict, extra: Optional[dict] = None) -> str:
    """按模板渲染出**可下发的指令**（不带前导斜杠，与既有 `cmd` 帧一致）。

    ⚠️ 用**显式替换**而不是 `str.format` —— 与 `main.py` 的 `_fill_placeholders`
    同一课：模板里合法地会含花括号（如未来要插 NBT），`format` 会把它当占位符
    并抛 `KeyError`，然后被兜底成**原样返回**。
    """
    vals = dict(values)
    if extra:
        vals.update(extra)
    text = action.template or ""
    if not text:
        # 新形态（apply 展开成 steps）没有模板命令 —— 这不是错误。
        # 调用方（_run_action_tool）把空 cmd 当「无需回执的命令」处理。
        return ""
    for k, v in vals.items():
        text = text.replace("{" + str(k) + "}", str(v))
    left = _RE_PLACEHOLDER.findall(text)
    if left:
        # 不该发生（parse_actions 已查过），但渲染前再兜一道——
        # 漏替换的字面量会原样发给玩家，属于静默故障。
        raise ActionError("动作 %r 渲染后仍残留占位符：%s" % (action.name, ", ".join(left)))
    return text


def resolve_karma(action: Action, values: dict, ai_cost=None) -> int:
    """算出这次该扣多少好感。

    两段式（用户 2026-10-10 定案）：
      · `table_then_ai`：**表里有就用表**（忽略 AI 报价）；表里没有则要 AI 报价
      · `fixed`：写死的值
      · `none`：免费

    ⚠️ 表内物品**忽略 AI 报价**是刻意的：否则 AI 报 0 就白拿。表外才信它。
    """
    model = str(action.karma.get("model") or "none")
    if model == "none":
        return 0
    if model == "param":
        # 它不产生「消耗」，只产生带符号的好感变化 —— 见 resolve_karma_delta。
        return 0
    if model == "fixed":
        n = action.karma.get("cost")
        return n if isinstance(n, int) else 0
    # table_then_ai
    table = action.karma_table
    # 用哪个参数查表：目录里可指定（默认 item）
    key_field = str(action.karma.get("table_key") or "item")
    key = values.get(key_field)
    if key is not None and str(key) in table:
        v = table[str(key)]
        return v if isinstance(v, int) else 0
    if ai_cost is None:
        raise ActionError(
            "物品 %r 不在价目表里，需要你按本工具的说明报一个 cost" % (key,)
        )
    return normalize_int(ai_cost, 0, action.karma.get("max_cost"))

def resolve_karma_delta(action: "Action", values: dict, cost: int) -> int:
    """这次动作对好感的**带符号**变化量。

    · 普通动作：**扣** `cost` ⇒ 返回 `-cost`
    · `karma.model: param`（送礼）：**加**玩家参数里的值 ⇒ 返回 `+delta`

    ⚠️ 与 `resolve_karma` 分开：那个管「这次要收多少」（永远非负），
    这个管「好感往哪边动」。合成一个会让 `param` 那套被迫返回负数，
    而 schema 里 `delta` 的 min/max 是按**玩家语义**写的（加 1~2）。
    """
    model = str(action.karma.get("model") or "none")
    if model != "param":
        return -int(cost)
    pname = str(action.karma.get("param") or "")
    if pname not in values:
        raise ActionError(
            "动作 %r 的 karma.model=param 指向参数 %r，但这次没给值"
            % (action.name, pname))
    try:
        return int(values[pname])
    except (TypeError, ValueError):
        raise ActionError(
            "动作 %r 的参数 %s 不是整数，无法当好感变化量" % (action.name, pname))
