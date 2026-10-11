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


# 确认卡 token 的起点。
#
# ⚠️ 历史：早先确认卡与本地菜单**共用一个 trigger 计分板**，靠数值区间分流
# （菜单码 < 10000 / 确认卡 >= 10000），所以这里必须从 10000 起。
# 2026-10-11 起**菜单与确认卡都改走 Paper 的点击回调**（`ClickEvent.callback`，
# 真人验证可用），计分板整体退役 —— 但 token 起点**保持不变**：已有的
# `_pending_actions` 键空间与测试都按它写，没有理由动。
TOKEN_BASE = 10000


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
        apply_rules = item.get("apply")
        if not template and not apply_rules:
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
        if model not in ("table_then_ai", "fixed", "none"):
            raise ActionError(
                "动作 %r 的 karma.model 只支持 table_then_ai / fixed / none，实际 %r"
                % (name, model)
            )
        if model == "fixed" and not isinstance(karma.get("cost"), int):
            raise ActionError("动作 %r 的 karma.model=fixed 时必须给整数 cost" % name)

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
                if op not in ("check", "clear", "give"):
                    raise ActionError(
                        "动作 %r 的 apply 第 %d 条 op 只能是 check/clear/give，实际 %r"
                        % (name, k + 1, op))
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
_RE_COMPONENTS_OK = re.compile(
    # 允许：字母数字 _ . / : = - , 各种括号与引号 空格 中文
    # 禁止：`;` 与换行 —— 它们是「第二条命令」的常见注入载体
    "^[A-Za-z0-9_./:=\-,\[\]{}()\"' \u4e00-\u9fff]*$"
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
    if not _RE_COMPONENTS_OK.match(s):
        bad = sorted({c for c in s if not _RE_COMPONENTS_OK.match(c)})
        raise ActionError(
            "components 里有不允许的字符 %s。"
            "分号与换行一律禁止（那是命令注入的载体）" % ("、".join(bad) or "?")
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
        entry = {
            "id": normalize_item_id(piece.strip()),
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
_SLOTS = frozenset(
    ["weapon.mainhand", "weapon.offhand", "armor.head", "armor.chest",
     "armor.legs", "armor.feet"]
    + ["hotbar.%d" % i for i in range(9)]
    + ["inventory.%d" % i for i in range(9, 36)]
)


def expand_steps(action: "Action", values: dict) -> list:
    """按 `action.apply` 把动作展开成 MC 可执行的步骤列表。

    每步形如 `{"op": "check"|"clear"|"give", "item": ..., "count": ...,
    "name": ...}`。`name` 只用于 MC 端「材料不足」的私聊文案。

    `apply` 里的 `from` 指向某个**参数名**，其值可以是：
      · `item_list`（解析成多条，每样一条 step）
      · `item_id`（单条）
    """
    rules = action.apply
    if not isinstance(rules, list) or not rules:
        raise ActionError("动作 %r 没有 apply，无法展开成步骤" % action.name)
    steps = []
    checks = []
    for i, rule in enumerate(rules):
        if not isinstance(rule, dict):
            raise ActionError("动作 %r 的第 %d 条 apply 不是映射" % (action.name, i + 1))
        op = str(rule.get("op") or "")
        if op not in ("check", "clear", "give", "modify"):
            raise ActionError(
                "动作 %r 的 apply 里 op 只能是 check/clear/give/modify，实际 %r"
                % (action.name, op))
        src = str(rule.get("from") or "")
        spec = action.params.get(src)
        if spec is None:
            raise ActionError("动作 %r 的 apply 指向了不存在的参数 %r" % (action.name, src))
        val = values.get(src)
        if val in (None, ""):
            continue
        typ = str(spec.get("type") or "str")

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
            items = [{"id": str(val), "count": cnt, "name": ""}]
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
            step = {"op": op, "item": it["id"], "count": it["count"]}
            if it.get("name"):
                step["name"] = it["name"]
            if it.get("components"):
                step["components"] = it["components"]
            # ⚠️ 所有 check 收进前面的桶，保证「全查完才扣」
            (checks if op == "check" else steps).append(step)
    return checks + steps


def build_card(action: "Action", values: dict, cost: int) -> dict:
    """产出给 MC 端渲染卡片的**数据**（不是 tellraw JSON）。

    MC 端用 Adventure 构造可点击组件 —— 它能把 token 捕获进闭包，
    所以**不需要计分板**（见 spec §10.5）。
    """
    spec = action.card if isinstance(action.card, dict) else {}
    title = str(spec.get("title") or action.label or action.name)
    lines = []

    # 物品类参数：按声明顺序，每个 item_list / item_id 出一行
    for pname, pspec in action.params.items():
        val = values.get(pname)
        if val in (None, ""):
            continue
        ptype = str(pspec.get("type") or "")
        if ptype == "item_list":
            items = expand_item_list(val)
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
            items = [{"id": str(val), "count": cnt, "name": ""}]
        else:
            continue
        lines.append({
            # 标签可用 card.<参数名>_label 覆盖，否则用参数名
            "label": str(spec.get(pname + "_label") or pspec.get("label") or pname),
            "items": items,
        })

    # 非物品参数里显眼的也带上（如改名动作的 custom_name）
    for pname, pspec in action.params.items():
        if str(pspec.get("type") or "") not in ("str",) or pname == "player":
            continue
        val = values.get(pname)
        if val:
            lines.append({"label": str(pspec.get("label") or pname), "text": str(val)})

    out = {"title": title, "lines": lines}
    if cost:
        out["cost"] = "{cost} 好感"  # MC 渲染时替换
        out["cost_value"] = cost
    # 按钮 hover（可选，缺省由 MC 用内置文案）
    for k in ("confirm_hover", "cancel_hover"):
        if spec.get(k):
            out[k] = str(spec[k])
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
