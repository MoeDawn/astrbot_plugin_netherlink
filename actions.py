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

    🔴 **为什么要有它**：`click_event: run_command` 能跑的只有 0 权限的
    `trigger <objective> set <整数>`（见 `docs/mc-26.3-spec.md` §1.11），
    而动作参数是任意结构 —— 塞不进一个整数。所以计分板只传 `token`，
    其余全存在这里。
    """

    token: int
    action_name: str
    values: dict
    player: str
    server_id: str
    cost: int
    cmd: str
    deadline: float


def new_token(counter: int) -> int:
    """下一个 token。

    ⚠️ **单调递增、绝不复用**：复用（或随机）都可能让「上一张过期卡的旧 token」
    撞上「新卡」—— 玩家点旧按钮就执行了新请求。递增从根上避免这件事，
    而且可复现（好写测试）。
    """
    return int(counter) + 1


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


def tellraw_escape(text: str) -> str:
    """把值里会破坏 JSON 的字符**转义**（不是替换）。

    与 `main.py` 的 `_tellraw_json_escape` 同一课：真 26.3 实测，**结构双引号
    完全没问题**，只有「值里的裸双引号」会报 `Expected literal ,`；
    把它们换成单引号则会**静默改掉数据**（群名 `A"B` → `A'B`）。
    """
    return (text or "").replace("\\", "\\\\").replace('"', '\\"')


def hint_components_json(components) -> str:
    """把组件映射渲染成 `show_item` 要的 **JSON 对象文本**。

    🔴 实测过两件事（真 26.3）：
      · `show_item` 是**平铺**的 —— `id` / `count` / `components` 与 `action`
        同级，**不能**写 `value`（写成 value 会报 `No key id in MapLike`）
      · `components` 是 **DataComponentPatch**（数据组件映射），
        与 `/give` 的方括号语法**不同形状**
    空组件必须给 `{}` —— 否则 `"components":` 后面什么都没有，JSON 直接坏。
    """
    if not components:
        return "{}"
    return json.dumps(components, ensure_ascii=False, separators=(",", ":"))


def render_confirm_card(template: str, values: dict, *, item_id: str,
                        count: int, cost: int, objective: str,
                        token: int, components=None) -> str:
    """按确认卡模板渲染出 `/tellraw` 的 JSON 文本。

    与 `render_command` 同一口径：**显式替换，不用 `str.format`**。
    """
    vals = dict(values)
    vals.update({
        "item_id": item_id,
        "count": count,
        # ⚠️ `show_item.count` 上限 **99**（源码 `ExtraCodecs.intRange(1, 99)`），
        #    超了**整张卡都发不出去**。所以展示用 count 单独夹一道。
        "hint_count": max(1, min(int(count), 99)),
        "hint_components": hint_components_json(components),
        "cost": cost,
        "objective": objective,
        "confirm_code": token,
    })
    text = template
    for k, v in vals.items():
        text = text.replace("{" + str(k) + "}", str(v))
    left = _RE_PLACEHOLDER.findall(text)
    if left:
        raise ActionError("确认卡模板渲染后仍残留占位符：%s" % ", ".join(left))
    return text


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
        template = str(item.get("template") or "")
        if not template:
            raise ActionError("动作 %r 缺 template" % name)

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

        out.append(Action(
            name=name,
            label=str(item.get("label") or name),
            desc=str(item.get("desc") or ""),
            params=params,
            template=template,
            karma=karma,
            confirm=bool(item.get("confirm", False)),
            done=str(item.get("done") or ""),
        ))
    return out


def load_actions(path) -> list:
    """读 YAML 文件并解析。**文件不存在返回空列表**（不是错误）。

    ⚠️ 用 `yaml.safe_load`：**绝不用 `yaml.load`**（能构造任意 Python 对象，
    而这份文件是可编辑的）。
    """
    import os
    if not path or not os.path.isfile(path):
        return []
    import yaml
    try:
        with open(path, "r", encoding="utf-8") as f:
            doc = yaml.safe_load(f)
    except Exception as e:
        raise ActionError("动作目录 %s 读不出来：%s" % (path, e)) from e
    return parse_actions(doc)


# ---------------------------------------------------------------------------
# 参数校验
# ---------------------------------------------------------------------------

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
    if t == "str":
        s = str(value)
        mx = spec.get("max_len")
        if isinstance(mx, int) and len(s) > mx:
            raise ActionError("参数 %s 太长（%d > %d）" % (pname, len(s), mx))
        return s
    raise ActionError("参数 %s 的类型 %r 不受支持" % (pname, t))


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
    text = action.template
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
