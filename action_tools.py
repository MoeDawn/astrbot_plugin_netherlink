# -*- coding: utf-8 -*-
"""把 `actions.yaml` 的条目变成 **LLM 工具**（AstrBot `FunctionTool`）。

与 `actions.py` 的分工：
  · `actions.py` —— 目录解析 / 参数校验 / 渲染 / 定价，**纯函数、不 import astrbot**
  · 本模块 —— 只做「接进 AstrBot」那一层：生成 JSON Schema、构造工具类

## 为什么一个动作一个工具（而不是一个通用壳）

见 design spec §2.1：通用壳 `mc_action(name, params)` 会把参数退化成自由格式，
**丢掉最强的格式约束**。每个动作一个工具时，参数逐个字段声明成 JSON Schema，
AstrBot 会按 schema 校验 —— 这就是「让 AI 一项项填写参数」的落地方式。

## 工具执行契约（读 AstrBot 源码确认）

`astr_agent_tool_exec.call_local_llm_tool`：`method_name == "run"` 时调
`handler(event, *args, **kwargs)`。所以工具类写 `async def run(self, event, **kwargs)`。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from astrbot.api import FunctionTool

# ⚠️ **双路导入**（与 main.py 里那一串同写法）：AstrBot 把插件当**包**时
# `from . import x` 成立；当**顶层模块**时只有 `import x` 能用。
# 写成裸 `import actions` 会让插件整个加载失败（实测：No module named）。
try:
    from . import actions as A
except ImportError:  # 插件以顶层模块方式加载时
    import actions as A

# 参数类型 → JSON Schema 片段。
# ⚠️ 只声明 AstrBot/JSON Schema 认识的关键字：ToolSchema 会拿
# `jsonschema.Draft202012Validator.META_SCHEMA` 校验，多写不认识的关键字会抛。
# ⚠️ 没列出来的类型会走 `{"type": "string"}` 兜底。下面前三个本来就该是
# 字符串（`item_list` 是 `id*数量=名; ...`、`slot_list` 是 `槽位=名; ...`、
# `components` 是 `key=value,key=value`），列出来是为了让这张表**诚实** ——
# 否则读者会以为「没列 = 没支持」，去改兜底反而改坏。
_SCHEMA_TYPE = {
    "player": {"type": "string"},
    "item_id": {"type": "string"},
    "item_list": {"type": "string"},
    "slot_list": {"type": "string"},
    "components": {"type": "string"},
    "int": {"type": "integer"},
    "str": {"type": "string"},
}


def build_parameters(action: A.Action) -> dict:
    """按目录声明生成 JSON Schema。

    两条规则：
      · **有 default 的字段不进 `required`** —— 与校验层的口径一致
        （见 `actions.validate_params` 的注释：`required: true` + `default`
        是自相矛盾的声明）
      · 只有 AstrBot 认得的 JSON Schema 关键字，别的不写
    """
    props: dict = {}
    required: list = []
    for pname, spec in action.params.items():
        if not isinstance(spec, dict):
            continue
        ptype = str(spec.get("type") or "str")
        node = dict(_SCHEMA_TYPE.get(ptype, {"type": "string"}))
        desc = str(spec.get("desc") or "").strip()
        if desc:
            node["description"] = desc
        if ptype == "int":
            if isinstance(spec.get("min"), int):
                node["minimum"] = spec["min"]
            if isinstance(spec.get("max"), int):
                node["maximum"] = spec["max"]
        if ptype == "str" and isinstance(spec.get("max_len"), int):
            node["maxLength"] = spec["max_len"]
        props[pname] = node
        if "default" not in spec and spec.get("required"):
            required.append(pname)
    out: dict = {"type": "object", "properties": props}
    if required:
        out["required"] = required
    return out


def build_description(action: A.Action) -> str:
    """工具描述 = 目录里的 desc + 定价规则（如果有）。

    🔴 **`pricing_rules` 必须进描述**：用户 2026-10-10 的要求 ——
    「需要 ai 自行定价的则把这套规则给 ai 看，让他定价」。表内物品用不到它，
    但 AI 在**报价之前**就得看到规则，所以只能放描述（不是参数说明）。
    """
    parts = [p for p in (action.desc or action.label or action.name,)]
    # 🔴 **价目表必须对 AI 可见**（2026-10-11 评审）：原先 `karma.table` 只存在
    #    于目录文件里，而 `desc` 与 `pricing_rules` 互相指着「上面的表 / 下面的
    #    价目表」—— AI 既不知道哪些物品算「表内」，也没依据判断该不该报 cost。
    #    后果两条：① 表外物品它不报 ⇒ 白多一轮往返；② 表内物品它报了价 ⇒
    #    查表命中、报价被**静默覆盖**，而它已经对玩家说过那个数了。
    #    ⚠️ 放在 desc 与 pricing_rules **之间** —— 后者说的「上面的表」才指得到。
    table = action.karma_table
    if table:
        pairs = "、".join("%s %s" % (str(k).split(":")[-1], v) for k, v in table.items())
        parts.append("已内置价目表(表内物品**不用**报 cost,报了也会被忽略):" + pairs)
    rules = action.pricing_rules.strip()
    if rules:
        parts.append(rules)
    # Top-level universal pricing framework (since version 6) appended **after** this action's rules:
    # this action first explains "how C is computed", the framework then explains "how L/I/R/exceed-tier/discount turn C into T";
    # if reversed, the references won't match up. Also provide the framework even when this action has no rules.
    common = (getattr(action, "pricing_common", "") or "").strip()
    if common:
        parts.append(common)
    return "\n".join(parts)


@dataclass
class ActionTool(FunctionTool):
    """一个由目录条目生成的工具。

    ⚠️ **继承 `FunctionTool` 并用 `@dataclass`**（AstrBot 官方推荐的写法）。
    字段顺序要求：`name` / `description` / `parameters` 是 `ToolSchema` 的无默认
    字段，必须排在最前；自定义字段一律带默认值、排在后面。
    """

    name: str = ""
    description: str = ""
    parameters: dict = field(default_factory=dict)
    # ↓ 自定义字段（都带默认值）
    action: Any = None
    plugin: Any = None

    async def run(self, event, **kwargs):
        """AstrBot 会把 AI 给的参数按 schema 过滤后传进来（`event` 在最前）。

        ⚠️ **不在这里做业务** —— 全部委托给插件的 `_run_action_tool`，
        那样「工具怎么被调」与「动作怎么执行」分开，后者可以在没有 AstrBot
        的环境里单测。
        """
        return await self.plugin._run_action_tool(self.action, event, kwargs)


def make_tool(action: A.Action, plugin) -> ActionTool:
    """把一条目录条目造成可直接 `add_llm_tools` 的工具。"""
    return ActionTool(
        name=action.name,
        description=build_description(action),
        parameters=build_parameters(action),
        action=action,
        plugin=plugin,
    )
