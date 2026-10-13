"""NetherLink 提示词与身份注入的纯函数。

从 main.py 外移（2026-10-13 重构阶段一）。全部是**无状态纯函数**：
不访问 self、不碰网络、不写盘。**行为零变化**。
"""

from __future__ import annotations

import json
import re

from astrbot.api import logger

# 往用户消息追加**逐请求变化**的上下文块（`extra_user_content_parts`）。
# 同样是守卫式导入：旧版 AstrBot 未必有 `astrbot.core.agent.message`。
try:
    from astrbot.core.agent.message import TextPart
except ImportError:  # pragma: no cover - 旧版兜底
    TextPart = None


def _has_injected_context(req) -> bool:
    """这个请求的**用户消息块**里已经注入过 netherlink 身份了吗？

    幂等判据。2026-10-09 起看 `extra_user_content_parts`——动态身份不再走
    system_prompt（那里只放静态前缀，判它会把「另一个插件注过静态段」
    误判成「我们已经注过了」）。
    """
    for part in getattr(req, "extra_user_content_parts", None) or ():
        text = getattr(part, "text", None)
        if isinstance(text, str) and "<netherlink_context>" in text:
            return True
    return False


def _append_ephemeral_user_context(req, text: str) -> None:
    """把一段**逐请求变化**的上下文追加到用户消息末尾（不写进会话历史）。

    🔴 **为什么不塞 system_prompt**：system prompt 是提示缓存的**前缀**，
    逐请求改写它会让缓存每次落空（2026-10-09 上架审核因此驳回）。
    改成 user 消息里的内容块后，前缀稳定、只有尾部变化。

    `.mark_as_temp()` 让框架把它标成 `_no_save`——**不持久化进对话历史**，
    否则每轮都会往历史里堆一份身份块，越积越多。
    （⚠️ 方法名是 `mark_as_temp` 不是 `make_as_temp`，见 message.py:68。）

    ⚠️ 失败一律静默跳过：注入失败不能连累对话本身。
    """
    if not (text or "").strip():
        return  # 空文本是正常情况（模板渲染成空），不必报
    if TextPart is None:
        # 🔴 **不静默**：导入失败时注入等于没做，而调用方会照打「已注入」。
        logger.error(
            "NetherLink: TextPart 不可用，身份注入被跳过 —— "
            "本版 AstrBot 的 astrbot.core.agent.message 路径可能已变"
        )
        return
    parts = getattr(req, "extra_user_content_parts", None)
    if parts is None:
        # 🔴 同上：req 上没有这个字段时追加无处可去。
        logger.error(
            "NetherLink: req 上没有 extra_user_content_parts"
            f"（类型 {type(req).__name__}），身份注入被跳过"
        )
        return
    before = len(parts)
    part = TextPart(text=text)
    mark = getattr(part, "mark_as_temp", None)
    if callable(mark):
        part = mark()
    parts.append(part)
    # ⚠️ **debug**：每轮对话都打，而下面 `inject_qq_identity` 那条已经
    # 报了「发起者是谁、是不是管理员」——同一件事打两遍就是刷屏。
    # 要核对块数/字数时把日志级别调到 debug。
    logger.debug(
        f"NetherLink: 身份块已追加到用户消息（{before} -> {len(parts)} 块，"
        f"{len(text)} 字）"
    )


# 非文本组件 → 占位符。⚠️ **缺失的后果是静默的**：群友发张图，
# 游戏里什么都没发生，发的人以为插件坏了（2026-09-24 用户提出）。
# 按**类型名**匹配（不 import AstrBot 的组件类）——组件类名稳定，
# 且本模块要能在 AstrBot 组件体系变化时退化而不是崩。
_COMPONENT_PLACEHOLDERS = (
    ("image", "[图片]"),
    ("face", "[表情]"),
    ("record", "[语音]"),
    ("video", "[视频]"),
    ("file", "[文件]"),
    ("forward", "[合并转发]"),
    ("json", "[卡片消息]"),
    ("xml", "[卡片消息]"),
    ("reply", "[回复]"),
    ("poke", "[戳一戳]"),
)


def _component_placeholder(comp) -> str:
    """给非文本组件找一个占位符；认不出来就返回空串（保持旧行为）。"""
    name = type(comp).__name__.lower()
    for key, label in _COMPONENT_PLACEHOLDERS:
        if key in name:
            return label
    return ""


def _mc_chain_to_plain(chain) -> str:
    """把消息链压成一行纯文本，供转发进 MC 公屏用。

    MC 公屏放不下图片/文件，所以非文本组件转成 `[图片]` 这类**占位符**——
    早先是**直接跳过**，结果是「群友发了图，游戏里毫无反应」，看着像坏了。

    ⚠️ 未知组件仍然跳过而不是抛错：抛错会让**整条回复**消失，比丢一个占位符严重得多。
    """
    try:
        # ⚠️ **三种入参都要认**，少一种就静默返回空串（表现为「跑了但什么都没发」）：
        #   · `MessageChain`   → `.chain`
        #   · `AstrBotMessage` → `.message`（**不是** `.chain`！字段名不一样）
        #   · 已拆好的组件列表 → 直接用
        if isinstance(chain, (list, tuple)):
            comps = list(chain)
        else:
            comps = None
            for attr in ("chain", "message"):
                got = getattr(chain, attr, None)
                if got is not None:
                    comps = list(got)
                    break
            comps = comps or []
    except Exception:
        return ""
    parts = []
    for comp in comps:
        text = getattr(comp, "text", None)
        if isinstance(text, str) and text:
            parts.append(text)
            continue
        ph = _component_placeholder(comp)
        if ph:
            parts.append(ph)
    return "\n".join(parts).strip()


def render_karma_rules(tpl: str, lo: int, hi: int, initial: int) -> str:
    """把 karma_rules 模板里的范围占位符替换成实际数值。

    支持的占位符：`{karma_min}` / `{karma_max}` / `{karma_initial}`。

    ⚠️ **为什么要有这层间接**：范围自 2026-09-23 起可配，而 `DEFAULT_KARMA_RULES`
    里原本**硬写着**「初始值 20,最低 -50,最高 100」。若把它写死，用户改了
    `karma_max` 之后提示词里的数字就与实际裁剪范围**脱节**——正是此前
    「提示词说 999、代码裁到 100」那个静默失效的成因。改成占位符后，
    插件启动时按实际配置渲染，两者必然一致。

    ⚠️ 抽成**模块级**函数（而不是只做实例方法）是为了让守卫也能算出期望值：
    测试里 `DEFAULT_KARMA_RULES` 是**模板**，而 `plugin.karma_rules` 是**渲染后**
    的文本，直接比对必然不等。守卫用同一个函数渲染即可对齐。

    用户自定义的 karma_rules 若不写占位符，原样返回（不做强制注入）。
    """
    for ph, val in (
        ("{karma_min}", lo),
        ("{karma_max}", hi),
        ("{karma_initial}", initial),
    ):
        tpl = tpl.replace(ph, str(val))
    return tpl


def _render_template(tpl: str, default: str, values: dict, required: tuple, label: str) -> str:
    """渲染注入模板；占位符写坏时回退默认模板并记 warning。

    ⚠️ **不能直接用 `str.format`**：模板里合法地会含 NBT 语法，例如
         data get entity <玩家ID> Inventory[{id:"minecraft:物品ID"}]
    其中 `{id:` 会被 `str.format` 当成格式化占位符并抛 `KeyError: 'id'`，
    于是**整段模板被判坏、静默回退**——用户改了默认值却永远看不到效果
    （2026-09-22 实测到的正是这个）。所以这里只**逐个替换已知占位符**，
    其余 `{...}` 原样保留。

    两道校验缺一不可：
      a. 输入模板必须含齐必需占位符 —— 用户把 `{identity}` 整段删掉时，
         替换阶段不会碰它、结果里也没有残留，只查结果查不出来，
         身份信息会被静默丢掉（正是「AI 认不出管理员」的成因）。
      b. 渲染结果里不得残留必需占位符的字面量 —— 兜住 `{foo}` 这类拼错。
    """
    missing = [ph for ph in required if ph not in tpl]

    def _fill(text: str) -> str:
        for key, val in values.items():
            text = text.replace("{" + key + "}", str(val))
        return text

    text = _fill(tpl)
    if missing or any(ph in text for ph in required):
        logger.warning(
            f"NetherLink: {label} 的模板占位符有误"
            f"（{'漏写 ' + str(missing) if missing else '渲染后仍残留必需占位符'}），"
            f"已回退默认模板。必需占位符：{required}"
        )
        return _fill(default)
    return text


# `§x§R§R§G§G§B§B` 的 RGB 形式必须**先**去掉，否则每个 `§f` 会被当成一个
# 独立码、在正文里剩下 "xff0000" 这种字面量。
_SECTION_RGB_RE = re.compile("§x(?:§[0-9a-fA-F]){6}")
_SECTION_CODE_RE = re.compile("§[0-9a-fk-orA-FK-OR]")


def _strip_section_codes(text: str) -> str:
    """去掉 MC 的 `§` 染色码（含 `§x§R§R§G§G§B§B` 的 RGB 形式）。

    ⚠️ **只在「§ 不会被解释」的场合用**。实测结论（真 Paper 26.3，见 DEVLOG）：
      · `kick` 的 reason 参数与 `/say` 的 text 同属 `MessageArgument`——
        服务端**接受** §（`kick Steve §ehello§bworld` 正常解析，一路走到
        「No player was found」），但**不把它当染色码**：`say §aGREEN` 在
        服务端控制台里原样打出 `§aGREEN`，而同一次控制台**确实**会把它自己
        的组件样式翻成 ANSI 色（那条报错就是 `[38;5;9m`）。也就是说 § 到了
        这里只是一个普通字符，留在踢出界面/公屏上就是字面垃圾。
      · 反过来，`chat` / `bot_reply` 那条路走 Java 的
        `LegacyComponentSerializer.legacySection()`，§ **是**有效的——
        **别**把本函数用到那条路上。
    """
    if not text:
        return text
    return _SECTION_CODE_RE.sub("", _SECTION_RGB_RE.sub("", text))


def _bind_hint_is_json(tpl: str) -> bool:
    """`template_bind_hint` 是 tellraw 的 JSON 数组吗？

    **自动识别**（2026-10-10）：以 `[` 开头**且**能 `json.loads` 成 list
    ⇒ JSON 模式；否则按老规矩当纯文本模板走 `_fmt`。
    这样**存量配置（纯文本）不用迁移**，行为一字不变。

    ⚠️ 只认 `[` 开头：MC 的组件 JSON 几乎都是数组形式（`["", {...}, ...]`）。
    纯文本模板里合法地会含 `[{server}]` 这种方括号，但它过不了 `json.loads`，
    于是正确地落回纯文本路径。
    """
    s = (tpl or "").strip()
    if not s.startswith("["):
        return False
    try:
        return isinstance(json.loads(s), list)
    except Exception:
        return False


def _fill_placeholders(tpl: str, values: dict) -> str:
    """显式逐个替换**已知**占位符，其余 `{...}` 原样保留。

    🔴 JSON 模式**必须**用它、不能用 `_fmt`：`str.format` 会把 `{"text":`
    当成占位符并抛 `KeyError: 'text'`，然后 `_fmt` 兜底成**原样返回**——
    玩家收到的就是字面量 `{group}` `{code}`。这与 `_render_template` 当年
    踩的「NBT 花括号」是同一课。
    """
    text = tpl
    for key, val in values.items():
        if val is None:
            continue
        text = text.replace("{" + str(key) + "}", str(val))
    return text


def _bind_hint_plain_text(rendered: str) -> str:
    """从 tellraw JSON 里抽出纯文本（所有 `text` 节点拼接）。

    踢出原因必须是纯文本：`KickCommand` 的 reason 走 `MessageArgument`
    → `Component.literal(text)`，不解析 JSON，且上限 **256 字符**，
    而完整 JSON 有 600+ 字符 —— 直接塞进 `kick` 会 `TOO_LONG` 报错。

    解析失败**抛 ValueError**：模板本身坏了就该让门禁降级（fail-open 放行），
    而不是发一句 JSON 字面量到玩家脸上。
    """
    try:
        doc = json.loads(rendered)
    except (ValueError, TypeError) as e:
        raise ValueError("template_bind_hint 不是合法 JSON: %s" % e) from e
    if not isinstance(doc, list):
        raise ValueError("template_bind_hint 必须是 JSON 数组")
    out = []
    for node in doc:
        if not isinstance(node, dict):
            continue
        # ⚠️ `_tellraw_only` 标记的节点**只给私聊看**，
        #    踢出界面是纯文本、**没有点击功能**，
        #    把「点击码可复制」带进去就是误导。
        if node.get("_tellraw_only"):
            continue
        txt = node.get("text")
        if isinstance(txt, str):
            out.append(txt)
    return "".join(out)


def _tellraw_json_escape(text: str) -> str:
    """把值里会破坏 JSON 的字符**转义**（不是替换）。

    🔴 **2026-10-10 实测推翻了两条旧认知**（真 26.3，`probe_quotes.py`）：

    | 情形 | 旧认知 | 实测 |
    |---|---|---|
    | JSON **结构**用的双引号 | 「会提前闭合指令」| ✅ **完全没问题**，`tellraw @a {"text":"hi"}` 正常通过 |
    | 值里转义过的 `\\"` | 未知 | ✅ 正常 |
    | **值里的裸双引号** | 「换成单引号」| ❌ 真正的问题在这里：`Expected literal ,` |

    旧实现把 `"` **替换**成 `'` —— 那会**静默改掉玩家的数据**
    （群名 `A"B` 变成 `A'B`），比报错更糟。正确的是**转义**。

    转义顺序：先反斜杠、再引号（反过来会把刚加的转义符再转一遍）。
    """
    return (text or "").replace("\\", "\\\\").replace('"', '\\"')
