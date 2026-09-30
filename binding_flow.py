# -*- coding: utf-8 -*-
"""绑定流程的纯逻辑：门禁判据、验证码、迁移规划。

**为什么单独成模块**：这三件事都有「输入→输出」的确定语义，且都**不该**碰
插件实例（也就没有 aiohttp/astrbot 依赖），所以能直接单测。`main.py` 只负责
把它们接到事件流上。

⚠️ **时间与随机数都是参数**，不在这里调 `datetime.now()` / `random`——
否则 5 分钟有效期与「重发覆盖」只能靠 sleep 测。
"""

import string

# 验证码：6 位（5 位在几百人群里碰撞概率不低，6 位成本一样）
CODE_LENGTH = 6
# 有效期 5 分钟；过期重新进服即可
CODE_TTL_SECONDS = 300

# 码表用**大写字母 + 数字**，实测 34 个字符。
# ⚠️ 排除 `O` 与 `I`（与 `0`/`1` 最容易看混的那两个字母）。
#   注意 `L`/`0`/`1` 是**保留**的——注释与代码必须一致，别照抄「排除 0/O/1/I/L」那种说法。
_ALPHABET = "".join(c for c in (string.ascii_uppercase + string.digits)
                    if c not in "OI")


def _positive_int(value, default: int) -> int:
    """配置来的**正整数**；非整数或非正数一律回退 `default`。

    ⚠️ 为什么不能「原样用」：`binding_code_length: 0` 会让码变成空串，
    而 `match` 那侧的 `is_code_shaped("")` 恒为假 —— 症状是**所有人都被踢、
    且永远绑不上**，正是门禁最怕的那种静默锁死。既然 0 在语义上没有意义，
    就回退默认值，宁可「配了没用」也不要「把全服新人挡在门外」。
    """
    try:
        n = int(value)
    except (TypeError, ValueError):
        return default
    return n if n > 0 else default


def _positive_ttl(value, default: float) -> float:
    """配置来的**正数秒数**；非数字或非正数一律回退 `default`。

    同上：`binding_code_ttl: 0` 会让每个码「发放即过期」（`now - issued_at >= 0`
    恒真），同样是全体锁死。**它不等于「永不过期」**——想长一点就填个大数。
    """
    try:
        v = float(value)
    except (TypeError, ValueError):
        return default
    return v if v > 0 else default


def describe_ttl(value) -> str:
    """有效期说成人话，供进服提示的 `{ttl}` 占位符使用（如 `5 分钟`）。

    ⚠️ **必须走 `_positive_ttl` 而不是直接用配置里那个值**：
    配 0 / 负数会被 `_positive_ttl` 回退成默认的 300 秒，真实有效期就是 5 分钟。
    若这里原样把 0 渲染成「0 秒」，提示语会对着玩家**说反话**——正是
    `render_karma_rules` 要消灭的那类「提示词与实际值脱节」。
    与它同理：一处渲染、一个来源，不靠「记得同步改两处」。
    """
    secs = _positive_ttl(value, CODE_TTL_SECONDS)
    if secs >= 60 and secs % 60 == 0:
        return "%d 分钟" % int(secs // 60)
    return "%d 秒" % int(secs)


def normalize_code(text: str) -> str:
    """归一化：去空白 → 全角转半角 → 转大写。

    全角那一半是刚需：中文输入法下打出的常是全角（与项目里
    `_SEPARATORS` 处理全角逗号是同一个教训）。
    """
    out = []
    for ch in str(text or "").strip():
        code = ord(ch)
        if code == 0x3000:          # 全角空格
            continue
        if 0xFF01 <= code <= 0xFF5E:  # 全角 ASCII 区
            code -= 0xFEE0
        out.append(chr(code))
    return "".join(out).upper()


def is_code_shaped(text: str, length: int = CODE_LENGTH) -> bool:
    """是否是「码」的形状：长度精确匹配，且全部是字母或数字。

    ⚠️ **本函数只判形状，不查码簿**。调用方先用它挡掉绝大多数群聊消息，
    再去查表——顺序反了会让每条群消息都去遍历码簿。
    """
    if length <= 0 or not text:
        return False          # `all()` 对空串是真空真——不挡掉的话 `is_code_shaped("", 0)` 会是 True
    if len(text) != length:
        return False
    # 大小写不敏感：`new_code` 只产大写，但群友手打可能是小写——
    # `match` 会先归一化，本函数自己也宽容一点，免得两个调用口径打架。
    up = text.upper()
    return all(c in string.ascii_uppercase or c in string.digits for c in up)


def new_code(rand_below, length: int = CODE_LENGTH) -> str:
    """生成一个新码。`rand_below(n)` 返回 `[0, n)` 的整数（注入以便测试）。

    `length` 由调用方传配置值（`binding_code_length`），缺省用模块常量。
    ⚠️ 非法值回退常量，见 `_positive_int`——**别**把它当成「可选的优化」删掉。
    """
    n = _positive_int(length, CODE_LENGTH)
    return "".join(_ALPHABET[rand_below(len(_ALPHABET))] for _ in range(n))


def issue(codes: dict, player: str, code: str, now: float) -> dict:
    """给某玩家记账一个码（重发**覆盖**旧的），返回新字典。

    同一个玩家只留一个待用码——积压一堆待用码只会让「哪个还有效」变成谜。
    """
    out = dict(codes)
    # ⚠️ 存**归一化后**的码（与 `match` 的判据一致）——否则调用方若把群里
    # 原样的全角/小写码交给本函数，存进去的码永远匹配不上。
    out[str(player)] = {"code": normalize_code(code), "issued_at": float(now)}
    return out


def match(codes: dict, table: dict, text: str, now: float,
          length: int = CODE_LENGTH, ttl: float = CODE_TTL_SECONDS) -> tuple:
    """在码簿里找 `text`。返回 `(player, qq)`。

    - 未命中 / 已过期 → `("", "")`
    - 命中且该玩家**已绑定** → `(player, 他已绑的 QQ)`，供调用方提示「你绑过了」
    - 命中且未绑定 → `(player, "")`

    ⚠️ `length` / `ttl` 由调用方传配置值，缺省用模块常量。**两者必须与
    发码时用的那套一致**——否则会出现「码发得出来、却永远兑不上」：
    `new_code(length=8)` 发 8 位码，而这里按 6 位判形状，那就是**全体锁死**。
    所以 `length` 也要喂给 `is_code_shaped`，不能只用在发码那一侧。

    ⚠️ 本函数**只读**：过期的码只是**不参与匹配**，清除是 `prune` 的活
    （调用方发新码前先 `prune` 一次，否则码簿只增不减）。
    """
    norm = normalize_code(text)
    if not is_code_shaped(norm, _positive_int(length, CODE_LENGTH)):
        return ("", "")
    ttl = _positive_ttl(ttl, CODE_TTL_SECONDS)
    hit_player = ""
    for player, rec in list(codes.items()):
        if not isinstance(rec, dict):
            continue
        if float(now) - float(rec.get("issued_at") or 0) >= ttl:
            continue
        if str(rec.get("code") or "") == norm:
            hit_player = str(player)
            break
    if not hit_player:
        return ("", "")
    rec = table.get(hit_player)
    bound_qq = str(rec.get("qq") or "").strip() if isinstance(rec, dict) else ""
    return (hit_player, bound_qq)


def prune(codes: dict, now: float, ttl: float = CODE_TTL_SECONDS) -> dict:
    """丢掉所有过期条目，返回新字典。

    `ttl` 由调用方传配置值（与 `match` 用**同一个**），缺省用模块常量。
    ⚠️ 两侧口径必须一致：`prune` 若按默认 300 清理，而 `match` 按配置的 30 判过期，
    清理就只是**提前**丢掉一些本来就兑不上的条目（后果轻）；反过来则会把
    `match` 认为还有效的条目清掉。**保持同步**是这里的唯一要求。
    """
    ttl = _positive_ttl(ttl, CODE_TTL_SECONDS)
    return {
        p: r for p, r in codes.items()
        if isinstance(r, dict)
        and float(now) - float(r.get("issued_at") or 0) < ttl
    }


def is_gated(table: dict, player: str, ignored: set, enabled: bool,
             exempt: set) -> bool:
    """这个人是否该被挡在门外。

    四道判据缺一不可：开关打开、有玩家名、未绑定、不在忽略名单也不在豁免名单。

    ⚠️ **按精确名字匹配，不做包含匹配**——`bot` 不该误伤 `robot`
    （与 `main.py` 的 `_is_ignored_player` 同口径）。
    ⚠️ **但比较时不区分大小写**：MC 上报的是**规范拼写**（`MoeDawn`），而配置项
    是手打的（可能写成 `moedawn`）。本项目的 `admin_mc` 就栽过这个坑
    （「填 `moedawn` 永远匹配不上」），修法是 `_admin_mc_lower` 统一小写比较。
    这里取同一口径——**豁免名单是管理员用途**，认不出来就会把管理员
    **踢出他自己的服务器**，比「事件被广播」严重得多。
    """
    if not enabled:
        return False
    name = str(player or "").strip()
    if not name:
        return False
    low = name.lower()
    ignored_low = {str(x).strip().lower() for x in ignored}
    exempt_low = {str(x).strip().lower() for x in exempt}
    if low in ignored_low or low in exempt_low:
        return False
    rec = table.get(name)
    if isinstance(rec, dict) and str(rec.get("qq") or "").strip():
        return False
    return True


def plan_migration(table: dict, records: dict, lo: int = -50, hi: int = 100) -> dict:
    """算出「绑定了的玩家，其 `mc:X` 该怎么并进 `qq:Q`」。**只算不写。**

    返回 `{qq:<QQ>: 求和后的值, ...}`，未绑定者不动。

    ⚠️ **基线必须取「已存在的那份 `qq:<QQ>` 值」**：规范 §4.3 写的是
    `qq:Q = clamp((qq:Q 或 mc:X 的值) + mc:X 的值)` —— 所以群里 60 + 游戏 70
    的结果是 100（夹到上限），不是 70。踩过一次：漏取基线时它只返回 mc 那一半。

    为什么不在这里写：迁移会**改动玩家数据**，规范明确要求「不自动做」——
    启动时只记一条 info，真正的执行点留给第三轮的面板按钮。这个函数就是
    那个按钮将来要调的东西。
    """
    out = {}

    def _num(v):
        """取好感值；非数字（含 bool）按 0。"""
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            return int(v)
        return 0

    for key, value in records.items():
        if not str(key).startswith("mc:"):
            continue
        player = str(key)[3:]
        rec = table.get(player)
        if not isinstance(rec, dict):
            continue
        qq = str(rec.get("qq") or "").strip()
        if not qq:
            continue
        qq_key = "qq:%s" % qq
        # 基线：该 QQ 已有的那份（可能来自群里聊出来的好感）
        merged = _num(records.get(qq_key)) + _num(value)
        out[qq_key] = max(lo, min(hi, merged))
    return out
