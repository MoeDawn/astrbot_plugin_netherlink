"""NetherLink 配置解析。

从 main.py 外移（2026-10-13 重构阶段一）。全部是**无状态纯函数**（原为
`@staticmethod`，不访问实例）。`_SEPARATORS` / `_OBSOLETE_CONFIG_KEYS` 两个
常量一并搬入 —— 它们只被这里的函数使用。
"""

from __future__ import annotations

import json
from pathlib import Path

from astrbot.api import logger

# 已从 schema 删除、但**存量配置里可能还留着**的键。
# ⚠️ 与 test_every_config_get_key_exists_in_schema 的 LEGACY_READS 是同一份名单：
#    那里管「代码会读的旧键」，这里管「该提醒用户清理的旧键」。
_OBSOLETE_CONFIG_KEYS = ("target_groups",)

# 中文输入法下极易打出的分隔符——它们不是半角逗号，直接 split(",") 会把
# 整串当成一个元素（如 admin_mc 变成 {"MoeDawn，Steve"}，永远匹配不上）。
# 统一归一化成半角逗号再切分。
_SEPARATORS = str.maketrans({"，": ",", "、": ",", "；": ",", ";": ",", "　": ","})


def find_stale_config(conf: dict, schema: dict) -> dict:
    """检查存量配置，分两类返回。

    为什么要它：AstrBot 的 `check_config_integrity` **只补缺失的键、从不覆盖
    已有的值**（`elif key not in conf: new_conf[key] = value`）。所以插件改了
    默认值，存量部署那边一点动静都没有——2026-09-23 的两个真 bug 都源于此
    （价目表没生效、名单被删空导致 QQ→MC 全断）。

    ⚠️ **两类必须分开，不能混成一个警告**：

    | 类别 | 含义 | 处理 |
    |---|---|---|
    | `obsolete` | 已废弃的键里**还有值** | **WARNING**——可操作，且正是咬过我们的情形 |
    | `customized` | 与当前默认值不同 | INFO 计数——**自定义是正常行为**，做成警告就是刷屏 |

    本项目自己的参考配置 40 项里有 17 项与默认不同，全报出来等于噪声。

    这个函数**只报告、不修改**：改配置是用户的事，插件擅自覆盖更危险。

    返回 `{"obsolete": [(键, 说明)], "customized": [(键, 说明)]}`。
    """
    obsolete: list = []
    customized: list = []
    for key in _OBSOLETE_CONFIG_KEYS:
        try:
            if conf.get(key):
                obsolete.append((key, f"已废弃，值为 {conf[key]!r:.60}——插件不再读取，可清空"))
        except Exception:
            pass

    for key, item in (schema or {}).items():
        if key in _OBSOLETE_CONFIG_KEYS:
            continue
        try:
            if key not in conf:
                continue
            cur, default = conf[key], item.get("default")
            if cur == default:
                continue
        except Exception:
            continue
        if isinstance(cur, str) and isinstance(default, str):
            customized.append((key, f"你的 {len(cur)} 字符 / 默认 {len(default)} 字符"))
        else:
            customized.append((key, f"你的 {cur!r:.60} / 默认 {default!r:.60}"))
    return {"obsolete": obsolete, "customized": customized}


def load_bundled_schema() -> dict:
    """读取**插件目录里**的 _conf_schema.json（默认值的权威来源）。

    读不到就返回空 dict——`find_stale_config` 对空 schema 自然只报废弃键，
    不会因为读文件失败而连累插件启动。
    """
    try:
        path = Path(__file__).resolve().parent / "_conf_schema.json"
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _as_str_list(raw) -> "list[str]":
    """把配置值归一成**字符串列表**。

    ⚠️ **为什么必须同时接受 str 与 list**（2026-09-23）：
    这几个"可配多个"的配置项已从 `type: "string"`（逗号分隔）改成
    `type: "list"`（WebUI 里的「修改列表项」弹窗）。但 AstrBot 的
    `check_config_integrity` **只补缺失的键、不覆盖用户已有的值**
    （源码里是 `elif key not in conf: new_conf[key] = value`），
    所以**已部署的实例拿到的仍是旧字符串**，而新装的拿到 list。

    两种都必须能解析，否则升级会直接让这些配置失效——且是静默失效。
    """
    if raw is None:
        return []
    if isinstance(raw, str):
        # 旧形态：单串，按分隔符切（容错中文分隔符，见 _SEPARATORS）
        text = raw.translate(_SEPARATORS)
        return [s.strip() for s in text.split(",") if s.strip()]
    if isinstance(raw, (list, tuple, set)):
        out = []
        for item in raw:
            text = str(item).translate(_SEPARATORS)
            # 元素内部也可能有分隔符（用户在单个条目里粘了 "a,b"）
            out.extend(s.strip() for s in text.split(",") if s.strip())
        return out
    logger.warning(f"NetherLink: 配置项类型无法识别的 {type(raw).__name__}，按空处理")
    return []


def _quick_command_entries(raw) -> list:
    """`quick_commands` 配置项 → 「一条条目一个字符串」的列表。

    🔴 **这里刻意不走 `_as_str_list` 的列表分支**——全仓库唯一一处这样做的地方，
    理由必须写下来，否则将来会有人把它「修」回去：

    `_as_str_list` 对**每一条列表项**还会再按半角逗号切一刀（它的注释写着
    「元素内部也可能有分隔符（用户在单个条目里粘了 "a,b"）」）。那条约定对
    `admin_mc` / `ws_ports` / `group_names` 是对的——那些项装的是**简单值**，
    多出来的逗号没有意义。而 `quick_commands` 的每一条是**任意指令文本**，
    逗号在指令里**有意义**：

        setblock 1 2 3 minecraft:chest{Items:[{id:"minecraft:diamond",count:1b}]}

    按逗号切不是「少一条」，而是**劈出来的两段各自都还带着名称与指令段**，
    于是双双被当成**合法按钮**注册进面板——管理员点下去执行的是一条他从没写过
    的（截断的）命令，而面板上看不出任何异常。切成两段都还合法的情形更糟：
    `tp @s 100 64 100, 200 64 200` 会静默跑成 `tp @s 100 64 100`。

    两种形态的数据形状**本来就不一样**，所以分开处理：

    - **str**（旧形态：逗号分隔的单串）→ 仍交 `_as_str_list`，兼容存量配置。
      单串形态里写不下带逗号的指令，那是它的固有限制。
    - **list / tuple / set**（`type: "list"` 的新形态）→ 每条**原样**取用，
      只去掉首尾空白，**不切逗号**。
    """
    if isinstance(raw, str):
        return _as_str_list(raw)
    if isinstance(raw, (list, tuple, set)):
        return [str(item).strip() for item in raw if str(item).strip()]
    if raw is None:
        return []
    logger.warning(
        f"NetherLink: quick_commands 配置项类型无法识别的 "
        f"{type(raw).__name__}，按空处理"
    )
    return []


def _read_int(config, key: str, default: int) -> int:
    """读一个整数配置项，任何异常都回退默认值。

    与 `_parse_csv` 等解析函数同一口径：配置来自 WebUI 手填，是外部输入，
    不能因为填了非数字就崩掉插件加载。
    """
    try:
        return int(config.get(key, default))
    except (TypeError, ValueError, OverflowError):
        logger.warning(f"NetherLink: 配置项 {key} 不是合法整数，已回退默认值 {default}")
        return default


def _parse_csv(raw) -> set[str]:
    """把配置解析成无重复集合。

    接受三种形态：旧的逗号分隔**字符串**（"111, 222"）、新的 **list**
    （["111","222"]）、以及 None/空。前者是已部署实例的存量形态，
    后者是 `type: "list"` 之后的新形态——两者都必须支持，见 _as_str_list。
    """
    return set(_as_str_list(raw))


def _parse_group_names(raw: str) -> dict[str, str]:
    """把 '群号:群名, 987654:生存服' 形式的配置解析成 {群号: 群名}。

    同样容错中文分隔符（全角逗号 / 顿号），并把全角冒号也归一化——
    `group_names` 走这里，写错一个字符就会
    静默失效。
    """
    result: dict[str, str] = {}
    for part in _as_str_list(raw):
        part = part.replace("：", ":")
        if not part:
            continue
        if ":" in part:
            gid, name = part.split(":", 1)
            gid, name = gid.strip(), name.strip()
            if gid and name:
                result[gid] = name
        else:
            result[part] = part  # 只填群号时群名用群号代替
    return result


def _parse_ws_ports(raw) -> list:
    """解析 ws_ports 配置 → [(显示名, port), ...]。

    - 留空/全非法 → []，插件**不监听任何端口**（启动时会明确告警）
    - "8765,8766" → [("", 8765), ("", 8766)]，这两台没有显示名，回退常量 "MC"
    - "生存服:8765" → [("生存服", 8765)]

    🔴 **冒号前那一段是「显示名」，不是身份**（2026-10-07 改版，用户要求）：
    内部身份、定向发指令、会话键**一律用 MC 端上报的 server-name**。
    显示名只用于展示（QQ 群消息前缀 / 面板 / 模板 `{server}`）。

    ⚠️ **空串表示「这台没有显示名」**，回退 `DEFAULT_SERVER_DISPLAY`。
    ⚠️ 以前这里写的是 `server_name`（port→id 的绑定），身份优先取它、其次才取
    上报名；那条路已删除——**身份只认上报名**，不再有第二来源。
    """
    out: list = []
    seen: set = set()
    for part in _as_str_list(raw):
        part = part.replace("：", ":")
        name, _, port_s = part.rpartition(":")
        if not port_s:
            name, port_s = "", part
        try:
            port = int(port_s)
        except (TypeError, ValueError):
            logger.warning(f"NetherLink: ws_ports 里的端口非法，已跳过: {part!r}")
            continue
        if not (0 < port < 65536):
            logger.warning(f"NetherLink: ws_ports 里的端口越界，已跳过: {part!r}")
            continue
        if port in seen:
            logger.warning(f"NetherLink: ws_ports 里的端口重复，已跳过: {part!r}")
            continue
        seen.add(port)
        out.append((name.strip(), port))
    return out
