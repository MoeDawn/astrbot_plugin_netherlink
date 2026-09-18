# -*- coding: utf-8 -*-
"""NetherLink — Minecraft(Paper) 服务器 × QQ 群双向消息互通。

架构：
    QQ群 <-> NapCat/Lagrange <-> AstrBot(本插件) <-WebSocket/JSON行-> MC Paper 插件

本插件职责：
    1. 启动 WebSocket 服务端，接收 MC 端插件连入（token 鉴权、断线重连由 MC 端负责）
    2. 监听绑定群的 QQ 消息 -> 转发给 MC 广播到公屏（self_id 识别机器人自身消息防循环）
    3. 接收 MC 事件（chat/join/leave/death/bot_chat）-> 按模板推送到绑定群；
       bot_chat（游戏内唤醒词消息）单独走 LLM 对话，回复仅发回游戏，QQ 不可见
    4. 注册 LLM 函数工具：
       - mc_command：以控制台身份执行 MC 指令并回传服务器真实输出。
         是否该执行、该收多少好感由 AI 依提示词自行判断，插件不做权限拦截；
         AI 报出的 cost 由插件原子执行（先扣后执行、失败回滚）
"""

import asyncio
import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional

import aiohttp
from aiohttp import web

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.star import Context, Star
from astrbot.core.utils.astrbot_path import get_astrbot_plugin_data_path

try:
    from .karma import (
        KARMA_MAX_RECORDS,
        KarmaStore,
        clamp_cost,
        clamp_value,
        identity_key,
        merge_records,
    )
except ImportError:  # 插件以顶层模块方式加载时
    from karma import (
        KARMA_MAX_RECORDS,
        KarmaStore,
        clamp_cost,
        clamp_value,
        identity_key,
        merge_records,
    )

# 游戏内一次 LLM 对话回复的最大长度（超出截断，MC 聊天框放不下太长的文本）
MC_REPLY_MAX_LEN = 900

# 好感度规则提示词默认值（AI 据此自主决定好感增减与指令消耗）
DEFAULT_KARMA_RULES = """\
[好感度规则]
你和玩家之间存在一个好感值，初始值 10，最低 -50，最高 100。
玩家获得成就、和你进行友善的让心情变好的对话，你可以增加此数值；
玩家发表不友好言论，你可以扣除好感。以上行为造成的好感变化每次不超过 ±2。
你和玩家对话时，根据此数值来改变说话方式和态度；如果因为对话导致好感度变化，你会隐晦地暗示。
当玩家请求你使用指令时，你调用 mc_command 工具时传入的 cost 参数就是本次消耗的好感，
插件会自动完成扣除，你不需要自己再扣一次。
请求的指令越 OP 消耗的好感越多（参考：tp 到其他玩家附近一次扣 1；
tp 到附近村庄这类需要定位+传送+有价值的地点一次扣 20；要 3 个铁锭扣 3；
要钻石或金苹果一次扣 10；纯查询类如 list/time/weather 不消耗）。
如果请求的指令消耗大于玩家剩余好感，你会拒绝执行并隐晦的透露原因。"""

# mc_command 工具描述默认值。刻意不含好感度内容——AI 调用本工具前
# 已用 mc_karma 查过好感，不达标自然不会调用。
#
# ⚠️ 本描述同时挂给三个面：QQ 外层 agent（`@filter.llm_tool` 注册的那份）、
# QQ 内层 agent、游戏侧 agent。其中**只有外层 agent 拿不到 `<netherlink_context>`**
# （它读的是框架内建的 system_prompt，插件注入不进去）。所以这里绝不能断言
# 「名单见 netherlink_context」——外层 agent 找不到名单，会把管理员的合法请求
# 当成"非管理员"直接拒掉，连内层 agent（那份名单就在它手里）都不会被启动。
# 只描述**场景**，把名单的来源写成条件式，三个面读起来才都是真的。
DEFAULT_MC_COMMAND_TOOL_DESC = """\
在 Minecraft 服务器上以控制台身份执行一条指令，并返回服务器真实输出。
执行前请自行判断这条指令是否属于高危操作（如改游戏模式、传送他人、给予物品、
封禁、op、清空区域等）。高危操作应审慎处理。
调用本工具后，系统可能要求你进一步确认消耗与权限；如需判断发起者身份，
以对话中提供的 <netherlink_context> 为准（若未提供，则按普通玩家对待）。"""

# mc_karma 工具描述默认值
DEFAULT_KARMA_TOOL_DESC = """\
查询或增减玩家好感度。目标是当前对话的发起者，无法指定其他玩家。
delta 为 0 时只查询，为正数时增加好感，为负数时扣除好感。
每次与玩家对话时，先调用本工具（delta=0）查看当前好感值，再据此调整你的说话方式和态度。
当玩家请求你执行 Minecraft 指令时，同样先查看好感，判断其剩余好感是否足以支付
该指令的消耗；不足以支付的，你会拒绝执行并隐晦的透露原因。
好感度的增减由对话内容本身决定（友善互动、不友好言论等），对照好感度规则执行。"""

# 游戏内对话附加提示词默认值，{server} 会替换为服务器名
DEFAULT_EXTRA_SYSTEM_PROMPT = "你当前处于一个我的世界服务器内,服务器名称为{server}"

# 旧版（计分板时代）karma_rules 默认值的特征串。
# AstrBot 会把 schema 的 default 落盘进 cmd_config.json，已保存过配置的部署会一直
# 返回落盘的那份，改 schema 也追不回来——旧文案里让 AI「先用 mc_karma 判断并扣除
# 好感度」，与 v3.0 的 cost 机制冲突，会让重构在存量部署上静默失效。因此启动时检测
# 该特征串并覆盖为当前默认值。
LEGACY_KARMA_RULES_MARKER = "dummy 型计分板"


class NetherLinkPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config

        # ---- 配置解析 ----
        self.ws_host: str = config.get("ws_host", "0.0.0.0")
        self.ws_port: int = int(config.get("ws_port", 8765))
        self.auth_token: str = config.get("auth_token", "")
        self.target_groups: set[str] = self._parse_csv(config.get("target_groups", ""))
        self.admin_qq: set[str] = self._parse_csv(config.get("admin_qq", ""))
        # 游戏内机器人唤醒词（前缀），默认与 QQ 侧唤醒一致
        self.mc_wake_prefixes: list[str] = [
            p.strip() for p in str(config.get("mc_wake_prefixes", "ai,助手")).split(",") if p.strip()
        ]
        # 名称与占位符：模板里可用 {server}/{group}/{bot} 分别替换为
        # 服务器名/群名/机器人游戏内名字
        self.mc_server_name: str = str(config.get("mc_server_name", "MC") or "MC")
        self.mc_bot_name: str = str(config.get("mc_bot_name", "ai") or "ai")
        self.group_names: dict[str, str] = self._parse_group_names(
            str(config.get("group_names", "") or "")
        )
        # 游戏侧对话的系统提示词拼装：
        # [WebUI 人格（可开关）] + [自定义提示词] + [karma 好感规则（始终注入）]
        self.enable_webui_persona: bool = bool(config.get("enable_webui_persona", True))
        # 注意：本项与下面三个 DEFAULT_* 的兜底口径不同，是有意为之，勿"修正"。
        # 本项允许留空关闭（消费点用 `if self.extra_system_prompt:` 判定，
        # WebUI 配置提示也写着"留空则不附加"），所以只有"键缺失"才回退默认值，
        # "键存在但为空串"必须保持 ""。下面三个则相反：空串会让 AI 失去规则约束
        # 或根本无法调用工具，属于坏配置，一律回退默认文案。
        self.extra_system_prompt: str = str(
            config.get("extra_system_prompt", DEFAULT_EXTRA_SYSTEM_PROMPT) or ""
        )
        # 好感度是强制机制，没有开关：想「不依赖好感度」只能改提示词
        # （把 karma_rules 的消耗表写成全 0），不能停用代码路径。
        self.karma_rules: str = str(
            config.get("karma_rules", DEFAULT_KARMA_RULES) or DEFAULT_KARMA_RULES
        )
        # 存量部署迁移：落盘的旧版规则与 cost 机制冲突，检测到就覆盖并告警
        if LEGACY_KARMA_RULES_MARKER in self.karma_rules:
            logger.warning(
                "NetherLink: 检测到落盘的旧版 karma_rules（计分板时代），"
                "已覆盖为当前默认规则——旧规则要求 AI 手动扣好感，与 mc_command 的 "
                "cost 机制冲突。如你曾自定义过该字段，请到 WebUI 重新确认。"
            )
            self.karma_rules = DEFAULT_KARMA_RULES
            try:
                self.config["karma_rules"] = DEFAULT_KARMA_RULES
                self.config.save_config()
            except Exception as e:
                logger.error(f"NetherLink: 迁移 karma_rules 写回配置失败: {e}")
        self.mc_command_tool_desc: str = str(
            config.get("mc_command_tool_desc", DEFAULT_MC_COMMAND_TOOL_DESC)
            or DEFAULT_MC_COMMAND_TOOL_DESC
        )
        self.karma_tool_desc: str = str(
            config.get("karma_tool_desc", DEFAULT_KARMA_TOOL_DESC)
            or DEFAULT_KARMA_TOOL_DESC
        )
        # 管理员游戏 ID（AstrBot 全局管理员的 admins_id 是 QQ 号，对游戏内无效）
        self.admin_mc: set[str] = self._parse_csv(config.get("admin_mc", ""))
        # 好感度参数
        # karma_initial 必须过 clamp_value：KarmaStore.get 在"无记录"路径上把
        # initial 原样返回（只有已存的值才走 clamp_value），配置里填 500 会让所有
        # 新玩家拿到越界的初始好感，并被 AI 当作事实报出去。
        # OverflowError 一并兜底：JSON 里的 1e400 解析成 inf，int(inf) 抛的是
        # OverflowError 而不是 ValueError，漏掉会直接崩掉插件加载。
        try:
            self.karma_initial: int = clamp_value(int(config.get("karma_initial", 10)))
        except (TypeError, ValueError, OverflowError):
            self.karma_initial = clamp_value(10)
        # 这两个数都是"外部输入"（WebUI 手填），必须夹住负值：
        #   karma_death_penalty < 0 会把死亡变成好感**奖励**（-(-2) = +2）；
        #   max_command_cost <= 0 会让 clamp_cost 一律返回 0，所有指令免费。
        # 0 都是合法选择（=死亡不扣 / 指令全免费），只夹负值，不回退默认。
        # OverflowError 一并兜底：JSON 里的 1e400 解析成 inf，int(inf) 抛的是
        # OverflowError 而不是 ValueError，漏掉会直接崩掉插件加载。
        try:
            self.max_command_cost: int = max(0, int(config.get("max_command_cost", 100)))
        except (TypeError, ValueError, OverflowError):
            self.max_command_cost = 100
        try:
            self.karma_death_penalty: int = max(
                0, int(config.get("karma_death_penalty", 2))
            )
        except (TypeError, ValueError, OverflowError):
            self.karma_death_penalty = 2
        # 对话触发的好感变化是否记一条日志（供运维观察 AI 的增减行为）
        self.log_karma_changes: bool = bool(config.get("log_karma_changes", True))

        self.templates = {
            "chat": config.get("template_chat", "[{server}] {player}: {text}"),
            "join": config.get("template_join", "[{server}] {player} 进入了服务器"),
            "leave": config.get("template_leave", "[{server}] {player} 离开了服务器"),
            "death": config.get("template_death", "[{server}] {message}"),
            "qq_to_mc": config.get("template_qq_to_mc", "⌜§a{group}§f⌟ <§b{sender}§f> §5{text}§f"),
            "bot_reply_game": config.get(
                "template_bot_reply_game", "⌜§c{bot}§f⌟ : §d{text}§f"
            ),
        }

        # ---- 运行时状态 ----
        self._runner: Optional[web.AppRunner] = None
        self._mc_ws: Optional[aiohttp.web.WebSocketResponse] = None  # 当前 MC 连接（单服务器）
        # MC 端 hello 握手上报的真实服务器名（区别于 mc_server_name 显示名）
        self._mc_server_reported: str = ""
        # 等待执行结果的指令：id -> asyncio.Future
        self._pending_cmds: dict[str, asyncio.Future] = {}
        # 游戏内对话进行中的玩家 -> asyncio.Lock，防止同玩家并发请求 LLM
        self._player_llm_locks: dict[str, asyncio.Lock] = {}

        # ---- 好感度（本地文件 + 配置项双写，供 WebUI 查看与手改） ----
        # 身份空间见 karma.identity_key：游戏内玩家 "mc:<游戏ID>"，QQ 群友 "qq:<QQ号>"，
        # 同一个人在两边是两份独立好感，不做映射。
        self._karma_dir: Path = Path(get_astrbot_plugin_data_path()) / "netherlink"
        self._karma_path: Path = self._karma_dir / "karma.json"
        self._karma_lock = asyncio.Lock()
        # 优先文件、其次配置项，同键取 updated 较新者（管理员可在 WebUI 手改配置）。
        # 加载失败必须让插件照常加载——好感度是软约束。各分支的实际保证：
        #   正常分支：store 绑定磁盘路径，改动落盘 + 写回配置项。
        #   降级分支：store 的 path=None，**磁盘文件绝不会被写**（读失败的文件原样
        #     留在盘上，修好后下次启动即可恢复）；但只要还能解析配置项，就继续用
        #     配置项里的记录播种，于是后续写回的是「配置原有记录 + 本次改动」，
        #     不会把管理员手填的条目清空。
        #   播种也失败：退化为空表且置 _karma_degraded，此时禁止写回配置项——
        #     内存里没有配置项的记录，写回等于把它们全部清掉。
        # KarmaStore.read 对缺失/损坏/非 UTF-8 文件返回空表且已归一化，不抛异常；
        # 这里兜的是更外围的意外（如 RecursionError、数据目录不可读）。
        self._karma_degraded: bool = False
        try:
            self._karma: KarmaStore = KarmaStore(
                merge_records(
                    KarmaStore.read(self._karma_path).snapshot(),
                    self._parse_config_records(),
                ),
                self._karma_path,
            )
        except Exception as e:
            logger.error(f"NetherLink: 好感度记录加载失败（磁盘文件不可读）: {e}")
            # 降级：path=None 的纯内存 store，磁盘文件因此不会被覆写。
            # 但仍要用配置项里的记录播种——否则下一次 delta!=0 的 _karma_add 会把
            # 只含本次改动这一条的快照写回 karma_records，把管理员手改的条目全清掉。
            try:
                self._karma = KarmaStore(
                    merge_records({}, self._parse_config_records()), None
                )
            except Exception as e2:
                logger.error(f"NetherLink: 降级加载好感记录失败（按空表继续）: {e2}")
                self._karma = KarmaStore({}, None)  # path=None：纯内存，不再落盘
                self._karma_degraded = True
        # 旧绑定表已废弃，不读不删，仅提示
        try:
            legacy = self._karma_dir / "bindings.json"
            if legacy.exists():
                logger.info(
                    "NetherLink: 检测到已废弃的 bindings.json（绑定表功能已移除），"
                    "插件不再读取该文件，可自行删除"
                )
        except OSError as e:
            logger.warning(f"NetherLink: 检查遗留 bindings.json 失败（忽略）: {e}")

        asyncio.create_task(self._start_ws_server())
        # 工具描述回填必须在 @filter.llm_tool 注册完成之后（即本类定义已被插件加载器
        # 扫描过），因此放在 __init__ 末尾；失败不影响工具可用性
        self._apply_configured_tool_descs()
        logger.info("NetherLink 已加载")

    # ------------------------------------------------------------------
    # 工具函数
    # ------------------------------------------------------------------
    @staticmethod
    def _parse_csv(raw: str) -> set[str]:
        """把 '111, 222' 形式的配置解析成无重复集合。"""
        return {s.strip() for s in (raw or "").split(",") if s.strip()}

    @staticmethod
    def _parse_group_names(raw: str) -> dict[str, str]:
        """把 '群号:群名, 987654:生存服' 形式的配置解析成 {群号: 群名}。"""
        result: dict[str, str] = {}
        for part in (raw or "").split(","):
            part = part.strip()
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

    # ------------------------------------------------------------------
    # 好感度存取（本地文件 + 配置项双写）
    # ------------------------------------------------------------------
    def _parse_config_records(self) -> dict:
        """解析 karma_records 配置项（JSON 文本或已是 dict）。失败返回空表。

        WebUI 里该配置可能是被手改过的 JSON 文本，解析失败按空表处理，
        绝不能因此挡住插件加载。
        """
        try:
            raw = self.config.get("karma_records", {})
        except Exception as e:
            logger.warning(f"NetherLink: 读取 karma_records 配置失败，按空表处理: {e}")
            return {}
        if isinstance(raw, dict):
            return raw
        try:
            parsed = json.loads(str(raw or "{}"))
        except (ValueError, TypeError):
            # json.JSONDecodeError 是 ValueError 子类，一并覆盖
            logger.warning("NetherLink: karma_records 配置解析失败，按空表处理")
            return {}
        return parsed if isinstance(parsed, dict) else {}

    def _apply_configured_tool_descs(self) -> None:
        """用配置项覆盖工具 schema 的描述。

        @filter.llm_tool 的描述来自装饰函数的 docstring，配置值无法在注册期注入，
        因此注册完成后回填。失败仅告警，不影响工具可用性。
        """
        if self.context is None:
            # 无上下文（如测试替身）时不存在工具管理器，没有可覆盖的目标，
            # 这不是错误，静默跳过——否则每次加载都会刷一条无意义的 warning
            return
        try:
            mgr = self.context.get_llm_tool_manager()
            # 两个工具都始终覆盖描述，没有任何开关能跳过 mc_karma 那一项：
            # 好感度是强制机制，工具描述就是 AI 唯一的策略来源。
            targets = [
                ("mc_command", self.mc_command_tool_desc),
                ("mc_karma", self.karma_tool_desc),
            ]
            for name, desc in targets:
                tool = mgr.get_func(name)
                if tool is None:
                    # 查不到只可能是"工具注册晚于插件实例化"或工具名被改，此时配置项
                    # 静默失效。必须留痕，否则真机上无法分辨"覆盖成功"与"什么都没做"。
                    logger.warning(
                        f"NetherLink: 未找到已注册的工具 {name}，"
                        f"配置的工具描述未生效（将使用代码内默认描述）"
                    )
                    continue
                if desc:
                    tool.description = desc
        except Exception as e:
            logger.warning(f"NetherLink: 覆盖工具描述失败（使用默认描述）: {e}")

    @staticmethod
    def _now_iso() -> str:
        """好感记录的 updated 时间戳（本地时间，秒级 ISO）。"""
        return datetime.now().isoformat(timespec="seconds")

    async def _karma_get(self, key: str) -> int:
        """读取好感值（不写盘）。AI 每次对话都会查询，必须无副作用。"""
        async with self._karma_lock:
            return self._karma.get(key, self.karma_initial)

    async def _karma_add(self, key: str, delta: int, origin: str = "") -> tuple:
        """增减好感并落盘 + 写回配置项，返回 (旧值, 新值)。

        delta=0 时只读不写，避免 AI 每次对话的查询触发写盘。

        origin 非空表示这是**对话触发**的变化（AI 依 karma_rules 自主增减，
        而非指令消耗 / 死亡惩罚 / 回滚补偿），在 log_karma_changes 开启时记一条
        info 日志——对话触发的变化没有别的痕迹，运维只能从这里观察 AI 的增减行为。
        传「游戏内对话」或「QQ 对话」标明来源侧。
        """
        async with self._karma_lock:
            # 先取一次旧值：add 在落盘失败时已经改过内存，只有提前取过才能报出真正的旧值
            before = self._karma.get(key, self.karma_initial)
            try:
                old, new = self._karma.add(
                    key, delta, self.karma_initial, self._now_iso()
                )
            except Exception as e:
                # 落盘失败（磁盘满/权限）或 delta 不是数字时不能让对话崩掉。
                # 旧值取改动前的快照，新值取内存当前真实值，与随后的 _karma_get 口径一致。
                logger.error(f"NetherLink: 好感度写入失败（{key}, delta={delta!r}）: {e}")
                now = self._karma.get(key, self.karma_initial)
                return before, now
            if delta:
                # last_dropped 是单槽状态，必须在同一把锁内紧接 add 之后读取，
                # 否则并发 add 会把它覆盖成别人的淘汰结果
                dropped = list(self._karma.last_dropped)
                if dropped:
                    logger.warning(
                        f"NetherLink: 好感度记录已达上限 {KARMA_MAX_RECORDS} 条，"
                        f"按 updated 淘汰最旧的 {len(dropped)} 条: {dropped}"
                    )
                self._sync_karma_to_config()
                if origin and self.log_karma_changes:
                    logger.info(
                        f"NetherLink: [{origin}] {key} 好感 {old} → {new}"
                        f"（{delta:+d}，范围 -50~100）"
                    )
            return old, new

    def _sync_karma_to_config(self) -> None:
        """把好感度快照写回配置项，WebUI 刷新即可见。失败仅告警。

        两道保护都做在本方法里，而不是留给调用方——写回是一件事，
        「当前状态能不能写」是这个方法自己的属性；靠调用方自觉，
        漏判一次就等于把管理员手填的 karma_records 清空。
          1. 降级态（内存表没能从配置项播种）下快照不完整，一律拒绝写入；
          2. 空快照绝不允许覆盖非空的 karma_records。
        """
        if self._karma_degraded:
            logger.error(
                "NetherLink: 好感记录处于降级态，跳过写回配置，"
                "以免清空管理员手填的 karma_records"
            )
            return
        try:
            snapshot = self._karma.snapshot()
            if not snapshot and self._parse_config_records():
                # 空快照绝不允许覆盖非空的 karma_records——那是管理员手填的记录，
                # 清掉就找不回来了
                logger.error("NetherLink: 好感快照为空但配置项有记录，跳过写回以免清空")
                return
            # schema 把本项声明为 type=text / default="{}"，落盘必须是字符串：
            # 写 dict 会让 WebUI 把对象塞进文本控件，且配置项类型在 dict 与 str
            # 之间反复横跳。读取侧 _parse_config_records 两种都认，不受影响。
            self.config["karma_records"] = json.dumps(snapshot, ensure_ascii=False)
            self.config.save_config()
        except Exception as e:
            logger.error(f"NetherLink: 写回 karma_records 配置失败: {e}")

    def _mc_connected(self) -> bool:
        return self._mc_ws is not None and not self._mc_ws.closed

    def _fmt(self, tpl: str, **kw) -> str:
        try:
            return tpl.format(**kw)
        except (KeyError, IndexError):
            return tpl  # 模板占位符写错时兜底原样输出

    # ------------------------------------------------------------------
    # WebSocket 服务端
    # ------------------------------------------------------------------
    async def _start_ws_server(self):
        """启动 WS 服务端，监听 MC 端连入。"""
        app = web.Application()
        app.router.add_get("/ws", self._ws_handler)
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, self.ws_host, self.ws_port)
        try:
            await site.start()
            logger.info(f"NetherLink WS 服务端已启动 ws://{self.ws_host}:{self.ws_port}/ws")
        except OSError as e:
            logger.error(f"NetherLink WS 端口 {self.ws_port} 启动失败: {e}")

    async def _ws_handler(self, request):
        ws = web.WebSocketResponse(heartbeat=30)
        await ws.prepare(request)
        logger.info("NetherLink: MC 端已连入，等待握手")

        server_name = "mc"
        async for msg in ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                except json.JSONDecodeError:
                    continue

                mtype = data.get("type")

                if mtype == "hello":
                    # 握手鉴权：token 不匹配直接断开
                    if data.get("token") != self.auth_token:
                        logger.warning("NetherLink: MC 端 token 校验失败，断开连接")
                        await ws.close(code=4001, message=b"auth failed")
                        return ws
                    server_name = data.get("server_name") or "mc"
                    self._mc_server_reported = server_name
                    # 单服务器设计：新连接握手成功时踢掉残留的旧连接（如 MC 端
                    # 断线重连后旧 socket 才超时），避免消息发进死管道
                    old = self._mc_ws
                    if old is not None and old is not ws and not old.closed:
                        logger.warning("NetherLink: 检测到旧 MC 连接，主动关闭")
                        await old.close(code=4002, message=b"replaced by new connection")
                    self._mc_ws = ws
                    logger.info(f"NetherLink: MC 服务器 [{server_name}] 握手成功")
                elif self._mc_ws is not ws:
                    continue  # 未通过握手的连接不发事件

                elif mtype == "command_result":
                    fut = self._pending_cmds.pop(data.get("id"), None)
                    if fut and not fut.done():
                        fut.set_result(str(data.get("output", "")))
                elif mtype == "heartbeat":
                    pass
                elif mtype == "bot_chat":
                    # 游戏内唤醒词消息：走 LLM，回复只发回游戏，QQ 不可见
                    asyncio.create_task(self._handle_bot_chat(data))
                else:
                    await self._dispatch_mc_event(mtype, data)

            elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSE):
                break

        if self._mc_ws is ws:
            self._mc_ws = None
            logger.warning("NetherLink: MC 服务器连接断开")
        return ws

    async def _dispatch_mc_event(self, mtype: str, data: dict):
        """把 MC 事件按模板渲染后推送到所有绑定群。"""
        try:
            srv = self.mc_server_name
            if mtype == "chat" and self.config.get("enable_chat", True):
                text = self._fmt(self.templates["chat"], server=srv, bot=self.mc_bot_name,
                                 player=data.get("player", "?"), text=data.get("text", ""))
            elif mtype == "join" and self.config.get("enable_join_leave", True):
                text = self._fmt(self.templates["join"], server=srv, bot=self.mc_bot_name,
                                 player=data.get("player", "?"))
            elif mtype == "leave" and self.config.get("enable_join_leave", True):
                text = self._fmt(self.templates["leave"], server=srv, bot=self.mc_bot_name,
                                 player=data.get("player", "?"))
            elif mtype == "death" and self.config.get("enable_death", True):
                # 注意：本开关同时控制死亡消息同步与死亡扣好感，是死亡惩罚**唯一**
                # 的门槛（好感度是强制机制，没有别的开关）。关掉它等于同时关闭死亡
                # 惩罚——这是刻意的（不想看到死亡播报的人多半也不想要静默扣好感），
                # schema 的 enable_death 说明里写明了这一点。
                # `or "?"` 与下面的 player != "?" 是同一个门：JSON null / 空串同样
                # 代表"这条事件没带玩家"，一律并到显示回退值 "?" 上，
                # 否则会建出 mc:None / mc: 这种假记录
                player = str(data.get("player") or "?")
                # 玩家死亡时 AI 不在场（死亡走 WS 事件、不走对话，没有它那一轮判断），
                # 只能由插件自己扣好感。扣减放在广播之前——广播失败（推送异常）绝不能
                # 连累这笔游戏事实，而 _karma_add 自己吞写盘异常、有外层 try 兜底，
                # 反过来也不会连累广播。
                # 也放在 _fmt 之前：_fmt 只兜 KeyError/IndexError，模板里写错格式说明符
                # （如 {message:d}）会抛 ValueError 逃到外层 except，从而连同扣减一起跳过
                # player 未知时回退值是 "?"，扣它会造出 mc:? 这条假记录，因此排除
                if self.karma_death_penalty and player != "?":
                    await self._karma_add(
                        identity_key("game", player, ""), -self.karma_death_penalty
                    )
                text = self._fmt(self.templates["death"], server=srv, bot=self.mc_bot_name,
                                 player=player, message=data.get("message", ""))
            else:
                return
            await self._broadcast(text)
        except Exception as e:
            logger.error(f"NetherLink: 处理 MC 事件 {mtype} 失败: {e}")

    def _mc_server_display(self) -> str:
        """LLM 上下文用的服务器名：优先 MC 端 hello 上报的真实名字，回退显示名配置。"""
        return self._mc_server_reported or self.mc_server_name

    # ------------------------------------------------------------------
    # 提示词拼装（游戏侧与 QQ 内层共用）
    # ------------------------------------------------------------------
    def _admin_context(self, identity: str, is_admin: bool, source: str) -> str:
        """管理员名单 + 当前发起者身份，供 AI 判断是否放行高危操作。

        名单只是参考信息，插件不做任何拦截——是否放行由 AI 决定。

        ⚠️ 只给**适用**的那份名单：QQ 侧发起时身份是 QQ 号，游戏 ID 名单对他的
        判定毫无帮助；游戏侧发起时我们根本不知道他的 QQ 号，给 QQ 名单反而会让
        AI 误以为掌握了他不在场的信息。名单本身由调用方按 source 选定，
        两侧的成员判定仍各用各的（_qq_is_admin / _game_is_admin）。

        名单为空时也照样输出带队名的那一行（写「未配置」），否则 AI 分不清
        "本服没有管理员"与"插件没告诉它"，等于把判断依据抽走了。
        """
        if source == "qq":
            roster = ", ".join(sorted(self.admin_qq)) or "（未配置）"
            roster_line = f"服务器管理员（QQ 号）：{roster}。\n"
        else:
            roster = ", ".join(sorted(self.admin_mc)) or "（未配置）"
            roster_line = f"服务器管理员（游戏 ID）：{roster}。\n"
        who = "是管理员。" if is_admin else "不是管理员。"
        return (
            "<netherlink_context>\n"
            f"这条消息来自 Minecraft 游戏服务器「{self._mc_server_display()}」。\n"
            f"{roster_line}"
            f"当前发起者：{identity}，{who}\n"
            "</netherlink_context>"
        )

    def _qq_is_admin(self, event) -> bool:
        """QQ 侧管理员判定：AstrBot 全局管理员或 admin_qq 白名单。

        只用于提示词注入（见 _admin_context），不参与任何放行判断。
        """
        try:
            if event.is_admin():
                return True
        except Exception:
            pass  # 平台/替身没有 is_admin 时退回名单判定
        try:
            return str(event.get_sender_id() or "") in self.admin_qq
        except Exception:
            return False

    def _game_is_admin(self, mc_id: str) -> bool:
        """游戏侧管理员判定：只认 admin_mc。

        AstrBot 全局管理员的 admins_id 与 admin_qq 都是 QQ 号，对游戏内身份无效，
        所以游戏侧没有"全局管理员"这回事。
        """
        return str(mc_id) in self.admin_mc

    async def _build_system_parts(
        self, umo: str, identity: str, is_admin: bool, source: str
    ) -> list:
        """拼装 LLM 对话的 system_prompt 各段（游戏侧与 QQ 内层共用），顺序即最终顺序。

        顺序：[WebUI 人格（可开关）] → [extra_system_prompt + karma_rules] → [管理员上下文]
        中间两段连成**同一个** part：用户要求好感度规则紧跟在自定义提示词之后拼接。
        段与段之间由调用点用 "\\n\\n" 连接（见 _handle_bot_chat）。
        source 透传给 _admin_context，决定给 AI 看哪一份管理员名单
        （"qq" / "game"）。

        最后一段（管理员上下文）永远存在，所以返回值永不为空。
        """
        parts: list[str] = []
        if self.enable_webui_persona:
            try:
                # get_default_persona_v3 是协程方法：漏掉 await 会拿到 coroutine 对象，
                # 随后的 persona.get(...) 抛 AttributeError 并被本兜底吞掉，
                # 人格于是静默失效（Task 9 修的正是这个 bug）。
                persona = await self.context.persona_manager.get_default_persona_v3(umo)
                if persona and persona.get("prompt"):
                    parts.append(str(persona["prompt"]))
            except Exception as e:
                # 读不到人格不能连累对话，但必须留痕——静默失败正是这个 bug 藏了这么久的原因
                logger.warning(f"NetherLink: 读取 WebUI 人格失败（跳过）: {e}")

        # 自定义提示词与好感规则连成一个 part。extra_system_prompt 留空 = 不附加
        # （见 __init__ 里的口径说明），此时好感规则仍独立贡献，且不留前导换行。
        blocks: list[str] = []
        extra = str(self.extra_system_prompt or "")
        extra = extra.replace("{server}", self._mc_server_display())  # 唯一支持的占位符
        if extra.strip():
            blocks.append(extra)
        if self.karma_rules:
            blocks.append(self.karma_rules)
        if blocks:
            parts.append("\n".join(blocks))

        parts.append(self._admin_context(identity, is_admin, source))
        return parts

    async def _handle_bot_chat(self, data: dict):
        """游戏内玩家用唤醒词跟机器人说话：转交给 AstrBot 的 LLM 管线处理。

        设计对齐 AstrBot 理念：插件不自建人设，而是把玩家消息作为 prompt 转交
        tool_loop_agent（使用 WebUI 配置的人格与模型），仅附加最小化的来源
        提示词（消息来自游戏服务器 + 玩家身份）。多轮上下文通过合成事件
        的 unified_msg_origin 挂到 AstrBot 会话管理器，同一玩家连续对话有记忆。
        回复广播回游戏公屏 + 同步 QQ 群。
        """
        player = str(data.get("player", "?"))
        text = str(data.get("text", "")).strip()
        # 剥掉唤醒前缀，剩余部分作为 prompt
        prompt = text
        for p in self.mc_wake_prefixes:
            if text.startswith(p):
                prompt = text[len(p):].strip()
                break
        if not prompt:
            prompt = "你好"

        lock = self._player_llm_locks.setdefault(player, asyncio.Lock())
        if lock.locked():
            await self._send_bot_reply("（上一条还在思考中，稍等一下…）", sync_qq=False)
            return

        async with lock:
            try:
                event = self._make_synthetic_event(player)
                umo = event.unified_msg_origin
                prov_id = await self.context.get_current_chat_provider_id(umo)

                # 系统提示词拼装（顺序见 _build_system_parts）：
                # [WebUI 人格（可开关）] + [自定义提示词 + karma 规则] + [管理员上下文]
                system_parts = await self._build_system_parts(
                    umo, player, self._game_is_admin(player), source="game"
                )

                # 附加提示词：来源与管理员名单已由 system_prompt 里的
                # _admin_context 交代（那里有服务器名、适用名单、发起者身份），
                # 这里只补它没说的——这条消息该怎么用工具。
                # mc_karma 永远在工具集里（好感度是强制机制），所以这句是无条件的。
                karma_hint = "调用前先用 mc_karma 查询好感以决定本次 cost。"
                context_hint = (
                    f"<netherlink_context>\n"
                    f"发送者是游戏内的玩家 {player}。\n"
                    "如果玩家想执行 MC 指令（改模式/传送/查名单等），调用 mc_command 工具。"
                    f"{karma_hint}\n"
                    "</netherlink_context>"
                )

                # 会话历史：按玩家挂会话，同一玩家连续对话有记忆
                conv_mgr = self.context.conversation_manager
                curr_cid = await conv_mgr.get_curr_conversation_id(umo)
                if not curr_cid:
                    curr_cid = await conv_mgr.new_conversation(umo)
                conversation = await conv_mgr.get_conversation(umo, curr_cid, create_if_not_exists=True)
                try:
                    history = json.loads(conversation.history) if conversation.history else []
                except (json.JSONDecodeError, TypeError):
                    history = []

                llm_resp = await self.context.tool_loop_agent(
                    event=event,
                    chat_provider_id=prov_id,
                    system_prompt="\n\n".join(system_parts) if system_parts else None,
                    prompt=f"{context_hint}\n\n玩家消息：{prompt}",
                    tools=self._build_mc_toolset(player),
                    contexts=history,
                    max_steps=6,
                )
                reply = (llm_resp.completion_text or "……").strip()[:MC_REPLY_MAX_LEN]

                # 把本轮对话写回会话历史（下次对话带上）
                from astrbot.core.agent.message import AssistantMessageSegment, TextPart, UserMessageSegment
                await conv_mgr.add_message_pair(
                    cid=curr_cid,
                    user_message=UserMessageSegment(content=[TextPart(text=prompt)]),
                    assistant_message=AssistantMessageSegment(content=[TextPart(text=reply)]),
                )

                # 游戏内按模板渲染（含 § 染色），QQ 群同步纯文本回复
                await self._send_bot_reply(reply, sync_qq=True)
            except Exception as e:
                logger.error(f"NetherLink: 游戏内 LLM 对话失败: {e}")
                await self._send_bot_reply("（机器人暂时无法思考，请稍后再试）", sync_qq=False)
            finally:
                self._player_llm_locks.pop(player, None)

    async def _send_bot_reply(self, text: str, sync_qq: bool):
        """向游戏公屏广播机器人回复（MC 端按模板渲染 § 染色），可选同步到 QQ 群。"""
        line = self.templates["bot_reply_game"].replace("{bot}", self.mc_bot_name).replace(
            "{text}", text.replace("§", "&")
        )
        await self._send_to_mc({"type": "bot_reply", "line": line})
        if sync_qq:
            # QQ 端正常输出回复文本（§ 染色码只属于游戏渲染，不同步）
            await self._broadcast(f"[{self.mc_server_name}] {self.mc_bot_name}: {text}")

    def _make_synthetic_event(self, player: str) -> AstrMessageEvent:
        """为游戏内玩家构造一个合成的消息事件（不走消息平台，仅用于 LLM 上下文）。

        sender.user_id 存游戏 ID，使工具闭包能从 event 拿到发起人身份。
        AstrMessageEvent 是抽象基类但无抽象方法，可直接实例化。
        """
        from astrbot.core.platform.astrbot_message import AstrBotMessage, MessageMember
        from astrbot.core.platform.message_type import MessageType
        from astrbot.core.platform.platform_metadata import PlatformMetadata

        msg_obj = AstrBotMessage()
        msg_obj.type = MessageType.OTHER_MESSAGE
        msg_obj.self_id = "netherlink_mc"
        msg_obj.sender = MessageMember(user_id=player, nickname=player)
        msg_obj.message_str = ""
        platform_meta = PlatformMetadata(
            name="netherlink_mc", description="NetherLink MC 端合成事件", id="netherlink_mc"
        )
        return AstrMessageEvent(
            message_str="", message_obj=msg_obj,
            platform_meta=platform_meta, session_id=player,
        )

    # ------------------------------------------------------------------
    # QQ 侧内层 agent 的工具（handler 模式）
    # ------------------------------------------------------------------
    async def _inner_mc_command(self, event, cmd: str, cost: int) -> str:
        """QQ 侧内层 agent 用的 mc_command handler。

        不能复用 self.mc_command——那是被 @filter.llm_tool 装饰过的版本：
        注册器对它的参数名与注解另有要求，且它走的是 AstrBot 自己的注册调用，
        不接受显式 ToolSet 的 handler(event, **kwargs) 约定。

        入参与顶层 mc_command 逐字相同（source="qq" + 真实 QQ 号），
        这样好感度键仍是 qq:{QQ号}、发起人身份仍是 QQ 昵称。
        """
        try:
            qq = str(event.get_sender_id() or "")
            return await self.exec_command_for(
                initiator=str(event.get_sender_name() or qq),
                cmd=cmd,
                source="qq",
                qq=qq,
                cost=cost,
            )
        except Exception as e:
            logger.error(f"NetherLink: 内层 mc_command 执行失败: {e}")
            return f"执行出错: {e}"

    async def _inner_mc_karma(self, event, delta: int) -> str:
        """QQ 侧内层 agent 用的 mc_karma handler。

        与顶层 mc_karma 同口径：目标固定为发起者（QQ 群友），无法指定他人，
        因此键空间是 qq:{QQ号}，与游戏侧 mc:{游戏ID} 互不干扰。
        """
        try:
            qq = str(event.get_sender_id() or "")
            key = identity_key("qq", str(event.get_sender_name() or qq), qq)
            if not int(delta or 0):
                return f"当前好感值：{await self._karma_get(key)}（范围 -50~100）。"
            old, new = await self._karma_add(key, int(delta), origin="QQ 对话")
            return f"好感值 {old} → {new}（本次 {int(delta):+d}）。"
        except Exception as e:
            logger.error(f"NetherLink: 内层 mc_karma 执行失败: {e}")
            return f"执行出错: {e}"

    def _build_qq_toolset(self, event):
        """构造 QQ 侧内层 agent 的工具集。

        这里**必须**是 handler 模式的 FunctionTool（handler=可调用），因为
        AstrBot 的 ToolSet 执行路径是 `tool.handler(event, **kwargs)`
        （`@filter.llm_tool` 注册的工具经 `_PermissionGuardedTool` 走的也是这一条）。
        游戏侧那套 `@dataclass class X(FunctionTool)` + 覆写 `call()` 属于另一个
        执行路径（call 模式，第一个参数是 ContextWrapper），把那种实例塞进本
        ToolSet 会因为没有 handler 而在真机上失败——本机跑不出来。

        event 参数**当前完全未使用**（下方构造不读它）。真实事件是在 ToolSet 执行
        时才由框架传给 handler 的，构造期拿不到也不需要；身份解析一律留在 handler
        里做（那才是 handler 约定的位置，挪到这里反而会把约定弄坏）。
        保留该形参只为与调用点 `self._build_qq_toolset(event)` 对称，不暗示任何
        隐藏语义（`_build_mc_toolset` 那边没有对应的形参，别照抄）。
        """
        from astrbot.core.agent.tool import FunctionTool, ToolSet

        inner_cmd = FunctionTool(
            name="mc_command",
            description=self.mc_command_tool_desc,
            parameters={
                "type": "object",
                "properties": {
                    "cmd": {
                        "type": "string",
                        "description": "完整的 Minecraft 指令，不带开头斜杠",
                    },
                    "cost": {
                        "type": "number",
                        "description": "本次执行消耗的好感值，由你按好感度规则决定；纯查询类传 0",
                    },
                },
                "required": ["cmd", "cost"],
            },
            handler=self._inner_mc_command,
        )
        # mc_karma 无条件在列：好感度是强制机制，没有任何配置能把它摘掉
        inner_karma = FunctionTool(
            name="mc_karma",
            description=self.karma_tool_desc,
            parameters={
                "type": "object",
                "properties": {
                    "delta": {
                        "type": "number",
                        "description": "好感变化量，正增负减；只查询时传 0",
                    },
                },
                "required": ["delta"],
            },
            handler=self._inner_mc_karma,
        )
        return ToolSet([inner_cmd, inner_karma])

    def _build_mc_toolset(self, player: str):
        """构造游戏内会话用的 mc_command 工具集合。

        只有游戏侧这一个调用面，发起人固定为 player（游戏 ID）——QQ 侧走的是
        `@filter.llm_tool` 注册的顶层 mc_command 与 `_build_qq_toolset`，
        与这里无关（早先这里有个永远传不到 "qq" 的 source 形参，已删除）。
        call() 覆写模式第一个参数是 ContextWrapper[AstrAgentContext]。
        """
        from astrbot.core.agent.tool import FunctionTool, ToolSet
        from pydantic import Field
        from pydantic.dataclasses import dataclass

        plugin = self

        @dataclass
        class McCommandTool(FunctionTool):
            """游戏侧 mc_command：发起人固定为 player，cost 由 AI 报价。"""

            name: str = "mc_command"
            description: str = plugin.mc_command_tool_desc
            parameters: dict = Field(
                default_factory=lambda: {
                    "type": "object",
                    "properties": {
                        "cmd": {
                            "type": "string",
                            "description": (
                                "完整的 Minecraft 指令，不带开头斜杠，"
                                "例如 'list' 或 'gamemode creative Steve'"
                            ),
                        },
                        "cost": {
                            "type": "number",
                            "description": "本次执行消耗的好感值，由你按好感度规则决定；纯查询类传 0",
                        },
                    },
                    "required": ["cmd", "cost"],
                }
            )

            async def call(self, context, **kwargs) -> str:
                cmd = str(kwargs.get("cmd", "")).strip().lstrip("/")
                # cost 原样透传：裁剪与扣费都在 exec_command_for 里做（唯一入口）
                return await plugin.exec_command_for(
                    initiator=player,
                    cmd=cmd,
                    source="game",
                    cost=kwargs.get("cost", 0),
                )

        @dataclass
        class McKarmaTool(FunctionTool):
            """游戏侧 mc_karma：读写本地好感表里当前对话玩家的那一份。

            与顶层 mc_karma 同口径——目标是当前发起者，无法指定其他玩家，
            因此没有 player 参数（否则 LLM 能给别人刷好感）。
            """

            name: str = "mc_karma"
            description: str = plugin.karma_tool_desc
            parameters: dict = Field(
                default_factory=lambda: {
                    "type": "object",
                    "properties": {
                        "delta": {
                            "type": "number",
                            "description": "好感变化量，正增负减；只查询时传 0",
                        },
                    },
                    "required": ["delta"],
                }
            )

            async def call(self, context, **kwargs) -> str:
                try:
                    delta = int(float(kwargs.get("delta", 0)))
                except (TypeError, ValueError, OverflowError):
                    return "错误：delta 必须是数字。"
                key = identity_key("game", player, "")
                if not delta:
                    return f"当前好感值：{await plugin._karma_get(key)}（范围 -50~100）。"
                old, new = await plugin._karma_add(key, delta, origin="游戏内对话")
                return f"好感值 {old} → {new}（本次 {delta:+d}）。"

        # mc_karma 无条件在列（好感度是强制机制，没有能摘掉它的配置）
        return ToolSet([McCommandTool(), McKarmaTool()])

    # ------------------------------------------------------------------
    # 指令执行核心（QQ llm_tool 与游戏内 tool_loop_agent 共用）
    # ------------------------------------------------------------------
    async def _run_console_cmd(self, cmd: str, timeout: float = 8.0) -> Optional[str]:
        """以控制台身份执行一条指令并返回服务器输出文本；失败/超时返回 None。

        不做权限校验（仅供内部工具使用）；扣费与回滚由 exec_command_for 负责。
        """
        if not self._mc_connected():
            return None
        cmd_id = uuid.uuid4().hex
        fut = asyncio.get_running_loop().create_future()
        self._pending_cmds[cmd_id] = fut
        if not await self._send_to_mc({"type": "command", "id": cmd_id, "cmd": cmd.lstrip("/")}):
            # 发送失败必须把登记撤掉，否则这里就是一份永不回执的僵尸 Future——
            # 只有 terminate() 才清得掉，其间 _pending_cmds 一直虚高。
            # 注意 _send_to_mc 返回 False 有两个来源：真的没连上/发送抛异常，
            # 以及"刚判断完就断线"的并发写竞态。竞态下这条 warning 属于误报，
            # 但方向是**多报**（宁可多留一条痕迹，也不放过真实的发送失败），
            # 这是刻意保留的：静默丢掉一条真实失败比多一条无用的 warning 贵得多。
            self._pending_cmds.pop(cmd_id, None)
            return None
        try:
            return await asyncio.wait_for(fut, timeout=timeout)
        except asyncio.TimeoutError:
            self._pending_cmds.pop(cmd_id, None)
            return None

    async def _rollback_karma(
        self, key: str, spend: int, baseline: int, reason: str, cmd: str
    ) -> bool:
        """回滚一次好感扣减，返回是否真正复原。异常绝不外抛。

        `_karma_add` 会吞掉写盘异常（磁盘满/权限）而只记一条通用日志，因此
        「回滚失败」不能靠异常捕获来判断，必须事后验证两件事：
          1. 内存水位回到扣减前（baseline）；
          2. 这次复原真的落到了盘上——`KarmaStore.add` 是先改内存再 `_save()`，
             写盘失败时内存照样是对的，进程内看着没事，但重启后那笔扣减会从
             磁盘复活，玩家等于白付了钱。
        任一验证不过就单独留痕：玩家被白扣且没有自动补救，运维只能靠这条日志
        发现并手工补偿。
        """
        try:
            await self._karma_add(key, spend)
            # 水位校验与落盘校验都必须留在 try 内：本方法承诺「异常绝不外抛」，
            # 而 _karma_get 与 _path 访问理论上仍可能抛。真抛了就会冒泡到
            # exec_command_for 的通用 except，只剩一条无归因的日志，
            # 「玩家被白扣」就失去了唯一可追查的痕迹。
            if await self._karma_get(key) != baseline:
                logger.error(
                    f"NetherLink: 好感回滚失败！[{key}] 被白扣 {spend} 点未退回"
                    f"（{reason}，内存水位未回到 {baseline}）: {cmd}"
                )
                return False
            if self._karma._path is not None:
                try:
                    # 补一次显式落盘：_karma_add 内部那次写盘失败已被它自己吞掉，
                    # 这里让同一个错误浮出来，回滚是否真的持久化才可判定。
                    self._karma._save()
                except Exception as e:
                    logger.error(
                        f"NetherLink: 好感回滚失败！[{key}] 被白扣 {spend} 点未退回"
                        f"（{reason}，写盘失败，重启后扣减会复活）: {cmd} — {e}"
                    )
                    return False
            return True
        except Exception as e:
            logger.error(
                f"NetherLink: 好感回滚失败！[{key}] 被白扣 {spend} 点未退回"
                f"（{reason}）: {cmd} — {e}"
            )
            return False

    async def exec_command_for(
        self, initiator: str, cmd: str, source: str, qq: str = "", cost: int = 0
    ) -> str:
        """执行指令并返回给 LLM 的结果文本。

        本方法不含任何权限判断、不持有指令名单——是否该执行由 AI 依提示词自行决定，
        插件只负责执行与原子化的好感扣费。

        扣费与执行在同一次调用内完成：好感不足则不执行；未连上服务器或执行超时
        则把扣掉的还回去。这样 AI 既跳不过扣费（扣费是执行的必经之路），
        也不会为一次没跑成的执行白付钱。

        source="qq"  : initiator 为发起人显示名（QQ 群昵称），qq 为发起人 QQ
        source="game": initiator 为游戏内玩家名
        cost         : AI 为本次执行报出的好感消耗。报价是外部输入，必经
                       clamp_cost 裁剪（非数字/负数归 0，超过 max_command_cost 按上限）。
                       好感度是强制机制，本方法**永远**按报价计价，没有开关能跳过。
        """
        try:
            if not cmd:
                return "错误：指令为空。"

            spend = clamp_cost(cost, self.max_command_cost)
            # 扣减前的水位，供回滚校验用。spend=0 时不读存储，此值不参与任何判断。
            cur = 0
            key = identity_key(source, initiator, qq)
            if spend:
                cur = await self._karma_get(key)
                if cur < spend:
                    logger.info(
                        f"NetherLink: 好感不足拒绝执行 [{key}] 需要 {spend} 现有 {cur}: {cmd}"
                    )
                    return (
                        f"好感不足：本次需要 {spend}，发起者当前好感为 {cur}，"
                        f"不足以支付，指令未执行。"
                    )
                await self._karma_add(key, -spend)

            # 扣减之后的所有 await 点都可能被取消：terminate() 取消 _pending_cmds 里的
            # Future（_run_console_cmd 里那个 await 直接吃到 CancelledError），卸载时
            # 框架也会取消本任务。CancelledError 自 3.8 起是 BaseException，下面那个
            # `except Exception` 接不住它——不接管的话，这笔已扣的好感既没换来执行、
            # 也不留任何痕迹，是唯一一条「扣了钱且无迹可查」的路径。
            try:
                if not self._mc_connected():
                    if spend and not await self._rollback_karma(
                        key, spend, cur, "MC 未连接", cmd
                    ):
                        return (
                            f"MC 服务器当前不在线，指令未执行；好感回滚亦失败，"
                            f"已扣的 {spend} 点未退回。"
                        )
                    return "MC 服务器当前不在线，无法执行指令。"

                output = await self._run_console_cmd(cmd)
                # None 与 "" 是两种不同的结局，绝不能折叠：
                #   None -> 8 秒内没有回执（执行失败）-> 回滚
                #   ""   -> 服务器执行了但无输出（成功）-> 照常收费
                if output is None:
                    if spend:
                        reason = "指令超时无回执"
                        if not await self._rollback_karma(key, spend, cur, reason, cmd):
                            return (
                                f"指令已发送，但 8 秒内未收到服务器回执；"
                                f"好感回滚亦失败，已扣的 {spend} 点未退回。"
                            )
                        logger.error(
                            f"NetherLink: 指令执行失败已回滚好感 {spend} [{key}]: {cmd}"
                        )
                    return "指令已发送，但 8 秒内未收到服务器回执（可能仍在执行），好感未扣除。"

                if spend:
                    if output:
                        return f"指令已执行（消耗好感 {spend}）。服务器输出：\n{output}"
                    return f"指令已执行（消耗好感 {spend}，无返回输出）。"
                if output:
                    return f"指令已执行。服务器输出：\n{output}"
                return "指令已执行（无返回输出）。"
            except asyncio.CancelledError:
                # 取消也要把钱退回去、并留痕；然后**重新抛出**——吞掉取消会让
                # 取消方（terminate 的调用者 / 框架卸载流程）永远等不到本任务结束。
                await self._refund_on_cancel(key, spend, cur, cmd)
                raise
        except Exception as e:
            logger.error(f"NetherLink: 执行指令失败: {e}")
            return f"执行出错: {e}"

    async def _refund_on_cancel(self, key: str, spend: int, cur: int, cmd: str) -> None:
        """取消路径的退费；异常绝不外抛（调用方紧接着要 re-raise，不能被挡住）。

        幂等：只在好感水位仍停在「扣减后」（cur - spend）时才退。取消可能落在
        `_rollback_karma` 内部的任意 await 点上（比如已经改完内存、正要去校验水位），
        此时再退一次就是白送好感；反过来，水位对不上也说明这笔钱压根没扣成。
        退费失败由 `_rollback_karma` 自己留痕，这里只负责不让它把异常带出去。
        """
        if not spend:
            return
        try:
            if await self._karma_get(key) != cur - spend:
                logger.info(
                    f"NetherLink: 指令被取消，但好感水位不在扣减后（应为 {cur - spend}），"
                    f"判定已退费或未扣成，跳过重复退费: {cmd}"
                )
                return
            if await self._rollback_karma(key, spend, cur, "任务被取消", cmd):
                # 成功退费同样要留痕：这条 warning 是"这笔钱动过又还回去了"的唯一记录，
                # 出问题时运维靠它把"取消"与"从没扣过"区分开（不留痕 = 无迹可查）
                logger.warning(
                    f"NetherLink: 指令被取消（未执行），已退回好感 {spend} [{key}]: {cmd}"
                )
        except asyncio.CancelledError:
            logger.error(
                f"NetherLink: 指令被取消时好感退费过程被中断（是否退回未知）！"
                f"[{key}] 涉及 {spend} 点: {cmd}"
            )
            raise
        except Exception as e:
            logger.error(f"NetherLink: 指令取消后退费失败 [{key}] 涉及 {spend} 点: {cmd} — {e}")

    async def _broadcast(self, text: str):
        """向所有绑定群推送文本。"""
        for group in self.target_groups:
            umo = f"aiocqhttp:GroupMessage:{group}"
            try:
                await self.context.send_message(umo, MessageChain().message(text))
            except Exception as e:
                logger.error(f"NetherLink: 推送到群 {group} 失败: {e}")

    async def _send_to_mc(self, payload: dict) -> bool:
        if not self._mc_connected():
            logger.warning("NetherLink: MC 服务器未连接，消息丢弃")
            return False
        try:
            await self._mc_ws.send_str(json.dumps(payload, ensure_ascii=False))
            return True
        except Exception as e:
            logger.error(f"NetherLink: 发送到 MC 失败: {e}")
            return False

    # ------------------------------------------------------------------
    # QQ 群消息 -> MC
    # ------------------------------------------------------------------
    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def on_group_message(self, event):
        """绑定群的普通消息转发进游戏公屏。"""
        try:
            if not self.config.get("enable_qq_to_mc", True):
                return
            group_id = str(event.get_group_id() or "")
            if group_id not in self.target_groups:
                return
            # 机器人自身消息（OneBot self_id == user_id）默认不回传 MC（防循环），
            # 开启 sync_bot_msgs 后机器人消息也按模板转发进游戏
            self_id = str(event.get_self_id() or "") if hasattr(event, "get_self_id") else ""
            if self_id and str(event.get_sender_id() or "") == self_id:
                if not self.config.get("sync_bot_msgs", False):
                    return
            text = (event.message_str or "").strip()
            if not text:
                return  # 纯图片/@ 等暂不转发
            # 群名：配置里优先，未配置退回群号；QQ 消息里的 § 码剥离防止伪造染色
            group_name = self.group_names.get(group_id, group_id)
            clean_text = text.replace("§", "&")
            # 模板在 Python 侧渲染完成（含 § 染色码），MC 端直接渲染文本组件
            line = (
                self.templates["qq_to_mc"]
                .replace("{group}", group_name)
                .replace("{sender}", str(event.get_sender_name() or "?"))
                .replace("{text}", clean_text)
            )
            await self._send_to_mc({"type": "chat", "line": line})
        except Exception as e:
            logger.error(f"NetherLink: QQ->MC 转发失败: {e}")

    # ------------------------------------------------------------------
    # LLM 函数工具：QQ 群聊自然语言 -> MC 指令（AstrBot 自动注册给群聊 LLM）
    # ------------------------------------------------------------------
    @filter.llm_tool(name="mc_command")
    async def mc_command(self, event, cmd: str, cost: int) -> str:
        """在 Minecraft 服务器上以控制台身份执行一条指令，并返回服务器真实输出。

        执行前请自行判断这条指令是否属于高危操作（如改游戏模式、传送他人、给予物品、
        封禁、op、清空区域等）。高危操作应审慎处理。
        调用本工具后，系统可能要求你进一步确认消耗与权限；如需判断发起者身份，
        以对话中提供的 <netherlink_context> 为准（若未提供，则按普通玩家对待）。

        Args:
            cmd(string): 完整的 Minecraft 指令，不带开头的斜杠，例如 "gamemode creative Steve"
            cost(number): 你为本次执行报出的好感消耗（纯查询类指令传 0）。最终消耗由掌握好感度规则的内层决策按规则确定
        """
        try:
            # 注意：这里**不再**因 MC 未连接而提前返回。好感度是本地功能，
            # 服务器离线时玩家/群友仍应能对话、查好感、加减好感；离线只让
            # 指令执行本身失败（exec_command_for 会拒绝并如实告知、并退费）。
            # 提前拦截会把整个 LLM 挡在门外，连带废掉好感度。
            qq = str(event.get_sender_id() or "")
            identity = str(event.get_sender_name() or qq)

            # 永远起内层 agent：AstrBot 主 agent 的 system_prompt 由框架内建，
            # 插件注入不了好感度规则与管理员名单，只能另起一个带自定义
            # system_prompt 的 agent 来承载本次决策。`cost` 是必填参数，AI 必须
            # 报价，而报价的依据（karma_rules）只有内层看得到——没有"跳过决策"的分支。
            # event 必须是**真实 QQ 事件**：is_admin()、unified_msg_origin、
            # 群会话与 conversation manager 全挂在它身上，合成事件会让它们全错。
            umo = event.unified_msg_origin
            prov_id = await self.context.get_current_chat_provider_id(umo)
            if not prov_id:
                return "未配置可用的 LLM 提供商，无法执行指令。"

            # 外层 agent（`@filter.llm_tool` 注册的那个面）拿不到好感度规则，它报的
            # cost 本身没有依据。但这个参数在 schema 里是必填的：模型必须给出一个值，
            # 如果就此丢掉，用户听到的价格与实际扣除的价格会来自两套互不相干的计算。
            # 所以把它当作「外层给你的初步报价」原样带进内层提示词——内层掌握规则，
            # 采纳/调整/推翻都由它定，最终价格与 exec_command_for 扣的是同一个数。
            # 过一遍 clamp_cost 只为把外部输入洗成合法整数（越界报价按上限显示）。
            quoted = clamp_cost(cost, self.max_command_cost)
            resp = await self.context.tool_loop_agent(
                event=event,
                chat_provider_id=prov_id,
                system_prompt="\n\n".join(
                    await self._build_system_parts(
                        umo, identity, self._qq_is_admin(event), "qq"
                    )
                ),
                prompt=(
                    f"QQ 群友「{identity}」请求在 Minecraft 服务器上执行这条指令：\n"
                    f"{cmd}\n"
                    f"外层决策给出的初步报价是 {quoted} 点好感。外层看不到好感度规则，"
                    "这个数字只是它的猜测，仅供参考。\n"
                    "请按好感度规则自行判断这条指令该不该执行、要消耗对方多少好感"
                    "（采纳、调整或推翻上面的报价都可以），"
                    "然后调用 mc_command 执行（cmd 传原指令，cost 传你决定的消耗）。"
                    "如果判断不该执行，直接说明原因，不要调用工具。"
                ),
                tools=self._build_qq_toolset(event),
                # 内层 agent 只需「查好感、报价、执行」三步，给一步余量即可，
                # 绝不能让它在此处循环
                max_steps=4,
            )
            # 「LLM 没给出文本」与「响应里根本没有这个字段」是两件事：
            # 前者是正常结果，静默回退兜底文案即可；后者意味着 AstrBot 的响应
            # 结构变了，继续静默就等于让插件永远假装正常。必须留痕。
            if hasattr(resp, "completion_text"):
                text = str(resp.completion_text or "").strip()
            else:
                logger.warning(
                    "NetherLink: 内层 agent 响应缺少 completion_text 字段"
                    "（AstrBot 响应结构可能已变），本次使用兜底回复"
                )
                text = ""
            return text[:MC_REPLY_MAX_LEN] if text else "指令处理完毕。"
        except Exception as e:
            logger.error(f"NetherLink: mc_command 执行失败: {e}")
            return f"执行出错: {e}"

    @filter.llm_tool(name="mc_karma")
    async def mc_karma(self, event, delta: int) -> str:
        """查询或增减玩家好感度，目标是当前对话的发起者。

        delta 为 0 时只查询，为正数时增加好感，为负数时扣除好感。
        每次与玩家对话时先调用本工具（delta=0）查看当前好感值，再据此调整说话方式和态度；
        玩家请求执行指令时，先判断其剩余好感是否足以支付该指令的消耗。

        Args:
            delta(number): 好感变化量，正增负减；只查询时传 0
        """
        try:
            qq = str(event.get_sender_id() or "")
            key = identity_key("qq", str(event.get_sender_name() or qq), qq)
            if not int(delta or 0):
                return f"当前好感值：{await self._karma_get(key)}（范围 -50~100）。"
            old, new = await self._karma_add(key, int(delta), origin="QQ 对话")
            return f"好感值 {old} → {new}（本次 {int(delta):+d}）。"
        except Exception as e:
            logger.error(f"NetherLink: mc_karma 执行失败: {e}")
            return f"执行出错: {e}"

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------
    async def terminate(self):
        """插件卸载/停用时清理：关闭 WS、取消挂起的指令、好感度落盘。"""
        if self._runner:
            await self._runner.cleanup()
        if self._mc_ws and not self._mc_ws.closed:
            await self._mc_ws.close()
        for fut in self._pending_cmds.values():
            if not fut.done():
                fut.cancel()
        self._pending_cmds.clear()
        # 好感度落盘：_karma_add 每次改动已经写过盘，这里兜的是「写盘失败/
        # 降级态下 path=None」的情形——path 为 None 时 _save 本就是空操作，
        # 这里再判一次是为了不依赖 store 的内部实现。失败只记日志，不阻断卸载。
        try:
            if self._karma._path is not None:
                self._karma._save()
        except Exception as e:
            logger.error(f"NetherLink: 退出时落盘好感度失败: {e}")
        logger.info("NetherLink 已卸载")
