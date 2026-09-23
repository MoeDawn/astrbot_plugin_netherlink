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
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import aiohttp
from aiohttp import web

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.star import Context, Star
from astrbot.core.utils.astrbot_path import get_astrbot_plugin_data_path

# ⚠️ 这个导入**必须放顶层**，且必须在插件加载期真的执行到。
# `mc_platform` 用 `@register_platform_adapter` 把适配器类注册进
# `platform_cls_map`——AstrBot 是「先加载插件、后初始化平台」，
# 靠的正是这期间注册进去的类型。若改成函数内惰性导入，注册就发生得太晚：
# 配置项明明在，框架却查不到该 type 的类，只会记一条「Platform adapter
# not found」然后跳过（2026-09-22 实测踩到）。
try:
    from . import mc_platform  # noqa: F401
except ImportError:  # 插件以顶层模块方式加载时
    import mc_platform  # noqa: F401

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

# 游戏侧在 AstrBot 里的**平台标识**。
# 唯一来源：平台适配器（迁移后）、合成事件、注入钩子的分流判据、钩子内的 umo 拼装
# 都读这一个常量——散落的字面量一旦对不上，症状是「注入静默不生效」或
# 「回复发不出去」，两者都不报错。Guard: test_game_platform_id_has_single_source。
GAME_PLATFORM_ID = "netherlink_mc"

# 插件自身的名字，必须与 metadata.yaml 的 `name` 逐字一致。
# 用途：on_plugin_loaded 钩子对**每个**插件加载都触发，靠它认出「这次加载的
# 是不是我」。写错不会报错，只会让重载后的重绑静默失效。
PLUGIN_NAME = "astrbot_plugin_netherlink"


def _game_umo(server_id: str) -> str:
    """游戏侧某台服务器的 unified_msg_origin。**一台服务器 = 一个会话**。

    用 GROUP_MESSAGE 而不是 FRIEND_MESSAGE：会话在语义上就是一个「群」
    （该服所有玩家在里面说话），且 GROUP 能让 `unique_session` 的
    按玩家隔离逻辑不生效——那个 builder 只认内置平台名，认不出我们。
    第三段用 server_id，与旧的自建 agent（session_id=server_id）保持一致，
    这样迁移前后是同一个会话，历史不会断。
    """
    return f"{GAME_PLATFORM_ID}:GroupMessage:{server_id}"


@dataclass(frozen=True)
class CmdResult:
    """一次控制台指令执行的结局。

    `ok=False` 表示**服务器明确回了失败**（Paper 侧捕获到异常，或 dispatchCommand
    返回 false），与 ok=True 但输出为空是两回事：

      ok=True  + output=""   -> 执行成功，只是没输出（照常收费）
      ok=True  + output=文本 -> 执行成功（照常收费）
      ok=False               -> 明确失败，**必须退费**
      （没有回执）            -> 超时，由 _run_console_cmd 返回 None 表示，同样退费

    早先 `_run_console_cmd` 只返回 Optional[str]，把「明确失败」和「执行成功」一起
    塞进带输出的字符串里，调用点无从分辨，于是失败也照扣好感。
    """

    ok: bool
    output: str


@dataclass
class McConn:
    """一台已握手 MC 服务器的连接。

    多服并存的核心数据结构：以前只有一个 `self._mc_ws` 字段，第二台服务器
    一连入就会把第一台踢掉（且两端各自重连，形成永久互踢）。现在按 server_id
    分别保存，同 id 才是「重连替换」，不同 id 并存。
    """

    ws: object          # aiohttp.web.WebSocketResponse
    port: int           # 它连入的本地端口（路线 A 里端口即身份来源）
    reported_name: str  # 握手上报的 server-name（仅作标识，不作显示名）

    @property
    def closed(self) -> bool:
        try:
            return bool(getattr(self.ws, "closed", True))
        except Exception:
            return True

# 好感度规则提示词默认值（AI 据此自主决定好感增减；指令的执行尺度见 mc_command 相关项）
DEFAULT_KARMA_RULES = """[好感度规则]
你和群友/玩家之间存在一个好感值,初始值 {karma_initial},最低 {karma_min},最高 {karma_max}.
你和对方对话时,根据此数值来改变说话方式和态度.
对方获得成就,和你进行正常/友善的对话,你可以调用好感度工具增加此数值;
对方发表不友好言论,你也可以调用好感度工具扣除好感.
以上行为造成的好感变化每次不超过 +-2,如果因为对话导致好感度变化,你会隐晦地暗示.
互动涉及好感度时,调用 mc_karma.
不要暴露好感值这一存在:不报数字,也不要说自己在查询或增减某个数值;要表达态度变化时,只用语气与措辞自然体现,例如更冷淡或更亲近."""

# mc_command 工具描述默认值。只留**简短说明 + 一律拒绝类**：
# 拒绝类必须在 AI 决定用不用工具之前就被看到，所以留在描述里；
# 价目表与「免费/可执行/超标计价」属于「怎么报价」，放进 cost 参数说明
# （2026-09-22 起是 full 模式：每轮全量下发，价差表也看得见。）
#
# ⚠️ 本描述同时挂给**两个面**：QQ 侧顶层 `@filter.llm_tool` 注册的 mc_command、
# 游戏侧同一份描述（2026-09-22 起游戏侧也走全局工具表）。
# 两面通常都拿得到 `<netherlink_context>`，但注入有若干不生效的边界（非
# aiocqhttp 平台、私聊、钩子抛错被吞），所以这里绝不能断言「名单见
# netherlink_context」——读不到的读者找不到名单，会把管理员的合法请求当成
# "非管理员"直接拒掉。只描述**场景**，把名单的来源写成条件式，两个面读起来
# 才都是真的。
DEFAULT_MC_COMMAND_TOOL_DESC = """\
在 Minecraft 服务器上以控制台身份执行一条指令,并返回服务器真实输出.
执行前先按下述情形判断:
一律拒绝的指令(无论对方好感多少都不执行):清空或重置成就进度这类能让对方
反复刷成就的,召唤末影龙/凋零等明显影响服务器的高危指令,破坏他人建筑,
清空区域,封禁他人等伤害其他玩家的指令.另外,
刷怪笼(spawner)原版无法获得的方块,与各类刷怪蛋(spawn_egg)一律不给玩家,玩家头颅除外.
遇到这类请求直接说明原因并拒绝,不要调用本工具.
其余情形按 cost 参数的说明报价与执行.
好感扣除由本工具自己完成:你在 cost 里报出价格,插件会在同一次调用内原子扣减
(不足则拒绝,执行失败则退回).不要用 mc_karma 再扣一次——mc_karma 只用于
对话性的好感增减,不用于支付指令费用.
若当前发起者是管理员(身份见系统提示词中的 <netherlink_context>),你可以给他更宽松的尺度:
对他的高危指令更倾向于放行,好感不足时也可以通融;但放行与否仍由你按指令本身和当前情境判断,不是无条件执行."""

# mc_karma 工具描述默认值
DEFAULT_KARMA_TOOL_DESC = """\
查询或增减当前对话发起者的好感度,无法指定其他玩家.
delta 为 0 时只查询,为正数时增加好感,为负数时扣除好感.
增减依据见好感度规则;执行指令要消耗多少,见 mc_command 的描述."""

DEFAULT_MC_COMMAND_COST_PARAM_DESC = """\
本次执行消耗的好感值,由你按指令的 OP 程度决定.
一,免费放行(传 0):纯查询类,如 list,time,seed,who,help;对方自己传送到自己附近的位置.
二,可以执行(正常收费):生成无害生物取乐,给发起者本人的普通物品,
以及其他不影响服务器秩序,不破坏他人体验的趣味指令.
参考价目:
自己 tp 到其他玩家附近 0;把其他玩家 tp 到自己附近 1;3 个铁锭 1;
钻石或金苹果 5;踢出(不是封禁)玩家 5;
tp 到附近村庄,樱花树林这类需要定位+传送+有价值的地点,按稀有度 10 起;
高级物品(龙蛋,下界之星,鞘翅)10 起;
原版无法获得但不危害游戏的物品(超出附魔上限的附魔,指定数值的生物等)15 起.
三,超规格物品按超标程度计价:给出属性或附魔超出原版正常范围的物品时,
超标越多收费越高;轻微超标按普通物品价格上浮,严重超标(如超出附魔上限数倍)
应从高报价,对方好感不足就拒绝.完全超标(远超原版上限,明显不该存在于正常
游戏中的物品)则直接拒绝执行,不要调用 mc_command.
以上仅为参考,其他指令的消耗自行判断;若本次消耗大于对方剩余好感,
你会拒绝执行并隐晦地透露原因.
注意:部分指令(tp 传送/kill 等)服务器执行后不会返回任何输出,这是正常的,
不要因为没看到输出就以为失败或重复执行."""

DEFAULT_KARMA_DELTA_PARAM_DESC = """\
好感变化量,正增负减;只查询时传 0"""

DEFAULT_MC_COMMAND_CMD_PARAM_DESC = """\
完整的 Minecraft 指令,不带开头的斜杠,例如 list 或 gamemode creative Steve"""

# 只有 QQ 侧有 server 参数（游戏侧来源由连接唯一确定，没有让 AI 选的余地）
DEFAULT_MC_COMMAND_SERVER_PARAM_DESC = """\
指令发往哪台 MC 服务器(填 server_id 或显示名).只有一台在线时可省略"""

# 上面两个原先**硬编码**在两侧（游戏侧 ToolSet 与顶层 docstring 的 Args 段），
# 措辞还不一样——「同一段文本多处副本必然漂移」的典型。收成配置项后两侧
# 共用一份默认值，用户也终于改得动。

# 玩家获得成就时发给 AI 的提示词默认值
DEFAULT_ADVANCEMENT_PROMPT = '系统通知:服务器[{server}]记录到玩家[{player}]达成了成就[{advancement}].这不是玩家对你说的话,而是服务器派给你的一个任务:请据此更新对该玩家的好感值,并发表看法.但不要暗示好感度的变化.好感增量按成就难度决定:越难获得的成就给得越多,范围 2 到 10.普通采集与探索类成就偏下限,稀有,危险或需要大量时间的成就偏上限.'

# 游戏内对话附加提示词默认值，{server} 会替换为服务器名
# 没有为某台服务器配 `server_display_names` 时的兜底显示名。
# 以前这一项是可配置的（mc_server_name），但它只能填一个值，多服下没有意义——
# 现在固定为 "MC"，与 schema 的 hint 一致（「留空则显示为 [MC]」）。
DEFAULT_SERVER_DISPLAY = "MC"

# 游戏侧注入给 AI 的系统上下文模板（**与 QQ 侧对称**）。
# 占位符：{identity} 当前发起者、{is_admin} 是/不是管理员（已含结尾句号）、
#        {server} 服务器显示名。
# 2026-09-22 由 extra_system_prompt 改名而来——身份行从代码生成改为模板承载，
# 用户就能在配置界面里看到并修改它（与 QQ 侧那份同等可配）。
#
# ⚠️ 外层的 <netherlink_context> 标签**不能删**：mc_command 的工具描述里写着
# 「身份见系统提示词中的 <netherlink_context>」。游戏侧少了它，AI 在系统提示词
# 里找不到该标签，就会把管理员当普通玩家——第五个坑（同名标签互相遮蔽）的
# 后遗症会原地复发。
#
# ⚠️ 末尾「先查一次好感」那句也是游戏侧专属，别顺手删掉：游戏侧会话按**服务器**
# 建、玩家进出频繁，每轮都该查；QQ 侧刻意不查（主 agent 有会话记忆，每轮强制
# 查会过频）。
DEFAULT_NETHERLINK_CONTEXT_GAME = """<netherlink_context>
你当前处于一个我的世界服务器内,服务器名称为{server}.
当前发起者:{identity},{is_admin}
玩家在游戏公屏里对你说的话,群名与玩家 ID 由 AstrBot 自行附带,不需要你复述.
玩家试图或暗示要给你东西时(例如"我给你金锭"),不要仅凭这句话就道谢并加好感——
东西还没真的到你手上.严格按以下顺序操作,不得跳过任何一步:
第一步,先用 data get entity <玩家ID> Inventory[{id:"minecraft:物品ID"}] 查看对方背包,
确认里面确实有所说的物品,以及实际有几个.
第二步,只清理对方明确说出的那个数量,例如对方说给 3 个钻石,
就执行 clear <玩家ID> minecraft:diamond 3.
绝不能不带数量执行 clear,那会清空该物品的全部.
第三步,物品确认取走后,才按物品价值调用好感度工具增加好感,
并在回话里体现出东西已经拿到.
若第一步没搜到该物品,直接告诉对方背包里没有,不要执行 clear,也不要加好感.
对方只是口头说说,没走到第二步时,不要当作已经收到了东西.
回复之前,先调用 mc_karma(delta 传 0)查一次当前发起者的好感值,再据此决定说话方式与态度.
</netherlink_context>"""

# QQ 侧注入给 AI 的系统上下文模板（**游戏侧不用它**）。
# 游戏侧那份并入「游戏内自定义提示词」，身份由代码生成——见 _admin_context。
# 占位符：{identity} 当前发起者、{is_admin} 是/不是管理员（已含结尾句号）。
# （{origin} / {roster} 已于 2026-09-22 删除，见 _admin_context_values）
DEFAULT_NETHERLINK_CONTEXT_QQ = """\
<netherlink_context>
当前发起者:{identity},{is_admin}
</netherlink_context>"""

# 机器人游戏内名字的兜底值。抽成常量只为让下面的配置读取有个单一来源，
# 便于与别处的默认值对齐。
# 那句文案现在不再提及机器人名）。
DEFAULT_BOT_NAME = "ai"




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


class NetherLinkPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config

        # ---- 配置解析 ----
        self.ws_host: str = config.get("ws_host", "0.0.0.0")
        # 多端口监听：**一台服务器占一个端口**，单服就只填一个。
        # 以前还有一个只能填单个端口的 `ws_port`，它是本项的子集，
        # 多服务器功能落地后已删除（用户 2026-09-19 确认）。
        # 形如 "8765,8766"（server-name 取 MC 端上报值）
        # 或   "survival:8765,creative:8766"（显式指定 server-name，推荐）
        self.ws_bindings: list = self._parse_ws_ports(config.get("ws_ports", ""))
        self.auth_token: str = config.get("auth_token", "")
        self.target_groups: set[str] = self._parse_csv(config.get("target_groups", ""))
        self.admin_qq: set[str] = self._parse_csv(config.get("admin_qq", ""))
        # 游戏内机器人唤醒词（前缀），默认与 QQ 侧唤醒一致
        self.mc_wake_prefixes: list[str] = [
            p.strip() for p in str(config.get("mc_wake_prefixes", "ai,助手")).split(",") if p.strip()
        ]
        # 名称与占位符：模板里可用 {server}/{group}/{bot} 分别替换为
        # 服务器名/群名/机器人游戏内名字。
        # 按服务器区分显示名：server_id -> 显示名；没配的服务器回退
        # DEFAULT_SERVER_DISPLAY（"MC"）。以前这里还有一个只能填一个值的
        # `mc_server_name`，多服务器功能落地后已删除（用户 2026-09-19 确认）。
        # 显示名始终由本插件决定——MC 端只上报身份（server_id），不决定显示成什么。
        self.server_display_names: dict = self._parse_group_names(
            str(config.get("server_display_names", "") or "")
        )
        self.mc_bot_name: str = str(
            config.get("mc_bot_name", DEFAULT_BOT_NAME) or DEFAULT_BOT_NAME
        )
        self.group_names: dict[str, str] = self._parse_group_names(
            str(config.get("group_names", "") or "")
        )
        # 游戏侧对话的系统提示词拼装：
        # [WebUI 人格（可开关）] + [自定义提示词] + [karma 好感规则（始终注入）]
        self.enable_webui_persona: bool = bool(config.get("enable_webui_persona", True))
        # 游戏侧改走**原生平台适配器**（见 mc_platform.py）。
        # 默认 **False**：该路径需要 AstrBot 重启一次才会实例化适配器，
        # 首次打开开关就期待它工作会得到一个静默的「没有回复」。
        # 打开后若适配器仍未实例化，_handle_bot_chat 会**响亮地**记一条 ERROR
        # 并回执玩家（自建 agent 已于 2026-09-22 删除，没有第二条路可退）。
        self.enable_game_platform: bool = bool(config.get("enable_game_platform", True))
        # 两份上下文模板都**空串回退默认值**——口径与其他提示词一致：
        # 想「不注入任何内容」应当删掉模板里的正文，而不是清空配置项
        # （清空会让默认值静默回来，用户以为关掉了其实没有）。
        # ⚠️ 2026-09-22 之前 extra_system_prompt 是「留空=不附加」的另一套口径，
        # 合并成 template_netherlink_context_game 后统一了。
        self.netherlink_context_qq: str = str(
            config.get("template_netherlink_context_qq", DEFAULT_NETHERLINK_CONTEXT_QQ)
            or DEFAULT_NETHERLINK_CONTEXT_QQ
        )
        # 游戏侧上下文模板。空串回退默认值（口径与其他提示词一致：想「不注入」
        # 应当删掉模板内容，而不是清空配置项——清空会让默认值静默回来）。
        self.netherlink_context_game: str = str(
            config.get("template_netherlink_context_game", DEFAULT_NETHERLINK_CONTEXT_GAME)
            or DEFAULT_NETHERLINK_CONTEXT_GAME
        )
        # 好感度是强制机制，没有开关：想「不依赖好感度」只能改提示词。
        # ⚠️ 消耗表 2026-09-21 起已不在本项里，而移到了 mc_command 的
        # cost 参数说明（mc_command_cost_param_desc）——改这里的消耗表**不再生效**。
        # 好感度参数
        # ⚠️ **范围可配**（karma_min / karma_max，2026-09-23 新增）。
        # 此前范围硬编码在 karma.py 的 KARMA_MIN/KARMA_MAX：用户在 karma_rules
        # 提示词里把范围改大，代码却照样裁到 -50~100，且**完全静默**。
        # 现在范围由配置决定，karma_rules 里的数字由插件按配置渲染
        # （占位符 {karma_min} / {karma_max}），两者不会再脱节。
        #
        # 归一规则：先各自兜底成整数，再**强制 lo < hi**（倒置区间会让
        # clamp_value 恒返回 lo，等于把所有人的好感钉死，必须挡住）。
        self.karma_min: int = self._read_int(config, "karma_min", -50)
        self.karma_max: int = self._read_int(config, "karma_max", 100)
        if self.karma_min >= self.karma_max:
            logger.warning(
                f"NetherLink: karma_min({self.karma_min}) >= karma_max({self.karma_max})，"
                f"区间非法，已回退默认 -50~100"
            )
            self.karma_min, self.karma_max = -50, 100
        # karma_initial 必须过 clamp_value：KarmaStore.get 在"无记录"路径上把
        # initial 原样返回（只有已存的值才走 clamp_value），配置里填 500 会让所有
        # 新玩家拿到越界的初始好感，并被 AI 当作事实报出去。
        # OverflowError 一并兜底：JSON 里的 1e400 解析成 inf，int(inf) 抛的是
        # OverflowError 而不是 ValueError，漏掉会直接崩掉插件加载。
        try:
            self.karma_initial: int = clamp_value(
                int(config.get("karma_initial", 20)), self.karma_min, self.karma_max
            )
        except (TypeError, ValueError, OverflowError):
            self.karma_initial = clamp_value(20, self.karma_min, self.karma_max)

        # ⚠️ 范围/初始值占位符：默认值里的 {karma_min} / {karma_max} /
        # {karma_initial} 在这里按**实际配置**渲染。这样用户改范围时，
        # 只改 karma_min/karma_max 即可，提示词里写的数字会自动跟上——
        # 不必再去逐个改提示词（那正是此前「提示词说 999、代码裁到 100」的成因）。
        #
        # 用户自定义的 karma_rules 若不写占位符，就原样使用（不做强制注入）。
        self.karma_rules: str = self._render_karma_rules(
            str(config.get("karma_rules", DEFAULT_KARMA_RULES) or DEFAULT_KARMA_RULES)
        )
        # 注入给 AI 的系统上下文模板，两场景各一份（见
        # DEFAULT_NETHERLINK_CONTEXT_QQ / DEFAULT_NETHERLINK_CONTEXT_GAME）。
        # 空串回退默认值——口径与 karma_rules / 工具描述一致：想「不注入任何内容」
        # 应当删掉模板里的标签行与内容（留空行），而不是靠清空配置项——后者会让
        # 「先查好感」这条覆盖游戏侧的指引静默消失。
        self.mc_command_tool_desc: str = str(
            config.get("mc_command_tool_desc", DEFAULT_MC_COMMAND_TOOL_DESC)
            or DEFAULT_MC_COMMAND_TOOL_DESC
        )
        self.karma_tool_desc: str = str(
            config.get("karma_tool_desc", DEFAULT_KARMA_TOOL_DESC)
            or DEFAULT_KARMA_TOOL_DESC
        )
        # 两个工具的**参数**说明也做成可配的（价格表就写在 cost 那一项里）。
        # 参数 schema 与描述一样：顶层工具来自 docstring 的 Args: 段，
        # 插件自建的两个 ToolSet 来自代码字面量——两处都要能配置覆盖。
        self.mc_command_cost_param_desc: str = str(
            config.get("mc_command_cost_param_desc", DEFAULT_MC_COMMAND_COST_PARAM_DESC)
            or DEFAULT_MC_COMMAND_COST_PARAM_DESC
        )
        self.mc_command_cmd_param_desc: str = str(
            config.get("mc_command_cmd_param_desc", DEFAULT_MC_COMMAND_CMD_PARAM_DESC)
            or DEFAULT_MC_COMMAND_CMD_PARAM_DESC
        )
        self.mc_command_server_param_desc: str = str(
            config.get(
                "mc_command_server_param_desc", DEFAULT_MC_COMMAND_SERVER_PARAM_DESC
            )
            or DEFAULT_MC_COMMAND_SERVER_PARAM_DESC
        )
        self.karma_delta_param_desc: str = str(
            # 注意键名是 mc_karma_delta_param_desc（与 schema 逐字一致）。
            # 2026-09-20 修：此前这里少写了 mc_ 前缀，键永远读不到，
            # 配置项自加入起就是死的——用户在 WebUI 改它毫无效果且无告警。
            config.get("mc_karma_delta_param_desc", DEFAULT_KARMA_DELTA_PARAM_DESC)
            or DEFAULT_KARMA_DELTA_PARAM_DESC
        )
        # 管理员游戏 ID（AstrBot 全局管理员的 admins_id 是 QQ 号，对游戏内无效）
        self.admin_mc: set[str] = self._parse_csv(config.get("admin_mc", ""))
        # 匹配用的小写副本：MC 登录名不区分大小写，而 getName() 返回**规范拼写**。
        # 用户填 moedawn、游戏里是 MoeDawn 时，区分大小写的比对永远匹配不上，
        # 且完全静默。admin_mc 本身保留原样，用于提示词里展示名单。
        self._admin_mc_lower: set[str] = {k.lower() for k in self.admin_mc}
        # 这两个数都是"外部输入"（WebUI 手填），必须夹住负值：
        #   karma_death_penalty < 0 会把死亡变成好感**奖励**（-(-2) = +2）；
        #   max_command_cost <= 0 会让 clamp_cost 一律返回 0，所有指令免费。
        # 0 都是合法选择（=死亡不扣 / 指令全免费），只夹负值，不回退默认。
        # OverflowError 一并兜底：JSON 里的 1e400 解析成 inf，int(inf) 抛的是
        # OverflowError 而不是 ValueError，漏掉会直接崩掉插件加载。
        try:
            self.max_command_cost: int = max(0, int(config.get("max_command_cost", 80)))
        except (TypeError, ValueError, OverflowError):
            self.max_command_cost = 80
        try:
            self.karma_death_penalty: int = max(
                0, int(config.get("karma_death_penalty", 2))
            )
        except (TypeError, ValueError, OverflowError):
            self.karma_death_penalty = 2
        # 对话触发的好感变化是否记一条日志（供运维观察 AI 的增减行为）
        self.log_karma_changes: bool = bool(config.get("log_karma_changes", True))
        # 成就处理：enable_advancement 开启时，玩家获得成就就把提示词发给 AI，
        # 由 AI 决定好感变化并回话（与死亡扣减不同——那是 AI 不在场的代码扣减）。
        # 留空则用默认提示词（与其他提示词字段一致：空串不表示"关闭"，用开关关）。
        self.enable_advancement: bool = bool(config.get("enable_advancement", True))
        self.advancement_prompt: str = str(
            config.get("advancement_prompt", DEFAULT_ADVANCEMENT_PROMPT)
            or DEFAULT_ADVANCEMENT_PROMPT
        )

        self.templates = {
            "chat": config.get("template_chat", "⌜{server}⌟ <{player}>: {text}"),
            "join": config.get("template_join", "[{server}] {player} 进入了服务器"),
            "leave": config.get("template_leave", "[{server}] {player} 离开了服务器"),
            "death": config.get("template_death", "⌜{server}⌟ {message}"),
            "qq_to_mc": config.get("template_qq_to_mc", "§a⌜{group}⌟§f <§b{sender}§f> §d{text}§f"),
            "bot_reply_game": config.get(
                "template_bot_reply_game", "§c⌜{bot}⌟§f : §d{text}§f"
            ),
        }

        # ---- 运行时状态 ----
        self._runner: Optional[web.AppRunner] = None
        # 已握手的 MC 连接：server_id -> McConn（多服并存）。
        # 早先这里是单个 `_mc_ws` 字段，第二台服务器连入会踢掉第一台，
        # 而两端都会重连，于是形成**永不停止的互踢**（见 docs/multi-server-plan.md）。
        self._mc_conns: dict = {}
        # 端口 -> 当前占用的 server_id。一个端口同一时刻只服务一台服务器：
        # 同 id 再连 = 断线重连（正常，替换旧的）；不同 id = 配置冲突（阶段 0：报错拒绝）。
        self._port_owner: dict = {}
        # 等待执行结果的指令：id -> asyncio.Future
        self._pending_cmds: dict[str, asyncio.Future] = {}
        # 游戏内对话进行中的玩家 -> asyncio.Lock，防止同玩家并发请求 LLM
        self._player_llm_locks: dict[str, asyncio.Lock] = {}
        # 最近一条**真实**进站群消息的 unified_msg_origin。_broadcast 需要主动
        # 发消息时必须自己拼 umo，而只有真实事件上的 umo 才一定正确（首段是
        # 平台标识、会话号是平台自己的写法）。学到它之后优先复用，见
        # _resolve_qq_platform_id。
        self._qq_umo_seen: str = ""

        # ---- 好感度（本地文件 + 配置项双写，供 WebUI 查看与手改） ----
        # 身份空间见 karma.identity_key：游戏内玩家 "mc:<游戏ID>"，QQ 群友 "qq:<QQ号>"，
        # 同一个人在两边是两份独立好感，不做映射。
        self._karma_dir: Path = Path(get_astrbot_plugin_data_path()) / "netherlink"
        self._karma_path: Path = self._karma_dir / "karma.json"
        self._karma_lock = asyncio.Lock()
        # 配置优先、文件兜底（管理员在 WebUI 手改的 karma_records 优先级最高）。
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
                    KarmaStore.read(self._karma_path, self.karma_min, self.karma_max).snapshot(),
                    self._parse_config_records(),
                    self.karma_min,
                    self.karma_max,
                ),
                self._karma_path,
                self.karma_min,
                self.karma_max,
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

        # 配置解析全是**静默**的：admin_mc 打错一个字、分隔符用了全角逗号，
        # 插件都不会报错，只会安静地把管理员当普通玩家。这条日志是唯一能立刻
        # 看出「我配的东西到底被解析成了什么」的地方——排查时先看它。
        logger.info(
            f"NetherLink: 配置解析结果 —— 管理员(游戏)={sorted(self.admin_mc) or '未配置'} "
            f"管理员(QQ)={sorted(self.admin_qq) or '未配置'} "
            f"绑定群={sorted(self.target_groups) or '未配置'} "
            f"端口绑定={self.ws_bindings}"
        )

        asyncio.create_task(self._start_ws_server())
        # 工具描述回填必须在 @filter.llm_tool 注册完成之后（即本类定义已被插件加载器
        # 扫描过），因此放在 __init__ 末尾；失败不影响工具可用性
        self._apply_configured_tool_descs()
        # 游戏侧平台适配器：补写配置项 + 把适配器重新绑到本实例。
        # 放在最后：它依赖上面的配置解析结果（server_display_names 等）。
        self._init_game_platform()
        logger.info("NetherLink 已加载")

    # ------------------------------------------------------------------
    # 工具函数
    # ------------------------------------------------------------------
    # 中文输入法下极易打出的分隔符——它们不是半角逗号，直接 split(",") 会把
    # 整串当成一个元素（如 admin_mc 变成 {"MoeDawn，Steve"}，永远匹配不上）。
    # 统一归一化成半角逗号再切分。
    _SEPARATORS = str.maketrans({"，": ",", "、": ",", "；": ",", ";": ",", "\u3000": ","})

    @staticmethod
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

    def _render_karma_rules(self, text: str) -> str:
        """把 karma_rules 里的范围占位符替换成实际配置值。"""
        return render_karma_rules(
            text, self.karma_min, self.karma_max, self.karma_initial
        )

    @staticmethod
    def _parse_csv(raw: str) -> set[str]:
        """把 '111, 222' 形式的配置解析成无重复集合。

        容错中文分隔符（全角逗号 / 顿号 / 分号 / 全角空格）——见 _SEPARATORS。
        """
        text = str(raw or "").translate(NetherLinkPlugin._SEPARATORS)
        return {s.strip() for s in text.split(",") if s.strip()}

    @staticmethod
    def _parse_group_names(raw: str) -> dict[str, str]:
        """把 '群号:群名, 987654:生存服' 形式的配置解析成 {群号: 群名}。

        同样容错中文分隔符（全角逗号 / 顿号），并把全角冒号也归一化——
        `server_display_names` 与 `group_names` 都走这里，写错一个字符就会
        静默失效。
        """
        text = (
            str(raw or "")
            .translate(NetherLinkPlugin._SEPARATORS)
            .replace("：", ":")
        )
        result: dict[str, str] = {}
        for part in text.split(","):
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

    @staticmethod
    def _parse_ws_ports(raw) -> list:
        """解析 ws_ports 配置 → [(server_name, port), ...]。

        - 留空/全非法 → []，插件**不监听任何端口**（启动时会明确告警）
        - "8765,8766" → [("", 8765), ("", 8766)]，server_name 待握手时用上报名补
        - "survival:8765" → [("survival", 8765)]

        server_name 为空串表示「由 MC 端上报的 server-name 决定」。

        ⚠️ 以前留空会退回那个单值配置 `ws_port`，现在它已删除——留空即不监听，
        这是刻意的失败方式（宁可明确不启动，也不要静默用一个陈旧的默认端口）。
        """
        out: list = []
        seen: set = set()
        text = str(raw or "").translate(NetherLinkPlugin._SEPARATORS)
        for part in text.split(","):
            part = part.strip()
            if not part:
                continue
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
        """用配置项覆盖工具 schema 的**描述与参数说明**。

        @filter.llm_tool 的 description 来自 docstring 的说明部分、参数 schema 来自
        `Args:` 段，两者都在**注册期**定型，那时配置值还不存在；因此只能注册完成后
        回填。失败仅告警，不影响工具可用性。

        参数回填的必要性（2026-09-19）：价目表从 `karma_rules` 挪进了
        `mc_command` 的 cost 参数说明——而参数 schema 原本写死在代码里，
        不回填的话用户就改不了它。
        """
        if self.context is None:
            # 无上下文（如测试替身）时不存在工具管理器，没有可覆盖的目标，
            # 这不是错误，静默跳过——否则每次加载都会刷一条无意义的 warning
            return
        try:
            mgr = self.context.get_llm_tool_manager()
            # 两个工具都始终覆盖描述，没有任何开关能跳过 mc_karma 那一项：
            # 好感度是强制机制，工具描述就是 AI 唯一的策略来源。
            # (工具名, 描述, {参数名: 参数说明})
            targets = [
                ("mc_command", self.mc_command_tool_desc,
                 {
                     "cmd": self.mc_command_cmd_param_desc,
                     "cost": self.mc_command_cost_param_desc,
                     "server": self.mc_command_server_param_desc,
                 }),
                ("mc_karma", self.karma_tool_desc,
                 {"delta": self.karma_delta_param_desc}),
            ]
            for name, desc, param_descs in targets:
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
                # 回填参数说明。parameters 是 dict（{"type","properties","required"}），
                # 只改 properties 里对应参数的文字，其余结构原样保留——写坏了会让
                # 模型看不到参数，比描述失效更严重。
                params = getattr(tool, "parameters", None)
                props = (params or {}).get("properties") if isinstance(params, dict) else None
                if props:
                    for pname, ptext in param_descs.items():
                        if ptext and pname in props and isinstance(props[pname], dict):
                            props[pname]["description"] = ptext
        except Exception as e:
            logger.warning(f"NetherLink: 覆盖工具描述失败（使用默认描述）: {e}")

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
                old, new = self._karma.add(key, delta, self.karma_initial)
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
                        f"按好感值从低到高淘汰 {len(dropped)} 条: {dropped}"
                    )
                self._sync_karma_to_config()
                if origin and self.log_karma_changes:
                    logger.info(
                        f"NetherLink: [{origin}] {key} 好感 {old} → {new}"
                        f"（{delta:+d}，范围 {self.karma_min}~{self.karma_max}）"
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

    def _mc_connected(self, server_id: str = "") -> bool:
        """是否有可用的 MC 连接。

        不传 server_id 时表示「**至少有一台**在线」——保留这个语义是为了让
        既有的单服调用点（离线判定、指令执行前的检查）不用改。
        传了则只认那一台。
        """
        if server_id:
            conn = self._mc_conns.get(server_id)
            return conn is not None and not conn.closed
        return any(not c.closed for c in self._mc_conns.values())

    def _fmt(self, tpl: str, **kw) -> str:
        try:
            return tpl.format(**kw)
        except (KeyError, IndexError):
            return tpl  # 模板占位符写错时兜底原样输出

    # ------------------------------------------------------------------
    # WebSocket 服务端
    # ------------------------------------------------------------------
    async def _start_ws_server(self):
        """启动 WS 服务端，监听 MC 端连入。

        **可监听多个端口**（见 ws_ports）：一台 MC 服务器占一个端口，单服就配一个。
        共用同一个 `app` 与 `_ws_handler`，用本地端口区分来源（路线 A「端口即身份」）。

        `ws_bindings` 为空（ws_ports 留空或全非法）时**不监听任何端口**——
        以前会退回那个单值配置 `ws_port`（已删除），现在刻意选择响亮地失败：
        配错端口却不自知的代价，比启动时看到一条 ERROR 大得多。
        """
        if not self.ws_bindings:
            logger.error(
                "NetherLink: 未配置任何监听端口，WS 服务端没有启动——"
                "请在 ws_ports 里填写，格式「server-name:端口」或只写「端口」，"
                "单台服务器也只需填一个。"
            )
            return

        app = web.Application()
        app.router.add_get("/ws", self._ws_handler)
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        for label, port in self.ws_bindings:
            tag = f"[{label}] " if label else ""
            try:
                site = web.TCPSite(self._runner, self.ws_host, port)
                await site.start()
                logger.info(
                    f"NetherLink WS 服务端已启动 {tag}ws://{self.ws_host}:{port}/ws"
                )
            except OSError as e:
                logger.error(f"NetherLink WS 端口 {port} 启动失败: {e}")

    def _online_servers(self) -> list:
        """在线服务器的 (server_id, 显示名) 列表，供 QQ 侧 AI 选择指令目标。"""
        return [
            (sid, self._mc_server_display(sid))
            for sid, conn in sorted(self._mc_conns.items())
            if not conn.closed
        ]

    def _build_online_servers_hint(self) -> str:
        """把在线服务器列给 AI，供它决定指令发往哪台。

        只有一台时也给（写明"仅一台"），否则 AI 会以为需要自己挑。没有在线
        服务器时明确说没有——别让 AI 以为可以执行。
        """
        online = self._online_servers()
        if not online:
            return "当前没有 MC 服务器在线，无法执行任何服务器指令。"
        if len(online) == 1:
            sid, disp = online[0]
            return f"当前只有一台 MC 服务器在线：{disp}（server_id: {sid}）。指令将发往它。"
        listed = "、".join(f"{disp}（server_id: {sid}）" for sid, disp in online)
        return (
            f"当前有 {len(online)} 台 MC 服务器在线：{listed}。"
            "请用 server 参数指明指令发往哪一台（填 server_id 或显示名都可）；"
            "不填会被拒绝发送。"
        )

    def _resolve_target_server(self, name: str) -> str:
        """把 AI 给的 `server` 参数（server_id 或显示名）解析成 server_id。

        支持两种写法：**server_id**（如 survival）与**显示名**（如 生存服）——
        AI 在上下文里看到的是显示名，但工具参数更可能照抄 id，两者都认。
        解析不出返回空串（调用方据此报错或退回唯一在线的那台）。
        """
        want = str(name or "").strip()
        if not want:
            return ""
        online = self._online_servers()
        for sid, disp in online:
            if want == sid or want == disp:
                return sid
        return ""

    def _bound_server_id(self, port: int) -> str:
        """该端口在配置里绑定的 server_id；未绑定则返回空串（由 MC 端上报名决定）。"""
        for label, p in self.ws_bindings:
            if p == port:
                return label
        return ""

    def _is_current_conn(self, server_id: str, ws) -> bool:
        """这条 ws 是否仍是该 server_id 的当前连接（未被重连替换掉）。"""
        conn = self._mc_conns.get(server_id)
        return conn is not None and conn.ws is ws

    async def _ws_handler(self, request):
        ws = web.WebSocketResponse(heartbeat=30)
        await ws.prepare(request)
        # 本连接的本地端口——路线 A 里它就是身份的来源
        try:
            port = int(request.transport.get_extra_info("sockname")[1])
        except Exception:
            port = 0
        bound_id = self._bound_server_id(port)
        logger.info(f"NetherLink: MC 端已连入（本地端口 {port}），等待握手")

        server_id = ""
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
                    reported = str(data.get("server_name") or "mc")
                    # 身份取值顺序：**配置里为该端口绑定的 id** 优先，其次 MC 端上报的
                    # server-name。前者更可靠（不依赖对方填对名字），推荐使用。
                    server_id = bound_id or reported

                    # ---- 阶段 0：同端口不同服务器的冲突，必须报错而不是静默踢掉 ----
                    # 以前是「新连接一律踢掉旧连接」，于是两台服务器互相踢、各自重连，
                    # 形成永不停止的抖动（两端退避都会重置回 3 秒），且日志里看不出。
                    owner = self._port_owner.get(port)
                    if owner and owner != server_id and not self._conn_closed(owner):
                        logger.error(
                            f"NetherLink: 端口 {port} 已被服务器 [{owner}] 占用，"
                            f"拒绝新连接 [{server_id}]。"
                            f"若这是另一台服务器，请在 ws_ports 里为它单独指定一个端口——"
                            f"两台服务器连同一个端口会互相踢下线，消息会随机丢失。"
                        )
                        try:
                            await ws.close(
                                code=4003, message=b"port in use by another server"
                            )
                        except Exception:
                            pass
                        return ws

                    # 同 id 再连 = 断线重连，关掉**这台服务器自己**的旧连接；
                    # 绝不能动别的服务器（以前那个「一律踢掉」正是多服抖动的根源）
                    old = self._mc_conns.get(server_id)
                    if old is not None and old.ws is not ws and not old.closed:
                        if bound_id:
                            # 端口已绑定 id，新连接必然被判为「同一台」，无法用 id 区分。
                            # 但旧连接**仍然活着**却来了新连接，是互踢的典型征兆：
                            # 真的重连时旧 socket 早已断开。两台服务器都指向同一个
                            # 已绑定端口时，各自都自称该 id，就会一直互相顶掉。
                            logger.warning(
                                f"NetherLink: 端口 {port}（绑定 [{bound_id}]）上的连接"
                                f"被【仍然在线】的新连接替换——上报名 "
                                f"[{old.reported_name}] -> [{reported}]。"
                                f"若这其实是两台不同的服务器，请为它们各配一个端口，"
                                f"否则会互相踢下线、消息随机丢失。"
                            )
                        else:
                            logger.warning(
                                f"NetherLink: 服务器 [{server_id}] 重复连入，关闭其旧连接"
                            )
                        try:
                            await old.ws.close(code=4002, message=b"replaced by new connection")
                        except Exception:
                            pass
                    self._mc_conns[server_id] = McConn(
                        ws=ws, port=port, reported_name=reported
                    )
                    self._port_owner[port] = server_id
                    logger.info(
                        f"NetherLink: MC 服务器 [{server_id}] 握手成功"
                        f"（端口 {port}，上报名 {reported}）"
                    )
                elif not self._is_current_conn(server_id, ws):
                    continue  # 未握手、或已被重连替换的连接不发事件

                elif mtype == "command_result":
                    fut = self._pending_cmds.pop(data.get("id"), None)
                    if fut and not fut.done():
                        # ok 是**必需**契约：Paper 端执行异常时回 ok=false。
                        # 早先 Java 端无条件写 ok=true、这里也只取 output，两边
                        # 一起把「显式失败」伪装成了成功——玩家被扣费且被告知
                        # 「指令已执行」，与「执行失败则退费」的承诺直接冲突。
                        # 缺字段一律当失败处理（fail-closed）：宁可退费，也不
                        # 让一次可疑的执行白扣好感。
                        fut.set_result(
                            CmdResult(
                                ok=bool(data.get("ok", False)),
                                output=str(data.get("output", "")),
                            )
                        )
                elif mtype == "heartbeat":
                    pass
                elif mtype == "bot_chat":
                    # 游戏内唤醒词消息：走 LLM，回复只发回游戏，QQ 不可见
                    asyncio.create_task(self._handle_bot_chat(data, server_id))
                else:
                    await self._dispatch_mc_event(mtype, data, server_id)

            elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSE):
                break

        # 只摘掉属于这条连接的登记。绝不能整个清空——多服并存时那会误伤别人，
        # 而且 _port_owner 也会跟着丢，后续重连会被误判成端口冲突。
        if server_id and self._is_current_conn(server_id, ws):
            self._mc_conns.pop(server_id, None)
            if self._port_owner.get(port) == server_id:
                self._port_owner.pop(port, None)
            logger.warning(f"NetherLink: MC 服务器 [{server_id}] 连接断开")
        return ws

    def _conn_closed(self, server_id: str) -> bool:
        conn = self._mc_conns.get(server_id)
        return conn is None or conn.closed

    async def _dispatch_mc_event(self, mtype: str, data: dict, server_id: str = ""):
        """把 MC 事件按模板渲染后推送到所有绑定群。

        `server_id` 来自连接（端口绑定或握手上报），决定 `{server}` 显示成什么。
        """
        try:
            srv = self._mc_server_display(server_id)
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
            elif mtype == "advancement":
                # 成就走**独立管线**：要起 AI 让它更新好感并回话，不是套模板广播。
                # 单独 create_task，避免把 LLM 往返（可能数秒）压在这条 WS 事件
                # 处理路径上、连累后续事件。
                asyncio.create_task(self._handle_advancement(data, server_id))
                return
            else:
                return
            await self._broadcast(text)
        except Exception as e:
            logger.error(f"NetherLink: 处理 MC 事件 {mtype} 失败: {e}")

    def _mc_server_display(self, server_id: str = "") -> str:
        """对外展示与 LLM 上下文统一使用的服务器名。

        取值顺序：
          1. `server_display_names` 里该 `server_id` 的映射
          2. `DEFAULT_SERVER_DISPLAY`（"MC"）——单服、或该 id 没配映射时

        这里刻意**不用** MC 端上报的 `server-name`：Paper 侧那个值只作**标识**
        （握手告知"我是哪台服务器"），显示名统一由本插件配置控制。这样 QQ 群消息
        前缀、模板 {server}、LLM 上下文三处看到的名字必然一致，不会出现
        「群里显示 [MC]、AI 却被告知服务器叫 mc」这种割裂。
        """
        if server_id:
            name = self.server_display_names.get(str(server_id))
            if name:
                return name
        return DEFAULT_SERVER_DISPLAY

    # ------------------------------------------------------------------
    # 提示词拼装（游戏侧）
    # ------------------------------------------------------------------
    def _admin_context(
        self, identity: str, is_admin: bool, source: str, server_id: str = ""
    ) -> str:
        """管理员名单 + 当前发起者身份 + 每次必查好感的要求，按模板渲染。

        名单只是参考信息，插件不做任何拦截——是否放行由 AI 决定。

        **本段是几个「面」唯一的共同内容**（游戏侧对话/成就、QQ 侧普通对话经
        on_llm_request 钩子），所以「每次回复前先查好感」这条要求写在这里才能
        全覆盖——写进 `karma_rules` 对 QQ 侧普通对话无效（那条路径看不到
        `karma_rules`）。

        ⚠️ 两个场景的默认文案**故意不同**（2026-09-21 拆分）：QQ 侧不带
        「先查好感」那句（主 agent 有会话记忆，每轮强制查会过频），游戏侧
        多一句「历史里方括号标注说话人」（会话按服务器建，多个玩家共用）。
        这**不是**「每次回复前先查好感覆盖四个面」那句注释的失效——它现在
        覆盖的是模板默认值里有它的那份（游戏侧）；QQ 侧改成靠 AI 自行判断
        何时查，是用户实测后的明确决定。

        QQ 侧走可配模板 template_netherlink_context_qq；游戏侧由代码生成
        （它并入「游戏内自定义提示词」，见 extra_system_prompt）。
        返回值永不为空。
        """
        values = self._admin_context_values(identity, is_admin, source, server_id)
        if source != "qq":
            self_label = "游戏侧系统上下文"
            tpl, default = self.netherlink_context_game, DEFAULT_NETHERLINK_CONTEXT_GAME
        else:
            self_label = "QQ 侧系统上下文"
            tpl, default = self.netherlink_context_qq, DEFAULT_NETHERLINK_CONTEXT_QQ
        # 两侧各走一份**可配模板**（2026-09-22 对称化）。
        # 渲染失败（占位符写坏 / 漏写必需占位符）会回退默认模板并记 warning。
        return _render_template(
            tpl, default, values,
            ("{identity}", "{is_admin}"),
            self_label,
        )

    def _admin_context_values(
        self,
        identity: str,
        is_admin: bool,
        source: str,
        server_id: str = "",
    ) -> dict:
        """算出注入模板的占位符值：只有三项。

        ⚠️ `{is_admin}` **始终给**（是/不是都要说）——用户 2026-09-22 明确要求
        「只需要包含当前说话的玩家是不是管理员」。

        `{server}` 给游戏侧模板用（显示名）。

        `{origin}`（来源 + 服务器列表）与 `{roster}`（完整名单）已于 2026-09-22
        删除：前者与 AstrBot 自带的 Group name、以及 `_build_online_servers_hint()`
        里那份**在线**服务器清单重复；后者对「是否管理员」这个判断没有增量信息。
        `admin_context_admin_only` 开关随之删除（它只管这两行）。
        占位符减到三个后，**模板里再写 {origin}/{roster} 会原样发出去**——
        `_render_template` 只替换已知的键，其余 `{...}` 一律保留（NBT 花括号
        那一课）。所以不要把它们加回模板提示词里。
        """
        return {
            "identity": identity,
            "is_admin": "是管理员." if is_admin else "不是管理员.",
            # 游戏侧模板里的 {server}——用**显示名**（server_display_names 配的，
            # 没配则兜底 MC），与 QQ 群前缀、模板 {server} 保持一致。
            "server": self._mc_server_display(server_id),
        }

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

        **大小写不敏感**：MC 登录名本身不区分大小写，但 `getName()` 返回规范拼写
        （MoeDawn），而 set 成员判断区分大小写。填 `moedawn` 会静默失效。
        """
        return str(mc_id).strip().lower() in self._admin_mc_lower



    async def _build_context(self, identity: str, is_admin: bool,
                             server_id: str = "", source: str = "qq",
                             extra: str = "", extra_in_front: bool = False) -> str:
        """拼装注入给 AI 的全部上下文。QQ 侧与游戏侧共用这一个函数。

        为什么合并（2026-09-22 用户要求）：走原生群聊后，AstrBot 自己会在玩家
        消息后附上 User ID / Nickname / Group name，两侧的差别就只剩「这是哪个
        场景」。以前维护两份近乎重复的拼装，只会漂移。

        内容顺序：好感规则 -> [extra（仅游戏侧）]-> 在线服务器清单 -> 管理员与身份。
        `source`：`"qq"` / `"game"`，决定 `_admin_context` 给哪一份文案
        （游戏侧多「唤醒词含义」与「先查一次好感」两句）。
        `extra`：仅游戏侧附加的提示词（`extra_system_prompt`），含背包查验那套
        流程——注给 QQ 侧只会让主 agent 去处理与它无关的事，所以那边给空串。
        `extra_in_front`：游戏侧要求 extra 紧跟在**好感规则**之后（用户
        2026-09-21 明确要求「好感度规则紧跟自定义提示词」）；QQ 侧则放末尾。
        """
        blocks = []
        if self.karma_rules:
            blocks.append(self.karma_rules)
        # 工具名（mc_karma）由 karma_rules 的默认值点出——
        # 用户 2026-09-22：好感规则本来就有配置项，把「互动涉及好感度时调用」
        # 写在那儿比单独维护一个 QQ_KARMA_HINT 常量更省事，也不会两处漂移。
        if extra_in_front and extra.strip():
            blocks.append(extra)
        # 在线服务器清单：告诉 AI 指令能发往哪台、不填会怎样。
        blocks.append(self._build_online_servers_hint())
        blocks.append(self._admin_context(identity, is_admin, source, server_id))
        if not extra_in_front and extra.strip():
            blocks.append(extra)
        return "\n\n".join(blocks)

    async def _handle_advancement(self, data: dict, server_id: str = ""):
        """玩家获得成就：把渲染好的提示词推进**平台管线**，由 AI 更新好感并回话。

        与死亡扣减的差别：死亡是 AI 不在场的**代码**扣减；成就是 AI **在场**的
        判断，所以要走一次完整的对话。

        2026-09-22 改走适配器（原先自己造合成事件、自己调 tool_loop_agent）：
          · 人格 / 工具 / agent 钩子 / 出站推群全部复用，不用维护第二套；
          · 与玩家对话进**同一个会话**，AI 有上下文；
          · 提示词里声明了这是**系统通知**（kind="advancement"），
            否则 AI 会把成就当成玩家在跟它邀功（用户实测）。
        """
        player = str(data.get("player") or "")
        advancement = str(data.get("advancement") or "")
        if not player or not advancement:
            return  # 缺字段就当没这回事，别拿 "?" 去建 mc:? 假记录
        if not self.enable_advancement:
            return

        try:
            adapter = self._resolve_game_adapter()
            if adapter is None or adapter._plugin is None:
                logger.warning(
                    "NetherLink: 平台适配器不可用，本次成就通知跳过"
                    "（首次启用请重启一次 AstrBot）"
                )
                return
            # 提示词里的 {player}/{server}/{advancement} 由插件替换——
            # 成就是客观事实，不该让 AI 去猜谁拿到了什么
            text = (
                self.advancement_prompt
                .replace("{player}", player)
                .replace("{server}", self._mc_server_display(server_id))
                .replace("{advancement}", advancement)
            )
            adapter.handle_advancement(
                player, server_id, text,
                display_name=self._mc_server_display(server_id),
            )
        except Exception as e:
            logger.error(f"NetherLink: 处理成就事件失败: {e}")

    async def _handle_bot_chat(self, data: dict, server_id: str = ""):
        """玩家用唤醒词对 AI 说话，交给原生平台管线。

        本方法只是 WS 层的薄入口：解析字段、定位适配器、委托出去。
        真正的对话由 AstrBot 完整 pipeline 处理（人格 / 记忆 / 工具 /
        agent 钩子 / 出站推群全部复用），见 mc_platform.py。

        2026-09-22 之前这里自己造合成事件 + 自己调 tool_loop_agent，那条路径
        绕过 pipeline，代价是拿不到工具状态提示、还要自己维护一整套工具与
        提示词拼装。已整体删除——现在只有这一条路。
        """
        player = str(data.get("player", "?"))
        text = str(data.get("text", "")).strip()
        if not text:
            return

        # 玩家那句唤醒消息本身也要推群。Paper 侧发完 bot_chat 就 return 了，
        # 不会再发一条 chat 事件；不补的话群里只看到 AI 的回答、
        # 看不到玩家问了什么（2026-09-19 用户要求）。
        # 推的是玩家原话（含唤醒词），与普通聊天同一个模板与开关。
        if self.config.get("enable_chat", True):
            try:
                await self._broadcast(
                    self._fmt(
                        self.templates["chat"],
                        # 必须带 server_id：漏传会让 {server} 展开成兜底值 MC，
                        # 多服务器时群里看到的前缀永远不对（2026-09-21 修的）。
                        server=self._mc_server_display(server_id),
                        bot=self.mc_bot_name,
                        player=player,
                        text=text,
                    )
                )
            except Exception as e:
                logger.error(f"NetherLink: 推送游戏内唤醒消息到群失败: {e}")

        adapter = self._resolve_game_adapter()
        if adapter is None or adapter._plugin is None:
            # 自建 agent 已删除，没有可回退的第二条路，如实告知别让玩家干等。
            logger.error(
                "NetherLink: 平台适配器不可用，游戏内对话无法进行"
                "（首次启用请重启一次 AstrBot 让框架加载平台）"
            )
            await self._send_to_mc(
                {
                    "type": "bot_reply",
                    "line": self.templates["bot_reply_game"]
                    .replace("{bot}", self.mc_bot_name)
                    .replace("{text}", "（我暂时没法回答，请检查机器人配置）"),
                },
                server_id,
            )
            return
        adapter.handle_bot_chat(
            player, server_id, text,
            display_name=self._mc_server_display(server_id),
        )
    async def send_game_line(self, text: str, server_id: str = "") -> None:
        """把一行文本按 `template_bot_reply_game` 渲染后发到**指定**服务器。

        只发游戏。要不要同步到 QQ 由**调用方分成两步**决定（见
        `sync_bot_reply_to_qq`）——早先把两件事塞进一个 `sync_qq` 布尔、
        让它穿过「事件 → 适配器 → 插件」三层，结果漏传两次（2026-09-22）。
        拆成两个方法后，**没有可以被忘记传的参数**。
        """
        line = self.templates["bot_reply_game"].replace(
            "{bot}", self.mc_bot_name
        ).replace("{text}", str(text).replace("§", "&"))
        await self._send_to_mc({"type": "bot_reply", "line": line}, server_id)

    async def sync_bot_reply_to_qq(self, text: str, server_id: str = "") -> None:
        """把 AI 的回复同步到**所有绑定群**（游戏内对话 → QQ 群）。

        只在「这是 AI 的最终答复」时调用。工具调用/思考那类过程消息不进群：
        否则群里会被每一步工具调用刷屏，而群友要的是「AI 说了什么」。
        """
        await self._broadcast(
            f"[{self._mc_server_display(server_id)}] {self.mc_bot_name}: {text}"
        )
        logger.info(
            f"NetherLink: Ai 回复已同步到 {len(self.target_groups)} 个绑定群"
        )

    def _init_game_platform(self) -> None:
        """补写平台配置项 + 把适配器重新绑到本插件实例。

        **为什么只是补配置项、不自己造实例**：AstrBot 在 core_lifecycle 里
        「先加载插件（plugin_manager.reload）后初始化平台
        （platform_manager.initialize）」，所以只要 config["platform"]
        里已有这条记录，重启后框架就会自己把适配器实例化。
        自己 new 一个再塞进 platform_insts 也能跑，但那样会绕过框架的
        生命周期管理（terminate/reload 都管不到它），得不偿失。

        **幂等按 id 判**：不判的话每次插件重载都会往配置里追加一条，
        越积越多；而且 `send_message` 只匹配**第一个**命中的实例，
        旧实例会继续吃消息。
        """
        if self.context is None:
            # 无上下文（测试替身、或框架尚未注入）时不存在平台管理器，
            # 也没有可写的配置——静默跳过。口径与 _apply_configured_tool_descs
            # 一致：这不是错误，刷一条 warning 只会污染日志。
            return
        try:
            cfg = self.context.get_config()
            platforms = cfg.get("platform")
            if not isinstance(platforms, list):
                logger.warning(
                    "NetherLink: 配置里 platform 不是列表，跳过游戏侧平台注册"
                )
                return
            # ⚠️ 只能 append 到**已存在的那个 list**，不能整体重新赋值：
            # PlatformManager 在 __init__ 时就把 config["platform"] 存成了
            # 活引用，重新赋值会让它继续指向旧 list。
            wanted = {
                "type": GAME_PLATFORM_ID,
                "id": GAME_PLATFORM_ID,
                "enable": True,
                "netherlink_managed": True,
            }
            existing = None
            for item in platforms:
                if isinstance(item, dict) and str(item.get("id")) == GAME_PLATFORM_ID:
                    existing = item
                    break
            if existing is None:
                platforms.append(dict(wanted))
                try:
                    cfg.save_config()
                except Exception as e:
                    logger.error(f"NetherLink: 写入平台配置失败（重启后需手动添加）: {e}")
                logger.info(
                    "NetherLink: 已在配置里登记游戏侧平台 "
                    f"（id={GAME_PLATFORM_ID}）。**需要重启 AstrBot 一次**，"
                    "框架才会实例化它（也可在 WebUI 的平台列表里手动启用）。"
                )
            elif not existing.get("enable"):
                # 用户手动关掉了这个平台——尊重它，不偷偷改回去
                logger.warning(
                    f"NetherLink: 平台 {GAME_PLATFORM_ID} 在配置里是 disabled，"
                    "游戏内对话将无法工作。请在 WebUI 里启用它。"
                )

            # 绑定**不在这里做**：本方法跑在插件加载期，而框架要到
            # `platform_manager.initialize()` 才实例化适配器——此处去看
            # 必然「尚未实例化」，绑不上（2026-09-22 实测踩到）。
            # 真正的绑定在 on_astrbot_loaded 钩子里（那个钩子按设计排在
            # 平台初始化之后），见 _bind_game_platform。
        except Exception as e:
            # 平台注册失败不能让插件加载失败——其余功能（QQ 侧、好感度）照常
            logger.error(f"NetherLink: 初始化游戏侧平台失败: {e}")





    # ------------------------------------------------------------------
    # 指令执行核心（QQ llm_tool 与游戏内 tool_loop_agent 共用）
    # ------------------------------------------------------------------
    async def _run_console_cmd(
        self, cmd: str, timeout: float = 8.0, server_id: str = ""
    ) -> Optional[CmdResult]:
        """以控制台身份执行一条指令。

        返回 None 表示**没有拿到回执**（未连接 / 发送失败 / 超时）；
        拿到回执则返回 CmdResult，其 ok 区分成功与明确失败。
        两种结局都要退费，但报给 AI 的话术不同。

        不做权限校验（仅供内部工具使用）；扣费与回滚由 exec_command_for 负责。
        """
        if not self._mc_connected(server_id):
            return None
        cmd_id = uuid.uuid4().hex
        fut = asyncio.get_running_loop().create_future()
        self._pending_cmds[cmd_id] = fut
        if not await self._send_to_mc(
            {"type": "command", "id": cmd_id, "cmd": cmd.lstrip("/")}, server_id
        ):
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
        self,
        initiator: str,
        cmd: str,
        source: str,
        qq: str = "",
        cost: int = 0,
        server_id: str = "",
    ) -> str:
        """执行指令并返回给 LLM 的结果文本。

        本方法不含任何权限判断、不持有指令名单——是否该执行由 AI 依提示词自行决定，
        插件只负责执行与原子化的好感扣费。

        扣费与执行在同一次调用内完成：好感不足则不执行；未连上服务器或执行超时
        则把扣掉的还回去。这样 AI 既跳不过扣费（扣费是执行的必经之路），
        也不会为一次没跑成的执行白付钱。

        source="qq"  : initiator 为发起人显示名（QQ 群昵称），qq 为发起人 QQ
        source="game": initiator 为游戏内玩家名
        server_id    : 指令发往哪台 MC 服务器。游戏侧由连接直接得出（连接即身份）；
                       QQ 侧由 AI 依上下文选定。为空时交由 _send_to_mc 判定：
                       只有恰好一台在线才发送，多台则拒发（宁丢不发错）。
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

                result = await self._run_console_cmd(cmd, server_id=server_id)
                # 三种结局绝不能折叠：
                #   None            -> 没有回执（未连接/发送失败/超时）-> 退费
                #   CmdResult(ok=F) -> 服务器**明确回了失败**          -> 退费
                #   CmdResult(ok=T) -> 执行成功（output 可为空串）      -> 照常收费
                # 早先只有 None 这一路退费，显式失败被当成成功照扣——
                # 与「执行失败则退费」的承诺冲突，玩家白付钱还被误导。
                failure = None
                if result is None:
                    failure = (
                        "指令已发送，但 8 秒内未收到服务器回执（可能仍在执行）"
                    )
                    reason = "指令超时无回执"
                elif not result.ok:
                    failure = f"指令执行失败。服务器输出：\n{result.output}"
                    reason = "服务器回报执行失败"
                if failure is not None:
                    if not spend:
                        return failure
                    if not await self._rollback_karma(key, spend, cur, reason, cmd):
                        return (
                            f"{failure}；好感回滚亦失败，已扣的 {spend} 点未退回。"
                        )
                    logger.error(
                        f"NetherLink: 指令执行失败（{reason}）已回滚好感 {spend} [{key}]: {cmd}"
                    )
                    return f"{failure}\n（好感未扣除）"

                output = result.output
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

    def _inject_platform_ids(self) -> set:
        """会走**身份注入**的平台标识集合（aiocqhttp 实例的 meta().id）。

        注入范围自 2026-09-20 起不再由 `target_groups` 界定：绑定群只管消息互通，
        身份注入与好感度对所有群生效。但仍**必须**限制平台——`on_llm_request`
        钩子是全局的，对每个 LLM 请求都触发；不认平台就会把「MC 群服互通」的
        上下文注入到 Telegram / 网页聊天等其他平台的请求里。

        返回空集表示当前没有 aiocqhttp 实例（钩子据此跳过）。
        """
        ids: set = set()
        try:
            for platform in self.context.platform_manager.platform_insts:
                meta = platform.meta()
                if getattr(meta, "name", "") == "aiocqhttp" and meta.id:
                    ids.add(str(meta.id))
        except Exception as e:
            logger.error(f"NetherLink: 解析注入平台标识失败: {e}")
        return ids

    def _is_game_event(self, event) -> bool:
        """这个事件是否来自**游戏侧**（插件自建 agent 的合成事件 / 迁移后的 MC 平台）。
        
        判据只看平台标识，不看消息类型——游戏侧没有「群/私聊」之分。
        两个标识都要认：
          · `netherlink_mc`——迁移到原生平台适配器后的真实事件；
          · 迁移前的合成事件（平台 id 也是 netherlink_mc）。
        刻意**不**在本函数里判「是不是我们该管的场景」，那是调用方的事——
        与 `_is_aiocqhttp_event` 的口径保持一致（那边也只判平台 + 消息类型）。
        """
        try:
            return str(event.get_platform_id() or "") == GAME_PLATFORM_ID
        except Exception:
            return False
    
    async def _build_game_context(
        self, identity: str, is_admin: bool, server_id: str = ""
    ) -> str:
        """游戏侧的上下文 = 人格 + 通用上下文（含游戏侧专属的附加提示词）。

        与 QQ 侧共用 _build_context，只在前面多一段 WebUI 人格
        （QQ 侧主 agent 自己会读人格，插件不用带）。
        """
        parts = []
        if self.enable_webui_persona:
            try:
                persona = await self.context.persona_manager.get_default_persona_v3(
                    _game_umo(server_id)
                )
                if persona and persona.get("prompt"):
                    parts.append(str(persona["prompt"]))
            except Exception as e:
                logger.warning(f"NetherLink: 读取 WebUI 人格失败（跳过）: {e}")
        # 游戏侧上下文的渲染（含 {server} 替换）现在由 _admin_context 走模板完成。
        parts.append(
            await self._build_context(
                identity, is_admin, server_id=server_id, source="game",
            )
        )
        return "\n\n".join(p for p in parts if p.strip())    
    def _is_aiocqhttp_event(self, event) -> bool:
        """这个事件是否来自 aiocqhttp 平台的**群消息**。

        两道判据缺一不可：
          · 群消息——`<netherlink_context>` 讲的是「群友在群里的身份」，
            私聊里注入它是误导；
          · 平台是 aiocqhttp——见 `_inject_platform_ids` 的理由。

        用 `get_platform_id()`（平台实例的唯一标识）而不是 `get_platform_name()`
        （适配器类型名）：用户可能同时跑多个同类适配器，只有 id 能确定是哪个实例。
        与 `_resolve_qq_platform_id` 的约定保持一致。
        """
        try:
            from astrbot.core.platform.message_type import MessageType

            if event.get_message_type() != MessageType.GROUP_MESSAGE:
                return False
            return str(event.get_platform_id() or "") in self._inject_platform_ids()
        except Exception:
            return False

    def _platform_inst_is_alive(self, platform_id: str) -> bool:
        """该平台标识此刻是否真的有对应实例（用户可能改名或删掉适配器）。"""
        try:
            for platform in self.context.platform_manager.platform_insts:
                if platform.meta().id == platform_id:
                    return True
        except Exception as e:
            logger.error(f"NetherLink: 校验平台实例失败: {e}")
        return False

    def _resolve_qq_platform_id(self) -> str:
        """解析发群消息要用的**平台标识**（umo 首段）。

        AstrBot 的 umo 形如 <平台标识>:<消息类型>:<会话号>，首段是
        PlatformMetadata.id —— 它是 WebUI 里用户可填的「机器人名称」
        （默认 default），**不是适配器类型名**。send_message 拿首段与各
        平台实例的 meta().id 做严格字符串相等匹配，匹配不上就记一条
        cannot find platform for session ... 并**丢弃消息**（返回 False，
        不抛异常，所以外层 try 接不到）。原实现写死
        f"aiocqhttp:GroupMessage:{群号}"，只在用户恰好把机器人命名为
        aiocqhttp 时成立，其余部署下**所有**主动推送都静默失效。

        取值顺序，与官方 API 的约定一致（Context.get_platform_inst 的
        docstring 说「可以通过 event.get_platform_id() 获取平台 ID」，
        而 AstrBot 内建的 message 工具在 umo 不完整时也是从**真实会话**
        取前两段补全，从不按适配器类型名猜）：

        1. **真实会话优先**：任何一条进站的绑定群消息都带着权威 umo
           （见 on_group_message 记录）。同类型开了多个适配器时，只有它
           能保证选中的是真正在服务这些群的那个。
        2. **按适配器类型反查**：还没收到过任何群消息时（如插件刚启动、
           或只发不收），退化为按 meta().name == "aiocqhttp" 找实例，
           再取用户配置的 meta().id。aiocqhttp 适配器的 meta().name 是
           字面量 "aiocqhttp"（见 aiocqhttp_platform_adapter.py），稳定。

        第 1 步学到的标识会先校验实例是否还在：用户在 WebUI 里改了机器人
        名称后它立即失效，自动落回第 2 步，不会卡在旧值上。

        不做缓存（第 2 步每次重查）：实例可热重载，遍历这个通常只有一两个
        元素的列表远比维护缓存失效便宜。

        返回空串表示当前没有可用实例（如 OneBot 适配器未启用）——调用方据此
        跳过推送并记日志，而不是发一条注定被丢弃的消息。
        """
        learned = self._qq_umo_seen.split(":", 1)[0] if self._qq_umo_seen else ""
        if learned:
            if self._platform_inst_is_alive(learned):
                return learned
            # 学到的标识已失效（适配器被改名/删除），丢弃后走下面的反查
            self._qq_umo_seen = ""
        try:
            for platform in self.context.platform_manager.platform_insts:
                meta = platform.meta()
                if meta.name == "aiocqhttp" and meta.id:
                    return str(meta.id)
        except Exception as e:
            logger.error(f"NetherLink: 解析 aiocqhttp 平台标识失败: {e}")
        return ""

    async def _broadcast(self, text: str):
        """向所有绑定群推送文本。"""
        if not self.target_groups:
            logger.warning("NetherLink: 没有配置绑定群，消息无处推送")
            return
        platform_id = self._resolve_qq_platform_id()
        if not platform_id:
            logger.warning(
                "NetherLink: 未找到 aiocqhttp 平台实例，消息无法推送到 QQ 群"
                "（请确认 OneBot/aiocqhttp 适配器已启用）"
            )
            return
        for group in self.target_groups:
            umo = f"{platform_id}:GroupMessage:{group}"
            try:
                # send_message 在找不到平台时**返回 False 而不抛异常**，
                # 只由 AstrBot 自己记一条 warning。这里显式接住返回值，
                # 把「哪个群被丢掉了」补进本插件的日志，避免再次静默。
                ok = await self.context.send_message(umo, MessageChain().message(text))
                if not ok:
                    logger.warning(f"NetherLink: 推送到群 {group} 未被接受，消息已丢弃")
            except Exception as e:
                logger.error(f"NetherLink: 推送到群 {group} 失败: {e}")

    async def _send_to_mc(self, payload: dict, server_id: str = "") -> bool:
        """向 MC 端发送一条下行消息。

        `server_id` 为空时：**只有恰好一台在线**才发送。0 台按「未连接」处理；
        多台则**拒绝发送并记 ERROR**——宁可丢一条也不要把指令发到错误的服务器上
        （多服并存时「随便挑一台」正是先前指令落到另一台服的原因）。
        """
        target = server_id
        if not target:
            alive = [sid for sid, c in self._mc_conns.items() if not c.closed]
            if not alive:
                logger.warning("NetherLink: MC 服务器未连接，消息丢弃")
                return False
            if len(alive) > 1:
                logger.error(
                    f"NetherLink: 有 {len(alive)} 台 MC 服务器在线"
                    f"（{'、'.join(sorted(alive))}），但本条消息未指定目标，已丢弃"
                )
                return False
            target = alive[0]
        conn = self._mc_conns.get(target)
        if conn is None or conn.closed:
            logger.warning(f"NetherLink: MC 服务器 [{target}] 未连接，消息丢弃")
            return False
        try:
            await conn.ws.send_str(json.dumps(payload, ensure_ascii=False))
            return True
        except Exception as e:
            logger.error(f"NetherLink: 发送到 MC [{target}] 失败: {e}")
            return False

    async def _broadcast_to_mc(self, payload: dict) -> int:
        """把一条下行消息发给**所有**在线的 MC 服务器，返回发成功的台数。

        与 `_send_to_mc` 的分工：
          · `_send_to_mc` 是**定向**发送（指令、机器人回复）——目标不明确就拒发，
            因为把指令发到错误的服务器比丢掉更糟
          · 本方法用于**天然属于全体**的消息（QQ 群友的聊天），广播是它的语义本身

        以前这里直接调 `_send_to_mc` 且不带 server_id，多台在线时会被那条
        「未指定目标」的保护整条拒掉——**QQ 消息完全进不了游戏**，而单服下
        看不出来（只有一台，照发）。这是多服务器改造的回归，2026-09-19 修。
        """
        sent = 0
        for sid, conn in list(self._mc_conns.items()):
            if conn.closed:
                continue
            try:
                await conn.ws.send_str(json.dumps(payload, ensure_ascii=False))
                sent += 1
            except Exception as e:
                logger.error(f"NetherLink: 广播到 MC [{sid}] 失败: {e}")
        if not sent:
            logger.warning("NetherLink: MC 服务器未连接，消息丢弃")
        return sent

    # ------------------------------------------------------------------
    # QQ 群消息 -> MC
    # ------------------------------------------------------------------
    @filter.on_astrbot_loaded()
    async def _bind_game_platform(self) -> None:
        """启动完成时把游戏侧适配器绑到本插件实例（必要时补加载）。

        必须是这个钩子，因为框架的启动顺序是：
            plugin_manager.reload()         <- 插件加载，__init__ 在这里跑
            platform_manager.initialize()   <- 平台实例化
            on_astrbot_loaded 钩子          <- 本方法在这里跑
        所以插件 __init__ 里去看适配器**必然**看到「尚未实例化」——
        2026-09-22 实测就是这么踩到的（日志里那句是误导）。

        同时做**补加载**：配置里有记录但框架没实例化出来时，由插件自己
        调 load_platform 拉起来。比只提示用户重启可靠——在 WebUI 里改完
        配置不重启也能生效。走 load_platform 而不是自己 new 实例，
        否则会绕过框架的生命周期管理（terminate/reload 都管不到它）。
        """
        if not self.enable_game_platform:
            return
        try:
            # 热重载时 `mc_platform` 会留在 `sys.modules` 里不被清掉
            # （框架只按 `data.plugins.<本插件>.*` 前缀清理，那是 **main.py 的**
            # 模块名，而本模块是以**顶层名** `mc_platform` 进缓存的）。
            # 后果：`@register_platform_adapter` 不会重跑，注册表里没有我们的
            # 类型，`have_adapter` 报 False → 需要重启。
            # 与 `_init_game_platform` 里那句「框架自动重新导入」的假设正相反，
            # 2026-09-22 用户实测到的重载报错就是这么来的。
            self._drop_platform_module_for_reload()
            adapter = self._resolve_game_adapter()
            if adapter is None:
                entry = self._game_platform_entry()
                if entry is not None and entry.get("enable"):
                    logger.info(
                        f"NetherLink: 平台 {GAME_PLATFORM_ID} 尚未实例化，正在补加载"
                    )
                    await self.context.platform_manager.load_platform(entry)
                    adapter = get_adapter(GAME_PLATFORM_ID)
            if adapter is None:
                logger.error(
                    f"NetherLink: 平台 {GAME_PLATFORM_ID} 仍未能实例化——"
                    "游戏内对话会回退到内置 agent。请查 AstrBot 启动日志里"
                    "有无该平台的加载报错。"
                )
                return
            # 与重载钩子同口径：实例可能是上次进程留下的旧类（改装后重启
            # 尤其常见），只 bind_plugin 换不掉它的方法代码，得重建。
            if type(adapter) is not self._current_adapter_class():
                logger.warning(
                    "NetherLink: 检测到陈旧的平台适配器实例（仍在运行旧代码），"
                    "正在重建它"
                )
                entry = self._game_platform_entry()
                if entry is not None:
                    try:
                        await self.context.platform_manager.reload(entry)
                    except Exception as e:
                        logger.error(f"NetherLink: 重建平台适配器失败: {e}")
                    adapter = self._find_live_adapter()
            if adapter is None:
                logger.error(
                    "NetherLink: 平台适配器重建后仍不可用，游戏内对话将回退。"
                )
                return
            adapter.bind_plugin(self)
            logger.info(
                f"NetherLink: 游戏侧平台适配器已就绪并绑定（id={GAME_PLATFORM_ID}）"
            )
            self._warn_about_display_flags()
        except Exception as e:
            logger.error(f"NetherLink: 绑定游戏侧平台适配器失败: {e}")

    @staticmethod
    def _drop_platform_module_for_reload() -> None:
        """热重载时主动丢弃 `mc_platform` 模块，让下次导入重新执行注册。

        为什么需要它：平台类型是靠 `@register_platform_adapter` 在**导入期**
        登记进 `platform_cls_map` 的。而插件重载时框架只清理
        `data.plugins.<插件名>.*` 前缀下的模块；`mc_platform` 是以**顶层名**
        进 `sys.modules` 的（main.py 的双形态导入兜底分支），不在清理范围内。
        它留在缓存里 → 装饰器不重跑 → 注册表空 → 插件以为「适配器没实例化」，
        提示用户重启（2026-09-22 实测）。

        注意：框架的 `unregister_platform_adapters_by_module` 已经把旧的
        类型从注册表摘掉了，所以这里只需让它**重新导入**一次即可补回来。
        """
        try:
            import sys as _sys

            _sys.modules.pop("mc_platform", None)
        except Exception:
            pass

    def _find_live_adapter(self):
        """在 `platform_insts` 里找**正在运行**的游戏侧适配器实例。

        这是比模块级 `_ADAPTERS` 字典更权威的来源：热重载时 `mc_platform`
        会被重新导入，那个字典是**新的、空的**，而实例是**老的**——只查字典
        会以为「没有实例」，进而错误地再 `load_platform` 一个，
        于是两个实例并存（`send_message` 只匹配第一个，回复可能走旧实例）。
        """
        try:
            insts = getattr(self.context.platform_manager, "platform_insts", None)
        except Exception:
            return None
        for inst in insts or []:
            try:
                if inst.meta().id == GAME_PLATFORM_ID:
                    return inst
            except Exception:
                continue
        return None

    @staticmethod
    def _lookup_game_adapter():
        """按模块级注册表取适配器（两种导入形态都试一次）。"""
        for attempt in (0, 1):
            try:
                if attempt == 0:
                    from mc_platform import get_adapter
                else:
                    from .mc_platform import get_adapter
                return get_adapter(GAME_PLATFORM_ID)
            except ImportError:
                continue
        return None

    def _resolve_game_adapter(self):
        """取游戏侧适配器：**先用运行中的实例**，其次才查模块注册表。"""
        return self._find_live_adapter() or self._lookup_game_adapter()

    def have_adapter(self) -> bool:
        """平台路径此刻是否真的可用（适配器在、且已绑到本插件实例）。"""
        try:
            adapter = self._resolve_game_adapter()
        except Exception:
            return False
        return adapter is not None and adapter._plugin is not None

    @filter.on_plugin_loaded()
    async def _rebind_game_platform_after_reload(self, metadata) -> None:
        """插件热重载后，把**仍然活着的**适配器重新绑到新插件实例。

        为什么需要它：重载会换掉插件对象，但框架只清理注册表里的类型，
        **不停掉已在运行的平台实例**（star_manager 的 unregister 只动
        `platform_cls_map`）。于是旧实例继续活着、却还引用着**旧插件对象**：
        读的是旧配置，写的是旧状态。修法是每次插件加载完都重绑一次。

        为什么不走适配器的模块级注册表（`get_adapter`）：重载时
        `mc_platform` 会被重新导入，那个字典是**新的、空的**，而实例是
        **老的**——两边对不上。实例存活的位置是 `platform_insts`，从那里找。
        """
        if not self.enable_game_platform:
            return
        try:
            name = getattr(metadata, "name", "") or ""
            # 钩子对**每个**插件加载都触发，必须只认自己
            if name and name != PLUGIN_NAME:
                return
            inst = self._find_live_adapter()
            if inst is None:
                return  # 实例不存在（首次启用还没重启过），交给启动钩子处理

            # ⚠️ 陈旧实例检测——这是本插件最难发现的一类 bug。
            # 框架热重载只把**类**从注册表摘掉，**不停掉正在运行的实例**
            # （star_manager 的 unregister 只动 platform_cls_map）。
            # 而 Python 实例持有的是**类对象引用**：mc_platform 重新导入后
            # 产生的是新类，旧实例仍指向旧类——于是它跑的是**旧代码**，
            # 插件这边改多少遍、跑在游戏里的都还是老逻辑（2026-09-22 实测：
            # 三轮修复全无效果，因为线上的实例根本没有那些代码）。
            # 只 bind_plugin 换不掉实例的方法代码，必须**重建**。
            if type(inst) is not self._current_adapter_class():
                logger.warning(
                    "NetherLink: 检测到陈旧的平台适配器实例（仍在运行旧代码），"
                    "正在重建它"
                )
                entry = self._game_platform_entry()
                if entry is not None:
                    try:
                        await self.context.platform_manager.reload(entry)
                    except Exception as e:
                        logger.error(f"NetherLink: 重建平台适配器失败: {e}")
                    inst = self._find_live_adapter()
            if inst is None:
                return
            inst.bind_plugin(self)
            logger.info(
                "NetherLink: 重载后已把游戏侧平台适配器重新绑定到新插件实例"
            )
        except Exception as e:
            logger.error(f"NetherLink: 重载后重绑平台适配器失败: {e}")

    @staticmethod
    def _current_adapter_class():
        """取**当前**模块里的适配器类，用于识别陈旧实例。

        导入前先丢弃缓存的模块，确保拿到的是刚加载进来的那份定义。
        """
        try:
            import sys as _sys

            for attempt in (0, 1):
                if attempt == 0:
                    import mc_platform as _mod
                else:
                    from . import mc_platform as _mod
                _sys.modules.setdefault("mc_platform", _mod)
                return getattr(_mod, "NetherLinkMcAdapter", None)
        except Exception:
            return None

    def _game_platform_entry(self) -> Optional[dict]:
        """取配置里那条游戏侧平台记录（没有则 None）。"""
        try:
            platforms = self.context.get_config().get("platform")
        except Exception:
            return None
        if not isinstance(platforms, list):
            return None
        for item in platforms:
            if isinstance(item, dict) and str(item.get("id")) == GAME_PLATFORM_ID:
                return item
        return None

    def _warn_about_display_flags(self) -> None:
        """提示用户打开 AstrBot 的三个展示开关。

        它们是「能不能看到 AI 在干什么」的总闸，且默认全关；不开的话
        注入与工具都正常、只是界面上看不到，极易被当成「插件没生效」。
        """
        try:
            ps = self.context.get_config().get("provider_settings") or {}
        except Exception:
            return
        want = [
            ("show_tool_use_status", "显示工具调用状态"),
            ("show_tool_call_result", "显示工具返回结果"),
            ("display_reasoning_text", "显示思考过程"),
        ]
        off = [label for key, label in want if not ps.get(key)]
        if off:
            logger.info(
                "NetherLink: 以下 AstrBot 开关当前是关的，游戏内看不到 AI 的"
                f"工具调用/思考：{chr(12289).join(off)}。"
                "要看到请在 WebUI 的提供商设置里打开对应项。"
            )

    @filter.on_llm_request()
    async def inject_qq_identity(self, event, req) -> None:
        """给 QQ 侧的 LLM 请求补上完整上下文。

        注入的是 `_build_context` 的全部产物：好感规则 + 查好感的引导 +
        在线服务器清单 + 管理员与来源（`<netherlink_context>`）。
        方法名保留 `inject_qq_identity` 是历史原因（起初只注身份），
        改名会牵动 AstrBot 的 handler 注册，收益不大。

        为什么需要它：QQ 侧**普通对话**走的是 AstrBot 主 agent，其 system_prompt
        由框架内建、插件注入不进去（见 claude.md 的权限模型一节）。于是群友只是
        跟 AI 聊天时，AI 完全不知道对方是谁。此前只有「群友请求执行指令、插件另起
        内层 agent」那条路径的 system_prompt 归插件管；内层 agent 已于 2026-09-21
        删除，QQ 侧只剩主 agent 一条路径，本钩子因此是唯一的注入点。它由 AstrBot
        提供（`astrbot/api/event/filter/__init__.py` 导出，
        `register_on_llm_request` 定义）。

        ⚠️ **这个钩子是全局的**：对**每一个** LLM 请求都会触发（包括其他插件的
        请求，如用户画像分析、AstrBot 自身的定时任务）。因此必须严格认准来源，
        绝不污染别人的提示词——只处理 **aiocqhttp 的群消息**（判据见下与
        `_is_aiocqhttp_event`）：私聊、其他平台、其他插件构造的请求一律不碰。

        幂等：system_prompt 里已有 `<netherlink_context>` 就不再追加。
        ⚠️ 这不是可有可无的优化：钩子是全局的，同一个 `ProviderRequest` 可能
        被重复处理，不去重会把整段上下文叠两次——AI 会同时看到两份身份，
        后一份还带着重复的好感规则。判断点放在**两条分支之前**，两份默认模板
        都带这个标签（游戏侧 2026-09-22 补回了包裹标签）。

        游戏侧**不再走自建 agent**（2026-09-22 删除）：它与 QQ 侧同为本钩子
        的两个分支，game 分支由 `_build_game_context` 拼装、QQ 分支由
        `_build_context` 拼装。

        2026-09-20 起**不再看 target_groups**：那项现在只管消息互通（转发与
        推群），本钩子对所有 aiocqhttp 群生效——未绑定群里的 AI 也认得出
        发起者与管理员。
        """
        try:
            # 幂等：这个请求已经注入过了就不再追加。见 docstring 里的说明——
            # **必须在两条分支之前**，否则重复处理时两边都会各叠一份。
            if "<netherlink_context>" in (req.system_prompt or ""):
                return
            # ⚠️ 必须先判游戏侧，再判 aiocqhttp。
            # 这两条是**互斥的两个场景**，但原先写成
            #     if not self._is_aiocqhttp_event(event): return
            # 而游戏侧的平台 id 不是 aiocqhttp —— 于是游戏侧事件在这一行就被
            # 整条挡掉，下面的游戏侧分支**永远到不了**（2026-09-22 排查时发现）。
            # 症状是静默的：游戏内对话照常进行，只是 AI 拿不到身份与管理信息。
            if self._is_game_event(event):
                # 游戏侧的身份**唯一来源**是本函数算出来的这份，工具与提示词都读它。
                # 为什么不能靠「AI 从历史里推断当前说话人」：会话是按**服务器**建的，
                # 一台服上所有玩家共用一个会话，历史里只有 `[A] 帮我传送` 这类没有
                # 主语的句子。AI 于是把指令挂到历史里最近出现过的那个人身上——
                # 「指令执行到上一个说话人身上」就是这么来的（2026-09-21 实测）。
                # 游戏侧的来源是 1:1 的（连接即身份），从 session_id 取出 server_id
                # 即可，不必像 QQ 侧那样查平台实例。
                direct = event.get_extra("netherlink_direct_initiator") or {}
                player = str(
                    direct.get("player")
                    or event.get_sender_name()
                    or event.get_sender_id()
                    or "?"
                )
                server_id = str(direct.get("server_id") or event.get_session_id() or "")
                # 工具在**同一次请求内**读这份 extra 取发起者，不再靠闭包
                # （闭包那套只有在插件自建 agent 时才成立）。
                event.set_extra(
                    "netherlink_ctx",
                    {"source": "game", "player": player, "server_id": server_id},
                )
                ctx = await self._build_game_context(
                    player, self._game_is_admin(player), server_id
                )
                req.system_prompt = ((req.system_prompt or "").rstrip() + "\n\n" + ctx).strip()
                logger.info(
                    f"NetherLink: 已为游戏侧对话注入身份 —— 发起者 {player}，"
                    f"服务器 {self._mc_server_display(event.get_session_id())}"
                )
                return
            if not self._is_aiocqhttp_event(event):
                return  # 私聊 / 其他平台 / 其他插件构造的请求，一律不碰
            identity = str(event.get_sender_name() or event.get_sender_id() or "?")
            is_admin = self._qq_is_admin(event)
            ctx = await self._build_context(identity, is_admin)
            req.system_prompt = ((req.system_prompt or "").rstrip() + "\n\n" + ctx).strip()
            # 注入是静默的，出问题时从日志完全看不出它有没有跑——留一条痕，
            # 排查「AI 认不出管理员」时先看这行有没有出现。
            logger.info(
                f"NetherLink: 已为 QQ 侧对话注入身份 —— 发起者 {identity}，"
                f"{'是' if is_admin else '不是'}管理员"
            )
        except Exception as e:
            # 注入失败不能连累对话本身
            logger.error(f"NetherLink: 注入 QQ 侧身份上下文失败: {e}")

    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def on_group_message(self, event):

        """绑定群的普通消息转发进游戏公屏；游戏侧事件则在此打开 LLM 阀门。

        ⚠️ **`event.is_at_or_wake_command` 必须在这里补上**（2026-09-22 实测定位）。
        `ProcessStage` 只在它为真时才发起 LLM 请求：
            if (not event._has_send_oper and event.is_at_or_wake_command
                    and not event.call_llm):  ->  agent_sub_stage
        而它**只由 `WakingCheckStage` 的「唤醒词 / @ / 私聊」三条路**置真——
        插件注册的普通 handler 只会把 `is_wake` 置真，**不碰这个字段**。
        症状：游戏内消息一路走到 `ProcessStage`、handler 也被调用了，
        然后管道静默结束（`pipeline execution completed.`），
        **LLM 请求根本没发起**，日志里连一条报错都没有。

        顺序是安全的：`ProcessStage` 先跑 handler（stage.py:37），
        再检查这个字段（stage.py:56）。
        """
        try:
            # 游戏侧（原生平台）的消息不能被 target_groups 拦住——那项只管
            # QQ 群的消息互通。这里放行并打开 LLM 阀门。
            if event.get_platform_id() == GAME_PLATFORM_ID:
                event.is_at_or_wake_command = True
                return
            group_id = str(event.get_group_id() or "")
            if group_id not in self.target_groups:
                return
            # 记下这条真实会话的 umo，供 _broadcast 主动推送时取首段（平台标识）。
            # 放在开关校验之前：即便 QQ->MC 转发关着，这条信息依然有效且有用。
            self._qq_umo_seen = event.unified_msg_origin
            if not self.config.get("enable_qq_to_mc", True):
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
            # 群消息天然属于全体：广播给所有在线服务器（见 _broadcast_to_mc）
            await self._broadcast_to_mc({"type": "chat", "line": line})
        except Exception as e:
            logger.error(f"NetherLink: QQ->MC 转发失败: {e}")

    # ------------------------------------------------------------------
    # 游戏侧身份：两个工具共用的取用点
    # ------------------------------------------------------------------
    @staticmethod
    def _game_identity_from(event):
        """若这个事件来自**游戏侧**，返回 {"player", "server_id"}；否则 None。

        这是两个工具判定「我该按游戏侧还是 QQ 侧执行」的**唯一**依据。
        不再有闭包绑定（那套只在插件自建 agent 时成立），也没有 player 参数
        ——否则 AI 能给任意玩家刷好感、或让指令落到别人头上（实测出现过）。
        """
        try:
            extra = event.get_extra("netherlink_ctx")
        except Exception:
            return None
        if not isinstance(extra, dict) or extra.get("source") != "game":
            return None
        player = str(extra.get("player") or "")
        server_id = str(extra.get("server_id") or "")
        if not player:
            return None
        return {"player": player, "server_id": server_id}

    async def _run_game_command(self, ctx_game: dict, cmd: str, cost) -> str:
        """游戏侧发起者的指令执行（来源与身份都由连接确定）。"""
        out = await self.exec_command_for(
            initiator=ctx_game["player"],
            cmd=str(cmd or "").strip().lstrip("/"),
            source="game",
            cost=cost,
            server_id=ctx_game["server_id"],
        )
        return out[:MC_REPLY_MAX_LEN] if out else out

    # ------------------------------------------------------------------
    # LLM 函数工具：QQ 群聊自然语言 -> MC 指令（AstrBot 自动注册给群聊 LLM）
    # ------------------------------------------------------------------
    @filter.llm_tool(name="mc_command")
    async def mc_command(self, event, cmd: str, cost: int, server: str = "") -> str:
        """在 Minecraft 服务器上以控制台身份执行一条指令,并返回服务器真实输出.
        执行前先按下述情形判断:
        一律拒绝的指令(无论对方好感多少都不执行):清空或重置成就进度这类能让对方
        反复刷成就的,召唤末影龙/凋零等明显影响服务器的高危指令,破坏他人建筑,
        清空区域,封禁他人等伤害其他玩家的指令.另外,
        刷怪笼(spawner)原版无法获得的方块,与各类刷怪蛋(spawn_egg)一律不给玩家,玩家头颅除外.
        遇到这类请求直接说明原因并拒绝,不要调用本工具.
        其余情形按 cost 参数的说明报价与执行.
        好感扣除由本工具自己完成:你在 cost 里报出价格,插件会在同一次调用内原子扣减
        (不足则拒绝,执行失败则退回).不要用 mc_karma 再扣一次——mc_karma 只用于
        对话性的好感增减,不用于支付指令费用.
        若当前发起者是管理员(身份见系统提示词中的 <netherlink_context>),你可以给他更宽松的尺度:
        对他的高危指令更倾向于放行,好感不足时也可以通融;但放行与否仍由你按指令本身和当前情境判断,不是无条件执行.

        Args:
            cmd(string): 完整的 Minecraft 指令,不带开头的斜杠,例如 list 或 gamemode creative Steve
            cost(number): 你为本次执行报出的好感消耗(纯查询类指令传 0)
            server(string): 指令发往哪台 MC 服务器(填 server_id 或显示名).只有一台在线时可省略
        """
        try:
            # 这里**没有**任何连接性前置判断：好感度是本地功能，服务器离线时
            # 仍应走到 exec_command_for（它自己会拒绝并退费），提前拦截会把
            # 整个 LLM 挡在门外、连带废掉好感度。
            ctx_game = self._game_identity_from(event)
            if ctx_game is not None:
                # 游戏侧：来源由**连接**决定，不经过 AI——玩家在 A 服说话，
                # 指令就该发到 A 服，没有让 AI 判断「填哪台」的余地。
                # 因此忽略 server 参数，并把发起者锁成 event 上带的那个人。
                return await self._run_game_command(ctx_game, cmd, cost)
            qq = str(event.get_sender_id() or "")
            identity = str(event.get_sender_name() or qq)
            # 决策与执行都在本函数内完成（2026-09-21 删掉 QQ 侧内层 agent：
            # karma_rules 已直接注入主 agent，报价依据可见，不再需要复核层）。
            # 代价是少一层复核，收益是每次指令由 2 次 LLM 往返降到 1 次。
            target = self._resolve_target_server(server)
            # 填了名字却不在线时**不执行**：宁可如实告知，也不让消息落到
            # _send_to_mc 的「多台在线时拒发」分支——那会让 AI 收到一个
            # 与它的意图无关的失败。
            if server and not target:
                online = self._online_servers()
                names = "、".join(f"{d}（{s}）" for s, d in online) or "（当前无在线服务器）"
                return f"没有名为「{server}」的服务器在线。在线的是：{names}"
            # 对称的那条：**没填 server 且多台在线**时同样提前拦下。
            # 不拦的话消息会落到 _send_to_mc 的「拒发」分支，而它只返回 False、
            # 不带原因，最终被翻译成「未收到服务器回执（可能仍在执行）」——
            # 一句**事实上错误**的话（根本没发送），AI 据此判断「服务器掉线了」，
            # 给用户的答复是「两台服务器都没回执，去问问管理员」（2026-09-22 实测）。
            if not server and len(self._online_servers()) > 1:
                online = self._online_servers()
                names = "、".join(f"{d}（server_id: {s}）" for s, d in online)
                return (
                    f"当前有 {len(online)} 台 MC 服务器在线，必须用 server 参数指明"
                    f"发往哪一台（填 server_id 或显示名都可）。在线的是：{names}"
                )
            out = await self.exec_command_for(
                initiator=identity, cmd=cmd, source="qq", qq=qq,
                cost=cost, server_id=target,
            )
            # 与游戏侧同样限长：服务器输出可能很长（满背包的 NBT、多人 list），
            # 整段塞进 LLM 上下文会持续膨胀。删内层 agent 时这段截断一度丢失。
            return out[:MC_REPLY_MAX_LEN] if out else out
        except Exception as e:
            logger.error(f"NetherLink: mc_command 执行失败: {e}")
            return f"执行出错: {e}"

    @filter.llm_tool(name="mc_karma")
    async def mc_karma(self, event, delta: int) -> str:
        """查询或增减当前对话发起者的好感度,无法指定其他玩家.
        delta 为 0 时只查询,为正数时增加好感,为负数时扣除好感.
        增减依据见好感度规则;执行指令要消耗多少,见 mc_command 的描述.

        Args:
            delta(number): 好感变化量,正增负减;只查询时传 0"""
        try:
            ctx_game = self._game_identity_from(event)
            if ctx_game is not None:
                # 游戏侧的 key 空间是 mc:{玩家名}，与 QQ 侧的 qq:{QQ号} 相互独立
                # （同一个人在两边是两份好感，这是刻意的，不做映射）。
                key = identity_key("game", ctx_game["player"], "")
                origin = "游戏内对话"
            else:
                qq = str(event.get_sender_id() or "")
                key = identity_key("qq", str(event.get_sender_name() or qq), qq)
                origin = "QQ 对话"
            if not int(delta or 0):
                return f"[{key}] 当前好感值：{await self._karma_get(key)}（范围 {self.karma_min}~{self.karma_max}）。"
            old, new = await self._karma_add(key, int(delta), origin=origin)
            return f"[{key}] 好感值 {old} → {new}（本次 {int(delta):+d}）。"
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
        for conn in list(self._mc_conns.values()):
            if not conn.closed:
                try:
                    await conn.ws.close()
                except Exception as e:
                    logger.warning(f"NetherLink: 关闭 MC 连接失败（忽略）: {e}")
        self._mc_conns.clear()
        self._port_owner.clear()
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
