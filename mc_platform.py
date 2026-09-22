# -*- coding: utf-8 -*-
# `MessageChain | None` 这类新式联合注解在 3.9 上会在**函数定义时**求值并抛
# TypeError（不是 3.10+ 的惰性求值）。本项目的测试跑在 3.9，加上这一行让注解
# 变成字符串、不在定义期求值。
from __future__ import annotations

"""NetherLink 游戏侧平台适配器 —— 把 MC 服务器伪装成 AstrBot 里的一个「群」。

背景（为什么要这么做）：
    原先游戏内对话由插件**自建 agent**（`context.tool_loop_agent(...)`）驱动。
    那条路径**绕过了整个 pipeline**，代价是：
      · AstrBot 的 agent 级钩子根本不触发——`star/context.py` 里
        `agent_hooks = kwargs.get("agent_hooks") or BaseAgentRunHooks()`，
        不显式传就是**空实现**，所以 `🔨 调用工具` 那类提示永远拿不到；
      · 工具参数靠插件自己拼，与 QQ 侧两套实现各自漂移；
      · 步数用尽时 runner 会拔掉工具并强推一段（回复分两段）。
    改成一个**真实的平台适配器**后，游戏内消息变成真正的 AstrBot 事件，
    走完整 pipeline：人格、记忆、工具状态提示、推理文本全都原生可用。

会话单位：**一台 MC 服务器 = 一个会话**（用户 2026-09-21 明确要求）。
    umo = `netherlink_mc:GroupMessage:<server_id>`
    玩家身份不进 umo——否则每来一个玩家就多一个会话，AI 看不到该服其他人
    在聊什么，这恰是用户要保留的能力。玩家身份改由**每轮事件**携带
    （见 `handle_bot_chat` 与 main.py 的注入钩子）。

启动方式（重要，两条路都留着）：
    AstrBot 在 `core_lifecycle` 里**先加载插件、后初始化平台**
    （`plugin_manager.reload()` 在 `platform_manager.initialize()` 之前），
    所以只要 `config["platform"]` 里**已有**一条 type=netherlink_mc 的记录，
    本适配器就会在启动时被框架正常实例化。
    该记录由 `NetherLinkPlugin._ensure_platform_config()` 在插件 `__init__`
    里补写（幂等），用户也可以在 WebUI 里手动添加（注册后它会出现在
    「添加平台」列表里——`dashboard/services/config_service.py` 会遍历
    `platform_registry` 合并进 config_template）。

热重载的坑：插件重载时 `star_manager` 只把适配器类从注册表移除，
**不会停掉正在跑的实例**，于是旧代码的实例会继续活着。因此插件侧用
`_bind_plugin()` 在每次加载时把 `self._plugin` 重新指到新插件对象，
而不是各存一份状态。
"""

import asyncio
import time

from astrbot.api import logger
from astrbot.api.platform import (
    AstrBotMessage,
    AstrMessageEvent,
    Group,
    MessageMember,
    MessageType,
    Platform,
    PlatformMetadata,
    register_platform_adapter,
)
from astrbot.core.message.components import Plain
from astrbot.core.message.message_event_result import MessageChain

# 平台标识：必须与 main.py 的 GAME_PLATFORM_ID 一致（那边有守卫钉住单一来源）。
# 注意 `PlatformManager._is_valid_platform_id` 禁止 id 里出现 `:` 与 `!`
# （umo 拿它做分隔符），所以这里只能是朴素标识符。
PLATFORM_ID = "netherlink_mc"
PLATFORM_TYPE = "netherlink_mc"

# 已实例化的适配器：platform_id -> adapter。
# 为什么用模块级字典而不是让插件持有：框架是用
# `cls_type(platform_config, settings, event_queue)` 构造我们的，
# **没有把插件对象传进来**。所以只能由适配器自己在 __init__ 里登记，
# 插件再用 _bind_plugin() 反向绑定。
_ADAPTERS: dict = {}


def get_adapter(platform_id: str = PLATFORM_ID):
    """取当前活着的适配器实例；没有则返回 None。"""
    return _ADAPTERS.get(platform_id)


class NetherLinkMcEvent(AstrMessageEvent):
    """游戏侧的事件。

    必须覆写 `send` / `send_streaming`：基类的实现**只上报 metrics、不发消息**
    （`astr_message_event.py`），不覆写的话所有回复与「🔨 调用工具」提示
    都会被静默吞掉——这正是本项目最忌讳的静默失效。
    """

    def __init__(self, message_str, message_obj, platform_meta, session_id, adapter):
        super().__init__(message_str, message_obj, platform_meta, session_id)
        self._adapter = adapter

    async def send(self, message: MessageChain | None) -> None:
        if message is None:
            return
        # 过程消息（工具调用/结果/思考）只进游戏；最终回复才同步 QQ。
        await self._adapter.deliver_chain(message, self.session_id)
        await super().send(MessageChain([]))

    async def send_streaming(self, generator, use_fallback: bool = False) -> None:
        """流式兜底。

        正常路径下我们在注入钩子里把 `enable_streaming` 关掉
        （见 main.py），消息走 `send()`。但**不能因此不实现这里**：
        用户换个 AstrBot 版本、或那个 extra 失效时，框架会把
        `run_agent` 的异步生成器交到这里，不实现就整段丢失。
        """
        async for chain in generator:
            # "break" 是「分段」标记（空链），不是内容
            if getattr(chain, "type", "") == "break":
                continue
            if chain is None or not chain.chain:
                continue
            # ⚠️ 必须与 send() 同口径地传 sync_qq：这里原先漏了，默认 False，
            # 于是**只要走流式路径，AI 的回复就永远不同步到 QQ 群**
            # （2026-09-22 用户实测：游戏群聊里能收到、其他群收不到）。
            await self._adapter.deliver_chain(chain, self.session_id)
        # 基类实现是空操作（真机上只上报 metrics），调它是为了保持框架契约；
        # 但测试替身的基类未必有这个方法，缺失时不该让整条回复丢掉。
        parent = getattr(super(), "send_streaming", None)
        if parent is not None:
            await parent(generator, use_fallback)


@register_platform_adapter(
    PLATFORM_TYPE,
    "NetherLink 游戏内模拟群聊（由 NetherLink 插件自动管理，一般无需手动配置）",
    default_config_tmpl={
        # register_platform_adapter 会补 type/enable/id，这里显式写清楚，
        # 便于用户在 WebUI 里看到与手动添加。
        "type": PLATFORM_TYPE,
        "id": PLATFORM_ID,
        "enable": True,
        "netherlink_managed": True,
    },
    support_streaming_message=False,
)
class NetherLinkMcAdapter(Platform):
    def __init__(self, platform_config: dict, platform_settings: dict, event_queue) -> None:
        super().__init__(platform_config, event_queue)
        self.settings = platform_settings
        self._meta = PlatformMetadata(
            name=PLATFORM_ID,
            description="NetherLink 游戏内模拟群聊",
            id=PLATFORM_ID,
            # 不支持真流式：MC 公屏逐字刷新没有意义，且非流式路径
            # 才会把 `🔨 调用工具` 作为独立消息 send() 出来。
            support_streaming_message=False,
            support_proactive_message=True,
        )
        self._shutdown = asyncio.Event()
        # 由 main.py 在插件加载时反向绑定；直接构造实例时可能仍是 None，
        # 所以每个用到的地方都要判空——绝不能因为没绑上就崩掉事件处理。
        self._plugin = None
        _ADAPTERS[PLATFORM_ID] = self
        logger.info(
            f"NetherLink: 游戏侧平台适配器已创建（id={PLATFORM_ID}）"
        )

    # ------------------------------------------------------------------
    # 插件绑定
    # ------------------------------------------------------------------
    def bind_plugin(self, plugin) -> None:
        """把适配器重新指到**当前**的插件实例。

        热重载后旧实例还活着，但它引用的旧插件对象状态可能是陈旧的；
        每次插件加载都重新绑一次，避免两份状态并存。
        """
        self._plugin = plugin

    def meta(self) -> PlatformMetadata:
        return self._meta

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------
    def run(self):
        """框架会 `asyncio.create_task(inst.run())`。

        本适配器是**被动**的：消息由插件通过 handle_bot_chat 直接 commit，
        发送由 deliver_chain 直接写 WS，没有需要常驻轮询的东西。
        所以这里只要挂住不返回——返回了 `_task_wrapper` 会把它当异常退出。
        """
        async def _idle():
            await self._shutdown.wait()

        return _idle()

    async def terminate(self) -> None:
        self._shutdown.set()
        _ADAPTERS.pop(PLATFORM_ID, None)
        logger.info("NetherLink: 游戏侧平台适配器已停止")

    # ------------------------------------------------------------------
    # 入站：玩家消息 -> 真实事件 -> 交给 pipeline
    # ------------------------------------------------------------------
    def _commit(
        self,
        player: str,
        server_id: str,
        text: str,
        display_name: str = "",
        kind: str = "chat",
    ) -> None:
        """把一条游戏侧消息变成 AstrBot 事件并推进队列（各入口共用）。

        - `session_id` 用 **server_id**：一台服一个会话（用户明确要求）。
        - `type` 用 GROUP_MESSAGE：语义上就是一个「群」，且能让
          `unique_session` 的按玩家隔离逻辑不生效（那个 builder 只认
          内置平台名，认不出我们）。
        - `self_id` 用 PLATFORM_ID：与 sender_id（玩家名）不同即可，
          避免被 `ignore_bot_self_message` 误判成机器人自己的消息。
        - `kind`：`chat` = 玩家在说话；`advancement` = 服务器发来的系统通知，
          不是玩家说的话（2026-09-22 用户实测：AI 会把成就通知当成玩家在
          邀功）。放进 extra 供注入钩子区分。
        """
        if not server_id:
            logger.warning(
                "NetherLink: 收到游戏内消息但没有 server_id，无法定位会话，已丢弃"
            )
            return
        try:
            msg = AstrBotMessage()
            msg.type = MessageType.GROUP_MESSAGE
            msg.self_id = PLATFORM_ID
            msg.session_id = server_id
            msg.message_id = f"{server_id}-{int(time.time() * 1000)}"
            msg.sender = MessageMember(user_id=player, nickname=player)
            # ⚠️ **必须建 Group**：没有它 `get_group_id()` 返回空，
            # AstrBot 的 `get_event_auto_name` 就会退回用**发送者名字**
            # 给会话命名——于是对话列表里「群名」显示成了玩家昵称
            # （2026-09-22 用户实测到的）。group_name 用服务器显示名，
            # 认不出时退回 server_id。
            msg.group = Group(
                group_id=server_id, group_name=display_name or server_id
            )
            msg.message = [Plain(text)]
            msg.message_str = text
            msg.raw_message = {"type": kind, "player": player, "text": text}

            event = NetherLinkMcEvent(
                message_str=text,
                message_obj=msg,
                platform_meta=self._meta,
                session_id=server_id,
                adapter=self,
            )
            # 身份落进 extra：注入钩子与工具都读它，不靠闭包
            # （闭包只在插件自建 agent 时才成立）。
            event.set_extra(
                "netherlink_ctx",
                {
                    "source": "game",
                    "player": player,
                    "server_id": server_id,
                    "kind": kind,
                },
            )
            # 关掉流式：MC 聊天框里一条一条刷没有意义，而且流式会把
            # 「🔨 调用工具」之类的通知拆散。**非流式路径**才会把
            # `send()` 的结果作为一条完整消息发出去（internal.py:155 的
            # `enable_streaming` extra 是官方提供的开关）。
            event.set_extra("enable_streaming", False)
            self.commit_event(event)
            logger.info(
                f"NetherLink: 游戏内消息已入队 [{server_id}] <{player}> "
                f"({kind}) {text[:40]}"
            )
        except Exception as e:
            logger.error(f"NetherLink: 构造游戏侧事件失败: {e}")

    def handle_bot_chat(
        self, player: str, server_id: str, text: str, display_name: str = ""
    ) -> None:
        """玩家用唤醒词对 AI 说话。"""
        self._commit(player, server_id, text, display_name, kind="chat")

    def handle_advancement(
        self, player: str, server_id: str, text: str, display_name: str = ""
    ) -> None:
        """玩家获得成就——**服务器发来的系统通知**，不是玩家说的话。

        `text` 是插件按 `advancement_prompt` 渲染好的整段（占位符已替换成
        客观事实）。走同一条管线的好处：人格、工具、出站推群全部复用，
        而且与玩家对话进**同一个会话**，AI 有上下文。
        """
        self._commit(player, server_id, text, display_name, kind="advancement")

    # ------------------------------------------------------------------
    # 出站：AstrBot 要发给这个「群」的内容 -> MC 下行
    # ------------------------------------------------------------------
    async def deliver_chain(self, chain: MessageChain, session_id: str) -> None:
        """把一条消息链发回游戏，并按需要同步到 QQ 群。

        **这里是「要不要推群」的唯一决策点**，不再由调用方传布尔值。
        早先把 `sync_qq` 从事件一路传到插件，漏传两次（流式那条分支、
        `send_by_session` 那条），症状都是「游戏里看得到、群里看不到」，
        且**完全静默**（默认值恰好是 False）。现在只有这一处判断，
        三条出站路径（send / send_streaming / send_by_session）都经过它。
        """
        plugin = self._plugin
        if plugin is None:
            logger.error(
                "NetherLink: 适配器尚未绑定插件实例，消息无法送达游戏（丢弃）"
            )
            return
        try:
            text = _chain_to_plain(chain)
            if not text:
                return
            # ① 游戏公屏：回复与过程消息都发
            await plugin.send_game_line(text, session_id)
            # ② QQ 群：只有 AI 的**最终答复**才同步。
            #    过程消息（🔨 调用工具 / 📎 返回结果 / 思考）不进群，
            #    否则群友会被每一步工具调用刷屏。
            if not _is_process_chain(chain):
                await plugin.sync_bot_reply_to_qq(text, session_id)
            else:
                # 常驻一行：出问题时这是**唯一**能立刻分辨「被判定为过程
                # 消息」还是「推群那步失败」的地方（2026-09-22 排查时
                # 正是缺了这一行，导致连查三轮）。
                logger.info(f"NetherLink: 过程消息只进游戏，不同步 QQ: {text[:30]}")
        except Exception as e:
            logger.error(f"NetherLink: 游戏侧消息下发失败: {e}")

    async def send_by_session(self, session, message_chain: MessageChain) -> None:
        """`context.send_message(umo, chain)` / 框架主动推送走这里。"""
        sid = getattr(session, "session_id", "") or ""
        await self.deliver_chain(message_chain, sid)
        await super().send_by_session(session, message_chain)


# 会被 AstrBot 标成这几种 type 的是**过程消息**（工具调用/工具结果/思考），
# 不是 AI 的答复。它们只进游戏公屏，不同步 QQ——否则群友会被刷屏，
# 而且那些文本（`🔨 调用工具: mc_command`）对群里也没有意义。
_PROCESS_CHAIN_TYPES = frozenset({"tool_call", "tool_call_result", "reasoning"})


def _is_process_chain(chain: MessageChain) -> bool:
    """这条消息链是不是「AI 在干活的过程」而非「AI 的答复」。"""
    return getattr(chain, "type", "") in _PROCESS_CHAIN_TYPES


def _chain_to_plain(chain: MessageChain) -> str:
    """把消息链压成一行纯文本。

    只取 Plain：MC 公屏放不下图片/文件，At/Json 在本平台上也没有意义。
    未知组件**跳过而不是抛错**——抛错会让整条回复消失。
    """
    if chain is None or not chain.chain:
        return ""
    parts = []
    for comp in chain.chain:
        text = getattr(comp, "text", None)
        if isinstance(text, str) and text:
            parts.append(text)
    return "\n".join(parts).strip()
