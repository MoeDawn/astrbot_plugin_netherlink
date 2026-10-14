# -*- coding: utf-8 -*-
"""NetherLink 事件分发 / 跨服服务（2026-10-14 重构阶段三 Task 2）。

从 main.py 的 NetherLinkPlugin 抽出**事件分发域**：MC 事件按模板渲染后推群
（dispatch_mc_event，含进服门禁挂接点与死亡扣好感）、服务器上下线播报与
下线的延迟确认（broadcast_server_status / schedule_offline_broadcast /
cancel_offline_broadcast / confirm_offline / conn_closed）、跨服转发
（cross_forward）。

与 `Transport(self)` / `KarmaService(self)` / `BindingService(self)` 同模式：
**持有 plugin 引用**做跨域访问（`plugin.transport` / `plugin.bindings` /
`plugin.karma` / config 派生开关 / `plugin._mc_conns` /
`plugin._server_offline_tasks` / `plugin._terminating`），**自己不拥有任何
域状态**——下线任务表与卸载标志仍由插件持有（`terminate()` 要直接撤销它们）。

方法名去掉了前导下划线（服务对象的公开入口），与 plugin 上的旧名一一对应：
  dispatch_mc_event          ← _dispatch_mc_event
  cross_forward              ← _cross_forward
  broadcast_server_status    ← _broadcast_server_status
  schedule_offline_broadcast ← _schedule_offline_broadcast
  cancel_offline_broadcast   ← _cancel_offline_broadcast
  confirm_offline            ← _confirm_offline
  conn_closed                ← _conn_closed
"""

from __future__ import annotations

import asyncio

from astrbot.api import logger

# 兄弟模块双路导入（与 main.py / nl_transport.py 同一个写法）：AstrBot 把
# 插件当**包**导入时 `from . import x` 成立；某些加载路径把它当**顶层模块**，
# 那时只有 `import x` 能用。
try:
    from . import binding_flow, bindings
except ImportError:  # 插件以顶层模块方式加载时
    import binding_flow
    import bindings

# 服务器断开后**延迟多久确认**再播报（秒）。
# 理由见 main.py `_server_offline_tasks` 的注释：避开指数退避重连的抖动。
# （2026-10-14 随事件分发域从 main.py 移入；main.py 经双路导入保留同名绑定，
#   测试经 `sys.modules["nl_dispatcher"]` 打桩改短它。）
_SERVER_OFFLINE_CONFIRM_DELAY = 5.0


class Dispatcher:
    """事件分发接缝：MC 事件 → 绑定群 / 其余在线服务器。

    ⚠️ 跨域状态一律走 `self.plugin.X`（连接表 / 下线任务表 / 卸载标志 /
    配置派生开关 / 绑定与好感服务都留在插件上）；本对象**无状态**，
    重建它不会丢任何东西。
    """

    def __init__(self, plugin) -> None:
        self.plugin = plugin

    def conn_closed(self, server_id: str) -> bool:
        conn = self.plugin._mc_conns.get(server_id)
        return conn is None or conn.closed

    async def dispatch_mc_event(self, mtype: str, data: dict, server_id: str = ""):
        """把 MC 事件按模板渲染后推送到所有绑定群。

        `server_id` **就是 MC 上报的 server-name**（2026-10-07 起；此前端口绑定可覆盖它）。
        `{server}` 展开成的是**显示名**（`ws_ports` 里按端口配的那个），与身份是两回事。
        """
        # 忽略名单里的名字：所有事件直接丢弃（见 _is_ignored_player）
        if self.plugin._is_ignored_player(data.get("player")):
            return
        try:
            srv = self.plugin.transport.server_display(server_id)
            if mtype == "chat" and self.plugin.config.get("enable_chat", True):
                text = self.plugin.transport.fmt(self.plugin.templates["chat"], server=srv, bot=self.plugin.mc_bot_name,
                                 player=data.get("player", "?"), text=data.get("text", ""))
            elif mtype == "join" and self.plugin.config.get("enable_join_leave", True):
                player = str(data.get("player") or "")
                # ⚠️ `enabled` 传的是**总开关**，不是它与子开关的与
                #    （2026-10-07 拆分）：总开关开着就**发提示**；
                #    子开关 `binding_join_gate` 只在门禁**内部**决定踢不踢。
                if binding_flow.is_unbound(
                    self.plugin.bindings.table, player,
                    ignored=self.plugin.mc_ignored_players,
                    enabled=self.plugin.enable_binding,
                    exempt=self.plugin.binding_exempt_players,
                ):
                    # ⚠️ 门禁整体包在 try 里，**一旦出错一律降级成「未拦截」**：
                    # 让它抛出去会落到本方法末尾那个 `except Exception`，而那条路
                    # 会跳过下面的推群——玩家进了服却在群里查无此人，正是 fail-open
                    # 要防的「凭空消失」，只不过是从错误路径到达的。
                    # （`_fmt` 只兜 KeyError/IndexError，用户把模板写成 `{code:d}`
                    #   这类格式错误会抛 ValueError，是真实可达的。）
                    try:
                        gated = await self.plugin.bindings.gate_unbound_player(player, server_id)
                    except Exception as e:
                        logger.error(f"NetherLink: 进服门禁执行失败，按未拦截处理: {e}")
                        gated = False
                    if gated:
                        # ⚠️ **continue 的逻辑**：门禁已把这个人踢了，就不再走下面的
                        # 推群/公屏——否则群里会看到一条「Steve 进入了服务器」，
                        # 紧接着又看到他被踢，纯噪声。
                        return
                    # 门禁**没有生效**（拿不到可用群号，或执行出错）：放行。
                    # 这里刻意**不** `return`——静默把玩家丢掉是最糟的结局
                    # （他既没被踢、也没在任何群里出现过，等于凭空消失）。
                # 已绑定（或门禁关闭）：顺手记下他出现过的服务器。
                # ⚠️ 只在**真新增**一台服时才落盘——进服是高频道，
                # 每次都写盘既慢又没意义（规范 §3.1：「只在见到新的服务器时才追加」）。
                updated, changed = bindings.note_server(
                    self.plugin.bindings.table, player, server_id
                )
                if changed:
                    self.plugin.bindings.table = updated
                    try:
                        bindings.save(self.plugin.bindings.path, self.plugin.bindings.table)
                    except Exception as e:
                        logger.warning(f"NetherLink: 记录玩家服务器失败（忽略）: {e}")
                text = self.plugin.transport.fmt(self.plugin.templates["join"], server=srv, bot=self.plugin.mc_bot_name,
                                 player=data.get("player", "?"))
            elif mtype == "leave" and self.plugin.config.get("enable_join_leave", True):
                text = self.plugin.transport.fmt(self.plugin.templates["leave"], server=srv, bot=self.plugin.mc_bot_name,
                                 player=data.get("player", "?"))
            elif mtype == "death" and self.plugin.config.get("enable_death", True):
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
                # 连累这笔游戏事实，而 KarmaService.add 自己吞写盘异常、有外层 try 兜底，
                # 反过来也不会连累广播。
                # 也放在 _fmt 之前：_fmt 只兜 KeyError/IndexError，模板里写错格式说明符
                # （如 {message:d}）会抛 ValueError 逃到外层 except，从而连同扣减一起跳过
                # player 未知时回退值是 "?"，扣它会造出 mc:? 这条假记录，因此排除
                if self.plugin.karma_death_penalty and player != "?":
                    await self.plugin.karma.add(
                        self.plugin.karma.identity_key("game", player, ""), -self.plugin.karma_death_penalty
                    )
                text = self.plugin.transport.fmt(self.plugin.templates["death"], server=srv, bot=self.plugin.mc_bot_name,
                                 player=player, message=data.get("message", ""))
            elif mtype == "advancement":
                # 成就走**独立管线**：要起 AI 让它更新好感并回话，不是套模板广播。
                # 单独 create_task，避免把 LLM 往返（可能数秒）压在这条 WS 事件
                # 处理路径上、连累后续事件。
                asyncio.create_task(self.plugin._handle_advancement(data, server_id))
                return
            else:
                return
            await self.plugin.transport.broadcast(text)
            # 跨服转发（`enable_cross_server` 控）。接在这里的两个理由：
            #   · 在 `_is_ignored_player` 之后 ⇒ 忽略名单自然生效
            #   · 在门禁的提前 `return` 之后 ⇒ 被踢的玩家不转发到别的服
            # ⚠️ 它与推群的开关**耦合**：某类事件的推群开关关着时，它也不会跨服
            #    转发（那段 if/elif 是同一个分支）。要独立得重构那串分支。
            await self.cross_forward(mtype, data, server_id)
        except Exception as e:
            logger.error(f"NetherLink: 处理 MC 事件 {mtype} 失败: {e}")

    # ------------------------------------------------------------------
    # 跨服互通
    # ------------------------------------------------------------------
    async def broadcast_server_status(self, server_id: str, online: bool) -> None:
        """服务器上线/下线时推送到所有绑定群（`enable_server_status` 控）。

        🔴 **下线走延迟确认**（用户 2026-10-10 要求）：MC 端重连是指数退避
        （3 秒起），网络抖一下就是「断开 → 3 秒后重连」，立即播报群里会看到
        「已断开」紧跟「已连接」。所以断开时**先挂一个 task**，等
        `_SERVER_OFFLINE_CONFIRM_DELAY` 秒；窗口内该 `server_id` 若重新握上手，
        `_cancel_offline_broadcast` 把它取消。**上线立即播**。

        ⚠️ 确认期结束时判的是「这个 server_id **现在**在不在线」，不是
        「有没有收到过新的 join」——重连可能换了新的 ws 对象。
        """
        if not self.plugin.enable_server_status:
            return
        key = "server_online" if online else "server_offline"
        try:
            text = self.plugin.transport.fmt(self.plugin.templates[key],
                             server=self.plugin.transport.server_display(server_id))
        except Exception as e:
            logger.warning(f"NetherLink: 服务器状态播报渲染失败（忽略）: {e}")
            return
        await self.plugin.transport.broadcast(text)

    def schedule_offline_broadcast(self, server_id: str) -> None:
        """断开时挂一个「延迟确认」任务（见 `_broadcast_server_status`）。"""
        # !!! 卸载中不挂新任务：terminate() 关连接会让 `_ws_session` 走到这里
        #     并挂出一个延迟确认任务；它会在新实例已播完「已连接」之后才触发「已断开」。
        if self.plugin._terminating:
            return
        if not self.plugin.enable_server_status:
            return
        self.cancel_offline_broadcast(server_id)
        task = asyncio.create_task(self.confirm_offline(server_id))
        self.plugin._server_offline_tasks[server_id] = task

    def cancel_offline_broadcast(self, server_id: str) -> None:
        """取消某个 server_id 待确认的下线播报（重连成功 / 重复断开时）。"""
        task = self.plugin._server_offline_tasks.pop(server_id, None)
        if task is not None and not task.done():
            task.cancel()

    async def confirm_offline(self, server_id: str) -> None:
        """等够确认窗口，若那台**仍不在线**就播报下线。"""
        try:
            await asyncio.sleep(_SERVER_OFFLINE_CONFIRM_DELAY)
            if self.conn_closed(server_id):
                await self.broadcast_server_status(server_id, online=False)
        except asyncio.CancelledError:
            # 重连成功时会被取消 —— **预期路径**，不进日志（CLAUDE.md 禁止吞取消）
            raise
        except Exception as e:
            logger.warning(f"NetherLink: 下线播报失败（忽略）: {e}")
        finally:
            self.plugin._server_offline_tasks.pop(server_id, None)

    async def cross_forward(self, mtype: str, data: dict, server_id: str) -> None:
        """把一条 MC 事件转发到**其余**在线服务器（`enable_cross_server` 控）。

        只转 chat / join / leave / death。**不转** bot_chat / advancement：
          · `bot_chat` 是唤醒词对话，转过去会成为**跨服 LLM 循环**的入口
          · `advancement` 已有 AI 好感播报，再转一遍是噪声

        🔴 **复用现有的 `chat` 下行帧**，不新增帧类型：下行 `chat` 在四端都不是
        聊天事件（Paper 走 `getServer().broadcast`，Fabric / NeoForge 走
        `broadcastSystemMessage`），**广播不触发 `AsyncChatEvent` /
        `ServerChatEvent`**，所以 MC 端不会把跨服行再上报 —— 防循环天然成立。
        代价是三个 MC 端**零改动**、不必重新编译。

        ⚠️ 调用点在 `_dispatch_mc_event` 里、`_is_ignored_player` **之后**，
        忽略名单自然生效。
        """
        if not self.plugin.enable_cross_server:
            return
        key = {"chat": "cross_chat", "join": "cross_join",
               "leave": "cross_leave", "death": "cross_death"}.get(mtype)
        if key is None:
            return
        try:
            line = self.plugin.transport.fmt(
                self.plugin.templates[key],
                server=self.plugin.transport.server_display(server_id),
                player=data.get("player", "?"),
                text=data.get("text", ""),
                message=data.get("message", ""),
            )
        except Exception as e:
            logger.warning(f"NetherLink: 跨服转发渲染失败（忽略）: {e}")
            return
        # ⚠️ **不发回来源服**（不回声）：三台在线时另外两台各收到一次。
        for sid, conn in list(self.plugin._mc_conns.items()):
            if sid == server_id or conn.closed:
                continue
            try:
                await self.plugin.transport.send_to_mc({"type": "chat", "line": line}, sid)
            except Exception as e:
                logger.warning(f"NetherLink: 跨服转发到 [{sid}] 失败（忽略）: {e}")
