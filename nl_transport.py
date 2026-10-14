# -*- coding: utf-8 -*-
"""NetherLink 传输层服务（2026-10-14 重构阶段三 Task 1）。

从 main.py 的 NetherLinkPlugin 抽出**横切总线**：QQ 群推送（broadcast）、
MC 下行（send_to_mc / broadcast_to_mc / broadcast_to_game）、连接可见性
（connected / server_display）与模板渲染兜底（fmt）。几乎所有域都要用
这层，所以先于其余域外移（阶段三 Task 1）。

与 `KarmaService(self)` / `BindingService(self)` 同模式：**持有 plugin 引用**
做跨域访问（`plugin._mc_conns` / `plugin._live_ws` / `plugin.context` /
`plugin.group_names` / `plugin._display_by_server` / `plugin._render_bot_reply` /
`plugin._resolve_qq_platform_id`），**自己不拥有任何域状态**——连接表与
显示名缓存仍由插件持有（WS 握手在插件那侧写它们），本服务只读。

方法名去掉了前导下划线（服务对象的公开入口），与 plugin 上的旧名一一对应：
  connected         ← _mc_connected
  server_display    ← _mc_server_display
  fmt               ← _fmt
  broadcast         ← _broadcast
  send_to_mc        ← _send_to_mc
  broadcast_to_mc   ← _broadcast_to_mc
  broadcast_to_game ← _broadcast_to_game
"""

from __future__ import annotations

import json

from astrbot.api import logger
from astrbot.api.event import MessageChain

# 本模块不 import 任何同目录兄弟模块（方法体只用到 json / logger /
# MessageChain 与 plugin 引用），因此没有 nl_karma_svc.py 那串双路导入。
# 反方向有：main.py 以双路导入方式引入本模块的 Transport 与
# DEFAULT_SERVER_DISPLAY（AstrBot 把插件当包导入时 `from . import x` 成立，
# 某些加载路径把它当顶层模块，那时只有 `import x` 能用）。

# 没在 `ws_ports` 里给那台配显示名时的兜底显示名（2026-10-14 随传输层
# 从 main.py 移入；main.py 经顶部 import 保留同名绑定，测试与注释仍引用）。
# 以前这一项是可配置的（mc_server_name），但它只能填一个值，多服下没有意义——
# 固定为 "MC"，与 schema 的 hint 一致（「留空则显示为 [MC]」）。
DEFAULT_SERVER_DISPLAY = "MC"


class Transport:
    """传输接缝：QQ↔MC 两个方向的发送与「在线状态怎么答」。

    ⚠️ 跨域状态一律走 `self.plugin.X`（连接表 / 上下文 / 群清单 /
    显示名缓存 / 渲染与平台反查方法都留在插件上）；本对象**无状态**，
    重建它不会丢任何东西——这也意味着测试里 `plugin.transport.xxx = stub`
    打桩后要再经 plugin.transport 调用才生效（打在 plugin 上的旧桩已失效）。
    """

    def __init__(self, plugin) -> None:
        self.plugin = plugin

    def connected(self, server_id: str = "") -> bool:
        """是否有可用的 MC 连接。

        不传 server_id 时表示「**至少有一台**在线」——保留这个语义是为了让
        既有的单服调用点（离线判定、指令执行前的检查）不用改。
        传了则只认那一台。
        """
        if server_id:
            conn = self.plugin._mc_conns.get(server_id)
            return conn is not None and not conn.closed
        return any(not c.closed for c in self.plugin._mc_conns.values())

    def fmt(self, tpl: str, **kw) -> str:
        try:
            return tpl.format(**kw)
        except (KeyError, IndexError):
            return tpl  # 模板占位符写错时兜底原样输出

    def server_display(self, server_id: str = "") -> str:
        """对外展示与 LLM 上下文统一使用的服务器名（**只影响展示**）。

        取值顺序：
          1. 握手时记下的显示名（来自 `ws_ports` 的 `显示名:端口`）
          2. `DEFAULT_SERVER_DISPLAY`（"MC"）——那台没配显示名时

        🔴 **本函数不影响身份**：身份 = MC 上报的 server-name。改这里只动
        「显示成什么」，不动指令定向、会话键、绑定表。

        ⚠️ **为什么要在握手时缓存**（插件上的 `_display_by_server`）而不能
        现查 `_mc_conns`：断线后 `_mc_conns` 里那条就没了，现查会退回常量
        `MC`——于是同一台服务器在线时叫「生存服」、掉线后历史消息里又叫
        「MC」，前后不一致。缓存住它，全生命周期只有一个名字。
        ⚠️ 以前这里读的是 `server_display_names`（按 server_id 配）——那个配置项
        已并入 `ws_ports`（2026-10-07）。
        """
        if server_id:
            name = self.plugin._display_by_server.get(str(server_id))
            if name:
                return name
        return DEFAULT_SERVER_DISPLAY

    async def broadcast(self, text: str):
        """向所有绑定群推送文本。"""
        if not self.plugin.group_names:
            logger.warning("NetherLink: 没有配置绑定群，消息无处推送")
            return
        platform_id = self.plugin._resolve_qq_platform_id()
        if not platform_id:
            logger.warning(
                "NetherLink: 未找到 aiocqhttp 平台实例，消息无法推送到 QQ 群"
                "（请确认 OneBot/aiocqhttp 适配器已启用）"
            )
            return
        for group in self.plugin.group_names:
            umo = f"{platform_id}:GroupMessage:{group}"
            try:
                # send_message 在找不到平台时**返回 False 而不抛异常**，
                # 只由 AstrBot 自己记一条 warning。这里显式接住返回值，
                # 把「哪个群被丢掉了」补进本插件的日志，避免再次静默。
                ok = await self.plugin.context.send_message(
                    umo, MessageChain().message(text)
                )
                if not ok:
                    logger.warning(f"NetherLink: 推送到群 {group} 未被接受，消息已丢弃")
            except Exception as e:
                logger.error(f"NetherLink: 推送到群 {group} 失败: {e}")

    async def send_to_mc(self, payload: dict, server_id: str = "") -> bool:
        """向 MC 端发送一条下行消息。

        `server_id` 为空时：**只有恰好一台在线**才发送。0 台按「未连接」处理；
        多台则**拒绝发送并记 ERROR**——宁可丢一条也不要把指令发到错误的服务器上
        （多服并存时「随便挑一台」正是先前指令落到另一台服的原因）。
        """
        target = server_id
        if not target:
            alive = [
                sid for sid, c in self.plugin._mc_conns.items() if not c.closed
            ]
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
        conn = self.plugin._mc_conns.get(target)
        if conn is None or conn.closed:
            logger.warning(f"NetherLink: MC 服务器 [{target}] 未连接，消息丢弃")
            return False
        try:
            await conn.ws.send_str(json.dumps(payload, ensure_ascii=False))
            return True
        except Exception as e:
            logger.error(f"NetherLink: 发送到 MC [{target}] 失败: {e}")
            return False

    async def broadcast_to_mc(self, payload: dict) -> int:
        """把一条下行消息发给**所有**在线的 MC 服务器，返回发成功的台数。

        与 `send_to_mc` 的分工：
          · `send_to_mc` 是**定向**发送（指令、机器人回复）——目标不明确就拒发，
            因为把指令发到错误的服务器比丢掉更糟
          · 本方法用于**天然属于全体**的消息（QQ 群友的聊天），广播是它的语义本身

        以前这里直接调 `send_to_mc` 且不带 server_id，多台在线时会被那条
        「未指定目标」的保护整条拒掉——**QQ 消息完全进不了游戏**，而单服下
        看不出来（只有一台，照发）。这是多服务器改造的回归，2026-09-19 修。
        """
        sent = 0
        for sid, conn in list(self.plugin._mc_conns.items()):
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

    async def broadcast_to_game(self, text: str) -> None:
        """把一段文本按 `template_bot_reply_game` 渲染后**广播给所有在线服务器**。

        ⚠️ 与插件侧 `send_game_line` 的唯一区别是**广播 vs 定向**：那个带
        server_id、只发给一台；群内绑定这条路径**不知道玩家在哪台服**，
        所以只能广播。这是刻意的取舍——宁可多广播一台，也不猜一台、
        更不因为「不知道去哪台」就干脆不发。渲染与 `send_game_line` **共用**
        插件上的 `_render_bot_reply`——一致性因此是结构上的，而不是
        docstring 里的一句承诺。
        """
        await self.broadcast_to_mc(
            {"type": "bot_reply", "line": self.plugin._render_bot_reply(text)}
        )
