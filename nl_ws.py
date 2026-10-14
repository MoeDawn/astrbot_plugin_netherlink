# -*- coding: utf-8 -*-
"""NetherLink WS 服务器服务（2026-10-14 重构阶段三 Task 5）。

从 main.py 的 NetherLinkPlugin 抽出**WS 服务器域**：多端口监听（start）、
连接入口与 `_live_ws` 的登记/注销（handle）、单条连接的完整生命周期
（session：握手鉴权 / 端口冲突与同名冲突检测 / 心跳 / 帧路由 / 断开清理）。

与 `Transport(self)` / `Dispatcher(self)` 同模式：**持有 plugin 引用**做跨域访问
（`plugin._live_ws` / `plugin._mc_conns` / `plugin._port_owner` /
`plugin._display_by_server` / `plugin._pending_cmds` / `plugin._pending_inspects` /
`plugin.dispatcher` / `plugin.actions` / `plugin._handle_bot_chat` /
`plugin._is_ignored_player`），**自己不拥有任何域状态**——连接表 / 端口占用 /
live 连接集合 / runner 与启动 task 仍由插件持有（`terminate()` 要直接撤销它们，
test_contracts 的 terminate 顺序守卫也钉在 main.py 的源码上）。

方法名去掉了前导下划线（服务对象的公开入口），与 plugin 上的旧名一一对应：
  start   ← _start_ws_server
  handle  ← _ws_handler
  session ← _ws_session
"""

from __future__ import annotations

import asyncio
import json

import aiohttp
from aiohttp import web

from astrbot.api import logger

# 兄弟模块双路导入（与 main.py / nl_dispatcher.py 同一个写法）：AstrBot 把
# 插件当**包**导入时 `from . import x` 成立；某些加载路径把它当**顶层模块**，
# 那时只有 `import x` 能用。
try:
    from .nl_protocol import CmdResult, McConn
except ImportError:  # 插件以顶层模块方式加载时
    from nl_protocol import CmdResult, McConn

# 卸载时等 WS 相关清理的上限（秒）。**远小于 aiohttp 默认的 60** ——
# 详见 plugin `terminate` 的 docstring：反顺序之后本不该真正用到，给短值是兜底，
# 保证保存/卸载不会因为某条连接卡住而拖到一分钟。
# （2026-10-14 随 WS 域从 main.py 移入；main.py 经双路导入保留同名绑定，
#   `terminate()` 仍用它限等待上限。）
_WS_SHUTDOWN_TIMEOUT = 3.0


class WsServer:
    """WS 接缝：MC 端连入的监听、鉴权与帧路由。

    ⚠️ 跨域状态一律走 `self.plugin.X`（连接表 / 端口占用 / live 连接集合 /
    runner / 待回执表 / 分发与动作服务都留在插件上）；本对象**无状态**，
    重建它不会丢任何东西——这也意味着测试里 `plugin.ws.session = stub`
    打桩后要再经 plugin.ws 调用才生效（打在 plugin 上的旧桩已失效）。
    ⚠️ `aiohttp` / `web` 的模块绑定经 main.py 顶部 import 保留同名可见
    （测试经 `main_module.web` / `main_module.aiohttp` 打桩与取 WSMsgType，
    与本模块拿到的是**同一个模块对象**，打桩照常生效）。
    """

    def __init__(self, plugin) -> None:
        self.plugin = plugin

    async def start(self):
        """启动 WS 服务端，监听 MC 端连入。

        **可监听多个端口**（见 ws_ports）：一台 MC 服务器占一个端口，单服就配一个。
        共用同一个 `app` 与 `handle`，用本地端口区分来源（路线 A「端口即身份」）。

        `ws_bindings` 为空（ws_ports 留空或全非法）时**不监听任何端口**——
        以前会退回那个单值配置 `ws_port`（已删除），现在刻意选择响亮地失败：
        配错端口却不自知的代价，比启动时看到一条 ERROR 大得多。
        """
        if not self.plugin.ws_bindings:
            logger.error(
                "NetherLink: 未配置任何监听端口，WS 服务端没有启动——"
                "请在 ws_ports 里填写，格式「显示名:端口」或只写「端口」，"
                "单台服务器也只需填一个。"
            )
            return

        app = web.Application()
        app.router.add_get("/ws", self.handle)
        # ⚠️ 显式给短 `shutdown_timeout`：默认是 **60 秒**（aiohttp
        # web_runner.py），只要还有挂起的 WS handler 就会等满它。
        # 正常情况下 terminate 已经**先**主动关了连接，这里不会真正用到；
        # 给短值是兜底——万一还有别的挂起请求，也不该把保存/卸载拖 60 秒。
        self.plugin._runner = web.AppRunner(app, shutdown_timeout=_WS_SHUTDOWN_TIMEOUT)
        await self.plugin._runner.setup()
        for label, port in self.plugin.ws_bindings:
            tag = f"[{label}] " if label else ""
            try:
                site = web.TCPSite(self.plugin._runner, self.plugin.ws_host, port)
                await site.start()
                logger.info(
                    f"NetherLink WS 服务端已启动 {tag}ws://{self.plugin.ws_host}:{port}/ws"
                )
            except OSError as e:
                logger.error(f"NetherLink WS 端口 {port} 启动失败: {e}")

    def _is_current_conn(self, server_id: str, ws) -> bool:
        """这条 ws 是否仍是该 server_id 的当前连接（未被重连替换掉）。"""
        conn = self.plugin._mc_conns.get(server_id)
        return conn is not None and conn.ws is ws

    async def handle(self, request):
        """WS 入口：登记连接 → 交给 `session` → **保证注销**。

        🔴 **为什么拆成两层**：`session` 里有 4 个提前 `return`（token 失败、
        端口冲突、重连替换…）。在这 4 处各写一遍注销，漏一个就等于没做；
        `try/finally` 一处就够。

        ⚠️ 登记的是**所有**活连接（含尚未握手的）——`_mc_conns` 里只有握过手的，
        而卸载时要关的是一切还在跑的：漏掉未握手的，aiohttp 会为它等满
        `shutdown_timeout`（2026-10-10 实测 6 秒）。
        """
        ws = web.WebSocketResponse(heartbeat=30)
        await ws.prepare(request)
        self.plugin._live_ws.add(ws)
        try:
            return await self.session(ws, request)
        finally:
            self.plugin._live_ws.discard(ws)

    async def session(self, ws, request):
        """一条 WS 连接的实际处理（握手 / 事件循环 / 断开清理）。"""
        # 本连接的本地端口——路线 A 里它就是身份的来源
        try:
            port = int(request.transport.get_extra_info("sockname")[1])
        except Exception:
            port = 0
        # 🔴 **身份就是上报名**（2026-10-07 起）。端口不再影响身份——它只用来
        #    决定「哪台服务器连的是哪个口」（端口冲突检测仍需要它）。
        #    以前这里还有一条「端口绑定的 id 优先」，已删除。
        # debug：紧接着就有一条「握手成功（端口 X，上报名 Y）」，信息更全
        logger.debug(f"NetherLink: MC 端已连入（本地端口 {port}），等待握手")

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
                    if data.get("token") != self.plugin.auth_token:
                        logger.warning("NetherLink: MC 端 token 校验失败，断开连接")
                        await ws.close(code=4001, message=b"auth failed")
                        return ws
                    reported = str(data.get("server_name") or "mc")
                    server_id = reported
                    # 显示名按**端口**查（ws_ports 的 `显示名:端口`），
                    # 只在首次见到这台时记一次。
                    _label = self.plugin.port_display_names.get(port)
                    if _label and not self.plugin._display_by_server.get(server_id):
                        self.plugin._display_by_server[server_id] = _label

                    # ---- 阶段 0：同端口不同服务器的冲突，必须报错而不是静默踢掉 ----
                    # 以前是「新连接一律踢掉旧连接」，于是两台服务器互相踢、各自重连，
                    # 形成永不停止的抖动（两端退避都会重置回 3 秒），且日志里看不出。
                    owner = self.plugin._port_owner.get(port)
                    if owner and owner != server_id and not self.plugin.dispatcher.conn_closed(owner):
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
                    old = self.plugin._mc_conns.get(server_id)
                    # 🔴 **同一个上报名、却来自不同端口 ⇒ 这是两台不同的服务器。**
                    #    新模型下身份只认上报名，于是两台都叫 "mc"（MC 端默认值）
                    #    会撞成同一台：后来的把先来的顶掉，两端各自重连，形成
                    #    **永不停止的互踢**——正是本项目多服改造要消灭的那个 bug。
                    #    旧模型里「端口绑定 id」顺带把它们分开了，现在没有了，
                    #    所以必须在这里显式识别并**拒绝**，而不是静默顶掉。
                    if (
                        old is not None
                        and old.ws is not ws
                        and not old.closed
                        and getattr(old, "port", None) not in (None, port)
                    ):
                        logger.error(
                            f"NetherLink: 服务器上报名冲突——[{server_id}] 已由"
                            f"端口 {old.port} 的连接占用，端口 {port} 又来了一个"
                            f"同名连接。身份用的是 MC 端上报的 server-name，"
                            f"两台服务器**必须填不同的 server-name**，否则会互相"
                            f"顶掉、消息随机丢失。已拒绝新连接。"
                        )
                        try:
                            await ws.close(
                                code=4004, message=b"duplicate server_name"
                            )
                        except Exception:
                            pass
                        return ws
                    if old is not None and old.ws is not ws and not old.closed:
                        logger.warning(
                            f"NetherLink: 服务器 [{server_id}] 重复连入，关闭其旧连接"
                        )
                        try:
                            await old.ws.close(code=4002, message=b"replaced by new connection")
                        except Exception:
                            pass
                    self.plugin._mc_conns[server_id] = McConn(
                        ws=ws, port=port, reported_name=reported
                    )
                    self.plugin._port_owner[port] = server_id
                    logger.info(
                        f"NetherLink: MC 服务器 [{server_id}] 握手成功"
                        f"（端口 {port}，上报名 {reported}）"
                    )
                    # 上线立即播；顺带撤销可能还挂着的下线播报（重连）
                    await self.plugin.dispatcher.broadcast_server_status(server_id, online=True)
                elif not self._is_current_conn(server_id, ws):
                    continue  # 未握手、或已被重连替换的连接不发事件

                elif mtype == "command_result":
                    fut = self.plugin._pending_cmds.pop(data.get("id"), None)
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
                elif mtype == "inspect_result":
                    # 查背包的回复（用户 2026-10-12）。纯文本，直接交给等着的 Future。
                    fut = self.plugin._pending_inspects.pop(data.get("id"), None)
                    if fut is not None and not fut.done():
                        fut.set_result(str(data.get("text") or ""))
                elif mtype == "heartbeat":
                    pass
                elif mtype == "bot_chat":
                    # 游戏内唤醒词消息：走 LLM，回复只发回游戏，QQ 不可见
                    if self.plugin._is_ignored_player(data.get("player")):
                        continue
                    asyncio.create_task(self.plugin._handle_bot_chat(data, server_id))
                elif mtype == "action_result":
                    # MC 端执行完确认卡的 steps 后回报（**点击回调**触发）。
                    # 这边只记结果 + 扣好感 —— **执行早已在 MC 端发生**。
                    # ⚠️ 单独起 task：要写好感（磁盘 IO），不压在事件路径上。
                    asyncio.create_task(
                        self.plugin.actions.handle_action_result(
                            str(data.get("player") or ""),
                            data.get("token"),
                            bool(data.get("ok", False)),
                            str(data.get("detail") or ""),
                        )
                    )
                elif mtype == "action_confirm":
                    # ⚠️ **旧路径**（计分板轮询上报）。2026-10-11 起确认卡改走
                    #    `action_result` + 点击回调，这条路只为**回退**保留。
                    asyncio.create_task(
                        self.plugin.actions.handle_action_confirm(
                            str(data.get("player") or ""),
                            data.get("token"),
                            server_id,
                        )
                    )
                else:
                    await self.plugin.dispatcher.dispatch_mc_event(mtype, data, server_id)

            elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSE):
                break

        # 只摘掉属于这条连接的登记。绝不能整个清空——多服并存时那会误伤别人，
        # 而且 _port_owner 也会跟着丢，后续重连会被误判成端口冲突。
        if server_id and self._is_current_conn(server_id, ws):
            self.plugin._mc_conns.pop(server_id, None)
            if self.plugin._port_owner.get(port) == server_id:
                self.plugin._port_owner.pop(port, None)
            logger.warning(f"NetherLink: MC 服务器 [{server_id}] 连接断开")
            # 下线**延迟确认**：抖动重连不该刷屏（见 dispatcher.schedule_offline_broadcast）
            self.plugin.dispatcher.schedule_offline_broadcast(server_id)
        return ws
