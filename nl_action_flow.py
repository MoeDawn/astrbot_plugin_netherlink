# -*- coding: utf-8 -*-
"""NetherLink 动作编排服务（2026-10-14 重构阶段三 Task 3）。

从 main.py 的 NetherLinkPlugin 抽出**动作编排域**：动作目录落地与陈旧告警
（actions_path / ensure_actions_file / warn_if_actions_stale）、目录驱动的
LLM 工具注册与摘除（register_tools / unregister_tools）、动作工具执行入口
（run_action_tool：校验 → 渲染 → 定价 → 需确认发卡 / 免确认直接执行）、
确认卡发放与两条结算路径（offer_action_confirm / handle_action_result /
handle_action_confirm / execute_action）、结算口径共用件（apply_action_karma
+ announce_action_done + write_action_audit——两条路径必须同源，见 TECH.md
动作层一节）与私聊告知（_tell_player）。

与 `Transport(self)` / `Dispatcher(self)` 同模式：**持有 plugin 引用**做跨域
访问（`plugin.transport` / `plugin.karma` / `plugin.context` / 配置派生开关 /
`plugin._pending_actions` / `plugin._action_token_seq` / `plugin._write_audit`），
**自己不拥有任何域状态**——待确认队列、token 计数器与工具名登记仍由插件持有
（`terminate()` 要直接清掉它们）。

方法名去掉了前导下划线（与 Transport / Dispatcher 同约定），与 plugin 上的
旧名一一对应：
  actions_path          ← _actions_path
  ensure_actions_file   ← _ensure_actions_file
  warn_if_actions_stale ← _warn_if_actions_stale
  unregister_tools      ← _unregister_action_tools
  register_tools        ← _register_action_tools
  run_action_tool       ← _run_action_tool
  offer_action_confirm  ← _offer_action_confirm
  handle_action_result  ← _handle_action_result
  write_action_audit    ← _write_action_audit
  apply_action_karma    ← _apply_action_karma
  announce_action_done  ← _announce_action_done
  handle_action_confirm ← _handle_action_confirm
  execute_action        ← _execute_action

域内私有 helper 随迁、名字不变（只被本域调用）：
`_is_our_action_tool` / `_action_server_id` / `_signed` / `_find_action` /
`_tell_player`。
"""

from __future__ import annotations

import time
import uuid
from pathlib import Path

from astrbot.api import logger

# 兄弟模块双路导入（与 main.py / nl_dispatcher.py 同一个写法）：AstrBot 把
# 插件当**包**导入时 `from . import x` 成立；某些加载路径把它当**顶层模块**，
# 那时只有 `import x` 能用。
try:
    from . import action_tools
except ImportError:  # 插件以顶层模块方式加载时
    import action_tools

try:
    from . import actions
except ImportError:  # 插件以顶层模块方式加载时
    import actions

try:
    from .nl_prompt import _strip_section_codes, _tellraw_json_escape
except ImportError:  # 插件以顶层模块方式加载时
    from nl_prompt import _strip_section_codes, _tellraw_json_escape


class ActionFlow:
    """动作编排接缝：目录 -> LLM 工具 -> 确认卡 -> 结算。

    ⚠️ 跨域状态一律走 `self.plugin.X`（待确认队列 / token 计数器 / 工具名
    登记 / context / 配置派生开关 / 好感与传输服务都留在插件上）；本对象
    **无状态**，重建它不会丢任何东西。
    """

    def __init__(self, plugin) -> None:
        self.plugin = plugin

    def actions_path(self) -> Path:
        """动作目录的路径。配了用配的，否则默认在 plugin_data 下。"""
        if self.plugin.actions_file:
            return Path(self.plugin.actions_file)
        return self.plugin._karma_dir / "actions.yaml"

    def ensure_actions_file(self, path: Path) -> None:
        """首次启动把插件自带的默认目录**落地**一份。

        ⚠️ 只落地、**从不覆盖**：这份文件是给人改的，覆盖等于删用户配置。
        """
        if path.exists():
            return
        src = Path(__file__).resolve().parent / "actions.default.yaml"
        if not src.is_file():
            logger.warning(f"NetherLink: 找不到默认动作目录 {src}，动作层不启用")
            return
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(src.read_bytes())
            logger.info(f"NetherLink: 已落地默认动作目录 -> {path}")
        except Exception as e:
            logger.warning(f"NetherLink: 落地动作目录失败（忽略）: {e}")

    def warn_if_actions_stale(self, path) -> None:
        """落盘的动作目录比插件自带的默认版**旧** ⇒ 记一条 warning。

        🔴 **为什么需要它**：`ensure_actions_file` **从不覆盖**已存在的文件
        （那是给人改的，覆盖等于删用户配置）。于是升级插件后，用户机器上
        还是**旧版**的 `actions.yaml`。2026-10-11 真机就栽在这：旧版
        `give_item` 有 `confirm` 而无 `apply`，新校验判它坏 ⇒
        `register_tools` **一个动作都不注册**，而界面上什么都看不见。

        ⚠️ **只告警，不自动改**。用户可能正拿着自己改过的目录 —— 悄悄换掉
        比不换更糟。要给他「知道该看一眼」的机会，而不是替他做决定。
        """
        try:
            src = Path(__file__).resolve().parent / "actions.default.yaml"
            _doc, want = actions.load_actions_doc(str(src))
            _cur, have = actions.load_actions_doc(str(path))
            if want and have < want:
                logger.warning(
                    f"NetherLink: 动作目录 {path} 的 version={have} 比插件自带的"
                    f"（version={want}）旧 —— 插件**不会自动覆盖**你的目录，"
                    f"请对照 {src.name} 手动更新（新版动作可能因缺字段而"
                    f"整个注册不上，症状是「AI 说没有这个工具」）。"
                )
        except Exception as e:
            logger.warning(f"NetherLink: 检查动作目录版本失败（忽略）: {e}")

    def unregister_tools(self) -> None:
        """把本插件上一轮注册的动作工具从注册表里摘掉。

        ⚠️ 为什么需要：`add_llm_tools` 只在**同名**时替换，而「上一轮注册过、
        这一轮从目录里删掉了」的那条**不会**被替换 —— 不主动清就是**僵尸工具**：
        AI 看得到它、能用它，但它已经不在目录里了（用户以为删掉了）。
        """
        if self.plugin.context is None or not self.plugin._action_tool_names:
            return
        try:
            mgr = self.plugin.context.get_llm_tool_manager()
        except Exception:
            return
        for name in list(self.plugin._action_tool_names):
            try:
                tool = mgr.get_func(name)
                if tool is not None and self._is_our_action_tool(tool):
                    mgr.remove_func(name)
            except Exception as e:
                logger.warning(f"NetherLink: 摘除动作工具 [{name}] 失败（忽略）: {e}")
        self.plugin._action_tool_names.clear()

    @staticmethod
    def _is_our_action_tool(tool) -> bool:
        """这个工具是不是**我们**注册的？

        🔴 用 `handler_module_path` 而不是名字前缀：前缀会误伤用户自己起的
        同名动作；module_path 是注册时由框架算出的归属标识。查不到时保守返回
        False（**宁可留下一个僵尸工具，也不要摘掉别人的工具**）。
        """
        mp = str(getattr(tool, "handler_module_path", "") or "")
        return bool(mp) and "astrbot_plugin_netherlink" in mp

    def register_tools(self) -> None:
        """读目录 → 为每个动作注册一个 LLM 工具。

        ⚠️ **任何失败都不连累插件启动**（本项目的既有约定），但必须有日志 ——
        静默不注册的症状是「AI 说没有这个工具」，与「没配」长得一样。
        """
        if self.plugin.context is None:
            return          # 测试替身没有工具管理器，静默跳过
        # 先摘掉上一轮的（含已从目录删掉的），再注册这一轮的
        self.unregister_tools()
        if not self.plugin.enable_actions:
            logger.info("NetherLink: 结构化动作层已关闭（enable_actions=false）")
            return
        path = self.actions_path()
        self.ensure_actions_file(path)
        self.warn_if_actions_stale(path)
        try:
            acts = actions.load_actions(str(path))
        except actions.ActionError as e:
            # 目录坏了要**响亮**：不注册任何动作，并说明是哪一条、为什么
            logger.error(f"NetherLink: 动作目录不可用，本次不注册任何动作 —— {e}")
            # ⚠️ 存下来给**面板**看。只记日志的话，症状是「AI 说没这个工具」，
            #    与「压根没配」长得一样 —— 2026-10-11 真机就栽在这。
            self.plugin._actions_error = str(e)
            return
        except Exception as e:
            logger.error(f"NetherLink: 读取动作目录失败（本次不注册任何动作）: {e}")
            self.plugin._actions_error = str(e)
            return
        self.plugin._actions_error = ""
        if not acts:
            logger.info(f"NetherLink: 动作目录 {path} 里没有动作")
            return
        try:
            tools = [action_tools.make_tool(a, self.plugin) for a in acts]
            self.plugin.context.add_llm_tools(*tools)
            self.plugin._action_tool_names = {t.name for t in tools}
            logger.info(
                f"NetherLink: 已注册 {len(tools)} 个动作工具"
                f"（{', '.join(sorted(self.plugin._action_tool_names))}）来自 {path}"
            )
        except Exception as e:
            logger.error(f"NetherLink: 注册动作工具失败: {e}")

    async def run_action_tool(self, action, event, raw_params: dict) -> str:
        """动作工具的执行入口（AI 调它时走这里）。

        流程：校验 → 渲染 → 定价 → **需要确认就发卡、不执行**；不需要就直接执行。
        ⚠️ 参数错的消息是**写给 AI 看的**，直接回，让它改参数重试。
        """
        try:
            values = actions.validate_params(action, raw_params)
        except actions.ActionError as e:
            return f"参数有误：{e}"
        try:
            cmd = actions.render_command(action, values)
        except actions.ActionError as e:
            return f"内部错误（模板渲染）：{e}"
        try:
            cost = actions.resolve_karma(action, values, raw_params.get("cost"))
        except actions.ActionError as e:
            return str(e)
        try:
            karma_delta = actions.resolve_karma_delta(action, values, cost)
        except actions.ActionError as e:
            return str(e)
        if action.confirm:
            return await self.offer_action_confirm(
                action, values, cost, cmd, event, karma_delta)
        # ⚠️ `karma_delta` 必须一起传 —— 无卡路径也怕 `model: param`
        #    （它的 cost 恒为 0，只传 cost 的话 delta 会被丢掉）。
        return await self.execute_action(action, values, cost, cmd, event,
                                          karma_delta=karma_delta)

    def _action_server_id(self, event) -> str:
        """这次动作该发往哪台 MC 服务器。

        🔴 **必须取来源服**（`netherlink_ctx.server_id`），不能留空 ——
        留空时 `transport.send_to_mc` 在多台在线时会**拒发**（那是刻意的保护：
        宁可丢一条也不要把指令发到错的服务器上）。用户实测两台服在线时
        就是这个症状：确认卡发不出去、AI 如实回「服务器不可用」。

        QQ 侧发起时没有来源服（`event` 为 None 或不是游戏侧）——
        那正是 `transport.send_to_mc` 空串语义该生效的场合。
        """
        try:
            ident = self.plugin._game_identity_from(event) if event is not None else None
        except Exception:
            ident = None
        return str(ident.get("server_id") or "") if ident else ""

    async def offer_action_confirm(self, action, values, cost, cmd, event,
                                    karma_delta: int = None) -> str:
        """私聊发确认卡 + 登记待确认。**本函数绝不执行指令。**

        执行发生在玩家点击之后（`handle_action_confirm`）—— 这正是
        「给物品要确认」的全部意义。
        """
        player = str(values.get("player") or "")
        if not player:
            return "参数有误：无法确定接收者，无法发起确认。"
        server_id = self._action_server_id(event)
        # 展开成 MC 可执行的步骤（spec §10：MC 不认识动作名，只认 check/clear/give）
        try:
            steps = actions.expand_steps(action, values)
            card = actions.build_card(action, values, cost)
        except actions.ActionError as e:
            return f"内部错误（动作展开）：{e}"
        # ⚠️ **没有 steps 是合法的** —— 有些动作只改好感、不动游戏
        #    （如 refuse_gift：拒不收，物品留在玩家背包）。
        #    原先这里直接返回错误，那类动作就完全没法用。
        now = time.monotonic()
        token = actions.new_token(self.plugin._action_token_seq)
        self.plugin._action_token_seq = token
        # 顺手清掉已过期的，别让队列无限涨
        self.plugin._pending_actions = actions.prune_pending(self.plugin._pending_actions, now)
        self.plugin._pending_actions[token] = actions.PendingAction(
            token=token,
            action_name=action.name,
            # ⚠️ 存**展开后的 steps**，不是原始 values —— 玩家点确认时不再回头
            #    读动作目录（目录可能已经改了，那就前后不一致了）
            values={"steps": steps, "values": dict(values)},
            player=player,
            server_id=server_id,
            cost=cost,
            cmd=cmd,
            deadline=now + self.plugin.action_confirm_ttl,
            # 带符号：普通动作 -cost，送礼（model=param）+delta
            karma_delta=(int(karma_delta) if karma_delta is not None
                         else -int(cost)),
        )
        # 🔴 **没有 steps ⇒ 不用发卡、不用等玩家点**：直接结算好感。
        #    理由：MC 端收到空的 steps 会渲染出一张「点了什么也不会发生」的卡，
        #    对玩家是纯干扰（refuse_gift 就是这类 —— 它只是「拒收 + 扣好感」）。
        if not steps:
            item = self.plugin._pending_actions.pop(token, None)
            if item is not None:
                await self.apply_action_karma(item, player)
            await self.announce_action_done(item, player)
            if item is not None:
                # 动作名写进审计的 cmd —— 管理员在面板上搜得到
                self.write_action_audit(item, player, True, via_card=False)
            return (
                f"已完成（{player} 的好感变化 {self._signed(item.karma_delta)}）。"
                f"\n你可以就此回一句简短的话，不要再调工具。"
            )
        # 卡片由 **MC 端渲染**（它用 Adventure 构造可点击组件，能把 token 捕获
        # 进回调闭包 ⇒ 不需要计分板）。这里只发数据。
        ok = await self.plugin.transport.send_to_mc(
            {"type": "action_offer", "token": token, "player": player,
             "action": action.name, "card": card, "steps": steps},
            server_id,
        )
        if not ok:
            self.plugin._pending_actions.pop(token, None)
            return "MC 服务器不可用，确认卡没发出去，本次没有执行。"
        logger.info(
            f"NetherLink: 已发确认卡 | 玩家={player} 动作={action.name} "
            f"好感={cost} token={token} 步骤={len(steps)} "
            f"有效期={self.plugin.action_confirm_ttl:g}s"
        )
        # 🔴 **必须说死「还没扣费」** —— 2026-10-11 真机暴露：AI 看到
        #    「消耗 2 好感」就对着玩家说「2 点好感跑不掉喵」，玩家以为已经付过钱，
        #    而实际上**分文未扣**。报价单里的数字会被 AI 当成既成事实，所以这句
        #    必须显式否掉：钱还没动、事还没做，全等玩家点那一下。
        # 🔴 **必须给出具体话术**，不能只说「别宣称已完成」——2026-10-11 真机：
        #    AI 回了「跑腿费 2 点好感，已经扣掉了」。不是它不听话，是返回里只写了
        #    禁止项、没写该说什么，模型就自己编了一句像样的。两边都写死。
        # 🔴 **话术要按 `karma_delta` 与 `cmd` 分支**（2026-10-11 评审）：
        #    原先写死「扣 {cost} 好感并执行：{cmd}」，对 `offer_karma` 是
        #    「扣 0 好感」（它其实要**加** 1~2 点），对空 `template` 的动作是
        #    「并执行：」后面空着 —— AI 会把这些原样复述给玩家。
        # ⚠️ 用本函数自己的  入参，**不要读 ** ——
        #     只在上面那个「空 steps」分支里才被赋值，
        #    走这条路的动作引它会 UnboundLocalError。
        d = int(karma_delta if karma_delta is not None else 0)
        if d > 0:
            what = f"给 {player} 加 {d} 好感"
        elif d < 0:
            what = f"扣 {abs(d)} 好感"
        else:
            what = "不涉及好感"
        tail = f"，然后执行：{cmd}" if cmd else ""
        return (
            f"已向 {player} 私聊发送确认卡。此刻什么都没发生：没给东西、没扣好感。"
            f"等 {player} 点「确认」那一刻，插件才会{what}{tail}"
            f"\n\n你现在只该回一句话，语义类似："
            f"「已经记下你的需求了，看看私信里的卡片，确认无误就点执行吧」。"
            f"\n不许说的：已经给了 / 已经扣了 / 好感我收下了 / 跑腿费付过了 —— "
            f"好感是玩家点确认时才扣的，现在还是原样。"
            f"\n也不要再调任何工具（包括 mc_karma）；就此停下，等他点。"
        )

    async def handle_action_result(self, player: str, token,
                                    ok: bool, detail: str = "") -> None:
        """MC 端执行完 steps 后回报（`action_result{player,token,ok,detail}`）。

        🔴 **执行已经发生了**（在 MC 端），这边只做两件事：
          ① 从待确认队列取走这条（防重复）
          ② `ok` 才扣好感 —— 用户 2026-10-11：「发卡前就扣了确认卡意义何在」
             ⇒ 好感**在玩家点确认之后**才扣

        ⚠️ **已知小口子**：MC 执行完、这个回报到达之前 AstrBot 掉线 ⇒
           东西给了、好感没扣。接受（与「先扣后退」的复杂度相比更划算）。
        """
        now = time.monotonic()
        item, _why = actions.take_pending(self.plugin._pending_actions, player, token, now)
        if item is None:
            # 过期/重复/不是他的 —— 静默。MC 那边已经执行了，这边无从补救，
            # 但也不该把别人的好感算到他头上。
            logger.warning(
                f"NetherLink: 收到无法匹配的 action_result | 玩家={player} "
                f"token={token} ok={ok}"
            )
            return
        if not ok:
            logger.info(
                f"NetherLink: 动作未执行（MC 端回报失败）| 玩家={player} "
                f"token={token} 原因={detail or '未说明'}"
            )
            self.write_action_audit(item, player, False, detail, via_card=True)
            return
        await self.apply_action_karma(item, player)
        # 🔴 **成功就必须播报** —— 2026-10-11 真机：改架构时漏了这段，
        #    玩家点了确认、东西到手了，公屏上却什么都没有（静默执行）。
        #    ⚠️ 放在 cost 判断之外 —— 免费动作一样要有播报。
        await self.announce_action_done(item, player)
        self.write_action_audit(item, player, True, via_card=True)
    @staticmethod
    def _signed(n: int) -> str:
        """好感的带符号写法（给 AI 看的话术里用）。"""
        return ("+" + str(n)) if n > 0 else str(n)

    def write_action_audit(self, item, player: str, ok: bool,
                            detail: str = "", via_card: bool = True) -> None:
        """动作结算落一条审计（与指令审计同一个开关、同一份 audit.jsonl）。

        🔴 **为什么单独一个 helper**：动作不产生「指令原文」，但
        `apply_action_karma` 在 `delta == 0` 时**提前返回**
        ⇒ 免费动作（`model: none`）在那里面记就漏了。所以审计必须挂在
        **结算点**上，而不是挂在扣好感那一步。四个结算点：
        `offer_action_confirm` 的空 steps 分支 / `handle_action_result` 的
        成功与失败分支 / `execute_action`。
        ⚠️ 2026-10-11 之前**一个都没有** —— 面板审计页对动作零记录（静默）。

        `initiator` / `cmd` / `event` 三个字段要齐 —— 面板的过滤只认它们
        （`panel.audit_view`）。动作没有指令，`cmd` 就放动作名，管理员搜得到。
        """
        try:
            self.plugin._write_audit(
                "action",
                initiator=player,
                cmd=item.action_name,
                server=item.server_id or "auto",
                action=item.action_name,
                token=item.token,
                ok=bool(ok),
                via_card=bool(via_card),
                delta=int(getattr(item, "karma_delta", 0) or 0),
                note=detail or "",
            )
        except Exception as e:
            logger.warning(f"NetherLink: 动作审计失败（已忽略）: {e}")

    async def apply_action_karma(self, item, player: str) -> None:
        """按 `karma_delta` 改好感（带符号：正加负扣）。

        🔴 **原先这里写死 `-item.cost`** —— 只会做减法，送礼那种要**加**好感的
        动作根本没法表达。现在统一走带符号的 `karma_delta`。

        ⚠️ 与 `exec_command_for` 同一口径：身份键走唯一入口，不传 origin
        （origin 是给「对话触发的自主增减」用的，动作结算不属于那一类）。
        """
        delta = int(getattr(item, "karma_delta", 0) or 0)
        if delta == 0:
            return
        key = self.plugin.karma.identity_key("game", player, "")
        await self.plugin.karma.add(key, delta)
        logger.info(
            f"NetherLink: 动作结算好感 | 玩家={player} 动作={item.action_name} "
            f"好感{self._signed(delta)} token={item.token}"
        )

    async def announce_action_done(self, item, player: str) -> None:
        """动作执行成功后播报（公屏 + 绑定群）。

        模板取动作自己的 `done`，缺省回退配置项 `template_action_done`。
        ⚠️ 播报失败不能连累扣费/执行（那已经发生了），只记一条 warning。
        """
        try:
            act = self._find_action(item.action_name)
            tpl = (getattr(act, "done", "") if act else "") or \
                self.plugin.templates.get("action_done", "")
            if not tpl:
                return
            vals = (item.values or {}).get("values") or {}
            # `{items}` 走 render_done_items —— 它按「显示名 → ID」的优先级取，
            # 修的是用户报的「播报显示物品 id 而不是显示名」。
            items = ""
            if act is not None:
                try:
                    items = actions.render_done_items(act, vals)
                except Exception as e:
                    logger.warning(f"NetherLink: 播报物品名渲染失败（忽略）: {e}")
            # 🔴 **把动作声明的每个参数都喂进去**（2026-10-11 加传送动作时发现）：
            #    `done` 是**目录驱动的**，能用任何参数名，而 `_fmt` 走
            #    `str.format` —— 少喂一个就 KeyError、被兜底成**原样返回**，
            #    公屏上就打出字面量 `{x} {y} {z}`。
            #    `{display_name}` 那次（同一天）是同一个坑的一个实例；
            #    修法是「全喂」，不是「再补一个」。
            kw = {k: v for k, v in vals.items() if isinstance(k, str)}
            kw.update(
                player=player,
                cost=item.cost,
                delta=getattr(item, "karma_delta", 0),
                items=items,
                item=str(vals.get("item") or vals.get("display_name") or ""),
                display_name=str(vals.get("display_name") or ""),
                count=int(vals.get("count") or 1),
                server=self.plugin.transport.server_display(item.server_id),
            )
            line = self.plugin.transport.fmt(tpl, **kw)
            # 🔴 **另发一份结构化部件**（用户 2026-10-11 要求）：公屏那条要把
            #    提到的物品**染蓝 + 带 show_item 悬停**，而纯文本行办不到。
            #    按 `{items}` 把模板切成「文本段 / 物品段」——
            #    QQ 群那条仍用纯文本 `line`（那边没有 hover 这回事）。
            parts = []
            if act is not None and "{items}" in tpl:
                entries = actions.render_done_item_entries(act, vals)
                kw_no_items = {k: v for k, v in kw.items() if k != "items"}
                for i, seg in enumerate(tpl.split("{items}")):
                    if i:
                        parts.append({"type": "items", "items": entries})
                    txt = self.plugin.transport.fmt(seg, **kw_no_items) if seg else ""
                    if txt:
                        parts.append({"type": "text", "text": txt})
            payload = {"type": "bot_reply", "line": line}
            if parts:
                payload["parts"] = parts
            await self.plugin.transport.send_to_mc(payload, item.server_id)
            await self.plugin.transport.broadcast(line)
        except Exception as e:
            logger.warning(f"NetherLink: 动作完成播报失败（忽略）: {e}")

    async def handle_action_confirm(self, player: str, token,
                                     server_id: str = "") -> str:
        """玩家点了确认：取出待确认项并执行。返回给玩家的回话。

        由 MC 端上报（计分板变更 → `action_confirm{player,token,server_id}`）调用。
        """
        if not self.plugin.enable_actions:
            return ""
        # 🔴 **纵深防御**：`< TOKEN_BASE` 的整数是 MC 端本地菜单的码
        # （1 / 10 / 1000+i），它本该在 MC 端就地消费掉、根本不上报。
        # 万一对面是旧版 jar 仍照旧上报，这里必须**静默忽略**——回一句
        # 「确认已失效」是错的：那既不是确认请求，玩家也没发过什么请求。
        try:
            if int(token) < actions.TOKEN_BASE:
                return ""
        except (TypeError, ValueError):
            pass
        now = time.monotonic()
        item, why = actions.take_pending(self.plugin._pending_actions, player, token, now)
        if item is None:
            await self._tell_player(player, why, server_id)
            return why
        act = self._find_action(item.action_name)
        if act is None:
            msg = "动作已不存在（目录可能改过），请重新发起请求。"
            await self._tell_player(player, msg, server_id or item.server_id)
            return msg
        return await self.execute_action(act, item.values, item.cost, item.cmd,
                                         server_id=server_id or item.server_id)

    def _find_action(self, name: str):
        """按名字从**当前**目录里找动作（不缓存：目录可改、重载后要拿新的）。"""
        if not name:
            return None
        try:
            for a in actions.load_actions(str(self.actions_path())):
                if a.name == name:
                    return a
        except Exception as e:
            logger.warning(f"NetherLink: 读动作目录失败（{name}）: {e}")
        return None

    async def _tell_player(self, player: str, text: str,
                           server_id: str = "", color: str = "gray") -> None:
        """私聊给某个玩家一句话（不广播）。用于告知「失效 / 过期 / 不是你的」。

        🔴 **用 `tellraw` 而不是 `tell`**（用户 2026-10-12）：原版 `tell` 会在
        消息前面加「某某某悄悄对你说」那种前缀，看着像玩家在跟他说话；
        `tellraw` 指定目标发的是干净的文本组件，还能上色。
        ⚠️ 与进服提示同一套：值里的双引号要**转义**（不是替换成单引号 ——
        那会静默改掉数据，见 `_tellraw_json_escape`）。`\n` 转义成 JSON 的换行。
        """
        if not player or not text:
            return
        try:
            safe = (_tellraw_json_escape(_strip_section_codes(text))
                    .replace("\n", "\\n"))
            payload = ('[{"text":"%s","color":"%s"}]' % (safe, color))
            await self.plugin.transport.send_to_mc(
                {"type": "command", "id": uuid.uuid4().hex,
                 "cmd": f"tellraw {player} {payload}"},
                server_id,
            )
        except Exception as e:
            logger.warning(f"NetherLink: 私聊告知玩家失败（忽略）: {e}")

    async def execute_action(self, action, values, cost, cmd, event=None,
                              server_id: str = "", karma_delta: int = None) -> str:
        """执行一个动作：改好感 + 下发指令 + 播报（`confirm: false` 的动作走这里）。

        🔴 **两条结算路径必须同口径**（2026-10-11：`refuse_gift` 三条全部踩中）：
          · `confirm: true`  → `offer_action_confirm` 只发卡；玩家点确认后
            MC 回报，由 `handle_action_result` → `apply_action_karma`
            + `announce_action_done` 结算
          · `confirm: false` → **本函数**当场结算
        原先本函数**自己另写了一套**减价与播报，于是两处立刻分叉：
          ① 只认 `cost`，而 `model: param`（如 `refuse_gift`）的
             `resolve_karma` 恒返回 0 ⇒ **delta 永远不生效**
          ② 播报只喂 player/cost/item/count/server，且只 `transport.broadcast`
             —— `{display_name}` 不在其中，`transport.fmt` 兜底**原样输出**；
             也漏了推 MC 公屏那一步
          ③ `template: ""` 的动作仍调 `exec_command_for("")`，它返回
             「错误：指令为空。」⇒ AI 把这条当动作失败
        ⇒ 现在统一复用上面那对函数，本函数不再碰 `_fmt`。
        """
        # ⚠️ 留空会走 `transport.send_to_mc` 的「未指定目标」保护（多台在线时拒发）。
        #    先从事件取来源服，仍为空才交给空串语义（QQ 侧发起的场合）。
        if not server_id:
            server_id = self._action_server_id(event)
        player = str(values.get("player") or "")
        # 🔴 `cmd` 为空 = 这个动作**没有指令可下发**（`template: ""`，
        #    如 refuse_gift 只改好感）。此时不要走 exec_command_for：
        #    它会把空串当用户错误回一句「错误：指令为空。」，而 AI 收到
        #    工具结果会以为动作失败了。
        out = ""
        if cmd:
            try:
                out = await self.plugin.commands.exec_command_for(
                    initiator=player, cmd=cmd, source="game",
                    cost=cost, server_id=server_id,
                )
            except Exception as e:
                logger.error(f"NetherLink: 执行动作 {action.name} 失败: {e}")
                return f"执行失败：{e}"
        # 播报与好感都交给与「点确认」路径共用的那对函数。
        item = actions.PendingAction(
            token=0,
            action_name=action.name,
            # 与 `offer_action_confirm` 登记的形状一致（`announce_action_done`
            # 只读 `values["values"]`，steps 是空列表）
            values={"steps": [], "values": dict(values)},
            player=player,
            server_id=server_id,
            cost=cost,
            cmd=cmd,
            deadline=0.0,
            karma_delta=(int(karma_delta) if karma_delta is not None
                         else -int(cost)),
        )
        # 🔴 **只在 `cost == 0` 时由这里改好感** —— 否则会和
        #    `exec_command_for` 的扣费**重复**（`karma_delta` 对非
        #    param 模型就是 `-cost`，同一笔钱收两遍）。
        #    `cost == 0` 正是 `model: param`（delta 全靠参数）与
        #    `model: none`（真免费，delta 也是 0）这两种情形。
        if not cost:
            await self.apply_action_karma(item, player)
        # ⚠️ 动作若有 `reason` 参数，**私聊**转达 —— 这是它 desc 承诺的语义
        #    （2026-10-11：原先没有任何一处读它，AI 以为说了、玩家什么都没看到）。
        #    私聊而非公屏：拒收理由上公屏会尴尬。
        reason = str(values.get("reason") or "").strip()
        if reason:
            await self._tell_player(player, reason, server_id)
        await self.announce_action_done(item, player)
        self.write_action_audit(item, player, True, via_card=False)
        if not cmd:
            return (
                f"已完成（{player} 的好感变化 {self._signed(item.karma_delta)}）。"
                f"\n你可以就此回一句简短的话，不要再调工具。"
            )
        return out

