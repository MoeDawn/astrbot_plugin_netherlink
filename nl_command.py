# -*- coding: utf-8 -*-
"""NetherLink 指令执行服务（2026-10-14 重构阶段三 Task 4）。

从 main.py 的 NetherLinkPlugin 抽出**指令执行域**：控制台指令下发与回执等待
（run_console_cmd）、背包查询单帧往返（inspect_items）、好感回滚与取消退费
（rollback_karma / refund_on_cancel）、以及带原子扣费的执行入口
exec_command_for（QQ llm_tool 与游戏内 tool_loop_agent 共用）。

与 `Transport(self)` / `Dispatcher(self)` / `ActionFlow(self)` 同模式：**持有
plugin 引用**做跨域访问（`plugin.transport` / `plugin.karma` / 配置派生开关
`plugin.max_command_cost` / `plugin.log_command_audit` / `plugin._write_audit`），
**自己不拥有任何域状态**——待回执表 `_pending_cmds` / `_pending_inspects` 仍由
插件持有（ws 帧路由与 terminate() 要直接读写它们）。

方法名去掉了前导下划线（与 Transport / Dispatcher / ActionFlow 同约定），与
plugin 上的旧名一一对应：
  run_console_cmd   ← _run_console_cmd
  inspect_items     ← _inspect_items
  rollback_karma    ← _rollback_karma
  refund_on_cancel  ← _refund_on_cancel
  exec_command_for  ← exec_command_for（原名无下划线，不动）

⚠️ plugin 上的 `@filter.llm_tool(name="inspect_items")` 方法**留在原地**（框架
把装饰器绑 Star 实例），它转发调 `self.commands.inspect_items(...)`。
"""

from __future__ import annotations

import asyncio
import uuid

from astrbot.api import logger

# 兄弟模块双路导入（与 main.py / nl_action_flow.py 同一个写法）：AstrBot 把
# 插件当**包**导入时 `from . import x` 成立；某些加载路径把它当**顶层模块**，
# 那时只有 `import x` 能用。
try:
    from .karma import clamp_cost
except ImportError:  # 插件以顶层模块方式加载时
    from karma import clamp_cost

try:
    from .nl_protocol import CmdResult
except ImportError:  # 插件以顶层模块方式加载时
    from nl_protocol import CmdResult


class CommandRunner:
    """指令执行接缝：报价裁剪 -> 原子扣费 -> 下发等回执 -> 失败退费。

    ⚠️ 跨域状态一律走 `self.plugin.X`（待回执表 / 好感服务 / 传输服务 /
    配置派生开关 / `_write_audit` 都留在插件上）；本对象**无状态**，重建它
    不会丢任何东西——这也意味着测试里 `plugin.commands.xxx = stub` 打桩后
    要再经 plugin.commands 调用才生效（打在 plugin 上的旧桩已失效）。
    """

    def __init__(self, plugin) -> None:
        self.plugin = plugin

    # ------------------------------------------------------------------
    # 指令执行核心（QQ llm_tool 与游戏内 tool_loop_agent 共用）
    # ------------------------------------------------------------------
    async def run_console_cmd(
        self, cmd: str, timeout: float = 8.0, server_id: str = ""
    ) -> CmdResult | None:
        """以控制台身份执行一条指令。

        返回 None 表示**没有拿到回执**（未连接 / 发送失败 / 超时）；
        拿到回执则返回 CmdResult，其 ok 区分成功与明确失败。
        两种结局都要退费，但报给 AI 的话术不同。

        不做权限校验（仅供内部工具使用）；扣费与回滚由 exec_command_for 负责。
        """
        if not self.plugin.transport.connected(server_id):
            return None
        cmd_id = uuid.uuid4().hex
        fut = asyncio.get_running_loop().create_future()
        self.plugin._pending_cmds[cmd_id] = fut
        if not await self.plugin.transport.send_to_mc(
            {"type": "command", "id": cmd_id, "cmd": cmd.lstrip("/")}, server_id
        ):
            # 发送失败必须把登记撤掉，否则这里就是一份永不回执的僵尸 Future——
            # 只有 terminate() 才清得掉，其间 _pending_cmds 一直虚高。
            # 注意 _send_to_mc 返回 False 有两个来源：真的没连上/发送抛异常，
            # 以及"刚判断完就断线"的并发写竞态。竞态下这条 warning 属于误报，
            # 但方向是**多报**（宁可多留一条痕迹，也不放过真实的发送失败），
            # 这是刻意保留的：静默丢掉一条真实失败比多一条无用的 warning 贵得多。
            self.plugin._pending_cmds.pop(cmd_id, None)
            return None
        try:
            return await asyncio.wait_for(fut, timeout=timeout)
        except asyncio.TimeoutError:
            self.plugin._pending_cmds.pop(cmd_id, None)
            return None

    async def inspect_items(self, player: str, slot: str, match: str,
                             limit: int, server_id: str) -> str:
        """让 MC 端查玩家背包，返回**给 AI 看的文本**。

        返回的文本直接可读（含每件物品的槽位、显示名、ID、组件）——
        AI 拿它去填 `swap_items.pay` / `modify_item.targets` / `offer_karma.item`。

        ⚠️ **离线必须如实说**（不能返回空串或「没查到」—— 那会让 AI
        以为玩家身上真没有，从而去做错的判断）。与 `_run_console_cmd`
        同一口径：拿不到回执就是拿不到，如实交代。
        """
        if not self.plugin.transport.connected(server_id):
            return "MC 服务器当前不在线，查不到背包。"
        req_id = uuid.uuid4().hex
        fut = asyncio.get_running_loop().create_future()
        self.plugin._pending_inspects[req_id] = fut
        if not await self.plugin.transport.send_to_mc(
            {"type": "inspect", "id": req_id, "player": player,
             "slot": slot, "match": match, "limit": int(limit or 10)},
            server_id,
        ):
            self.plugin._pending_inspects.pop(req_id, None)
            return "发给 MC 服务器的请求没送出去（连接刚断？），查不到背包。"
        try:
            return await asyncio.wait_for(fut, timeout=8.0)
        except asyncio.TimeoutError:
            self.plugin._pending_inspects.pop(req_id, None)
            return "MC 服务器 8 秒没回应，查不到背包。"

    async def rollback_karma(
        self, key: str, spend: int, baseline: int, reason: str, cmd: str
    ) -> bool:
        """回滚一次好感扣减，返回是否真正复原。异常绝不外抛。

        `KarmaService.add` 会吞掉写盘异常（磁盘满/权限）而只记一条通用日志，因此
        「回滚失败」不能靠异常捕获来判断，必须事后验证两件事：
          1. 内存水位回到扣减前（baseline）；
          2. 这次复原真的落到了盘上——`KarmaStore.add` 是先改内存再 `_save()`，
             写盘失败时内存照样是对的，进程内看着没事，但重启后那笔扣减会从
             磁盘复活，玩家等于白付了钱。
        任一验证不过就单独留痕：玩家被白扣且没有自动补救，运维只能靠这条日志
        发现并手工补偿。
        """
        try:
            await self.plugin.karma.add(key, spend)
            # 水位校验与落盘校验都必须留在 try 内：本方法承诺「异常绝不外抛」，
            # 而 self.karma.get 与 _path 访问理论上仍可能抛。真抛了就会冒泡到
            # exec_command_for 的通用 except，只剩一条无归因的日志，
            # 「玩家被白扣」就失去了唯一可追查的痕迹。
            if await self.plugin.karma.get(key) != baseline:
                logger.error(
                    f"NetherLink: 好感回滚失败！[{key}] 被白扣 {spend} 点未退回"
                    f"（{reason}，内存水位未回到 {baseline}）: {cmd}"
                )
                return False
            if self.plugin.karma.store._path is not None:
                try:
                    # 补一次显式落盘：KarmaService.add 内部那次写盘失败已被它自己吞掉，
                    # 这里让同一个错误浮出来，回滚是否真的持久化才可判定。
                    self.plugin.karma.store._save()
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

            spend = clamp_cost(cost, self.plugin.max_command_cost)
            # 扣减前的水位，供回滚校验用。spend=0 时不读存储，此值不参与任何判断。
            cur = 0
            key = self.plugin.karma.identity_key(source, initiator, qq)
            # 审计：**请求**先留痕。放在这里是因为此刻 source/initiator/qq/
            # 裁剪后的报价/目标服都已确定；而「好感不足」这类拒绝也要留痕，
            # 所以不能放到执行成功之后。
            if self.plugin.log_command_audit:
                # ⚠️ **debug**：紧接着的「成功」那条会带上全部字段，
                # 每条指令打两行就是刷屏。拒绝/失败是异常路径，各有自己的日志。
                # `_write_audit` 照写——audit.jsonl 的结构化记录不受影响。
                logger.debug(
                    f"NetherLink: [指令审计] 请求 来源={source} "
                    f"发起者={initiator} qq={qq or '-'} "
                    f"目标={server_id or '自动'} 报价={spend} 指令={cmd}"
                )
                self.plugin._write_audit(
                    "request", source=source, initiator=initiator, qq=qq or "",
                    server=server_id or "auto", cmd=cmd, quoted=spend, spent=0, note="",
                )
            if spend:
                cur = await self.plugin.karma.get(key)
                if cur < spend:
                    logger.info(
                        f"NetherLink: 好感不足拒绝执行 [{key}] 需要 {spend} 现有 {cur}: {cmd}"
                    )
                    self.plugin._write_audit(
                        "rejected", source=source, initiator=initiator, qq=qq or "",
                        server=server_id or "auto", cmd=cmd, quoted=spend, spent=0,
                        note=f"好感不足（需要 {spend} 现有 {cur}）",
                    )
                    return (
                        f"好感不足：本次需要 {spend}，发起者当前好感为 {cur}，"
                        f"不足以支付，指令未执行。"
                    )
                await self.plugin.karma.add(key, -spend)

            # 扣减之后的所有 await 点都可能被取消：terminate() 取消 _pending_cmds 里的
            # Future（_run_console_cmd 里那个 await 直接吃到 CancelledError），卸载时
            # 框架也会取消本任务。CancelledError 自 3.8 起是 BaseException，下面那个
            # `except Exception` 接不住它——不接管的话，这笔已扣的好感既没换来执行、
            # 也不留任何痕迹，是唯一一条「扣了钱且无迹可查」的路径。
            try:
                if not self.plugin.transport.connected():
                    self.plugin._write_audit(
                        "failed", source=source, initiator=initiator, qq=qq or "",
                        server=server_id or "auto", cmd=cmd, quoted=spend, spent=0,
                        note="MC 未连接",
                    )
                    if spend and not await self.rollback_karma(
                        key, spend, cur, "MC 未连接", cmd
                    ):
                        return (
                            f"MC 服务器当前不在线，指令未执行；好感回滚亦失败，"
                            f"已扣的 {spend} 点未退回。"
                        )
                    return "MC 服务器当前不在线，无法执行指令。"

                result = await self.run_console_cmd(cmd, server_id=server_id)
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
                    self.plugin._write_audit(
                        "failed", source=source, initiator=initiator, qq=qq or "",
                        server=server_id or "auto", cmd=cmd, quoted=spend, spent=0,
                        note=reason,
                    )
                    if not spend:
                        return failure
                    if not await self.rollback_karma(key, spend, cur, reason, cmd):
                        return (
                            f"{failure}；好感回滚亦失败，已扣的 {spend} 点未退回。"
                        )
                    logger.error(
                        f"NetherLink: 指令执行失败（{reason}）已回滚好感 {spend} [{key}]: {cmd}"
                    )
                    return f"{failure}\n（好感未扣除）"

                # 成功路径的**唯一**一条日志（请求那条已降 debug）——所以它要
                # 自带全部字段，别指望读者再去看上一行。格式用 `键=值` 空格分隔，
                # 不给值加引号（`{initiator}` 会输出 `'桃核大'`，那对人是噪声）。
                if self.plugin.log_command_audit:
                    logger.info(
                        f"NetherLink: [指令审计] 成功 来源={source} "
                        f"发起者={initiator} qq={qq or '-'} "
                        f"目标={server_id or '自动'} 报价={spend} 消耗={spend} "
                        f"指令={cmd}"
                    )
                self.plugin._write_audit(
                    "success", source=source, initiator=initiator, qq=qq or "",
                    server=server_id or "auto", cmd=cmd, quoted=spend, spent=spend, note="",
                )
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
                await self.refund_on_cancel(key, spend, cur, cmd)
                raise
        except Exception as e:
            logger.error(f"NetherLink: 执行指令失败: {e}")
            return f"执行出错: {e}"

    async def refund_on_cancel(self, key: str, spend: int, cur: int, cmd: str) -> None:
        """取消路径的退费；异常绝不外抛（调用方紧接着要 re-raise，不能被挡住）。

        幂等：只在好感水位仍停在「扣减后」（cur - spend）时才退。取消可能落在
        `_rollback_karma` 内部的任意 await 点上（比如已经改完内存、正要去校验水位），
        此时再退一次就是白送好感；反过来，水位对不上也说明这笔钱压根没扣成。
        退费失败由 `_rollback_karma` 自己留痕，这里只负责不让它把异常带出去。
        """
        if not spend:
            return
        try:
            if await self.plugin.karma.get(key) != cur - spend:
                logger.info(
                    f"NetherLink: 指令被取消，但好感水位不在扣减后（应为 {cur - spend}），"
                    f"判定已退费或未扣成，跳过重复退费: {cmd}"
                )
                return
            if await self.rollback_karma(key, spend, cur, "任务被取消", cmd):
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
