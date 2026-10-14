# -*- coding: utf-8 -*-
"""绑定域服务：QQ <-> 游戏账号绑定表 + 进服门禁 + 群内发码兑换。

2026-10-13 阶段二从 `main.py` 外移。与阶段一（纯函数外移）的差别：**这次搬的是
实例方法与实例状态** —— 域状态（绑定表 / 码簿 / 落盘路径 / 码簿锁）归本服务自己
拥有（`self.table` / `self.codes` / `self.path` / `self.lock`），跨域操作（模板渲染、
下行帧、推群、配置项）一律走 `self.plugin.X`。

⚠️ **数据根只从插件取**（`plugin._karma_dir`），**不**在本模块自己调
`get_astrbot_plugin_data_path()`：测试重定向的是 **main 模块的名字绑定**
（`test_karma_integration` 的 `main_module` fixture），本模块自己 import 一份再
调用，在真机（装了真 astrbot）上拿不到那份重定向 —— 会把测试数据写进真实数据目录。
"""

from __future__ import annotations

import asyncio
import random
import time
import uuid
from datetime import datetime
from pathlib import Path

from astrbot.api import logger

# ⚠️ 与 main.py 里那一串同目录模块**同一个双路导入写法**（坑 14）：
# AstrBot 把插件当**包**导入时 `from . import x` 成立；某些加载路径把它当
# **顶层模块**，那时只有 `import x` 能用。写成裸导入 = 插件**整个加载失败**
# （不是「绑定功能不可用」，而是 QQ<->MC 互通 / 好感度 / 指令执行一起没）。
try:
    from . import binding_flow
    from . import bindings
except ImportError:  # 插件以顶层模块方式加载时
    import binding_flow
    import bindings

# 提示词渲染的纯函数（阶段一已外移到 nl_prompt.py）——门禁渲染提示时要用。
try:
    from .nl_prompt import (
        _bind_hint_is_json,
        _bind_hint_plain_text,
        _fill_placeholders,
        _strip_section_codes,
        _tellraw_json_escape,
    )
except ImportError:  # 插件以顶层模块方式加载时
    from nl_prompt import (
        _bind_hint_is_json,
        _bind_hint_plain_text,
        _fill_placeholders,
        _strip_section_codes,
        _tellraw_json_escape,
    )


class BindingService:
    """绑定域的运行时状态与流程。

    ⚠️ 构造顺序要求：插件的 `__init__` 必须**先**设好 `_karma_dir`
    （本服务经它定位数据目录），**再**构造本服务。
    """

    def __init__(self, plugin) -> None:
        self.plugin = plugin
        # 绑定流程的码簿（内存态）：进程重启后旧码失效是合理的。
        # 码簿是跨协程的共享可变状态，读写都要拿锁。
        self.codes: dict = {}
        self.lock = asyncio.Lock()
        # 绑定表：QQ <-> 游戏 ID。与 karma.json 同目录。
        # ⚠️ `self.table` 就是运行期**唯一**那份表（好感键读它）——重绑落地时
        # 必须**重新赋值这个属性**；只写盘不赋值，内存里会一直是旧表直到重启。
        self.path: Path = bindings.bindings_path(plugin._karma_dir.parent)
        try:
            self.table: dict = bindings.load(self.path)
        except Exception as e:
            # 绑定表坏了不该挡住插件加载——退化为「谁都没绑定」
            logger.error(f"NetherLink: 绑定表加载失败（按空表继续）: {e}")
            self.table = {}

    def snapshot(self) -> dict:
        """绑定表的只读快照。工具在协程里读它，不直接读可变属性。"""
        return dict(self.table)

    def commit_table(self, new_table: dict, old_table: dict) -> bool:
        """把新绑定表**整体替换**进运行期那份并落盘；落盘失败则**回滚内存**。

        ⚠️ 必须整体替换 `self.table`（不是就地改）——`bindings.upsert` /
        `bindings.remove` 都返回新表，而运行期那份是**唯一真源**，两处必须同步换掉。
        ⚠️ 落盘失败时**把内存也退回去**：内存改了、文件没改，等于面板显示改好了、
        重启后回到旧值——那是最难查的一种「成功」。退回去之后内存与磁盘始终一致，
        调用方只需据返回值如实报错。
        """
        self.table = new_table
        try:
            bindings.save(self.path, self.table)
            return True
        except Exception as e:
            self.table = old_table
            logger.error(
                f"NetherLink: 绑定表落盘失败（已回滚内存，两边保持一致）: {e}"
            )
            return False

    async def gate_unbound_player(self, player: str, server_id: str) -> bool:
        """未绑定玩家进服：发码 + 公屏提示，**并按 `binding_join_gate` 决定踢不踢**。

        🔴 **两个开关的分工**（2026-10-07 用户要求）：
          · `enable_binding`（**总开关**）—— 关掉 = 没有绑定功能：不发码、不提示、
            不踢（调用方根本不会走到这里）。
          · `binding_join_gate`（**子开关**）—— **只管踢不踢**。关掉时玩家
            **照常收到提示与验证码**、照常进服，只是**不被踢出**。
        ⚠️ 拆分前两者是**串联**的（`enabled = enable_binding and binding_join_gate`），
        于是「只想提醒、不想踢人」这种合理配置**做不到**——关掉子开关会把提示
        也一起关掉。现在可以了。

        **返回值**：`True` = 已把他踢出去（调用方必须 `return`，别再推群）；
        `False` = 没踢（子开关关着、拿不到可用群号、或执行出错），
        调用方照常走后面的推群逻辑。
        ⚠️ **返回 `False` 绝不该把玩家丢掉**：他收了提示、进了服，就该照常出现在群里。

        ⚠️ 三条刻意的设计（都有守卫盯着，改动前先想清楚）：
        1. **只走游戏内**——不额外 `_broadcast`，否则一条 join 会推两次群
        2. **私聊发码，不广播**（2026-10-09 用户要求）：走 `command` 帧执行
           `/tell <玩家> <提示>`，只发给本人。此前用 `bot_reply` 广播到公屏，
           同服任何玩家都能看到别人的验证码，可以抢先绑定。
        3. **踢出用 `command` 帧**（`kick`）：与 `/tell` 同一条控制台通道，
           四个 MC 端通用，且踢出界面上显示的原因就是那份说明
        4. **仍然落服务端日志**（用户要求保留）：实测真 Paper 26.3，`/tell` 从
           控制台发出时走 `CommandSourceStack.sendChatMessage` →
           `MinecraftServer.sendSystemMessage` → `LOGGER.info("System chat: ...")`，
           所以「码写进日志」这条特性不变，不必改任何 MC 端

        **返回值**：`True` = 已经把他拦下了，调用方应当 `return`（不要再推群）；
        `False` = 门禁**没有生效**（拿不到可用群号，或执行出错），
        调用方要照常走后面的推群逻辑——**静默把玩家丢掉是最糟的结局**。

        ⚠️ **拿不到一个「能被插件处理的」群号，就放行（fail-open）：不踢、不发码**。
        两种情形：
          1. 压根没有群号——没配 `binding_group`，且 `group_names` 为空或多于一个；
          2. `binding_group` 填了一个**不在 `group_names` 里**的群号。
        后果完全一样，而且很重：`on_group_message` 与 `on_decorating_result` 都以
        「群号 ∈ `group_names`」为门槛，**那个群的消息插件根本不处理**——玩家拿到的
        是一张永远兑换不了的码。若还把他踢了，他连进来喊一声都做不到。
        所以这里必须放行：**一个字段打错，不该让全服新人被挡在门外**。

        ⚠️ 这条判据放在**本方法里**而不是 `groups()` 里：那个函数的契约
        就是「配了 `binding_group` 就原样返回它，否则返回全部绑定群」，它**不做**
        成员校验；「群号可不可用、该不该因此放行」是**门禁的策略**。
        ⚠️ **「留空 = 全部绑定群都可用」是用户 2026-10-07 的要求**：改动前那种
        「多个群却留空 → 只记一条 warning、门禁不生效」的行为已废除。
        ⚠️ 发码**本来**就已在所有绑定群里生效（`on_group_message` 的门槛是
        「群号 ∈ `group_names`」），本函数只决定**提示玩家去哪些群**。

        ⚠️ 这里**不额外记 warning**：配置问题应该报**一次**，而进服是高频事件，
        逐次刷只会把别的东西淹掉。提示改在 `__init__` 的启动检查里报
        （搜「binding_group 不在绑定群名单里」），那里才是运维会看的地方。
        """
        groups = self.groups()
        # ⚠️ 除了「一个群都没配」，「群号不在绑定群名单里」同样要放行——见 docstring。
        usable = [
            g for g in groups if str(g).strip() and g in self.plugin.group_names
        ]
        if not usable:
            return False
        # ⚠️ `binding_code_length` / `binding_code_ttl` 必须**真的传下去**：
        # 它们曾被读进 self 却没有任何调用点使用（`binding_flow` 内部只认自己的
        # 模块常量），于是「配了 60 秒 TTL、结果还是 5 分钟」静默发生。
        # ⚠️ 位数也要传给 `match`（下一处），否则发 8 位码、按 6 位判形状 =
        # 码永远兑不上 = 全体锁死。
        code = binding_flow.new_code(
            random.randrange, self.plugin.binding_code_length
        )
        async with self.lock:
            self.codes = binding_flow.issue(
                binding_flow.prune(self.codes, time.monotonic(),
                                   self.plugin.binding_code_ttl),
                player, code, time.monotonic(),
            )
        # 走到这里 `usable` 必非空、且每个成员都在 `group_names` 里。
        # `{group}` 是**群名**（顿号分隔）；留空配置下就是全部绑定群的名字。
        group_label = self.group_names(usable)
        # ---- 渲染：JSON 模式与纯文本模式各走各的渲染器 ----
        # 值里的双引号会提前闭合 `/tellraw` 的 JSON，先换成单引号。
        values = {
            "server": self.plugin.transport.server_display(server_id),
            "player": player,
            "code": code,
            "group": group_label,
            "ttl": binding_flow.describe_ttl(self.plugin.binding_code_ttl),
        }
        safe_values = {k: _tellraw_json_escape(str(v)) for k, v in values.items()}
        hint_tpl = self.plugin.templates["bind_hint"]
        if _bind_hint_is_json(hint_tpl):
            # ✅ JSON 模式：显式替换（不碰 JSON 结构）→ 私聊发 tellraw
            #    踢出原因从同一份 JSON 的 `text` 节点推导，两边不会漂移。
            rendered = _fill_placeholders(hint_tpl, safe_values)
            plain = _strip_section_codes(_bind_hint_plain_text(rendered))
            hint_cmd = f"tellraw {player} {rendered}"
        else:
            # 纯文本模式（存量配置）：照旧走 `_fmt`。
            # ⚠️ `_fmt` 的 ValueError 语义**必须保留**：
            #    `test_gate_failure_degrades_to_not_gated` 用它验证「渲染炸了也要 fail-open」。
            plain = _strip_section_codes(self.plugin.transport.fmt(hint_tpl, **values))
            hint_cmd = f"tell {player} {plain}"
        # 提示**始终私聊发**（走到这里说明总开关开着、且拿得到可用群号）。
        # ⚠️ 不广播：码只该给本人，广播会让同服其他玩家抢绑。
        await self.plugin.transport.send_to_mc(
            {"type": "command", "id": uuid.uuid4().hex, "cmd": hint_cmd},
            server_id,
        )
        # ⚠️🔴 **插件自己记一条**（2026-10-10 用户要求）：
        # `/tellraw` 走 `ServerPlayer.sendSystemMessage` 只发 packet，**不落服务端日志**；
        # 而原来的 `/tell` 会走 `MinecraftServer.sendSystemMessage`
        # = `LOGGER.info("System chat: ...")`。换成 tellraw 后那条留痕会消失，
        # 所以由插件自己补上（与其他日志同样单行、竖线分隔）。
        logger.info(
            f"NetherLink: 绑定验证码已下发 | "
            f"服务器={values['server']} 玩家={player} "
            f"验证码={code} 有效期={values['ttl']}"
        )
        if not self.plugin.binding_join_gate:
            # 子开关关着 = 只提醒、不踢。玩家照常进服，调用方继续推群。
            return False
        # 踢出原因必须是**纯文本**：`KickCommand` 的 reason 走
        # `MessageArgument` → `Component.literal(text)`，不解析 JSON，且上限 256 字符。
        await self.plugin.transport.send_to_mc(
            {"type": "command", "id": uuid.uuid4().hex, "cmd": f"kick {player} {plain}"},
            server_id,
        )
        return True

    def groups(self) -> list:
        """哪些群可以接受验证码，按 `group_names` 的插入顺序返回**群号**列表。

        契约（改动前先读完）：
          · 配了 `binding_group` → 只返回它（成员校验**不在这里**做）；
          · 留空 → 返回**全部**绑定群（用户 2026-10-07 要求：留空 = 所有
            消息互通的群都能发码）；
          · `group_names` 为空 → 空列表。

        ⚠️ **本函数不做成员校验**：`binding_group` 配了一个不在 `group_names` 里的
        群号时照样原样返回——「这个群号能不能用、该不该因此放行」是**门禁的策略**。
        这是改动前就定下的分工，别合并进来。

        ⚠️ **返回群号、不是群名**：门禁要拿它跟 `group_names` 的键比对；而 `{group}`
        占位符要的是**群名**，由 `group_names` 映射。两者形态不同，
        别为了少一个函数而混用——混用会让门禁那句成员校验直接判假、门禁静默失效。

        ❌ 这里以前叫 `_binding_group_id`，且在「配了多个群却留空」时记一条 warning
        并返回空串——后果是**门禁整个不生效**（fail-open 放行）。用户明确要求改成
        「留空 = 所有互通群都可用」，所以那条 warning 一并删除：现在不再存在
        「拿不到群号」这个状态，只剩「一个群都没配」。
        """
        if self.plugin.binding_group:
            return [self.plugin.binding_group]
        return list(self.plugin.group_names.keys())

    def group_names(self, groups: list) -> str:
        """把群号列表渲染成给玩家看的**群名**串（顿号分隔）。

        `{group}` 占位符用它。查不到群名时回退群号本身——`group_names` 的既有口径
        就是「只填群号时值等于键」。配了 `binding_group` 时列表只有一个，
        行为与改动前完全一致。
        """
        return "、".join(self.plugin.group_names.get(g, g) for g in groups)

    async def try_bind_with_code(self, event, group_id: str, text: str) -> bool:
        """把一条群消息当作绑定码试试。**吞下即返回 True**（调用方 return，不再转发）。

        ⚠️ 形状判据在 `binding_flow.match` 内部（它先 `is_code_shaped` 再遍历码簿，
        非码形状一律返回 `("", "")`）——所以普通群聊原样落到本方法返回 False 那条路，
        照旧被转发进游戏。**先判形状再查码簿**的顺序也在那里，几百人的群下这不是
        白白的遍历开销。

        ⚠️ `servers` 传空串（`upsert` 的第 5 个参数）是**已知取舍**：这条路径只拿到
        一个群消息，**不知道玩家在哪台服**（码簿里只存 code/issued_at，见
        `binding_flow.issue`）。不影响绑定本身（键是游戏 ID），他下次进服时
        `_dispatch_mc_event` 的 `note_server` 会把当时那台服补进 `servers`。

        ⚠️ 落盘失败**必须让用户知道**：内存里的表已经改了，重启后却会没掉——不回话
        的话他会以为绑好了。这条路上 `return True`（吞掉这条消息）也是刻意的：
        码不该因为一次写盘失败而被原样转发进游戏公屏。
        """
        now = time.monotonic()
        already_bound = False
        async with self.lock:
            player, already = binding_flow.match(
                self.codes, self.table, text, now,
                self.plugin.binding_code_length, self.plugin.binding_code_ttl,
            )
            if not player:
                return False
            if already:
                # 已经绑过了：不覆盖。这里**只置标记**，回话挪到锁外（见下）。
                already_bound = True
            else:
                self.codes.pop(player, None)

        if already_bound:
            # ⚠️ **回话必须在锁外**：`_reply_to_group` 是一次 QQ 网络发送，而
            #    `gate_unbound_player` 需要**同一把锁**——在锁里 await 会把并发的
            #    进服门禁一起拖住（适配器卡住时尤其明显）。回话不碰任何共享状态，
            #    没有理由占着锁。
            await self.plugin._reply_to_group(
                group_id, "你已经绑定过游戏账号了；如需换绑请在管理面板操作"
            )
            return True

        qq = str(event.get_sender_id() or "")
        qq_name = str(event.get_sender_name() or "")
        self.table = bindings.upsert(
            self.table, player, qq, qq_name, "", "code",
            datetime.now().isoformat(timespec="seconds"),
        )
        try:
            bindings.save(self.path, self.table)
        except Exception as e:
            # ⚠️ 落盘失败要让用户知道——否则重启后绑定就没了，而他以为成功了
            logger.error(f"NetherLink: 绑定表落盘失败: {e}")
            await self.plugin._reply_to_group(group_id, "绑定写入失败，请稍后重试或联系管理员")
            return True

        await self.plugin.transport.broadcast_to_game(
            self.plugin.transport.fmt(
                self.plugin.templates["bind_success_game"],
                player=player, qq=qq, qq_name=qq_name,
            )
        )
        await self.plugin._reply_to_group(
            group_id,
            self.plugin.transport.fmt(
                self.plugin.templates["bind_success_qq"],
                player=player, qq=qq, qq_name=qq_name,
            ),
        )
        return True
