# -*- coding: utf-8 -*-
"""NetherLink 好感度服务（2026-10-13 重构阶段二）。

从 main.py 的 NetherLinkPlugin 抽出：好感读写 / 降级 / 迁移 / 配置镜像同步。
⚠️ 与阶段一不同：这里搬的是**实例方法与状态**，不是纯函数。
Plugin 上留 `self.karma` 作为入口。

与 `BindingService(self)` 同模式：**持有 plugin 引用**做跨域操作
（`plugin.config` / `plugin.bindings.table` / `plugin._parse_config_records`），
**自己拥有域状态**（`store` / `lock` / `degraded`）。

⚠️ **store 由插件构造、本服务只收下**（见 `__init__` 的 docstring）：
构造那一段是按「正常 / 配置播种 / 空表+降级」三态走的，依赖
`_parse_config_records`——它**留在插件上**（`__init__` 与面板两处都用，
`test_panel_routes` 还直接 `monkeypatch.setattr(NetherLinkPlugin, …)`）。
"""

from __future__ import annotations

import asyncio
import json

from astrbot.api import logger

# ⚠️ 与 main.py 里那一串同目录模块**同一个双路导入写法**（坑 14）：
# AstrBot 把插件当**包**导入时 `from . import x` 成立；某些加载路径把它当
# **顶层模块**，那时只有 `import x` 能用。写成裸导入 = 插件**整个加载失败**
# （不是「好感功能不可用」，而是 QQ↔MC 互通 / 绑定 / 指令执行一起没）。
try:
    from . import binding_flow
    from . import bindings
    from .karma import KARMA_MAX_RECORDS, karma_identity_key
except ImportError:  # 插件以顶层模块方式加载时
    import binding_flow
    import bindings
    from karma import KARMA_MAX_RECORDS, karma_identity_key


class KarmaService:
    """好感域的运行时状态与流程。

    ⚠️ 构造顺序要求：插件的 `__init__` 必须**先**把 store 建好
    （含降级三态的判定），**再**构造本服务。
    """

    def __init__(self, plugin, store, lock=None, degraded: bool = False) -> None:
        """收下插件引用与**已经建好的** KarmaStore。

        ⚠️ **store 的构造刻意留在插件 `__init__`**，不由本服务自建：那一段
        按「正常 / 配置播种 / 空表 + 降级」三态走，且要调
        `_parse_config_records`——那个方法**留在插件上**（`__init__` 与面板
        都要用，`test_panel_routes` 还直接 patch 插件类）。**域状态归本服务、
        加载策略归插件**，两边不重叠。
        """
        self.plugin = plugin
        # 域状态：好感表 / 它的锁 / 降级标志
        self.store = store
        self.lock = lock if lock is not None else asyncio.Lock()
        self.degraded = degraded
        # 域参数：构造时从插件取一份。插件上那份**仍然留着**——面板 handler 与
        # `mc_command` 的提示词都要读 `self.karma_min` 等，两处语义同一。
        self.config = plugin.config
        self.karma_min = plugin.karma_min
        self.karma_max = plugin.karma_max
        self.karma_initial = plugin.karma_initial
        self.log_karma_changes = plugin.log_karma_changes

    async def get(self, key: str) -> int:
        """读取好感值（不写盘）。AI 每次对话都会查询，必须无副作用。"""
        async with self.lock:
            return self.store.get(key, self.karma_initial)

    async def add(self, key: str, delta: int, origin: str = "") -> tuple:
        """增减好感并落盘 + 写回配置项，返回 (旧值, 新值)。

        delta=0 时只读不写，避免 AI 每次对话的查询触发写盘。

        origin 非空表示这是**对话触发**的变化（AI 依 karma_rules 自主增减，
        而非指令消耗 / 死亡惩罚 / 回滚补偿），在 log_karma_changes 开启时记一条
        info 日志——对话触发的变化没有别的痕迹，运维只能从这里观察 AI 的增减行为。
        传「游戏内对话」或「QQ 对话」标明来源侧。
        """
        async with self.lock:
            # 先取一次旧值：add 在落盘失败时已经改过内存，只有提前取过才能报出真正的旧值
            before = self.store.get(key, self.karma_initial)
            try:
                old, new = self.store.add(key, delta, self.karma_initial)
            except Exception as e:
                # 落盘失败（磁盘满/权限）或 delta 不是数字时不能让对话崩掉。
                # 旧值取改动前的快照，新值取内存当前真实值，与随后的 get 口径一致。
                logger.error(f"NetherLink: 好感度写入失败（{key}, delta={delta!r}）: {e}")
                now = self.store.get(key, self.karma_initial)
                return before, now
            if delta:
                # last_dropped 是单槽状态，必须在同一把锁内紧接 add 之后读取，
                # 否则并发 add 会把它覆盖成别人的淘汰结果
                dropped = list(self.store.last_dropped)
                if dropped:
                    logger.warning(
                        f"NetherLink: 好感度记录已达上限 {KARMA_MAX_RECORDS} 条，"
                        f"按好感值从低到高淘汰 {len(dropped)} 条: {dropped}"
                    )
                self.sync_to_config()
                if origin and self.log_karma_changes:
                    logger.info(
                        f"NetherLink: [{origin}] {key} 好感 {old} → {new}"
                        f"（{delta:+d}，范围 {self.karma_min}~{self.karma_max}）"
                    )
            return old, new

    async def set(self, key: str, target: int) -> tuple:
        """把某键**设成** `target`，返回 `(结果, 旧值, 新值)`。

        结果二选一（与 `delete` 同一套风格，调用方按它决定回什么给面板）：
        - `"set"`：内存与配置项都按目标值更新完了（`old` / `new` 是真实前后值）；
        - `"degraded"`：内存表没能从配置项播种，**整条设置被拒绝**（什么都没动，
          `old` / `new` 都是 None）。

        实现是「读出当前值、算出 delta、再走 `add`」——内存、镜像、
        配置项因此全部经**既有入口**更新。面板不自己碰 `KarmaStore`：
        那会多出一条写法不同的写回路径，两处迟早漂移。

        🔴 **降级态必须在动手之前拒绝**（理由同 `delete`）：那张内存表
        没能从配置项播种、是不完整的；而 `_save()`（构造时 `path=None`）与
        `sync_to_config` 在降级态下**都拒绝写**——照常走下去，管理员会
        拿到一份「看起来成功、实际一个字节都没存」的回执。先改内存再放弃写回
        同样不行，那会留下「内存变了、配置项没变」的分叉，重启后静默回滚。
        ⚠️ **判据直接读 `self.degraded`、不另取 `lock`**：它是
        构造期定死的常量、之后不再变化；而下面的 `get` / `add`
        **各自会取同一把锁**（`asyncio.Lock` 不可重入），在这里持锁再调它们会死锁。

        ⚠️ **取当前值与写回之间存在竞态**：这中间 AI 的对话性增减可能插进来，
        于是最终值不是 `target`。面板是管理员功能、同一时刻只有一个人在用，
        这个窗口可以接受；真要消除得让 `add` 支持「绝对值」语义，
        那是改一个被多处调用的接口，代价与收益不成比例。

        ⚠️ **目标值等于 `karma_initial` 且该键原本没有记录时不会新建记录**：
        `add` 在 delta=0 时直接返回、不写盘（既有约定，避免每轮对话的
        查询都触发写盘）。此时「没有记录」与「记录等于 initial」在 `get`
        眼里本来就是同一个值——语义上确实什么都没变，面板照实显示即可。

        ⚠️ 校验（整数、范围、非空键）**不在这里**，在面板 handler 里做完才调本方法：
        本方法是「已经合法的目标值」的落地，不是入参闸门。
        """
        if self.degraded:
            logger.error(
                f"NetherLink: 好感记录处于降级态，拒绝设置 {key} —— "
                f"写回会清掉管理员手填的 karma_records"
            )
            return "degraded", None, None
        current = await self.get(key)
        old, new = await self.add(key, int(target) - int(current))
        return "set", old, new

    def write_config(self) -> bool:
        """把当前好感快照**无条件**写回配置项，返回是否真的写了。

        ⚠️ **与 `sync_to_config` 的分工**，别把两者合并：
        那条是**内部记账**路径，带一条「空快照不许覆盖非空配置」的守卫，
        本意是护着管理员在 WebUI 手填的 `karma_records`。删掉**最后一个**键时
        快照恰好为空，那条守卫会**拒绝写回**——于是配置项留着旧记录、镜像文件
        被写成空的、面板显示「删除成功」，而下次启动配置项把记录**原地带回来**。
        管理员说删，就得真的删，所以这里显式写。
        ⚠️ **降级态仍然不写**（`degraded`）：那张内存表没能从配置项播种，
        是不完整的，拿它写回等于清掉管理员手填的记录。这是一条**一致性**守卫，
        与"内部记账还是显式操作"无关，任何路径都不该绕开它。
        （`delete` 还会在**动内存之前**再判一次，那里才是主判据；
        本方法这一道是防线，保的是将来新增的调用方。）
        """
        if self.degraded:
            logger.error(
                "NetherLink: 好感记录处于降级态，跳过显式写回配置，"
                "以免清空管理员手填的 karma_records"
            )
            return False
        try:
            snapshot = self.store.snapshot()
            # 与 sync_to_config 同一形态：schema 把本项声明为 type=text，
            # 落盘必须是字符串（写 dict 会让 WebUI 把对象塞进文本控件）。
            self.config["karma_records"] = json.dumps(snapshot, ensure_ascii=False)
            self.config.save_config()
            return True
        except Exception as e:
            logger.error(f"NetherLink: 显式写回 karma_records 配置失败: {e}")
            return False

    async def delete(self, key: str) -> tuple:
        """删除一个好感键，返回 `(结果, 旧值)`。

        结果四选一（字符串常量，调用方按它决定回什么给面板）：
        - `"deleted"`：内存与配置项都已删掉，`old` 是被删掉的那个值；
        - `"missing"`：本来就没有这条记录，**无变化、也没写盘**（`old` 为 None）；
        - `"degraded"`：内存表没能从配置项播种，**整条删除被拒绝**（什么都没动）；
        - `"write_failed"`：内存删了、**配置项没写成**（写盘抛异常）——重启后
          这条记录会从配置项回来。调用方必须把这种「只有一半生效」如实报出来。

        ⚠️ **删除 ≠ 设成 0**：删掉之后 `get` 返回 `karma_initial`（默认 20），
        而且面板上不再有这一行；「设成 0」是 `set` 的活。两者在
        「值恰好等于 initial」时会看起来一样，但记录的有无不同——这一点必须让
        管理员看得见（handler 把它写进了返回体的 message）。
        ⚠️ **降级判定必须在动内存之前**：先删内存、再放弃写回，会留下
        「内存里没了、配置项里还在」的分叉，比直接拒绝糟糕得多。
        """
        async with self.lock:
            if self.degraded:
                logger.error(
                    f"NetherLink: 好感记录处于降级态，拒绝删除 {key} —— "
                    f"写回会清掉管理员手填的 karma_records"
                )
                return "degraded", None
            existed, old = self.store.remove(key)
            if not existed:
                return "missing", None
            if not self.write_config():
                # 走到这里只可能是写盘抛了异常（降级态上面已经拦掉）。内存已经删了、
                # 配置项没写成——重启后这条记录会从配置项回来。这种「只有一半生效」
                # 的状态必须报出来，混进 "deleted" 里就是撒谎。
                logger.error(
                    f"NetherLink: 好感记录 {key} 已从内存删除，但写回配置项失败 —— "
                    f"重启后它会从配置项回来"
                )
                return "write_failed", old
            return "deleted", old

    async def migrate(self) -> tuple:
        """把已绑定玩家的 `mc:` 好感并入其 `qq:<QQ>`，返回 `(结果, 明细, 警告)`。

        结果四选一（与 `delete` 同一套风格，调用方按它决定回什么给面板）：
        - `"migrated"`：已并入并**写回了配置项**；
        - `"nothing"`：没有可迁移项，**一个字节都没动**（幂等的第二次点击走这里）；
        - `"degraded"`：内存表没能从配置项播种，**整类操作被拒**（什么都没动）；
        - `"write_failed"`：内存已改、**配置项没写成**——重启后回落到迁移前的状态。

        🔴 **为什么「算」与「写」分开、且只落一次盘**：
        任务书原稿建议「先写 `qq:` 的新值、再删 `mc:` 的旧键，中途失败要能重入」。
        那两句**不能同时成立**——两步各落各的盘时，第二次重入会拿**已经合并过的**
        `qq:` 当基线再加一次 `mc:`（10 + 20 变成 50 而不是 30）；反过来「先删后写」
        则是丢数据。

        这里改成：**从同一份快照算出完整目标，在内存里一次做完，最后只写一次
        配置项**。于是配置项（唯一权威来源）要么没动、要么是完整结果，两种都可
        安全重试；而重入时 `plan_migration` 会从**当前**状态重算——`mc:` 键已经
        没了，它返回空表，所以第二次点击是「无可迁移项」，绝不会再加一遍。

        ⚠️ `plan_migration` 的 lo/hi **必须传配置值**（`self.karma_min` /
        `self.karma_max`）：它的默认值写死 -50/100，而好感范围是可配的——用默认值
        会把改了范围的部署**静默**夹到错误的上限——面板显示的数字于是与规则里的
        范围对不上。
        ⚠️ 逐键写入走 `KarmaStore.add` / `remove`（与 `add` 同一个入口：
        **先改内存、后落盘**）。所以某一步的**镜像**写失败时，内存里的目标值已经
        到位——这里记一条 error 继续走完，让内存终态完整（真正的权威落点是最后
        那一次配置项写入，那一步失败才是 `write_failed`）。
        ⚠️ 绑定表由 `BindingService` 持有（`self.plugin.bindings.table`）——
        绑定域在阶段二先行外移，这里**不能**再写 `self.bindings.table`。
        """
        async with self.lock:
            if self.degraded:
                logger.error(
                    "NetherLink: 好感记录处于降级态，拒绝迁移 —— "
                    "写回会清掉管理员手填的 karma_records"
                )
                return "degraded", [], []
            snapshot = self.store.snapshot()
            plan = binding_flow.plan_migration(
                self.plugin.bindings.table, snapshot, self.karma_min, self.karma_max
            )
            if not plan:
                return "nothing", [], []
            # ⚠️ 要删哪些 `mc:` 键**由 binding_flow 给**，不在这里重写一遍判据：
            #    两处各写一份迟早分叉，而分叉的表现是删错/漏删、全程静默。
            consumed = binding_flow.migrated_keys(self.plugin.bindings.table, snapshot)

            changes = []
            for qq_key in sorted(plan):
                qq = qq_key[len("qq:"):]
                players = [
                    k[3:] for k in consumed
                    if bindings.lookup(self.plugin.bindings.table, k[3:]) == qq
                ]
                changes.append({
                    "key": qq_key,
                    "qq": qq,
                    "old": self.store.get(qq_key, self.karma_initial),
                    "value": plan[qq_key],
                    "players": players,
                    "removed": ["mc:%s" % p for p in players],
                })

            warnings = []
            for qq_key in sorted(plan):
                current = self.store.get(qq_key, self.karma_initial)
                try:
                    self.store.add(
                        qq_key, int(plan[qq_key]) - int(current), self.karma_initial
                    )
                except Exception as e:
                    warnings.append("镜像写入失败：%s" % qq_key)
                    logger.error(f"NetherLink: 迁移写 {qq_key} 时镜像落盘失败: {e}")
            for mc_key in consumed:
                try:
                    self.store.remove(mc_key)
                except Exception as e:
                    warnings.append("镜像写入失败：%s" % mc_key)
                    logger.error(f"NetherLink: 迁移删 {mc_key} 时镜像落盘失败: {e}")

            if not self.write_config():
                logger.error(
                    "NetherLink: 好感迁移已改内存，但写回 karma_records 失败 —— "
                    "重启后会回落到迁移前的状态，请修好后重启再试"
                )
                return "write_failed", changes, warnings

            logger.info(
                f"NetherLink: 好感迁移完成 —— {len(plan)} 个 qq: 键、"
                f"{len(consumed)} 条 mc: 记录并入"
            )
            return "migrated", changes, warnings

    def sync_to_config(self) -> None:
        """把好感度快照写回配置项，WebUI 刷新即可见。失败仅告警。

        两道保护都做在本方法里，而不是留给调用方——写回是一件事，
        「当前状态能不能写」是这个方法自己的属性；靠调用方自觉，
        漏判一次就等于把管理员手填的 karma_records 清空。
          1. 降级态（内存表没能从配置项播种）下快照不完整，一律拒绝写入；
          2. 空快照绝不允许覆盖非空的 karma_records。
        """
        if self.degraded:
            logger.error(
                "NetherLink: 好感记录处于降级态，跳过写回配置，"
                "以免清空管理员手填的 karma_records"
            )
            return
        try:
            snapshot = self.store.snapshot()
            if not snapshot and self.config_has_records():
                # 空快照绝不允许覆盖非空的 karma_records——那是管理员手填的记录，
                # 清掉就找不回来了。判据走 config_has_records：配置读不出来时
                # 也算「有」，同样不写。
                logger.error("NetherLink: 好感快照为空但配置项有记录，跳过写回以免清空")
                return
            # schema 把本项声明为 type=text / default="{}"，落盘必须是字符串：
            # 写 dict 会让 WebUI 把对象塞进文本控件，且配置项类型在 dict 与 str
            # 之间反复横跳。读取侧 _parse_config_records 两种都认，不受影响。
            self.config["karma_records"] = json.dumps(snapshot, ensure_ascii=False)
            self.config.save_config()
        except Exception as e:
            logger.error(f"NetherLink: 写回 karma_records 配置失败: {e}")

    def config_has_records(self) -> bool:
        """配置项里有没有记录——给写回守卫用的 fail-safe 判据。

        读不出来时**当作有**：空快照因此不会覆盖它。方向与
        `_parse_config_records` 抛异常一致——宁可少写一次，不可清空。
        ⚠️ `_parse_config_records` **留在插件上**（`__init__` 与面板都要用），
        这里经 `self.plugin` 调它。
        """
        try:
            return bool(self.plugin._parse_config_records())
        except Exception:
            return True

    def identity_key(self, source: str, initiator: str, qq: str) -> str:
        """好感身份键的唯一入口——把绑定表填进 `karma.karma_identity_key`。

        ⚠️ 绑定表由 `BindingService` 持有（`self.plugin.bindings.table`）；
        四个调用点都走本方法，别处不再直接读表。
        ⚠️ 名字**必须**含子串 `identity_key`：`tests/test_contracts.py` 有两条
        AST 断言，要求 `exec_command_for` / `dispatch_mc_event` 的函数体里出现
        这个子串。改成 `_karma_key` 会让那两条以「未找到」失败。
        """
        return karma_identity_key(source, initiator, qq, self.plugin.bindings.table)
