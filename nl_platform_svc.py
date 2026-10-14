# -*- coding: utf-8 -*-
"""NetherLink 平台适配器服务（2026-10-14 重构阶段三 Task 5）。

从 main.py 的 NetherLinkPlugin 抽出**平台适配器域**：平台配置项的补写与
登记状态检查（init）、适配器解析（find_live_adapter / lookup_adapter /
resolve_adapter——**运行中的实例优先**于模块注册表）、陈旧实例检测
（current_adapter_class）与配置记录定位（entry）、可用性问答（have_adapter）、
热重载时丢弃缓存的 mc_platform 模块（drop_module_for_reload）。

🔴 **两个 `@filter` 钩子留在 plugin 上**（`_bind_game_platform` /
`_rebind_game_platform_after_reload`——框架把装饰器绑到 Star 实例），
它们的方法体转发本服务的 `self.platform.*`；框架交互（load_platform /
reload / bind_plugin）也留在钩子里。

与 `Transport(self)` / `Dispatcher(self)` 同模式：**持有 plugin 引用**做跨域
访问（`plugin.context` / config），**自己不拥有任何域状态**。

方法名与 plugin 上的旧名一一对应：
  init                   ← _init_game_platform
  drop_module_for_reload ← _drop_platform_module_for_reload
  find_live_adapter      ← _find_live_adapter
  resolve_adapter        ← _resolve_game_adapter
  lookup_adapter         ← _lookup_game_adapter
  current_adapter_class  ← _current_adapter_class
  have_adapter           ← have_adapter（原名无下划线）
  entry                  ← _game_platform_entry
"""

from __future__ import annotations

from astrbot.api import logger

# 本模块不 import 任何同目录兄弟模块（`mc_platform` 在方法体内按双形态
# 函数级导入，与原实现一致——它必须在 main.py 的**模块层**被 import 一次，
# 注册才赶得上框架的平台初始化；这里不重复那件事）。
# 反方向有：main.py 以双路导入方式引入本模块的 GamePlatform。

# 游戏侧在 AstrBot 里的**平台标识**。
# ⚠️ 与 main.py 的 GAME_PLATFORM_ID 逐字一致（那边有守卫钉住单一来源，
#    见 test_mc_platform.py 的 test_platform_service_id_matches_main——
#    与 mc_platform.PLATFORM_ID 的守卫同一条口径）。散落的字面量一旦对不上，
#    症状是「注入静默不生效」或「回复发不出去」，两者都不报错。
GAME_PLATFORM_ID = "netherlink_mc"


class GamePlatform:
    """平台适配器接缝：配置登记、适配器解析与陈旧实例检测。

    ⚠️ 跨域状态一律走 `self.plugin.X`（context / config 都留在插件上）；
    本对象**无状态**，重建它不会丢任何东西——这也意味着测试里
    `plugin.platform.resolve_adapter = stub` 打桩后要再经 plugin.platform
    调用才生效（打在 plugin 上的旧桩已失效）。
    """

    def __init__(self, plugin) -> None:
        self.plugin = plugin

    def init(self) -> None:
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
        if self.plugin.context is None:
            # 无上下文（测试替身、或框架尚未注入）时不存在平台管理器，
            # 也没有可写的配置——静默跳过。口径与 _apply_configured_tool_descs
            # 一致：这不是错误，刷一条 warning 只会污染日志。
            return
        try:
            cfg = self.plugin.context.get_config()
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
            # 平台初始化之后），见 plugin._bind_game_platform。
        except Exception as e:
            # 平台注册失败不能让插件加载失败——其余功能（QQ 侧、好感度）照常
            logger.error(f"NetherLink: 初始化游戏侧平台失败: {e}")

    def drop_module_for_reload(self) -> None:
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

    def find_live_adapter(self):
        """在 `platform_insts` 里找**正在运行**的游戏侧适配器实例。

        这是比模块级 `_ADAPTERS` 字典更权威的来源：热重载时 `mc_platform`
        会被重新导入，那个字典是**新的、空的**，而实例是**老的**——只查字典
        会以为「没有实例」，进而错误地再 `load_platform` 一个，
        于是两个实例并存（`send_message` 只匹配第一个，回复可能走旧实例）。
        """
        try:
            insts = getattr(self.plugin.context.platform_manager, "platform_insts", None)
        except Exception:
            return None
        for inst in insts or []:
            try:
                if inst.meta().id == GAME_PLATFORM_ID:
                    return inst
            except Exception:
                continue
        return None

    def lookup_adapter(self):
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

    def resolve_adapter(self):
        """取游戏侧适配器：**先用运行中的实例**，其次才查模块注册表。"""
        return self.find_live_adapter() or self.lookup_adapter()

    def have_adapter(self) -> bool:
        """平台路径此刻是否真的可用（适配器在、且已绑到本插件实例）。"""
        try:
            adapter = self.resolve_adapter()
        except Exception:
            return False
        return adapter is not None and adapter._plugin is not None

    def current_adapter_class(self):
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

    def entry(self) -> dict | None:
        """取配置里那条游戏侧平台记录（没有则 None）。"""
        try:
            platforms = self.plugin.context.get_config().get("platform")
        except Exception:
            return None
        if not isinstance(platforms, list):
            return None
        for item in platforms:
            if isinstance(item, dict) and str(item.get("id")) == GAME_PLATFORM_ID:
                return item
        return None
