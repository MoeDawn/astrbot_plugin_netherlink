"""NetherLink 协议层数据类。

从 main.py 外移（2026-10-13 重构阶段一）。**行为零变化** —— 原两个类逐字搬入，
只加模块级导入与 `from __future__ import annotations`。
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CmdResult:
    """一次控制台指令执行的结局。

    `ok=False` 表示**服务器明确回了失败**（Paper 侧捕获到异常，或 dispatchCommand
    返回 false），与 ok=True 但输出为空是两回事：

      ok=True  + output=""   -> 执行成功，只是没输出（照常收费）
      ok=True  + output=文本 -> 执行成功（照常收费）
      ok=False               -> 明确失败，**必须退费**
      （没有回执）            -> 超时，由 run_console_cmd 返回 None 表示，同样退费

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
