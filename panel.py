# -*- coding: utf-8 -*-
"""管理面板的**数据整形层**。

本模块只做一件事：把插件的**运行时状态**整形成能直接 `json_response` 出去的形状。
它**不 import web、不 import 插件**，所以能脱离 AstrBot 单测。

⚠️ 路由前缀必须是**插件名**：dashboard 拿注册时的 route **原样**与
`/api/v1/plugins/extensions/<插件>/<子路径>` 比对（源码是正则，不是 FastAPI）。
不带前缀的路由**永远匹配不上**，而且不报错——只表现为 404。

⚠️ **本模块不产出 `online_seconds`**：`McConn` 上只有 `ws` / `port` /
`reported_name` 三个字段，**没有连接时间戳**，算出不来。`servers_view` 的
`now` 形参因此**当前未被使用**——保留它是为了不改动既定的调用/测试签名
（面板要显示在线时长，得先在 MC 端上报连接时刻，那是另一件事）。
"""

# 🔴 必须与 metadata.yaml 的 `name` 一致。
ROUTE_PREFIX = "astrbot_plugin_netherlink"


def route(sub: str) -> str:
    """拼一条 API 路由。**全项目只此一处**拼路由，免得前缀写漏。"""
    return "/%s/%s" % (ROUTE_PREFIX, str(sub).lstrip("/"))


def servers_view(conns: dict, display_of, now: float) -> list:
    """在线服务器列表。

    ⚠️ 只列**未关闭**的连接：`_mc_conns` 里可能残留已关闭的条目，
    面板按「在不在线」展示，关掉的一律不算在线。
    ⚠️ `display` 走 `_mc_server_display`（配置的显示名），**不是** MC 上报名
    ——上报名只作标识，显示名一律以插件配置为准（既有约定）。
    ⚠️ `now` 目前未使用，见模块 docstring（没有连接时间戳，算不出在线时长）。
    """
    out = []
    for sid, conn in sorted(conns.items()):
        if getattr(conn, "closed", True):
            continue
        out.append({
            "id": str(sid),
            "display": str(display_of(sid)),
            "port": getattr(conn, "port", 0),
            "reported_name": str(getattr(conn, "reported_name", "") or ""),
        })
    return out
