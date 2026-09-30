# -*- coding: utf-8 -*-
"""QQ ↔ 游戏账号的绑定表。

一个键 = 一个**游戏 ID**（MC 端上报的规范拼写），值含绑定的 QQ 号与一份
**绑定当时**的群昵称快照（不实时更新——每条群消息都去刷新会平白多一次写盘）。

⚠️ 与 `karma.json` / `audit.jsonl` 同目录（`netherlink/`），便于整体备份。

⚠️ **命名**：本模块刻意用 `load` / `save` / `upsert` / `lookup` / `record_for`，
不用 `_load_bindings` / `_save_bindings` 这类名字——项目的契约测试
（`test_no_removed_identifiers_remain`）把这些标识符钉为「已删除、不得复活」。
"""

from pathlib import Path

try:
    from .store import atomic_write_json, read_json
except ImportError:  # 插件以顶层模块方式加载时
    from store import atomic_write_json, read_json


def bindings_path(data_dir: Path) -> Path:
    """绑定表的位置：`<插件数据根>/netherlink/bindings.json`。

    ⚠️ `data_dir` 是**插件数据根**（`get_astrbot_plugin_data_path()` 的返回值，
    形如 `.../plugin_data`），不是 `netherlink/` 那一层——本函数自己拼。
    """
    return Path(data_dir) / "netherlink" / "bindings.json"


def load(path: Path) -> dict:
    """读绑定表；文件缺失/损坏返回空表。

    混进来的坏条目（非对象、缺 `qq`）逐条丢弃，不让整份读不出来。
    """
    raw = read_json(Path(path), {})
    out = {}
    for player, rec in raw.items():
        if not isinstance(rec, dict):
            continue
        if not str(rec.get("qq") or "").strip():
            continue
        servers = rec.get("servers")
        out[str(player)] = {
            "qq": str(rec["qq"]).strip(),
            "qq_name": str(rec.get("qq_name") or ""),
            "servers": [str(s) for s in servers] if isinstance(servers, list) else [],
            "bound_at": str(rec.get("bound_at") or ""),
            "method": str(rec.get("method") or ""),
        }
    return out


def save(path: Path, table: dict) -> None:
    """原子写绑定表（先写 .tmp 再 os.replace）。"""
    atomic_write_json(Path(path), table)


def lookup(table: dict, player: str) -> str:
    """玩家名 → QQ 号；未绑定返回空串。"""
    rec = table.get(player)
    if not isinstance(rec, dict):
        return ""
    return str(rec.get("qq") or "").strip()


def record_for(table: dict, player: str) -> dict:
    """玩家名的完整记录（**与表脱钩**的副本）；未绑定返回空 dict。

    ⚠️ `servers` 是 list，而 `dict(rec)` 只拷顶层——浅拷贝会让调用方经由返回值的
    列表就地改到表内数据。这里显式重建 `servers`，使返回值与表互不影响。
    """
    rec = table.get(player)
    if not isinstance(rec, dict):
        return {}
    out = dict(rec)
    out["servers"] = list(rec.get("servers") or [])
    return out


def upsert(table: dict, player: str, qq: str, qq_name: str, server_id: str,
           method: str, now: str) -> dict:
    """写入/覆盖一条绑定，返回**新表**（不改入参）。

    重绑（同一个游戏 ID 换 QQ）＝ 覆盖 `qq`/`qq_name`/`bound_at`/`method`；
    `servers` 取**并集**——只在见到新的服务器时追加，重复进服不产生重复项。
    """
    out = dict(table)
    prev = out.get(player) if isinstance(out.get(player), dict) else {}
    servers = list(prev.get("servers") or [])
    sid = str(server_id or "").strip()
    if sid and sid not in servers:
        servers.append(sid)
    out[str(player)] = {
        "qq": str(qq).strip(),
        "qq_name": str(qq_name or ""),
        "servers": servers,
        "bound_at": str(now),
        "method": str(method),
    }
    return out


def note_server(table: dict, player: str, server_id: str) -> tuple:
    """记下「这个**已绑定**玩家在某台服上出现过」。返回 `(新表, 是否变了)`。

    ⚠️ 只对已绑定的玩家记账——未绑定者没有记录，`servers` 无处可存
    （等他绑定时 C3 会把当时那台服写进去）。
    ⚠️ 返回 `changed` 让调用方**只在真变了时才落盘**：进服是高频事件，
    每次都写盘既慢又没意义（规范 §3.1：只在见到「新的」服务器时才追加）。
    ⚠️ 未命中任何条件时**原样返回入参对象**（不是副本）——调用方据此跳过写盘。
    """
    rec = table.get(player)
    if not isinstance(rec, dict) or not str(rec.get("qq") or "").strip():
        return table, False
    sid = str(server_id or "").strip()
    if not sid:
        return table, False
    servers = list(rec.get("servers") or [])
    if sid in servers:
        return table, False
    servers.append(sid)
    out = dict(table)
    new_rec = dict(rec)
    new_rec["servers"] = servers
    out[player] = new_rec
    return out, True
