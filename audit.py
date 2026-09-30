# -*- coding: utf-8 -*-
"""指令审计的结构化落盘。

此前审计**只往日志写一行文本**（`log_command_audit`），后果是出事时只能在日志里
肉眼翻、没法按玩家筛选、日志还会轮转。本模块把它落成结构化记录，供管理面板查询。

**为什么是 JSONL**：审计是追加语义。JSON 数组每加一条要重写整个文件；
JSONL 直接 append，只在超上限时才重写一次。

⚠️ 与 `store.py` 的分工：那边是「一个 JSON 对象」，这里是「每行一个对象」，
读写方式不同，所以不复用（复用会让两边都变形）。
"""

import json
import os
from pathlib import Path

# 保留最近多少条。超过就丢最旧的——审计是"最近发生了什么"，不是归档。
AUDIT_MAX_ENTRIES = 1000


def audit_path(data_dir: Path) -> Path:
    """审计文件的位置：`<插件数据目录>/netherlink/audit.jsonl`。

    ⚠️ 与好感度文件（`netherlink/karma.json`）同目录，便于整体备份。
    ⚠️ `data_dir` 是**插件数据根**（即 `get_astrbot_plugin_data_path()` 的返回值，
       形如 `.../plugin_data`），**不是** `netherlink/` 那一层——本函数会自己
       拼上 `netherlink/`。传错会得到 `plugin_data/netherlink/netherlink/audit.jsonl`。
    """
    return Path(data_dir) / "netherlink" / "audit.jsonl"


def append_audit(path: Path, record: dict, limit: int = AUDIT_MAX_ENTRIES) -> None:
    """追加一条审计记录；超过 limit 时裁剪到最近 limit 条。

    ⚠️ 本函数**不抛异常**的保证由调用方负责——审计是旁路功能，
    写不进去也绝不能连累指令执行本身（调用点用 try/except 包住）。
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")
    _trim(path, limit)


def _trim(path: Path, limit: int) -> None:
    """行数超过 limit 时，只保留最近的 limit 条（原子替换）。"""
    try:
        with open(path, encoding="utf-8") as f:
            lines = [ln for ln in f.read().splitlines() if ln.strip()]
    except OSError:
        return
    if len(lines) <= limit:
        return
    keep = lines[-limit:]
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        f.write("\n".join(keep) + "\n")
    os.replace(tmp, path)


def read_audit(path: Path, limit: int = 100) -> list:
    """读最近 limit 条，**新的在前**。

    损坏的行（崩溃留下的半行）被跳过，而不是让整份读不出来。
    """
    path = Path(path)
    if not path.exists():
        return []
    try:
        with open(path, encoding="utf-8") as f:
            lines = f.read().splitlines()
    except OSError:
        return []
    out = []
    for line in reversed(lines):
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
        except ValueError:
            continue
        if isinstance(rec, dict):
            out.append(rec)
        if len(out) >= limit:
            break
    return out
