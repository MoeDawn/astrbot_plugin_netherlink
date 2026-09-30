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
    """行数超过 limit 时，只保留最近的 limit 条（原子替换）。

    ⚠️ `limit` 至少按 1 处理：`limit <= 0` 时 `lines[-limit:]` 会退化成
    整个列表（`-0` 就是 `0`），为负则变成 `lines[k:]` 只留尾部——实测
    6 条记录被裁成一行空白。上限是运维可填的值，必须防住。
    ⚠️ 捕获 `ValueError` 是为了 `UnicodeDecodeError`（它**不是** `OSError`）。
    文件里只要有**一个**解不出来的字节，整次 `read()` 都会抛——不接住
    就等于「崩过一次之后，此后每次 append 都在裁剪这步炸掉」，
    被调用方吞掉后审计**永久停记**。
    ⚠️ 这里**刻意不用** `errors="replace"`（`read_audit` 用了）：本函数会把
    内容**写回磁盘**，而替换字符一旦落盘就不可逆。读不干净时宁可不裁剪
    （返回，文件保持原样），也不拿解不出来的字节去重写整份文件。
    代价是：坏字节存在期间不再裁剪，文件会越过上限增长——那是可恢复的
    占用问题，不是数据损坏。
    """
    if limit < 1:
        limit = 1
    try:
        with open(path, encoding="utf-8") as f:
            lines = [ln for ln in f.read().splitlines() if ln.strip()]
    except (OSError, ValueError):
        return
    if len(lines) <= limit:
        return
    keep = lines[-limit:]
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        f.write("\n".join(keep) + "\n")
    os.replace(tmp, path)


def read_audit(path: Path, limit: int = 100) -> list:
    """读最近 limit 条，**新的在前**。`limit <= 0` 一律返回空列表。

    损坏的行被跳过，而不是让整份读不出来。损坏有**两种**，都要接住：
    ① 崩溃留下的半行——那通常**不是**非法 JSON，而是**非法 UTF-8**：
       本模块用 `ensure_ascii=False` 落盘，中文玩家名是裸多字节序列，
       崩在半路会留下解不出来的字节尾；
    ② 内容不是合法 JSON。

    ⚠️ 光把异常接住**不够**：`read()` 是**整文件一次性解码**的，只要有一个
    坏字节，整次解码就失败——`except` 接住它只是把「抛异常」换成「返回空
    列表」，文件照样一条都读不出来（面板上表现为审计莫名其妙是空的，更糟）。
    所以这里必须用 `errors="replace"`：坏字节解成 U+FFFD，**逐行**解析照常
    进行，那条坏行随即被下面的 `json.loads` 判为非法并跳过，好行一条不少。
    本函数是**只读**的，替换字符不会写回磁盘，对文件零风险。
    ⚠️ `UnicodeDecodeError` 是 `ValueError` 的子类、**不是** `OSError`。
    有了 `errors="replace"` 它已不可能出现，但 except 仍按
    `(OSError, ValueError)` 写——与 `store.py` 的 `read_json` 同口径
    （纵深防御，并覆盖其它 `ValueError`）。
    """
    path = Path(path)
    if limit <= 0:
        return []
    if not path.exists():
        return []
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            lines = f.read().splitlines()
    except (OSError, ValueError):
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
