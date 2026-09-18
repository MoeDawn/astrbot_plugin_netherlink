# -*- coding: utf-8 -*-
"""原子 JSON 文件读写。

好感度存本地文件后，写入必须原子化——正常写入过程中进程被杀时，不能留下半截
JSON，否则下次启动会读到损坏文件而丢掉全部好感记录。

注意：本模块不调用 os.fsync，因此**不保证断电场景下的持久性**（重命名可能先于
数据块落盘）。进程被杀是覆盖的，断电不是。好感度是软约束，用户已确认接受该取舍。
"""

import json
import os
from pathlib import Path


def atomic_write_json(path: Path, payload: dict) -> None:
    """先写临时文件再原子替换，避免写入中断损坏目标文件。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)
    os.replace(tmp, path)


def read_json(path: Path, default: dict) -> dict:
    """读取 JSON 对象；文件缺失、损坏或不是对象时返回 default。

    捕获 ValueError 同时覆盖 json.JSONDecodeError（内容不是合法 JSON）与
    UnicodeDecodeError（文件不是合法 UTF-8，如被杀毒/加密驱动破坏），
    两者都是 ValueError 子类。
    """
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else default
    except (OSError, ValueError):
        return default
