# -*- coding: utf-8 -*-
"""好感度纯算法与本地存储。

身份空间刻意分成两个互不相关的键空间——游戏内玩家用游戏 ID，QQ 群友用 QQ 号，
同一个人在两边是两份独立好感，不做任何映射。
"""

import math
from pathlib import Path

try:
    from .store import atomic_write_json, read_json
except ImportError:  # 插件以顶层模块方式加载时
    from store import atomic_write_json, read_json

KARMA_MIN = -50
KARMA_MAX = 100
# 好感度记录软上限，超出时淘汰（已无时间戳可排序，见 evict_oldest）
KARMA_MAX_RECORDS = 500


def clamp_value(v: object) -> int:
    """好感值裁剪到 [KARMA_MIN, KARMA_MAX]。

    任何输入都不得抛异常——本函数在插件初始化路径（KarmaStore.read → merge_records）
    上被调用，异常会直接崩掉插件加载。

    非数字归 0。溢出按方向夹到边界：JSON 里的 1e400 会被解析成 inf，而
    int(inf) 抛的是 OverflowError（不是 ValueError，不会被降级捕获），
    所以这里显式分流。inf 视作"极大"夹到 KARMA_MAX，与超大整数 10**400 的处理
    保持一致；-inf 夹到 KARMA_MIN。nan 无法判断方向，且 min/max 对 nan 的比较
    恒为 False（会让 nan 意外变成 KARMA_MAX），故显式归 0。
    """
    try:
        n = int(v)
    except (TypeError, ValueError, OverflowError):
        try:
            f = float(v)
        except (TypeError, ValueError, OverflowError):
            return 0
        if math.isnan(f):
            return 0
        if math.isinf(f):
            return KARMA_MAX if f > 0 else KARMA_MIN
        n = int(f)
    return max(KARMA_MIN, min(KARMA_MAX, n))


def clamp_cost(v: object, cap: int) -> int:
    """把 AI 报价裁剪成合法的消耗值。

    非数字、负数一律归零；超过 cap 的裁剪到 cap。任何输入都不得抛异常——
    报价来自 LLM，是外部输入（Task 7 调用点）。
    溢出处理同 clamp_value：inf 视作"极大"夹到 max(0, cap)（cap 为负是错误配置，
    结果只能是 0，绝不能是负数），-inf/nan 归 0。
    布尔值在 Python 里是 int 的子类（True == 1），Acceptable。
    """
    if isinstance(v, bool):
        # 同样要过 max(0, min(cap, ...))：cap=0/-5 时 True 不能给出 1
        return max(0, min(cap, 1 if v else 0))
    try:
        n = int(v)
    except (TypeError, ValueError, OverflowError):
        try:
            f = float(v)
        except (TypeError, ValueError, OverflowError):
            return 0
        if math.isnan(f):
            return 0
        if math.isinf(f):
            # max(0, cap)：负的 cap 是错误配置，不能让 inf 报价变成负数
            # （Task 7 用 -spend，负消耗会变成好感增加）。与末尾 max(0, ...) 同口径。
            return max(0, cap) if f > 0 else 0
        n = int(f)
    return max(0, min(cap, n))


def identity_key(source: str, initiator: str, qq: str) -> str:
    """构造好感度身份键。

    source="qq" 时用 QQ 号，否则用游戏 ID（initiator）。
    """
    if source == "qq":
        return f"qq:{qq}"
    return f"mc:{initiator}"


def merge_records(file_records: dict, config_records: dict) -> dict:
    """合并文件与配置里的好感记录。

    **配置优先**：`config_records`（WebUI 里 `karma_records` 的值）是管理员手改入口，
    优先级与可信度都最高，同键一律覆盖文件里的旧值。文件只在配置没有该键时兜底
    （插件每次改动都写回配置，正常情况下两边一致；文件里多出来的条目＝配置被清过）。

    记录形态是「键 -> 好感值」，值为纯数字。非数字条目（含 bool、字符串、None、
    缺失）按损坏丢弃，让 get() 回退到 initial——与历史上非 dict 条目的口径一致。
    早期版本是 {"value": N, "updated": T}，此处兼容读取旧形态的 value。
    """
    merged: dict = {}

    def _extract(rec):
        """从条目里取出好感值；旧形态（dict）取 value，新形态直接用。"""
        if isinstance(rec, dict):
            rec = rec.get("value")
        if isinstance(rec, bool) or not isinstance(rec, (int, float)):
            return None
        try:
            return clamp_value(rec)
        except (TypeError, ValueError, OverflowError):
            return None

    # 先放文件（兜底），再用配置覆盖（管理员手改优先）
    for src, override in ((file_records, False), (config_records, True)):
        if not isinstance(src, dict):
            continue
        for key, rec in src.items():
            value = _extract(rec)
            if value is None:
                continue
            if not override and key in merged:
                continue
            merged[key] = value
    return merged


def evict_oldest(records: dict, limit: int) -> tuple:
    """超过 limit 条时淘汰超额部分，返回 (保留, 被淘汰的键列表)。

    记录里已不再存时间戳（配置项只显示「QQ 号: 好感值」），因此无法按新旧排序。
    改为按**值降序**保留——好感高的留下，避免管理员手改过的条目被默默清掉；
    值是 int 之间的稳定比较，无平局歧义（Python 的 sorted 稳定，同值保持插入序）。
    """
    if len(records) <= limit:
        return records, []
    ordered = sorted(records.items(), key=lambda kv: kv[1], reverse=True)
    dropped = [k for k, _ in ordered[limit:]]
    return {k: v for k, v in records.items() if k not in set(dropped)}, dropped


class KarmaStore:
    """好感度记录的内存视图 + 落盘。path 为 None 时纯内存（测试用）。

    线程/协程安全的串行化由调用方（插件）用 asyncio.Lock 保证；
    本类只负责数据正确性与文件原子写。
    """

    def __init__(self, records: dict, path=None):
        self._records = records
        self._path = Path(path) if path else None
        # 上次 add 淘汰掉的键（供调用方记 warning）；无淘汰时为空列表
        self.last_dropped: list = []

    @classmethod
    def read(cls, path) -> "KarmaStore":
        """从文件加载；文件不存在或损坏时返回空表。

        加载时经 merge_records 归一化：手改过的文件可能有非 dict 条目或越界值，
        不归一化会让 snapshot() 抛 TypeError（插件初始化路径没有 try/except）。
        """
        return cls(merge_records(read_json(Path(path), {}), {}), path)

    def get(self, key: str, initial: int) -> int:
        """读取好感值；无记录返回 initial。不写盘。

        兼容旧形态（{"value": N}）：读到 dict 时取 value，避免升级时把
        存量记录当成损坏条目丢掉。
        """
        rec = self._records.get(key)
        if isinstance(rec, dict):
            rec = rec.get("value")
        if isinstance(rec, bool) or not isinstance(rec, (int, float)):
            return initial
        try:
            return clamp_value(rec)
        except (TypeError, ValueError, OverflowError):
            return initial

    def add(self, key: str, delta: int, initial: int) -> tuple:
        """增减好感并落盘，返回 (旧值, 新值)。delta=0 时只读不写。"""
        old = self.get(key, initial)
        if not delta:
            return old, old
        new = clamp_value(old + int(delta))
        self._records[key] = new
        self._records, dropped = evict_oldest(self._records, KARMA_MAX_RECORDS)
        self.last_dropped = dropped
        self._save()
        return old, new

    def snapshot(self) -> dict:
        """返回记录的副本，供写回配置项与落盘。

        形态是「键 -> 好感值」（纯数字），与 `_save` 写进磁盘的形状一致——
        管理员在 WebUI 看到的 `karma_records` 与 `karma.json` 内容完全相同，
        只显示 QQ 号（或游戏 ID）与好感值，不带时间戳。

        不做过滤：非数字条目早已被 merge_records 挡在门外，内存里只有合法值。
        本方法在任何输入下都不抛异常（插件初始化路径会调用它，没有 try/except）。
        """
        return dict(self._records)

    def _save(self) -> None:
        if self._path is None:
            return
        atomic_write_json(self._path, self._records)
