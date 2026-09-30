# -*- coding: utf-8 -*-
"""好感度纯算法与本地存储。

身份空间有两个入口：`identity_key` 是纯函数（游戏侧恒用游戏 ID）；
`karma_identity_key` 会查绑定表——**已绑定的玩家改用其 QQ 号**，于是同一个人在
游戏内与群里共享一份好感（2026-09-30 第二轮起）。未绑定者仍用 `mc:<游戏ID>`。
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


def clamp_value(v: object, lo: int = KARMA_MIN, hi: int = KARMA_MAX) -> int:
    """好感值裁剪到 [lo, hi]（默认 KARMA_MIN ~ KARMA_MAX）。

    ⚠️ 范围 2026-09-23 起**可配**（`karma_min` / `karma_max`），所以不再硬编码——
    此前用户在 `karma_rules` 提示词里把范围改大，代码却照样裁到 -50~100，
    而且是**静默的**（不报错、不告警）。现在提示词里的数字由插件按配置渲染
    （占位符 `{karma_min}` / `{karma_max}`），两者不会再脱节。

    `lo > hi` 这类错误配置由调用方归一，本函数只保证「不抛异常」——
    真的传入倒置区间时，max(lo, min(hi, n)) 会返回 lo，不会崩。
    任何输入都不得抛异常——本函数在插件初始化路径（merge_records 归一化配置项）上
    被调用。⚠️ 该加载块**有** try/except（两条 `logger.error` + 降级分支），
    所以异常不会崩掉插件加载；代价是**降级为空表**（`_karma_degraded`，
    记录全丢）。正因如此，本函数仍然一个异常都不该抛。

    非数字归 0。溢出按方向夹到边界：JSON 里的 1e400 会被解析成 inf，而
    int(inf) 抛的是 OverflowError（不是 ValueError，不会被降级捕获），
    所以这里显式分流。inf 视作"极大"夹到 hi，与超大整数 10**400 的处理
    保持一致；-inf 夹到 lo。nan 无法判断方向，且 min/max 对 nan 的比较
    恒为 False（会让 nan 意外变成 hi），故显式归 0。
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
            return hi if f > 0 else lo
        n = int(f)
    return max(lo, min(hi, n))


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


def karma_identity_key(source: str, initiator: str, qq: str, table: dict) -> str:
    """好感身份键——**需要查绑定表**的入口。

    与 `identity_key` 的区别只有一个：**游戏侧发起者若已绑定，键改成 `qq:<QQ号>`**。
    于是同一个人在游戏内与 QQ 群里是**同一个键 → 一份好感**（规范 §4.1）。

    - `source == "qq"`：本来就用 QQ 号，`table` 不参与（QQ 侧无需换算）
    - `source == "game"`：`initiator` 在 `table` 里且 `qq` 非空 → `qq:<QQ>`；
      否则退回 `mc:<游戏ID>`（未绑定者的兜底，否则他没有任何记录可落）

    ⚠️ `table` 传空 dict 时本函数**等价于** `identity_key`——旧的
    `test_exec_identity_key_follows_source` 因此仍应通过（无绑定 → 键不变）。
    """
    if source == "qq":
        return identity_key(source, initiator, qq)
    rec = table.get(initiator) if isinstance(table, dict) else None
    bound = str(rec.get("qq") or "") if isinstance(rec, dict) else ""
    if bound:
        return "qq:%s" % bound
    return identity_key(source, initiator, qq)


def merge_records(file_records: dict, config_records: dict,
                  lo: int = KARMA_MIN, hi: int = KARMA_MAX) -> dict:
    """合并/归一化好感记录表。

    **两种用法**（2026-09-30 起）：

    1. **归一化单表**（插件启动路径用这个）：传空 `file_records`，
       即 `merge_records({}, config_records, lo, hi)`。此时它就是「把配置项
       过一遍筛子」——丢损坏条目、拆旧形态、夹范围，并**返回一个全新的 dict**。
       最后这点很关键：`_parse_config_records()` 在配置项已是 dict 时返回的是
       AstrBot 那个对象本身，直接交给 `KarmaStore` 会让一次 `_karma_add`
       绕开 `save_config` 就地改内存配置。
    2. **并集**（`KarmaStore.read` 用，读镜像 / 从镜像恢复）：配置优先，
       文件只在配置没有该键时兜底。⚠️ **当前没有任何调用点两侧都非空**——
       `KarmaStore.read` 给的是空配置表（`merge_records(file, {})`），插件启动给的是
       空文件表，所以「配置覆盖文件」这半边**只是契约**，实装于第二轮「从镜像恢复」
       （单测 `test_merge_records_config_overrides_file_and_unions` 直接钉住它）。
       ⚠️ **插件启动时不再做这个并集**——那正是「从配置项删一条会被文件带回来」
       的成因。文件现在只作镜像。

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
            return clamp_value(rec, lo, hi)
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

    def __init__(self, records: dict, path=None, lo: int = KARMA_MIN, hi: int = KARMA_MAX):
        self._records = records
        self._path = Path(path) if path else None
        # 好感范围由配置决定（karma_min / karma_max），store 只持有不解释
        self._lo = lo
        self._hi = hi
        # 上次 add 淘汰掉的键（供调用方记 warning）；无淘汰时为空列表
        self.last_dropped: list = []

    @classmethod
    def read(cls, path, lo: int = KARMA_MIN, hi: int = KARMA_MAX) -> "KarmaStore":
        """从文件加载；文件不存在或损坏时返回空表。

        加载时经 merge_records 归一化：手改过的文件可能有非 dict 条目或越界值，
        不归一化会让 snapshot() 抛 TypeError。

        ⚠️ 2026-09-30 起**插件不再从这条路径加载**（启动只读 `karma_records`
        配置项），当前只有单测在调用它。它保留是给第二轮「从镜像恢复」用的
        （设计规范 §4.4）。
        """
        return cls(merge_records(read_json(Path(path), {}), {}), path, lo, hi)

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
            return clamp_value(rec, self._lo, self._hi)
        except (TypeError, ValueError, OverflowError):
            return initial

    def add(self, key: str, delta: int, initial: int) -> tuple:
        """增减好感并落盘，返回 (旧值, 新值)。delta=0 时只读不写。"""
        old = self.get(key, initial)
        if not delta:
            return old, old
        new = clamp_value(old + int(delta), self._lo, self._hi)
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
        本方法在任何输入下都不抛异常——2026-09-30 起插件初始化路径**不再**调用它，
        唯一的生产调用点 `_sync_karma_to_config` 把它包在 try/except 里。
        """
        return dict(self._records)

    def _save(self) -> None:
        if self._path is None:
            return
        atomic_write_json(self._path, self._records)
