# -*- coding: utf-8 -*-
"""指令审计的结构化落盘。

此前审计**只往日志写一行文本**（`log_command_audit`），后果是出事时只能在日志里
肉眼翻、没法按玩家筛选、日志还会轮转。本模块把它落成结构化记录，供管理面板查询。

**为什么是 JSONL**：审计是追加语义。JSON 数组每加一条要重写整个文件；
JSONL 直接 append，只在超上限时才重写一次。

⚠️ 与 `store.py` 的分工：那边是「一个 JSON 对象」，这里是「每行一个对象」，
读写方式不同，所以不复用（复用会让两边都变形）。
⚠️ 但**粒度差异是有后果的**：`store.py` 的「读不出来就整体回退默认值」
对本格式是**错的**——一行坏掉不该毁掉整份。两个函数的容错策略因此各不相同，
详见 `read_audit` 与 `_trim` 的 docstring。
⚠️ 另一个必须两边一致的约定是**「一行」的判据**：`_trim` 按 `b"\\n"` 切、
`read_audit` 按 `"\\n"` 切。判据不同会**静默丢记录**——见 `read_audit` 的说明。
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

    ⚠️ 全程**按字节**处理，不解码。两条理由缺一不可：
    ① **不能让坏字节关掉裁剪**：文本模式 `read()` 是**整文件一次性解码**，
       一个坏字节就抛 `UnicodeDecodeError`；把它接住（或捕 `ValueError`）
       都等于**永久跳过裁剪**——实测 limit=5 时 60 次 append 留下 **61 行**，
       文件无界增长，且没有任何自愈路径。按字节切分完全不受影响。
    ② **不能让替换字符落盘**：`errors="replace"` 会把坏字节解成 U+FFFD，
       而本函数要把内容**写回磁盘**，替换字符一旦落盘就不可逆——审计日志
       的价值就在保真，不能拿它换裁剪生效。按字节处理则坏字节原样带过。
    对本格式这是**精确**的：`0x0A` 不可能出现在 UTF-8 多字节序列内部，
    而 `json.dumps` 会把记录里的换行转义成 `\\n`，所以一条记录绝不跨行。
    ⚠️ 落盘是 **CRLF**：`append_audit` 走文本模式，Windows 上 `\\n` 会被转成
    `\\r\\n`（实测 `b'{"event": "request"}\\r\\n'`），所以被保留的行本来就以
    `\\r` 结尾。按 `b"\\n"` 切、**原样写回**——`\\r` 必须保留，不能顺手规范化
    掉（那会改写幸存行，对 git 也是内容变更）。
    ⚠️ **这里按 `b"\\n"` 切，`read_audit` 必须按 `"\\n"` 切**——见后者的说明。
    """
    if limit < 1:
        limit = 1
    try:
        raw = path.read_bytes()
    except OSError:
        return
    lines = [ln for ln in raw.split(b"\n") if ln.strip()]
    if len(lines) <= limit:
        return
    keep = lines[-limit:]
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "wb") as f:
        f.write(b"\n".join(keep) + b"\n")
    os.replace(tmp, path)


def read_audit(path: Path, limit: int = 100) -> list:
    """读最近 limit 条，**新的在前**。`limit <= 0` 一律返回空列表。

    损坏的行被跳过，而不是让整份读不出来。损坏有**两种**：
    ① 崩溃留下的半行——那通常**不是**非法 JSON，而是**非法 UTF-8**：
       本模块用 `ensure_ascii=False` 落盘，中文玩家名是裸多字节序列，
       崩在半路会留下解不出来的字节尾；
    ② 内容不是合法 JSON（由下面的逐行 `json.loads` 判掉）。

    ⚠️ ① 靠 **`errors="replace"` 预防**，**不是**靠捕获异常：
    `read()` 是**整文件一次性解码**的，只要有一个坏字节，整次解码就失败
    ——把它 `except` 接住只是把「抛异常」换成「返回空列表」，文件照样一条
    都读不出来（面板上表现为审计莫名其妙是空的，比抛异常更糟）。
    `errors="replace"` 让坏字节解成 U+FFFD、**逐行**解析照常进行，那条坏行
    随即被 `json.loads` 判为非法并跳过，**好行一条不少**。
    本函数是**只读**的，替换字符不会写回磁盘，对文件零风险。
    ⚠️ `except` 因此**只捕 `OSError`**（文件不存在、路径是目录等）。
    刻意**不写** `ValueError`：解码失败已被预防掉、不可能再发生，
    把不可能发生的异常接住只会让将来的回归变成「面板静默变空」
    ——正是本条要消灭的症状。

    ⚠️ 按 `"\\n"` 切行，**不用 `splitlines()`**：两者**不是**同一个判据
    ——`splitlines()` 还会在 U+0085(NEL) / U+2028(LS) / U+2029(PS) 上断开，
    而 `json.dumps(ensure_ascii=False)` **不转义**这三个字符（实测它们原样
    落盘）。于是含它们的记录被**切成两段**、两段都不是合法 JSON → 被跳过
    → **静默丢一条记录**，而 `_trim` 却按一条数它（两边判据就此分叉）。
    （VT / FF / FS 不受影响：`json.dumps` 会转义它们，到不了文件里。）
    切完每段可能带首尾空白，由循环里的 `line.strip()` 去掉。
    """
    path = Path(path)
    if limit <= 0:
        return []
    if not path.exists():
        return []
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            lines = f.read().split("\n")
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
