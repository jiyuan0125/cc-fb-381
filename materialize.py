#!/usr/bin/env python3
"""按波次增量刷新 SQLite 中的运单在途指标表。

口径见 notes.txt。用法:
    python materialize.py                    # 处理 waves/ 下全部 csv，按文件名顺序
    python materialize.py waves/2026091508.csv
    python materialize.py waves --db metrics.db --log run_log.txt
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import re
import sqlite3
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

TIME_FMT = "%Y-%m-%d %H:%M:%S"
REQUIRED_COLS = ("运单号", "时间", "节点", "城市")
NODE_RE = re.compile(r"^(?:[PTDR]\d{2}|S)$")
STATUS_ORDER = ("已揽收", "运输中", "派送中", "退回中", "退回重派中", "已签收")

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    waybill    TEXT NOT NULL,
    event_time TEXT NOT NULL,
    node       TEXT NOT NULL,
    city       TEXT NOT NULL,
    PRIMARY KEY (waybill, event_time, node, city)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS metrics (
    waybill         TEXT PRIMARY KEY,
    origin_time     TEXT NOT NULL,   -- 起点时间
    origin_node     TEXT NOT NULL,   -- 起点节点码；非 P 表示按临时口径起算
    last_node       TEXT NOT NULL,   -- 当前最后节点
    last_city       TEXT NOT NULL,
    last_event_time TEXT NOT NULL,
    node_count      INTEGER NOT NULL,-- 途经节点数
    elapsed_hours   REAL NOT NULL,   -- 已用时效
    status          TEXT NOT NULL,
    return_count    INTEGER NOT NULL,
    updated_wave    TEXT NOT NULL,
    updated_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS wave_runs (
    wave_id          TEXT NOT NULL,
    content_hash     TEXT NOT NULL,
    processed_at     TEXT NOT NULL,
    rows_parsed      INTEGER NOT NULL,
    new_events       INTEGER NOT NULL,
    dup_events       INTEGER NOT NULL,
    updated_waybills INTEGER NOT NULL,
    status_dist      TEXT NOT NULL,
    PRIMARY KEY (wave_id, content_hash)
);
"""


def now_str() -> str:
    return datetime.now().strftime(TIME_FMT)


def parse_time(s: str) -> datetime:
    return datetime.strptime(s, TIME_FMT)


def read_wave(path: Path) -> tuple[list[tuple[str, str, str, str]], list[tuple[int, str]]]:
    """读取一波 csv，返回 (规范事件行, [(行号, 原因)])。"""
    rows: list[tuple[str, str, str, str]] = []
    bad: list[tuple[int, str]] = []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None or any(c not in reader.fieldnames for c in REQUIRED_COLS):
            raise ValueError(f"{path}: 表头必须包含 {REQUIRED_COLS}，实际为 {reader.fieldnames}")
        for lineno, rec in enumerate(reader, start=2):
            wb = (rec.get("运单号") or "").strip()
            ts = (rec.get("时间") or "").strip()
            node = (rec.get("节点") or "").strip().upper()
            city = (rec.get("城市") or "").strip()
            if not wb or not node or not city:
                bad.append((lineno, "存在空字段"))
                continue
            try:
                dt = parse_time(ts)
            except ValueError:
                bad.append((lineno, f"时间格式非法: {ts!r}"))
                continue
            if not NODE_RE.match(node):
                bad.append((lineno, f"节点码非法: {node!r}"))
                continue
            rows.append((wb, dt.strftime(TIME_FMT), node, city))
    return rows, bad


def dedupe_in_file(rows: list[tuple[str, str, str, str]]) -> list[tuple[str, str, str, str]]:
    seen: set[tuple[str, str, str, str]] = set()
    out = []
    for r in rows:
        if r in seen:
            continue
        seen.add(r)
        out.append(r)
    return out


def fetch_events(conn: sqlite3.Connection, waybills) -> dict[str, list[tuple]]:
    grouped: dict[str, list[tuple]] = defaultdict(list)
    wl = list(waybills)
    for i in range(0, len(wl), 500):
        chunk = wl[i : i + 500]
        marks = ",".join("?" * len(chunk))
        sql = (
            f"SELECT waybill, event_time, node, city FROM events "
            f"WHERE waybill IN ({marks}) ORDER BY waybill, event_time, node, city"
        )
        for wb, ts, node, city in conn.execute(sql, chunk):
            grouped[wb].append((ts, node, city))
    return grouped


def derive_metrics(events: list[tuple[str, str, str]]) -> dict:
    """events 已按 (时间, 节点, 城市) 升序，且已去重。"""
    pickup = [e for e in events if e[1].startswith("P")]
    origin = min(pickup, key=lambda e: e[0]) if pickup else events[0]
    last = events[-1]
    return_count = sum(1 for e in events if e[1].startswith("R"))
    prefix = last[1][0]
    if prefix == "S":
        status = "已签收"
    elif prefix == "R":
        status = "退回中"
    elif return_count > 0:
        status = "退回重派中"
    else:
        status = {"P": "已揽收", "T": "运输中", "D": "派送中"}[prefix]
    elapsed = (parse_time(last[0]) - parse_time(origin[0])).total_seconds() / 3600.0
    return {
        "origin_time": origin[0],
        "origin_node": origin[1],
        "last_node": last[1],
        "last_city": last[2],
        "last_event_time": last[0],
        "node_count": len(events),
        "elapsed_hours": round(elapsed, 2),
        "status": status,
        "return_count": return_count,
    }


def refresh_metrics(
    conn: sqlite3.Connection, waybills: set[str], wave_id: str, updated_at: str
) -> Counter:
    grouped = fetch_events(conn, sorted(waybills))
    dist: Counter = Counter()
    for wb in sorted(waybills):
        events = grouped.get(wb)
        if not events:
            continue
        m = derive_metrics(events)
        conn.execute(
            """
            INSERT INTO metrics (waybill, origin_time, origin_node, last_node, last_city,
                                 last_event_time, node_count, elapsed_hours, status,
                                 return_count, updated_wave, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(waybill) DO UPDATE SET
                origin_time=excluded.origin_time,
                origin_node=excluded.origin_node,
                last_node=excluded.last_node,
                last_city=excluded.last_city,
                last_event_time=excluded.last_event_time,
                node_count=excluded.node_count,
                elapsed_hours=excluded.elapsed_hours,
                status=excluded.status,
                return_count=excluded.return_count,
                updated_wave=excluded.updated_wave,
                updated_at=excluded.updated_at
            """,
            (
                wb,
                m["origin_time"],
                m["origin_node"],
                m["last_node"],
                m["last_city"],
                m["last_event_time"],
                m["node_count"],
                m["elapsed_hours"],
                m["status"],
                m["return_count"],
                wave_id,
                updated_at,
            ),
        )
        dist[m["status"]] += 1
    return dist


def format_dist(dist: Counter) -> str:
    if not dist:
        return "-"
    items = [(s, dist[s]) for s in STATUS_ORDER if dist[s]]
    items += [(s, n) for s, n in dist.items() if s not in STATUS_ORDER]
    return " ".join(f"{s}={n}" for s, n in items)


def append_log(log_path: Path, line: str) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    new_file = not log_path.exists()
    with log_path.open("a", encoding="utf-8") as f:
        if new_file:
            f.write("# 运单指标波次刷新日志\n")
        f.write(line + "\n")


def process_wave(conn: sqlite3.Connection, path: Path, log_path: Path) -> None:
    wave_id = path.stem
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    stamp = now_str()

    already = conn.execute(
        "SELECT 1 FROM wave_runs WHERE wave_id=? AND content_hash=?", (wave_id, digest)
    ).fetchone()
    if already:
        line = (
            f"{stamp} | wave={wave_id} | file={path} | skipped: 该波次内容已处理过"
            f" | updated_waybills=0 | status: -"
        )
        append_log(log_path, line)
        print(line)
        return

    rows, bad = read_wave(path)
    unique = dedupe_in_file(rows)
    waybills_in_file = {r[0] for r in unique}

    existing: set[tuple[str, str, str, str]] = set()
    wl = sorted(waybills_in_file)
    for i in range(0, len(wl), 500):
        chunk = wl[i : i + 500]
        marks = ",".join("?" * len(chunk))
        sql = (
            f"SELECT waybill, event_time, node, city FROM events WHERE waybill IN ({marks})"
        )
        existing.update(conn.execute(sql, chunk).fetchall())

    new_rows = [r for r in unique if r not in existing]
    affected = {r[0] for r in new_rows}

    with conn:  # 单波次一个事务
        if new_rows:
            conn.executemany(
                "INSERT INTO events (waybill, event_time, node, city) VALUES (?,?,?,?)",
                new_rows,
            )
        dist = refresh_metrics(conn, affected, wave_id, stamp) if affected else Counter()
        dist_text = format_dist(dist)
        conn.execute(
            """
            INSERT INTO wave_runs (wave_id, content_hash, processed_at, rows_parsed,
                                   new_events, dup_events, updated_waybills, status_dist)
            VALUES (?,?,?,?,?,?,?,?)
            """,
            (
                wave_id,
                digest,
                stamp,
                len(rows),
                len(new_rows),
                len(rows) - len(new_rows),
                len(affected),
                dist_text,
            ),
        )

    line = (
        f"{stamp} | wave={wave_id} | file={path} | rows={len(rows)}"
        f" | new_events={len(new_rows)} | dup_events={len(rows) - len(new_rows)}"
        f" | updated_waybills={len(affected)} | status: {dist_text}"
    )
    if bad:
        line += f" | bad_rows={len(bad)}"
    append_log(log_path, line)
    if bad:
        detail = "; ".join(f"line {ln}: {why}" for ln, why in bad[:5])
        append_log(log_path, f"    ! 跳过非法行: {detail}")
        print(f"warning: {path} 跳过 {len(bad)} 行非法数据", file=sys.stderr)
    print(line)


def discover(paths: list[str]) -> list[Path]:
    files: list[Path] = []
    seen: set[Path] = set()
    for raw in paths:
        p = Path(raw)
        if p.is_dir():
            candidates = sorted(x for x in p.iterdir() if x.is_file() and x.suffix.lower() == ".csv")
        elif p.is_file():
            candidates = [p]
        else:
            raise SystemExit(f"路径不存在: {p}")
        for c in candidates:
            rp = c.resolve()
            if rp not in seen:
                seen.add(rp)
                files.append(c)
    return files


def main() -> int:
    ap = argparse.ArgumentParser(description="按波次增量刷新运单在途指标 (SQLite)")
    ap.add_argument("paths", nargs="*", default=["waves"], help="波次 csv 文件或目录，默认 waves/")
    ap.add_argument("--db", default="metrics.db", help="SQLite 数据库路径，默认 metrics.db")
    ap.add_argument("--log", default="run_log.txt", help="运行日志路径，默认 run_log.txt")
    args = ap.parse_args()

    files = discover(args.paths)
    if not files:
        print("没有找到待处理的 csv 文件", file=sys.stderr)
        return 1

    db_path = Path(args.db)
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(SCHEMA)
        for f in files:
            process_wave(conn, f, Path(args.log))
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
