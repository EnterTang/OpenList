#!/usr/bin/env python3
"""Clean duplicate subscription transfer/move tasks on a running OpenList instance."""

from __future__ import annotations

import json
import re
import sqlite3
import sys
import urllib.error
import urllib.request

BASE_URL = "http://127.0.0.1:25044"
ADMIN_USER = "admin"
ADMIN_PASSWORD = "Twain1996!"
DB_PATH = "/tmp/openlist_cleanup.db"
DEFAULT_TRANSFER_PRIORITY = ["pan123", "pan115", "quark", "aliyun_drive"]


def provider_rank(text: str) -> int:
    text = (text or "").lower()
    if "/123/" in text or text.startswith("123"):
        return 0
    if "/115/" in text or "115cdn" in text or "115.com" in text:
        return 1
    if "/quark/" in text or "quark" in text:
        return 2
    if "/ali/" in text or "alipan" in text or "aliyun" in text:
        return 3
    return 99


def episode_key(text: str) -> tuple[int, int] | None:
    match = re.search(r"S(\d+)E(\d+)", text, re.I)
    if match:
        return int(match.group(1)), int(match.group(2))
    return None


def api_request(path: str, token: str | None = None, data=None, method: str = "GET"):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = token
    body = None if data is None else json.dumps(data).encode()
    request = urllib.request.Request(f"{BASE_URL}{path}", data=body, headers=headers, method=method)
    with urllib.request.urlopen(request, timeout=60) as response:
        return json.load(response)


def login() -> str:
    payload = api_request(
        "/api/auth/login",
        data={"username": ADMIN_USER, "password": ADMIN_PASSWORD},
        method="POST",
    )
    return payload["data"]["token"]


def parse_move_task(name: str) -> dict | None:
    match = re.match(
        r"move \[/([^]]+)\]\(([^)]+)\) to \[/([^]]+)\]\(([^)]+)\)",
        name or "",
    )
    if not match:
        return None
    src_storage, src_path, _dst_storage, dst_path = match.groups()
    episode = episode_key(f"{src_path} {dst_path}")
    return {
        "src_storage": src_storage,
        "src_path": src_path,
        "dst_path": dst_path,
        "episode": episode,
    }


def task_keep_score(task: dict) -> tuple:
    parsed = parse_move_task(task.get("name", "")) or {}
    src_path = parsed.get("src_path", task.get("name", ""))
    name = task.get("name", "").lower()
    quality = 2 if "mytvsuper" in name else (1 if "myvideo" in name else 0)
    return (
        provider_rank(src_path),
        -quality,
        -int(task.get("state", 0) == 1),
        -task.get("total_bytes", 0),
    )


def cleanup_move_tasks(token: str) -> int:
    tasks = api_request("/api/task/move/undone", token=token).get("data", [])
    groups: dict[tuple, list[dict]] = {}
    for task in tasks:
        parsed = parse_move_task(task.get("name", ""))
        if parsed and parsed["episode"]:
            season, episode = parsed["episode"]
            key = (season, episode, parsed["dst_path"].rsplit("/", 1)[0])
        else:
            key = ("misc", task.get("id"))
        groups.setdefault(key, []).append(task)

    cancel_ids: list[str] = []
    keep_count = 0
    for group in groups.values():
        if len(group) == 1:
            keep_count += 1
            continue
        group.sort(key=task_keep_score)
        keep_count += 1
        cancel_ids.extend(task["id"] for task in group[1:])

    if cancel_ids:
        api_request("/api/task/move/cancel_some", token=token, data=cancel_ids, method="POST")
    print(f"move tasks: total={len(tasks)} keep={keep_count} cancel={len(cancel_ids)}")
    return len(cancel_ids)


def item_keep_score(row: sqlite3.Row) -> tuple:
    name = (row["file_name"] or "").lower()
    quality = 2 if "mytvsuper" in name else (1 if "myvideo" in name else 0)
    return (
        provider_rank(row["source_path"] or ""),
        -quality,
        -int(row["file_size"] or 0),
    )


def cleanup_subscription_items() -> tuple[int, int]:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute(
        """
        SELECT id, subscription_id, season, episode, target_path, source_path, file_name, file_size, status
        FROM x_subscription_items
        WHERE status IN ('transferring', 'pending')
        ORDER BY subscription_id, season, episode, file_size DESC
        """
    )
    rows = cur.fetchall()
    groups: dict[tuple, list[sqlite3.Row]] = {}
    for row in rows:
        key = (row["subscription_id"], row["season"], row["episode"])
        groups.setdefault(key, []).append(row)

    skip_ids: list[int] = []
    pending_ids: list[int] = []
    for group in groups.values():
        if len(group) == 1:
            if group[0]["status"] == "transferring":
                pending_ids.append(group[0]["id"])
            continue
        group.sort(key=item_keep_score)
        pending_ids.append(group[0]["id"])
        skip_ids.extend(row["id"] for row in group[1:])

    if skip_ids:
        cur.executemany(
            "UPDATE x_subscription_items SET status='skipped', last_error=? WHERE id=?",
            [("cleanup: duplicate episode superseded by higher-priority source", item_id) for item_id in skip_ids],
        )
    if pending_ids:
        cur.executemany(
            "UPDATE x_subscription_items SET status='pending', last_error='' WHERE id=?",
            [(item_id,) for item_id in pending_ids],
        )

    cur.execute(
        "UPDATE x_subscription_items SET status='pending', last_error='' WHERE status='transferring'"
    )

    cur.execute("SELECT id, source_config FROM x_subscriptions WHERE source_type='telegram'")
    for sub_id, raw in cur.fetchall():
        cfg = json.loads(raw or "{}")
        if cfg.get("transfer_priority"):
            continue
        cfg["transfer_priority"] = ["123", "115", "quark", "aliyun"]
        cur.execute(
            "UPDATE x_subscriptions SET source_config=? WHERE id=?",
            (json.dumps(cfg, ensure_ascii=False), sub_id),
        )

    cur.execute("SELECT value FROM x_setting_items WHERE key='subscription_config'")
    row = cur.fetchone()
    if row:
        cfg = json.loads(row[0] or "{}")
        telegram = cfg.get("telegram") or {}
        if not telegram.get("transfer_priority"):
            telegram["transfer_priority"] = DEFAULT_TRANSFER_PRIORITY
            cfg["telegram"] = telegram
            cur.execute(
                "UPDATE x_setting_items SET value=? WHERE key='subscription_config'",
                (json.dumps(cfg, ensure_ascii=False),),
            )

    conn.commit()
    conn.close()
    print(f"subscription items: skipped={len(skip_ids)} reset_pending={len(pending_ids)}")
    return len(skip_ids), len(pending_ids)


def main() -> int:
    db_only = "--db-only" in sys.argv
    if not db_only:
        try:
            token = login()
        except urllib.error.URLError as exc:
            print(f"login failed: {exc}", file=sys.stderr)
            return 1
        cleanup_move_tasks(token)
    cleanup_subscription_items()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
