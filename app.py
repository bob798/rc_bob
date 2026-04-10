"""
HTTP 通知系统 — 最小可行实现

启动: python app.py          (默认重试间隔 30s/2m/10m/1h)
演示: FAST_MODE=1 python app.py  (压缩为 2s/4s/6s/8s 方便观察)

架构分层:
  API 接入层  → POST /notifications, GET /notifications/<id>/status
  持久层     → SQLite notifications 表 (生产用 PostgreSQL)
  调度层     → Delivery Worker 1s 轮询
  适配层     → adapters.py (Adapter 模式)
  死信层     → triage_dead_letter 自动分级
"""
import os
import sys
import json
import uuid
import time
import sqlite3
import logging
import threading
from datetime import datetime, timedelta, timezone

import requests as http_client
from flask import Flask, request, jsonify

from adapters import EVENT_REGISTRY, get_adapter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("notify")

# ============================================================
# 1. 数据库
# ============================================================

DB_PATH = "notifications.db"

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def init_db():
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS notifications (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            request_id      TEXT UNIQUE NOT NULL,
            event_type      TEXT NOT NULL,
            biz_id          TEXT NOT NULL,
            provider        TEXT NOT NULL,
            payload         TEXT NOT NULL,            -- JSON string
            callback_url    TEXT,
            status          TEXT NOT NULL DEFAULT 'pending',
            priority        INTEGER DEFAULT 0,
            retry_count     INTEGER DEFAULT 0,
            max_retries     INTEGER DEFAULT 5,
            next_retry_at   TEXT,                     -- ISO 8601
            last_error      TEXT,
            created_at      TEXT DEFAULT (datetime('now')),
            updated_at      TEXT DEFAULT (datetime('now')),
            UNIQUE(event_type, biz_id)
        );
        CREATE INDEX IF NOT EXISTS idx_pending
            ON notifications (provider, status, next_retry_at);
    """)
    conn.close()
    log.info("Database initialized")

# ============================================================
# 2. 状态机
# ============================================================

VALID_TRANSITIONS = {
    "pending":     ["sending"],
    "sending":     ["delivered", "retrying", "dead_letter"],
    "retrying":    ["sending"],
    "dead_letter": ["pending", "failed"],  # pending = 重入队
}

def transition(conn, notif_id: int, from_status: str, to_status: str, **extra):
    if to_status not in VALID_TRANSITIONS.get(from_status, []):
        raise ValueError(f"Invalid transition: {from_status} → {to_status}")
    sets = ["status=?", "updated_at=datetime('now')"]
    vals = [to_status]
    for k, v in extra.items():
        sets.append(f"{k}=?")
        vals.append(v)
    vals.append(notif_id)
    conn.execute(f"UPDATE notifications SET {','.join(sets)} WHERE id=?", vals)
    conn.commit()

# ============================================================
# 3. 重试策略（指数退避）
# ============================================================

FAST_MODE = os.environ.get("FAST_MODE") == "1"
RETRY_DELAYS = [2, 4, 6, 8] if FAST_MODE else [30, 120, 600, 3600]

def next_retry_at(retry_count: int):
    if retry_count >= len(RETRY_DELAYS):
        return None
    dt = datetime.now(timezone.utc) + timedelta(seconds=RETRY_DELAYS[retry_count])
    return dt.strftime("%Y-%m-%d %H:%M:%S")

# ============================================================
# 4. 死信自动分级
# ============================================================

def triage_dead_letter(conn, notif):
    """
    503/502/Timeout → 重入队延迟 30min (FAST_MODE: 10s)
    401/403/404     → 标记 failed
    其他            → 标记 failed + 日志(生产: 创建人工工单)
    """
    error = notif["last_error"] or ""
    status_code = int(error.split(":")[0]) if error and error[0].isdigit() else 0

    if status_code in (502, 503) or "timeout" in error.lower():
        delay = 10 if FAST_MODE else 1800
        retry_at = (datetime.now(timezone.utc) + timedelta(seconds=delay)).strftime("%Y-%m-%d %H:%M:%S")
        conn.execute(
            "UPDATE notifications SET status='pending', next_retry_at=?, retry_count=0, updated_at=datetime('now') WHERE id=?",
            (retry_at, notif["id"]),
        )
        conn.commit()
        log.warning(f"[DEAD_LETTER] requeued (retryable) → {notif['request_id']}")
    elif status_code in (401, 403, 404):
        transition(conn, notif["id"], "dead_letter", "failed", last_error=f"auth/perm error: {error}")
        fire_callback(notif, "failed")
        log.error(f"[DEAD_LETTER] failed (auth) → {notif['request_id']}")
    else:
        transition(conn, notif["id"], "dead_letter", "failed", last_error=f"unknown: {error}")
        fire_callback(notif, "failed")
        # 生产环境: 此处创建人工工单
        log.error(f"[DEAD_LETTER] failed (unknown, needs manual review) → {notif['request_id']}")

# ============================================================
# 5. 简化版回调（生产用独立 Callback Worker）
# ============================================================

def fire_callback(notif, status: str):
    url = notif["callback_url"]
    if not url:
        return
    body = {
        "request_id": notif["request_id"],
        "event_type": notif["event_type"],
        "biz_id": notif["biz_id"],
        "status": status,
        "attempts": notif["retry_count"],
    }
    try:
        http_client.post(url, json=body, timeout=5)
        log.info(f"[CALLBACK] {status} → {notif['request_id']}")
    except Exception as e:
        log.warning(f"[CALLBACK] failed: {e}")

# ============================================================
# 6. Delivery Worker
# ============================================================
#
# 生产环境并发控制 (PostgreSQL):
#   BEGIN;
#   SELECT * FROM notifications
#   WHERE status IN ('pending','retrying')
#     AND (next_retry_at IS NULL OR next_retry_at <= NOW())
#     AND provider = :provider
#   ORDER BY priority DESC, created_at ASC
#   LIMIT 10
#   FOR UPDATE SKIP LOCKED;
#   UPDATE ... SET status='sending';
#   COMMIT;
#
# MVP 简化: 单 Worker 线程, SQLite 无需行锁

def delivery_worker():
    """每 1s 轮询, 取 pending/retrying 任务投递"""
    # 生产环境: per-provider 隔离, 多 Worker 进程
    # 熔断: 同一 provider 连续失败 10 次 → 暂停 5 分钟 → 告警
    while True:
        try:
            conn = get_db()
            now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

            # Claim batch
            rows = conn.execute("""
                SELECT * FROM notifications
                WHERE status IN ('pending', 'retrying')
                  AND (next_retry_at IS NULL OR next_retry_at <= ?)
                ORDER BY priority DESC, created_at ASC
                LIMIT 10
            """, (now,)).fetchall()

            for row in rows:
                notif = dict(row)
                try:
                    deliver_one(conn, notif)
                except Exception as e:
                    log.error(f"[WORKER] unexpected error for {notif['request_id']}: {e}")

            conn.close()
        except Exception as e:
            log.error(f"[WORKER] loop error: {e}")

        time.sleep(1)


def deliver_one(conn, notif: dict):
    """投递单条通知, 处理成功/失败/重试耗尽"""
    from_status = notif["status"]
    transition(conn, notif["id"], from_status, "sending")

    adapter = get_adapter(notif["provider"])
    req = adapter.build_request(notif)

    try:
        resp = http_client.request(**req)
        if adapter.is_success(resp):
            transition(conn, notif["id"], "sending", "delivered")
            fire_callback(notif, "delivered")
            log.info(f"[DELIVERED] {notif['request_id']}")
        else:
            error = adapter.extract_error(resp)
            handle_failure(conn, notif, error)
    except http_client.Timeout:
        handle_failure(conn, notif, "timeout")
    except http_client.ConnectionError:
        handle_failure(conn, notif, "connection_error")


def handle_failure(conn, notif: dict, error: str):
    """失败处理: retrying (退避) 或 dead_letter (分级)"""
    new_count = notif["retry_count"] + 1
    retry_at = next_retry_at(new_count)

    if retry_at:
        transition(conn, notif["id"], "sending", "retrying",
                   retry_count=new_count, next_retry_at=retry_at, last_error=error)
        log.warning(f"[RETRY {new_count}] {notif['request_id']} → next at {retry_at}")
    else:
        transition(conn, notif["id"], "sending", "dead_letter", last_error=error)
        log.error(f"[DEAD_LETTER] retries exhausted → {notif['request_id']}")
        triage_dead_letter(conn, {**notif, "last_error": error})

# ============================================================
# 7. Flask API
# ============================================================

app = Flask(__name__)

@app.route("/notifications", methods=["POST"])
def create_notification():
    data = request.get_json(force=True)

    # 校验必填字段
    for field in ("event_type", "biz_id", "payload"):
        if field not in data:
            return jsonify(error=f"missing field: {field}"), 400

    # 查事件注册表
    event = EVENT_REGISTRY.get(data["event_type"])
    if not event:
        return jsonify(error=f"unknown event_type: {data['event_type']}"), 400

    request_id = "req_" + uuid.uuid4().hex[:12]
    conn = get_db()
    try:
        conn.execute(
            """INSERT INTO notifications
               (request_id, event_type, biz_id, provider, payload, callback_url, priority, max_retries)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                request_id,
                data["event_type"],
                data["biz_id"],
                event["provider"],
                json.dumps(data["payload"]),
                data.get("callback_url"),
                data.get("priority", 0),
                event.get("max_retries", 5),
            ),
        )
        conn.commit()
        log.info(f"[CREATED] {request_id} ({data['event_type']}:{data['biz_id']})")
    except sqlite3.IntegrityError:
        # 幂等: event_type + biz_id 重复 → 返回已有 request_id
        row = conn.execute(
            "SELECT request_id FROM notifications WHERE event_type=? AND biz_id=?",
            (data["event_type"], data["biz_id"]),
        ).fetchone()
        request_id = row["request_id"]
        log.info(f"[IDEMPOTENT] duplicate → {request_id}")
    finally:
        conn.close()

    return jsonify(request_id=request_id), 202


@app.route("/notifications/<request_id>/status")
def get_status(request_id):
    conn = get_db()
    row = conn.execute(
        "SELECT request_id, event_type, biz_id, provider, status, retry_count, next_retry_at, last_error, created_at, updated_at FROM notifications WHERE request_id=?",
        (request_id,),
    ).fetchone()
    conn.close()
    if not row:
        return jsonify(error="not found"), 404
    return jsonify(dict(row))

# ============================================================
# 8. Main
# ============================================================

if __name__ == "__main__":
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    init_db()

    mode = "FAST" if FAST_MODE else "NORMAL"
    log.info(f"Starting notification service ({mode} mode)")
    log.info(f"Retry delays: {RETRY_DELAYS}s")

    worker = threading.Thread(target=delivery_worker, daemon=True)
    worker.start()
    log.info("Delivery worker started (polling every 1s)")

    # 生产环境: gunicorn -w 4 app:app + 独立 worker 进程
    app.run(port=8000, debug=False, use_reloader=False)
