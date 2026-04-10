"""
端到端演示脚本

用法:
  终端 1: FAST_MODE=1 python app.py
  终端 2: python demo.py

演示场景:
  1. 成功投递:     pending → sending → delivered
  2. 重试后成功:   pending → sending → retrying → ... → delivered
  3. 死信(401):    pending → sending → retrying(多次) → dead_letter → failed
  4. 幂等去重:     重复提交返回相同 request_id
"""
import time
import threading

import requests
from flask import Flask, request as flask_request

# ============================================================
# Mock 外部供应商服务器 (port 9999)
# ============================================================

mock_app = Flask("mock_provider")
mock_app.logger.disabled = True

import logging as _logging
_logging.getLogger("werkzeug").setLevel(_logging.ERROR)

fail_counters = {}

@mock_app.route("/webhook", methods=["POST"])
def webhook():
    fail_mode = flask_request.args.get("fail")
    call_id = flask_request.args.get("id", "default")

    if fail_mode == "always":
        return {"error": "Unauthorized"}, 401

    if fail_mode and fail_mode.isdigit():
        n = int(fail_mode)
        fail_counters.setdefault(call_id, 0)
        fail_counters[call_id] += 1
        if fail_counters[call_id] <= n:
            return {"error": "Service Unavailable"}, 503

    return {"status": "ok"}, 200


def start_mock_server():
    mock_app.run(port=9999, debug=False, use_reloader=False)

# ============================================================
# 工具函数
# ============================================================

API = "http://localhost:8000"

def send(event_type, biz_id, payload=None, label=""):
    resp = requests.post(f"{API}/notifications", json={
        "event_type": event_type,
        "biz_id": biz_id,
        "payload": payload or {},
    })
    data = resp.json()
    print(f"\n{'='*60}")
    print(f"  [{label}] POST /notifications → {resp.status_code}")
    print(f"  request_id: {data.get('request_id')}")
    return data.get("request_id")


def poll_status(request_id, label="", max_wait=60):
    """轮询状态变化直到终态"""
    seen = set()
    start = time.time()
    last_status = None
    while time.time() - start < max_wait:
        resp = requests.get(f"{API}/notifications/{request_id}/status")
        info = resp.json()
        status = info["status"]
        retry = info.get("retry_count", 0)
        key = f"{status}:{retry}"
        if key not in seen:
            seen.add(key)
            err = (info.get("last_error") or "")[:60]
            extra = f"  error={err}" if err else ""
            print(f"  [{label}] status={status:<12} retries={retry}{extra}")
            last_status = status
        if status in ("delivered", "failed"):
            print(f"  [{label}] ✓ DONE")
            return status
        time.sleep(0.5)
    print(f"  [{label}] ⏰ timeout ({max_wait}s), last={last_status}")
    return last_status

# ============================================================
# 演示场景
# ============================================================

def run_demo():
    time.sleep(1)

    # --- 场景 1: 成功投递 ---
    print("\n▶ 场景 1: 正常投递 (直接成功)")
    rid1 = send("order.payment_success", "order_001",
                payload={"order_id": "001", "amount": 99.9},
                label="SUCCESS")
    poll_status(rid1, "SUCCESS")

    # --- 场景 2: 重试后成功 (前2次503, 第3次200) ---
    print("\n▶ 场景 2: 重试后成功 (供应商前2次返回503)")
    rid2 = send("order.demo_retry", "order_002",
                payload={"order_id": "002"},
                label="RETRY")
    poll_status(rid2, "RETRY", max_wait=90)

    # --- 场景 3: 死信 (始终401 → 重试耗尽 → 自动分级为 failed) ---
    print("\n▶ 场景 3: 死信分级 (供应商始终返回401)")
    rid3 = send("order.demo_dead", "order_003",
                payload={"order_id": "003"},
                label="DEAD")
    poll_status(rid3, "DEAD", max_wait=120)

    # --- 场景 4: 幂等去重 ---
    print("\n▶ 场景 4: 幂等去重 (重复提交相同 event_type + biz_id)")
    rid4 = send("order.payment_success", "order_001",
                payload={"order_id": "001", "amount": 99.9},
                label="IDEMPOTENT")
    if rid4 == rid1:
        print(f"  [IDEMPOTENT] ✓ 返回相同 request_id: {rid4}")
    else:
        print(f"  [IDEMPOTENT] ✗ 期望 {rid1}, 得到 {rid4}")

    print(f"\n{'='*60}")
    print("  Demo complete!")
    print(f"{'='*60}")


# ============================================================
# Main
# ============================================================

if __name__ == "__main__":
    print("Starting mock provider on :9999 ...")
    t = threading.Thread(target=start_mock_server, daemon=True)
    t.start()

    print("Running demo against :8000 ...\n")
    try:
        run_demo()
    except requests.ConnectionError:
        print("\n  ERROR: Cannot connect to :8000")
        print("  Please run: FAST_MODE=1 python app.py")
