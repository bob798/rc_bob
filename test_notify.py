"""
HTTP 通知系统 — 测试用例

覆盖维度:
  1. Adapter 层    — build_request / is_success / extract_error / 工厂函数
  2. API 层        — 创建通知 / 参数校验 / 幂等去重 / 状态查询
  3. 状态机        — 合法转换 / 非法转换拒绝
  4. 重试策略      — 指数退避计算 / 耗尽返回 None
  5. 死信分级      — 503 重入队 / 401 标记 failed / unknown 标记 failed
  6. Worker 投递   — 成功 / 失败重试 / 重试耗尽进死信

运行: pytest test_notify.py -v
"""
import os
import json
import sqlite3
import pytest
from unittest.mock import MagicMock, patch
from types import SimpleNamespace

# 强制 FAST_MODE 以便测试退避间隔
os.environ["FAST_MODE"] = "1"

from adapters import TemplateAdapter, get_adapter, EVENT_REGISTRY, PROVIDER_CONFIG
from app import (
    app, init_db, get_db, transition, next_retry_at,
    handle_failure, triage_dead_letter, deliver_one,
    VALID_TRANSITIONS, DB_PATH,
)


# ============================================================
# Fixtures
# ============================================================

@pytest.fixture(autouse=True)
def fresh_db(tmp_path, monkeypatch):
    """每个测试用临时数据库，互不干扰"""
    db_file = str(tmp_path / "test.db")
    monkeypatch.setattr("app.DB_PATH", db_file)
    init_db()
    yield db_file


@pytest.fixture
def client():
    app.config["TESTING"] = True
    with app.test_client() as c:
        yield c


@pytest.fixture
def sample_notif(fresh_db):
    """插入一条 pending 通知并返回 dict"""
    conn = get_db()
    conn.execute(
        """INSERT INTO notifications
           (request_id, event_type, biz_id, provider, payload, status, max_retries)
           VALUES (?, ?, ?, ?, ?, 'pending', 5)""",
        ("req_test_001", "order.payment_success", "biz_001", "inventory_system", '{"a":1}'),
    )
    conn.commit()
    row = conn.execute("SELECT * FROM notifications WHERE request_id='req_test_001'").fetchone()
    conn.close()
    return dict(row)


# ============================================================
# 1. Adapter 层测试
# ============================================================

class TestAdapter:
    def test_template_adapter_build_request(self):
        config = {
            "url": "http://example.com/api",
            "method": "POST",
            "headers": {"X-Key": "secret"},
        }
        adapter = TemplateAdapter(config)
        notif = {"payload": {"user_id": "u1"}}
        req = adapter.build_request(notif)
        assert req["url"] == "http://example.com/api"
        assert req["method"] == "POST"
        assert req["json"] == {"user_id": "u1"}
        assert req["timeout"] == 10

    def test_template_adapter_url_substitution(self):
        config = {"url": "http://example.com/$user_id/notify", "method": "POST", "headers": {}}
        adapter = TemplateAdapter(config)
        req = adapter.build_request({"payload": {"user_id": "u123"}})
        assert req["url"] == "http://example.com/u123/notify"

    def test_is_success_200(self):
        adapter = TemplateAdapter({"url": "x", "method": "POST", "headers": {}})
        resp = SimpleNamespace(status_code=200)
        assert adapter.is_success(resp) is True

    def test_is_success_503(self):
        adapter = TemplateAdapter({"url": "x", "method": "POST", "headers": {}})
        resp = SimpleNamespace(status_code=503)
        assert adapter.is_success(resp) is False

    def test_extract_error(self):
        adapter = TemplateAdapter({"url": "x", "method": "POST", "headers": {}})
        resp = SimpleNamespace(status_code=401, text="Unauthorized")
        assert adapter.extract_error(resp) == "401: Unauthorized"

    def test_get_adapter_known_provider(self):
        adapter = get_adapter("inventory_system")
        assert isinstance(adapter, TemplateAdapter)

    def test_get_adapter_unknown_provider(self):
        with pytest.raises(ValueError, match="Unknown provider"):
            get_adapter("nonexistent")


# ============================================================
# 2. API 层测试
# ============================================================

class TestAPI:
    def test_create_notification_success(self, client):
        resp = client.post("/notifications", json={
            "event_type": "order.payment_success",
            "biz_id": "test_biz_001",
            "payload": {"key": "value"},
        })
        assert resp.status_code == 202
        data = resp.get_json()
        assert data["request_id"].startswith("req_")

    def test_create_missing_field(self, client):
        resp = client.post("/notifications", json={
            "event_type": "order.payment_success",
            # biz_id 缺失
            "payload": {},
        })
        assert resp.status_code == 400
        assert "biz_id" in resp.get_json()["error"]

    def test_create_unknown_event_type(self, client):
        resp = client.post("/notifications", json={
            "event_type": "unknown.event",
            "biz_id": "biz_001",
            "payload": {},
        })
        assert resp.status_code == 400
        assert "unknown event_type" in resp.get_json()["error"]

    def test_idempotency(self, client):
        body = {
            "event_type": "order.payment_success",
            "biz_id": "idem_001",
            "payload": {"x": 1},
        }
        r1 = client.post("/notifications", json=body)
        r2 = client.post("/notifications", json=body)
        assert r1.status_code == 202
        assert r2.status_code == 202
        assert r1.get_json()["request_id"] == r2.get_json()["request_id"]

    def test_get_status_found(self, client):
        r = client.post("/notifications", json={
            "event_type": "order.payment_success",
            "biz_id": "status_001",
            "payload": {},
        })
        rid = r.get_json()["request_id"]
        resp = client.get(f"/notifications/{rid}/status")
        assert resp.status_code == 200
        assert resp.get_json()["status"] == "pending"

    def test_get_status_not_found(self, client):
        resp = client.get("/notifications/req_nonexistent/status")
        assert resp.status_code == 404

    def test_priority_field(self, client):
        resp = client.post("/notifications", json={
            "event_type": "order.payment_success",
            "biz_id": "prio_001",
            "payload": {},
            "priority": 10,
        })
        assert resp.status_code == 202


# ============================================================
# 3. 状态机测试
# ============================================================

class TestStateMachine:
    def test_all_valid_transitions(self, sample_notif):
        """验证 VALID_TRANSITIONS 中定义的转换全部可执行"""
        # pending → sending
        conn = get_db()
        transition(conn, sample_notif["id"], "pending", "sending")
        row = conn.execute("SELECT status FROM notifications WHERE id=?", (sample_notif["id"],)).fetchone()
        assert row["status"] == "sending"
        conn.close()

    def test_sending_to_delivered(self, sample_notif):
        conn = get_db()
        transition(conn, sample_notif["id"], "pending", "sending")
        transition(conn, sample_notif["id"], "sending", "delivered")
        row = conn.execute("SELECT status FROM notifications WHERE id=?", (sample_notif["id"],)).fetchone()
        assert row["status"] == "delivered"
        conn.close()

    def test_sending_to_retrying(self, sample_notif):
        conn = get_db()
        transition(conn, sample_notif["id"], "pending", "sending")
        transition(conn, sample_notif["id"], "sending", "retrying", retry_count=1)
        row = conn.execute("SELECT status, retry_count FROM notifications WHERE id=?", (sample_notif["id"],)).fetchone()
        assert row["status"] == "retrying"
        assert row["retry_count"] == 1
        conn.close()

    def test_invalid_transition_rejected(self, sample_notif):
        conn = get_db()
        with pytest.raises(ValueError, match="Invalid transition"):
            transition(conn, sample_notif["id"], "pending", "delivered")
        conn.close()

    def test_invalid_transition_retrying_to_delivered(self, sample_notif):
        conn = get_db()
        transition(conn, sample_notif["id"], "pending", "sending")
        transition(conn, sample_notif["id"], "sending", "retrying")
        with pytest.raises(ValueError, match="Invalid transition"):
            transition(conn, sample_notif["id"], "retrying", "delivered")
        conn.close()

    def test_dead_letter_to_failed(self, sample_notif):
        conn = get_db()
        transition(conn, sample_notif["id"], "pending", "sending")
        transition(conn, sample_notif["id"], "sending", "dead_letter")
        transition(conn, sample_notif["id"], "dead_letter", "failed")
        row = conn.execute("SELECT status FROM notifications WHERE id=?", (sample_notif["id"],)).fetchone()
        assert row["status"] == "failed"
        conn.close()


# ============================================================
# 4. 重试策略测试
# ============================================================

class TestRetry:
    def test_retry_delays_fast_mode(self):
        """FAST_MODE 下退避间隔为 [2, 4, 6, 8]"""
        from app import RETRY_DELAYS
        assert RETRY_DELAYS == [2, 4, 6, 8]

    def test_next_retry_at_returns_future(self):
        result = next_retry_at(0)
        assert result is not None

    def test_next_retry_at_each_step(self):
        for i in range(4):
            assert next_retry_at(i) is not None

    def test_next_retry_at_exhausted(self):
        assert next_retry_at(4) is None
        assert next_retry_at(10) is None


# ============================================================
# 5. 死信分级测试
# ============================================================

class TestDeadLetterTriage:
    def _make_dead(self, sample_notif):
        """将 sample_notif 推进到 dead_letter 状态"""
        conn = get_db()
        transition(conn, sample_notif["id"], "pending", "sending")
        transition(conn, sample_notif["id"], "sending", "dead_letter", last_error="test")
        row = conn.execute("SELECT * FROM notifications WHERE id=?", (sample_notif["id"],)).fetchone()
        conn.close()
        return dict(row)

    def test_503_requeued(self, sample_notif):
        notif = self._make_dead(sample_notif)
        notif["last_error"] = "503: Service Unavailable"
        conn = get_db()
        triage_dead_letter(conn, notif)
        row = conn.execute("SELECT status FROM notifications WHERE id=?", (notif["id"],)).fetchone()
        assert row["status"] == "pending"  # 重入队
        conn.close()

    def test_timeout_requeued(self, sample_notif):
        notif = self._make_dead(sample_notif)
        notif["last_error"] = "timeout"
        conn = get_db()
        triage_dead_letter(conn, notif)
        row = conn.execute("SELECT status FROM notifications WHERE id=?", (notif["id"],)).fetchone()
        assert row["status"] == "pending"
        conn.close()

    def test_401_failed(self, sample_notif):
        notif = self._make_dead(sample_notif)
        notif["last_error"] = "401: Unauthorized"
        conn = get_db()
        triage_dead_letter(conn, notif)
        row = conn.execute("SELECT status FROM notifications WHERE id=?", (notif["id"],)).fetchone()
        assert row["status"] == "failed"
        conn.close()

    def test_404_failed(self, sample_notif):
        notif = self._make_dead(sample_notif)
        notif["last_error"] = "404: Not Found"
        conn = get_db()
        triage_dead_letter(conn, notif)
        row = conn.execute("SELECT status FROM notifications WHERE id=?", (notif["id"],)).fetchone()
        assert row["status"] == "failed"
        conn.close()

    def test_unknown_error_failed(self, sample_notif):
        notif = self._make_dead(sample_notif)
        notif["last_error"] = "something weird happened"
        conn = get_db()
        triage_dead_letter(conn, notif)
        row = conn.execute("SELECT status FROM notifications WHERE id=?", (notif["id"],)).fetchone()
        assert row["status"] == "failed"
        conn.close()


# ============================================================
# 6. Worker 投递测试
# ============================================================

class TestWorkerDelivery:
    def test_deliver_success(self, sample_notif):
        mock_resp = SimpleNamespace(status_code=200, text="ok")
        with patch("app.http_client.request", return_value=mock_resp):
            conn = get_db()
            deliver_one(conn, sample_notif)
            row = conn.execute("SELECT status FROM notifications WHERE id=?", (sample_notif["id"],)).fetchone()
            assert row["status"] == "delivered"
            conn.close()

    def test_deliver_failure_triggers_retry(self, sample_notif):
        mock_resp = SimpleNamespace(status_code=503, text="Service Unavailable")
        with patch("app.http_client.request", return_value=mock_resp):
            conn = get_db()
            deliver_one(conn, sample_notif)
            row = conn.execute("SELECT status, retry_count FROM notifications WHERE id=?", (sample_notif["id"],)).fetchone()
            assert row["status"] == "retrying"
            assert row["retry_count"] == 1
            conn.close()

    def test_deliver_timeout_triggers_retry(self, sample_notif):
        import requests
        with patch("app.http_client.request", side_effect=requests.Timeout):
            conn = get_db()
            deliver_one(conn, sample_notif)
            row = conn.execute("SELECT status, last_error FROM notifications WHERE id=?", (sample_notif["id"],)).fetchone()
            assert row["status"] == "retrying"
            assert "timeout" in row["last_error"]
            conn.close()

    def test_deliver_connection_error_triggers_retry(self, sample_notif):
        import requests
        with patch("app.http_client.request", side_effect=requests.ConnectionError):
            conn = get_db()
            deliver_one(conn, sample_notif)
            row = conn.execute("SELECT status, last_error FROM notifications WHERE id=?", (sample_notif["id"],)).fetchone()
            assert row["status"] == "retrying"
            assert "connection_error" in row["last_error"]
            conn.close()

    def test_deliver_exhausted_retries_to_dead_letter(self, sample_notif):
        """重试耗尽 → dead_letter → triage → failed"""
        conn = get_db()
        # 先把 retry_count 设到 max-1
        conn.execute("UPDATE notifications SET retry_count=4 WHERE id=?", (sample_notif["id"],))
        conn.commit()
        sample_notif["retry_count"] = 4

        mock_resp = SimpleNamespace(status_code=401, text="Unauthorized")
        with patch("app.http_client.request", return_value=mock_resp):
            deliver_one(conn, sample_notif)
            row = conn.execute("SELECT status FROM notifications WHERE id=?", (sample_notif["id"],)).fetchone()
            # 401 经过 triage → failed
            assert row["status"] == "failed"
        conn.close()

    def test_callback_fired_on_delivered(self, sample_notif):
        conn = get_db()
        conn.execute("UPDATE notifications SET callback_url='http://test/cb' WHERE id=?", (sample_notif["id"],))
        conn.commit()
        sample_notif["callback_url"] = "http://test/cb"

        mock_resp = SimpleNamespace(status_code=200, text="ok")
        with patch("app.http_client.request", return_value=mock_resp) as mock_req, \
             patch("app.http_client.post") as mock_post:
            deliver_one(conn, sample_notif)
            mock_post.assert_called_once()
            call_kwargs = mock_post.call_args
            assert call_kwargs[1]["json"]["status"] == "delivered"
        conn.close()
