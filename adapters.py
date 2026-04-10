"""
供应商适配层 — Adapter 模式

职责：屏蔽不同外部供应商的 HTTP 协议差异。
新增供应商只需：1) 加 PROVIDER_CONFIG  2) 如有特殊逻辑则写 CustomAdapter
"""
from abc import ABC, abstractmethod
from string import Template

# ============================================================
# Adapter 抽象接口
# ============================================================

class NotificationAdapter(ABC):
    @abstractmethod
    def build_request(self, notification: dict) -> dict:
        """构建 requests 库所需参数: {url, method, headers, json, timeout}"""

    @abstractmethod
    def is_success(self, response) -> bool:
        """供应商级成功判定（有些 200 但 body 里是失败）"""

    @abstractmethod
    def extract_error(self, response) -> str:
        """提取错误信息，用于日志和死信分级"""


# ============================================================
# TemplateAdapter — 配置驱动，覆盖大部分简单 POST 场景
# ============================================================

class TemplateAdapter(NotificationAdapter):
    def __init__(self, config: dict):
        self.config = config

    def build_request(self, notification: dict) -> dict:
        payload = notification.get("payload", {})
        url = Template(self.config["url"]).safe_substitute(payload)
        headers = {
            k: Template(v).safe_substitute(payload)
            for k, v in self.config.get("headers", {}).items()
        }
        return dict(
            url=url,
            method=self.config.get("method", "POST"),
            headers=headers,
            json=payload,
            timeout=10,
        )

    def is_success(self, response) -> bool:
        return 200 <= response.status_code < 300

    def extract_error(self, response) -> str:
        return f"{response.status_code}: {response.text[:200]}"


# ============================================================
# 注册表 — event_type → provider → adapter
# ============================================================

PROVIDER_CONFIG = {
    "inventory_system": {
        "adapter_type": "template",
        "url": "http://localhost:9999/webhook",
        "method": "POST",
        "headers": {"Content-Type": "application/json"},
    },
    # 以下为 demo 演示用 provider，模拟不同故障场景
    "demo_flaky": {
        "adapter_type": "template",
        "url": "http://localhost:9999/webhook?fail=2&id=flaky",
        "method": "POST",
        "headers": {"Content-Type": "application/json"},
    },
    "demo_dead": {
        "adapter_type": "template",
        "url": "http://localhost:9999/webhook?fail=always",
        "method": "POST",
        "headers": {"Content-Type": "application/json"},
    },
}

EVENT_REGISTRY = {
    "order.payment_success": {
        "provider": "inventory_system",
        "max_retries": 5,
    },
    # demo 用事件类型
    "order.demo_retry": {
        "provider": "demo_flaky",
        "max_retries": 5,
    },
    "order.demo_dead": {
        "provider": "demo_dead",
        "max_retries": 5,
    },
}


def get_adapter(provider: str) -> NotificationAdapter:
    config = PROVIDER_CONFIG.get(provider)
    if not config:
        raise ValueError(f"Unknown provider: {provider}")
    if config["adapter_type"] == "template":
        return TemplateAdapter(config)
    # custom adapter 扩展点:
    #   adapter_registry = {"CRMAdapter": CRMAdapter, ...}
    #   return adapter_registry[config["adapter_class"]]()
    raise ValueError(f"Unknown adapter_type: {config['adapter_type']}")
