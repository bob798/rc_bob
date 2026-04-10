# HTTP 通知系统 — Claude Code 上下文

## 项目背景

企业内部多个业务系统在关键事件发生时，需要调用外部系统供应商提供的 HTTP(S) API 进行通知。本项目设计并实现一个内部通知投递服务。

## 已完成的设计决策（经过多轮辩证讨论确认）

### 核心定位

- 对投递结果**负责到底**的内部服务，不是纯技术中间件
- 投递失败由通知系统团队协调第三方，业务方无需补偿逻辑
- V1 **只依赖数据库**，无 Redis、无 MQ、无额外中间件

### 投递语义

- At-Least-Once + 幂等键（event_type + biz_id 联合唯一）
- 不做顺序保证、不做鉴权限流（V1）

### 架构分层

| 层 | 组件 | 职责 |
|---|---|---|
| 接入层 | API Gateway | 参数校验 · event_type 路由 · 同步写 DB · 返回 202 |
| 持久层 | notifications 表 | 状态机 · 事务写入 · SKIP LOCKED |
| 调度层 | Delivery Worker | per-provider 隔离 · 1s 轮询 · 指数退避 · 熔断 |
| 适配层 | Provider Adapter | template \| custom 路由 · build_request · is_success · extract_error |
| 死信层 | Dead Letter Engine | 自动分级：503 重入队 / 401 标记失败 / unknown 人工 |
| 回调层 | Callback Worker | 独立通道 · 终态回调业务方 · 轻量重试 2 次 |
| 可观测层 | 查询接口 + 死信看板 | GET /notifications/{id}/status · 批量重发 |

### 通讯协议

#### 请求（业务方 → 通知系统）

```json
POST /notifications

{
  "event_type":    "order.payment_success",       // 必填，路由依据
  "biz_id":        "order_20260410_001",           // 必填，幂等去重
  "payload":       {},                             // 必填，透传给 Adapter
  "callback_url":  "http://order-svc/callback",    // 可选
  "priority":      0                               // 可选，默认0
}

→ 202 { "request_id": "req_abc123" }
```

#### 事件注册表（event_type → provider 映射）

```yaml
event_registry:
  order.payment_success:
    provider: inventory_system
    adapter_type: template
    description: "支付成功通知库存扣减"
    owner: 订单团队
    max_retries: 5

  ad.user_registered:
    provider: ad_platform
    adapter_type: custom
    adapter_class: AdPlatformAdapter
    description: "用户注册通知广告归因"
    owner: 广告团队
```

命名规范：`{业务域}.{动作_过去式}`，小写 + 下划线 + 点号分隔。

#### 回调（通知系统 → 业务方）

```json
POST {callback_url}
X-Notify-Signature: hmac-sha256(body, secret)

{
  "request_id":   "req_abc123",
  "event_type":   "order.payment_success",
  "biz_id":       "order_20260410_001",
  "status":       "delivered | handling | failed",
  "attempts":     3,
  "finished_at":  "2026-04-10T15:30:00Z",
  "error":        null | "504 Gateway Timeout"
}
```

终态：delivered（成功）、handling（死信跟进中）、failed（永久失败）。

### 数据模型

```sql
CREATE TABLE notifications (
    id              BIGSERIAL PRIMARY KEY,
    request_id      VARCHAR(64) UNIQUE NOT NULL,
    event_type      VARCHAR(128) NOT NULL,
    biz_id          VARCHAR(128) NOT NULL,
    provider        VARCHAR(64) NOT NULL,
    payload         JSONB NOT NULL,
    callback_url    VARCHAR(512),
    status          VARCHAR(20) NOT NULL DEFAULT 'pending',
    priority        INT DEFAULT 0,
    retry_count     INT DEFAULT 0,
    max_retries     INT DEFAULT 5,
    next_retry_at   TIMESTAMP,
    last_error      TEXT,
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW(),
    UNIQUE(event_type, biz_id)
);

CREATE INDEX idx_pending ON notifications (provider, status, next_retry_at)
    WHERE status IN ('pending', 'retrying');
```

状态机：`pending → sending → delivered | retrying → dead_letter → handling → delivered | failed`

### 并发控制

- 多 Worker 用 `SELECT ... FOR UPDATE SKIP LOCKED` 避免重复消费
- Worker 按 provider 分组，per-provider 隔离
- 熔断：同一 provider 连续失败 10 次 → 暂停 5 分钟 → 告警

### 重试策略（指数退避）

```
第1次: 30s → 第2次: 2min → 第3次: 10min → 第4次: 1h → 第5次: 放弃进死信
```

### 死信自动分级

| HTTP 响应 | 处理 |
|---|---|
| 503/502/Timeout | 自动重入队，延迟 30min 再试 |
| 401/403 | 标记 failed · 告警运营更新密钥 |
| 404 | 直接 failed · 回调业务方 |
| 其他/响应异常 | 人工工单 |

### Adapter 路由

- `adapter_type: template` → 纯模板变量填充，TemplateAdapter(config)
- `adapter_type: custom` → 代码实现，adapter_registry[adapter_class]()
- 接口：build_request(payload)、is_success(response)、extract_error(response)

### 明确不做的（AI 建议取舍）

| 不采纳 | 理由 |
|---|---|
| Kafka/MQ | V1 流量不需要，DB 够用 |
| Redis 信号层 | 1s 轮询已满足，少一个组件少一种故障 |
| 事件溯源 | 通知是发完即忘 |
| 业务方 Outbox | 复杂度不应推给业务方 |
| V1 定 SLA | 没有运行数据支撑的 SLA 是拍脑袋 |

### 演进触发条件

| 信号 | 动作 |
|---|---|
| pending > 10,000 | 检查供应商故障 |
| 轮询 P99 > 3s | 加 Worker |
| 表 > 500 万行 | 归档历史 |
| QPS > 500/s | 迁移 MQ |
| DB CPU > 70% 持续 15min | 扩容或迁移 |

## 待实现

以上设计已确认，接下来进入代码实现阶段。
