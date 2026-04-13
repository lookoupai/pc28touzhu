# 平台与执行器接口契约草案

## 1. 原则

执行器只消费“可执行任务”，不直接访问复杂来源模型。

平台负责：

- 来源接入
- 标准化
- 策略决策
- 下发执行任务

执行器负责：

- 拉取待执行任务
- 实际发言
- 回传执行结果

## 2. 拉取待执行任务

建议接口：

`GET /api/executor/jobs/pull`

请求头建议：

- `Authorization: Bearer <token>`
- `X-Executor-Id: <instance-id>`

建议返回字段：

```json
{
  "items": [
    {
      "job_id": "job_001",
      "signal_id": "sig_001",
      "lottery_type": "pc28",
      "issue_no": "20260407001",
      "bet_type": "big_small",
      "bet_value": "大",
      "message_text": "大10",
      "stake_plan": {
        "mode": "flat",
        "amount": 10,
        "base_stake": 10,
        "multiplier": 2,
        "max_steps": 6,
        "refund_action": "hold",
        "cap_action": "reset"
      },
      "target": {
        "type": "telegram_group",
        "key": "-1001234567890"
      },
      "idempotency_key": "exec-user1-20260407001-big",
      "execute_after": "2026-04-07T15:00:00Z",
      "expire_at": "2026-04-07T15:01:00Z"
    }
  ]
}
```

## 3. 上报执行结果

建议接口：

`POST /api/executor/jobs/<job_id>/report`

建议请求体：

```json
{
  "executor_id": "executor-node-01",
  "attempt_no": 1,
  "delivery_status": "delivered",
  "remote_message_id": "12345",
  "executed_at": "2026-04-07T15:00:08Z",
  "raw_result": {
    "chat_id": "-1001234567890"
  },
  "error_message": null
}
```

`delivery_status` 建议枚举：

- `delivered`
- `failed`
- `expired`
- `skipped`

## 4. 执行器心跳

建议接口：

`POST /api/executor/heartbeat`

目的：

- 标记执行器在线状态
- 上报版本和节点信息
- 支持托管与自托管实例管理

## 5. 幂等要求

平台下发任务时必须提供稳定的 `idempotency_key`。

执行器必须做到：

- 同一 `idempotency_key` 不重复发送
- 重启后仍可识别已投递任务
- 回报时附带相同任务标识

## 6. 到期控制

所有执行任务都应包含：

- `execute_after`
- `expire_at`

执行器必须在本地判断：

- 未到时间不发送
- 过期后不发送
