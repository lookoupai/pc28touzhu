# 领域模型草案

## 1. 建模原则

不要延续旧项目“固定几列预测字段”的设计。

新项目必须围绕：

- 来源
- 原始内容
- 标准信号
- 订阅关系
- 执行任务
- 执行回执

进行建模。

## 2. 核心实体

### 2.1 users

平台用户。

建议字段：

- `id`
- `username`
- `email`
- `password_hash`
- `role`
- `status`
- `created_at`
- `updated_at`

### 2.2 signal_sources

定义信号来源。

建议字段：

- `id`
- `owner_user_id`
- `source_type`
- `name`
- `status`
- `visibility`
- `config_json`
- `created_at`
- `updated_at`

说明：

- `source_type` 可取 `internal_ai`、`public_predictor`、`website_feed`、`telegram_channel` 等
- `config_json` 保存来源接入参数

### 2.3 source_raw_items

保存来源原始记录，用于追溯。

建议字段：

- `id`
- `source_id`
- `external_item_id`
- `issue_no`
- `published_at`
- `raw_payload`
- `parse_status`
- `parse_error`
- `created_at`

### 2.4 normalized_signals

平台内部标准信号。

建议字段：

- `id`
- `source_id`
- `source_raw_item_id`
- `lottery_type`
- `issue_no`
- `bet_type`
- `bet_value`
- `confidence`
- `normalized_payload`
- `status`
- `published_at`
- `created_at`

说明：

- 一个原始记录可以解析出多个标准信号
- `normalized_payload` 应允许扩展更多玩法参数

### 2.5 user_subscriptions

定义用户订阅了哪些来源。

建议字段：

- `id`
- `user_id`
- `source_id`
- `status`
- `strategy_json`
- `created_at`
- `updated_at`

补充说明：

- `strategy_json` 不应只保存单一金额
- 推荐兼容字段：`mode`、`stake_amount`、`base_stake`、`multiplier`、`max_steps`、`refund_action`、`cap_action`

### 2.6 delivery_targets

定义用户的执行目标，例如 Telegram 群组。

建议字段：

- `id`
- `user_id`
- `executor_type`
- `target_key`
- `target_name`
- `template_id`
- `status`
- `created_at`
- `updated_at`

### 2.7 message_templates

定义投注文案模板。

建议字段：

- `id`
- `user_id`
- `lottery_type`
- `bet_type`
- `template_text`
- `status`
- `created_at`
- `updated_at`

### 2.8 execution_jobs

平台下发给执行器的任务。

建议字段：

- `id`
- `user_id`
- `signal_id`
- `delivery_target_id`
- `executor_type`
- `idempotency_key`
- `planned_message_text`
- `stake_plan_json`
- `execute_after`
- `expire_at`
- `status`
- `error_message`
- `created_at`
- `updated_at`

补充说明：

- `stake_plan_json` 至少保留本次实际执行金额 `amount`
- 同时建议保留完整策略快照，便于后续接入倍投或更复杂的资金管理逻辑

### 2.9 execution_attempts

执行尝试日志。

建议字段：

- `id`
- `job_id`
- `executor_instance_id`
- `attempt_no`
- `delivery_status`
- `remote_message_id`
- `raw_result`
- `error_message`
- `executed_at`

## 3. 建模注意事项

### 3.1 不要把玩法写死成固定列

PC28 后续可能支持：

- 大小
- 单双
- 组合
- 极值
- 豹子
- 对子
- 顺子
- ABC
- 边/中

因此：

- `bet_type` 负责描述玩法类别
- `bet_value` 负责描述玩法值
- 复杂扩展信息进入 `normalized_payload`

### 3.2 原始内容与标准信号必须分开

不能只保留解析结果，否则以后很难：

- 修解析器
- 回放历史数据
- 解释为什么生成这个投注信号

### 3.3 执行任务与执行尝试必须分开

否则无法清晰处理：

- 重试
- 幂等
- 失败原因
- 手动补发
