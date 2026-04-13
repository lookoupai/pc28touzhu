# AITradingSimulator -> pc28touzhu 信号协议草案

## 1. 目标

本文档定义 `AITradingSimulator` 向 `pc28touzhu` 输出预测信号时的推荐协议。

设计目标：

- `AITradingSimulator` 专注预测，不承担执行逻辑
- `pc28touzhu` 专注信号接入、订阅、派发与 Telegram 执行
- 协议同时兼容：
  - 直接执行
  - AI 二次分析
  - 后续新增来源

因此信号协议分为两层：

- 执行视图 `execution-view`
- 分析视图 `analysis-view`

## 2. 总体原则

### 2.1 两层输出，不混用

执行视图是稳定协议，给 `pc28touzhu` 直接消费。  
分析视图是增强协议，给 AI、风控、融合策略或人工审查使用。

不要把执行器直接建立在分析视图上，否则：

- 字段容易漂移
- 结构过重
- 执行层被来源细节污染

### 2.2 JSON 必须稳定

不允许把自然语言段落作为唯一机器输入。  
信号必须始终提供结构化 JSON 字段。

### 2.3 来源与执行解耦

`AITradingSimulator` 输出的是预测结果，不是执行任务。  
执行任务仍由 `pc28touzhu` 根据用户订阅、账号、群组和风控规则生成。

## 3. 执行视图

### 3.1 用途

执行视图用于：

- 创建 `source_raw_items`
- 标准化为 `normalized_signals`
- 进入 `dispatch`
- 派发为 `execution_jobs`

### 3.2 推荐字段

```json
{
  "schema_version": "1.0",
  "signal_id": "pc28-predictor-12-20260408001-big_small",
  "source_type": "ai_trading_simulator",
  "source_ref": {
    "platform": "AITradingSimulator",
    "predictor_id": 12,
    "predictor_name": "主策略A",
    "share_level": "records"
  },
  "lottery_type": "pc28",
  "issue_no": "20260408001",
  "published_at": "2026-04-08T12:00:00Z",
  "signals": [
    {
      "bet_type": "big_small",
      "bet_value": "大",
      "confidence": 0.78,
      "message_text": "大10",
      "normalized_payload": {
        "stake_amount": 10,
        "primary_metric": "big_small"
      }
    }
  ]
}
```

### 3.3 字段说明

- `schema_version`
  - 信号协议版本
- `signal_id`
  - 来源侧生成的稳定唯一标识
- `source_type`
  - 固定为 `ai_trading_simulator`
- `source_ref`
  - 来源标识，不参与执行，但用于追溯和展示
- `lottery_type`
  - 当前建议固定为 `pc28`
- `issue_no`
  - 期号，执行层必须依赖它做幂等
- `published_at`
  - 信号发布时间
- `signals`
  - 一条来源记录可以生成多个候选投注信号

### 3.4 `signals[]` 字段

- `bet_type`
  - 例如 `big_small`、`odd_even`、`combo`
- `bet_value`
  - 例如 `大`、`小`、`单`、`双`、`大单`
- `confidence`
  - 可选，范围建议 `0 ~ 1`
- `message_text`
  - 可选，来源侧建议给出默认投注文本
- `normalized_payload`
  - 可扩展附加字段，例如 `stake_amount`
  - 建议逐步兼容更完整的资金策略字段：`base_stake`、`multiplier`、`max_steps`、`refund_action`、`cap_action`

## 4. 分析视图

### 4.1 用途

分析视图用于：

- AI 二次分析
- 多来源融合
- 风控评估
- 人工审阅

### 4.2 推荐字段

```json
{
  "schema_version": "1.0",
  "signal_id": "pc28-predictor-12-20260408001-big_small",
  "lottery_type": "pc28",
  "issue_no": "20260408001",
  "published_at": "2026-04-08T12:00:00Z",
  "predictor": {
    "predictor_id": 12,
    "predictor_name": "主策略A",
    "prediction_method": "自定义策略",
    "prediction_targets": ["number", "big_small", "odd_even", "combo"]
  },
  "prediction": {
    "prediction_number": null,
    "prediction_big_small": "大",
    "prediction_odd_even": "单",
    "prediction_combo": "大单",
    "confidence": 0.78,
    "reasoning_summary": "近期大号连续回补，大小偏大。"
  },
  "performance": {
    "recent_20_hit_rate": 0.55,
    "recent_100_hit_rate": 0.53,
    "current_hit_streak": 2,
    "current_miss_streak": 0
  },
  "context": {
    "history_window": 60,
    "primary_metric": "big_small",
    "profit_rule_id": "pc28_high"
  },
  "raw": {
    "prompt_snapshot": "...",
    "raw_response": "..."
  }
}
```

### 4.3 原则

- 分析视图可以更丰富
- 但不能替代执行视图
- 执行层只提取其中与投注动作直接相关的最小字段

## 5. 推荐对外接口

### 5.1 执行视图接口

建议未来由 `AITradingSimulator` 提供：

- `GET /api/export/signals/pc28`
- `GET /api/export/predictors/<predictor_id>/signals`

返回建议为：

```json
{
  "items": [
    {
      "...": "execution-view"
    }
  ]
}
```

### 5.2 分析视图接口

建议未来由 `AITradingSimulator` 提供：

- `GET /api/export/signals/pc28/analysis`
- `GET /api/export/predictors/<predictor_id>/signals/<issue_no>/analysis`

返回建议为：

```json
{
  "items": [
    {
      "...": "analysis-view"
    }
  ]
}
```

## 6. AITradingSimulator 侧建议

### 6.1 应做

- 提供稳定 JSON 接口
- 输出 `signal_id`
- 输出 `issue_no`
- 输出 `bet_type` / `bet_value`
- 输出 `confidence`
- 可选输出 `message_text`

### 6.2 暂不要求

- 直接生成 Telegram 投注任务
- 直接理解订阅、账号和群组
- 管理执行状态

## 7. pc28touzhu 侧接入方式

### 7.1 执行视图接入

执行视图进入：

- `signal_sources`
- `source_raw_items`
- `normalized_signals`

随后再进入：

- `user_subscriptions`
- `delivery_targets`
- `execution_jobs`

### 7.2 分析视图接入

分析视图不直接派发。  
应作为以下能力的输入：

- AI 重评分
- 多来源冲突处理
- 风控过滤
- 下注额策略

## 8. 兼容性建议

### 8.1 向前兼容

- 新字段只增不删
- 未识别字段允许忽略
- `schema_version` 必须保留

### 8.2 幂等

幂等推荐基于：

- `source_type`
- `signal_id`
- `issue_no`
- `bet_type`
- `bet_value`

### 8.3 编码

- 全部使用 UTF-8
- 时间统一 ISO8601 UTC

## 9. 当前项目里的对应关系

执行视图最接近当前 `raw item -> normalize` 的输入格式。  
也就是说，当前 `pc28touzhu` 可以直接消费这种结构：

```json
{
  "signals": [
    {
      "lottery_type": "pc28",
      "issue_no": "20260408001",
      "bet_type": "big_small",
      "bet_value": "大",
      "confidence": 0.78,
      "message_text": "大10",
      "stake_amount": 10,
      "base_stake": 10,
      "multiplier": 2,
      "max_steps": 6,
      "refund_action": "hold",
      "cap_action": "reset"
    }
  ]
}
```

这是当前最容易落地的第一版兼容协议。
