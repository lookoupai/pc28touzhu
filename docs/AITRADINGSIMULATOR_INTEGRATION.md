# AITradingSimulator 接入说明

## 1. 目标

本文档说明如何把 `/www/wwwroot/AITradingSimulator` 作为 `pc28touzhu` 的第一个正式来源接入。

关键前提：

- `AITradingSimulator` 仍然只负责预测
- `pc28touzhu` 负责信号聚合、订阅、派发与执行

## 2. 分工边界

### 2.1 AITradingSimulator 负责

- 生成预测结果
- 输出标准化 JSON 信号
- 输出可选分析视图
- 提供来源追溯信息

### 2.2 pc28touzhu 负责

- 注册来源
- 拉取信号
- 保存 raw item
- 标准化为 signal
- 用户订阅来源
- 用户绑定 Telegram 账号与群组
- 生成执行任务
- 发送 Telegram 消息

## 3. 第一阶段推荐接法

不要一开始做双向深度耦合。  
第一阶段建议只做单向拉取：

1. `AITradingSimulator` 提供导出接口
2. `pc28touzhu` 通过 `ai_trading_simulator_export` 来源类型拉取
3. 拉取结果进入 `raw_items`
4. 再标准化为 `signals`

## 4. AITradingSimulator 需要补什么

建议新增两个接口中的一个即可：

- `GET /api/export/signals/pc28`
- `GET /api/export/predictors/<predictor_id>/signals`

第一版只要返回执行视图即可。

## 5. pc28touzhu 这边怎么配

在来源管理里新增一个 `source`：

- `source_type=ai_trading_simulator_export`
- `config.fetch.url` 指向 `AITradingSimulator` 的导出接口

例如：

```json
{
  "fetch": {
    "url": "https://your-ai-platform.example.com/api/export/predictors/12/signals?view=execution",
    "headers": {
      "Accept": "application/json"
    },
    "timeout": 10
  }
}
```

现成模板见 [AITRADINGSIMULATOR_SOURCE_TEMPLATE.json](/www/wwwroot/pc28touzhu/docs/AITRADINGSIMULATOR_SOURCE_TEMPLATE.json)。

说明：

- 当前已经有专用来源类型 `ai_trading_simulator_export`
- 它会直接识别 `AITradingSimulator` 的导出 payload
- 自动把 `signal_id` 作为 `external_item_id`
- 自动提取 `issue_no` 与 `published_at`

## 6. 最小推荐 payload

建议 `AITradingSimulator` 第一版就返回：

```json
{
  "items": [
    {
      "schema_version": "1.0",
      "signal_id": "pc28-predictor-12-20260408001-big_small",
      "source_type": "ai_trading_simulator",
      "source_ref": {
        "predictor_id": 12,
        "predictor_name": "主策略A"
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
          "stake_amount": 10,
          "base_stake": 10,
          "multiplier": 2,
          "max_steps": 6,
          "refund_action": "hold",
          "cap_action": "reset",
          "primary_metric": "big_small",
          "share_level": "records"
        }
      ]
    }
  ]
}
```

## 7. normalize 适配建议

当前 `pc28touzhu` 的 `normalize_raw_item()` 更适合直接读取：

```json
{
  "signals": [...]
}
```

现在已经实现的做法是：

- `AITradingSimulator` 返回：
  - `items[].signals`
- `pc28touzhu` 的 `ai_trading_simulator_export` 抓取后保留整个 payload 为 `raw_payload`
- `normalize_raw_item()` 会自动摊平 `items[].signals`
- 每个子信号会补上：
  - `issue_no`
  - `lottery_type`
  - `signal_id`
  - `source_ref`
- `normalized_payload` 会保留：
  - `stake_amount`
  - `base_stake`
  - `multiplier`
  - `max_steps`
  - `refund_action`
  - `cap_action`
  - `primary_metric`
  - `share_level`

## 8. 第二阶段再做什么

等第一阶段跑通后，再考虑：

- 拉取多个 predictor
- 拉取公开方案榜单
- 拉取分析视图
- 用 AI 做二次融合
- 私有方案鉴权

## 9. 推荐顺序

1. 在 `AITradingSimulator` 补一个稳定的 JSON 导出接口
2. 在 `pc28touzhu` 新增一个 `source_type=ai_trading_simulator_export` 的来源
3. 跑通：
   - fetch
   - raw item
   - normalize
   - signal
   - dispatch
   - telegram executor
4. 再扩分析视图和多 predictor 场景

## 10. 当前最重要的判断

第一阶段不要追求“最通用”。  
先让 `AITradingSimulator` 成为一个稳定来源。

只要这条链跑通，后面接网站、频道、别人的分享方案都会顺很多。
