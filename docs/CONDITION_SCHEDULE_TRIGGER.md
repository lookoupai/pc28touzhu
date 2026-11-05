# 自动跟单：条件与定时组合

在自动触发规则中选择 **条件 + 定时（时段内满足条件才触发）**，可同时保存星期、时间窗口、开始条件和附加条件区。

例如需要每天北京时间 18:00 后才检查条件：

1. 选择“条件 + 定时”。
2. 选择需要启用的星期；保留一个窗口，开始设为 `18:00`，结束留空（当天 `24:00`）。
3. 配置命中率、连挂或连续开奖条件，需要时增加附加条件区，保存规则。

18:00 之前不检查条件；18:00 起按现有检查周期判断，条件满足后才能开轮。开始时间包含，结束时间不包含；当天 24:00 后按次日星期和窗口重新判断。统计条件仍采用原有统计范围，不会把统计起点截到 18:00。

时段只限制开新轮，已启动的轮次继续按原有止盈、止损、过关和风控规则运行。“等待条件”再次开轮时重新判断条件和冷却；过关“直接续开”跳过条件与冷却，但仍受时间窗口、每日次数和日风控限制。

组合模式沿用条件触发的重复开轮逻辑，不额外施加“每窗口每天一轮”的纯定时限制。开启“立即派发最新信号”时，信号必须在当前窗口内发布，且未超过配置的最大延迟。已有纯条件、纯定时规则不自动切换。

接口使用既有 `schedule` JSON，无需数据库迁移：

```json
{
  "trigger_mode": "condition_schedule",
  "schedule": {
    "timezone": "Asia/Shanghai",
    "weekdays": ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"],
    "windows": [{"id": "evening", "start": "18:00", "end": "24:00"}],
    "signal_max_age_seconds": 300
  },
  "conditions": [{"type": "miss_streak", "metric": "big_small", "operator": "gte", "threshold": 5}],
  "guard_groups": []
}
```

`condition_schedule` 会校验并保存两组配置；条件列表或时间窗口列表为空时拒绝保存。`24:00` 仅能作为窗口结束时间；跨夜需求可拆成两段当天窗口。

验证：`python3 -m unittest tests.test_condition_schedule tests.test_cycle_api`，以及 `node tests/test_cycle_ui.cjs`（按脚本说明提供 jsdom）。测试使用临时数据库。
