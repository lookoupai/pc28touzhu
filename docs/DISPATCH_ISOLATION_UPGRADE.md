# 派单隔离升级说明

## 行为边界

- 有路由的自动规则使用路由自己的轮次、金额和倍投状态。订阅直派不再根据历史规则推断归属，双方的止盈止损不相互关闭。
- 同一个用户、信号、订阅、投递目标只创建一个任务。所有路径在同一事务中检查、创建事件和派单，数据库唯一索引提供最后一道约束。
- 去重按信号 ID 和投递目标记录 ID 计算，不合并不同来源、玩法、订阅或目标记录。
- 手动停轮保留已经形成的金额和基线，只取消尚未投递的待执行任务。已投递单的延迟结算回到原轮次，不创建新轮，也不修改后来新轮的余额和倍投步数。
- 新轮实际准备派单时重置路由基线及倍投步数；活动轮跨天继续时不重置。定时规则仍遵守每天一次的开轮约束。
- 父规则轮在子路由结束时主动收口，跨日检查也执行收口，不依赖用户打开页面。
- 轮次历史同时返回订阅与路由记录，并通过 `scope` 区分；订阅日统计仍是汇总报表，不作为路由间共享的风控余额。
- 旧的无路由自动规则仍使用订阅运行状态。需要与订阅直派完全独立的自动规则，应配置显式路由。

## 群组派单模式

`delivery_targets.dispatch_mode` 可在群组编辑表单中设置。

| 值 | 页面名称 | 允许的派单来源 |
| --- | --- | --- |
| `shared` | 互斥并存 | 规则和订阅直派均可，受同一去重约束限制 |
| `rule_only` | 仅自动规则 | 有明确规则归属的派单 |
| `direct_only` | 仅手动跟单 | 不归属自动规则的订阅直派 |

旧配置升级后默认为 `shared`，不会自动改成仅规则模式。`shared` 下规则停止后，独立直派仍可能发单；若群组必须随规则停止，部署前应确认使用 `rule_only`。

## 数据库升级

新增字段：

- `delivery_targets.dispatch_mode`。
- `subscription_progression_events.auto_trigger_runtime_run_id`。
- `subscription_runtime_runs.auto_trigger_rule_run_id`。
- `auto_trigger_route_subscription_runtime_runs.auto_trigger_rule_run_id`。

新增唯一索引 `idx_execution_jobs_signal_subscription_target`。索引在旧表列迁移完成后创建，不清理或合并历史任务。

升级不清零金额、累计盈亏或历史阈值，也不根据停轮说明文字推断并删除旧状态。新事件显式记录所属轮次；旧事件缺少关联字段时，只在同一用户、路由、订阅及事件创建时间覆盖的轮次中查找。找不到归属时结算报错，需核对历史数据，不自动补造轮次。

部署前可只读检查重复键，结果必须为空：

```bash
sqlite3 -readonly "/www/wwwroot/pc28touzhu/pc28touzhu.db" ".timeout 5000" "
SELECT user_id, signal_id, subscription_id, delivery_target_id, COUNT(*) AS duplicate_count
FROM execution_jobs
WHERE subscription_id IS NOT NULL
GROUP BY user_id, signal_id, subscription_id, delivery_target_id
HAVING COUNT(*) > 1;
"
```

如果存在重复，不要直接删除历史账目或绕过索引；先核对执行结果，再单独确认修复方案。

## 部署顺序

以下步骤涉及生产操作，需要另行确认。本次代码修改不自动执行这些步骤。

1. 确认目标群组使用的派单模式，以及现有历史阈值是否需要单独处理。
2. 在维护窗口停止派单、执行器、结算和其他数据库写入进程，备份数据库；保留在途单，不重放历史信号。
3. 在隔离副本执行 `DatabaseRepository.initialize_database()` 并校验金额、任务数量、完整性和外键；没有问题后升级生产库。
4. 所有读取仓储代码的长驻进程统一切换到同一版本，再恢复服务。不能仅重启平台或自动触发服务。

| 服务 | 需要更新的调用链 |
| --- | --- |
| `pc28touzhu-platform.service` | 配置、人工派单、停轮及页面查询 |
| `pc28touzhu-source-sync.service` | 信号同步和持续派单 |
| `pc28touzhu-auto-trigger.service` | 条件及定时开轮 |
| `pc28touzhu-pc28-auto-settlement.service` | 延迟结算和风控收口 |
| 已启用的执行器、告警、收益 Bot、日报服务 | 同一版本的仓储及平台接口 |

5. 核对进程启动时间和部署版本，观察一轮真实信号的任务归属、唯一性及结算收口。代码文件已更新不代表旧进程已加载新代码。

如需回退，先停止相关进程并核对升级后新增任务。不要直接用升级前备份覆盖已有新账目的生产库。

## 验证

```bash
PYTHONPATH=src "./.venv/bin/python" -m unittest discover -s "tests" -p "test_*.py"
node --check "src/pc28touzhu/ui/autobet.js"
git diff --check
```

`tests/test_dispatch_isolation.py` 覆盖跨路径并发去重、异常回滚、相互独立的风控、跨日收口、手动停轮及旧单延迟结算。

已在 2026-09-04 的历史备份副本连续执行两次升级。37,870 条任务记录及全部财务、日统计记录内容保持不变，`integrity_check` 为 `ok`，外键错误为 0。该副本没有待结算路由事件，延迟结算场景由独立测试覆盖。
