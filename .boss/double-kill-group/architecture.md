# 双组、杀组架构

## 摘要

- 需求来源：用户确认 `docs/DOUBLE_KILL_GROUP_DESIGN.md` 后要求实施；用户随后要求不使用子代理，全部工作由主代理串行执行。
- 沿用 Python/SQLite/现有 HTML 与 JavaScript，两项目不引入新框架或依赖。
- 每组一个信号、一个资金事件、每目标一个发送任务；事件内冻结 2/3 个 combo 子注，整组一次结算。
- 金额为每组合金额；组级总额、逐注盈亏、预测覆盖命中与财务结果分别保存。
- 新玩法需要显式 selected 筛选；旧 all 仅跟随原有玩法。来源导出新玩法需 opt-in，投注端兼容从 combo 派生。

## 技术调研与决策

调研依据为两个项目现有代码及可运行测试：SQLite 仓储已有派单与结算事务、progression_event 的 JSON 快照、单任务完整发送回执；旧过关 main/aux 发送有一对一从任务索引。

| 决策 | 候选一 | 候选二 | 选择理由 |
| --- | --- | --- | --- |
| 组级存储 | 父事件＋JSON 子注快照 | 新父子表及多任务发送 | 选一：一次消息天然对应一次事件，避免重复财务入账、部分子注重试、分笔推进倍投 |
| 组级发送 | 单任务完整文案 | 扩展旧过关多任务合并 | 选一：复用成熟领取、幂等、重试路径，旧过关语义不变 |
| 玩法兼容 | all 保留原范围、selected 显式启用 | all 自动包含新玩法 | 选一：不扩大旧方案投注范围 |
| 来源兼容 | 结构化导出＋旧 combo 本地派生 | 强制来源全部升级 | 选一：新来源显式信号优先，旧来源下一期即可提供派生信号 |

不引入第三方组单框架或新数据库；业务规则放到纯领域模块，数据库事务与持久化留在 Repository。

## 领域契约

- 组合排序：`大单、大双、小单、小双`；双组为任意两个不同组合的规范排序，以 `+` 连接。
- 杀组 bet_value 是被排除的单个组合；中文展示加“杀”，机器值不加。
- 旧双组别名：单组→大单+小双，双组→大双+小单；杀组允许兼容“杀小双”。
- `normalized_payload.selected_combos: string[]`：双组长度 2、杀组长度 3，唯一且合法；传入与 bet_value 不一致时拒绝。
- `normalized_payload.excluded_combo: string|null`：仅杀组；与 selected_combos 互补。
- `normalized_payload.group_derivation: string`：从 combo 派生使用 `opposite_combo_v1`；`origin_combo` 保存原预测。
- 资金策略 fixed/martingale/follow_source 的 amount/base_stake 仍是每组合配置；生成任务时 amount 与事件 stake_amount 为整组总额，任务另存 per_combo_amount 和 legs。
- `settlement_snapshot.group` 保存规范玩法与组合，使用冻结的规则/赔率逐注结算；`result_context.group_settlement` 保存子注结果、总额、利润、损失、净收益。
- `prediction_hit` 独立存储组级覆盖命中；`resolved_result_type` 按组级净收益正/负/零映射 hit/miss/refund，仅用于资金推进。统计命中优先取 prediction_hit，旧事件保持原口径。
- 未发送作废使用原 void 状态，不能记成开奖退本。组级手动 hit/miss 无法确定具体赔率和部分退本，必须通过开奖结果结算。
- win_streak_roll 是独立结构策略，不接受双组/杀组作实际投注；配置边界和派单兜底应明确拒绝组合使用。

## API 契约

沿用原有鉴权和响应结构，不新增身份/权限入口。

| 方法 | 路径 | 认证 | 输入 | 响应/错误 |
| --- | --- | --- | --- | --- |
| GET | 来源 `/api/export/predictors/<predictor_id>/signals` | 原所有者会话/导出 Token/公开规则 | `view=execution`；可选 `include_group_plays=1` | 原 items/signals 结构；opt-in 新增 double_group/kill_group；非法参数 400 |
| GET | 来源 `/api/export/predictors/<predictor_id>/performance` | 原规则 | 原参数 | 原 metrics 新增 double_group/kill_group，保留 recent_20/50/100 和 streaks 字段 |
| POST | 投注 `/api/platform/subscriptions` 及原更新入口 | 原用户会话 | `play_filter.mode=selected`；selected_keys 含 `double_group:大单+小双` 或 `kill_group:小双` | 原 item 结构；无效组合/策略冲突 400 |
| POST | 投注原结算 resolve 入口 | 原用户会话 | progression_event_id、draw_context | 原 resolved/progression，增加 prediction_hit 和组级子注明细 |

所有新增字段均为兼容扩展。来源默认执行导出不变；旧订阅 all 不跟新玩法。自动触发的 fixed_metric/matched_metric 选择新玩法视为显式启用，继承/keep 尊重原订阅范围。

## 风险与应对

| 触发条件 | 应对 |
| --- | --- |
| 命中组合遇退本、其他子注亏损 | 逐注算钱，净收益推进倍投，prediction_hit 单独统计 |
| 来源同时提供 combo 和新玩法 | 显式新玩法优先，派生按同源同期去重；非法显式组不静默替换 |
| 来源仅支持旧 combo | 本地派生；已解析历史记录不回放出单 |
| 配置中途改变 | 结算使用事件冻结组与赔率，重试使用保存文案/金额 |
| 旧 all 订阅 | 组信号在过滤层默认拒绝，新玩法需 selected |
| 发布期间旧 worker | 新玩法在所有相关 worker 升级后启用；不运行生产迁移或自动启用方案 |

## 验证

领域覆盖全部 6 双组、4 杀组与各开奖；临时 SQLite 验证派单、回执、部分退本、倍投、风控、报表、路由及重启幂等；来源测试使用 fresh_app_harness；浏览器使用临时库与现有本地测试工具。
