# 开发任务

## 摘要

- Blast Radius：高；涉及玩法、资金和统计。用户明确确认原分析方案后要求实施，已授权该范围。
- 风险确认触发项：超过 10 文件、事件模型扩展。只改代码和测试，不操作生产数据库、服务或外部发送。
- 原子任务串行，主代理为全部文件 owner，无并行写集。
- P0：双组/杀组下注；每组合金额；正确资金结算；一次发送/一次推进；显式启用；来源名称/导出/收益一致；旧功能回归。

## Repo Preflight

- 两仓库均 main，origin/HEAD 指向 main。投注项目初始只有 docs/DOUBLE_KILL_GROUP_DESIGN.md 未跟踪；来源已有 reports/、三个 backtest 脚本、services/pc28_split_roll.py、tests/test_pc28_split_roll.py 未跟踪，保留不改。
- CI：两边 `.github/workflows/docker-image.yml` 为镜像发布，不作为测试通过证据。
- 投注 Python unittest，`PYTHONPATH=src python3 -m unittest discover -s tests`；UI 有 jsdom/playwright-core 临时工具路径，见 tests/README.md。
- 来源 Python unittest，`.venv` 存在；测试 harness 使用临时数据库。具体解释器依赖通过测试探测。
- 契约来源：domain/pc28_play_filter.py、subscription_strategy.py、settlement_rules.py、auto_trigger_service.py、api/app.py 和来源 app.py/utils/pc28.py。
- 保留当前认证和租户范围；使用仓储现有事务、SQL 参数绑定和事件幂等。
- 不新增外部依赖，不删除文件，不执行线上迁移；Nullable prediction_hit 字段由现有初始化迁移代码添加，测试仅使用临时库。

## 任务与写集

| 任务 | 写集（相对各自项目根目录） | 验证 |
| --- | --- | --- |
| T1 领域与边界 | 投注 domain/pc28_group_play.py、domain/pc28_play_filter.py、domain/subscription_strategy.py、domain/settlement_rules.py；tests/test_group_play.py | 集合与逐注收益，部分退本 |
| T2 资金闭环 | 投注 services/dispatch_service.py、services/platform_service.py、executor/db_repository.py、executor/profit_reporting.py；tests/test_group_dispatch.py | 临时库发送/结算/倍投/统计/重试 |
| T3 来源兼容 | 投注 services/normalize_service.py、services/source_sync_service.py（需要时）、tests/test_group_source_contract.py；来源 utils/pc28.py、app.py、services/profit_simulator.py、lotteries/registry.py、utils/prompt_assistant.py；来源 tests/test_group_play.py | 导出/派生/收益一致与旧导出不变 |
| T4 规则 | 投注 domain/draw_streak.py、domain/prediction_streak.py、services/auto_trigger_service.py、services/telegram_bot_service.py；tests/test_group_triggers.py | 规则指标、方向、路由切换与 Bot |
| T5 前端 | 投注 ui/autobet.js、auto_triggers.js/html、workspace_subscriptions.html、workspace_templates.html、dashboard.js、records.js、ui_text.js；来源 static/app.js、home.js、public_predictor.js、templates/dashboard.html、public_lottery_home.html；tests/test_group_ui.cjs、tests/test_group_browser.cjs、tests/group_ui_server.py | 选择/保存/展示/金额/兼容、浏览器 |
| T6 集成与交付 | 两边有关旧测试的预期兼容更新、README.md；投注 tests/verify_group_cross_project.py、docs/GROUP_PLAYS.md、docs/DOUBLE_KILL_GROUP_DESIGN.md、docs/SIGNAL_PROTOCOL.md、.boss/double-kill-group/* | 全量回归、文档与证据 |

新增测试文件先运行得到行为缺失证据，再实施对应功能。任何资金/幂等/隔离失败时停止推进，修复后重跑。

## Contract Matrix

| ID | UI/文案 | Payload/Schema | Persistence/业务 | Evidence |
| --- | --- | --- | --- | --- |
| CM1 | 双组/杀组及明确方向 | double_group、kill_group 与合法集合 | selected 显式启用 | test_group_play/test_group_ui |
| CM2 | 每组合金额、本期总额 | per_combo_amount、legs、amount | 一组一事件一消息 | test_group_dispatch |
| CM3 | 预测命中、实际盈亏 | prediction_hit、group_settlement | 部分退本按实际金额、一次推进 | test_group_dispatch |
| CM4 | 来源导出/模拟与执行一致 | include_group_plays=1 | 旧默认导出保留、派生去重 | 两项目 group 测试 |
| CM5 | 新玩法规则与 Bot | metric/selected_keys | 继承范围隔离、路由显式选型 | test_group_triggers |

## 完成记录

T1–T6 实现完成，主代理串行实施，没有继续使用子代理。完整 QA 证据见 qa-report.md。方案细化为一组一个资金事件，未扩展旧过关任务的发送索引。新增可空 prediction_hit 字段、组信号查找索引、日报触发器的兼容升级均仅在临时测试库验证。
