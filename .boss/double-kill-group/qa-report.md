# 双组、杀组验证报告

## 摘要

- 结论：代码与功能验证通过。投注端 737 项、来源端 285 项完整回归全部通过。
- 两项目 67,072 个离线结算场景一致，涵盖全部三球开奖、8 套规则及分币下注金额。
- DOM 与 Chromium 流程通过；实际创建/回读方案、规则方向、模板口令、记录展示及移动布局均验证。
- 关键玩法和结算模块行/分支综合覆盖率 86%；此数值不代表整个仓库覆盖率。
- 旧版本数据库迁移在临时库回放，既有资金汇总保持不变，新组事件能正确入账。

## 验证证据

所有 Python 验证使用临时数据库或纯计算，不访问生产数据库。浏览器请求限定本地 fixture，不发送外部消息。

| 验证 | 命令 | 结果 | 证据 |
| --- | --- | --- | --- |
| 投注端完整回归与关键模块覆盖 | `PYTHONPATH=src:/tmp/pc28-cycle-test-tools COVERAGE_FILE=/tmp/pc28-group-final.coverage python3 -m coverage run --branch --source=pc28touzhu.domain.pc28_group_play,pc28touzhu.domain.settlement_rules -m unittest discover -s tests -p 'test_*.py' -q` | 737 项通过，147.075 秒 | `evidence/pc28-unittest.txt`、`evidence/coverage.txt`、`evidence/coverage.json` |
| 来源端完整回归 | `.venv/bin/python -W ignore::DeprecationWarning -m unittest discover -s tests -p 'test_*.py' -q` | 285 项通过，93.594 秒 | `evidence/source-unittest.txt` |
| 来源重点回归 | `.venv/bin/python -W ignore::DeprecationWarning -m unittest tests.test_group_play tests.test_profit_simulator tests.test_pc28_fullpay_simulation tests.test_signal_export -q` | 36 项通过 | `evidence/source-targeted.txt` |
| 两项目结算对账 | `/www/wwwroot/AITradingSimulator/.venv/bin/python tests/verify_group_cross_project.py` | 67,072 场景通过 | `evidence/cross-project.json` |
| DOM 契约 | `node tests/test_group_ui.cjs` | 通过 | `evidence/dom.txt` |
| 浏览器端到端 | `node tests/test_group_browser.cjs` | 通过，无 pageerror、手机无水平溢出 | `evidence/browser.txt` 及两张 PNG |
| 旧库迁移 | 从当前基线提交加载旧 Repository 和旧日报触发器，在临时库初始化，再用新 Repository 连续初始化两次并结算组单 | 通过，旧金额不重复回填 | `evidence/legacy-migration.json` |
| 静态检查 | 两项目 `git diff --check`、修改的 7 个 JavaScript 文件 `node --check`、核心 Python AST 解析 | 通过 | 实际命令退出码均为 0 |

coverage 使用已有 `/tmp/pc28-cycle-test-tools` 工具，没有修改项目依赖。该环境缺少 C tracer，使用 Python tracer 完成同一覆盖计数。

## 关键业务断言

1. 双组六种合法集合、杀组四种排除项、旧别名、重复/非法组合和元数据冲突均校验。
2. 旧 all/空筛选不消费新玩法；显式组信号优先；同源同期组信号复用，冲突拒绝；旧 combo 同期修订不改变冻结组且仍可入库。
3. 每组一次完整发送、一次资金事件；多笔金额和规则冻结。失败重试保持原截止时间，未知结果/过期不重发，迟到失败不能覆盖成功。
4. 普通命中扣除其他子注本金；特殊退款只退中奖子注；命中但净亏时预测统计仍记命中，倍投按净亏推进一次。
5. 用户隔离、伪造收益拒绝、冻结方向与规则、按玩法止损、净收益止盈、批量统计、规则路由资金归属均覆盖。
6. 重复结算及重复初始化不重复入账；旧资金历史、旧过关、原有三类玩法完整回归通过。
7. 来源可保存双组/杀组为主玩法和收益玩法；无 combo 目标时拒绝。默认导出不自动追加新玩法，opt-in 导出结构化集合，表现接口输出两类统计。
8. DOM/浏览器验证每组合金额与总额、方案保存回读、Bot 纯函数命令、新玩法方向、组合模板逐注渲染、预测命中与整组亏损同时展示。

## 对账范围及发现

- 64,000 场景：8 套结算规则 × 1,000 种三球开奖 × 4 个原组合预测 × 2 类组玩法，每组合金额 10。
- 3,072 场景：8 个分币/较大金额 × 6 个代表性开奖 × 8 套规则 × 4 个原组合预测 × 2 类玩法。
- 对比覆盖命中、整组财务结果、总投注、总返还、净收益和各子注命中/退款状态。
- 修正来源高赔 ABC 原先未退本的模拟配置，与执行端原有规则对齐。
- 修正组模拟的分币舍入口径：执行端按逐注净收益舍入后加回本金，来源组模拟采用同样方式。普通旧单注模拟的舍入流程保持原状。

## 发布边界

没有执行生产迁移、生产负载测试、真实 Telegram 群发送或服务重启。代码已准备好发布；启用步骤和回滚边界见 `deploy-report.md`、`docs/GROUP_PLAYS.md`。

**STATUS**: DONE
