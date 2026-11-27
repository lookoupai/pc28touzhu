# cycle-recovery 独立 QA 验证报告

## 摘要

- **验证结论**：功能验收通过，无已知阻塞缺陷；完整 Python 回归、新旧真实浏览器与最后冻结版本定向回归均已通过，可进入最终副本演练确认及受控发布。
- **已执行**：全仓 Python unittest **1080 通过 / 0 失败 / 0 跳过**，232.908 秒；新恢复页 DOM/真实 WSGI/Chromium 三组检查通过，旧周期页五组检查通过，含 1440 与 390 两视口。
- **最终冻结版本**：受影响集合 **109 通过 / 0 失败 / 0 跳过**，24.931秒，覆盖独立15项、核对API、生命周期、受理、执行器恢复/运行/窗口。源码hash核对无执行期间变化。
- **独立新增**：`tests/test_cycle_recovery_qa.py` 核心11项与API4项合计15项全部通过；发现的嵌套证据类型错误返回500已由后端修复，同一断言通过。测试断言未弱化。
- **覆盖率**：未测。系统 Python 与生产 venv 均没有 coverage 模块；按不新增依赖约束未安装，不虚报百分比。
- **发布前置**：冻结代码静态门禁已通过；根代理协调的最终一致副本演练通过并保留生产一致备份后可发布。本报告不替代该实库迁移证据。

## 环境与执行范围

- 工作树：`/www/wwwroot/pc28touzhu-cycle-recovery`，基线 `43fd8a2`。
- 全部业务测试使用临时 SQLite/真实仓储、WSGI 服务。外部来源/开奖/Telegram 使用边界模拟，无生产写入、无真实外发消息。
- 浏览器测试运行本机临时 WSGI，拦截外部网络。新恢复页面核验整个库读取前后摘要保持不变；旧页面配置变更仅作用于自身临时库。
- 全仓套件只运行一次。运行期间最后回执审计修补尚在收尾；最终对补丁影响的 API、回报、受理、恢复和执行器集成重跑109项定向套件。全仓1080与定向109有重叠，不相加成1189个独立用例，也不声称全仓套件覆盖了其启动后所有变更。

## 实测记录

| 范围 | 完整命令 | 退出码 | 通过/失败/跳过 | 失败详情 |
| --- | --- | --- | --- | --- |
| 全仓 Python | `PYTHONPATH=src python3 -m unittest discover -s tests -p 'test_*.py' -q` | 0 | 1080 / 0 / 0 | 无；232.908秒，日志 `qa-final-python.txt` |
| 新恢复页真实浏览器 | `PC28_UI_EVIDENCE_DIR=/www/wwwroot/pc28touzhu-cycle-recovery/.boss/cycle-recovery/evidence/qa-final node tests/test_cycle_recovery_ui.cjs --browser` | 0 | 3检查组 / 0 / 0 | 无；日志 `qa-final-recovery-browser.txt` |
| 原周期页真实浏览器 | `PC28_UI_EVIDENCE_DIR=/www/wwwroot/pc28touzhu-cycle-recovery/.boss/cycle-recovery/evidence/qa-legacy node tests/test_cycle_ui.cjs --browser` | 0 | 5检查组 / 0 / 0 | 无；日志 `qa-final-legacy-browser.txt` |
| 核心独立 QA | `PYTHONPATH=src python3 -m unittest tests.test_cycle_recovery_qa -v`（增加最终API用例前） | 0 | 11 / 0 / 0 | 无；5.088秒，细节 `qa-core-report.md` |
| 最终独立 API 首次有效运行 | `PYTHONPATH=src python3 -m unittest tests.test_cycle_recovery_qa.CycleRecoveryApiIndependentTests -v` | 1 | 3 / 1 / 0 | 嵌套 `verified_message` 传list，期望400实际500；1.395秒，回派修复 |
| 最终冻结版本定向回归 | `PYTHONPATH=src python3 -m unittest tests.test_cycle_recovery_api tests.test_cycle_recovery_qa tests.test_cycle_recovery tests.test_execution_acceptance tests.test_executor_recovery tests.test_executor_runtime tests.test_executor_send_window -q` | 0 | 109 / 0 / 0 | 无；24.931秒，日志 `qa-final-targeted.txt` |

浏览器脚本不使用计数型测试框架，以上3/5为脚本实际报告的检查组，不等于断言数量，也不与Python用例相加。新恢复检查包含21个历史档案的真实分页、管理员/其他用户授权、同页当前待命/新轮运行、未决本金独立显示、XSS转义、旧字段兼容、手机不横向溢出、页面无错误、核对页面无写请求。旧页面包含表单、连挂条件、手动状态、真实WSGI配置持久化/额度隔离，以及浏览器两视口。

## 独立发现与修复要求

| 问题 | 红测证据 | 修复职责 / 验证 |
| --- | --- | --- |
| 旧 cancelled事件仍有unknown发送，反复占据limit首条，后续credit饿死 | `test_inconsistent_cancelled_ledger_does_not_starve_a_ready_credit`，第二档期望resolved实际awaiting_reconciliation | 后端记录检查时间/原因，原独立11项复验通过 |
| 历史leg_event_ids含null，使TypeError中止全批扫描 | `test_invalid_legacy_leg_identifier_isolated_from_healthy_rule_recovery` | 后端边界验证正整数list，错误按归属隔离，原独立11项通过 |
| 核对后迟到原回执可能降级明确结论 | 技术评审与后端新增三种顺序：accepted→unknown、accepted→缺ack delivered、rejected→late delivered | 独立事实审计避免污染canonical attempts；最终109项定向套件已通过这三项 |
| 嵌套核对证据类型缺校验 | `test_oversized_and_malformed_evidence_are_rejected_without_side_effect`，verified_message为list返回500 | 后端补嵌套字典/时间字段校验，最终109项中的同一断言通过，无删改/跳过 |

独立测试写作中修正过两处fixture错误：执行任务列表不含顶层leg_key，改按实际send_with_job_id选主辅；复用含super()的setUp绑定不正确，改为独立fixture初始化。均未改变业务断言；fixture错误不计为产品缺陷。

## 已核验的业务与安全承诺

- 原故障数值完整闭环：第一关真实结算到22.39余额、13.93链注；第二关13本金占用后9.39；下一轮只补0.61，已实现利润3.93保留。
- 旧轮过原截止摘离，新轮有真实run/events/jobs，不靠人工恢复；原日次数不退回、同窗不重复、正常placed跨日不受影响。
- 多archive逆序查清，只在未来开轮消费一次credit，正在运行周期的balance/stake/关数/pending不受改写；chain/all_in返还去向分别核验。
- 同日真实迟到亏损仍触发原日风控；昨日亏损不误停今天；人工stop保持优先。
- 部分已结算腿K与未决本金R独立计算，无重复profit；未知旧单不盲目void或重发。
- API匿名401、非owner403、管理员可读；只读白名单与无资金修改；核对租约过期/被新租约替换均拒绝。
- 严格协议错期、错reply、错机器人、错金额、时间越窗、legacy式纯传输确认不足、未经证明not_sent均拒绝；未知保留原账。

## 静态与覆盖率

最终冻结后已执行 `python3 -m compileall -q src/pc28touzhu tests/test_cycle_recovery.py tests/test_cycle_recovery_api.py tests/test_cycle_recovery_qa.py tests/test_executor_recovery.py`、`node --check src/pc28touzhu/ui/auto_triggers.js`、`node --check tests/test_cycle_recovery_ui.cjs`、`git diff --check`，退出码均0。`qa-final-code-hashes.json` 保存修改源文件及独立API/QA测试SHA256；最终109项执行后逐文件核对，变化列表为空。

`python3 -m coverage --version` 与 `/www/wwwroot/pc28touzhu/.venv/bin/python -m coverage --version` 均返回 No module named coverage。未增加依赖，不声称达到80%行覆盖率。行为验证由真实数据库和WSGI的全回归/独立用例支撑；不能据此声称所有代码分支已覆盖。

依赖清单、锁文件、Docker、CI、systemd部署配置没有本次意外修改（针对这些路径 `git diff --name-only` 输出为空）。

## 状态

**STATUS**: DONE
**REASON**: 独立功能验收完成，全回归、新旧真实浏览器和最终冻结代码109项定向测试均通过，无已知功能阻塞。覆盖率未测原因已如实说明；发布仍使用根代理与DevOps的最终实库副本/备份证据。
