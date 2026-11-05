## 摘要

- Wave 3 完整回归实际运行 **647 项，647 通过、0 失败、0 跳过**；之后针对旧无路由规则停用／跨日风控修复，124 项相关回归通过，仓储新增 2 项精准验证通过。最终收集 649 项，但没有宣称执行了649项全量；不能把重复执行数量相加当作独立用例。
- 页面 DOM 两组及真实 WSGI＋SQLite 一组回放通过；真实 Chromium 在 390、1440 宽度下检查规则页和手动页，4 个场景均无脚本错误、无页面横向溢出，隔离 API 健康检查为 200/ok。
- 新增领域模块行覆盖 89.74%、自动周期服务行覆盖 90.91%；分支分别为 77.78%、79.12%，行与分支综合覆盖 87%。覆盖率只针对这两个新增关键模块，不代表整个仓库。
- 独立发现的归属、幂等键、并发配置变化和迁移隔日解封问题均已修复并保留回归测试；未发现尚未修复的 P0/P1。
- **不能宣称全部 Boss 门禁通过**：Gate 0 的 `innerHTML` 静态正则告警在干净基线 5b37efa 也会失败；附加 OWASP 扫描也仅 a07-xss 静态匹配告警，其他 8 类检查通过。本次插入值已审查转义且 XSS 回放通过。Gate 1/2 的框架或压测跳过不计作验证证据。代码尚未生产部署。

# 验证范围与版本

- 工作树：`/tmp/pc28-cycle-isolation`，基线 `5b37efa`，Python 3.12.3、Node 22.21.1。
- 测试时间：2026-11-05 UTC；最终代码指纹见 `evidence/qa-final-tree.json`：`0fb896fd836a5ea47a61ee00938b8c7c76f2310307d05ae375324ffc4bcd92a0`。
- 指纹覆盖 `git diff HEAD -- src tests README.md` 和所有未跟踪的 src/tests 文件内容；文档与截图不在指纹范围。
- 所有数据库都是匿名合成的临时 SQLite；没有连接生产 API、复制生产数据库、重启生产服务或调用 Telegram 发送。第三方开奖/预测与发送回执仅在边界由 fixture 提供，业务、仓储、WSGI schema 与事务使用真实实现。

## 1. Verification / 自动化测试结果

下列命令均在工作树根目录执行。临时 coverage 包位于 `/tmp/pc28-cycle-test-tools`；由 PyPI 纯 Python wheel 下载并核对 SHA256，未增加项目运行依赖。C tracer 不存在仅导致使用 Python tracer，不影响计数。

| 范围 | 实际命令 | 退出码 | 通过/失败/跳过 | 证据与失败详情 |
|---|---|---:|---|---|
| 完整回归＋关键模块覆盖 | `PYTHONPATH=src:/tmp/pc28-cycle-test-tools COVERAGE_FILE=/tmp/pc28-cycle-final.coverage python3 -m coverage run --branch --source=pc28touzhu.domain.auto_trigger_cycle,pc28touzhu.services.auto_trigger_cycle_service -m unittest discover -s tests` | 0 | 645/0/0 | `evidence/qa-final-unittest.log`；111.981 秒；无失败 |
| 最后并发修复后追加验证 | `PYTHONPATH=src:/tmp/pc28-cycle-test-tools COVERAGE_FILE=/tmp/pc28-cycle-final.coverage python3 -m coverage run --append --branch --source=pc28touzhu.domain.auto_trigger_cycle,pc28touzhu.services.auto_trigger_cycle_service -m unittest tests.test_cycle_runtime tests.test_cycle_qa` | 0 | 35/0/0 | `evidence/qa-final-concurrency.log`；15.397 秒；含最后新增的 2 项，未重复完整库 |
| root 最终 Wave 3 全套复验 | `PYTHONPATH=src python3 -m unittest discover -s tests` | 0 | 647/0/0 | `evidence/qa-wave3-final.log`；93.598 秒；root实际执行并复核，无失败 |
| 旧无route停用／当前日风控修复后回归 | `PYTHONPATH=src python3 -m unittest tests.test_cycle_runtime tests.test_win_streak_roll_stop tests.test_win_streak_roll_dispatch tests.test_auto_trigger_service` | 0 | 124/0/0 | `evidence/qa-final-legacy-guard.log`；34.555秒；业务owner执行、root确认命令与结果，无失败 |
| 仓储旧归属辨识精准验证 | `PYTHONPATH=src python3 -m unittest tests.test_cycle_repository.CycleRepositoryTests.test_bound_legacy_owner_survives_rule_disabled_and_routes_added tests.test_cycle_repository.CycleRepositoryTests.test_unbound_manual_runtime_never_guesses_historical_rule_owner` | 0 | 2/0/0 | `evidence/qa-final-repository-owner.log`；独立QA复跑2项，0.473秒；包含在最终649项收集中，未冒充全库重跑 |
| DOM＋真实 API/持久化 | `node tests/test_cycle_ui.cjs` | 0 | 3 场景组/0/0 | `evidence/qa-final-ui.log`；新建、编辑、legacy 保存、恢复、状态回显、零投注任务；无失败 |
| Chromium 布局＋健康 | `node /tmp/pc28-cycle-qa-browser.cjs` | 0 | 4/0/0 | `evidence/qa-final-browser.log`、`qa-rules-390.png` 等；可复用 `node .boss/cycle-isolation/evidence/qa-browser-replay.cjs` 重放 |
| 匿名迁移复核 | 直接 Python 回放脚本，已保存为 `PYTHONPATH=src:. python3 .boss/cycle-isolation/evidence/qa-migration-replay.py` | 0 | 4/0/0 | `evidence/qa-migration-replay.json`；旧自动在途、已收工、无路由、歧义归属，重复初始化保持历史事件和账本行不变 |
| JS 语法及 Python 编译 | `node --check src/pc28touzhu/ui/auto_triggers.js`；`node --check src/pc28touzhu/ui/autobet.js`；`python3 -m py_compile src/pc28touzhu/domain/auto_trigger_cycle.py src/pc28touzhu/services/auto_trigger_cycle_service.py` | 各 0 | 3 检查/0/0 | 无错误输出 |
| diff 检查 | `git diff --check` | 0 | 1/0/0 | 无空白错误 |
| 干净基线 Boss Gate 0 | `git archive 5b37efa` 到独立临时目录，执行 `boss project init cycle-baseline` 后 `boss runtime evaluate-gates cycle-baseline gate0` | 1 | 不计测试 | `evidence/qa-baseline-gate0.log`；仅 `unsafe-patterns` 失败，发现 1 类模式，与本次一致 |

## 2. Evidence Summary / 关键路径证据

| 契约 | 真实证据 | 结论 |
|---|---|---|
| CM-001 手动/自动/各规则隔离 | runtime `test_manual_and_two_rule_states_are_independent`；QA 手动旧无route在途保护、独立恢复 | 通过 |
| CM-002 保守幂等迁移 | repository migration 组；QA 歧义 pending 结算后保持、隔日不能再开、迁移后新 manual 不误迁移 | 通过 |
| CM-003 默认/枚举/旧配置 | API 默认与非法 quota、legacy 新建拒绝；DOM legacy 保存；真实 SQLite 回读 | 通过 |
| CM-004 每日计次 | 并发 claim、外层回滚、零派单不扣、同rule不同sub额度、多route一次 | 通过 |
| CM-005 等待条件/续开 | 新证据必须晚于完成期；continuation 跳过触发冷却；fetch中配置变化与结束状态变化事务复验 | 通过 |
| CM-006 跨日 | QA 午夜旧轮保留日期/关数、完成后新日起轮quota；UI显示启动日与统计日 | 通过 |
| CM-007 同目标与多route | 同sub同目标不重复；不同sub幂等键独立；慢route终态屏障；未启动route不能稍后加入 | 通过 |
| CM-008 旧无route运行 | repository route0迁移；runtime保留旧轮进度到整轮结束；手动重置在旧轮期间拒绝；新轮implicit route账本隔离 | 通过 |
| CM-009 scoped恢复 | 匿名401、其他用户400、不在scope400、非法id400；运行/满额拒绝；保存额度不解封；legacy转rule恢复只影响所选sub | 通过 |
| CM-010 UI/API一致 | 真实表单 collect payload → 本地 WSGI → SQLite → 列表回显；手动null/0状态、错误与加载恢复、XSS名称 | 通过 |

补充定时分支已验证：同一个允许窗口可在新一期第二次启动，仍计每日额度；窗口前发布信号、窗口外与超龄信号不会启动或扣次。两条既有跨日敏感、隐式手动解封自动测试按新隔离语义更新，测试未通过改变系统时钟或跳过来规避。

## 3. Findings / 发现与修复

| 问题 | 影响 | 修复后的证据 |
|---|---|---|
| 歧义迁移的 `scope=blocked` 被结算当manual处理 | 清空未知归属 pending 并错写余额 | QA `test_ambiguous_pending_remains_blocked_after_legs_settle` 通过，保留原状态及迁移证据 |
| 过关 job 幂等键缺 subscription | 同rule不同方案共享source/target时第二方案无故零派单 | QA `test_quota_close_of_one_subscription_does_not_stop_other_subscription` 通过；key含subscription，原同群claim保留 |
| fetch期间条件已经提高仍用旧matched启动 | 用户保存新规则后按旧阈值下注 | QA `test_condition_change_during_prediction_fetch_uses_current_threshold` 通过；事务发现配置变化则等待下次检查 |
| 未知迁移状态隔日可能被日重置解封 | 含糊余额进入手动派单 | QA `test_ambiguous_migration_cannot_restart_automatically_on_next_day` 通过 |
| fetch期间另一worker完成新轮，未重新核对最新完成期 | 用刚完成期证据重复启动 | 最后追加 runtime `test_completion_during_fetch_rechecks_latest_completed_issue_in_transaction` 通过 |
| 多route乱序完成可能回退完成期号 | 后续重新使用旧证据 | 最后追加 runtime `test_last_completed_evidence_keeps_newest_route_when_older_route_finishes_last` 通过 |
| 旧无route绑定在规则停用或增加路由后被查询过滤 | 后续信号可能误作为手动下注，绕过停用或当前日风控 | 新仓储绑定归属测试2项及124项相关回归通过；旧owner识别保留，自动派单统一检查当前规则和当前日风控 |

## 4. QA Attack Checks / 覆盖率与安全审查

| 模块 | 行覆盖 | 分支覆盖 | 综合覆盖 |
|---|---:|---:|---:|
| `domain/auto_trigger_cycle.py` | 89.74% | 77.78% | 86% |
| `services/auto_trigger_cycle_service.py` | 90.91% | 79.12% | 87% |
| 两模块合计 | 90.79% | 79.00% | 87% |

实测文件：`evidence/qa-final-coverage.json`、`qa-final-coverage.txt`。未覆盖行已逐行列于 txt，主要为损坏输入与来源异常、停用/风险拒绝的部分组合、迁移恢复拒绝边界；未据此声称每个错误分支均验证。未测整个数据库大文件行覆盖或生产负载 P99，不能以页面加载 986–1346ms 当作生产性能 SLA。

新 UI 的规则/方案/路由名称、期号、金额和属性字符串在拼接 HTML 前均经过 `escapeHtml` 或固定枚举文本；错误消息使用 textContent。测试将 `<img onerror>` 等名称作为数据验证，未插入可执行节点。Gate 0 仅按是否出现 `innerHTML =` 判定，不能证明上下文 XSS；干净基线也有同告警。本报告保留该失败，不修改门禁代码或刻意改写代码绕开扫描。

root 执行的附加 OWASP 扫描结果为 failed：a07-xss 正则统计 76 处 innerHTML 赋值，包含 alerts/auth_panel 等既有模块；其余 8 类检查通过。该扫描没有分析转义上下文，不能单独证明本次引入 XSS，也不能用当前功能测试声称全仓所有旧输出已安全审计。

## 5. 交付限制

- 功能与回归验证通过，生产发布尚未执行；见 `deploy-report.md`。
- 迁移回放使用真实 SQLite 与匿名合成旧记录，没有对生产 2GB 库做迁移，因此生产耗时、存量归属完整率须在备份及维护窗口中核对。
- Legacy 无路由已运行轮次保留原账本与完整轮次进度直至整轮结束；下一新轮才进入内部继承路由。不是结算一腿或一期就中断旧轮。
- Boss 自动门禁结果不能替代上述测试证据：Gate 0 有已验证基线告警，Gate 1/2 的未识别/跳过不计通过。结论为 **功能 QA 通过，门禁有已知基线告警，未生产部署**。
