# 实验增量续算独立 QA

## 摘要

- **验证结论：通过，允许进入受控发布。** 独立10项、根代理全量1158项、最后修改后的完整规则实验室238项及三条真实浏览器流程均通过。无已知阻塞缺陷。
- **独立通过率：10 / 10，0 失败，0 跳过。** 全部在 256 MiB 地址空间硬限制下执行，39.141 秒，整组进程峰值 RSS 104,328 KiB。
- **生产副本证据：** 原批次 8 的 26 个实验定义、ID、旧日账保持，实际从 3511344 追加至测试开始时已知的 3511690；561 条新增决策，逐实验净收益/结算数/注额增量逐分对账。
- **行覆盖率未测量：** 当前环境未安装 coverage，沿用现有 unittest/真实 SQLite/WSGI/浏览器行为证据，不新增依赖或把用例数冒充行覆盖率。
- **公平调度问题已关闭：** 持久轮转游标记录实际已尝试来源，成功/失败和进程重启均前进；15项引擎专项及最终238项规则实验室回归通过。

## 测试执行记录

工作目录均为 `/www/wwwroot/pc28touzhu-lab-continuation`。本表只记录实际执行或根代理提供明确退出码且日志已核对的结果。全量、专项和独立测试存在重叠，分别列示，不相加虚报总通过数。

| 范围 | 命令 | 退出码 | 通过/失败/跳过 | 失败详情 |
|---|---|---:|---|---|
| 最初独立反例准备 | `env PYTHONPATH=src RULE_LAB_MIGRATION_SNAPSHOT=/tmp/pc28-continuation-baseline.db python3 -m unittest tests.test_rule_lab_continuous_qa -v` | 1 | 3 / 2 / 0 | 当时 worker 连续采集尚未接入，缓存污染与三段修订两例在采集水位处失败；最终已通过 |
| 缺失旧暖机预测红测 | `env PYTHONPATH=src python3 -m unittest tests.test_rule_lab_continuous_qa.ContinuationIndependentQaTests.test_missing_legacy_warm_prediction_is_not_invented_as_a_known_no_signal -v` | 1 | 0 / 1 / 0 | 删除1099期缓存预测后仍 eligible=true，错误地将缺行当作已知无信号 |
| 同一暖机红测修复后 | 同上一命令 | 0 | 1 / 0 / 0 | 无；现在返回 warm_prediction_missing，禁止无证据接管 |
| 12个月追加与一年停机补算 | `env PYTHONPATH=src python3 -m unittest tests.test_rule_lab_continuous_qa.ContinuationIndependentQaTests.test_twelve_monthly_appends_keep_twenty_six_versions_and_original_baseline -v` | 0 | 1 / 0 / 0 | 无；5.254秒 |
| 最终独立 QA（含副本） | 下方完整256 MiB命令 | 0 | 10 / 0 / 0 | 无；39.141秒 |
| 根代理全量回归（公平性最终修改前启动） | `env PYTHONPATH=src RULE_LAB_MIGRATION_SNAPSHOT=/tmp/pc28-continuation-baseline.db RULE_LAB_READONLY_SOURCE_DB=/www/wwwroot/AITradingSimulator/pc28_predictor.db python3 -m unittest discover -s tests -q` | 0 | 1158 / 0 / 0 | 无；328.548秒 |
| 多来源公平性红测 | `env PYTHONPATH=src python3 -m unittest tests.test_rule_lab_continuous_worker.ContinuousWorkerTests.test_capture_budget_rotates_after_the_stream_actually_processed tests.test_rule_lab_continuous_worker.ContinuousWorkerTests.test_capture_failure_rotates_before_the_next_worker_restart -q` | 1 | 0 / 2 / 0 | 三次重启采集尝试为[1,1,1]，预期[1,2,3]；已修复 |
| 公平性修复后引擎区域回归 | `env PYTHONPATH=src python3 -m unittest tests.test_rule_lab_continuous_worker -q` | 0 | 15 / 0 / 0 | 无；17.099秒 |
| 最后修改后的全部规则实验室测试 | `env PYTHONPATH=src RULE_LAB_MIGRATION_SNAPSHOT=/tmp/pc28-continuation-baseline.db RULE_LAB_READONLY_SOURCE_DB=/www/wwwroot/AITradingSimulator/pc28_predictor.db python3 -m unittest discover -s tests -p 'test_rule_lab*.py' -q` | 0 | 238 / 0 / 0 | 无；116.141秒 |
| 原规则实验室浏览器 | `node tests/test_rule_lab_ui.cjs` | 0 | 1流程 / 0 / 0 | 无；真实WSGI创建/暂停/运行/结算/CSV/响应丢失/权限/XSS/手机键盘 |
| 原金额重测浏览器 | `node tests/test_rule_lab_amount_ui.cjs` | 0 | 1流程 / 0 / 0 | 无；原金额行为及手机布局保持 |
| 新增量续算浏览器 | `node tests/test_rule_lab_continuation_ui.cjs` | 0 | 1流程 / 0 / 0 | 无；只读预览/默认关闭/分页采集/手动与自动/暂停/26ID/历史段月报/幂等恢复/组归属/桌面手机 |


独立最终命令：

```bash
env PYTHONPATH=src \
  RULE_LAB_MIGRATION_SNAPSHOT=/tmp/pc28-continuation-baseline.db \
  RULE_LAB_READONLY_SOURCE_DB=/www/wwwroot/AITradingSimulator/pc28_predictor.db \
  RULE_LAB_QA_EVIDENCE_PATH=/tmp/pc28-continuation-production-copy-qa.json \
  python3 - <<'PYQA'
import resource
import unittest
resource.setrlimit(resource.RLIMIT_AS, (256*1024*1024, 256*1024*1024))
suite = unittest.defaultTestLoader.loadTestsFromName('tests.test_rule_lab_continuous_qa')
result = unittest.TextTestRunner(verbosity=2).run(suite)
print('MAX_RSS_KIB', resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
raise SystemExit(not result.wasSuccessful())
PYQA
```

根代理门禁日志：`/tmp/pc28-continuation-full-root.log`、`/tmp/pc28-continuation-capture-fairness-red.log`、`/tmp/pc28-continuation-capture-fairness-green.log`、`/tmp/pc28-continuation-final-lab-root.log`、`/tmp/pc28-continuation-existing-browser.log`、`/tmp/pc28-continuation-amount-browser.log`、`/tmp/pc28-continuation-ui-flow-final.log`。

独立日志：`/tmp/pc28-continuation-qa-red.log`、`/tmp/pc28-continuation-qa-warm-gap-red.log`、`/tmp/pc28-continuation-qa-warm-gap-green.log`、`/tmp/pc28-continuation-qa-twelve-months.log`、`/tmp/pc28-continuation-qa-final.log`。

## 独立反例及真实产物

1. 并发同一预览及 request_id 接管：恰好一个 created=true、一个幂等返回，单个系列/基线，原资金、日账、轮次不变。
2. 捕获预算不足：storage_limit 后全部表行哈希保持，未发布半页输入或提前推进水位。
3. 普通缓存污染：相同期号的冲突缓存不替换冻结档案，连续回放经济状态与控制组一致。
4. 三段修订隔离：A 已消费后源修订 A，B 与 C 的暖机继续使用原预测值；没有在 C 通过较新的全局 revision 间接引入修订。
5. 同一预测的结算元数据：pending→settled 在下一段 B 可见，保留实际 settled_at；旧检查点字节保持，方向修订仍被隔离。
6. 旧暖机缺失：缓存只剩开奖、缺少预测证据时拒绝接管，不伪造 no_prediction。
7. 历史缺档公平性：不可恢复的已消费旧期缺口仍记录 archive_gap，但新输入继续被保存；不会每次重试旧页而丢失后续来源数据。
8. 12 次逐月追加：原26实验变为13段（含基线），实验总数仍26；原日期及检查点保持，累计跨度超过一年。另一个自动系列模拟离线一年，按每片≤90天追赶至昨日，重启不重复记账或建段。
9. 生产一致副本双次迁移：全部旧表旧列逐行哈希保持、quick_check/FK 通过、series 默认为0、26版本与下一期3511344正确，GET预览无写入。
10. 生产副本实际追加：只读真实源，副本写入新段；原定义/旧日账保持，每实验累计差值等于新增已结决策合计。

生产副本最后一次追加证据：`/tmp/pc28-continuation-production-copy-qa.json`。记录时已知最高期3511690，计算同样到3511690；10个tick，最长2033.687ms，副本阶段峰值RSS66,116KiB，实验目录28,422,136字节。2秒是协作式tick预算，单笔事务完成可带来约34ms尾部超出；仍在256MiB/1GiB约束内。

所有生产副本操作都先从固定23MiB快照复制到临时目录；原快照文件字节哈希保持。上游仅通过mode=ro读取，没有复制2GiB源库、迁移源库、修改真实下注账本或发送消息。

## 契约与风险审查映射

| 契约/风险 | 验证依据 | 当前状态 |
|---|---|---|
| CM-001/002/009，R6/R8 | series默认关闭、API GET零写入/幂等、独立副本迁移、12个月26ID稳定 | 独立、全量及最终区域回归通过 |
| CM-003/004，R2/R3 | archive源删除/版本冻结/日期清理证据、旧暖机未知缺样拒绝、Engine无上游旧输入仍续算 | 对应实现已审查，最终238项区域测试通过 |
| CM-005，R5/R8 | quota原子性、暂停>90天输入保护、压缩哈希、递归目录预算、256MiB实际副本运行 | 独立通过 |
| CM-006，R1 | Engine轮次/逐期同前沿全量vs分段、晚结果跨月、预算中断、多页pending；独立三段修订/元数据 | 最终区域测试通过 |
| CM-007/008，R6 | manual/auto分离、持久幂等、今日已知最高期追平、昨日自动截止、一年停机90天分片、pause不自动恢复 | 独立长期/副本、公平调度专项及最终区域回归通过 |
| CM-010，R4/R6 | study共同manifest/截止、100运行槽、child禁止独立追加、运行中比较不可比 | Engine/API用例及最终区域回归通过 |
| CM-011/012，R7 | 月报time_complete/pending/coverage分离、旧段不可变、>90天新金额档案真实重算 | Monthly/API用例及最终区域回归通过 |
| CM-013 | 匿名401、非管理员403、他人404、child409、16KB、no-store、仅segments查询白名单、丢失响应原ID恢复 | API及三条真实浏览器流程通过 |
| CM-014 | 生产副本迁移/资源/默认关闭；实际部署仍由根代理执行 | 副本通过，线上核验不在本阶段冒充完成 |

## 浏览器与边界说明

已目视检查 `/tmp/pc28-continuation-desktop.png` 和 `/tmp/pc28-continuation-mobile.png`：1440/390界面可读，26版本、计算/采集/结算水位、pending与开关语义清晰，未观察到裁切或重叠。三条自动化真实WSGI浏览器由根代理统一执行，均exit0；已核对其实际日志。

- 模拟不是实盘下单；验收未调用真实发送。
- 长期新金额重测使用当前已保存档案的冻结视图重新计算；不承诺与历史所有段最初使用的不同输入版本完全相同，页面应保留该口径说明。
- 未采集且已清理的数据不能恢复；24个月是空间预算内保留目标，不能保证无限来源/无限离线时长。
- 变更行覆盖率、跨操作系统浏览器与长期真实时间浸泡未测量；本次用确定性跨年/重启/故障场景、真实SQLite和资源硬限制覆盖关键正确性。


## 放行结论

R1～R8 的关键正确性风险均有实现审查与可执行行为证据，最后的采集/到期轮转问题已通过红→绿及完整区域回归。数据结构迁移、生产副本追加和既有UI回归已验证，可以由根代理执行授权的实验库加法发布与线上只读核验。

本报告不声明生产已部署或现有实验已开启自动续算；线上核验由部署报告记录。新功能默认关闭，迁移不会创建或启动所有旧实验的自动任务。

**STATUS: DONE_WITH_CONCERNS**

**REASON:** 所有阻塞测试通过；行覆盖率、跨操作系统浏览器和长期真实时间浸泡未测量，已明确范围，不影响本次关键正确性门禁。
