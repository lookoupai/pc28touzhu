# 轻量规则实验室独立 QA 报告

## 摘要

- **验证结论：通过，允许进入发布。** 完整 Python 回归 934 项通过、0 失败、0 跳过；独立真实浏览器流程通过。没有待修复的 P0/P1 问题。
- 新增五个 Python 模块语句覆盖 **90.23%（1081/1198）**；每个模块语句覆盖均超过 80%。分支覆盖77.73%，合并显示86.68%，三者不可混称。
- 100个运行实验的离线前向流程峰值 RSS 约33.6MiB；冻结判断 P50 68.34ms、P99 85.10ms，结算 P50 44.22ms、P99 76.16ms。实际运行仍以systemd资源额度为准。
- 历史400期回放分别验证50/100/500个保存版本；500个版本按最多100运行分组，没有绕过全局额度。临时源文件哈希保持不变。
- 未执行长时间浸泡、生产并发压测或 Lighthouse；本报告不宣称生产 SLA、内存长期无泄漏或前端性能评分。全部负载测试使用临时数据库；生产来源仅有限只读解析。

## 环境与证据范围

日期2026-11-26，工作树 `/www/wwwroot/pc28touzhu-rule-lab`。现有栈为Python标准库WSGI、SQLite和原生页面；没有安装依赖，没有写生产源库、生产投注账本，没有发送投注消息。

完整回归运行时包含原基线843项及新增91项。QA新增 `tests/test_rule_lab_qa.py` 共7个测试；其中三球枚举测试执行16000个逐注结算对照，但仍只计为一个测试，不膨胀用例数量。浏览器脚本作为一个端到端工作流脚本计数，其多个断言不当作多个独立测试。

长期证据随代码保存：

- `evidence/benchmark.json`：历史回放、存储增长、WSGI响应时间的原始小型结果。
- `evidence/live-benchmark.json`：实际live代码路径的临时数据测量。
- `evidence/coverage-summary.txt`：逐模块覆盖及未覆盖行；完整coverage数据库/JSON留在服务器`/tmp`，不提交。

## 测试执行记录

所有命令在工作树根目录执行。下列通过计数有重叠，不相加为独立测试总数。

| 范围 | 完整命令 | 退出码 | 通过/失败/跳过 | 失败详情与证据 |
|---|---|---:|---|---|
| 独立边界 | `env PYTHONPATH=src python3 -m unittest tests.test_rule_lab_qa -v` | 0 | 7/0/0 | 无；3.771秒，`/tmp/pc28-rule-lab-qa-boundaries.log` |
| 完整回归及新增模块覆盖 | `nice -n 10 env PYTHONPATH=src:/tmp/pc28-cycle-test-tools COVERAGE_FILE=/tmp/pc28-rule-lab-qa.coverage python3 -m coverage run --branch --source=pc28touzhu.domain.rule_lab,pc28touzhu.services.rule_lab_store,pc28touzhu.services.rule_lab_source,pc28touzhu.services.rule_lab_service,pc28touzhu.rule_lab_worker -m unittest discover -s tests -p 'test_*.py' -q` | 0 | 934/0/0 | 无；187.181秒，`/tmp/pc28-rule-lab-qa-full.log` |
| 浏览器完整流程 | `env PYTHONPATH=src node tests/test_rule_lab_ui.cjs` | 0 | 1/0/0 | 无；`/tmp/pc28-rule-lab-qa-browser.log` |
| 历史资源测量 | `nice -n 10 env PYTHONPATH=src python3 -m tests.rule_lab_benchmark --issues 400` | 0 | 3/0/0 | 无；50/100/500版本，`evidence/benchmark.json` |
| 前向资源测量 | `nice -n 10 env PYTHONPATH=src python3 -m tests.rule_lab_benchmark --live-only` | 0 | 3/0/0 | 无；50/100/500保存版本，`evidence/live-benchmark.json` |
| 覆盖报告 | `env PYTHONPATH=src:/tmp/pc28-cycle-test-tools COVERAGE_FILE=/tmp/pc28-rule-lab-qa.coverage python3 -m coverage report -m` | 0 | 1/0/0 | 无；`evidence/coverage-summary.txt` |
| QA脚本语法 | `python3 -m py_compile tests/test_rule_lab_qa.py tests/rule_lab_benchmark.py` | 0 | 2/0/0 | 无 |
| 变更格式 | `git diff --check` | 0 | 1/0/0 | 无 |

coverage环境没有C tracer，实际使用Python tracer执行；警告不是跳过测试。统计覆盖时另对真实worker启动器执行 `--once`、持有flock时第二实例退出、SIGTERM正常结束3个临时库进程检查，结果3/0/0；退出码依次0、预期1、0，且不存在的源库没有被创建。日志`/tmp/pc28-rule-lab-qa-launcher.log`。这些追加检查只为覆盖独立进程启动路径，没有修改业务代码或重复全套测试。复现命令框架如下（给每次执行分配独立临时目录；单实例锁用 `fcntl.flock` 持有，SIGTERM通过 `subprocess.Popen.send_signal` 发送）：

```bash
RULE_LAB_DATA_DIR=/tmp/isolated-rule-lab-qa/lab RULE_LAB_SOURCE_DB=/tmp/isolated-rule-lab-qa/absent.db \
PYTHONPATH=src:/tmp/pc28-cycle-test-tools COVERAGE_FILE=/tmp/pc28-rule-lab-qa.coverage \
python3 -m coverage run --append --branch \
  --source=pc28touzhu.domain.rule_lab,pc28touzhu.services.rule_lab_store,pc28touzhu.services.rule_lab_source,pc28touzhu.services.rule_lab_service,pc28touzhu.rule_lab_worker \
  -m pc28touzhu.rule_lab_worker --once
```

## 独立验证结论

| Contract | 已验证证据 |
|---|---|
| CM-1 两套OK配对模板 | 领域/API/浏览器验证大小单双50、组合78版本；只允许两个规则ID，保存后暂停 |
| CM-2 正确结算 | QA对1000种三球、8个方向、两规则共16000笔用独立规则判断实赔/退本/全亏及生产逐注round2；高赔完整赔率不预截断 |
| CM-3 时间与前瞻 | 独立两世界测试改变目标结果及未来后缀，目标及更早的入场决策完全一致；runtime测试真实冻结时刻、40秒截止、晚到开奖/预测、缺期、停机不回填 |
| CM-4 状态与幂等 | 分页1与200、每个tick重启两种回放成绩完全一致；并发暂停/revision、防重水位、已冻结方向修订仍结算、旧pending清算、较早冲突阻断后期结算 |
| CM-5 权限 | 实际WSGI与浏览器覆盖匿名401、普通用户403、其他管理员资源404；24种JSON字段类型组合返回400，无部分批次生成 |
| CM-6 页面闭环 | 真实临时平台库、源库、实验库，生成→启动→暂停→配对成绩→每日及最近明细→CSV；丢失创建响应后核对不重提；恶意名称作为文本；键盘焦点和窄屏无页面横向溢出 |
| CM-7 资源与清理 | 两项独立并发测试分别验证100运行、500保存上限在事务中成立；清理保留累计与防重水位、最近100期及pending；旧回放按本次入账时间保留明细；soft预算仍允许pending结算；systemd上限与单实例测试 |
| CM-8 源安全与共享 | 测试源文件哈希在回放与live tick前后不变；共享撤销停止新决策、旧pending仍结算；只读索引分页、查询超时、合法pending→settled和三球校验；生产有限样本解析通过 |

只读审查曾发现“预测冲突误清除纯开奖特征”和“早期冲突被后期结算越过”两个问题，已由core修复并进入本次完整回归。主代理另发现的回放明细立即过期、磁盘预留结算、清理吞吐等问题也已修复并纳入934项。没有为通过测试删除断言或跳过失败项。

## 覆盖率

| 模块 | 已覆盖/语句数 | 语句覆盖 |
|---|---:|---:|
| domain.rule_lab | 203/212 | 95.75% |
| rule_lab_worker | 288/342 | 84.21% |
| services.rule_lab_service | 78/80 | 97.50% |
| services.rule_lab_source | 205/230 | 89.13% |
| services.rule_lab_store | 307/334 | 91.92% |
| 合计 | 1081/1198 | **90.23%** |

分支370/476=77.73%；含分支的coverage显示86.68%。未覆盖主要是极端数据格式、操作系统I/O异常、某些时间预算中断路径与main连续失败退避组合。逐行清单见`coverage-summary.txt`。覆盖范围不含整个原有WSGI大文件及JS语句覆盖；新增接口和页面通过功能测试验证，不能把90.23%泛称全仓覆盖。

## 资源测量

每个规模在新子进程执行，使用真实独立SQLite与worker。预测由确定性fixture提供，不调用AI模型。测量过程没有启用systemd的0.2核节流，因此历史墙钟不能当成生产完成时间。400期是约一天的测试输入，候选触发密度取决于fixture，并非每个候选每期都会投注。

| 保存版本 | 同时运行峰值 | 历史400期墙钟 | CPU时间 | 回放峰值RSS | 初始实验库 | 完成后实验库 | 新增明细 |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 50 | 50 | 7.114s | 5.919s | 33.98MiB | 284KiB | 1.21MiB | 3450 |
| 100 | 100 | 15.640s | 12.739s | 34.23MiB | 492KiB | 2.16MiB | 6900 |
| 500 | 100 | 73.859s | 60.830s | 34.73MiB | 2.06MiB | 9.85MiB | 34500 |

单tick最大观测约2.015～2.031秒；2秒是步骤间检查的软预算，当前短事务会完成，并非恰好2秒强制杀进程。systemd CPUQuota/MemoryMax才是运行资源控制。500版本由5组顺序运行，不是500个在线并发。

前向测量单独走“已有100期历史→新pending信号→冻结决策→新增开奖→结算”代码路径，各20期，并补20次没有新信号的tick。100版本结果：

| 路径 | P50 | P99 | 样本数 |
|---|---:|---:|---:|
| 冻结判断tick | 68.340ms | 85.095ms | 20 |
| 结算tick | 44.221ms | 76.156ms | 20 |
| 无新信号tick | 19.208ms | 27.632ms | 20 |

该前向样本峰值RSS33.61MiB，全部60次tick CPU时间合计2.2483秒。用虚拟时钟推进，不做真实数小时等待；P99在20个样本中基本是最大值，不能推断长期尾延迟。

WSGI性能使用实际平台SQLite用户查询、Cookie校验、独立账本与响应序列化；每个端点预热10次后测100次。三个保存规模中overview P99为3.924～4.267ms、batch results P99为8.314～9.800ms。没有网络往返、反向代理或并发压力，结果仅表明本地有界查询未触发历史重算。

保留策略不能保证磁盘永不增长：成绩和日汇总持续积累，实际明细密度不同，且SQLite删除后复用页不会立刻缩文件。每5分钟最多清理500行，在预算内分批处理；接近1GiB预算会停止新模拟，保留已有成绩。这里不把样本库增长外推为固定年费或永久容量承诺。

## 生产来源有限只读检查

以 `RuleLabSource('/www/wwwroot/AITradingSimulator/pc28_predictor.db')` 调用公开来源列表、latest_draw、history_draws和get_predictions；合计读取数据行上界160，小于200。15个合格来源，解析70条三球开奖及20条预测，0解析错误。首次选中的来源近期没有预测，返回空列表；改用另一合格来源后20条均可解析，不补造缺失预测。两次样本检查分别9.857ms和8.179ms，exit0，无写入或worker回放。

EXPLAIN QUERY PLAN确认开奖分页使用 `idx_draws_issue (lottery_type,issue_no)`，预测按 `sqlite_autoindex_predictions_1 (predictor_id,issue_no)` 查询；没有日期全表过滤。临时详细证据：`/tmp/pc28-rule-lab-qa-source-smoke.json`、`/tmp/pc28-rule-lab-qa-source-prediction-smoke.json`。报告未包含API凭证、原始预测正文或用户私密数据。

## 未验证与发布边界

- 未做生产高峰或长时间浸泡测试。首次上线仍应观察真实worker RSS、CPU、库增长、心跳及业务响应，当前测量只支持轻量首版的容量判断。
- 未运行 Lighthouse，不声称达到某个Lighthouse分数；桌面1440和手机390宽度的真实页面已实际检查，浏览器无console/page错误，键盘及布局断言通过。
- 历史源不是不可变预测版本档案，回测始终标记为历史回放；成绩不是稳定盈利证明。前向决策有实际观察与冻结时刻，但模拟不含群组拒单、网络派单和实盘限额。
- 当前QA未部署生产服务。生产启用、Git提交与GitHub备份由主代理/DevOps完成并写部署报告；本报告不会把测试通过写成已经上线。

## 发布期间发现的 WAL 权限问题与定向复验

2026-11-26，发布后在真实 systemd 挂载命名空间发现：上游最后一个 SQLite 连接关闭后可能删除 `-wal`／`-shm`；源目录整体只读时，即使使用 `mode=ro`，连接也无法创建缺失的同步辅助文件。因此此前普通进程里的源只读测试通过，不等于完整生产沙箱权限已经验证。

经主代理授权，运维修正只涉及unit访问策略、相应测试和文档：源目录允许SQLite管理WAL／SHM，源主数据库 `pc28_predictor.db` 仍单独只读挂载；所有源连接继续 `mode=ro` 与 `PRAGMA query_only=ON`。两项目 `.env` 和生产投注账本对worker不可读，网络隔离保持。QA只读复核diff确认无应用结算/预测/账本逻辑改动。

该修复有明确的权限边界：源目录中未单独保护的其他文件仍可由服务用户写入，不能宣称“只有实验目录可写”，也不能宣称写权限精确限定为两个辅助文件名。源库主文件若被维护流程替换inode，需重启worker重新绑定。实际业务SQL写入仍被拒绝；没有使用`immutable=1`或`nolock=1`绕过活库一致性。

| 定向检查 | 命令 | 退出码 | 通过/失败/跳过 | 结果 |
|---|---|---:|---|---|
| 运维与源回归 | `env PYTHONPATH=src python3 -m unittest tests.test_rule_lab_ops tests.test_rule_lab_source -q` | 0 | 16/0/0 | 0.377秒，`/tmp/pc28-rule-lab-qa-wal-regression.log` |
| unit校验 | `systemd-analyze verify deploy/systemd/pc28touzhu-rule-lab.service` | 0 | 1/0/0 | 无错误输出 |
| diff格式 | `git diff --check` | 0 | 1/0/0 | 无错误输出 |

另只读复核DevOps实际命名空间证据 `/tmp/pc28-rule-lab-sandbox-probe.json`：进入运行worker的mount/net命名空间并降权为www，起始WAL／SHM均不存在时，15个公开来源与5条开奖读取成功，耗时5.654ms；主数据库挂载只读，`O_WRONLY`打开被errno30拒绝，零行SQL UPDATE也被只读保护拒绝；源`.env`、平台`.env`、投注账本均不可读，实验目录可写。该probe由DevOps执行、QA读取证据确认，没有把它记成QA再次执行。

原934项完整回归记录仍是权限修复前的真实记录；本修复后只执行上述16项相关回归，没有宣称重新跑过934项。上线服务和Git版本的最终状态以部署报告为准。该问题已修复，无新增阻塞项。

## 状态

**STATUS: DONE_WITH_CONCERNS**

**REASON:** 功能、回归、权限、结算和有界资源验收全部通过；生产长期负载、Lighthouse以及真实下注链路不在本次已验证范围。
