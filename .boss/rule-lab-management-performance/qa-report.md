# 实验管理、性能与空间回收独立 QA

## 当前结论

**W4最终QA与W5发布验收均通过，所有门禁已完成。** 冻结版本全项目1,197项通过、0失败、0跳过，独立专项及真实浏览器通过，`390aa45`已上线；CM9确认原数据、602个实验ID、队列和运行进度保持，回放继续前进。

- 冻结版本完整回归 **1,197通过、0失败、0跳过**，329.800秒，真实进程退出码0。
- 独立反例 **8通过、0失败、0跳过**，覆盖并发删除、批次/实验/组/系列ID、共享来源、超预算清理、队列编辑竞态、维护前置守卫和真实VACUUM中断。
- 本轮实际找到并推动修复两个缺陷：分页删除复用`lab_series.id`可能误删另一新系列；systemctl状态查询失败仍放行维护转换。两个反例均有失败证据和最终通过结果。
- 实际应用CLI在生产一致快照的隔离副本完成v4→v5加法迁移、NONE→INCREMENTAL转换、外置备份及恢复验证；22张原表832,227行全部原列哈希保持。
- 普通100版本、92组合、连续档案三种相同负载经济摘要均与旧版相同；实测吞吐分别约1.78×、1.82×、1.46×，不是生产速度保证。
- 行覆盖率未测量，未安装依赖；不以用例数或功能覆盖表冒充行覆盖率。

## 环境与数据边界

开发工作树 `/www/wwwroot/pc28touzhu-lab-management`，基线`20cb785`，Python标准库unittest/SQLite WAL及已有Playwright/Chromium。本轮独立QA仅写临时数据库或生产一致快照的副本，未修改生产实验、真实下注或上游预测库。所有性能比较使用固定fixture输入、模拟时钟、单CPU affinity和256MiB地址空间限制；转换证明同样限制256MiB。

测试数量按每条命令分别记录，多个集合存在重叠，不加总为独立通过数量。

## 已执行测试证据

| 范围 | 命令/执行入口 | 退出码 | 通过/失败/跳过 | 证据 |
|---|---|---:|---|---|
| 全项目最终冻结版本 | `unittest discover -s tests -q`，含下述生产副本/只读来源环境变量 | 0 | 1197/0/0，329.800秒 | `/tmp/pc28-management-full-final-root.log`；根代理session21060实际exit0 |
| 全项目首轮（运行期间CLI发生修复，非最终冻结版本） | `unittest discover -s tests -q`，含生产副本/只读来源环境变量 | 1 | 1193/1/0，353.180秒 | `/tmp/pc28-management-full-root.log`；唯一失败为已导入旧CLI的unknown-writer-state反例 |
| 独立管理与转换反例，最终 | `env PYTHONPATH=src python3 -m unittest tests.test_rule_lab_management_qa -v` | 0 | 8/0/0，1.519秒 | `/tmp/pc28-management-qa-final-targeted.log` |
| Data管理/旧行为回归 | 数据组合命令A（表后列出） | 0 | 99/0/0，25.755秒 | `/tmp/pc28-management-data-final.log` |
| 真实存储/CLI状态矩阵补测 | `PYTHONPATH=src python3 -m unittest discover -s tests -p test_rule_lab_storage.py -v` | 0 | 5/0/0，0.624秒 | `/tmp/pc28-management-data-cli-final.log` |
| 管理真实WSGI接口 | `env PYTHONPATH=src python3 -m unittest tests.test_rule_lab_management_api -v` | 0 | 9/0/0，10.900秒 | `/tmp/pc28-management-api-integration.log` |
| Worker优化与旧连续行为 | `env PYTHONPATH=src python3 -m unittest tests.test_rule_lab_performance tests.test_rule_lab_worker tests.test_rule_lab_continuous_worker -q` | 0 | 48/0/0，24.489秒 | `/tmp/pc28-management-worker-integrated.log` |
| 实际应用CLI生产快照副本 | `env PYTHONPATH=src python3 /tmp/pc28-management-app-storage-qa.py` | 0 | 完整断言脚本通过；不是unittest用例数 | `/tmp/pc28-management-app-storage-qa.json`及`.log` |
| 原始SQLite有界回收证明 | `python3 /tmp/pc28-management-storage-copy-qa.py` | 0 | 完整断言脚本通过 | `/tmp/pc28-management-storage-copy-qa.json`及`.log` |
| 管理真实浏览器最终验收 | `node tests/test_rule_lab_management_ui.cjs` | 0 | 完整交互脚本通过 | `/tmp/pc28-management-ui-final.log`，由已完成工具stdout/exit0归档，未重跑 |
| 既有实验真实浏览器回归 | `node tests/test_rule_lab_ui.cjs` | 0 | 脚本通过 | `/tmp/pc28-management-old-browser.log` |
| 既有金额重测真实浏览器回归 | `node tests/test_rule_lab_amount_ui.cjs` | 0 | 脚本通过 | `/tmp/pc28-management-amount-browser.log` |
| 既有连续实验真实浏览器回归 | `node tests/test_rule_lab_continuation_ui.cjs` | 0 | 脚本通过 | `/tmp/pc28-management-continuation-browser.log` |

数据组合命令A由Data owner从当次原始执行记录核对：

```bash
env PYTHONPATH=src python3 -m unittest tests.test_rule_lab_management tests.test_rule_lab_storage tests.test_rule_lab_management_qa tests.test_rule_lab_batch_queue tests.test_rule_lab_series tests.test_rule_lab_archive tests.test_rule_lab_store tests.test_rule_lab_comparison tests.test_rule_lab_risk tests.test_rule_lab_continuous_monthly -q
```

该次99项执行早于最终新增CLI/独立反例，同命令在最终版本的用例数可能增加；表中99/99只对应原日志时间点。

根代理首轮全项目回归1194项中仅`test_unknown_writer_state_cannot_start_offline_conversion`失败：进程开始时导入了旧CLI，运行期间新增反例及后续修复不会替换该进程已导入模块。该失败已由冻结后的8项独立专项和5项存储/CLI专项重新验证通过。随后根代理在同一冻结版本重新执行完整回归，1,197项全部通过、0失败、0跳过，耗时329.800秒，实际session21060退出码0。命令为`env PYTHONPATH=src RULE_LAB_MIGRATION_SNAPSHOT=/tmp/pc28-continuation-baseline.db RULE_LAB_READONLY_SOURCE_DB=/www/wwwroot/AITradingSimulator/pc28_predictor.db python3 -m unittest discover -s tests -q`，最终日志`/tmp/pc28-management-full-final-root.log`。首轮失败结果保留如实记录，发布依据为最终冻结版本完整通过结果。

## 独立反例与缺陷闭环

1. 同一删除预览的两个线程并发请求，只能对应同一个job或其中一个409，不能重复物理清理；完成后容量和外键一致。
2. 删除最高ID比较组及所有子批次/实验后，重新初始化再建组，三个ID范围均严格增加；同账号删除回执可恢复，跨账号404，原参考批次保留。
3. 最后一个连续series删除时，另一个账号的未启动普通批次仍保护共享预测及输入档案；最后一个普通消费者删除后才允许回收共享输入。
4. 现有数据库超过预算时，删除job仍能分批前进，实际清除目标对象并释放版本容量。
5. 队列读取资格后、启动前用户原地编辑，新定义必须保持暂停且退出原队列；Data补了原子`start_queued_batch`复核。
6. **实际阻塞缺陷**：旧series物理删除而job尚未清完时新建另一个series，旧实现复用最高series ID。job仍持有旧ID，后续可能误删新series。原反例失败`1 not greater than 1`，见`/tmp/pc28-management-qa-series-reuse-red.log`。持久ID分配覆盖series两处创建入口后，最终测试继续执行旧job到完成，并确认新系列enabled/capture_enabled、其segment仍保留、外键无错误。
7. **实际维护守卫缺陷**：systemctl连接失败返回空状态时仍调用转换。原反例`SystemExit not raised`，见`/tmp/pc28-management-cli-unknown-state-red.log`。修复后只有明确inactive/failed且退出码3允许继续，未知状态/错误/超时拒绝；独立反例与Data全状态矩阵均通过。
8. 用SQLite真实progress_handler在VACUUM执行中触发`SQLITE_INTERRUPT`，确认源库事务回滚、2,000行原记录完整、外置备份可读取，二者mode仍0、quick_check通过、外键无错误。

最初管理方法尚未接入时的5项失败记录在`/tmp/pc28-management-qa-red.log`，属于测试准备阶段，不能冒充实际功能缺陷。首次5项通过记录在`/tmp/pc28-management-qa-green-first.log`；之后发现的新缺陷再经历上述失败→修复→最终8项通过。

## 实际维护CLI与生产副本证明

脚本只读取`/tmp/pc28-management-baseline.db`，另建临时目录内副本。使用独立于应用实现的按主键排序、逐行SHA256计算原表原列，避免80多万行同时进入内存；原快照大小及文件SHA256全过程保持。

| 检查 | 实测结果 |
|---|---|
| 原数据 | 22张表，832,227行，219,639,808字节，schema4/auto_vacuum0 |
| v5加法初始化 | 0.010秒；全部原列哈希保持，仍为auto_vacuum0，未隐式VACUUM |
| 真实CLI默认预览 | 退出0；数据库文件哈希保持，未创建备份 |
| 真实宿主活跃服务下apply | 退出2；拒绝转换，未创建备份，文件哈希保持 |
| 隔离转换 | 仅模拟systemctl三服务明确inactive(exit3)，实际CLI参数/备份/SQLite/VACUUM/完整性/哈希代码全部真实执行 |
| 实际CLI转换耗时 | 含备份、两次原列哈希和检查共16.834秒 |
| 转换后 | schema5、auto_vacuum2，200,114,176字节；WAL保持；quick_check=ok、外键错误0 |
| 备份 | 219,672,576字节，位于实验目录外、权限0600；schema5/auto_vacuum0，原列哈希一致 |
| 恢复 | 把备份复制至另一临时目录并initialize，全部原列哈希仍相同 |
| 资源 | 256MiB地址空间限制，峰值RSS60,124KiB；脚本总计46.761秒 |

另一个原始SQLite证明先在副本增加8,667,136字节专用临时数据，再删除并调用`incremental_vacuum(256)`且完整消费游标：实际释放256个freelist页、257个物理页（包括指针映射页），文件缩小1,052,672字节，单步0.007秒。最初脚本将物理页减少误断言为最多256，已按freelist语义修正观测；应用功能门禁没有放宽。

Data真实存储测试进一步验证新增库模式2、分批回收后文件缩小且存留记录不丢、读事务占用时job为waiting且freed_bytes=0、释放读事务后继续完成、低磁盘空间预检拒绝。未拿生产服务做中断演示。

## 性能等价与收益

使用同一benchmark在旧`20cb785`与最终管理维护已接入的代码上运行，不将cProfile计时作为倍速分母。

| 负载 | 旧版秒 | 新版秒 | 吞吐倍率 | 摘要检查 |
|---|---:|---:|---:|---|
| 大小/单双100版本、800批次期数，两次均值 | 44.9141 | 25.2016 | 1.78× | 6,981条决策，经济摘要相同 |
| 92组合、200期 | 13.1270 | 7.2206 | 1.82× | 664条决策，经济摘要相同 |
| 连续档案100版本、400批次期数 | 31.1258 | 21.2883 | 1.46× | 含基线7,075条决策，经济摘要相同 |

比较覆盖逐笔、实验stats/risk_runtime、下注与结算水位、日账、日风险、轮次、pending与处理游标；没有减少版本数、事件或风控来换时间。最终样本峰值RSS约41～50MiB。连接复用限于单tick，每个读写事务正常结束，外部暂停和删除仍受revision复核。详细CPU耗时、哈希、重复样本及复现命令见`performance.md`和`/tmp/pc28-management-performance-comparison.json`。

## CM9生产发布验收

根代理已完成受控发布与线上只读核验，本节仅归档既有结果，没有重新运行测试。发布版本`390aa4508c8c8ad18073f7a4a5c7f3fce27d75bf`，rollout session77762退出0，报告`/tmp/pc28-management-rollout-20261127T155206Z.json`。三个实验写者在71.946秒恢复；停写快照的22张旧表、1,739,112行全部原列哈希相同，602个实验ID、队列、状态、时钟与游标保持，schema5/auto_vacuum2生效。worker仍为1核/256MiB，队列仍为5% CPU/128MiB；上游进程PID1250128保持。

随后online session82220退出0，证据`/tmp/pc28-management-online.json`：1440桌面和390手机通过，只读检查0个POST、0个页面错误，管理与删除预览没有修改实验；执行器在线且最大心跳年龄2秒。批次12处理数从停写时15,532继续至15,715，批次16从15,880继续至16,015；根代理已查看两张手机截图。发布脚本的`deployed_pending_http_verification`中间状态由这份后续线上验证完成闭环；完整发布及备份恢复路径见`deploy-report.md`。

## 产品与契约核对

管理浏览器使用临时真实WSGI/SQLite，脚本`tests/test_rule_lab_management_ui.cjs`覆盖：未开始编辑保持暂停、名称XSS文本、过期预览409、容量拒绝无半批、提交后响应丢失只读回执恢复、显式重试使用原request_id/payload、fork不改原成绩、完整名称确认/取消零写、batch/study删除依赖和job完成、真实存储回收。桌面1440与手机390检查横向不溢出、关键触控目标至少44px、退出恢复焦点、无pageerror。独立QA已检查手机编辑与删除截图。最终运行证据为`/tmp/pc28-management-ui-final.log`：产品owner把原exec session53611、最终chunk7951a0的stdout及exit_code=0如实归档，明确不是重新执行；此后产品代码未变。

截图目录`/tmp/pc28-management-ui-evidence/`含`management-desktop.png`、`management-mobile.png`、`delete-mobile.png`。三个既有浏览器回归也全部通过，日志见测试表。

| 契约/风险 | 关闭证据 | 当前状态 |
|---|---|---|
| CM1/R1 提速等价与可见暂停 | 三种固定基准相同摘要；worker专项48项；连接生命周期反例 | 通过 |
| CM2/R5 编辑/fork/rename不污染成绩 | management/API/真实浏览器；队列交错反例 | 通过 |
| CM3/R2/R3 不复活/ID不复用 | queue原子启动；batch/study/experiment/series最高ID反例；旧work与fresh-deleting outcomes均拒绝 | 通过 |
| CM4/R6 有界删除及恢复 | 每步limit断言、重建store续作业、pending无伪结算、worker空闲推进 | 通过 |
| CM5/R4 依赖及跨账号共享保护 | 独立source/study约束与另一owner普通批次反例 | 通过 |
| CM6/R7 真实空间回收 | 物理文件缩小、读锁waiting零释放、超预算清理、低空间拒绝 | 通过 |
| CM7 一次转换与恢复 | 真CLI预览/活跃拒绝、systemctl失败封闭、生产副本全原列、真实VACUUM中断 | 通过 |
| CM8/R8 API归属与重试 | 9项真实WSGI、未知字段/16KB/no-store/跨owner/只读GET；浏览器丢响应原请求重试 | 通过 |
| CM9 活跃回测受控切换 | 390aa45上线；22旧表1,739,112行哈希、602ID/队列/状态保持；线上0POST/0error；批次12及16继续前进 | 通过 |

**STATUS: DONE**

**REASON:** W4完整回归、独立专项、真实浏览器、经济结果等价、副本迁移/恢复及W5生产发布只读验收全部通过；两项实际缺陷已关闭，CM1～CM9全部完成。行覆盖率未测量，不将其表述为已覆盖。
