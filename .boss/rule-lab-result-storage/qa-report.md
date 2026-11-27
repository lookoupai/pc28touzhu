# 结果型实验存储独立 QA

## 摘要

- **W4 QA通过，可进入受控发布。** ROOT冻结完整回归1259项通过、0失败、0跳过，310.386秒；产品真实WSGI/SQLite+DOM交互通过。CM10生产上线验收待发布，布局未实测。
- 独立反例已证明真实SQLite删除失败会回滚结果及水位、单独古老pending不钉住全部输入、普通待启动批次的有限日期及暖机得到保护、超预算仍能清冗余。
- 四项数据/来源边界问题及一项重测分流问题均已由真实反例关闭：待启动预测日期保护、旧可见时间前沿、预取后来源双删、连续冻结前沿被普通cache污染，以及活动连续段被current_source回退绕过。具体红→绿证据见缺陷闭环。
- 旧0b3d84d与新实现1CPU/256MiB三组100/600版本风险矩阵的全部经济摘要一致，新版终态和闭轮均0，耗时无观察到的回退；各测量后新增边界修复另由专项和ROOT冻结回归覆盖。
- 生产一致副本真实迁移与分批回收通过：777,437,184字节主库降至100,503,552字节，目录100,536,320字节（约95.9MiB）。2,626,498已结记录及32,682闭轮清零，47pending和全部原成绩/当前状态保持；不是估算释放量。

## 独立执行证据

工作目录 `/www/wwwroot/pc28touzhu-lab-results`，全部行为测试使用临时真实SQLite/来源fixture，没有写生产库、原快照或上游库。

| 范围 | 准确命令 | 退出码 | 通过/失败/跳过 | 失败详情/证据 |
| --- | --- | ---: | --- | --- |
| 原实现行为RED | `env PYTHONPATH=/tmp/pc28-results-baseline-code-0b3d84d/src:/www/wwwroot/pc28touzhu-lab-results/src:/www/wwwroot/pc28touzhu-lab-results python3 -m unittest tests.test_rule_lab_results_qa -v`（当时4项） | 1 | 0/4/0 | 旧策略保留明细、保留已消费输入且无pending删除事务；`/tmp/pc28-results-qa-original-red.log` |
| 新实现两项数据边界RED | `env PYTHONPATH=src python3 -m unittest tests.test_rule_lab_results_qa -v`（当时6项） | 1 | 4/2/0，1.025秒 | `_legacy_frontier...`错误保存11-28而非11-30可见时间；`_unstarted_dates...`丢预测；`/tmp/pc28-results-qa-boundaries.log` |
| 边界修复后新进程 | 同上（6项） | 0 | 6/0/0，0.821秒 | 无；`/tmp/pc28-results-qa-boundaries-green.log` |
| 已批准多段重测独立保护 | `env PYTHONPATH=src python3 -m unittest tests.test_rule_lab_results_qa.ResultsIndependentQaTests.test_approved_long_replay_keeps_later_chunks_after_original_consumer_is_deleted -v` | 0 | 1/0/0，4.738秒 | 无；`/tmp/pc28-results-qa-long-replay-final.log` |
| 最终独立8项 | `env PYTHONPATH=src python3 -m unittest tests.test_rule_lab_results_qa -v` | 0 | 8/0/0，6.155秒 | 无；`/tmp/pc28-results-qa-eight-final.log` |
| 历史完整状态收敛 | `env PYTHONPATH=src python3 -m unittest tests.test_rule_lab_results_qa.ResultsIndependentQaTests.test_completed_segment_history_does_not_retain_complete_per_experiment_state -v` | 0 | 1/0/0 | 无；`/tmp/pc28-results-qa-segment-state-final.log` |

测试自身的观测/夹具调整已按既定契约纠正，未因此改生产实现：连续段末期尚未可见，102个已决期为101已结+1pending，不能强求102结算；fixture当前时间在12月，维护不得用11月时钟倒退后断言新维护周期已经执行。最终用例明确断言该pending保留及在未来维护周期收敛。长区间重测夹具原先依赖旧全A seed；新策略已取消该行为，测试现显式将仍由两个合法draft保护的cache经正式保存页写入完整本地档案，模拟升级前已有完整输入。之后删原消费者并验证后续chunk，不要求生产恢复旧长期归档。

## 独立场景

1. 使用真实SQLite AFTER DELETE trigger在pending删除后触发ABORT，核对stats、日账、水位、pending、revision与cursor整笔回滚；移除trigger后仅一次重试入账。
2. 一个古老pending与末100期暖机仅保护离散期号/窗口，其他已消费区间可删除。
3. 尚未开始同来源回放只保护所选日期及前100期；日期之外输入不因空游标永久被钉住。
4. 已保存档案中的prediction没有open_at，仍必须借对应draw日期保护其待启动范围。
5. 较早一期开奖晚于最大已结期才发布时，迁移前沿必须包含其真实可见时间。
6. 已超普通写入预算时分批删除仍可执行，原结果不被重算或重复加账。
7. 已批准超过90天的重测，删除原实验和其他保护者后，新暂停消费者自身保护后续chunk；上游逐期数据删除后仍可完成已批准区间。
8. 4段完成后的最新完整checkpoint保留，旧baseline标记不可恢复，parent/child summary均不再嵌套完整每实验累计/风险状态；经济结果摘要保持。

## 已复现缺陷与关闭证据

| 编号 | 问题及风险 | RED证据 | 修复后实际验证 | 状态 |
| --- | --- | --- | --- | --- |
| Q1 | 档案prediction的open_at为NULL，待启动日期保护未继承draw日期，可能提前删除任务所需预测 | `/tmp/pc28-results-qa-boundaries.log`，6项中该项失败 | `/tmp/pc28-results-qa-boundaries-green.log` 6/6，随后独立8/8 | 已关闭 |
| Q2 | 旧前沿只看最后已结期，漏掉更早期但更晚公布的开奖，可能给后续模拟错误时间边界 | 同上，保存11-28而应为11-30可见时间 | 同上6/6及独立8/8；保守历史MAX/既有检查点前沿 | 已关闭 |
| I1 | 预取draw后来源同时删除该draw与prediction，空JOIN被视为正常无预测并继续计算 | `test_source_draw_and_prediction_removed_after_prefetch_are_rejected`，`/tmp/pc28-results-replay-double-delete-red.log` 0/1/0，退出1 | Data冻结39项新进程，包含该用例，`/tmp/pc28-results-data-final-frozen.log` 39/0/0、18.028秒、session87200退出0 | 已关闭 |
| I2 | 连续段实际使用冻结archive，frontier却优先读普通cache；普通cache修改时间污染已计算日期 | `test_continuous_frontier_uses_frozen_input_instead_of_polluted_ordinary_cache`，`/tmp/pc28-results-frontier-frozen-red.log` 0/1/0、退出1 | 同一冻结39项验证；frontier改读实际本段revision/暖机引用 | 已关闭 |
| P1 | 活动连续段本地预览不允许重测，但current_source fallback又返回available | `test_active_continuation_does_not_fall_back_to_current_source`，`/tmp/pc28-results-product-active-replay-red.log` 0/1/0、退出1 | `/tmp/pc28-results-product-active-replay-green.log` 1/0/0、0.687秒、退出0；后Product77项通过 | 已关闭 |

I1/I2最小复现由Review完成，Data将其写为正式行为测试；QA读取实际RED与GREEN日志并核对用例所属测试模块，没有以静态代码审查代替执行证据。 Review在修复后再次独立运行两项闭环，2/0/0、0.772秒、退出0，`/tmp/pc28-results-review-closure.log`；最终tech-review已确认I1/I2关闭。Data最后39项准确命令：

```bash
env PYTHONPATH=src python3 -m unittest tests.test_rule_lab_results_series tests.test_rule_lab_results_replay tests.test_rule_lab_retention tests.test_rule_lab_results_migration tests.test_rule_lab_continuous_worker -q
```

Core另修复初始化复活旧大索引的问题：`ResultsIndexInitializationTests`四反例初始3失败/1通过，`/tmp/pc28-results-core-index-red.log`；修复后命令 `env PYTHONPATH=src python3 -m unittest tests.test_rule_lab_results_store.ResultsIndexInitializationTests tests.test_rule_lab_results_store.ResultsOnlyStoreTests tests.test_rule_lab_results_worker -q`，21/0/0、1.730秒、session12112退出0，日志`/tmp/pc28-results-core-index-green.log`。新库建立pending部分索引，旧库启动不重建全量索引，清理收尾负责移除旧索引。

## 固定副本

原快照：`/tmp/pc28-results-baseline.db`，只读校验SHA256 `5e44a9a50e6da2a95d4330f0f7b7d6207f9f098c20f29c60827b39d1e99a53f3`。

| 项目 | 原始值 |
| --- | ---: |
| 数据库字节 | 777,437,184 |
| schema / auto_vacuum | 5 / 2 |
| 实验版本 | 602 |
| pending / settled | 47 / 2,626,498 |
| active / closed轮次 | 86 / 32,682 |
| 连续系列 / 计算段 / 检查点 | 5 / 9 / 5 |
| 日汇总 | 14,893 |

必须另复制到临时目录后迁移。受保护摘要包括原参数、stats/daily/runtime所有原key、水位、pending、当前风险日/活动轮、原cursor全部旧key与状态/进度。允许cursor新增retention_v1，但须校验前沿。允许清除的旧终态、闭轮、历史完整快照与无依赖输入不能仍要求原行数不变。副本实际验证已完成，详见下节。


### 实际副本迁移与回收

命令：`env PYTHONPATH=src python3 /tmp/pc28-results-copy-qa.py`，ROOT收到实际exec session76839退出0。证据`/tmp/pc28-results-copy-qa.json`、`.log`。脚本固定一个CPU、256MiB地址空间，先只读原快照SHA，再复制到临时目录。

实际调用维护CLI的`main()`，仅模拟`systemctl is-active`为三个写者明确inactive/exit3；参数解析、备份、schema初始化、frontier回填、保护hash及SQLite检查全部真实执行，生产服务未停。CLI默认预览文件SHA不变；apply不自动清历史，执行6.396秒。随后使用真实保留及回收入口分步清理，中途第8/130/1024步重建store并核对保护字段，最后复制外置备份验证恢复。

| 检查 | 实测结果 |
| --- | --- |
| 主文件 | 777,437,184 → 100,503,552字节，减少约87.1% |
| 清理后实验目录 | 100,536,320字节，含当前SHM/WAL状态，约95.9MiB |
| 终态逐笔 / 闭轮 | 2,626,498 / 32,682 → 0 / 0 |
| 必须保留 | 47pending、86活动轮、602个实验全部原配置/stats/runtime/决定与结算水位、全部日汇总、原batch状态/进度/cursor旧keys、series与stream水位完全一致 |
| 已验证边界 | 7个已开始批次frontier_verified；3个未开始批次未伪造前沿 |
| 有界维护 | 5865步，最大500扫描/500处理，一步最长458.452ms |
| 清理耗时 | 204.382秒；含复制/CLI/哈希/恢复整个验证221.388秒 |
| 内存与CPU | 峰值69,464KiB，1CPU affinity，256MiB地址空间 |
| 自动回收任务 | 仅1个复用内部job，无每页生成历史任务 |
| 完整性 | schema6、auto_vacuum2、WAL保持、quick_check=ok、FK错误0 |
| 备份 | 777,437,184字节，实验目录外，0600；恢复原结果有效 |
| 隔离性 | 原快照SHA保持；临时实验副本及本次临时备份已清理 |

剩余空间主要是尚未开始的组合实验需要的共享输入：`lab_input_versions`约55.4MB、普通预测缓存约13.5MB、覆盖约7.2MB及其索引；全部94,520档案行、69,566预测、23,202开奖仍有消费者，未为凑目标误删。新版物理数据库变小不能直接称为服务器净释放，因为真实发布回滚备份会短期额外占用；此QA临时副本和备份已随临时目录销毁。

副本长进程使用启动时加载的实现；运行期间Data继续修正维护计数/索引等细节。该实测证明主要清理路径与受保护数据不变，不能冒称后续每一行代码都由同一次长进程执行。后续变动以相关专项和ROOT最终冻结回归验证；`/tmp/pc28-results-qa-current-code-hashes.json`是副本完成后采样，明确不是追溯加载时hash。

## 风控矩阵和容量基准

工具 `tests/rule_lab_results_benchmark.py`，旧代码用 `git archive 0b3d84d src`提取至`/tmp/pc28-results-baseline-code-0b3d84d`。同一个工具和临时确定性输入，5种止盈/止损/熔断/最大轮次/冷却/附加日保护 × 大小/单双 × 10个条件版本，共100版本；600版本由6组排队执行，峰值运行仍100。

程序固定宿主可用的一个CPU，RLIMIT_AS=256MiB。比较既有全部stats字段、risk_runtime及活动轮、日汇总、决定/结算水位、批次真实游标/质量和未结算义务的规范化摘要；不依赖将被删除的历史逐笔。新实现回放后维护单列计时，源fixture字节保持。

| 旧基线 | 耗时 | 峰值RSS | 目录字节 | 结算/闭轮 | 经济摘要 |
| --- | ---: | ---: | ---: | --- | --- |
| 100版本×100期 | 9.1773秒 | 28,928KiB | 1,171,456 | 1092/200 | `f6bf365f6605b74c8fde2eab28696b1c455935c0c18da0d86db67ae0fcf1cf8c` |
| 100版本×400期 | 35.8038秒 | 31,196KiB | 2,015,232 | 3269/577 | `a846901431402dcddb420720486308e500003180b26383dcc88d621f666e7836` |
| 600版本×100期 | 57.5183秒 | 46,976KiB | 5,181,440 | 6552/1200 | `7bff564e397d235689046bd791f5c18fe124bb31becfde3222e5291869abefb7` |

旧版命令模板（cwd为提取旧代码目录）：`env PYTHONPATH=/tmp/pc28-results-baseline-code-0b3d84d/src:/www/wwwroot/pc28touzhu-lab-results/src:/www/wwwroot/pc28touzhu-lab-results python3 /www/wwwroot/pc28touzhu-lab-results/tests/rule_lab_results_benchmark.py --versions <100或600> --issues <100或400> --output <JSON路径>`，上述3项实际退出0。证据 `/tmp/pc28-results-matrix-before-{100x100,100x400,600x100}.{json,log}`。

最终配对命令：`env PYTHONPATH=src python3 -m tests.rule_lab_results_benchmark --versions <100或600> --issues <100或400> --output <JSON>`。3组实际退出0，经济摘要逐项相同，source fixture字节保持，均没有已结算逐笔或闭轮存留。回放后把虚拟维护时间推进2分钟，真实清理和回收耗时包含在新版总耗时中。

| 负载 | 旧版总耗时 | 新版含维护耗时 | 旧/新CPU秒 | 旧/新目录字节 | 新版峰值RSS |
| --- | ---: | ---: | --- | --- | ---: |
| 100×100 | 9.1773 | 8.8271 | 8.2834 / 8.0031 | 1,171,456 / 794,624 | 28,288KiB |
| 100×400 | 35.8038 | 35.3661 | 32.4910 / 32.0636 | 2,015,232 / 790,528 | 28,544KiB |
| 600×100 | 57.5183 | 50.7582 | 50.1808 / 46.7289 | 5,181,440 / 3,035,136 | 44,504KiB |

同100版本从100期增至400期，新版目录仍约0.76MiB；这两个场景在同一自然日，结果日行数同为80，说明新增逐期历史没有随期数积累。600版本目录约2.9MiB，包含所有参数与结果；均只保留100期共享暖机。跨日会按必要每日结果增长，不代表永久固定大小。

证据`/tmp/pc28-results-matrix-after-{100x100,100x400}.json`、`/tmp/pc28-results-matrix-final-after-600x100.json`及各同名log；配对校验汇总`/tmp/pc28-results-matrix-comparison.json`。首次600新版采样与777MB副本在同一CPU竞争，墙钟98.5552秒、CPU56.0659秒，明确排除为性能结论；副本退出后独立重测得到上表50.7582秒。没有将受竞争样本隐藏成算法回退，也未把最好值当线上承诺。

## 产品与ROOT阶段回归

- Product最终77项通过、0失败/跳过，36.218秒，ROOT收到session91284退出0；`/tmp/pc28-results-product-api-final.log`。准确命令：`env PYTHONPATH=src python3 -m unittest tests.test_rule_lab_results_api tests.test_rule_lab_api tests.test_rule_lab_continuous_api tests.test_rule_lab_management_api tests.test_rule_lab_monthly tests.test_rule_lab_continuous_monthly -q`。
- `node tests/test_rule_lab_results_ui.cjs`真实临时WSGI/SQLite+jsdom交互通过，ROOT收到session56334退出0；`/tmp/pc28-results-product-ui-final.log`。覆盖结果页、资金/日/月报告、current_source预览后真实创建重测/启动/结算、原成绩保持、改名/XSS、续算默认及新结果可接续、缺输入拒绝、存储、CSV和管理流程。没有打开Chrome或新浏览器profile，**未测布局、截图、触屏和实际浏览器绘制**；不冒称桌面/手机视觉验收。
- ROOT独立W1共26项通过、0失败/跳过，2.687秒，session68554退出0；`/tmp/pc28-results-w1-root.log`。准确命令：`env PYTHONPATH=src python3 -m unittest tests.test_rule_lab_results_store tests.test_rule_lab_results_worker tests.test_rule_lab_retention -q`。26是当次用例数；之后新增索引反例，因此相同命令当前用例数可增加。

- ROOT在最后I1/I2修复后的新进程再次执行 `node tests/test_rule_lab_results_ui.cjs`，session33829退出0，日志`/tmp/pc28-results-dom-root.log`，真实WSGI/DOM全流程再次通过；同样没有布局验证。

### ROOT首轮完整回归

`/tmp/pc28-results-full-root.log`：1259项，1257通过/2失败/0跳过，296.471秒，ROOT确认退出1。两项失败为旧测试仍按旧存储结构取证：

- `test_production_copy_append_keeps_twenty_six_ids_and_reconciles_every_cent`：从已删除终态`lab_decisions`求和得到0，而实际新增净利为-454分；Data改用仅测试临时的真实事务结算轨迹继续逐分对账，保留经济断言。
- `test_second_worker_exits_without_advancing_the_ledger`：原断言`lab_runtime`必须0行，但schema6合法初始化了1行保留元数据；Core改为第二worker启动前后整库快照完全不推进，保留独占锁行为断言。

这两项不是运行时代码回退或放宽计算规则。Core独占锁专项已通过：`env PYTHONPATH=src python3 -m unittest tests.test_rule_lab_ops -v`，4/0/0、0.270秒、退出0，`/tmp/pc28-results-core-ops-green.log`。新断言比较第二worker执行前后全部lab表快照相同、初始化retention元数据原样、无worker heartbeat，未削弱“不能推进账本”。Data两个真实生产快照副本用例也已通过，2/0/0、14.803秒，session38742退出0，`/tmp/pc28-results-legacy-copy-final-all.log`：成功删除的事务轨迹与实际outcome、stake关联，继续对每实验精确核对net/count/stake及原日账、配置和26个ID。两个旧观测修正均未改运行时代码。ROOT第二次冻结全量已通过，准确命令 `env PYTHONPATH=src RULE_LAB_MIGRATION_SNAPSHOT=/tmp/pc28-continuation-baseline.db RULE_LAB_READONLY_SOURCE_DB=/www/wwwroot/AITradingSimulator/pc28_predictor.db python3 -m unittest discover -s tests -q`，session58590实际退出0，`/tmp/pc28-results-full-final-root.log`：1259通过/0失败/0跳过、310.386秒。首轮如实保留失败，不将失败轮写成通过。

## 契约矩阵核验

| 契约 | 已有实际证据 | 状态 |
| --- | --- | --- |
| CM1 成绩保留、终态不留 | Core原子写入专项；独立副本原值hash；三组矩阵终态/闭轮0 | 通过 |
| CM2 重启和重复不重复入账 | SQLite ABORT原子回滚/重试；Core持久水位反例；副本3次store重启 | 通过 |
| CM3 风控口径不变 | 5种风险参数×两玩法×10条件，100/600版本经济摘要完全一致 | 通过 |
| CM4 有界输入依赖保护 | 古老pending离散保护、未开始日期/暖机、NULL预测日期修复、长重测后续chunk | 通过 |
| CM5 冻结版本与时钟 | Data39含连续缓存污染修复、连续worker冻结/迟到用例；独立可见时间前沿 | 通过 |
| CM6 历史状态精简后仍可续 | 4段完整payload收敛且经济摘要不变；长重测源删除后完成 | 通过 |
| CM7 重测实际范围 | Product77接口、Data39来源双删拒绝、真实DOM current_source创建/结算和缺档禁用 | 通过 |
| CM8 清理及空间回收真实 | 777MB副本95.9MiB、500行/256页、1自动job、超预算维护及真实备份 | 通过 |
| CM9 权限及原管理 | Product77包含真实owner/admin/输入边界；DOM创建/改名/删除/续算；Ops4独占进程快照不变 | 通过 |
| CM10 线上迁移保留进度 | 一致副本已通过；生产实际执行/状态恢复/原进度和缩盘待发布证据 | 待发布验收 |

布局/触屏/浏览器绘制和变更行覆盖率未实测；没有把DOM执行当作视觉覆盖，也没有把用例数量换算覆盖率。

## 最终门禁与保留说明

- ROOT冻结完整回归已通过1259/0/0、310.386秒；首轮1257/2/0及两项旧观测修正的专项证据保留，运行时代码未因此改动。W4没有剩余阻塞项。
- 生产发布后的原进度/状态恢复及实际缩盘。
- 变更行覆盖率尚未测量；不得用用例数代替覆盖率。

**STATUS: DONE_WITH_CONCERNS**

**REASON:** W4全部测试门禁通过，可进入受控发布；CM10留待生产验收，布局与变更行覆盖率未实测。
