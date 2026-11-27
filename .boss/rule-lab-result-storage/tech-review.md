# 结果型实验存储技术评审

## 摘要

- 评审结论：**有条件通过，可进入实现；所有结果一致性和依赖清理门禁通过后才能发布。** 用户已明确授权实施，目标是比较条件、止盈止损与熔断的模拟结果。
- 首要变更是原子结果落库：累计成绩、日结果、风险状态及水位提交成功后移除pending，既有终态逐笔和闭轮后台清理。不保留50笔/30轮，不另建压缩明细档案。
- 当前已有足够的统计状态支撑已有结果显示；无需先扩大熔断统计产品范围。原本没有的指标不补0，也不从不完整明细反推。
- 高风险集中在去重与崩溃恢复、已清输入后的时钟前沿、共享暂停/队列输入、冻结版本和检查点压缩。仅删除明细、仅压缩checkpoint或仅保留100期均不足以满足契约。
- 保留现有650保存/100运行、1核/256MiB与1GiB预算；不新增依赖、服务或定时器。设计中的150MiB是固定副本的待验证目标，生产当前量级不同，必须实测报告。

## 1. 基线与范围

评审基线`0b3d84d`，实现工作树`/www/wwwroot/pc28touzhu-lab-results`，当前schema5。设计来源为本目录`design.md`，用户补充要求是“看结果就行，其他只保留必要的”。生产最初只读采样约716MiB、602个版本、4个连续系列，状态继续变化；不得把旧采样当发布时固定状态。

沿用Python标准库、WSGI、SQLite WAL、原生JavaScript。`pyproject.toml`没有必需第三方服务端依赖，CI只构建/推送Docker镜像，不自动执行测试。完整回归采用`PYTHONPATH=src python3 -m unittest discover -s tests -q`，浏览器有真实临时WSGI/SQLite fixture。生产只读源使用单连接约250ms查询预算。

本轮不改真实投注账本、预测源保留策略、资金规则或既有时间可见性。结果集与配置受保护，明确授权删除的是冗余的终态明细、闭轮、无依赖输入与旧完整快照。

## 2. 已确认的实现事实

| 位置 | 事实 | 必要调整 |
| --- | --- | --- |
| `commit_step` | 已用revision、批次游标和`last_decided_issue`防止重复决策；outcome只有pending存在才入账 | 同事务删除已结pending，继续保留多层水位保护 |
| `apply_outcome` | 已保存逐笔累计资金高点、最大回撤、当前/最大连亏 | 保留原值，不能从日净利重建逐笔指标 |
| `settle_risk` | 活动轮、冷却、完成/盈亏轮数、单轮止盈/止损数已有持久状态 | 闭轮无需另存历史；旧统计不重新累计 |
| `_monthly_report` | 由日结果和累计状态生成；但起止/可见时间仍回查输入 | 先持久化边界，后清理旧输入 |
| `_snapshot/_warm_map` | 完整实验状态、risk_days、pending、暖机被复制到每段 | 只留最新与活动直接前置payload；pending超100期仍保护 |
| `_segment_summary` | 每段含每实验累计stats及完整risk_runtime，child和parent又各存一份 | 历史段缩成批次级增量/范围/质量，当前结果为权威 |
| `ensure_stream` | 默认从当前缓存最早期到last补seed，而非仅暖机 | 取消已消费A全区间seed，并防旧seed状态重灌已清历史 |
| `protected_floors` | 只看series，使用全局最小floor | 改为所有消费者的有限区间+离散pending集合；普通completed/暂停/未开始同样考虑 |
| `prune/maintenance` | 30天终态与两年档案规则，且仅tick剩余预算时运行 | 替换为结果型维护，不与新维护双重执行 |
| `ArchiveSource` | 暖机方向固定，但同内容hash的pending→settled允许推进 | 保持这一语义，不能因精简而把状态永远冻结或读入方向修订 |

## 3. 风险与关闭要求

| 编号 | 等级 | 风险 | 必须关闭的证据 |
| --- | --- | --- | --- |
| R1 | 高 | 删除终态后重复决策/重复结算导致收益翻倍 | 新事务/旧revision/进程重启/重复outcome结果不变；事务中断只有提交前或提交后两种状态 |
| R2 | 高 | 清理历史开奖后无法判断续算事件前沿，错误使用现实时间 | 迁移前后时间元数据可核验；A清理后B与A+B结果一致；证据不足明确阻止接管 |
| R3 | 高 | 只保护series或running误删普通completed暖机、暂停/队列输入 | 含跨owner与study慢子方案的保护集合测试；新消费者与GC交错不误删 |
| R4 | 高 | 最早pending当全局floor导致一个旧期钉住大量输入；反过来仅留100期误删pending | 旧pending离散保护，中间无依赖历史可清；>200迟到结算不丢 |
| R5 | 高 | 最新修订替代冻结方向，或pending状态永远不推进 | 三段源修订隔离、同hash状态合法推进、每次实际读取属于固定revision |
| R6 | 高 | checkpoint/segment summary继续复制完整状态，空间仍按版本×段膨胀 | 12个月续算后完整payload数按最新+活动依赖有界；历史段不含per-experiment完整runtime |
| R7 | 高 | 清掉已有输入后重新seed/recheck，反复膨胀 | 保留清理水位与覆盖摘要；运行多个维护/采集周期后不重灌已消费区间 |
| R8 | 高 | 缺档重测被截短、当前源被误称原冻结输入 | local_frozen/current_source明确分开，端点/启动再验证，缺区间409或运行错误，不假完整 |
| R9 | 高 | 老数据清理吞掉pending、当前日风控或统计重复累计 | 生产一致副本受保护结果/状态哈希保持；仅白名单冗余可删 |
| R10 | 中 | 500行维护预算叠加、一直无剩余时间、昂贵全表扫描 | 统一tick删除预算、扫描同样有界、持久游标；空闲/满预算也能前进 |
| R11 | 中 | SQLite只删除不缩文件或WAL busy误报释放 | 实测DB/WAL/目录字节；busy等待，增量回收<=256页，预算与实际空间分开 |
| R12 | 中 | 新增风控统计用0冒充旧未知历史，或改旧计算语义 | 保持已有字段值；新增指标标记未记录/起算边界，非本轮必需指标不阻塞核心 |

## 4. 方案调整

1. **新指标范围收敛。** 保留已有可观察结果；本次不强制补齐此前未记录的全部日熔断计数。新规则仍须声明有界状态，但不新增泛化参数矩阵生成器。
2. **重测不建设第二套永久档案。** 完整本地视图可继续archive replay；本地已清而上游仍可用时，明确按current_source重算，输入实时变化和源清理风险不得伪装成冻结历史。范围验证超时返回503/unknown，不能猜available。
3. **旧段摘要必须一起精简。** 除checkpoint外，`baseline_json`和两层`summary_json`也必须收敛。保留ID、幂等身份、摘要和实际范围；不删已有结果日账。
4. **以实际停止状态发布。** 当前实验可能在开发期间触发资源暂停，发布采样并记录每个批次/服务状态；不得盲目把用户暂停恢复为running。仅原来因资源暂停且满足恢复规则的计算可以按明确策略恢复。

## 5. 放行边界

用户授权已覆盖设计范围内的实现、数据迁移和冗余清理，不需要再次索取相同授权。实际发布仍须以代码冻结后的测试、生产一致副本迁移/续算/真实回收证据为前提。开发期间只操作隔离工作树、临时数据库或一致副本。

正式发布不得使用“旧表所有行保持”作为通过条件，因为本轮明确要删冗余；应对配置、累计结果、每日结果、原ID、游标、水位、pending和当前风控分别比对，并核对被删除集合完全落在白名单。

**STATUS: DONE_WITH_CONCERNS**

**REASON:** 设计和实现边界可行，以上R1–R12需由后续开发/QA证据关闭；本评审不声称功能或迁移已通过。

## 6. 实现只读审查记录

已对当前Core原子pending结算、Data保留集合与series/archive快照调整、Product重测分流做只读审查。没有修改正式实现或测试；以下复现均使用既有测试fixture生成的临时SQLite库，未操作生产。

### 已确认问题与最终关闭状态

| 编号 | 严重度 | 原触发与实际结果 | 影响 | Owner/状态 |
| --- | --- | --- | --- | --- |
| I1 | 高 | `validate_replay_page`之前已读draw，随后源将同一期draw与prediction都删除；missing JOIN从draw开始，`MIN(NULL)`使`_absence_allowed`返回True | 执行中输入已清却被当作普通缺预测，可能继续并声称区间完成 | Data/source，已修复并复核关闭 |
| I2 | 高 | 连续段冻结issue1106为12月1日；普通缓存同一期改为12月5日并标conflict；真实worker完成冻结段后`retention_v1.processed_open_at`和`calculated_through_date`误为12月5日 | 普通缓存污染权威进度日期，影响月报、追加日期及重测范围 | Data/frontier，已修复并复核关闭 |

I1最小复现命令在DEV以`env PYTHONPATH=src python3`执行：构建`ResultsReplayAvailabilityTests`现有fixture，预览available=True后读取1102 draw，删除源1102的prediction和draw，再调用`validate_replay_page(1,[cached_draw],[],observed_at=now)`；实际返回True（退出0），应拒绝缺档。不能以空JOIN结果当完整覆盖证明。

I2最小复现使用`ContinuousWorkerTests`现有fixture：`make_baseline(round_mode=False)`→`enable/capture`→`append(...,'2026-12-01',...)`；仅将普通`lab_draws`的1106 `open_at`改为`2026-12-05T15:59:30Z`并标conflict，保留冻结archive原值；`finish_append(...,'1106')`后实际冻结draw为`2026-12-01T15:59:30Z`，保存和报告却为12月5日。连续frontier必须读本段实际消费的冻结draw，不能优先普通缓存。

Product此前发现active continuation的本地预览不可用后被current_source fallback绕过，已加入active_segment硬阻断；根代理确认对应API反例已通过，本次不重复记为开放缺陷。

### 审查覆盖与限制

- 新结算在原事务中更新累计/day/risk/水位后删除pending，重复outcome仍需pending存在；未见绕过旧revision/决定水位的新增路径。
- 完成段baseline改为明确轻量标记，直接前置与最新checkpoint有保护；完成子方案先压缩baseline后组汇总读取child.summary，未见读取已裁剪baseline的正常路径。
- 新`_segment_summary`已去掉每实验完整cumulative/runtime，月快照只留当前revision；旧层叠summary由维护同时精简。
- 尚在处理的旧索引初始化修复、已被QA关闭的NULL prediction.open_at保护和legacy最晚可见时间问题不重复作为缺陷。
- 原子崩溃、实际生产副本与性能证据由Core/QA报告承担，本审查不将代码阅读当作测试通过。

### 冻结实现复核

I1当前在同一个源只读事务中先验证全部待检查issue仍有开奖，再使用absence元数据；开奖已删除时返回409 `replay_input_unavailable`。I2当前连续路径按work_context的archive revision、直接前置/当前完整检查点中的warm输入读取实际draw，不再落回普通缓存；普通模式仍可使用既有缓存。

独立复核实际命令：

```bash
env PYTHONPATH=src python3 -m unittest tests.test_rule_lab_results_replay.ResultsReplayAvailabilityTests.test_source_draw_and_prediction_removed_after_prefetch_are_rejected tests.test_rule_lab_results_series.ResultsSeriesTests.test_continuous_frontier_uses_frozen_input_instead_of_polluted_ordinary_cache -v
```

实际退出码0，2通过、0失败、0跳过，0.772秒，证据`/tmp/pc28-results-review-closure.log`。Data冻结专项另有39/0/0、18.028秒证据`/tmp/pc28-results-data-final-frozen.log`，本轮已读取其真实末尾结果。

**最终审查结论：I1/I2关闭，当前没有开放的代码审查阻塞。** 本轮未修改运行代码或测试，未扩大测试范围；最终生产发布仍以ROOT冻结版本完整回归、独立QA和受控发布门禁为准。本结论不把正在运行的全量回归写作已通过。

**STATUS: DONE**

**REASON:** 两项已复现实现缺陷均已修复并独立复核，代码审查阻塞已清零；最终发布等待完整QA门禁。
