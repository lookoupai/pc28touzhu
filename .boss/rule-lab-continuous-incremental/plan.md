---
type: plan
outputFor: [scrum-master, frontend, backend]
dependencies: [existing-design, tech-review]
---

# 实验增量续算实施计划

## 摘要

- **实施策略**：原实验 ID 和累计账本继续使用，增加系列/请求/段/检查点；所有连续回放输入由不可变本地档案供给。
- **模块划分**：5 个功能模块，4 个验收里程碑；先固定存储/输入契约，再并行引擎、接口与界面，最后集成验收。
- **关键依赖**：输入覆盖先于创建可执行段；事件时间一致性先于自动续算；共同完成段先于比较差值发布。
- **技术决策**：现有 SQLite + 单 worker，无新增依赖/外部调度；加法 schema v4；冷热 payload 使用标准库 zlib 和 SQLite BLOB。
- **风险应对**：明确晚结算和档案缺口阻塞，保护100期暖机/pending/未消费输入，所有新模式默认关闭。

## 1. 实施策略

### 1.1 整体思路

将现有已完成批次或 study 接为稳定 series，冻结基线审计快照；用户开启采集或手动追加后，共享 stream 保存新输入。追加请求先持久化为 waiting_inputs，仅在完整覆盖且边界安全后创建冻结段。worker 从实际 last_issue+1 推进累计状态，完成本段时原子写入检查点/段摘要；下一次追加继续使用相同实验。

### 1.2 原则与约束

- 不修改原批次 start_date/end_date、条件、注额、赔率和风控，不生成新的实验 ID，不占新的500保存额度。
- 正常同时运行版本100、源页≤200、暖机100期、worker默认2秒预算、MemoryMax256MiB、总目录1GiB；保持用户已选加速/省资源模式。
- 原来的普通历史/live路径保持；连续模式带明确 `continuation` 工作项上下文。
- 保留所有状态，段末允许 pending；累计最多跨度不受90天限制，单次追加/追赶分片≤90天。
- 用户通过产品开关显式启用；结构迁移不创建系统 schedule，也不自动启用现有批次。
- 真正无预测可按原规则跳过；缺档或无法确认的覆盖不得以无预测代替。

### 1.3 前置条件

- 已有设计与上游自然月保留已确认；原60天表述不再作为生产事实。
- repo preflight 已知：main基线46c392f；Python WSGI/SQLite/原生UI；CI只有Docker构建推送；unittest + 本地 Playwright/jsdom；无需安装依赖。
- 现有 schema、API鉴权、状态与容量以代码为准；未覆盖的旧数据保持标注，不能由迁移补造证据。

## 2. 模块划分

| ID | 模块 | 职责 | 依赖 | 复杂度 |
|---|---|---|---|---|
| M-01 | 系列和分段账本 | 接管预检、基线、幂等请求、段、检查点、所有权和容量 | 无 | 高 |
| M-02 | 持久输入档案 | 共享捕获、覆盖分类、版本冻结、冷热存储、引用保护、归档读取 | M-01契约 | 高 |
| M-03 | 连续执行 | 有界采集调度、每日due、手动请求、时钟推进、pending、study队列 | M-01/M-02 | 高 |
| M-04 | 续算操作和报告 | API、累计/分段/月报、状态开关、覆盖/错误展示、浏览器流程 | M-01契约，完成依赖M-03 | 高 |
| M-05 | 验收和发布 | 等价性、迁移副本、资源、权限、完整回归、上线核验 | M-01～04 | 高 |

依赖：M-01契约 → M-02 → M-03 → M-05；M-01契约 → M-04 → M-05。M-04可先依据冻结契约做UI/接口测试，不并行修改store。

## 3. 里程碑计划

| 里程碑 | 交付 | 验收门禁 |
|---|---|---|
| MS-1 | v4/档案/接管事务/请求契约 | 旧库逐行不变，迁移幂等；无源A时仍读档案；覆盖失败不推进；请求幂等；保留/空间测试 |
| MS-2 | 单批次及组真正续算 | 同冻结输入 A+B 对比 A→重启→B：逐笔、累计、回撤、轮次和日风险相同；晚结果与pending跨段；组共同输入 |
| MS-3 | 手动/自动操作和报告可用 | API鉴权/边界/重复提交；1440/390浏览器创建请求、追加、暂停采集/计算、查看旧段/月报 |
| MS-4 | 生产可发布 | 全量回归和独立QA通过，生产一致副本迁移，预算核验，备份/默认关闭/健康验证 |

## 4. 技术方案细节

### 4.1 数据模型

| 对象 | 最小字段/职责 | 不变量 |
|---|---|---|
| lab_series | owner_kind(batch/study), owner_id, user_id, config_hash, revision, capture_enabled, compute_enabled, daily_at, timezone, next_run_at, state,error | owner唯一；默认两开关false；compute要求capture；只允许Asia/Shanghai |
| lab_continuation_requests | series_id, request_id, kind, payload_hash, status, target_end_date, result_json, created_at | request_id在series内唯一；同ID异payload409；响应丢失后可找回同请求 |
| lab_segments | series_id, sequence, predecessor, kind(baseline/append), requested_end_date, cutoff_issue, cutoff_at, archive_revision, manifest_hash, status, summary_json | 同series只有一个活动段；无新期不创建空段；组共用冻结输入视图 |
| lab_checkpoints | segment_id,batch_id,engine_version,configuration_hash,state_hash,payload | 原子保存完整stats/risk_runtime、相关风险日、活动轮、pending、实际游标、事件前沿、100期暖机；payload可压缩 |
| lab_capture_streams | source_key,predictor_id,revision,after_issue,highest_seen_issue,covered_through_issue,last_success_at,state,error | 同源同预测器共享；最高见到≠已覆盖；失败不推进coverage |
| lab_input_versions | source_key,kind,predictor_id,issue_key,version/observation_revision,content_hash,payload_json/payload_blob,observed_at | 内容不可变；状态/settled_at更新也保留新观察版本；开奖跨预测器共享 |
| lab_input_coverage | stream_id,issue_key,revision,class,checked_at,details | class明确present/no_prediction/invalid_prediction/waiting/archive_gap；无证据不标完整 |
| lab_monthly_snapshots | series_id,batch_id,experiment_id,month,revision,payload,reason | 完整月份冻结快照；旧月晚结算修订新增revision，旧快照保留 |

可合并低频元数据，但不能丢失这些不变量。实现记录实际表名，API不暴露存储细节。

### 4.2 稳定内部契约

下列方法由 Data owner 统一实现；方法名如需调整须先通知 Engine/API owner并更新任务契约，禁止各自发明兼容层。

- `continuation_preview(user_id, owner_kind, owner_id, *, now)`：只读返回资格、blockers、revision、原版本数、实际起点、状态哈希。无series时返回未启用视图，不能因GET建基线。
- `set_continuation(user_id, owner_kind, owner_id, payload, *, now)`：校验revision/幂等后接管或改开关；事务生成基线/保护暖机。API校验后的payload仍在store校验所有权及状态。
- `request_append(user_id, owner_kind, owner_id, payload, *, now)`：持久请求；覆盖未齐返回waiting_inputs；已追平返回up_to_date，无新段。
- `list_segments(user_id, owner_kind, owner_id, *, limit=20, before_id=None)`：有界稳定倒序，含逐实验增量与累计截面，返回next_cursor。
- `input_coverage(user_id, owner_kind, owner_id)`：三水位、质量计数、缺口范围、捕获状态、存储预算。
- `capture_work_items(*, now, limit=...)` / `commit_capture(stream_id, expected_revision, *, draws, predictions, coverage, next_issue, now)`：有界读取任务与原子输入/覆盖提交。
- `prepare_due_appends(*, now, limit=...)`：处理到期及手动请求，覆盖完备后事务冻结段；每次最多一个90天分片，按series公平推进。
- `archive_source(batch_id)`：获取当前段冻结输入视图；提供现有source方法 `latest_draw/get_draw_by_issue/draws_page/history_draws/get_predictions/find_start_issue` 和特征格式 `get_history/input_evidence`。权限查询在外层对真实source执行；adapter不回落上游。
- `get_work_item(batch_id)`：已有返回值追加 `batch.continuation`，至少含series_id,segment_id,archive_revision,effective_end_date,frontier_time,engine_version；内部batch可派生effective end_date/study cutoff，原数据库批次字段保持。
- `complete_segment(batch_id, expected_revision, *, cursor, now)`：原子冻结该child段结束检查点、增量摘要及完成状态；组最后child完成才发布共同段；允许pending并保留前沿。
- `commit_step`：原有幂等/资金事务继续使用；连续输入通过冻结版本校验，不能被普通cache的另一个版本 conflict 污染或反向覆盖。

`continuation` 公共摘要统一字段：
`enabled,capture_enabled,compute_enabled,revision,state,calculated_through_issue,calculated_through_date,captured_through_issue,captured_through_date,settled_through_issue,pending_count,next_run_at,active_segment_id,error`。

**开关与暂停语义**：`compute_enabled` 仅控制“自动续算”（每日自动追加），不作为显式手动追加的前置。`capture_enabled=true, compute_enabled=false` 时，手动 append 仍运行本次请求，且不会开启每日自动续算。关闭自动续算只阻止未来自动请求，不取消已提交的手动任务。活动段暂停/恢复使用既有 batch/study `state` 接口，UI 文案为“暂停本段计算”；暂停期间仍采集（capture 开启时），调度器绝不自动恢复用户暂停的活动段。

**客户端版本令牌**：API `expected_revision` 是不透明字符串，接管前为 `legacy:<statehash>`，接管后为 `series:<id>:<rev>`。客户端从预览原样传回，不解析、递增或转成整数；整数应校验拒绝。数据库内部 revision/worker CAS 仍可使用整数，此约定只针对外部令牌。

生命周期state：`disabled,idle,catching_up,waiting_inputs,running,paused,archive_gap,storage_paused,source_unavailable,error`。它与原批次status不是同一个enum；UI必须分开展示。API blockers使用稳定code和中文message。

### 4.3 API

基础路径 `/api/platform/rule-lab/{batches|studies}/{id}`，同一语义复用两类owner。

| 方法/后缀 | payload/参数 | 响应 |
|---|---|---|
| GET continuation-preview | 无 | continuation,eligible,blockers,expected_revision,next_issue,configuration_hash,saved_experiments |
| POST continuation | request_id,expected_revision,capture_enabled,compute_enabled,daily_at,timezone | created,continuation；重复同请求200 |
| POST append | request_id,expected_revision,end_date | created,queued,up_to_date,request_id,continuation,segment；已持久等待返回202，创建段201，幂等/无新期200 |
| GET segments | limit 1..50,before_id正整数 | segments,next_cursor；只此新列表路由开放这两个query键 |
| GET input-coverage | 无 | continuation,coverage,storage |

约束：严格未知字段拒绝；request_id沿用现有比较组格式；日期真实、不得倒退/超未来，单次≤90天，累计可长于90天。child batch入口返回409 group_action_required。所有接口沿用admin/session/owner、no-store、POST16KB；他人记录404，匿名401，非管理员403。GET不得触发捕获、迁移基线或追加。

### 4.4 引擎与档案关键决策

1. 采集按source授权和保留边界检查，使用成功读取事务获得每期coverage。source返回读取失败时不落“no_prediction”。缺样判断必须是预测器/日期范围证据：可用该预测器归档日汇总的最新日期保守界定疑似清理区间；不能以全局最早预测日期替代。absent且落在已归档/清理范围内应标archive_gap；真实仍保留区间成功读取的缺样可标no_prediction并继续按既有算法跳过。fixture缺少证据元数据时同样保守处理，不能默认为历史无预测。
2. 顺序捕获加有界近期/待结算重查，观察到的改写另建版本；冻结段取`observation_revision<=archive_revision`的既定版本。暖机既有已消费版本作为基线证据固定，不被后来修订替换。
3. 先查下一条未处理决策时间，结算可见时间≤该时间的pending，再决策。段末不能用现实现在flush；无下一时点则保存当前前沿与pending，下段接手。已知下一时点存在时可按同一引擎顺序推进到该边界，但不得越过下一条未处理决策。
4. 无论tick开始pending sweep、回放中settle、段末finish都使用同一连续前沿规则。条件逐期与轮次模式都覆盖，不能只修round模式。
5. 旧基线接管要求completed、scheduler版本兼容、无历史conflict/expired/review_required、暖机完整；最后已结可见时刻不得晚于新段第一决策。无法恢复旧前沿时明确阻止，不改旧成绩。
6. 每个capture tick保留独立小预算，暂停计算仍采集；source之间公平，所有待续算series共享stream，单个关闭不停止其他消费者。
7. 仅 `compute_enabled=true` 才产生每日自动追加；显式手动追加与该开关无关，调度器不得自动恢复已暂停活动段。自动daily_at默认00:10北京时间，只算已结束日；next_run_at持久化，停机恢复分≤90天片追赶，一天只有一个幂等请求键；尚无新期不建空段。只实现功能，迁移不启用现有自动更新。
8. >90天已消费payload可用zlib压缩为SQLite BLOB，分页处理、压缩前后hash一致；24个月是预算内保留目标。未消费/100期暖机/pending/活动段保护优先。禁止达到预算后删除这些依赖；计算软阈值约80%，捕获硬阈值仍为现有安全余量，显示明确暂停。
9. 长期金额重测使用同一档案重新计算新实验，跨90天分片；不得线性放大旧收益。档案不完整则指出不能完整重测的月份，不回落缺失上游。

### 4.5 界面和报告

详情增加“增量续算”区：显示已计算/已采集/已结算、开关、追加截止、请求状态、原版本数；“自动续算”仅控制每日追加；“暂停本段计算”通过既有 state 接口暂停当前段，仍保留后续数据，二者不得混用。首次开通展示配置保持及起点，确认提交后禁止重复按钮；丢失响应用GET刷新找回，不能换request_id盲重试。

累计报告派生series有效截止；原月报数据不被原end_date截断。历史段展示原累计截面和本段增量，只对可加净利/计数做差；最大回撤等非可加指标展示快照而非相减。提供time_complete/pending_count/coverage_quality/comparable。手动追加至今日只表示追到已采集期号，不能把尚未结束的自然日标完整。比较组运行中保留上一共同完成段对比，不能混合快慢子批次的不同范围。

## 5. 风险应对

R1→M-03时间边界测试；R2/R3/R4→M-02冻结档案和故障源fixture；R5/R8→M-01/M-02迁移与空间测试；R6→M-01/M-03事务/容量/组测试；R7→M-04报告快照和浏览器验收。任一关键断言失败，停止下一wave并修复，不通过重跑全量掩盖差异。

## 6. 测试策略

### 6.1 测试范围

unittest覆盖纯函数、临时SQLite、实际worker+假源、WSGIAPI；Playwright覆盖真实临时WSGI桌面1440/手机390。生产仅只读核验和授权发布，不发送真实消息或投注。

### 6.2 关键测试点

- 同一冻结输入 A+B 与 A→进程重建→B：在同一事件时间前沿比较每实验逐笔决策、结算、整数分累计、高点/回撤、连亏、轮次、日风险；不能拿旧引擎现实时间flush结果与新引擎保留pending结果做不对称比较。轮次/逐期模式均覆盖。
- A已从上游删除且B已采集仍续算；未采集B清理后阻止；暂停>90天依赖不丢；源修订不改旧段。
- 月底迟到结果、下一期决策早于上期可见、pending跨段、active_round跨日，最后一注不强平。
- 0新期、并发请求、同request不同body、过期字符串令牌、重启、部分页、磁盘满都不重复记账；拒绝整数expected_revision。
- 自动续算关闭时手动追加可运行且不修改开关；关闭自动不取消已提交手动任务；active pause后跨daily到期/tick仍暂停，采集继续。
- study同段共同输入、运行槽100、追加12个月实验数不变、child禁止单独续算。
- 旧月快照不改、晚结果产生revision，累计月度对账，资金重测真实重算，端到端UI可操作。
- v1/v2/v3迁移到v4旧列逐行一致、重复初始化一致、quick_check/fk检查；资源仍符合现有限额。
- 完整现有unittest回归、既有rulelab浏览器流程、新增增量浏览器流程、JS语法和diff检查。

## 7. 部署与回滚

仅实验库做一致备份，放预算目录外；不复制/迁移大体积上游库。先在一致副本完整迁移/接管演练，确认旧26版本和原列保持，再协调停止实验worker、部署、初始化v4、重启API/worker，维持其他实际执行服务。

v3旧代码不能直接读v4；回滚应优先关闭新功能并保留v4兼容代码。新写入后不得用旧备份覆盖新成绩。所有现有series自动开关默认false；生产核验GET、服务心跳、原数据指纹、资源模式，启用真实批次另按用户实际操作/明确授权。

## 8. 任务拆解指引

### 8.1 拆解原则

按MS-1～4拆Evidence Waves，每个wave定义红测、绿门禁、Contract Matrix、Stop Condition。先固定本节方法/字段，owner变化必须同步依赖方。

### 8.2 建议任务

M-01：旧库迁移红测→schema/基线→幂等请求/段→完整checkpoint和容量。
M-02：源覆盖分类红测→共享捕获/版本→archive adapter→保护/压缩/预算。
M-03：分段等价红测→全部读取路由→事件前沿/pending→组/每日/重启。
M-04：接口红测→严格端点→累计/月报/段报告→UI→真实浏览器闭环。
M-05：独立对等/缺口/权限/资源审计→完整回归→一致副本→发布。

### 8.3 并行化建议

Data owner单独拥有store、source、档案模块/模型和对应tests；Engine owner只写worker及引擎tests；API owner只写service/API/月报与APItests；UI owner只写页面和浏览器fixture/tests。各owner可并行阅读，中央store不得多人写入。MS-1数据契约完成后引擎/API/UI可并行实现；集成时各自修复自身写集。

**STATUS**: DONE_WITH_CONCERNS

**REASON**: 已给出可分派的存储、输入、引擎、API与验收契约；实现须关闭tech-review的8项正确性门禁。
