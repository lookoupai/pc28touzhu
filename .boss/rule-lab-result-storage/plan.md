# 结果型实验存储实施计划

## 摘要

- 先改变新增写入，再以持久游标清既有冗余，最后物理回收；不清掉历史后继续制造相同终态明细。
- Core独占中央store/worker和已有领域统计；Data负责输入依赖、series/archive、迁移清理与存储；Product负责服务/API/UI契约。中央store所需Data入口由Core集成，禁止并行修改中央文件。
- 明确schema6、结果型详情、保留元数据与重测输入基础；复用既有权限、事务、队列、资源限制和回收流程，不增加新依赖或进程。
- 分阶段验证原子落库、依赖清理、真实页面、完整回归和生产副本。迁移保留结果与当前状态，允许删除仅限白名单冗余。
- 最终容量以真实固定副本和上线后实际字节为准；完整输入积压仍受保护，不能为了达标删除必需输入。

## 1. 模块与里程碑

| 模块 | 职责 | 依赖 | 验收 |
| --- | --- | --- | --- |
| M1 结果原子落库 | pending结算后删除、闭轮只累计、详情声明results_only、当前时钟持久化 | 固定M2 helper契约 | 同输入结果一致；重复/崩溃不重复记账；终态行不增长 |
| M2 依赖保留与迁移 | schema6元数据、有限保护集合、有界清理、快照精简、seed/recheck防重灌、真实回收 | M1停止新终态；共享契约 | 普通/连续/暂停/study/pending安全；受保护值不变；文件实际缩小 |
| M3 结果页与重测范围 | 隐藏明细/旧修订，保持曲线/导出；显示清理状态；预览和提交核对重测输入基础 | M1详情、M2状态及范围方法 | 真实接口和临时页面可用、权限/缺档/丢响应正确 |
| M4 独立QA与容量 | 矩阵、故障交错、生产副本、完整回归、容量/性能曲线 | M1–M3冻结 | R1–R12关闭或明确实际边界，不把跳过计为通过 |
| M5 受控发布 | 备份、加法迁移、按原状态恢复、后台收敛、线上只读验收 | M4通过 | 原成绩/602IDs/进度保持、真实空间下降、新终态不再增长 |

## 2. 范围与不变量

保留现有所有结果字段：stats、daily、risk_runtime中的累计和活动状态、冻结条件/赔率/金额、游标、决定/结算水位。既有资金/最大回撤/最长连亏/轮次统计不能从精简后的结果重新推算或重新累计。

`lab_decisions`升级后是pending工作集。新输入conflict或skipped已计入既有跳过统计后亦不留下终态明细。活动轮以risk_runtime为权威；闭轮只有累计结果，`lab_rounds`不再作为历史展示源。

尚不存在的日熔断触发数等指标不属于本轮强制新增范围。若顺便新增，只能从明确起点记录并返回未知历史状态；不能影响原字段或伪造0。

## 3. 共享接口与写入责任

以下是Core/Data/Product间固定契约。内部方法调用方式可保持项目现有命名风格，但变更必须同步此节和消费者，不能各自添加互不相同的接口。

### 3.1 Data提供的保留模块

建议新模块`rule_lab_retention`，导出：

```python
init_schema(c)
update_frontier(c, batch, cursor, *, draws=(), outcomes=()) -> dict
maintain(store, *, now, limit=500) -> dict
status(c) -> dict
replay_input_preview(store, user_id, batch_id, experiment_id, *, now) -> dict
```

- `init_schema`只做加法结构/轻量维护状态初始化，不在API启动时扫描/删除百万行；重复调用幂等。Core在原initialize末尾调用并将user_version置6。
- `update_frontier`返回新cursor，不单独提交。Core在`commit_step`本次成功结果确定后、更新batch前调用。Data不得从传入一整页draws把尚未消费的未来可见时间提前推进；只采纳本次实际处理/实际成功结算的期。Core应传入**已成功入账outcomes**，不是原始待尝试outcomes。
- `maintain`单次扫描及删除数量均有明确上限，返回`processed_rows`（实际删/清理的行数）、`scanned_rows`、`maintenance_pending`、`phase`。被保护行的扫描推进计入`scanned_rows`预算，不能为了凑500条可删行扫完整库；Core只按实际`processed_rows`扣删除余量。
- `status`只读已保存的维护元数据，不做dbstat或全表count；状态至少包括`mode:'results_only'`、`version:1`、`phase`、`removed_rows`、`error`、`updated_at`。未知的字节/待清理行估算用null或不返回，不填0冒充已清完。
- `replay_input_preview`只检查owned本地输入、既有冻结可用性和当前已算有效区间，不创建batch/series/job，不写入保护。下节定义响应。

Core在RuleLabStore加薄包装`retention_maintenance(*,now,limit=500)`及`replay_input_preview(user_id,batch_id,experiment_id,*,now)`；Data不直接编辑中央store。

### 3.2 Core落库顺序

1. BEGIN IMMEDIATE后重读batch，校验revision、deleting/running、游标不倒退。
2. 新决定受old batch watermark及last_decided_issue约束，记录必要pending并更新决定水位。
3. outcome按期号排序；只有pending存在且没有更早pending时才处理。保留可见时间和冻结输入校验。
4. 应用原stats/daily/risk算法，更新last_settled_issue和活动轮；成功终态直接DELETE对应pending。跳过同样只计一次。
5. 保存全部脏实验状态、真实成功结算的时间前沿、batch cursor及revision，COMMIT。异常回滚当前事务。

外部看到的两种状态只能是“pending在、结果未加”或“结果已加、pending消失”。不能先删pending后在另一事务加结果。

### 3.3 时钟前沿

cursor增加具名`retention_v1`对象，至少保存：

- `first_open_at`：实际首个回测目标开奖时间；不能把日开始时间冒充实际边界。
- `processed_open_at`：当前实际last_issue的开奖时间。
- `settled_available_at`：已经成功应用的结算输入最晚可见时间。
- `frontier_verified`：是否已有可验证证据；不完整旧迁移须明确false而非猜测。

既有`simulation_time`、last/scan/next水位、history_floor与evidence不改变语义。Data先通过旧记录/缓存/保留档案在有界迁移中补齐，未验证批次的必要旧边界输入受保护，直到证据补齐或明确标为不可接管；不得先清数据后回填。

Data修改_snapshot/_preview/_summary等读路径；Core修改_monthly_report读取持久边界。跨owner/非法输入不泄漏边界元数据。

### 3.4 维护与物理回收配额

Core在tick前半段执行既有management维护，取得实际`maintenance_rows`；给retention的余量为`max(0,500-management_rows)`。两类维护共享500行/tick，不能每类各500后宣称总限额500。旧5分钟`prune`/`maintain_archive`路径在新策略下禁用或委托同一机制，避免叠加。

保留维护进度推动原短循环，空闲/资源暂停也能清理。单tick2秒预算不提高，来源采集和结算不能被长事务饿死；锁忙保持任务可重试。

retention释放页后复用现有回收队列：最多一个活动自动回收job，不每tick新建回执；管理员手动请求仍幂等。物理回收总预算保持每tick至多256个freelist页，忙时等待，按实际文件字节报告。Data可给storage模块增加共享低层回收helper，但必须与management唯一调用预算协调。

清理与已冻结pending结算属于保留成绩所需工作，使用现有reserve事务，不得被普通新增计算80%页限锁死；新增计算、输入采集和reserve写入使用统一命名的预算阈值，页面按实际较早生效的限制解释。已超预算库仍须能分批删除并回收；不能为了清理先调用会拒绝的新增数据门禁。

## 4. 输入与检查点依赖

### 4.1 必需保护集合

Data以同一写事务的当前batch/series/segment/queue状态计算保护；任何清理候选删除前必须重新验证当前依赖，不依赖事务外旧快照。

- 每个已开始batch（含普通completed/paused）的实际末100期暖机及验证过的前沿；没有series也不能漏。
- 所有pending离散期号和其精确冻结输入引用；不能用最老pending到现在的整个跨度代替。
- 所有活动/暂停连续段、study最慢子方案、未消费区间及各自archive revision。
- 已明确创建/排队的普通未开始回测按有限start/end范围保护已取得的共享输入；空游标不能pin全库。
- capture启用series的已采集未消费范围、已有小重查窗口及确需状态重查项。

同来源输入共用；开奖按全部来源消费者保护。可保留受保护期所需的多个revision，但不能因为全局某个旧revision存在就保护所有历史输入。优先保守保留有限受保护区间的多版本，然后以实测决定是否需要更细版本GC，不在首版过度优化。

### 4.2 已消费数据与元数据

取消默认全A seed；新adopt仅保存暖机/前沿，随后采集新增输入。迁移中已有seed游标不能重新打开已消费范围。coverage/recheck清理必须保存必要的范围质量摘要与清理边界，`covered_through_issue`和`after_issue`不能倒退。

旧输入经政策清理不等于此前采集缺档；报告继续保留原known_skips/冲突事实。所有warm引用必须携带固定方向/创建时间，合法同hash状态变化按可见时间更新。

### 4.3 快照和摘要

- 每batch仅保留最新可继续的完整checkpoint，以及活动段需要的直接前置checkpoint。study某子方案提前完成不允许清掉另一个子方案尚需的前置快照。
- 已完成段`baseline_json`只在计算段总结尚需要时存在，完成总结后移除完整payload；标识/哈希/汇总留存。
- `_segment_summary`改为批次级增量计数/金额、起止范围、pending数量和质量；不在child.summary_json及parent.summary_json重复保存每实验累计stats和完整runtime。
- 历史月快照只保留每月当前有效值。旧revision浏览取消；日汇总和当前月结果仍是权威。
- 旧payload移除使用明确`checkpoint_retained:false`或storage状态，不能伪造空字典为有效检查点。存在NOT NULL字段时用具名轻量标记并让所有读路径识别，或迁移重建该表；不能依赖无保护的`_unpack({})`继续运行。

## 5. 重测输入契约

新增只读路由：`GET /api/platform/rule-lab/batches/{batch_id}/experiments/{experiment_id}/replay-preview`。禁止查询参数、保持owner/no-store，响应：

```json
{
  "available": true,
  "reason": "",
  "requested_range": {"start_date": "2026-10-01", "end_date": "2026-11-26"},
  "verified_range": {"start_date": "2026-10-01", "end_date": "2026-11-26"},
  "input_basis": "local_frozen"
}
```

`input_basis`只取`local_frozen|current_source|unavailable`。requested_range取原实验实际已算的有效累计区间，不取尚未完成的未来请求。unavailable必须说明缺失/不支持的范围；源读取超时/锁忙返回503，不能返回available=true，也不能把暂时无法核验写成永远缺档。

1. Data本地预览检查完整目标范围、100期暖机、coverage与冻结版本；验证可用后，既有archive创建事务再次检查并发布新batch/series保护。不能先释放原依赖再创建新消费者。
2. 本地旧区间已清而源仍可用时，Product调用Data提供的源`replay_availability(predictor_id,start_date,end_date,*,observed_at)`，同源read-only/.25s预算，返回同样的范围和available/reason；不能用仅MIN/MAX当内部连续性证明。
3. current_source是按当前仍可读取记录重新模拟，明确可能与旧输入修订不同；创建/启动再次核验。已经取得的本地部分按新batch区间保护，尚未取得的上游数据不假称冻结。执行中源清理造成缺档必须停下并显示不完整，不能缩短范围或计作普通无预测完成。
4. 首版不新增异步完整复制旧区间的档案系统。普通回放现有<=90天范围继续遵守；累积区间超过该限制且没有可用完整本地archive时明确unavailable，不能在后端静默截短。
5. 原有POST replay请求体仍为stake_amount/risk_amount_mode；在提交路径内部重新验证输入基础。原实验与冻结赔率不变，复制的新参数从零计算。普通create/fork使用现有当前源口径，不宣称旧冻结输入。

Data/Core/Product若发现完整source覆盖无法在预算内核验，应返回可重试503或明确不支持，而不是放开无界扫描。记录该范围的验收证据，不能弱化“原本无预测”和“数据已清”区别。

## 6. 详情和页面契约

Core详情保持既有experiment/daily/monthly字段，新增：

```json
{"detail_policy":"results_only","retention_version":1,"pending_count":0,"recent":[],"recent_rounds":[]}
```

Product移除逐笔和历史轮次DOM及“无投注明细”误导文案；保留已有累计轮指标、当前活动轮、资金曲线、月度结果、CSV和比较功能。月报历史revision选择器移除，但不能删除当前结果和缺样提示。

storage在原字段上增加`retention`对象；已知精确字节来自维护采样并附时间，GET仅读缓存。`detail_retention_days`保持兼容时返回0并同时给明确mode，不能继续显示30天。

## 7. 实施与测试分波

| 波 | 内容 | 红测 | 绿门禁 | 下一波前提 |
| --- | --- | --- | --- | --- |
| W1 | 原子结果写入与schema6接点 | 结算后终态存在、重复恢复、闭轮历史产生 | 结果/日账/风险/水位相同且终态0；新旧对应worker测试 | Core接口可调用，Data可从只读版本边界接入 |
| W2 | 依赖保留、快照收敛、迁移/回收 | 普通completed、古老pending、暂停队列、源修订、旧seed重灌、重复summary | 对应保护集合及12段有界状态；真实副本结果哈希与实际缩文件 | source/store契约冻结 |
| W3 | 真实API与页面 | old UI仍展示明细、缺档重测误available | owner/no-store/错误码；真实临时WSGI页面结果可读、重测分支/管理正常 | Core+Data接口通过 |
| W4 | 独立QA、性能容量、全量 | 交错/故障反例 | 相同输入矩阵、1197既有全套语义更新后全绿及新增测试；真实资源证据 | 代码冻结且所有实际失败关闭 |
| W5 | 发布和实际空间收敛 | 生产不制造红测 | 原服务状态/结果/ID/进度保持、后台清理完成、文件下降、新终态不增长 | W4明确通过 |

旧测试若断言历史逐笔行数/旧revision存留，必须由相应owner明确改成新的用户可观察结果和运行状态契约；不能删除测试或加skip掩盖差异。精度、并发、权限、失败处理断言不能削弱。测试允许在临时库保存旧逐笔轨迹用于流式对照，不加入生产长期日志。

## 8. 发布与容量验收

迁移只做加法及有界元数据补齐；新程序先停止新增终态，再通过后台游标清理已有大表。维护CLI统一命名为`scripts/maintenance/migrate_rule_lab_results.py`，默认预览，显式apply需检查实验三个写者已停止、外置备份和可用空间；不能凭systemctl未知/失败查询推断已停。发布前保存生产一致副本与受保护字段摘要；线上暂停queue→worker→API，备份、升级、校验后只恢复原active服务。

实测须分别给出：初始目录DB/WAL、可删终态规模、必要输入保护规模、清理完成目录、释放页、备份大小、峰值RSS及耗时。结果/必要pending/状态不变是硬门禁；150MiB量级目标受输入积压解释，不可以扩大预算或删必要数据“达标”。

开发初期约716MiB是动态生产状态。若开发期间空间保护暂停批次，记录实际原因；清理完成后按照已有资源恢复规则恢复原批准队列，不能复活用户手动暂停或已编辑退出队列的实验。

## 9. Scrum拆解约束

Core拥有中央store、worker、必要domain统计及其核心测试。Data拥有retention、series/archive/source/storage、维护CLI及其数据测试。Product拥有service/API/UI、页面fixture/浏览器/API测试和用户文档。中央文件不并行落盘，跨owner的修复由owner操作。

QA独立拥有反例测试与报告，DevOps拥有发布脚本/记录；所有生产写入等待根代理确认具体冻结commit和最终QA。任务拆解需列出所有旧测试的owner，避免全量失败后多人同时修改同一文件。

**STATUS: DONE**

**REASON:** 实施契约、边界与门禁已冻结，可据此拆解并开展开发；本文未声称实现或测试完成。
