# 结果型存储实施任务规格

## 摘要

- 用户已明确授权实施既定结果型存储方案，包括清理不再需要的终态明细；核心承诺为成绩及运行进度保留，结算后不再持久保存逐笔/闭轮记录。
- 16个实施任务，分5个Evidence Wave。冻结后的实际写集47个文件，其中41个代码/脚本/测试/用户文档和6个本feature产物。Core独占中央store与worker，Data独占保留/档案模块，Product独占service/API/UI，QA补独立反例，Release只做受控上线。下文写集以文件为边界，禁止跨owner并行改同文件。
- 关键路径：schema6小迁移与pending原子结算 → 有界依赖清理与新读路径 → 产品闭环 → 一致副本/完整回归 → 保留进度上线。高复杂度，初估约90～130次工具调用，实际以测试和问题闭环为准。
- **Blast Radius：高。** 实际写集47条路径（已按冻结时git status收敛；最初67条计划路径中的未用文件未改），涉及大于10个文件、数据模型/迁移、状态机、路由和有意删除冗余数据，无依赖新增、无安装命令、无真实投注/上游数据库改动。风险确认触发项已由用户“方案设计好就开始实施”及所认可方案覆盖，记录现有授权，不重复询问。
- 所有新增行为先写有效反例并运行RED，再实现GREEN；模块缺失导致的import失败不代替行为RED。W1～W4全部通过，最终冻结完整回归1259项通过、0失败/跳过；W5尚未发布；不能把专项结果当成发布全部门禁已通过。

## 1. Repo Preflight

| 项目 | 实际事实/检查来源 |
| --- | --- |
| Git | 生产默认main，基线`0b3d84d`且派发时干净；隔离开发目录`/www/wwwroot/pc28touzhu-lab-results`，分支`feat/rule-lab-results-only`。用`git status --short`、`git branch --show-current`核对。 |
| 技术栈 | `pyproject.toml`：Python>=3.8，setuptools，src layout；实际为stdlib WSGI/SQLite WAL和原生JS/HTML/CSS，无新增框架。 |
| CI | `.github/workflows/docker-image.yml`仅在main/tag构建并推送amd64/arm64镜像，未运行lint/test；因此本轮在开发机显式执行门禁。 |
| Python测试 | `env PYTHONPATH=src python3 -m unittest discover -s tests -q`；已有单元、临时真实SQLite/WSGI和只读来源副本测试。上次1197项约330秒是历史基线，不是本轮结果。 |
| 页面测试 | `tests/test_rule_lab_ui.cjs`、amount/continuation/management等使用临时真实WSGI；现有脚本会启动headless Chromium，不能直接认为本轮允许照跑。Product需按当前用户浏览器限制，优先可用受支持测试profile，或真实WSGI+DOM执行并明确未实测视觉边界；不得另开Chrome或读取用户Cookie。 |
| 语法/格式 | Python `compileall`或AST检查、`node --check`、`git diff --check`；没有独立类型检查命令。 |
| 模型与接口 | `rule_lab_store.initialize`当前schema5；业务常量650保存、100运行、每批100、历史单段90天；`app._handle_rule_lab_api`统一session/admin/owner、no-store、写体16KB、严格字段和查询参数白名单。 |
| 资源 | 1核/256MiB实验worker、1GiB实验目录预算；沿用现有批准队列及每日续算，不新增调度器。 |
| 数据风险 | 结算记录目前含终态并按30天入账日期留存；数据迁移此次允许删除指定冗余，必须逐项保护配置、stats、日账、pending、水位/时钟、活动状态、队列和ID。不能再用“所有旧行完全相同”作为清理验收。 |
| 副本 | ROOT正在生成`/tmp/pc28-results-baseline.db`只读一致快照；测试必须另复制到临时目录后写，不直接初始化该原快照。上游2GB库不复制、不写。 |
| 依赖安装 | 无。现有Python/Node测试依赖复用；不得为方便测试自行修改pyproject或安装新库。 |
| 尚未实测 | 最终schema6副本缩减率、容量/性能曲线、完整回归结果、发布耗时及视觉验证均待本轮记录。 |

## 2. 已冻结的首批契约与集成边界

实施计划以`plan.md`为技术细节权威；当前已由Review/ROOT确认以下门槛，接口补充先同步owner再写入，不能并行猜名字。

1. Core：`commit_step`同事务按顺序更新累计stats、日账、risk、决定/结算水位，再`DELETE`成功结算/跳过的pending；没有pending的重复outcome不重复入账。新decision仍检查批次和实验持久水位。
2. Core：`_save_round`仅保留active运行需要，不继续写closed；现有`risk_runtime_json.active_round`及累计轮次为权威。新增过去未统计的熔断指标不阻塞此轮降占用，也不能填0冒充完整历史。
3. Data：`rule_lab_retention.init_schema(c)`仅schema6加法和小型状态初始化，禁止自动全库清理；`maintain(store, *, now, limit=500)`按持久化游标有界执行并动态复核依赖。`status(c)`只读已缓存维护状态，至少`mode/version/phase/removed_rows/error/updated_at`，未知估算不用0代替。
4. Core接入Data hook：tick前先执行`management_maintenance(limit=500,max_pages=256)`，再将`500-processed_rows`传给`retention.maintain`（余量0则不调用）；两者共用每tick最多500行删除额度。retention返回`{processed_rows,scanned_rows,maintenance_pending,phase}`，processed_rows是实际删除/清理数量，scanned_rows另计，扫描与删除都受上限约束；关闭旧5分钟prune/maintain_archive调用以免叠加清理。Data只复用一个活动reclaim job，不能每tick另建。
5. Core详情：`recent=[]`、`recent_rounds=[]`、`detail_policy="results_only"`、`retention_version=1`、`pending_count`；累计结果和日/月结果继续原口径。
6. Data负责取消全A seed、依赖区间与离散pending保护、最新/活动前置检查点保留、完整历史payload清理与重测输入覆盖。提供`retention.update_frontier(c,batch,cursor,*,draws=(),outcomes=()) -> new_cursor`；Core在commit_step末尾同事务调用，outcomes必须是本次已成功入账项，不能把一页未消费draws的未来时间提前推进；cursor新增`retention_v1.first_open_at/processed_open_at/settled_available_at/frontier_verified`。Core接`retention_maintenance(*,now,limit=500)`及`replay_input_preview(user_id,batch_id,experiment_id,*,now)` store wrappers。历史段仅保留批次增量、范围与质量摘要，必须消除`_segment_summary`及双层summary中嵌套的每实验完整cumulative/runtime。
7. Product沿用既有权限和写接口幂等校验，去掉历史逐笔/轮次/旧月报revision UI；空recent不是零投注。新增`GET /batches/{bid}/experiments/{eid}/replay-preview`（无查询参数），响应`available,reason,requested_range,verified_range,input_basis`，其中input_basis为`local_frozen|current_source|unavailable`。Data提供本地coverage证明及源`replay_availability(predictor_id,start_date,end_date,*,observed_at)`有界可用性，Product在原replay提交时再次核对。不能仅MIN/MAX判定内部完整覆盖，锁忙/超时返回503；没有完整本地档案的>90天旧区间明确不可用。current_source明确不是原冻结视图；本轮不扩展异步全区间捕获，运行时发现缺档报错不宣称完整。
8. `/storage`新增`retention`对象并保持原实际空间字段；兼容`detail_retention_days=0`，与mode共同说明不留已结算明细。source旧记录清掉后依然保留旧成绩，不得把源缺档当普通无预测完成。
9. 正式`plan.md`已落盘，以上接口已对齐其§3～6，可据写集派发Core/Data/Product；跨文件调用未接好时只能标集成待验，不在模块内部悄悄加另一套默认数据留存策略。

## 3. 冻结后的实际文件写集与任务

以下按真实git status登记；每个路径只归一个owner。原计划中的domain统计、domain月报、CSS、管理模块等无需更改，已从实际写集移除。实现直接复用既有经济/风控算法，没有新增过去未统计的熔断指标。

| Task | Owner | 实际文件输出列表 | 最终内容/证据 |
| --- | --- | --- | --- |
| T01 | Core | `tests/test_rule_lab_results_store.py` | 原子pending删除、重复结算、事务失败、部分索引、共享维护行预算；测试专用结算轨迹helper供旧语义断言使用 |
| T02 | Data | `src/pc28touzhu/services/rule_lab_retention.py`；`tests/test_rule_lab_retention.py` | runtime策略状态、frontier回填、依赖集合、有界阶段清理、单个复用reclaim job |
| T03 | Core | `src/pc28touzhu/services/rule_lab_store.py` | schema6接线、pending-only原子落库、active-only轮次、详情契约、current_source原子marker、80%/90%门禁 |
| T04 | Core | `src/pc28touzhu/rule_lab_worker.py`；`tests/test_rule_lab_results_worker.py` | tick共享维护预算、source重测执行复核、时钟和断点、结果型路径 |
| T05 | Data | `src/pc28touzhu/services/rule_lab_series.py`；`src/pc28touzhu/services/rule_lab_archive.py`；`tests/test_rule_lab_results_series.py` | 有限暖机/离散pending/活跃冻结保护、取消全A seed、最新checkpoint与轻量段摘要 |
| T06 | Data | `src/pc28touzhu/services/rule_lab_source.py`；`tests/test_rule_lab_results_replay.py` | source实际覆盖证明、预览/执行两次校验、预测/开奖同时被清时准确阻止 |
| T07 | Data | `src/pc28touzhu/services/rule_lab_storage.py`；`scripts/maintenance/migrate_rule_lab_results.py`；`tests/test_rule_lab_results_migration.py` | readonly默认预览、显式apply/backup、可选有界drain、drain_complete真实状态、受保护字段哈希 |
| T08 | Product | `src/pc28touzhu/services/rule_lab_service.py`；`src/pc28touzhu/api/app.py`；`tests/test_rule_lab_results_api.py` | replay-preview、current_source/local_frozen区别、提交复核及原权限/字段约束 |
| T09 | Product | `src/pc28touzhu/ui/rule_lab.js`；`src/pc28touzhu/ui/rule_lab.html` | 移除逐笔/闭轮/旧月报revision，结果/风险/资金/日月/管理仍可用，存储清理状态明确 |
| T10 | Product | `tests/test_rule_lab_results_ui.cjs`；`tests/rule_lab_results_ui_server.py` | 真实临时WSGI/SQLite+jsdom全流程；没有启动Chrome或读取用户profile，未测绘制/布局/触屏 |
| T11 | Core | `tests/test_rule_lab_store.py`；`tests/test_rule_lab_worker.py`；`tests/test_rule_lab_risk.py`；`tests/test_rule_lab_long_streak_worker.py`；`tests/test_rule_lab_continuous_worker.py` | 原终态物理行断言转为测试临时轨迹与实际结果/状态；保留金额、风险、顺序与时钟断言 |
| T12 | Data | `tests/test_rule_lab_archive.py`；`tests/test_rule_lab_series.py`；`tests/test_rule_lab_continuous_qa.py`；`tests/test_rule_lab_qa.py` | schema6、短保留/旧快照/接管行为与原输入版本结果对齐 |
| T13 | Product | `docs/RULE_LAB.md`；`docs/RULE_LAB_RESULT_STORAGE_DESIGN_20261127.html`；`tests/test_rule_lab_continuous_monthly.py` | 文档更新当前实现；月报当前结果保留而历史revision不再积累 |
| T14 | QA | `tests/test_rule_lab_results_qa.py`；`tests/rule_lab_results_benchmark.py`；`.boss/rule-lab-result-storage/qa-report.md` | 8个独立反例，生产一致副本迁移/重启/恢复，3组参数矩阵和实际容量性能 |
| T15 | Core / Product按文件独占 | Core：`tests/test_rule_lab_comparison.py`、`tests/test_rule_lab_ops.py`；Product：`tests/test_rule_lab_api.py`、`tests/test_rule_lab_ui.cjs`、`tests/test_rule_lab_continuation_ui.cjs`、`tests/rule_lab_ui_server.py` | 真实旧契约受影响用例更新，继续保留资金和版本隔离断言；旧headless脚本本轮未执行 |
| T16 | Release | `.boss/rule-lab-result-storage/deploy-report.md`；临时发布脚本`/tmp/pc28-results-*` | 仅准备完成；等待ROOT冻结完整QA与exact commit GO后单执行者发布 |

Review另拥有`tech-review.md`、`plan.md`，Scrum拥有本文与`waves.json`；与上述47文件总数一致。没有改动`domain/rule_lab.py`、`domain/rule_lab_risk.py`、`domain/rule_lab_monthly.py`、`rule_lab.css`、`rule_lab_management.py`、`domain/rule_lab_continuation.py`，没有依赖、锁文件或unit变更。

## 4. 并行安全组与冲突处理

| Group | 可并行任务 | 前置 | 不可并行的交叉点 |
| --- | --- | --- | --- |
| G0 合同与RED | T01/Core、T02/Data测试部分、T08/Product测试部分、T14/QA反例 | Review已确认首批契约、本文写集 | 测试文件各自独占；不要先改核心代码再伪造RED |
| G1 主实现 | T03/Core、T02/T05/T06/Data、T08/T09/Product | 各自RED实测失败（已完成） | Data不写store；Product不写series/source；Core只按Data契约接hook |
| G2 集成 | T04/Core、T07/Data、T10/T13/Product | 相应G1代码接口已落盘 | 同一owner内任务串行；跨owner接口只消息协商，不越权修改 |
| G3 回归 | T11/Core、T12/Data、T15各文件owner、T14/QA | G2通过且运行时代码冻结 | 全回归启动后不临时加入/修改测试导致同进程加载版本混用；新发现失败冻结修复后重跑适当门禁 |
| G4 发布 | T16/Release | W4最终通过、ROOT提供exact commit | 任意时刻只有一个发布执行者，其他agent禁止重复停止服务/迁移 |

## 5. Contract Matrix

| ID | Contract | UI / Copy | Client Payload | Server Schema | Persistence / Business Rule | Test Evidence |
| --- | --- | --- | --- | --- | --- | --- |
| CM1 | 成绩保留、终态不留 | 结果与日/月曲线保留，无历史逐笔/闭轮入口 | 既有detail读取 | `detail_policy=results_only`，空recent兼容 | stats/daily/risk/watermarks与pending删除同事务 | T01/T03、results_store、results_worker |
| CM2 | 重启/重复不重复记账 | 进度/累计值稳定 | 既有worker提交 | revision与持久水位校验 | 无pending的outcome无效果；较早pending优先 | results_store重复/崩溃反例、results_qa |
| CM3 | 风控口径不改变 | 净利、回撤、连亏、轮数等既有指标一致；未知新指标不冒充0 | 同一冻结配置 | 原配置/赔率/模式 | 按每笔舍入和时间顺序更新，不以汇总缩放 | 旧/新参数矩阵、results_benchmark |
| CM4 | 有界输入依赖保护 | 暂停积压有原因，不宣称无限追平 | 原pause/append/queue | 普通/连续/study/暂停/待启动都纳入 | 暖机100+离散pending+未消费范围并集，同来源共享 | retention/results_series/独立共享消费者反例 |
| CM5 | 版本冻结与时钟 | 比较组同输入同截止 | 原continuation | archive revision/cutoff/warm exact refs | 旧方向不被新修订污染，合法pending状态按可见时间推进 | results_series三段修订、late pending |
| CM6 | 去掉历史副本而可续算 | 当前状态/必要段进度可见，无历史恢复承诺 | GET segments | `checkpoint_retained`真实 | 最新checkpoint+活动直接前置保留；旧完整payload删除前无依赖 | results_series、results_qa A清理后B等价 |
| CM7 | 重测只用可用输入 | 预览实际可用范围，缺区间说明 | 新只读replay-preview；原replay写接口 | `replay_input_unavailable`409，提交复核 | 建新对象前证明覆盖并保护；不截短/填0；旧成绩仍可读 | results_replay、results_api、results_ui |
| CM8 | 清理和物理回收真实 | 实际目录/空闲页/积压/维护进度分开 | GET storage / 既有reclaim | cached attribution有采样时间 | 每tick共享500行、每步256页，busy等待，允许超预算清冗余 | retention/results_migration、storage busy、results_qa |
| CM9 | 权限及原管理不回归 | 现有创建/编辑/复制/删除/续算入口继续 | 原严格payload/原request_id重试 | admin+owner/no-store/16KB/query白名单 | ID不复用，队列不复活，旧合法回执可恢复 | results_api+原API/management/queue回归 |
| CM10 | 生产迁移保留原成绩进度 | 原批次持续，空间实际下降 | 无生产测试POST | schema6仅增量，CLI apply前置明确 | 原配置/stats/daily/pending/cursor队列受保护；清掉允许冗余 | results_migration+真实副本hash+发布只读进度证据 |

## 6. Evidence Waves

机器可执行命令以同目录`waves.json`为准，均为argv数组；本文不将任何尚未运行结果写为通过。

| Wave | 范围 | 依赖 | Stop Condition | 状态 |
| --- | --- | --- | --- | --- |
| W1 | 原子结果落库、小schema、当前状态前沿 | 无 | 任一重复收益、旧stats/日账改变、pending丢失即停止 | passed |
| W2 | 依赖保留/快照收敛/重测覆盖/有界迁移 | W1 | 删活跃或共享输入、串冻结版本、重写已清历史、无限维护即停止 | passed（专项） |
| W3 | API/结果页/存储真实状态与旧入口兼容 | W1、W2 | 空recent被当零投注、越权、丢响应重复、新UI未真实执行即停止 | passed |
| W4 | 独立反例、容量性能、真实副本与冻结全回归 | W1～W3 | 非允许字段变化、未解释空间/性能回退、全量失败即停止 | passed |
| W5 | 单执行者受控上线与物理清理收敛 | W4 | 无备份、原进度/资源/数据校验不符或健康失败即停止并按记录恢复 | pending |

## 7. 本轮实施边界

不增加留全量明细开关、冷归档服务或新调度任务；不扩大预算/650版本上限；不读取用户浏览器凭证；不删除用户实验定义或上游数据。完整QA通过后才允许上线，用户已授权方案中的冗余清理，不再另请确认。

运行时代码与相应专项已冻结，实际写集已按git status收敛，具体最终契约和证据见下节。ROOT最终冻结完整回归1259项全部通过，W4已放行，W5仍等待生产发布验收。

**STATUS: DONE**

**REASON:** 最终实现、专项与冻结完整回归均已通过并登记，W1～W4完成；W5等待发布，不能提前将任务标为已上线。


## 8. 最终实现契约补充

- 保留策略复用`lab_runtime`的唯一键`result_retention`（版本1），记录phase/cursor/scanned_rows/removed_rows/error/updated_at/sweep_finished_at。完成一轮后idle至少60秒再开始下次扫描；没有新增定时器或大元数据表。
- `maintain`动态检查保护集合，单次processed和scanned均不超过传入limit；Core先原管理维护，再只给剩余500行预算。`prune`/`maintain_archive`成为结果型保留薄入口，旧30天/90天策略不再执行。
- schema6首次添加pending部分索引；停止按finalized_at维护终态历史。新结果不写终态行，现存旧终态仅由后台持久游标清理。
- Core `create_batch(..., replay_input_basis=None|current_source)`在创建事务里写cursor marker。source重测在开始、执行分页与完成边界再次验证；预测和开奖同时被源删不能被当成“当时无预测”。本地冻结与当前源输入基础在API上区分。
- `retention.update_frontier`读取连续段冻结revision/暖机精确输入，不能从全局最新修订缓存污染已成功结算的可见时间。`_backfill`只新增cursor.retention_v1，不改变原cursor key、revision、stats及日账；旧证据不足不伪造frontier_verified。
- CLI默认完全只读预览；`--apply --backup <实验目录外新路径>`仅初始化和补齐小型前沿；`--drain`额外运行有界清理/回收，必须同时apply。`--max-steps`限制总步数，`drain_complete`只有保留扫描与回收job都结束才true，步数用尽或busy返回false；不得拿applied=true冒充全清完。
- CLI对三个写者只接受明确inactive/failed且返回码3，其他/超时/未知拒绝。备份不覆盖旧文件，600权限，实验目录外；原结果/配置/pending/活动状态/队列受保护。输出备份72小时保留说明，无自动新增日程。
- Product使用真实WSGI/SQLite+jsdom执行页面全流程；没有启动其他Chrome/profile，没有视觉截图、手机触屏或布局测量。旧页面脚本按新契约调整，但本轮不把未执行的headless脚本写为通过。
- 新增日熔断指标不在本轮实现；原累计stats/risk算法原样复用。旧测试需要逐笔轨迹时由`tests/test_rule_lab_results_store.py`临时observer记录，仅用于测试，不写生产明细。

## 9. 实际测试证据及尚未结束的门禁

| 范围 | 实际结果 | 证据 |
| --- | --- | --- |
| ROOT W1独立边界 | 26通过/0失败/0跳过，2.687秒，exit0 | `/tmp/pc28-results-w1-root.log` |
| Core组合回归 | 123/0/0，38.465秒，exit0 | `/tmp/pc28-results-core-final.log` |
| Core最终索引专项 | 21/0/0，1.730秒，exit0 | `/tmp/pc28-results-core-index-green.log` |
| Data旧行为组合 | 88通过/0失败/2可选副本跳过，21.172秒，exit0 | `/tmp/pc28-results-data-final.log`；90是执行总数，不是90通过 |
| Data最终冻结专项 | 39/0/0，18.028秒，exit0 | `/tmp/pc28-results-data-final-frozen.log`；包含源双删和冻结前沿两个Review反例红→绿 |
| Product API组合 | 77/0/0，36.218秒，exit0 | `/tmp/pc28-results-product-api-final.log` |
| Product真实DOM流程 | `node tests/test_rule_lab_results_ui.cjs`，exit0 | `/tmp/pc28-results-product-ui-final.log`；jsdom，不是视觉验收 |
| 独立QA | 8/0/0，exit0 | `/tmp/pc28-results-qa-eight-final.log` |
| 真实一致副本 | 777,437,184字节主文件→100,503,552；目录100,536,320字节（约95.9MiB），原保护字段一致 | `/tmp/pc28-results-copy-qa.json`；这不是线上释放量，后加修复由专项/冻结全套补验 |
| 三组参数矩阵 | 100×100期、100×400期、600×100期全部经济摘要相同，终态/闭轮0行 | `/tmp/pc28-results-matrix-comparison.json`，所有脚本exit0 |
| ROOT首轮全项目 | 1257通过/2失败/0跳过，296.471秒，exit1；两项旧测试观测与新存储契约不符 | session91796，`/tmp/pc28-results-full-root.log` |
| ROOT最终冻结全项目 | **1259通过/0失败/0跳过，310.386秒，exit0** | session58590，`/tmp/pc28-results-full-final-root.log` |
| ROOT DOM再验证 | 真实WSGI/SQLite+jsdom流程通过，exit0；布局未测 | session33829，`/tmp/pc28-results-dom-root.log` |

ROOT完整门禁实际命令：`env PYTHONPATH=src RULE_LAB_MIGRATION_SNAPSHOT=/tmp/pc28-continuation-baseline.db RULE_LAB_READONLY_SOURCE_DB=/www/wwwroot/AITradingSimulator/pc28_predictor.db python3 -m unittest discover -s tests -q`。最终冻结回归实际退出0，W4已放行；W5仍须生产受控切换和真实空间收敛证据。


### 首轮完整回归失败及最终关闭

首轮两项失败均为旧测试的存储观测需要更新，未修改运行时代码来迎合断言：

1. 旧生产副本追加测试从已被新策略删除的终态逐笔表求和，错误得到0；改用仅测试进程的临时结算轨迹，继续逐分核对所有结果。两项副本专项最终2/0/0、14.803秒、exit0。
2. `tests/test_rule_lab_ops.py`假设被锁挡住的第二worker启动后`lab_runtime`必须0行；合法`result_retention`初始化元数据使原假设失效。Core新增该文件owner，改成启动前后全部lab表行保持相同且没有worker心跳，原“不推进实验”不变量更直接。`env PYTHONPATH=src python3 -m unittest tests.test_rule_lab_ops -v`实际4/0/0、0.270秒、exit0，日志`/tmp/pc28-results-core-ops-green.log`。

修复后运行时代码保持冻结，ROOT重跑全部1259项通过。Review的I1（源预测和开奖同时清理）与I2（冻结前沿被最新修订污染）另经2/0/0、0.772秒独立反例关闭。完整测试、独立反例及副本证据均保留真实首轮失败记录。
