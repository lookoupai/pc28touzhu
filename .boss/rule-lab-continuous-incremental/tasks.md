---
type: tasks
outputFor: [frontend, backend, qa]
dependencies: [existing-design, tech-review, plan]
---

# 实验增量续算开发任务规格

## 摘要

- **任务总数**：14；后端10、前端2、独立验收及发布2。
- **关键路径**：T-001→T-002→T-003→T-004（MS-1）→T-005/T-006（MS-2）→API/UI集成（MS-3）→T-013→T-014。
- **预估复杂度**：高；约90～150次有实质内容的工具操作，复杂状态测试可能追加；不是完成时间承诺。
- **Blast Radius**：高；计划28个明确文件（新建12、修改16），涉及实验账本、输入档案、worker、API与报告；0依赖安装、0锁文件/部署配置变更、0删除。
- **风险确认触发项**：文件数和加法schema/队列状态满足技能触发条件；本轮用户已明确授权实施增量续算，上级已确认必要可逆代码/测试/迁移在授权内，不重复索要许可。生产操作由根代理统一协调，不授权数据删除或默认启用所有实验。
- **共同契约**：以 `plan.md §4.2` 的Python方法、公共摘要/状态enum及 `§4.3` HTTP字段为唯一来源；不得另造签名。变更由Data/Tech Lead先通知全部消费者并同步本规格。

**已冻结的操作语义**：`compute_enabled` 是每日“自动续算”开关，不限制显式手动 append；采集开启、自动关闭时可只运行本次手动请求，且不隐式开启自动续算。关闭自动不取消已提交手动任务。活动段暂停/恢复使用旧 batch/study `state` 接口（UI“暂停本段计算”），调度器不得自动解暂停，采集可继续。

**版本令牌**：外部 `expected_revision` 必须是不透明字符串：`legacy:<statehash>` 或 `series:<id>:<rev>`；原样回传，禁止整数化/客户端递增。内部store/worker CAS整数与此外部令牌区分。

## 文档信息

- 功能：`rule-lab-continuous-incremental`；版本1.4；日期2026-11-27。
- 需求来源：用户“将修改上传 GitHub，然后开始开发实验增量续算功能”；既有HTML设计、`tech-review.md`、`plan.md`，代替另建PRD/架构。
- 本文使用 Boss `tasks.md.template` 结构，适配现有Python/原生JS工程。

## 0. Repo Preflight 摘要

| 事实 | 发现结果 | 证据 |
|---|---|---|
| 默认/当前分支 | main / feat/rule-lab-continuous-incremental，基线46c392f，开发前无未提交代码 | git symbolic-ref、branch、status |
| 生产仓库 | 原修改已推送；本轮源库保留策略AI项目b2b21fb已上线 | 根代理发布记录 |
| 栈/包管理 | Python3.12.3执行；pyproject声明>=3.8，setuptools；stdlib WSGI/SQLiteWAL；原生JS/CSS/HTML | pyproject.toml、app/store |
| CI | 仅DockerHub镜像构建推送，无lint/test门禁 | .github/workflows/docker-image.yml |
| Python测试 | `env PYTHONPATH=src python3 -m unittest discover -s tests -q`；全量包含单元/DB/WSGI集成，浏览器另跑 | tests/README.md、test_rule_lab*.py |
| 浏览器 | Playwright+临时WSGI/SQLite；已有 /tmp/pc28-cycle-ui-tools/node_modules 与 Chromium 可执行；无需安装 | tests/test_rule_lab_ui.cjs、test_rule_lab_amount_ui.cjs、路径检查 |
| schema来源 | store.initialize，现为v3；新加法v4，旧列逐行保持 | services/rule_lab_store.py |
| 业务常量 | 100运行/500保存，页≤200，暖机100；默认2秒tick，单次/分片≤90天；累计可超90天 | store/source/worker/domain，plan |
| 资源 | 现网加速1core，MemoryMax268435456，目录1GiB；不得恢复成旧默认0.2core | 根代理systemctl只读结果，store budget |
| 权限 | session+admin；user_id归属；源要求enabled且公开records/analysis | api._dispatch_rule_lab、source._PUBLIC |
| HTTP约束 | POST≤16KB、no-store；旧入口全拒绝query；仅新segments放行limit/before_id | api/app.py:1414附近 |
| 路由 | 单个WSGI分发函数，batch/study显式正则 | api/app.py |
| 上游保留 | 预测当前月＋前两月，开奖365天；早于已有最早数据不会恢复 | AI项目b2b21fb部署证据，覆盖设计需更新旧60天表述 |
| 现网接管样本 | batch8，26版本，22914期，实际3511343→3511344；replay_max3511545非起点；pending0 | 根代理只读核对 |
| 迁移副本 | /tmp/pc28-continuation-baseline.db，约23MiB；测试再复制，不修改原快照 | 根代理SQLite backup记录 |
| 未知项 | schema4各新表最终索引尺寸、压缩比、长期增长速度unknown，必须实测，不拿估算当保证 | 现有v3无这些实体 |

## 1. 任务概览

### 1.1 统计与风险

计划28文件：12创建、16修改、0删除；新增用例数量由测试场景展开后记录，当前不虚报。10个高复杂度、3个中复杂度、1个低复杂度任务。核心跨层风险R1～R8逐项进入Contract Matrix和wave停止条件。

依赖清单、锁文件、安装命令、构建/部署配置均不在写集。真实业务下注账本、上游大库、现网源保留配置不在写集。新增代码路径如需扩大，先更新对应owner写集，禁止多人编辑中央store。

## 2. 任务详情

Story S-001：固定原实验配置和成绩，从上次实际计算边界继续；可选持续保存后续输入及每日自动追加，提供可核对的分段与长期报告。

### Task T-001：系列、基线与加法迁移 [BE]

**执行者/owner**：Data。**复杂度**：高。**依赖**：无（先落实 plan §4.2 契约）。

**文件输出列表 / 写集**：

| 文件路径 | 操作 | 写集风险 | owner |
|---|---|---|---|
| `src/pc28touzhu/services/rule_lab_store.py` | 修改 | 共享文件，同owner任务串行 | Data |
| `src/pc28touzhu/services/rule_lab_series.py` | 创建 | 共享文件，同owner任务串行 | Data |
| `src/pc28touzhu/domain/rule_lab_continuation.py` | 修改 | 共享文件，同owner任务串行 | Data |
| `tests/test_rule_lab_series.py` | 创建 | 共享文件，同owner任务串行 | Data |
| `tests/test_rule_lab_store.py` | 修改 | 共享文件，同owner任务串行 | Data |

**实现步骤**：先增加 v3→v4、重复初始化、默认关闭、旧列保持和接管预检红测，再实现加法结构、稳定策略身份和只读预检。GET 不得建基线。真实基线保留原日期、26 个实验 ID、累计金额和风险状态。

**测试/验收用例**：旧库哈希不变；v1/v2/v3 兼容；不合格基线、暖机缺失、review_required/conflict/pending 边界明确拒绝。

**完成标志**：红测证据已记录（验证/发布任务引用前置红测），对应绿门禁通过，真实DB/API/DOM产物可查，无超写集变更。

### Task T-002：来源覆盖证明和不可变输入档案 [BE]

**执行者/owner**：Data。**复杂度**：高。**依赖**：T-001 的内部契约冻结；可与 T-001 的独立测试准备交错，不并行写 Data 文件。

**文件输出列表 / 写集**：

| 文件路径 | 操作 | 写集风险 | owner |
|---|---|---|---|
| `src/pc28touzhu/services/rule_lab_source.py` | 修改 | 独占 | Data |
| `src/pc28touzhu/services/rule_lab_archive.py` | 创建 | 共享文件，同owner任务串行 | Data |
| `tests/test_rule_lab_archive.py` | 创建 | 共享文件，同owner任务串行 | Data |

**实现步骤**：先构造源已归档、真实无预测、读取失败、输入修订的红测；实现 source_key、预测器/日期覆盖证据、共享版本捕获与完整 archive adapter。计划全部读取接口见 plan §4.2，不另造签名。首次启用还须分页导入本批次原区间仍在普通缓存中的A输入，并在导入完成前保护这些缓存；只保存最后100期足够续算，却不足以承诺旧年月的新金额重测。

**测试/验收用例**：不存在的历史行不能默认为 no_prediction；成功捕获才推进水位；同一冻结 archive_revision 重读相同；删除上游已捕获输入后仍可读本地；现有10月A缓存被有界纳入档案，确实缺失的旧月份明确拒绝完整重测。

**完成标志**：红测证据已记录（验证/发布任务引用前置红测），对应绿门禁通过，真实DB/API/DOM产物可查，无超写集变更。

### Task T-003：幂等请求、分段与完整检查点 [BE]

**执行者/owner**：Data。**复杂度**：高。**依赖**：T-001、T-002。

**文件输出列表 / 写集**：

| 文件路径 | 操作 | 写集风险 | owner |
|---|---|---|---|
| `src/pc28touzhu/services/rule_lab_store.py` | 修改 | 共享文件，同owner任务串行 | Data |
| `src/pc28touzhu/services/rule_lab_series.py` | 修改 | 共享文件，同owner任务串行 | Data |
| `src/pc28touzhu/domain/rule_lab_continuation.py` | 修改 | 共享文件，同owner任务串行 | Data |
| `tests/test_rule_lab_series.py` | 修改 | 共享文件，同owner任务串行 | Data |

**实现步骤**：实现 set_continuation/request_append/list_segments/input_coverage/prepare_due_appends/complete_segment；同 series 一个活动段、请求 ID+payload hash+revision 事务校验。保存各实验 stats、risk_runtime、日风险、活动轮、pending、实际游标、事件前沿与100期暖机。

**测试/验收用例**：重复和并发不重复建段/下注；同请求异载荷409；无新期不建空段；组原子排队、child 禁独立追加；版本数量和原定义不变；旧段快照不改。

**完成标志**：红测证据已记录（验证/发布任务引用前置红测），对应绿门禁通过，真实DB/API/DOM产物可查，无超写集变更。

### Task T-004：长期依赖保护、压缩和资源预算 [BE]

**执行者/owner**：Data。**复杂度**：高。**依赖**：T-003。

**文件输出列表 / 写集**：

| 文件路径 | 操作 | 写集风险 | owner |
|---|---|---|---|
| `src/pc28touzhu/services/rule_lab_store.py` | 修改 | 共享文件，同owner任务串行 | Data |
| `src/pc28touzhu/services/rule_lab_archive.py` | 修改 | 共享文件，同owner任务串行 | Data |
| `tests/test_rule_lab_archive.py` | 修改 | 共享文件，同owner任务串行 | Data |

**实现步骤**：保护所有启用 series 的未消费输入、100期暖机、pending、活动段及尚未完成归档的原区间缓存；已消费旧 payload 分页 zlib 压缩到 SQLite BLOB，压缩前后校验 hash。完整目录计量并给采集/检查点预留空间，计算和采集分别暂停。

**测试/验收用例**：暂停超过90天仍可续算；压缩不改读值/版本；磁盘满不推进水位、不删必要输入；共享 stream 不被单消费者关闭；24个月仅预算内目标。

**完成标志**：红测证据已记录（验证/发布任务引用前置红测），对应绿门禁通过，真实DB/API/DOM产物可查，无超写集变更。

### Task T-005：连续回放输入路由和事件时间前沿 [BE]

**执行者/owner**：Engine。**复杂度**：高。**依赖**：MS-1 数据门禁；可在 MS-1 完成前只阅读契约和准备红测。

**文件输出列表 / 写集**：

| 文件路径 | 操作 | 写集风险 | owner |
|---|---|---|---|
| `src/pc28touzhu/rule_lab_worker.py` | 修改 | 共享文件，同owner任务串行 | Engine |
| `tests/test_rule_lab_continuous_worker.py` | 创建 | 共享文件，同owner任务串行 | Engine |

**实现步骤**：先写同冻结输入 A+B 与 A→重启→B 的红测；连续模式全部读取走 archive_source，包括 tick pending sweep、history、evidence、settle 和 finish。逐期/轮次都禁止用现实时间越过下一决策。

**测试/验收用例**：同事件前沿逐笔、整数分净利、资金高点/回撤、连亏、活动轮/日风险等价；月末迟到结果和 pending 跨段；最后一注不强平；预算中断不漏入场。

**完成标志**：红测证据已记录（验证/发布任务引用前置红测），对应绿门禁通过，真实DB/API/DOM产物可查，无超写集变更。

### Task T-006：采集、每日到期及比较组连续执行 [BE]

**执行者/owner**：Engine。**复杂度**：高。**依赖**：T-005；与 API/UI 独立写集可并行。

**文件输出列表 / 写集**：

| 文件路径 | 操作 | 写集风险 | owner |
|---|---|---|---|
| `src/pc28touzhu/rule_lab_worker.py` | 修改 | 共享文件，同owner任务串行 | Engine |
| `tests/test_rule_lab_continuous_worker.py` | 修改 | 共享文件，同owner任务串行 | Engine |

**实现步骤**：既有单 worker 内公平执行有界捕获、手动等待请求和持久 daily_at 到期任务；默认00:10北京时间只处理已结束日，停机按≤90天片追赶。冻结组共同输入/截止、按100运行槽逐个子批次推进。

**测试/验收用例**：自动关闭时手动请求仍执行且不开自动；关闭自动不取消已提交手动请求；暂停本段计算后跨到期/tick不得自动恢复，采集继续；停机/重启/重复页无重算；当日手动追加不冒充完整自然日；一日唯一请求；组只发布共同完成段；单源异常不阻塞其他源。

**完成标志**：红测证据已记录（验证/发布任务引用前置红测），对应绿门禁通过，真实DB/API/DOM产物可查，无超写集变更。

### Task T-007：严格续算服务和 HTTP 契约 [BE]

**执行者/owner**：API。**复杂度**：高。**依赖**：plan §4.2/4.3 冻结；真实绿门禁依赖 MS-1/MS-2。

**文件输出列表 / 写集**：

| 文件路径 | 操作 | 写集风险 | owner |
|---|---|---|---|
| `src/pc28touzhu/services/rule_lab_service.py` | 修改 | 共享文件，同owner任务串行 | API |
| `src/pc28touzhu/api/app.py` | 修改 | 共享文件，同owner任务串行 | API |
| `tests/test_rule_lab_continuous_api.py` | 创建 | 独占 | API |

**实现步骤**：先写真实 WSGI 请求与 SQLite 产物红测，再接 batch/study 对等 continuation-preview、continuation、append、segments、input-coverage。严格字段/日期/布尔和不透明字符串expected_revision（拒绝整数）；只有 segments 开放 limit/before_id 查询白名单。

**测试/验收用例**：匿名401、非管理员403、他人404、child409；16KB/no-store；未知参数拒绝；GET零写入；同幂等键找回结果；202等待、201建段、200重试/追平匹配实际状态。

**完成标志**：红测证据已记录（验证/发布任务引用前置红测），对应绿门禁通过，真实DB/API/DOM产物可查，无超写集变更。

### Task T-008：累计、分段、月报与长期金额重测 [BE]

**执行者/owner**：API。**复杂度**：高。**依赖**：T-007；所需存储快照由 Data 通过 T-003/T-004 提供，API 不写 store。

**文件输出列表 / 写集**：

| 文件路径 | 操作 | 写集风险 | owner |
|---|---|---|---|
| `src/pc28touzhu/services/rule_lab_service.py` | 修改 | 共享文件，同owner任务串行 | API |
| `src/pc28touzhu/domain/rule_lab_monthly.py` | 修改 | 独占 | API |
| `tests/test_rule_lab_continuous_monthly.py` | 创建 | 独占 | API |

**实现步骤**：报告使用 series 有效区间；旧月份冻结/修订另存 revision；段返回每实验增量及累计截面。长期新金额重测读取档案并分≤90天片，缺档明确拒绝，不线性放大收益。

**测试/验收用例**：月度与日账/累计对账；旧段/旧月不被新增月份覆盖；非可加回撤不相减或相加；time_complete/pending_count/coverage_quality/comparable 独立；长期重测真实运行。

**完成标志**：红测证据已记录（验证/发布任务引用前置红测），对应绿门禁通过，真实DB/API/DOM产物可查，无超写集变更。

### Task T-009：新增浏览器 fixture 和交互红测 [FE]

**执行者/owner**：UI。**复杂度**：中。**依赖**：plan §4.3/4.5；可与 Data/Engine 独立准备红测，不改现有共用 fixture。

**文件输出列表 / 写集**：

| 文件路径 | 操作 | 写集风险 | owner |
|---|---|---|---|
| `tests/rule_lab_continuation_ui_server.py` | 创建 | 独占 | UI |
| `tests/test_rule_lab_continuation_ui.cjs` | 创建 | 独占 | UI |
| `tests/test_rule_lab_ui.cjs` | 修改，仅旧“不支持追加日期”帮助文案断言 | 独占 | UI |

**实现步骤**：基于真实临时 WSGI/SQLite 创建可接管旧批次、源追加和 worker tick 工具；外部请求禁止。测试只用本地 fixture，无生产账号或凭证。

**测试/验收用例**：首次预览、开关保存、手动追加、等待/阻塞、重复提交、响应丢失找回、暂停计算继续采集、历史段/月报、1440/390宽度均有可观测断言。

**完成标志**：红测证据已记录（验证/发布任务引用前置红测），对应绿门禁通过，真实DB/API/DOM产物可查，无超写集变更。

### Task T-010：续算界面和状态展示 [FE]

**执行者/owner**：UI。**复杂度**：高。**依赖**：T-009 红测；接口字段冻结后可与 Engine/API 实现并行，真实绿门禁等 T-006/T-008。

**文件输出列表 / 写集**：

| 文件路径 | 操作 | 写集风险 | owner |
|---|---|---|---|
| `src/pc28touzhu/ui/rule_lab.js` | 修改 | 独占 | UI |
| `src/pc28touzhu/ui/rule_lab.html` | 修改 | 独占 | UI |
| `src/pc28touzhu/ui/rule_lab.css` | 修改 | 独占 | UI |

**实现步骤**：沿用既有样式在详情增加增量续算：已计算/已采集/已结算、开关、截止日期、原版本数、段列表。区分“自动续算”和既有state控制的“暂停本段计算，仍保留后续数据”，结构化显示 blocked原因。丢失响应先GET找回，禁止换 request_id 盲重试。

**测试/验收用例**：UI enum 只使用 plan 生命周期；无误导完成/连续标记；手机无整页横溢；键盘焦点、加载/空态/错误态可用；未启用批次保持默认关闭。

**完成标志**：红测证据已记录（验证/发布任务引用前置红测），对应绿门禁通过，真实DB/API/DOM产物可查，无超写集变更。

### Task T-011：旧续接预检与 schema 测试兼容 [BE]

**执行者/owner**：Data。**复杂度**：中。**依赖**：T-004；不与 Data 其他任务并行落盘。

**文件输出列表 / 写集**：

| 文件路径 | 操作 | 写集风险 | owner |
|---|---|---|---|
| `tests/test_rule_lab_continuation.py` | 修改 | 独占 | Data |
| `tests/test_rule_lab_store.py` | 修改 | 共享文件，同owner任务串行 | Data |
| `tests/test_rule_lab_risk.py` | 修改，仅schema断言3→4 | 独占 | Data |
| `tests/test_rule_lab_comparison.py` | 修改，仅schema断言3→4 | 独占 | Data |

**实现步骤**：按实际新语义更新旧只读预检/版本断言，保留原始状态、所有权、无写副作用及旧赔率/风险冻结测试。不得通过删除旧断言来掩盖回归。

**测试/验收用例**：原规则实验室存储、风险、比较和续接基础测试全部通过；迁移新增列不替换旧列值。

**完成标志**：红测证据已记录（验证/发布任务引用前置红测），对应绿门禁通过，真实DB/API/DOM产物可查，无超写集变更。

### Task T-012：产品说明和能力边界 [BE]

**执行者/owner**：API。**复杂度**：低。**依赖**：T-008/T-010 实际功能可运行后。

**文件输出列表 / 写集**：

| 文件路径 | 操作 | 写集风险 | owner |
|---|---|---|---|
| `docs/RULE_LAB.md` | 修改 | 独占 | API |

**实现步骤**：说明实际入口、自动开关默认关闭、捕获与计算差异、手动/每日追加、历史段/月报、源删除和配额限制；更新原“无法续算”文字。只写已验收行为，不宣称无限保留/无限离线恢复。

**测试/验收用例**：文案与真实返回枚举、90天分片、100/500容量和1GiB预算一致。

**完成标志**：红测证据已记录（验证/发布任务引用前置红测），对应绿门禁通过，真实DB/API/DOM产物可查，无超写集变更。

### Task T-013：独立正确性、迁移副本和资源验收 [SHARED]

**执行者/owner**：QA。**复杂度**：高。**依赖**：MS-1～MS-3 绿门禁；可提前只写未实现功能的独立红测，完成审计等待全部实现。

**文件输出列表 / 写集**：

| 文件路径 | 操作 | 写集风险 | owner |
|---|---|---|---|
| `tests/test_rule_lab_continuous_qa.py` | 创建 | 独占 | QA |
| `.boss/rule-lab-continuous-incremental/qa-report.md` | 创建 | 独占 | QA |

**实现步骤**：独立检查实现/测试质量，补关键反例；从 /tmp/pc28-continuation-baseline.db 再复制临时库迁移两次，比较旧列逐行hash和quick_check/FK/默认关闭。仅在副本接管批次8并追加，生产source只读，验证下一期3511344及26IDs。根代理统一跑一次全量和旧/新浏览器回归。

**测试/验收用例**：R1～R8全部有证据；无源A续B/缺B阻止/源修订/pending跨月/空间满；旧库原列一致；副本追加金额与日账正确；256MiB/1GiB/1core约束不变。

**完成标志**：红测证据已记录（验证/发布任务引用前置红测），对应绿门禁通过，真实DB/API/DOM产物可查，无超写集变更。

### Task T-014：发布、线上核验和可恢复记录 [SHARED]

**执行者/owner**：DevOps。**复杂度**：中。**依赖**：T-013通过，根代理协调执行发布。

**文件输出列表 / 写集**：

| 文件路径 | 操作 | 写集风险 | owner |
|---|---|---|---|
| `.boss/rule-lab-continuous-incremental/deploy-report.md` | 创建 | 独占 | DevOps |

**实现步骤**：仅停止实验worker，实验库SQLite一致备份放预算目录外，合并代码并加法迁移，重启API+实验worker。保持用户1core/256MiB；GET核对原成绩/配置/新入口、worker和实际执行器心跳。记录版本/备份/回滚路径。

**测试/验收用例**：不备份或迁移2GB上游库、不写真实业务库、不激活全部旧批次、不发真实消息/投注；旧v3代码不可直接读v4，新结果产生后禁止旧备份覆盖。

**完成标志**：红测证据已记录（验证/发布任务引用前置红测），对应绿门禁通过，真实DB/API/DOM产物可查，无超写集变更。

## 3. 实现前检查清单

- [x] 已有设计/plan/评审已读；现有代码、权限与资源事实已核对。
- [x] 隔离分支就绪；Python/浏览器依赖可用，无安装命令。
- [x] Data独占store/source/archive/continuation；Engine不写source/store；API不写store；UI不改现有共用fixture。
- [ ] 各owner先编写并实际执行新红测，再实现；记录日志路径与失败原因。
- [ ] Data确认plan §4.2方法契约可供消费者实现，变更直接协调后同步文档。

## 4. 依赖和验收

### 4.1 并行安全组

Data 实际负责人为 `/root/continuation_storage`。其生命周期/检查点实现可放入 `services/rule_lab_series.py`，中央 `rule_lab_store.py` 集成仍由该负责人独占。Data 可委派一个 Backend 子代理处理 archive 子组，唯一写集是 `services/rule_lab_archive.py`、`services/rule_lab_source.py`、`tests/test_rule_lab_archive.py`；其余 Data 文件由 storage 本体负责。两个子组文件互斥，父代理不得同时编辑子代理写集；具体子代理名称宣布后补登记。T-004 涉及两子组的部分由各自唯一写者完成，再由 storage 汇总 W1 证据。

调度历史：Engine/API/UI 初始仅准备红测，再经根代理放行实现。Data 的 archive 子代理因平台并发限制中断，确认子代理闲置后由 `/root/continuation_storage` 接回 archive/source/archive-tests 写权；Data 全局owner不变，不存在同时写入。新增的 risk/comparison 两文件只改schema期望3→4，既有 UI 脚本只更新过时日期帮助断言。

验收最终记录（发布前）：W1 最终65项通过（4.414秒），最后公平性补测3项通过（3.084秒）；W2 原69项通过（41.687秒），新增两项公平性后完整Engine专测15项通过（17.099秒）。W3 API 59项通过（32.307秒）、新增浏览器真实流程通过，根代理既有规则实验室及金额浏览器均通过。W4 根代理完整套件实际运行1158项通过（328.548秒）；最后公平性变更后，再跑全部实验室测试238项通过（116.141秒），包含只读生产副本/快照测试，0跳过。独立QA在256MiB约束下10项通过（39.141秒），峰值104MiB，QA报告正在整理。W1～W4门禁已通过，W5发布仍待执行。

以上分别记录真实执行结果；未运行一套合并后的“1161项全量”，不得据新增用例数推算或宣称。W2/W3既有证据日志分别为 `/tmp/pc28-continuation-engine-final.log`、`/tmp/pc28-continuation-ui-flow-final.log`，其他日志由根代理/QA最终报告登记。

| 组 | 可推进任务 | 前置/写集约束 |
|---|---|---|
| A | Data T-001～T-004串行；Engine/API/UI只读契约、准备各自红测 | 各测试文件互斥；同Data文件不并行 |
| B | Engine T-005→T-006；API T-007→T-008；UI T-009→T-010 | MS-1通过后可并行正式实现；依据冻结签名，真实集成等待依赖方绿门禁 |
| C | Data T-011；API T-012；UI修复自身浏览器写集 | 各owner完成其B组任务；禁止跨owner代写 |
| D | QA T-013 | MS-1～3通过；QA新增独立测试，不修改开发owner文件；缺陷回派对应owner |
| E | DevOps T-014 | QA通过；根代理控制实际生产操作 |

生产实现顺序以证据门禁为准；不要求独立测试准备空等，也不得用“可以并行”绕过MS-1/MS-2验收。

### 4.2 Evidence Wave

机器命令在 `waves.json`，使用argv，无shell拼接/重定向。红测在功能实现前运行；wave4/5引用前序已记录红测，独立QA新增反例如发现缺陷须补红→绿。完整回归只由根代理在集成稳定后统一执行，子agent不重复运行整仓套件。

| Wave | 范围 | Owner | 红测/绿门禁 | Contract | Stop Condition |
|---|---|---|---|---|---|
| W1 | v4、基线、请求、档案、覆盖、配额 | Data | series/archive新测试红→绿＋既有store/source/continuation | CM-001～005、009 | 旧列改变、缺档冒充无预测、非原子水位、依赖被prune删除，任一存在即停 |
| W2 | 有界采集、事件前沿、真正续算、study/daily | Engine | continuous_worker红→绿＋旧worker/risk/comparison | CM-006～008、010 | 同前沿A+B不一致、未来结果提前生效、重启重结算或组输入不同即停 |
| W3 | API、累计/分段/月报、UI | API/UI | continuous_api/monthly/UI红→绿＋真实浏览器 | CM-001、007、010～013 | 权限/幂等/响应丢失/旧报告保护/手机主流程任一失败即停 |
| W4 | 独立QA、副本接管、资源、全回归 | QA/根代理 | 新QA反例＋W1～3红测证据；全量及旧/新UI | 全部 | R1～R8未关闭、旧列hash变化、26ID变化、超资源即不可发布 |
| W5 | 授权上线和只读核验 | DevOps/根代理 | 复用W1～4证据；真实服务/DB/GET核验 | CM-014 | 备份/迁移失败、默认自动启用、健康失败，停止扩展且保留新成绩 |

### 4.3 Contract Matrix

| ID | Contract | UI / Copy | Client Payload | Server Schema | Persistence | Business Rule | Test Evidence |
|---|---|---|---|---|---|---|---|
| CM-001 | 默认关闭且GET只读 | 未启用/保留后续数据/自动续算分开 | continuation指定request_id/revision/两开关 | plan §4.2/4.3，compute自动要求capture，手动不要求compute | series+baseline只在POST生成 | 迁移不自动采集/计算 | series、continuous_api、UI |
| CM-002 | 稳定策略和实际起点 | 原26版本、已算至实际期号 | 不接收金额/赔率等配置改写 | 配置hash与revision核对 | 原batch/exps不改，基线段 | 3511343→3511344，不能跳到maxbound | series、continuous_qa |
| CM-003 | 已归档输入可独立续算 | 已采集水位区别已计算 | 不由客户端回传输入状态 | archive adapter冻结revision | 不可变版本/coverage/manifest及原A的有界导入 | history/pending/evidence全读档案，无上游fallback | archive、continuous_worker |
| CM-004 | 无预测、缺档、读取失败不同 | 真实缺样/等待/缺档分别展示 | 无 | coverage class稳定，错误有code | 成功读取证据和疑似清理日期 | 无证据缺行不能当no_prediction | archive、API、UI |
| CM-005 | 保护依赖并诚实限额 | 采集暂停/计算暂停/空间占用 | capture保留/compute自动；本段暂停用state（compute⇒capture） | 预算1GiB、页200、暖机100 | 未消费/pending/暖机引用，压缩保hash | >90天暂停不清依赖；空间不足不偷偷丢档 | archive、continuous_qa |
| CM-006 | 分段与完整回放等价 | pending可跨段，不冒充全结算 | append目标end_date | 同事件前沿/引擎版本 | 全量checkpoint和累计事务 | 净利/回撤/连亏/轮/日风险一致，段末无现实flush | continuous_worker、continuous_qa |
| CM-007 | 手动追加幂等且无新期不造段 | 等待输入/已追平/本次新增 | request_id,expected_revision,end_date | expected_revision不透明字符串，严格日期≤今天，单片≤90天 | 请求唯一、一个活动段 | 自动关闭仍可手动；不开自动；202/201/200对应真实结果；重复无重记 | series、continuous_api、UI |
| CM-008 | 自动追加可停机补跑 | 次日到期、暂停计算仍采集 | daily_at/timezone（Asia/Shanghai） | 固定北京时间，00:10默认 | next_run_at与日请求持久化 | 已结束日＋90天分片，公平共享stream；关闭自动不取消手动；不恢复用户暂停段 | continuous_worker |
| CM-009 | 追加不增加实验数量 | 原版本数不变 | 无新模板/金额字段 | 100运行/500保存不变 | segment增加，experiment稳定 | 等待采集/到期不占运行槽 | series、worker、QA |
| CM-010 | study共同冻结输入 | 上一共同完成段/正在追加 | study同等入口；child请求拒绝 | child409，组共同manifest/cutoff | 组原子段和child进度 | 不混合不同段差值；按槽运行 | series、worker、API、monthly |
| CM-011 | 报告可核对且旧快照保留 | 累计/本段/按月/历史段 | 有界segments limit1..50,before_id | pending/coverage/time_complete/comparable独立 | 月报revision、段累计截面 | 不相加最大回撤；今日部分时段不能称完整日 | monthly、API、UI |
| CM-012 | 真实长期新金额重测 | 超90天可分片重测/缺档明确原因 | 既有replay金额和风险模式 | 真实档案回放，不能线性缩放收益 | 新实验定义+分片状态 | 原版不变，缺档不静默使用上游残片 | monthly、API、worker |
| CM-013 | 访问边界与移动端操作 | 真实服务端中文错误 | 新路由严格未知字段拒绝 | admin/session/owner、16KB/no-store | 越权无写入 | 只有segments允许指定query；丢失响应GET找回 | continuous_api、UI |
| CM-014 | 加法上线可核查 | 功能可用但旧批次未自动开启 | 发布不代用户启用 | schema4兼容检查 | 旧列hash、quick_check、FK、一致备份 | 不写真实业务库/不发投注；保留新成果后恢复 | continuous_qa、qa/deploy报告 |

## 5. 文件变更汇总

以下为全局唯一owner表；同文件出现在多个任务时只允许该owner顺序修改。若实施选择将大模块拆为新文件，先向根代理提交精确路径并更新此表，不以通配符隐式扩大写集。

| 文件 | 操作 | owner |
|---|---|---|
| `src/pc28touzhu/services/rule_lab_store.py` | 修改 | Data |
| `src/pc28touzhu/services/rule_lab_series.py` | 创建 | Data |
| `src/pc28touzhu/domain/rule_lab_continuation.py` | 修改 | Data |
| `tests/test_rule_lab_series.py` | 创建 | Data |
| `tests/test_rule_lab_store.py` | 修改 | Data |
| `src/pc28touzhu/services/rule_lab_source.py` | 修改 | Data |
| `src/pc28touzhu/services/rule_lab_archive.py` | 创建 | Data |
| `tests/test_rule_lab_archive.py` | 创建 | Data |
| `src/pc28touzhu/rule_lab_worker.py` | 修改 | Engine |
| `tests/test_rule_lab_continuous_worker.py` | 创建 | Engine |
| `src/pc28touzhu/services/rule_lab_service.py` | 修改 | API |
| `src/pc28touzhu/api/app.py` | 修改 | API |
| `tests/test_rule_lab_continuous_api.py` | 创建 | API |
| `src/pc28touzhu/domain/rule_lab_monthly.py` | 修改 | API |
| `tests/test_rule_lab_continuous_monthly.py` | 创建 | API |
| `tests/rule_lab_continuation_ui_server.py` | 创建 | UI |
| `tests/test_rule_lab_continuation_ui.cjs` | 创建 | UI |
| `tests/test_rule_lab_ui.cjs` | 修改，仅旧不支持追加日期帮助断言 | UI |
| `src/pc28touzhu/ui/rule_lab.js` | 修改 | UI |
| `src/pc28touzhu/ui/rule_lab.html` | 修改 | UI |
| `src/pc28touzhu/ui/rule_lab.css` | 修改 | UI |
| `tests/test_rule_lab_continuation.py` | 修改 | Data |
| `tests/test_rule_lab_risk.py` | 修改，仅schema断言3→4 | Data |
| `tests/test_rule_lab_comparison.py` | 修改，仅schema断言3→4 | Data |
| `docs/RULE_LAB.md` | 修改 | API |
| `tests/test_rule_lab_continuous_qa.py` | 创建 | QA |
| `.boss/rule-lab-continuous-incremental/qa-report.md` | 创建 | QA |
| `.boss/rule-lab-continuous-incremental/deploy-report.md` | 创建 | DevOps |

## 6. 代码规范和验证边界

沿用Python snake_case、原生JS/既有CSS，不引入框架/依赖。JSON/API字段以plan契约为准，输入统一在边界验证；资金仍用整数分、冻结原赔率/规则。源只读，实验库独立；所有开发测试用临时库，生产一致快照只能再复制后迁移。子agent不得直接操作生产服务/数据库、提交或推送；由根代理集中发布。

新增测试应断言真实API/DB/DOM行为，不断言某私有函数“被调用”来代替正确性。禁止降低现有测试或修改全局常量来绕过容量、快照与时序失败。QA报告如有受限场景必须具体列明，不用通过数替代覆盖。

## 变更记录

| 版本 | 日期 | 变化 |
|---|---|---|
| 1.0 | 2026-11-27 | 按最终plan冻结四owner写集、五个证据wave、14条跨层契约；必要实施已授权 |
| 1.1 | 2026-11-27 | 增加 Data 生命周期 helper rule_lab_series.py；明确 storage 本体与 archive 子组互斥写集、W1 前仅依赖方红测准备 |

版本1.2澄清（2026-11-27）：compute_enabled仅自动每日追加；手动追加独立，本段暂停沿用state；expected_revision明确为不透明字符串。

版本1.3实际写集（2026-11-27）：根代理授权 Data 修改 risk/comparison 的schema断言、UI 修改旧帮助文案断言；记录Data接回闲置子代理写集及W2/W3分项证据，W1最终闭环和W4仍未完成。

版本1.4验收状态（2026-11-27）：按根代理实际结果记录W1～W4通过，分别保留1158项全量与后续238项实验室回归证据，不推算未运行的合并全量数；W5仍待发布。
