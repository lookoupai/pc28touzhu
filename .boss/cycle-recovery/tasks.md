# cycle-recovery 开发任务规格

## 摘要

- **需求来源**：用户在设计评审后明确要求异常旧轮到期退出实时调度，未决账务后台继续处理；下一计划窗口不等待人工；随后授权“那开始修复”。此要求优先于旧 HTML 设计中“核对前阻塞后续开轮”的条款。
- **任务总数**：12；后端/执行器 8，前端 2，独立 QA 与发布 2。
- **关键路径**：T02 归档模型 → T03 原子摘离 → T04 原轮结算/资金调整 → T05 调度接入 → T09 API/UI → T11 独立验收 → T12 发布。
- **Blast Radius**：高；预计最多 27 个代码/测试文件，涉及 SQLite 增量 schema、任务队列、周期状态、资金计算、API 与 UI。依赖清单、锁文件、认证机制、构建/部署配置均不变；无依赖安装命令，无删表/删文件计划。
- **风险确认触发项**：超过 10 文件，数据模型/迁移、任务队列、全局周期状态和 API 路由。**授权已满足**：本会话用户已在明确接受“按轮次保留旧账、未决资金单独占用、后续调度继续”的修订方向后要求开始修复；不重复请求同一范围确认。新破坏性或范围外操作仍需重新评估。
- **复杂度与工具预算**：高；实施约 65–100 次工具调用，关键路径约 40–60 次；这是估算，不压缩验证范围。
- **施工方式**：隔离工作树 `/www/wwwroot/pc28touzhu-cycle-recovery`。生产目录 `/www/wwwroot/pc28touzhu` 在发布验收前只读。
- **状态**：全部 Wave 与 ship 已完成，生产版本 `02614e5` 为 `released_verified`。W4 独立 QA：全量 1080 项通过（232.908 秒），最终冻结回归 109 项通过（24.931 秒），两组有重叠不相加；新恢复浏览器 3 项、既有浏览器 5 项通过，代码审查 DONE，最终隔离副本演练通过（224.711 秒）。配对备份及正式迁移验证原 39 表哈希一致；旧轮 1937 恰好归档 1 条、保留本金 13、可用策略余额 9.39，原任务/流水/财务/配置/额度均保持。9 个服务 active，规则实验室 PID 未变。规则 13 于北京时间 14:46:05 自然启动今日新轮 1968，首次合并发送 delivered、今日启动 1 次，旧未确认记录不再阻塞。发布停机 291.965 秒：迁移前 runuser PATH 脚本失败，改绝对路径后恢复，发布记录将说明详情。新契约以 `architecture.md` 为权威。

## Repo Preflight

| 项目事实 | 已确认内容与证据 |
| --- | --- |
| Git | 基线 `43fd8a2`，生产 `main`；隔离分支 `fix/cycle-recovery-20261127`。初始仅一个未跟踪的本次设计 HTML，已复制到隔离工作树；`git status --short`、`git worktree list` 已检查。 |
| 默认远端分支 | `unknown`：已知本机生产分支为 main，未依赖远端默认分支开展本次修复。 |
| CI | `.github/workflows/docker-image.yml` 仅 main/tag 的 Docker 多平台 build/push；无 lint/test gate。不能将 CI 镜像构建代替本次回归。 |
| 语言与包管理 | Python 3.12.3、Node 22.23.4；`pyproject.toml` 使用 setuptools，Telethon 可选依赖 `>=1.42,<2`。标准库 unittest，命令显式 `PYTHONPATH=src`。没有项目 npm test 脚本。 |
| 架构 | WSGI API、SQLite 仓储；`DatabaseRepository.transaction()` 为 `BEGIN IMMEDIATE`，嵌套 savepoint；无 ORM。 |
| 自动化 | Python 单元/临时 SQLite 集成/API E2E；vanilla JS DOM 与浏览器脚本。既有 jsdom/playwright-core 工具目录 `/tmp/pc28-cycle-ui-tools/node_modules`、`/tmp/pc28-draw-trigger-ui-check/node_modules` 已存在，不自动安装。 |
| 契约来源 | `domain/auto_trigger_cycle.py`、`domain/betting_protocol.py`、`executor/db_repository.py` schema、`services/job_service.py` 状态校验、`api/app.py` 路由与用户授权、`ui/auto_triggers.js` 当前消费结构。 |
| 状态枚举 | execution jobs：pending/sending/delivered/failed/expired/skipped/unconfirmed/rejected；流水主要 pending/placed/settled/void/cancelled；session waiting/running/continuation/day_closed/blocked。新增归档状态单独定义，不伪装任务已成功。 |
| 业务常量 | 期距 210 秒；本规则北京时间 02:01、08:01、14:01、20:01 起的四窗口，每窗最多 1、每日最多 4；领取租约现为 60 秒；默认最多 3 次尝试、30 秒起退避；所有重试受原 expire_at 限制。 |
| 资金语义 | active cycle balance 是策略账本余额，不是权威真实钱包；既有基础本金补足逻辑存在。未决本金只扣一次，迟到资本调整只在下一新开轮消费，补足额需显式记录。 |
| 基线验证 | 父代理已跑 9 组相关测试，144 项通过，47.993 秒；离线复现 EOF 连接分类、同尝试迟到成功被去重、void 不闭轮、cancelled 不能结清四个问题。 |
| 生产部署 | systemd，服务用户 www，生产 cwd `/www/wwwroot/pc28touzhu`，数据库约 1.9 GB；部署预检由 DevOps 独立产出。 |
| 迁移边界 | 仅 additive schema、幂等初始化；不重写历史事件/赔率/日额度。任何整库回滚可能覆盖新业务记录，因此发布回退以关闭新功能和前向修复为主。 |

## 固定业务合同

1. 只有异常发送过原截止时间才摘离；普通 placed 等待开奖、健康跨日周期保持原行为。
2. 一个异常会话所有参与路由同事务归档，以 `(rule_run_id,route_id)` 唯一保存原 state/policy/pending/归属；旧任务和流水不盲目 void。
3. 设原余额 `B`、摘离时已结算但尚未推进的净额 `K`、未决本金 `R`：交给未来轮次的可用策略余额是 `B + K - R`。所有状态分支均需检查是否已经应用 K，不能重复入账。
4. 摘离后老 run 禁止发送，session 解除该异常的活动指针；人工停止、禁用、日风控等独立状态继续生效。
5. 历史结果到达后归原 run、原 stat_date、原赔率。归档资本释放为 `R + 后续实际净额`，只在下一新开轮事务消费一次，不改当下 running state 的关数、stake、pending。同一统计日的真实新增损益仍可触发合法日风控，不能为了隔离周期而屏蔽真实风险。
6. 原定时门槛、冷却、日/窗口次数继续检查；不补过期投注、不追补错过窗口、不中途人工伪造新轮。
7. 未决账务是后台状态；人工可以处理最终账务争议，但从来不是未来计划开轮的前置条件。
8. 资本调整按归档时的 roll_mode 处理：原 chain credit 只在后续 chain 新轮消费；中途改 all_in 时保留，改回 chain 才消费。原 all_in 返还记 `independent_cycle_return`，不注入其他新轮，也不虚挂永久 pending credit。credit 消费与新轮首单创建在同一事务，`cycle_no_jobs_created` 必须回滚消费标记。

## 任务与写集

所有文件均相对工作树根目录。`BE-core`、`BE-transport`、`FE` 是写集 owner，不允许其他代理并行修改这些文件。任务拆分不代表新代理数量，同一 owner 连续完成相互依赖任务。

### T01 [BE] 发送前 EOF 分类与发送后未知边界

**owner**：BE-transport；**依赖**：无；**Wave**：W1；**复杂度**：中，约 6–10 调用。

| 文件 | 操作 | 写集风险 | owner | 用途 |
| --- | --- | --- | --- | --- |
| `src/pc28touzhu/executor/telethon_sender.py` | 修改 | 独占 | BE-transport | 明确准备/RPC 边界，补 EOF 类异常；只读证据读取后续仍由此 owner 实施。 |
| `src/pc28touzhu/executor/runtime.py` | 修改 | 独占 | BE-transport | 精确回报阶段、保留发送后未知，不把领取标识当是否发送证据。 |
| `src/pc28touzhu/executor/models.py` | 修改（按需） | 独占 | BE-transport | 结构化异常/证据类型，兼容既有异常。 |
| `tests/test_executor_recovery.py` | 创建 | 独占 | BE-transport | 准备阶段与发送阶段的同类异常成对复现。 |

先写 `TransportClassificationTests` 红测，证明 connect 的 IncompleteReadError 不应成为未知；再最小修改。发送开始后的同异常必须未知，发送次数不增加。无真实 Telegram 调用。

### T02 [BE] 归档账本与幂等增量模型

**owner**：BE-core；**依赖**：无；**Wave**：W1；**复杂度**：高，约 6–10 调用。

| 文件 | 操作 | 写集风险 | owner | 用途 |
| --- | --- | --- | --- | --- |
| `src/pc28touzhu/executor/db_repository.py` | 修改 | 中央共享文件，仅此 owner | BE-core | `auto_trigger_cycle_archives` 增量表、索引、序列化、查询、资本消费事务；归档关联 run 保留规则。 |
| `tests/test_cycle_recovery.py` | 创建 | 多任务复用，仅此 owner | BE-core | `CycleRecoveryMigrationTests`，重复初始化、原表保持、唯一/归属约束。 |

归档字段按 architecture：原完整快照、归属、`settled_net_at_detach`、`reserved_stake`、`spendable_balance_at_detach`、状态、资本 credit、消费 run、日期和原因；主键/唯一键按原 run+route。读写方法必须显式 owner。迁移不自行结束生产 run。现有 `prune_auto_trigger_rule_runtime_data` 不能继续无条件删除旧 run：对保留归档关联的 run 加 `NOT EXISTS` 保护，并测试正常清理继续运行、未决/待消费归档不会失去账务归属或产生外键错误。归档未决或 resolved 但 credit 尚未消费时，关联 run/event 以及 rule/subscription/route 的删除也必须阻止，不能只检查 pending/placed/active。

### T03 [BE] 过期异常旧轮原子摘离

**owner**：BE-core；**依赖**：T02；**Wave**：W2；**复杂度**：高，约 8–12 调用。

| 文件 | 操作 | 写集风险 | owner | 用途 |
| --- | --- | --- | --- | --- |
| `src/pc28touzhu/services/cycle_recovery_service.py` | 创建 | 多任务复用，仅此 owner | BE-core | `detach_expired_delivery_cycles`、范围/截止判定、异常终止。 |
| `src/pc28touzhu/executor/db_repository.py` | 修改 | 中央共享文件，仅此 owner | BE-core | 全参与路由归档+余额占用+关闭 run/runtime+session 转换同事务。 |
| `tests/test_cycle_recovery.py` | 修改 | 多任务复用，仅此 owner | BE-core | `CycleRecoveryDetachTests`：到期/未到期、两腿、多路由、重复/并发摘离、人工停轮。 |

不得将普通待开奖置为异常；不能清旧 pending 后不存归档；保留旧任务接受迟到回报但禁止再领取/发送。无真实未知资金损益记账。混合已结算/未结算的 K/R 逐腿计算。

### T04 [BE] 历史结算与资本延迟释放

**owner**：BE-core；**依赖**：T03；**Wave**：W2；**复杂度**：高，约 8–12 调用。

| 文件 | 操作 | 写集风险 | owner | 用途 |
| --- | --- | --- | --- | --- |
| `src/pc28touzhu/services/cycle_recovery_service.py` | 修改 | 多任务复用，仅此 owner | BE-core | `resolve_archived_cycles`，历史终态与资本释放。 |
| `src/pc28touzhu/services/pc28_auto_settlement_service.py` | 修改 | 独占 | BE-core | 正常周期与归档周期分流；可信 cancelled 终态，保留原 run 账务。 |
| `src/pc28touzhu/executor/db_repository.py` | 修改 | 中央共享文件，仅此 owner | BE-core | 资本 credit 一次消费和补足额记录；旧 run 财务不覆盖新轮。 |
| `tests/test_cycle_recovery.py` | 修改 | 多任务复用，仅此 owner | BE-core | `CycleRecoverySettlementTests` / `CycleRecoveryExposureTests`。 |

覆盖未知最终未发/拒绝/全输/命中/退款；确认下注后按原赔率开奖结算，不能接收任意客户端净额。负/零/正 credit、多个归档、混合腿和重复结算均恰好一次。新轮运行中 credit 不改变其关数或 pending，只在后续兼容模式开轮消费。chain → all_in → chain 切换保留待消费 credit；origin all_in 按 independent_cycle_return 审计处置。首单创建失败/事务回滚不能留下已消费标记。

### T05 [BE] 调度、领取与巡检接入

**owner**：BE-core；**依赖**：T04；**Wave**：W2；**复杂度**：高，约 6–10 调用。

| 文件 | 操作 | 写集风险 | owner | 用途 |
| --- | --- | --- | --- | --- |
| `src/pc28touzhu/services/auto_trigger_service.py` | 修改 | 独占 | BE-core | 在既有 worker 周期有界清理异常，派单前调用恢复扫描。 |
| `src/pc28touzhu/services/auto_trigger_cycle_service.py` | 修改 | 独占 | BE-core | 新轮余额消费 credit；识别异常摘离与独立停止原因。 |
| `src/pc28touzhu/services/job_service.py` | 修改（按需） | 独占 | BE-core | 独立核对入口/有界读取契约。 |
| `src/pc28touzhu/executor/db_repository.py` | 修改 | 中央共享文件，仅此 owner | BE-core | 最终资格防旧 run 发送、租约超时清理、运行态查询字段。 |
| `tests/test_cycle_recovery.py` | 修改 | 多任务复用，仅此 owner | BE-core | `CycleRecoverySchedulingTests`：次日和同日后续窗口、日/窗 quota、正常跨日、冷却、原停用。 |

运行读取 API 本身无写入副作用。后台扫描按最旧待处理分页，不能先截取最近失败造成饥饿。只恢复异常的调度占用，不触发 run-once 测试投注。

### T06 [BE] 核对证据独立 API 与权限

**owner**：BE-core；**依赖**：T04；**Wave**：W3；**复杂度**：高，约 6–10 调用。

| 文件 | 操作 | 写集风险 | owner | 用途 |
| --- | --- | --- | --- | --- |
| `src/pc28touzhu/api/app.py` | 修改 | 中央共享文件，仅此 owner | BE-core | 历史归档 GET、执行器核对 POST/只读任务入口。 |
| `src/pc28touzhu/services/cycle_recovery_service.py` | 修改 | 多任务复用，仅此 owner | BE-core | 单调证据补强，原尝试事实保留，冲突审计。 |
| `src/pc28touzhu/executor/db_repository.py` | 修改 | 中央共享文件，仅此 owner | BE-core | 原 attempt 身份、组级原子更新与 reconciliation audit。 |
| `tests/test_cycle_recovery_api.py` | 创建 | 独占 | BE-core | WSGI 端到端 owner/admin/other/executor、证据/版本/幂等校验。 |

固定端点：`GET /api/platform/auto-trigger-rules/{id}/cycle-recoveries`；`GET /api/executor/reconciliations/pull`；`POST /api/executor/jobs/{id}/reconcile`。核对领取使用独立 `reconciliation_token`，只允许核对回报，不能作为发送 claim。核对 POST 同时携带核对租约和原 attempt 身份。执行器 pull 可按现有领取惯例写入专用核对租约，但不改资金/发送资格；平台历史 GET 必须无写。原 `report` 同尝试重复去重继续保留，新接口允许有证据地 unknown → accepted/rejected/transport_confirmed；unknown/no-evidence 只能安排后续检查，不能 void。首版不开放凭缺失消息猜测 `not_sent`，真正确定未发仅由原执行器持久化的发送前失败证据证明。

固定 DTO：核对 pull 返回 `{items:[原job字段 + reconciliation_token + original_attempt:{token,no,executor_id,executed_at} + delivery_evidence:{message_id,send_started_at,send_finished_at,acceptance,opening_evidence}]}`。原 job 字段包括 `job_id,idempotency_key,lottery_type,issue_no,message_text,target,telegram_account,expire_at`，但没有新的发送 `attempt_token`。target/protocol 从冻结身份恢复；当前账号 session 身份改变时不查询、不改钱。核对 POST body 是 `{reconciliation_token,original_attempt_token,outcome,evidence,reason}`，outcome 限 `unknown|transport_confirmed|accepted|rejected`；执行器 ID 取认证 header，不能由请求体冒充。`transport_confirmed` 仅在原 legacy 协议可进入其既有结算口径。

### T07 [BE] 执行器持久化证据与后台只读核对

**owner**：BE-transport；**依赖**：T01，T06 契约冻结（已满足），不等待 T06 API 实现或 BE-core W2；**Wave**：W2-transport-reconciliation；**复杂度**：高，约 6–10 调用。

| 文件 | 操作 | 写集风险 | owner | 用途 |
| --- | --- | --- | --- | --- |
| `src/pc28touzhu/executor/state.py` | 修改 | 独占 | BE-transport | RPC 成功即保存消息 ID；重启只补报，不丢传输证据。 |
| `src/pc28touzhu/executor/api_client.py` | 修改 | 独占 | BE-transport | 已冻结的只读核对 API 客户端。 |
| `src/pc28touzhu/executor/runtime.py` | 修改 | 独占 | BE-transport | 有界后台核对和重启补报，不阻塞正常发送循环。 |
| `src/pc28touzhu/executor/telethon_sender.py` | 修改 | 独占 | BE-transport | 只读查询原消息/直接回复，按原账号与目标查证。 |
| `tests/test_executor_recovery.py` | 修改 | 独占 | BE-transport | `TransportReconciliationTests`：缺证据/歧义、重启/限流、无任何发送。 |

证据不充分继续 unknown，但归档已脱离调度。无消息 ID 时不凭同文案或最近100条未找到判定未发；远端账号/目标快照保持原任务值。后台查证错误不得杀死发送主循环。本 Wave 先以冻结 DTO 的离线 API fake 验证，W3 再跑真实临时 WSGI/SQLite 集成；不能以 mock 成功宣称真实后端接口已可用。

### T08 [BE] 只读运行态与告警数据

**owner**：BE-core；**依赖**：T05；**Wave**：W3；**复杂度**：中，约 4–6 调用。

| 文件 | 操作 | 写集风险 | owner | 用途 |
| --- | --- | --- | --- | --- |
| `src/pc28touzhu/services/platform_service.py` | 修改 | 独占 | BE-core | 未决历史账告警、执行记录归档关系，不冒充阻塞今日调度。 |
| `src/pc28touzhu/executor/db_repository.py` | 修改 | 中央共享文件，仅此 owner | BE-core | cycle_runtime 字段、归档分页与聚合。 |
| `tests/test_cycle_recovery_api.py` | 修改 | 独占 | BE-core | 返回字段、user隔离、GET无mutation、分页。 |

`cycle_runtime` 增加 `unresolved_run_count`、`reserved_stake`、`pending_capital_credit`、`last_interrupted_run_id`、`last_interrupted_at`、`last_interruption_reason`，routes 增加 reserved/credit。明确金额为策略账本，非权威真实钱包余额。返回历史列表必须 owner/subscription/run 过滤、limit 上限与稳定顺序，不把全部历史灌入规则列表。

### 前端派发用冻结合同（BE-core 已确认）

- **历史 GET**：`/api/platform/auto-trigger-rules/{rule_id}/cycle-recoveries?subscription_id={id}&limit=20&before_id={cursor}`；`subscription_id` 可省略，`before_id` 必须为正整数，`limit` 默认 20、最大 100，按归档 id 倒序。
- **响应**：`{items: [...], next_cursor: null | positive_integer}`；下页将 `next_cursor` 作为 `before_id`。不能将列表 items 视为全部历史总数。
- **item 白名单**：`id, rule_id, subscription_id, route_id, rule_run_id, stat_date, issue_no, status, reason, detached_at, resolved_at, initial_reserved_stake, reserved_stake, capital_credit, credit_applied_run_id, credit_applied_at, capital_disposition, recovery_error`。`capital_disposition` 为派生枚举 `deferred_chain_credit|independent_cycle_return`，后者显示独立周期返还，不显示为等待注入新轮的 credit。没有原 `state/policy/pending` JSON、凭证或账号配置，前端不得依赖这些字段。
- **status**：`awaiting_reconciliation` 显示“历史结果待核对”；`awaiting_settlement` 显示“历史投注待结算”；`resolved` 显示“历史账务已处理”。它们是旧档案状态，不能覆盖卡片顶部当前 session 的 waiting/running/stopped 状态。
- **权限**：使用既有登录会话；规则 owner 可读，管理员可读其他用户规则。服务端从原 rule 获取 owner，普通越权 403。前端不提交任意 owner 来扩大权限，未登录由既有登录流处理。
- **当前摘要**：使用上面的 runtime 六项新增字段及 route 的 reserve/credit；字段缺省时兼容旧响应，不造 0 覆盖 unknown，不显示报错栈。`capital_credit` 为 null 时显示“尚未确定”，真实 0 显示 0。
- **最小界面**：现有规则卡片增加旧轮提示与“查看历史账务”只读展开/详情，支持分页/加载失败重试；金额明确是策略资金。无人工修改大表单，无“重新发送旧任务”动作，旧账未决不会让当前待命显示为“必须人工处理才能开轮”。
- **测试**：DOM mock 对齐上述真实合同；浏览器临时库必须验证 authenticated GET、倒序分页、空态/失败、当前轮与旧账并存、XSS 转义、移动端。未知状态保守显示“状态未知”，不自动解释为已处理。

此合同已冻结，T09/T10 与后端真实 WSGI/API、DOM及浏览器集成已通过 W3。父代理统一派发，FE 不修改后端路径。

### T09 [FE] 当前轮与未决旧账分开展示

**owner**：FE；**依赖**：T08 契约冻结，渲染可用 mock 先行；**Wave**：W3；**复杂度**：中，约 4–6 调用。

| 文件 | 操作 | 写集风险 | owner | 用途 |
| --- | --- | --- | --- | --- |
| `src/pc28touzhu/ui/auto_triggers.js` | 修改 | 独占 | FE | 现有卡片展示中断旧轮/未决本金，当前待命或新轮状态独立。 |
| `src/pc28touzhu/ui/ui_text.js` | 修改（按需） | 共享词汇，仅此 owner | FE | 中断、后台核对、资金占用等准确文案。 |
| `src/pc28touzhu/ui/records.js` | 修改（按需） | 独占 | FE | 未决任务不显示可重发，展示归属原轮。 |
| `src/pc28touzhu/ui/auto_triggers.css` | 修改（按需） | 独占 | FE | 沿用现有样式，仅必要布局。 |
| `tests/test_cycle_recovery_ui.cjs` | 创建 | 独占 | FE | mock 契约红测/DOM 状态与 XSS 转义。 |

避免“必须人工处理才能开轮”的旧文案；可说明后台仍核对旧账。金额 0、无归档、多个旧轮、字段缺省兼容、恶意名称及手机端可读性均覆盖。

### T10 [FE] 浏览器完整流程验收

**owner**：FE；**依赖**：T06/T08/T09；**Wave**：W3；**复杂度**：中，约 4–6 调用。

| 文件 | 操作 | 写集风险 | owner | 用途 |
| --- | --- | --- | --- | --- |
| `tests/cycle_recovery_ui_server.py` | 创建 | 独占 | FE | 临时 SQLite fixture，禁止生产配置和任何真实发送。 |
| `tests/test_cycle_recovery_ui.cjs` | 修改 | 独占 | FE | `--browser` 本机浏览器流程：旧轮已中断/新轮待命或运行/未决历史明细。 |

临时库驱动真实 WSGI 读接口；拦截外部网络。桌面与移动端均验证当前状态、未决本金、无重发动作、恢复后历史仍可查。该测试不是生产投注。

### T11 [QA] 独立边界、并发与全回归

**owner**：QA；**依赖**：W1–W3 完成；**Wave**：W4；**复杂度**：高，约 8–12 调用。

| 文件 | 操作 | 写集风险 | owner | 用途 |
| --- | --- | --- | --- | --- |
| `tests/test_cycle_recovery_qa.py` | 创建 | 独占 | QA | 独立跨日、多归档、重复/并发、资金守恒和错 owner 测试。 |
| `.boss/cycle-recovery/qa-report.md` | 创建 | 独占 | QA | 测试证据、阻塞项、风险和放行结论。 |

QA 不直接改 BE/FE owner 文件，发现问题回派。每个归档恰好一次占用和释放，迟到结果不改当前轮；不同用户/规则/路由隔离。全 unittest、JS相关DOM/browser、编译检查、diff检查通过才发布。

### T12 [DevOps] 副本演练、受控发布与生产恢复核验

**owner**：DevOps；**依赖**：T11 放行；**Wave**：W4 后发布；**复杂度**：高，约 8–12 调用。

| 文件 | 操作 | 写集风险 | owner | 用途 |
| --- | --- | --- | --- | --- |
| `.boss/cycle-recovery/deploy-preflight.md` | 创建/修改 | 独占 | DevOps | 解释器、服务、备份、磁盘、健康检查预检。 |
| `.boss/cycle-recovery/deploy-report.md` | 创建 | 独占 | DevOps | 副本演练、迁移比对、发布与线上状态证据。 |
| `scripts/maintenance/rehearse_cycle_recovery.py` | 创建（如需） | 独占 | DevOps | 明确数据库参数的副本审计与幂等演练，默认不触及生产。 |

无发布配置变化。生产前一致备份数据库与执行器状态；副本验证 old table数据保持、外键、初始化幂等、1937 摘离与 13 元 reserve 恰好一次。未决任务不改 void，不发送测试投注；自然窗口检查新轮。生产一致性写入只有 QA 放行且父代理明确派发发布后进行。

## 并行安全组与依赖

| 安全组 | 任务 | 前置 | 写集约束 |
| --- | --- | --- | --- |
| A | T01 + T02 | 当前规格 | transport 与 backend 完全互斥，可并行红绿。 |
| B | T03 → T04 → T05；T07 可独立并行 | W1绿门禁；T07仅需T01与已冻结T06契约 | backend生命周期写集与transport只读核对写集互斥；两条W2独立验收。 |
| C | T06 → T08；T09 | 两条W2均通过，相关契约冻结 | T06/T08 同 owner 串行；FE仅自身文件。T07此时完成离线验证，进入真实API集成验证。 |
| D | T10 | 后端API与FE绿门禁 | FE负责浏览器fixture，不写后端实现。 |
| E | T11 | W2核心通过可开始专属文件验证；最终门禁等待W3 | QA独立测试文件，不与原owner实现并行写；准备完成不等于最终放行。 |
| F | T12 | W2核心通过可隔离副本演练；生产发布须最终QA放行 | 仅DevOps执行生产部署，其他写代理先停笔；提前准备不得修改生产。 |

中央文件 `executor/db_repository.py`、`api/app.py`、全部改动 services 统一归 BE-core；任何其他 owner 需要扩写时先报 Boss 重排写集。新 domain helper 如确有必要，须先给出确切路径并追加本文，不能无归属新建共享规则。

## Contract Matrix

| ID | Contract | UI / Copy | Client Payload | Server Schema | Persistence | Business Rule | Test Evidence |
| --- | --- | --- | --- | --- | --- | --- | --- |
| CM-1 | 发送前EOF可重试，发送后未知不重发 | 失败/后台核对 | structured stage + retryable | 既有 job状态，精确phase | attempt + executor本地证据 | 原expire_at与次数不扩张 | `tests.test_executor_recovery.TransportClassificationTests` |
| CM-2 | 异常到期自动摘离 | 旧轮已中断，当前待命 | worker内部调用 | archive unique(run,route) | 完整旧snapshot、K/R | 未发新信号也能释放调度占用 | `CycleRecoveryDetachTests` |
| CM-3 | 保留未决资金且只扣一次 | 未决本金，不冒充损失 | 只读 reserved_stake | 非负reserve、明确credit | archive + active carry | B+K−R，无重复扣 | `CycleRecoveryExposureTests` |
| CM-4 | 旧账晚到不改新轮 | 历史待结算/已结算 | reconcile证据 | 原run/owner/issue/roll_mode | 原账本、credit_applied_run_id | 兼容模式新轮消费一次、首单失败回滚；同日真实日风控仍生效 | `CycleRecoverySettlementTests` / `CycleRecoveryExposureTests` |
| CM-5 | 今日调度不等人工 | 等待原窗口/新轮运行 | 原规则配置不改 | 原quota/session gate | 日/窗次数不回退 | 原4窗口，不补投 | `CycleRecoverySchedulingTests` |
| CM-6 | 同尝试证据可补强且审计 | 后台核对结果 | original attempt + evidence | 独立reconcile接口 | 不覆写原attempt，独立audit | unknown可提升，冲突不静默改账 | `tests.test_cycle_recovery_api` |
| CM-7 | 只读自动查证不发送 | 历史仍核对 | read-only task | 冻结账号/目标 | 查证次数/证据 | 无证据保持未知但不挡新轮 | `TransportReconciliationTests` |
| CM-8 | 授权隔离及平台GET无副作用 | 仅本人/管理员可见 | rule/sub分页参数 | 既有授权 | user/rule/sub归属 | 其他用户不能读或改；执行器核对GET仅写独立租约 | `tests.test_cycle_recovery_api` |
| CM-9 | 正常跨日与人工风控不退化 | 原有正常运行/停止文案 | 原配置 | 原状态语义 | 原stat_date/run | 仅异常摘离，不普遍午夜重置 | 既有 `tests.test_cycle_qa` + recovery scheduling |
| CM-10 | UI展示兼容与无重发入口 | 当前与历史分开 | runtime新增可选字段 | 与architecture一致 | GET only | 空态/XSS/手机可读 | `node tests/test_cycle_recovery_ui.cjs` / `--browser` |
| CM-11 | 增量迁移与生产证据保留 | 无用户配置重置 | 显式副本DB参数 | additive幂等 | 老表行哈希/外键/quick_check，归档关联run清理保护 | 未决/待消费credit保护run/event及rule/sub/route删除 | `CycleRecoveryMigrationTests` + 副本演练 |

## Evidence Waves

实际可执行 argv、写集、合同和停止条件见 `waves.json`。开发代理应在实现前创建对应测试、运行并保存“缺功能导致”的 RED 证据，不把缺文件/导入失败当成功红测。

| Wave | 内容 | RED / GREEN | 停止条件 | 后续依赖 |
| --- | --- | --- | --- | --- |
| W1（已完成） | 传输边界 + 归档模型 | 父代理复核58项通过，12.795秒，编译/diff通过 | 不允许迁移改历史数据、分类失去发送后保护 | 两条互斥W2 |
| W2-cycle-lifecycle（已完成） | 摘离/资本/迟到结算/调度 | 父代理复核168项通过，54.137秒，编译/diff通过 | 未决本金被当利润/退款、旧账污染新run、正常跨日退化 | W3 |
| W2-transport-reconciliation（已完成） | 执行器只读核对，使用冻结DTO离线fake | 父代理复核51项通过，5.016秒，executor编译/diff通过 | 任何实际发送、遗漏原身份、查证错误杀死主循环 | W3真实API集成仍必需 |
| W3（已完成） | 核对API与页面集成 | API/真实WSGI/DOM通过；新恢复浏览器3项、既有浏览器5项由父代理/QA核验通过 | 任何补投、越权、平台历史GET写入、UI暗示人工gate | W4 |
| W4（已完成） | 独立验收、迁移演练与生产发布 | 全量1080项232.908秒；冻结109项24.931秒（有重叠）；代码审查DONE；副本224.711秒通过；02614e5 released_verified | 配对备份及原39表哈希一致；1937归档/保留本金准确；1968自然开轮且首次发送成功 | ship已完成，发布事实见deploy-report.md |

## 范围纪律

- 不安装新依赖，不更改认证、配置密钥、Docker/CI/systemd设置，不向外发送测试消息。
- 用户授权的核心是恢复既有计划；未授权增加新时间表或投注策略，不能创建新定时任务。
- 所有测试和演练使用临时/备份数据库。未拿到明确生产发布派发前不迁移生产、不重启服务。
- 文档/证据是实施辅助，不自动作为向用户交付的额外报告；最终对话以修复结果、验证和真实限制为主。

**STATUS**: DONE
**REASON**: 写集、测试序列、归档资金合同及独立核对租约接口已明确；架构细节变动须先同步规格再进入对应后续任务。
