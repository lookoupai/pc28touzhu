## 摘要

- 评审结论：方案通过，可进入隔离工作树实施；这不是已实现或可直接发布的结论。
- 核心决定符合 PRD：手动保留原周期，自动按规则/方案/投递隔离状态，规则/方案共享一次日额度；自动无显式路由通过内部继承路由进入自动账本。
- 高风险集中在存量归属迁移、零派单/并发计次、跨日旧轮、多路由结束屏障和旧恢复入口绕过；必须按 `tasks.md` 的三波红绿证据验证。
- API、UI 的 mode/次数/结束动作、状态字段及恢复范围已对齐。当前无阻塞设计问题，不引入运行依赖或修改部署设施。
- 用户已明确授权本次设计及开工。工作仅在 `/tmp/pc28-cycle-isolation`；本评审不授权操作线上数据库或发送真实投注。

# 技术方案评审报告

## 1. 评审依据

评审日期：2026-11-04；基线 `5b37efa`；分支 `feat/cycle-isolation`。已阅读 `prd.md` FR-001 至 FR-013、`architecture.md`、`ui-spec.md`、`ui-design.json`，并检查现有 repository、派单、自动结算、启动/恢复入口及相关测试。

现状证据：`subscription_cycle_states` 以 subscription_id 为唯一键；`_dispatch_win_streak_roll_legs` 无论手动或自动都访问此表。无 route 的旧自动路径进入订阅 runtime/财务。`reset_win_streak_roll_cycle_for_restart` 会解封关联自动规则。上述三处说明只加规则表单不够，需要运行归属改造。

## 2. 设计结论

| 维度 | 结论 | 依据 |
|---|---|---|
| 状态归属 | 通过 | 自动周期按 rule/subscription/route，session/quota 按 rule/subscription，不能让第一个路由的 pending 阻挡其他路由。 |
| 财务兼容 | 通过 | 内部继承路由复用已分离自动账本；旧无 route 在途保留原事件归属直到结算，不重算历史钱。 |
| 日启动控制 | 通过 | 原子 claim、独立 daily starts、唯一新 run 身份；两腿和多个投递不重复计次；派单为零回滚。 |
| 跨日 | 通过 | 活跃周期沿用启动日与进度；日盈亏仍按结算日；不靠 midnight 重置清掉 pending。 |
| API/UI 契约 | 通过 | `cycle_policy`、`cycle_runtime`、`manual_cycle_state` 与 scoped resume 完整；旧 legacy 明确回显且不能从 rule 切回。 |
| 实施复杂度 | 高 | 涉及持久化、派单、结算、自动与手动操作、两个页面；已有框架可实现，无新增中间件理由。 |

UI 复核已修正一处时窗文案：等待条件的定时规则应“等待新一期且处于允许窗口”，不能误写成必须等待下一个窗口。JSON 使用现有原生页面结构；无需实现新的 React 应用。

## 3. 风险与必须兑现的控制

| ID | 触发条件 | 影响 | 等级 | 必须验证的控制 |
|---|---|---|---|---|
| R-01 | 老共享状态混有手动/自动证据或 pending 归属不完整 | 资金复制、在途丢失或意外续投 | 高 | 唯一可信归属才迁移；歧义原始快照留存并 blocked；重复初始化幂等；前后历史金额与任务总数不变。 |
| R-02 | API 检查与 worker 并发或同一期重试 | 多开轮、次数超限 | 高 | `BEGIN IMMEDIATE` 内重读/claim/派单；同一期防重复；重复操作仅一条新 run、一次 quota。 |
| R-03 | 相同信号/方案/目标已有手动或另一规则占位 | 零派单仍耗额度或引用他方 leg | 高 | 保留既有同群唯一 claim；在过关早分支同样检查；冲突方没有 pending、新 run 和已用次数。 |
| R-04 | 多路由仅一条先完成或另一条尚在途 | 提前续开、错用余额 | 高 | participating routes 只含真正起轮者；全部终态才能结束会话，close_day/风控可阻止续派但保留已发单结算。 |
| R-05 | 午夜时旧轮未完成 | 重置连赢/金额或额度记错日期 | 高 | 自动周期传启动日；当前日风控仍检查；旧轮终结后才认领新日额度，测试冻结 UTC/北京时间。 |
| R-06 | 改名/改条件/增上限/旧 resume-day/手动 restart | 解封别的归属或绕过额度 | 高 | 更新不重写运行快照/计数；手动只影响手动；resume-cycle 校验 owner、作用范围、在途、quota、风控与停用。 |
| R-07 | 某方案额度满/close_day 而规则还关联其他方案 | 误停止整条规则 | 高 | mode=rule 的次数与普通收工只作用 rule+subscription，规则日止盈止损才作用整规则。 |
| R-08 | 用户新建而测试默认使用旧语义 | 为使旧测试通过而恢复有害默认 | 中 | 生产新建默认 rule/1/wait_condition；旧行为回归显式 seed 旧数据库或 repository legacy，不改变生产默认。 |
| R-09 | 选直接续开但旧模式在纯算法内自行 running | 绕开 quota/停用/风控 | 高 | 新 mode 每轮先终态，再由统一启动入口处理下一信号；不能继续使用手动 new_cycle 旁路。 |

## 4. 实施与验证原则

1. repository owner 独占 `db_repository.py`；业务 owner 通过 architecture §3.7 使用稳定方法，不并发编辑中央仓储。
2. 数据波通过后才接受业务集成绿门禁。前端可在字段冻结后并行实现，但不能用 mock 测试作为唯一提交契约证据。
3. 红测试先落盘并由 root 执行 `verify-wave --phase red`；必须是目标行为断言失败，不以模块缺失或依赖未安装作为红证据。
4. 既有基线为 582 项，580 通过；2 项失败来自 `test_win_streak_roll_stop.py` 固定 2026-11-04，而当前北京时间已跨日。这两项还断言手动 restart 解封自动，正是新需求要替换的行为；使用固定时钟改为隔离断言，禁止改系统时钟或跳过测试。
5. 保存、迁移、模拟下注及 UI/API 验证仅使用临时数据库。真实 Telegram 边界用 fake executor/回执，不调用真实消息发送。
6. 阶段结束必须检查完整 diff、最终全套回归、迁移对账与真实 API 契约；未通过不得宣称可发布。

## 5. 工程约束与发布边界

Python 使用现有 snake_case、SQL 参数绑定、transaction/savepoint；新增自动状态行必须包含 user/rule/subscription/route owner，不能依赖 UI 传入值来确定权限。金额、退款与整数拆注沿用现有领域逻辑。UI 复用现有控件、隐藏兼容路由，不增加路由独立结构。

高 Blast Radius 的原因是数据模型、迁移与调度行为，不是引入依赖。用户当前“开始设计方案开工”已授权这些范围的可逆代码和隔离测试，故无需重复确认。真实发布需代码/SQLite 备份与所有 repository worker 统一版本切换；本轮文档不执行该操作。

## 6. 放行结论

- 设计阻塞项：0。
- 放行 Wave 1 数据层与字段契约明确的前端工作；Wave 2 业务依赖 Wave 1 绿门禁。
- 本报告列出的风险控制是实施验收条件，不是已经通过的测试声明。最终证据由 QA 与 root 记录。

## 7. 实施后独立审查（2026-11-05）

审查范围为隔离工作树当前实现 diff：repository 归属校验/迁移/claim/内部路由，自动启动事务、手动/自动派单与结算、scoped恢复、跨日以及新增UI输出。仅只读生产代码，修复由对应owner执行；本审查未操作线上数据库或worker。

### 7.1 已发现并修复的问题

| ID | 严重度 | 可复现路径与影响 | 修复与复验 |
|---|---|---|---|
| IR-001 | P1 | 旧状态归属冲突迁移为 manual stopped/migration_owner_ambiguous；无pending时，次日派单仍调用ensure_daily_state清掉停止并实际派两腿，手动reset也可能擦除阻断。 | manual dispatch在每日重置前永久拦migration_*；reset guard即使无pending也检查。`test_migration_blocked_manual_never_reopens_by_date_or_restart`通过。 |
| IR-002 | P1 | 外层评估缓存waiting session后网络fetch；另一个worker在fetch期间启动并完成4000001；外层返回证据4000001，事务只复核last_start而未复核last_completed，导致完成期证据被复用且starts变2。 | 事务重读session后再次校验continuation模式、last_completed_issue和cooldown。独立真实SQLite嵌套cycle复验得到inner_triggered=1、outer_triggered=0、starts=1、reason=cycle_waiting_new_issue；对应回归测试通过。 |

本次独立执行的回归命令：

```text
env PYTHONPATH=src python3 -m unittest tests.test_cycle_runtime.CycleRuntimeTests.test_migration_blocked_manual_never_reopens_by_date_or_restart tests.test_cycle_runtime.CycleRuntimeTests.test_completion_during_fetch_rechecks_latest_completed_issue_in_transaction
```

结果：2项通过；另以CycleFixture运行真实嵌套启动、发送回执、结算和外层重判序列复验IR-002，通过。`git diff --check`通过。

### 7.2 实现确认与明确边界

- 自动新轮在事务中复核规则配置、方案参数、scope/status；claim与双腿派单同事务，零派单回滚。手动与各rule使用不同周期状态，implicit route不暴露为用户可编辑路由。
- 多route参与集合只含成功启动者；未起投者的runtime被关闭，已经发送的route全部终态后才结束会话。日额度以rule+subscription计一次。
- 已确认用户/规则/方案/路由owner校验、手动reset不隐式恢复自动、pending保护、日额度与原日风控分层；配置改动不覆写本轮快照。
- legacy route=0兼容精化采用root最终裁定：旧无路由轮保持原账本与关数，继续到自然达标/爆仓，之后新轮才内部route；旧轮running（包括两期间无pending）期间禁止手动reset。架构已同步该说明；合成回归由业务owner和QA完成。
- 新UI动态rule/subscription/route名称、期号、状态说明和属性通过escapeHtml或textContent输出；未发现新增可利用XSS路径。Boss gate0的全项目innerHTML粗扫命中既有模式，不能据此要求整站重写，也不能把粗扫失败声明为绿色；基线误报与门禁实际结果由QA如实记录。

当前已审范围内没有尚未处理的P0/P1发现。此结论不替代QA最终全套回归、时窗/legacy兼容测试与迁移证据验收；测试或后续实质修改暴露问题时必须重新审查相应路径。
