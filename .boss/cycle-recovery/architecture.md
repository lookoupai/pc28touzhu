# 异常轮次自动退出调度与历史账务隔离

## 摘要

- 来源：用户在确认设计后明确指出 210 秒一期，过期后人工处理不能成为后续开轮前提；随后授权“那开始修复”。旧 HTML 方案中“先核对完再解锁新轮”已被此需求取代。
- 采用增量归档：保留当前周期表，新增按 `rule_run_id + route_id` 唯一的历史周期档案。异常任务过原发送截止后自动关闭旧轮执行所有权，未来窗口按既有规则启动。
- 未确认投注保留原任务、流水和完整策略快照；资金只作未决占用，不伪造未下注、退款、输赢。迟到结果只记旧账，返还资金在下一次新开轮边界应用。
- 正常 placed 等待开奖、正常跨日、人工停止及日风控的原语义保持。无证据历史单可继续待核对，但不持有运行会话锁。

## 1. 已确认代码事实

定时开轮 `evaluate_roll_rule` 在同一 SQLite `BEGIN IMMEDIATE` 事务创建日次数、run、session、路由 runtime 与首期任务。后续期由来源同步派发。主辅两腿分账但只发送一条消息。周期 `pending_plan` 未清会阻止下一期；session/run 未闭会阻止新轮。

`auto_trigger_cycle_states` 主键 `(rule_id, subscription_id, route_id)`，只能保存当前周期；本方案不破坏现有读写契约。正常旧单财务结算已使用 `auto_trigger_runtime_run_id` 与冻结赔率快照，并保护新 runtime 的财务基线；仍需隔离周期快照和返还资金应用。

本次故障：rule 13 / subscription 21 / route 13 / run 1937；期号 3510978；jobs 64488、64489；events 64481、64482；待发本金 13。第一关后策略余额 22.39、当前轮注码 13.93、第一关净利 3.93。异常实际阶段不明，不能仅凭 IncompleteReadError 推断未发送。

## 2. 归档与退出调度

### 触发条件

满足原任务发送截止已到，并存在 `unconfirmed` 或租约已失效的 `sending` 等不明发送，且归属本轮 pending_plan。租约过期只负责把可能已发标为未知，不能触发重发。正常 `placed` 待开奖本身不是异常，不按跨日或 pending 年龄强制结束。

明确未发的终止任务按现有规则推进；如果已进入异常中断/人工停止收尾，则 `void` 和有证据的 `cancelled` 是零新增损益终态。不要把所有正常单期跳过都改为结束整个策略。

### 原子步骤

1. 按 user/rule/subscription/run 校验当前 session、run、全部参与 route 和 pending 的 event/job 归属，在同一事务锁定。
2. 某一路由触发异常退出时，归档同会话的全部参与路由，包含其他路由的 placed 待开奖；禁止遗漏后写路由覆盖历史。
3. 取消明确尚未发出的 pending/可证明未发的 failed；in-flight sending 和 unconfirmed 保留未知，绝不通过取消伪装成未下注。迟到 RPC/回报可能继续到来，档案必须承接它们。
4. 档案保存完整原 state、policy、pending_plan、事件 ID、run/stat_date、已结算净额与保留本金。写入唯一键后才从当前周期清 pending，并以保守可用余额留下 stopped 当前记录供下一轮继承。
5. 关闭旧 rule run 和所有参与 route runtime 的执行入口，清 session 的 active_run_id。仅解除运行占用；若已有人工停止/禁用/日风控/迁移阻塞，继续保留对应阻塞。否则 session 转 waiting，不立即派单，不补昨天错过窗口。
6. 调度使用原时段、冷却、新证据与次数判定开新轮。旧日次数不退款，新轮只计其新日。

必须同时从派单、重试、领取、最终发送资格检查挡住已结束旧 run。数据库事务外已提交的发送无法撤回，必须由历史档案和保留金额兜住。

## 3. 资金公式与边界

对于 chain 路由，定义：

- `B`：pending.state.balance，必须使用原待处理期开始前快照。
- `K`：归档事务时，本 pending 的已经 settled 事件净额之和。void/cancelled 经证据确认后贡献 0。
- `R`：同 pending 中尚未终态的事件本金之和，包括 unknown 与 placed。只计算归属准确的原事件，不包括其他 run。
- 退出后供未来轮继承的余额：`C = B + K - R`。扣除发生一次，唯一档案防止重试重复扣。
- 后台全部终态时，原待处理期总净额为 `N`，应释放返还：`credit = R + N - K`。未下注返本金，输光返 0，命中返本金及利润。

归档固定 `K/R`；部分旧腿先结算时可暂保整个档案本金，直到全部终态一次释放，避免分段重复入账。若原数据存在无法证明的本金/余额不一致，保留异常证据并不给出伪造资金数；该路由资金门槛不能被绕过。

`credit` 不直接改正在运行的新周期。只在下一次开轮事务中将所有尚未应用的同归属 credit 加入前一可用余额，并标记 `credit_applied_run_id`。之后再执行现有 `max(carry, base_amount)`。新增本金 `top_up_amount = max(0, base_amount - adjusted_carry)` 必须记录。当前余额是策略模拟/滚仓记账余额，不能宣传为真实目标账户钱包余额。

示例：B=22.39，K=0，R=13，退出后可用余额9.39。下一轮base10需要显式补充0.61；13仍显示为历史保留金额，不算损失或已返还。如果旧单最后亏13，credit0；未发credit13；净赢5，credit18。新轮关数/本期stake/pending从不因旧单结果改变。

`all_in` 原本每轮独立投入 base，并不继承旧余额。保留/释放金额作为历史资本审计展示，不注入新轮 base/stake；每轮独立投入需准确记录。不要把 chain 的资本抵扣规则错误套到 all_in。资本归属按archive冻结模式判断：origin chain credit只可被后续chain新轮消费；用户中途切到all_in不能把credit标已用而无资金去向，改回chain后再应用。origin all_in返还明确标记 `independent_cycle_return` 作为原独立周期返还审计，不注入新chain/all_in轮，也不假挂为永久待消费credit。归档快照必须记录该处置方式。

## 4. 数据契约

### auto_trigger_cycle_archives（新）

| 字段 | 含义/约束 |
| --- | --- |
| id | INTEGER PRIMARY KEY |
| user_id / rule_id / subscription_id / route_id / rule_run_id | 原归属，校验一致；UNIQUE(rule_run_id,route_id) |
| stat_date / issue_no | 原开轮日与原待处理期 |
| state_json / policy_json / pending_plan_json | 完整冻结原状态，不被新轮覆盖 |
| settled_net_at_detach | K |
| reserved_stake | R，实际保留；后台全部终态后可置0，但原R必须另保在快照或initial_reserved_stake |
| spendable_balance_at_detach | C，供审计 |
| capital_credit | 已确定可释放返还；未知时为空/0且status区分 |
| credit_applied_run_id / credit_applied_at | 一次性消费标识；all_in审计应用不影响stake |
| status | awaiting_reconciliation / awaiting_settlement / resolved |
| reason / detached_at / resolved_at / updated_at | 故障归因与时间 |

索引：未解决状态及最早更新时间、user/rule/subscription/run、待应用credit。档案及未决原事件不得被日常清理删除；需同步审查 prune、删除规则/方案/路由和正文归档保护。已经resolved但credit尚未消费的档案也必须保护归属和返还权，不能仅检查pending/placed/active状态。

新增调用建议集中 `services/cycle_recovery_service.py`：`detach_expired_delivery_cycles`、`resolve_archived_cycles`；事务写入和 credit 消费由 Repository 封装。现有 active 周期扫描与 archive 扫描分开，历史解析绝不调用 active `upsert_auto_trigger_cycle_state` 或 `finish_roll_session`。

### 显示契约（现有规则响应增量字段）

`cycle_runtime[]`：`unresolved_run_count`、`reserved_stake`、`pending_capital_credit`、`last_interrupted_run_id`、`last_interrupted_at`、`last_interruption_reason`；routes[] 增 `reserved_stake`、`pending_capital_credit`。日启动次数、当前关数、active_run_id保持原义。新开轮记录 `top_up_amount` 及已应用credit信息（可放run audit JSON，实际字段由backend保持一致）。

前端显示“旧轮已结束，历史投注结果待核对；已保留本金X；后续按原计划启动”。不要把历史待核对误显示为“当前轮运行中”，不要给过期旧任务重发按钮。

## 5. API 和回执契约

| 接口 | 约定 |
| --- | --- |
| GET /api/platform/auto-trigger-rules/{id}/cycle-recoveries | 只读历史归档；可选subscription_id、正整数before_id；limit默认20最大100，按id倒序；响应`{items,next_cursor}`，next_cursor用于before_id。owner/admin授权，其他用户403 |
| GET /api/executor/reconciliations/pull | 本次实现：领取有界复查任务，独立核对租约，不是发送授权，按已知消息ID/持久化结果优先；仅executor认证。沿用现有executor GET pull惯例，仅此GET写核对租约；远端查询不发送消息 |
| POST /api/executor/jobs/{id}/reconcile | 独立补强证据入口，原attempt身份+核对身份，结果只能unknown或可信monotonic送达/accepted/rejected/not_sent；不借原report去重绕过 |

核对原始attempt不覆盖，追加审计；组级原子处理。严格受理协议复用可信bot/reply_to/issue/金额/目标映射，legacy仅能标明传输确认，不伪装受理。查不到消息不能判定未发送；无消息ID缺少可证明的唯一性时只报unknown，首版不做模糊文本自动认定。缺证据继续档案unknown、退避查证，不阻塞新轮。网络查询在事务外，账号锁和外部限流保护，不发探测消息；核心detachment不依赖网络查证成功。

平台历史items采用白名单：id、rule/subscription/route/run ID、stat_date、issue_no、status、reason、时间、initial_reserved_stake/reserved_stake/capital_credit/credit_applied_run_id、capital_disposition（deferred_chain_credit或independent_cycle_return）、recovery_error。不得向平台页面返回完整state_json/policy_json/pending_plan_json或执行器身份凭证。

### 5.1 执行器最小DTO

pull返回 `{items:[...]}`；单项复用完整现有job DTO字段，包括 `job_id,signal_id,idempotency_key,lottery_type,issue_no,bet_type,bet_value,stake_plan,message_text,target,telegram_account,execute_after,expire_at`，增加 `reconciliation_token`、`original_attempt:{token,no,executor_id,executed_at}`、`delivery_evidence:{message_id,send_started_at,send_finished_at,acceptance,opening_evidence}`。禁止下发新的发送attempt_token；核对token绝不是发送资格。建议每个执行器循环最多处理一个核对任务，核对失败不得阻止本轮正常发送。

target key、protocol及账号identity采用原job冻结值。当前账号session身份变更或原账号不可读取时只记录identity_unavailable，不用新账号/新目标替代旧目标查证。读取优先级为执行器已持久化的原job结果，然后已知message_id精确查询及可信关联回复；核对任务单次有界，不延误正常发送。

reconcile请求为 `{reconciliation_token,original_attempt_token,outcome,evidence,reason}`，outcome首版限 `unknown|transport_confirmed|accepted|rejected`。evidence沿用原sender结果的消息ID、时间、完整原消息内容、关联受理结构。执行器ID由认证header绑定核对lease；校验原attempt、原账户/目标/内容和消息关联。unknown只记录重查退避；不得把查无消息升级not_sent。迟到有效传输/受理/拒单证据按冻结协议处理，追加审计并同事务更新完整主辅成员。

新读取的不透明（opaque）轮号回执，不能仅凭发送时保存的matching opening/current_opening映射原期：这些快照可能早于该迟到回执。首版对此保持unknown，除非已有原实时阶段持久化的可信accepted完整证据；若未来要自动核验，必须补该回执时点完整历史映射。服务端同步校验，不只相信outcome字符串。

迟到成功必须先进入旧event按原冻结赔率与真实开奖结算，再更新归档状态和credit。重复核对、同一回报多次到达、结算与新开轮并发，必须最多应用一次。已resolved结论被矛盾证据挑战时记录冲突，不静默重写账务。旧事件仍更新原统计日的真实日风控；同日迟到盈亏合法触发同日新轮风控停止应保留，这与禁止旧周期覆盖新周期资金/关数不同。跨日旧事件不得被记入今天。

## 6. 部署与验证边界

使用增量CREATE TABLE/ADD COLUMN，重复初始化幂等，不在schema迁移时认定旧投注输赢。上线后显式运行有界恢复扫描可处理1937：快照与保留13、释放调度所有权，不发送测试投注。线上真实下注/旧账结论仍依证据。

必须验证：正常cross-midnight保留；unknown过期释放session；多route原子归档；旧placed后结算；同一reserve不重复扣；credit仅未来新开应用；人工stop/日风控不被解除；其他rule/sub/owner隔离；双腿及单腿玩法兼容；正文/运行记录清理不破坏未决档案。
