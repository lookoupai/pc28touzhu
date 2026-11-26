# 规则实验室：按轮风控增量方案

## 摘要

- 基于 `083d607` 增量扩展，保持两个OK结算、固定金额单腿、独立模拟库和现有资源预算。
- 旧批次缺省 `legacy`，继续逐期判断；新UI选择 `round`，条件只负责开轮，轮内每个可用期继续固定注跟随。
- 风控快照在创建/未启动编辑时固定；每实验版本独立轮状态和按北京时间日期保存的日状态，不改真实规则或来源。
- 数据库v1→v2仅增加列和独立模拟表，旧列/收益/游标保持原值；worker不读取业务主库。

## 用户授权与本次边界

用户要求先完善再启动，并选择沿用现有自动触发规则的数值。现有未启动批次1保持来源7“频次趋势V1”、组合、每注1、两套OK不变。可用参考为规则6/其明确路由与订阅：轮止盈10/止损10；日止盈20/止损20；命中率低于25%且实赔+全亏至少20单时当日停止；冷却为距上次开轮至少10期。原配置未给下注次数/日轮数限制，因此这些为0不限。开发过程中不启动或改生产批次，由root发布后明确赋值。

不新增预测算法、过关/倍投、多腿、第三种结算、依赖或服务；不改上游数据库/业务账本/资源unit权限。

## 1. 风控配置合同

CreateBatch增加可选 `risk_policy`。未传时归一化为 `{"version":1,"mode":"legacy"}`，保持旧调用兼容。round完整结构：

```json
{
  "version": 1,
  "mode": "round",
  "round": {"enabled": true, "profit_target": 10, "loss_limit": 10, "max_bets": 0},
  "day": {"enabled": true, "profit_target": 20, "loss_limit": 20, "max_bets": 0, "max_rounds": 0,
    "hit_rate_gate": {"enabled": true, "threshold": 25, "min_settled_count": 20}},
  "additional_day_guards": [],
  "cooldown_issues": 10,
  "source": {"rule_id": 6, "rule_name": "参考规则", "route_id": 1, "route_name": "参考路由",
    "subscription_id": 7, "subscription_name": "参考方案"}
}
```

- 金额是非负有限数、最多2位小数、最大100000000；次数和冷却为0..1000000整数，拒绝bool/未知字段。0是该项不限，disabled代表整个对应层不生效但值保留。
- enabled必须bool。命中率门控启用时0<threshold<100、min_settled_count>=1。退款不入分母，复用现有`hit_rate_gate_stop_reason`的严格低于语义。round模式必须至少有一个启用层的止盈/止损/最多下注退出条件；仅日轮数或命中率门控不算明确退出条件，全部为0时拒绝保存/启动。
- `additional_day_guards`最多1个与day同形对象，保留规则级日控制与路由override的独立条件，不能假合并两个不同命中率门槛。独立检查，任一触发即停止。
- source只含白名单ID/名称，作为“复制自”说明；不允许包含凭证、来源config或任意原始策略。手工编辑后UI标为“已修改预设”，不暗示和生产同步。
- 阈值在实际单注结算后判断，允许最后一注越线，绝不裁剪净收益。无论何种停止，已冻结pending仍必须按原方向/赔率结算。

## 2. 轮与日状态机

每实验runtime：`round_seq,last_start_issue,active_round,last_stop_reason`。active_round保存`round_no,started_at,start_issue,start_date,bet_count,settled_count,hit_count,miss_count,refund_count,net_cents,status,stop_reason`。bet_count是已冻结期数；结算与跳过分别累计。每实验最多一笔pending，禁止在结果未知时再开下一笔。

开轮检查顺序：批次running/无pending → 当日sticky停止/下注额度 → 若已有active_round则继续，不要求条件再满足 → 无active_round时检查日轮数、距last_start_issue冷却、当前条件 → 成功冻结第一注才开轮并增加日开轮次数。

轮退出：单轮止盈、止损、最多下注；或当日止盈、止损、最多下注、命中率熔断。`max_rounds`只阻止开新轮，已启动轮可以结束。轮结束后保留独立摘要，下一轮必须重新满足条件与冷却。

`lab_risk_days`以(experiment_id,stat_date)唯一保存日净利、实赔/全亏/退款、已结算投注数、开轮数、停止原因。日期是北京时间。日停止一旦发生就sticky，暂停/恢复、后续pending盈亏反弹都不解除；下一日有独立行。

轮允许跨午夜继续，除非触发日或轮退出。日停止会关闭当前轮；次日重新等待条件。历史回放结束时未完成轮保留active且详情标“区间结束，轮次未完结”，不虚构收工和利润。

决策预占的日额按目标开奖日：历史已知open_at，live使用最新开奖+210秒的保守预计开奖时间，写入decision风险日期。最终投注数、盈亏、命中门控按实际open_at归日；已冻结决策不能因实际开奖跨午夜而取消。若预计日与实际日不同，只影响预占释放及实际结算日，后续准入以最新实际状态判断。旧日pending结算更新旧日行，不将新日状态回拨。日开轮数按首次决策预占的目标日期固定记录。

跨日/延迟时不使用未来收益提前停止：日状态只有已结算结果；worker按期号顺序先结已冻结单，再尝试下一决策。回放的同次事务也按期号交错准入与结算，不能先生成整页全部决策再统一风控。

round回放cursor保存simulation_time：每期决策前仅释放open_at及source_created_at都不晚于该决策点的pending。暂停后仅可结算到最后已推进的模拟时点，不能按现实now穿越未来。游标先标replay_complete（禁止再生成区间内决策），再final flush实际已公布pending，最后completed。页大小/重启不改变这条序列。

新目标输入冲突不启新轮。已冻结预测后来改向不取消原单；开奖冲突仍保留pending隔离。无效首注的轮摘要标“无有效投注”，保留开轮事实与冷却，避免未知时点反复重新触发；实际投注/盈亏为0。

## 3. 原子账本与迁移

v2新增：

- lab_batches.risk_policy_json（默认legacy）；lab_experiments.risk_runtime_json；lab_decisions.round_no、risk_date。
- lab_risk_days：experiment_id/stat_date，net_cents、hit/miss/refund、bet_count、round_count、stop_reason。
- lab_rounds：experiment_id/round_no，开闭时间/期号、计数、净利、状态/停止原因。按finalized_at保留30天，详情最多显示最近30轮，active/pending轮不清理；所有清理共享每批500行限额。累计完成/盈利/亏损轮数及轮止盈止损次数保存在runtime，清理摘要不减累计。

迁移在模拟库事务中使用PRAGMA table_info加列；v1旧行风险为legacy、无轮记录，不更改旧列。初始化幂等。完整数据和游标/收益/日表旧列逐行哈希测试。

`commit_step`对round模式再次执行准入和结算状态转换；worker预检只是省资源。状态、decision、outcome、daily、risk day、round summary、cursor/revision同事务。拒绝准入只不插decision，不生成逐期跳过日志；结果返回risk_blocked计数（可选新增，不破坏旧调用）。未知/过期revision整批不写。

单轮结束摘要和累计状态在同事务写入；同一outcome重试不再次更新风险。暂停后仍结pending；风控配置编辑和启动/推进共享revision，任一并发胜出后另一方必须重新检查。

## 4. 预设提取与HTTP

RuleLabService可注入`platform_repository`，仅API进程按当前owner调用现有list_auto_trigger_rules/list_subscriptions；worker既不注入也不读业务主库。返回值只用白名单字段重建。

预设粒度：规则×明确路由×明确订阅×玩法；不把多个路由拼成一套。只映射固定金额单腿来源策略，win_streak_roll/多腿/无法证明固定结构返回不可用原因或不列入。来源名仅用已有subscription/source展示安全名字，不传config。选择预设只复制风控，不改变实验来源、模板、注额或OK赔率。

轮风险：route.subscription_risk_mode override→其配置；disabled→关闭；inherit_subscription→订阅risk按play_limits该玩法覆盖；inherit_rule→rule daily中的金额控制。日风险：rule daily独立保留；route inherit_rule去重，override独立additional guard，disabled只关闭route层，不能撤销rule层。cycle legacy日轮数0；明确rule cycle max_starts_per_day可以映射日max_rounds；时段配额/直接续开/过关语义不假装支持。

API增量：

| 方法/路径 | 请求 | 成功/失败 |
|---|---|---|
| GET /api/platform/rule-lab/options | 当前登录owner | 增加risk_presets数组，每项含id/label/metric/policy/说明；无他人配置 |
| POST .../batches | 原字段+risk_policy可选 | 保持201，batch带risk_policy和risk_editable |
| POST .../batches/{id}/risk-policy | {risk_policy:完整对象} | 200 {batch}；仅owner、paused、started_at null、processed0、无decision；否则409 risk_policy_locked |
| GET批次/详情 | 无变化 | batch包含policy；实验增加risk_state（当前日、状态、当日净利/开轮/本轮净利/累计轮结果/stop reason）；详情增加recent_rounds最多30 |
| CSV | 无变化 | 增加模式、风控JSON、累计轮数/轮结果/当前轮收益/状态及停止原因列 |

公开API依然admin+owner，严格白名单、16KB上限；无需新权限机制。options的user_id由会话提供，浏览器不可指定。

## 5. 测试及交付

先纯状态与v1迁移红绿；再worker跨期/跨日/两OK分歧/重启；再preset继承及权限/API编辑race；最后桌面/窄屏浏览器与完整回归。测量新增状态存储和有界页面查询，不降低既有资源限制。root独立复验和发布；本代理不提交/部署/改生产批次。
