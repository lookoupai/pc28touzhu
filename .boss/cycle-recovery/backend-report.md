# 后端实现证据

## W1：归档模型

- RED：`PYTHONPATH=src python3 -m unittest tests.test_cycle_recovery.CycleRecoveryMigrationTests -v`，3 项均因缺少 run-scoped archive 明确失败。
- GREEN：migration 与既有 cycle_repository 共 34 项通过，9.980 秒；`compileall`、`git diff --check` 通过。
- 新增 `auto_trigger_cycle_archives`，按原 run/route 唯一，保留原状态、策略、pending、K/R 与资金释放字段；初始化不处理线上轮次。
- 日常清理保留档案所引用的原 run/credit应用run及原日统计，防止断开历史归属。
- 当前阶段仅修改仓储与隔离测试；尚未接入调度恢复。

## W2：异常轮次退出、旧账与资本隔离

- 首批 RED：13 项因缺失摘离行为及旧轮阻塞次日启动失败；后续增加坏档案、缺腿与部分路由信用返还红测，实测复现后修复。
- GREEN：新 24 项 + 原 9 组共 168 项，59.435 秒全部通过。首次广回归发现 2 项 legacy stop_reason 被覆盖，已限定只覆盖仍 running 的人工停止结果，针对与广回归均通过。
- 原子归档全部参与路由；未知本金仅保留一次，未来开轮不等待后台旧账。旧任务不重发、不盲目 void，原日/原轮次归属保留。
- 新轮边界消费确定 credit 并记录 top_up；中途当前轮不被旧账覆盖；首单整体回滚及未起投路由 credit/carry 还原已覆盖。
- 日常 route_has_open_run 排除已归档旧事件，仍检查未归档在途事件；正常 placed 跨日继续等待结算。
- 损坏 JSON/缺失或遗漏腿/本金不一致按单归属记录错误，不阻断其他开轮；SQLite 系统错误不吞。
- 已验证四定时窗口、同窗限额、次日新任务原 run 隔离、并发摘离、人工停止和归档删除保护。
- 待 W3：后台证据 API/核对、只读运行态与历史页，legacy route0 唯一目标映射兼容。

## W3：独立核对 API 与运行态

- RED：8 项 WSGI 契约测试因缺少端点及运行态字段失败；实现后全部通过。追加迟到原回执覆盖核对结果、后台旧账告警、legacy route0 资本映射的独立 RED/GREEN。
- 最终针对验证：`tests.test_cycle_recovery_api tests.test_cycle_recovery tests.test_cycle_recovery_qa tests.test_executor_recovery.TransportApiIntegrationTests` 共 47 项，19.646 秒全部通过；`compileall` 与 diff-check 通过。
- GET 历史档案使用用户/管理员原归属授权，分页白名单返回，不泄露原始账号配置或证据，不修改账务。
- 执行器使用独立核对租约，原账号/目标/内容/期号冻结；unknown 只更新重查记录。完整传输/受理证据独立审计，主辅任务原子更新；原 attempt 保留。
- 严格协议在服务端重新解析可信机器人回复、关联消息、原期号与完整金额。新读历史不透明轮号拒绝；本地当时证据须满足时序。未知不推断未发。
- 已独立核对的明确结果不被尚未上报的原尝试迟到 unknown 覆盖；迟到事实保留但不降级任务。
- 当前运行态增加未决旧轮数（distinct run）、保留本金、待用 credit、上次中断、每路由新增本金及错误；真实告警说明不阻塞后续计划。
- legacy route0 仅凭原任务唯一目标映射到对应 implicit route，carry 和返还只应用一次；不能唯一映射时保持原档案，不猜测。

## 最终审查补丁

- 原领取在租约过期后可能先完成独立核对，再迟到回报 unknown，或严格协议迟到 delivered 却缺失受理证据。统一保留已核对的明确结果，不再经过原回报的重新降级校验。
- 已核对 rejected 后的迟到 delivered 不能放入 canonical `execution_attempts`，否则聚合会误认有成功下注而不释放资金。新增 `execution_ignored_reports` 专表保存原报告事实，按 leader/executor/token/attempt_no 唯一；幂等补报不写有效发送结果。
- 对应 RED 明确复现“credit 无法释放”，修后验证 reserve=0、返还本金、重复同步不复活 void、重复原报告只有一条事实审计。
- 最终 API 12 + acceptance 14 + 独立 QA 11 + transport API 1，共 38 项通过，16.192 秒；diff-check 通过。新增表需要纳入最终发布副本初始化演练。

## API

| 方法 | 路径 | 作用 |
| --- | --- | --- |
| GET | `/api/platform/auto-trigger-rules/{id}/cycle-recoveries` | `{items,next_cursor}`，可选 subscription_id/before_id，limit 默认20、上限100 |
| GET | `/api/executor/reconciliations/pull` | 最多10条，仅独立复查租约，原任务DTO无发送attempt_token |
| POST | `/api/executor/jobs/{id}/reconcile` | 原attempt与复查lease绑定，unknown/transport_confirmed/accepted/rejected，幂等审计 |

**STATUS**: DONE
**REASON**: W1-W3 后端代码及测试完成，代码冻结交根代理独立最终 QA；未部署或修改生产数据。
