# cycle-recovery 最终代码审查

## 摘要

- 结论：代码审查通过，当前无未解决 Blocker/P1；发布仍须独立 QA 全回归与数据库副本演练通过。
- 审查基线：43fd8a2 与隔离工作树当前改动；覆盖新增周期档案/资本处理、自动退出、历史证据接口、执行器线程/回执持久化与页面数据边界。
- 审查期间发现的数据错误阻塞调度、历史 opaque 误映射、迟到原回报降级及反向拒单审计污染均已由原 owner 修复并验证。
- 未修改业务 owner 文件或生产数据。本文不替代 QA/deploy 报告。

## 已修复发现

| 级别 | 发现及影响 | 修复与验证 |
| --- | --- | --- |
| P1 | resolve档案单条缺事件/坏数据抛异常，调度前全局解析导致所有新轮停摆 | 逐档事务错误隔离、保留本金、记录checked/error、轮转扫描。独立临时复现：next_triggered=1、archive_error_count=1、reserve仍10 |
| P1 | 缺失leg列表可被空事件解析误释放reserve | 校验原run/route/issue完整事件集合、ID类型及本金总额，缺失只报错；未知不退款 |
| P1 | 新开部分路由未实际派单，但credit已经消费 | discard未启动route恢复原cycle/carry、撤销credit及legacy carry应用标记；整体首单失败同事务回滚 |
| P1 | 后台新读取opaque成功回复套用旧matching opening/current_opening误认原期 | 远端opaque保持unknown；本地原accepted须重解析可信reply、原身份及acceptance_checked_at时序。服务端执行同等约束 |
| P1 | 无原attempt记录的lease-expiry核对成功后，原迟到unknown能把任务降级 | report入口尊重已核对确定状态；包括原report同样声称delivered但缺strict ack的路径 |
| P1 | guard把迟到delivered写入effective attempts，已核对rejected的void腿被成功attempt污染、无法释放本金 | 新增execution_ignored_reports独立存原回报事实及幂等身份，不进入有效发送/终态聚合。独立回归证明archive resolved、reserve0、credit10，重复回报不重复审计 |
| P2 | 默认session fallback与核对identity判断不一致 | 同fallback并冻结sender_session_path；真正session变更继续拒绝 |
| P2 | legacy route 0历史carry/credit无法映射到新implicit route | 只在原事件唯一target可证明且新route implicit时映射，记录capital_route/carry应用归属；独立相关用例通过 |

## 关键审查结论

1. **执行与账务隔离**：过原截止的异常发送退出原run，全部参与路由同事务归档，unknown事件不被伪造void/miss/refund。正常placed待开奖和正常跨日保留。
2. **资本守恒**：原pending快照B、已结净额K及未决本金R固定，余额B+K-R；credit按R+N-K只在下一兼容chain开轮消费。all_in独立返还、chain/all_in切换、部分路由回滚和原日真实风险均有明确处理。
3. **迟到记账**：既有原runtime、冻结赔率、原stat_date路径继续使用；历史解析不调用当前周期推进。真实同日风险可停止当天新轮，不混淆为周期污染。
4. **核对权限**：平台历史GET为owner/admin授权的白名单输出；无完整state/policy/pending和执行器凭证。执行器核对使用独立租约及原attempt/executor绑定，不获得发送资格。
5. **证据约束**：原账号/target/text/issue/message时间检查、可信reply关联及玩法金额校验；remote_history需原消息精读证据。无消息/模糊证据保持unknown，自动查证不发送消息。
6. **并发与线程**：核对线程有独立event loop，账号锁保护共享sender；每次connect新建client并finally清理，不复用跨loop活动client。前台优先与有界读预算避免历史读取占满实时发送。
7. **持久化与幂等**：RPC返回证据在业务ack等待前保存，本地重启可补证；核对审计和迟到忽略事实独立保存，effective attempts不被矛盾事实污染。
8. **清理与可见性**：未决归档和待消费credit保留关联，坏档案隔离并轮转，页面明确区分当前执行和历史未决本金；不提供过期旧消息重发入口。

## 本次独立运行证据

定向第一组5项全部通过（2.047秒）：

- late_unknown_report_cannot_undo_verified_reconciliation
- late_delivered_without_ack_cannot_undo_verified_acceptance
- wrong_account_target_or_original_token_cannot_apply
- remote_opaque_acceptance_never_uses_old_opening
- legacy_route_zero_carry_and_credit_follow_unique_original_target

最终guard改为独立ignored audit后，以下3项重复验证通过（1.261秒）：

- late_unknown_report_cannot_undo_verified_reconciliation
- late_delivered_without_ack_cannot_undo_verified_acceptance
- late_delivered_cannot_poison_verified_rejection_or_duplicate_audit

以上存在重叠，不相加宣称完整测试数。`git diff --check`通过。父代理此前已执行核心168项与发送核对门禁；最终完整数量以QA报告为准。

## 实际限制

旧事件没有message ID且本地没有可证明回执时，系统不会猜是否下注；该档案保留unknown但不再占运行会话。策略余额和自动补基础本金不是目标平台真实钱包余额，UI明确说明。新API采用既有executor共享认证信任边界，没有在此次修复重建认证系统。

**STATUS**: DONE

**REASON**: 已发现的阻塞问题修复并完成定向独立复核；代码审查可放行进入最终QA/发布门禁。
