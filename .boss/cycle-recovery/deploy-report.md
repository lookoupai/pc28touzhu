# cycle-recovery 生产发布记录

## 摘要

- **已发布并验证**：生产由 `43fd8a2a37ed18ac07dd65ed3f22913d5f9a5dd2` 快进到 `02614e542b24397800cf5d1849f6c7d339f3e82f`。
- 旧轮 **1937** 已退出实时调度并按原 run 归档，未决本金投影 **13.00**，可用策略余额投影 **9.39**。未知任务与原始流水保留，没有伪造退款或输赢。
- **自然开轮已恢复**：北京时间 **2026-11-27 14:46:05**，规则 13 在原 14:01–14:59 窗口启动今日第 1 轮 **1968**；第一期 **3511597** 的主辅任务 **64990 / 64991** 于 14:46:07 成功发送，后续只读观察已经正常结算。
- 原 8 个业务服务和维护 timer 已恢复；实验室 worker 未停止且 PID 保持。健康检查 200/ok，执行器 online。
- 维护实际 **291.965 秒**，超过预期 210 秒；原因是私有发布脚本调用 `runuser` 时 PATH 不包含 `/usr/sbin`，在迁移前失败。改用绝对路径后复用已验证备份继续完成，未回滚或重复备份。

## 版本与部署方式

沿用现有 systemd、www 用户和 `/www/wwwroot/pc28touzhu/.venv/bin/python`（Python 3.12.3）；没有安装新依赖、复制 systemd unit、修改部署平台或更改生产配置。

发布前已通过父 Agent 的完整 QA 与代码审查：全量 1080 项、最终定向 109 项，以及最终副本演练。副本演练和冻结源码证据详见 `deploy-preflight.md`；不把这些数量相加为独立测试数量。

生产证据与备份目录：

`/var/backups/pc28touzhu/releases/cycle-recovery-20261127/production-20261127-063942/`

目录受限，数据库和原始元数据为 0600；不提交数据库、执行器状态、会话或凭证。此前未跟踪的设计文档已保存为该目录下的 `design-before.html`，没有丢弃用户文件。

## 实际维护步骤与异常处置

1. 发布预检确认旧 HEAD、新目标提交和开发工作区一致；生产无已跟踪改动。发送队列 pending=0、sending=0、unconfirmed=2；health 为 200/ok，可用磁盘约 103.6 GiB。
2. 06:40:41 UTC 进入维护，暂停维护 timer；停止来源同步与自动触发，再停止执行器、结算和辅助服务，最后停止平台。8 个主库写入者确认停止，无其他进程持有主库文件。rule-lab 保持运行。
3. 在全部写入者停止时，用 SQLite backup API 生成主库和执行器状态库配对备份，记录哈希、文件大小、权限，并验证完整性。随后在备份上计算原表原列保护基线。
4. 57.913 秒时备份校验完成；保存旧设计文档后，生产代码已成功快进到 02614e5。下一步启动 www 身份迁移时遇到 `FileNotFoundError`，独立核查确定系统存在 `/usr/sbin/runuser`，只是当前 PATH 无该目录。此时数据库尚未迁移。
5. 私有脚本修正绝对路径，父 Agent 确认后从迁移阶段继续，复用已经验证的配对备份与保护基线。没有盲目回滚数据库或再次执行发布起点。
6. 使用 www 身份对明确的生产数据库执行两次 `initialize_database()`；原 39 张表的原列哈希保持，完整性和外键差量通过。
7. 只对 user 1 / rule 13 执行 `detach_expired_auto_trigger_cycles`，第一次归档 1 轮，第二次归档 0；校验受影响范围及原任务/流水/attempt/日额度后，启动平台并验证 health，再恢复原来 active 的其余服务、执行器和 timer。
8. 06:45:33 UTC 左右完成恢复，manifest 标记 `released_verified`；总维护耗时 291.965 秒。发布期间没有调用 run-once、手动派发测试投注或发送测试消息。

## 迁移与恢复证据

| 检查 | 结果 | 证据文件 |
|---|---|---|
| 主库与执行器状态备份 | 停写后配对备份完成，哈希/大小已记录 | `manifest.json`、`production-before.db`、`executor-state-before.db` |
| schema 初始化 | 原表原列 `changed_original_tables=[]`；quick_check=ok、无新增外键违规 | `production-migration.json` |
| 旧轮恢复 | 1937 → closed，reason=delivery_uncertain_expired；归档 1 条，reserve=13.00，可用投影=9.39 | `production-scoped-recovery.json` |
| 重复恢复 | detached_count=0，无重复 reserve | `production-scoped-recovery.json` |
| 旧消息/账目 | 任务 64488/64489 与流水/attempt 原字段保持；两任务仍 unconfirmed，原 updated_at 保持 | `production-scoped-recovery.json`、`production-natural-observation.json` |
| 影响范围 | 仅旧轮 state/session/run/runtime 的允许字段变化；日额度、配置与规则 7 记录保持 | `production-scoped-recovery.json` |
| 服务与健康 | 原 8 服务+timer 恢复；rule-lab PID 保持；health 200/ok | `manifest.json` |
| 自然新轮 | 1968 在原 fallback 窗口创建，今日 starts_count=1；旧日仍 starts_count=1 | `production-natural-observation.json` |
| 自然执行/结算 | 新期 3511597 的任务 64990/64991 delivered，流水已 settled | `production-natural-observation.json` |

`reserve=13` 是策略账本的未决本金占用，不是声称已锁定投注平台真实钱包余额。旧 run 的未知输赢没有进入已实现损益，也没有被当作已退款。

## 发布后观测与边界

06:47:12 UTC 只读检查确认旧归档仍 `awaiting_reconciliation`；后台已进行 3 次独立核对，原因 `message_id_unavailable`。这表示历史记录没有可信消息 ID，自动对账不能凭空确认这笔是否已发送，但已经不再阻塞新轮。旧任务状态和原时间戳保持。

06:48:26 UTC 的自然业务观察确认新一期正常结算、执行器 online。此证据验证实际恢复的开轮/发送/结算链路；不保证所有未来网络故障都会取得可用历史证据，也不宣称旧 13 元已完成对账。

发布后初始日志中，来源同步与执行器均未见失败计数增加或 traceback；核对接口请求为 200。该结果是短期运行观测，不替代长期监控。

## 回退约束

生产已恢复正常写入并创建新轮 1968，不得覆盖回发布前数据库。若出现问题，先保存当前一致快照并停止新增调度，再做前向修复或明确的配对恢复。旧代码不理解归档/未决资金/迟到调整，不能把“仅新增表”当作任意退回旧执行器与调度的充分条件。

**STATUS: DONE_WITH_CONCERNS**

**REASON:** 修复已上线，旧轮退出调度、自然新轮与结算已验证；旧发送结果仍缺消息 ID，保留未决账目；维护因 PATH 修正耗时超出目标，已如实记录。
