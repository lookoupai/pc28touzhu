# cycle-recovery 发布预检

## 摘要

- 2026-11-27 04:19:47 UTC 完成只读核查；生产代码 `43fd8a2a37ed18ac07dd65ed3f22913d5f9a5dd2`，主分支 `main`。
- 本次仅准备发布方案；没有停止/重启服务、迁移数据库、创建生产任务、发送消息或改写生产文件。
- 现有部署为本机 systemd，8 个业务服务采用 `www:www`、`/www/wwwroot/pc28touzhu/.venv/bin/python`，真实解释器为 `/usr/bin/python3.12`。无需换部署平台、修改 unit 或安装依赖。
- 主数据库约 1.90 GiB，执行器持久化库约 1.07 MiB，均为 SQLite `delete` journal；磁盘可用约 110 GiB。当前备份目录没有可作为本次回滚依据的主库备份，发布前必须新建。
- 发布条件：最终修复提交、独立 QA 通过、生产一致副本迁移/行为演练完成、原业务表保护校验通过、负责人明确下达发布步骤。目前不具备立即发布的代码与 QA 条件。

## 1. 当前生产事实

| 项目 | 核查结果 |
|---|---|
| 生产根目录 | `/www/wwwroot/pc28touzhu` |
| 隔离开发目录 | `/www/wwwroot/pc28touzhu-cycle-recovery`，分支 `fix/cycle-recovery-20261127` |
| 生产工作区变更 | 仅此前本会话生成的未跟踪 `docs/EXECUTION_UNCONFIRMED_RECOVERY_DESIGN_20261127.html`；发布时应保留旧文档，不使用 `git clean/reset --hard` |
| 主库 | `pc28touzhu.db`，2,036,703,232 字节，497,242 页，`journal_mode=delete`，`www:www`，0660 |
| 执行器状态 | `executor-state-executor-001.db`，1,126,400 字节，275 页，`journal_mode=delete`，`www:www`，0600 |
| API | `0.0.0.0:35100`；本机 `GET http://127.0.0.1:35100/api/health` 返回 `{"status":"ok"}` |
| 磁盘 | `/dev/vda1` 180 GiB，使用 70 GiB，可用 110 GiB；项目、`/var/backups` 与 `/tmp` 同文件系统 |
| 维护 timer | `pc28touzhu-payload-maintenance.timer` active；下一次为 2026-11-27 20:30 UTC。发布临界区须记录并临时停止 timer，原来未运行的一次性 service 不启动 |
| 实验室 worker | `pc28touzhu-rule-lab.service` active，但明确禁止访问主库，只写 `data/rule_lab`；本次未修改其模块则保持运行 |

主库的 8 个业务服务均 active/running：

1. `pc28touzhu-platform.service`
2. `pc28touzhu-source-sync.service`
3. `pc28touzhu-auto-trigger.service`
4. `pc28touzhu-pc28-auto-settlement.service`
5. `pc28touzhu-telegram-executor@executor-001.service`
6. `pc28touzhu-telegram-bot.service`
7. `pc28touzhu-telegram-report.service`
8. `pc28touzhu-alert-notifier.service`

`deploy/systemd/pc28ctl.sh` 的 restart 会复制 unit 并 `daemon-reload`，且默认不含实验室服务；本次建议使用精确 service 清单，避免无关 unit 覆盖。

## 2. 故障状态与旁路业务

只读查询确认：

| 对象 | 当前值 |
|---|---|
| 会话 | rule 13 / subscription 21，running，active_run_id 1937，原统计日 2026-11-26 |
| 路由周期 | route 13，balance 22.39，cycle_stake 13.93，day_pnl 3.93，periods 1 |
| 未决期 | 3510978，pending stake 13.00 |
| 未决任务 | 64488 / 64489 均 unconfirmed；expire_at 为 2026-11-25T18:07:48Z |
| 当前发送队列 | 查询时无 pending / sending，全库只有上述 2 条 unconfirmed |
| 另一活动轮 | rule 7 / subscription 16 / run 1958，统计日 2026-11-27，active |

以上是观测时刻快照，不保证发布时队列仍为空。停止服务前必须重新查询所有规则的 sending/pending 和 placed，不能因为故障规则已停滞而忽略正常规则 7。

业务目标来自用户最新修订：异常轮到原发送截止时间后退出调度，把未知任务和原周期按 run 归档并保留未决敞口；后续原定时开轮不等待人工。不得把 unconfirmed 转成假退款、假未发或已结算亏损；迟到结算只进入原 run，并按新实现的资金调整机制影响可用资金。

## 3. 一致副本与演练

### 3.1 保密与落盘

建议发布证据目录：`/var/backups/pc28touzhu/releases/cycle-recovery-20261127-<UTC时间>/`，目录 0700，数据库/状态库/原始证据文件 0600。原始数据不放进 Git，也不作为用户附件发送。

记录旧/新提交、代码 diff 清单、服务 active 状态与 PID、数据库路径/属主/模式、配置文件 SHA256；不输出 `.env`、会话凭证、用户/账号原始记录。仅展示恢复所需业务标识及聚合校验结果。

### 3.2 在线演练副本的安全选择

主库采用 rollback journal；不能在写入期间直接 `cp` 单个数据库文件。使用 SQLite backup API，从 `file:...?...mode=ro` 源连接备份到受限目录的新文件。优先尝试小步备份（例如每次 256～1024 页），通过 progress callback 检查最大总时间并中止重试，**不显式开启持久 source 读事务**，避免长期占据共享锁。

持续写入会让分段备份重起；历史发布记录已遇到该情况。若 30 秒预算内无法完成，停止尝试、删除这次未完成目标及其临时 sidecar，不能把部分文件当作有效备份。随后在计划好的短维护窗口先停所有主库写入者，再做一次完整 backup；不要无限重试、改主库 journal 模式或强行锁表。

完整有效 backup 结束后，在副本执行 `PRAGMA quick_check`，读取规则 13 的归属链和两腿是否完整。演练副本只连接本地文件，不启动真实 worker，不导入 `.env`，不访问 Telegram 或开奖接口。需要开奖时使用冻结 fixture。

### 3.3 演练迁移入口

迁移入口为 `DatabaseRepository(explicit_copy_path).initialize_database()`，在隔离开发目录设置 `PYTHONPATH=src`，用生产的 `.venv/bin/python` 执行。不要直接启动 `main` 或 `build_repository()` 的默认路径，以免误连生产库或加载生产运行配置。

建议对照演练前后：

- schema 新增结构符合最终代码，没有破坏性重建或旧表删除；旧列行数及内容哈希保持不变。
- 规则、订阅、路由、账号、目标、消息模板、运行配置原列哈希相同。
- execution_jobs、execution_attempts、progression events、各级日统计及既有 run 原列内容在纯 schema 迁移中不变。
- `quick_check=ok`，外键违规相对迁移前没有新增，重复初始化幂等。
- 执行恢复逻辑的行为演练单独进行，并生成预期变更清单：仅目标旧轮归档、敞口保留、活动指针释放与允许的资金投影变化。
- 对账任务/原消息不会被重新发送；不得调用线上 run-once 验证。

正式发布前需要一份经验证的副本演练结论；副本取样时间与正式备份时间不同，因此正式维护窗口仍重新备份。

## 4. 正式切换顺序（待后续下达）

1. 核验最终提交与 QA 测试针对同一树；确认生产 HEAD 未变化、无陌生未提交改动；发布应 `--ff-only`，避免 force-reset。若设计 HTML 被新提交跟踪，把生产未跟踪旧版安全移入受限发布目录后再快进，不能丢文档。
2. 保存服务与 timer 原状态。暂停 payload-maintenance timer；若对应一次性服务正在运行，等待其完成，不能强杀正在归档的写入。
3. 先停 source-sync 和 auto-trigger，禁止继续产生新任务；在平台仍可回报时观察 sending 清零，给执行器正常回报的机会。每次等待不超过 30 秒，累计预算有界。未能排空时记录全部在途任务，按新未知恢复规则保留，不做伪造完成。
4. 停执行器；再停 auto-settlement、bot、report、alert 等主库进程；最后停 platform。确保全部预定写入者 inactive，确认没有其他主库进程/事务。实验室 worker 默认不停止。
5. 在无写入状态下依次 SQLite backup 主库与执行器状态库，保存完整性结果、文件尺寸、SHA256。此时两库的业务时点一致。复制 `.env` 不必要；如留存配置仅进入受限目录，不输出。
6. 生产代码快进到已验证提交，按原文件模式保证 www 可读。使用 www 身份在明确生产路径初始化新增结构；不要直接执行开轮或发送 worker 来“顺便”迁移。
7. 正式迁移检查旧列保护哈希/计数、schema、quick_check、外键差量和幂等性。首次恢复如由后台自动巡检负责，迁移阶段不得顺带手动修改故障任务；恢复动作的审计单独记录。
8. 先启动 platform，验证本机 health 200/ok；再启动 auto-settlement、source-sync、auto-trigger 与原来 active 的辅助业务服务；最后启动原执行器实例。恢复原 timer 状态。服务清单严格按发布前快照，不启用原本停用的服务。
9. 全部服务使用新进程核验，检查短期日志异常、执行器心跳及故障 run 1937 的归档/未决敞口/活动指针；确认规则 7 的正常轮次和其他配置未被改动。

维护窗口目标应小于一期 210 秒，但该数字不能代替一致备份或校验。副本演练需先测出备份、初始化与校验用时，再选低风险窗口；如果时间明显超出应改计划，而不是跳过数据保护。

## 5. 发布验证与回退边界

真实只读验证项目：

- `/api/health` 200 与 `{"status":"ok"}`，相关静态资源 200。
- 8 个业务服务按原 active 状态恢复，解释器/WorkingDirectory 正确，心跳正常。
- 1937 完整原 pending 快照在新 run 归档中，原未知任务仍可追溯；保留原日 2026-11-26 及已结算第一关收益。
- 调度当前周期不再被 1937 占用，后续新轮必须经过原时段、冷却、风控与额度；不能通过直接开轮创造线上测试投注。
- 未决本金只保留一次，后续新轮不会重复扣 reserve；迟到结果不能覆盖当前 win_streak、cycle_stake 或 pending_plan。
- 核对最终实现支持的 GET 页面/接口展示“执行已结束、账务待核对”，不显示可以补发原过期投注。

回退分为两个阶段：

1. **尚未恢复生产写入：** 若迁移/校验失败，保持服务停止；可恢复配对旧代码与同一维护窗口主库+执行器库，恢复原属主和权限后启动。
2. **已恢复业务写入或已经归档旧轮：** 禁止覆盖旧数据库。保存当前一致快照，优先前向修复。单纯新增表不等于旧代码行为兼容：旧代码不理解 reserve、归档和迟到调整，直接回退旧执行器/调度可能重复使用资金或丢账。必须通过最终实现的兼容开关/停止新调度进行受控降级，不能宣称任意 git checkout 即安全回滚。

## 6. 当前待满足项

- 新 schema、自动退出与归档/资金调整代码尚待完成，无法验证实际迁移耗时与回退兼容性。
- 独立 QA 对 timeout、并发、跨日、原期迟到结果、两腿原子、重复 reserve/释放、多路由、人工停止和旧版本兼容尚待给出结论。
- 需生成本次有效主库一致副本，并完成迁移及故障恢复分支演练。
- 正式部署只在父 Agent 给出已通过 QA 的最终提交并明确下达发布步骤后执行。当前任务不是发布授权。

## 7. 在线副本尝试结果

父 Agent 后续授权执行有界在线 SQLite backup，仍禁止停服务或迁移生产。2026-11-27 使用只读源连接、每批 512 页、busy timeout 0.2 秒、重试休眠 0.02 秒、总预算 30 秒。

- 主库复制在 30.279 秒触发预算中止，共 5,796 次 progress callback、15 次进度回退，符合持续写入导致备份重新复制的现象。
- 已关闭两端连接，并删除此次不完整 `production-preflight.db` 及任何对应 sidecar；没有把不完整文件当作有效备份。
- 脱敏结果：`/var/backups/pc28touzhu/releases/cycle-recovery-20261127/preflight/backup-metadata.json`，文件 0600，目录 0700。
- 因主库预算耗尽，按要求停止本轮备份，不继续反复尝试。执行器状态副本未执行；当前没有可用的本次主库基线，保护表哈希与迁移演练尚不能进行。
- 未停止/重启任何服务，也未改生产数据库或代码。下一步需由父 Agent 选择受控短暂冻结写入窗口，生成一致副本后再做演练；不擅自扩大为长时间全量读锁备份。

## 8. 一次全步复制与保护基线结果

父 Agent 再次明确授权一次全步 `pages=-1` backup，要求独立子进程 30 秒硬超时，开始前 pending/sending=0 且最近 10 秒无新执行任务。该次门槛两项均为 0，执行如下：

| 副本 | 大小 | backup 用时 | 检查 |
|---|---:|---:|---|
| `preflight/production-preflight.db` | 2,037,112,832 字节 | 11.180 秒 | quick_check=ok，39 表 |
| `preflight/executor-state-preflight.db` | 1,134,592 字节 | 0.042 秒 | quick_check=ok，1 表 |

两副本保存在 `/var/backups/pc28touzhu/releases/cycle-recovery-20261127/` 下，04:35:42 UTC 完成检查，均 0600。这是两次分别在线取样，**不是最终切换时的同步配对备份**。

全部指纹计算只读取副本：

- `preflight/protected-main-baseline.json`：39 表、2,707,013 行、foreign_key_check 无违规；耗时 47.761 秒。
- `preflight/protected-executor-baseline.json`：1 表、755 行、foreign_key_check 无违规；耗时 0.026 秒。
- 指纹文件保留原列名称、排序键、行数和 SHA256，不输出原始行或凭证。可用受限目录的 `table_fingerprint.py --baseline <原基线>` 比较迁移后的原列内容。
- 主库文件 SHA256：`1480b55c0aab2e4890adc3dd64f2fb68a726815fc287fe88bb6a9ecb906c6872`。
- 执行器文件 SHA256：`0c2dc534d0b4b6ac9de220bf943c125b2de32d008fddf6f5b372a45cbf161633`。

**线上影响观测：** 04:35:40 UTC，执行器记录 `cycle error: timed out` 并由 systemd 自动重启，NRestarts 由此前 1 增至 2。时间与全步备份的读取锁区间重合，可能是平台写入等待导致执行器 API 超时；此证据不能单独证明全部底层原因，但不能声称备份对线上零影响。随后服务 active/running，health 返回 ok。未主动停止或重启服务，未创建发送任务。

正式备份必须采用第 4 节的停止写入者顺序，不再重复在线全步复制。当前一致演练主库已具备，仍未执行迁移，等待后端实现和 QA 演练入口。

## 9. W2 副本迁移与归档演练

父 Agent 确认 W2 的 168 项测试、compile 与 diff 门禁通过后，授权仅在隔离克隆执行演练。使用新增 `scripts/maintenance/rehearse_cycle_recovery.py`；该脚本拒绝生产路径、原预检备份与硬链接，禁网络，并把 SQLite 连接限制在唯一显式克隆。运行未加载 `.env`、未启动 worker、未调用开轮或发送。

命令：

```bash
nice -n 10 ionice -c3 /www/wwwroot/pc28touzhu/.venv/bin/python \
  scripts/maintenance/rehearse_cycle_recovery.py \
  --db /var/backups/pc28touzhu/releases/cycle-recovery-20261127/preflight/cycle-recovery-rehearsal.db \
  --baseline /var/backups/pc28touzhu/releases/cycle-recovery-20261127/preflight/protected-main-baseline.json \
  --output /var/backups/pc28touzhu/releases/cycle-recovery-20261127/preflight/rehearsal-result.json \
  --now 2026-11-27T06:00:00Z
```

结果 **passed，242.302 秒**：

- 39 张原表的原列行数和哈希与取样基线完全吻合。
- 首次初始化仅新增 `auto_trigger_cycle_archives`，原表内容保持；重复初始化的 schema 与数据幂等。各阶段 quick_check=ok，外键违规无新增。
- 只对 user 1 / rule 13 调用 `detach_expired_auto_trigger_cycles`：run 1937 新增归档 1 条，完整保留原 pending 快照，reserve=13.00，可用投影余额=9.39，session waiting / active_run_id=null。
- execution_jobs、execution_attempts、subscription_progression_events 全部原列哈希保持，包括两条未知流水的 updated_at；已知第一关和其他钱账、配置、额度均不改。
- 原表变化仅为指定旧轮的 state/session/run/runtime，外加新增归档表的 sqlite_sequence 项；具体字段校验通过，范围外行全部保持，规则 7 不变。
- 第二次 detach 返回 0，所有表保持，不重复冻结本金。

原始脱敏结果及源码哈希位于受限 preflight 目录：`rehearsal-result.json`、`rehearsal-source-before.json`、`rehearsal-source-after.json`，均 0600。此演练期间后端继续 W3 实现，`db_repository.py` 运行前后哈希变化，其他三个相关文件不变。因此它是 W2 生命周期证据，最终发布前仍需对冻结代码的新增 W3 schema 做补充验证，不能把本次结果等同最终完整发布验收。

## 10. 最终 schema 全量演练与冻结补验

使用原静态预检库创建新的 `cycle-recovery-final-rehearsal.db`，重新执行完整基线、两次初始化、精确旧轮归档与重复归档，**passed，224.711 秒**。新增表为：

- `auto_trigger_cycle_archives`
- `execution_reconciliation_state`
- `execution_reconciliation_audits`
- `execution_ignored_reports`

全部 39 原表的原列迁移前后保持；1937 归档、13.00 reserve、9.39 可用投影、原 jobs/events/attempt 与其他规则不变、重复处理无变化、完整性/外键全部通过。脱敏完整证据为 `preflight/final-rehearsal-result.json`。

运行期间最终补入了 API 输入校验。独立使用后端重建的旧源码检查，其 SHA256 精确匹配本次演练起始值 `e7fd946757905824e57f3f57f1f359fbd309b2a82d1834d5a16454937568ebcf`；与冻结版 `ba3cc950593cb051a3f840b992dca850aa72b8ef7903bf70c2886e24c1c00635` 逐方法 AST 对比，唯一变化为 `_verified_reconciliation_result`，无方法增删，迁移、归档及资金方法未变。

随后在该冻结源码上重新初始化并重复 detach，重新扫描全部表：schema 不变、changed_tables=[]、detach_count=0、quick_check=ok、FK 违规 0，执行前后源码 hash 同为 ba3cc950。证据分别为 `final-validator-diff-verification.json` 和 `final-rehearsal-frozen-idempotency.json`。

私有发布编排脚本已经准备，仅做语法/help 检查，尚未执行：`/var/backups/pc28touzhu/releases/cycle-recovery-20261127/release_cycle_recovery.py`，0700。默认 `--phase preflight`，只有父 Agent 下达发布和目标提交后才执行 `--phase cutover`。它按本预案保存原 active 状态、停写、生成主库+执行器配对备份、原列校验、www 迁移、精确恢复规则 13，再恢复原服务。失败保存受限 manifest，不自行覆盖生产库或删除证据。

**STATUS: DONE_WITH_CONCERNS**

**REASON:** 生产只读预检与可执行发布顺序已完成；代码、QA 与副本演练尚未就绪，因此没有执行生产变更。
