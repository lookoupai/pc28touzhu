# 周期隔离发布记录（2026-11-05）

本记录补充开发阶段的 QA 与发布准备报告，只记录本次实际生产发布证据。开发报告中“未部署”的描述保留为当时事实，不追改历史。

## 版本与备份

- 原生产版本：`5b37efaaf84e735d5bb3d2656b02e447867f5e2e`。
- 功能提交：`25b3183cbc03e8687a750c2cd694483edd40f1c1`；实际生产部署版本为闭轮归属修复 `52e29586d9ec5021d7c1812a4796f64897c08f75`。
- 生产目录：`/www/wwwroot/pc28touzhu`；数据库：`/www/wwwroot/pc28touzhu/pc28touzhu.db`。
- 发布证据及备份目录：`/var/backups/pc28touzhu/releases/cycle-isolation-20261105/`。备份与原始审计文件保留在服务器受限目录，不提交数据库或凭证。

## 发布前实库副本演练

使用 SQLite backup API 生成一致副本。第一次分段备份受线上连续写入影响未完成，旧 `rehearsal.db` 不构成可用备份或验证证据，已连同其 journal 删除；有效演练使用完整一致副本。

第一轮演练保持 15 张业务表原列哈希不变、外键无新增违规、`quick_check=ok`、重复初始化幂等，但发现方案 17 的旧同日多规则作废记录造成归属误判。发布在生产切换前暂停，补丁要求当前闭轮的结束时间、完整双腿、期数、逐期整数投注金额与余额链同时吻合，才缩小历史归属证据；未采用简单忽略作废记录的方式。

补丁的 3 项新增回归覆盖可证明的后续闭轮、旧轮重叠以及最新记录作废/金额不符；仓储两组回归实际执行 106 项，106 通过，0 失败，耗时 23.744 秒。开发阶段完整回归为 647 项；上述数量存在重叠，不相加成完整独立用例数。

第二轮 `rehearsal-verified-migration.json` 实测耗时 16.96 秒：15 张保护表原列逐行 SHA256 完全一致，`changed_tables=[]`、`new_fk_violations=[]`、`quick_check=["ok"]`、`idempotent=true`。独立只读核验结果如下：

| 旧方案 | 确认归属 | 余额 | 期数 | 迁移后状态 |
|---|---|---:|---:|---|
| 17 | 自动规则 9 / 路由 9 / 运行轮 1516 | 1.20 | 3 | stopped，day_closed=1；会话 day_closed |
| 18 | 自动规则 11 / 路由 11 / 运行轮 1518 | 2.35 | 7 | stopped，day_closed=1；会话 day_closed |

两条原状态均属于自动规则。统计日、余额、当前轮金额、日盈亏、连胜数、目标关数、状态、停止原因、期数、待结算内容、策略、日轮数和封盘字段均与迁移前原始证据一致。两会话均无 active_run_id；没有重复生成手动余额，没有遗留 blocked 迁移。旧账本及作废记录全部保留。

## 实际生产迁移与恢复

**已上线。** 2026-11-05 UTC，原有 8 个服务统一停止后，使用 SQLite backup API 生成 `production-before.db`，大小 2,121,392,128 字节、权限 0600。生产代码统一切换至 `52e2958`，直接初始化仓储执行迁移；没有通过 run-once 或创建测试投注来验证。

`production-migration.json` 实际用时 43.23 秒：15 张保护表原列哈希全部相同、0 新增外键违规、`quick_check=ok`、再次初始化幂等。两条旧状态归属与上表相同，无歧义封锁，手动状态表为空。原规则、路由及方案配置列哈希不变。

平台于 03:47:49 UTC 启动，其余原有 7 个服务于 03:48:21 UTC 恢复。03:49:08 UTC 独立检查：

| 检查项 | 实际结果 | 服务器证据 |
|---|---|---|
| 8 个原有服务 | 全部 active/running，NRestarts=0；新 MainPID 与启动时间已记录 | `qa-release-verification.json` |
| 本机 `/api/health` | HTTP 200，`{"status":"ok"}` | `qa-release-verification.json` |
| 新自动/手动页面脚本 | 两个 assets 均 HTTP 200，新增功能标记核验通过 | `health-after.json` |
| 规则 11 配置 | status 原为 active、现仍 active，全部旧列与备份相同；新配置列仍兼容旧策略 | 独立只读对比 production-before.db 与在线库 |
| 规则 11 周期 | legacy，day_closed，active_run_id=null，max_cycles_per_day=1 | 独立只读在线库检查 |
| 旧封盘状态 | 规则 9 余额 1.20、规则 11 余额 2.35，均保持 stopped/day_closed；无重复手动余额 | `production-migration.json` 及恢复后的只读检查 |

恢复的服务为 platform、source-sync、auto-trigger、pc28-auto-settlement、alert-notifier、telegram-bot、telegram-report 和 telegram-executor@executor-001。没有新增或启用其他实例。`manifest.json` 记录实际版本、服务清单、数据库原属主/权限；最终状态为 released_verified。功能代码已推送 GitHub main，发布者核对本地与远端差异为 0/0；本文件作为发布证据另行提交。

生产重新开始正常业务后，不能直接覆盖迁移前数据库作回滚，以免丢失新任务或回执。需先停止相关写入并保存当前库，再对账决定前向修复或配对回滚。

## 扫描结果与限制

开发阶段 Boss Gate 0 的 innerHTML 静态告警在干净基线也存在；附加 OWASP 扫描仅 a07-xss 模式告警，其余 8 类检查通过。新增 UI 的动态值已审查转义，恶意名称回放通过；这些证据不等于全仓输出点安全审计，也不构成“所有扫描全绿”。未识别测试框架或跳过压测的门禁不计作已验证项目。

发布核验使用只读健康检查、数据库对账与服务状态检查，不人为创建投注任务或发送测试 Telegram 消息。恢复既有执行器后可能继续既有规则的正常业务；不能以健康检查声称已验证生产吞吐或所有未来投注结果。
