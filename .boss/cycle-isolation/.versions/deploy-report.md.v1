## 摘要

- 本次仅完成隔离工作树实现与验证，**没有生产部署、生产数据迁移、服务重启或真实消息发送**。
- 本地真实 WSGI＋临时 SQLite 已验证健康接口 200/ok；UI配置创建、编辑、legacy保存和恢复待命可持久化，fixture退出确认投注任务为零。
- 数据变更是加性 schema 加保守归属迁移；四类匿名旧数据回放保持历史事件和账本不变，歧义归属阻止自动恢复。
- 发布需备份数据库并统一停止、切换所有访问仓储的进程；禁止只重启平台和自动触发而让来源同步、结算、Bot或执行器混用旧版本。
- 回滚须配对代码和数据库快照；恢复生产发送后不得直接回滚旧数据库丢弃新下注记录。

# 发布准备报告

## 1. 已完成的隔离验证

环境：`/tmp/pc28-cycle-isolation`、Python 3.12.3、Node 22.21.1；既有 Python WSGI＋SQLite＋原生 JS，没有新增运行依赖或修改 systemd/CI。

| 验证 | 实际结果 | 证据 |
|---|---|---|
| 本地健康接口 | HTTP 200，`{"status":"ok"}` | `evidence/qa-final-browser.log` |
| 真实配置 UI→WSGI→SQLite | 3组回放通过，0投注任务 | `evidence/qa-final-ui.log`、`tests/cycle_ui_server.py` |
| 桌面/手机规则页与手动页 | 4场景通过，无pageerror或页面横向溢出 | `evidence/qa-final-browser.log`、`qa-*.png` |
| 旧自动pending/closed/route0/ambiguous迁移 | 4类通过，重复初始化幂等，旧事件/账本行不变 | `evidence/qa-migration-replay.json` |
| 完整后端回归与最后修复 | root完整647项通过；最后旧无route门禁修复后124项相关＋2项仓储验证通过；未宣称649项全量 | `/tmp/pc28-cycle-wave3-final.log`、`/tmp/pc28-cycle-final-legacy-guard.log`、`qa-report.md` |

本地 WSGI 绑定随机 `127.0.0.1` 端口，测试结束已停止，临时库已清理；不存在持续公开的预览服务。可在需要时运行 `node tests/test_cycle_ui.cjs` 或 `node .boss/cycle-isolation/evidence/qa-browser-replay.cjs` 重新建立隔离验证环境。

## 2. 数据变化及兼容原则

- 保留手动 `subscription_cycle_states`，增加归属版本标记。
- 新增自动周期、规则方案会话、日启动次数和迁移证据表；规则新增cycle_policy，路由新增is_implicit。
- 旧缺cycle_policy规则维持legacy生命周期快照；不自动切换到新默认，不自动解封已收工。
- 确认自动归属的旧共享状态移至自动表，原始证据留存；不复制余额到手动，不改变已入账盈亏或事件/任务归属。
- 旧无route运行轮次沿原路径排空整轮，完成后下一次启动才走内部继承路由；手动重置在此期间受保护。
- 不明归属标blocked；不能通过换日、手动重开或普通保存自动复活。

## 3. 生产发布步骤（尚未执行）

1. 确认候选代码与 `qa-final-tree.json` 指纹相符，完成最终review；记录已知Gate 0基线静态告警，不将Gate1/2跳过当作验证。
2. 选择维护窗口，记录当前手动/自动在途、已收工及最新已发送期号。停止所有访问同一数据库或发消息的 `pc28touzhu` 服务，包括 platform、source-sync、auto-trigger、pc28-auto-settlement、alert-notifier、telegram-bot、telegram-report、全部 telegram-executor 实例。只停止存在的服务，不安装或启用未运行实例。
3. 使用 SQLite backup API 生成完整一致快照，并记录代码旧版本、数据库真实路径、文件权限。不要只复制有活动WAL的主db文件；不要输出环境凭证。
4. 在停止的进程条件下切换同一版代码，用与生产一致解释器只调用 `DatabaseRepository.initialize_database()` 执行迁移。不要以run-once或发送来验证。
5. 运行SQLite完整性/外键检查，并对比迁移前后 progression事件/任务数量及已结算金额总和、待结算leg IDs、closed/blocked归属；若不平则保持服务停止并回滚代码＋数据库配对快照。
6. 先启动平台并检查 `/api/health`，只读读取规则/手动状态确认归属；然后按统一版本恢复结算、来源同步、自动触发及其余原有worker，执行器最后恢复。首次恢复即可能按现有规则继续投递，必须在完成上述对账后执行。
7. 检查服务active、无反复重启及迁移异常；观察第一轮新事件的rule/sub/route/session归属、日次数和任务幂等。新功能不自行提高现有规则额度，不改变实际用户收工设置。

## 4. 回滚边界

- 恢复发送前：所有服务仍停止时，可恢复旧代码与迁移前一致数据库快照，并核对权限后恢复原服务。
- 恢复发送后：先停止派单/执行及相关worker、保存新数据库，不直接用旧快照覆盖新增任务/回执。需保留已发送记录并制定前向修复或经过对账的回滚，避免重复发送或丢账。
- 新旧进程不能同时写共享状态。迁移期间的legacy证据和blocked说明应保留供核对。

## 5. 未执行项目

生产数据库迁移、生产启动耗时测量、生产负载P99、真实Telegram投递和生产规则解封均未执行；这些不能由匿名fixture的结果替代。当前交付是可审查的代码、完整测试证据和发布步骤，不是上线成功声明。
