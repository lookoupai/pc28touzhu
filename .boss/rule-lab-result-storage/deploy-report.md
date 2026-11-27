# 结果型实验存储发布准备与验收记录

## 摘要

- 当前仅完成**只读发布准备，尚未部署**。最终QA通过后，根代理须提供明确GO及已验收commit，才能启动唯一发布执行者。
- 继续现有本机systemd部署，实验worker保持1核/256MiB，批准队列保持5%CPU/128MiB；无依赖安装、新进程平台或新定时任务。
- 本次有意移除终态逐笔、闭轮及无依赖历史，验收应保护配置、累计成绩、日账、pending、当前风控、水位和队列，不能再要求所有旧表行数不变。
- 已完成生产HTTP和实际页面脚本DOM的只读基线，24次GET、0写入；未启动浏览器进程，未读取用户浏览器Cookie。视觉布局仍由产品验证记录，不把DOM测试冒充截图验收。
- 实际CLI参数已适配为一份备份流程；默认只初始化，另有显式drain参数。CLI仍在补最终保护与退出条件，停写窗口是否集中清理等待QA实测和ROOT最终决定，尚未放行apply。

## 1. 只读预检

2026-11-27 16:59 UTC采样：生产main为`0b3d84d`且干净；schema5、auto_vacuum=2、WAL。602个版本（ID495～1096）、10个批次、6个连续系列。目录807,876,224字节，主库807,542,784字节，内部空闲页0；磁盘可用约99GiB。

批次11运行中，14/17尚未开始；其他状态会随队列和每日续算变化。正式发布必须停写后重新读取，不能把准备阶段采样当作固定状态。服务均active：API PID1355033、worker PID1355039、队列PID1355041、执行器PID1355075；上游预测应用PID1250128保持。

| 检查 | 命令 | 退出码 | 结果与证据 |
| --- | --- | ---: | --- |
| 状态、资源、磁盘 | `python3 /tmp/pc28-results-release.py --output /tmp/pc28-results-release-preflight.json` | 0 | 生产只读，资源上限及源进程保持 |
| 固定快照保护摘要自检 | `python3 /tmp/pc28-results-release.py --verify-snapshot /tmp/pc28-results-baseline.db` | 0 | 602版本、14,893日账、47pending、331当前依赖风险日、5series/9segments；仅证明摘要可读取/自比，不代表迁移通过 |
| HTTP及生产JS的DOM基线 | `node /tmp/pc28-results-release-online.cjs --baseline` | 0 | `/tmp/pc28-results-release-http-before.json`，24GET，结果/日月/管理/续算/存储实际执行 |
| 脚本语法 | `python3 -m py_compile /tmp/pc28-results-release.py`；`node --check /tmp/pc28-results-release-online.cjs` | 0 | 通过 |
| 未冻结CLI时拒绝apply | 仅验证本地脚本参数前置分支，未停服 | 2 | 在读取/执行迁移前拒绝，生产无变化 |

固定快照由根代理通过SQLite backup生成：`/tmp/pc28-results-baseline.db`，777,437,184字节，SHA256 `5e44a9a50e6da2a95d4330f0f7b7d6207f9f098c20f29c60827b39d1e99a53f3`。发布准备没有复制或写入它，也没有读取/备份上游2GB数据库。

## 2. 已准备的脚本

- `/tmp/pc28-results-release.py`：默认只读；提供动态预检、受保护状态读取/对比，以及受控发布入口。
- `/tmp/pc28-results-release-online.cjs`：HTTP + jsdom，只执行实际生产`rule_lab.js`，拦截所有非GET和外站请求。会检查原ID/批次8经济摘要、进度不倒退、results_only详情、日/月结果、管理/续算、空间策略和重测预览。
- `/tmp/pc28-results-release-migration-arguments.json`：已按真实CLI写入 `--database {database} --backup {backup} --apply`，只做小迁移。
- `/tmp/pc28-results-release-migration-drain-arguments.json`：相同参数加 `--drain --max-steps 100000`，供ROOT依据QA耗时选择。两份均为严格白名单argv数组，不执行shell片段；发布脚本记录`drain_complete`，未明确完成不能写成清理成功。

所有临时证据与脚本权限0600。应用会话签名在进程内存中由现有应用配置生成，未读取浏览器配置或浏览器凭证，也不把密钥/会话写入日志。

## 3. 待执行的发布流程

根代理通过最终QA并明确GO后：

1. 核对生产工作树干净、已验收commit是当前HEAD的后继、CLI存在、冻结参数可读、备份路径不存在且可用空间足够。
2. 记录原服务启停状态，依次停止queue→worker→API。再次读取实际实验状态、原队列字节及受保护字段摘要。
3. 快进生产代码，以绝对路径`/usr/sbin/runuser -u www`调用实际迁移CLI；由该CLI生成一份外置0600原库备份并迁移schema6。ROOT依据QA的777MB固定副本实测（204.382秒清理、95.9MiB主库、结果和47pending保护通过）选择集中drain参数；每步仍限制500行和256回收页，直到明确完成或保留未完成原因。实际生产数据和耗时动态不同，按正式记录为准。
4. 对备份与迁移后数据库核对受保护语义：原配置/累计stats/日账、所有原pending、当前依赖风险日、batch旧cursor字段与水位、系列配置、段身份、控制回执/ID高水位。允许新增`retention_v1`等元数据，不能覆盖旧语义。
5. 校验schema6、INCREMENTAL/WAL、quick_check和FK；队列清单字节保持。只启动之前active的服务，其他业务服务与上游进程不重启。
6. 等待执行器因短时API中断自动重连，核对1核/256MiB及队列限额保持，原ID仍在、进度不倒退。先记录`deployed_pending_http_and_cleanup_verification`，不提前宣称任务完成。
7. 用只读HTTP/DOM脚本验收新页面与实际API；观察后台有界清理及物理回收达到稳定状态，并确认新终态不增长。

脚本执行形式：

```bash
python3 /tmp/pc28-results-release.py --target <根代理验收commit> --apply
node /tmp/pc28-results-release-online.cjs
```

仅当ROOT选择集中清理时，发布命令另加 `--migration-arguments /tmp/pc28-results-release-migration-drain-arguments.json`。CLI自身生成唯一数据库备份，发布脚本只另备份小型队列文件，不重复创建第二份数据库备份。

如果发布前已有`resource_paused`，先记录具体原因。清理后只能恢复可证明属于原批准计算且因空间保护暂停的对象；不能把用户手动暂停或已编辑退出队列的实验一律恢复。Core已确认恢复接口：停写时记录同时满足`resource_paused`、原manifest批准、`queue_eligibility`仍eligible且配置hash相同的白名单。普通批次通过`service.set_batch_state(user_id,batch_id,{"state":"running"},now=...)`恢复并检查容量；连续系列按owner粒度恢复，要求系列非用户暂停且仍有active段，不改变capture/compute。已完成且无active段的系列无需恢复，原每日设置继续生效。遇运行容量不足，发布任务持续追踪白名单待恢复项，不能遗忘或扩大到普通paused对象。此步骤只在最终发布清理之后执行，不在预检中执行。

### 最新CLI适配与恢复白名单预检

17:34:50 UTC只读采样，目录859,045,951字节；批次17因“模拟存储空间不足”处于resource_paused、processed=30、revision=65。原manifest已批准且started=true、configuration_hash一致、无queue opt-out，未接入series；它是本次唯一恢复白名单候选，其他completed/paused状态不自动恢复。证据：`/tmp/pc28-results-release-preflight-cli-ready.json`。正式发布仍须停写后重新取得该白名单。

真实CLI默认预览命令 `PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 scripts/maintenance/migrate_rule_lab_results.py --database /www/wwwroot/pc28touzhu/data/rule_lab/rule_lab.db` 退出0，只读返回schema5目标6及空间信息；未apply。脚本已适配实际retention运行key `result_retention`，语法检查通过。

ROOT根据固定副本实测选择停写集中drain。发布脚本现已接入：只有`drain_complete is True`且保护摘要、完整性、队列校验均通过，才在三个写者仍停止时调用单独的www进程恢复白名单中的批次17。该进程再次核对manifest字节hash、原批准started、owner、configuration hash、queue eligibility/opt-out、resource_paused状态及原revision；如属series，还核对series revision、active段及采集/自动开关。通过现有`service.set_batch_state`恢复，进度不得变更，其他批次不在本次恢复范围。恢复后记录resumed_ids，并在服务启动后等待17处理数前进；容量/输入等异常存入recovery_pending_ids并将发布标为pending_recovery，不能遗忘。清理未完成绝不恢复。

准备验证使用假的runner与服务读取函数，4个分支通过：未完成drain不调用恢复、只允许17且排除18、writer active时拒绝、空白名单无操作；只做AST/参数分支测试，没有真实子进程、服务或数据库写入。实际恢复仍等待ROOT最终GO。

## 4. 空间收敛和备份边界

固定副本约263万终态，若严格每tick最多500行，至少需约5,253个tick；250ms循环的纯间隔下限约22分钟，持续回放占用时间还会延长。QA必须量化清理吞吐，正式发布应记录实际收敛时间，不能仅看到第一批删除成功就宣称空间已回收。

最终分别记录DB、WAL、目录实际字节、内部空闲页、终态/闭轮剩余数量、受保护输入积压和备份字节。清理行数不是物理释放量，保留的外置备份也不是“免费空间”。

本次只生成一份回滚数据库备份，另存小型队列文件。验收后记录72小时人工复核期限及移除命令，不创建新定时器，不删除此次范围之外的旧备份。恢复后新成绩不得被旧备份覆盖；schema6不能直接退回仅识别schema5的旧程序。

## 5. 正式发布结果

待最终QA、实际CLI契约和根代理GO后补充：commit、备份路径、暂停时长、受保护摘要、真实缩减量、清理收敛耗时、HTTP/DOM结果与服务/进度恢复。

**STATUS: DONE_WITH_CONCERNS**

**REASON:** 只读发布准备和线上基线已完成，实际CLI参数已适配，等待其最终QA冻结以及正式迁移、恢复白名单和清理后验收。
