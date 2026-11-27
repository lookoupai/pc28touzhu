# 结果型实验存储发布与验收记录

## 摘要

- **已发布并验收通过**：代码`d3f8c9f`，2026-11-27 UTC，现有systemd平台。
- 实验主库从**819.25 MiB降至52.30 MiB，减少93.62%**；终态逐笔与闭轮记录均清为0，恢复计算后仍为0。
- 602个实验版本、累计成绩、23,422条日账、62笔迁移时pending、541个必要风险日及原控制/游标字段保持；仅按用户方案删除历史冗余。
- 三个实验写者暂停182.658秒后恢复。仅恢复原批准且因空间保护暂停的批次17，已从原30期推进到1,028期，无错误。
- 正式离线drain同QA条件限制为1个CPU、256MiB地址空间；worker/队列资源限制保持。来源同步、自动触发、自动结算及上游应用PID保持。
- 25次真实GET及生产页面脚本DOM验证通过，0POST、0外站请求、0异常；未启动浏览器进程，不声称本轮完成视觉截图验收。

## 1. 发布前门禁与准备

ROOT明确放行exact commit `d3f8c9f`。本轮完整回归1259项通过、0失败、0跳过，310.386秒；接口/真实DOM流程、矩阵旧新经济结果、固定生产副本和独立评审均通过，详见同目录QA报告。

准备脚本：

- `/tmp/pc28-results-release.py`：默认只读，正式apply必须明确commit；保护语义摘要、单备份CLI、限定恢复白名单、服务与进度验收。
- `/tmp/pc28-results-release-migration-drain-arguments.json`：严格argv数组，包含`--database {database} --backup {backup} --apply --drain --max-steps 100000`。
- `/tmp/pc28-results-release-resume-source.txt`：只恢复获准批次17的独立进程，复核全部停写时前置条件。
- `/tmp/pc28-results-release-online.cjs`：HTTP+jsdom执行真实生产HTML/JavaScript，并拦截非GET或外站请求。

所有临时发布证据和备份权限0600。应用认证签名只在进程内存中使用，未读取用户浏览器Cookie，未向日志输出密钥或会话。

准备时schema5、INCREMENTAL、WAL、602个版本。运行中的队列继续产生实验结果，最终发布前主库实际859,045,888字节；批次17在30期处因空间保护暂停。因此发布采用停写集中有界drain，不在恢复运行后等待长时间后台处理旧百万级明细。

## 2. 实际执行记录

| 检查 | 实际命令 | 退出码 | 结果/证据 |
| --- | --- | ---: | --- |
| 正式发布与drain | `python3 /tmp/pc28-results-release.py --target d3f8c9f --apply --migration-arguments /tmp/pc28-results-release-migration-drain-arguments.json` | 0 | session62242；`/tmp/pc28-results-release-apply.log`及`/tmp/pc28-results-release-rollout-20261127T180555Z.json` |
| 发布后HTTP/DOM | `node /tmp/pc28-results-release-online.cjs` | 0 | session55801；`/tmp/pc28-results-release-online.json`及`.log`，25GET/0写 |
| 恢复后真实行数和进度 | 只读SQLite读取终态/闭轮/版本/批次17及retention元数据 | 0 | `/tmp/pc28-results-release-after-cleanup.json`，18:09:55 UTC |
| 公开资源 | `curl -A 'Mozilla/5.0' -fsS https://tg.dca.de5.net/assets/rule_lab.js`，与生产文件逐字节比较 | 0 | HTTP200，内容相同；ROOT独立复核亦通过 |
| 发布资源wrapper隔离验证 | www账户临时probe读取CPU affinity、RLIMIT_AS与argv | 0 | 1CPU、268,435,456字节限制，含空格参数保持；未操作生产 |
| 恢复白名单分支验证 | 假runner/服务状态的4分支检查 | 0 | 未完成drain不调用；仅17；writer active拒绝；空名单不操作 |

发布前只读HTTP/DOM基线亦通过：`node /tmp/pc28-results-release-online.cjs --baseline`，24GET、0写，`/tmp/pc28-results-release-http-before.json`。基线是实际线上程序执行，不是mock API。

## 3. 停写、备份、迁移和保护校验

发布在18:05:55 UTC开始，按queue→worker→API停止。停写1.423秒后完成服务状态确认，然后动态采集原状态及队列，快进到已验收版本，由www账户调用实际维护CLI。

离线CLI运行在显式资源wrapper内：`os.sched_setaffinity`固定允许CPU集合中的一个CPU，`resource.RLIMIT_AS`软/硬上限均256MiB，通过结构化`sys.argv`和`runpy.run_path`执行CLI，无shell参数拼接。实测进程CPU affinity为CPU0，RSS约32MiB，最大地址空间268,435,456字节。

| 项目 | 实际结果 |
| --- | --- |
| 数据库备份 | `/tmp/pc28-results-release-backup-20261127T180555Z.db`，859,045,888字节，www所有，0600 |
| 小型队列备份 | `/tmp/pc28-results-release-queue-20261127T180555Z.json`，4,159字节，0600 |
| schema | 5→6，INCREMENTAL=2与WAL保持 |
| CLI时间 | 177.983秒，含单份备份、迁移、drain和保护检查 |
| 清理步数 | 6,439步，每步最大500行，物理回收每步最多256个freelist页 |
| 清理结果 | `drain_complete=true`，retention phase=idle，移除3,098,309条冗余记录 |
| 完整性 | quick_check=ok，FK错误0 |
| 停写总时间 | 182.658秒；全部发布步骤185.785秒 |

停写期间对原库、唯一备份、迁移清理后的库分别比较受保护字段。原配置、累计stats、每日成绩、runtime已有字段、pending原行、原cursor各字段、系列设置、段身份、控制回执/ID高水位保持。cursor允许新增已验证的`retention_v1`；不以JSON序列化字节顺序变化误报，不允许覆盖原含义。

保护摘要：`f60dba2a9704f45447e7f72af7909cac23fa8739122216f2acac9c49caa8e2d6`，覆盖602版本、10批次、23,422日账、62pending、541必要风险日、8series、12segments。队列文件字节在迁移过程中保持。

本次允许清理终态逐笔、已结束轮次、无依赖旧风险日/输入/快照，因此没有错误地要求所有旧表全部旧行仍存在。

## 4. 空间与恢复计算

| 体积 | 清理前 | 清理完成、恢复写入前 |
| --- | ---: | ---: |
| 实验主库 | 859,045,888B（819.25MiB） | 54,837,248B（52.296875MiB） |
| 实验目录 | 859,140,527B | 54,874,175B |
| 主库减少量 | — | 804,208,640B（766.953125MiB），93.6165% |

实际已保留的输入范围按生产当时的计算依赖判定。比QA固定副本95.9MiB更小，是因为生产其他批次已继续完成、相应输入不再被待算任务保护；没有为达到数字而删除必要输入。

全部保护校验后，三个写者仍停止时再次验证原候选17：原manifest批准且started、configuration hash保持、user归属一致、queue eligibility有效且无opt-out、状态仍resource_paused、revision仍65、错误确为存储不足。17未接入series。随后通过原`service.set_batch_state`恢复为running，原进度30不变；不修改其他批次，不改变任何自动采集/每日续算开关。

恢复记录`resumed_ids=[17]`，waiting/blocked均空。服务启动后立即确认17推进至72；18:09:55进一步推进至1,028，error为空。该时刻终态逐笔数0、closed轮数0，pending为68（恢复后正常新生成的工作记录），602个版本保持。后台以后仍做小批依赖维护，当前phase变化不代表初次drain未完成。

## 5. 服务与线上界面

| 服务 | 发布后PID | 结果 |
| --- | ---: | --- |
| 平台API | 1408558 | active |
| 实验worker | 1408564 | active，CPU1核/256MiB保持 |
| 批准队列 | 1408568 | active，5%CPU/128MiB保持 |
| Telegram执行器 | 1408628 | API恢复后自动重连，线上心跳1秒 |
| 上游预测应用 | 1250128 | PID保持，未重启/改库 |
| 来源同步/自动触发/自动结算 | 1149035 / 1149036 / 1149027 | 全部PID保持 |

公开访问地址：`https://tg.dca.de5.net/rule-lab`。

HTTP+jsdom验证实际生产脚本：原批次8经济摘要保持，累计与日/月结果、资金表现、管理和增量续算DOM可读；详情`detail_policy=results_only`、`retention_version=1`、recent与recent_rounds为空，逐笔/历史轮次/旧月修订DOM移除；storage返回结果保留策略；原区间重测预览显示`current_source`且可用，没有声称仍拥有原全历史冻结档案。共25GET，0POST、0外站请求、0JS异常。

本次未开启任何新浏览器，DOM执行不提供实际像素布局证明，该边界如实保留。

## 6. 备份生命周期与恢复边界

当前回滚数据库备份859,045,888字节仍单独保留，不将主库缩小量声称为服务器净磁盘腾空。生命周期记录：`/tmp/pc28-results-release-backup-20261127T180555Z.json`，验收后72小时人工复核移除，不创建新定时器，不删除本次范围外的旧备份。

ROOT已实际清除本轮专用测试原快照及其WAL/SHM，共777,469,952字节；证据`/tmp/pc28-results-test-snapshot-disposal.json`。正式生产回滚备份859,045,888字节仍保留。QA临时迁移副本亦已清理。测试副本移除与生产主库回收分别计量，不触碰本次范围外旧备份。

schema6上线后不直接退回仅支持schema5的旧代码，不用旧备份覆盖恢复后新增的成绩。本次发布一次成功，无回滚覆盖、无待恢复白名单项。

**STATUS: DONE**

**REASON:** 正式迁移、完整有界清理、真实缩盘、保护数据核验、限定批次恢复、服务健康及线上只读结果验证全部通过。
