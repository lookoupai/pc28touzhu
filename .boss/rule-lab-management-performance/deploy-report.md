# 实验管理、性能与空间回收发布记录

## 发布结果

2026-11-27 UTC，已部署代码 `390aa4508c8c8ad18073f7a4a5c7f3fce27d75bf`。W1～W5全部通过，实验管理、性能优化和分批空间回收已上线。冻结版本完整回归1197项通过、0失败、0跳过；新管理页面和三个既有浏览器流程通过。

根代理执行经过预检的受控脚本，依次暂停批准队列、实验worker和API，备份后迁移schema5并转换SQLite空间模式。停写71.946秒，总发布检查73.924秒；此前活跃服务均已恢复，两批正在计算的实验继续推进。

## 实际执行证据

| 检查 | 命令 | 退出码 | 结果 |
|---|---|---:|---|
| 发布与数据核验 | `python3 /tmp/pc28-management-rollout.py --target 390aa45 --apply` | 0 | `/tmp/pc28-management-rollout-20261127T155206Z.json`，session77762 |
| 线上只读浏览器 | `node /tmp/pc28-management-online.cjs` | 0 | `/tmp/pc28-management-online.json`，session82220 |
| 公开静态资源 | GET `https://tg.dca.de5.net/assets/rule_lab.js` | 0 | HTTP200，响应内容与生产代码字节一致 |
| 冻结版本全回归 | `PYTHONPATH=src`及生产副本/只读来源环境变量下 `python3 -m unittest discover -s tests -q` | 0 | 1197/0/0，329.800秒，`/tmp/pc28-management-full-final-root.log` |

发布日志 `/tmp/pc28-management-rollout-final.log`、线上日志 `/tmp/pc28-management-online-final.log`。认证信息只在进程内存中使用，没有写入报告或日志。

## 数据、队列与备份

停写后动态采集并校验22张原表、1,739,112行原数据全部旧列；迁移和整理前后流式哈希完全一致。10个批次、602个版本（ID495～1096）、2个连续系列保持，批次状态、游标、已处理期数、原开始时间和队列文件字节保持。没有产生管理任务，没有删除现有实验，也没有新建实验或更改续算设置。

| 备份 | 路径 |
|---|---|
| schema4原实验库 | `/tmp/pc28-rule-lab-before-management-20261127T155206Z.db` |
| schema5整理前实验库 | `/tmp/pc28-rule-lab-before-vacuum-20261127T155206Z.db` |
| 原批准队列清单 | `/tmp/pc28-rule-lab-queue-before-management-20261127T155206Z.json` |

三份备份位于实验预算目录之外，权限0600。实验主文件由478,371,840字节整理为436,006,912字节；备份仍被保留，此数字不是服务器净磁盘释放量。生产现为schema5、WAL、auto_vacuum=2，quick_check=ok、外键错误0。恢复写入后文件会随实验增长。

## 服务与进度

| 服务 | 恢复后PID | 核验 |
|---|---:|---|
| 平台API | 1355033 | active |
| 实验worker | 1355039 | active，1核CPU、256MiB内存保持 |
| 批准队列 | 1355041 | active，5%CPU、128MiB内存保持 |
| Telegram执行器 | 1355075 | API恢复后自动重连，线上心跳2秒 |
| 上游预测应用 | 1250128 | PID保持，未重启或修改上游数据库 |

来源同步、自动触发、自动结算三个业务服务PID保持。恢复后的只读检查确认批次12由15,532期推进至15,715期，批次16由15,880期推进至16,015期。完成批次8的经济摘要与发布前完全相同。

## 线上界面验收

真实生产API和页面均只读检查，0POST、0外部拦截、0浏览器异常。storage返回incremental_ready=true及各项实际空间；批次8已计算、不可原地编辑但可新建修改版，删除预览显示26版本且完整名称未填写时确认按钮不可用；尚未开始批次17可编辑。

桌面1440与手机390无横向溢出，根代理复核手机管理/删除预览及存储截图：`/tmp/pc28-management-online-desktop.png`、`/tmp/pc28-management-online-mobile.png`、`/tmp/pc28-management-online-storage-mobile.png`。管理预览响应实测13ms。线上没有提交改名、编辑、复制、删除或回收操作。

## 恢复边界

已保留升级前与整理前备份。schema5写入后不能直接切回只识别schema4的旧代码，也不能用旧备份覆盖服务恢复后生成的新成绩。此次升级一次成功，未执行恢复覆盖。后续删除按500行、空间回收按至多256个freelist页分步推进；遇到占用显示等待，释放量按实际文件记录。

**STATUS: DONE**

**REASON:** 完整回归、原数据保持、资源及服务恢复、进度继续和线上只读验收全部通过，CM9关闭。
