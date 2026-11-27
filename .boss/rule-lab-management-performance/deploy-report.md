# 实验管理、性能与空间回收发布记录

## 摘要

- 当前为发布准备阶段，**尚未部署**。最终QA和根代理明确放行后才执行线上变更。
- 复用现有本机systemd部署，不新增平台、依赖或资源额度。
- 线上实验继续运行；发布计划短暂停止批准队列、实验worker、API三个写者，保留原状态、进度、pending及全部历史成绩。
- 不删除真实实验，不开启新的实验/续算计划，不改上游预测库或真实业务账本。

## 只读预检

2026-11-27，生产main为`20cb785`且工作树干净。schema4、auto_vacuum=NONE、WAL，602个实验版本、10个批次、2个连续系列。批次9/10已自然完成，12/16继续运行；发布前会重新采集实际状态，不使用此历史采样作为固定前提。

实验worker保持1核/256MiB，批准队列保持5%CPU/128MiB；来源同步、自动触发、自动结算和实际执行器均active。上游预测进程PID1250128保持。预检时实验库约348MiB，动态增长；转换CLI只读预估约713MiB额外可用空间，文件系统可用约101GiB，实际发布时重新检查。

| 检查 | 命令 | 退出码 | 结果 |
|---|---|---:|---|
| 动态状态/服务/空间 | `python3 /tmp/pc28-management-rollout.py` | 0 | `/tmp/pc28-management-release-preflight.json` |
| 转换默认预览 | `env PYTHONPATH=src python3 scripts/maintenance/prepare_rule_lab_storage.py --database /www/wwwroot/pc28touzhu/data/rule_lab/rule_lab.db` | 0 | NONE状态和所需/可用字节；无apply |
| 旧版线上GET基线 | `node /tmp/pc28-management-online.cjs --baseline` | 0 | 602版本、10批次，0POST；`/tmp/pc28-management-http-before.json` |
| 脚本语法 | `python3 -m py_compile /tmp/pc28-management-rollout.py`、`node --check /tmp/pc28-management-online.cjs` | 0 | 通过 |

认证信息仅在进程内存中使用，证据不写Cookie、密钥或密码。所有临时发布脚本/证据权限0600。

## 待执行的受控发布

根代理完成最终测试并提供已验收commit后执行：

```bash
python3 /tmp/pc28-management-rollout.py --target <已验收commit> --apply
node /tmp/pc28-management-online.cjs
```

脚本依序停止queue→lab worker→API，记录此前服务启停状态；备份独立实验库和批准队列清单到预算目录之外。使用绝对路径`/usr/sbin/runuser`以www账户迁移schema5，并调用实际维护CLI转换到INCREMENTAL。转换另留一份schema5整理前备份。全部备份保留，不能把实验主文件缩小量声称为服务器净磁盘释放量。

转换后以流式、顺序无关的多重哈希核对每张旧表的全部旧列和行数，核对quick_check、FK、WAL及auto_vacuum=2；原batch/experiment IDs、状态、游标、processed/pending和队列文件保持。迁移不得产生管理任务。只恢复此前active的三个服务；已停止或自然完成的队列不被盲目重新启动。实际执行器可能因API短时不可达自动重连，健康检查等待稳定恢复再判断；其他业务服务不重启，上游PID保持。

线上检查严格GET：查看storage分项和incremental_ready、完成/运行批次的管理和删除预览、尚未启动批次的可编辑状态，以及桌面1440/手机390的管理表单和存储面板。不会提交改名、编辑、新建、删除或回收请求。恢复后核对原ID仍存在、进度不倒退、旧批次8结果摘要保持及执行器心跳。

## 恢复边界

schema5已写入后不能直接切回只识别v4的旧代码。出错时保留备份和当前库，恢复此前active服务，记录失败阶段；不以旧库覆盖恢复后已经产生的新成绩。空间模式转换未完成时明确报告未就绪，不能把新增页面当作回收已经成功。

## 正式发布结果

待最终放行后补充实际commit、备份、停写耗时、原行数核对、资源、HTTP及桌面/手机结果。
