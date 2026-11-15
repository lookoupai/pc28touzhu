# 信号正文自动归档

每天北京时间 04:30，由独立 systemd timer 归档并清空超过 90 天的正文。
保留全部记录、ID、期号、方向、关联关系；不删除账本，不执行 VACUUM。
信号的发布时间和入库时间必须都已超过 90 天，新补录的旧期信号仍保留 90 天。

归档文件保留在 `backups/payload_archive/`：

- 原有 `*.csv.gz` 原样保留，新工具不会覆盖或用它们授权清理。
- `pending/*.jsonl.gz` 是已持久化、尚待应用的批次；失败或中断后自动重试。
- `complete/*.jsonl.gz` 是已处理批次，持续保留，不自动删除。

每个新归档含格式版本、数据库路径及文件标识、截止时间、完整原始记录、行数和 SHA256。
读取到文件末尾并通过 gzip 完整性、SHA256、字段及数据库归属校验后才开始写事务。
事务内逐条核对所有字段及当前业务状态，只清空仍然相同、仍可清理的记录。
新补入的数据不会被其他批次清空；内容变化、未解析、未结算、待执行、失败待重试和运行中轮次引用的数据都会保留。

每批最多 500 行（定时服务使用 200 行），批次间让出写锁。
同一数据库使用文件锁防止维护任务并发。每次每表最多新增归档 50,000 行，积压在后续运行继续处理。
首次运行建立部分索引，后续扫描排除已清空的正文；单个索引创建超过 10 秒会中止并报错。
出错时服务每 5 分钟重试，一小时内最多尝试 3 次；查看 journal 定位问题，修复后可手动重试。

## 安装与查看

以默认项目路径和数据库路径为例：

```bash
sudo install -d -o www -g www -m 2770 /www/wwwroot/pc28touzhu/backups/payload_archive
# 不截断已有锁文件；让手动运行和 www 服务共用同一把锁。
sudo touch /www/wwwroot/pc28touzhu/pc28touzhu.db.payload-maintenance.lock
sudo chown www:www /www/wwwroot/pc28touzhu/pc28touzhu.db.payload-maintenance.lock
sudo chmod 660 /www/wwwroot/pc28touzhu/pc28touzhu.db.payload-maintenance.lock
sudo cp deploy/systemd/pc28touzhu-payload-maintenance.service deploy/systemd/pc28touzhu-payload-maintenance.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now pc28touzhu-payload-maintenance.timer
systemctl list-timers pc28touzhu-payload-maintenance.timer
journalctl -u pc28touzhu-payload-maintenance.service -n 30 --no-pager
```

定时器支持关机错过后的补跑，不依赖业务 worker，也不重启业务服务。
若修改项目或数据库路径，需要同步修改 unit 的工作目录、读写目录、命令参数及锁文件权限。
`./pc28` 的业务服务管理独立于此维护定时器，停止维护使用：

```bash
sudo systemctl disable --now pc28touzhu-payload-maintenance.timer
sudo systemctl stop pc28touzhu-payload-maintenance.service
```

## 手动维护

```bash
# 只读预览（默认每表最多 50,000 行）
python3 scripts/maintenance/archive_signal_payloads.py
# 正常增量执行；也会先恢复待应用批次
python3 scripts/maintenance/archive_signal_payloads.py --archive --apply
# 只归档；之后仅处理这些待应用批次
python3 scripts/maintenance/archive_signal_payloads.py --archive
python3 scripts/maintenance/archive_signal_payloads.py --apply
# 查看服务失败后，修复原因并重试
sudo systemctl reset-failed pc28touzhu-payload-maintenance.service
sudo systemctl start pc28touzhu-payload-maintenance.service
```

`--force` 已移除。无新格式 pending 归档时，`--apply` 不会清空任何正文。
手动运行请使用 root 或 www，归档目录的组继承权限允许二者互相恢复未完成批次。
数据库文件被替换或恢复后，旧 pending 的文件标识将不匹配：不要强行应用；先核对保留这些归档，再为当前数据库重新归档。
中途断电可能留下 `.partial-*` 文件，它们从未授权数据库清理；确认没有维护任务运行后可单独移走检查。

新归档为 gzip JSON Lines，第一行是批次信息，最后一行是行数及内容校验值，中间每行是包含原始正文的完整记录。
旧记录需要恢复时，可以从归档按表名和 ID 找回正文；应先核对当前记录，避免覆盖随后产生的新内容。
归档只是被修改字段的恢复来源，不等价于全库备份。

清空正文主要减缓数据库增长。记录、索引和压缩归档仍会增加，文件大小没有固定上限。
`--vacuum` 只能单独执行，应安排业务写入暂停的维护窗口；日常任务不会压缩数据库。
