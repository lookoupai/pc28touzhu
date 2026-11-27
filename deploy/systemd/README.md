# systemd 部署模板

这个目录只提供 unit 文件模板，不会自动改系统配置。

正文归档使用独立的 `pc28touzhu-payload-maintenance.timer`，每天北京时间 04:30 执行。安装权限、重试和恢复说明见 [正文归档维护](../../scripts/maintenance/README.md)；仅复制 `.service` 文件不会启用此定时器。

## 简化入口

推荐直接用仓库内脚本统一管理：

```bash
./pc28 up executor-001
```

`./pc28` 是仓库根目录的统一入口；它会自动转发到 `deploy/systemd/pc28ctl.sh`。
同时会优先使用项目根目录下的 `.venv/bin/python` 或 `venv/bin/python`；如果都不存在，才回退到系统 `python3`。

常用命令：

```bash
./pc28 up executor-001
./pc28 restart executor-001
./pc28 down executor-001
./pc28 status executor-001
./pc28 logs executor-001
./pc28 settlement
```

不传 `executor-001` 时，默认管理 `platform + source-sync + auto-trigger + auto-settlement + alert + telegram-bot + telegram-report` 七个服务。

## 手动安装

1. 复制 unit 文件到 systemd：

```bash
sudo cp /www/wwwroot/pc28touzhu/deploy/systemd/*.service /etc/systemd/system/
sudo systemctl daemon-reload
```

2. 确保 `/www/wwwroot/pc28touzhu/.env` 已配置（至少包含 `DATABASE_PATH`、`EXECUTOR_API_TOKEN` 等基础项；Telegram 相关项也可以先放 `.env` 里作为默认值，后续再通过 `/admin/telegram` 网页配置覆盖并热更新）。

如果你把 Telegram 相关依赖装在虚拟环境，建议优先创建项目内 `.venv`：

```bash
cd /www/wwwroot/pc28touzhu
python3 -m venv ".venv"
source ".venv/bin/activate"
pip install -e ".[telegram]"
```

3. 启动平台：

```bash
sudo systemctl enable --now pc28touzhu-platform.service
```

4. 启动一个执行器实例（`%i` 会映射到环境变量 `EXECUTOR_ID`）：

```bash
sudo systemctl enable --now pc28touzhu-telegram-executor@executor-001.service
```

`pc28touzhu-telegram-executor@.service` 会在 `ExecStart` 中强制注入 `EXECUTOR_ID=%i` 和 `ONCE=false`，避免被 `.env` 里的同名配置覆盖后退化成单次执行。

5. 启动告警 notifier（Bot）：

```bash
sudo systemctl enable --now pc28touzhu-alert-notifier.service
```

`pc28touzhu-alert-notifier.service` 会在 `ExecStart` 中强制注入 `ALERT_NOTIFIER_ONCE=false`，避免被 `.env` 里的同名配置覆盖后退化成单次执行。

6. 启动来源自动同步 worker：

```bash
sudo systemctl enable --now pc28touzhu-source-sync.service
```

`pc28touzhu-source-sync.service` 会在 `ExecStart` 中强制注入 `SOURCE_SYNC_ONCE=false`。默认会自动扫描“已被激活跟单使用的来源”，执行 `fetch -> normalize -> dispatch`。

7. 启动 PC28 自动结算 worker：

```bash
sudo systemctl enable --now pc28touzhu-pc28-auto-settlement.service
```

`pc28touzhu-pc28-auto-settlement.service` 会在 `ExecStart` 中强制注入 `PC28_AUTO_SETTLEMENT_ONCE=false`。只要后台配置启用了自动结算，它就会持续拉取最近开奖并把 `placed` 状态的记录结算为 `hit/refund/miss`。

8. 启动收益查询 Bot：

```bash
sudo systemctl enable --now pc28touzhu-telegram-bot.service
```

`pc28touzhu-telegram-bot.service` 会在 `ExecStart` 中强制注入 `TG_BOT_ONCE=false`。

9. 启动日报与月度排行榜推送（每月 1 日自动补发上月月榜）：

```bash
sudo systemctl enable --now pc28touzhu-telegram-report.service
```

`pc28touzhu-telegram-report.service` 会在 `ExecStart` 中强制注入 `TG_REPORT_ONCE=false`。

## 观察与排障

查看日志：

```bash
journalctl -u pc28touzhu-platform.service -f
journalctl -u pc28touzhu-source-sync.service -f
journalctl -u pc28touzhu-pc28-auto-settlement.service -f
journalctl -u pc28touzhu-telegram-executor@executor-001.service -f
journalctl -u pc28touzhu-alert-notifier.service -f
journalctl -u pc28touzhu-telegram-bot.service -f
journalctl -u pc28touzhu-telegram-report.service -f
```

常见问题：

- Bot 报 `chat not found`：用 Bot `getUpdates` 确认 `chat.id`，并确保 Bot 已加入目标群且可发言。
- SQLite 只读：确认 `DATABASE_PATH` 文件可写，以及其所在目录可写（SQLite 需要写入 journal 文件）。
# 规则实验室速度切换

在基础worker单元安装完成后，单独安装固定资源helper（不能从www可写的项目目录直接以sudo执行）。只允许将`pc28touzhu-rule-lab.service`的CPUQuota设为20%或100%，不授权任意systemctl或其他参数。

```bash
sudo install -d -o root -g root -m 0755 /usr/local/libexec
sudo install -o root -g root -m 0755 deploy/systemd/pc28-rule-lab-resource-mode /usr/local/libexec/pc28-rule-lab-resource-mode
sudo visudo -cf deploy/systemd/pc28-rule-lab-resource.sudoers
sudo install -o root -g root -m 0440 deploy/systemd/pc28-rule-lab-resource.sudoers /etc/sudoers.d/pc28-rule-lab-resource
sudo visudo -cf /etc/sudoers.d/pc28-rule-lab-resource
```

发布后管理员网页GET/POST `/api/platform/rule-lab/performance`读取/修改全服务速度，POST仅接受`{"mode":"economy"}`或`{"mode":"fast"}`。helper固定unit/参数，清空调用环境、串行锁、超时；www无权更改helper及父目录。状态同时检查systemd与cgroup v2的实际CPU限额，内存硬上限必须保持256MiB。非本部署方式或缺helper时显示不可切换，不伪造成功。

`systemctl set-property`实时生效且默认持久化到systemd控制drop-in，不需要重启worker。基准unit继续CPUQuota20%；加速100%是1核上限。切换后核对MainPID不变、CPUQuotaPerSecUSec（200ms或1s）与`cpu.max`（quota/period为0.2或1），MemoryMax、禁swap及安全属性不变。空闲时仍保持低实际CPU使用。默认发布验证后回到省资源。

卸载控制时先用helper切回economy，再删除此sudoers文件和helper。持久化CPU限额在`/etc/systemd/system.control/pc28touzhu-rule-lab.service.d/`，不要用`systemctl revert`清除其他管理员配置。
