# systemd 部署模板

这个目录只提供 unit 文件模板，不会自动改系统配置。

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

不传 `executor-001` 时，默认管理 `platform + source-sync + auto-settlement + alert + telegram-bot + telegram-report` 六个服务。

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

9. 启动日报排行榜推送：

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
