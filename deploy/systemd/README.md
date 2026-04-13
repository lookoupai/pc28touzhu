# systemd 部署模板

这个目录只提供 unit 文件模板，不会自动改系统配置。

## 简化入口

推荐直接用仓库内脚本统一管理：

```bash
./pc28 up executor-001
```

`./pc28` 是仓库根目录的统一入口；它会自动转发到 `deploy/systemd/pc28ctl.sh`。

常用命令：

```bash
./pc28 up executor-001
./pc28 restart executor-001
./pc28 down executor-001
./pc28 status executor-001
./pc28 logs executor-001
```

不传 `executor-001` 时，默认只管理 `platform + alert` 两个服务。

## 手动安装

1. 复制 unit 文件到 systemd：

```bash
sudo cp /www/wwwroot/pc28touzhu/deploy/systemd/*.service /etc/systemd/system/
sudo systemctl daemon-reload
```

2. 确保 `/www/wwwroot/pc28touzhu/.env` 已配置（至少包含 `DATABASE_PATH`、`EXECUTOR_API_TOKEN`、Bot 告警相关配置等）。

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

## 观察与排障

查看日志：

```bash
journalctl -u pc28touzhu-platform.service -f
journalctl -u pc28touzhu-telegram-executor@executor-001.service -f
journalctl -u pc28touzhu-alert-notifier.service -f
```

常见问题：

- Bot 报 `chat not found`：用 Bot `getUpdates` 确认 `chat.id`，并确保 Bot 已加入目标群且可发言。
- SQLite 只读：确认 `DATABASE_PATH` 文件可写，以及其所在目录可写（SQLite 需要写入 journal 文件）。
