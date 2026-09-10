# pc28touzhu

PC28 投注信号平台与 Telegram 执行器。

当前仓库已经不是“骨架”，而是一个可运行的最小闭环实现，覆盖：

- 平台侧来源、原始数据、标准信号、订阅、派发任务
- 用户侧登录会话与基础管理页面
- Telegram 执行器拉取任务、发送、回执、心跳
- 执行异常告警与 Telegram Bot 通知
- Telegram Bot 绑定、手动方案/自动触发分类收益查询、日报与月度排行榜推送
- `systemd` 部署脚本与基础单元测试

## 当前能力

- **平台 API**：WSGI 应用，入口为 `src/pc28touzhu/main.py`
- **Web 页面**：
  - 用户首页：`/`
  - 执行记录：`/records`
  - 异常提醒：`/alerts`
  - 自动投注配置：`/autobet`
  - 管理控制台：`/admin`
- **执行链路**：`signal -> subscription -> delivery_target -> execution_job -> executor report`
- **执行器模型**：支持多用户、多 Telegram 账号、多投递目标
- **运行模式**：
  - 本地最小验证：`./pc28 api`、`./pc28 fake`、`./pc28 seed`
  - 真实 Telegram 执行：`./pc28 executor executor-001`
  - 来源自动同步：`./pc28 source-sync`
  - PC28 自动结算：`./pc28 settlement`
  - 收益查询 Bot：`./pc28 bot`
  - 日报与月报排行榜推送：`./pc28 report`
  - `systemd` 托管：`./pc28 up executor-001`

## 目录结构

```text
src/pc28touzhu/     核心代码：API、配置、领域模型、服务、执行器、前端静态页
deploy/systemd/     systemd unit 与统一运维脚本
docs/               架构、领域模型、接口契约、产品方案
tests/              unittest 测试
data/               本地运行数据目录（不要提交 session）
pc28                统一命令入口
fake_executor.py    模拟执行器
telegram_executor.py 真实 Telegram 执行器
platform_alert_notifier.py 告警通知 worker
src/pc28touzhu/pc28_auto_settlement_worker.py PC28 自动结算 worker
telegram_profit_bot.py Telegram 收益查询 Bot worker
telegram_daily_reporter.py Telegram 日报推送 worker
seed_demo.py        演示数据注入脚本
```

## 环境要求

- Python `>=3.8`
- Linux + `systemd`（若使用托管部署）
- 可选依赖：`Telethon`（若使用真实 Telegram 执行器）

## 快速开始

### 1）安装依赖

```bash
python3 -m venv ".venv"
source ".venv/bin/activate"
pip install -U pip
pip install -e .
```

如果要运行真实 Telegram 执行器：

```bash
pip install -e ".[telegram]"
```

### 2）准备配置

```bash
cp ".env.example" ".env"
```

默认会从项目根目录 `.env` 读取配置，读取逻辑在 `src/pc28touzhu/config.py`。
对于 Telegram 告警、收益查询 Bot、日报推送，`.env` 现在只作为默认值；上线后更推荐在后台控制台 `/admin/telegram` 里维护，保存后对应 worker 会在下一轮自动热更新，无需重启。

最小必填项通常是：

```env
DATABASE_PATH=pc28touzhu.db
EXECUTOR_API_TOKEN=change-me
SESSION_SECRET=replace-me
```

如果要运行真实 Telegram 执行器，还需要：

```env
TELEGRAM_API_ID=123456
TELEGRAM_API_HASH=your_api_hash
TELEGRAM_SESSION=telegram-session
TELEGRAM_PHONE=+8613800000000
```

如果要运行收益查询 Bot 和日报推送，还需要：

```env
TG_BOT_ENABLED=true
TG_BOT_TOKEN=your_bot_token
TG_BOT_POLL_INTERVAL_SECONDS=30
TG_BOT_BIND_TOKEN_TTL_SECONDS=600

TG_REPORT_ENABLED=true
TG_REPORT_TARGET_CHAT_ID=-1001234567890
TG_REPORT_SEND_HOUR=9
TG_REPORT_SEND_MINUTE=0
TG_REPORT_TOP_N=10
TG_REPORT_TIMEZONE=Asia/Shanghai
```

收益查询 Bot 的统计口径分为两类：手动启动的跟单方案，以及自动触发规则。常用查询命令如下：

```text
/profit                         手动方案今日汇总，无数据时查昨日
/profit 昨天                    手动方案指定日期汇总
/profit 本月                    手动方案本月汇总（也支持“这个月”）
/profit 上个月                  手动方案上月汇总
/profit auto 上个月             自动触发上月汇总
/plan 上个月                   手动方案按方案汇总
/plan 方案名 2026-08            指定手动方案月度汇总
/plan PC28方案单双共识 上个月    单独查询该方案上月的手动投注盈亏
/plan #订阅ID 2026-08           用订阅 ID 精确查询，适用于同名方案
/rule                           自动触发规则昨日列表
/rule 规则名 2026-08             指定规则月度汇总
/rule 单双共识定时跟单 这个月    单独查询该规则本月的自动投注盈亏
/rule #规则ID 2026-08            用规则 ID 精确查询，适用于同名规则
/auto 2026-08                  自动触发月度总览
```

频道榜单包含用户的手动方案与自动触发总盈亏，并列出两类汇总；私聊查询按方案或规则分开。统计统一按北京时间的结算日期归属，跨日、跨月轮次不会全部计入开轮日。

报表使用独立的 `profit_report_daily_stats` 日汇总，按用户、订阅、规则和结算日归并金额与笔数，月报由日汇总计算。结算写入与报表更新处于同一数据库事务；首次升级启动时，只回填一次仍然存在的已结算明细，重复启动不会重复入账。旧版 worker 在升级期间写入的结算也由数据库触发器同步。

当前自动清理策略并非统一保留两个月：

| 数据 | 保留周期 |
| --- | --- |
| 跳过的自动触发事件 | 7 天 |
| 触发成功、失败事件 | 30 天 |
| 自动触发规则运行记录 | 90 天 |
| 自动触发规则风控日统计 | 365 天 |
| 报表日汇总 | 长期保留，不随上述清理删除 |

逐笔结算明细目前没有统一的 60 天自动清理策略。即使之后清理明细，已入账的报表日汇总也会保留；长期统计不需要永久保留每笔投注明细。

删除方案或规则会保留其已结算盈亏、原 ID 和删除时的名称，查询结果标注“已删除”，历史金额继续计入用户总盈亏与排行榜。重建同名配置会获得新 ID，查询遇到同名时会列出候选 ID；私聊始终限定在 Telegram 当前绑定用户的数据内。删除前需要先归档；方案有待执行或待结算投注、规则有运行中的轮次或待结算投注时，会要求先停止并完成结算。

升级前已经删除且没有备份的结算明细无法恢复；旧风控日统计可能混合手动与自动，或按开轮日记账，因此不会用它们冒充缺失的结算历史。

日报在设定时间发送前一天的盈利/亏损榜，各取前 10 名（可通过 `TG_REPORT_TOP_N` 调整）；每月 1 日同一时间额外发送上月月度榜。日报和月报分别幂等，重启 worker 不会重复发送。

## 常用命令

统一入口是仓库根目录脚本 `./pc28`：

```bash
./pc28 help
./pc28 api
./pc28 seed
./pc28 fake
./pc28 executor executor-001
./pc28 alert
./pc28 source-sync
./pc28 settlement
./pc28 bot
./pc28 report
./pc28 test
```

`systemd` 运维命令：

```bash
./pc28 up executor-001
./pc28 restart executor-001
./pc28 down executor-001
./pc28 status executor-001
./pc28 logs executor-001
./pc28 sync
```

说明：

- 不传 `executor_id` 时，默认管理 `platform + source-sync + auto-trigger + auto-settlement + alert + telegram-bot + telegram-report` 七个服务
- 传入 `executor_id` 时，会额外管理对应执行器实例
- `./pc28 executor executor-001` 会以常驻模式运行真实执行器

## 本地验证流程

### 平台 API

```bash
./pc28 api
```

默认监听：

- `http://127.0.0.1:35100`

可访问页面：

- `GET /`
- `GET /records`
- `GET /alerts`
- `GET /autobet`
- `GET /admin`

### 注入演示任务

```bash
./pc28 seed
```

该脚本会根据 `.env` 中的演示配置写入一条可执行任务，用于验证“平台派发 -> 执行器回报”闭环。

### 运行模拟执行器

```bash
./pc28 fake
```

模拟执行器不会连接 Telegram，只会拉取任务、打印内容并回报 `delivered`。

### 运行真实 Telegram 执行器

```bash
./pc28 executor executor-001
```

真实执行器入口是 `telegram_executor.py`，当前基于 `Telethon`。

支持两种会话模式：

- **首次手机号登录**：配置 `TELEGRAM_PHONE`，首次运行时登录并写入 `TELEGRAM_SESSION`
- **直接复用 session**：`TELEGRAM_SESSION` 指向已有可用 Telethon session

注意：

- `TELEGRAM_SESSION` 必须按账号隔离
- 不支持直接使用 Telegram Desktop 的 `tdata`
- `*.session` 文件不应提交到 Git

## 核心业务模型

当前主链路围绕以下实体组织：

- `users`：平台用户
- `sources`：来源配置
- `raw_items`：抓取到的原始载荷
- `signals`：标准化投注信号
- `subscriptions`：订阅规则
- `telegram_accounts`：用户 Telegram 账号
- `delivery_targets`：投递目标，必须绑定到某个 Telegram 账号
- `execution_jobs`：最终待执行任务
- `executor_instances`：执行器心跳与运行状态
- `platform_alerts`：平台告警与通知记录

这意味着当前系统已经切到“多用户 + 多账号 + 多目标”的执行模型，不再是单账号、单群的简单发消息脚本。

托管账号删除采用“归档后删除”策略：没有启用中群组和待执行任务即可删除；有历史执行记录时保留账号墓碑和历史显示，清除手机号、Session 等凭据，并解除停用/归档群组的账号绑定。

用户端与后台共用的账户弹窗现在由服务端在页面渲染阶段注入统一模板；`auth-panel.js`、`account-menu.js`、`auth-guard.js`、`ui-text.js` 这类共享资源也统一按文件更新时间附带版本参数，避免浏览器继续命中旧缓存。

## API 概览

### 认证接口

- `POST /api/auth/register`
- `POST /api/auth/login`
- `POST /api/auth/logout`
- `GET /api/auth/me`
- `POST /api/auth/change-password`

### 平台接口

按资源分组的主要前缀：

- `/api/platform/users`
- `/api/platform/sources`
- `/api/platform/raw-items`
- `/api/platform/signals`
- `/api/platform/subscriptions`
- `/api/platform/telegram-accounts`
- `/api/platform/delivery-targets`
- `/api/platform/message-templates`
- `/api/platform/execution-jobs`
- `/api/platform/execution-failures`
- `/api/platform/alerts`

普通平台接口默认只允许当前登录用户访问自己的资源；跨用户查询和平台运维能力已收口到管理员接口。

### 管理员接口

- `/api/platform/admin/support`
- `/api/platform/admin/telegram-settings`
- `/api/platform/admin/executors`
- `/api/platform/admin/alerts`

### 执行器接口

- `GET /api/executor/jobs/pull`
- `POST /api/executor/jobs/<job_id>/report`
- `POST /api/executor/heartbeat`

说明：

- 平台管理接口走登录会话
- 执行器接口走 `Bearer Token`

## 关键配置项

来自 `.env.example` 的常用配置分组如下。

### 平台

- `HOST`
- `PORT`
- `DATABASE_PATH`
- `EXECUTOR_API_TOKEN`
- `SESSION_SECRET`
- `EXECUTOR_STALE_AFTER_SECONDS`
- `EXECUTOR_OFFLINE_AFTER_SECONDS`
- `AUTO_RETRY_MAX_ATTEMPTS`
- `AUTO_RETRY_BASE_DELAY_SECONDS`
- `ALERT_FAILURE_STREAK_THRESHOLD`

### 执行器

- `PLATFORM_BASE_URL`
- `EXECUTOR_ID`
- `PULL_LIMIT`
- `ONCE`
- `TELEGRAM_API_ID`
- `TELEGRAM_API_HASH`
- `TELEGRAM_PHONE`
- `TELEGRAM_SESSION`

### 告警通知

- `ALERT_TELEGRAM_ENABLED`
- `ALERT_TELEGRAM_BOT_TOKEN`
- `ALERT_TELEGRAM_TARGET_CHAT_ID`
- `ALERT_NOTIFY_REPEAT_SECONDS`
- `ALERT_NOTIFIER_INTERVAL_SECONDS`
- `ALERT_NOTIFIER_ONCE`

### 演示数据

- `ISSUE_NO`
- `BET_TYPE`
- `BET_VALUE`
- `TARGET_KEY`
- `IDEMPOTENCY_KEY`
- `MESSAGE_TEXT`
- `STAKE_AMOUNT`

## 测试

运行全部单元测试：

```bash
./pc28 test
```

当前测试目录为 `tests/`，基于 `unittest discover`。

## 部署

`systemd` 模板与说明见：

- `deploy/systemd/README.md`

统一运维脚本实际转发到：

- `deploy/systemd/pc28ctl.sh`

## 相关文档

- `PROJECT_BRIEF.md`
- `docs/ARCHITECTURE.md`
- `docs/DOMAIN_MODEL.md`
- `docs/API_CONTRACT.md`
- `docs/SIGNAL_PROTOCOL.md`
- `docs/AITRADINGSIMULATOR_INTEGRATION.md`
- `docs/PRODUCT_UX_REDESIGN.md`

## 安全说明

以下文件默认不应提交：

- `.env`
- `*.db`
- `*.session`
- `.ace-tool/`
- `venv/`

如果仓库用于公开托管，先确认本地 `data/` 目录下不存在任何敏感 session、账号或运行时数据。
