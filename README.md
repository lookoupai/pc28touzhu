# pc28touzhu

PC28 投注信号平台与 Telegram 执行器。

当前仓库已经不是“骨架”，而是一个可运行的最小闭环实现，覆盖：

- 平台侧来源、原始数据、标准信号、订阅、派发任务
- 用户侧登录会话与基础管理页面
- Telegram 执行器拉取任务、发送、回执、心跳
- 执行异常告警与 Telegram Bot 通知
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

## 常用命令

统一入口是仓库根目录脚本 `./pc28`：

```bash
./pc28 help
./pc28 api
./pc28 seed
./pc28 fake
./pc28 executor executor-001
./pc28 alert
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

- 不传 `executor_id` 时，仅管理 `platform + alert`
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

## API 概览

### 认证接口

- `POST /api/auth/register`
- `POST /api/auth/login`
- `POST /api/auth/logout`
- `GET /api/auth/me`

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
- `/api/platform/executors`
- `/api/platform/execution-failures`
- `/api/platform/alerts`
- `/api/platform/support`

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
