# pc28touzhu

基于“预测信号平台 + Telegram 执行体系”思路搭建的新项目。

当前仓库目标不是直接复刻旧项目，而是围绕以下核心能力重新设计：

- 多来源预测方案接入
- 标准化投注信号建模
- 用户订阅与策略编排
- Telegram 自动发言执行
- 托管版与自托管版并存

## 当前状态

当前仓库还处于骨架阶段，已明确：

- 旧仓库 `AITradingSimulator` 不再继续承载该新业务主线
- 平台核心优先自研
- `Telegram-Panel` 仅作为后续可选执行适配器，不作为平台核心
- 第三方项目接入前必须确认许可证与商用授权

详细背景见 [PROJECT_BRIEF.md](/www/wwwroot/pc28touzhu/PROJECT_BRIEF.md)。

产品与体验改造方案见 [PRODUCT_UX_REDESIGN.md](/www/wwwroot/pc28touzhu/docs/PRODUCT_UX_REDESIGN.md)。

## 目录约定

```text
docs/         架构、领域模型、接口契约文档
platform/     平台核心：来源、标准化、订阅、任务编排
executor/     执行端：Telegram 账号会话、投递、回执
adapters/     第三方来源适配、第三方执行端适配
migrations/   数据库迁移
tests/        自动化测试
```

## 当前优先级

第一阶段只做最小闭环：

1. 接入一个来源
2. 生成标准信号
3. 将信号投递给一个 Telegram 群
4. 回写执行结果

协议与集成文档：

- [SIGNAL_PROTOCOL.md](/www/wwwroot/pc28touzhu/docs/SIGNAL_PROTOCOL.md)
- [AITRADINGSIMULATOR_INTEGRATION.md](/www/wwwroot/pc28touzhu/docs/AITRADINGSIMULATOR_INTEGRATION.md)
- [AITRADINGSIMULATOR_SOURCE_TEMPLATE.json](/www/wwwroot/pc28touzhu/docs/AITRADINGSIMULATOR_SOURCE_TEMPLATE.json)

## 推荐下一步

- 先完成核心表设计
- 再定义平台对执行器的 API 契约
- 最后开始实现最小执行链路

## 快速验证

最简单的统一入口：

- `./pc28 up executor-001`
- `./pc28 down executor-001`
- `./pc28 status executor-001`
- `./pc28 logs executor-001`
- `./pc28 api`
- `./pc28 fake`
- `./pc28 test`

停止 `systemd` 服务时：

- `./pc28 down` 只停止 `platform + alert`
- `./pc28 down executor-001` 会连同该执行器一起停止

统一配置入口：

- 项目根目录的 `.env`
- 示例见 [`.env.example`](/www/wwwroot/pc28touzhu/.env.example)
- 读取逻辑统一在 [config.py](/www/wwwroot/pc28touzhu/src/pc28touzhu/config.py)

运行单元测试：

`./pc28 test`

启动最小平台 API（WSGI）：

`./pc28 api`

打开用户端首页：

`GET /`

打开用户端执行记录页：

`GET /records`

打开用户端异常提醒页：

`GET /alerts`

打开用户端自动投注配置页：

`GET /autobet`

说明：

- 用户端首页现在提供 `AITradingSimulator` 公开方案页链接或导出链接导入入口
- 高级来源配置、抓取、标准化与排障仍在管理控制台进行
- 用户端 `自动投注配置` 页现在支持最小启停：可暂停/恢复托管账号、投递群组和跟单策略
- 用户端 `自动投注配置` 页现在支持全局暂停/恢复，会批量切换现有账号、投递群组和跟单策略状态

打开管理控制台：

`GET /admin`

写入一条演示任务（用于测试拉取/回报闭环）：

`./pc28 seed`

运行一个模拟执行器（不接 Telegram，仅打印并回报 delivered）：

`./pc28 fake`

运行真实 Telegram 执行器：

`./pc28 executor executor-001`

运行告警 Telegram 通知 worker：

`./pc28 alert`

## 当前 API

认证接口：

- `POST /api/auth/register`
- `POST /api/auth/login`
- `POST /api/auth/logout`
- `GET /api/auth/me`

平台管理接口：

- `GET /admin`
- `GET /api/platform/users`
- `GET /api/platform/sources`
- `POST /api/platform/sources`
- `GET /api/platform/telegram-accounts`
- `POST /api/platform/telegram-accounts`
- `POST /api/platform/sources/<source_id>/fetch`
- `GET /api/platform/raw-items`
- `POST /api/platform/raw-items`
- `POST /api/platform/raw-items/<raw_item_id>/normalize`
- `GET /api/platform/signals`
- `POST /api/platform/signals`
- `POST /api/platform/signals/<signal_id>/dispatch`
- `GET /api/platform/subscriptions`
- `POST /api/platform/subscriptions`
- `GET /api/platform/delivery-targets`
- `POST /api/platform/delivery-targets`
- `GET /api/platform/execution-jobs`
- `POST /api/platform/execution-jobs/<job_id>/retry`
- `GET /api/platform/executors`
- `GET /api/platform/execution-failures`
- `GET /api/platform/alerts`

执行器接口：

- `GET /api/executor/jobs/pull`
- `POST /api/executor/jobs/<job_id>/report`
- `POST /api/executor/heartbeat`

说明：

- 平台管理接口现在走登录会话
- 执行器接口仍然走 Bearer Token

历史兼容：

- 如果某个旧用户是在认证系统上线前创建、没有密码
- 可以用同用户名再次走一次注册
- 系统会自动补全密码并完成登录

## 最小来源抓取配置

当前支持 `source_type=http_json` 的最小抓取模式，来源配置 JSON 可写成：

```json
{
  "fetch": {
    "url": "https://example.com/feed.json",
    "issue_no_path": "data.issue_no",
    "external_item_id_path": "data.id",
    "published_at_path": "meta.published_at"
  }
}
```

抓取后会自动创建一条 `raw item`，再由 `POST /api/platform/raw-items/<raw_item_id>/normalize` 转成标准信号。

现在也支持 `source_type=ai_trading_simulator_export`，用于直接对接 `AITradingSimulator` 的导出接口。

## Telegram 执行器

真实 Telegram 执行器当前基于 `Telethon` 最小用户账号链路：

- 安装依赖：`pip install '.[telegram]'`
- 必填配置：`TELEGRAM_API_ID`、`TELEGRAM_API_HASH`、`TELEGRAM_SESSION`
- `TELEGRAM_PHONE` 只在“首次手机号登录”模式必填
- 会话文件：`TELEGRAM_SESSION`
- 启动入口：`telegram_executor.py`

当前支持两种模式：

- 首次手机号登录：提供 `TELEGRAM_PHONE`，首次运行会触发 Telethon 登录，并把会话保存到 `TELEGRAM_SESSION`
- 直接复用现成 session：如果 `TELEGRAM_SESSION` 已经指向一个有效 Telethon session，会直接复用，不要求 `TELEGRAM_PHONE`

注意：

- 现成 session 需要是 Telethon 可直接使用的 session，不是 Telegram Desktop 的 `tdata`
- 每个账号必须使用独立的 `TELEGRAM_SESSION`

## 多用户执行模型

当前派发链路已经切到多用户模型：

- 每个用户可拥有多条 `telegram_accounts`
- 每个 `delivery_target` 必须绑定到某个 `telegram_account`
- 派发 `signal` 时，会按“用户订阅 + 用户目标 + 目标绑定账号”生成 `execution_jobs`
- 也就是说，任务不再只是“发到哪个群”，而是“由哪个用户的哪个 Telegram 账号发到哪个群”

当前执行器也已经切到多账号模式：

- `pull_jobs` 下发的任务会携带 `telegram_account`
- `telegram_executor.py` 会按任务中的 `telegram_account.session_path` / `telegram_account.phone` 选择对应会话
- `.env` 中的 `TELEGRAM_PHONE` / `TELEGRAM_SESSION` 现在是默认兜底，不再是唯一账号配置

最近补齐的运维闭环：

- Dashboard 已支持查看当前用户的 `execution_jobs`
- 可按 `status` / `signal_id` 筛选任务
- 发送异常会回写 `failed`
- 已过期未执行任务会自动转为 `expired`
- `failed` / `expired` / `skipped` 任务可手动重试，重置为 `pending`
- Dashboard 也支持查看 `executor_instances` 心跳与最近失败任务
- 可以直接看到执行器在线状态、版本、累计投递/失败次数和最近失败原因
- `pull_jobs` 前会自动扫一轮失败任务，并按指数退避重排到 `pending`
- Dashboard 新增告警面板，聚合执行器 stale/offline、连续失败过多、自动重试耗尽任务
- 告警通知可通过 `platform_alert_notifier.py` 用 Telegram Bot 发到管理群，并带发送去重/重复提醒/恢复通知状态

## 运维阈值

可通过 `.env` 调整以下平台策略：

- `EXECUTOR_STALE_AFTER_SECONDS`
- `EXECUTOR_OFFLINE_AFTER_SECONDS`
- `AUTO_RETRY_MAX_ATTEMPTS`
- `AUTO_RETRY_BASE_DELAY_SECONDS`
- `ALERT_FAILURE_STREAK_THRESHOLD`

告警通知 worker 相关配置：

- `ALERT_TELEGRAM_ENABLED`
- `ALERT_TELEGRAM_BOT_TOKEN`
- `ALERT_TELEGRAM_TARGET_CHAT_ID`
- `ALERT_NOTIFY_REPEAT_SECONDS`
- `ALERT_NOTIFIER_INTERVAL_SECONDS`
- `ALERT_NOTIFIER_ONCE`
