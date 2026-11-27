# 前端实现与验收

## 完成内容

- 当前规则状态与历史账务分开展示：等待触发/进行中保持原义，旧轮未确认不再显示“当前暂不可启动”。
- 规则运行卡片新增历史未决轮数、保留本金、待用于后续轮次的返还；路由余额明确为策略余额，显示本轮补充策略本金。
- “查看历史账务”为只读展开，真实 GET 分页使用 `{items,next_cursor}`；支持加载中、错误重试、空态、收起重开。下一页失败保留已经加载的历史。
- 历史记录区分待核对、待结算、已处理及未知状态；null 返还显示“尚未确定”，真实 0 显示 0.00，独立周期返还不冒充待注入新轮资金。
- 动态内容统一转义；认证切换后的迟到响应不插入页面；历史区没有重发旧投注或必须人工恢复的入口。
- 沿用原样式，未增加依赖或改变其他页面结构。记录页已有未确认任务不可重试的服务端/界面逻辑，本次不另加可发送操作。

## TDD 证据

RED：`node tests/test_cycle_recovery_ui.cjs` 首次以 `expired old run needs a separate history summary` 断言失败，证明旧轮摘要和恢复展示确实缺失，非导入/工具错误。

GREEN：

```text
node tests/test_cycle_recovery_ui.cjs --browser
PASS recovery DOM
PASS recovery Chromium desktop 1440 / mobile 390
PASS recovery real WSGI / SQLite

PC28_UI_EVIDENCE_DIR=.boss/cycle-recovery/evidence node tests/test_cycle_ui.cjs --browser
PASS UI forms
PASS streak UI
PASS manual UI
PASS real WSGI + SQLite create / edit / legacy save / resume
PASS Chromium desktop 1440 / mobile 390

node --check tests/test_cycle_recovery_ui.cjs
node --check src/pc28touzhu/ui/auto_triggers.js
python3 -m py_compile tests/cycle_recovery_ui_server.py
git diff --check
```

实际既有回归命令未设 evidence 环境变量，原测试生成的 4 张 cycle-isolation 截图已复制为本功能 `legacy-*` 截图并将原路径恢复为 HEAD 基线；以上列出的带环境变量命令用于后续复跑，避免覆盖旧功能产物。

## 真实链路验证

新 fixture 使用临时 SQLite 和 `RecoveryFixture` 真实开轮/归档方法创建 21 个未决旧轮，以及另一条规则的归档旧轮 + 新运行轮。未加载生产配置、未启动 worker、未发送消息。HTTP 仅允许页面所需 GET；浏览器拦截外部请求。

- 未认证历史接口返回 401；认证用户读取真实历史和倒序游标分页。
- 页面首次加载 20 条，点击后载入第 21 条，无重复/丢失。
- 新运行轮仍显示“进行中”，同卡片独立显示旧轮保留本金 10.00。
- 等待规则显示“等待触发”，保留未决历史，不出现人工处理阻塞提示。
- 桌面/手机无横向溢出、无 pageerror、无 POST/发送请求。
- fixture 退出时比较所有表完整行内容 SHA256，全部 GET/页面验证前后不变。
- DOM 另覆盖初始/下一页失败、malformed cursor、空态、未知枚举、null/0、legacy 缺字段、名称/期号注入文本及认证切换迟到响应。

覆盖率未单独采集；以上为真实行为断言与回归证据，不宣称整个历史页面代码达到某覆盖率。

## 写集

- 修改：`src/pc28touzhu/ui/auto_triggers.js`、`src/pc28touzhu/ui/auto_triggers.css`。
- 新增：`tests/test_cycle_recovery_ui.cjs`、`tests/cycle_recovery_ui_server.py`。
- 证据：`.boss/cycle-recovery/evidence/recovery-1440.png`、`recovery-390.png`；既有页面回归截图保留为 `legacy-*`。

## 状态

**STATUS**: DONE

**REASON**: 当前/历史隔离展示、只读分页和真实临时库桌面/手机回归均通过；未修改生产状态。
