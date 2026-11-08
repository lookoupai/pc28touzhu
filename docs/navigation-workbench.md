# 统一导航与工作台

原首页与总控台合并为 `/` 工作台。全站共用固定侧栏；960px 以下使用可键盘操作的抽屉导航。配置向导在未完成时展开，已配置用户看到运行摘要、待处理事项、策略链路和最近记录。

## 页面职责

| 分组 | 页面 |
| --- | --- |
| 工作台 | `/` |
| 策略管理 | `/autobet/subscriptions`、`/auto-triggers` |
| 资源配置 | `/autobet/sources`、`/autobet/accounts`、`/autobet/targets`、`/autobet/templates`、`/autobet/bot` |
| 运行监控 | `/records`、`/alerts` |
| 平台管理 | `/admin` 及现有分区，只有管理员展示入口，后端继续鉴权 |

来源导入统一在来源页，Bot 绑定迁到独立页。工作台的批量启停放在折叠区，操作前说明影响数量及范围，部分失败逐项反馈；沿用原有账号/群组/跟单状态 API，不改变自动触发、派单或结算规则。

## 维护入口

- `api/ui_navigation.py`：页面注册表、导航分组、权限标签、旧片段映射。
- `api/app.py`：共享框架注入、路径规范化、模板选择与静态资源版本。
- `ui/app_shell.html/css/js`：共享导航、桌面/手机布局、角色展示、离开未保存表单提示。
- `ui/home.html/js` 与 `workbench.css`：工作台。最近任务统计明确限于最近 20 条，告警限于最近 6 条，不冒充当日或全量统计。
- `ui/autobet.html`：配置页外壳；正文按路径注入 `workspace_sources/accounts/targets/templates/subscriptions.html`。配置逻辑暂共用 `autobet.js`，只为实际存在的表单初始化；已删除原总控台的概览、向导和 Bot 逻辑。
- `ui/bot_binding.html/js`：独立 Bot 绑定。

## 地址与状态

`/autobet` 及尾斜杠路径由服务器规范化并保留查询参数。URL fragment 不会发送给服务器，因此旧锚点由共享脚本映射，保留查询条件。例如 `/autobet#botBindingSection` 到 `/autobet/bot`，`/#importSection` 到 `/autobet/sources#sourceForm`，`/admin#executionJobsSection` 到 `/admin/execution`。

全局高亮只按页面路径决定；区块锚点不再参与菜单高亮。记录、告警和后台筛选保存在 URL 中；方案的 `subscription_id`、规则的 `rule_id` 与 `stat_date` 可刷新、收藏和返回。跟单方案可跳到关联规则，规则可返回关联方案。

配置表单使用 `data-track-changes`。离开页面提示未保存修改；规则列表刷新保留编辑草稿，保存成功或显式重置后清除修改标记。账户及筛选表单不参与草稿判断。未保存内容不持久化到浏览器存储。

后台访问受限页保留同一导航和账户弹窗，管理员登录后回到原后台地址；普通用户仍由服务端拒绝。

## 验证与发布

见 `tests/README.md` 的导航回归命令。改动需要整体发布 Python 页面路由与 UI 文件：旧后端不认识新增资源和共享占位符，不应单独覆盖前端。部署时重启 Web API 进程以加载路由代码；不需要数据库迁移或重启投注 worker。

此次采用多页面导航；切换页面仍进行正常文档加载。配置脚本可在后续按业务继续拆分，不需要引入前端框架。
