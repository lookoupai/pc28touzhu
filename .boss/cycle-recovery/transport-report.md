# 执行器修复验证

## W1：发送阶段分类

- RED：`PYTHONPATH=src python3 -m unittest tests.test_executor_recovery.TransportClassificationTests -v`，3 个用例/4 个失败分支。连接 EOFError、IncompleteReadError 和目标准备 EOF 均被误记未知；发送 RPC 后 EOF 缺明确阶段证据。
- GREEN：`PYTHONPATH=src python3 -m unittest tests.test_executor_recovery.TransportClassificationTests tests.test_executor_send_window tests.test_executor_runtime tests.test_execution_acceptance -q`，35 项通过，4.086 秒。
- 改动：仅 `executor/telethon_sender.py`。发送前连接/实体解析 EOF 归类为已知未发送的可重试故障；RPC 开始后同类异常仍未知，附发送起始时间。实体解析不再吞掉网络 EOF 后误报目标无效。
- 测试断言真实 runtime 回报与发送调用次数，所有传输为离线模拟；无生产状态或真实消息变更。
- `git diff --check` 通过。
- W1 已经父代理门禁放行；以下为后续独立 transport Wave。

## W2-transport：持久化与只读后台核对

- 首批 RED：8 个测试因缺少提前持久化、核对入口和异步隔离失败；随后 GREEN。
- 边界 RED：总读取预算取消、前台优先、原 session 不匹配 3 个测试失败；随后 GREEN。
- 追加核对边界：原 attempt 身份、可信机器人/期号、历史不透明轮号缺证据、远端消息缺失、账号锁忙均覆盖。
- 最终 GREEN：`PYTHONPATH=src python3 -m unittest tests.test_executor_recovery tests.test_executor_send_window tests.test_executor_runtime tests.test_execution_acceptance tests.test_cycle_recovery_api -q`，**65 项通过，7.923 秒**。其中 transport 新文件25项（分类3、核对21、真实API整链1项含legacy/strict两子场景）。执行器 compileall 和 diff check 通过。
- RPC 返回后立即把 message ID、原任务身份和原期开盘证据持久化，再等待受理。受理阶段崩溃重启只回报未知并保留证据，不重发。
- 核对在闲置循环启动单个后台线程、单个在途任务；慢查询不阻塞后续发送主循环。前台账号请求优先，账号锁忙立即跳过；总远端读取预算 5 秒，包含跨进程 session 锁和连接/身份读取/消息读取，清理最多另用 1 秒。后台失败只记录异常类，不泄露凭证。
- 优先使用匹配原 job/attempt/账号/目标/协议的持久化证据；否则只按已知 message ID 精确读取原消息及有界历史回复。无 ID、未找到、期号/金额/机器人不符均维持 unknown，绝不推断未发或调用发送。
- 严格协议同时保留原 `acceptance_reply` 的发送者、回复关联、时间与文本，服务器可独立核验；旧本地 accepted 缺该证据必须重读。隐式默认账号没有冻结 session 路径时不查询，不能拿修改后的默认账号代查历史。
- Tech Lead 发现的 P1 已按 RED/GREEN 修复：新读取的历史不透明轮号回复不能借旧 opening 快照认定原期，直接维持 unknown；本地原 accepted 也必须有可信原回复和 acceptance_checked_at，并满足发送/回执/检查时间界限。证据显式区分 local_result 与 remote_history。
- 接口客户端遵循冻结 DTO：`GET /api/executor/reconciliations/pull?limit=1`，`POST /api/executor/jobs/{id}/reconcile`。后台核对租约与发送领取标识分离。
- 真实 WSGI/SQLite 整链已完成：先建立未确认旧轮并摘离归档，执行器原尝试持久化成功证据，经真实 ExecutorApiClient 路由向平台领取核对及回报，原两腿进入 placed，session 保持 waiting，未发送消息。legacy/严格受理两子场景均通过；原尝试证据不覆盖。
- 身份边界：本地证据必须匹配原 job、原 executor/attempt 及冻结任务身份；新远端查询核对原账号session，账号空session仅在原证据证明fallback路径一致时允许，修改后的默认session不得代查。

**STATUS**: DONE
**REASON**: transport 分类、提前持久化、只读核对、时间与身份边界及真实 WSGI 整链均通过；未提交、未触碰生产。
