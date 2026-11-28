# 自助实验室任务与写集

## 摘要

- 已批准输入：design.md、HTML原型、用户“继续推进修改”；实现以plan.md §2 wire v1为前后端唯一契约。
- Backend独占Python共享文件及新增API/表/队列；Frontend独占HTML/CSS/JS与新UI fixture。两个集成owner，无并发同文件写入；root已批准Backend将纯条件编译模块与其专属测试委托一位子代理（DOMAIN）。
- Blast Radius：高；新schema、全局队列状态、≥10个文件，均属于用户已批准设计范围。依赖安装、认证改动、真实跟单修改、实验结果删除均不在范围内。
- W1～W4依次验收，代码owner可并行准备互不重叠文件；不合并不通过的Wave。不另请求已授权事项确认。

## 1. Repo Preflight

| 事实 | 证据 |
| --- | --- |
| Git | 默认生产main，干净HEAD8a82362；隔离工作区`/www/wwwroot/pc28touzhu-lab-self-service`，分支feat/rule-lab-self-service |
| 框架 | Python WSGI stdlib + vanilla JS/CSS + SQLite；pyproject setuptools，Python声明>=3.8，现有生产实际3.12 |
| CI | `.github/workflows/docker-image.yml`只有Docker构建推送，无lint/test工作流；本轮不改CI |
| Python测试 | `env PYTHONPATH=src python3 -m unittest discover -s tests -q`；单模块同样unittest；tests包含集成/worker/API |
| UI测试 | jsdom真实临时WSGI；模块`/tmp/pc28-draw-trigger-ui-check/node_modules/jsdom`；临时fixture只访问本机 |
| 浏览器 | 已发现并由root启动验证chromium `/root/.cache/ms-playwright/chromium-1243/chrome-linux64/chrome`，playwright-core `/tmp/pc28-cycle-ui-tools/node_modules/playwright-core`；无需安装 |
| Schema | 当前6；schema7加表/列，`RuleLabStore.initialize`集中迁移 |
| 路由 | `/api/platform/rule-lab`集中路由、严格query字段与16KiB限制；静态JS显式映射 |
| 访问 | 当前session管理员门禁 + user_id隔离；不改认证系统 |
| 常量 | 2000保存、80批次/用户、100运行；每批100、每条件双赔率现状；新增选择不得绕过 |
| 运行数据 | jnd28批次60～77正在有限回放；enable_continuation=false；生产54批次1806版本，数据随worker推进 |
| 迁移证据 | root已有`/tmp/pc28-self-service-release/preflight.db` schema6一致副本，FK clean；只允许在副本试验 |
| 不可逆范围 | 本轮无删除生产实验/文件；模板删除为软标记；正式发布只能由root，开发owner不得写生产库/重启服务 |

## 2. 写集owner与任务

文件前缀均相对于隔离工作区。M=修改，A=新增。没有删除文件。除明确标注FE fixture及DOMAIN两个文件外，Python均Backend owner；新增范围外文件先向root说明，避免重复共享写入。

| Task | Owner | 写集 | 工作/验收 |
| --- | --- | --- | --- |
| B01a | DOMAIN（BE委托） | A `src/pc28touzhu/domain/rule_lab_templates.py`; A `tests/test_rule_lab_templates_domain.py` | 纯条件规范化/语义去重/内置key对账；不读写数据库，不改中央domain模块 |
| B01b | BE | M `src/pc28touzhu/domain/rule_lab.py` | 调用纯编译模块、build_experiments冻结集成；DOMAIN交付后串行衔接 |
| B02 | BE | A `src/pc28touzhu/services/rule_lab_templates.py`; M `services/rule_lab_store.py` | schema7、模板CRUD/收据、出处列；迁移重复幂等 |
| B03 | BE | M `services/rule_lab_service.py`; M `api/app.py` | 模板路由/资产路由/128KB定向放宽、权限错误、选取解析 |
| B04 | BE | M `domain/rule_lab_comparison.py`; M `services/rule_lab_store.py`; M `services/rule_lab_service.py` | 单条件/单赔率自选组、政策去重、旧三字段create兼容 |
| B05 | BE | A `services/rule_lab_bulk.py`; M `services/rule_lab_service.py`; M `services/rule_lab_store.py` | bulk预览/单事务创建/容量/idempotency |
| B06 | BE | A `services/rule_lab_queue.py`; M `services/rule_lab_store.py`; M `rule_lab_worker.py` | 统一普通+study历史入队、bounded晋级、公平及全局名额 |
| B07 | BE | M `services/rule_lab_management.py`; M `services/rule_lab_series.py`; M `scripts/maintenance/run_rule_lab_batch_queue.py` | 旧state/fork/delete/edit/series旁路一致；配置冻结与清单停止兼容 |
| B08 | BE | A `scripts/maintenance/import_rule_lab_queue.py` | 默认只读、显式apply导入原清单；旧unit仍运行时拒绝，重复导入幂等 |
| B09 | BE | A `tests/test_rule_lab_templates.py`; A `tests/test_rule_lab_self_service_api.py`; A `tests/test_rule_lab_queue.py` | 业务红测→绿测，模板/快照/API/队列/故障交错 |
| B10 | BE | A `tests/test_rule_lab_self_service_migration.py`; M 既有`tests/test_rule_lab_*.py`（**排除FE fixture**） | schema期望升级、兼容新状态，原结果与续算等回归 |
| F01 | FE | M `src/pc28touzhu/ui/rule_lab.html`; M `ui/rule_lab.css`; M `ui/rule_lab.js` | 新入口/抽屉、RuleLabUI bridge、display_status、保留现结果布局 |
| F02 | FE | A `src/pc28touzhu/ui/rule_lab_self_service.js` | 条件/风控模板CRUD、bulk向导、选项对照、队列完整可用 |
| F03 | FE | A `tests/rule_lab_self_service_ui_server.py`; A `tests/test_rule_lab_self_service_ui.cjs` | 真实临时API DOM关键流程；不mock-only，不用生产数据 |
| F04 | FE | A `tests/test_rule_lab_self_service_browser.cjs`; M 既有`tests/test_rule_lab_*.cjs` | 手机/桌面浏览器流程；更新已改变预期但不削弱旧测试 |

B01a与BE其他互斥任务可以并行；B01b依赖B01a的函数契约/交付；B01b～B08串行依赖主要按plan M1/M2/M3，B09/B10随对应任务先创建红测。Frontend可在wire冻结后与Backend并行准备，API未通时只允许有效红测，不称验收完成。F03 fixture为唯一FE可写Python；BE不得改它。DOMAIN独占domain/rule_lab_templates.py及test_rule_lab_templates_domain.py；BE不能同时编辑，中央domain/rule_lab.py仍由BE独占。全部中央store/service/api与schema仅BE写，根不与BE同时修改。

正式产物文档owner：lead写tech-review/plan/tasks/waves；BE/FE将执行证据回报root，root可指定独立QA写qa-report，DevOps写deploy-report。开发owner不git merge生产、不push、不装依赖、不向外部发消息。

## 3. Contract Matrix

| ID | 承诺/界面 | 载荷/服务端 | 落库/业务规则 | 证据 |
| --- | --- | --- | --- | --- |
| CM01 | 条件模板可增删改，内置只读 | plan2.1/2.2，整数revision，kind定义白名单 | 当前模板revision+快照；50去重/20,50,100窗口 | templates + API + UI CRUD |
| CM02 | 模板改删不变旧成绩 | ConditionSelection、batch_snapshot | conditions/risk/odds保留；旧hash不含provenance | templates + migration + fork续算 |
| CM03 | 原风控基准固定，重复不生成 | StudySelection + Candidate | normalize后去重；2～12；保持冻结赔率 | self_service_api + comparison worker |
| CM04 | 1条件×1赔率×2方案=2版本 | condition_keys/settlement_rule_ids | 仅交集完整集合，缺配对拒绝 | API实际计数、组完成可比 |
| CM05 | 批量预览准确，零半组 | BulkConfig/preview_revision/request_id | 一个事务收据+批次+实验+可选queue | 并发容量、末项失败、丢响应重试 |
| CM06 | 草稿/等待/暂停不同 | queue_state/display_status/actions | 唯一owner条目，暂停/移出不自动恢复 | queue测试+真实UI |
| CM07 | 100运行名额不越界 | 旧state与新enqueue统一约束 | 写事务重读全局计数，study轮转 | queue并发、worker、旧API回归 |
| CM08 | 原清单接着跑 | 仅本地白名单CLI | 旧unit停→幂等导入，ID/游标/结果不改 | migration实际副本，导入中故障重启 |
| CM09 | 不自动建续算/真实方案 | enqueue不接受schedule字段 | 不增lab_series，不写业务主库 | queue/API表存在与计数断言 |
| CM10 | 同名可区分，日期独立 | name_pattern固定三个占位符 | names不含自动日期，改名不换hash | bulk预览+rename后仍eligible |
| CM11 | 权限/失败安全 | 401/403/404、request_id冲突409 | user_id隔离、收据与写同事务 | API真实授权矩阵/超载/XSS输入 |
| CM12 | 旧结果视图继续可用 | 原响应字段保持，新字段追加 | schema7不改旧值，日/月结果独立 | 全回归+既有layout/observation/results DOM |

## 4. Evidence Waves

机器命令和writeSet在waves.json；下面定义执行意图。红测必须在该Wave实现之前实际失败并说明缺哪个业务行为；文件缺失或import失败只算准备未完成。重跑同命令绿才可验收。

| Wave | 范围 | 红测重点 | 绿门禁 | Stop condition |
| --- | --- | --- | --- | --- |
| W1 | 模板CRUD/schema/冻结与fork | 无模板路由、改模板后旧结果保持、内置key对账 | 新模板/API用例+旧domain/risk/management | 旧hash/成绩改变、权限泄露或半迁移即停 |
| W2 | selective study + bulk | 单赔率被旧约束拒绝、批量幂等/容量 | API+comparison worker+并发 | 任何半组、赔率替换、伪可比即停 |
| W3 | 队列/旧入口/清单导入 | 暂停被重启、并发超100、导入重复 | queue+迁移+旧batch_queue/series | 多启动者、进度倒退、隐式schedule即停 |
| W4 | 页面全链/独立回归 | 模板与新建/队列UI缺失可操作行为 | 新UI+浏览器、旧结果DOM、Python全量 | 主路径不可用、旧结果回归、容量错误即停 |

## 5. 发布与核对

开发交付必须包含每Wave实际命令/结果、变更摘要、未覆盖项。root独立检查Python/JS、diff与依赖变化，再决定具体验收通过。当前存在的旧测试skip需单列原因，不新增skip掩盖失败。

正式发布依次：副本schema7/连续计算等价→外置备份→确认唯一启动者切换方案→代码快进及变更文件读取权限→迁移/有限清单导入→平台/worker恢复→真实只读页面/API+进度核对。仍运行jnd28仅自然推进，严禁把preflight备份写回生产。新生产UI CRUD验证只操作临时模板/草稿并清理其自身，不能改用户实验。

STATUS: DONE
REASON: 完整写集和wire已冻结，用户既有授权覆盖本轮修改，可以派发Backend/Frontend。
