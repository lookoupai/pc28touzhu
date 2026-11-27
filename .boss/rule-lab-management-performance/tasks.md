# 实验管理、性能与空间回收任务规格

## 摘要

- 任务10项；高Blast Radius，涉及实验账本/调度/加法schema/新管理API，不涉及真实下注或上游预测库。
- 用户已授权性能优化和编辑删除/回收功能；必要可逆实施在本轮范围内，不因文件数量重复索要许可。现有真实实验的删除仍只能由用户在产品确认后发生。
- 关键依赖：性能store冻结→Data集成→产品闭环→独立QA→受控发布；helpers/API/UI红测准备可并行。
- 固定HTTP/Python/字段契约见plan §3；禁止各owner另造不兼容签名。方法变更先通知消费者和根代理。
- 预计80～130次实质工具操作；这是复杂度说明，不是耗时承诺。

## Repo Preflight

| 项目 | 已确认事实/证据 |
|---|---|
| Git | 默认main；当前feat/rule-lab-management-performance，基线20cb785；开始时无代码变更 |
| 技术 | pyproject setuptools，Python声明>=3.8、实际python3.12；stdlibWSGI/SQLiteWAL，原生JS/CSS/HTML |
| CI | .github/workflows/docker-image.yml只做构建推送，没有测试任务 |
| 依赖 | optionalTelethon沿用；本轮无依赖安装或锁文件更改 |
| 测试 | env PYTHONPATH=src python3 -m unittest discover -s tests -q；浏览器node *.cjs临时WSGI，既有Playwright/jsdom无需安装 |
| schema | store.initialize当前v4；series/archive扩展表；新管理v5加法，NONE→INCREMENTAL另走CLI |
| 路由 | api._dispatch_rule_lab；仅segments已开放明确query白名单，其余仍拒绝query |
| 权限 | session/admin + user_id归属；来源公开状态；POST16KB/no-store |
| 常量 | 保存650/运行100，输入页200/暖机100，清理批500；1核256MiB/目录1GiB |
| 线上状态 | 两个50版本批次运行、7批待队列、原26保留，总602；一次性队列manifest驱动 |
| 风险资料 | /tmp/pc28-management-baseline.db一致副本219639808字节，测试先复制；源2GB库不动；磁盘约108GB空闲（父任务只读提供） |
| unknown | 实际性能热点占比、转换耗时、可回收率，必须测量后写入验收，不能用推测填数 |

## 文件所有权与交接

本轮执行者：Perf=`/root/management_perf`，Data=`/root/management_data`，APIUI=`/root/management_product`。输入档案GC归Data，中央store按下述顺序交接；不并行改写。

中央 `src/pc28touzhu/services/rule_lab_store.py` 先由Perf修改；其绿门禁通过且明确冻结后，根代理交给Data做schema/管理集成。两人不可同时编辑。Perf继续独占worker，Data需要worker维护hook时通过Perf实现或由根代理显式交接。Data可先写新helpers和独立tests，APIUI可先写接口/浏览器红测。除下表外新增路径需先向根代理确认。


| owner | 精确写集 |
|---|---|
| Perf | `src/pc28touzhu/rule_lab_worker.py` |
| Perf | `tests/test_rule_lab_performance.py` |
| Perf | `tests/rule_lab_performance_benchmark.py` |
| Perf | `.boss/rule-lab-management-performance/performance.md` |
| Data | `src/pc28touzhu/services/rule_lab_management.py` |
| Data | `src/pc28touzhu/services/rule_lab_storage.py` |
| Data | `src/pc28touzhu/services/rule_lab_series.py` |
| Data | `src/pc28touzhu/services/rule_lab_archive.py` |
| Data | `scripts/maintenance/run_rule_lab_batch_queue.py` |
| Data | `scripts/maintenance/prepare_rule_lab_storage.py` |
| Data | `tests/test_rule_lab_management.py` |
| Data | `tests/test_rule_lab_storage.py` |
| Data | `tests/test_rule_lab_comparison.py`（仅schema版本断言4→5） |
| Data | `tests/test_rule_lab_continuous_qa.py`（仅schema版本断言4→5） |
| Data | `tests/test_rule_lab_risk.py`（仅schema版本断言4→5） |
| Data | `tests/test_rule_lab_series.py`（仅schema版本断言4→5） |
| APIUI | `src/pc28touzhu/services/rule_lab_service.py` |
| APIUI | `src/pc28touzhu/api/app.py` |
| APIUI | `src/pc28touzhu/ui/rule_lab.js` |
| APIUI | `src/pc28touzhu/ui/rule_lab.html` |
| APIUI | `src/pc28touzhu/ui/rule_lab.css` |
| APIUI | `tests/test_rule_lab_management_api.py` |
| APIUI | `tests/rule_lab_management_ui_server.py` |
| APIUI | `tests/test_rule_lab_management_ui.cjs` |
| APIUI | `docs/RULE_LAB.md` |
| QA | `tests/test_rule_lab_management_qa.py` |
| QA | `.boss/rule-lab-management-performance/qa-report.md` |
| DevOps | `.boss/rule-lab-management-performance/deploy-report.md` |
| Perf→Data串行交接 | `src/pc28touzhu/services/rule_lab_store.py` |

## 任务与依赖

| 任务 | Owner/复杂度 | 具体输出和红测 | 依赖 |
|---|---|---|---|
| T1 热点基准 | Perf/中 | performance_benchmark固定2×50高触发+92组合，固定时钟/快照，记录cProfile/SQL/CPU/RSS；新test先证明待优化开销/状态边界 | 无 |
| T2 等价优化 | Perf/高 | worker/store必要热点；每页事务原子、revision/暂停/删除guard不绕开；优化前后结果hash一致；完成后store交接 | T1 |
| T3 v5管理基础 | Data/高 | management/storage新模块、回执/job/队列opt-out/ID高水位；所有创建路径单调ID；旧列不变红测 | 契约固定，store改动等T2 |
| T4 rename/edit/fork | Data/高 | 同事务资格/容量/来源定义、pristine替换、已有成绩fork新暂停；rename保持hash；queue不自动启动编辑后版本 | T3 |
| T5 删除job | Data/高 | preview→fence→分批清所属数据，依赖study拒绝、child归属、pending取消无伪结算、共享源保护、receipt对象删后仍可查 | T3/T4 |
| T6 存储回收 | Data/高 | storage_status细分、回收job、正页上限incremental_vacuum消费游标、WALbusy重试；compact CLI默认预览/显式apply | T3 |
| T7 队列/worker接线 | Data+Perf串行/中 | Data queue_eligibility改脚本，404/删除/编辑终态移出；Perf worker每tick有界管理维护，待删除/回收任务短周期推进，不能15秒/500行导致大删除拖数小时 | T5/T6，中央文件owner规则 |
| T8 服务/API/UI | APIUI/高 | 严格管理路由、可见编辑模式/新版本、删除确认/进度、存储分项；真实WSGI新fixture桌面/390手机，无生产写入 | 契约固定后可并行，实现绿门禁等T4～7 |
| T9 独立QA | QA/高 | 并发delete-vs-worker、旧preview、ID复用、共享archive、队列重启、大删除分页、存储busy/空间不足/物理缩小；生产副本转换旧列hash | 全部实现 |
| T10 发布 | DevOps/中 | 临时脚本只停实验API/worker/queue保存原状态；独立库备份/转换/校验/恢复；线上只读，不测试删真实实验 | QA放行 |

## Evidence Waves

命令全部在waves.json中用argv表达；每阶段必须记录实际红→绿。W1性能、W2数据与回收、W3产品、W4完整QA、W5上线。独立工作可按写集并行准备，但验收依赖不能跳过。根代理只在集成稳定后跑全项目，子agent只跑所属专项，避免重复占用资源。

## Contract Matrix

| ID | 契约 | UI/请求 | 服务/持久化约束 | 测试证据 |
|---|---|---|---|---|
| CM1 | 提速不改结果 | 原运行方式和额度保持 | 同输入/时钟/版本，stats/risk/日账/pending/游标相同，暂停删除仍生效 | performance、worker、continuous_worker；前后基准 |
| CM2 | 按状态编辑 | 改名称/编辑未开始/修改并新建三种 | opaque管理revision不随每期波动；pristine状态事务复核；原成绩只读 | management/API/UI |
| CM3 | 原队列不复活改删对象 | 编辑后提示已退出排队；删除移出 | queueoptout/tombstone、404终态removed、全部ID入口单调；rename不退出 | batch_queue、management、QA |
| CM4 | 有界永久删除 | preview名称确认、进度、恢复查询 | 先禁写/禁调度再≤500行清理；receipt幂等对象删后仍可查；pending不伪结算 | management/QA/API/UI |
| CM5 | 共享和依赖保护 | 明确保留共享输入/先删依赖组 | study child拒绝；source被study引用409；其他consumer/cache需求保留 | management/QA |
| CM6 | 物理空间真实 | 实际占用/可复用/预计可回收/实际释放 | freelist与db/WAL分开；busy不报成功；预算不绕过磁盘实际限制 | storage真实文件tests/UI |
| CM7 | 安全一次转换 | 存储未就绪提示，线上请求不VACUUM | CLI默认预览；停三个实验写者、外置备份、2×额外空间、模式/FK/quickcheck/旧列hash | storage副本、QA、deploy |
| CM8 | API权限与重试 | 按钮禁重、丢失响应查job/原请求重试 | admin/owner/no-store/16KB/未知字段拒绝，跨owner404；GET零写 | management_api/浏览器 |
| CM9 | 活跃回测可切换 | 无需等待全部跑完 | 原服务状态/进度/成绩保持，1核256MiB；真实实验不删，上游不改 | 生产副本/发布GET/后续进度 |

## 风险与停止条件

任何性能等价失败、误删其他实验/共享输入、删除后重写、ID复用、容量越界、旧库转换数据差异，都必须修复后再进入下一阶段。业务定义新增/删除由新UI显式用户操作触发，本次部署不代用户执行。若某阶段依赖尚未实现，可准备红测但不得把测试基础错误当行为红测。

**STATUS: DONE_WITH_CONCERNS**

**REASON:** 精确写集、串行交接与验收契约已准备，可派发实现；性能收益及转换耗时待实测。

## 最终写集和验收补充

以实际git diff和未跟踪新文件核对：本轮未修改既有test_rule_lab_batch_queue.py，队列竞态由新management/QA测试验证；四个既有迁移测试只将schema4断言更新为5，原列保持断言保留。中央store已经完成Perf→Data串行交接；worker维护hook由Perf独占完成。实际源码/脚本/测试/用户说明共26个变更文件，另有本feature产物文档；无依赖清单、锁文件或部署unit变更。

最终内部接口补充见plan§3：management_receipt先恢复原始请求回执，edit/fork的request_payload用于区分原始载荷与规范化配置，start_queued_batch原子复核清单hash/opt-out/未启动状态及运行额度，overview.management_jobs恢复删除对象已消失时的任务状态。ID allocator覆盖archive/series稳定身份。

W1～W3专项已通过，新管理真实浏览器完整流程退出码0（/tmp/pc28-management-ui-final.log，归档原工具输出非重跑）；W4独立QA8/8和CLI5/5、三条原浏览器均通过。首轮ROOT全量1194项中1193通过、1失败：未知服务状态的CLI guard在该进程导入旧模块后才完成修复；修复后新进程专项8/8及5/5通过。冻结修复代码完整重跑1197项全部通过，0失败、0跳过，329.800秒，实际退出码0（session21060），日志/tmp/pc28-management-full-final-root.log。W4已放行；ROOT已完成390aa45发布及只读线上核验，W5已通过。本次未提交或部署。

## 最终发布状态（CM9）

ROOT执行发布脚本并观测退出码0：`/tmp/pc28-management-rollout-20261127T155206Z.json`（session77762）。实验写入暂停71.946秒，22张原表共1,739,112行旧列哈希全部相同，602个实验ID、批准队列及批次状态/游标保持。schema5、auto_vacuum=2；实验数据库文件478,371,840→436,006,912字节。独立备份保留，以上文件缩小不能说成服务器净释放空间。

线上只读检查退出码0：`/tmp/pc28-management-online.json`（session82220），0 POST、无页面错误，1440/390桌面手机通过；worker仍1核/256MiB，执行器在线心跳2秒，上游PID1250128不变。恢复后批次12处理数15532→15715、批次16处理数15880→16015，证明原运行工作继续推进。CM9已由实际发布/进度证据关闭；W1～W5均通过。

正式发布版本390aa45；详细过程由ROOT维护deploy-report.md，QA由其owner更新。本文仅登记已提供的实际证据，未再次部署或提交。
