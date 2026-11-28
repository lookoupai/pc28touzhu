---
type: tasks
outputFor: [frontend, backend, qa]
dependencies: [approved-user-request, tech-review, plan]
---

# 列表观察规则开发任务

## 摘要

- 8项任务：后端3项、前端2项、独立QA2项、部署1项。
- 关键路径：T1统计 → T2同快照读取 → T3HTTP → T5真实页面 → T6/T7验收 → T8发布。
- Blast Radius为中高：最终实际写集24个物理文件（含7份内部文档）；2份旧UI测试保持未改，中央store/service/app与UI文件由固定owner管理，无依赖、迁移、执行器／队列变更。
- 风险确认触发项：≥10路径、已有路由入口及页面状态变更。用户已明确批准，属于既有授权的可逆实施，根代理已裁决继续，不额外提问。
- API及字段以 `plan.md` §3 为冻结契约；时间／金额口径不由前端自行猜测。

## 文档信息

功能 `rule-lab-observation-list`，版本1.0，2026-11-28。需求来源为批准消息 `msg_2d586e2739bb4d109062a9afc29b4b55`，前序数据分析见 `/tmp/pc28-list-signals-checkpoint.md`。本目录是工程内部记录，未请求用户报告或附件。

## 0. Repo Preflight

| 事实 | 结果 | 证据 |
|---|---|---|
| 默认／生产分支 | main，基线ca1d918，初始clean | 根代理git检查 |
| 开发分支 | feat/rule-lab-observation-list | git branch --show-current |
| 工作目录 | /www/wwwroot/pc28touzhu-lab-observation | 隔离worktree |
| CI | Docker构建和推送，无测试门禁 | .github/workflows/docker-image.yml |
| 技术栈 | Python标准库WSGI、SQLite、原生JS/CSS | pyproject.toml、app/store/ui代码 |
| 测试 | Python unittest；真实WSGI＋jsdom | tests/test_rule_lab_*，tests/rule_lab_results_ui_server.py |
| JS依赖 | 已有jsdom路径/tmp/pc28-draw-trigger-ui-check/node_modules/jsdom | 根代理既有验证记录，不安装 |
| schema／枚举 | Python纯域／store／API直接定义；模拟schema6 | rule_lab_monthly.py、rule_lab_store.py |
| 常量 | 650保存、100运行、每批100、组5方案；1核/256MiB/1GiB维持 | store常量、既有systemd配置 |
| 访问控制 | admin session＋owner查询＋no-store | app._dispatch_rule_lab与store._owned |
| 路由 | 既有batch/study GET增加成对观察参数，其他query白名单保持 | plan §3；app.py |
| 数据变化 | 无schema、迁移、重算、源读取、生产写入或新调度 | 本次明确范围 |
| 读基线 | 一CPU/256MiB一致副本：92版本批次14～44ms、202 SQL、133411B；10版本组4.9～7.4ms、53 SQL、24054B；RSS25324KiB | 根代理QA实测 /tmp/pc28-observation-baseline-20261128.db |

## 1. 写集与责任

| 文件 | 操作 | owner | 任务 |
|---|---|---|---|
| src/pc28touzhu/domain/rule_lab_observation.py | 新建 | Backend | T1 |
| src/pc28touzhu/services/rule_lab_observation.py | 新建 | Backend | T2 |
| src/pc28touzhu/domain/rule_lab_monthly.py | 按需最小重构 | Backend | T1；只共享日期语义 |
| src/pc28touzhu/services/rule_lab_store.py | 修改共享文件 | Backend | T2 |
| src/pc28touzhu/services/rule_lab_series.py | 根代理批准的最小修改 | Backend | T2；删除中空子组摘要可读 |
| src/pc28touzhu/services/rule_lab_service.py | 修改共享文件 | Backend | T3 |
| src/pc28touzhu/api/app.py | 修改共享文件 | Backend | T3 |
| tests/test_rule_lab_observation.py | 新建 | Backend | T1/T2，串行同owner |
| tests/test_rule_lab_observation_api.py | 新建 | Backend | T3 |
| src/pc28touzhu/ui/rule_lab.html | 修改 | Frontend | T4/T5，串行同owner |
| src/pc28touzhu/ui/rule_lab.js | 修改共享文件 | Frontend | T4/T5，串行同owner |
| src/pc28touzhu/ui/rule_lab.css | 修改 | Frontend | T4/T5，串行同owner |
| tests/rule_lab_observation_ui_server.py | 新建 | Frontend | T4/T5 |
| tests/test_rule_lab_observation_ui.cjs | 新建 | Frontend | T4/T5 |
| docs/RULE_LAB.md | 修改 | Frontend | T5 |
| .boss/rule-lab-observation-list/ui-spec.md | 新建 | Frontend | T4/T5；列表布局与状态规范 |
| tests/test_rule_lab_observation_qa.py | 新建 | QA | T6 |
| tests/rule_lab_observation_benchmark.py | 新建 | QA | T7 |
| .boss/rule-lab-observation-list/qa-report.md | 新建 | QA | T6/T7 |
| .boss/rule-lab-observation-list/deploy-report.md | 新建 | DevOps | T8 |
| .boss/rule-lab-observation-list/{tech-review.md,plan.md,tasks.md,waves.json} | 新建 | Planner | 已完成规划 |

最终写集共24个物理文件；两项旧UI测试无需兼容改写，原文件直接通过回归；月报公共pending参数与series空集合修复实际保留。未列路径必须先向根代理报告owner与必要性，不能偷偷扩大写集；尤其不得修改旧测试fixture改变已有意义。计划者不写运行代码，QA不改实现，DevOps不在授权前动生产。

## 2. 任务详情

### T1 [BE] 纯统计与连续窗口

依赖：冻结contract。写集：domain observation、monthly最小公共提取、test_observation。先RED实现精确N月份、complete-only锚点、缺月/无样本占位、原始ROI、月日结回撤、两段14天可用性、窗口最佳3天和回撤持续。恢复天数示例：窗口前一日高点0，第1天亏、第3天回0，持续3个自然日；当前未恢复只算至实际已处理日，不延伸未来。输入不变性与跨年闰月、全零／全负、月初部分覆盖均须测试。复杂度高。

### T2 [BE] 同快照批量读取

依赖T1。写集：services observation、store、test_observation（同owner），另含根代理批准的series摘要一行空集合默认值修复。为显式观察路径建立query_only BEGIN，原row stats和日账／pending同快照。批次元数据只读一次，按IDs批量日账、全账count/net/stake核对、pending汇总，不逐行调用完整_monthly_report。全部months明细必须对照原月报口径。新增观察相关SQL数量不得随实验数线性增长；旧风险视图已有查询不是本轮重写目标。删除分批清理期间组子批次可能全部移除而组行仍在；新旧GET都需返回200空rows且保持owner边界，不能因max空集合返回500。该修复不更改队列／迁移语义。复杂度高。

### T3 [BE] HTTP与兼容

依赖T2。写集：service、app、test_observation_api。两参数必须成对、严格范围、拒绝重复／未知及其他路由附带；无参数原响应保持。admin、wrong-owner、method、no-store、源不可用但历史观察可读、所有请求无写入验证。复杂度中。

### T4 [FE] 设置和纯客户端判定RED

依赖冻结contract；可与T1～T3并行，真实数据接线验收等待W1。写集：html/js/css、新UIfixture/test。默认2/包含部分/3%/200笔/10天；自定义1～12，2/3/6快捷、可选spread/DD注数/恢复天数、red/blue/purple、仅匹配和运行字段开关。localStorage版本校验／异常安全；应用和恢复默认。精确原始分值比较，收益与平稳标签独立。UI RED必须实际因缺新控件／行为失败。复杂度中。

### T5 [FE] 两列表主路径与回归

依赖T3、T4。写集同T4＋文档；两份既有UI测试保持未改并直接回归。只有月份／部分月模式改变才读API；其他设置本地重绘。请求序号＋响应window回显防乱序；加载／失败不误用旧设置。批次及比较表核心列统一、过滤排序分页一致，运行信息及窗口敏感性摘要展开。保留管理／删除／续算／原金额估算／CSV与详情交互；不启动浏览器或读取profile。复杂度高。

### T6 [QA] 独立口径与安全反例

依赖contract可先RED，最后验收依赖T3/T5。独占qa测试及qa-report。独立oracle覆盖exactN缺月、未来/未处理、满月边界、pending窗口内外、2.995舍入、count/net/stake缺行、组未可比、参数拒绝和query_only写保护。生产一致副本只读比对观察与已验证月报，不初始化或写原副本。复杂度中。

### T7 [QA] 性能和保护证据

依赖T2/T3；独占benchmark＋qa-report（顺序写）。用100版本×12个月及500版本比较压力，另用固定一致副本比较有／无观察。记录SQL新增量、响应字节、耗时、RSS与无数据库变更。性能门禁：无按实验增加的日账／月报查询；响应不带daily/equity；1CPU/256MiB可完成；真实92版本响应目标<300ms且没有明显秒级退化，若环境噪声越界须重复定位后报告。复杂度中。

### T8 [DevOps] 发布准备与执行

依赖QA通过；预备只读可并行。独占deploy-report与/tmp临时验证脚本。记录现有原定义／成绩／日账指纹与worker/source/executor状态；根代理给准确commit后快进生产并只重启platform API。线上GET＋jsdom读观察、改设置、验证零写请求；核对原定义/结果和运行资源，外部静态资源字节匹配。无DB迁移、worker重启、续算开关改动。异常回退代码并重载API，不用旧DB覆盖新成绩。复杂度中。

## 3. 实现前检查

[x] 工作树及基线已建立；[x] 现有月报/store/API/UI已读；[x] JSON契约由根代理确认；[x] 无新依赖；[x] 责任边界明确；[x] 各owner记录有效RED后实施，最终GREEN证据已齐备。

## 4. 依赖、并行与证据波

| 并行组 | 任务 | 约束 |
|---|---|---|
| A | T1/T2/T3后端顺序；T4前端；T6独立RED；T8只读准备 | runtime写集隔离，均先RED |
| B | T5接线；T6独立GREEN；T7性能 | W1已通过；QA不改实现 |
| C | T8执行 | W2/W3全部通过＋根代理准确提交GO |

| Wave | 范围 | 主owner | Contract行 | Stop condition |
|---|---|---|---|---|
| W1 | 统计、数据快照、HTTP | Backend | CM1～CM6 | 日期／原始金额／所有权／只读任一失败禁止进入集成放行 |
| W2 | 设置、两个列表、旧交互 | Frontend | CM1～CM9 | 旧数据套新口径、丢失旧功能、过滤分页或持久化失败须修复 |
| W3 | QA、性能、上线 | QA/DevOps顺序 | CM1～CM10 | 无真实证据／不一致账本／越界资源／线上回归禁止宣称完成 |

机器命令见waves.json，均argv不含shell重定向。Python执行环境PYTHONPATH=src；ROOT会实际执行边界检查，不只接受报告。

### Contract Matrix

| ID | 承诺 | UI／copy | Client | Server／持久化 | 业务规则 | 证据 |
|---|---|---|---|---|---|---|
| CM1 | 连续N月不压缩 | X/N月、阶段／完整 | months+partial | months exact N、missing占位；无DB写 | 起月不足／中间缺月不能自动减N | observation+QA+UI |
| CM2 | 原始金额阈值 | ROI%、差距pp、DD注数 | 阈值本地 | net/stake/DD整数分 | 2.995不能过3，0投注ROI未知 | observation+QA+UI |
| CM3 | 同快照只读 | 当前成绩与摘要一致 | 单GET | query_only BEGIN | 无源读取／无结果或schema写 | API+并发QA |
| CM4 | 日期与近期口径 | 窗口末14天／截至日期 | 共享owner窗口 | recent/previous.available | 未覆盖／pending为null不当零，不滑动不同窗口 | observation+QA |
| CM5 | 质量优先 | 待核对／已知跳过 | 不自动覆盖blocker | 6个blocker enum | oldengine/conflict/gap/windowpending/mismatch/notcomparable | API+QA+UI |
| CM6 | 接口严格／兼容 | 错误可重试 | 两参数成对 | owner/admin/no-store、legacy无观察 | 其他routes保持白名单 | API |
| CM7 | 设置持久化 | 本浏览器保存 | version1 localStorage | 无服务端偏好写 | 无效存储可恢复，未知色值拒绝 | UI |
| CM8 | 纯展示筛选 | 高亮＋文字／仅匹配 | 除window外零GET/POST | 不改策略／注额／日续算 | 两列表统一过滤／分页，旧响应丢弃 | UI |
| CM9 | 风险敏感性诚实 | 窗口内、日结、最佳3天排除 | 读取摘要 | left_censored+currentunderwater | 不冒充全历史／反事实重测 | observation+QA+UI |
| CM10 | 有界与线上保护 | 无额外性能模式变化 | 紧凑payload |650/100/1GiB/256MiB不变 | 新查询按batch不是按row；原账本保留 | benchmark+deploy |

## 5. 规范及变更记录

遵循项目实际Python/原生JS，不引入模板里的React/TypeScript。数据纯函数、API、页面、QA边界清楚；新增异常不能吞掉并显示成功。2026-11-28初始计划由Planner完成，后续必须按实际实现更新写集与验证状态；本文件不声明代码或测试已经通过。


## 6. 发布前任务收尾状态

| 任务／证据波 | 状态 | 实际证据 |
|---|---|---|
| T1～T3 / W1 | 已通过 | 后端domain/API及既有月报/API回归；根代理最终27项2.196s通过，含延迟首日与空组删除边界 |
| T4～T5 / W2 | 已通过 | 新DOM与未改动的金额／结果DOM全通过；原始分值边界、存储、失败、乱序、403认证取消和展开阅读覆盖 |
| T6～T7 / W3 QA | 已通过 | 全量Python1284项、2可选跳过，293.726s；638版本月度对齐与500版本压力通过 |
| T8 / W3 发布 | 待准确提交放行 | DevOps完成准备；根代理提交后执行线上发布和保护核对，本文件不宣称已上线 |

最终根代理日志：`/tmp/pc28-observation-full-python.log`、`/tmp/pc28-observation-root-targeted.log`、`/tmp/pc28-observation-root-ui.log`、`/tmp/pc28-observation-root-amount.log`、`/tmp/pc28-observation-root-results.log`。全量开始后新增2项domain边界用最终27项覆盖。旧 `tests/test_rule_lab_amount_ui.cjs` 与 `tests/test_rule_lab_results_ui.cjs` 为回归输入，不在最终写集。
