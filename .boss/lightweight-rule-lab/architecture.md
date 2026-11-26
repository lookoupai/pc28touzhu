# 轻量规则实验室架构

## 摘要

- 沿用现有Python WSGI、原生HTML/CSS/JS与SQLite；新建独立模拟库和一个受限worker，不新增依赖、不修改上游代码。
- 只允许 `pc28_netdisk_regular` 与 `pc28_high_regular`；模板生成成对实验，首版固定金额单腿跟随，不接入真实执行链。
- 历史回放与前向模拟分开；前向先冻结尚未开奖的预测/决策，再按新开奖结算，进程停机期间错过的信号不回填成前向投注。
- API只负责配置与有界汇总查询；worker分页提取共享输入、增量统计，游标/资金/汇总同事务提交。CPU 0.2核、内存256MiB、目录1GiB为整个worker的预算。
- 管理员入口、逐批次user_id隔离；上游仅使用公开逐期预测器。源路径由服务器配置，浏览器不可传文件路径。

## 1. 调研依据与选型

已阅读本仓 `api/app.py`、`api/ui_navigation.py`、`config.py`、`domain/settlement_rules.py`、`domain/pc28_profit_rules.py`、`docs/roll_trigger_analysis_20261105/analyze.py`；源仓 `database.py` 中draw/prediction/predictor表及索引。主机已观测4核、约16GB，两个业务库各约2GB，不能把全库加载到内存。

官方资料已读取：SQLite URI 文档 https://sqlite.org/uri.html （`mode=ro`，活库不可使用`immutable=1`）；systemd资源控制 https://www.freedesktop.org/software/systemd/man/latest/systemd.resource-control.html （CPUQuota/MemoryHigh/MemoryMax）。选择依据是现有结构和资源约束，不引入新的框架。

| 选型 | 候选比较 | 决策 |
|---|---|---|
| UI/API | 现有原生页面+WSGI能复用登录和导航；React/FastAPI需额外构建和部署；独立报表无法操作实验 | 扩展现有页面与API |
| 运行 | 单worker可序列化事务并受cgroup约束；每规则线程放大状态和争用；Celery/Redis需要新常驻服务 | 单Python worker，stdlib flock防并发 |
| 存储 | 独立SQLite隔离写锁；主库加表污染账本热路径；PostgreSQL需新服务 | 独立SQLite，WAL、短事务、小缓存 |
| 源读取 | 只读本机SQLite有索引、无接口负载；每规则HTTP重复序列化；全库副本每次复制数GB | 只读连接按游标/期号小批量读取，服务端固定路径 |
| 结算 | 复用已有冻结结算函数有退本验证；自己按赔率重写易遗漏；派发到实盘模拟器有副作用 | 纯函数复用，禁止调用dispatch/Telegram |
| 条件 | 有界模板易解释、去重；任意表达式增加复杂度；ML搜索资源与过拟合成本高 | `basic_v1`单条件模板包 |

## 2. 运行与领域契约

### 2.1 首版范围与模板

每个批次选择一个公开预测器、一个metric、一个mode与固定注额。每个候选固定跟随该预测器目标期同metric方向。支持metric `big_small`、`odd_even`、`combo`；不实现过关/倍投/复合多腿。这样可以准确比较触发条件本身，不能把这里的收益标作三关收益。

`basic_v1`确定性生成并按canonical condition去重：

| type | 参数 |
|---|---|
| always | 无条件基准1条 |
| miss_streak | 预测当前连错 >=3/4/5/6/7 |
| hit_streak | 预测当前连中 >=2/3/4 |
| draw_streak | 每个实际方向连续开出 >=3/4/5 |
| draw_absence | 每个方向连续不出 >=5/8/12 |
| hit_rate | 近20/50/100期，大小单双 <=35% 或 >=60%；组合 <=15% 或 >=35% |

二元玩法的`draw_absence`归一化为另一方向`draw_streak`再去重。因此大小/单双25条件、50实验；组合39条件、78实验。每个条件严格配两种OK规则，名称和ID取现有目录，不存在用户传入第三种规则的入口。未来模板包必须换版本；旧definition/snapshot不变。

初始状态paused；用户显式启动后running。`live`运行直到暂停；`backtest`完成指定区间后completed。`resource_paused`用于磁盘或全局活动额度保护，需要显式恢复；`error`保存简短可操作原因，修复后可恢复同游标。暂停不删除配置、不重置收益；恢复不补造停机/暂停期间的前向决策。已经冻结的pending决策仍可结算。

### 2.2 数据输入与可见性

上游数据库默认 `/www/wwwroot/AITradingSimulator/pc28_predictor.db`。仅使用必要字段：

- predictors：id、name、lottery_type、enabled、share_level、share_predictions。资格为pc28、enabled=1、share_predictions=1且share_level严格属于`records`或`analysis`；未知值拒绝。每次创建与worker每轮核验；撤销共享时停止新决策，已冻结决策可结算。不得SELECT *，不得读取API key/prompt。
- lottery_draws：id、issue_no、open_time、result_number、source_payload、created_at。解析 `source_payload.number` 的三个0..9整数，或已有三球字段；sum(triplet)必须等于result_number。只把三个数字和时间写模拟库，丢弃大正文。open_time无时区按Asia/Shanghai，created_at UTC；updated_at不能代表发布时间。
- predictions：id、predictor_id、issue_no、prediction_big_small/odd_even/combo、status、created_at、settled_at。冻结首次使用的必要字段；后续检测到方向冲突计数并隔离，不能静默改写历史结算。input_hash只含预测方向/期号/创建时间或三球号码等不可变输入，不含status、settled_at、updated_at；pending→settled及补齐settled_at是合法状态更新，保持首次观测时间/方向不变，不误判为冲突。

源连接 `mode=ro`、query_only、busy_timeout约250ms、缓存2MiB；查询有200行批次和查询超时（progress_handler）。读完即关闭/释放事务，不在计算期间持有源库读事务。使用已有主键/`(predictor_id,issue_no)`/`(lottery_type,issue_no)`索引；禁止每个候选分别查源库。每批开奖期号共用一次预测查询，再复用所有候选。

这里的只读指应用不修改上游业务数据库内容，不表示SQLite不会创建辅助文件。上游启用WAL，最后连接关闭时可能回收`-wal`/`-shm`；辅助文件不存在时，`mode=ro`连接仍可能需要所在目录的创建权限。worker部署边界见§6，不能用`immutable=1`或`nolock=1`规避活库WAL与锁语义。

历史日期在源库没有open_time索引，必须先按已有`(lottery_type,issue_no)`索引做keyset分页（每页最多200行），取出后在内存筛日期并推进游标；不得把无索引日期过滤与排序放在LIMIT前扫描全表，也不得`CAST(issue_no)`排序。只接受一致的纯数字期号宽度，异常宽度/非数字期号隔离；暖机另走同样有界分页，不能为寻找起点一次扫描所有正文。

**历史回放：** 日期按北京时间inclusive，最多90天，只有源库/精简库实际留存数据可用。worker按期号顺序导入和回放，最大200期/批。目标期预测必须created_at早于该期开奖前40秒；用于历史表现特征的预测还必须settled_at早于当次决策时点。所有条件只读目标期之前的已知结果。当前源库不是不可变预测版本档案，因此mode始终展示“历史回放”，附记录版本限制，不能声称严格前瞻。缺三球、时间不明、缺期或冲突时跳过并计原因，不能猜值。源库分页期号范围通过索引取得，若区间没有数据返回空结果而非编造。

用于历史条件的每条开奖还必须满足`source_created_at <= decision_time`，open_time已过去并不等于当时已经被来源公布/采集；尚不可见开奖不能更新命中率、连开或遗漏。批量扫描到某期开奖结果不代表该结果可用于更早的决策。

**前向模拟：** worker每15秒有界读取关注预测器的最新pending信号和最新开奖。先结算已冻结决策，再更新已经公布的历史特征，然后检查目标为最新实际开奖期号+1的pending预测。只有该目标尚未开奖、预测是当前观察到的pending、条件已满足时，才写入不可变decision，记录本机observed_at/decided_at、目标期号、方向、条件值及特征截止期号。不得只凭created_at将已经settled的旧预测认作前向。目标开奖到达后，额外验证decided_at <= 实际open_time-40s，不满足则标记`late_decision`不计投注。停机错过的期数是missed/skip，不回填虚拟成交。

使用最新最多100条已可用预测/开奖作为滚动窗口。历史暖机数据可以供未来条件计算，但不能产生暖机期间的前向投注；方向命中率与高赔退款统计分别计算。整数期号缺口重置连续计数；窗口不足N条不满足N期命中率，预测缺失不得跨期拼接连中/连挂。live重复poll同一信号/开奖不重复推进状态。

### 2.3 结算与统计

创建实验时冻结 `build_settlement_snapshot()` 的规则和赔率，存catalog hash。逐注用 `resolve_pc28_result_for_signal(rule=frozen_rule)` 和 `resolve_pc28_settled_hit_detail(snapshot=frozen_snapshot)`。未中净利=-stake，退本=0，实赔按已有函数取净利。金额用整数分累计；生成配置注额最多2位小数、有限且>0。

精度核验：现有生产 `settlement_rules.py:615` 与 `pc28_profit_rules.py:617` 均逐注 `round(stake * (odds - 1), 2)`，仓储再次按2位记账。因此OK高赔大小单双赔率完整保留 **2.846**，1元实赔命中的生产净利是 **1.85**（未舍入数学值1.846），模拟必须采用生产函数返回值再转换整数分；不能把赔率预先截成2.85，也不能独自改成累计原始1.846而与账本不一致。UI金额显示2位、赔率保留最多4位，两者不要共用金额格式器。当前schema保持整数分，不改变生产舍入政策。

每个实验有独立累计状态：settled_count、hit_count（实赔）、refund_count、miss_count、direction_hit_count、stake_total、net_profit、equity_peak、max_drawdown、current_loss_streak、max_loss_streak、trigger_count、skipped_count。ROI=100*net_profit/stake_total；空样本ROI=null。退本终止“连续亏损”但不算赢；字段解释保持一致。最大回撤从每笔按时间顺序净值更新，不能拿每日盈亏反推。sample_label采用 `<100:样本较少`、其余`仍需验证`，不能自动显示“稳定盈利”。

一个期号的decisions、outcomes、累计、日汇总、worker游标同模拟库事务提交；唯一键防重复。重启可以重算最后批次但不能重复入账。计算阶段先生成有界结果、短事务落盘；API修改状态需在worker提交前重新检查。

## 3. 文件与模块

| 文件 | 职责 |
|---|---|
| `src/pc28touzhu/domain/rule_lab.py` | 模板归一化、条件匹配、滚动特征、冻结结算、增量指标；无I/O |
| `src/pc28touzhu/services/rule_lab_store.py` | 独立SQLite schema、配置/有界查询、原子状态与结果、空间/明细保留 |
| `src/pc28touzhu/services/rule_lab_source.py` | 上游只读查询、最小字段解析、时间/三球校验、输入共享 |
| `src/pc28touzhu/services/rule_lab_service.py` | API业务入口、所有权/状态校验、生成批次、CSV |
| `src/pc28touzhu/rule_lab_worker.py` | 单进程预算、锁、live与backtest调度、可恢复推进 |
| `src/pc28touzhu/api/app.py` | 注入lab服务及认证后的路由/静态资源 |
| `src/pc28touzhu/api/ui_navigation.py` | 管理员导航 `/rule-lab` |
| `src/pc28touzhu/ui/rule_lab.{html,css,js}` | 沿用app shell的实验操作/结果页面 |
| `deploy/systemd/pc28touzhu-rule-lab.service` | 受限常驻worker（不新增timer） |
| `docs/RULE_LAB.md` | 使用方式、资源预算、数据保留、真实限制和运维 |
| `tests/test_rule_lab*.py` / `tests/test_rule_lab_ui.cjs` | 领域、数据源/存储/worker、API权限和浏览器路径 |

配置由独立模块服务构造时读取环境，可不扩展全局dataclass。默认 `RULE_LAB_DATA_DIR=<project>/data/rule_lab`；库名`rule_lab.db`，锁`worker.lock`。API测试允许显式注入临时store/source，生产HTTP不接受路径。

## 4. 数据模型

全部新增表仅在独立库，schema用PRAGMA user_version版本化、初始化幂等，无生产库迁移。JSON只用于小配置/窗口状态，禁止写原始正文。

| 表 | 主要字段与约束 |
|---|---|
| lab_batches | id PK, user_id, name, mode, predictor_id, predictor_name_snapshot, metric, template_pack, stake_cents, start_date/end_date nullable, status, phase, error, created_at, started_at, updated_at, cursor_json, processed_issues, total_issues nullable, skipped_json；user_id/id索引 |
| lab_experiments | id PK, batch_id FK, condition_key, name, condition_json, settlement_rule_id, settlement_snapshot_json, stats_json；UNIQUE(batch_id,condition_key,settlement_rule_id) |
| lab_draws | issue_no PK, source_id, open_at_utc, source_created_at, observed_at, ball0/1/2, input_hash；按issue索引共享 |
| lab_predictions | predictor_id+issue_no PK, source_id, created_at, settled_at, first_observed_at, first_status, big_small/odd_even/combo, input_hash, conflict；只存最小字段，首次值冻结 |
| lab_decisions | id PK, experiment_id, issue_no, bet_value, stake_cents, decided_at, decision_mode, feature_issue_no, condition_value_json, status(pending/settled/skipped), skip_reason, settled_at, result_type, net_cents, refund_reason；UNIQUE(experiment_id,issue_no)，状态/期号索引 |
| lab_daily | experiment_id+stat_date PK, settled_count, hit_count, refund_count, miss_count, direction_hit_count, stake_cents, net_cents, triggered_count, skipped_count |
| lab_runtime | key PK,value_json；worker心跳、阶段、最近耗时、预算暂停原因、游标版本 |

可以合并无额外行为的仓储wrapper，但不得把SQL放到HTTP处理器中。所有按id查询必须联结batch.user_id；其他管理员不能读取别人的实验。

### 数据保留

- 实验配置/累计状态长期保留；日统计先保留365天，超期可合并月统计（首版不需要实施月合并，达到预算先暂停）。
- settled/skipped decisions默认30天，仅按完成时间清理；pending始终保留。删除明细不回写减少累计。
- 精简输入默认90天，但保护活动回测游标之前所需100期暖机、pending决策所需目标和live窗口；暂停批次不能无限钉住数据，超过可保留区间恢复时报`input_expired`，不能补造。
- 清理每次最多500行，只在模拟库短事务执行；不运行VACUUM，不复制生产库，不自动写巨型CSV。导出直接流式响应或限制当前结果最多100行，无落地大文件。
- 检查目录DB/WAL/SHM/临时文件总大小，软阈值960MiB阻止新建与新回放，保留空间提交暂停状态；1GiB是应用预算，SQLite max_page_count给DB硬上限且另给WAL预留，空间耗尽必须可恢复。日志用限速journal、不输出逐期噪声，不宣称整个系统journal受1GiB预算。

## 5. HTTP契约

所有API要求现有Cookie登录且is_admin；同时按当前user.id隔离。为与既有接口一致，成功直接返回对象，错误统一`{"error":"中文原因","code":"machine_code"}`，不额外套success/data。CSV例外。无匿名数据返回。

| 方法 | 路径 | 用途 | 请求 | 成功响应 | 错误 |
|---|---|---|---|---|---|
| GET | `/api/platform/rule-lab/options` | 生成表单 | 无 | 200 Options | 401/403/503 |
| GET | `/api/platform/rule-lab` | 我的批次与状态 | 无 | 200 Overview | 401/403/503 |
| POST | `/api/platform/rule-lab/batches` | 创建暂停批次 | CreateBatch | 201 `{batch:Batch,generated_count:int}` | 400/401/403/409/503 |
| POST | `/api/platform/rule-lab/batches/{id}/state` | 暂停/恢复 | `{state:"running"|"paused"}` | 200 `{batch:Batch}` | 400/401/403/404/409/503 |
| GET | `/api/platform/rule-lab/batches/{id}` | 结果表 | 无 | 200 `{batch:Batch,experiments:Experiment[]}` | 401/403/404/503 |
| GET | `/api/platform/rule-lab/batches/{id}/experiments/{eid}` | 明细 | 无 | 200 `{experiment:Experiment,daily:Daily[],recent:Decision[]}` | 401/403/404/503 |
| GET | `/api/platform/rule-lab/batches/{id}/export.csv` | 导出当前汇总 | 无 | 200 UTF-8 BOM CSV，attachment | 401/403/404/503 |

页面 `/rule-lab`仅管理员可访问，普通用户导航不显示。静态资源`/assets/rule_lab.css`、`/assets/rule_lab.js`。页面通过auth guard显示错误但服务器仍强制认证。

### 完整字段定义

所有时间为ISO8601 UTC含Z；日期为北京时间YYYY-MM-DD。API金额以元number（两位小数），百分率number保留两位，不用格式化字符串冒充number。nullable明确返回null。

**CreateBatch**：`name:string`（trim后1..80）、`mode:"live"|"backtest"`、`predictor_id:positive int`、`metric:"big_small"|"odd_even"|"combo"`、`template_pack:"basic_v1"`（可省默认）、`stake_amount:number`（可省默认1；0.01..10000，最多2位小数）、`start_date:string|null`、`end_date:string|null`。live日期必须null/省略；backtest两日期必填，start<=end且跨度<=90天，end不能晚于今日。拒绝NaN/Infinity、布尔整数、未知字段；不能传user_id/路径/赔率覆写。生成数>100拒绝，不静默截断。

**Options**：`predictors:[{id:int,name:string}]`；`metrics:[{id:string,name:string}]`；`settlement_rules:[{id:string,name:string,big_small_odds:number,small_odd_big_even_odds:number,big_odd_small_even_odds:number,refund_note:string}]`恰好两项；`template_pack:"basic_v1"`；`default_stake_amount:1`；`limits:{max_experiments_per_batch:100,max_running_experiments:100,max_total_experiments:500,max_backtest_days:90,detail_retention_days:30,disk_budget_bytes:1073741824,cpu_quota_percent:20,memory_max_bytes:268435456}`；`source_available:bool`、`source_message:string`。max_running/total均全服务全用户共享。

**Overview**：`batches:Batch[]`（最新最多50个，超过拒绝创建以免隐藏）、`worker:Worker`、`storage:{used_bytes:int,budget_bytes:int,detail_retention_days:int}`。

**Worker**：`state:"starting"|"idle"|"running"|"stale"|"resource_paused"|"error"`、`heartbeat_at:string|null`、`message:string`、`last_tick_ms:number|null`、`processed_issues:int`。超过60秒未心跳显示stale，不等同于规则暂停。心跳首次不存在显示starting。

**Batch**：`id:int,name:string,mode:string,predictor_id:int,predictor_name:string,metric:string,template_pack:string,stake_amount:number,start_date:string|null,end_date:string|null,status:"paused"|"running"|"completed"|"error"|"resource_paused",phase:"idle"|"warming"|"observing"|"importing"|"replaying",experiment_count:int,processed_issues:int,total_issues:int|null,created_at:string,started_at:string|null,updated_at:string,error:string,skip_counts:object<string,int>,history_notice:string`。history_notice仅backtest填预测版本限制，live为空字符串。

**Experiment**：`id:int,batch_id:int,name:string,condition:{type:string,metric:string,operator:"gte"|"lte"|null,threshold:number|null,window_size:int|null,bet_value:string|null},settlement_rule_id:string,settlement_rule_name:string,settled_count:int,hit_count:int,refund_count:int,miss_count:int,direction_hit_count:int,stake_total:number,net_profit:number,roi_percent:number|null,max_drawdown:number,max_loss_streak:int,trigger_count:int,skipped_count:int,sample_label:string`。

**Daily**：`date:string,settled_count:int,hit_count:int,refund_count:int,miss_count:int,stake_total:number,net_profit:number`，最多最近90天、日期升序。

**Decision**：`issue_no:string,decided_at:string,settled_at:string|null,bet_value:string,stake_amount:number,status:"pending"|"settled"|"skipped",result_type:"hit"|"refund"|"miss"|null,net_profit:number|null,skip_reason:string,refund_reason:string`，最近50条、时间降序。

CSV列固定：experiment_id,name,condition,settlement_rule,settled_count,hit_count,refund_count,miss_count,stake_total,net_profit,roi_percent,max_drawdown,max_loss_streak,trigger_count,skipped_count。转义逗号/引号/换行，字符串若以`=+-@`或控制字符开头前缀单引号，避免公式注入。

### 错误码

| code | HTTP | 场景 |
|---|---|---|
| invalid_input | 400 | 字段、日期、状态、候选数无效 |
| unauthorized | 401 | 未登录 |
| forbidden | 403 | 非管理员 |
| not_found | 404 | 批次/实验不存在或属于其他用户 |
| capacity_exceeded | 409 | 全局配置数/运行数达到上限 |
| invalid_transition | 409 | completed不能继续；paused→paused可幂等成功 |
| input_expired | 409 | 必需输入已清理；须新建批次 |
| storage_limit | 503 | 空间预算不足 |
| source_unavailable | 503 | 源库不可读；不暴露绝对路径或SQL |
| store_busy | 503 | 模拟库短暂写锁，客户端可重试 |

## 6. 资源与部署

系统服务单worker、启动flock；`CPUQuota=20%`是0.2核（本4核机器约总算力5%），`MemoryHigh=192M`、`MemoryMax=256M`、`Nice=10`、`IOSchedulingClass=idle`、日志速率限制。输入连接每页<=200行、实验每批<=100，全局运行<=100，保留总实验<=500；单次工作预算最多2秒计算，yield后再继续。失败重试带有界退避，API线程不能调用回测入口。

worker每15秒一次live采集；空闲间隙处理一页backtest，live优先。不要新增计划任务timer。源缺失/锁忙只报告并重试，不写生产库；内存硬限制可能终止进程，原子checkpoint保证重启不重账。磁盘/内存压力暂停新的历史批次，pending结算保留小额预算，不使用无限积压队列。

**上游WAL访问边界：** worker允许上游源目录 `/www/wwwroot/AITradingSimulator` 在其systemd namespace中可写，以兼容SQLite `-wal`/`-shm` 的创建与回收；同时通过显式 `ReadOnlyPaths=/www/wwwroot/AITradingSimulator/pc28_predictor.db` 把主数据库文件绑定为只读。应用连接始终使用`mode=ro`及`PRAGMA query_only=ON`，禁止网络访问，源项目`.env`不可读。这不是操作系统级“仅两个辅助文件可写”的精确白名单：源目录写权限确实比两个文件宽，文档和验收不能声称整个源目录只读，也不能声称应用只读能替代文件系统权限边界。该安排不改上游代码、配置或业务数据，不新增常驻进程、不复制大数据库。

验收应在实际systemd namespace内以服务用户验证：WAL辅助文件存在和不存在时均能读取有效数据；主文件写入被拒绝；`.env`读取和网络访问被拒绝；源读取连接本身拒绝SQL写操作。若仍失败，停止报告失败原因，不能回退到`immutable`、`nolock`、可写源连接或解除主文件只读绑定。

部署前先用临时模拟库与源fixture跑测试，再只读生产源执行少量解析/性能检查。提供systemd service文件与启动说明；启用新模拟worker属于用户已授权功能，但仅生成paused实验，不能自动改实盘规则。无主库迁移，只需重启平台加载新UI/API；现有执行器与其他服务不必重启。

## 7. 风险、证据与验收映射

| 风险触发条件 | 应对/测试 | 对应需求 |
|---|---|---|
| 预测在开奖后才被采集、或停机补历史 | live只冻结pending且目标未开，结算验证40秒；测试早/晚/停机/重复poll | R3 |
| 源时间UTC与北京时间混用 | 显式解析；跨日/跨月fixture | R3/R5 |
| 缺期、晚结算历史特征、输入冲突 | reset/不足跳过，settled_at门控；不能拼接 | R1/R3 |
| 高赔命中遇13/14/对子/顺子/豹子 | 枚举测试与已有函数一致；只命中退本、未中全亏 | R2 |
| 中途崩溃/重复回放/暂停恢复 | 事务与唯一键；注入提交前失败及重试，累计/日汇总相等 | R4/R5 |
| 大量候选/反复点击 | 全局上限、确定性包、短API；跨用户并发创建/启动测试 | R4/R7 |
| 其他用户猜id、前端路径注入 | 管理员+所有权+严格字段白名单；无源路径API | R7 |
| 库增长、清理与活动任务竞争 | quota含WAL、保护pending、批量删除；预算边界/保留期测试 | R4/R8 |
| 查询源活库阻塞主业务 | 索引有界分页、短只读事务、超时；查询计划与实测耗时 | R4 |
| WAL辅助文件被回收后目录只读导致读取失败，或部署权限描述失真 | 源目录可写以支持辅助文件生命周期、主文件显式只读绑定；应用ro/query_only、网络禁止、.env不可读；真实namespace验证各边界，明确目录可写范围较宽 | R7 |
| 页面成果不完整 | 生成→启动→暂停→详情→导出浏览器E2E；空/错误/窄屏 | R6 |

首版必须记录50/100/500候选的离线fixture耗时和峰值RSS，真实只读输入小样本解析；不能用理论20万判断代替性能证据。对大规模历史回放不承诺即时完成，页面显示真实进度、暂停原因和更新时间。

## 变更记录

- 2026-11-26：根据用户已接受方案与最新两套OK预设限制制定首版；技术范围固定为单腿固定金额，明确前向证据、全局预算与管理员权限。
- 2026-11-26评审修订：公开级别采用records/analysis白名单；三球与result_number交叉校验；哈希排除合法状态变化；历史开奖以source_created_at门控；无日期索引使用期号keyset后筛选。核实生产逐注金额round2，保留完整赔率精度。
- 2026-11-26部署修订：依据真实WAL连接行为允许源目录创建/回收SQLite辅助文件，主数据库文件仍由ReadOnlyPaths绑定只读，应用保持ro/query_only，网络与.env访问禁止；如实注明源目录权限不是仅aux文件的OS白名单。仅调整部署安全边界与相关验证，不改变业务/API/schema。
