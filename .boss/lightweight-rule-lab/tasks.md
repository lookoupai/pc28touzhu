# 轻量规则实验室开发任务

## 摘要

- 按 W1 核心领域和账本 → W2 来源/运行与 API/UI → W3 资源验证与发布执行。开发者先写行为红测，再实现并过绿门禁。
- W1 由 **core** 独占领域/仓储及对应测试；W2 的 **runtime**、**api**、**frontend** 写集互斥，可在 W1 验收后并行；**ops** 与 **qa** 负责最终资源与验收证据。
- Blast Radius：高（新增独立数据库、API/导航、worker部署配置及超过10个文件），但无生产主库迁移、无依赖安装、无上游修改、无实盘配置写入。
- 风险确认触发项：多文件/API入口/部署配置已命中；本轮用户“可以……没有的话就可以做”授权已由主代理核对覆盖此可逆独立模拟实现，**授权已满足，不重复请求**。任何新增实盘写入/消息发送不在本任务范围。
- 高复杂度集中在时间可见性、事务与worker；任务数7组，预估实现/核验工具调用约50～90次。首版严格使用固定金额单腿与两套OK预设。

## 1. Repo Preflight

| 项目 | 已确认事实 / 证据 |
|---|---|
| Git | 两个生产主目录 clean，fetch后 origin/main...HEAD 为0/0；旧worktree发布记录与已备份版本相同。开发目录 `/www/wwwroot/pc28touzhu-rule-lab`，分支 `feat/lightweight-rule-lab`，基线 `1209e3b` |
| 上游 | `/www/wwwroot/AITradingSimulator`，基线 `5562150`；只读 `pc28_predictor.db`，15个当前合格公开预测器 |
| 技术栈 | Python标准库WSGI、SQLite、原生HTML/CSS/JS；`pyproject.toml`无新增运行依赖 |
| CI | `.github/workflows/docker-image.yml`只构建发布Docker，不能替代功能测试 |
| 基线 | `PYTHONPATH=src .../.venv/bin/python -m unittest discover -s tests -p 'test_*.py' -q`：843项通过、153.587秒；`/tmp/pc28-rule-lab-baseline.log` |
| 测试工具 | unittest；coverage位于`/tmp/pc28-cycle-test-tools`；Playwright Core位于`/tmp/pc28-cycle-ui-tools/node_modules`；Chromium位于`/root/.cache/ms-playwright/chromium-1243/chrome-linux64/chrome`。不安装依赖 |
| HTTP/认证 | `api/app.py`单线程WSGI，已有Cookie管理员检查；新增API必须admin且按batch.user_id隔离；API不可运行回测 |
| 结算常量 | 仅`pc28_netdisk_regular`、`pc28_high_regular`；生产函数逐注round2，完整赔率2.846不预先截断 |
| DB与索引 | 上游draw唯一索引(lottery_type,issue_no)，prediction唯一索引(predictor_id,issue_no)；无日期索引，采用期号keyset分页后筛日期；活库不使用immutable |
| 迁移 | 仅独立lab库schema初始化与user_version；不调用生产主库迁移，不改清理策略 |

所有写集外变更先向主代理报告并更新owner；依赖/锁文件变化必须停止检查。公共HTTP完整schema见architecture §5，本文只补内部Python契约。

## 2. 跨模块内部契约（W1冻结）

采用标准库dict/list，不引入DTO框架。时间字符串统一UTC ISO8601含Z，函数参数`now`/`decision_at`均同格式；期号是十进制字符串，按数值递增，源只接受当前同长度有效期号区间。核心模块可以添加私有helper，但不能改下列公共行为而不通知依赖owner。

### 2.1 Domain：`pc28touzhu.domain.rule_lab`

```python
ALLOWED_SETTLEMENT_RULE_IDS = ('pc28_netdisk_regular', 'pc28_high_regular')
METRICS = ('big_small', 'odd_even', 'combo')
def normalize_batch_payload(payload: dict, *, now: str) -> dict: ...
def build_conditions(metric: str) -> list[dict]: ...
def build_experiments(metric: str) -> list[dict]: ...
def build_features(metric: str, history: list[dict], *, decision_at: str) -> dict: ...
def match_condition(condition: dict, features: dict) -> dict: ...
def settle_bet(snapshot: dict, metric: str, bet_value: str,
               stake_cents: int, triplet: list[int]) -> dict: ...
def empty_stats() -> dict: ...
def apply_outcome(stats: dict, outcome: dict, *, stake_cents: int) -> dict: ...
```

- `normalize_batch_payload`按CreateBatch白名单验证，返回内部Config：`name,mode,predictor_id,metric,template_pack,stake_cents,start_date,end_date`；默认stake=100分，日期格式/范围/未来/NaN/bool等按架构。错误使用`ValueError`，service转`invalid_input`。
- Condition具有HTTP定义的全部字段（不适用填null），外加稳定`condition_key`和`name`；类型`always/miss_streak/hit_streak/draw_streak/draw_absence/hit_rate`。25/39个条件，无条件基准第一条；二元absence归一化去重。
- ExperimentDefinition：`condition_key,name,condition,settlement_rule_id,settlement_rule_name,settlement_snapshot`。两种规则对应同一条件相邻；每批50/78个定义，完整冻结snapshot。
- HistoryRow：`issue_no,open_at,draw_available_at,triplet,prediction_created_at,prediction_settled_at,prediction_values,draw_conflict,prediction_conflict`。`prediction_values`为`{big_small,odd_even,combo}`，方向可null；`build_features`也接受单一`prediction_value`（提供时优先，供单metric fixture）。冲突字段分离：纯开奖特征仅受draw_conflict阻断，预测命中特征受开奖或预测任一冲突阻断，不能因预测修订破坏无冲突的实际连开统计。兼容旧`conflict`字段，每个显式字段缺失时分别回退到`row.get('conflict',False)`；新store返回两种显式标志及旧合并字段。draw_available_at取源created_at；所有history都必须在目标之前，调用者传入最多100行（可包含不可用/缺预测记录作连续性屏障）。此结构保持get_history无需metric参数。
- FeatureSnapshot：`metric,latest_issue_no,hit_streak,miss_streak,draw_streaks,draw_absences,rates,gap_detected`。`draw_streaks/absences`为方向→整数；`rates`为字符串`20/50/100`→`{sample_count:int,hit_count:int,rate:float|null}`。晚公布的draw或晚created/settled的prediction不进入对应特征，且不能越过它们拼接连续期。N期窗口不足N条则rate=null。纯开奖连续性不因缺预测而清零，预测连续性必须清零。目标结果永不参与。
- Match：`{matched:bool,value:number|null,reason:string}`；样本不足理由`insufficient_history`，正常未满足理由`not_matched`；不得因此生成逐次明细。
- Settlement：`{result_type:'hit'|'refund'|'miss',direction_hit:bool,net_cents:int,refund_reason:str}`；必须同时使用冻结rule判断退本与冻结snapshot算净利，不能读取新目录替代。调用生产round2结果转分。
- Stats内部整数分字段：`settled_count,hit_count,refund_count,miss_count,direction_hit_count,stake_cents,net_cents,equity_peak_cents,max_drawdown_cents,current_loss_streak,max_loss_streak,trigger_count,skipped_count`。`apply_outcome`只累加结算相关计数和资金，不增加trigger_count/skipped_count；不原地修改传入stats。零净利结束连续亏损。trigger/skipped由store幂等更新。

### 2.2 Store：`pc28touzhu.services.rule_lab_store`

```python
class RuleLabError(Exception):
    # __init__(code: str, message: str, status: int = 400)
    # 对外属性code/message/status；不得携带源库路径或SQL。
    ...
class RuleLabStore:
    def __init__(self, db_path=None, *, budget_bytes=1073741824,
                 detail_retention_days=30): ...
    def initialize(self): ...
    def create_batch(self, user_id, config, predictor_name, experiments, *, now): ...
    def set_batch_state(self, user_id, batch_id, state, *, now): ...
    def list_batches(self, user_id): ...
    def get_batch(self, user_id, batch_id): ...
    def list_experiments(self, user_id, batch_id): ...
    def get_experiment_detail(self, user_id, batch_id, experiment_id): ...
    def storage_status(self): ...
    def worker_status(self, *, now): ...
    def set_worker_status(self, payload, *, now): ...
    def list_work_batches(self): ...
    def get_work_item(self, batch_id): ...
    def get_history(self, predictor_id, *, before_issue, limit=100): ...
    def get_pending_decisions(self, batch_id, *, limit=200): ...
    def commit_step(self, batch_id, expected_revision, *, cursor, now,
                    draws=(), predictions=(), decisions=(), outcomes=(),
                    progress=None): ...
    def prune(self, *, now, limit=500): ...
```

构造默认解析`RULE_LAB_DATA_DIR`或项目`data/rule_lab`；测试总是显式传临时db。`initialize`幂等、不接触主库。公开查询返回architecture Batch/Experiment/Daily/Decision schema；`get_experiment_detail`返回`{experiment,daily,recent}`。`create_batch/set_batch_state`返回Batch；`storage_status`返回Overview.storage；`worker_status`返回Worker。所有用户方法owner联结验证，不存在/他人资源统一404。总实验500、运行100、每用户批次50均在写事务原子检查，不能靠service预查保证。

`get_work_item`内部返回`{batch,experiments}`：batch含`id,user_id,mode,predictor_id,metric,stake_cents,start_date,end_date,status,revision,cursor,processed_issues,skip_counts,created_at,started_at`；实验含`id,batch_id,condition,settlement_snapshot,settlement_rule_id,stats`。`list_work_batches`返回最多50个running或存在pending的batch内部摘要，live优先，paused/resource_paused/error有pending仍可结算。`revision`在状态修改和成功推进时递增。`get_history`返回升序HistoryRow且排除before_issue，保留缺失预测屏障。

共享缓存输入：

- DrawRecord：`issue_no,source_id,open_at,source_created_at,observed_at,triplet,input_hash`。
- PredictionRecord：`predictor_id,issue_no,source_id,created_at,settled_at,observed_at,status,big_small,odd_even,combo,input_hash`。
- 缓存哈希只包括不变方向/号码/来源创建时刻；status/settled_at合法推进不改变哈希。预测方向冲突不可覆盖首次值，设置conflict；**新决策、历史回放及后续特征**遇到已知冲突才跳过。**已在开奖前冻结的live决策不因后来预测修订而撤销或改为skipped**，仍按原Decision.bet_value及冻结结算配置结算，防止按事后信息选择性剔除交易。pending→settled允许合法补充。
- 开奖三球发生冲突时必须隔离并报告数据错误，不得用新号码覆盖首次记录；受影响的未结算决策保持待处理，不能静默选一份号码结算或视为零盈亏。已入账结果保留，禁止自动重算或撤销旧账；人工核实前不继续使用冲突开奖产生新的条件/结算。

DecisionInput：`experiment_id,issue_no,bet_value,stake_cents,decided_at,decision_mode,feature_issue_no,condition_value`。初始pending；唯一(experiment_id,issue_no)。OutcomeInput：`experiment_id,issue_no,status:'settled'|'skipped',settled_at,result_type,net_cents,direction_hit,refund_reason,skip_reason`；skipped时result_type=null/net_cents=null/direction_hit=false。Settlement转换由worker完成，store验证状态和类型后合并。

`commit_step`是唯一批量业务写入口，事务内完成缓存、新决策、pending→终态、累计/日汇总、cursor/progress与revision。返回`{committed:bool,reason:str,inserted_decisions:int,settled:int,skipped:int}`。revision不符时整批不写，返回`stale_revision`；重放相同已完成决策不重账。batch不是running时禁止decisions，已有pending outcomes仍可写（用重新加载的revision）；禁止旧progress把API暂停覆盖为running。progress允许`phase,processed_issues_delta,total_issues,skip_counts_delta,status,error`，status仅worker可设completed/error/resource_paused；无条件覆盖running禁止。日统计按结果settled_at北京时间归属；backtest传目标实际开奖时刻，不能全部记回放当天。cursor不透明小dict，由runtime拥有语义，store只事务保存。

长期cursor/累计保留，明细清理后不允许旧cursor倒退导致重账；commit拒绝同batch cursor中`last_issue_no`倒退。重复批次试算可以新建batch，不重置旧账。每条终态decision另保存内部`finalized_at=本次入账now`；每日统计仍按`settled_at`的开奖归属计算，但30天明细保留按finalized_at计，避免今天回放旧数据后立即被清理。worker有剩余时间预算时每5分钟调用一次prune，单次跨明细/预测/开奖合计最多500行。prune只删已完成明细/过期且不被pending、最近100期和活动回测暖机保护的输入；若无法证明输入可删则保留并预算暂停。prune不修改累计，不执行VACUUM；API状态和worker心跳在预算软限额时保留少量写入余量。错误码遵循architecture §5。

## 3. W1任务（可先派发）

### T1 [BE] 领域模板、特征与冻结结算

| 文件 | 操作 | owner | 风险 |
|---|---|---|---|
| `src/pc28touzhu/domain/rule_lab.py` | 新增 | core | 独占 |
| `tests/test_rule_lab_domain.py` | 新增 | core | 独占 |

先写测试验证两玩法50/组合78、二元去重、缺期/晚到/不足窗口、目标不可见、冻结赔率与退本、金额round2、回撤逐注和连续亏损；红测后实现。与生产函数对照三球特殊情况及所有八种方向；改变目录后冻结结果保持不变。复杂度中；无依赖。

### T2 [BE] 独立账本、状态与幂等检查点

| 文件 | 操作 | owner | 风险 |
|---|---|---|---|
| `src/pc28touzhu/services/rule_lab_store.py` | 新增 | core | 独占；跨模块仓储契约 |
| `tests/test_rule_lab_store.py` | 新增 | core | 独占 |

依赖T1；同core串行实现。使用临时SQLite验证初始化、owner隔离、全局额度并发、状态revision竞态、提交前失败回滚/重试、重复结算、paused pending结算、合法状态补充与输入冲突、日汇总/累计一致、清理不改变成绩、磁盘预算（含WAL）和busy错误。公开API与内部方法使用§2精确字段。复杂度高。

W1验收：实际运行waves.json中的wave-1绿门禁；主代理核对diff无依赖/主库/上游改动，再开放W2。W1仅允许上述4文件写入。

## 4. W2跨模块契约

### 4.1 Source：`pc28touzhu.services.rule_lab_source`

```python
class RuleLabSource:
    def __init__(self, db_path=None, *, timeout_seconds=0.25): ...
    def list_predictors(self): ...
    def get_predictor(self, predictor_id): ...
    def latest_draw(self, *, observed_at): ...
    def get_draw_by_issue(self, issue_no, *, observed_at): ...
    def find_start_issue(self, start_date, *, observed_at): ...
    def draws_page(self, *, after_issue='', before_issue=None,
                   limit=200, observed_at): ...
    def history_draws(self, *, before_issue, limit=100, observed_at): ...
    def get_predictions(self, predictor_id, issue_nos, *, observed_at): ...
```

默认路径由服务端`RULE_LAB_SOURCE_DB`或架构指定上游路径决定，HTTP不接受路径。构造不读取生产数据，调用才打开mode=ro/query_only短连接，设置250ms忙超时、查询进度截止与小缓存；每次读取后立即关闭。所有字段显式选择，禁止SELECT *和凭证字段。

- predictor公开记录`{id:int,name:str}`；资格为enabled且pc28且share_predictions且share_level属于records/analysis。`get_predictor`不符合时返回None，`list_predictors`仅合格列表。
- `latest_draw/get_draw_by_issue`返回§2 DrawRecord或None；缺失返回None，最新记录存在但格式不合法时抛安全`RuleLabError(source_unavailable,...)`，不能悄悄以更早一期冒充最新。`get_draw_by_issue`为pending结算按唯一索引查询，查询次数按目标期共享，不能每实验查一次。
- `find_start_issue(start_date,*,observed_at)`返回目标北京时间日期起点及之后最早可用期号字符串，无匹配返回None。使用现有issue索引进行有界二分与邻近seek，最多64次单行查询，共用一次250ms查询截止；缺号时通过索引取得邻近实际行推进边界，不能在缺号区间无限重试。依赖PC28期号与open_time单调递增，已发现日期/期号非单调或时间无效时显式报错拒绝，不以错误起点继续回放；不做日期WHERE全表扫描或CAST排序。二分有界探测不能冒充对全历史单调性的完整验证。
- `draws_page/history_draws`返回Page：`{items:DrawRecord[],next_issue:str|null,scanned_count:int,has_more:bool,skip_counts:dict}`。先索引分页再解析，最多200行；next_issue反映源页最后扫描的期号，包含解析失败行，防止卡死；warm历史返回升序且只含before_issue之前至多100个源行。解析错误计原因，缺期在特征层可见。
- `get_predictions`每次最多200个已知issue，返回`{items:PredictionRecord[],skip_counts:dict}`，按(predictor_id,issue_no)索引，空输入不查库。方向为空可合法存null，非法方向/时间记录skip；状态pending/settled为不同合法阶段。UTC与北京时间转换规则见架构。
- input_hash由source计算、store复核或信任格式后冻结；哈希不含status/settled_at/observed_at。保留created_at不变值。源row mutation冲突由store标记，不能覆盖。

### 4.2 Worker：`pc28touzhu.rule_lab_worker`

```python
class RuleLabWorker:
    def __init__(self, store, source, *, max_tick_seconds=2.0,
                 page_size=200): ...
    def tick(self, *, now=None) -> dict: ...
def main(): ...
```

`tick`显式注入store/source；now省略才取当前UTC。返回`{processed_issues:int,inserted_decisions:int,settled:int,skipped:int,state:str,message:str,elapsed_ms:float}`。生产main才构建默认实例、flock、短批处理/有界退避和低噪声日志；live采集频率15秒，历史回放有工作时每短批让出0.25秒后可续跑，两者分别调度，不能让历史每页固定等15秒，也不能让历史续跑把live轮询加速为每0.25秒。不得import派发器或发消息，不创建主业务仓储。

顺序：结算所有batch已有pending → 正在运行live观察 → 运行backtest下一页；每个小步骤重查revision。共享同一期的draw/同来源prediction/features缓存到本tick局部对象。单tick预算不能被大列表循环绕过；至少每期/批间检查monotonic截止，未完成下次继续。已暂停pending清算不受创建运行额度拦截。

前向：以最新已公布完整draw的issue+1查询pending预测；先同步并读取最多100历史缓存，按当前decision_at计算；只冻结当前观察到且尚未开奖的目标，source历史settled绝不补造live决策。用上期时间+210秒的保守截止作初筛，时钟缺失拒绝；结算时必须再次用目标实际open_at-40秒验证。方向在首次decision冻结，不被后续source修订替换，也不因后续预测冲突而撤销该决策；已有live pending仍按原bet_value结算。预测冲突门控只适用于尚未冻结的新决策和历史回放输入；开奖三球冲突按§2.2隔离报错，不静默改写旧账。暂停恢复从新的当前目标继续，旧pending仍清算。

历史：创建/首次运行固定回放上界，用find_start_issue索引定位开始日期，再仅加载该起点前最多100期暖机；不能从全年最早一期顺扫到近7天起点。无匹配时明确完成为无可用数据。定位后沿draw期号keyset小页导入，内存按北京时间日期过滤。历史决策时点目标open_at-40秒，目标预测created_at需更早；历史特征同时检查draw.source_created_at、prediction.created_at/settled_at，不把当前目标结果送入build_features。暖机不计收益。遇源缺少、非法三球、迟到预测或不足窗口，计skip；只有真正匹配才写Decision。符合的回放Decision可在同commit_step提交Outcome，settled_at用开奖时刻，日汇总不记到运行当天。扫描到固定上界/结束日期后completed；源库不足报告实际覆盖和skip，不创造数据。

cursor由runtime负责，小型JSON（不含大正文）：`version=1,scan_after_issue,last_issue_no,history_floor_issue,replay_max_issue`；live另有`last_observed_draw_issue,last_decided_issue`。不同模式不使用字段可省略；last_issue_no只前进、不重置。history_floor_issue标明活动工作需要的100期暖机最早值，store清理用它保护输入。批次每次阶段/游标变化通过commit_step提交，不能在SQL或第二事务中单独改统计。store.get_history不足而源保留已不足时显式报告，不能用零命中率继续。

Worker需要任意已缓存目标时可使用source.get_draw_by_issue（只读索引）；缓存查询均通过store现有get_history/get_pending_decisions/get_work_item，不在worker直接写SQL。若需额外有界store方法，runtime先向core提出签名并更新本文，禁止私下改变已冻结返回结构。

### 4.3 Service与WSGI：`pc28touzhu.services.rule_lab_service`

```python
class RuleLabService:
    def __init__(self, store=None, source=None): ...
    def options(self, *, now=None): ...
    def overview(self, user_id, *, now=None): ...
    def create_batch(self, user_id, payload, *, now=None): ...
    def set_batch_state(self, user_id, batch_id, payload, *, now=None): ...
    def batch_detail(self, user_id, batch_id): ...
    def experiment_detail(self, user_id, batch_id, experiment_id): ...
    def export_csv(self, user_id, batch_id): ...
```

方法返回architecture §5成功对象；export_csv返回UTF-8 BOM的bytes。create返回`{batch,generated_count}`，set返回`{batch}`，batch_detail返回`{batch,experiments}`。服务层做严格请求验证、source资格检查、构造定义、CSV转义；SQL只在store。禁止任意服务端文件路径/user_id/结算规则override从payload透传。source不可用时options按契约可返回source_available=false及空预测列表；其他操作按明确错误状态返回，不暴露路径。

WSGI在PlatformApiApplication构造器末尾增加可选`rule_lab_service=None`注入；生产访问实验端点时再懒构建，旧页面/既有测试不初始化模拟库。既有Cookie认证后新增admin校验、当前user.id传service。RuleLabError按status/code/message返回，普通ValueError转400 invalid_input；不改变旧接口异常行为。HTTP方法/路径严格按architecture，不接受任意DELETE/PUT或批次owner。静态资源与导航只由api owner修改；`/rule-lab`页面本身也必须管理员认证。api owner负责已有导航回归预期调整，不能删掉原有路由覆盖。

实现后的有界请求约束：lab GET不接受query字段，POST正文最大16KiB，超出均400 invalid_input；页面不发送未定义分页/过滤参数。启动前核验公开源资格；暂停不依赖源可用性，源故障时仍能停止新模拟。

## 5. 后续任务与写集

### T3 [BE] 只读来源与单worker

| 文件 | 操作 | owner | 风险 |
|---|---|---|---|
| `src/pc28touzhu/services/rule_lab_source.py` | 新增 | runtime | 独占 |
| `src/pc28touzhu/rule_lab_worker.py` | 新增 | runtime | 独占 |
| `tests/test_rule_lab_source.py` | 新增 | runtime | 独占 |
| `tests/test_rule_lab_worker.py` | 新增 | runtime | 独占 |

依赖T1/T2通过。先写只读源fixture测试、再写worker虚拟时钟测试：正常及异常时间/三球、分享白名单、SQL只读与分页计划、pending转settled、live迟到/重启/暂停/重复poll、历史未来泄漏、缺期、source锁忙/不可用、两批次共用源、每tick预算和恢复。测试不得访问生产或外网。复杂度高。

### T4 [BE] API与入口集成

| 文件 | 操作 | owner | 风险 |
|---|---|---|---|
| `src/pc28touzhu/services/rule_lab_service.py` | 新增 | api | 独占 |
| `src/pc28touzhu/api/app.py` | 修改 | api | 共享入口，唯一owner |
| `src/pc28touzhu/api/ui_navigation.py` | 修改 | api | 导航注册，唯一owner |
| `tests/test_rule_lab_api.py` | 新增 | api | 独占 |
| `tests/test_ui_navigation.py` | 修改 | api | 导航回归，唯一owner |

依赖T1/T2；与T3并行时使用固定Source契约fixture，不能去改source文件。真实WSGI测试：匿名401、普通用户403、不同管理员404；创建201与50/78实存；运行/暂停/重复状态、总额度；未知字段/NaN/bool/日期/注额校验；详情/日汇总/CSV公式与XSS文本；请求不调用worker。复杂度中。

### T5 [FE] 实验页面与浏览器流程

| 文件 | 操作 | owner | 风险 |
|---|---|---|---|
| `src/pc28touzhu/ui/rule_lab.html` | 新增 | frontend | 独占 |
| `src/pc28touzhu/ui/rule_lab.css` | 新增 | frontend | 独占 |
| `src/pc28touzhu/ui/rule_lab.js` | 新增 | frontend | 独占 |
| `tests/test_rule_lab_ui.cjs` | 新增 | frontend | 独占 |
| `tests/rule_lab_ui_server.py` | 新增 | frontend | 独占 |

依赖UI两份契约与HTTP schema；可与T3/T4并行写文件，红测先在临时fixture运行失败；最终浏览器必须通过实际WSGI API，不以纯mock代替集成验收。沿用shell、token与管理权限；创建默认暂停，明确两套OK/样本量/固定金额，结果成对比较，时间北京时间，明细只有最近50条，不伪造曲线/指标。测试覆盖生成→启动→暂停→结果→详情→下载，401/403/错误恢复、未知结果不自动重提、恶意名称安全、手机无溢出、键盘焦点。复杂度中。

### T6 [SHARED] 资源服务与运维

| 文件 | 操作 | owner | 风险 |
|---|---|---|---|
| `deploy/systemd/pc28touzhu-rule-lab.service` | 新增 | ops | 部署配置，唯一owner |
| `docs/RULE_LAB.md` | 新增 | ops | 独占 |
| `tests/test_rule_lab_ops.py` | 新增 | ops | 独占 |
| `.boss/lightweight-rule-lab/deploy-report.md` | 新增 | ops | 正式部署证据 |

依赖T3可运行、W2最终绿门禁。unit必须单worker、CPUQuota20%、MemoryHigh192M/Max256M、Nice10、IO idle、Restart有界与限速journal；无timer、无生产写路径权限扩大。文档解释每实验单位、两套OK、paused创建、历史限制、0.2核含义、目录1GiB不含共享journal、恢复和保留策略。只读实源小样本验证在QA通过后进行；是否启动/生产重启按主代理授权流程，不能自主重启其他业务服务。未部署时报告准确标“待部署”，不冒充成功。复杂度中。

### T7 [SHARED] 独立QA与资源测量

| 文件 | 操作 | owner | 风险 |
|---|---|---|---|
| `tests/test_rule_lab_qa.py` | 新增 | qa | 独占，独立边界回归 |
| `tests/rule_lab_benchmark.py` | 新增 | qa | 有界离线性能脚本 |
| `.boss/lightweight-rule-lab/qa-report.md` | 新增 | qa | 正式验收证据 |

依赖W2；QA只补缺失高风险用例，不复刻开发测试。复核结果账户不串用、目录冻结、退本/输钱、过去数据不可冒充前瞻、明细清理后游标不倒退、余额日汇总一致、状态与磁盘/内存预算。离线50/100/500实验测耗时、RSS、DB增长；500为保存/离线计算规模，不绕过100运行上限。运行新增模块coverage、整个原有unittest、浏览器E2E。负载测试仅临时数据库；实源仅小样本只读解析，不全库扫描压测。若发现实现bug，反馈对应owner修复，qa不越界改业务文件。复杂度中。

## 6. 并行安全组与依赖

| 组 | 任务 | 前置 | 写集约束 |
|---|---|---|---|
| G1 | T1 → T2 | 设计评审已过 | 同core串行，其他人只读4个文件 |
| G2 | T3、T4、T5 | W1已过且§4契约冻结 | runtime/api/frontend互不写同文件；API注册由api独占 |
| G3 | T6、T7 | W2已过 | ops只unit/doc/ops test/report；qa只独立test/benchmark/report |

若W2发现W1需修正，由core修正其owner文件，暂停依赖该改动的测试直到接口重新确认；不能由runtime/api直接修改store。最终集成修复仍按原owner分配。主代理每波检查实际git diff与依赖清单，无意外变更后推进。

## 7. Contract Matrix

| ID | Contract | UI/Copy | Client Payload | Server Schema | Persistence | Business Rule | Test Evidence |
|---|---|---|---|---|---|---|---|
| CM-1 | 两套OK配对模板 | 50/78版本，固定两套 | metric/template_pack | 唯一两rule ID | 实验definition+snapshot | 二元absence去重，基准首条 | domain+API+browser |
| CM-2 | 正确结算 | 实赔/退本/全亏分列 | stake_amount | 2位stake，完整赔率 | 整数分stats/daily | 复用冻结生产round2 | domain+QA |
| CM-3 | 时间与前瞻 | live/历史明确分开 | mode/dates | 已知时间UTC | decided_at/feature_issue | live先冻结、迟到skip、历史门控 | domain+worker |
| CM-4 | 状态与幂等 | 保存暂停、可恢复 | state | revision/allowed states | 原子cursor+decisions+stats | 重复不重账，paused pending可结 | store+worker+API |
| CM-5 | 权限隔离 | 管理员入口 | 无user_id/path | admin+owner | batch.user_id | 跨管理员也404 | API+browser |
| CM-6 | 页面闭环 | 批次/结果/明细/CSV | 架构字段 | 直接对象，创建201 | 有界结果查询 | UI不触发回测计算 | API+browser |
| CM-7 | 资源与清理 | 真实状态/存储预算 | 无任意大枚举 | 100运行/500保存 | quota/WAL/retention | 单worker、1GiB、清理不损累计 | store+worker+ops+benchmark |
| CM-8 | 源安全与共享 | 合格来源列表 | predictor_id | records/analysis白名单 | 最小三球/方向缓存 | 源只读/索引分页/状态合法推进 | source+worker+QA |

## 8. Evidence Wave与停止条件

实际命令以`waves.json`的argv数组为准；所有红测先创建测试并确认因功能缺失而失败，不能把语法/环境错误当合格红测。W1数据/幂等失败禁止进入依赖实现；W2任一权限、未来泄漏、结算或浏览器主路径失败必须修复；W3资源硬限制、回归或真实源解析失败不得发布。已知旧基线问题只能有明确基线复现，不可口头豁免。没有运行的检查在报告明确标明未运行，不从843项旧基线推断新增功能正确。

## 9. 验证状态（2026-11-26，发布前）

| Wave | 已完成验证 | 状态 |
|---|---|---|
| W1 | 主代理当时独立执行56项通过；后续冲突、清理、预算补测已进入最终完整回归 | 验证通过 |
| W2 | 主代理当时独立执行83项及真实浏览器流程通过；API/UI/runtime已完成互斥写集集成 | 验证通过 |
| W3 | QA完整934项通过、0失败、0跳过；真实浏览器通过；ops 4项通过；systemd-analyze verify退出0；资源benchmark及生产有限只读源解析通过 | 验证通过，待发布 |

最终证据以`qa-report.md`和`deploy-report.md`为准。完整回归187.181秒；新增五个Python模块语句覆盖90.23%（1081/1198），分支覆盖77.73%，合并显示86.68%，并非全仓覆盖。100运行版本live样本峰值RSS约33.6MiB，500保存版本按最多100运行分组测量，无源写入。证据在`evidence/benchmark.json`、`evidence/live-benchmark.json`、`evidence/coverage-summary.txt`及报告列出的临时日志。

各阶段计数有重叠，不能将56、83与934相加。未做生产长时间浸泡、生产并发压测或Lighthouse，不据此声称生产SLA。**截至本节更新，尚未发布生产；Git提交/推送、安装启动worker与平台重启的实际结果须由主代理/DevOps另行记录，测试通过不等于已经上线。**
