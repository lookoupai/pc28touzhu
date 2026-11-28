# 自助模板、对比和队列实施计划

## 摘要

- 本文冻结第一轮前后端接口；`design.md`给出产品意图，本文补齐可直接编码的字段与返回值。
- M1模板与快照、M2自选对比/批量、M3唯一队列、M4正式页面/迁移验证顺序交付。
- schema7只加表和出处列；所有旧condition/赔率/风险/结果/进度不重写。资源上限2000/80/100保持。
- 两个代码owner，Backend维护共享Python/路由/资产映射，Frontend维护全部页面及新E2E fixture；无新依赖。

## 1. 数据与架构约束

沿用设计的lab_templates、lab_command_receipts、lab_queue_items与三个lab_batches新列；模板使用当前版本+实验快照，无需无限模板历史表。`lab_templates.definition_json`存完整规范化定义；`kind=condition`定义单玩法，`kind=risk`定义可适用玩法集合。删除软标记，当前库每用户每类≤100项。实验运行读取自身condition_json/risk_policy_json/settlement_snapshot_json，不能访问模板表。

队列条目owner_kind(batch/study)+owner_id唯一。表中state只取queued/running/paused/removed/completed/error/resource_paused；草稿是没有队列条目的可派生状态。每条独立revision随队列控制改变，普通worker逐期推进不使UI暂停token持续失效。metadata、模板展示名、纯batch改名不进入旧配置hash。

旧API载荷保持接受，新增选择只能从拥有权限的模板/批次解析。schema初始化允许0～7，重复迁移不改旧值。自定义模板pack保存custom_v1；复制旧批次默认拿该批次实际条件和赔率（当前赔率目录仅在用户明确选择重新生成时使用），更换玩法要求新条件模板。

## 2. 冻结wire契约 v1

前缀 `/api/platform/rule-lab`。新增写入POST的request_id为8～80字符；只读preview无request_id；旧POST载荷保持原状，旧/state等不新增必填request_id，其他revision是后端opaque字符串；模板revision另为正整数。HTTP创建201、幂等200；读/更新200；异载荷同ID409；错误 `{error,code}`。新模板/预览/批量/新式study可128KiB，旧其他路由16KiB。数值禁NaN/Infinity/bool代数字。以下对象内字段白名单，未列字段拒绝。

### 2.1 公共对象

`TemplateRef`：内置 `{kind:"builtin",key:"basic_v2",metric:"odd_even"}` 或自定义 `{kind:"custom",id:12}`。冻结源为 `{kind:"batch_snapshot",batch_id:60}`，只用于选条件/复制，不能在模板库列为新模板。

`ConditionInput`：`{type,operator,threshold,window_size,bet_value}`，不适用字段省略或null。type=always/miss_streak/hit_streak/draw_streak/draw_absence/hit_rate。服务端补metric、name、condition_key；普通模板always固定首条。相同整数阈值保持整数JSON形态，禁止35→35.0改变已有key。连续阈值整数1～100；窗口20/50/100；命中率0～100两位小数；操作gte/lte。最多50去重条件；同义大小/单双不出规范化到相反开出。旧内置key必须保持。

`Template`：
```json
{"id":12,"ref":{"kind":"custom","id":12},"kind":"condition","name":"单双连错精选","description":"","metrics":["odd_even"],"builtin":false,"revision":1,"definition":{"metric":"odd_even","conditions":[{"type":"always","metric":"odd_even","operator":null,"threshold":null,"window_size":null,"bet_value":null,"name":"无条件基准","condition_key":"..."}]},"condition_count":1,"created_at":"...","updated_at":"..."}
```
内置id=null，ref稳定，revision使用内置版本整数；内置不可编辑/删除。risk Template的definition为`{metrics:[...],policy:<完整normalize_risk_policy结果>}`，condition_count=null。内部policy.source复制真实规则时由服务端整理；保存自定义risk模板后独立，不向真实规则写入。

`ConditionSelection`：`{template_ref:<TemplateRef>,expected_revision:1,condition_keys:[...]}`。keys可省略表示该模板全部；明确空数组无效；必须属于模板，内置可从GET列表得到完整conditions。batch_snapshot.expected_revision用源batch的configuration_hash字符串；新实验selected_keys不足基准时服务器自动补always并明确返回。

`RiskSelection`：`{policy:<完整风控>}` 或 `{template_ref:{kind:"custom",id:12},expected_revision:1}` 二选一；自定义模板kind必须risk且支持对应metric。复用已有真实规则preset时UI先展开policy，本次解析继续验证真实出处所有权；自定义模板出处另存snapshot，不能混入rule_id字段。

`Capacity`在原`{saved,running,max_saved,max_running,remaining_saved,remaining_running}`基础上加`{saved_batches,max_batches:80,remaining_batches}`，batch数是当前用户，版本数是全服务。前端禁止写死1806或剩余194。

纯领域编译接口（DOMAIN owner）：`normalize_condition(metric,value)`；`compile_conditions(metric,values,require_baseline=True) -> {conditions,duplicates_removed,warnings}`；`normalize_template(kind,name,description,definition,preview=False) -> {kind,name,description,metrics,definition,condition_count,duplicates_removed,warnings}`。模块不访问数据库；中央rule_lab.py由Backend在交付后串行集成。

### 2.2 模板接口

| 方法/路径 | 请求 | 响应 |
| --- | --- | --- |
| GET /templates | 可选kind、metric两个query，重复/未知拒绝 | `{templates:[Template],limits:{max_conditions:50,max_templates_per_kind:100,windows:[20,50,100]}}` |
| GET /templates/{id} | 自定义本人项 | `{template:Template}` |
| POST /templates/preview | `{kind,name,description,definition}`（name可空预览） | `{valid:true,definition,conditions:[...],condition_count,duplicates_removed,warnings:[]}`；risk conditions=[]/count=null |
| POST /templates | `{request_id,kind,name,description,definition}` | `{created:true,template:Template}` |
| POST /templates/{id}/edit | `{request_id,expected_revision:<整数>,name,description,definition}` | `{created:true,template:Template}`；revision+1 |
| POST /templates/{id}/delete | `{request_id,expected_revision:<整数>}` | `{created:true,deleted:true,template_id:12}` |

condition definition输入 `{metric,conditions:[ConditionInput]}`；risk definition输入 `{metrics:[...],policy}`。复制使用POST新项，内置只读由服务器拒绝，不只靠按钮禁用。所有模板写请求必须允许丢响应同request_id重试；删除后收据仍可获取相同结果，但其他用户仍404。

### 2.3 批量创建

`BulkConfig`：
```json
{"mode":"backtest","sources":[15,16],"start_date":"2026-10-18","end_date":"2026-11-27","stake_amount":1,"metric_configs":[{"metric":"odd_even","condition_selection":{"template_ref":{"kind":"custom","id":12},"expected_revision":1},"risk_selection":{"policy":{"version":1,"mode":"legacy"}}}],"name_pattern":"{source} · {metric} · {template}","names":[]}
```
name_pattern只允许三种固定替换标记，不能执行表达式；names可选覆盖列表`[{predictor_id,metric,name}]`。来源≤20、metric_configs≤3且metric不重复、展开≤60批次。live日期必须null；新bulk历史排队支持，live只保存草稿，enqueue=true拒绝并提示live需要单独启动（无限前向任务不能占住有限队列）。

POST `/batches/preview` 接BulkConfig；返回：
```json
{"available":true,"preview_revision":"bulk:...","batches":[{"predictor_id":15,"predictor_name":"jnd28一阶转移增强","metric":"odd_even","name":"jnd28一阶转移增强 · 单双 · 单双连错精选","condition_count":3,"experiment_count":6,"start_date":"2026-10-18","end_date":"2026-11-27","template_snapshot":{},"risk_policy":{}}],"batch_count":2,"new_experiments":12,"capacity":{},"blockers":[],"warnings":[]}
```
模板不存在/非法参数直接4xx；容量不足仍200 available=false+blockers，保留清单。preview_revision包含完整展开定义和版本/赔率目录hash，排除瞬时capacity；提交仍重查capacity。

POST `/batches/bulk` 接`{...BulkConfig,request_id,preview_revision,enqueue:<bool>}`；返回`{created:true,batches:[既有Batch对象],generated_count:12,queued_count:2,creation_request_id:<request_id>}`。同收据重试created=false，绝不重复批次。接收同义重复来源时去重并warning；对单个组合失败整组回滚。双赔率普通批次保持现状。

旧POST /batches接受旧载荷，新增可选condition_selection/risk_selection；二者提供时必须由服务端解析快照，不要求客户端构造custom_v1。新模板在创建、fork、未启动编辑三个入口都可用；从旧batch复制且没有显式condition_selection更改时保持原冻结定义，不能重新展开basic_v2覆盖选中的subset。

### 2.4 自选风控对比

新增GET `/batches/{id}/comparison-preview`沿用既有响应并加`conditions:[冻结Condition]`、`settlement_rules:[{id,name}]`、`source_revision:<配置hash>`，使前端可选条件/赔率；原variants仍返回。

候选输入 `Candidate` 之一：
- `{kind:"preset",key:"round_tp_half"}`，key可baseline/round_tp_half/round_sl_half/day_sl_half/gate_off及既有gate键。
- `{kind:"gate",threshold:45,min_settled_count:30}`。
- `{kind:"risk_template",template_id:12,expected_revision:1}`。
- `{kind:"policy",label:"我的风控",policy:<完整政策>}`（方便编辑后的单次对照）。

`StudySelection`：`{source_batch_id,condition_keys:[...],settlement_rule_ids:[...],candidates:[Candidate]}`。至少1条件/1实际存在的赔率、服务器固定补baseline；去重后至少2、最多12方案。选择源缺失condition×赔率配对则4xx，不静默创建不完整组。自选比较不强制加入always条件；固定补的是风险baseline，而不是无条件入场条件。

POST `/studies/preview`接StudySelection，返回`{available,preview_revision,source_batch_id,source_name,source_revision,conditions,settlement_rules,variants:[现有variant结构],candidate_count,new_experiments,capacity,blockers,warnings}`。variant结构仍`{key,label,available,reason,policy,changes}`，客户端不能靠数组位置猜身份。

POST `/studies`旧三字段路径保持；新路径接`{...StudySelection,name,request_id,preview_revision,enqueue}`。返回现有`{created,study}`。创建的各child采用源实验冻结条件与赔率，当前原目录是否变化不影响。用户只选一套赔率时，返回rows和输入比较仍正常。所有接口不写源batch。

### 2.5 队列与状态

GET `/queue?state=active|completed|all`（缺省active）返回：
```json
{"items":[{"id":1,"owner_kind":"batch","owner_id":60,"name":"...","state":"queued","expected_revision":"queue:...","queue_order":4,"experiment_count":50,"processed_issues":0,"total_issues":null,"waiting_reason":"需要50个运行名额，当前剩余0","actions":["pause","remove"],"error":"","start_date":"2026-10-18","end_date":"2026-11-27"}],"drafts":[{"owner_kind":"batch","owner_id":78,"name":"...","expected_revision":"management:...","experiment_count":6}],"capacity":{},"updated_at":"..."}
```
items为已登记队列对象，drafts仅当前用户尚未启动、未被比较组管理、无series且不在队列的普通历史批次。completed筛选不返回drafts。展示本人详情，capacity仅全局聚合。owner对象可加predictor_name/metric/source fields，但actions是服务器权威；前端不得自行推断可恢复状态。

POST `/queue/enqueue`接`{request_id,owner_kind,owner_id,expected_revision}`（首次使用management token；已有队列对象使用queue token），返回`{created:true,item:<QueueItem>}`。completed不能重跑，series owner转回既有续算入口，不放普通队列。

POST `/queue/{id}/state`接`{request_id,expected_revision,action:"pause"|"resume"|"remove"|"retry"}`，响应同上。remove允许queued/paused/running，运行先暂停并保留进度。pure rename不变队列配置hash。

旧Batch对象保留status字段并增加`queue_state:null|<state>、display_status:"draft"|"queued"|<原状态>、queue_item_id:null|id`。未登记旧完成/live/series对象按旧status呈现，不强推入队。旧live/state保持直接启动语义；旧普通历史/state必须同步队列：start先入队，可在事务中立即尝试晋级以兼容空闲时running；容量不足返回成功queued而不是丢弃请求。UI优先显示display_status；旧worker只读真实status=running。

比较组state同步队列；旧advance_studies为新调度模块的适配入口，不能保留两个独立晋级循环。series启动仍复用既有能力，所有启动者共享同事务100名额校验。旧queue_optout的removed信号仅代表原清单自动授权失效，用户显式resume/enqueue可以建立新意图；不能永久把批次锁死。`set_risk_policy`直达入口同样必须退出未启动队列。完成批次采用series续算后，旧队列completed记录不能覆盖实际series的running/paused展示。

## 3. 前端模块桥接

不复制request/auth/drawer逻辑。主rule_lab.js在末尾发出`rulelab:ready`并暴露`window.RuleLabUI={request,refresh,openDrawer,dismissDrawer,feedback,getContext,setBusy}`。getContext只返回必要的`{options,batch,selectedId,accessDenied,loading,mutating}`；各模块不直接改主state。setBusy保证跨抽屉互斥，refresh保持当前结果选择与页码。模块初始化监听ready，若bridge已存在立即初始化，保证加载顺序/测试独立eval均成立。

Frontend可选一个`rule_lab_self_service.js`集中管理新增UI，避免模块拆分成多层包装。新增页头按钮/抽屉由主HTML给静态ID；所有新增script显式映射到API资产路由（Backend owner）。如果拆templates/queue/bulk多个JS，先通知Backend补资产映射；推荐首版一个文件。页面路由仍/rule-lab。

正式UI用真实API response渲染，无原型hardcoded来源、1806额度或fake queue。无条件基准不可取消；风险policy编辑复用现有完整risk editor桥接（扩充createRiskEditor/fillRisk/riskValue/riskError接口），而不是原型简化字段。

## 4. 里程碑与门禁

| 模块 | 内容 | 依赖/复杂度 | 绿门禁 |
| --- | --- | --- | --- |
| M1 | 模板CRUD、schema7、条件编译/冻结、旧fork兼容 | 现有规范 / L | 模板改删不改旧数据/哈希；内置key完全一致 |
| M2 | 选择比较、原子bulk、收据/容量 | M1 / L | 1×1×2=2版本、冻结赔率、并发/失败零半组 |
| M3 | 持久队列、worker调度、导入CLI/旁路 | M1/M2 / XL | 100额度、暂停删除交错、重启及清单幂等接续 |
| M4 | 正式页面、API/DOM、移动布局、生产副本迁移 | M1～3 / L | 模板→创建→比较→队列全链真实服务器成功 |

## 5. 数据/生产迁移要点

发布前使用schema6一致副本证明重复初始化及旧表旧列不变。切换jnd28时停止旧清单进程但不暂停批次，读取最终manifest和库；导入18个ID，running/completed保持，未启动且未optout才queued；人为暂停/removed尊重。新的后台worker取得唯一启动权后旧脚本不能再拉起。捕获前后ID/冻结定义/统计/日汇总/游标/真实跟单配置指纹；允许自然新增结果但不允许倒退/重置。当前enable_continuation=false保持。

CLI默认只读预览，--apply必须显式，--manifest仅操作员本地路径且先证明旧清单unit inactive。HTTP不接受服务器路径。若不用CLI停止服务而由root先停，其检查仍须存在；无法确认即拒绝导入。回退优先前向修复，绝不覆盖已有新成绩。

## 6. 测试策略与任务指引

新测试先写并红测（功能缺失的断言，不以模块导入失败冒充业务红测）。使用临时真实SQLite、WSGI与来源fixture，复用现有domain/risk/worker测试；FE测试从真实API获取payload再提交。关键证明：历史冻结、单赔率组、原子创建/并发容量、queue旁路控制、迁移重放、同ID重试与权限。

新旧Python全回归和已有结果/金额/观察/布局DOM测试根代理执行。浏览器候选可用则跑临时profile真实页面，不读用户profile、不向外发消息。无新依赖安装。测试未覆盖真实画面时明确DOM边界。

STATUS: DONE
REASON: Wire对象、路径、状态、兼容与两代码owner边界已冻结，可按tasks派发。
