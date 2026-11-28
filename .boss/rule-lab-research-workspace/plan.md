# 研究工作区实施计划与接口契约

## 摘要

- 在一期自助实验室上完成跨来源排名、候选收藏、批量删除/改名/手动续算和来源数据质量预检四个闭环。用户已授权实施、发布和GitHub上传。
- 复用原生JS抽屉、管理员会话、独立SQLite、现有删除job和续算引擎。schema8仅新增收藏表，命令收据复用lab_command_receipts；不重写旧配置或成绩，不新增依赖。
- 所有批量写入都在一个连接/事务中预检全部对象后提交；不得循环调用各自提交的旧公开API。批量续算只发一次手动append，不接收定时设置。
- 排名使用统一明确月份窗口，按实际冻结条件、赔率、风控和注额分组；跨来源预测不同是研究对象，不要求预测digest相同。无完整可用样本者展示原因，不授予名次。
- 数据质量是有界只读分页扫描，明确“当前保留数据的分批检查”，不能冒称原回放完整证据。最多20来源，逐来源请求，每页≤200期、区间≤366天，可取消/继续。

## 1. 范围与兼容

本契约冻结上述四块确定需求。脚本条件、AND/OR、资金约束与自定义赔率尚由root处理用户范围澄清，未包含在此wire中；如用户纳入，由root先补契约再派发，不用这里的暂列代替最终用户答复。

旧2000全服务保存版本、80批次/用户、100全服务运行版本不变。旧单项管理、续算、队列、结果API载荷保持。新功能开发不代表实际删除任何生产批次、修改名称或启用续算计划；这些必须由用户在完成后的页面明确操作。已有9个续算控制保持。

## 2. 公共规范

前缀`/api/platform/rule-lab`。新路径全部沿用401匿名、403非管理员、404他人对象；未知/重复query、bool代ID、NaN、超长、未知字段拒绝。新POST写入`request_id`8～80字母数字短横/下划线。读写都返回JSON；创建201、命令/编辑200、删除/追加首次接受202、幂等重放200；同ID异操作/异内容409。预览不需要request_id。

对象选择`OwnerRef={owner_kind:"batch"|"study",owner_id:<正整数>}`。比较组子批次只能用组owner；选择列表不列子批次供单独批量管理。同一OwnerRef去重；最大20owner。canonical JSON指纹不受对象键顺序影响。只允许新research/favorites端点128KiB，旧限制不全局改变。

## 3. 研究选项与跨来源排名

### 3.1 GET /research/options

无query，返回：
`{sources:[{id,name}],conditions:[{metric,condition_key,name,source_count}],settlement_rules:[{id,name}],owners:[OwnerItem],limits:{max_sources:20,max_owners:20,max_favorites:500,max_months:12,max_quality_days:366,quality_page_size:200}}`。

sources/conditions来自本人当前未删除实验；quality来源可复用已有`/options.predictors`全部当前共享来源。条件按(metric,condition_key)去重，不用显示名称判同义。OwnerItem包含owner_kind/owner_id/name/status/display_status/predictor_id/predictor_name/metric/start_date/end_date/experiment_count/expected_revision，以及`continuation`既有摘要；其expected_revision为原管理token。owners仅独立批次+比较组（含删除中但操作禁用），总量受已存80批次约束，不能暗中截断。

### 3.2 POST /research/rankings（只读）

请求精确字段：
```
{predictor_ids:[1,2],metric:"odd_even",condition_key:"...",
 settlement_rule_id:"pc28_high_regular",months:2,include_partial:false,
 end_date:"2026-11-27",sort:"roi",limit:50,cursor:null}
```
来源1～20去重；月份1～12；end_date合法且不晚于北京时间今天；sort=roi/net_profit/drawdown；limit1～100。一个条件+一套赔率使候选最多当前80批次。首次cursor=null，后续用原参数+返回cursor。完整筛选集合在同一SQLite只读事务加载，不得先截页再排总名次。分页游标绑定规范筛选和结果revision；结果在翻页期间变化则409并提示刷新，不能造成重复/漏行。

统一window用`observation_window(end_date,months,include_partial,now)`生成。扩展`load_observations`可接受服务端已验证的window覆盖值，旧调用不传时完全保持现行为；不能每批各取自己的end_date后排序。

响应：
```
{window:{...既有window字段},revision:"ranking:...",items:[RankRow],
 groups:[{key,label,eligible_count,source_count}],total:80,next_cursor:null,
 warnings:["跨来源结果为同口径参考，预测输入不同，不代表因果优劣"],generated_at:"..."}
```
RankRow：`{batch_id,experiment_id,batch_name,predictor_id,predictor_name,metric,condition_key,condition_name,settlement_rule_id,settlement_rule_name,group_key,group_label,rank:<整数|null>,eligible:<bool>,reasons:[{code,message}],metrics:{net_cents,stake_cents,roi_percent,settled_count,betting_days,daily_drawdown_cents},observation:<原对象>,favorite_id:<id|null>,source_updated_at}`。

group_key包含完整condition语义、实际settlement_snapshot（不可只按赔率ID）、normalize_risk_policy去source出处、原stake_cents、mode及统一window；显示名、模板revision/出处、predictor_id不进入口径分组。live、deleting、旧引擎、输入冲突/过期、日账与累计不符、窗口未覆盖/未完整、窗口待结算、无样本、known_skips均eligible=false；恢复窗口left_censored本身不是收益排序阻断。组内至少2个不同来源的合格候选才赋rank；同来源多个复制批次仍可列，但不能制造“跨来源”比较。相同排序指标并列同名次（competition rank，后续跳号），次级稳定顺序experiment_id。

从真实日账计算该window累计net/stake、结算数、有下注日和日结回撤；stake=0时ROI=null，不能当0。当前未完成月若明确include_partial=true仍可展示阶段值，但rank=null并提示“窗口未完整”。不在界面产生跨group总名次。

## 4. 候选收藏

schema8新增`lab_favorites`：`id INTEGER PRIMARY KEY AUTOINCREMENT,user_id,source_batch_id,source_experiment_id,name,notes,snapshot_json,revision INTEGER DEFAULT1,created_at,updated_at,deleted_at`；本人活动源experiment唯一索引；每用户最多500活动项。snapshot_json保存服务器读取的来源名称/IDs、metric、stake_cents、实际condition和settlement_snapshot、risk_policy、template_snapshot/risk_template_snapshot、收藏时累计成绩及时间。没有cascade FK，源批次删除不会删除收藏快照。编辑只改name/notes，不改冻结定义。

- GET `/favorites?limit=50&before_id=123`：仅这两个可选query。`{items:[Favorite],next_cursor:<id|null>,total,limit:50}`，ID倒序seek；读取先LIMIT+1证明has_more，不能以count==limit猜下一页。返回本人活动项。
- POST `/favorites`：`{request_id,batch_id,experiment_id,name,notes}`。name1～80，notes0～500。来源/实验归属、删除态由服务端验证后冻结。先查命令收据再解引用，成功后源删除的原请求重试仍返回原结果。重复收藏同活动experiment返回同ID、`already_saved:true`，不偷偷覆盖已有名称/笔记。
- POST `/favorites/{id}/edit`：`{request_id,expected_revision:<int>,name,notes}`。
- POST `/favorites/{id}/delete`：`{request_id,expected_revision:<int>}`，软删除，重放不复活；不存在/他人404。
- POST `/favorites/{id}/create`：`{request_id,expected_revision:<int>,name,start_date,end_date}`。只创建历史草稿；保留收藏的原predictor/metric/每注金额/单条件/单赔率/完整风险，当前模板和赔率目录不替换。明确使用收藏时间定义重新计算，不复制成绩。源批次已删仍可创建，只要当前共享来源仍可用、冻结定义合法、容量有余；保留独立快照出处。一次写事务检查revision、容量和收据，返回`{created,batch,generated_count:1,favorite_id}`。日期须合法，既有历史回测限制沿用。

Favorite：`{id,name,notes,revision,snapshot,created_at,updated_at,source:{available:<bool>,batch_id,experiment_id,state,updated_at},current_result:<现累计摘要|null>,saved_result:<冻结摘要>,result_label:"当前成绩"|"收藏时快照"}`。不存在当前源时current_result=null，明确标源已删除/定义已变化，只能看保存时成绩；不把旧成绩标最新。页面收藏列表支持本地名称搜索、查看冻结条件/风险/赔率、修改笔记、移除、查看当前结果、按快照新建草稿。

## 5. 批量管理

### 5.1 POST /management/bulk-preview（只读）

请求：
```
{action:"rename"|"delete"|"append",owners:[OwnerRef],
 names:[{owner_kind,owner_id,name}] | null,end_date:<日期|null>}
```
rename必须每个目标有且仅有一个合法name，另两种names=null；append须end_date，其余end_date=null。拒绝空选择、未知对象、子批次与重复覆盖名；重复OwnerRef可规范去重并warning。返回`{available,preview_revision:"bulk-management:...",action,items:[{...OwnerItem,eligible,blockers,old_name,new_name,counts,continuation}],totals:{owners,batches,experiments,...},blockers:[{owner_kind,owner_id,code,message}],warnings,confirmation_text:<string|null>}`。

preview_revision绑定规范请求+所有管理/续算revision+依赖集合及冻结配置，不绑定无关统计自然前进。进行中删除统计变化只影响即时预估数量；提交在同事务重新计数。任一成员不可操作available=false，明确列出，不能自动跳过。

删除支持同时选组和其原来源批次：若来源的所有依赖组均在本次删除集合则合法；漏选任一依赖组则整组阻断并列出名称。组内子批次不能再独立选。预览将所有实际受影响batch IDs去重，避免双计数/双job。删除确认短语固定`删除 N 个对象`，N为规范化owner数，完整清单始终可见。

### 5.2 POST /management/bulk

请求为原预览字段加`{request_id,preview_revision,confirm_text:<string|null>}`；delete必须精确等于confirmation_text，其他confirm_text=null。

先读取lab_command_receipts再解引用；读写事务中重查所有revision、权限、资格与依赖。任一失败全部回滚，包括job、续算请求、queue、对象状态和收据。成功返回`{created,action,affected_owners,items:[{owner_kind,owner_id,name,job:<既有Job|null>,append:<既有AppendResult|null>}],jobs:[Job],request_id}`。旧收据重试只返回原结果，不重新暂停/重命名/续算。

实现约束：
- 抽取现rename_owner/request_delete/request_append的同连接内部helper；旧API仍由wrapper创建连接调用helper。bulk不可嵌套新连接，不允许先commit第一个再操作第二个。
- rename仅更新名称/管理generation，冻结配置hash与队列入场意图不变。
- delete一次事务先停止所选对象与其子项、退出新队列、关闭其capture/compute、取消相关段；所有删除job先为study创建，再为独立源batch创建，使依赖组先清理。继续复用有界management_maintenance，不同步删除大量日账。共享输入保留规则沿用；收藏快照不级联清除。
- append复用严格续算预检，不能绕过暖机、输入完整性、同组边界、已有待处理段等。没有series时沿用现一次性request_append的adopt/capture=true、compute=false；已有series保留compute/daily/next_due设置，capture=false时整批拒绝并明确提示开启后重试。页面不接受自动开启或修改schedule字段。需先明确显示“为新接入对象保留后续输入，仅此次手动追加”。
- 同一原子组中的append可产生waiting_inputs，后台按原continuation公平/容量机制推进；返回每项真实状态，不把已接受当已完成。

## 6. 来源数据质量预检

POST `/research/data-quality`（只读）请求：
`{predictor_id:<int>,metric:<metric>,start_date,end_date,cursor:<opaque|null>,limit:200}`。
日期范围1～366天、不晚于北京时间今天。单页limit1～200；UI最多选择20来源，每次只发一个页请求，整个扫描最多1,600,000个“来源×期”检查，达到总预算显示未完成而非截断为完整。用户可缩小日期/来源重新检查，不允许自动无限扫描。

第一请求在来源只读事务中确定合法共享来源、来源身份、固定observed_at和截止期，定位起始期（复用find_start_issue的索引seek）。游标包含版本、筛选指纹、after_issue、固定截止期、observed_at/来源身份；须有长度上限、严格类型和范围校验，拒绝不同筛选复用、倒退及超出截止期。每页在同一只读源事务读取开奖与对应预测，日期/期号边界再次验证；禁止修改来源表、加索引或读取任意客户端文件路径。

响应：
```
{predictor:{id,name},metric,range:{start_date,end_date},scan:{observed_at,read_at,
 cutoff_issue,first_issue,last_issue,scanned_issues,next_cursor,complete,source_identity},
 counts:{draw_rows,valid_draws,missing_draw_issues,invalid_draws,present,on_time,
 late,invalid_prediction,confirmed_no_prediction,unknown_or_archived,waiting},
 rates:{prediction_coverage_percent:<number|null>,confirmed_missing_percent:<number|null>},
 days:[{date,...counts}],examples:[{issue_no,date,category,message}],warnings:[{code,message}],
 boundary:{start_verified,end_verified}}
```
counts是**本页**不是客户端传入累加数；days本页按北京时间目标开奖日聚合；examples≤20。scanned_issues为本页实际检查基数；UI累加页统计并同时标总已扫期数、来源完成情况。取消保留已读结果与每来源next_cursor，可继续；刷新页面清空扫描会话，不保存假全局缓存。请求失败保留原cursor，可重试同页；只在成功收到后移动cursor，避免重复计数。

分类复用当前源规则：同页实际开奖三球/和值/时间合法才valid；预测方向按选定metric合法、生成时间不晚于实际开奖前40秒（与当前回放决策截止一致）才on_time；缺预测且日归档元数据证明处于保留区间才confirmed_no_prediction；已归档/元数据缺失则unknown_or_archived；来源不可共享直接403。不能把null方向算有效；无效行和重复冲突单列。内部期号缺口计实际gap长度，日期分配不明确的gap只列总量，不凭210秒猜分日。范围前后无可保留边界证据时start/end_verified=false并warning。

coverage分母是本页已经实际核对的有效开奖期数，present统计选metric有效预测；confirmed_missing率仅在该页不存在unknown/invalid/missing_draw/waiting且边界已确认时给数值，否则null并解释。“0%已确认缺预测”不等于历史完全无缺档。UI用全扫描counts和同一规则重新算汇总率，任何页不明就保留unknown。多页不是冻结事务，显示固定首次observed_at与每页read_at及“分批检查，源数据可能更新”；不得将这份现状检查标为某旧实验回放的输入证据。来源身份变化409，界面要求重新检查。

## 7. 页面与桥接

页头新增“研究工作区”，使用一个共享抽屉，内部标签“来源排名 / 候选收藏 / 批量管理 / 数据质量”。沿用RuleLabUI request、drawer、mutating、denyAccess、refresh及现有完整风控摘要；新增`rule_lab_research.js`独立模块，由app.py显式资产映射。主结果详情增加“收藏候选”，传入当前batch/experiment；排名行也可收藏/打开原结果。

结果页、模板库和运行队列继续工作。抽屉手机全宽、按钮≥44px；未保存名称/笔记提示离开，扫描取消不触发后台写入；403必须清空账户相关缓存/扫描并关闭抽屉。批量delete按钮在预览可用+完整短语匹配后启用；bulk写入网络未知保留原request_id和payload，显式重试核对。

## 8. 迁移、资源与发布

schema8新增favorites，无旧列回填。initialize接受0～8，重复执行旧表旧列/水位完全不变；旧results维护工具从5/6/7扩充8，旧manifest导入器可扩充只读/已handoff schema8检查，不重导生产清单。生产副本由root提供`/tmp/pc28-research-release/preflight-v7.db`。

当前授权不需要再次确认常规迁移或部署。root负责备份、停止必要写者、快进、www读取权限、迁移、恢复、只读UI/数据/队列进度核对，再push GitHub。开发不操作生产用户对象。

STATUS: DONE
REASON: 四项确定后续功能的wire、事务边界、分页与失败语义已冻结；高级范围待root采纳用户回答后另补。
