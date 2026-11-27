# 实验管理、性能与空间回收实施计划

## 摘要

- 先测性能并冻结优化，再交接中央store；管理新模块、API与UI可依据固定契约并行准备。
- 原实验定义和历史结果不混改；大删除后台执行，停用与写入隔离先于物理清理。
- schema v5加法管理元数据、幂等job与ID高水位；原业务列保持。已有continuation及每日时间设置继续复用。
- 空间模式通过维护CLI一次转换；以后worker分批回收，界面显示真实释放量。
- 实验继续跑到发布窗口；所有性能和删除验证在临时库/生产一致副本完成。

## 1. 模块与里程碑

| 模块 | 内容 | 依赖 | 验收 |
|---|---|---|---|
| M1 性能 | cProfile/SQL计数、连接/加载/写入热点优化 | 无 | 同输入经济结果一致，实测耗时/CPU/RSS及重复样本 |
| M2 管理 | 编辑/fork/rename、删除job、queue opt-out、ID高水位 | 固定契约；store集成等M1交接 | 并发、重启、归属、版本容量、共享数据保护 |
| M3 存储 | 统计、增量回收job、一次转换CLI | M2 job基础 | NONE真实转换、分批回收文件缩小、busy/空间不足不误报 |
| M4 产品 | WSGI接口、批次/组入口、编辑表单、删除确认/进度、存储面板 | 固定M2/M3契约 | 临时真实WSGI桌面/手机闭环 |
| M5 QA/发布 | 独立反例、全回归、副本恢复、短暂停机切换 | M1～M4通过 | 原运行游标/结果保持、健康恢复、线上GET可用 |

## 2. 产品规则

1. batch/study可直接改名称；名称不改变策略hash。续算时间和开关仍走已有continuation接口。
2. batch仅在paused、started_at为空、processed=0、无decisions/series/study归属时允许原地编辑定义。全部配置校验通过后事务替换实验定义，结果仍为空；退出本轮批准队列，保持暂停，页面提示需手动启动。
3. 已运行/有成绩的batch可“修改并新建版本”：新batch暂停、结果为空，旧batch及累计数据不变。容量不足明确拒绝，不自动删除旧成绩。不扩展为修改某一已算实验的历史。
4. study child禁止独立改定义/删除；通过study整体删除。独立source batch若被study引用，返回dependent_studies及依赖ID/名称，要求先显式处理对应比较组，不隐式连带删除。
5. 删除确认展示版本数、数据类型、正在运行/待结算数、共享输入会保留的原因。用户确认后停止选中对象的实验写入与后续调度，后台物理清理；删除操作不可撤销，不伪造结算或将pending计为亏损。
6. 无引用的共享输入才可清理。仍被其他series、重测绑定、普通批次/待启动批次引用的来源输入保留；不能因最后一个series消失而误删普通回测还需要的cache。

## 3. 固定HTTP与服务/存储契约

基础路径 `/api/platform/rule-lab`。保持session+admin+owner、no-store、POST16KB、严格未知字段拒绝。所有管理revision都是opaque string，客户端原样传回，不能用worker频繁递增的revision导致重命名永远冲突；token至少覆盖配置/名称、生命周期和依赖关系。写入事务再次复核实际资格。request_id沿用8～80字母数字下划线短横线。

| HTTP | 请求 | 返回 |
|---|---|---|
| GET `/{batches|studies}/{id}/management` | 无 | `{owner_kind,owner_id,name,expected_revision,editable,can_fork,config,blockers,queue_managed}`；config仅batch有 |
| POST `/{batches|studies}/{id}/rename` | `{request_id,expected_revision,name}` | `{created,owner}`，首次/重试均200 |
| POST `/batches/{id}/edit` | `{request_id,expected_revision,config}` | `{created,batch,queue_removed}`；config完整既有创建payload，name包含其中 |
| POST `/batches/{id}/fork` | `{request_id,expected_revision,config}` | `{created,batch,source_batch_id}`；首次201、重试200；新batch paused |
| GET `/{batches|studies}/{id}/delete-preview` | 无 | `{eligible,expected_revision,name,counts,blockers,shared_inputs_retained}` |
| POST `/{batches|studies}/{id}/delete` | `{request_id,expected_revision,confirm_name}` | `{created,job}`；首次202、同请求200；confirm_name必须等于预览名称 |
| GET `/management-jobs/{id}` | 无 | `{job}`；owner可在对象物理删除后查询 |
| GET `/storage` | 无 | 存储状态（下述字段），无写入 |
| POST `/storage/reclaim` | `{request_id}` | `{created,job}`，首次202/重试200；不触发同步VACUUM |

`config`包含name/mode/predictor_id/metric/template_pack/stake_amount/start_date/end_date/risk_policy；复用normalize_batch_payload及来源/风控验证。新version默认当前catalog与新所选template，必须在界面注明其配置为新版本；仅改名字不刷新赔率。用户要旧冻结赔率的按金额重测仍保留现有入口。

服务同名方法由API owner实现；Data在RuleLabStore提供下列接口（首次定义后不得各自另造兼容层）：

- `management_preview(user_id,owner_kind,owner_id,*,now)`
- `rename_owner(user_id,owner_kind,owner_id,payload,*,now)`
- `edit_batch(user_id,batch_id,payload,*,predictor_name,experiments,now,request_payload=None)`：payload内config为服务校验后的规范化配置；request_payload保留原始HTTP请求用于幂等哈希，不能用规范化后的字段改变重试身份。
- `fork_batch(user_id,batch_id,payload,*,predictor_name,experiments,now,request_payload=None)`：同样保留原始请求哈希。
- `delete_preview(user_id,owner_kind,owner_id,*,now)`
- `request_delete(user_id,owner_kind,owner_id,payload,*,now)`
- `management_receipt(user_id,owner_kind,owner_id,payload,action)`：只读先找同账号原始请求回执；对象已删、源权限变化或catalog更新后，合法旧请求仍可恢复，不再次执行副作用。
- `management_job(user_id,job_id)`
- `management_jobs(user_id,limit=20)`：只读最近20项当前账号job，实际通过overview的`management_jobs`数组返回，页面重开或丢失POST响应后恢复状态；未新增无界列表路由。
- `request_reclaim(user_id,payload,*,now)`
- `management_maintenance(*,now,limit=500,max_pages=256)`：worker有剩余预算时调用一次，返回本次处理数/状态，不无限循环；delete优先、回收次之。
- `queue_eligibility(user_id,batch_id,configuration_hash)`：返回`eligible`/`removed`及原因；删除、原地编辑、404均为终态移出，不能让清单永远blocked。rename不移出。
- `start_queued_batch(user_id,batch_id,configuration_hash,*,now)`：资格检查与启动在同一个写事务完成，返回`started,removed,reason`或batch；前一次只读检查通过不能替代最终原子复核。
- `storage_status()`扩展原字段，兼容旧消费者。

job统一字段：`id,kind(delete|reclaim),status(queued|running|waiting|completed|error),owner_kind,owner_id,processed_rows,total_rows,freed_bytes,error,created_at,updated_at,phase,released_versions`。total_rows可在预览后重新核对；UI不把估算/删除行数换算成已释放字节。删除job完成后报告容量释放及存储实际变化；回收jobbusy保持waiting。

存储字段：原`used_bytes,budget_bytes,detail_retention_days`保留；新增`database_bytes,wal_bytes,shm_bytes,allocated_bytes,reusable_bytes,reclaimable_bytes,auto_vacuum,incremental_ready,reclaim_job`。reusable以freelist_count*page_size，reclaimable是可回收整页估计，需明确不是承诺物理释放；实际freed_bytes以维护前后文件大小测量，外部文件计入used_bytes。全局存储数据仅管理员接口可读，不能从普通用户路径泄漏归属。

## 4. 数据与执行约束

最终采用4张管理表：`lab_management_receipts`、`lab_management_jobs`、`lab_object_controls`、`lab_id_watermarks`。ID高水位统一覆盖batch、experiment、study、capture_stream、archive_revision、series；保留旧archive revision栅栏，不因删除来源或series复用其身份。保证以下不变量：

- schema5初始化只加结构/索引、初始化ID高水位，不清旧表、不启动删除/回收、不改旧用户实验状态。
- 所有batch/study/experiment创建路径（普通、study复制、金额重测、fork、pristine重建）以及series/archive的稳定身份统一显式取持久单调ID；事务回滚不得使已发布ID复用。
- 删除请求事务先保存receipt/job、禁用系列capture/compute、取消活动段/等待请求、标记对象deleting、bump批次revision、队列opt-out。已有worker拿到旧work必须失败；即便重新读取deleting对象也不能settle/commit。
- 逐批清理依赖表：decisions→risk_days/daily/rounds→monthly/checkpoints/segmentchild→segments/requests/series→experiments→batch/study，使用实际FK拓扑，段自引用从后往前。无跨对象误删；receipt/tombstone最小记录保留用于重试和ID安全。
- pending删除只是撤销模拟对象，不产生新的输赢；不要求所有历史结果先结清，否则运行中的实验无法管理。
- 边删除边释放保存版本额度只能以实际删除完成的实验定义计算，不把仍需清理的同一ID开放给新建。
- 名称/配置编辑与queue_start竞态必须同事务校验；queue不凭旧manifest状态恢复用户手动暂停或修改后的实验。
- 性能优化不持有跨tick长读事务，不能让WAL增长不收敛；连接/缓存复用必须结束写事务并在revision检查后处理状态。

## 5. 空间回收

新空库在建表前配置auto_vacuum=INCREMENTAL；旧库NONE由 `scripts/maintenance/prepare_rule_lab_storage.py` 完成一次转换，非API初始化副作用。实际参数为 `--database <实验库>`、默认预览；`--apply --backup <实验目录外文件>` 显式执行。CLI逐一检查实验API/worker/queue的systemctl状态，只接受返回码3且状态inactive/failed；未知、空响应或bus查询失败都拒绝，不能凭非零返回码推断已停。CLI本身不停止/启动服务，由发布步骤保持原服务状态；独立库一致备份和磁盘可用满足VACUUM临时空间预估后执行；checkpoint完成后设置模式并VACUUM，再quick_check/FK、原列数据哈希、模式与文件大小检查。保持WAL模式/所有者，恢复原服务启停状态和运行进度。不要读取或转换上游2GB库。

日常job只执行正整数页数上限的 `PRAGMA incremental_vacuum(N)`，完整消费返回游标，N不能为0。一次调用结束释放锁；WAL `wal_checkpoint(TRUNCATE)`返回busy则等待后续，不误报已回收。可复用页在预算计算中体现，但文件系统真实可用空间不足仍拒绝新增写入。SQLite的文件页可复用、物理占用不能简单相减后绕过1GiB总预算。

## 6. 验收与发布

性能基准至少覆盖50+50高触发按轮、92组合、普通/连续输入；固定输入/时钟和初始库，比较逐笔、stats、risk_runtime、日账、活动轮、pending与游标。运行资源保持1核/256MiB，记录耗时/CPU/RSS，不能只看一轮噪声给倍速承诺。

管理测试覆盖未开始编辑/容量溢出无半批、已有结果fork不改原版、rename安全、删除运行中/暂停/完成/有series/有pending/study、重复请求/过期preview/越权、共享源、最高ID删除后重建、原队列不复活。删除大fixture验证每tick上限和崩溃重启继续。

存储测试在真实SQLite制造大量数据并删除，验证free页、转换及增量回收后db/WAL/目录实际变化；有读锁、磁盘空间不足、转换中断保留可恢复结果。发布前生产一致副本比对旧列；线上仅GET/健康/进度恢复核验，不拿真实实验做删除演示。

## 7. 最终实现与已确认验收（2026-11-27）

- 性能实现为tick内thread-local SQLite连接复用、每次业务提交仍单独事务、元数据工作项读取，未改风控/赔率/逐期事件时钟；普通100版本平均约1.78×、92组合约1.82×、连续档案约1.46×，三个经济账本摘要完全一致。实测口径及原始证据见performance.md，不承诺所有线上负载固定倍速。
- 管理数据及关联回归99项通过；最终独立并发/身份/共享源/CLI反例8项通过；CLI/真实空间回收专项5项通过。
- 原页面、金额重测、连续续算三条旧真实浏览器流程已由根代理确认退出码0。新管理真实浏览器流程也已确认退出码0：`node tests/test_rule_lab_management_ui.cjs`，证据`/tmp/pc28-management-ui-final.log`归档原工具输出而非重跑。
- 最终冻结ROOT项目回归1197项全部通过，0失败、0跳过，329.800秒，退出码0；W4通过，W5仍等待实际发布。

最终全量门禁记录补充：首轮1194项中1193通过、1失败（CLI未知服务状态保护，根代理确认该进程使用修复前已导入模块）。修复后的独立8项和CLI5项新进程均通过；冻结代码完整重跑`/tmp/pc28-management-full-final-root.log`已完成：1197通过、0失败、0跳过，329.800秒，session21060退出码0。W4已据最终实际结果放行；W5保持待发布。
