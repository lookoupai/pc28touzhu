# 风控比较组与资源队列增量合同

## 摘要

- 基线0cc7c75；仅扩展独立lab schema v3、现有API/UI/worker，无新依赖或业务库写入。
- 从本人backtest+round批次复制五个fresh子批次，每条件及两OK完整冻结snapshot原样复制；原批次独立保留，成绩不拿来充当基准。
- 整组创建同事务、幂等；组默认paused。全局最多100running，同组一次一个子批次，FIFO排队和阶段轮转。
- 组首次启动冻结统一开奖/时间截止；warm和逐期输入顺序指纹一致且所有子批次完成才可比，其他情况不显示相对增益或冠军。

## 五候选

`baseline`完整原policy；`round_tp_half`仅单轮止盈减半；`round_sl_half`仅单轮止损减半；`day_sl_half`仅主日层止损减半；`gate_off`关闭主日及additional所有日层hit_rate_gate.enabled。来源说明标为复制后修改，其余policy逐字段保留。

只有对应enabled且正数、半值至少0.01、两位金额可表达且与原值不同才生成减半候选；否则preview中unavailable+中文原因。金额按分整数减半向下取整（例如0.03→0.01，明确显示实际变化，不声称精确50%）。gate原本全部关闭则不可用。规范policy去重，同值只保留首个候选。baseline必有，少于两种有效方案拒绝创建。当前10/10/20/25%配置生成5×78=390版本；加原78共468，仍受500保存上限。

`day_sl_half`不改additional guard；此候选显示“主日止损20→10，附加层保持”，避免两层都改的多因素混淆。gate_off是同一种开关因素作用于所有日层。

## 存储与事务

新增lab_studies：id,user_id,request_id,name,source_batch_id,request_hash,status,queued_at,cutoff_issue,cutoff_at,error,created_at,updated_at。
新增lab_study_batches：study_id,batch_id UNIQUE,variant_key,position,label,change_json；外键及同study variant唯一。无旧表列修改，user_version=3；已有v2旧列/行不变。

创建事务重新读取本人源批次与实验，检查源backtest+round、未归属其他study、两OK配对/模板有效、容量及每人batch50；生成全部child、experiments及关系。失败整体rollback。child配置source/stake/template/dates、conditions、完整settlement snapshot与原值相同，只risk policy不同。request_id为ASCII 8..80安全字符，unique(user_id,request_id)，同请求重试返回同组；同token不同source/name拒409，不创建部分记录。

子批次继续用现有status；parent status=paused/queued/running/completed/error/resource_paused。child只能通过parent控制，普通batch state/risk-policy返回409 study_managed。paused parent原子暂停当前running child、revision+1，已冻结pending仍按已有模拟时钟结算。resume入队继续既有cursor，completed不可重置。

调度事务：先对running parent核对当前child终态；completed→余child则parent重新queued排队尾、全部则completed；error/resource_paused→parent同态，不跳过剩余。按queued_at,id FIFO取队头，容量不足继续queued显示原因而非error。额足时才running child、revision+1、started_at保持首值。一个study最多一running；全服务100总额度与单batch抢额同BEGIN IMMEDIATE。

首次调度需从源只读取得latest并冻结cutoff_issue/cutoff_at（首次成功入队后worker拿到）；同group后续child禁止重新取latest作为回放上界。无源时保留queued并报告原因，错误可恢复但不伪造截止。worker每tick尝试调度，空队列/额度占用也保留心跳，main无running时最多15秒再次尝试。

## 输入可比性

每个child cursor追加evidence={digest,count,warm_count,first_issue,last_issue,invalid/conflict标志}。SHA256链按canonical JSON记录warm100和逐期输入：期号、三球、open_at、source_created_at、预测方向、created_at、settled_at、status，以及预测缺失标记；排除source row id、observed_at与采集运行时刻。逐处理期和cursor在同事务更新，重启/分页不重复hash。warm独立前缀防边界混淆。

整个group共同cutoff时间门控，source_created_at或预测created_at晚于cutoff不作为统一输入；源后加期号超过cutoff不进入子批次。历史模拟时钟/晚公布pending规则保持。

最终仅当所有有效child completed、warm及目标digest/count/首末覆盖一致、有效目标count>0、没有输入冲突/清理中断时`comparable=true`。输入修订使fingerprint不同或cache冲突即不可比；返回明确reason，不显示delta、不按利润排序冠军。缺失预测、无效开奖等跳过计数一并比较；相同缺口不等于全日期完整，显示实际首末期/期数和警告。

指纹不是不可变源档案：当前源历史版本可能已被覆盖，报告标“同一实际观察输入”，不声称恢复最初预测发布版本。若后排因清理/修订不一致，保留成绩但重新创建有效覆盖区间再比较。

## HTTP合同

已有admin+owner及16KB请求限制沿用。新API返回直接JSON对象，错误沿RuleLabError。

| 方法路径（前缀/api/platform/rule-lab） | 请求 | 响应 |
|---|---|---|
| GET /batches/{id}/comparison-preview | 无 | source_batch_id,variants[{key,label,available,reason,policy,changes}],candidate_count,new_experiments,capacity{saved,running,max_saved,max_running,remaining_saved} |
| POST /studies | {source_batch_id:int,name:str,request_id:str} | 201 {study,created:bool}；重试200同study；容量409整体不创建 |
| GET /studies | 无 | {studies:Study[]}本人最多50 |
| POST /studies/{id}/state | {state:'running'|'paused'} | {study}；running请求意味着加入队列 |
| GET /studies/{id} | 无 | {study,rows:ComparisonRow[]}最多500行 |
| GET /studies/{id}/export.csv | 无 | BOM CSV，公式防注入，含risk/cutoff/evidence/comparable/delta |

Study：id,name,source_batch_id,status,created_at,updated_at,cutoff_issue/null,cutoff_at/null,error,waiting_reason,children[{batch_id,variant_key,label,status,processed_issues,experiment_count,changes}],completed_children,total_children,comparable,comparison_reason,actual_coverage。

ComparisonRow：experiment_id,batch_id,condition_key,condition/name,settlement_rule_id/name,variant_key/label,risk_policy,net_profit,roi_percent,stake_total,settled_count,max_drawdown,worst_day_profit|null,profitable_days,betting_days,completed_rounds,sample_label,comparable,delta_net_profit|null,delta_roi_pp|null,delta_drawdown|null。差值只与同condition+同OK fresh baseline对应；ROI分母是各方案实际投注量，差值标百分点。不得把所有互斥实验利润求和当实盘收益。

## 实现顺序与风险

新pure domain/rule_lab_comparison.py生成候选/指纹；store组事务与调度；service/API；worker截止/证据；页面和CSV；测试。旧单批次行为不改变。

风险：500保存额度390创建原子；暂停与worker提交revision；输入晚修订且共享cache首次冻结不能伪装一致；回放末尾pending不得提前结算；组错误不得悄悄跳过。全由针对性测试和root独立复验判定。资源仍0.2核/256MiB/1GiB，无自动启动新组。
