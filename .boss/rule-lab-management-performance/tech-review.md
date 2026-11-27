# 实验管理、性能与空间回收技术评审

## 摘要

- **结论：有条件通过，立即开发；无需等九批回测结束。** 开发和验证使用隔离工作树/副本；上线只短暂停止实验写入，保留运行状态和进度。
- 性能必须有相同输入、相同版本数、相同资源下的前后实测及结果一致性证明，不能用减少实验或放宽风控换速度。
- 编辑分为直接改名称、未开始时改定义、已有成绩后复制新版本；删除必须停止所有恢复入口，并保护共享输入和比较组关系。
- 空间回收分为记录删除、内部可复用页、实际文件缩小。旧库 NONE→INCREMENTAL 需一次受控 VACUUM；线上普通请求不能执行完整 VACUUM。
- 用户已授权实现及必要可逆上线；高影响文件/加法结构由本轮授权覆盖。**本轮没有授权删除任何现有真实实验**，删除验收仅用临时库。

## 事实与范围

基线 `20cb785`，工作树 `/www/wwwroot/pc28touzhu-lab-management`。Python标准库WSGI、SQLite WAL、原生HTML/JS/CSS；CI只有Docker构建推送，无新增依赖。运行上限100、保存650、单worker1核/256MiB、实验目录1GiB。当前9/10运行，11～17等待一次性队列；总602版本。普通回测和连续系列均在范围内。

代码确认：`series._configuration` 不包含名称，包含来源、玩法、模板、注额、风控、条件/赔率与ID。现有主键为普通INTEGER PRIMARY KEY，删除最高ID后可能复用；新删除功能必须补永久单调ID分配。study.source_batch_id指向原批次，child有唯一study归属，不能按孤立batch清理。

## 阻塞风险

| ID | 风险 | 必须处理/证据 |
|---|---|---|
| R1 | 性能缓存看不到暂停/删除或改变逐期时序 | revision仍在事务内复核；真实两连接并发、暂停race、删除race；优化前后经济状态逐项一致 |
| R2 | 删除期间worker/每日调度/外部批准队列重新写入 | 同事务标记deleting、禁开关、revision失效、队列排除；commit_step拒绝全部写入；重启仍不恢复 |
| R3 | ID复用让旧清单/回执误操作新实验 | 持久单调分配batch/study/experiment ID，覆盖所有创建/复制/重测入口；删除最高ID后再建测试 |
| R4 | 删除一批误删其他消费者共享输入 | 精确归属清理；仅最后消费者移除后回收无引用stream/档案；旧普通批次/未启动批次的输入需求也要计入 |
| R5 | 修改已算策略污染原成绩；编辑排队批次后自动开跑 | 有成绩只能fork新暂停版本；原ID/账本保持；pristine edit显式退出已批准队列，rename不退出 |
| R6 | 大量DELETE/VACUUM锁住API或耗尽磁盘 | 删除job分批≤500行；完整VACUUM仅维护CLI且预检磁盘/停实验写者；后续incremental_vacuum有正页上限 |
| R7 | WAL/空闲页被当成已释放磁盘，空间门禁阻止复用 | 分开报告实际目录、DB/WAL、freelist可复用；容量预检考虑可复用页但不超文件系统预算；实测物理缩小 |
| R8 | 请求重试、旧preview、跨owner、study依赖 | 幂等请求+opaque revision+事务所有权；删除回执在对象删完后仍可读；依赖study时明确409 |

## 上线决策

可以在实验仍运行时开发；上线前记录各批实际游标、累计净利和pending，停止队列、实验worker及API三个写者，备份独立实验库。新结构加法迁移和空间模式转换通过后，比对全部旧业务列、quick_check/FK，再按原服务状态恢复。上游预测库、真实下注账本和其他服务不参与转换。若临时空间不足，则阻止维护转换而不是删除数据腾空间。

SQLite依据：https://www.sqlite.org/pragma.html#pragma_auto_vacuum 、https://www.sqlite.org/lang_vacuum.html 。DELETE在NONE模式只产生freelist；INCREMENTAL需已有pointer map；旧库转换需VACUUM；完整VACUUM可能需约两倍原库大小的额外空闲空间。WAL收缩需检查checkpoint结果，busy不等于成功。

**STATUS: DONE_WITH_CONCERNS**

**REASON:** 方案可进入实现；R1～R8须由下面的分阶段测试关闭后发布。

## 实现落地补充

性能选择tick内连接复用与元数据读取，所有业务commit仍独立原子提交；最终三种负载经济账本一致，实测吞吐约1.78×/1.82×/1.46×。管理使用专用generation/token，运行进度不导致用户确认必然过期；队列最后启动通过同事务start_queued_batch封住编辑竞态。ID保护扩展至capture_stream/archive_revision/series，避免分批删除中误关联新series。

CLI最终名称为prepare_rule_lab_storage.py；无法确认任何一个实验写者停止时拒绝apply。独立QA8项与CLI5项已经通过；最终冻结全项目回归1197项全部通过（0失败、0跳过，329.800秒，退出码0），W4验收放行；实际发布及线上核验尚未执行，W5保持pending。
