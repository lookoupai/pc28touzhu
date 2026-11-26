# 风控比较组开发验证

## 当前状态

实现、开发验证与root独立完整验证均通过，正式代码已冻结；**尚未部署、没有创建或启动生产比较组**。原批次和业务主库未改。

## 实际测试证据

| 项目 | 结果 | 记录 |
|---|---|---|
| 新比较模块RED | 因功能不存在失败，随后实现 | `/tmp/pc28-comparison-red.log` |
| 比较+API targeted | 35项通过11.463秒 | `/tmp/pc28-comparison-w2.log` |
| 最终lab targeted | 125项通过、0失败，23.280秒 | `/tmp/pc28-comparison-targeted-final.log` |
| 最终真实浏览器 | exit0；现有流程和比较preview/创建/启动/暂停恢复/全部完成/delta/CSV/权限/窄屏通过 | `/tmp/pc28-comparison-ui-final.log` |
| 语法/diff | node --check及git diff --check通过 | 终端实际结果 |
| root独立完整回归 | **969项通过、0失败，237.434秒，exit0** | `/tmp/pc28-comparison-full.log` |
| root独立浏览器 | exit0 | `/tmp/pc28-comparison-root-ui.log` |

用例数有重叠，不能相加。root最终覆盖摘要见`evidence/coverage-summary.json`：新增pure comparison模块43/44语句覆盖**97.73%**；本次4模块1094/1244语句覆盖**87.94%**、394/528分支覆盖**74.62%**，合并显示83.97%。worker语句覆盖77.93%，原main运行循环等不在该覆盖快照中；未为了比例重复旧路径，不声称全项目覆盖率或所有运行场景均被验证。

## root独立迁移与资源验证

生产lab一致副本由v2升级v3，两次initialize后9张原表全部旧列逐行一致、quick_check=ok；摘要`evidence/migration.json`。生产库本身未在此验证中修改。

`evidence/benchmark.json`记录390比较版本＋原78暂停版本=468保存版本，峰值运行78；五个风控方案各120期全部完成且comparable=true，源文件哈希不变。wall **37.851秒**、CPU **33.714秒**、峰值RSS **40,124KiB（约39.18MiB）**；模拟目录从2,076,672增加到3,158,016字节，21次tick最大约2.060秒，比较查询77.912ms。

资源测试采用临时fixture、未施加cgroup CPU节流，不能把该耗时当成生产0.2核下的完成时间或SLA。测量确认保存390但仅运行78的调度边界，没有提高原100运行/500保存限制；长期生产负载和极端磁盘场景仍不在该样本内。

## 核心覆盖

- 五单因素候选、不可用原因、规范去重；复制fresh基准及原条件/两OK冻结快照不重建目录；原来源/注额/日期不变。
- 390版本整组原子创建、原78+390=468；500超额无半组；第三子组故障注入整体rollback；并发同request_id只建一组，相同token异意图409。
- group控制child，单独start/edit409；全局100额度；已有批次占额queued；FIFO队头/子完成重排队尾；pause与worker stale revision；resume不重置进度；child失败阻止余组自动运行。
- 每个child共同cutoff；后添加开奖不进入后child；真实五child按不同page1/200、每tick重启仍证据一致可比；源修订后不可比、delta=null；待结算输入变到cutoff后明确error而非无限pending。
- 暖机与逐期指纹覆盖实际缓存、实际条件特征、源方向/时间及缺失/冲突，排除观察时间。中断暖机提交后恢复仍补唯一warm指纹。晚created/settled按共同cutoff屏蔽。
- v2真实schema自包含fixture→v3重复初始化，全部旧列/行相同；既有v1迁移仍保留旧列、收益、pending与游标。
- API admin+owner，未知字段/非法请求标识、CSV可比信息及公式转义；UI不把互斥收益加总。
- 浏览器真实完成250版本小fixture（资源额度允许）；390方案在已有更多批次时显示总568/500并禁止创建，未修改服务端限额。完成后CSV有真实收益5.88和可比True，baseline差值0。

## 限制与说明

共同截止+指纹一致表示此次实际观察输入一致，不证明源原始历史预测不可变。相同缺口可能仍有覆盖不足，展示真实期数/跳过原因并保留样本提示。后写入且cutoff后不可见的记录可能挤占源100期暖机范围，导致后child可用暖机不足；指纹拒绝比较，不静默补造。

指纹不包含实际运行观察时间、策略盈亏或本次入账时间；日跳过汇总中预测无效按逐处理期记录，避免不同分页边界重复统计造成假不一致。只有全部completed、同覆盖指纹、无冲突、存在历史输入才显示delta；零结算行delta仍为空。

UI没有收益冠军排序、预测完成耗时或未经验证完成百分比，默认条件+OK配对比较。组默认paused，用户显式启动才占运行额度。资源unit及0.2核/256MiB/1GiB限制未变。
