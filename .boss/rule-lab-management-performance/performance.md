# 回放性能优化证据

## 摘要

- 优化不改变赔率、条件、风险公式、逐期事件时间或原子记账边界；在单次有界 worker tick 内复用 SQLite 连接，每次业务提交仍单独提交。
- 待结算入口先读取批次/时钟元数据，确实有待结算时再读取实验定义，减少重复解码。
- 临时输入、固定模拟时钟、单 CPU affinity、256 MiB 地址空间限制下，普通100版本、92组合版本和连续档案三种负载的逐笔/资金/风控/日账/轮次摘要均与旧代码完全一致。
- 删除/空间回收在每次 tick 执行一次有界维护，删除对象从运行快照中消失不会把其他批次标成错误。

## 基线与方法

旧版本为 `20cb785`。首次性能诊断在该版本代码尚未改动时执行；后续对照通过 `git archive 20cb785 src` 提取旧代码到 `/tmp/pc28-management-perf-baseline-20cb785`，不启动生产 worker、不读取生产输入或账本。新测试工具 `tests/rule_lab_performance_benchmark.py` 在两份代码上生成相同临时 SQLite 数据。

基准固定为 1 元、单轮±10、日±20、20次结算后25%命中率门槛、冷却10期，输入是有连段和混合结果的确定性fixture。大小和单双各50版本同时运行；组合92版本单独运行；连续档案场景先建立前一天100期基线，再追加200期。不是旧版无风控的轻负载基准。

进程固定到宿主可用CPU列表的第一个CPU；使用 `resource.RLIMIT_AS=256MiB`。未提高 worker tick 的2秒预算。计时覆盖真实worker循环和SQLite提交，不包括fixture创建；连续模式的历史基线准备和输入采集不计入追加耗时，两边口径一致。宿主同时运行其他服务，单次速度不等于线上保证。

## 热点测量

带 cProfile 的旧代码100版本、每批400期：55.160秒，21,876,415次函数调用。

| 热点 | 次数 | 累计秒数 |
|---|---:|---:|
| get_work_item | 3,005 | 24.767 |
| JSON loads | 859,723 | 18.027 |
| SQLite连接生命周期 `_connect` | 14,522生成器步进，7,892个连接 | 15.801 |
| SQLite commit | 1,488 | 5.146 |
| SQLite close | 7,892 | 4.351 |

上述累计耗时存在调用嵌套，不能直接相加。性能对比使用下节**无 profiler**数据。

## 无 profiler 对照

| 负载 | 旧版墙钟秒 | 优化后墙钟秒 | 吞吐倍率 | 旧版CPU秒 | 优化后CPU秒 | 优化后峰值RSS |
|---|---:|---:|---:|---:|---:|---:|
| 大小+单双100版本、800批次期数（最终集成样本） | 43.5735 | 24.5486 | 1.775× | 37.5543 | 22.4835 | 43,324 KiB |
| 组合92版本、200期 | 13.1270 | 7.2206 | 1.818× | 12.1081 | 6.8704 | 41,368 KiB |
| 连续档案100版本、400批次期数 | 31.1258 | 21.2883 | 1.462× | 27.2555 | 19.4497 | 50,184 KiB |

早期普通100版本样本在加入管理维护前测得46.2547→29.4281秒，只用于原型方向验证；最终结论采用已经集成管理维护的样本。最终100版本第二次重复为25.8546秒、CPU23.1491秒、RSS43,724KiB，摘要仍相同。两个旧版样本43.5735/46.2547秒，两个最终新版样本24.5486/25.8546秒；均值44.9141→25.2016秒，约1.78×吞吐、43.9%耗时减少。相同宿主负载有波动，因此不把单次最优值作为承诺。

各场景的经济账本 SHA256（旧版与新版一致）：

- 普通100版本：`09ddcae770e76d13efb094539c22bb93e8f1a372acb35983ca176b4904b1e6c5`，6,981条模拟决策。
- 92组合：`92c8bfff207735342b53d2ac8f6ecfb1f86fd797ef9afdd309063b30bfd7c52d`，664条模拟决策。
- 连续档案：`9a04790e1cac5a4b331de05824542756132ff57e8c57edc672aa32cfe7e55ce1`，7,075条模拟决策（含已有基线）。

摘要覆盖每实验 stats/risk_runtime/决定与结算水位、逐笔下注方向/金额/时点/状态/结果/轮号、全部日账/日风险/轮次及批次处理数/跳过统计。物理数据库字节、事务revision次数和tick数可能变化，不应作为经济结果比较字段。

## 正确性与管理集成

- `worker_session()` 仅复用连接，不跨业务操作保留事务。每次显式读快照退出后rollback结束，外部暂停/编辑立即可见；所有写入仍核对最新revision。
- 连接是thread-local、仅限tick；嵌套正在使用的连接不会被另一个操作借用。异常只回滚当前事务，已成功提交不回滚，tick结束关闭连接释放WAL依赖。
- `get_work_batch()` 返回与完整工作项相同的批次/续算/时钟上下文，不解码实验JSON。
- `management_maintenance(limit=500,max_pages=256)` 在回放前有界执行，job忙不停止其他批次；持久待处理状态驱动0.25秒间隔继续，空闲时也可完成删除。
- 删除期间旧工作快照读取到404/deleting时跳过；错误处理不会把已删除对象重新写回错误状态。

## 测试证据

| 命令/范围 | 退出码 | 通过/失败/跳过 | 证据 |
|---|---:|---|---|
| `env PYTHONPATH=src python3 -m unittest tests.test_rule_lab_performance -v`（初始session红测） | 1 | 0/4/0 | `/tmp/pc28-management-perf-red.log`，缺少worker_session/get_work_batch |
| 同上（最终6项） | 0 | 6/0/0 | `/tmp/pc28-management-worker-final-green.log` |
| 用旧20cb785 worker + 当前管理store执行两项维护集成例 | 1 | 0/2/0 | `/tmp/pc28-management-worker-prior-hook-red.log`，空闲删除不推进、运行中删除把tick标error |
| `env PYTHONPATH=src python3 -m unittest tests.test_rule_lab_performance tests.test_rule_lab_worker tests.test_rule_lab_continuous_worker -q` | 0 | 48/0/0 | `/tmp/pc28-management-worker-integrated.log`，24.489秒 |
| 初始优化旧worker/store/risk/comparison/continuous/QA回归 | 0 | 99/0/2 | `/tmp/pc28-management-perf-regression.log`，101项；2项生产副本需显式环境变量，未计为通过 |

三个基准均通过完整处理数量与completed状态断言，且经济摘要相同。原始数据为 `/tmp/pc28-management-{perf-before-unprofiled,paired-repeat-before,paired-repeat-after,combo-before,combo-after,continuous-before,continuous-after,paired-final-repeat2}.json`；profiler为 `/tmp/pc28-management-perf-before.prof` 及 `.txt`。

## 可复现命令

当前代码：

```bash
env PYTHONPATH=src python3 -m tests.rule_lab_performance_benchmark --scenario paired --issues 400
env PYTHONPATH=src python3 -m tests.rule_lab_performance_benchmark --scenario combo --issues 200
env PYTHONPATH=src python3 -m tests.rule_lab_performance_benchmark --scenario continuous --issues 200
```

旧代码使用同一benchmark脚本，放在旧代码临时目录中执行，PYTHONPATH先指向旧src、再包含当前src及repo根（后两者供现有tests bootstrap识别，避免其把当前src插到旧版之前）。两边在独立新进程生成新临时库。

本证据不代表所有历史区间都会获得相同倍速；当前优化主要减少连接和JSON重复工作，实际触发量、数据库大小和上游读取延迟仍影响耗时。
