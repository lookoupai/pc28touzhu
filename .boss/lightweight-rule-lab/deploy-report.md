# 轻量规则实验室发布记录

## 摘要

- **已上线并完成核验。** 访问 `https://tg.dca.de5.net/rule-lab`；管理员入口，模拟库为空，未创建或启动任何实验。
- 功能代码为 `a3415a6cfc50d5c188218cc1089b1c68b4f998d1`，主代理已推送 GitHub feature/main 并快进生产；发布中修正了 WAL 辅助文件访问，最终 unit 已生效，该小修复由主代理另行提交同步。
- 新 worker 实测限制为 0.2 核、内存高水位 192MiB／硬上限 256MiB、swap 0；空闲 cgroup 内存约 9.2MiB，目录约 229KiB。空闲观测不等于满载容量测试，负载证据见 QA 报告。
- 只主动重启平台 API 与新模拟 worker。平台短暂不可用导致原执行器自动退出并在约 2 秒后恢复；其他 6 个既有 worker PID 未变。发布后执行器心跳 online，未人为创建投注或发送测试消息。

## 版本、时间与实际动作

- 生产目录：`/www/wwwroot/pc28touzhu`；发布前 `1209e3b14fe8163b7f490fcaa67a0ddda83b80bd`，本次功能 `a3415a6cfc50d5c188218cc1089b1c68b4f998d1`。
- 2026-11-26 06:15:35 UTC：安装并启用 `pc28touzhu-rule-lab.service`，仅主动重启 `pc28touzhu-platform.service` 加载 UI/API。
- 2026-11-26 06:29:08 UTC：WAL 沙箱问题修复后仅重启新模拟 worker，最终 PID `604711`，NRestarts=0。
- 2026-11-26 06:35 UTC：健康、空库与心跳最终核验通过。Git 提交、合并和推送均由主代理统一执行，DevOps未并发改Git历史。

实际安装过程复用 `deploy/systemd/pc28touzhu-rule-lab.service` 和现有 `pc28-python.sh`，无依赖安装、无上游代码或清理配置修改、无主库迁移、无新timer/端口/Nginx/DNS变更。模拟服务不加载生产 `.env`。

按 `git diff --name-only 1209e3b a3415a6 -- src` 精确检查并将本次10个代码/静态文件设为0644，使www可读；未递归改项目或凭证权限。新模拟目录 `www:www 0750`，数据库/WAL/SHM/锁文件为0600。

## 运行与HTTP证据

| 检查 | 实际结果 | 证据 |
| --- | --- | --- |
| 新服务 | enabled、active/running、PID604711、NRestarts=0 | `systemctl show/is-enabled pc28touzhu-rule-lab.service` |
| 平台API | PID598763、active/running；本机和公网HTTPS `/api/health` 均200、status=ok | 实际HTTP GET与systemctl |
| 匿名实验访问 | `/rule-lab`、`/api/platform/rule-lab`、`/api/platform/rule-lab/options` 均401 | `/tmp/pc28-rule-lab-http-verification.json` |
| 管理员页面/API | page200；options200，15个可用来源、恰好两套OK；overview200，0批次 | 同上；仅内存生成Cookie并GET，不输出/落地凭证 |
| 静态文件 | rule_lab.js和rule_lab.css均200 | 同上 |
| 空库 | batch/experiment/decision/daily均0；worker idle，heartbeat `2026-11-26T06:35:24.728482Z`，最近tick0.967ms | `/tmp/pc28-rule-lab-empty-ledger.json` |
| 存储 | 最终采样234176 bytes，含DB/WAL/SHM/lock；应用预算1073741824 bytes | 同上 |

线上没有现存普通活跃用户供403实测，未为验证创建用户；非管理员403及跨管理员404已由临时数据库API/E2E测试覆盖，不能将其写成已在生产实测。

## 生效资源与权限

`systemctl show`最终实测：

| 属性 | 实际值 |
| --- | --- |
| CPUQuotaPerSecUSec | 200ms，即平均0.2核 |
| MemoryHigh / MemoryMax / MemorySwapMax | 201326592 / 268435456 / 0 bytes |
| MemoryCurrent / MemoryPeak | 9666560 / 9924608 bytes（空闲采样） |
| TasksMax / Nice / IOSchedulingClass | 16 / 10 / 3（idle） |
| PrivateNetwork / ProtectSystem | yes / strict |
| LogRateLimitInterval / Burst | 1min / 20 |
| ReadWritePaths | 模拟目录与上游项目目录 |
| ReadOnlyPaths | 上游pc28_predictor.db主文件与投注端项目目录；嵌套模拟目录例外可写 |

目录1GiB是应用预算，不是文件系统配额；不包含共享journal。没有将此空闲结果当作100规则负载结果，满载fixture性能与RSS见 `.boss/lightweight-rule-lab/qa-report.md`。

## 发布中发现并修复的WAL访问问题

第一版将整个源项目只读挂载。实际进入worker mount/net命名空间、降权为www后，`RuleLabSource.list_predictors()`在SQLite `PRAGMA cache_size`时报“unable to open database file”。诊断确认源库是WAL模式，但上游最后连接关闭时会删除 `-wal`／`-shm`；源主文件可读，缺失的辅助文件无法在只读目录创建。

同样的host `mode=ro`／`query_only`连接可以打开，并创建辅助文件；持有该只读连接期间，原沙箱也立即可读取。这确认是辅助文件生命周期问题。未使用 `immutable=1`、`nolock=1`、全库复制或改上游配置。

主代理批准的最小修复：源目录加入ReadWritePaths，以便SQLite管理WAL/SHM；源主文件单独ReadOnlyPaths绑定，连接继续mode=ro/query_only；两项目 `.env` 对worker屏蔽。权限取舍是源目录内其他未单独保护的文件也对该服务用户可写，不能宣称仅模拟目录可写。源业务表不被本功能写入。若维护更换源主文件inode，须重启lab以刷新绑定。

修复后实际probe进入PID604711的mount/net命名空间并降权www，结果如下（`/tmp/pc28-rule-lab-sandbox-probe.json`）：

- 开始时 `-wal` 与 `-shm` 均不存在，公开预测器列表及最多5期开奖读取成功，耗时5.654ms。
- 源主文件mount只读；`os.open(..., O_WRONLY)`被errno30拒绝，没有写入字节。
- 同一mode=ro/query_only连接执行零行`UPDATE predictors SET id=id WHERE 0`被readonly拒绝，未修改业务记录。
- 源 `.env`、投注端 `.env` 和投注主账本均不可读；模拟目录可写，源目录可管理辅助文件。

probe未继承worker seccomp过滤，但确实运行在其mount/net命名空间、相同www身份；实际RestrictAddressFamilies/PrivateNetwork等由systemd属性核验。没有访问外网、建立真实投注或复制业务数据。

## 既有服务影响与恢复

发布前PID快照：`/tmp/pc28-rule-lab-deploy-before.json`；恢复核验：`/tmp/pc28-rule-lab-existing-services-final.json`。

- 平台API由3504827变为598763，为计划内重启。
- 来源同步3504843、自动触发4152579、自动结算3504848、异常通知3144602、收益Bot596691、日报3144607均保持原PID且active/running。
- 执行器原PID4152850、NRestarts5；06:15:35 UTC日志出现Connection refused并exit1，06:15:37系统自动恢复为PID598807、NRestarts6。DevOps未执行该服务restart命令，但平台重启确实产生了这次短暂影响。
- 06:33:22 UTC只读查询 `executor_instances`：online，last_seen `06:33:21Z`，距采样1.35秒。最近10个任务都是发布前已delivered；未出现验证任务。未以这10行宣称完整账本审计或所有历史任务均无影响。

无需修改旧执行器代码；当前服务已恢复。本次没有人为创建实验、真实任务或发送消息。

## 运维测试与门禁

| 范围 | 命令 | 退出码 | 通过／失败／跳过 | 说明 |
| --- | --- | ---: | --- | --- |
| 初始RED | `PYTHONPATH=src python3 -m unittest tests.test_rule_lab_ops -q` | 1 | 0／4／0 | unit未创建；`/tmp/pc28-rule-lab-ops-red.log` |
| 初始GREEN | 同上 | 0 | 4／0／0 | `/tmp/pc28-rule-lab-ops-green.log` |
| WAL修订RED | 同上 | 1 | 3／1／0 | 原unit尚未允许辅助文件目录；`/tmp/pc28-rule-lab-ops-wal-red.log` |
| WAL修订GREEN | 同上 | 0 | 4／0／0 | `/tmp/pc28-rule-lab-ops-wal-green.log` |
| unit验证 | `systemd-analyze verify /etc/systemd/system/pc28touzhu-rule-lab.service` | 0 | 1／0／0 | 实际安装版本无错误输出 |
| diff | `git diff --check` | 0 | 1／0／0 | 开发工作树 |

QA另行独立复跑ops+source共16项通过；功能完整回归、coverage、浏览器和性能以QA报告为准，不与重复运行计数相加。

## 回退与后续

需要回退时先停止并禁用新lab服务，保留独立模拟库；由主代理回退本次代码并重启平台API。不要覆盖生产账本。平台重启前应考虑现有执行器对短暂API不可用的恢复行为，重启后核对心跳。

用户现在可在页面选择来源、模式与玩法，保存50或78个暂停实验，再自行启动。资源限制和两套OK预设已经生效。新模拟功能发布后的unit修复及本报告等待主代理统一提交、推送并同步生产跟踪文件；运行中的/etc unit已是修复版本。
