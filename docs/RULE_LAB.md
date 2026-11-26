# 轻量规则实验室

管理员登录后从导航打开“规则实验室”，地址为 `/rule-lab`。现有服务器入口是 `https://tg.dca.de5.net/rule-lab`。实验只做模拟记账，不创建执行任务、不发送投注消息；不同管理员只能查看自己的批次。

## 使用方式

1. 选择公开的 PC28 预测器、大小／单双／组合、固定每注金额，以及“实时模拟”或“历史回放”。历史日期按北京时间计算，最多 90 天。
2. 保存批次。大小或单双生成 25 个条件、50 个实验版本；组合生成 39 个条件、78 个实验版本。每个条件都包含以下两种结算预设：

   | 预设 | 大小／单双 | 小单／大双 | 大单／小双 | 退本 |
   | --- | ---: | ---: | ---: | --- |
   | OK 游戏网盘 `pc28_netdisk_regular` | 1.98 | 3.6 | 4.2 | 无特殊退本 |
   | OK 游戏高赔常规 `pc28_high_regular` | 2.846 | 6.78 | 6.33 | 命中遇 13／14、对子、顺子（含环绕）或豹子退本；未中全亏 |

3. 新批次保存后为**暂停**，点击启动才开始模拟。全服务最多同时运行 100 个实验版本、保存 500 个版本；额度由所有管理员共享。暂停会释放运行额度，保留成绩；已冻结的待结算记录仍会完成结算。
4. 对照同一条件两套规则的实赔、退本、亏损、净收益、ROI、最大回撤及样本量。无条件跟随是基准。ROI 为净收益除以累计投注额，不是每轮本金收益率。
5. 点击实验查看最近 90 个有记录日的汇总，以及最近 50 条明细；可导出当前批次的汇总 CSV。页面手动刷新，不会因为打开榜单而运行回测。

本版采用**固定金额、单腿、逐期跟随**。不模拟倍投、多腿或过三关，不能把这里的实赔比例当作过关率。配置和赔率在创建时冻结；修改来源、金额或条件需要新建批次。金额按已有生产结算函数逐注保留两位，再用整数分累计；例如 1 元按高赔 2.846 实赔时净利记 1.85 元，赔率本身不会先改成 2.85。

## 历史回放与实时模拟

历史回放仅使用仍存在的逐期数据，按开奖前 40 秒的决策时点检查预测发布时间、历史开奖可见时间和历史预测结算时间。源库不是不可变版本档案，因此历史结果始终标为回顾性模拟，不冒充真正前瞻。缺期、缺三球、不完整窗口或过晚预测会跳过并计原因。

实时模拟先记录 worker 实际观察到的下一期待开奖预测和条件，再用随后公布的三球结算；开奖后还会复核决策至少早于开奖 40 秒。停机、暂停或资源不足期间错过的期数不会补造为实时投注。后来的预测方向修订不取消已冻结的决策；开奖三球冲突会保留待结算状态并报错，等待核对。

恢复历史批次时，若必需输入已清理且源也不能提供，会报 `input_expired`；需新建有数据覆盖的区间，不能把缺失数据当作零命中。源撤销共享或停用后停止新决策；公开权限仅接受已启用、共享逐期记录的 `records`／`analysis` 预测器。

## 资源与数据保留

所有实验共用精简开奖和预测记录。源库以 `mode=ro` 和短连接读取，单页最多 200 行、查询超时约 250ms；不复制大段提示词、模型响应或整库。单 worker 每次约 2 秒工作预算，实时采集间隔至少 15 秒，历史回放在间隙分批推进。

| 资源 | 限制与含义 |
| --- | --- |
| CPU | systemd `CPUQuota=20%`，即最多平均 0.2 核；4 核机器约总算力 5% |
| 内存 | `MemoryHigh=192M`、`MemoryMax=256M`、`MemorySwapMax=0`；超限可能终止 worker，重启后从事务检查点继续 |
| 进程／优先级 | 单实例文件锁，`TasksMax=16`、`Nice=10`、空闲 I/O 优先级 |
| 网络 | worker 私有网络命名空间，仅允许 Unix socket；不加载生产 `.env` 或读取投注账本 |
| 文件写入 | systemd 只放开 `data/rule_lab`；代码和源项目只读 |
| 存储预算 | 模拟目录 1GiB 应用预算，含 DB／WAL／SHM 和目录内临时文件；约 960MiB 时暂停新增模拟，SQLite 主文件另设较低页数上限以预留空间 |
| 日志 | 状态变化才输出，并由 journal 每分钟最多 20 条限速；系统 journal 不包含在模拟目录的 1GiB 中 |

1GiB 是应用预算，**不是文件系统硬配额**；既有共享磁盘、系统日志和手工放入的备份不受 SQLite 页数限制。不要将备份长期堆在模拟目录。磁盘不足时先保留已有成绩与暂停原因；SQLite 删除释放的页可复用，文件通常不会立即缩小。本功能不自动运行 `VACUUM`。

- 配置、累计成绩、防重水位和每日汇总长期保留；每日汇总目前没有自动 365 天删除或月合并。
- 已结算／已跳过明细按**实际完成入账时间 `finalized_at`**保留 30 天。今天回放旧日期产生的明细仍从今天起算；收益归属日仍按历史开奖北京时间。
- 待结算记录不按普通明细清理。精简输入保留约 90 天，并保护最新 100 期窗口、待结算目标和运行中回放所需暖机区间。
- worker 在有剩余预算时约每 5 分钟尝试一次清理，每次最多 500 行，不循环清空；这是进程内维护，没有新增定时器。资源暂停时仍可尝试清理。锁忙或持续计算会降低实际清理速度，不保证每天固定处理量。
- 清理明细不减少累计收益、最大回撤和日汇总；历史成绩仍可查看。仅有汇总不能重建逐期顺序，未来新条件只能回放仍保留的输入。

## 部署配置

复用服务器现有 systemd 和 Python launcher，无新增依赖、无上游修改、无生产主库迁移。以下命令仅在功能版本已合入生产目录并通过 QA 后执行。现有 `pc28-python.sh` 会选择项目虚拟环境并设置 `PYTHONPATH=src`。

worker 的两个路径由 unit 明确设置，网页不能传路径：

| 变量 | 默认部署值 |
| --- | --- |
| `RULE_LAB_DATA_DIR` | `/www/wwwroot/pc28touzhu/data/rule_lab` |
| `RULE_LAB_SOURCE_DB` | `/www/wwwroot/AITradingSimulator/pc28_predictor.db` |
| `PYTHONDONTWRITEBYTECODE` | `1`，只读代码目录不生成字节码 |

平台 API 默认使用同一模拟目录；若改路径，平台服务的环境与 worker unit、读写白名单必须一起调整，不能只改 worker。数据库用 `www` 用户创建，目录建议 `0750`，文件由 worker `UMask=0077` 创建。部署前只核对并调整本次新增/修改代码与静态资源的读取权限，**不要递归改变整个项目、`.env`、会话或账本权限**。

```bash
sudo install -d -o www -g www -m 0750 /www/wwwroot/pc28touzhu/data/rule_lab
sudo install -m 0644 /www/wwwroot/pc28touzhu/deploy/systemd/pc28touzhu-rule-lab.service /etc/systemd/system/pc28touzhu-rule-lab.service
sudo systemd-analyze verify /etc/systemd/system/pc28touzhu-rule-lab.service
sudo systemctl daemon-reload
sudo systemctl enable --now pc28touzhu-rule-lab.service
sudo systemctl restart pc28touzhu-platform.service
```

只重启平台 API 加载新页面/接口；现有执行器、自动触发、结算和来源同步服务不需要重启。启动新 worker 只创建空模拟库，不生成实验、不启用实盘规则。无须新增 timer、开放端口、改变 Nginx 或 DNS。

源库当前采用 WAL。`www` 必须能读取数据库及已存在的 `-wal`／`-shm` 文件；沙箱不给源库写权限。若源维护期间文件不可读，worker 会报告来源暂不可读并重试，不能通过 `immutable=1` 跳过 WAL 一致性，也不能扩大源目录写权限来规避问题。

## 验证、监控与恢复

```bash
systemctl status pc28touzhu-rule-lab.service --no-pager
systemctl show pc28touzhu-rule-lab.service -p ActiveState -p SubState -p MainPID -p NRestarts -p CPUQuotaPerSecUSec -p MemoryCurrent -p MemoryPeak -p MemoryMax -p MemorySwapMax
journalctl -u pc28touzhu-rule-lab.service -n 30 --no-pager
curl --fail --max-time 5 http://127.0.0.1:35100/api/health
```

登录页面核对来源可选、两个 OK 预设、worker 心跳和存储预算。心跳超过 60 秒未更新显示过期，应结合服务状态和日志查看；健康接口只能证明平台在响应，不能代替模拟结果核验。未经登录的实验 API 应返回 401，非管理员返回 403，其他管理员不能读到自己的批次。

`resource_paused` 表示暂停新增工作，已记录成绩仍可读。确认可回收明细已按策略清理、目录没有额外备份、磁盘可用后，再从页面恢复；不要删除数据库来“修复”空间错误。若服务因连续失败进入启动限制，先排查原因，再执行：

```bash
sudo systemctl reset-failed pc28touzhu-rule-lab.service
sudo systemctl start pc28touzhu-rule-lab.service
```

停止和回退：先 `systemctl disable --now pc28touzhu-rule-lab.service`，再将本次代码回退至发布前已记录版本并重启平台 API。保留独立模拟目录；不恢复或覆盖生产账本。新服务使用独立库，无须停止已有投注执行服务。SQLite 备份应使用其 backup API 获得一致副本，并存到模拟目录之外；不直接复制正在写入的主文件遗漏 WAL。

仅用于开发/运维临时目录的单步检查：设置独立临时 `RULE_LAB_DATA_DIR` 后运行 `deploy/systemd/pc28-python.sh -B -u -m pc28touzhu.rule_lab_worker --once`。不要在运行中的同一个模拟目录再启动第二个实例；文件锁会拒绝它。
