# 按轮风控发布记录

## 当前状态

**已部署并核验，批次1仍暂停。** 根代理于2026-11-26 UTC完成生产切换、独立模拟库v2迁移与批次风控配置。代码版本`e583f67`，功能分支及main均已推送GitHub；生产main与远端0/0且干净，上游main也保持0/0且干净。

- 开发基线：`083d607`。
- 分支：`feat/rule-lab-risk-controls`。
- 开发目录：`/www/wwwroot/pc28touzhu-rule-lab`。
- 生产入口：`https://tg.dca.de5.net/rule-lab`，新页面和脚本已返回200。
- 生产独立模拟库：`/www/wwwroot/pc28touzhu/data/rule_lab/rule_lab.db`。
- 业务主库、上游项目和worker资源unit均不需要修改。

## 已完成发布前证据

- 最终针对性测试96项通过；根代理独立浏览器退出0。
- 根代理完整回归952项通过、0失败，220.245秒，exit 0；`/tmp/pc28-risk-full.log`。新增纯风险模块语句覆盖91.2%，本次5模块语句覆盖88.12%、分支77.46%；摘要`evidence/coverage-summary.json`，限制见qa-report。
- 生产模拟库一致副本v1→v2重复初始化，7旧表全部旧列不变，完整性ok、批次1保留；`evidence/migration.json`。
- 100版本×400期合成资源测量峰值约36.57MiB、源哈希不变；`evidence/benchmark.json`。
- 真实只读预设核对：`6:6:7:combo`正确映射规则6/路由6/订阅7的数值，只复制风控。

## 实际发布与核验

先停止旧lab worker，再通过一致备份保存`/var/backups/pc28touzhu/releases/rule-lab-risk-controls-20261126-080513/rule_lab-v1.db`：405,504字节、权限0600、quick_check=ok。生产代码快进至`e583f67`后重启平台并启动新worker，自动迁移schema v2；随后root以www用户再次initialize确认幂等和完整性ok。

生产迁移核验6张原业务表全部旧列逐行与备份一致，`changed_old_tables=[]`；只排除正常变化的心跳表lab_runtime。记录`/tmp/pc28-risk-production-migration.json`。没有业务主库迁移或上游数据修改。只对9个变更源码文件精确设置0644读取权限，未递归修改项目、环境或会话权限。

根代理通过真实管理员API调用`POST /api/platform/rule-lab/batches/1/risk-policy`获得200，复制明确预设`6:6:7:combo`：

| 项目 | 保存值 |
|---|---|
| 单轮 | 止盈10、止损10、最多下注0不限 |
| 日 | 止盈20、止损20、最多下注/开轮0不限 |
| 命中率熔断 | 实赔/(实赔+全亏)低于25%，最少20单，退款不入分母 |
| 冷却 | 距上次开轮至少10期 |
| 原批次配置 | 来源7“频次趋势V1”、每注1、78实验、原回测日期及模板均保持 |
| 运行状态 | paused、started_at=null、processed_issues=0、全部实验settled_count=0 |

保存结果证据为`/tmp/pc28-risk-deployed-batch.json`，报告只保存必要摘要，不复制完整用户/路由记录。此次配置没有启动模拟。

发布后`/api/health`、`/rule-lab`、`/assets/rule_lab.js`均200，匿名实验API401；9个服务均active。除主动更新的平台与lab外，其他7个worker PID全部未变，本次执行器没有重启。平台与lab的NRestarts均为0；其他服务的既有累计重启值不视为本次发布重启。证据`/tmp/pc28-risk-release-health.json`。

公网HTTPS健康检查另行返回200。上述功能提交以`e583f67`为准，最终文档备份提交由根代理另外记录。

资源限制保持CPUQuota20%（0.2核）、MemoryMax268435456、MemorySwapMax0；核验时lab处于空闲，MemoryCurrent11,599,872字节（约11.1MiB）。这是空闲采样，不是满载或长期性能SLA。

## 恢复边界

v2库不能交旧v1 worker直接读取。回退前先停止模拟worker并保留当前lab库；若已有新成绩，不能用旧备份覆盖。可选择前向修复或保持模拟停止，真实业务账本不受本次独立库迁移影响。部署、Git和生产配置由根代理执行，开发代理只更新本文事实记录。
