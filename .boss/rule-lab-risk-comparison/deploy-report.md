# 风控比较组发布准备

**已部署并核验，比较组已创建且仍暂停。** 功能版本`469b628`已推送GitHub feature与main，生产main干净且与远端0/0。基线0cc7c75，开发分支feat/rule-lab-risk-comparison；开发验证与root独立完整回归969项（237.434秒、0失败）、真实浏览器、v2→v3一致副本迁移和资源检查均已完成。

迁移摘要`evidence/migration.json`确认9张原表旧列不变、完整性ok。`evidence/benchmark.json`确认468保存/峰值78运行、五方案各120期完成可比、源哈希不变，峰值约39.18MiB；未施加cgroup节流，不能推算生产完成时间。覆盖率摘要`evidence/coverage-summary.json`，范围和未覆盖限制见qa-report。

本次仅lab schema v3新增组及关系表，保留全部原v2数据；没有上游或业务主库迁移，无新依赖、无unit权限和资源额度修改。

## 实际发布

root于2026-11-26 12:12 UTC先停止旧lab worker，将独立模拟库一致备份至`/var/backups/pc28touzhu/releases/rule-lab-comparison-20261126-121244/rule_lab-v2.db`：425,984字节、权限0600、完整性ok。生产代码快进至`469b628`，仅精确设置变更源码0644读取权限；以www用户initialize两次完成v3迁移并验证幂等/quick_check=ok，随后重启平台、启动lab。

生产8张原业务表旧列逐行与备份一致，排除正常心跳表；证据`/tmp/pc28-comparison-production-migration.json`。没有源仓或业务主库迁移，没有提高资源unit权限或限额。旧源码不能直接读取v3，应保留备份并避免覆盖任何新成绩。

## 实际比较组创建

真实管理员API预览返回5个候选、390个新版本。root从仍暂停的原批次1提交：

`POST /api/platform/rule-lab/studies`

```json
{"source_batch_id":1,"name":"组合连挂观察 · 五组风控对比","request_id":"risk-comparison-batch1-20261126"}
```

第一次请求返回201，创建study **1**，名称“组合连挂观察 · 五组风控对比”；相同request_id重试返回200、created=false且同一id，未重复创建。五个child为**2～6**，各78版本；全部paused、started_at=null、processed_issues=0、无已结算记录。原batch1的API结果完整保持不变，原78+新390=468保存版本。证据`/tmp/pc28-comparison-deployed-study.json`，本文只记录必要摘要。

**创建不等于启动。** 用户下一步应点击比较组1的“启动比较队列”；原单批次1保持paused，避免原78与子方案78同时竞争全局100额度而使队列等待。root未启动比较组或子批次。

API `/studies/{id}`提供最多500行比较；`/studies/{id}/state`只收running/paused，running表示入队；`/studies/{id}/export.csv`包含风险与证据。原batch1不受队列控制，child不得单独启动或改风险。

## 服务与权限核验

本机健康、页面、新脚本均200，匿名studies API401，公网HTTPS健康200；9个服务均active。平台和lab为主动重启；执行器在平台短暂停止时遇到API Connection refused，退出status1后由systemd自动恢复，核验时online、last_seen为`2026-11-26T12:18:52Z`。其余6个worker PID未变，不能描述为其余7个全部未变。证据`/tmp/pc28-comparison-release-health.json`和`/tmp/pc28-comparison-executor-restart.json`。

CPUQuota20%（0.2核）、MemoryMax256MiB、MemorySwapMax0和模拟目录1GiB应用预算保持；空闲内存采样约9.375MiB，不是长期或满载性能承诺。验证没有创建真实执行任务或发送投注消息。

本次Git、部署及生产比较组配置由root执行，开发代理只更新本报告。功能提交为`469b628`，后续发布文档备份提交由root另行记录。
