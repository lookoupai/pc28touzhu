# 风控比较组发布准备

**尚未部署，已通过发布前验证。** 基线0cc7c75，开发分支feat/rule-lab-risk-comparison；开发验证与root独立完整回归969项（237.434秒、0失败）、真实浏览器、v2→v3一致副本迁移和资源检查均已完成。

迁移摘要`evidence/migration.json`确认9张原表旧列不变、完整性ok。`evidence/benchmark.json`确认468保存/峰值78运行、五方案各120期完成可比、源哈希不变，峰值约39.18MiB；未施加cgroup节流，不能推算生产完成时间。覆盖率摘要`evidence/coverage-summary.json`，范围和未覆盖限制见qa-report。

本次仅lab schema v3新增组及关系表，保留全部原v2数据；没有上游或业务主库迁移，无新依赖、无unit权限和资源额度修改。

发布步骤由root执行：先停止旧lab worker、对独立lab库做一致备份，切换新版代码并迁移v3；核对原表旧列、完整性、主平台/worker及匿名权限。更新后的旧源码不能读取v3，应保留备份并避免覆盖任何新成绩。

用户授权创建五风控比较，root可以从仍暂停的原批次1新建，原batch不变：

`POST /api/platform/rule-lab/studies`

```json
{"source_batch_id":1,"name":"组合连挂观察 · 五组风控对比","request_id":"risk-comparison-batch1-20261126"}
```

预计原78+新390=468保存版本，study和五child均paused，0处理0结算；**创建不等于启动**。root完成后记录实际study/child数量和状态，用户需明确点击“启动比较队列”才开始。

API `/studies/{id}`提供最多500行比较；`/studies/{id}/state`只收running/paused，running表示入队；`/studies/{id}/export.csv`包含风险与证据。原batch1不受队列控制，child不得单独启动或改风险。

版本、GitHub备份、部署时间、迁移/健康、实际生产创建结果和资源采样必须等root执行后补真实证据。本代理没有提交、推送、部署或修改生产数据。
