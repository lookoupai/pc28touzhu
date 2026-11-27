# 运行速度切换及预算边界修复验证

2026-11-27。

- 完整回归：1005项通过、0失败，195.509秒，`/tmp/pc28-performance-full.log`。
- 控制器/API/运维针对性34项通过，`/tmp/pc28-performance-tests.log`。权限admin/global、固定参数、无通用shell、sudo超时/失败不报成功、systemd/cgroup不一致阻止切换、相同模式幂等、helper非法参数拒绝、GET不sudo、切换不修改实验账本均覆盖。
- 真实WSGI浏览器全流程exit0，`/tmp/pc28-performance-ui-final.log`；economy→fast→economy，实际字段更新，丢POST响应后GET核对且只发1次POST；现有比较/月报/权限/手机流程通过。
- 组合7～30模板独立浏览器exit0，`/tmp/pc28-performance-long-streak-ui.log`。
- helper `sh -n`、sudoers `/usr/sbin/visudo -cf`、JS语法、diff检查通过。

## 速度与计算一致性

临时数据与两个独立systemd-run单元；主生产服务/源库/账本未参与。相同180期、78组合版本，0.2核55.519秒/CPU10.499秒/峰值RSS60444KiB；1核10.877秒/CPU9.021秒/峰值RSS64248KiB。时间比5.10，逐实验condition/rule/stats/risk摘要一致。两组实际cpu.max为20000/100000和100000/100000，内存限256MiB、swap0、Nice10、IO idle。完整摘要`evidence/benchmark.json`。systemd-run报告的MemoryPeak不含所有RSS共享页，采用进程ru_maxrss记录，不将其误当cgroup峰值。

这只验证小样本和实际额度生效，不是完整两个月的SLA。以样本推算原2～3小时可约25～40分钟，仍需完整数据实测。

## 本次发现与修复

首轮双档结果摘要不一致，未忽略。定位旧按轮回放在settle_pending因预算耗尽返回后仍推进下一期，因pending而错过本该允许的入场。确定性回归旧版8笔vs4笔失败`/tmp/pc28-speed-budget-red.log`；修复在同一期返回下tick继续后通过`/tmp/pc28-speed-budget-green.log`。节流实测再次运行，两档结果一致。

新建历史批次标记replay_scheduler_version=2。旧按轮已处理批次返回review_required提示；历史成绩保持可读且未改写，比较组不输出delta。测试确认只读标记不改变实验成绩。不能仅因为输入指纹相同就把调度不确定性的旧风控结果当可靠比较，后续需用户决定复核范围，未自动重跑。

## 发布边界

无schema迁移，保留安全unit全部隔离及内存限制，worker部署需一次重启加载修复，但用户日常切档不重启。新装root拥有的固定helper和严格sudoers；不从www可写项目路径提权执行。仅页面/API授权管理员可选两档，不改实验配置或发送投注。

本次将已验证组合专用模板一同发布；增量续跑仍只有只读预检，未提供真正追加区间、自动归档或清理保护，不宣称已实现。
