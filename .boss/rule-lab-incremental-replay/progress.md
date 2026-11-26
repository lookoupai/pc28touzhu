# 开发进度：未上线

2026-11-26，分支`feat/rule-lab-incremental-replay`，生产仍`f3f15ae`。用户要求现在设计开发、当前实验跑完再切换，随后追加组合长连错条件。

## 已完成

- `design.md`：增量续跑、分段状态、数据归档保护、月度/累计口径、90天单次追加与长期累计、资源及迁移门禁。明确首次边界结果迟到不能用更晚状态提前决策，需快照回退或阻止续接。
- W1只读预检：domain/rule_lab_continuation.py + store.inspect_continuation_checkpoint。检查全组完成/无pending、共同游标及证据、旧条件/赔率一致、100期暖机；返回配置/状态摘要。保留active轮，不修改资金或调度。`continuation_available=false`明确尚未提供实际续跑。
- 组合专用模板`combo_long_miss_v1`：3、4、5、6、7、8、10、12、15、20、25、30加无条件基准，共26版本；原basic_v1不变。后端校验combo专用，options元数据、UI模板选择/动态数量/正确详情名称；创建paused，五风控复制130版本。旧条件key及完整快照保持。

## 实际验证

- `PYTHONPATH=src python3 -m unittest discover -s tests -p 'test_rule_lab*.py' -q`：**151通过、0失败，24.359秒**，`/tmp/pc28-incremental-long-streak-lab.log`。
- 新worker fixture36期连续预测错误，≥7/8/10/12/15/20/25/30按正确期号首次决策；两OK配对金额正确，每tick重建worker仍无重复结算。
- 原浏览器全流程及选择模板/非combo回退：`/tmp/pc28-long-streak-regression-ui.log` exit0。
- 新专用真实WSGI浏览器：刷新保留模板、创建paused26、分页找到≥30、两OK及详情名、390px无整页横向溢出，`/tmp/pc28-long-streak-ui.log` exit0；截图`/tmp/pc28-long-streak-mobile.png`已目视核验。
- JS语法及diff检查通过。未为这次未上线增量重复全项目983项；发布前必须执行完整门禁。
- 生产只读预检确认当前组未全部完成，不可冻结续接；没有初始化/写生产库。此前诊断JSON中的“不可比”提示已修正为仅completed组才判断，避免把正在运行导致的暂不可比误报为历史损坏。

## 尚未完成

W2分段事务/快照迁移、W3有界输入采集与未消费保护、W4真正续接worker及连续等价性测试、W5续跑API/UI和历史段报告、W6生产副本/资源/完整回归。未实现自动采集、真正续跑、补9月合并或旧批次归档释放容量。

## 生产状态与发布边界

本次未合并main、未部署、未重启、未创建/启动实验，worker保持PID719342/NRestarts0，现有五组继续原条件。全部完成后也不能仅凭预检就上线，须完成后续门禁；现有保存468/500未增加，未提高额度。新模板已实现不等于线上当前实验已经包含更长门槛。
