# 按轮风控实施任务

## 摘要与授权

唯一实现owner为core，root负责只读审查/独立测试/发布；不得再派生代理。用户已明确完善、沿用现有风控；风险在独立模拟库可逆增量范围，无主业务迁移/外发/真实下注。新增批次模式、模拟库schema v2和API/ui文件已在授权范围。先由root确认本增量方案，未确认不动正式代码。

基线：feat/rule-lab-risk-controls @083d607，工作树干净；旧实现934测试曾过。仅复用Python/SQLite/原生UI现有工具，无依赖安装。

## 写集和波次

| Wave | 任务 | 文件 |
|---|---|---|
| W1 | 纯风控状态、配置校验、v2账本迁移/事务/旧数据保护 | 新domain/rule_lab_risk.py；domain/rule_lab.py；services/rule_lab_store.py；新tests/test_rule_lab_risk.py及既有store/domain测试 |
| W2 | worker轮内继续/日门控/截止未完结、明确预设继承、未启动编辑API、CSV | rule_lab_worker.py；services/rule_lab_service.py；api/app.py；既有worker/api测试 |
| W3 | 新建/编辑风控、风险摘要和轮详情，使用文档与浏览器 | ui/rule_lab.html/css/js；tests/test_rule_lab_ui.cjs；tests/rule_lab_ui_server.py；docs/RULE_LAB.md |
| QA | 覆盖率、targeted集成与浏览器，root独立全回归 | .boss/rule-lab-risk-controls/qa-report.md；临时测试日志 |

前缀均为src/pc28touzhu（tests/docs除外）。其他业务/上游/unit禁止修改。风险语义见architecture，公共字段需要变更先报告root。

## 必测合同

1. TP/SL/最多注正确停止、末注越线保留、退款不入hit-rate分母、低于而非等于。
2. 轮内不要求重复命中、结束后重新条件和距last_start冷却；两个OK版本风险独立。
3. 日停止sticky、跨日active轮保持、日停后次日等条件、晚到旧日pending不重置新日、日maxround不截断当前轮。
4. 原v1真实schema迁移旧列逐表哈希相等、初始化幂等；legacy逐期行为不变；重复和重启不重账。
5. 编辑只允许未启动paused，owner/权限和并发启动revision双保险；已运行后immutable。
6. preset按route/sub/metric正确继承；route disabled不撤销rule；两日层不假合并；无匹配不自动选；不改实验来源/注额。
7. UI保存与编辑、字段开关/0说明、风险摘要、最近轮、CSV、移动端、错误恢复；无外发和生产写入。

任何结算/时间/迁移/权限测试失败先修复，不以界面完成替代正确性。日志只写临时路径或增量证据目录，不提交库/秘密。生产发布由root执行。
