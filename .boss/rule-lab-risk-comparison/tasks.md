# 风控比较组实施任务

## 授权与写集

用户“那就继续”授权五风控候选、条件×风控对比与受限队列。root+core最多2并发；core唯一实现owner，禁止派生代理。基线0cc7c75，独立worktree/branch feat/rule-lab-risk-comparison。root审合同后实施，不提交/部署/生产写。

允许src/pc28touzhu中的新domain/rule_lab_comparison.py、现有lab domain/store/service/worker/apiapp/ui三文件；新tests/test_rule_lab_comparison.py、自包含tests/fixtures/rule_lab_v2.sql及必要已有lab测试/fixture；docs/RULE_LAB.md；本.boss增量文档。源reader必要只读字段逻辑可小改前需通知root；不改上游/unit/依赖/其他业务。

W1纯候选与组原子创建/schema v3：五候选精确单因素/不可用去重、复制旧snapshot、390原子500cap/idempotency、v2旧列hash。
W2队列/worker/API：100全局/FIFO/parent pause-child不可单控/revision/restart/error；共同cutoff；warm+逐期fingerprint页大小/重启；改源不可比；完整5组结束及delta。
W3界面/文档：preview容量→创建paused→queue start/pause→进度/比对/CSV→child详情；权限/恶意名称/窄屏；390saved78active资源fixture。

所有波次先行为红测再green，root独立检查；已通过测试无新改动不反复跑。报告仅实际证据，未发布明确未发布。
