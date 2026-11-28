# 研究工作区后端交付证据

## 已实现

- schema8只加lab_favorites；候选冻结配置/摘要与当前结果分离，源删除后快照可读，按快照新建一条条件/一套原赔率的独立草稿。CRUD、容量、归属和命令重试均复用现有事务规范。
- research/options与显式共同窗口排名；按实际生效结算语义、完整政策、注额分组，显示名/出处不拆组。完整无下注月不当缺月；整个窗口无样本不排名。跨来源不要求预测摘要相同。
- bulk-preview/bulk对改名、删除、手动append进行一次事务全体预检和提交。原单项公开方法复用同连接helper；删除依赖闭包、组先清理、已有暂停/段冲突、收据重试完整保留。
- 有界真实源质量页，索引seek+≤200行读取，0.25秒原源deadline保留。开奖前40秒、归档证明、null方向、无效和值、跨页gap、已知日期异常归日、未知边界分别处理。游标HMAC签名，绑定范围/固定截止/首次观察/来源身份；服务重启明确重查。
- 旧results维护工具及manifest兼容schema8，旧API与原配置hash不变。

## 实际命令与结果

测试命令均`env PYTHONPATH=src python3 -m unittest <modules> -q`，在隔离工作树运行。

| 阶段 | 范围 | 实际结果 | 日志 |
| --- | --- | --- | --- |
| W1 RED | research/research_api/research_migration | 6个新增断言因404/schema7失败（另6旧导入测试通过，后已修import alias） | /tmp/pc28-research-W1-red.log |
| W1 GREEN | 同3模块 | 6/6 | /tmp/pc28-research-W1-first.log |
| W2 RED | bulk_management | 3项因新增bulk API404失败 | /tmp/pc28-research-W2-red.log |
| W2 GREEN | 新research/favorites/bulk、rootQA、旧management/series/queue | 56/56 | /tmp/pc28-research-W2-green.log |
| W3 RED | quality | 4项因新API404失败 | /tmp/pc28-research-W3-red.log |
| W3 GREEN | quality/source/rootQA | 32/32 | /tmp/pc28-research-W3-green.log |
| 宽回归 | 新4模块/迁移、rootQA、旧观察/管理/续算/queue/source/selfservice/results | 136/136 | /tmp/pc28-research-backend-wide.log |
| 质量边界修复 | quality + rootQA | 22/22 | /tmp/pc28-research-quality-boundary-green.log |
| 依赖rename修复 | bulk_management + rootQA | 22/22 | /tmp/pc28-research-rename-closure-green.log |

root的独立QA分别补并复现：赔率元数据拆组、完整零下注月份被误拒、首期缺口误标已验证、有效日期无效开奖遗漏日账、study改名自我改变source依赖token。全部按反例修复，未改root测试来迎合实现。

root另外完成完整发现集1371项（1369通过、2既有外置环境skip），最终依赖rename补丁由专项22项验证；不把重复专项相加成新的独立全量测试数。root核心新增模块行覆盖率93.10%，生产副本schema7→8重复初始化原旧列/水位完全一致；这些由root独立收集，详见qa-report与外置收据。

STATUS: DONE
REASON: 后端及所有已知独立反例已通过；源码冻结供最终QA/发布。
