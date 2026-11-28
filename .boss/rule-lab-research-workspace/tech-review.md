# 研究工作区技术评审

## 摘要

- 有条件通过：四项确定的后续能力可复用现有实验室实现，必须先证明原子批量与真实共同窗口后才能发布。
- 用户已授权“上传GitHub，然后把后续全部做完”；已有一期主分支79376e7已由root上传。此轮不再重复确认正常开发/测试/发布。
- 最高风险是跨来源伪排名、循环提交导致半组、把数据清理误判为无预测，以及收藏源删除后误用旧成绩。plan.md给出边界和证据要求。
- schema8只新增收藏，旧实验仍读自身冻结快照；无依赖、认证体系、真实规则或资源限额变更。

## 1. 事实与复用依据

| 现有代码 | 已核验行为 | 本轮使用方式 |
| --- | --- | --- |
| services/rule_lab_observation.py | 单一只读事务，批量日账/待结算读取；当前窗口由最大owner_end生成 | 追加服务端统一window可选参数，旧调用默认保持 |
| domain/rule_lab_observation.py | 月份范围、实际覆盖、待结算、历史一致性、冲突/旧引擎阻断、日结回撤 | 复用完整性语义，统一窗口计算；不以累计统计代替窗口值 |
| services/rule_lab_management.py | rename/delete各自开write连接，delete同事务停任务+创建异步job | 抽同连接内部helper，bulk在唯一事务复用 |
| services/rule_lab_series.py | request_append可一次性adopt，无series时仅capture=1；已有compute/daily不覆盖 | bulk严格调用同事务append逻辑，不新增定时载荷 |
| services/rule_lab_source.py | 索引期号seek、≤200期分页、0.25秒源deadline；capture_page区分no_prediction与archive_gap | 同事务读取与分类复用，不能直接将过滤后的items数当全部源行数 |
| services/rule_lab_store.py | schema7；2000保存/80每用户批次/100运行；BEGIN事务由_connect负责 | 加favorites schema8及薄入口，旧列不变 |
| services/rule_lab_retention.py | 只保留结果与运行依赖，旧输入可能清理 | 质量检查明确当前源保留范围，收藏只冻配置和摘要 |
| ui/rule_lab.js/self_service.js | 原生JS shared drawer/request/auth/dirty-state bridge | 新单文件research模块、四tab抽屉，复用权限清理 |

## 2. 必须处理的风险

| 严重度 | 风险 | 具体处理与验收 |
| --- | --- | --- |
| Blocker | 不同截止日期/赔率版本/政策混排 | 一个显式window；精确冻结定义分组；无完整证据rank=null；跨来源不要求预测hash相同 |
| Blocker | 逐对象调用公开write方法，部分已提交 | 内部同连接helpers；最后一项故障触发整组回滚，包含job/receipts/queue/series |
| Blocker | 批量删除源与依赖比较组产生FK或孤儿 | 全选择集合验证依赖闭包，study jobs先于source jobs；保护外部未选依赖 |
| Blocker | 批量append隐式开启每天任务或恢复用户暂停 | 不接受schedule字段；原compute/daily保持，新adopt只有手动append；已有active/paused段拒绝 |
| Blocker | 源缺行被显示0%缺失 | 归档元数据/未知/无效/迟到分别计数，缺证据时rate=null，固定200页预算 |
| Major | 跨页源数据变化被当冻结报告 | 固定首次observed_at/截止期，记录read_at，说明分批现状；身份变化409，中断可续读 |
| Major | 收藏源已删仍显示新鲜成绩 | immutable saved_result与可选current_result分开，source状态明确，旧请求重放不复活 |
| Major | 页面重复提交、权限失效留草稿 | 原请求ID与载荷复用，403清理缓存/扫描/抽屉；真实浏览器验证 |
| Major | schema版本7硬编码旁路工具拒绝8 | results迁移工具及相关测试兼容8；已handoff生产不重复导入 |

## 3. 发布边界

迁移仅加结构，部署时不对用户实验执行bulk命令。先使用root外置schema7副本验证首次/重复初始化旧列和水位不变，再验证正式API、页面和worker继续推进。真实下注规则、订阅、当前队列、续算开关均须做前后指纹核对。生产发生自然新增成绩允许，进度倒退不允许。

根与单个实现代理总并发≤2；实现代理按Backend再Frontend角色串行写入，root只做审查/独立验证/发布准备，避免同文件并发编辑。

STATUS: DONE
REASON: 四项确定后续功能可实施；必须按plan/waves的红绿门禁交付，未认可伪排名、半组或静默缺失的实现。
