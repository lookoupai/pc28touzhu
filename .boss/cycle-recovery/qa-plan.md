# 异常轮次恢复独立 QA 计划

## 摘要

- **用户承诺**：异常发送超过原截止后，旧轮退出实时调度；未知账务按原轮保存，后续计划开轮不以人工核对为前提。
- **核心验收**：从真实 SQLite/WSGI 流程证明“旧未知 → 摘离 → 下一有效窗口新开 → 旧结果迟到 → 下一轮边界只消费一次返还”。仅将 running 改成 blocked 不算修复。
- **资金边界**：原快照余额 B、已结算净额 K、未决本金 R；摘离余额 B+K−R，最终资本 credit=R+N−K。未决资金不算损失，不重复扣留，不直接改当前运行周期。
- **验证状态**：本文件为实施前独立审查和待执行矩阵，未声称新功能测试通过。父代理提供的既有 144 项通过作为基线；独立 QA 会在实现后重新执行必要集合。
- **施工边界**：QA 仅拥有本目录 QA 文档与 `tests/test_cycle_recovery_qa.py`；收到根代理实施完成通知后再新增独立测试。所有测试用临时 SQLite，外部传输/开奖作为边界模拟；生产只读，不发送真实消息。

## 1. 实际代码审查发现与合同澄清

以下行号基于 `43fd8a2`，路径前缀为 `src/pc28touzhu/`。

| 优先级 | 具体证据 | 风险及必须验证的行为 |
| --- | --- | --- |
| P0 | `executor/db_repository.py:4949` 规则删除、约 `:6940` 方案删除只检查 pending/placed/active，再批量删除 run/runtime/cycle | 未解决 archive 会被原 pending 事件间接挡住，但 resolved 且 credit 尚未消费时原条件可能放行。必须显式保留这类资本债权和证据，不能因新增 FK 导致不明 500，不能静默丢返还。 |
| P0 | `executor/db_repository.py:6687` 清理按日期直接删除 `auto_trigger_rule_runs` / 日统计 | archive 关联的旧 run、原事件归属和待消费 credit 不能被清理破坏。验证正常无关联历史仍可清理；归档保留条件不得只覆盖 unknown 而漏 pending credit。 |
| P0 | `executor/db_repository.py:8764` 旧 event 原 stat_date 写日统计，`:8776` 调用日风控，`:6055` 按该日关闭 runtime/run | “旧账不污染新轮”针对当前周期余额、关数、stake、pending；同日真实迟到盈亏仍应触发该日原有风控。跨日旧结果不得停今天。不能为了隔离周期而绕过真实日止损。 |
| P0 | `executor/db_repository.py:10827` 原 report 用原 attempt 身份去重；`_sync_progression_event_delivery_status` unknown 分支保留 pending | 新核对通道需要独立、幂等、组级原子处理；不能简单放开旧 report 重复覆盖。已确认成功不得被迟到失败/未知降级；未知不得靠“查不到消息”变 void。 |
| P0 | `services/auto_trigger_cycle_service.py:103` 开轮复用前一 balance，现有 `max(carry,base)` 补基础额；周期表单当前记录 | reserve 扣一次后不能每次新开再扣；消费 credit 和首单派单必须同事务，首单失败回滚不能吃掉 credit。基础补足应明确记录，不标成旧本金返还。 |
| P1 | `services/pc28_auto_settlement_service.py:60` 周期终态只认 settled/void；人工停止会取消 pending 为 cancelled | 仅真实未发的 cancelled 才能视为零损益终态。未知发送不能因人工停轮变成未投；人工停止仍须阻止后续自动开轮。 |
| P1 | `executor/db_repository.py:6870` 订阅自动活动仅返回 active 或今天 run | 昨日中断旧轮若完全从活动列表消失，用户需仍能看到未决本金和历史明细；不能误显示当前运行，不能丢历史查询入口。 |
| P1 | `executor/db_repository.py:223` 结算优先 event settlement snapshot；执行器身份包含目标/账号/协议快照 | 改名、改赔率、换目标、换账号后，迟到核对和结算仍用原身份和原事件赔率。当前目标配置不能替历史任务提供成功证据。 |

## 2. 独立功能与资金矩阵

| ID | 场景 | 必须观察的真实结果 | 实施后证据 |
| --- | --- | --- | --- |
| QA-01 | unknown 未到截止 / 恰好截止 / 已到截止 | 未到不摘离；到期只摘一次；旧任务仍未知、archive完整、session无旧活动指针 | SQLite detach 集成 |
| QA-02 | 只有 placed 等开奖，跨 00:00 | 无异常归档，原 run/日归属继续，原跨日测试通过 | 正常跨日回归 |
| QA-03 | 昨日旧轮 unknown，今日下个有效窗口 | 新 run、新事件和新任务真实存在；旧 run 禁止领取；原日启动数不回退，今日只加一次 | 完整调度 E2E |
| QA-04 | 本次数字 B=22.39、K=0、R=13 | 摘离余额9.39；reserve13；新轮base10仅记录新增0.61；多次scan/new start不重复扣13 | 数值守恒 |
| QA-05 | 一腿已经settled净+2，另一腿本金4未知，B=20 | 摘离余额18；最终未知腿净−4时credit0，净0时credit4，净+3时credit7；已结算+2只算一次 | 混合腿守恒 |
| QA-06 | 第一次归档后又产生第二个归档，两个结果乱序到达 | 每个archive唯一；每份reserve扣一次；credit按归属累加且各只消费一次 | 多归档乱序 |
| QA-07 | credit确定时新轮正运行 | 新轮state/stake/win_streak/pending及原快照逐字段不变；credit仅pending；再新开时一次消费 | 迟到结算隔离 |
| QA-08 | 消费credit后的首单派单失败 / 事务异常 | 新run/额度/credit消费标记全部回滚；下一有效开轮还能使用该credit | 失败原子性 |
| QA-09 | 归档路由A未知、路由B placed、路由C已终态 | 同会话全部参与路由归档；B晚开奖仍归原runtime；冻结后没有旧run新发送 | 多路由与迟到RPC |
| QA-10 | 人工stop/禁用/日止损在摘离前后发生 | 只释放异常运行占用，不解除独立停止。已知未发取消终态可收尾，未知仍保留 | 停止优先级 |
| QA-11 | 同日旧结算触发日止损 vs 次日才查清昨天结果 | 同日按既有风控停止；次日只更新旧日统计不关闭今天；旧运行周期字段隔离 | 日风险正确归属 |
| QA-12 | all_in以及修改roll_mode | 独立base不接收错误chain credit；原归档策略快照决定资本去向；不能静默丢credit | 模式/配置边界 |
| QA-13 | 两个worker同时摘离、resolve与开新轮并发 | 只有一个archive/扣留/消费；SQLite完整事务，无孤儿/半腿状态 | 双连接并发 |
| QA-14 | stop/recovery/archive后重放旧成功或失败回报 | 成功只回原事件；失败不降级成功；无重复profit/daily统计；原attempt证据保留 | 回报/核对幂等 |
| QA-15 | archive未知、awaiting_settlement、resolved-credit-pending分别删除/清理 | 该保留的事实与资本保留；错误有可读说明；正常无关历史仍可删 | 清理/删除边界 |
| QA-16 | 非法owner、其他用户rule/sub、伪造executor核对租约 | 不能读取或改变其他归属；原数据哈希不变；无500泄漏 | WSGI权限测试 |
| QA-17 | 同文案多消息、错误bot/reply_to/issue/金额、读取网络失败 | 只保留未知/安排重查；发送计数为0；后续普通发送仍能执行 | 传输边界测试 |
| QA-18 | 平台GET归档页/规则列表反复刷新 | 零数据修改，稳定分页无漏重；不返回认证凭证 | 真实WSGI只读检查 |
| QA-19 | 当前waiting/新轮running + 昨日archive未知 | UI同时正确显示当前状态和历史保留额；无旧任务重发按钮；手机/桌面可读、名称转义 | 真WSGI浏览器 |
| QA-20 | 增量初始化重复、旧版本资料、未知原run归属损坏 | 旧业务列不改；初始化幂等；归属无法证明不伪造金额/释放资本，清晰记录问题 | 副本/迁移检查 |

## 3. 测试安排与防止伪通过

1. 开发者先跑 RED→GREEN。QA 审查断言是否验证真数据库新run/新任务存在，不能仅检查`status=waiting`或mock调用次数。
2. 独立QA新增用例集中 `tests/test_cycle_recovery_qa.py`，优先补开发者没有覆盖的混合腿、资金守恒、消费回滚、同日风控、清理、并发和多归档；不重复简单getter测试。
3. 原9组回归、全unittest、相关DOM与浏览器按根代理协调执行，避免多个代理重复大套件争用资源。用完整命令、退出码、通过/失败/跳过、失败明细记录到 `qa-report.md`。
4. 浏览器需要真实临时WSGI和SQLite fixture，外部网络拦截，仅开奖/Telegram作为外部边界模拟。不能以纯DOM mock替代关键链路验收。
5. 覆盖率针对新增/变更行为采集；超大仓储的全文件覆盖率和本次变更行覆盖率分别说明，不能把已有未测行掩盖新逻辑缺口。
6. 静态门禁包括Python编译、JS语法、`git diff --check`、依赖/系统配置无意外改动。不存在的CI test gate明确标注，不以Docker成功代替测试。

## 4. 发布前阻断条件

- 仍需先人工核对才能新开，或改成另一个长期blocked状态。
- 未确认任务被直接void/退款/记损失，或者截止后重发旧期。
- reserve或credit重复，旧结算改新轮stake/关数，原日额度被重置。
- 归档遗漏参与路由，人工停止/真实日风控被自动恢复绕过。
- 删除/清理破坏未决账或未消费credit；查询接口跨用户泄露或产生账务写入。
- 关键路径只由mock支撑、缺真实SQLite/WSGI用户路径、测试失败未修，或没有一致生产备份/副本演练。

## 状态

**STATUS**: DONE_WITH_CONCERNS
**REASON**: 独立计划已完成；实现与对应测试尚待开发者交付，暂不能作发布放行结论。
