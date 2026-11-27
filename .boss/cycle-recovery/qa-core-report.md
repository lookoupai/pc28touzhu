# 核心生命周期独立 QA 结果

## 摘要

- 核心独立用例最终 **11 通过 / 0 失败 / 0 跳过**，5.088 秒，退出码 0。
- 新增测试：`tests/test_cycle_recovery_qa.py`。真实临时 SQLite、真实派单/结算/调度代码；仅外部来源绩效、时钟和发送结果通过既有 fixture 隔离，无生产写入或真实消息。
- 已发现并回派修复两项恢复扫描问题：取消但仍未知的档案导致后续 credit 饥饿；非法历史 leg ID 导致 TypeError 中断整批扫描。修复后同一断言通过。
- 本报告只放行核心新增用例，不是生产发布放行。W3 API/执行器核对/UI 与最终集成门禁尚待完成；此轮未重跑全仓套件，也未采集覆盖率。

## 最终执行证据

工作目录：`/www/wwwroot/pc28touzhu-cycle-recovery`

| 命令 | 退出码 | 通过/失败/跳过 | 失败详情 |
| --- | --- | --- | --- |
| `PYTHONPATH=src python3 -m unittest tests.test_cycle_recovery_qa -v` | 0 | 11 / 0 / 0 | 无；5.088 秒 |
| `git diff --check` | 0 | 静态检查，不适用测试计数 | 无 |

## 独立通过的行为

1. 实际第一关结算生成余额22.39、下注链13.93；第二关13本金保留后余额9.39，下一轮显式补0.61，原3.93利润与reserve13保留。
2. 截止前1微秒不摘离，原截止时即摘离，不额外等待一个开奖周期。
3. 两个旧archive逆序返还，当前运行周期完全不变；下一开轮一次消费20，两个归档消费ID准确。
4. chain→all_in→chain切换保留原chain credit，all_in独立返还不错误注入新chain余额。
5. 同日旧单真实亏损仍触发日止损；昨天旧单迟到亏损只更新昨天日统计，不停今天新轮，不覆盖今天cycle。
6. 未尝试任务人工停止后，可信cancelled结清pending；会话保持manual_stop，次日也不自动恢复。
7. 格式损坏archive的检查轮转，不阻止后续已可释放credit。
8. cancelled但仍有unknown发送的archive保留占用并轮转，不饿死其他已终态archive。
9. 非法历史leg ID被隔离为本轮数据错误；健康另一规则能在同次扫描摘离。
10. 真实历史非合并两腿，一腿先结算K=6.93、另一腿R=3，余额13.93；后腿亏3后credit0，K未重复入账，原日最终净利3.93/结算2笔。

## 发现与修复闭环

| 问题 | RED证据 | 根因 | 最终结果 |
| --- | --- | --- | --- |
| 取消档案造成credit扫描饥饿 | 初次8项7通过/1失败，3.676秒，exit1；`test_inconsistent_cancelled_ledger_does_not_starve_a_ready_credit` 期望第二档resolved，实际awaiting_reconciliation | SQL按event终态预筛，代码发现job仍unknown后continue却不更新检查时间；limit1总选同一条 | Backend为该分支记录检查时间/原因，最终11项全过 |
| malformed leg ID中断所有恢复 | 增补3项中该用例TypeError，exit1；`test_invalid_legacy_leg_identifier_isolated_from_healthy_rule_recovery` | `int(None)`异常不被仅捕获ValueError的批处理隔离 | Backend在边界验证list/正整数并抛可隔离ValueError，最终11项全过 |

增补测试首次运行另出现测试fixture错误：执行任务列表不返回顶层leg_key；该辅助选取改用实际存在的send_with_job_id字段，未改变业务断言。该项单独复跑1通过/0失败/0跳过，0.468秒，exit0，之后纳入最终11项。

## 未覆盖与后续门禁

- 等待 W3 冻结后独立验证真实 WSGI 核对接口的认证、越权、租约、证据约束及 GET 白名单。
- 等待真实临时 WSGI 浏览器流程与前端最终契约。
- 最终集成后再执行受影响完整回归、覆盖率/未覆盖说明、编译和diff；由根代理安排避免重复资源竞争。
- 副本演练和生产一致备份/部署证据由 DevOps 提交；本报告未观察其结果。

## 状态

**STATUS**: DONE_WITH_CONCERNS
**REASON**: 核心独立测试全绿；API/UI/核对集成和最终发布验证尚未完成。
