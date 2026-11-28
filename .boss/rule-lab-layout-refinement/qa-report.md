# 列表布局调整 QA 报告

## 摘要

- **QA结论：通过，可进入发布。** 根代理冻结实现后复验五套真实WSGI／DOM程序全部成功，相关观察API 6项全部通过；没有未修复的阻断缺陷。
- 独立布局程序使用真实WSGI、临时SQLite和真实页面JavaScript，只允许GET；旧结果管理回归的创建／删除仅作用于临时测试库。不启动浏览器、不读取profile。
- 覆盖五列概览、同窗口数值、三视图零请求、比较子方案独立详情、抽屉焦点与过期响应、旧入口和原结果保持。
- CSS只检查自适应声明与DOM结构，不代表像素布局或跨浏览器验收。

## 执行记录

工作目录：/www/wwwroot/pc28touzhu-lab-layout。

| 范围 | 命令 | 退出码 | 通过／失败／跳过 | 失败详情 |
|---|---|---:|---|---|
| 独立布局RED | node tests/test_rule_lab_layout_qa.cjs | 1 | 0／1／0（完整验证程序） | 缺少labResultTabs；/tmp/pc28-layout-qa-red.log |
| 隐藏选择器CSS反例RED | node tests/test_rule_lab_layout_qa.cjs | 1 | 0／1／0 | labStudySelection带hidden但computed display为grid，应为none；/tmp/pc28-layout-qa-selector-red.log |
| 焦点恢复反例RED | node tests/test_rule_lab_layout_qa.cjs | 1 | 0／1／0 | 管理抽屉重复调用无preventScroll的focus；/tmp/pc28-layout-qa-focus-red.log |
| 最终独立GREEN | node tests/test_rule_lab_layout_qa.cjs | 0 | 1／0／0（完整验证程序） | 无；/tmp/pc28-layout-qa-final.log，session93296 |
| QA脚本语法 | node --check tests/test_rule_lab_layout_qa.cjs | 0 | 1／0／0 | 无 |
| 选择失败页码反例RED | node tests/test_rule_lab_layout_ui.cjs | 1 | 0／1／0 | 选择失败后内部分页被重置，第二页点击下一页错误返回21–40而应41–50；/tmp/pc28-layout-page-rollback-red.log |
| 根代理最终布局主流程 | node tests/test_rule_lab_layout_ui.cjs | 0 | 1／0／0 | 无；/tmp/pc28-layout-root-ui.log，session86991 |
| 根代理最终独立QA | node tests/test_rule_lab_layout_qa.cjs | 0 | 1／0／0 | 无；/tmp/pc28-layout-root-qa.log，session23459 |
| 根代理旧观察流程 | node tests/test_rule_lab_observation_ui.cjs | 0 | 1／0／0 | 无；/tmp/pc28-layout-root-observation.log，session73093 |
| 根代理旧金额流程 | node tests/test_rule_lab_amount_ui.cjs | 0 | 1／0／0 | 无；/tmp/pc28-layout-root-amount.log，session78956 |
| 根代理旧结果管理流程 | node tests/test_rule_lab_results_ui.cjs | 0 | 1／0／0 | 无；/tmp/pc28-layout-root-results.log，session39288 |
| 相关API烟测 | env PYTHONPATH=src PYTHONDONTWRITEBYTECODE=1 python3 -m unittest tests.test_rule_lab_observation_api | 0 | 6／0／0 | 无；0.623秒，/tmp/pc28-layout-root-api.log |
| 最终JS语法 | node --check src/pc28touzhu/ui/rule_lab.js | 0 | 1／0／0 | 无；根代理最终确认 |
| 最终空白检查 | git diff --check | 0 | 1／0／0 | 无；根代理最终确认 |

## 已通过的独立场景

1. 默认仅实验tab可见，创建／系统／设置关闭，dialog移出main避免inert锁住弹层。
2. 概览五列，每格至多两个短内容块、无嵌套details，每行一个原生条件button，无嵌套tr按钮语义。
3. 选择10月完整月，窗口净利与累计值故意不同，按实际API对照窗口、月份ROI和日结回撤。
4. 三种视图及月度ROI／回撤切换零GET。
5. 行空白和条件入口打开详情；Tab循环、ESC／遮罩关闭恢复焦点、页码、视图和滚动。
6. 关闭加载中的抽屉不会被迟到响应重开；关闭旧行后打开新行，旧响应不能覆盖。
7. 比较表提前止盈子方案TP5，注额10换算TP50，不套主批次TP10→100；主选择、tab、视图、过滤保持。
8. 创建／系统／管理／续算／CSV入口可达，原批次和比较组API结果deepEqual保持。
9. CSS媒体查询包含结果和抽屉布局规则，横向滚动受表容器限制。真实CSS在页面脚本前载入；inactive选择器／结果panel及hidden抽屉的computed display为none，所有移出main的抽屉仍处于lab-page样式范围。
10. 概览按收益排序准确使用窗口summary.net_cents；以1个月完整10月与全历史排序不同的实际fixture对照前20个ID，切排序零GET。
11. 比较组详情继续显示累计Δ净利／ΔROI／Δ回撤，与实际API行对照；该累计对照块明确标注累计，不混称所选窗口。
12. 详情、管理与续算关闭的所有focus调用均带preventScroll；修复后只使用统一dismissDrawer恢复，防第二次默认focus导致跳滚。

原生button的Enter／Space默认激活由浏览器提供，jsdom不执行其默认键盘click；本程序只对原生语义和自定义Tab／ESC键盘handler作可核验证明，不冒充真实浏览器按键验收。

## 最终导航与回归验证

- 读取新批次或比较组失败时，选择框回滚至实际显示的对象，原行、过滤和分页均保持；仅在新对象读取成功后重置页码。最终主流程验证在原第二页选择失败后继续到第三页，避免只检查静态页码文字却遗漏内部状态。
- 观察窗口3个月请求失败并关闭设置后，顶部“仅匹配”从已应用设置重建，只改匹配开关、不重发隐藏失败草稿GET；当前仍清晰显示2个月口径。
- 从另一tab创建实验／比较组成功后，自动切换正确tab且聚焦可见选择器；创建403时已关闭的抽屉不被错误重开。
- 原观察流程继续验证2.995%及2.004pp边界、设置持久化、样本／阶段／完整状态、分页、乱序、失败与认证变化；原金额流程及旧结果管理创建／改名XSS／删除／续算／CSV均通过。

## Contract Matrix验收

| 合同 | 证据 | 结论 |
|---|---|---|
| L1 结果优先与唯一tab | 主布局及独立QA，真实CSS hidden与选择框检查 | 通过 |
| L2 五列且同窗口口径 | 独立实际API窗口与累计不同反例、窗口profit排序 | 通过 |
| L3 三视图本地切换 | 两套布局程序请求计数零增量 | 通过 |
| L4 月度／风险含义 | 实际月ROI、日结回撤、最佳3天、回撤持续及窗口标签；CSS媒体规则 | 通过，像素未测 |
| L5 单入口详情 | 每行一个原生button及行空白点击、自定义焦点键盘 | 通过 |
| L6 抽屉生命周期与原位置 | Tab／ESC／遮罩、preventScroll、迟到响应、认证变化、页码回滚 | 通过 |
| L7 旧功能可达 | 原观察／金额／结果管理三套最终回归 | 通过 |
| L8 仅展示变更 | diff无Python运行文件变化、API6项、独立GET原结果deepEqual | 开发QA通过；线上保护归部署记录 |

## 最终结论与限制

**QA通过，可部署冻结提交。** 没有未修复的布局交互阻断项，线上保护另归发布记录。本次只改UI／文档／DOM测试，没有Python运行接口或账本变更，不重复整仓Python测试。未采集JS行／分支覆盖率，不以完整用户流程通过冒充覆盖率百分比。

首次实现后的GREEN尝试曾因QA把视图group button误当tab读取aria-selected而失败；已按实际且合规的aria-pressed更正该测试假设，主结果tab仍验证aria-selected。此修正没有降低功能断言或掩盖运行缺陷。

jsdom只能验证DOM、事件与CSS计算结果；未执行实际浏览器截图、手机像素宽度或跨浏览器验收，原生button键盘默认动作也未冒充真实浏览器证明。
