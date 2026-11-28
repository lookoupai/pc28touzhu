# 布局调整任务

## 摘要与仓库事实
- 技术栈：原生JS/CSS/HTML、Python标准库WSGI，既有jsdom位于 `/tmp/pc28-draw-trigger-ui-check/node_modules/jsdom`；不安装依赖。
- 生产及开发基线601bfab；开发分支feat/rule-lab-layout-refinement初始clean。CI只有Docker构建；本次使用已有真实WSGI DOM脚本。
- 只重组显示，现有观察API、650保存/100运行/256MiB限制、模拟账本与调度不改；用户已批准布局调整，范围可逆。

## 1. 写集与任务
| 任务 | 文件（owner独占） | 验收 |
|---|---|---|
| T1 Frontend布局/交互 | `src/pc28touzhu/ui/rule_lab.{html,js,css}` | 结果优先、双tab、5列概览、三视图、单入口详情、抽屉状态 |
| T2 Frontend真实DOM | `tests/test_rule_lab_layout_ui.cjs` | 先RED后实现；窗口金额、0GET视图切换、焦点恢复和原金额详情 |
| T3 Frontend必要兼容 | `tests/test_rule_lab_observation_ui.cjs`、`tests/test_rule_lab_amount_ui.cjs`、`tests/test_rule_lab_results_ui.cjs`；`docs/RULE_LAB.md` | 仅结构断言适配，原创建/管理/删除/续算和统计意义保持；旧fixture不改 |
| T4 QA独立验证 | `tests/test_rule_lab_layout_qa.cjs`、`.boss/rule-lab-layout-refinement/qa-report.md` | 独立语义/生命周期反例，DOM与CSS边界，不修改运行实现 |
| T5 DevOps发布 | `.boss/rule-lab-layout-refinement/deploy-report.md`及/tmp临时脚本 | 准确提交GO前只读准备；API重载、线上只读/原数据校验 |
| 规划 | 本目录`plan.md`、`tasks.md`、`waves.json` | Planner仅文档，不改运行代码 |

最终实际写集以diff为准，三份旧测试只在确需结构适配时修改。新ID由Frontend及时通知QA；不并行写同文件。Frontend与QA可先分别编写RED，T4 GREEN须等T1/T2就绪。

## 2. Contract Matrix
| ID | 用户承诺 | 客户端/数据约束 | 验证 |
|---|---|---|---|
| L1 | 首屏只显示当前结果tab | 另一个tab aria-hidden/hidden；系统和创建默认关闭 | UI+QA |
| L2 | 概览5列且口径统一 | 最低月ROI、最高月日结DD、summary.net_cents；组风控并入身份 | UI+QA对照故意不同的窗口/累计值 |
| L3 | 三视图快速切换 | 本地重绘，不GET、不改变选定窗口/过滤 | UI+QA请求计数 |
| L4 | 月度和风险不丢含义 | partial/missing/pending保持；风险≤6列；月度只容器滚动 | UI+QA+CSS |
| L5 | 每行一次打开详情 | 条件button可键盘；行点击；不产生重复按钮 | UI+QA |
| L6 | 抽屉关闭回原位置 | ESC/遮罩/按钮、焦点、scroll锁、旧请求、认证变更 | UI+QA生命周期反例 |
| L7 | 旧功能仍可用 | 管理删除/创建/续算/导出/金额估算入口保留 | 三份旧DOM回归 |
| L8 | 仅展示改变 | 无新Python/API写、账本不变、只重载API | diff检查+线上GET保护 |

## 3. 证据波与放行
- W1：Frontend RED → 布局实现 → 新DOM＋旧观察/金额/结果DOM通过；准确窗口金额、默认5列、抽屉任何一项失败先修复。
- W2：独立QA＋相关API烟测＋diff检查，最后由根代理准确提交放行部署；不拿jsdom声称像素布局通过。
- argv命令见waves.json；只使用现有依赖，Python环境PYTHONPATH=src。记录实际退出码与日志，未发布前保持release pending。


## 4. 发布前收尾
T1～T5已完成，W1及W2通过；提交60ac9a4已部署并完成线上核验。根代理新布局、独立QA、原观察、金额与结果5份DOM均退出0，相关API6项0.623秒通过。失败选择回滚／保留页码、快捷开关零GET与草稿隔离、创建成功tab/焦点和403清理均已覆盖，复核未解决问题0。旧批次及比较按钮长列表实际移除，不是隐藏保留。正式运行变更仅HTML/JS/CSS，其他变更为测试和文档；发布后的核验记录见下。


## 5. 发布完成
2026-11-28部署提交 `60ac9a4`；记录 `/tmp/pc28-layout-release-rollout-20261128T053753Z.json`。线上 `/tmp/pc28-layout-release-online.json` 及同名log验证40次GET、0写请求、6项布局检查通过、1个执行器在线；公网JS/CSS均200且与生产文件一致。638个原版本的配置及经济数据指纹保持。W1、W2全部通过，部署细节以deploy-report.md为准。
