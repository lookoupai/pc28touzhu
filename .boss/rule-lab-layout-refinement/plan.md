# 实验室列表布局调整

## 摘要与评审
- 用户 `msg_6733f1a5ca634f40814d74c6cd4f4cd7` 已批准“结果优先、概览5列、三种视图、详情侧栏”；本次做中等结构调整，主问题是次要信息与结果同权重、单元格段落过多。
- 基线 `601bfab`，隔离开发目录 `/www/wwwroot/pc28touzhu-lab-layout`；仅HTML/JS/CSS、相关DOM测试和使用说明，不改Python、API、日账、赔率或调度。
- 评审通过：现有observation包含所需窗口统计，可在前端重组，无新增网络请求或资产。主要风险是窗口／累计金额混称、抽屉关闭失焦、旧功能入口丢失及筛选响应乱序。
- 验证采用真实WSGI＋jsdom与结构/CSS检查；不启动浏览器或读取profile，不声称像素视觉验收。

## 1. 页面结构与信息口径
1. 首屏为紧凑页头：当前批次／比较组选择、刷新、新建、系统；下面是“实验结果／风控对比”两个tab，同一时刻仅一张结果列表可见。
2. 系统资源、创建表单默认关闭，通过抽屉打开；现有管理、删除、续算、导出、风控比较入口继续可达，不为了视觉精简删功能。
3. 筛选工具条仅短摘要“近2月 · 每月≥3%”、仅匹配开关和“调整条件”；调整条件打开已有观察设置，全部原阈值与本地持久化规则保留。
4. 视图“概览／月度／风险”只切换本地数据，不发GET。tab与视图切换保留当前选择、过滤、排序和页码（超出过滤结果时夹取合法页）。
5. 概览**恰好5列**：条件＋赔率（组行附风控名）、观察状态、最低月ROI、最高月日结回撤、窗口净利。三个数值全部来自同一 `observation` 窗口，窗口净利不得取全历史 `item.net_profit`。
6. 月度视图按月份列展示，可在ROI／日结回撤切换；列顺序、部分月星号、无样本与缺月语义保留。只有月度大表允许容器内横向滚动。
7. 风险视图最多6列：身份、状态、窗口去最佳3天净利、窗口最长/当前回撤持续、窗口末14天/前14天表现，以及必要风险数值；单元格短标签，不重新塞入解释段落。
8. 共同来源、日期范围、每注金额放在表上方；行内只保留异常／差异（待结算、缺档、部分或样本状态）。不能因移走说明而把阶段值显成完整月值。
9. 手机概览改为每项两行的紧凑信息布局，保持条件、状态、关键值可见；不能把桌面8列挤成整页横向滚动。月度滚动局限于表容器。

## 2. 详情与抽屉行为
- 每行仅一个可聚焦的条件入口，行非交互空白点击同样打开；不得保留“详情＋月度成绩＋子批次”等重复按钮。嵌套控件事件不误触发行打开。
- 详情复用既有 `labExperimentDetail` 及其月报／金额／本金控件；桌面右侧抽屉，手机占满视口，标题与关闭按钮固定。其他表单／系统抽屉同一时刻最多一个，避免叠层。
- 使用dialog语义、明确标题关联、焦点进入与Tab约束；ESC／遮罩／关闭一致恢复到原入口。背景滚动锁定与inert/等价处理成对恢复，保留列表滚动位置、tab、视图及过滤结果。
- 切换行／关闭抽屉后，已过期详情请求不能重新打开抽屉或覆盖下一行；认证变更清理抽屉及加载状态。
- 观察窗口请求沿用既有序号/echo保护；抽屉重排不得绕开新旧口径隔离。所有金额换算继续仅本地，窗口筛选不受详情估算影响。

## 3. DOM与兼容约定
保留旧表体 `labResultsBody`、`labStudyRows`、各详情表体和表单控件ID、原分页/操作函数语义。Frontend确认新增ID：`labResultTabs`、`labTabExperiments`、`labTabStudies`；`labSelectionBar`、`labBatchSelection`、`labStudySelection`、`labBatchSelect`、`labStudySelect`；`labViewOverview`、`labViewMonthly`、`labViewRisk`（配data-result-view）、`labMonthlyMetricSelect`（roi/drawdown）；`labQuickObservationSummary`、`labQuickMatchesOnly`、`labAdjustObservation`、`labResultMeta`；表头更新目标 `labResultsTable`、`labStudyTable`；触发 `labNewExperiment`、`labSystemToggle`，wrapper `labSystemPanel`，关闭 `labObservationClose`、`labGeneratorClose`、`labSystemClose`，公共遮罩 `labDrawerBackdrop`。测试优先role、aria与稳定data属性。
所有dialog移出main，以免背景inert同时锁住弹层。比较行详情使用独立 `detailContext={batchId,batch,sourceTab}`，不得借selectBatch切换底层tab／选择；关闭后回原比较结果。
旧批次／比较按钮长列表已移除，新的select调用原选择逻辑，页面仅保留这一套选择入口。累计逐笔回撤和累计净利继续在详情可查。

## 4. 交付和发布
两道门禁：布局主路径与旧交互回归，然后独立QA/线上保护。只改UI时不重跑完整Python套件；相关API烟测验证既有响应即可。根代理给准确提交后快进生产并仅重载platform API，线上GET检查、原数据保护与资源状态通过后交付。

## 5. 实现后只读复核
主结构已符合5列概览、三视图、本地切换及独立比较详情上下文；桌面固定表宽、手机网格、月度容器滚动和抽屉背景隔离具备对应CSS/DOM结构。未启动浏览器，不能宣称像素视觉验收。
两项复核问题已按RED/GREEN关闭：选择失败回滚选择器并保留原表／页码；快捷“仅看符合”只修改当前生效设置且零GET，不带入失败草稿。创建成功切换tab及恢复可见焦点、403不重开抽屉也已通过验证。未解决问题0；允许准确提交进入发布，尚未宣称上线。

发布前证据：根代理5份DOM日志 `/tmp/pc28-layout-root-{ui,qa,observation,amount,results}.log` 均退出0；相关API6项0.623秒通过，日志 `/tmp/pc28-layout-root-api.log`。运行代码仅3份UI文件，无Python、数据或调度变更。
