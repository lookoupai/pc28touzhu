# 中文回测报告术语与计算口径核对

2026-11-26：用户明确反馈“本金压力测算”“历史回撤压力倍数”“可承受本金回撤（%）”难懂，要求查GitHub常见回测总结。先查Context7的VeighNa官方文档，再直接只读GitHub仓库源码。没有下载执行外部代码、安装依赖或复制其实现。

## 实际来源

1. VeighNa `vnpy/vnpy_ctabacktester`，commit `a374dc65d506bb8e123738f5025e2480dc74ca10`，[StatisticsMonitor](https://github.com/vnpy/vnpy_ctabacktester/blob/a374dc65d506bb8e123738f5025e2480dc74ca10/vnpy_ctabacktester/ui/widget.py#L640)：起始资金、结束资金、总收益率、年化收益、最大回撤、百分比最大回撤、最大回撤天数、总盈亏、夏普比率、收益回撤比；图表包括资金/净值、回撤、每日盈亏和盈亏分布。
2. VeighNa `vnpy/vnpy_ctastrategy`，commit `7a8768de9784dda35a7b261a7ade1dbfbff50919`，[calculate_statistics](https://github.com/vnpy/vnpy_ctastrategy/blob/7a8768de9784dda35a7b261a7ade1dbfbff50919/vnpy_ctastrategy/backtesting.py#L310)：资金=初始资金+累计日净利；回撤百分比以资金历史高点为分母；收益回撤比为总净利/最大回撤金额；日资金不再为正时不继续计算一般收益统计。
3. RQAlpha `ricequant/rqalpha`，commit `0bf04946e8250b457e30c3c81d338a6b925da0bb`，[report.py](https://github.com/ricequant/rqalpha/blob/0bf04946e8250b457e30c3c81d338a6b925da0bb/rqalpha/mod/rqalpha_mod_sys_analyser/report/report.py#L167)：报告分“概览”“年度指标”“月度收益”“月度超额收益（几何）”。代码中的“月胜率”此处是相对基准的超额胜率，不可直接声称与本项目盈利月占比同义。只参考报告术语/组织，不引入基准超额比较。
4. QuantStats `ranaroussi/quantstats`，commit `d1a214fb906e76f974f45cdccf923b601c67a756`，[reports.py](https://github.com/ranaroussi/quantstats/blob/d1a214fb906e76f974f45cdccf923b601c67a756/quantstats/reports.py#L1709)：月度收益热图、Avg. Up/Down Month、Win Month、Max Drawdown、Profit Factor等。英文项目提供指标类型参照，不声称它提供中文UI标准。

查阅文件下载在`/tmp/pc28-report-terminology/`，本文记录来源和版本供复核。上述项目并无统一强制中文标准，但均没有把“回撤×默认倍数÷容忍比例”当作常规回测本金结论。RQAlpha另有历史情景压力测试，不应与之前的任意倍数换算混为一谈。

## 对本项目的结论

旧公式数学上可作为人为风险预算假设，但默认2倍缺乏统计校准，也不能推出最低本金；只换成常见名字仍会误导，故删除两个参数及反推金额。

采用“回测报告 / 月度表现 / 资金表现”，以累计净利润、已结算笔数、胜率、最长连续亏损、最大回撤、收益回撤比为主。保留投注特有口径：投注收益率=净利润/累计投注额，不能叫本金收益率；胜率明确退款不计。收益回撤比不是平均盈利/平均亏损的“盈亏比”，当前日汇总不能可靠恢复逐笔盈亏分布，因此不伪造盈亏比或盈利因子。

资金表现采用唯一可选输入“初始资金”，无默认值。原数据新增日结累计净利润曲线（整数分），用户输入后换算资金。最大回撤率按每个时点的资金高点逐点计算，零亏损显示0%；零结算、缺少完整日历史、资金归零/为负时分别说明。初始资金未参与原引擎的资金不足停投检查，因此这是报表换算，不能宣称完成资金约束回测或建议投入某一金额。

每月净利润和月内日结回撤保持原账本口径。月度胜率明确只算完整且有结算的月份；无基准时不展示超额收益。年化收益、夏普等虽常见，但需明确日历、无风险收益与资本假设，不在本次强行加入。

## 变更边界

仅新增详情响应equity只读字段，保留既有monthly/capital字段兼容；不迁移数据库、不改变风控/下注/调度、不清零已有结果。首次资金值纳入高点，避免只看首个交易日而漏掉开局亏损。完整日数据判断同时核对结算数、整数分净利和窗口是否截断。
