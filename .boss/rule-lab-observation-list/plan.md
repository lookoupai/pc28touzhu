# 列表观察规则实施计划

## 摘要

- 用户已于 2026-11-28 批准“可配置列表观察规则”，需求来源为会话 `msg_2d586e2739bb4d109062a9afc29b4b55` 及前序分析；本次进入 review → tasks → dev → qa → ship。
- 使用既有批次／比较组 GET 的显式观察参数，批量读取现存日汇总，在与成绩相同的只读快照中生成摘要；不访问预测来源、不回放、不迁移、不改账本或调度。
- 观察窗口必须是连续自然月；部分月、未计算、无交易、缺失月不可被压缩掉。观察条件与月间波动分别展示，不给“稳定盈利分数”。
- 页面同时更新实验表与比较表，默认展示两个月、月 ROI≥3%、每月≥200笔且≥10个投注日；支持 1～12 个月、完整月模式、可选风险限制和高亮色，设置只保存在本浏览器。
- 三道证据波：纯统计及只读契约、真实页面交互、独立边界与资源／部署检查。正式发布只重载平台 API，保留现有模拟 worker 及所有实验状态。

## 1. 实施策略

基线为 `ca1d918`，开发目录 `/www/wwwroot/pc28touzhu-lab-observation`。复用 Python 标准库 WSGI、SQLite、原生 JavaScript／CSS，不新增依赖。旧请求不带观察参数时保持既有形状与成本；新页面一次读取所选批次或组的全部观察摘要，不逐行调用详情接口。

所有金额先用整数分计算。日/月 ROI 为该区间净利之和÷投注额之和；退款仍计入已有投注额分母。显示可四舍五入到两位小数，筛选必须以原始分值计算，不能将 2.995% 显示为 3.00% 后判为达标。所有窗口默认使用原模拟注额，详情里的金额估算不反向影响列表筛选。 客户端可将最多两位小数的阈值转为百分之一单位，以 BigInt 交叉乘比较：月 ROI 门槛为 `net_cents×10000 >= threshold_hundredths×stake_cents`；差距上限使用最大／最小 ROI 的原始分数交叉乘，避免二进制浮点在恰好2个百分点时误判。

## 2. 模块划分

| 模块 | 职责 | 依赖 | 复杂度 |
|---|---|---|---|
| M1 连续月份与风险摘要 | 纯函数选择窗口、汇总月度和近期结果、形成质量阻断原因 | 既有月报日期及覆盖语义 | 高 |
| M2 一致只读查询 | 批次／组事务、批量日汇总和核对、HTTP 参数校验 | M1 | 中 |
| M3 观察设置与列表 | 设置持久化、精确筛选、语义高亮、紧凑摘要、运行信息展开 | M2 已冻结契约 | 高 |
| M4 独立验收与发布 | 金额／日期反例、并发只读、性能、旧交互、线上保护校验 | M1～M3 | 中 |

## 3. 冻结 API 契约

### 3.1 请求

仅以下两个既有 GET 接受观察参数：

- `/api/platform/rule-lab/batches/{id}?observation_months=2&observation_partial=true`
- `/api/platform/rule-lab/studies/{id}?observation_months=2&observation_partial=true`

参数必须成对提供，`observation_months` 为标准十进制字符串 `1`～`12`，`observation_partial` 严格为 `true` 或 `false`。重复、空、单独提供、未知参数、越界或非标准值均返回既有 `400 invalid_input`。其他接口保持原来的查询白名单，尤其不能让观察参数进入状态写接口或分段分页。鉴权／所有权／`Cache-Control: no-store` 保持现状。无参数请求不附加 observation。

### 3.2 响应

原 `batch + experiments` 或 `study + rows` 结构保持；显式请求时，每个实验／比较行附加下述 `observation`。`version` 固定为 1。空值为 JSON `null`，不能使用 NaN、Infinity 或用零冒充未知。

```json
{
  "version": 1,
  "original_stake_cents": 100,
  "window": {
    "requested_months": 2,
    "include_partial": true,
    "start_month": "2026-10",
    "end_month": "2026-11",
    "start_date": "2026-10-01",
    "end_date": "2026-11-27",
    "actual_through_date": "2026-11-27",
    "available_months": 2,
    "complete_months": 1,
    "has_partial": true,
    "period_reached": true
  },
  "months": [
    {
      "month": "2026-10",
      "status": "complete",
      "complete_month": true,
      "range_start": "2026-10-01",
      "range_end": "2026-10-31",
      "settled_count": 698,
      "betting_days": 31,
      "stake_cents": 69800,
      "net_cents": 5544,
      "roi_percent": 7.94,
      "daily_close_drawdown_cents": 2033,
      "worst_day_cents": -2000,
      "pending_count": 0
    }
  ],
  "summary": {
    "net_cents": 9525,
    "stake_cents": 130100,
    "settled_count": 1301,
    "min_month_roi_percent": 6.6,
    "roi_spread_pp": 1.34,
    "min_month_count": 603,
    "min_month_days": 27,
    "min_month_drawdown_cents": 1977,
    "max_month_drawdown_cents": 2033,
    "worst_day_cents": -2000,
    "best_three_days_cents": 7022,
    "net_without_best_three_days_cents": 2503,
    "longest_underwater_days": 15,
    "current_underwater_days": 0,
    "recovery_left_censored": false
  },
  "recent": {
    "start_date": "2026-11-14",
    "end_date": "2026-11-27",
    "available": true,
    "settled_count": 331,
    "stake_cents": 33100,
    "net_cents": 3146,
    "roi_percent": 9.5
  },
  "previous": {
    "start_date": "2026-10-31",
    "end_date": "2026-11-13",
    "available": true,
    "settled_count": 293,
    "stake_cents": 29300,
    "net_cents": 779,
    "roi_percent": 2.66
  },
  "quality": {
    "blockers": [],
    "warnings": ["known_skips"],
    "input_warnings": {"insufficient_history": 100},
    "history_consistent": true,
    "study_comparable": true
  }
}
```

示例仅展示一条月份对象，实际 `months` **恰好为 requested_months 条**，升序连续。`summary.roi_spread_pp` 等显示字段可保留合理精度，但客户端判断月 ROI 及 spread 时必须由每行 `net_cents/stake_cents` 求值；不得以这两个展示字段做边界判定。`original_stake_cents` 用于回撤“相当于多少注”的换算。

`months[].status` 枚举：`complete`、`partial`、`pending`、`no_samples`、`not_replayed`、`not_observed`、`missing`。前六种沿用月报语义；请求窗口超出实验起点时提供 `missing` 占位：金额／笔数／天数为 0，ROI／回撤／最差日为 null、complete_month=false。真正完整且没有交易的月份为 `no_samples`，不是 missing，仍不能通过每月样本与 ROI 门槛。

`available_months` = 所选连续月份中，有结算且已进入实际处理范围的月份数；`complete_months` = 完整且有结算月份数。`has_partial` 表示所选有样本月份中存在未完整月份。`period_reached` 表示实际处理前沿已覆盖所选窗口的最后一个目标日，不代表自然月完整或来源从未缺失。未结束的当日、未开始批次、尚未回放至窗口末端必须为 false。

### 3.3 窗口锚点

1. 批次锚点为其配置／增量有效结束日；比较组锚点为所有子批次有效结束日最大值。两者均不晚于北京时间今天。
2. 同一返回的所有行使用相同的 `start_date/end_date`。不能因某子方案尚未处理到最新月而偷偷使用其更早窗口；通过 actual_through_date、not_replayed、missing、period_reached 显示差异。
3. 包含未完整月：以锚点月份为末月，回溯 N−1 个自然月；末日为锚点日。
4. 仅完整月：末月必须同时满足月末不晚于锚点日、该自然月在现实时间已结束；然后回溯 N−1 个连续月。不能向前搜索 N 个有交易／达标月份。例如锚点 11 月 27 日且 N=2，目标就是 9、10 月；只有 10 月数据时显示 1/2，不换成 10、11 月。
5. 每行 actual_through_date 取实际已处理前沿与目标结束日较早者；无前沿为 null。日结记录只证明该日有活动，不能单凭最后日账将整月判完整。已完成固定回放可借助 cutoff 证明区间已结束，增量采用 completed_through_date。
6. 月完整性须与现有 monthly_report 对同批次同月份一致，包括部分起月、月底中午、跨月 pending、当前追加未完成等情况。

### 3.4 指标定义与范围

- 月 ROI：月内净利合计／投注额合计×100%；未有有效投注额为 null。
- 月间差距：所选全部有可计算 ROI 月份的最大 ROI−最小 ROI，单位为百分点；不足两个月为 null。缺月仍不能被当作完整 N 月通过。
- 月回撤：月初累计净利从 0 开始，按每天净利累计，记录前高至后低最大差；只称“日结回撤”。月回撤范围为各有交易月份的 min/max。累计原列表最大回撤仍是原全区间逐笔指标，不改名混用。
- 最近／前一段：以**共同观察窗口的 end_date**为末日，最近 14 天为 `[end−13,end]`，前 14 天为 `[end−27,end−14]`，每段固定自然日，不能随行偏移。首日早于实际观察起点、末日尚未被实际处理覆盖或段内尚有待结算时 available=false，settled_count／stake_cents／net_cents／roi_percent 全部为 null；不能把未来日期当作零，也不移到更早时段。available=true 且无交易时笔数、金额为0，ROI 为 null。最近与前一段分别判断 available。界面明确写“窗口末14天／截至 YYYY-MM-DD”。 若实际首次观察日期晚于配置起始日，且该首次时间不是当日00:00，首个完整覆盖日保守取次日；该日期恰为14天首日时必须available=false。配置起始日的正常首个开奖（例如00:05）仍沿用既有月报日界约定，不能因此把全部正常回放判为缺日；展示覆盖不等于证明来源全天从未缺失。
- 最佳三天与去除后净利：仅在所选窗口内取净利为正的最多三个最高日，summary.net_cents 减去这三天合计。不排除负天，不重新模拟风控，不将其称预测收益。
- 回撤持续：仅在所选窗口内，以窗口前一日累计净利 0 为初始高点，按自然日计算距离最近达到／刷新高点的天数；回到旧高点即恢复。例如窗口前一日高点0，第1天亏、第3天回0，持续为3个自然日；恢复当天计入日差。无投注日仍计自然日。最长值包含尚未恢复的当前段，同时返回 current_underwater_days。无交易或未覆盖窗口时值 null 或明确的部分数据状态，不延伸到未实际处理的未来日期。
- 若实验在窗口前已开始而此前高点未载入，recovery_left_censored=true；文案“窗口内最长回撤持续”并提示窗口起点以前的高点未纳入，不能称全历史恢复时间。该指标从窗口起算，缺少前史不是 history_mismatch。

### 3.5 质量状态

`quality.blockers` 为以下去重字符串，客户端按顺序优先显示“待核对”，即使数值通过也不自动标达标：

- `legacy_engine`：旧回放算法需复核。
- `input_conflict`：输入冲突、报告 coverage_quality=conflict。
- `archive_gap`：输入过期／缺档，不能确认数据覆盖。
- `window_pending`：所选窗口仍有待结算，不能把未定结果当最终。窗口之外的 pending 不得阻断本窗口；最近／前一段的 available 也只检查各自区间。
- `history_mismatch`：全日账的结算笔数／净利／投注额与累计 stats 不一致；必须用全日账聚合核对，不能用所选窗口合计误报。
- `study_not_comparable`：比较组当前未满足共同输入可比条件；不自动给排名／达标标签，仍显示已有数字。

`quality.warnings`：`known_skips`（含原 input_warnings 字典）、`partial_window`、`window_not_reached`、`recovery_window_boundary`。普通暖机不足／晚到预测等已知跳过仅警示，不能让当前所有实验永远被阻断。停用／网络瞬时不可读不改变已保存历史的有效性；实时采集状态仍显示在原位置。

## 4. 查询与一致性

- 新观察读路径一次 `query_only` 连接并 `BEGIN`：所有者验证、原批次／组、实验 stats、日账聚合、观察结果均同快照。不得先调用分离连接的 get_batch/list_experiments 再补观察值。
- 每个批次的月报元信息（continuation、frontier、cutoff、首次观察、review_required）只读取／解析一次；不能每实验调用完整 `_monthly_report` 及 snapshots 查询。
- 目标实验数量遵守现有批次≤100、比较组≤5×100、全服务≤650；一次按 IDs 和有界日期批量读取目标日账。读取起点为 min(所选窗口起点, 前14天起点)，终点为共同末日，最多约一年；无静默截断。
- 用按实验 GROUP BY 的单次全日账聚合取得 count/net/stake 核对 stats；返回结果不附全日账或曲线，只附最多12个月与汇总。
- pending 按实验／目标月份批量汇总；来源 SQLite 和业务账本均不接触。
- 原 API 若不请求观察保持旧成本；新增批次／组读路径的查询增长以批次数增长，不随实验数新增月报／pending 查询。

## 5. 客户端设置与交互

设置模型固定为 version 1：months=2、includePartial=true、minimumRoi=3、minimumCount=200、minimumDays=10、maximumSpread=null、maximumMonthlyDrawdownUnits=null、maximumUnderwaterDays=null、highlight='red'、matchesOnly=false、showRuntime=false。months为1～12整数，minimumCount为≥1整数，minimumDays为1～31整数，ROI可为−100～1000的有限数且最多两位小数；可选上限空表示不限制，启用后须为有限非负数（天数整数）。色彩白名单 red/blue/purple。用户可输入自定义月数并用2／3／6快捷选择。

只将偏好写入带版本号的 localStorage 键，不保存实验成绩或凭证，不承诺跨设备同步。读取失败／格式无效安全恢复默认并提示；不能让旧无效字段注入样式。设置表单显式应用／恢复默认；应用有效设置后才更新保存。只有 months/includePartial 改变才刷新所选批次及组的单个 GET；阈值、颜色、仅匹配、运行信息展开仅重绘。所有请求用请求序号／设置回显拒绝旧响应覆盖最新设置；网络失败时显示当前展示口径与待应用口径，不能用旧数据冒充新设置。

两个列表共用一份观察设置。观测状态优先级：质量阻断 → 数据未就绪／历史不足 → 样本不足 → 不满足数值条件 → 阶段达标或完整月达标。收益门槛通过但可选波动／风险门槛不通过，应分开展示“收益达标／波动超限”等原因，不能伪装成无收益。已知跳过、近期转弱、最佳三天依赖为辅助提示，不默认替用户追加隐藏门槛。

列表结构保持紧凑：条件＋赔率（比较表另加风控）、观察状态、近月 ROI／最低与差距、月日结回撤范围及多少注、月最少样本／天数、近期两段表现、累计原结果／操作。窗口内去除最佳三天净利与回撤持续放在轻量展开内容；运行轮状态／日期／本轮金额也放入展开，保留原信息。分页／筛选／原排序及新增最低月 ROI、月间差距、月回撤排序在应用规则后计算，过滤为空有明确空态。

红色仅用于“达标”文字／徽标的可选强调，不将整行收益统一染红；阶段达标用橙色，样本不足用灰色，风险／数据提示用橙色加具体文字。原金额正负样式可继续保留，必须用状态文字避免仅靠颜色表达。手机端优先保留条件、状态、月表现，再展开更多，复用44px触控和原焦点样式。

## 6. 风险与验证

重点反例：2.995%边界；中间零交易／缺失月；只完整月少于N；月底未收盘；pending旧月；并发结算；累计日账缺行但净利恰巧抵消；组未完成窗口不同；负值和0回撤；跨年闰月；窗口前高点截断；最后14天未计算；筛选分页；localStorage损坏／禁用；颜色和输入XSS；切换设置响应乱序；无数据源情况下只读仍可用。

性能使用真实一致副本和至少100版本×12个月合成数据（比较组覆盖500版本）验证新增查询数、响应大小、耗时及峰值内存。运行速度／100运行上限／650保存上限／1GiB预算保持。验收不启动浏览器进程、不读取用户浏览器资料，使用真实WSGI＋jsdom；布局验证以结构／CSS和DOM检查为证，不能声称浏览器像素验收。

## 7. 里程碑

| 里程碑 | 交付 | 验收 |
|---|---|---|
| W1 | 纯统计＋批量只读接口 | 日期、舍入、ownership、同快照、legacy响应通过 |
| W2 | 观察设置＋两个列表 | 保存／筛选／颜色／空态／乱序和原交互通过 |
| W3 | 独立QA＋发布 | 反例、容量性能、线上GET与原账本保护通过 |

## 8. 任务拆解指引

以统计核心、读取接线、页面状态、交互验收分别拆分；中央 store 与 app 由同一后端 owner，UI全部由同一前端 owner。独立QA不改实现；发现缺陷交回owner。发布准备可与开发并行，只读检查；正式重载由根代理放行准确提交后执行。

## 9. 部署与回滚

QA通过后，由根代理给出准确提交；DevOps核对生产工作树clean、记录原实验定义／结果／日账和服务状态后快进，只重载platform API。无依赖安装、无schema迁移、无worker或业务服务主动重启。上线用GET验证两个观察列表及参数拒绝、页面设置交互、原数据指纹与服务健康。若出现功能回归，可回退本次代码并重载API；数据库持续保留，禁止用旧副本覆盖已有结果。
