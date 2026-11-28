# 列表观察规则 QA 验证报告

## 摘要

- **QA结论：通过，可进入发布。** 根代理整仓Python执行1284项：1282通过、0失败、2项可选跳过；最终边界修复后27项统计／API／独立QA专项再次全部通过。
- 观察读取前后固定副本及合成压力账本的全表内容指纹一致，没有初始化或写入原副本，也未接触生产业务库或预测源。
- 真实92版本批次观察响应实测75～80ms，新增日账查询固定为2条；500版本×12个月、181,000日行约1.92～2.00秒，最大RSS139.5MiB，受1CPU／256MiB地址空间限制执行成功。
- 新观察页面、旧金额估算、旧结果管理三套真实WSGI＋jsdom流程均通过；设置精确比较、持久化、两个列表分页、响应乱序／失败和登录变化、展开阅读及原结果保持均有执行证据。
- 两个新增统计核心模块实测行覆盖率分别为97.18%和94.19%，均超过80%门槛；未测分支覆盖率，不将stdlib trace的错误100%摘要当覆盖率。
- 未启动浏览器、未访问用户浏览器资料；DOM验证不等于像素布局或真实浏览器兼容性验收。线上部署及部署后GET保护校验由发布环节记录。

## 测试执行记录

所有命令在 `/www/wwwroot/pc28touzhu-lab-observation` 执行。固定副本为根代理通过SQLite一致备份生成的 `/tmp/pc28-observation-baseline-20261128.db`，29,581,312字节，测试只读打开。

| 范围 | 完整命令 | 退出码 | 通过／失败／跳过 | 失败详情 |
|---|---|---:|---|---|
| 独立攻击RED | `env PYTHONPATH=src python3 -m unittest tests.test_rule_lab_observation_qa` | 1 | 0／9／0 | 新查询返回400而应200；另8项因尚无observation_months接口参数报TypeError。日志 `/tmp/pc28-observation-qa-red.log` |
| 独立攻击GREEN | `env PYTHONPATH=src python3 -m unittest tests.test_rule_lab_observation_qa` | 0 | 9／0／0 | 无；0.841秒，日志 `/tmp/pc28-observation-qa-green1.log` |
| 固定副本语义＋资源压力 | `env PYTHONPATH=src python3 -m tests.rule_lab_observation_benchmark --database /tmp/pc28-observation-baseline-20261128.db --synthetic --output /tmp/pc28-observation-qa-benchmark.json` | 0 | 1／0／0（验证程序） | 无；程序逐项核对638个实验，并执行8种读路径各3次计时。日志 `/tmp/pc28-observation-qa-benchmark.log` |
| 测试文件语法 | `python3 -m py_compile tests/test_rule_lab_observation_qa.py tests/rule_lab_observation_benchmark.py` | 0 | 1／0／0（检查命令） | 无 |
| 根代理整仓Python | `env PYTHONPATH=src PYTHONDONTWRITEBYTECODE=1 nice -n 10 python3 -m unittest discover -s tests` | 0 | 1282／0／2 | 无；293.726秒，`/tmp/pc28-observation-full-python.log`。运行开始早于最后2项首日覆盖测试及对应修复，最终代码另由下一行专项补验 |
| 最终统计／API／独立QA | `env PYTHONPATH=src PYTHONDONTWRITEBYTECODE=1 python3 -m unittest tests.test_rule_lab_observation tests.test_rule_lab_observation_api tests.test_rule_lab_observation_qa` | 0 | 27／0／0 | 无；2.196秒，`/tmp/pc28-observation-root-targeted.log`，包含最终首日覆盖及空删除组修复 |
| 新观察页面主流程 | `node tests/test_rule_lab_observation_ui.cjs` | 0 | 1／0／0（完整交互程序） | 无；根代理复验 `/tmp/pc28-observation-root-ui.log`；前端最终同程序日志 `/tmp/pc28-observation-ui-final.log` |
| 原金额估算流程 | `node tests/test_rule_lab_amount_ui.cjs` | 0 | 1／0／0（完整交互程序） | 无；根代理复验 `/tmp/pc28-observation-root-amount.log` |
| 原结果管理流程 | `node tests/test_rule_lab_results_ui.cjs` | 0 | 1／0／0（完整交互程序） | 无；根代理复验 `/tmp/pc28-observation-root-results.log` |
| 核心行覆盖率采集 | `env PYTHONPATH=src PYTHONDONTWRITEBYTECODE=1 python3 -m trace --count --summary --coverdir /tmp/pc28-observation-trace --module unittest tests.test_rule_lab_observation tests.test_rule_lab_observation_api tests.test_rule_lab_observation_qa` | 0 | 27／0／0 | 无；4.705秒，`/tmp/pc28-observation-trace.log`；正确覆盖率算法见下节 |
| 最终JS语法 | `node --check src/pc28touzhu/ui/rule_lab.js` | 0 | 1／0／0（检查命令） | 无；根代理最终确认 |
| 最终空白检查 | `git diff --check` | 0 | 1／0／0（检查命令） | 无；根代理最终确认 |

## 独立攻击内容

测试使用真实SQLite和真实WSGI应用。合成账本明确是统计边界fixture，不冒充真实回放成绩；`ForbiddenSource`拒绝任何来源属性访问，任何误读源数据立即使测试失败，不伪造来源成功。

1. 显式观察GET是真实可用功能，返回no-store、原始599／20000分（2.995%），没有数据库内容变化；旧无参数响应不含观察字段。
2. 连续三个月之间的零交易月保持no_samples；完整月模式按日历回溯，缺失起月保持missing，不压缩成最近有交易的月份。
3. 实际观察从11月20日才开始时，窗口末14天仍锚定11月14～27日，available=false且数值为null，不偷偷滑动日期。
4. 所选窗口之外删除一正一负的两个日账，净利仍相同，count／stake核对仍准确发现history_mismatch；正常前史不被误报。
5. 决策时间在10月、目标risk_date在11月的pending，只阻断相应11月窗口，不污染10月完整性；最近／前一段分别判断。
6. 回撤持续计算包含没有投注的自然日与恢复当天；测试最长4天、当前2天，窗口前史截断单列，2.5原注额保留，最佳3天仅作用于所选窗口。
7. 最后目标日只处理至中午且批次仍运行时，period_reached与窗口末14天available均为false；已经覆盖的前14天仍可用。
8. 比较组尚未完成时所有行带study_not_comparable；落后子方案使用共同窗口并明确实际截止，不能用较早窗口冒充可比较。
9. 首次日账查询期间另一SQLite连接真正提交新结算：读连接为query_only且已BEGIN，同次返回旧stats及旧日账摘要，后续新请求才同时看见新值。

## 生产一致副本对照

所有638个实验逐月检查：月份状态、完整标志、结算笔数、投注日数、净利分、投注额分、月内日结回撤分、最差日分，与旧月报相同。观察ROI保留6位用于展示，按两位显示口径对照旧月报；阈值仍必须由前端原始整数分精确判断，不使用显示舍入值。

全表内容SHA256在读前后均为：

`51fa40867b222ae18d46654f336483ddbd59cccb86ca300d909d1ae2af1d6b6d`

文件未被初始化或迁移；各表按rowid稳定读取，BLOB只计内容哈希和长度，未将原始payload写入报告。

## 资源与查询证据

实际通过 `os.sched_setaffinity` 限定1个CPU，`resource.RLIMIT_AS`限定268,435,456字节。以下均为同一次验证程序内的3次实际计时范围，不是长期P95／P99指标。

| 场景 | 无观察耗时 | 带观察耗时 | 带观察JSON | 日账查询 无／有 |
|---|---:|---:|---:|---:|
| 真实92版本、2月 | 14.5～15.0ms | 75.0～79.9ms | 301,907B | 0／2 |
| 真实10版本比较组、2月 | 4.2～7.0ms | 11.7～15.3ms | 42,397B | 10／12 |
| 合成100版本、12月 | 15.2～15.9ms | 391～401ms | 601,022B | 0／2 |
| 合成500版本比较组、12月 | 126.8～133.1ms | 1.919～2.003秒 | 3,316,966B | 500／502 |

比较组旧路径本来每行读取最差日／盈利日；本次观察额外日账查询恒为2，没有按实验新增完整月报调用。响应不含daily或equity曲线明细，月份长度严格等于请求数。

压力数据为500个版本、181,000条日汇总，代表已累计12个月的保留结果。为了直接构造该性能边界，临时fixture将创建后的日期与日账扩展至12个月；这不改变正式创建接口单次90天限制，也不声称执行过一整年真实回放。压力读前后全表SHA256均为：

`256d9ad0e0ad1edc562ca3e3321a72a372fd32544444dcd419597cfc0648a87b`

整体峰值RSS142,856KiB（约139.5MiB）。最大500行×12月组读取约2秒、3.32MB响应是实测上限场景成本；真实92版本路径满足<300ms目标。后续版本数和月份进一步增加时需重新衡量，不能将本证据外推为无限规模。

## 最终修复与回归

- 首次可见输入晚于配置起日且发生在当天中午时，不再把该日上午视为已覆盖。例如配置11月1日、首次观察11月14日中午，窗口末14天11月14～27日明确不可用；正常配置11月14日且首期00:05的行为保持。已增加两个独立场景，并经最终27项专项及trace执行通过。
- 比较组分批删除到没有子批次的短暂状态，旧GET和带观察GET均返回正常空组，不再因空集合max计算失败；越权仍返回404。修复使用空集合默认值与观察空载入判断，没有改删除过程或权限。
- 设置折叠摘要会显示已开启的可选限制，避免过滤原因隐形；展开更多观察信息时自动刷新暂停，关闭后恢复。
- 观察请求挂起期间登录变化、403以及取消加载均恢复正确控件状态；旧响应不能覆盖新设置。相关前端反例已完成RED→GREEN，最终真实页面程序及根代理复验均通过。

## 核心行覆盖率

环境没有安装coverage包，本次没有新增依赖。根代理使用Python标准库trace执行27项测试，再按`.cover`命中行前缀和`trace._find_executable_linenos`可执行行集合的并集重算分母。未使用trace缺少`--missing`时出现的“仅命中行作分母、摘要100%”结果。

正确计算证据为 `/tmp/pc28-observation-line-coverage.json`：

| 新核心模块 | 命中／可执行行 | 行覆盖率 | 门槛 | 未命中行 |
|---|---:|---:|---:|---|
| domain/rule_lab_observation.py | 172／177 | 97.18% | ≥80%，通过 | 47、48、49、51、59 |
| services/rule_lab_observation.py | 81／86 | 94.19% | ≥80%，通过 | 17、31、76、77、89 |

未命中主要为live起日、未开始／无前沿、续算完成日期路径及兼容simulation_time／日账兜底、无事务误调用防线。部分续算真实数据另有638记录固定副本对照；该副本程序不在本次27项trace进程中，不混入覆盖率分子。这里只宣称上述两个新增统计核心模块的**行覆盖率**，没有统计JS、全仓或分支覆盖率。

## 访问控制与输入安全

最终27项包含6项真实WSGI／SQLite API测试：匿名401、普通账号403、其他管理员读取非本人批次／组404；两参数成对与范围／枚举／重复／未知键拒绝；其他端点查询白名单保持；新旧读路径无数据库改动且无来源依赖。UI程序核验HTML名称作为文本处理、localStorage损坏与不可用、颜色白名单和输入范围；旧结果管理程序继续核验改名XSS与删除确认流程。

## Contract Matrix 验收

| 合同 | 实际证据 | 结论 |
|---|---|---|
| CM1 连续N月／缺月 | 域与QA统计＋新DOM阶段／完整／1-of-2状态 | 通过 |
| CM2 原始金额边界 | 599/20000分API；DOM2.995%与2.004pp反例 | 通过 |
| CM3 同快照只读 | QA真实并发writer＋query_only；固定副本全表哈希 | 通过 |
| CM4 共同时窗与覆盖 | 末日中午／首日中午／起月不足／组落后子方案反例 | 通过 |
| CM5 质量优先 | 域旧引擎／冲突／缺档；API与QA pending／缺日账／组不可比；DOM阻断 | 通过 |
| CM6 API严格与兼容 | 最终6项API＋旧无参数与空删除组GET | 通过 |
| CM7 偏好持久化 | DOM保存恢复／损坏与禁用localStorage | 通过 |
| CM8 展示筛选和读生命周期 | DOM两列表50／250行分页、仅匹配、乱序／503／403／登录变化、零写请求 | 通过 |
| CM9 风险敏感性诚实 | QA最佳三天／回撤天数／窗口截断；DOM更多信息与金额详情回归 | 通过 |
| CM10 有界与结果保护 | 固定副本＋100／500×12月压力；部署后线上校验由deploy-report另行记录 | 开发QA通过，线上核对属发布步骤 |

## 遗留限制与发布结论

- 最大500行×12月比较组读取约2秒、3.32MB响应是已知实测成本；真实当前92版本列表小于80ms，内存及查询增长符合本次门禁。
- 没有启动真实浏览器，因此不声称完成像素布局、截图或跨浏览器兼容性验收；本次按已约定的真实WSGI＋jsdom方式验证用户主流程。
- 固定副本／压力程序在最后首日覆盖与空删除组两处边界修复之前执行；正常生产副本及非空压力组输入不受影响，最终27项专项验证了新增边界，不重复无关联压力运行。
- 当前没有未修复的阻断缺陷；**QA通过，可进入批准代码的部署及线上只读保护校验**。本报告不代替部署成功记录。
