# 列表观察规则发布报告

## 摘要

- 发布完成：根代理放行准确提交 `fa287edbc6b85352572598de0a25ddefb056f4e9` 后，已快进生产并通过真实HTTP、页面DOM、账本保护和公网资源核验。
- 现有生产平台为本机 Python WSGI＋systemd，入口 `https://tg.dca.de5.net/rule-lab`。复用既有部署，不更换平台或新增依赖。
- 本次仅快进生产代码并重启 `pc28touzhu-platform.service`，新API PID为1629337；模拟worker PID1408564保持。无数据库迁移、实验／风控／增量计划改写。
- 用户已批准实现及正常发布。下列 ROOT GO 是团队对准确验收版本的内部执行门禁，不是额外向用户索取许可。

## 发布前证据

只读预检使用 `/tmp/pc28-observation-release.py`，实际退出 0，证据 `/tmp/pc28-observation-release-preflight.json`。HTTP 基线使用 `/tmp/pc28-observation-release-online.cjs --baseline`，实际退出 0，5次 GET、无写请求，证据 `/tmp/pc28-observation-release-http-before.json`。

| 项目 | 已核对状态 |
|---|---|
| 生产提交 | `ca1d9180a681f5e7c01e24301cd22ad6d1bce410`，main，工作树干净 |
| 开发目录 | `/www/wwwroot/pc28touzhu-lab-observation` |
| 模拟库 schema | 6 |
| 实验数据 | 28批次、638版本、29,220日汇总、3比较组、9增量系列 |
| 模拟目录大小 | 29,633,227字节（预检时点，后续可能正常变化） |
| 平台 API | active，PID 1426801 |
| 模拟 worker | active，PID 1408564，1核、MemoryMax 256MiB、禁swap、NRestarts 0 |
| 旧批次一次性队列 | `pc28touzhu-rule-lab-batch-queue.service` inactive；不为本次功能重新启动 |
| 来源同步／触发／结算 | active，PID分别1149035／1149036／1149027 |
| 实盘执行器 | `pc28touzhu-telegram-executor@executor-001.service` active，PID1408628；线上核验还会检查API心跳 |

不输出配置密钥、认证Cookie或其他账户凭证。HTTP脚本只在子进程内存中生成当前管理员会话，并限制所有调用为同源GET；不启动浏览器、不读取浏览器资料。

## 保护范围

发布脚本在真正执行前重新采样，避免使用准备时点的陈旧状态。

1. 全部批次与实验定义、冻结条件与赔率、比较组关系、增量采集／计算开关和每日时间必须保持相同指纹。
2. 已完成且不属于增量系列、也没有待结算的批次作为固定成绩范围。当前范围为批次8与15个比较子方案：共16批次、56版本、2,806日汇总；同时保护其风险日及活动轮。
3. 原有增量实验允许后台继续计算，逐批检查已处理期数不倒退，不把自然增加的日汇总或收益错误判为发布改写。
4. worker、一次性队列、来源同步、自动触发、自动结算的运行状态、PID和资源限制必须保持；执行器允许既有自动重连，但必须在线且有新心跳。
5. 数据库schema必须仍为6。无需复制整库备份或暂停数据生产；既有QA一致副本不由发布脚本修改。

## 准备脚本与门禁

- `/tmp/pc28-observation-release.py`：默认只读预检；`--apply`必须同时提供40位已验收提交号和内部ROOT GO。确认开发HEAD完全匹配且干净，生产main干净，目标可快进，改动路径属于本功能白名单，无删除文件。白名单包含根代理额外批准的 `rule_lab_series.py` 空子批次比较组只读边界修复（`max(..., default='')`），不涉及状态迁移。捕获最新保护指纹及HTTP基线后快进，校正本次运行文件可读权限，仅重启平台API，再核对账本／服务保护。成功状态暂记 `deployed_pending_http_verification`，不能据此宣称页面验收完成。
- `/tmp/pc28-observation-release-online.cjs`：默认验证正式部署后的真实HTTP与页面JS，`--baseline`只抓取旧接口基线。生产验证覆盖2／3／6月和完整月口径、原始字段不变、月度原始分值与加权ROI、连续月份占位、批次／比较组共享窗口、页面设置、本地阈值／颜色／筛选不发写请求、详情金额换算、执行器在线。

准备脚本已通过 `python3 -m py_compile` 与 `node --check`。页面IDs已与最终实现核对，发布核验脚本还修正了允许加载中再次应用规则时的等待条件，以及列表行和详情按钮同有实验ID时的按钮选择器。随后使用固定生产副本的临时WSGI及当前开发HTML／JS实际执行完整核验：8个观察响应、36次GET、0写请求、0脚本错误，2／3／6月、完整月、阈值／颜色／筛选和金额换算均通过。证据为 `/tmp/pc28-observation-release-fixture.log` 与 `/tmp/pc28-observation-release-fixture-online.json`。该临时核验明确跳过生产执行器心跳；正式模式仍检查。随后已在正式生产环境再次执行并通过，结果见下方。

根代理验收后使用：

```bash
python3 /tmp/pc28-observation-release.py --apply --accepted-commit <已验收40位提交> --root-go ROOT_APPROVED_OBSERVATION_RELEASE
node /tmp/pc28-observation-release-online.cjs
```

公网资源额外以普通浏览器User-Agent执行GET，与生产JS／CSS字节校验；不使用默认urllib User-Agent的403响应推断页面故障。

## 回退与监控

若部署后新查询或页面验证失败，先保留执行记录并交根代理判断；不自动重置数据库或暂停实验。代码回退使用对应功能提交的 `git revert` 后仅重启平台API，schema无变化，故不回退任何账本文件。执行前必须核对生产提交与工作树，避免覆盖同时发生的其他修改。

沿用现有服务心跳和systemd监控，不新增定时任务、通知或外部服务。发布完成需核对HTTP 200、无页面脚本错误、只读行为、固定成绩指纹、增量进度、worker资源及执行器在线。

## 正式发布与验证结果

根代理确认全量运行1284项，其中1282通过、2项可选跳过，新增目标27项与3套真实WSGI／DOM流程通过后，放行上述准确提交。实际发布命令退出0：

```bash
python3 /tmp/pc28-observation-release.py --apply --accepted-commit fa287edbc6b85352572598de0a25ddefb056f4e9 --root-go ROOT_APPROVED_OBSERVATION_RELEASE
node /tmp/pc28-observation-release-online.cjs
```

- 发布记录：`/tmp/pc28-observation-release-rollout-20261128T045328Z.json`，最终状态 `verified`；命令日志 `/tmp/pc28-observation-release-apply.log`。
- 线上核验：`/tmp/pc28-observation-release-online.json`、`/tmp/pc28-observation-release-online.log`。8个新增观察响应全部通过，整个流程37次同源GET、0写请求、0脚本错误；2／3／6月及完整月、局部阈值／颜色／仅匹配、恢复默认、详情金额估算均使用真实生产HTML和JS执行。
- 公网JS／CSS均返回200且与生产文件逐字节相同；证据 `/tmp/pc28-observation-release-public-assets.json`。JS SHA256为 `bcf30e115eace99dd3180a1ed1a5947b2ad5021ea3f7e6185ad3e9be13fd341b`，CSS为 `d6d5dcb5ab9590b6e6b1ec43ec24ddbe5fe6588c82fe397d25e2c6175d25913f`。
- 全部638个版本、28批次、3比较组、9增量系列的身份及配置指纹保持，schema仍为6；固定16批次／56版本／2,806日汇总以及风险日、活动轮指纹完全相同。全库日汇总当前仍为29,220行；原有增量进度无倒退。
- API PID1629337；worker PID1408564、1核／256MiB／禁swap保持；来源同步、自动触发、结算与旧一次性队列的状态、PID和资源均保持。执行器PID1408628仍在线，采样心跳年龄2秒。
- 此轮采样新增观察响应最大37.98ms（批次8和比较组2的2／3／6月及完整月请求），只是本次检查的端到端样本，不当作通用性能保证；大型响应基准见独立QA报告。

正式首轮核验曾因发布脚本将 `no-store` 断言错误套到已有 `/api/platform/admin/executors` 路由而退出。已通过独立GET确认该旧路由HTTP200但无Cache-Control，而新增rule-lab观察接口HTTP200且有 `no-store`；将核验断言限定在本功能路由后完整重跑通过。首轮日志保留 `/tmp/pc28-observation-release-online-first.log`，未因此修改应用运行代码或弱化新接口检查。

本次页面验证为HTTP＋jsdom，不启动浏览器或用户配置；不声称像素布局或截图验收。根代理负责最终文档提交与推送，DevOps未自行提交或推送。
