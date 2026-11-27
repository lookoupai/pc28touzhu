# 实验增量续算发布记录

## 摘要

- **发布完成并通过线上核验。** 代码提交 `b563e760d75d8658f3e5452298c6702a70008ddf`，实验库已升级到 schema 4。
- 原 schema 3 的 10 张业务表、103,786 行全部原列逐行指纹一致；批次 8 的 26 个实验、22,914 期、原区间与成绩保持。
- 线上续算预览可用，下一期 3511344；采集及自动续算均默认关闭，series 和 segment 数量均为 0。
- API、实验 worker 和实际执行器均正常；实验 worker 保持 1 核／256 MiB，上游预测进程未变化。
- 地址：<https://tg.dca.de5.net/rule-lab>。桌面及 390px 手机已进行真实认证 GET 浏览器检查，全程 0 POST。

## 发布前门禁

根代理放行时提供：完整回归 1,158 项通过（328.548 秒）、最终公平调度改动后的实验室 238 项通过（116.141 秒），原流程、金额重测、增量续算三条浏览器流程通过。独立 QA 已完成生产一致副本迁移、真实追加、资源及历史结果保护验收；详见 `qa-report.md`。

本次复用既有 systemd、Python WSGI 和 SQLite WAL 部署，没有依赖安装或服务配置变更。

## 执行过程及恢复

2026-11-27 12:59 UTC，根代理明确放行精确提交 `b563e76`。发布脚本仅停止 API 和实验 worker，对独立实验库执行预算目录外的一致备份，快进代码并修正变更运行文件读取权限。

第一次执行在调用迁移进程时因 PATH 不包含 `/usr/sbin`，未找到 `runuser`，脚本按失败恢复流程重新启动 API 和 worker。改用 `/usr/sbin/runuser` 后重跑；新服务初始化已完成加法 schema 4，重复迁移保持幂等。第二次执行的迁移、原列指纹和启动均通过，随后的即时健康检查正好采样到实际执行器因 API 短暂重启而自动恢复的间隔，因此脚本记录该健康断言失败。执行器随后自行恢复，没有额外重启或数据回退。

两次尝试的失败记录保留原样，没有改写为成功。最终独立后置检查重新比对第一次 schema 3 备份与当前生产库，并验证服务、HTTP 和桌面／手机界面，结果通过。发布脚本已补上最多 40 秒的健康恢复检查，未再次重启生产服务来重跑脚本。

## 备份与数据核验

- 原始 schema 3 一致备份：`/tmp/pc28-rule-lab-before-continuation-20261127T125906Z.db`，0600，位于模拟目录预算之外。
- 幂等重试前备份：`/tmp/pc28-rule-lab-before-continuation-20261127T125955Z.db`，0600。
- 最终后置证据：`/tmp/pc28-continuation-release-verified.json`。
- 10 张原业务表、103,786 行原列逐行 SHA256 保持；排除会正常更新心跳的 `lab_runtime`。
- `PRAGMA quick_check=ok`，外键检查 0 异常，schema 4，`lab_series=0`、`lab_segments=0`。
- 批次 8：状态 completed、26 个实验（495～520）、已处理 22,914 期、实际末期 3511343、下一期 3511344、待结算 0。
- HTTP 读取的经济字段摘要与发布前相同，未创建续算配置、追加请求或新实验。

## 服务与线上结果

| 检查 | 实际结果 |
|---|---|
| API | active/running，PID 1284641 |
| 实验 worker | active/running，PID 1284647；CPUQuotaPerSecUSec=1s；MemoryMax=268435456 |
| 实际执行器 | `pc28touzhu-telegram-executor@executor-001.service` active/running，PID 1284661；在线接口心跳间隔 0 秒 |
| source-sync | active，原 PID 1149035 保持 |
| auto-trigger | active，原 PID 1149036 保持 |
| pc28-auto-settlement | active，原 PID 1149027 保持 |
| 上游预测应用 | 原 PID 1250128 保持 |
| 新续算接口 | preview 返回 eligible=true、next_issue=3511344、26 个版本，no-store |
| 默认状态 | enabled=false、capture_enabled=false、compute_enabled=false；历史段为空 |
| 真实浏览器 | 桌面 1440×1000、手机 390×844；续算面板可读，0 页面错误、0 POST、0 外部请求 |
| 公网静态文件 | `https://tg.dca.de5.net/assets/rule_lab.js` HTTP 200，包含新续算入口 |

本地一次预览请求测得 18 ms；这只是单次线上检查，不宣称 P99 或长期运行性能。

## 可复核执行记录

| 命令／检查 | 退出码 | 结果 |
|---|---:|---|
| `python3 /tmp/pc28-continuation-rollout.py` | 0 | 只读预检通过，生产原为干净 main `46c392f`、schema 3 |
| `python3 -m py_compile /tmp/pc28-continuation-rollout.py` | 0 | 脚本语法检查通过 |
| `node --check /tmp/pc28-continuation-online.cjs` | 0 | 浏览器检查脚本语法通过 |
| `node /tmp/pc28-continuation-online.cjs --baseline` | 0 | 旧版线上 GET 基线通过 |
| 第一次 `python3 /tmp/pc28-continuation-rollout.py --target b563e76 --apply` | 1 | `runuser` PATH 问题，服务已恢复；日志 `...rollout-execution.log` |
| 修正路径后重跑同一命令 | 1 | 迁移和旧列核验通过，即时健康断言遇执行器自动恢复；日志 `...rollout-execution-retry.log` |
| 独立后置原 schema 3 指纹／schema 4／服务核验 | 0 | 10 表 103,786 行一致，quick_check/FK 正常，全部服务 active |
| `node /tmp/pc28-continuation-online.cjs` | 0 | 真实 GET 接口和桌面／手机检查通过，经济摘要一致 |
| 公网 JS GET 及新入口检查 | 0 | HTTP 200，新代码已可获取 |

线上检查证据 `/tmp/pc28-continuation-online.json`，日志 `/tmp/pc28-continuation-online.log`，截图 `/tmp/pc28-continuation-online-desktop.png` 和 `/tmp/pc28-continuation-online-mobile.png`。认证信息只在进程内存使用，证据不包含 Cookie、密码或密钥。

## 恢复边界

schema 4 不能直接交由只接受 schema 3 的旧程序运行。故障时优先保留新库，关闭新功能并使用兼容 schema 4 的修复版本；若发布后已经产生新段或新成绩，不得直接用旧备份覆盖。

本次未备份、迁移或写入上游大库，未直接重启其他业务服务，未开启真实批次的后续采集或自动续算，没有发送消息或投注。实际执行器因 API 短暂重启自行恢复，已在后置检查核验在线。
