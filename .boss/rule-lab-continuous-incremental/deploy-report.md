# 实验增量续算发布记录

## 摘要

- **状态：发布预检完成，尚未部署。** 等待根代理完成集成测试、全量回归及正式 QA 后放行。
- 复用现有 systemd 与 Python WSGI 部署，不安装依赖、不修改服务配置。
- 生产当前为 `46c392f`、实验库 schema 3；批次 8 保持 26 个版本、22,914 期，下一期为 3511344。
- 发布脚本与线上 GET 检查脚本已准备；本阶段只运行只读预检和旧版 HTTP 基线检查。

## 预检证据

日期：2026-11-27。开发目录 `/www/wwwroot/pc28touzhu-lab-continuation`，生产目录 `/www/wwwroot/pc28touzhu`。

| 检查 | 实际结果 |
|---|---|
| 生产 Git | main `46c392f`，工作区干净 |
| 实验账本 | 23,416,832 字节，www:www、0600；schema 3；待结算 0 |
| 旧批次 | 批次 8 已完成；26 个版本；区间 2026-10-01～2026-11-26；实际末期 3511343 |
| API | active，预检 PID 1226263 |
| 实验 worker | active，预检 PID 1226270；CPUQuotaPerSecUSec=1s；MemoryMax=268435456 |
| 实际执行器 | `pc28touzhu-telegram-executor@executor-001.service` active；HTTP 查询有 1 个在线执行器，心跳间隔 1 秒 |
| 其他业务服务 | source-sync、auto-trigger、pc28-auto-settlement 均 active |
| 上游预测应用 | 原进程 PID 1250128 存在，预检未改变 |
| 原经济报告 | 通过认证 GET 保存 26 个实验 ID 与经济字段摘要，供发布后比对 |

执行记录：

| 命令 | 退出码 | 说明 |
|---|---:|---|
| `python3 /tmp/pc28-continuation-rollout.py` | 0 | 默认只读预检，证据 `/tmp/pc28-continuation-release-preflight.json` |
| `python3 -m py_compile /tmp/pc28-continuation-rollout.py` | 0 | 发布脚本语法检查 |
| `node --check /tmp/pc28-continuation-online.cjs` | 0 | 浏览器检查脚本语法检查 |
| `node /tmp/pc28-continuation-online.cjs --baseline` | 0 | 旧版线上 GET 基线，证据 `/tmp/pc28-continuation-http-before.json`；未发送 POST |

认证信息只在检查进程内存中使用，证据文件不包含 Cookie、密码或密钥。脚本与证据保存在本机受限权限的 `/tmp` 文件中。

## 待放行的发布步骤

根代理先完成测试并提交最终代码，取得明确提交 SHA。以下脚本已准备，**本阶段未执行 `--apply`**：

```bash
python3 /tmp/pc28-continuation-rollout.py --target <已验收提交SHA> --apply
node /tmp/pc28-continuation-online.cjs
```

发布脚本先核对生产干净、目标可快进、旧批次仍已完成且未启用续算。随后短暂停止实验 worker 和 API，避免旧 API 在 schema 切换期间初始化 v3；其他业务服务保持运行。

只对独立实验库执行 SQLite 一致备份，备份放 `/tmp/pc28-rule-lab-before-continuation-<UTC时间>.db`，位于 1 GiB 模拟目录预算之外。快进代码后修正变更运行文件的读取权限，以 www 用户执行加法迁移，再逐行核对所有原表原列（排除正常 heartbeat 的 lab_runtime）。验收包括 schema 4、quick_check、外键检查及 `lab_series=0`，之后启动 API 和实验 worker，并核对 1 核、256 MiB 及上游进程未变化。

线上检查仅允许本地 API 的 GET：原 26 个 ID/经济报告不变、续算预览可用且下一期正确、采集与自动续算均默认关闭、历史段为空；桌面和 390px 手机可打开续算面板，页面无错误或横向溢出。检查还读取实际执行器心跳与实验 worker 资源限制。

不会备份、迁移或写入上游大库，不创建续算 series，不开启自动任务，不发送消息或投注。

## 恢复边界

schema 4 发布后不能直接换回只接受 schema 3 的旧代码。故障时优先保留新库，关闭新功能并使用兼容 schema 4 的修复版本。发布脚本失败会尝试恢复 API 与实验 worker 的运行，记录失败阶段，但不自动覆盖实验数据库。

如果发布后已产生新段或新成绩，不得把旧备份直接覆盖回去。任何数据恢复都须先评估已新增记录，避免丢失用户结果。

## 正式发布结果

待根代理放行及实际上线核验后补充提交号、备份路径、服务 PID、原数据指纹结果和 GET/桌面/手机证据。
