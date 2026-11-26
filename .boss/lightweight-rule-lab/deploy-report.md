# 轻量规则实验室部署准备

## 摘要

- **状态：待发布。** 运维配置正在验证，尚未安装或启动新生产服务、未重启平台、未修改生产数据库。
- 复用现有 systemd、Python launcher 和 `tg.dca.de5.net` 反向代理；入口将为 `https://tg.dca.de5.net/rule-lab`。
- 新 worker 仅写独立模拟目录，读取源库，禁网络、不加载业务凭证；CPU 0.2 核、内存硬限 256MiB、禁用该服务 swap。
- 发布前必须等待完整 QA 和主代理放行；发布时只重启平台 API，新模拟库从空配置启动，实验由页面保存为暂停。

## 发布前只读勘察

2026-11-26 实际观测：

| 检查 | 结果 |
| --- | --- |
| 生产代码 | `/www/wwwroot/pc28touzhu`，`main` 为 `1209e3b14fe8163b7f490fcaa67a0ddda83b80bd`，当时工作区干净 |
| 平台服务 | `pc28touzhu-platform.service` active/running，用户组 `www:www`，工作目录为生产目录，监听 35100 |
| 既有入口 | Nginx `tg.dca.de5.net` 将 `/` 反代到 `127.0.0.1:35100`；无需新增代理配置 |
| 健康检查 | 本机 `http://127.0.0.1:35100/api/health` 返回 `{"status":"ok"}`；HTTP 与 HTTPS 公网健康地址均返回 200 |
| 用户权限 | `www` 成功以 SQLite `mode=ro` 查询源库；可在现有 `data` 目录创建新目录 |
| 来源 | `/www/wwwroot/AITradingSimulator/pc28_predictor.db`，WAL 模式；部署后需在只读沙箱内复核读取 |
| 资源控制 | systemd 255，cgroup v2；支持 CPUQuota、MemoryHigh/Max 和 MemorySwapMax |

上述检查只证明勘察时状态，不是新版本发布证据。源文件在开发工作树可能为 root/0600；发布前按本次 Git 变更清单精确核对服务用户读取权限，禁止递归改项目敏感文件权限。

## 运维验证证据

命令在开发工作树 `/www/wwwroot/pc28touzhu-rule-lab` 执行，运行数据均为自动清理的临时目录。

| 检查 | 命令 | 退出码 | 通过／失败／跳过 | 证据 |
| --- | --- | ---: | --- | --- |
| RED | `PYTHONPATH=src python3 -m unittest tests.test_rule_lab_ops -q`（unit尚未创建） | 1 | 0／4／0 | `/tmp/pc28-rule-lab-ops-red.log`；四项均因所需unit不存在而失败 |
| GREEN | `PYTHONPATH=src python3 -m unittest tests.test_rule_lab_ops -q` | 0 | 4／0／0 | `/tmp/pc28-rule-lab-ops-green.log`；资源/访问策略、真实launcher空库初始化、重复实例拒绝 |
| unit语法/路径 | `systemd-analyze verify deploy/systemd/pc28touzhu-rule-lab.service` | 0 | 1／0／0 | 无错误输出；没有安装或启动生产unit |
| diff | `git diff --check` | 0 | 1／0／0 | 无错误输出 |

单步检查仅验证临时目录内空实验服务能初始化并写入心跳，不把它当作真实源沙箱权限、持续内存或生产吞吐验证。实际只读沙箱访问与cgroup有效上限在发布时再检查。

## 产物与部署顺序

- `deploy/systemd/pc28touzhu-rule-lab.service`：可重复安装的资源与访问限制配置，不增加 timer。
- `docs/RULE_LAB.md`：用户操作、数据保留、资源边界、安装与回退步骤。
- `tests/test_rule_lab_ops.py`：资源策略及真实 launcher 在临时目录的初始化/单实例检查。

发布顺序：完整 QA 通过 → 主代理提交、合并已验证代码并备份 GitHub → 精确检查新代码/静态文件权限 → 建立 `www` 独立模拟目录 → 安装/校验 unit → 启动新 worker → 仅重启平台 API → 读取真实服务限制、空库心跳、来源解析与权限响应 → 补充本报告。Git 提交、合并和推送由主代理统一执行，DevOps不并发操作Git历史。

不安装新依赖，不更改上游清理策略，不执行主库迁移，不创建真实投注，不发送测试消息。生产代码提交、服务 PID、最终健康和资源指标将在实际发布后记录；当前不能声称已上线。
