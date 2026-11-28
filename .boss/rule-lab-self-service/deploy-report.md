# 自助实验室发布记录

发布目标：现有 Python WSGI/systemd 平台，入口 https://tg.dca.de5.net/rule-lab 。已于2026-11-28 14:30 UTC完成切换，运行代码 `b91ebfc3f72f6f856c87e1d2ae4a1e4d04e9fd5e`、schema7。开发实现及门禁见 qa-report.md、backend-evidence.md、frontend-evidence.md。

## 已准备的发布流程

外置发布工具目录 `/tmp/pc28-self-service-release/`，`release.py` 默认只读，正式执行需要验收的完整提交ID和机器可读验证收据。

1. 核对生产与开发工作树、服务、有限清单及真实规则/订阅指纹。
2. 停止有限清单启动器、实验worker与平台API，取得最终清单和一致数据库备份到 `/var/backups/pc28touzhu/releases/`。
3. 代码快进，按既有www用户设置运行文件读取权限；重复初始化schema7，逐表核对全部旧列、水位不变。
4. 导入jnd28的18个原任务，仅新增队列/移交元数据；保留ID、赔率、条件、政策、统计和游标。
5. 核对完整性及真实业务定义不变后恢复API和唯一worker；旧有限启动器保持停止。其他同步、触发、结算与执行服务不参与本次切换。
6. 真实线上GET-only桌面/手机检查及进度检查。

失败路径在已开始切换后明确停止实验相关写者，记录原因并由root前向修复；不自动覆盖旧库，不用早期副本替换已有新成绩。此编排已用本地mock测试覆盖API重启后校验失败情形。

## 已完成证据

- 生产切换前只读检查：54批次、1806实验版本、9既有续算计划；worker仍为1核/256MiB，jnd28不启用自动续算。
- schema6外置副本两次初始化schema7，旧表旧列全部保持、完整性通过。
- 真实导入CLI副本4阶段：只读预览、旧controller运行时拒绝、首次导入、重复导入；18成员保持，副本状态2完成/2运行/14等待，未新增series。
- 发布工具5项安全测试通过。正式发布后补充提交、备份、只读验证和进度结果。

## 实际发布与线上核对

- 主功能提交 `a10e94ea88416740d6d72950e2384dadae2850f6`；现场补充提交 `b91ebfc3f72f6f856c87e1d2ae4a1e4d04e9fd5e` 仅修复停止的临时systemd单元被回收后的识别。
- 14:28:30 UTC开始切换，14:30:38 UTC恢复完成。停止后的临时启动器显示not-found/inactive/PID0，首次导入保护检查拒绝，旧列校验已通过且未导入半组。新增真实边界红测后修复，15项迁移/队列回归通过，并再次在生产外置副本跑4阶段导入，然后前向恢复；未还原数据库。
- 备份：`/var/backups/pc28touzhu/releases/self-service-20261128T142830Z/rule_lab-v6.db`，保留停写时的一致schema6数据库。
- 发布过程：`/tmp/pc28-self-service-release/20261128T142830Z/rollout.json`；精确补充验收：`acceptance-transient-fix.json`。
- 正式18成员交接：8完成、2运行、8排队。全部旧表旧列、水位、条件/赔率快照、统计及游标在迁移/导入阶段原样保持；真实订阅/规则/路由定义不变。
- 平台与worker均active、NRestarts=0；worker保持1核/256MiB，旧有限启动器inactive；原同步/触发/结算服务PID和资源设置不变。
- 线上认证后health、页面、options、templates、queue均200；匿名新API返回401，公网入口维持登录保护。
- 线上1440×1000、390×844 Chromium实际页面通过：模板库4个内置项、进行中队列10条、无横向溢出、无JS异常；仅42次GET，0写请求和0外部请求。截图 `online-1440.png`、`online-390.png` 位于外置发布目录。
- 原54批次与1806版本均存在；已完成非续算实验成绩保持；9个既有续算控制完全不变，jnd28未新增series。原运行批次67/68均在恢复后推进；14:31:34 UTC再次读取worker为running、无错误，已处理分别4484/3828期。
- 最初journal出现一次来源临时读取错误，后续worker自动恢复并继续推进，未造成实验错误或进度回退。

STATUS: DONE
REASON: 功能、最终回归、生产副本、正式迁移及线上只读页面/进度核对均完成。
