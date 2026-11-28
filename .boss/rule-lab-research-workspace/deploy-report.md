# 研究工作区部署记录

## 摘要

研究工作区已发布至现有平台 https://tg.dca.de5.net/rule-lab 。运行提交 `0cd596c911a16effdaa014c44718a07eabde4169`，实验库schema8。2026-11-28 15:29:23～15:29:33 UTC完成切换，迁移与线上只读核对通过。功能提交已推送GitHub origin/main（79376e7→0cd596c，远端已同步）；最终发布记录的文档提交由root随后推送。

## 已执行发布

复用Python WSGI/systemd，不新增依赖、端口、DNS、后台计划或服务。仅停止平台API与实验worker，取得一致schema7数据库备份后代码快进并按www运行用户核对读取权限；重复初始化schema8，旧表旧列与既有ID水位完全一致，再恢复两服务。没有重新导入一期已交接的有限清单，也没有操作用户的批量删改/续算功能。

- 发布收据：`/tmp/pc28-research-release/20261128T152923Z/rollout.json`，状态`deployed_verified`。
- 发布前一致备份：`/var/backups/pc28touzhu/releases/research-workspace-20261128T152923Z/rule_lab-v7.db`。
- 精确提交验收：`/tmp/pc28-research-release/`下的迁移、Python、覆盖率、性能和acceptance证据。
- 平台PID1944775、worker PID1944799，均active，NRestarts=0。worker保持1核/256MiB；其他既有服务PID与资源配置保持。

## 数据保持与进度

上线只读核对文件为`/tmp/pc28-research-release/online-verification.json`：

- 原54批次、1806实验版本仍存在；原条件、赔率等冻结定义保持。
- 无续算的已完成实验稳定成绩保持；9个既有series控制完全一致。
- 真实跟单订阅、触发规则、路由等业务定义指纹保持。
- jnd28原18任务当时为12完成、5等待、1运行；原运行批次72恢复后继续增加处理期数，没有进度回退。
- 新接口仍拒绝匿名访问（401）。健康、页面及原options/templates/queue检查均200。

## 真实线上页面

`/tmp/pc28-research-release/online-ui.json`记录临时独立Chromium上下文的1440×1000、390×844两种视口：

- 四个研究标签与原模板库均可操作读取；9个来源选项、10个排名结果行、34个管理owner、4个内置模板均来自实际API。
- 来源9、2026-11-26质量检查按3页顺序读取，完成402来源期，实际边界已验证，覆盖率100%、确认缺失率0%；这是当前保留区间的证据，不伪造旧回放证明。
- 两种视口无横向溢出，0 JavaScript异常，0外部请求，0变更请求。
- 只发送GET及明确只读的rankings/data-quality POST；没有创建生产收藏、没有提交bulk改名/删除/追加。
- 实际截图：`online-rank-1440.png`、`online-quality-1440.png`、`online-rank-390.png`、`online-quality-390.png`，位于外置发布目录。

## 验证边界

1371项完整Python发现集（1369通过、2既有环境skip）及最终依赖改名专项、真实DOM/浏览器、生产规模只读性能与迁移副本门禁详见qa-report.md。schema8为加法迁移，发布不还原早期副本覆盖新成绩。

本次交付四项确定后续功能：同口径跨来源排名、候选收藏、原子批量管理、当前源数据质量预检。任意脚本条件、AND/OR、资金约束模拟和新结算赔率未列入已实现范围。

STATUS: DONE
REASON: 功能已上线，迁移、旧数据保持、真实桌面/手机页面及原任务继续推进均有实际证据。
