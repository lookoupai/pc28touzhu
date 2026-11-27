# 速度切换发布与实测

2026-11-27，功能`8334324`已合并生产main并推送GitHub。上线范围包括两档CPU切换、旧回放预算边界修复/旧成绩提示，以及先前完成的组合7～30专用模板。没有DB结构迁移、外部源写入、新模拟任务或投注发送。

## 备份与成绩

确认study1全部5个child完成，各22861期，无running批次、无pending决策。停止空闲lab后用SQLite backup一致备份：`/var/backups/pc28touzhu/releases/rule-lab-speed-20261127-021737/rule_lab-before-speed.db`，242507776字节、0600、quick_check=ok。

上线后对备份与当前库逐表逐行sha256核对，排除正常变化的worker心跳，10张表完全一致：6批次、468实验、22961开奖、22960预测、764001决策、18550每日汇总、18550日风险、38416轮次、1比较组、5子组关系。`/tmp/pc28-performance-ledger-audit.json`。旧成绩没有覆盖或自动重跑。

旧按轮成绩因为未使用修复后的调度标记，被只读返回review_required；study仍completed、5/5，但不再显示delta。该标记是存在已知算法风险，不是宣称所有历史结算错误。详情可查看原成绩及明确提示。

## 安装与服务

root拥有0755的`/usr/local/libexec/pc28-rule-lab-resource-mode`；sudoers0440的`/etc/sudoers.d/pc28-rule-lab-resource`只允许www调用economy/fast两个固定命令，visudo通过。父目录root0755；helper不读取项目或.env，固定systemd unit与CPUQuota属性。原unit隔离、MemoryMax256MiB、swap0不变。

部署主动重载API835874→1017619，重启lab719342→1017623以加载预算修复；这是本次代码发布需要。旧执行器因API短暂重载自动恢复835899→1017652/NRestarts10→11，02:21:53 UTC核验online、last_seen02:21:51Z。其余6个常驻worker PID未变，一次性maintenance原inactive保持。前后证据`/tmp/pc28-performance-release-before.json`和`/tmp/pc28-performance-release-after.json`。

## 真实切换验证

真实管理员浏览器只允许GET及performance路径的POST，实际发送2次POST：economy→fast→economy。worker PID始终1017623、NRestarts0；MemoryMax268435456；cpu.max分别100000/100000与20000/100000。加速时systemctl cat确认持久化100%，最后`/etc/systemd/system.control/pc28touzhu-rule-lab.service.d/50-CPUQuota.conf`为20%，刷新页面仍省资源。未实际重启服务器验证，但持久化路径与systemd机制已核验。

旧5组完成状态、复核提示、新组合模板26版本均正常。390px无整页横向溢出，无JS错误/额外写请求。证据`/tmp/pc28-performance-online.json`，桌面/手机截图`/tmp/pc28-performance-fast-desktop.png`、`/tmp/pc28-performance-fast-mobile.png`已目视检查。凭证仅进程内存使用。公网HTTPS健康ok，公网JS与生产文件一致。

**当前保留省资源0.2核。** 速度切换无需重启/重新计算。180期78版本fixture真实节流实测55.519→10.877秒，结果相同；详见QA与benchmark.json，不将此样本当完整两个月运行时间承诺。

增量续跑尚未完成：本次仅携带只读预检代码，没有新增续跑入口、延长区间、归档或清理保护。后续工作须考虑旧版回放审计标记，不能将旧结果直接作为已验证续接基线。

## 回退

需要回退速度UI时先固定helper切回economy，保留账本和备份，再按目标代码版本回退API/UI。移除sudo授权前先将持久化CPU设回20%；不要systemctl revert全部unit配置。预算边界修复应保留，避免旧回放问题复现。
