# 常规回测报告修正发布

2026-11-26功能`a6d5fd8`已合并生产main并推送GitHub feature/main。无DB迁移，无新依赖，无上游或任务参数变更。

精确更新月度domain、HTML/JS/CSS文件权限0644，只主动重启平台API加载日结曲线读取。worker保持PID719342/NRestarts0，第二组16381→17173期继续推进；第一组完成22861期不变。平台PID815929→835874；原执行器因API短暂停止自动重启815957→835899/NRestarts9→10，于16:51:56 UTC核验online、心跳同秒。其余6个常驻worker PID不变，一次性维护服务原inactive保持。未发送投注或创建任务。

证据`/tmp/pc28-report-release-before.json`、`/tmp/pc28-report-release-after.json`。公网HTTPS健康ok，新JS与生产文件字节一致，Git main与origin/main为0/0且干净。

真实管理员浏览器仅本机GET：旧倍数/容忍度/反推本金文案已不存在；初始资金默认空；实验79输入1000后显示期末1009.40、累计收益率0.94%、日结最大回撤率10.57%、最低日终946.20，同时逐笔最大回撤金额129.80。输入1触发资金归零提醒并不显示收益率/回撤率。月度CSV金额保持，移动端390px无整页溢出，无JS错误，无写请求。

证据`/tmp/pc28-report-online.json`及`/tmp/pc28-report-online-desktop.png`、`/tmp/pc28-report-online-mobile.png`。凭证仅进程内存使用，未写入产物。用户具体实验122的逐笔/可用余额复核见qa-report.md，不将日结图冒充逐笔现金曲线。

全项目983项与浏览器验收见qa-report.md。回退仅需还原本次代码并重载API，独立账本无须恢复或覆盖。
