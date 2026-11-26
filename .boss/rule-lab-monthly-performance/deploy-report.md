# 月度成绩发布

2026-11-26，功能版本`f18dd68`已部署生产`/www/wwwroot/pc28touzhu`并推送GitHub feature/main。后续手机图形字号调整仅改变JS/CSS，浏览器完整流程再次通过，日志`/tmp/pc28-monthly-ui-final.log`。

无数据库迁移、无新依赖、无上游改动、无模拟配置或成绩重置。源码精确赋0644供www读取。仅主动重启平台API加载新的详情读取逻辑；worker仍PID719342、NRestarts0，第二组从6207推进到6645期，第一组保持完成22861期，其余队列保持。资源限额未改。

平台PID719336→815929；执行器因API短暂重载自动退出并恢复，PID719374→815957、NRestarts8→9。15:50:52 UTC核验online、last_seen15:50:51Z；其他6个常驻worker PID未变。payload-maintenance是原本inactive的一次性服务，无需启动。对比证据`/tmp/pc28-monthly-release-before.json`及`/tmp/pc28-monthly-release-after.json`。本次未人为发送投注、创建真实任务或模拟批次。

公网HTTPS健康ok，公网JS与生产文件一致。真实管理员浏览器只允许本机GET，验证实验79：10月-35.20、11月+44.60；逐笔回撤129.80、默认压力倍数2及20%比例换算1298.00；改比例10%换算2596.00，填本金1000显示压力回撤25.96%并提示超过容忍度。CSV金额正确，0写请求/0外部请求/0JS错误，390px手机无整页横向溢出。详情统计与当前策略的任何收益均不构成保证。

线上证据：`/tmp/pc28-monthly-online.json`、`/tmp/pc28-monthly-online-desktop.png`、`/tmp/pc28-monthly-online-mobile.png`、`/tmp/pc28-monthly-capital-mobile.png`，会话凭证仅进程内存使用，不写入这些产物。用户需刷新整张页面加载新的月度入口。

回退可还原本次代码并重载平台；独立实验账本无需恢复或覆盖，worker可继续现有任务。
