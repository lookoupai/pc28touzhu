# 布局调整发布准备

- 状态：只读准备完成；尚未更新线上，等待根代理验收准确提交。
- 当前线上：main `601bfab6b8a9ff7ca2644eb692588b24adf8bbcb`，工作树干净，入口 `https://tg.dca.de5.net/rule-lab`。
- 发布范围：仅 `rule_lab.html`、`rule_lab.js`、`rule_lab.css` 运行文件及配套文档／测试；不修改Python、API、schema、实验或调度。快进后仅重启 `pc28touzhu-platform.service`。

## 已执行预检

`/tmp/pc28-layout-release.py` 默认只读，实际退出0；预检证据 `/tmp/pc28-layout-release-preflight.json`。线上638版本、28批次、29,220日汇总、3比较组、9增量系列，schema6。API PID1629337；模拟worker PID1408564，1核／256MiB／禁swap；来源、自动触发、结算与执行器正常，旧一次性队列保持inactive。

保护范围复用上一轮已验证规则：全部定义、冻结条件赔率、比较关系及增量控制必须相同；16个非增量完成批次的56版本／2,806日汇总及风险状态完全保持；原增量实验允许自然增长，但进度不得倒退。两次只读采样的保护自检已通过。

`/tmp/pc28-layout-release-online.cjs --baseline` 实际13次GET通过，保存旧成绩与2／3／6月、完整月的8个观察响应指纹。认证仅在内存，所有请求限制为同源GET。证据 `/tmp/pc28-layout-release-http-before.json`。

## 发布门禁与待执行验证

脚本 `/tmp/pc28-layout-release.py --apply` 必须同时给出40位已验收提交号与 `ROOT_APPROVED_LAYOUT_RELEASE`；要求开发HEAD准确且干净、生产main干净、可快进、只含UI运行白名单和本次文档／测试、无删除。正式执行前重新采样并刷新HTTP基线，更新后核对账本与服务，先标记待HTTP核验。

新版页面验证已按冻结DOM契约准备：默认仅一张列表、概览五列且来自同一观察窗口，三种视图切换零GET，原长列表不可见，新建／系统／设置／详情抽屉、ESC／遮罩／焦点恢复，比较组独立详情不切换底层实验选择，金额换算使用当前行的风控与成绩。真实页面JS用jsdom运行，不启动浏览器，不读取profile；不以此声称像素布局验收。最后检查执行器心跳和公网JS／CSS200字节一致。

Python脚本编译、Node语法检查、预检与旧HTTP基线均通过。实现落地后，用固定生产副本的临时WSGI和当前开发HTML／JS实际演练发布脚本：638版本、39次GET、0写请求、0脚本错误，五列口径、三个本地视图、抽屉焦点、观察规则及比较组独立详情金额换算通过。日志 `/tmp/pc28-layout-release-fixture.log`、结果 `/tmp/pc28-layout-release-fixture-online.json`；临时模式不验证生产执行器，正式模式仍检查。首跑发现旧长列表DOM已按设计移除，发布脚本由“必须隐藏”修正为“缺失或隐藏”后通过，首日志已保留。当前前端边界修复与QA收尾仍在进行，正式版本和线上更新须等根代理冻结，不将临时演练记为发布成功。

```bash
python3 /tmp/pc28-layout-release.py --apply --accepted-commit <40位已验收提交> --root-go ROOT_APPROVED_LAYOUT_RELEASE
node /tmp/pc28-layout-release-online.cjs
```

回退为对应UI提交的代码revert并仅重启API，不回退数据库。根代理统一提交／推送发布记录；本角色不提交、不推送、不另建定时任务。
