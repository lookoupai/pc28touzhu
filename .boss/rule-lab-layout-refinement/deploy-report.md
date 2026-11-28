# 布局调整发布报告

- 状态：发布及线上核验完成。根代理验收提交 `60ac9a4f470281575a46af4a057573374491b711`，已于2026-11-28 05:37 UTC快进生产。
- 线上：main `60ac9a4f470281575a46af4a057573374491b711`，运行更新后工作树干净，入口 `https://tg.dca.de5.net/rule-lab`。
- 发布范围：仅 `rule_lab.html`、`rule_lab.js`、`rule_lab.css` 运行文件及配套文档／测试；不修改Python、API、schema、实验或调度。快进后仅重启 `pc28touzhu-platform.service`。

## 已执行预检

`/tmp/pc28-layout-release.py` 默认只读，实际退出0；预检证据 `/tmp/pc28-layout-release-preflight.json`。线上638版本、28批次、29,220日汇总、3比较组、9增量系列，schema6。API PID1629337；模拟worker PID1408564，1核／256MiB／禁swap；来源、自动触发、结算与执行器正常，旧一次性队列保持inactive。

保护范围复用上一轮已验证规则：全部定义、冻结条件赔率、比较关系及增量控制必须相同；16个非增量完成批次的56版本／2,806日汇总及风险状态完全保持；原增量实验允许自然增长，但进度不得倒退。两次只读采样的保护自检已通过。

`/tmp/pc28-layout-release-online.cjs --baseline` 实际13次GET通过，保存旧成绩与2／3／6月、完整月的8个观察响应指纹。认证仅在内存，所有请求限制为同源GET。证据 `/tmp/pc28-layout-release-http-before.json`。

## 发布门禁与临时演练

脚本 `/tmp/pc28-layout-release.py --apply` 必须同时给出40位已验收提交号与 `ROOT_APPROVED_LAYOUT_RELEASE`；要求开发HEAD准确且干净、生产main干净、可快进、只含UI运行白名单和本次文档／测试、无删除。正式执行前重新采样并刷新HTTP基线，更新后核对账本与服务，先标记待HTTP核验。

新版页面验证已按冻结DOM契约准备：默认仅一张列表、概览五列且来自同一观察窗口，三种视图切换零GET，原长列表不可见，新建／系统／设置／详情抽屉、ESC／遮罩／焦点恢复，比较组独立详情不切换底层实验选择，金额换算使用当前行的风控与成绩。真实页面JS用jsdom运行，不启动浏览器，不读取profile；不以此声称像素布局验收。最后检查执行器心跳和公网JS／CSS200字节一致。

Python脚本编译、Node语法检查、预检与旧HTTP基线均通过。实现落地后，用固定生产副本的临时WSGI和当前开发HTML／JS实际演练发布脚本：638版本、39次GET、0写请求、0脚本错误，五列口径、三个本地视图、抽屉焦点、观察规则及比较组独立详情金额换算通过。日志 `/tmp/pc28-layout-release-fixture.log`、结果 `/tmp/pc28-layout-release-fixture-online.json`；临时模式不验证生产执行器，正式模式仍检查。首跑发现旧长列表DOM已按设计移除，发布脚本由“必须隐藏”修正为“缺失或隐藏”后通过，首日志已保留。上述为临时演练证据；最终边界修复通过根代理验收后已执行正式更新，线上结果如下。

```bash
python3 /tmp/pc28-layout-release.py --apply --accepted-commit <40位已验收提交> --root-go ROOT_APPROVED_LAYOUT_RELEASE
node /tmp/pc28-layout-release-online.cjs
```

回退为对应UI提交的代码revert并仅重启API，不回退数据库。根代理统一提交／推送发布记录；本角色不提交、不推送、不另建定时任务。


## 正式发布结果

根代理最终5套DOM流程（layout、独立QA、observation、amount、results）全部实际退出0，相关API 6项通过（0.623秒）。由于本次运行改动仅3个UI文件，未重复执行无关的完整Python套件。

```bash
python3 /tmp/pc28-layout-release.py --apply --accepted-commit 60ac9a4f470281575a46af4a057573374491b711 --root-go ROOT_APPROVED_LAYOUT_RELEASE
node /tmp/pc28-layout-release-online.cjs
```

实际发布和线上脚本均退出0。发布记录 `/tmp/pc28-layout-release-rollout-20261128T053753Z.json` 最终状态 `verified`；执行日志 `/tmp/pc28-layout-release-apply.log`；线上证据 `/tmp/pc28-layout-release-online.json`、`/tmp/pc28-layout-release-online.log`。

- 真实页面40次同源GET、零写请求、零脚本错误，6组布局主路径验证通过；默认五列全部与原观察窗口数值一致，概览／月度／风险切换不发请求，实验／比较组只显示当前列表，抽屉关闭恢复焦点，比较组详情使用自身风控及金额。
- 原成绩接口及8个2／3／6月、完整月观察响应的完整指纹与发布前一致。
- schema6、638版本、28批次、29,220日汇总、3比较组、9增量系列保持。全部定义及增量控制、固定16批次56版本2,806日成绩与风险状态指纹一致；增量进度无倒退。
- API新PID1647173；worker PID1408564未重启，1核／256MiB／禁swap保持；来源同步、自动触发、自动结算和旧队列状态／PID保持。执行器按既有机制自动重连，PID由1408628变为1647221，线上采样在线且心跳年龄1秒。
- 公网JS／CSS均200且与生产文件逐字节一致，证据 `/tmp/pc28-layout-release-public-assets.json`。JS SHA256 `064c1678591973618d91fde7576fdda14ae4a7de22867715a71bb3f99c0af49c`；CSS `d8a0f6c894fddd217e28b1fdcb767415215de6bc387d42dcb508fb6b06e1a917`。

核验方式为真实HTTP＋jsdom，未启动浏览器，不声称像素截图验收。无需数据库回退；代码回滚应revert该UI提交并仅重启API。根代理负责最终记录提交与推送，DevOps未自行提交或推送。
