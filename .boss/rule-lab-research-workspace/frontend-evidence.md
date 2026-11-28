# 研究工作区页面交付证据

新增rule_lab_research.js与一个四标签共享抽屉，真实API完成排名、候选、批量管理、质量分页。没有引入框架/依赖，结果页和一期模板/队列保留。共享bridge按drawerId独立close/reset，避免新模块覆盖旧模块的草稿与权限处理。

## 红绿与真实流程

- 新DOM红测实际因缺少labResearchButton入口失败，日志 `/tmp/pc28-research-UI-red.log`。
- 新DOM全链通过：真实两来源排名、收藏/修改、批量改名、顺序分页质量检查，0脚本错误；`/tmp/pc28-research-UI-green.log`。
- Chromium最终通过：真实同窗口排名、收藏提交成功后丢响应且同request_id重试、名称XSS文本、笔记、快照单版本草稿、批量改名丢响应原请求重试、精确短语删除、一次手动append不启用每日、来源删除后“收藏时快照”、详情收藏关闭回到可见入口、质量200期页取消至400后继续到406不重复、403清缓存/禁入口、旧模板草稿guard仍工作、金额spinner step1、390px无页面/抽屉内横向溢出且主要按钮≥44px。日志 `/tmp/pc28-research-browser-final.log`。
- 质量只读请求不经过全局mutation-finally，首轮403关闭后入口未立即禁用；已真实红测复现，补共享控件刷新后最终浏览器通过。
- 旧selfservice真实DOM/Chromium及layout DOM分别通过：`/tmp/pc28-research-selfservice-DOM.log`、`/tmp/pc28-research-selfservice-browser.log`、`/tmp/pc28-research-layout-DOM.log`。

## 视觉检查

实际查看 `/tmp/pc28-research-ui-evidence/` 的rank-desktop.png、favorites-mobile.png、quality-results-mobile.png，桌面同口径分组、手机端标题/输入/指标、异常说明可读；质量无证据缺失率为“—”，不画0%伪完整率。收藏与排名仅显示本玩法冻结赔率及退款摘要，不输出0～27号码整表。

批量提交成功后重新读取并重建选择清单，清空已处理选择，保留成功反馈。删除中对象不再可选；网络未知状态仍保留原preview/payload及request_id。扫描切换标签或关闭时停止后续页请求，仅点击“取消检查”时在当前标签保留已读结果并可继续；切换工具、关闭后重新进入或刷新页面均需重新检查。

STATUS: DONE
REASON: 新四个真实页面闭环及旧共享抽屉/模板回归全部通过。
