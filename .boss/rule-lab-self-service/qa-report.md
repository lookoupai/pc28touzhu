# 自助实验室验收

当前状态：实现验收通过，可以发布冻结提交。生产尚未切换。

## 执行环境与范围

开发工作树 `/www/wwwroot/pc28touzhu-lab-self-service`。所有写入测试使用临时 SQLite、真实 WSGI 和本机 Chromium。外置生产副本只复制后试验，不回写生产。未安装依赖、扩大资源额度或创建任何定时任务。

## 已完成门禁

| 项目 | 结果 | 证据 |
| --- | --- | --- |
| 最终完整Python发现集 | 1339项：1337通过、0失败、2既有可选环境跳过；两个模块分组分片无遗漏 | `/tmp/pc28-self-service-release/python-shard-0.json`、`python-shard-1.json` |
| 新核心模块行覆盖率 | 802/887 = 90.42%，各模块82.79%以上，无分支或JS覆盖率声明 | `/tmp/pc28-self-service-release/coverage.json` |
| W1/W2 根独立门禁 | 101 通过 | `/tmp/pc28-self-service-root-W2.log` |
| 后端最终专项 | 174 通过，无跳过 | `/tmp/pc28-self-service-backend-final.log` |
| 最后故障与兼容修复 | 9 通过，无跳过 | `/tmp/pc28-self-service-final-delta.log` |
| 真实页面回归 | 10 组通过 | `/tmp/pc28-self-service-final-ui.json` |
| schema6→7 生产副本迁移 | 初次/重复初始化均通过；旧列和水位不变 | `/tmp/pc28-self-service-release/migration-final-precommit.json` |
| 生产副本队列导入 | 4 阶段通过，18 个成员保留 | `/tmp/pc28-self-service-release/queue-import-interim.json` |
| 发布工具本地验证 | 5 通过 | `/tmp/pc28-self-service-release/test_release_helpers.py` |
| 语法与差异检查 | 通过 | Python compile、node --check、git diff --check |

页面10组为 self_service_ui、self_service_browser、layout_qa、observation_ui、results_ui、amount_ui、layout_ui、management_ui、continuation_ui、long_streak_ui；覆盖新模板/批量/对比/队列，以及旧成绩、金额估算、改名删除、续算与长连错。权限失效修复后另复验新浏览器、新DOM和旧layout DOM，均通过。

## 独立行为证明

- 同请求并发与丢响应重试只创建一次；两个请求争抢有限保存额度时只有一组成功，另一组明确拒绝。
- 最后一个来源写入失败，整个批量创建零新增；模板过期不建半组。
- 已删模板的原批次复制仍保留原条件与历史赔率。一个非基准条件×一套赔率×两种风控恰好生成两版本。
- 旧恢复收据不能撤销用户后来暂停；移出项不会被tick重启，显式重新入队保留游标。
- 25个小任务全部能入场，后位完成/错误能轮转同步，比较组子方案完成后排尾。
- 来源在读取过程中永久撤权转为可读错误并释放名额；临时503继续重试。已有进度、结果和待结算规则保留。
- 模板与队列权限按账号隔离；匿名401、普通用户403、他人对象404；页面文本安全渲染，失效权限清空草稿。
- 导入只增新队列与交接元数据；运行旧启动器时拒绝，重复导入不改任何记录，旧启动器随后识别移交并退出。

## 明确的既有例外

默认全量中的两个生产副本审计测试需要旧指定快照及只读来源环境变量，原来即按条件跳过。此轮使用当前schema6实际副本执行独立迁移/导入校验，完整续算以真实临时来源及浏览器流程验证。

旧综合 `tests/test_rule_lab_ui.cjs` 第一条空状态文本已不属于当前布局，在未修改基线8a82362同样失败；保留文件，未标记通过。已用当前页面10组实际流程覆盖本次验收，基线失败证据见 `/tmp/pc28-self-service-baseline-legacy-ui.log`。

最终上线需对实际提交再跑生产副本初始化/导入并生成精确提交验收收据；上线后只读核对旧定义、成绩、续算配置与任务继续推进。

最后全量曾发现旧finite manifest无request_prefix的兼容错误，已修并在最终1339项完整发现集中通过。最后源码/测试文件指纹与全量开始时一致，未以旧提交测试替代新实现验收。

覆盖率使用stdlib行追踪并以可执行行全集为分母，排除非源码的行0；没有安装依赖。

新增API本机真实WSGI顺序请求各40次，模板/队列/比较预览P99分别36.12/9.74/21.41ms，均低于500ms；仅代表50版本临时数据集，不声称生产负载分位。当前为原生JS页面，无Lighthouse工具或新前端框架；采用真实Chromium流程、手机溢出与交互实测，没有虚构Lighthouse分数或长期内存结论。
