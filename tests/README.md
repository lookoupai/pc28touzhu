# tests

测试目录。

第一阶段建议优先覆盖：

- 来源解析
- 标准信号转换
- 幂等键生成
- 执行任务过期判断
- 执行结果回写

测试重点应放在领域逻辑，而不是 UI。


## 导航与页面回归

```bash
PYTHONPATH=src python3 -m unittest tests.test_ui_navigation tests.test_api -q
node tests/test_navigation_ui.cjs
node tests/test_cycle_ui.cjs --browser
```

浏览器验证使用临时 SQLite 数据库，不读取 `.env`、不启动 worker，外部请求被拦截；导航 fixture 只允许登录/退出写请求，批量操作失败通过拒绝写入验证。`test_cycle_ui.cjs` 在自己的临时库中回放规则保存、轮次恢复。

浏览器依赖 `playwright-core`，表单 DOM 回归额外需要 `jsdom`。可通过 `PC28_UI_NODE_MODULES` 指定已有依赖目录，通过 `PC28_UI_CHROMIUM` 指定 Chromium 可执行文件；默认沿用已有本地测试工具目录。`PC28_UI_EVIDENCE_DIR` 可指定截图目录。

导航用例覆盖所有用户页和管理员页的桌面/手机尺寸、菜单高亮、权限、旧链接、筛选参数、关联方案、登录回到原页、未保存编辑提示和加载失败。
