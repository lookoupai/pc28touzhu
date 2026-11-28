# 研究工作区任务、写集与验收

## 摘要

- feature=rule-lab-research-workspace，契约为plan.md。确定范围：排名、收藏、批量管理、质量预检；高级范围由root处理用户答复后追加。
- 总并发严格root+一个实现代理。该代理按Backend→Frontend角色串行；root负责只读审查、独立测试和发布准备。不再派生子代理。
- Blast Radius高：预计运行文件13～17，新增schema8与批量写事务。用户已明确授权继续全部后续功能与发布/上传；无依赖安装、无实际生产删除、无自动新定时任务，不追加重复确认。
- 各wave可并行准备测试文档，但进入下一wave必须先通过上一wave门禁。不得把404/import失败之外的测试准备错误当业务红测。

## 1. Repo preflight

| 项目 | 事实 |
| --- | --- |
| 工作树 | /www/wwwroot/pc28touzhu-lab-research-workspace，feat/rule-lab-research-workspace，base79376e7，初始干净 |
| 主仓库 | /www/wwwroot/pc28touzhu，main79376e7，一期已push origin/main |
| 语言与运行 | Python WSGI stdlib+SQLite，原生JS/CSS；pyproject声明>=3.8，实际3.12；没有Node构建 |
| CI | .github/workflows/docker-image.yml仅Docker构建/推送，不包含自动测试 |
| Python | env PYTHONPATH=src python3 -m unittest discover -s tests -q；上期发现1339、1337通过、2既有外置环境skip |
| UI | jsdom=/tmp/pc28-draw-trigger-ui-check/node_modules/jsdom；playwright-core=/tmp/pc28-cycle-ui-tools/node_modules/playwright-core；Chromium=/root/.cache/ms-playwright/chromium-1243/chrome-linux64/chrome |
| schema | 7，initialize集中；新版仅加favorites至8 |
| 额度 | 全服务2000版本/100运行，每用户80批次；新收藏500活动项；bulk20owner；quality20来源×单页200期×≤366天 |
| 认证 | 现会话管理员门禁；新全部对象user_id归属；源必须共享records/analysis |
| 迁移基准 | /tmp/pc28-research-release/preflight-v7.db，root提供一致外置副本 |
| 约束 | 不装依赖，不改真实下注逻辑，不操作生产bulk删除/改名/追加，不创建新自动日程 |

## 2. 任务与文件owner

| ID | 角色 | 文件写集（相对工作树） | 工作与验收 |
| --- | --- | --- | --- |
| B01 | Backend | A services/rule_lab_research.py；M services/rule_lab_observation.py；A tests/test_rule_lab_research.py | research options、统一window排名、组内名次/并列、完整性与实际赔率分组、快照分页 |
| B02 | Backend | A services/rule_lab_favorites.py；M services/rule_lab_store.py | schema8 favorites，CRUD冻结、软删收据、容量与所有权 |
| B03 | Backend | M services/rule_lab_service.py；M api/app.py；A tests/test_rule_lab_research_api.py | 新路径字段白名单/状态/body/query、收藏按冻结配置新建单版本草稿；映射新JS资产 |
| B04 | Backend | A services/rule_lab_bulk_management.py；M services/rule_lab_management.py；M services/rule_lab_series.py | 同连接helpers、批量预览和原子改名/删除/append；删除依赖闭包与job排序；旧单项兼容 |
| B05 | Backend | A services/rule_lab_quality.py；M services/rule_lab_source.py；A tests/test_rule_lab_quality.py | 源真实行有界只读分页、分类/分母/边界/截止/游标、超时重试 |
| B06 | Backend | A tests/test_rule_lab_bulk_management.py；A tests/test_rule_lab_research_migration.py；M scripts/maintenance/migrate_rule_lab_results.py；M scripts/maintenance/import_rule_lab_queue.py（若schema8只读兼容需要）；M既有test_rule_lab_* .py版本/旧入口测试 | 故障/并发/越权/幂等、旧列迁移、数据hash保持、旧版本工具兼容 |
| F01 | Frontend | M ui/rule_lab.html/css/js；A ui/rule_lab_research.js | 一个研究抽屉四tab，共享bridge，原详情收藏入口，已有结果/模板/队列不回归 |
| F02 | Frontend | A tests/rule_lab_research_ui_server.py；A tests/test_rule_lab_research_ui.cjs；A tests/test_rule_lab_research_browser.cjs；M既有CJS仅更新真实变化 | 临时真实API DOM/Chromium排名、收藏冻结/源删、批量3操作、质量分页取消重试/手机号、权限失效 |

所有src路径均补`src/pc28touzhu/`。Backend拥有全部Python（排除FE fixture），Frontend拥有HTML/CSS/JS及CJS/fixture；同一代理先完成BE再切FE，无中央文件并发写。新文件超出列表需先告知root；常规拆小函数可由owner决策，不追加用户确认。root可写独立QA测试和发布脚本，但与owner先对齐写集。

正式计划文档由本lead/scrum角色编写；Backend/Frontend交付各自evidence.md；root安排qa-report/deploy-report最终证据。开发owner不commit/push/merge生产或重启服务，root最后统一完成。

## 3. Contract Matrix

| ID | 承诺 | 验证 |
| --- | --- | --- |
| R01 | 统一窗口，同口径组内排名，异赔率快照不混组 | 两源不同截至日期、同ID旧新赔率、不同风险/注额、相同指标并列、partial/pending/unknown不授rank |
| R02 | 排名只读且分页不先截断 | >limit候选，真实全体排序；过期cursor409；原账本逐列hash不变 |
| R03 | 收藏配置冻结、当前与收藏时成绩区分 | 源继续运行后current更新saved不变；源删后saved可读/current=null；相同ID重试不复活 |
| R04 | 收藏快照可新建单条件单赔率草稿 | 原模板删/赔率目录变后仍原定义；容量拒绝零新增；原成绩不复制 |
| R05 | bulk全部预检/唯一事务 | 第二对象失败时名称/job/queue/series/receipt完全回滚；跨用户不泄露 |
| R06 | bulk删除闭包，后台有界，收藏保留 | 源+所有依赖组合法、漏依赖阻断；study先清理、外部共享输入和favorite不删 |
| R07 | 手动bulk append不制造日程或复活暂停 | 新adopt compute=false，旧compute/daily保持；active/paused段整组拒绝；原26版本ID/基线不变 |
| R08 | 数据质量缺失不造假 | absent有归档证据/无元数据不同分类、late按开奖前40秒、invalid/null/duplicate、内部gap与日期边界unknown |
| R09 | 有界扫描可取消/继续/重试 | 200页上限、20来源/总预算、游标越界/错scope/来源身份变更、超时保留进度不重复累加 |
| R10 | 账号/网络/移动状态完整 | 401/403/404、严格字段、XSS、丢响应原请求、手机按钮44px无内部/页面横向溢出 |
| R11 | schema8旧数据与既有行为保持 | schema7副本初始化两次旧表列/水位不变；旧管理/续算/队列/结果全回归 |

## 4. Evidence waves

| wave | 范围 | 红测 | 绿门禁 | 停止条件 |
| --- | --- | --- | --- | --- |
| W1 | 收藏/schema8与排名 | API缺失；不同窗口/赔率混排反例；收藏源删冻结 | research+api+迁移 +旧observation/management | 任意旧hash改变、越权泄露或伪排名 |
| W2 | 原子批量管理 | 第二目标失败未回滚；依赖组/暂停/手动append | bulk_management +旧management/series/queue/continuous_worker | 半组、越过删除依赖、隐式新schedule或暂停被复活 |
| W3 | 真实来源质量 | 缺行归零、late/invalid错分、截页冒充完整 | quality +旧source/worker +超时边界 | 源库写入、无证据却标完整、无预算循环 |
| W4 | 四闭环UI/全量发布 | 新入口/真实行为缺失 | 新DOM+Browser；旧selfservice/layout/results/continuation；Python全量；副本schema8 | 主路径不可用、旧结果回归、进度倒退、未解释失败 |

## 5. 完成标准

各wave留实际命令、退出码与未覆盖项；修复不是把断言删除。无coverage包时可用stdlib line tracer并以可执行行全集为分母，不能输出伪100%；新核心模块≥70%。根独立复核后才能提交/部署；既有两环境skip及旧过期genericUI单列，不冒充通过。正式发布后只读确认配置、成绩、ID/水位、续算设置以及队列运行进度，上传最终GitHub主分支。

STATUS: DONE
REASON: 确定范围的写集owner、wire、证据波和生产边界完整，可由root审查后派发Backend。
