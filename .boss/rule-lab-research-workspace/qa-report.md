# 研究工作区QA验收

## 摘要

测试已通过：Backend/Frontend验证和root完整Python、生产副本、性能核对均完成。最终提交与生产部署由root执行，发布结果待记录。

| 范围 | 已知结果 | 证据 |
| --- | --- | --- |
| 完整Python发现集 | 1371项：1369通过、0失败、2既有显式生产副本环境skip | /tmp/pc28-research-release/python-shard-0.json、python-shard-1.json |
| 完整集之后的依赖rename修复 | 新root反例+bulk/rootQA共22项通过；不重复计入1371独立数 | /tmp/pc28-research-release/backend-final-delta.log |
| 新research DOM | 真实排名/收藏/改名/质量分页通过 | /tmp/pc28-research-UI-green.log |
| 新research Chromium | 桌面1440/手机390完整4闭环、丢响应、权限/XSS/焦点/扫描取消恢复通过 | /tmp/pc28-research-browser-final.log |
| 旧一期selfservice | DOM与Chromium通过 | /tmp/pc28-research-selfservice-DOM.log、selfservice-browser.log |
| 旧layout共享抽屉 | DOM通过 | /tmp/pc28-research-layout-DOM.log |
| root旧结果/管理/续算等 | layout_qa、observation、results、amount、management、continuation、long_streak 7组通过 | root外置UI汇总 |
| 新核心Python行覆盖率 | 459/493 = 93.10%（32项目标测试），所有新核心模块超过80%；非分支/非JS覆盖率 | /tmp/pc28-research-release/coverage.json |
| 生产副本schema7→8 | 两次初始化旧表旧列/水位完全保持 | /tmp/pc28-research-release/migration-final-precommit.json |
| 生产规模只读API | options P99约253ms，rank29ms，20对象bulk-preview92ms，质量200期23.46ms；40次顺序请求 | root性能外置JSON |

核心独立反例验证：单窗口十月不混入十一月利润；赔率ID相同但实际赔率不同不得同组，显示名改动不得拆组；无下注完整月不当缺档；翻页变更拒绝旧游标；收藏源物理删除后的原收据重试及冻结草稿；同收藏请求并发唯一；bulk最后对象过期和第二次数据库写失败全事务回滚；删除/append的jobs/streams/checkpoints/收据也全回滚；同命令study/source顺序改名不自我stale。质量边界-40秒有效/-39秒迟到、null/未知归档、缺期、每日无效开奖、撤销共享都已验证。

限制如实保留：两个可选旧环境测试按原skip；旧早期generic test_rule_lab_ui.cjs在一期前基线已因旧结构过期，不作本轮通过证据。质量分页是现状读取而非跨页数据库冻结，不用来证明旧实验完整回放；源码和UI均明确这一含义。没有长期内存/Lighthouse分数声明。

待root完成：精确提交clean-tree核对、正式迁移/服务恢复、线上只读四tab与原实验/队列/续算指纹进度核对、GitHub上传最终提交。正式结果记录deploy-report.md。

STATUS: DONE_WITH_CONCERNS
REASON: 已实现功能与测试通过，尚待root最终提交和正式发布验收，不存在已知未修复功能阻断。
