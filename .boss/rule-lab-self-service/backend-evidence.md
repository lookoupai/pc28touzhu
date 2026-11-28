# 自助实验室后端交付证据

状态：后端实现与对应回归完成，等待 root 的全量、页面及正式发布验收。生产库、服务和 Git 主分支未由本代理操作。

## 最终行为

- schema7 仅新增模板、命令收据、持久队列和实验出处列。运行与续算读取实验自身的冻结定义，不追随模板编辑或删除。
- 模板 CRUD、条件规范化/同义去重、风控模板与原实验配置复制完成；旧内置 condition_key 保持一致。
- 多来源/多玩法预览、单事务批量创建、容量复检与幂等收据完成。自选比较支持单条件、单赔率，固定加入原风控基准并按完整政策去重。
- 比较组的条件出处 keys 对齐实际子集。原风控基准继承准确出处，模板候选保存 ref/name/revision，快捷变体不冒充原模板；手动引用真实规则时使用服务器核实后的名称及修改标志。差异逐字段展示主/附加日层等参数的旧值与新值。
- 历史批次与比较组由一个持久队列启动。旧历史 `/state` 进入同一队列；live 和既有 series 保持原入口及同一 SQLite 事务内的全局 100 版本名额检查。比较组每完成一个子批次回到队尾，暂停/移出/编辑/删除均禁止自动恢复。
- 调度每次最多处理 20 个对象、100ms 预算；旧比较组收编也计入上限。运行条目最多核对 10 个且保存轮转游标，为排队晋级保留一半时间预算。工作读取最多 100 个运行批次，待结算暂停批次另有 20 个上限。
- `import_rule_lab_queue.py --db PATH --manifest PATH --legacy-unit UNIT [--apply]` 默认只读，正式导入要求 schema7 且原启动器已明确停止。只增队列/交接元数据，不改任何实验旧列、进度、统计、ID、水位或 series。相同清单重复导入完整幂等，已暂停/移出项不会复活，同清单标识的成员或原指纹变化拒绝覆盖。
- 原清单启动器识别对应 user/prefix 的交接记录后正常退出；单成员旧启动入口也检查数据库队列归属，防止重启旧服务后出现第二启动者。旧结果清理工具兼容 schema7，保护快照对新增列补默认值。

## 实际执行

命令均在 `/www/wwwroot/pc28touzhu-lab-self-service`，Python 前缀为 `env PYTHONPATH=src python3 -m unittest`。

| 范围 | 用例/命令后缀 | 实际结果 | 日志 |
| --- | --- | --- | --- |
| W1 功能缺失红测 | `tests.test_rule_lab_templates tests.test_rule_lab_self_service_api -q` | 61 项，5 项预期失败：模板接口缺失；其余既有用例通过 | `/tmp/pc28-self-service-W1-red.log` |
| W2 功能缺失红测 | 两个 bulk / selective study 用例 | 2 项均因接口 404 失败 | `/tmp/pc28-self-service-W2-red.log` |
| W3 功能缺失红测 | `tests.test_rule_lab_queue -q` | 4 项均因队列接口 404 失败 | `/tmp/pc28-self-service-W3-red.log` |
| 出处/差异修复红测 | selected risk template provenance、双日层具体数值用例 | 2 项实际业务断言失败，修复后纳入最终绿测 | `/tmp/pc28-self-service-review-red.log` |
| 真实规则出处修复红测 | 手动候选规范化用例 | 1 项因客户端名称未被服务器替换失败，修复后纳入最终绿测 | `/tmp/pc28-self-service-provenance-red.log` |
| W3 兼容门禁 | queue、self_service_migration、batch_queue、series、continuous_worker、results_migration、ops | 50/50 通过，无跳过 | `/tmp/pc28-self-service-W3-finalize.log` |
| 最终后端宽门禁 | templates_domain、templates、self_service_api、self_service_qa、queue、self_service_migration、comparison、api、management、management_api、batch_queue、series、continuous_worker、results_migration、ops、worker | 174/174 通过，无跳过 | `/tmp/pc28-self-service-backend-final.log` |
| 最后队列查询调整复测 | `tests.test_rule_lab_queue tests.test_rule_lab_self_service_migration -q` | 12/12 通过，无跳过 | `/tmp/pc28-self-service-queue-final.log` |
| 最终来源撤销修复红测 | 真实来源在资格检查后、分页核对前撤销共享；临时503对照 | 2项，永久403仍占运行名额的断言失败；503对照通过 | `/tmp/pc28-self-service-source-status-red.log` |
| 最终增量门禁 | 403永久撤销、503临时失败、旧finite无prefix清单、6项迁移验证 | 9/9通过，无跳过 | `/tmp/pc28-self-service-final-delta.log` |

最后一次查询调整在 LIMIT 前排除已经物理删除的 owner，避免长期保留的队列墓碑挤掉当前可见任务。编译检查覆盖所有改动的 Python 运行模块与三个维护脚本；`git diff --check` 通过。

## 行为证据要点

- 25 个两版本任务在有限 tick 内全部启动，不再停在前 20 个；第 21/22 个完成/错误能够轮转同步。
- 比较组子任务完成后，排在它后面的普通任务先得到名额；错误子任务必须显式重试。
- 真实临时 SQLite 导入验证覆盖 schema6 默认只读、schema7 重复初始化、active/unknown 启动器拒绝、第二项写入失败整组回滚、相同清单原样重试和原启动器退出。
- 原有 rename 回归改为验证原配置 hash 不变、数据库队列仍运行；旧 manifest 被数据库接管是新授权边界，不能再用旧启动器 eligibility 代替当前队列状态。
- root全量发现的旧finite无request_prefix清单已兼容：仅存在prefix时查询整清单handoff；每成员数据库队列归属仍单独保护。
- 分页读取期间来源永久撤销（source_unavailable/403）会将批次及队列转为需要处理，释放名额且保留全部统计和游标，需明确retry；临时503仍允许自动重试。真实来源在两次读取间撤销共享的用例复现后修复，未改变待结算处理。
- root/独立 QA 另有真实并发 bulk 容量竞争、同 request_id 重试、生产外置副本迁移/导入和桌面/手机页面验证。本文件不将这些验证冒记为本代理执行。

STATUS: DONE
REASON: 后端源码和对应测试已完成并通过；已停止修改，交给 root 执行全量与发布门禁。
