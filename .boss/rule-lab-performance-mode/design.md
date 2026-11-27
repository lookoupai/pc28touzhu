# 模拟速度切换

2026-11-27用户要求平时低资源运行、需要结果时加速。当前五方案都已完成22861期，原batch1暂停，后台idle。主机4核、约9.6GiB可用内存，当前lab CPUQuota20%、MemoryMax256MiB。CPU额度是主要速度限制，增加内存不会直接提高单worker计算速度。

## 实现

页面后台状态区提供“省资源 / 加速”按钮。省资源CPU20%=0.2核，加速CPU100%=1核；两档MemoryMax256MiB、禁swap、Nice10/低IO优先级、一次最多100运行版本/500保存版本不变。额度相对单核，不是整机百分比。真实速度取决于SQLite/源查询和主机竞争，不能把5倍额度直接叫5倍速度。

采用systemd实时set-property，无需重启worker，不改变游标/下注/风控/模拟时间。默认基准unit仍20%，手动档位由systemd持久化drop-in，服务/服务器重启后保持；空闲不占满额度。不存在自动加速或自动开始实验。

API只给管理员开放GET/POST `/api/platform/rule-lab/performance`，POST只接收`{mode:economy|fast}`。档位全服务共用，对所有管理员实验生效，不伪装成单批次配置。GET读取实际systemd及cgroup CPU限额与内存，不读前端缓存当实际状态；POST执行后再次读取核对，失败/超时不声称成功。网页断网未知结果时只GET核对，不自动重提POST。

API进程www通过sudo仅能执行root拥有、不可被www写入的固定helper及两个固定参数。helper安装到`/usr/local/libexec/pc28-rule-lab-resource-mode`；sudoers只列这两个完整命令，不授予通用systemctl/脚本写权限。helper不导入项目代码、不读.env/数据库，以空环境执行绝对路径systemctl，固定unit，固定CPU属性，root目录锁串行，超时有界。Web读取helper安装状态，未安装/服务inactive/无法读取真实限额时禁用按钮并给出原因。主模拟服务的文件/网络权限不变。

## 验证与发布

- 临时单元/集成：模式枚举/拒绝额外字段/用户权限、GET不修改、POST固定argv/超时/实际值不符、真实cgroup属性解析、helper非法参数拒绝；没有测试访问生产sudo helper。
- 浏览器：切换两档、当前态/CPU上限显示、按钮禁用、服务不可用/丢响应后只读核对、手机操作。
- 使用隔离fixture和独立systemd临时unit，在0.2核/1核下完成同一回放，逐实验成绩及输入一致；测实际wall/CPU/RSS，避免未经测量宣称加速倍数。
- 发布前完整回归；生产仅空闲切fast→economy核验cgroup/持久化，worker PID不变、旧账本统计不变。最后保留省资源模式。
- 无DB迁移或队列修改。本次可连同已验证组合7～30模板发布。增量续跑目前只有只读检查，分段/归档/真正续跑尚未完成，不因本次速度切换宣称已可用。

## 实测发现的旧回放边界问题

第一次双档fixture结果hash不同。定位到按轮回放在进入settle_pending前已耗尽预算，该函数按设计返回，但外层仍推进当前draw并因上笔pending跳过本期新下注。添加确定性时间耗尽测试，旧版失败（基准8笔、耗尽路径4笔），修复后通过。现在在结算阶段预算用尽时不推进当前期，下一tick重复该期，已完成的结算有幂等保护。

新建历史批次游标记录replay_scheduler_version=2。旧按轮批次已处理>0且无该版本标记时返回review_required及明确提示，保留原数据，比较组不再输出delta。前向模拟/未运行/旧逐期模式不受此标记影响。不能自动重算并覆盖用户已完成账本。本次虽只加速度选项，已发现的正确性问题必须一并修复和披露。
