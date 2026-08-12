# 订阅源按集预筛选设计

## 目标

中心服务在 TG 或 Pansou 一次订阅解析产生的分享候选全部完成元数据检查后，按电视剧的季集槽位只选择一个文件，再创建 worker 搬运任务。来源优先级严格优先于文件大小，默认顺序为 `123 > 115 > Quark > Aliyun`；同一来源内存在多个版本时再选择较大的文件。

## 数据流

- TG 集群扫描先收集本次所有匹配消息中的分享链接，并为整次扫描创建一个 observation。中心服务收到全部 inspect manifest 后统一选择候选，不再按单条 TG 消息提前创建 Move 任务。
- Pansou 保留现有的整次搜索 observation，但集群消费端读取与非集群路径相同的全局 `telegram.transfer_priority` 配置。
- inspect 幂等键同时绑定 observation、消息和分享身份。重试时同一批次复用任务；候选集合变化时使用新任务，避免新旧 observation 的 manifest 永久等不齐。
- 尚未等齐的 inspect manifest 标记为 `incomplete` 并退出 pending 队列；缺失 manifest 后续到达时仍与已有记录合并并完成消费。
- 非集群 TG/Pansou 继续复用现有候选汇总路径，并在保存到临时盘之前过滤已经接受搬运任务的季集。
- 已有 `transferring` 或 `transferred` 任务的季集视为已接受槽位。后续 observation 即使发现更高优先级来源，也只记录为跳过，不再创建第二个搬运任务。
- 同一订阅的手动与定时运行在中心进程内串行，避免并发扫描同时越过 standalone 的槽位检查。
- 无法识别集号的电视剧文件维持现有透传行为，避免误删特典或命名异常文件。

## 选择规则

同一 `(subscription_id, season, episode)` 内依次比较：

1. 若槽位已经接受过搬运任务，保留最早接受的任务，不再重新选择；
2. 否则按来源在配置优先级中的排名选择，排名更高者胜出；
3. 来源相同时选择文件更大的版本；
4. 大小相同时使用稳定的 provider/path/file ID/source key 顺序决胜。

电影和无法识别集号的电视剧文件不改变现有语义。

## 验证

- TG 两条不同消息分别包含同一集的分享时，所有 inspect task 使用同一个 observation key 和总候选数。
- 123 文件小于 Quark 文件时，123 仍是唯一搬运候选。
- 同一来源的同集多个版本仍只保留较大的文件。
- Pansou 集群路径使用全局自定义来源优先级。
- 后续 observation 发现同集新来源时不会追加 worker 搬运任务。
- 同一来源对象的大小、修改时间或哈希变化不会覆盖已经接受的任务状态，也不会重新派发。
- observation 候选变化时 inspect 幂等键随之变化，完全相同的候选则保持稳定。
- standalone 中被已接受槽位拦截的其它来源会持久化为 `skipped`，且不会执行临时保存。
- 不完整 manifest 不会阻塞后续 observation；同批缺失结果到达后仍可从 `incomplete` 恢复为 `consumed`。
- 运行 `internal/subscription` 与 `internal/cluster` 的相关 Go 测试，并执行格式化和静态检查。
