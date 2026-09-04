# Agent Note: 未证实归属的对象发布进入 unresolved

Status: implemented

## 问题

[对象存储命名空间](./2026-08-21-namespace-in-an-object-store.md)先登记 reservation，再以 create-only `Put` 发布对象，最后提交路径引用。原决定允许超龄 reservation 进入 garbage，也把任意 `Put` 失败立即 `Abandon` 成 garbage。这个规则把“有记录的 key”误当成“这次写入拥有的对象”。

`Put` 返回错误时，调用方不能证明对象没有落地，也不能证明同一 key 下已有对象属于本次 reservation。条件创建可能已经成功但响应丢失；`EEXIST` 则明确说明已有对象没有被本次调用创建。两种结果若都进入 garbage，清扫器随后按 key 删除，会把归属未知甚至确定属于别人的对象当成本次失败写入的垃圾。仅等待一段时间不能补出归属证明；时钟前跳还会让仍在进行的上传被错误判为可删。

## 决定

对象记录增加 durable `unresolved` 状态。只有 `Put` 返回 `nil` 才证明 create-only publication 完成；任何 `Put` error 都通过 `Quarantine` 把 reserved record 转成 unresolved，保留原错误，并且永远不把该 key 交给 `Garbage`。`Commit` 只接受 reserved；unresolved 不能重新解释成一次成功写入。

`Garbage` 只返回已经由权威 namespace transaction 标记为 garbage 的对象。reserved 不再因创建时间或调用方给出的 grace 变成 garbage；进程崩溃、请求中断或远端结果未知留下的 reservation 同样进入 fail-stop 状态，不产生删除授权。`Put` 已经成功、随后 `Commit` 失败是唯一允许调用 `Abandon` 的上传失败路径，因为此时本进程已经证明该 key 下的对象由本次 reservation 创建。

reserved 与 unresolved 都计入 pending object 数量和字节 admission，也由 `ObjectStatus` 暴露。它们不会被时间自动释放；积累到阈值时新 reservation 以 `EAGAIN` 停止，要求运维诊断或将来的显式 reconcile 工具处理。这个有界停服结果比依据延迟猜测执行破坏性删除更安全。

schema v1/v2 写下 pending row 时没有保存如今需要的 requested bytes，也没有能证明对象归属的状态语义。旧数据库只有全部 object row 都是 referenced、且 namespace/node/size 一致性验证通过时才允许迁移；任何 reserved、garbage 或其它 non-referenced row 都使整个 open transaction 以 `EIO` 失败，migration 不提交。升级不能把旧 state 2 当成现在的 deletion authority，也不能为缺失的 pending bytes 编造数值。

这项决定取代旧决定中“超龄 reservation 可以成为 garbage”和“任意 `Put` error 立即 `Abandon`”两项规则。旧 note 保留当时为何选择上传前登记的理由；本 note 拥有对象归属不足时的终止状态。它不改变已经解除 namespace 引用的对象进入 garbage，也不改变[在途读者与清扫器](../../proposed/architecture/2026-08-21-readers-in-flight-and-the-sweeper.md)所讨论的 referenced→garbage 删除宽限期。

## 备选方案

**给 reservation 和对象都写入随机 claim，并按 claim 条件删除。** 能在丢失响应后重新读取对象 metadata，证明它是否属于 reservation；Azure 可配合 ETag，localdisk 可把 claim 放进 envelope。输在它同时改变 metastore schema、全部 `Objects` 方法、Azure metadata 与本地磁盘格式。当前没有安全 reconcile 工具消费这项证明，先保存 unresolved 已经能阻止错误删除，并由 pending threshold 把代价封顶。

**让 `Put` 返回 created、conflict、unknown 等 disposition。** 能区分明确的 `EEXIST` 与部分失败。输在没有 durable claim 时，unknown 仍然不能安全删除；任何跨进程丢失的返回值也不能代替持久归属。最终仍需要 unresolved，因此增加 disposition 不能消除本决定的核心状态。

**保留一小时 reservation grace。** 不改 schema，崩溃残留会自动回收。输在一小时是对最长上传时间和 wall clock 的猜测：上传超过阈值或时钟前跳就能让活跃 key 进入删除路径。延长阈值只降低发生频率，不建立归属证明。

**把 `EEXIST` 视为未创建，其它错误继续 `Abandon`。** 修复最直接的重复 key。输在连接错误、超时和服务端错误都可能发生在条件创建落地之后；错误分类无法证明对象归属，依 errno 决定是否删除仍会把未知结果变成确定事实。

**只在进程内 pin active reservation。** 能阻止同一进程的 sweeper 与正在进行的 `Put` 竞争。输在崩溃会丢失 pin，另一个进程或重新打开的 store 仍只能看到年龄；它解决并发时序，不能解决持久归属。

## 后果

- `Put` 的 nil/error 边界成为删除权限边界：nil 只在 create-only publication 已完成时返回；error 不能授权清扫对象。
- 一次未知发布不会覆盖或删除旧对象，也不会被报告为成功。原错误与 `Quarantine` 失败同时保留，unresolved 状态查询失败则整项操作失败。
- crash reservation 和 ambiguous upload 会永久占用 pending admission，直到有带独立归属证明的恢复工具处理。阈值使成本有限，但一个持续失败的 store 最终会拒绝新写；这是可观察的 fail-stop，不是自动恢复。
- 含 legacy pending state 的 v1/v2 数据库需要离线、带外的归属恢复才能升级；自动迁移只覆盖 referenced-only 的可验证历史。拒绝发生在迁移事务内，不留下半升级 schema。
- garbage queue 重新只表达 namespace 已经证明不再引用的对象，清扫器不再把时间当作 ownership proof。给真正 garbage 增加读者宽限期仍是独立提案。
- 新的 object state 是持久语义；完整性检查、状态统计和契约用例必须共同认识它，未知 state 继续以 `EIO` 失败。
