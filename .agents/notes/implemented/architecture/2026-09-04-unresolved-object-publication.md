# Agent Note: 未证实归属的对象发布进入 unresolved

Status: implemented

## 问题

[对象存储卷](./2026-08-21-volume-in-an-object-store.md)先登记 reservation，再以 create-only `Put` 发布对象，最后提交路径引用。原决定允许超龄 reservation 进入 garbage，也把任意 `Put` 失败立即 `Abandon` 成 garbage。这个规则把“有记录的 key”误当成“这次写入拥有的对象”。

`Put` 返回错误时，调用方不能证明对象没有落地，也不能证明同一 key 下已有对象属于本次 reservation。条件创建可能已经成功但响应丢失；`EEXIST` 则明确说明已有对象没有被本次调用创建。两种结果若都进入 garbage，清扫器随后按 key 删除，会把归属未知甚至确定属于别人的对象当成本次失败写入的垃圾。仅等待一段时间不能补出归属证明；时钟前跳还会让仍在进行的上传被错误判为可删。

## 决定

对象记录增加 durable `unresolved` 状态。只有 `Put` 返回 `nil` 才证明 create-only publication 完成；任何 `Put` error 都通过 `Quarantine` 把 reserved record 转成 unresolved，保留原错误，并且永远不把该 key 交给 `Garbage`。`Commit` 只接受 reserved；unresolved 不能重新解释成一次成功写入。

`Garbage` 只返回已经由权威 metastore transaction 标记为 garbage 的对象。reserved 不再因创建时间或调用方给出的 grace 变成 garbage；进程崩溃、请求中断或远端结果未知留下的 reservation 同样进入 fail-stop 状态，不产生删除授权。`Put` 已经成功、随后 `Commit` 失败是唯一允许调用 `Abandon` 的上传失败路径，因为此时本进程已经证明该 key 下的对象由本次 reservation 创建。`Abandon` 还必须确认该对象未被 named 或 retained detached 节点引用；referenced、未知 commit 或已被 poison 的 metastore 都不能被解释成可删除。

[显式文件占有](./2026-09-07-file-locks.md)在上传后的原生最终发布处判定权限。grant 在上传期间到期，且尚未取得最终许可时，`Commit` 明确拒绝，成功 `Put` 留下的未引用对象可经 `Abandon` 成为 garbage。权限在到期前取得、提交稍后完成则按已确定顺序处置，不能仅凭响应时已过期宣称未提交。卷 commit、rollback 或 publication accounting 无法确定时，SQLite 与 authority 一同失败隔离；即使卷已知未修改，未知 accounting 也不能被 cleanup 掩盖。`Quarantine` 仍只表达 `Put` 没有证明归属，不能把未知卷 commit 重新命名成一次失败上传。

[持续文件句柄](./2026-09-08-live-file-handles.md)保留同一节点的当前引用。unlink 或 rename 替换使仍有 pin 的文件 detached，其当前对象仍为 referenced，并继续计入配额；删除名字不产生删除这些字节的权限。最后物理 close 才能在同一事务中释放节点与配额、把当前对象标记为 garbage。旧 epoch 的 detached 回收由独占 opener 在全库验证之后完成，不能靠 reservation 年龄或名字缺失推断。

范围写入的内部 revision CAS 也沿用同一边界：明确的未提交冲突在成功 `Put` 后允许 `Abandon`，清理成功才能重新读取当前节点并重试。`Quarantine` 或 `Abandon` 失败须保留原错误与清理错误，并停止该次自动重试；已知提交前的清理拒绝或取消本身不必隔离整个 coordinator。提交、回滚或最后 close 的 accounting 结果不明，以及原有 poison 状态，才维持相应的持久失败隔离，不能通过重放清理解除。retired 引用先失去发布权，已接纳 I/O 排空后才释放物理 pin，关闭不是新的延迟提交路径。

reserved 与 unresolved 都计入 pending object 数量和字节 admission，也由 `ObjectStatus` 暴露。它们不会被时间自动释放；积累到阈值时新 reservation 以 `EAGAIN` 停止，要求运维诊断或将来的显式 reconcile 工具处理。这个有界停服结果比依据延迟猜测执行破坏性删除更安全。

schema v1/v2 写下 pending row 时没有保存如今需要的 requested bytes，也没有能证明对象归属的状态语义。旧数据库只有全部 object row 都是 referenced、且卷/node/size 一致性验证通过时才允许迁移；任何 reserved、garbage 或其它 non-referenced row 都使整个 open transaction 以 `EIO` 失败，migration 不提交。升级不能把旧 state 2 当成现在的 deletion authority，也不能为缺失的 pending bytes 编造数值。

这项决定取代旧决定中“超龄 reservation 可以成为 garbage”和“任意 `Put` error 立即 `Abandon`”两项规则。旧 note 保留当时为何选择上传前登记的理由；本 note 拥有对象归属不足时的终止状态，文件占有 note 拥有最终发布许可与重启保护，文件句柄 note 拥有 detached 引用何时解除。已经解除所有节点引用的对象进入 garbage；[在途读者与清扫器](../../proposed/architecture/2026-08-21-readers-in-flight-and-the-sweeper.md)所讨论的旧内容 object pin／删除宽限期仍独立，保留节点并不冻结某次读取捕获的 object key。

## 备选方案

**给 reservation 和对象都写入随机 claim，并按 claim 条件删除。** 能在丢失响应后重新读取对象 metadata，证明它是否属于 reservation；Azure 可配合 ETag，localdisk 可把 claim 放进 envelope。输在它同时改变 metastore schema、全部 `Objects` 方法、Azure metadata 与本地磁盘格式。当前没有安全 reconcile 工具消费这项证明，先保存 unresolved 已经能阻止错误删除，并由 pending threshold 把代价封顶。

**让 `Put` 返回 created、conflict、unknown 等 disposition。** 能区分明确的 `EEXIST` 与部分失败。输在没有 durable claim 时，unknown 仍然不能安全删除；任何跨进程丢失的返回值也不能代替持久归属。最终仍需要 unresolved，因此增加 disposition 不能消除本决定的核心状态。

**保留一小时 reservation grace。** 不改 schema，崩溃残留会自动回收。输在一小时是对最长上传时间和 wall clock 的猜测：上传超过阈值或时钟前跳就能让活跃 key 进入删除路径。延长阈值只降低发生频率，不建立归属证明。

**把 `EEXIST` 视为未创建，其它错误继续 `Abandon`。** 修复最直接的重复 key。输在连接错误、超时和服务端错误都可能发生在条件创建落地之后；错误分类无法证明对象归属，依 errno 决定是否删除仍会把未知结果变成确定事实。

**只在进程内 pin active reservation。** 能阻止同一进程的 sweeper 与正在进行的 `Put` 竞争。输在崩溃会丢失 pin，另一个进程或重新打开的 store 仍只能看到年龄；它解决并发时序，不能解决持久归属。

## 后果

- `Put` 的 nil/error 边界成为删除权限边界：nil 只在 create-only publication 已完成时返回；error 不能授权清扫对象。
- 一次未知对象发布不会覆盖或删除旧对象，也不会被报告为成功。原错误与 `Quarantine` 失败同时保留，unresolved 状态查询失败则整项操作失败；未知卷 commit 则保留原记录与失败隔离状态，`Abandon` 失败和原提交错误同时返回。
- crash reservation 和 ambiguous upload 会永久占用 pending admission，直到有带独立归属证明的恢复工具处理。阈值使成本有限，但一个持续失败的 store 最终会拒绝新写；这是可观察的 fail-stop，不是自动恢复。
- 含 legacy pending state 的 v1/v2 数据库需要离线、带外的归属恢复才能升级；自动迁移只覆盖 referenced-only 的可验证历史。拒绝发生在迁移事务内，不留下半升级 schema。
- garbage queue 只表达 metastore 已证明没有 named 或 detached 节点引用的对象，清扫器不把时间或名字缺失当作 ownership proof。给已脱离节点的旧内容对象增加读者宽限期仍是独立提案。
- 新的 object state 是持久语义；完整性检查、状态统计和契约用例必须共同认识它，未知 state 继续以 `EIO` 失败。
