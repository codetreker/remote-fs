# Agent Note: 以权威观察约束子项选择

Status: implemented

## 问题

平台入口可以从一次完整目录观察中判断一组原始名字是否能够无歧义表示，再选择其中一个叶名执行打开或创建。`ChildCondition` 只能核对所选槽位是否缺席、是否仍指向同一节点及其 metadata 条件；它不能证明同目录中的其它名字、祖先目录 revision 或已观察的名字边仍与作出选择时一致。

若先调用观察接口核对 `NamespaceGuards`，再以独立的 `OpenAt` 或 `OpenChildRef` 选择子项，两个调用之间的名字变化仍可能让最终动作使用已经过期的平台映射。失败前已经清空或替换内容、登记 Use、保留引用或接受关闭删除义务，会留下不能作为一次失败整体回滚的部分效果。

## 决定

### 子项选择携带权威证据

`ChildSelection` 把一个 `ChildName` 与可选 `NamespaceGuards` 组成同一个值。`AtomicFileOpener.OpenAt` 与 `NodeReferences.OpenChildRef` 接受该值；没有 guards 时保持按父身份和原始叶名选择子项的原有语义。

guards 证明调用方据以选择这个叶名的目录 revision、确切 `(ParentID, RawLeaf, ChildID)` 边和可选根关系仍成立。`ChildCondition` 继续独立证明所选槽位为 Absent、Any 或 SameNode，并在 SameNode 时核对 metadata predicates。两者约束不同事实，任一不符都返回 `ErrConditionConflict`。

SQLite 的有界 preflight 只验证父 `DirectoryTarget` 和可选 Scope，不提前核对 guards 或 child facts。最终 authority transaction 在同一 publication gate 内再次验证父／Scope，再一次性核对全部 guards，随后选择叶名并核对 `ChildCondition`。只有这些事实确定后才推导本次打开实际需要的 publication intent；Keep 打开已有对象不制造发布，创建、清空、替换或接受 `CloseIntent` 等实际 mutation 按推导出的 intent 执行。guard 冲突返回零值结果，不留下节点、内容、引用、Use claim 或删除义务。与名字集合无关的内容和 metadata 修改不推进 directory revision，因此不会使仍然成立的目录观察失效。

### 选择值属于动作身份

公开入口先对调用方给出的 `ChildSelection` 执行结构与上限检查，再分配或深拷贝其叶名、Scope、revision 与 edge 名字。objectstore 与 HTTP 共用集中式 canonicalization：directory guards 按 ParentID、edge guards 按 ParentID／RawLeaf／ChildID 排序，使 guards 成为顺序无关的语义集合。只重排 guards 后复用同一 `FileActionID` 返回原结果；改变任一 guard 事实、ChildName 或目标条件则以 `EINVAL` 拒绝。

原动作已经完成时，重投返回保存的结果，不按重投时的 namespace 重新执行或重新核对 guards。响应丢失后发生的目录变化不能把一次已完成打开改报为冲突，也不能制造第二个引用或再次接受副作用。

### 包装层与 HTTP 保留同一选择

limited、locked、objectstore、localstore 与 replicated 包装层转发完整 selection，不从本地副本、缓存或旧路径构造 guard。replicated 仍把请求送到 authority，并以原 mutation barrier 确认成功结果；SQLite replica 的本地 directory revision 不能作为 `NamespaceGuards`。

HTTP v4 的 `file.open-at` 与 `file.open-child-ref` 保留既有顶层 `child`，并增加可选的顶层 `guards`；server 在严格解码后把二者组装成 `ChildSelection`。canonical base64、每类 256 项、64 KiB guard retention、64-byte revision 与 4096-byte leaf 上限在授权和 native action 前检查。guards 不改变基础 Operation、`OpenAccess` 或补充效果的授权序列，也不成为业务身份。

FUSE 把 Linux 的精确字节叶名包装成不带 guards 的 `ChildSelection`。它不从 replica 本地 revision 推导权威证据；当前路径遍历与 `MutateName` 的行为不变。

### 范围边界

范围内是 `OpenAt` 与 `OpenChildRef` 对平台中立目录观察的最终核对，以及这些调用的公开类型、原生事务、动作历史、包装层、HTTP 和副本转发。guarded open 接受的 `CloseIntent` 与创建、清空、替换等打开效果一起受同一次选择约束。

范围外是 `LookupAt`、`MutateName`、`NameCommand`、`FileMutation`、显式 pending-delete 命令、当前路径遍历、通知、overflow/rescan、缓存恢复、Windows 名字映射、SMB 文件命令和原生 Windows 验收。后续操作不能把本决定解释为所有名字修改都已经 guarded；每个新增入口仍须在自己的最终名字效果事务中核对证据。

## 备选方案

**先观察，再调用原有打开。** 两个调用之间仍有名字变化窗口。它只能证明预检时成立，不能证明产生引用或名字效果时成立。

**同时把 guards 接入全部名字和文件修改。** 每个操作的效果、授权、动作回执和失败面不同；在同一次改动中扩展所有入口会把可独立审查的保证重新绑成一个大范围。其余入口保持现有输入，后续分别增加最终事务检查。

**让 authority 直接执行 Windows 名字比较。** 这会把大小写、Unicode 与保留名规则施加给 Linux 和编程入口，并要求每个 storage 实现理解客户端平台，因此不选。authority 只核对平台中立的 revision、原始边和根关系。

## 后果

平台入口可以把一次完整目录观察转成一个原子、可核对的子项选择；观察以后发生的任何相关目录 revision 或 edge 变化都会使新动作在产生部分效果前冲突。代价是所有 `AtomicFileOpener`、`NodeReferences` 实现和 HTTP peer 都迁移到 `ChildSelection`，且目录中与目标无关但会推进 revision 的名字变化也会保守地拒绝旧选择。

本决定部分接续并取代[有界权威名字观察](2026-09-20-bounded-authoritative-name-observations.md)中“guards 只进入只读观察”的范围边界，也扩展[持久节点身份与原子文件操作](2026-09-20-durable-identity-and-atomic-file-operations.md)定义的子项选择输入；两份决定的 observation、身份、action 与原子效果语义保持不变。它只交付[Windows 系统网络驱动器](../../proposed/feature/2026-09-16-windows-network-drive-support.md)所需的中立选择保证，不交付 Windows 名字规则、其它 guarded mutation、SMB 文件命令、通知、缓存恢复或原生验收。
