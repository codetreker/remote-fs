# Agent Note: 持久节点身份与原子文件操作

Status: implemented

## 问题

普通文件引用已经能跨 rename、unlink 与同名替换保留原对象，但目录 inode 发起的子项操作仍可能重新从旧路径解析父目录。另一个客户端把原目录移走并在旧名字建立替代目录时，先前取得的目录 inode 可能在替代目录里创建、删除或打开子项。一次独立的父身份检查不能封住检查与修改之间的窗口。

平台入口还需要把存在性、对象身份、metadata 条件、创建或替换效果、初始状态、Use claim、删除义务与返回引用作为一个结果。把这些步骤拆开会在并发变化或响应丢失时留下无法安全重投的部分效果。关闭时删除尤其不能只存在于客户端或进程内引用：接受以后，即使连接、进程或 authority 重启，原对象的义务也不能消失或落到同名替代物。

## 决定

### 子项操作以父身份和原始叶名寻址

`DirectoryTarget` 使用非零 NodeID，可选携带一个确切活目录引用的 `UseScope`；`ChildName` 在该父身份下携带不解释平台规则的原始字节叶名。Scope 存在但失效时操作失败，不退回裸 NodeID 或旧路径。每个叶名最多 4096 字节，这一上限同时约束 identity API、基础路径 API 的每个规范化 component、SQLite 持久名字与副本输入。目录必须仍有名字并保持目录种类；detached 目录不接受新子项。

`NamespaceAccess.LookupAt` 查询一个子项，`MutateName` 原子执行创建、建目录、建符号链接、删除、删空目录与改名。`ChildCondition` 统一表达 Any、Absent 或 SameNode，并可对 SameNode 携带多个 metadata namespace 的版本／缺席条件。空 token 表示 namespace 必须缺席，非空 token 逐字节比较。rename 的源槽和目标槽分别检查，最终事务拒绝第三个占据输出名字的对象。

这些操作本身不承担目录 revision、整目录 snapshot 或当前名字观察；[有界权威名字观察](2026-09-20-bounded-authoritative-name-observations.md)以独立只读能力接续这些结果，不改变 mutation 的原子边界。

### 打开、引用与条件效果属于一个权威结果

`AtomicFileOpener.OpenAt` 使用 `ChildSelection` 在同一次原生顺序中解析父身份与叶名，核对可选 `NamespaceGuards` 和 `ChildCondition`，执行 Keep、ResetContent 或 ReplaceNode，应用对应 initial fields，登记 Use claim 和可选关闭删除义务，再返回捕获的 Attr、Outcome 与 File。`NodeReferences.OpenChildRef` 对普通文件、目录和符号链接执行同样的子项选择，`OpenNodeRef` 直接按 NodeID 选择已有节点。guard 接入的决定与边界见[权威子项选择](2026-09-22-guard-authoritative-child-selection.md)。

`NodeReference` 没有字节方法，但编译期保证 `ScopedReference` 与 `ReferenceStateAccess`：成功 preflight 后，调用方一定能取得确切活引用的 Scope，并能原子读取 Attr、符号链接目标、detached 状态和 pending-delete generation。metadata 与删除修改仍按引用的 MetadataAccess 权限及可选能力分别检查。NodeReference 可以声明 ReadData／WriteData 作为与其它入口兼容的 Use claim，这些声明不授予字节方法。

所有三种打开都带 `FileActionID`。ID 使用 FileSession 当前 action epoch 与随机 nonce；同 ID、同输入返回原结果，同 ID、不同输入拒绝。`FileActions.QueryFileAction` 报告 pending、completed、not-executed、unknown 或 retired。只有存在保留记录时 receipt 才带 Operation；没有记录或旧 authority 无法证明原操作时，空 Operation 只与 not-executed、unknown 或 retired 一起出现。unknown 与 retired 都不能解释为未执行。

返回 error 时若 `OpenResult.File` 或 `NodeOpenResult.Reference` 非 nil，清理所有权仍转给调用方。包装器与 HTTP pending-ACK 路径必须保留这个部分结果，不能用后续 Stat 重建原 Attr 或 Outcome。

### 条件修改共用同一套 metadata predicate

`ConditionalFileMutation` 在保留引用上执行 truncate、attribute、WriteAt 或 Append，并允许每一种效果在同一事务中携带 metadata 更新。显式 ExpectedSize、ExpectedMetadata 与 metadata 更新自身的 expected version 都在最终发布事务中比较；已知不符返回 `ErrConditionConflict` 且没有内容、长度、属性或 metadata 效果。Append 在已知内部 revision 竞争后按新的 EOF 重建候选，普通 `File.WriteAt` 与 `Truncate` 不因此增加打开时版本前置条件。

名字源、rename 目标、OpenAt/OpenChildRef、关闭删除义务及立即 pending-delete 使用同一个 metadata predicate 语义。平台客户端由此能把 READONLY 等 opaque metadata 的观察与名字／删除效果放入一个原子判断，storage 不解释平台字段。

### 删除义务具有独立持久身份

`CloseIntent` 带 128-bit 随机、规范小写十六进制 `DeleteIntentID`。它在原子打开中接受并绑定原 NodeID、名字关联、条件与预授权固定效果；相同 ID 不能代表另一个 intent。引用关闭或退役时，authority 在释放 Use claim 和物理 pin 前持久触发或消费该义务。

节点 pending 状态与每个 armed intent 分开保存。显式 `SetPendingUnlink` 原子核对节点、metadata、Use/share、种类与目录为空条件后推进 generation；`ClearPendingUnlink` 必须给出当前 generation，只清除节点当前 pending 状态，不删除其它引用尚未触发的义务。pending 节点拒绝冲突的新打开与名字修改。

删除 intent 的持久状态区分 armed、pending、completed、明确未执行与 cleanup failed；失败状态保存封闭 errno 分类。`QueryDeleteIntent` 以 durable ID 查询，新的 FileSession 在 authority 重启后仍可取得原义务。终态记录继续占用有界历史，只有带独立 `FileActionID` 的 `AcknowledgeDeleteIntent` 可以幂等删除记录并释放容量；ACK 自己进入普通 action replay/query。成功 ACK 后 authority 不保留 tombstone，也不再承诺拒绝该 ID 的重用，后续查询返回 unknown；调用方必须永久不复用已经 ACK 的 DeleteIntentID。名字已与原对象分离或目录触发时非空会成为明确未执行，绝不删除后来占据同名位置的对象。可重试清理失败继续由 authority 持有。

### schema v7 与组合边界

SQLite migration 7 为节点和 change 增加符号链接目标与 pending generation，并持久保存 delete intents、原对象／名字关联、请求摘要、状态和失败分类。link target 与 metadata 共用每 volume 的持久 metadata budget；[有界权威名字观察](2026-09-20-bounded-authoritative-name-observations.md)加入的 directory revision 也进入同一计数。迁移保留 NodeID、高水位、内容、名字、用量和日志事实，继续经过 Commit 与 witness Accept。

`metastore/sqlite` 持有身份解析、条件比较、名字事务、pending 状态与恢复扫描；`storage/objectstore` 持有内容 staging、引用 drain 和条件内容重建；limited、locked、replicated 与 HTTP 包装器逐层转发能力、action、scope、部分打开结果和清理所有权。恢复 orphan/pending cleanup 时，`PublicationAccountingChain` 只复制不可变配额 hook；新 cleanup context 保留自己的 deadline/cancel cause，不暴露原请求的授权、scope、proof 或其它值。`MaintenanceAccounting` 在恢复前绑定当前完整计费链。HTTP v4 使用原 file session registry 与 action epoch，不另建只属于 transport 的正确性来源。

FUSE 的已有 inode Open 使用 OpenNode，Create 使用 OpenAt，Opendir 与 Readlink 使用 OpenNodeRef；Lookup 与 create/mkdir/symlink/unlink/rmdir/rename 使用父目录 NodeID 到达 authority，只有已打开 directory handle 的 Lookup 再附带活 Scope。OpenChildRef 保留给需要原子取得任意子节点引用的编程入口。普通文件 fd 继续持有 File；Readdir 由独立的[有界权威名字观察](2026-09-20-bounded-authoritative-name-observations.md)使用已打开目录的身份与 Scope。组合 Setattr 仍由多个调用组成，ConditionalFileMutation 没有被 FUSE 冒充为整项事务。

## 备选方案

**先检查父 NodeID，再执行路径操作。** 检查和最终修改之间仍能发生 rename 或 replacement，无法阻止操作落到替代目录。

**只在 HTTP 保存 action journal。** 直接 Go 调用仍无法安全重投，且 transport 成为原生操作语义的唯一来源。动作身份和查询因此进入 FileSession 契约，HTTP 只转发。

**把关闭删除只绑在活引用上。** 进程或 authority 重启会遗忘已经接受的义务，违反已经返回给客户端的结果。删除 intent 使用独立持久 ID；普通引用与 Scope 仍按 authority incarnation 失效。

**同时交付目录 metadata 与当前名字观察。** 这些是可增量增加的只读能力，会引入独立的结果预算、授权与 wire 形状；本决定只保留其所需的父身份与原始名字形状，不提前交付观察能力。

## 后果

目录子项操作不再依赖可能过时的父路径，创建、替换、初始 metadata、Use claim、删除义务与返回引用具有一个可核对的原子结果。代价是 OpenAt/OpenNodeRef/OpenChildRef 与新 mutation 都需要 action ID，SQLite 增加 v7 持久状态，关闭可能执行持久名字效果并返回清理错误，所有包装层都必须保留部分结果和查询语义。

本决定部分取代[文件目标、显式内容依据与目录父身份](../../proposed/architecture/2026-08-20-nothing-pins-an-open-file.md)中的目录父身份部分。该提案关于 R-CC-1 调用方显式内容版本依据的部分仍是 proposed；这里的 metadata/size 条件和内部 content revision 都不宣称完成那项工作流。

[有界权威名字观察](2026-09-20-bounded-authoritative-name-observations.md)另行交付目录 metadata observation、reference current-name 和只读 guard；[权威子项选择](2026-09-22-guard-authoritative-child-selection.md)随后把这些 guards 接入 OpenAt 与 OpenChildRef，不改变这里定义的 action 与原子效果边界。[安全且有界的本机 SMB 端点](2026-09-21-secure-bounded-smb-endpoint.md)另行交付协议、安全会话与 share 生命周期。其余 guarded mutation 与 Windows 文件／名字映射仍未交付，系统不因此宣称 Windows 支持已经完成。
