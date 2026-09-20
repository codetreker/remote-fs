# Agent Note: 有界权威名字观察

Status: implemented

## 问题

按路径列目录会在目录改名或同名替换后重新解析到另一个对象，不能兑现已经打开的目录句柄仍指向原对象。平台适配还需要在一个权威状态中取得完整目录成员、每个成员的属性、目录自身当前名字，以及可用于核对后续名字依赖操作的证据；把这些事实拆成多次查询会留下无法封闭的并发窗口。

目录和名字结果都含有调用方不能预先限制的原始名字与 metadata。实现若先载入完整结果再检查 HTTP body 或调用方容量，会让一个逻辑上有界的接口仍可在 authority、包装层或 client 上无界驻留。无法确认名字绑定时返回 Detached、空目录或猜测路径，也会把损坏或不可达伪装成权威事实。

## 决定

### 应用目录枚举绑定目录身份

`NamespaceAccess.ReadDirNode` 与 `ReadDirNodeBounded` 使用 `DirectoryTarget` 的非零 NodeID 定位目录；可选 Scope 存在时同时验证确切活引用。目标已失效、不是目录或已经 detached 时失败，不回退到旧路径。

一次成功枚举返回同一权威捕获中的全部原始叶名、属性和 `DirectoryObservation{ParentID, Revision}`。Revision 是非空、不透明、只可比较相等的目录名字集合版本；条目按原始名字排序，任一条目或捕获本身无法验证时整份结果失败。普通应用枚举在原生顺序执行 `ReadEntries` Use 检查。

FUSE 的 `Opendir` handle 已持有 NodeReference 与 Scope。第一次 `Readdirent` 通过这个身份调用 bounded 枚举并保存一份 stream；seekdir 与后续读取复用同一捕获。挂载层不再从 inode 的旧路径调用公开 `List`，并继续以捕获开始前的本地 serial 边界清理名字索引，避免较晚学到的成员被旧捕获删除。

### 完整目录 metadata 与引用名字是独立观察能力

`DirectoryMetadataObserver.ObserveDirectoryMetadata` 接受 DirectoryTarget、`DirectoryMetadataOptions{Guards, IncludeName}` 与调用方拥有的 `ListResult`。成功结果的全部 entries、目录 revision，以及请求时可选的目录自身 `NameObservation` 来自同一个权威捕获。它使用独立的 `file.observe-directory-metadata` 授权操作，不从 `ReadEntries`、`ReadMetadata` 或名字修改权限推导准入。

`ReferenceNameObserver` 位于既有 File 与 NodeReference 上，并通过无 I/O 的 `ReferenceIdentity` 核对返回的 NodeID 没有替换引用身份。`ObserveName` 使用独立的 `file.observe-name` 授权操作，返回三种封闭状态：

- `NameRoot` 表示所选 volume 的根，不携带父身份或叶名；
- `NameLinked` 表示当前唯一的父 NodeID 与原始叶名字节；
- `NameDetached` 表示已经验证该节点当前没有名字，不携带父身份或叶名。

缺行、重复绑定、损坏状态、引用失效或无法读取不能解释为 Detached。所有可变 token、叶名、属性和 metadata 在越过接口边界时由接收方拥有。

### guard 与 revision 在同一次读取中核对

`NamespaceGuards` 可以携带目录 revision、确切的 `(ParentID, RawLeaf, ChildID)` 边，以及可选 RootID。每个目录、child 和父／叶槽在一组 guard 中只能出现一次；边不能成环，RootID 存在时全部 guard 必须形成以它为根的可验证祖先关系。调用本身与所有 guards、目标 observation 和可选当前名字在同一 SQLite read transaction 与 publication gate 内核对；任何不符返回 `ErrConditionConflict`，不暴露部分结果。

guards 只进入 ObserveDirectoryMetadata 与 ObserveName。OpenAt、OpenChildRef、NameCommand、FileMutation 与 delete intent 的输入没有 guards，当前路径遍历也不消费这些证据。把 guard 用于修改时必须在最终名字效果的同一事务中增加字段和检查，不能在调用前预检后继续执行既有 mutation。

### revision 持久化并随名字集合推进

SQLite schema v8 为每个当前目录保存八字节、非零的 `directory_revision`；非目录必须保存空值。新目录从 1 开始，成功增加、移除或移动名字时在修改名字的同一事务中推进受影响父目录，跨目录 rename 分别推进两个父目录。失败、回滚和不改变名字集合的 no-op 不推进；达到可表示上限时修改以 `EOVERFLOW` 失败。

revision 进入 Node、新产生的 retained change、snapshot、replica、启动完整性检查和每 volume 的 `metadata_used`。旧 schema 迁移为每个现有目录建立初始 revision；迁移前的 retained history 保持未知 nil，不从历史提交次数猜测版本。token 只证明一次捕获与后续 guard 是否相同，不表达顺序、通知位置或 change-log position。

### 每一层在载入前接受预算

通用契约允许一份目录捕获最多 65,536 个条目、驻留 charge 最多 8 MiB；SQLite 的 `MaxDirectoryEntries` 与 `MaxDirectoryBytes` 可以选择不超过这两个硬上限的更紧值，零值使用硬上限。名字观察按固定状态与真实叶名长度计量。`ListResult` 在生产方载入叶名和 metadata 前逐项 reserve，完整目录 metadata 还在载入可选自身名字前 reserve prefix。调用失败或 collector 失败使整份结果不可读取，不能交付已产生的前缀。

一组 guards 的 directory 与 edge 各最多 256 项，合计驻留最多 64 KiB；单个 revision 最多 64 字节，单个叶名继续受 4096 字节上限约束。HTTP 另外用实际 JSON、canonical base64 和 response envelope 计算调用方的 `ResultBytes` 与 body 上限；server、client 和 native retention 任一上限更紧时，整次调用按更紧者失败。

limited、locked、objectstore、localstore 与 replicated 保留 capability preflight、读取 context、session/reference 生命周期、结果预算和身份复核。replicated 在确认本地副本健康后回源 authority，不从副本或旧路径推断当前名字。HTTP v4 宣告 DirectoryMetadata 与 ReferenceName capability，并以 `file.read-dir-node`、`file.observe-directory-metadata`、`file.observe-name` 三个独立操作转发；严格 DTO 使用 lower-camel 字段与 canonical base64，缺字段、未知字段、身份替换、部分结果或畸形状态都以协议错误失败。

### 范围边界

范围内是 identity-bound 应用 Readdir、独立授权的完整目录 metadata、reference Root／Linked／Detached、持久 directory revision、有限预算及其包装器、HTTP、授权和 FUSE 接入。

范围外是 guarded mutation、当前路径遍历、通知、overflow/rescan、缓存失效与恢复、Windows 名字映射、SMB endpoint 和原生 Windows 验收。

这些延后项保持可加：guards 是独立、平台中立的证据类型，既有 mutation 请求不接受它们，接入时必须在最终名字效果的同一事务增加检查；revision 只可比等，不作为事件游标，观察 API 不建立 watcher，client 不从 token 推导持续缓存；核心保存原始名字，不做 UTF-8、大小写或保留名判定，平台适配在完整目录观察之上执行自己的无歧义检查。

## 备选方案

**只持久化目录 revision，保留按路径 Readdir。** 这能为将来保存 token，但已打开目录句柄仍可能在改名后枚举同名替代目录，调用方也没有一次取得 revision 与完整 entries 的接口。它会让同一事实出现两套枚举路径和预算规则。

**只提供当前名字查询，不提供完整目录捕获。** 这能回答一个引用此刻的绑定，却不能证明目录成员属于同一状态，也无法让平台对整份名字集合做无歧义检查。

**同时把 guards 接入所有名字修改、加入通知和 Windows/SMB。** 这会把只读证据、最终 mutation 顺序、持续输出恢复和平台协议绑成一个审查面。guard 类型与 revision 现在保持平台中立，后续 mutation 以新增字段和最终事务检查接入；通知与平台适配分别使用它们，不改变本次观察结果。

## 后果

已打开目录的应用枚举、完整目录 metadata 与引用当前名字都有身份绑定、权威、有限且 fail-closed 的答案。目录 revision 为后续条件名字操作提供持久证据，Root／Linked／Detached 使平台无需从旧路径或缺失数据猜测当前绑定。代价是 SQLite 增加 v8 持久字段和计量，三项新的授权／HTTP 操作及所有包装层都必须维护独立预算，FUSE directory handle 会为一次枚举保留完整捕获直到 handle 释放。

本决定接续[持久节点身份与原子文件操作](2026-09-20-durable-identity-and-atomic-file-operations.md)预留的目录观察与当前名字边界，并使[中立元数据与访问控制](2026-09-16-neutral-metadata-and-access-controls.md)中的 DirectoryMetadata 与 ReferenceName 协商位成为实际能力。它只部分满足[Windows 系统网络驱动器](../../proposed/feature/2026-09-16-windows-network-drive-support.md)需要的权威名字事实；平台名字规则、guarded mutation、通知、缓存恢复、SMB 和原生 Windows 验收仍由该提案及后续决定拥有。
