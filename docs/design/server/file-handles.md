# 保留文件、metadata 与访问控制

本文描述 `storage.FileStorage`、保留的节点身份、原子子项操作、名字与目录观察、中立 metadata、删除义务、使用声明与范围控制。按路径的基础 volume API 见[顶层设计](../architecture.md)，显式 S/X 扩展见[文件占有](file-locks.md)，Linux 的平台解释见[client 设计](../client/architecture.md)。既有引用与内容语义由[活跃文件句柄](../../../.agents/notes/implemented/architecture/2026-09-08-live-file-handles.md)拥有；能力的取舍见[中立元数据与访问控制](../../../.agents/notes/implemented/architecture/2026-09-16-neutral-metadata-and-access-controls.md)、[持久节点身份与原子文件操作](../../../.agents/notes/implemented/architecture/2026-09-20-durable-identity-and-atomic-file-operations.md)及[有界权威名字观察](../../../.agents/notes/implemented/architecture/2026-09-20-bounded-authoritative-name-observations.md)。

## 一、身份与会话

[`storage.FileStorage`](../../../packages/storage/files.go) 在 `BoundedStorage` 之外提供 `CheckFileStorage` 与 `NewFileSession`。检查必须在挂载或提供能力前完成；不能用按旧路径重新打开来替代保留身份。随附实现通过 objectstore 与具有原生独占所有权的 SQLite metastore 提供此能力。只有共享数据库所有权的基础 SQLite API 继续提供路径操作，文件能力检查以 `EOPNOTSUPP` 拒绝。

`FileSession` 拥有有限时长、文件与节点引用、在途操作、use owner、范围动作历史和文件动作历史。`OpenFile` 以路径解析目标，`OpenNode` 直接使用节点 ID；`OpenAt` 在一个目录身份下原子选择普通文件；`OpenNodeRef` 与 `OpenChildRef` 返回没有字节方法的 `NodeReference`。`StatNode` 与 `SetNodeAttr` 继续提供短调用形式。路径变化不改变已经返回的 File 或 NodeReference。打开不读取完整内容，也不自动取得 advisory range 或 S/X grant。

| 打开条件 | 权威结果 |
|---|---|
| `ExpectedID` 与实际目标不符，或 `OpenNode` 的身份已不存在 | `ESTALE`，不改用同名新节点 |
| `Create` 且 `Exclusive`，目标已存在 | `EEXIST` |
| 非排他创建遇到并发创建者 | 打开已经存在的那个对象，保留它已有的 metadata |
| `Truncate` | 需要写权限；截断与返回的身份属于同一次有序打开结果 |
| 普通文件引用用于目录或符号链接 | 分别为 `EISDIR`、`ELOOP` |
| `OpenAt` / `OpenChildRef` 的父 Scope 已失效 | `ESTALE`，不退回裸 NodeID 或路径 |
| SameNode 或 metadata 条件不符 | `ErrConditionConflict`，不取得引用、不登记 Use、不接受删除义务 |

`FileOpenOptions` 嵌入 `storage.OpenAccess`，共享 Read、Write、Create、Truncate、Exclusive 五项打开意图；ExpectedID、InitialMetadata 与 Use 是旧路径打开的参数。`OpenAtOptions` 另用 Keep、ResetContent、ReplaceNode 表达已有目标效果，按实际分支应用 initial fields。创建、排他判断、目标身份与 metadata 条件、清空或替换、Use claim、关闭删除义务、返回 Attr/Outcome 和引用保留属于同一次权威结果。

打开自动把 Read/Write 转为 `ReadData` / `WriteData`，再与显式 Use 合并。`UseClaim{Uses,Deny}` 与同一节点上的其它 claim 双向比较；任一 Deny 与对方 Uses 相交时，新打开在取得引用前以冲突失败。Use 不授予 File 方法、业务权限或 Strong proof。

`NodeReference` 只提供属性、Scope、State 与 Close；它不提供 ReadAt、WriteAt 或 Truncate。`NodeRefOptions.MetadataAccess` 控制属性与 metadata 权限，Use 可声明 ReadData／WriteData 以参与其它入口的兼容性检查，但不会据此增加字节方法。NodeReference 编译期包含 `ScopedReference` 与 `ReferenceStateAccess`，因此通过 `NodeReferences` preflight 后不会在取得目录引用后才以 `EOPNOTSUPP` 拒绝 Scope 或 State。

`FileSessionOptions` 要显式选择有效值，调用方可从 `DefaultFileSessionOptions` 开始。默认 lease 为 30 秒、动作历史为 1 分钟、单文件大小为 1 GiB，每会话最多 4096 个引用、64 个活跃操作、256 个操作等待者、4096 个 use owner、65536 个范围、1024 个 pending range action 与 16384 个范围动作。会话上限还受 volume 与 HTTP registry 的共享上限约束。

`Renew` 确认会话继续有效；`Status` 只观察，不续期。返回的 epoch、revision、剩余 lease 与 history 时间用于核对同一会话。client 从请求开始时刻计算保守的本地截止时间；旧响应、普通 I/O 成功、TCP 存活均不延长已确认期限。过期或旧 server epoch 的能力返回 `ESTALE`，未知结果返回 `EIO`，不会恢复到旧路径。底层发布检查仍是引用、Uses、range 与 Strong 权限的最终判定者。

### 可选接口

每种能力的 Check 方法检查完整包装链；不支持时，Check 和调用都返回 `EOPNOTSUPP`。

| 接口 | 当前责任 |
|---|---|
| `AtomicFileOpener` | 按目录身份原子打开、创建、清空或替换普通文件 |
| `NamespaceAccess` | 按父 NodeID/Scope 执行 LookupAt、完整有界 ReadDirNode 与 MutateName |
| `NodeReferences` | 按 NodeID 或父身份打开普通文件、目录及符号链接的 NodeReference |
| `ScopedReference` / `ReferenceStateAccess` | 返回确切活引用的 Scope，以及同次捕获的 Attr、link target、detached/pending 状态 |
| `DirectoryMetadataObserver` | 在独立授权下返回完整 entries、directory revision 与可选目录当前名字 |
| `ReferenceIdentity` / `ReferenceNameObserver` | 核对保留引用的固定 NodeID，并观察 Root、Linked 或 Detached 当前绑定 |
| `FileActions` | 核对 session 内有限 action receipt，查询并显式 ACK durable 删除终态 |
| `MetadataAccess` | 按 NodeID 对一个 metadata namespace 作 CAS |
| `ReferenceMetadataAccess` | 通过保留 File 对一个 metadata namespace 作 CAS |
| `UseOwners` | 以有效 File scope 注册和退役 range owner |
| `RangeControl` | 查询冲突、批量编辑、核对、取消和按 domain 清理范围 |
| `DeleteIntent` | 设置或按 generation 清除节点 pending deletion |
| `ConditionalFileMutation` | 在最终发布处比较 size/metadata 条件并修改内容或属性 |

Go 的 NamespaceAccess 与 DirectoryMetadataObserver 可以独立实现。HTTP v4 的 DirectoryMetadata capability 是一个 transport bundle gate：server 只有在 FileSession 的完整包装链同时通过 CheckNamespaceAccess 与 CheckDirectoryMetadataObservation 时才宣告 true，remote FileSession 的 ReadDirNode、ReadDirNodeBounded 与 ObserveDirectoryMetadata 都要求它。Namespace bit 继续单独表示 LookupAt 与 MutateName。ReferenceName 在 File/NodeReference 上独立宣告。能力缺失时不能由路径查询、副本或缓存模拟。

## 二、保留节点、名字与回收

SQLite schema v8 在 v7 的符号链接目标、节点 pending generation 和 durable delete intents 之外，为每个目录保存持久、非零的名字集合 revision。`Remove` 或覆盖目标的 `Rename` 移除普通文件名字时，有引用的文件成为 detached 并保留原 NodeID、属性与内容；目录和符号链接的 NodeReference 同样固定身份，名字删除须遵守其 pending/引用条件。volume 日志、快照与目录遍历只包含仍有名字的节点，detached 文件的后续修改不制造虚构路径事件。

保留节点的内容仍属于 volume 的实际用量。最后一个引用先退役，在最终发布门处禁止新的修改授权；已经接纳的 I/O 排空之后才物理释放。最后释放在事务内处理用量、当前对象与待回收对象。已知未生效的容量拒绝保留引用供清理重试；结果不明时保留所有权并封锁后续使用，不能提前归还配额。

`SQLiteOptions.MaxRetainedFiles` 默认 65536，按 volume 共享。退役引用仍占物理名额，直到最后释放完成。满额时创建并打开必须在产生 volume 副作用之前拒绝。多个 objectstore 包装同一 volume 时使用同一份引用、Use 与 range 预算，不能借另建包装器绕过上限。

拥有数据库原生 EX 锁的启动过程先有界验证所有 volume，包括 detached/pending 节点、delete intent、对象关系和配额。没有 durable obligation 的旧 epoch 无主引用可以回收；armed 或 pending 删除责任保留到 Strong 恢复屏障建立后，经正常发布门重试。使用量、垃圾记录和提交代际通过同一个 Commit/Accept 结果生效。验证失败不先回收一部分；普通共享 opener 不执行恢复清理。持久见证、数据库所有权和关闭失败的处理见[本地持久对象存储](local-disk-object-store.md)。

### 身份 namespace

`DirectoryTarget` 使用父 NodeID，并可携带该目录确切活引用的 Scope。`ChildName.RawLeaf` 是最多 4096 字节的原始单段名字；空值、`.`、`..`、斜杠和 NUL 被拒绝，不加入 Windows 或 UTF-8 规则。基础路径 API 的 `CleanPath` 对每个规范化 component 使用同一上限，SQLite 持久名字和 replica ingest 也在载入或提交前核对它。`LookupAt` 只查询该槽位。`MutateName` 的创建、建目录、建符号链接、删除、删空目录与 rename 在一个 SQLite transaction 中重新核对父身份、源 `ChildCondition`、rename 目标条件、metadata predicates、Use 与 Strong publication。

`ChildCondition` 的 Any 不要求目标身份，Absent 要求槽位缺席，SameNode 同时要求非零 NodeID；metadata 条件只能与 SameNode 一起使用。空 token 要求 namespace 缺席，非空 token 要求版本相等。已知不符返回 `ErrConditionConflict` 且零效果。rename 的 source leaf、destination observed leaf 与 output leaf 分开表达，既验证调用方观察的替换对象，也拒绝输出名字被第三个对象占据。

公开 List/ListBounded 保持路径入口；FileSession 的 ReadDirNode/ReadDirNodeBounded 以 DirectoryTarget 执行身份绑定的应用枚举，并在原生顺序检查 `ReadEntries`。两者都返回完整 entries，但只有身份入口同时返回该捕获的 directory revision。

### 名字与目录观察

`DirectoryObservation{ParentID, Revision}` 标识一次完整目录捕获。Revision 是非空、不透明、只可比较相等的 token；新目录从 1 开始，成功改变名字集合的 create、remove 或 rename 在同一事务中推进相应父目录。失败、回滚和不改变集合的 no-op 不推进；跨目录 rename 分别推进两个父目录，耗尽可表示范围时以 `EOVERFLOW` 拒绝修改。

`DirectoryMetadataObserver.ObserveDirectoryMetadata` 接受 DirectoryTarget、`DirectoryMetadataOptions{Guards, IncludeName}` 与空的 caller-owned ListResult。成功时 entries、revision 与可选目录自身 NameObservation 来自同一 publication gate 和 SQLite read transaction。它使用独立授权，不从 `ReadEntries`、`ReadMetadata` 或名字修改权限推导。生产方在载入叶名和 metadata 前逐项 reserve；请求自身名字时先 reserve prefix。任一错误使 collector 和 observation 整体失败，不允许读取已产生前缀。

File 与 NodeReference 的 `ReferenceNameObserver` 复用既有 session、引用生命周期与固定 NodeID。Root 表示所选 volume 根，Linked 携带当前唯一父 NodeID 与原始叶名字节，Detached 表示已经验证没有当前绑定；Root 与 Detached 不携带父身份或叶名。缺行、重复绑定、损坏或不可达都失败，不能转成 Detached。

`NamespaceGuards` 携带最多 256 个目录 revision、256 条确切 `(ParentID, RawLeaf, ChildID)` 边和可选 RootID，合计驻留最多 64 KiB。重复目录、重复 child、重复父／叶槽、cycle 或无法到达 RootID 的关系在访问 storage 前拒绝；每个 revision 最多 64 字节，叶名继续受 4096 字节上限约束。有效 guards 与目标 observation 在同一次读取中比较，不符返回 `ErrConditionConflict` 且无部分结果。guards 只进入 ObserveDirectoryMetadata 与 ObserveName；OpenAt、OpenChildRef、NameCommand、FileMutation 和 delete intent 不接受它们。

通用契约允许一份目录捕获最多 65,536 个 entries，native retention charge 最多 8 MiB；SQLite 的 `MaxDirectoryEntries` 与 `MaxDirectoryBytes` 可配置为不超过硬上限的更紧值，零值选择默认硬上限。名字观察按固定状态与真实叶名长度收费。directory revision 随 authority 的当前 Node、新 change 与原生 snapshot 传播并计入 `metadata_used`。HTTP v4 replication 不传该字段；SQLite replica 为缺失 revision 的目录维护本地 opaque token，并在本地 replay 名字变化时替换它，观察 API 不返回这份非权威状态。v8 迁移清空没有可信 revision 的旧 retained history，同时切换 log incarnation 并把窗口位置归零，使持有旧游标的 replica 明确 reseed。revision 不是通知游标或 change-log position，观察接口不建立 watcher，也不提供缓存恢复。

### 文件动作与结果核对

每个 OpenAt、OpenNodeRef、OpenChildRef、MutateName、条件文件修改及 pending-delete 修改都携带 `FileActionID`。ID 由当前 FileSession 的 action epoch 与随机 nonce 构成；相同 ID 只接受完全相同的输入。重复请求返回原方法结果，不重复创建引用、应用 mutation 或接受删除义务。

`FileActions.QueryFileAction` 返回 pending、completed、not-executed、unknown 或 retired。保留记录携带规范 `storage.Operation`；找不到或无法归属旧记录时 Operation 为空，且只能与 not-executed、unknown 或 retired 配对。history 到期、session 退役或 authority 重启后，有限 action receipt 可以变成 retired/unknown；这两者都不是安全重投的未执行证明。

打开返回 error 但仍带非 nil File/NodeReference 时，调用方拥有该引用并须继续清理。Attr 与 OpenOutcome 属于原 action receipt，不能用后续 Stat 猜测。HTTP pending ACK、limited、locked、objectstore 与 replicated wrapper 都保留同一所有权。

### pending deletion 与 durable intent

`CloseIntent` 在打开事务中接受，携带由 `NewDeleteIntentID` 生成的 128-bit 小写十六进制 durable ID，并绑定原 NodeID、名字关联、metadata 条件、Use 与文件／空目录条件。它仍处于 armed 时不等于节点已经 pending；引用显式关闭、session 清理或 authority 接管旧责任时才触发。触发时名字已经 unlink/replacement 分离，或目录非空，则该 intent 进入明确未执行终态，不能作用于同名替代物。

`SetPendingUnlink` 立即在同一事务中核对引用、metadata、Use/share、节点种类与目录为空条件，再推进非零 pending generation。`ClearPendingUnlink` 必须给出当前 generation；竞争或旧 generation 返回 `ErrConditionConflict`，且清除一个节点状态不会删除其它 armed intent。pending 节点拒绝冲突的新打开和名字操作，已有相容引用继续按其权限访问。

delete intent 持久记录 armed、pending、completed、not-executed 或 cleanup-failed；失败记录携带封闭 errno 分类。终态记录继续占用有界历史，直到 `AcknowledgeDeleteIntent` 以自己的 FileActionID 幂等删除记录并释放容量；ACK 也可通过 QueryFileAction 核对。成功 ACK 后不保留 tombstone，后续 QueryDeleteIntent 返回 unknown，authority 不再承担该 ID 的非复用保证；调用方必须永久不复用已经 ACK 的 ID。新 session 可在 authority 重启后查询尚未 ACK 的原义务；普通 File、NodeReference、Scope、Use owner 与 range 不随这份持久记录恢复。

`SQLiteOptions.MaxDeleteIntents` 默认每 volume 65536 条，计入 armed、pending、失败及尚未 ACK 的终态。达到上限时，新 CloseIntent 在产生打开或名字效果前以 `EAGAIN` 拒绝；清理、查询和 ACK 保持可用，使已接受责任能够终结并释放名额。

## 三、一次读取与一次修改

`File.ReadAt` 在同一份 `FileState` 下返回属性与区间字节，EOF 返回该状态的属性与空字节。多次读取可以看见同一对象后续已经完成的修改；一个成功返回的区间不会混合两份 revision。文件引用钉住节点，不钉住某个内容 revision。属性包含 `NodeKind`、共同时间和 opaque metadata；平台权限不参与内容 revision。

objectstore 仍以不可变完整对象保存内容。读取先捕获节点状态，在分配前预留当前对象与返回区间的内存，再通过 `GetBounded` 取对象并切片。捕获的旧对象被并发替换并回收时重新读取状态；当前仍引用的对象缺失是 `EIO`，持续竞争耗尽有限尝试是 `EAGAIN`。节点引用没有消除[读取与清扫之间的竞争](../../../.agents/notes/proposed/architecture/2026-08-21-readers-in-flight-and-the-sweeper.md)。

`WriteAt` 只替换指定区间；`Truncate` 保留前缀，增长部分为零。一次修改读取当前完整状态，预留旧内容与下一份内容，构造替换对象，经 Reserve、不可变 Put 和原生 revision CAS 发布。对象上传不持有最终发布门。只有已知没有提交且暂存清理成功的 CAS 竞争才能重新基于当前状态尝试；真实故障或未知提交结果不会被重试掩盖。

最终事务同时核对内容 revision、节点身份、会话或引用的有效期、Use claim、强制范围、显式 S/X proof 和发布记账。检查与修改按同一次最终转换排序。上传开始时有效不代表上传结束时仍可发布；引用退役、会话失效或授权过期后，尚未取得最终授权的修改失败。原生 Commit/Accept 的未知结果继续为 `EIO` 并保留故障原因。

两个普通 fd 对重叠区间的修改可按实际提交顺序都成功，未被后一次修改触及的区间被保留。内部 revision CAS 负责拼接当前状态，不是对「打开时内容版本」的承诺；[显式内容版本工作流](../../../.agents/notes/proposed/architecture/2026-08-19-ordering-and-versions.md)仍有独立的调用方依据与冲突报告问题。

`ConditionalFileMutation` 是独立的原子条件入口。Truncate、attribute、WriteAt 与 Append 都可携带同事务 metadata 更新；ExpectedSize、ExpectedMetadata 与更新 payload 的 Version 在最终 publication 中比较。已知不符返回 `ErrConditionConflict`，内容、长度、属性和 metadata 均不修改。Append 在内部 content revision 竞争时重新捕获当前 EOF 并有界重建候选；这仍不是 R-CC-1 的通用内容版本 token，普通 File 修改也不继承这些显式条件。

内容或长度修改由 authority 推进 `ModTime` 与 `ChangeTime`。`BirthTime`、`AccessTime` 和 `ModTime` 可按 `AttrChange` 显式设置；`ChangeTime` 不可由调用方指定，并在共同属性、metadata 或名字关联确实变化时由 authority 推进。零 `time.Time` 是合法显式值，nil 才表示不修改。

`Sync` 检查已完成修改的健康与持久性边界。直接 `File.Close` 退役引用及其 Use claim，不推断 POSIX 进程 owner，也不提交本地 dirty 内容。每次 `WriteAt`、`Truncate` 的错误都落在对应调用上；之后的 `Close` 不能把已经返回成功的修改丢弃。

### metadata namespace

`Attr.Metadata` 是 `map[string]OpaquePayload`。namespace 只允许小写 ASCII 字母、数字、点、下划线和连字符；每节点最多 16 项，key 最长 128 字节，单值最长 32 KiB，规范编码总长最多 64 KiB，版本 token 最长 64 字节。编码按 key 排序并带长度前缀；畸形、乱序、重复、超限或尾随数据使整份属性不可用。

`MetadataAccess.SetMetadata(node, namespace, expectedVersion, data)` 和引用上的 `ReferenceMetadataAccess.SetMetadata` 都只改变一个 namespace。空 expected version 要求该 namespace 缺席；非空值必须逐字节等于当前 authority version。空 data 是存在的 payload。成功结果携带新分配的非空版本，保留其它 namespace，并推进 `ChangeTime`；已知条件不符以 `ErrConditionConflict` 证明没有修改。同一 predicate 也用于 ChildCondition、OpenAt/OpenChildRef、名字修改、pending deletion 与 ConditionalFileMutation，平台字段不进入 authority 的解释。

metadata 返回值与 Attr 载入前先经过 `AttrResultBudget`。每 volume 的 SQLite `MaxMetadataBytes` 默认 64 MiB，包含当前／detached 节点与 retained change 的规范 metadata、link target 和 directory revision；它独立于内容 quota、单节点上限和 HTTP 的驻留预算。

## 四、使用声明、范围与 owner

[`packages/advisory`](../../../packages/advisory/coordinator.go) 在同一 volume 内协调三个独立 domain。`DomainRecord` 与 `DomainWholeFile` 只约束参与者；`DomainEnforced` 的显式 policy 约束真实数据访问。普通 File 的 Use claim、owner、range、动作历史与会话生命周期都由同一个 volume coordinator 计量，不能通过再建包装器绕过。

公开 List/ListBounded 在 authority 的 native 读取顺序中派生 `ReadEntries`，因此 replicated wrapper 不能从本地目录副本直接回答。它先检查副本连续性，再把完整目录读取交给远端；失效副本仍以 `EIO` 失败，健康副本也不能绕过当前 deny。

| domain | 编辑 | 作用 |
|---|---|---|
| `DomainRecord` | `Replace`、`Subtract` | 传统记录锁的范围替换、分割和解除 |
| `DomainWholeFile` | `Replace`、`Subtract`，可选择 `DropBeforeAcquire` | 整文件 advisory 的转换与解除 |
| `DomainEnforced` | `AddExact`、`RemoveExact` | 独立 ClaimID 和显式 `DenySelf` / `DenyOthers`，约束 `ReadData` / `WriteData` |

`Bytes` 使用 unsigned Start 和正 Length；`Boundary` 使用独立 CutAt，不从零长度猜测 EOF。`RangeShared` / `RangeExclusive` 只表达冲突关系，核心不推断 SMB 或 POSIX 的重复获取、转换和解除政策。平台 adapter 负责选择命令，authority 负责按同一最终顺序执行冲突检查与受控 I/O。

`UseScope` 绑定一个确切 File 或 NodeReference。token 必须非空、不超过 128 字节、不含 NUL 且是有效 UTF-8；它只作原样相等比较。DirectoryTarget 携带 Scope 时同时核对 NodeID、session、引用存活与目录种类，失败不降为裸身份访问。`UseOwners.NewUseOwner` 同样核对 NodeID、scope、session 和引用存活；内部 UseOwner 只在所属 session 的控制请求中定位状态，不授予额外权限，也不作为冲突持有者身份返回。`OwnerReference` 随引用结束，`OwnerExplicit` 由调用方明确退役。Group 只合并同一 session 内的死锁参与者，不共享 claim、range 或 scope 豁免。

`OwnerOptions.Diagnostic` 是调用方拥有的不透明 `uint64`，唯一用途是作为 `RangeConflict.Owner` 报告另一持有者。它不参与 owner 身份、授权、scope、Group、range 所有权或清理。冲突可以跨 session 返回相同 Diagnostic；调用方负责解释它，authority 不用内部 UseOwner 编号补缺或替代。

`RangeControl.GetConflict` 只查询一个实际冲突。`Apply` 一次接纳最多 64 条命令，返回的 Claims 与 Effects 也分别最多 64 项；完整回执在任何释放或授予前完成容量验证。Rejected 结果的 `FailedAt` 是原 Commands 中失败项的零基下标；request-wide admission 拒绝没有这个位置。此前成功的 release 保留在 Effects 中，本批 acquisition 回滚。`DropBeforeAcquire` 已释放的旧范围即使随后获取 Pending 或 Rejected，也不能被隐藏成完全未执行。

`Apply`、`Query` 与 `Cancel` 使用原有 `LockRequestID` epoch 和 nonce。相同 ID 的不同 intent 以 `EINVAL` 拒绝；旧 epoch 中未见过的 ID 不重新执行。取消只有在结果证明没有遗留 grant 时才能成为安全的中断；授予已经获胜时返回该事实，结果未知时相关访问持续失败。

Use claim、owner、range、等待与普通文件 action history 只存在于当前 authority 的有界内存中。FileSession 退役、authority 重启或 incarnation 改变后，旧 File、NodeReference、scope、owner 和 range 均以 `ESTALE` 或相应不可用错误失效，不从 SQLite 或复制日志恢复，也不按同名或同 NodeID 对象静默重建。调用方须建立新 FileSession 并重新申请状态。delete intent 与独立 Strong S/X 各有自己的持久恢复保证，不把旧引用恢复为活能力。

FUSE 将 `flock` 映射到 whole-file domain，将传统 POSIX `fcntl` 映射到 record domain。record owner 注册时把内核提供的 POSIX PID 写入 Diagnostic；kernel owner cookie 与内部 UseOwner 不越过这条映射。Linux `pid_t` 是有符号值，`F_GETLK` 只把 1 至 `math.MaxInt32` 的 Diagnostic 转成 PID，缺失或越界为 `EIO`。fork/dup、访问模式、转换及关闭规则仍留在 FUSE：`Flush` 对对应 owner 执行 `Drop`，最终 `Release` 关闭引用。直接 File API 不推断 POSIX 进程 owner。完整 `F_OFD_*` 仍不在兼容承诺内。

默认 volume 上限为 1024 个会话、32768 个 owner、262144 个 range、262144 个 action、8192 个 waiter 和 65536 条死锁图边。会话数据、心跳、范围获取、核对和释放使用分开的 admission；数据物化不能耗尽续期与清理能力。

## 五、HTTP、复制与资源

HTTP 文件请求先执行[业务授权](authorization.md)，再读取或触碰 Session、File、NodeReference、动作历史或 durable intent。OpenAt/OpenNodeRef/OpenChildRef 先授权自身 Operation 和导出的 OpenAccess，再按固定顺序授权实际包含的 remove、set-attr、set-metadata 或 set-pending 效果；全部允许后 native action 才执行。LookupAt、ReadDirNode、ObserveDirectoryMetadata、ObserveName、MutateName、条件 mutation、pending set/clear、action/intent query 与 intent ACK 分别使用自己的规范 Operation。已有 bearer 引用、action ID 或 durable intent ID 都不能绕过当前请求授权；authority 自主完成已经接受的固定删除效果时不重新解释成外部请求。

HTTP v4 统一转发基础 volume、中立 Attr、metadata、文件引用、目录／名字观察、range 和强 S/X。请求的 `op` 直接使用 `storage.Operation` 的规范值；二进制内容、原始叶名、revision、metadata version 和 payload 使用 canonical base64。协议拒绝未知、重复、缺席、null 或无关字段，所有结果都携带 v4 marker 与封闭 errno 词汇；v3 路由不提供兼容旁路。

server 的 session 能力宣告 AtomicOpen、Namespace、References、FileActions、Metadata、Owners、Ranges 与 DirectoryMetadata；DirectoryMetadata 只在 NamespaceAccess 和 DirectoryMetadataObserver 的完整 backing chain 都可用时为 true。remote client 用这个 bit 同时 gate ReadDirNode 与 ObserveDirectoryMetadata，Namespace bit 只覆盖其余 identity namespace 操作。File 与 NodeReference 按实际方法宣告 Metadata、Scope、State、Delete、Conditional 与 ReferenceName。v4 client 只在对应 bool 为 true 时暴露可选接口，任意未知 capability 字段仍是协议错误。

OpenAt、OpenNodeRef 与 OpenChildRef response 携带 storage action 捕获的 node 与 outcome；旧 Open/OpenNode 保留原有 transport journal 与 ACK 形状，不因此取得 storage `FileActionID`。File/NodeReference.Close 和 FileSession.Close 可携带清理产生的 barrier。client 必须验证新原子打开的引用身份与原 action 一致，不能用一次新的 Stat 填补缺失字段。

随机能力标识会话与文件／节点引用。Open 结果在有限 PendingAck 时间内保留，client 收到能力后单独确认；无确认的引用被回收。OpenAt、OpenNodeRef、OpenChildRef 与新 mutation 在 dispatch 前取得 `FileActionID`，server 以完整输入摘要保留原结果；`QueryFileAction` 不接受调用方补填 Operation 来猜测缺失记录。响应丢失后不能单凭请求 context 取消推断动作未发生。delete intent 使用另一个 durable ID，其状态不随普通 action history 淘汰。

普通已有文件的 Open 已返回能力、但 ACK 失败时，client 先用返回的同一 Session／File 能力执行 Close 清理。只有 Create 与 Truncate 均为 false、原 ACK 错误同时满足 `errors.Is(err, context.Canceled)` 与 `storage.ErrnoOf(err) == EINTR`，且该次清理的原始 error 为 nil，才不返回 File 并保留 EINTR。清理的 ESTALE 不能当作成功，判定发生在既有 ESTALE 抑制之前。带创建／截断意图、deadline、未知 ACK 或会话故障，以及清理 EIO／ESTALE 或独立错误，仍返回无 File 的 EIO；原有创建或截断效果不被解释成未发生。已终止的 native 引用不会由迟到 ACK 或原打开动作的重放重新创建。这项分类不增加重试。

文件控制通道使用独立的 256 KiB envelope 上限和 admission；Strong 控制继续使用自己的 16 KiB 上限。阻塞 range 通过短的 Apply/Query/Cancel 交换维持，不长期占用 HTTP worker。数据 JSON 在编码前核对 envelope 与 base64 后的总长度；区间读取、Attr/metadata、目录 entries 与当前名字在调用 backend 或载入变长 payload 前扣除返回预算。`file.read-dir-node`、`file.observe-directory-metadata` 与 `file.observe-name` 使用独立严格 DTO；ResultBytes、server body、client body 与 native retention 任一更紧时整次失败。

`HandlerOptions.Files` 默认在整个 registry 内允许 64 个会话，每个会话分别最多保留 16384 个数据动作与 16384 个清理动作，PendingAck 为 5 秒；可接纳的会话 options 受 handler 上限约束。`Handler.Close(ctx)` 停止 admission，退役并排空它创建的 registry；backend 仍归调用方。独立 server 先排空 HTTP 请求，再完成 handler 清理，最后关闭自己拥有的 backend；清理失败不释放 backend 所有权。

replicated storage 转发原子打开、身份 namespace、identity-bound directory enumeration、DirectoryMetadataObserver、ReferenceNameObserver、NodeReference、FileActions、metadata、scope、pending deletion、条件 mutation、owner 和 range 能力。路径节点事实与 opaque metadata 进入 SQLite 副本；HTTP v4 Node wire 不携带 authority directory revision，副本只为自身树维护不可导出的本地 token。公开 Stat 可由副本回答，公开 List/ListBounded 及三项名字观察在确认副本健康后回源 authority，因此 guards 永远不与本地 token 比较。引用、Use claim、owner、range 与普通 action history 属于远端 authority/session；durable delete intent 属于远端持久 volume，二者都不写入客户端副本。产生名字或属性日志的成功修改返回权威 barrier，replica 等待同一 incarnation 的位置达到该值；detached 修改没有路径事件，barrier 仍可证明现有 volume 进度。

volume 默认单文件上限 1 GiB，同时物化内容上限 2 GiB，最多 32 次 materialization、8 次状态竞争尝试，每次数据操作预算 30 秒。替换预留当前与下一份内容，读取预留完整对象与返回区间；不能只按 patch 的长度收费。单会话、transport body、backend 对象与配额可施加更紧的边界。有限预算在保留超限内容之前拒绝，已经持有的 reservation 在取消或已知失败清理后释放；未知发布或记账结果保留相应所有权并封锁。

独立 server 暴露 `-max-retained-files`、`-max-file-size`、`-max-file-staging-bytes`、`-file-operation-timeout`、`-http-max-file-sessions`、`-http-max-file-actions`、`-http-max-file-cleanup-actions`、`-http-file-open-ack-timeout` 与 `-file-session-lease/history`。SQLite 的 metadata 总量由 `MaxMetadataBytes` 配置；staging 预算覆盖同时物化的完整旧、新内容，至少容纳两份最大文件。其它上限由库 options 配置；部署形态仍为 localstore 或 Azure Blob + SQLite。

## 六、与显式 S/X 的关系

强 S/X 保护 volume 资源的内容、存在和身份，所有修改在原生发布处检查 proof。它不由 Open、Use claim 或 range 自动取得；有效 Strong proof 也不豁免 Uses/Deny 或 enforced range policy。只拥有 S 的调用方不能修改受保护内容；没有活动保护冲突时，普通匿名修改不必先取得 X。

经授权的 unlink 或覆盖会使旧的命名资源成为 `TargetGone`，原 S/X grant 不转移到同名新对象。已打开 File 的身份、Use claim 与 range owner 随旧对象继续存在。S/X 的有限 lease、恢复 grace、管理动作核对与 FileSession 的存活和 range 等待分别计时；任何一者的成功都不能延长另一者。
