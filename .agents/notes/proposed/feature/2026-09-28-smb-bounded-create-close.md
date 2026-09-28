# Agent Note: 有界 SMB CREATE 与 CLOSE

Status: proposed

## 问题

当前 SMB 端点已验证请求签名、建立本机身份与 volume tree，并为同一 SMB session／export 共用一份 authority `FileSession`；文件命令仍返回 `STATUS_NOT_SUPPORTED`。`storage.AtomicFileOpener.OpenAt`、`NodeReferences.OpenChildRef`、有界目录观察、权威 child guards、文件动作回执和引用关闭结果已经具备平台中立基础，却没有把 Windows CREATE 的名字、访问与共享语义连接到这些原子动作的端点 owner。若先查询路径，再独立打开，名字替换可让 CREATE 命中未经核对的新对象；若创建后才检查句柄容量、引用身份或响应编码，失败可能留下已经生效但无人持有的对象和共享限制。

CLOSE 不能只从传输错误推断远端引用是否释放。响应丢失时重新生成关闭动作，或在 `Released` 已确认但同步 barrier 未确认时回收全部 owner，都会使句柄、共享 claim 和清理责任互相矛盾。此决定须在 [Windows 网络驱动器整体提案](2026-09-16-windows-network-drive-support.md)之下单独交付，遵守 [R-FS-8、R-WIN-2、R-WIN-4、R-WIN-6、R-WIN-9 与 R-WIN-10](../../../../docs/spec/requirements.md)；已交付的[权威子项选择](../../implemented/architecture/2026-09-22-guard-authoritative-child-selection.md)、[可恢复引用关闭](../../implemented/architecture/2026-09-23-recoverable-close-ownership.md)和[分配量账本](../../implemented/architecture/2026-09-23-virtual-allocation-ledger.md)是输入契约。

## 提案

### 交付边界与组件

7.3 交付 `CREATE`、`CLOSE` 和同一已签名 compound 中 CREATE→CLOSE 所需的 related FileId 传递。接受 `FILE_OPEN`、`FILE_CREATE`、`FILE_OPEN_IF`、`FILE_OVERWRITE`、`FILE_OVERWRITE_IF` 五种 disposition；`FILE_SUPERSEDE` 属于 Issue #30 第 8 项，在此明确拒绝。有字节读或写权限的普通文件用 `OpenAt` 取得 `storage.File`；没有字节方法的普通文件打开，包括 DELETE-only、execute-only 和 attribute-only，用 `OpenChildRef` 取得 `storage.NodeReference`。有非空最终叶名的目录也用 `OpenChildRef`；空名字的 share root 用 `OpenNodeRef` 取得 root 的 `NodeReference`。一个 SMB FileId 只拥有其中一种引用，且固定绑定同一 NodeID、tree、authority epoch、访问与 share 声明。SMB 文件命令经 `Share.Backend` 的 `FileSession`，使用 HTTP backend 时再穿过 HTTP client／Handler；端点不把 HTTP 的会话关闭等同于单个 SMB CLOSE。

发布前与首次 tree connect 执行该段需要的 capability 检查：完整且有界的权威目录 metadata 观察、guarded 原子 child open、引用身份、共享准入、权威分配量、动作历史和端点提供 action ID 的可恢复关闭。`CheckFileStorage` 单独不足以证明这些能力。首次 tree connect 在公开 TreeId 前确认 backend volume 与可信 `Share.Volume`、authority incarnation 和 session 作用域一致；缺少或冲突时拒绝该 tree，保留已建立 session 的清理责任。用于这项证明的 `BackendIdentity` 是由可信宿主与经认证 backend 绑定的中立能力，不能是回显调用方 label。第一方 native、limited／locked／replicated 包装与 HTTP 两端须传递同一预检与身份事实；第三方 backend 缺能力时在任何创建、截断或引用保留前失败。

`packages/smb` 负责 SMB 名字映射、请求语义、FileId、状态和清理 owner；`storage` 负责最终选择、Use／share 冲突排序、引用身份与动作回执；HTTP 只透明传输这些中立事实。Windows 128-bit 对象 file ID 从持久、不可复用的 authority NodeID 导出，不能使用本地 SMB FileId；本段 CREATE 返回的对象身份须与后续打开保持一致，目录枚举和完整信息类在后续段落接入。

### 计划的目录与文件职责

以下是实施 PR 的拟定文件布局，不声称这些文件已经存在。修改现有文件时只加入该职责，不把名字解析、句柄注册和 wire 编码集中到 `commands_session.go`。

| 路径 | 变更 | 职责 |
|---|---|---|
| `packages/smb/internal/wire/create.go` | 新增 | 有界解析 CREATE 固定体、名字、context、flags、disposition；编码 CREATE 响应及不可声明的能力位。 |
| `packages/smb/internal/wire/close.go` | 新增 | 有界解析 CLOSE 的 FileId／flags，编码含可选 postquery 属性的 CLOSE 响应。 |
| `packages/smb/windows_name.go` | 新增 | 从 UTF-16 组件到原始 leaf 的可逆 Windows 名字映射、case-fold 比较、保留名／歧义拒绝；不把规则下沉至 storage。 |
| `packages/smb/name_resolver.go` | 新增 | 非空路径逐级完整 `DirectoryMetadataObserver` 捕获、预算、目录身份与 `NamespaceGuards` 建立，生成最终 `ChildSelection`；空名字另取可信 root ID，不伪造最终叶名或以 lookup＋open 模拟 guarded open。 |
| `packages/smb/create.go` | 新增 | 校验 CREATE 意图、权限／共享转换、容量预留、action 生成；有字节方法走 `OpenAt`，无字节方法或目录走 `OpenChildRef`，root 走 `OpenNodeRef`；核对回执与结果。 |
| `packages/smb/handles.go` | 新增 | session/tree 的 FileId 表、代际与不可复用计数器、typed reference、并发操作入场栅栏、active／cleanup-only 配额和索引。 |
| `packages/smb/close.go` | 新增 | 预存 close action、CLOSE 排空、`CloseWithAction` 结算、barrier 与错误结果、重复关闭及 cleanup-only 推进。 |
| `packages/smb/commands_session.go`, `connection.go` | 修改 | 已验证请求分派到 CREATE／CLOSE；每个已验证 compound 维持局部 related context；session/tree 结构持有句柄表。 |
| `packages/smb/session_cleanup.go`, `authority_session.go`, `server.go`, `config.go`, `status.go` | 修改 | tree／session／export 退休接入同一句柄清理，配置显式句柄与 unresolved-owner 上限，报告清理状态而非虚假回收。 |
| `packages/smb/status.go` 或 `operation_status.go` | 修改或新增 | 在有界诊断账本中按 opaque owner／action ID 暴露未知结果和清理进度，净化业务身份与名字。 |
| `packages/storage/capabilities.go`, `capability_validation.go`, `files.go`, `name_observation.go` | 修改 | 给普通文件打开显式 metadata 权限、引用身份、关闭 action／barrier settlement、保留的清理回执额度和 backend 身份加入预检契约；保持平台中立。 |
| `packages/storage/{objectstore,localstore,limited,locked,replicated}/` | 修改 | 按实现能力把 metadata 授权、预检、关闭 action 与结果穿透整个包装链；authority 原子排序，数据动作与清理回执使用独立有界额度。 |
| `packages/transport/httprest/file_*.go` | 修改 | 严格编解码中立身份、显式 metadata 权限、关闭 action／settlement；HTTP 在 open 时预留对应 close 回执额度，响应丢失后同 ID 重投并单调推进 barrier。 |
| `packages/smb/*_test.go`, `packages/smb/internal/wire/*_test.go`, `packages/storage/*_test.go`, `packages/transport/httprest/*_test.go` | 新增／修改 | wire 边界、名字竞争、容量零效果、共享冲突、响应丢失、关闭结算和跨 adapter 行为回归。 |
| `docs/design/client/smb-endpoint.md`, `docs/design/server/file-handles.md`, `docs/testing.md` 及相应 implemented Agent Note | 修改／新增 | 与落地代码同 PR 记录实际结构、契约、测试门和决定。 |

这组目录保留后续 7.4 文件数据命令、7.5 枚举、7.6 删除义务及第 8 项名字／范围控制所需的 typed handle 和同一 owner 生命周期；本段不创建那些命令的成功路径。

### 内部数据形状与状态机

拟定的内部值保持语义分层；字段名用于说明职责，实施时可依 Go 结构调整，但不得丢失事实：

```text
OpenIntent = {session, tree, principal, volume, authorityEpoch,
              rawSignedFrame, normalizedComponents, desiredAccess,
              shareAccess, disposition, createOptions, contexts,
              actionID, guardedSelection, expectedObject, responseBudget}
HandleOwner = {fileID: (sessionIncarnation, monotonicCounter), treeID,
               nodeID, authorityEpoch, kind: File | NodeReference,
               grantedAccess, shareClaim, reference, openActionID,
               closeActionID, closeIntent, state, admittedIO, settlement}
CloseSettlement = {actionID, released: known-true | known-false | unknown,
                   barrier: none | pending | settled | unknown, semanticError}
```

`OpenIntent` 由已验证请求建立，不能只保存文件名或最终 response；`actionID` 在对象效果前按 `FileSessionStatus.ActionEpoch` 生成并与不可变负载一起保存在 owner。已存在的中立 `FileActionReceipt` 只能报告动作状态，不能还原打开时返回的引用、属性、outcome 或关闭时的 `Released`；因此响应丢失时优先以相同 ID、相同语义负载重投原方法取得原结果，不把 `Completed` 本身当完整结果。`NotExecuted` 才容许基于新观察生成新动作；`Unknown`／`Retired` 不能推断零效果。对普通 Windows 应用，端点是 R-FS-8 所说的 action 调用方；应用不接收内部 action ID，最终无法确认时本次 SMB 请求返回 I/O 未知错误，宿主诊断账本保留有界线索。

`HandleOwner` 的状态为 `Reserved → Opening → Live → Closing → Released`；`Opening` 取得非 nil 引用但未能公布成功时走 `CleanupOnly`，`Closing` 中释放未知也走 `CleanupOnly`；引用已释放而 barrier 待结算时走 `BarrierOnly`，仅 barrier 明确结算后到 `Released`。本地响应写入失败不逆转 `Live` 或权威动作；其 owner 按 tree/session 继续清理。`Live` 接纳操作时增加计数，`Closing` 先封门再等待已接纳操作结束，不能把仍在途的 I/O 当作没有保护的空闲引用。重复 CLOSE 读取同一 owner 的结算状态并推进原动作，不制造第二个 close ID。

FileId 使用 session incarnation 与单调计数构成 128-bit 不可猜测／不可复用的打开实例标识；服务重启、新 session 或计数耗尽均不复用旧值。索引键同时校验已签名 session、TreeId 和 FileId，避免另一个 tree 或 session 借数字命中引用。此 ID 与返回给 Windows 的稳定对象标识分开；后者对应不可复用的 authority NodeID。既有 tree 在同一 export 上共用 authority FileSession，但每个 handle 仍由创建它的 tree 持有并随该 tree 清理。

显式配置的 `MaxHandles` 限制所有 active、opening、cleanup-only 和 barrier-only owner；每个 FileSession 的 `MaxFiles` 和 authority 共享物理名额仍独立生效。另有 `MaxUnresolvedOwners`／诊断记录字节上限；若达到上限，拒绝新的效果动作但继续允许 CLOSE、回执查询和退休清理使用预留容量。每份 owner 预留 FileId、结果编码预算、open action 和可能的清理状态后，才调用可能创建或清空对象的 authority 方法；失败退还未使用配额。引用即使与错误同时返回，也由该 owner 保留并清理。清理未知时任何统计都不能把它记为释放，饱和时新 CREATE 在权威对象效果前返回资源错误。

端点自己的 owner 配额不能保证 authority 接受关闭回执。每份 FileSession 设置独立的有界 cleanup receipt lane：会话建立时为 session-close 保留一格，每次成功或可能已成功的 open 在最终效果前再为该引用保留一格；占用涵盖未关闭引用、未确认 close、已释放但仍在历史期内的 close 回执和 barrier-only owner。每次释放把该引用的预留格转成同 ID 回执，只有其历史期结束且 barrier／owner 责任结清后才退还；因此频繁打开／关闭也不能靠累积已释放回执挤占仍活跃引用的清理名额。新 open 若不能在 native action history、HTTP registry 和所有包装层同时取得对应额度，就在对象效果前拒绝。普通 data/open actions 只占数据 lane，达到现有 `MaxLockActions` 不得阻断已预留的 close；HTTP 的 `MaxCleanupActions` 也不能由不带 owner 预留的其它 cleanup 动作吃掉。回执查询和同 ID 重投使用已有记录，不消耗新格；清理 lane 满时继续服务已保留的 owner，并拒绝新的会增加责任的动作。额度和历史保留字节都设定上限，不通过无界扩容满足此保证。

### 从已签名 CREATE 到权威结果

1. `connection.run` 在帧、compound、credit 上限内解析命令，`dispatch` 用原始 command bytes 验签后确认已认证、未过期的 SMB session、tree/export、身份与 request admission。未知或未签名的文件请求在 backend 调用前拒绝。相关 compound 的继承字段仅在同一已签名 frame 局部生效；绝不改写用于签名验证的原始字节。
2. wire parser 核对 StructureSize、offset／length、UTF-16 完整性、context 数量／对齐／重复、option/disposition 合法组合和响应可编码上限。禁止 ADS、reparse／durable／persistent 授予、DFS/replay 等已排除语义；可选且未授予的 preallocation context 仅在不改变基本打开效果时忽略，响应不能宣称执行。要求改变打开效果的未知 context 在任何 authority 调用前失败。`FILE_SUPERSEDE`、当前尚不支持的 delete-on-close 创建意图和不兼容 flag 返回明确 unsupported，不悄悄降级为普通打开。
3. `windows_name.go` 从 root 到最终 leaf 对每级 Windows 名字作规范化与可表示性检查。`name_resolver.go` 通过 authority 的完整有界目录 metadata 捕获，核对同级 Windows case-fold 唯一性、原始 leaf 与不可复用对象 ID，并构建每级 revision／edge guards 和最终 `ChildCondition`。未观察全体同级条目就不能断言名字不存在；无法完整观察或发现歧义则整次 CREATE 失败。最终选择是带 guards 的 `ChildSelection`，不允许再独立 `LookupAt` 后执行无 guard open。纯名字观察不能替代最终效果点的核对。
   `NameLength=0` 是 share root 本身，不经过 final-leaf resolver，也不构造空 leaf 的 `ChildSelection`。已授权且绑定此 export 的 backend 通过 `Storage.Stat(ctx, "")` 取得可信 root `Attr.ID`，在同一 `FileSession` 上以 `NodeReferences.OpenNodeRef(rootID, NodeRefOptions{Kind: NodeDirectory, Target: SameNode(rootID), Action, Use, MetadataAccess})` 取得引用。只接受 `FILE_OPEN` 和 `FILE_OPEN_IF` 对已存在目录的打开分支；`FILE_CREATE` 因 root 已存在而失败，`FILE_OVERWRITE`／`FILE_OVERWRITE_IF` 与非目录、截断意图在效果前拒绝。`OpenNodeRef` 的最终身份条件、share claim 和 authority 排序仍必须成立；`Stat` 的结果不能被当作一份已取得的引用。
4. 在产生对象效果前，按 SMB desired access、share access、disposition 和对象类型形成中立 `UseClaim` 与 `OpenAtOptions`／`NodeRefOptions`，预检当前授权、readonly／metadata predicate、allocation 与 identity 能力，预留 owner 和响应预算。read/write/delete 三类访问及三类 sharing 必须双向核对：新 access 受旧 deny 限制，旧 access 受新 deny 限制。判断依据是 authority 最终排序的 per-NodeID claim，不是 SMB 端点自己的过时表。目标已存在时 `FILE_CREATE` 的排他条件与 `FILE_OPEN` 的必存在条件由最终 `ChildCondition` 保证；两个 `*_IF` 允许相应创建分支。`FILE_OVERWRITE`／`*_IF` 使用同次权威打开的 `ResetContent`，并在最终事务同时核对 `READONLY` 的 metadata version、更新 `ARCHIVE`，不先清空再补写属性。

先展开 `GENERIC_READ`、`GENERIC_WRITE`、`GENERIC_EXECUTE`、`GENERIC_ALL` 等别名，再核对请求的每个具体 right；无法在打开效果前确定授予集合的 `MAXIMUM_ALLOWED` 或未支持 right 明确失败。目录与普通文件按各自 right 解释同一个数值位，不能把目录枚举错当成文件字节读取。[MS-FSA §2.1.5.1.2.2](https://winprotocoldocs-bhdugrdyduf5h2e4.b02.azurefd.net/MS-FSA/%5bMS-FSA%5d.pdf)的 share 检查把 data／execute read、data／append write 和 DELETE 分组；属性及 EA right 不参加这三组冲突。映射如下；`Uses` 表示共享冲突声明，不自行授予 `File` 或 `NodeReference` 方法。

| 已展开的 SMB desired right／share flag | `UseClaim` 或 `NodeRefOptions` 结果 | 实际操作权限 |
|---|---|---|
| 普通文件 `FILE_READ_DATA`／`FILE_EXECUTE`；目录 `FILE_LIST_DIRECTORY`／`FILE_TRAVERSE` | 分别加入 `Uses.ReadData`、`Uses.ReadEntries` | execute／traverse 仅形成 share-read 声明；7.4 字节读、7.5 目录枚举仍在各自调用时核对。 |
| 普通文件 `FILE_WRITE_DATA`／`FILE_APPEND_DATA`；目录 `FILE_ADD_FILE`／`FILE_ADD_SUBDIRECTORY` | 加入 `Uses.WriteData` | 目录 `NodeReference` 不因此获得字节写方法；后续名字创建另行授权。 |
| `DELETE` | 加入 `Uses.DeleteName` | 不直接执行删除；后续名字或 disposition 动作另行授权。 |
| `FILE_READ_ATTRIBUTES`／`FILE_READ_EA`；`FILE_WRITE_ATTRIBUTES`／`FILE_WRITE_EA` | 目录／metadata-only 引用分别设置 `MetadataAccess.ReadMetadata`／`WriteMetadata`；这些 right 本身不加入 `Uses` | `WriteMetadata` 作为 CREATE 写访问授权，后续属性修改再次按实际 operation 授权。 |
| 无 `FILE_SHARE_READ` | 加入 `Deny.ReadData | Deny.ReadEntries` | 同时挡住其它持有者的文件字节读和目录枚举 claim。 |
| 无 `FILE_SHARE_WRITE`；无 `FILE_SHARE_DELETE` | 分别加入 `Deny.WriteData`、`Deny.DeleteName` | `Deny` 与其它引用的 `Uses` 双向比较，包括非 SMB 入口。 |
| 只有同步、控制等不属上述类别的 right | 不凭这些 right 添加数据 `Uses` 或 metadata permission | 不从共享声明推导额外数据／metadata 能力。 |

`FILE_SHARE_*` 已设置的位只表示不加入对应 `Deny`，绝不扩大 `Uses`。该表把 metadata right 与数据／目录／删除 share 类别分开：只有 metadata right 的请求不暗中声明 `ReadData` 或 `WriteData`。普通文件 `OpenAtOptions.Read`／`Write` 只由已授予的字节访问决定。7.3 给 `OpenAtOptions` 增加独立的 `MetadataAccess` 字段，并把现有 `OpenAtOptions.Check` 的 `Uses.ReadData == Read`、`Uses.WriteData == Write` 改为 `Read ⇒ Uses.ReadData`、`Write ⇒ Uses.WriteData`；多出的 Use 只表达 `FILE_EXECUTE` 等分享兼容声明，不授予 File 字节方法。这一独立字段和验证规则贯穿 native、HTTP、动作 canonicalization 和包装层；SQLite `OpenAt` 不再无条件给 `ReadMetadata|WriteMetadata`。数据读＋属性写映为 `Read=true, Write=false, Uses=ReadData, MetadataAccess=WriteMetadata`；数据写＋属性读映为 `Read=false, Write=true, Uses=WriteData, MetadataAccess=ReadMetadata`。任意没有字节读写权的普通文件打开，包括 DELETE-only、execute-only、attribute-only 及其组合，都走 `NodeRefOptions{Kind: NodeRegular, Use, MetadataAccess}`；DELETE-only 保留 `Uses.DeleteName` 和授予的 DELETE right，执行权只保留 share-read 分类，二者都不产生 `ReadAt`／`WriteAt` 方法。创建缺失普通文件也可用这条 `OpenChildRef` 分支；要求清空内容的 disposition 则必须有字节写权，不能借 NodeReference 偷做截断。`FILE_EXECUTE + FILE_WRITE_DATA` 以 `Read=false, Write=true, Uses=ReadData|WriteData` 取得 File，仍不授予字节读。FileId 保存每个已授予的 SMB right、字节访问和 metadata 权限；原生引用的属性修改与 7.4 命令分派都核对 `WriteMetadata`，不能只靠端点前置判断。目录／metadata-only 引用上的 `ConditionalFileMutation.MutateFile(MutateAttributes)` 继续使用现有能力，不新增 mutation 接口。
5. 非空路径的 `OpenAt`／`OpenChildRef` 在同一次 authority 结果里核对 guards、child 条件、分享冲突、容量、业务授权所对应的请求参数，并执行创建／截断／取得引用；空名字的 `OpenNodeRef` 用 root `SameNode` 条件和同样的 share／owner 规则取得目录引用。端点不得在返回之后补做决定成功是否成立的名字、对象身份或 allocation 检查。返回后只验证已承诺的结果结构：Attr NodeID 与引用的不可变 `ReferenceNodeID` 一致、outcome 与 disposition 一致、allocation-known 且分配字节数合法、时间与 Windows 属性可编码；违约不报告成功，保留任何非 nil 引用供清理。`AllocationSize` 按权威字节值报告，不强加 4096 字节对齐。
6. 在 owner 中安装引用及权威结果后编码 CREATE response，FileId 才可对同一 session/tree 的后续请求可见。`CreateAction` 由 `Opened`／`Created`／`Reset` 映射；`Replaced` 属第 8 项。空响应、编码失败或连接丢失仍留下 owner，走同一退休路径。若请求 deadline 先结束，只能查询或用原 action ID 与原负载重投；历史窗口到期、authority epoch 更换或 session 退休后报告未知，不以新路径再开一次猜测结果。

`FILE_OPEN` 对不存在目标失败而不创建；`FILE_CREATE` 对任意已占用目标失败；`FILE_OPEN_IF` 对存在目标保持内容、对缺失目标创建；`FILE_OVERWRITE` 要求已有普通文件并原子清空；`FILE_OVERWRITE_IF` 对已有文件原子清空、对缺失目标创建。目录及任何无字节写权的 NodeReference 分支不能被当作可写字节文件，也不能处理允许截断的 disposition。`CreateOptions` 中目录、非目录、同步等被支持位在对应引用类型和响应 flag 上一致；不能满足的选项在打开效果前拒绝。

### CLOSE、related compound 与清理次序

`CLOSE` 仅接受拥有该 FileId 的 session/tree，先在 owner 上原子切到 `Closing`，禁止后继新 I/O，再等待已接纳操作排空。进入该状态前生成 close action ID，连同引用身份与不可变 close 意图写入 owner；向 `CloseWithAction(ctx, id)` 传入这个 ID。中立 `CloseSettlement` 明确区分引用释放与 barrier 状态；HTTP 现有 `barrierPending` 必须在 adapter 处转成中立结果，并传给所有包装层。每次重投都用同一引用、同一意图、同一 ID；若同 ID 携不同输入，authority 拒绝而不执行。原生 close 的 `Released` 与语义错误一经确定即不可变；同 ID 重投只允许 barrier 从 pending／unknown 单调推进到 settled，不再次调用原生 close。也可以用同 ID 的独立 settlement 查询推进 barrier，但不能把首次 pending 冻成永久结果。`QueryFileAction` 可判断动作是否仍被记录，Completed 不代替原方法返回的释放、语义错误和最新 barrier 状态。

close 路径在 owner、引用和 action ID 已登记时，即使普通 FileSession 因 lease 到期、retired 或 data admission fence 拒绝新 I/O，也必须能读取已有 close 回执、重投同一 close 或继续窄清理；不能把现有 `runFileAction` 的 active／expiry/data-lane 检查原样套在 close 上。它不能接受新字节或名字效果，也不能让无原 owner 的新 action 越过 session fence。authority 已自动释放引用时，回执报告已知释放事实；确实无法确认时保留 endpoint owner 与未知结果，不凭 epoch 更换推断成功。

释放已确认且 barrier 为 none／settled 时去掉 FileId、回收 owner 与配额；释放已确认但 barrier pending／unknown 时移除可访问引用，保留 `BarrierOnly` owner、action 和查询容量，直到原 barrier 明确结算。释放为 false 或未知则保持 `CleanupOnly` 引用和 claim，不把错误写成成功，也不让同名后继误以为共享限制已离开 authority。旧引用如果因 session lease 或 authority incarnation 更换而失效，FileId 持续报告失效，绝不重新打开同名对象。CLOSE 的 postquery attribute 是可选响应附加信息：获取失败时按 SMB 允许方式清除 postquery 标志，不能撤销已确认的释放事实；若协议要求的响应字段无法可靠编码，则本次响应失败且 owner 仍依已知释放／barrier 事实推进。

`TREE_DISCONNECT`、`LOGOFF`、TCP 断开、export unpublish 与 server stop 都调用同一 `closeHandle` 状态机。顺序是封住新的 tree／session admission，等待已接纳 frame 和 handle I/O，逐 owner 推进原 close action，确认释放／barrier，最后关闭共享的 authority session 与 export 引用。清理期间保留 owner、容量和可重试错误；失败后 `stopping` 对象仍可重试，不能报告资源已回收。不同 tree 共用同一 authority FileSession 时，退出其中一个 tree 只清它的 handles；最后一个 tree 退休才关闭那份 session。`Unpublish` 不需等待无关 export 的身份认证和清理锁；已接受的删除义务仍由 authority 保留并在 7.6 接入可发现的持久 owner。普通 CLOSE 不触发 7.6 尚未接受的 delete-on-close 语义。

客户端显式 `CLOSE` 是一项新请求：在把 owner 从 `Live` 切到 `Closing` 前，按 `OpFileClose` 核对当前业务授权；明确拒绝则返回拒绝并保持原句柄可用。客户端显式 `TREE_DISCONNECT` 可在开始退休前按现有 `closeTreeAuthorized` 路径授权，拒绝时保留整个 tree。已接受的 owner 退休则属于既有责任的固定续作：`LOGOFF`、TCP 断线、会话过期、`Unpublish`、server stop 和已开始的 tree 退休使用 `closeTreeContext` 同类内部路径，不对每个 handle 或 tree 再执行当前业务授权。特别是显式 LOGOFF 不可因逐 tree `OpFileSessionClose` 授权撤销而留下半退役 session。中立 `CloseWithAction`／HTTP close 必须凭原 session、引用、action ID 和已接纳 owner 验证这项窄清理权，允许释放／barrier 结算继续；它不是允许新打开、状态查询或其它写入的通用绕权凭据。当前 policy 被撤销仍不得使内部 cleanup 永久持有旧引用或 share claim；新外部查询按当前权限授权。内部 cleanup 的取消、超时与结果未知仍保留原 owner 待重试。

related compound 的 FileId 占位只绑定到**同一已签名 frame**内前一个成功 CREATE 的 owner，且后继 request 的 session/tree 仍须独立核对。CREATE 失败时相关 CLOSE 返回前驱失败的合适状态，不能引用上一次 frame 或另一个 tree 的 FileId。compound 中 CREATE 成功而 CLOSE 失败时 CREATE 的权威打开结果不能被回滚；其 owner 继续存活或进入清理状态。编码所有子响应和 credit 计算以前先预算最坏结果；不得在已产生效果之后因 compound response 超额抛弃 owner。CANCEL、replay flag 和 durable reconnect 仍按现有显式拒绝路径处理，不解释为关闭重试；内部 action 重投独立于客户端 SMB replay flag。

### 授权、状态映射与观测

SMB 签名与 `Principal` 只认证本机调用身份。新 CREATE、显式 CLOSE 与外部查询以可信 `Share.Volume` 及精确 `storage.Operation` 调用 `Config.Authorize`；`desiredAccess`／share 转换不额外授予业务权。内部 cleanup 只继续已接纳的 owner，不重新授权成新业务动作。授权失败在效果前返回拒绝；授权服务不可达是 I/O 未知，不能映成文件不存在。错误码的最小映射表如下，最终由协议实现核对 MS-SMB2 对应条件：

| 条件 | SMB 状态原则 |
|---|---|
| 未签名、身份不符、访问或业务授权被拒绝 | `STATUS_ACCESS_DENIED`；无 backend 效果。 |
| 显式 CLOSE 授权被拒绝 | `STATUS_ACCESS_DENIED`；FileId 保持 `Live`，原引用和 claim 不变。 |
| 断线／LOGOFF／stop 时 policy 已撤销 | 内部 owner 清理继续；清理错误如实报告或记录，不能把授权拒绝伪装成已释放。 |
| 目标不存在、排他创建撞名、非目录／目录类型冲突 | 分别映为 `STATUS_OBJECT_NAME_NOT_FOUND`、`STATUS_OBJECT_NAME_COLLISION`、对应对象类型错误；只能在权威条件已知时使用。 |
| 双向 share claim 冲突 | `STATUS_SHARING_VIOLATION`；不能在冲突前产生创建或截断效果。 |
| DELETE-only 等无字节写权的引用请求 overwrite／truncate | `STATUS_ACCESS_DENIED`；不把 NodeReference 当数据 File，且无内容效果。 |
| FileId 不属于该 tree／session、已释放、authority epoch 失效 | 分别映为无效句柄或已失效网络句柄状态；不根据路径重绑定。 |
| 本地／authority 容量耗尽 | `STATUS_INSUFFICIENT_RESOURCES`；先证明确无对象效果。 |
| namespace guard 或 metadata predicate 冲突 | 重做有限次完整权威观察并以新 action 重试，只在原动作明确未执行时；持续冲突返回可重试的明确失败。 |
| transport 中断、动作 Unknown／Retired、不可确认 allocation／barrier | I/O 未知错误；不得报告不存在、成功或旧值。 |
| 未交付的 disposition、context、命令 | `STATUS_NOT_SUPPORTED`，在 backend 效果前返回。 |

现有 `statusError` 只覆盖较粗的 errno 类别；实施时应由 CREATE/CLOSE 的 typed domain errors 映射精确 Windows 状态，避免把 share conflict、目标冲突和未知结果一律压成通用 I/O。对外诊断公开 opaque owner／action ID、export、状态、上次错误类别、active／cleanup-only／barrier-only 计数与有限历史；不输出凭据、签名密钥、业务路径或可冒充身份的信息。日志与状态不能把确认已释放、仍有 barrier 的 owner 算为 live handle，也不能把仍有清理责任的 owner 算为零。

### 7.3 与后续 PR 的形状约束

本 PR 内实现的保证：最终 guarded 选择、双向共享排序、容量先预留、打开与 close action 身份、typed handle、释放与 barrier 分离、生命周期清理和可观察错误。`READ`／`WRITE`／`FLUSH`／文件信息与属性操作归 7.4；完整 `QUERY_DIRECTORY` 归 7.5；关闭时删除和显式 disposition 归 7.6／8；SUPERSEDE、rename、通知、缓存恢复及范围 LOCK／CANCEL 归第 8 项；WNet 与真实 Windows 验收归 9／10。它们的推迟分别增加命令、目录快照、持久删除状态和原生环境；不要求把 7.3 已有引用重新设计成路径句柄。对此，7.3 的 FileId 不能只保存名字，close owner 不能依赖 CREATE 响应送达，目录引用不能伪装为字节 File，authority `UseClaim` 不能只有本地预检，响应缓存不能把旧视图当权威结果。

7.3 只让 CREATE/CLOSE 可审查，不宣布整个 Windows drive 已满足 R-WIN-2、R-WIN-8 或发布资格。现有 [HTTP Handler.Close CI 竞态](https://github.com/codetreker/remote-fs/actions/runs/35974116040)需要独立回归修复，作为 HTTP backing 的生产资格和停止验收门；它不改变 7.3 对单个文件引用的 close 设计。

## 备选方案

**把 7.3–7.6 及第 8 项压成一个文件面 PR。** 可以一次演示完整网络盘，但 CREATE 的 authority 选择、句柄生命周期、I/O revision、目录快照和名字修改各有不同的原子边界，单个 diff 难以审查一项效果失败是否留下部分状态。此提案保留每段所需的 typed handle 与 action 形状，分开验证其语义。

**仅用现有路径 `OpenFile` 和客户端 share 表。** 路径观察与打开之间可能换对象，另一个 SMB session 或非 SMB 入口也可能在本地表外授予冲突访问；无法满足 R-FS-8 与 R-WIN-6 的最终排序。因此选 guarded `OpenAt`／`OpenChildRef` 与 authority Use claim。

**收到 CLOSE 错误就丢弃 FileId，再由 session 到期清理。** 释放未知会留下共享 claim 或引用，而到期之前 endpoint 已谎报资源可用；释放已确认但 barrier 待结算又会丢掉剩余责任。端点必须保存预先生成的 close action 与分离状态。

**在 7.3 同时支持 SUPERSEDE 和 delete-on-close。** 原位 SUPERSEDE 须在保留原 authority 对象 ID 的同时产生本次打开的新 SMB FileId；改名覆盖同名目标则使名字指向另一对象，不能与原位 SUPERSEDE 共用身份转换规则。delete-on-close 另增持久删除义务的可观察状态。Issue #30 已把 SUPERSEDE 放在第 8 项，delete-on-close 放在 7.6。7.3 先拒绝对应请求，且 FileId／owner 结构不阻止后续加入这些义务。

## 验收标准

- 对五种 disposition 分别覆盖目标缺席、目标已有普通文件、目录／metadata-only 类型、同时创建赢家，以及创建／截断与名字替换竞争；guard 或 child 条件失败时零对象、零引用、零 share 效果。
- `NameLength=0` 的 share root 用 `Stat("")` 的 root NodeID 和同一 authority session 的 `OpenNodeRef` 取得目录 FileId；`FILE_OPEN`／`FILE_OPEN_IF` 成功且 CLOSE 能释放该引用，`FILE_CREATE`、overwrite、非目录与截断请求无副作用失败。root FileId 保留 metadata／share 权限，7.5 可用它作 root `QUERY_DIRECTORY`；跨 tree、response 丢失与错误／非 nil 引用仍按普通 owner 路径处理。
- 两个 SMB session 和非 SMB 入口的 read/write/delete access 与 share 三向矩阵均由 authority 双向排序；冲突发生在创建／截断前，释放后才准入后继。已打开 FileId 经同名替换仍指原 NodeID。
- 精确 access/share 映射矩阵覆盖普通文件 `FILE_READ_DATA`、目录 `FILE_LIST_DIRECTORY`、写／删除 right、只含 metadata right、三种 `FILE_SHARE_*` 的每个有／无组合与 generic 展开。缺 `FILE_SHARE_READ` 同时冲突 `ReadData` 和 `ReadEntries`，metadata-only 不取得字节能力；外部普通读、目录枚举与名字效果都在 authority 最终顺序受对应 claim 约束。
- 目录和 metadata-only CREATE 分别覆盖只读属性、只写属性、读写属性以及 generic 权限展开：`NodeRefOptions.MetadataAccess`、`UseClaim` 和 open 授权一致；没有写权限的 FileId 在 7.4 属性修改入口被拒绝，有写权限的引用可使用现有 `ConditionalFileMutation.MutateAttributes`，且 share conflict 仍在打开效果前失败。
- 令 endpoint `MaxHandles=1` 和底层 `MaxFiles=1`，验证额满 CREATE 没有文件、长度、属性、Use 或分配量副作用；opening／cleanup-only／barrier-only 都计费，清理仍能推进。
- 分别填满 objectstore 普通 `MaxLockActions`、HTTP data-action history、其它 cleanup action 与已释放 close 回执；已有每个 live FileId 的 CLOSE、同 ID 重投、receipt 查询和 session 退休仍可推进。每次 open 前若任一层不能为该引用及 session-close 保留清理额度，CREATE 无对象效果失败；有限历史到期后额度才回收。
- 普通文件混合 `FILE_READ_DATA + FILE_WRITE_ATTRIBUTES`、`FILE_WRITE_DATA + FILE_READ_ATTRIBUTES`、`FILE_EXECUTE`／`GENERIC_EXECUTE` only、`FILE_EXECUTE + FILE_WRITE_DATA`、DELETE-only 和 attribute-only 打开分别验证 byte flags、Use、显式 `MetadataAccess`、业务授权与原生属性权限；所有无字节权限的普通文件均经 `NodeRefOptions{Kind: NodeRegular}` 打开，不获得字节方法。DELETE-only 保留 `Uses.DeleteName`，可供后续 7.6 disposition／8.1 rename；DELETE-only 的 `FILE_CREATE`／`FILE_OPEN_IF` 可以取得无字节方法的新文件引用，overwrite 在效果前失败。混合 execute／write 不获得字节读，未授予写属性的 `File`／`NodeReference` 不能靠旧的无条件 native metadata grant 修改属性。
- 删除或延迟 `OpenAt`、`OpenChildRef`、CLOSE、HTTP response 的返回帧，验证同 actionID／同输入恢复原结果，不创建第二份引用；`Unknown`／`Retired` 报 I/O 未知，并能从有界诊断状态定位 owner。
- 让 open 返回 `reference != nil` 与 error、或返回 Attr NodeID 与引用 NodeID 不符／非法 allocation；都不报告成功，引用由预留 owner 清理。有效非 4096 对齐分配字节值在 CREATE/CLOSE 响应原样表示。
- CLOSE 同时测试 `Released=false`、`Released=true + barrier pending`、语义错误、重复请求、postquery 属性失败和断线后 tree／session 退休；统计与所有权跟真实状态一致，旧 FileId 无法重绑定。
- same-ID CLOSE 在原生 close 仅执行一次的前提下保留不可变 `Released`／语义错误，并使 barrier pending／unknown 最终转为 settled；普通 session lease 已过期或处于 retired 时仍可取已登记 owner 的 close 回执并完成窄清理，不能接受新数据动作。
- 撤销 CREATE 后的新权限，验证显式 CLOSE 授权拒绝保持 FileId／claim；再断开连接、显式 LOGOFF、停止 export／server，验证内部清理仍用原 action 释放引用并撤销 claim，超时或未知时保留 owner 供重试；外部查询仍因当前授权拒绝。显式 TREE_DISCONNECT 授权拒绝保持原 tree，但其后断线 cleanup 能退休它。
- 同一签名 compound 的 CREATE→CLOSE 可用 all-ones 占位 FileId；前驱失败、跨 tree、跨 frame 和畸形占位均不接触错误引用。未知 context／unsupported flag、SUPERSEDE、delete-on-close 在效果前拒绝。
- `gofmt`、相关 SMB wire／protocol、storage capability、HTTP recovery 的普通与 race 测试、针对该 diff 的 vet／build／文档链接检查通过；记录真实命令和输出，不能以测试跳过当通过。

## 风险

完整目录 metadata 捕获是每级按名打开的冷路径成本，深目录或大目录可能提高延迟；预算耗尽必须明确失败。性能优化只能在可证明的 revision 绑定索引或完整观察缓存上进行，不能退回不完整 lookup。authority session 动作历史有期限，未知结果未必能在 Windows 原调用中恢复；有界诊断保留事实但不是普通应用的动作查询 API。第三方 backend 的 capability 预检和跨 wrapper 传递可能要求同步扩展接口；若缺少可证明的 volume 身份、不可变引用身份、分配量或可恢复 close，不得让其进入会产生效果的 CREATE 路径。

此段不解决 Windows redirector 对未交付 I/O、通知和缓存的行为，也不授予 durable handle。生产可用性须通过后续 PR 和实机门；本段的资源上限与响应预算需要在实施时用负载和恶意帧验证，避免诊断账本本身成为无界缓存。
