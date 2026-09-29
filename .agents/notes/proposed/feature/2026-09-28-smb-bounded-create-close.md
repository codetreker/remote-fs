# Agent Note: 有界 SMB CREATE 与 CLOSE

Status: proposed

## 问题

当前 SMB 端点已验证请求签名、建立本机身份与 volume tree，并为同一 SMB session／export 共用一份 authority `FileSession`；文件命令仍返回 `STATUS_NOT_SUPPORTED`。中立的 guarded child open、节点引用、目录观察和[可恢复引用关闭](../../implemented/architecture/2026-09-23-recoverable-close-ownership.md)由 storage 提供，但没有把 Windows CREATE 的名字、访问和共享语义接入端点的 FileId owner。若先查路径再打开，名字替换可让 CREATE 命中未经核对的新对象；若权威效果发生后才检查句柄容量或结果可编码性，失败会留下无人持有的引用或共享声明。

SMB CLOSE 还需要在本机维护一次打开的稳定 owner。连接断开、响应丢失或 barrier 未结算时，单个 NTSTATUS 无法说明引用和清理责任是否结束。该 owner 必须复用中立关闭尝试的 action ID、generation 和确定性结果，不得根据 transport error 重新关闭另一对象。本提案只交付 SMB CREATE/CLOSE，遵守 [R-FS-8、R-WIN-2、R-WIN-4、R-WIN-6、R-WIN-9 与 R-WIN-10](../../../../docs/spec/requirements.md)，处在[整体 Windows 提案](2026-09-16-windows-network-drive-support.md)之下。

## 提案

### 交付边界与组件

7.3 交付 `CREATE`、`CLOSE` 和同一已签名 compound 中 CREATE→CLOSE 所需的 related FileId 传递。接受 `FILE_OPEN`、`FILE_CREATE`、`FILE_OPEN_IF`、`FILE_OVERWRITE`、`FILE_OVERWRITE_IF` 五种 disposition；`FILE_SUPERSEDE` 属于 8.1。普通文件有字节读写权时，`AtomicFileOpener.OpenAt` 返回 `storage.File`；只有 DELETE、execute、metadata 等非字节权时，`NodeReferences.OpenChildRef` 返回 `storage.NodeReference`。目录叶名也走 `OpenChildRef`；空名字的 share root 走 `OpenNodeRef`。一个 SMB FileId 恰好拥有一份 typed reference，固定绑定 NodeID、tree、authority epoch、实际授予的访问、Use/share claim 和关闭 owner。

SMB 文件命令经 `Share.Backend` 的 `FileSession`；HTTP backend 经 HTTP client／Handler 到 authority。端点将已有中立 `ReferenceCloseActions`、绑定引用的 `CloseOwnerStatus`、`CloseAttempt{Action,Generation}`、`ReferenceCloseResult{Released,Determined}` 和 barrier settlement 作为依赖；它不再修改 SQLite 最终释放、`DropUseExact`、HTTP 关闭 wire 或中立回执预算。中立实现若缺失可恢复关闭能力，tree 在产生任何打开效果前拒绝。首次 tree connect 还须核对可信 `Share.Volume` 与经认证 backend 的 volume／authority incarnation、session 作用域及所需 capability；不能把调用方 label 回显当身份凭据。

`packages/smb` 负责 Windows 名字、请求与状态编码、FileId、session/tree owner、显式授权和本机清理调度。storage 负责最终 guarded 选择、Use/share 冲突排序、引用身份和关闭动作的权威事实。Windows 128-bit 对象 file ID 从持久且不可复用的 NodeID 导出，与每次打开的新 SMB FileId 分开。

### 计划的目录与文件职责

以下是此实现 PR 的拟定布局；职责在对应文件中落地，已存在文件只扩展本段需要的行为。

| 路径 | 职责 |
|---|---|
| `packages/smb/internal/wire/create.go`、`close.go` | 有界解析 CREATE/CLOSE 的固定体、偏移、名字、context、flags、FileId；编码响应，不宣称未授予的能力。 |
| `packages/smb/windows_name.go` | UTF-16 组件到 raw leaf 的可逆表示、Windows case-fold 比较、保留名和歧义拒绝；规则不进入 storage。 |
| `packages/smb/name_resolver.go` | 逐级完整目录 metadata 捕获和预算，形成最终 `ChildSelection` 的 parent identity、edge 与 revision guards；空名字走 root 分支。 |
| `packages/smb/create.go` | desired access/share/disposition 转换、授权与容量预留、action 生成、`OpenAt`／`OpenChildRef`／`OpenNodeRef` 分派、权威结果验证。 |
| `packages/smb/handles.go` | session/tree FileId 表、不可复用 ID、typed reference、在途 I/O 栅栏、active／cleanup-only／barrier-only 计费。 |
| `packages/smb/close.go` | 稳定 owner、绑定引用的 CloseOwnerStatus 核对、内部尝试接管、同 ID 对账、Ready 后的新 generation、barrier-only settlement。 |
| `packages/smb/commands_session.go`、`connection.go` | 已签名命令分派；仅在同一已签名 frame 中传递 related FileId，不修改验签原始字节。 |
| `packages/smb/session_cleanup.go`、`authority_session.go`、`server.go`、`config.go`、`status.go` | tree/session/export 退休与同一 owner 清理路径，容量上限和有界 opaque 诊断。 |
| `packages/storage/capabilities.go`、`capability_validation.go`、`files.go` 及 native／wrapper／HTTP 的 open 能力文件 | 仅在确有缺口时补齐普通文件的独立 metadata access、execute/share 声明、稳定引用身份与 backend 身份预检；关闭尝试机制归中立关闭决定。 |
| `packages/smb/*_test.go`、`packages/smb/internal/wire/*_test.go` 与受影响的 storage／HTTP open 测试 | 协议边界、名字竞争、容量零效果、访问与共享矩阵、响应丢失和跨 adapter 引用身份。 |
| `docs/design/client/smb-endpoint.md`、受影响的 server design、`docs/testing.md` 与对应 implemented Agent Note | 随实际代码记录端点结构、错误路径和决定。 |

FileId 形状须保留 7.4 I/O、7.5 枚举、7.6 删除义务与第 8 项名字／范围控制需要的对象身份和 owner；本段不建立那些命令的成功路径。

### 打开意图、FileId 与容量

```text
OpenIntent = {signedSession, tree, principal, trustedVolume, authorityEpoch,
              normalizedComponents, desiredAccess, shareAccess, disposition,
              createOptions, contexts, actionID, guardedSelection, responseBudget}
HandleOwner = {fileID: (sessionIncarnation, monotonicCounter), treeID,
               nodeID, authorityEpoch, kind: File | NodeReference,
               grantedAccess, useClaim, reference, openActionID,
               state, admittedIO, activeCloseAttempt, retainedAttempts[]}
CloseAttempt = {actionID, generation, immutableReferenceAndIntent,
                releaseFact: unknown | determinedFalse | determinedTrue,
                barrier: none | pending | settled | unknown, semanticError}
```

`OpenIntent` 由已验证请求建立，不能只存路径或 response。动作 ID 在对象效果前按 `FileSessionStatus.ActionEpoch` 生成，并与不可变负载保留在 owner。中立 `FileActionReceipt` 报告动作状态，不能单独还原打开引用、属性、outcome 或关闭结果；响应丢失时用同一 ID、同一方法和同一负载重投取得 typed result。只有打开动作的 `NotExecuted` 才允许重新观察名字并生成新打开动作；`Unknown`／`Retired` 不证明零效果。关闭动作的同 generation 换 ID 还要求中立层给出绑定本引用和原尝试的 `CloseActionNotExecutedError{CurrentEpoch}`，证明新的显式动作在任何关闭效果前因 ID 落后于清理 epoch 被拒绝。Windows 应用只收到真实 NTSTATUS，内部 ID 留在有界宿主诊断账本。

FileId 由 session incarnation 与单调计数组成 128-bit 不可复用打开实例标识；重启、新 session 或计数耗尽均不得复用。索引同时校验已签名 session、TreeId 与 FileId。相同 export 的多个 tree 可共用 authority FileSession，但每个 FileId 仍由创建它的 tree 持有并随该 tree 清理。`MaxHandles` 包含 opening、live、cleanup-only、barrier-only；`MaxUnresolvedOwners` 和诊断字节另有上限。每次 CREATE 在可能创建／截断对象之前预留 FileId、响应编码、open action 和 cleanup owner 的容量；中立层的 close receipt 容量由已交付的可恢复关闭能力预留。已释放但回执仍在历史期的引用继续占用中立容量，新 CREATE 不能借端点空闲 FileId 绕开这份限制。引用即使与错误同时返回，也由预留 owner 清理；额度不足时新 CREATE 在权威效果前失败。

`HandleOwner` 依次经过 `Reserved → Opening → Live → Closing → Released`。取得引用但响应不能成功公布时进入 `CleanupOnly`；释放已确认而 barrier 待结算时进入 `BarrierOnly`。本地响应发送失败不回滚已完成权威效果。`Live` 操作入场增加计数；切到 `Closing` 先封门，再等待此前已接纳操作完成。owner 只在引用释放、barrier 结清和本机清理责任结束后删除；历史权威回执按自身保留期计费。

### 从已签名 CREATE 到权威结果

1. `connection.run` 在帧、compound 和 credit 上限内解析命令；`dispatch` 对原始命令字节验签并核对 session、tree、身份、可信 volume、authority epoch 与请求准入。未签名或过期的请求在 backend 前拒绝。related context 只在这一已签名 frame 内存在。
2. wire parser 在任何 authority 效果前验证 StructureSize、offset／length、UTF-16 完整性、context 数量／对齐／重复、选项与 disposition 组合，以及 response 最大长度。ADS、reparse、durable／persistent、DFS、replay 与效果未知的 context 按明确状态拒绝；不会暗中降级为普通打开。`FILE_SUPERSEDE` 和 delete-on-close 创建意图在各自后续任务实现前拒绝。
3. `windows_name.go` 逐组件核验可表示性；`name_resolver.go` 对每级父目录取得完整、有界的权威目录 metadata，按 Windows case-fold 验证唯一性，并形成 parent/edge/revision guards。未观察完整同级目录或存在不可表示歧义时失败。非空最终 leaf 由带 guards 的 `ChildSelection` 交给权威 `OpenAt`／`OpenChildRef`；不能先 `LookupAt`，再执行无 guard open。
4. `NameLength=0` 是 share root。由可信 export 上的 `Storage.Stat(ctx, "")` 得到 root NodeID，在同一 FileSession 以 `OpenNodeRef(rootID, NodeRefOptions{Kind: NodeDirectory, Target: SameNode(rootID), Action, Use, MetadataAccess})` 取得引用。只接受 `FILE_OPEN` 或 `FILE_OPEN_IF` 对现存目录的打开分支；`FILE_CREATE` 因 root 已存在而失败，overwrite、非目录与截断在效果前拒绝。`Stat` 本身不是活引用或 share admission。
5. 在对象效果前展开 generic rights，检查每个具体 right、share flag、对象类型、业务授权、readonly/metadata 条件、身份与 allocation capability，并构造中立 `UseClaim`、`OpenAtOptions` 或 `NodeRefOptions`。authority 在最终排序点双向比较新旧 Uses/Deny，判断存在性、排他创建、创建／清空、初始 metadata 与引用保留；端点本地观察不取代这次核对。
6. 返回后核对被承诺的结果结构：Attr NodeID 等于引用的稳定 `ReferenceNodeID`、outcome 符合 disposition、分配量已知、时间与 Windows 属性可编码。违约不能报告成功，任何非 nil 引用仍由 owner 清理。安装引用和权威结果后才编码并公布 FileId。连接断开、编码失败或 deadline 先结束时，只按原 open action 核对，不凭旧路径重新执行另一打开。

### Windows 访问与共享映射

`GENERIC_READ`／`GENERIC_WRITE`／`GENERIC_EXECUTE`／`GENERIC_ALL` 先按文件或目录类型展开；不能在打开效果前确定授予集的 `MAXIMUM_ALLOWED` 和未支持 right 明确拒绝。MS-FSA 的 share 分类只包含 data／execute read、data／append write 和 DELETE；属性及 EA rights 不参加三组 share 冲突。`Uses` 是冲突声明，不自行授予 `File` 或 `NodeReference` 方法。

| Windows right／share flag | 中立声明 | 实际授予 |
|---|---|---|
| 文件 `FILE_READ_DATA`／`FILE_EXECUTE`；目录 `FILE_LIST_DIRECTORY`／`FILE_TRAVERSE` | 分别加入 `Uses.ReadData`、`Uses.ReadEntries` | execute／traverse 形成 share-read 声明，字节读和目录枚举仍需各自 right。 |
| 文件 `FILE_WRITE_DATA`／`FILE_APPEND_DATA`；目录 `FILE_ADD_FILE`／`FILE_ADD_SUBDIRECTORY` | 加入 `Uses.WriteData` | 目录引用不获得字节写；名字创建另行授权。 |
| `DELETE` | 加入 `Uses.DeleteName` | 打开不执行删除；后续操作仍需请求授权。 |
| `FILE_READ_ATTRIBUTES`／`FILE_READ_EA`、`FILE_WRITE_ATTRIBUTES`／`FILE_WRITE_EA` | 在引用上授予 `MetadataAccess.ReadMetadata`／`WriteMetadata`，不增加 Uses | 7.4 属性命令按实际 right 再检查。 |
| 缺少 `FILE_SHARE_READ` | `Deny.ReadData | Deny.ReadEntries` | 阻挡其它引用的字节读和目录枚举声明。 |
| 缺少 `FILE_SHARE_WRITE`／`FILE_SHARE_DELETE` | 分别为 `Deny.WriteData`／`Deny.DeleteName` | 与其它入口的 Uses 双向比较。 |
| 仅同步／控制 right | 不增加数据 Uses 或 metadata 权限 | 不推导额外数据方法。 |

设置的 `FILE_SHARE_*` 位仅表示不加入相应 Deny。普通文件 `OpenAtOptions.Read`／`Write` 只由已授予的字节访问决定，独立的 `MetadataAccess` 不暗中扩大字节权限。`Read ⇒ Uses.ReadData` 与 `Write ⇒ Uses.WriteData`，但 execute 可以加入额外 `Uses.ReadData` 而不给 `ReadAt`。`FILE_READ_DATA + FILE_WRITE_ATTRIBUTES` 映为 `Read=true, Write=false, Uses=ReadData, MetadataAccess=WriteMetadata`；`FILE_EXECUTE + FILE_WRITE_DATA` 映为 `Read=false, Write=true, Uses=ReadData|WriteData`。没有字节读写 right 的普通文件，包括 DELETE-only、execute-only、attribute-only，走 `NodeRefOptions{Kind: NodeRegular, Use, MetadataAccess}`，仍能以 `FILE_CREATE`／`FILE_OPEN_IF` 创建缺失目标；需要清空内容的 disposition 必须有字节写权。

`FILE_OPEN` 需要目标存在；`FILE_CREATE` 对任何已有目标报冲突；`FILE_OPEN_IF` 对已有目标保持内容，对缺失目标创建；`FILE_OVERWRITE` 需要已有普通文件并清空；`FILE_OVERWRITE_IF` 对已有文件清空、缺失时创建。清空与 `ARCHIVE` metadata 更新在同一次 guarded 权威打开中提交，`READONLY` predicate 在最终事务核对。目录、NodeReference 和无字节写权的打开都不能偷做截断。未支持的 CreateOptions 位在效果前拒绝；被支持的目录／非目录与同步位要同引用类型和响应一致。

### CLOSE、related compound 与退休

显式 `CLOSE` 先核对拥有 FileId 的 session/tree 和当前业务 `OpFileClose` 授权；拒绝时 FileId、引用及 claim 不变。成功准入后，owner 切至 `Closing`，封住新 I/O 并排空已接纳调用。它通过绑定该引用的 `CloseOwnerStatus` 核对释放事实、内部清理已接纳的 `Current`、下一 generation、清理 epoch 和 `Ready`；有 `Current` 时接管原 ID，即使尝试由断线／retirement 清理建立也不另起动作。只有完整 backend 链报告 `Current=nil, Ready=true` 时才在本地保存新 `CloseAttempt{Action,Generation}`、原引用、owner 和不可变意图，ID 从状态的 `CurrentEpoch` 生成，然后调用中立 `CloseWithAction`。状态不可达或 `Ready=false` 且无 `Current` 时保留 cleanup owner，等待回执名额或继续查询，不凭普通 FileSession Status 或过时本地 epoch 推断可关闭。释放事实来自 `ReferenceCloseResult`：`Released=false,Determined=false` 是未知，只可按原 action／generation 查询或同 ID 重投；中立实现可安全重新进入原引用的 native close 以结算该尝试，但不能重复权威效果；`Determined=true,Released=false` 证明原引用和 claim 仍受权威保护，旧 ID 保留失败结果，下一次获授权 CLOSE 或内部清理可在同一 owner 下生成新 ID／递增 generation；`Released=true` 不再 native close，只让同一尝试的 barrier 单调结算。中立 session 退休后 `CloseOwnerStatus` 和已持有引用的窄 close admission 仍可推进当前动作或下一 generation，普通数据和新打开继续拒绝。若一个新显式 close ID 在中立层准入及引用效果前因 ID 落后于清理 epoch 收到 `CloseActionNotExecutedError{CurrentEpoch}`，owner 保持同一 generation、记录旧 ID 的未执行证明，再用当前 epoch 生成新 ID；未来 epoch 的 `EINVAL`、普通 `ESTALE`、取消或 Unknown 绝不允许这条 remint。旧 ID 即使后来查询为 Retired，也不能再用于产生效果。`QueryFileAction.Completed` 不能单独代替 typed close result。

已释放且 barrier 待结算时，本机移除可访问引用，保留 `BarrierOnly` owner、原 ID 和查询容量。确定未释放时保留 `CleanupOnly` 引用；释放未知时本机也保留 owner 与原 ID，但不能把本机 Unknown 说成 authority 的旧 claim 一定存在，因 native 可能已释放而 HTTP 响应丢失。中立 close history 过期、authority 换代或引用不再可证明时，旧 FileId 失效，端点报告 I/O 未知，不能以新 session 或旧路径接管该引用。HTTP 特有 pending barrier 在 adapter 边界转换成中立结算事实。

`TREE_DISCONNECT`、`LOGOFF`、TCP 断线、unpublish 和 server stop 复用同一 owner 退休器：封住新准入，排空已接纳 I/O，按当前尝试事实核对／重投或在确定未释放后开下一尝试，结清 barrier，再退休 authority session。退出一个 tree 只清其 FileId；最后一个 tree 结束才关闭共享 authority FileSession。已接受的退休属于固定清理责任，不因新业务授权撤销而丢弃；新外部显式 CLOSE 和 TREE_DISCONNECT 仍执行当前授权。清理失败时 stopping owner、诊断记录和容量保留供继续结算，不报告资源已回收。

related compound 的 FileId 占位仅绑定同一已签名 frame 内前一个成功 CREATE；后继命令仍独立核对 session/tree。前驱失败、跨 frame 或跨 tree 均不能借占位碰到另一引用。CREATE 成功而 CLOSE 失败时不回滚已完成的打开；owner 继续存活或进入清理。编码子响应和 credit 以前预算最坏结果。SMB CANCEL、replay flag 与 durable reconnect 不解释为中立 close action 重投。

### 状态映射与诊断

| 条件 | SMB 状态原则 |
|---|---|
| 身份、签名或业务授权失败 | `STATUS_ACCESS_DENIED`；权威效果前拒绝。 |
| 目标缺席、排他创建撞名、对象类型不符 | 只在最终权威条件已知时映为相应 `STATUS_OBJECT_*`。 |
| 双向 share 冲突 | `STATUS_SHARING_VIOLATION`；创建／截断前失败。 |
| 无写权引用要求 overwrite，或未交付的 disposition／context | 分别拒绝访问或 `STATUS_NOT_SUPPORTED`；无内容效果。 |
| 本机／权威容量耗尽 | `STATUS_INSUFFICIENT_RESOURCES`；必须证明未产生对象效果。 |
| FileId 不属于该 tree／session，或 authority epoch 失效 | 无效／失效句柄；不按路径重绑。 |
| 关闭已确定 `Released=false` | 本次报告原清理错误，owner／claim 保留；后续独立尝试可续试。 |
| 关闭已确定 `Released=true`、barrier 未结算 | 报真实 barrier 错误，同 ID 续结算。 |
| transport 中断、动作 Unknown／Retired、不可证实的身份／分配量／barrier | I/O 未知；不映为不存在、成功或旧值。 |

CREATE 的 namespace guard 或 metadata predicate 冲突，只有原动作明确未执行才可重新做完整有限的权威观察并用新 ID 尝试；持续竞争返回明确冲突。端点诊断保存 opaque owner/action ID、export、owner 状态、最后错误类别及 active／cleanup-only／barrier-only 计数，按字节和条数限额；不保存 SID、密钥、原始路径、文件内容或未经净化的底层错误。状态不能把仍有清理责任的 owner 算作已回收。

## 备选方案

**把 7.3–7.6 与第 8 项合成一个文件面 PR。** 可以一次演示更完整的网络盘，却混合 guarded 打开、I/O revision、目录快照、名字修改和锁排序，无法聚焦审查每个权威效果的失败边界。typed FileId 与稳定 owner 保留后续能力需要的对象形状。

**把中立关闭修正和 SMB CREATE/CLOSE 放在同一 PR。** SQLite 的共享 claim 最终释放、关闭 action/receipt 与 HTTP 清理容量跨多个包，且可由现有 storage／HTTP 入口独立验证。先交付该中立契约，使本 PR 只审查 Windows 请求如何使用它；若中立预检缺失，本 PR 在产生打开效果前拒绝。

**只用路径 `OpenFile` 和本地 share 表。** 路径观察与打开之间可以换对象，另一个 SMB session 或 Linux／SDK 入口也可在本地表外授予冲突访问，不能满足 R-FS-8 与 R-WIN-6 的最终排序。

**收到 CLOSE 错误就丢弃 FileId。** 失败时引用可能仍存在，或已释放但 barrier 尚未结算；仅靠错误类别无法正确撤销 owner。稳定 owner 和 typed 中立结果分别结算两种事实。

**在 7.3 同时支持 SUPERSEDE 和 delete-on-close。** SUPERSEDE 的原位重置仍须保留已有对象 ID；delete-on-close 增加持久删除义务与 pending 观察。Issue #30 分别把它们放在 8.1 和 7.6；本段明确拒绝未交付请求，FileId 结构保留后续状态。

## 验收标准

- 五种 disposition 对目标缺席、已有文件、目录／metadata-only 类型、并发创建赢家、创建／截断与名字替换竞争均有直接 backend 和 HTTP adapter 用例。guard、child 条件或 share 冲突发生时，没有对象、内容、属性、引用、Use 或分配量副作用。
- 空名字 root 的 `FILE_OPEN`／`FILE_OPEN_IF` 通过 root NodeID 和同一 session 的 `OpenNodeRef` 获得目录 FileId；`FILE_CREATE`、overwrite、非目录与截断无副作用失败。root 引用可关闭且可供后续 7.5 枚举。
- 普通文件与目录的 read/write/delete access × 三种 share 位双向矩阵跨两个 SMB session 和至少一个非 SMB 入口验证。metadata-only、DELETE-only、execute-only、execute+write、字节读+属性写、字节写+属性读分别验证 byte flags、Use、metadata permission 和结果 FileId；无字节权不取得 `File` 字节方法。
- `MaxHandles=1`、底层 `MaxFiles=1`、open action/cleanup receipt 容量满、opening 与 cleanup-only 持有容量时，新 CREATE 在权威效果前拒绝；已释放但回执未过期仍占用的 capacity 不被新 CREATE 借用。保留 owner 的 CLOSE、绑定引用的 CloseOwnerStatus、同 ID 查询／重投及 session 退休仍可推进。
- 响应丢失、取消、open 返回 error+非 nil reference、Attr ID 与引用 ID 不符、allocation 缺失、编码失败各保留 owner 并按原动作恢复或清理；同名 replacement 不使旧 FileId 改绑。
- CLOSE 经直接 backend 与 HTTP adapter 验证未知结果仅重投原 ID，并允许 native 对原尝试安全重进而不重复删除／释放效果；显式新 ID 在执行前落后于 native 清理 epoch 时只凭 `CloseActionNotExecutedError{CurrentEpoch}` 在同 generation 换 ID；未来 epoch 的 ID 以 `EINVAL` 无回执拒绝，旧 ID 在证明期为 NotExecuted、缺失时可转 Retired，始终不能重新执行；确定未释放后才以新 generation 开始下一次 close；已释放而 barrier pending 时仅结算原动作。每步核对 FileId、share claim、已接纳 I/O、owner 配额、native 尝试与实际权威效果次数。原语义错误与 Released 事实同时保留。
- 显式 CLOSE 授权拒绝保持 FileId／claim；随后断线、LOGOFF、tree 退出、unpublish 或 stop 的内部退休仍可清理已接受的 owner。退休后内部清理先发起 Pending／Unknown 尝试、外部随后继续时，外部须由精确 FileId 的 `CloseOwnerStatus.Current` 接管原 ID；内部已确定未释放时，仅 `Ready=true` 才使用 `NextGeneration`／`CurrentEpoch` 新建尝试，普通 FileSession Status 仍拒绝。一个 tree 退出不关闭其它 tree 共用的 authority session。
- 同一已签名 compound 的 CREATE→CLOSE 支持 all-ones related FileId；前驱失败、跨 tree／frame 和畸形占位均不触及错误引用。SUPERSEDE、delete-on-close、未支持 context 在权威效果前拒绝。
- 对本 PR 实际受影响的 SMB、storage open 和 HTTP open 路径执行正常及必要 race 验证，运行对应 build、vet、格式和文档链接检查，记录实际输出。原生 Windows 资格由 PR9／PR10 的独立提案验收。

## 风险

每级目录的完整 metadata 观察使深目录和大目录冷路径更慢；预算耗尽必须明确失败。可优化为与 revision 绑定的权威索引或可证明失效的观察缓存，不可退回不完整查找。中立 action history 有期限；回执到期后的未知结果可能无法在原 Windows 请求内结清，有界诊断不能冒充应用层成功。第三方 backend 若不能证明 volume 绑定、稳定引用身份、分配量或可恢复 close，受影响 tree 在效果前拒绝。

本段只让 CREATE/CLOSE 可审查，不宣称已满足文件 I/O、目录枚举、通知、WNet 映射或完整 Windows drive 发布资格。普通用户 445、登录会话关联、redirector 缓存和 Home／Pro × x64／ARM64 的原生证据由后续[映射与夹具](2026-09-28-smb-native-wnet-fixture.md)和[资格验收](2026-09-28-smb-native-qualification.md)取得。
