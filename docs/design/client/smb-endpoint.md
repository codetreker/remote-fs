# 本机 SMB 端点

`packages/smb` 是 Windows client 进程内的本机呈现层。Windows 系统 SMB client 通过 loopback TCP 连接它；端点把认证后的 share 连接绑定到调用方提供的 `storage.FileStorage`。SMB 只存在于 client 与同机操作系统之间，client 到远端 authority 的边界仍是平台中立的 storage／HTTP 契约。

端点支持 SMB 3.1.1 协商、认证、消息签名、share 连接、CREATE/CLOSE 与会话清理。五种非 supersede disposition 建立稳定 FileId 和权威 share claim；其余文件、目录、名字和锁命令返回明确的不支持结果。该命令集合不构成可浏览或可读写的 Windows network drive。

## 一、嵌入与发布

`smb.New` 接受 `Config{Authenticator, AuthorizeIdentity, Authorize, Limits, Logger}`。构造函数先验证两个授权接口、认证 provider 和全部资源上限，不取得 Windows credential、不启动 goroutine，也不打开 listener。`packages/smb/windows` 提供 SSPI `Authenticator`、读取进程身份的 `CurrentIdentity` 和只允许该确切身份的 `AllowIdentity`；非 Windows 构建保留同一 API，并在实际取得平台能力时返回不支持。

一个 `Server` 可以发布有限个 `Share`。每个 share 携带不区分大小写的 SMB 名字、业务授权 label `Volume`、实际存储 pin `BackendVolume storage.VolumeID`／`RootNodeID` 和一份调用方拥有的 `storage.FileStorage`。发布时验证这些配置及 `CheckFileStorage`，但不建立 FileSession，也不接管 backend 的关闭责任。名字为空、过长、含 SMB 分隔／保留字符、前后空白或与 `IPC$` 冲突时拒绝。

`Serve` 只接受已经绑定到 loopback TCP 地址的 listener，并在验证成功后取得其所有权。每条 accepted connection 还要再次核对 peer 是 loopback；其它来源在进入协议处理前关闭。Server 不创建 WNet 映射、不安装驱动、不修改防火墙、系统 SMB 配置或全局缓存策略。

```text
Windows SMB client
        │ loopback TCP, SMB 3.1.1
        ▼
Server ── connection ── authenticated session ── tree
  │                                             │
  └── Export ── auth label + backend pins + FileStorage ◀──┘
                         │
                         ▼
              remote storage / authority
```

`IPC$` 只建立 control tree，并且只支持关闭该 tree。普通 share 的第一次 `TREE_CONNECT` 核对 backend 的 Volume／Authority／RootNodeID 与宿主 pin，建立 FileSession，预检完整文件能力，再核对 session descriptor 与 Status.Epoch；验证成功后为该 authenticated SMB session 与 export 安装一份 authority session；同一 SMB session 对同一 export 的多个 tree 共用它。不同 SMB session、不同 export 或不同本地身份不共享 FileSession。

## 二、协议与消息完整性

连接使用 Direct TCP framing。四字节前缀中的类型必须为零，长度在读取 payload 前受 `MaxFrameBytes` 限制；短帧、越界 offset、无效 alignment、非法 UTF-16、重复的唯一 negotiate context 和超量 compound/context 都使连接失败，不能用截断或默认值继续解释。

首包可以是 SMB1 形状的 wildcard negotiate，它只选择 SMB2 framing；随后必须进行真正的 SMB 3.1.1 `NEGOTIATE`。端点只接受 SHA-512 preauthentication integrity 和 AES-CMAC signing，返回的 `MaxTransactSize`、`MaxReadSize` 与 `MaxWriteSize` 都来自 `MaxIOBytes`。preauthentication hash 使用收到和发出的原始 wire bytes，不能从解码后的字段重建。

SMB2 compound frame 在一个有界 payload 中解析。每项保存自己的 header、body 和用于签名的完整 command bytes；related compound 只继承同一 frame 中前一项的 SessionId、TreeId，以及成功 CREATE 产生的 FileId。继承值在解码后的执行上下文中传递，不改写用于验签的 command bytes。all-ones FileId 只可使用这一 frame 的前驱结果，半个 all-ones 值拒绝。混用 related 与 unrelated 风格、把 `NEGOTIATE` 或 `SESSION_SETUP` 放入 compound、非法 offset／alignment、非法 credit 或重复 MessageId 都拒绝。response 逐项保留对应状态，related 前项失败时后项不执行受控效果。

认证完成后，每个携带 SessionId 的请求都必须使用该 session 的 AES-CMAC signing key 验证，合法请求及能够归属该 signer 的错误 response 也由同一 key 签名。unsigned session request 的拒绝保持 unsigned；带 signed flag 但 MAC 错误的请求得到 signed access-denied。已建立 session 的 reauthentication 在解码 token 前先验签，畸形或被篡改的 exchange 不能得到 unsigned 旁路。sessionless ECHO 可以在 negotiate 后使用，并且不继承其它 session 的 signer。未知或已经退役的 session、replay/DFS flag 与失效身份均在访问 tree 或 backing 前失败。session key 只用于派生 signing key，临时 token 与 key buffer 在使用后清零；日志不写入 token、key、SID 或显示名。

## 三、Windows 身份与授权

`packages/smb/windows` 通过 SSPI `Negotiate` 接受 SPNEGO token。每次 authentication exchange 独立取得 inbound credential；`AcceptSecurityContext` 必须建立 integrity-capable、非 null session 的 security context。完成后从 context token 读取用户 SID 与 `TOKEN_STATISTICS.AuthenticationId`，把二者组成授权身份；account display name 只用于诊断，不参与相等比较或准入。

标准组合先用 `CurrentIdentity` 捕获宿主进程 token 的 SID 与 logon-session ID，再把结果交给 `AllowIdentity` 作为 `Config.AuthorizeIdentity`，只允许精确匹配。每次初始 authentication 和 reauthentication 都在安装 principal／signer 或替换 expiry 前调用它；明确拒绝产生 access denied，无法决定产生 I/O failure。anonymous、Guest、LocalSystem、LocalService 与 NetworkService 明确拒绝。同一个 SID 的另一次登录不是同一身份，不能接管 `PreviousSessionId`、复用 session 或访问它的 tree。

SMB 本机身份与远端业务身份是两层独立保护。`AuthorizeIdentity` 决定一个已验证的操作系统主体能否建立或刷新 SMB session；端点随后把该 `Principal` 放进 request context，并用独立的 `Config.Authorize` 对 trusted volume 与实际 FileSession 操作作准入。`TREE_CONNECT` 授权 `file.backend-identity`、`file.session-open` 与 `file.status`，后台续期授权 `file.renew`，显式 tree/session 清理授权 `file.session-close`。CREATE 按引用类型及实际打开效果授权 open、metadata 和修改操作；完整目录观察单独授权 `file.observe-directory-metadata`，root Stat 授权 `volume.stat`。显式 CLOSE 授权 `file.close`；远端 HTTP 对绑定引用的关闭状态查询仍独立授权。已接受的内部退休继续履行原清理责任。backing remote storage 继续使用调用方为远端 authority 配置的身份和凭据，SMB SID 不替代它。

authentication exchange 受 `HandshakeTimeout` 约束，完成后的 identity 另受 provider 返回的 security-context expiry 约束。未完成的 exchange 到期后自主关闭，不等待下一次 `SESSION_SETUP` 才回收；reauthentication 使用新的 generation，旧 timer 不能关闭新的 exchange。identity 到期后，普通命令收到 signed `STATUS_NETWORK_SESSION_EXPIRED`，session、signer 与 tree 保留以允许 signed SESSION_SETUP 重新认证；只有同一 SID 与登录会话的成功 reauthentication 才更新 expiry 并恢复工作，LOGOFF 始终可以清理该 session。

## 四、文件适配的组成

| 文件 | 责任 |
|---|---|
| `internal/wire/files.go`、`create_contexts.go` | CREATE/CLOSE 的固定体、偏移、名字、contexts 与 FileId，响应结构 |
| `names.go`、`name_compare_*.go` | 可表示名字、保留名、UTF-16 code-unit 等价与排序 |
| `namespace.go` | 完整权威 sibling 观察、pinned root、revision/edge ancestry guards |
| `tree_capabilities.go`、`authority_session.go` | backend/session identity、完整能力预检、共享 FileSession 与续期 |
| `commands_file.go`、`windows_metadata.go` | CREATE 意图、访问／share／disposition、Windows metadata 与原子结果投影 |
| `file_handles.go`、`commands_close.go`、`authority_recovery.go`、`handle_diagnostics.go` | typed owner、准入／容量、恢复、关闭尝试与结算 |
| `session_cleanup.go`、`connection.go`、`server.go` | tree/session/export 退休、compound 执行与 signer 所有权 |

每个 authority session 保存不可变 `FileSessionIdentityResult{Backend, SessionEpoch}`。Backend 的 Volume、Authority 与 RootNodeID 全部必须和经验证 backend 相同，Volume／Root 还须匹配宿主 pin，SessionEpoch 必须等于其 Status.Epoch。Share.Volume 只用于业务授权。新的 tree 在公布成功前完成 AtomicFileOpener、NodeReferences、DirectoryMetadataObserver、StableReferenceIdentity、OpenMetadataAccess、FileActions、AllocationReporting 与 RecoverableReferenceClose 的完整链检查；wrapper 缺失能力时明确拒绝。身份与中立能力的定义见[文件句柄设计](../server/file-handles.md#一身份与会话)。

### 名字选择

SMB 名字是严格 UTF-16，转换为可逆 raw leaf；NUL、孤立 surrogate、ADS、保留设备名、非法组件与尾部点／空白拒绝，不做 Unicode normalization。Windows 使用 CompareStringOrdinal 的显式长度、case-insensitive 比较；其它平台按规范的 BMP 大写表映射 UTF-16 code unit，代理项和未列项保持原值。比较不能使用 Go EqualFold 或 ToUpper。

每级父目录在 DirectoryMetadataObserver 调用前以已验证 principal 和可信 Share.Volume 授权 OpFileObserveDirectoryMetadata，然后取得完整、有界捕获；名字观察不要求 replication.snapshot。拒绝时不调用 observer，也不产生打开效果。所有 sibling 的名字都先核对可表示性与大小写唯一性；坏名字或等价重名使整次按名选择失败，不能省略或猜测。无关的合法符号链接保留在完整观察中；选中的叶或中间 component 为符号链接时拒绝。根 Stat 每次都必须匹配固定 RootNodeID。选择保存父 NodeID、精确 raw edge 与各级 directory revision，最终以带 NamespaceGuards 的 ChildSelection 交给 authority；取消、观察不完整或祖先身份重复使选择失败。

### 打开意图与返回事实

FILE_OPEN 要求已有目标，FILE_CREATE 要求缺席，FILE_OPEN_IF 保持已有目标或创建。FILE_OVERWRITE 要求已有普通文件并清空，FILE_OVERWRITE_IF 清空已有文件或创建；两种清空都必须有字节写权。DataFile 的 READONLY 检查与 metadata predicate 在同一 guarded authority 动作内提交；只读已有文件的 DELETE-only FILE_OPEN 仍可取得 NodeReference。overwrite 把请求属性集合加 ARCHIVE 作为原子结果；已有 HIDDEN/SYSTEM 位必须也出现在请求中，否则在清空前拒绝。已有对象的 smb.windows namespace 版本或缺席条件同样固定在原意图中，不能依据旧 metadata 清空后来变为只读的对象。

有字节权限的普通文件经 OpenAt 返回 File；目录、metadata-only、DELETE-only、execute-only 经 OpenChildRef 返回 NodeReference。空名字只打开固定根目录：OPEN/OPEN_IF 通过同一 FileSession 的 OpenNodeRef 和 SameNode 条件取得引用，OPEN_IF 不创建；CREATE 撞名，overwrite／非目录意图拒绝。SUPERSEDE、delete-on-close 和未支持效果在 backend 前拒绝。

Generic rights 按规范完整展开，MAXIMUM_ALLOWED 和未支持权限拒绝。GENERIC_ALL 展开为 FILE_ALL_ACCESS（0x001f01ff），包含未交付的 DELETE_CHILD、WRITE_DAC 和 WRITE_OWNER，因此返回 STATUS_NOT_SUPPORTED；不能删掉这些权限后部分授予。READ_DATA 授予字节读，WRITE_DATA/APPEND_DATA 授予字节写；EXECUTE 声明 ReadData share Use，却不授予字节读。目录 LIST_DIRECTORY/TRAVERSE 声明 ReadEntries，ADD_FILE/ADD_SUBDIRECTORY 声明 WriteData；目录引用没有字节方法。DELETE 声明 DeleteName，打开不删除。READ_ATTRIBUTES/READ_EA、WRITE_ATTRIBUTES/WRITE_EA 分别授予独立 ReadMetadata、WriteMetadata，不参加 data/write/delete share 分类。

data／execute／write／append／DELETE 权限参与 sharing；Use 非零时，缺少 SHARE_READ 设置 Deny.ReadData|ReadEntries，缺少 SHARE_WRITE 设置 Deny.WriteData，缺少 SHARE_DELETE 设置 Deny.DeleteName。metadata/control-only 的 Use=0 时忽略 ShareAccess，Deny=0，不能形成双向 Windows sharing conflict；authority 在实际 NodeID 上双向比较参与者的 Uses/Deny。OpenAt Read/Write 只由字节权限决定，MetadataAccess 显式独立。execute+write 的 Use 因而同时含 ReadData|WriteData，但没有字节读。支持目录／非目录与互斥同步选项；同步选项要求 SYNCHRONIZE。

CREATE 属性先接受支持的可设置位与 DIRECTORY，再去除 DIRECTORY/NORMAL，作为规范打开意图的属性集合。NORMAL 与其它位组合时忽略；DIRECTORY 不独立决定 kind，实际种类由目录／非目录 CreateOptions 和解析目标决定，不因属性与 options 不同而额外拒绝。DIRECTORY+FILE_DIRECTORY_FILE 可创建／打开目录，DIRECTORY+FILE_NON_DIRECTORY_FILE 仍按普通文件种类处理；响应结构位从捕获的 kind 派生，不写入 smb.windows。已有目标与目录／非目录 option 的类型冲突仍拒绝，未支持属性位仍在效果前拒绝，持久 metadata 编码保持严格。

CREATE contexts 有严格数量、alignment、Next 范围、重复与 payload 检查。well-formed DHnQ、DH2Q、RqLs、AlSi 与协议 reserved GUID 仅解析并忽略，response 始终 oplock NONE、无 context，不建立 durable/persistent/lease 状态。畸形 recognized context 或互斥 durable 请求是 invalid parameter；well-formed reconnect、MxAc、QFid、以及 ExtA/SecD/TWrp、app-instance、virtual-disk 与未知 context 在效果前明确不支持。协议 reserved CREATE/CLOSE 字段按标准忽略。

动作保存原 action ID、方法、selection 与 options。引用身份、Outcome、已知 allocation 的精确字节值、共同时间和 Windows 属性从同次原子结果验证；不要求 4096 字节粒度，也不推断尚未取得的 volume geometry。Created 必须有 BirthTime/ChangeTime，Reset 必须有 ChangeTime；既有对象缺失的可选时间以 unknown 零值表达。ReferenceNodeID 必须等于 Attr.ID，不用后续 Stat 拼出成功响应。error 与非 nil 引用同时返回时，预留 owner 立即接管；无引用的未知结果保留同 ID、同输入恢复入口。只有绑定原动作的 NotExecuted 证明允许丢弃未执行意图或重新观察。内部恢复续查和 replay 使用原授权后的不可变意图，不重新执行 endpoint 当前策略检查；远端 HTTP 公共 RPC 继续各自授权。公布 FileId 前在本地锁外读取新的 raw Status，核对固定 session epoch 和 liveness；随后在 authority install、tree admission 与所属 session 锁内核对 context、tree/authority stopping、session retirement、本地确认期限和原预留槽，原子安装 Live。失败保留已获引用与 owner，不回滚已完成效果。Windows 应用收到 NTSTATUS，内部动作 ID 留在有界宿主诊断中。

## 五、容量与 FileId owner

Limits 分别限制 export、connection、session、tree、pending request、compound/context、frame/I/O/token、完整目录观察、handle、未结清 owner 与诊断字节。所有值显式选择，零值不表示无界。MaxIOBytes 同时成为协商公布的 transact/read/write 上限；协商后完整 request 受 I/O 加固定 envelope 约束，control command 另受 68 KiB 上限且只消耗一个 credit，数据 command 的 credit charge 覆盖每个 64 KiB payload 单元。FileSessionOptions 另限制原生引用、owner、range、等待与动作历史。

CREATE 在对象效果前预留 response frame 空间、FileId 与 owner。server 全局 owner registry 包含 reserved/opening、live、cleanup-only、barrier-only；其上限为 min(MaxHandles, MaxUnresolvedOwners, MaxDiagnosticBytes/256)，每份可能未结清的 owner 在效果前预留固定 256 字节诊断槽。原生／HTTP 的 close receipt 历史继续按自身期限计费；端点回收一个 FileId 不使该容量提前可复用。容量不足在创建／清空前失败。

FileId 使用不复用的 session 实例与单调计数，避开全部或半个 all-ones 值；查找同时验证 session、tree 与固定 identity descriptor。owner 拥有一份 File 或 NodeReference、固定 NodeID、实际 access、Use、open action 和当前 close attempt。状态为 Reserved、Live、CleanupOnly 或 BarrierOnly；取消、编码／发送失败和 tree 退休不能丢弃已取得引用或未知动作。

每个 tree 单独登记文件操作。retirement 先 fence 新准入，取消 pending request，等此前已接纳操作结束，再按确切引用清理该 tree 的 FileId。backend I/O 与等待不持有 tree/session map lock。同一 SMB session/export 的多个 tree 共用 authority session，FileId 不在 tree 间迁移；单个 tree 退出而兄弟 tree 仍 live 时，未结清 owner 保留，不关闭兄弟的 FileSession。整份 authority 退休的父释放证明可以结清剩余 typed 或匿名 owner，随后减少 tree/authority refs。

一次 connection 持有 negotiate transcript、credit 集合、pending requests 和 session table；全局 registry 另计所有 connection 的 session charge。仅从 connection map 删除 session 不释放全局容量。TreeId/SessionId 单调分配并检查耗尽。每份 response frame 在 session retirement 前登记为 signer 使用者；native/auth/tree 退休与最后 response 构造／签名均结束后，才移除 registry owner 并清零 signing key。

## 六、CLOSE、退休与状态

外部 CLOSE 先验证 owner 与当前业务授权。拒绝不改变 FileId、引用或 claim；OpFileClose 准入后同一 owner 排空已接纳工作，后续绑定 status/query/attempt replay 作为已接受的窄清理责任继续，不在中途重新调用 Config.Authorize。HTTP 公共 RPC 的独立授权仍然成立。没有本地 attempt 时读取确切引用的 CloseOwnerStatus：Current 非 nil 时接管其原 action/generation；只有 Current=nil、Ready=true 且未释放，才用 CurrentEpoch／NextGeneration 保存新尝试。内部退休和外部 CLOSE 使用同一 cursor，不用普通 FileSession.Status 或旧路径推导清理身份。没有本地 attempt 而 cursor 已报告 Released 时，通过确切引用的隐式 CloseWithResult 继续核对 settlement，不生成新 action。

Released=false、Determined=false 只续作原尝试；Released=false、Determined=true 保留原错误和引用，下次根据 Ready 开始新 generation。效果前 CloseActionNotExecutedError{CurrentEpoch} 是同 generation 换当前 epoch ID 的唯一证明；普通 ESTALE、未来 epoch 的 EINVAL、取消或 transport error 均不允许 remint。QueryCloseAttempt 的 Completed 只允许按原 CloseAttempt 取回完整结果，不证明释放或 settlement。

Released=true 但错误含 CloseSettlementError 时，owner 转为 BarrierOnly，保留原 attempt 和独立 SemanticErr，后续只结算该动作。Pending 与 Unknown 分别表示已知未完成、证据不可达；marker 缺席才确认完整链结算，原生语义错误仍可同时存在。identity 换代、receipt 过期或证据不足产生 I/O 未知；不会按新 session 或旧路径重新打开。中立关闭与 HTTP receipt 机制见[文件句柄设计](../server/file-handles.md#一身份与会话)。

CREATE 丢失响应、没有 typed reference 且原 action 因 lease 到期持续 ESTALE，或已有引用在父 HTTP session 退休后无法取得精确结果时，端点保留原未知错误与 owner。authority retirement gate 固定 tree membership，验证它与 refs 完全对应、没有 opening tree，全部共享 tree 已 fenced/drained，authority 禁止新 tree/open。恢复器先推进各 owner 的原引用／pending-open 精确清理，再串行取得全部 closeLifetime gate，调用确切 raw FileSession.CloseWithResult。

result.Check 有效、Released=true 且无 CloseSettlementError 是整个父 session 全部引用与完整链 settlement 的终结证明，不额外要求 Determined。恢复器在每个 handle cleanup 内终结全部剩余 typed/匿名 owner、清除 callback、归还 charge 一次；逐引用的 terminalErr、缓存 SemanticErr、原 open Unknown 和 close attempt/history 继续保留，迟到调用返回错误而不重投。父语义错误也缓存并报告，原动作不被改写为成功或未执行。父 Unknown／未释放／Pending／Unknown settlement 保留剩余责任并重试同一 parent。

隐式 HTTP FileSession.CloseWithResult 仅在原精确动作路径返回 ESTALE 且缺失 typed close result 时，以内部 file.session-release-result 读取确切 session capability 的有限终态。投影只返回已验证 native 全父释放／inline settlement、当前 server barrier 与原 releaseErr，不新建 action/receipt、延长 history 或再次 native close；不存在、到期、未验证终态或 barrier 故障仍报错。显式动作接口与 receipt 结果保持原语义，投影不提供逐动作结论。

TREE_DISCONNECT 清理自己的 handles，再释放共享 authority ref；有 live 兄弟 tree 时不能使用匿名 owner 的父关闭恢复。LOGOFF 发布 session retirement，取消除当前 LOGOFF 外的请求，关闭 authentication、全部 tree 和 orphan authority，最后等待 response frame。连接断开执行同一无授权内部退休路径。续期或业务检查失败使 authority fenced 并进入清理，旧 revision 不延长已确认 deadline。失败或未知继续持有 export、session 与全局容量供原 owner 重试。

Export.Unpublish 对 live tree 或正在取得 export 的 connect/request 返回 busy，原 mapping、tree 与 FileSession 保持有效；没有使用者后才清理并移除。export cleanup 只加入持有目标 export 的 connection/session/authority；不等待另一 export 的清理。退休 session 中已经关闭的非最后 authority 立即释放自己的 entry/export ref，其余 authority 继续承担 session retirement。最后 authority 即使 raw session 已关闭，也保留 entry/ref，直到在调用方 context 内取得 authentication lock，并以不可取消 commit 同时移除 authority、记录 resourcesClosed 和释放 export ref。超时保留该已关闭 owner，后续重试不重复 raw close。等待同一 owner 的 authentication/cleanup 服从调用方 context。

Server.Shutdown 永久停止 listener 与新工作，等待 connection 退出，清理 tree/handles/FileSession，移除确认静止并结清的 export；失败项保留供重试。调用方 backend 始终由调用方拥有。Status 报告 serving/stopping/stopped、export、live/cleanup-only connection、session、identity-expired session、tree、pending request、fenced authority、cleanup failure，以及 Handles、OpeningHandles、CleanupOnlyHandles、BarrierOnlyHandles。Server.HandleOwners() 受预留条数／字节预算约束，只暴露 opaque FileId/session/tree/NodeID、open action、close attempt、owner 状态和 LastStatus uint32，不保存 SID、密钥、路径、内容或原始 backend 错误。协议表项退役不能替代 native 释放与 settlement。

## 七、命令与需求边界

可成功的命令为 NEGOTIATE、SESSION_SETUP、ECHO、TREE_CONNECT、TREE_DISCONNECT、LOGOFF、CREATE 与 CLOSE。FLUSH、READ、WRITE、LOCK、IOCTL、QUERY_DIRECTORY、CHANGE_NOTIFY、QUERY_INFO、SET_INFO 和 OPLOCK_BREAK 不执行 backing 操作并返回 STATUS_NOT_SUPPORTED。CANCEL 的 async 语义未建立，CANCEL、无法识别或畸形 command fail closed，终止连接而不执行受控效果。

对象不存在、撞名、类型不符只有在权威条件已确定时映射相应文件错误；typed share 冲突映射 STATUS_SHARING_VIOLATION，不能被一般 EAGAIN 资源错误遮蔽。容量耗尽映射 STATUS_INSUFFICIENT_RESOURCES。无法证明的引用身份、allocation、时间、动作或 settlement 返回 I/O 错误，不能报告成功、缺席或旧值。

这套结构对应 R-FS-5 至 R-FS-9 的打开／引用部分，R-CC-14／R-WIN-6 的共享准入，R-INT-1 至 R-INT-3 的嵌入和资源，以及 R-ERR-1、R-ERR-2、R-WIN-9 的真实错误与安全准入。范围锁、文件数据与信息、目录枚举、持久关闭删除、名字修改、通知／缓存、WNet 和完整 Windows 资格仍由独立能力实现。取舍与依赖见[有界 CREATE/CLOSE](../../../.agents/notes/implemented/feature/2026-09-28-smb-bounded-create-close.md)。
