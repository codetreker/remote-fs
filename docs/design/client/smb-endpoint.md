# 本机 SMB 端点

`packages/smb` 是 Windows client 进程内的本机呈现层。Windows 系统 SMB client 通过 loopback TCP 连接它；端点把认证后的 share 连接绑定到调用方提供的 `storage.FileStorage`。SMB 只存在于 client 与同机操作系统之间，client 到远端 authority 的边界仍是平台中立的 storage／HTTP 契约。

端点支持 SMB 3.1.1 协商、认证、消息签名、share 连接、CREATE/CLOSE、regular-file READ／flags=0 WRITE／FLUSH、五类文件 QUERY_INFO、Basic／EOF SET_INFO、五类 volume QUERY_INFO、六类 QUERY_DIRECTORY 与会话清理。五种非 supersede disposition 建立稳定 FileId 和权威 share claim；名字修改和锁命令返回明确的不支持结果。该命令集合不构成完整 Windows network drive。

## 一、嵌入与发布

`smb.New` 接受 `Config{Authenticator, AuthorizeIdentity, Authorize, Limits, Logger}`。构造函数先验证两个授权接口、认证 provider 和全部资源上限，不取得 Windows credential、不启动 goroutine，也不打开 listener。`packages/smb/windows` 提供 SSPI `Authenticator`、读取进程身份的 `CurrentIdentity` 和只允许该确切身份的 `AllowIdentity`；非 Windows 构建保留同一 API，并在实际取得平台能力时返回不支持。

一个 `Server` 可以发布有限个 `Share`。每个 share 携带不区分大小写的 SMB 名字、业务授权 label `Volume`、实际存储 pin `BackendVolume storage.VolumeID`／`RootNodeID` 、可选 `VolumeLabel *string` 和一份调用方拥有的 `storage.FileStorage`。发布时验证这些配置及 `CheckFileStorage`，但不建立 FileSession，也不接管 backend 的关闭责任。名字为空、过长、含 SMB 分隔／保留字符、前后空白或与 `IPC$` 冲突时拒绝。VolumeLabel 的 nil 表示来源缺席，已知空值表示无 label；已知值必须为无 NUL 的有效 UTF-8，最多 32 个 UTF-16 code unit。Export 独立复制 label，后续宿主修改原值不改变已发布事实。

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

SMB2 compound frame 在一个有界 payload 中解析。每项保存自己的 header、body 和用于签名的完整 command bytes；related compound 只继承同一 frame 中前一项的 SessionId、TreeId，以及成功 CREATE 产生的 FileId。继承值在解码后的执行上下文中传递，不改写用于验签的 command bytes。all-ones FileId 只可使用这一 frame 的前驱结果，半个 all-ones 值拒绝。混用 related 与 unrelated 风格、把 `NEGOTIATE` 或 `SESSION_SETUP` 放入 compound、非法 offset／alignment、非法 credit 或重复 MessageId 都拒绝。response 逐项保留对应状态，related 前项失败时后项不执行受控效果。QUERY_INFO 与 QUERY_DIRECTORY 只有携带有效正常结果 body 的 BUFFER_OVERFLOW warning 才允许后续 related CLOSE；目录 NO_SUCH_FILE／NO_MORE_FILES 沿前驱失败路径，受抑制的 CLOSE 不释放原 ownership，仍需独立关闭或正常 teardown。

认证完成后，每个携带 SessionId 的请求都必须使用该 session 的 AES-CMAC signing key 验证，合法请求及能够归属该 signer 的错误 response 也由同一 key 签名。unsigned session request 的拒绝保持 unsigned；带 signed flag 但 MAC 错误的请求得到 signed access-denied。已建立 session 的 reauthentication 在解码 token 前先验签，畸形或被篡改的 exchange 不能得到 unsigned 旁路。sessionless ECHO 可以在 negotiate 后使用，并且不继承其它 session 的 signer。未知或已经退役的 session、replay/DFS flag 与失效身份均在访问 tree 或 backing 前失败。session key 只用于派生 signing key，临时 token 与 key buffer 在使用后清零；日志不写入 token、key、SID 或显示名。

## 三、Windows 身份与授权

`packages/smb/windows` 通过 SSPI `Negotiate` 接受 SPNEGO token。每次 authentication exchange 独立取得 inbound credential；`AcceptSecurityContext` 必须建立 integrity-capable、非 null session 的 security context。完成后从 context token 读取用户 SID 与 `TOKEN_STATISTICS.AuthenticationId`，把二者组成授权身份；account display name 只用于诊断，不参与相等比较或准入。

标准组合先用 `CurrentIdentity` 捕获宿主进程 token 的 SID 与 logon-session ID，再把结果交给 `AllowIdentity` 作为 `Config.AuthorizeIdentity`，只允许精确匹配。每次初始 authentication 和 reauthentication 都在安装 principal／signer 或替换 expiry 前调用它；明确拒绝产生 access denied，无法决定产生 I/O failure。anonymous、Guest、LocalSystem、LocalService 与 NetworkService 明确拒绝。同一个 SID 的另一次登录不是同一身份，不能接管 `PreviousSessionId`、复用 session 或访问它的 tree。

SMB 本机身份与远端业务身份是两层独立保护。`AuthorizeIdentity` 决定一个已验证的操作系统主体能否建立或刷新 SMB session；端点随后把该 `Principal` 放进 request context，并用独立的 `Config.Authorize` 对 trusted volume 与实际 FileSession 操作作准入。`TREE_CONNECT` 授权 `file.backend-identity`、`file.session-open` 与 `file.status`，后台续期授权 `file.renew`，显式 tree/session 清理授权 `file.session-close`。CREATE 按引用类型及实际打开效果授权 open、metadata 和修改操作；完整目录观察单独授权 `file.observe-directory-metadata`，root Stat 授权 `volume.stat`。显式 CLOSE 授权 `file.close`；远端 HTTP 对绑定引用的关闭状态查询仍独立授权。已接受的内部退休继续履行原清理责任。backing remote storage 继续使用调用方为远端 authority 配置的身份和凭据，SMB SID 不替代它。

authentication exchange 受 `HandshakeTimeout` 约束，完成后的 identity 另受 provider 返回的 security-context expiry 约束。未完成的 exchange 到期后自主关闭，不等待下一次 `SESSION_SETUP` 才回收；reauthentication 使用新的 generation，旧 timer 不能关闭新的 exchange。identity 到期后，普通命令收到 signed `STATUS_NETWORK_SESSION_EXPIRED`，session、signer 与 tree 保留以允许 signed SESSION_SETUP 重新认证；只有同一 SID 与登录会话的成功 reauthentication 才更新 expiry 并恢复工作，LOGOFF 始终可以清理该 session。

## 四、文件适配的组成

| 文件 | 责任 |
|---|---|
| `internal/wire/files.go`、`create_contexts.go`、`file_io.go`、`file_info.go`、`query_directory.go` | CREATE/CLOSE、内容／信息／目录命令的固定体、偏移、contexts、FileId 及响应结构 |
| `names.go`、`name_compare_*.go` | 可表示名字、保留名、UTF-16 code-unit 等价与排序 |
| `namespace.go` | 完整权威 sibling 观察、pinned root、revision/edge ancestry guards |
| `tree_capabilities.go`、`authority_session.go` | backend/session identity、完整能力预检、共享 FileSession 与续期 |
| `commands_file.go`、`windows_metadata.go`、`content_metadata.go` | CREATE 意图、访问／share／disposition、Windows metadata 与固定派生效果 |
| `commands_file_io.go`、`commands_file_info.go`、`commands_volume_info.go`、`commands_directory.go`、`file_io_gate.go` | 内容／文件／volume／目录命令、File／NodeReference 共用 FIFO、授权／身份复核与关闭排空 |
| `directory_snapshot.go`、`directory_pattern.go` | 完整有界捕获、紧凑保留表示、raw-order 分页、DOS matcher 和累计费用 |
| `mutation_owner.go`、`flush_owner.go`、`write_diagnostics.go` | immutable WRITE／Basic／EOF 条件动作与 FLUSH confirmation、同动作恢复、有界失败事实与宿主责任转移 |
| `file_handles.go`、`commands_close.go`、`authority_recovery.go`、`handle_diagnostics.go` | typed owner、准入／容量、恢复、关闭尝试与结算 |
| `session_cleanup.go`、`connection.go`、`server.go` | tree/session/export 退休、compound 执行与 signer 所有权 |

每个 authority session 保存不可变 `FileSessionIdentityResult{Backend, SessionEpoch}`。Backend 的 Volume、Authority 与 RootNodeID 全部必须和经验证 backend 相同，Volume／Root 还须匹配宿主 pin，SessionEpoch 必须等于其 Status.Epoch。Share.Volume 只用于业务授权。新的 tree 在公布成功前完成 AtomicFileOpener、NodeReferences、DirectoryMetadataObserver、StableReferenceIdentity、OpenMetadataAccess、OpenContentMetadata、FileActions、AllocationReporting 与 RecoverableReferenceClose 的完整链检查；wrapper 缺失能力时明确拒绝。身份与中立能力的定义见[文件句柄设计](../server/file-handles.md#一身份与会话)。

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

### 文件内容与引用准入

READ／WRITE／FLUSH 经原签名 frame 分派，related FileId 在 frame-local context 中解析，原字节不改。每次请求核对 exact typed FileId、tree／session／authority descriptor、原 File／NodeReference 类型及实际访问权，先授权再入队，取得 turn 后在 backing dispatch 前重新授权。READ 用 OpFileRead，所有 WRITE 用 OpFileWrite，非空派生效果另用 OpFileSetMetadata，条件观察用 OpFileObserveMetadata，FLUSH 用 OpFileSync；拒绝不能把先前 Unknown 改成 NotExecuted。

逐句柄 FIFO 与 running pointer 由 file_io_gate.go 管理。MaxHandleIORequests 默认 32，不能超过 MaxRequests；queued 项保留原 frame 费用，取得 turn 前不复制写内容。短锁按 authority installation、tree fileMu、session mu、handle ioMu 顺序取得，放锁后才授权／调用 backing。不同 FileId 并行；同一对象的最终内容顺序归 authority。

READ 一次 ReadAt 返回 Data／Attr，验证 NodeID、kind、size、allocation 与捕获 EOF；成功零字节及不足 MinimumCount 为 END_OF_FILE，正请求在 captured EOF 前非法零读为 I/O 错误。短读不补读拼接。WRITE flags=0 支持普通范围及 append-only 权限的 MutateAppend；-1 选 authority EOF，其它高位 offset 拒绝。WRITE_THROUGH-only INVALID_PARAMETER，UNBUFFERED 与配对 NOT_SUPPORTED，效果前失败。空 WRITE 用零效果显式 MutateWriteAt，无 ARCHIVE／时间／内容变化。

mutation owner 在 dispatch 前预留原 Data、Attr／Metadata、action／token／effect、完整 response 与诊断预算，WRITE／Basic／EOF 共用现有 WriteOwners／RetainedWriteBytes／WriteFailures 与 acknowledgment 统计。MaxWriteOwners 默认 128，MaxRetainedWriteBytes 默认 32 MiB，global 与每 export 都计费。非空 WRITE 用 smb.windows 的八字节 sealed descriptor，最终 token CAS 与内容／ARCHIVE／时间同事务；READONLY、非法格式或 CAS 冲突不放行。Unknown 结束 active turn，但保留原 immutable owner，后续只原 action Query／typed replay；Completed receipt 不提供 typed Attr／barrier，bound NotExecuted 条件冲突才允许最多两次新条件尝试。

CREATE 的 initial metadata 普通授权与固定 descriptor enrollment 分别核对：普通 OpFileSetMetadata 的 ContentMetadataEffects 为空，限制于派生效果的许可不允许创建或设置 HIDDEN／SYSTEM 等任意 initial attributes。直接 SMB 与远端 HTTP 保留同一区分，不能将两项按 operation 名称合并。

FLUSH owner 无 Data，用原 File.Sync 作有限 confirmation；失败与丢响应不建立 HTTP action pending，不重写任何内容。CLOSE 先 fence 新 I/O、移除 queued、排空 running，再有界结算 mutation／flush owner；Unknown 可继续 exact close 取得 no-future-publication 与 settlement 证明。post-query Attr 在 drain 后捕获。

### 文件信息与条件修改

QUERY_INFO 的 `InfoType=File` 显式支持 Basic `0x04`（40 bytes）、Standard `0x05`（24）、Internal `0x06`（8）、NetworkOpen `0x22`（56）和 Id `0x3B`（24）。Basic／NetworkOpen 要求 FILE_READ_ATTRIBUTES，各自只用本次原引用 Stat 的完整 Attr；Standard／Internal／Id 使用 `ReferenceStateAccess.State(ctx, StateScalars)` 的本次 scalar snapshot 确认 exact live 引用，不能用退休后仍可读的 ReferenceNodeID 代替生命期。Standard 同次返回 EOF／allocation／kind／detached／pending，具名 link count 为1、detached 为0。每类以实际语义 Operation 在入队前和 turn 后授权，固定输出不足不发送半个结构。

FILE_READ_ATTRIBUTES 与公开 ReadMetadata 保持绑定；其余三类不增加 metadata 读权。File 与 NodeReference 都在同一 FIFO 下查询，所有返回 NodeID／kind 与捕获 identity 相符。NetworkOpen／Standard 的 allocation 先验证中立已知字节事实，再从既有 AllocationReporting.AllocationUnitBytes 读取原 authority session 的可信不变单位并核对整数倍；HTTP 的单位或原失败已在授权 enrollment 中取得。单位不可用或事实不自洽使相应类失败，实际单位错误保留原分类，不中止 tree 其它工作。64-bitserial 从 durableVolumeID 稳定投影，不同发布 volume 的碰撞在发布前拒绝；128-bit 对象 ID 无损编码不可复用 NodeID，与每次打开的 SMBFileId 独立。

Basic SET_INFO 要求 FILE_WRITE_ATTRIBUTES，严格40bytes；允许 READONLY／HIDDEN／SYSTEM／ARCHIVE 及 Birth／Access／Mod 时间，零保持原值，NORMAL 清空可设置位；实际 volume 根的 HIDDEN／SYSTEM 保留，通过有效 mask 处理其它可设置位，subtree share 根不冒充实际 volume 根。DIRECTORY 等报告位由 kind 派生，非零 ChangeTime 与-1／-2 时间控制明确拒绝；其它时间在 mutation 前完成 FILETIME 范围检查。通过既有 ReferenceMetadataAccess 的 Namespace purpose 请求 smb.windows、MaxPayloadBytes=8 取得条件，调用 `MutateAttributes` 一次提交 AttrChange 和 namespace 更新。WRITE_ATTRIBUTES-only 每次观察使用 WriteMetadata 和当前 namespace／bound 授权，不取得公开 Stat／StateFull／ReadMetadata，也不增加 namespace enrollment。条件重试只刷新 smb.windows 的 CAS token，以及实际 volume 根必须保留的 HIDDEN／SYSTEM 位；原时间、数据和其余请求属性不变。只在原 action 绑定 NotExecuted 时允许有限新尝试。

EOF SET_INFO 要求普通 File 的 FILE_WRITE_DATA，严格8bytes 且长度非负；APPEND-only／目录／metadata-only 明确拒绝。`MutateTruncate` 在最终 authority 事务中核对 READONLY 条件并同时提交长度、ARCHIVE、时间与日志；既有 scope／Use／配额继续参与最终许可。属性与 EOF 均使用 mutation_owner.go 保存完整不可变 command，Unknown 只核对／重投原 action，不能 remint。

QUERY_INFO／SET_INFO 复用原签名 frame、typedFileId、response 预算与 CLOSE 排空。支持的五个文件 QUERY 类按协议忽略 Flags、AdditionalInformation 和 InputBufferLength／Offset，含不能描述有效 buffer 的输入；SET 非 securityAdditionalInformation 必须为零；SET payload 不得越过当前 compoundcommand。完整 response、条件 metadata／Attr 与诊断在 backing 前计费；SET 的 Attr 返回预算统一由原 mutation owner执行并用于原动作恢复。security／quota、allocation、disposition、rename／link、EA／stream、compression／reparse 与 position／mode 类保持明确未支持；未支持 class 返回 NOT_SUPPORTED；畸形 length／flag 按各自 NTSTATUS 拒绝，backing 无部分效果。

### Volume 信息

`InfoType=Filesystem` 支持 Volume `0x01`、Size `0x03`、Device `0x04`、Attribute `0x05` 与 FullSize `0x07`。所有类复用原 `admitInformation`，句柄 rights 为零；先在入队前及取得 turn 后授权 `file.state` 与实际查询，StateScalars 确认 exact live File／NodeReference 的对象、kind 和 scope。Size／FullSize 使用 `volume.space`，其它三类使用 `file.backend-identity`；不授予 Stat／StateFull／ReadMetadata 或字节读权。编码前再次核对当前授权和 context。

Volume 使用原 session 的可选持久创建事实、Export 复制的可信 label，以及 `uint32(volumeSerial)`；32-bit 字段不承担唯一性，64-bit FileId reservation 保留。Device 仅设置 FILE_DEVICE_DISK=7 与 REMOTE_DEVICE=0x10。Attribute 的 filesystem name 为 `remote-fs`，flags 仅为 CASE_PRESERVED_NAMES=2 与 UNICODE_ON_DISK=4，component 上限为既有 255 code unit。

Size／FullSize 每次调用实际 backend 的 Space，使用原 AllocationReporting 的不可变非零单位。逻辑 bytes-per-sector 等于该单位，sectors-per-unit 为 1；单位须可由 uint32 表示。Space 先验证 Coherent 与 Used<=Total，Total／Avail 须可整除；FullSize 还验证 Total−Used。Size 分别报告 Total 与 caller Avail；FullSize 另报告 actual free，不由 caller Avail 推测 Used。单位、Space 缺席或来源错误、超额与不可表达只失败相应容量类；缺席／不支持容量源也映射 I/O 错误，权限拒绝仍保留。tree 和原 FileId 不被容量 fence，读取、缩短与关闭按原契约继续。

Volume 的最小输出请求为 24，但 serialized prefix 只有 18 字节；已知空 label 的完整 payload 可为 18 字节，不插入六字节 padding。Attribute 最小请求和 prefix 均为 12。Size／Device／FullSize 分别要求完整 24／8／32 字节，短请求在 authority 工作前失败。变长 label/name 不足时返回 BUFFER_OVERFLOW 与正常 QUERY_INFO body；声明完整 UTF-16 字节长度，复制可用容量的原始字节 prefix，包括奇数字节。最大 Volume payload 为 82 字节，完整对齐 response frame 为 160 字节，预留按真实最大类结果计算，不按任意声明 buffer 扩大。

创建时间或 label 缺席只使 Volume 类失败，容量错误只使 Size／FullSize 失败。filesystem 类表中已定义未实现的类返回 NOT_SUPPORTED，未知类返回 INVALID_INFO_CLASS。该查询路径不建立 action／mutation owner、不产生修改效果，也不从旧路径或旧 Space 合成结果。

### 目录枚举

QUERY_DIRECTORY 支持 Names `0x0C`（12-byte header）、Directory `0x01`（64）、Full `0x02`（68）、Both `0x03`（94）、IdFull `0x26`（80）和 IdBoth `0x25`（104）。全部请求要求活目录 FileId、FILE_LIST_DIRECTORY、当前 `file.read-dir-node` 与 `file.state` 授权。每页和 EOF 都调用原 NodeReference.State(StateScalars)，通过 native SQL 与 objectstore IOHealth 核对当前对象、目录 kind、scope 与生命期；返回前再次检查策略和本地退休状态。新的健康观察不替换冻结快照的 revision。

首次 capture 或 restart 在 `file.scope` 授权后从原 NodeReference.Scope(ctx) 取得并验证实际 Scope，传给 `DirectoryTarget{NodeID, Scope}`。原 `ReadDirNodeBounded`／ListResult 提供完整 capture、ParentID 与合法非空 opaque revision；出错或超限时 collector 及 observation 整体失败。端点对所有 raw name、ID、kind 和 Windows 大小写歧义先验证，再进行过滤；不匹配的坏名也使全捕获失败。条目保持 ListResult 的 raw-name 顺序，辅助 Windows comparer 排序只检测冲突。

每个目录 FileId 保留一份与 class 独立的紧凑 snapshot、expression、next ordinal 和独立 settled-empty 状态。entry 保存六类 codec 的共同 wire scalar、UTF-16 名字、ID／kind、matched bit 和固定 rich-field 状态；完整转换后不保留 raw name、无关 metadata、Attr 或错误链。Names 不要求 FILE_READ_ATTRIBUTES，不能因其不需要的时间／allocation 不可表达而失败；rich class 每次在过滤前验证全部 entry 所需字段，class 切换不重取。目录 Size 未指定，EOF=0；Full／Both 系列的 symlink EaSize 为 IO_REPARSE_TAG_SYMLINK `0xA000000C`，非 reparse 为零，short-name 字段为空。

普通 continuation 忽略新的 expression，保持原 matched selection；RESTART_SCANS／REOPEN 丢弃旧 entries，完整重取，均采用非空 replacement expression，缺席时保持原 expression。失败不恢复旧 snapshot。matcher 对 UTF-16 units 使用既有 Windows 大小写等价，支持 `*`、`?`、DOS_STAR、DOS_QM 和 DOS_DOT；`?` 消耗一个 unit，`*`／`*.*` 全匹配。expression 上限为 255 units，迭代有界 frontier 与全过滤累计 `MaxDirectoryMatchWork` 防止指数回溯，超过工作量在暴露任何条目前失败。

只输出真实 children；`.`／`..` 是合法 expression，没有合成自身或父项。FileIndex 为零，输入 index 与 INDEX_SPECIFIED 忽略。首项固定 header 装不下为 INFO_LENGTH_MISMATCH；header 可放但名字不足时，正常 body 返回 BUFFER_OVERFLOW、完整 FileNameLength、原始 byte prefix 和 NextEntryOffset=0，保留该项供较大 continuation 重试。后续 entry 必须完整，条目起点八字节对齐、inter-entry padding 为零，最后 offset 零且无必需 final padding。RETURN_SINGLE_ENTRY 最多一条；完整 response 构造与最终授权通过后才推进完整项。

fresh search 无匹配为 NO_SUCH_FILE，随后由独立 settled 状态返回 NO_MORE_FILES；partial warning 不结算 EOF。健康、授权或 capture 失败不转成这两个结果。HTTP 单独授权实际 capture／State RPC，后来的 backend list-policy 撤销不收回已经交给 SMB 的完整 capture；宿主当前 SMB Authorizer 控制每个新页面，不增加 recapture 或只检查授权的 RPC。

## 五、容量与 FileId owner

Limits 分别限制 export、connection、session、tree、pending request、compound/context、frame/I/O/token、完整目录观察、handle、未结清 owner 与诊断字节。所有值显式选择，零值不表示无界。MaxIOBytes 同时成为协商公布的 transact/read/write 上限；协商后完整 request 受 I/O 加固定 envelope 约束，control command 另受 68 KiB 上限且只消耗一个 credit，READ/WRITE 的 credit charge 为 max(1,ceil(requested Length/65536))，不把 header／compound padding 算作 payload；READ 在 backing 前预留 requested Length 的完整响应。FileSessionOptions 另限制原生引用、owner、range、等待与动作历史。

目录状态使用单一 `MaxRetainedDirectoryBytes`，默认 64 MiB；Server 与 Export 在 handleMu 下记录同一费用，Export 归属没有独立 quota。`Status.RetainedDirectoryBytes` 包含 snapshot、cursor 和未完成 capture reservation。既有 `MaxDirectoryBytes` 默认 8 MiB，限制 collector；`MaxDirectoryMatchWork` 默认 16 Mi 次 state visits，限制完整过滤的累计工作。保留 expression 的 cursor 固定费用为 1024 字节，每个 entry 的最终费用为 `256+2×UTF16NameUnits`，包含每页临时 wire view 的 scalar 费用。

capture 的 F=64 KiB 包含 cursor／matcher 固定 allowance，L 根据可用累计余额和既有 collector cap 缩小。backing 前预留 `F−1024+4L` staging，entry 再按 `2×ObservedEntryBytes+1536` 同时扣累计余额与 collector 额度；此收费覆盖已有 HTTP 成功 body 缓冲与转换期间的多份表示。callback 取得的累计 tally 独立于 ListResult.UsedBytes，locally rejected final reservation 也必须归还。完整 capture、全名验证和过滤后，临时对象不再可达才释放 staging／多余费用，最终只保留 cursor 与紧凑 entry。详细计费式由[目录枚举决定](../../../.agents/notes/implemented/feature/2026-09-28-smb-directory-enumeration.md)拥有；该上限不声称测量整个 Go heap，原 transport／response 上限仍成立。cursor 每页可扫描／复制剩余匹配项，连续 single-entry 页总工作量为 O(N²)；8 MiB collector 与至少 2048 字节 entry capture charge 使单次 capture 少于 4096 项。

restart 先释放旧 entries；失败不留可服务 snapshot，仍为需要保留的 expression 计费。CLOSE fence／drain 后，在 pending Basic reconcile 和 native close 之前释放 read-only cache；drain 超时保留原 state／charge，native close 未知不使已经排空的 cache 继续保留。tree／session／disconnect／shutdown 和最终 owner removal 使用同一幂等释放路径，资源饱和仍保留清理与状态能力。

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

### 内容失败事实与宿主责任

write_diagnostics.go 在 dispatch 前预留 WRITE／FLUSH 失败事实。Status 的 WriteOwners、UnknownWrites、PendingFlushes、RetainedWriteBytes 与 copied WriteFailures 区分内容持有、已终结资源及原 execution unknown；ResponseSent 不证明应用消费。SMB 原 payload 只在 exact reference／完整父 no-future-publication 与包含 replica 的全链 settlement 正面证明后释放。HTTP 下游已释放而 action gate 排空超时时，仍返回 Released=true 的 Pending CloseSettlementError，保留本层 copy 与原 SemanticErr；SMB 继续保留 owner／charge，后续同关闭 owner 续作排空。失败事实不因 FileId 删除或断线消失。

WriteFailure 保存 typed WriteOwnerID 的随机 incarnation／单调 generation、export／FileId generation、NodeID、action、operation、byte count／digest、execution、FLUSH durability、ResponseFinal、Terminal、PayloadReleased、ReferenceReleased、ChainSettled 与固定 error category，不保存 Data。Server.AcknowledgeWriteFailures 全批验证 final terminal settled IDs 后才移除记录；宿主必须先保存并接管继续报告事实，ack 不改变 Unknown、不确认执行、不重发内容。未确认资源或尚未交出失败事实时 Shutdown／Unpublish 返回错误并保留 stopping；责任完成后后续停止可成功。

## 七、命令与需求边界

可成功的命令为 NEGOTIATE、SESSION_SETUP、ECHO、TREE_CONNECT、TREE_DISCONNECT、LOGOFF、CREATE、CLOSE，以及 regular-file READ、flags=0 WRITE、FLUSH、五种文件 QUERY_INFO、Basic／EOF SET_INFO、五种 volume QUERY_INFO 与六种 QUERY_DIRECTORY 信息类。未支持的信息类、LOCK、IOCTL、CHANGE_NOTIFY 和 OPLOCK_BREAK 不执行 backing 操作并返回 STATUS_NOT_SUPPORTED。CANCEL 的 async 语义未建立，CANCEL、无法识别或畸形 command fail closed，终止连接而不执行受控效果。

对象不存在、撞名、类型不符只有在权威条件已确定时映射相应文件错误；typed share 冲突映射 STATUS_SHARING_VIOLATION，不能被一般 EAGAIN 资源错误遮蔽。容量耗尽映射 STATUS_INSUFFICIENT_RESOURCES。无法证明的引用身份、allocation、时间、动作或 settlement 返回 I/O 错误，不能报告成功、缺席或旧值。

文件 I/O 已确认的 EDQUOT 映射 STATUS_QUOTA_EXCEEDED，与底层 ENOSPC 区分；含 EIO 的未知结果优先保持 STATUS_IO_DEVICE_ERROR，不能凭 joined quota errno 宣称明确未执行。

这套结构对应 R-FS-5 至 R-FS-9 的打开／引用部分，R-CC-14／R-WIN-6 的共享准入，R-INT-1 至 R-INT-3 的嵌入和资源，以及 R-ERR-1、R-ERR-2、R-WIN-9 的真实错误与安全准入。范围锁、持久关闭删除、名字修改、通知／缓存、WNet 和完整 Windows 资格仍由独立能力实现。取舍与依赖见[有界 CREATE/CLOSE](../../../.agents/notes/implemented/feature/2026-09-28-smb-bounded-create-close.md)、[文件内容访问](../../../.agents/notes/implemented/feature/2026-09-28-smb-file-data-io.md)、[文件信息](../../../.agents/notes/implemented/feature/2026-09-28-smb-file-information.md)、[volume 信息](../../../.agents/notes/implemented/feature/2026-09-28-smb-volume-information.md)及[目录枚举](../../../.agents/notes/implemented/feature/2026-09-28-smb-directory-enumeration.md)。
