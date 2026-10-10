# Agent Note: 有界 SMB CREATE 与 CLOSE

Status: implemented

## 问题

SMB 打开必须把 Windows 的名字、访问和共享意图转成同一权威结果。先查路径再打开会把名字替换后的对象当成原目标；权威效果之后才检查句柄容量、引用身份或响应可编码性，会留下不能交付且无人清理的引用。共享检查也必须覆盖同一 volume 的其它入口，不能只依赖端点自己的句柄表。

关闭存在两个独立事实：引用是否释放，以及完整 adapter 链是否结算。响应丢失、连接断开或 mutation barrier 未确认时，单个 NTSTATUS 不能说明清理责任已经结束。端点须持有稳定 owner，并在中立关闭历史的有效期内核对或续作同一尝试。本决定交付 [R-FS-5 至 R-FS-9、R-CC-14、R-INT-3、R-WIN-2、R-WIN-4、R-WIN-6、R-WIN-9 与 R-WIN-10](../../../../docs/spec/requirements.md) 的 CREATE/CLOSE 部分，接续[整体 Windows 提案](../../proposed/feature/2026-09-16-windows-network-drive-support.md)。

## 决定

### 交付边界与分工

7.3b 接入已签名 CREATE、CLOSE 和同一 compound 内 CREATE→CLOSE 的 related FileId。支持 FILE_OPEN、FILE_CREATE、FILE_OPEN_IF、FILE_OVERWRITE、FILE_OVERWRITE_IF 五种 disposition。普通文件有字节权限时取得 `storage.File`；只有 metadata、DELETE 或 execute 权限时取得 `storage.NodeReference`。目录也使用 NodeReference，空名字从固定 root NodeID 打开。一份 SMB FileId 恰好拥有一份 typed reference，绑定 tree、session descriptor、NodeID、授予权限、Use/share claim 与关闭 owner。

SMB 负责 Windows 名字比较、标志解释、wire 编码、本机 FileId、操作授权与清理调度；storage 负责最终 guarded selection、创建／清空、metadata 条件、跨入口 Use/share 排序和引用身份。端点消费[可恢复引用关闭](../architecture/2026-09-23-recoverable-close-ownership.md)的 CloseAttempt、CloseOwnerStatus 与释放结果；SQLite 最终释放、DropUseExact、关闭动作历史和 HTTP 关闭回执仍由该决定拥有。

| 内容 | 范围与延期代价 | 保留的结构 |
|---|---|---|
| CREATE/CLOSE、身份、真实错误、容量与清理所有权 | 本次保证；延期会重写效果与退休路径 | 引用和 owner 在效果前登记，未知结果固定原动作 |
| READ/WRITE/FLUSH、信息与目录枚举 | 后续命令使用已有对象能力，增加命令与结果预算 | File 与 NodeReference 不互相冒充，访问授予与 Use 分开保存 |
| delete-on-close、SUPERSEDE、rename/disposition | 增加持久义务和 guarded 名字／原位重置动作；请求明确拒绝 | FileId 不以路径为身份，不把关闭等同于删除动作 |
| 范围锁、CANCEL、通知／leases／缓存 | 增加中立 owner、async 请求与变更源；请求不取得成功路径 | 不授予 oplock，不把 SMB replay 当成中立动作重投 |
| WNet 与原生 Windows 资格 | 需要系统 redirector、445、登录会话与缓存证据 | 本机协议验证不宣称 network-drive 发布资格 |

### 身份与能力在效果前验证

宿主提供 `Share.Volume` 作为业务授权 label，并独立提供 `BackendVolume storage.VolumeID` 与 `RootNodeID` 作为存储 pin。`BackendIdentityResult{Volume, Authority, RootNodeID}` 来自实际 backend；`FileSessionIdentityResult{Backend, SessionEpoch}` 来自其新 session。tree 发布前必须核对 backend descriptor、host pins、session descriptor 和 `Status.Epoch`，同时检查完整名字观察、原子打开、节点引用、稳定引用身份、独立 metadata 权限、动作历史、分配量和可恢复关闭能力。任何层无法证明时拒绝 tree，未宣布的 session 继续由 owner 清理。

SQLite 的 Volume 由已验证 DatabaseID 与 root NodeID 组成；共享 fileDomain 使用密码学随机 Authority，同一域的 sessions 共享它，新域更换 incarnation。SessionEpoch 保留其既有语义。身份投影不增加持久 schema，wrapper 与 HTTP 不用授权 label、路径或每会话 nonce 模拟 backend 身份。`StableReferenceIdentity` 预检承诺两种返回引用都提供不可变 ReferenceNodeID，包括 error 与引用同时返回的情况。

`InlineCloseSettlement` 用于 HTTP handler 的 CloseRecovery 宣告：它保证下游报告 Released 时已在完整链内结算。SQLite 提供保证，objectstore、locked 与 limited 检查下层；HTTP client 与 replica 明确不提供。该 gate 拒绝嵌套 HTTP server 下隐藏的 downstream pending barrier；普通 native→HTTP→replica→SMB 链通过中立 settlement marker 继续工作，SMB 不要求 inline settlement。

### 源码职责

| 路径 | 职责 |
|---|---|
| `packages/smb/internal/wire/files.go`、`create_contexts.go` | 有界 CREATE/CLOSE 解析、FileId 与 context 结构、响应编码 |
| `packages/smb/names.go`、`namespace.go`、`name_compare_*.go` | Windows 名字可表示性、UTF-16 比较、完整目录观察与 ancestry guards |
| `packages/smb/commands_file.go`、`windows_metadata.go` | CREATE 意图、访问／disposition 映射、授权、原子结果验证与 Windows 属性／时间投影 |
| `packages/smb/file_handles.go`、`commands_close.go`、`handle_diagnostics.go` | typed FileId owner、容量、固定关闭尝试、释放／settlement 与 CLOSE 响应 |
| `packages/smb/connection.go`、`commands_session.go` | 保留验签字节的命令分派和 frame 内 related FileId |
| `packages/smb/tree_capabilities.go`、`authority_session.go`、`session_cleanup.go`、`server.go`、`config.go` | 实际 backend/session 绑定、tree admission、退休、上限与状态 |
| `packages/storage/file_identity.go`、`close_settlement.go` 与 native／wrapper／HTTP 的投影 | 中立身份、独立 metadata 授予和完整链结算事实；不解释 Windows 标志 |

### 有界 owner 与权威动作

端点在可能创建或清空对象之前预留 FileId、opening owner、响应空间及清理责任。server 全局 owner registry 按 min(MaxHandles, MaxUnresolvedOwners, MaxDiagnosticBytes/256) 限制 opening、live、cleanup-only 与 barrier-only；每个可能未结清的 owner 在效果前收费一个固定 256 字节诊断槽。中立层自己预留的 close receipt 不被端点空闲名额代替；终态历史仍占原容量时，新打开不能绕开它。

FileId 由不可复用 session 实例与单调序号组成，两个 64-bit 部分均避开 all-ones 占位。查找同时验证 session、tree 和原 identity descriptor。owner 保存 typed reference、固定 NodeID、权限、Use、原 open action、未决恢复输入及当前 close attempt；它不能改绑后来占据原路径的对象。

打开在效果前保存 action ID、原方法和不可变选择／options。收到 error 与非 nil 引用时立即保留清理责任；没有引用但动作未知时保留同一输入的恢复入口。QueryFileAction 仅报告动作状态，不补造引用和 Attr；按同 ID、同方法、同输入重投才取得 typed result。只有绑定原动作的 NotExecuted 证明允许丢弃未执行意图或重新观察并生成下一动作。Unknown、Retired、超时与取消都不能证明零效果。已接受的恢复入口保存原授权后的不可变意图，内部续查与 replay 不重新执行 endpoint 业务授权；HTTP 的公共 RPC 仍分别执行现有授权。

### 完整名字观察与 guarded CREATE

Windows 名字比较按 UTF-16 code unit、不作 normalization。Windows 使用显式长度的 CompareStringOrdinal；其它平台使用规范的 973 项 BMP 大写映射，未列项保持原值，代理项不映射。比较表的来源、摘要与规范算法保存在源码中；Go EqualFold/ToUpper 不能替代这项关系。

每一级先核对固定 root，再取得完整、有界、权威 directory metadata。所有 sibling 都检查可表示性与 case 唯一性；一个非法名字或两个等价名字使整次观察失败，不能在过滤或精确命中后忽略。无关的合法符号链接不使目录失败；选中的叶项或中间路径若是符号链接则拒绝。结果携带每级 directory revision、精确 raw edge 与父节点身份；最终 OpenAt/OpenChildRef 同次重验 guards、目标及 metadata 条件。

空名字只指 share root，每次 Stat 必须匹配已核对的 pin，随后以同一 session 的 OpenNodeRef 和 SameNode 条件打开现存目录。OPEN/OPEN_IF 不创建根；CREATE 报已存在，overwrite、非目录与截断在效果前拒绝。普通 OPEN 保持内容；CREATE 要求缺席；OPEN_IF 保持已有对象或创建；OVERWRITE 只清空已有普通文件；OVERWRITE_IF 清空已有普通文件或创建。清空、请求属性集合加 ARCHIVE 和 metadata predicate 属于同一次 guarded authority 动作；若已有 HIDDEN/SYSTEM 位，overwrite 请求必须包含对应位，否则在清空前拒绝。READONLY 只拒绝 DataFile 的字节写／append 打开；对已有只读文件的 DELETE-only FILE_OPEN 可以取得 NodeReference，打开不执行删除。

CREATE 结果使用原子动作返回的 Attr/Outcome，核对引用的不可变 NodeID、disposition 分支、已知 allocation 的精确字节值、时间和 Windows 属性可编码性；CREATE 不要求 4096 字节粒度，volume geometry 属于信息能力。Created 结果必须有已知 BirthTime/ChangeTime，Reset 必须有已知 ChangeTime；既有对象未提供的可选时间以协议 unknown 零值表达。不以随后 Stat 重建原打开结果。公布 FileId 前先在本地锁外取得新的 raw Status，核对固定 epoch 与 liveness；再在 authority 安装锁、tree admission 锁和所属 session 锁内核对 context、tree/authority stopping、session retirement、已确认本地 deadline 与预留槽，原子进入 Live。任一失败保留已经取得引用的清理责任。响应编码或发送失败不回滚已完成效果，owner 转为 cleanup-only。前驱成功的 related FileId 仅在同一已签名 frame 内传递，后继仍核对 session/tree；原始签名字节保持不变。

### 访问、共享与 context

Generic rights 按规范完整展开成具体权限，未实现权限和 MAXIMUM_ALLOWED 明确拒绝。GENERIC_ALL 的 FILE_ALL_ACCESS（0x001f01ff）包含未交付的 DELETE_CHILD、WRITE_DAC 和 WRITE_OWNER，因而返回 STATUS_NOT_SUPPORTED，不静默删掉这些权限后授予子集。Use 是 share 冲突声明，metadata 与字节方法是实际授予，两者不能互相推导。

| Windows right | Use/share 分类 | 返回引用上的实际权限 |
|---|---|---|
| READ_DATA；目录 LIST_DIRECTORY | ReadData；目录 ReadEntries | 文件字节读；目录没有字节方法 |
| EXECUTE；目录 TRAVERSE | ReadData；目录 ReadEntries | 无字节读，不推导枚举权限 |
| WRITE_DATA/APPEND_DATA；目录 ADD_FILE/ADD_SUBDIRECTORY | WriteData | 只有普通 File 获得字节写 |
| DELETE | DeleteName | 打开不删除，名字命令仍另行授权 |
| READ_ATTRIBUTES/READ_EA | 不增加 Use | ReadMetadata |
| WRITE_ATTRIBUTES/WRITE_EA | 不增加 Use | WriteMetadata |
| SYNCHRONIZE／支持的控制权限 | 不增加 Use | 不增加数据或 metadata 方法 |

只有 data／execute／write／append／DELETE 权限让 Windows 打开参与 sharing。Use 非零时，缺少 SHARE_READ 设置 Deny.ReadData|ReadEntries，缺少 SHARE_WRITE 设置 Deny.WriteData，缺少 SHARE_DELETE 设置 Deny.DeleteName；Use=0 的 metadata/control-only 打开忽略 ShareAccess，Deny=0。因而任一 Windows 打开不参与时都不形成双向 sharing conflict；参与的 claim 在同一 authority NodeID 上双向比较，冲突在创建／清空与保留引用前失败。OpenAt 的 Read/Write 只来自实际字节权限，MetadataAccess 显式独立。execute+write 因此声明 ReadData|WriteData，却只授予字节写；attribute-only、DELETE-only、execute-only 使用 NodeReference。清空内容必须有字节写权。

接受目录／非目录选项和互斥的同步 alert/nonalert 选项；同步选项需要 SYNCHRONIZE。SUPERSEDE、delete-on-close、reparse、ADS 及其它未实现效果在 backend 前拒绝。CREATE/CLOSE 的协议 reserved 字段按标准忽略，不把它们作为新的功能开关。

well-formed DHnQ、DH2Q、RqLs、AlSi 和协议 reserved GUID context 只解析并忽略：返回 oplock NONE、零 response contexts，不建立 durable/persistent/lease 状态。重复、混用互斥 durable 请求或畸形 recognized context 是 invalid parameter。well-formed reconnect、MxAc 与 QFid 明确不支持；ExtA、SecD、TWrp、app-instance、virtual-disk 和未知 context 也在效果前拒绝。这样 optional request 不形成隐含授予，而会改变打开效果的 context 不能被悄悄降级。

### 关闭与退休

外部 CLOSE 先核对 FileId owner、session/tree、身份与当前 OpFileClose 授权；拒绝不改变引用和 claim。准入后 owner 封住新工作，排空已接纳调用；随后绑定引用的 status/query/attempt replay 属于本次已接受的窄清理授权，不在关闭中途重新调用 Config.Authorize。HTTP 公共 RPC 的独立授权继续成立。没有本地当前尝试时，读取绑定确切引用的 CloseOwnerStatus：有 Current 即接管其原 action/generation；只有 Current=nil、Ready=true 且尚未释放，才按 CurrentEpoch/NextGeneration 保存新 CloseAttempt。退休内部清理发起的动作使用相同规则，普通 session Status 不能代替清理 cursor。没有本地 attempt 但 cursor 已报告 Released 时，用确切引用的隐式 CloseWithResult 核对完整结算，不凭释放事实生成新 action；未执行候选仍须接管内部 Current。

| 中立事实 | owner 后续动作 |
|---|---|
| Released=false、Determined=false | 保留原尝试，只按同 ID／generation 查询或重投 |
| Released=false、Determined=true | 保留本次错误和未释放责任；后续按 Ready 和新 generation 开始下一尝试 |
| 效果前 CloseActionNotExecutedError{CurrentEpoch} | 原 generation 可换当前 epoch 的 ID；普通 ESTALE、EINVAL、取消均不构成证明 |
| Released=true、CloseSettlementError | 移除可访问状态，保留 BarrierOnly、原 attempt 与原语义错误，续作结算 |
| Released=true、无 settlement marker | 引用与完整链结算已确认，回收本机 owner；原语义错误仍如实报告 |

Pending/Unknown marker 中 SemanticErr 与暂时 Cause 分开；同一尝试结算成功只移除 marker。CloseOwnerStatus.Released 或动作 Completed 不替代 typed close result 和 settlement。旧 history 过期、authority 换代或证据不足时报告 I/O 未知，不能按旧路径或新 session 接管原引用。

TREE_DISCONNECT、LOGOFF、断线与 stop 先 fence tree，等已接纳工作结束，再排空该 tree 自己的 FileId，最后减少共享 authority ref；最后一个 tree 才关闭 FileSession。内部退休推进已接受的清理责任，不重新请求业务授权；新的外部关闭仍授权。失败继续持有 owner、export 与容量。Unpublish 对仍有 live tree 或在途 connect/request 的 export 保持 busy；静止后的清理遵守同一责任结算。

状态分别报告 live、opening、cleanup-only 与 barrier-only handles，按条数和字节预留诊断。Server.HandleOwners() 返回 opaque FileId/session/tree/NodeID、open action、close attempt、owner 状态与 LastStatus uint32；没有 SID、密钥、原始路径、内容或原始 backend 错误。状态不因协议表项退役而把未结清责任算作回收。

## 备选方案

**把文件 I/O、枚举、删除、名字修改与锁一起交付。** 这些动作共享 FileId，却分别引入内容 revision、目录捕获、持久义务和范围排序；放在一个 PR 无法聚焦审查每次权威效果的失败边界。typed owner 保留它们需要的身份和清理形状。

**把中立最终关闭和 SMB 适配放在同一 PR。** SQLite claim 释放、关闭 action/receipt 和 HTTP 清理预算由所有入口共同依赖，且可独立验证。先交付中立关闭，再由本决定接入 SMB；本次只补身份、独立 metadata 与 settlement 的事实投影。

**只用路径 OpenFile 和本地 share 表。** 观察与打开之间能换对象，另一个 SMB session、Linux 或 SDK 也能在本地表外绕过 share 冲突；它不能满足最终 guarded 选择和跨入口排序。

**把 Released 或非 nil 错误当成 owner 已结束。** 已释放可能仍欠 barrier，非 nil 错误也可能是释放后的语义错误；二者均不能独立回答完整链结算。中立 marker 保存必要事实，稳定 attempt 固定恢复对象。

**让 HTTP server 接受下游 barrier 链并扩展关闭协议。** 这需要在 server 回执与 registry 清理中继续托管另一层 settlement。完整链 InlineCloseSettlement 检查使不支持的嵌套形态在 SMB 打开前被拒绝，保留现有关闭机制的审查范围。

## 后果

CREATE/CLOSE 有可独立审查的签名、名字选择、访问、共享、身份和清理路径；容量拒绝发生在权威效果前，响应失败仍有 owner。无字节权的引用没有字节方法，metadata 权限不随字节权限扩大。SMB 不需要理解 native 最终释放细节。

每级完整目录观察使深路径和大目录冷打开昂贵；MaxDirectoryBytes 耗尽明确失败。优化只能采用 revision 耦合的权威索引或可证明失效的观察缓存。action history 有限，窗口之外的未知动作可能无法在原 Windows 调用中结清；有界诊断不能冒充应用成功。不能提供身份、allocation、稳定引用或可恢复关闭的第三方链拒绝 tree；HTTP server 下尚有异步 settlement 的组合不宣告 CloseRecovery。

本决定不交付文件数据、目录浏览、名字修改、范围锁、通知、WNet 或原生发布资格。它们由[文件 I/O 与信息](../../proposed/feature/2026-09-28-smb-file-data-information.md)、[目录枚举](../../proposed/feature/2026-09-28-smb-directory-enumeration.md)、[关闭删除义务](../../proposed/feature/2026-09-28-smb-close-delete-obligation.md)及整体 Windows 提案中的独立工作承担。实际结构见[SMB 端点设计](../../../../docs/design/client/smb-endpoint.md)，错误与竞争路径验证见[测试策略](../../../../docs/testing.md)。
