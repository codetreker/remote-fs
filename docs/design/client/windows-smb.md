# Windows 本机 SMB 接入

本页描述 Windows client 将远端 volume 发布为本机网络驱动器的结构。Windows 语义由本机 SMB 适配解释，远端提供通用 FileStorage、身份条件与跨入口访问保护；边界取舍见[平台客户端隔离决定](../../../.agents/notes/implemented/architecture/2026-09-16-isolate-platform-filesystem-clients.md)。跨角色接口见[顶层设计](../architecture.md)，共同权威见[保留对象](../server/file-handles.md)。

## 组件与所有权

Windows 应用与资源管理器使用系统 SMB 客户端连接宿主进程内的 loopback SMB endpoint。远端继续提供 storage 接口与 HTTP；它不承担 SMB 服务。Linux FUSE 与 Windows SMB 是独立的呈现入口，选择一条路径不要求加载另一平台的实现。

| 组件 | 职责 |
|---|---|
| [`packages/smb`](../../../packages/smb/config.go) | SMB 3.1.1 协商、认证交换、签名、tree/open 映射、请求准入与有界清理 |
| [`packages/smb/internal/wire`](../../../packages/smb/internal/wire) | 长度、偏移、compound 和命令体的严格解析与编码 |
| [`packages/smb/windows`](../../../packages/smb/windows/mapping.go) | Windows SSPI、已验证 SID 的授权适配、当前用户的盘符映射与移除 |
| `Share.Backend` 的 `storage.FileStorage` | 通用节点／目录引用、版本条件、claims、范围 CAS、动作回执与清理 |
| `Share.Changes` | 宿主注入的 Subscribe、Resume、Checkpoint，提供与 backend 同源的完整变更观察 |

SMB 核心属于仓库的 MIT 实现；签名密钥派生选取的 BSD 代码保留在[许可声明](../../../packages/smb/internal/signing/NOTICE)中。核心依赖 storage、metastore 和 authz 契约，不导入 HTTP transport。宿主可把 HTTP 的订阅、续订与 checkpoint 方法接到 `ChangeSource`，也可提供满足同一契约的其它来源。

`smb.New` 要求显式的 Authenticator、Authorizer 和有效 Limits。构造不打开监听端口，也不创建系统映射。`Server.Serve` 接受宿主交给它的 loopback TCP listener；非 loopback 或非 TCP listener 被拒绝。`Publish` 返回一个 Export；底层 backend 与外部身份配置仍由宿主持有。

## 发布与访问身份

`Publish` 校验 FileStorage 能力、FileState 的 volume 身份及事件预算、通知来源与本机 Limits，不启用持久 Windows 命名策略，也不修改其它入口的名字规则。share 名称与可信 `Share.Volume` 授权标识分别配置，不能从请求中的 share 名冒充业务 volume 身份。SMB 在实际名字操作中完成可表示性与唯一性检查。

`IPC$` 是不区分大小写的保留 share 名，不能通过 Publish 绑定业务 volume。已认证且通过签名校验的 SMB session 可以建立独立的 IPC 控制树；它与 volume tree 共用该会话的 MaxTrees 预算，并随 tree disconnect、logoff 或连接清理释放。响应的 pipe-share 类型只描述控制树类别，不承诺 named-pipe 或 RPC 能力。控制树不取得 Export、volume backend、FileSession、通知源或 volume 授权请求，也不枚举服务端资源。tree 级正常操作仅有 TREE_DISCONNECT；session 级 ECHO／LOGOFF 保持原有行为。未支持的 pipe／RPC／控制操作明确失败，畸形 IOCTL 被拒绝，SMB 3.1.1 的 FSCTL_VALIDATE_NEGOTIATE_INFO 按协议终止连接。

Windows 的 `NewAuthenticator` 在每次 Begin 时取得独立的 inbound Negotiate credentials，不保存密码。认证交换使用 SSPI 返回的身份与 session key；匿名、guest 或不可用的身份不获得正常 session。原生 SSPI 调用是同步调用，取消会在原生工作返回后阻止身份交付，Close 等待已经进入的原生调用结束。

`CurrentUserSID` 取得当前进程用户的 SID，`AllowSID` 只接受 SMB context 中由 SSPI 验证的同一 SID。宿主显式选择这项策略或自己的 Authorizer，并继续决定访问远端所用的身份。loopback 地址本身不授予另一个本机用户访问权限；elevated 与非 elevated 登录会话也不由 helper 合并。

初始连接可先发送 SMB1 帧形状的 multi-protocol NEGOTIATE 前导。核心只接受严格校验、包含 `SMB 2.???` 的 NEGOTIATE，返回 SMB2 wildcard `0x02ff` 响应；下一步必须是真正的 SMB2 格式 NEGOTIATE。前导不建立认证会话，不开放 SMB1 文件操作；重复前导、其它 SMB1 命令或跳过正式协商的 SESSION_SETUP 均被拒绝。

正式协商只接受 SMB 3.1.1、SHA-512 preauthentication integrity 与 AES-CMAC signing；preauthentication hash 从真正的 3.1.1 协商开始，正常 session 的签名不能关闭。这项 bootstrap 不扩张旧版 Windows 支持范围。SMB 在本地校验完整 Windows intent，再以通用 Effects、Claim 与目标身份接受业务授权；远端 HTTP handler 按自己的可信身份独立授权，本机授权不替代远端授权。

SMB SessionID 由 Server 实例统一分配，在该实例内跨连接唯一且不复用；它与 MappingStatus 中的 OS 登录 SessionID 是不同身份。SESSION_SETUP 的保留字段 Channel 被忽略，不代表接受 multichannel binding。PreviousSessionId 只在新认证成功并完成认证 context 收尾后处理，使用最终成功请求中的值：零、当前候选 session、已不存在或属于其它 SID 的旧 ID 被忽略；同 SID 的旧 session 通过其原拥有者退役并清理。旧状态清理失败阻止新身份激活，保留清理所有权；这条路径不恢复旧 tree、FileId 或 durable handle。

## 保留对象与远端结果

SMB 的 clientBackend／clientSession／clientFile 是本地平台适配，底层共用 storage.FileSession 和 File。保留对象可为普通文件、目录或 metadata-only 引用，rename、unlink 与同名替换不改变原引用的节点身份。平台类型、Windows failure 与 DesiredAccess／ShareAccess 不进入远端协议。

CREATE 在本地解释六种 disposition，选用 RetainAt、CreateAndRetainAt、ResetAndRetainAt 或 ReplaceAndRetainAt。精确目标、初始 metadata、内容重置、claim、引用与 Prepared 意图由一次固定原子操作提交。Rename 的 Destination.Name 描述观察到的确切目的，NewName 单独携带请求的输出拼写；ancestor witness 在最终效果处校验，不能由旧父路径重建。

访问声明区分内容与 metadata：metadata-only open 不占内容 Uses／Excludes，目录不因 ShareWrite 限制就禁止创建子项；实际 ListDirectory 引用可排斥 RemoveEntry。Windows read／write／delete sharing 由本地映射为共同 claims，所有入口的内容与名字修改仍经过权威冲突检查。append-only 且没有 WriteData 的 WRITE 以 wire offset 作为 ExpectedSize 条件；大小不匹配的已知未提交结果映射为本地 access denied，不增加远端 Append flag。

Windows 范围批次在本地从同一 RangeSnapshot 计算最终集合、已应用前缀与平台错误，再提交 ReplaceRanges。每次 acquisition 独立保留，精确 unlock 不消除重复 shared lock。结构界限先检查，元素语义按顺序解释：无效后项可保留已成功前缀，真正冲突要求回滚的前缀则被移除。未知 CAS 只核对原动作，不按后来快照重算旧计划。

FILE_DELETE_ON_CLOSE 在创建／保留时安装 Prepared，不立即阻止相容 open；关闭或 expiry 才激活。目录在关闭时非空则不激活；这一步不重新检查 DOS readonly。SetDisposition(TRUE) 此刻检查 readonly、空目录和共享条件并 DrainEntry，FALSE 只取消当前 generation 的 drain，不消除其它 Prepared。Draining 目录拒绝经旧父引用的插入，最终 detach 再检查 IfEmpty；意图跟随 EntryID，不误删旧名字上的替代物。

READ 返回同一捕获状态的属性与范围字节；WRITE、截断及属性修改等待远端结果，FLUSH 检查已发布状态。write-through 与 mapping 的 UseWriteThrough 不等于 FILE_NO_INTERMEDIATE_BUFFERING；后者明确以不支持拒绝。

每个动作保留原 epoch／nonce 和本地计划。通用 receipt 的状态、Effects 与冲突供本地投影，Windows Applied 数量和平台错误由该计划保存；有错误不等于全部回滚。HTTP 不自动核对未知结果。SMB 明确 QueryAction／CancelAction 当前拥有的原动作；本次 NotAdmitted 不解答较早的未知提交。无法核对时 tree 与引用失败并保留清理责任，不包装成一次成功修改。

共享冲突、范围冲突、delete-pending、未持有范围及非 reparse point 在 SMB 内映射为不同 status。业务拒绝为 access denied；不可达与未知结果保持 I/O failure，不产生空目录、虚构不存在或旧内容。R-CON-5 中应用大 I/O 被拆分时的保证单位仍未决定，单个请求的验证不证明整个应用调用。

## 零缓存权利的 SMB lease

NEGOTIATE 保留 ClientGUID，并宣告 lease 与 directory-lease 协议能力。CREATE 在请求 OplockLevel=LEASE 且携带一个有效 RqLs 时，支持 32 字节 V1 与 52 字节 V2 context；成功响应使用 OplockLevel=LEASE 和 LeaseState=NONE（零）。普通打开、未携带 RqLs 的请求以及非 lease 打开仍返回 oplock NONE。编码器不复制请求中的缓存位，不授予 read、write 或 handle caching 权利，也不发送 lease break。

Server 的有界表以 `(ClientGUID, LeaseKey)` 关联记录；ClientGUID 不代替认证身份。每个打开关联自己的权威 `(VolumeIdentity, NodeID)` 与有界名字信息。重复 key 先作零内容使用的身份探测，对非根目标把 ExpectedNodeID 带到最终条件操作；根目录核对不可替换的根身份。成功关联打开使 DeleteOnClose 置位后，该标志在记录存活期间保持，可容纳额外独立对象关联，不重绑定既有打开。

V1／V2 响应格式由本次请求决定，记录中的 epoch、parent-key 元数据保持自己的生命周期；V1 回复省略 V2 字段，不清空已有元数据。普通 oplock 请求与 durable 请求仍可以被拒授而不使普通打开失败，durable context 不返回授予，跨连接恢复旧文件也不因此成立。

pending admission、已关联引用和 fenced 结果各有清理所有者。已确认的 Close 释放对应关联；tree 退役不足以证明尚未核对的原生打开已经结束。未决 token 由原 tree 或共享 FileSession的 orphan 记录持有，只有对应引用与在途操作确认清理后才归还预算。关闭失败保留这些有界状态，不以遗忘记录制造可用容量。

零权利 lease 不改变 backend API、通知来源或同步确认。原生验收已观察到 live.bin 的有效 V2 RqLs／State=NONE，但负查询后一秒可见性失败，后续专用目录阶段未执行；具体记录和边界见[测试策略](../../testing.md#windows-11-arm64-原生入口)。

## 名字、metadata 与符号链接

SMB 使用 ListAt 获取同一 DirectoryRevision 下的完整有界目录，在本地检查 UTF-8、Windows 名字表示、保留名和大小写唯一性。任一不兼容条目使受影响的观察失败，不能遗漏条目或任选一个匹配。其它入口仍可写入这些名字。按身份 I/O 和不要求位置的 Stat 不重做名字解析；需要名字的操作必须取得并核对当前位置。

EntryLocation 包含和 Attr／link target 一致捕获的祖先链。SMB 验证每段名字与同版本目录投影，所有名字相关修改携带完整 witness；祖先改名或祖先同级出现冲突名字，都会使旧条件失效。CheckObservation 在输出完整路径等多次观察结果前确认条件，root、linked 与 detached 各自有明确状态。

`smb.windows` 是 SMB 自己拥有的 opaque metadata key。version 1 的 8 字节 payload 由 little-endian DOS 可设置属性和 DirectorySymlink hint 两个 uint32 组成；更新保留其它 key。缺少该 key 时使用 Config.AbsentDOSAttributes，零表示结合通用 Kind 呈现基本类型／normal 属性；已有 payload 损坏或版本不支持时明确失败，不套用缺省值。通用创建／change time 缺失保持未知；FILE_BASIC_INFORMATION 中显式 ChangeTime=-1 在修改前拒绝。

CHANGE_NOTIFY 使用通用不可变 Notification 的 SubjectKind、ChangeMask 和 Before／After EventImage。每份图像包含事件时 Attr、opaque metadata 与 EntryLocation，删除或后续修改不丢失这些事实。SMB 据此解释目录符号链接和 Windows filters，远端没有 Windows 推导的 Directory 位。rename 旧名／新名属于同一事件组，队列不能只保留一半。

Subscribe 从当前已提交 tail 开始，Resume 核对 incarnation、位置和历史连续性，Checkpoint 给目录观察提供明确边界。通知队列按事件数与字节数计费；历史丢失、无法表示或队列不足要求重新枚举，不能报告没有变化。来源不可达或无法保持健康时相关访问失败。变更源、checkpoint 与 backend 属于同一 authority。

GET/SET_REPARSE_POINT 只处理符号链接。SMB 在本地解析目标并约束于 Export，通过通用 SetKind 对空且唯一引用持有的节点原子设置 kind、target 和 opaque metadata，保留 NodeID；目录链接 hint 属于 SMB metadata。remote 将 target 当作数据，不解释 UNC、宿主路径或 Windows 父跳转。普通打开遇到链接时使用权威捕获的 target／location 与本地 suffix，不能通过旧路径猜测目标。

Linux FUSE 观察通用链接类型，类型转换后同一节点保持稳定 inode。Linux Readlink／Symlinker 仍返回 EOPNOTSUPP；类型一致性不等于已经支持 Linux 链接创建或解析。

## 有界资源

[`smb.DefaultLimits`](../../../packages/smb/config.go) 必须由宿主显式选择；遗漏配置不会被解释为无限。默认值如下：

| 项目 | 默认值 |
|---|---:|
| connections / sessions / trees / opens | 16 / 16 / 32 / 1024 |
| requests / compound 元素 / create contexts | 128 / 32 / 16 |
| SMB frame / 单次 I/O | 2 MiB / 1 MiB |
| authentication token | 65535 字节 |
| directory result | 8 MiB |
| notify events / notify bytes | 1024 / 8 MiB |
| handshake / request / cleanup timeout | 30 秒 / 1 分钟 / 30 秒 |

Server 的 lease 表在所有连接之间共享 `MaxOpens` 个 slot 和 `MaxDirectoryBytes` 元数据字节上限，分别默认 1024 与 8 MiB；它复用配置值，但与普通打开及单次目录结果分别计费。每次打开先预留两个最大本地名字信息路径、固定结构和 volume identity 的预算，安装成功后转为实际记录与逐打开关联的费用。`Status.LeaseSlots` 和 `Status.LeaseBytes` 报告包括 pending／fenced 所有权在内的占用。

文件会话使用 `storage.DefaultFileSessionOptions` 的独立上限与有限历史，见[文件句柄](../server/file-handles.md#身份属性与会话)。请求输入在解码与分配前检查长度、credit、compound 和 context 数量；目录结果与通知有各自的总量边界。提高某项上限仍须通过 Limits 的关系校验。

通用 Notification 最多 256 层祖先、64 KiB 祖先名字，canonical 编码至多 256 KiB；单次 Change 的名字、content key、metadata、target 与 notification 变长字段总量至多 512 KiB。全局 Record／decoder 执行这些资源界限，和某个平台是否接入无关。

Windows 路径投影在 SMB 本地受 65792 原始字节与 32767 UTF-16 code units 限制，分隔符计入；target 与未解析 suffix 各至多 4096 字节。目录观察、位置验证及最终 witness 覆盖名字相关修改。通用 remote 只执行自身固定资源边界，不安装 Windows 路径阈值或全 volume profile。

HTTP 使用共同 file／file-control 通道和结果 envelope，资源归属见[server 内存边界](../server/architecture.md#六请求与响应的内存边界)。SMB 本地计划、目录投影、lease 表与通知队列另外计费；提高远端 body 上限不会取消本地预算。

## 当前用户的盘符映射

`windows.Map(ctx, MappingOptions{LocalPath, Share, TCPPort})` 仅连接 `\\127.0.0.1\share`。helper 要求 Windows 11 24H2 或以上，接受单个盘符、有效 share 名与非零 TCP port；不提权、不占用已有盘符、不修改机器策略，也不接收密码。

Windows SMB 客户端要求同一 server、同一 transport 的映射共用一个端口；不同 share 名不隔离这项连接状态。宿主应让各 Export 的映射复用一个 Server 和 listener 端口。Unmount 只结束所拥有的映射，不保证 Windows 立即清除该 server 的端口关联；端口冲突可以在认证前被系统拒绝，不能据此判定身份或业务授权失败。helper 不通过全局断开连接或重试隐藏这项拒绝。

创建使用系统明确支持的 typed 参数：指定 TcpPort、TCP transport、UseWriteThrough、RequireIntegrity，并关闭 Persistent、GlobalMapping 和 SaveCredentials。参数缺失或类型不相容会失败。固定 PowerShell 程序通过 stdin 接收 JSON 值，share 名不拼入命令语法。

创建参数被接受与 OS 身份被观察是两件事。`MappingStatus.ParametersAccepted` 表示创建成功接受了请求参数；另记录实际观察到的 LocalPath、RemotePath、DOS-device target 和 connection status。provider 可以暴露 TcpPort、RequireIntegrity、UseWriteThrough 或 TransportType 等属性；helper 的公共契约不依赖或验证这些 provider 属性，也不假定它们有跨 provider 一致的查询保证。Status 不把这些字段报告为已查询或已验证，不依赖 provider-specific generation，且不进行一次新的 OS 查询。

Mapping 记录 SID、AuthenticationID 和 SessionID。Unmount 再次核对登录身份、盘符、UNC 与 DOS-device target，只移除自己创建并仍能识别的映射。宿主必须独占管理这个盘符，禁止在 Mapping 存活时由外部替换它，包括替换为 OS 无法区分的相同映射。这里没有跨进程或全局盘符注册表。

创建结果未知返回 `ErrMappingUncertain`，不取得删除某个观察结果的权限。创建已确认但后续核验或回滚失败时，Map 可以同时返回非 nil Mapping 和 error；调用方保留该对象继续清理，不能只丢弃错误。回滚有独立的 30 秒预算，并遵守同样的 ownership 检查。

## 停止与能力边界

普通 Unmount 遇到打开文件返回 `ErrMappingBusy`，保留可用映射；ForceUnmount 是显式破坏性断开，后续 I/O 可以失败，不宣称无损关闭或远端修改已经确认。移除映射不关闭 Export 或 Server。

Export.Unpublish 在仍有 opens 或活跃请求时返回 `ErrBusy`。宿主按映射、export、server 及外部 backend 的各自 ownership 收尾；Server.Shutdown 停止接纳并清理自己拥有的连接、tree 与会话，失败通过返回值和有限 Status 计数暴露。renew 使用会话 lease 的三分之一作为节奏；授权或续期失败关闭受影响的 tree。没有依赖 import 副作用建立的 listener、信号处理或系统映射。

SMB lease 只按上述 NONE 状态协商；普通打开保持 oplock NONE，不授予缓存权利或传统 oplock，也不提供 durable handles、multichannel、跨连接恢复旧引用或 encryption。`DHnQ`／`DH2Q` 可以被拒授而保留成功的普通打开，不返回 durable 授予 context。`FILE_DISALLOW_EXCLUSIVE` 按 SMB2 规则忽略。本库将 `FILE_OPEN_FOR_BACKUP_INTENT` 按普通打开处理，保留原 DesiredAccess／ShareAccess 与业务授权，不据此授予 SeBackupPrivilege／SeRestorePrivilege 或绕过权威访问检查；`FILE_NO_INTERMEDIATE_BUFFERING` 仍明确拒绝。未知 create context、未支持的信息类别和控制请求继续失败；这些处理不声明完整 NTFS 兼容，alternate data streams、Windows ACL 管理与共享 mmap 一致性仍在需求非目标内。

实现的协议检查、真实 authority 集成与 Windows 原生验收分别见[测试策略](../../testing.md)。源码或非 Windows 上的测试通过不能替代 Windows 11 ARM64 上真实 SSPI、系统映射与应用文件操作的执行结果。
