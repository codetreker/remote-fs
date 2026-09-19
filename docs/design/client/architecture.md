# client 角色

volume 的使用者。持有一份 remote storage，把 volume 呈现为本地目录，并维持这一呈现所需的全部本地状态。

本文只写 client 内部。基础 storage、保留文件、锁控制与 RPC 的边界见 [`../architecture.md`](../architecture.md)。

## 一、内部构成

| 组件 | 职责 | 需求 |
|---|---|---|
| **remote storage** `packages/transport/httprest` | 基础 storage 操作逐次转换为 HTTP 请求，不缓存内容。复制的订阅与快照使用独立长连接；`DialOptions` 限制 stream silence、body 与 admission，超时由调用方配置。 | R-INT-3、R-INT-5、R-INT-9 |
| **显式锁控制** | HTTP client 实现锁 Service，调用方保留 Session / Owner 与原动作身份，以 `WithScope` 构造独立、不可变的修改 proof 集合。控制请求具有独立预算。 | R-CC-3、R-CC-6 至 R-CC-11、R-INT-3 |
| **本地副本** `packages/storage/replicated` | 一个 storage 装饰器：路径 `Stat`/负名字 Stat 可走本地副本；公共 `List/ListBounded` 经可用性检查后回源，身份能力走 authority。副本是一份 SQLite（`packages/metastore/sqlite` 的 `Replica`），由变更流喂着。 | R-CON-1~4、R-ERR-1、R-ERR-2、R-INT-3、R-SEC-3 |
| **挂载呈现层** `packages/fuse` | 把一份 storage 呈现为本地目录。持有 FileSession、对象引用与内核 owner 的映射；文件以 direct I/O 逐次读写。仅 Linux。 | R-FS-1、R-CON-1~3、R-ERR-1、R-ERR-2、R-WS-5、R-INT-3、R-INT-8 |
| **生命周期** | 挂载的建立与拆除。 | R-WS-2 |

```
   程序 ──▶ 内核 VFS
                │
                ▼
        ┌──────────────────────────────┐
        │          挂载呈现层          │
        │  每个打开对象一个服务端引用  │
        └──────────────┬───────────────┘
                       │ storage 接口
                       ▼
        ┌──────────────────────────────┐
        │           本地副本           │
        │  本地 Stat；目录列表回源    │
        └───────┬──────────────┬───────┘
                │              │ storage 接口
                │              ▼
                │      ┌──────────────────────┐
                │      │    remote storage    │
                │      │   一次调用一次请求   │
                │      └──────────┬───────────┘
                │ 变更流          │ RPC
                ▼                 ▼
```

挂载呈现层要求 `storage.FileStorage`，并在 session 建立时检查 AtomicFileOpener、NamespaceAccess、MetadataAccess、UseOwners、RangeControl 与 NodeReferences。本地具备这一能力的 storage 可直接挂载；它同样不知道底下那份 storage 有没有副本。随附的 localstore 与 Azure 服务端都提供 change log。集成方不提供日志时，复制入口以 `ENOSYS` 说明没有副本，此后每一次 metadata 查询都是一次远端请求。

remote storage 的 `DialOptions.MaxBodyBytes` 缺省为 1 GiB，限制 non-write 请求与 non-streaming response；`MaxWriteBytes` 在默认 options 中保持零值，拨号时继承 settled `MaxBodyBytes`，显式值必须为正且不大于它。`Write` 在发请求之前按 `MaxWriteBytes` 以 `EFBIG` 拒绝。读取 response 时先检查 `Content-Length`，再用 limit reader 检查实际字节数，因此错误或缺失的长度也不能绕过 `MaxBodyBytes`。过大的 `Read` 以 `EFBIG` 返回；过大的 listing、attribute、space 或 error message 是无法解码的协议答案，以 `EIO` 返回。无法安全计算四倍 response reservation 的 `MaxBodyBytes`，包括 `MaxInt64`，在拨号前被拒绝。

SSE 不把整个 stream 保存在内存里，但每一帧仍有独立的 `DialOptions.MaxFrameBytes`，默认 8 MiB。scanner 在读取下一帧时按这个值限制自己的 buffer；server 与 client 可以选择不同的值，实际可用上限由较小者决定，超出 client 上限的帧使 stream 失败。`remote-fs -http-max-frame-bytes SIZE` 把这个 client 上限交给部署方，使提高了 server frame 上限的 volume 仍能被挂载；它与 server 的同名 flag 接受相同的 1024 进制 suffix，显式非正值在连接前被拒绝。一个 stream reader 同时只保留一帧；`httprest.Storage` 不拥有调用方建立的 stream 数量，所以调用方仍须约束自己同时打开的 subscription 与 snapshot。

每个基础数据调用都要先取得 client 自己的 response admission。默认同时保留 64 份响应、允许 64 个等待者，aggregate 上限为 8 GiB；每份都按 `4 * MaxBodyBytes` 预留，覆盖 raw body、decoded listing 与转换过程的同时保留。Subscribe、Resubscribe 与 Snapshot 在发出 HTTP 前也取得同一名额，用来约束 stream 尚未成功建立时可能返回的普通 error body；确认 `200 text/event-stream` 后立即释放，后续 frame 由 `MaxFrameBytes` 约束。等待者已满时，`Stat`、`Write`、`Create` 或 stream setup 都会在发出 HTTP 请求前以 `EAGAIN` 失败；context cancellation 会移除等待计数。non-stream admission 一直持有到 response 解码、mutation response/barrier 验证完成。`ReadBounded` 取 client 与调用方 byte bound 中较小者；`ListBounded` 把解码后的 entry 逐项交给调用方的 `ListResult`。普通 `Read` 与 `List` 仍返回完整 materialized value，但整个 HTTP body 及其同时表示都在上述单体与 aggregate 边界内。server 侧的 backend 预算与 response admission 见 [`../server/architecture.md`](../server/architecture.md#六请求与响应的内存边界)。

目录 metadata 与引用名字观察是独立可选能力，FUSE 不要求它们或 ReferenceIdentity。支持时，包装器将 guards、IncludeName、ListResult 与引用身份交给 authority，名字不从副本重建；失败不回退到旧路径。原 File 字节方法及 State/Stat 保持自己的捕获，完整接口见[文件能力设计](../server/file-handles.md#显式-metadata-与名字观察)。

**FUSE 到这一层为止。** 挂载层将 Lookup、Create、Mkdir、Unlink、Rmdir、Rename 与目录读取转换为父 NodeID/原始叶名的能力调用。普通 fd 使用 File，无 fd 的身份属性使用 StatNode/SetNodeAttr；目录 handle 保留 NodeReference 及其 Scope。FileSession 拥有服务端引用，挂载层拥有内核编号、POSIX codec 与 owner 映射，不以过时父路径重新定位对象。 Opendir 的目录引用只申请 ReadMetadata 与 ReadEntries，读打开不要求写打开授权。目录 fd 的 chmod/时间修改在同一 handle mutex 中核对当前 Scope 与保存值相等，必要时读取原引用的 metadata 版本，再通过 session 的固定 NodeID CAS/SetNodeAttr 执行；Releasedir 使用同一锁关闭唯一引用。FileSession 的最终 publication guard 与 identity-operation 排空继续处理到期/退役，这条路径不能省略引用检查而只发裸 ID 修改。

### SMB 协议与会话端点

packages/smb 提供本机端点和认证会话，internal/wire 负责有界报文，internal/signing 负责 preauth/KDF/AES-CMAC 及签名会话销毁。Request slice 借用输入帧，拥有者在处理结束前保留帧；旧协议代码的复用范围与 NOTICE 见[协议与会话决定](../../../.agents/notes/implemented/architecture/2026-09-16-smb-protocol-primitives.md)。

[Config](../../../packages/smb/config.go) 必须显式提供 Authenticator、Authorize 和有效 Limits；调用方可选择 DefaultLimits 后调整，零值不被自动补齐。Share{Name, Volume, Backend} 把名称绑定到可信业务 volume 和原 storage.FileStorage，配置使用 keyed literal。host 保留 authenticator、logger 和 backend 的所有权；每次 Begin 返回的 Authentication 由端点关闭。

New 创建端点，Publish 在检查 backend 前预留 export 名额、验证 share 名并拒绝大小写重复及 IPC$ 保留名；不扫描 volume 或打开根引用。Serve 只接受 loopback TCP listener/peer，且仅能启动一次；成功接纳的 listener 由端点在取消或 Shutdown 时关闭，拒绝的 listener 仍由 host 拥有。Unpublish 在活动请求或打开引用存在时返回 ErrBusy；开始退役后拒绝新 tree，失败清理继续由原 Export 保留到重试成功。

SMB 3.1.1 支持直接及 SMB1 形状的 negotiate bootstrap，要求签名；bootstrap 不表示支持旧 dialect。packages/smb/windows 的 SSPI Begin/Step/Close 持有单次 native context，Principal.SID 用于身份判断，Name 不授予访问。每个受支持语义操作用当前 principal context 和可信 Volume 重新调用业务授权；先验证的新 principal 才能退役对应 previous session，不能借旧身份上下文通过授权。

每个已分配 SMB session 拥有一个认证到期 watcher，受现有 MaxSessions 和连接 worker 归属约束，同一时刻至多等待一个 timer。HandshakeTimeout 从本次交换开始计算，MORE_PROCESSING 和其它 session 的流量不延长它；Begin/Step 与最终安装使用同一绝对 deadline。watcher 在 authMu 下核对 generation/armed/deadline，成功、失败或退役使旧到期事件失效，无需等下一帧才处理遗弃的交换。初次认证到期退役该 session；重新认证到期只终止新交换，保留原身份与 signer。仍执行的 provider 调用与失败的 Close 保留拥有者/额度，不能并发销毁 context 或宣告已释放。

一个 SMB session 与 Export 共享一份直接 FileSession，多 tree 只增加引用计数和一份续期 worker。初次 Status 建立保守期限；旧 revision 回复不能延长新期限，Renew/epoch 异常 fencing 所有共享 tree，旧 handle 不重绑。IPC$ 是独立控制 tree，没有 volume、FileSession、root 或文件引用。TREE_CONNECT 不 pin 根，也不为后续每次 I/O 增加 Status。

handle 的 NodeReference 是唯一关闭拥有者；普通文件的可选 File 只是同一对象的接口别名。open reservation 在安装前拥有原 Attr/Outcome、返回字节和一个名额；错误与非 nil 引用同返仍进入 cleanup 注册表。安装与退役有序，退役后到达的 open 不能成功安装；借用者和 native 清理完成前，引用与 charge 均不释放。关闭不补做 Stat 或名字查询。

晚到的 TREE_CONNECT 在外层 defer 先结算 creator/export 计数，再复核同一退役完成条件。只有 session 已退役、没有认证/最终安装、watcher 已停止、opening tree/tree/authority 均为空，才标记资源已收齐；已有 retiring-frame 门继续等待最后响应/签名用户后归还 signer 和全局 session 额度。认证收尾、watcher 退出、最后 authority 移除也复用这个纯状态复核，不再次发起 native cleanup；未退役 session 的失败打开不被误删。

Shutdown 和 Unpublish 的并发调用共享当前 cleanup attempt 的不可变结果；后续调用才能重试。Shutdown 返回本轮清理结果，历史失败保留在 Status/日志诊断中，不把已经恢复的资源继续报告成本轮错误；未知关闭仍保留原 connection/session/tree/open/export 额度，不因协议句柄已移除而归还。取消等待者不取消或遗弃正在进行的尝试。

默认 server-wide exports/connections/sessions 为 32/16/16；每 session 最多 32 tree，每 tree 和每 export 的 opens 各受 1024 上限约束。每连接 128 request 包含 pending async；compound/context 上限为 32/16，frame 为 2 MiB，I/O 为 1 MiB，token 为 65535 字节，handshake/request/cleanup 为 30/60/30 秒。FileSession 另有自己的限制。分配、捕获结果及 compound 回复在效果前收费，删除 map entry 不返还仍保留的 backing 容量。目录观察使用现有目录 byte/entry 与 guard 上限；通知配置不表示已经分配监听资源。

credits 核对长度和读写范围，重复/越界拒绝；资源拒绝仍产生签名响应。related compound 保持顺序及继承身份，async 以唯一 AsyncID 核对 CANCEL 和当前 session，pending/terminal 的 credit 归还各一次。LOGOFF/断线分别等待真实借用与响应/签名用户，注册表锁不跨授权、storage I/O 或等待。

当前实现包含会话/控制、不带 postquery 的 CLOSE、受限 CREATE、READ/WRITE/FLUSH 及下述 QUERY_INFO。SET_INFO、范围、目录枚举、通知与映射尚未接入，相关请求在 session/tree 检查后明确拒绝。它不是可用的 Windows 网络驱动器。[测试策略](../../testing.md#smb-本机会话端点)区分真实 TCP、引用安装 fixture、交叉构建和原生运行；此前会话端点源码已有对应的 Windows ARM64 包级运行；它不覆盖新增 CREATE，也不等于系统 SMB 重定向器的文件访问验收。剩余接入、历史时间和缓存决策仍由[平台提案](../../../.agents/notes/proposed/architecture/2026-09-16-platform-client-capabilities.md)承接。

### SMB 名字与原子 CREATE

[名称解析](../../../packages/smb/namespace.go)使用当前 principal 对根 Stat 授权，再逐父按 OpReplicationSnapshot 取得完整有界 metadata 观察。每次观察重验已有前缀 guards，检查所有原始子名的 Windows 表示及大小写冲突后才选择目标；公开 ReadEntries 入口不能作为失败 fallback。解析保存实际 raw leaf、SameNode/Absent、root/目录/边 guards 和捕获 Attr，最终打开原子核对这些事实。只有完整观察证明的最终缺席才可让 OPEN/OVERWRITE 返回 STATUS_NO_SUCH_FILE；中间组件缺失有独立状态，backend ENOENT 保留原因并按 EIO 处理。

支持字面 tree-relative 路径：空值或单个反斜线表示根，可有一个前导反斜线；末尾单个分隔符保留目录要求。重复分隔符、点组件、正斜线和所有 ADS/冒号（包括 ::$DATA）明确拒绝，不作名字修复。叶名最多 255 UTF-16 单元，路径最多 32767，并服从中立 raw-byte/guard 上限；设备名、尾部点/空格和非法字符按平台规则拒绝。Windows 使用带显式 UTF-16 长度的 CompareStringOrdinal(ignoreCase)，非 Windows 的真实名字解析不支持；可移植测试比较器不是生产替代。

[CREATE](../../../packages/smb/create.go)支持普通文件和目录的 OPEN/CREATE/OPEN_IF，普通文件已有目标的 OVERWRITE/OVERWRITE_IF 需要明确 WRITE_DATA 并使用 ResetContent；已有目标的 SUPERSEDE、symlink 打开及未实现选项明确拒绝。缺席目标的允许创建分支通过 OnCreate 初始化；清空通过 OnReset，普通文件创建/清空设置 ARCHIVE。既有 readonly、hidden/system 与删除意图的检查依赖 `smb.windows` 版本/缺席条件，随 SameNode 交给最终原子打开；返回结果直接使用捕获 Attr/Outcome。

DesiredAccess/share mask 映射成既有 Uses/Deny；目录 LIST 对应 ReadEntries，EXECUTE/写意图也保留独立相容性 claim。普通字节引用使用 OpenAt，其余使用 NodeReference；后者内部 ReadMetadata 用于生成协议结果，仍在 OpenAccess.Read 中如实授权，不会给应用增加 FILE_READ_ATTRIBUTES。每个语义操作使用当前业务授权；reset 的属性/metadata、armed CloseIntent 分别取得对应权限。

单次 CREATE 最多四轮解析加最终打开，只有已知无效果的 ErrConditionConflict/EAGAIN 可重新观察。打开返回引用或非零结果后发生错误、结果未知、身份/Outcome 不符或清理失败均不能重提。效果前的 response reservation 和 AttrResultBudget 约束捕获结果，非 nil 引用即归原 cleanup 拥有者；成功安装后仍持有结果直到响应已消费，退役/取消不能释放尚被使用的内容。

[`smb.windows` codec](../../../packages/smb/windows_metadata.go)保存 12 字节 Data：SMW 加格式字节 1、LE uint32 DOS 属性、LE uint32 hints（bit0 为目录 symlink）。authority 的 OpaquePayload.Version 仍是原样 CAS token。缺席不提供 Windows 专有位，present 畸形为 EIO、未知格式为 EOPNOTSUPP；DIRECTORY/REPARSE_POINT 由捕获 NodeKind/hint 派生，无其它位的普通文件显示 NORMAL，初值复制保留其它平台 metadata key。四个共同时间来自同一次 Attr；必需 BirthTime/ChangeTime 未知即拒绝，FILETIME 只接受非负有符号 64 位范围并向下取 100 ns，不填造历史值。

[大小投影](../../../packages/smb/file_projection.go)把普通文件同一捕获 EOF 向上按 512 字节换算为稠密虚拟 extent，目录大小为零；这是平台分配表示，不是物理占用或预留配额。Share.Volume 必须由 host 提供稳定的规范身份；其加固定域前缀的 SHA256 前八字节作为 LE uint64 的展示 serial，share 别名不参与。它不用于授权、不承诺全局无碰撞，也不冒充设备 serial。QFid 返回捕获 NodeID 和该 serial；MxAc 明确返回不支持（匹配 ChangeTime 时 NONE_MAPPED），不制造最大授权 mask。未支持的 AlSi 按协议忽略；CREATE 外层字段/上下文边界继续检查，不授予缓存或 durable/reconnect 能力。

信息编码区分 basic/network/tag 所需的 FILE_READ_ATTRIBUTES 与身份/access 等结果，输入不足或不支持的 class 明确失败。helper 与 CREATE 的验证见[测试策略](../../testing.md#smb-名字metadata-与-create)，数据和查询命令使用下述原引用路径。

### SMB 保留引用上的字节与信息命令

[READ/WRITE/FLUSH](../../../packages/smb/file_io.go)仅在当前 tree 查找 FileID，借用原 handle 到响应构造结束，沿原 File/NodeReference、授权 context 与关闭排空执行。缺失引用为 FILE_CLOSED；Windows granted-access 与每次当前业务授权都须满足，不重开路径、不调用每次 I/O Status。EIO 分类优先于其包裹的冲突，未知修改没有成功 Count 或自动重发；已知范围/共享冲突、quota、容量错误分别映射。

READ 要求 FILE_READ_DATA 与普通 File 别名，只执行一次 ReadAt；同次返回的 Attr/数据须匹配原 NodeID、类型、大小和请求范围，不先 Stat 或用另一 revision 补齐。正长度在 EOF 处返回 END_OF_FILE，短于 MinimumCount 也返回该状态，其余短读返回实际数据。目录 LIST 和 metadata-only 引用不提供字节读取；channel、RDMA、压缩与未支持 flags 在访问前拒绝。

WRITE 要求 WRITE_DATA 或 APPEND_DATA。append-only 忽略传入 offset，通过原子 MutateAppend 取得实际 EOF；具 WRITE_DATA 的普通非负 offset 直接 WriteAt，负 offset 的 append 语义通过同一能力执行，缺少 current-position 事实的 -2 明确拒绝。每条命令只调用一次原写入，nil error 才返回完整 len(Data)，任何错误都不猜部分 Count、不在 SMB 层重试。零长度仍执行 native 无效果检查；READ/WRITE 的 offset/length 在转换前检查，数据由 MaxIOBytes/MaxFrameBytes 限制。当前不支持 unbuffered/RDMA；SMB 3.1.1 的 WRITE_THROUGH 单独 flag 按该分支规则为 INVALID_PARAMETER，未缓冲组合明确不支持，已有同步 publication 保持。

普通 WRITE 不读取或修改 Windows ARCHIVE，也不要求它已置位；clear/absent 可以在成功写后保持不变，不额外要求 WRITE_ATTRIBUTES。共同时间由原生内容发布维护，CREATE/reset 的 ARCHIVE 初始化独立保留。原引用已获写权时不以每次重新读取 readonly 撤销该权利。FLUSH 需要 write/append grant 和 File 别名，授权后实际等待 Sync；目录/metadata-only 没有 Sync 能力即明确拒绝，不以 State/Status 或空成功代替。

[返回值准入](../../../packages/smb/file_commands.go)在借用引用后、所有 ReadAt/WriteAt/Append/Stat/State/Sync 之前，从与 CREATE 共用的 registry.resultBytes/MaxDirectoryBytes 预留 MetadataRetentionBytes(MaxMetadataBytes)+512；State 再计入目标和 generation 的硬上限。耗尽或退役在 native/HTTP 调用前拒绝，锁不跨 I/O，响应构造完成后精确归还且晚于原操作结束。SMB frame/data 预算与这份 metadata 保留预算分开，合法大 metadata 不挤掉完整协商数据块。HTTP 自己的 body/response pools 约束中间表示；任意 AttrResultBudget callback 不被序列化，不能当作远端预留保证，原 context 中已有 callback 继续传递。

[文件 QUERY_INFO](../../../packages/smb/query_info.go)使用以下事实来源；所有 class 仍有当前 host 授权，内部 metadata 权限不扩展 Windows granted mask。

| class | 事实与权限 |
|---|---|
| 4 Basic、34 NetworkOpen、35 AttributeTag | 一次原 reference.Stat；要求 FILE_READ_ATTRIBUTES。34 的大小来自同次 Attr，35 不要求历史时间 |
| 5 Standard | 一次 ReferenceState，Attr/Detached/PendingUnlink 同次捕获；detached 或 pending 显示 links=0/DeletePending=true，否则 1/false |
| 6 Internal、59 FileId | 已保留 NodeID，59 使用与 QFid 相同的指定 serial；以 OpFileStat 授权披露但不调用 Stat/State |
| 7 EA、8 Access | 已知未暴露 EA 的大小零、实际 expanded granted mask；以 OpFileStat 授权但不查询 backend |

class 7 的零来自已声明的空 EA 接口，内部 opaque metadata 不是 EA。Position/Mode/All 等缺少 current-position/mode/name 的 class、其它未实现查询均明确不支持。固定结果容量不足在观察前返回 INFO_LENGTH_MISMATCH，SMB 3.1.1 携带规定的空 error context；不返回截断的固定结构。所选 file/filesystem class 忽略协议规定无语义的 AdditionalInformation/Flags/input 字段；decoder 仍保留声明 InputLength，credits 按 max(InputLength, OutputLength) 收费，忽略输入内容不允许少收容量。

[文件系统 QUERY_INFO](../../../packages/smb/filesystem_information.go)的 Size(3)/FullSize(7)每次授权后只调用一次 Backend.Space，要求 Coherent。以 512 字节虚拟单元向下换算 Total/Avail；FullSize 的 ActualAvailable 为 max(Total−Used,0)/512，明确表示未用虚拟 volume 预算，CallerAvailable 继续反映较紧的实际 backing 限制，不宣称物理空闲 cluster。Device(4)只声明 remote disk 接口；Attribute(5)只声明保留大小写的 Unicode 名字、255 单元上限及 REMOTEFS 显示名，不冒充 NTFS/ACL/EA/stream 能力。可变显示名截短使用偶数 UTF-16 前缀和 BUFFER_OVERFLOW；固定前缀不足仍失败。Volume(1)缺少创建时间/label，物理 sector/alignment 等未知事实不填默认值。

### 业务身份与授权结果

HTTP client 的凭据附加和轮换由嵌入方提供；server 使用业务 context 中的稳定身份执行[volume 操作授权](../server/authorization.md)。一份 mount／replica 及其 Session、File、Owner、Grant 固定用于同一身份。同一身份轮换凭据可保持连接，切换身份须创建新的客户端状态，不能带走前一身份的缓存。

直接 SDK 保留普通 422 和 stream fault 中的授权 EACCES／EIO。强锁响应包含 lockCode 或 recorded 任一字段时必须完整通过 native 校验，不能用半份 native envelope 冒充普通授权错误；拒绝核对不把原未知动作改为未执行。初始 frame 与所有 Next 包装保留有效 errno，缺省 errno 的 generic fault、null／畸形／未知 errno 均为 EIO。

副本观察到任一 follower 失败后仍统一为 EIO；这个状态与直接 SDK 收到 EACCES 不同。服务端逐次出站检查权限，但已交付的元数据和字节无法收回；下一次检查或业务 context 取消被观察前，副本仍可能回答已同步内容。撤权不是客户端缓存擦除协议。

### 显式占有与修改 proof

remote storage 使用 HTTP v4，同时提供基础数据操作与锁 Service。调用方用 enrollment ticket 建立 Session、创建 Owner、Resolve 现有普通文件并显式 Acquire；普通 FUSE Open 没有自动获取策略。Session、Owner、管理 Request 与本地描述符、TCP 连接、复制 incarnation 分别拥有生命周期，断开连接不提前解除已确认保护。

成功 Resolve 返回资源引用的有限期限、当前 tick，以及这次有效控制活动延长后的 `HistoryExpiresMillis`。后者描述 Owner / Session 的动作核对窗口，不延长 grant，也不由资源引用的有效期推导。重复 Acquire 保留原 ResourceRef 全部字段，新的 Resolve 观测不能改写已经提交的意图。

`WithScope` 复制有界 proof 集合并共享 endpoint、HTTP client 与 admission；`Scope` 提供相同的有界 storage 视图。所有修改及 WithBarrier 方法携带该集合，普通读与有界读省略它。replicated storage 把 scope 传给远端修改，同时保留本地 metadata 查询及既有 confirmation barrier。锁控制继续使用显式 Owner 参数，不从 scope 推导新的控制身份。

Acquire 返回立即结果或 Pending 登记；Wait 是远端等待意图的期限，不占着一条 HTTP 请求等待授予。调用方用原 Request QueryAction 或 Cancel，SDK 不自动轮询、续期或制造新身份重试。已记录的 Rejected 与 typed error 一起返回，使冲突后来消失时仍能核对原结果；未接纳与结果未知分别处理。

GrantStatus 的剩余时间由服务端对未取整的 deadline 与 now 求差再向下取整。SDK 以原请求发送起点加这个间隔建立保守提示，旧 receipt 不开始新 lease，普通读取成功也不刷新提示。最终权限始终由服务端检查。原授权方退役后，旧意图返回退役或结果未知，不能在新授权方中重做；字段与取整规则见[文件锁协议](../server/file-locks.md#结果与期限)。

控制请求与响应固定至多 16 KiB，独立的 `MaxConcurrentLockControls` 与 `MaxWaitingLockControls` 默认各 16，每份活跃操作预留 64 KiB。容量检查不占用数据 response 或复制 stream 的名额。state-changing control 进入 dispatch 后丢失响应时保持结果未知；Resolve、QueryAction、QueryGrant 与 Status 遵循只读取消。缺少 v4 marker、非法 scope 或不一致 receipt 都明确失败。服务端强锁 callback／native 生命周期的失败响应保持 native Unavailable／EIO、recorded=false 且无动作回执；该 wire 协议没有 EINTR code。SDK 本地在 HTTP Do 前接受的取消，以及只读控制在 client 侧接受的取消，仍可返回 EINTR，不经过 native wire 编码。业务策略拒绝另走普通 EACCES／EIO envelope。

## 二、元数据副本与权威读取

内核的目录项、属性与负项超时都是 0。公共 replicated.Storage.List/ListBounded 在原有 readiness 检查后直接调用远端，所有目录 Uses/Deny 在实际读取时判定；没有本地列表 fallback，也没有“先检查权限再读缓存”的窗口。显式路径 Stat 与负名字 Stat 仍可从副本答复。内部 metastore.Replica.List 保持本地实现，供复制视图操作与读写交接使用。

FUSE Lookup 使用 NamespaceAccess.LookupAt，以父 NodeID 定位叶名；Open/属性读取和目录枚举同样使用权威身份能力。目录 handle 的 Scope 在读取时核对，仅豁免该有效引用自身的 claim；不带 Scope 的目录读取也受同 session 中其它引用的 deny 约束。副本故障时普通操作失败，续期/核对与清理仍可联系原 authority。公共目录遍历付出逐目录远端往返，缓存不消除这些调用。

普通文件的 Open 与 Create 返回 `FOPEN_DIRECT_IO`。文件读取经过挂载层的健康检查与 `File.ReadAt`，不让同一 inode 的页缓存把旧 handle 内容交给新 handle。属性与返回区间来自同一次权威读取；direct I/O 不承诺共享 mmap 的完整行为。

副本以变更流的连续观察状态决定是否作答：流被观测为断开时作废，需要连续副本视图的操作以 EIO 失败（R-ERR-1、R-ERR-2）。副本可用性没有 TTL；权威目录读取仍另外核对实际访问限制。普通续订在追到 opening tail 后恢复可用；首次构建与全量重建使用[快照后的固定回放目标](../../../.agents/notes/implemented/bug-fix/2026-09-07-gate-rebuilt-replicas-on-replay.md)。

构建先保留原订阅，再逐页写入快照。客户端读到 snapshot 的语义 EOF 后，先关闭该 HTTP 响应释放连接，再调用 Checkpoint 取得新鲜的 `(incarnation, committed position)`。快照位置不得早于原订阅的 opening tail；checkpoint 不得早于快照，且其 incarnation 必须与原订阅一致。snapshot 只报告捕获位置；incarnation 一致性由 checkpoint 与原订阅核对。

本地 Seeding.Complete 成功后记录已安装的快照位置，唯一的 reader 随后沿原订阅丢弃已包含的事件、应用快照之后的事件，直到达到或超过固定 checkpoint。位置允许跳跃，不要求逐整数相邻；零位置且已达到目标时不等待一条不存在的事件。Complete 与每次 Apply 的成功位置都会在内部推进，即使副本仍以 EIO 拒绝查询；中途失败后的续订以已提交树的位置继续，而不是沿用更早的公开状态。最终追平前不清除失败状态。

`Options.ReplayTimeout` 必须为正，DefaultOptions 取十秒，从客户端观察到 snapshot EOF 时起覆盖响应关闭、Checkpoint、Complete 和目标回放。订阅握手与快照传输在这段预算之前，仍受各自 context 与既有边界约束。独立 CLI 继承此默认值；mutation 的 ConfirmationGrace 保持独立。超时、取消、流错误、化身不符或位置不相容使 gate 以 EIO 失败并保留原因；Checkpoint 的 ENOSYS 也不能被当作“不提供复制”而降级为直接模式。

构建的 Snapshot 与 Checkpoint 保留调用方 context 值；原订阅属于 storage lifetime 的子 context，构建期间临时关联调用方取消。失败由 reader 关闭订阅，取消回调只发出取消，不与 reader 并发 Close。成功前解除并等待临时取消关联，再确认构建与 lifetime 仍有效，最后恢复查询；成功返回后取消原构建 context 不切断订阅。固定目标之后的修改由正常 follower 接续，不移动目标追逐每个未来提交，也不把这一门槛描述为全局即时新鲜度证明。

内部 SQLite replica 的所有 SQL 读共享与 reader pool 一致的 16 个名额。List/ListBounded 先取得独立的 15 个 listing 名额，再进入共享 SQL 名额和读阶段；Stat 直接竞争共享名额，使扫描 backlog 不占满短查询的入口。实现按 `max(1,N-1)` 限制 listing，所有读的总量仍不超过 N，没有增加连接池。

等待 listing/SQL 名额不持有读阶段，取消或后续准入失败归还已取得的名额；查询退出先离开阶段，再归还 SQL 与 listing 名额。Position 不查询 SQLite，只取得无取消的共享阶段。这个调度只隔离尚未准入的扫描队列，不使 Stat 越过已经登记的写者或撤回已捕获快照。

私有读写门在共享读阶段与独占写阶段之间交接。写者登记后，新读者排队，现有读者排空后进入写阶段；写者退出时先为已经等待的有限读者批次预留名额，再唤醒它们，下一写者等待这些活跃或预留读者全部退出。等待取消撤回相应名额；门只保存固定数量的计数与共享通知状态，不保存逐等待者队列。`Apply` 与整次 `Reseed` 不占 SQL 读取名额，直接使用独占阶段，后者在 `Seeding.Complete` 完成事务或 `Seeding.Close` 中止时释放；`Add` 或 `Complete` 的前置校验失败仍须由调用方 `Close`。取得多项所有权时，列表先取 listing quota；随后按共享 SQL 名额、replica 门、commit gate、database health lock 的顺序，各入口只取得自己需要的部分。此机制保证阶段间交接，不承诺多个写者之间的 FIFO 或已进入操作的执行时长；取舍见[副本写者推进](../../../.agents/notes/implemented/bug-fix/2026-09-07-let-replica-writers-progress.md)。

**「流一断」是被观测到的，不是被假定的。** 服务端在无话可说时按固定间隔发一行心跳，这一层给「一个字节都没来」设一个数倍于心跳的上限，超限与流上任何一次失败走同一条路。没有这条，一条被切断的 TCP 与一个安静的 volume 是同一个观测结果 —— 沉默 —— 而副本会一直答下去，且没有时间上界。上限压在**正在等的那次读**上而不是压在连接上，因为首次同步期间没有人读变更流；计时由**字节**重置而不是由帧重置，因为一个快照分页可以是一整行一兆字节。

这份副本因此不是缓存。server 提供 change log 时，每个会产生日志的 mutation 在发出请求前先 admission 一条 fixed-size confirmation record，不保留 target path、direction 或 touched-name history。`ConfirmationGrace`、`MaxActiveConfirmations` 与 `MaxWaitingConfirmations` 默认分别为 10 秒、64 与 64；`remote-fs` 用 `-confirmation-grace`、`-max-active-mutation-confirmations` 与 `-max-waiting-mutation-confirmations` 暴露同一组设置。active 名额不足时有限等待；请求尚未发出时，纯调用方取消为 `EINTR`、deadline 为 `EIO`，实际容量饱和或 storage 开始关闭则以 `EAGAIN` 拒绝，原始原因被保留。规范的 `ENOSYS` 表明不提供复制时不建立副本，也不保留 confirmation state。snapshot EOF 后的 checkpoint／回放门错误统一为 EIO，即使原因链包含 ENOSYS，也不能触发无副本模式。

mutation 成功后，replicated client 从严格验证过的 response 取得 `(incarnation, position)` barrier，把它与当前 replica incarnation/generation 对齐，再等待本地 position 达到或越过它。event 先于 HTTP response 到达时当前位置已经足够，立即完成；另一个 writer 的较早 change 不能误确认本次 mutation，因为 barrier 不早于本次 commit。stream rebuild 改变 generation、barrier incarnation 不匹配、stream failure、调用方取消、storage 关闭或 grace 到期都以 `EIO` 报告「volume 已改变但本地副本无法确认」。失败只结束该调用，不把仍连续的 stream 单独判坏；迟到事件仍按 change-log 顺序应用。空 attribute change 与 rename onto itself 不产生日志，仍发送给 server 取得 pathname 的权威结果，但不预留或等待 barrier。

副本的建立、作废与恢复规则，以及写入方等待 mutation barrier 的原因，见[元数据复制](../../../.agents/notes/implemented/architecture/2026-08-27-metadata-replication.md)。

文件能力与 scoped 视图共同转发原 FileSession；NodeReference、Scope、metadata、namespace、范围和删除能力保持原远端身份，属性与字节不从名字副本重建。部分打开出错仍有引用时保留可关闭结果，不能在封装错误时遗弃它。Close 先完成原清理，再确认 authority 的 barrier；已损坏的 live stream 使确认返回 EIO，不跳过关闭。修改使用同一远端 authority，并通过现有 confirmation barrier 核对 volume 进度；失去名字的文件不制造路径事件。

## 三、打开的是对象引用

一个 handle 保存 `storage.File` 和访问方式。Open 使用节点 ID，Create 经 OpenAt 把创建、排他条件、初始 POSIX metadata 与截断交给一次权威打开；非排他创建已有对象时保留已有 metadata。`O_TRUNC` 在 open 返回前完成，即使之后没有任何 write。

```
打开   ──▶ OpenNode / OpenAt，取得对象引用，不取内容
读取   ──▶ File.ReadAt，返回同一状态的属性与区间字节
写入   ──▶ File.WriteAt，同步确认指定区间的修改
截断   ──▶ File.Truncate，同步确认长度与内容
关闭   ──▶ 清理 owner 与引用
```

既有 fd 看到同一对象的后续修改。rename、unlink 或同名替换后，它继续指向原对象；新打开的名字可指向另一个对象。`Getattr` 从 File、NodeReference 或 StatNode 取得当前身份属性；共同时间由 SetAttr/SetNodeAttr 修改，chmod 通过指定 POSIX namespace 的版本 CAS。没有 fd 的 truncate 先按节点身份取得短期引用，再截断与清理，不能通过旧路径修改替换者。

普通 fd 写入按实际顺序组合，重叠区间以较后生效的操作为准。Open 不自动获取 advisory 或强 S/X 权限；显式 scope 由服务端最终发布检查执行。内部内容 revision 用于构造当前对象的补丁，不代表调用方携带了显式内容版本依据。

### 大小与物化预算

`Options.MaxFileSize` 默认 1 GiB，零值选默认，负值在挂载前拒绝。挂载把有效值传给 FileSession；服务端在每份捕获的 revision 被物化或暂存前检查会话与 volume 上限。写入终点或目标长度超限时整次以 `EFBIG` 拒绝。截断到零可以丢弃超限旧内容，不取回旧 body。

FUSE 不保存全文件缓冲区，objectstore 仍可能完整读取、重建不可变对象。每次读取与替换受[服务端物化预算](../server/file-handles.md#五http复制与资源)、transport body、对象存储与配额共同约束；提高某一层上限不会放宽其它层。只读 open 不取回超限内容，后续实际读取或非零截断仍在物化前拒绝。

## 四、advisory locks 与关闭

挂载启用 FUSE locks，raw bridge 将 `Getlk`、`Setlk`、`Setlkw` 映射到 session 的 UseOwners/RangeControl。`flock` 使用 OFD owner，dup/fork 共享，最后一个共享 fd 释放；传统 POSIX 锁使用该挂载内核 owner 的显式映射，关闭同一文件的任一 fd 都解除该 owner 的所有 POSIX 范围。PID 只用于报告冲突，不在独立挂载之间充当全局 owner。

DomainWholeFile 与 DomainRecord 独立；FUSE 将 inclusive `[start,end]` 转成 Bytes(start,end-start+1)，flock 覆盖 `[0,MaxInt64]`。flock EX 可用于只读 fd，POSIX 写锁要求可写。flock 转换先解除旧锁，POSIX 失败转换保留旧范围。阻塞调用以短请求登记、查询和取消，在会话健康时可持续等待；单次 HTTP 超时不是整个锁等待的截止时间。取消核对证明没有残留授予后才返回 `EINTR`。FUSE 遇到未知锁结果时封锁整个挂载，普通操作持续为 `EIO`，停止续期并退役 FileSession；单个 fd 的解锁或关闭不恢复该挂载，调用方须完成清理并重新挂载。原生 advisory API 对 owner 的独立清理能力不改变这项挂载级终止。完整范围与历史契约见[advisory 设计](../server/file-handles.md#四advisory-范围与-owner)。

FUSE 注册 OwnerReference 或 OwnerExplicit，把同进程的记录锁 owner 放进 session 内的死锁 Group；Group 不共享权限。PID 保留在本地诊断映射，远端只返回不授予权限的 OwnerDiagnostic。`Flush` 通过 RangeControl.Drop 清理本次关闭的记录锁 owner，`Release` 结束 File 引用及其 whole-file owner 生命周期。它们不提交内容；`Fsync` 调用 `File.Sync` 检查已完成修改的健康与持久性。多次 Flush 不产生重复内容写入，最后 Release 的错误不能作为写入失败的唯一报告位置。

`Options.FlushTimeout` 用于文件清理与挂载建立，默认 30 秒，负值在挂载前拒绝。清理 context 忽略关闭线程的取消，保留请求值与较早 deadline；预算只限定清理尝试，不承诺 mutex 等待、内核 Unmount 或整个 Mount.Wait 的耗时。底层 storage 的 Close 仍由它的拥有者负责。

## 五、取消与故障分别作答

挂载层通过 `storage.ErrnoOf` 分类错误，`nil` 为成功。已接受的请求取消返回 `EINTR`；deadline、未知错误与无法证明修改结果的失败返回 `EIO`。go-fuse 的请求 context 被取消后，原 FUSE 请求仍得到回复。

文件创建和 Mkdir 的初始 metadata 使用原子结果；同时修改大小、POSIX metadata 或时间的 Setattr 仍可能包含多个受控阶段。某阶段已经产生效果后，后续取消通过拥有最终分类的 `EIO` 保留原始原因，不能把整个操作报告成未发生。效果开始前接受的取消仍为 `EINTR`。

remote storage 在 HTTP `Do` 前接受取消时返回 `EINTR`，已发出的只读操作也可放弃读取。修改进入 dispatch 后，请求取消不能证明未执行；文件动作通过有界历史核对，仍不能确定的结果以 `EIO` 报告。打开的响应与确认失败必须清理或退役相应引用，不能留下调用方未知的无限引用。没有 Create／Truncate 的已有文件打开，在 ACK 为带 context.Canceled 的规范 EINTR、且同一能力的 Close 清理原始结果为 nil 时，返回无 File 的 EINTR；其余 ACK 失败保持 EIO，完整条件见[文件确认协议](../server/file-handles.md#五http复制与资源)。这不把任意打开变成可重试操作，也不改变丢失 ACK 的既有核对。成功修改未取得副本 barrier 确认时同样为 `EIO`。网络 errno 不进入 volume 错误链。

到达挂载层的错误不会改写成空目录或不存在。文件 direct I/O 与权威目录读取也不替代副本连续性；各自依赖的 readiness、身份与访问检查都须成立。分类与阶段判定见[请求中断](../../../.agents/notes/implemented/bug-fix/2026-08-22-eio-from-a-freshly-mounted-mountpoint.md)。

## 六、volume 答不上来的，挂载呈现层不代答

storage 契约有四个共同时间字段、opaque metadata 与整个 volume 的容量；Linux mode 由客户端 codec 解释，没有通用属主或直接映射的 xattr API。凡是内核问到而契约答不上的，挂载呈现层报错：

| 被问到 | 回答 |
|---|---|
| `chown` 改成挂载者以外的属主 | EPERM |
| 契约之外的其它属性 | EPERM |
| 扩展属性 | EOPNOTSUPP |
| `renameat2` 的 `RENAME_EXCHANGE`、`RENAME_NOREPLACE` | EINVAL |
| 符号链接指向哪里 | EOPNOTSUPP |
| 硬链接 | 不提供（R-FS-4） |
| 类型无法命名的节点 | EIO |

`chmod` 使用 posix.permissions.v1 的原子 payload 更新，`utimens` 使用已有 File/NodeReference 或节点身份；文件和目录的初始权限与创建在同一次权威操作中保存。属主是唯一一个报错的属性：volume 不带属主，挂载点把每个节点都报成挂载它的那个用户（R-SEC-1），因此把属主改成那个用户就是它已经是的样子，改成别人则无处存放。

posix.permissions.v1 保存四字节 little-endian uint32，仅允许 07777。缺席时只显示普通文件 0644、目录 0755、符号链接 0777；存在的畸形 payload 或缺失 authority version 报 EIO，不回退默认值。ctime 优先显示已知 ChangeTime，否则显示 ModTime；两种展示投影都不写回 authority，也不补齐未知历史 BirthTime。

目录的链接数一律为 1。

### 符号链接

基础 volume 的十一个路径方法不含 symlink/readlink。NamespaceAccess 的 NameSymlink 与 ReferenceState.LinkTarget 可供编程调用使用；FUSE 没有实现对应的创建/readlink handler，仍按既有接口拒绝链接解析。已有链接的 Kind/Size/metadata 可以如实呈现：

| 对一个符号链接做 | 结果 |
|---|---|
| `lstat`、列目录 | 报告为符号链接，模式与长度都是链接自己的 |
| `readlink` | EOPNOTSUPP |
| `stat`、`open`、读、写 | EOPNOTSUPP —— 内核解析这些路径时先 `readlink` |
| `rm` | 删掉链接本身，它指向的文件不动 |
| 改名 | 搬动链接本身 |

于是**不存在「以链接的名字拿到它指向的那个文件」这条路径**。答不出指向哪里就报 EOPNOTSUPP，不报 EINVAL —— 后者的意思是「这不是一个链接」。

## 七、容量

volume 报出自己的容量，挂载呈现层把它换算成内核要的块数：

| 内核要的 | 从哪来 |
|---|---|
| 块大小 | 固定 4096 字节。volume 按字节计量，块只是报出去时的计价单位 |
| 总块数 | volume 的总量 |
| 空闲块数 | 总量减已用，不为负 |
| 可用块数 | volume 报的「还能写入的量」 |
| inode 数、空闲 inode 数 | 都是 0。这里不数 inode，0 是「没有 inode 表」的报法，`df` 把它显示成没有这一栏，而不是显示成已经用尽 |
| 名字长度上限 | 255 |

三个块数都向下取整：装不满的一块不计入。

自己没有容量可报的 volume 以 `ENOSYS` 拒绝，这个拒绝原样到达调用方 —— `df` 说「功能未实现」，那是实话。三个数不能同时为真时是 EIO。两者都不换成编造的数字（R-WS-5、R-ERR-2）：FUSE 库对不作答的文件系统的默认回答是一个全零结构，读起来是一块没有剩余空间的盘，而先查空间再决定写不写的程序会照着它行事。

**默认配置下，这一整节只对挂载它的那个用户成立。** 别的调用方问 `statfs` 时，内核自己回答，回的是一个清零的结构，请求根本到不了这一层。于是 `sudo df` 与同机的其它本地用户看到的都是一块 0 字节、0 可用的盘 —— 那正是上一段拒绝去编造的那个答案，而挂载这一侧没有任何东西能改变它。

改变它的开关在主机上，不在本系统里：fuse 模块参数 `allow_sys_admin_access`（`/sys/module/fuse/parameters/allow_sys_admin_access`，默认关）打开后，初始 user namespace 中带 `CAP_SYS_ADMIN` 的调用方绕过这项检查，`sudo df` 于是问到这一层，读到的是真数字。因此这条代价是有条件的：默认配置下 `df` 是一条只对挂载者有效的通道，要让 root 也看得见，需要运维在主机上打开那个参数。

问容量这一次调用带自己的 2 秒截止时间，不用挂载点通用的操作超时：`df` 会走遍机器上的每一个挂载点，一个够不到的服务端否则会让整台机器上的 `df` 卡满那个超时。

### 配额在修改调用上裁决

`WriteAt` 与 `Truncate` 同步执行原生发布记账，超出配额以 `EDQUOT` 返回对应的 write 或 truncate。挂载不缓存剩余容量，也不在数据修改之前调用 Space；Statfs 仍独立查询容量。缩短只在发布成功后释放差额，失败保留原内容与收费，避免其它写者提前花掉尚未释放的字节。

失去名字但仍被 fd 引用的文件继续计入用量。最后引用退役、在途操作排空且物理释放完成后才回收容量；未知结果不能伪造空闲空间。机制与通用包装器的独立边界见[容量上限](../../../.agents/notes/implemented/architecture/2026-08-21-space-limit.md)。

## 八、节点 ID 直接成为内核编号

`storage.Attr.ID` 是不透明、非零且不会被复用为另一对象的 volume 内节点身份。挂载把它直接报告为 inode number；同一对象被其它挂载移动到此前未见过的名字时，也保持原编号。节点 ID 不由路径或宿主 inode 推算，不使用局部新编号补救下层错误复用。

挂载仍保留一棵名字成员树，用于同名查询复用、删除、改名和 List 结果清理。节点内的本地 serial 只区分本次 listing 开始前已知的成员与期间新发现的成员，不是对外 inode。相同名字返回不同 ID 或类型时替换成员记录；被覆盖的旧 inode 可以继续被 fd 引用，失去名字不使它变成新对象。

名字树随挂载结束清理，节点身份由 volume 保持。其它 client 的删除或替换由后续权威 Lookup/ReadDirNode 观察；目录操作只携带父 NodeID，不使用该树派生的过时路径。

两个随附 backend 都使用 SQLite 的持久节点身份。[宿主目录后端已移除](../../../.agents/notes/implemented/simplification/2026-09-08-remove-the-host-directory-backend.md)；第三方实现仍须满足 R-FS-5 与 R-INT-11，不能直接报告可能被复用的宿主 inode。

## 九、生命周期

一次挂载拥有一个 FileSession。`Options.FileSession` 未提供时使用默认 options，显式 options 在建立前验证；实际 MaxFileSize 与挂载大小界限一致。后台续期在上一份已确认 lease 内完成，成功状态只以保守的请求起点更新 deadline。服务端重启、会话退役或期限耗尽使挂载失败，不按路径重开文件，也不自动重新取得 advisory lock。

`Unmount` 失败，例如仍有使用者而返回 `EBUSY` 时，会话继续续期。内核连接退出后，挂载停止续期并尝试排空全部引用；个别 Release 缺失也由会话清理覆盖。`Mount.Done()` 在这次清理尝试结束后关闭，`Mount.Wait()` 返回它的错误，Done 关闭不意味着清理成功。独立 client 等待 Done 后才释放 replica，释放失败保留其目录与错误。

提供 change log 的 volume 在初始快照完成前不可挂载使用，失败不降为直通查询。首次 Subscribe 发出前已关联构建 context 与 storage lifetime；构建取消会结束握手/快照/回放，失败由唯一拥有者清理订阅。成功交接时停止并等待临时取消关联，此后取消原 New context 不会切断运行中的订阅。副本位于只允许属主访问的专属目录，`-replica-dir` 指定位置；只有清理成功才删除它。冷挂载仍需等待快照，R-WS-4 的冷启动成本保持独立。

## 十、能力边界

本地副本只复制名字与元数据；内容由保留 File 逐次读取，普通文件不使用内核页缓存。元数据缓存、负项和失效策略的后续选择见[内核缓存](../../../.agents/notes/proposed/architecture/2026-08-19-kernel-cache-and-unreachable.md)。

同步写入不在离线时返回成功，不把未知失败重试为新写入。原生补丁实现可在已知未提交的 revision 竞争后有界重试；这与应用重做一个结果未知的修改不同。普通 fd 没有隐含的内容版本前置条件，显式版本工作流仍独立。

目录 handle 保留 NodeReference 和 Scope，首次枚举捕获有界目录流；已捕获流不承诺在每个 Readdirent 之间重建快照。无 handle 的父 inode 操作是 fresh 身份请求，必须通过本次授权、种类/linkedness 和访问检查；失效 Scope 不降成该路径。FUSE 的精确叶名请求不默认携带目录 revision，因此无关 sibling 的创建不会成为全目录 CAS 冲突。

标准 advisory 包括 flock 与传统 POSIX 范围锁，完整 `F_OFD_*` 和 mmap 行为不由此推出。显式 S/X 仍单独取得，挂载不自动选择持锁策略。

## 十一、部署形态

作为库嵌入集成方既有的 daemon service，或作为独立二进制运行（R-INT-1、R-INT-4）。作为库时不注册信号处理、不写标准输出、不调用进程退出、不修改进程级设置、包加载时不产生副作用（R-INT-2）；日志只写入调用方给定的目的地，未给定则丢弃。

独立 client 用 `-max-file-size`、`-file-session-lease` 与 `-file-session-history` 配置文件会话，默认分别为 1 GiB、30 秒与 1 分钟。`-timeout` 默认 30 秒，约束单次远端交换或文件清理尝试；健康会话中的阻塞 advisory 等待可以跨多次交换。

只使用 remote storage 而不挂载是第三种用法（R-INT-5），这条路径不依赖 FUSE，因此不受 Linux 限制。
