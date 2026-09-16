# Agent Note: Windows 本机 SMB 接入 package

Status: implemented

## 问题

Windows 上未经修改的程序需要访问远端 volume，集成方需要把这项能力嵌入自己的 Go 程序。安装额外文件系统驱动增加了部署与分发成本；复制到普通本地目录再上传无法保持逐次远端确认、同一文件身份和断线错误行为。

目标系统为 Windows 11 24H2 及更新版本。业务程序拥有远端连接、访问身份和进程生命周期，系统中的其它用户不能仅凭同机连接取得访问权限。Windows Server 不属于增加的验收平台。文件身份、同步确认、可见性和错误保证继续适用。

本决定拥有本机 SMB 引擎、身份、映射、通知与缓存边界的取舍。建立这项接入时，Windows 专用 authority 和持久命名 policy 用于集中共享与名字约束；这些跨层机制由[平台客户端隔离决定](../architecture/2026-09-16-isolate-platform-filesystem-clients.md)取代。该决定保留通用权威保护，把平台解释放回客户端；本页继续记录原选择的理由、仍有效的本机机制与未通过的原生验收。

## 决定

### 自有协议引擎与宿主所有权

[`packages/smb`](../../../../packages/smb/config.go) 在宿主进程内提供 loopback SMB 3.1.1 服务，Windows 系统客户端通过自定义 TCP 端口访问 share。远端继续提供 storage 与 HTTP；本机协议不增加远端 SMB 暴露，也不要求额外文件系统驱动。

协议、调度、签名与结果转换由仓库自己的 Go 实现持有。核心采用仓库的 MIT 许可；SP800-108 签名密钥派生所采用的 BSD 代码保留[来源与许可声明](../../../../packages/smb/internal/signing/NOTICE)。这项选择使原生文件动作、授权和异步取消能够使用同一套明确语义，也意味着仓库承担协议实现与维护责任。

```text
Windows 应用 → 系统 SMB 客户端 → loopback SMB
                                  |
                             packages/smb
                                  |
                 通用 FileStorage + 宿主注入的 ChangeSource
                                  |
                      HTTP／SSE → 远端 authority
```

SMB 核心依赖 storage、metastore 和 authz，不导入 HTTP、SQLite、FUSE 或 CLI。`Share.Backend` 与 `Share.Changes` 分别提供权威操作和同源的 Subscribe／Resume／Checkpoint。现有集成使用 HTTP 与 SSE，宿主也可以提供满足相同契约的来源；backend 和变更流必须指向同一 authority。把这份关系留在宿主配置中，避免协议引擎拥有另一套远端连接或目录副本。

`New` 校验显式配置，`Serve` 接受宿主交付的 loopback TCP listener。Publish 检查通用 FileStorage、volume 身份及观察预算，并拥有 Export 的观察器；它不安装全 volume 命名策略。映射、Export、Server 与外部 backend 分别关闭：Unpublish 在仍有打开对象或活跃请求时返回 busy，清理失败保留停止中的所有权；Shutdown 停止接纳并等待自己拥有的资源，不关闭调用方的 backend。

系统客户端的保留 `IPC$` 连接由独立控制树表示，不能把它配置成一个业务 volume Export。它只接受已认证、通过签名校验的 session，计入同一 MaxTrees 预算并遵守 session/tree 清理。pipe-share 响应不代表实现了管道对象；控制树没有 volume backend、FileSession、通知或发现接口，不制造一个 volume 身份来请求业务授权。它正常处理 TREE_DISCONNECT，session 的 ECHO／LOGOFF 保持原义；未支持的 pipe、RPC 与控制命令明确失败。已发布 volume 的授权和后端访问保持原有路径。

### 本机身份与显式映射

[`packages/smb/windows`](../../../../packages/smb/windows/auth.go) 使用真实 SSPI Negotiate 交换取得 token 身份和 session key，每次 Begin 持有独立的 native credentials/context。匿名、guest、无可用签名密钥的交换被拒绝。连接允许一次严格限定的 SMB1 帧形状 multi-protocol NEGOTIATE 前导：必须包含 `SMB 2.???`，wildcard `0x02ff` 响应之后仍须进入真正的 SMB2 格式协商，期间不建立认证会话或接纳文件操作。正式 dialect 只接受 SMB 3.1.1、SHA-512 preauthentication integrity 与 AES-CMAC signing，hash 从正式协商开始，正常会话要求签名。这个入口只处理系统客户端的协商前导，不提供 SMB1 文件操作或旧版 Windows 支持；重复前导被拒绝。同步 SSPI 调用返回后仍检查取消，Close 与已进入的原生调用串行收尾，不把取消解释成原生工作已经停止。

本机 Authenticator 与 Authorizer 必填。CurrentUserSID／AllowSID 提供已验证 SID 的显式策略，业务可注入自己的策略。可信 Share.Volume 由宿主配置；本地解释完整 Windows intent 后，以通用 Effects、Claim 与目标身份授权。远端 HTTP 身份由业务 transport 管理，两端授权独立；token 和签名 key 不进入日志。凭据轮换不替换现有会话已验证身份，重新认证必须遵守 SID 规则。

SMB SessionID 在每个 Server 实例内跨连接统一分配且不复用；它不是 OS 的登录 SessionID。SESSION_SETUP.Channel 按保留字段忽略，binding flag 仍不受支持。只有新认证成功且认证 context 完成收尾后，才使用最终成功请求的 PreviousSessionId：不存在、属于其它 SID 或指向当前候选 session 的值被忽略；同 SID 的旧 session 由原拥有者退役、排空并清理。清理失败阻止新身份激活并保留旧所有权。这支持会话替换的协议顺序，不恢复旧 tree、FileId、durable handle 或 multichannel。

`Map` 只在显式调用时改变当前用户的系统映射。helper 检查 Windows client edition、build 和必要的 typed 参数，将 `TcpPort`、TCP transport、`UseWriteThrough`、`RequireIntegrity` 与关闭持久／全局／凭据保存的选项交给 `New-SmbMapping`。程序固定，值通过 JSON stdin 传入，不拼入 PowerShell 语法；它不请求提权，也不修改注册表、安全策略或系统 445 服务。系统拒绝权限或不支持的参数时，错误保留给宿主。[Microsoft 的端口说明](https://learn.microsoft.com/en-us/windows-server/storage/file-server/smb-ports)与[映射参数](https://learn.microsoft.com/en-us/powershell/module/smbshare/new-smbmapping?view=windowsserver2025-ps)定义系统能力，实际 loopback 认证仍须原生验收。

Windows 的同一 server／transport 映射共用一个端口，宿主为各 Export 复用同一 Server／listener。share 名不同或先移除旧映射，不保证 redirector 已经忘记这个端口关联；端口不一致可在新认证之前被明确拒绝，不能作为身份策略拒绝的证据。本机 adapter 保留该错误，不使用全局断开连接或重试来清除它。端口复用只解决连接条件，不证明 negative lookup 或目录缓存的一致性。

`MappingStatus` 区分参数已被接受与实际观察到的盘符、UNC、连接状态、DOS-device target。provider 可以暴露端口或策略属性；helper 不依赖或验证这些 provider 属性，不假定跨 provider 一致的查询保证，也不依赖 provider-specific generation。Mapping 保存 SID、AuthenticationID、SessionID，并在移除前核对实际映射。宿主必须独占管理选定盘符，因为同一登录中的外部替换若恢复为相同 tuple，系统观察无法证明其代际。创建结果未知不取得删除权；已确认创建但核验或回滚失败可以返回 Mapping 与 error，调用方保留该对象继续清理。

普通 `Unmount` 使用非强制移除，有打开文件时保留映射并返回 busy。`ForceUnmount` 是明确的破坏性断开，不保证在途 I/O 成功或远端结果已确认。二者都不关闭 Export／Server。[WNetCancelConnection2W](https://learn.microsoft.com/en-us/windows/win32/api/winnetwk/nf-winnetwk-wnetcancelconnection2w)的按登录映射与 force 行为，是这些所有权限制的依据。

### 平台解释与同一权威

当时选择 WindowsStorage／WindowsSession／WindowsFile，是为了让父身份、共享、delete-pending 与批次效果进入同一原生发布门，使 Linux／HTTP 不能绕过限制；本机锁表不能约束另一台机器。其代价是平台规则进入公共 storage、HTTP 和数据库。[平台客户端隔离决定](../architecture/2026-09-16-isolate-platform-filesystem-clients.md)以固定共同原语承接保护，保留这一跨入口保证。

SMB 内部保存 Windows intent、错误和 range planner，通用 FileSession 提供 Retain／条件创建重置、claims、范围 CAS、Prepared／drain 和原动作结果。NodeID 与 SMB FileId 分开，EntryID 随 rename，unlink 后引用不转向替代物。READ 捕获同一属性与字节，修改等待远端确认，Close 不是首次提交。

动作使用原 epoch／nonce；本地计划保留 Windows Applied 和部分错误，通用 receipt 确认 Effects 与状态。Unknown 由拥有计划的适配器显式 Query／Cancel 原 ID，HTTP 不自动核对或重新修改。本次 NotAdmitted 不否认此前未知效果；不能核对时 fence 并保留清理责任。[LOCK](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/670c7eda-e683-4923-9477-414303959613)、[UNLOCK](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/79eb3c91-563b-4d48-a51c-0974f9d144f8) 与 [CANCEL](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/57bae3d3-5dd7-4a5f-92cb-fc52e2087dad) 的顺序不能压缩成一个成功布尔值。

### 名字、链接与不可变通知

本机 SMB 使用同版本的完整有界目录投影与完整 ancestor witness，解释 Windows 名字、大小写和路径范围。其它入口可写入 Windows 无法表示的名字，相关 Windows 观察明确失败，旧身份 I/O 独立成立。初始方案的持久 policy 曾约束所有入口且卸载不撤销；这一代价由平台隔离决定消除，不代表放宽 R-CC-14。

DOS 和目录链接 hint 位于 smb.windows opaque key；修改保留其它 key，缺席使用显式缺省，坏数据失败。符号链接目标是通用数据，SMB 在 Export 内解释并用 SetKind 原子转换空、唯一引用持有的节点。NodeID 和已有 S/X 保护保持，新的 Resolve 仍只接受普通文件；FUSE 呈现真实类型与稳定 inode，Readlink／Symlinker 仍拒绝。完整行为见[Windows 设计](../../../../docs/design/client/windows-smb.md#名字metadata-与符号链接)。

Notification 和同一 changes 行一起提交，使用事件时 SubjectKind、ChangeMask 与前后 Attr／metadata／EntryLocation 图像。SMB 本地分类目录符号链接，删除后仍不查当前树。rename 两半属于一组，覆盖目标的删除有自己的记录；无名对象不制造旧路径事件。复用 Position、predecessor 链、incarnation、保留窗口与 SSE，避免第二份日志的提交／裁剪关系。

Export 建立订阅并达到固定 checkpoint 后才健康。首次 CHANGE_NOTIFY 先登记观察者再取 B，保留 >B 的事件；同一 open 沿用过滤器、递归范围与队列。历史缺口或容量不足要求重新枚举，损坏／不可达报告真实故障；恢复连续性不抹去已丢历史的结果。通知不是 Windows 内核缓存一致性的证明。[CHANGE_NOTIFY](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/05869c32-39f0-4726-afc9-671b76ae5ca7) 与 [FILE_NOTIFY_INFORMATION](https://learn.microsoft.com/en-us/windows/win32/api/winnt/ns-winnt-file_notify_information) 定义协议表示。

### 有界资源与缓存边界

宿主显式选择 DefaultLimits 或完整 Limits，连接、会话、tree/open、请求、compound、contexts、frame/I/O、目录、通知与各阶段期限都有边界。FileSession 有独立有限历史，HTTP bulk、control 与 waits 分别 admission；结果按完整 metadata、witness 和 receipt 编码计费。各池配置不代表一个合并的总额，资源归属见[server 内存边界](../../../../docs/design/server/architecture.md#六请求与响应的内存边界)。

映射请求 UseWriteThrough，并禁止离线缓存。SMB 处理真正的 V1／V2 lease 协商：有效 lease CREATE 返回 OplockLevel=LEASE 与 RqLs，编码器把 LeaseState 固定为 NONE；普通打开仍返回 oplock NONE。请求的 read、write 和 handle caching 位不被授予，也不发送 break。`DHnQ`／`DH2Q` 仍可被拒授而使普通打开成功，不返回 durable 授予 context；不提供 durable/persistent handles、multichannel、旧文件跨连接恢复或 encryption。未知 create context 继续拒绝。

Server 保留 ClientGUID，用 ClientGUID／LeaseKey 关联有界记录，每次 open 绑定 VolumeIdentity／NodeID。重复 key 先作零内容使用的身份探测，再以 ExpectedNodeID 和完整条件保留非根目标；根身份不可替换。成功 open 设置的 DeleteOnClose 在记录存活期间保持，允许额外独立关联，不重绑定旧 open。响应使用本次 V1／V2 格式，V1 不清空已有 epoch／parent metadata。

lease 表在所有连接之间使用 MaxOpens slot 和 MaxDirectoryBytes 元数据上限；这是独立预算，不能与普通打开或单次枚举误算成一个合并总额。准入先预留两个最大路径及固定结构，状态计入 LeaseSlots／LeaseBytes。未知结果和清理失败不能提前归还预算：pending／fenced token 由原 tree 或共享 authority 的 orphan 记录持续持有，只有引用与在途操作确认结束才释放。具体协议与身份规则见[Windows 接入设计](../../../../docs/design/client/windows-smb.md#零缓存权利的-smb-lease)。

`FILE_DISALLOW_EXCLUSIVE` 按 SMB2 规则忽略；本库对 `FILE_OPEN_FOR_BACKUP_INTENT` 明确选择普通打开策略，原 DesiredAccess／ShareAccess 与业务授权仍适用，不授予 SeBackupPrivilege／SeRestorePrivilege，也不绕过访问检查。这不放宽 `FILE_NO_INTERMEDIATE_BUFFERING` 的不支持结果。UseWriteThrough 的 forced-unit-access 语义与 `FILE_NO_INTERMEDIATE_BUFFERING` 不同，前者不能证明关闭了所有属性、negative 或目录缓存。系统全局缓存设置保持宿主环境原状；真实程序的同步确认、可见性与断线读取仍须验收。[SMB 客户端缓存设置](https://learn.microsoft.com/en-us/powershell/module/smbshare/set-smbclientconfiguration?view=windowsserver2025-ps)不能作为库私自改变机器策略的理由。

## 备选方案

**远端直接提供 SMB。** 减少本机协议转换，但改变远端暴露和业务认证接入方式，不能直接复用现有 HTTP 网络路径。本机 adapter 保留独立远端 transport，共同文件接口仍供编程入口使用。

**WinFsp／Dokany 挂载。** 更接近 Windows 文件系统回调，但增加驱动安装、分发和生命周期。完整跨客户端范围锁还必须证明锁参数与取消／晚到授予之间的原子关系，不能只增加回调便声称兼容。这些成本不符合无需额外驱动的部署目标。

**识别 lease 请求后只返回普通 oplock NONE。** 这保留了不授予缓存权利的边界，却没有建立以 lease key 关联 epoch 与逐打开身份的协议记录，也不能由这个回复推出 Windows 负查询在一秒内可见。零权利 lease 明确处理这些协议状态，但实际原生负查询可见性验收仍失败；live-file 的合法零权利回复不能代替这一秒断言，也不能证明未执行到的目录阶段。

**本机目录索引或 metadata replica。** 一致快照加日志能够维护类型和祖先关系，但带来初始化、缺口重建和空间预算。权威提交已经持有旧／新状态，直接记录通知事实可以保留 HTTP 读取路径。本机 replica 仍可单独评估；它必须从读取副本的实际 apply 顺序产生通知，并与 Linux nativelease 依赖分离，不能用空实现伪造 Windows 支持。

**权威端另开按目录通知订阅。** 能在服务端过滤相对路径，但引入监视注册、作用域、游标与恢复协议。独立通知日志另有双日志提交和同步裁剪义务。复用完整 changes 行获得所需身份和顺序，没有增加第二份权威历史。

**Samba VFS 或 SMBLibrary helper。** Samba 的成熟 VFS 面向 Unix/Linux 部署，Windows 需要额外运行环境。SMBLibrary 带来 C# runtime/helper 与 IPC，锁等待／取消仍需扩展。它们未成为独立 Go package 的引擎。[Samba](https://www.samba.org/samba/what_is_samba.html)、[SMBLibrary 文件接口](https://github.com/TalAloni/SMBLibrary/blob/2edbcf3161084b51dbe11b8a960b0bc7c224b851/SMBLibrary/NTFileStore/INTFileStore.cs)。

**直接嵌入现成 Go SMB 服务端。** 调查的 `macos-fuse-t/go-smb2` 在所检查版本中无条件接受 LOCK、不支持 CANCEL、没有真实通知源，认证接口也不能从外部 package 实现；它的 AGPL／商业许可还需要单独选择。购买许可不能补足这些语义缺口。[锁、取消与通知](https://github.com/macos-fuse-t/go-smb2/blob/c0e6b139796e67ce8da148aa5942c55a7dff2fe8/server/file_tree.go)、[认证接口](https://github.com/macos-fuse-t/go-smb2/blob/c0e6b139796e67ce8da148aa5942c55a7dff2fe8/server/authenticator.go)。

**从其它实现提取协议引擎。** Sombrero 有 MIT 的服务端代码，但绑定 Sia/PostgreSQL，局部锁和通知也不能直接成为本系统的权威实现。CloudSoda/go-smb2 提供客户端，不能承担服务端角色。最终由仓库持有服务端引擎，只选择有明确来源与许可的签名派生代码。[Sombrero 源码](https://github.com/mike76-dev/siasmb/blob/34430ba85f9683b69df7d723611efaa3d41743a3/server.go)、[许可与第三方声明](https://github.com/mike76-dev/siasmb/blob/34430ba85f9683b69df7d723611efaa3d41743a3/LICENSE)、[CloudSoda 项目声明](https://github.com/CloudSoda/go-smb2/blob/0b399b9d036cfa042bdd8c618b8abc85ee5ee3f0/README.md)。

**只提供可读 share，或同时扩张 LAN SMB／NTFS／透明恢复。** 只读范围不能完成既定写入与文件语义；扩大部署、ACL、持久句柄和恢复则引入不同的安全与会话保证。当前实现维持多个 Export 的独立生命周期，但不把网络管理 API、全局映射或完整 NTFS 纳入公共能力。

## 后果

Windows 使用系统自带客户端，业务能够在同一 Go 进程掌握本机服务、映射、授权和远端连接。代价是维护 SMB 的协商、签名、异步请求、取消、枚举、通知及错误转换；协议版本标签或一条成功连接不能证明这些行为符合要求。

独立 package 不消除跨层成本。共享保护需要所有入口参与共同 claims／ranges 和最终发布；本机平台解释承担一致目录投影与版本竞争。事件时祖先和 metadata 增加每次修改的 CPU、日志及网络字节，资源上限会拒绝不能完整保存的事实。HTTP control 与本机 lease／计划预算分别 retention，不能只计算 ordinary response。

直接 HTTP 查询避免让 Windows 依赖 Linux SQLite replica，但元数据往返承担远端 RTT。未来的 replica 优化须保留健康门控和与 apply 同序的通知，不能让缓存命中代替权威可用性。零权利 lease 已经历真实 Windows 的一秒负查询验收并失败；该运行未进入专用目录阶段。它不修改 backend API，也不构成 cache grant／break 方案。同步确认、签名和不授予缓存权限也有吞吐成本；性能改进不放宽错误、身份或确认语义。UAC 的映射可见性属于按用户部署约束，helper 不通过全局映射或弱化系统安全绕开它。

### 原生可见性验收失败

[030e07a 的 CI](https://github.com/codetreker/remote-fs/actions/runs/34982701023)中两个 Linux 作业通过；服务端重建后的真实认证与不同 SID 策略拒绝通过。`TestNativeWindowsHTTPBridge` 已观察到 live.bin 的有效 V2 RqLs／State=NONE，但在负查询后一秒可见性断言失败。后续专用目录与断线阶段未执行；四条 CREATE 观察及记录边界见[测试策略](../../../../docs/testing.md#windows-11-arm64-原生入口)。

这条原生链路使用有界内存 authority，不证明 Windows SQLite 持久实现。独立 SSPI 认证与取消已有真实 Windows 执行证据；这些通过项与本次可见性失败分别成立，完整 Windows 接入仍未通过验收。

完整接入保留以下验收义务；现有测试的具体对应关系由[测试策略](../../../../docs/testing.md)维护，未执行或尚未覆盖的场景不能记作已通过：

1. **本机接入与身份。** 在真实 Windows 11 24H2+ client edition 中保持系统 445 服务运行，使用自定义回环端口、签名和非 guest 认证。验证普通／提升登录会话的映射可见性、资源管理器访问及其它本机用户拒绝，记录 edition、build 和实际身份交换。
2. **确认时点与断线。** 暂停 Win32 WriteFile、SetEndOfFile、FlushFileBuffers 对应的远端确认，观察应用不能提前成功，确认前后的字节、大小与 EOF 一致。断线读取已有内容、属性、negative lookup 和目录时报告真实错误；UseWriteThrough 或 share flags 本身不能代替这些观察。
3. **跨客户端与通知。** 两个 Windows 客户端以及 Windows＋Linux FUSE＋HTTP 验证持续打开后的写入、增长、缩短在一秒内可见。分别核对文件／空目录删除过滤、递归目录改名／删除、跨观察范围移动、覆盖目标和延迟消费时的历史身份。首次监视暂停 checkpoint 时的 >B 事件、连续 notify 调用间的事件、改名前后整组交付均不得丢失，也不依赖定时重扫。
4. **对象、目录与名字。** 通过 FileIdInfo 观察重复打开、改名、跨客户端和重连后新打开的稳定对象身份；替换或删除后重建必须产生新身份，旧有效引用继续报告原对象。查找与最终操作之间插入替换，覆盖父目录改名、相对操作和枚举竞争。验证大小写冲突、非法 UTF-8、并发祖先移动和目录版本条件；其它入口名字不受 Windows 约束，相关投影失败且纯身份访问保持。链接 confinement 与非法信息类拒绝仍须验证。
5. **Windows 访问限制。** 覆盖六种 disposition、metadata-only、目录引用、双向 ShareAccess、delete-on-close/pending、共享／排他范围、等待／取消及三种锁族的独立关系。从 HTTP 和 FUSE 尝试绕过既有 Windows 限制。批量加锁的冲突回滚、批量解锁后项失败、非法后项保留先前授予分别核对实际状态；“解锁 A 后在未持有 C 上失败”仍须证明 A 已解锁。
6. **故障与关闭。** 覆盖远端已执行但响应丢失、重复 MessageId/action、取消赢／授予赢／无法核对、gateway/server 重启、期限到达、busy 卸载、映射创建失败和清理超时。facts 缺失／损坏、祖先超限、帧能力不足、序列化边界、历史缺口与队列溢出各保留正确错误；不能将损坏当作普通重扫，不能在生成 facts 失败后留下已提交修改。所有者关闭后无无限任务、重复 mutation 或假成功。
7. **嵌入与凭据生命周期。** 业务程序仅通过公共 API 注入 backend、认证、授权、logger 与 listener；New 无后台或系统映射副作用。两个 share 的 Publish／Unpublish、busy／超时／失败相互隔离。持有文件与锁时轮换本机 provider 或远端凭据，区分旧会话身份、新交换、刷新失败和实时撤销；Windows 路径不依赖 Unix nativelease，Linux 调用方不强制导入 SMB。
8. **协议与资源。** 畸形长度、偏移、compound、credits、UTF-16、范围、签名与重放输入需要错误及 fuzz 检查；慢客户端、open／锁／notify／枚举预算耗尽后仍保持有界。CANCEL 不发独立响应，目标只有一个终态。停止后的 goroutine、socket、系统映射与远端引用清理分别核对，未支持能力不被宣告。
9. **覆盖率与性能。** 正常与可用平台的 race 执行保留错误路径，覆盖率只归属自身 package，禁止 `-coverpkg`，阈值不变；Windows ARM64 使用普通 native build，不能声称执行了该平台不支持的 race。CI 拒绝 skip，并检查映射残留。冷挂载、源码树列目录、小文件 I/O 与远端 RTT 的真实成本仍需记录，不能以关闭同步确认或降低一致性换取吞吐。

### 大请求的保证单位仍未决定

[R-CON-5【未决】](../../../../docs/spec/requirements.md) 同时适用于 Linux 和 Windows：应用调用超过 SMB MaxReadSize／MaxWriteSize 或 FUSE 请求上限时，会被拆成多个请求。单个协议／storage 请求的捕获、提交与确认不能证明整次应用调用只观察一个修订，也不自动赋予失败的大写入全量回滚。[SMB READ](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/ff304074-293b-4106-a5ea-c19c35ca736a)与[WRITE](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/49dce94d-71fd-4fdf-b730-a60d6b27fbba)的请求边界需要与实际系统调用对照。

这里没有选择应用调用、协议请求或存储操作作为该保证单位，也没有增加跨片段 snapshot／transaction。仍需用超过两种请求上限的读写，在片段之间插入并发覆盖和提交失败，关联实际完成字节、应用错误、读取修订与权威提交。这个契约决定仍未完成，真实 Windows 可见性验收已经失败，因此完整 Windows 符合性尚未确认；既有单次操作义务不因此缩减。若原生行为不能满足已定保证，保留失败证据并重新讨论实现，不静默改成最终一致或断线可读。
