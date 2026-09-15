# Agent Note: Windows 本机 SMB 接入 package

Status: implemented

## 问题

Windows 上未经修改的程序需要访问远端 volume，集成方需要把这项能力嵌入自己的 Go 程序。安装额外文件系统驱动增加了部署与分发成本；复制到普通本地目录再上传无法保持逐次远端确认、同一文件身份和断线错误行为。

目标系统为 Windows 11 24H2 及更新版本。业务程序拥有远端连接、访问身份和进程生命周期，系统中的其它用户不能仅凭同机连接取得访问权限。Windows Server 不属于增加的验收平台。[需求](../../../../docs/spec/requirements.md)中的 R-FS-9、R-CC-14、R-INT-8 与 R-INT-14 定义这项能力；文件身份、同步确认、可见性和错误保证继续适用。

## 决定

### 自有协议引擎与宿主所有权

[`packages/smb`](../../../../packages/smb/config.go) 在宿主进程内提供 loopback SMB 3.1.1 服务，Windows 系统客户端通过自定义 TCP 端口访问 share。远端继续提供 storage 与 HTTP；本机协议不增加远端 SMB 暴露，也不要求额外文件系统驱动。

协议、调度、签名与结果转换由仓库自己的 Go 实现持有。核心采用仓库的 MIT 许可；SP800-108 签名密钥派生所采用的 BSD 代码保留[来源与许可声明](../../../../packages/smb/internal/signing/NOTICE)。这项选择使原生文件动作、授权和异步取消能够使用同一套明确语义，也意味着仓库承担协议实现与维护责任。

```text
Windows 应用 → 系统 SMB 客户端 → loopback SMB
                                  |
                             packages/smb
                                  |
                 WindowsStorage + 宿主注入的 ChangeSource
                                  |
                      HTTP／SSE → 远端 authority
```

SMB 核心依赖 storage、metastore 和 authz，不导入 HTTP、SQLite、FUSE 或 CLI。`Share.Backend` 与 `Share.Changes` 分别提供权威操作和同源的 Subscribe／Resume／Checkpoint。现有集成使用 HTTP 与 SSE，宿主也可以提供满足相同契约的来源；backend 和变更流必须指向同一 authority。把这份关系留在宿主配置中，避免协议引擎拥有另一套远端连接或目录副本。

`New` 校验显式配置；`Serve` 接受宿主交付的 loopback TCP listener。`Publish` 检查 backend 的 Windows 能力、已启用状态及观察预算，并拥有该 Export 的观察器。映射、Export、Server 与外部 backend 分别关闭：普通 `Unpublish` 在仍有打开对象或活跃请求时返回 busy，清理失败保留停止中的所有权；`Shutdown` 停止接纳并等待自己拥有的资源。调用方拥有的 backend 不随某个 share 停止而关闭。

系统客户端的保留 `IPC$` 连接由独立控制树表示，不能把它配置成一个业务 volume Export。它只接受已认证、通过签名校验的 session，计入同一 MaxTrees 预算并遵守 session/tree 清理。pipe-share 响应不代表实现了管道对象；控制树没有 volume backend、WindowsSession、通知或发现接口，不制造一个 volume 身份来请求业务授权。它正常处理 TREE_DISCONNECT，session 的 ECHO／LOGOFF 保持原义；未支持的 pipe、RPC 与控制命令明确失败。已发布 volume 的授权和后端访问保持原有路径。

### 本机身份与显式映射

[`packages/smb/windows`](../../../../packages/smb/windows/auth.go) 使用真实 SSPI Negotiate 交换取得 token 身份和 session key，每次 Begin 持有独立的 native credentials/context。匿名、guest、无可用签名密钥的交换被拒绝。连接允许一次严格限定的 SMB1 帧形状 multi-protocol NEGOTIATE 前导：必须包含 `SMB 2.???`，wildcard `0x02ff` 响应之后仍须进入真正的 SMB2 格式协商，期间不建立认证会话或接纳文件操作。正式 dialect 只接受 SMB 3.1.1、SHA-512 preauthentication integrity 与 AES-CMAC signing，hash 从正式协商开始，正常会话要求签名。这个入口只处理系统客户端的协商前导，不提供 SMB1 文件操作或旧版 Windows 支持；重复前导被拒绝。同步 SSPI 调用返回后仍检查取消，Close 与已进入的原生调用串行收尾，不把取消解释成原生工作已经停止。

本机 Authenticator 与 Authorizer 是必填配置。`CurrentUserSID` 和 `AllowSID` 提供明确的挂载者 SID 策略；业务也可以注入自己的策略。可信 `Share.Volume` 由宿主配置，对已发布 volume 的访问在本机身份放入请求 context 后逐次授权，WindowsOpenIntent 保留数据、metadata、delete 与共享意图。远端 HTTP 身份由业务 transport 管理，两端授权各自执行；令牌和签名密钥不进入日志。凭据轮换不能替换既有会话的已验证身份，重新认证必须保持同一 SID。

`Map` 只在显式调用时改变当前用户的系统映射。helper 检查 Windows client edition、build 和必要的 typed 参数，将 `TcpPort`、TCP transport、`UseWriteThrough`、`RequireIntegrity` 与关闭持久／全局／凭据保存的选项交给 `New-SmbMapping`。程序固定，值通过 JSON stdin 传入，不拼入 PowerShell 语法；它不请求提权，也不修改注册表、安全策略或系统 445 服务。系统拒绝权限或不支持的参数时，错误保留给宿主。[Microsoft 的端口说明](https://learn.microsoft.com/en-us/windows-server/storage/file-server/smb-ports)与[映射参数](https://learn.microsoft.com/en-us/powershell/module/smbshare/new-smbmapping?view=windowsserver2025-ps)定义系统能力，实际 loopback 认证仍须原生验收。

`MappingStatus` 区分参数已被接受与实际观察到的盘符、UNC、连接状态、DOS-device target。provider 可以暴露端口或策略属性；helper 不依赖或验证这些 provider 属性，不假定跨 provider 一致的查询保证，也不依赖 provider-specific generation。Mapping 保存 SID、AuthenticationID、SessionID，并在移除前核对实际映射。宿主必须独占管理选定盘符，因为同一登录中的外部替换若恢复为相同 tuple，系统观察无法证明其代际。创建结果未知不取得删除权；已确认创建但核验或回滚失败可以返回 Mapping 与 error，调用方保留该对象继续清理。

普通 `Unmount` 使用非强制移除，有打开文件时保留映射并返回 busy。`ForceUnmount` 是明确的破坏性断开，不保证在途 I/O 成功或远端结果已确认。二者都不关闭 Export／Server。[WNetCancelConnection2W](https://learn.microsoft.com/en-us/windows/win32/api/winnetwk/nf-winnetwk-wnetcancelconnection2w)的按登录映射与 force 行为，是这些所有权限制的依据。

### 同一权威中的访问、身份与结果

[`WindowsStorage`、`WindowsSession`、`WindowsFile`](../../../../packages/storage/windows_contract.go) 表达保留文件／目录、metadata-only 打开、六种 disposition、share/access、delete-pending、范围批次和原动作核对。父目录身份与 leaf、ExpectedID 和保留引用进入最终操作，避免客户端先查路径再修改时命中替代对象。稳定 node ID 与一次 SMB open 的 FileId 分开；改名、unlink 或同名替换不转移旧引用的身份。

共享检查是双向的，Windows 范围限制与 I/O 在原生 authority 排序。SQLite、objectstore、localstore、locked、limited 及 HTTP 传递同一语义，已有 Linux／HTTP 路径和普通文件入口也遵守生效中的 Windows 约束。Windows 强制范围锁、POSIX/flock advisory 和显式 S/X 各保留自己的规则；本机锁表无法约束另一台机器，故不作为共享状态的权威。

动作使用原 epoch-and-nonce 身份和有限历史。批量加锁冲突可撤回此前授予，批量解锁或某些后项错误可保留此前效果；receipt 的 Applied 数量和原错误一起保留。取消、重复请求或丢失响应通过原动作 QueryAction／CancelAction 核对，无法确定时返回 I/O failure 并 fence 受影响访问，不换新 ID 重做。SMB compound 按序执行，但不承诺将已完成子项回滚成未发生。[SMB 加锁](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/670c7eda-e683-4923-9477-414303959613)、[解锁](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/79eb3c91-563b-4d48-a51c-0974f9d144f8)与[CANCEL](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/57bae3d3-5dd7-4a5f-92cb-fc52e2087dad)的区别不能压缩成统一成功／失败布尔值。

READ 使用一次权威捕获的属性与范围字节；WRITE、截断和属性修改等待远端结果，关闭不是首次提交时机。会话续期与后台清理有自己的有限生命周期；远端重启或期限失效不按旧路径重开、不重取锁后复活旧引用。typed WindowsFailure 与符号 errno 保留 sharing violation、range conflict、delete-pending 等区别，未知或不可达不变成空目录、缺失文件或旧内容。具体状态机与资源归属见[文件句柄](../../../../docs/design/server/file-handles.md)和[Windows 接入设计](../../../../docs/design/client/windows-smb.md)。

### 命名、符号链接与不可变通知事实

Windows 命名能力由管理方逐个 volume 显式启用，Publish 只检查状态。原生发布门内验证既有名字并提交 policy 1；固定 Unicode 15.0 simple uppercase 比较、保留设备名与表示限制随后约束所有入口。不兼容名字导致拒绝，不自动改名或隐藏；停止 share 不撤销 volume policy。Windows 时间、DOS attributes 与符号链接目标持久保存在节点上，普通 volume 的字节名字规则不因本机启动 SMB 自动变化。

完整名字同时受 65792 字节和 32767 UTF-16 code units 限制；目录移动也验证后代在新位置的完整路径。SetLink 在最终权威操作中把空、独占引用的文件或目录转换为符号链接，保持 ID 和目录链接标志，目标至多 4096 字节且不能逃出 volume。既有 ResourceRef 与有效 S/X grant 仍绑定原节点；新的按路径 Resolve 继续只接纳普通文件，不能把类型转换解释成旧 grant 已失效。Linux FUSE 报告真实类型并保持 inode，Readlink／Symlinker 仍为 `EOPNOTSUPP`。这项类型一致性不扩张 Linux 链接创建或解析能力。

[`metastore.Notification`](../../../../packages/metastore/notification.go) 与原 changes 行一起提交：SubjectID、SubjectKind、Directory、精确 ChangeMask 和修改前后祖先链都是事件发生时的事实。删除仍保留 `Removed.Node=nil` 的复制含义；通知中的独立身份、类型与 Directory 标志足以过滤文件、目录和目录链接的名字变化。改名前后属于同一记录，覆盖目标的删除保留为自己的记录；无名对象的后续修改不制造旧路径事件。发送端不查询当前树来补历史路径。

这份事实复用日志 Position、previous-position 连续链、incarnation、保留窗口与 SSE，不增加第二份需要协调提交和裁剪的日志。全局 Record 和 decoder 对 Change 的名字、来源名字、content key 与 canonical notification 原始变长字段总量限制为 512 KiB；通知另有 256 层祖先、64 KiB 名字和 256 KiB canonical 编码上限。长度进入生产前检查、result reservation、完整性检查与传输预算；不能先提交必需事实无法传递的修改。

Export 建立订阅并追到固定 checkpoint 后才可健康服务。首次 CHANGE_NOTIFY 先暂存观察者，再取得 checkpoint B；保留 >B 的事件，后续同一 open 沿用原过滤器、递归范围与队列。按事件时的祖先身份生成相对路径，改名前后作为整组计入有界队列。历史缺口、无法表示或容量不足要求重新枚举；损坏或不可达报告真实故障。连续性恢复不能抹去原观察者已经失去历史的结果。通知是变更提示，不是 Windows 内核缓存一致性的证明。[CHANGE_NOTIFY](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/05869c32-39f0-4726-afc9-671b76ae5ca7)与[FILE_NOTIFY_INFORMATION](https://learn.microsoft.com/en-us/windows/win32/api/winnt/ns-winnt-file_notify_information)规定其协议表示。

### 有界资源与缓存边界

宿主显式选择 `DefaultLimits` 或完整有效的 Limits。连接、会话、tree/open、请求、compound、create context、frame/I/O、枚举、通知和各阶段期限都有边界；文件会话另有有限历史与存续期。HTTP 数据与 Windows control 使用独立 admission，后者按完整属性路径、symlink 观察和错误 receipt 的最坏 JSON 编码计费。各池的 byte 设置不是一个合并的进程总额，宿主必须计入并存池的额外 retention；公式归[server 内存边界](../../../../docs/design/server/architecture.md#六请求与响应的内存边界)所有。

映射请求 UseWriteThrough，SMB 不授予缓存 lease/oplock，并声明禁止离线缓存；不提供 durable/persistent handles、multichannel、跨连接恢复或 encryption。已知的可选 oplock、`RqLs`、`DHnQ` 与 `DH2Q` 请求不必使普通打开失败：authority 允许打开后，响应保持 oplock NONE 且不附带 lease／durable 授予 context。未知 create context 仍被拒绝。`FILE_DISALLOW_EXCLUSIVE` 按 SMB2 规则忽略，实际共享与访问检查保持不变；这不放宽 `FILE_NO_INTERMEDIATE_BUFFERING` 的不支持结果。UseWriteThrough 的 forced-unit-access 语义与 `FILE_NO_INTERMEDIATE_BUFFERING` 不同，前者不能证明关闭了所有属性、negative 或目录缓存。系统全局缓存设置保持宿主环境原状；真实程序的同步确认、可见性与断线读取仍须验收。[SMB 客户端缓存设置](https://learn.microsoft.com/en-us/powershell/module/smbshare/set-smbclientconfiguration?view=windowsserver2025-ps)不能作为库私自改变机器策略的理由。

## 备选方案

**远端直接提供 SMB。** 减少本机协议转换，但改变远端暴露和业务认证接入方式，不能直接复用现有 HTTP 网络路径。本机 adapter 保留独立远端 transport；公共 WindowsStorage 仍可供其它入口使用。

**WinFsp／Dokany 挂载。** 更接近 Windows 文件系统回调，但增加驱动安装、分发和生命周期。完整跨客户端范围锁还必须证明锁参数与取消／晚到授予之间的原子关系，不能只增加回调便声称兼容。这些成本不符合无需额外驱动的部署目标。

**本机目录索引或 metadata replica。** 一致快照加日志能够维护类型和祖先关系，但带来初始化、缺口重建和空间预算。权威提交已经持有旧／新状态，直接记录通知事实可以保留 HTTP 读取路径。本机 replica 仍可单独评估；它必须从读取副本的实际 apply 顺序产生通知，并与 Linux nativelease 依赖分离，不能用空实现伪造 Windows 支持。

**权威端另开按目录通知订阅。** 能在服务端过滤相对路径，但引入监视注册、作用域、游标与恢复协议。独立通知日志另有双日志提交和同步裁剪义务。复用完整 changes 行获得所需身份和顺序，没有增加第二份权威历史。

**Samba VFS 或 SMBLibrary helper。** Samba 的成熟 VFS 面向 Unix/Linux 部署，Windows 需要额外运行环境。SMBLibrary 带来 C# runtime/helper 与 IPC，锁等待／取消仍需扩展。它们未成为独立 Go package 的引擎。[Samba](https://www.samba.org/samba/what_is_samba.html)、[SMBLibrary 文件接口](https://github.com/TalAloni/SMBLibrary/blob/2edbcf3161084b51dbe11b8a960b0bc7c224b851/SMBLibrary/NTFileStore/INTFileStore.cs)。

**直接嵌入现成 Go SMB 服务端。** 调查的 `macos-fuse-t/go-smb2` 在所检查版本中无条件接受 LOCK、不支持 CANCEL、没有真实通知源，认证接口也不能从外部 package 实现；它的 AGPL／商业许可还需要单独选择。购买许可不能补足这些语义缺口。[锁、取消与通知](https://github.com/macos-fuse-t/go-smb2/blob/c0e6b139796e67ce8da148aa5942c55a7dff2fe8/server/file_tree.go)、[认证接口](https://github.com/macos-fuse-t/go-smb2/blob/c0e6b139796e67ce8da148aa5942c55a7dff2fe8/server/authenticator.go)。

**从其它实现提取协议引擎。** Sombrero 有 MIT 的服务端代码，但绑定 Sia/PostgreSQL，局部锁和通知也不能直接成为本系统的权威实现。CloudSoda/go-smb2 提供客户端，不能承担服务端角色。最终由仓库持有服务端引擎，只选择有明确来源与许可的签名派生代码。[Sombrero 源码](https://github.com/mike76-dev/siasmb/blob/34430ba85f9683b69df7d723611efaa3d41743a3/server.go)、[许可与第三方声明](https://github.com/mike76-dev/siasmb/blob/34430ba85f9683b69df7d723611efaa3d41743a3/LICENSE)、[CloudSoda 项目声明](https://github.com/CloudSoda/go-smb2/blob/0b399b9d036cfa042bdd8c618b8abc85ee5ee3f0/README.md)。

**只提供可读 share，或同时扩张 LAN SMB／NTFS／透明恢复。** 只读范围不能完成既定写入与文件语义；扩大部署、ACL、持久句柄和恢复则引入不同的安全与会话保证。当前实现维持多个 Export 的独立生命周期，但不把网络管理 API、全局映射或完整 NTFS 纳入公共能力。

## 后果

Windows 使用系统自带客户端，业务能够在同一 Go 进程掌握本机服务、映射、授权和远端连接。代价是维护 SMB 的协商、签名、异步请求、取消、枚举、通知及错误转换；协议版本标签或一条成功连接不能证明这些行为符合要求。

独立 package 不消除跨层成本。共享访问与锁必须进入 metastore、存储包装层、HTTP 和所有既有访问路径；命名 policy 持久限制已启用 volume，卸载不恢复宽松名字规则。事件保存祖先事实增加每次修改的 CPU、日志和网络字节，完整路径及事件边界会拒绝无法表示的修改。HTTP control 独立池占用额外 retention，不能只计算 ordinary response 的上限。

直接 HTTP 查询避免让 Windows 依赖 Linux SQLite replica，但元数据往返承担远端 RTT。未来的 replica 优化须保留健康门控和与 apply 同序的通知，不能让缓存命中代替权威可用性。同步确认、签名和不授予缓存权限也有吞吐成本；性能改进不放宽错误、身份或确认语义。UAC 的映射可见性属于按用户部署约束，helper 不通过全局映射或弱化系统安全绕开它。

### 原生验证仍待完成

Windows 11 ARM64 CI 已配置普通 Go build 下的真实 SSPI、系统 Map 和 Win32 文件操作测试，并检查实际 client edition、build 与架构。`TestNativeWindowsHTTPBridge` 使用真实 HTTP／SMB／SSPI 链路和有界内存 authority，观察自定义端口、签名 WRITE、暂停 HTTP 确认时的 WriteFile 行为，以及持续打开、negative lookup、目录变化、断线和 busy Unmount；它不证明 Windows SQLite 持久实现。独立 SSPI 认证与取消用例已在真实 Windows 11 Enterprise build 26200 上通过；完整 Map 与原生接入验收仍未完成，Linux 测试与交叉编译也不提供这些结论。

完整接入保留以下验收义务；现有测试的具体对应关系由[测试策略](../../../../docs/testing.md)维护，未执行或尚未覆盖的场景不能记作已通过：

1. **本机接入与身份。** 在真实 Windows 11 24H2+ client edition 中保持系统 445 服务运行，使用自定义回环端口、签名和非 guest 认证。验证普通／提升登录会话的映射可见性、资源管理器访问及其它本机用户拒绝，记录 edition、build 和实际身份交换。
2. **确认时点与断线。** 暂停 Win32 WriteFile、SetEndOfFile、FlushFileBuffers 对应的远端确认，观察应用不能提前成功，确认前后的字节、大小与 EOF 一致。断线读取已有内容、属性、negative lookup 和目录时报告真实错误；UseWriteThrough 或 share flags 本身不能代替这些观察。
3. **跨客户端与通知。** 两个 Windows 客户端以及 Windows＋Linux FUSE＋HTTP 验证持续打开后的写入、增长、缩短在一秒内可见。分别核对文件／空目录删除过滤、递归目录改名／删除、跨观察范围移动、覆盖目标和延迟消费时的历史身份。首次监视暂停 checkpoint 时的 >B 事件、连续 notify 调用间的事件、改名前后整组交付均不得丢失，也不依赖定时重扫。
4. **对象、目录与名字。** 通过 FileIdInfo 观察重复打开、改名、跨客户端和重连后新打开的稳定对象身份；替换或删除后重建必须产生新身份，旧有效引用继续报告原对象。查找与最终操作之间插入替换，覆盖父目录改名、相对操作和枚举竞争。验证大小写冲突、非法 UTF-8、启用与并发命名操作的顺序、取消／未知核对和卸载后 policy 保持；同时验证链接 confinement 与非法信息类拒绝。
5. **Windows 访问限制。** 覆盖六种 disposition、metadata-only、目录引用、双向 ShareAccess、delete-on-close/pending、共享／排他范围、等待／取消及三种锁族的独立关系。从 HTTP 和 FUSE 尝试绕过既有 Windows 限制。批量加锁的冲突回滚、批量解锁后项失败、非法后项保留先前授予分别核对实际状态；“解锁 A 后在未持有 C 上失败”仍须证明 A 已解锁。
6. **故障与关闭。** 覆盖远端已执行但响应丢失、重复 MessageId/action、取消赢／授予赢／无法核对、gateway/server 重启、期限到达、busy 卸载、映射创建失败和清理超时。facts 缺失／损坏、祖先超限、帧能力不足、序列化边界、历史缺口与队列溢出各保留正确错误；不能将损坏当作普通重扫，不能在生成 facts 失败后留下已提交修改。所有者关闭后无无限任务、重复 mutation 或假成功。
7. **嵌入与凭据生命周期。** 业务程序仅通过公共 API 注入 backend、认证、授权、logger 与 listener；New 无后台或系统映射副作用。两个 share 的 Publish／Unpublish、busy／超时／失败相互隔离。持有文件与锁时轮换本机 provider 或远端凭据，区分旧会话身份、新交换、刷新失败和实时撤销；Windows 路径不依赖 Unix nativelease，Linux 调用方不强制导入 SMB。
8. **协议与资源。** 畸形长度、偏移、compound、credits、UTF-16、范围、签名与重放输入需要错误及 fuzz 检查；慢客户端、open／锁／notify／枚举预算耗尽后仍保持有界。CANCEL 不发独立响应，目标只有一个终态。停止后的 goroutine、socket、系统映射与远端引用清理分别核对，未支持能力不被宣告。
9. **覆盖率与性能。** 正常与可用平台的 race 执行保留错误路径，覆盖率只归属自身 package，禁止 `-coverpkg`，阈值不变；Windows ARM64 使用普通 native build，不能声称执行了该平台不支持的 race。CI 拒绝 skip，并检查映射残留。冷挂载、源码树列目录、小文件 I/O 与远端 RTT 的真实成本仍需记录，不能以关闭同步确认或降低一致性换取吞吐。

### 大请求的保证单位仍未决定

[R-CON-5【未决】](../../../../docs/spec/requirements.md) 同时适用于 Linux 和 Windows：应用调用超过 SMB MaxReadSize／MaxWriteSize 或 FUSE 请求上限时，会被拆成多个请求。单个协议／storage 请求的捕获、提交与确认不能证明整次应用调用只观察一个修订，也不自动赋予失败的大写入全量回滚。[SMB READ](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/ff304074-293b-4106-a5ea-c19c35ca736a)与[WRITE](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/49dce94d-71fd-4fdf-b730-a60d6b27fbba)的请求边界需要与实际系统调用对照。

这里没有选择应用调用、协议请求或存储操作作为该保证单位，也没有增加跨片段 snapshot／transaction。仍需用超过两种请求上限的读写，在片段之间插入并发覆盖和提交失败，关联实际完成字节、应用错误、读取修订与权威提交。这个契约决定和真实 Windows 验收尚未完成，因此完整 Windows 符合性尚未确认；既有单次操作义务不因此缩减。若原生行为不能满足已定保证，保留失败证据并重新讨论实现，不静默改成最终一致或断线可读。
