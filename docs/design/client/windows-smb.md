# Windows 本机 SMB 接入

本页描述 Windows client 将远端 volume 发布为本机网络驱动器的结构，对应 R-FS-9、R-CC-14、R-INT-8、R-INT-14 与既有身份、可见性和错误要求。跨角色接口见[顶层设计](../architecture.md)，权威文件状态见[文件句柄](../server/file-handles.md)。

## 组件与所有权

Windows 应用与资源管理器使用系统 SMB 客户端连接宿主进程内的 loopback SMB endpoint。远端继续提供 storage 接口与 HTTP；它不承担 SMB 服务。Linux FUSE 与 Windows SMB 是独立的呈现入口，选择一条路径不要求加载另一平台的实现。

| 组件 | 职责 |
|---|---|
| [`packages/smb`](../../../packages/smb/config.go) | SMB 3.1.1 协商、认证交换、签名、tree/open 映射、请求准入与有界清理 |
| [`packages/smb/internal/wire`](../../../packages/smb/internal/wire) | 长度、偏移、compound 和命令体的严格解析与编码 |
| [`packages/smb/windows`](../../../packages/smb/windows/mapping.go) | Windows SSPI、已验证 SID 的授权适配、当前用户的盘符映射与移除 |
| `storage.WindowsStorage` | 显式命名启用、权威状态、Windows 会话与保留对象 |
| `Share.Changes` | 宿主注入的 Subscribe、Resume、Checkpoint，提供与 backend 同源的完整变更观察 |

SMB 核心属于仓库的 MIT 实现；签名密钥派生选取的 BSD 代码保留在[许可声明](../../../packages/smb/internal/signing/NOTICE)中。核心依赖 storage、metastore 和 authz 契约，不导入 HTTP transport。宿主可把 HTTP 的订阅、续订与 checkpoint 方法接到 `ChangeSource`，也可提供满足同一契约的其它来源。

`smb.New` 要求显式的 Authenticator、Authorizer 和有效 Limits。构造不打开监听端口，也不创建系统映射。`Server.Serve` 接受宿主交给它的 loopback TCP listener；非 loopback 或非 TCP listener 被拒绝。`Publish` 返回一个 Export；底层 backend 与外部身份配置仍由宿主持有。

## 发布与访问身份

管理方使用 `EnableWindows` 和原动作 ID 显式启用命名能力。结果未知时以 `QueryWindowsActivation` 核对该动作。`Publish` 只验证能力、权威 Enabled 状态、通知来源及预算相容性，不替管理方启用命名规则。share 名称与可信 `Share.Volume` 授权标识分别配置，不能从请求中的 share 名冒充业务 volume 身份。

Windows 的 `NewAuthenticator` 在每次 Begin 时取得独立的 inbound Negotiate credentials，不保存密码。认证交换使用 SSPI 返回的身份与 session key；匿名、guest 或不可用的身份不获得正常 session。原生 SSPI 调用是同步调用，取消会在原生工作返回后阻止身份交付，Close 等待已经进入的原生调用结束。

`CurrentUserSID` 取得当前进程用户的 SID，`AllowSID` 只接受 SMB context 中由 SSPI 验证的同一 SID。宿主显式选择这项策略或自己的 Authorizer，并继续决定访问远端所用的身份。loopback 地址本身不授予另一个本机用户访问权限；elevated 与非 elevated 登录会话也不由 helper 合并。

SMB 协商只接受 3.1.1、SHA-512 preauthentication integrity 与 AES-CMAC signing；正常 session 的签名不能关闭。请求的 volume 操作、完整 Windows open intent 和关闭／续期控制都经过业务授权。远端 HTTP handler 继续按它自己的可信身份与同一语义词汇授权，SMB 本机授权不替代远端授权。

## 保留对象与远端结果

`WindowsSession` 持有有限期、动作历史、保留引用、共享模式与范围锁。`WindowsFile` 可以指向普通文件、目录或仅请求 metadata 的打开；操作以保留身份寻址，rename、unlink 与同名替换不把原引用转向新节点。

CREATE 将展开后的 DesiredAccess、ShareAccess、Disposition、目录种类、DeleteOnClose 与 OpenReparsePoint 一并交给 authority。六种 disposition 的存在性判断、创建／覆盖、属性及返回引用属于同一个结果。父目录以保留引用或节点 ID 加 leaf name 寻址，不能由客户端拼回一个可能已过时的父路径。

READ 返回同一捕获状态的属性与范围字节；WRITE、截断及属性修改等待远端结果，FLUSH 检查已经发布的状态。SMB 请求携带的 write-through 和本机映射的 UseWriteThrough 不等于 `FILE_NO_INTERMEDIATE_BUFFERING`；`FILE_NO_INTERMEDIATE_BUFFERING` 明确以不支持拒绝。

每个有副作用的 Windows 动作使用会话 epoch 与原 nonce。已知 receipt 保留成功、拒绝、取消及 lock batch 的 Applied 数量；有错误不代表此前元素已经回滚。未知结果以原 ID QueryAction／CancelAction 核对，不能换新 ID 重做。无法核对时，受影响的 tree 与引用失败并保留未确认计数，不把它包装为一次成功写入。

共享冲突、范围冲突、delete-pending、未持有范围及非 reparse point 使用独立 WindowsFailure，避免只凭 errno 混淆 SMB status。业务拒绝为 access denied；未知结果与不可达保持 I/O failure，不产生空目录、虚构的不存在或旧内容。应用大读写被拆成多个请求时的保证单位仍由 R-CON-5【未决】约束，单个协议请求的验证不等于整个应用调用的证明。

## 目录观察与符号链接

目录枚举返回 authority 捕获的 WindowsBasicAttr，包括实际的四项时间、DOS attributes 与 DeletePending。FILE_BASIC_INFORMATION 中显式的 ChangeTime=-1 在产生修改前以不支持拒绝，不被解释为自动写入当前时间。完整当前路径只在保留对象的 WindowsNameInfo 中返回；root、linked、detached 是不同状态，空路径不能代替一次失败的路径查询。

CHANGE_NOTIFY 使用变更记录内的不可变 Notification。记录包含 SubjectID、类型、Directory 标志、精确 ChangeMask，以及修改前后的祖先身份、名字与 leaf；不能按通知到达时的树反查历史路径。Directory 标志也区分目录符号链接的名字通知。rename 的旧名与新名属于同一变更组，队列不能只保留其中一半。

Subscribe 从当前已提交 tail 开始；Resume 校验 incarnation、位置和历史连续性，Checkpoint 为目录观察提供明确边界。通知队列按事件数与字节数计费；历史丢失、无法表示或队列不足要求重新枚举，使用 rescan 结果，不能报告没有变化。来源不可达或无法维持健康时，share 的相关访问失败。变更流、checkpoint 与 backend 必须属于同一 authority。

GET/SET_REPARSE_POINT 仅处理符号链接。SetLink 在 authority 内把空、独占引用的文件或目录转换为链接并保留节点 ID；目录链接保留 DOS directory 属性。目标采用受限 volume 路径语法，长度至多 4096 字节，父目录相对跳转由 authority 校验 confinement。普通打开遇到链接保留 authoritative target、位置与未解析后缀，不能在本机猜测它的目标。

Linux FUSE 可观察这个节点的真实链接类型，并在类型转换后保持同一节点的稳定 inode。Linux Readlink 和 Symlinker 入口仍返回 `EOPNOTSUPP`；这项类型一致性不等于 Linux 已能创建或解析完整符号链接路径。

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

文件会话使用 `storage.DefaultFileSessionOptions` 的独立上限与有限历史，见[文件句柄](../server/file-handles.md#一身份与会话)。请求输入在解码与分配前检查长度、credit、compound 和 context 数量；目录结果与通知有各自的总量边界。提高某项上限仍须通过 Limits 的关系校验。

authority 的单项 Notification 最多 256 层祖先，祖先名字预算 64 KiB，canonical 编码预算 256 KiB。同一 Change 的当前／来源名字、content key 和 canonical notification 的原始变长字段之和限制为 512 KiB；全局 Record 与 decoder 都执行该资源界限，它不以 Windows 命名策略是否启用为条件，也不是 HTTP 或 SMB 编码后的 frame 大小。

Windows 完整 volume-relative 路径同时受 `WindowsMaxNameInfoBytes=65792` 原始字节和 `WindowsMaxPathUTF16Units=32767` UTF-16 code units 限制，分隔符也计入。命名启用扫描与启用后的写入、目录改名在提交前校验这些边界，目录移动还须覆盖后代的完整路径。链接 target 与未解析 suffix 各至多 4096 字节。字段在发布前验证，不先提交再发现无法观察。

HTTP Windows 数据与控制入口分别为 `/v3/windows` 和 `/v3/windows-control`。client 与 server 的 Windows control 池各自独立，复用现有 lock-control concurrency／waiter 设置和 MaxInFlightResponseBytes byte 设置；它们在 ordinary response 池之外计费，不是共享一个全局总额。请求保持 16 KiB 控制 body 上限，response 另按完整动作 receipt、属性路径、symlink target／suffix、activation 与错误诊断的最坏 JSON 编码预留四倍空间。过小的 MaxBodyBytes 在能力检查、启用和会话建立时拒绝；公式与资源归属见[server 内存边界](../server/architecture.md#六请求与响应的内存边界)。

## 当前用户的盘符映射

`windows.Map(ctx, MappingOptions{LocalPath, Share, TCPPort})` 仅连接 `\\127.0.0.1\share`。helper 要求 Windows 11 24H2 或以上，接受单个盘符、有效 share 名与非零 TCP port；不提权、不占用已有盘符、不修改机器策略，也不接收密码。

创建使用系统明确支持的 typed 参数：指定 TcpPort、TCP transport、UseWriteThrough、RequireIntegrity，并关闭 Persistent、GlobalMapping 和 SaveCredentials。参数缺失或类型不相容会失败。固定 PowerShell 程序通过 stdin 接收 JSON 值，share 名不拼入命令语法。

创建参数被接受与 OS 身份被观察是两件事。`MappingStatus.ParametersAccepted` 表示创建成功接受了请求参数；另记录实际观察到的 LocalPath、RemotePath、DOS-device target 和 connection status。Windows 的 documented mapping query 不提供可核验的端口、策略标志或 generation，因此 Status 不声称查询过这些事实，也不进行一次新的 OS 查询。

Mapping 记录 SID、AuthenticationID 和 SessionID。Unmount 再次核对登录身份、盘符、UNC 与 DOS-device target，只移除自己创建并仍能识别的映射。宿主必须独占管理这个盘符，禁止在 Mapping 存活时由外部替换它，包括替换为 OS 无法区分的相同映射。这里没有跨进程或全局盘符注册表。

创建结果未知返回 `ErrMappingUncertain`，不取得删除某个观察结果的权限。创建已确认但后续核验或回滚失败时，Map 可以同时返回非 nil Mapping 和 error；调用方保留该对象继续清理，不能只丢弃错误。回滚有独立的 30 秒预算，并遵守同样的 ownership 检查。

## 停止与能力边界

普通 Unmount 遇到打开文件返回 `ErrMappingBusy`，保留可用映射；ForceUnmount 是显式破坏性断开，后续 I/O 可以失败，不宣称无损关闭或远端修改已经确认。移除映射不关闭 Export 或 Server。

Export.Unpublish 在仍有 opens 或活跃请求时返回 `ErrBusy`。宿主按映射、export、server 及外部 backend 的各自 ownership 收尾；Server.Shutdown 停止接纳并清理自己拥有的连接、tree 与会话，失败通过返回值和有限 Status 计数暴露。renew 使用会话 lease 的三分之一作为节奏；授权或续期失败关闭受影响的 tree。没有依赖 import 副作用建立的 listener、信号处理或系统映射。

核心不授予 SMB leases/oplocks、durable handles、跨连接恢复旧引用或 encryption 能力。未实现的 create context、信息类别和控制请求明确拒绝，不能凭静默忽略宣称完整 NTFS 兼容；alternate data streams、Windows ACL 管理与共享 mmap 一致性仍在需求非目标内。

实现的协议检查、真实 authority 集成与 Windows 原生验收分别见[测试策略](../../testing.md)。源码或非 Windows 上的测试通过不能替代 Windows 11 ARM64 上真实 SSPI、系统映射与应用文件操作的执行结果。
