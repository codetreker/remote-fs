# Agent Note: Windows 系统网络驱动器支持

Status: proposed

## 问题

Windows 11 上未经修改的应用和文件管理器需要访问同一份远端 volume，并且看见与 Linux 挂载、SDK 相同的对象、已提交内容与失败事实。安装第三方文件系统驱动增加分发、升级与清理责任；同步到普通本地目录使应用成功返回和远端确认分离，也不能维持对象身份、跨入口保护或断线时的真实错误。

Windows 系统客户端会缓存文件内容、属性、目录和“不存在”。一个只把 SMB 请求翻译成远端读写的服务，即使每个独立响应正确，仍可能让已经连接的程序看到过期结果。Windows 名字、共享模式、范围锁和关闭时删除有平台特有的解释；远端 volume 仍须保持平台中立，且其它入口不能绕过同一对象上的权威保护。

## 提案

宿主在自己的 Windows 进程中运行只监听 loopback 的 SMB 3.1.1 端点，把已授权的一个 volume 发布给当前登录会话的系统网络驱动器。普通程序使用 Windows 自带的 SMB redirector，无须驱动安装或管理员权限。目标客户端为 Windows 11 24H2 及以上 Home、Pro；Windows Server 和更早版本不在目标内。完整用户契约由 [`docs/spec/requirements.md`](../../../../docs/spec/requirements.md) 的 R-FS-5 至 R-FS-9、R-CON、R-CC-14、R-INT-1、R-INT-8、R-WIN、R-ERR、R-WS-4 与 R-WS-5 定义。

### 系统边界与部署拓扑

```text
Windows 应用／Explorer ── Windows 文件 API ── SMB redirector
                                                   │ loopback SMB 3.1.1
                                                   ▼
宿主进程 ┌── WNet 映射拥有者 ─────── packages/smb Server ────────────┐
         │   当前登录会话            SSPI／签名／Windows codec          │
         │                          session → tree → FileId           │
         │                                    │                         │
         │              Share{可信 volume, storage.FileStorage}       │
         └────────────────────────────────────┼─────────────────────────┘
                                              │ 中立存储契约
                       ┌──────────────────────┴───────────────────────┐
                       │ 直接 adapter          或 HTTP client adapter │
                       └──────────────────────┬───────────────────────┘
                                              ▼
                              远端 authority／volume 与其它入口
```

宿主选择可信 volume 身份、构造 backend、远端 HTTP client 与凭据、认证和授权策略、listener，并拥有 WNet 映射及整个进程生命周期；`packages/smb` 在 `Serve` 后拥有 listener、SMB 会话、tree、FileId 和清理责任，不能替宿主关闭 `Share.Backend`。`Share.Backend` 是 `storage.FileStorage`，HTTP adapter 只是可选实现。使用 HTTP 时文件命令经过 SMB → FileStorage → HTTP client → 远端 Handler → authority；本机 `Handler.Close` 处理 HTTP 服务停机的会话排空，SMB `CLOSE` 只退休一份文件引用。现有 [HTTP 关闭失败的 CI](https://github.com/codetreker/remote-fs/actions/runs/35974116040)暴露 `Handler.Close` 竞态：首次清理的 `EIO` 可能被第二次清理覆盖成成功。使用 HTTP backing 的生产资格和服务停机验收以独立修复及失败回归通过为前置条件。

中立契约只承载对象身份、名字观察、用途／共享拒绝集合、范围、metadata namespace、动作结果和固定错误。Windows comparer、disposition、NTSTATUS、本机 SID 或 WNet 身份都停留在本机 codec／宿主；authority 根据宿主提供的远端业务身份逐操作授权。本机 SID 加登录会话 LUID 只准入 loopback SMB 会话，不构成远端业务身份。

已交付的 endpoint 结构详见[客户端 SMB 设计](../../../../docs/design/client/smb-endpoint.md)。基础决定由 [安全且有界的 SMB 端点](../../implemented/architecture/2026-09-21-secure-bounded-smb-endpoint.md)、[中立 metadata 与访问控制](../../implemented/architecture/2026-09-16-neutral-metadata-and-access-controls.md)、[持久对象身份与原子文件动作](../../implemented/architecture/2026-09-20-durable-identity-and-atomic-file-operations.md)、[权威名字观察](../../implemented/architecture/2026-09-20-bounded-authoritative-name-observations.md)、[权威子项选择](../../implemented/architecture/2026-09-22-guard-authoritative-child-selection.md)、[可恢复关闭](../../implemented/architecture/2026-09-23-recoverable-close-ownership.md)和[虚拟分配账](../../implemented/architecture/2026-09-23-virtual-allocation-ledger.md)拥有。本文只说明它们如何组合成完整 Windows 入口；不把已经存在的 endpoint 内部实现重写一遍。当前文件、目录、名字命令仍明确不支持，WNet 映射和 Windows 11 redirector 的完整验收尚未交付。

### 运行时状态与所有权

| 状态 | 唯一拥有者与键 | 必须保持的事实 |
|---|---|---|
| Published export | 宿主发布的可信 volume；`Server` 持有发布与 stopping 状态 | 一份 share 只指向一个 volume；取消发布先封住新 tree，busy 失败不改变现有映射与引用。 |
| SMB session | 已经 SSPI 认证的 SID、登录 LUID、签名会话和不可复用 SessionId | 每个请求复核绑定；身份失效封住新工作，已经接纳的清理仍可继续。 |
| Tree | SMB session + export + TreeId | 一个 tree 只列出自己的 FileId；同一 session 对同一 export 的多个 tree 共用一份 authority FileSession。 |
| Authority FileSession | SMB session + export | 持有有限 lease、epoch、action history、引用与 advisory owners；renew 只延长已确认的同一 incarnation。 |
| FileId | tree + 不可复用的本地打开实例 | 恰好绑定一份带类型的 `File` 或 `NodeReference`、对象 ID、用途／share、原打开动作与清理 owner；绝不由路径或 TCP connection 重新绑定。 |
| Authority action | 原逻辑动作 ID + session／volume scope | 仅在有效 action epoch 与有限 receipt window 内可查询／同 ID 重投；过期仍可能未知，超时不生成第二个动作。 |
| Delete intent | authority 持久义务 + 宿主 per-volume `DeleteIntentOwner` | 接受时锁定原对象和可随 rename 移动的同一名字关联、授权与 owner；发起句柄关闭／连接、会话或宿主终止使 `armed` 进入 `pending`，立即拦截冲突新打开；最后相关句柄结束才移除名字。终态与失败可恢复并最终 ACK。 |
| ChangeSource cursor／缓存安全状态 | 宿主绑定到同一 volume／incarnation 的中立变更源；每个 export 的订阅与可报告对象／名字视图 | 缺口、失联或 overflow 立即使相关缓存答案不可信；树及 active detached 引用的完整重取和事件衔接后才恢复成功报告。 |

volume serial 从可信且稳定的 volume 身份导出；Windows 128-bit file ID 从不可复用的 authority 对象身份导出，命名空间分配持久化并检查碰撞。它们与 SMB 每次打开的 FileId 分属不同层次。同一对象改名或重新打开保持标识；同名替换获得新标识。打开结果的 `Attr.ID` 必须和所获引用的稳定身份一致。现有 `ReferenceIdentity` 是可选接口，`AtomicFileOpener`／`NodeReferences` 不保证返回引用实现它；7.3 须增加覆盖整个 FileSession、HTTP 与 wrappers 的预检能力，承诺两种返回引用都提供不可变 `ReferenceNodeID`，在对象效果前验证完整包装链。预检违约后若仍返回无身份引用，不能宣布成功，并须保留已获引用供清理。

所有操作遵守同一生命期顺序：封住新准入 → 排空已接纳操作 → 确认 authority 的释放／barrier 事实 → 撤销本地 owner 与额度。`ReferenceCloseResult.Released=true` 只证明引用已释放；该中立结果没有 `BarrierPending` 字段。现有 `File.CloseWithResult(ctx)`／`NodeReference.CloseWithResult(ctx)` 不接受动作 ID；7.3 提议中立 `CloseWithAction(ctx, FileActionID)`，由 endpoint 在效果前生成 actionID、连同引用身份与不可变 close 意图写入 cleanup owner，再作为参数传给 native、wrappers、HTTP。返回中立 `CloseSettlement{Released, BarrierState, ActionID}`；`BarrierState` 区分无义务、待结算、已结算和未知，HTTP 特有的 pending error 在 adapter 边界转换。相同引用／意图／actionID 的重投返回原释放和 barrier 结果；同一 ID 改变引用或意图明确拒绝。首次响应丢失时 endpoint 已持有原 ID，只能查询或按原 ID 重投，不能从响应字段倒推动作身份。若释放同时仍有待结算或未知 barrier，保留原动作与 cleanup owner，直到 barrier 明确结算；不能只凭 Released 回收整份责任。释放未知或为 false 时保留引用 owner。tree disconnect、LOGOFF、TCP 断开、unpublish 和 server shutdown 均复用这条退休路径。普通句柄与 advisory 锁不跨 authority incarnation 透明恢复；一旦连续性丧失，旧 FileId 持续失败，不能重新绑定同名对象。

### 准入、身份与能力检查

发布前验证配置的 endpoint 安全能力、资源额度和 storage 包装链；宿主把可信 `Share.Volume` 与 backend 实际指向的远端 volume 绑定。`httprest.Dial` 不发网络请求，当前 `Publish` 只检查 `CheckFileStorage`，而现有 `NewFileSession`／`FileSessionStatus` 与 HTTP 响应不携带 volume 身份，所以当前代码不能证明远端目标。提案增加中立 `BackendIdentity` 预检：backend 返回不可变 volume ID、authority incarnation 和由可信宿主配置／远端认证机制验证的绑定证明；直接 adapter、HTTP client、server 和 wrappers 均传递与验证同一事实，不把调用方传入的 label 回显当证明。第一次 `TREE_CONNECT` 在返回成功前建立 authority FileSession，验证该证明与 `Share.Volume` 完全一致、session incarnation、必需能力和当前操作授权；失败撤销未宣布的 tree，并继续清理已建立但未宣布的 session。后续能力随着交付段启用，不能以 `CheckFileStorage` 一项代替：7.3 要求完整名字观察、原子打开、引用身份、动作回执、分配量与携带调用方 actionID 的可恢复 close；7.4–8.3 分别要求条件内容／metadata 修改、完整目录快照、名字 guard、范围控制及绑定同 volume/incarnation 的 `ChangeSource`。包装 adapter 任一层不能提供已声明能力时拒绝受影响的 share／tree，而不是在产生对象效果后才尝试补救。

本机连接须由 SSPI 得到 SID 与登录 LUID；匿名、guest、同 SID 另一 LUID 均拒绝。每个 SMB 请求须完成签名／完整性校验、SessionId 与 TreeId 绑定、身份未失效检查、trusted volume 与操作授权，再进入 FileStorage。重新认证仅可在同一 SID／LUID 与既定 session 规则下延续本地准入；远端权限撤销影响新动作，不能抹去已接受动作的结算和清理责任。身份过期与签名失效分别映射为准确的协议失败，不允许降级到 guest、unsigned 或路径级匿名访问。HTTP adapter 独立承载远端凭据和业务主体；不从本机 SID 生成远端身份。

概念性的请求上下文包含 `connectionIncarnation, SessionId, TreeId, MessageId, command, principalRef, trustedVolume, deadline, actionID`。其中 `principalRef` 指向受保护的认证对象，不将 SID、token、密钥放入普通日志。每次新 mutation 在准入前分配稳定 actionID；同一逻辑动作的传输重试和结果核对保留该 ID，SessionId 或 TCP 重连不充当幂等键。身份与授权检查对每次请求重新执行；已经被 authority 接纳的固定后续效果按其原授权和持久责任结算。

### 原生发布、映射和 tree 建立

1. 宿主为每个 drive 构造 `PublishedDrive(exportID, trustedVolumeID, backendRef, SID+LUID owner, WNet target, mappingState)`，预检 loopback listener、目标平台和容量；`Serve` 后由宿主显式发布 share。发布不会自行创建系统映射。
2. 当前登录会话使用 WNet 建立系统连接。WNet 目标须指向同一 loopback endpoint 与唯一 share；连接和移除结果连同映射身份可查询。`TREE_CONNECT` 在签名 session 下选择 export，核对可信 volume／远端绑定和权限，创建或复用该 SMB session 对该 export 的一份 FileSession，成功后才安装 tree。
3. 非强制 unmap 是原子准入门：先以不持有 WNet callback 所需锁的原子状态转换，对指定映射设临时 admission fence，**在同一时点快照**该映射已打开句柄、锁与 active／in-flight 操作数（不计正在执行的 unmap 控制请求本身）；fence 只拒绝新打开／tree 准入，仍接受 TREE_DISCONNECT、LOGOFF、既有 I/O 排空和已接纳清理。快照中任何句柄、锁或在途操作非零就立即报告 busy 并撤回临时 fence，即使该操作随后完成且未留下句柄；原映射与 owner 完整可用。只有快照为空才进入 WNet 移除；无 busy 时在 fence 持有期间调用非强制 WNet 移除，防止检查后新打开；返回码本身不总能证明映射是否已变，未确认结果前不退休原句柄。随后核对原映射的 WNet 身份与 endpoint owner，结果分三类：(a) 明确无效果且原映射身份未变，撤回临时 fence，保留句柄／owner，返回移除失败；(b) 映射已确认移除，进入永久 `stopping`，移除操作如实报告 `MappingRemoved=true`，后续资源退休由独立 `Stop`／`Status` 报告 `CleanupPending` 和 owner；(c) 映射身份仍无法确认，保留 fence、映射 owner 与 `Unknown` 状态，不报告普通失败或成功，按同一移除意图继续查询。未知状态后若确认原映射仍在，才允许回滚 fence；确认已移除则走 (b)。`Stop` 仅在映射与所有清理责任均完成时报告完全成功，清理失败保留 `stopping` 与可查询 owner。停止永久封住新连接与操作，排空已接纳动作，退休 tree／session、锁和 delete owner，确认 WNet 映射及 listener 清理。每个失败点注入验证 busy 回滚、不可逆阶段重试及 owner 计费；同一 cleanup 可重试。外部提供的 backend 和 HTTP client 不由 SMB 关闭。

原生可行性门先在 Windows 11 24H2+ Home、Pro 各用普通用户探测 IPv4／IPv6 loopback、可用 host alias、445 端口占用、WNet 目标、SSPI、签名、`TREE_CONNECT` 和 teardown；记录本端点接纳的 TCP 445 socket、SSPI context 中的 SID 与 `AuthenticationId` LUID、宿主交互式登录 LUID 以及签名 TREE_CONNECT，不能把内置 LanmanServer 的响应误认成本端点。若 redirector 使用 network logon 造成 LUID 不同，须设计并原生证明安全的同一登录会话关联，不能简单拒绝真实用户或放宽到仅 SID。现有 endpoint 尚不支持 root 文件命令，映射因后续 root 请求失败时应根据端点 trace 区分已验证的连接／认证／tree 与未实现文件命令。不得用管理员权限、驱动或整机 SMB／防火墙／缓存／安全策略变化使其通过。[Microsoft 的替代 SMB 端口配置](https://learn.microsoft.com/en-us/windows-server/storage/file-server/smb-ports)要求提升权限，而 [`WNetAddConnection2W`](https://learn.microsoft.com/en-us/windows/win32/api/winnetwk/nf-winnetwk-wnetaddconnection2w)无端口参数；普通用户可达性须在目标版本证明。完整映射在 root 命令具备后复验。TCP 445、本机身份关联和每类缓存的原生可行性均是发布阻塞门；任一失败，先确定满足 R-WIN-1／R-WIN-8／R-WIN-9 的机制再继续相应交付。

### Windows 名字解码与权威选择

SMB codec 在 frame 长度和偶数字节检查后按 UTF-16 精确解码；NUL、孤立 surrogate、超过 255 个 UTF-16 code unit 的组件，以及空名、`.`、`..`、路径分隔符、`< > : " / \ | ? *`、U+0000–001F、末尾空格／点和 Windows 设备名都在任何 authority 效果前拒绝。设备名含 `CON`、`PRN`、`AUX`、`NUL`、`COM1–9`、`LPT1–9` 及数字上标 ¹²³ 形式，即使后接扩展名仍拒绝；ADS、设备路径与 NT 前缀不转成普通 volume 名字。有效 supplementary pair 往返转换为 UTF-8；不进行 NFC 或其它 Unicode normalization，原始 authority 名字拼写保持不变。规则与边界须以 [Windows 命名规则](https://learn.microsoft.com/en-us/windows/win32/fileio/naming-a-file)和原生 redirector 测试核对。

大小写等价使用 [Windows `CompareStringOrdinal`](https://learn.microsoft.com/en-us/windows/win32/api/stringapiset/nf-stringapiset-comparestringordinal) 的显式长度、`ignoreCase=TRUE`，不使用 Go `EqualFold`、当前 locale 或自选 normalization。对父目录的一次完整有界观察，先验证所有 raw leaf 都是合法 UTF-8 且可表示，再用该 comparer 检查所有 sibling；0 个匹配为不存在，1 个选择其精确 raw leaf，多个匹配或任何无法表示 sibling 使受影响的父目录按名观察整体失败。结果携带根到父每一级 revision／raw edge guard，在最终 authority 动作重验；并发插入 `Foo`／`foo` 必须导致 guard 冲突或被正确排序，不能选错对象。原生向量覆盖 `A/a`、`CON.txt`、尾点／尾空格、`a:b`、emoji surrogate、非法 UTF-8 authority leaf、组合与预组 `é`、255／256 UTF-16 单元、sigma／Turkish I，并把 comparator 结果与 redirector 实际行为交叉验证。[Unicode normalization](https://learn.microsoft.com/en-us/windows/win32/intl/using-unicode-normalization-to-represent-strings)只作为不擅自规范化的边界参考。

完整 `DirectoryMetadataObserver` 捕获后才能判断 sibling 歧义，使 7.3 的冷路径按所经目录的 child 数付出有界排序／比较成本；单点 lookup 不能证明不存在大小写冲突。7.3 以正确性优先，记录不同目录规模与深度的冷打开时延及资源消耗。后续若需要优化，只允许 revision 绑定的权威 Windows 名字索引，或由完整观察建立且随变更失效的验证缓存；任一优化必须证明冲突名字不会被漏掉，R-WS-4 的速度不能靠不完整查找取得。

### 权威打开、FileId 与结果恢复

SMB 名字逐组件解码，按 R-FS-9 在完整父目录观察上检查不可表示名字与大小写歧义。路径选择生成 `NamespaceGuards{RootID, 每级目录 revision, 精确 raw-name edge}`；末级带目标存在／身份条件。现有中立 guard 上限为 256 条、64 KiB；超限在效果前明确失败。打开向最终 authority 动作传入父引用、原始叶名字、完整 guards、disposition、用途／share、初始 metadata 与 actionID。authority 在同一个排序点重新核对整条祖先链和末级条件，并决定存在性、创建或截断、双向共享准入、引用授予及初始属性。不能把早先的客户端查找当作最终 guard，也不能先 open 再另行 truncate 或补共享 claim。

| SMB 打开意图 | 最终权威动作 |
|---|---|
| create | 目标不存在才创建；已存在明确冲突。 |
| open | 目标存在且类型匹配才保留引用。 |
| open-if | 已存在则打开；不存在则创建；并发创建者结果由最终事务决定。 |
| overwrite | 已存在文件在同一动作内截断；不存在失败。 |
| overwrite-if | 已存在则截断，不存在则创建；两种结果均返回准确的 create action。 |
| supersede | 8.1 通过受完整 guards 约束的 `OpenAtOptions.Existing=ReplaceNode`（或同等原子替换并打开动作）直接返回 `OpenResult{File, Attr, Outcome}`；旧对象由旧引用持有，不能先 `NameCommand` 替换后另开。 |

metadata-only 与目录打开取得 `NodeReference`，普通数据文件取得 `File`；一个 FileId 只拥有其中一种。所有实际对象效果之前预留 FileId slot、frame／结果预算和待清理 owner；容量耗尽时 authority 看不到创建或截断。FileId 由 session incarnation 与单调计数构成，计数溢出时拒绝新打开，不能复用仍可能出现在请求中的标识。`OpenAt`／`OpenChildRef` 即使伴随错误返回非 nil 引用，也须交给预留的 owner 清理，不能因错误分支而丢弃。动作接受后无论响应编码、网络或本地注册是否失败，该 session/tree 都保留引用及原 actionID，核对原动作并清理；不能通过新路径打开猜测原结果。SMB endpoint 作为 FileStorage 调用方生成随机 actionID，保存不可变的原请求负载；只在 FileSession 的有限 history 和有效 action epoch 内由 endpoint 内部查询／同 ID、同负载重投。session 退休、history 过期或 authority incarnation 改变后的 `Unknown`／`Retired` 不证明未执行，不能承诺 `QueryFileAction` 一定给出终态，也不能用新 action 或路径重试；本次 SMB 调用返回未知结果对应的 I/O 错误。需要新增按 opaque actionID／ownerID 查询的宿主诊断接口或事件，保留有界的 unresolved owner 状态与清理进度；现有 `Server.Status` 的聚合计数不能回答单个动作。owner 已退休后该状态只表明无法确认结果，不承诺跨进程的普通动作回放或精确完成事实。R-FS-8 的动作查询调用方在此架构中是 SMB endpoint：它在有效回执窗口内内部查询或安全重投同一个逻辑动作。未经修改的 Windows 应用不持有 FileActionID，也不提供逐动作查询 API；不能确认时本次调用返回明确的未知结果／I/O 错误。宿主的有界 per-operation ledger 仅供诊断和验收核对，不冒充应用查询能力。应用遇到 I/O 错误须重新观察当前权威对象，但观察结果不能冒充原动作回执；只有持久 delete intent 有跨重启查询契约。权威结果中的对象身份与引用身份、allocation-known／allocation bytes、属性预算必须在对 SMB 宣告成功前核对。异常结果不把已产生效果说成未执行。

SMB CREATE 和 CLOSE 的 `AllocationSize` 报告权威分配**字节数**，不要求 4096 对齐；[MS-SMB2 CREATE response](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/d166aa9e-0b53-410e-b35e-3933d8131927)使用字节语义。分配量能力要在效果前预检；若 adapter 违约返回未知或非法值，结果为错误且保留已获引用。可选且未授予的 `AlSi` preallocation context 可被忽略，但响应不得声称预分配；改变基础打开语义却未支持的 context／flag 在对象效果前明确失败。Windows 信息类中依赖 cluster 几何的转换在 7.4 确定接口和报告规则，并以有效 allocation byte 及可证明的 volume geometry 验证；不能把内置 volume 的 4096 粒度强加给第三方 backend。

### FileId 绑定的 I/O、信息与关闭

READ、WRITE、FLUSH、EOF、文件／volume 信息与属性操作从已签名 header 定位 FileId，依次核对 tree、引用类型、访问权、session incarnation、当前授权及 authority 能力／状态，再由其对象身份调用中立 FileStorage；捕获结果必须先通过完整 response budget 才可编码。READ 使用身份稳定的 `ReadAt`，数据与 Attr 来自同一内容 revision；按请求的 `MinimumCount`、捕获 EOF 与短读规则响应。未支持的 RDMA channel 在后端访问前明确拒绝；分块请求不宣称整个应用调用的一次全局快照。WRITE、EOF 截断和可设置属性通过 `FileMutation` 条件动作提交：把已观察 Windows 属性版本放入 `ExpectedMetadata`，把同一版本的 `ARCHIVE` 更新放入 `Metadata`，在最终 authority 效果点核对 `READONLY` 并同时提交内容／长度与属性。若元数据 CAS 冲突，只有确认原动作 `NotExecuted` 后才能重读并使用新 action；`Unknown` 只查询／重投原 action。overwrite／overwrite-if 的 `ResetContent` 路径同样用 `OpenAtOptions.Target.ExpectedMetadata` 在最终打开动作检查 `READONLY`，并用 `Initial.OnReset.Metadata` 与截断同次设置 `ARCHIVE`，即使原长度已经为零；若创建新对象则使用 `OnCreate` 的 Windows 初始属性。远端确认前不向 Windows 返回成功，失败不能推迟到 CLOSE。FLUSH 以 `Sync` 确认引用健康与后端 durability barrier；CLOSE 不承担延后写回。改名、unlink、同名替换后已有 FileId 仍访问原身份，当前名字查询从身份观察得到最新关联或准确的无名状态。

Windows codec 只解释 `READONLY`、`HIDDEN`、`SYSTEM`、`ARCHIVE` 四个可设置位；目录、reparse、normal 从节点种类及位状态推导。`READONLY` 阻止该入口发起的不相容写与删除，但其它入口不解释 Windows 专有位。缺失的历史 creation／change 时间稳定显示零，查询不推测、不写回；显式零时间表示保持原值。空间的总量／已用／可用取自权威 `Space` 与分配账，不能用本地缓存或零占位。所有响应按 SMB credits、frame 和信息类长度做有界编码；结果超预算明确失败，不截断完整语义。

CLOSE 对 FileId 封住新操作，等待已接纳 I/O，再以已预存于 owner 的 actionID 调 `CloseWithAction`；网络与编码失败只查询／重投同一引用、同一意图的原动作。已确认释放但仍有同步／删除 barrier 错误时去掉文件引用、保留中立剩余义务 owner；释放事实未知时 FileId 进入 cleanup-only，不把其额度回收。CLOSE 的 postquery 属性获取若失败而引用释放已确认，可按协议清除 postquery 标志并报告真实关闭结果，不能为了属性查询失败倒退已确认的释放事实。重复 CLOSE、tree 退出、LOGOFF、断线、unpublish 和 stop 对同一清理责任只推进状态，不重复创建动作。range lock 和共享 claim 在其引用完成排空并得到 authority 释放事实后才离开权威排序。旧句柄在 authority epoch 更换或 lease 失效后返回失效错误；不重开同名目标，也不在未知 close 结果时偷偷释放保护。

### 目录、名字修改与删除义务

目录路径遍历使用 `DirectoryMetadataObserver` 的完整权威 metadata 观察来建立 guards；应用可见 QUERY_DIRECTORY 使用 `DirectoryReader.ReadDirNodeBounded` 的 `DirectoryObservation` 与结果 collector 获取一次完整、有界的目录捕获，两者职责不同。对捕获的**全部**条目验证 UTF-8、Windows 名字表示、case-fold 后唯一性、对象 ID 和编码预算，任何条目失败则整次枚举失败，不能漏掉坏条目后返回其余内容。一个目录 FileId 的枚举 cursor 分页同一次冻结捕获；`RESTART_SCANS` 与 `REOPEN` 丢弃旧 cursor、重新捕获并从头返回，完整新捕获验证失败则不输出条目；`RETURN_SINGLE_ENTRY` 最多返回一个完整条目并只推进已返回条目。`INDEX_SPECIFIED` 只接受该 FileId 当前冻结捕获已发出的 FileIndex，并定位该捕获内相应 cursor；旧捕获或其它句柄的 index 明确失败，绝不把它当路径或跨捕获续游标。输出 buffer 太小而装不下下一完整条目时保持 cursor，返回协议规定的 buffer 错误；达到 cap 时以完整条目边界分页，不截断名字或 metadata。无法完成的快照、超额目录、revision 冲突或不可确认状态均为 I/O／资源错误，不返回空目录。按名操作在其它入口制造的歧义存在时整体拒绝；已经打开的身份 I/O 不受其原名字是否可表示影响。

rename、move、replace、unlink、supersede 和普通 disposition 必须在最终 authority 事务中验证源与目标各自的完整祖先 guards、原始名字、对象／名字关联和共享／readonly 约束。`ChildCondition.ExpectedMetadata` 与 `PendingUnlinkCommand.ExpectedMetadata` 带入 Windows 属性版本，在最终名字效果点阻止并发设置的 `READONLY` 被旧客户端观察绕过。目前中立 `NameCommand` 有源／目标条件但没有 `NamespaceGuards`；8.1 对普通 rename／replace／unlink 扩展中立受 guard 的名字动作或提供等价的最终事务核对，supersede 则扩展 guarded `OpenAt(ReplaceNode)`；不能以客户端先查再调用 `MutateName` 或替换后另开填补。名字动作含稳定 actionID 和有限期可查询 receipt；响应丢失后不以后来复用源名或目标名的对象推断或重放新动作。rename 事件保留一对旧／新名字关联；同名 replacement 仍为两个对象身份。

关闭时删除与普通 disposition 是两种状态。delete-on-close 打开在任何创建／截断效果前要求宿主提供按 volume 隔离、跨进程重启可恢复的 `DeleteIntentOwner`。authority 在接受打开时以 `CloseIntent.ExpectedMetadata` 检查当时的 `READONLY` 属性版本与删除授权，并持久绑定原对象、当时的名字关联和义务 owner；后续触发是该已接纳动作的固定效果，无须在执行时重新取得已撤销的授权。普通 disposition 在设置时立刻核对当前身份、共享、readonly 和目录为空，并按其自身动作 receipt 恢复；清除它不撤销其它句柄已经接受的关闭删除义务。

发起句柄显式关闭，或其连接、会话、宿主进程终止时，义务从 `armed` 进入 `pending`，立即拒绝冲突新打开／名字动作；已经允许删除共享的旧句柄继续按其权限访问。实际移除等待最后相关句柄结束，只尝试移除与原对象保持同一绑定、可随 rename 移动的关联。关联在触发前已由 unlink／replacement 分离，或目录触发时非空，结果为明确未执行／相应失败；后来占据同名的对象绝不能被删。启动、停止和异常恢复用宿主 owner 分页 `ListDeleteIntents`，按 ID 查询并完成原清理，达成完成或明确未执行终态且本地责任处理完才 ACK。查询、补充清理和 ACK 作为新请求仍受当前授权约束；授权失败或远端不可达时保留 owner、资源占用和状态，不静默遗忘。authority 崩溃重启后已接受义务仍可由同一 owner 找到。

### 共享、范围锁、取消与请求排序

打开声明的 `Uses`、`Deny` 在 authority 最终打开点双向比较；其它入口的打开及名字 mutation 必须经过同一排序边界，不能以协议入口不同绕过。已有持有者禁止新请求，新持有者也禁止已有用途时，冲突在创建、截断、replace、rename、delete 等效果前被拒绝。已接纳 I/O 在关闭或新保护请求到达时可按既定顺序完成；不能先释放 claim 再等待它。Windows 不应将本地 READONLY 或 SID 规则伪装成跨入口业务授权。

字节范围 LOCK／UNLOCK 翻译为中立 `RangeControl` 的 `DomainEnforced` 请求，绑定原文件引用、区间和 lock request ID。共享锁的 `DenyOthers=WriteData`，排他锁的 `DenyOthers=ReadData|WriteData`；UNLOCK 按原 `ClaimID` 做 `RemoveExact`，不从坐标猜测或释放别人的 claim。等待锁、立即失败、批次部分成功、取消、释放与响应丢失均按 authority 的 `RangeAttempt`、`FailedAt`、`Effects` 与 surviving release 语义报告；失败不能把已确认成功的解除重新加锁，也不能保留一个未授予的锁。SMB CANCEL 要能找到已登记的 pending request 并取消其等待，而不能因为当前连接先处理 CANCEL 就跳过原请求的权威状态核对。取消与授予交错时查询原动作确定是否已授予；未知结果保持 owner 直至确定或过期回收。普通 advisory range protection 不跨 authority incarnation 恢复，旧持有者持续失败。独立的强 S/X 保护沿用 R-CC-6 至 R-CC-11 的期限与恢复契约。

### 变更源、Windows 缓存与故障恢复

`storage.FileStorage` 当前没有变更订阅；[HTTP `Storage.Subscribe`](../../../../packages/transport/httprest/subscribe.go) 是具体 adapter 的方法，[metastore `Log`](../../../../packages/metastore/metastore.go) 是远端名字变更日志，不能假设任意 `Share.Backend` 都有同一流。8.3 提出平台中立的 `ChangeSource` 可选能力，或由宿主显式提供的等价依赖：与 `Share.Backend` 的 `BackendIdentity` 绑定同一可信 volume、authority incarnation 和授权身份，暴露订阅、可验证 checkpoint、按页读取的原子 scoped snapshot、按 incarnation+position 恢复及缺口／overflow 错误。HTTP adapter 可把现有订阅包装进该能力；直接 adapter 要独立满足同样契约。缺少变更源的 share 在 8.3 的可见性能力预检时拒绝，不退化为 TTL、固定周期目录扫描或看似健康的无通知模式。

协调器状态为 `Seeding → Ready → Fenced → Recovering`。建基线先订阅同一 incarnation 并记住游标 P；再取得**同一权威时刻**的只读 snapshot 与它原子绑定的 checkpoint S，分页读取保守的“已向 redirector 成功报告、因此它可能仍持有”的内容、文件信息、正／负查找、目录事实所涉及的目录／对象 ID（即使 endpoint 本地无缓存或 SMB lease），以及授予 lease、仍有未完成 `CHANGE_NOTIFY` watch 的作用域和 active detached 引用。`WATCH_TREE` 的作用域含被监视目录的整棵子树，变更事件须以对象父链判定当前属于该子树，并在跨子树 rename 后更新归属；watch 即使从未建立目录 listing 或 lease，也占独立 scope 与队列预算。已报告事实的集合是客户端潜在缓存的有界保守上界；服务端无法直接检查 redirector 私有缓存，不能因为本地条目被驱逐就从恢复 scope 删除。容量满时在新的可缓存成功答复前拒绝或 fence，只有经原生证明已使对应 redirector 事实失效才回收条目；正常大树若因此无法继续服务，8.3 机制不通过资格门。每页有字节／条目预算，整个 snapshot 由短期一致性 token 或等价快照机制固定，不把全 volume 装入内存；没有缓存／lease 的冷查找直接回源 authority。选择固定 tail T，回放 S 之后至 T 的全部变更，再追上 live stream，确认同一 incarnation、无缺口且每个受保护对象的 revision 不回退后进入 Ready。订阅先于 snapshot，保证 S 之后变更在订阅 retention 中；若 backend 不能原子取得 snapshot+checkpoint，就改用 snapshot **之前**的游标 P 回放 P 之后的全部变更，并证明分页基线与重放合成不会漏掉早扫描目录的并发修改，不能在扫描结束后才取 checkpoint 并跳过期间事件。

Fenced 时所有可能由 endpoint 回答的旧视图失败；对断流前已挂起的普通 watch 或 `WATCH_TREE`，若不能完整重建其目录／子树事件范围，回应 `STATUS_NOTIFY_ENUM_DIR` 并要求下一次订阅前重新权威观察；子树过大或变更队列溢出也走此路径，不能悄悄从新尾部续订。Recovering 在有界队列和页预算内重建。snapshot token 过期、retention 已 trim、队列 overflow、scope 增长超额、active watch 队列超额、incarnation 改变或任一组件不可达时丢弃该次基线并重试；持续不能完成时保持 Fenced，状态暴露失败原因。active detached 引用也纳入对象状态捕获与事件回放，不能树基线完成就宣称这些 FileId 已恢复。8.3 的负载门须以远大于内存预算的真实大树、深目录、含仅由 redirector 缓存但 endpoint 本地已驱逐的正／负查找，并在首个分页后修改早扫描目录，证明恢复能前进或明确拒绝，不能因普通大 volume 永久误判为损坏。

现有 [`metastore.Change`](../../../../packages/metastore/metastore.go) 以 `Parent/Name/From/Node` 描述名字，[客户端架构](../../../../docs/design/client/architecture.md)记录无名对象写入可能没有命名事件。仅靠该日志会漏掉已打开、失去最后名字的对象内容修改，违背 R-FS-6 与 R-CON-1。此提案选择扩展中立变更语义，增加按不可复用对象 ID 表达内容、长度、属性 revision 的 mutation 事件，包括 detached 对象；名字事件仍携带原始关联。生产端在确认修改的同一权威发布顺序中产生该事件，订阅端按对象 ID 找到所有 active FileId、仅对实际授予且匹配的 lease key 发 break，并使相应本机对象视图失效。恢复基线从 active 引用直接读取 detached 对象的状态，回放其后对象事件。若底层 adapter 无法产生完整对象事件，只有经真实 Windows 客户端证明 detached FileId 的内容缓存从未拦截权威读取时，才可为该 adapter 选择禁用对应缓存；否则该 adapter 不取得 R-WIN-8 资格。

authority 的 rename 事件保持旧／新关联，但 SMB 对目录 watch 的投影遵循 [FILE_NOTIFY_INFORMATION](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-fscc/634043d7-7b39-47e9-9e26-bda64685e4c9)：同目录改名向同一 watch 依次发 `OLD_NAME`、`NEW_NAME`；跨目录移动分别向源 watch 发 `REMOVED`、目标 watch 发 `ADDED`，不伪造跨 watch 的一个成对 SMB payload。普通 watch 只投影所监视目录的事件，`WATCH_TREE` 按权威父链投影整棵子树内的名字、内容、大小和属性事件；跨子树移动分别按移出与移入计算。内部事件与日志保留同一动作的关联，供 cache invalidation 和诊断。该投影与 R-WIN-3 约定一致：同目录通知成对，跨目录分别通知移除与新增；内部变更仍保留同一动作的旧／新关系。

Windows redirector 的文件内容、文件信息、目录与负查找有不同缓存路径；[lease break](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/4f35576a-6f3b-40f0-a832-1c30b0afccb3)只作用于匹配 lease key，[CHANGE_NOTIFY](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/05869c32-39f0-4726-afc9-671b76ae5ca7)只回应已挂起的请求。服务端有序事件、break 和通知都不能单独证明应用首次读取会发 SMB 请求；返回 EIO 也只覆盖实际到达 endpoint 的请求。因此每一类缓存都须在真实 Home／Pro redirector 上分别预热、由另一入口修改或只切断 authority／变更源，再观察首次应用调用是否在一秒内取得权威结果或明确失败。不得通过修改[系统级 SMB 客户端缓存配置](https://learn.microsoft.com/en-us/powershell/module/smbshare/set-smbclientconfiguration)取得通过。

流静默失联尤其危险：现有 HTTP stream [keepalive](../../../../packages/transport/httprest/limits.go) 和客户端[静默超时](../../../../packages/transport/httprest/client.go)分别为 10 秒、30 秒，不能满足一秒故障与失效窗口。8.3 须定义并测得最坏情况的 `变更提交/失联 → 检测 → fence/lease break → redirector 处理 → 首次应用调用` 总时延；先不授予 lease，在真实客户端测量六类缓存路径；仍有本地命中时再逐类试验服务器主动 break 或等效机制。对内容、信息、目录、正负查找分别测量。健康路径的可见性由事件驱动，失联检测可以有有界健康机制，但不能用固定周期权威数据轮询或缓存到期代替事件。只断开普通 authority 路径或只断开变更流时，未知旧视图均不能作为成功答案。若任一缓存类没有无需全局设置、普通用户可用的失效／拒绝机制，或静默失联窗口无法达标，应先修改架构与相应规范，不得宣告 Windows 支持。

### 故障结果、预算与可观测性

| 故障时点 | 外部结果 | 保留的责任 |
|---|---|---|
| 准入、能力或容量预检失败 | 明确错误；authority 无对象效果 | 预留额度回滚，无引用或 action owner。 |
| authority 已接纳，响应丢失 | 未知结果；同一 action 查询／重投 | 原 session／tree 的引用、动作与清理 owner；回执退休后仅保留有界 Unknown 诊断与尚未释放的清理责任。 |
| authority 失联、校验失败或状态损坏 | I/O 错误，缓存 fail closed | 原已接纳义务和未确认动作；不返回旧字节、空目录或不存在。 |
| 本机身份过期或签名失效 | 已认证会话失效／访问错误 | 已接纳动作仍清理；新操作无准入。 |
| authority epoch 更换或 lease 未确认 | 旧 FileId 持续失效 | 已接受持久 delete intent 继续由 authority／owner 恢复。 |
| cleanup 超时或返回部分释放 | `stopping` 与具体失败可查询 | 未确认资源继续计费；仅确认 `Released` 的引用可回收，pending barrier 继续保留。 |

每个可积累对象设明确的数量或字节上限：export、connection、session、tree、FileId、并发请求、SMB frame／I/O、目录捕获、通知队列、变更重取、action receipt、range lock／waiter、pending delete intent、cleanup owner、日志字段和错误摘要。额度在会产生效果前预留，跨 session 和 volume 的合计也受限；计量反映 active 与 cleanup-only。保留独立的清理与状态查询容量，使某 volume 饱和时其 owner 仍可退休，另一 volume 仍可查询和停止。默认数值和可支持 volume 数须由负载测量确定，不把测试 N 当产品默认值。状态暴露每个 export 的 serving/stopping、authority epoch/fence、change stream 健康、active/pending/unknown owner 数及最近错误类别；不能把 cleanup-only owner 报成已释放。

每个异步边界显式继承可关联标识，同时独立记录其取消来源与清理 owner；不能只依赖原 SMB request 的 context，因为响应、连接或进程可能先结束。

| 边界 | 关联键与 owner | 取消、错误和状态传播 |
|---|---|---|
| SMB request → FileStorage／可选 HTTP → authority receipt | exportID、connection incarnation、SessionId／TreeId／MessageId、FileId generation、opaque actionID、HTTP request ID；endpoint 持有原负载和 action owner | 请求 deadline 可取消等待，不能证明已接纳动作未执行；receipt 以原 actionID 回到 owner，错误保留内部 cause，SMB 只映射经净化的类别。 |
| session renewal | exportID、session incarnation、authority epoch／revision、session ownerID 与 renewal attemptID | renewal 用自身 deadline；失败 fence 新工作，启动独立 bounded cleanup，不能沿用已结束请求的取消状态。 |
| change stream／通知重取 | exportID、stream incarnation、cursor／revision、rebuild attemptID 与 cache fence ownerID | 断流、overflow 或取消先标记不可信；重取失败保留 fence、error category 与 cursor，不向任何请求报告成功旧视图。 |
| pending LOCK／CANCEL | session／FileId generation、lock request ID、ClaimID、async request ID、lock ownerID | 原请求取消只触发 `RangeControl.Cancel`；与授予竞争时 `Query` 原请求，记录仍持有的 claim 和批次 surviving effects。 |
| reference／tree／session cleanup | cleanup ownerID、原 actionID、cleanup attemptID、引用释放和 barrier 状态 | 与客户端断线分离的有界 cleanup context；只在权威确认后减计数，timeout／error category 留在 owner 及 status。 |
| delete-intent recovery | trusted volume、持久 `DeleteIntentOwner`／intent ID、recovery attemptID | 当前授权保护查询／ACK；已接纳效果由 authority 继续；失败保留原义务、终态与下一次恢复位置。 |

结构化事件至少含 `event, export_id, session_incarnation, file_generation, action_id, owner_id, attempt_id, phase, outcome, error_category, owner_state, released, barrier_pending, timestamp`，空字段仅在不适用时省略。字段值是有界 opaque 标识或固定枚举；不得含 SID、LUID、token、签名密钥、原始名字、内容、请求体或未经净化的底层错误文本。内部错误保持 cause 链供程序判断，日志按 `component/operation/phase/error_category` 净化，重复错误以有界计数和少量最近样本汇总。一个上线后的具体排障查询是 `owner_id = "own-7f3a"`：按 timestamp 排列该 owner 的 SMB 准入、authority receipt、cleanup attempt、barrier 与最终状态；若只知道 actionID，先在宿主的有界 per-operation ledger 按 `action_id` 查到 ownerID。该 ledger 至少记录 volume、session／handle generation、opaque actionID、operation、state、expiry 和 ownerID，不存 raw path/content；`Server.Status` 聚合计数不替代它，也不把已过期回执报告成完成。

对调用方返回准确的 NTSTATUS：名字／类型／共享／权限／容量／不支持／失效与一般 I/O 故障分别映射；远端未知、损坏和无法确认时不得映射成不存在或成功。每个映射只在 SMB 层发生，远端继续使用中立错误。测试须覆盖状态码与无副作用的对应关系。

### 分段交付与证明门

以下编号沿用 [Issue #30](https://github.com/codetreker/remote-fs/issues/30)。每个聚焦 PR 同时交付相应实现、`docs/design/`、implemented Agent Note、正常／错误／资源边界测试；后段不能用自己的补丁掩盖前段错误。分段只是依赖顺序，最终 Windows 支持仍须满足完整 R-WIN-2 与 WN 矩阵。

| 段 | 交付结构 | 该段必须证明 |
|---|---|---|
| 7.3 有界 CREATE/CLOSE | 原生发布可行性门；名字 codec 与最终 guarded `OpenAt`／`OpenChildRef`；五种非 supersede 打开；共享准入；FileId 与 close owner；related compound CREATE→CLOSE 的 FileId 传递。 | 最终选择无 check-then-act；容量耗尽无对象效果；响应丢失保留原动作／引用；CLOSE 与断线沿同一预提交 actionID 结算，不会过早释放 claim；compound 只沿成功的同帧 CREATE 传递 all-ones FileId，不改签名覆盖的原始帧。 |
| 7.4 身份 I/O 与信息 | READ、WRITE、FLUSH、EOF、条件 metadata／ARCHIVE，文件与 volume 查询；有界 response/credits。 | 返回内容与属性同 revision；写入远端确认；metadata CAS 冲突在明确未执行后才能新建动作，未知结果只查／重投原动作；allocation 与 cluster geometry 经过信息类门。 |
| 7.5 目录观察 | 完整 `ReadDirNodeBounded` 捕获、全量名字验证、per-FileId 冻结 cursor、restart 重取。 | 无部分枚举、无歧义漏项；目录 revision 和身份一致；预算耗尽明确失败。 |
| 7.6 删除义务 | 打开时接受关闭删除、宿主持久 per-volume owner、CloseIntent、分页恢复和 ACK。 | 接受前 owner 可恢复；原对象／关联不变；崩溃重启后义务可查询并抵达终态。 |
| 8.1 名字修改 | supersede 用 guarded `OpenAt(ReplaceNode)` 原子返回引用；普通 disposition、rename／move／replace／unlink 扩展中立 `NameCommand`，把源／目标完整 ancestry guards 穿过 HTTP、wrappers、metastore 到最终事务。 | 不能只验证末级 raw slot；同名替换不改变旧句柄身份；结果丢失可按原 action 恢复。 |
| 8.2 共享与范围 | 跨入口名字效果的共享保护、LOCK／UNLOCK、等待、批次、异步 pending request 与 CANCEL。 | 权威授予与已接纳 I/O 有序；取消竞态以 `RangeControl.Query/Cancel` 结算，不把 context 取消当作未授予。 |
| 8.3 通知与缓存 | 中立 `ChangeSource` 与对象 ID mutation feed、同目录 rename 成对投影／跨目录双 watch 投影、redirector lease／oplock 或实证等效失效、缺口重取。 | active detached 句柄的对象变化可见；预热的内容／属性／目录／正负查找首次观察在一秒内得到权威事实；断流与 overflow fail closed；无周期数据轮询依赖。 |
| 9 原生映射与 fixture | 当前登录会话 WNet 所有权、停止恢复；持久远端 volume 的 Linux authority、Windows x64／ARM64 客户端与可控故障 fixture。 | 无提权／全局策略变化；非强制 busy 原映射完整；可分别切断 authority 与变更流、控制签名后 frame、提交前后响应、delete 状态崩溃和名字复用。 |
| 10 原生资格 | Home／Pro × x64／ARM64 正式 redirector 矩阵、需求追踪、冷状态性能和资源测量。 | WN-01 至 WN-17 全部 executed/pass、无 skip；R-WS-4 的量化门槛先写入 spec 后再通过，不以单测或模拟客户端替代。 |

SMB related compound 中 CREATE 成功后的 all-ones FileId 是同一签名 frame 内后续命令的占位引用。解析层必须在该 frame 的已验证上下文里把它绑定到前一成功 CREATE 的 FileId；前置 CREATE 失败则相关后继失败，不能从另一 frame 或另一个 tree 取旧 FileId。此状态不改动原始已签名字节，也不替代每个后继操作自己的准入和授权核对。[MS-SMB2](https://winprotocoldocs-bhdugrdyduf5h2e4.b02.azurefd.net/MS-SMB2/%5bMS-SMB2%5d.pdf)的 CREATE、CLOSE 与 related operation 章节拥有协议编码细节。

### 范围切分与开放决策

对象身份、同步确认、真实错误、一秒可见性、跨入口共享／范围保护、持久关闭删除与本机身份完整性属于结构性保证，不能延至最终验收才补。延期能力及其代价如下；“形状约束”限制当前实现，避免以后只能推倒重做。

| 延期项 | 分类与代价 | 当前形状约束／外部结果 |
|---|---|---|
| Windows Server、旧 Windows 与 macOS | 功能；需新增平台分支与原生矩阵 | 平台探测集中；不支持平台明确拒绝发布。 |
| 局域网访问本机 SMB | 功能；需新增暴露、认证、防火墙与部署责任 | listener 和本机主体不固化为远端业务身份；当前只监听 loopback。 |
| ACL 编辑与完整 NTFS metadata | 功能；需权限继承、持久格式和迁移 | opaque metadata 保持 namespace／版本边界；不支持编辑明确失败。 |
| ADS、hard link、reparse、稀疏／压缩／加密 | 功能；需新增对象／流／配额语义 | 节点种类、主内容和平台 metadata 分离；请求明确 unsupported。 |
| durable／persistent handle、multichannel、透明故障转移 | 保证；需重设持久 session 与跨连接 owner | 普通 FileId 不等于连接或可恢复句柄；不授予能力，旧句柄在断线后失败。 |
| 离线写入与自动重放 | 保证；需冲突模型、日志和身份延续 | 断线失败；有效 receipt 窗口内按原动作查询，过期保留 Unknown 诊断；本地缓存不能成为第二 authority。 |
| 跨底层请求的大 I/O 全局快照／回滚 | 保证；需跨请求事务 | 只承诺每个已报告片段的当前契约，不宣称应用调用级原子性。 |
| Windows 本地 metadata replica | 功能；需初始化、恢复、磁盘与缺口模型 | 正确性只依赖权威观察；将来副本不能成为 Windows 专用远端 schema。 |

仍须经实证或接口审查确定的点：普通用户可靠到达 loopback WNet 目标的方式；Windows redirector 对内容、文件信息、目录和正负查找缓存各自接受何种无全局策略的失效动作；第三方 FileStorage 有效分配字节与 Windows cluster 信息类之间的可报告 geometry；`NameCommand` 两侧 ancestry guard 的中立 wire／存储形状；可用资源默认额度及 R-WS-4 冷状态门槛。前两项必须在大规模文件命令和缓存实现之前分别通过原生门；geometry 在 7.4 信息类宣布支持前确定；guard 在 8.1 任何名字 mutation 前落地；额度与性能门槛在 10 宣告支持前经测量写入规范。未证明时只对应能力保持明确不支持，不能以假定的成功状态跨门。

交付 PR 以单一目的为界：7.3–7.6 分开；8 按名字修改、范围控制、通知缓存的依赖顺序切分；9 的生命周期与 fixture 根据审查负担分开或合并；10 只承载资格与证据。Issue checklist 与设计文档随已交付状态同步；此 proposed note 在全部决定落地时按 Agent Note 生命周期规则改写为 implemented。

## 备选方案

**在 CREATE/CLOSE 可用后即宣告 Windows drive 支持。** 这缩小了交付范围，却无法满足 R-WIN-2 的读写、名字、锁、通知和映射，也不能证明系统 redirector 的缓存与身份行为，因此只把 7.3 定义为内部可审查的中间交付。

**把剩余文件命令、映射与原生验收放入一个大 PR。** 它减少 PR 数量，但会使权威操作、协议编码和真实客户端问题相互遮蔽。依赖顺序明确的聚焦 PR 让每段的所有权与失败结果可以独立核验。

**由远端服务直接暴露 SMB。** 它减少本机转发层，但改变现有网络暴露、认证与部署边界，使每个远端环境都承担 Windows 协议和防火墙责任，也难以保持“选择 Windows 客户端不改变服务端”的集成目标，因此不选。

**使用 WinFsp 或 Dokany 建立本机文件系统。** 回调模型能够直接接入 Windows 文件操作，但引入驱动安装、签名、升级和异常清理责任，与无需第三方驱动的交付约束冲突，因此不选。

**同步到普通本地目录后异步上传。** 它最容易让应用打开文件，却无法提供逐次远端确认、跨机器一秒可见、同一对象身份、共享／范围保护和断线错误真实性，因此不选。

**把 Windows 名字和属性规则固化到整个 volume。** 它会让同一份数据因是否发布 Windows 入口而改变 Linux／SDK 的合法行为，并要求所有存储实现理解一个客户端平台，因此不选。

## 验收标准

以下每个 case 的用户可见结果都使用正式 Windows 11 24H2+ Home／Pro × x64／ARM64 系统客户端、真实 Windows 文件 API、实际远端 authority 和生产组合。结果从应用返回值与 authority 状态交叉确认；WN-15／WN-16 的实际 SMB 协议客户端只补充 Windows 原生 API 无法精确构造的 wire 请求证据；可由正式 redirector 发起的行为仍须原生执行，协议证据不冒充原生结果。模拟客户端、源码检查和交叉编译不计为通过。每个 case 都必须 executed/pass，不允许 skip；不适用只能由 spec 中明确的排除条款证明。

| Case | 外部动作与故障时点 | 必须观察到 |
|---|---|---|
| WN-01 发布与身份 | 在 Windows 11 24H2+ Home／Pro × x64／ARM64 上，普通用户分别在创建者会话、同用户第二登录会话、另一用户、anonymous 和 guest 下连接两个 volume；在已连接后的下一请求前篡改或撤销完整性 | 每个 drive 只呈现自己的 volume；记录本端点接纳的 TCP 445 socket 和 SSPI SID／AuthenticationId LUID 与宿主交互式登录会话的安全关联；仅创建者会话成功且每个请求绑定该会话；错误身份、篡改与降级在 authority 收到文件操作前失败；不安装驱动、不提权 |
| WN-02 打开矩阵 | 对不存在和已存在目标执行 create、open、open-if、overwrite、overwrite-if 与 supersede，并覆盖 metadata-only 和目录打开；在存在性判断与最终生效之间替换目标 | 每种存在性、截断、替换与返回句柄结果唯一；目标竞争时明确失败或作用于已验证对象；失败无部分效果，不用 open 后补 truncate／replace |
| WN-03 读写与提交 | `WriteFile`、改变 EOF、flush；分别在提交前、提交后响应前丢失连接，并在并发读取中暂停发布；触发 quota 拒绝 | 成功只在远端确认后返回；完成、明确未执行、未知可区分并可按同一动作核对；提交前其它入口看不到新状态，成功读取不混合前后版本；未触及范围保持，失败不延迟到 close |
| WN-04 身份与替换 | Windows B、Linux、SDK 分别对 Windows A 预热并已打开的文件和目录执行 rename、same-name replacement、unlink 与同名重建；记录两个 authority 对象、volume serial、128-bit Windows file ID、当前名字、属性与内容；旧对象无名后继续读、写、截断、查属性，并跨正常重连重复打开 | rename 与正常重连保持同一对象身份；replacement／重建产生新身份；旧句柄全部操作仍落在旧对象，新路径落在新对象，双方修改互不污染；authority restart 后旧句柄失败而新打开取得当前对象 |
| WN-05 名字与目录 | 从 Linux／SDK 创建大小写等价、非法 UTF-8、保留或 Windows 不可表示名字；Windows 执行查找、枚举、同目录 rename、跨目录 move、通知和已有句柄 I/O | 同目录 OLD_NAME 后接 NEW_NAME、跨目录源 watch REMOVED／目标 watch ADDED，内部事件保留同一动作关系；每个相关按名操作整体失败且不漏项、不猜测、不改数据；其它入口名字不变；已有身份 I/O 继续；一次成功枚举来自完整有界观察 |
| WN-06 属性与时间 | 对四个可设置位和三个派生位执行位级 truth table；以 `READONLY` 分别尝试 Windows 写／删除并用 Linux／SDK 修改；分别构造只缺 creation、只缺 change、两者都缺的节点，重复查询并核对 authority；逐项执行 create、read、content/length write、rename、属性修改及零／非零时间设置，并在 archive 原子提交点注入故障与竞争 | 位的设置、派生、拒绝与授权效果符合 R-WIN-5；Windows 内容修改与 archive 同时生效，Linux／SDK 不解释该位；未知字段稳定为零且查询不写回；每个时间字段只按 R-WIN-5 保持或更新 |
| WN-07 共享与范围 | 对 Windows A/B、Linux 与 SDK 的 read/write/delete/rename/replace 执行 access × share 正负矩阵；在已接纳 I/O 与新授予之间制造竞争；执行重叠／不重叠的共享／排他范围、等待、立即失败、取消与多条 lock/unlock 批次，并与引用关闭、会话终止和结果丢失交错 | 所有允许组合成功，双向冲突按最终顺序在效果前拒绝且所有入口遵守；既有已接纳 I/O 可完成；claim/lock 只由其引用持有并在排空后释放；冲突获取回滚规定前缀、成功解除不被后项失败恢复；未知结果只核对原动作 |
| WN-08 删除生命周期 | 在“打开已确认”“触发关闭已接纳”“delete-pending 已建立”三个时点分别注入 rename、unlink、Windows／Linux／SDK replacement、最后相关句柄关闭、冲突新打开、目录非空、连接丢失、客户端崩溃、stopping 与 authority 崩溃重启；接受义务后先撤销新删除操作授权，观察已接受效果；再分别撤销查询与 ACK 授权并发起这些新请求，区分普通 disposition 与关闭时删除 | 发起句柄关闭或连接／会话／宿主结束即进入 pending 并拦截新冲突，旧允许句柄仍可用，最后相关句柄后才移除名字；rename 跟随原对象关联，unlink／replacement 明确未执行；状态按 armed／pending／completed／明确未执行／失败转换；接受后无需重新授权执行；新查询与 ACK 各自重新授权，撤销各权限只阻止相应新请求；非空目录不丢子项；替代物不被删除；旧对象由旧句柄持有并最终回收；重启后状态可查询且同一清理重试无重复效果 |
| WN-09 可见性与通知 | 在两台不同机器上，Windows A 分别预热正／负查找、内容、大小、属性、目录与标识；先在超过所配置静默窗口的空闲期证明没有周期性 authority 查询，再在 authority 与变更通道都健康时让 Windows B、Linux、SDK 各完成写入、增长、截断、创建、rename、replacement、delete。每次记录确认时点，在受控的一秒窗口后段发起对应观察，要求调用在一秒截止前完成且返回当前权威状态；再独立验证一秒后发起的第一次对应观察也直接成功。两次观察分别记录 invocation、completion 和结果，不以多次重试命中代替。独立地只切断 A 的变更通道而保留普通 authority 访问，并用持续 churn 超过配置为 N 的通知上限；在分页基线扫描首个目录后修改已扫描目录以检验 snapshot/checkpoint/replay；预先让 endpoint 驱逐本地事实、但保留已向 redirector 发出过的正／负查找及信息答复，断流恢复时验证这些潜在客户端缓存仍被覆盖；在没有 listing／lease 的目录分别挂起普通 CHANGE_NOTIFY 与 `WATCH_TREE`，于子目录内容修改、跨子树 rename、断流和 overflow 后核对事件归属、完整重取或 `STATUS_NOTIFY_ENUM_DIR` 后的权威重观察；另让仍打开但已 unlink 的原对象从第二引用写入并观察第一引用 | 健康基线的第一次观察必须成功并得到权威状态；同机第二进程立即可见，跨机器已挂载进程在规定窗口内第一次观察成功；同目录 rename 通知成对、跨目录在两个 watch 上正确投影并保留内部关联；detached 句柄继续看到原对象修改；不能靠后台轮询。变更通道丢失、overflow 或恢复期间无法建立完整权威视图时明确以 I/O 错误失败；不从不完整状态成功回答。恢复必须纳入重取期间的变化，或在预算不足时明确失败；恢复成功后的第一次对应观察必须成功并返回当前权威状态 |
| WN-10 故障真实性 | Windows A 预热各类缓存后只切断 A 的 authority 路径；断开后在任何心跳／静默检测发生前立即对每类预热缓存发起一次原生 API 调用，再于检测并 fence 后各调用一次。仍连通入口在断线期执行存在→改变／删除／替换和不存在→创建；A 查询连接、降级与未确认写入状态，恢复后各调用一次 | 断线后立即调用与检测后调用都不能得到旧缓存成功，均以 I/O 故障失败，状态可查询；恢复后的第一次调用得到断线期间形成的新状态与新标识；未知写入不被自动重放；不能返回旧字节、空目录或 `not found` |
| WN-11 unmap 与 stop | 在临时 admission fence 前后并发打开并触发非强制 unmap；另让无句柄的在途操作在 fence 后、原实现可能等待期间完成。busy 后用原句柄继续 I/O 并由第三方验证锁仍冲突。随后在 WNet 移除前／后注入明确无效果、已移除和响应／查询均不确定三种结果，并在 stopping 的拒绝新工作、排空、释放句柄／锁／删除义务各阶段注入失败和丢响应，并真实终止宿主进程后由同一普通用户清理 | fence 时刻在途操作非零即 busy，即使随后完成；busy 原子回滚 admission fence、原映射完整；只有核实无效果且映射身份未变才回滚，已移除如实返回 `MappingRemoved=true` 并由 Stop／Status 报告 cleanup pending，未知结果保持 fence 和可查询 owner；stopping 永久拒绝新工作、列出未完成责任，同一 cleanup 重试幂等，已释放责任不重复释放；调用方 backend 保持可用；移除成功后无原映射且 cleanup pending 如实可查询；Stop 完全成功后没有未结清责任；进程终止不要求管理员 |
| WN-12 嵌入与资源 | 同进程发布两个 volume，轮换凭据、撤销错误 volume／operation 的授权；对计数上限配置小值 N 并验证第 N、N+1、释放一个、再申请一个；对字节上限分别提交恰好等于、超过一字节和释放后重试的 payload | package 无进程级副作用；两个 volume 生命周期与资源归属隔离；超限明确失败且占用不增长，释放后恢复精确容量；饱和 volume 不妨碍另一个 volume 的状态查询和停止 |
| WN-13 部分存储与持久损坏 | 分别使名字、对象字节、状态组成部分不可达，篡改或移除持久结构与对象内容，再通过 Windows 读取、列目录和打开 | 每类故障均为 I/O 错误；不拼出部分成功，不初始化空 volume，不返回无法验证的字节；状态报告指出受影响组成部分 |
| WN-14 平台中立合规 | 检查公开 API、wire schema、持久 schema 与 storage interface，只允许对象身份、metadata namespace、用途、范围、固定状态与中立错误；用不理解 Windows 语义的替代 adapter 运行打开、属性、共享、锁与删除组合 | 无 Windows comparer、disposition、flag、状态码或本地主体类型进入远端契约；Windows metadata 更新保留其它 namespace；Linux／SDK 名字、授权和非 Windows metadata 不变 |
| WN-15 排除项 | 以版本化有限请求 corpus 覆盖 ADS、ACL 编辑、hard link、reparse 创建／遍历、稀疏／压缩／加密、multichannel、failover 与离线写入：原生 API 可到达的请求用 redirector，其余 wire 形态由独立协议客户端补证并标注来源。用原生 redirector 执行普通打开、断开重连和旧句柄访问；另用实际 SMB 协议客户端精确发送 durable／persistent CREATE context，核对响应后尝试恢复与重放，该协议证据不冒充原生证据；同时比较快照驱动安装、LAN 监听与全局缓存／安全设置前后状态 | 独立的不支持操作返回明确 unsupported，authority 无部分效果。协议客户端请求可选 durable 或 persistent context 时，普通打开可以成功，但响应不得授予该能力；原生 redirector 的旧句柄在断线重连后不能恢复，对它的操作失败。服务端不保留可恢复句柄状态或重放记录。旧平台拒绝发布；没有驱动、LAN 暴露或全局策略变化 |
| WN-16 支持面完整性 | 查询文件种类、大小、标识、当前名字、四类时间、受支持属性、volume 容量／已用／可用空间；分别订阅名字、内容、大小、属性变化；对版本化有限 corpus 中每个未支持信息类、控制操作和标志发起可达的原生请求，未暴露给 Windows API 的 wire 形态由实际协议客户端补证；在固定真实数据集与冷状态下测量连接、首次枚举、最初读取和小文件操作 | 支持项返回权威事实且变化分类正确；空间数字满足 R-WS-5；冷挂载结果通过已写入 spec 的 R-WS-4 门槛；未支持项返回明确 unsupported，authority 无请求或无部分效果，不返回占位零值 |
| WN-17 修改结果丢失 | 对 rename、replace、delete、属性与时间修改分别在明确未提交和已提交但响应丢失处断开；在重投前让其它入口复用源名或目标名；原生应用记录返回值，已授权宿主按 per-operation ledger 的 actionID／ownerID 查询 endpoint 的结算 | 应用收到无法确认的 I/O 错误；宿主证据显示 endpoint 在有效回执窗口内只查询／重投原动作，不重复效果、不把另一调用方的状态认作原结果，也不作用于后来占据名字的对象；回执退休后保持 Unknown |

需求与 case 双向追踪如下；一项需求可以由多个 case 共同证明，但不能没有 case。

| 需求 | Case |
|---|---|
| R-FS-5、R-FS-6、R-INT-11 | WN-04、WN-08 |
| R-FS-7、R-CON-3、R-ERR-3 | WN-03 |
| R-FS-8 | WN-02、WN-03、WN-07、WN-08、WN-17 |
| R-FS-9 | WN-05 |
| R-CON-1、R-CON-2、R-CON-4 | WN-09 |
| R-CC-14 | WN-07 |
| R-ERR-1、R-ERR-2、R-ERR-4 | WN-10、WN-17 |
| R-ERR-5 | WN-11 |
| R-ERR-6、R-ERR-7、R-ERR-8 | WN-13 |
| R-INT-1、R-INT-2 | WN-01、WN-12 |
| R-INT-3 | WN-09、WN-12 |
| R-INT-7、R-SEC-1、R-SEC-4、R-SEC-5、R-SEC-6 | WN-01、WN-10、WN-12 |
| R-INT-8 | WN-01、WN-14、WN-15 |
| R-WIN-1、R-WIN-2 | WN-01、WN-02、WN-15、WN-16、WN-17 |
| R-WIN-3 | WN-05、WN-09 |
| R-WIN-4 | WN-04、WN-09、WN-10 |
| R-WIN-5 | WN-06 |
| R-WIN-6 | WN-07 |
| R-WIN-7 | WN-08 |
| R-WIN-8 | WN-09、WN-10 |
| R-WIN-9 | WN-01、WN-12 |
| R-WIN-10 | WN-08、WN-11 |
| R-WS-4、R-WS-5 | WN-16 |

### 验收证据

每个 case 记录 Windows build、应用调用、两个端点的主体身份、双方认证证据、volume 身份、authority incarnation、故障注入点、实际返回状态和权威最终状态。Windows A/B 是两台不同机器上的隔离原生客户端上下文；每份证据记录 host identity；WN-09 的变更通道与普通 authority 路径可分别切断，Home 与 Pro 的结果分别归档。fixture 对 WN-01 可在签名后定点篡改 frame，对 WN-03／WN-17 可在 authority commit 前后分别丢响应，对 WN-08 可在 delete-intent 各持久状态点崩溃重启，并用 barrier 让其它入口在同一原始名字上并发复用。涉及缓存的 case 分别记录确认时点、受控一秒窗口内观察的调用与完成时点及结果、超过一秒后第一次观察的调用与完成时点及结果；两次各只调用一次，不通过重试取得成功。涉及未知结果的 case 记录原动作 ID、宿主查询的 owner／ledger 状态和后续核对结果，分清有效回执窗口内结算与回执退休后的 Unknown。日志和测试产物必须净化凭据与内容。

成功标准是 WN-01 至 WN-17 全部 executed/pass，需求追踪没有空项，相应 package 的正常、race、错误路径、资源上限与无 skip 检查通过。Windows 平台不支持的测试工具能力须用绑定同一 case ID 的真实原生替代证据说明，不能把未执行写成通过。性能优化不能放宽同步确认、可见性或故障行为。

### 显式延期

R-WS-4 的“迅速可用”缺少可证实的工作负载和时延阈值，R-SCALE-1 也明确禁止从当前源码树假设推导设计数字。本提案不凭空填写性能门槛。Issue #30 的最终原生资格工作拥有这项测量：它必须固定真实数据集、冷状态、硬件、网络、重复次数和分位数，记录连接、首次枚举、最初读取及小文件操作；在宣告 Windows 支持前必须依据测量确定 R-WS-4 的可验证门槛、同步写回 spec 并通过。未解决门槛或未通过的资格不能宣告完成。

各类资源的默认额度与支持的最大同时 volume 数同样留给实现和测量；本提案只要求每个累积资源可配置、有界、耗尽可恢复，并保证资源饱和时状态查询与停止仍可进行。WN-12 使用任意足够小的测试配置验证这些结构性行为，不把测试数值变成产品默认值。

## 风险

Windows 系统缓存可能在远端变更后继续回答旧内容、旧目录或旧的“不存在”，并且不同信息类可能采用不同缓存路径。满足一秒可见性需要以真实系统客户端逐项证明；某一种通知或缓存设置的通过不能外推到其它观察。

Windows 报告的文件标识可能来自连接或协议上下文，而不是远端对象身份。同名替换中即使字节正确，只要旧、新对象被报告成同一身份，应用仍可能把缓存和状态关联到错误对象。该项是发布阻塞条件。

共享、锁和 delete-pending 跨越多个入口与失败边界。只在客户端保存会允许其它入口绕过或在崩溃后遗忘；把完整 Windows 状态放进远端又会污染平台中立性。共同原语必须足以表达保护与清理责任，同时保持 Windows 解释留在客户端。

历史时间的零值政策保证不伪造事实，但部分 Windows 应用可能把它显示为平台纪元。原生验收必须确认文件管理器与目标工具仍可用；若不可接受，需要先修改契约并明确迁移语义，不能在实现中悄悄换成推测时间。

系统客户端的安全协商、登录会话可见性和非强制移除行为受 Windows 平台约束。实现方向若无法同时满足普通用户、当前会话隔离、完整性保护和可清理生命周期，就不能以放宽安全策略、全局缓存设置或自动强制断开换取通过。

本提案不解决一次应用大 I/O 被拆成多个底层请求后的整体快照或全量回滚。验收必须按实际完成字节与错误报告判断现有单次操作保证，不能把片段级证据写成应用调用级原子性。
