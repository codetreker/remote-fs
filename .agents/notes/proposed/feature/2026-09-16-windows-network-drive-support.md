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

volume serial 从可信且稳定的 volume 身份导出；Windows 128-bit file ID 从不可复用的 authority 对象身份导出，命名空间分配持久化并检查碰撞。它们与 SMB 每次打开的 FileId 分属不同层次。同一对象改名或重新打开保持标识；按名 replace／同名重建获得新标识；`FILE_SUPERSEDE` 在原对象上原子重置，保持该对象标识。打开结果的 `Attr.ID` 必须和所获引用的稳定身份一致。现有 `ReferenceIdentity` 是可选接口，`AtomicFileOpener`／`NodeReferences` 不保证返回引用实现它；7.3 须增加覆盖整个 FileSession、HTTP 与 wrappers 的预检能力，承诺两种返回引用都提供不可变 `ReferenceNodeID`，在对象效果前验证完整包装链。预检违约后若仍返回无身份引用，不能宣布成功，并须保留已获引用供清理。

所有操作遵守同一生命期顺序：封住新准入 → 排空已接纳操作 → 确认 authority 的释放／barrier 事实 → 撤销本地 owner 与额度。`ReferenceCloseResult.Released=true` 只证明引用已释放；该中立结果没有 `BarrierPending` 字段。现有 `File.CloseWithResult(ctx)`／`NodeReference.CloseWithResult(ctx)` 不接受动作 ID；7.3 提议中立 `CloseWithAction(ctx, FileActionID)`，由 endpoint 在效果前生成 actionID、连同引用身份与不可变 close 意图写入 cleanup owner，再作为参数传给 native、wrappers、HTTP。返回中立 `CloseSettlement{Released, BarrierState, ActionID}`；`BarrierState` 区分无义务、待结算、已结算和未知，HTTP 特有的 pending error 在 adapter 边界转换。相同引用／意图／actionID 的重投返回原释放和 barrier 结果；同一 ID 改变引用或意图明确拒绝。首次响应丢失时 endpoint 已持有原 ID，只能查询或按原 ID 重投，不能从响应字段倒推动作身份。若释放同时仍有待结算或未知 barrier，保留原动作与 cleanup owner，直到 barrier 明确结算；不能只凭 Released 回收整份责任。释放未知或为 false 时保留引用 owner。tree disconnect、LOGOFF、TCP 断开、unpublish 和 server shutdown 均复用这条退休路径。普通句柄与 advisory 锁不跨 authority incarnation 透明恢复；一旦连续性丧失，旧 FileId 持续失败，不能重新绑定同名对象。

### 准入、身份与能力检查

发布前验证配置的 endpoint 安全能力、资源额度和 storage 包装链；宿主把可信 `Share.Volume` 与 backend 实际指向的远端 volume 绑定。`httprest.Dial` 不发网络请求，当前 `Publish` 只检查 `CheckFileStorage`，而现有 `NewFileSession`／`FileSessionStatus` 与 HTTP 响应不携带 volume 身份，所以当前代码不能证明远端目标。提案增加中立 `BackendIdentity` 预检：backend 返回不可变 volume ID、authority incarnation 和由可信宿主配置／远端认证机制验证的绑定证明；直接 adapter、HTTP client、server 和 wrappers 均传递与验证同一事实，不把调用方传入的 label 回显当证明。第一次 `TREE_CONNECT` 在返回成功前建立 authority FileSession，验证该证明与 `Share.Volume` 完全一致、session incarnation、必需能力和当前操作授权；失败撤销未宣布的 tree，并继续清理已建立但未宣布的 session。后续能力随着交付段启用，不能以 `CheckFileStorage` 一项代替：7.3 要求完整名字观察、原子打开、引用身份、动作回执、分配量与携带调用方 actionID 的可恢复 close；7.4–8.3 分别要求条件内容／metadata 修改、完整目录快照、名字 guard、范围控制及绑定同 volume/incarnation 的 `ChangeSource`。包装 adapter 任一层不能提供已声明能力时拒绝受影响的 share／tree，而不是在产生对象效果后才尝试补救。

本机连接须由 SSPI 得到 SID 与登录 LUID；匿名、guest、同 SID 另一 LUID 均拒绝。每个 SMB 请求须完成签名／完整性校验、SessionId 与 TreeId 绑定、身份未失效检查、trusted volume 与操作授权，再进入 FileStorage。重新认证仅可在同一 SID／LUID 与既定 session 规则下延续本地准入；远端权限撤销影响新动作，不能抹去已接受动作的结算和清理责任。身份过期与签名失效分别映射为准确的协议失败，不允许降级到 guest、unsigned 或路径级匿名访问。HTTP adapter 独立承载远端凭据和业务主体；不从本机 SID 生成远端身份。

概念性的请求上下文包含 `connectionIncarnation, SessionId, TreeId, MessageId, command, principalRef, trustedVolume, deadline, actionID`。其中 `principalRef` 指向受保护的认证对象，不将 SID、token、密钥放入普通日志。每次新 mutation 在准入前分配稳定 actionID；同一逻辑动作的传输重试和结果核对保留该 ID，SessionId 或 TCP 重连不充当幂等键。身份与授权检查对每次请求重新执行；已经被 authority 接纳的固定后续效果按其原授权和持久责任结算。

### 发布、映射与 tree 的责任交接

宿主显式构造 `PublishedDrive{exportID, trustedVolumeID, backendRef, SID/LUID owner, WNet target}`；`packages/smb` 只在签名会话的 `TREE_CONNECT` 中创建或复用该 session/export 的一份 authority FileSession，完成 `BackendIdentity` 绑定、能力与权限核对后才公布 tree。WNet 映射是宿主的独立系统资源，端点不替宿主连接、移除或关闭 backend。[原生映射与夹具提案](2026-09-28-smb-native-wnet-fixture.md)拥有控制器的 `Prepared/Connecting/Connected/Fenced/RemovalUnknown/RemovedCleanupPending/Stopped` 状态机、持久映射 owner 和故障注入接口。

非强制移除的跨边界保证是：设置临时 admission fence 的同一时点快照原映射的句柄、锁及 active/in-flight 操作；任一非零即 busy 并撤回 fence，哪怕在途操作随后完成。fence 不持有 WNet callback 所需锁，仍允许已接纳 I/O、CLOSE、TREE_DISCONNECT、LOGOFF 与清理。空闲时的 WNet 返回须按原映射身份判断“确认无效果／确认移除／未知”；只有确认无效果才能撤回 fence。确认移除如实返回映射已移除，`Stop/Status` 单独报告未结清 owner；未知保持 fence 和同一意图。完整停止排空 FileId、authority session、锁、delete intent 与 listener 后才报告成功。

Windows 11 24H2+ Home/Pro × x64/ARM64 的普通用户可行性必须在本端点真实接纳的 loopback TCP 445 socket、签名 `TREE_CONNECT`、SSPI SID 与 `AuthenticationId` LUID 上证明。若 redirector 采用不同 network logon LUID，须用不可伪造的同一登录会话关联；仅凭 SID 或 loopback 不准入。[替代端口配置](https://learn.microsoft.com/en-us/windows-server/storage/file-server/smb-ports)涉及提升权限，[`WNetAddConnection2W`](https://learn.microsoft.com/en-us/windows/win32/api/winnetwk/nf-winnetwk-wnetaddconnection2w)没有端口参数。不能以高端口协议客户端、管理员权限或改全局设置代替 445/WNet 实证。端口、身份与每类 Windows 缓存的原生可行性均是发布阻塞门。

在 7.3 实施前先运行**一次性调研门**：用现有 `smb.New/Publish/Serve` 与 `CurrentIdentity/Authenticator` 接口写仓库 `.tmp` 内的临时 Windows harness，让普通用户在 Home/Pro 发起原生 WNet 连接尝试，保留确由本端点接纳的 445 socket、SSPI SID／LUID、签名 `TREE_CONNECT`、同 SID 另一登录会话／其它用户／anonymous／guest 的拒绝和失败后的清理 trace。现有文件命令返回 `STATUS_NOT_SUPPORTED`，WNet 可能在随后 root CREATE／QUERY_INFO 阶段拒绝持久映射；该门只证明 445、身份关联和签名 tree 可达，不要求成功挂载 drive，不是 9 的发布控制器，也不宣称 WN-01 通过。净化证据及未解决平台差异附在当前设计审查记录中，临时 harness 用完删除；任一门未证明就先修订发布机制，不开始依赖该前提的 7.3。

### Windows 名字到权威对象

SMB codec 精确解析 UTF-16，拒绝 NUL、孤立 surrogate、保留设备名、非法组件及 Windows 不可表示名字；不改写 authority 原始名字，也不对 volume 做 Unicode normalization。Windows 等价性使用 [`CompareStringOrdinal`](https://learn.microsoft.com/en-us/windows/win32/api/stringapiset/nf-stringapiset-comparestringordinal) 的显式长度、不区分大小写比较。每一级按名选择都需完整、有界的父目录观察：一个精确匹配可以选择，零个可形成缺席条件，多于一个或任一影响该父目录的不可表示 sibling 使相关按名操作整体失败。选择保存 trusted root、每级目录 revision 和精确 raw edge，最终 authority 效果点重验；客户端早先的 lookup 不构成授权。完整 codec 规则、向量和冷路径成本归[有界 CREATE/CLOSE 提案](2026-09-28-smb-bounded-create-close.md)。这种完整观察可能使大目录打开昂贵；R-WS-4 资格要测冷路径，优化只能使用 revision 耦合的权威索引或已验证且可失效的观察缓存。

CREATE 与 CLOSE 建立后续操作依赖的 FileId：五种非 supersede disposition 对非空路径在一次 guarded `OpenAt`／`OpenChildRef` 动作内决定存在性、创建／截断、双向共享、引用和初始属性；空名字的 share root 则由同一 FileSession 的 `OpenNodeRef` 按 root 身份条件取得目录引用；supersede 留给 8.1 的 guarded 原子**原位**重置并打开动作，保持既有对象 ID，返回该对象的一份新引用与新的 SMB FileId。每个 FileId 只拥有一份 `File` 或 `NodeReference`，固定 tree、authority epoch、对象 ID、用途／share 与清理 owner。容量和身份能力在效果前预检；端点获得非 nil 引用后即使后续结果错误，也继续拥有清理责任。`ReferenceIdentity` 目前可选，7.3 要保证两种引用经 native、HTTP、wrapper 都能稳定报告 ID。相关 compound 的占位 FileId 只指向同一已签名 frame 中前一个成功 CREATE，不能借其它 session/tree 的旧句柄；编码细节由[7.3 提案](2026-09-28-smb-bounded-create-close.md)拥有。

每项修改在进入 authority 前生成稳定 actionID 并保存不可变输入；SMB endpoint 是 R-FS-8 的调用方，只在有效 receipt 窗口内查询或同 ID、同输入重投。`Unknown`／`Retired` 不能证明未执行，不能根据后来占据路径的对象补做；Windows 应用只得到明确 I/O 错误，不收到内部 actionID。宿主的有界 per-operation ledger 供诊断和验收核对。关闭还需中立 `CloseWithAction(ctx, FileActionID)`：ID 在效果前写入 owner，结果分别报告引用 `Released` 与剩余 barrier；释放引用、结算 barrier、delete intent 与退还额度是不同事实。CLOSE、断线、tree/session 退出和 stop 都推进同一 owner，不能因传输失败伪造释放。

### FileId 数据、目录与名字效果

文件 READ 使用身份稳定的 `File.ReadAt`，成功字节与属性来自同一 revision；WRITE、EOF 和属性／时间修改经过 authority 的条件动作，同次提交内容／长度与 Windows `ARCHIVE`，最终效果点核对 `READONLY` metadata token。目录或 metadata-only FileId 的属性／时间组合更新使用现有 `NodeReference.(ConditionalFileMutation).MutateFile(MutateAttributes)`；字节写入／截断仍不属于此引用，不能调用两次 setter 拼出部分效果。FLUSH 确认后端 durability barrier；CLOSE 不延后写回。Windows 属性解释只在本地 codec；其它入口不解释这些位。allocation 在 CREATE/CLOSE 中按权威字节数报告；文件／volume 信息类只有在权威 Attr、Space 与经中立 `VolumePresentation` 证明的身份／几何足够时才能成功，不能强加内置 volume 的 4096 粒度。配额下调导致 `Used > Total` 或容量结构无法准确表示时，仅受影响的容量 `QUERY_INFO` 明确失败；tree 继续允许读取、释放引用与回收空间，不对整个 share 设置故障 fence。信息类 allowlist、`MinimumCount`、输出预算、时间零值及 `NodeReference` 的条件属性动作归[文件 I/O 与信息提案](2026-09-28-smb-file-data-information.md)。

应用可见 QUERY_DIRECTORY 通过目录 FileId 的活 scope 取得一次完整有界 `DirectoryReader.ReadDirNodeBounded` 捕获，先验证**所有**名字与属性，再从冻结的同一 revision 分页；坏条目不能被 pattern 过滤掉。普通续页不冒充实时观察；restart/reopen 重取，FileIndex 只在本句柄当前捕获有效。捕获、cursor、flags、buffer 与预算由[目录枚举提案](2026-09-28-smb-directory-enumeration.md)拥有。目录路径遍历的 `DirectoryMetadataObserver` 与应用目录捕获是不同能力；后者不能替代前者的最终 mutation guards。

rename、move、replace、unlink 与普通 disposition 在最终 authority 事务核对源／目标**两侧**完整 ancestry guards、对象／关联、共享、pending、`READONLY` 和授权；目前 `NameCommand` 只有末级条件，8.1 须扩展中立命令及 HTTP/wrappers/metastore 的传递。supersede 必须在单个 guarded authority 效果中重置原对象并取得新引用，保持原对象 ID；不能先重置后另开。按名 replace 才是新对象占据旧名字。旧 FileId 在原对象无名或同名被替换后仍指向原对象；Windows file ID 从不可复用 authority ID 导出，不等于每次打开的 SMB FileId。普通可清除的 disposition 与不可撤销的已接受关闭删除义务分账；详见[受 guard 的名字修改提案](2026-09-28-smb-guarded-name-mutation.md)。

关闭时删除在打开时接受，并由宿主为每个 volume 持久保存 `DeleteIntentOwner`；没有已同步、可跨进程恢复的 owner 就在打开效果前拒绝。发起句柄关闭或连接、会话、宿主终止使义务立即进入 `pending`，阻止冲突新打开；原本兼容的旧句柄继续使用，最后相关句柄离开才移除名字。rename 让义务跟随原对象的同一关联；unlink／replacement 分离原关联后义务明确未执行，新同名对象不受影响。查询、分页恢复与 ACK 分别按当前授权处理，已接受的固定删除效果不因随后权限撤销而取消。状态、持久 owner 格式与崩溃恢复由[关闭删除提案](2026-09-28-smb-close-delete-obligation.md)拥有。

共享 claim 和强制字节范围保护都在同一 authority 对象排序，Windows、Linux 与 SDK 不能换入口绕过。共享范围锁以 `DenySelf=WriteData, DenyOthers=WriteData` 拒绝持有者自身与其它 owner 的写入；排他范围锁拒绝其它 owner 的读取与写入；解除必须按原 ClaimID。LOCK 的异步等待与 CANCEL 使用同一 pending owner；取消不证明授予未发生，批次中已成功解除的效果即使后项失败也保留。完整 `RangeControl`、批次回执、async credit/签名和资源责任归[范围锁与取消提案](2026-09-28-smb-range-lock-cancel.md)。

### 变更源与 Windows 可见性

`FileStorage` 当前没有通用订阅能力；8.3 提议与 backend `BackendIdentity` 绑定同 volume/incarnation 的中立 `ChangeSource`，让所有入口提交的名字事件和**按对象 ID**的内容／长度／属性事件有序可观察，包括已 detached 但仍打开的对象。endpoint 保留“曾向 redirector 成功报告、它可能仍缓存”的有界保守事实集合；本地驱逐不能证明客户端也已驱逐。断流、缺口或 overflow 进入 Fenced，不能从旧内容、目录或负查找生成成功。

恢复须先订阅，再取与 checkpoint 原子绑定、分页且范围有界的 scoped snapshot，回放固定目标位置以前的事件并追 live 流；scope 含已报告事实、active FileId 与所有 watch（包括 `WATCH_TREE` 子树）。不能原子绑定 checkpoint 时，只能从 snapshot **之前**的 cursor 重放，并证明早扫描目录的并发改动不会丢失。无法恢复的 pending watch 收到 `STATUS_NOTIFY_ENUM_DIR`，必须重新权威观察。同目录 rename 在一个 watch 内为 OLD_NAME→NEW_NAME，跨目录分别为源 REMOVED／目标 ADDED；内部事件仍保留同一动作关系。精确 event、watch 与 bounded recovery 由[变更通知及缓存提案](2026-09-28-smb-change-notify-cache-coherence.md)拥有。

这套服务端结构**尚未证明** Windows redirector 的内容、文件信息、目录、正负查找和标识缓存都会失效；服务端 EIO 只覆盖实际到达它的请求。lease break 只作用于匹配的已授予 key，CHANGE_NOTIFY 只回应待处理的 watch；静默失联前的本机缓存命中尤其需要实证。8.3 用已交付的 9 号 WNet／故障夹具在四个 OS cell 做聚焦 WN-09／WN-10 原生证明：各类预热后的一次调用须在一秒界限内看到权威值或明确错误，故障时还须在健康检测**之前**立即调用一次。不能靠系统级缓存设置、TTL 或周期性数据轮询通过。若某类没有普通用户可用的充分失效机制，SMB 方案不能宣称满足 R-WIN-8；具体原生门与故障证据归[资格提案](2026-09-28-smb-native-qualification.md)。

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

[Issue #30](https://github.com/codetreker/remote-fs/issues/30)的剩余工作分成九个**各有独立提案文件**的实现 PR；表中顺序按技术依赖排列，因此 9 号映射／夹具在 8.3 缓存工作前交付；这份总提案持有整体拓扑、跨任务不变量、最终 WN 矩阵和放弃的架构路线，不替代分项提案的具体接口、文件布局、算法及段内测试。每段实际落地时同 PR 更新 `docs/design/`、implemented Agent Note、代码和覆盖该段错误路径的测试；后段不能把前段未确认结果掩盖为成功。

| 段与拥有提案 | 依赖与本段结果 | 下一段取得的保证 |
|---|---|---|
| [7.3 有界 CREATE/CLOSE](2026-09-28-smb-bounded-create-close.md) | 在 445／身份／签名 TREE 可达性调研门后交付五种非 supersede 打开、typed FileId、权威 guarded 选择、共享准入和调用方带 ID 的 close。 | 句柄容量先于效果预留；响应丢失、tree 退休和 barrier 各有 owner；7.4 可按原对象 I/O。 |
| [7.4 文件 I/O 与信息](2026-09-28-smb-file-data-information.md) | 使用 7.3 FileId；READ/WRITE/FLUSH/EOF、条件内容／属性动作、目录引用已有 `ConditionalFileMutation.MutateFile(MutateAttributes)` 的条件属性动作和可证明的文件／volume 信息。 | 写入与 `ARCHIVE` 同步提交；身份与 allocation/geometry 信息不伪造；7.5 可投影完整目录条目。 |
| [7.5 目录枚举](2026-09-28-smb-directory-enumeration.md) | 使用目录引用和信息 codec；完整有界捕获、全量 Windows 名字验证、冻结 cursor 分页。 | 单次枚举不混 revision、不漏坏名字；后续名字修改仍须独立 guards。 |
| [7.6 关闭删除义务](2026-09-28-smb-close-delete-obligation.md) | 使用原子打开／close owner；宿主持久 per-volume owner、`CloseIntent`、armed→pending→终态、分页恢复与 ACK。 | 义务在发起句柄消失后仍可由 authority 完成，8.1 的普通 disposition 不会撤销它。 |
| [8.1 受 guard 的名字修改](2026-09-28-smb-guarded-name-mutation.md) | 使用完整目录观察和 7.6 pending；双侧 ancestry guard 的 rename／replace／unlink、普通 disposition、原对象身份稳定的原位 supersede。 | 旧引用与新路径不混，名字效果可按原 action 结算；8.2/8.3 获得身份与关联事件。 |
| [8.2 范围锁与取消](2026-09-28-smb-range-lock-cancel.md) | 在同一对象保护排序上接入 `RangeControl`、async LOCK/CANCEL、批次 surviving effects。 | 读写和名字效果跨 Windows/Linux/SDK 服从同一保护，pending request 有可结算 owner。 |
| [9 原生 WNet 与夹具](2026-09-28-smb-native-wnet-fixture.md) | 在 7.3–8.2 的文件／名字／锁基础上交付正式 WNet 控制器、持久远端 authority 与三条独立故障路径。 | 四 OS cell 有可复验的 socket/session/authority 绑定与可控注入，8.3 可据此做聚焦缓存证明。 |
| [8.3 变更通知及缓存](2026-09-28-smb-change-notify-cache-coherence.md) | 开始时用 9 的原生夹具对无 lease、候选 break／通知机制逐缓存类做限范围 spike；再基于实证与前段对象／名字事件、async owner 交付 `ChangeSource`、WATCH_TREE、scoped replay、保守客户端事实账与分类失效。 | 四 OS cell 的聚焦 WN-09／10 缓存首次观察与故障门通过，最终完整矩阵仍由 10 判定。 |
| [10 原生资格](2026-09-28-smb-native-qualification.md) | 在完成 7.3–9 后执行 Home/Pro × x64/ARM64、两台 Windows host、WN-01–17、冷状态测量。 | 全部必需 case executed/pass，无 skip；R-WS-4 量化门槛先写入 spec 后通过，发布证据可按 hash 与源事件复核。 |

7.3–8.2 作为中间交付审查；9 在 8.3 前提供正式 WNet 控制器与故障夹具。8.3 开始时用该夹具测候选缓存失效机制；候选实现后再跑聚焦 WN-09／10，不把 spike 当作最终通过。7.3 前的一次性门只证明现有 endpoint 的 445／登录会话／签名 TREE 可达，文件命令仍 unsupported，不能替代 9 对完整 WNet 映射和生命周期的证明，也不能替代 10 的完整资格。“Windows 网络驱动器支持完成”只在 10 的所有发布门通过后成立。

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

仍须经实证或接口审查确定的点：普通用户对本机 445、SSPI 登录会话关联与签名 TREE 的可达性；所需文件／信息命令具备后的完整 WNet 映射；Windows redirector 对内容、文件信息、目录、正负查找与标识缓存各自接受何种无全局策略的失效动作；第三方 FileStorage 分配字节与 Windows geometry 的可报告关系；`NameCommand` 两侧 ancestry guard 的中立 wire／存储形状；资源默认额度及 R-WS-4 冷状态门槛。第一项由 7.3 前的一次性原生调研给出窄证据，第二项由 PR9 正式控制器和夹具证明；缓存候选机制在 8.3 开始时用 PR9 夹具试验，不能要求在实现候选前完全证明，也不能在未通过聚焦 WN-09／10 时宣称一致性。geometry 在 7.4 信息类宣布支持前确定；guard 在 8.1 名字效果前落地；额度与性能门槛在 10 宣告支持前经测量写入规范。未证明的能力保持明确失败，不能以假定的成功状态跨门。

九份分项提案分别对应九个聚焦实现 PR；7.3–7.6、8.1–8.2、9、8.3、10 按表中依赖顺序交付，不以历史提交机械拆分，也不因预计 merge conflict 扩大单个 PR。每个 PR 的 diff 只包含其提案仍需交付的概念；Issue checklist 与设计文档随实际落地同步。全部架构决定落地后，本总提案按 Agent Note 生命周期规则改写为 implemented。

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
| WN-02 打开矩阵 | 对不存在和已存在目标执行 create、open、open-if、overwrite、overwrite-if 与 supersede，并覆盖 metadata-only 和目录打开；在存在性判断与最终生效之间替换目标 | 每种存在性、截断、替换与返回句柄结果唯一；supersede 对已存在文件在原对象上提交，旧／新打开报告同一稳定对象 ID 但各有自己的 SMB FileId；目标不存在时在同一原子打开中创建新对象；按名 replace 才替换为新对象 ID；目标竞争时明确失败或作用于已验证对象；失败无部分效果，不用 open 后补 truncate／replace |
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
| WN-16 支持面完整性 | 查询文件种类、大小、标识、当前名字、四类时间和受支持属性；在可精确表示的 volume 查询容量／已用／可用空间，再分别构造 `Used > Total`、字节值不能被容量单位整除和缺少可信 geometry 的 volume 发起相同容量 `QUERY_INFO`，随后读取文件并执行有助于释放空间的修改；分别订阅名字、内容、大小、属性变化；对版本化有限 corpus 中每个未支持信息类、控制操作和标志发起可达的原生请求，未暴露给 Windows API 的 wire 形态由实际协议客户端补证；在固定真实数据集与冷状态下测量连接、首次枚举、最初读取和小文件操作 | 可表示时容量三值与 authority 实测值及 R-WS-5 完全一致；不可表示时受影响的容量 `QUERY_INFO` 明确返回 I/O 错误、不截断或伪造数字，原 tree 的读取、引用释放与空间回收仍按身份／授权／共享／配额规则工作；其它支持项返回权威事实且变化分类正确；冷挂载结果通过已写入 spec 的 R-WS-4 门槛；未支持项返回明确 unsupported，authority 无请求或无部分效果，不返回占位零值 |
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
