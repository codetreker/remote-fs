# Agent Note: Windows 系统网络驱动器支持

Status: proposed

## 问题

Windows 11 上未经修改的程序需要访问同一份远端 volume。安装第三方文件系统驱动增加分发、升级和清理责任；同步到普通本地目录则失去逐次远端确认、对象身份、跨入口保护和断线时的真实错误。Windows 系统客户端还可能缓存文件内容、属性、目录与“不存在”：协议响应正确本身不足以证明应用看到的是当前事实。

volume 是平台中立的逻辑文件树。Windows 的名字、属性、共享模式、范围锁与关闭时删除必须被准确解释，但不能将 Windows 规则施加给 Linux 与 SDK，也不能让其它入口绕过同一对象上的权威保护。旧句柄、新句柄以及 Windows 报告的文件标识在同名替换后仍必须指向各自对象。

## 提案

集成方在自己的 Windows 进程中运行本机 SMB 3.1.1 端点，将已授权的 volume 发布给当前登录会话的系统网络驱动器。端点只监听 loopback；普通程序和文件管理器使用 Windows 系统客户端，无需第三方驱动。宿主拥有进程、日志、远端连接、远端身份与授权策略；SMB 包拥有协议会话、句柄及其清理责任。这个选择保留系统网络驱动器 UX 与宿主生命周期，但仓库须承担协议安全、并发、取消及系统缓存交互。支持 Windows 11 24H2 及以上；Windows Server 与更早的 Windows 版本不在目标内。

```text
Windows application / Explorer
        │ Windows file API
        ▼
Windows SMB redirector ── loopback SMB 3.1.1 ── packages/smb
                                                │ authenticated session → tree → file handle
                                                ▼
                                          storage.FileStorage
                                                │ host-selected adapter; HTTP is optional
                                                ▼
                                    remote authority / volume
```

`Share.Backend` 是调用方提供的 `storage.FileStorage`，但该基础接口本身不保证 SMB 所需的全部可选能力。使用 HTTP adapter 时，SMB 文件操作通过中立的 FileStorage 与 HTTP client 到远端 authority；直接或其它 adapter 也走同一 FileStorage 契约。本机 Windows SID 加登录会话 ID 只准入这个 loopback 端点，远端业务身份与逐操作授权另由宿主和 authority 执行。SMB `CLOSE` 释放一份文件引用；HTTP 服务端的 `Handler.Close` 是服务停机时的会话排空，两者不是同一个操作。HTTP 服务端存在已观察到的关闭竞态：[失败的 CI](https://github.com/codetreker/remote-fs/actions/runs/35974116040)中，同一次 `Handler.Close` 的首次清理 `EIO` 可能被紧接着的第二次清理覆盖为成功。它需要独立的聚焦修复及真实失败回归；不阻止 7.3 的契约设计，但使用 HTTP backing 的生产资格及涉及服务停机的原生验收必须以该修复通过为前置条件。

完整用户契约由 [`docs/spec/requirements.md`](../../../../docs/spec/requirements.md) 的 R-FS-5 至 R-FS-9、R-CON、R-CC-14、R-INT-1、R-INT-8、R-WIN 与 R-ERR 条目定义。交叉编译、协议单测、内存后端和自制客户端不构成 Windows 支持完成的证据；必须执行本文 WN-01 至 WN-17 的真实系统客户端矩阵。

### 已交付基础与交付边界

[中立元数据与访问控制](../../implemented/architecture/2026-09-16-neutral-metadata-and-access-controls.md)提供 NodeKind、共同时间、opaque metadata、Uses/Deny 与 range 原语；[持久节点身份与原子文件操作](../../implemented/architecture/2026-09-20-durable-identity-and-atomic-file-operations.md)提供 NodeReference、父身份名字操作、条件 mutation、action receipt 与持久 delete intent；[可恢复的引用关闭与删除义务归属](../../implemented/architecture/2026-09-23-recoverable-close-ownership.md)提供关闭的释放事实与未 ACK 义务的 owner 恢复；[有界权威名字观察](../../implemented/architecture/2026-09-20-bounded-authoritative-name-observations.md)提供目录 metadata、revision 与引用当前名字；[权威子项选择](../../implemented/architecture/2026-09-22-guard-authoritative-child-selection.md)使 OpenAt／OpenChildRef 在最终选择处核对观察证据；[虚拟分配账](../../implemented/architecture/2026-09-23-virtual-allocation-ledger.md)提供已知／未知分配字节及内置 volume 的配额依据。这些由已合并的 #37、#38、#39 及更早工作交付。

[安全且有界的本机 SMB 端点](../../implemented/architecture/2026-09-21-secure-bounded-smb-endpoint.md)已有 SMB 3.1.1、SSPI、签名、loopback listener、session/tree/export 生命周期。它对文件、目录与名字命令仍返回明确不支持；没有可浏览、可读写的 Windows drive，也没有 WNet 映射。Linux CI 与 Windows Server 2025 的 SSPI 测试证明该基础的局部行为，不构成 Windows 11 redirector 验收。

### 端到端边界

- Windows codec 只在 SMB 端点解释名字、create disposition、信息类、属性和 NTSTATUS；远端接口只表达对象身份、用途、共享拒绝集合、字节范围、metadata namespace、固定状态转换与中立错误。
- 已认证 SMB request 先绑定 principal、session、tree 与目标 export，再在 FileStorage 的权威动作处以远端身份授权。每次打开得到的 SMB FileId 只索引一份有标签的 authority 引用：普通文件由 `OpenAt` 返回 `File`，目录或 metadata-only 由 `OpenChildRef` 返回 `NodeReference`；不为一个 FileId 同时取得两份引用。SessionId、TreeId、路径和 TCP connection 都不能成为稳定对象身份。
- 稳定的 Windows volume serial 与 128-bit 对象 file ID 分别由可信 volume 身份及不可复用的 authority 对象身份导出，并跨重启、重新发布保持稳定；volume namespace 分配持久化并检查碰撞，不能截断或未经碰撞检查地散列不同 volume 为同一身份。打开结果的 `Attr.ID` 必须与引用的 `ReferenceIdentity` 核对后才能报告成功。同一对象重开保持标识，同名替换给出新标识；此对象标识与每次打开的 SMB FileId 不同。
- 每个可修改状态的逻辑动作有稳定 action identity。超时或断线后的结果是完成、明确未执行或未知；未知必须核对或重投同一动作，不能用新动作猜测，也不能把后来占据同名路径的对象认作原结果。
- 共享、范围保护和删除义务在所有入口共同经过的权威边界排序。SMB 会话负责翻译 Windows 规则与跟踪拥有者；authority 负责跨入口准入、持久结果及已接纳义务。
- 本机缓存只优化已确认状态。authority 或必要的变更观察失联时，文件、目录、属性与负查找不能以过期状态成功；恢复必须补齐缺口并覆盖重取期间的变化，资源预算不足时明确失败。

### 原生映射可行性门

在投入 7.3 的完整文件命令之前，先用已交付的安全 SMB endpoint 在 Windows 11 24H2+ Home 与 Pro 各做一次普通用户原生连接探测：端点绑定可用的 loopback 地址与系统客户端可达的端口，尝试系统 WNet 路径，经 SSPI、签名和 `TREE_CONNECT` 建立会话，并核对 listener、tree 与 FileSession 的清理结果。现有端点对文件命令返回 unsupported；若 WNet 映射因紧随其后的 root metadata 请求而失败，抓包和端点 trace 必须将已证明的端口／认证／tree 可达性与尚待 7.3／7.4 的命令失败分开，完整映射结果留待文件命令具备后复验。验证必须不提权、不安装驱动、不改全局 SMB／防火墙／缓存／安全设置，也不能依赖已经部署的系统配置偶然放行。记录端口占用、实际客户端目标、应用返回、端点请求、身份和 cleanup 证据；Home 与 Pro 任一失败都不能推断另一个版本通过；单独的 `WNetAddConnection2` 失败也不能证明端口或认证不可行。

这个门只证明系统客户端能以 R-WIN-1 的权限边界到达现有会话基础，完整映射是否可用只在所需 root 命令已实现后判定；不宣称文件命令、WNet 生命周期或 WN-01 至 WN-17 已通过。它放在前面是因为 [Microsoft 的替代 SMB 端口配置](https://learn.microsoft.com/en-us/windows-server/storage/file-server/smb-ports)使用提升权限的命令，而 [`WNetAddConnection2W`](https://learn.microsoft.com/en-us/windows/win32/api/winnetwk/nf-winnetwk-wnetaddconnection2w)没有端口参数；普通用户能否可靠使用 loopback 默认端口及宿主能否持有它，必须在目标系统上实证。若门失败，应先决定满足 R-WIN-1 的发布机制并更新本提案与设计，不能用管理员映射或全局策略变更掩盖失败。文件读写可用后，还须尽早用原生系统客户端分别预热内容、文件信息、目录、正查找和负查找缓存，再让其它入口修改，验证不改系统全局设置即可主动使每类缓存失效。候选路径包括文件 lease/oplock break、目录通知和权威重新观察；CHANGE_NOTIFY 是客户端发起的订阅请求，本身不能证明 redirector 的全部缓存类会失效。若任何类无法在一秒内完成失效，应先修订缓存机制，不得把 TTL 到期当作通过。第 9 段仍负责完整 WNet 所有权、失败恢复与持久远端 fixture，第 10 段仍负责全部原生资格。

### 交付顺序与每段完成条件

下面的编号沿用 [Issue #30](https://github.com/codetreker/remote-fs/issues/30)。每段必须在自己的聚焦 PR 中交付实现、设计文档、Agent Note 与覆盖实际边界的测试；后段不能把前段的错误结果掩盖为成功。

| 段 | 范围和依赖 | 段内可观察的完成条件 |
|---|---|---|
| 7.3 有界 CREATE/CLOSE | 依赖原生映射可行性门、权威子项选择、分配量、可恢复 close。交付五种非 supersede 打开、metadata-only／目录打开、Windows 名字选择、共享准入、FileId 归属及关闭。 | 权威动作一次给出存在性、初始 metadata、截断和引用；容量耗尽没有对象效果；打开响应丢失可按同一动作恢复；CLOSE、tree/session 退出与断线清理在 `Released` 和 barrier 结果未定时仍保留 owner。 |
| 7.4 FileId I/O 与信息 | 依赖 7.3 的稳定句柄；READ、WRITE、FLUSH、EOF、基本信息与属性／时间查询和修改，以及 volume 容量、已用与可用空间的 QUERY_INFO。 | 内容与 `ARCHIVE` 在同一权威效果内改变；同步提交、片段结果、非零时间和未知时间遵守 spec；旧句柄在改名或失名后仍操作原对象。容量三值来自权威 `Space`，分配量相关信息类的 geometry 必须经下述决策门确认。 |
| 7.5 目录观察 | 依赖 Windows 名字 codec 和完整有界 `DirectoryMetadataObserver`；实现应用可见 QUERY_DIRECTORY 与当前名字。 | 一次枚举全有或全无，冲突／不可表示名字整体失败；目录 revision 与名字身份可核对；资源预算耗尽明确失败。权威 metadata 观察和 SMB 枚举 cursor 是不同责任。 |
| 7.6 删除义务 | 依赖引用关闭结果、原对象及名字关联；实现 create 时接受的关闭删除义务、宿主提供的持久 per-volume `DeleteIntentOwner` 与 `CloseIntent`；普通 disposition 随 8.1 的名字修改交付。 | owner 不可持久恢复时在接受前拒绝；启动和停止按页列出未 ACK 义务、查询并继续清理、确认后 ACK；义务跨断线、会话停止及 authority 重启，原关联被移除或替换时明确未执行，新同名对象不受影响。 |
| 8.1 名字修改 | 依赖前述身份、观察与义务；实现 supersede、普通 disposition、rename／move／replace、unlink、current-name 更新。 | guard 在最终 authority 事务核对，动作 receipt 可恢复；文件标识随对象而非名字，旧引用与新路径不会合并。 |
| 8.2 共享与范围保护 | 在 7.3 已有的打开共享准入上补齐跨入口 rename/delete/replace 约束、字节范围 LOCK／UNLOCK、等待与 CANCEL。 | 授予、已接纳 I/O 和清理有统一权威顺序；取消、部分批次与丢失结果不留下错误锁，也不提前释放保护。 |
| 8.3 通知与缓存 | 依赖名字／数据修改事件、有界 authority change stream 和原生预热缓存可行性证据；实现 CHANGE_NOTIFY、overflow 重取、文件 lease/oplock break 或经原生证明的等效协议动作，以及内容／文件信息／目录／正负查找各类缓存失效。 | 健康通道上第一次观察满足一秒可见；变更通道失联、overflow 与恢复中的不完整视图明确失败；重取期间的变化不丢失。 |
| 9 原生映射与环境 | 依赖可用文件命令；交付当前登录会话 WNet 发布／移除与停止整合、拥有持久远端 volume 的 Linux authority、Windows ARM64 客户端 fixture。 | 普通用户不提权、不改全局策略可映射和清理；非强制 busy 不破坏原映射；两个 volume 与身份隔离；fixture 能分别故障注入 authority 和 change stream，并控制已签名 SMB frame 的篡改、提交前／提交后响应丢失、delete-intent 崩溃重启与并发名字复用。 |
| 10 原生资格 | 依赖 7.3–9；Windows 11 24H2+ Home 和 Pro 各运行正式 redirector 矩阵，记录业务返回与 authority 状态。 | WN-01 至 WN-17 全部 executed/pass，无 skip；需求追踪、平台差异、冷挂载性能与资源上限有可审计证据。 |

7.3 是下一段实际实现工作。它覆盖五种打开：create、open、open-if、overwrite、overwrite-if；supersede 的 replacement 效果随 8.1 交付。对未支持的 CREATE context、信息类和标志应按协议规定分别忽略可选提示或明确失败，不能把未实现功能声称已授予。7.3 的 PR 不混入 READ/WRITE、应用目录枚举或关闭删除效果；其完成标准是它自己的权威句柄与清理契约成立，完整网络驱动器资格仍由 10 判断。

### 7.3 的权威打开与清理设计

SMB 能力预检是一组显式的可选能力检查，不以 `CheckFileStorage` 代替：7.3 至少要求权威完整名字观察、`AtomicFileOpener`、`NodeReferences`、`FileActions`、`AllocationReporting`、引用身份和可恢复关闭；后续段再加入目录读取、属性 metadata、范围控制和变更流。发布／映射前检查能在 storage 层验证的完整包装链；第一次 `TREE_CONNECT` 建立共享 authority FileSession 后、向 redirector 报告成功前检查 session 级能力。预检失败时撤销尚未宣布的 tree 并确认清理，不把不适配的 share 宣称可用；若原子打开结果仍违反已预检的承诺，保留返回的引用并进入可重试清理，不能在对象效果之后假装未执行。

Windows 路径逐组件解码并按 R-FS-9 观察每级目录。`NamespaceGuards` 必须携带可信 `RootID`、从根到父的全部目录 revision 与精确 raw-name edge，末级另带被选子项条件；打开以父 `NodeReference` 的 scope、原始叶名字、完整 guards 和动作 ID 进入 `AtomicFileOpener`／`OpenChildRef`。最终 authority 事务再次核对整条祖先链与叶条件，并在同一结果内完成存在性、创建／截断、共享 `UseClaim`、引用授予与初始属性；较早的路径查询不能替代该核对。客户端在产生对象效果前预留 FileId slot、内存预算和待清理 owner；容量耗尽返回错误且不留下创建或截断。即使响应丢失或协议编码失败，已经被 authority 接受的引用也由原 session/tree 的 owner 保留并按原动作核对，不能直接丢弃。

每个 FileId 绑定不可复用的本地句柄身份、稳定对象 ID、用途／share、恰好一份带 `File` 或 `NodeReference` 标签的 authority 引用、动作 ID、所属 tree、所属 authenticated SMB session 与 export。一个 SMB session 对同一 export 的多个 tree 共享一份 authority FileSession；tree 只拥有自己的 FileId 集合，不产生独立的 authority session。CLOSE 在该句柄上停止新准入，排空已接纳 I/O，以 `CloseWithResult` 处理结果；`Released=true` 时可释放本地文件引用，但 `BarrierPending=true` 仍须保留 barrier 的 action／owner 并按同一动作核对；释放事实未知或为 false 时继续保留文件清理 owner。tree disconnect、LOGOFF、TCP 断开、unpublish 与 server shutdown 走同一引用退休路径，身份过期仅阻止新工作而不凭空宣布旧资源释放。清理错误应体现在状态中，并遵守现有 session/export 容量所有权。

SMB CREATE/CLOSE 响应中的 `AllocationSize` 是权威属性报告的**字节数**，不要求它是 4096 的整数倍。`AllocationKnown=false` 在有对象效果前由 capability 检查拒绝；若原子打开仍返回未知或非法值，必须作为错误并保留已经接受的引用供同一动作清理，不能把 EOF 猜作 allocation。[MS-SMB2 CREATE response](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/d166aa9e-0b53-410e-b35e-3933d8131927)定义该字段的字节语义。`AlSi` preallocation context 若只是未支持的可选请求，按 [MS-SMB2 CREATE context](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/3f343618-01b0-4aaf-b8a3-b270f9a6d334)忽略且不声称预分配；其它改变基础打开语义的未知请求明确失败。

7.4 需要单独决定 `FileStandardInformation` 等要求 cluster 几何的 Windows 信息类如何从通用分配字节与 volume 粒度形成可证明的结果。内置 volume 的 4096 字节虚拟 cluster 已知，第三方 FileStorage 只承诺有效分配字节，可能报告 1 字节等非对齐值；不能把内置粒度强加给通用接口，也不能在打开效果已提交后临时拒绝 7.3 的有效值。该决策在 7.4 的接口与原生测试确定前不得把相关信息类标为已支持。

### 后续语义约束

- 属性与时间仅由 Windows codec 解读：`READONLY`、`HIDDEN`、`SYSTEM`、`ARCHIVE` 可设置；目录、reparse 和 normal 由对象种类或位状态推导。Windows 内容或长度修改和 `ARCHIVE` 同时提交；其它入口不解释平台位。缺失的历史创建／变更时间稳定显示零值，查询不推测也不写回；显式零时间表示保持不变。
- delete-on-close 在打开成功时完成授权，由 authority 作为绑定原对象与名字关联的持久义务持有。宿主须先提供可跨进程重启恢复、按 volume 隔离的 `DeleteIntentOwner`；缺少这个 owner 时，在任何打开效果之前拒绝该选项。启动与停止使用此 owner 分页 `ListDeleteIntents`、按 ID 查询、在已完成或明确未执行且本地责任处理后 `AcknowledgeDeleteIntent`；查询与 ACK 仍按当前身份授权，失败保留 owner 和可查询状态。普通 disposition 在 8.1 设置时立即检查共享、readonly、目录为空和目标身份；清除普通 disposition 不撤销另一句柄的关闭删除义务。关联在触发前已分离或目录触发时非空，义务明确未执行；不能删除后来复用名字的对象。
- 普通句柄、共享与范围锁不承诺跨 authority incarnation 透明恢复；失去 advisory 锁连续性的旧持有者持续失败，不自动重取锁。独立的强 S/X 保护仍按 R-CC-6 至 R-CC-11 的期限与恢复契约成立，已接受的持久删除义务由 authority 接续；失去连续性不能把旧 FileId 绑定新对象。
- 成功读写需要实际 authority 确认；断线、损坏、未知结果和不完整目录视图返回 I/O 错误，不返回旧字节、空目录或不存在。固定周期轮询与超时缓存不能替代事件驱动失效及有界恢复。

### 范围切分与交付组织

对象身份、同步确认、错误真实性、一秒可见性、跨入口共享／范围保护、关闭删除义务和本机身份完整性是结构性保证。缺少其中之一会使打开、修改、缓存或生命周期的基本边界错误，不能留给最终验收修补。完整 Windows 语义仍以 R-WIN-2 和 WN 矩阵为交付目标；分段仅表达依赖顺序，不降低完成条件。

以下能力明确延后；其中“必须保留的形状”是当前实现不得封死的扩展点。

| 延后项 | 分类 | 延后成本 | 必须保留的形状 |
|---|---|---|---|
| Windows 11 24H2 之前版本与 Windows Server | feature | 增加平台矩阵、兼容分支与原生验收 | 平台探测和错误集中，旧平台不能假成功 |
| 局域网直接访问本机入口 | feature | 增加网络暴露、证书／防火墙、远端主体与部署责任 | 本机用户身份不能编码成远端 volume 身份；监听与发布所有权可替换 |
| ACL 编辑与完整 NTFS metadata | feature | 增加权限继承、持久格式、授权映射和迁移 | opaque metadata 有命名空间和版本边界；业务授权不被平台 ACL 取代 |
| reparse point、alternate data streams、hard link、稀疏／压缩／加密文件 | feature | 每种对象或数据流需要模型、配额、复制和错误语义 | 节点种类、主内容和平台 metadata 不合并成不可扩展的单一 blob |
| durable／persistent handle、multichannel 与透明故障转移 | guarantee | 需要持久会话身份、恢复证明与跨连接资源所有权，属于重新设计 | 普通句柄身份不等同于连接、路径或可恢复句柄；连续性丧失时明确失败 |
| 离线写入和重放 | guarantee | 需要冲突模型、日志、身份延续与未知结果处理，属于重新设计 | 断线明确失败；缓存不能成为第二 authority |
| 跨多个底层请求的大 I/O 整体快照或全量回滚 | guarantee | 需要跨请求事务与操作边界证明，属于重新设计 | 不宣称应用调用级原子性；已报告片段仍遵守读取、写入和错误契约 |
| Windows 本地 metadata replica | feature | 增加初始化、恢复、存储与缺口重建 | 正确性依赖权威观察；优化副本不成为 Windows 专用远端 schema |

交付 PR 以单一目的为界：7.3、7.4、7.5、7.6 各自可独立审查；8 按名字修改、范围保护、通知缓存三项依赖顺序切分；9 的发布生命周期与原生 fixture 可按审查负担组合或拆开；10 是验收与证据，不把任何前段未通过的语义塞入资格 PR。每个 PR 的 diff 在其审查时只包含该段仍要交付的概念；不按历史提交机械拆分，也不为减少 merge conflict 扩大 PR。Issue checklist 与设计文档随实际已交付状态同步，proposed note 在全部决定落地时按 Agent Note 规则改写为 implemented。

## 备选方案

**在 CREATE/CLOSE 可用后即宣告 Windows drive 支持。** 这缩小了交付范围，却无法满足 R-WIN-2 的读写、名字、锁、通知和映射，也不能证明系统 redirector 的缓存与身份行为，因此只把 7.3 定义为内部可审查的中间交付。

**把剩余文件命令、映射与原生验收放入一个大 PR。** 它减少 PR 数量，但会使权威操作、协议编码和真实客户端问题相互遮蔽。依赖顺序明确的聚焦 PR 让每段的所有权与失败结果可以独立核验。

**由远端服务直接暴露 SMB。** 它减少本机转发层，但改变现有网络暴露、认证与部署边界，使每个远端环境都承担 Windows 协议和防火墙责任，也难以保持“选择 Windows 客户端不改变服务端”的集成目标，因此不选。

**使用 WinFsp 或 Dokany 建立本机文件系统。** 回调模型能够直接接入 Windows 文件操作，但引入驱动安装、签名、升级和异常清理责任，与无需第三方驱动的交付约束冲突，因此不选。

**同步到普通本地目录后异步上传。** 它最容易让应用打开文件，却无法提供逐次远端确认、跨机器一秒可见、同一对象身份、共享／范围保护和断线错误真实性，因此不选。

**把 Windows 名字和属性规则固化到整个 volume。** 它会让同一份数据因是否发布 Windows 入口而改变 Linux／SDK 的合法行为，并要求所有存储实现理解一个客户端平台，因此不选。

## 验收标准

以下每个 case 的用户可见结果都使用正式 Windows 11 24H2+ 系统客户端、真实 Windows 文件 API、实际远端 authority 和生产组合。结果从应用返回值与 authority 状态交叉确认；WN-15 的协议客户端只补充 redirector 无法精确构造的 CREATE context 证据，不能替代该 case 的原生结果。模拟客户端、源码检查和交叉编译不计为通过。每个 case 都必须 executed/pass，不允许 skip；不适用只能由 spec 中明确的排除条款证明。

| Case | 外部动作与故障时点 | 必须观察到 |
|---|---|---|
| WN-01 发布与身份 | 在 Windows 11 24H2 Home 与 Pro 上，普通用户分别在创建者会话、同用户第二登录会话、另一用户、anonymous 和 guest 下连接两个 volume；在已连接后的下一请求前篡改或撤销完整性 | 每个 drive 只呈现自己的 volume；仅创建者会话成功且每个请求绑定该会话；错误身份、篡改与降级在 authority 收到文件操作前失败；不安装驱动、不提权 |
| WN-02 打开矩阵 | 对不存在和已存在目标执行 create、open、open-if、overwrite、overwrite-if 与 supersede，并覆盖 metadata-only 和目录打开；在存在性判断与最终生效之间替换目标 | 每种存在性、截断、替换与返回句柄结果唯一；目标竞争时明确失败或作用于已验证对象；失败无部分效果，不用 open 后补 truncate／replace |
| WN-03 读写与提交 | `WriteFile`、改变 EOF、flush；分别在提交前、提交后响应前丢失连接，并在并发读取中暂停发布；触发 quota 拒绝 | 成功只在远端确认后返回；完成、明确未执行、未知可区分并可按同一动作核对；提交前其它入口看不到新状态，成功读取不混合前后版本；未触及范围保持，失败不延迟到 close |
| WN-04 身份与替换 | Windows B、Linux、SDK 分别对 Windows A 预热并已打开的文件和目录执行 rename、same-name replacement、unlink 与同名重建；记录两个 authority 对象、volume serial、128-bit Windows file ID、当前名字、属性与内容；旧对象无名后继续读、写、截断、查属性，并跨正常重连重复打开 | rename 与正常重连保持同一对象身份；replacement／重建产生新身份；旧句柄全部操作仍落在旧对象，新路径落在新对象，双方修改互不污染；authority restart 后旧句柄失败而新打开取得当前对象 |
| WN-05 名字与目录 | 从 Linux／SDK 创建大小写等价、非法 UTF-8、保留或 Windows 不可表示名字；Windows 执行查找、枚举、rename、通知和已有句柄 I/O | 每个相关按名操作整体失败且不漏项、不猜测、不改数据；其它入口名字不变；已有身份 I/O 继续；一次成功枚举来自完整有界观察 |
| WN-06 属性与时间 | 对四个可设置位和三个派生位执行位级 truth table；以 `READONLY` 分别尝试 Windows 写／删除并用 Linux／SDK 修改；分别构造只缺 creation、只缺 change、两者都缺的节点，重复查询并核对 authority；逐项执行 create、read、content/length write、rename、属性修改及零／非零时间设置，并在 archive 原子提交点注入故障与竞争 | 位的设置、派生、拒绝与授权效果符合 R-WIN-5；Windows 内容修改与 archive 同时生效，Linux／SDK 不解释该位；未知字段稳定为零且查询不写回；每个时间字段只按 R-WIN-5 保持或更新 |
| WN-07 共享与范围 | 对 Windows A/B、Linux 与 SDK 的 read/write/delete/rename/replace 执行 access × share 正负矩阵；在已接纳 I/O 与新授予之间制造竞争；执行重叠／不重叠的共享／排他范围、等待、立即失败、取消与多条 lock/unlock 批次，并与引用关闭、会话终止和结果丢失交错 | 所有允许组合成功，双向冲突按最终顺序在效果前拒绝且所有入口遵守；既有已接纳 I/O 可完成；claim/lock 只由其引用持有并在排空后释放；冲突获取回滚规定前缀、成功解除不被后项失败恢复；未知结果只核对原动作 |
| WN-08 删除生命周期 | 在“打开已确认”“触发关闭已接纳”“delete-pending 已建立”三个时点分别注入 rename、unlink、Windows／Linux／SDK replacement、最后相关句柄关闭、冲突新打开、目录非空、连接丢失、客户端崩溃、stopping 与 authority 崩溃重启，并区分普通 disposition 与关闭时删除 | 状态按 armed／pending／completed／明确未执行／失败转换；接受后无需重新授权执行；非空目录不丢子项；替代物不被删除；旧对象由旧句柄持有并最终回收；重启后状态可查询且同一清理重试无重复效果 |
| WN-09 可见性与通知 | Windows A 分别预热正／负查找、内容、大小、属性、目录与标识；先在超过所配置静默窗口的空闲期证明没有周期性 authority 查询，再在 authority 与变更通道都健康时让 Windows B、Linux、SDK 各完成写入、增长、截断、创建、rename、replacement、delete。每次记录确认时点，在受控的一秒窗口后段发起对应观察，要求调用在一秒截止前完成且返回当前权威状态；再独立验证一秒后发起的第一次对应观察也直接成功。两次观察分别记录 invocation、completion 和结果，不以多次重试命中代替。独立地只切断 A 的变更通道而保留普通 authority 访问，并用持续 churn 超过配置为 N 的通知上限 | 健康基线的第一次观察必须成功并得到权威状态；同机第二进程立即可见；rename 通知成对；不能靠后台轮询。变更通道丢失、overflow 或恢复期间无法建立完整权威视图时明确以 I/O 错误失败；不从不完整状态成功回答。恢复必须纳入重取期间的变化，或在预算不足时明确失败；恢复成功后的第一次对应观察必须成功并返回当前权威状态 |
| WN-10 故障真实性 | Windows A 预热各类缓存后只切断 A 的 authority 路径；仍连通入口在断线期执行存在→改变／删除／替换和不存在→创建；A 各调用一次并查询连接、降级与未确认写入状态，恢复后各调用一次 | 断线期间全部以 I/O 故障失败，状态可查询；恢复后的第一次调用得到断线期间形成的新状态与新标识；未知写入不被自动重放；不能返回旧字节、空目录或 `not found` |
| WN-11 unmap 与 stop | 持有句柄、锁和 I/O 时非强制 unmap；busy 后用原句柄继续 I/O 并由第三方验证锁仍冲突。随后在 stopping 的拒绝新工作、排空、释放映射／句柄／锁／删除义务各阶段注入失败和丢响应，并真实终止宿主进程后由同一普通用户清理 | busy 是全有或全无；stopping 永久拒绝新工作、列出未完成责任，同一 cleanup 重试幂等，已释放责任不重复释放；调用方 backend 保持可用；成功后无映射或无主责任；进程终止不要求管理员 |
| WN-12 嵌入与资源 | 同进程发布两个 volume，轮换凭据、撤销错误 volume／operation 的授权；对计数上限配置小值 N 并验证第 N、N+1、释放一个、再申请一个；对字节上限分别提交恰好等于、超过一字节和释放后重试的 payload | package 无进程级副作用；两个 volume 生命周期与资源归属隔离；超限明确失败且占用不增长，释放后恢复精确容量；饱和 volume 不妨碍另一个 volume 的状态查询和停止 |
| WN-13 部分存储与持久损坏 | 分别使名字、对象字节、状态组成部分不可达，篡改或移除持久结构与对象内容，再通过 Windows 读取、列目录和打开 | 每类故障均为 I/O 错误；不拼出部分成功，不初始化空 volume，不返回无法验证的字节；状态报告指出受影响组成部分 |
| WN-14 平台中立合规 | 检查公开 API、wire schema、持久 schema 与 storage interface，只允许对象身份、metadata namespace、用途、范围、固定状态与中立错误；用不理解 Windows 语义的替代 adapter 运行打开、属性、共享、锁与删除组合 | 无 Windows comparer、disposition、flag、状态码或本地主体类型进入远端契约；Windows metadata 更新保留其它 namespace；Linux／SDK 名字、授权和非 Windows metadata 不变 |
| WN-15 排除项 | 对 ADS、ACL 编辑、hard link、reparse 创建／遍历、稀疏／压缩／加密、multichannel、failover 与离线写入逐项发起原生操作。用原生 redirector 执行普通打开、断开重连和旧句柄访问；另用实际 SMB 协议客户端精确发送 durable／persistent CREATE context，核对响应后尝试恢复与重放，该协议证据不冒充原生证据；同时比较快照驱动安装、LAN 监听与全局缓存／安全设置前后状态 | 独立的不支持操作返回明确 unsupported，authority 无部分效果。协议客户端请求可选 durable 或 persistent context 时，普通打开可以成功，但响应不得授予该能力；原生 redirector 的旧句柄在断线重连后不能恢复，对它的操作失败。服务端不保留可恢复句柄状态或重放记录。旧平台拒绝发布；没有驱动、LAN 暴露或全局策略变化 |
| WN-16 支持面完整性 | 查询文件种类、大小、标识、当前名字、四类时间、受支持属性、volume 容量／已用／可用空间；分别订阅名字、内容、大小、属性变化；对每个未支持信息类、控制操作和标志发起原生请求；在固定真实数据集与冷状态下测量连接、首次枚举、最初读取和小文件操作 | 支持项返回权威事实且变化分类正确；空间数字满足 R-WS-5；冷挂载结果通过已写入 spec 的 R-WS-4 门槛；未支持项返回明确 unsupported，authority 无请求或无部分效果，不返回占位零值 |
| WN-17 修改结果丢失 | 对 rename、replace、delete、属性与时间修改分别在明确未提交和已提交但响应丢失处断开；在重投前让其它入口复用源名或目标名，再查询或重投同一逻辑动作 | 每项结果区分完成、明确未执行与未知；核对或重投不重复效果，不把另一调用方的状态认作原结果，也不作用于后来占据名字的对象 |

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

每个 case 记录 Windows build、应用调用、两个端点的主体身份、双方认证证据、volume 身份、authority incarnation、故障注入点、实际返回状态和权威最终状态。Windows A/B 是隔离的原生客户端上下文；WN-09 的变更通道与普通 authority 路径可分别切断，Home 与 Pro 的结果分别归档。fixture 对 WN-01 可在签名后定点篡改 frame，对 WN-03／WN-17 可在 authority commit 前后分别丢响应，对 WN-08 可在 delete-intent 各持久状态点崩溃重启，并用 barrier 让其它入口在同一原始名字上并发复用。涉及缓存的 case 分别记录确认时点、受控一秒窗口内观察的调用与完成时点及结果、超过一秒后第一次观察的调用与完成时点及结果；两次各只调用一次，不通过重试取得成功。涉及未知结果的 case 记录原动作与后续核对结果。日志和测试产物必须净化凭据与内容。

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
