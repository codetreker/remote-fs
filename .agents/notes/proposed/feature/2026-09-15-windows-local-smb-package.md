# Agent Note: Windows 本机 SMB 接入 package

Status: proposed

## 问题

Windows 上未经修改的程序需要访问远端 volume，集成方需要把这项能力嵌入自己的 Go 程序。安装额外文件系统驱动增加了部署与分发成本；复制到普通本地目录再上传无法保持逐次远端确认、同一文件身份和断线错误行为。

目标系统为 Windows 11 24H2 及更新版本。交付仍以 package 为主，业务程序提供远端连接、访问身份与生命周期。Windows Server 不是本提案增加的验收平台。

本提案细化 [需求](../../../../docs/spec/requirements.md) 中的 Windows 接入目标。它描述待实现的方案；现有 Linux 实现不因此获得 Windows 支持声明。

## 提案

新增独立 Go package `packages/smb`，在 Windows 客户端进程内提供仅本机可访问的 SMB 服务。Windows 系统 SMB 客户端把 share 映射为盘符；一个 share 对应一个远端 volume。

```text
Windows 应用
    | Win32 文件操作
Windows 系统 SMB 客户端
    | 回环 TCP、自定义端口
packages/smb
    | 注入的 storage 能力
远程客户端适配器
    | HTTPS + 当前 SSE 变更订阅
远端 HTTP handler / Authorizer
    | 同一份权威文件、共享限制与锁状态
volume
```

远端传输保持可替换。当前仓库的 HTTP 变更流是 SSE，不能把可替换传输的目标写成已经实现了 WebSocket。

### package 与依赖

| 位置 | 职责 | 依赖限制 |
|---|---|---|
| `packages/smb` | SMB 协议、share 路由、身份绑定、句柄转换、请求调度、状态与关闭 | 接受存储能力与认证 provider；不导入 HTTP、SQLite、FUSE 或 CLI |
| `packages/smb/internal/...` | 选定协议引擎的适配、签名、请求状态机、NTSTATUS 映射 | 第三方引擎类型不进入公共存储 API |
| `packages/smb/windows` | 本机身份认证的 Windows 实现、盘符映射和移除 | 显式 Windows API；不在 init、构造器或 Serve 中修改映射 |
| `packages/storage` | 可跨传输表达的文件访问、共享限制、目录引用、范围锁与结果核对契约 | 沿用一组公共语义定义，authz 和传输直接复用 |
| 原生 metastore、storage wrappers、HTTP | 最终发布处的统一检查、状态保持与传输 | 本机 SMB 进程不是跨机器锁的权威 |

上述是同一 Go module 中的独立 package，不另建仓库或 module。Linux 调用方不导入 SMB 即不承担其运行时依赖。盘符映射不与协议服务绑定，业务可以自己映射，也可以只使用 UNC 路径；自定义端口连接须先经过系统支持的映射配置。

公共 API 的拟定形态如下，类型名在实现时与现有 storage 定义统一：

```go
type Share struct {
    Name    string
    Volume  string
    Backend storage.WindowsStorage
}

type Config struct {
    Authenticator Authenticator
    Authorize     authz.Authorizer
    Limits        Limits
    Logger        *slog.Logger
}

func New(config Config) (*Server, error)
func (s *Server) Publish(share Share) (*Export, error)
func (e *Export) Unpublish(ctx context.Context) error
func (s *Server) Serve(ctx context.Context, listener net.Listener) error
func (s *Server) Shutdown(ctx context.Context) error
func (s *Server) Status() Status
```

`storage.WindowsStorage` 是待增加的必需能力组合，不是现有接口。它包含普通树访问、可保留的文件和目录引用、Windows 访问约束，以及有界且可报告健康状态的变更观察。仅实现当前 `storage.Storage` 或 `FileStorage` 不足以通过构造检查；不得使用按路径重新打开的替代实现。

当前可复用与需要扩展的位置：[`FileStorage/FileSession/File`](../../../../packages/storage/files.go) 已持有身份引用和同步范围写入，但 OpenAccess 只有 `Read/Write/Create/Truncate/Exclusive`，且 OpenFile 拒绝目录；[`authz.AccessRequest`](../../../../packages/authz/authz.go) 已直接复用 storage 操作与打开意图；[`HTTP file client`](../../../../packages/transport/httprest/file_client.go) 已承载远端会话与结果核对；[`replicated.Storage`](../../../../packages/storage/replicated/replicated.go) 仍具体依赖 SQLite replica，需拆开 Windows 无法使用的权威持久存储依赖。

`New` 只校验配置，不监听、不连接远端、不启动后台任务。Publish 校验已准备的 backend 能力，并在短锁内发布 share；share 名称按 SMB 规则比较，重复名拒绝。backend 由调用方准备并保持可用，server 不关闭调用方拥有的 backend。认证/资源配置在服务期间不可变；share 独立发布和停止，不影响其它 volume。

Export 拥有 share 注册、tree、会话内属于本 share 的引用和观察器。普通 Unpublish 在同一准入锁内检查活动引用；有打开文件或未完成操作时返回 busy，保持访问有效。成功时阻止新 tree/open、排空该 share 的控制任务并核对其远端清理。失败或超时保留 stopping 状态与占用的 share 名，重试只继续原次清理；该名字直到确认退休才可重新发布。Serve 停止时统一停止所有 Export。可显式请求强制停止，但不得伪装成无损卸载；旧请求/引用失败，未知远端结果仍按 fencing 规则处理。

远端文件会话按 `(Export incarnation, SMB session)` 隔离；Export 清理仅终止其 tree、引用与远端会话，不关闭其它 Export 共用的 SMB 连接/认证会话。连接或整个 Server 结束时才清理该连接或 Server 拥有的全部资源。

Authenticator 与本机 Authorizer 都是必填配置；即使其它 authz 调用场景允许 nil，SMB New 也拒绝缺少本机授权策略。Windows helper 提供只允许指定挂载者 SID 的策略，放宽到其它本机身份需要业务显式替换该策略。Logger 的 nil 表示不输出日志，不改变错误返回和 Status。

`Serve` 成功接管 listener 后负责关闭它；只接受可证明绑定到回环 TCP 地址的 listener，地址不可判定或绑定通配地址都拒绝。一个 Server 只运行一次 Serve。停止时关闭准入和连接，取消等待并回收本服务建立的引用；所有后台任务均属于 Serve 生命周期。

`Shutdown` 请求停止并等待排空。截止时间到达返回超时并保留可查询的清理状态，不能报告所有远端引用已经释放。Serve 最终返回运行错误与清理错误的保留原因；进程崩溃后的远端状态由会话到期与隔离契约清理。Status 为有界快照，不含路径、凭据或文件内容。

### 本机连接与映射

系统 SMB 客户端从 Windows 11 24H2 起支持自定义 TCP 端口。使用独立回环端口与系统 445 服务共存；不修改系统 SMB Server 的监听设置。微软给出的映射流程要求管理员权限，不能把“免额外驱动”描述成“所有操作均不需要提权”。[Microsoft: alternative SMB ports](https://learn.microsoft.com/en-us/windows-server/storage/file-server/smb-ports)

```powershell
New-SmbMapping -LocalPath R: -RemotePath \\127.0.0.1\work -TcpPort 1445
```

该命令只展示官方参数与拟定回环地址的组合；本机认证、自定义端口、普通应用访问与 UAC 会话可见性尚须原生 Windows 验证。不能以文档支持自定义端口推断回环映射已经通过。

`packages/smb/windows` 提供显式 `Map(ctx, MappingOptions)` 和返回对象的 `Unmount(ctx)`。映射对象记录实际用户登录会话、盘符、share 与端口；只移除自己成功创建且仍匹配的映射，不抢占已有盘符。Map 不拥有 Server/Export；映射失败仅回滚自己创建的系统映射，宿主按自己的启动事务清理本次新建 Export，不能关闭其它 share 共用的 Server。正常卸载有打开文件时报告 busy，保留服务与映射；强制断开必须由调用方单独明确请求，并使旧句柄失败。禁止通过全局注册表或安全策略变化消除认证、签名或 UAC 检查。

### 身份与授权

回环地址限制连接来源，不证明连接进程属于挂载者。SMB SESSION_SETUP 必须经过真实的 SPNEGO 认证交换。`Authenticator` 为每次握手创建独立状态，接受有界 token，返回继续交换或终态；终态包含经验证的本机身份和签名所需 session key。失败、超时与缺少签名能力均拒绝会话。密钥只在协议层持有，关闭时清除，禁止记录 token 和密钥。

Windows provider 使用系统安全能力实现交换，将已验证的本机身份放入请求 context；业务通过现有 `authz.Authorizer` 决定该身份能否执行 share 对应的操作。`Share.Volume` 是业务配置的可信授权标识，必须与其 Backend 的实际目标一致，不能使用 SMB 请求中的名字作为远端身份。默认策略只允许配置的挂载者 SID；不使用 guest、匿名或关闭签名的兼容路径。provider 可由业务替换，协议 package 不引入用户表、密码库或远端登录机制。SSPI 在本机回环、指定服务身份下的完整交换和会话密钥获取属于验收门槛，不能以返回一个 SID 的回调代替协议认证。

身份验证只建立可信身份，授权仍按每次操作进行。share 的 backend 与远端身份由业务配置绑定，不从客户端提供的 volume 名、SID 字符串或请求头推导。远端 Authorizer 继续按当前业务凭据及实际 storage.Operation/OpenAccess 作决定；本机授权拒绝也发生在 backend 调用前。业务撤销本机会话访问后停止其后续准入及通知，已准入操作按既有错误与结果核对规则结束。

SMB 侧和 HTTP 侧共享操作语义，不重复定义一组 authz 文件操作。远端凭据通过业务提供的传输更新，不复制到 SMB session。多个本机用户共享同一远端身份是业务的显式授权决定，不是默认行为。

凭据轮换由 provider/业务 transport 自身的并发安全更新能力完成，不替换 Server 配置或已验证身份。新的本机认证交换使用当前 provider 凭据；既有 SMB session 保持其已协商的 session key，凭据更新不自动断开它。新交换失败只拒绝新会话；显式撤销权限仍按实时 Authorizer 决定。若协议请求 session reauthentication，只有确认同一身份才可更新该会话，不能用换凭据切换到另一用户。远端 HTTP 凭据同样在后续请求中更新，失败沿原调用错误返回，不能重新执行已不明的 mutation。验收包含持有文件/锁时轮换、同时建立新 SMB 会话、刷新失败及显式撤销的区别。

Windows 11 24H2 的部分 edition 默认要求 SMB 签名；本方案在所有目标 edition 都要求签名，不随系统弱配置降级。[Microsoft: SMB signing](https://learn.microsoft.com/en-us/windows-server/storage/file-server/smb-signing)

### 文件对象与访问约束

SMB FileId 由本地连接/会话 incarnation 与远端保留引用关联，不能用路径或复用的小整数重建。TreeId 只引用配置的 share。复合请求中的相关 FileId 按该复合请求的成功结果解析；前项失败不得让后项作用于旧句柄。普通 CREATE 的查找、创建、截断、共享检查和引用返回在远端同一次有序操作中完成。

扩展 storage 的公共语义，至少包括：

| 操作数据 | 必须表达的内容 |
|---|---|
| 打开请求 | 数据读写/追加、删除、属性读写与安全信息读取意图；允许其他打开者的 Read/Write/Delete；六种 create disposition；文件/目录/任意类型；期望身份；delete-on-close |
| 打开结果 | 保留引用、实际身份/类型/属性、created/opened/overwritten/superseded 的真实结果 |
| 目录引用 | 身份绑定的枚举、相对创建/改名/删除、通知；改名后继续指向原目录 |
| 名字修改 | 源与目标父目录身份、叶子名、期望源/目标身份和 replace 语义；不得由客户端 Stat 后裸路径 mutation 拼接 |
| 范围锁 | 与 Linux advisory 不同的 Windows 锁族、SMB open 所有者、范围、共享/排他、立即失败/等待、动作身份、核对与取消 |
| 属性 | 现有身份、大小和时间，加上实际支持的 Windows 时间/DOS 属性；未知的可写属性不得成功后丢弃 |

Generic access 在 SMB 层展开为 storage 的语义访问集合；MAXIMUM_ALLOWED 只能根据实际授权与后端能力得到，不能无条件授予全部权限。metadata-only 和目录打开均为合法独立操作，不冒充数据读取。

每个后续操作同时检查当前业务授权、open 授予的访问集合及当前共享/锁状态；不能因已经通过 CREATE 就允许超出该引用权限的读写、删除或属性修改。SMB 协议用于打开引用的 FileId 与用于文件属性查询的稳定 node identifier 分开：重复打开同一对象可以产生不同 FileId，但其文件身份保持一致。Windows file identifier 直接无损编码权威 node ID，volume identity 另行区分所属 volume；不截断、不随机重分配、不从路径哈希生成。跨连接、客户端和正常重启保持同一节点身份，已删除 ID 按权威高水位机制不复用。

共享检查是双向的：新打开的读/写/删除意图必须被所有既有打开允许，新打开的 share mask 也必须允许既有访问意图。检查与授予、最后关闭和相关 mutation 在同一权威状态中排序。Linux/HTTP 打开仍保持默认允许共享、不自动取锁，但其实际访问和后续修改受已存在的 Windows 限制约束。

Windows 删除请求进入明确的 delete-pending 状态，拒绝不允许的新打开；名字何时移除与保留引用何时回收分别定义。既有合法引用继续指向原对象。Linux unlink 的既有保留对象语义不变；是否允许其进入由 Windows 共享限制与删除状态决定。目录删除必须在最终操作中判断非空、子项变化和打开状态，不能信任先前列表。

Windows 范围锁对其它所有者的冲突 I/O 生效，包括 HTTP/FUSE 访问；同一 SMB open 的数据操作按 Windows 锁所有者规则判定。对每次读取的版本取得、写入/截断的最终发布都检查。同主所有者重叠规则与解锁错误按 SMB 协议实现，不能套用 POSIX 的范围合并替换。既有 flock、POSIX advisory 和强 S/X 继续分别执行自身规则。

批量 LOCK 不是统一的全成全败事务。请求加锁遇到 FAIL_IMMEDIATELY 冲突时按协议撤回本次此前取得的范围；批量解锁按序生效，后项失败保留此前成功解锁的结果；某些非法后项也会保留先前授予。整个动作在远端有序执行，结果历史同时记录终态与实际保留的范围变化；错误不代表动作没有影响。不能在本机循环调用远端单范围操作后用一个错误掩盖部分成功，回滚结果未知必须隔离并核对。[MS-SMB2: acquiring locks](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/670c7eda-e683-4923-9477-414303959613)、[unlocking ranges](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/79eb3c91-563b-4d48-a51c-0974f9d144f8)

共享限制、Windows 锁与有效引用的失效需要同一会话 fencing：远端重启或失去连续性时，旧 SMB open 失败；在旧确认仍可能有效的期间阻止冲突访问。不能在后台按路径重新打开、重取锁后继续提供旧 FileId。

### 名称、属性与目录

文件名查找保持后端的身份与大小写规则。Windows 的名称兼容策略作为 share 的必需能力校验：后端必须在统一的命名规则下提供 Windows 可访问的名字和大小写比较，不可只在 SMB 创建时检查而让 HTTP 写入制造冲突。Windows 不可表示的既存名字、大小写冲突或非法编码使该 share 明确不可用；枚举不能隐藏冲突项、替换名字或把两个对象合成一个。实现前固定比较算法和版本，并覆盖所有命名写入及冷打开扫描；扫描须有界、可取消并暴露耗时。

这是 Windows 接入对目标 volume 的准入要求，不修改未启用该能力的 Linux volume。启用必须在权威端验证并原子地标记命名策略；扫描与并发命名写入不能出现空档。该策略不意味着给旧数据增加运行时迁移或自动改名。

SMB 文件属性只报告真实存储值或协议允许的、已声明的派生值。创建时间、change time、DOS readonly 等经支持的可写字段需要持久保存，并参与同一文件修改顺序。合成的 security descriptor 只表达当前 share 访问模型，不声称已有完整 Windows ACL 管理；设置不支持的安全信息、ADS、EA 或其它信息类返回对应不支持状态。协议必须提供的标准查询不能用统一 NOT_SUPPORTED 代替。

符号链接沿用 volume 边界与文件身份要求：支持的链接通过合法的 SMB reparse/symlink 语义表示，不能把越界目标作为本机路径访问。Windows 权限、相对/绝对链接和打开链接本身的行为纳入验收；无法保持既定符号链接保证的 backend 不通过 Windows 能力检查。

枚举状态绑定目录身份和打开引用，保存有界快照/游标以及 pattern、信息类和重启位置。缓冲区不足与枚举结束分别返回协议状态。目录变化允许新枚举看到新状态，不能把失败当作枚举完毕；正在枚举的目录改名不会切换到原路径的新目录。

### 缓存与变更

映射要求 `UseWriteThrough`，强制签名；服务端不授予 read/write/handle caching lease 或 oplock，share 禁止离线缓存，不声明 durable/persistent handles 或 transparent failover。设置必须由映射结果验证，不能仅把选项传给命令后假定生效。

```powershell
New-SmbMapping -LocalPath R: -RemotePath \\127.0.0.1\work -TcpPort 1445 `
    -UseWriteThrough $true -RequireIntegrity $true -Persistent $false
```

微软描述 `UseWriteThrough` 为 forced unit access。它与不授予缓存权限一起构成待验证配置，不能单凭这些标志就宣布满足 R-FS-7/R-CON-1/R-ERR-1。[Microsoft: New-SmbMapping](https://learn.microsoft.com/en-us/powershell/module/smbshare/new-smbmapping?view=windowsserver2025-ps)

`NO_CACHING` share flag 约束离线缓存，不等于禁用所有目录、属性和 negative cache。Windows 另有全局客户端缓存设置，本方案不修改这些机器级参数。[Microsoft: SMB client configuration](https://learn.microsoft.com/en-us/powershell/module/smbshare/set-smbclientconfiguration?view=windowsserver2025-ps)

远端逐次确认后才返回 SMB WRITE/SET_INFO 成功；FLUSH 校验真实引用健康及持久屏障，CLOSE 不是首次提交时机。不缓存未确认的成功写入，结果未知时返回 I/O 类失败并保留动作状态。

查询与读取首先取得 backend 的健康结论。使用现有 replicated 客户端时，必须将 SQLite replica 与 Linux 原生持久权威的依赖分开，并从 replica 完成 apply 后输出有界事件，包含失联、缺口和重建状态；不能旁开一个与读取副本顺序无关的通知流。

最终交付保留现有 metadata replica 的低延迟路径；Windows 使用可移植的 replica 包和共享 SQL primitives，不使用 nativelease 的空实现。直接 HTTP backend 可用于隔离验证 SMB 与远端操作，但不代替冷挂载、目录性能和故障门控验收。SMB package 本身不依赖采用哪一种 backend。

CHANGE_NOTIFY 挂在保留的目录引用下，记录过滤器和递归范围。队列溢出返回要求重新枚举的真实状态；流失联或重建使查询遵循不可用门控，不能把缺口后的事件当作连续历史。通知只是程序的变更提示，不能代替系统缓存一致性；持续打开句柄、属性缓存、负缓存以及断线读取都需要独立验证。

### 请求、取消与错误

连接、会话、tree、open 和异步请求各有独立身份与所有者。SMB MessageId/AsyncId 映射到一次本地操作，远端动作使用现有可核对的 SDK action identity；重投递相同消息不得额外打开文件、授予锁或执行 mutation。不同内容复用同一身份拒绝。保留结果的历史有界，过期后报告未知或终止原会话，不把过期动作当作新动作执行。

CANCEL 是引用原 MessageId/AsyncId 的控制消息，先按其命令类型路由到目标请求，不能被普通请求的“相同身份、不同内容”去重规则拒绝。

相关 compound 子请求依协议顺序执行，但不是数据库事务。已经完成的创建/写入不因后项失败被伪装为未发生；后项使用前项产生的真实引用。异步等待使用 SMB pending/AsyncId，不阻塞连接的 CANCEL、关闭与其它独立请求。

CANCEL 自身不发送响应，目标请求只产生一个最终结果。等待中的锁必须先核对取消与授予竞态：证明没有遗留授予才返回取消；授予已完成可以返回成功；核对不明则报告 I/O 类错误并隔离相关会话。不能把 Go context 取消映射成“远端动作未发生”。原请求结束之后不再返回第二个终态。[MS-SMB2: CANCEL](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/57bae3d3-5dd7-4a5f-92cb-fc52e2087dad)

对当前没有可核对动作身份的名字修改，必须在权威接口增加原子执行与结果核对能力后才支持重投递。未知 WRITE/SET_INFO 不重新构造一次普通请求重试。断线与服务停止不证明远端未执行；未释放引用和不确定动作在权威端继续计入资源及配额，直到确认退役。

本地清理只在配置的 CleanupTimeout 内执行核对。到期后停止该所有者的网络重试与续期、释放它独占的 worker/buffer/socket，将其原远端 session 标为不可恢复，返回清理未知；Export 清理不关闭共享连接。仅保留受 MaxTombstones 与 HistoryTTL 限制的状态摘要。摘要容量耗尽拒绝新准入，不能覆盖仍需查询的有效历史；历史到期后查询明确返回过期/未知，旧 incarnation 永不复用。有限摘要到期不等于权威引用或配额已释放：远端按已确认的 session 期限 fence 后排空回收，重启也不得恢复旧权限。后台任务不因等不到远端证明无限存活。

现有 HTTP 错误是符号化 errno，SMB 根据语义映射 NTSTATUS；不把 Linux 数值转换成 Windows 数值。至少区分不存在、身份失效、权限拒绝、sharing violation、lock conflict、delete pending、目录非空、quota exceeded、资源上限、不支持、取消与 I/O 未知。后端不可达不能返回 NO_MORE_FILES、OBJECT_NAME_NOT_FOUND 或空的成功数据。错误链保留到业务日志，同时 wire response 仅暴露协议需要的信息。

### 有界资源与可观测性

`Limits` 必须覆盖连接/认证交换、SMB session/tree/open、单请求长度、总在途字节、credits、并发 backend 调用、等待锁、通知、枚举快照、结果历史与清理时间。设置提供一组明确的默认值，零值是否使用默认必须统一定义；显式非法或互相矛盾的配置在 New 拒绝。具体数值依据协议 credit 单位和实测内存成本确定，不能把单请求上限当作进程总上限。

接收长度、compound offset、UTF-16 长度、read/write range 和数组计数在分配和解码前校验。客户端不能通过 credits 绕过总字节预算。连接写出队列有界、带截止时间；慢客户端导致该请求/会话终止，不无限保留通知或后端结果。不同文件的请求可并发，同一请求、引用关闭和权威发布的顺序由各自状态机保障。

每个异步边界都有归属：连接读写循环归 Serve；认证、请求与 compound 子项归连接/session；锁等待与 notify 归 open；续期与健康观察归后端会话；清理任务归其所有者直到确认完成或 CleanupTimeout 到达，后者只留下有界摘要。所有任务由有限的 task group 管理并参与关闭等待。后台任务从父 context 派生，附带 server instance、share、session、MessageId/AsyncId、open 和远端 action 标识；不得把未认证输入直接当成身份标签。

日志由调用方传入的 slog.Logger 输出，错误记录包含 `component`、`phase`、`error_kind`、经过清理的原因和上述可用标识。不得直接打印可能包含 URL 凭据、认证 token、路径或文件数据的原始错误。Status 包含 connecting/ready/unavailable/stopping、已确认的后端健康与剩余引用/等待/未知动作数量，不记录文件名。

业务使用 JSON slog handler 时，可按 `component=smb`、`server_instance`、`session_id`、`message_id` 过滤一次失败请求，再通过 `remote_action_id` 关联服务端核对日志。验收需制造一个远端提交结果不明和一个取消竞态，确认日志中可以区分“拒绝”“已执行”“结果未知”，且没有凭据。无模型调用或模型工具 API，模型 token 预算不适用。

## 备选方案

**远端直接提供 SMB。** 减少本机一层协议转换，但改变远端暴露与业务认证接入方式，且不能复用既有 HTTP 网络路径。当前选择本机 adapter；公共 storage 能力保持可用于其它入口。

**WinFsp/Dokany 挂载。** 提供更直接的 Windows 文件系统回调，但引入额外驱动安装、分发与生命周期，偏离已确定的客户端部署目标。

**Samba VFS 或 SMBLibrary helper。** Samba 提供成熟协议与 VFS，但官方部署面向 Unix/Linux，Windows 需要额外运行环境。SMBLibrary 有较完整的 Windows 文件接口，但带来 C# helper/runtime 与 IPC，锁等待/取消仍需扩展。两者不作为当前独立 Go package 的默认实现。[Samba](https://www.samba.org/samba/what_is_samba.html)、[SMBLibrary 文件接口](https://github.com/TalAloni/SMBLibrary/blob/2edbcf3161084b51dbe11b8a960b0bc7c224b851/SMBLibrary/NTFileStore/INTFileStore.cs)

**直接嵌入现成 Go SMB 服务端。** 调查的 `macos-fuse-t/go-smb2` 具有自定义 VFS 和 listener，但当前 LOCK 无条件成功、CANCEL 不支持、变更通知未接入真实事件源，且认证接口不能由外部包实现，不能原样采用。[LOCK/CANCEL/notify 实现](https://github.com/macos-fuse-t/go-smb2/blob/c0e6b139796e67ce8da148aa5942c55a7dff2fe8/server/file_tree.go)、[认证接口](https://github.com/macos-fuse-t/go-smb2/blob/c0e6b139796e67ce8da148aa5942c55a7dff2fe8/server/authenticator.go)。其 README 列明 AGPL/商业授权；本仓库使用 MIT，引入之前需要明确许可选择，不能因它使用 Go 就当作可直接嵌入的依赖。

**维护独立的 Go 协议实现。** 保持 package 边界、授权和权威操作可控，但需要承担 SMB 协商、签名、异步状态机、协议测试与后续维护。Sombrero 有 MIT 的实际服务端代码可评估提取，然而其实现绑定 Sia/PostgreSQL，局部锁和通知也不能原封不动保留。[源码](https://github.com/mike76-dev/siasmb/blob/34430ba85f9683b69df7d723611efaa3d41743a3/server.go)、[许可与第三方声明](https://github.com/mike76-dev/siasmb/blob/34430ba85f9683b69df7d723611efaa3d41743a3/LICENSE)。CloudSoda/go-smb2 是客户端，不是可选服务端引擎。[项目声明](https://github.com/CloudSoda/go-smb2/blob/0b399b9d036cfa042bdd8c618b8abc85ee5ee3f0/README.md)

本提案推荐保持 Go package 与内部引擎隔离，优先评估许可适用的 Go 实现提取/维护；尚未选定可以满足要求的协议引擎。引擎选定须有版本、许可依据、缺失功能清单和下列原生验收证据。不能以“SMB 3.1.1 协商成功”代替锁、取消、认证或缓存行为的证明。

### 范围与代价

| 分类 | 本次交付边界 | 延后的代价与必须保留的形态 |
|---|---|---|
| 保证 | 同步写入、真实错误、文件身份、共享限制、锁连续性、用户隔离、有界资源 | 全部属于交付条件；不得以先支持读写为由删掉 |
| 功能 | Windows 11 24H2+ 本机 share、盘符映射、现有普通文件/目录与符号链接操作 | 旧 Windows、远端 LAN SMB、额外驱动不在目标；拒绝不支持的环境 |
| 功能 | 支持必要的 SMB 文件/目录/空间/安全信息查询 | 完整 NTFS、ADS/EA、ACL 编辑等扩展拒绝；新增支持需扩展真实后端能力，不能提前宣称 |
| 形态 | 一个 listener 可独立发布和停止多个 share | 不交付网络管理 API 或跨进程 daemon 控制；以后可封装同一 Export 生命周期，不把唯一全局 volume 写死在引擎 |
| 形态 | 失联后旧引用明确失效 | durable handles、multichannel、透明重连延后；不声明能力，不按路径恢复；以后增加须另做会话恢复设计 |
| 形态 | 业务提供 transport/backend 和认证 provider | 不把 HTTP、SQLite 或用户体系写进 SMB API；将来替换无需重写协议 |
| 保证 | Windows 实机与跨协议验收 | 交叉编译和 Linux 模拟客户端不替代真实 Windows 缓存/UAC/认证测试 |

较小的范围只做本机可读 share，会放弃既定写入与文件语义，不能作为目标的完成。较大的范围同时提供 LAN SMB、NTFS 扩展与透明恢复，会引入不同的部署、安全和持久会话合同，独立决策后才能扩展。

## 验收标准

实现按依赖顺序验证，每项都属于最终交付；失败时保留真实结论，不自动放宽需求。

1. **本机接入与引擎门槛。** 真实 Windows 11 24H2+、系统 445 服务保持运行、自定义回环端口、签名开启、非 guest、本机身份交换、默认私有 share。分别验证普通/提升用户会话中的映射和资源管理器访问；其它本机用户访问被拒绝。明确要验收的 edition 与系统 build。
2. **操作时点。** 对普通 Win32 WriteFile、SetEndOfFile、FlushFileBuffers 人工暂停远端确认，证明应用不会先成功；确认前后读取、大小和 EOF 一致。断线后对已缓存内容、属性、负查找和目录枚举测试真实 I/O 失败。
3. **跨客户端一致性。** 两个 Windows 客户端，以及 Windows+现有 Linux FUSE+HTTP，覆盖持续打开下写入/增长/缩短、一秒内可见、创建/改名/删除通知。没有依赖定时重扫获得正确性。
4. **对象与目录身份。** 打开后改名、unlink、同名替换、目录改名后相对操作、枚举中替换；旧句柄不访问替代对象。真实 Windows 使用 GetFileInformationByHandleEx(FileIdInfo) 记录文件和 volume 标识：重复打开、连接重建后的重新打开、跨客户端和改名保持同一对象标识；原名字替换与删除后重建必须得到新标识，旧有效句柄仍报告原标识。连接重建不恢复旧 FileId。身份竞态测试必须在查找与最终操作之间插入替换。名称冲突、符号链接与非法信息类均明确响应。
5. **Windows 访问语义。** 六种 disposition、metadata-only、目录打开、双向 ShareAccess、delete-on-close/pending、范围锁共享/排他/等待/取消、批量加锁冲突回滚和批量解锁部分成功、与各类 Linux 锁的独立及交互规则。至少覆盖“先解锁已持有 A、再解锁未持有 C”失败后 A 仍已解锁，以及非法后项不应撤销的先前授予。分别从 HTTP 和 FUSE 尝试绕过已授予的限制。
6. **故障与关闭。** 远端已执行但响应丢失、重复 MessageId/action、取消赢/授予赢/无法核对、gateway/server 重启、期限到达、notify 缺口/溢出、断线重建、busy 卸载、映射创建失败、清理超时。没有重复 mutation、假成功或悬空无限资源。
7. **package 验收。** 最小业务程序只通过公共 API 注入 backend/security/logger/listener 即可运行；New 无后台/系统副作用；不导入 SMB 的 Linux 程序无需其依赖；Windows 不依赖 Unix nativelease。两个 share 同时工作时独立 Publish/Unpublish，busy/超时/映射失败不影响另一个 share。持有句柄与锁时轮换本机及远端凭据，既有访问身份保持、新连接采用新凭据、刷新失败不伪装成功。示例保留真实凭据提供边界，不内置账号。
8. **资源与协议。** 畸形包、compound、credits、长度溢出、签名失败与重放 fuzz；慢读写客户端、耗尽 open/锁/notify/枚举预算；关闭后的 goroutine/socket/映射回收。未支持的 capability 不在协商中宣告。
9. **验证与性能。** Go 单测按 `xxx.go`/`xxx_test.go` 合并组织；正常和 race 覆盖真实错误路径，覆盖率按包统计，禁止 `-coverpkg`。Windows CI 使用固定系统版本并拒绝 skip。记录冷挂载、源码树列目录、小文件读写和远端 RTT；不以关闭同步写入或降低一致性换取吞吐。

Windows 系统缺失、引擎缺少必需行为或许可未决时，设计可以继续审查，不能把实现状态标为完成。实施时同步现有 `docs/design/`、测试说明与 package 示例；这份提案与实现一起移入 implemented 并按实际方案重写。

## 风险

最大风险是 Windows 内核缓存及本机认证行为尚未实测。UseWriteThrough、无 lease/oplock、禁止离线缓存必须一起通过真实程序验收；若仍无法满足既有保证，须带失败证据重新讨论，不静默改成最终一致或断线可读。

独立 package 并不意味着只改客户端。Windows 访问约束需要扩展共享 storage 契约、权威后端、wrappers、HTTP 与 authz 意图；可移植 replica 也需要依赖拆分。公共接口隔离限制了耦合，但不能消除这些必要成本。

SMB 引擎维护范围显著大于普通 HTTP handler，现有候选的协议版本标签不代表符合行为。引擎许可和采用维护分支还是自有实现仍待确定；商业授权也不自动补足锁与取消等缺陷。不得在这些结论未定时估计只有少量适配工作。

Windows 命名策略会限制启用该能力的 volume；不兼容既存目录不能自动修复。UAC 映射可见性可能需要明确的按用户部署与提权步骤，但不能以全局映射暴露业务凭据。同步确认和禁用缓存会放大远端 RTT 的影响，性能改进必须保持同样的可观察保证。
