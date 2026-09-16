# Agent Note: 将平台文件系统语义隔离到客户端

Status: proposed

## 问题

系统需要把同一份远端 volume 呈现给 Windows、Linux 和编程调用方。平台协议可以不同，但同一对象的身份、修改结果、用量和访问冲突不能因入口不同而互相矛盾。平台适配如果进入公共 storage、HTTP 和数据库，就会要求每个实现理解它不服务的平台；如果全部改成本机状态，又无法阻止其它客户端绕过已经生效的限制。

现有 Windows 接入把 WindowsStorage、WindowsSession、WindowsFile、命名启用、共享模式和专用 HTTP 动作放进远端权威链路；Linux 的 FileSession 也携带 flock／POSIX owner 与转换规则。仅把 Windows 类型改叫 native，或把两个协议都搬进一个更大的 interface，不能形成平台边界。现有 FileSession 加显式 S/X 也不足以表达有界目录视图、原子父身份条件、跨入口访问声明和精确的范围状态转换。

本提案替换的是平台解释所在的位置，以及承载它所需的公共原语；不是把所有流量改走 SMB，也不是取消跨客户端的约束。[现有 Windows 决定](../../implemented/feature/2026-09-15-windows-local-smb-package.md)保留已实施方案及其代价。[当前设计](../../../../docs/design/architecture.md)继续描述实际代码，本提案尚未实现。

## 提案

### 平台适配与共同权威

所有操作系统词汇由客户端解释。SMB 负责 Windows intent、CREATE flags、status、名字比较、路径表示限制、DOS metadata、删除规则和范围批次；FUSE 负责 Unix mode／UID／GID 的呈现与解释、flock、POSIX 进程 owner、关闭事件和锁转换。remote 不根据调用方平台分支，不导入 Windows UTF-16 限制或 Linux LockFamily／PID 规则。

共同权威保留节点种类、身份、内容、大小、通用时间事实、版本、有限会话与动作结果，以及任何调用方都必须遵守的访问声明和受约束范围。业务授权仍独立保护 Read、Write、ModifyMetadata 等通用操作，不能因权限 metadata 由客户端解释就允许绕过服务端授权。

```text
Windows 程序 -- 系统 SMB --> Windows 客户端内的 SMB server
                              | Windows 规则、平台错误与名字比较
Linux 程序 ----- FUSE ------> FUSE 客户端适配
                              | Unix 呈现、进程 owner 与关闭规则
编程调用方 ------------------+
                              |
                    一份通用 FileSession 契约
                              |
                   既有 file HTTP 协议与 registry
                              |
                节点 / 目录版本 / 原子操作 / claims
                              |
                 storage 与持久 metastore 权威
```

| 现有内容 | 目标归属与变化 |
|---|---|
| WindowsStorage／WindowsSession／WindowsFile | 由既有 FileSession 的通用保留对象和条件操作承载；SMB 保存自己的平台适配对象，不新增另一套远端 session |
| `/v3/windows`、`/v3/windows-control` 与 Windows registry | 在实施时移除，扩展既有 file 动作、codec、registry、receipt 和 control admission |
| WindowsOpenIntent、WindowsFailure、DOS 与 Windows 命名规则 | SMB 本地类型与转换；远端只接收已解释的通用意图、精确名字和条件 |
| flock／POSIX LockFamily、PID 和进程关闭规则 | FUSE 本地 owner 映射与状态转换；远端只接受不透明 owner／冲突域和通用范围 |
| `Attr.Mode` 混合节点类型与 Unix 权限位 | 拆为通用 NodeKind 与有界、版本化的 opaque metadata；Unix mode 位、UID/GID 的含义由 FUSE 解释 |
| Windows creation/change time、symlink target | 采用通用节点时间事实和作为数据存储的符号链接目标；不保存 Windows 专用 SQL 列 |
| DOS／平台权限 metadata | 有界 opaque payload，与节点修改原子提交；remote 不解释其中的 Windows 或 Unix 字段 |

平台 metadata 不是授权令牌。其它客户端创建的节点可以缺少可选的平台 payload；这是正常状态，适配器采用明确、可配置并有文档的缺省呈现，例如从通用 NodeKind 推导 DOS 基本类型位，或按显式挂载策略呈现 FUSE mode。缺省权限呈现不授予远端通用访问，缺失的历史时间仍为未知，不能编造为创建时刻。已经存在的 payload 若版本未知、损坏或无法表达，适配器明确失败，不能套用缺省值掩盖坏数据，也不覆盖自己无法解释的其它 metadata。通用 payload 的大小、版本条件和授权仍由 remote 检查。

### 一组固定原语，而非可编程事务

下表是既有 FileSession 的目标演进形状，名称用于说明责任；不另立 Windows 对应接口。每项都须有独立于 Windows 的用途和有限资源语义。

| 原语 | 固定语义与非 Windows 用途 |
|---|---|
| 保留 file／directory／symlink | 引用绑定对象身份，名字变化不重开；目录相对操作、备份与文件管理器也需要它 |
| NodeMetadataRevision／DirectoryRevision | 前者覆盖节点 metadata，后者覆盖目录名字与身份关系；避免编辑器或文件管理器根据旧视图覆盖新状态 |
| 有界 `ListAt(parentRef, revision, cursor)` | 在同一目录版本下分页；版本变化明确冲突，不能拼接几份目录视图 |
| 有界 EntryLocation／ancestor witness | 一致返回当前 entry 位置、祖先身份与目录版本，并与节点 metadata／link target 同时捕获；用于受限根内的路径操作、改名后的名称显示与审计 |
| `RetainAt`／`CreateAndRetainAt` | 对精确父引用、名字字节、预期节点和目录版本作一次有序决定；需要时将初始 metadata、claim、引用与预备删除意图一并提交 |
| 固定 AtomicReplace／Rename | 原子验证来源、目的父身份、名字、节点及版本条件，并完成固定替换；open disposition 要求的内容重置、初始 metadata、claim 与引用也在同一提交中，不能随后无条件 Truncate |
| 条件 metadata 更新 | 以节点版本更新通用字段与 opaque payload；用途包括平台 metadata、应用标记和索引状态 |
| 不可变通用变更事实 | 保留事件时 Kind 与有界前后 metadata／revision 图像，删除后仍可分类；索引、审计与各平台通知均需要它 |
| AccessClaim 与范围集合 CAS | 所有入口遵守共同访问顺序，客户端不需要改写其它 owner 的状态 |
| PrepareRemoval／DrainEntry | 给引用安排持久的退役后清理，或停止新使用并等现有使用者结束；后台对象清理也有此需求 |

server 不接收比较器、脚本、任意操作列表或 on-close callback。条件是协议明确列出的身份、版本、claim 和容量事实；原子操作只执行自己固定定义的转换。通用时间与 symlink target 由原生事务保存，平台方不以本机时钟猜测权威状态。

结果跨层只传递值。当前 `metastore.WindowsResult` 嵌入含有 `storage.WindowsFile` 的 `storage.WindowsActionResult`，又持有 nativeReference；目标 metastore receipt 只含动作状态、效果数量、版本和不透明 reference ID。metastore、storage 和 HTTP 各自解析所属层的引用，metastore 不持有负责传输字节的 storage interface。HTTP DTO 仅完成必要的线格式转换，不能再产生第二份动作状态机或 registry。

### 名字、目录观察与符号链接

[R-FS-9、R-INT-8 与 R-INT-14](../../../../docs/spec/requirements.md)将 Windows 名字解释限定在 Windows 入口。目标 Publish 不调用持久 EnableWindows，也不安装全 volume 的 Windows 命名 profile。HTTP、FUSE 和 SDK 可以创建 Windows 无法表达的名字；Windows 入口不得为容纳这些名字而隐去条目、自动改名或删除数据。

SMB 在受影响目录上执行有界、同一 DirectoryRevision 的完整分页观察，在本地检查 UTF-8、大小写等价、保留名及 Windows 路径表示。查询一个名字也可能需要检查整个父目录，才能判断大小写匹配是否唯一。目录版本失效就中止这份观察，以有限重试和调用期限重新获取；缓存必须经权威版本确认，不能把旧目录视图当作当前状态。Publish 不要求预先遍历整个 volume。另一入口随后写入不能表示或产生歧义的名字时，下一次相关 Windows 名字操作、列举或通知明确失败。

已经保留的对象仍按身份绑定。无需重新观察名字的内容 I/O 和属性查询可继续；需要返回名字的 metadata 查询必须经过同样的可表示性检查。节点内容与名称投影的失败不能互相冒充。远端只限制通用请求、条目、payload 与分页资源，不使用 Windows UTF-16 阈值作为公共名字约束。

保留引用通过有界 EntryLocation 取得当前 entry 名字、父身份与到指定根的祖先 witness；这些事实与所用节点 metadata revision、symlink target 在同一一致观察中捕获。witness 覆盖每一段 parent／entry 关系，以及本地名字唯一性检查实际使用的所有父 DirectoryRevision。SMB 仍以这些版本执行有界目录观察；名称相关的最终操作原子验证这份固定身份／版本条件，不能只检查最后一个父目录。祖先被移动，或其它入口在祖先旁插入大小写冲突条目，都使旧条件失效，即使最后一个父目录自身未变。路径型查询在返回前也须确认这些观察仍属于同一有效版本。

这是一份有深度、条目和字节上限的位置证明，不是全 volume 图快照。remote 只验证精确字节、身份、祖先关系与版本，不执行 Windows 比较。引用已 detached、祖先超限或版本冲突时明确返回相应状态，不能用旧路径补造当前名字。纯身份 I/O 不携带这类名字条件，也不会因无关名字变动重新打开对象。

symlink target 在 remote 中是有界数据，不携带 UNC 或宿主路径执行含义。SMB 在本地解析目标，通过保留的父目录身份和精确 lookup 逐段遍历，结合一致的位置／祖先 witness 与最终条件将结果限制在 Export 内；绝对路径、UNC、父目录跳转和循环不能逃逸到宿主或另一 Export。FUSE 独立决定 Unix 目标的解释。目标遍历语义及其错误需要平台验收，不能仅凭通用 target 字段宣称现有 Linux Readlink 已获得支持。

通知同样不回查当前节点猜测过去。通用不可变变更记录保存事件时 NodeKind、前后 EntryLocation／祖先事实及有界 opaque metadata 和 revision 图像；被删除、替换或后来修改后，所需图像仍由保留历史拥有。SMB 本地据此判断目录符号链接及 Windows filter，remote 不保存 Windows 推导的 Directory 标志。若图像缺失、损坏或越过历史／资源边界，通知明确失效，不能丢掉条目或用后来 metadata 替代事件时事实。

### 跨入口访问声明

`AccessClaim{Uses, Excludes}` 描述实际使用与排斥的通用资源行为。对同一资源，新旧声明冲突当且仅当 `new.Uses & old.Excludes != 0` 或 `old.Uses & new.Excludes != 0`。普通保留引用登记实际 Uses、Excludes 为零；无引用的短暂读、写、删除也在开始效果前取得原子 admission。长操作发布最终效果时须再次与新取得的声明排序，不能在先检查、后提交之间绕过冲突。

| 通用行为 | claim 分类边界 |
|---|---|
| 读取节点／stream 内容 | ReadContent；只查询 metadata 不算内容读取 |
| 写入或重置节点／stream 内容 | WriteContent；只修改 metadata 不算内容写入 |
| 删除或替换受保护的目录项 | RemoveEntry；按目标 entry／node 的固定关联规则判定 |
| 插入子名字、读取或修改 metadata | 独立的 NameInsert／Metadata 授权与并发条件，不自动登记父目录 WriteContent |

目录的 DirectoryRevision CAS 是并发事实，不是共享访问位。创建子文件不能因父目录被以不允许 ShareWrite 的方式打开就自动失败。SMB 将 DesiredAccess／ShareAccess 映射到实际内容和删除使用；metadata-only open 不凭空取得 ReadContent。FUSE 与其它客户端同样登记自身实际行为。

例如 A 保留读引用并排斥 RemoveEntry、允许 WriteContent：另一 HTTP 写入可以成功，HTTP 删除必须冲突。显式 S/X 锁、访问声明和 advisory ranges 分别回答排他控制、实际使用相容性和协作锁问题，不能相互替代。R-CC-14 的跨入口保护由共同权威执行，平台适配不能依赖其它入口自愿遵守 Windows 规则。

### 范围状态与有序批次

remote 提供有界范围快照，以及对调用方自有集合的 CAS。每次 acquisition 有独立、不透明的 ID；集合保存每项及其 multiplicity，不按相同或重叠区间合并、去重。相同范围的两个 shared acquisition 只解除一个后，另一个仍约束冲突写。SMB 本地选择精确 unlock 所对应的 acquisition，FUSE 可在本地按 POSIX 规则规划合并后的替换集合，remote 不替平台消除身份。

guard revision 必须覆盖影响判定的其它 owner 范围、容量、过期与冲突域变化；调用方只能替换自己的集合。SMB 本地解释 Windows batch、精确 unlock 与平台错误，FUSE 本地解释 flock／POSIX 转换和进程关闭事件。advisory 冲突域与 owner 均为不透明 ID，remote 不解析 PID；强制范围在每次相关 I/O 中检查，未参与 advisory 的 I/O 不被 advisory 阻止。

SMB 先检查报文结构与条目数量，再按协议顺序解释每一项语义。从同一快照计算最终自有集合、已应用效果数量与平台错误后，CAS 才能确认该决定。以下部分效果不能被“整个批次原子成功或失败”抹掉：

| 本地批次路径 | 应保留的状态与结果 |
|---|---|
| lock 前缀成功，后来遇到 UNLOCK flag | INVALID_PARAMETER；此前成功的 locks 保留 |
| lock 遇到真实冲突并要求 FAIL_IMMEDIATELY | 冲突结果；本批次已取得的前缀回滚 |
| unlock 前缀成功，后来遇到 SHARED／EXCLUSIVE flag | INVALID_PARAMETER；此前完成的 unlocks 保留 |

这些顺序来自 SMB 的 [Processing Lock Requests](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/670c7eda-e683-4923-9477-414303959613) 与 [Processing Unlock Requests](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/79eb3c91-563b-4d48-a51c-0974f9d144f8)。generic receipt 只确认哪个状态版本和效果已提交，SMB 保留自己计算的平台错误。即使决定不改变集合，冲突答复也须验证 guard revision，不能返回旧快照上的冲突。

CAS 明确 NotApplied 且版本冲突时，客户端可在有界的快速重试内重新捕获、规划并使用新动作。结果未知时查询原 action ID，不得根据后来状态重新规划同一个已可能提交的动作。非阻塞竞争明确返回 Conflict；无法取得容量也明确失败，不能扩大集合或把未知结果当作零效果。

R-CC-13 的阻塞 advisory 等待不受任意 MaxRetries 截断。快速 CAS 重试耗尽后，转入通用 revision-change 等待／订阅，以有界 pending 记录、请求取消和 session 续期维持等待，条件改变后重新捕获。有效 session 下阻塞调用可以继续等待，不进行忙轮询。通用等待只报告版本变化、就绪或依赖冲突事实，不替平台决定授予；客户端取消、session 失效和未知动作仍须完成确切结果协调。普通非阻塞操作与目录重新观察保留明确的尝试次数／调用期限，不能把两类等待混为一谈。

共同 coordinator 以不透明 owner／resource 登记等待及其 revision 依赖，保留跨 session、跨文件的有界环检测，不能让两个客户端各自等待对方而失去已有的 POSIX 死锁检测。依赖登记、条件确认、取消与授予后的移除在同一有序边界发生；状态变化使旧依赖失效时重新核对，不能以旧图报告死锁或漏掉新环。确定成环返回通用 Deadlock，由 FUSE 映射 EDEADLK；登记容量或有界搜索不足以判定时明确失败，不报告无环。coordinator 不接收 PID 或 POSIX 转换规则。清理与 session 过期移除其所有等待依赖，避免残留边改变后来请求的结果。

### 预备删除与停止新使用

删除需要两种独立状态。Prepared 是绑定某个保留引用的持久退役意图，尚不禁止新引用或新子项；entry 自身则有 Active、Draining、Detached 生命周期。`PrepareRemoval` 在 Retain／Create 的同一事务内安装意图，`DrainEntry` 原子禁止新的引用并等待现有使用者退役。对于目录，Draining 还必须拒绝 NameInsert 和 rename-in，包括通过已经保留的父目录引用发起的操作；有效引用不绕过目录当前状态条件。它们服务于通用的“本次使用结束后清理”与“停止新使用、等使用者退出再删除”，不携带 Windows flag 或可执行 callback。

```text
引用:  Live + Prepared -- Close / finite expiry --> Retired
                              |
                              +-- 条件成立 --> 激活 entry drain
                              +-- IfEmpty 不成立 --> 消耗意图，不激活

entry: Active -- DrainEntry --> Draining -- 同一 entry 无引用 --> Detached
          ^                       |
          +-- CancelDrain(generation)
```

意图绑定稳定 EntryIdentity，并随 entry rename 保留；节点身份与 entry 身份不能互换。旧路径被新文件占用时，退役旧引用不得删除新文件。准备、激活与引用退役需要持久、可恢复的原子状态转换，进程崩溃和有限 session 过期走同一清理所有权；不能把 pending 状态只放在 SMB 内存。

PrepareRemoval 是已经通过通用 Authorizer 的一个固定删除动作，绑定 entry、reference 与条件；退役执行这份已接纳意图的剩余效果，不取得任意新删除权限。新的 Prepare、Drain、Cancel 请求各自检查通用授权；正常请求后来被拒绝，不表示已接纳的意图从未存在。取消 Prepared 的 token、drain generation 和 session／引用的寿命分别决定其有效性。最终效果仍检查当前 S/X、claims、完整性与 quota/accounting；清理结果未知时保留所有权和可查询状态。此动作不保存凭据，也不增加通用授权委托 API。

| Windows 动作 | SMB 本地判断与通用原子转换 |
|---|---|
| CREATE 带 FILE_DELETE_ON_CLOSE | 检查 DELETE 权限、readonly 与共享相容性；Retain／Create 与 PrepareRemoval 一起完成，entry 仍 Active，允许相容的后续 open |
| 上述引用 Close／到期 | 激活预备意图并退役引用；不重新检查 DOS readonly。目录使用 IfEmpty 的关闭时条件，非空则消耗意图并退役，不标记删除 |
| SetDisposition(TRUE) | 此刻检查 readonly、目录是否为空和共享条件；以对应 metadata／目录版本及 claims 条件立即 DrainEntry |
| SetDisposition(FALSE) | 清除当前 entry 的 drain；不取消任何引用自己的 Prepared 意图，后来 Close 仍可再次激活 |
| 最后一个相关引用退役 | 在同一 entry／link 无 open 时完成 detach；目录在最终效果处再次检查 IfEmpty，不递归删除后来出现的内容，也不会删除同名的新 entry |

CREATE 的时点遵循 [CreateFileW 的 FILE_FLAG_DELETE_ON_CLOSE](https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-createfilew)，显式 disposition 与关闭条件分别遵循 [MS-FSA SetDisposition](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-fsa/386d9ec5-e0f6-4853-b175-c05be01419e0) 和 [MS-FSA close](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-fsa/d142c93a-72bc-4b05-9d96-8e00371c3308)。SMB 先从一致快照判断平台资格，再把确切版本与通用条件送交原子操作；remote 不能重新实现 readonly 或 Windows share 分支。目录 IfEmpty 与禁止向 Draining 目录插入都是通用条件，由权威在相应最终效果处验证。Prepared 期间仍允许增加子项，关闭时非空按上表消耗意图而不激活 drain。

`CancelPrepared` 使用所属意图 token，和 `CancelDrain` 分开。后者要求当前 drain generation 与通用修改授权，可由另一个获授权引用调用，不局限于发起者；旧 token 不能撤销后来重新建立的 drain。当前 entry 只有一个可清除的 drain 状态，不用每个句柄的计数模拟 SetDisposition(FALSE)。关闭中发生未知结果时，同一个动作 receipt 决定是否已退役或激活；重新 open 不能恢复旧引用的资格。

### 会话、授权与 HTTP

沿用有限 session、action ID、结果查询与 fencing。共同协议要容纳 Retain、条件名字操作、metadata、claims、范围 CAS 和删除生命周期，但只使用既有 file registry、动作 codec、receipt 状态机和控制预算。业务授权接收通用意图、对象身份与作用范围；完整 Windows DesiredAccess／ShareAccess、Unix 权限位和进程 owner 不成为远端授权 API 的平台字段。

metadata revision 与 DirectoryRevision 不替代授权或有效 session。每次原子操作同时验证引用归属、代次、输入条件与 admission；发布过程保留现有真实错误和未知结果的 EIO／fencing 语义。重复 action 只能得到同一结果。取消后已经确认的效果按 receipt 返回，尚未确认的效果不能因客户端重新取得引用而伪装成旧 handle 仍有效。

原动作核对只在有效 session 与声明的历史窗口内承诺可重放结果。会话退役、服务重启或历史过期后，查询明确返回 retired／unknown；持久清理责任与动作答复的可用期限是不同事实，不能承诺永久查询到唯一结果。未知答复保持 EIO／fencing，也不允许用新 acquisition 冒充旧引用恢复。

解释客户端文件系统语义的 imports、系统调用和 build tags 位于客户端适配。公共 storage、metastore、HTTP 不依赖 SMB、Windows 客户端 API 或 FUSE 进程类型；后端仍可通过宿主 OS 的平台文件实现持久 I/O、fsync、flock 与 native lease，例如 localdisk 使用的 Unix 系统调用。这些后端机制不解释来访客户端的 Windows／POSIX 请求，也不迁入客户端。SMB 的 loopback listener、SSPI 身份、Export 边界和本机 mapping 所有权仍是 Windows 客户端职责；这次隔离不把所有 SDK／HTTP 调用改经 SMB。

### 有界资源与可观察性

保留对象、Prepared 意图、drain、claims、每项范围 acquisition、action receipts 和清理任务都有会话及全局数量／字节预算。admission 在分配、持久化或发送效果前取得；输入列表、opaque metadata、位置 witness、不可变事件图像、分页游标和结果 envelope 有固定公共上限。目录扫描设总条目／字节、页大小、重试次数与期限；碰到上限明确失败，不返回部分目录成功。平台更严格的名字和 UTF-16 限制由适配器施加，不能成为通用存储名字规则。

持久清理所有权贯穿请求取消、Close、session expiry、服务退出与崩溃恢复。已知未提交才可释放 admission；未知效果保留有界 receipt 和清理责任，无法在预算内确认时拒绝新工作。不可为了恢复容量提前忘记一个仍可能生效的 intent 或引用。

状态与诊断应能关联 session／action／reference／EntryIdentity、节点与目录版本、guard revision、drain generation、效果数量和 admission 用量。日志记录阶段、结果类别与经过净化的身份，不输出认证 token、opaque payload 或文件内容。追踪限于拥有的请求／进程，具备总大小、时间和清理边界。平台 status 保留在客户端，公共错误保留原始因果链；观察能力不能新增另一份事实权威。

验证时记录一个通用 SessionID／ActionID，在提交后丢弃答复，然后用原 ID 执行 QueryAction，核对唯一效果、Status 的 admission 计数和仍由谁持有清理责任。宿主配置结构化日志时，可用这两个 ID 关联 request → HTTP → storage publication；revision wait、renew／expiry 与 finalizer 的异步交接携带请求 context 或有归属的任务 ID。验证取消及最终清理后计数回归，未知阶段则保留预期责任。不得以无 context 的 goroutine 或全局 stdout 代替这一边界，也不要求新的遥测系统。

### 持久格式迁移

目标实现恢复 0001 与 0003 的原始迁移和历史 v1–v3 fixture，保留 0002、0004、0005；最终共同字段只进入新的 0006。现有未合并的 expanded v5 不成为兼容格式。此提案不改 SQL 或 fixtures，实施时必须先核对已发布历史内容，不能通过修改旧测试数据库伪造兼容性。

升级在一个受控事务中依次执行来源版本完整性检查、迁移和新格式完整性检查，包括真实 v5。新增通用 kind／metadata revision、目录版本、entry 身份、claims 与删除状态必须有可验证的来源映射；平台权限 metadata 迁移为明确版本的 opaque 值，不留 Windows 专用 SQL 列。提交前发现坏数据或迁移失败时回滚事务，确认回滚后保留来源库；不能在检查完成前提交迁移。Commit 结果未知、回滚无法确认或提交后 witness 发布失败时，沿用既有 durability 协议保留未知状态并 fencing，不能声称旧库已经恢复。

旧通知缺少完整祖先事实时不能补造历史。升级重置保留的通知历史及 incarnation，使旧游标明确失效，同时保留已有节点 ID 和分配 high-water；新历史从可证明的边界开始。验收必须覆盖空库、真实 v1–v5 升级、各来源坏库回滚、v6 重新打开，以及高水位、身份、内容、quota 与未知动作状态的完整性。

## 备选方案

| 方案 | 取舍 |
|---|---|
| 保持 server Windows 专用栈 | 已能集中执行共享与删除规则，但公共接口、HTTP、SQL 和其它实现持续承担 Windows 语义，与平台边界不符 |
| 把 Windows 类型重命名为 native／common | 名称变化不提供独立用途，也不消除 Windows flag、status、路径比较与删除时序，不能解决耦合 |
| 只保留现有 FileSession 与 S/X | 结构简单，但不能表达身份条件 open、共享访问、目录一致观察、有序范围部分效果和持久退役清理 |
| 让所有入口通过 SMB | 可复用一个平台解释器，但改变 SDK／HTTP 的部署与依赖，也使 Unix 行为受 Windows 协议约束 |
| 远端可编程事务、比较器或关闭 callback | 表达能力广，代价是远程执行环境、不可审计的状态转换及资源上界，超出固定文件契约 |
| 全部在客户端保存 claims、范围和 pending 删除 | 减少远端状态，但其它入口可绕过；客户端崩溃会遗失义务，不能满足跨入口和恢复保证 |

选择固定共同原语会扩大通用契约，但每项必须通过独立用途、线性化点、有限资源和恢复语义审查。不能为填满 Windows 操作表就引入一个没有边界的条件语言。平台规则变更应主要修改适配器；需要新增共同能力时，仍需证明它是共同事实而非隐藏的平台分支。

## 实施依赖

以下顺序描述实施依赖，本提案不执行其中的代码或数据库修改。

1. 固定共同数据形状与错误语义：NodeKind、opaque metadata、稳定 entry／reference、有界位置与不可变事件图像、各 revision、纯值 receipt、claims、含 acquisition 身份的范围快照和删除转换；逐项给出独立用途、权限与预算。
2. 实现通用权威与持久事务，包括条件内容重置／创建、短暂操作 admission、发布重检、Prepared／drain／退役，以及来源版本迁移和回滚；共同状态不可先由平台缓存代替。
3. 将既有 FileSession 与 file HTTP 的 codec、registry、动作结果、控制预算和授权推进到同一词汇。移除 Windows 平行协议以及远端 Linux owner 规则，逐层验证纯值 receipt 和引用归属。
4. 在 SMB 与 FUSE 映射平台语义，包含完整目录比较、有序范围部分效果、Unix mode／owner 和关闭行为。修改平台适配与真实跨入口回归须一起完成。
5. 在真实平台运行现有验收及下表补充情形，确认公共层无客户端平台语义依赖，再同步当前 design 和实现决定。缓存验收与大 I/O 保证单位的未决事项不能因结构完成被标记通过。

## 验收标准

| 验收族 | 必须可观察的结果 |
|---|---|
| 包与协议边界 | 公共层不依赖 SMB／Windows 客户端 API／FUSE 进程类型，后端宿主持久 I/O 依赖保持；一种 session／action registry；未知动作、非法 ID、错属引用和超限请求明确拒绝 |
| 属性与身份 | 原有 Linux mode、UID/GID 呈现、node kind、时间、rename／unlink 后引用行为保持；跨入口创建后缺少可选 payload 使用已定义呈现，历史时间不编造；已存在 opaque 值损坏明确失败，未知字段不被丢失 |
| 条件 open／replace | 在 lookup 后替换父、目的 entry 或节点；只能对预期身份提交。内容重置、初始 metadata、claim、引用与 PrepareRemoval 要么一起完成，要么无效果 |
| 名字与目录 | Windows casefold 冲突、非法 UTF-8、保留名、长路径和跨分页 rename 均明确失败或重试；其它入口仍可创建这些名字。保留最终父目录后再移动祖先或插入祖先同级歧义名字，旧 witness 必须失败；rename 后名称查询取得当前位置；无遗漏条目、部分成功或隐式改名 |
| symlink 与通知 | 相对目标、父跳转、循环、UNC、绝对宿主路径和跨 Export 目标；target／metadata 捕获后祖先变化不能逃逸。目录符号链接删除、替换及后来 metadata 修改仍按事件时图像分类；缺失／超限图像明确失败，不查询当前节点补造 |
| 访问声明 | 所有入口各自参与内容读／写／删除 admission；互换新旧顺序结果一致。读且 deny-delete 不阻止允许的 HTTP 写；metadata-only 和 child-create 不误占内容分享位 |
| 范围与等待 | 相同／重叠 shared acquisition 保留独立 ID，一次 exact unlock 后剩余项仍阻止冲突写；所有 batch 顺序分支及保留／回滚前缀；guard 冲突、容量变化与 expiry；旧快照不能报告无效果错误。跨 session／跨文件形成及解除死锁环，登记／取消／授予并发、有界搜索失败和依赖清理均有明确结果；阻塞等待跨多次 revision／续期保持，取消、关闭、非阻塞与 session 失效按原保证结束 |
| 删除生命周期 | CREATE 后相容再开、readonly 在准备后变化、Prepared 目录增加子项后关闭、Draining 目录通过旧父引用 create／rename-in 被拒绝、最终 IfEmpty 重检、FALSE 后其它 Prepared 重激活、跨引用 CancelDrain、旧 generation 拒绝、rename 与旧名替换、最后引用与有限 expiry 并发 |
| 失败与持久性 | 请求发送前、事务提交前后、receipt 答复丢失、客户端／服务器崩溃及清理失败；有效 session／历史窗口内相同 action 核对唯一结果，退役／重启／过期明确 retired 或 unknown；未知效果不返回成功或空值，也不释放仍有责任的容量 |
| 授权、强锁与 quota | 每种固定操作及延迟清理穿过相同授权／S/X／用量检查；被拒绝时无旁路效果，真实释放字节与清理责任一致 |
| 资源与并发 | 用真实 SQLite 和后端覆盖全局／会话预算、分页、等待者、opaque 字节、claims、范围和 intent 上限；race、取消及重复调用不得泄漏引用、goroutine 或持久清理义务 |
| 格式与历史 | 空库、真实 v1–v5、提交前坏库／迁移失败的原子回滚和 v6 reopen；Commit 未知与提交后 witness 失败保留未知／fencing，不伪报来源库已恢复；历史 reset 不伪造祖先，ID、高水位与保留对象正确；无 expanded-v5 特例 |

各包用自己的测试二进制计入覆盖率，保留正常／race、真实后端、错误路径及原有负载数量；Windows ARM 原生检查与 Linux race 的职责维持[测试策略](../../../../docs/testing.md)。原有跨入口、持久化、取消与失败断言不得仅因接口变化删除。受影响的缓存可见性必须由原生系统调用证明，协议响应和交叉编译不能替代。

## 风险

新边界使 Windows 不能要求其它入口遵守全 volume 命名 profile。Windows 操作可能因后来出现的不可表示名字或歧义而失败；这种失败显式暴露，既有身份数据不被改写。跨入口访问声明、强锁、范围强制、授权和未知结果保证仍由共同权威承载，不因平台规则搬迁而放宽。代价是 SMB 必须执行有界全目录观察、处理 revision 冲突，并与 FUSE 各自维护清晰的平台映射。

[原生 CI 34982701023](https://github.com/codetreker/remote-fs/actions/runs/34982701023) 中，Linux 两项检查成功，Windows 重启后认证／不同 SID 拒绝通过；`TestNativeWindowsHTTPBridge` 的一秒负查找可见性验收失败。此前 `live.bin` 已得到有效 V2 StateNONE lease，记录中没有出现远端创建后新的 `new.bin` CREATE；记录边界与四次 CREATE 的事实见[当前 Windows 验收](../../../../docs/testing.md)。后续专门的目录缓存阶段未到达，不能据此宣称它已验证。

共同原语与平台隔离不会自动修复该缓存结果。全局 TTL=0 未被批准或采用，主动缓存权利与 break 策略没有被本提案引入。该原生一秒门槛仍是交付条件；R-CON-5 在应用层大 I/O 被拆分时的保证单位也仍未决定，既有断线错误保证继续适用。实现迁移、Linux 行为保持和原生缓存验收必须分别给出证据，不能用其中一项成功替代其它项。
