# Agent Note: 平台客户端使用同一文件核心的中立能力

Status: proposed

## 问题

Linux 挂载与 Windows 本机 SMB 接入面对的是同一份 volume。已有文件引用必须在改名、删除名字和同名替换后继续指向原对象；另一入口取得的共享限制或范围锁，也必须约束本入口真正发生的访问。只在 SMB 进程里维护这些状态，会让远端的 Linux 与编程访问绕过保护。

平台规则又不能变成 volume 的规则。Windows 按自己的名字比较规则识别 `a` 与 `A`，Linux 可以在同一目录创建这两个名字。Windows 必须拒绝有歧义的名字观察，但一个先前打开的文件不能仅因邻近名字冲突就失去按身份访问的能力。远端需要执行可验证的身份、存在性、使用权限与原子修改约束，平台客户端负责决定什么约束对应本地请求。

现有 `FileSession` / `File` 已经提供稳定的文件引用、当前修订读取、同步范围修改、有限续期、关闭排空与动作核对。将平台接入同时变成另一套文件身份、内容提交或引用生命期，会迫使已经满足 R-FS-5 至 R-FS-8 的路径重新证明同一组保证。需要补齐的是目录父身份、原子打开的附加约束、元数据引用、跨入口使用限制和删除意图；这些能力须落到已有提交与清理顺序中。

## 提案

在 `packages/storage` 的现有文件核心上增加可选的中立能力。Linux 的 syscall、FUSE owner 解释及 Windows 的 SMB 请求、名字比较、访问掩码、状态码转换分别留在各自客户端包。远端只执行已归一化的命令，不解析某种平台协议。

[活跃文件句柄](../../implemented/architecture/2026-09-08-live-file-handles.md)继续拥有普通 File 的身份、内容和生命周期；[有限强占有](../../implemented/architecture/2026-09-07-file-locks.md)继续拥有强 S/X 的期限、最终发布检查和重启屏障。本提案具体化[文件目标与目录父身份](2026-08-20-nothing-pins-an-open-file.md)中的目录能力，显式内容版本比较仍由该提案单独拥有。

### 进入实现前的门禁

本提案明确通用能力的可实现契约，Windows 集成仍有两项前置条件。广泛铺开 SMB/backend 修改前，先用聚焦的 Windows 11 24H2+ 原生探针确认 negative-name 缓存的实际行为和满足一秒、非 TTL 可见性的可行路径；已有失败不能留到最后才重跑。该探针必须经过真实重定向器、远端创建和负查找，记录请求及通知时序，不能由模拟客户端或不授予 data lease 推导成功。没有可行证据时，Windows 集成尚不具备实现准入条件，需要明确产品决定；不自动引入全局 TTL、驱动或 lease-break 扩展。

旧 volume 的 BirthTime/ChangeTime 显示政策也须先明确，随后以实际 SMB 查询与资源管理器验证可用性影响。新建节点维护中立真实时间与这个历史显示选择分别成立。通用能力可以按自身独立契约审查，不因其设计完成就宣称 Windows 目标已经可交付。

### 范围与代价

| 需求与事项 | 分类 | 本提案的范围及延后代价 |
|---|---|---|
| R-FS-5 至 R-FS-8、R-WS-5 至 R-WS-7：身份、同步修改、配额、退役排空 | 保证 | 复用当前实现并增加组合验收；不得以平台改造替换现有 Byte I/O 或弱化未知结果隔离 |
| R-FS-8、R-FS-9：父身份、原子打开、客户端名字判断 | 保证 | 增加父 NodeID 加原始叶名的权威操作，客户端依赖目录观察时携带原子检查；名字判定留在客户端 |
| R-CC-12 至 R-CC-14：advisory、共享限制、强制范围保护 | 保证 | 保留命令式 owner、等待与动作历史，引入中立冲突规则；所有远端入口在实际访问处检查 |
| R-INT-8、R-INT-14：Windows 11 24H2+ 本机 SMB package | 功能 | 明确接口映射和真实系统验收；实现由独立平台包承接，依赖不能进入 Linux 包 |
| 元数据与目录引用、删除待定效果 | 保证 | 归入已有 FileSession，删除效果使用持久元数据；不得以关闭失联为由撤销已确认删除 |
| 第三种客户端与其它传输 | 形态 | 延后时只增加适配与协议编码；不能把 FUSE、SMB 类型放入存储、原生元数据或通用传输 |
| 普通 Byte I/O 的通用结果查询、全操作 receipt | 功能 | 继续由既有未决写入工作负责；普通未知写入保持 `EIO` 与 fencing，不产生成功推测 |
| 大应用调用的跨请求一致性 | 未决需求 | R-CON-5 保持未决；不得以单次请求测试证明整个应用调用，也不新增跨请求快照或事务 |
| SMB durable/persistent handles、旧版 Windows、第三方驱动、全局缓存 TTL | 功能或产品目标之外 | 请求不支持的功能明确失败；既有 NodeID 与 FileSession 不绑定平台连接，可保留日后另行设计的空间 |
| oplock、lease cache break 与客户端数据缓存 | 功能 | 不启用需要这类承诺的缓存授权；真实 Windows 无缓存路径若仍不能满足可见性，交付门禁失败并重新决定范围 |

### 分工与扩展成本

```text
Linux FUSE                  Windows 本机 SMB                 SDK
  inode / kernel owner        名字比较 / SMB handle / status    显式选择能力
           \                      |                         /
            FileSession + File + 可选中立能力
                   | 当前 HTTP 文件控制与数据路径
                   | replicated / limited / locked
                   | objectstore File + advisory coordinator
                   | 原生元数据事务 / 发布许可 / retained nodes
```

`File.ReadAt(ctx, offset, length) (FileRead, error)`、`WriteAt(ctx, offset, data) (Attr, error)`、`Truncate(ctx, size) (Attr, error)`、`Stat`、`SetAttr`、`Sync` 与 `Close` 的签名和现有成功含义保持不变。`ReadAt` 的 Attr、内容与 EOF 来自同一捕获状态；零长和短读仍服从现有契约。方法内部携带已经绑定于该 File 的中立使用者身份，调用者不需要每次传入平台句柄。

| 增加一种能力时改哪里 | 必须承担的成本 | 保持稳定的部分 |
|---|---|---|
| 新平台名字规则 | 平台客户端的比较、校验和错误映射；复用目录观察与 guarded 命令 | 服务端不增加平台名称、大小写折叠表或 volume 模式 |
| 新访问限制 | 中立 access mask、冲突判定与全部受控入口的测试 | NodeID、内容对象格式、File 读写签名与同步确认 |
| 新锁适配 | 客户端 owner/关闭映射及中立命令表达所需的行为 | coordinator 的请求身份、等待、Query/Cancel、历史容量及隔离机制 |
| 新传输 | 可选能力协商、类型编码和现有控制路径的动作核对 | 原生事务、引用生命周期和所有权；连接不是 owner |
| 新后端 | 原子能力、真实发布许可、引用保留和计量证明 | 不要求实现 SMB/FUSE；缺能力时构造失败 |

能力检查在发布挂载或 share 之前完成，并沿包装链验证到底层。不能靠接口断言成功就宣称 native backend 具备原子性；每项能力都有契约测试。缺失能力返回明确不支持，Windows package 不通过多次 `Stat`、路径重开或本地 mutex 模拟原子远端能力。

### 身份、名字与附加能力

NodeID 继续使用 `Attr.ID` 的 volume 内身份。目录项由父 NodeID 与原始叶名定位，期望目标由现有 NodeID 检查；硬链接不在目标内，因此无需独立分配 EntryID。叶名是未经平台规范化的字节串，传输必须无损编码，不能让 JSON 的无效 UTF-8 替换改变实际名字。空叶名、分隔符和越界长度在能力入口拒绝。

以下是供实现与契约测试使用的接口形状；附加能力由 `FileSession` 的独立接口提供。指针字段表达明确有无；条件枚举使用有效的非零值，不能让零值暗示另一种权限或存在性条件。

```go
type ChildName struct {
    Parent DirectoryTarget
    RawLeaf []byte
}

type ChildCondition struct {
    State ExpectedChild // Any, Absent, SameNode
    NodeID uint64       // SameNode requires nonzero
}

type DirectoryObservation struct {
    ParentID uint64
    Revision []byte     // opaque equality token for the name set
}

type NamespaceGuards struct {
    Directories []DirectoryObservation
    Edges []ObservedEdge // parent ID + raw leaf + expected child ID
    RootID uint64        // anchor of the observed path, when supplied
}

type DirectoryTarget struct {
    NodeID uint64
    Scope *UseScope // present for an operation on a retained handle
}

type RenameTarget struct {
    Parent DirectoryTarget
    ObservedLeaf []byte
    Expected ChildCondition
    OutputLeaf []byte
}

type OpenAtOptions struct {
    Read, Write bool
    Create, Exclusive bool
    Target ChildCondition
    Guards *NamespaceGuards
    Use    UseClaim
    Existing ExistingEffect // Keep, ResetContent, ReplaceNode
    Initial InitialState
    CloseIntent *CloseIntent
}

type InitialState struct {
    OnCreate InitialFields
    OnReset InitialFields
    OnReplace InitialFields
}

type InitialFields struct {
    Attr AttrChange
    Metadata map[string][]byte
}

type OpenResult struct {
    File File
    Attr Attr              // captured by the atomic open
    Outcome OpenOutcome    // Opened, Created, Reset, Replaced
}

type AtomicFileOpener interface {
    OpenAt(context.Context, ChildName, OpenAtOptions) (OpenResult, error)
}

type NamespaceAccess interface {
    ReadDirNode(context.Context, DirectoryTarget) (ObservedDirectory, error)
    MutateName(context.Context, NameCommand) (NameResult, error)
}
```

`ReadDirNode` 在一次权威状态捕获中返回原始名字、各条 Attr 与 DirectoryObservation；身份为目录、当前访问获准、列表完整且资源预算足够才返回成功。完整列表的内存与编码大小受显式上限约束，超过上限明确失败；不在这里引入目录分页快照。观察令牌只说明一个目录名字集合，不能用作内容版本、Session revision、授权 proof 或全 volume 日志位置。

DirectoryTarget 有两种明确准入。由已有目录 handle 发起的操作携带 Scope，原生入口验证该引用仍有效、属于当前 session、绑定所给 NodeID 且有相应用途；目录枚举要求 ReadEntries，名字修改仍接受其实际语义的业务授权；关闭、到期、fencing 或身份不匹配即失败，不能丢掉无效 Scope 再退回裸 ID。裸 NodeID 是一次新的、经业务授权的身份操作，原子核对当前节点种类与 linkedness，目录不存在或已 detached 即失败；它不继承任何引用自我豁免。FUSE 依当前父 inode 发起的 mkdir/lookup 可使用这个入口，不要求为每次操作创建临时目录引用。FUSE 的目录 handle 操作则持有并传入对应引用。

ReadEntries 是独立的中立目录用途。NamespaceAccess 绑定既有 FileSession 及其 store/volume；“匿名”仅表示没有引用 Scope，不表示没有身份认证或业务授权。配置授权时，HTTP 在读取前以可信 volume 和规范的目录读取 Operation 调用既有授权器；直接 Go 调用遵守宿主已有授权边界。请求 context 承载这些身份与授权信息，不新增 mandatory actor 对象、request ID 或临时 owner registry。

服务器由 ReadDirNode 操作本身确定 Uses=ReadEntries，调用方不能传空 Uses 来跳过检查。无 Scope 时在 native gate 下检查该 NodeID 的全部 claims，同一 session 的 deny 也不豁免；检查通过后在同一顺序捕获有界目录列表，后来的相冲突 grant 不能插入检查与捕获之间。带 Scope 时验证属于当前 session 的确切目录引用仍有效、NodeID 一致且具备 ReadEntries，只豁免这一个引用自身的 claim，其它引用的 deny 仍然有效。外来、关闭、失效或不匹配的 Scope 明确失败，不回退成无 Scope 请求。

Windows adapter 将平台的列表目录权限/共享规则映射成 ReadEntries 及相应 exclusions，纯属性查询不自动取得列表权限。fresh 裸 ID 操作每次独立接受授权、linkedness 和上述匿名用途检查，不从曾经打开过该 NodeID 推导权限。

OpenAt 的 Create/Exclusive 决定允许不存在及排他创建，Target 进一步约束精确名字的身份或缺失；不相容的组合在接纳前拒绝。Reset 要求 WriteData，ReplaceNode 需要创建权限以及对被替换对象的 DeleteName 权限；允许继续的 Keep 不应用初始化字段。

`NameCommand` 是有界 tagged union：创建目录、创建符号链接、删除一个名字、移动/替换一个名字。每个父目标用 DirectoryTarget 表达，实际被修改的槽位使用原始叶名与 ChildCondition；创建携带初始属性/metadata，符号链接还携带原始目标字节。源缺失、目标替换、父身份失效、类型错误、目录非空、按 NodeID 祖先关系检测的环形移动及不允许的替换均在同一权威操作内拒绝。响应返回实际 NodeID 和修改结果，不能靠后续 Stat 拼出成功。

rename 的目标分开表达 ObservedLeaf/Expected 和 OutputLeaf。Windows 可能观察到目标目录中的 `bar`/NodeB，却要求结果叫 `BAR`：前者指定允许移除的精确槽位与身份，后者指定写入的原始名字；服务端不做大小写折叠。最终事务分别验证源槽位、被覆盖槽位和输出槽位；OutputLeaf 若与 ObservedLeaf 不同，只允许为空或属于本次已验证的源/被覆盖节点，任何第三个占位者都使操作明确未执行。大小写改名可以识别源与目标为同一 NodeID，仍不能跳过第三占位者检查。输入 alias、重复目标及不支持的替换组合在改变前拒绝。

OpenAt/OpenChildRef 的 ChildName.Parent 同样可携带保留目录的 Scope，最终打开事务验证该父引用；客户端不得把父 handle 的失败降成一次 fresh 裸 ID 打开。`OpenAt` 把父身份验证、叶名解析、Target/Guards 比较、访问共享检查、创建或截断、初始属性、保留引用与动作成功放在同一个有序操作中。既有 `OpenFile`、`OpenNode` 的普通文件限制与非排他 Create 打开竞争胜者的语义保持成立；Windows CREATE disposition 由客户端选择对应命令条件，不能由后端猜测。`OpenResult.Attr/Outcome` 在原子打开内捕获并随现有动作结果保留；随后 File.Stat 观察的是当前状态，不能代替打开结果。ResetContent 保留 NodeID 并清空内容；ReplaceNode 新建 NodeID、按既有 discard 规则保留仍被 pin 的旧节点与其 charge。

| SMB disposition（仅客户端解释） | 中立存在条件与效果 | 成功结果 |
|---|---|---|
| OPEN | 必须存在，Keep | Opened |
| CREATE | 必须不存在，新建 | Created |
| OPEN_IF | 存在 Keep，否则新建 | Opened / Created |
| OVERWRITE | 必须存在，ResetContent | Reset |
| OVERWRITE_IF | 存在 ResetContent，否则新建 | Reset / Created |
| SUPERSEDE | 存在 ReplaceNode，否则新建 | Replaced / Created |

InitialState 明确区分新建、重置和替换时应用的 AttrChange 与 metadata namespace；Keep 不修改这些字段。未被选中的分支不得产生效果，不支持的字段在任何改变前拒绝。Linux 初始权限由客户端编码到对应 InitialFields.Metadata 的 POSIX namespace，每个效果只有一份初值。Reset/Replace 的属性处理由客户端提供受支持的初始或替换字段，与实际内容效果处于同一 native open callback；不能通过后续通用 SetAttr 补齐，因为现有多字段 SetAttr 允许部分完成。元数据条件只有对应明确 disposition 需要时才加入这个原子命令。

Windows 在路径遍历中对实际依赖的每个目录取得完整名字观察，检查表示与歧义，然后将所用 DirectoryObservation 与观察到的父子边组成可选 NamespaceGuards。它覆盖本地判断实际依赖的祖先，不能只校验最终一两个父目录：祖先目录在途中被改名、替换或产生 Windows 名字冲突，同样可能使整条路径的解释失效。需要限制在某个根下的请求携带 RootID 及从该锚点观察到的边，服务端在同一次原生事务中核对节点、边和目录修订的当前关联。

每个请求的 guard 数、边数、总字节数受显式配置上限约束，重复目录去重后必须具有相同令牌；超过预算明确失败。服务器只比较中立事实，不执行 Windows 名字规则，不持有完整祖先 EventImages。调用者未依赖某项目录观察时不发送它；按已保留对象身份进行且不依赖路径解释的操作也不重新遍历祖先。

Guard 失效证明本次未执行，客户端在有界尝试及请求截止时间内重新观察；持续变化则明确失败。所有 guard 在本次效果之前一起验证，不能因为本次 rename 改动了自己的观察集合而产生伪冲突。枚举遇到不可表示或有歧义名字时，整次受影响枚举失败，不能返回删减后的成功列表；名字通知保持同样的可表示性检查。

Linux 精确叶名访问默认不携带目录 Guard：`Create(parentID, "x", Absent)` 只检查该父与该名字，无关 sibling 变化不能让创建失败。持有目录 inode 后发生父路径改名或替换，操作仍只访问该目录 NodeID 或明确返回失效，绝不沿旧路径访问替代目录。需要名字策略的客户端承担观察冲突成本，这项成本不扩大到其它入口。

### 元数据与目录引用

`File` 继续表示可读写普通文件。元数据打开和目录打开使用同一 FileSession 下的可选 `NodeReferences`，不放宽普通 `FileOpenOptions.Check` 的读写要求：

```go
type NodeReference interface {
    Stat(context.Context) (Attr, error)
    SetAttr(context.Context, AttrChange) (Attr, error)
    Close(context.Context) error
}

type NodeOpenResult struct {
    Reference NodeReference
    Attr Attr
    Outcome OpenOutcome
}

type NodeReferences interface {
    OpenNodeRef(context.Context, uint64, NodeRefOptions) (NodeOpenResult, error)
    OpenChildRef(context.Context, ChildName, NodeRefOptions) (NodeOpenResult, error)
}
```

`NodeRefOptions` 的字段为 Kind、Target、Guards、Use、MetadataAccess、Create、Exclusive、InitialState 与 CloseIntent；它明确节点种类、目标条件、可选 NamespaceGuards、使用声明、属性权限及与 OpenAt 相同的初始字段。创建并打开目录也走原子命令，返回同一次捕获的 Attr/Outcome，不能先创建再补引用而报告完整成功。目录枚举通过已保留目录的 NodeID 发起。没有内容权限的引用不能获得 File 字节方法；仅查询属性的打开不虚构数据读取权限，也不因此取得强 S/X 或 advisory 锁。

NodeReference 使用现有 session registry、上限、续期、发布 fencing、并发准入、Close 排空及 HTTP 操作记录。它在 registry 中是引用种类的扩展，不建立另一套 session、租约或编号空间。有效引用保留被删除的对象身份；已删除目录不能继续创建子项，其属性查询可以继续访问原对象，按名操作明确失败。元数据引用和目录引用对最终回收计数，关闭未知时不提前释放其计量。

需要查询打开对象状态的客户端使用可选的 ReferenceStateAccess，在一次 native 状态捕获中返回该引用的 Attr、Detached 和当前 PendingUnlink/token。它复用已有 metastore.FileState.Detached 并增加读取 pending 元数据，不把这些状态铺进全部 Attr 或每次 Byte I/O：

```go
type ReferenceState struct {
    Attr Attr
    Detached bool
    PendingUnlink bool
    PendingGeneration []byte
}

type ReferenceStateAccess interface {
    State(context.Context) (ReferenceState, error)
}
```

File 与 NodeReference 可暴露此能力，查询先验证引用/session 存活和本次读取授权；关闭、退役或不支持明确失败，不能按当前路径补查，也不能默认 false。Windows adapter 从这些真实事实映射自己的 DeletePending/标准信息；armed CloseIntent 不等于已经 PendingUnlink。需要同一次查询的大小与状态时使用返回的 Attr，不能拼接另一时刻的 File.Stat。

Attr 使用中立的 `NodeKind` 表示普通文件、目录和符号链接，POSIX 权限编码在 Linux 客户端拥有的 metadata namespace；现有 ID、Size、AccessTime、ModTime 的事实及字节 I/O 返回位置保持不变。Linux 适配层将 NodeKind 与该 payload 映射成 `fs.FileMode`，Windows 适配层作自己的属性映射。不会为了泛化而增加当前并未提供的 UID/GID 行为。

平台类型清理包含下列必要的公开参数变化，不能只移动 package 却把 POSIX 解释留在远端：

| 现有位置 | 中立形状及调用迁移 |
|---|---|
| Attr.Mode | 替换为 NodeKind 与有界 opaque metadata；Linux 类型/权限转换留在适配器 |
| AttrChange.Mode / SettableMode | 从通用属性变更移出；mode 修改成为 POSIX namespace 的 payload 更新，权限位有效性由 Linux 客户端检查 |
| FileOpenOptions.Mode | 替换为新建时的 InitialMetadata；普通 OpenFile/OpenNode 保持原身份、非排他创建和同步效果，沿同一 native callback 执行 |
| Storage.Create/Mkdir 中的 `fs.FileMode` 初值 | 使用中立创建属性（共同时刻与 InitialMetadata）；节点种类由操作本身确定，权限初值由调用者编码 |
| File 上的 Linux 锁方法及 FileLock/Flock/POSIX/PID/Errno | 移入本提案的可选中立 RangeControl 与结果；FUSE 本地解释 Linux 行为 |

这些是对公开类型的明确迁移，不提供保留旧平台语义的兼容 alias。既有 ordinary 调用改传中立初值后仍走原文件、发布与回收管线；File 的 ReadAt/WriteAt/Truncate 参数、返回 Attr 的位置及行为不变。File.SetAttr 的共同时间变更仍保留现有保证，修改 POSIX payload 通过可选 metadata 能力完成；组合更新的 atomicity 只在明确要求的原子 Open/条件修改中加强。

平台确需保存、共同事实又无法表达的属性使用有界命名空间 payload：`map[namespace]OpaquePayload`。Windows package 拥有版本化的 `windows.file.v1`，保存 DOS 属性等平台字段；自动维护的创建/变更时间不放在这个 blob 中。服务器只检查 namespace、字节数、条件版本及操作准入，不解释 DOS 字段。缺失 Windows DOS 扩展位表示无额外位；目录按 NodeKind 加 DIRECTORY，符号链接加 REPARSE_POINT，普通文件最终无位时使用 NORMAL，不从 POSIX 位推导 READONLY 或默认增加 ARCHIVE。存在但无法解析的 payload 必须报错，不能当作缺失或替换成默认。修改只触及指定 namespace，保留其他 namespace 键与内容。

Attr 增加中立的可选 BirthTime 与 ChangeTime，缺失明确表示权威端没有该事实。未知与任意合法时间值分开编码；这两个时间都不是版本。所有新建入口在实际创建节点的同一个事务中记录 authority time，包括 volume 根、路径 Create/Mkdir、OpenAt/OpenFile 的新建、路径 Write 新建及 ReplaceNode 的新节点。普通打开、续期与引用计数不产生这两个时间。

| 已有原生效果 | BirthTime | ChangeTime 及事务位置 |
|---|---|---|
| 新建节点或替换成新 NodeID | 本次创建时间 | 同一次创建时间；初始字段与引用一起确认 |
| File WriteAt/Truncate、Open reset、路径内容替换 | 保留 | 在既有 replaceNodeContent/对象 commit 最终发布处更新；Reserve、Put、失败 CAS 不更新时间 |
| SetAttr、指定 namespace payload 更新 | 保留，除非调用方明确设置该字段 | 在 applyChange/同一 metadata 事务更新；允许明确设置的 ChangeTime 取该请求值，否则取本次 authority time |
| 创建/删除子项、rename | 移动对象保留，新增对象按创建处理 | 父目录在现有 touch 处更新；rename 的移动对象在记录改名事实前更新，不能只更新父目录 |
| unlink 后保留的 detached 节点 | 保留 | 名字确实移除时更新该节点；最终无对象的物理回收无可查询时间 |
| PendingUnlink 状态实际改变 | 保留 | 与该状态改变同一事务更新；仅 armed intent、引用 close/renew 等内部生命周期不改节点时间 |
| replica apply / snapshot ingest | 保留 authority 的值及已知性 | 保留 authority 的值及已知性，禁止用客户端接收时刻填充 |

BirthTime/ChangeTime 的显式设置使用中立时间字段并接受相同操作授权；它记录调用方明确要求保存的属性值，不反推真实历史。一个实际 mutation 由原生回调一次取时，相关节点与日志使用同一确定结果；同一事务重试仅在最后成功效果中留下时间。已知未执行、CAS 竞争、真正无效果的请求不使未知时间变已知；写入相同字节但原有路径会确认一次写入效果的情况，仍按该有效写入更新。内部配额、reservation、GC、lease 或数据库 generation 的变化不更新节点 ChangeTime，普通读也不新增 atime 副作用。

旧节点迁移时 BirthTime 与 ChangeTime 均为未知。第一次后续有效修改可以使 ChangeTime 已知，不创造 BirthTime；这一点使 Linux/SDK 修改后的 Windows 查询不会依赖陈旧的 Windows-only blob。

**待决定：历史时间的 Windows 显示政策。** 严格策略在必须提供未知时间的 SMB 查询上明确失败；这会影响旧 volume 中未能提供 BirthTime 的文件，即使后来写入已使 ChangeTime 已知，也不能声称旧 volume 已完整可用。另一选择是显式的 SMB 兼容投影，须规定值、适用条件及可见的“非历史事实”含义，并由真实 Explorer/程序验证；它只能是客户端视图，不能写回 authority 或被描述成真实创建历史。当前未选定投影值。[MS-FSCC FileBasicInformation](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-fscc/16023025-8a78-492f-8b96-c873b042ac50)中零的“不修改”含义属于 SET，不是 QUERY 的未知标志。该选择是旧数据的互操作门禁，不改变新建节点记录真实 authority 时间的决定。

`SetMetadata(ctx, nodeID, namespace, expectedVersion, payload)` 是可选能力，比较与更新在一个元数据事务里进行，冲突证明本次未执行。这个版本只属于该 payload，不参与普通 `WriteAt` 的内容竞争；不能把整份 Attr 的 CAS 加到每次字节修改上。创建、重置或替换所需的 payload 分别取 InitialState 对应字段，随原子 Open 同时持久化；窗口内 HTTP 重放返回原结果，不能重新执行 payload 修改。payload 上限按节点、单个 namespace 及 volume 汇总限制，未知结果继续保留实际计量。

### 使用声明与范围命令

Windows 共享模式被归一化为读取、写入和删除三种用途及拒绝用途，不把 SMB access/share mask 下传。一个有效打开持有中立 `UseClaim`；用途表明该引用能够执行的受控操作，拒绝用途约束同对象的其它使用者。

```go
type Uses uint8 // ReadData, WriteData, ReadEntries, DeleteName

type UseClaim struct {
    Uses Uses
    Deny Uses
}

type ExtentKind uint8 // Bytes, Boundary

type Range struct {
    Kind ExtentKind
    Start, Length uint64 // Bytes: Length > 0
    CutAt uint64         // Boundary: an explicit cut between bytes
}

type RangeCommand struct {
    Domain ConflictDomain
    Mode RangeMode // Shared, Exclusive
    Range Range
    Edit RangeEdit // Replace, Subtract, AddExact, RemoveExact
    Claim ClaimID  // required by RemoveExact
    Wait bool
    Conversion ConversionRule
}

type RangeAttempt struct {
    Request LockRequestID
    State AttemptState // Pending, Granted, Rejected, Cancelled, Released
    Commands []RangeCommand
    Claims []ClaimID
    Conflict RangeConflict
    Rejection RejectionCode
    Effects []RangeEffect // includes releases caused by conversion
    EverGranted bool
    HistoryRemaining time.Duration
}

type RangeConflict struct {
    Found bool
    Owner OwnerDiagnostic // not an authority reference
    Range Range
    Mode RangeMode
}

type RangeControl interface {
    GetConflict(context.Context, UseOwner, RangeCommand) (RangeConflict, error)
    Apply(context.Context, UseOwner, []RangeCommand, LockRequestID) (RangeAttempt, error)
    Query(context.Context, UseOwner, LockRequestID) (RangeAttempt, error)
    Cancel(context.Context, UseOwner, LockRequestID) (RangeAttempt, error)
    Drop(context.Context, UseOwner, ConflictDomain) error
}
```

新打开与每个既有 claim 双向比较：`new.Uses & old.Deny != 0` 或 `old.Uses & new.Deny != 0` 即冲突；缺少声明不会变成免检。Linux 普通 File 打开持有其读写用途、Deny 为空，元数据引用按实际用途声明。既有读写打开也会阻止后来试图拒绝它的 Windows 打开。引用自身的 deny 不禁止它已经获准的用途；身份相同的另一个独立打开仍是其它使用者。

`UseScope` 是既有保留引用的不可伪造绑定，复用该引用的 session 与 capability；它不创建新 owner registry 或租约。可选 `ScopedReference` 从 File/NodeReference 暴露这个绑定，普通 File 字节方法内部使用同一绑定。`NameCommand`、删除意图和 ConditionalFileMutation 的命令各携带有界 `TargetUse{NodeID, Scope}` 集合：服务器逐项确认引用仍有效、属于当前 session、具有实际用途且指向被作用 NodeID。源对象和被覆盖目标分别检查，源 scope 不授予目标豁免；同 session 的另一个打开也不是同一使用者。没有 scope 的直接路径操作按匿名瞬时用途检查，不能推断豁免。

Namespace 的改名、替换和删除声明瞬时 DeleteName 用途，分别检查实际移动/移除的源对象与被覆盖目标；目录仅在该目录对象本身被移动或移除时参与 DeleteName 冲突，父目录仍验证原生修改准入；条件与动作在同一个原生事务里完成。路径型 Write、Truncate、SetAttr 以及其它没有 File 的直接入口按真实效果进入同一检查，不能以没有 Windows owner 为理由绕过限制。元数据只读打开是否需要数据用途由平台映射决定；后端不能把纯属性查询默认解释为 ReadData。

Range 是显式 tagged union：Bytes 使用 unsigned Start 和正 Length，Boundary 使用 CutAt；未使用字段必须为零。Bytes 以数学整数检查 `Start + Length - 1 <= MaxUint64`，计算不得先溢出；Bytes(MaxUint64,1) 有效，Bytes(MaxUint64,2) 拒绝。Boundary 不代表数据字节；它与 Bytes(a,n) 的相交关系是 `a < CutAt && CutAt < a+n`，边界零及两个 Boundary 互不相交。核心执行这组明确的 extent 代数，不从一个未经解释的零 Length 猜测调用方语义。

FUSE 将内核已经归一化的 inclusive `[start,end]` 转成 Bytes(start,end-start+1)；flock 的完整 Linux 范围为 Bytes(0,1<<63)，POSIX 的零长度到未来 EOF 在 FUSE 转为对应的非零 Bytes。Windows adapter 按 [MS-FSA 2.1.5.8](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-fsa/369103ec-a8af-452b-8006-aff07b925b61)解释原始 offset/length，把正长度映射为 Bytes、零长度映射为 Boundary(offset)；其边界行为由 [MS-FSA 2.1.4.10](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-fsa/124bb289-eeef-4653-b9c6-4fb93dd07a21)约束。正长度 File I/O 仍使用现有 int64 offset/长度和文件大小预算，转换为 Bytes 接受 enforced 检查。

`GetConflict` 只查询一个拟议范围的真实冲突或已确认无冲突，不产生锁动作；`Query` 只核对先前动作。二者不可互换。RangeAttempt/RangeConflict 是中立控制结果，保留动作历史语义但不继续嵌入现有 Linux FileLock、PID 或 syscall.Errno。

Linux 客户端将 flock 和传统 POSIX 请求归一化为两个独立 advisory 冲突域；Windows 的范围锁进入 enforced 域。域名是服务端固定的中立能力枚举，调用者不能任意创建一个冲突域来逃避保护。范围命令及返回值不携带 Linux `syscall.Errno`、PID 或 SMB 状态码，返回中立的已知拒绝原因与不透明诊断 owner；平台在本地转换。与错误链有关的普通 File 错误保持原语义。

归一化仍保留 Linux 的真实行为：flock owner 归于 open file description，只读 fd 可以取得排他锁；转换先丢弃原锁。传统记录锁归于 session 内进程与文件，部分替换/解锁保持拆分合并，转换失败保留原范围；冲突查询返回一个真实冲突。客户端用独立 owner 映射处理这两类行为，显式发出关闭事件，不能由 File.Close 猜测 PID。Windows owner 绑定实际 open，归一化命令表达锁模式、范围和解除方式；不借用 POSIX 的关闭任意 fd 规则。

`packages/advisory` 的 coordinator 保留 owner、范围集合、等待队列、请求 epoch、结果历史与死锁检查。为命令补充中立冲突域、范围更改规则和 enforced 检查，不将每次操作改造成完整范围快照 CAS。完整快照会让不相交锁互相冲突，并要求调用方重新建立等待、转换和取消顺序，既有命令历史不能因此被丢弃。

advisory 两域只约束参与者。enforced 的 shared 范围禁止所有 owner（包括持有者）的重叠写入，exclusive 范围禁止其它 owner 的重叠读取与写入；同一 owner 的 exclusive 只豁免该 exclusive 的冲突检查，不豁免仍存在的 shared 限制。锁可延伸到 EOF 之外。Truncate 检查实际改变的 `[min(oldSize,newSize), max(oldSize,newSize))` 区间，大小捕获、检查和改变不能分离；同长截断仍接受引用和权限健康检查。以原始内容为依据重建完整对象的内部读不获得跨平台数据读取权限，也不能通过内部写整对象扩大本次受控修改区间。

Windows adapter 在每个实际 open 上串行维护已经确认的 claim ledger，负责该平台的申请及解除政策：已有 exclusive 允许同 open/key 再取得相交 shared，却不允许再次取得相交 exclusive；已有 shared 拒绝同 open/key 的相交 exclusive 申请。相容的重复 shared 分别保存。按原始 offset/length 和 open/key 解除时，优先选择完全匹配的 exclusive，再选择 shared，遵守 [MS-FSA 2.1.5.9](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-fsa/84e68de6-31a6-4cba-afd4-00b1bac6d1a2)。这些是 adapter 的序列化输入政策，远端不内置 SMB LockIntent 表或 exclusive-first 选择。

核心提供 AddExact/RemoveExact、独立 ClaimID、明确的持有者/其它访问者保护政策及有界原子编辑。ClaimID 来自原动作及批次索引，重复投递返回同一组 claim；它不会把同 owner 的重叠锁归并成 POSIX 范围，删除不存在的 claim 明确失败。已确认的保护继续约束所有实际数据访问，共享保护的持有者禁止写入不能因 adapter 本地 ledger 而失效。[LockFileEx](https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-lockfileex)的同 handle shared/exclusive 重叠例子在 adapter 保持两项记录，核心保存两份独立保护。

ledger 只应用由原动作 Query/Cancel 或成功响应证明的结果；状态未知时 fence 该 ledger 与受影响引用的访问，停止后续依赖它的申请与解除选择并进入既有 session 清理。不能依据本地超时猜测某条 claim 不存在，再提交另一个可能破坏 multiplicity 的动作。

范围覆盖、精确解除和批量锁请求必须按客户端实际协议映射成明确命令。Apply 的有界命令批次在 coordinator 一次接纳、检查并执行；Windows 需要全有或全无的批次在失败时不改变任何 claim。Linux flock 的 DropBeforeAcquire 是另一种明确效果：冲突或等待之前已经释放原锁，其拒绝不是“无效果”，RangeAttempt.Effects 和动作历史保留这次释放。不能依次执行若干 Apply 后把部分成功说成全部未执行。客户端不支持的请求组合须返回对应的不支持错误，能力校验与验收必须证明支持目标没有依赖该组合。

### 实际访问与最终修改的顺序

share/enforced 的授予、解除、到期释放、读取修订捕获及最终修改共用后端既有的目标/观察顺序。先取得 native gate，再短暂取得 coordinator mutex 完成有界状态检查与转换；SQL 与对象 I/O 不在该 mutex 下执行，持有它时不得反向进入 native gate。等待唤醒只选择候选，候选回到 native gate 内重新验证 owner、期限、目标与冲突后才能授予。既有 advisory.pumpLocked 的 enforced 分支需要这个重新进入步骤，单加冲突 predicate 不足以建立顺序。最终修改沿既有 busy/完成通知保留许可到发布结束，结果未知先 fencing 再允许后继状态观察；无需让全局 coordinator mutex 覆盖长 I/O。

普通 File 读写不增加预先的远端 Status RPC。File 已持有 session 与引用身份；每个请求仍通过现有授权、准入及失效检查。新增检查进入同一条原生路径：

```text
File.WriteAt
  → 原有请求授权与 session/reference 准入
  → 捕获当前内容修订、构造范围补丁、预留对象与配额
  → 原有 native.Commit 最终入口
      引用仍有效 + strong proof + share/enforced 条件
      + 必要的局部条件 + revision CAS
  → 元数据/内容发布与原有配额结算
  → 返回当前 Attr
```

`objectstore.mutate` 与 `native.Commit` 继续拥有 revision 竞争重试、节点 pin、未决发布和配额账本。只有明确未提交且清理成功的内部竞争允许重试；新增访问冲突返回确定拒绝，未知发布仍为 `EIO` 并隔离受影响范围。不能因为锁检查失败而吞掉已经产生的未决对象所有权。

share/enforced 的实际访问目标由 native File 的 NodeID 枚举，包括仍被有效引用保留的 detached 节点；不能沿名字重新解析，也不能沿用“没有名字就不进入目标集合”的分支。它们按自己所属的 FileSession 与实际 open 生命周期继续约束同一保留对象。

Strong 保持既有生命周期：经授权的名字移除令强资源 TargetGone，File 与 advisory 继续保留旧对象，不恢复或延长此前的 S/X。新增 share/enforced 目标检查不能把已退役 Strong grant 重新变成有效权限，也不能因二者都使用 NodeID 就合并目标存活规则。仍有名字的 pending-delete 最终删除先经过正常 Strong gate；名字移除之后的 detached 回收继续采用既有行为。

读操作在捕获一个修订前取得访问许可。捕获与新 enforced 授予在同一顺序里，该次已获准捕获的读取可以完成，新锁不能倒推撤回已交付或已准入的读取；锁成功后再准入的冲突读不能捕获内容。现有 pin 保留的是节点，未承诺固定捕获修订的内容对象；旧内容被回收时继续使用现有有界重取与 EAGAIN 失败，每次重新捕获修订都重新接受 enforced 检查，不能借第一次许可跨越后来的授予。长对象下载不持有 coordinator 全局 mutex，读取仍纳入原有操作排空。写准备不提前获得最终发布许可，等待上传不能延长 session 或强 S/X 期限。

Windows disposition、append-only 或要求原属性仍成立的操作若不能由普通 Open/Write 表达，使用小型可选 `ConditionalFileMutation`，明确条件和效果：预期长度/指定 metadata namespace 版本与本次范围或长度改变在同一个最终入口比较。append 先按捕获修订的 EOF 构造候选补丁，最终入口用既有 revision CAS 及必要长度条件确认该 EOF 仍成立；明确竞争时按新的当前 EOF 重新构造，沿原有有界重试发布，不能先 Stat 再无条件按旧偏移 WriteAt。truncate 和初始 payload 必须同一次 Open 生效。它复用既有物化、revision 重试与 Commit，不改变普通 `WriteAt`，不构成整个应用大写入的事务。支持的每一种条件都须有具体平台操作和反例测试；不能提供任意脚本或远端谓词。

### 删除意图与引用结束

删除能力区分每个引用上已接受但尚未触发的 CloseIntent，与节点上已经生效的 PendingUnlink。它们都使用既有 NodeID 和有界记录，固定触发条件为 OnReferenceClose 或 Now，不形成通用工作流引擎。

OnReferenceClose 随 OpenAt/OpenChildRef 原子接纳、持久保存并绑定返回引用。它尚未激活节点 PendingUnlink，因此不单凭这项意图拒绝后来相容的打开，也不禁止目录新增子项；共享权限仍正常检查。当带意图的那个引用关闭或到期时就触发，不能等到最后一个引用才激活。Windows adapter 按 [MS-FSA Close](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-fsa/d142c93a-72bc-4b05-9d96-8e00371c3308)选择固定触发条件：普通文件激活 PendingUnlink，目录仅在触发时为空才激活，非空时消费该 close intent 而不删除目录。触发判断和消费/激活在同一原生有序事务完成，不能先释放引用再补做。

Now 用于显式 disposition：设置时就验证 DELETE 用途、共享限制、目录为空及必要的名字 Guard，经正常 Strong publication gate 后激活节点 PendingUnlink；之后相冲突的新打开明确返回 PendingDelete。已有引用继续访问同一对象。按 [MS-FSA FileDispositionInformation](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-fsa/386d9ec5-e0f6-4853-b175-c05be01419e0)，清除操作以当前 pending generation 为条件清除节点 pending 状态，不是对每个 open 的 pending 位作 OR；它不抹掉其它引用上已 armed 的 OnReferenceClose，这些引用随后关闭仍可重新激活。

每个引用至多一项 armed intent，节点 pending token 只表达当前转换代际，使用现有有界动作核对及节点持久元数据。失效 generation 的清除明确未执行，不跨越另一次激活；不能用旧路径删除同名替代物。解除、改名、替换及最终名字移除都验证实际 NodeID 与名字条件。最后一个相关引用结束后才尝试最终 unlink；已删除的无名目录仅供已有引用查询属性与结束生命周期，不能新增子项。

每个有关引用结束时，native Retire 先阻止该引用的新 I/O 与最终发布，并持久激活/消费它已经接受的 close intent，然后才释放该引用的 Uses/deny/range claims。激活失败时，沿现有 advisory.Retire 的 fence 失败处理保留 grants 和清理所有权，不能先撤销保护再丢失后续删除义务。已准入操作继续排空；最后一个相关引用结束后，finishClose/startClose 的可重试清理再尝试名字删除、detached 内容回收与配额结算；只有完整清理已知成功才结束 Close，不能先标记成功关闭再把匿名任务交给无主队列。仍有名字的删除必须通过正常 strong publication gate；它是设置 intent 时已获业务授权、绑定 NodeID 与条件的固定清理效果，以系统匿名身份执行，不携带或重用原持有者的 strong proof。任何仍有效的 S/X 都阻止这次匿名删除，既有 grant 可以延迟它，新增 grant 继续按既有强占有规则准入。`cleanup:true` 的绕过仅用于名字已经移除之后的 detached 物理回收。

删除冲突或结果未知时保留 durable intent、charge 与可重试的清理所有权。Close 返回已知引用退役后的 pending/失败，不能声称已经完整删除；接受意图、引用退役、名字删除、物理回收分别报告已知效果。沿现有 startClose 在失败后可再次尝试的机制和 HTTP registry 持有清理对象，不能删除 registry 条目后留下无主义务；一个 owner 清理错误不跳过其它依法拥有的清理。

重试由重复 Close、现有 registry/session 到期维护和启动恢复触发。每次尝试受现有操作超时及并发上限限制，失败不在调用内无界循环，也不为每个 intent 建后台 goroutine。需要扩展的具体挂接点是：当 session 已退役但已命名 pending 节点因 strong 冲突仍未删除时，HTTP/native 清理拥有者继续被既有维护扫描，直到确定完成；当前只回收 detached 节点的启动扫描须同样识别这类 intent，通过正常 strong gate 重试。实现须证明这些所有者在 session registry 清理后仍有落点，否则这项能力未完成。维护轮次可以延迟物理清理，但不能使 PendingDelete 状态下的新打开越过未完成义务。

服务器重启可以退役 FileSession 与旧引用，但必须读取已确认删除的持久清理状态，继续兑现该效果后才允许冲突的新打开。恢复不能把删除 intent 丢弃成普通有名节点，也不能依据旧名字删除新节点。这个义务保存的是已经接受的文件系统效果，不恢复原 SMB handle、File 对象或逐条锁动作历史。

### v6 迁移与恢复

新增字段由 migration 6 承载，migration 1–5 不变。已有 NodeID allocator/high-water、内容对象、revision、detached 计量与 Strong lease evidence 保持其所有权；不保存新的 File lease、EntryID 或持久 range/Uses 表。

| v5 数据 | v6 的确定映射与校验 |
|---|---|
| nodes.id、volume、content、size、content_revision、detached | 原值保留；校验对象关系、计量和 detached 约束，不能重新编号或因改型释放内容 |
| nodes.mode | 当前合法普通文件/目录 type bits 映射为 NodeKind，权限/特殊位编码为规范的 POSIX payload；其它或畸形 type bits 拒绝，不能截断后变成合法类型。新 symlink 能力不借迁移接受旧无效种类 |
| atime/mtime 的 sec+nsec | 原精度保留并校验；不改成升级时刻 |
| 不存在的 BirthTime/ChangeTime | 两组可选值均 Unknown；用明确 NULL/存在位保持与真实零时间可区分 |
| 原始 BLOB 目录叶名、volume root/used、对象 reservation/归属 | 原值保留；不经过 UTF-8 或 Windows 名字归一化 |
| historical changes 中已有 mode/时间属性 | 对存在的历史属性做同样的 kind/POSIX 映射，新时间为 Unknown；原本无属性的事件仍无属性。保持 position/high-water，不用当前节点补写过去事实 |
| 原来不存在的 CloseIntent/PendingUnlink | 初始化为确实没有新能力动作的状态；v6 已存在的记录必须校验完整结构、NodeID、原引用 incarnation/触发状态与 charge 归属，缺失或损坏不能作空状态 |
| database/backing-store identity、node/change high-water、lease_recovery/witness | 原值保留并继续原验证；schema 升级不重置 authority 证据 |

迁移沿现有 [PrepareConfigured](../../../../packages/metastore/sqlite/internal/schema/prepare.go) 的一个 SQL transaction 执行。先按原 schema 读取版本，核对 accepted witness/WAL、database identity/high-water，再对旧布局做完整 preflight；当前 preflight 的 `< v5` 条件必须扩展出明确的 v5 检查，防止先把畸形 mode 转换后掩盖源损坏。旧节点、对象、条目关系、usage 与历史日志通过检查后，Reach(6) 在同一事务中更新结构、数据和 schema version，然后按新格式检查全部 volume 和 backing-store 绑定。

独占 opener 的 schema 准备阶段仅保留现有“已无名字的 detached 节点”回收。仍有名字的 PendingUnlink 或旧 incarnation 的 armed intent 必须完整保留，不能在 migration/schema 启动事务里提前 unlink：此时 Strong authority 与恢复屏障尚未建立。节点在恢复检查未完成时对相冲突的新打开保持 blocked，旧引用与 intent 的发起引用按 incarnation 退役，不恢复 handles。

完成 schema/witness 验证后先建立既有 Strong machinery 及其恢复 gate，再由有界的现有恢复/维护挂接点处理持久 intent：旧 armed 引用按关闭触发条件激活或消费，已 pending 节点在正常 Strong gate 真正允许后才删除。恢复期间可查询真实 blocked/pending 状态，不能把“等待恢复”说成无 intent，也不能开放冲突的新引用。所有节点表、事件写入、snapshot 和 replica ingest 同步使用新字段；复制接收保留 authority facts，不能以旧 wire mode 或缺失字段生成默认成功。

完成既有 AdvanceGeneration 与 SQL Commit 后才执行独立 witness.Accept，二者沿当前 [durability](../../../../packages/metastore/sqlite/durable.go) 顺序确认。SQL commit 或 witness accept 不确定时 constructor 返回失败、隔离 coordinator 并关闭数据库池，保留 WAL 和持久证据；不得重置版本、删除 WAL 或盲目重跑迁移。

重新打开按现有 [ReconcileStartup](../../../../packages/metastore/sqlite/internal/dbstate/startup.go) 核对 accepted/visible generation、DatabaseID、high-water 和启动前 WAL 证据。witness 的 dbstate.State 只包含数据库身份、generation、node/change high-water，不包含 schema version；版本、旧布局 preflight 与新结构完整性检查仍是独立义务，不能用 witness 相符代替 schema 验证。

下表描述支持 v6 的新 binary 沿现有机制恢复；当前最高版本为 v5 的 binary 仍拒绝 v6。假设升级前是完整已确认的 v5 状态 S、generation G，升级事务只推进一次 generation 得到 v6/G+1。A 为 accepted witness generation，V 为 SQL 可见 generation，C 为 checkpointed witness generation，W 表示启动前 WAL 非空；每一允许项均要求 witness 有效、同一 DatabaseID、high-water 不倒退，以及对应 schema/数据完整性检查通过。

| 重新打开时可见状态 | 允许条件或拒绝原因 | 随后的既有准备流程 |
|---|---|---|
| v5/G，A=G | 完整 dbstate.State 与 accepted 相等，且 C=G 或 W 为真 | Reach 应用 v6；AdvanceGeneration 到 G+1，SQL Commit 后 Accept(G+1) |
| v6/G+1，A=G | 仅 W 为真且可见 high-water 不低于 accepted 才允许；这是有 WAL 证据的已提交、尚未 witness 接受状态 | Reach 见当前 schema，不重跑 migration 6；正常 Prepare 仍推进到 G+2，Commit 后 Accept(G+2) |
| v6/G+1，A=G+1 | 完整 State 相等，且 C=G+1 或 W 为真 | 不重跑迁移；正常 Prepare 推进到 G+2，再 Commit/Accept |
| v5/G，A=G+1 | 可见 generation 比已确认值旧，无论 WAL 是否存在都拒绝 | 不迁移、不降低 witness |
| v6/G+1，A=G，W 为假 | 缺少已提交新 generation 所需的 WAL 证据；即使 SQL 可读或 C=G 也拒绝 | 不自动采用新状态、不重新执行 migration 6 |
| 同代不同完整 State、不同 DatabaseID、high-water 回退，或 C<A 且 W 为假 | 既有证据不一致或缺失条件 | 拒绝打开，不初始化或合成替代 witness |

SQL Commit 或 witness Accept 的未知结果因此可以在重新打开后证明为完整旧状态或完整新状态，不能接受混合格式。迁移未提交时从完整旧 schema 重做；已提交 v6 时不重跑数据转换，但仍遵守现有 Prepare 每次推进 generation 的行为。验收在 preflight、Reach、SQL Commit 与 witness.Accept 前后注入退出/未知结果，逐项覆盖上表；schema version 与 witness State 分别验证，1–5 文件哈希不变。

### Strong 与新增保护的组合

每种机制独立给出许可，满足其中一种不豁免其它机制。普通访问不隐式取得 Strong，Strong proof 也不是 share/range owner。

| 操作或转换 | 既有 Strong 规则 | share/enforced 的附加条件 |
|---|---|---|
| Open（无创建/截断）与属性查询 | 不隐式加 S/X；普通读取不因 X 被拒绝 | 原子检查实际 Uses/Deny；metadata-only 不虚构 ReadData，目录列表须有 ReadEntries |
| ReadAt 捕获一个修订 | S/X 不阻止普通读取 | 对该实际 NodeID/range 取得许可；重新捕获修订重新检查 |
| WriteAt、Truncate、Reset | S 不授予修改；X 需要本次明确的有效 proof；匿名修改不得越过有效保护 | 同时验证引用用途及范围约束，共享范围对持有者的写入限制也有效 |
| SetAttr/SetMetadata 与有名 pending 状态修改 | 经既有实际 mutation/Strong gate，旧 proof 不可退回匿名 | 按本次实际 metadata 用途与 scope 检查，不能一律套成数据读写 |
| rename/unlink/ReplaceNode | 对真正移动/移除的源和替换目标检查，获准名字移除后 Strong TargetGone | DeleteName 与实际源/目标 scope 各自检查；单纯 byte-range 保护不被扩大成名字锁 |
| 最后 pending unlink | 系统匿名固定清理效果，任何有效 S/X 都可延迟它 | PendingUnlink 阻止相冲突的新打开；保留引用先排空，未知不释放 charge |
| share/range 申请、解除、等待 | 不创建、续期或替代 Strong grant，也不复活 TargetGone | 与实际 I/O/名字效果共用 native 顺序；新 grant 不撤回已经准入的旧读取 |
| 上传期间 proof/session 到期 | 最终发布处拒绝过期显式 proof，不能因为无竞争者就匿名完成 | 引用失效同样阻止最终发布；准备/上传不占住或延长许可 |
| authority 重启 | 原有剩余期限恢复屏障继续保护修改 | 旧 FileSession incarnation 退役；旧引用不能重绑定。持久 pending 清理在正常 Strong 恢复顺序内完成 |

### Session 与保护的重启语义

建议让 Windows share/range 与持有它们的同一个 FileSession incarnation 共生：Renew 延长既有有限 session，Close/到期先 fencing 再排空、释放 claims 与范围；server authority 重启使旧 session 与全部引用失效。短暂 HTTP 超时不证明 authority 退役，同一 incarnation 的保护和清理所有权保持到已确认关闭或既有期限结束；客户端不得在连续性未知时恢复访问。重启后新 session 能建立新保护，但不能把旧 handle 自动重新打开并宣称锁从未丢失。

该建议遵守 R-CC-14 对丢失连续性后明确失败的要求，不将强 S/X 的 R-CC-8 自动赋给 Windows 共享模式。代价是服务器重启后应用必须重新打开；在确认旧 authority 退役并完成恢复清理后，新调用方可以取得与旧 Windows claim 冲突的访问。客户端必须使旧引用持续报错，不能仅更新其 epoch 继续使用。

**更强的可选保证：Windows share/range 在 server 重启后继续阻止冲突直至原确认期限。** 这不是当前 R-CC-14 已经承诺的义务，本提案未选择它。若增加该保证，需要在 existing FileSession 的恢复所有权下增加可证明的剩余保护或保守屏障，确定影响读取、打开及修改的范围；不能只复用仅拦修改的强 S/X 屏障。它增加持久确认、恢复拒绝和计量成本，但仍不授权另建 File 租约体系。两种选择都不改变强 S/X 的已确认期限与重启保护，也不撤销已经持久接受的删除意图。Microsoft 的 [SMB 断开处理](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/ae543f57-d993-4905-bce0-3cfc446c1fbb)和[持久打开清理条件](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/eb5bfe99-47fe-4e87-8e87-08a084dcefb6)区分普通打开与 durable/resilient/persistent 例外，[服务端初始化](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/04e0383e-e2b4-49c2-803f-1313d9c0b2ff)建立新的打开及会话表。由此可支持本提案的推论：在不提供这些持久句柄扩展的目标内，复用 FileSession incarnation 退役是可行方向；最终行为仍须经真实客户端断线与重启用例证明。

### 结果核对、取消和容量

新能力的 Go 接口不普遍增加 request ID。直接 Go 调用只执行一次，由 native callback 确认效果；调用方遇到未知结果不能再发一次当作重试，按原错误/fencing/清理契约处理。既有 objectstore 内部 revision CAS 的已知未提交重试仍保留。网络重复投递在已经存在的 HTTP FileSession action journal 里核对，新增控制操作只扩展该路由的操作种类、input digest 与结果内容。

| 操作 | HTTP 路由及重放 | 直接 Go / 失败后的所有权 |
|---|---|---|
| OpenAt、OpenNodeRef、OpenChildRef，含 Uses 与 armed CloseIntent | 新 file-control opcode 进入现有 session action journal；相同 ID+digest 回放原结果，不同输入拒绝；返回引用沿现有 pending-open ACK | 直接调用一次；未知时 session/reference fencing，保留新引用或 intent 的清理所有权，不能第二次 open |
| 新 NamespaceAccess.MutateName、SetMetadata、ConditionalFileMutation、Now/clear PendingUnlink | 明确放入现有 file-control action journal；digest 包含 scope、guard 集合、全部原始名字、条件和 metadata；不返回引用，因此没有 open ACK | 直接调用一次；已知条件冲突证明无本次效果，未知按原生 EIO/隔离处理；不添加 native universal receipt |
| 已有 File.WriteAt/Truncate/SetAttr/Sync 与 SetNodeAttr | 保持当前 fileActionRequired 路由及有界同动作恢复，读写签名不变 | 保持同步成功与未知错误契约；不是给全部存储 API 新建 receipt |
| 既有路径 Storage 写入/名字/属性操作 | 保留现有路径协议及错误规则，不被偷偷搬到新 journal | 已知普通失败按原规则；未知修改不盲目重投，不承诺本提案未提供的结果查询 |
| ReadDirNode、Stat/metadata/ReferenceState 查询、普通 ReadAt | 只读请求，不创建动作记录；再次查询是新的观察 | 失败不返回猜测的数据；涉及 session/保护连续性未知时 fence，普通只读传输失败保持相应 I/O 错误 |
| Range Apply/Query/Cancel、Drop | Apply/Query/Cancel 保留原 lock request epoch/nonce 与 coordinator 历史；Drop 保留现有 file-control 幂等清理路由 | 只有锁控制继续携带它本来需要的 RequestID；Cancel 不构成未授予证明 |
| 引用 Close、intent 触发、expiry 清理、Session.Close | Close 保持 capability 幂等及 registry 清理，不进通用 action map；intent 触发用引用已有 token/状态 | Retire 激活/消费失败保留 claims、charge 和清理拥有者；重试同一关闭，不建立第二个 intent |

所有 file-control 请求仍先按当前实际语义授权，再查 journal；旧结果也不能绕过本次授权。新 opcode 保留 HTTP FileLimits.MaxActions/MaxCleanupActions、epoch 与有限 history，不在容量满时淘汰仍可核对的记录。元数据与 namespace 修改的响应保留既有 replication mutation barrier，不能回放成功后让 replica 越过未知发布状态。

Open 响应丢失时，在既有有界恢复 context 中按同一请求重放，只有 ACK 完成才交付 File/NodeReference；ACK 或核对无法完成时 fence 并关闭原 session，服务端继续拥有未确认引用的清理。ACK 丢失不再执行 open 或重复 armed intent。Close 只有清理已知完成才从 registry 移除；重复关闭已经移除的 capability 才返回幂等成功。

| 故障注入点 | 可报告结果 | 重放/清理断言 |
|---|---|---|
| 准入容量拒绝、输入/条件验证失败，native 尚未执行 | 明确未执行/已知拒绝 | 不新增引用、claim、intent 或 charge；不能将未知 native 错误归为此类 |
| 新 open 已成功、响应或 ACK 丢失 | 原 action 已成功或尚未知；未 ACK 不向调用方交付引用 | 同一 journal 恢复仅一个引用/intent；到期清理维持其实际 charge |
| 新 name/metadata/conditional/Now 操作 SQL 成功、HTTP 响应丢失 | journal 内的原成功；无法核对则 EIO/unknown | 同 action 不再次修改 generation/时间/名字；直接 Go 未知不重投 |
| range grant/release 与 Cancel/回复丢失相撞 | Pending、Granted、Cancelled/Released、已知拒绝或未知分别保留 | Query 证明 surviving claim；未知 fence ledger，不能选另一 claim 试着解除 |
| intent 激活/消费、最后 unlink、quota settle 失败 | 明确已有的 intent/退役效果与后续失败，或未知 | 正常 Strong gate、claims 释放顺序和 charge 所有权不丢；清理由同一记录再次尝试 |
| 原 epoch 退役、history 外或 authority 重启 | 退役/未知，不能当作未执行 | 不重新接纳旧 action；旧引用失败，持久清理按现有恢复路径接续 |


UseOwner 在 FileSession 内登记，明确属于引用结束时清理或客户端显式 Drop 的中立生命期；创建时绑定实际节点和已保留引用的用途，不能凭一个任意 owner 数值获得读取或写入权限。FUSE 选择 open-description 或进程/节点映射，Windows 选择实际 open；后端登记与计量使用原 coordinator，同一数值在不同 session 没有同一身份。

RangeControl 继续使用 `LockRequestID` 的 epoch 与随机 nonce、已接纳动作历史和 `EverGranted`。Pending、Granted、Rejected、Cancelled、Released 与结果未知保持可区分；Query 反映动作后来的授予/解除状态，但不能改变原决定。Cancel 与授予有序，只有结果证明没有遗留 grant 才能向 Linux 返回可安全重试的 `EINTR`。历史窗口外或旧 authority 返回退役/未知，绝不当成未执行或再次接纳。

FileSession 的 Renew/Status 继续报告确认剩余时间、revision、action epoch 和 history window，保守扣除往返耗时，重放旧响应不能延长期限。Windows adapter 按已确认期限安排续期，未知保护连续性时停止受影响 I/O 并进入清理。强 S/X 的 session、FileSession、HTTP 连接和复制 incarnation 各自保留既有角色。

资源沿既有预算核算：所有引用种类共享 MaxFiles，活跃操作与排空共享 MaxOperations，claims 计入引用或 owner 预算，range 与 pending 沿用 MaxLockRanges/MaxPendingLocks，advisory 动作记录沿用 MaxLockActions/History，HTTP journal 另按 FileLimits.MaxActions/MaxCleanupActions 计量。目录观察和 payload 有单次、每节点及 volume 聚合字节上限，待删除意图计入被保留节点预算。新增资源必须在产生效果前预留；满额时明确拒绝本次准入，终止 owner/session 和必要清理不依赖空余动作槽位。不能用逐条淘汰仍有效的历史释放容量。

错误分清三类：已知拒绝且无本次效果、已知成功效果与后续清理失败、结果未知。前两类保留具体原因和阶段，最后一类按现有 `EIO`/fencing 处理；不发明通用 ActionReceipt 来替代 File 的同步结果。`NotFound` 只用于权威确认不存在，NodeID 已失效与闭合引用分别保留现有 ESTALE/EBADF 含义。共享冲突、范围冲突、DeletePending、观察冲突与不支持由中立控制错误区分，再映射成本地错误。

排障复用现有请求 context、volume/NodeID、FileSession epoch/revision、动作 ID 和错误链；凭据、能力引用以及敏感 payload 不进入日志。必须能区分哪类能力未提供、在哪一已知阶段拒绝、是否 fenced 及哪项清理未完成。这里不新增独立遥测系统，也不以日志存在证明行为正确。

### 平台操作映射

| 平台操作 | 复用的核心与附加能力 | 成功前必须成立 |
|---|---|---|
| Linux Open/Read/Write/Truncate/Sync | 原 FileSession 与 File | 原有身份、当前修订、同步确认、访问限制和强 S/X 检查 |
| Linux Create/Mkdir/Unlink/Rename/Readdir | Parent NodeID + 精确 RawLeaf 的原子 namespace 能力 | 正确父身份和被作用名字；默认没有目录集合 Guard |
| Linux flock / POSIX fcntl | neutral advisory 命令 | 各自 owner、转换、等待、GetLock、关闭规则与当前 Query/Cancel 证明 |
| SMB CREATE 普通文件 | OpenAt + UseClaim + 必要 Guard | disposition、目标存在性、大小/初始属性、共享准入与同一返回 File |
| SMB CREATE 元数据/目录 | NodeReferences + 同一原子打开与 session | 节点种类、元数据权限、必要创建效果与保留引用 |
| SMB READ/WRITE/FLUSH | File.ReadAt/WriteAt/Sync | 单次请求的同步确认、对应 enforced 范围准入与无本地 dirty 成功 |
| SMB QUERY/SET_INFO | File/NodeReference Stat/SetAttr + 所需 payload 能力 | 同一 NodeID、中立权限；平台 payload 版本仅约束本次 payload |
| SMB rename/disposition | 原子 namespace / durable pending-delete intent | 名字 Guard、共享 DELETE、实际目标及删除效果保持身份绑定 |
| SMB LOCK/CANCEL | enforced RangeControl 与原历史 | 单次请求的完整结果、真实冲突、取消与授予顺序 |
| SMB QUERY_DIRECTORY/CHANGE_NOTIFY | 按目录身份观察 + 现有 change log 与客户端复制 | 名字可表示且无歧义、观察连续性与有界事件状态；缺口明确失败 |
| SMB CLOSE / share Stop | 原引用 Close / FileSession.Close | 原子退役、排空、owner 清理、删除 intent、物理回收及配额结果 |

Windows 名字通知使用现有 change log、客户端 replica 以及有界的前后事实解析；删除和改名需要的事实必须来自相关日志/复制状态，不能靠当前名字猜测过去节点。不默认在每条事件携带完整祖先树或 EventImages。若通知所需事实已不在有界保留窗口，相关观察明确失败并重新建立，不能报空变更。事件最多帮助获得新视图；受控操作的正确性仍来自权威原子检查。

## 备选方案

**在现有 File 核心上增加可选中立能力。** 这是本提案。它保留已经交付的对象身份、修订重试、配额和关闭机制，把新增行为放入相应原生入口。代价是包装链须对每种能力作真实检查，新增共享/范围约束仍须覆盖所有路径型和引用型访问；接口是可选的，不意味着已启用 volume 上的保护可以被可选执行。

**仅在 Windows SMB package 内实现共享模式、范围锁与名字检查。** 改动局限于新客户端，Linux 和服务端可以保持原样。它不能阻止另一台 Windows、Linux 或 SDK 的冲突修改，也无法把客户端名字检查与远端变更合成原子结果，违反 R-FS-9 与 R-CC-14。进程内锁和重新 Stat 都不能补出这个保证。

**重写通用文件内核，统一 EntryID、全操作 receipt、完整锁快照与独立 lease。** 可以为每个操作提供统一的结果类型和状态模型，但会替换 File 的同步成功边界、现有 owner 命令和 retained-file 回收，再次引入身份映射、全量状态竞争与生命周期恢复证明。当前需求不要求这些替换；已有机制能承接新增约束时，这部分成本没有对应的产品收益。

**让远端理解 Windows 文件模型并直接提供 SMB。** 能在一个服务里处理名字、句柄和锁协议，但平台依赖和规则进入所有远端部署，volume 名字语义也容易随 share 发布变化，违反 R-INT-8 与 R-INT-14。系统自带 SMB 客户端仍可通过本机 adapter 使用，不需要远端承担 Windows 协议。

## 验收标准

下面是实现准入和交付门禁，不能把本提案或编译通过当作这些断言已经成立。

1. **接口与依赖。** 编译检查确认 Linux/Windows 平台包可以独立导入；存储与通用传输没有 FUSE/SMB 类型。现有 File 字节方法签名、普通修改语义、NodeID allocator/high-water、objectstore.mutate/native.Commit、session 续期及 HTTP open ACK 路径保留。每个新增能力由一个实际平台操作及其失败反例证明必要。
2. **已有文件核心回归。** package-local 测试覆盖 rename/unlink/replacement 后的 File 读写、同修订 EOF/Attr、竞争范围写、非零 truncate、未知提交、pin 回收、配额、session 到期、Close 并发、Open 响应与 ACK 丢失。至少一项负对照在移除关键 guard/fence 的实现上失败，证明测试命中保护位置。
3. **名字与打开原子性。** 确定性插入父改名/同名替换、保留父引用关闭与 OpenAt/OpenChildRef 相撞、Windows 检查后祖先换名或新增大小写冲突、排他创建竞争、创建截断与属性提交失败。错误不得留下半次成功引用；两个 Linux 调用在同目录创建不同精确名字都应成功，不因无关 sibling 变化被目录 Guard 拒绝。无效 UTF-8 必须跨实际 HTTP 编码原样到达观察边界。rename 覆盖观察到的 `bar`/NodeB 并输出 `BAR`，验证第三个输出占位者导致全次拒绝；大小写变化不能扩大允许删除的节点集合。
4. **跨入口保护。** 双向 shared-use 冲突、纯属性打开、共享删除、被替换目标、enforced shared/exclusive、跨 EOF 范围、uint64 末端溢出与显式边界（Boundary(0)、边界在 Bytes 内外、Bytes(MaxUint64,1) 有效与 Bytes(MaxUint64,2) 拒绝）、重复 shared claim 与 exclusive 优先解除、截断、raw Storage 与 File、Linux 与两个 Windows 客户端交错均在实际存储入口验证。上传中到期、读准入与授予相撞、范围转换、批次失败、Cancel/Grant 竞争及容量满的终止操作都有可控调度。明确覆盖 share/enforced 在 unlink 后继续约束同一 detached NodeID；同时确认经授权的名字移除仍令 Strong TargetGone，不恢复旧 grant。
5. **持久删除与引用种类。** 普通/元数据/目录引用同一预算，目录 detached 后禁止建子项；armed CloseIntent 允许后来相容打开与目录子项，指定引用关闭即触发；非空目录触发被消费，Now 在设置时验证目录为空，清除 pending 不清除其它 armed intent。已确认删除义务后 kill/reopen 仍兑现原 NodeID 效果，同名替代物不受影响。每个持久转换边界注入错误、退出与未知结果，特别在 Retire 激活 close intent 失败时断言 claims/grants/charge 仍保留，配额不提前回收，必要状态损坏拒绝启动。迁移使用固定旧数据夹具，包含畸形 v5 mode、历史日志、Unknown 时间及 accepted/visible generation/WAL 组合，1–5 的内容哈希不变；named pending 在 Strong 初始化前不被 unlink。
6. **真实原生客户端。** Windows 11 24H2+ 的原生 SMB 重定向器、资源管理器和未修改程序，经真实本机 adapter 与远端 backend 验证；Linux 挂载同时参与。覆盖 native 大请求拆分、CREATE/disposition、share/range、metadata-only、目录、取消/断线/重连、关闭及停服。跨编译、模拟 SMB 客户端和进程内存储测试各自有用，但不能代替这项门禁。
7. **缓存与可见性。** 在实际 Windows OS 上记录生效的缓存授权和行为，先缓存不存在/目录枚举/属性/文件内容，再由另一入口创建、改名、写入、截断或删除，确认已打开引用和新的按名访问满足 R-CON-1/R-CON-2/R-CON-4。特别验证重定向器 negative-name 缓存：若一秒后仍因 native 缓存看不到创建，或只靠 TTL 到期才可见，即失败。不能用全局 registry/TTL 设置消除失败，也不能因不授予 SMB data lease 就宣称 negative cache 不存在。
8. **系统资源和覆盖。** 每个修改包由自己的测试 binary 测量覆盖，门槛遵循当前 testing.md：每包 70%、每函数 50%、总体 85%，其它 package 的集成测试不计入生产覆盖。相关正常与 race 测试通过 `assert-every-test-ran.sh`、`-count=1` 执行，不能跳过；需要 Azurite/FUSE/Windows 的检查分别具备真实依赖。记录实际命令、输出、测试平台与未执行项目；失败后核对 mount、FUSE connection、服务和子进程残留。

目录读取的准入验收还须覆盖以下组合，并在检查后暂停、并发授予冲突 claim，确认列表捕获顺序：

| 请求 | 必须观察到的结果 |
|---|---|
| 已授权、无 Scope；同 session 的另一个引用拒绝 ReadEntries | 拒绝读取，不因同 session 豁免 |
| 已授权、无 Scope；其它 session 的引用拒绝 ReadEntries | 同样拒绝，不能用空 Uses 绕过 |
| 自己的有效精确 Scope，只有该引用自身的 deny | 具备 ReadEntries 时按自身豁免读取；另一个引用有 deny 时仍拒绝 |
| 外来、已关闭或 NodeID 不匹配的 Scope | 明确失败，不回退 fresh 裸 ID 请求 |
| 业务授权拒绝或不能决定 | native 读取前失败，无目录数据返回；不因存在有效 Scope 绕过 |

既有[原生 Windows 验收记录](https://github.com/codetreker/remote-fs/actions/runs/34982701023)已经观察到 negative lookup 后的一秒可见性断言失败；该记录使用内存 authority，后续独立目录阶段未执行，因此不证明 Windows SQLite 持久行为。这个已知失败必须被新的实际验收解释并消除，不能仅将它列为理论风险。相关既有行为的源码入口为[File 契约](../../../../packages/storage/files.go)、[对象文件修改](../../../../packages/storage/objectstore/file.go)、[原生保留与发布](../../../../packages/metastore/sqlite/files.go)、[原生发布计量](../../../../packages/metastore/sqlite/publication.go)以及 [HTTP 文件控制](../../../../packages/transport/httprest/file_server.go)。

R-CON-5 的应用调用保证单位与 native Windows 缓存路径是明确的开放门禁。若单个 SMB/存储操作可证、整个应用调用不可证，报告两者边界；若 negative-name 缓存阻止 R-CON，Windows 目标尚未验收。将来确需 cache break 协议时须独立明确范围和成本，不能悄然扩大本提案或宣称已满足。

## 风险

**附加能力仍可能遗漏绕过路径。** 只给 File 加检查会漏掉路径写入、替换和 SetAttr；只给提交加检查会漏掉 enforced read。验收必须从全部入口向实际访问追踪，并以跨入口竞争证明覆盖。

**可选 NamespaceGuards 会产生真实冲突成本。** Windows 依赖完整名字集合判断时，同目录变化可能迫使重试，即使变化未触及它最终选择的叶名。这是客户端名字策略的成本；精确名字操作不能默认继承它。后续优化需要更精细的中立观察事实，不能让服务端引入 Windows 比较。

**元数据与删除使持久格式增加义务。** 不能把无法解析 payload 当缺失，不能丢弃已经确认的删除 intent；恢复、大小限制和配额必须与节点状态一起验证。纯元数据/目录引用也会延长对象保留，因此 MaxFiles 不能只计算有内容的 File。

**Strong 可以延迟删除。** 已接受的 pending-delete 不禁止后来取得的 Strong grant，固定删除效果必须等到匿名修改真正获准。持续的有效保护可能长期延迟清理，关闭会明确失败或仍待清理，intent 与 charge 保留；不能为了完成关闭而越过保护，也不能把这项成本藏成成功。

**生命周期选择影响重启后的冲突窗口。** FileSession 共生可以沿用当前引用退役；跨重启到期保护则需要额外恢复证明。本提案选择前者作为推荐方向，后者需要新增明确需求，不能由含混的“断线后失败”替代。强 S/X 剩余保护与已接受删除效果在两种选择中均保持。

**历史时间会影响旧 volume 可用性。** 新节点的 authority 时间不能修复旧节点缺失的 BirthTime。严格失败会阻止部分 SMB 查询及依赖它们的程序；兼容投影需要明确选择并验证，不能以新建文件成功来宣布历史数据也可用。

**Windows 原生缓存可能阻断既定目标。** 本机 SMB 不等于没有 OS 缓存，negative-name 缓存和大请求拆分可能使单个请求级证据不足。这个风险不能靠扩大范围、降低一秒保证或增加全局缓存设置掩盖；真实系统测试结果决定 Windows 接入能否宣布可用。
