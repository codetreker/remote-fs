# Agent Note: Windows 客户端接入中立文件能力

Status: proposed

## 问题

Windows 程序通过系统自带 SMB 客户端访问 volume 时，名字解释、共享模式、范围锁、属性及本机缓存必须同时符合其平台行为和跨入口的文件保证。原生客户端可能在远端提交后继续回答先前的不存在，内部目录监听也可能与应用本来有效的共享模式冲突；远端字节读写正确不足以证明网络驱动器可用。

Linux 和编程入口可以创建 Windows 无法表示或存在大小写歧义的名字。Windows 必须拒绝受影响的名字观察，却不能让无关名字变化使已有 File 的身份访问失效。metadata-only 打开又不等于公开列目录权限，内部名字解析需要的数据不能借一个被分享限制拒绝的用户列表调用取得。

[中立文件能力](../../implemented/architecture/2026-09-16-neutral-file-capabilities.md)已经接续本提案的通用核心：NodeKind/metadata、原子 namespace、保留引用、Uses/ranges、删除意图、v6、HTTP/v4，以及有界目录 metadata/引用名字观察。本文保留 Windows 本机接入及其尚需验证的组合，规格中的 Windows 目标保持完整，不因拆清已经交付的部分而缩减。

## 提案

[SMB 协议基础组件](../../implemented/architecture/2026-09-16-smb-protocol-primitives.md)部分交付本提案的报文/签名/认证依赖，Windows SSPI helper 也有独立实现；原生 SSPI 组件已有自己的实测，仍不代表 endpoint、映射、文件适配或组合后的认证/缓存验收完成。下述平台接入与剩余能力仍是同一目标。

### 接入形态与现有依赖

Windows 11 24H2+ 使用系统 SMB 重定向器；独立、可嵌入的 Go 客户端 package 负责本机 listener、身份隔离、share 发布/停止和 SMB 解释。远端继续使用中立接口，不部署 Windows 专有服务、不为 volume 切换名字模式，也不要求第三方文件系统驱动。业务方控制远端身份、凭据轮换与每个语义操作授权，默认仅创建者能访问本机映射。

接入复用现有 FileSession/File、NodeID 与同步内容管线，普通 Windows 打开随同一 incarnation 有效，断线未知时 fencing，authority 退役后旧 handle 明确失败。短暂 HTTP 超时不代表权限已解除，也不按路径重新创建引用或静默重获锁。Strong 的有限期限、恢复保护与 TargetGone 保持独立；不在此引入 durable/persistent handles、第二份 File lease 或通用动作 receipt。

| Windows 操作 | 已有中立能力 | Windows 客户端仍须完成的工作 |
|---|---|---|
| CREATE 普通文件 | AtomicFileOpener/OpenAt、捕获 Attr/Outcome、Uses、InitialState、CloseIntent | 六种 disposition、访问/share mask、初始化属性和结果映射 |
| metadata-only / 目录 CREATE | NodeReferences、ScopedReference、ReferenceStateAccess | 正确的 metadata/目录权限，引用失败后的清理；不能冒充 File 字节访问 |
| READ/WRITE/FLUSH | File.ReadAt/WriteAt/Sync 与条件 MutateFile | SMB 分段、部分结果与同步确认；R-CON-5 的跨应用调用单位保持未决 |
| QUERY/SET_INFO | 共同时间、namespace CAS、ReferenceState | 平台属性/历史时间显示与字节格式，真实 pending/detached 事实 |
| rename / disposition | MutateName、Set/ClearPendingUnlink | 观察名与输出名、generation、armed 与已 pending 的区别及平台状态码 |
| LOCK / CANCEL | UseOwners/RangeControl | 序列化的已确认 claim ledger、重复获取与精确解除政策、未知结果 fencing |
| 目录枚举与 CHANGE_NOTIFY | 权威 ReadDirNode/Bounded、现有日志/复制事实 | 名字表示、筛选和一致观察，缓存透明性及可取消的本机生命周期 |
| CLOSE / share Stop | 原 Close/Session.Close、barrier 与捕获清理拥有者 | 应用/内部资源区分、busy 卸载和失败后的真实结果；不重复计量 |

Open/create、错误非 nil 的部分引用、Remove/RemoveDir 空成功结果以及 Close 先清理再确认的事实遵守核心契约。包装器不以随后 Stat 重建原打开结果，也不把已知效果后的失败伪装成完全未执行。

### 名字解析与内部 metadata 观察

客户端按 Windows 规则检查原始名字的表示和无歧义性；非法 UTF-8、不可表示名字或冲突使相应按名访问、枚举及通知明确失败，不能删减成功列表或猜一个对象。Linux/SDK 名字规则保持原样，按已经保留的对象身份进行的 I/O 不因无关名字冲突失效。

变更的最终操作携带实际依赖的 NamespaceGuards：目录 revision、观察到的父子边与所需 root anchor。OpenAt、MutateName 等在原生有序操作里检查；rename 的 ObservedLeaf/Expected 与 OutputLeaf 分开，第三占位者不得被顺手覆盖。只按身份工作的请求不重走名字路径，精确 Linux 操作不默认取得这些 guard。

公开 ReadDirNode 的 ReadEntries 检查已经生效，不能为 Windows 内部 resolver 而削弱它。当前通知 manager 没有一份可直接承担名字解析的静态缓存；把 Linux SQLite Replica 直接带到 Windows 还涉及现有 nativelease 依赖及打开路径的可移植性，不能把交叉编译一个 HTTP 包说成完整复制栈可用。

已交付的 DirectoryMetadataObserver 复用 DirectoryTarget、DirectoryMetadataOptions{Guards, IncludeName} 与 ListResult，在原 FileSession/原生读取顺序中核对父 Scope/linkedness 和前缀 guards，返回完整子项与 DirectoryObservation。IncludeName 可同时取得父目录自身的 Root/Linked 绑定；名字前缀与子项在载入前按实际大小计费，任何错误使整个结果不可读。接口与预算由[文件能力设计](../../../../docs/design/server/file-handles.md#显式-metadata-与名字观察)拥有。

Windows resolver 仍须把这份能力接入名字投影与逐组件选择。HTTP 的只读 discriminator 固定映射既有 OpReplicationSnapshot，披露 snapshot 已授权的 metadata 子集；允许公开枚举不隐含该权限，也不能把被 ReadEntries 拒绝的显式 QUERY_DIRECTORY 改走此入口。客户端不能传任意 Uses 选择绕过行为。Windows 直接使用 portable HTTP，不因此引入 SQLite/replicated 的未完成移植工作。

只有这种成功、完整且 guards 相符的观察才能判定哪个名字组件缺失。普通 Open/Lookup 返回的 ENOENT 可能来自父身份或其它阶段，不是“已证明最终叶名不存在”的凭据；不能用临时打开引用进行无副作用的缺失探测。

### 已打开对象的当前名字绑定

ReferenceNameObserver 已能在有效引用上捕获当前 NodeID、ParentID/RawLeaf 或明确的 Root/Detached。ReferenceIdentity 是无 I/O 的可选标量 getter，不能授予观察或延长期限；State/Stat 仍不返回名字。Windows 按 handle 的 rename 和当前名字 QUERY_INFO 需要使用这份观察，不能继续依赖打开时的旧路径。

handle rename 以观察到的父/叶名、SameNode 和实际引用 Scope 构造既有 NameCommand，最终原子检查 source、Uses 与 guards；期间再次改名返回已知条件冲突后可在原请求预算内刷新。Root/Detached 不能伪造源名字；未知修改不重新提交。

当前路径查询从引用 ObserveName 开始，逐父调用 IncludeName 的完整目录观察。每层检查 Windows 表示和歧义，保留有界边/目录 token 后释放完整子项；到达选定 root 后，再对最初引用调用带全部 guards 的 ObserveName，核对其存活和整条链。只有最终核对成功才返回组合路径。循环、深度/guard/输出上限、断线和失效引用均明确失败，不扫描全 volume 或复用旧路径。

这段平台遍历和结果映射仍待接入。名字观察与另一次属性查询不是同一快照；不为 QUERY_INFO 虚构已实现的 Attr-plus-name 原子事务。公开目录枚举、属性、rename/delete 各自仍按自己的权限执行，snapshot-authorized 内部观察不替代这些操作。

### 打开、共享与删除的映射

Windows 依据观察到的 payload 决定允许的访问、破坏性 disposition 或 CloseIntent 时，须把实际依赖的 namespace 版本/缺席条件放入已有 OpenAtOptions/NodeRefOptions.ExpectedMetadata，并绑定 SameNode。只有最终旧目标仍满足条件才能打开；已知无效果条件冲突可在原请求预算内重新观察。缺席目标使用 Absent 和空条件，不能把新节点的缺席 payload 当作对旧对象的确认。平台的只读/hidden/system 等解释仍在客户端，中立核心只比较版本。

OPEN/CREATE/OPEN_IF 分别选择已有、缺席或任一存在性；OVERWRITE/OVERWRITE_IF 使用保留身份的 ResetContent，SUPERSEDE 使用 ReplaceNode 并保留被 pin 的旧对象。创建/清空/替换的共同时间与 Windows payload 进入对应 InitialState，同一次成功返回捕获 Attr/Outcome；不能先 Stat、改内容再补初始属性。

共享模式映射成中立 Uses/Deny，所有入口继续接受 native 冲突检查。应用的 metadata-only 不虚构字节读取，普通目录枚举必须具有 ReadEntries。内部系统资源不能悄然增加一个会阻止本来合法应用打开的 share claim；也不能为内部监听而忽略应用明确的共享限制。

范围命令使用既有 Bytes/Boundary 和独立 ClaimID。Windows 客户端序列化已确认的 ledger，解释自己的同 open 重复获取、共享/独占和精确解除顺序；核心执行显式 DenySelf/DenyOthers。只有成功响应或同动作核对证明的结果进入 ledger，未知时停止依赖该状态的后续选择并清理，不能再申请一次猜测原锁不存在。

FILE_DELETE_ON_CLOSE 对应 armed OnReferenceClose，指定引用结束时触发；显式 disposition 对应 Now 或当前 pending generation 的清除，不能抹掉其它 armed intent。State 提供同次捕获的 Attr/LinkTarget/Detached/PendingUnlink；CLOSE、断线与 share Stop 必须保留已接受的持久删除义务和真实清理错误，普通 session 的退役不等于撤销已提交效果。

### 原生缓存透明性

[固定诊断入口](../../implemented/testing/2026-09-16-native-smb-cache-diagnostic.md)已经提供可考的原生观察，结论按基底、overlay、variant 和实际系统分别成立。带连续性 overlay 的监听可以跨越被测的通知间隔，但目录共享对照也显示 LIST/share-deny 与内部监听可能在重定向器侧冲突；不能只靠服务端特殊放行解决尚未发出的请求。

接入必须选择一个既满足一秒、非 TTL 可见性，又不改变应用共享行为的实际路径。FindFirstChangeNotification 共存性与最终缺失状态的独立诊断不等于产品选择；缺失状态只可由成功的 guarded metadata 观察确定最终/中间组件，未知、权限或父身份失败仍报错。相同 Win32 映射也不能证明重定向器缓存相同。

生产验证使用实际 package、HTTP/v4 与新 backend，不使用诊断原型代替。需要覆盖文件内容、属性、名字不存在、目录结果、改名/删除及已打开引用，断线时不能让缓存报空或不存在。全局 cache lifetime、安装驱动、放宽系统安全设置和完整 lease-break 协议不因一个失败的探针自动进入范围。

### 时间与平台属性

共同 BirthTime/ChangeTime 由原生所有创建/修改入口维护，Windows payload 只保存平台专有属性；不能把会被 Linux/SDK 修改的 ChangeTime 只放在 Windows blob。不存在的 DOS 扩展位与畸形的 present payload 分开，后者明确失败，其它平台的 namespace 不被覆盖。

历史节点的 BirthTime/ChangeTime 可能未知。严格拒绝必需时间查询会影响旧 volume 的网络驱动器可用性；显式兼容投影则须决定显示值、适用条件和“非历史事实”的含义，不能写回 authority 或被当成真实创建时间。该显示选择保持未决；Linux 已选择的 ctime/权限展示不能自动成为 Windows 政策。新建文件成功也不能证明历史数据可用。

## 备选方案

**只实现本机 SMB 协议。** 协议处理之外仍有 native 缓存、共享冲突和跨入口效果。只用模拟客户端或纯内存测试会漏掉真实重定向器行为。

**内部名字解析使用公开 ReadDirNode。** 它正确执行 ReadEntries；在目录列表被 share deny 拒绝但 metadata 查找仍合法时，不能承担内部 resolver。独立 metadata 观察复用既有 snapshot 权限，不削弱公开枚举。

**直接复用 SQLite Replica 作为 Windows 名字缓存。** 它能提供静态名字视图，但依赖图和打开路径仍有 Linux nativelease 移植工作，通知 manager 本身也没有这份完整缓存。按父有界观察只增加实际需要的查询与 guards，不把这次接入扩大成数据库移植。

**把所有名字按 Windows 规则存进远端。** 会改变 Linux/SDK 的合法名字及 share 发布的效果。平台只解释自己的观察，原始名字、NodeID 与中立条件留在 authority。

**增加统一 EntryID、独立 File lease 或全操作 receipt。** 已交付核心已经提供对象、保留引用、命令历史和同步确认；Windows 接入继续复用它们，不能因平台实现而再次替换。

## 验收标准

- 实际 Windows 11 24H2+ 的系统客户端与资源管理器经过本机 package、HTTP/v4 和目标 backend 工作，默认本地身份隔离成立；跨编译或原型通过不代替此项。
- 原生缓存路径不依赖 TTL 经过；应用独占/共享打开、内部资源、正常/失败卸载以及断线错误组合全部保持语义，R-CON-1/R-CON-2/R-CON-4 分别有实际证据。
- 内部 metadata 观察使用固定 snapshot 权限，ReadDirNode/Public List 的拒绝保持。Scope、前缀替换/歧义、零结果预算、非法 UTF-8、多小 namespace、错误及取消均不产生部分成功；普通 ENOENT 不推断缺失组件位置。
- 另一客户端改名后，按已打开 handle 的 rename 使用有界当前绑定观察并在最终操作核对；当前绑定获取前/后的替换、再次改名、detached 与关闭都不能落到旧名字的新对象。
- 六种 disposition、metadata-only/目录引用、rename 的观察/输出叶名、大小写冲突、armed/Now/clear 的区别、范围 multiplicity 与核对/取消，均经真实平台请求验证。
- Windows、Linux 与 SDK 相互验证 Uses/Deny 和 enforced 范围，普通 advisory 与 Strong 分别保持。authority 重启使旧普通 handle 明确失败；Strong 恢复和 durable pending 效果继续兑现。
- 历史时间显示政策明确并有旧 volume 实测；新建与跨入口修改的共同时间来自真实 authority。不得以新的默认值填平未知历史。
- 返回结果、引用/owner/queue/payload/trace 均有界，失败清理可归属；相关包 normal/race/no-skip、包内覆盖与真实平台结果分别记录。源码检查不能写成运行通过。

## 风险

内部 metadata 观察与公开枚举使用不同既有权限，映射错误会造成越权或把合法查找误拒绝；已交付 facet 的固定语义须在 Windows 适配中保持，不能给调用者选择豁免或将公开 QUERY_DIRECTORY 改走内部入口。

原生缓存和共享模式的组合仍可能让某条方案不可交付。出现失败要明确其实际条件并继续处理既定目标，不能通过降低一秒保证、忽略 deny 或把诊断源码当生产实现掩盖。

历史时间严格失败与兼容投影有不同的旧数据可用性成本，未选择前不能承诺所有已有 volume 都能完整呈现。R-CON-5 的大应用调用单位同样保持未决，不新增跨请求快照/事务，也不缩减已有单次操作保证。
