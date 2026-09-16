# Agent Note: 中立文件能力复用引用与发布

Status: implemented

## 问题

平台客户端需要表达原子创建/替换、按目录身份访问、只查元数据的打开、共享限制、强制范围保护及删除意图。只在客户端保存这些状态，会让同一 volume 的其它入口绕过限制；把平台名字、mode 或锁协议下传，又会使所有远端部署承担某个平台的规则。

按名访问还需要完整的目录 metadata 和已打开对象的当前名字。公开枚举正确执行 ReadEntries，不能承担只有 snapshot metadata 权限的内部解析；保存打开时的路径也无法跟随另一客户端的改名。

已有 FileSession/File 已经承担保留对象、当前修订读取、同步修改、有限期限、动作核对与回收。扩展平台接入需要新的受控操作，但不需要再建立一套文件身份、内容提交和引用生命周期。

## 决定

### 在既有 File 核心上增加可选能力

[`storage`](../../../../packages/storage/capabilities.go) 分别暴露原子 OpenAt、身份 namespace、NodeReference、引用状态/Scope、metadata CAS、UseOwner、RangeControl、删除意图和条件文件修改。每种能力检查完整包装链；不支持即失败，不能通过路径重开、先验 Stat 或本地 mutex 模拟原子保证。

NodeID 分配器与 high-water 保持原有唯一身份。[活跃文件](2026-09-08-live-file-handles.md)继续拥有 File.ReadAt/WriteAt/Truncate 的同步行为，objectstore 仍以 Node→Reserve/Put→revision Commit 构造当前对象的补丁。已知未提交且清理成功的竞争才有界重试；未知提交、计量或清理不变成成功。

File 的字节方法不携带新的动作 ID 或平台句柄。NodeReference 使用同一 retainedFile、session/HTTP registry 与关闭排空，提供属性而没有字节方法。返回错误但仍带非 nil File/Reference 时，调用方继续拥有清理义务；包装器不能丢掉部分打开结果。Scope 只代表这个确切的原生引用，在所属 volume 上验证存活和 NodeID；经 FileSession/HTTP 使用时还核对会话绑定，不从同 session 或相同数字推导豁免。

### 平台属性与中立事实分开

Attr 包含 NodeKind、ID、Size、AccessTime、ModTime、可选 BirthTime/ChangeTime 及有界 opaque metadata。mode 类型位与权限解释留在 Linux 客户端；创建和打开传入 InitialMetadata，SetAttr 只修改共同时间。namespace 的版本由 authority 分配，只比较该 payload；空期望版本要求缺席，空数据仍是存在的值，其它 namespace 不被覆盖。

metadata 使用排序键与长度前缀的规范编码。可选时间明确区分未知与合法时间值；新节点在创建事务记录 authority 时间，内容、属性和名字的实际修改在最终事务维护 ChangeTime。旧节点与历史日志缺失的时间保持未知，replica 保留 authority 的值，不能用接收时刻或 mtime 生成历史。

Linux 的 `posix.permissions.v1` 是四字节 little-endian uint32，范围为 07777；存在但畸形报错，缺席只在挂载显示时采用普通文件 0644、目录 0755、符号链接 0777。Linux ctime 显示优先使用真实 ChangeTime，未知时显示 ModTime；这份投影不写回 authority，也不决定 Windows 的历史时间显示政策。

### 名字操作检查实际身份与观察

父目标既可以是 fresh NodeID，也可以携带目录引用 Scope。前者接受本次业务授权、种类和 linkedness 检查且没有引用豁免；后者必须是有效的确切父引用，失败不回退成 fresh 请求。LookupAt 是 metadata 查询；ReadDirNode/ReadDirNodeBounded 明确派生 ReadEntries，在同一 native 顺序检查所有适用 claims 并捕获目录。公共 replicated.List 同样回源检查，不能先检查权限再使用缓存答案。

可选 NamespaceGuards 核对实际观察的目录 revision、父子边与 root anchor，预算有界；FUSE 精确叶名操作不默认带全目录条件。rename 分开表达观察到的替换槽位和 OutputLeaf，最终事务验证源、替换对象与输出第三占位者。创建/改名返回捕获 Attr，Remove/RemoveDir 的已知成功可返回空 NameResult；不能为删除补做一次 Stat 或把空成功改成 EIO。

OpenAt 的创建、保留身份清空或新身份替换、初始 metadata、UseClaim、armed CloseIntent 与返回 Attr/Outcome 在一个原生有序操作内完成。ConditionalFileMutation 的显式大小/namespace 条件与效果同在最终发布检查；Append 根据当前 EOF 构造候选，已知 revision 竞争才重建，普通 WriteAt 不增加外部版本前置条件。

### metadata 披露与当前绑定独立观察

DirectoryMetadataObserver 在原 FileSession 下按父身份、可选 Scope 和 NamespaceGuards 捕获完整子项；IncludeName 选择同时取得目录自身绑定。ReferenceNameObserver 在原 File/NodeReference 下返回固定 NodeID 的 Root、Linked 或 Detached，Linked 带当前 ParentID/RawLeaf。Root 不是缺失的默认值，Detached 不重建旧名字。名字与另一次 State/Stat 各有自己的捕获，不承诺额外的属性/名字事务。

两种观察的 HTTP discriminator 固定授权为既有 OpReplicationSnapshot，不能由 caller 传任意 Uses 选择豁免。它们披露已有 snapshot 权限覆盖的有界 metadata，不授予应用 ReadEntries/ReadMetadata 或名字修改。引用仍须属于当前 session 且有效；允许 DeleteName 的引用不必增加 ReadMetadata 才能发现自己的绑定，后续 rename 仍受独立授权和最终 Scope/Uses/guards 约束。

可选 ReferenceIdentity 只读取原引用已有的非零 NodeID，不执行 I/O；关闭后身份可保留，操作的存活检查不变。名字能力依赖这个 getter，基础 File/NodeReference 和 FUSE 不新增要求。HTTP 支持名字能力时把旧式打开的标量身份留在原 action receipt，全部打开核对 getter 与请求/捕获身份；失败继续交给原 pending-open 清理拥有者。

原生观察先验证固定 SQL header，再按实际叶长检查名字驻留和边界编码预算，最后载入名字；不加载未返回父/guard 的 opaque metadata。IncludeName 通过 ListResult.ReservePrefix 在子项前计入自身名字，不伪造目录 entry，不按最大叶长浪费短名字预算。任何 header、prefix、entry 或取消错误使整份结果不可用；没有半份名字/列表成功。context sizing callback 是边界内部接合点，不序列化成远端 per-call 限额。

这些只读能力复用原 native gate、引用准入、HTTP 数据通道和包装器，replicated 回源，不新建身份、日志、schema、lease 或动作历史。成本是显式远端观察及客户端最终 guard 核对；它提供当前绑定事实，Windows 的名字投影、祖先组合和请求映射仍由平台接入完成。

观察能力的回归保留真实原生后端，同时把昂贵准备限制在判据需要的范围。objectstore 两个只读拒绝矩阵各建立一次不变的节点内容，每个子例仍独立创建并检查关闭 proxy、session、引用与 worker；底层 authority 由父 fixture 唯一关闭。native 目录字节边界在一个真实 mutation 中逐项准备，再检查精确 payload/count 和 volume 完整性；超长名字仍远超硬上限，并用比例分配界线及实际提前载入负向对照验证拒绝发生在物化前。它们不减少子例、改用虚假后端或放宽生产预算。

### Uses 与范围保持独立语义

UseClaim 的 Uses/Deny 双向比较。native Scope token 绑定实际引用，即使直接调用 metastore、没有 FileSession，也不能绕过已经生效的限制。FileSession 中注册的 UseOwner 指定 OwnerReference 或 OwnerExplicit；Group 只合并死锁参与者，不合并锁、引用或权限。

RangeControl 保留请求 epoch/nonce、有限历史、Query/Cancel 与等待。DomainRecord、DomainWholeFile 是 advisory，DomainEnforced 用显式 DenySelf/DenyOthers 控制数据访问；核心不推断 SMB 的重复独占申请或 exclusive-first 解锁政策。Bytes/Boundary 明确区分正长度范围与字节间边界，AddExact/RemoveExact 保存独立 ClaimID，Replace/Subtract 保留范围代数。Commands、Claims、Effects 分别最多 64 项，无法保留完整回执时在释放或授予前拒绝。

DropBeforeAcquire 的已知释放属于动作 Effects，即使后续冲突也不能说成完全未执行。原生 use/grant 与最终访问有序；长 SQL/对象 I/O 不占 coordinator mutex。引用先在 native gate 中 fencing、DropUse，再于 gate 外完成 Drop/RetireOwner 与等待推进；清理锁顺序不因新能力倒置。取消本身不能证明未授予，未知结果使受影响的访问失败。

### 删除义务与回收沿原拥有者结束

armed CloseIntent 绑定引用，与节点 PendingUnlink 分开。指定引用退役时激活或消费自己的 intent；非空目录的 OnReferenceClose 条件被消费而不删除，Now 在设置时要求其条件成立。清除当前 pending generation 不抹掉其它 armed intent。

native Retire 在释放 claims 前持久完成 intent 的触发，失败保留保护和清理所有权。仍有名字的最终删除经过正常 Strong publication gate，以固定、预先授权的清理效果执行，不复用过期 proof；有效 S/X 可以延迟它。已无名字的物理回收沿既有 cleanup 路径。[Strong](2026-09-07-file-locks.md)的获准名字移除仍令资源 TargetGone，不能用 retained NodeID 复活旧 grant。

MaintenanceAccounting 在既有 commit gate 中核对实际用量、初始化账本并替换一条维护 accounting chain。当前 incarnation 的活引用清理由创建时捕获的 hooks 负责；恢复扫描跳过它们。旧 incarnation 的恢复清理在同一 gate 下取得当前维护 chain，两者不重复结算。未知结果保留 intent、引用或 charge，关闭错误不被后台队列转换成已完成。

### v6 与 HTTP/v4

migration 6 将合法旧 mode 转成 NodeKind/POSIX payload，保留 NodeID、内容、用量、原始名字和历史日志事实，旧 BirthTime/ChangeTime 为未知；migration 1–5 不变。旧 v5 布局先 preflight，转换和版本推进在既有 SQL transaction 中完成，再走 Commit→witness.Accept。witness 不保存 schema version，新旧结构验证仍独立。schema 准备只回收已 detached 对象，有名 pending 必须等 Strong 初始化与恢复 gate 可用后再处理。

HTTP/v4 使用中立 Attr、metadata、范围及能力 DTO，旧 v3 路由明确失败。它扩展原 registry/journal/epoch/Open ACK，未为每个 native Go 方法增加 RequestID。直接 Go 调用一次，未知结果按原有 fencing/清理处理；HTTP 新控制操作沿原 journal 做相同输入重放，路径基础 API 保留各自错误边界。Close 的清理效果先完成，replica 再核对 barrier；不能为等待副本先跳过原拥有的清理。

返回属性通过请求级 AttrResultBudget 在载入 metadata、修改或保留引用前预留；目录结果用调用方的 ListResult 在变长数据载入前收费。HTTP 文件控制的 256 KiB envelope 与 Strong 控制的 16 KiB 分开，metadata 编码和 map 保留同时受限。详细接口、错误与参数由[文件能力设计](../../../../docs/design/server/file-handles.md)拥有。

## 备选方案

**只在平台客户端维护锁与名字检查。** 无法约束其它入口，也无法把本地名字观察与远端变更合成一个原子结果。平台解释保留在客户端，实际身份、冲突与发布检查由 authority 执行。

**重写文件核心，统一 EntryID、全操作 receipt 和独立 lease。** 当前 NodeID、同步 File、revision CAS 与回收已经承担这些职责；替换它们会重做对象身份和未知发布证明，却没有对应需求。新增控制请求使用原 journal，新增引用种类使用原 retainedFile。

**每次发送完整锁快照。** 快照覆盖会把独立区间的变化变成整体竞争，还须重新定义等待、转换和取消。命令式增删保留已有历史与顺序，独立 claim 只增加实际需要的 multiplicity。

**内部解析复用公开枚举或记住的打开路径。** 前者增加应用 ReadEntries 前提，后者在跨客户端改名后过时。独立 snapshot-authorized 观察保留权限分工，并提供最终修改可以再次核对的事实；不为一个当前名字查询维护整份 volume 缓存。

**远端解释 Windows 或 POSIX 属性与名字。** 会让某个平台改变所有入口的规则。NodeKind、共同时间与 opaque namespace 保持事实中立，平台 codec/错误映射在客户端。

## 后果

中立能力可以由 Linux 和编程入口使用，所有包装器保留引用、预算、scope、原始错误和后续清理所有权。代价是 native 原子操作、v6 数据转换、更多有界控制状态以及逐次权威目录读取；本地副本仍保存名称与属性，但公共 List/ListBounded 不以缓存绕过 ReadEntries 限制。

较宽的 metadata 使大量扫描排队也可能阻挡已更新副本上的短 Stat。内部 Replica 的 List/ListBounded 因此先取最多 N-1 个 listing 名额，再共享原 N 个 SQL 名额与公平读写门；默认仍是总量 16，扫描最多 15，不通过扩大连接数改变负载。Stat 不越过写者阶段，扫描也不在等待 quota 时持有阶段；[写者推进](../bug-fix/2026-09-07-let-replica-writers-progress.md)的原理由保留。

四个观察测试文件的局部配对中，两个 SQLite 根普通/race 包耗时分别从 1.447/17.660 秒变为 0.296/5.144 秒；objectstore 两个根、十八个 verdict 分别从 0.635/11.634 秒变为 0.099/2.651 秒。观察代码的已覆盖块未减少，原子例与 oracle 保留；1 MiB 名字的提前物化对照分配 2,108,088–2,108,200 字节，超过保留的 256 KiB 上限并在指定断言失败。这是相同 flags 的单次局部前后观察，不是隔离 benchmark 或整条 CI 的加速保证；目标 profile 也不代表整包覆盖门禁通过。

本决定部分接续[平台客户端提案](../../proposed/architecture/2026-09-16-platform-client-capabilities.md)的通用核心和[文件目标提案](../../proposed/architecture/2026-08-20-nothing-pins-an-open-file.md)的目录父身份。它保留 live-file、Strong、quota 与未知对象发布的既有理由；显式内容版本、R-CON-5 的应用调用单位和其它独立提案不因能力名称相近而完成。

Windows 本机 SMB 的实际接入与缓存透明性继续由平台提案承接。诊断原型的结果不替代交付实现；已交付的观察能力仍需接入 Windows 名字解释，共享模式/通知共存与历史时间显示不能由通用能力自动推导。本决定不宣称 Windows 已交付，不缩减规格中的 Windows 目标。
