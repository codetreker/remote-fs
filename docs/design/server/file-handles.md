# 保留文件与中立访问能力

本文描述 FileStorage、保留节点、身份 namespace、metadata、使用声明与范围控制。基础路径接口见[顶层设计](../architecture.md)，Strong 见[文件占有](file-locks.md)，Linux 解释见[client 设计](../client/architecture.md)。既有引用/内容决定由[活跃文件](../../../.agents/notes/implemented/architecture/2026-09-08-live-file-handles.md)拥有，能力扩展的取舍见[中立文件能力](../../../.agents/notes/implemented/architecture/2026-09-16-neutral-file-capabilities.md)。

## 一、身份与会话

FileStorage 在 BoundedStorage 之外提供 CheckFileStorage 和 NewFileSession。检查验证完整底层链的身份保留、发布 fencing、资源与返回属性预算；缺少原生能力报 EOPNOTSUPP，不能按旧路径重开。随附 objectstore/localstore 依赖独占 SQLite 文件能力，共享 opener 不取得另一拥有者的保留引用。

FileSession 拥有有限期限、引用、在途操作、owner 和动作历史。OpenFile 以路径打开，OpenNode 以 NodeID 打开；StatNode/SetNodeAttr 保留身份查询。File 始终是同一普通文件，ReadAt、WriteAt、Truncate、SetAttr、Sync 与 Close 不带平台句柄或通用 RequestID。旧 epoch、失效 session 或已退役身份失败，不绑定同名替代物。

FileOpenOptions 的 Read/Write/Create/Truncate/Exclusive、ExpectedID 与 InitialMetadata 共同验证。普通文件打开要求至少一种字节访问，OpenNode 不接受创建/排他；非排他创建可以打开竞争胜者，初始 metadata 只应用新节点。Truncate 与返回身份同属原子打开；目录/链接分别以 EISDIR/ELOOP 拒绝普通 File。打开不取回完整内容，也不隐式取得 advisory 或 Strong。

默认 session lease 为 30 秒、History 为 1 分钟，单文件 1 GiB；MaxFiles=4096、MaxOperations=64、MaxWaiters=256、MaxLockOwners=4096、MaxLockRanges=65536、MaxPendingLocks=1024、MaxLockActions=16384。零值 options 无效，调用方可显式选择默认集再调整。volume/HTTP 的总量边界可以更紧。

Renew 返回确认的 epoch、revision、保守剩余 lease/history；Status 只观察。client 从发送起点扣除请求耗时，旧响应、普通 I/O 或 TCP 存活不延长期限。普通数据调用不先做一次 Status RPC。引用关闭/权限不足为 EBADF，失效身份为 ESTALE，未知为 EIO；最后的许可仍在 native 入口检查。

### 可选接口

每个接口的 Check 方法检查整条包装链，call 在不支持时同样拒绝。

| 接口 | 责任 |
|---|---|
| AtomicFileOpener | OpenAt，原子打开/创建/清空/替换 |
| NamespaceAccess | LookupAt、ReadDirNode/Bounded、MutateName |
| NodeReferences | OpenNodeRef/OpenChildRef，返回无字节方法的 NodeReference |
| ReferenceStateAccess、ScopedReference | 同次捕获 Attr/LinkTarget/Detached/PendingUnlink；取得确切引用 Scope |
| DirectoryMetadataObserver | 按父身份捕获完整 metadata 列表，可选同时取得该目录的名字绑定 |
| ReferenceIdentity、ReferenceNameObserver | 不作 I/O 的固定 NodeID；显式捕获 Root/Linked/Detached 当前绑定 |
| MetadataAccess、ReferenceMetadataAccess | 按 NodeID 或保留引用替换一个 namespace |
| UseOwners、RangeControl | owner 注册/退役、冲突查询、批量编辑、动作核对和解除 |
| DeleteIntent | SetPendingUnlink/ClearPendingUnlink |
| ConditionalFileMutation | 按显式条件执行 Truncate/Attributes/WriteAt/Append |

NodeReference 使用同一 retainedFile、session registry、预算和 Close 排空，只提供 Stat/SetAttr/Close。Meta-only 与目录引用不伪装成可读写 File。State 的 Attr、原始 LinkTarget 与 pending/detached 状态由同一捕获提供，失败不能用 false 或当前路径查找补齐。

## 二、保留节点与回收

SQLite v6 保留既有 detached、content revision、NodeID allocator 和 high-water，并保存 NodeKind、metadata、可选时间、directory revision 和删除状态。名字移除后，仍有引用的文件/节点保留原身份；目录 detached 后不能接受新子项。日志与快照的名字树不包含 detached 节点，其后内容修改不制造旧路径事件。

MaxRetainedFiles 默认每 volume 65536，所有引用种类共享；原生 Scope token 和 Uses 也绑定这份引用。直接 metastore 打开即使没有 FileSession 仍受 native claims 约束。多个包装器不能通过重新建 coordinator 绕过共享预算。

Retire 在最终门处阻止后续访问/发布并触发已接受的 close intent，然后排空已准入操作、释放引用与内容。已知清理失败保持可重试拥有者；未知发布、结算或释放保留 charge，不能报告空间已回收。节点 pin 不代表捕获内容 revision 永不被清扫。

### 删除意图

每个引用的 armed CloseIntent 与节点 PendingUnlink 分开。OnReferenceClose 随原子打开接纳；它本身不阻止后来相容的打开或目录子项。该引用关闭/到期即触发，普通文件激活 pending，UnlinkIfEmpty 的非空目录消费意图而不删除。Now 在设置时验证条件并立即生效。清除要求当前 PendingGeneration，只清除节点当前 pending，不抹掉其它引用尚未触发的 intent。

触发/消费持久成功之前不释放相应 Uses/ranges；失败沿原 fencing 失败路径保留保护与清理拥有者。最后相关引用结束后，有名 unlink 以固定预先授权的系统清理效果进入正常 Strong gate；活动 S/X 可以阻止它，不能复用已过期 proof。cleanup 绕过只用于已经 detached 的物理回收。

当前 incarnation 的活引用持有创建时捕获的 cleanup/accounting hooks。恢复扫描跳过这些 live intent；它只处理旧 incarnation，并在 native gate 中取得当前 MaintenanceAccounting chain。BindMaintenanceAccounting 在同一个 gate 内读实际 Usage、调用不可失败的初始化回调、绑定一条维护 chain，不能把用量初始化与清理准入拆开。当前维护链不覆盖仍由 live 引用拥有的捕获链，两者不重复计量。

启动的 schema 阶段只回收已 detached 对象，保留有名 pending/armed 元数据。Strong authority 和恢复屏障建立后，有界 RetryPendingUnlinks 才能沿正常 gate 激活/消费旧 intent 和删除名字。受影响的新打开保持拒绝；旧引用不恢复为新 handles。完整恢复证据见[本地持久存储](local-disk-object-store.md)。

## 三、一次读取与一次修改

ReadAt 捕获同一 FileState 的 Attr、大小与字节；EOF 返回该状态的 Attr 和空数据，正长度短读可返回部分字节。后续调用可见其它已确认修改，不固定打开时快照。objectstore 物化不可变完整对象，捕获的旧内容被并发回收时有界重取；每次重捕获仍检查实际 ReadData/enforced 范围。当前引用内容缺失为 EIO，持续竞争耗尽为 EAGAIN。

WriteAt/Truncate 从当前状态构造区间补丁，保留未触及字节，扩展为零；Reserve、Put 与 native revision Commit 保持原流水线。上传不占最终许可，也不延长 session/Strong。最后事务检查引用、内容 revision、Uses、enforced 范围、Strong proof、预算与记账；只有已知未提交并已清理的 revision 冲突才重试。

ConditionalFileMutation 可携带 ExpectedSize、只读 ExpectedMetadata 及本次 metadata 更新的期望版本。条件失败不执行效果；Append 以捕获 EOF 构造候选，最终 revision 比较失败后才重新构造。它不为普通 WriteAt 引入打开时内容前置条件，也不提供跨应用大调用的事务。

每次实际创建记录 BirthTime/ChangeTime；内容、显式属性/metadata 和名字变化在对应原生事务维护 ChangeTime，replica 保留原值。旧数据 Unknown 保持 nil，普通读、renew、reservation/GC 或失败比较不生成历史。Sync 只检查已完成修改的健康/持久性，Close 没有未提交的本地 dirty 内容要发布。

### namespace 与返回结果

ChildName 使用 DirectoryTarget 加 RawLeaf。Scope 存在时核对 exact live 引用及用途，失效不能当作缺席；裸 NodeID 则是独立授权的 fresh 身份访问。LookupAt 是零用途的 metadata 查询，不等于公开列目录。ReadDirNode 派生 ReadEntries，在 native gate 内检查 claims 和捕获列表；无 Scope 时同 session 的 deny 也适用。

NamespaceGuards 同时验证实际观察的目录 revision、边与可选 root；精确 FUSE 操作不默认带它。每类 directories/edges 至多 256 项，总 guard 64 KiB；RawLeaf 至多 4096 字节，Scope token 至多 128 字节，TargetUses 至多 16 项。完整目录至多 65536 项及 8 MiB，调用方预算可以更紧；ReadDirNodeBounded 先 Reserve 名字/metadata，再载入变长值，错误使 ListResult.Fail，不返回 prefix。

OpenAt 的 Keep/ResetContent/ReplaceNode 分别保留状态、同身份清空和新身份替换；InitialState 按实际分支应用。原子效果还包括 UseClaim、intent、返回引用与捕获 Attr/Outcome。rename 的 ObservedLeaf/Expected 与 OutputLeaf 分别验证，不能覆盖第三占位者。Remove/RemoveDir 可成功返回空 NameResult；其余命令成功须有捕获 Attr。

OpenResult/NodeOpenResult 在错误时仍可带非 nil 引用，调用方必须关闭它；replicated/HTTP 包装器可以只暴露 cleanup 能力，不能丢掉引用。NameResult.Attr 与错误同返表示效果可能已经发生，取消不能改成可安全重试；已知回滚返回零结果。

### 显式 metadata 与名字观察

[DirectoryMetadataObserver](../../../packages/storage/directory_metadata.go) 是 FileSession 的可选能力。ObserveDirectoryMetadata 接受 DirectoryTarget、DirectoryMetadataOptions{Guards, IncludeName} 和空 ListResult，返回 DirectoryMetadataObservation{Observation, Name}。native 在同一 gate/读取事务中核对 guards、目录种类、linkedness 和可选确切 Scope，捕获 DirectoryRevision 与完整子项。它披露 snapshot 已授权的 metadata；不派生应用 ReadEntries，也不扩大应用 ReadMetadata。失效 Scope 仍失败，不能回退到裸 NodeID。ReadDirNode 与 public List 保持各自授权和 share 检查。

IncludeName 为 false 时没有 Name 字段或名字前缀预算；为 true 时，目录自身的 NameObservation 与列表属于同一次捕获，只能是 Root 或 Linked。所有错误使 ListResult.Fail，返回零 observation；只有方法与 result.Entries 都成功后，列表才可使用。成功完整观察可供客户端判断缺失组件；普通 ENOENT 不能证明错误发生在哪一段。

[ReferenceNameObserver](../../../packages/storage/name_observation.go) 可选地附在 File/NodeReference 上。ObserveName(ctx, guards) 使用原引用准入，返回固定 NodeID 及 NameRoot、NameLinked 或 NameDetached。Linked 具有当前 ParentID 和原始 RawLeaf；Root 确认选定 volume 的根，Detached 明确没有当前名字，两者均不带 parent/leaf。它不取得属性、不授予 rename/delete，不把重复绑定、缺失节点或损坏事实解释成 detached。Name 与另一次 State/Stat 是各自的捕获，不承诺共同快照。

ReferenceIdentity.ReferenceNodeID 只返回引用已拥有的非零 NodeID，不作 I/O、不取得 gate 或延长期限，关闭后仍可返回该身份；实际观察继续检查存活。它不是 File/NodeReference 或 FUSE 的必需接口。名字能力的检查要求 getter 成功，每次观察的 NodeID 必须与它一致；不支持为 EOPNOTSUPP，零身份或矛盾响应为错误。

native 先用固定 SQL header 核对节点/父种类、volume、detached、唯一绑定、名字存储类型和实际长度，再调用 NameObservationBudget，随后才载入叶名字节。未返回的父/guard 节点不加载 opaque metadata。默认名字驻留 charge 为 512+4×实际叶长；callback 只能以复制 scalar 和长度做有界尺寸计算，至少保留该 charge，不能 I/O、重入或保留输入。畸形 header 在 callback 前失败，短名字不按最大叶长预收费。

IncludeName 先将完整名字 charge 交给 ListResult.ReservePrefix，再为每个子项预留；prefix 不占 entry 数量。ReservePrefix 只允许在 entry 预留前调用一次，负数、溢出、重复或迟调用使整份结果失败。原有目录项/字节硬上限同时包含这个前缀。HTTP 还计入精确 JSON scalar、envelope 与实际长度的 base64，server 取 ResultBytes 与 MaxBody 较小值。目录请求再受调用方 ListResult 限制；本地 context callback 不被序列化，不能据此承诺远端 per-call 限额，client 在有界解码后执行自己的 callback。

名字观察没有 action、ACK、barrier、日志或新引用，失败返回零值并保留原因。objectstore 通过原 session/引用准入转发，limited/locked 保持上下文，replicated 回源；不能用本地副本或记住的路径重建当前绑定。后续名字修改仍需原 NameCommand 的 SameNode、Scope、Uses 与 guards 在最终事务核对，观察本身不证明稍后的操作成功。

## 四、advisory 范围与 owner

UseClaim 以 `(new.Uses & old.Deny) != 0` 或反向交集判冲突。实际操作派生用途，不能通过空声明绕过已生效限制；Reference Scope 只豁免自身的 claim，不豁免同 session 的其它引用。业务授权、读写权限和 claims 分别验证。

UseOwners 在原 session/coordinator 中注册 OwnerReference 或 OwnerExplicit，必须绑定活节点和 Scope。Group=0 是独立死锁参与者，非零 Group 只在 session 内归并等待图参与者，不共享范围或权限。owner 的诊断编号不授予访问，任意数值不能替代注册。

| 中立域 | 编辑与约束 |
|---|---|
| DomainRecord | Replace/Subtract；失败保留原范围，共享/排他只约束参与者 |
| DomainWholeFile | Replace/Subtract；允许明确 DropBeforeAcquire，Linux 选择 whole-file 范围 |
| DomainEnforced | AddExact/RemoveExact；独立 ClaimID，不合并重复获取；RangePolicy.DenySelf/DenyOthers 明确限制实际 ReadData/WriteData |

核心不从 RangeMode 推导 SMB 的全部申请政策。客户端负责其同 owner 的重复独占规则、精确解除选择和确认后的 ledger；原生 I/O 始终执行已经授予的显式 Policy。共享保护可配置 DenySelf=WriteData，使持有者也不得写入，不能因 owner 相同绕过它。

Bytes 是 unsigned Start 加正 Length，检查数学上的 `Start+Length-1` 不超过 MaxUint64；Boundary 是独立 CutAt，不从零 Length 猜测 EOF。Boundary 与 Bytes(a,n) 在 `a<CutAt<a+n` 时相交，两个 Boundary 不相交。FUSE 将 POSIX 的未来 EOF 转为对应正长度 Bytes，平台零长度解释留在客户端。

GetConflict 只查询实际冲突，不产生动作。Apply 对最多 64 条命令接纳一次，Claims、Effects 也各最多 64 项；完整结果装不下时返回 RangeTooLarge/EFBIG，不能先授予或执行 drop 再失败。DropBeforeAcquire 分两段有序处理，原锁的释放写入 Effects，即使后续 Pending/Rejected 也保留；其它原子编辑不把部分修改包装成未执行。

LockRequestID 仍由当前 action epoch 与随机 nonce 组成。同 ID 不同意图拒绝，旧 epoch 中未见记录不重新执行；历史保留原动作结果与 EverGranted。Query/Cancel 在 owner 退役后仍可沿 RequestNode 核对尚保留的历史。Pending、Granted、Rejected、Cancelled、Released 与未知分开；只有证明没有遗留授予时，平台才能返回可安全重试的 EINTR。

coordinator 在不持有 mutex 时进入 native Order，取得 gate 后进行有界状态检查；长 I/O 不在该 mutex 中执行。等待唤醒选出候选后重新进入 native 顺序检查，不能直接在 pump 中越过数据访问。DropUse 在 native gate 内进行，随后 gate 外的 Drop/RetireOwner 清理并推进等待。原生 fencing 失败保留状态，不用释放锁掩盖未完成的引用退役。

默认每 volume 最多 1024 session、32768 owner、262144 range、262144 action、8192 waiter、65536 死锁边；同一原生 volume 共享 coordinator，配置不一致拒绝组合。session data 由 MaxOperations 限制，心跳、范围申请、核对/释放沿原有独立 control 分区各允许两个活跃调用，大块物化不能耗尽续期和清理名额。

## 五、HTTP、复制与资源

HTTP/v4 使用原 file/file-control registry，servedFile 可持有 File 或 NodeReference；元数据引用没有可调用的字节方法。所有请求在触碰 capability、回放结果或 native 数据前先执行[业务授权](authorization.md)。替换、armed intent、reset/条件修改的额外语义也须准入，不能只授权一个包装操作。

| 请求类别 | 结果与核对 |
|---|---|
| OpenFile/OpenNode/OpenAt/OpenNodeRef/OpenChildRef | 原 action journal/input digest；成功引用先 pending，ACK 后交给 caller；相同动作不重复引用或 armed intent |
| namespace/metadata/条件修改/pending 控制 | /v4/file 交换，按操作分类使用原 journal；同 ID 不同 body、budget 或 scope 拒绝 |
| Range Apply/Query/Cancel | coordinator 的原请求 epoch/nonce/历史；取消不证明未授予 |
| Stat/State/Scope/LookupAt/ReadDirNode/metadata 与名字观察等只读 | 当前真实结果，失败不填默认值；不为读取新建通用 receipt |
| Close/Session.Close | capability 幂等及原 registry cleanup；清理成功后再捕获 barrier；缺失已关闭引用仍保留幂等成功 |
| 基础路径 Storage API | 原本的单次请求/错误规则，不改造成所有 native 调用都有 RequestID |

直接 Go 调用不隐藏重投未知操作。HTTP 对丢失动作响应使用原请求在有界 recovery context 中核对，未完成恢复时 fence 并退役 session。没有 Create/Truncate 的已有普通文件打开，若 ACK 为带 context.Canceled 的规范 EINTR，且同一能力原始 Close 结果为 nil，才保留无 File 的 EINTR；其余 ACK 不明或清理失败保持 EIO，不撤销已经发生的创建/截断效果。

支持名字观察的打开核对 ReferenceNodeID：OpenAt/NodeReference 与捕获 Attr.ID 一致，旧 OpenFile/OpenNode 与指定 ExpectedID/NodeID 一致。旧式打开只在名字能力存在时把 node scalar 加入原 action receipt，client 再核对请求身份；不支持该能力保持原回复形状。getter 或身份校验在打开后失败仍归原 pending-open 清理拥有者，不能补做 Stat。

原生 Open/Name 后置检查失败保留发生效果的 EIO 分类和原因；返回的可关闭引用不因 replica barrier 不可用而丢失。replicated wrapper 先执行 Close 与 authority 清理，再确认 progress；broken live stream 使确认失败，而不是阻止清理。detached 内容修改无路径事件，但仍可返回真实当前 barrier。

HTTP 文件控制 envelope 上限为 256 KiB，与 Strong 控制的 16 KiB 分开。ResultBytes 在返回 Attr/State/Payload/目录结果的请求中明确携带，server 取 peer、server 与相应 control 预算较小值。原生 producer 对将返回的目标检查 AttrResultBudget，再载入 metadata 或产生修改/retention；内部父观察不被错误地按返回目标预算计费。固定 scalar 与 canonical metadata 长度足以预检，callback 不作 I/O、重入或保留输入。

metadata JSON 上界与 MetadataRetentionBytes 的较大值用于 response 计量，原四倍响应池覆盖 wire、解码 map 与转换。无 metadata 的返回不额外收取 payload charge，较小合法配置仍可使用。ReadDirNodeBounded 在原始名字/metadata 载入前逐项预留，Range Apply 先验证最大完整 Commands/Claims/Effects response 能装进预算，再进入授予。

metadata 最多 16 namespace，每个 key 128 字节、值 32 KiB，规范编码总量 64 KiB，版本 token 至多 64 字节。nil/空初值不创建字段；present empty Data 不是 absent，返回版本缺失或畸形编码失败。原始叶名与 LinkTarget 使用 byte 字段，不能经 UTF-8 替换改变目标。

HandlerOptions.Files 默认整个 registry 64 session，每 session 分别最多 16384 MaxActions 和 16384 MaxCleanupActions，PendingAck 为五秒。advisory 的 MaxLockActions 是另一预算。历史满不驱逐仍有效记录，终止与清理沿原专用 admission。Handler.Close 只拥有它创建的 registry，backend 由调用方在清理完成后关闭。

SQLite 每 volume 的 metadata/link-target 总量由 Options.MaxMetadataBytes 限制，默认 64 MiB，独立于内容 quota。metadata_used 包含节点、detached 和 retained change 副本，由 SQL triggers 与完整性校验共同维护；它不是 HTTP map 驻留预算，二者均须满足。

原有物化预算保持：单文件 1 GiB、同时物化 2 GiB、最多 32 次 materialization、八次状态竞争尝试、单次数据操作三十秒。替换预留完整旧/新内容，读预留当前完整对象与区间；较低 session、transport、对象后端和 quota 限制共同生效。已知失败可清理 reservation，未知保留所有权。

## 六、与显式 S/X 的关系

Strong 不由普通 Open、范围锁或共享声明自动取得；有效 proof 不豁免 Uses/Deny 或 enforced Policy。S 不允许修改，X 只授权携带相符 proof 的实际目标，过期 proof 不能在没有竞争者时退回匿名执行。

获准 unlink/替换令原命名资源 TargetGone，原 Strong grant 不转到新对象。File/NodeReference 及其有效 share/enforced/advisory 状态仍按自己的原生引用生命周期作用于 detached NodeID；它们不复活旧 S/X。Strong 的持久期限/恢复屏障、FileSession 的 incarnation/续期、HTTP 连接与复制 incarnation 分开计时。普通 Windows session 不能从 Strong 的成功推导出持久句柄保证。
