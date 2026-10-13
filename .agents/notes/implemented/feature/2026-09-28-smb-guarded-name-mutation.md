# Agent Note: SMB 受完整路径保护的名字修改

Status: implemented

## 问题

SMB 按 Windows 名字修改文件树，已打开句柄却绑定对象。其它入口可以在观察与提交之间改名、移动祖先、替换末级名字或改变 Windows 属性。只保存打开时路径、只比较末级 raw slot，都会让动作越过 share 根或作用于同名替代物。响应丢失后重新观察当前路径，也不能证明原动作是否执行。

目录改名还需要区分原目录自己的打开引用与仍处于其子树的后代引用；host 只知道自己的 FileId，不能证明 authority 上没有 Linux、SDK、metadata-only 或尚未结算关闭的后代引用。Supersede 与普通 disposition 也分别依赖固定效果用途、真实返回权限和可清除状态，不能通过复制一套名字接口或恢复账本解决。

本决定落实 [Windows 网络驱动器总方案](../../proposed/feature/2026-09-16-windows-network-drive-support.md)的 8.1，接续[有界原子打开](2026-09-28-smb-bounded-create-close.md)、[目录枚举](2026-09-28-smb-directory-enumeration.md)与[关闭删除义务](2026-09-28-smb-close-delete-obligation.md)。需求为 [R-FS-6、R-FS-8、R-FS-9、R-WIN-2、R-WIN-4–7](../../../../docs/spec/requirements.md)。调研基线与协议证据保留在[复用调研](../../../../docs/research/2026-10-13-smb-name-mutation-reuse.md)；实际调用链见[SMB 端点](../../../../docs/design/client/smb-endpoint.md)与[文件句柄](../../../../docs/design/server/file-handles.md)。

## 决定

### 范围与复用

SMB 支持普通 FileRenameInformation、FileDispositionInformation 与 SMB 3.1.1 FileNormalizedNameInformation，并支持第六种 FILE_SUPERSEDE 打开。rename、move、replacement 使用现有 MutateName；文件／空目录删除由 ordinary disposition 与现有最后引用关闭路径完成；中立 NameRemove／NameRemoveDir 同样接受完整 guards。

新增公开字段只有三个：`NameCommand.Guards`、`NameCommand.RequireNoOpenDescendants`、`OpenAtOptions.ResetUse`。StateScalars 暴露的是已有 PendingGeneration；CLEAR 的空 token 使用已有 Generation 字段表达无 pending CAS。接口、RPC、schema、默认额度、action 类型和恢复 registry 不增加。

| 已有机制 | 本决定中的职责 |
|---|---|
| MutateName、RenameTarget、ChildCondition | 源／目标身份、metadata 条件、observed/output leaf 与第三占用者核对。 |
| NamespaceGuards 与 authority checker | 源／目标 ancestry 的单个有界 union，最终效果点验证。 |
| ReferenceNameObserver、IncludeName、ListResult | 从原引用当前关联向上捕获源路径，按完整 sibling 集做 Windows 投影。 |
| 原 coordinator.pins、fileDomain 引用额度 | 目录 rename 的跨入口后代打开检查；保留既有 pin 生命周期。 |
| ResetContent、Initial.OnReset、Reset outcome | 原对象 supersede、原子 metadata 与新 per-open FileId。 |
| Set／ClearPendingUnlink、共用 generation、CloseIntent | ordinary episode、CAS、独立 DOC 义务及最终删除。 |
| mutationOwner、pendingWrite、FileActions、barrier | 原动作恢复、FIFO、有限重观察、真实责任与额度结算。 |
| recordRenamed／Removed／NamedChanged | 旧／新 location、原对象身份和动作关联。 |

Windows 名字、属性、NTSTATUS 与协议 flags 留在 SMB。authority 只接收 raw 名字、NodeID、neutral uses、opaque metadata、条件和动作 ID。locked／limited／replicated 按实际 backend 配置转发同一完整值；直接和 HTTP 调用都到同一 authority 效果，不要求固定 wrapper 顺序。

### 完整路径命令与最终事务

NameCommand 的可选 Guards 是一个 rooted 图：源、目标共同 directory revision 与 exact edge 去重；同一节点的 revision 或绑定不同立即冲突。存在 terminal child edge 可以指向普通文件，不要求其 child 也是 observed directory。一个 union 共同受 256 directories、256 edges、64 KiB guard bytes 和实际请求预算约束。

NameCommand.Check 核对 rooted coverage、实际 source／destination 父目录、存在 terminal binding 与允许的分支，拒绝缺祖先、错终点或无关证据。Absent 由父 revision 与原 ChildCondition 表示；OutputLeaf 仍由 RenameTarget 的第三占用者检查保护。SQLite 再把 RootID 绑定实际 s.root。

prepareNamespaceMutation 的先行 inspection 与最终 transaction 都核对证据；最终 write transaction 按同一 commit gate 验证 revision／edge、源与目标 ChildCondition／metadata、pending、DeleteName／sharing、目录种类／空／防环、输出槽与请求的后代引用条件，随后写 entries、detach／回收、时间与现有事件。任一失败零名字效果；exact same-name no-op 也不能绕过 pending、sharing 或后代引用条件。

aggregate pending 一律阻止新名字 mutation；持有 DeleteName token 不能免除这个条件。原关闭删除沿 native trigger／discard 路径完成，不依赖 MutateName 的 token 例外。rename replacement 分离目标旧关联，兼容旧引用保持原 NodeID；新路径指向源对象，不能转移旧引用。

objectstore 与 HTTP 各自从已有 child canonicalization 提取 guard helper，供 ChildSelection 与 NameCommand 共用排序与 deep clone。合法 guards 在 clone、digest 和转发中保留完整事实；空且 RootID=0 的 NameCommand union 在产生效果前由公开命令检查拒绝，不能从各层 helper 的空值归一行为推导其合法性。ChildSelection 的既有 empty-to-nil 语义保持。guards、后代条件、metadata 与 ResetUse 都进入既有 action digest，重排相同证据不改变动作，改变 payload 以 EINVAL 拒绝。

### 原引用的当前名字

name_selection.go 从 h.reference() 的 ObserveName(nil) 开始，记录原 NodeID／binding；Linked 时以 ParentID 调用 IncludeName 的完整有界目录捕获。projectDirectory 验证所有 sibling 的 UTF-8、Windows 可表示性与 ordinal 唯一性，再以 exact raw leaf 与 NodeID 找到 descendant。最低层捕获提供 source Attr 与 Windows metadata token。

每层只保留 compact revision、exact edge、父 binding 和必要 source Attr／token，释放完整 listing 后继续向上。构造中的 partial guards 使用 RootID=0，避免尚未捕获的祖先被当成已证明；到 tree 固定 RootNodeID 后才设置 rooted graph 并整体验证。visited identity 防环，终点不能是另一根。forward resolver 与 upward capture 共用真实 staging／entry／compact 保留计费，forward 专有的祖先排除不用于 upward 列表，因为父目录合法地包含已走过的 child directory。

helper 返回任何名字或 SET metadata 条件前，都调用原引用 ObserveName(sourceGuards) 并比较原 binding；这在一个 authority capture 中核对整条路径，拒绝由不同历史时点拼出的、从未存在的路径。rename 目标复用 resolveCreateName，按 share 根解析；合并证据后再次 ObserveName(union) 比较同一 binding，最终 MutateName 还会重验。

观察使用现有 reserveDirectoryCapture／entry accounting／releaseDirectoryBytes，先计费 HTTP staging、projection 和 retained compact bytes，再装载／克隆变长数据。无法完整观察、歧义、取消或祖先异常均失败，不回答旧开时路径或虚假的不存在。

### 目录后代引用禁令

SMB source directory 的 NameRename 设置 RequireNoOpenDescendants；仅该 operation 允许该字段。FUSE／既有 raw-name 调用保持 false。source 自身引用不算后代，仍按其 DeleteName 与共享检查；任一 authority-retained 后代引用即使允许 SHARE_DELETE 也阻止动作。

同一 coordinator.pins 按 volume 嵌套 NodeID／count，私有 count／retain／release helpers 服务现有接纳、关闭、unlink 与物理回收。没有第二引用清单、subtree index 或 counter。metadata-only／Use=0、未知打开效果的引用和尚未最终释放的 close pin 都计入；最后已结算释放删除空 volume group。

最终 namespace transaction 在 exact no-op 早退前，只遍历当前 volume 的正数 pinned NodeID；candidate 数由既有 MaxRetainedFiles 约束。nameObservationHeader 的 entries_by_node indexed LIMIT 2 查询验证 kind、Detached、唯一关联、parent kind／volume，沿 ParentID 向上走，不装载 raw 名字。source 自身排除；只有已核实 Detached 且没有 association 的 candidate 才忽略。到 source 返回 EACCES；到真实 root 表示不在子树，环、损坏或无法确认失败。

MaxIntegrityRecords 同时限制 candidate 与 ancestry 工作量；超限 EFBIG、I/O、取消不能解释为无后代打开。grant、close、move 和检查共享 commit gate，Unknown release 不构成已关闭证明。替代 backend 必须履行该中立条件，不能用 host-local 扫描代替；不支持时明确失败。

### 协议投影

class 10 TYPE_2 按首字节 Boolean 解释 ReplaceIfExists，非零为 true；七字节 reserved 与合法 padding 忽略。固定头、空／odd／越界 counted UTF-16、非零 RootDirectory、leading backslash 目标按各自长度或参数状态拒绝。目标 share-relative，source 来自原句柄当前关联。exact raw 同名是经过全部条件的成功 no-op，case-only 是保持 NodeID 的真实 rename。

source READONLY 本身不禁止 rename；不同已存在目标在 ReplaceIfExists=false 时 collision，目录或 READONLY replacement target 为 ACCESS_DENIED，pending 为 DELETE_PENDING。Windows metadata CAS 固化准入观察；Linux／SDK raw 名字和属性解释不改变。相容旧目标引用继续访问旧对象是项目身份保证，与 MS-FSA ordinary rename 的 opened-target 条件须在原生资格中区分。

class 13 buffer 长度零为 INFO_LENGTH_MISMATCH；长度至少一字节只读取首 Boolean、忽略 surplus。TRUE 与 FALSE 都要求原 FileId 的 DELETE 以及当前授权／live DeleteName use。TRUE 检查 READONLY 与目录为空；FALSE 不执行这两项检查。

class 48 FileNormalizedNameInformation 以当前权威路径输出 leading backslash、backslash 分隔符和 root 的单 backslash。uint32LE FileNameLength 为完整真实 UTF-16LE 字节长度，无 NUL。OutputBufferLength<8 为带既有 SMB 3.1.1 error context 的 INFO_LENGTH_MISMATCH；其它不足返回 even-byte prefix、真实全长、ByteCount=4+prefixBytes 与 BUFFER_OVERFLOW。known Detached 为 INVALID_PARAMETER，无法确认是 I/O failure。direct remote class 9 保持 NOT_SUPPORTED。

connection.responseBudget 在 compound 接纳前按 variable class 48 实际上限计费；relatedOperationFailed 只接受经验证的正常 QUERY_INFO warning body，使合法 class 48 overflow 后的 related CLOSE 执行。不能把截断结果长度当完整名字长度，也不能把畸形 warning 当成功继续。

### ordinary disposition 与共用 generation

TRUE 使用原 PendingUnlinkCommand。源 Windows metadata 来自已验证的 bounded directory capture，ExpectedMetadata 固化版本；native retained-reference transaction 核对原 NodeID、当前唯一关联、DeleteName／sharing、metadata 与目录为空。该动作按原对象当前关联设置状态，不重新按旧路径选对象，因此不增加 Guards／Selection 字段。

State(StateScalars) 携带既有有界 PendingGeneration，只有 aggregate PendingUnlink 为真才有非空 token，最多 64 bytes。它不含 opaque metadata、时间、link target、内容或 revision，也不授予 DELETE／ReadMetadata。SET／CLEAR 的既有 FULL mutation result、预算与 receipt 保持；完整 metadata 超过结果预算时真实失败，不由 scalar 投影规避。

ordinary／DOC 共用 pending_generation。DOC trigger／startup 的 aggregate false→true 增量保留；ordinary SET 以 ordinary 位 false→true 推进，即使 DOC 已使 aggregate pending 为真。重复 ordinary TRUE 与 CLEAR 不额外推进，后续 ordinary episode 有新 token，阻止持续 DOC pending 下 clear／set 的 ABA。

CLEAR 的 Generation 仍是必填 canonical JSON 字段；空字符串表示 nil、即明确期望 aggregate false，missing／null 拒绝。每次 no-op 也执行实际 OpFileClearPendingUnlink 授权和原 runFileAction。native 先核对 live ownership／publication、DeleteUse／sharing，再捕获当前 aggregate token 与 ordinary 位；逐字节不符为零效果 conflict。

匹配而 ordinary false 时，通过现有 planned transaction 的 mutate=false 分支返回有界 FULL state 并核对 publication，不改变 event、ChangeTime、quota、generation 或 strong publication。匹配且 ordinary true 才按原 SetAttrMutation 清 ordinary 位。DOC-only 的正确 token CLEAR 是授权 no-op，不触碰 delete_intents；空 token 遇到新的 pending 不能成功。

最后相关引用关闭后的删除仍由原 native close／discard 完成，不额外按旧名字 unlink；普通 disposition 不建立新 durable intent 或 recovery session。

### 原对象 supersede 与真实权限

FILE_SUPERSEDE 先固定观察分支。已存在普通文件使用 SameNode／ExpectedMetadata、Create=false、ResetContent、ResetUse=DeleteName；不存在使用 Absent、Create=true、Keep、ResetUse=0 与 OnCreate。存在性改变在效果点冲突且零效果；只有原动作被绑定 NotExecuted 证明后才能重观察、新 action 和重新按分支授权。

存在分支通过既有 normalizeAccess 增加有效 DELETE／WRITE_ATTRIBUTES／WRITE_EA 和 WriteMetadata，保持 incoming READ／WRITE／APPEND data rights，不增加 ReadMetadata；DOC 仍保留独立 incoming DELETE 检查。该分支即使没有字节权限也用 AtomicFileOpener。不存在分支仅按普通 create 的 incoming rights 授权；有 data rights 用 OpenAt，无 data rights 用现有 regular OpenChildRef，不放宽 ordinary AtomicOpen 的 no-data 校验，也不附加 removal 授权。

ResetUse=0 保持 normal WriteData reset；非零仅接受 DeleteName，且要求 SameNode、Create=false、ResetContent 与 DeleteName claim。固定 reset effect 以 CheckUse(DeleteName) 校验，不向 data-only CheckIO 传该用途；仍是既有 WriteMutation，保留 quota、strong、content publication 和耐久排序。OpenAccess.Write 是实际字节 grant，Truncate 是效果；HTTP 对 DeleteName reset 补既有 OpVolumeRemove 和实际 metadata／attr 授权。

OnReset 以请求的支持属性集合替换 Windows 可设置位、强制 ARCHIVE；已有 HIDDEN／SYSTEM 必须出现在请求中。内容、ModTime／ChangeTime 与 Windows metadata 同次提交，Birth／Access 与其它 namespace 保持。READONLY 按 incoming data-write／append 与 DOC 规则核对，不 blanket 拒绝仅 DeleteName 的 reset。

native 仍返回 Reset 和原 NodeID；只有 CREATE disposition 0 把它投影 FILE_SUPERSEDED，overwrite 的 Reset 为 FILE_OVERWRITTEN。本次引用得到新 SMB FileId，旧相容引用保留原对象。零 data grant 的 File.Read／Write 和公开 Sync 返回 EBADF；read-only／write-only Sync 保持原合同。精确 CloseWithResult 不调用公开 Sync，关闭和 adapter settlement 不受该 guard 阻挡。

新增 supersede 模式在既有 context shape 验证后、效果前拒绝非零 AlSi 为 NOT_SUPPORTED；无／零 AlSi 继续。其它 CREATE 模式的既有 allocation gap 保留独立资格边界，本决定不新增 allocator 或声称忽略的 reservation 已授予。

### 统一 owner、有限重观察与计费

原 mutationOwner 保存 exactly-one 私有 File／Name／SetPending／ClearPending command、operation、clone、charge 和具体结果校验。NameResult 的 source identity、合法无 Attr remove 和 ReferenceState 的原对象／pending generation 先按具体 variant 验证，再进入现有 Attr 诊断与 settlement。

同一 h.pendingWrite、FIFO、FileActions、MutationBarrier 和 WriteOwnerID 沿用。Committed 只接受归属原 Action／operation 的结果；只有绑定原动作的 NotExecuted 允许有限重观察与新 action。Completed／Pending 使用原 payload typed replay；Unknown、Retired、无 operation 的证明或查询失败保留原动作和责任，不猜当前路径。native Released 不等于外层 Settled，CLOSE／Stop 继续报告未结清 owner。

重观察由原 bounded capture 计费；获得 fresh payload 尺寸后，在 clone、安装或 dispatch 前由同一 writeLedger 原子调整 charge，预留 old+fresh owned payload 并存峰值。成功替换后释放旧 payload、降到 fresh charge；不足或构造失败回滚临时 charge，不装新 action、不产生新效果，保留原 NotExecuted diagnostic。没有第二 registry 或 retry framework。

现有 Config.Logger、operation headers、WriteFailure／Status／HandleOwners 关联原 opaque owner／action，记录拒绝、dispatch、核对、Unknown、有限重试与关闭结算；日志不含 credentials、内容、raw 路径或 metadata payload。事件继续由 recordRenamed 保留同动作旧／新 location，8.3 可分别投影同目录 OLD／NEW 与跨目录 REMOVED／ADDED。

### 模块归属

| 位置 | 当前责任 |
|---|---|
| `smb/commands_name.go`、`name_selection.go`、`namespace.go` | 协议执行、原引用当前 ancestry、source／target union 与完整观察计费。 |
| `smb/commands_file.go`、`create_contexts.go`、`internal/wire/file_info.go`、`connection.go` | supersede 分支、权限／outcome、AlSi、class 10／13／48、响应与 related warning。 |
| `smb/mutation_owner.go`、`write_diagnostics.go` | 原责任链的 command union、具体结果与 peak charge adjustment。 |
| `storage/capabilities.go`、`capability_validation.go`、`reference_state.go` | 三个中立字段、组合／coverage 校验及已有 scalar token。 |
| `objectstore/file_actions.go`、`capabilities.go`、`node_reference.go`、`file.go` | 原 payload canonical／冻结、typed 结果、scalar token 与 no-data Sync grant。 |
| `httprest/file_capability_wire.go`、`file_json.go`、`file_authorization.go`、`file_reference_state.go`、`file_result_budget.go` | 原 DTO、严格字段／预算、实际效果授权、scalar 投影及既有恢复链。 |
| `sqlite/namespace_guards.go`、`namespace_mutations.go`、`durable.go`、`files.go` | rooted command coverage、最终名字效果、同一 pins 清单及后代关联检查。 |
| `sqlite/atomic_open.go`、`publication.go`、`file_access.go`、`node_references.go`、`pending_unlink.go`、`options.go` | reset effect use、原 publication、ordinary／DOC 状态和现有工作上限。 |

wrappers 保留已有生产接口与完整参数透传；没有新 wrapper capability。相关接受／错误／竞争／预算验证由[测试策略](../../../../docs/testing.md)拥有，原有观察、子项选择和 CloseIntent 决定保留各自理由及范围。

## 备选方案

**独立 GuardedNameMutator 与双侧 guards。** 它重复既有 mutation／HTTP／wrapper／receipt 链，共同 ancestry 还需要两套预算和版本一致性规则。单个 rooted union 直接进入原命令，真实缺口只在字段与最终效果检查。

**先 lookup 或保存路径缓存，再无 guards 修改。** 观察与提交间可移动祖先或替换槽位，缓存不能证明当前关联。原 ReferenceNameObserver／IncludeName 的完整捕获和 final guard 校验提供当次证明，无需祖先引用链及额外 close 责任。

**独立 SupersedeFile／OnSupersede／Superseded。** 它们重复 ResetContent、OnReset、Reset outcome 与原子打开 receipt。真正差异是固定效果用途；ResetUse 表达该差异，SMB 边缘投影 Windows outcome。

**伪造 WriteData 或 metadata 读权。** 它给 DELETE-only handle 额外字节能力、误拒绝 deny-write／allow-delete 旧引用，或扩大 ordinary DELETE handle 的 metadata 权限。现有 effect-use 与 snapshot-authorized 观察分别表达实际责任。

**为 ordinary disposition 新建 ancestry 字段、ledger 或每请求 generation。** 原引用与 native 当前唯一 association 已选择对象；另一 ledger 会混淆普通 clear 与 DOC。共用 token 只补 ordinary episode ABA，保留 DOC 增量，不让每次 TRUE／CLEAR 制造无状态转换的 generation。

**host 看到 false 就让 CLEAR 成功。** 它跳过远端当前授权与 CAS，可能忽略稍后出现的 pending。明确空 token 经同一 RPC／action 到 authority 验证，匹配时才授权 no-op。

**把 pins 另建为 subtree registry 或归属 fileDomain。** 前者必须同步两份清单，后者改变原 coordinator 生命周期与物理回收归属。按 volume 分组原 pins 保留唯一引用事实和原责任寿命。

**允许已有后代引用时目录 rename。** 仅 source DeleteName／sharing 的工作量更小，但偏离 MS-FSA 的 descendant-open MUST 拒绝；后代允许 SHARE_DELETE 不构成豁免。原 pins 与最终事务提供标准所需接纳条件。

**只交付 rename，或同时交付锁／通知。** 前者留下 ordinary 删除与 supersede 不完整；后者加入独立 async range 与客户端缓存证明。8.1 交付完整名字能力，锁、WNet、通知和资格继续按独立目的实施。

## 后果

名字效果、普通删除状态和原对象 supersede 都使用既有中立操作、动作历史和清理责任。完整名字证据、metadata、共享与后代引用在最终 authority 排序，旧引用不会转到替代物，Unknown 不能触碰后来占据名字的对象。成功 class 48 对当前路径给出一个完整验证的观察点；截断响应和 compound 关闭仍按真实协议结果处理。

代价是粗粒度 directory revision 可因无关 sibling 变化产生保守冲突，完整双路径可能超过既有 guard／capture／frame 预算。目录后代检查有 MaxRetainedFiles 与 MaxIntegrityRecords 的显式工作边界，深 ancestry 或额度不足明确失败。重观察的旧／新 payload 同时计费，资源不足可以让已知未执行的请求终止；不能削短证据或自动延长重试。

StateScalars 的低权限 token 不改变 SET／CLEAR FULL result 的 metadata 驻留和预算，完整结果限制仍可能拒绝 mutation。更广泛的结果 projection／隐私改造需要独立合同与 receipt／预算同步；本决定没有给其余入口新增 metadata 读权。

本决定部分扩展[有界权威名字观察](../architecture/2026-09-20-bounded-authoritative-name-observations.md)、[权威子项选择](../architecture/2026-09-22-guard-authoritative-child-selection.md)与[持久节点身份和原子文件操作](../architecture/2026-09-20-durable-identity-and-atomic-file-operations.md)，保留它们独有的观察、身份、action、schema 与打开理由。名字事件继续供[通知提案](../../proposed/feature/2026-09-28-smb-change-notify-cache-coherence.md)消费。

范围 LOCK／CANCEL、WNet、Windows redirector 缓存失效与完整 Windows 11 Home／Pro × x64／ARM64 发布资格分别由 [8.2](../../proposed/feature/2026-09-28-smb-range-lock-cancel.md)、[9](../../proposed/feature/2026-09-28-smb-native-wnet-fixture.md)、[8.3](../../proposed/feature/2026-09-28-smb-change-notify-cache-coherence.md)与 [10](../../proposed/feature/2026-09-28-smb-native-qualification.md)承担。AlSi 的旧 CREATE 模式缺口保持可见。协议、direct／HTTP 与 fixture 验证不构成这些原生资格。
