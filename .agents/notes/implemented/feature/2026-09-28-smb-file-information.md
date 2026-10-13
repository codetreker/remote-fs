# Agent Note: SMB 文件信息

Status: implemented

## 问题

Windows 程序需要通过已打开的文件或目录查询信息，修改属性／时间，截短或扩展普通文件。名字可能改名、删除或被替换，已有句柄仍须指向原对象。有效的 zero-access、WRITE-only 或 WRITE_ATTRIBUTES-only 句柄并不具有公开 metadata 读权；信息查询和条件修改不能暗中扩大该权限。属性／时间组合与 EOF／ARCHIVE 必须各在同一动作中确认，未知结果不能通过创建新动作掩盖。

## 决定

SMB 文件信息在[有界 CREATE/CLOSE](2026-09-28-smb-bounded-create-close.md)的 typed FileId 与[文件内容访问](2026-09-28-smb-file-data-io.md)的准入／恢复链上提供五种文件 QUERY_INFO 和 Basic／EOF SET_INFO。信息均来自原引用，本次查询验证当前生命期，不能用旧路径、打开时保存的属性或仅在退休后仍存在的固定 ID 回答。

该决定服务 [R-FS-6 至 R-FS-9、R-ERR-1 至 R-ERR-4、R-INT-3、R-INT-8、R-INT-11、R-WIN-2、R-WIN-4、R-WIN-5、R-WS-5](../../../../docs/spec/requirements.md)。[volume 信息](2026-09-28-smb-volume-information.md)独立拥有 volume QUERY_INFO、label／creation fact、当前 Space 与逻辑容量投影；本决定只建立文件信息必需的 identity／allocation 单位事实。[目录枚举](2026-09-28-smb-directory-enumeration.md)独立拥有完整捕获、分页和目录 cursor。结构见[SMB 端点设计](../../../../docs/design/client/smb-endpoint.md)与[文件契约](../../../../docs/design/server/file-handles.md)，验证归[测试策略](../../../../docs/testing.md)。

### 逐类信息源与权限

| `InfoType=0x01` 类 | 句柄权与本次权威来源 | 完整输出 |
|---|---|---|
| `FileBasicInformation` `0x04` | FILE_READ_ATTRIBUTES；原 File／NodeReference 的同次 Stat | 40 字节，Birth／Access／Mod／Change、Windows 属性及保留位 |
| `FileStandardInformation` `0x05` | 活引用与当前查询授权；StateScalars | 24 字节，EOF／allocation／kind、detached 及 pending；无 hard link 模型的具名 link count 为 1、detached 为 0 |
| `FileInternalInformation` `0x06` | 活引用与当前查询授权；本次 StateScalars 确认原 NodeID | 8 字节，原不可复用对象 ID |
| `FileNetworkOpenInformation` `0x22` | FILE_READ_ATTRIBUTES；同一次 Stat | 56 字节，四个时间、EOF／allocation、Windows 属性 |
| `FileIdInformation` `0x3B` | 活引用与当前查询授权；本次 StateScalars、固定可信 volume identity | 24 字节，稳定 64-bit volume serial 与 128-bit 对象 ID |

Basic／NetworkOpen 不能拼接多次 Stat 的字段。Standard／Internal／Id 不要求 FILE_READ_ATTRIBUTES，复用 `ReferenceStateAccess.State(ctx, StateScalars)` 返回同一 snapshot 的 Attr.ID／Kind／Size／AllocationSize／AllocationKnown 和 Detached／PendingUnlink；四类时间、opaque metadata 与 link target 必须缺席；已有 bounded PendingGeneration 随 aggregate pending 返回，由[名字修改](2026-09-28-smb-guarded-name-mutation.md)用于 ordinary CLEAR，也不先载入完整 State 再裁字段。`StateFull` 保留完整结果并要求 ReadMetadata，FUSE 明确使用 Full。投影零值非法，当前业务授权携带实际投影，引用生命期和 NodeID 仍逐次验证。普通文件 Size 非负，目录 Size 未指定；Windows 按目录 kind 报告 EOF=0。

NodeID 在同一 volume 内不可复用；128-bit ID 对 uint64 NodeID 做无损单射编码，不增加另一份持久 ID map。64-bit serial 从 canonical durable VolumeID 稳定投影，不同发布 volume 的投影碰撞在发布准入拒绝；SMB 每次打开的 FileId 只标识 capability，与对象 ID 分离。[volume 信息](2026-09-28-smb-volume-information.md)使用该 serial 的低 32 位；该字段不承担唯一性保证，原 64-bit reservation 保留。

AllocationKnown／AllocationSize 保留权威分配字节，单位是既有 AllocationReporting 的独立可选结果。Standard／NetworkOpen 需要会话内不变的可信非零单位验证整数倍，不能取整或从 EOF 推算。HTTP 在授权 session enrollment 中保留单位或原失败，不增加单位查询RPC；单位不可用不降低字节 capability 或使整个tree失败。需要该事实的文件类明确报告相应错误，当前访问授权和健康仍由本次 StateScalars／Stat保证。单位不依赖 Space allowance 或 host 物理 sector；SQLite 文件消费者复用原 exclusive authority身份路径。

### 既有状态与 metadata 能力的投影

两种状态投影复用原 State 路径、引用 checker、HTTP `file.state` envelope 与 wrappers。SQLite 在同一 transaction 中捕获节点和 pending deletion；Scalars 不载入完整 Attr／metadata／link target，Full 保留原公开事实。私有 pending capture 统一核对 explicit pending、generation 与 accepted durable intent；projection-aware result validation 拒绝低权限结果携带 protected 字段，limited 对两种投影都保持实际 allocation 规则。

条件观察复用 `ReferenceMetadataAccess.ObserveMetadata(ctx, MetadataObservationRequest)`，没有平行观察接口或 namespace enrollment。请求 purpose 必须明确为 ContentEffect 或 Namespace。ContentEffect 以 EffectIndex 选择原 writable File 已封存的固定转换，要求数据写权，禁止同时指定 namespace／bound；Namespace 每次指定实际 namespace 与 MaxPayloadBytes，要求 WriteMetadata，禁止 EffectIndex，不授予 ReadMetadata。SMB Basic 使用 Namespace=smb.windows、bound=8，当前策略每次核对这个确切请求；WRITE／EOF 使用 ContentEffect purpose 核对原 descriptor。观察授权不代替普通 metadata 更新授权。

返回 `MetadataObservation{NodeID,Value}` 区分确认缺席与存在空值，返回数据独立拥有，错误为零。私有 namespace capture 复用有界 canonical envelope 读取，各 purpose 在捕获前后分别核对权限、生命期、descriptor 或 per-call bound。已有 CheckMetadataAccess 与 OpenMetadataAccess 验证完整修改后的契约；固定 ContentMetadataEffects 继续封存和参与 mutation digest，其原子转换不被普通 metadata setter 代替。

### Basic 与 EOF 的原子动作

Basic SET_INFO 严格解析 40 字节，要求 FILE_WRITE_ATTRIBUTES，经 `MutateAttributes` 一次提交允许的 AttrChange 与 smb.windows 更新。File、目录和 metadata-only NodeReference 共享这条属性路径，不获得字节方法。允许 READONLY／HIDDEN／SYSTEM／ARCHIVE，以及 Birth／Access／Mod 时间；NORMAL、DIRECTORY 从报告事实派生，不持久化。

零时间和零属性保持原值，NORMAL 清除可设置属性集合；实际 volume 根的 HIDDEN／SYSTEM 保留，按有效 mask 处理其它可设置位，根目录与 subtree share 根不混淆。ChangeTime 由 authority 管理，非零输入不能直接设置。-1／-2 的逐句柄时间控制不在该能力内，明确失败；其它非零时间在效果前完成 FILETIME 范围和溢出验证。缺席历史 Birth／Change 查询稳定报告零，不写回新事实；修改时间不能由本机时钟补造。

EOF SET_INFO 严格解析 8 字节非负长度，只接受普通 File 的 FILE_WRITE_DATA，APPEND-only／目录／metadata-only 不满足。`MutateTruncate` 将长度与已 enrollment 的固定 ARCHIVE 效果同次提交，最终同时核对 READONLY namespace token、原对象 scope、用途／访问保护及额度。truncate 后补 setter 会形成部分效果，不能使用。

Basic 条件动作保存原时间与请求属性，重读只提供 smb.windows token和实际volume根的HIDDEN／SYSTEM保留位；没有任意namespace／mask／整份command刷新框架。只有原动作绑定的NotExecuted条件冲突才可刷新同一namespace的condition与Version并创建有界新动作。EOF 使用 sealed ContentEffects，客户端不提交替代 payload；authority 验证 descriptor 并计算 ARCHIVE，保留其它位和 namespace。授权失败、quota、READONLY 翻转和条件冲突没有部分内容／属性／时间效果。

### 准入、预算与恢复

QUERY_INFO／SET_INFO 的原签名字节不改写，related FileId 只在 frame-local execution context 中解析。固定体、当前 compound command extent 与 payload 长度做无溢出检查；每类显式登记 rights、来源与完整输出长度。支持的五个文件 QUERY_INFO 类按协议忽略 Flags、AdditionalInformation、InputBufferLength／Offset，不消费其数据，即使字段不能描述当前 command 中的 buffer。SET 的非 security AdditionalInformation 必须为零；未交付类与畸形输入在 backing 前拒绝。固定输出 buffer 不足不发送半个结构。

逐引用 FIFO 同时覆盖 File／NodeReference。入队前与取得 turn 后分别核对身份、实际访问权及当前授权；CLOSE 先 fence 新请求、移除 queued、排空 running，再恢复已接纳条件动作并执行精确关闭。恢复错误不能阻断取得 no-future-publication 证明的关闭路径。不同 FileId 并行，同一对象最终顺序由 authority 负责。

条件 mutation owner 保存原 Action、kind、target、Attr、Metadata、ExpectedMetadata、Data 及 ContentEffects 的深复制，所有保留字节和 diagnostic 在效果前计费；SET和恢复的Attr预算由原owner统一执行。内部模块为 mutation_owner.go，公开 MaxWriteOwners／MaxRetainedWriteBytes、WriteOwners／WriteFailures 与 AcknowledgeWriteFailures 共用原有条件修改统计和宿主责任转移，不另建一套生命周期。结果校验按 mutation kind 区分，目录与 metadata-only 只接受 Attributes。Completed 只允许同 ID／reference／负载 typed replay 取回完整 Attr／barrier；Unknown／Retired／无 Operation 的 NotExecuted 不能 remint。保留 payload 仅在 exact 引用或完整父 Released 及全链 settlement 证明后释放，终态 failurefacts 继续由宿主明确接管报告责任。HTTP 的 explicitmutation 恢复沿原 filecapability、nestedAction 与 immutableintent 隔离，不阻塞 sibling 查询、renew、状态和 cleanup。

### 范围切分

| 延后能力 | 延后代价与保持的结构 | 未交付请求的结果 |
|---|---|---|
| [7.6 关闭删除](../../implemented/feature/2026-09-28-smb-close-delete-obligation.md)与[8.1 名字修改](2026-09-28-smb-guarded-name-mutation.md) | 增加持久义务与 ancestryguards；旧 CREATE 名字不当作当前事实 | ordinary disposition、rename 与 normalized current name 由各自独立决定拥有；link 保持 unsupported |
| [8.2 范围锁](../../proposed/feature/2026-09-28-smb-range-lock-cancel.md) | 增加跨入口范围保护；EOF／Basic 继续走受控 reference 和 authority 最终检查 | LOCK／asyncCANCEL 按未交付协议结果处理 |
| [8.3 通知与缓存](../../proposed/feature/2026-09-28-smb-change-notify-cache-coherence.md) | 增加失效协议与原生证明；不能将 Go／wire 查询成功当作 Windows 私有缓存资格 | CHANGE_NOTIFY 明确 unsupported |

allocation setter、EA／streams／compression／reparse、position／mode setter、security／quota 信息和 FScontrol 不在类表。未支持的信息类返回 NOT_SUPPORTED；支持类的畸形 length／flag 按规范返回 INVALID_PARAMETER 或指定长度错误，不用占位字段凑成功。

## 备选方案

**让所有句柄取得 ReadMetadata，再使用 Stat／State。** Standard、Internal 与 Id 可复用已有结果，但有效 WRITE-only／zero-access 句柄会得到时间和任意 metadata 读取权。StateScalars 只返回所需事实，保持公开权限独立。

**WRITE_ATTRIBUTES-only 先 Stat，再修改 namespace。** 它扩大 DesiredAccess。既有 metadata CAS 能力增加明确 Namespace purpose 的有界条件观察，仍按 WriteMetadata 与每次实际 namespace／bound 授权；无需另建打开 scope registry。

**Stat 与 State 分别查询并拼接 Standard。** 两次捕获之间可发生截短、unlink 或 pending 变化，成功结构可能从未成立。StateScalars 在一个权威 snapshot 中返回全部字段。

**在本段交付完整 volume 信息。** label、creationfact、Space、容量三值与各 classflag 有独立来源和失败证明，会使一个文件信息目的包含另一套业务。既有 allocation／identity 契约使五个文件查询完整，volume 类单独完成。

**EOF 调用 Truncate 后再补 ARCHIVE。** 两个动作之间可观察到部分状态，回复丢失也无法恢复一个完整结果。现有 conditionalmutation 扩展 sealed 效果到 Truncate，最终 CAS 一次完成。

**仅分别测试 SMB 与 FUSE API。** 各自成功不能证明两个同时打开的入口共享新时间、EOF 与对象身份，也不能检出一方误写另一平台 metadata。有限 direct／HTTP 联合矩阵使用原 fd 和 FileId 验证交接，不将 FUSE 改用新 SMB 权限接口。

**把 FUSE 挂载测试放进默认 SMB package suite。** 会使 Windows runner 与无 mountpoint 的非挂载 jobs 承担 Linux 依赖。独立 test-only package 和测试标签将实际挂载放在既有 mounted gate，并保留严格 verdict。

**从 Space 取得文件分配单位。** 未配置容量 allowance 的 native volume 可以对 Space 返回 ENOSYS，却仍持有已知文件分配事实。单位 getter 属于已报告 allocation 的既有契约，使文件类的整除校验不依赖独立容量配置。

**增加独立 Volume geometry 接口。** Standard／NetworkOpen 只消费分配单位，不消费 sector tuple；新增接口会让 native、HTTP 与 wrappers 保存额外 checker／DTO／capability。AllocationReporting 提供其实际需要的可信单位，未来容量类再定义所需完整事实。

**为低权限信息和两类条件观察增加平行接口／routes。** 每个新增接口都要求 native、HTTP、wrappers 和引用保存另一套 checker／capability／request 分派，即使内部 SQL 共用，机制仍重复。原 State 通过严格 Full／Scalars 投影表达不同读取权限，原 ReferenceMetadataAccess 通过互斥 purpose 表达不同观察权限；同一机制保留显式权限和预算，不增加重叠 capability bit 或 namespace enrollment。

**为两个 namespace 观察或两个 pending 查询复制 SQL。** 同一事实会产生不同的长度、类型与 corruption 判断。既有公开能力内部共享私有 capture，各 purpose／projection 分别执行权限、结果与预算验证；共享实现不合并授权。

**为 SET_INFO 再建一份恢复／关闭 owner。** 属性和 EOF 的未知动作会与 WRITE 争用同一引用，增加两套 fence、预算和责任终结顺序。通用条件 owner 保存完整不可变 command，以 kind 区分授权和 result 规则，并复用原关闭证明。

**持久化另一份128-bit 身份 map。** 现有不可复用 uint64NodeID 可以无损编码到128bit，同 volume 内没有 hash 碰撞；新增 map 会引入 schema、迁移与回收职责。保持 volume serial 与对象 ID 的组合，独立检查 volume 投影碰撞。

### 测试资源归属

HTTP 文件信息、metadata观察与恢复矩阵共用各自顶层的真实 authority准备，每项新建对象、HTTPpair、session、journal和fault状态，不继承别项的action或dirty内容。纯错误传递fixture只返回原注入错误，storage与十二个lockservice方法都没有健康fallback；健康publication／lock用例仍需真实native后端。fixture准备与测试判据的取舍由[测试工作量决定](../testing/2026-09-09-scale-test-work-to-its-assertions.md)记录。CI watchdog、strict无skip检查、函数coverage和protectedcheck均保持原标准。

command-only SMB share矩阵没有后台续期任务，每次新打开之前通过真实FileSession.Renew与authority.acceptStatus刷新admission epoch。lease和actionhistory分别保留原期限；中立semanticAction的epoch由nativejournal验证，HTTPelapsed window不替代它；用长lease不能使已经退役的action epoch重新有效。短 history 回归验证过期 epoch 拒绝与真实续期后的 CREATE，share 矩阵独立验证原 sharing 约束。

同 volume 的联合验收位于 test-only `packages/smb/internal/mountacceptance`，使用真实签名 SMB TCP 与实际 FUSE 挂载，在 direct／HTTP 下分别建立独立 session，并保留原 Linux fd 与 SMB FileId。它核对共享时间、长度、内容、补零与身份，以及 Windows 属性／ARCHIVE 和 POSIX mode 的独立解释。`linux && rfs_mountacceptance` 只控制测试收集，CI mounted job 通过 strict runner 执行整包并检查挂载清理；原非挂载 SMB、Windows 和 coverage 层不借此新增平台依赖。

## 后果

五个文件查询覆盖所需权限与真实事实；State 投影与 metadata 观察 purpose 保持公开 metadata 读权和字节方法分离，已有机制承担它们的身份、checker、预算及传输。HTTP、native 与 wrappers 必须检查整条能力链并验证 exact 身份，不能由可选方法存在直接宣布支持。

逐引用 FIFO 限制单句柄吞吐；Unknown 条件动作继续占有限 owner／bytes／diagnostic，容量满时拒绝新工作并保留关闭与状态能力。Basic 的条件重试同时保存原属性语义和更新 Version，不通过重试改变已接受意图。

文件 allocation 需要的单位来自 AllocationReporting 可选不变结果，第三方缺失事实时相应类明确失败，其它类和文件操作按自身能力继续。文件信息没有新增磁盘 schema；serial 投影与 NodeID 编码的稳定性使重连、改名、unlink 和同名 replacement 保留原对象区别。

package-local 正常、错误、权限、资源与并发用例验证类表、单 snapshot、条件提交、typedreplay 及关闭结算。Go／wire 与 WindowsAPIunit 测试不等于 Windows11redirector／WNet、通知或私有缓存一致性资格；这些由各自独立交付验证。
