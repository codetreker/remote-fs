# Agent Note: SMB 文件信息

Status: implemented

## 问题

Windows 程序需要通过已打开的文件或目录查询信息，修改属性／时间，截短或扩展普通文件。名字可能改名、删除或被替换，已有句柄仍须指向原对象。有效的 zero-access、WRITE-only 或 WRITE_ATTRIBUTES-only 句柄并不具有公开 metadata 读权；信息查询和条件修改不能暗中扩大该权限。属性／时间组合与 EOF／ARCHIVE 必须各在同一动作中确认，未知结果不能通过创建新动作掩盖。

## 决定

SMB 文件信息在[有界 CREATE/CLOSE](2026-09-28-smb-bounded-create-close.md)的 typed FileId 与[文件内容访问](2026-09-28-smb-file-data-io.md)的准入／恢复链上提供五种文件 QUERY_INFO 和 Basic／EOF SET_INFO。信息均来自原引用，本次查询验证当前生命期，不能用旧路径、打开时保存的属性或仅在退休后仍存在的固定 ID 回答。

该决定服务 [R-FS-6 至 R-FS-9、R-ERR-1 至 R-ERR-4、R-INT-3、R-INT-8、R-INT-11、R-WIN-2、R-WIN-4、R-WIN-5、R-WS-5](../../../../docs/spec/requirements.md)。[volume 信息提案](../../proposed/feature/2026-09-28-smb-file-data-information.md)独立拥有 volume QUERY_INFO、label／creation fact、Space 与完整 presentation；本决定只建立文件信息必需的 identity／allocation geometry 基础。结构见[SMB 端点设计](../../../../docs/design/client/smb-endpoint.md)与[文件契约](../../../../docs/design/server/file-handles.md)，验证归[测试策略](../../../../docs/testing.md)。

### 逐类信息源与权限

| `InfoType=0x01` 类 | 句柄权与本次权威来源 | 完整输出 |
|---|---|---|
| `FileBasicInformation` `0x04` | FILE_READ_ATTRIBUTES；原 File／NodeReference 的同次 Stat | 40 字节，Birth／Access／Mod／Change、Windows 属性及保留位 |
| `FileStandardInformation` `0x05` | 活引用与当前查询授权；scalar Information | 24 字节，EOF／allocation／kind、detached 及 pending；无 hard link 模型的具名 link count 为 1、detached 为 0 |
| `FileInternalInformation` `0x06` | 活引用与当前查询授权；本次 scalar Information 确认原 NodeID | 8 字节，原不可复用对象 ID |
| `FileNetworkOpenInformation` `0x22` | FILE_READ_ATTRIBUTES；同一次 Stat | 56 字节，四个时间、EOF／allocation、Windows 属性 |
| `FileIdInformation` `0x3B` | 活引用与当前查询授权；本次 scalar Information、固定可信 volume identity | 24 字节，稳定 64-bit volume serial 与 128-bit 对象 ID |

Basic／NetworkOpen 不能拼接多次 Stat 的字段。Standard／Internal／Id 不要求 FILE_READ_ATTRIBUTES，使用 `ReferenceInformationAccess.Information`，一次 snapshot 返回 NodeID、Kind、Size、AllocationSize、AllocationKnown、Detached、PendingUnlink；不返回时间、opaque metadata 或名字，不授予 byte/setter 方法。普通文件 Size 非负，目录的 Size 不指定字节长度；Windows 信息依目录 kind 报告 EOF=0。公开 Stat／State 继续遵守 ReadMetadata。FileSession 的 `OpenReferenceInformation` 在完整链检查两种返回引用，当前业务授权仍逐次执行。

NodeID 在同一 volume 内不可复用；128-bit ID 对 uint64 NodeID 做无损单射编码，不增加另一份持久 ID map。64-bit serial 从 canonical durable VolumeID 稳定投影，不同发布 volume 的投影碰撞在发布准入拒绝；SMB 每次打开的 FileId 只标识 capability，与对象 ID 分离。未来 32-bit volume serial 的投影与碰撞规则由 volume 信息独立决定。

AllocationKnown／AllocationSize 先遵守中立字节事实验证，再核对信息类所需 cluster 整数倍；非对齐值不能暗中取整。`VolumeGeometryAccess` 返回可信 `VolumeGeometry{Volume, BytesPerSector, SectorsPerAllocationUnit}`，Volume 必须匹配固定 backend identity。随附 authority 报告经过验证的逻辑 4096×1 allocation geometry，不读 host 物理磁盘。replica 转发远端事实，limited 遵守既有未知 allocation 规则。缺席／非法几何只使依赖它的类失败，不废弃整个 tree。

### 限定 namespace 条件观察

WRITE_ATTRIBUTES-only Basic setter 通过 `MetadataObservations` 在 OpenAt／NodeRefOptions 中封存 `MetadataObservationScope{Namespace, MaxPayloadBytes}`。`OpenMetadataObservation` 验证完整链；`ReferenceMetadataObserver.ObserveMetadata(index)` 在 exact live、具有 WriteMetadata 的引用上，只观察已批准范围，返回 NodeID 与存在／缺席的 OpaquePayload。它不要求或授予 ReadMetadata，也不允许任意 namespace 查询。

范围在打开时深复制、校验数量、namespace 唯一性和 payload 上限，并进入 action digest。SMB 只 enrollment 八字节 smb.windows；确认缺席与存在空值保持区别，未知版本或错误 payload 在修改前失败。业务授权分别核对 enrollment、观察及实际属性／metadata 修改；打开批准不构成终身权限。HTTP 和 wrappers 保留真实能力、范围、原值与错误，不能用公开 Stat 兜底。

### Basic 与 EOF 的原子动作

Basic SET_INFO 严格解析 40 字节，要求 FILE_WRITE_ATTRIBUTES，经 `MutateAttributes` 一次提交允许的 AttrChange 与 smb.windows 更新。File、目录和 metadata-only NodeReference 共享这条属性路径，不获得字节方法。允许 READONLY／HIDDEN／SYSTEM／ARCHIVE，以及 Birth／Access／Mod 时间；NORMAL、DIRECTORY 从报告事实派生，不持久化。

零时间和零属性保持原值，NORMAL 清除可设置属性集合；实际 volume 根的 HIDDEN／SYSTEM 保留，按有效 mask 处理其它可设置位，根目录与 subtree share 根不混淆。ChangeTime 由 authority 管理，非零输入不能直接设置。-1／-2 的逐句柄时间控制不在该能力内，明确失败；其它非零时间在效果前完成 FILETIME 范围和溢出验证。缺席历史 Birth／Change 查询稳定报告零，不写回新事实；修改时间不能由本机时钟补造。

EOF SET_INFO 严格解析 8 字节非负长度，只接受普通 File 的 FILE_WRITE_DATA，APPEND-only／目录／metadata-only 不满足。`MutateTruncate` 将长度与已 enrollment 的固定 ARCHIVE 效果同次提交，最终同时核对 READONLY namespace token、原对象 scope、用途／访问保护及额度。truncate 后补 setter 会形成部分效果，不能使用。

Basic 条件动作固定 ExpectedMetadata 与更新 payload 的 Version；只有原动作绑定的 NotExecuted 条件冲突才可同时刷新两者并创建有界新动作。EOF 使用 sealed ContentEffects，客户端不提交替代 payload；authority 验证 descriptor 并计算 ARCHIVE，保留其它位和 namespace。授权失败、quota、READONLY 翻转和条件冲突没有部分内容／属性／时间效果。

### 准入、预算与恢复

QUERY_INFO／SET_INFO 的原签名字节不改写，related FileId 只在 frame-local execution context 中解析。固定体、当前 compound command extent 与 payload 长度做无溢出检查；每类显式登记 rights、来源与完整输出长度。支持的五个文件 QUERY_INFO 类按协议忽略 Flags、AdditionalInformation、InputBufferLength／Offset，不消费其数据，即使字段不能描述当前 command 中的 buffer。SET 的非 security AdditionalInformation 必须为零；未交付类与畸形输入在 backing 前拒绝。固定输出 buffer 不足不发送半个结构。

逐引用 FIFO 同时覆盖 File／NodeReference。入队前与取得 turn 后分别核对身份、实际访问权及当前授权；CLOSE 先 fence 新请求、移除 queued、排空 running，再恢复已接纳条件动作并执行精确关闭。恢复错误不能阻断取得 no-future-publication 证明的关闭路径。不同 FileId 并行，同一对象最终顺序由 authority 负责。

条件 mutation owner 保存原 Action、kind、target、Attr、Metadata、ExpectedMetadata、Data 及 ContentEffects 的深复制，所有保留字节和 diagnostic 在效果前计费。内部模块为 mutation_owner.go，公开 MaxWriteOwners／MaxRetainedWriteBytes、WriteOwners／WriteFailures 与 AcknowledgeWriteFailures 共用原有条件修改统计和宿主责任转移，不另建一套生命周期。结果校验按 mutation kind 区分，目录与 metadata-only 只接受 Attributes。Completed 只允许同 ID／reference／负载 typed replay 取回完整 Attr／barrier；Unknown／Retired／无 Operation 的 NotExecuted 不能 remint。保留 payload 仅在 exact 引用或完整父 Released 及全链 settlement 证明后释放，终态 failurefacts 继续由宿主明确接管报告责任。HTTP 的 explicitmutation 恢复沿原 filecapability、nestedAction 与 immutableintent 隔离，不阻塞 sibling 查询、renew、状态和 cleanup。

### 范围切分

| 延后能力 | 延后代价与保持的结构 | 未交付请求的结果 |
|---|---|---|
| [7.4c volume 信息](../../proposed/feature/2026-09-28-smb-file-data-information.md) | 添加五个 volume 类、label／creation 事实与当前 Space 来源；不重写已建立 identity／geometry 基础 | volume QUERY_INFO 明确 unsupported |
| [7.5 目录枚举](../../proposed/feature/2026-09-28-smb-directory-enumeration.md) | 添加有界冻结快照与 cursor；目录引用保留稳定 identity | QUERY_DIRECTORY 明确 unsupported |
| [7.6 关闭删除](../../proposed/feature/2026-09-28-smb-close-delete-obligation.md)与[8.1 名字修改](../../proposed/feature/2026-09-28-smb-guarded-name-mutation.md) | 增加持久义务与 ancestryguards；旧 CREATE 名字不当作当前事实 | disposition、rename／link、当前名字类明确 unsupported |
| [8.2 范围锁](../../proposed/feature/2026-09-28-smb-range-lock-cancel.md) | 增加跨入口范围保护；EOF／Basic 继续走受控 reference 和 authority 最终检查 | LOCK／asyncCANCEL 按未交付协议结果处理 |
| [8.3 通知与缓存](../../proposed/feature/2026-09-28-smb-change-notify-cache-coherence.md) | 增加失效协议与原生证明；不能将 Go／wire 查询成功当作 Windows 私有缓存资格 | CHANGE_NOTIFY 明确 unsupported |

allocation setter、EA／streams／compression／reparse、position／mode setter、security／quota 信息和 FScontrol 不在类表。未支持的信息类返回 NOT_SUPPORTED；支持类的畸形 length／flag 按规范返回 INVALID_PARAMETER 或指定长度错误，不用占位字段凑成功。

## 备选方案

**让所有句柄取得 ReadMetadata，再使用 Stat／State。** Standard、Internal 与 Id 可复用已有结果，但有效 WRITE-only／zero-access 句柄会得到时间和任意 metadata 读取权。scalarInformation 只返回所需事实，保持公开权限独立。

**WRITE_ATTRIBUTES-only 先 Stat，再修改 namespace。** 它同样扩大 DesiredAccess；用限定 enrollment 观察只取得批准 namespace 的条件证据，仍由 WriteMetadata 与当前授权限制。

**Stat 与 State 分别查询并拼接 Standard。** 两次捕获之间可发生截短、unlink 或 pending 变化，成功结构可能从未成立。scalarInformation 在一个权威 snapshot 中返回全部字段。

**在本段交付完整 volume 信息。** label、creationfact、Space、容量三值与各 classflag 有独立来源和失败证明，会使一个文件信息目的包含另一套业务。最小 geometry／identity 基础使五个文件查询完整，volume 类单独完成。

**EOF 调用 Truncate 后再补 ARCHIVE。** 两个动作之间可观察到部分状态，回复丢失也无法恢复一个完整结果。现有 conditionalmutation 扩展 sealed 效果到 Truncate，最终 CAS 一次完成。

**为 SET_INFO 再建一份恢复／关闭 owner。** 属性和 EOF 的未知动作会与 WRITE 争用同一引用，增加两套 fence、预算和责任终结顺序。通用条件 owner 保存完整不可变 command，以 kind 区分授权和 result 规则，并复用原关闭证明。

**持久化另一份128-bit 身份 map。** 现有不可复用 uint64NodeID 可以无损编码到128bit，同 volume 内没有 hash 碰撞；新增 map 会引入 schema、迁移与回收职责。保持 volume serial 与对象 ID 的组合，独立检查 volume 投影碰撞。

### 测试资源归属

HTTP 文件信息、metadata观察与恢复矩阵共用各自顶层的真实 authority准备，每项新建对象、HTTPpair、session、journal和fault状态，不继承别项的action或dirty内容。纯错误传递fixture只返回原注入错误，storage与十二个lockservice方法都没有健康fallback；健康publication／lock用例仍需真实native后端。fixture准备与测试判据的取舍由[测试工作量决定](../testing/2026-09-09-scale-test-work-to-its-assertions.md)记录。CI watchdog、strict无skip检查、函数coverage和protectedcheck均保持原标准。

command-only SMB share矩阵没有后台续期任务，每次新打开之前通过真实FileSession.Renew与authority.acceptStatus刷新admission epoch。lease和actionhistory分别保留原期限；用长lease不能使已经退役的action epoch重新有效。短 history 回归验证过期 epoch 拒绝与真实续期后的 CREATE，share 矩阵独立验证原 sharing 约束。

## 后果

五个文件查询覆盖所需权限与真实事实；scalar 和限定 namespace 观察增加中立契约，却保持公开 metadata 读权与字节方法分离。HTTP、native 与 wrappers 必须检查整条能力链并验证 exact 身份，不能由可选方法存在直接宣布支持。

逐引用 FIFO 限制单句柄吞吐；Unknown 条件动作继续占有限 owner／bytes／diagnostic，容量满时拒绝新工作并保留关闭与状态能力。Basic 的条件重试同时保存原属性语义和更新 Version，不通过重试改变已接受意图。

文件 allocation 依赖可信 geometry，第三方缺失事实时相应类明确失败，其它类和文件操作按自身能力继续。文件信息没有新增磁盘 schema；serial 投影与 NodeID 编码的稳定性使重连、改名、unlink 和同名 replacement 保留原对象区别。

package-local 正常、错误、权限、资源与并发用例验证类表、单 snapshot、条件提交、typedreplay 及关闭结算。Go／wire 与 WindowsAPIunit 测试不等于 Windows11redirector／WNet、通知或私有缓存一致性资格；这些由各自独立交付验证。
