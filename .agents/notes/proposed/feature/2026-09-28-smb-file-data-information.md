# Agent Note: SMB 文件与 volume 信息投影

Status: proposed

## 问题

Windows 程序需要通过已经打开的对象查询或修改文件信息，并取得真实的 volume 身份、时间与容量。打开后的路径可能改名、删除或被替换；按旧路径重查会把属性操作转给新对象。未知时间、分配量及 geometry 不能由当前时间、固定 cluster 或缓存数字猜测。EOF 与 ARCHIVE 必须同动作改变，属性／时间组合不能拆成部分成功。

本提案保留[Windows 总提案](2026-09-16-windows-network-drive-support.md)中 7.4 的文件／volume 信息决定，按两个独立目的交付：**7.4b 文件信息**，**7.4c volume 信息**。已落地的[7.4a 文件内容访问](../../implemented/feature/2026-09-28-smb-file-data-io.md)独立拥有 READ／WRITE／FLUSH、byte-only 派生效果、逐句柄准入与 action owner。这里保留信息类、EOF、geometry 的详细设计；各段开工前再将对应目的定稿成自己的独立提案。

需求依据为 [R-FS-6 至 R-FS-9、R-WIN-2、R-WIN-4、R-WIN-5、R-WS-5](../../../../docs/spec/requirements.md)。它们只承担相应信息能力，不宣布完整 Windows drive 已完成。

## 提案

### 交付与依赖

| 目的 | 范围 | 依赖与形成的保证 |
|---|---|---|
| 7.4b 文件信息 | 文件 QUERY_INFO 的五种类；Basic 属性／时间与 EOF SET_INFO。 | 使用 7.3 typed FileId 与 7.4a gate／action owner；普通 metadata 修改保持独立权限；EOF 将同一派生效果扩展到 MutateTruncate。 |
| 7.4c volume 信息 | 可信 identity／serial、label／creation fact、geometry 与五种 volume QUERY_INFO。 | 可信 VolumePresentation 贯穿 HTTP／wrappers；容量无法表达时只失败对应查询，不封整个 tree。 |

文件／volume 信息以 `InfoType/class → rights → 完整输出预算 → 权威来源 → encoder` 登记显式 allowlist。FileId、session incarnation、tree、reference kind、对象 ID 与当前授权每次核对；已有引用永远不转成旧名字查找。不能从 partial Attr 或错误拼出成功信息。未知的类、flags 与修改组合在 backing 前明确拒绝。

### 目录与模块形状

以下是计划位置；`windows_metadata.go` 已由 CREATE/CLOSE 建立，信息阶段只复用与扩展相关转换。

| 路径 | 职责 |
|---|---|
| `packages/smb/commands_file_info.go` | 7.4b 文件信息类、rights、引用事实及条件属性／EOF 动作。 |
| `packages/smb/commands_volume_info.go`、`volume_geometry.go` | 7.4c volume 类、identity／Space／geometry 的数值一致投影。 |
| `packages/smb/internal/wire/file_info.go` | QUERY_INFO／SET_INFO extent、长度、class codec 与完整输出。 |
| `packages/smb/windows_metadata.go` | 属性、FILETIME 与已存在 namespace 格式，不保存远端状态。 |
| `packages/storage/capabilities.go` 与 validation | 中立 VolumePresentation；EOF 复用 7.4a sealed ContentMetadataEffects，不另建内容路径。 |
| HTTP capability／wire 与 storage wrappers | 可信 presentation 原样转送，缺席／错误不补 geometry。 |
| native／objectstore | 真实 presentation facts 与 identity projection；既有条件属性／长度 publication 的完整返回。 |
| 相邻 package-local tests 与实施时 docs | 类表 vectors、事实不自洽、权限、预算、条件竞争与同动作恢复。 |

### 引用、属性与 EOF

7.4b 使用 7.4a 逐句柄 gate 和 immutable owner，确保查询／修改与同句柄 CLOSE 顺序一致。普通文件基本信息依实际 metadata access 调 Stat；没有 byte-read 的 metadata-only FileId 可使用 NodeReference，但不会因此获得字节方法。当前名字类不能回显 CREATE 时保存的旧名字，归后续受 guard 名字工作。

EOF 需要 FILE_WRITE_DATA，通过同一个 `ConditionalFileMutation.MutateFile(MutateTruncate)` 提交长度与已 enrollment 的固定 ARCHIVE 效果；READONLY 的 namespace 观察、显式 ExpectedMetadata 条件、当前附加授权、action digest、bound NotExecuted 与恢复规则归[7.4a](../../implemented/feature/2026-09-28-smb-file-data-io.md)。把派生效果适用 kind 扩展到 Truncate 是 7.4b 的必要接入，不能用 Truncate 后 SetMetadata 两步模拟。

显式 Basic 属性／时间修改需要 FILE_WRITE_ATTRIBUTES 和相应 MetadataAccess，由 `MutateAttributes` 一次提交任意允许的 metadata／Attr 与条件。目录和 metadata-only FileId 使用现有 `NodeReference.(ConditionalFileMutation)` 的 MutateAttributes；WriteAt／Truncate 对这类引用仍是 EBADF。每次 SET_INFO 核对当前业务授权、exact scope 与条件；不能调用两个 setter 拼出部分效果。

### 信息类、容量与时间

信息类使用显式 allowlist，按 `InfoType／class → 所需句柄 access → 固定／变长长度 → authority 来源 → encoder` 登记。以下数值取自 [MS-SMB2](https://winprotocoldocs-bhdugrdyduf5h2e4.b02.azurefd.net/MS-SMB2/%5bMS-SMB2%5d.pdf) QUERY_INFO／SET_INFO 和 [MS-FSCC](https://winprotocoldocs-bhdugrdyduf5h2e4.b02.azurefd.net/MS-FSCC/%5bMS-FSCC%5d.pdf) 2.4／2.5；固定长度按各结构定义做 exact check，输出短于固定部分为 `STATUS_INFO_LENGTH_MISMATCH`。不支持的类在 backing 调用前明确失败，不以零字段凑成功。

| QUERY_INFO `InfoType=0x01` 类 | access 与权威来源 | 输出规则 |
|---|---|---|
| `FileBasicInformation` `0x04` | `FILE_READ_ATTRIBUTES`；当前引用 Attr 的 Birth／Access／Mod／ChangeTime、Windows metadata 四位及 kind | 固定 40 字节：四个 FILETIME、属性、保留位；缺席历史 Birth／Change 报稳定零，不查询旧路径。 |
| `FileStandardInformation` `0x05` | 活引用及当前查询授权；Attr.Size／AllocationSize，`ReferenceState` 的 Detached／PendingUnlink，kind | 固定 24 字节：link count 在无 hard link 模型下为具名 1／detached 0，另含 DeletePending、Directory、EOF、allocation。 |
| `FileInternalInformation` `0x06` | 活引用及当前查询授权；不可复用 NodeID | 固定 8 字节 ID，与 7.3 的稳定对象 ID 投影一致。 |
| `FileNetworkOpenInformation` `0x22` | `FILE_READ_ATTRIBUTES`；同一 Attr 的时间、EOF、allocation、属性 | 固定 56 字节；未知或不可表达字段使整类失败。 |
| `FileIdInformation` `0x3B` | 活引用及当前查询授权；可信 volume identity 和 NodeID | 固定 24 字节：同一 volume 64-bit serial 与对象 128-bit ID，跨重连稳定；不把 SMB FileId 当对象 ID。 |

| QUERY_INFO `InfoType=0x02` 类 | access 与权威来源 | 输出规则 |
|---|---|---|
| `FileFsVolumeInformation` `0x01` | 活 FileId 与当前 volume 查询授权；可信 volume ID、稳定 label／creation fact | 固定 18 字节加 UTF-16 label：32-bit serial、creation time；creation fact 缺失则整类失败，不用当前时间或零猜测。 |
| `FileFsSizeInformation` `0x03` | 同上；权威 `Space.Total`／`Space.Avail` 与 volume geometry | 固定 24 字节：`Used <= Total` 时分别编码实测总量／调用方可用量；此类没有 Used 字段，不把 `Total-Avail` 当作实测 Used。超额时明确失败。 |
| `FileFsDeviceInformation` `0x04` | 同上；endpoint 的磁盘／远端设备事实 | 固定 8 字节 device type／characteristics，只设置真实能力位。 |
| `FileFsAttributeInformation` `0x05` | 同上；当前实际支持的大小写、名字长度与文件系统能力 | 固定 12 字节加 UTF-16 filesystem name；仅设置已实现 flag。 |
| `FileFsFullSizeInformation` `0x07` | 同上；权威 `Space.Total`、`Space.Used`、`Space.Avail` 与 geometry | 固定 32 字节：仅当 `Used <= Total` 时，actual available=`Total-Used`，caller available=`Avail`；两者分别报告且精确可编码。超额时失败。 |

| SET_INFO `InfoType=0x01` 类 | access、输入和动作 |
|---|---|
| `FileBasicInformation` `0x04` | `FILE_WRITE_ATTRIBUTES`；严格 40 字节、可设置四位和三个非零时间。零保持原值；`-1`／`-2` 的逐句柄时间控制语义在本段不提供，明确 unsupported。经 `MutateAttributes` 一次写入 metadata／时间；ChangeTime 输入不可设置。 |
| `FileEndOfFileInformation` `0x14` | 普通文件 `FILE_WRITE_DATA`；严格 8 字节非负 EOF，以 `MutateTruncate` 和同动作 ARCHIVE 更新。目录／metadata-only 句柄失败。 |

`InfoType=0x03/0x04` 的 security／quota、文件 allocation `0x13`、delete disposition `0x0D`、rename／link、EA、streams、compression、reparse、position／mode setter 及 FS control／object ID setter 均不在本段；已定义但未实现者返回 `STATUS_NOT_SUPPORTED`，协议不认识的类返回 `STATUS_INVALID_INFO_CLASS`，畸形长度／flag 返回 `STATUS_INVALID_PARAMETER` 或指定长度错误。按名类由 8.1、关闭删除由 7.6 负责。固定长度类的 output buffer 不足时不输出半个结构；此 allowlist 没有可部分成功的 variable-length 文件信息结果。codec golden vectors 固定每个字段、保留位、offset 与 padding。

基础信息的可设置字段为 `READONLY`／`HIDDEN`／`SYSTEM`／`ARCHIVE` 与创建、访问、写入时间。`DIRECTORY`、`REPARSE_POINT` 从 kind 推导，`NORMAL` 只在没有其它可报告位时出现；这些派生位和变更时间不能由调用方直接设置。显式零时间表示保持原值，不是 Unix epoch。缺席的历史 BirthTime／ChangeTime 固定报告零且查询不写回；其它时间来自 Attr。所有非零时间经过范围、安全转换和 FILETIME 边界校验，非法值在 mutation 前失败。内容／长度修改由 authority 更新时间；属性／时间动作更新 ChangeTime，不在本机补钟。

`Attr.AllocationKnown` 与 `AllocationSize` 是字节事实：先做 `CheckAllocation`，再要求该信息类所需的已知值，不把第三方 backend 强迫为 4096 对齐。SMB CREATE 可原样报告有效分配字节；[MS-FSCC](https://winprotocoldocs-bhdugrdyduf5h2e4.b02.azurefd.net/MS-FSCC/%5bMS-FSCC%5d.pdf) 的文件／目录信息结构若要求分配量为 cluster 整数倍，必须额外验证该类的 geometry 约束，不能将非对齐值暗中向上取整并冒充权威分配。`Space{Total, Used, Avail}` 先做 `Coherent`。Windows cluster 相关信息必须有可信的 `bytesPerSector`、`sectorsPerAllocationUnit` 与其乘积，且 `Total`／`Avail`／实际空闲能按该几何无虚构地表达；内置 authority 可由其已证明的 4096 虚拟 cluster 提供。第三方 backend 通过中立的可选 `VolumePresentation{VolumeID, Label, CreatedAt, BytesPerSector, SectorsPerAllocationUnit}` 能力提供不可变身份、时间及几何，并由 HTTP／wrappers 原样转送；label 也可由宿主可信 share 配置给出，但不能被请求方冒名替换。

`Space.Used` 是独立测量值，允许超过 `Total`。[MS-FSCC](https://winprotocoldocs-bhdugrdyduf5h2e4.b02.azurefd.net/MS-FSCC/%5bMS-FSCC%5d.pdf) 的 `FileFsSizeInformation` 只有总量／可用量，`FileFsFullSizeInformation` 只有总量／调用方可用／实际空闲量，均没有直接的已用量字段。Size 类分别编码实测 `Total`、`Avail`，不声称 `Total-Avail` 就是实测 Used；FullSize 类从实测 `Used` 得到 actual available=`Total-Used`，并另行编码实测 caller available=`Avail`。当 `Used > Total`，把 actual available 编成零会让 `Total-actualFree` 看起来恰好等于 `Total`，隐瞒超额的实测 Used；这两个标准容量 QUERY_INFO 类在该状态均明确失败，不返回貌似完整的容量。任一字段不能被 cluster 几何精确编码、`Space` 不自洽或 `Space` 缺失，也使相应容量类失败。每次容量查询读取当前 authority `Space`，不使用上次查询的数字；状态可暴露可查询但无法在 SMB 标准容量类表达的 overquota 原因。

超额不撤销 SMB tree 或既有 FileId：READ、身份信息查询和有助于释放空间的截短、删除与 CLOSE 仍按原访问权、共享和 authority 准入执行，增长由 authority 配额判定并在该次写入报容量错误。配额在发布后下降时也只改变后续容量类结果及真实写入准入，不对整个 tree 加全局 fence。没有配额而 `Space` 返回 `ENOSYS` 时容量类明确失败，文件操作依其自身能力继续；FileId、allocation 等非容量字段只在其各自的事实缺失时失败。Volume serial 必须来自可信 volume identity 的稳定投影，不能依连接随机生成；同一 volume 的 FileId 与 serial 组合不混淆别的 volume。

### 结果、预算与动作

QUERY_INFO 输入／输出长度以无溢出算术核对；固定类完整部分不足时返回对应长度／buffer 错误，不输出半个 FILETIME、对象 ID 或容量结构。属性丰富 Attr 与 label 的完整结果在 backing 前预留预算；metadata、frame、outstanding response、global／share 额度有限，拒绝信息工作不消耗清理专用容量。

SET_INFO 使用 7.4a 同 action owner 与 typed replay。Completed receipt 不携带原 Attr，仍需同 ID／同负载重投原方法；Unknown、Retired、授权失败或未绑定 NotExecuted 不能新建修改。时间／属性组合的 `Metadata` 是公开 metadata 权限的更新，EOF 的 `ContentEffects` 是已密封数据派生能力；两者不互相授予权限。

### 范围切分

| 延后能力 | 类型与保持的结构 | 请求结果 |
|---|---|---|
| [7.5 目录枚举](2026-09-28-smb-directory-enumeration.md) | 功能；另加冻结完整快照和 cursor，目录引用不转换为路径。 | QUERY_DIRECTORY unsupported。 |
| [7.6 关闭删除](2026-09-28-smb-close-delete-obligation.md) | 持久义务；打开时 owner 取得，不能借 SET_INFO 无主删除。 | delete disposition unsupported。 |
| [8.1 名字修改](2026-09-28-smb-guarded-name-mutation.md) | 功能与 ancestry guards；旧名字不能冒充当前名字。 | rename／link／当前名字类 unsupported。 |
| [8.2 范围锁](2026-09-28-smb-range-lock-cancel.md) | 保证；信息修改继续走受控 reference 与同一对象排序。 | LOCK 保持未实现行为。 |
| [8.3 通知与缓存](2026-09-28-smb-change-notify-cache-coherence.md) | 保证；对象／revision 事实保留，不能用本段查询通过宣称私有缓存一致。 | Windows 缓存资格未完成。 |

## 备选方案

**所有 SET_INFO 类使用通用结构并把未知字段归零。** 无法证明的 allocation、时间或容量会被误报为事实，受支持类也可能静默忽略设置。选择显式类表与完整结构检查。

**Windows 层统一用 SQLite 的 4096 cluster。** 第三方 FileStorage 可以报告有效非对齐分配字节；固定 cluster 会让信息与真实额度矛盾。只在 capability 证明时采用该几何。

**完整 7.4 一个 PR。** 数据、文件信息和 volume geometry 同时审查会交叠不同权限与故障证明。选择三个目的，内容访问先落实关闭排序与动作确认；7.4b/c 复用这些机制并独立增加类表／presentation。

直接 WriteAt 后补 ARCHIVE 的原备选及拒绝理由，由独立[7.4a 决定](../../implemented/feature/2026-09-28-smb-file-data-io.md)拥有。

## 验收标准

- 文件五类与 volume 五类的每个字段、固定长度、padding 和访问权都有 golden vectors；未知／损坏来源不能给 plausible 数据。
- Basic 属性／时间组合在 exact File／NodeReference 上单动作提交；EOF 与 READONLY 条件、ARCHIVE、长度同时完成，rename／unlink／replacement 后仍是原对象。
- 同 action 恢复、metadata／byte 权限分离、非法 flags／classes／lengths 与结果预算有零效果拒绝测试。
- 四个时间、可设置／派生属性、NodeID／serial、allocation 与 geometry 逐字段来自权威事实；缺席历史 Birth／Change 报稳定零，不暗中迁移事实。
- Space.Used>Total 时两个容量类明确失败，tree、读取和释放能力继续；正常 Avail<Total-Used 时分别报告 caller 与 actual 空闲；变化后下次查询读当前事实。
- 第三方无 Space、不可信 geometry、非对齐 allocation、无法表达或溢出只使相应类失败；不假定所有 backend 是内置 cluster。
- 每段包含实现、package-local 正常／错误／资源测试、相关设计同步及独立 implemented note；协议模拟不能替代最终原生资格。

## 风险

第三方分配量与 geometry 可能无法共同证明部分 Windows 信息，受影响类需明确失败；真实 redirector 对失败容量的解释仍由后续原生验收观察。标准容量结构不能表达超额 Used，不能将保留真实错误及读／释放能力称作完整三元组容量验收。

EOF 和显式属性并发会增加条件冲突，必须复用 7.4a 的绑定未执行与新动作界线。volume creation fact 无真实来源时对应类失败，不能用当前时间／零补齐。真实 Windows 缓存可能掩盖正确后端查询；通知与原生资格仍是独立工作。
