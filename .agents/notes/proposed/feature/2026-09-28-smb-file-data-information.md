# Agent Note: SMB 身份文件 I/O 与信息投影

Status: proposed

## 问题

SMB `CREATE` 取得稳定 FileId 后，Windows 程序仍需要经该引用读写内容、截断、刷新并查询或修改文件信息。路径可在打开后改名、删除或指向另一个节点；每次按旧路径重新查找会把已打开的引用转给新对象。Windows 的属性、时间和空间信息还不能由零值、固定 cluster 或本机缓存猜测。写入后补设 `ARCHIVE` 会在两次权威操作之间暴露不符合 R-WIN-5 的状态，且响应丢失时无法判断哪一半完成。

本提案是[Windows 网络驱动器总提案](2026-09-16-windows-network-drive-support.md)中 7.4 的独立交付；依赖 [7.3 的有界 CREATE/CLOSE](../../implemented/feature/2026-09-28-smb-bounded-create-close.md)所持有的 FileId、FileSession、引用清理、共享准入与同动作回执。它实现 [R-FS-6 至 R-FS-9、R-WIN-2、R-WIN-4、R-WIN-5](../../../../docs/spec/requirements.md) 在身份文件 I/O 与信息查询上的部分，不宣布整个 Windows drive 已完成。

## 提案

在 `packages/smb` 内将已签名、已授权的 READ／WRITE／FLUSH／QUERY_INFO／SET_INFO 请求路由到 7.3 保留的 `File` 或 `NodeReference`。FileId 的 session incarnation、tree、引用种类和授予的访问权须在每次请求核对；引用完成关闭或其 authority 退休后不回退到名字查找。普通文件读取用 `File.ReadAt`；Windows 写入、长度和属性修改使用 `ConditionalFileMutation.MutateFile`：端点解释所观察的 `READONLY` 位，权威动作在最终排序点核对同一 metadata token 并提交内容／长度与 `ARCHIVE`。成功响应必须代表服务端确认的结果；丢失响应只查询或重投同一 action。

7.4 只发布能够从权威事实完整构造的信息类。未支持的信息类、flags、channel 与修改组合在接触 backing 前返回明确的 unsupported／invalid 状态。空间信息由权威 `Space` 加上可验证的 volume 几何投影；几何证明不足时不构造貌似正确的 cluster 数。目录枚举、按名变更、关闭时删除、范围锁与缓存失效分别由 7.5、7.6 和 8.x 交付。

### 目录与模块形状

下列是本 PR 的计划位置，不表示这些文件已经存在；实施时名称可随当前源码结构调整，但所有权保持不变。

| 路径 | 状态 | 职责 |
|---|---|---|
| `packages/smb/commands_file_io.go` | 新增 | FileId 准入、读写／刷新调度、action owner 和响应结算；不解析 wire 偏移。 |
| `packages/smb/commands_file_info.go` | 新增 | 文件／volume 信息类表、访问权要求、身份 Attr 与空间投影。 |
| `packages/smb/windows_metadata.go` | 新增 | 四个可设置位与派生位、时间编码、metadata token／payload 转换；不持有远端状态。 |
| `packages/smb/volume_geometry.go` | 新增 | 在 tree 建立前校验 volume presentation／容量几何，将字节 `Space` 转成 Windows cluster 响应。 |
| `packages/smb/internal/wire/file_io.go` | 新增 | READ、WRITE、FLUSH 请求及响应的长度、offset、channel、credit 安全解码／编码。 |
| `packages/smb/internal/wire/file_info.go` | 新增 | QUERY_INFO／SET_INFO 的受支持信息类 codec、长度和对齐检查。 |
| `packages/smb/commands_session.go`、`session_registry.go`、`config.go` | 修改 | 接上 7.3 的 FileId 路由、每连接 I/O 预算与持有 action 的生命周期。 |
| `packages/storage/capabilities.go`、`packages/storage/capability_validation.go` | 修改 | 定义、预检中立 `VolumePresentation` 及其数值一致性，不改变 `ConditionalFileMutation` 的动作形状。 |
| `packages/transport/httprest/file_capability_server.go`、`file_capability_client.go`、wire codec 和 `packages/storage/limited/` | 修改 | 让可信 volume presentation 穿过 HTTP 和 wrapper，保留缺席与错误语义，不回填假 geometry。 |
| `packages/metastore/sqlite/`、`packages/storage/objectstore/` | 按需新增／修改 | 为内置 volume 提供持久或可证明的 presentation／geometry；数据迁移须明确历史 creation fact 的真实来源。 |
| `packages/smb/*_test.go`、`packages/smb/internal/wire/*_test.go`、storage／HTTP capability tests | 新增／修改 | 类表 golden vectors、条件动作竞争、重复回执、非法 allocation／geometry、credits 与异常路径。 |
| `docs/design/client/smb-endpoint.md` 与相关 `docs/design/server/` | 修改 | 记录已交付命令、能力协商、动作和信息投影；对应 implemented note 与测试同 PR 提交。 |

### 准入与命令路径

请求经 7.3 的签名、会话、tree、FileId 与 related compound 解析后，按 `request → 授权／引用准入 → 额度预留 → backing → 结果核对 → wire 编码` 处理。调度表显式区分读、写、属性读、属性写、同步与 volume 查询；每次经当前 `Authorize` 复核对象与操作。普通 READ／WRITE 仅接受 regular `File` 和相应打开访问；目录／metadata-only `NodeReference` 不获得字节方法。查询文件基本信息只要求可读属性的引用，不能因没有 data-read 权限而转用路径。引用上的当前名字查询留给后续名字观察：不能回显 CREATE 时的旧名字。

请求 offset、length、`MinimumCount`、信息类长度与结构大小先用无溢出算术校验，并以 SMB negotiated MaxRead／MaxWrite、`Limits.MaxIOBytes`、frame 上限、当前 credits 和结果 charge 的最小值约束；不可把 client 声明的长度直接用于分配。未支持的 RDMA channel 或压缩、稀疏、allocation 修改请求在 backing 前失败。credit 只在完整响应形成后按连接现有规则返还；预留失败不能执行修改。完整签名 frame 的字节不因 related FileId 替换而变动。

READ 使用 `File.ReadAt(ctx, offset, boundedLength)` 的同一 `FileRead{Data, Attr}`，核对 Attr.ID 等于句柄身份、kind、长度、有效 allocation 和返回字节不超过请求／EOF，且 `MinimumCount` 与协议允许的短读／EOF 结果一致。一次 READ 只承诺该次 `ReadAt` 捕获的一份内容 revision；一次较大的应用调用被 redirector 分为多次 SMB READ 时不承诺跨请求快照。读失败或结果不自洽时不发旧数据，也不合成 EOF。

WRITE 用一个 `FileMutation{Action, Kind: MutateWriteAt, Offset, Data, ExpectedMetadata, Metadata}`；SET_INFO 的 EOF 用 `MutateTruncate`；时间／属性用 `MutateAttributes`。端点先读取 Windows 属性 namespace，拒绝已观察到的 `READONLY` 与不相容的写入；`ExpectedMetadata` 保存该观察的版本或缺席条件，`Metadata` 同时写入新的四位状态。最终 authority 排序点比较 token，再一次性提交内容／长度和 `ARCHIVE`；若期间属性位变化，CAS 必须明确未执行，不能让早先对 READONLY 的判断继续放行。成功改变内容或长度即使长度最终仍为零也置 `ARCHIVE`；明确未执行的 CAS 冲突才允许重新观察并生成新 action。显式属性清除 `ARCHIVE` 与其它设置一起以单个动作提交；并发写入与清除按 authority 顺序决定最终位。

目录和 metadata-only FileId 只持有 `NodeReference`，不能假装持有可写字节的 `File`。现有 `NodeReference` 的可选 `ConditionalFileMutation` 接受 `MutateAttributes`，以同一 `FileMutation{Action, Kind: MutateAttributes, ExpectedMetadata, Metadata, Attr}` 组合属性／时间修改；对该引用提交 `MutateWriteAt`／`MutateTruncate` 明确得到 `EBADF`。7.3 创建这些引用时，若 SMB 打开请求授予 metadata 修改权限，`NodeRefOptions.MetadataAccess` 必须包括 `WriteMetadata`；7.4 每次 SET_INFO 仍核对句柄 access、当前授权、活引用 scope 与元数据条件。受支持 tree 在接纳文件命令前预检这一能力贯穿 backing chain；不能用先 `SetMetadata`、后 `SetAttr` 的两个动作模拟组合修改。

SMB 不凭缓存预检放行 READONLY：它读取权威 metadata，并用条件动作在最终效果点证明该观察仍成立。其它入口不解释 Windows 位。普通 `File.WriteAt`／`Truncate` 继续供不请求 Windows 位语义的入口使用；本入口不能用它们和后续 `SetAttr` 拼接。既有 7.3 reset／create 的初始属性原子语义保持一致。FLUSH 调 `File.Sync` 确认引用健康和 backing 所需 durability barrier；CLOSE 不承担延后提交。Sync 的未知或错误按 I/O 错误交付，不能报告“已刷新”。

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

### 动作记录、错误与资源

每次权威修改在调用前生成一次 `FileActionID`，复制不可变请求负载并由 7.3 的 per-session action owner 保留至终态或有界退休。HTTP 客户端可能自身结算一次丢失的响应；SMB 层收到未知结果时可用 `FileActions.QueryFileAction` 判断记录状态，但 `FileActionReceipt{Action, Operation, Outcome}` 不携带原成功／错误或 Attr。`Completed` 也必须用同 ID、同负载重投原 `MutateFile` 方法，取回并核对原 typed result；`NotExecuted` 才可重新观察后发起新动作；`Pending` 等待受限；`Unknown`／`Retired`、查询故障、authority incarnation 变化不能被当作未执行，当前 SMB 请求返回 I/O 错误并保留可诊断 owner。传输取消本身不证明 mutation 未发生。不能把 Windows 程序的再次 WRITE 当作同一动作，因为应用不持有内部 action ID。

`EBADF`／已关闭 FileId 对应无效句柄；目标身份或会话退休对应目标消失／网络会话错误；已验证的访问拒绝、share 冲突、容量不足各映射到可区分的 SMB 状态；协议字段错误是 invalid parameter；不支持的类是 not supported；未知效果、失联、损坏结果和预算承诺违约是 I/O 错误。映射表只对已知的最终 errno 做选择，不把所有失败压为 not found。即使 backing 返回部分 Attr 与错误，也不把部分结果编码为成功。

每个请求在调用 backing 前预留输入字节、候选完整响应、已接纳 action 记录和 outstanding frame／credit 费用；包含复杂 metadata 的响应在编码前经过 Attr result budget 和整数溢出检查。响应不能完整装入声明的 output buffer 时按该类协议语义返回 buffer error 或完整条目边界结果，不截断 FILETIME、ID、状态或未声明字段。每个 volume 与全局资源额度均有限；拒绝数据工作仍保留清理与状态查询容量。额度、active／pending／unknown action 和最后错误类别进 `Status`，不暴露内容或认证密钥。

### 本 PR 范围切分

| 延后能力 | 类型、代价与保持的形状 | 7.4 外部结果 |
|---|---|---|
| [7.5 目录枚举](2026-09-28-smb-directory-enumeration.md) | 功能；另增冻结快照与 cursor。目录 FileId 已保留身份与 scope，7.4 不把它转换成路径。 | `QUERY_DIRECTORY` 明确 unsupported。 |
| [7.6 关闭时删除](2026-09-28-smb-close-delete-obligation.md) | 保证；必须由打开时持久 owner 取得，不能在 7.4 借 SET_INFO 的 delete flag 做无主效果。 | delete disposition 请求明确 unsupported。 |
| [8.1 按名改动／当前名字](2026-09-28-smb-guarded-name-mutation.md) | 功能加 guard 形状；当前名字不能回显 CREATE 的旧路径。 | rename、link、当前名字类明确 unsupported。 |
| [8.2 范围锁](2026-09-28-smb-range-lock-cancel.md) | 保证；请求和权威 I/O 的未来排序不能被旁路。7.4 的 I/O 必须走同一受控引用接口而不是直接调用 object store。 | LOCK 仍明确 unsupported。 |
| [8.3 通知与缓存](2026-09-28-smb-change-notify-cache-coherence.md) | 保证；7.4 不宣称预热 Windows 缓存的跨入口一秒可见。对象身份与 revision 信息不被丢弃，以供后续失效。 | 尚不宣告 Windows drive 满足 R-WIN-8。 |

## 备选方案

**直接调用 `File.WriteAt`，再调用 `SetAttr` 设置 `ARCHIVE`。** 两个效果之间可以发生其它读取、故障或响应丢失；内容已成功但 `ARCHIVE` 未变会违反 R-WIN-5，且事后无法用同一 action 重投这两个效果，因此不选。

**所有 SET_INFO 类使用一个通用结构并将未知字段归零。** 可减少 codec 分支，却会把无法证明的分配量、时间或空间说成事实，并可能在一个受支持类里忽略不支持的设置，因此不选。

**Windows 层统一采用内置 SQLite 的 4096 字节 cluster。** 内置 volume 的账本已有此几何，但 `FileStorage` 允许第三方实现报告有效的非对齐分配字节；固定值会让空间查询与实际额度矛盾，因此只在 capability 可证明时采用。

## 验收标准

- READ 在 rename、unlink、同名替换后仍返回旧 FileId 的原对象，字节与 Attr 属于同一 revision；并发覆盖下不出现拼接成功；EOF、短读、`MinimumCount`、正负 offset／长度边界均有协议测试。
- WRITE、EOF 与显式属性／时间设置在 authority 的一个动作中核对 READONLY 及 metadata token；内容／长度成功时 ARCHIVE 与数据同时可见。CAS 冲突、远端提交前后断线、一次／多次响应丢失、会话退休分别得到可判定的成功、明确未执行或未知错误。
- 目录与 metadata-only FileId 的属性／时间组合修改由现有 `NodeReference.(ConditionalFileMutation).MutateFile(MutateAttributes)` 单一身份动作完成；改名、detached、同名替换时不落到替代节点，scope 和 metadata 条件在最终排序点验证。
- FLUSH 观察真实 barrier；失败不能等到 CLOSE 才暴露。READONLY、错误 access、错误引用种类、过期 FileId、tree 不匹配和授权撤销在 backing 效果前失败。
- 文件身份、四个时间、可设置／派生属性和容量／allocation 查询与权威事实逐字段相符；未知时间返回稳定零，非对齐 allocation 可报告为字节；不可信 geometry、无 `Space`、溢出与非法请求明确失败。
- `Space.Used > Space.Total` 时 SMB tree 和 FileId 仍可用于读取及释放配额；两个容量 QUERY_INFO 类明确失败，不把 Used 截到 Total。正常容量中 `Avail < Total-Used` 时 Size 类精确报告自己的两个字段，FullSize 类分别报告调用方可用和从实测 Used 得出的实际空闲。发布后 quota 改变由下一次容量查询读取当前事实，写入准入按 authority 当次额度执行。
- wire 解码、输出长度、credits、frame、action 与 per-volume 额度在边界下有测试；没有部分成功或未计费 owner。HTTP、直接 adapter 与 wrapper 的相同条件语义被 contract tests 覆盖；现有 Linux／SDK 写入语义不因 Windows metadata 改变。
- 本 PR 包含实现、针对性正常／错误／资源测试、`docs/design/` 同步及改写后的 implemented note；不以模拟协议客户端通过替代最终真实 Windows 验收。

## 风险

第三方 backend 的分配量与 geometry 可能无法共同证明某些 Windows 空间信息类，受影响的类明确失败；发布前须用真实 redirector 验证这些失败不会被 Windows 误认作零容量。标准 SMB 容量结构不能表示超额时的实测 Used，所以 Windows 程序在该状态无法通过标准容量类取得 R-WS-5 的完整三元组；保留真实错误及读／释放能力是明确的兼容取舍，不能标成完整容量验收通过。条件 metadata 动作会在并发属性写入下增加明确冲突及重读次数，必须保持同一动作结果与新动作重试的界线。真实 Windows redirector 的缓存可能掩盖正确的后端读取；该能力属于 8.3 和最终原生验收，7.4 的通过不能当作缓存一致性通过。
