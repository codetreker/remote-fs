# Agent Note: SMB volume 信息投影

Status: proposed

## 问题

Windows 程序需要通过已经打开的对象取得真实的 volume 身份、时间与容量。已有用量可能超过后来下调的配额；标准 SMB 容量结构不能直接报告 Used。未知 creation time、allocation geometry 或容量不能由当前时间、固定 cluster 和旧数字猜测；失败的容量查询也不能阻止程序读取文件或释放空间。

本提案拥有[Windows 总提案](2026-09-16-windows-network-drive-support.md)中的 **7.4c volume 信息**。普通文件 READ／WRITE／FLUSH 由[文件内容访问](../../implemented/feature/2026-09-28-smb-file-data-io.md)拥有；五种文件查询与 Basic／EOF 修改由[文件信息](../../implemented/feature/2026-09-28-smb-file-information.md)拥有。文件信息建立的可信 volume identity／allocation geometry 基础只负责文件字段，不能代替本提案的容量、label、creation fact 与能力投影。

需求依据为 [R-WIN-2、R-WIN-5、R-WS-5、R-INT-8](../../../../docs/spec/requirements.md)。本提案只承担 volume 信息能力，不宣布完整 Windows drive 已完成。

## 提案

### 权威来源与查询

volume 信息以 `InfoType/class → rights → 完整输出预算 → 权威来源 → encoder` 登记显式 allowlist。每次查询核对 FileId、session incarnation、tree、reference kind、对象 ID、固定 volume 身份与当前查询授权；已有引用不转换成旧名字查找。不能从部分事实或错误拼出成功信息。未知 class、flag 与畸形输入在 backing 前明确拒绝。

中立可选 presentation 能力携带 `VolumeID`、稳定 `Label`、`CreatedAt`、`BytesPerSector` 与 `SectorsPerAllocationUnit`，不含 Windows 协议或平台模式。FileId serial 与 FileFsVolumeInformation serial 同源于可信 volume identity 的稳定投影；不能按连接随机生成，跨重连的同一 volume 身份不变。label 也可由宿主可信 share 配置给出，不能被请求方冒名替换。creation fact 缺失时相关类整体失败，不用零或当前时间补齐。

最小身份／geometry 能力与完整 presentation 分开检查。第三方 backend 缺少 label、creation fact 或 Space 时，只失败依赖该事实的类；FileId 查询及文件读写仍依各自能力工作。HTTP、objectstore、locked、limited 与 replicated wrappers逐层传递实际来源并保留 checker，不由包装器名字或内置 storage 类型推导保证。

### 目录与职责

| 路径 | 职责 |
|---|---|
| `packages/smb/commands_volume_info.go`、`volume_geometry.go` | volume 类表、固定 identity、当前 Space 与 geometry 的精确投影 |
| `packages/smb/internal/wire/file_info.go` | QUERY_INFO extent、长度、class codec 与完整输出 |
| `packages/storage` | 中立 presentation 与 geometry 能力、来源一致性和数值验证 |
| HTTP capability／wire 与 storage wrappers | 真实能力、严格 DTO、identity 与 geometry 原样转送 |
| native／objectstore | 经健康验证的持久 identity、creation fact、label、allocation／capacity 单位及实测 Space |
| 相邻 package-local tests、实施时设计文档 | 类表 vectors、数值不可表达、预算／授权、当前状态与能力故障 |

volume 查询复用逐引用 gate 与原 typed FileId。它没有条件修改动作，不复用 SET_INFO owner；当前 authority 状态与完整响应都在本次查询中取得，不缓存上一次的 Space 数字。

### 信息类

类值与固定长度按 [MS-SMB2](https://winprotocoldocs-bhdugrdyduf5h2e4.b02.azurefd.net/MS-SMB2/%5bMS-SMB2%5d.pdf) QUERY_INFO 和 [MS-FSCC](https://winprotocoldocs-bhdugrdyduf5h2e4.b02.azurefd.net/MS-FSCC/%5bMS-FSCC%5d.pdf) 2.5 逐结构验证。所有类的 `InfoType=0x02`，要求活 FileId 与当前 volume 查询授权。

| 类 | 权威来源 | 输出规则 |
|---|---|---|
| `FileFsVolumeInformation` `0x01` | 持久 volume identity、稳定 label／creation fact | 固定 18 字节加 UTF-16 label：32-bit serial 与 creation time；任何必要事实缺失使整类失败 |
| `FileFsSizeInformation` `0x03` | 当前实测 `Space.Total`／`Space.Avail` 与 geometry | 固定 24 字节：`Used <= Total` 时精确编码实测总量／调用方可用量；不将 `Total-Avail` 冒充实测 Used |
| `FileFsDeviceInformation` `0x04` | endpoint 实际磁盘／远端设备事实 | 固定 8 字节 device type／characteristics，只设置真实能力位 |
| `FileFsAttributeInformation` `0x05` | 当前实际大小写、名字长度与文件系统能力 | 固定 12 字节加 UTF-16 filesystem name，仅设置已实现 flag |
| `FileFsFullSizeInformation` `0x07` | 当前实测 `Space.Total`、`Space.Used`、`Space.Avail` 与 geometry | 固定 32 字节：actual available=`Total-Used`，caller available=`Avail`；两者分别报告且精确可编码 |

已定义但未实现的类返回 `STATUS_NOT_SUPPORTED`，协议不认识的类返回 `STATUS_INVALID_INFO_CLASS`；畸形 length／flag 按规范的长度错误或 `STATUS_INVALID_PARAMETER` 失败。security／quota、volume label setter、FS control／object ID setter 不属于本提案。固定结构不足不能输出半个时间、serial 或容量字段，变长 label/name 的结构与长度按各类规范处理。

### 容量与 geometry

`Attr.AllocationKnown` 与 `AllocationSize` 是节点实际占用额度的字节事实；SMB 不从 Size 推算，不对第三方有效非对齐值暗中向上取整。需要 cluster 整数倍的结构必须额外验证该类的 geometry 约束。`BytesPerSector`、`SectorsPerAllocationUnit` 和其乘积来自可信能力；随附 authority 只有在完整链证明其 4096 虚拟 cluster 时提供对应几何，不能在 Windows 端统一写死。单位为零、乘积溢出、非法 geometry 或字段不可整除，都使受影响信息类失败。

`Space{Total, Used, Avail}` 先做 `Coherent`。Used 是独立测量值，允许超过 Total；容量报告与写入准入使用同一种额度口径，仍由引用持有的 detached 文件继续计入。Size 类编码实测 Total、Avail，不声称 `Total-Avail` 等于 Used；FullSize 类从实测 Used 得到 actual available=`Total-Used`，另行编码 caller available=`Avail`。caller available 小于 actual available 时保留两者区别。

标准 Size／FullSize 结构没有直接的 Used 字段。`Used > Total` 时将 actual available 设零会使 Windows 得到恰好等于 Total 的用量，隐藏超额。因此两个容量类在该状态均明确返回 I/O 错误，不截断、舍入或虚构容量。字节值不能被所需容量单位整除、Space 缺失／不自洽或无法取得可信几何，同样失败。每次查询读取当前 authority Space；状态可报告实测值与标准容量类无法表达的原因，但不得重用旧成功数字。

超额或容量查询失败不撤销 tree 与既有 FileId。READ、身份查询和有助于释放空间的截短、删除与 CLOSE 仍依原访问权、共享与 authority 准入执行；增长由 authority 在造成它的调用中拒绝并报告配额错误。发布后下调配额只改变后续容量类结果及真实写入准入，不对整个 tree 加容量 fence。Space 返回 `ENOSYS` 时容量类明确失败，文件操作按自身能力继续。

### 预算与错误

QUERY_INFO 输入／输出长度按无溢出算术核对，完整固定部分与变长 label/name 在 backing 前预留结果预算；metadata、frame、outstanding response、global／share 额度有限。新的信息工作不能消耗清理专用容量。authority 不可达、结果 identity 不符、事实缺失或不可表达时整类明确失败，不使用零字段或较旧数据构造成功结果。

### 范围切分

| 延后能力 | 延后代价与保持的结构 | 可观察结果 |
|---|---|---|
| [7.5 目录枚举](2026-09-28-smb-directory-enumeration.md) | 增加冻结完整快照与 cursor；目录引用不转换成路径 | QUERY_DIRECTORY 按独立能力决定支持 |
| [7.6 关闭删除](2026-09-28-smb-close-delete-obligation.md) | 增加持久义务；不得借 volume 查询产生删除效果 | disposition 由独立能力处理 |
| [8.1 名字修改](2026-09-28-smb-guarded-name-mutation.md) | 增加 ancestry guards；旧打开名字不能冒充当前绑定 | 名字查询／修改由独立类表处理 |
| [8.2 范围锁](2026-09-28-smb-range-lock-cancel.md) | 增加跨入口范围保护；查询继续使用原受控引用 | LOCK 按独立交付范围处理 |
| [8.3 通知与缓存](2026-09-28-smb-change-notify-cache-coherence.md) | 增加失效协议与原生验证；不得将查询正确性当作私有缓存资格 | 本提案不宣称缓存一致性 |

## 备选方案

**所有信息类使用通用结构并把未知字段归零。** 未知时间、allocation 或容量会被误报为事实，部分设置也可能静默忽略。显式类表、真实来源和完整结构检查让失败留在对应请求。

**Windows 层统一使用 SQLite 的 4096 cluster。** 第三方 FileStorage 可以报告有效非对齐分配字节，固定 cluster 会使查询与真实额度相矛盾。只有完整 capability 证明时才采用该几何，无法表达的类明确失败。

**只提供 volume identity，容量类一律延后。** 它可以避免几何与超额用量的表达问题，但不完成 Windows 程序需要的容量信息。身份及最小 allocation geometry 归文件信息，当前查询真实 Space 与标准容量编码归此独立目的。

**完整 7.4 一个 PR。** 文件内容、文件信息与 volume geometry 交叠不同权限和故障证明。三个独立目的分别落实内容的 owner／关闭排序、文件信息的权限／条件修改，以及 volume 的实测容量投影；仍共享同一引用与身份基础。

EOF 与固定 ARCHIVE 效果的取舍由[文件信息](../../implemented/feature/2026-09-28-smb-file-information.md)拥有；WriteAt 后补 ARCHIVE 的原备选由[文件内容访问](../../implemented/feature/2026-09-28-smb-file-data-io.md)拥有。

## 验收标准

- 五个 volume 类的每个字段、固定／变长长度、padding、访问权与预算具有 golden vectors；错误或损坏来源不返回貌似可信数据。
- identity／serial 跨重连稳定，label／creation fact 来自可信来源；缺席事实不补零或当前时间。
- Space.Used>Total 时两个容量类明确失败，原 tree 的读取与空间释放继续；正常 Avail<Total-Used 时分别报告 caller 与 actual 空闲。
- 下调配额或完成写入后的下一次查询读取当前 Space，内容／分配／容量口径一致。
- 第三方无 Space、无可信 geometry、非对齐 allocation、容量不能整除、溢出或能力链缺失只失败依赖该事实的类；不假定所有 backend 都是内置 cluster。
- 每个实现包含 package-local 正常／错误／资源测试、相应设计同步与独立 implemented note；模拟协议不能替代最终原生资格。

## 风险

第三方 allocation 与 geometry 可能无法共同证明某些 Windows 信息，相关类必须明确失败；真实 redirector 对失败容量查询的解释由原生验收观察。标准容量结构无法表达超额 Used，保留真实错误以及读取／释放能力不能宣称已通过完整容量三元组验收。

volume creation fact 无真实来源时对应类失败。查询路径的正确性不证明真实 Windows 缓存已失效；通知及 network-drive 原生资格仍需各自验证。
