# Agent Note: SMB 完整目录捕获与句柄内分页

Status: proposed

## 问题

Windows 目录枚举按句柄多次领取有限大小的结果。远端目录在两次请求间可能修改，而且同一 volume 允许 Linux／SDK 创建非法 UTF-8、Windows 不可表示或大小写等价的名字。若每页独立向远端读取，或先过滤坏名字再输出，Windows 程序会把遗漏条目误认为不存在；若返回上一页的缓存并在失联时补空目录，错误会变成真实的删除依据。

本提案是[Windows 网络驱动器总提案](2026-09-16-windows-network-drive-support.md)中 7.5 的独立交付，依赖 [7.3 的有界目录 FileId](../../implemented/feature/2026-09-28-smb-bounded-create-close.md) 和 [7.4 的身份信息投影](2026-09-28-smb-file-data-information.md)。它解决 [R-FS-9、R-WIN-2、R-WIN-3](../../../../docs/spec/requirements.md) 的目录枚举部分。目录变更通知和预热目录缓存的一秒失效由 8.3 负责；本提案不宣称完成它们。

## 提案

一个目录 FileId 的 QUERY_DIRECTORY 使用 `DirectoryReader.ReadDirNodeBounded` 对其 `DirectoryTarget{NodeID, Scope}` 做一次完整的权威捕获。`ListResult` 在读取每个名字与 metadata 前收取由 SMB 保留形状决定的费用；任何出错都使本次捕获失败，不能把已收集前缀当作完整目录。捕获全部条目之后，先验证所有 raw name、对象身份、种类、属性与编码预算，再从已验证的不可变 snapshot 按 SMB output buffer 分页。过滤模式只决定向调用方输出哪些条目，不能决定哪些 backing 条目免于 Windows 名字验证。

每个目录 FileId 拥有一份冻结 snapshot、查询模式和下一条 cursor。首次请求建立，普通后续请求只从该份 snapshot 输出；`RESTART_SCANS`／`REOPEN` 释放旧捕获并取得新的完整捕获。`RETURN_SINGLE_ENTRY` 限制本次最多一个完整条目。`INDEX_SPECIFIED` 只能引用同一 FileId 当前捕获实际发出的 index；旧捕获、别的句柄或未发出 index 明确失败。完成枚举返回协议规定的无更多项状态，保留与旧快照相同的 revision 直到 restart 或 CLOSE。正常 page 不能混入新 revision。

### 目录与模块形状

下列是本 PR 的计划位置，不表示这些文件已经存在。

| 路径 | 状态 | 职责 |
|---|---|---|
| `packages/smb/directory_snapshot.go` | 新增 | 捕获入口、`ListResult` 精确保留费用、整目录 Windows 名字／属性验证，创建不可变 snapshot。 |
| `packages/smb/directory_cursor.go` | 新增 | FileId 内的 snapshot generation、模式、issued index 与 cursor；同步、restart、close 时释放。 |
| `packages/smb/commands_directory.go` | 新增 | QUERY_DIRECTORY 准入、flags、授权、预算预留、分页结果与错误转换。 |
| `packages/smb/internal/wire/query_directory.go` | 新增 | 请求路径与 flags 解码、信息类 codec、完整条目链接偏移和 output buffer 限制。 |
| `packages/smb/windows_name.go` | 修改／复用 | 与 7.3 CREATE 同一 Windows 名字验证及 case-fold 规则；不在目录入口另造一套。 |
| `packages/smb/session_registry.go`、`config.go`、`status.go` | 修改 | 每 FileId 与每 volume 的 snapshot／cursor 额度、active 与失败状态、CLOSE／tree 退出释放。 |
| `packages/storage/`、HTTP、wrappers | 契约核对，必要时修改 | 保证 `DirectoryReader` 的完整 bounded capture、`DirectoryObservation`、scope 和 collector 失败语义贯穿全部 backing chain。 |
| `packages/smb/*_test.go`、`packages/smb/internal/wire/*_test.go`、storage／HTTP bounded-directory tests | 新增／修改 | 各信息类 golden vectors、DOS wildcard、全量坏名、revision、分页／index、预算与清理回归。 |
| `docs/design/client/smb-endpoint.md` 与相关 server design | 修改 | 记录当前已实现目录捕获和失败边界；implemented note 与针对性测试同 PR 提交。 |

### 捕获与验证

QUERY_DIRECTORY 先核对签名、session、tree、活 FileId、目录种类、可列目录访问和当前 authority 授权；不得把句柄原路径作为目录来源。`NodeReference` 的活 scope 连同 NodeID 传给 `DirectoryTarget`；scope 无效不能回退到裸 ID，目录改名后仍由原身份选择，已经 detached 且没有可服务的目录状态时返回权威错误。7.3 的 FileId cleanup owner 继续承担请求入场和 CLOSE 排空，snapshot 与句柄同寿命且可先于引用释放。

`NewListResult(maxCaptureBytes, fixedBytes, entryBytes)` 的费用函数按原始名字、最坏 UTF-16 名字编码、Attr scalar、metadata、FileIndex 表项、Go slice／map 与 SMB 结果临时预算计费，并做整数溢出检查。费用须可由调用方在 backend 加载 payload 前计算；编码长度只在整次 capture 完成后精确验证。一个 entry 超额或调用中途失败即清除 collector，返回资源／I/O 错误，绝不留下可分页的前缀。包括零条目的 observation 也须有可信 `ParentID` 与非空、合法的 opaque revision；不把异常零条目视为目录为空。

在任何条目可见之前，对完整捕获逐个检查：raw leaf 是有效 UTF-8、无 NUL／分隔符／Windows 禁止形式，按 7.3 同一 Unicode case-fold 规则不会与另一个名字相等，Attr.ID 非零且 kind 可投影，所需分配量／时间／metadata 值能被选定信息类编码。`ReadDirNodeBounded` 的 authority 契约保证条目与 observation 属于同一父目录的同一次捕获；端点须核对 observation 的 ParentID 与句柄目录 ID，不能通过逐条路径查询补证。若一个目录包含 `a`／`A`、非法 UTF-8、保留名、尾随空格或点等不可表示名字，整次查询失败；即使 pattern 不匹配坏条目也一样。不能自动重命名、剔除、替换为 U+FFFD 或修改 volume。已打开对象的身份 READ／WRITE 仍可工作。

冻结 snapshot 保存 `{ParentID, Revision, Entries[], pattern, generation, next, issuedIndices}`；每条 entry 保存原始名字、已验证 UTF-16 名字、权威 Attr 和稳定 ID。`ListResult.Entries()` 按 raw name 字节序排序，snapshot 冻结这一顺序；不能称其为 authority 扫描顺序，也不再按 Windows 比较器二次排序。`DirectoryObservation.Revision` 是 opaque 相等 token，不解析成序号或时间；它用于诊断、restart 和后续通知恢复的同一快照边界。7.5 不要求普通续页与当前目录 revision 一致：续页代表原捕获；restart 必须取得新的权威完整状态或失败。

### Cursor、flags 与 wire

请求 decoder 在访问 backing 前核对 `FileInformationClass`、`Flags`、FileId、输入 pattern 的 UTF-16 严格解码、长度、对齐和 `OutputBufferLength`。[MS-SMB2](https://winprotocoldocs-bhdugrdyduf5h2e4.b02.azurefd.net/MS-SMB2/%5bMS-SMB2%5d.pdf) 2.2.33 与 [MS-FSCC](https://winprotocoldocs-bhdugrdyduf5h2e4.b02.azurefd.net/MS-FSCC/%5bMS-FSCC%5d.pdf) 2.4 定义信息类和条目布局。本段的 allowlist 如下；固定字节是每条记录的最小头部，后接 UTF-16 名字，再按所选类计算 `NextEntryOffset` 对齐。所有输出字段来自同一 `ObservedEntry.Attr`、其 raw leaf 或稳定 NodeID，不能再按路径查一次。

| `FileInformationClass` | 最小固定头部与字段来源 |
|---|---|
| `FileNamesInformation` `0x0C` | 12 字节：virtual FileIndex 与 UTF-16 名字；只需完整名字验证，不需 Attr 时间／allocation 字段。 |
| `FileDirectoryInformation` `0x01` | 64 字节：FileIndex、Attr 四个时间、EOF、已知且可按 geometry 表示的 allocation、属性、UTF-16 名字。 |
| `FileFullDirectoryInformation` `0x02` | 68 字节：基本字段加 EA size；该入口不提供 EA，权威可证明无 EA 时报告零。 |
| `FileBothDirectoryInformation` `0x03` | 94 字节：full 字段加 ShortNameLength／ShortName；无短名索引时长度零、数组清零，不制造 8.3 alias。 |
| `FileIdFullDirectoryInformation` `0x26` | 80 字节：full 字段加稳定 64-bit NodeID。 |
| `FileIdBothDirectoryInformation` `0x25` | 104 字节：both 字段加稳定 64-bit NodeID。 |

扩展目录类 `0x3C`、`0x4E`、`0x4F`、`0x50`、`0x51` 需要额外 reparse／128-bit ID 布局，本段先返回 `STATUS_NOT_SUPPORTED`；真实 Windows redirector 若把其中之一作为正常枚举的必需请求，须在本 PR 内增补相同字段来源和测试后才能宣告 7.5 完成。未知类返回 `STATUS_INVALID_INFO_CLASS`。对 `0x01/0x02/0x03/0x25/0x26`，有效但不符合 MS-FSCC 该类 cluster 边界的 allocation 会使该类查询明确失败，不能修改 Attr 或捏造对齐量。短名编码为空仅在结构允许时使用；不响应短名查找。逐类 golden vectors 固定 `NextEntryOffset`、FileIndex、FILETIME、attribute、EA／short name、ID 和 padding。

Pattern 编译器按 [MS-FSA](https://winprotocoldocs-bhdugrdyduf5h2e4.b02.azurefd.net/MS-FSA/%5bMS-FSA%5d.pdf) 2.1.4.4 在已验证 Unicode 名字上匹配，不使用 Go glob／regex：`*` 匹配零或多个字符，`?` 恰一字符，`"`（DOS_DOT）匹配点或名字末尾零字符，`>`（DOS_QM）匹配一个字符或在点／末尾跳过连续 DOS_QM，`<`（DOS_STAR）只扩展到名字最后一个点的匹配边界；`*` 和 `*.*` 匹配全部。大小写忽略规则复用 7.3 的 Windows 比较器，不能在过滤时另造 Unicode 映射。编译后的有界状态机按 `(patternPosition, namePosition)` 去重，限制 pattern 长度和最多状态数；超限是明确资源错误，不指数级回溯。空 `FileNameLength` 在首次查询解释为 `*`，后续普通查询保持已固定 pattern；`RESTART_SCANS` 可在 pattern 缺席或与原 pattern 相同时重新捕获，`REOPEN` 允许给出新 pattern 并完整重取。无法验证的字面字符、路径分隔符或非法 flag 组合在捕获前失败。

第一条查询在成功取得完整 snapshot 后应用 pattern。`RESTART_SCANS` 放弃旧 snapshot，按当前协议允许的 search expression 完整重取并把 cursor 放到首个匹配项；`REOPEN` 同样重新取得捕获并重建搜索状态。capture 或验证失败时本次不输出任何条目；旧 snapshot 不再作为成功 fallback，句柄保持需要下一次明确重取或关闭的状态。`RETURN_SINGLE_ENTRY` 最多打包一条完整 entry。`INDEX_SPECIFIED` 只把 caller index 映射到本 FileId、当前 generation 已发出的 entry，并定位该捕获内的下一次输出位置；没有映射时拒绝，不把数值当 authority index、路径或可跨快照续用的 offset。

`FileIndex` 是 FileId 内 append-only 虚拟目录流的 32-bit byte offset：每条用独立于本次查询信息类的固定 canonical record charge（最大支持头部、UTF-16 名字及对齐）推进；重取时从上次高水位继续分配，旧数值不复用。映射仅接受本句柄当前 snapshot 中已经发出的 offset；若耗尽可表达范围，该句柄拒绝新的分页／restart，CLOSE 释放句柄。每个已发出的 index 映射只随当前 snapshot 保留，消耗 per-handle 容量。并发 QUERY_DIRECTORY 对同一 FileId 在 cursor lock 下串行：先完整编码并核对 output buffer，再原子推进 cursor 和登记已发出的 indices；若承载响应的连接写入失败，连接与 FileId 进入 7.3 的清理路径，不允许在同一不确定连接上继续跳页。新连接不继承旧 FileId；仍有效的同一连接可通过显式 restart 重新取得确定状态。

输出 builder 只放能完整容纳的条目，逐条计算 8 字节对齐的 `NextEntryOffset`，最后一条为零；固定字段、UTF-16 名字和 padding 一起计入。若第一个匹配条目装不下 buffer，不移动 cursor，返回 `STATUS_INFO_LENGTH_MISMATCH`；若已有至少一条完整条目，则返回该前缀并仅推进这些条目。首次捕获无匹配项返回 `STATUS_NO_SUCH_FILE`，已发出过条目后 cursor 到尾部返回 `STATUS_NO_MORE_FILES`，不把 authority 错误归入其中。QUERY_DIRECTORY 的 `CreditCharge` 按 `OutputBufferLength` 验证，且 output 不超过 negotiated `MaxTransactSize`、本端点 `MaxFrameBytes`、`Limits.MaxIOBytes` 和当前可用 credits；这些上限与 `RETURN_SINGLE_ENTRY`、snapshot charge 同时约束打包。不能截断名字、Attr 或跨请求混用 revision。

本投影仅输出 `ReadDirNodeBounded` 捕获的真实 child；不会凭旧路径合成 `.`／`..`，首条名字装不下时也不返回一条带半个名字的成功结果。[MS-FSA](https://winprotocoldocs-bhdugrdyduf5h2e4.b02.azurefd.net/MS-FSA/%5bMS-FSA%5d.pdf) 2.1.5.6.3 的 Microsoft object-store 算法在部分 first／restart 查询加入这两个伪项，且固定头部装得下而名字不完整时可返回 `STATUS_BUFFER_OVERFLOW` 与部分名字；[MS-SMB2](https://winprotocoldocs-bhdugrdyduf5h2e4.b02.azurefd.net/MS-SMB2/%5bMS-SMB2%5d.pdf) 3.3.5.18 将底层目录查询过程留给实现，因此这里不把 MS-FSA 的行为当作必需保证。7.5 的原生验收须分别以根／非根目录、显式 `.`／`..` pattern、restart、紧贴固定头部的 output buffer 验证 Windows redirector 与常见目录 API 是否接受本投影；若真实客户端依赖伪项或 partial-name overflow，须在 7.5 宣告完成前增加同一捕获里的可信父身份／属性来源和明确 cursor 结算，不能用旧 CREATE 路径合成。

### 错误与资源

无效 FileId、非目录引用、缺失 list access、错误 tree、撤销授权、退休 session、失效 scope、完整捕获失败、目录过大、非法名字、revision 不自洽和 response budget 违约各保留可区分的内部错误类别。协议映射只把已证明的目标不存在映射为 not found；失联、不完整结果、名字歧义和无法确认状态是 I/O／资源错误，不能转为 `STATUS_NO_MORE_FILES`。在不完整 capture 上不得保留 cursor。`ListResult.Fail` 后的 entries 即使非空也不能用。

每个 FileId 同时最多一份 snapshot；全局、每 volume、每 tree、每句柄均有 configurable 字节／条目上限，并把编码暂存、issued index map、pattern 和 metadata 算进去。捕获前保留总预算，逐 entry 再收费；预算耗尽时释放本次临时内容并明确失败。CLOSE、TREE_DISCONNECT、LOGOFF、session 过期以及 endpoint 停止要沿既有 owner 排空路径释放 snapshot；unknown cleanup 仍计费，不能把它从状态中抹掉。某 volume 饱和时另一 volume 的状态查询与清理可继续。

### 本 PR 范围切分

| 延后能力 | 类型、代价与保持的形状 | 7.5 外部结果 |
|---|---|---|
| 8.1 rename／move／当前名字 | 功能；目录 snapshot 的 raw leaf 与 node ID 不作为 mutation guard，后续按名效果仍须另取完整 ancestry guards。 | 名字修改与当前名字类保持 unsupported。 |
| 8.3 CHANGE_NOTIFY／缓存恢复 | 保证；snapshot 保留 opaque revision 与身份，但普通续页不冒充实时观察；后续通知需重新取得完整状态。 | `CHANGE_NOTIFY` 明确 unsupported，预热目录缓存的一秒可见性尚未宣告。 |
| 增量流式目录分页 | 形状约束；若将来支持，必须保持完整捕获和全量歧义验证的证明，不能边读边报。 | 超大目录达到 cap 时明确失败。 |
| 持久或跨连接目录 cursor | 功能；需要持久 handle 和可恢复状态，超出普通 FileId 生命周期。 | 断线后的旧 FileId／index 无效。 |

## 备选方案

**每个 QUERY_DIRECTORY 页重新向 authority 读取。** 可以降低句柄持有内存，但页间目录变化会导致漏项、重复或同一成功枚举混用 revision；按名歧义也可能落在尚未取到的页。当前 `DirectoryReader` 提供有界完整捕获，故不选。

**先按 Windows pattern 过滤，再验证名字。** 它减少编码工作，但不匹配 pattern 的非法名字仍使目录无法无歧义表示；过滤后成功会把受影响条目隐藏，违反 R-FS-9，因此不选。

**使用 authority 的目录 revision 作为 SMB FileIndex。** Revision 是 opaque 的整目录相等 token，而 FileIndex 需要在某个句柄的某份冻结结果中定位一条已发出 entry；两个含义不同，不能互换。

## 验收标准

- 一次正常枚举的所有页来自同一 `ParentID`／revision，`RESTART_SCANS`／`REOPEN` 完整重取，普通续页不读取新目录；同一 FileId 上并发请求不漏项、不重复推进。`RETURN_SINGLE_ENTRY` 和 `INDEX_SPECIFIED` 的 cursor 行为逐项验证。
- 在最后一个 entry 放入非法 UTF-8、Windows 保留形式、大小写等价项或畸形 Attr 时，整次查询失败且没有任何成功前缀；pattern 不匹配该 entry 也失败。其它入口的数据不被改写，已打开身份 I/O 仍指向原节点。
- 在读取首项、中项、末项与 metadata 时注入 authority 断线、collector 超限、revision 冲突、scope 失效与授权撤销；每次都返回真实错误，不返回空目录或部分 snapshot。
- 以最小固定 buffer、差一字节 buffer、恰好容纳单条／多条、超大名字、32-bit index 逼近耗尽、credits 与 frame 边界验证 encoder；每条输出完整，失败不推进 cursor，旧 generation／别的句柄 index 不可复用。
- 真实 Windows redirector 及 `FindFirstFile`／相关文件 API 覆盖根与非根目录的首次、restart、`.`／`..` pattern 与首条仅固定头部可容纳的缓冲区；观测本投影的无伪项／无 partial-name 行为是否可互操作，失败则在本 PR 内补齐所需行为。
- HTTP、wrapper 和直接 backend 对 bounded capture 的完整性、预算、scope 与 revision 语义有 contract tests；目录 CLOSE 和 tree／session cleanup 没有遗留 snapshot 或未计费资源。本 PR 更新 design 与 implemented note，不以模拟客户端代替最终真实 Windows 验收。

## 风险

完整捕获在大目录上消耗内存和首项等待时间；上限过低会拒绝真实工作负载，上限过高会损害宿主进程，必须在 10 的冷状态与资源测量中确定默认值。Windows redirector 实际使用的查询类、pattern 和 restart 组合须经原生 trace 补齐 allowlist；没有证据的类明确失败。冻结快照只保证一次枚举内部一致，不能自己解决客户端对目录的预热缓存；8.3 必须验证通知、overflow 重取和首次相关操作的一秒可见性。
