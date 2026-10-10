# Agent Note: SMB 范围锁与异步取消

Status: proposed

## 问题

Windows 文件句柄可以申请共享或排他的字节范围锁，也可以在等待期间取消请求。授权、实际范围 I/O、取消、关闭和响应丢失可能同时发生；只在 SMB 端点维护一张锁表，会让 Linux、SDK 和另一台 Windows 客户端绕过保护。把取消当成“未授予”又会让已经授予的锁失去本地 owner。一个 SMB LOCK 批次还可能先成功解除旧锁、再在后续获取时失败，不能把整个批次误报为原样未执行。

本提案依赖 [Windows 网络驱动器总提案](2026-09-16-windows-network-drive-support.md)、[CREATE/CLOSE 句柄](../../implemented/feature/2026-09-28-smb-bounded-create-close.md)和[名字修改](2026-09-28-smb-guarded-name-mutation.md)的对象身份、authority incarnation、可恢复动作和句柄退休原则，以及 `R-CC-14`、`R-WIN-7`、`R-FS-8`。本 PR 交付 LOCK、UNLOCK、异步 pending 与 CANCEL，以及同一对象上跨入口强制保护。名字操作的共享模式准入由前序名字修改工作提供；此处连接权威范围排序与已存在的共享 claim，不重新设计 CREATE。

## 提案

`packages/storage.RangeControl` 已有 `Apply`、`Query`、`Cancel`、`Drop`，`RangeCommand` 已有 `DomainEnforced`、`AddExact`、`RemoveExact`、`RangePolicy`，回执已有 `FailedAt`、`Effects` 与 `HistoryRemaining`。SMB 层复用这些中立词汇，扩展其权威实现及 HTTP/wrapper 贯通性；不在中立接口加入 SMB `FileId`、`AsyncId` 或 Windows 错误码。每个被授权的文件打开引用取得自己的 `UseOwner`，其 `UseScope` 固定为已验证的对象 ID、引用世代和 authority incarnation。不同 SMB `FileId` 即使指向同一对象也不共用 owner；引用终止后 owner 不得被新引用复用。

`LOCK` 解析器先验证整个 frame 的长度、element 边界和批次数量；结构畸形在效果前失败。第一个 element 决定请求是获取还是解除；后续相反类型在该 element 以 `STATUS_INVALID_PARAMETER` 停止；此前成功的获取或解除都保留。可表示的 element 按 wire 顺序构造中立命令，语义冲突或无此锁由 authority 逐项报告 `FailedAt`，此前成功解除不能因后项失败被提前验证抹掉。共享申请使用 `RangeShared`、`DenySelf=WriteData`、`DenyOthers=WriteData`；排他申请使用 `RangeExclusive` 与 `DenyOthers=ReadData|WriteData`。每项获取使用 `AddExact`；其 `ClaimID` 由唯一 `LockRequestID` 和 element index 派生。每个 claim 同时记录同一 Open 的 `UseOwner`、该 Open 持有期间稳定的内部中立 `OwnerKey`、准确 offset／length、模式和在权威范围列表中的相对次序。SMB LOCK／UNLOCK wire 携带 `FileId`、`Offset`、`Length` 和 flags，不携带 `LockKey` 或 `ClaimID`；端点由已验证的 `FileId` 定位 Open，再取该 Open 在建立时分配的 `OwnerKey`，用它提交 `RemoveMatching` 意图，不能用本地候选数猜测目标。

[MS-FSA §2.1.5.9](https://winprotocoldocs-bhdugrdyduf5h2e4.b02.azurefd.net/MS-FSA/%5bMS-FSA%5d.pdf) 的匹配域是同一 Open、LockKey、offset 和 length。在 authority 对该对象的最终排序内，`OrderedRangeControl` 遍历自己的 `ByteRangeLockList` 投影：每遇到匹配项先记为候选；遇到第一个匹配的 exclusive 立即选它；遍历结束仍无 exclusive 时选最后一个匹配的 shared。一次 UNLOCK 只解除这个 ClaimID，未命中返回范围未锁定。列表是权威 coordinator 显式维护的有界顺序：`AddExact` 在同一对象串行化点把 claim 插入尾部，`RemoveExact` 删除该项而保持其它项次序；重放不插入，不能由时间戳、`ClaimID` 大小或 endpoint 收包顺序重建遍历。`RemoveMatching` 只供有序动作使用；coordinator 在同一个串行化步骤中选中 ClaimID、把具体 `RemoveExact` 固化在该动作 ledger，再执行解除。回执的 `Commands` 保留原始 `RemoveMatching` 以供重投逐字比较，`Effects` 携带所选 `RemoveExact` 与 ClaimID。响应丢失后按原动作 ID 查询或重投，始终使用已固化的 ClaimID，不能重新扫描而误解下一把同区间锁。

现有 `advisory.Session.Apply` 在动作登记前对整批执行 `checkCommands`，遇到后项非法会让前项解除完全不生效。为此新增中立 `OrderedRangeControl.ApplyOrdered`、`RemoveMatching` 和 `OrderedBatchRule`（按首项 edit class 停止），沿用 `RangeControl.Query/Cancel/Drop` 的同一 request ID 和回执 ledger：它只预验证帧级界限与不可恢复的身份字段，在一个持久 request 下逐项验证及执行；违反 `OrderedBatchRule` 的首个 index 或其它逐项失败记为 `FailedAt`。只有立即冲突的获取失败触发本批先前获取回滚；后项类型非法、参数错误或解除失败保留已成功的前缀。`Effects` 精确记录最终存活的获取与解除；只有 `Rejection=RangeBlocked`、失败项不等待且 `FailedAt` 指向获取时，才允许前缀获取因冲突回滚，重投只读同一结算。请求意图的原始有序命令、首项类型及每个已选择的 `RemoveExact` ClaimID 写入 ledger，重投不能改变。旧 `Apply` 的预验证和非 SMB 调用语义保持原状；SMB 发布时必须核实 adapter 链提供这个可选能力。`Query` 对两种 action 均返回同一 `RangeAttempt` 形状，receipt 过期仍遵循 Unknown 规则。

所有已接纳的范围 I/O、锁授予／解除和引用关闭进入 authority 对同一对象的最终排序。获取的冲突核对与授予在此排序点完成；授予之前已经接纳的 I/O 可以完成，授予之后才接纳的冲突 I/O 被拒绝。`DomainEnforced` 约束所有入口；Linux advisory `DomainRecord`、强 S/X `DomainWholeFile` 保留各自语义，不能用一种替代另一种。名字操作和共享模式 claim 在相同对象与作用域内排序，避免 rename／replace 或改入口绕开保护。`GetConflict` 仅供诊断／优化，不能作为效果前最后一道授权判断。

## 所有权与状态

每个 `LOCK` 有 endpoint 生成并在提交前登记的 `LockRequestID`，绑定 `(volume, authority incarnation, SMB session generation, tree generation, FileId generation, UseOwner, 有序命令字节语义)`。请求状态是 `Registered → Applying → Pending → Settled`，或 `Registered/Applying/Pending → Reconciling → Settled/Unknown`；`Unknown` 保留 owner、request ID、可验证 intent 与额度，只允许查询／原 ID 重投。`RangeAttempt` 的 `Pending` 不是授予；`Granted`、`Rejected`、`Cancelled`、`Released` 的精确 `Effects` 与 `Claims` 决定本地 claim ledger。`EverGranted` 只记录历史上是否发生过授予，不等于当前仍有 claim；被回滚的获取可以使它为 true 而 `Claims` 为空。端点在发布任何响应前先把回执与 owner 状态协调好，写响应失败不撤销已经确认的权威效果。

端点在调用 `ApplyOrdered` 前登记 pending owner，并用与网络请求 context 分离、受 session/cleanup 生命周期约束的 context 启动至多一个有界 worker。短同步等待内未完成时，唯一 response writer 发送 `STATUS_PENDING`，生成在该连接内唯一、并与原 `MessageId`、session、tree、FileId 绑定的 `AsyncId`，把 pending record 保持在连接和 session 的有界登记表。`ApplyOrdered` 没有结果回调：worker 完成信号唤醒结算器；若返回 Pending、网络响应丢失或取消与授予交错，结算器以有界退避 `Query` 原 ID，并在 session 退休／deadline 时调用 `Cancel`，直到终态或明确 Unknown。不能在原请求 context 取消时杀掉唯一查询 owner。最终响应使用同一 `AsyncId`。中间与最终响应各自独立签名；请求 credit charge 只归原请求，response credit grant 按 SMB2 的中间／最终响应规则记账且不得重复授予。复合 frame 中的等待 LOCK 先把已验证的请求字段复制到 owner，再让后续命令按 compound 顺序完成；异步完成不能借用原 frame 缓冲区。异步响应的生成、签名、credit accounting 与重复完成只归该 record 一个 writer 所有；连接断开会停止写 frame，但不会抹去 authority request owner。完成后从 pending 表移走前，先完成权威结算与 claim ledger 转移。

`CANCEL` 先按签名和会话验证原报文，再以异步请求的 `AsyncId`，或同步请求的 `MessageId`，在同一 session 的 pending 表定位目标；generation 不匹配、目标已结算或不存在不影响别的请求。CANCEL 自身没有响应；它只标记目标的取消意图，并以原 `UseOwner`／`LockRequestID` 调用 `RangeControl.Cancel`。原 `LOCK` 的 goroutine 与 CANCEL 不得各自作出独立终态，二者通过同一 pending record 的串行结算器合并 `Apply/Cancel/Query` 结果。取消和授予竞争时，先按原回执的 `State/Rejection/FailedAt/Effects/Claims` 结算目标，而非凭 `EverGranted` 或“有 live claim”选择状态。`Granted` 且句柄仍 live 时取消失败，保留 claim，向原 LOCK 发送成功；`Rejected` 且保留部分已获取 claim 时，原 LOCK 仍返回其 `STATUS_INVALID_PARAMETER` 等真实失败，并保留这些 claim；只有终态为 `Cancelled` 且确认没有残余授予才能给原 LOCK 发送取消结果。若句柄／连接已死亡，则由其 cleanup owner 另行排空并释放已授予的 claim。结果仍未知时原 LOCK 以 I/O 错误结束，保留隔离的 owner，不把 transport context 的取消当成权威取消证据。

## 批次、释放与恢复

一次 SMB LOCK 请求作为一个有序 `ApplyOrdered`，不拆成多个不相关的动作 ID。成功获取在失败 element 前进入持久前缀；只有不等待的冲突获取失败才回滚本批先前新增的 claim。获取批次后遇到 UNLOCK 类型、参数错误或其它非冲突失败时，已成功获取的 claim 保留；解除批次后遇到 LOCK 类型或后续解除失败时，已成功 `RemoveExact` 仍有效。权威回执的 `FailedAt` 定位失败 element，`Effects` 只列仍然生效的获取及解除，`Claims` 列出当前仍有效的本动作获取 claim；`Rejected` 可以带 `EverGranted=true`、非空 `Claims` 和获取 `Effects`。`EverGranted` 不独立决定 SMB 结果。端点逐项校验回执中的 owner、request、element、claim 与结果上限，再更新 ledger；回执格式不一致视为结果未知并隔离受影响 owner，不从本地旧状态推算。协议错误映射只使用已确认的 `RejectionCode`：冲突、资源满、无此锁、参数非法、等待死锁与过期各有确定响应；无法区分则 I/O 失败，不返回成功。

HTTP 的 `validateFileAttempt` 当前拒绝 `Rejected && EverGranted`，`validateRangeEffects` 当前只允许 rejected 回执含解除 `Effects`。实施时两处都按中立 `OrderedRangeControl` 回执规则修订：对 `FailedAt` 以前的每项，核对有序 `Effects`、派生的 `ClaimID` 和 `Claims`；冲突回滚只允许移除本动作先前获取，不能移除既有 claim；失败项及之后不能出现新效果。`RemoveMatching` 仅在 `ApplyOrdered` 的原始意图中合法；回执必须含同序选中的 `RemoveExact` ClaimID，HTTP 校验 `Commands` 与原始请求一致、效果的 `OwnerKey/offset/length` 与命令相同、所选 ID 有效且不能被重投改写。权威列表中的第一个 exclusive／最后 shared 只有 authority 能核对，HTTP 不根据本地时间推断选择。`Rejected` 的 `EverGranted` 可为 true；HTTP 能核对的约束是非空 `Claims` 或获取 `Effects` 必须有 `EverGranted=true`，无存活效果但 `EverGranted=true` 仅允许上述冲突回滚形状，不能把该位用作 live claim 判断。`rangeResponseBound` 要覆盖最大长度 `Rejected` 回执同时含失败位置、全部前缀获取 `Effects`、全部存活 `Claims` 和错误 envelope，服务端／客户端同一上限；`file_range_receipt_test.go` 与 HTTP round-trip 测试覆盖获取→UNLOCK 类型错误、部分存活获取、冲突回滚及篡改回执拒绝。若失败项本身的命令字段违反 `RangeCommand.Check`，HTTP 只允许它作为与原请求逐字一致、位于 `FailedAt` 的未执行项；其它命令与所有效果仍须完整校验，不能放宽整份回执。

`Query` 和 `Cancel` 必须根据原 owner、request ID 查询原逻辑动作；相同 ID 配另一份命令拒绝。回执保留窗口至少覆盖 endpoint 允许的查询／重试期。窗口届满且仍无可确认结果时，旧 `FileId` 持续失败并保持诊断中的 Unknown；不得新建 lock request 来“补做”猜测中的动作。authority incarnation 改变时旧引用和所有 claim 均失效；端点不可按路径重新取得锁。`Drop(owner, DomainEnforced)` 只用于本 owner 被确定退休、已接纳 I/O 已排空后的兜底清理；其结果未知时继续保留 cleanup owner，不把本地 ledger 清空。CLOSE、tree 退出、LOGOFF、TCP 断开、unpublish 与 stop 均走同一退休器，先封住新准入，再排空该引用的已接纳 I/O，最后核实 claim／引用释放；一个 claim 失败不跳过其它依法需要执行的清理。

共享模式 claim 与范围 claim 在 authority 均按原对象身份持有。跨同 volume 的 Linux/SDK 写、读取、rename、replace，以及另一 SMB tree 的访问，必须走相同最终保护判断；已 unlink 的 detached 对象仍受其原引用的 claim 约束。同名新对象有新身份，不能继承旧 claim。若 adapter 不能保证跨入口效果排序，发布具备 LOCK 的 share 时拒绝其能力，不能降级到端点本地锁表。

## 计划的目录结构

下列路径是实施时的文件布局，不表示它们现在已经存在。中立类型继续由 `packages/storage/ranges.go` 与 `packages/storage/capabilities.go` 拥有；需要补强语义时同步更新权威实现与 wrapper，不建 Windows 专用远端 API。

| 路径 | 责任 |
|---|---|
| `packages/smb/internal/wire/lock.go`（新增） | 有界 LOCK/CANCEL 请求解码和响应编码；原始签名字节保持不变。 |
| `packages/smb/range_locks.go`（新增） | SMB `FileId` → Open → 内部 `OwnerKey`，SMB element → `RangeCommand`、已确认 claim ledger、结果映射与同 owner 结算；UNLOCK 发权威 `RemoveMatching`。 |
| `packages/smb/pending_requests.go`（新增） | `(session generation, MessageId/AsyncId)` 索引、唯一响应 writer、取消与授予竞态；处理独立异步 frame 的签名和 credit 生命周期。 |
| `packages/smb/commands_session.go`、`connection.go`、`session_cleanup.go`（修改） | 命令派发、async response framing、断线和退休时的请求排空。 |
| `packages/storage/ranges.go`、`packages/storage/capabilities.go`（修改） | 中立 `OwnerKey`、仅有序动作接受的 `RemoveMatching`、回执校验和跨入口排序契约；不接收 SMB 类型。 |
| `packages/advisory/actions.go`、`edits.go`、`reconciliation.go`、`waiting.go`、`ranges.go`（修改） | 新 `ApplyOrdered` 的逐项停止、对象级有序 claim 列表、`RemoveMatching` 选中并固化 ClaimID、持久 `FailedAt/Effects`、重投／查询及 pending 唤醒；旧 `Apply` 不变。 |
| `packages/storage/objectstore/file_locks.go` 与其底层 coordinator（修改） | 授予、I/O、关闭及批次效果在同一权威对象顺序提交。 |
| `packages/storage/{limited,locked,replicated}/capabilities.go`、`packages/transport/httprest/file_json.go`、`file_range_receipt.go`、`file_range_receipt_test.go`（修改） | capability 转发、原 ID 重投与回执保真；HTTP 校验允许 `Rejected` 携带历史授予和存活获取，按 `FailedAt/Effects/Claims` 检查前缀、回滚及响应字节上界。 |
| `docs/design/client/smb-endpoint.md`、适用的 server/storage design、对应 `*_test.go`（修改／新增） | 随代码记录已落地结构与契约、协议和跨入口回归。 |

## 资源上限

配置分别限制每 session／tree 的 pending LOCK 数、每引用 claim 数及其固定 `OwnerKey` 存储、每对象权威列表的 claim 条目／字节、全 export 的 range owner 与 claim 数、单请求 element 数、排队等待数、回执字节与保留时间、取消结算时限。`RemoveMatching` 扫描只在已计费的对象列表上执行，工作量受每对象上限约束；固化的 selected ClaimID 与原始 intent 同受 receipt 字节及保留期约束。所有登记在调用 authority 前预留容量；超限在没有效果时返回确定资源错误。额度在 authority 确认释放、取消或无效果后回收；Unknown 持续计费。队列饱和不能阻止状态查询、CANCEL、CLOSE 或 stop 的保留清理通道。默认额度和超时需在真实负载测量后定值，不能靠无界 goroutine 或无限 receipt 维持正确性。

## 备选方案

**端点本地锁表。** 实现成本低，但 Linux、SDK 与其它 authority 入口可绕过它，无法满足跨入口保护，因此不采用。

**按坐标解除任意匹配锁，或在端点要求候选唯一。** 前者可能选错同一 Open/key 内的 claim；后者会拒绝合法的重复 shared 与 exclusive+shared 锁。两者都无法按权威列表遍历选择指定的一项，因此不采用。

**收到 CANCEL 直接返回取消成功。** 无需查询 authority，但授予可能已经越过最终排序点；本地删除 pending record 会留下无主锁，因此不采用。

**把批次拆成多个独立 `Apply`。** 有利于逐项错误报告，但在响应丢失后无法以一个稳定 ID 核对批次 surviving effects，且改变 Windows 请求可观察的顺序与回滚范围，因此不采用。

## 验收标准

实施 PR 的确定性测试至少覆盖：共享／排他重叠与边界、`uint64` 溢出、同一引用多 claim、跨引用同区间、Linux/SDK/另一 tree 的读写与名字保护；同一 Open/内部 key/范围两个 shared 按列表最后 shared 先解除、exclusive+shared 先解除列表中第一个 exclusive、不同 Open 永不互选、中立层的错误 key 不得命中、每次 UNLOCK 只解除一项；跨对象同名替换与 detached 旧引用；首项决定获取／解除类型、相反类型后项的 `STATUS_INVALID_PARAMETER` 与两种方向的先前 surviving `Effects`，以及仅不等待冲突获取才回滚先前获取；重复 request ID 与改变 intent；授权失败和容量满无副作用；pending → `STATUS_PENDING` → 最终响应与签名；CANCEL 按同步 `MessageId` 和异步 `AsyncId` 定位、跨 session 错误 ID 不命中；取消先于、同时、晚于授予，尤其 CANCEL 与 `Rejected` 但存活部分 claim 的批次竞争时原 LOCK 仍报告原失败并保留 claim；UNLOCK 在选中后响应丢失，同 ID 重投仍解除原 ClaimID 且下一次新 UNLOCK 才选择剩余项；每次解除后用 Linux/SDK/另一 tree 验证剩余 claim 仍阻挡冲突；查询失败、receipt 过期、authority 换代；CLOSE／断线／stop 与在途 I/O、清理失败交错。对权威测试使用可控 barrier 确定排序，race 测试验证 pending record 只有一个 final writer。HTTP 与直接 adapter 执行同一契约测试；真实 Windows 的共享／范围矩阵和取消行为进入最终原生验收。

完成标准是：每个已报告结果都有权威回执；冲突跨入口在效果前拒绝；批次 `FailedAt` 与 surviving effects 精确；取消后没有无 owner 的授予；引用清理后无遗留 claim；未知结果可以按原 ID 追踪而不会被重做。此 PR 不包含变更通知、Windows 缓存失效或 WNet 映射；这些能力由独立提案拥有。本 PR 只为后续通知保留按对象 ID 观察锁相关状态的形状，不在锁结果上伪造通知。

## 风险

最大风险是异步 SMB credit／签名生命周期与 authority 长等待相互交错；需要由独立 pending record 严格拥有最终 frame，并用协议客户端构造同步和异步 CANCEL。另一风险是第三方 FileStorage 声称 `RangeControl` 却不能跨入口保护名字效果；能力检查必须验证具体 adapter 的语义，静态接口断言不足以宣称合规。长时间 Unknown 会占用有限额度，这是故意保留的清理责任；当容量耗尽时应拒绝新工作并暴露状态，不能靠过期猜测释放。
