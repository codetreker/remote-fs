# Agent Note: SMB 变更通知与 Windows 缓存一致性

Status: proposed

## 问题

远端 authority 的一次成功修改可以由另一台 Windows、Linux 或 SDK 客户端发起，而已连接的 Windows redirector 可能保留文件内容、信息、目录及正负查找结果。SMB 端点即使每次收到请求都回源，也无法纠正根本没有到达端点的缓存命中。普通目录订阅又只覆盖名字，已打开但失去最后名字的对象仍可被旧引用修改。变更流断线、历史裁剪或分页恢复时，静默继续报告旧事实会造成错误成功；简单刷新目录也可能跳过扫描期间的并发改动。

本提案细化 [Windows 网络驱动器总提案](2026-09-16-windows-network-drive-support.md) 的 8.3，依赖[文件数据与信息](2026-09-28-smb-file-data-information.md)、[目录枚举](2026-09-28-smb-directory-enumeration.md)、[名字修改](2026-09-28-smb-guarded-name-mutation.md)与[范围控制](2026-09-28-smb-range-lock-cancel.md)中的对象身份与作用域。[PR 9 原生 WNet 与故障 fixture](2026-09-28-smb-native-wnet-fixture.md)须先交付，8.3 才能在真实 redirector 上验证缓存机制。它实现 `R-CON-1` 至 `R-CON-4`、`R-WIN-3` 与 `R-WIN-8` 的变更传播和恢复机制；能否对每类私有缓存执行充分失效由 8.3 的聚焦原生门验证，最终支持资格仍由 PR 10 判定。

## 提案

新增平台中立的 `ChangeSource` capability。达到完整 R-WIN-8 支持的 export 在 `Publish` 和每次 `TREE_CONNECT` 时必须确认它存在并以可信 `BackendIdentity` 绑定同一 volume ID、authority incarnation 和授权身份；HTTP client、直接 adapter 与每层 wrapper 均传递该证明。缺失或失联时拒绝建立可缓存 tree，不能退化到 TTL 或无通知成功。能力包含：从位置订阅有序变更、查询当前流健康和 retention、取得一致的 scoped snapshot token 与其原子 checkpoint、按页读取 snapshot、从 checkpoint 重放及显式 gap/overflow 错误。cursor 至少由 `(volume, incarnation, position)` 构成；不能拿另一 volume／incarnation 的 cursor 继续。事件交付和 snapshot/page 请求各自执行现有授权；健康检查不是固定周期的权威数据轮询。

生产端在改变权威状态的最终提交顺序中发布事件。名字事件携带原始 parent ID、name、object ID，rename/move 同一 action 的 source/target 关系和序号；内容、长度、属性事件按不可复用 object ID 表达，并包含已 unlink 的 detached 对象。事件 revision 与提交排序一致，不能在成功返回后才由尽力而为的旁路通知补发。每个 authority 写入者在同一提交中记录 journal 事件和持久 wake sequence；提交后共享的 commit-level notifier 向直接 `ChangeSource` 与 HTTP publisher 唤醒各自的 journal reader。订阅者先检查 wake sequence 与 tail，再登记等待，登记后再次检查，防止提交发生在检查与等待之间；唤醒可以合并，但醒后必须从各自 cursor 读至当时 tail。若 notifier 不可达或 sequence 表明漏唤醒，立即 fence 并恢复，不能静默等待下一次写入。HTTP Handler 独有的通知只能覆盖经 HTTP 的写入，不能作为全体 writer 的唤醒源。健康心跳只检查 liveness，不能代替 journal reread。当前 `metastore.Change` 的 `Modified` 只表示某个名字下节点改了内容、大小或属性，没有 field mask，也不能覆盖最后名字已失去的对象。实施时可扩充中立 mutation event；若旧源只有 `Modified`，投影只能保守多报 content/size/attributes，绝不能凭猜测少报。基于 ordered prior-node mirror 推导字段差异也必须证明 mirror 完整、无缺口且跨 detached 对象，否则仍多报或拒绝能力。

新增事件类型时同步迁移现有 HTTP SSE 编解码、replicated backend 的订阅消费者及本地 replica apply；未知事件类型必须显式拒绝并 fence，不能当成普通名字事件跳过。replicated wrapper 取得 source position 后，须等该 position 的全部变化已应用到它所服务的本地视图，或绕过 replica 直接回源并核验同一 revision，才能向 SMB 宣告该 cursor 可读。对 SMB 自身发起的 mutation，权威成功回执附带中立 commit watermark；endpoint 在成功响应前等待 ChangeSource 消费并使全部本地视图与 break/失效动作覆盖该 watermark，或使用等价的直接回源及确认 barrier。此保证不依赖本机碰巧先收到自己发起的事件，满足 R-CON-4；barrier 失败时保留已提交动作回执并报告真实故障。

每个 export 的 coordinator 持有有限的“曾向 redirector 交付可缓存结果、因而它可能缓存”的保守事实集合，按 object ID、目录 ID 与名字键区分内容、信息、正查找、负查找、目录、lease 及 watch scope。对可缓存成功响应，以及可被 redirector 缓存的“不存在”等失败响应，在编码和发送前都预留对应事实及字节额度；即使 frame 只写出一部分、客户端可能已经采纳答案，也保留这份责任，直到能证明连接终止或相应失效完成。不能因为 HTTP/SMB 返回错误就假定负查找未缓存。本地数据缓存即使驱逐，也不意味着 redirector 私有缓存已经失效，因此不能删掉保守记录。只有经协议与原生实验证明的 break／失效或连接终止，才回收对应事实。若容量不足，拒绝新的可缓存成功或负查找响应，或 fence 此 export；不能无记录地交付可缓存结果。一个 export 的 overflow 不污染另一个 volume。

## 订阅与恢复算法

coordinator 状态为 `Seeding → Ready → Fenced → Recovering`。启动时先订阅同一 incarnation 并保存位置 P；随后取得原子绑定的 scoped snapshot token + checkpoint S，并按页读取完整基线。scope 包含所有曾报告事实的目录／对象、全部未完成 watch 的目录或 `WATCH_TREE` 子树，以及 active detached FileId 的对象。分页由固定 token 保持同一权威时刻；每页验证 token、页序号、对象 ID 唯一性、父子关系和名字可表示性。选择固定 replay tail T，应用 `(S,T]` 的全部事件，再追 live stream；核对同一 incarnation、`TrimmedThrough` 未越过已确认游标、retention 明确覆盖回放区间、scope 内 revision 不回退，且失效动作都已完成后进入 Ready。metastore position 可因其它 volume 的提交而出现空号，不能以数字逐一相邻判定缺口；缺口由 source 的 retention／cursor 连续性证明与事件应用确认决定。

如果 backend 无法原子取得 snapshot+checkpoint，必须从 snapshot **之前**的 P 回放全部后续事件，并证明每个先扫描目录在扫描后发生的变化都可重放覆盖；不能扫描完成后才取 checkpoint。恢复期间新事件进入有限 queue；达上限、token 过期、retention 缺口、incarnation 改变、页校验失败或 scope 扩张超限时丢弃这次基线、保持 Fenced 并报告明确故障。连续失败时不无限分配 goroutine 或内存；状态暴露失败原因、最后安全 cursor 和受影响 scope。旧 FileId 在 authority incarnation 更换后持续失效，不能仅靠新 snapshot 重新绑定同名对象。

Ready 中每个事件先增加受影响 scope 的 generation，把旧 generation 的本地答复标记不可用，再执行可证明的 redirector 失效／lease break，最后允许后续应用操作获得成功。每个在途 read/list/负查找在开始时捕获 scope generation，权威观察后、发布 SMB 成功前再次核对；若其间发生失效，丢弃旧结果并按当前 generation 重新观察或明确失败，不能让慢请求把旧成功发布在新事件之后。对象事件按 object ID 命中所有 live FileId，包括 detached；名字事件同时更新 source/target 父链与正负查找事实。stream 失联、心跳超时或任何 gap 立即进入 Fenced。Fenced／Recovering 中所有可能从不完整事实形成的成功答复都失败；能直接作权威冷观察的请求也必须证明其结果与该请求涉及的 redirector 旧缓存已隔离。不能把已知旧字节、旧属性、空目录或先前“不存在”返回为当前成功。

## CHANGE_NOTIFY 与客户端缓存

普通 `CHANGE_NOTIFY` watch 按一个目录 ID 登记；`WATCH_TREE` 按整棵子树登记，不依赖已有 listing 或 lease。coordinator 维护有界 parent graph，利用权威对象 ID 判断事件发生前后是否属于该子树。目录跨子树移动时，旧位置的移出与新位置的移入分别判定；重命名一个目录不能要求逐个子孙立即改写绝对路径。parent graph 缺口或作用域太大时，watch 进入 overflow，而不漏掉后代事件。按 [MS-SMB2 CHANGE_NOTIFY 处理算法](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/1e03994f-ccce-4fc8-a370-4efa610b3e05)，每个目录 FileId 在首次 CHANGE_NOTIFY 注册时固定 filter mask 与 `WATCH_TREE` 设置；同一 FileId 的后续请求忽略新传入的 filter／flag，继续使用首次设置；watch owner 持有 session/tree/FileId generation、cursor、有界 FIFO event queue、FIFO pending request queue 和唯一响应 writer。一个事件按请求登记顺序交付，不能由后来的 pending request 抢先消费；CANCEL 只移除自己的 pending request，关闭退休整个 watch 并沿用异步请求登记机制。复合请求中等待的 notify 不得阻塞同 frame 的后续命令完成，也不得让回应在原签名 frame 生命周期结束后复用借来的字节。

名字事件按 `FILE_NOTIFY_INFORMATION` 在目标 watch 内投影：对要求事件细节的非零缓冲，同目录 rename 的 `OLD_NAME`、`NEW_NAME` 必须连续放入同一个响应；若 FIFO 首个 pending request 的 response buffer 装不下整对，完成该 pending request 并返回 `STATUS_NOTIFY_ENUM_DIR`，不能无限保留该请求或只发送旧名。跨目录 move 在源 watch 发 `REMOVED`、目标 watch 发 `ADDED`。`WATCH_TREE` 的通知名字相对被监视目录编码，而非仅最后一个路径组件。内部 event 保留 action ID 与两个端点的关联，SMB payload 不捏造跨 watch 的配对。内容、大小、属性按 filter 投影；旧 `Modified` 没有 field mask 时对相关 filter 保守多报。每条输出在编码前检查 Windows 名字可表示性；不可表示、截断或队列容量不足时，整项通知明确 overflow，不能交付部分成功。按 [MS-SMB2 CHANGE_NOTIFY 响应规则](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/05869c32-39f0-4726-afc9-671b76ae5ca7)，`OutputBufferLength=0` 是合法的 event-only 请求；事件到达时可以成功完成，响应不包含事件细节，不把零长度当成畸形或永久 pending。需要异步等待时用独立 `STATUS_PENDING` 与最终响应；`CANCEL` 自身不回包，原 watch 依据同一 pending owner 完成或取消。

恢复无法完整重建某 watch 的事件区间时，向原 pending watch 返回 `STATUS_NOTIFY_ENUM_DIR`，清空其不可信队列，并要求后续 watch 先完成权威重观察。`WATCH_TREE` 覆盖子树全部相关名字和对象变化；子树过大可明确 overflow，不能悄悄从新尾部订阅。正常 notify 成功仅表明被报告的事件及其顺序，不替代缓存失效。没有活跃 watch 的目录仍可能被 redirector 缓存，其已报告事实必须参与失效与恢复。

对于 SMB lease／oplock，只对实际授予且与 object ID、session、lease key 匹配的 holder 发 break；等待确认和超时均按协议进入保守 fence，不把“已发送 break”当成客户端已处理。没有授予 lease 时也可能存在 redirector 自有的文件信息、目录或负查找缓存，所以先在真实客户端逐类测量无 lease 情形。健康变更路径需记录从 authority commit 到事件消费、fence、break/notify、首次应用调用完成的最坏时延；静默断流路径需包括健康探测。当前 HTTP SSE keepalive 与客户端静默超时量级为秒十与数十秒，不能直接满足一秒目标；8.3 必须给 stream 单独的有界 liveness 合约，并测量总预算。探测可以按固定节拍确认连接健康，却不能周期性扫描权威数据或依赖缓存 TTL 来取得可见性。

逐类原生证明内容、大小／属性、目录枚举、正查找、负查找和标识缓存：预热后从另一入口修改，记录第一下应用调用是否到达端点、何时完成及结果；然后只切断普通 authority、只切断 change stream，各在健康机制触发前立即调用一次，随后在 fence 后再调用一次。若某一类私有缓存不接收普通用户权限下的失效／拒绝机制，就不能声明整体满足一秒和故障真实性。不能修改系统级 SMB 缓存或安全配置来通过测试。原生证明结果决定实际授予 lease 的 policy；没有证据前不授予缓存能力。

## 计划的目录结构

下列路径为实施时的设计，不表示文件已存在。中立源可由宿主注入；具体 HTTP SSE adapter 与 localstore 生产端需各自证明同一契约。每个新接口都以 volume/object/position 为词汇，不引入 SMB watch flag。

| 路径 | 责任 |
|---|---|
| `packages/storage/change_source.go`（新增） | `ChangeSource`、`ChangeCursor`、`SnapshotToken`、按对象 ID 的中立 mutation event、分页和 gap 错误。 |
| `packages/metastore/metastore.go`、其持久实现（修改） | 同权威顺序的名字／对象事件、原子 snapshot checkpoint、detached 对象 revision、全 writer 共用的 commit wake。 |
| `packages/storage/localstore/change_source.go`（新增） | 直接 backend 的订阅、snapshot、重放与身份绑定。 |
| `packages/transport/httprest/change_source.go`（新增）及现有 `subscribe.go`（修改） | SSE 事件与快照 wire、恢复 cursor、独立健康检测。 |
| `packages/storage/{limited,locked,replicated}/change_source.go`、`packages/storage/replicated/build.go`（新增／修改） | 不丢失 volume/incarnation 证明、event revision、gap 与页面边界；replica apply cursor 追上前不宣告可读。 |
| `packages/smb/change_coordinator.go`（新增） | export 级 `Seeding/Ready/Fenced/Recovering`、基线重放、保守事实账本与诊断。 |
| `packages/smb/change_notify.go`（新增） | watch scope、parent graph、filter 投影、overflow 与 CANCEL 结算。 |
| `packages/smb/cache_fence.go`（新增） | object ID 失效、授予 lease 的 break 与可证明的恢复完成。 |
| `packages/smb/internal/wire/change_notify.go`（新增） | 有界 CHANGE_NOTIFY、OPLOCK_BREAK 编解码和异步响应。 |
| `packages/smb/commands_session.go`、`session_cleanup.go`、配置及状态文件（修改） | 命令派发、watch/lease 所有权、资源额度、停止与查询。 |
| `docs/design/client/smb-endpoint.md`、storage/transport 设计与对应测试（修改／新增） | 同 PR 写入已交付结构、恢复契约和故障测试。 |

## 资源与故障

配置限制每 export 的曾报告事实条目及字节、对象／目录 parent graph、active detached 引用、watch 数、每 watch 队列条目及字节、全局 live event queue、snapshot 并发／页大小／总时限、retention 回放窗口、lease holder 数及 break 等待、stream 静默时限。计数资源与字节资源分别检查 N、N+1、释放后再准入；饱和时保留取消、状态查询和停止清理的额度。snapshot 从页读取，不把整 volume 装入内存。高 churn 下若不能在预算内追上 tail，就持续 Fenced 并明确失败；不得无限扩大 retention 或静默跳过旧事件。per export coordinator 独立，不能因一份 volume 的慢 watcher 阻断另一份 volume 的健康与停止。

## 备选方案

**固定周期扫描或 TTL。** 它可最终发现变化，却让一秒首次观察取决于时钟碰巧到点；在扫描间隙也可能返回旧成功，因此不采用。

**只使用当前 `metastore.Change` 名字日志。** 它已有订阅与 snapshot 基础，但失去最后名字的对象修改没有可报告名字，`Modified` 也没有字段分类；直接投影会漏 active FileId 和某些 filter，因此必须扩充中立对象事件或严格多报并补 detached 来源。

**端点本地 cache entry 驱逐后忘记该事实。** 能降低记录量，却无法证明 Windows redirector 同时丢弃其私有结果；恢复时会漏掉仅存在于客户端的旧事实，因此不采用。

**只发送 CHANGE_NOTIFY，不处理 lease 与其它缓存。** 目录监听者会收到事件，但文件内容、属性和负查找缓存不一定发请求或持有 watch；无法证明 R-WIN-8，因此不采用。

**无界全 volume 快照。** 实现直观，但大 volume 和高 churn 下会占满内存且永远追不上；采用有界 scoped snapshot、原子 checkpoint 和明确 overflow。

## 验收标准

实现 PR 的自动测试应覆盖：生产端名字及 detached 对象 event 在提交顺序中出现，直接与 HTTP 订阅均被所有 authority writer 的 commit wake 唤醒；same-directory rename 的 OLD/NEW、cross-directory 的 REMOVED/ADDED；无 listing 的普通 watch 与 `WATCH_TREE`，深目录与目录跨子树移动；`Modified` 无 field mask 时多报而不漏报；首次 watch 固定 filter/`WATCH_TREE` 并忽略后续变更、FIFO pending、零输出缓冲的 event-only 成功、未授权 snapshot/subscribe；cursor 换 volume／incarnation、retention trim、source 显式 gap、其它 volume 造成的合法 position 跳号、重复事件、token 过期、分页在早扫描目录后并发修改、active detached 对象在扫描后修改；本地事实驱逐但 redirector 可能持有旧正／负答案，负查找失败答复与部分发送前已预留责任；SMB 自身 mutation 的 commit watermark 响应 barrier、replica apply 落后、在途旧 read/list/负查找与新事件竞争；queue、parent graph、page bytes 与保守账本各自恰到／超过上限；CANCEL、tree 关闭、连接断开与通知到达竞态。可控 barrier 使“订阅 → snapshot+checkpoint → 早页后修改 → replay”排序确定，断流恢复后的第一答复必须正确或明确失败。带大于内存预算的真实目录树验证分页完成、增长受控和负载下 liveness；race 测试验证唯一 watch 响应 writer。

8.3 的聚焦原生门使用 PR 9 fixture，在 Windows 11 Home／Pro × x64／ARM64 四个组合分别执行 `WN-09`／`WN-10` 的缓存相关动作：内容、大小、属性、目录、正负查找和标识的预热与跨机器修改；健康时修改确认一秒后发起的第一次相关操作必须返回权威结果，且按规范要求的窗口内调用能在截止前完成。对已预热缓存，分别在 authority 断线但 change stream 尚活、change stream 断线但 authority 尚活、两者均断、静默失联检测前与检测后调用一次；首次调用不能错误成功。记录应用调用、SMB 请求到达、break ACK、权威读取和返回时刻，以证明没有靠多次重试偶然命中。任一组合或缓存类失败即保持 8.3 门关闭并修改架构／规范。PR 10 另行执行完整 `WN-01` 至 `WN-17` 原生资格矩阵；8.3 的聚焦结果不替代全量资格，也不由模拟客户端或协议单测代替。

本 PR 包含中立变更源、snapshot/replay、SMB watch 投影、缓存失效协调和上述聚焦原生证明；复用 PR 9 已交付的 WNet 映射／fixture。最终平台资格报告与额外的 Windows 属性类型分别由其所属后续任务处理。若前序名字工作无法提供稳定 parent/object ID 与 rename 关联，本 PR 必须等其契约齐备，不能从路径字符串逆推身份。

## 风险

最大的未知是 Windows redirector 在普通用户权限、无需全局策略更改的条件下能否按每一类缓存满足一秒成功可见与故障真实性。另一个风险是“曾交付可缓存结果的事实”保守集合可能随长连接和大树持续增长；额度满时必须拒绝新可缓存答复，正常负载是否可接受需要大树实测。新增对象事件必须与权威提交严格同序；如果某存储实现只能提供事后异步通知，它不能声明此 capability。静默失联检测与 lease break 的尾延迟可能吃光一秒预算；只有真实端到端时序能判定方案是否可交付。故障或 overflow 下持续 Fenced 会降低可用性，但保留错误真实性和后续安全恢复可能性。
