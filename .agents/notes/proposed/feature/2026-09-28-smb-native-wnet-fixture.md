# Agent Note: Windows 原生映射与可注入验收夹具

Status: proposed

## 问题

SMB 端点能接受合成请求，并不意味着普通 Windows 程序能通过系统网络驱动器使用它。WNet 连接由当前登录会话发起，系统 redirector 选择端口、目标名和认证上下文；本地 445 可能已被占用。进程 token 的登录 LUID 也未必等于 SSPI 在入站网络连接上报告的 LUID。没有实机证据，不能把普通用户可达性和“同一登录会话”当成已交付能力。

驱动器移除还跨越系统映射、SMB 端点与远端文件会话。WNet 调用失败时，原映射可能仍在，也可能已经移除而清理未完成；只按返回码释放本地 owner 会使占用句柄、锁和删除义务失去归属。现有模拟测试无法确定系统调用和远端提交的先后顺序，无法形成可复现的 Windows 验收证据。

## 提案

本任务交付可嵌入的 Windows 映射控制器，以及共用真实 [`cmd/remote-fs-server`](../../../../cmd/remote-fs-server/main.go) 和持久 volume 的原生故障夹具。映射控制器只管理宿主显式请求的一份映射与其对应 SMB export；`packages/smb` 保持协议、FileId 与远端清理 owner。使用方提供 listener、可信 share、backend、认证和授权策略；包初始化、构造 controller 与导入 Windows 包都没有 WNet 或系统设置副作用。本提案细化[完整 Windows 入口提案](2026-09-16-windows-network-drive-support.md)中的原生发布与 WN-01／WN-11／WN-12，现有[安全有界端点决定](../../implemented/architecture/2026-09-21-secure-bounded-smb-endpoint.md)继续拥有 SMB 会话与身份规则。

任务内含普通用户 445 探测、WNet 映射和非强制移除、同会话身份验证、真实 HTTP backing 的故障注入、Windows Home／Pro × x64／ARM64 夹具可执行性。任务不实现 SMB 文件命令、变更缓存策略或最终 WN-01–17 全矩阵判定；后者由[原生资格提案](2026-09-28-smb-native-qualification.md)拥有。控制器必须保留后续资格测试能使用的精确事实和失败状态，不把一个未证明的平台行为编码为成功假设。

### 目录与依赖

以下是目标源码布局；“新建／修改”是本任务未来实现 PR 的文件责任，不表示这些文件已存在。

| 路径 | 动作 | 责任 |
|---|---|---|
| `packages/smb/windows/mapping.go` | 新建 | `MappingController`、公开输入／结果／状态；无全局实例。 |
| `packages/smb/windows/mapping_windows.go` | 新建 | WNet wide-char 调用、目标映射身份查询、普通用户平台预检。 |
| `packages/smb/windows/mapping_other.go` | 新建 | 非 Windows 的明确 unsupported 实现，保证跨平台构建。 |
| `packages/smb/windows/mapping_owner_windows.go` | 新建 | 宿主指定目录内的映射意图、原子持久更新、恢复扫描；持久化失败阻止 WNet 副作用。 |
| `packages/smb/windows/session_binding_windows.go` | 新建 | 将宿主 token、SSPI 身份与实测登录会话关系交给准入策略；仅接受已证明的关联。 |
| `packages/smb/mapping_admission.go` | 新建 | export 级 admission fence、在同一锁域内的 busy 快照及解除；不调用 WNet。 |
| `packages/smb/server.go`, `packages/smb/config.go`, `packages/smb/status.go` | 修改 | 向显式宿主暴露有界映射所需的发布、fence、停止与 cleanup 查询；不替宿主持有 WNet。 |
| `packages/smb/windows/mapping_windows_test.go`, `packages/smb/mapping_admission_test.go` | 新建 | WNet 适配器故障、busy 竞争、身份拒绝、cleanup owner 状态机。 |
| `internal/integration/windowsnative/fixture.go`, `barrier.go` | 新建 | 真实 server／persistent volume 启停、事件 barrier 和源绑定；不进入库 API。 |
| `internal/integration/windowsnative/fault_smb.go`, `fault_http.go`, `fault_change.go` | 新建 | 三条独立代理路径、定点注入与双 socket／receipt 证据。 |
| `internal/integration/windowsnative/corrupt_offline.go`, `evidence.go` | 新建 | WN-13 停机后持久破坏、前后 digest、schema 事件与净化。 |
| `internal/integration/windowsnative/testdata/` | 新建 | 版本化故障脚本与有限 probe corpus；无凭据、真实用户路径或测试结果。 |
| `docs/design/client/smb-endpoint.md`, `docs/testing.md` | 修改 | 只同步实际落地的映射边界和夹具运行方式。 |

`packages/smb/windows` 依赖 `packages/smb`，只装 Windows 系统调用与身份验证；`packages/smb` 不导入 `windows`，避免反向依赖。`internal/integration/windowsnative` 只依赖公开 API、真实二进制与独立故障代理；不通过访问私有表项制造“通过”。fixture 不重写 `remote-fs-server` 的存储路径，也不另造内存 authority。

### 公开控制面与状态机

控制器的概念 API 为 `NewMappingController(server, WNetAdapter, MappingOptions)`、`Connect(ctx, MappingRequest) (MappingResult, error)`、`Unmap(ctx, MappingID) (UnmapResult, error)`、`Stop(ctx) (StopResult, error)`、`Status(ctx) MappingStatus`。`MappingOptions` 必须包含宿主提供的 durable owner store；缺少或无法同步持久化时拒绝 Connect。`WNetAdapter` 仅在平台实现和受控测试替身之间注入，宿主不能用它绕过 SMB 身份检查。`MappingRequest` 包含稳定的 host 选择的 `ExportID`、盘符／目标 UNC、`Share.Volume`、期望的 owner SID／登录会话、listener identity 和可查询的意图 ID；不携带密码到日志或持久证据。`MappingID` 包含本次宿主控制器实例与 WNet 映射目标的不可复用 nonce；盘符本身不是身份。`MappingResult` 明确返回映射是否由本控制器创建、选定 UNC、endpoint socket 证据和 owner；未知结果不会包装为成功。

每份映射的互斥状态为 `Prepared → Connecting → Connected → Fenced → RemovalUnknown | RemovedCleanupPending → Stopped`。**调用 WNet 连接之前**先将 `Prepared`、本次 connect 意图及 `Connecting` owner 原子写入宿主指定的 durable store，刷到持久介质并验证成功；仅在此前置条件成立后产生系统映射副作用。明确无效果的连接可回到 `Prepared`，但也须先持久化结算；连接结果不确定时保留 `Connecting` owner，查询目标与本端点握手事实，不能用新意图盲目重连。成功连接、fence、移除意图、WNet 结算与 cleanup 责任也先写临时记录、flush、原子替换并刷持久目录元数据，再向调用方报告可见状态；平台无法证明写入持久性则保持 Unknown 且不能继续新副作用。`Connected` 中系统映射身份、export owner 和本机 LUID 绑定不可变；若同一盘符被其它进程复用，不能认作原映射。`Status` 逐份返回状态、目标身份、已接纳操作数、FileId／lock 数、cleanup owner 数、最后一次 WNet 结果与结算来源。状态查询在资源饱和或 stopping 时仍可用。

`Unmap` 先通过 `packages/smb` 在一个同步点设置临时 admission fence，并快照该映射的打开 FileId、锁及 active／in-flight 操作。任何计数非零都返回 busy、原子撤回 fence，继续允许旧映射 I/O；即使操作随后完成也不能更改该次 busy 判定。快照为零才在不持有 SMB registry 锁时以**本地盘符设备名**调用非强制 `WNetCancelConnection2W`；远端 UNC 是无盘符连接的取消目标，不能用于取消本 drive mapping。fence 期间拒绝新 tree／open，允许已接纳 I/O、CLOSE、TREE_DISCONNECT、LOGOFF 和清理收束。不得改用强制移除以得到成功。

WNet 结算恰有三种：① **可证明无效果且原映射实例未变**，撤回 fence、保留原 owner 与操作能力；② **可证明原映射实例已移除**，返回 `MappingRemoved=true`，进入永久 stopping，并由 `Stop`／`Status` 报告独立的 `CleanupPending`；③ **实例归属仍不确定**，返回 Unknown、保留 fence、映射 owner 和原移除意图，继续按同一意图查询。`WNetCancelConnection2W` 对 drive mapping 按本地设备名操作，没有映射 generation/CAS；`WNetGetConnectionW` 只能给出目标 UNC，也不能证明同盘符映射实例未变。每次映射使用不可复用、只指向本端点的唯一 share/UNC，作为诊断与 source-binding 线索，但它不使盘符取消具备比较并交换语义，也不能消除同盘符同 UNC 的移除—重建 ABA。取消前后同时收集 WNet 返回、系统目标查询和本端点已认证 session／tree 生命周期；只有原生测试证明这些事实足以把原 owner 与系统效果唯一关联时才认定①或②，否则保持 Unknown/fence，不从字符串相等推断结算。盘符须由本控制器独占管理；若外部程序仍可并发重用该设备名而不能防止错取消，发布门失败。

**调用 WNet 非强制移除之前**持久化 `Fenced`、cancel 意图、唯一 UNC、owner SID／LUID、原连接证据和 cleanup owner 并完成 flush；超时与进程崩溃都保留这条记录。重启恢复先从同一 durable store 读取并校验完整性，重新取得当前进程 token SID／LUID，只有精确匹配原 owner 才允许查询或继续结算；其它登录会话不能接管。随后重建 endpoint admission fence、核对系统目标、本端点 socket/session 与持久意图，在证明原实例无效果或已移除前不复用盘符、不删除 owner、不调用新取消意图。记录由宿主选择项目部署目录并限制当前用户访问；不放在用户 home 的隐藏目录或 AppData。恢复证据缺失、记录损坏或查询本身不可靠时返回 Unknown 并保留 fence，不能清除未知映射。WNet 返回码、盘符与 UNC 字符串任何一项单独都不构成成功证据。

`Stop` 永久拒绝新工作，保留每一份未退休的 FileId、tree、FileSession、锁与 delete owner，按端点已有清理结果逐项重试；`Released=true` 只退休已释放的引用，仍 pending／unknown 的 barrier 与删除义务保留原动作。只有映射确认消失、listener 与全部本端点 owner 已结算，`Stop` 才返回完全成功。外部 `Share.Backend` 和 HTTP client 的关闭由宿主执行，且须在端点清理之后；清理错误作为可查询 owner 留存。映射状态机使用固定上限和逐次释放，不能因重试累积无限错误链或未受限日志。

### 原生连接与身份门

连接顺序是：验证目标 OS／架构与普通用户身份；尝试独占绑定候选 loopback IPv4／IPv6 445；确认 accepted socket 来自本端点而非 LanmanServer；发布唯一 share；调用 WNet 连接；从本端点 trace 核对 dialect 3.1.1、签名协商、SSPI context、TREE_CONNECT 与稳定的 volume identity。WNet 无自选端口字段，所以不得以可连接高端口的协议客户端代替本项。若 445 被占用或需要提权，明确失败并保存证据；不关停系统服务、不修改 firewall、SMB client、安全或缓存的全局设置。host alias 只能在证明普通用户可创建、只指向 loopback 且证据可核对时使用。

`CurrentIdentity` 返回宿主 token 的 SID／AuthenticationId LUID；入站 `Authenticator` 在 `AcceptSecurityContext` 后查询安全上下文 token 的 SID、`TokenStatistics.AuthenticationId`、`TOKEN_ORIGIN` 和可用的 LSA 登录会话信息，记录 API 成功／失败及被验证的字段。原生 gate 先用映射创建者的同一 interactive logon 调用 WNet，捕获入站 token 并比较 SID／LUID；再用同 SID 的独立登录会话、另一 SID、guest、匿名各尝试连接，验证必须拒绝。若入站 AuthenticationId 与创建者一致，按精确 SID＋LUID 准入；若它是新建 network-logon LUID，则仅在找到**Windows 官方支持、普通用户可调用、不可由对方伪造的 API 证据**，能够把该入站安全上下文唯一追溯到创建者 interactive LUID，并在上述负控中保持隔离时，才定义该映射的关联规则。`TOKEN_ORIGIN` 为零、LSA 只有 network-logon 记录或 SID 相同均不证明父登录会话；证据字段为空或无法建立单射时保持拒绝。候选 API、OS build、token 类型、原始返回码、正负控制结果与确认的关联边一起归档；没有这条证据链的 cell 失败，不以 SID-only、loopback、进程名、UNC 或 WNet 成功放行。签名被篡改与 session 失效也在 authority 文件操作前拒绝。本机 SID 不变成远端业务凭据。

### 夹具拓扑、故障接口与证据

每个 OS cell 由隔离的 Windows 普通用户会话运行本地 endpoint 与 native probe。测试控制器在独立进程启动**实际** `cmd/remote-fs-server`，为它提供测试私有且可重启的持久 SQLite metadata 与对象 volume；Linux／SDK 第二入口访问同一 volume，不直接改底层文件。服务重启必须复用原数据目录以检验 durable owner、对象身份和回执。`remote-fs-server` 的二进制 digest、服务配置摘要、volume ID／incarnation、各入口身份都记录为证据元数据；密钥和文件内容仅记录不可逆 digest／长度。

三条可分别控制的路径为：Windows 本地 SMB frame、endpoint→HTTP authority 请求／响应、authority→endpoint 变更通道。**直连 445 基线**由本端点直接占用 loopback 445 并记录它实际接受的 socket；**代理篡改运行**由代理占用客户端所见 445，再转发给本端点受控 loopback listener，记录客户端→代理和代理→端点两端 socket、nonce、frame digest 与关联 ID。篡改运行不能充当端点直接占用 445 的证明。SMB 代理能够在签名后按指定 frame 序号篡改或中断；HTTP 代理可在请求进入 authority 前、authority 确认提交后而响应返回前、响应部分写出后定点断开；变更代理可独立暂停、丢帧、制造 cursor gap／overflow，不影响普通 HTTP 访问。故障脚本使用 `caseID, stepID, path, boundary, occurrence, barrierID, action`，运行时为每个 arm 点记录 request/action ID、endpoint connection incarnation、authority commit／receipt 证据。barrier 基于可观察事件释放，不靠固定睡眠猜测提交点。各代理必须 fail closed：未命中预期注入点即标记 case setup failure，不能把未注入的成功运行算成故障通过。

WN-13 的持久损坏使用**离线** injector：先经 barrier 停止新请求、确认 `remote-fs-server` 已退出且 SQLite／对象 store 句柄关闭；对测试私有的 metadata 文件、状态账和对象内容分别记录修改前 digest，按版本化脚本做定点删除／篡改，再记录修改后 digest 和 fault ID，最后启动**同一二进制、同一数据目录、同一 volume**。运行记录 server PID、退出码、停止／重启事件、路径类别而非用户绝对路径、两个 digest、注入脚本 digest 和恢复后首个 Windows 调用／authority 状态。在线更改数据库或对象文件不算 WN-13 证据；若停机前无法确认所有写入已结束，case 为 setup failure。

原生 probe 通过 Windows 文件 API 与 WNet API 记录调用、参数类别、调用／完成 monotonic 时间、返回码、volume serial、文件 ID 和对象 digest；另一个只读 source-binding 记录为 `hostID, userSIDHash, hostLogonLUIDHash, endpointSocket, SMBSessionID, treeID, exportID, volumeID, authorityIncarnation, nativeCallID, actionID`。跨进程关联只能依赖这组受保护的 ID，不依赖日志行时间邻近。每个运行输出 `manifest.json`、`events.jsonl`、`faults.jsonl`、`assertions.json`、`artifacts.sha256`；schema 版本和 corpus digest 固定，运行结束后只读归档，敏感字段净化。`manifest` 记录 OS edition/build/arch、二进制 digest、配置 digest、host/session identity 摘要、两个入口及代理版本；`assertions` 对每个预期状态标记 pass/fail/setup-failure 并引用事件 ID。原始 trace 与归档内容 hash 绑定，避免只保留人工摘要。

夹具为 WN-09 预留两台**不同物理或虚拟主机**的 Windows A/B 角色，以及每个 host 独立可切断的 HTTP／变更路径；同机两个登录会话不能冒充两台 host。OS 矩阵必须覆盖 Windows 11 24H2+ Home x64、Home ARM64、Pro x64、Pro ARM64；每 cell 的普通用户、445、LUID、签名与映射 teardown 均单独记录。跨架构的编译成功不代替原生执行。现有 CI 没有这些 host 时报告“未执行”，不产生绿色资格结论。

## 备选方案

**只用合成 SMB 客户端和 mock WNet。** 它能稳定构造 wire 边界，但不能证明 Explorer、redirector、445、登录会话关联和真实 WNet 移除；保留为单元故障注入，不承担原生门。

**让测试 authority 使用内存 backend。** 它缩短启动时间，却无法验证 HTTP 回执、持久对象、崩溃重启和删除义务；夹具使用实际服务和持久 volume。

**用管理员权限改端口或系统设置。** 可以绕过 445 占用，却不会证明 R-WIN-1 的普通用户与无全局设置条件，且改变受测对象，因此不采用。

**由 SMB package 自动创建与移除映射。** 导入库会隐含系统副作用，也不能由多 volume 宿主决定哪些 drive 生效和谁承担停止责任；显式控制器把这个决定留给宿主。

## 验收标准

- `packages/smb/windows` 在 Windows 四个 OS cell 可由普通用户调用；每个 cell 证明本端点接纳真实 445 socket、签名 TREE_CONNECT、映射身份与安全的 SID／LUID 关联。任何无法证明的 cell 为 fail，不能用模拟请求补成 pass。
- WNet busy、可证明无效果、确认移除和 Unknown 的故障路径逐个验证：busy 恢复原映射与 I/O，确认移除保留 cleanup pending，Unknown 保留 fence 与 owner；同意图重试不双重释放。故障在每次 durable intent flush 前／后及 WNet 副作用前／后终止进程，重启后仅同 SID／LUID 的 owner 可恢复，盘符／UNC ABA 不能被文本相等误判。
- 真实 `remote-fs-server` 的持久 volume 经 HTTP 代理分别复现提交前断开、提交后响应丢失和服务崩溃重启；SMB 与变更代理能独立注入且精确命中事件 barrier。直连 445 与代理 445 分别运行并绑定两端 socket。WN-13 的离线破坏保留停机／重启 barrier 与修改前后 digest。未命中注入点时测试失败。
- 每 cell 生成可校验 digest 与 schema 的净化证据包，记录测试二进制、OS、host、会话、socket、SMB tree、volume、动作及权威结算的绑定；无 skip 冒充通过。库构造和导入没有 WNet 或全局系统副作用，外部 backend 最后仍由宿主持有。

## 风险

Windows redirector 可能始终走系统占用的 445，或普通用户无权建立满足要求的 loopback 目标；设计文档不能消除这个阻塞。网络登录 LUID 若无法安全关联宿主会话，同一用户的实际映射也可能被正确的 fail-closed 策略拒绝。两者都必须先取得实机证据并形成可审查机制；不能通过放宽身份、签名或系统策略宣告支持。

WNet 查询与进程崩溃之间可能再次出现身份不确定区间，且按本地盘符取消没有映射 generation CAS。唯一 UNC 只能帮助诊断，持久 owner 与 fence 仅保护本端点准入；若原生负控仍能制造 same-letter／same-UNC ABA 或在本控制器之外重用盘符而无法证明取消只作用于本 owner，发布门失败。故障代理的插入会改变时延；资格测试必须保留不注入的基线，并在 manifest 中记录代理拓扑和版本。
