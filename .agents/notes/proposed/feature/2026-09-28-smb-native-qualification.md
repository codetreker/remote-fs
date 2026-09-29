# Agent Note: Windows SMB 原生资格与发布证据

Status: proposed

## 问题

协议单元测试、交叉编译和绿色 Linux CI 都不能证明 Windows 11 自带 redirector 对映射、缓存、文件身份、共享冲突和断线故障给出的真实行为。单次 Explorer 操作成功也无法证明负查找、目录、内容和属性缓存的首次观察正确；重试后才得到正确值可能已经违反一秒可见性。平台行为在 Home／Pro、x64／ARM64 之间不能从其中一个环境外推。

资格结论还需要可复查的来源链。没有固定 corpus、实际 host 身份、调用时序、注入点、远端最终状态与原始 trace 的绑定，测试名称和“通过”摘要不能回答是否用了真实普通用户、是否命中故障时点、是否在另一台 Windows 主机上观察，也不能复现性能门槛的由来。

## 提案

本任务只交付 Windows 原生资格矩阵、测量与不可变证据；不在资格 PR 中实现遗漏的生产功能。以[完整 Windows 入口提案](2026-09-16-windows-network-drive-support.md)的 WN-01 至 WN-17 和 [`docs/spec/requirements.md`](../../../../docs/spec/requirements.md) 为可观察契约，以[原生映射与夹具提案](2026-09-28-smb-native-wnet-fixture.md)的真实 server、persistent volume、故障代理与映射控制器为执行基础。资格结果只能是 `pass`、`fail`、`not-executed` 或 `invalid-setup`；任一非 pass 不构成 Windows 支持声明。需求新增或验收语义变化必须先更新 spec，再改 corpus 与矩阵。

资格运行在 Windows 11 24H2+ 的四个独立 OS cell：Home x64、Home ARM64、Pro x64、Pro ARM64。每个 cell 使用普通用户、原生 WNet／文件 API 和真实本机 445 SMB 端点，连接到真实 `cmd/remote-fs-server` 的持久 volume；Linux／SDK 入口与 Windows B 按 case 参与。每 cell 独立执行 WN-01–17，不能拿另一个 cell 的通过补足缺口。WN-09 的 Windows A 与 B 必须位于**两台不同 host**，各有可核验 host identity、独立端点／映射和可单独切断的普通 HTTP／变更通道；同机 VM 的 host 关系在证据中明确记录，不能把两个进程算为两台主机。

### 目录与依赖

以下是资格 PR 的目标布局。它复用先前夹具，不修改 `packages/smb`、`packages/storage`、`packages/transport` 等生产实现。

| 路径 | 动作 | 责任 |
|---|---|---|
| `internal/integration/windowsnative/qualification/runner.go` | 新建 | 四 cell × WN-01–17 调度、前置条件和结论聚合；没有静默 skip。 |
| `internal/integration/windowsnative/qualification/cases_*.go` | 新建 | 按 ID 实现原生 API 动作、barrier 与权威断言；每 case 可单独重跑并引用 corpus 版本。 |
| `internal/integration/windowsnative/qualification/evidence.go` | 新建 | 类型化事件、净化、schema 校验、hash 清单和不可变归档。 |
| `internal/integration/windowsnative/qualification/performance.go` | 新建 | 冷状态工作负载、环境记录、样本与分位数计算。 |
| `internal/integration/windowsnative/cmd/qualify-coordinator/` | 新建 | 可运行的双主机控制器；签发一次性 probe 命令、barrier 与最终包索引。 |
| `internal/integration/windowsnative/cmd/qualify-probe/` | 新建 | 每台 Windows host 上由普通用户运行的 WNet／文件 API probe 与本机事件采集。 |
| `internal/integration/windowsnative/cmd/qualify-verify/` | 新建 | 离线验证 schema、digest、事件链、四 cell 完整性与冻结输入。 |
| `internal/integration/windowsnative/qualification/testdata/corpus.json` | 新建 | WN-01–17 的有限版本化操作／故障向量、源类别与期望结果。 |
| `internal/integration/windowsnative/qualification/testdata/evidence.schema.json` | 新建 | manifest、event、fault、assertion、sample、artifact 的机器可验形状。 |
| `internal/integration/windowsnative/qualification/testdata/unsupported-corpus.json` | 新建 | WN-15／16 未支持操作和 wire context 的有限样本及证据来源。 |
| `docs/testing.md` | 修改 | 资格运行命令、host 条件、归档与判定规则。 |
| `docs/spec/requirements.md` | 修改 | R-WS-4 的实测冷状态工作负载与量化门槛，在支持声明前定稿。 |

`cases_*.go` 仅通过 fixture 的公开 driver、Windows API probe 与独立协议客户端发起动作；协议客户端只构造 native API 无法准确表达的 SMB wire 请求。`qualify-coordinator` 和各 host 的 `qualify-probe` 以测试私有、双向认证的控制连接交换版本化消息：`Register(runID, hostID, OS, binaryDigest)`、`Arm(caseID, vectorID, faultID)`、`BarrierReached(eventID, sequence)`、`Release(barrierID)`、`InvokeOnce(callID, APIClass)`、`Observed(callID, result, nativeTimes, traceRefs)`、`Abort(reason)`。每条消息带 run nonce、单调 sequence、ack 和有限超时；断开、重复或乱序使该向量 invalid-setup，不能在不确定状态下再次发起 native 调用。fixture 故障代理只认 coordinator 签发的同一 run/case/barrier ID。`qualify-verify` 离线重算断言，不信任 coordinator 的“pass”文本。输入 corpus 没有真实凭据或用户路径。运行产物写入测试操作员指定的仓库内 `.tmp/windows-native-runs/<run-id>/`，完成后复制为只读、内容寻址的发布证据包；不提交原始敏感日志或本机路径。

最终发布索引绑定**同一冻结的**源码 commit、设计/spec digest、配置 digest、schema/corpus digest 与 authority/proxy 构建来源；每架构各有明确的 Windows endpoint／probe 二进制 digest，Home/Pro 在相同架构上也要记录实际使用的 digest。四 cell 的证据必须同时匹配这些输入并构成一个完整运行集合。为诊断而重跑单个失败向量不能与旧的其它 cell 拼接成 final pass；代码、配置、corpus、阈值或代理版本一旦变化，重新生成四 cell 完整资格集合。索引可指向外部不可变归档，但每条记录须可下载并按 hash 校验；只有摘要而无原始净化证据不算通过。

### Case 分层与判定机制

每个 case 有机器可读的 `caseID, corpusVectorID, requiredRoles, nativeCallPlan, faultPlan, oraclePlan, requiredArtifacts`。runner 先证明 host／用户／端口／服务／volume 的前置条件，再 arm 故障、等待精确 barrier、执行原生调用、读取 authority 终态，最后由独立断言器计算结论。一次调用的成功只对那次调用有效；窗口内“再试一次”不算首次观察。任何环境前置条件失败为 `invalid-setup`，未安排运行则 `not-executed`，断言不符为 `fail`，不能转换成 skip。执行中的 cleanup 失败也令 case 失败，并保留 owner 状态证据。

| Case | 资格驱动与主要 oracle |
|---|---|
| WN-01 | 每 cell 的普通用户 WNet 连接、445 accepted socket、签名 TREE、SSPI SID／安全 LUID 关联；跨用户／跨登录／guest／篡改负路径，在 authority 文件操作前失败。 |
| WN-02 | 六种 create disposition、目录／metadata 打开、最终动作前目标替换；核对单一结果与无部分效果。 |
| WN-03 | 写、EOF、flush、quota 和提交前／后响应丢失；以 action receipt 与 authority 状态区分未执行、已完成、未知。 |
| WN-04 | rename／replacement／unlink／重建后，从旧 FileId 和新路径分别读写；核对 authority node ID、Windows file ID 与 volume serial。 |
| WN-05 | 由 Linux／SDK 注入 Windows 不可表示、大小写冲突及 UTF-8 名字；Windows 查找／枚举／通知不能漏项或改对象。 |
| WN-06 | 属性位 truth table、四类时间的零／非零语义、archive 与内容原子提交；检查其它 metadata namespace 未被覆盖。 |
| WN-07 | 跨 Windows／Linux／SDK 的 access × share、范围锁、取消及响应丢失矩阵；最终权威顺序和清理 owner 是 oracle。 |
| WN-08 | delete armed／pending／completed 与各持久边界的断线、崩溃、重启、名字复用；旧对象、替代物与义务结算分别断言。 |
| WN-09 | 两台不同 Windows host 上预热正负查找、内容、大小、属性、目录、身份；健康第一次观察、变更通道断流／overflow／恢复、WATCH_TREE 与 detached handle 独立断言。 |
| WN-10 | authority 断线后在健康检测前立即调用一次，再在 fence 后调用一次；预热内容不能以旧值、空目录或假 ENOENT 成功返回。 |
| WN-11 | admission fence 与无句柄在途 I/O 竞争、busy 回滚、WNet 三态移除、cleanup pending 和宿主终止后同用户恢复。 |
| WN-12 | 同进程双 volume、凭据轮换、授权撤销及每类计数／字节上限 N／N+1／释放再申请。 |
| WN-13 | metadata、对象字节与持久状态分别不可达或损坏；任何成功读取必须来自完整可证事实。 |
| WN-14 | 审核公开 API／wire／持久 schema，并由不理解 Windows 的 adapter 操作相同 volume；平台语义不得穿透中立层。 |
| WN-15 | 版本化 unsupported corpus 由原生 API 优先、必要时独立协议客户端发起；durable／persistent 请求不得被授予，旧句柄断线后不得恢复。 |
| WN-16 | 查询支持的信息类、空间真值与变化分类；对 unsupported corpus 验证明确拒绝；执行冷状态性能工作负载。 |
| WN-17 | 每类名字／metadata 修改在提交前与提交后丢响应，第三方复用名字；只结算原 action，应用得到未知 I/O 错误。 |

WN-09 先在**未预热受测条目**的独立静默阶段运行超过配置的 idle window，以 authority／endpoint trace 证明没有周期性查询；静默结束才为每个向量建立新条目并预热，不能在预热后等待完整静默期。WN-09／10 对正／负查找、内容、大小、属性、目录和 ID 分别做有限的重复 native probe，并用完整 SMB／endpoint trace 归类：重复调用无对应 SMB 请求且返回原事实为 `local-hit`；每次都到达端点为 `remote-served`；同类别两种路径均出现则分别保留向量。trace 不完整或请求被挂起但归属不明为 invalid-setup，不能凭猜测选类别。

`local-hit` 向量在首次 native 读取和同一 API／路径的重复 hit 后，保持同一映射／会话，立即由 writer 修改或由代理切断 authority，再执行**第一次**故障后 native 观察。记录 `primeAt, hitAt, mutationOrOutageAt, firstObserveAt`，并用同 OS／API／路径类别的无变更对照在不短于实际 `hitAt→mutationOrOutageAt` 的年龄再次证明 hit；若对照已 miss，重新预热该向量，不能声称故障前有活缓存。`remote-served` 向量无需伪造 cache hit：先以 trace 证明重复调用确实送到端点，随后按同样的 mutation/outage 和一次性首次观察流程执行，以该观察的 SMB 请求、远端调用／失败及 authority 真值证明正确结果或真实 I/O 错误。某类别不缓存不是发布失败；缺少其可核验的实际路径、首次观察或故障断言才是缺口。

WN-09 的健康路径由 writer host 在收到**权威确认**时以自己的 monotonic 时钟记录 `t0`，随后向 coordinator 发已签名 `CommitConfirmed(eventID)`；coordinator 才可向另一 host 签发一次性的 `InvokeOnce`。运行前独立测量控制链往返上界并固定于 manifest，超出预设预算时不启动向量，记 invalid-setup；运行开始后不因迟到改写结论。后段窗口向量由 writer host 在自己的 `t0+δ` 发 `Release`（δ 固定于 corpus 且小于 1 秒），observer 收到后只调用一次 native API，并把 `Observed` 连同结果返回 writer host；**writer host 收齐结果和 observer 完成证明的本机时点必须 ≤ `t0+1s`**，且结果为当前权威版本。这个往返判定比用户调用完成更保守，但完全使用 writer 一台主机的时钟，不依赖跨主机时钟同步；一旦 release 发出，旧值、错误成功、超时或结果迟到都为 fail，命令未送达／观察未发生另记 invalid-setup 并保留事件链。另一次独立向量由 writer host 在 `t0+1s` 后发送 release，observer 第一次观察必须直接成功并返回当前版本；`local-hit` 类的故障前缓存寿命对照须覆盖从 `hitAt` 到本次 mutation 的实际年龄，`remote-served` 类保留该次请求到端点的 trace。窗口外后续重试不能修补失败。断流、overflow、恢复和 endpoint 已驱逐事实但 redirector 仍可能缓存的情形分别产生事件链，恢复完成后的第一次对应观察仍必须成功或按契约明确 I/O 失败。

WN-10 的调用必须落在任何心跳／静默检测之前，并以代理断开时间与 endpoint 检测 trace 证明顺序；若调用前已经检测到断线，该向量为 `invalid-setup`，不能当作“立即故障”通过。断线期间由其它入口改变旧事实；恢复第一次观察核对新对象身份与版本。WN-03／17 的“响应丢失”由 authority commit receipt 和代理丢包事件共同证明，应用返回和 endpoint ledger 分开记录，不把最终状态推断为应用已成功。WN-15／16 每个 corpus 条目声明 `source=native-api` 或 `source=protocol-client` 及为何 native 不可达；协议证据不能代替可达的原生结果。

### 证据模型与不可变性

每 cell 输出一个内容寻址包，至少包含以下实体。所有时间均保留 host monotonic 纳秒、wall UTC、时钟来源和最大同步误差；跨 host 先后以协议 barrier／authority sequence 为准，不靠墙钟碰巧接近。

| 实体 | 必需字段与关系 |
|---|---|
| `RunManifest` | schema/corpus 版本与 digest、源码 commit、每个二进制 digest、OS build／edition／arch、host ID、普通用户 SID／LUID 的受保护摘要、endpoint 445 socket、authority volume ID／incarnation、backend 与代理拓扑、测试配置摘要。 |
| `NativeCall` | `callID, caseID, vectorID, hostID, processID, API, inputClass, invokedAt, completedAt, resultCode, bytes, volumeSerial, fileID, observedDigest, pathProofID`；不记录密钥或文件明文。 |
| `PathProof` | `local-hit` 时记录预热／重复 call、无对应 SMB 请求的完整 trace、同一 session、有效寿命对照与 `primeAt/hitAt/mutationOrOutageAt/firstObserveAt`；`remote-served` 时记录重复调用及故障后首次观察对应的 SMB 请求、endpoint→authority 调用／失败、authority 真值和同一 session。每事实类别每 OS cell 至少一条实际路径。 |
| `ControlEvent` | run nonce、发送／接收 host、message sequence、barrier／call ID、writer host 的 `t0`／release／观察回执时点及控制链预算。 |
| `FaultEvent` | `faultID, armedAt, triggeredAt, path, boundary, barrierID, requestID, actionID, authoritySequence, effect`；未触发为 setup failure。 |
| `AuthorityFact` | `volumeID, incarnation, nodeID, parent/edge revision, contentDigest, length, metadataDigest, receiptID, commitSequence, capturedAt`；来自真实 authority 的查询或持久记录。 |
| `Assertion` | `caseID, vectorID, expectedContract, actualEventIDs, oracleFactIDs, verdict, reasonCode`；只引用同一运行的 immutable ID。 |
| `PerformanceSample` | 工作负载 ID、数据集及 quota ledger 的前／后 digest、冷状态证明、新 SMB SessionId、CPU／内存／磁盘／网络配置、连接／枚举／读取／小文件调用时延、重复序号与原始事件 ID。 |

每个包按确定次序 canonicalize，生成 `artifacts.sha256` 与顶层 manifest digest；归档后不得原位改写。发现净化遗漏时生成新包并保留旧 digest 的撤销记录，不能静默覆盖；凭据、SMB token、实际文件内容和本机用户路径不进入包。证据审查器重新校验 schema、hash、case×cell 完整性、event 引用、time/barrier 顺序、authority 身份绑定和每条断言的 oracle 来源。丢失原始 trace、代理命中记录或远端真值时，该向量不能通过。

### 冷状态性能门槛

R-WS-4 当前只有“冷挂载后迅速可用”的定性要求。资格 PR 先固定真实数据集构成、目录宽度／深度、文件大小分布、已有内容、持久 volume、host 硬件档次、网络延迟／带宽、后端配置、冷启动方法与重复次数。每次重复从同一测试私有的**持久 volume 快照**恢复完整 dataset、metadata 与 quota ledger，核对恢复后 digest／已用空间／可用空间与基线完全一致，再启动实际 server；上次小文件写入不得污染下次样本。冷状态同时清除 endpoint 可丢弃缓存并由普通用户非强制移除上次映射、停止旧 endpoint、建立唯一新 UNC 的映射和新 Windows 进程；从签名 TREE_CONNECT trace 证明**新的 SMB SessionId**，从 authority 证明新 FileSession。若 redirector 复用旧 SMB session 或无法安全移除旧映射，本次为 invalid-setup，不能当成冷样本；不得用全局缓存开关改变目标行为。测量连接、第一次目录枚举、最初读取与小文件 create/write/close 的单次原始延迟，公布各 cell 样本及分位数、失败率、方差和完整环境，不只公布最好值。

根据这些基线和产品可接受等待时间，在资格判定**之前**把量化工作负载、阈值、统计量、最大失败率与支持硬件条件写入 `docs/spec/requirements.md` 的 R-WS-4；然后冻结 corpus 和阈值，再跑四 cell 的最终资格。若阈值不能达成，资格为 fail，优化须由另一个生产改动 PR 负责，随后重跑受影响矩阵；不能调高阈值或删慢样本换取当次通过。R-WS-5 容量值同样必须从 authority 真值和实际 quota 拒绝核验，不能由 UI 显示的漂亮数字代替。

## 备选方案

**以协议级 SMB 客户端作为发布验收。** 它可精确构造非法 wire 请求，却不能证明 Windows redirector 的缓存、WNet、普通用户和 LUID 行为；仅用于 WN-15／16 原生 API 不可表达的向量。

**只跑一台 Pro x64 机器。** 能加快反馈，但 Home 和 ARM64 的网络栈／系统策略可能不同，不能推出四个受支持 cell 的结论。

**发现问题时在资格 PR 内直接修生产代码。** 使资格证据与实现变更混在一个不稳定 baseline，失去每个失败对应哪份实现的可审查性；问题归属原功能 PR 或单独修复 PR，资格在新 digest 上重跑。

**用定时轮询直到读到新值。** 它证明最终一致，不能证明首次观察与一秒时限；资格以单次受控观察和时序证据判定。

## 验收标准

- 四个 OS cell 的 WN-01–17 每个必需 corpus 向量均为 `pass`，无 `not-executed`、`invalid-setup`、skip 或未解释排除。WN-09 的 A/B 属于不同 host，保留可核验 host 与双路径隔离证据；WN-09／10 的每类事实按实际 `local-hit`／`remote-served` 路径证明首次观察与故障结果，只有实际缓存的路径要求 mutation／outage 前的活缓存证据。
- 每份 pass 由应用调用、真实 endpoint socket／session、故障命中、权威状态和清理终态的事件链支撑；证据包可独立通过 schema、hash、关联和时序审查。未支持 wire 向量明确标注协议客户端来源。
- R-WS-4 的固定工作负载与量化阈值已在 spec 生效，最终四 cell 的冷状态样本通过该阈值；R-WS-5 的真值与拒绝点可复核。
- 无生产功能改动混入资格 PR。任何失败均作为失败保存，定点重跑仅用于诊断；最终发布索引只接受同一冻结源码／spec／配置／corpus／代理来源、各架构二进制 digest 明确对应的四 cell 完整通过集合。冷状态重复均有相同 dataset／quota 基线和新的 SMB SessionId。

## 风险

普通用户 445、登录 LUID 安全关联与每一类 Windows 缓存的首次观察仍是实证阻塞项。即使其它 case 全绿，只要其中一项未证明，不能宣告 Windows 支持。宿主或系统更新可能改变 redirector 行为；资格包绑定确切 OS build 和架构，新 build 的支持范围必须重新评估。性能样本受硬件与网络波动影响，环境记录与冻结阈值使结果可解释，但不保证未测硬件拥有相同性能。
