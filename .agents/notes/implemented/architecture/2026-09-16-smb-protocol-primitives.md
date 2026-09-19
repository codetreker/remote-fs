# Agent Note: SMB 协议与会话直接组合中立引用

Status: implemented

## 问题

SMB 接入既需要报文、签名和身份认证，也需要把平台操作正确绑定到远端文件。前者可以独立验证，后者必须遵守已经交付的 NodeID、FileSession/File 与中立能力。把旧平台 backend 镜像一起带入，会让文件引用、动作历史与生命周期出现第二套解释；重写已经有独立测试的纯协议算法又增加了无关变化。

端点还必须为认证会话、未安装引用和失败清理提供同一拥有者。只把报文入口接起来会让后续每个命令各自处理配额、续期与退役；丢失关闭结果时，协议 session 已移除也不能证明底层资源释放。

## 决定

### 复用协议与认证基础

[`packages/smb`](../../../../packages/smb/auth.go) 定义 Principal、Authenticator/Authentication 交换与 context 传递；[`internal/wire`](../../../../packages/smb/internal/wire) 负责有界报文、compound、请求/响应结构及 UTF-16、lease/lock/notify/reparse 数据；[`internal/signing`](../../../../packages/smb/internal/signing) 负责 preauth hash、签名密钥派生、AES-CMAC 与签名会话销毁。它们复用[固定来源中的独立协议部分](https://github.com/codetreker/remote-fs/tree/1cb9ad7f49d998de4daa4d562d766b18cf06ce16/packages/smb)，并以自己的 package 测试验证。

协议 parser 按调用方的 byte/command/context 上限检查结构。Request 的 slice 借用输入帧，调用方在处理结束前保留该帧；解析错误不变成空请求。签名会话串行协调 Destroy 与正在执行的 Sign/Verify，销毁后明确失败并清除派生密钥。存在 SMB 3.0 密钥派生 helper 或 SMB1 形状的 negotiate bootstrap，不构成 endpoint 支持相应旧 dialect 的承诺。

[`packages/smb/windows`](../../../../packages/smb/windows/auth.go) 提供 inbound Negotiate 的 SSPI authenticator、当前用户 SID 和精确 SID policy。导入或构造 Authenticator 不取得凭据，Begin 为每次交换取得独立凭据。Step 与 Close 共享 native context 的串行所有权；取消不能强制中断已经进入的同步 SSPI call，native 返回后不向已取消调用交付身份。Token/SessionKey 的返回缓冲由调用方拥有，派生签名密钥后须清除不再使用的 key。

Principal.SID 是身份，Name 只用于诊断。WithPrincipal 本身不做认证，集成方只能把已验证结果交给请求 context；AllowSID 比较该确切 SID，不从显示名推导权限。非 Windows 平台调用 native 功能明确拒绝，平台 SECURITY_STATUS 保留为错误码，不把 token/凭据写入诊断。该 policy 不代替业务方的远端 volume 操作授权。

SP800-108 派生实现保留原[NOTICE](../../../../packages/smb/internal/signing/NOTICE)及其中的来源、版权和再分发条件，代码与测试不脱离这个记录。旧源码的复用范围止于协议基础组件；新的会话端点直接使用现有 FileSession/NodeReference，旧 windowsBackend/windowsSession/windowsFile、EntryID 和通用 FileAction receipt 不进入当前适配层。

### 端点沿原引用拥有者管理会话

[端点](../../../../packages/smb/server.go)提供 New、Publish、Serve、Unpublish、Shutdown 和 Status。[配置](../../../../packages/smb/config.go)显式给出认证、当前业务授权、有限资源和可信 Share.Volume；host 保留 backend/provider/logger，端点拥有接纳的 loopback listener、连接及每次 Authentication。Publish 先预留有限 export 容量，不扫描 volume 或建立永久 root 引用。SMB 3.1.1 的签名和有界 credits/compound/async CANCEL 由同一连接拥有者处理，IPC$ 是不含 FileSession 的控制 tree。

每个 SMB session 与 Export 只创建一份直接 FileSession，多个 tree 共用其有限期限与一个续期 worker。初次 Status 和确认的 Renew 维护期限，旧回复不延长新状态，退役不重绑旧 handle。每次语义操作使用当前认证 Principal 和可信 Volume 授权；previous-session 退役必须使用刚验证的 principal，不能继承另一身份的授权。

NodeReference 是 handle 唯一 Close 拥有者，普通 File 只作为同一对象的接口别名。open reservation 在原结果交付前保留 Attr/Outcome、字节和名额；即使返回错误，非 nil 引用仍有可达 cleanup 拥有者。安装与原 authority 退役有序，晚到引用不能安装成功；引用、借用者和签名/响应用户分别排空，失败清理继续占原 connection/session/tree/open/export 额度。

未完成认证由现有 session 拥有一个到期 watcher。整段交换共享固定 HandshakeTimeout，后续 token 与无关流量不延期；generation/armed 检查使旧 timer 不能关闭新交换或已经成功的认证。初次到期退役 session，重新认证到期保留原 signer/身份。provider Step/Close 保持串行，deadline 后的成功不安装；不遵守取消的 provider 或失败 Close 继续占原拥有者与额度，等待真实结束或既有清理重试。

退役完成是一份共用状态判据，覆盖认证及 watcher、opening tree、tree、authority 和最后响应/签名用户。最后一个 TREE_CONNECT 创建者结算自己和 export 的计数后再次复核，避免已无资源却继续保留 session 额度；复核本身不重做 LOGOFF 或 native Close。只有确知全部拥有者收尾才进入原 frame 清理，不用计数归零推断失败的关闭已经成功。

[cleanup attempt](../../../../packages/smb/cleanup_attempt.go)让并发调用共享本轮不可变结果，后来调用才能重试。Shutdown 返回当前清理尝试的错误，历史故障继续留在诊断状态；恢复后的成功不被旧错误永久覆盖，当前未知也不被旧成功覆盖。registry 锁不跨授权、I/O 或等待；Status 和结构化日志只报告计数、阶段、协议请求标识和固定错误分类，不记录 SID、名字、token、key 或 payload。

会话/控制、不带属性的 CLOSE、受限 CREATE、READ/WRITE/FLUSH 与选定 QUERY_INFO 已接入；SET_INFO、范围、目录枚举、通知与映射仍明确不支持。具体 API、格式与限额由[client 设计](../../../../docs/design/client/architecture.md#smb-名字与原子-create)拥有，已接入打开不代表可用的 Windows 网络驱动器。

### 名字解释和打开绑定到同一权威条件

[resolver](../../../../packages/smb/namespace.go)使用按父的完整 metadata 观察，将平台表示、大小写选择与最终打开分开：客户端用 Windows CompareStringOrdinal 检查全部子名，authority 只验证原始父/叶名、NodeID、目录 revision、边及 metadata CAS。每层使用已有 snapshot 权限，不借公开目录读取绕过 ReadEntries；成功的完整观察才证明缺失组件位置。保留这些事实到最终 OpenAt/NodeReference，才能避免本地检查正确却打开另一个对象。

[CREATE](../../../../packages/smb/create.go)沿既有 reservation 和唯一 NodeReference 拥有者安装引用，普通 File 仍是别名。OPEN/CREATE/OPEN_IF 和具明确 WRITE_DATA 的普通文件清空分支已接入；已有 SUPERSEDE、symlink 与未接入命令明确拒绝。平台 readonly/hidden/system 条件通过 ExpectedMetadata 约束旧目标，创建/清空初值和 ARCHIVE 与效果一起发布。总共四轮的重新观察只处理已知无效果条件冲突，未知结果或清理失败不制造第二次 mutation。

[平台 metadata](../../../../packages/smb/windows_metadata.go)在单个 `smb.windows` Data 中保存有版本的 DOS 位/hint；authority token 与平台格式版本分开，其它平台 key 保留。共同时间使用真实捕获事实；缺少历史 BirthTime/ChangeTime 拒绝必需响应，不把零值、mtime 或当前时间解释为已知历史。内部 metadata 读取在业务打开授权中如实申报，但不自动成为应用的 FILE_READ_ATTRIBUTES。

[512 字节稠密虚拟 extent 与指定 serial](../../../../packages/smb/file_projection.go)是平台展示政策。allocation 来自同一 EOF，serial 来自 host 的规范 Share.Volume，不能解释成物理占用、配额预留、授权或全局唯一设备身份。QFid 使用原 NodeID 与该 serial，协议 open FileID 保持独立；无法计算的最大访问权限明确返回状态，不填虚构 mask。这些显示规则使 CREATE 结果有完整来源，而不向中立 backend 增加 Windows 数据模型。

### 字节操作与信息披露保持原引用事实

[字节命令](../../../../packages/smb/file_io.go)借用已安装 File，READ 只使用一次 ReadAt 的同修订属性与数据；WRITE 只执行一次普通写或原子 append，未知结果不返回猜测 Count、不重试。FLUSH 实际等待 Sync；没有该能力的目录/metadata-only 引用明确不支持。当前授权、granted mask 与真实错误分别检查，不通过路径或额外 Status 给旧 handle 续命。

结果 metadata 在 native 调用前使用与 CREATE 相同的 resultBytes/MaxDirectoryBytes 预留固定硬上限，frame/data 有自己的限制。这个成本会让容量不足的调用在无效果时拒绝，但避免远端返回大 metadata 后才发现没有保留空间。HTTP 中间表示仍由既有 response pools 负责，context callback 不能冒充跨 HTTP 的预算协议；不增加新池或核心 API。

普通写入保持 Windows payload 不变，包括 ARCHIVE。现有原子内容能力不能同时更新该 payload；要求 bit 已存在会阻止本来合法的写入，前后追加 SetMetadata 又不能证明与字节同次生效。clear/absent 在成功写后可能保持原样，CREATE/reset 的 ARCHIVE 初始化仍执行。**原子 ARCHIVE-on-write** 保留为具名后续工作：需要评审现有 FileMutation 组合、最终事务、权限及一次 quota/publication 结算，并有真实原子性和未知效果用例，不能通过延迟 Close 修补暗示已经兑现。

[信息命令](../../../../packages/smb/query_info.go)按 class 选择一次 Stat、一次 State 或已拥有的 identity/access，不为所有查询先取属性。Standard 的 pending/detached 与大小来自同次 State；EA=0 表示平台不暴露 EA，内部 namespace 不改称 EA。已知虚拟接口和逻辑 Space 支撑有限 filesystem class；512 单元、指定 serial 和未用虚拟预算均不变成物理 backend 事实。所需时间或未实现的 position/mode/name 缺失仍拒绝。协议要求忽略的输入字段与声明长度 credits 分开，固定输出不足在任何观察前失败。

## 备选方案

**连同旧平台文件适配一起复用。** 协议状态机与文件方法会同时带回另一套 backend、句柄和动作模型，违反[中立核心](2026-09-16-neutral-file-capabilities.md)已经选定的复用边界。文件适配须直接组合现有 File/NodeReference 与可选能力。

**从头重写全部协议基础算法。** 可以统一写法，但会失去已有畸形报文、签名向量与身份 context 回归的直接复用，又不能解决尚未完成的文件/名字绑定。将独立代码和对应测试一起接入，把变化集中在实际需要的适配上。

**只接入 framing，再由每个文件命令管理自己的资源。** 这会重复认证、引用安装、续期与清理，并在跨命令取消/退役时失去唯一拥有者。先落实有界 session/tree/handle 管理，后续命令直接消费同一引用；不为每个命令创建另一层文件 backend。

**协议句柄移除后立即释放额度。** native Close 的失败仍可能保留引用和保护；提前退款允许反复断线累积无界资源。保留原额度直到确认清理使错误可见，代价是故障持续时明确拒绝后续准入。

**用连接读期限或下一帧回收遗弃认证。** 已认证 session 的正常流量不能证明另一交换仍有进展；连接级期限也不能只终止那个交换。每个 session 的有限 watcher 只观察自己的 generation，避免每次交换另起 timer callback 堆积在阻塞 provider 后。

**由晚到打开者重新调用 LOGOFF。** 这把已经完成的计数结算变成另一轮远端清理。统一的完成判据只核对已知资源状态，并保留原清理尝试及响应/签名拥有者。

## 后果

协议基础组件已经能够单独编译与测试。该批普通/race 验证各有 70 个 pass、无 fail/skip，输入在运行期间保持不变；vet 与 Windows ARM64/AMD64 测试二进制构建通过。普通测试中的 fuzz 函数只执行现有 seeds，不等于持续 fuzz；Linux 选择的 Windows helper 测试与交叉构建都不证明 native SSPI 交换/清理已经运行。具体命令和分层由[测试策略](../../../../docs/testing.md#smb-协议基础组件)拥有。

原生 SSPI 另由[当前 checkout 作业](../../../../.github/workflows/native-smb-gate.yml)执行四个 package 的完整测试，列举并要求两项真实 SSPI 根，使用独立的 Windows 包内覆盖门槛。它记录当前源码/环境/profile，不使用固定原型或 overlay；诊断原型与当前源码验证分别保留来源。

[原生运行 35095465239](https://github.com/codetreker/remote-fs/actions/runs/35095465239)以当前源码 checkout `328d5f64e22f9392816c69dca9a3c7707a0662ad`（PR head `8451a2ee2080323be8bb9ab171260701fa77342f`）在 Windows 11 Enterprise build 26200 ARM64 完成 42 个根、71 个 verdict，两项 native SSPI 根均通过且无 fail/skip。Windows package 自身覆盖为 135/171（78.9%），全部四包合计 762/805（94.66%），最低函数为 66.7%；来源校验无意外差异。该结果只证明当时的协议/认证组件，不覆盖后来增加的端点，也不能替代文件适配或映射验收。

端点这批普通/race 验证各执行 54 个根、87 个 verdict，分别为 0.023/1.052 秒，无 fail/skip；packages/smb 自身覆盖 86.2%，最低函数 50%，vet 与 Windows ARM64/AMD64 构建通过。previous-principal 负向对照在指定授权断言失败。真实 TCP 用例验证会话协议，引用安装用例直接提供 neutral fixture 引用，不能当作 wire CREATE 或真实 backend 文件访问。[原生包级运行 35118013777](https://github.com/codetreker/remote-fs/actions/runs/35118013777/job/104868296920)以 checkout `b8c8a18401bde71f3cab8842585134e4560d3b21`（PR head `47a132d6af0264dbb150a36207de760c7d1891e1`）在 Windows 11 Enterprise build 26200 ARM64、Go 1.26.8 执行四包的 94 个根、156 个通过 verdict，无 fail/skip，包含新增端点的 52 个根和两项真实 SSPI 根。47 个源码文件含新增 18 文件均与该 head 的 Windows CRLF 检出相符；四包自身覆盖合计 2171/2441（88.94%），最低函数 50%。这证明对应源码的原生协议/会话包级行为，不是系统 SMB 重定向器或完整文件适配验收。

名字/helper 的普通/race 聚焦验证分别通过 10 根/14 verdict 和 11 根/46 verdict；CREATE 聚焦验证为 18 根/54 verdict，SMB 包的该源码普通验证为 104 根/222 verdict、自身覆盖 88.6%、149 个函数均不低于 50%。这些本地与构建证据不延用旧原生收据；新 CREATE 尚未取得系统客户端验收。

本机连接、会话、guarded CREATE、保留引用上的字节命令和选定信息查询已具备；名字/属性修改、范围、目录枚举、通知和映射仍待接入，组合后的鉴权与文件行为继续需要真实平台验收。[中立观察能力](2026-09-16-neutral-file-capabilities.md)的其余 Windows 接入、历史时间显示和缓存透明性继续由[Windows 提案](../../proposed/architecture/2026-09-16-platform-client-capabilities.md)承接；本决定接续其协议依赖与会话拥有权，不缩减目标或把诊断原型成功当成交付实现。
