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

[cleanup attempt](../../../../packages/smb/cleanup_attempt.go)让并发调用共享本轮不可变结果，后来调用才能重试。Shutdown 返回当前清理尝试的错误，历史故障继续留在诊断状态；恢复后的成功不被旧错误永久覆盖，当前未知也不被旧成功覆盖。registry 锁不跨授权、I/O 或等待；Status 和结构化日志只报告计数、阶段、协议请求标识和固定错误分类，不记录 SID、名字、token、key 或 payload。

文件 CREATE、名字/属性解释、范围、通知和映射尚未接入；会话/控制与不带属性的 CLOSE 清理路径之外明确返回不支持。新增代码承担后续命令共用的拥有权和准入，不假装提供已经可用的文件系统。具体 API 与限额由[client 设计](../../../../docs/design/client/architecture.md#smb-协议与会话端点)拥有。

## 备选方案

**连同旧平台文件适配一起复用。** 协议状态机与文件方法会同时带回另一套 backend、句柄和动作模型，违反[中立核心](2026-09-16-neutral-file-capabilities.md)已经选定的复用边界。文件适配须直接组合现有 File/NodeReference 与可选能力。

**从头重写全部协议基础算法。** 可以统一写法，但会失去已有畸形报文、签名向量与身份 context 回归的直接复用，又不能解决尚未完成的文件/名字绑定。将独立代码和对应测试一起接入，把变化集中在实际需要的适配上。

**只接入 framing，再由每个文件命令管理自己的资源。** 这会重复认证、引用安装、续期与清理，并在跨命令取消/退役时失去唯一拥有者。先落实有界 session/tree/handle 管理，后续命令直接消费同一引用；不为每个命令创建另一层文件 backend。

**协议句柄移除后立即释放额度。** native Close 的失败仍可能保留引用和保护；提前退款允许反复断线累积无界资源。保留原额度直到确认清理使错误可见，代价是故障持续时明确拒绝后续准入。

## 后果

协议基础组件已经能够单独编译与测试。该批普通/race 验证各有 70 个 pass、无 fail/skip，输入在运行期间保持不变；vet 与 Windows ARM64/AMD64 测试二进制构建通过。普通测试中的 fuzz 函数只执行现有 seeds，不等于持续 fuzz；Linux 选择的 Windows helper 测试与交叉构建都不证明 native SSPI 交换/清理已经运行。具体命令和分层由[测试策略](../../../../docs/testing.md#smb-协议基础组件)拥有。

原生 SSPI 另由[当前 checkout 作业](../../../../.github/workflows/native-smb-gate.yml)执行四个 package 的完整测试，列举并要求两项真实 SSPI 根，使用独立的 Windows 包内覆盖门槛。它记录当前源码/环境/profile，不使用固定原型或 overlay；诊断原型与当前源码验证分别保留来源。

[原生运行 35095465239](https://github.com/codetreker/remote-fs/actions/runs/35095465239)以当前源码 checkout `328d5f64e22f9392816c69dca9a3c7707a0662ad`（PR head `8451a2ee2080323be8bb9ab171260701fa77342f`）在 Windows 11 Enterprise build 26200 ARM64 完成 42 个根、71 个 verdict，两项 native SSPI 根均通过且无 fail/skip。Windows package 自身覆盖为 135/171（78.9%），全部四包合计 762/805（94.66%），最低函数为 66.7%；来源校验无意外差异。该结果只证明当时的协议/认证组件，不覆盖后来增加的端点，也不能替代文件适配或映射验收。

端点这批普通/race 验证各执行 54 个根、87 个 verdict，分别为 0.023/1.052 秒，无 fail/skip；packages/smb 自身覆盖 86.2%，最低函数 50%，vet 与 Windows ARM64/AMD64 构建通过。previous-principal 负向对照在指定授权断言失败。真实 TCP 用例验证会话协议，引用安装用例直接提供 neutral fixture 引用，不能当作 wire CREATE 或真实 backend 文件访问。新增端点尚无原生 Windows 运行收据。

本机连接与会话入口已经具备，中立文件命令、名字解释和映射仍待接入，并须对组合后的鉴权与文件行为完成真实平台验收。[中立观察能力](2026-09-16-neutral-file-capabilities.md)的 Windows 接入、历史时间显示和缓存透明性继续由[Windows 提案](../../proposed/architecture/2026-09-16-platform-client-capabilities.md)承接；本决定接续其协议依赖与会话拥有权，不缩减目标或把诊断原型成功当成交付实现。
