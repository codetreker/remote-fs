# Agent Note: SMB 协议基础组件独立于文件适配

Status: implemented

## 问题

SMB 接入既需要报文、签名和身份认证，也需要把平台操作正确绑定到远端文件。前者可以独立验证，后者必须遵守已经交付的 NodeID、FileSession/File 与中立能力。把旧平台 backend 镜像一起带入，会让文件引用、动作历史与生命周期出现第二套解释；重写已经有独立测试的纯协议算法又增加了无关变化。

## 决定

[`packages/smb`](../../../../packages/smb/auth.go) 定义 Principal、Authenticator/Authentication 交换与 context 传递；[`internal/wire`](../../../../packages/smb/internal/wire) 负责有界报文、compound、请求/响应结构及 UTF-16、lease/lock/notify/reparse 数据；[`internal/signing`](../../../../packages/smb/internal/signing) 负责 preauth hash、签名密钥派生、AES-CMAC 与签名会话销毁。它们复用[固定来源中的独立协议部分](https://github.com/codetreker/remote-fs/tree/1cb9ad7f49d998de4daa4d562d766b18cf06ce16/packages/smb)，并以自己的 package 测试验证。

协议 parser 按调用方的 byte/command/context 上限检查结构。Request 的 slice 借用输入帧，调用方在处理结束前保留该帧；解析错误不变成空请求。签名会话串行协调 Destroy 与正在执行的 Sign/Verify，销毁后明确失败并清除派生密钥。存在 SMB 3.0 密钥派生 helper 或 SMB1 形状的 negotiate bootstrap，不构成 endpoint 支持相应旧 dialect 的承诺。

[`packages/smb/windows`](../../../../packages/smb/windows/auth.go) 提供 inbound Negotiate 的 SSPI authenticator、当前用户 SID 和精确 SID policy。导入或构造 Authenticator 不取得凭据，Begin 为每次交换取得独立凭据。Step 与 Close 共享 native context 的串行所有权；取消不能强制中断已经进入的同步 SSPI call，native 返回后不向已取消调用交付身份。Token/SessionKey 的返回缓冲由调用方拥有，派生签名密钥后须清除不再使用的 key。

Principal.SID 是身份，Name 只用于诊断。WithPrincipal 本身不做认证，集成方只能把已验证结果交给请求 context；AllowSID 比较该确切 SID，不从显示名推导权限。非 Windows 平台调用 native 功能明确拒绝，平台 SECURITY_STATUS 保留为错误码，不把 token/凭据写入诊断。该 policy 不代替业务方的远端 volume 操作授权。

SP800-108 派生实现保留原[NOTICE](../../../../packages/smb/internal/signing/NOTICE)及其中的来源、版权和再分发条件，代码与测试不脱离这个记录。复用范围止于协议基础组件；旧 windowsBackend/windowsSession/windowsFile、EntryID、通用 FileAction receipt 与引用管理不成为当前适配层。

## 备选方案

**连同旧平台文件适配一起复用。** 协议状态机与文件方法会同时带回另一套 backend、句柄和动作模型，违反[中立核心](2026-09-16-neutral-file-capabilities.md)已经选定的复用边界。文件适配须直接组合现有 File/NodeReference 与可选能力。

**从头重写全部协议基础算法。** 可以统一写法，但会失去已有畸形报文、签名向量与身份 context 回归的直接复用，又不能解决尚未完成的文件/名字绑定。将独立代码和对应测试一起接入，把变化集中在实际需要的适配上。

## 后果

协议基础组件已经能够单独编译与测试。该批普通/race 验证各有 70 个 pass、无 fail/skip，输入在运行期间保持不变；vet 与 Windows ARM64/AMD64 测试二进制构建通过。普通测试中的 fuzz 函数只执行现有 seeds，不等于持续 fuzz；Linux 选择的 Windows helper 测试与交叉构建都不证明 native SSPI 交换/清理已经运行。具体命令和分层由[测试策略](../../../../docs/testing.md#smb-协议基础组件)拥有。

原生 SSPI 另由[当前 checkout 作业](../../../../.github/workflows/native-smb-gate.yml)执行四个 package 的完整测试，列举并要求两项真实 SSPI 根，使用独立的 Windows 包内覆盖门槛。它记录当前源码/环境/profile，不使用固定原型或 overlay；诊断原型与当前源码验证分别保留来源。

[原生运行 35095465239](https://github.com/codetreker/remote-fs/actions/runs/35095465239)以当前源码 checkout `328d5f64e22f9392816c69dca9a3c7707a0662ad`（PR head `8451a2ee2080323be8bb9ab171260701fa77342f`）在 Windows 11 Enterprise build 26200 ARM64 完成 42 个根、71 个 verdict，两项 native SSPI 根均通过且无 fail/skip。Windows package 自身覆盖为 135/171（78.9%），全部四包合计 762/805（94.66%），最低函数为 66.7%；来源校验无意外差异。该结果证明这批实际协议/认证组件，不能替代尚未接入的 endpoint、文件适配或映射。

这些包尚不提供连接服务、share 发布、映射或已验证的 Windows 网络驱动器。接入仍须实现 endpoint/会话处理和中立文件适配，并对组合后的鉴权与文件行为完成真实平台验收。[中立观察能力](2026-09-16-neutral-file-capabilities.md)的 Windows 接入、历史时间显示和缓存透明性继续由[Windows 提案](../../proposed/architecture/2026-09-16-platform-client-capabilities.md)承接；本决定只部分交付其协议依赖，不缩减目标或把诊断原型成功当成交付实现。
