# Agent Note: 由嵌入方提供 volume 操作授权

Status: implemented

## 问题

一个 volume 可由业务中的多个访问身份使用，只有业务方知道用户、凭据、角色和当前权限。按 HTTP 方法或路径拦截无法完整表达文件打开的读写意图，也容易漏掉持续输出、文件引用、锁动作历史和清理入口。文件系统内置另一套用户体系又会重复宿主已有的认证与凭据生命周期。

[规格](../../../../docs/spec/requirements.md)的 R-INT-7、R-SEC-5、R-SEC-6 把认证留给业务方，并要求可替换的操作授权与明确的撤权边界。[初始范围决定](../process/2026-08-19-mvp-scope.md)对鉴权的延后由本文部分接续；原有存储和错误纪律不变。[volume 管理提案](../../proposed/architecture/2026-08-19-volume-in-the-contract.md)继续拥有实例注册、选择与生命周期。

## 决定

`packages/authz` 定义 transport-neutral 的 Authorizer、AccessRequest 和 Operation，AccessRequest.Open 复用 `storage.OpenAccess`。FileOpenOptions 嵌入同一类型，打开验证与策略输入不维护两份布尔字段。handler 将可信配置的 volume 与完整语义意图交给业务 callback；身份由业务 context 提供，用户、角色、凭据签发和验证不进入文件系统。配置缺省保持已有嵌入方式；启用时 Authorizer 与 volume 必须同时提供，不能让远端字段选择策略资源。

文件 wire 的 op 与授权输入直接共用 authz.Operation，解锁有独立的 FileUnlock；普通与强锁 URL 则在路由规格中声明语义操作，传输名称不承担角色含义。每个请求一次入口回调，具体操作由业务方归组为角色。共享的 OpenAccess 同时表达读、写、创建、截断、排他创建，避免把复合打开拆成多个时刻的策略决定。锁模式不充当内容权限：只读 fd 的 EX flock 是合法操作，解锁和其它 cleanup 独立可控。volume.write 自身可能创建文件，不能只拒绝 create 就宣称禁止创建。

通用的有界传输 admission 可先限制策略调用与错误响应占用；授权先于受控 capability、动作回执、Log、订阅与 snapshot 捕获。已有 capability 仍是 bearer，重复请求也要检查；拒绝核对只说明本次尝试未获准，不抹去原动作的未知结果。服务器自己的 expiry 和 shutdown 回收独立于调用者的 cleanup 权限。

订阅、续订和快照在入口与每个出站数据／控制／保活单元前读取当前策略。已拥有的有界预取保留，排队页面在拒绝后不出站；回调不进入逐行生产循环。已准入单元可以完成，策略更新不与存储提交或 socket write 原子排序。请求取消和 Stop 共用现有写入结束机制，Stop 保持非阻塞，排空与 backend 关闭仍由原生命周期拥有者完成。

内部授权错误用固定 Error() 和可信 errno 防止策略服务的错误文本进入 HTTP／SSE，同时通过 Unwrap 保留本地 cause。明确 ErrDenied 为 EACCES，其余策略故障为 EIO；原请求取消仍由协议与操作阶段分类：普通 volume／file 的已接受发送前取消可为 EINTR，服务端强锁生命周期响应保留 native Unavailable／EIO，不制造新的 code；SDK 本地取消分类不变。普通授权 envelope 与 native 锁回执严格区分，SSE 的可选 errno 保留 typed 拒绝，旧 generic fault 仍是 EIO。直接 SDK 分类不改变副本观察失败后统一不可用的规则。

完整 API、操作表、执行顺序、错误格式与业务接入例子由[授权设计](../../../../docs/design/server/authorization.md)拥有；[测试策略](../../../../docs/testing.md)记录真实 handler／SDK／副本的验证分工。

## 备选方案

**为授权单独定义打开意图和动作映射。** 可使 authz 只依赖标准库，但 FileOpenOptions、wire dispatch 与授权输入会各自保存同一份事实。新增字段或动作时，逐字段复制与分支映射容易漏掉同步。共享基础 storage.OpenAccess 和语义操作值，使验证、执行与策略读取同一定义，不引入具体 backend 或 HTTP 依赖。

**只使用外层认证 middleware。** 它继续是合法的认证与部署入口，但通用 method/path 检查不能直接表达 Open 意图、锁动作历史或流内发送边界。可选语义 callback 把这些入口交给同一份业务策略。

**内置用户、角色和 token 管理。** 可以形成完整部署控制面，却会接管宿主已有的身份、签发和策略生命周期。中性 callback 使业务方复用已有体系；库不决定角色名称或凭据协议。

**只在 stream 建立时授权。** 少了重复策略查询，但旧连接可在撤权后继续收到新数据。逐有界发送单元检查保留后续准入边界，代价是流速受策略服务延迟和可用性影响。

**将 capability 绑定到库定义的 Principal。** 可以按用户限制能力归属，却需要定义身份稳定性、序列化、切换与跨协议比较。当前 bearer 语义加逐请求授权不承担这套模型；业务方必须隔离不同访问身份的客户端状态。

## 后果

业务可以在相同稳定身份下轮换凭据，也可以用当前策略拒绝后续请求与流输出。配置 volume 和 backing 的正确配对属于可信嵌入边界，通用存储接口没有可验证的业务名称。已有 mount／replica、缓存和 capability 不能跨身份复用；切换身份须建立新状态。

撤权不回收已经交付的字节或元数据，也不撤销已准入操作。策略更新本身不打断已准入的阻塞写，只有下一次检查、业务 context 取消或 Stop 能使其后续进展停止。任何忽略取消的业务 callback 或 backend 都可能阻塞排空；库不能通过遗弃任务或并发强制关闭仍被使用的资源伪造完成。

路径 ACL、部分目录视图、Principal 绑定、独立二进制认证入口和多 volume 注册表均不包含在此能力中。它们需要各自明确身份或管理契约，不能从现有 callback 的存在推导。其它传输可以复用中性操作类型，但必须保留 R-SEC-5／R-SEC-6 的检查顺序和错误边界；当前 HTTP adapter 不引入其它传输依赖。
