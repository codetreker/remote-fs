# 本机 SMB 端点

`packages/smb` 是 Windows client 进程内的本机呈现层。Windows 系统 SMB client 通过 loopback TCP 连接它；端点把认证后的 share 连接绑定到调用方提供的 `storage.FileStorage`。SMB 只存在于 client 与同机操作系统之间，client 到远端 authority 的边界仍是平台中立的 storage／HTTP 契约。

端点支持 SMB 3.1.1 协商、认证、消息签名、share 连接与会话清理。文件、目录和名字命令不在支持面；这些命令返回明确的不支持结果，并且不会访问 backing。这个边界不构成可浏览或可读写的 Windows network drive。

## 一、嵌入与发布

`smb.New` 接受 `Config{Authenticator, AuthorizeIdentity, Authorize, Limits, Logger}`。构造函数先验证两个授权接口、认证 provider 和全部资源上限，不取得 Windows credential、不启动 goroutine，也不打开 listener。`packages/smb/windows` 提供 SSPI `Authenticator`、读取进程身份的 `CurrentIdentity` 和只允许该确切身份的 `AllowIdentity`；非 Windows 构建保留同一 API，并在实际取得平台能力时返回不支持。

一个 `Server` 可以发布有限个 `Share`。每个 share 由不区分大小写的 SMB 名字、可信的 host-selected volume identity 和一份调用方拥有的 `storage.FileStorage` 组成。发布时执行 `CheckFileStorage`，但不建立 FileSession，也不接管 backend 的关闭责任。名字为空、过长、含 SMB 分隔／保留字符、前后空白或与 `IPC$` 冲突时拒绝。

`Serve` 只接受已经绑定到 loopback TCP 地址的 listener，并在验证成功后取得其所有权。每条 accepted connection 还要再次核对 peer 是 loopback；其它来源在进入协议处理前关闭。Server 不创建 WNet 映射、不安装驱动、不修改防火墙、系统 SMB 配置或全局缓存策略。

```text
Windows SMB client
        │ loopback TCP, SMB 3.1.1
        ▼
Server ── connection ── authenticated session ── tree
  │                                             │
  └── Export ── trusted volume + FileStorage ◀──┘
                         │
                         ▼
              remote storage / authority
```

`IPC$` 只建立 control tree，并且只支持关闭该 tree。普通 share 的第一次 `TREE_CONNECT` 为该 authenticated SMB session 与 export 建立一份 authority session；同一 SMB session 对同一 export 的多个 tree 共用它。不同 SMB session、不同 export 或不同本地身份不共享 FileSession。

## 二、协议与消息完整性

连接使用 Direct TCP framing。四字节前缀中的类型必须为零，长度在读取 payload 前受 `MaxFrameBytes` 限制；短帧、越界 offset、无效 alignment、非法 UTF-16、重复的唯一 negotiate context 和超量 compound/context 都使连接失败，不能用截断或默认值继续解释。

首包可以是 SMB1 形状的 wildcard negotiate，它只选择 SMB2 framing；随后必须进行真正的 SMB 3.1.1 `NEGOTIATE`。端点只接受 SHA-512 preauthentication integrity 和 AES-CMAC signing，返回的 `MaxTransactSize`、`MaxReadSize` 与 `MaxWriteSize` 都来自 `MaxIOBytes`。preauthentication hash 使用收到和发出的原始 wire bytes，不能从解码后的字段重建。

SMB2 compound frame 在一个有界 payload 中解析。每项保存自己的 header、body 和用于签名的完整 command bytes；related compound 只继承同一 frame 中前一项的 SessionId 与 TreeId。混用 related 与 unrelated 风格、把 `NEGOTIATE` 或 `SESSION_SETUP` 放入 compound、非法 offset／alignment、非法 credit 或重复 MessageId 都拒绝。response 逐项保留对应状态，related 前项失败时后项不执行受控效果。

认证完成后，每个携带 SessionId 的请求都必须使用该 session 的 AES-CMAC signing key 验证，合法请求及能够归属该 signer 的错误 response 也由同一 key 签名。unsigned session request 的拒绝保持 unsigned；带 signed flag 但 MAC 错误的请求得到 signed access-denied。已建立 session 的 reauthentication 在解码 token 前先验签，畸形或被篡改的 exchange 不能得到 unsigned 旁路。sessionless ECHO 可以在 negotiate 后使用，并且不继承其它 session 的 signer。未知或已经退役的 session、replay/DFS flag 与失效身份均在访问 tree 或 backing 前失败。session key 只用于派生 signing key，临时 token 与 key buffer 在使用后清零；日志不写入 token、key、SID 或显示名。

## 三、Windows 身份与授权

`packages/smb/windows` 通过 SSPI `Negotiate` 接受 SPNEGO token。每次 authentication exchange 独立取得 inbound credential；`AcceptSecurityContext` 必须建立 integrity-capable、非 null session 的 security context。完成后从 context token 读取用户 SID 与 `TOKEN_STATISTICS.AuthenticationId`，把二者组成授权身份；account display name 只用于诊断，不参与相等比较或准入。

标准组合先用 `CurrentIdentity` 捕获宿主进程 token 的 SID 与 logon-session ID，再把结果交给 `AllowIdentity` 作为 `Config.AuthorizeIdentity`，只允许精确匹配。每次初始 authentication 和 reauthentication 都在安装 principal／signer 或替换 expiry 前调用它；明确拒绝产生 access denied，无法决定产生 I/O failure。anonymous、Guest、LocalSystem、LocalService 与 NetworkService 明确拒绝。同一个 SID 的另一次登录不是同一身份，不能接管 `PreviousSessionId`、复用 session 或访问它的 tree。

SMB 本机身份与远端业务身份是两层独立保护。`AuthorizeIdentity` 决定一个已验证的操作系统主体能否建立或刷新 SMB session；端点随后把该 `Principal` 放进 request context，并用独立的 `Config.Authorize` 对 trusted volume 与实际 FileSession 操作作准入。`TREE_CONNECT` 依次授权 `file.session-open` 与 `file.status`，后台续期授权 `file.renew`，显式 tree/session 清理授权 `file.session-close`。backing remote storage 继续使用调用方为远端 authority 配置的身份和凭据，SMB SID 不替代它。

authentication exchange 受 `HandshakeTimeout` 约束，完成后的 identity 另受 provider 返回的 security-context expiry 约束。未完成的 exchange 到期后自主关闭，不等待下一次 `SESSION_SETUP` 才回收；reauthentication 使用新的 generation，旧 timer 不能关闭新的 exchange。identity 到期后，普通命令收到 signed `STATUS_NETWORK_SESSION_EXPIRED`，session、signer 与 tree 保留以允许 signed SESSION_SETUP 重新认证；只有同一 SID 与登录会话的成功 reauthentication 才更新 expiry 并恢复工作，LOGOFF 始终可以清理该 session。

## 四、资源与所有权

`Limits` 分别限制 export、connection、authenticated/preauthenticated session、tree、pending request、compound command、negotiate context、frame、I/O 与 authentication token。`MaxIOBytes` 同时成为协商公布的 transact/read/write 上限；协商后完整 request 不能超过它加固定协议 envelope，control command 另受 68 KiB 上限且只能消耗一个 credit，数据命令的 credit charge 至少覆盖每个 64 KiB payload 单元。`FileSessionOptions` 继续限制 authority session 自己的文件、owner、range、等待与动作历史。所有值必须显式选择；零值不表示无界。

一次 connection 持有自己的 negotiate transcript、credit 集合、pending requests 和 session table。全局 session registry 另外限制所有 connection 的 session 总数；只从某个 connection map 删除 session 不释放这份全局 charge。TreeId 与 SessionId 单调分配并检查耗尽，已退休的 ID 不重新绑定新对象。

一个 volume tree 持有 export 引用和共享 authority session 的一份 ref。authority session 建立并验证一份有限 `FileSession`，在已确认 lifetime 内按保守期限续期；旧 revision 不延长 deadline，renew／authorization 失败使 authority session fenced 并进入清理。失败或结果未知的关闭继续保留 export、session 与全局容量，直到同一拥有者重试成功，不能为了释放名额宣告清理完成。

每个 accepted request 有独立的 `RequestTimeout` 和 pending charge。response frame 在 session retirement 前登记为 signer 的使用者；只有 native/auth/tree 资源已经完成退休，并且最后一份 response 已完成构造与签名，session 才能从 registry 移除并清零 signing key。这样 `LOGOFF`、断线或超时不能在并发 response 仍使用 key 时销毁 signer。

## 五、关闭与可观测状态

`TREE_DISCONNECT` 取消已经登记到该 tree 的 pending request，再关闭它持有的 authority ref；同一 export 的最后一个 tree 关闭共享 FileSession。这份清理只覆盖现有 control request 和 authority ownership，不构成文件工作的 per-tree admission fence。`LOGOFF` 先发布 session retirement，取消除当前 LOGOFF 外的请求，关闭 authentication、全部 tree 和 orphan authority session，最后等待 response frame 释放。连接断开执行相同的无授权清理路径，不能把断线理解为资源已经释放。

`Export.Unpublish` 对任何 live tree 或正在取得该 export 的 connect／request 返回 busy，原 mapping、tree 与 FileSession 保持有效；只有没有使用者时才进入清理并移除 export。`Server.Shutdown` 另行永久停止 listener 和新工作，等待 connection 退出，强制清理 tree／FileSession，移除已经静止并确认释放的 export，并保留失败者供重试。调用方提供的 backend 始终由调用方拥有，server 只关闭自己建立的 FileSession。

`Status` 报告 serving/stopping/stopped、仍发布或正在停止的 export、活跃及 cleanup-only connection、session、identity-expired session、tree、pending request、fenced authority session 与累计 cleanup failure。identity expiry 表示本机认证需要重新建立；fenced authority 表示它已拒绝新用途但仍被 owner 保留，可能正在排空、等待 cleanup，也可能 native FileSession 已关闭而其它引用尚未释放。两者不能互相代替。状态报告实际仍被拥有的资源，不以协议表项已经删除代替 native cleanup 完成。

## 六、命令边界

可成功的命令只有 `NEGOTIATE`、`SESSION_SETUP`、`ECHO`、`TREE_CONNECT`、`TREE_DISCONNECT` 与 `LOGOFF`。普通 volume tree 上已识别但未实现的 CREATE、CLOSE、FLUSH、READ、WRITE、LOCK、IOCTL、QUERY_DIRECTORY、CHANGE_NOTIFY、QUERY_INFO、SET_INFO 与 OPLOCK_BREAK 不执行 backing 操作，并返回 `STATUS_NOT_SUPPORTED`。CANCEL 的动作语义属于 range/async command 支持面；在该语义存在之前，CANCEL 与无法识别或畸形的 command fail closed，终止连接而不执行受控效果。

文件适配使用端点保留的边界：tree 绑定 `FileStorage` 与 authority FileSession；请求上下文携带已验证 principal、session、tree、message 和取消生命周期；compound parser 保留逐 command framing；文件身份不会从路径、TCP connection、SessionId 或 TreeId 推导。Windows 名字规则、metadata codec、handle table、share-mode／range 映射、delete-on-close、通知与缓存恢复属于独立设计。

端点不提供 WNet 发布／移除流程，也不声称通过真实 Windows redirector 的可浏览、读写或缓存验收。它提供这些入口所依赖的 secure bounded session foundation。

## 七、需求对应

- package、无进程级副作用和独立 Windows 依赖对应 R-INT-1 与 R-INT-2；
- 逐项资源上限、状态和 cleanup ownership 对应 R-INT-3、R-ERR-4 与 R-WIN-10 的 endpoint 部分；
- loopback、SSPI identity、强制 signing、逐 FileSession 操作授权与 secret-free diagnostics 对应 R-SEC-1、R-SEC-4、R-SEC-5、R-WIN-1 与 R-WIN-9 的本机入口部分；
- 未实现命令的 fail-closed 边界对应 R-WIN-2 与 R-ERR-1、R-ERR-2。

R-FS-5 至 R-FS-9、R-CON、R-CC-14、R-INT-8、R-WIN-3 至 R-WIN-8、WNet lifecycle 和完整 R-WIN-10 不由这个 endpoint foundation 单独满足。
