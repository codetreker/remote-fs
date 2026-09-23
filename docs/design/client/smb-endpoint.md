# 本机 SMB 端点

`packages/smb` 是 Windows client 进程内的本机呈现层。Windows 系统 SMB client 通过 loopback TCP 连接它；端点把认证后的 share 连接绑定到调用方提供的 `storage.FileStorage`。SMB 只存在于 client 与同机操作系统之间，client 到远端 authority 的边界仍是平台中立的 storage／HTTP 契约。

端点支持 SMB 3.1.1 协商、认证、消息签名、share 连接、CREATE/CLOSE 与引用清理。CREATE 可打开或创建普通文件与目录，包括 metadata-only 引用；文件数据和目录枚举命令仍返回明确的不支持结果。这个边界尚不构成可浏览或可读写的 Windows network drive。端点与文件引用的取舍分别见[安全且有界的本机 SMB 端点](../../../.agents/notes/implemented/architecture/2026-09-21-secure-bounded-smb-endpoint.md)和[有界 SMB 文件引用](../../../.agents/notes/implemented/architecture/2026-09-23-bounded-smb-file-handles.md)。

## 一、嵌入与发布

`smb.New` 接受 `Config{Authenticator, AuthorizeIdentity, Authorize, Limits, Logger}`。构造函数先验证两个授权接口、认证 provider 和全部资源上限，不取得 Windows credential、不启动 goroutine，也不打开 listener。`packages/smb/windows` 提供 SSPI `Authenticator`、读取进程身份的 `CurrentIdentity` 和只允许该确切身份的 `AllowIdentity`；非 Windows 构建保留同一 API，并在实际取得平台能力时返回不支持。Windows 名字比较使用本机的大小写无关 ordinal 规则；不具备该规则的平台不能凭近似比较器成功解析 CREATE。

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

SMB2 compound frame 在一个有界 payload 中解析。每项保存自己的 header、body 和用于签名的完整 command bytes；related compound 从同一 frame 中紧前一项继承 SessionId 与 TreeId，紧前一项成功 CREATE 时还可继承其 FileId。混用 related 与 unrelated 风格、把 `NEGOTIATE` 或 `SESSION_SETUP` 放入 compound、非法 offset／alignment、非法 credit 或重复 MessageId 都拒绝。response 逐项保留对应状态，related 前项失败时后项不执行受控效果。CREATE/CLOSE 的固定响应预算在任何会产生权威效果的命令之前计入整个 compound 的 frame 上限。

认证完成后，每个携带 SessionId 的请求都必须使用该 session 的 AES-CMAC signing key 验证，合法请求及能够归属该 signer 的错误 response 也由同一 key 签名。unsigned session request 的拒绝保持 unsigned；带 signed flag 但 MAC 错误的请求得到 signed access-denied。已建立 session 的 reauthentication 在解码 token 前先验签，畸形或被篡改的 exchange 不能得到 unsigned 旁路。sessionless ECHO 可以在 negotiate 后使用，并且不继承其它 session 的 signer。未知或已经退役的 session、replay/DFS flag 与失效身份均在访问 tree 或 backing 前失败。session key 只用于派生 signing key，临时 token 与 key buffer 在使用后清零；日志不写入 token、key、SID 或显示名。

## 三、Windows 身份与授权

`packages/smb/windows` 通过 SSPI `Negotiate` 接受 SPNEGO token。每次 authentication exchange 独立取得 inbound credential；`AcceptSecurityContext` 必须建立 integrity-capable、非 null session 的 security context。完成后从 context token 读取用户 SID 与 `TOKEN_STATISTICS.AuthenticationId`，把二者组成授权身份；account display name 只用于诊断，不参与相等比较或准入。

标准组合先用 `CurrentIdentity` 捕获宿主进程 token 的 SID 与 logon-session ID，再把结果交给 `AllowIdentity` 作为 `Config.AuthorizeIdentity`，只允许精确匹配。每次初始 authentication 和 reauthentication 都在安装 principal／signer 或替换 expiry 前调用它；明确拒绝产生 access denied，无法决定产生 I/O failure。anonymous、Guest、LocalSystem、LocalService 与 NetworkService 明确拒绝。同一个 SID 的另一次登录不是同一身份，不能接管 `PreviousSessionId`、复用 session 或访问它的 tree。

SMB 本机身份与远端业务身份是两层独立保护。`AuthorizeIdentity` 决定一个已验证的操作系统主体能否建立或刷新 SMB session；端点随后把该 `Principal` 放进 request context，并用独立的 `Config.Authorize` 对 trusted volume 与实际 FileSession 操作作准入。`TREE_CONNECT` 依次授权 `file.session-open` 与 `file.status`，后台续期授权 `file.renew`，显式 tree/session 清理授权 `file.session-close`。backing remote storage 继续使用调用方为远端 authority 配置的身份和凭据，SMB SID 不替代它。

authentication exchange 受 `HandshakeTimeout` 约束，完成后的 identity 另受 provider 返回的 security-context expiry 约束。未完成的 exchange 到期后自主关闭，不等待下一次 `SESSION_SETUP` 才回收；reauthentication 使用新的 generation，旧 timer 不能关闭新的 exchange。identity 到期后，普通命令收到 signed `STATUS_NETWORK_SESSION_EXPIRED`，session、signer 与 tree 保留以允许 signed SESSION_SETUP 重新认证；只有同一 SID 与登录会话的成功 reauthentication 才更新 expiry 并恢复工作，LOGOFF 始终可以清理该 session。

## 四、资源与所有权

`Limits` 分别限制 export、connection、authenticated/preauthenticated session、tree、每棵 tree 的 handle、pending request、compound command、negotiate context、frame、I/O、authentication token 和一次目录观察的保留字节。`MaxHandles` 默认每棵 tree 256，`MaxDirectoryBytes` 默认 8 MiB；在权威打开前预留的槽位、活句柄和清理未确认的句柄均计入前者。`MaxIOBytes` 同时成为协商公布的 transact/read/write 上限；协商后完整 request 不能超过它加固定协议 envelope，control command 另受 68 KiB 上限且只能消耗一个 credit，数据命令的 credit charge 至少覆盖每个 64 KiB payload 单元。`FileSessionOptions` 继续限制 authority session 自己的文件、owner、range、等待与动作历史。所有值必须显式选择；零值不表示无界。

一次 connection 持有自己的 negotiate transcript、credit 集合、pending requests 和 session table。全局 session registry 另外限制所有 connection 的 session 总数；只从某个 connection map 删除 session 不释放这份全局 charge。TreeId 与 SessionId 单调分配并检查耗尽，已退休的 ID 不重新绑定新对象。

一个 volume tree 持有 export 引用和共享 authority session 的一份 ref，并独自拥有其 FileId 到保留引用的映射、句柄额度和文件工作栅栏。FileId 的 volatile 部分由 SMB session 单调分配，跨同一 session 的 tree 不复用；它表示一次打开，不是 R-WIN-4 的稳定对象标识。authority session 建立并验证一份有限 `FileSession`，在已确认 lifetime 内按保守期限续期；旧 revision 不延长 deadline，renew／authorization 失败使 authority session fenced 并进入清理。失败或结果未知的关闭继续保留实际尚被拥有的引用、export、session 与容量，直到同一拥有者重试成功，不能为了释放名额宣告清理完成。

每个 accepted request 有独立的 `RequestTimeout` 和 pending charge。response frame 在 session retirement 前登记为 signer 的使用者；只有 native/auth/tree 资源已经完成退休，并且最后一份 response 已完成构造与签名，session 才能从 registry 移除并清零 signing key。这样 `LOGOFF`、断线或超时不能在并发 response 仍使用 key 时销毁 signer。

## 五、关闭与可观测状态

`TREE_DISCONNECT` 先封住该 tree 的新文件工作并取消已登记的 pending request，等待已接纳的文件工作退出，再逐个关闭该 tree 拥有的 File/NodeReference，最后释放共享 authority ref。同一 export 的最后一个 tree 才关闭共享 FileSession。单个引用关闭失败不阻止其它引用的清理；`CloseWithResult.Released` 决定是否释放 FileId 和额度。未确认释放者由原 tree 持有并供清理重试；最后一个 tree 也可在共享 FileSession 的关闭已确认释放全部引用后解除这些槽位。`LOGOFF` 先发布 session retirement，取消除当前 LOGOFF 外的请求，关闭 authentication、全部 tree 和 orphan authority session，最后等待 response frame 释放。连接断开执行相同的无授权清理路径，不能把断线理解为资源已经释放。

`Export.Unpublish` 对任何 live tree 或正在取得该 export 的 connect／request 返回 busy，原 mapping、tree 与 FileSession 保持有效；只有没有使用者时才进入清理并移除 export。export cleanup 在加入 connection、session 或 authority 的清理序列前先核对该 owner 是否持有目标 export；session retirement bookkeeping 只检查已经退休的目标 owner。退休 session 仍有其它 authority 时，已经关闭的目标 authority 立即从该 session 移除并释放自己的 export ref，剩余 authority 继续承担 session retirement，不把不同 export 的清理绑定在一起。最后一个 authority 即使已经完成 native close，也继续保留 authority entry 与 export ref，直到在调用方 context 内取得 authentication lock，并以一个不可取消的 commit 同时移除 authority、记录 `resourcesClosed` 和释放 export ref。超时或取消保留这个已关闭 authority 作为 retry owner；不能丢失 owner 后让重试误报成功。等待同一 owner 的 authentication 或 cleanup 时服从调用方 context，不能被无关 session 或其它 export 的工作越过 deadline。`Server.Shutdown` 另行永久停止 listener 和新工作，等待 connection 退出，强制清理 tree／FileSession，移除已经静止并确认释放的 export，并保留失败者供重试。调用方提供的 backend 始终由调用方拥有，server 只关闭自己建立的 FileSession。

`Status` 报告 serving/stopping/stopped、仍发布或正在停止的 export、活跃及 cleanup-only connection、session、identity-expired session、tree、仍占额度的 handle、pending request、fenced authority session 与累计 cleanup failure。handle 数包含正在打开而已预留的槽位，以及关闭未确认而仍由 tree 持有的引用。identity expiry 表示本机认证需要重新建立；fenced authority 表示它已拒绝新用途但仍被 owner 保留，可能正在排空、等待 cleanup，也可能 native FileSession 已关闭而其它引用尚未释放。两者不能互相代替。状态报告实际仍被拥有的资源，不以协议表项已经删除代替 native cleanup 完成。

## 六、名字、打开与关闭

普通 volume tree 上的 CREATE 先验证 request 的固定长度、UTF-16 名字、offset、选项和有界 contexts。路径只解释为 tree 内的字面组件，不折叠 `.` 或 `..`，不修复尾随点／空格、保留设备名或数据流。Windows 本机大小写无关 ordinal 比较用于逐层选择；每层 `DirectoryMetadataObserver.ObserveDirectoryMetadata` 返回一份完整且有界的目录。只要这份目录包含非法 UTF-8、Windows 无法表示的名字或等价名字冲突，整个查找失败。根与每层观察在各自语义授权下取得；后续观察带上已取得的 ancestor guards。最终的 `ChildSelection` 包含 root 身份、目录 revision、已走过的边、目标的 SameNode 或 Absent 条件；既有目标的 `smb.windows` metadata 版本或缺席也随条件进入最终事务，使 READONLY 判断不会落在过期属性上。`OpenAt`／`OpenChildRef` 在最终权威事务中重新核对这些事实，冲突不能转成对旧路径或同名替代物的打开。

CREATE 在任何可能创建或截断的权威动作之前预留 tree handle 槽位和 session 内唯一的 FileId，并要求 FileSession 的 `AllocationReporting.CheckAllocationReporting` 通过预检。普通文件数据引用使用 `OpenAt`，目录和 metadata-only 引用使用 `OpenChildRef`；打开意图、初始 Windows metadata、Use claim 与目标效果进入同一个权威动作。读、写、删除 access 和 share 声明在客户端映射为中立 `UseClaim{Uses,Deny}`，由 authority 与其它入口的 claim 双向比较。句柄保存其自身获授 access，Use claim 本身不授予后续文件方法。CREATE 响应中的 create action、属性、EndOfFile、AllocationSize 和时间来自该动作返回的 `OpenOutcome` 与 `Attr`。SMB 端点采用 4096 字节的 cluster 几何，要求分配量已知、非负且按此粒度对齐；不相容的权威结果在 CREATE 效果前失败，不能把未知编码为零或从逻辑长度推导。中立的 `AllocationReporting` 只保证已知事实，不承诺 4096 字节粒度。内置 volume 以该粒度为每个普通文件维护已预留字节，目录及符号链接为零。未知的历史创建／变更时间投影为零，不从当前时钟猜测。Windows 特有的 READONLY、HIDDEN、SYSTEM 与 ARCHIVE 存在独立的 `smb.windows` opaque metadata 中，结构属性由节点种类投影；新建普通文件的 ARCHIVE 随创建原子提交。

打开返回错误但携带 File 或 NodeReference 时，tree 仍拥有该引用并执行清理。响应写出失败也不撤销已成功的权威打开；已分配 FileId 不能移交另一打开或按名字重建。权威结果未知时保留原 action ID 和输入，核对或重投原动作；不能以新动作再次创建或截断。CLOSE 根据 FileId 在所属 tree 中找到原引用，先阻止该句柄的新工作，再在当前授权下调用 `CloseWithResult`。`Released=true` 使引用与槽位退役，即使关闭同时报告语义错误；`Released=false` 保留清理责任、FileId 和额度，随后只允许关闭重试。CLOSE 的可选属性包含同一引用当前 `Attr` 的 EndOfFile 与已知 AllocationSize；属性无法取得或分配量未知时，已确认释放仍可用 `Flags=0` 报告关闭成功，不能返回零值冒充属性。无法确认释放必须如实报错。

可成功的控制命令还有 `NEGOTIATE`、`SESSION_SETUP`、`ECHO`、`TREE_CONNECT`、`TREE_DISCONNECT` 与 `LOGOFF`。普通 volume tree 上已识别但未实现的 FLUSH、READ、WRITE、LOCK、IOCTL、QUERY_DIRECTORY、CHANGE_NOTIFY、QUERY_INFO、SET_INFO 与 OPLOCK_BREAK 不执行 backing 操作，并返回 `STATUS_NOT_SUPPORTED`。SUPERSEDE、关闭时删除、reconnect 与未支持的 CREATE 效果在任何部分修改前明确失败。有效的 allocation-size context 在不支持预分配时被忽略，畸形 context 被拒绝；可选的 durable／persistent 与 lease 请求只有在不授予相应能力时才能与普通打开同时成功。CANCEL 的动作语义属于 range/async command 支持面；在该语义存在之前，CANCEL 与无法识别或畸形的 command fail closed，终止连接而不执行受控效果。

端点不提供 WNet 发布／移除流程，也不声称通过真实 Windows redirector 的可浏览、读写或缓存验收。

## 七、需求对应

- package、无进程级副作用和独立 Windows 依赖对应 R-INT-1 与 R-INT-2；
- 逐项资源上限、句柄容量、状态和 cleanup ownership 对应 R-INT-3、R-ERR-4、R-WS-7 与 R-WIN-10 的 endpoint 部分；
- loopback、SSPI identity、强制 signing、逐 FileSession 操作授权与 secret-free diagnostics 对应 R-SEC-1、R-SEC-4、R-SEC-5、R-WIN-1 与 R-WIN-9 的本机入口部分；
- guarded CREATE、身份保留、共享准入与 CLOSE 对应 R-FS-6、R-FS-8、R-FS-9、R-CC-14 与 R-WIN-2、R-WIN-5、R-WIN-6 的相应部分；
- 未实现命令的 fail-closed 边界对应 R-WIN-2 与 R-ERR-1、R-ERR-2。

R-FS-5 的系统报告身份、数据 I/O、R-CON、完整 R-CC-14、R-INT-8、目录枚举与通知、关闭时删除、范围锁、WNet lifecycle 和完整 R-WIN-2 至 R-WIN-10 仍需其它命令与真实 Windows 入口验收。
