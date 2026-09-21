# Agent Note: 安全且有界的本机 SMB 端点

Status: implemented

## 问题

Windows 系统网络驱动器需要一个由系统 SMB client 能够连接的本机端点。若文件适配先于协议安全与资源所有权落地，后续补签名、认证到期或 session retirement 会改变每个命令的入口和清理顺序；文件 handle、共享限制和删除义务都会建在不可依赖的会话上。

本机 listener 并不天然等于可信调用者。同一台机器上仍有其它用户、其它登录会话、anonymous、Guest 与 service account；只比较用户名或 SID 无法区分同一用户的另一登录会话。SMB signing key、SSPI context、pending response 和远端 FileSession 又分别跨越不同的调用边界，任何一个被提前释放都会把已认证 session 变成未验证请求或泄漏 authority 资源。

SMB frame、compound request、authentication token、connection、session、tree 与 pending request 都由不受信任的本机 peer 驱动。仅限制 socket 数量或在解码后检查长度，仍允许单条连接制造无界驻留。

## 决定

### 协议安全先于文件命令

`packages/smb` 提供可嵌入的本机 endpoint，只成功处理 SMB framing bootstrap、SMB 3.1.1 NEGOTIATE、SESSION_SETUP、ECHO、TREE_CONNECT、TREE_DISCONNECT 与 LOGOFF。已识别但未实现的 CREATE、CLOSE、READ、WRITE、FLUSH、LOCK、IOCTL、QUERY_DIRECTORY、CHANGE_NOTIFY、QUERY_INFO、SET_INFO 与 oplock work 不进入 backing，并返回 `STATUS_NOT_SUPPORTED`。CANCEL 的动作语义不在本决定内；CANCEL、未知 command 与畸形 request fail closed，终止连接而不执行受控效果。

Direct TCP payload 在分配前受 byte bound 限制。SMB2 header、compound offset/alignment、command count、negotiate context、UTF-16 和变长字段使用严格 decoder；解析结果借用一份被请求生命周期持有的 bounded frame。compound request 保留每个 command 的原始 bytes、MessageId、SessionId、TreeId 与 related 关系，协议层不把后续文件命令压成一个无上下文 callback。

端点只协商 SMB 3.1.1、SHA-512 preauthentication integrity 与 AES-CMAC signing。preauthentication transcript 使用实际收发的 wire bytes；SSPI session key 通过 SMB 3.1.1 KDF 派生 signing key。携带 SessionId 的 request 必须先验签；unsigned session request 的拒绝保持 unsigned，带 signed flag 但 MAC 错误或已建立 session 的 malformed reauthentication 得到 signed 拒绝。replay／DFS flag 不被静默接受。session retirement 等到所有已经登记的 response 完成签名后才销毁 key。

### SSPI 身份同时绑定用户与登录会话

`packages/smb/windows` 每次 Begin 使用 Windows SSPI `Negotiate` 建立独立 inbound authentication exchange。完成结果必须提供 integrity、非 null session、当前有效的 security-context expiry、可用 session key，以及 context token 的用户 SID 与 `TOKEN_STATISTICS.AuthenticationId`。AuthenticationId 编码为固定 16 位小写十六进制 logon-session ID。

`Principal` 的 authority identity 是 `(SID, LogonSessionID)`；display name 只用于诊断。`Config.AuthorizeIdentity` 是本地身份的强制准入点，在初始 authentication 和 reauthentication 安装 principal／signer 或替换 expiry 前执行。标准组合使用 `CurrentIdentity` 读取宿主进程 token，再以 `AllowIdentity` 只接受完全相同的二元组，并拒绝 anonymous、Guest、LocalSystem、LocalService 与 NetworkService。相同 SID 的另一登录会话不能替换 previous session 或取得它的 tree。token、session key 与 authentication buffer 在使用后清零，日志只记录 protocol state 和 opaque connection/session/tree 编号。

SSPI 身份只保护本机入口。Share 仍携带 host-selected volume identity 与调用方提供的 FileStorage；独立的 `Config.Authorize` 对该可信 volume 和实际 FileSession operation 作决定。backing remote storage 的远端身份和 credential 继续由调用方拥有，本机 SID 不被解释为远端业务身份。

### owner 层级决定清理顺序

Server 拥有 loopback listener、export registry、connection 和全局 session capacity。listener 只有在确认本地地址为 loopback 后才转移所有权；accepted peer 另行核对 loopback。Publish 校验 FileStorage 能力但不取得 backend 所有权。非强制 Unpublish 在任何 live tree 或 connect／request 已取得 export 时以 busy 保留原 mapping 和资源；没有使用者时才进入清理并移除 export。export cleanup 逐层跳过未持有目标 export 的 connection 与 session，只为已经退休的目标 session 运行 retirement bookkeeping；必须加入同一 owner 的 authentication 或 cleanup 时，等待服从调用方 context。Server shutdown 另行强制 fence 新工作、清理 tree／FileSession，并只移除已经静止且确认释放的 export。

connection 拥有 negotiate transcript、credit、pending request 和本连接的 session table；全局 registry 另外持有 session charge。authenticated session 拥有 SSPI identity、signer、tree 和每个 export 的 authority session。同一 session 连接同一 export 的多个 tree 共享一份 FileSession，tree 只是 SMB alias，不成为第二个远端 owner。

authority session 在 `file.session-open`、`file.status`、`file.renew` 与 `file.session-close` 的当前授权下建立、核对、续期和清理。它只接受同一 epoch 中单调前进的 status revision，以 response receipt time 消耗 Remaining；过期、fenced、retired 或 identity 改变都使它停止。续期失败先 fence 新 tree/work，再以独立 cleanup context 关闭；未知 cleanup 继续占用 export 与 session capacity。

TREE_DISCONNECT 取消已经登记到该 tree 的 pending request 并关闭一个 tree；同一 authority 的最后一个 tree 排空并关闭共享 FileSession。文件命令进入支持面时还必须在同一所有权下增加 per-tree work admission fence。LOGOFF 先发布 session retirement，取消其它 request，再关闭 authentication、trees 和 orphan authority。断线走同一 ownership 清理，不能仅删除协议 map。`Server.Shutdown` 永久拒绝新 accept/auth/tree work，关闭 listener 与连接，等待 admitted goroutine，然后重试全部未决 cleanup。已经失败的 cleanup 由原 Server、Export、session 或 tree 保留，可由后续调用重试。

### 每种累积资源独立有界

`Limits` 分开约束 export、connection、session、tree、pending request、compound command、negotiate context、frame bytes、I/O bytes、authentication token、handshake、request 和 cleanup 时间。协商后的 request 另受 `MaxIOBytes + fixed envelope` 约束；control command 固定不超过 68 KiB 且只用一个 credit，数据 command 按每 64 KiB payload 至少消耗一个 credit。authority FileSession 继续使用自己的文件、operation、waiter、owner、range 与 action-history 上限。调用方显式选择 defaults，单个零值不表示关闭限制。

frame bound 在读取 payload 前检查，token 和 context 在保留前检查，compound 和 request admission 在 dispatch 前检查。connection 饱和不分配第二份状态；session 的全局 charge 直到 native/authentication 资源与已登记 response frame 都退休才释放。Status 分别报告 identity-expired session、cleanup-only connection、已经拒绝新用途但仍被 owner 保留的 fenced authority，以及 cleanup failure，使本机身份到期与未完成的 authority retirement 保持可区分。

authentication exchange 有独立 expiry watcher。新的 reauthentication generation 复用同一 watcher；过期 generation 不能关闭后来的 exchange。已建立 identity 到期后，普通命令返回 signed session-expired 状态，signer、session 与 tree 保留以接受 signed SESSION_SETUP；同一身份重新认证成功才解除 fence，LOGOFF 仍可清理。provider 不响应取消时，`Server.Shutdown` 和断线仍等待它离开，不能遗弃 native context 后报告 cleanup 成功。

### 范围边界

本决定交付 Windows client 所需的 secure bounded SMB endpoint/session foundation，并只部分满足 R-WIN-1、R-WIN-9 与 R-WIN-10。它不交付可浏览或可读写 share，不声明 Windows network-drive support 已完成。

Windows name/metadata projection、authoritative resolver、CREATE/CLOSE/READ/WRITE/FLUSH、QUERY_INFO、directory enumeration、share-mode admission、delete-on-close 与 handle cleanup 保持在后续文件适配中。SUPERSEDE、disposition、rename、current-name traversal、CHANGE_NOTIFY、overflow/rescan、cache invalidation 和 byte-range LOCK/CANCEL 保持在后续 mutation/concurrency 工作中。WNet mapping、Windows VM lifecycle、真实 redirector 的身份／缓存／故障验收也不由本决定完成。

平台中立约束保持不变：SMB status、Windows name comparer、create disposition、本地主体和协议 handle 不进入 storage/HTTP schema；tree 保留 FileStorage 与 FileSession 的扩展位置，未来 FileId 绑定 retained File/NodeReference，不能由路径、connection、SessionId、TreeId 或 NodeID 直接充当。directory revision 仍只可比等，不是 notification cursor；当前端点不建立 watcher 或 TTL cache。

## 备选方案

**把 endpoint、CREATE 和读写命令一次交付。** 这样可以更早得到可浏览 share，但协议安全、session cleanup、文件身份和 Windows 操作映射会成为同一个审查面。任何 authentication 或 retirement 缺陷都会迫使文件 handle 所有权一起重写，因此不选。

**只比较 Windows SID。** API 更小，但同一用户的另一登录会话拥有相同 SID，不能满足 mapping creator session 的隔离，也无法安全处理 PreviousSessionId，因此不选。

**让每个 tree 建立一份独立 FileSession。** 实现直接，但同一 authenticated SMB session 对同一 export 的多个 alias 会获得彼此独立的远端 owner；LOGOFF、renewal 与容量回收需要重复协调，后续 handle cleanup 也无法沿 SMB session 统一排空，因此不选。

**把 signing key 与 TCP connection 绑定。** 查找更简单，但 signer 的真实拥有者是 authenticated session；session retirement、reauthentication 和 response frame 可以与 connection lifetime 交错，提前清零会使已接纳 response 无法签名，因此不选。

**接受 wildcard／LAN listener，再依赖认证阻止访问。** 认证不能消除意外网络暴露、端口扫描和部署责任；Windows 入口的目标是同机系统网络驱动器，因此 listener 和 accepted peer 都必须是 loopback。

## 后果

文件适配获得一条已经验证身份、消息完整性、share 归属、FileSession lifetime 与资源上限的命令入口。authentication timeout、LOGOFF 与并发 TREE_CONNECT、以及 signer 与 response frame 的竞态在加入文件 handle 之前已经有可执行回归。

代价是当前 endpoint 只能建立和拆除安全 session/tree，所有文件与目录操作仍明确失败。Windows 上的 listener/WNet 组合、实际系统客户端行为和缓存语义仍需后续原生验收；跨平台 Go 测试与 native SSPI 单测不能替代这些结果。

本决定接续[Windows 系统网络驱动器支持](../../proposed/feature/2026-09-16-windows-network-drive-support.md)、[中立元数据与访问控制](2026-09-16-neutral-metadata-and-access-controls.md)、[持久节点身份与原子文件操作](2026-09-20-durable-identity-and-atomic-file-operations.md)与[有界权威名字观察](2026-09-20-bounded-authoritative-name-observations.md)。完整协议和 runtime 契约见[本机 SMB 端点](../../../../docs/design/client/smb-endpoint.md)。
