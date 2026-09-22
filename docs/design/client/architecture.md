# client 角色

volume 的使用者。持有一份 remote storage，把 volume 呈现为 Linux 本地目录，并提供 Windows 本机 SMB 的安全会话端点；Windows 文件命令不在该端点的支持面。

本文只写 client 内部。基础 storage、保留文件、锁控制与 RPC 的边界见 [`../architecture.md`](../architecture.md)。

## 一、内部构成

| 组件 | 职责 | 需求 |
|---|---|---|
| **remote storage** `packages/transport/httprest` | 基础 storage 操作逐次转换为 HTTP 请求，不缓存内容。复制的订阅与快照使用独立长连接；`DialOptions` 限制 stream silence、body 与 admission，超时由调用方配置。 | R-INT-3、R-INT-5、R-INT-9 |
| **显式锁控制** | HTTP client 实现锁 Service，调用方保留 Session / Owner 与原动作身份，以 `WithScope` 构造独立、不可变的修改 proof 集合。控制请求具有独立预算。 | R-CC-3、R-CC-6 至 R-CC-11、R-INT-3 |
| **本地副本** `packages/storage/replicated` | 一个 storage 装饰器：按路径 `Stat` 走本地 SQLite，公开 `List` / `ListBounded` 与身份目录／名字观察在确认副本健康后回源 authority，其余操作也走远端。副本由 v4 变更流喂着，并为自己的目录树维护不可作为 authority 证据的本地 revision。 | R-CON-1~4、R-ERR-1、R-ERR-2、R-INT-3、R-SEC-3 |
| **挂载呈现层** `packages/fuse` | 把一份 storage 呈现为本地目录。持有 FileSession、已打开目录的 NodeReference、普通 File、UseOwner 与内核 owner 的映射；以父 NodeID 执行子项操作，目录 handle 以 Scope 捕获一次完整 Readdir，文件以 direct I/O 逐次读写。仅 Linux。 | R-FS-1、R-FS-5、R-FS-6、R-FS-8、R-CON-1~3、R-ERR-1、R-ERR-2、R-CC-12、R-CC-13、R-WS-5、R-INT-3、R-INT-8 |
| **本机 SMB 端点** `packages/smb`、`packages/smb/windows` | 接受 loopback 上的 Windows SMB client，执行 SMB 3.1.1 协商、SSPI 身份验证、强制消息签名以及 connection/session/tree/export 生命周期；每个 authenticated session 与 export 共享一份 FileSession。文件与目录命令返回不支持。 | R-INT-1~3、R-SEC-1、R-SEC-4、R-SEC-5、R-WIN-1、R-WIN-9、R-WIN-10 |
| **生命周期** | Linux mount 与 SMB Server／Export 的建立、停止和清理重试。 | R-WS-2、R-WIN-10 |

Linux mount 的数据路径如下；Windows endpoint 的内部层次见[本机 SMB 端点](smb-endpoint.md)。

```
   程序 ──▶ 内核 VFS
                │
                ▼
        ┌──────────────────────────────┐
        │          挂载呈现层          │
        │  每个打开对象一个服务端引用  │
        └──────────────┬───────────────┘
                       │ storage 接口
                       ▼
        ┌──────────────────────────────┐
        │           本地副本           │
        │  查名字与按路径属性走它      │
        └───────┬──────────────┬───────┘
                │              │ storage 接口
                │              ▼
                │      ┌──────────────────────┐
                │      │    remote storage    │
                │      │   一次调用一次请求   │
                │      └──────────┬───────────┘
                │ 变更流          │ RPC
                ▼                 ▼
```

挂载呈现层要求 `storage.FileStorage`，在建立 FileSession 前执行能力检查。本地具备这一能力的 storage 可直接挂载；它同样不知道底下那份 storage 有没有副本。随附的 localstore 与 Azure 服务端都提供 change log。集成方不提供日志时，复制入口以 `ENOSYS` 说明没有副本，此后每一次 metadata 查询都是一次远端请求。

remote storage 的 `DialOptions.MaxBodyBytes` 缺省为 1 GiB，限制 non-write 请求与 non-streaming response；`MaxWriteBytes` 在默认 options 中保持零值，拨号时继承 settled `MaxBodyBytes`，显式值必须为正且不大于它。`Write` 在发请求之前按 `MaxWriteBytes` 以 `EFBIG` 拒绝。读取 response 时先检查 `Content-Length`，再用 limit reader 检查实际字节数，因此错误或缺失的长度也不能绕过 `MaxBodyBytes`。过大的 `Read` 以 `EFBIG` 返回；过大的 listing、attribute、space 或 error message 是无法解码的协议答案，以 `EIO` 返回。无法安全计算四倍 response reservation 的 `MaxBodyBytes`，包括 `MaxInt64`，在拨号前被拒绝。

SSE 不把整个 stream 保存在内存里，但每一帧仍有独立的 `DialOptions.MaxFrameBytes`，默认 8 MiB。scanner 在读取下一帧时按这个值限制自己的 buffer；server 与 client 可以选择不同的值，实际可用上限由较小者决定，超出 client 上限的帧使 stream 失败。`remote-fs -http-max-frame-bytes SIZE` 把这个 client 上限交给部署方，使提高了 server frame 上限的 volume 仍能被挂载；它与 server 的同名 flag 接受相同的 1024 进制 suffix，显式非正值在连接前被拒绝。一个 stream reader 同时只保留一帧；`httprest.Storage` 不拥有调用方建立的 stream 数量，所以调用方仍须约束自己同时打开的 subscription 与 snapshot。

每个基础数据调用都要先取得 client 自己的 response admission。默认同时保留 64 份响应、允许 64 个等待者，aggregate 上限为 8 GiB；每份都按 `4 * MaxBodyBytes` 预留，覆盖 raw body、decoded listing 与转换过程的同时保留。默认 1 GiB body 使每个 List 预留 4 GiB，因此 aggregate byte bound 会先把并发压到 2 个活跃 List，另有至多 64 个调用等待。Subscribe、Resubscribe 与 Snapshot 在发出 HTTP 前也取得同一名额，用来约束 stream 尚未成功建立时可能返回的普通 error body；确认 `200 text/event-stream` 后立即释放，后续 frame 由 `MaxFrameBytes` 约束。等待者已满时，`Stat`、`Write`、`Create` 或 stream setup 都会在发出 HTTP 请求前以 `EAGAIN` 失败；context cancellation 会移除等待计数。non-stream admission 一直持有到 response 解码、mutation response/barrier 验证完成。`ReadBounded` 取 client 与调用方 byte bound 中较小者；`ListBounded` 把解码后的 entry 逐项交给调用方的 `ListResult`。普通 `Read` 与 `List` 仍返回完整 materialized value，但整个 HTTP body 及其同时表示都在上述单体与 aggregate 边界内。server 侧的 backend 预算与 response admission 见 [`../server/architecture.md`](../server/architecture.md#六请求与响应的内存边界)。

**平台呈现到这一层为止。** FUSE 把 Linux 内核请求翻译为基础 volume、FileStorage 和中立 identity/metadata/range 调用。已有 inode 的 Open 使用 OpenNode，Create 使用 OpenAt，Opendir 与 Readlink 使用 OpenNodeRef，Lookup 与名字修改使用 LookupAt/MutateName。挂载 preflight 分别要求 NamespaceAccess 与 DirectoryReader；普通 node 操作携带稳定父 NodeID，已打开的 directory handle Lookup 与 Readdir 再附带活 Scope，后者使用 ReadDirNodeBounded。远端会话只有在 v4 DirectoryMetadata bundle bit 为 true 时暴露 DirectoryReader；in-process DirectoryReader 不依赖 DirectoryMetadataObserver。OpenChildRef 是编程入口可用的原子子项引用能力。普通 fd 使用 File，无 fd 的身份属性使用 NodeReference 或 StatNode/SetNodeAttr。

Windows 的本机 SMB endpoint 与 FUSE 平行，不经过 Linux 副本或 inode 层。它只把一个受信配置的 share 绑定到 FileStorage，并把 SSPI 验证的本地 SID／登录会话放入授权 context；远端身份与 credential 仍由 backing remote storage 持有。协议、安全、资源和清理契约见[本机 SMB 端点](smb-endpoint.md)。文件命令、名字解析、Windows metadata、共享／范围语义、通知与 cache recovery 不在该 endpoint 的支持面。

### 业务身份与授权结果

HTTP client 的凭据附加和轮换由嵌入方提供；server 使用业务 context 中的稳定身份执行[volume 操作授权](../server/authorization.md)。一份 mount／replica 及其 Session、File、Owner、Grant 固定用于同一身份。同一身份轮换凭据可保持连接，切换身份须创建新的客户端状态，不能带走前一身份的缓存。

直接 SDK 保留普通 422 和 stream fault 中的授权 EACCES／EIO。强锁响应包含 lockCode 或 recorded 任一字段时必须完整通过 native 校验，不能用半份 native envelope 冒充普通授权错误；拒绝核对不把原未知动作改为未执行。初始 frame 与所有 Next 包装保留有效 errno，缺省 errno 的 generic fault、null／畸形／未知 errno 均为 EIO。

副本观察到任一 follower 失败后仍统一为 EIO；这个状态与直接 SDK 收到 EACCES 不同。服务端逐次出站检查权限，但已交付的元数据和字节无法收回；下一次检查或业务 context 取消被观察前，副本仍可能回答已同步内容。撤权不是客户端缓存擦除协议。

### 显式占有与修改 proof

remote storage 使用 HTTP v4，同时提供基础数据操作、中立文件能力与 Strong Service。调用方用 enrollment ticket 建立 Strong Session、创建 Owner、Resolve 现有普通文件并显式 Acquire；普通 FUSE Open 没有自动获取 Strong。Strong Session/Owner、FileSession/File/UseOwner、管理 Request、本地描述符、TCP 连接与复制 incarnation 分别拥有生命周期，断开连接不提前解除已确认保护。

成功 Resolve 返回资源引用的有限期限、当前 tick，以及这次有效控制活动延长后的 `HistoryExpiresMillis`。后者描述 Owner / Session 的动作核对窗口，不延长 grant，也不由资源引用的有效期推导。重复 Acquire 保留原 ResourceRef 全部字段，新的 Resolve 观测不能改写已经提交的意图。

`WithScope` 复制有界 proof 集合并共享 endpoint、HTTP client 与 admission；`Scope` 提供相同的有界 storage 视图。所有修改及 WithBarrier 方法携带该集合，普通读与有界读省略它。replicated storage 把 scope 传给远端修改，同时保留本地 metadata 查询及既有 confirmation barrier。锁控制继续使用显式 Owner 参数，不从 scope 推导新的控制身份。

Acquire 返回立即结果或 Pending 登记；Wait 是远端等待意图的期限，不占着一条 HTTP 请求等待授予。调用方用原 Request QueryAction 或 Cancel，SDK 不自动轮询、续期或制造新身份重试。已记录的 Rejected 与 typed error 一起返回，使冲突后来消失时仍能核对原结果；未接纳与结果未知分别处理。

GrantStatus 的剩余时间由服务端对未取整的 deadline 与 now 求差再向下取整。SDK 以原请求发送起点加这个间隔建立保守提示，旧 receipt 不开始新 lease，普通读取成功也不刷新提示。最终权限始终由服务端检查。原授权方退役后，旧意图返回退役或结果未知，不能在新授权方中重做；字段与取整规则见[文件锁协议](../server/file-locks.md#结果与期限)。

Strong 控制请求与响应固定至多 16 KiB；文件 metadata/range 控制使用独立的 256 KiB envelope。容量检查不占用数据 response 或复制 stream 的名额。state-changing control 进入 dispatch 后丢失响应时保持结果未知；只读核对遵循自己的取消边界。缺少 v4 marker、非法 scope 或不一致 receipt 都明确失败。服务端 Strong callback／native 生命周期的失败响应保持 native Unavailable／EIO、recorded=false 且无动作回执；该 wire 协议没有 EINTR code。SDK 本地在 HTTP Do 前接受的取消，以及只读控制在 client 侧接受的取消，仍可返回 EINTR，不经过 native wire 编码。业务策略拒绝另走普通 EACCES／EIO envelope。

## 二、路径起点来自副本，子项操作到达权威

内核的目录项超时、属性超时、负项超时都是 0。挂载根与路径起点可由本地 SQLite 副本定位；子项 Lookup 与 mutation 使用稳定父 NodeID 到达 authority。Opendir 建立的 directory handle 另持有 NodeReference/Scope，其 handle Lookup 与 Readdir 同时验证活引用。第一次 Readdir 通过 DirectoryReader.ReadDirNodeBounded 取得一次完整权威捕获并保存 stream，seekdir 与后续读取复用该捕获；普通应用枚举在 authority 的最终顺序执行 `ReadEntries` 检查。目录名字被删除后，exact scoped Readdir 仍访问原 detached 空目录；裸 NodeID 与 metadata observation 拒绝它。公开 List/ListBounded 继续先要求副本可用，再按路径回源。

挂载呈现层不向内核发送失效通知；副本不可用时，普通 volume 与文件 I/O 返回 EIO，文件会话的 action query、delete-intent query、续期和清理仍可联系服务端。副本仍消除初始路径 Stat 与负查找回源，但它不再决定已经取得的父 inode 后续修改落在哪个目录。

普通文件的 Open 与 Create 返回 `FOPEN_DIRECT_IO`。文件读取经过挂载层的健康检查与 `File.ReadAt`，不让同一 inode 的页缓存把旧 handle 内容交给新 handle。属性与返回区间来自同一次权威读取；direct I/O 不承诺共享 mmap 的完整行为。

副本以变更流的连续观察状态决定是否作答：流被观测为断开时作废，需要连续副本视图的操作以 EIO 失败（R-ERR-1、R-ERR-2）。没有过期时间或逐查询回源校验。普通续订在追到 opening tail 后恢复可用；首次构建与全量重建使用[快照后的固定回放目标](../../../.agents/notes/implemented/bug-fix/2026-09-07-gate-rebuilt-replicas-on-replay.md)。

构建先保留原订阅，再逐页写入快照。客户端读到 snapshot 的语义 EOF 后，先关闭该 HTTP 响应释放连接，再调用 Checkpoint 取得新鲜的 `(incarnation, committed position)`。快照位置不得早于原订阅的 opening tail；checkpoint 不得早于快照，且其 incarnation 必须与原订阅一致。snapshot 只报告捕获位置；incarnation 一致性由 checkpoint 与原订阅核对。

SQLite schema v8 为目录增加持久 revision 时，authority 原子清空无法携带该事实的旧 retained changes、切换 log incarnation 并把窗口位置归零。持有旧 incarnation／position 的 replica 不能续读这段已删除 history，沿既有 mismatch 分支重新取得当前树。HTTP v4 snapshot 与 change 不携带 authority DirectoryRevision；SQLite replica 为 revisionless 输入生成本地 opaque token，并在 replay create、remove 或 rename 时替换相应父目录 token。

本地 Seeding.Complete 成功后记录已安装的快照位置，唯一的 reader 随后沿原订阅丢弃已包含的事件、应用快照之后的事件，直到达到或超过固定 checkpoint。位置允许跳跃，不要求逐整数相邻；零位置且已达到目标时不等待一条不存在的事件。Complete 与每次 Apply 的成功位置都会在内部推进，即使副本仍以 EIO 拒绝查询；中途失败后的续订以已提交树的位置继续，而不是沿用更早的公开状态。最终追平前不清除失败状态。

`Options.ReplayTimeout` 必须为正，DefaultOptions 取十秒，从客户端观察到 snapshot EOF 时起覆盖响应关闭、Checkpoint、Complete 和目标回放。订阅握手与快照传输在这段预算之前，仍受各自 context 与既有边界约束。独立 CLI 继承此默认值；mutation 的 ConfirmationGrace 保持独立。超时、取消、流错误、化身不符或位置不相容使 gate 以 EIO 失败并保留原因；Checkpoint 的 ENOSYS 也不能被当作“不提供复制”而降级为直接模式。

构建的 Snapshot 与 Checkpoint 保留调用方 context 值；原订阅属于 storage lifetime 的子 context，构建期间临时关联调用方取消。失败由 reader 关闭订阅，取消回调只发出取消，不与 reader 并发 Close。成功前解除并等待临时取消关联，再确认构建与 lifetime 仍有效，最后恢复查询；成功返回后取消原构建 context 不切断订阅。固定目标之后的修改由正常 follower 接续，不移动目标追逐每个未来提交，也不把这一门槛描述为全局即时新鲜度证明。

SQLite `Replica` 自身的 `Stat`、`List`、`ListBounded` 先取得 SQL 读取名额，再进入共享读阶段。名额数与 reader pool 使用同一份 `Options.MaxReaderConnections`，默认 16；等待名额的调用不持有读阶段。两次等待都接受调用 context，等阶段失败时归还名额；查询结束时先退出阶段，再归还名额。组合层只用其中的 Stat 回答公开路径查询；公开 List/ListBounded 委托 remote authority。`Position` 不查询 SQLite，使用无取消的共享阶段，不占 SQL 名额。

私有读写门在共享读阶段与独占写阶段之间交接。写者登记后，新读者排队，现有读者排空后进入写阶段；写者退出时先为已经等待的有限读者批次预留名额，再唤醒它们，下一写者等待这些活跃或预留读者全部退出。等待取消撤回相应名额；门只保存固定数量的计数与共享通知状态，不保存逐等待者队列。`Apply` 与整次 `Reseed` 不占 SQL 读取名额，直接使用独占阶段，后者在 `Seeding.Complete` 完成事务或 `Seeding.Close` 中止时释放；`Add` 或 `Complete` 的前置校验失败仍须由调用方 `Close`。取得多项所有权时的顺序为 SQL 读取名额、replica 门、commit gate、database health lock，各入口只取得自己需要的部分。此机制保证阶段间交接，不承诺多个写者之间的 FIFO 或已进入操作的执行时长；取舍见[副本写者推进](../../../.agents/notes/implemented/bug-fix/2026-09-07-let-replica-writers-progress.md)。

**「流一断」是被观测到的，不是被假定的。** 服务端在无话可说时按固定间隔发一行心跳，这一层给「一个字节都没来」设一个数倍于心跳的上限，超限与流上任何一次失败走同一条路。没有这条，一条被切断的 TCP 与一个安静的 volume 是同一个观测结果 —— 沉默 —— 而副本会一直答下去，且没有时间上界。上限压在**正在等的那次读**上而不是压在连接上，因为首次同步期间没有人读变更流；计时由**字节**重置而不是由帧重置，因为一个快照分页可以是一整行一兆字节。

这份副本因此不是缓存。server 提供 change log 时，每个会产生日志的 mutation 在发出请求前先 admission 一条 fixed-size confirmation record，不保留 target path、direction 或 touched-name history。`ConfirmationGrace`、`MaxActiveConfirmations` 与 `MaxWaitingConfirmations` 默认分别为 10 秒、64 与 64；`remote-fs` 用 `-confirmation-grace`、`-max-active-mutation-confirmations` 与 `-max-waiting-mutation-confirmations` 暴露同一组设置。active 名额不足时有限等待；请求尚未发出时，纯调用方取消为 `EINTR`、deadline 为 `EIO`，实际容量饱和或 storage 开始关闭则以 `EAGAIN` 拒绝，原始原因被保留。规范的 `ENOSYS` 表明不提供复制时不建立副本，也不保留 confirmation state。snapshot EOF 后的 checkpoint／回放门错误统一为 EIO，即使原因链包含 ENOSYS，也不能触发无副本模式。

mutation 成功后，replicated client 从严格验证过的 response 取得 `(incarnation, position)` barrier，把它与当前 replica incarnation/generation 对齐，再等待本地 position 达到或越过它。event 先于 HTTP response 到达时当前位置已经足够，立即完成；另一个 writer 的较早 change 不能误确认本次 mutation，因为 barrier 不早于本次 commit。stream rebuild 改变 generation、barrier incarnation 不匹配、stream failure、调用方取消、storage 关闭或 grace 到期都以 `EIO` 报告「volume 已改变但本地副本无法确认」。失败只结束该调用，不把仍连续的 stream 单独判坏；迟到事件仍按 change-log 顺序应用。空 attribute change 与 rename onto itself 不产生日志，仍发送给 server 取得 pathname 的权威结果，但不预留或等待 barrier。

副本的建立、作废与恢复规则，以及写入方等待 mutation barrier 的原因，见[元数据复制](../../../.agents/notes/implemented/architecture/2026-08-27-metadata-replication.md)。

文件能力与 scoped 视图共同转发原 FileSession；File、NodeReference、目录捕获、current-name、action receipt、delete intent、metadata 与 range 控制不从名字副本重建。名字观察与目录捕获在确认副本健康后回源 authority；名字或属性修改使用同一远端 authority，并通过现有 confirmation barrier 核对 volume 进度。detached 对象修改不制造路径事件。

## 三、打开的是对象引用

一个打开的目录 handle 保存 `storage.NodeReference` 及其 Scope，一个普通文件 handle 保存 `storage.File`、访问方式和 Scope。Create 的 OpenAt 及 Opendir/Readlink 的 OpenNodeRef 从 FileSession action epoch 生成 `FileActionID`；已有 inode 的 Open 使用旧 OpenNode 协议。OpenAt 把存在性、身份/metadata 条件、创建、POSIX 初始 metadata、Use claim 和返回对象交给一次权威动作；已有文件的 `O_TRUNC` 由 OpenNode 在返回前完成，即使之后没有任何 write。OpenChildRef 保留给编程入口。

```
打开   ──▶ OpenAt / OpenNode，取得对象引用，不取内容
读取   ──▶ File.ReadAt，返回同一状态的属性与区间字节
写入   ──▶ File.WriteAt，同步确认指定区间的修改
截断   ──▶ File.Truncate，同步确认长度与内容
关闭   ──▶ 清理 owner 与引用
```

既有 fd 看到同一对象的后续修改。rename、unlink 或同名替换后，它继续指向原对象；新打开的名字可指向另一个对象。普通子项操作以父 NodeID 定位，已打开 directory handle 的 Lookup 还验证原目录 Scope，二者都不从旧父路径选择替代目录。`Getattr` 与 `Setattr` 优先使用已有 File/NodeReference；没有 fd 的 truncate 先按节点身份取得短期引用，再截断与清理。

普通 fd 写入按实际顺序组合，重叠区间以较后生效的操作为准。Open 注册读写所隐含的 Uses，但不自动获取 advisory range 或 Strong 权限；scope 只标识该引用。内部内容 revision 用于构造当前对象的补丁，不代表调用方携带了显式内容版本依据。

### 大小与物化预算

`Options.MaxFileSize` 默认 1 GiB，零值选默认，负值在挂载前拒绝。挂载把有效值传给 FileSession；服务端在每份捕获的 revision 被物化或暂存前检查会话与 volume 上限。写入终点或目标长度超限时整次以 `EFBIG` 拒绝。截断到零可以丢弃超限旧内容，不取回旧 body。

FUSE 不保存全文件缓冲区，objectstore 仍可能完整读取、重建不可变对象。每次读取与替换受[服务端物化预算](../server/file-handles.md#五http复制与资源)、transport body、对象存储与配额共同约束；提高某一层上限不会放宽其它层。只读 open 不取回超限内容，后续实际读取或非零截断仍在物化前拒绝。

## 四、中立 range 与 Linux 关闭语义

挂载启用 FUSE locks，raw bridge 将 `Getlk`、`Setlk`、`Setlkw` 映射到 FileSession 的 `UseOwners` 与 `RangeControl`。`flock` 选择 whole-file domain，使用 OFD owner，dup/fork 共享，最后一个共享 fd 释放；传统 POSIX 锁选择 record domain，使用该挂载内的内核 owner，关闭同一文件的任一 fd 都解除该 owner 的所有 record range。注册 record owner 时，FUSE 把内核给出的 POSIX PID 作为 caller-owned Diagnostic；冲突响应只返回该值，不暴露内部 UseOwner。`F_GETLK` 要求它在有符号 Linux `pid_t` 的 1 至 `math.MaxInt32` 内，缺失或越界为 `EIO`。相同 PID 不在独立挂载之间合并 owner。

两个 advisory domain 独立；flock EX 可用于只读 fd，POSIX 写锁要求可写。FUSE 用 `DropBeforeAcquire` 表达 flock 先释放再转换，用 `Replace` / `Subtract` 保留 POSIX 失败转换与范围代数。阻塞调用以短请求登记、查询和取消，在会话健康时可持续等待；单次 HTTP 超时不是整个锁等待的截止时间。取消核对证明没有遗留 grant 后才返回 `EINTR`。未知结果封锁整个挂载，普通操作持续为 `EIO`，直到重新挂载。完整范围与历史契约见[range 设计](../server/file-handles.md#四使用声明范围与-owner)。

`Flush` 对本次内核 owner 执行 record-domain Drop，`Release` 结束 File 引用及其 whole-file owner。它们不提交内容；`Fsync` 调用 `File.Sync` 检查已完成修改的健康与持久性。多次 Flush 不产生重复内容写入，最后 Release 的错误不能作为写入失败的唯一报告位置。

`Options.FlushTimeout` 用于文件清理与挂载建立，默认 30 秒，负值在挂载前拒绝。清理 context 忽略关闭线程的取消，保留请求值与较早 deadline；预算只限定清理尝试，不承诺 mutex 等待、内核 Unmount 或整个 Mount.Wait 的耗时。底层 storage 的 Close 仍由它的拥有者负责。

## 五、取消与故障分别作答

挂载层通过 `storage.ErrnoOf` 分类错误，`nil` 为成功。已接受的请求取消返回 `EINTR`；deadline、未知错误与无法证明修改结果的失败返回 `EIO`。go-fuse 的请求 context 被取消后，原 FUSE 请求仍得到回复。

文件／目录／symlink 创建、unlink、rmdir 与 rename 使用原子 identity operation。每个动作的随机 ID 在调用开始前固定；同 ID 重投不能重复产生引用、名字或删除义务。组合 Setattr 仍按 resize、共同时间、POSIX metadata CAS 分阶段执行；前段已经产生效果后，后续取消或失败以 `EIO` 保留部分效果，不能报告成未发生。只有 action receipt 明确 not-executed 时才可安全发起新的逻辑动作。

remote storage 在 HTTP `Do` 前接受取消时返回 `EINTR`，已发出的只读操作也可放弃读取。修改进入 dispatch 后，请求取消不能证明未执行；文件动作通过有界历史核对，仍不能确定的结果以 `EIO` 报告。打开的响应与确认失败必须清理或退役相应引用，不能留下调用方未知的无限引用。没有 Create／Truncate 的已有文件打开，在 ACK 为带 context.Canceled 的规范 EINTR、且同一能力的 Close 清理原始结果为 nil 时，返回无 File 的 EINTR；其余 ACK 失败保持 EIO，完整条件见[文件确认协议](../server/file-handles.md#五http复制与资源)。这不把任意打开变成可重试操作，也不改变丢失 ACK 的既有核对。成功修改未取得副本 barrier 确认时同样为 `EIO`。网络 errno 不进入 volume 错误链。

到达挂载层的错误不会改写成空目录或不存在。第二节保留的副本重建缺口仍可能使按名字查询作出过早的旧答案；文件 direct I/O 不替代副本连续性。分类与阶段判定见[请求中断](../../../.agents/notes/implemented/bug-fix/2026-08-22-eio-from-a-freshly-mounted-mountpoint.md)。

## 六、volume 答不上来的，挂载呈现层不代答

storage 契约有 NodeKind、共同时间与 opaque metadata，也有整个 volume 的容量，但没有属主和通用扩展属性接口。凡是内核问到而契约答不上的，挂载呈现层报错：

| 被问到 | 回答 |
|---|---|
| `chown` 改成挂载者以外的属主 | EPERM |
| 契约之外的其它属性 | EPERM |
| 扩展属性 | EOPNOTSUPP |
| `renameat2` 的 `RENAME_EXCHANGE`、`RENAME_NOREPLACE` | EINVAL |
| 硬链接 | 不提供（R-FS-4） |
| 类型无法命名的节点 | EIO |

`chmod` 将权限与 special bits 编码进 `posix.permissions.v1`，通过已有 File 或节点身份对该 namespace 作 CAS；`utimens` 修改共同时间。创建文件时的权限 payload 在权威打开中设置，目录创建后同样写入该 namespace。缺席 payload 只在展示层采用普通文件 `0644`、目录 `0755`、符号链接 `0777`，不写回 authority；存在但畸形时返回 `EIO`。属主是唯一一个报错的属性：volume 不带属主，挂载点把每个节点都报成挂载它的那个用户（R-SEC-1），因此把属主改成那个用户就是它已经是的样子，改成别人则无处存放。

目录的链接数一律为 1。

### 符号链接

FUSE 通过 MutateName 创建符号链接，并以短期 NodeReference 的 State 读取原始 link target。底层按 NodeKind 保存链接自身，不跟随目标：

| 对一个符号链接做 | 结果 |
|---|---|
| `lstat`、列目录 | 报告为符号链接，模式与长度都是链接自己的 |
| `readlink` | 返回引用同次 State 捕获的 link target |
| `stat`、`open`、读、写 | 内核先通过 `readlink` 解析；底层普通文件打开仍拒绝链接 |
| `rm` | 删掉链接本身，它指向的文件不动 |
| 改名 | 搬动链接本身 |

链接目标、Attr.Kind 与 Attr.Size 必须来自相容状态；目标缺失或无法验证时明确失败，不返回空目标。

## 七、容量

volume 报出自己的容量，挂载呈现层把它换算成内核要的块数：

| 内核要的 | 从哪来 |
|---|---|
| 块大小 | 固定 4096 字节。volume 按字节计量，块只是报出去时的计价单位 |
| 总块数 | volume 的总量 |
| 空闲块数 | 总量减已用，不为负 |
| 可用块数 | volume 报的「还能写入的量」 |
| inode 数、空闲 inode 数 | 都是 0。这里不数 inode，0 是「没有 inode 表」的报法，`df` 把它显示成没有这一栏，而不是显示成已经用尽 |
| 名字长度上限 | 255 |

三个块数都向下取整：装不满的一块不计入。

自己没有容量可报的 volume 以 `ENOSYS` 拒绝，这个拒绝原样到达调用方 —— `df` 说「功能未实现」，那是实话。三个数不能同时为真时是 EIO。两者都不换成编造的数字（R-WS-5、R-ERR-2）：FUSE 库对不作答的文件系统的默认回答是一个全零结构，读起来是一块没有剩余空间的盘，而先查空间再决定写不写的程序会照着它行事。

**默认配置下，这一整节只对挂载它的那个用户成立。** 别的调用方问 `statfs` 时，内核自己回答，回的是一个清零的结构，请求根本到不了这一层。于是 `sudo df` 与同机的其它本地用户看到的都是一块 0 字节、0 可用的盘 —— 那正是上一段拒绝去编造的那个答案，而挂载这一侧没有任何东西能改变它。

改变它的开关在主机上，不在本系统里：fuse 模块参数 `allow_sys_admin_access`（`/sys/module/fuse/parameters/allow_sys_admin_access`，默认关）打开后，初始 user namespace 中带 `CAP_SYS_ADMIN` 的调用方绕过这项检查，`sudo df` 于是问到这一层，读到的是真数字。因此这条代价是有条件的：默认配置下 `df` 是一条只对挂载者有效的通道，要让 root 也看得见，需要运维在主机上打开那个参数。

问容量这一次调用带自己的 2 秒截止时间，不用挂载点通用的操作超时：`df` 会走遍机器上的每一个挂载点，一个够不到的服务端否则会让整台机器上的 `df` 卡满那个超时。

### 配额在修改调用上裁决

`WriteAt` 与 `Truncate` 同步执行原生发布记账，超出配额以 `EDQUOT` 返回对应的 write 或 truncate。挂载不缓存剩余容量，也不在数据修改之前调用 Space；Statfs 仍独立查询容量。缩短只在发布成功后释放差额，失败保留原内容与收费，避免其它写者提前花掉尚未释放的字节。

失去名字但仍被 fd 引用的文件继续计入用量。最后引用退役、在途操作排空且物理释放完成后才回收容量；未知结果不能伪造空闲空间。机制与通用包装器的独立边界见[容量上限](../../../.agents/notes/implemented/architecture/2026-08-21-space-limit.md)。

## 八、节点 ID 直接成为内核编号

`storage.Attr.ID` 是不透明、非零且不会被复用为另一对象的 volume 内节点身份。挂载把它直接报告为 inode number；同一对象被其它挂载移动到此前未见过的名字时，也保持原编号。节点 ID 不由路径或宿主 inode 推算，不使用局部新编号补救下层错误复用。

挂载仍保留一棵名字成员树，用于同名查询复用、删除、改名和目录捕获结果清理。节点内的本地 serial 只区分本次 capture 开始前已知的成员与期间新发现的成员，不是对外 inode，也不由持久 directory revision 代替。相同名字返回不同 ID 或类型时替换成员记录；被覆盖的旧 inode 可以继续被 fd 引用，失去名字不使它变成新对象。

名字树随挂载结束清理，节点身份由 volume 保持。其它 client 的删除或替换在后续 Lookup/List 或 identity-bound Readdir 中被观察到；普通子项操作的父 NodeID 与已打开目录 handle 的 NodeReference/Scope 决定权威父对象，本地父路径只用于内核呈现。

两个随附 backend 都使用 SQLite 的持久节点身份。[宿主目录后端已移除](../../../.agents/notes/implemented/simplification/2026-09-08-remove-the-host-directory-backend.md)；第三方实现仍须满足 R-FS-5 与 R-INT-11，不能直接报告可能被复用的宿主 inode。

## 九、生命周期

一次挂载拥有一个 FileSession。`Options.FileSession` 未提供时使用默认 options，显式 options 在建立前验证；实际 MaxFileSize 与挂载大小界限一致。后台续期在上一份已确认 lease 内完成，成功状态只以保守的请求起点更新 deadline。authority 重启、会话退役或期限耗尽使 File、NodeReference、UseOwner、range 与有限 FileAction history 全部失效；挂载不按路径重开引用，也不自动重新取得 range。durable DeleteIntent 由新 session 按 ID 查询和继续清理，但不恢复旧 handle。节点事实和 opaque metadata 可由新会话再次读取，旧持有者连续性不能由这些持久事实推导。

`Unmount` 失败，例如仍有使用者而返回 `EBUSY` 时，会话继续续期。内核连接退出后，挂载停止续期并尝试排空全部引用；个别 Release 缺失也由会话清理覆盖。`Mount.Done()` 在这次清理尝试结束后关闭，`Mount.Wait()` 返回它的错误，Done 关闭不意味着清理成功。独立 client 等待 Done 后才释放 replica，释放失败保留其目录与错误。

提供 change log 的 volume 在初始快照完成前不可挂载使用，失败不降为直通查询。首次 `Subscribe` 仍使用 storage lifetime，`New` 的 context 取消不能终止这一步，详见[取消首次副本订阅](../../../.agents/notes/proposed/bug-fix/2026-09-07-cancel-initial-replica-subscription.md)。副本位于只允许属主访问的专属目录，`-replica-dir` 指定位置；只有清理成功才删除它。冷挂载仍需等待快照，R-WS-4 的冷启动成本保持独立。

## 十、能力边界

本地副本只复制名字与元数据；内容由保留 File 逐次读取，普通文件不使用内核页缓存。元数据缓存、负项和失效策略的后续选择见[内核缓存](../../../.agents/notes/proposed/architecture/2026-08-19-kernel-cache-and-unreachable.md)。

同步写入不在离线时返回成功，不把未知失败重试为新写入。原生补丁实现可在已知未提交的 revision 竞争后有界重试；这与应用重做一个结果未知的修改不同。普通 fd 没有隐含的内容版本前置条件，显式版本工作流仍独立。

目录子项 Lookup、mutation 与 Readdir 使用 NodeID 和可选 NodeReference Scope；reference current-name、完整有界 directory metadata observation 与持久 authority directory revision/guard 由[有界权威名字观察](../../../.agents/notes/implemented/architecture/2026-09-20-bounded-authoritative-name-observations.md)提供。guards 只约束回源的只读观察，不与 SQLite replica 的本地 revision 比较，也不进入名字 mutation 或当前路径遍历。目录 revision 不是通知游标，挂载层不据此实现缓存失效或恢复。

标准 advisory 通过中立 range 表达 flock 与传统 POSIX 范围锁，完整 `F_OFD_*` 和 mmap 行为不由此推出。enforced range 为其它平台保留，当前 Linux 不把它冒充 advisory。显式 S/X 仍单独取得，挂载不自动选择 Strong 策略。SMB endpoint 提供协议、安全会话与 share 生命周期；Windows create/share/disposition 映射、文件与目录命令、通知、WNet 发布和 cache 验收不在其支持面，不能据此宣称 Windows 支持完成。

## 十一、部署形态

Linux client 可作为库嵌入既有 daemon service，或作为独立二进制运行（R-INT-1、R-INT-4）。Windows SMB endpoint 只作为 package 嵌入调用方进程。两者作为库时都不注册信号处理、不写标准输出、不调用进程退出、不修改进程级设置、包加载时不产生副作用（R-INT-2）；日志只写入调用方给定的目的地，未给定则丢弃。

独立 client 用 `-max-file-size`、`-file-session-lease` 与 `-file-session-history` 配置文件会话，默认分别为 1 GiB、30 秒与 1 分钟。`-timeout` 默认 30 秒，约束单次远端交换或文件清理尝试；健康会话中的阻塞 range 等待可以跨多次交换。

只使用 remote storage 而不挂载是第三种用法（R-INT-5），这条路径不依赖 FUSE，因此不受 Linux 限制。
