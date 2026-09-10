# 打开的文件与 advisory locks

本文描述 `storage.FileStorage`、服务端保留的文件身份和标准 advisory locks。按路径的基础 volume API 见[顶层设计](../architecture.md)，显式 S/X 扩展见[文件占有](file-locks.md)，FUSE 的内核映射见[client 设计](../client/architecture.md)。决定与代价见[活跃文件句柄](../../../.agents/notes/implemented/architecture/2026-09-08-live-file-handles.md)。

## 一、身份与会话

[`storage.FileStorage`](../../../packages/storage/files.go) 在 `BoundedStorage` 之外提供 `CheckFileStorage` 与 `NewFileSession`。检查必须在挂载或提供能力前完成；不能用按旧路径重新打开来替代保留身份。随附实现通过 objectstore 与具有原生独占所有权的 SQLite metastore 提供此能力。只有共享数据库所有权的基础 SQLite API 继续提供路径操作，文件能力检查以 `EOPNOTSUPP` 拒绝。

`FileSession` 拥有有限时长、文件引用、在途操作和 advisory 状态。`OpenFile` 以路径解析目标，`OpenNode` 直接使用节点 ID；`StatNode` 与 `SetNodeAttr` 为目录及未持有普通文件引用的属性调用保留身份。`File` 是一个已打开普通文件的引用，路径变化不改变它的目标。打开不读取完整内容，也不自动取得 advisory lock 或 S/X grant。

| 打开条件 | 权威结果 |
|---|---|
| `ExpectedID` 与实际目标不符，或 `OpenNode` 的身份已不存在 | `ESTALE`，不改用同名新节点 |
| `Create` 且 `Exclusive`，目标已存在 | `EEXIST` |
| 非排他创建遇到并发创建者 | 打开已经存在的那个对象，保留它的创建模式 |
| `Truncate` | 需要写权限；截断与返回的身份属于同一次有序打开结果 |
| 普通文件引用用于目录或符号链接 | 分别为 `EISDIR`、`ELOOP` |

`FileOpenOptions` 至少要求读取或写入一种访问方式；排他创建必须同时指定创建。权限模式只用于新建节点。只读引用可以修改契约支持的 mode、atime、mtime；内容写入、截断和 POSIX 排他锁仍检查写访问。

`FileSessionOptions` 要显式选择有效值，调用方可从 `DefaultFileSessionOptions` 开始。默认 lease 为 30 秒、动作历史为 1 分钟、单文件大小为 1 GiB，每会话最多 4096 个引用、64 个活跃操作、256 个操作等待者、4096 个锁 owner、65536 个范围、1024 个 pending lock 与 16384 个锁动作。会话上限还受 volume 与 HTTP registry 的共享上限约束。

`Renew` 确认会话继续有效；`Status` 只观察，不续期。返回的 epoch、revision、剩余 lease 与 history 时间用于核对同一会话。client 从请求开始时刻计算保守的本地截止时间；旧响应、普通 I/O 成功、TCP 存活均不延长已确认期限。过期或旧 server epoch 的能力返回 `ESTALE`，未知结果返回 `EIO`，不会恢复到旧路径。底层发布检查仍是权限的最终判定者。

## 二、保留节点与回收

SQLite schema v5 在节点上保存 `detached` 与内容 revision。`Remove` 或覆盖目标的 `Rename` 移除名字；仍有引用的普通文件保留原节点及内容。原 fd 可继续读取、修改与查询这个对象，新路径指向的对象独立存在。volume 日志、快照与目录遍历只包含仍有名字的节点，脱离名字后的修改不制造虚构路径事件。

保留节点的内容仍属于 volume 的实际用量。最后一个引用先退役，在最终发布门处禁止新的修改授权；已经接纳的 I/O 排空之后才物理释放。最后释放在事务内处理用量、当前对象与待回收对象。已知未生效的容量拒绝保留引用供清理重试；结果不明时保留所有权并封锁后续使用，不能提前归还配额。

`SQLiteOptions.MaxRetainedFiles` 默认 65536，按 volume 共享。退役引用仍占物理名额，直到最后释放完成。满额时创建并打开必须在产生 volume 副作用之前拒绝。多个 objectstore 包装同一 volume 时使用同一份引用与 advisory 预算，不能借另建包装器绕过上限。

拥有数据库原生 EX 锁的启动过程先有界验证所有 volume，包括 detached 节点、对象关系和配额，再原子回收旧 epoch 遗留的无主节点。使用量、垃圾记录和提交代际通过同一个 Commit/Accept 结果生效。验证失败不先回收一部分；普通共享 opener 不执行这条回收路径。持久见证、数据库所有权和关闭失败的处理见[本地持久对象存储](local-disk-object-store.md)。

## 三、一次读取与一次修改

`File.ReadAt` 在同一份 `FileState` 下返回属性与区间字节，EOF 返回该状态的属性与空字节。多次读取可以看见同一对象后续已经完成的修改；一个成功返回的区间不会混合两份 revision。文件引用钉住节点，不钉住某个内容 revision。

objectstore 仍以不可变完整对象保存内容。读取先捕获节点状态，在分配前预留当前对象与返回区间的内存，再通过 `GetBounded` 取对象并切片。捕获的旧对象被并发替换并回收时重新读取状态；当前仍引用的对象缺失是 `EIO`，持续竞争耗尽有限尝试是 `EAGAIN`。节点引用没有消除[读取与清扫之间的竞争](../../../.agents/notes/proposed/architecture/2026-08-21-readers-in-flight-and-the-sweeper.md)。

`WriteAt` 只替换指定区间；`Truncate` 保留前缀，增长部分为零。一次修改读取当前完整状态，预留旧内容与下一份内容，构造替换对象，经 Reserve、不可变 Put 和原生 revision CAS 发布。对象上传不持有最终发布门。只有已知没有提交且暂存清理成功的 CAS 竞争才能重新基于当前状态尝试；真实故障或未知提交结果不会被重试掩盖。

最终事务同时核对内容 revision、节点身份、会话或引用的有效期、显式 S/X proof 和发布记账。检查与修改按同一次最终转换排序。上传开始时有效不代表上传结束时仍可发布；引用退役、会话失效或授权过期后，尚未取得最终授权的修改失败。原生 Commit/Accept 的未知结果继续为 `EIO` 并保留故障原因。

两个普通 fd 对重叠区间的修改可按实际提交顺序都成功，未被后一次修改触及的区间被保留。内部 revision CAS 负责拼接当前状态，不是对「打开时内容版本」的承诺；[显式内容版本工作流](../../../.agents/notes/proposed/architecture/2026-08-19-ordering-and-versions.md)仍有独立的调用方依据与冲突报告问题。

`Sync` 检查已完成修改的健康与持久性边界。直接 `File.Close` 退役引用并清理它记录的 flock，不推断 POSIX 进程 owner，也不提交本地 dirty 内容。每次 `WriteAt`、`Truncate` 的错误都落在对应调用上；之后的 `Close` 不能把已经返回成功的修改丢弃。

## 四、advisory 范围与 owner

[`packages/advisory`](../../../packages/advisory/coordinator.go) 在同一 volume 内协调两种独立的冲突域。advisory lock 只约束自愿参与的加锁者；不持锁的写入、截断、改名和删除照常遵守基础文件语义及显式 S/X 检查。

| 规则 | `flock` | 传统 POSIX `fcntl` |
|---|---|---|
| 范围 | 整个对象，SH / EX / UNLOCK | 包含端点的字节范围，读锁 / 写锁 / 解锁 |
| owner | open file description；dup、fork 共享，另一次 open 独立 | 同一 FileSession 内内核给出的进程 owner 与文件身份 |
| 关闭 | 最后一个共享描述符释放 | 该 owner 关闭同一文件的任一 fd 即释放其全部 POSIX 范围 |
| fork | 继承共享的 OFD 占有 | 子进程不继承父进程的 POSIX 占有 |
| EX 访问条件 | 只读 fd 也可取得 | 写锁要求可写 fd，读锁要求可读 fd |
| 转换 | 先解除旧占有，再尝试新模式 | 失败保留旧范围；成功后分割、合并和替换相关区间 |

表中的关闭是 Linux 描述符事件的语义。直接使用 File API 的调用方须对该进程关闭事件显式执行 `DropLocks(owner, POSIX)`；`File.Close` 没有 owner 参数，不能替调用方解除该进程在同一对象上的 POSIX 范围。FUSE Flush 负责携带本次内核 owner 执行这项清理，最终 Release 关闭引用与 flock。`FileSession.Close` 则退役该会话的全部状态。

`LockOwner` 是会话内的不透明数，零值有效；PID 只用于 `F_GETLK` 的诊断结果。两个挂载会话中相同 PID 不会合并 owner。范围终点可以是 `MaxInt64`，表示延续到以后增长的 EOF；负范围已由内核规范化后进入 FUSE bridge。无冲突查询明确返回未发现，不能编造一个 owner。

完整 `F_OFD_*` 语义不在兼容承诺内。内核交给 FUSE 的命令已经规范化，daemon 不能靠此接口可靠区分所有 OFD 请求，因此也不承诺逐命令辨识并拒绝它们。

`SetLock` 立即返回终态或 `Pending`；`QueryLock`、`CancelLock` 使用相同 owner、对象与 `LockRequestID` 核对。Request ID 由服务端动作 epoch 与随机 128-bit nonce 组成，重试保留原意图。相同 ID 的不同参数以 `EINVAL` 拒绝；已退役历史中的未知 ID 为 `ESTALE`，不重新执行。终态至少保留所声明的 History 时间，当前 epoch 的记录不能为接纳新动作而驱逐。

非阻塞冲突为 `EAGAIN`，容量不足为 `ENOLCK`，已经判定的 POSIX 死锁为 `EDEADLK`。阻塞等待的存活由健康且持续续期的 FileSession 决定，不采用强 S/X `Wait` 的有限等待意图。等待记录、owner、范围、动作历史与死锁图都有独立上限。取消只有在确认未授予或已经释放后才能成为 `EINTR`；若授予已经获胜，核对结果保留该事实。原生 advisory 层无法确定的锁结果封锁受影响持有者的 I/O，显式解除或关闭后才清除，不能静默重获锁继续执行。FUSE 对未知锁结果采用更大的终止范围：整个挂载停止续期、退役会话并持续报错，需要重新挂载；原 owner 的清理不使该挂载恢复。

默认 volume 上限为 1024 个会话、32768 个 owner、262144 个范围、262144 个动作、8192 个等待者和 65536 条死锁图边。所有访问同一原生 volume 的包装器共享这份 coordinator；配置不一致拒绝组合。会话 admission 分为四类：数据操作使用 `MaxOperations`，心跳、advisory 获取、核对与释放三个分区各允许 2 个活跃调用。各自满额以 `EAGAIN` 拒绝，新加锁与大文件 staging 都不能耗尽续期或释放名额。

## 五、HTTP、复制与资源

HTTP v3 增加 `file` 与 `file-control` 操作入口，保留基础 volume 与强 S/X 协议。请求以 operation 区分打开、身份查询、区间读写与锁控制；二进制路径和内容使用 JSON 的 base64 byte 字段。协议对未知、重复、缺席、null 或无关字段进行验证，所有结果仍携带 v3 标记与封闭 errno 词汇。FileSession 的时间间隔使用 Go duration 的整数纳秒表示，不能按强 S/X 的毫秒字段解释。

随机能力标识会话与文件引用。Open 结果在有限 PendingAck 时间内保留，client 收到能力后单独确认；无确认的引用被回收。数据修改、打开和改变锁状态的请求使用有界动作记录核对，过期历史不能让旧请求变成新执行。响应丢失后不能单凭请求 context 取消推断打开未发生或锁未取得。控制通道使用固定 16 KiB 上限与独立 admission，阻塞锁通过短的 Set/Query/Cancel 交换维持，不长期占用 HTTP worker。数据 JSON 在编码前核对 envelope 与 base64 后的总长度；区间读取在调用 backend 前为返回 envelope 扣除预算，不能只按原始字节数推断 body 大小。

`HandlerOptions.Files` 默认在整个 registry 内允许 64 个会话，每个会话分别最多保留 16384 个数据动作与 16384 个清理动作，PendingAck 为 5 秒；可接纳的会话 options 受 handler 上限约束。`Handler.Close(ctx)` 停止 admission，退役并排空它创建的 registry；backend 仍归调用方。独立 server 先排空 HTTP 请求，再完成 handler 清理，最后关闭自己拥有的 backend；清理失败不释放 backend 所有权。

replicated storage 同时转发基础与 scoped 文件能力，并保留原 remote session、对象和锁 owner。元数据查名字仍可使用副本；打开文件的属性与字节直接查询服务端保留对象。文件修改成功后，提供日志的 server 返回当时的权威 barrier，replica 等待同一 incarnation 的位置达到该值； detached 修改没有路径事件，barrier 仍可证明现有 volume 进度。没有日志的直接 HTTP client 不制造 barrier；需要复制确认却缺少 barrier 时以 `EIO` 失败。

volume 默认单文件上限 1 GiB，同时物化内容上限 2 GiB，最多 32 次 materialization、8 次状态竞争尝试，每次数据操作预算 30 秒。替换预留当前与下一份内容，读取预留完整对象与返回区间；不能只按 patch 的长度收费。单会话、transport body、backend 对象与配额可施加更紧的边界。有限预算在保留超限内容之前拒绝，已经持有的 reservation 在取消或已知失败清理后释放；未知发布或记账结果保留相应所有权并封锁。

独立 server 暴露 `-max-retained-files`、`-max-file-size`、`-max-file-staging-bytes`、`-file-operation-timeout`、`-http-max-file-sessions`、`-http-max-file-actions`、`-http-max-file-cleanup-actions`、`-http-file-open-ack-timeout` 与 `-file-session-lease/history`。其中 staging 预算覆盖同时物化的完整旧、新内容，至少容纳两份最大文件。其它上限由库 options 配置；部署形态仍为 localstore 或 Azure Blob + SQLite。

## 六、与显式 S/X 的关系

强 S/X 保护 volume 资源的内容、存在和身份，所有修改在原生发布处检查 proof。它不由 `Open`、`flock` 或 `fcntl` 自动取得。只拥有 S 的调用方也不能修改受保护内容；没有活动保护冲突时，普通匿名修改不必先取得 X。

经授权的 unlink 或覆盖会使旧的命名资源成为 `TargetGone`，原 S/X grant 不转移到同名新对象。已打开文件的保留身份与 advisory owner 随旧对象继续存在。S/X 的有限 lease、恢复 grace、管理动作核对与 FileSession 的存活和 advisory 等待分别计时；任何一者的成功都不能延长另一者。
