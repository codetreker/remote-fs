# 保留文件、metadata 与访问控制

本文描述 `storage.FileStorage`、服务端保留的文件身份、中立 metadata、使用声明与范围控制。按路径的基础 volume API 见[顶层设计](../architecture.md)，显式 S/X 扩展见[文件占有](file-locks.md)，Linux 的平台解释见[client 设计](../client/architecture.md)。既有引用与内容语义由[活跃文件句柄](../../../.agents/notes/implemented/architecture/2026-09-08-live-file-handles.md)拥有，新增能力的取舍见[中立元数据与访问控制](../../../.agents/notes/implemented/architecture/2026-09-16-neutral-metadata-and-access-controls.md)。

## 一、身份与会话

[`storage.FileStorage`](../../../packages/storage/files.go) 在 `BoundedStorage` 之外提供 `CheckFileStorage` 与 `NewFileSession`。检查必须在挂载或提供能力前完成；不能用按旧路径重新打开来替代保留身份。随附实现通过 objectstore 与具有原生独占所有权的 SQLite metastore 提供此能力。只有共享数据库所有权的基础 SQLite API 继续提供路径操作，文件能力检查以 `EOPNOTSUPP` 拒绝。

`FileSession` 拥有有限时长、文件引用、在途操作、use owner 和范围动作历史。`OpenFile` 以路径解析目标，`OpenNode` 直接使用节点 ID；`StatNode` 与 `SetNodeAttr` 为目录及未持有普通文件引用的属性调用保留身份。`File` 是一个已打开普通文件的引用，路径变化不改变它的目标。打开不读取完整内容，也不自动取得 advisory range 或 S/X grant。

| 打开条件 | 权威结果 |
|---|---|
| `ExpectedID` 与实际目标不符，或 `OpenNode` 的身份已不存在 | `ESTALE`，不改用同名新节点 |
| `Create` 且 `Exclusive`，目标已存在 | `EEXIST` |
| 非排他创建遇到并发创建者 | 打开已经存在的那个对象，保留它已有的 metadata |
| `Truncate` | 需要写权限；截断与返回的身份属于同一次有序打开结果 |
| 普通文件引用用于目录或符号链接 | 分别为 `EISDIR`、`ELOOP` |

`FileOpenOptions` 嵌入 `storage.OpenAccess`，共享 Read、Write、Create、Truncate、Exclusive 五项打开意图；ExpectedID、InitialMetadata 与 Use 是文件打开自己的参数。Check／CheckNode 连同身份、metadata 和声明验证它们，至少要求读取或写入一种访问方式；排他创建必须同时指定创建。InitialMetadata 只用于新建节点。只读引用仍可修改契约支持的共同时间与 metadata；内容写入、截断和 POSIX 写锁继续检查写访问。

打开自动把 Read/Write 转为 `ReadData` / `WriteData`，再与显式 Use 合并。`UseClaim{Uses,Deny}` 与同一节点上的其它 claim 双向比较；任一 Deny 与对方 Uses 相交时，新打开在取得引用前以冲突失败。Use 不授予 File 方法、业务权限或 Strong proof。

`FileSessionOptions` 要显式选择有效值，调用方可从 `DefaultFileSessionOptions` 开始。默认 lease 为 30 秒、动作历史为 1 分钟、单文件大小为 1 GiB，每会话最多 4096 个引用、64 个活跃操作、256 个操作等待者、4096 个 use owner、65536 个范围、1024 个 pending range action 与 16384 个范围动作。会话上限还受 volume 与 HTTP registry 的共享上限约束。

`Renew` 确认会话继续有效；`Status` 只观察，不续期。返回的 epoch、revision、剩余 lease 与 history 时间用于核对同一会话。client 从请求开始时刻计算保守的本地截止时间；旧响应、普通 I/O 成功、TCP 存活均不延长已确认期限。过期或旧 server epoch 的能力返回 `ESTALE`，未知结果返回 `EIO`，不会恢复到旧路径。底层发布检查仍是引用、Uses、range 与 Strong 权限的最终判定者。

### 可选接口

每种能力的 Check 方法检查完整包装链；不支持时，Check 和调用都返回 `EOPNOTSUPP`。

| 接口 | 当前责任 |
|---|---|
| `ScopedReference` | 为一个确切、仍存活的 File 返回不透明 `UseScope` |
| `MetadataAccess` | 按 NodeID 对一个 metadata namespace 作 CAS |
| `ReferenceMetadataAccess` | 通过保留 File 对一个 metadata namespace 作 CAS |
| `UseOwners` | 以有效 File scope 注册和退役 range owner |
| `RangeControl` | 查询冲突、批量编辑、核对、取消和按 domain 清理范围 |

当前能力集合不包含原子 OpenAt、NodeReference、身份 namespace 操作、引用状态、删除意图、条件文件修改、目录 metadata 或当前名字观察。HTTP v4 为它们保留协商位。server 对未实现的 facet 明确发送 false；同版本 client 接受并忽略自己没有实现的 true 值，方法是否存在仍由本地接口决定。预留位置只允许 server/client 在 v4 内错开升级，不构成当前行为承诺。

## 二、保留节点与回收

SQLite schema v6 在 v5 的 `detached` 与内容 revision 之外保存 `NodeKind`、可选 BirthTime / ChangeTime 和规范 metadata。`Remove` 或覆盖目标的 `Rename` 移除名字；仍有引用的普通文件保留原节点及内容。原 fd 可继续读取、修改与查询这个对象，新路径指向的对象独立存在。volume 日志、快照与目录遍历只包含仍有名字的节点，脱离名字后的修改不制造虚构路径事件。

保留节点的内容仍属于 volume 的实际用量。最后一个引用先退役，在最终发布门处禁止新的修改授权；已经接纳的 I/O 排空之后才物理释放。最后释放在事务内处理用量、当前对象与待回收对象。已知未生效的容量拒绝保留引用供清理重试；结果不明时保留所有权并封锁后续使用，不能提前归还配额。

`SQLiteOptions.MaxRetainedFiles` 默认 65536，按 volume 共享。退役引用仍占物理名额，直到最后释放完成。满额时创建并打开必须在产生 volume 副作用之前拒绝。多个 objectstore 包装同一 volume 时使用同一份引用、Use 与 range 预算，不能借另建包装器绕过上限。

拥有数据库原生 EX 锁的启动过程先有界验证所有 volume，包括 detached 节点、对象关系和配额，再原子回收旧 epoch 遗留的无主节点。使用量、垃圾记录和提交代际通过同一个 Commit/Accept 结果生效。验证失败不先回收一部分；普通共享 opener 不执行这条回收路径。持久见证、数据库所有权和关闭失败的处理见[本地持久对象存储](local-disk-object-store.md)。

## 三、一次读取与一次修改

`File.ReadAt` 在同一份 `FileState` 下返回属性与区间字节，EOF 返回该状态的属性与空字节。多次读取可以看见同一对象后续已经完成的修改；一个成功返回的区间不会混合两份 revision。文件引用钉住节点，不钉住某个内容 revision。属性包含 `NodeKind`、共同时间和 opaque metadata；平台权限不参与内容 revision。

objectstore 仍以不可变完整对象保存内容。读取先捕获节点状态，在分配前预留当前对象与返回区间的内存，再通过 `GetBounded` 取对象并切片。捕获的旧对象被并发替换并回收时重新读取状态；当前仍引用的对象缺失是 `EIO`，持续竞争耗尽有限尝试是 `EAGAIN`。节点引用没有消除[读取与清扫之间的竞争](../../../.agents/notes/proposed/architecture/2026-08-21-readers-in-flight-and-the-sweeper.md)。

`WriteAt` 只替换指定区间；`Truncate` 保留前缀，增长部分为零。一次修改读取当前完整状态，预留旧内容与下一份内容，构造替换对象，经 Reserve、不可变 Put 和原生 revision CAS 发布。对象上传不持有最终发布门。只有已知没有提交且暂存清理成功的 CAS 竞争才能重新基于当前状态尝试；真实故障或未知提交结果不会被重试掩盖。

最终事务同时核对内容 revision、节点身份、会话或引用的有效期、Use claim、强制范围、显式 S/X proof 和发布记账。检查与修改按同一次最终转换排序。上传开始时有效不代表上传结束时仍可发布；引用退役、会话失效或授权过期后，尚未取得最终授权的修改失败。原生 Commit/Accept 的未知结果继续为 `EIO` 并保留故障原因。

两个普通 fd 对重叠区间的修改可按实际提交顺序都成功，未被后一次修改触及的区间被保留。内部 revision CAS 负责拼接当前状态，不是对「打开时内容版本」的承诺；[显式内容版本工作流](../../../.agents/notes/proposed/architecture/2026-08-19-ordering-and-versions.md)仍有独立的调用方依据与冲突报告问题。

内容或长度修改由 authority 推进 `ModTime` 与 `ChangeTime`。`BirthTime`、`AccessTime` 和 `ModTime` 可按 `AttrChange` 显式设置；`ChangeTime` 不可由调用方指定，并在共同属性、metadata 或名字关联确实变化时由 authority 推进。零 `time.Time` 是合法显式值，nil 才表示不修改。

`Sync` 检查已完成修改的健康与持久性边界。直接 `File.Close` 退役引用及其 Use claim，不推断 POSIX 进程 owner，也不提交本地 dirty 内容。每次 `WriteAt`、`Truncate` 的错误都落在对应调用上；之后的 `Close` 不能把已经返回成功的修改丢弃。

### metadata namespace

`Attr.Metadata` 是 `map[string]OpaquePayload`。namespace 只允许小写 ASCII 字母、数字、点、下划线和连字符；每节点最多 16 项，key 最长 128 字节，单值最长 32 KiB，规范编码总长最多 64 KiB，版本 token 最长 64 字节。编码按 key 排序并带长度前缀；畸形、乱序、重复、超限或尾随数据使整份属性不可用。

`MetadataAccess.SetMetadata(node, namespace, expectedVersion, data)` 和 File 上的 `ReferenceMetadataAccess.SetMetadata` 都只改变一个 namespace。空 expected version 要求该 namespace 缺席；非空值必须逐字节等于当前 authority version。空 data 是存在的 payload。成功结果携带新分配的非空版本，保留其它 namespace，并推进 `ChangeTime`；已知条件不符以 `ErrConditionConflict` 证明没有修改。

metadata 返回值与 Attr 载入前先经过 `AttrResultBudget`。每 volume 的 SQLite `MaxMetadataBytes` 默认 64 MiB，包含当前节点、detached 节点和 retained change 中的规范 metadata；它独立于内容 quota、单节点上限和 HTTP 的驻留预算。

## 四、使用声明、范围与 owner

[`packages/advisory`](../../../packages/advisory/coordinator.go) 在同一 volume 内协调三个独立 domain。`DomainRecord` 与 `DomainWholeFile` 只约束参与者；`DomainEnforced` 的显式 policy 约束真实数据访问。普通 File 的 Use claim、owner、range、动作历史与会话生命周期都由同一个 volume coordinator 计量，不能通过再建包装器绕过。

公开 List/ListBounded 在 authority 的 native 读取顺序中派生 `ReadEntries`，因此 replicated wrapper 不能从本地目录副本直接回答。它先检查副本连续性，再把完整目录读取交给远端；失效副本仍以 `EIO` 失败，健康副本也不能绕过当前 deny。

| domain | 编辑 | 作用 |
|---|---|---|
| `DomainRecord` | `Replace`、`Subtract` | 传统记录锁的范围替换、分割和解除 |
| `DomainWholeFile` | `Replace`、`Subtract`，可选择 `DropBeforeAcquire` | 整文件 advisory 的转换与解除 |
| `DomainEnforced` | `AddExact`、`RemoveExact` | 独立 ClaimID 和显式 `DenySelf` / `DenyOthers`，约束 `ReadData` / `WriteData` |

`Bytes` 使用 unsigned Start 和正 Length；`Boundary` 使用独立 CutAt，不从零长度猜测 EOF。`RangeShared` / `RangeExclusive` 只表达冲突关系，核心不推断 SMB 或 POSIX 的重复获取、转换和解除政策。平台 adapter 负责选择命令，authority 负责按同一最终顺序执行冲突检查与受控 I/O。

`UseScope` 绑定一个确切 File 引用。`UseOwners.NewUseOwner` 同时核对 NodeID、scope、session 和引用存活；任意数值 owner 不授予权限。`OwnerReference` 随引用结束，`OwnerExplicit` 由调用方明确退役。Group 只合并同一 session 内的死锁参与者，不共享 claim、range 或 scope 豁免。

`RangeControl.GetConflict` 只查询一个实际冲突。`Apply` 一次接纳最多 64 条命令，返回的 Claims 与 Effects 也分别最多 64 项；完整回执在任何释放或授予前完成容量验证。`DropBeforeAcquire` 已释放的旧范围会记录在 Effects 中，即使随后的获取 Pending 或 Rejected，也不能把结果说成完全未执行。

`Apply`、`Query` 与 `Cancel` 使用原有 `LockRequestID` epoch 和 nonce。相同 ID 的不同 intent 以 `EINVAL` 拒绝；旧 epoch 中未见过的 ID 不重新执行。取消只有在结果证明没有遗留 grant 时才能成为安全的中断；授予已经获胜时返回该事实，结果未知时相关访问持续失败。

Use claim、owner、range、等待与动作历史只存在于当前 authority 的有界内存中。FileSession 退役、authority 重启或 incarnation 改变后，旧 File、scope、owner 和 range 均以 `ESTALE` 或相应不可用错误失效，不从 SQLite 或复制日志恢复，也不按同名或同 NodeID 对象静默重建。调用方须建立新 FileSession 并重新申请状态。只有独立 Strong S/X 机制具有自己的持久恢复保证。

FUSE 将 `flock` 映射到 whole-file domain，将传统 POSIX `fcntl` 映射到 record domain。内核 owner、PID 诊断、fork/dup、访问模式、转换及关闭规则都留在 FUSE：`Flush` 对对应 owner 执行 `Drop`，最终 `Release` 关闭引用。直接 File API 不推断 POSIX 进程 owner。完整 `F_OFD_*` 仍不在兼容承诺内。

默认 volume 上限为 1024 个会话、32768 个 owner、262144 个 range、262144 个 action、8192 个 waiter 和 65536 条死锁图边。会话数据、心跳、范围获取、核对和释放使用分开的 admission；数据物化不能耗尽续期与清理能力。

## 五、HTTP、复制与资源

HTTP 文件请求先执行[业务授权](authorization.md)，再读取或触碰 Session、File 与动作历史。Open 的读写、创建、截断意图通过 `AccessRequest.Open` 交给一次 callback；InitialMetadata 与 Use 先通过协议验证，再由获准的 native open 原子执行。metadata 修改、range apply 和 range drop 使用各自的规范 Operation。已有 bearer 引用不绑定业务身份，也不能绕过检查。被拒绝的 ack、renew、核对或 close 不产生对应副作用，服务器自主 expiry／shutdown 回收仍由原拥有者执行。

HTTP v4 统一转发基础 volume、中立 Attr、metadata、文件引用、range 和强 S/X。请求的 `op` 直接使用 `storage.Operation` 的规范值；二进制内容、metadata version 和 payload 使用 canonical base64。协议拒绝未知、重复、缺席、null 或无关字段，所有结果都携带 v4 marker 与封闭 errno 词汇；v3 路由不提供兼容旁路。

当前 server 的 session 能力结果只宣告 Metadata、Owners 和 Ranges，File 能力只宣告 Metadata 与 Scope。wire 结构还保留 DirectoryMetadata、ReferenceName、AtomicOpen、Namespace、References、State、Delete 和 Conditional bool；没有相应 Go 方法的 v4 client 接受这些已知预留字段为 true，但不会调用它们。任意未知字段仍是协议错误。

同一兼容形状还允许 Open/OpenNode response 携带可选 `node`，以及 File.Close/FileSession.Close response 携带可选 barrier。当前 server 不依赖这些字段表达结果；不使用它们的 v4 client 可以安全忽略。它们分别为身份结果与可能产生持久修改的清理预留位置，不能被解释为对应能力已实现。

随机能力标识会话与文件引用。Open 结果在有限 PendingAck 时间内保留，client 收到能力后单独确认；无确认的引用被回收。数据修改、打开和 range 状态请求使用有界动作记录核对，过期历史不能让旧请求变成新执行。响应丢失后不能单凭请求 context 取消推断打开未发生或 range 未授予，已有的 ACK 丢失核对路径继续执行。

普通已有文件的 Open 已返回能力、但 ACK 失败时，client 先用返回的同一 Session／File 能力执行 Close 清理。只有 Create 与 Truncate 均为 false、原 ACK 错误同时满足 `errors.Is(err, context.Canceled)` 与 `storage.ErrnoOf(err) == EINTR`，且该次清理的原始 error 为 nil，才不返回 File 并保留 EINTR。清理的 ESTALE 不能当作成功，判定发生在既有 ESTALE 抑制之前。带创建／截断意图、deadline、未知 ACK 或会话故障，以及清理 EIO／ESTALE 或独立错误，仍返回无 File 的 EIO；原有创建或截断效果不被解释成未发生。已终止的 native 引用不会由迟到 ACK 或原打开动作的重放重新创建。这项分类不增加重试。

文件控制通道使用独立的 256 KiB envelope 上限和 admission；Strong 控制继续使用自己的 16 KiB 上限。阻塞 range 通过短的 Apply/Query/Cancel 交换维持，不长期占用 HTTP worker。数据 JSON 在编码前核对 envelope 与 base64 后的总长度；区间读取和 Attr/metadata 结果在调用 backend 或载入变长 payload 前扣除返回预算。

`HandlerOptions.Files` 默认在整个 registry 内允许 64 个会话，每个会话分别最多保留 16384 个数据动作与 16384 个清理动作，PendingAck 为 5 秒；可接纳的会话 options 受 handler 上限约束。`Handler.Close(ctx)` 停止 admission，退役并排空它创建的 registry；backend 仍归调用方。独立 server 先排空 HTTP 请求，再完成 handler 清理，最后关闭自己拥有的 backend；清理失败不释放 backend 所有权。

replicated storage 转发 metadata、scope、owner 和 range 能力。路径节点事实与 opaque metadata 进入 SQLite 副本；公开 Stat 可由副本回答，公开 List/ListBounded 回源 authority。File、Use claim、owner、range 和控制历史仍属于远端 authority/session，不写入副本。File 的属性、字节、scope 和 range 控制直接访问 authority。metadata 与文件修改成功后，提供日志的 server 返回当时的权威 barrier，replica 等待同一 incarnation 的位置达到该值；detached 修改没有路径事件，barrier 仍可证明现有 volume 进度。没有日志的直接 HTTP client 不制造 barrier；需要复制确认却缺少 barrier 时以 `EIO` 失败。

volume 默认单文件上限 1 GiB，同时物化内容上限 2 GiB，最多 32 次 materialization、8 次状态竞争尝试，每次数据操作预算 30 秒。替换预留当前与下一份内容，读取预留完整对象与返回区间；不能只按 patch 的长度收费。单会话、transport body、backend 对象与配额可施加更紧的边界。有限预算在保留超限内容之前拒绝，已经持有的 reservation 在取消或已知失败清理后释放；未知发布或记账结果保留相应所有权并封锁。

独立 server 暴露 `-max-retained-files`、`-max-file-size`、`-max-file-staging-bytes`、`-file-operation-timeout`、`-http-max-file-sessions`、`-http-max-file-actions`、`-http-max-file-cleanup-actions`、`-http-file-open-ack-timeout` 与 `-file-session-lease/history`。SQLite 的 metadata 总量由 `MaxMetadataBytes` 配置；staging 预算覆盖同时物化的完整旧、新内容，至少容纳两份最大文件。其它上限由库 options 配置；部署形态仍为 localstore 或 Azure Blob + SQLite。

## 六、与显式 S/X 的关系

强 S/X 保护 volume 资源的内容、存在和身份，所有修改在原生发布处检查 proof。它不由 Open、Use claim 或 range 自动取得；有效 Strong proof 也不豁免 Uses/Deny 或 enforced range policy。只拥有 S 的调用方不能修改受保护内容；没有活动保护冲突时，普通匿名修改不必先取得 X。

经授权的 unlink 或覆盖会使旧的命名资源成为 `TargetGone`，原 S/X grant 不转移到同名新对象。已打开 File 的身份、Use claim 与 range owner 随旧对象继续存在。S/X 的有限 lease、恢复 grace、管理动作核对与 FileSession 的存活和 range 等待分别计时；任何一者的成功都不能延长另一者。
