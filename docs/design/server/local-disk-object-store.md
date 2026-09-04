# 本地持久对象存储

本文描述随附的本地持久 storage。它把一份 workspace 的对象、元数据、变更日志、恢复状态与所有权锁放在同一个本地目录下，对外仍实现顶层设计定义的 `storage.Storage`。server 如何选择并暴露它，见 [`architecture.md`](architecture.md)。

这份设计受 R-ERR-6～8、R-INT-3、R-INT-6、R-INT-13、R-WS-1、R-WS-3、R-WS-5、R-WS-6 与 R-SEC-3 约束。

## 一、组合与 API

`packages/storage/localstore` 是组合边界：

```
localstore.Store
  └─ objectstore.Storage
       ├─ localdisk.Objects       不可变对象、物理容量、恢复与独占锁
       └─ sqlite.Store            命名空间树、配额、对象状态与变更日志
```

`localstore.Open(ctx, Config)` 打开或初始化组合存储。`Config` 包含根目录、workspace 名、正数配额、变更日志窗口、`sqlite.ObjectLimits`、普通与 snapshot SQLite reader-connection 上限、integrity-record 上限、`localdisk.Options` 与 `objectstore.Options`。根目录必须预先存在；其绝对路径不能含 SQLite file URI 会解释的 `%`、`?`、`#`；workspace 为 1～`localstore.MaxWorkspaceBytes`（1024）字节；配额不得低于 4096 字节。effective pending-byte threshold 必须不小于 effective local maximum object size，使每个 local-disk 接受的对象都能单独进入 pending backlog。所有配置和相互关系在根目录被修改之前完成校验。

组合层总会把传入的 `localdisk.Options.CompositeInitialization` 置为 true，使初始化 intent 在 local-disk lifetime lock 下创建。`localdisk.Open` 返回的 `CompositeInitializationState()` 是 `NoCompositeInitialization`、`CompositeInitializationStarted` 或 `CompositeInitializationResumed`；standalone `localdisk.Objects` 的零值 option 不创建组合层 intent。

返回的 `*localstore.Store` 实现 `storage.Storage` 与可供 server 使用的 `storage.BoundedStorage`，并另外提供：

| 方法 | 作用 |
|---|---|
| `Log()` | 返回与命名空间修改同事务提交的 durable change log |
| `Sweep(ctx, limit)` | 删除至多 `limit` 个已记录的未引用对象 |
| `MaintenanceStatus()` | 返回最近一次后台清扫的时间、删除数与错误 |
| `Status(ctx)` | 合并逻辑空间、对象记录、本地磁盘与维护状态 |
| `Close()` | 停止维护，排空操作，关闭元数据，再释放磁盘所有权 |

`Status` 会查询每个 durable component，并把全部错误合并返回；结果同时带 effective `ObjectLimits`。字段只用于诊断，带错误的结果不构成一份成功的部分状态。独立 server 因此只在整个查询成功时打印状态。

## 二、目录格式

一份已完成初始化的根目录为：

```
ROOT/                         0700，归 server 的有效用户所有
  FORMAT                      对象格式、版本、store UUID 与校验和
  OWNER.lock                  lifetime ownership lock
  LOCALSTORE                  READY marker，含 store UUID 与唯一 workspace name
  metastore.sqlite            命名空间、配额、对象状态与变更日志
  metastore.sqlite-wal        SQLite 按需创建
  metastore.sqlite-shm        SQLite 按需创建
  objects/                    0700
    .put-k...                 durable Put recovery record
    .delete-k...              durable Delete recovery record
    00/ ... ff/               按 key 摘要首字节分片
      k...                    已发布对象
      .stage-k...             尚未完成清理的 staging name
```

所有持久文件是归 server 有效用户所有的普通文件。格式、锁、marker、元数据与对象文件为 `0600`；目录为 `0700`。打开已初始化的根目录时，缺少必须项、类型或 ownership 不符、符号链接替代文件、格式或校验和不合法都使打开失败；`objects/` 中无法识别的恢复项同样失败。SQLite 主文件、WAL 与 shared-memory 文件若仍是可信的 singly linked owner-owned regular file，打开会先把 mode 收紧到 `0600`；其它 durable component 的错误 mode 不被修复。

`LOCALSTORE.init` 只表示组合层正在进行第一次初始化；它不是 READY 状态，也不能让一个已有的、无法证明来源的对象库自动生成新元数据。local-disk 层先在 lifetime lock 下创建空 intent，组合层取得 store UUID 后把它原子替换为带 UUID、workspace name 与 checksum 的 binding。`LOCALSTORE` 是完成标记：只有对象格式、绑定后的 SQLite 数据库和这个 marker 都 durable 后，根目录才可以作为已初始化 store 重新打开。它把 root 永久绑定到一个 workspace name；以另一个名字打开会失败，不会在同一数据库中新建空 namespace。

## 三、初始化、READY 与重新打开

第一次 `Open` 按以下顺序建立可恢复状态：

1. 以目录描述符锚定根目录，校验根目录与祖先路径的保护条件。
2. `localdisk.Open` 取得根目录 lifetime lock。`FORMAT` 不存在时，它在同一把锁下 durable 地创建或验证 `LOCALSTORE.init`，再建立 `OWNER.lock`、`objects/` 和带随机 UUID 的 `FORMAT`，并对对象发布能力做探测。
3. 组合层核对 `localdisk.Open` 返回的初始化状态；一个非 READY store 必须是这次开始或恢复的已知初始化。随后以 staging file 与 rename durable 地把 intent 绑定到 store UUID 和 workspace，已有 binding 必须逐项匹配。
4. 以 `metastore.sqlite` 打开 `sqlite.OpenBound`；新数据库在创建 namespace 之前绑定 `FORMAT` 的 UUID，已有数据库必须已经绑定同一个 UUID。
5. 以 staging file、hard link 和根目录 `fsync` 发布 `LOCALSTORE`。marker 带同一个 UUID、workspace name 与自身校验和。
6. 删除 `LOCALSTORE.init` 并 `fsync` 根目录。此后 store 处于 READY 状态。

进程在步骤 2 至 5 之间终止时，下一次 `Open` 只恢复有合法初始化意图的已知中间状态。空 intent 只允许在 metastore 尚不存在时绑定；空 intent 旁已经出现数据库时，无法证明数据库属于哪个 workspace，打开失败。`FORMAT` 已存在却既没有合法 `LOCALSTORE`、也没有合法初始化意图时打开失败；READY store 缺少 `metastore.sqlite` 时也打开失败。初始化不会把一个不能证明来历的残存 object store 解释为空 workspace。

重新打开 READY store 时，持久身份必须形成一条完整绑定：

- `FORMAT` 中的对象存储 UUID；
- `LOCALSTORE` 中相同的 UUID 与唯一 workspace name；
- SQLite `backing_store` 中的数据库级绑定。

SQLite binding 覆盖数据库里的所有 namespace，因为其中所有 object key 都在同一 object store 中解释；READY marker 进一步把本地 root 限定为一个 workspace。组合层要求数据库恰好含这一条 workspace row，并在 `sqlite.OpenBound` 前后都核对；丢失该 row、出现另一条 namespace 或空 replacement database 都不会触发自动创建。已含 namespace 的未绑定数据库不能事后绑定；绑定后的数据库也不能通过普通 `sqlite.Open` 绕过校验。任一 UUID 或 workspace 不匹配都在 READY announcement 与 HTTP serving 之前失败。

SQLite writer 使用 WAL、单 writer connection、`BEGIN IMMEDIATE` 与 `synchronous=FULL`。打开时还会读回 `PRAGMA synchronous`，不是 FULL 就拒绝使用该 writer。命名空间修改、用量计数、对象状态和 change-log position 在同一个 SQLite transaction 中提交。普通 namespace/log read 与长期 snapshot 使用两个独立 reader pool，分别由 `MaxReaderConnections` 与 `MaxSnapshotReaderConnections` 限制实际 SQLite connection 数；零值各自选择 16，负值与 `math.MaxInt` 在打开数据库前被拒绝。任一池已占满时，后续读取等待该池的 connection 并遵从 request 或 snapshot context 取消，慢 snapshot 不占用普通/event read pool。

## 四、本地文件系统与路径信任

`localdisk.Open` 在修改根目录之前拒绝已知 remote filesystem：NFS、CIFS/SMB、9P、AFS、Ceph、Coda 与 NCP。根目录、`objects/`、每个 shard、最终对象、SQLite 文件、初始化 binding 与 READY marker 必须保持相同的 device 与 `statx` mount identity；对象树还逐项核对 filesystem type。mount identity 无法取得时以 `EOPNOTSUPP` 拒绝，bind mount 或 submount 不能把这些被核对的 entry 换到另一基底。未识别的 filesystem type 必须通过一次 hard-link publication probe：创建 source、第一次链接成功、第二次链接同一目标得到 `EEXIST`，然后清理并同步目录。通过 probe 仍不证明 crash durability；部署底层必须真实提供本地 `flock`、`linkat` 与 file/directory `fsync` 语义。

根目录自身必须由 server 的有效用户持有且 mode 为 `0700`。从 `/` 到根目录的每一级父目录都必须由 root 或该用户持有，并且满足以下任一条件：

- 其他用户不可写；
- 父目录带 sticky bit，下一层 entry 由 root 或 server 用户持有。

这项校验阻止信任边界外的 principal 在打开期间替换路径分量。root、同 UID 进程和管理员仍能改名或替换这些分量，属于部署信任边界；部署必须在 store 的整个 lifetime 内约束它们。组合层在打开各个 pathname-based component 前后比对根目录的 device 与 inode，但这不是跨整个运行期的 rename lease。

对象层同时对根目录描述符与 `OWNER.lock` 取得 non-blocking exclusive `flock`。第二个 opener 得到 `EBUSY`。锁从 `Open` 成功前一直持有到 `Close` 排空所有 storage 操作并关闭 metastore 之后，保证变更日志唤醒与磁盘恢复都只有一个 active owner。

## 五、key、分片与对象 envelope

对象 key 是 metastore 生成的不透明字节串，不是文件路径，也不是内容哈希。local-disk 实现接受至多 120 字节的 key，并用以下可逆、单射的布局寻址：

```
shard   = hex(SHA-256(key)[0])
name    = "k" + base64.RawURLEncoding(key)
object  = objects/shard/name
stage   = objects/shard/".stage-"+name
```

分片只限制单个目录的 entry 数量；完整 key 仍写进对象，不能从摘要推断。key 超过上限以 `ENAMETOOLONG` 拒绝，编码后的名字不得被解释成路径。

每个对象由固定 72 字节 header、原始 key 和 payload 组成：

| 字段 | 校验 |
|---|---|
| magic、version、header length、reserved bits | 必须是本实现认识的唯一取值 |
| store UUID | 必须等于 `FORMAT` |
| key length 与原始 key | 必须等于调用方请求的 key |
| payload length | 必须在 configured per-object limit 内，且与文件长度相等 |
| SHA-256 | 必须等于读回 payload 的摘要 |

`Get` 在返回任何 payload 之前校验 entry 是私有普通文件，并验证整个 envelope 与摘要。合法 shard 中缺少所请求的最终对象是 `ENOENT`；shard、对象类型、格式、身份、key、长度或摘要不可信是 `EIO`。`Put` 与 `Delete` 也先验证已经存在对象的 metadata、envelope、key 与长度，不能覆盖或清理一份格式无法辨认的文件；它们不读取 payload 来重算摘要。

`Open` 不遍历每个 shard 中的全部 payload。没有 recovery record 指向的对象在对应 `Get` 时才完成 checksum 校验；payload-only corruption 因此使受影响的读取失败，不使无关 workspace 操作或启动扫描整份数据。garbage deletion 可以在 envelope 可信时删除一份 payload 已损坏的对象。

## 六、durable publication 与删除

对象写入以 durable recovery record 包围实际目录修改。record 的操作与 key 完全编码在 `.put-k...`／`.delete-k...` 文件名中；文件创建时为空，内容不承载格式。`Put` 的顺序是：

1. 在 `objects/` 创建 `.put-...`，同步 marker file，再同步 `objects/`。
2. 在目标 shard 创建 `.stage-...`，写入 envelope、key 与 payload，`fsync` staging file 后关闭。
3. 用同一 shard 内的 `linkat` 把 staging inode 发布到 final name；目标已存在时得到 `EEXIST`，不会覆盖。
4. `fsync` shard，使 final name durable。
5. 删除 staging name，重新 `fsync` shard。
6. 删除 recovery record，`fsync objects/`。

`Delete` 先 durable 地创建 `.delete-...`，验证 final object，删除 final name 并 `fsync` shard，最后删除 marker 并 `fsync objects/`。final name 已不存在时，只要 shard 本身可信，删除仍收敛为成功；已有 shard 仍会被同步，使一次先前在 directory barrier 处失败的删除可以由重试完成。

`Open` 在接受请求前扫描有界数量的 recovery records。Put record 使 final object 保留、staging name 被删除；Delete record 使 final 与 staging name 都被删除；对应 shard 随后同步，最后 marker 被删除并同步。未知 entry、超过恢复上限、marker 与 key 编码不合法、缺少它所指向的 shard，都会使打开失败。

publication 已经发生后，任何无法证明 directory barrier 或 cleanup durability 的错误都会 poison 当前 `localdisk.Objects`。之后的读、写、删除、容量和状态调用继续返回 `EIO`，直到关闭并通过下一次 `Open` 的 recovery 得到可证明的磁盘状态。

一份文件内容的 namespace 写入按 `Reserve → Put → Commit` 进行。reservation 先 durable 地进入 SQLite；`Put` 完成上述对象 barrier；`Commit` 再用 FULL-synchronous transaction 同时更新路径、节点属性、逻辑用量、对象状态与 change log。`Write` 只有在 `Commit` 成功后才返回成功。因此：

- reservation 提交后、`Put` 得到确定结果前终止，留下不可自动删除的 reserved record；
- `Put` 返回错误时，`Quarantine` 把 reserved 转成 unresolved；错误不能证明该 key 下的对象由本次调用建立，因此它不进入删除队列。`EEXIST` 这类 object-key 事实不能被呈现为 namespace path 的事实，对外以 `EIO` 报告；
- `Put` 成功后 `Commit` 失败，`Abandon` 才把 reservation 转成 garbage，因为成功的 create-only `Put` 已经证明这次写入拥有对象；
- `Commit` 实际已落地但回复丢失时，收尾会看到 referenced 状态并拒绝改成 garbage，这次 `Write` 以 `EIO` 报告结果未知；
- `Commit` 返回成功后终止，重新打开得到同一个名字、属性、内容与 change-log position。

`Quarantine` 与 `Abandon` 使用 storage-lifetime context，request cancellation 不会取消这项收尾；收尾失败与原操作错误一起返回。unresolved 与 reserved 都持久保留，不因经过一段时间而变成 garbage；时间不是对象 ownership proof。garbage 只在 sweep 成功删除并忘记对象后才释放 pending admission room。

零字节文件不创建对象；其内容直接由 metastore 中没有 object key 的零长度节点表示。

## 七、容量与资源上限

本地持久 store 必须带正数 workspace quota。SQLite 精确记录已经被 namespace 引用的 payload bytes，并在改变引用与大小的同一 transaction 中以 `EDQUOT` 拒绝超额写入。reserved、unresolved 与 garbage object、envelope 与 recovery state 不计入逻辑 `Used`，但占用物理磁盘。

`sqlite.ObjectLimits` 另行限制一份 namespace 中 reserved、unresolved 与 garbage records 合计的数量和记录 payload bytes。单个请求的 payload 大于 `MaxPendingBytes` 时 `Reserve` 以 `EFBIG` 拒绝，因为任何后台工作都无法让它单独装进阈值；请求本身能装下、但现有 backlog 使新增记录越过数量或字节阈值时返回 `EAGAIN`。拒绝与插入在同一 write transaction 中完成，不上传对象，也不留下 reservation。

unresolved 没有自动恢复或删除路径。重复的未知 `Put` 结果可以占满 pending threshold，使后续写入持续返回 `EAGAIN`；状态查询会单独报出它们，系统不会为恢复 admission 而删除 ownership 未被证明的字节。

零值 fields 由 `ObjectLimits.Effective()` 换成默认值；负值与 `math.MaxInt64` 被拒绝，没有 unbounded 取值。commit、覆盖、删除和改名必须继续完成其权威状态转换，其中解除 live object 引用的操作可以把 garbage backlog 推到阈值之上；`Quarantine` 与 `Abandon` 也始终可用，且 reserved→unresolved 或 reserved→garbage 都保持 pending count/bytes 不变。`ObjectStatus.OverLimit` 只在 count 或 bytes 严格大于 effective threshold 时为 true；恰好等于阈值仍是 within-limit 状态，但新的 reservation 可能已经没有余量。over-limit 时新的 reservation 持续返回 `EAGAIN`，garbage 清扫与 namespace shedding 仍可进行，直到回到阈值内。limits 是 serving configuration，重新打开可以选择不同的有限值。

`Reserve` 的热路径只读取 indexed pending totals；namespace 打开与 `ObjectStatus` 执行完整性验证。每个 namespace 必须是从唯一 root 可达的一棵树：root 没有 incoming entry，每个非 root 节点恰有一个同 namespace 的名字，cycle、孤儿与跨 namespace entry 都是 `EIO`。`namespaces.used` 必须是非负整数，并等于对全部 regular-file size 做 overflow-checked streaming sum 的结果；object state 与 size 同样逐项验证。SQLite storage class 也逐列验证：entry/change name 是 BLOB，identity/key 是非空 text，标量为 integer，可空 change node/from fields 必须按 kind 成组出现，mode、size、position 与纳秒范围有效。任何会被 driver 强制成可信零值或改变 cursor order 的记录都以 `EIO` 拒绝。v1/v2 迁移在同一 transaction 内对全库执行这些检查，失败时 schema 与数据都不部分前进。

`sqlite.Options.MaxIntegrityRecords` 在 recursive CTE 之前限制一次验证接触的 namespace seed、node、relevant entry、object、log 与 change records 合计；entry 的 label、parent 或 child 任一接触 namespace 都计费。`DefaultMaxIntegrityRecords` 为 1,000,000，`MinIntegrityRecords` 为 3，只容纳一个 namespace seed、空 root 与 mandatory log row；零值选择默认，低于 3 与 `math.MaxInt64` 在数据库或 local-store root 被接触前以 `EINVAL` 拒绝。超出 serving limit 是 `EFBIG`，并要求显式提高配置；无法完成计量是 `EIO`。legacy migration 使用全库合计，不能让另一个 namespace 的工作量或 corruption 藏在这次打开的名字之外。

`Space` 组合两个同时成立的测量：

```
Total = configured workspace quota
Used  = SQLite 记录的 referenced payload bytes
Avail = min(max(quota - Used, 0), localdisk.Available)
```

`localdisk.Available` 从根目录所在 filesystem 的 `statfs` 读取 `Bavail`，再扣除 maintenance reserve、正在 publication 的物理 reservations，以及一份最大 envelope/key overhead；可用 inode 不足以再建立 object 与 recovery entry 时为零。任何一侧无法测量都会使整个 `Space` 失败，不能用另一侧的数值拼出成功答案。

在写 staging file 之前，`Put` 还会按 envelope、实际 key 与 payload 的总字节数预留物理容量。物理空间不足是 `ENOSPC`；workspace quota 不足是 `EDQUOT`。删除和后台清扫不受 payload publication reserve 阻挡，maintenance reserve 为它们和 SQLite 保留进展空间。

这是一项 admission measurement，不是随后一次写入必定成功的承诺。其它进程可以在采样后消耗空间，filesystem metadata、block rounding、copy-on-write 与 delayed allocation 也可能产生 `statfs` 没有归入 payload 的成本；最终 filesystem syscall 的 `ENOSPC` 仍是权威结果。in-flight reservation 只协调本进程并发 publication，maintenance reserve 通过收紧 admission 留出余量，但不被描述为底层 filesystem 无论如何都不能占用的硬分区。

默认边界为：

| 配置 | 默认值 |
|---|---:|
| 单个 local object payload | 1 GiB |
| 同时 admitted 的 local object operations | 64 |
| admitted object bytes 总量 | 2 GiB |
| maintenance reserve | 64 MiB |
| `Open` 检查的 recovery records | 4096 |
| pending reserved + unresolved + garbage objects | 4096 |
| pending reserved + unresolved + garbage payload bytes | 8 GiB |
| SQLite ordinary/event reader connections | 16 |
| SQLite snapshot reader connections | 16 |
| SQLite integrity records examined | 1,000,000 |
| 后台 sweep interval | 1 分钟 |
| 每次后台 sweep 的对象数 | 64（最大 1,048,576） |
| change-log floor / cap / age | 100 entries / 10000 entries / 10 分钟 |

`MaxInFlightBytes` 必须至少容纳一份最大对象及其 envelope/key，recovery-record 上限不得低于 operation 上限。等待 admission 的调用服从 `context.Context`；`Close` 拒绝新的 admission，并等待已经进入的调用离开。

## 八、维护、状态与关闭

`objectstore.New` 使用 `DefaultOptions()`（1 分钟 interval、64 个对象一批），`NewWithOptions` 接受显式覆盖；interval 必须为正，batch 必须在 1 到 `MaxSweepBatch`（1,048,576）之间。两者都启动一个由 storage lifetime 持有的 sweeper，并立即安排一次 startup sweep。成功的 `Write`、`Remove`、`Rename` 与把 reservation 改成 garbage 的 `Abandon` 都发送一个容量为 1 的触发信号，突发修改会合并；周期 ticker 也会触发，因此一次 transient failure 在 backend 恢复后无需重启或新 mutation 就会重试。一次只运行一个 sweep，metastore 通过 garbage-only 索引按创建时间取至多配置的 batch；正好删满一个 batch 时重新排队，继续以有界批次排空已经可达的 backlog。reserved 与 unresolved 不是清扫候选。对象删除成功后才调用 `Forget`；`Forget` 只接受 garbage 或已经缺席的 key，对 reserved、unresolved、referenced 以 `EINVAL` 拒绝，未知 state 以 `EIO` 拒绝，一个批次的检查与删除在同一 transaction 中完成。任一 component 的错误都保留记录以供下次重试。

`MaintenanceStatus` 保存最近一次已完成尝试的时间、删除数与错误。后续成功会清除旧错误。关闭会取消正在运行的 background sweep；这次尝试返回的 cancellation 及其错误链仍保存在状态中，不会被静默抹掉。

`localstore.Status` 同时报告：

- workspace 的 `Total`、`Used`、`Avail`；
- reserved、unresolved 与 garbage object 的数量和 payload bytes；
- pending-object 的数量/字节阈值，以及 backlog 是否 `OverLimit`；
- SQLite ordinary/event-reader 与 snapshot-reader connection 上限；
- SQLite integrity record work 上限；
- store UUID、物理可用字节、in-flight operations/bytes 与 recovery-record 数；
- effective sweep interval/batch 与最近一次后台 sweep 结果；
- durability failure 已 poison store 时的失败原因。

`localdisk.Status` 使用一份与 data-plane operation/byte admission 分离的 serialized control slot。普通操作与字节名额全部耗尽时，状态仍能读取准确的 in-flight 数；同时只允许一个 control operation，`Close` 等 data-plane 与 control slot 都排空后才释放 descriptors。

独立 server 的 SIGHUP 对 local store 启动带 2 秒 context deadline 的异步状态查询并写入 stderr，不重新计数、不修改状态，也不增加 unauthenticated HTTP status endpoint。成功输出分别报告 reserved、unresolved 与 garbage，pending-object thresholds、SQLite ordinary/snapshot reader-connection、integrity-record work 与 effective sweep interval/batch，并区分 backlog 在阈值内和已经 `OverLimit`；能单独装进 byte threshold 的 reservation 因 backlog 越界时以 `EAGAIN` 失败，单个 payload 自身超过 byte threshold 则是 `EFBIG`。同一时刻至多运行一次；查询运行期间已经被 signal loop 取出的 SIGHUP 不再启动一份并发查询，仍留在 signal channel 的一个信号可在本次完成后触发下一次。SIGINT／SIGTERM 会取消并等待 status goroutine，状态查询不占住 signal loop；已经进入的不可取消 filesystem syscall 仍须返回后才能完成等待。查询任一 component 失败时打印整次 status 失败，不打印一组看似成功的部分数字。

`Close` 先标记 storage 已关闭并拒绝新 namespace 操作，停止并等待后台维护，再排空已经进入的操作；随后关闭 SQLite，最后关闭 object store descriptors 与 lifetime locks。已经进入的 mutation 若在 worker 停止后才产生 garbage，其 durable record 由下一次 `Open` 安排的 startup sweep 接管。独立 server 先停止 HTTP admission、等待 handler 离开，再执行 `Close`，所以即使 graceful-shutdown deadline 到期，仍不会在 blocked request 使用 storage 时提前释放所有权。

## 九、独立 server 入口

本地持久形态的命令行为：

```
remote-fs-server \
  -listen ADDR \
  -local-store ROOT \
  -workspace NAME \
  -quota SIZE \
  [METASTORE OPTIONS] [LOCAL OPTIONS] [HTTP OPTIONS]
```

`-local-store`、`-dir` 与 `-blob-container` 三选一。`-workspace` 与 `-quota` 在 local-store 形态中必填；local-only flag 用在其它形态会在 storage 被打开前拒绝。对应默认值来自上一节与 server HTTP handler 的默认值，flag 只负责把显式覆盖传给拥有该限制的 package。省略 `-http-max-write-bytes` 时继承 `-http-max-body-bytes`；显式的零值与其它非正值一样被拒绝。local-store 在打开任何磁盘资源前验证 effective write 上限不大于 effective `-local-max-object-bytes`。`-http-max-body-bytes` 仍可更大，因为 listing、错误与其它 non-write body 使用它。

下列 local-store 与 metastore 资源 flags 与 package option 一一对应；`-max-pending-*`、两个 reader-connection flags、`-max-integrity-records` 与 `-sweep-*` 由 Azure 与 local 两种 metastore/objectstore-backed 形态共用，`-local-*` 只对 local store 有效。HTTP request、non-streaming response 与 replication-frame flags 及默认值由 [`architecture.md`](architecture.md#五复制那三个操作)和[同文第六节](architecture.md#六请求与响应的内存边界)定义。

| flag | option |
|---|---|
| `-local-max-object-bytes` | `localdisk.Options.MaxObjectBytes` |
| `-local-max-in-flight-operations` | `localdisk.Options.MaxInFlightOperations` |
| `-local-max-in-flight-bytes` | `localdisk.Options.MaxInFlightBytes` |
| `-local-maintenance-reserve-bytes` | `localdisk.Options.MaintenanceReserveBytes` |
| `-local-max-recovery-entries` | `localdisk.Options.MaxRecoveryEntries` |
| `-max-pending-objects` | `sqlite.ObjectLimits.MaxPendingObjects`；Azure/local 共用 |
| `-max-pending-bytes` | `sqlite.ObjectLimits.MaxPendingBytes`；Azure/local 共用 |
| `-max-reader-connections` | `sqlite.Options.MaxReaderConnections`；Azure/local 共用 |
| `-max-snapshot-reader-connections` | `sqlite.Options.MaxSnapshotReaderConnections`；Azure/local 共用 |
| `-max-integrity-records` | `sqlite.Options.MaxIntegrityRecords`；Azure/local 共用 |
| `-sweep-interval` | `objectstore.Options.SweepInterval`；Azure/local 共用 |
| `-sweep-batch` | `objectstore.Options.SweepBatch`；Azure/local 共用 |

server 在接触 local-store root 前先完成 HTTP option 校验并取得 listener，再执行 `localstore.Open` 与首次 `Status`。只有 READY marker、identity binding、recovery、容量和所有 component 状态都成功，并且 listener 与 signal handling 都已建立后，才打印 `serving ... at http://...`；accept loop 紧接着启动。启动失败会关闭已经取得的 listener 与 storage 资源，释放 lifetime lock，并把 cleanup failure 并入命令结果。

本地持久形态提供 metastore change log，所以复制 endpoints 可用；它与 `-dir` 不同，client 可以建立本地元数据副本。HTTP request、non-streaming response、replication frame 与 backend result budget 见 [`architecture.md`](architecture.md#五复制那三个操作)和[同文第六节](architecture.md#六请求与响应的内存边界)。
