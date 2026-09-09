# 本地持久对象存储

本文描述随附的本地持久 storage。它把一份 workspace 的对象、元数据、变更日志、文件占有恢复证据与所有权锁放在同一个本地目录下，对外提供路径操作和持续绑定节点的文件句柄，并向 server 提供与原生提交绑定的文件占有 authority。server 如何选择并暴露它，见 [`architecture.md`](architecture.md)；句柄与占有分别见[文件句柄](file-handles.md)和[文件占有](file-locks.md)。

这份设计受 R-FS-6～8、R-CC-3、R-CC-6～13、R-ERR-6～8、R-INT-3、R-INT-6、R-INT-13、R-WS-1、R-WS-3、R-WS-5～7 与 R-SEC-3 约束。

## 一、组合与 API

`packages/storage/localstore` 是组合边界：

```
localstore.Store
  └─ objectstore.Storage
       ├─ localdisk.Objects       不可变对象、物理容量、恢复与独占锁
       └─ sqlite.Store            命名空间、保留节点、配额、变更日志与最终发布
            ├─ advisory          句柄与建议锁的 namespace 资源域
            └─ locking.Authority  强 S/X 占有；恢复证据由组合层绑定
```

SQLite 的 schema、数据库状态、原生 lease 证据与日志组件由[内部模块设计](sqlite-modules.md)定义；本页拥有它们组合后的持久化与生命周期。

`localstore.Open(ctx, Config)` 打开或初始化组合存储。`Config` 包含根目录、workspace 名、正数配额、变更日志窗口、`sqlite.ObjectLimits`、普通与 snapshot SQLite reader-connection 上限、integrity-record／name-byte 上限、`MaxRetainedFiles`、`advisory.Config`、`localdisk.Options`、`objectstore.Options`、`Locks *locking.Options` 与 `InitializeLocks`。根目录必须预先存在；其绝对路径不能含 SQLite file URI 会解释的 `%`、`?`、`#` 或 NUL；workspace 为 1～`localstore.MaxWorkspaceBytes`（1024）字节；配额不得低于 4096 字节。effective pending-byte threshold 必须不小于 effective local maximum object size，使每个 local-disk 接受的对象都能单独进入 pending backlog。workspace、quota、log window、local-disk、SQLite、句柄与占有上限，以及两者的 byte-limit 关系在根目录被修改前校验。

`Locks` 启用与 SQLite namespace 成对的 authority；首次建立占有状态或继续匹配的初始化 intent 还须显式设置 `InitializeLocks`。已绑定 root 重新打开必须提供 `Locks`，省略它不能关闭保护。真正尚未绑定的组合存储仍可由库调用方按原始 storage API 使用，但没有可供 server 接受的占有 authority。

组合层总会把传入的 `localdisk.Options.CompositeInitialization` 置为 true，使初始化 intent 在 local-disk lifetime lock 下创建。`localdisk.Open` 返回的 `CompositeInitializationState()` 是 `NoCompositeInitialization`、`CompositeInitializationStarted` 或 `CompositeInitializationResumed`；standalone `localdisk.Objects` 的零值 option 不创建组合层 intent。

返回的 `*localstore.Store` 实现 `storage.Storage`、`storage.BoundedStorage` 与 `storage.FileStorage`；配置 `Locks` 后，server 通过 `locked.New` 验证并使用这份 namespace 与 authority 的配对。它另外提供：

| 方法 | 作用 |
|---|---|
| `Log()` | 返回与命名空间修改同事务提交的 durable change log |
| `LockService()` | 返回与本 namespace 原生发布绑定的文件占有管理服务 |
| `NewFileSession(ctx, options)` | 创建有界文件会话，打开与持续访问同一节点；过期或退役后不按路径重开 |
| `Sweep(ctx, limit)` | 删除至多 `limit` 个已记录的未引用对象 |
| `MaintenanceStatus()` | 返回最近一次后台清扫的时间、删除数与错误 |
| `Status(ctx)` | 合并逻辑空间、对象记录、本地磁盘、checkpoint 与维护状态 |
| `Close()` | 停止维护，完成元数据 checkpoint 与关闭，再释放磁盘所有权 |

`Status` 会查询每个 durable component，并把全部错误合并返回；结果同时带 effective `ObjectLimits`。字段只用于诊断，带错误的结果不构成一份成功的部分状态。独立 server 因此只在整个查询成功时打印状态。

## 二、目录格式

根目录使用以下持久与可恢复名字；staging、probe、recovery name 与 `LOCALSTORE.init` 只在对应协议尚未收敛时存在：

```
ROOT/                         0700，归 server 的有效用户所有
  FORMAT                      对象格式、版本、store UUID 与校验和
  .FORMAT.stage               FORMAT publication 的恢复 staging name
  OWNER.lock                  lifetime ownership lock
  LOCALSTORE                  READY marker，含 store UUID 与唯一 workspace name
  .LOCALSTORE.stage           READY publication 的恢复 staging name
  LOCALSTORE.init             初始化期间的空 intent 或已绑定 intent
  .LOCALSTORE.init.stage      初始化 binding 的瞬时 staging name
  METASTORE                   SQLite 提交与 checkpoint 的外部见证
  .METASTORE.stage            见证原子替换的瞬时 staging name
  .leases.intent              占有状态的绑定与 READY 标记
  .leases.witness             独立的最大租期见证
  .leases.intent.stage        占有初始化的恢复 staging name
  .leases.witness.stage       租期见证的恢复 staging name
  .leases.probe-source        占有持久化能力探针
  .leases.probe-target        占有持久化能力探针
  metastore.sqlite            命名空间、配额、对象状态与变更日志
  metastore.sqlite-journal    SQLite 按需创建
  metastore.sqlite-wal        SQLite 按需创建
  metastore.sqlite-shm        SQLite 按需创建
  objects/                    0700
    .store-identity           objects root 的 store identity
    .publish-probe-source     hard-link 能力探针的恢复文件
    .publish-probe-target     hard-link 能力探针的恢复名字
    .publish-probe-occupied   create-only 能力探针的恢复文件
    .put-k...                 durable Put recovery record
    .delete-k...              durable Delete recovery record
    00/ ... ff/               按 key 摘要首字节分片
      .store-identity         已发布的 shard identity
      .store-identity.stage   shard identity 的恢复 staging name
      k...                    已发布对象
      .stage-k...             尚未完成清理的 staging name
```

所有持久文件是归 server 有效用户所有的普通文件。格式、锁、marker、元数据与对象文件为 `0600`；目录为 `0700`。打开已初始化的根目录时，缺少必须项、类型或 ownership 不符、符号链接替代文件、格式或校验和不合法都使打开失败；`objects/` 中无法识别的恢复项同样失败。SQLite 主文件、rollback journal、WAL 与 shared-memory 文件若仍是可信的 singly linked owner-owned regular file，打开会先把 mode 收紧到 `0600`；其它 durable component 的错误 mode 不被修复。

`LOCALSTORE.init` 只表示组合层正在进行第一次初始化；它不是 READY 状态，也不能让一个已有的、无法证明来源的对象库自动生成新元数据。local-disk 层先在 lifetime lock 下创建空 intent，组合层取得 store UUID 后把它原子替换为带 UUID、workspace name 与 checksum 的 binding。`LOCALSTORE` 是完成标记：只有对象格式、绑定后的 SQLite 数据库、`METASTORE` 见证和这个 marker 都 durable 后，根目录才可以作为已初始化 store 重新打开。它把 root 永久绑定到一个 workspace name；以另一个名字打开会失败，不会在同一数据库中新建空 namespace。

`.LOCALSTORE.stage` 不表示 READY。打开时先删除并同步这个可重建的 publication staging name，再只从 `LOCALSTORE` final name 判断是否完成；stage-only 会回到未完成初始化，final+stage 会保留 final 并完成 cleanup。

## 三、初始化、READY 与重新打开

第一次 `Open` 按以下顺序建立可恢复状态：

1. 以目录描述符锚定根目录，校验根目录与祖先路径的保护条件。
2. `localdisk.Open` 取得根目录 lifetime lock。`FORMAT` 不存在时，它在同一把锁下 durable 地创建或验证 `LOCALSTORE.init`，再建立 `OWNER.lock`、`objects/` 和带随机 UUID 的 `FORMAT`，并对对象发布能力做探测。
3. 组合层核对 `localdisk.Open` 返回的初始化状态；一个非 READY store 必须是这次开始或恢复的已知初始化。随后以 staging file 与 rename durable 地把 intent 绑定到 store UUID 和 workspace，已有 binding 必须逐项匹配。
4. 对已知的新初始化建立或复用私有 `metastore.sqlite`；已绑定 intent 旁的数据库若经只读查询确认 `sqlite_schema` 没有任何对象，也属于 SQLite 已写 header、尚未建立 schema 的 bootstrap。已有 schema 则先验证数据库已经绑定同一 store UUID 且恰好含目标 workspace。已有 schema 使用 `RequireExistingNamespace` 打开；只有已证明尚未建立数据库的新初始化可以使用 `CreateNamespaceIfMissing`。
5. SQLite 建立或验证 schema、数据库身份、命名空间和持久高水位，提交打开事务，再原子发布第一份 `METASTORE`。见证落盘前不会报告元数据打开成功。
6. 配置 `Locks` 时，在已取得的 root ownership 下建立或验证占有 binding、SQLite 恢复记录与独立 witness，完成 `.leases.intent` 的 READY 状态，再启用 authority。首次建立需要 `InitializeLocks`；已有状态不能因该选项而重置。
7. 以 staging file、hard link 和根目录 `fsync` 发布 `LOCALSTORE`。marker 带同一个 UUID、workspace name 与自身校验和。
8. 删除 `LOCALSTORE.init` 并 `fsync` 根目录。此后 store 处于 READY 状态。

进程在初始化期间终止时，下一次 `Open` 只接受下列可证明状态：

| READY | intent | metastore | `METASTORE` | 结果 |
|---|---|---|---|---|
| 无 | 空 | 主文件与 auxiliary 均缺失 | 缺失 | 绑定 intent，建立新数据库与 workspace |
| 无 | 已绑定 | 缺失、私有零字节主文件，或 `sqlite_schema` 为空的 bootstrap | 缺失 | 按 binding 继续建立数据库与 workspace；bootstrap 判定不依赖 WAL 大小 |
| 无 | 已绑定 | 已初始化 | 缺失或有效 | 在打开事务内要求目标 workspace 已存在；验证后补齐见证与 READY |
| 无 | 缺失 | 任意 | 任意 | `EIO`，初始化来源无法证明 |
| 无 | 空 | 已有主文件或 auxiliary | 任意 | `EIO`，空 intent 不能决定已有 metadata 属于哪个 workspace |
| 任意 | 任意 | 缺失或零字节主文件旁存在 auxiliary | 任意 | `EIO`，状态不能由合法 SQLite 初始化产生 |
| 任意 | 任意 | 缺失或零字节主文件 | 存在 | `EIO`，见证不能先于已初始化数据库存在 |
| 有 | 已绑定 | 已初始化 | 有效 | 验证全部身份后删除残留 intent |
| 有 | 缺失 | 已初始化 | 有效 | 正常重新打开 |
| 有 | 空、损坏或带 `.LOCALSTORE.init.stage` | 任意 | 任意 | `EIO`，完成状态旁存在不可解释的初始化状态 |
| 有 | 缺失或已绑定 | 缺失、零字节、身份不符或 workspace row 缺失 | 任意 | `EIO`，不重建空 namespace |
| 有 | 缺失或已绑定 | 已初始化且身份／workspace 有效 | 缺失、损坏或不匹配 | `EIO`，READY 缺少确认其 metadata lineage 的见证 |

`sqlite_schema` 为空只在无 READY、intent 已绑定且 witness final/stage 都缺失时证明初始化尚未建立 metadata；空 intent、READY 或 witness state 旁的同一文件都不能作为 bootstrap。表中的“有效 `METASTORE`”还必须满足 WAL 规则：若见证的 checkpoint generation C 小于 accepted generation A，打开前必须存在带 frame 的 WAL；缺失、空或只有 header 都失败。`FORMAT` 已存在却没有 READY 和合法 intent 时同样失败。所有拒绝都保留不能安全解释的文件；初始化不会把残存 object store、replacement database 或丢失 workspace row 解释成一份新的空 workspace。

重新打开 READY store 时，持久身份必须形成一条完整绑定：

- `FORMAT` 中的对象存储 UUID；
- `LOCALSTORE` 中相同的 UUID 与唯一 workspace name；
- SQLite `backing_store` 中的数据库级绑定；
- SQLite `database_state` 的数据库 identity、generation 与 node/change 高水位；
- `METASTORE` 中相同的 store、workspace、数据库 identity、已接受状态与 checkpoint generation。

启用文件占有的 root 还必须同时满足下文的 native binding、SQLite `lease_recovery` 与 `.leases.witness` 恢复规则。`LOCALSTORE` 的 READY 不代替占有状态的 READY，也不能授权补造缺失的占有证据。

SQLite binding 覆盖数据库里的所有 namespace，因为其中所有 object key 都在同一 object store 中解释；READY marker 进一步把本地 root 限定为一个 workspace。组合层在 SQLite 打开前后要求数据库恰好含这一条 workspace row，durable open 本身也在同一事务内执行 `RequireExistingNamespace`。外层 preflight 用 `LIMIT 2` 分别读取 backing binding 与 namespace 的 storage class、定长 ID 和 variable length；只有 cardinality、类型与预期长度都成立后才加载 store/workspace 字符串，不用 `count(*)` 扫描全表，也不把超大 TEXT/BLOB 载入内存。丢失该 row、出现另一条 namespace 或空 replacement database 都不会触发自动创建。已含 namespace 的未绑定数据库不能事后绑定；绑定后的数据库也不能通过普通 `sqlite.Open` 绕过校验。任一 store、database 或 workspace identity 不匹配都在 READY announcement 与 HTTP serving 之前失败。

SQLite writer 使用 WAL、单 writer connection、`BEGIN IMMEDIATE`、`synchronous=FULL`，并关闭自动 checkpoint。durable writer 的每条物理 connection 还启用 SQLite `PERSIST_WAL` file control；accepted state 尚未完成 witnessed checkpoint 时，异常关闭、`Abort` 或未确认 `Accept` 不会顺手删除仍供恢复判定的 WAL。打开时读回 `PRAGMA synchronous`，不是 FULL 就拒绝使用该 writer。命名空间修改、用量计数、对象状态、change-log position、高水位与 generation 在同一个 SQLite transaction 中提交。普通 namespace/log read 与长期 snapshot 使用两个独立 reader pool，分别由 `MaxReaderConnections` 与 `MaxSnapshotReaderConnections` 限制实际 SQLite connection 数；零值各自选择 16，负值与 `math.MaxInt` 在打开数据库前被拒绝。任一池已占满时，后续读取等待该池的 connection 并遵从 request 或 snapshot context 取消，慢 snapshot 不占用普通/event read pool。

普通读取先在 database health 的共享门内检查 authority 健康状态，启动只读 transaction，并用对 `database_state` 的第一条常量查询钉住 SQLite snapshot；随后释放 health 门，再执行路径扫描、page production 与 caller-owned result accounting。最终发布从权限判定到 commit、witness 或失败隔离结束期间阻止新的 snapshot capture；已经捕获的旧 snapshot 可以继续完成。对象读取在该 snapshot 中取得 node、size 与不可变 object key，再在门外取得 payload。长扫描、payload I/O 和调用方预算回调不会继续阻挡 mutation 的 commit/Accept 边界。

只读 transaction 使用其拥有的 context 解释查询与回滚结果。纯取消保留原因并返回 `EINTR`；该 context 已取消时，直接的 `sql.ErrTxDone` 可表示 `database/sql` 已自动回滚，包括回调成功后才发生的取消。SQLite `SQLITE_INTERRUPT` 只在读取 context 确已取消时映射为取消；deadline、真实查询或独立 cleanup 故障仍是错误。未知 commit 与 poison 拥有 `EIO` 分类，不因保留的 context 原因改成 `EINTR`。该规则也用于 client 的 SQLite replica，取舍见[请求中断](../../../.agents/notes/implemented/bug-fix/2026-08-22-eio-from-a-freshly-mounted-mountpoint.md)。

`METASTORE` 是 SQLite WAL 之外的确认边界。它记录完整的已接受状态 A：数据库 identity、generation、node high-water 与 change high-water，并另记已经完整进入主数据库的 checkpoint generation C，始终满足 `0 <= C <= A.generation`。每次 durable open 或 mutation 先提交 SQLite，再用 `.METASTORE.stage` 写入并同步下一份 A，以 rename 原子替换 `METASTORE`，最后同步 root；见证发布完成后调用才可返回成功。stage 不是确认记录，重开时只把 final name 当作 A，但还要求残留 stage 通过完整内容校验。checkpoint 在创建 stage 后、写完前中断，会让已有完整 final 与恢复证据的 workspace 也以 `EIO` 拒绝打开；缺口见[恢复中断的见证 stage](../../../.agents/notes/proposed/bug-fix/2026-09-07-recover-interrupted-witness-stages.md)。确认发布失败会 poison SQLite，后续读、写与 checkpoint 均以 `EIO` 拒绝。

rename 本身失败时，publisher 删除未接受的 stage 并同步 root；清理失败则保留错误与现场。`Checkpoint` publication 失败不会 poison accepted state，清理成功后后台 worker 或关闭可以重试；这也包括 rename 已发生、最终 root barrier 失败的 checkpoint 尝试。`Accept` publication 的任何失败都会 poison 当前 SQLite，因为已提交状态没有完成 acknowledgment；rename 已发生而 root barrier 失败时，重开再以 final witness、WAL 与 visible state 对账。

打开前先记录 WAL 是否存在以及是否超过 32 字节 header。若 `C < A.generation`，非空 WAL 是已确认状态仍然存在的必要证据，缺失、空文件或仅有 header 都以 `EIO` 拒绝。SQLite 打开后可见状态的数据库 identity 必须等于 A；generation 小于 A 表示确认状态回退，等于 A 时全部高水位必须完全相同。只有打开前已有非空 WAL 时，可见 generation 才可大于 A；这表示 SQLite commit 已发生而见证尚未完成，打开事务会继续前进并发布新的 A。高水位在任何恢复路径上都不能倒退。

### 文件占有的持久恢复

schema migration `0004_lease_recovery.sql` 为整份数据库保存 database ID、StateID、已接受 generation 与最大租期，以及可选的下一代 prepared generation 与最大租期。localstore 的数据库仍只持有 root 所绑定的那一份 workspace。`.leases.witness` 独立保存同一证据；root inode 上的 create-only xattr `user.remote-fs.lease-state` 把证据目录、名字、store/workspace 与 StateID 绑定在一起，`.leases.intent` 保存相同 binding 和初始化 READY 状态。UUID 只证明身份，generation 与两份独立证据共同检测单边回退。

一次更长的 Acquire 或 Renew 得到确认之前，SQLite 先在 FULL transaction 中写入 Prepared，并完成对应的 `METASTORE` 确认；随后原子替换并同步 `.leases.witness`；最后另一个 FULL transaction 把 Prepared 收入 Accepted 并清空 Prepared，再完成 `METASTORE` 确认。租期高水位只增不减，准备所需 I/O 在最终资源转换之外执行，完成后仍须重新验证 grant 和期限。

令 A 为 Accepted，P 为与 A 身份相同、generation 恰好多一且最大租期不减的 Prepared，重开只接受以下状态：

| SQLite | 独立 witness | 恢复 |
|---|---|---|
| A，无 P | A | 接受 A |
| A，有 P | A | 把 witness 推进到 P，再完成 P |
| A，有 P | P | 完成 P |

其余缺失、损坏、身份不符或 generation 组合均以 `EIO` 拒绝。初始化只能建立匹配 durable intent 的零代证据；已有 READY binding 缺少 witness 不会被重新初始化。占有专用 stage 只在 final evidence 已验证之后按其协议清理，其恢复规则与 `METASTORE` stage 独立。

恢复起点采用 SQLite 实际取得数据库原生排他锁时捕获的 monotonic 时间，早于加载租期证据。authority 从这个起点等待完整的持久最大租期；期间普通读取和状态查询可用，新授权与 namespace 修改以 Recovering 拒绝。配置调小不缩短旧保护，重启不会把以前确认的剩余保护当作空状态。运行期 grant 与 action receipt 不持久化；旧 authority 及其调用方身份退役，重启后的核对返回退役或结果未知，详见[文件占有](file-locks.md)。

占有初始化与普通重开都会验证 native binding，并探测本地 `flock`、xattr、原子 rename 和 file/directory `fsync` 能力。证据目录与被绑定 root 必须处在当前同一 mount；mount ID 只在运行期比较，不作为跨重启 identity。canonical evidence directory 是持久 binding 的一部分，移动 root 需要显式迁移，不能仅改配置路径。

## 四、本地文件系统与路径信任

`localdisk.Open` 在修改根目录之前拒绝已知 remote filesystem：NFS、CIFS/SMB、9P、AFS、Ceph、Coda 与 NCP。根目录、`objects/`、每个 shard、最终对象、SQLite 文件、初始化 binding、`METASTORE` 与 READY marker 必须保持相同的 device 与 `statx` mount identity；对象树还逐项核对 filesystem type。mount identity 无法取得时以 `EOPNOTSUPP` 拒绝，bind mount 或 submount 不能把这些被核对的 entry 换到另一基底。未识别的 filesystem type 必须通过一次 hard-link publication probe：创建 source、第一次链接成功、第二次链接同一目标得到 `EEXIST`，然后清理并同步目录。通过 probe 仍不证明 crash durability；部署底层必须真实提供本地 `flock`、`linkat` 与 file/directory `fsync` 语义。

根目录自身必须由 server 的有效用户持有且 mode 为 `0700`。从 `/` 到根目录的每一级父目录都必须由 root 或该用户持有，并且满足以下任一条件：

- 其他用户不可写；
- 父目录带 sticky bit，下一层 entry 由 root 或 server 用户持有。

这项校验阻止信任边界外的 principal 在打开期间替换路径分量。root、同 UID 进程和管理员仍能改名或替换这些分量，属于部署信任边界；部署必须在 store 的整个 lifetime 内约束它们。组合层在打开各个 pathname-based component 前后比对根目录的 device 与 inode，但这不是跨整个运行期的 rename lease。

对象层同时对根目录描述符与 `OWNER.lock` 取得 non-blocking exclusive `flock`。localstore 的 durable SQLite opener 还持有数据库文件的原生 `LOCK_EX`；原始非独占 SQLite opener 持有同一 native file 上的 `LOCK_SH`，使同进程与跨进程的既存 raw handle 都不能与独占 owner 并存。冲突 opener 得到 `EBUSY`，这些所有权只在全部 SQLite connection 成功关闭后释放。保留文件引用要求实际 EX ownership，只有 SH 的 Store 以 `EOPNOTSUPP` 拒绝这项能力；进程内 pin 不能保护另一写进程的回收。`ConfigureLeaseRecovery` 验证实际 EX owner 与绑定的原生 `LeaseAnchor`，`EnableLocks` 只接受已配置该恢复状态且仍持有 EX 的 Store；调用方提供一个持久化接口不能代替这些条件。完整规则见[文件占有](file-locks.md)和[文件句柄](file-handles.md)。

## 五、key、分片与对象 envelope

对象 key 是 metastore 生成的不透明字节串，不是文件路径，也不是内容哈希。local-disk 实现接受至多 120 字节的 key，并用以下可逆、单射的布局寻址：

```
shard   = hex(SHA-256(key)[0])
name    = "k" + base64.RawURLEncoding(key)
object  = objects/shard/name
stage   = objects/shard/".stage-"+name
```

分片只限制单个目录的 entry 数量；完整 key 仍写进对象，不能从摘要推断。key 超过上限以 `ENAMETOOLONG` 拒绝，编码后的名字不得被解释成路径。

每个 shard 由一个版本化、带 checksum 的 `.store-identity` 绑定到 store UUID 和 shard byte。`Objects` 持有固定的 256 个 context-aware shard token；每次打开 shard 时只在目录创建、identity 创建／恢复、类型与 filesystem 校验以及对应 directory barrier 期间持有该 shard 的 token。相同 shard 的这段初始化被串行化；已取得 waiting ticket 的其它 shard 继续并行，超过 bounded waiting capacity 的新调用以 `EAGAIN` 拒绝。token 在对象 envelope 检查、payload I/O 与对象 publication 之前释放。

新的 shard identity 先以 create-only 方式写入 `.store-identity.stage`，完整写入后 `fsync` 文件，再用 `linkat` create-only 地发布 `.store-identity`。实现重新打开两个名字，要求它们是同一 device/inode 且 link count 都为 2，随后 `fsync` shard、删除 stage，并再次 `fsync` shard。final name 因而只在完整内容已经同步后出现，不能被并发 opener 读到半写 marker。

重开时，只有以下中间状态可自动收敛：正确且是 shard 唯一 entry 的 stage-only 状态会继续 publication；正确且 final/stage 指向同一 inode、link count 为 2 的状态会先重做 publication barrier，再删除 stage。完整 final-only marker 的 link count 必须为 1。stage 与其它 entry 并存、两个名字指向不同 inode、额外 hard link、marker 损坏或身份不符都以 `EIO` 拒绝并保留现场。只读打开不会把 stage-only shard 修改成可用状态。

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

对象写入以 durable recovery record 包围实际目录修改。`.put-k...`／`.delete-k...` 文件名可逆编码 key 与操作；固定 96 字节内容另带版本、store UUID、操作、key 摘要与 checksum。文件名和内容必须逐项一致，另一份 store 的同名记录不能取得恢复权限。`Put` 的顺序是：

1. 在 `objects/` 创建 `.put-...`，同步 marker file，再同步 `objects/`。
2. 在目标 shard 创建 `.stage-...`，写入 envelope、key 与 payload，`fsync` staging file 后关闭。
3. 用同一 shard 内的 `linkat` 把 staging inode 发布到 final name；目标已存在时得到 `EEXIST`，不会覆盖。
4. `fsync` shard，使 final name durable。
5. 删除 staging name，重新 `fsync` shard。
6. 删除 recovery record，`fsync objects/`。

`Delete` 先 durable 地创建 `.delete-...`，验证 final object，删除 final name 并 `fsync` shard，最后删除 marker 并 `fsync objects/`。final name 已不存在时，只要 shard 本身可信，删除仍收敛为成功；已有 shard 仍会被同步，使一次先前在 directory barrier 处失败的删除可以由重试完成。

`Open` 在接受请求前先验证全部已有 shard identity，再扫描有界数量的 recovery records。Put record 使 final object 保留、staging name 被删除；Delete record 使 final 与 staging name 都被删除；对应 shard 随后同步，最后 marker 被删除并同步。未知 entry、超过恢复上限、marker 的名字／内容／key 不一致、缺少它所指向的 shard，都会使打开失败。

shard identity 或对象 publication 已经发生后，任何无法证明 directory barrier、link 结果或 cleanup durability 的错误都会 poison 当前 `localdisk.Objects`。成功与 fact-bearing 结果都在返回前与 poison 状态线性化；poison 一旦先建立，已经通过早期健康检查、正在等待 key/shard/admission 的调用也不能再返回 `ENOENT`、`EEXIST`、`EFBIG`、`ENOSPC` 或成功等磁盘事实，而统一返回 `EIO`。context cancellation 的 `EINTR` 保留为 cancellation，不被改写成磁盘结论。实例保持失败直到关闭并通过下一次 `Open` 的 recovery 得到可证明的磁盘状态。

一份文件内容的 namespace 写入按 `Reserve → Put → Commit` 进行。reservation 先 durable 地进入 SQLite；`Put` 完成上述对象 barrier。`Commit` 在原生 writer gate 内解析实际目标并准备路径、节点属性、逻辑用量、对象状态与 change log；事务真正提交前，authority 对同一目标上的显式 mutation proof、冲突与到期状态作最终判定。匿名修改也经过该判定。上传不占住授权门，也不延长期限。

最终许可与 FULL-synchronous commit、`METASTORE` 确认和结果处置构成一个有序转换。许可在到期前取得的修改可以稍后完成，但受影响资源的后继授权、解除、续期及新的权威 snapshot capture 都不能越过其结果。原生 backend gate 先于 authority 锁取得；只有最终有界转换处于 authority 发布回调内。无法确定 commit、rollback 或配额结算结果时，namespace 与 authority 一同失败隔离；即使已经证明 namespace 未修改，也不能忽略未知的 accounting 状态。

`Write` 只有在 `Commit` 成功后才返回成功。因此：

- reservation 提交后、`Put` 得到确定结果前终止，留下不可自动删除的 reserved record；
- `Put` 返回错误时，`Quarantine` 把 reserved 转成 unresolved；错误不能证明该 key 下的对象由本次调用建立，因此它不进入删除队列。`EEXIST` 这类 object-key 事实不能被呈现为 namespace path 的事实，对外以 `EIO` 报告；
- `Put` 成功后，最终权限检查因过期或冲突拒绝，或 `Commit` 在可证明未发布时失败，`Abandon` 才把 reservation 转成 garbage，因为成功的 create-only `Put` 已经证明这次写入拥有对象；
- `Commit` 实际已落地但回复丢失时，已被引用的对象不能改成 garbage；SQLite 的未知 commit 还会使收尾在健康检查处失败，这次 `Write` 以 `EIO` 报告结果未知；
- `Commit` 返回成功后终止，重新打开得到同一个名字、属性、内容与 change-log position。

`Quarantine` 与 `Abandon` 使用 storage-lifetime context，request cancellation 不会取消这项收尾；收尾失败与原操作错误一起返回。SQLite commit 结果未知或已被 poison 时，`Abandon` 不能绕过健康检查补出删除权限；对象记录保留供恢复判定。unresolved 与 reserved 都持久保留，不因经过一段时间而变成 garbage；时间不是对象 ownership proof。garbage 只在 sweep 成功删除并忘记对象后才释放 pending admission room。

零字节文件不创建对象；其内容直接由 metastore 中没有 object key 的零长度节点表示。

### 保留节点与范围修改

`metastore.FileStore` 在同一原生 gate 内完成路径或节点身份核对、创建／截断与引用保留。`ExpectedID` 不匹配以 `ESTALE` 失败；创建不存在的文件、排他创建判断、初始属性与返回的引用属于同一次结果。保留引用绑定 node ID，改名不改变它；unlink 或 rename 替换目的地时，仍有 pin 的普通文件成为 `detached`，其当前 object 引用、属性与配额继续保留。路径查询与 named snapshot 不暴露 detached 节点，其后修改不产生 named change event；句柄仍可读取、修改当前状态。

句柄的 `ReadAt` 每次从一份 `FileState` 取得 node、size、object key 和 content revision，再在原生 gate 外读取该不可变对象。返回的属性、范围与 EOF 来自这同一状态。旧对象已被清扫且节点已指向另一对象时有界重试；节点仍指向缺失对象时以 `EIO` 失败。一次调用可以输给持续覆盖而返回 `EAGAIN`，不会拼接两份状态。

`WriteAt` 与 `Truncate` 先读取当前完整对象，在有界内存中生成下一份完整内容：范围之外保留当前字节，扩展部分为零。随后按 `Reserve → Put → Commit` 发布，以读取时的 `content_revision` 做原生 CAS；revision 不符表示未提交，成功清理本次新对象后才能重新读取并尝试。真实提交、持久确认或 cleanup 结果未知时立即报错并保留失败隔离，不重放可能已生效的写入。普通重叠写入可依实际顺序各自成功；这项内部重试不是调用方显式内容版本校验的承诺。每次写入和截断成功前完成服务端确认，关闭不补交旧整文件快照。

关闭或会话退役先在最终发布 gate 内撤销引用权限，再排空已接纳的 materialization 与上传，最后释放 native pin。最后一个 pin 关闭 detached 节点时，在同一事务中删除节点、按实际剩余大小释放配额，并把当前对象标为 garbage；真正对象删除由 sweeper 执行。已被逻辑退休的引用不能因为物理字节尚在而继续发布。关闭结果未知会保留 pin 并失败隔离，不宣告配额已释放；旧 epoch 的引用明确失效，不按原路径重建。会话与 advisory 锁的生命周期见[文件句柄](file-handles.md)。

## 七、容量与资源上限

本地持久 store 必须带正数 workspace quota。SQLite 精确记录 named 与 retained detached 文件当前引用的 payload bytes，并在改变引用与大小的同一 transaction 中以 `EDQUOT` 拒绝超额写入。失去名字不释放仍由打开引用持有的配额；最后物理引用已知关闭后才扣除。reserved、unresolved 与 garbage object、envelope 与 recovery state 不计入逻辑 `Used`，但占用物理磁盘。

`sqlite.ObjectLimits` 另行限制一份 namespace 中 reserved、unresolved 与 garbage records 合计的数量和记录 payload bytes。单个请求的 payload 大于 `MaxPendingBytes` 时 `Reserve` 以 `EFBIG` 拒绝，因为任何后台工作都无法让它单独装进阈值；请求本身能装下、但现有 backlog 使新增记录越过数量或字节阈值时返回 `EAGAIN`。拒绝与插入在同一 write transaction 中完成，不上传对象，也不留下 reservation。

unresolved 没有自动恢复或删除路径。重复的未知 `Put` 结果可以占满 pending threshold，使后续写入持续返回 `EAGAIN`；状态查询会单独报出它们，系统不会为恢复 admission 而删除 ownership 未被证明的字节。

零值 fields 由 `ObjectLimits.Effective()` 换成默认值；负值与 `math.MaxInt64` 被拒绝，没有 unbounded 取值。commit、覆盖、删除和改名必须继续完成其权威状态转换，其中解除 live object 引用的操作可以把 garbage backlog 推到阈值之上；`Quarantine` 与 `Abandon` 也始终可用，且 reserved→unresolved 或 reserved→garbage 都保持 pending count/bytes 不变。`ObjectStatus.OverLimit` 只在 count 或 bytes 严格大于 effective threshold 时为 true；恰好等于阈值仍是 within-limit 状态，但新的 reservation 可能已经没有余量。over-limit 时新的 reservation 持续返回 `EAGAIN`，garbage 清扫与 namespace shedding 仍可进行，直到回到阈值内。limits 是 serving configuration，重新打开可以选择不同的有限值。

`Reserve` 的热路径只读取 indexed pending totals；namespace 打开与 `ObjectStatus` 执行完整性验证。每个 namespace 的 named 节点必须是从唯一 root 可达的一棵树：root 没有 incoming entry，其余 named 节点恰有一个同 namespace 的名字。detached 节点只能是非 root 的普通文件，没有名字，也不能作为 entry 的父或子节点；其它孤儿、cycle 与跨 namespace entry 都是 `EIO`。`namespaces.used` 必须是非负整数，并等于 named 与 detached 全部 regular-file size 的 overflow-checked streaming sum；object state 与 size 同样逐项验证。SQLite storage class 也逐列验证：entry/change name 是 BLOB，identity/key 是非空 text，标量为 integer，`detached` 只能为 0 或 1，`content_revision` 必须为正，可空 change node/from fields 必须按 kind 成组出现，mode、size、position 与纳秒范围有效。对 schema version、database identity、backing-store binding、snapshot/log header 与 append tail 等动态标量，查询先用 `typeof`／长度条件把错误类型投影成 NULL，再由 Go 拒绝 storage class，不让 driver 把超大 BLOB 或错误类型强制成可信的整数／字符串。任何会改变 cursor order 的记录也以 `EIO` 拒绝。

`database_state` 保存 32 ASCII 字节的小写十六进制 database identity，以及单调的 generation、node high-water 与 change high-water。节点 ID 和 change position 都由对应 high-water 显式分配，再以显式主键插入；SQLite `AUTOINCREMENT` 与 `sqlite_sequence` 继续作为冗余约束。数据库打开、checkpoint、`DurableState` 与每个 `Since` page 要求 sequence 和对应 high-water 完全相等，并通过按 storage-class discriminator／最大 identity 排序的 expression indexes 读取全数据库的类型异常与最大 surviving node/change reference；任一 namespace 的引用超过高水位都会失败，不扫描全部 rows。每次分配重新核对 sequence；change append 还要求现有 committed tail 严格小于新分配的位置，否则事务以 `EIO` 回滚。namespace 完整性查询再校验目标 namespace 的逐行关系。达到 `math.MaxInt64` 时以 `ENOSPC` 拒绝，不绕回或复用。`METASTORE` 中的外部副本还能检测 `database_state` 与 `sqlite_sequence` 被一同回退的情况。

每条 retained change 保存 `previous_position`，指向同 namespace 的上一条位置；第一条 surviving change 指向 `trimmed_through`，最后一条等于 `committed_position`。全局 change position 可以因其它 namespace 的记录产生空洞，所以完整性检查沿 predecessor 链而不要求 `position + 1`。`Open`、`Snapshot` 与 `ObjectStatus` 在暴露 namespace 前验证整条 retained chain，完整工作受 `MaxIntegrityRecords` 限制。`Since` 使用索引读取 database state、sequence、log header、oldest/newest 与按需的 page anchor，再把实际 page row 数收紧到剩余 record budget；请求的 limit 很大而实际日志很小时仍只按存在的工作计费，anchor 已耗尽预算且 tail 尚未返回时以 `EFBIG` 拒绝。它逐项验证 page-local predecessor，工作量与实际页大小成正比；跨到缺口的 page 使 caller-owned result 整体失败，不暴露该页已经产生的 prefix。较早且完全位于后续缺口之前的 page 可以成功；缺口页的 stream error 使 consumer 立即作废副本，随后带同一 incarnation/position 的续订持续失败，直到持久日志被带外修复，或出现合法的 incarnation/window rebuild boundary。

v1/v2 迁移在同一 transaction 内对全库执行 storage class、树、对象、用量、序列与历史格式检查，失败时 schema 与数据都不部分前进。v1 没有 retained history；v2 没有 predecessor，不能把旧 rows 升格为连续性证明。迁移保留 node/change 全局高水位，删除 v2 retained changes，把每条日志切换到新的 incarnation 并将 tail／trim 归零；迁移后的第一条 change 仍严格高于旧 change high-water，现有 replica 因 incarnation mismatch 重建。

schema v5 的 `0005_retained_files.sql` 增加 `nodes.detached` 与 `nodes.content_revision`，既有节点迁移为 named、revision 为 1。每次内容发布都推进 revision，包括空内容再次发布；耗尽时以 `EOVERFLOW` 失败。revision 不复用作节点身份、S/X generation 或 change position。独占 opener 在启动事务中先按完整性和字节预算验证所有 namespace，再回收前一 serving epoch 遗留的 detached 节点：释放各自 quota，把当前对象标为 garbage，最后删除节点。任一 namespace 的损坏都使整次恢复回滚，不能先删除再验证；SH opener 不获得这项回收权限。原生 owner 隔离保证另一有效实例的 pin 不会被回收。

`sqlite.Options.MaxIntegrityRecords` 在 recursive CTE 之前限制一次验证接触的 namespace seed、node、relevant entry、object、log 与 change records 合计；entry 的 label、parent 或 child 任一接触 namespace 都计费。`DefaultMaxIntegrityRecords` 为 1,000,000，`MinIntegrityRecords` 为 3，只容纳一个 namespace seed、空 root 与 mandatory log row；零值选择默认，低于 3 与 `math.MaxInt64` 在数据库或 local-store root 被接触前以 `EINVAL` 拒绝。超出 serving limit 是 `EFBIG`，并要求显式提高配置；无法完成计量是 `EIO`。legacy migration 使用全库合计，不能让另一个 namespace 的工作量或 corruption 藏在这次打开的名字之外。

`sqlite.Options.MaxIntegrityBytes` 在对 entry/change name 执行 `instr` 等 content-sensitive 校验前，以不会 materialize BLOB 的 SQLite `length` 分批累计变长字段。namespace 校验统计相关 entry name 与 retained change 的 destination/source name；legacy migration 统计全库对应字段。默认 64 MiB，零值选择默认，非正值与 `math.MaxInt64` 在接触数据库前以 `EINVAL` 拒绝；总量超过上限以 `EFBIG` 失败。这个限制约束完整性扫描读取的变长内容，`Since` page 的返回 payload 仍由 caller-owned `ChangeResult` byte budget 独立约束。

`Space` 组合两个同时成立的测量：

```
Total = configured workspace quota
Used  = SQLite 记录的 named + retained detached payload bytes
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
| 等待 key/shard 或 active admission 的 local object operations | 256 |
| admitted object bytes 总量 | 2 GiB |
| maintenance reserve | 64 MiB |
| `Open` 检查的 recovery records | 4096 |
| pending reserved + unresolved + garbage objects | 4096 |
| pending reserved + unresolved + garbage payload bytes | 8 GiB |
| SQLite ordinary/event reader connections | 16 |
| SQLite snapshot reader connections | 16 |
| SQLite integrity records examined | 1,000,000 |
| SQLite integrity name bytes examined | 64 MiB |
| namespace 中保留的 native file references | 65536 |
| retained-file 单个完整内容 | 1 GiB |
| namespace materialization 的 current + next bytes | 2 GiB |
| namespace 同时 materialization operations | 32 |
| 一次 retained-file 内容读取／发布的尝试数 | 8 |
| retained-file 单次操作期限 | 30 秒 |
| 后台 sweep interval | 1 分钟 |
| 每次后台 sweep 的对象数 | 64（最大 1,048,576） |
| change-log floor / cap / age | 100 entries / 10000 entries / 10 分钟 |

`MaxInFlightBytes` 必须至少容纳一份最大对象及其 envelope/key，recovery-record 上限不得低于 active operation 上限。`MaxWaitingOperations` 必须是小于 `math.MaxInt` 的正数；等待名额已满时新调用以 `EAGAIN` 拒绝。请求先取得 waiting ticket，再等待 per-key/per-shard token，完成 shard 初始化后才把 ticket 提升为 active operation/byte reservation，因此一个 shard 的等待者不会占满全部 active 名额并阻塞其它 shard。等待过程服从 `context.Context`；`Get` 与 `Delete` 在 active admission 被取消时会泄漏此前打开的 shard descriptor，详见[取消时关闭 shard 描述符](../../../.agents/notes/proposed/bug-fix/2026-09-07-close-shard-descriptors-on-cancel.md)。`Close` 拒绝新的 admission，并等待 waiting、active 与 control operation 离开；排空这些计数不能回收已泄漏的描述符。

`MaxRetainedFiles` 统计 namespace 中的 native 引用数，同一节点的多次打开分别计费；满额以 `EAGAIN` 拒绝。`advisory.Config` 同时拥有 namespace 级 materialization operation／byte budget，句柄内容操作在读取 payload 和分配下一份内容前收费。写入收取 current + next，读取收取完整对象加返回范围；单项不能装入上限时为 `EFBIG`，容量被其它操作占用时为 `EAGAIN`。`MaxMaterializedBytes` 必须至少容纳两份 `MaxFileBytes`，共享 namespace 的 opener 必须使用相同配置。这些预算不替代 localdisk 物理 admission、pending object 阈值或 HTTP retention 上限。

## 八、维护、状态与关闭

`objectstore.New` 使用 `DefaultOptions()`（1 分钟 interval、64 个对象一批），`NewWithOptions` 接受显式覆盖；interval 必须为正，batch 必须在 1 到 `MaxSweepBatch`（1,048,576）之间。两者都启动一个由 storage lifetime 持有的 sweeper，并立即安排一次 startup sweep。成功的 `Write`、`Remove`、`Rename` 与把 reservation 改成 garbage 的 `Abandon` 都发送一个容量为 1 的触发信号，突发修改会合并；周期 ticker 也会触发，因此一次 transient failure 在 backend 恢复后无需重启或新 mutation 就会重试。一次只运行一个 sweep，metastore 通过 garbage-only 索引按创建时间取至多配置的 batch；正好删满一个 batch 时重新排队，继续以有界批次排空已经可达的 backlog。reserved 与 unresolved 不是清扫候选。对象删除成功后才调用 `Forget`；`Forget` 只接受 garbage 或已经缺席的 key，对 reserved、unresolved、referenced 以 `EINVAL` 拒绝，未知 state 以 `EIO` 拒绝，一个批次的检查与删除在同一 transaction 中完成。对象删除失败保留 garbage 记录；元数据提交结果未知时保持失败隔离，不能承诺记录仍在并按确定未提交的结果继续清扫。

`MaintenanceStatus` 保存最近一次已完成尝试的时间、删除数与错误。后续成功会清除旧错误。关闭请求取消正在运行的 background sweep，并等待它结束；若这次尝试返回 cancellation，其错误链仍保存在状态中，不会被静默抹掉。

独立的 checkpoint worker 在 durable metastore 打开时及每次 A 前进后收到一个容量为 1 的合并信号。它与 mutation/witness publication 使用同一串行门执行 `PASSIVE` checkpoint；一次尝试会延后同时到达的 mutation，但 snapshot pin 或错误后的等待不持有该门。等待期间的新信号只记录仍有工作，不绕过一秒 retry interval。一次真实错误保留为 checkpoint status 的 `LastError`，后续完整 checkpoint 会清除它。只有 SQLite 报告全部 WAL frame 已复制、当前 durable state 仍等于 A，且更新后的 `METASTORE` 已同步，C 才前进到 A。

`localstore.Status` 同时报告：

- workspace 的 `Total`、`Used`、`Avail`；
- reserved、unresolved 与 garbage object 的数量和 payload bytes；
- pending-object 的数量/字节阈值，以及 backlog 是否 `OverLimit`；
- SQLite ordinary/event-reader 与 snapshot-reader connection 上限；
- SQLite integrity record work 上限；
- SQLite integrity variable-name byte work 上限；
- `METASTORE` 的 accepted／checkpointed generation、是否仍有 checkpoint pending，以及上次成功后首个尚未清除的 checkpoint error；
- store UUID、物理可用字节、waiting/active operations、in-flight bytes 与 recovery-record 数；
- effective sweep interval/batch 与最近一次后台 sweep 结果；
- durability failure 已 poison store 时的失败原因。

`localdisk.Status` 使用一份与 data-plane waiting/active/byte admission 分离的 serialized control slot。waiting、active 或字节名额耗尽时，状态仍能读取准确的计数；同时只允许一个 control operation，`Close` 等 data-plane 与 control slot 都排空后才释放 descriptors。

独立 server 的 SIGHUP 对 local store 启动带 2 秒 context deadline 的异步状态查询并写入 stderr，不重新计数或修改状态。成功输出分别报告 reserved、unresolved 与 garbage，pending-object thresholds、SQLite ordinary/snapshot reader-connection、integrity-record/name-byte work、local waiting/active admission、checkpoint 的 accepted/checkpointed generation 与 pending，以及 effective sweep interval/batch；backlog 单独区分在阈值内和已经 `OverLimit`。同一项工作还查询成对 authority 的独立状态，输出 ready/recovering/unavailable、恢复剩余时间与资源计数，能力值不进入输出。文件占有的 HTTP `lock-status` 控制操作由[文件占有协议](file-locks.md)定义，不返回这份磁盘诊断。

能单独装进 byte threshold 的 reservation 因 backlog 越界时以 `EAGAIN` 失败，单个 payload 自身超过 byte threshold 则是 `EFBIG`。同一时刻至多运行一次状态工作；查询运行期间已经被 signal loop 取出的 SIGHUP 不再启动一份并发查询，仍留在 signal channel 的一个信号可在本次完成后触发下一次。SIGINT／SIGTERM 会取消并等待 status goroutine，状态查询不占住 signal loop；已经进入的不可取消 filesystem syscall 仍须返回后才能完成等待。checkpoint `LastError`、任一 component 或 lock status 查询失败时只打印整次 status failure，不格式化一组看似成功的 partial figures。

SQLite `Forget` 用调用方 context 等待 commit gate 准入。准入后，本批逐项校验、删除 SQL、日志 trim、generation 更新以及 BeginTx、Commit / Accept 和 rollback 都由 `context.WithoutCancel` 持有生命周期，存储关闭时的调用方取消不会中断已接纳的 SQL。事务取得明确的提交或显式回滚结果后才释放 gate；准备错误在回滚成功时保留原错误，真实 Commit、Accept 或 rollback 故障继续以 `EIO` 失败隔离。清扫器按既有配置限制每批记录数，关闭排空已接纳的整个批次。普通 namespace mutation 的 context 与提交规则不变。

`Close` 先停止 file-session admission，逐个撤销会话与引用的发布权限，排空已接纳操作并关闭保留引用。失败时两半存储及其所有权继续保留，后续关闭可以重试可证明的未完成清理；不能先关闭数据库再等最后一个文件引用释放。直接关闭 SQLite 时仍有 retained file references 则以 `EBUSY` 拒绝。

文件引用已排空后，组合层拒绝新的 namespace 操作，停止并等待后台清扫与 checkpoint worker，再排空已经进入的 namespace 操作；这段 worker 等待发生在 close context 建立之前。durable metastore 随后用 5 秒内部 context 等待 commit gate 并串行执行完整 `FULL` checkpoint；active reader 仍在使用 pool 时立即以 `EBUSY` 拒绝本轮关闭。全部 WAL frame 已复制且 checkpoint 见证已同步后，关闭流程才清除 writer connection 的 `PERSIST_WAL`，然后关闭 writer pool。所有 SQLite connection 都无错误关闭后，组合层才关闭 lease anchor、object-store descriptors、root anchor 与 lifetime locks。旧 authority 退役，持久最大租期继续约束下次接管。已经进入的 mutation 若在 sweeper 停止后才产生 garbage，其 durable record 由下一次 `Open` 安排的 startup sweep 接管。最后 detached close 的 commit、rollback 或 accounting 结果未知会 poison SQLite 并隔离 authority；后续关闭不能把它当作一次确定未提交的清理重新执行。

`Open` 在 metastore 已建立后失败时也保持同一 ownership 顺序：先以这个 bounded graceful close 收敛；只要尚未产生 terminal pool-close result，未向调用方暴露的 SQLite store 就走 terminal `Abort`，即使 reader pools 已关也关闭剩余 writer。`Abort` 不主动清除 `PERSIST_WAL`：witnessed checkpoint 尚未完成时保留恢复证据；若本轮已经完成 checkpoint 见证、但清除 file-control 的调用报错，flag 状态不作保证，WAL 也已经不是恢复所必需。只有 Abort/Close 无错误证明 handles 已关闭后才释放 object/root locks；cleanup 自身失败时所有权保留。SQLite constructor 在 Store 建立前清理已打开 pools 时也使用同一边界：全部 close 成功的普通构造失败释放内部 coordinator 和外部 root；任一 pool close error 不能证明 native handle 已消失，错误带 ownership-retained 标记，SQLite coordinator 与 localstore root lock 都保留到进程退出。

同一轮并发 `Close` 调用共享一个结果。active reader 在 reader pools 关闭前返回 `EBUSY`；reader pools 成功关闭后，checkpoint busy/failure、见证失败或 context cancellation 留下“reader pools 已关、writer 与所需 WAL 证据保留”的 retryable 状态。checkpoint 见证成功后的 `PERSIST_WAL` 清除失败同样保留 writer 并允许重试，但 file-control 可能已经清除，状态不作保证；此时 `A = C`，安全性不再依赖 WAL。后续 `Close` 跳过已完成步骤并继续收敛。任一 reader/snapshot/writer pool `Close` 返回错误时，native handle 是否已部分释放无法证明，该错误成为 terminal result，后续调用持续返回同一错误；即使 writer error 发生在 `PERSIST_WAL` 已清除之后，object-store 与 root 所有权仍保留到进程退出。只有 durable metastore 无错误关闭后才释放 lifetime locks。见证 `Accept` 失败已经使 database state 不可证明，同样阻止锁提前释放。独立 server 先停止 HTTP admission、等待 handler 离开，再执行 `Close`，所以即使 graceful-shutdown deadline 到期，仍不会在 blocked request 使用 storage 时提前释放所有权。

## 九、独立 server 入口

本地持久形态的命令行为：

```
remote-fs-server \
  -listen ADDR \
  -local-store ROOT \
  -workspace NAME \
  -quota SIZE \
  [-initialize-lock-state] \
  [METASTORE OPTIONS] [LOCAL OPTIONS] [FILE OPTIONS] [LOCK OPTIONS] [HTTP OPTIONS]
```

`-local-store` 与 `-blob-container` 二选一。`-workspace` 与 `-quota` 在 local-store 形态中必填；local-only flag 用在 Azure 形态会在 storage 被打开前拒绝。对应默认值来自上一节与 server HTTP handler 的默认值，flag 只负责把显式覆盖传给拥有该限制的 package。省略 `-http-max-write-bytes` 时继承 `-http-max-body-bytes`；显式的零值与其它非正值一样被拒绝。local-store 在打开任何磁盘资源前验证 effective write 上限不大于 effective `-local-max-object-bytes`。`-http-max-body-bytes` 仍可更大，因为 listing、错误与其它 non-write body 使用它。

两种 server 形态都启用文件占有。`-initialize-lock-state` 只允许首次绑定或恢复匹配的初始化 intent；普通启动验证已有证据。localstore 在自己的私有 root 内保存证据，Azure 形态在 metastore 数据库旁保存证据。通用 `-lock-*` 上限与独立 HTTP control admission 见[文件占有](file-locks.md)。启动状态可为 recovering：server 已能回答读取和控制状态，新的授权与修改仍受恢复屏障阻止。

下列 local-store 与 metastore 资源 flags 与 package option 一一对应；`-max-pending-*`、两个 reader-connection flags、`-max-integrity-records`、`-max-integrity-bytes` 与 `-sweep-*` 由 Azure 与 local 两种 metastore/objectstore-backed 形态共用，`-local-*` 只对 local store 有效。HTTP request、non-streaming response 与 replication-frame flags 及默认值由 [`architecture.md`](architecture.md#五复制那三个操作)和[同文第六节](architecture.md#六请求与响应的内存边界)定义。

| flag | option |
|---|---|
| `-local-max-object-bytes` | `localdisk.Options.MaxObjectBytes` |
| `-local-max-in-flight-operations` | `localdisk.Options.MaxInFlightOperations` |
| `-local-max-waiting-operations` | `localdisk.Options.MaxWaitingOperations` |
| `-local-max-in-flight-bytes` | `localdisk.Options.MaxInFlightBytes` |
| `-local-maintenance-reserve-bytes` | `localdisk.Options.MaintenanceReserveBytes` |
| `-local-max-recovery-entries` | `localdisk.Options.MaxRecoveryEntries` |
| `-max-pending-objects` | `sqlite.ObjectLimits.MaxPendingObjects`；Azure/local 共用 |
| `-max-pending-bytes` | `sqlite.ObjectLimits.MaxPendingBytes`；Azure/local 共用 |
| `-max-reader-connections` | `sqlite.Options.MaxReaderConnections`；Azure/local 共用 |
| `-max-snapshot-reader-connections` | `sqlite.Options.MaxSnapshotReaderConnections`；Azure/local 共用 |
| `-max-integrity-records` | `sqlite.Options.MaxIntegrityRecords`；Azure/local 共用 |
| `-max-integrity-bytes` | `sqlite.Options.MaxIntegrityBytes`；Azure/local 共用 |
| `-max-retained-files` | `sqlite.Options.MaxRetainedFiles`；Azure/local 共用 |
| `-max-file-size` | `advisory.Config.MaxFileBytes`；不得超过对象和 pending-byte 上限 |
| `-max-file-staging-bytes` | `advisory.Config.MaxMaterializedBytes`；至少两份最大文件 |
| `-file-operation-timeout` | `advisory.Config.FileOperationTimeout`；包含内容 revision 重试 |
| `-sweep-interval` | `objectstore.Options.SweepInterval`；Azure/local 共用 |
| `-sweep-batch` | `objectstore.Options.SweepBatch`；Azure/local 共用 |

server 在接触 local-store root 前先完成 HTTP option 校验并取得 listener，再执行 `localstore.Open` 与首次 `Status`。只有 READY marker、identity binding、recovery、容量和所有 component 状态都成功，并且 listener 与 signal handling 都已建立后，才打印 `serving ... at http://...`；accept loop 紧接着启动。启动失败会关闭已经取得的 listener；storage 资源只在 cleanup 无错误证明 SQLite handles 已关闭后释放。pool cleanup 无法证明 handle 状态时，lifetime lock 按本节规则保留到进程退出，cleanup failure 并入命令结果。

本地持久形态提供 metastore change log，复制 endpoints 可用，client 可以建立本地元数据副本。HTTP request、non-streaming response、replication frame 与 backend result budget 见 [`architecture.md`](architecture.md#五复制那三个操作)和[同文第六节](architecture.md#六请求与响应的内存边界)。
