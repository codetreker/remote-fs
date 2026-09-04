# Agent Note: 用本地磁盘持有对象存储命名空间

Status: implemented

## 问题

[对象存储命名空间](./2026-08-21-namespace-in-an-object-store.md)把一份 workspace 拆成两项持久状态：metastore 持有树、属性、用量与变更日志，`Objects` 持有按不透明键寻址的不可变字节。Azure Blob 能承担后一项，却让只需要单机持久化的部署也必须提供网络服务、凭据与另一套故障面。S7 与 R-INT-13 要求随附一种只依赖一台 Linux 机器和一个私有持久目录、重启后继续服务同一 workspace 的形态。

把对象改放进普通目录并不足以形成一份可部署的 storage。对象文件、SQLite、两者的归属关系、独占所有权、初始化中断、断电后的目录项持久性、物理磁盘容量、垃圾回收和关闭顺序必须共同给出一个答案。任一项各自成立而组合关系未经证明时，最危险的结果不是启动失败，而是 SQLite 指向另一批对象、一次已返回成功的写在重启后消失，或者磁盘已满却仍向调用方报告 workspace 尚可写。

这份实现还运行在可嵌入的 server 里。单个对象、并发操作、在途字节、恢复工作和后台清扫若没有上限，一个合法流量或一次崩溃留下的状态就能无界占用宿主进程与磁盘（R-INT-3）。

## 决定

交付两层可分别复用的实现：

| 包 | 责任 |
|---|---|
| `packages/storage/objectstore/localdisk` | 在一个本地文件系统目录里实现 `objectstore.Objects`，负责对象格式、原子发布、完整性、恢复、物理容量和生命周期锁 |
| `packages/storage/localstore` | 打开并绑定本地对象目录与 SQLite metastore，完成初始化协议、workspace 配额、后台清扫、状态查询和整体关闭 |

`cmd/remote-fs-server -listen ADDR -local-store DIR -workspace NAME -quota SIZE` 打开这份组合实现。`DIR` 必须已经存在，是服务进程拥有的 `0700` 目录，并且路径上的祖先目录不能允许信任边界之外的主体替换下一级路径。一个进程在整个服务期内独占该 root；第二个 opener 以 `EBUSY` 失败。`-http-max-write-bytes` 单独限制 file-content request；省略时继承 `-http-max-body-bytes`。local-store 形态在接触 root 之前要求有效 write 上限不大于 `-local-max-object-bytes`，避免 server 先保留一份 backend 必定以 `EFBIG` 拒绝的 request body。

### 范围切分

范围包含这些结构性保证：

- **Guarantee：** 对象以 create-only 方式原子发布，成功返回前建立文件内容与目录项的持久化顺序（R-CON-3、R-ERR-3、R-WS-6）。
- **Guarantee：** root、对象、metastore 和完成标记互相证明属于同一份 store/workspace；证明缺失、格式不明或内容损坏时以错误停止（R-ERR-1、R-ERR-2、R-ERR-6、R-ERR-7、R-ERR-8、R-INT-11）。
- **Guarantee：** workspace 的逻辑配额与底层文件系统的实测余量共同限制可写量；两者失败时保留原本的失败，不用另一半拼出成功答案（R-WS-5、R-ERR-6）。
- **Guarantee：** local object 的操作数、在途字节、恢复记录与单轮清扫工作，以及 HTTP request/response retention 都有可配置上限；pending object ledger 有 reservation-admission 阈值和 over-limit 状态，维护失败可查询（R-INT-3、R-ERR-4）。
- **Guarantee：** 持久文件仅属主可读，root 中的符号链接、非常规文件和不受保护的路径被拒绝（R-SEC-3）。

[有界的 Read 与 List response](./2026-09-04-bounded-read-and-list-responses.md)同样是 **Guarantee**：server 只接受实现 `storage.BoundedStorage` 的 backend，调用时把预算传到产生结果的一侧，并以独立的 response operation、waiter 与 aggregate byte admission 约束 handler 持有的结果。localdisk 在 envelope 已验证、payload allocation 和 read admission 之前执行 read 上限；SQLite 按序逐项填充有界 listing result，不先建立完整中间 slice。

压缩、加密、按内容去重、pack/compaction、远程文件系统上的 root、多个 active owner，以及 active-active 共享同一份本地 store 都是 **Feature**。它们可以各自增加；当前没有对应能力，已知远程文件系统类型与第二个 owner 在打开时明确失败，未知文件系统仍须承担未被探针证明的 crash semantics。跨平台实现同样不在范围内，系统当前只承诺 Linux（R-INT-8）。

下列延期属于 **Shape**，因此当前格式受这些禁令约束：

- 对象键不得由内容摘要充当；节点版本、对象身份与内容完整性必须保持为三个值，否则去重会把「同样的字节」误成「同一个对象」。
- 对象不得裸写成只有 payload 的文件；manifest 与 envelope 必须带版本，使加密、压缩或格式迁移可以拒绝未知形状，而不靠猜测解释旧字节。
- metastore 不得仅凭路径邻接就假定对象属于自己；绑定必须使用持久 store ID，移动或恢复文件时仍能验证归属。
- 后台维护不得成为一次 namespace 修改成功的条件；垃圾记录必须持久，清扫失败由状态暴露并可重试。

### root、初始化与所有权

持久目录的主体形状是：

```text
root/
  FORMAT
  OWNER.lock
  LOCALSTORE
  metastore.sqlite
  objects/
    .store-identity
    <00..ff>/
      .store-identity
```

`metastore.sqlite-wal`、`metastore.sqlite-shm` 与 rollback journal 按 SQLite 运行状态出现；256 个 shard 只在第一次写入落到其中时创建。`FORMAT` 保存带版本和 checksum 的随机 UUID；objects root 与每个 shard 另有带版本、checksum、UUID、directory kind 与 shard byte 的 identity marker，防止同一文件系统上的目录被 graft 到另一份 store。对象键先以 SHA-256 首字节分 shard，再用无 padding 的 URL-safe base64 编入一个文件名；编码可逆且不把键解释为路径。`LOCALSTORE` 保存同一 UUID、唯一 workspace name 与 checksum，因此一个 root 永久持有一份 workspace，以另一个名字重新打开会失败。SQLite schema 的 `backing_store` 单例行保存 UUID：`sqlite.OpenBoundWithOptions` 只允许空库建立绑定，已绑定的库只能由同一 ID 打开，已有 namespace 的未绑定库不能事后宣称属于这批对象。local store 每次打开还要求数据库恰有一个 namespace，且名字与完成标记相同。

`localstore.Open` 通过 `localdisk.Options.CompositeInitialization` 请求组合初始化。`localdisk.Open` 先取得 root 的 lifetime lock，再完整验证无 `FORMAT` root 中已有的 `OWNER.lock`、`.FORMAT.stage`、`objects/`、publication probe 与初始化意图；任一项 owner、mode、type 或 filesystem identity 不符时不创建任何新状态。验证通过后，它才 create-only 地建立并同步一个 empty `LOCALSTORE.init`，随后建立 `OWNER.lock`、`objects/` 与 `FORMAT`；返回 `CompositeInitializationStarted`、`CompositeInitializationResumed` 或 `NoCompositeInitialization`。组合层拿到 store UUID 后，在任何 metastore 文件可以被创建之前，把 UUID、workspace name 与 checksum 写入 `.LOCALSTORE.init.stage`，`fsync` 后以 rename 原子替换初始化意图并同步 root。metastore 成功绑定并打开之后，组合层才以 create-only 的 hard link 发布同一绑定的 `LOCALSTORE`，最后删除并同步初始化意图。启动中断可以凭已绑定意图继续；崩溃若发生在 empty intent 阶段，尚无 metadata，下一次 opener 可以选择 workspace。一个只有 `FORMAT`、却既无完成标记也无初始化意图的 root，不能被补造一份空 metastore；绑定之后以另一个 workspace 打开、完成标记存在而数据库缺失、UUID 不符或数据库无法证明绑定时同样失败。

root directory descriptor 与 `OWNER.lock` 都持有非阻塞排他 `flock`。`localstore` 的 root anchor 保存打开时的 device/inode；`localdisk.VerifyRootPath` 则在每次核对时比较 root descriptor 与配置路径的 type、inode 和 `statx` mount ID。组合层在打开 SQLite 前后执行两项验证；路径祖先的 owner、mode 与 sticky-bit 规则阻止其他本地用户替换路径组件。同 UID 进程和管理员仍属于部署信任边界，文件锁和这项即时核对都不被描述为抵御该边界内部的恶意修改。

已格式化的 root 不是可自行补造状态的目录。缺失或畸形的 `FORMAT`、`OWNER.lock`、`objects/`，无法解析的 recovery record，以及操作途中发现的 untracked staging state 都失败关闭；lazy shard 和 ordinary final object 只在 recovery 或 data operation 实际打开它们时验证，尚未创建仍是合法缺席。root 在任何修改前记录 device、filesystem type 与 `statx` mount ID；`FORMAT`、`OWNER.lock`、`.FORMAT.stage`、`objects/`、publication probe、recovery record、shard、staging 与 final object 每次打开或创建都必须与它相同。已有状态跨 device/type/mount 是损坏，以 `EIO` 失败；只有 root 本身属于已知远程文件系统，或运行环境不能提供 mount identity / hard-link publication 能力时使用 `EOPNOTSUPP`。初始化绑定、完成标记、metastore 主文件与辅助文件也必须留在 root 的 device/mount。metastore 主文件必须 single-link，WAL/SHM/journal 在 SQLite 已 unlink 但 descriptor 尚打开时可以是 zero-link；它们只要仍是属主拥有、没有多余 hard link 的 regular file，错误 mode 就会在服务前修复为 `0600`。symlink、错误 owner/type、多重链接或跨 mount 仍被拒绝。已知的 NFS、CIFS/SMB、9P、AFS、CephFS、Coda 与 NCP 文件系统在打开时以 `EOPNOTSUPP` 拒绝；其它类型仍必须通过 hard-link create-if-absent 发布探针。探针证明正常运行需要的原子操作，诚实的本地 `flock` 与 crash-time `fsync` 语义仍是部署前提；`statx(STATX_MNT_ID)` 是这份实现的运行时要求。

`localstore.Store` 拥有两半资源。关闭开始时先拒绝新的 namespace 操作，再取消并等待后台清扫，随后等待已进入的操作完成、关闭 metastore，最后关闭对象目录与 lifetime lock。并发的 `Close` 调用得到同一个合并结果；metastore 与 object store 的关闭错误都不会被丢弃。

### 对象格式、完整性与原子发布

一个对象文件由固定 72-byte envelope、原始 key 和 payload 组成。envelope 保存 magic、格式版本、store UUID、key 长度、payload 长度与 payload 的 SHA-256。`Get` 在返回任何 payload 前验证文件是属主拥有的 `0600` regular file，随后验证格式、UUID、原始 key、文件长度、配置上限与 SHA-256；symlink、目录替换、截断、等长篡改、跨 store 搬入和 envelope/key 不一致均以 `EIO` 失败。`FORMAT`、绑定后的初始化意图与完成标记带 checksum；最初的 empty intent 只证明 root lock 下已经开始组合初始化，绑定完成后才证明 UUID 与 workspace。任何一种畸形状态都不会被解释为一个新 store。

recovery record 不是空的哨兵文件。它是带版本和 checksum 的固定 binary record，保存 store UUID、put/delete operation 与原始 key 的 SHA-256；文件名仍以可逆编码携带 key。恢复开始前同时验证名字、内容与 filesystem identity，另一份 store 留下的同名 record 不能获得删除本 store 对象的权限。

`Put` 对一个 key 的持久化顺序是：

1. 在 `objects/` 根下创建并 `fsync` 一条恢复记录，再 `fsync` 该目录；
2. 在目标 shard 里独占创建 staging file，写入 envelope、key 与 payload，并 `fsync` 文件；
3. 用同一目录内的 `linkat(2)` 把 staging inode 发布到最终名字；目的已存在时返回 `EEXIST`，不会覆盖；
4. `fsync` shard，删除 staging 名并再次 `fsync` shard；
5. 删除恢复记录并 `fsync` `objects/` 根。

`Delete` 在 unlink 前同样先持久化恢复记录，unlink 后 `fsync` shard，再删除并同步恢复记录。一个对象已经不存在是成功状态，但已有 shard 仍执行 directory `fsync`，使上一次「unlink 已发生、barrier 失败」的重试可以收敛。打开 store 时逐条恢复 `.put-*` 与 `.delete-*` 记录：未完成的 put 清除 staging，已发布 put 保留 final；delete 确保 final 与 staging 都消失，然后清除记录。delete record 与 staging 同时存在、不同 inode 的 FORMAT/stage 或 publication-probe residue，以及 foreign recovery 内容都是不可能从合法协议产生的状态，必须保留现场并以 `EIO` 停止，不能按最方便的方向猜测恢复。

一次 directory barrier 或恢复状态清理失败之后，进程无法再证明它刚才的磁盘修改处于哪一侧。`localdisk` 保存首个 durability failure，使后续 `Get`、`Put`、`Delete`、容量与状态操作以 `EIO` 失败；它不会在已知不可信的磁盘状态上继续给出看似正常的对象答案。

### SQLite 与组合一致性

metastore 使用 WAL，writer connection 明确设置 `synchronous=FULL`、`foreign_keys=ON` 与 immediate transaction，并在打开时读回 `PRAGMA synchronous` 验证生效值。一个 `Reserve` 事务在上传前记录对象 key、请求的 payload 大小与创建时间；`Commit` 再把对象改为 referenced、记录 digest，并在同一事务中更新路径、逻辑用量和变更日志。`Put` 只有 nil error 才证明 create-only publication 完成；任意 `Put` error 都通过 storage-lifetime context 调用 `Quarantine`，把 reservation 变成不可提交、不可清扫的 unresolved。只有 `Put` 成功而 `Commit` 失败时才 `Abandon` 成 garbage，因为这一条路径已经证明本次 reservation 创建了对象。每项 cleanup failure 都与原失败一并返回。进程在 publication 结果确定前崩溃时，reserved record 同样不会因年龄获得删除权限；这项 supersession 由[未证实对象发布进入 unresolved](./2026-09-04-unresolved-object-publication.md)记录。对象字节先持久化、命名空间后指向它，所以失败与崩溃留下的是没有名字引用的 bounded pending state，不是有名字却没有字节的文件。

`sqlite.ObjectLimits` 是一个 namespace 对 reserved、unresolved 与 garbage 合计数量及 recorded payload bytes 的 reservation-admission 阈值。一个 requested object 自身大于 byte 阈值时以 `EFBIG` 拒绝，因为任何维护都无法使它单独装下；对象本身可装下、但新增 record 会让当前 count/bytes 越过阈值时，`Reserve` 在同一事务里以 `EAGAIN` 拒绝。失败 upload 因而不能靠不断建立新 reservation 无界增加 SQLite。它不是 garbage 的硬上限：`Commit`、`Abandon`、`Quarantine`、删除、删目录和 rename 都不查询阈值，因为权威提交与终止当前 reservation 的操作必须仍可达。`Commit` 从 pending 合计中移除 record；`Abandon` 与 `Quarantine` 只改 state，不增加 count/bytes；解引用操作则可以新增 garbage。因此清扫不可用、unresolved 只能留存，或配置在重开时收紧，都可使既有 backlog 处于 over-limit；打开不因此失败，`ObjectStatus.OverLimit` 会暴露它，新 reservation 失败，直到 garbage 清扫或显式 recovery 让合计值回到阈值内。打开与 `ObjectStatus` 查询会校验未知 object state、负 size、referenced/node 关系、rooted tree、used accounting 与 SQLite storage class；entry name 必须是 nonempty BLOB，log tail/position 必须自洽，change 的 kind-dependent node/from nullable group、mode/size/nsec range 与 payload class 必须完整。拒绝发生在 snapshot/`Since` 之前，避免 TEXT/BLOB cursor 排序漏行或 NULL scalar 被 driver 还原成可信零值。执行 recursive reachability CTE 前先用 scalar aggregate 限制 namespace seed、nodes、label/parent/child 任一关系触及该 namespace 的 distinct entries、objects、mandatory log row 与 changes 的合计；legacy preflight 则统计 database 全部对应 rows。总 work 超过 `MaxIntegrityRecords` 时以 `EFBIG` 拒绝，不启动递归遍历。legacy v1/v2 open 在 schema migration 前执行同一 preflight，因此拒绝不会留下半迁移数据库。`Reserve` 的正常 admission 查询不承担全表校验。

startup 还要求 `logs.committed_position` 与最新 surviving change position 完全一致。任一方向 mismatch 都按[日志尾完整性](./2026-09-04-log-tail-integrity-fails-open.md)在 trim 与其它 maintenance 之前以 `EIO` 拒绝，不通过 mint 新 incarnation 把 corruption 解释成一段新历史。

本地对象的 SHA-256 是实现自己对输入 buffer 的完整性记录，因此 `localdisk.Put` 按 `Objects` 契约返回 `nil` digest；它不能冒充下层服务对「实际存下来的字节」给出的独立报告。Azure Blob 仍返回服务端 digest，metastore 的 digest 字段继续允许 `NULL`。完整性、对象身份与将来的去重判据没有被合并。

schema 通过只追加的 `0003_backing_store.sql` 从 v2 前滚到 v3。v2 的最终形状由 `testdata/version2.sql` 独立钉住，v1、v2 和当前 schema 都有各自的结构见证；已落地迁移不随当前 DDL 改写。这补上了[元数据复制](./2026-08-27-metadata-replication.md)留下的「下一个迁移必须为 v2 建历史见证」义务。

### 逻辑配额与物理容量

`objectstore.Objects` 增加 `Available(ctx)` 与 `Close()`。前者只回答 backing store 还能提供多少 payload byte，不回答 workspace 的配额；无法实测有限物理容量的实现稳定地返回 `ENOSYS`。`objectstore.Storage.Space` 先取得 metastore 的逻辑 `Total / Used / Avail`，验证三者自洽，再把 `Avail` 收紧为它与 `Objects.Available` 的较小值。只有错误树的每个叶子都是 `ENOSYS` 时才使用逻辑数；一个同时含 `ENOSYS` 与 I/O failure 的错误仍然失败，不会被当作「此能力不支持」。

本地 store 要求部署方给出至少 4096 bytes 的正 quota。metastore 的 `Used` 只统计名字仍引用的逻辑 payload；reserved、unresolved 与 garbage 不吃 workspace 配额，但继续占物理盘，分别由状态查询报告。写入在 `Reserve` 与 `Commit` 的事务中执行逻辑配额，越过 workspace allowance 返回 `EDQUOT`。

`localdisk.Available` 从 root 的 `statfs` 取得 `f_bavail * f_bsize`，再扣除 maintenance reserve、并发 publication 已预留的 envelope/key/payload，以及一次最大 envelope/key overhead；inode 计数可用而剩余量少于四个时，可写量为零，因为一个尚无 shard 的 key 还需要 shard directory、shard identity、recovery record 与 staging/final inode。一次 `Put` 建好并验证 shard 后，在建立恢复记录前以同一把容量锁预留完整 encoded object 大小；此时少于两个 inode 或 byte 余量不足都返回 `ENOSPC`。这些数字是从 `statfs` 得出的保守 ceiling，不承诺文件系统的 block rounding 或 absolute capacity，也不能阻止同一文件系统上的外部写者同时消耗容量；最终系统调用的 `ENOSPC` 继续原样暴露。

这项组合保持了[workspace 容量上限](./2026-08-21-space-limit.md)的语义：`Total` 与 `Used` 是 workspace 的逻辑账，`Avail` 同时服从额度与底层现实。它也使「物理盘满」与「workspace 额度用完」继续分别表现为 `ENOSPC` 与 `EDQUOT`。

### 维护、资源上限与状态

`localdisk.Options` 的零值选择一组有界默认值：单对象 payload 1 GiB、64 个在途 data operation、2 GiB encoded/read payload 在途字节、64 MiB maintenance reserve，以及打开时最多检查 4096 条 recovery record。status 与 root verification 共用一项独立的 serialized control slot，因此 data admission 饱和时仍有一条诊断路径；关闭会同时排空 data 与 control operation。`sqlite.ObjectLimits` 的默认 pending backlog 阈值是 4096 个对象与 8 GiB recorded payload；SQLite ordinary/event reader pool 与 long-lived snapshot reader pool 默认各最多 16 条 connection，后者独立存在，使慢 snapshot 不会耗尽 bounded log `Since` 与 namespace read 的全部名额。aggregate integrity work 默认最多 1,000,000 个 namespace/node/entry/object/log/change records。`localstore.Config.MaxReaderConnections`、`MaxSnapshotReaderConnections` 与 `MaxIntegrityRecords` 的零值继承对应默认值；两个 reader limit 必须为正且有限，integrity limit 至少是 `sqlite.MinIntegrityRecords = 3`（namespace seed、root 与 mandatory log row）且不能取 `math.MaxInt64`。这些配置在 root 或 database 被接触前验证。local store 要求有效 pending-byte 阈值至少容纳有效的单对象 payload 上限，避免一份配置声明对象可写、reservation 却永远不能接纳它。local object key 最多 120 bytes，使可逆编码、staging 名与恢复记录都落在受支持本地文件系统的单个 pathname component 内；打开时还会核对实际 `NAME_MAX`。local store 的 workspace name 最多 1024 bytes，并被持久化进完成标记。恢复记录上限不得小于在途操作上限；单对象加 envelope/key 必须装进在途字节上限。等待容量或操作名额遵从 context cancellation，关闭会唤醒等待者并排空已进入的操作。server 以 `-max-pending-objects`、`-max-pending-bytes`、`-max-reader-connections`、`-max-snapshot-reader-connections` 与 `-max-integrity-records` 为 local-store 和 Azure Blob 两种 metastore-backed 形态暴露相同的 SQLite resource bounds。

`objectstore.New` 与 `objectstore.NewWithOptions` 都拥有启动、event-driven continuation 与 periodic retry 三种清扫触发；前者使用 `objectstore.DefaultOptions()` 的一分钟周期和 64 个对象 batch，后者接受显式配置。单轮 batch 必须在 1 到 `objectstore.MaxSweepBatch = 1 << 20` 之间，避免一次 maintenance attempt 退化为无界工作。`cmd/remote-fs-server` 用通用的 `-sweep-interval` 与 `-sweep-batch` 为 Azure 和 local-store 两种 objectstore-backed 形态配置后者；默认值同样是一分钟与 64，超过 1,048,576 或用于 directory 形态都在打开 storage 之前被拒绝。会产生 garbage 的成功内容替换/删除，以及 `Put` 已成功但 `Commit` 失败后的 `Abandon`，向 coalesced channel 投递非阻塞信号；一轮若删满 batch，会继续安排有界工作，直到不足一批、失败或被取消。普通 `Put` error 进入 unresolved，不触发删除。显式与自动清扫共用 context-aware permit 串行执行。garbage record 在删除成功之后才从 metastore 忘记；失败保留 record，`MaintenanceStatus` 公开最近一次时间、删除数与完整错误。namespace 操作不等待对象删除，关闭也不执行无界且可能失败的最终清扫；worker 停止后才完成的 operation 若留下 garbage，由 durable record 与下次打开的初始清扫接管。

`localstore.Status` 在一次查询中返回绑定的 workspace、逻辑空间、reserved/unresolved/garbage 的数量与字节、pending 阈值及 `OverLimit`、effective SQLite ordinary/snapshot reader-connection 与 integrity-record work limits、物理可用量、在途操作与字节、recovery record 数、durability failure 和最近的清扫结果。它直接读取 metastore 的逻辑 `Space`，再通过 control slot 调用 `localdisk.Status` 并收紧可写量，不经过 namespace `Space` 的 ordinary object admission。某一项查询失败时仍收集其它项，并把所有错误合并返回；部分字段可用于诊断，但整个快照不会被标成成功。server 的 SIGHUP 对 local store 执行这项只读查询，不重算事务内维护的逻辑用量；同一时刻至多运行一项查询，每项带独立的两秒 context deadline，查询不会阻塞主 signal loop。deadline 依赖下层操作合作取消，已经进入不可取消的系统调用时不是硬性的 wall-clock 上限。

HTTP server 与 client 的 non-streaming body 同样有 1 GiB 默认单体上限；`MaxWriteBytes` 是其中独立的 file-content 上限，零值继承 `MaxBodyBytes`，非零值不得更大。handler 分别限制 request 与 response 的 operation、waiter 和 aggregate retained bytes；response reservation 覆盖 storage result、wire conversion 与 encoded body 可同时存在的保守峰值。server 在读取 `Write` body 前按 write 上限取得 request 名额，在读取 `SetAttr` 前按一般 body 上限取得名额，bodyless operation 只探测是否出现第一个字节；过大的 write 在调用 storage 前以 `EFBIG` 失败。Read 预算传给 backend，List 逐 entry 预留并提交到有界 `ListResult`；超限或中途失败使整个 result 不可读取，不会先完整 materialize 再丢弃。CLI 用 `-http-max-write-bytes` 暴露 write 值，省略时继承 `-http-max-body-bytes`；local-store 还要求继承或显式给出的结果不超过 `-local-max-object-bytes`。CLI 选项与 local-store 组合配置先完成校验，listener 也在 root 初始化之前取得；独立 command 的 connection/header/idle bounds 见[HTTP connection 上限](./2026-09-04-standalone-http-connection-limits.md)。

## 备选方案

**更小的切法：只交付 `localdisk.Objects`，让部署方分别配置 SQLite 与对象目录。** 少一层 package，也能复用现有 `objectstore.Storage`。输在两份路径配置没有持久绑定，初始化中断无法判定该补建数据库还是拒绝，关闭顺序与 lifetime lock 也落到每个集成方手里；一次配错会把一棵树指向另一批字节。`packages/storage/localstore` 把这些组合义务放在唯一拥有两半资源的位置。

**直接使用 `localdir`。** 目录本身同时持有名字和字节，部署最短。输在它没有 metastore 的事务、持久变更日志与绑定身份，不能走 metadata replication 的生产路径，也不能以不可变对象加原子指针切换来隔离 namespace commit。它仍是一份合法的独立 storage，不承担本决定要提供的部署形状。

**把 payload 存进 SQLite BLOB。** 一份文件就能同时提交树和字节，备份与绑定最直接。输在整文件写入进入单 writer 与 WAL，大对象复制、checkpoint 和数据库膨胀都被放到元数据关键路径；把字节留在独立不可变文件里，使 SQLite 事务只承担小而固定的 metadata 修改。

**内容寻址或 pack file。** 会直接买到去重、更少 inode 或顺序 I/O，是更大的切法。输在引用计数、碰撞信任边界、compaction、读者 pinning 与 crash recovery 会同时进入交付；而[按内容哈希合并重复对象](../../proposed/architecture/2026-08-21-merge-duplicate-objects.md)尚未证明重复量值得这套结构。versioned envelope 与不透明 key 保留以后增加它们的空间。

**只依赖 rename 发布。** staging rename 到 final 的代码更短。输在常规 rename 会替换已存在的目标，不能兑现 create-only `Put`；依赖 `RENAME_NOREPLACE` 又把发布绑定到另一组文件系统支持。hard link 在同一 shard 内原子增加 final 名，并天然以 `EEXIST` 拒绝重复 key，启动探针还能直接验证这两个性质。

**把 quota 当作唯一容量。** 不查询 backing filesystem，组合层无需扩展 `Objects`。输在 workspace 尚有额度而物理盘只剩维护空间时会报告一份不存在的可写量；SQLite checkpoint、删除 recovery record 与垃圾回收随后都可能失去落脚处。逻辑与物理容量各自实测再取较小值，保留了两个限制的不同含义。

**先运行 recursive integrity CTE，结束后再判断 namespace 是否太大。** 查询最直接，也能得到精确 reachable set。输在 resource ceiling 只在最昂贵的 allocation 已经发生后生效；scalar count preflight 先限制完整 retained-record work，再允许递归验证，才能让 `MaxIntegrityRecords` 成为 admission 而不是事后诊断。

**同时加入压缩、加密、去重、pack、远程文件系统和多 owner。** 会减少以后再改磁盘格式的次数。输在这些能力没有需求依据，而且会把 key 管理、密钥轮换、compaction、分布式锁与新的恢复状态一起放进正确性证明。当前格式只约束不封死这些方向，不预先交付它们。

## 后果

买到的：

- 单机部署可以沿用 object-store namespace 的事务、节点身份、持久变更日志与 metadata replication，不依赖 Azure 服务。
- 每一份持久状态都有可核对的格式、store ID 与初始化完成条件；错误的 metastore/object pairing、损坏或不完整 root 在服务流量之前被拒绝。
- 对象 publication 与 deletion 有逐步落盘顺序和重启恢复记录；一次无法判定持久性的失败会毒化当前实例，不继续返回未经证明的成功。
- workspace 的逻辑余量与宿主文件系统的物理余量同时进入 `Space`，reserved、unresolved 与 garbage 的物理成本可查询，删除与 SQLite 维护保有独立空间。
- local object 层、namespace 清扫、HTTP request/response 与关闭过程都有显式生命周期和资源上限；pending object ledger 有显式 admission 阈值，失败的后台工作不会被成功修改的返回值吞掉。
- `Objects` 的共同契约由 `objectstoretest` 在 memory、Azure Blob 与 local disk 实现上复用；local disk 另对格式损坏、断电 seam、容量、恢复、锁与 root 替换进行故障注入。组合层的失败分工见[对象接口上的失败注入](../testing/2026-08-22-failures-injected-at-the-objects-boundary.md)。

付出的：

- 一个 logical file 对应一个独立 immutable object，覆盖一个 byte 仍写出整份 payload；大量小对象还会消耗 inode 与目录项。pack、增量块与去重不在当前实现里。
- `Get` 必须分配完整 payload 并读完后校验 SHA-256 才返回；上限使内存有界，但没有 streaming read/write。
- HTTP response 仍是 non-streaming：每个成功 Read/List 在配置上限内保留完整结果和 encoded body。response admission 为 listing 的同时存在形状预留保守倍数，换来确定上界，也会在实际结果较小时降低可并发数量。
- SQLite 仍只有一个 writer。对象 I/O 不在事务内，不同 key 的对象操作可以并行；metadata commit 仍在本地数据库的单写者处排队。
- 超过 integrity-record ceiling 的合法大 namespace 会在 open 或 `ObjectStatus` 时以 `EFBIG` 失败，必须显式提高 `MaxIntegrityRecords`；默认一百万不是对 namespace 规模的协议上限，而是阻止 recursive validation 与完整关系检查无界工作的 serving configuration。
- root 必须位于满足本地 `flock`、hard link 与 `fsync` 语义的文件系统，权限与祖先目录约束比服务一个普通目录更严格。未知文件系统通过探针不等于其 crash semantics 已被运行时证明。
- `statfs` 只能描述采样时刻，root 所在文件系统的其它使用者能在采样后抢走空间；filesystem metadata、block rounding、copy-on-write 与 delayed allocation 也不等于 payload 的逻辑字节。publication reservation 只协调本 Store 进程内的写入，最终系统调用仍可能返回 `ENOSPC`。
- 进程若在 reservation 建立后、publication 结果确定前崩溃，该 record 不会自动获得删除权限。普通 `Put` error 也留下 unresolved；两者可能永久占用 pending admission，直到未来有带归属证明的 recovery 工具处理。localdisk 自己的 staging/recovery residue 仍在下次打开时按已验证的本地协议收敛。
- pending 阈值是 reservation admission，不是强制删除既有状态。释放 namespace 的操作可以把 garbage 推到阈值之上，unresolved 可以使 backlog 无法由清扫下降，调低配置也可以在 reopen 时得到 `OverLimit`；这些情况下能独立装下的新写以 `EAGAIN` 停止，单个 requested object 超过 byte 阈值则是 `EFBIG`。删除与 garbage 清扫继续有路可走，但不会越权删除 unresolved。
- 打开时恢复工作被 `MaxRecoveryEntries` 硬性限制。超过上限会以 `EOVERFLOW` 停止服务，运维必须先诊断异常积累，系统不会为了可用性无界扫描或丢弃未知记录。
- store root 是本系统拥有的格式，不是可用文件管理器直接编辑的目录。旁路修改任何对象、marker、SQLite 文件或权限都可能使下一次访问或打开失败；这是 fail-closed 的结果。

仍存在的 Azure 与通用 object-store 缺口由[对象存储后端已知没做的事](../../proposed/architecture/2026-08-22-gaps-in-the-object-store-backend.md)继续跟踪。对象发布错误的归属规则由[未证实对象发布进入 unresolved](./2026-09-04-unresolved-object-publication.md)拥有；本决定不改变[过期写入拒绝与 write lease](../../proposed/architecture/2026-08-20-nothing-pins-an-open-file.md)、[在途读者 pinning](../../proposed/architecture/2026-08-21-readers-in-flight-and-the-sweeper.md)或[按内容合并对象](../../proposed/architecture/2026-08-21-merge-duplicate-objects.md)的状态。
