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

`cmd/remote-fs-server -listen ADDR -local-store DIR -workspace NAME -quota SIZE` 打开这份组合实现。`DIR` 必须已经存在，是服务进程拥有的 `0700` 目录，并且路径上的祖先目录不能允许信任边界之外的主体替换下一级路径。一个进程在整个服务期内独占该 root；第二个 opener 以 `EBUSY` 失败。server 的文件占有状态首次建立需要 `-initialize-lock-state`，之后按持久 binding 重开。`-http-max-write-bytes` 单独限制 file-content request；省略时继承 `-http-max-body-bytes`。local-store 形态在接触 root 之前要求有效 write 上限不大于 `-local-max-object-bytes`，避免 server 先保留一份 backend 必定以 `EFBIG` 拒绝的 request body。

[显式文件占有](./2026-09-07-file-locks.md)把运行期 authority 与 SQLite 最终发布绑定，并在这个私有 root 中保存重启保护证据。它沿用本决定的对象／metastore 分工、独占所有权与失败关闭规则；本 note 继续拥有对象格式、组合初始化与磁盘持久性的理由。

### 范围切分

范围包含这些结构性保证：

- **Guarantee：** 对象以 create-only 方式原子发布，成功返回前建立文件内容与目录项的持久化顺序（R-CON-3、R-ERR-3、R-WS-6）。
- **Guarantee：** root、对象、metastore 和完成标记互相证明属于同一份 store/workspace；证明缺失、格式不明或内容损坏时以错误停止（R-ERR-1、R-ERR-2、R-ERR-6、R-ERR-7、R-ERR-8、R-INT-11）。
- **Guarantee：** workspace 的逻辑配额与底层文件系统的实测余量共同限制可写量；两者失败时保留原本的失败，不用另一半拼出成功答案（R-WS-5、R-ERR-6）。
- **Guarantee：** local object 的操作数、在途字节、恢复记录与单轮清扫工作，以及 HTTP request/response retention 都有可配置上限；pending object ledger 有 reservation-admission 阈值和 over-limit 状态，维护失败可查询（R-INT-3、R-ERR-4）。
- **Guarantee：** 持久文件仅属主可读，root 中的符号链接、非常规文件和不受保护的路径被拒绝（R-SEC-3）。

[有界的 Read 与 List response](./2026-09-04-bounded-read-and-list-responses.md)同样是 **Guarantee**：server 的成对 namespace 必须实现 `storage.BoundedStorage`，调用时把预算传到产生结果的一侧，并以独立的 response operation、waiter 与 aggregate byte admission 约束 handler 持有的结果。localdisk 在 envelope 已验证、payload allocation 和 read admission 之前执行 read 上限；SQLite 按序逐项填充有界 listing result，不先建立完整中间 slice。

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
  .FORMAT.stage
  OWNER.lock
  LOCALSTORE
  .LOCALSTORE.stage
  LOCALSTORE.init
  .LOCALSTORE.init.stage
  METASTORE
  .METASTORE.stage
  .leases.intent
  .leases.witness
  .leases.intent.stage
  .leases.witness.stage
  .leases.probe-source
  .leases.probe-target
  metastore.sqlite
  metastore.sqlite-journal
  metastore.sqlite-wal
  metastore.sqlite-shm
  objects/
    .store-identity
    .publish-probe-source
    .publish-probe-target
    .publish-probe-occupied
    .put-k...
    .delete-k...
    <00..ff>/
      .store-identity
      .store-identity.stage
      k...
      .stage-k...
```

`LOCALSTORE.init`、staging name、probe 与 recovery record 只在对应初始化、publication、删除或 cleanup 尚未收敛时存在；`metastore.sqlite-wal`、`metastore.sqlite-shm` 与 rollback journal 按 SQLite 运行状态出现。`.leases.intent` 与 `.leases.witness` 是占有绑定后的持久文件，完成初始化后仍保留。256 个 shard 只在第一次写入落到其中时创建。`FORMAT` 保存带版本和 checksum 的随机 UUID；objects root 与每个 shard 另有带版本、checksum、UUID、directory kind 与 shard byte 的 identity marker，防止同一文件系统上的目录被 graft 到另一份 store。对象键先以 SHA-256 首字节分 shard，再用无 padding 的 URL-safe base64 编入一个文件名；编码可逆且不把键解释为路径。

`LOCALSTORE` 保存同一 UUID、唯一 workspace name 与 checksum，因此一个 root 永久持有一份 workspace，以另一个名字重新打开会失败。SQLite schema 的 `backing_store` 单例行保存 UUID；`database_state` 保存 database identity、generation 与 node/change 高水位。`METASTORE` 在 WAL 之外保存相同 store/workspace/database identity、完整 accepted state 与 checkpoint generation。local store 每次打开要求数据库恰有一个 namespace 且名字与完成标记相同；已有数据库必须使用 `RequireExistingNamespace`，丢失 workspace row 不会触发创建。外层 binding preflight 分别以 `LIMIT 2` 读取至多两条 binding/namespace metadata，先验证 storage class 与长度，再加载定长 store ID 和受 `MaxWorkspaceBytes` 限制的名字；额外 namespace 与超大 TEXT/BLOB 都在载入前失败。

`.LOCALSTORE.stage` 不是 READY authority，打开时先删除并同步，再只读取 final marker；stage-only 回到未完成初始化，final+stage 保留 final 并完成 cleanup。

`localstore.Open` 通过 `localdisk.Options.CompositeInitialization` 请求组合初始化。`localdisk.Open` 先取得 root 的 lifetime lock，再完整验证无 `FORMAT` root 中已有的 `OWNER.lock`、`.FORMAT.stage`、`objects/`、publication probe 与初始化意图；任一项 owner、mode、type 或 filesystem identity 不符时不创建任何新状态。验证通过后，它才 create-only 地建立并同步一个 empty `LOCALSTORE.init`，随后建立 `OWNER.lock`、`objects/` 与 `FORMAT`；返回 `CompositeInitializationStarted`、`CompositeInitializationResumed` 或 `NoCompositeInitialization`。组合层拿到 store UUID 后，在任何 metastore 文件可以被创建之前，把 UUID、workspace name 与 checksum 写入 `.LOCALSTORE.init.stage`，`fsync` 后以 rename 原子替换初始化意图并同步 root。

空 intent 只允许与完全缺失的 metastore 相邻；旁边已有零字节主文件、SQLite auxiliary 或 `METASTORE` 时无法证明 workspace，打开失败。已绑定 intent 可以继续缺失／私有零字节数据库的初始化；SQLite 可能在 schema 建立前已经写出非零 header，这时组合层用只读 `sqlite_schema` 查询证明没有任何 schema object，且只在无 READY、无 witness final/stage 时把它归为 bootstrap，判定不依赖 WAL 文件大小。已有 schema 时先读回 backing-store binding 和 namespace cardinality，再在 durable open 的同一事务中要求 workspace 已存在。`METASTORE` 成功发布，且配置的占有 authority 已验证恢复证据之后，组合层才以 create-only hard link 发布同一绑定的 `LOCALSTORE`，最后删除并同步 intent。READY store 必须同时有 initialized database、有效 `METASTORE` 和恰好一条匹配 workspace row；完成标记存在而任一项缺失、UUID／database identity 不符、workspace row 丢失、出现空 intent 或初始化 stage 时都以 `EIO` 拒绝，不补造空 namespace。READY 后遗留的有效 bound intent 是 publication 已完成而 cleanup 尚未完成的唯一可清理状态。

`localstore.Config.Locks` 显式启用成对的 authority，`InitializeLocks` 允许建立首次占有 binding 或继续匹配的 durable intent。root inode 的 create-only xattr `user.remote-fs.lease-state` 与 `.leases.intent` 一起固定 canonical 证据目录、名字、store/workspace 和 StateID；SQLite 的 `lease_recovery` 与独立 `.leases.witness` 保存 Accepted／Prepared 最大租期证据。新的更长租期只有在 Prepared、witness、Accepted 依次持久化后才可确认；每次 SQLite 前进仍经过 `METASTORE`。重开从实际取得数据库原生排他锁时捕获的新 monotonic 起点等待完整持久最大租期，期间拒绝新授权与修改，读取和状态仍可用。配置调小不会提前释放旧保护，省略 `Locks` 也不能把已有 binding 当作未启用。正常重开与显式初始化都探测 `flock`、xattr、原子 rename 和 file/directory `fsync`；改动 canonical root 路径需要显式迁移。恢复状态表与动作退役语义由[文件占有设计](../../../../docs/design/server/file-locks.md)拥有。

root directory descriptor 与 `OWNER.lock` 都持有非阻塞排他 `flock`。启用文件占有的 localstore 同时持有数据库 native file 的 `LOCK_EX`，所有原始 SQLite opener 持有相同文件的 `LOCK_SH`；既存 raw handle 因而不能在同进程或另一进程里继续与 authority 并存。配置恢复与启用 authority 都验证真实 EX ownership，恢复还必须使用与其绑定的原生 `LeaseAnchor`。这些锁只有在全部 SQLite connection 成功关闭后才能释放。`localstore` 的 root anchor 保存打开时的 device/inode；`localdisk.VerifyRootPath` 则在每次核对时比较 root descriptor 与配置路径的 type、inode 和 `statx` mount ID。组合层在打开 SQLite 前后执行两项验证；路径祖先的 owner、mode 与 sticky-bit 规则阻止其他本地用户替换路径组件。同 UID 进程和管理员仍属于部署信任边界，文件锁和这项即时核对都不被描述为抵御该边界内部的恶意修改。

已格式化的 root 不是可自行补造状态的目录。缺失或畸形的 `FORMAT`、`OWNER.lock`、`objects/`，无法解析的 recovery record，以及操作途中发现的 untracked staging state 都失败关闭；lazy shard 和 ordinary final object 只在 recovery 或 data operation 实际打开它们时验证，尚未创建仍是合法缺席。root 在任何修改前记录 device、filesystem type 与 `statx` mount ID；`FORMAT`、`OWNER.lock`、`.FORMAT.stage`、`objects/`、publication probe、recovery record、shard、staging 与 final object 每次打开或创建都必须与它相同。已有状态跨 device/type/mount 是损坏，以 `EIO` 失败；只有 root 本身属于已知远程文件系统，或运行环境不能提供 mount identity / hard-link publication 能力时使用 `EOPNOTSUPP`。初始化 binding、完成标记、`METASTORE`、metastore 主文件与辅助文件也必须留在 root 的 device/mount。metastore 主文件必须 single-link，WAL/SHM/journal 在 SQLite 已 unlink 但 descriptor 尚打开时可以是 zero-link；它们只要仍是属主拥有、没有多余 hard link 的 regular file，错误 mode 就会在服务前修复为 `0600`。symlink、错误 owner/type、多重链接或跨 mount 仍被拒绝。已知的 NFS、CIFS/SMB、9P、AFS、CephFS、Coda 与 NCP 文件系统在打开时以 `EOPNOTSUPP` 拒绝；其它类型仍必须通过 hard-link create-if-absent 发布探针。探针证明正常运行需要的原子操作，诚实的本地 `flock` 与 crash-time `fsync` 语义仍是部署前提；`statx(STATX_MNT_ID)` 是这份实现的运行时要求。

`localstore.Store` 拥有两半资源。关闭开始时先拒绝新的 namespace 操作，再取消并等待后台清扫和 checkpoint worker，随后等待已进入的操作完成；worker 等待发生在 close context 建立前。durable metastore 使用 5 秒内部 context 等待 commit gate 与完整 `FULL` checkpoint；active reader 仍在使用 pool 时立即以 `EBUSY` 拒绝本轮关闭，释放后由下一次 `Close` 重试。reader pools 成功关闭后，checkpoint busy/failure、见证失败或 cancellation 留下 reader 已关、writer 与所需 WAL 证据保留的 retryable 状态；后续调用跳过已完成步骤继续收敛。完整 checkpoint 和 checkpoint witness 已同步后才尝试清除 `PERSIST_WAL`，再关闭 writer pool。清除调用失败时 writer 保留并允许重试，但 file-control 可能已经清除；此时 `A = C`，WAL 不再是必需的恢复证据。任一 reader/snapshot/writer pool close error 无法证明 native handle 状态，成为后续调用持续返回的 terminal error；writer close error 即使发生在 flag 已清除后也保留 ownership。两类失败都保留 object/root ownership 到成功重试或进程退出，只有全部 pools 无错误关闭后组合层才释放对象目录、root anchor 与 lifetime lock。同一轮并发 `Close` 调用得到同一个结果，metastore 与 object store 的关闭错误不会被丢弃。`Open` 在 metastore 建立后失败时先尝试相同的 bounded close；只要尚未产生 terminal pool-close result，未暴露的 store 就执行 terminal `Abort`。它不主动清除 `PERSIST_WAL`：witnessed checkpoint 尚未完成时保留恢复证据；已经完成见证但清除调用报错时 flag 状态未知且 WAL 已非必需。只有无错误关闭后才释放 ownership。SQLite constructor 在 Store 建立前清理 pool 时，任一 close error 同样保留内部 coordinator 和外部 root ownership 到进程退出；全部 pools 成功关闭的普通构造失败才释放两层所有权。

### 对象格式、完整性与原子发布

一个对象文件由固定 72-byte envelope、原始 key 和 payload 组成。envelope 保存 magic、格式版本、store UUID、key 长度、payload 长度与 payload 的 SHA-256。`Get` 在返回任何 payload 前验证文件是属主拥有的 `0600` regular file，随后验证格式、UUID、原始 key、文件长度、配置上限与 SHA-256；symlink、目录替换、截断、等长篡改、跨 store 搬入和 envelope/key 不一致均以 `EIO` 失败。`FORMAT`、绑定后的初始化意图与完成标记带 checksum；最初的 empty intent 只证明 root lock 下已经开始组合初始化，绑定完成后才证明 UUID 与 workspace。任何一种畸形状态都不会被解释为一个新 store。

每个 shard 的打开、创建与 identity 恢复由固定 256 个 context-aware token 中对应的一项串行化；不同 shard 不共享 token，已取得 bounded waiting ticket 的其它 shard 可以继续，同一 shard 的 token 在对象检查与 payload I/O 前释放。waiting capacity 满时新操作以 `EAGAIN` 拒绝。identity 先完整写入并 `fsync` `.store-identity.stage`，再以 `linkat` create-only 地发布 final name；两个名字必须验证为同一 inode、link count 为 2，随后同步 shard、删除 stage 并再次同步。final name 因此不会暴露半写内容，并发的两个首次 `Put` 也不会把同一个空 shard 初始化成互相竞争的 marker。

重开会完成正确且独占 shard 的 stage-only publication，也会清理 final/stage 同 inode 的已同步 publication。final-only marker 必须只有一个 link；stage 与对象并存、两个名字不同 inode、额外 hard link、内容或 store/shard identity 损坏都保留现场并以 `EIO` 停止。live publication 的 link、验证、barrier 或 cleanup 结果无法确定时，整份 `localdisk.Objects` 被 poison 到下一次关闭重开。

recovery record 不是空的哨兵文件。它是带版本和 checksum 的固定 binary record，保存 store UUID、put/delete operation 与原始 key 的 SHA-256；文件名仍以可逆编码携带 key。恢复开始前同时验证名字、内容与 filesystem identity，另一份 store 留下的同名 record 不能获得删除本 store 对象的权限。

`Put` 对一个 key 的持久化顺序是：

1. 在 `objects/` 根下创建并 `fsync` 一条恢复记录，再 `fsync` 该目录；
2. 在目标 shard 里独占创建 staging file，写入 envelope、key 与 payload，并 `fsync` 文件；
3. 用同一目录内的 `linkat(2)` 把 staging inode 发布到最终名字；目的已存在时返回 `EEXIST`，不会覆盖；
4. `fsync` shard，删除 staging 名并再次 `fsync` shard；
5. 删除恢复记录并 `fsync` `objects/` 根。

`Delete` 在 unlink 前同样先持久化恢复记录，unlink 后 `fsync` shard，再删除并同步恢复记录。一个对象已经不存在是成功状态，但已有 shard 仍执行 directory `fsync`，使上一次「unlink 已发生、barrier 失败」的重试可以收敛。打开 store 时逐条恢复 `.put-*` 与 `.delete-*` 记录：未完成的 put 清除 staging，已发布 put 保留 final；delete 确保 final 与 staging 都消失，然后清除记录。delete record 与 staging 同时存在、不同 inode 的 FORMAT/stage 或 publication-probe residue，以及 foreign recovery 内容都是不可能从合法协议产生的状态，必须保留现场并以 `EIO` 停止，不能按最方便的方向猜测恢复。

一次 shard identity／对象 directory barrier 或恢复状态清理失败之后，进程无法再证明它刚才的磁盘修改处于哪一侧。`localdisk` 保存首个 durability failure，并让成功与 fact-bearing result 最终与 poison 状态线性化；poison 已建立时，较早通过健康检查而仍在等待 key/shard/admission 的调用也不能返回成功、`ENOENT`、`EEXIST`、`EFBIG` 或 `ENOSPC` 等磁盘事实，统一以 `EIO` 失败。context cancellation 的 `EINTR` 保留，不被改写成磁盘结论。它不会在已知不可信的磁盘状态上继续给出看似正常的对象答案。

### SQLite 与组合一致性

metastore 使用 WAL，writer connection 明确设置 `synchronous=FULL`、`foreign_keys=ON`、`wal_autocheckpoint(0)` 与 immediate transaction，并在打开时读回 `PRAGMA synchronous` 验证生效值。durable writer 的每条物理 connection 还启用 SQLite `PERSIST_WAL` file control；accepted state 尚未完成 witnessed checkpoint 时，异常 pool close、`Abort` 与未确认 `Accept` 保留恢复所需的 WAL。一个 `Reserve` 事务在上传前记录对象 key、请求的 payload 大小与创建时间；`Commit` 再把对象改为 referenced、记录 digest，并在同一事务中更新路径、逻辑用量、变更日志、持久高水位与 generation。

文件占有的最终判定处于原生 transaction commit 之前，使用同一 writer gate 内解析的实际节点身份。匿名与显式 scoped 修改都受活动保护约束；上传不占住 authority，也不能延长租期。最终许可、SQLite commit、`METASTORE` 确认与结果处置保持同一顺序，后继冲突权限和新的 snapshot capture 不能越过它。commit、rollback 或配额结算无法确定时，SQLite 与 authority 共同失败隔离，不能因 namespace 已知未修改而忽略未知 accounting。

`Put` 只有 nil error 才证明 create-only publication 完成；任意 `Put` error 都通过 storage-lifetime context 调用 `Quarantine`，把 reservation 变成不可提交、不可清扫的 unresolved。`Put` 成功而 `Commit` 被明确拒绝时，包括上传后 grant 已过期的拒绝，`Abandon` 可把 reservation 转成 garbage，因为本次 reservation 已证明拥有对象且路径未引用它。未知 commit 使 metastore 失败关闭，`Abandon` 无法绕过该状态取得删除权限；已成为 referenced 的对象也不能被改成 garbage。每项 cleanup failure 都与原失败一并返回。进程在 publication 结果确定前崩溃时，reserved record 同样不会因年龄获得删除权限；这项 supersession 由[未证实对象发布进入 unresolved](./2026-09-04-unresolved-object-publication.md)记录。对象字节先持久化、命名空间后指向它，所以未提交的字节留作 bounded pending state，已提交的名字仍有对应字节。

普通读取在 database health 的共享门内检查 authority 健康状态并启动 read transaction，以对 `database_state` 的常量查询钉住 SQLite snapshot，随后释放 health 门再扫描和调用 caller-owned result accounting。读取因此取得最终发布之前的完整旧视图，或 witness 已完成之后的新视图；已经捕获的旧 snapshot 可以完成。对象读取从 snapshot 取得 node、size 与 object key 后在门外取得字节。一个大 page、payload I/O 或调用方预算回调不会跨整个生产过程阻挡 mutation 的 commit/Accept。

`METASTORE` 把 SQLite commit 的确认边界放在 WAL 之外。它记录 accepted state A：database identity、generation、node high-water 与 change high-water，并记录已经完整进入主数据库的 checkpoint generation C。durable open 与每项 mutation 先提交 SQLite，再把下一份 A 写入并同步 `.METASTORE.stage`，以 rename 原子替换 final，最后同步 root；A 发布成功后调用才返回成功。stage 只证明见证更新开始，不是 acknowledgment。重开却会在读取 final 之外完整校验残留 stage；一个在 stage 创建后、写完前中断的 checkpoint 会使完整的已确认 workspace 以 `EIO` 拒绝打开。[恢复中断的见证 stage](../../proposed/bug-fix/2026-09-07-recover-interrupted-witness-stages.md)处理这一恢复缺口，保留 final、WAL 与数据库的确认规则。A 发布失败会 poison SQLite，后续 read、write 与 checkpoint 都以 `EIO` 停止。

见证 rename 失败会删除未接受 stage 并同步 root；stage cleanup 失败保留错误与现场。`Checkpoint` publication 失败不 poison accepted state，清理成功后 checkpoint/close 可以重试；这也包括 rename 已发生而 root barrier 失败的 checkpoint 尝试。`Accept` publication 的任何失败都 poison 当前实例；rename 已发生而 root barrier 失败时，重开依据 final witness、WAL 与 visible database state 对账。

自动 checkpoint 被关闭；容量为 1 的 coalesced signal 在打开和每次 A 前进后唤醒后台 worker。worker 与 mutation/witness publication 共用串行门执行 `PASSIVE` checkpoint；一次尝试会延后同时到达的 mutation，snapshot pin 或错误后的等待不持有该门。未完成时每秒重试，期间的新信号不能重置 timer。真实 checkpoint error 保留到一次完整成功，`Status` 同时报告 A、C、pending 与该错误。只有所有 WAL frame 已进入主数据库、当前 durable state 仍与 A 完全一致且新的 witness 已同步时，C 才推进到 A。关闭时执行 `FULL` checkpoint；active reader、checkpoint 或 witness failure 都保留 writer、WAL 与 lifetime lock 并允许重试。C 已推进后才尝试清除 `PERSIST_WAL`；清除失败时 flag 状态未知、writer 与所有权保留且关闭可重试，pool close failure 则进入 terminal close 状态。

重开在 SQLite 接触 WAL 前记录 WAL 是否存在以及是否超过 32-byte header。`C < A.generation` 时，非空 WAL 是 accepted state 仍存在的必要证据；缺失、空或 header-only 都以 `EIO` 拒绝。SQLite 打开后 visible database identity 必须等于 A；visible generation 小于 A 是已确认状态回退，等于 A 时完整高水位必须相同。只有打开前已经有非空 WAL 时，visible generation 才可大于 A；这表示 SQLite commit 已落盘而 A 尚未发布，open transaction 会继续推进并发布新 A。外部见证因此还能检测 `database_state` 与 `sqlite_sequence` 一同回退的状态。

`sqlite.ObjectLimits` 是一个 namespace 对 reserved、unresolved 与 garbage 合计数量及 recorded payload bytes 的 reservation-admission 阈值。一个 requested object 自身大于 byte 阈值时以 `EFBIG` 拒绝，因为任何维护都无法使它单独装下；对象本身可装下、但新增 record 会让当前 count/bytes 越过阈值时，`Reserve` 在同一事务里以 `EAGAIN` 拒绝。失败 upload 因而不能靠不断建立新 reservation 无界增加 SQLite。它不是 garbage 的硬上限：`Commit`、`Abandon`、`Quarantine`、删除、删目录和 rename 都不查询阈值，因为权威提交与终止当前 reservation 的操作必须仍可达。`Commit` 从 pending 合计中移除 record；`Abandon` 与 `Quarantine` 只改 state，不增加 count/bytes；解引用操作则可以新增 garbage。因此清扫不可用、unresolved 只能留存，或配置在重开时收紧，都可使既有 backlog 处于 over-limit；打开不因此失败，`ObjectStatus.OverLimit` 会暴露它，新 reservation 失败，直到 garbage 清扫或显式 recovery 让合计值回到阈值内。

打开与 `ObjectStatus` 查询会校验未知 object state、负 size、referenced/node 关系、rooted tree、used accounting、SQLite storage class、数据库 identity、高水位与 retained-log predecessor chain；entry name 必须是 nonempty BLOB，change 的 kind-dependent node/from nullable group、mode/size/nsec range 与 payload class 必须完整。节点 ID 和 change position 从 `database_state` 的高水位显式分配，`sqlite_sequence` 是冗余的同事务记录；数据库打开、每次分配与每个 `Since` page 要求两者一致，namespace 完整性入口要求所有引用不超过高水位，到达 `math.MaxInt64` 时停止分配。旧身份不会复用。完整决定见[SQLite 持久身份使用显式高水位](../bug-fix/2026-09-07-persistent-sqlite-identities-use-explicit-high-water-marks.md)。

每条 retained change 的 `previous_position` 指向同 namespace 的上一条位置；第一条指向 `trimmed_through`，最后一条等于 `committed_position`。位置全数据库共享，namespace 内合法地存在其它 namespace 留下的数值空洞，所以检查沿 predecessor 而不是要求 `position + 1`。`Open`、`Snapshot` 与 `ObjectStatus` 在暴露 namespace 前验证整条 retained chain；`Since` 通过索引取得 page 起点的锚点并只验证本页 predecessor，page row 数由 anchor 后剩余的 `MaxIntegrityRecords` budget 收紧，避免每个 subscriber 的每一页重复扫描完整窗口。一个完全位于后续缺口之前的 page 可以成功；跨到缺口的 page 整体失败且不暴露该页 prefix，stream failure 使 consumer 作废副本。同一 incarnation/position 的后续续订会在缺口继续失败，直到带外修复或合法 rebuild boundary。细节由[保留日志不连续时拒绝打开](./2026-09-04-retained-log-integrity-refuses-open.md)记录。

执行 recursive reachability CTE 前先用 scalar aggregate 限制 namespace seed、nodes、label/parent/child 任一关系触及该 namespace 的 distinct entries、objects、mandatory log row 与 changes 的合计；legacy preflight 则统计 database 全部对应 rows。总 work 超过 `MaxIntegrityRecords` 时以 `EFBIG` 拒绝，不启动递归遍历。对 entry/change name 执行 content-sensitive 检查前，再以不会 materialize BLOB 的 `length` 分批累计变长字段；超过 `MaxIntegrityBytes` 同样以 `EFBIG` 拒绝。默认 name-byte budget 是 64 MiB，full namespace check 使用它，`Since` 返回字段使用 caller-owned page byte budget。legacy v1/v2 open 在 schema migration 前执行两项 preflight，因此拒绝不会留下半迁移数据库。`Reserve` 的正常 admission 查询不承担全表校验。

本地对象的 SHA-256 是实现自己对输入 buffer 的完整性记录，因此 `localdisk.Put` 按 `Objects` 契约返回 `nil` digest；它不能冒充下层服务对「实际存下来的字节」给出的独立报告。Azure Blob 仍返回服务端 digest，metastore 的 digest 字段继续允许 `NULL`。完整性、对象身份与将来的去重判据没有被合并。

schema 的 `0003_durable_state.sql` 从 v2 前滚到 v3，建立 backing-store binding、database identity/generation、高水位、全局 identity type/max expression indexes 与带 predecessor 的 change 表。v1/v2 migration 先在同一 transaction 内验证全库结构、对象归属、序列、树与日志 tail；v2 的 retained rows 没有 predecessor，不能被宣称为已验证的连续历史，因此迁移保留全局 node/change 高水位、清空 retained changes，并为每个 namespace 生成新 incarnation、把 tail／trim 归零。迁移后的新身份与位置仍严格高于旧高水位。v2 的最终形状由 `testdata/version2.sql` 独立钉住，已落地 migration 不随当前 DDL 改写。这补上了[元数据复制](./2026-08-27-metadata-replication.md)留下的「下一个迁移必须为 v2 建历史见证」义务。`0004_lease_recovery.sql` 另增占有恢复证据，generation 与租期数值不充当 node version 或 change position。

### 逻辑配额与物理容量

`objectstore.Objects` 增加 `Available(ctx)` 与 `Close()`。前者只回答 backing store 还能提供多少 payload byte，不回答 workspace 的配额；无法实测有限物理容量的实现稳定地返回 `ENOSYS`。`objectstore.Storage.Space` 先取得 metastore 的逻辑 `Total / Used / Avail`，验证三者自洽，再把 `Avail` 收紧为它与 `Objects.Available` 的较小值。只有错误树的每个叶子都是 `ENOSYS` 时才使用逻辑数；一个同时含 `ENOSYS` 与 I/O failure 的错误仍然失败，不会被当作「此能力不支持」。

本地 store 要求部署方给出至少 4096 bytes 的正 quota。metastore 的 `Used` 只统计名字仍引用的逻辑 payload；reserved、unresolved 与 garbage 不吃 workspace 配额，但继续占物理盘，分别由状态查询报告。写入在 `Reserve` 与 `Commit` 的事务中执行逻辑配额，越过 workspace allowance 返回 `EDQUOT`。

`localdisk.Available` 从 root 的 `statfs` 取得 `f_bavail * f_bsize`，再扣除 maintenance reserve、并发 publication 已预留的 envelope/key/payload，以及一次最大 envelope/key overhead；inode 计数可用而剩余量少于四个时，可写量为零，因为一个尚无 shard 的 key 还需要 shard directory、shard identity、recovery record 与 staging/final inode。一次 `Put` 建好并验证 shard 后，在建立恢复记录前以同一把容量锁预留完整 encoded object 大小；此时少于两个 inode 或 byte 余量不足都返回 `ENOSPC`。这些数字是从 `statfs` 得出的保守 ceiling，不承诺文件系统的 block rounding 或 absolute capacity，也不能阻止同一文件系统上的外部写者同时消耗容量；最终系统调用的 `ENOSPC` 继续原样暴露。

这项组合保持了[workspace 容量上限](./2026-08-21-space-limit.md)的语义：`Total` 与 `Used` 是 workspace 的逻辑账，`Avail` 同时服从额度与底层现实。它也使「物理盘满」与「workspace 额度用完」继续分别表现为 `ENOSPC` 与 `EDQUOT`。

### 维护、资源上限与状态

`localdisk.Options` 的零值选择一组有界默认值：单对象 payload 1 GiB、64 个 active data operation、256 个 waiting operation、2 GiB encoded/read payload 在途字节、64 MiB maintenance reserve，以及打开时最多检查 4096 条 recovery record。请求先取得 bounded waiting ticket，在 per-key/per-shard coordination 完成后才提升为 active operation/byte reservation；同一 shard 的等待者不会占满全部 active 名额，waiting 已满时新调用以 `EAGAIN` 拒绝。`Get` 与 `Delete` 在提升前已打开 shard descriptor，却只在提升成功后登记关闭；等待被取消会释放 ticket 但泄漏 descriptor，[取消时关闭 shard 描述符](../../proposed/bug-fix/2026-09-07-close-shard-descriptors-on-cancel.md)处理这项无界累积。status 与 root verification 共用一项独立的 serialized control slot，因此 data admission 饱和时仍有一条诊断路径；关闭会同时排空 waiting、active 与 control operation。

`sqlite.ObjectLimits` 的默认 pending backlog 阈值是 4096 个对象与 8 GiB recorded payload；SQLite ordinary/event reader pool 与 long-lived snapshot reader pool 默认各最多 16 条 connection，后者独立存在，使慢 snapshot 不会耗尽 bounded log `Since` 与 namespace read 的全部名额。aggregate integrity work 默认最多 1,000,000 个 namespace/node/entry/object/log/change records 与 64 MiB 变长 entry/change name bytes。`localstore.Config.MaxReaderConnections`、`MaxSnapshotReaderConnections`、`MaxIntegrityRecords` 与 `MaxIntegrityBytes` 的零值继承对应默认值；两个 reader limit 必须为正且有限，record limit 至少是 `sqlite.MinIntegrityRecords = 3`（namespace seed、root 与 mandatory log row），byte limit 必须为正，两项都不能取 `math.MaxInt64`。这些配置在 root 或 database 被接触前验证。local store 要求有效 pending-byte 阈值至少容纳有效的单对象 payload 上限，避免一份配置声明对象可写、reservation 却永远不能接纳它。local object key 最多 120 bytes，使可逆编码、staging 名与恢复记录都落在受支持本地文件系统的单个 pathname component 内；打开时还会核对实际 `NAME_MAX`。local store 的 workspace name 最多 1024 bytes，并被持久化进完成标记。恢复记录上限不得低于 active operation 上限；单对象加 envelope/key 必须装进在途字节上限。等待资源遵从 context cancellation，关闭会唤醒等待者并排空已进入的操作。server 以 `-local-max-waiting-operations` 暴露 local waiting 上限，并以 `-max-pending-objects`、`-max-pending-bytes`、`-max-reader-connections`、`-max-snapshot-reader-connections`、`-max-integrity-records` 与 `-max-integrity-bytes` 为 local-store 和 Azure Blob 两种 metastore-backed 形态暴露 SQLite resource bounds。

`objectstore.New` 与 `objectstore.NewWithOptions` 都拥有启动、event-driven continuation 与 periodic retry 三种清扫触发；前者使用 `objectstore.DefaultOptions()` 的一分钟周期和 64 个对象 batch，后者接受显式配置。单轮 batch 必须在 1 到 `objectstore.MaxSweepBatch = 1 << 20` 之间，避免一次 maintenance attempt 退化为无界工作。`cmd/remote-fs-server` 用通用的 `-sweep-interval` 与 `-sweep-batch` 为 Azure 和 local-store 两种 objectstore-backed 形态配置后者；默认值同样是一分钟与 64，超过 1,048,576 或用于 directory 形态都在打开 storage 之前被拒绝。会产生 garbage 的成功内容替换/删除，以及 `Put` 已成功但 `Commit` 失败后的 `Abandon`，向 coalesced channel 投递非阻塞信号；一轮若删满 batch，会继续安排有界工作，直到不足一批、失败或被取消。普通 `Put` error 进入 unresolved，不触发删除。显式与自动清扫共用 context-aware permit 串行执行。garbage record 在删除成功之后才从 metastore 忘记；失败保留 record，`MaintenanceStatus` 公开最近一次时间、删除数与完整错误。namespace 操作不等待对象删除，关闭也不执行无界且可能失败的最终清扫；worker 停止后才完成的 operation 若留下 garbage，由 durable record 与下次打开的初始清扫接管。

`localstore.Status` 在一次查询中返回绑定的 workspace、逻辑空间、reserved/unresolved/garbage 的数量与字节、pending 阈值及 `OverLimit`、effective SQLite ordinary/snapshot reader-connection 与 integrity-record/name-byte work limits、accepted/checkpointed generation、checkpoint pending/error、物理可用量、waiting/active operations、在途字节、recovery record 数、durability failure 和最近的清扫结果。它直接读取 metastore 的逻辑 `Space`，再通过 control slot 调用 `localdisk.Status` 并收紧可写量，不经过 namespace `Space` 的 ordinary object admission。某一项查询失败时仍收集其它项，并把所有错误合并返回；部分字段可用于诊断，但整个快照不会被标成成功。server 的 SIGHUP 在同一项异步工作内查询 local-store 状态和独立的 lock status，不重算事务内维护的逻辑用量；成功时输出 accepted/checkpointed generation、pending、占有 ready/recovering/unavailable 与有界资源计数，不输出 capability。任一查询或 checkpoint error 只报告整个 status failure。同一时刻至多运行一项查询，每项带独立的两秒 context deadline，查询不会阻塞主 signal loop。deadline 依赖下层操作合作取消，已经进入不可取消的系统调用时不是硬性的 wall-clock 上限。

HTTP server 与 client 的 non-streaming body 同样有 1 GiB 默认单体上限；`MaxWriteBytes` 是其中独立的 file-content 上限，零值继承 `MaxBodyBytes`，非零值不得更大。handler 分别限制 request 与 response 的 operation、waiter 和 aggregate retained bytes；response reservation 覆盖 storage result、wire conversion 与 encoded body 可同时存在的保守峰值。server 在读取 `Write` body 前按 write 上限取得 request 名额，在读取 `SetAttr` 前按一般 body 上限取得名额，bodyless operation 只探测是否出现第一个字节；过大的 write 在调用 storage 前以 `EFBIG` 失败。Read 预算传给 backend，List 逐 entry 预留并提交到有界 `ListResult`；超限或中途失败使整个 result 不可读取，不会先完整 materialize 再丢弃。CLI 用 `-http-max-write-bytes` 暴露 write 值，省略时继承 `-http-max-body-bytes`；local-store 还要求继承或显式给出的结果不超过 `-local-max-object-bytes`。CLI 选项与 local-store 组合配置先完成校验，listener 也在 root 初始化之前取得；独立 command 的 connection/header/idle bounds 见[HTTP connection 上限](./2026-09-04-standalone-http-connection-limits.md)。

## 备选方案

**更小的切法：只交付 `localdisk.Objects`，让部署方分别配置 SQLite 与对象目录。** 少一层 package，也能复用现有 `objectstore.Storage`。输在两份路径配置没有持久绑定，初始化中断无法判定该补建数据库还是拒绝，关闭顺序与 lifetime lock 也落到每个集成方手里；一次配错会把一棵树指向另一批字节。`packages/storage/localstore` 把这些组合义务放在唯一拥有两半资源的位置。

**直接使用 `localdir`。** 目录本身同时持有名字和字节，部署最短。输在它没有 metastore 的事务、持久变更日志与绑定身份，不能走 metadata replication 的生产路径，也不能以不可变对象加原子指针切换来隔离 namespace commit。它仍是一份合法的独立 storage，不承担本决定要提供的部署形状。

**把 payload 存进 SQLite BLOB。** 一份文件就能同时提交树和字节，备份与绑定最直接。输在整文件写入进入单 writer 与 WAL，大对象复制、checkpoint 和数据库膨胀都被放到元数据关键路径；把字节留在独立不可变文件里，使 SQLite 事务只承担小而固定的 metadata 修改。

**内容寻址或 pack file。** 会直接买到去重、更少 inode 或顺序 I/O，是更大的切法。输在引用计数、碰撞信任边界、compaction、读者 pinning 与 crash recovery 会同时进入交付；而[按内容哈希合并重复对象](../../proposed/architecture/2026-08-21-merge-duplicate-objects.md)尚未证明重复量值得这套结构。versioned envelope 与不透明 key 保留以后增加它们的空间。

**只依赖 rename 发布。** staging rename 到 final 的代码更短。输在常规 rename 会替换已存在的目标，不能兑现 create-only `Put`；依赖 `RENAME_NOREPLACE` 又把发布绑定到另一组文件系统支持。hard link 在同一 shard 内原子增加 final 名，并天然以 `EEXIST` 拒绝重复 key，启动探针还能直接验证这两个性质。

**直接创建并写入 shard 的最终 identity marker。** 少一个 staging name 和两次目录同步。输在最终名字一旦可见，另一个并发 opener 就可能读到零字节或半写 marker；两个无关 key 的首次写入会互相制造 `EIO`/`EEXIST`。完整 stage 经 hard link 发布，使 final 的出现本身成为内容已经同步的原子事件。

**用一把全局锁串行化全部 shard。** 可以消除 identity 初始化竞争。输在每次 `Get`、`Put`、`Delete` 的 shard 验证都会经过同一临界区，256 个独立目录不再并行。固定的 per-shard token 只串行目录与 marker 阶段，payload I/O 保持独立。

**只相信 SQLite 主文件和启动时可见的 WAL。** 不需要外部见证。输在一个已确认 transaction 仍只存在 WAL 时，删除 WAL 会让 SQLite 合法打开更旧、结构完整的主文件；数据库内部没有一份不随这次回退而回退的事实可供比较。`METASTORE` 把 accepted/checkpointed generation 与 identity 放在 WAL 之外。

**每次 mutation 返回前强制完整 checkpoint。** 主文件始终追上 accepted transaction，就不需要让 WAL 成为重启必需状态。输在长期 snapshot 可以阻止 checkpoint，每一项 metadata 修改会被最慢读者阻塞；即使没有 snapshot，每次写都把 checkpoint I/O 放进成功路径。外部 A/C 见证允许 mutation 在 WAL 已同步且 A 已发布后返回，由单个后台 worker 定期重试 checkpoint。

**底层 pool close 报错后释放 root lock。** 进程可以退出或让另一个 opener 接管。输在 `database/sql` 的 close error 不能证明 native handle 已全部释放；另一个 owner 可能与残留 SQLite connection 同时写同一 root。terminal error 保留 lifetime ownership 到进程退出。

**底层 pool close 报错后再次调用 Close。** 与 checkpoint failure 一样提供重试。输在一次 native close 可能已经部分关闭资源，API 只返回错误而不报告剩余 handle；重复 close 不能被证明是安全、有效的重试。active-reader、checkpoint、见证、`PERSIST_WAL` 清除与取消失败只要尚未产生 pool close error，就保留可证明的 retryable 状态；file-control 清除失败只证明 writer 仍由 Store 持有，不证明 persistent flag 的最终值。

**已有绑定数据库缺少 workspace row 时重新创建。** 能把丢行后的数据库恢复为可打开的空 namespace。输在无法区分合法新建与 metadata corruption；一个损坏的 workspace 会被成功呈现为空树。只有已证明全新的初始化使用 create-if-missing，已有 schema 统一要求 workspace 已存在。

**把 quota 当作唯一容量。** 不查询 backing filesystem，组合层无需扩展 `Objects`。输在 workspace 尚有额度而物理盘只剩维护空间时会报告一份不存在的可写量；SQLite checkpoint、删除 recovery record 与垃圾回收随后都可能失去落脚处。逻辑与物理容量各自实测再取较小值，保留了两个限制的不同含义。

**先运行 recursive integrity CTE，结束后再判断 namespace 是否太大。** 查询最直接，也能得到精确 reachable set。输在 resource ceiling 只在最昂贵的 allocation 已经发生后生效；scalar count preflight 先限制完整 retained-record work，再允许递归验证，才能让 `MaxIntegrityRecords` 成为 admission 而不是事后诊断。

**只用 record count 约束完整性扫描。** 行数有限，查询结构最简单。输在 SQLite 的 BLOB 长度不受行数限制；一条损坏的超大 entry/change name 会在 `instr` 等内容检查时分配远超预算的内存。独立 `MaxIntegrityBytes` 先按长度收费，再允许 content-sensitive validation。

**用 `count(*)` 验证 bound database 只有一个 workspace，再直接加载字符串。** SQL 最短。输在 preflight 本身会扫描任意多的损坏 rows，并可在长度检查前把超大 TEXT/BLOB 复制进进程；它绕过随后才生效的 integrity budget。读取至多两条 metadata 先证明 cardinality 与长度即可得到同一结论。

**先占 active operation/byte 名额，再等待 key 与 shard。** admission 代码更短。输在同一 shard 上的等待者可以占满全部 active 名额，使另一个 shard 也无法运行；等待者还会无界保留 payload 与 goroutine。bounded waiting ticket 把 coordination 与 active resource reservation 分开，满额以 `EAGAIN` 拒绝。

**同时加入压缩、加密、去重、pack、远程文件系统和多 owner。** 会减少以后再改磁盘格式的次数。输在这些能力没有需求依据，而且会把 key 管理、密钥轮换、compaction、分布式锁与新的恢复状态一起放进正确性证明。当前格式只约束不封死这些方向，不预先交付它们。

## 后果

买到的：

- 单机部署可以沿用 object-store namespace 的事务、节点身份、持久变更日志与 metadata replication，不依赖 Azure 服务。
- 每一份持久状态都有可核对的格式、store/database ID 与初始化完成条件；错误的 metastore/object pairing、丢失 workspace、WAL 回退、损坏或不完整 root 在服务流量之前被拒绝。
- shard identity、对象 publication 与 deletion 有逐步落盘顺序和重启恢复记录；同 shard 的首次使用安全串行，不同 shard 与 payload I/O 保持并行。一次无法判定持久性的失败会毒化当前实例，不继续返回未经证明的成功。
- WAL 外部见证把已确认提交与已 checkpoint 提交分开记录；已返回成功的 metadata 不能因 WAL 消失静默回退，checkpoint failure 可查询并重试。
- workspace 的逻辑余量与宿主文件系统的物理余量同时进入 `Space`，reserved、unresolved 与 garbage 的物理成本可查询，删除与 SQLite 维护保有独立空间。
- local object 层、namespace 清扫、HTTP request/response 与关闭过程都有显式生命周期和资源上限；pending object ledger 有显式 admission 阈值，失败的后台工作不会被成功修改的返回值吞掉。
- `Objects` 的共同契约由 `objectstoretest` 在 memory、Azure Blob 与 local disk 实现上复用；local disk 另对格式损坏、断电 seam、容量、恢复、锁与 root 替换进行故障注入。组合层的失败分工见[对象接口上的失败注入](../testing/2026-08-22-failures-injected-at-the-objects-boundary.md)。

付出的：

- 一个 logical file 对应一个独立 immutable object，覆盖一个 byte 仍写出整份 payload；大量小对象还会消耗 inode 与目录项。pack、增量块与去重不在当前实现里。
- `Get` 必须分配完整 payload 并读完后校验 SHA-256 才返回；上限使内存有界，但没有 streaming read/write。
- HTTP response 仍是 non-streaming：每个成功 Read/List 在配置上限内保留完整结果和 encoded body。response admission 为 listing 的同时存在形状预留保守倍数，换来确定上界，也会在实际结果较小时降低可并发数量。
- SQLite 仍只有一个 writer。对象 I/O 不在事务内，不同 key 的对象操作可以并行；metadata commit 仍在本地数据库的单写者处排队。
- local object 的 waiting capacity 是硬 admission ceiling；达到 256 个默认等待者时，即使调用方愿意继续等，新操作也以 `EAGAIN` 失败。它限制被保留的 payload 与 goroutine，并让已 admitted 的其它 shard 保持进展。
- 每项 durable open 与 mutation 在 SQLite commit 后还要原子替换并同步一次 `METASTORE`；这是返回成功的额外 fsync 成本。`C < A` 时 WAL 是必需状态，备份和运维必须把主文件、WAL 与见证作为一个一致集合。
- 长 snapshot 可以让 WAL 与 `METASTORE` 的 pending generation 持续增长。独立 reader pool 保护普通读取，后台 checkpoint 保持有界重试，但不能越过仍存活的 SQLite read mark。
- 底层 SQLite pool close failure 会把运行期 Store 或 constructor cleanup 留在 terminal ownership 状态并继续占有 database/store root；恢复可用性需要重启进程，不能在同一进程里把未知 native-handle 状态包装成一次成功重试。每个终态 Store 保留的是一组固定 pools、descriptors 与 locks，不随后续调用增长；一个进程若依次打开多个独立 root，仍可能累积多组固定资源，部署层必须把终态关闭错误当作进程级故障处理。跨 root 的全局 fail-stop 或实例数量上限属于独立的进程生命周期设计。
- 超过 integrity-record 或 integrity-name-byte ceiling 的合法大 namespace 会在 open、snapshot 或 `ObjectStatus` 时以 `EFBIG` 失败，必须显式提高 `MaxIntegrityRecords`／`MaxIntegrityBytes`；默认一百万条记录与 64 MiB 名字不是协议上限，而是阻止 recursive validation、关系检查和变长内容扫描无界工作的 serving configuration。
- root 必须位于满足本地 `flock`、hard link 与 `fsync` 语义的文件系统，权限与祖先目录约束比服务一个普通目录更严格。未知文件系统通过探针不等于其 crash semantics 已被运行时证明。
- `statfs` 只能描述采样时刻，root 所在文件系统的其它使用者能在采样后抢走空间；filesystem metadata、block rounding、copy-on-write 与 delayed allocation 也不等于 payload 的逻辑字节。publication reservation 只协调本 Store 进程内的写入，最终系统调用仍可能返回 `ENOSPC`。
- 进程若在 reservation 建立后、publication 结果确定前崩溃，该 record 不会自动获得删除权限。普通 `Put` error 也留下 unresolved；两者可能永久占用 pending admission，直到未来有带归属证明的 recovery 工具处理。localdisk 自己的 staging/recovery residue 仍在下次打开时按已验证的本地协议收敛。
- pending 阈值是 reservation admission，不是强制删除既有状态。释放 namespace 的操作可以把 garbage 推到阈值之上，unresolved 可以使 backlog 无法由清扫下降，调低配置也可以在 reopen 时得到 `OverLimit`；这些情况下能独立装下的新写以 `EAGAIN` 停止，单个 requested object 超过 byte 阈值则是 `EFBIG`。删除与 garbage 清扫继续有路可走，但不会越权删除 unresolved。
- 打开时恢复工作被 `MaxRecoveryEntries` 硬性限制。超过上限会以 `EOVERFLOW` 停止服务，运维必须先诊断异常积累，系统不会为了可用性无界扫描或丢弃未知记录。
- store root 是本系统拥有的格式，不是可用文件管理器直接编辑的目录。旁路修改任何对象、marker、SQLite 文件或权限都可能使下一次访问或打开失败；这是 fail-closed 的结果。

仍存在的 Azure 与通用 object-store 缺口由[对象存储后端已知没做的事](../../proposed/architecture/2026-08-22-gaps-in-the-object-store-backend.md)继续跟踪。对象发布错误的归属规则由[未证实对象发布进入 unresolved](./2026-09-04-unresolved-object-publication.md)拥有。[显式文件占有](./2026-09-07-file-locks.md)拥有原生发布权限与重启保护；[打开文件的身份与过期写入](../../proposed/architecture/2026-08-20-nothing-pins-an-open-file.md)继续保留默认 Open 策略与内容版本／CAS 缺口，[在途读者 pinning](../../proposed/architecture/2026-08-21-readers-in-flight-and-the-sweeper.md)和[按内容合并对象](../../proposed/architecture/2026-08-21-merge-duplicate-objects.md)也各自独立。
