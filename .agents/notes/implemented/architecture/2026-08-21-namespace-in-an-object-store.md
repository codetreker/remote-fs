# Agent Note: 把命名空间放进对象存储

Status: implemented

## 问题

对象存储没有目录。它没有权限位、没有访问时间、没有改名，也没有任何「同时改两样东西」的手段。它有一组扁平的键，和键下面的字节。

`storage.Storage` 要的其余一切都得存在别处，而「别处」有两个选项。一是贴在字节旁边——每个 blob 挂一份自己的元数据。那样 `SetAttr` 是一次网络请求，`Stat` 是一次网络请求，列一个目录是一次列举加上每个条目一次请求；更要命的是改名一个目录变成 O(n) 次复制加删除，中途失败留下一棵搬了一半的树，而契约里没有任何东西能描述那个状态。二是把树放进一个本来就擅长回答树的问题、也本来就能一次改几行或者一行都不改的系统里。

第二条路要求承认一件此前没有承认过的事：一份 storage 实现可以有它自己的外部依赖。当时的 `localdir` 没有——它就是一个目录。这份实现需要一个对象存储和一个数据库，两者都由部署方提供和运维，两者都能各自失败。

## 决定

`packages/storage/objectstore` 是一份 `storage.Storage`，由两个可替换的下层拼起来：

| 包 | 承担什么 |
|---|---|
| `packages/storage/objectstore` | 组合两者，实现契约 |
| `packages/storage/objectstore/azblob` | 一份 `Objects` 实现：Azure Blob 中的不可变字节 |
| `packages/storage/objectstore/localdisk` | 一份 `Objects` 实现：本地磁盘上的不可变文件、完整性与物理容量 |
| `packages/storage/localstore` | 组合 local-disk objects 与绑定到同一 store ID 的 SQLite，拥有初始化、维护与关闭 |
| `packages/metastore` | `Store`：名字的树、节点的属性、路径到对象键的指向 |
| `packages/metastore/sqlite` | 第一份 `Store` 实现，也提供绑定到 backing store ID 的打开方式 |

本地磁盘实现的格式、持久化与组合所有权由[本地磁盘对象存储](./2026-09-04-local-disk-object-store.md)记录；[显式文件占有](./2026-09-07-file-locks.md)把 authority 绑定到 metastore 的原生最终发布。两者扩展这份两层结构，树与对象继续各自持有名字和字节。`limited` 保留为可复用的配额包装器，第三方 storage 继续按库契约接入。宿主目录形态的结束由[移除宿主目录后端](../simplification/2026-09-08-remove-the-host-directory-backend.md)记录，下文涉及 `localdir` 的比较保留该决定发生时的理由。

**写入的顺序是登记、上传、指过去。** `Reserve` 先在库里落一行「我要写这个键」并提交，然后字节被放到那个键下，最后 `Commit` 切换节点的 object 引用并在同一个事务里记账。路径操作在提交时解析实际节点，文件句柄直接使用已保留的节点身份。上传不占住强占有 authority；发布前对显式 proof、活动保护与期限作最终判定，匿名修改也受约束。最终许可与提交、持久确认和结果处置保持同一顺序，后继冲突权限与新的权威视图不能越过它。对象一经写入不再修改，已经捕获旧 node/object key 的读者可以在门外取得完整字节；后续句柄读取重新观察同一节点的当前状态。

`Put` error 把 reservation 变成不可清扫的 unresolved；`Put` 成功而 `Commit` 明确拒绝时，`Abandon` 可把仍未引用的 reservation 变成 garbage。上传后 grant 过期是这种明确拒绝。未知 commit 或已被 poison 的 metastore 不允许收尾凭猜测取得删除权限，已被引用的对象同样不能 abandoned。调用方已经取消的 request 不会取消这些 storage-owned cleanup，清理本身失败则与原失败一起返回。原子切换路径引用使其它客户端看不到半份内容，而对单个 blob 的任何一串写入都给不了它。

**树按 `(parent, name)` 存，不按完整路径存。** 改名一个目录改一行，而不是把子树里每一行的前缀都 UPDATE 一遍。附带的好处是行号天然就是[存储操作词汇](../../proposed/architecture/2026-08-19-storage-operation-vocabulary.md)要的那种「跨改名稳定、删除后不复用」的节点身份，将来要用时不必推翻重来。

[持续文件句柄](./2026-09-08-live-file-handles.md)使用这份身份保留已打开的普通文件。unlink 或 rename 覆盖后仍被引用的节点成为 detached，保留当前内容、属性与配额，named snapshot 与路径访问不包含它，后续 detached 修改不产生 named change event。最后物理引用关闭才释放配额并授权清扫；先逻辑退休、再排空 I/O 的顺序阻止迟到上传继续发布。该机制部分替代「每次都从路径找文件」的访问方式，保留本 note 对树与不可变对象分层的决定。

**名字是 `BLOB`，按字节序排。** Linux 上一个名字是任意字节序列，`README` 和 `readme` 是两个文件，而按 locale 规则排出来的目录顺序和这个系统里其它任何实现都对不上。这一点在 `packages/transport/httprest/message.go` 里已经有先例：线上 `Entry.Name` 是 `[]byte`，因为 `encoding/json` 会把非法 UTF-8 字节换成 U+FFFD。

**时间是两列整数**（秒与纳秒），不是一个纳秒整数。契约测试要求 `1902-01-01` 与 `2400-06-01T12:00:00.5Z` 都完整往返，而 int64 纳秒只覆盖 1678 到 2262。

**配额在提交的同一个事务里校验，`Space` 的逻辑总量与已用量是精确数。** 用量包含 named 与仍被引用的 detached 文件；`Usage` 在没有配置 allowance 时也能报告该逻辑总量。这份实现不被 `limited` 包起来——见下。`Objects` 能实测底层容量时，可写量还会收紧为配额余量与物理余量的较小值；测量失败按对象存储失败暴露，只有稳定的 `ENOSYS` 表示该实现没有这项能力。

## 为什么先登记再上传

一个对象在 T 时刻落到存储里，引用它的那个事务在 T+δ 提交。清扫器如果在这中间跑，并且照着「删掉所有没人引用的对象」去做，它删的是别人正要用的数据。

调查过的系统分成两类做法（每一条的出处见[对象存储后端](../../../../docs/research/object-store-backends.md)）。一类先传后提交、中间什么都不记，于是只能靠时间去猜：JuiceFS 一小时，SeaweedFS 五小时，Iceberg 三天，Delta Lake 七天——每一家的文档都带着一句「间隔太短会损坏数据」的警告，因为这个间隔实际上是在赌一次写入能有多慢。另一类不猜：s3ql 在上传**之前**就把一行意图写进元数据库，于是每个对象从诞生那一刻起就有一条已提交的记录说明它是谁的；它的清扫器因此完全不需要宽限期。Ceph RGW 从另一头解决——它从不扫描，回收项在解除引用的那个原子操作里入队。

这份实现两头都取：上传前登记使每个 key 在 publication 之前已有 durable record，权威解引用 transaction 则把内容替换、无 pin 的删除或 detached 最后关闭所释放的对象明确标成 garbage。删除名字本身不授权清扫仍由打开引用持有的内容。`Garbage` 只返回 deletion-authorized record；reserved 不因年龄变成 garbage，`Put` error 进入不可清扫的 unresolved，只有成功 `Put` 证明归属、且节点状态允许时，`Abandon` 才能把失败写入标为 garbage。清扫器因此既不列举 container，也不靠宽限期猜测对象归属。完整的 nil/error 删除权限边界见[未证实对象发布进入 unresolved](./2026-09-04-unresolved-object-publication.md)。

s3ql 那套之所以成立，前提是它强制单挂载独占——它的清扫器会直接删掉库里不认识的对象。这里不需要那个前提，因为所有写入者共用同一个 metastore，登记行对谁都可见。

## 父目录必须存在

[MVP 范围](../process/2026-08-19-mvp-scope.md)在「会毁数据的」一节里点名了这份实现：

> `unlink()` 之后的 `close()` 落在一个占位路径上。……在 `localdir` 上它响亮地失败（暂存文件要建在 `.go-fuse.…` 这个不存在的目录里，ENOENT 一路传回 `close`）；**在一个命名空间里没有真实目录的 storage 上——也就是这份契约被塑造成现在这个样子所面向的对象存储——它成功**，`close` 返回 0，数据落在一个没有人会再去读、也没有人会去清理的键上。

所以路径 `Commit` 到一个父目录不存在的位置是 ENOENT，且**绝不创建中间层**。这不是一条可以在实现里放松的性能取舍，它是这份 note 存在的理由之一：把路径当扁平键的实现会静默地成功，而那正是上面那段话描述的丢数据。持续文件句柄按保留的节点提交，失去名字后仍可访问原文件，不依赖旧父目录，也不会重建旧名字；关闭只结束引用，不补交一份按路径定位的内容。

## 版本不是内容哈希

对象的内容哈希被记下来，留给将来的去重（见[按内容哈希合并重复对象](../../proposed/architecture/2026-08-21-merge-duplicate-objects.md)）。它**不是**节点的版本。

[没有东西钉住一个打开的文件](../../proposed/architecture/2026-08-20-nothing-pins-an-open-file.md)记下了原因：

> **内容派生的版本认不出两个内容相同的对象。** 版本是内容哈希时，检查退化成「这个名字上的字节还是我读到的字节」，而不是「这还是我打开的那个对象」。

同一份哈希既做去重依据又做版本，等于把那个退化设计进来。所以内容哈希、节点身份与内容版本保持独立，文件占有的 GrantID／generation 也不充当其中任何一个值。

事务性的 metastore 把保留文件的 `content_revision` 比较与引用切换放在同一个事务中。范围写入以当前完整内容生成替换对象，只有明确未提交的 revision 冲突且清理成功时才重新读取和尝试；默认最多 8 次，持续竞争可返回 `EAGAIN`。重叠范围写按实际提交顺序生效，不会用旧整文件覆盖本次未触及的字节。强 S/X 与 advisory 锁分别控制自己的协议，普通 Open 不自动取得它们；调用方显式内容版本校验仍是独立工作，不能把这项内部 CAS 当作其交付。

## SQLite 的单写者

[顺序与版本](../../proposed/architecture/2026-08-19-ordering-and-versions.md)否决过「全局临界区横跨提交与取号」，理由是它把所有写入串行到一次网络/磁盘往返之后，违反 R-CC-2「写不同文件必须完全互不干扰」。

SQLite 只有一个写者。这里选择接受它，理由是被否决的那个形状里，临界区**包住了网络往返**；这里事务只含元数据操作，字节的上传发生在事务之外，被串行掉的是一次本地提交。这个区别是这份实现成立的全部依据，也是它的上限：R-CC-2 在这份实现上只以这个口径成立，一个真正让不同文件的写互不干扰的部署需要一份 PostgreSQL 实现，而 `metastore.Store` 的接口是照着外部数据库设计的，没有任何一处假设单进程或本地文件。

提供 retained-file 的 SQLite opener 必须持有数据库 EX ownership，只有 SH 的原始 opener 不能发布这项能力。外部对象存储使用 `sqlite.OpenLocking`：它同时持有 database inode 的 lifetime `LOCK_EX` 与进程内独占 coordinator，以数据库身份绑定强占有证据，并在数据库旁固定保存 `.<database basename>.leases.intent` 和 `.witness`。非独占原始 SQLite opener 持有同一 native file 的 `LOCK_SH`，既存 raw handle 与独占 owner 在同进程和跨进程都互斥；配置强占有恢复与启用 authority 必须验证真实 EX owner，恢复还要求绑定的原生 `LeaseAnchor`。独占启动先验证所有 namespace 的完整性与用量，再在同一事务中回收旧 epoch 的 detached 节点；旧引用失效，不按路径恢复。

Accepted／Prepared、独立 witness 与最大租期覆盖整份数据库；运行期选择的 namespace 不进入永久 anchor identity。同一数据库不能供两个 active locked server 共用，但可以重新打开其中另一份已存在的 namespace；每次接管仍从取得数据库 EX 的 monotonic 起点执行完整最大租期屏障。原始未绑定的 SQLite API 仍可独立使用；已有 native binding 或租期证据时不能借它关闭保护。localstore 的私有 root 继续永久绑定单一 workspace，并在 root lifetime lock 外持有数据库 EX。证据、所有权和恢复屏障的代价由[显式文件占有](./2026-09-07-file-locks.md)记录。

## 备选方案

**属性贴在 blob 自己的元数据上，不要数据库。** 这是最初的方案，也是唯一一个不引入外部数据库的方案。输在三处：改一个属性是一次网络请求；空目录在扁平命名空间里无法表示，得靠零字节的 marker blob 约定；改名一个目录是 O(n) 次复制加删除，没有任何一步是原子的，中途崩溃留下一棵搬了一半的树。前两条是代价，第三条是这个方案根本给不出契约要的语义。

**blob 名就是命名空间里的路径。** 好处是拿 Storage Explorer 或 azcopy 直接看得懂、拿得走。输在它把改名的代价钉死在 O(n)，而且 POSIX 文件名要转义才能当 blob 名——转义规则一旦定下就是磁盘格式。不透明键把这两个问题一起消掉，代价是容器里的东西离开这个系统就没有意义。

**内容寻址：键就是内容的哈希。** 白拿去重与幂等写。输在删除需要引用计数才安全，而引用计数是又一份要维护、要修复的账；不透明键加「无人引用即垃圾」的判定不需要计数，去重则可以在之后作为一次合并动作补上。

**一张表，完整路径做主键。** `Stat` 与 `List` 最直接。输在改名一个目录要 UPDATE 整个子树的前缀，而 [MVP 范围](../process/2026-08-19-mvp-scope.md)已经点过这个痛点；并且它让「不透明节点身份」以后变得昂贵，而那是一份仍然有效的提案。

**靠宽限期扫描回收垃圾。** 实现最简单，也是多数系统的做法。输在它的正确性是一个关于延迟的猜测——每一家采用它的系统都在文档里写着间隔太短会损坏数据。上传前登记让这个猜测完全不必要。

**继续用 `limited` 包住它。** 与当时 `localdir` 的组合完全一致，可以复用已有的 quota 测试。输在两处：当时的 `limited` 在每次 `Write` 前多做一次 `Stat`，对网络后端就是每次写多一个往返（光是原子性那个用例就是 200 次）；而它 `New` 时要走一遍整个命名空间来播种计数器，对一个能一句话问出用量的库来说是荒谬的。更根本的是这套路径采样的账会漂移——[空间上限](./2026-08-21-space-limit.md)为此引入了 `SIGHUP`/`Recount`——而事务里维护的计数器不会。

**SQLite 驱动。** 三个候选都实测过。`mattn/go-sqlite3` 需要 cgo，与 [FUSE 库选型](../../../../docs/research/fuse-libs.md)记下的「Zero cgo」直接冲突。另外两个都是纯 Go 且都通过了同一组探针（WAL、字节序、非法 UTF-8 往返、唯一约束可判定），量出来的体积记在[对象存储后端](../../../../docs/research/object-store-backends.md)：`modernc.org/sqlite` v1.57.0 是 24 个模块、vendor 140 MB、1886 个 .go 文件；`github.com/ncruces/go-sqlite3` v0.35.3 是 15 个模块、14 MB、389 个文件。选了前者，理由是它是部署最广的纯 Go 驱动，而驱动藏在 `database/sql` 后面，换掉的代价接近零。这个取舍值得在依赖体量变成问题时重新考虑。

**一个 workspace 一个 container。** 隔离最干净，删 workspace 是一次调用。输在 container 名有形式限制，且删除后名字要隔一段时间才能重用；共用 container 按前缀分则让创建与销毁 workspace 不碰 container 管理面。

## 后果

买到的：

- **跨进程的原子改名**，包括目录。当时的 `localdir` 靠同一个文件系统内的 `rename(2)` 使两个进程各挂一次同一个目录时仍能原子改名；对象存储上没有任何等价物，而一个事务有。
- **精确的用量**，不漂移，不需要遍历，也不需要一个修复它的信号。
- **比较与写入有可共享的原子事务位置**；占有权限在这里判定，内容版本／CAS 仍保留独立实现空间。
- **符号链接的缺口在这里是空的**：契约没有任何操作能造出一条链接，所以一个只经由契约触达的命名空间永远不会持有链接。当时的 `localdir` 要处理链接，是因为它坐在一个别人也能动的目录上；metastore 没有「外面」。

付出的：

- **依赖从 2 个模块变成 20 余个。** 这个仓库此前明确为了守住依赖体量否决过一整个 FUSE 库。
- **服务端多了一个要运维的数据库**，且它与对象存储可以各自失败——R-ERR-6 是为此新加的。
- **写不同文件在 SQLite 实现上会互相阻塞**，R-CC-2 只以上面那个口径成立。
- **崩溃留下垃圾对象**。它们不可读、不计入用量、也不会变成错误的答案，但在后台清扫到达之前一直占着存储；清扫的批次、周期与失败状态由拥有 namespace 的组合层管理。
- **一个读者可能输给一个不停歇的写者**。读是两步——问树、取对象——而一次提交会把旧对象交给异步或定期清扫，于是读者手里的键可能在取回前被删掉。读者认得出这件事并重来，次数耗尽则报 `EAGAIN`；代价与去掉这条代价的办法见[在途的读者与清扫器](../../proposed/architecture/2026-08-21-readers-in-flight-and-the-sweeper.md)。
- **一个字节的修改要重传整个文件**。这是整文件提交的固有代价，[存储操作词汇](../../proposed/architecture/2026-08-19-storage-operation-vocabulary.md)已经记下它，这份实现没有改变它。
