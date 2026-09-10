# Agent Note: SQLite 持久身份使用显式高水位

Status: implemented

## 问题

SQLite metastore 的 node ID 是卷对外暴露的持久身份，change position 是 replica 判断事件新旧与是否追上的依据。两者一旦分配就不能在删除原 row 后指向另一件事；否则旧 inode reference 会指到新节点，或 replica 会把新 mutation 当作已经见过的位置跳过。

`INTEGER PRIMARY KEY AUTOINCREMENT` 通过 `sqlite_sequence` 避免正常删除后的 rowid 重用，但 `sqlite_sequence` 本身是普通可修改状态。若最高 row 已被删除，sequence row 又被删除或调低，剩余数据的最大值无法证明历史上曾经分配到哪里；下一次插入可以复用已消失的身份。只核对当前最大 row 因此不足以在服务前判断数据库是否仍保持持久身份语义。

## 决定

schema v3 的 singleton `database_state` 保存 `node_high_water` 与 `change_high_water`，与 database identity 和 generation 一起提交。迁移从 `sqlite_sequence`、现存 node、卷 root、entry、retained change 中的 node reference，以及 log/change position 与 trim boundary 中取最大值，建立不会低于任何可见证据的初始高水位。

节点创建和 change append 使用显式 ID，不依赖隐式 rowid。写事务先要求 `sqlite_sequence` 与对应高水位完全相等，检查尚未到达 `math.MaxInt64`，将高水位增加一，再用这个显式值插入 `AUTOINCREMENT` 表；SQLite 在同一事务内把冗余 sequence 推进到相同值。change append 还要求当前卷的 committed tail 严格小于新位置，避免运行中抬高的 tail 被写成新记录的 predecessor。任一项失败使整个 transaction 回滚。身份空间耗尽以 `ENOSPC` 拒绝，不绕回。

数据库打开、checkpoint、`DurableState`、每次身份分配与每个 `Since` page 要求 `sqlite_sequence.nodes == node_high_water`、`sqlite_sequence.changes == change_high_water`。durable-state validation 还通过按 storage-class discriminator 与最大 identity 排序的 expression indexes 检查整个数据库：类型异常或任一卷的 surviving reference 超过高水位都以有界 edge lookup 失败。打开、`ObjectStatus` 与其它卷完整性入口继续逐行要求：

- `database_state` 恰有一条、database identity 与所有 counter 的 storage class 和范围有效；
- 卷 root、node、entry parent/child、change parent/from/node 都不超过 node high-water；
- log committed/trimmed position、change position 与 `previous_position` 都不超过 change high-water。

replica snapshot 可以按任意 row 顺序观察已有正 ID；`Seeding` 累计本轮最大值，在 `Complete` 时一次把 node high-water 推进到原值与最大 observed ID 的较大者并核对 `sqlite_sequence`。增量 `Created` change 必须带严格大于当前 high-water 的 ID。这样 source identity 原样复制，不引入另一套本地编号，也不接受一个旧 ID 再次成为新节点，同时避免每个 snapshot row 重读和更新 allocator state。

本地持久 store 的 `METASTORE` 在 SQLite 之外复制 accepted generation 与两项高水位。数据库内的 `database_state` 能发现 sequence 单独损坏；外部见证还能发现两份内部记录被一同回退。这个组合属于[本地磁盘对象存储](../architecture/2026-09-04-local-disk-object-store.md)的 durable commit boundary。

这项决定补强[名字不是身份](./2026-09-01-a-name-is-not-an-identity.md)中的节点身份，并与[元数据复制](../architecture/2026-08-27-metadata-replication.md)及[保留日志连续性](../architecture/2026-09-04-retained-log-integrity-refuses-open.md)共同保证 position 不回退、不复用且 retained segment 不缺行。

## 备选方案

**只保留 SQLite `AUTOINCREMENT`。** 正常 API 删除不会重用 rowid，实现也最短。输在 `sqlite_sequence` 丢失或被调低时没有第二份持久证据；最高已删除 ID 不再存在于业务表中，启动校验无法从 surviving rows 恢复它。

**每次打开用现存 row 的最大 ID 修复 sequence。** 能修复 sequence 落后于仍存在记录的状态。输在无法发现已经删除的最高 ID，修复会把持久身份回退到更小值并为后续复用授权。

**只把高水位放进 `METASTORE`。** 能检测本地持久 store 的回退。输在普通 SQLite metastore 与 replica 没有该文件，且每次 ID 分配都要让 schema 内的事务依赖另一个文件才能保持原子；`database_state` 先与业务修改同事务提交，localstore 再用外部见证确认完整状态。

**改用随机 128-bit node ID 与 change identity。** 删除后碰撞概率极低，也不依赖序列。输在 storage/metastore/HTTP/FUSE identity 现有契约和 SQLite schema 都使用有序 `int64`；change position 还必须支持顺序比较。它改变跨包与 wire 形状，不能只修复持久分配证明。

## 后果

- 删除最高 node/change row、清空／调低／抬高 `sqlite_sequence`，或把 committed tail 抬到下一份可分配 position 之上，都不能让下一次插入复用或倒接旧值；不一致在 durable-state check、下一次身份分配、append 或下一页 change 读取时以 `EIO` 失败。
- 本地持久 store 能检测 database state 与 sequence 一同回退；已确认 generation 的身份高水位不会因 WAL 丢失静默倒退。
- 每次 ID/position 分配多一次 singleton row 读取与更新，并继续维护 SQLite 自带 sequence 作为冗余检查。
- schema v3 迁移必须扫描旧数据库中所有可能保存 node ID 或 change position 的位置；这项工作纳入 legacy integrity budget，超限时迁移以 `EFBIG` 停止。
- `math.MaxInt64` 成为明确的持久空间终点。到达后必须迁移身份表示，系统不会通过回绕恢复写入能力。
