# SQLite 内部模块

`packages/metastore/sqlite` 是 SQLite 公开入口，定义 Store、Replica、Seeding、配置与原生恢复。文件行为见[保留对象](file-handles.md)，复制见[client](../client/architecture.md)，持久组合见[本地对象存储](local-disk-object-store.md)。模块拆分和平台边界分别由[模块决定](../../../.agents/notes/implemented/architecture/2026-09-09-sqlite-internal-modules.md)与[平台隔离决定](../../../.agents/notes/implemented/architecture/2026-09-16-isolate-platform-filesystem-clients.md)记录。

## 入口与组件

| 位置 | 拥有的职责 |
|---|---|
| `sqlite` 根 package | 公开类型与构造器、Store/Replica/Seeding、数据库 coordinator、事务发布与结果、文件 pin/domain、通用会话与动作、claims／ranges／删除意图、snapshot 与 reseed 生命周期、Close/Abort |
| [`internal/schema`](../../../packages/metastore/sqlite/internal/schema) | 迁移资源、schema 准备和绑定、来源版本与通用格式完整性、opaque metadata／entry／删除状态及恢复 SQL |
| [`internal/dbstate`](../../../packages/metastore/sqlite/internal/dbstate) | 数据库持久状态与启动证据、generation 和身份高水位的分配、校验及对账 |
| [`internal/nativelease`](../../../packages/metastore/sqlite/internal/nativelease) | 原生文件所有权、lease anchor、持久证据编码和文件系统操作 |
| [`internal/changes`](../../../packages/metastore/sqlite/internal/changes) | 日志窗口、记录、裁剪、不可变通知事实，以及有界 page/row 解码 |
| [`packages/internal/fileaccess`](../../../packages/internal/fileaccess) | 通用 claims、范围集合、等待依赖与有界死锁检查，不拥有 SQL 事务或平台解释 |
| [`packages/internal/filebudget`](../../../packages/internal/filebudget) | 完整内容物化与数据操作预算，供原生 file domain 使用 |
| [`internal/sqlvalue`](../../../packages/metastore/sqlite/internal/sqlvalue) | SQL scalar、key、time 约定和所需的最小 query 接口 |
| [`internal/sqlerr`](../../../packages/metastore/sqlite/internal/sqlerr) | 数据库错误的既有包装和分类 |
| [`internal/integration`](../../../packages/metastore/sqlite/internal/integration) | 通过公开入口运行的测试与测试 fixture，无生产实现 |

生产依赖由根 package 向内部组件展开，内部组件不导入根 `sqlite`。`schema` 依赖 `dbstate`、`changes`、`sqlvalue` 和 `sqlerr`；`changes` 依赖 `dbstate` 与 `sqlvalue`；`dbstate` 依赖 `sqlvalue`。`nativelease` 使用原生 I/O 依赖，`sqlvalue` 与 `sqlerr` 不拥有数据库生命周期。

## 原子操作的所有者

根 package 的 databaseCoordinator 持有 commit admission、health fence、文件 pin 与每 volume 的 fileDomain。fileDomain 共享会话、claims、ranges、内容预算和恢复所有权；包装同一 volume 不能另建预算。固定操作在根 package 排序，fileaccess 只提供共同访问状态，transport 不另建保护表。SQL 准备、健康／授权检查、Commit／见证、结果和 rollback／fencing 由同一所有者完成。fileaccess 在冲突／容量扫描后、效果或已接纳 no-op 前，在内部 mutex 下执行 native 提供的可信本地 Guard，重新核对 context、session、引用与发布资格；Guard 不执行 I/O、不重入 coordinator，也不是远程策略 callback。owner／session 的退役清理不依赖这项请求 Guard。dbstate／changes 使用调用方 transaction，不取得另一份发布或关闭所有权。

`schema.Prepare` 的 schema 迁移、绑定、验证与恢复继续使用原来的一个事务。根 package 把 `durableOpen` 投影成普通准备数据，包含是否存在见证；`CommitWitness` 本身不进入 schema 组件，见证发布仍由根 package 在提交后执行。

根日志与通知捕获代码在权威修改状态下组装 metastore.Change，再交给 changes.Record。Notification 的事件时 Kind、ChangeMask、前后 Attr／opaque metadata／EntryLocation 与修改一起提交；删除后仍有完整图像。Record／decoder 对全部 volume 执行同一 Change 的 512 KiB 变长字段总量界限，Notification 另有编码上限。日志发送不反查当前 Store 补历史事实；snapshot 与 Seeding 的捕获、commit admission 和释放仍由根 package 拥有。

服务格式保存通用 kind、metadata revision、directory revision、可空 creation／change time、opaque metadata、link target、稳定 entry ID、drain 与 removal intents。名字按精确字节比较；Windows profile、DOS 列与运行期 POSIX mode 解释不进入 schema。历史 mode 的一次性迁移保存原权限事实，既不补造 owner／历史时间，也不成为服务期平台策略。

Strong 与 File 的 SQL 恢复各占固定状态，原生 evidence I/O 由 nativelease 执行。Strong 保持原记录；File 的 Accepted／Prepared 加入 Quiescent，并有独立 Pending／Ready 初始化。静止事实的持久完整性覆盖数据库全部 volume，generation 递增且最大租期不下降。数据库级 fileAdmission 覆盖 Active 发布／会话登记及 Store.Close；静止证明检查所有 fileDomain、共享 Store、全局 pins 和持久义务。File drain 确认后才能发布 Quiescent，下一次会话先持久 Active；恢复任务、context 和失败时的资源保留属于 Store 关闭所有权，见[持久恢复](local-disk-object-store.md)。

## 公开类型与内部值

公开构造、Store／Replica／Seeding、DurableState／DurableStartup、Window、ObjectLimits 和 ObjectStatus 留在根 package。Options.Files 使用公共 storage.FileServiceOptions，内部 fileaccess／filebudget 配置显式转换，内部 coordinator 不成为调用方可替换的状态权威。

根 [`LeaseAnchor`](../../../packages/metastore/sqlite/lease_anchor.go) 是以 `nativelease.Anchor` 为底层类型的独立定义，既有方法通过指针转换转发。转换保持同一地址和同一份 mutex、descriptor 与生命周期，不能复制 Anchor 值。构造器转换返回指针，Load/Advance 转换 evidence 值；nil receiver 沿用原有方法的行为。

错误组件保持原有 `Is`、`Unwrap` 与 `Classification` 语义，调用点不为穿过目录边界增加额外包装。组件不建立通用回调框架或向外暴露 coordinator 锁。

## 资源与测试归属

当前 schema 为 6。迁移 0001–0005 保持已发布原文，历史 v1–v3 fixture 保持原布局；0006_shared_file_facts.sql 添加通用字段并移除 mode 列。EntryID 从同一已见证 node_high_water 分配，但与 NodeID 分开且不重用；迁移调整 nodes sequence，高水位格式不因此改变。旧通知缺少完整图像，迁移清空保留历史、更换 incarnation 并保留 change 高水位。真实来源预检、迁移与目标完整性在同一事务；提交前失败可回滚，Commit 未知或提交后见证失败进入 durability fencing，不伪报来源恢复。golden 与历史数据位于[集成 testdata](../../../packages/metastore/sqlite/internal/integration/testdata)，不参与运行时初始化。

当前 schema 的两个损坏矩阵在所属顶层测试中持有私有、不可变的健康数据库种子，每个子用例获得独立文件和连接。种子复制不是生产 schema 或恢复入口；完整 checkpoint、关闭与隔离检查由[测试准备规则](../../testing.md#sqlite-测试准备与隔离)约束。

测试按实现职责合并到对应的 `xxx_test.go`，例如 `objects.go` 的事务与清理用例归入 `objects_test.go`，`replica.go` 的副本用例归入 `replica_test.go`。公开 metastore 契约入口是根 `contract_test.go`；它在顶层拥有一个物理数据库，每个子用例拥有唯一的 volume 对和各自 Open／Close 生存期，跨例隔离由专门回归约束。匿名发布拒绝矩阵留在根 `publication_test.go`，每种占有模式复用一份 fixture，逐项验证拒绝前后及跨项状态不变；其它 publication 与恢复 fixture 的生命周期独立。依赖私有状态的故障注入留在根 package。通过公开入口运行的跨组件用例仍在 `internal/integration`，按被测职责组织并共享原有 helper；子进程入口与调用者属于同一个测试二进制，原生 anchor 测试跟随 `nativelease`。

根 `snapshot_test.go` 直接检查生产 snapshot query；`internal/dbstate/identity_test.go` 检查身份高水位的实际 SQL。模块边界不提供仅为测试导出的生产接口。

SQLite 验证递归执行 `./packages/metastore/sqlite/...`。覆盖率使用 Go 默认的包内统计：每个生产包只由它自己的测试二进制计入覆盖，根包或 integration 对其它包的调用不为被调用包增加覆盖率。`internal/integration` 的测试继续运行，但仅省略没有生产语句的汇总行；该子树不放生产代码或生产子 package。命令与阈值由[测试策略](../../testing.md)拥有，各组件的局部断言与共用契约执行器的自身测试见[包内补测决定](../../../.agents/notes/implemented/testing/2026-09-09-sqlite-package-local-coverage.md)。分包验证与全仓库 CI 分别裁定，不能相互代替。
