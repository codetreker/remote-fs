# SQLite 内部模块

`packages/metastore/sqlite` 是公开的 SQLite 实现入口。server 的 `Store`、client 使用的 `Replica` 与 `Seeding` 都在该 package 定义；本页只描述这份实现的代码归属和依赖。文件、复制和持久恢复的行为分别由[文件句柄](file-handles.md)、[client 设计](../client/architecture.md)和[本地持久对象存储](local-disk-object-store.md)定义，拆分理由见[模块边界决定](../../../.agents/notes/implemented/architecture/2026-09-09-sqlite-internal-modules.md)。

## 入口与组件

| 位置 | 拥有的职责 |
|---|---|
| `sqlite` 根 package | 公开类型与构造器、Store/Replica/Seeding、数据库 coordinator、事务发布与结果、文件/节点 pin、identity namespace、atomic open、conditional mutation、delete intent、metadata CAS、snapshot 与 reseed 生命周期、Close/Abort |
| [`internal/schema`](../../../packages/metastore/sqlite/internal/schema) | 迁移资源、schema 准备和绑定、volume/metadata/link-target/delete-intent 完整性与 detached/pending 恢复 SQL |
| [`internal/dbstate`](../../../packages/metastore/sqlite/internal/dbstate) | 数据库持久状态与启动证据、generation 和身份高水位的分配、校验及对账 |
| [`internal/nativelease`](../../../packages/metastore/sqlite/internal/nativelease) | 原生文件所有权、lease anchor、持久证据编码和文件系统操作 |
| [`internal/changes`](../../../packages/metastore/sqlite/internal/changes) | 日志窗口、记录、裁剪，以及有界 page/row 解码 |
| [`internal/sqlvalue`](../../../packages/metastore/sqlite/internal/sqlvalue) | SQL scalar、key、time 约定和所需的最小 query 接口 |
| [`internal/sqlerr`](../../../packages/metastore/sqlite/internal/sqlerr) | 数据库错误的既有包装和分类 |
| [`internal/integration`](../../../packages/metastore/sqlite/internal/integration) | 通过公开入口运行的测试与测试 fixture，无生产实现 |

生产依赖由根 package 向内部组件展开，内部组件不导入根 `sqlite`。`schema` 依赖 `dbstate`、`changes`、`sqlvalue` 和 `sqlerr`；`changes` 依赖 `dbstate` 与 `sqlvalue`；`dbstate` 依赖 `sqlvalue`。`nativelease` 使用原生 I/O 依赖，`sqlvalue` 与 `sqlerr` 不拥有数据库生命周期。

## 原子操作的所有者

根 package 的单一 `databaseCoordinator` 持有 commit admission、health fence、文件 pin 与 domain。运行期修改的顺序由这一处保持：准入、准备 SQL、trim/generation、健康与权限检查、Commit/见证、结果与 rollback/fencing。`dbstate` 与 `changes` 的 SQL 函数使用调用方交给它的 transaction 或 queryer，不取得另一份发布或关闭所有权。

`schema.Prepare` 的 schema 迁移、绑定、验证与恢复继续使用原来的一个事务。根 package 把 `durableOpen` 投影成普通准备数据，包含是否存在见证；`CommitWitness` 本身不进入 schema 组件，见证发布仍由根 package 在提交后执行。

中立节点事实与 metadata 位于根 package 的原发布顺序中。`namespace.go` 解析 DirectoryTarget 和 ChildCondition；`namespace_mutations.go` 原子执行名字效果；`atomic_open.go` 组合选择、初始状态、Use、delete intent 与引用保留；`node_references.go` 提供无字节方法的保留节点；`conditional_file.go` 在最终发布处比较 size/metadata 条件；`pending_unlink.go` 持有 durable intent、generation 与恢复清理。`metadata.go`、`file_access.go`、`reference_scope.go` 与 `reference_order.go` 继续把 metadata、Use、scope 和最终访问绑定同一个 native gate。它们复用 databaseCoordinator、retainedFile 和 change log，不建立第二套事务所有者。

根 `log.go` 用节点查询组装完整的 `metastore.Change`，再交给 `changes.Record`。日志组件不回调 Store 来补充身份。snapshot 的取得、事务结果与资源释放仍由根 package 拥有；`Seeding` 在原有生命周期内持有 commit admission，不因目录拆分提前释放。

SQL lease recovery 与 `LeaseRecovery` 留在根 package，原生 evidence I/O 在 `nativelease`。保留引用仍先退役再排空、触发已接受删除义务并回收；旧 incarnation 的 orphan/pending cleanup 使用重新绑定的 `PublicationAccountingChain`，不携带原请求的授权、scope 或 Strong proof。后端与 authority 的锁顺序、各阶段 context 和关闭失败时的所有权规则继续由原来的拥有者执行。

## 公开类型与内部值

公开 import path、类型定义身份、字段和方法集保留在根 package，包括 `DurableState`、`DurableStartup`、`Window`、lease 值与接口、`ObjectLimits` 和 `ObjectStatus`。传入组件的值显式转换成内部表示，组件结果再转换回公开值；内部类型不借 alias 成为公开类型的实际定义位置。

根 [`LeaseAnchor`](../../../packages/metastore/sqlite/lease_anchor.go) 是以 `nativelease.Anchor` 为底层类型的独立定义，既有方法通过指针转换转发。转换保持同一地址和同一份 mutex、descriptor 与生命周期，不能复制 Anchor 值。构造器转换返回指针，Load/Advance 转换 evidence 值；nil receiver 沿用原有方法的行为。

错误组件保持原有 `Is`、`Unwrap` 与 `Classification` 语义，调用点不为穿过目录边界增加额外包装。组件不建立通用回调框架或向外暴露 coordinator 锁。

## 资源与测试归属

当前 schema 版本为 7。[`internal/schema/migrations`](../../../packages/metastore/sqlite/internal/schema/migrations) 的 `0001` 至 `0007` 按序嵌入并重放；6 把 mode 转为 NodeKind 与 `posix.permissions.v1` 并增加共同时间、规范 metadata 与持久计量，7 增加 link target、pending generation 与 durable delete-intent 表。迁移保留 NodeID、高水位、名字、内容、用量与日志事实，并继续由外部见证确认。可读 schema golden 与历史布局 fixture 位于 [`internal/integration/testdata`](../../../packages/metastore/sqlite/internal/integration/testdata)；测试数据不参与运行时初始化。

节点与 retained changes 的 metadata envelope 及 link target 长度由 SQL triggers 同步计入 `volumes.metadata_used`，覆盖 detached/pending 节点与历史副本。完整性检查验证 trigger 定义、storage class、规范 envelope、pending generation、intent 关联、计数与实际合计。`Options.MaxMetadataBytes` 默认每 volume 64 MiB，独立于内容 quota、单节点上限和 integrity 工作预算；载入 payload 前先核对大小，增长越界失败，replica ingest、日志裁剪和物理删除使用同一记账。

当前 schema 的两个损坏矩阵在所属顶层测试中持有私有、不可变的健康数据库种子，每个子用例获得独立文件和连接。种子复制不是生产 schema 或恢复入口；完整 checkpoint、关闭与隔离检查由[测试准备规则](../../testing.md#sqlite-测试准备与隔离)约束。

测试按实现职责合并到对应的 `xxx_test.go`，例如 `objects.go` 的事务与清理用例归入 `objects_test.go`，`replica.go` 的副本用例归入 `replica_test.go`。公开 metastore 契约入口是根 `contract_test.go`；它在顶层拥有一个物理数据库，每个子用例拥有唯一的 volume 对和各自 Open／Close 生存期，跨例隔离由专门回归约束。匿名发布拒绝矩阵留在根 `publication_test.go`，每种占有模式复用一份 fixture，逐项验证拒绝前后及跨项状态不变；其它 publication 与恢复 fixture 的生命周期独立。依赖私有状态的故障注入留在根 package。通过公开入口运行的跨组件用例仍在 `internal/integration`，按被测职责组织并共享原有 helper；子进程入口与调用者属于同一个测试二进制，原生 anchor 测试跟随 `nativelease`。

根 `snapshot_test.go` 直接检查生产 snapshot query；`internal/dbstate/identity_test.go` 检查身份高水位的实际 SQL。模块边界不提供仅为测试导出的生产接口。

SQLite 验证递归执行 `./packages/metastore/sqlite/...`。覆盖率使用 Go 默认的包内统计：每个生产包只由它自己的测试二进制计入覆盖，根包或 integration 对其它包的调用不为被调用包增加覆盖率。`internal/integration` 的测试继续运行，但仅省略没有生产语句的汇总行；该子树不放生产代码或生产子 package。命令与阈值由[测试策略](../../testing.md)拥有，各组件的局部断言与共用契约执行器的自身测试见[包内补测决定](../../../.agents/notes/implemented/testing/2026-09-09-sqlite-package-local-coverage.md)。分包验证与全仓库 CI 分别裁定，不能相互代替。
