# SQLite 内部模块

`packages/metastore/sqlite` 是公开的 SQLite 实现入口。server 的 `Store`、client 使用的 `Replica` 与 `Seeding` 都在该 package 定义；本页只描述这份实现的代码归属和依赖。文件、复制和持久恢复的行为分别由[文件句柄](file-handles.md)、[client 设计](../client/architecture.md)和[本地持久对象存储](local-disk-object-store.md)定义，拆分理由见[模块边界决定](../../../.agents/notes/implemented/architecture/2026-09-09-sqlite-internal-modules.md)。

## 入口与组件

| 位置 | 拥有的职责 |
|---|---|
| `sqlite` 根 package | 公开类型与构造器、Store/Replica/Seeding、数据库 coordinator、事务发布与结果、文件 pin/domain、snapshot 与 reseed 生命周期、Close/Abort |
| [`internal/schema`](../../../packages/metastore/sqlite/internal/schema) | 迁移资源、schema 准备和绑定、命名空间完整性与 detached 恢复 SQL |
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

根 `log.go` 用节点查询组装完整的 `metastore.Change`，再交给 `changes.Record`。日志组件不回调 Store 来补充身份。snapshot 的取得、事务结果与资源释放仍由根 package 拥有；`Seeding` 在原有生命周期内持有 commit admission，不因目录拆分提前释放。

SQL lease recovery 与 `LeaseRecovery` 留在根 package，原生 evidence I/O 在 `nativelease`。保留文件仍先退役再排空、回收；后端与 authority 的锁顺序、各阶段 context 和关闭失败时的所有权规则继续由原来的拥有者执行。

## 公开类型与内部值

公开 import path、类型定义身份、字段和方法集保留在根 package，包括 `DurableState`、`DurableStartup`、`Window`、lease 值与接口、`ObjectLimits` 和 `ObjectStatus`。传入组件的值显式转换成内部表示，组件结果再转换回公开值；内部类型不借 alias 成为公开类型的实际定义位置。

根 [`LeaseAnchor`](../../../packages/metastore/sqlite/lease_anchor.go) 是以 `nativelease.Anchor` 为底层类型的独立定义，既有方法通过指针转换转发。转换保持同一地址和同一份 mutex、descriptor 与生命周期，不能复制 Anchor 值。构造器转换返回指针，Load/Advance 转换 evidence 值；nil receiver 沿用原有方法的行为。

错误组件保持原有 `Is`、`Unwrap` 与 `Classification` 语义，调用点不为穿过目录边界增加额外包装。组件不建立通用回调框架或向外暴露 coordinator 锁。

## 资源与测试归属

已落地的 `0001` 至 `0005` 迁移由 [`internal/schema/migrations`](../../../packages/metastore/sqlite/internal/schema/migrations) 嵌入并重放。目录移动不改变 SQL 内容、schema 版本或升级顺序。可读的 schema golden 与独立 v2/v3 历史 fixture 放在 [`internal/integration/testdata`](../../../packages/metastore/sqlite/internal/integration/testdata)；测试数据不参与运行时初始化。

公开 metastore 契约入口留在根 `sqlite_test.go`，与私有状态紧密耦合的故障注入也留在根 package。通过公开入口运行的黑盒测试集中在 `internal/integration`，共享原有的测试 helper；子进程入口与调用它的测试属于同一个测试二进制。原生 anchor 的局部测试跟随 `nativelease`。

根 `snapshot_plan_test.go` 直接检查生产 snapshot query；身份高水位的执行计划用例跟随 `dbstate` 的实际 SQL。模块边界不提供仅为测试导出的生产接口。

SQLite 验证递归覆盖 `./packages/metastore/sqlite/...`，生产覆盖率包含所有抽出的组件。`internal/integration` 仅从 go-cov 的结果汇总行中排除，测试仍执行且其覆盖的生产语句仍计入；这个 test-only 子树不放生产代码或生产子 package。命令与全部门禁由[测试策略](../../testing.md)拥有。
