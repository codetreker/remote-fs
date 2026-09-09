# Agent Note: SQLite 按职责拆分内部模块

Status: implemented

## 问题

SQLite 实现同时承载 schema 迁移、完整性校验、持久高水位、原生 lease 证据、变更日志、文件保留和元数据副本。它们集中在一个目录时，通用 SQL 约定与具体生命周期相互穿插，查找一个职责需要跨越不相关的文件与测试。

这些职责又不是彼此独立的服务：一次修改的准入、发布、见证和失败处置必须共同决定是否能继续服务。为减少目录内文件数而拆散这条顺序，会增加锁、回调和关闭协议；把公开类型移到内部 package 再起别名，也会改变依赖方观察到的类型定义身份。

## 决定

### 提取组件，保留原子操作的拥有者

根 `packages/metastore/sqlite` 保留公开 Store、Replica、Seeding、LockingStore 与 LeaseRecovery，以及同一个私有数据库 coordinator。运行期事务、最终发布、health fence、文件 pin/domain、snapshot 与 reseed 的生命期、Close/Abort 仍由根 package 组织。

内部组件按已有职责划分：`schema` 拥有迁移、绑定、完整性和 detached 恢复 SQL；`dbstate` 拥有持久状态、高水位及对账；`nativelease` 拥有原生文件所有权与 lease evidence I/O；`changes` 拥有日志记录、裁剪和有界解码；`sqlvalue` 与 `sqlerr` 分别提供 SQL 值约定和既有错误分类。依赖向内部展开，不反向导入根 package。

schema 准备继续使用原来的一次事务。根 package 只向它传递启动数据和见证是否存在，不传入 CommitWitness 回调。日志的节点身份组装仍在根 `log.go`，然后把完整 Change 交给日志组件。内部组件因此不需要通过回调重新进入 Store，也不需要导出 coordinator 的锁。

这保留了[持久对象存储](2026-09-04-local-disk-object-store.md)的 Commit/见证/关闭边界、[显式文件占有](2026-09-07-file-locks.md)的最终发布顺序，以及[保留文件](2026-09-08-live-file-handles.md)的退役、排空和回收次序。目录划分不重新决定这些行为，资源上限、取消边界和错误链沿用原有语义。

### 公开类型仍在原处定义

公开值、接口和常量继续由根 package 定义，跨组件显式转换内部值。公开类型的 PkgPath、名称、字段和方法集不因提取组件而改变。`LeaseAnchor` 在根 package 定义为以内部 Anchor 为底层类型的独立类型，既有方法通过指针转换转发：同一地址、mutex 与 descriptor 只有一个拥有者，不复制带状态的值。Load/Advance 转换 evidence，构造器转换指针，nil receiver 保留原有行为。

错误分类下沉到 `sqlerr`，不在调用处增加为了包边界而存在的包装。`Is`、`Unwrap` 与 `Classification` 继续表达原来的已知结果和不确定结果。

### 测试与持久资源随职责归属

迁移文件放在 `internal/schema/migrations`，已落地 SQL 内容和 schema 版本保持不变。schema golden 与独立 v2/v3 fixture 放在 `internal/integration/testdata`，只调整当前位置对应的生成命令说明。迁移不可改写的理由仍由[元数据复制](2026-08-27-metadata-replication.md)拥有。

公开 metastore 契约与依赖私有状态的故障注入留在根 package。原有黑盒测试及其共享 fixture 放在 test-only 的 `internal/integration`，子进程入口与调用者仍编进同一测试二进制。原生 anchor 的局部测试跟随 nativelease，snapshot 与身份执行计划测试直接检查各自拥有者使用的生产 SQL，不增加测试专用的生产导出接口。

测试仍递归执行，抽出的生产组件全部参与覆盖率。go-cov 只省略纯测试 integration package 的汇总行，它的测试和覆盖到的语句继续计入；匹配按 substring 进行，因此该目录及其子目录不能放生产代码。覆盖阈值、失败判据和预算不变，命令由[测试策略](../../../../docs/testing.md)维护。

## 备选方案

**把整个实现搬到 internal，再由根 package 转发 Store。** 这只移动了平坦目录，没有形成职责边界，还会改变具体类型的定义归属。保留公开定义并提取其依赖组件，才能同时减少内部耦合和保持类型身份。

**把事务、发布、pin 和 replica 所有权继续拆成独立 package。** 它要求新增跨包生命周期协议，或暴露原始锁和 Store 回调来维持原子次序。已有低层职责可以独立提取，不需要为目录整理重新设计这些协议。

**所有黑盒测试留在根目录。** 生产代码移出后，根目录的大部分文件仍然来自这些测试。将原有测试群及共享 helper 放进 integration 保留了测试边界，也避免为挪动测试扩大生产 API，或把它们拼成少数难以浏览的大文件。

## 后果

目录树反映了迁移、状态、日志与原生证据的责任，修改者可以沿单向依赖找到各自的实现和局部测试。代价是公开值与内部值之间的显式转换、指针转发，以及验证时必须覆盖完整子树；只运行根 package 不再代表验证整份 SQLite 实现。

这项结构边界依赖两个约束：原子发布和关闭继续由原来的 coordinator 组织，integration 始终保持 test-only。未来若需要改变公开类型、schema、取消或资源预算，应作为相应行为决定处理，不能藏在目录重排里。现有持久化、lease 和文件句柄决定继续有效；完整组件归属见[SQLite 内部设计](../../../../docs/design/server/sqlite-modules.md)。
