# Agent Note: 减少重复的 SQLite 测试准备

Status: implemented

## 问题

部分 SQLite 用例花大量时间准备相同状态：两个当前 schema 的损坏矩阵在每个子用例重建健康数据库，随后才注入它要检查的损坏；索引准入用例则用 20,000 次 Go 到 SQLite 的调用写入固定记录。前者要证明损坏被拒绝，后者要证明大集合上的索引与准入行为，重复构造本身不是这些判据。公共契约的邻居 namespace 只需制造真实日志间隙，持续创建新节点却让后续检查面对越来越大的无关树。

[基线 CI](https://github.com/codetreker/remote-fs/actions/runs/34327807431)的 checks 作业耗时 457 秒，mounted 作业 246 秒。它们包含并发执行的包、测试与清理，不能把用例耗时直接相加，也不能单凭作业时长断言某类 CPU 或 I/O 成本。需要在同一配置下测量准备方式的变化，同时保持原来的测试命题。

## 决定

### 邻居产生真实日志，目录保持固定

根 [metastore 契约夹具](../../../../packages/metastore/sqlite/contract_test.go)仍在同一文件数据库中打开被测 namespace 与邻居。每次被测修改之前，邻居对自己的既有根执行 SetAttr；原子计数器生成唯一的 ModTime，确保每次调用都发布真实 Modified 事件并消耗全局 position。失败仍使测试失败，不能省略间隙。

邻居的根身份、模式和空目录保持不变。回归用例直接读取两边日志，验证被测 position 稀疏、八次邻居修改的时间值各异（含四个并发调用），并检查树没有增长。时间值唯一不要求并发调用按计数器次序提交。把旧的逐次 Create 夹具装回去会使该回归失败；未改变 metastore 契约原有的操作和结果断言。

### 固定大集合由一条 SQL 构造

[`TestPendingAdmissionSeeksPastALargeReferencedSet`](../../../../packages/metastore/sqlite/objects_test.go)在原有事务内用递归 CTE 写入 20,000 条 referenced object。记录按 ordinal 插入，key、namespace、state、size、NULL digest、时间字段与原集合相同；`RowsAffected` 必须恰为 20,000。

用例仍对生产查询执行 EXPLAIN，并通过实际 Reserve 核对准入结果。它不缩小集合、不改成内存数据库，也不跳过真正被检查的路径；减少的是逐行跨越 Go/SQLite 调用边界的准备工作。

### 损坏矩阵复制完整、私有的健康种子

[`TestOpenRefusesInconsistentNamespaceIntegrity` 与 `TestRetainedIntegrityRefusesInvalidDetachedState`](../../../../packages/metastore/sqlite/internal/integration/integrity_test.go)各自在顶层用例中建立一份种子。种子通过原有真实 Open 与公开 mutation 形成，两个 namespace 的 Store 和查询句柄均检查关闭结果；确认 journal mode 为 WAL 后执行 TRUNCATE checkpoint，要求 busy、frames、checkpointed 全为零，再关闭最后句柄，读取完整主数据库字节和对应 fixture 元数据。

每个子用例把这份不可变镜像写入新路径的 `0600` 普通文件，拥有独立 inode、连接和可变状态。ID、高水位、日志和对象 key 保持一致，原来的 detach、损坏、Open 或先打开后 ObjectStatus 的顺序与错误断言保持原样。

种子同时接受健康与隔离检查：第一份副本通过公开 Create 写入并能 Stat；第二份副本能通过 Open/ObjectStatus，且看不到第一份副本的探针。顶层 cleanup 再核对种子字节未变。没有全局缓存、共享活句柄或共享可变数据库，也不能在 WAL 尚未完整归并时只复制主文件。

复用仅限这两个当前 schema 的损坏矩阵。迁移、WAL、恢复、lease、原生文件身份及跨进程用例继续使用各自原有的准备路径；这些用例的状态形成过程本身属于被验证的行为。

## 备选方案

**每个损坏子用例重新构造完整健康数据库。** 它天然隔离，但为相同前置状态重复执行 schema 与公开 mutation。顶层只构造一次、每例复制独立数据库，保留真实来源和隔离，并通过健康对照检查复制本身。

**逐行执行已准备的 INSERT。** 它保留相同数据，却仍需 20,000 次调用。集合值和顺序固定，可以由一条 SQLite 语句构造；精确行数、EXPLAIN 和实际 Reserve 继续约束结果。

**邻居为每个间隙创建新节点。** 它确实产生真实日志，却同时增长被验证数据库中的另一棵树。间隙的目的在于消耗其它 namespace 的 position；修改一个既有根可以满足这个目的，并由日志与隔离断言确认。

## 后果

同一环境以 `GOMAXPROCS=2`、`-race -count=1 -p=1` 串行运行，预热编译独立记录。每项优化前后各测三次，以下为顶层测试耗时中位数，包含各自准备与子用例，优化后的矩阵也包含新增健康／隔离对照：

| 用例 | 原准备方式 | 优化后 | 减少 |
|---|---:|---:|---:|
| 公共 metastore 契约 | 45.30 秒 | 40.30 秒 | 11.0% |
| 20,000 条 referenced object 准入 | 4.41 秒 | 2.48 秒 | 43.8% |
| 当前 namespace 完整性拒绝矩阵 | 17.90 秒 | 2.97 秒 | 83.4% |
| detached 状态拒绝矩阵 | 16.97 秒 | 4.29 秒 | 74.7% |

每次比较保留原 verdict 名称集合：公共契约为 97 个，准入用例为 1 个，两个损坏矩阵合计为 80 个；全部通过，没有失败或跳过。新增邻居回归另以 race 构建验证，旧夹具的负向对照在预期断言失败。编译、进程及包的耗时不混入这张表。这些是局部匹配结果，不能直接推算新的流水线耗时；整条 CI 的效果仍须由实际运行观察。

代价是为矩阵保存一份种子字节，并为每个子用例复制文件、重新打开真实 SQLite。种子必须完整、不可变且只在所属顶层用例内存活；损坏或隔离检查失败就结束测试，不能回退成一份看似有效的新数据库。

[包内覆盖](2026-09-09-sqlite-package-local-coverage.md)、race、`-count=1`、断言和数据规模保持不变；[执行预算](../process/2026-09-08-budget-ci-race-test-execution.md)不因优化而放宽。此决定沿用[在所属边界注入失败](2026-08-22-failures-injected-at-the-objects-boundary.md)与[资源由创建者清理](2026-08-22-a-mount-belongs-to-whoever-attached-it.md)的约束，具体测试准备规则见[测试策略](../../../../docs/testing.md#sqlite-测试准备与隔离)。
