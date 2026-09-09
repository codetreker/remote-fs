# Agent Note: 各包以自己的测试验证生产职责

Status: implemented

## 问题

[覆盖率规则](../../../../docs/testing.md#覆盖率是必要的从来不是充分的)只由一个包自己的测试二进制为该包计入覆盖率。跨组件测试能验证组合行为，但它经过依赖包的代码，不表示该依赖已有足够的包内测试。

两个不同范围的基线测量都出现了「测试通过，覆盖门禁未满足」：

| 测量范围 | 通过的顶层测试／全部判定 | 语句覆盖率 | 低于 50% 的函数 |
|---|---:|---:|---:|
| SQLite 子树，Go 默认包内统计 | 263／831 | 57.37% | 100 |
| 全仓库 CI 的覆盖率步骤 | 1221／3832 | 75.9% | 143，分布于 15 个包 |

后者对应 [c086503 的 CI](https://github.com/codetreker/remote-fs/actions/runs/34322383181)，失败来自门槛，未发现测试断言失败或 race 报告。SQLite 子树的百分比不能代表全仓库，包级达到 70% 也不能代替每函数达到 50%。这组缺口包括 SQLite 组件、公开值与包装器，以及现行门禁计量的共用契约执行器。

## 决定

### 在实现所属包内断言结果

为这 15 个包补充自己的测试，继续按 `xxx.go` 对应 `xxx_test.go` 组织。测试直接断言成功结果、边界拒绝、取消、真实故障和资源释放；现有跨组件用例继续承担组合验证。生产实现、公开接口、schema 与覆盖率阈值和排除规则保持原样。下表路径相对于 `packages/`。

| 所属包 | 局部测试承担的验证 |
|---|---|
| `metastore/sqlite` | 构造与绑定、有限配置、checkpoint 与见证结果、发布记账、advisory 配对、replica 变更及失败回滚 |
| `metastore/sqlite/internal/schema`、`metastore/sqlite/internal/dbstate` | 真实 SQL 上的迁移、完整性与预算、持久身份和高水位、事务提交／回滚、WAL 启动证据、detached 回收 |
| `metastore/sqlite/internal/changes` | 类型与 payload 解码、日志记录、裁剪、前驱链、page 预算及原错误保留 |
| `metastore/sqlite/internal/nativelease` | 原生 SH/EX 所有权、路径与 descriptor 核对、证据编码、拒绝后的文件与锁保留 |
| `metastore/sqlite/internal/sqlvalue`、`metastore/sqlite/internal/sqlerr` | SQL 类型与基数、溢出边界、时间与 key 表示、真实约束错误和错误链分类 |
| `metastore`、`storage` | 属性和路径词汇、容量与参数边界、发布前检查、有限结果及首次失败保存 |
| `storage/limited`、`transport/httprest` | 保留引用的 quota 结算与拒绝、远端 advisory 取消与原动作核对 |
| `storage/lockcontract`、`storage/objectstore/objectstoretest` | 共用契约执行器在本包运行真实 fixture，并由同一测试二进制中的故障实现验证拒绝路径 |
| `storage/objectstore/memory`、`storage/objectstore/azblob` | 有界读取、调用方字节所有权、实际长度与 EOF、截断或读取故障、body 关闭和错误分类 |

SQLite 组件使用真实数据库和调用方事务观察状态变化，必要时在 SQL 或现有依赖边界注入故障。共用契约执行器有自己的调用测试；不会因为它们也被其它包运行，就省去自身的验证。测试机制与具体断言映射由[测试策略](../../../../docs/testing.md)维护。

### 统计与行为证据分别成立

测量使用 Go 默认包内 instrumentation，同目录的外部测试包属于自己的测试二进制。integration 和其它包的调用不给被调用包增加覆盖率。每包 70%、每函数 50%、全仓库 85% 继续使用原门槛；只跳过已有的结果汇总行，不改变测试执行或生产代码的排除范围。

局部 profile 须含真实执行块及函数记录，并与冻结的测试和生产输入对应。分包测试、race 与覆盖结果分别记录，最终交付仍以同一精确 commit 的完整 CI 为准；分包结果不能替代全仓库的 85% 判据。保留原来的 skip、无测试、缺少 verdict、超时及残留清理检查。

## 备选方案

**只保留已有跨组件用例。** 它们能验证接口组合和完整流程，却不能为依赖包提供本地测试覆盖，也不能单独判断某个内部组件的拒绝或清理分支。保留这些用例并补充所属包的断言，使两种证据各自成立。

## 后果

15 个目标包的分包本地复测均达到包级 70% 与每函数 50%，其中最低包覆盖率为 objectstoretest 的 73.2%。这些结果证明局部补测的覆盖边界，不是全仓库 CI 的通过声明。完整 CI 仍必须验证全部测试、race 与全仓库合计门槛。

局部 fixture 与直接错误用例增加维护和执行成本，也使参数、拒绝原因、事务状态与清理结果能在拥有它们的包内被检查。[SQLite 模块决定](../architecture/2026-09-09-sqlite-internal-modules.md)继续拥有实现归属；本决定只补测试，不改变原子发布、租约、配额或生命周期。

覆盖率仍只是必要证据。一个经过所有语句却没有检查错误结果的用例，不能替代这些行为断言；后续修改仍须保持各包自己的测试责任。
