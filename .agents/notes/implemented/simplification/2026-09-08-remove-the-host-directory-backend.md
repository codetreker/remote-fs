# Agent Note: 服务端保留组合存储，移除宿主目录后端

Status: implemented

## 问题

直接导出宿主目录与持有一份受管理的持久 workspace，是两种不同的存储责任。宿主 inode 会复用，目录改名会改变路径解析，内容替换会换 inode；让这些操作满足稳定身份、S/X 保护与最终发布顺序，需要独立的目标固定、协调、计费和故障处理。

随附本地持久形态已经由 SQLite 命名空间与 localdisk 对象组成。再维护宿主目录形态，还要为它持有单独的状态目录、绑定、恢复证据与 CLI 配置，并让测试覆盖第二套原生发布机制。把它当作方便的测试底层也会保留这些责任。[R-INT-13](../../../../docs/spec/requirements.md)要求私有本地目录能持久保存 workspace，没有要求直接服务已有宿主目录树。

## 决定

删除 `packages/storage/localdir` 及独立 server 的 `-dir` 入口。服务端保留两种形态：`localstore` 使用 SQLite 与本地不可变对象，Azure 形态使用 SQLite 与 Blob。宿主目录专属的 StateRoot、目录资源上限、配额遍历旗标和 SIGHUP recount 入口随实现一起删除；旧旗标作为未知参数拒绝，没有兼容别名或自动转成另一种存储格式。

本地持久 workspace 的确认后持久性、完整性、重启保护与跨客户端可见性要求不变。`localdisk` 是保留的对象实现，与被删除的宿主目录 storage 不是同一组件。两种服务端形态继续提供显式 S/X、有限历史、最终发布检查与变更日志。

库的边界继续保持可替换：`Storage`、有界结果、原生发布与配对锁服务的义务不变，`limited` 仍能为履行其契约的第三方 backend 执行配额。调用方没有提供 change log 时，复制操作仍明确返回 `ENOSYS`；不能用删除一个随附实现来把可选日志改成接口的隐含必需项。

库测试复用真实 SQLite 与内存对象组成的 [memoryfixture](../../../../packages/storage/lockcontract/memoryfixture/memory.go)，二进制与持久恢复测试使用保留的 localstore 和 Azure。需要表达特定身份、符号链接属性或错误的测试在对应接口装饰结果；真实信号、Close 与配额边界继续由原断言验证。过期暂存缩短的配额用例迁到 `limited`，不因删除旧 fixture 而删除该保证。

本决定部分替代[早期范围](../process/2026-08-19-mvp-scope.md)、[文件锁](../architecture/2026-09-07-file-locks.md)与[容量上限](../architecture/2026-08-21-space-limit.md)中的宿主目录实现选择，保留它们的通用契约与历史理由。[本地磁盘对象存储](../architecture/2026-09-04-local-disk-object-store.md)和[对象命名空间](../architecture/2026-08-21-namespace-in-an-object-store.md)继续拥有保留的组合形态。

[元数据复制](../architecture/2026-08-27-metadata-replication.md)与[有界读取](../architecture/2026-09-04-bounded-read-and-list-responses.md)保持原契约；[读取可能输给写者](../architecture/2026-09-01-a-read-may-lose-to-a-writer.md)、[名字与身份](../bug-fix/2026-09-01-a-name-is-not-an-identity.md)、[标准库错误](../bug-fix/2026-08-21-the-standard-library-answers-for-the-kernel.md)和[请求中断](../bug-fix/2026-08-22-eio-from-a-freshly-mounted-mountpoint.md)中的旧宿主目录证据保留为历史，当前测试使用保留的组合与接口装饰。已完成的[缩短结算](../bug-fix/2026-09-07-release-shrunk-quota-after-commit.md)继续约束 `limited`。

[观察源与通道](../../proposed/architecture/2026-08-19-observation-source-and-channels.md)、[workspace 契约](../../proposed/architecture/2026-08-19-workspace-in-the-contract.md)、[打开文件的内容依据](../../proposed/architecture/2026-08-20-nothing-pins-an-open-file.md)、[对象存储缺口](../../proposed/architecture/2026-08-22-gaps-in-the-object-store-backend.md)和[通用目录改名计费](../../proposed/bug-fix/2026-09-07-keep-quota-accounting-stable-across-directory-renames.md)仍包含不依赖宿主目录后端的未完成内容，保持 proposed。

## 备选方案

**保留并完整维护宿主目录后端。** 可以直接服务已有目录，代价是继续维护独立的身份映射、路径协调、发布计费、持久绑定与恢复协议；这些保证不能只靠通用包装器在写入前查一次路径来兑现。

**把宿主目录后端藏为测试专用实现。** 可以减少 fixture 迁移，但仍须维护它的操作语义与原生状态，还会让集成测试依赖一条不交付的存储路径。现有 SQLite／内存对象组合已能提供真实 namespace 操作，持久性则由保留的 localstore 验证。

## 后果

调用方不能再用随附二进制直接发布已有宿主目录，需要选择受管理的本地持久存储、Azure，或自行接入满足契约的 backend。本决定不提供旧目录的迁移工具，也不把既有目录内容当作新存储格式自动接受。

以后若增加另一种 backend，仍须满足稳定逻辑身份、完整内容读取、有界资源、所有修改的最终授权与已确认租期的恢复保护；不能通过原路径的无条件读写或另一份不配对的锁服务绕过它们。第三方 `limited` 路径采样的祖先改名缺口、普通内容版本前置条件与自动 Open 占有策略保持各自范围，删除宿主目录实现不等于补齐这些保证。
