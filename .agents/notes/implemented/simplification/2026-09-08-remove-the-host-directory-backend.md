# Agent Note: 服务端保留组合存储，移除宿主目录后端

Status: implemented

## 问题

直接导出宿主目录与持有一份受管理的持久 volume，是两种不同的存储责任。宿主 inode 会复用，目录改名会改变路径解析，内容替换会换 inode；让这些操作满足稳定身份、S/X 保护与最终发布顺序，需要独立的目标固定、协调、计费和故障处理。

随附本地持久形态已经由 SQLite volume 与 localdisk 对象组成。再维护宿主目录形态，还要为它持有单独的状态目录、绑定、恢复证据与 CLI 配置，并让测试覆盖第二套原生发布机制。把它当作方便的测试底层也会保留这些责任。[R-INT-13](../../../../docs/spec/requirements.md)要求私有本地目录能持久保存 volume，没有要求直接服务已有宿主目录树。

## 决定

删除 `packages/storage/localdir` 及独立 server 的 `-dir` 入口。服务端保留两种形态：`localstore` 使用 SQLite 与本地不可变对象，Azure 形态使用 SQLite 与 Blob。宿主目录专属的 StateRoot、目录资源上限、配额遍历旗标和 SIGHUP recount 入口随实现一起删除；旧旗标作为未知参数拒绝，没有兼容别名或自动转成另一种存储格式。

本地持久 volume 的确认后持久性、完整性、重启保护与跨客户端可见性要求不变。`localdisk` 是保留的对象实现，与被删除的宿主目录 storage 不是同一组件。两种服务端形态继续提供显式 S/X、有限历史、最终发布检查与变更日志。

库的边界继续保持可替换：`Storage`、有界结果、原生发布与配对锁服务的义务不变，`limited` 为提供有界结果与原生发布计费的第三方 backend 执行配额，未提供计费能力时在构造阶段拒绝，范围由[目录改名中的配额记账](../bug-fix/2026-09-07-keep-quota-accounting-stable-across-directory-renames.md)拥有。调用方没有提供 change log 时，复制操作仍明确返回 `ENOSYS`；不能用删除一个随附实现来把可选日志改成接口的隐含必需项。

库测试复用真实 SQLite 与内存对象组成的 [memoryfixture](../../../../packages/storage/lockcontract/memoryfixture/memory.go)，二进制与持久恢复测试使用保留的 localstore 和 Azure。需要表达特定身份、符号链接属性或错误的测试在对应接口装饰结果；当时迁移保留了真实信号、Close 与配额边界的原断言。后续[实时文件句柄](../architecture/2026-09-08-live-file-handles.md)改变提交与关闭职责，验证须按同步修改、引用和 advisory 清理分别表达，不能沿用旧缓冲路径的通过记录。过期暂存缩短的配额用例迁到 `limited`，删除旧 fixture 没有删除该保证。

本决定部分替代[早期范围](../process/2026-08-19-mvp-scope.md)、[文件锁](../architecture/2026-09-07-file-locks.md)与[容量上限](../architecture/2026-08-21-space-limit.md)中的宿主目录实现选择，保留它们的通用契约与历史理由。[本地磁盘对象存储](../architecture/2026-09-04-local-disk-object-store.md)和[对象 volume](../architecture/2026-08-21-volume-in-an-object-store.md)继续拥有保留的组合形态。

[元数据复制](../architecture/2026-08-27-metadata-replication.md)与[有界读取](../architecture/2026-09-04-bounded-read-and-list-responses.md)保持原契约；[读取可能输给写者](../architecture/2026-09-01-a-read-may-lose-to-a-writer.md)、[名字与身份](../bug-fix/2026-09-01-a-name-is-not-an-identity.md)、[标准库错误](../bug-fix/2026-08-21-the-standard-library-answers-for-the-kernel.md)和[请求中断](../bug-fix/2026-08-22-eio-from-a-freshly-mounted-mountpoint.md)中的旧宿主目录证据保留为历史，当前测试使用保留的组合与接口装饰。已完成的[缩短结算](../bug-fix/2026-09-07-release-shrunk-quota-after-commit.md)继续约束 `limited`。

[观察源与通道](../../proposed/architecture/2026-08-19-observation-source-and-channels.md)、[volume 契约](../../proposed/architecture/2026-08-19-volume-in-the-contract.md)、[打开文件的内容依据](../../proposed/architecture/2026-08-20-nothing-pins-an-open-file.md)和[对象存储缺口](../../proposed/architecture/2026-08-22-gaps-in-the-object-store-backend.md)仍包含不依赖宿主目录后端的未完成内容，保持 proposed。

## 备选方案

**保留并完整维护宿主目录后端。** 可以直接服务已有目录，代价是继续维护独立的身份映射、路径协调、发布计费、持久绑定与恢复协议；这些保证不能只靠通用包装器在写入前查一次路径来兑现。

**把宿主目录后端藏为测试专用实现。** 可以减少 fixture 迁移，但仍须维护它的操作语义与原生状态，还会让集成测试依赖一条不交付的存储路径。现有 SQLite／内存对象组合已能提供真实 volume 操作，持久性则由保留的 localstore 验证。

## 后果

调用方不能再用随附二进制直接发布已有宿主目录，需要选择受管理的本地持久存储、Azure，或自行接入满足契约的 backend。本决定不提供旧目录的迁移工具，也不把既有目录内容当作新存储格式自动接受。

以后若增加另一种 backend，仍须满足稳定逻辑身份、完整内容读取、有界资源、所有修改的最终授权与已确认租期的恢复保护；提供 FileStorage 时还须原生保留对象及其生命周期，不能通过重开原路径或另一份不配对的协调服务模拟。`limited` 的原生计费接入、显式内容版本工作流与目录父身份约束保持各自范围。普通 Open 不自动取得 advisory 或强 S/X，宿主目录实现的删除不改变这个选择。
