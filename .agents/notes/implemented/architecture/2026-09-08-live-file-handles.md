# Agent Note: 打开的文件保留身份并逐次确认修改

Status: implemented

## 问题

普通程序把 fd 当作一个仍然存在的对象引用。其它调用方覆写同一个文件后，原 fd 应能读取它的当前内容；名字被改名、删除或覆盖后，fd 仍应指向原对象。按路径重建对象会把旧 fd 的操作送到同名新文件，每次打开保存一份字节又会让同一对象的 fd 长期分叉。

把修改留到关闭时整份替换还改变了常规系统调用的含义：成功的 `write` 尚未发布，配额或网络失败可能只出现在 `close`，两个描述符的局部修改会互相覆盖。`dup` 与 `fork` 能产生多次 `Flush`，而最后一次 `Release` 的错误不传给应用；把提交绑到其中任一事件，都不能同时解决修改时机与错误归属。

协作程序还需要标准 `flock` 与传统 POSIX `fcntl` 范围锁。它们的 owner、关闭、转换和等待语义不同于显式 S/X 权限；把标准 advisory 请求解释成强制授权，会改变未参与加锁程序的普通写入行为。

## 决定

### fd 保留对象，读写逐次发生

挂载通过 `FileStorage` 建立有限 FileSession，以 `OpenNode` 保留普通文件身份。每个 `File.ReadAt` 返回同一对象状态的属性与区间字节；后续读取可看到已经完成的修改。FUSE 使用 direct I/O，不保留每 fd 或每节点的 dirty 全文件副本。身份属性使用 `StatNode`、`SetNodeAttr` 或已有 File；节点 ID 直接成为内核 inode number，跨挂载改名到新名字也保留同一编号。本地 serial 只记录名字成员的发现次序；路径变化不构成重新绑定的理由。

`WriteAt` 与 `Truncate` 同步发布并在各自调用上报告结果。普通重叠写入按实际提交顺序生效，未修改区间保留。原生 revision CAS 只解决构造替换时的并发状态变化；它不把打开时内容作为强制前置条件。显式版本工作流仍由[排序与版本](../../proposed/architecture/2026-08-19-ordering-and-versions.md)和[操作词汇](../../proposed/architecture/2026-08-19-storage-operation-vocabulary.md)拥有。

Open 不 hydration 内容。objectstore 的不可变完整对象格式保持独立：区间读取和补丁仍可物化当前完整内容，替换同时预留当前与下一份内容。卷默认单文件 1 GiB、同时物化 2 GiB，有限尝试与操作截止时间限制竞争成本。只有确认未提交且清理成功的 revision 竞争可以重新尝试，未知结果以 `EIO` 暴露。

### 名字离开后仍计量与回收

SQLite v5 将仍被引用的无名普通文件保存为 detached 节点。名字树和日志不包含它，fd 继续使用它，用量仍包括它。最后一个引用先在发布门处退役、排空已接纳操作，再物理释放并结算对象与配额。未知提交、记账或清理结果保留相关所有权，不提前腾出可复用名额或容量。

只有原生独占数据库所有者提供 retained-file 能力。启动有界验证全部卷后，原子回收旧 epoch 无主节点；共享 opener 不能误收另一活跃所有者的引用。原有[对象发布次序](2026-08-21-volume-in-an-object-store.md)、[本地持久对象边界](2026-09-04-local-disk-object-store.md)及[未知发布的所有权](2026-09-04-unresolved-object-publication.md)继续成立。

### 标准 advisory 与强权限分别解释

flock 按 open file description 归属，dup/fork 共享，最后一个共享描述符关闭后释放；EX 可以在只读 fd 上取得。传统 POSIX 锁按挂载会话内的内核 owner 与文件归属，同一文件的任一 fd 关闭都释放该 owner 的范围，fork 不继承。PID 只用于诊断，不跨挂载合并身份。两种锁的冲突域独立；flock 转换先放弃旧锁，POSIX 失败转换保留旧范围。

健康会话中的阻塞加锁可跨多次短请求持续等待，所有 owner、范围、pending、动作历史和死锁图都有上限。动作使用 epoch 与随机 nonce 核对；取消只有在确认没有残留授予后才报告 `EINTR`。原生 advisory 丢失占有连续性时，相关 I/O 持续失败直到显式解除或关闭。FUSE 遇到未知锁结果则封锁整个挂载、停止续期并退役会话，单个 fd 的解锁或关闭不恢复它，须清理并重新挂载。两层都不能自动重获锁掩盖失效窗口。

标准 advisory 不阻止未参与加锁者的修改。[显式 S/X](2026-09-07-file-locks.md)继续保护内容、存在与身份，并在最终发布检查 proof。普通 Open 不自动加锁；经授权的名字移除令强资源 `TargetGone`，fd 与 advisory 随旧对象保留。FileSession、advisory owner、强 S/X Session/Owner、HTTP 连接与复制 incarnation 的生命周期分别核对。

### 关闭只结束它拥有的生命周期

`Flush` 显式用内核 owner 调用 DropLocks 处理该关闭事件的 POSIX 清理，最终文件释放结束引用与 flock；两者不承担内容提交。直接 File API 的 Close 不推断进程 owner，集成方须同样显式报告 POSIX 关闭事件；FileSession.Close 才结束会话全部状态。`Sync` 保留已完成写入的健康、持久性确认，所以 `写临时文件 → fsync → rename` 仍有明确顺序。`FlushTimeout` 是清理与挂载建立预算，已经不是上传时机。

挂载在已确认期限内续期 FileSession。失败的 Unmount 保持续期；内核真正退出后，挂载停止并排空会话，即使个别 Release 没有到达。`Mount.Done` 表示清理尝试结束，`Mount.Wait` 返回清理错误，关闭一个引用失败也不跳过其它引用的清理。HTTP handler 同样只清理自己创建的 registry，调用方在它结束之后才关闭 backend。

## 备选方案

**保留每 fd 全文件快照，在重新打开时刷新。** 它不能满足原 fd 读取同一对象当前状态，也不能让两个 fd 的局部修改按普通系统调用顺序组合；缓存失效通知不能修复已经分叉的私有内容。

**每节点共享稀疏暂存，dirty 时由 Flush 或 fsync 提交。** 这是[写会话与稀疏暂存](../../rejected/architecture/2026-08-19-write-session-and-staging.md)提出的机制，可减少内容取回并为同机映射提供真实暂存文件，但仍把普通 `write` 成功与权威提交分开，需要定义失败内容、重复关闭和崩溃后的保留。同步修改消除了这组本地 dirty 状态，代价是每次写入都承担远端确认。

**最后一个引用释放时提交。** 它有唯一结束点，但 FUSE Release 的错误不能传回应用，不能作为成功写入后的最后失败出口。

**普通 Open 自动取得强 S/X。** 这会把未参与加锁的程序也变成受强制占有策略控制的调用方；标准 advisory 的只读 flock EX、范围、owner 与等待规则也不能由 S/X 代替。强权限保留为显式扩展。

## 后果

fd 的身份、读入的属性和字节来自同一保留对象；配额、网络与持久化失败直接落到同步修改。旧的[内容与长度混用](../bug-fix/2026-09-07-bind-buffered-reads-to-their-size.md)与[跨句柄页缓存](../bug-fix/2026-09-07-prevent-cross-handle-page-cache-staleness.md)分别保留具体触发、证据和修复边界。direct I/O 付出内核页缓存命中率，逐次写入付出往返和完整对象重新物化的成本。objectstore 的每次区间读仍可获取完整对象，顺序读大文件会重复承担这份传输与分配；区间接口本身没有交付对象后端的范围传输。多个普通写者可能都成功，不再把所有普通 fd 修改解释为显式版本比较。

保留身份不等于保留每一份历史内容，也不自动修复目录操作的父身份竞争。[打开文件身份提案](../../proposed/architecture/2026-08-20-nothing-pins-an-open-file.md)保留路径目录操作与显式内容依据；[读取与清扫](../../proposed/architecture/2026-08-21-readers-in-flight-and-the-sweeper.md)保留旧内容键在读取前被回收的协调问题。文件 direct I/O 也不交付[非零元数据缓存与失效策略](../../proposed/architecture/2026-08-19-kernel-cache-and-unreachable.md)。

同机 mmap 的完整行为没有由 direct I/O 自动得到保证，跨客户端 mmap 一致性继续在 R-FS-4 之外；恢复共享映射能力必须单独定义写入确认与缓存交互。完整 `F_OFD_*` 语义同样没有承诺，且 FUSE 归一化后的请求不足以可靠逐条识别它们。

R-ERR-3 对系统已经接纳却尚未确认的内容仍有条件性的保留义务；同步失败不等于每次失败都生成一份持久本地恢复记录。通用未知写入结果查询、R-ERR-4 清单，以及未来异步内容的预算与销账仍由[暂存预算与销账](../../proposed/architecture/2026-08-19-staging-budget-and-discharge.md)拥有。没有本地既有状态也能冷挂载，不授权删除其它流程必须保留的失败内容。

本决定部分取代[MVP 的关闭提交与整文件缓冲](../process/2026-08-19-mvp-scope.md)，保留其当时的取舍；实现、资源边界和协议见[文件句柄设计](../../../../docs/design/server/file-handles.md)，可执行断言见[测试策略](../../../../docs/testing.md)。
