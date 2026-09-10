# Agent Note: 按操作阶段处理请求中断

Status: implemented

## 问题

FUSE 请求因信号被中断后，go-fuse 会取消请求 context。若操作响应取消且没有产生副作用，把它报告为 `EIO` 会让原本能够处理 `EINTR` 的程序失败；但已经发出的远端修改也不能仅因 context 被取消就宣称操作已撤回。这分别关系到 [R-FS-2 与 R-ERR-1、R-ERR-2](../../../../docs/spec/requirements.md)：正常的请求中断要可识别，真正故障与未知修改结果必须保留。

2026-08-22 的调查在 Linux 6.8.0-137、go1.26.5 上记录了约 6 / 73 次命令用例失败，CI 历史 13 次中有 1 次。失败涉及读取、改名后的属性查询、建目录与额度测试中的打开文件。临时日志捕获过以下错误；协议路径以占位符保留，因为具体版本与故障无关：

```text
stat "over.bin": Get "http://…/<protocol>/stat?path=over.bin": context canceled: input/output error
```

当时 Python 驱动的同批二进制在重载下 52 / 52 次通过，Go 测试驱动约 5% 复现。它支持信号中断相关的解释，但没有记录到能证明每次失败都由 SIGURG 异步抢占触发的信号轨迹。当时的用例使用 `-dir` 服务端，没有元数据副本，因此故障不限于复制路径。该后端已由[移除宿主目录后端](../simplification/2026-09-08-remove-the-host-directory-backend.md)取消；这份观测仍说明请求中断的分类必须由共用层处理。

四项历史用例各运行 100 次的固定样本记录了 398 次通过与两次失败：一次文件关闭返回 `EAGAIN`；一次在配额 65536 字节、已有 32768 字节时，`Write` 成功接受 36864 字节，直到 `Close` 才以 `EDQUOT` 拒绝。原样本未保存取消原因的轨迹，因此不能把其每次失败直接归因于特定信号。

当时的同步信号探针分别证明了两条机制。关闭请求在写入确认 admission 前被中断，真实 `replicated.Write` 返回 `EAGAIN`，HTTP `Write` 发出零次，存储中的内容保持原样，但 `Close` 已消耗描述符，随后查询该描述符得到 `EBADF`。另一条探针在初次 `Space` 查询期间中断 `Write`：真实查询返回 `EINTR`，缓冲区仍接受全部 36864 字节，提交才被配额拒绝；存储里的目标内容没有越过配额。关闭不能依赖调用方重试，未完成的容量探测也不能在先接受内容后把拒绝推迟到关闭。这些是旧缓冲路径的观测，当前[实时文件句柄](../architecture/2026-09-08-live-file-handles.md)已将普通修改改为同步确认。

## 决定

### 错误分类保留操作结果

`storage.ErrnoOf` 是 FUSE 与 HTTP errno 编码共用的分类入口。`nil` 对 FUSE 表示成功，返回 0；用于编码失败的 `ErrnoNameOf(nil)` 仍返回 `EIO`。已接受的纯取消归为 `EINTR`；deadline、无法命名的错误与不确定修改归为 `EIO`。

分类逐节点折叠错误树。当前节点提供 `Classification() error` 时，其返回值拥有整个子树的分类，外层仍可保留底层原因供诊断；分类器不能跨过它搜到一个较深的取消并覆盖结果。独立错误分支中的真实故障优先于取消，不受 `errors.Join` 顺序影响。

### 取消能否被接受取决于操作阶段

| 位置 | 取消结果 |
|---|---|
| 尚未调用 HTTP `Do` | 可以证明请求未发出，返回 `EINTR` |
| 副本在请求发出前等待 mutation confirmation admission | 纯取消为 `EINTR`；deadline 为 `EIO`；实际容量饱和或 storage 正在关闭仍为 `EAGAIN` |
| 已发出的只读 HTTP 请求 | 放弃本次读取，返回 `EINTR` |
| mutation 已进入 HTTP `Do`，未获得权威结果 | 无法证明请求未发出或修改未发生，返回 `EIO` |
| mutation 已成功，副本 barrier 确认被取消 | 卷已经改变，返回 `EIO` |
| FUSE 复合操作已有本地或卷效果，后续步骤被取消 | 整个操作不能作为未执行的请求重试，返回 `EIO` |
| Open/Create、Close 或 Renew 已派发，远端结果无法核对 | 可能改变引用或生命周期，返回 `EIO`，不能据传输取消推断未发生 |
| advisory 获取被取消 | 只有核对证明无残留授予才返回 `EINTR`；未知结果使受影响 I/O 隔离 |
| `Flush` 收到关闭线程的取消 | 以独立且有 deadline 的 context 完成 owner 清理，不承担内容提交 |

HTTP transport 的底层 errno 保持隔离：连接 Unix socket 失败时的 `ENOENT` 不能变成卷不存在。FUSE 的 Create、Mkdir、Setattr 仍保留已发生效果；创建并打开在原生结果里完成，File / FileSession 的属性操作按身份访问，没有设置时间前提交其它 handle 缓冲区的阶段。后续取消的原因可以被追溯，但外层 `EIO` 不被其覆盖。

SQLite 的纯只读取消保留 context 原因并归为 `EINTR`。只读事务清理使用拥有该事务的 context 判断自动回滚：[database/sql 的 Tx.awaitDone](https://github.com/golang/go/blob/e3336a22ad3f0a90bd252c95d8b5544e02674205/src/database/sql/sql.go#L2207-L2230)在 context 取消后主动回滚，[再次 Rollback](https://github.com/golang/go/blob/e3336a22ad3f0a90bd252c95d8b5544e02674205/src/database/sql/sql.go#L2324-L2359)可直接返回 `sql.ErrTxDone`。这种收尾也可能发生在查询回调成功之后。真实查询错误与独立清理故障不会被取消覆盖；deadline、无法命名的故障、未知 commit 或 poison 仍为 `EIO`。SQLite code 9 只在确有已取消的读取 context 时解释为取消，不能仅凭 `SQLITE_INTERRUPT` 数字推断请求已撤回。

### 关闭清理不依赖调用方重新关闭

[Linux close 先从描述符表移除 fd，再执行 filp_flush](https://github.com/torvalds/linux/blob/e8f897f4afef0031fe618a8e94127a0934896aba/fs/open.c#L1539-L1554)。[Go 的关闭实现也不重试 EINTR](https://github.com/golang/go/blob/e3336a22ad3f0a90bd252c95d8b5544e02674205/src/internal/poll/fd_unixjs.go#L18-L24)，因为同一数字可能已经被复用于另一个文件。关闭失败后的重试不具备普通读取的前提。

当前 Flush 清理内核指定的 POSIX owner，最终 Release 清理 flock 并释放 File 引用。它们从请求 context 保留值与较早 deadline，忽略关闭线程的取消；`Options.FlushTimeout` 为清理提供预算，零值取 `DefaultFlushTimeout` 的 30 秒，负值在挂载前拒绝。独立命令的 `-timeout` 同时配置 HTTP 操作与这一预算。

完成 context 在执行清理之前建立，等待会消耗既有 deadline。它不承诺 mutex、Mount.Wait 或 Unmount 的总耗时；同一引用的并发关闭共享结果，未知清理保留错误并隔离 I/O。关闭一项失败不能跳过其它依法需要的清理。普通 WriteAt / Truncate 已在原调用里同步确认，Flush 没有待提交字节；Fsync 调用 Sync，继续核对引用健康与必要的持久性屏障。

### 配额拒绝发生在同步修改中

旧实现用 roomGauge 预检本地缓冲区增长，取消必须在接受那些字节前返回，并保留旧探测时间以便立即重试。同步写穿去掉了这个预检器：后端在 WriteAt / Truncate 的实际发布处检查配额，拒绝在同一次调用中返回，不再允许先接受 dirty 内容后于 Close 失败。Statfs 仍查询 Space，查询的取消与未知结果继续通过共享 errno 分类器传播。

修复继续拥有错误的阶段判定与跨层传播。[首次副本订阅的构建取消](../../proposed/bug-fix/2026-09-07-cancel-initial-replica-subscription.md)仍需要初始化与长期订阅的生命周期交接；[容量上限](../architecture/2026-08-21-space-limit.md)与其计费决定分别拥有权威用量、实际发布结算及通用包装的剩余限制。

### 原请求必须得到回复

[go-fuse 的 Context 约定](https://github.com/hanwen/go-fuse/blob/423b377e1452ab7b3522229185a3047f72e3f966/fuse/context.go#L12-L18)规定，文件系统接受请求取消时回复 `EINTR`；[其 API 说明](https://github.com/hanwen/go-fuse/blob/423b377e1452ab7b3522229185a3047f72e3f966/fuse/api.go#L413-L419)说明非忽略信号与 Go 抢占均可产生中断。[Linux FUSE 文档](https://github.com/torvalds/linux/blob/e8f897f4afef0031fe618a8e94127a0934896aba/Documentation/filesystems/fuse.rst#L136-L173)允许忽略 `INTERRUPT` 继续原请求，或向原请求回复 `EINTR`。[go-fuse 的协议发送逻辑](https://github.com/hanwen/go-fuse/blob/423b377e1452ab7b3522229185a3047f72e3f966/fuse/protocol-server.go#L63-L94)区分 `INTERRUPT` 消息自身与原请求：连接仍存活时，原请求的 `EINTR` 会正常序列化并回复。

## 备选方案

**在错误链里找到 `context.Canceled` 就统一返回 `EINTR`。** 它会覆盖与取消并列的真实故障，也会越过保留取消原因的未知 commit、poison 或已发生效果。按错误树与当前节点的权威分类折叠，才能保留这些区别。

**返回 `ECANCELED`。** 现有 wire 词汇表已包含 `EINTR`，Go 的相关文件操作也能处理该 errno；加入另一个取消词汇不能解决操作阶段的问题。采用 go-fuse 对被接受中断的 `EINTR` 建议。

**不回复被中断的原 FUSE 请求。** `FUSE_INTERRUPT` 不免除原请求的回复义务；忽略原回复会让调用线程继续等待。请求被接受取消时仍回复 `EINTR`。

**在测试里重试、预热或关闭异步抢占。** 这些做法改变信号或首次操作条件，掩盖实际程序仍会遇到的错误。保留原用例的断言和执行条件，另加确定性的信号与阶段测试。

**让 `Flush` 像普通查询一样接受取消并返回 `EINTR`。** `close(2)` 已经消耗描述符，普通 Go `File.Close` 不会以同一描述符重试；未提交内容可能随 `Release` 消失。只给 `Flush` 一次受预算约束的完成机会，保留其它可取消操作的行为。

**容量探测被取消后继续使用无测量状态。** 它把尚未完成的配额检查变成写入许可，使拒绝拖到 `Close`。在修改缓冲区前传播已分类的 `EINTR`，并保留旧刷新时间，才能让立即重试重新完成探测。

## 后果

被接受的中断以 `EINTR` 到达调用方；deadline、独立故障与未知修改结果保持 `EIO`。FUSE 与 wire 编码共用分类，保留底层原因不再等于允许它覆盖外层操作结果。

错误传播的成本落在能证明状态的几处：HTTP 请求是否派发、SQLite 只读事务如何结束、修改是否已生效，以及关闭之后的清理能否确认。File 与 advisory 控制另有有界动作核对；普通内容修改的未知结果不能靠重试掩盖。清理预算不是卸载时延保证，失败也不授权提前释放未确认的引用或配额。

该修复当时的三项真实信号用例共有五种子进程场景，普通执行与固定五次 `-race` 均通过。读取场景在 HTTP `Stat` 已进入服务端后发送 `SIGUSR1`：原始 `Fstatat` 得到 `EINTR`，普通 `os.Stat` 成功，随后两者读到完整内容。关闭场景关联同一 `FLUSH` 与 `INTERRUPT`，确认 Close 成功、HTTP 只写入一次。容量场景中，原始 Write 得到 EINTR，普通 Go Write 重新探测后得到 EDQUOT，两者分别查询一次和两次 Space，目标保持为空。恢复旧分类、Flush 或忽略容量中断的对照令对应断言失败。这些结果记录的是原来的关闭提交与缓冲区预检机制，不是当前同步文件 API 的验收回执。

以 [32249d3](https://github.com/codetreker/remote-fs/commit/32249d3c91defcd92d62f205523d6c267e874ee8) 为基线的修复通过固定历史样本验收：四项原始 `cmd` 用例各执行 100 次 `-race`，合计 400 次通过、零失败、零跳过，耗时 301.085 秒；运行期间 77 个生产 Go 文件的内容哈希保持不变。

确定性阶段测试覆盖错误树、只读事务清理、HTTP 派发前后与 FUSE 复合效果，完整策略见[测试策略](../../../../docs/testing.md#请求中断与修改结果)。这些验证不保证任意信号都会取消系统调用，也不将原调查中的每次失败归因于一个未经跟踪的具体信号。
