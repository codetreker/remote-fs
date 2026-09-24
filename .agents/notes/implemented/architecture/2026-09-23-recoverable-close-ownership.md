# Agent Note: 可恢复的引用关闭与删除义务归属

Status: implemented

## 问题

引用关闭可以同时结束引用占有并触发已经接受的删除义务。删除触发或清理失败时，单一 `error` 不能说明引用是否已经释放；调用方若把错误一律当作未释放，可能保留已经消失的能力，若一律当作已释放，则可能丢掉仍须重试的 owner。传输取消尤其不能证明服务端没有完成关闭。

关闭时删除义务已有持久 ID 和状态，但进程丢失原 ID 后，重启的新会话无法发现尚未 ACK 的责任。只按已知 ID 查询，要求调用方在本地额外保存一份与 authority 一致的完整清单；关闭及本地记录之间的故障窗口会使义务永久不可见。

## 决定

### 关闭报告引用归属

`FileSession`、`File` 与 `NodeReference` 提供 `CloseWithResult(ctx) (ReferenceCloseResult, error)`；`ReferenceCloseResult.Released=true` 表示该次调用结束后，调用方已经释放这项引用。成功关闭必须报告 true。删除义务的语义错误可以与已释放引用同时返回；清理尚未确认时，错误和未释放结果共同保留重试责任。原有 `Close` 保留为只返回错误的便利入口，内部遵守同一生命周期。HTTP 和所有第一方包装层传递原结果，不用错误种类或取消推断释放事实。

HTTP 的 `file.close` 与 `file.session-close` 携带 `FileActionID`。服务端在能力退役后仍保留有界关闭回执；一次会话关闭先失败、后由另一个 ID 完成时，终态记录保留每个仍在历史期限内的动作及原结果。同一 ID 与输入重投不再次执行原生关闭，`QueryFileAction` 报告该动作状态；已释放关闭可报告 Completed，这不证明 mutation barrier 已知。查询不携带 `Released` 或原错误，丢失响应须重投原关闭动作取得它们。`closeResult.barrierPending=true` 只出现在 `released=true`、无 barrier 的错误响应中：重投只继续核对同一已完成效果的 barrier，不能恢复引用或重复触发删除义务。`barrierPending=false` 是最终结果，即使没有 Log 而不携带 barrier；它仍保留原生关闭的语义错误。

### 持久 owner 与单调发现顺序

`CloseIntent` 在接受时保存调用方给定的 `DeleteIntentOwner`。owner 是调用方跨自身重启保存的发现命名空间，不是授权凭据，也不改变原义务所绑定的 NodeID 与名字关联。同一 owner 的 `ListDeleteIntents` 按每 volume 持久、单调递增的序号分页；`DeleteIntentCursor` 只表示已经扫描到的序号。ACK 移除终态记录后，旧序号不复用。调用方可以从零游标重新扫描，或持久保存下一游标后继续；并发 ACK 不会使后续记录前移到已扫描位置。

`QueryDeleteIntent` 和 `AcknowledgeDeleteIntent` 同时指定 owner 与 ID。错误 owner 的查询返回 Unknown，ACK 为幂等零效果；它不能读取或 ACK 另一 owner 的义务。终态 ACK 仍是带独立 `FileActionID` 的幂等动作，可由 `QueryFileAction` 核对；未 ACK 的状态和发现顺序跨 authority 重启保留。列表有固定页数上限，不能把有界持久记录一次性装入响应。owner、游标和 ID 都不能替代每次请求的业务授权。

### 权威边界

SQLite 在原有删除义务事务与见证提交中保存 owner、序号和不可回退的每 volume 高水位；旧 v8 记录迁移为以原 intent ID 为 owner，使持有旧 ID 的调用方仍能核对。owner 长度为 1 至 128 字节，要求有效 UTF-8 且不含 NUL；游标不能超过 `MaxInt64`，单页最多 256 条。HTTP 的 list、query 和 ACK 各自按独立 `storage.Operation` 授权，授权在读取持久状态或动作历史之前完成。limited、locked、objectstore、localstore 与 replicated 包装层保持同一 owner 和游标语义，replica 不保存这份责任账本。

本决定交付平台中立的所有权和发现能力。SMB 句柄、FileId、文件命令和消费删除义务的端点生命周期由后续文件适配工作接入；当前 SMB 端点继续拒绝未实现的文件命令。

## 备选方案

**把任何 Close 错误解释为引用仍存活。** 删除义务可能已经完成且引用已经释放，调用方会重试一份不再存在的能力；对取消和响应丢失也无法作出正确判断。

**只靠调用方保存每个 DeleteIntentID。** 服务端接受义务与调用方持久记录之间存在故障窗口。即使 ID 查询本身耐重启，也无法发现调用方从未成功记录的 ID。

**在本次变更中同时接入 SMB 文件命令。** 那会把中立的恢复契约与句柄表、FileId 和 Windows 错误映射绑在同一审查范围。当前结果类型与持久 owner 已经为后续适配保留所需事实；增加消费者不要求改写这套持久记录。

## 后果

调用方可以把关闭后的引用责任与删除义务的业务结果分开处置，并在丢失单个 intent ID 后通过持久 owner 找回未 ACK 的义务。代价是每条义务增加 owner 和持久序号，迁移与持久高水位需要和原有见证、容量及完整性检查一起维护；持久 owner 由调用方保存，遗失 owner 仍不能从未知身份枚举全 volume 的义务。ACK 后没有 tombstone，调用方仍须避免复用已经确认的 ID；序号高水位不会因 ACK 回退。HTTP 会话关闭保留历史期限内的有限动作回执，终态记录与活跃会话共同占用 admission 容量。回执过期后，已释放引用的清理失败继续进入有界汇总：累计次数和首个、近期错误样本供 `Handler.Close` 报告，不能由汇总重建已到期动作的原结果。

本决定接续[持久节点身份与原子文件操作](2026-09-20-durable-identity-and-atomic-file-operations.md)的 durable delete intent、[活跃文件句柄](2026-09-08-live-file-handles.md)的关闭所有权，以及[业务授权](../feature/2026-09-10-host-provided-authorization.md)的逐请求准入；它扩展查询和关闭结果，不改变那些决定的对象绑定、原子打开与已接受效果语义。当前契约见[文件句柄设计](../../../../docs/design/server/file-handles.md)，验证边界见[测试策略](../../../../docs/testing.md)。
