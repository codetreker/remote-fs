# Agent Note: 可恢复的引用关闭与删除义务归属

Status: implemented

## 问题

引用关闭可以同时结束引用占有并触发已接受的删除义务。删除触发或清理失败时，单个 `error` 无法说明引用是否已释放；传输取消也不能证明 authority 未执行。调用方误判释放会丢掉仍需重试的 owner，误判保留又可能对已经结束的能力发起新动作。

持有共享 `UseClaim` 的引用若在可能失败的最终化之前解除 claim，则最终化已知失败并报告引用保留时，其它冲突打开已能获准。关闭响应丢失后，旧动作的语义结果和同步 barrier 也不能单靠动作状态还原。一次确定失败需要以新动作再试；释放未知只能核对同一动作。这要求关闭 owner 和每次尝试都有稳定身份，且回执容量在打开效果前保留。

关闭时删除义务已有持久 ID 和状态，但进程丢失原 ID 后，重启的新会话无法发现尚未 ACK 的责任。只按已知 ID 查询，要求调用方在本地另存一份与 authority 一致的完整清单；关闭和记录之间的故障窗口会使义务永久不可见。

## 决定

### 关闭结果与尝试身份

`FileSession`、`File` 和 `NodeReference` 的 `CloseWithResult(ctx)` 返回 `ReferenceCloseResult{Released, Determined}` 与原错误。`Released=true` 本身表示本次调用后引用已确认释放，成功关闭必须报告 true；旧实现可以不设置 `Determined`，新 action-aware 实现同时设置 true。删除义务的语义错误可与已释放引用同时返回。仅当 `Released=false` 时，`Determined=true` 才表示 authority 能证明原引用、pin、Use claim 与必要容量仍保留，调用方可再次尝试；`Released=false, Determined=false` 保留未知事实，调用方只能继续核对原动作。`Close` 是只返回错误的便利入口，仍走相同生命周期。

需要调用方持有动作 ID 的引用实现 `ReferenceCloseActions`：`CloseWithAction(ctx, CloseAttempt{Action, Generation})` 返回同一 typed close 结果，`QueryCloseAttempt` 仅查询动作状态。`Generation` 从 1 开始；当前尝试确定未释放才可递增并采用新 action ID。相同 generation／ID 的已确定结果重投返回原释放事实与语义错误；Unknown 的同 ID 重投可在原引用上安全重新进入 native close，以 `finalizationDone`、原 claim 核对和释放 fence 结算未知状态，不重复权威删除或释放效果。没有未执行证明就同 generation 改 ID、跳代、未知结果换 ID、旧 ID 过期后重新执行均拒绝。只有新的**显式**尝试携带落后于当前清理 epoch 的 ID、并在回执准入和引用效果前被拒绝，才允许同 generation 换 ID：`CloseActionNotExecutedError{CurrentEpoch}` 证明该 ID 未执行，调用方才可用当前 epoch 的新 ID 重试。未来 epoch ID 直接返回 `EINVAL`，不建立回执或未执行证明；普通 `ESTALE`、超时和 Unknown 也不能授权换 ID。旧 ID 在证明保留期内仍报告未执行；清理 epoch 前进且没有保留回执后，绑定查询报告 Retired，旧 ID 始终不能重新作用于引用。`QueryFileAction` 或 `QueryCloseAttempt` 的 Completed 不携带 `Released` 或原错误，响应丢失必须用原 ID 和 generation 重投 close 方法取得完整结果。

`RecoverableReferenceClose.CheckRecoverableReferenceClose` 在 session 上检查整个包装链。每次可能成功的打开先为该引用保留两格关闭回执，并在 session 建立时保留 session-close 的格；数据动作历史饱和、会话普通数据准入退休或已释放动作的历史尚未到期，不占用仍活引用已保留的关闭机会。确定未释放后第二次关闭可以在旧失败回执仍保留时接纳；更多失败尝试须等待已终结的最早回执到期，`CloseOwnerStatus.Ready` 在名额重获后才变为 true，旧 generation 仍由 fence 拒绝。Pending／Unknown 回执保留当前 owner，不按时间逐出；已释放引用的终态回执仍占其历史名额，新 effectful open 在这些名额到期前可能被拒绝，到期清理再回收。`MaxCloseActions` 约束全部保留引用与回执的容量；内置 objectstore 每份打开预留两格，HTTP registry 还为每份文件和 session-close 保留 cleanup 额度。replicated 包装器另把每份显式动作未执行证明登记在有界 session 账本；只有绑定原动作的 `QueryCloseAttempt` 返回 Retired，或 parent session 的权威 `Released=true` 及 barrier 均结算，才回收该证明。仅本地超时或另一动作成功不能让旧 ID 在同一 session 重新产生效果。包装器把本地新关闭动作预留容量与下层状态合并为对外 `Ready`；未执行证明账本饱和不遮蔽其它可成功关闭的引用。若一次已获 Ready 的新动作后来在效果前因 ID 落后于清理 epoch 被拒绝，而证明账本暂满，包装器保留该引用的待存证明并返回 `EAGAIN`；调用方沿原 ID 续查，直到绑定旧证明 Retired 腾出名额后才换 ID。隐式清理在前一次确定未释放后按 `NextGeneration` 继续，不能重置为第一代。每份回执与 owner 均按界限计费，不以无界历史维持正确性。

HTTP 的 `file.close` 与 `file.session-close` 携带 transport 动作 ID 和 generation，并编码 `Released`／`Determined`。显式 `ReferenceCloseActions.CloseWithAction` 将调用方的 ID 原样传至 native；隐式 `CloseWithResult` 使用独立的 HTTP transport ID，HTTP server 调用 native 的 `CloseWithResult`；内置 objectstore 在自己的会话内选择并保留清理 ID。显式尝试的 ID 若落后于 native 清理 epoch 且在效果前被拒绝，HTTP 以封闭的 `CloseNotExecutedEpoch` 错误字段传递证明；未来 epoch 返回 `EINVAL`，不把普通 transport `ESTALE` 猜成未执行。HTTP client 对当前清理 epoch 的新候选 ID，先以原引用绑定的 `QueryCloseAttempt` 确认 NotExecuted，再安装本地 close cursor。旧 epoch 候选只可从 server 取得效果前 typed 未执行证明；其它服务端拒绝使 cursor 暂时不确定时，client 保留有界待核对状态并在同 ID 重投中继续绑定查询，只有精确 NotExecuted 才撤销本地候选。已取得的本地未执行证明按配置的 History 有界保留，到期后裁剪；HTTP server 的旧 ID 证明记住确认时的清理 epoch，原 ID 重投仍返回该证明，不因另一动作已释放引用而改写结果。服务端在能力退役后仍保留有限关闭回执，同 ID／generation 的已确定回执重投返回原生事实；Unknown 回执允许原 ID 安全恢复 native close。初次执行在发布回执并关闭 `done` 前独占结果；跨动作对账在释放本次重投锁后执行，未完成初次发布或仍由其它重投持锁的动作留给后续清理，不等待其它动作的重投锁。存在未完成初次发布或仍被重投占用的回执，或 native `QueryCloseAttempt` 失败、外部请求取消使核对未能取得释放证据时，原引用能力和会话继续保留，由独立清理续作；迟到的初次 Unknown 不能覆盖已经核对的 Released。已发布且可取得重投锁的显式 Unknown，只有 native 查询成功但不能将原 ID 绑定到已完成动作，才按原 Unknown 转入 terminal 历史；查询失败不能作为对账完成或丢弃绑定的依据，native session 已释放也不能证明该动作已释放。PendingAck 回收遇到在途回执时退役会话并安排续作；native session 已释放但 HTTP 回执仍需续核对时，`closeReconciling` 保存首次 native 释放事实和原语义错误，保留清理循环；后续轮次只续核对 HTTP 回执，不重复 native session 关闭或累计同一清理错误。待核对回执能够按其原事实转入 terminal 历史后，`Handler.Close` 才完成；超时保留同一清理 owner。已释放而 mutation barrier 未确认时，同一动作只继续 barrier，不再 native close；重投响应再次丢失也保留先前 `Released=true` 与原语义错误，barrier 可从 pending 单调结算。第一次会话关闭确定未释放、后来由新 ID 完成时，历史期内每次尝试各自保留结果。`file.close-owner-status` 的外部请求每次单独按 `OpFileCloseOwnerStatus` 授权，并绑定确切 file capability；活跃或退休但仍保留的引用把 native `CloseOwnerStatus` 原样穿过 HTTP。会话已经终结时，只有仍在回执期限内、绑定该 file capability 且确认 `Released=true` 的关闭动作可以回答已释放状态，不能借此恢复其它引用或推断未确认释放。HTTP client 和 limited、locked、replicated 等第一方包装层逐层传递 typed 结果、显式动作身份、未执行证明与原错误，不从错误类型或取消猜测释放。

### 退休后的窄清理准入

advisory session 退休或租期结束后，普通数据、owner、锁历史与 `FileSession.Status` 继续拒绝。`WithCloseAdmission` 只对已持有确切引用的清理路径提供会继续前进的 close epoch，并在同一临界区核对新 close action 与回执准入；`CloseOwnerStatus` 也从这条窄通道取得 epoch。内部 `CloseWithResult` 可据该 epoch 为尚未释放的原引用生成清理尝试，确定未释放后按下一 generation 继续；外部持有同一引用的调用方能从状态中接管其 Pending／Unknown 尝试，或在 `Ready=true` 时使用当前 epoch 和下一 generation。旧 epoch ID 即使回执已到期也不能重绑为新的关闭效果。此通道不能重新开放字节、名字、范围锁或普通 action history。

### 权威最终释放顺序

objectstore 的 File／NodeReference 关闭与 session fence 先退休引用并排空已接纳 I/O，清理辅助 owner；它们不在 SQLite 最终释放之前主动解除 Use。SQLite 在原共享 claim 仍安装时执行可能失败的 pending-unlink 或 detached 对象最终化。pending-unlink 的最终发布携带最后关闭引用的确切 owner：`CheckCloseUnlink` 验证原 NodeID、Scope 与完整 `UseClaim` 仍安装，只排除该 Scope 自己的 DeleteName deny，其它 claim 的拒绝继续生效。已经接受的删除义务可由只读的最后引用结算，不要求它重新取得 DeleteName use；无原引用的恢复则仍按匿名 `CheckUse` 核对，不能借 cleanup 标志绕过其它 holder。确认持久提交后记录 `finalizationDone`，同一活 session 的后续尝试从该点继续，不重复提交。提交不确定或原 claim 不可证明时，authority 保留所有权并 fence 冲突准入；不能报告确定未释放。

原生最终化完成后，在同一 commit gate 内以 `DropUseExact(NodeID, Scope, UseClaim)` 严格验证并解除原 claim。缺失或不匹配表明保护事实不可证明，authority fence 并返回未知；旧的幂等 `DropUse` 空成功不作释放证明。严格解除前若发生明确失败，原 claim 与 pin 均在，可以报告 `Determined=true, Released=false`；解除成功后没有其它可失败步骤，立即撤销 pin、引用表和容量，报告 `Released=true, Determined=true`。新冲突打开不能在 claim 解除与引用释放之间插入。跨 authority 进程重启不恢复旧易失引用；持久 delete-intent 仍由自身账本恢复。

### 持久删除 owner 与发现顺序

`CloseIntent` 接受时保存调用方提供的 `DeleteIntentOwner`。owner 是调用方跨自身重启保存的发现命名空间，不是授权凭据，也不改变义务绑定的 NodeID 和名字关联。同一 owner 的 `ListDeleteIntents` 按每 volume 持久、单调递增序号分页；`DeleteIntentCursor` 只表示已扫描的序号。ACK 移除终态记录后旧序号不复用。调用方可从零重新扫描，或持久保存下一游标继续；并发 ACK 不会使后续记录移动到已扫描位置。

`QueryDeleteIntent` 和 `AcknowledgeDeleteIntent` 都指定 owner 与 ID。错误 owner 的查询为 Unknown，ACK 是幂等零效果；不能读取或 ACK 另一 owner 的义务。终态 ACK 使用独立 `FileActionID`，可经 `QueryFileAction` 核对。未 ACK 的状态和发现顺序跨 authority 重启保留。每页有限，不能一次性载入整个持久清单；owner、cursor 与 ID 均不替代每次请求的业务授权。

SQLite 在删除义务事务与见证提交中保存 owner、序号和每 volume 不可回退高水位；旧 v8 记录迁移为以原 intent ID 为 owner，使持有旧 ID 的调用方仍可核对。owner 长度 1 至 128 字节，有效 UTF-8 且无 NUL；游标不超过 `MaxInt64`，单页最多 256 条。HTTP 的 list、query、ACK 分别按独立 `storage.Operation` 授权，授权发生在读取持久状态或动作历史之前。limited、locked、objectstore、localstore 与 replicated 包装层保持相同 owner/cursor 语义；replica 不保存责任账本。

## 备选方案

**把任何 Close 错误解释为引用仍存活。** 删除义务可能已经完成且引用已经释放；取消或丢失响应也不证明 authority 未执行。该解释会允许以新动作再次触发关闭，却没有原引用可核对。

**先解除 Use，再完成可能失败的最终化。** 最终化失败时引用仍占 pin 和容量，但冲突新打开已不受原共享限制。把 claim 解除留到最终化之后，并严格核对原 claim，使确定未释放与可观察共享保护一致。

**只为调用方保存一个关闭 action ID。** 第一次尝试确定失败后，相同 ID 的重投必须返回相同失败，无法再执行一次关闭；若直接更换 ID，又会让响应丢失时产生重复效果。稳定 owner 配合递增 generation 明确区分可继续核对的原尝试与获准的新尝试。

**只靠调用方保存每个 DeleteIntentID。** authority 接受义务与调用方持久记录之间存在故障窗口。即使单个 ID 查询耐重启，也不能找回从未成功记录的 ID。

**在中立关闭变更中接入 SMB 文件命令。** 这会把通用关闭顺序、HTTP 回执与 Windows FileId、名字及状态映射绑在同一审查范围；SMB 端点使用已验证的中立能力即可单独实施。

## 后果

调用方能区分已释放、确定仍持有和未知三种关闭事实。确定失败时，原共享声明和 pin 继续阻止冲突新打开；新尝试仅在同一 owner 下前进。未知时保留原 ID／generation；同一尝试可重进可恢复的 native close，但不能重复权威效果，也不把传输失败或回执过期说成无效果。已释放但 barrier 待结算时只核对同一动作，语义错误和释放事实不会被 barrier 错误抹去。代价是每个引用及会话预留关闭回执容量，终态失败的历史记录暂占额度，更多失败尝试可能需要等待回执到期；authority 崩溃导致旧易失引用失效时，无法把未知旧动作凭新 session 伪装为已确认。

调用方还可在丢失单个 intent ID 后凭持久 owner 找回未 ACK 的删除义务。每条义务增加 owner 和持久序号，迁移和高水位与见证、容量、完整性检查一起维护；遗失 owner 后不能从未知身份枚举全 volume 义务。ACK 后没有 tombstone，调用方不得复用已确认 ID；序号高水位不会因 ACK 回退。HTTP 回执过期后，已释放引用的清理失败进入有界汇总，只保存累计次数及首个和近期错误样本供 `Handler.Close` 报告，不能从汇总还原已过期动作结果。

本决定接续[持久节点身份与原子文件操作](2026-09-20-durable-identity-and-atomic-file-operations.md)的 durable delete intent、[活跃文件句柄](2026-09-08-live-file-handles.md)的关闭所有权和[业务授权](../feature/2026-09-10-host-provided-authorization.md)的逐请求准入。它不改变对象绑定、原子打开或已接受效果语义。当前结构见[文件句柄设计](../../../../docs/design/server/file-handles.md)，验证边界见[测试策略](../../../../docs/testing.md)；SMB 端点的使用方式由[有界 CREATE/CLOSE 提案](../../proposed/feature/2026-09-28-smb-bounded-create-close.md)说明。
