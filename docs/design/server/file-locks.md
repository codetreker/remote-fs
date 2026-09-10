# 文件占有与排他修改

文件锁保护现有普通文件的逻辑身份。稳定权限 `S` 允许多个持有者并存，排他权限 `X` 允许一个持有者修改；稳定权限本身不能用于修改。服务端对显式携带授权的修改与匿名修改执行同一套冲突检查。

本文描述锁机制。普通文件的内容版本前置条件与 FUSE `Open` 自动取得哪种权限是独立事项；锁没有替调用方选择这两项策略。保证由 [R-CC-3、R-CC-6 至 R-CC-11](../../spec/requirements.md)定义，取舍由[实现决定](../../../.agents/notes/implemented/architecture/2026-09-07-file-locks.md)记录。

## 组件与所有权

| 组件 | 责任 |
|---|---|
| `packages/locking` | 公共类型、错误分类、授权方状态、会话和持有者、有界动作历史，以及发布排序；只依赖标准库 |
| `packages/storage/locked` | 把原生支持发布检查的 backend 与它的授权方配成一个 volume；提供匿名操作及显式 mutation scope |
| 原生 storage / metastore | 在实际 volume 变更中确定受影响文件，并在最终发布边界接受授权检查 |
| HTTP handler / client | 传递控制动作、结果与 mutation proof；管理请求的资源预算独立于普通数据请求 |
| 独立 server | 配置私有持久证据、取得 volume 的独占写入所有权，并管理恢复与关闭 |

handler 接受的是已配对的 volume 与锁服务。把一个锁服务与另一份未经约束的 raw storage 并列，不能形成可发布的服务端。第三方 backend 必须提供原生发布集成；包装层不能靠一次路径 `Stat` 加一次独立 `Write` 模拟这项能力。

## 身份与显式 scope

Session、Owner、Resource、Grant 与 Request 各有独立身份。Owner 属于一个 Session；资源身份指向该授权方认识的一个 backend 文件；Grant 指向一次授予；Request 指向同一持有者的一次管理意图。身份不复用，旧授权方的身份不能指向新授权方里的对象。

Session、Owner 与 Grant 是不可伪造的 bearer capability，使用至少 128 bit 的密码学不可预测性，或认证其授权方及父级绑定的 MAC；单调编号本身不能充当权限。验证同时检查 Session / Owner / Grant 的归属关系。能力值不进入错误、日志、全局状态或 URL。部署方控制 enrollment 入口的访问，这套所有权证明不引入身份提供者。

`GrantRef` 包含授予身份、资源身份与 generation。它不表示文件内容版本，也不表示 change-log position。续期 revision 与剩余保护间隔描述授予当前的状态，不能用作内容比较前置条件。

`Scope(MutationScope)` 构造一份有界、不可随调用方后续修改而变化的 proof 集合，只向修改操作携带 Owner 与明确给出的 GrantRef。`Read`、`Stat`、`List` 仍是普通读取，成功不证明某项 grant 仍然有效；需要了解占有状态的调用方使用控制查询。

只有现有普通文件可以被 Resolve 和 Acquire。目录、子树与不存在的目录项作为锁目标时明确拒绝。ResourceRef 有明确的有限有效期，过期后拒绝使用，不转而绑定新文件。Resolve 只是发现，Acquire 在授予转换处重新确认同一个资源仍存在且类型受支持。

文件改名后，授权仍约束原文件的逻辑身份；同名替换不会使授权转移到新节点。目录改名不会把后代的文件授权提升为路径锁或子树锁。

## 管理操作与结果

| 操作 | 行为 |
|---|---|
| `BeginEnrollment` | 发出短期、带授权方身份与有效期的认证 ticket；不建立 Session 或保留动作记录 |
| `OpenSession` | 原子消费 ticket；重复消费返回同一 Session 身份 |
| `CreateOwner` | 在 Session 中创建持有者；同一 Request 的重复请求回放原结果 |
| `RetireOwner` / `CloseSession` | 终止持有者或会话，取消等待并解除其授权；资源已满时仍可执行 |
| `Resolve` | 把普通文件路径解析成当前授权方的逻辑资源引用 |
| `Acquire` | 以明确的模式、有限 TTL 与最大等待间隔申请占有 |
| `Renew` | 以新的管理 Request 明确延长现有 grant 的保护期限 |
| `Release` | 解除 GrantRef 指定的授予；重复调用不产生另一动作 |
| `Cancel` | 原子地阻止尚未获准的 Acquire，或解除由该 Acquire 已经取得的 grant |
| `QueryAction` / `QueryGrant` | 分别核对原动作结果与 grant 的当前生命周期 |
| `Status` | 通过独立的 `StatusService` 查询安全的计数、恢复与不可用状态；不需要 enrollment，缺少能力时明确报告不可用 |

Acquire 的结果为 `Pending`、`Granted`、`Cancelled`、`TimedOut` 或带分类的 `Rejected`；Renew 成功为 `Renewed`。Pending 尚未决定最终结果，只能前进到一个终态。立即冲突、AlreadyHeld、排队目标消失及已接纳的 Renew 失败都保留为原动作结果，条件后来改变也不会重新执行。授予当前状态可从 `Active` 前进到 `Released`、`Expired`、`TargetGone` 或 `OwnerRetired`；Query 可以同时报告原 Acquire 已获准与该 grant 现已到期。

响应以授权方单调时钟表达期限与当前状态。SDK 的本地有效期只是保守提示，不能代替服务端最终权限判定；编码与取整规则见下文「HTTP v3 编码」。

控制错误区分 `Invalid`、`UnsupportedTarget`、`Conflict`、`AlreadyHeld`、`RequestMismatch`、`Capacity`、`Retired`、`OutcomeUnknown`、`StaleResource`、`StaleGrant`、`UnrelatedProof`、`Recovering` 与 `Unavailable`。输入错误映射 `EINVAL`，不支持的目标为 `EOPNOTSUPP`，占有冲突为 `EBUSY`，容量或恢复中为 `EAGAIN`，退役、失效与 proof 相关性失败为 `ESTALE`，未知或不可用为 `EIO`。管理响应仍保留 typed code 与是否已记录的区别，不能从 errno 反推生命周期。响应丢失后核对原授权方、持有者与 Request，不能在新的授权方里悄悄创建另一份意图。

## 有界历史与终止

Enrollment ticket 的认证信息包含授权方与单调生存期内的有效期。消费结果与全部活跃 Session 各有独立的全局容量；只有成功预留两者的位置之后才创建 Session。同一 ticket 的消费结果保留到 ticket 到期，包括 Session 已经关闭的情况。过期 ticket 不能被当作新的建会话请求。

Owner、Resource、Action、活跃 grant 与排队动作分别有全局容量；每个 Session 对持有者数量与 `CreateOwner` 历史再设上限，每个 Owner 对动作历史、当前 grant 与排队申请也分别设限。Acquire 和 Renew 在改变任何状态前先预留一条完整意图记录；相同 Request 与相同意图回放结果，语义字段不同则拒绝。活跃持有者的历史不独立淘汰。容量满时返回未接纳这一次投递，不创建动作 receipt；这不是延迟到达的同意图副本永远不会获准的证明。

Release 以不可复用的 GrantRef 定位自己的结果，已知 Acquire 的 Cancel 使用原记录，均不需要新历史名额。Cancel 与授予在同一状态转换中排序：取消等待，或解除该申请已经取得的授权；此后重复 Acquire 不能恢复它。对尚未见到的 Request，Cancel 可以预留一条有界的 Cancelled 记录来阻止迟到申请，其 Acquire 意图保持缺席，迟到 payload 不改写原 receipt；无容量时返回 OutcomeUnknown，不能确认取消。Query 尚未见到的意图同样是 OutcomeUnknown。终止 Owner 或 Session 在容量已满时仍可执行，取消等待、释放授权并阻止此后迟到的动作。

Session 有有限的 idle 期限，不短于最大 lease 与最大排队间隔；经验证的控制活动可以延长当前期限；发生延长时，成功响应或带已记录动作结果的拒绝响应返回更新后的 history expiry tick。历史有效期随活跃 Owner / Session 维持，不靠淘汰单条记录腾出容量。会话不能在已经确认的 grant 保护期结束之前被忘记；明确终止会话则是持有者主动解除保护。长时间使用的 Owner 可能耗尽动作容量而不能再次 Renew，原保护期限保持不变，调用方需要明确结束并重新建立有容量的持有者。

Renew 使用 `max(原 deadline, 转换时刻 + 请求 TTL)`，不缩短已确认期限。提高持久时长高水位是转换前的准备；I/O 之后重新检查 Owner、grant、generation 与期限，准备期间已经到期的 grant 不会复活。并发提高被串行化，较小的记录不能覆盖已完成的较大值。

一份 Acquire 的排队意图最多存活其声明的有限 Wait。立即授予、拒绝或完成排队登记后，HTTP 请求就返回；Pending 不占用长期 HTTP 控制名额。请求结束与动作终止是不同事件，丢失响应不会自动产生另一个申请，也不自动取消原申请。调用方用原 Request 执行 Cancel 或 Query，SDK 不自动轮询或启动等待 goroutine。一名 Owner 在一个资源上至多持有一个当前 grant，另一个 Acquire 返回 `AlreadyHeld`；没有隐式升级、降级或递归计数。

排队记录移除时同时清空 backing slice 不再使用的引用，队列不能在逻辑长度之外继续固定已经结束的动作与 Owner；仍有效的动作历史按自身规则保留。授权方被隔离后不再启动或重新进入资源 worker，已有 worker 退出；维护循环继续按尚存 Pending 的各自 Wait 期限清理排队状态。停止 worker 不虚构新的授予或拒绝，也不让有界等待变成无限保留；对外查询继续遵守既有不可用错误语义。

## 修改覆盖与冲突

`S` 与 `S` 相容；`X` 与其他 Owner 的任何 grant 不相容。Owner 自己持有的 `S` 也不是修改许可。普通快照读取不受 `X` 访问控制；显式 `SetAttr` 修改支持的 mode、访问时间或修改时间属于受保护的修改。普通读取可能产生的平台 atime 副作用不构成稳定 atime 的承诺；`S` 的稳定保证覆盖内容、存在性与逻辑身份。

| 修改 | 实际受影响的普通文件 |
|---|---|
| `Write`、`SetAttr`、File `WriteAt` / `Truncate` / `SetAttr`、带截断的 Open | 实际目标文件；保留引用按节点身份解析 |
| `Remove` | 被删除的文件 |
| `Rename` | 源文件，以及目的地被替换的现有文件 |
| 目录创建、删除或改名 | 不能借此绕过实际受影响普通文件的保护；不把整个子树当作文件资源 |

匿名修改只在不冲突于当前 grant 时允许。显式 scope 中的每一份 proof 必须属于当前授权方、Session、Owner 与 generation，仍在有效期内，并且与这次修改的实际文件集合相交。实际受保护的每个文件都必须由调用方有效的 `X` 覆盖。任何过期、失效或无关 proof 都使操作失败，不能退回匿名执行；空 `SetAttr` 与 self-Rename 也验证提供的 scope。

## 发布与观察的排序

上传、暂存和最终发布是分开的阶段。对象存储的 Reserve 与不可变对象 Put 不持有文件发布许可；SQLite 在修改 entries/nodes 的事务内解析实际目标。删除、改名与属性修改进入同一原生发布检查入口；`limited` 将 scope 和发布能力传到下层。第三方 backend 也必须在实际最终转换中确定资源与效果，不能由包装层提前推断。

暂存完成且实际受影响资源已经确定后，最终转换在后端的观察排序门内检查权限、proof 相关性、冲突与单调期限，并在该位置取得授权顺序。过期后尚未取得许可的修改失败，即使没有继任持有者。文件资源解析与最终变更不能分成一次先验 `Stat` 和之后不受约束的操作。

已取得顺序的不可分割转换可在不可中断的内核或 SQL 操作中稍后完成；相冲突的新授权、解除与新的权威视图捕获必须等待它的确定结果。之前已捕获的视图可以完成读取。观察门在捕获视图之后、批量内容读取之前释放，授权方也不因排队申请或暂存上传而持有跨资源的锁。SQLite 原有 writer / health 串行化仍承担其原生事务顺序。

`Publish` 只调用一次原生最终转换回调，回调不能含上传或任意调用方函数。返回结果明确区分效果是否已知、哪些资源退休、是否发生修改及原始错误。已知效果先更新资源绑定，再开放观察与授权；已知失败保留未发生效果的映射，部分 `SetAttr` 仍保留其真实结果。volume 效果不明，或发布计费的结算、撤销结果不明时，隔离授权方与 volume，直到显式恢复。后者即使已知文件没有改变也成立：保留正确的资源绑定不能证明配额账本仍可使用。`IsPublicationAccountingUncertain` 区分这种失败与准备失败但成功撤销的普通错误，并保留主错误与清理错误。无条件 deferred completion 不能用过时映射重新开放准入。

授予使用同一原生 live-target guard：先在授权状态 mutex 之外发现资源，取得后端目标排序后重新验证存在性、身份与类型，再进入授权状态转换。顺序是 backend 在前、authority mutex 在后；暂存、高水位持久准备与等待其他资源都不持有 authority mutex，回调与生命周期操作也不能反向重入。

SQLite 的新视图捕获包括开始只读事务、钉住 snapshot，以及在其中取得 node / object key，之后释放观察准入再读取大块对象或产出快照页。内容读取、调用方计费回调与编码在捕获之后执行。其它 backend 同样须区分固定视图与随后读取；一个可变化的目录 FD 不构成不可变列表。异步副本与内核缓存继续遵守各自的已有契约，不因锁控制而获得新的线性一致性保证。

该许可不是一段新 lease，不在上传之前取得，也不把任意长的准备工作算作尚未到期的授权。保留文件还检查 FileSession 与引用是否有效；内容 revision CAS、显式内容版本前置条件仍各自独立于权限检查。

## Backend 资源身份

SQLite-backed volume 使用 volume 内的节点身份作为原生资源键。覆写内容保留节点身份，改名移动该节点，删除后同名创建得到另一个节点。经授权的 unlink 或覆盖令旧的命名资源退休为 `TargetGone`；已经打开的 File 与标准 advisory 随 detached 对象保留，强 S/X 不转移到同名新对象。对外 ResourceID 同时区分授权方，不与内容修订、grant generation 或日志位置互换。

授权方资源映射受上限约束。有效 ResourceRef、排队动作、活跃 grant 或发布需要目标时，backend 保留相应的原生引用；终态历史可以只保留退休 ResourceID。退休身份不重新分配给别的文件。第三方 backend 须保持这一映射与实际文件一致，不能直接使用可能被复用的宿主 inode 号充当稳定身份。绕开本系统直接修改其私有存储仍是规格中的非目标。

## 重启与持久证据

持久证据记录此前已经发放过的最大 lease 时长，由状态记录中的 Accepted / Prepared 与绑定 volume 的独立 Witness 共同证明；UUID 相同不能单独证明没有回退。提高过程在持久 raise mutex 下串行执行：保存并同步下一代 Prepared，推进并同步 Witness，最后令 Accepted 等于 Prepared 并清空 Prepared，确认完成之后才能向调用方承诺更长期限。Prepared 的 generation 必须恰好是下一代，时长不减，volume / state 身份一致。保存结果不明时不能继续授予依赖该提高的期限。

| 打开时的状态 | 处理 |
|---|---|
| Accepted=A，Witness=A，无 Prepared | 接受同一已确认状态 |
| Accepted=A，Witness=A，Prepared=P | 验证 P 后可保守完成提高 |
| Accepted=A，Witness=P，Prepared=P | 完成 Accepted=P 的发布 |
| 其它不一致、必要证据缺失或损坏 | 拒绝打开 |

Witness 不降低，时长也不按当前配置截短。这两份证据检测任一单独组件的降低或旧状态回放；所有独立证据被一起进行一致的管理员回滚，不在没有外部可信锚的检测承诺内。

新的授权方先取得数据库的独占写入所有权，再读取并校验该证据。恢复以实际取得数据库排他 flock 的时刻为起点，配置从原生拥有者取得该时刻，不采信任意调用方时间戳。使用新的单调时钟等待完整的已记录时长期间，修改与授予被拒绝，普通快照读取及恢复状态查询保持可用。旧进程已由生命周期所有权隔离，旧上传不能借新授权方发布 volume。这个等待不依赖跨重启的墙钟连续性。

恢复之后使用新的授权方身份。旧意图、Owner 与 GrantRef 明确返回 `Retired` 或 `OutcomeUnknown`，不会被解释为未曾授予，也不会重新执行。精确动作回放只在原授权方及其活跃 Owner / Session 的历史窗口内成立；剩余保护通过恢复屏障保留，动作历史不逐条落盘。

首次初始化有独立的持久 intent / binding 阶段，只有匹配的已记录初始化可以继续；Open 缺少状态不能被当作新 volume。SQLite migration `0004` 的 `lease_recovery` 保存 DatabaseID、StateID 与 Accepted / Prepared；本地持久组合的两次事务还经过原有数据库提交见证，独立 lease Witness 则在中间推进。

`sqlite.OpenLocking` 拥有整份数据库的原生 flock 与进程内独占 coordinator。数据库 inode 的 `user.remote-fs.lease-state` xattr 与相邻的 `.<数据库文件名>.leases.intent`、`.witness` 绑定数据库身份与规范化的证据目录；Accepted / Prepared 与 Witness 的最大 lease 时长覆盖整份数据库。运行时选择的 volume 不成为永久 anchor identity。后续进程可以选择同一数据库中的另一个已有 volume，但须取得全数据库所有权并完成数据库级恢复等待；多份活跃 server 不能同时共享它。真正未绑定的 raw SQLite API 仍有独立的库用途，已经绑定的库不能靠关闭配置或 raw API 绕过保护。

raw SQLite opener 也先取得同一个原生数据库文件的共享 flock，锁服务 constructor 取得排他 flock。既存 raw handle 仍在时，接管以 `EBUSY` 失败；授权方存活时，新的 raw opener 同样失败。这个互斥覆盖同进程与跨进程，不允许两类写入口并存。ConfigureLeaseRecovery 与 EnableLocks 验证实际的排他拥有者和 native anchor，不能靠注入任意持久化对象把未受保护的 Store 变成授权方。普通 raw 路径可经过符号链接，但检查针对同一底层 inode；数据库路径中的 `%`、`?`、`#` 与 NUL 明确拒绝，避免 native 绑定检查与 SQLite file URI 打开的文件不一致。

本地持久组合沿用其私有根的 lifetime ownership，lease 证据为 `.leases.intent` 与 `.leases.witness`，根 inode 带同名 xattr 绑定。intent 的 READY 状态与独立 checksummed witness 都必须有效；初始化只恢复匹配的持久 intent，缺少 READY 证据不创建新身份。证据目录迁移需要显式迁移过程，不能仅修改路径配置。SQLite 的锁拥有者只有在数据库成功关闭后才释放原生所有权；最终原生 FD 的 Close 失败只尝试一次并缓存结果。已启用锁且未配置数据库提交见证的直接 SQLite Store，也在退出授权方并排空 commit gate 后重新检查 coordinator poison；这一期间发生的清理提交失败保留原原因、缓存关闭错误并继续持有排他所有权，不能因 pool 已关闭就报告干净关闭。

服务前验证本地 xattr、flock、同 mount 改名、文件与目录 fsync 能力，不支持的配置明确失败。缺失、损坏、替换或归属不匹配的绑定与 READY 状态不触发自动初始化。证据和对象的暂存属于各自私有存储格式，不能出现在 volume 中。

## HTTP v3 编码

所有端点使用 `/v3/`，每个响应都有 `Remote-Fs-Protocol: 3` 与 `Cache-Control: no-store`。基础 volume 操作名、octet write body 与 mutation barrier 形状保留；v2 不被 scoped client 接受。

### 控制端点

控制请求都是有界的 JSON POST。能力不放入 URL；表中对象使用公共 locking 类型的 JSON 字段，未知或重复成员无效，所有列出的字段均须存在。

| `/v3/` 下的端点 | 请求体 | 成功响应 |
|---|---|---|
| `session-enrollment` | `{}` | `{"ticket": EnrollmentTicket}` |
| `session-open` | `{"ticket": EnrollmentTicket}` | `{"session": Session}` |
| `session-close` | `{"session": SessionID}` | `{}` |
| `owner-create` | `{"session": SessionID, "request": RequestID}` | `{"owner": Owner}` |
| `owner-retire` | `{"owner": OwnerRef}` | `{}` |
| `lock-resolve` | `{"owner": OwnerRef, "path": Bytes}` | `{"resource": ResourceRef}` |
| `lock-acquire` | Acquire DTO | `{"action": ActionResult}` |
| `lock-renew` | Renew DTO | `{"action": ActionResult}` |
| `lock-release` | `{"owner": OwnerRef, "grant": GrantRef}` | `{"release": ReleaseResult}` |
| `lock-cancel` | `{"owner": OwnerRef, "request": RequestID}` | `{"cancel": CancelResult}` |
| `lock-query-action` | `{"owner": OwnerRef, "request": RequestID}` | `{"action": ActionResult}` |
| `lock-query-grant` | `{"owner": OwnerRef, "grant": GrantRef}` | `{"grant": GrantStatus}` |
| `lock-status` | `{}` | `{"status": Status}` |

`Bytes` 是 `[]byte` 的标准 JSON base64，保留非 UTF-8 文件名。Acquire DTO 的必需字段为 `owner`、`request`、`resource`、`mode`、`ttlMillis`、`waitMillis`；Renew DTO 为 `owner`、`request`、`grant`、`ttlMillis`。receipt 内嵌意图使用相同 DTO。

TTL 必须是正的整毫秒，Wait 是非负整毫秒；转换为 `time.Duration` 前检查存在性、整数类型、乘法范围及服务配置的更小上限。SDK 的亚毫秒输入明确失败，不自动取整。

### Mutation scope

`Remote-Fs-Mutation-Scope` 只有一个 header 值，内容是 UTF-8 JSON 的无 padding base64url：

```json
{"owner":{"session":"SESSION","owner":"OWNER"},"grants":[{"id":"GRANT","resource":"RESOURCE","generation":1}]}
```

示例中的身份是占位符。缺少 header 表示通过同一授权方匿名执行；存在时必须有完整非空 OwnerRef 与有界数组。空数组不提供 grant 权限。空值、重复 header、非法编码、重复或未知 JSON 成员、缺字段、非法身份与超长输入在修改前拒绝。

这个 header 只用于 `setattr`、`write`、`create`、`mkdir`、`remove`、`removedir`、`rename`；普通读取与控制端点收到它都是非法交换。scoped client 的所有修改，包括 WithBarrier 方法，都携带独立复制的 scope；读取与有界读取都省略它。空 SetAttr 和 self-Rename 仍保留并验证所给 scope。header 解析成功不授予最终发布权限。

scope JSON 固定至多 16 KiB、16 份 proof、每个 opaque capability 至多 512 字节，编码后的上限在 base64 解码前检查。授权方可以配置更紧的限制；各字段分别达标不表示组合后的 JSON 一定装得下。客户端在复制保留 scope 或编码请求前完成相同验证。

### 结果与期限

ActionResult 的 `recorded` 表示存在原动作记录，receipt 保存 kind、request、原意图与确定的 outcome；`grant` 是独立的当前 GrantStatus。GrantStatus 同时携带 ref、mode、state、revision、deadlineMillis、nowMillis、remainingMillis 与 historyExpiresMillis。Session、Owner、ReleaseResult 与 CancelResult 也分别携带历史窗口及当前 tick；这些字段的缺席不能当作零。原 receipt 的 deadline 与 revision 不被新状态覆盖。

ResourceRef 的必需字段为 `id`、`expiresMillis`、`nowMillis`、`historyExpiresMillis`。成功 Resolve 延长 Owner 所属 Session 的活跃历史窗口，直接 API 的 `HistoryExpiresMillis` 与 wire 字段同时报告延长后的期限。资源引用的 expiresMillis 与动作核对的 historyExpiresMillis 分别计时，不能互相替代；Acquire 请求及 receipt 内的 ResourceRef 保留同一组完整字段，重放原意图不改写其中的旧观测。

`remainingMillis` 按未取整的单调时刻计算 `floor(max(deadline-now, 0) / 1ms)`。分别取整两个绝对 tick 再相减会夸大保护：deadline 为 1.1ms、now 为 0.9ms 时，结果必须是 0。终态为 0，尚余不足一毫秒的活跃 grant 也不提供可用的本地保护提示。

SDK 以产生这份 GrantStatus 的请求首次发送时刻加 `remainingMillis` 建立保守提示，同一请求的内部重试不能重置这个起点。资源与历史窗口只有绝对整毫秒字段，提示使用 `max(expiresMillis-nowMillis-1, 0)` 的余量并检查算术溢出。提示不能代替服务器检查；回放的旧 receipt 也不能重新开始一段 lease。Acquire 的重复请求保留原 ResourceRef 全部字段，新 Resolve 意味着新的明确意图。

锁权威错误使用 `422` 与 `errno`、`message`、`lockCode`、`recorded`；已记录的 Acquire / Renew 拒绝，以及 QueryAction 返回该拒绝时，还必须带匹配的 action。SDK 同时返回可核对的 ActionResult 和 typed error。未接纳的容量拒绝没有 action；未知 code、errno 不一致或结果与意图不符都是协议失败，不能造出一份零值 receipt。非锁错误继续使用既有 errno/message 形状。

### 控制准入与取消

控制请求与响应的协议上限都是 16 KiB，client 与 server 分别使用独立于数据请求和复制流的 operation / waiter admission。`HandlerOptions` 与 `DialOptions` 的 `MaxConcurrentLockControls`、`MaxWaitingLockControls` 默认各为 16；零值选择默认，负数或大于 65536 的值拒绝，零不表示关闭队列。每个活跃控制操作预留四份 16 KiB，覆盖保留的 wire 与严格 DTO 解码，aggregate 由并发上限推导。Acquire 登记 Pending 后立即释放 HTTP 名额，Wait 约束远端意图的生存期。

控制端点虽统一使用 POST，Resolve、QueryAction、QueryGrant 与 Status 仍是只读操作，已发出的请求可以接受取消。其余操作进入 HTTP dispatch 后，响应丢失、截断或取消只能说明结果未知；原请求身份与错误原因保留，调用方必须核对或终止该意图。dispatch 前接受的取消遵守普通 `EINTR` 分类。SDK 不在失败后制造新的 Session、Owner 或 Request 重做管理动作。

## 集成与生命周期

普通 `flock` 与传统 POSIX `fcntl` 是[保留文件接口](file-handles.md)的 advisory 操作，不创建强 S/X Owner 或 grant。它们允许未参与加锁者执行普通修改，阻塞等待按 FileSession 的健康续期维持，不采用这里 Acquire 的有限 Wait。普通 Open 不自动选择任何加锁策略。

独立 server 的 Azure Blob 与本地持久对象存储两种形态都建立配对的 enforcing volume 与锁服务，向 HTTP v3 同时发布数据操作、锁管理操作和显式 mutation scope。协议不通过忽略未知 proof、旧授权方身份或非法 scope 保持兼容；无法识别的结果保持错误。

控制 admission、Session、Owner、grant、等待申请、动作历史及本地资源映射分别有界。授权方动作历史满额时，Release、已知 Acquire 的 Cancel 与 Owner / Session 终止仍有执行路径；控制请求本身继续服从独立的 HTTP admission。TCP 断开不解除已经确认的占有，显式生命周期结束与有限 lease / idle 到期负责释放。

FUSE 继续按已有 `Open`、`Flush`、`Fsync` 规则工作，不在打开时自动取得权限。集成方显式申请并为修改构造 scope。锁机制提供权限与发布排序，不补齐普通写入的内容版本比较、不改变已知的页缓存与缓冲长度缺口，也不自动恢复未保存的本地写入。
