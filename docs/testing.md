# 测试策略

本仓库怎么测：分层，以及那些让「全绿」有意义的规则。命令在根 [AGENTS.md](../AGENTS.md)；理由在对应的 Agent Note 里。

## 分层

- **契约** —— `storage` 接口的义务写成一套可执行的用例，住在任何实现之外。普通目录、本地持久对象存储、HTTP 另一端与其它实现都跑同一批用例。**每一种实现通过同一套用例，是「这个接口是一层抽象、而不是对某一份实现的描述」的唯一证据。** 新的 storage 义务加进这套用例，而不是加在某个实现旁边 —— 只在一处验证过的义务，其它实现不会知道它存在。server backend 另跑 `storage.BoundedStorage` 契约：依赖在服务前可被校验，Read 在完整 payload 分配前拒绝超限，List 在保留越界 entry 前拒绝，且取消会停止产生结果。`objectstore.Objects` 与 `metastore.Store` 各有自己独立的契约套件；metastore 套件还验证 bounded `Incarnation` 在复制 identity 前拒绝超限、`Barrier` 原子返回同一 identity/committed position，并验证 `Since`/`Next` 在载入变长 payload 前预算、页满时不跳过下一项、单项超限与 production error 使整页不可读取。组合层再验证跨接口的顺序与错误保存。
- **单元** —— 核心逻辑：路径清洗与越界拒绝、errno 映射、打开文件的那份缓冲区、FUSE 单文件上限、HTTP request/response 的单体、operation、waiter 与 aggregate byte 上限、stream 的单帧预算、derived event/cursor products 与 snapshot-page admission、subscription 与 snapshot 上限、replica mutation confirmation 的 active 与 waiter 上限、quota measurement 的单目录与 frontier byte 上限、local-disk object active/waiting/byte 上限、SQLite reader-connection 与 integrity-record/name-byte 上限、持久 ID 高水位、日志 predecessor chain、WAL 见证与 checkpoint 状态、消息编解码与 URL 往返。**不需要挂载点、不需要 `/dev/fuse`、不需要特权** —— 一份本地目录 storage，或者直接构造的内部结构，就足以驱动它们。每项上限分别验证边界值、超限错误、并发 admission 与取消，不能用一项恰好更紧的上限代替另一项的测试；write 上限低于 protocol body 时，超限 write 必须失败，而落在 protocol body 内的 listing 仍须成功。request 与 response admission 分别占满后，用例断言有界等待、超额等待者的 `EAGAIN`、context cancellation 与释放后的恢复。response 用例同时覆盖 server 侧固定结果的 operation/waiter 占用、队列中的 `Write` 不读 request body、client 在解码或验证 mutation barrier 前不释放 reservation，以及 stream setup 的 error body 也必须经过同一 client admission。frame 用例分别覆盖 client/server 不同但相容的上限、超限 change 与 snapshot row、oversized pre-stream identity、tail 已前进却返回空 bounded page、subscription/frame 与 snapshots/frame 的 checked products、snapshot-page aggregate 与 waiter 饱和、取消释放，以及不可能成立的 option 组合；隔离用例占住 snapshot admission 后仍要观察 change frame 到达，证明 bulk transfer 的 gate 没有被放到 subscription 路径上。另一条集成用例让 snapshot producer 等待超过 client silence bound，断言 keepalive 保持 stream 可用，释放 admission 后 rows 继续到达。subscription 用例占满名额后断言新 stream 以 `EAGAIN` 拒绝，并在 client 关闭和 `Handler.Stop` 后恢复；另一条用例让 stream 从 opening tail 之后继续收到 live change，断言 `Position` 前进后 `CaughtUp` 仍为 true。mutation confirmation 用例断言 fixed-size admission 发生在 request 发出前；纯取消为 `EINTR`、deadline 为 `EIO`，满 waiter queue 或 storage 关闭为 `EAGAIN`，这些路径均保留原因且不会发送 mutation；另覆盖 event/response 两种先后顺序、并发 writer 造成的 later barrier、barrier incarnation/generation mismatch、grace、stream/request failure 与关闭时释放 active count。quota measurement 用例分别卡住一份目录的 `Entry` 加名字和 active/pending 完整路径组成的 frontier，验证精确边界、超限时不保留越界项、从不调用 ordinary `List`、取消，以及 recount 失败后保留原计数并释放 gate。SQLite reader 用例占满小型连接池，断言后续读取等待、取消后退出，并在释放一个 snapshot 后恢复 admission；integrity-record/name-byte 用例分别验证含 mandatory log row 的空 namespace 最小值、精确边界、跨 namespace label/parent/child 与 object/log/change 计费、超限 `EFBIG`、取消后 store 仍可用，以及 pre-open validation。偏重边界情况、错误路径、并发交错，以及回归的永久用例。**一个关于 errno 映射的测试若需要挂载点，说明有个边界划错了。**
- **对拍** —— 把 `fuse` 挂在本地目录实现上（不经网络），对挂载点与一个普通目录施加同一串操作，比较每一步观察到的结果、错误码，以及之后两棵树的路径、模式、大小与内容。不一致即缺陷 —— 不需要预先枚举「应该是什么样」，普通目录就是答案。适合随机化操作序列，因为它不需要预期值。刻意的偏差必须是 [`spec/requirements.md`](spec/requirements.md) 里明确列出的非目标，按名字跳过并注明是哪一条。
- **故障注入** —— 在被测那一层自己的下游接口上制造麻烦，下游有几个接口就注入几个。`storage` 接口上是一个专门制造麻烦的包装层：够不到的命名空间、提交失败、报不出类型的节点、以及在一次操作已经部分成功之后才失败。`objectstore.Objects` 的 `Get`、`Put`、`Delete`、`Available` 与 `Close` 都分别注入错误；能力拒绝只接受纯 `ENOSYS`，与其它错误 join 在一起时不能遮掉 measurement failure。组合层还要验证关闭顺序、两个 durable half 的错误同时保留。local-disk 实现在 `fsync`、`linkat`、`unlinkat` 与容量查询处注入错误，覆盖 shard identity/object publication 已发生但 durability 无法证明的状态；localstore 在 witness publication、WAL 缺失与 checkpoint 处注入，验证确认失败 poison、可重试 checkpoint 与锁保留。传输层覆盖拒绝不能产生有界结果的 backend，body 截断、request/response 单体与 admission 上限、等待队列溢出、bodyless 操作不保留意外 body、metastore 在 bounded page 中途失败或给出过大 payload、mutation 已提交后 `Log.Barrier` 失败、缺席/null/畸形 barrier、缺失答案、错误 framing，以及回答者根本不是本协议的情况。
- **端到端** —— 整条链路一起跑：一个服务端、两个各自挂载的客户端，验证一边写入另一边一秒内可见、以及服务端消失时每个操作都报错。两台机器在这里由两个挂载点代替，缺的只有主机之间的网络。交付出去的两个二进制也在这一层：旗标、诊断、退出码，以及 Ctrl-C 之后挂载点确实消失。

对拍必须跑在**交付出去的那套配置**上。为了让它好过而调松的任何一处 —— 内核超时、提交时机、单文件上限 —— 都会让它去验证一条生产中不存在的路径。

普通对拍中的六处时间设置调用使用 [`comparisonChtimes`](../packages/fuse/fuse_test.go)：每处 `os.Chtimes` 的总尝试次数至多八次，仅在上一次返回 `EINTR` 时重做完全相同的路径、绝对 atime 与 mtime，包括明确省略某个时间的参数。挂载点和普通目录使用同一规则，只重试这一次调用；其它错误立即返回，八次仍中断则保留最后的 `EINTR` 并使对拍失败。这里比较最终的 atime / mtime，ctime 不在对拍结果中。专门验证中断的真实信号用例继续断言第一次系统调用的结果，不使用这个辅助函数。

## 覆盖率是必要的，从来不是充分的

它只证明那些行跑过了，不证明特性按交付的样子工作。

覆盖率文件本身也须有实际执行证据：mode 头之外有覆盖块、非零执行计数与真实函数记录。`assert-every-test-ran.sh` 当前会把显式 `-coverprofile` 再交给清单调用，可能将已执行的 profile 覆盖为仅有 mode 头的文件；[保留执行覆盖率的提案](../.agents/notes/proposed/bug-fix/2026-09-08-preserve-executed-coverage-in-strict-test-runs.md)单独处理这一缺陷。脚本退出成功或日志中的百分比不能替代对最终文件的核对。

**未覆盖的行往往是死代码 —— 该做的是把它删掉，而不是补一个测试去盖住它。** CI 的覆盖率闸门会把未覆盖的块逐块列出来；名单上的每一行，先问它是不是根本不该存在。

## 优先用真实实现，而不是 mock

只 mock 昂贵或不确定的边界：网络、时钟。下游全部保持真实。

一个手搓的替身只能证明桥梁在搬运字节，不能证明交付出去的那个东西按断言的方式工作。测挂载行为时，用真实的 `fuse` 加真实的 storage；测 local-disk object store 时使用真实的本地 filesystem 与真实 SQLite。只在要精确命中某个 barrier failure 时替换那一个 filesystem operation，成功路径与其余下游仍保持真实。

## 失败要注入，不要制造

制造一个真实的失败 —— 把客户端指向一个没人监听的地址 —— 只证明**那个客户端**会怎么归类**那一种**错。这是客户端自己那一层的用例，在那里跑一次就够了。

组合层要证的是另一件事：**不管下游报什么，都不会被改写成一个关于名字的答案**（R-ERR-2、R-ERR-6）。「不管什么」是这条保证的全部内容，而一个真实的失败只送得进一种错 —— 一个拒绝连接的地址给的是 EIO，凭证被拒、请求超时、以及一个 errno 词汇表里根本没有的错误，它一个都产生不了。所以组合层在它自己的下游接口上注入，把整套词汇送进去。

这与上一节不冲突：注入的是**错误**，不是实现。被点名的那一次调用换了答案，它下面仍然是真的对象存储和真的数据库，被读的那个对象仍然是一次真的 `Put` 写进去的。

## 不为一个用例不断言的东西付出等待

判据只有一句：**这段等待里发生的事，有没有一条被断言？**

没有就不许留。SDK 的重试退避是典型 —— 一个死地址上的调用要睡够四次尝试的间隔，而用例断言的是这个失败被归成哪一类，第四次尝试的错误和第一次归类完全一样。这样的等待要在用例里显式关掉，并写明关掉它不影响断言的什么。

这类等待不会让任何用例变红，只会让整个套件慢到没人愿意在推送前跑它 —— 而一个没人跑的检查等于没有。

## 验证世界，而不是自述

一条端到端断言必须**从另一条路径**重新读取事实 —— 直接读服务端被交出去的那个目录、从第二个挂载点读、重新执行命令。

**读被测那一方自己的说法不算验证。** 挂载点只给得出它选择报告的东西，客户端只给得出它自认为发出去的东西，二进制只给得出它自己打印的那行「已挂载」。一个只断言这些的测试，恰恰放过了「自述在说谎」这一类缺陷 —— 而那正是最需要被抓住的一类。

断言未被触碰的文件逐字节相同。

## 一个防护只有在回归真的能让它失败时才算防护

引入回归 → 看它变红 → 撤销。**没做过这一步的防护测试，等于没有。** 这个仓库里已有的每一条防护都过了这一关：被真的破坏过一次，看着变红，再装回去。

这条对反向判据（要求某种东西*不存在*）尤其重要：一个检查「包里没有任何写向调用方输出的调用」的测试，若从未在真有一处这种调用时失败过，它证明不了任何事。

**发现一个防护根本不可能失败时，重写它，而不是相信它。** 上面那个检查在一个源文件都没解析到时会通过；一个靠「够不到的 unix socket 带着 ENOENT 冒出来」才有意义的用例，在那个前提不再成立的那天什么也不测。两者都要把前提本身写成用例里的一条断言。

## 空转的用例必须让自己失败

一个碰巧什么都没检查的用例，读起来和一个通过了的用例一模一样。

原子替换那条用例就是这么写的：两个读者若没有各自观察到两个值中的每一个，说明它们整段时间都跑在两次写入之间，于是它判自己失败。凡是靠交错、靠时机、靠某个前提成立才有意义的用例，都要把那件事断言出来。

## 请求中断与修改结果

[请求中断](../.agents/notes/implemented/bug-fix/2026-08-22-eio-from-a-freshly-mounted-mountpoint.md)按阶段验证，不能只测一条 `context.Canceled → EINTR` 映射。错误分类用例覆盖直接与包裹的取消、deadline、已命名 errno、未知错误、独立故障和取消的两种 join 顺序，以及当前节点的权威分类；`ErrnoOf(nil)` 为 0，`ErrnoNameOf(nil)` 为 `EIO`。

SQLite 只读用例覆盖查询取消、预算回调取消、回调成功后才取消、`database/sql` 自动回滚产生的直接 `ErrTxDone`，以及真实查询和 cleanup 故障。SQLite code 9 只有与实际已取消的读取 context 同时出现时才按取消解释。每种情况都验证原因保存和最终 errno；未知 commit 与 poison 仍为 `EIO`。

HTTP 用例区分 `Do` 前取消、已发出的只读请求与已发出的 mutation，断言是否到达服务端以及最终 errno。真实网络错误不能泄漏 `ENOENT` 等底层 errno；成功修改后的 barrier 取消仍为 `EIO`。FUSE 复合操作分别在任何效果之前及已有 namespace 或句柄效果之后取消，验证后者不会返回暗示整个操作未执行的 `EINTR`。

`Flush` 用例覆盖请求值保存、关闭线程取消被隔离、默认 30 秒与显式预算、负值拒绝、较早请求 deadline 保留，以及有未提交内容时至多一次底层 `Write`。预算必须在等句柄 mutex 之前起算，锁等待后只剩原 deadline 的余额；用例验证 context 的 deadline，不把它当作 mutex、`Mount.Wait` 或 `Unmount` 耗时上限。`Fsync` 与设置时间前的句柄提交仍须响应取消，已有副作用时保持 `EIO`。

容量探测用例在第一次或一次过期的 `Space` 查询中分别返回直接、包裹、由 context 产生及经 wire 解码的 `EINTR`，断言缓冲区和 dirty 状态不改变、探测占用释放、旧有效数字与旧刷新时间保留。立即重试必须重新探测；配额 65536 字节、已有 32768 字节、准备写入 36864 字节时，拒绝必须发生在 `Write`，目标仍为空。与取消合并的独立故障不能变成 `EINTR`；其它 advisory measurement fault 与 `ENOSYS` 用例继续按各自策略验证。

真实信号探针在实际 HTTP `Stat` 已进入服务端后，向执行系统调用的子进程线程发送 `SIGUSR1`，同时观察原 FUSE 与 HTTP 请求 context 被取消。原始 `Fstatat` 必须得到 `EINTR`，普通 `os.Stat` 依靠标准库处理中断后成功，两个调用方随后都须读到完整内容。`Close` 探针则在提交进入等待后发送信号，要求普通关闭成功、底层只提交一次且随后从 storage 读到相同内容；它不依赖重试已经消耗的描述符。容量探针验证普通 Go `Write` 处理中断后重新取得测量，并以 `EDQUOT` 拒绝超额内容。

信号探针独立于纯映射测试，也不把 SIGURG 当作所有历史失败已经证实的原因。四项历史 `cmd` 用例分别以 `-race` 固定采样 100 次，共 400 次；失败如实保留，应用层不增加 `EIO` 重试，不关闭异步抢占，不预热被测操作。这些聚焦样本与信号探针分别提供运行证据，不以重跑整个无关命令套件代替阶段断言。

## 显式文件锁

[文件锁设计](design/server/file-locks.md)的验证分为共享 namespace 契约、authority 状态机、native 最终发布、持久恢复和 HTTP 编码。`X` 证明本次修改的权限，内容版本比较另有自己的义务；普通打开或读取不会隐式取得租约，读取成功也不能充当租约仍有效的断言。

### 保护、身份与有界历史

[共享契约](../packages/storage/lockcontract/contract.go)在普通目录、quota 包装、SQLite/objectstore 和 HTTP namespace 上使用同一套操作。用例覆盖 `S/S` 共存、`S/X` 与不同 Owner 的 `X/X` 冲突，匿名修改与只有 `S` 的持有者自身修改都被拒绝；带有效 `X` 的 Write、显式 mode/atime/mtime、Remove 与 Rename 才能修改受保护目标。普通读取在 `S`、`X` 下仍可完成，无活动保护冲突时匿名修改继续可用。每次拒绝后从 storage 重新读取内容，不能只检查错误码。

身份用例让祖先目录改名、内容原子替换、目标覆盖、删除与同名重建真正发生，验证源文件保持 ResourceID、被覆盖目标退役、旧 proof 不会指向新节点。覆盖式 Rename 必须同时提供受保护源与目标的 `X`；多余或不相关 proof 也必须失败。Scope 用例修改调用方原 proof slice，断言已创建的 scope 不变；显式匿名 scope 不继承外层权限。已解除或到期的 proof 即使没有竞争者，也不能退回匿名执行；空 SetAttr 与自身 Rename 同样验证它。目录、符号链接、缺失路径等不受支持的目标由各 native 适配器另测，普通目录还覆盖 hard link 与外部 inode 变化。

[authority 契约](../packages/locking/authority_contract_test.go)用可控制时钟分别断言不可变动作回执与当前 Grant 状态：冲突或 AlreadyHeld 的已记录拒绝在竞争结束后仍原样重放，成功 Acquire 的原回执在 Release、Expiry 或 TargetGone 后不变，重复 RequestID 携带不同意图必须拒绝。Renew 不缩短已有期限，重复 Renew 不延长第二次，持久水位准备期间到期的 Grant 不得复活。排队写者先于后来读者；排队超时、已授予后取消、尚未到达的 Acquire 被取消，以及取消后迟到的原请求均有确定性交错。控制请求 context 结束不会撤销已经受理的 Pending 意图。

[容量用例](../packages/locking/contract_capacity_test.go)分别耗尽 Session、已消费 enrollment ticket、Owner、Resource、Action、Grant、全局及各层队列、proof 数与请求字节预算，不能用一个较紧的上限代替另一个。活动 Session 上限独立于 ticket 到期；ticket 重放不会创建第二个 Session，原 Session 已关闭也不能借旧 ticket 创建另一个。活动 Owner 的动作回执不被逐条逐出，满历史仍保留 Release、已知 Cancel、RetireOwner 与 CloseSession 的清理路径；新的未见 Cancel 只有取得有界 tombstone 才能确认取消，否则为 OutcomeUnknown。未见动作查询同样不报告“未授予”。NotAdmitted 不产生虚构回执；Renew 在历史满时保留此前已确认期限。引用到期释放 Resource 容量但不缩短已有 Grant，Owner/Session 退役后旧能力不能重新执行动作。

[后台清理失败回归](../packages/locking/contract_regression_test.go)在手动时钟中确认目标到期 timer 已注册，并暂停它的返回；推进时钟后才放行，使后台路径实际观察到到期。仅看到曾经注册过的 timer，不能证明当前等待仍使用它。用例继续断言后台 `Forget` 失败使授权方停止发布，且 `Close` 不再次尝试结果不明的清理。

### 最终发布与观察次序

[localdir 用例](../packages/storage/localdir/operations_test.go)与 [objectstore 用例](../packages/storage/objectstore/locking_publication_test.go)在真实 staging 阶段暂停上传，随后推进时钟或取得新的冲突 Grant；恢复上传后必须在最终发布拒绝旧 proof 或匿名修改，原内容保持完整，staging 不暴露为 namespace entry，字节预算最终归还。另一文件的修改必须在暂停期间完成，证明慢上传没有占用它的发布权。SQLite 另让 staging 后的路径指向不同节点，断言 [Commit 根据实际目标验证](../packages/metastore/sqlite/publication_test.go)，Reserve 成功不代表最终发布已经获准。

发布交错用例暂停已经取得最终许可的 native transition，随后启动同资源的 Release、Grant 查询与新的权威视图；它们必须等待该发布结果。localdir 与 authority 独立用例另断言其它文件在最终发布暂停期间可前进；SQLite 保留自身 writer / health 串行化，其发布用例验证新视图等待与旧快照可读。时钟越过租约期限也不能使查询越过未完成发布并先报告 Expired。这里断言授权与效果的次序，不要求已获准的 rename、commit 或同步在到期时刻前物理返回。已经捕获的 immutable object 读取与有界目录快照可以在门外完成；List 的 caller charging 被暂停时，后续修改仍能完成，而旧结果继续呈现捕获时的名字和属性。

失败用例区分未发生、已知发生和结果未知。SetAttr 部分效果与原始错误必须同时保留；已发生的内容替换即使同步或确认失败也更新逻辑绑定，删除与目标覆盖先退役真实受影响身份，再开放后继动作。结果未知、记账收尾失败或持久确认无法证明时，namespace 与 authority 都拒绝继续报告成功；Close 排空在途发布与持久水位准备，关闭失败保留原因和必要所有权，不能对可能已经复用的描述符重试 Close。

[native quota 用例](../packages/storage/limited/publication_test.go)在最终目标确定后计算真实的旧、新大小：增长先预留，只有已知应用才返还缩减或删除释放的字节；未应用的失败返还增长预留并保留原用量。用例暂停 staging 后改名祖先目录并重建原路径，再核对两份内容、即时 Used 与 Recount，避免提前 Stat 的大小被用于另一节点。已应用但回复失败仍按实际效果结算；[未知结果或 unwind/settlement 失败](../packages/storage/limited/publication_uncertainty_test.go)保守保留预留，并使后续修改、Space 与 Recount 失败，已在 staging 的调用也不能越过该状态。嵌套 quota 另验证准备被拒绝后各层预留均已归还。只有实现 native 最终发布记账能力的包装层能同时暴露锁服务；这些断言不把 opaque 第三方 storage 的路径采样包装解释为具有同样保证。

### 恢复与独占所有权

[SQLite 恢复用例](../packages/metastore/sqlite/lock_recovery_test.go)分别在 Prepared 提交、见证写入前后与 Accepted 完成处中断，重新打开后核对恢复出的最大时长与 Prepared 已清除。拒绝用例逐项构造缺失记录、缺失或回退见证、单边状态回退、错误身份、部分 Prepared、跳代、下降的时长与错误字段类型，断言 `EIO`。并发提高水位必须保持单调，取消或持久失败不能确认提高成功，注入的见证错误保留原因链。已有 v3 数据库的迁移先核对原 accepted witness，再改变 schema。

普通目录的 [state 用例](../packages/storage/localdir/state_test.go)与 [fault 用例](../packages/storage/localdir/state_fault_test.go)覆盖明确 Init/Open、匹配的中断初始化 intent、private StateRoot 与服务目录的持久绑定、丢失 READY、复制或替换状态目录，以及 root lifetime lock。恢复期从实际取得独占所有权的单调时刻起算，不能在读取证据后重新起算，也不能被较小的新配置缩短。xattr、flock、rename、文件及目录 fsync 的能力探测在初始化和重开时都要验证失败；暂存和探测残留的清理受数量与字节边界约束。SQLite-backed 模式的 [native lease anchor](../packages/metastore/sqlite/lease_anchor_test.go)另覆盖 workspace/database 绑定、remote filesystem 拒绝、记录校验和、路径替换、探测误报成功及中断 stage 的恢复。

[独立 SQLite 所有权用例](../packages/metastore/sqlite/locking_store_test.go)验证 raw opener 的共享 flock 与授权方的排他 flock 在同进程、跨进程中双向排斥；数据库绑定后不能通过 raw constructor、路径别名或并发拥有者绕过保护。恢复配置和启用入口必须验证真实排他拥有者与 native anchor；重开另一个已有 namespace 时仍须读取同一数据库级最大时长并等待完整恢复间隔，选择不同名字不创建新证据。local store 继续验证单 workspace 根绑定，不能省略锁配置或丢弃见证来恢复为空的 authority。真实子进程在租约已确认后遭 `SIGKILL`，由新的进程或组合 store 重开：内容仍完整，恢复期间读取和状态查询可用，修改为 `EAGAIN`，较小配置不能缩短此前水位；旧 Owner、Grant 和动作不能在新 authority 下重放执行。故障注入与真实退出分别证明具体持久边界和进程生命周期，不把它们当作断电或设备缓存验证。

### HTTP v3 与客户端

[server 用例](../packages/transport/httprest/lock_server_test.go)验证 `/v3/` 协议标记和旧版本拒绝，以及匿名与 scoped 调用都抵达同一个执行保护的 namespace。Scope header 的空值、重复值、错误 base64url、缺失或重复成员、未知字段、超长值和错误使用位置必须在修改前拒绝，随后 Stat 证实目标未创建；读取与控制操作不接受 mutation scope。能力值只在 body/header 中传递，URL 与错误诊断不能泄漏它们。[Scope 用例](../packages/transport/httprest/lock_scope_test.go)逐一验证所有 mutation、WithBarrier 与无效果 mutation 保留复制后的 proof，读取不发送它，匿名 handler 不继承被包装客户端的权限。

[副本转发用例](../packages/storage/replicated/lock_service_test.go)分别把基础 replica 和 scoped 视图交给真实 HTTP handler。代理查询须找到原授权方的 grant，匿名修改被拒绝，显式 proof 修改成功后本地 replica 立即可见；经代理 Release 后，再从原授权方确认 Released。随后匿名写与普通读仍可用，关闭代理 HTTP 服务后底层 replica 仍能写入，内容由原服务端重新读取核对。

[client 编解码用例](../packages/transport/httprest/lock_client_test.go)区分缺失字段与合法零值，覆盖枚举、整数毫秒、溢出、嵌套意图、截断和不一致的成功或错误结果。HTTP 422 的已记录拒绝必须同时返回原 ActionResult 与 typed error；`lockCode`、errno、recorded、意图、Grant 与回执 variant 不匹配时按协议错误处理，未受理的失败没有虚构 receipt。丢失 Acquire 回复后使用原 Owner、RequestID 与 ResourceRef 核对结果，迟到的原请求不能越过已确认 Cancel，也不能用新身份重新申请。错误归类继续保存原 context 或网络错误原因。

[历史窗口用例](../packages/locking/history_test.go)与 wire 用例推进时钟后核对 Resolve 的 `HistoryExpiresMillis` 已报告延长后的 Session 历史期限，同时区分资源引用本身的 expiry。响应缺少 historyExpiresMillis 必须解码失败，Acquire 的嵌套 ResourceRef 与 receipt 也保留该必需字段。其它成功控制与带回执的已记录拒绝均核对当前历史期限；Acquire / Renew 重放在等待查询前不能先延长期限，再因取消返回一份没有期限的错误。测试同时核对实际延长与返回的期限，不能只断言内部 Session 尚未到期。

Pending 用例先占满 authority 的申请队列，随后确认 HTTP control admission 已归还，Renew、QueryAction、Cancel 与 Release 仍可完成；Acquire 的 Wait 是受理后意图的有限寿命，不是 HTTP 长请求的占用时间。另一组用例同时耗尽 server body/response 与 client response admission，enrollment 和状态控制仍须完成。控制 admission 的配置用例分别检查默认值、非法上限和有效配置的传递；client 名额满时，拒绝必须发生在发送前，并保留 `recorded = false`，不能声称动作已有结果。

时间断言使用不同于墙钟的 authority ticks。[毫秒回归用例](../packages/locking/contract_regression_test.go)构造到期 1.1 ms、当前 0.9 ms 的边界，要求仍 Active 的 Grant 报告 `RemainingMillis = 0`，不能把分别取整后的两个时间相减得到 1 ms。客户端本地提示从请求发送时刻加 remaining 起算，零余额不获得新期限，Expired 不产生有效期限；原回执的 TTL、deadline 与 revision 不能重置当前保护。控制操作在 dispatch 前取消为 `EINTR`；已 dispatch 的状态修改即使因取消丢失回复也为结果未知的 `EIO`，只读 Query 的 POST 仍按读取语义分类。上述用例不以 HTTP method 判断操作是否已有副作用。

## 元数据副本的读写交接

[副本读写门](../.agents/notes/implemented/bug-fix/2026-09-07-let-replica-writers-progress.md)分别验证类间次序和真实入口。门的确定性交错用例先证明写者已登记，再放开旧读者；读者批次必须在唤醒前保留名额，尚未获调度的读者也不能被下一写者越过。覆盖批次内读者的正常进入与取消、后来读者进入下一批、多写者中只撤销本次取消，以及取消最后一个等待写者后重新放行读取。

真实 `Replica` 用例覆盖 `Apply` 与 `Reseed` 等门取消、等待 commit gate 时释放外层名额，以及 `Stat`、`List`、`ListBounded` 在 reseed 后排队时的取消；失败的 `ListResult` 不能暴露已保留前缀。SQL 读取名额另验证与 reader pool 容量一致、名额耗尽时调用不进入阶段、交接后取消归还名额，以及 `Position` 与写者不消耗 SQL 读取名额。完整、回滚、无效 row 与提交失败的 reseed 都同时观察树和 `Position`，并验证重复 `Close`；读者批次还必须在多个真实 `Apply` 的积压之间得到执行机会。取消用例保留现有错误分类与 context 原因。

压力与可见性使用不同的时间判据。[SQLite 压力用例](../packages/metastore/sqlite/replica_progress_test.go)先占满现有 reader pool，再启动 128 个公开 `List` 调用。用例在门的互斥保护下确认只有与 pool 容量相等的读者持有共享访问，其余调用仍在阶段外等待 SQL 名额；确认 `Apply` 已登记等待后才释放 reader pool。4096 文件的读取循环保持运行，直到 `Apply` 成功；race detector 下给已获准的扫描留出 30 秒死锁检测上限，这个上限不代表复制延迟。

[真实 HTTP/SSE 全负载可见性用例](../packages/storage/replicated/replica_acceptance_test.go)使用 4096 文件与 128 个持续列目录的读者，以一秒为独立写者提交后另一客户端观察到新元数据的上限。用例先观察每个读者都成功完成列目录，再提交变更，并断言计时窗口内列目录继续推进；调用进入某个回调不构成负载成立的证据。

这项验收只在测试文件上使用 `rfs_acceptance` build tag，生产实现没有对应分支。CI 在其它包的并行测试与 race suite 之前，以正常构建、串行执行整个 `packages/storage/replicated` 包，使用 `assert-every-test-ran.sh`，不按 `-run` 缩小集合。原有的一秒 HTTP/SSE 可见性用例仍在默认测试集合中并接受 race 检查；阶段交接、取消与 4096 文件压力用例也保留默认 race 覆盖。全负载一秒断言和 race 下的死锁判据分别验收，不互相替代。

```
.github/scripts/assert-every-test-ran.sh -tags rfs_acceptance -count=1 \
  -p=1 -parallel=1 -timeout 3m ./packages/storage/replicated
```

## 测真实入口

「真实入口」指交付出去的那个形态：真实挂载的挂载点、构建出的二进制、被第三方 import 的 package。

库内部直接调用能通过，而挂载后不行 —— 这类失败只有真实入口能暴露。作为库被链接的那条路径同样要测：不注册信号处理、不写标准输出、不调用进程退出。**这一条要两种检查一起用**：把测试二进制重新当作一个普通程序执行，断言它真实的描述符上什么都没有；以及在源码的语法树上静态检查同一批禁令。两者抓的不是同一类东西 —— 运行时那条抓的是依赖替我们打印的东西，静态那条抓的是没有任何测试到达的代码。

构建出的 `remote-fs-server` 二进制覆盖普通目录的启动、挂载与停止，常见命令行拒绝，以及 local store 的跨进程重启持久性与独占锁竞争。需要证明独占所有权跨进程生效时，第二个真实进程直接尝试打开同一根目录，不以首个进程的日志代替事实。

[文件锁二进制用例](../cmd/locks_test.go)分别启动普通目录、quota-limited 目录、local store 与 Azure Blob 三种 mode 的四种配置，经过真实 HTTP 验证 `S/S`、匿名与同 Owner 的 `S` 修改拒绝、`X` 修改、改名后的身份保持、删除重建和不相关 proof 拒绝，再重新读取目标与未触碰文件。重启用例在已确认 3 秒租约后杀死 server，以 500 ms 配置重开，要求恢复剩余时长仍大于 500 ms、期间读取成功而修改为 `EAGAIN`；恢复后修改可用，旧 proof 为 `ESTALE`，旧动作查询为 Retired。普通目录另用真实进程拒绝缺少、复制、替换或重新初始化已有 StateRoot。Blob 路径使用同一 Azurite 依赖，不能以另外两种 mode 的结果代替它。

[锁配置用例](../cmd/remote-fs-server/lock_configuration_test.go)验证容量和期限逐项传入 authority，`-initialize-lock-state` 是显式动作，`-lock-state-root` 仅用于普通目录，非法配置在 listener 或状态初始化之前拒绝；native 目录预算和 HTTP write/staging 上限另有配置断言。[状态用例](../cmd/remote-fs-server/status_test.go)检查 ready、recovering、unavailable 和各项计数，不输出 Authority 能力材料；状态失败不拼接部分容量数字，不可用的 authority 不宣布就绪，缺失 status 能力的 namespace 被关闭且关闭错误保留。这些包内断言验证参数与 lifecycle wiring，跨进程结论仍由二进制用例提供。

其余聚焦的 server 入口行为在 `cmd/remote-fs-server` 包内验证：配置解析与默认值，三种 storage mode 的打开路径，READY 与 signal ownership 的顺序，SIGHUP 的异步 recount 或 metastore-backed status，SIGINT／SIGTERM 的 admission 停止与 handler 排空，以及 partial-open 或 shutdown failure 后的资源释放。quota measurement flags 只允许用于 quota-limited `-dir`；pending、reader、integrity、sweep、snapshot-frame 与 subscription flags 只允许用于 metastore/objectstore-backed mode，local waiting-operation flag 只允许用于 local store，且 invalid bounds 在 listener/root mutation 前拒绝。sweep 用例还拒绝非正 interval/batch 与超过 `MaxSweepBatch` 的 effectively-unbounded batch。write-bound 用例分别验证 local object 上限、Blob 5000 MiB 上限与 pending-byte threshold 的精确边界和超限拒绝。startup 的 directory/frontier 超限不会打印 READY，recount 的对应超限与取消保留此前 `Space.Used` 且服务继续工作；两种 metastore-backed status 都断言打印 effective reader/integrity-record/name-byte limits，local status 另打印 waiting/active operations。blocked recount 用例还断言终止信号在等待 handler 之前关闭 admission 并取消遍历，shutdown 同时使 listener 不再接受新连接。这些用例直接调用命令内部的 opener 与 lifecycle helper；它们验证同一条命令代码路径，不构成已构建二进制的进程边界证据。

`cmd/remote-fs` 包内测试同样区分入口层次：mutation-confirmation 与 client frame flags 的默认、help、invalid-before-network 和 forwarding 直接驱动 `run`、dial/replica helper 及真实 HTTP stream；构建出的 mount 二进制端到端用例仍走默认配置。前者证明 command wiring 与复制路径，后者证明交付程序的进程、信号与挂载边界，结论不能互换。

独立 HTTP server 的资源用例使用真实 TCP listener：占满 accepted-connection 名额后底层 `Accept` 不再前进，connection 的单次与重复 `Close` 只释放一份名额，关闭饱和的 listener 会唤醒正在等待的 `Accept`。配置用例允许普通目录使用一条 connection、要求 metastore-backed mode 至少两条，并在取得 listener 前拒绝非正 timeout。只发一部分 header 的连接在 `ReadHeaderTimeout` 内被关闭，keep-alive connection 超过 `IdleTimeout` 后被关闭；同一用例断言 request-wide `ReadTimeout` 与 `WriteTimeout` 保持为零。shutdown 用例覆盖已经存在和尚未被 tracker 观察到的 `StateNew` connection，确保 stopping state 会关闭 late notification，不把退出安全性押在 header timeout 上。

## 每次改动必须带什么

**任何非平凡改动都要在同一次改动里新增或更新测试。** 判据与 Agent Note 相同。

- 改了行为 → 聚焦测试
- 改了跨包契约 → 契约两侧各有测试
- 修了缺陷 → 一个**在修复前会失败**的测试
- 改了错误路径 → 测那条错误路径

**错误路径不是可选覆盖。** 这个系统最重要的保证 —— 不可达即报错、不静默丢数据 —— 全都活在错误路径上；只测成功路径等于没测那些保证。

其中一条不能推迟：**够不到命名空间时，不得回答一个读起来像事实的答案。** 不是「文件不存在」，不是空目录，不是一份编造出来的属性（R-ERR-1、R-ERR-2）。十一个操作各自解析自己的失败，因此每一个都要各测一次；只测其中一个，是在赌另外十个的作者当时想的是同一件事。

## 对象存储后端分别使用真实基底

### Azure Blob

`packages/storage/objectstore/azblob` 对一个真的 Blob 端点跑，那个端点是 Azurite。它定义在 [`deployments/azurite.yml`](../deployments/azurite.yml)：本地 `make azurite` 起、`make azurite-down` 停；CI 的两个 job 各自在依赖 Blob 的测试之前启动同一模拟器，并等待它能够回答请求。挂载 job 的真实 Azure 二进制锁与重启用例依赖该端点，因此启动步骤位于对拍和端到端测试之前；后面的全 module 覆盖率闸门继续使用这份模拟器。

**够不到模拟器时这一层失败，不跳过。** 依赖缺席是一个必须报出来的事实，不是一个可以让用例自己消失的条件。

模拟器的版本和 SDK 的版本是一对，不是两个独立选择：Azurite 每个 release 都会抬高它接受的 `x-ms-version` 上限，超出上限的请求被答以 400 InvalidHeaderValue 而不是被服务。所以那份定义钉住具体的 tag 而不是 `latest`，理由写在 azblob 的 package 注释里。

### SQLite metastore 迁移

迁移用例从独立手写的 v1/v2 数据库开始，不用当前 migration 反向构造历史。只含 referenced object row 且结构、计数、`sqlite_sequence` 与日志 tail 一致的旧库必须前滚到当前 schema；含任何 non-referenced object row 的 v1/v2 库必须以 `EIO` 拒绝。这条用例同时防止旧的零字节 pending 记录绕过当前 byte threshold，以及旧的 time-derived garbage 被新清扫器误当作 ownership-proven 对象删除。v2 retained changes 在迁移后必须为空、incarnation 必须改变、tail／trim 必须归零，node/change 高水位必须覆盖迁移前的全部 surviving reference 与 sequence；迁移后的第一份 node/change 严格使用更大的值。已完全 trim、`committed_position = trimmed_through > 0` 且没有 surviving row 的合法 v2 日志也必须可以迁移。

当前 schema 与历史迁移都要用 corruption fixtures 验证每个 namespace 恰是一棵 rooted tree：root 无 incoming entry，非 root 恰有一个同 namespace parent，所有节点可达，cycle、孤儿与跨 namespace entry 都失败。另用整数、负数、溢出与 mismatch fixtures 验证 `namespaces.used` 等于全部 regular-file size 的 streaming sum。storage-class fixtures 把 entry/change name 改成 TEXT、把 schema/database/binding/log scalar 或 nullable change group 改成错误的 NULL/type，并构造非法 kind、position、mode、size 与 nanoseconds；snapshot/log 不能漏行或接受 driver coercion 产生的可信零值。日志 fixtures 删除中间或尾部 retained change，并分别篡改 `previous_position`、`trimmed_through`、`committed_position`：`Open`、`Snapshot` 与 `ObjectStatus` 的完整链验证必须以 `EIO` 失败且不写入新 incarnation；`Since` 允许缺口之前的完整 page，跨到缺口的 page 必须整体失败且不暴露该页 prefix。空或较小日志在 caller 给出很大 limit 时仍按实际 anchor/row work 成功，page budget 耗尽且 tail 尚未返回时才以 `EFBIG` 拒绝。整链 record ceiling 由 `Open` 与 `ObjectStatus` 的精确边界／超限用例直接覆盖；snapshot row production 另有 caller-owned byte-budget 故障用例。`MaxIntegrityBytes` 用当前 schema 的精确边界与超大 corrupt entry name 验证 `EFBIG`；legacy migration 直接覆盖 record ceiling。ID fixtures 删除 node/change sequence、协同回退 internal high-water/sequence、把高水位压到其它 namespace 的引用之下，并覆盖 node/change exhaustion；另抬高 committed tail，断言 append 在发布新 witness 前回滚。本地 durable fixture 再用外部 accepted state 拒绝协同回退。每一种拒绝都要断言版本、schema 与数据没有部分前进；namespace 结构在 `Open` 与 `ObjectStatus` 两条入口覆盖，database-wide sequence/witness 拒绝由 durable open、page anchor 和分配路径覆盖。

### 本地持久对象存储

`packages/storage/objectstore/localdisk` 与 `packages/storage/localstore` 在测试专用的真实本地目录里运行，验证的不只是 `Objects` 契约，还包括磁盘格式与 reopen 行为：

- `FORMAT`、`LOCALSTORE` READY marker、初始化 intent、`METASTORE`、SQLite binding／database identity／generation／高水位、directory marker、recovery record 与 object envelope 的版本、UUID、workspace、key、长度和 checksum；
- 根目录、祖先路径、owner、mode、symlink、remote filesystem、submount 与 lifetime lock；
- 初始化在每个 durable boundary 中断后只恢复已记录的 intent；已绑定 intent 能恢复 SQLite 已写非零 header、但 `sqlite_schema` 仍为空的 bootstrap，且该判定不依赖 WAL 大小。空 intent 旁出现 metastore、已有数据库丢失 workspace row，以及 READY store 缺失或错配 database/witness/component 都失败；binding preflight 用匹配 workspace 加额外 namespace 和超大 binding/workspace TEXT/BLOB 验证 bounded cardinality 与超限拒绝；
- 同 shard 的并发首次写入只串行目录／identity 阶段，不同 shard 继续前进；取消能退出 shard token 等待。identity stage-only 与 final+stage 同 inode 在 reopen 收敛，损坏、额外 hard link、不同 inode 与 stage 旁额外 entry 保留现场并失败；
- `fsync`、hard-link publication、unlink 与 marker cleanup 的顺序，shard identity/object publication barrier 结果不确定后的 health poisoning，以及 reopen 恢复出的事实；在 poison 前已经通过健康检查、随后等待 key/shard 的调用仍须在返回处转成 `EIO`，不能泄漏成功或 fact-bearing errno；waiting operation 上限满时返回 `EAGAIN`，同 shard 等待者不消耗 active slots，已 admitted 的其它 shard 继续前进；
- workspace quota 与 physical availability 取更小值，maintenance reserve、in-flight reservations、inode exhaustion 与 measurement failure；
- recovery records 与 object operations/bytes 受 admission 上限约束；pending byte threshold 对单个装不下的 payload 返回 `EFBIG`，现有 reserved/unresolved/garbage backlog 压力才以 `EAGAIN` 拒绝新 reservation；local store 在碰磁盘前验证 pending-byte threshold 能容纳最大 local object；authoritative shedding 造成的 `OverLimit`、reopen 后继续 admission refusal 与 garbage 清扫恢复都要覆盖；
- `Put` 错误把 reservation 转成 unresolved，不因超时或 sweep 被删除；只有 create-only `Put` 成功后的 `Commit` 失败才 `Abandon` 为 garbage。用例覆盖 collision 不向 namespace 泄漏 `EEXIST`、旧对象经立即与重启后清扫仍被保留、Put/Commit 回复丢失、request cancellation、收尾失败、pending admission，以及 reserved→unresolved/garbage 保持 count/bytes；清扫的 startup、event-driven、periodic、满 batch 自调度、串行化与 shutdown cancellation status 都有独立测试；
- SQLite ordinary-reader 与 snapshot-reader pool 分别覆盖默认、配置前校验、占满后的等待/取消与 connection 释放；另用一份 held snapshot 占满专用池，同时断言普通 log/read 仍可使用另一池；
- 修改返回成功后构造 intact、缺失、空和仅含 header 的 WAL，结合 `A = C` 与 `A > C` 验证重开只接受可证明状态；见证 stage/final、checksum、store/workspace/database identity mismatch 和 visible generation 前进／回退分别覆盖。`Accept` 失败必须 poison 并阻止读取；snapshot pin 使 status 报告 pending checkpoint，后台重试在释放 pin 后推进 C，真实 checkpoint error 保留到成功。open-time `Accept` failure 要保留可供下一次恢复的 WAL，成功 `Close` 则只在完整 checkpoint 和见证同步后释放 persistent WAL；清除调用失败保持可重试且不关闭 writer，不断言 flag 的最终值；
- data-plane 饱和时 status 仍通过 dedicated control slot 报出 waiting/active/in-flight-byte 数；组合 status 另报告 accepted/checkpointed generation、pending 与 checkpoint error。SIGHUP 成功输出 checkpoint generations/pending、SQLite reader、integrity-record/name-byte 与 effective sweep interval/batch，且有 deadline；checkpoint error 时只输出整次 status failure，不格式化 partial figures。关闭时先排空 handler、maintenance、waiting/active data-plane 与 control operation，并等待 checkpoint worker 退出，再建立 5 秒内部 context 约束 commit-gate admission 与 checkpoint；active reader 立即返回 `EBUSY`。reader pools 已关闭后的 checkpoint、见证或取消 failure 要断言 writer、所需 WAL 证据与磁盘锁保持，并发调用共享本轮错误且后续 `Close` 能从部分关闭状态重试；`PERSIST_WAL` 清除失败只断言 writer/ownership 保留且可重试，不断言 flag 或 WAL 的最终状态。pool close failure 要断言进入 terminal result、后续返回同一错误且所有权保留到进程退出。post-metastore `Open` failure 另验证 bounded close 失败后对未暴露 store 执行 `Abort`，无错误关闭前不会释放 object/root ownership；SQLite constructor pool cleanup failure 则验证 ownership-retained error 使内部 coordinator 与外部 root lock 都留到进程退出，普通构造失败仍释放。

durability 用例在修改操作返回成功后关闭并重新打开组合 store，从另一条读取路径验证名字、属性、内容、用量、change log、database generation 与见证都存在。corruption 用例逐项修改磁盘文件，断言打开或受影响操作以 I/O 错误失败，不能只断言进程没有 panic。

仓库内的 crash 验证同时使用 barrier fault injection、人工构造的 recovery residue，以及一个在 mutation 返回成功、WAL 仍被 snapshot pin 住时遭 `SIGKILL` 的真实子进程。前两类精确覆盖每个持久化边界，子进程覆盖 SQLite commit 已确认但进程没有执行正常 close 的整体路径；它们都不宣称模拟电源中断、设备写缓存或文件系统掉电恢复。真实 filesystem 与硬件是否兑现 crash-time `fsync` 语义仍是部署前提。

## 挂载相关的测试是独立的一套

它们需要 `/dev/fuse`，且卸载不总是第一次就成功；CI 因此把这一层单独放进一个 job，并给 `go test` 一个比 job 更短的 `-timeout` —— 卡住的卸载要留下 goroutine 栈，而不是被超时静默杀掉。单元测试必须在任何地方都能跑 —— 那是它们真的会被跑的前提。

**每一个挂载都在 `t.Cleanup` 里拆掉，拆不掉就让这一轮失败。** 留在机器上的挂载点或 FUSE 连接会拖垮之后的每一次运行，而留下它的那一轮通常已经报告成功了。会挂载的两个包的 `TestMain` 因此都走 `fusetest.Run`：它在跑用例之前把 `TMPDIR` 指到一个本次运行专用的目录，跑完只报告那个目录下面还挂着的东西。

**一个检查只能对它归属得了的东西下结论。** 挂载表是全机器共享的，而 `go test` 一个包起一个二进制并行地跑，所以「表里有一个 `fuse.remote-fs` 挂载」这句话说不出它是谁挂的 —— 曾经据此判红的那个检查，抓到的是隔壁那个包的挂载点，也会抓到机器主人自己挂着在用的那一个。归属靠的是那个专用目录：`t.TempDir` 在它下面建目录、子进程继承 `TMPDIR`，于是用例能拿到挂载点的每一条路径都在它下面。

CI 在这一层跑完之后再查一遍机器：没有 `fuse.remote-fs` 挂载、没有多出来的 FUSE 连接、没有活着的 `fusermount` 进程。**那一遍扫全机器是成立的**，因为它跑在所有 `go test` 进程都退出之后、跑在一台跑完就扔的 runner 上；进程内那一遍两个条件都不具备，所以它不去扫全机器。进程内那一遍也看不到 panic 或 `-timeout` 之后的状态 —— `TestMain` 那时拿不回控制权 —— 那一段同样由 CI 这一遍兜着。

**在 CI 里，跳过就是失败。** 没有 `/dev/fuse` 时这一层会跳过自己，而 `go test` 照样退出 0 —— 那份绿色和「全部通过」逐字一致，而这两层是整个系统能工作的唯一证据。所以 CI 不看退出码就下结论：它检查有没有用例被跳过、有没有用例连结论都没给出。要在本地复现这一层，先确认这台机器真的能挂载，而不是让它替你把问题跳过去。

推送前只跑覆盖这次改动的检查，不跑全量；穷尽是 CI 那次全量运行的职责，它自己会启动，不需要谁记得去按。**没跑过的检查不许说通过。**
