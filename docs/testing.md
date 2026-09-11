# 测试策略

本仓库怎么测：分层，以及那些让「全绿」有意义的规则。命令在根 [AGENTS.md](../AGENTS.md)；理由在对应的 Agent Note 里。

## 分层

- **契约** —— `storage` 接口的义务写成一套可执行的用例，住在任何实现之外。SQLite 与对象存储的组合实现、本地持久对象存储、HTTP 另一端与其它实现都跑同一批用例。**每一种实现通过同一套用例，是「这个接口是一层抽象、而不是对某一份实现的描述」的唯一证据。** 新的 storage 义务加进这套用例，而不是加在某个实现旁边 —— 只在一处验证过的义务，其它实现不会知道它存在。server backend 另跑 `storage.BoundedStorage` 契约：依赖在服务前可被校验，Read 在完整 payload 分配前拒绝超限，List 在保留越界 entry 前拒绝，且取消会停止产生结果。`objectstore.Objects` 与 `metastore.Store` 各有自己独立的契约套件；metastore 套件还验证 bounded `Incarnation` 在复制 identity 前拒绝超限、`Barrier` 原子返回同一 identity/committed position，并验证 `Since`/`Next` 在载入变长 payload 前预算、页满时不跳过下一项、单项超限与 production error 使整页不可读取。组合层再验证跨接口的顺序与错误保存。
- **单元** —— 核心逻辑：路径清洗与越界拒绝、errno 映射、打开引用的身份与生存期、逐次范围写和截断、FUSE 单文件上限、HTTP request/response 的单体、operation、waiter 与 aggregate byte 上限、stream 的单帧预算、derived event/cursor products 与 snapshot-page admission、subscription 与 snapshot 上限、replica mutation confirmation 的 active 与 waiter 上限、quota measurement 的单目录与 frontier byte 上限、local-disk object active/waiting/byte 上限、SQLite reader-connection 与 integrity-record/name-byte 上限、持久 ID 高水位、日志 predecessor chain、WAL 见证与 checkpoint 状态、消息编解码与 URL 往返。**不需要挂载点、不需要 `/dev/fuse`、不需要特权** —— 真实 SQLite 与内存 Objects 的组合，或者直接构造的内部结构，就足以驱动它们。每项上限分别验证边界值、超限错误、并发 admission 与取消，不能用一项恰好更紧的上限代替另一项的测试；write 上限低于 protocol body 时，超限 write 必须失败，而落在 protocol body 内的 listing 仍须成功。request 与 response admission 分别占满后，用例断言有界等待、超额等待者的 `EAGAIN`、context cancellation 与释放后的恢复。response 用例同时覆盖 server 侧固定结果的 operation/waiter 占用、队列中的 `Write` 不读 request body、client 在解码或验证 mutation barrier 前不释放 reservation，以及 stream setup 的 error body 也必须经过同一 client admission。frame 用例分别覆盖 client/server 不同但相容的上限、超限 change 与 snapshot row、oversized pre-stream identity、tail 已前进却返回空 bounded page、subscription/frame 与 snapshots/frame 的 checked products、snapshot-page aggregate 与 waiter 饱和、取消释放，以及不可能成立的 option 组合；隔离用例占住 snapshot admission 后仍要观察 change frame 到达，证明 bulk transfer 的 gate 没有被放到 subscription 路径上。另一条集成用例让 snapshot producer 等待超过 client silence bound，断言 keepalive 保持 stream 可用，释放 admission 后 rows 继续到达。subscription 用例占满名额后断言新 stream 以 `EAGAIN` 拒绝，并在 client 关闭和 `Handler.Stop` 后恢复；另一条用例让 stream 从 opening tail 之后继续收到 live change，断言 `Position` 前进后 `CaughtUp` 仍为 true。mutation confirmation 用例断言 fixed-size admission 发生在 request 发出前；纯取消为 `EINTR`、deadline 为 `EIO`，满 waiter queue 或 storage 关闭为 `EAGAIN`，这些路径均保留原因且不会发送 mutation；另覆盖 event/response 两种先后顺序、并发 writer 造成的 later barrier、barrier incarnation/generation mismatch、grace、stream/request failure 与关闭时释放 active count。quota measurement 用例分别卡住一份目录的 `Entry` 加名字和 active/pending 完整路径组成的 frontier，验证精确边界、超限时不保留越界项、从不调用 ordinary `List`、取消，以及 recount 失败后保留原计数并释放 gate。SQLite reader 用例占满小型连接池，断言后续读取等待、取消后退出，并在释放一个 snapshot 后恢复 admission；integrity-record/name-byte 用例分别验证含 mandatory log row 的空 volume 最小值、精确边界、跨 volume label/parent/child 与 object/log/change 计费、超限 `EFBIG`、取消后 store 仍可用，以及 pre-open validation。偏重边界情况、错误路径、并发交错，以及回归的永久用例。**一个关于 errno 映射的测试若需要挂载点，说明有个边界划错了。**
- **对拍** —— 把 `fuse` 挂在 SQLite 与内存 Objects 的组合实现上（不经网络），对挂载点与一个普通目录施加同一串操作，比较每一步观察到的结果、错误码，以及之后两棵树的路径、模式、大小与内容。不一致即缺陷 —— 不需要预先枚举「应该是什么样」，普通目录就是答案。适合随机化操作序列，因为它不需要预期值。刻意的偏差必须是 [`spec/requirements.md`](spec/requirements.md) 里明确列出的非目标，按名字跳过并注明是哪一条。
- **故障注入** —— 在被测那一层自己的下游接口上制造麻烦，下游有几个接口就注入几个。`storage` 接口上是一个专门制造麻烦的包装层：够不到的 volume、提交失败、报不出类型的节点、以及在一次操作已经部分成功之后才失败。`objectstore.Objects` 的 `Get`、`Put`、`Delete`、`Available` 与 `Close` 都分别注入错误；能力拒绝只接受纯 `ENOSYS`，与其它错误 join 在一起时不能遮掉 measurement failure。组合层还要验证关闭顺序、两个 durable half 的错误同时保留。local-disk 实现在 `fsync`、`linkat`、`unlinkat` 与容量查询处注入错误，覆盖 shard identity/object publication 已发生但 durability 无法证明的状态；localstore 在 witness publication、WAL 缺失与 checkpoint 处注入，验证确认失败 poison、可重试 checkpoint 与锁保留。传输层覆盖拒绝不能产生有界结果的 backend，body 截断、request/response 单体与 admission 上限、等待队列溢出、bodyless 操作不保留意外 body、metastore 在 bounded page 中途失败或给出过大 payload、mutation 已提交后 `Log.Barrier` 失败、缺席/null/畸形 barrier、缺失答案、错误 framing，以及回答者根本不是本协议的情况。
- **端到端** —— 整条链路一起跑：一个服务端、两个各自挂载的客户端，验证一边写入另一边一秒内可见、以及服务端消失时每个操作都报错。两台机器在这里由两个挂载点代替，缺的只有主机之间的网络。交付出去的两个二进制也在这一层：旗标、诊断、退出码，以及 Ctrl-C 之后挂载点确实消失。

对拍必须跑在**交付出去的那套配置**上。为了让它好过而调松的任何一处 —— 内核超时、提交时机、单文件上限 —— 都会让它去验证一条生产中不存在的路径。

公共边界各有包内直接用例：[metastore 页结果](../packages/metastore/bounded_test.go)核对预留、提交、整页失效和 payload 所有权，[发布 guard](../packages/metastore/files_test.go)核对组合顺序、首个失败及父 context 不变；[storage 值类型](../packages/storage/storage_test.go)核对路径边界、明确的零值属性与容量一致性。[limited 句柄](../packages/storage/limited/files_test.go)通过 OpenNode 在改名后截断同一对象，核对内容、Used 和超额拒绝后的原状态。[HTTP CancelLock](../packages/transport/httprest/file_client_test.go)取消真实 Pending 请求并重复核对同一 Request，原持有者释放后仍为 Cancelled，不能留下迟到授予。

普通对拍中的六处时间设置调用使用 [`comparisonChtimes`](../packages/fuse/fuse_test.go)：每处 `os.Chtimes` 的总尝试次数至多八次，仅在上一次返回 `EINTR` 时重做完全相同的路径、绝对 atime 与 mtime，包括明确省略某个时间的参数。挂载点和普通目录使用同一规则，只重试这一次调用；其它错误立即返回，八次仍中断则保留最后的 `EINTR` 并使对拍失败。这里比较最终的 atime / mtime，ctime 不在对拍结果中。专门验证中断的真实信号用例继续断言第一次系统调用的结果，不使用这个辅助函数。

对拍失败保留双方原错误的类型与文本。模式修改复合步骤先 WriteFile 再 Chmod，失败后才用独立五秒 context 查询 backing 属性及至多 32 字节内容；这些是失败后的额外观察，不重试原操作、不改变原来的失败判定。仍未解释的 EIO 由[独立调查](../.agents/notes/proposed/testing/2026-09-09-trace-unexplained-fuse-eio.md)记录，不能从步骤名称或未复现的批次推断原因。

## CI 工具链与缓存

两个作业使用[共享 setup action](../.github/actions/setup-go/action.yml)固定 Go 1.26.8，并按作业、工具链、依赖和源码 SHA 保存 module／编译缓存；同作业前缀及经校验的旧快照可作为种子。缓存复用编译工作，`-count=1` 仍使每次调用实际执行测试。包分配、race、覆盖率和严格 verdict 门禁不变。缓存上传也消耗时间与空间，净收益须看完整作业；原因与兼容边界见[缓存决定](../.agents/notes/implemented/process/2026-09-09-refresh-ci-go-build-caches.md)。

## CI 执行预算

checks 作业总上限为二十分钟，其中 contract / unit 步骤以 `-race -count=1 -timeout 10m` 执行，每个包测试二进制的累计预算为十分钟。这是整包执行的 watchdog，单项 deadline 与行为断言各自成立。严格的 skip、无测试与缺失 verdict 检查继续执行；串行副本可见性验收仍使用三分钟进程预算和一秒可见性判据，覆盖率门禁与包划分保持原义。预算依据、较晚发现整包挂起的代价及重新调查的条件见[执行预算决定](../.agents/notes/implemented/process/2026-09-08-budget-ci-race-test-execution.md)。

## 覆盖率是必要的，从来不是充分的

它只证明那些行跑过了，不证明特性按交付的样子工作。

每个 package 的覆盖率只由它自己的 Go 测试二进制计入，同目录的外部测试包也属于这份二进制。其它 package 或 `internal/integration` 的测试即使执行了该包的生产代码，也不给该包增加覆盖率。使用 Go 默认的 package-local instrumentation，阈值保持每包 70%、每函数 50%、总体 85%。根 [AGENTS.md](../AGENTS.md) 的 `--skip-result-packages` 只省略路径匹配 `cmd` 或 `packages/metastore/sqlite/internal/integration` 的结果行，测试仍执行且失败仍使检查失败；integration 自身没有生产语句，其测试不为其它包计入覆盖率。参数按包路径子串匹配，integration 子树必须保持纯测试代码与 fixtures。包内直接测试及其验证边界见[package-local 测试实施记录](../.agents/notes/implemented/testing/2026-09-09-sqlite-package-local-coverage.md)。局部测试与覆盖率 profile 的结论分别核对，不能替代最终全局 CI 的覆盖率门禁结论。

覆盖率文件本身也须有实际执行证据：mode 头之外有覆盖块、非零执行计数与真实函数记录。`assert-every-test-ran.sh` 当前会把显式 `-coverprofile` 再交给清单调用，可能将已执行的 profile 覆盖为仅有 mode 头的文件；[保留执行覆盖率的提案](../.agents/notes/proposed/bug-fix/2026-09-08-preserve-executed-coverage-in-strict-test-runs.md)单独处理这一缺陷。脚本退出成功或日志中的百分比不能替代对最终文件的核对。

**未覆盖的行往往是死代码 —— 该做的是把它删掉，而不是补一个测试去盖住它。** CI 的覆盖率闸门会把未覆盖的块逐块列出来；名单上的每一行，先问它是不是根本不该存在。

## 优先用真实实现，而不是 mock

只 mock 昂贵或不确定的边界：网络、时钟。下游全部保持真实。

一个手搓的替身只能证明桥梁在搬运字节，不能证明交付出去的那个东西按断言的方式工作。测挂载行为时，用真实的 `fuse` 加真实的 storage；测 local-disk object store 时使用真实的本地 filesystem 与真实 SQLite。只在要精确命中某个 barrier failure 时替换那一个 filesystem operation，成功路径与其余下游仍保持真实。

[memoryfixture](../packages/storage/lockcontract/memoryfixture/memory.go)为库测试组合真实 SQLite、objectstore 和内存 Objects，通过 `sqlite.OpenLocking` 取得 native 所有权与持久租约证据；它的对象内容不提供重启持久性，持久性由 local store 用例验证。FUSE、HTTP 与 quota 测试通过 volume API 准备内容和重新读取结果。对拍的另一端仍是独立的普通目录，挂载根 mode 与它保持一致；inode 用例保留真实 `Getdents` 观察以及删除、重建和另一客户端修改后的身份断言。

符号链接的 FUSE 用例在真实 volume 节点上使用 [`linkStorage`](../packages/fuse/fuse_test.go)装饰 Stat/List、StatNode 和 retained File 返回的 Attr，按节点 ID 保留符号链接 kind 与链接文本长度。与普通目录的真实 symlink 对比后，断言 dangling link 仍存在、查找和列目录保持同一 inode、读取和跟随为 `EOPNOTSUPP`，删除链接不改变目标内容；这个 fixture 不向 SQLite 添加符号链接实现。可选日志能力由 [无日志副本用例](../packages/storage/replicated/replicated_test.go)单独验证：只暴露 `locked.Backend` 并向 handler 传入 nil Log，构建 replica 必须返回 `ENOSYS` 且不是 `EIO`，不能因真实组合底层恰有日志而漏测缺失能力。

## 失败要注入，不要制造

[HTTP 选项拒绝矩阵](../packages/transport/httprest/server_test.go)各自复用原有 failingStorage，每项从完整的 options 值复制后破坏目标字段；原 Check／constructor 拒绝、可用配置与零值默认配置成功的对照都保留。夹具复用不能把原失败包装器换成会掩盖 capability 或构造顺序的替身。

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

[原子替换用例](../packages/storage/storagetest/storagetest.go)执行 100 次写入、两个读者与 128 KiB／96 KiB 两种内容。每次成功读取都必须匹配某个完整值，读者汇总未观察到两种值就失败；合法的 EAGAIN 仍按原规则计数，其它错误失败。成功核对或 EAGAIN 后使用 `runtime.Gosched` 让出调度，读取循环没有次数上限、睡眠或固定间隔。

选择调度和重复次数时，额外测量每个读者的完整值观察、读写 API 区间重叠，并用无额外等待的非原子发布负向对照核对检出能力。100 次写入的采用还比较了六个包各自测试二进制的覆盖块集合和原判定名称。较少重复次数降低统计暴露；看到两种值、API 调用重叠或覆盖块相同都不能证明覆盖每个内部发布窗口，具体证据与代价见[测试工作量决定](../.agents/notes/implemented/testing/2026-09-09-scale-test-work-to-its-assertions.md)。凡是依赖交错、时机或前置状态才有意义的用例，都须区分已断言条件与测量尚不能证明的条件。

## 业务方提供的操作授权

[授权设计](design/server/authorization.md)拥有语义操作表；测试按该表核对映射，普通、bounded 与 barrier 入口保持同一语义。[操作词汇用例](../packages/storage/operations_test.go)核对 storage.Operation 常量；[authz 值类型用例](../packages/authz/authz_test.go)核对函数适配器保留原 context 与错误，以及策略修改 AccessRequest 副本不会改变调用方的值。身份来自 host 自己的 context key，Volume 来自 handler 的可信配置，已有 capability 保持 bearer 语义。

[入口授权用例](../packages/transport/httprest/authorization_test.go)验证 Authorizer 与 Volume 同时缺省、成对配置、typed-nil 和 nil 函数拒绝，以及不改写不透明 Volume。普通操作先拒绝再允许，分别核对拒绝时 backend／barrier 未被调用、允许后原 backend 错误仍返回，成功修改只在授权之后读取 barrier。非法请求不触发策略；并发请求保留各自的 host 值，请求结束解除派生 context，未启用 hook 的路径保持原行为。

[文件授权用例](../packages/transport/httprest/authorization_file_test.go)在 capability 查询、引用创建和动作记录之前核对每种 file 操作。合法打开的全部意图经共享的 storage.OpenAccess 值和一次 callback 传入，wire 分发与授权共用规范操作值，显式 file.unlock 与申请／转换分别核对；非法参数在策略之前拒绝。用例先取得真实引用和动作回执，再拒绝 ack、renew、status、重放、修改与 close，核对原回执、期限、pending ack、引用和内容均未改变，拒绝的新动作没有记录。允许后重放仍返回原引用，原动作才可继续执行。

[强占有授权用例](../packages/transport/httprest/authorization_lock_test.go)逐项检查控制操作在 native service 或 status capability 访问前授权，并重新检查曾经成功的相同请求。拒绝只描述本次入口结果，不能泄漏已有动作回执或捏造 lockCode、recorded、Cancelled、Released 等 native 结果。文件侧另用只读描述符取得真实 EX flock，拒绝显式解锁、取消、DropLocks 与查询后，再从允许的控制路径核对原 Grant 和历史仍存在。调用方 cleanup 可以被拒绝；内部 lease 到期仍释放 retained 字节，且不会再次替调用方请求授权。

授权错误用例区分明确的 ErrDenied 标记与无法完成策略查询的故障，包括包装、join 和带 native 分类的底层错误。本地保留 cause，普通 HTTP、file、lock 与 stream writer 只输出可信的固定 EACCES／EIO 和消息；不把 callback 的原始文本或 native 回执写出。原请求生命周期先于策略分类：volume/file 可证明尚未 dispatch 的调用方取消沿用 `EINTR`，deadline 或策略自身超时保持相应 `EIO`；强锁服务端的生命周期和非 native 服务错误保持 `Unavailable/EIO`、`recorded=false` 且无 action。策略拒绝或故障使用独立的普通 EACCES／EIO envelope。

入口 callback 使用既有有界 response admission，满额时可在调用策略之前返回 `EAGAIN`。Stop 先取消并返回，ServeHTTP 仍须等待 callback 排空，期间不能继续调用 backend；请求结束也不得取消 host 原始 context。流的初始授权仅在启用 hook 时使用该 admission，拒绝响应写完后才归还，允许后在进入 Log、订阅与 snapshot 资源之前归还，不把名额占到流结束。

[流授权用例](../packages/transport/httprest/stream_authorization_test.go)分别验证 subscribe、resubscribe、snapshot 在初始 Log 与受控资源准入前检查，nil Log 和已满的订阅／snapshot 名额不能越过拒绝。start／rebuild、change、snapshot open／rows／done 与 keepalive 前再次使用原操作和 host context 检查；拒绝或策略故障后，队列中的页面、保活与成功 done 均不出站，terminal fault 只携带固定错误。用例同时确认页面确实已预取，以及最终 frame、snapshot 和订阅名额已归还。

背压用例让初始 Flush 或活跃 frame 的写入阻塞，再取消 host context 或 Stop，核对写入被结束机制打断，健康流不被附加默认 write deadline。terminal fault 的写入同样受结束预算约束；snapshot producer 收到取消后必须先退出，原拥有者再关闭 snapshot，不能在 Next 仍执行时强制关闭或提前宣布清理完成。授权约束每次准入，已准入单元可以完成，已经交付的数据不被当作可撤回。

[强锁 SDK 用例](../packages/transport/httprest/lock_client_test.go)仅接受带完整协议标记和有界 body 的普通 `{errno,message}` 授权错误；一旦出现 lockCode 或 recorded，就走完整 native envelope 校验，残缺字段不能回退猜测。丢失 Acquire 回复后，拒绝的 query／cancel 保留此前未知结果。[stream fault 用例](../packages/transport/httprest/stream_fault_test.go)拒绝重复字段、null、空值和未知 errno，旧的无 errno fault 保持 `EIO`；[SDK 发送边界用例](../packages/transport/httprest/subscribe_test.go)在初始帧与各个 Next 位置保留 typed `EACCES/EIO`。直接 SDK 的授权拒绝与 replica 观察 follower 故障后的统一 `EIO` 分别核对，都不能返回空目录或虚构的不存在。

## 打开的文件对象与标准锁

[文件句柄设计](design/server/file-handles.md)按对象身份、逐次发布、会话生存期和 advisory 锁分别验证。[公开类型用例](../packages/storage/files_test.go)拒绝无访问权限、非法创建/截断组合、不一致的 OpenNode 身份、无界会话配置、非法锁范围和 action epoch/nonce。实现缺少 FileStorage 能力时必须明确拒绝，不能重新按路径打开来模拟保留的对象。普通 Open 不获取 advisory 锁或强 S/X；FileSession 与强占有 Session 的生存期分别成立。

### 当前内容、身份与发布

[objectstore 句柄用例](../packages/storage/objectstore/files_test.go)先打开对象，再从另一入口覆盖、扩展、缩短、改名、unlink 和替换名字。每次 ReadAt 同时核对内容、大小、EOF 与节点 ID；原引用继续读取对象的当前版本，已占据旧名字的替代物逐字节不变。OpenNode、StatNode 与 SetNodeAttr 按既有身份访问，失效身份为 `ESTALE`；只读引用仍能按权限策略设置 mode 和时间；缺少读写访问，或 Session 仍有效但引用已关闭时为 `EBADF`，Session 到期或退役为 `ESTALE`。创建并打开的用例同时核对 ExpectedID、排他创建、既有 mode、初始 mode 与截断，不能只断言返回了一个引用。

范围写的交错用例暂停一份不可变对象上传，让另一描述符先完成修改，再恢复原 WriteAt；最终内容必须包含两次已完成范围修改。SQLite 的每节点 content revision 与 CAS 用于重新读取当前版本并重算本次范围，不能据此拒绝较早打开的普通描述符。另测零填充、截断、空对象 ABA、revision 耗尽与原状态保留。读取遇到已经收集的旧 revision 可以重取当前对象；当前 metadata 所指对象缺失必须 `EIO`，不能返回空内容。

文件大小上限用例分别收紧 FileSessionOptions.MaxFileSize 与 native MaxFileBytes：先 Stat，再从另一入口增大文件，随后即使只读一个字节，ReadAt、WriteAt、非零 Truncate 和 Sync 也必须在 GetBounded/Put 前以 `EFBIG` 拒绝。Truncate(0) 不读取或暂存被丢弃的旧内容，超限且不可读的对象仍能按权限清空，Usage 随之归零；强 S/X 的最终权限检查继续成立。测试分别计数对象读取与上传，不能只检查最后的 errno。

[native 发布用例](../packages/metastore/sqlite/files_test.go)分别在上传前、最终发布前和已获准的发布期间退役引用或 Session。退役必须阻止尚未取得最终许可的 WriteAt、Truncate、创建打开与身份属性修改；已取得许可的事务先完成，再交接后继权限。强 S/X proof 仍在最终发布验证，名字消失时强占有退役，普通打开引用继续保留原节点。已知 cleanup 拒绝保留 pin 以供重试，接受结果未知则保留物理所有权与错误原因。

### 无名字对象的用量与恢复

[retained quota 用例](../packages/storage/limited/files_test.go)用两个打开引用保留已 unlink 的对象，断言字节继续计入 Used，后续增长也被计费，第一次 Close 不释放第二个引用仍需要的字节。最后一次有效释放按对象当前大小结算一次，重复 Close 不重复返还；失败 cleanup、取消创建请求后到期、startup 与 Recount 都核对真实 retained 用量。Recount 与最终 cleanup 交错时必须重取一致用量，不能用只遍历可见树的方法漏掉 detached 文件。

[local store 关闭用例](../packages/storage/localstore/files_test.go)在关闭 durable storage 前退役 retained 引用，重开后核对无名字对象已清理、Used 已释放且旧名字没有重建。最后引用的记账拒绝必须使 Close 保留原错误，第二个 opener 仍以 `EBUSY` 失败；移除故障后重新 Close 才能释放物理所有权。

[retained integrity 用例](../packages/metastore/sqlite/internal/integration/integrity_test.go)将可见 rooted tree 与 detached regular file 分开验证：后者不进入目录快照，仍引用合法对象并计入 quota 与 integrity work；detached directory/root、非法标记、revision 或错误用量均失败。[恢复用例](../packages/metastore/sqlite/internal/integration/files_test.go)在独占打开时清理数据库内每个 volume 的遗留 detached 对象，即使 pending admission 已满仍完成必要清理并报告实际 OverLimit；损坏图在清理前拒绝，失败事务保持节点、对象、用量和 durable generation。[迁移用例](../packages/metastore/sqlite/internal/integration/schema_test.go)从受见证保护的旧 schema 前滚，核对已有节点与 accepted state，并在旧 schema 损坏或预算不足时保持原数据。

### flock 与 POSIX 记录锁

[advisory 状态机用例](../packages/advisory/coordinator_test.go)逐步核对共享/排他冲突、同模式重复申请、范围替换、拆分、合并、部分解锁及未来 EOF。flock 转换失败时旧锁已放弃，POSIX 转换失败保留原范围；GetLock 返回真实冲突。两种 family 互不冲突，Session 与 owner 共同确定持有者，PID 只用于诊断，普通 I/O 不参与 advisory 冲突判定。跨文件等待检测 `EDEADLK`；有界搜索无法确定时明确拒绝，不能把未知当成无冲突。

[advisory 上限用例](../packages/advisory/bounds_test.go)分别耗尽 Session、Owner、范围、pending 与 action history，核对拒绝不会丢弃旧锁、拆分失败不改变原范围、取消及历史到期能回收对应名额。阻塞申请可以在有效且持续续租的 Session 内等待，不继承强 S/X 的有限 Wait 策略。取消与授予的交错保留可核对结果；只有 Cancelled/Released 证明没有遗留授予时才能返回可重试的 `EINTR`，结果未知保持 `EIO` 和 I/O 隔离。退役失败不能先放出旧 Grant，DropLocks 只清理指定对象、Owner 与 family。

[FUSE bridge 用例](../packages/fuse/advisory_bridge_test.go)通过真实 raw callback 验证内核 LockOwner 的传递，包括 owner 为零、没有加过锁的描述符关闭、POSIX 任一描述符关闭与 flock 最后一次 Release 的区别。缺失或饱和的 raw metadata 不得继续以错误 owner 执行动作；取消必须先核对远端结果。`unknown cancellation` 分支直接断言挂载 volume 的健康检查为 `EIO`，覆盖整个挂载的失败范围。Release 中 owner 清理失败仍尝试关闭引用并保存错误，同时核对挂载整体已被隔离。

### HTTP、副本与清理所有权

[FUSE Session 用例](../packages/fuse/session_test.go)让多个成功续租跨过原始期限后继续读取同一引用，再显式停止 Session，核对旧引用已失效、调用方拥有的 storage 仍可用。续租失败不能延长最后确认期限。连续性丢失后等待后台退出，再核对整个挂载的健康检查与停止结果持续为 `EIO`，原 FileSession 的引用不能继续写入；停止会取消在途 Renew、等待 Session 退役，并让重复停止共享同一个关闭结果。挂载级终止后需要新挂载建立新 Session，native DropLocks 的 Owner 级清理继续独立成立。迟到的 Status 回复不能从收到回复的时刻重新起算租期。

native admission 用例用一个暂停的 Put 占满 Session 唯一的 data slot，先确认普通 File.Stat 为 `EAGAIN`，再核对 Renew 的 revision 前进且 Status 仍可用。另一用例暂停最终发布，验证静态能力探测可以完成，等待 contextual authority 的新 Session 创建可以取消。四类 admission 分别验证 data 的 MaxOperations，以及心跳、advisory 获取、核对与释放三个各 2 个活跃调用的分区。占满获取或核对分区后，同类调用为 `EAGAIN`，Renew 与数据访问仍能完成；获取饱和时 DropLocks 仍可释放。心跳名额也单独验证满额拒绝与取消归还，Close 在控制调用仍等待时继续前进。续租或 enrollment 不能因为大文件 staging 而变成不可退出的全局等待。

[HTTP 句柄用例](../packages/transport/httprest/file_test.go)用真实 SQLite/objectstore 验证同一对象的覆盖、unlink、替换与跨 handler advisory 协调。丢失 Open、Truncate、ACK 或 Close 回复后核对原动作，不能制造第二个引用或重新执行一次修改；Handler.Close 只退役自身登记的 Session，另一 handler 的会话仍可用。[registry 用例](../packages/transport/httprest/file_internal_test.go)覆盖 Session/action 上限、未 ACK 的 Open 在 Session 持续 Renew 时仍到期回收，以及旧窗口或已逐出的动作不能再次执行。

[HTTP 回归用例](../packages/transport/httprest/file_regression_test.go)拒绝缺失 Offset、Owner 等合法零值字段和不完整锁冲突回执；较晚到达的旧 Renew 回复不能缩短或重启已确认期限。Open 已有效果后取消返回 `EIO` 并清理未交给调用方的引用，Read、Stat、StatNode、GetLock、QueryLock 与 Status 的纯读取取消保持 `EINTR`。data history 满时 ACK 和 DropLocks 仍可清理，独立 MaxCleanupActions 耗尽则返回 `EIO` 并退役相应 Session 与 native 锁。合法的 1024 字节 body 配置仍须完成 Session 与文件调用，scoped File 保留已复制 proof，读取不携带 proof。

[副本句柄用例](../packages/storage/replicated/files_test.go)让 retained File 的读取、属性与锁控制直接核对 authority。带名字的创建和修改确认 metadata barrier；unlink 后继续操作旧对象不制造新名字或复制事件，原 Position 可以保持不变，同名替代物不受影响。流失败时 retained 控制仍须可用，不能拿 SSE 健康度代替 FileSession 生存期。[失败用例](../packages/storage/replicated/file_fault_test.go)与[admission 用例](../packages/storage/replicated/files_internal_test.go)覆盖缺失、错误或负 barrier，未返回 Open 的引用回收，已发出修改的原动作核对，以及共享 confirmation pool。MaxFileSessions 同时计入正在远端创建、仍存活及 cleanup 未确认的会话；发送前满额为 `EAGAIN`，确认 Close 才归还名额，未知清理继续占用。wrapper 不替 FUSE 启动续租 timer。

### 内核入口验收

[live file 用例](../packages/fuse/live_files_test.go)使用两个各自持有 HTTP client、metadata replica、FileSession 与 inode cache 的真实挂载点，并使用交付的 direct I/O 配置。读描述符先读过旧范围，再由另一挂载点覆盖、增长、缩短至零，逐次核对相同 inode 的内容、大小和 EOF；rename、unlink 与替换后对旧描述符写入和设置属性，原对象与名字上的替代物分别读取。WriteAt、普通写和未写字节的 O_TRUNC 都须在 Close 前从 authority 可见；相邻范围与重叠修改按实际执行次序保留结果。并发 Create/Open 验证排他创建只有一个成功者，非排他调用打开同一赢家并保持既有 mode。

同一 [live 文件用例](../packages/fuse/live_files_test.go)保留两条精确读取回归：65536 字节 `A` 覆写为 `BBB` 后，旧 fd 先 Stat 再读取，结果必须为 `BBB`；1 MiB `A` 等长覆写为 `B`，经过 1200 毫秒后在同一读挂载新开 fd，旧 fd 先读，再按 64 KiB 交错读取，两个 fd 全部返回 `B`。两者均核对大小与 EOF，不能只用一般覆写成功替代原触发顺序。

[标准锁内核用例](../packages/fuse/advisory_locks_test.go)通过真实系统调用和子进程验证 flock 共享/排他、转换与阻塞，dup/fork 共享同一 open file description，最后一个相关描述符关闭才释放。POSIX 用例覆盖 F_GETLK、范围拆分和失败转换、读写访问权限、fork 不继承、同进程任一同文件描述符关闭释放全部范围，以及其它文件/持有者不受影响。非合作读写、unlink 与另一种 lock family 仍须成功。这些测试需要真实挂载执行、每项 verdict 和残留清理回执；编译成功不能构成内核语义验收。

busy unmount 用例在真实挂载点保留打开的描述符和排他 flock，使 Unmount 失败；Done 仍未关闭，越过原始租期后续租继续、引用可读写、另一挂载点仍被 flock 拒绝。关闭描述符后实际 Unmount 与 Wait 成功，Session 只关闭一次，另一持有者才可取得锁。这个用例分别核对内核卸载结果和 native 会话状态，不能用一个模拟的关闭回调代替挂载生命周期。

外部 kernel teardown 用例在挂载 Session 中额外打开一个未交给内核的真实 HTTP File，unlink 后先确认其字节仍被计费。通过外部 fusermount 拆掉本用例拥有的挂载，随后等待 Done/Wait，核对 Session 关闭一次、额外引用失效、detached 字节归还。这个引用从未进入内核，因此它的释放确实依赖整个 Session 清理，不能碰巧由逐文件 RELEASE 完成。

## 请求中断与修改结果

[请求中断](../.agents/notes/implemented/bug-fix/2026-08-22-eio-from-a-freshly-mounted-mountpoint.md)按阶段验证，不能只测一条 `context.Canceled → EINTR` 映射。错误分类用例覆盖直接与包裹的取消、deadline、已命名 errno、未知错误、独立故障和取消的两种 join 顺序，以及当前节点的权威分类；`ErrnoOf(nil)` 为 0，`ErrnoNameOf(nil)` 为 `EIO`。

SQLite 只读用例覆盖查询取消、预算回调取消、回调成功后才取消、`database/sql` 自动回滚产生的直接 `ErrTxDone`，以及真实查询和 cleanup 故障。SQLite code 9 只有与实际已取消的读取 context 同时出现时才按取消解释。每种情况都验证原因保存和最终 errno；未知 commit 与 poison 仍为 `EIO`。

[Forget 事务用例](../packages/metastore/sqlite/objects_test.go)分别取消尚未准入与已准入的批次：前者以 `EINTR` 退出并保留原记录，后者完成真实 Commit / Accept，不能因调用方取消捏造失败。[SQL 执行中取消用例](../packages/metastore/sqlite/objects_test.go)在 `objects` 的 DELETE 与 `database_state` 的 UPDATE 已进入 SQLite 时取消调用方，断言没有原生 rollback、garbage 记录已删除、generation 恰推进一次，读取和 Close 仍成功且 SQL 连接已释放。真实提交、见证或回滚故障仍须保持 `EIO` 与失败隔离。[关闭交错用例](../packages/metastore/sqlite/objects_test.go)让已经接纳的 Forget 在授权方退役后发生真实 driver COMMIT 故障，断言 Close 保留该原因、重复 Close 返回相同缓存错误，另一原生排他 opener 仍以 `EBUSY` 失败。[组合关闭用例](../packages/metastore/sqlite/objects_test.go)验证已准入的整批 Forget 完成、待准入者以 `EINTR` 退出，并重开同一数据库检查 garbage 记录。

HTTP 用例区分 `Do` 前取消、已发出的只读请求与已发出的 mutation，断言是否到达服务端以及最终 errno。真实网络错误不能泄漏 `ENOENT` 等底层 errno；成功修改后的 barrier 取消仍为 `EIO`。FUSE 复合操作分别在任何效果之前及已有 volume 或句柄效果之后取消，验证后者不会返回暗示整个操作未执行的 `EINTR`。

[关闭清理用例](../packages/fuse/completion_test.go)覆盖请求值保存、关闭线程取消被隔离、默认 30 秒与显式 FlushTimeout、负值拒绝和较早请求 deadline 保留。预算在等 close mutex 之前起算，等待后只剩原 deadline 的余额；一次引用关闭只调用一次底层 Close，并发或重复关闭共享原结果，真实失败不重试，晚于预算返回的已确认成功不被改写成失败。Flush 清理 POSIX owner，Release 清理最后的 flock owner 并关闭引用；它们不发布文件内容。Fsync 调用 File.Sync 验证已发布内容的健康与持久屏障，继续响应请求取消，不能顺带退役引用。FlushTimeout 不构成内核 Unmount 或 Mount.Wait 的耗时上限。

[范围写与截断用例](../packages/fuse/space_cancellation_test.go)在 retained File 边界注入直接和包裹的取消、deadline、`EINTR` 与独立故障，核对原内容不变，下一次调用仍得到新的权威 `EDQUOT`。Space 被调用即让测试失败，确保普通写入直接依赖 native quota。真实配额已满时增长和扩展失败，缩短成功后能立即使用释放的空间；已确认成功之后才到达的取消保持成功。已有副作用或结果未知不能伪装成可安全重试的 `EINTR`，与取消合并的独立故障也不能被较轻的分类覆盖。Statfs 独立调用 Space 报告容量。

[读取信号探针](../packages/fuse/interruption_linux_test.go)在实际 HTTP 读取已进入服务端后，向执行系统调用的子进程线程发送 `SIGUSR1`，同时观察原 FUSE 与 HTTP 请求 context 被取消。原始 `Fstatat` 必须得到 `EINTR`，普通 `os.Stat` 依靠标准库处理中断后成功，两个调用方随后都须读到完整内容。[关闭与写入探针](../packages/fuse/interruption_write_linux_test.go)分别暂停 owner 清理与 retained WriteAt。Close 收到中断后清理 context 仍有效，普通关闭成功，Write 加 Close 总计只发出一次 WriteAt，随后重新读到已确认内容；不重试已经消耗的描述符。原始 Write 在进入下游前得到 `EINTR`，Go Write 重试后由实际 quota 以 `EDQUOT` 拒绝；两条路径分别核对调用次数和目标仍为空。

信号探针独立于纯映射测试，也不把 SIGURG 当作所有历史失败已经证实的原因。四项历史 `cmd` 用例曾分别以 `-race` 固定采样 100 次，共 400 次；其失败记录和结果只对应当时实现，不能充当当前文件句柄路径的验收。当前验证同样不增加应用层 `EIO` 重试，不关闭异步抢占，不预热被测操作；每条阶段断言取得自己的执行证据，不以重跑整个无关命令套件代替它。

## 显式文件锁

[文件锁设计](design/server/file-locks.md)的验证分为共享 volume 契约、authority 状态机、native 最终发布、持久恢复和 HTTP 编码。`X` 证明本次修改的权限，内容版本比较另有自己的义务；普通打开或读取不会隐式取得租约，读取成功也不能充当租约仍有效的断言。

### 保护、身份与有界历史

[共享契约](../packages/storage/lockcontract/contract.go)在 SQLite/objectstore 与 HTTP volume 上使用同一套操作。用例覆盖 `S/S` 共存、`S/X` 与不同 Owner 的 `X/X` 冲突，匿名修改与只有 `S` 的持有者自身修改都被拒绝；带有效 `X` 的 Write、显式 mode/atime/mtime、Remove 与 Rename 才能修改受保护目标。普通读取在 `S`、`X` 下仍可完成，无活动保护冲突时匿名修改继续可用。每次拒绝后从 storage 重新读取内容，不能只检查错误码。

身份用例让祖先目录改名、内容原子替换、目标覆盖、删除与同名重建真正发生，验证源文件保持 ResourceID、被覆盖目标退役、旧 proof 不会指向新节点。覆盖式 Rename 必须同时提供受保护源与目标的 `X`；多余或不相关 proof 也必须失败。Scope 用例修改调用方原 proof slice，断言已创建的 scope 不变；显式匿名 scope 不继承外层权限。已解除或到期的 proof 即使没有竞争者，也不能退回匿名执行；空 SetAttr 与自身 Rename 同样验证它。共享契约与 SQLite 适配器另覆盖根、目录和缺失路径不能作为申请目标。

[authority 契约](../packages/locking/authority_contract_test.go)用可控制时钟分别断言不可变动作回执与当前 Grant 状态：冲突或 AlreadyHeld 的已记录拒绝在竞争结束后仍原样重放，成功 Acquire 的原回执在 Release、Expiry 或 TargetGone 后不变，重复 RequestID 携带不同意图必须拒绝。Renew 不缩短已有期限，重复 Renew 不延长第二次，持久水位准备期间到期的 Grant 不得复活。排队写者先于后来读者；排队超时、已授予后取消、尚未到达的 Acquire 被取消，以及取消后迟到的原请求均有确定性交错。控制请求 context 结束不会撤销已经受理的 Pending 意图。

[容量用例](../packages/locking/contract_capacity_test.go)分别耗尽 Session、已消费 enrollment ticket、Owner、Resource、Action、Grant、全局及各层队列、proof 数与请求字节预算，不能用一个较紧的上限代替另一个。活动 Session 上限独立于 ticket 到期；ticket 重放不会创建第二个 Session，原 Session 已关闭也不能借旧 ticket 创建另一个。活动 Owner 的动作回执不被逐条逐出，满历史仍保留 Release、已知 Cancel、RetireOwner 与 CloseSession 的清理路径；新的未见 Cancel 只有取得有界 tombstone 才能确认取消，否则为 OutcomeUnknown。未见动作查询同样不报告“未授予”。NotAdmitted 不产生虚构回执；Renew 在历史满时保留此前已确认期限。引用到期释放 Resource 容量但不缩短已有 Grant，Owner/Session 退役后旧能力不能重新执行动作。

[队列生命周期用例](../packages/locking/resources_internal_test.go)分别移除队首、中间和队尾，核对幸存项顺序、authority/Owner 计数及底层 slice 空闲位置均不再保留 action 指针；Owner 退役后整个 backing array 不能继续引用其历史。fence 使已有资源 worker 退出并保留 Pending，随后禁止重新启动 worker；维护循环仍按每个 Wait deadline 结束等待，包括先于队首到期的项，逐项清除队列引用而保留动作历史。到期清理不解除 fence，QueryAction 与 Publish 继续返回 Unavailable 并保留原始原因，不把无法核对的结果说成可用的回执。

[后台清理失败回归](../packages/locking/contract_regression_test.go)在手动时钟中确认目标到期 timer 已注册，并暂停它的返回；推进时钟后才放行，使后台路径实际观察到到期。仅看到曾经注册过的 timer，不能证明当前等待仍使用它。用例继续断言后台 `Forget` 失败使授权方停止发布，且 `Close` 不再次尝试结果不明的清理。

### 最终发布与观察次序

[objectstore 用例](../packages/storage/objectstore/locking_publication_test.go)在真实组合的 Objects.Put 边界暂停 staging，随后推进时钟或取得新的冲突 Grant；恢复上传后必须在最终发布拒绝旧 proof 或匿名修改，原内容保持完整。另一文件的修改必须在暂停期间完成，证明慢上传没有占用它的发布权。SQLite 另让 staging 后的路径指向不同节点，断言 [Commit 根据实际目标验证](../packages/metastore/sqlite/publication_test.go)，Reserve 成功不代表最终发布已经获准。

authority 的[发布交错用例](../packages/locking/contract_concurrency_test.go)暂停已经取得最终许可的 native 回调，要求同资源的 Release 等待，而另一资源的发布仍可前进。Grant 查询用例另推进时钟越过租约期限，查询必须等未完成发布结束后再报告 Expired。SQLite 保留自身 writer / health 串行化，其发布用例断言新 Stat 等待发布、已捕获快照仍读到旧版本，以及发布后新视图取得新大小和对象 key。objectstore 的已捕获 immutable object 读取在 Get 暂停期间允许后续授权和发布，恢复后仍返回旧的完整内容。这里验证授权与效果的次序，不要求已获准的 commit 或同步在到期时刻前物理返回。

失败用例区分准备拒绝、已知效果和结果未知。SQLite 准备失败保留 reservation 与 Grant，删除与目标覆盖退役真实受影响身份；[记账收尾失败](../packages/metastore/sqlite/publication_test.go)与[持久确认失败](../packages/metastore/sqlite/publication_test.go)使 volume 和 authority 停止发布，错误原因仍可核对。复合修改的部分效果由 FUSE 阶段用例验证，已有副作用时不返回暗示整个操作未执行的 `EINTR`。Close 排空在途发布与持久水位准备，关闭失败保留原因和必要所有权，不能对可能已经复用的描述符重试 Close。

[native quota hook 用例](../packages/storage/limited/publication_test.go)在真实 volume 外注入最终发布结果，单独核对根据最终目标计算的旧、新大小：增长先预留，只有已知应用才返还缩减或删除释放的字节；未应用的失败返还增长预留并保留原用量。用例暂停 staging 后改名祖先目录并重建原路径，再核对两份内容、即时 Used 与 Recount，避免提前 Stat 的大小被用于另一节点。已应用但回复失败仍按实际效果结算；[未知结果或 unwind/settlement 失败](../packages/storage/limited/publication_uncertainty_test.go)保守保留预留，并使后续修改、Space 与 Recount 失败，已在 staging 的调用也不能越过该状态。嵌套 quota 另验证准备被拒绝后各层预留均已归还。只有实现 native 最终发布记账能力的包装层能同时暴露锁服务；这些断言不把 opaque 第三方 storage 的路径采样包装解释为具有同样保证。

[到期缩减用例](../packages/storage/limited/lease_quota_test.go)使用真实 SQLite、objectstore、内存 Objects 与外层 limited，metadata allowance 设为零以使外层独立承担记账。在 Objects.Put 暂停缩减写入后，立即核对旧字节仍占满配额且另一写入为 `EDQUOT`；推进时钟使 proof 到期，恢复后要求 `StaleGrant`、原内容不变，Space 和 Recount 都仍报告原用量。随后有效的匿名缩减才释放差额，另一文件必须能够恰好用完该差额，再由 Recount 核对总用量。

### 恢复与独占所有权

[SQLite 恢复用例](../packages/metastore/sqlite/lock_recovery_test.go)分别在 Prepared 提交、见证写入前后与 Accepted 完成处中断，重新打开后核对恢复出的最大时长与 Prepared 已清除。拒绝用例逐项构造缺失记录、缺失或回退见证、单边状态回退、错误身份、部分 Prepared、跳代、下降的时长与错误字段类型，断言 `EIO`。并发提高水位必须保持单调，取消或持久失败不能确认提高成功，注入的见证错误保留原因链。已有 v3 数据库的迁移先核对原 accepted witness，再改变 schema。

[native lease anchor](../packages/metastore/sqlite/internal/nativelease/anchor_test.go)覆盖显式初始化与重开、匹配的中断初始化 intent、数据库和状态身份绑定、缺失或损坏见证、复制或替换证据路径，以及 lifetime ownership。xattr、flock、rename、文件及目录 fsync 的能力探测在初始化和重开时验证错误与误报成功，known remote filesystem 在修改前拒绝，中断的探测与 stage 只清理可证明属于本次状态的残留。恢复期从实际取得独占所有权的单调时刻起算，较小的新配置不能缩短已记录时长；这些证据位于 SQLite-backed 存储自身的持久边界。

[SQLite 拥有者用例](../packages/metastore/sqlite/locking_store_test.go)与[跨进程所有权用例](../packages/metastore/sqlite/locking_store_test.go)验证 raw opener 的共享 flock 与授权方的排他 flock 在同进程、跨进程中双向排斥；数据库绑定后不能通过 raw constructor、路径别名或并发拥有者绕过保护。恢复配置和启用入口必须验证真实排他拥有者与 native anchor；重开另一个已有 volume 时仍须读取同一数据库级最大时长并等待完整恢复间隔，选择不同名字不创建新证据。local store 继续验证单个 volume 的根绑定，不能省略锁配置或丢弃见证来恢复为空的 authority。真实子进程在租约已确认后遭 `SIGKILL`，由新的进程或组合 store 重开：内容仍完整，恢复期间读取和状态查询可用，修改为 `EAGAIN`，较小配置不能缩短此前水位；旧 Owner、Grant 和动作不能在新 authority 下重放执行。故障注入与真实退出分别证明具体持久边界和进程生命周期，不把它们当作断电或设备缓存验证。

### HTTP v3 与客户端

[server 用例](../packages/transport/httprest/lock_server_test.go)验证 `/v3/` 协议标记和旧版本拒绝，以及匿名与 scoped 调用都抵达同一个执行保护的 volume。Scope header 的空值、重复值、错误 base64url、缺失或重复成员、未知字段、超长值和错误使用位置必须在修改前拒绝，随后 Stat 证实目标未创建；读取与控制操作不接受 mutation scope。能力值只在 body/header 中传递，URL 与错误诊断不能泄漏它们。[Scope 用例](../packages/transport/httprest/lock_scope_test.go)逐一验证所有 mutation、WithBarrier 与无效果 mutation 保留复制后的 proof，读取不发送它，匿名 handler 不继承被包装客户端的权限。

[副本转发用例](../packages/storage/replicated/lock_service_test.go)分别把基础 replica 和 scoped 视图交给真实 HTTP handler。代理查询须找到原授权方的 grant，匿名修改被拒绝，显式 proof 修改成功后本地 replica 立即可见；经代理 Release 后，再从原授权方确认 Released。随后匿名写与普通读仍可用，关闭代理 HTTP 服务后底层 replica 仍能写入，内容由原服务端重新读取核对。

[client 编解码用例](../packages/transport/httprest/lock_client_test.go)区分缺失字段与合法零值，覆盖枚举、整数毫秒、溢出、嵌套意图、截断和不一致的成功或错误结果。HTTP 422 的已记录拒绝必须同时返回原 ActionResult 与 typed error；`lockCode`、errno、recorded、意图、Grant 与回执 variant 不匹配时按协议错误处理，未受理的失败没有虚构 receipt。丢失 Acquire 回复后使用原 Owner、RequestID 与 ResourceRef 核对结果，迟到的原请求不能越过已确认 Cancel，也不能用新身份重新申请。错误归类继续保存原 context 或网络错误原因。

[历史窗口用例](../packages/locking/history_test.go)与 wire 用例推进时钟后核对 Resolve 的 `HistoryExpiresMillis` 已报告延长后的 Session 历史期限，同时区分资源引用本身的 expiry。响应缺少 historyExpiresMillis 必须解码失败，Acquire 的嵌套 ResourceRef 与 receipt 也保留该必需字段。其它成功控制与带回执的已记录拒绝均核对当前历史期限；Acquire / Renew 重放在等待查询前不能先延长期限，再因取消返回一份没有期限的错误。测试同时核对实际延长与返回的期限，不能只断言内部 Session 尚未到期。

Pending 用例先占满 authority 的申请队列，随后确认 HTTP control admission 已归还，Renew、QueryAction、Cancel 与 Release 仍可完成；Acquire 的 Wait 是受理后意图的有限寿命，不是 HTTP 长请求的占用时间。另一组用例同时耗尽 server body/response 与 client response admission，enrollment 和状态控制仍须完成。控制 admission 的配置用例分别检查默认值、非法上限和有效配置的传递；client 名额满时，拒绝必须发生在发送前，并保留 `recorded = false`，不能声称动作已有结果。

时间断言使用不同于墙钟的 authority ticks。[毫秒回归用例](../packages/locking/contract_regression_test.go)构造到期 1.1 ms、当前 0.9 ms 的边界，要求仍 Active 的 Grant 报告 `RemainingMillis = 0`，不能把分别取整后的两个时间相减得到 1 ms。客户端本地提示从请求发送时刻加 remaining 起算，零余额不获得新期限，Expired 不产生有效期限；原回执的 TTL、deadline 与 revision 不能重置当前保护。控制操作在 dispatch 前取消为 `EINTR`；已 dispatch 的状态修改即使因取消丢失回复也为结果未知的 `EIO`，只读 Query 的 POST 仍按读取语义分类。上述用例不以 HTTP method 判断操作是否已有副作用。

## 元数据副本的读写交接

五个副本确认、容量及 Close 用例使用[事件交付门](../packages/storage/replicated/harness_test.go)：真实 bootstrap 完成后才 arm，在转交响应字节前等待，用例先确认 entered，再执行对应取消或 Close，最后在原来的到达／完成位置释放。请求取消保留原 context 错误；两秒确认 grace、五秒／十秒容量 grace、二十毫秒 waiter 观察、一秒 Close 界限和原结果断言保持。慢 snapshot、replay 与全负载可见性继续使用各自原有条件。

[确认 bookkeeping 用例](../packages/storage/replicated/bookkeeping_test.go)确定性地使发送回调返回成功 barrier、确认前已发生 follower 失败，分别覆盖 barrier 尚未到达和已经到达；核对发送回调仅调用一次、PathError 的操作／路径、EIO 及未能确认已发生修改的诊断，并要求 active／waiter 归零。原有真实 HTTP 双故障用例保留，不能把竞争中哪一条错误先返回当作稳定覆盖条件。夹具边界与取舍见[测试工作量决定](../.agents/notes/implemented/testing/2026-09-09-scale-test-work-to-its-assertions.md)。

[副本读写门](../.agents/notes/implemented/bug-fix/2026-09-07-let-replica-writers-progress.md)分别验证类间次序和真实入口。门的确定性交错用例先证明写者已登记，再放开旧读者；读者批次必须在唤醒前保留名额，尚未获调度的读者也不能被下一写者越过。覆盖批次内读者的正常进入与取消、后来读者进入下一批、多写者中只撤销本次取消，以及取消最后一个等待写者后重新放行读取。

真实 `Replica` 用例覆盖 `Apply` 与 `Reseed` 等门取消、等待 commit gate 时释放外层名额，以及 `Stat`、`List`、`ListBounded` 在 reseed 后排队时的取消；失败的 `ListResult` 不能暴露已保留前缀。SQL 读取名额另验证与 reader pool 容量一致、名额耗尽时调用不进入阶段、交接后取消归还名额，以及 `Position` 与写者不消耗 SQL 读取名额。完整、回滚、无效 row 与提交失败的 reseed 都同时观察树和 `Position`，并验证重复 `Close`；读者批次还必须在多个真实 `Apply` 的积压之间得到执行机会。取消用例保留现有错误分类与 context 原因。

压力与可见性使用不同的时间判据。[SQLite 压力用例](../packages/metastore/sqlite/replica_test.go)先占满现有 reader pool，再启动 128 个公开 `List` 调用。用例在门的互斥保护下确认只有与 pool 容量相等的读者持有共享访问，其余调用仍在阶段外等待 SQL 名额；确认 `Apply` 已登记等待后才释放 reader pool。4096 文件的读取循环保持到写者结果返回，随后停止并取消本用例拥有的读者 context，join 全部读者并检查门空闲、读取 permit 为零。只有停止标志、私有取消原因、错误链中的 `context.Canceled` 和 `EINTR` 分类同时成立，才忽略预期退出；其它错误仍失败。30 秒写者截止时间、Applied、Stat 模式与 Position 断言保持，不能提前撤掉负载。结果后的清理不属于写者延迟，局部测量见[减少无用测试工作](../.agents/notes/implemented/testing/2026-09-09-reduce-test-work.md)；该死锁上限不代表复制延迟。

[真实 HTTP/SSE 全负载可见性用例](../packages/storage/replicated/replica_acceptance_test.go)使用 4096 文件与 128 个持续列目录的读者，以一秒为独立写者提交后另一客户端观察到新元数据的上限。用例先观察每个读者都成功完成列目录，再提交变更，并断言计时窗口内列目录继续推进；调用进入某个回调不构成负载成立的证据。

这项验收只在测试文件上使用 `rfs_acceptance` build tag，生产实现没有对应分支。CI 在其它包的并行测试与 race suite 之前，以正常构建、串行执行整个 `packages/storage/replicated` 包，使用 `assert-every-test-ran.sh`，不按 `-run` 缩小集合。原有的一秒 HTTP/SSE 可见性用例仍在默认测试集合中并接受 race 检查；阶段交接、取消与 4096 文件压力用例也保留默认 race 覆盖。全负载一秒断言和 race 下的死锁判据分别验收，不互相替代。

```
.github/scripts/assert-every-test-ran.sh -tags rfs_acceptance -count=1 \
  -p=1 -parallel=1 -timeout 3m ./packages/storage/replicated
```

## 测真实入口

「真实入口」指交付出去的那个形态：真实挂载的挂载点、构建出的二进制、被第三方 import 的 package。

库内部直接调用能通过，而挂载后不行 —— 这类失败只有真实入口能暴露。作为库被链接的那条路径同样要测：不注册信号处理、不写标准输出、不调用进程退出。**这一条要两种检查一起用**：把测试二进制重新当作一个普通程序执行，断言它真实的描述符上什么都没有；以及在源码的语法树上静态检查同一批禁令。两者抓的不是同一类东西 —— 运行时那条抓的是依赖替我们打印的东西，静态那条抓的是没有任何测试到达的代码。

构建出的 `remote-fs-server` 二进制使用 local store 与 Azure Blob 两种 mode，覆盖启动、HTTP 访问、停止、常见命令行拒绝，以及 local store 的跨进程重启持久性与独占锁竞争。需要证明独占所有权跨进程生效时，第二个真实进程直接尝试打开同一份存储，不以首个进程的日志代替事实。

[文件锁二进制用例](../cmd/locks_test.go)在两种 mode 中经过真实 HTTP 验证 `S/S`、匿名与同 Owner 的 `S` 修改拒绝、`X` 修改、改名后的身份保持、删除重建和不相关 proof 拒绝，再重新读取目标与未触碰文件。重启用例在已确认 3 秒租约后杀死 server，以 500 ms 配置重开，要求恢复剩余时长仍大于 500 ms、期间读取成功而修改为 `EAGAIN`；恢复后修改可用，旧 proof 为 `ESTALE`，旧动作查询为 Retired。Blob 路径使用同一 Azurite 依赖，local store 的结果不能代替它。

[文件句柄二进制用例](../cmd/file_handles_linux_test.go)分别在 localstore 与 Azure 中验证已打开 fd 的当前读取、同步修改，以及 rename、unlink、同名替换后的原对象保留；排他 advisory 通过真实挂载协调。重启用例紧接 WriteAt 的成功返回终止服务端，以这次写入本身的确认验证持久性；Sync 的健康与持久性检查由独立场景验证。重启后旧引用明确失败，已确认字节保留，新挂载可以建立新的引用与取得 EX，不能按旧能力重新执行。

[元数据缓存用例](../cmd/replicated_test.go)的 `TestWalkingAMountedTreeCachesNamesAndConfirmsIdentityAttributes` 遍历具名节点并检查缺失名字，要求没有具名 Stat/List 请求，同时必须观察到权威身份属性请求并记录实际次数；周期文件会话续期可交错，其它数据或修改请求均失败。目录改名只允许一个 rename 请求，加上身份属性和续期，子树 inode 保持不变。计数起点等待已完成的文件关闭确认，避免此前异步 Release 混入遍历操作。

[锁配置用例](../cmd/remote-fs-server/lock_configuration_test.go)验证容量和期限逐项传入 authority，`-initialize-lock-state` 是显式动作，非法配置在 listener 或状态初始化之前拒绝。`-dir`、`-lock-state-root`、目录预算与 CLI quota measurement 参数作为未知 flag 拒绝，目标 local store 保持空目录。[状态用例](../cmd/remote-fs-server/status_test.go)检查 ready、recovering、unavailable 和各项计数，不输出 Authority 能力材料；状态失败不拼接部分容量数字，不可用的 authority 不宣布就绪，缺失 status 能力的 volume 被关闭且关闭错误保留。这些包内断言验证参数与 lifecycle wiring，跨进程结论仍由二进制用例提供。

其余聚焦的 server 入口行为在 `cmd/remote-fs-server` 包内验证：配置解析与默认值、两种 storage mode 的打开路径、READY 与 signal ownership 的顺序、SIGHUP 的 metastore-backed status、SIGINT／SIGTERM 的 admission 停止与 handler 排空，以及 partial-open 或 shutdown failure 后的资源释放。pending、reader、integrity、sweep、snapshot-frame 与 subscription 参数到达各自组件，local waiting-operation 参数仅用于 local store，invalid bounds 在 listener/root mutation 前拒绝。sweep 用例拒绝非正 interval/batch 与超过 `MaxSweepBatch` 的 batch；write-bound 用例分别验证 local object 上限、Blob 5000 MiB 上限与 pending-byte threshold 的精确边界和超限拒绝。两种 status 都断言打印 effective reader/integrity-record/name-byte limits，local status 另打印 waiting/active operations。status 阻塞时，终止仍先停止 HTTP admission 并取消 status，handler 排空期间保持 native 所有权；不响应取消的 status 只能在 HTTP shutdown 后参与等待。这些用例直接调用命令内部的 opener 与 lifecycle helper；它们验证同一条命令代码路径，不构成已构建二进制的进程边界证据。

`cmd/remote-fs` 包内测试同样区分入口层次：mutation-confirmation 与 client frame flags 的默认、help、invalid-before-network 和 forwarding 直接驱动 `run`、dial/replica helper 及真实 HTTP stream；构建出的 mount 二进制端到端用例仍走默认配置。前者证明 command wiring 与复制路径，后者证明交付程序的进程、信号与挂载边界，结论不能互换。

独立 HTTP server 的资源用例使用真实 TCP listener：占满 accepted-connection 名额后底层 `Accept` 不再前进，connection 的单次与重复 `Close` 只释放一份名额，关闭饱和的 listener 会唤醒正在等待的 `Accept`。两种 mode 都要求至少两条 connection，并在取得 listener 前拒绝非正 timeout。只发一部分 header 的连接在 `ReadHeaderTimeout` 内被关闭，keep-alive connection 超过 `IdleTimeout` 后被关闭；同一用例断言 request-wide `ReadTimeout` 与 `WriteTimeout` 保持为零。shutdown 用例覆盖已经存在和尚未被 tracker 观察到的 `StateNew` connection，确保 stopping state 会关闭 late notification，不把退出安全性押在 header timeout 上。

## 每次改动必须带什么

**任何非平凡改动都要在同一次改动里新增或更新测试。** 判据与 Agent Note 相同。

- 改了行为 → 聚焦测试
- 改了跨包契约 → 契约两侧各有测试
- 修了缺陷 → 一个**在修复前会失败**的测试
- 改了错误路径 → 测那条错误路径

**错误路径不是可选覆盖。** 这个系统最重要的保证 —— 不可达即报错、不静默丢数据 —— 全都活在错误路径上；只测成功路径等于没测那些保证。

其中一条不能推迟：**够不到 volume 时，不得回答一个读起来像事实的答案。** 不是「文件不存在」，不是空目录，不是一份编造出来的属性（R-ERR-1、R-ERR-2）。十一个操作各自解析自己的失败，因此每一个都要各测一次。[不可达端到端用例](../cmd/endtoend_test.go)先确认挂载实际使用的是 `*replicated.Storage`；关闭服务端后，沿用一秒 `settled` 等待，直接对这份副本执行根路径 Stat，直到它返回 `EIO`。inode 属性的权威 StatNode 请求可能在日志跟随器作废副本前就失败，不能代替这项前置条件。副本已观察到断流后，十个 OS 结果断言各执行一次，继续检查 `EIO` 并拒绝空目录或虚构的不存在。

## 对象存储后端分别使用真实基底

[lockcontract](../packages/storage/lockcontract/contract_test.go)与 [objectstoretest](../packages/storage/objectstore/objectstoretest/objectstoretest_test.go)在各自测试二进制中运行真实 SQLite authority 或 memory Objects，并核对非零 fixture 数与逐项清理。故障探针执行同一测试二进制中的缺陷替身，必须命中匿名修改或 create-only 的指定失败断言；子进程 profile 不导入覆盖率统计。[memory](../packages/storage/objectstore/memory/memory_test.go)另核对有界读取的精确边界、取消和返回数据不共享存储字节；[Azure Blob](../packages/storage/objectstore/azblob/azblob_test.go)核对连接字符串、读取 framing 与对象缺席的服务错误，[错误分类](../packages/storage/objectstore/azblob/errors_test.go)保留 service cause，并防止 container 缺席或独立故障被改写为对象不存在。

### Azure Blob

`packages/storage/objectstore/azblob` 对一个真的 Blob 端点跑，那个端点是 Azurite。它定义在 [`deployments/azurite.yml`](../deployments/azurite.yml)：本地 `make azurite` 起、`make azurite-down` 停；CI 的两个 job 各自在依赖 Blob 的测试之前启动同一模拟器，并等待它能够回答请求。挂载 job 的真实 Azure 二进制锁与重启用例依赖该端点，因此启动步骤位于对拍和端到端测试之前；后面的全 module 覆盖率闸门继续使用这份模拟器。

**够不到模拟器时这一层失败，不跳过。** 依赖缺席是一个必须报出来的事实，不是一个可以让用例自己消失的条件。

模拟器的版本和 SDK 的版本是一对，不是两个独立选择：Azurite 每个 release 都会抬高它接受的 `x-ms-version` 上限，超出上限的请求被答以 400 InvalidHeaderValue 而不是被服务。所以那份定义钉住具体的 tag 而不是 `latest`，理由写在 azblob 的 package 注释里。

### SQLite metastore 迁移

SQLite 的测试按对应生产模块归组：[根包公共契约](../packages/metastore/sqlite/contract_test.go)继续通过公开 metastore 接口运行；既有黑盒用例和共享测试辅助代码位于 [internal/integration](../packages/metastore/sqlite/internal/integration)，golden schema 与历史数据库 SQL 位于它的 [testdata](../packages/metastore/sqlite/internal/integration/testdata)。事务、发布、关闭与副本协调的白盒故障注入保留在根包，原生 lease anchor 测试随 `internal/nativelease`，错误分类的局部测试随 `internal/sqlerr`，[身份查询计划用例](../packages/metastore/sqlite/internal/dbstate/identity_test.go)随 `internal/dbstate`。[snapshot 计划用例](../packages/metastore/sqlite/snapshot_test.go)留在根包，直接检查生产 `pageQuery`。验证整个 SQLite 实现时使用 `./packages/metastore/sqlite/...`，包含这些测试归属下的原有用例。

SQLite 的包内直接用例按各模块持有的边界核对结果：

| 包 | 主要断言 |
|---|---|
| [sqlite](../packages/metastore/sqlite) | 公共构造与配置传递，accepted state 的检查和 checkpoint，advisory 共享及退役，Replica 各类变更的身份与回滚。 |
| [sqlvalue](../packages/metastore/sqlite/internal/sqlvalue) | SQL 类型与整数值一致，时间和 key 表示保真，精确影响行数及溢出边界。 |
| [sqlerr](../packages/metastore/sqlite/internal/sqlerr) | 真实 SQLite 错误、取消与独立故障的分类，原因链及不确定持久结果的优先级。 |
| [dbstate](../packages/metastore/sqlite/internal/dbstate) | 身份和 generation 只随调用方事务发布，拒绝 sequence／高水位损坏与耗尽，启动核对 accepted lineage 和 WAL。 |
| [schema](../packages/metastore/sqlite/internal/schema) | 迁移、树、对象归属和用量验证，work/name-byte 精确边界，拒绝与 detached 回收失败时整笔事务回滚。 |
| [changes](../packages/metastore/sqlite/internal/changes) | 变长 payload 加载前预算，页边界与 predecessor 链，Record 与 tail 的事务一致性，按数量、年龄和保留下限 trim 且不影响其它 volume。 |
| [nativelease](../packages/metastore/sqlite/internal/nativelease) | evidence 字段边界、共享与排他 native ownership、绑定和文件身份核对；验证拒绝不释放仍持有的锁，Close 缓存结果且不重试可能已复用的 descriptor。SQLite 句柄关闭不确定时的所有权保留由根包用例验证。 |

#### SQLite 测试准备与隔离

[匿名发布拒绝矩阵](../packages/metastore/sqlite/publication_test.go)按 S／X 各复用一份真实 fixture，九种操作各自保留调用前后 List、Space 与 Conflict 断言。每项开始时还对比该 fixture 的初始列表和配额，不能把前一项污染当作下一项的起点；18 个 leaf 和次序保持。复用只属于这个拒绝矩阵，不扩展到其它有状态 publication 或恢复用例。

根 metastore 契约由顶层拥有一个真实文件数据库，每个子用例使用唯一的 volume／neighbour 对，仍按原 allowance、默认选项与子用例 context 打开和关闭独立 Store。顺序隔离回归确认前一例已经关闭，新例的目录、snapshot、配额、对象队列和日志起点干净，且新例修改后前一例的元数据、配额、对象状态和双方日志不变；共享数据库 identity 与高水位保持合法。

邻居在每次被测修改前用唯一 ModTime 更新既有根，发布真实 Modified 日志并消耗全局 position；其目录不增长。局部回归核对两边日志、稀疏的被测 position、含并发调用的唯一时间值，以及邻居根身份、模式和空目录。间隙发布失败仍使测试失败，不能把稀疏序列换成只在单个 volume 上成立的连续序列。

索引准入用例在原有事务内用一条递归 CTE 准备 20,000 条 referenced object，保留精确 key、volume、state、size、NULL digest、时间字段和插入顺序，并检查 RowsAffected。生产 EXPLAIN 与实际 Reserve 的断言继续执行，不能用减少行数缩短准备。

当前 schema 的 volume 与 detached 完整性拒绝矩阵各自建立一次健康种子，种子仍由真实 Open 和公开 mutation 产生。全部 Store 和 raw handle 成功关闭、WAL TRUNCATE checkpoint 的 busy/frames/checkpointed 均为零之后，才保存完整主库字节和 fixture 元数据。每例写入新的私有 `0600` 文件，持有独立 inode、连接与可变状态，再执行原来的 detach、损坏及 Open/ObjectStatus 顺序。

复制机制同时验证健康与隔离：一份副本的公开写入可读，另一份副本通过 Open/ObjectStatus 且看不到该写入；顶层 cleanup 核对种子字节未变。种子不跨顶层测试共享，也不保留共享活句柄。迁移、WAL、恢复、lease、文件身份和跨进程用例保持原有准备路径；不能复制掉它们要验证的状态形成过程。选择与局部匹配测量见[减少无用 SQLite 测试工作](../.agents/notes/implemented/testing/2026-09-09-reduce-test-work.md)。

#### 迁移与完整性断言

迁移用例从独立手写的 v1/v2 数据库开始，不用当前 migration 反向构造历史。只含 referenced object row 且结构、计数、`sqlite_sequence` 与日志 tail 一致的旧库必须前滚到当前 schema；含任何 non-referenced object row 的 v1/v2 库必须以 `EIO` 拒绝。这条用例同时防止旧的零字节 pending 记录绕过当前 byte threshold，以及旧的 time-derived garbage 被新清扫器误当作 ownership-proven 对象删除。v2 retained changes 在迁移后必须为空、incarnation 必须改变、tail／trim 必须归零，node/change 高水位必须覆盖迁移前的全部 surviving reference 与 sequence；迁移后的第一份 node/change 严格使用更大的值。已完全 trim、`committed_position = trimmed_through > 0` 且没有 surviving row 的合法 v2 日志也必须可以迁移。

当前 schema 与历史迁移都要用 corruption fixtures 验证每个 volume 的可见节点恰是一棵 rooted tree：root 无 incoming entry，其余可见节点恰有一个同 volume parent，并且全部可达；cycle、未标记的孤儿与跨 volume entry 都失败。v5 引入的 detached regular file 按保留对象验证，不进入可见树，仍计入用量与完整性工作。另用整数、负数、溢出与 mismatch fixtures 验证`volumes.used`等于全部 regular-file size 的 streaming sum。storage-class fixtures 把 entry/change name 改成 TEXT、把 schema/database/binding/log scalar 或 nullable change group 改成错误的 NULL/type，并构造非法 kind、position、mode、size 与 nanoseconds；snapshot/log 不能漏行或接受 driver coercion 产生的可信零值。日志 fixtures 删除中间或尾部 retained change，并分别篡改 `previous_position`、`trimmed_through`、`committed_position`：`Open`、`Snapshot` 与 `ObjectStatus` 的完整链验证必须以 `EIO` 失败且不写入新 incarnation；`Since` 允许缺口之前的完整 page，跨到缺口的 page 必须整体失败且不暴露该页 prefix。空或较小日志在 caller 给出很大 limit 时仍按实际 anchor/row work 成功，page budget 耗尽且 tail 尚未返回时才以 `EFBIG` 拒绝。整链 record ceiling 由 `Open` 与 `ObjectStatus` 的精确边界／超限用例直接覆盖；snapshot row production 另有 caller-owned byte-budget 故障用例。`MaxIntegrityBytes` 用当前 schema 的精确边界与超大 corrupt entry name 验证 `EFBIG`；legacy migration 直接覆盖 record ceiling。ID fixtures 删除 node/change sequence、协同回退 internal high-water/sequence、把高水位压到其它 volume 的引用之下，并覆盖 node/change exhaustion；另抬高 committed tail，断言 append 在发布新 witness 前回滚。本地 durable fixture 再用外部 accepted state 拒绝协同回退。每一种拒绝都要断言版本、schema 与数据没有部分前进；volume 结构在 `Open` 与 `ObjectStatus` 两条入口覆盖，database-wide sequence/witness 拒绝由 durable open、page anchor 和分配路径覆盖。

### 本地持久对象存储

`packages/storage/objectstore/localdisk` 与 `packages/storage/localstore` 在测试专用的真实本地目录里运行，验证的不只是 `Objects` 契约，还包括磁盘格式与 reopen 行为：

- `FORMAT`、`LOCALSTORE` READY marker、初始化 intent、`METASTORE`、SQLite binding／database identity／generation／高水位、directory marker、recovery record 与 object envelope 的版本、UUID、volume、key、长度和 checksum；
- 根目录、祖先路径、owner、mode、symlink、remote filesystem、submount 与 lifetime lock；
- 初始化在每个 durable boundary 中断后只恢复已记录的 intent；已绑定 intent 能恢复 SQLite 已写非零 header、但 `sqlite_schema` 仍为空的 bootstrap，且该判定不依赖 WAL 大小。空 intent 旁出现 metastore、已有数据库丢失 volume 记录，以及 READY store 缺失或错配 database/witness/component 都失败；binding preflight 用匹配 volume 加额外 volume 和超大 binding/volume TEXT/BLOB 验证 bounded cardinality 与超限拒绝；
- 同 shard 的并发首次写入只串行目录／identity 阶段，不同 shard 继续前进；取消能退出 shard token 等待。identity stage-only 与 final+stage 同 inode 在 reopen 收敛，损坏、额外 hard link、不同 inode 与 stage 旁额外 entry 保留现场并失败；
- `fsync`、hard-link publication、unlink 与 marker cleanup 的顺序，shard identity/object publication barrier 结果不确定后的 health poisoning，以及 reopen 恢复出的事实；在 poison 前已经通过健康检查、随后等待 key/shard 的调用仍须在返回处转成 `EIO`，不能泄漏成功或 fact-bearing errno；waiting operation 上限满时返回 `EAGAIN`，同 shard 等待者不消耗 active slots，已 admitted 的其它 shard 继续前进；
- volume 配额 与 physical availability 取更小值，maintenance reserve、in-flight reservations、inode exhaustion 与 measurement failure；
- recovery records 与 object operations/bytes 受 admission 上限约束；pending byte threshold 对单个装不下的 payload 返回 `EFBIG`，现有 reserved/unresolved/garbage backlog 压力才以 `EAGAIN` 拒绝新 reservation；local store 在碰磁盘前验证 pending-byte threshold 能容纳最大 local object；authoritative shedding 造成的 `OverLimit`、reopen 后继续 admission refusal 与 garbage 清扫恢复都要覆盖；
- `Put` 错误把 reservation 转成 unresolved，不因超时或 sweep 被删除；只有 create-only `Put` 成功后的 `Commit` 失败才 `Abandon` 为 garbage。用例覆盖 collision 不向 volume 泄漏 `EEXIST`、旧对象经立即与重启后清扫仍被保留、Put/Commit 回复丢失、request cancellation、收尾失败、pending admission，以及 reserved→unresolved/garbage 保持 count/bytes；清扫的 startup、event-driven、periodic、满 batch 自调度、串行化与 shutdown cancellation status 都有独立测试；
- SQLite ordinary-reader 与 snapshot-reader pool 分别覆盖默认、配置前校验、占满后的等待/取消与 connection 释放；另用一份 held snapshot 占满专用池，同时断言普通 log/read 仍可使用另一池；
- 修改返回成功后构造 intact、缺失、空和仅含 header 的 WAL，结合 `A = C` 与 `A > C` 验证重开只接受可证明状态；见证 stage/final、checksum、store/volume/database identity mismatch 和 visible generation 前进／回退分别覆盖。`Accept` 失败必须 poison 并阻止读取；snapshot pin 使 status 报告 pending checkpoint，后台重试在释放 pin 后推进 C，真实 checkpoint error 保留到成功。open-time `Accept` failure 要保留可供下一次恢复的 WAL，成功 `Close` 则只在完整 checkpoint 和见证同步后释放 persistent WAL；清除调用失败保持可重试且不关闭 writer，不断言 flag 的最终值；
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
