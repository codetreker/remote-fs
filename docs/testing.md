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

顺序对拍的七处原始 Unlink/Rmdir 使用[单调用 helper](../packages/fuse/comparison_interruption_test.go)，同一路径最多八次，仅在 EINTR 且没有 EIO 时重试。夹具由单一顺序执行者修改名字，不并发重建目标；ENOENT、类型错误和 EIO（包括与 EINTR 并存）保持原错误，不转换成删除成功。若已经删除后仍返回 EINTR，后一次 ENOENT 仍会暴露对拍差异；这不是通用的 Linux namespace EINTR 无效果保证。整个复合步骤、ReadFile/WriteFile 和 Close 不重放，专门测试中断的原始首调用断言保持。四个根、14 个 verdict 的普通/race 检查包括原完整对拍序列、真实挂载的效果前中断及已执行删除后的单次 EIO；两份恢复旧 raw-call 的对照分别在 Unlink/Rmdir 断言失败。

目录 inode 对照使用[原始目录 helper](../packages/fuse/directory_listing_test.go)：每次 Open/Getdents 最多尝试八次，只重试该次 EINTR；Getdents 保持同一 fd、buffer、当前 offset 和已有名字映射，不从头重枚举。有效 buffer 下已交付条目以正长度批次返回并只解析一次，EINTR 没有可重放的批次。其它错误和耗尽的 EINTR 立即保留操作/路径及原因，已打开 fd 只 Close 一次，Close 错误也使结果失败。四个根、11 个 verdict 的普通/race 检查含两项未改断言的真实挂载用例；分别去掉 Open 或 Getdents 的单调用处理都会命中对应确定性中断断言。此规则不修改生产取消分类，也不用于专门核对首个中断的信号用例；[阶段语义](../.agents/notes/implemented/bug-fix/2026-08-22-eio-from-a-freshly-mounted-mountpoint.md)继续独立拥有未知效果与关闭规则。

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

[文件授权用例](../packages/transport/httprest/authorization_file_test.go)在 capability 查询、引用创建和动作记录之前核对每种 file 操作。合法打开用共享 OpenAccess，Replace/armed/reset/条件修改另外核对其实际语义；NameCommand 按具体 volume Operation 授权，范围编辑与 Drop 分别核对；非法参数在策略之前拒绝。用例先取得真实引用和动作回执，再拒绝 ack、renew、status、重放、修改与 close，核对原回执、期限、pending ack、引用和内容均未改变，拒绝的新动作没有记录。允许后重放仍返回原引用，原动作才可继续执行。

[强占有授权用例](../packages/transport/httprest/authorization_lock_test.go)逐项检查控制操作在 native service 或 status capability 访问前授权，并重新检查曾经成功的相同请求。拒绝只描述本次入口结果，不能泄漏已有动作回执或捏造 lockCode、recorded、Cancelled、Released 等 native 结果。文件侧另用只读描述符取得真实 EX flock，拒绝显式范围编辑、取消、Drop 与查询后，再从允许的控制路径核对原 Grant 和历史仍存在。调用方 cleanup 可以被拒绝；内部 lease 到期仍释放 retained 字节，且不会再次替调用方请求授权。

授权错误用例区分明确的 ErrDenied 标记与无法完成策略查询的故障，包括包装、join 和带 native 分类的底层错误。本地保留 cause，普通 HTTP、file、lock 与 stream writer 只输出可信的固定 EACCES／EIO 和消息；不把 callback 的原始文本或 native 回执写出。原请求生命周期先于策略分类：volume/file 可证明尚未 dispatch 的调用方取消沿用 `EINTR`，deadline 或策略自身超时保持相应 `EIO`；强锁服务端的生命周期和非 native 服务错误保持 `Unavailable/EIO`、`recorded=false` 且无 action。策略拒绝或故障使用独立的普通 EACCES／EIO envelope。

入口 callback 使用既有有界 response admission，满额时可在调用策略之前返回 `EAGAIN`。Stop 先取消并返回，ServeHTTP 仍须等待 callback 排空，期间不能继续调用 backend；请求结束也不得取消 host 原始 context。流的初始授权仅在启用 hook 时使用该 admission，拒绝响应写完后才归还，允许后在进入 Log、订阅与 snapshot 资源之前归还，不把名额占到流结束。

[流授权用例](../packages/transport/httprest/stream_authorization_test.go)分别验证 subscribe、resubscribe、snapshot 在初始 Log 与受控资源准入前检查，nil Log 和已满的订阅／snapshot 名额不能越过拒绝。start／rebuild、change、snapshot open／rows／done 与 keepalive 前再次使用原操作和 host context 检查；拒绝或策略故障后，队列中的页面、保活与成功 done 均不出站，terminal fault 只携带固定错误。用例同时确认页面确实已预取，以及最终 frame、snapshot 和订阅名额已归还。

背压用例让初始 Flush 或活跃 frame 的写入阻塞，再取消 host context 或 Stop，核对写入被结束机制打断，健康流不被附加默认 write deadline。terminal fault 的写入同样受结束预算约束；snapshot producer 收到取消后必须先退出，原拥有者再关闭 snapshot，不能在 Next 仍执行时强制关闭或提前宣布清理完成。授权约束每次准入，已准入单元可以完成，已经交付的数据不被当作可撤回。

[强锁 SDK 用例](../packages/transport/httprest/lock_client_test.go)仅接受带完整协议标记和有界 body 的普通 `{errno,message}` 授权错误；一旦出现 lockCode 或 recorded，就走完整 native envelope 校验，残缺字段不能回退猜测。丢失 Acquire 回复后，拒绝的 query／cancel 保留此前未知结果。[stream fault 用例](../packages/transport/httprest/stream_fault_test.go)拒绝重复字段、null、空值和未知 errno，旧的无 errno fault 保持 `EIO`；[SDK 发送边界用例](../packages/transport/httprest/subscribe_test.go)在初始帧与各个 Next 位置保留 typed `EACCES/EIO`。直接 SDK 的授权拒绝与 replica 观察 follower 故障后的统一 `EIO` 分别核对，都不能返回空目录或虚构的不存在。

## 保留对象、访问能力与标准锁

[文件句柄设计](design/server/file-handles.md)按对象身份、逐次发布、会话生存期和 advisory 锁分别验证。[公开类型用例](../packages/storage/files_test.go)拒绝无访问权限、非法创建/截断组合、不一致的 OpenNode 身份、无界会话配置和 action epoch/nonce；[中立范围](../packages/storage/ranges_test.go)另核对 extent、编辑、policy 与回执大小。实现缺少 FileStorage 能力时必须明确拒绝，不能重新按路径打开来模拟保留的对象。普通 Open 不获取 advisory 锁或强 S/X；FileSession 与强占有 Session 的生存期分别成立。

### 当前内容、身份与发布

[objectstore 句柄用例](../packages/storage/objectstore/files_test.go)先打开对象，再从另一入口覆盖、扩展、缩短、改名、unlink 和替换名字。每次 ReadAt 同时核对内容、大小、EOF 与节点 ID；原引用继续读取对象的当前版本，已占据旧名字的替代物逐字节不变。OpenNode、StatNode 与 SetNodeAttr 按既有身份访问，失效身份为 `ESTALE`；只读引用仍能按权限策略设置共同时间与客户端 metadata；缺少读写访问，或 Session 仍有效但引用已关闭时为 `EBADF`，Session 到期或退役为 `ESTALE`。创建并打开的用例同时核对 ExpectedID、排他创建、既有 metadata、初始 metadata 与截断，不能只断言返回了一个引用。

范围写的交错用例暂停一份不可变对象上传，让另一描述符先完成修改，再恢复原 WriteAt；最终内容必须包含两次已完成范围修改。SQLite 的每节点 content revision 与 CAS 用于重新读取当前版本并重算本次范围，不能据此拒绝较早打开的普通描述符。另测零填充、截断、空对象 ABA、revision 耗尽与原状态保留。读取遇到已经收集的旧 revision 可以重取当前对象；当前 metadata 所指对象缺失必须 `EIO`，不能返回空内容。

文件大小上限用例分别收紧 FileSessionOptions.MaxFileSize 与 native MaxFileBytes：先 Stat，再从另一入口增大文件，随后即使只读一个字节，ReadAt、WriteAt、非零 Truncate 和 Sync 也必须在 GetBounded/Put 前以 `EFBIG` 拒绝。Truncate(0) 不读取或暂存被丢弃的旧内容，超限且不可读的对象仍能按权限清空，Usage 随之归零；强 S/X 的最终权限检查继续成立。测试分别计数对象读取与上传，不能只检查最后的 errno。

[native 发布用例](../packages/metastore/sqlite/files_test.go)分别在上传前、最终发布前和已获准的发布期间退役引用或 Session。退役必须阻止尚未取得最终许可的 WriteAt、Truncate、创建打开与身份属性修改；已取得许可的事务先完成，再交接后继权限。强 S/X proof 仍在最终发布验证，名字消失时强占有退役，普通打开引用继续保留原节点。已知 cleanup 拒绝保留 pin 以供重试，接受结果未知则保留物理所有权与错误原因。

### 无名字对象的用量与恢复

[retained quota 用例](../packages/storage/limited/files_test.go)用两个打开引用保留已 unlink 的对象，断言字节继续计入 Used，后续增长也被计费，第一次 Close 不释放第二个引用仍需要的字节。最后一次有效释放按对象当前大小结算一次，重复 Close 不重复返还；失败 cleanup、取消创建请求后到期、startup 与 Recount 都核对真实 retained 用量。Recount 与最终 cleanup 交错时必须重取一致用量，不能用只遍历可见树的方法漏掉 detached 文件。

[local store 关闭用例](../packages/storage/localstore/files_test.go)在关闭 durable storage 前退役 retained 引用，重开后核对无名字对象已清理、Used 已释放且旧名字没有重建。最后引用的记账拒绝必须使 Close 保留原错误，第二个 opener 仍以 `EBUSY` 失败；移除故障后重新 Close 才能释放物理所有权。

[retained integrity 用例](../packages/metastore/sqlite/internal/integration/integrity_test.go)将可见 rooted tree 与各类合法 detached 节点分开验证：它们不进入目录快照，普通文件内容与符号链接目标仍计入 quota/integrity；detached root、带子项的 detached directory、非法标记、revision 或错误用量均失败。[恢复用例](../packages/metastore/sqlite/internal/integration/files_test.go)在独占打开时清理数据库内每个 volume 的遗留 detached 对象，即使 pending admission 已满仍完成必要清理并报告实际 OverLimit；损坏图在清理前拒绝，失败事务保持节点、对象、用量和 durable generation。[迁移用例](../packages/metastore/sqlite/internal/integration/schema_test.go)从受见证保护的旧 schema 前滚，核对已有节点与 accepted state，并在旧 schema 损坏或预算不足时保持原数据。

### flock 与 POSIX 记录锁

[advisory 状态机用例](../packages/advisory/coordinator_test.go)逐步核对共享/排他冲突、同模式重复申请、范围替换、拆分、合并、部分解锁及未来 EOF。flock 转换失败时旧锁已放弃，POSIX 转换失败保留原范围；GetConflict 返回真实冲突。DomainWholeFile 与 DomainRecord 互不冲突，session 与注册 owner 共同确定持有者；PID 仅由 FUSE 本地诊断映射解释，普通 I/O 不参与 advisory 冲突判定。跨文件等待检测 `EDEADLK`；有界搜索无法确定时明确拒绝，不能把未知当成无冲突。

[advisory 上限用例](../packages/advisory/bounds_test.go)分别耗尽 Session、Owner、范围、pending 与 action history，核对拒绝不会丢弃旧锁、拆分失败不改变原范围、取消及历史到期能回收对应名额。阻塞申请可以在有效且持续续租的 Session 内等待，不继承强 S/X 的有限 Wait 策略。取消与授予的交错保留可核对结果；只有 Cancelled/Released 证明没有遗留授予时才能返回可重试的 `EINTR`，结果未知保持 `EIO` 和 I/O 隔离。退役失败不能先放出旧 Grant，RangeControl.Drop 只清理指定 owner 的域。

[FUSE bridge 用例](../packages/fuse/advisory_bridge_test.go)通过真实 raw callback 验证内核 LockOwner 的传递，包括 owner 为零、没有加过锁的描述符关闭、POSIX 任一描述符关闭与 flock 最后一次 Release 的区别。缺失或饱和的 raw metadata 不得继续以错误 owner 执行动作；取消必须先核对远端结果。`unknown cancellation` 分支直接断言挂载 volume 的健康检查为 `EIO`，覆盖整个挂载的失败范围。Release 中 owner 清理失败仍尝试关闭引用并保存错误，同时核对挂载整体已被隔离。

### HTTP、副本与清理所有权

[FUSE Session 用例](../packages/fuse/session_test.go)让多个成功续租跨过原始期限后继续读取同一引用，再显式停止 Session，核对旧引用已失效、调用方拥有的 storage 仍可用。续租失败不能延长最后确认期限。连续性丢失后等待后台退出，再核对整个挂载的健康检查与停止结果持续为 `EIO`，原 FileSession 的引用不能继续写入；停止会取消在途 Renew、等待 Session 退役，并让重复停止共享同一个关闭结果。挂载级终止后需要新挂载建立新 Session，native RangeControl.Drop 的 owner 级清理继续独立成立。迟到的 Status 回复不能从收到回复的时刻重新起算租期。

native admission 用例用一个暂停的 Put 占满 Session 唯一的 data slot，先确认普通 File.Stat 为 `EAGAIN`，再核对 Renew 的 revision 前进且 Status 仍可用。另一用例暂停最终发布，验证静态能力探测可以完成，等待 contextual authority 的新 Session 创建可以取消。四类 admission 分别验证 data 的 MaxOperations，以及心跳、advisory 获取、核对与释放三个各 2 个活跃调用的分区。占满获取或核对分区后，同类调用为 `EAGAIN`，Renew 与数据访问仍能完成；获取饱和时 RangeControl.Drop 仍可释放。心跳名额也单独验证满额拒绝与取消归还，Close 在控制调用仍等待时继续前进。续租或 enrollment 不能因为大文件 staging 而变成不可退出的全局等待。

[HTTP 句柄用例](../packages/transport/httprest/file_test.go)用真实 SQLite/objectstore 验证同一对象的覆盖、unlink、替换与跨 handler advisory 协调。丢失 Open、Truncate、ACK 或 Close 回复后核对原动作，不能制造第二个引用或重新执行一次修改；Handler.Close 只退役自身登记的 Session，另一 handler 的会话仍可用。[registry 用例](../packages/transport/httprest/file_internal_test.go)覆盖 Session/action 上限、未 ACK 的 Open 在 Session 持续 Renew 时仍到期回收，以及旧窗口或已逐出的动作不能再次执行。

[HTTP 回归用例](../packages/transport/httprest/file_regression_test.go)拒绝缺失 Offset、Owner 等合法零值字段和不完整锁冲突回执；较晚到达的旧 Renew 回复不能缩短或重启已确认期限。Open 已有效果后取消返回 `EIO` 并清理未交给调用方的引用，Read、Stat、StatNode、GetConflict、Query 与 Status 的纯读取取消保持 `EINTR`。data history 满时 ACK 和 RangeControl.Drop 仍可清理，独立 MaxCleanupActions 耗尽则返回 `EIO` 并退役相应 Session 与 native 锁。合法的 1024 字节 body 配置仍须完成 Session 与文件调用，scoped File 保留已复制 proof，读取不携带 proof。

[副本句柄用例](../packages/storage/replicated/files_test.go)让 retained File 的读取、属性与锁控制直接核对 authority。带名字的创建和修改确认 metadata barrier；unlink 后继续操作旧对象不制造新名字或复制事件，原 Position 可以保持不变，同名替代物不受影响。流失败时 retained 控制仍须可用，不能拿 SSE 健康度代替 FileSession 生存期。[失败用例](../packages/storage/replicated/file_fault_test.go)与[admission 用例](../packages/storage/replicated/files_internal_test.go)覆盖缺失、错误或负 barrier，未返回 Open 的引用回收，已发出修改的原动作核对，以及共享 confirmation pool。MaxFileSessions 同时计入正在远端创建、仍存活及 cleanup 未确认的会话；发送前满额为 `EAGAIN`，确认 Close 才归还名额，未知清理继续占用。wrapper 不替 FUSE 启动续租 timer。

### 中立 metadata、namespace 与返回预算

[metadata 用例](../packages/storage/metadata_test.go)验证规范编码、独立 namespace 版本、present empty 与 absent、深复制及非法输入；[native metadata](../packages/metastore/sqlite/metadata_test.go)核对 CAS 保留其它 key、版本耗尽不发布、初值预算在插入/分配 ID 前拒绝、净 retained payload 经日志裁剪后的总量计费，以及所有实际创建/修改维护共同时间。旧 Unknown 时间不能被空值或当前时刻补齐。

[namespace 用例](../packages/metastore/sqlite/namespace_mutations_test.go)保留父身份经过改名/复用，验证目录观察在效果前拒绝、rename 输出拼写不移除第三占位者、source 与 displaced identity 的 Strong 检查、cycle/类型/非空目录拒绝，以及符号链接目标的 quota 和引用保留。Remove/RemoveDir 空 NameResult 成功是专门保留的结果形状，不允许通用 Attr 检查将已提交删除改成 EIO。

[NodeReference 用例](../packages/metastore/sqlite/node_references_test.go)分别验证 metadata 权限、raw leaf 字节、外来/关闭 Scope、真实 DeleteName 自我豁免与原生顺序。FUSE 的[目录/权限用例](../packages/fuse/directory_test.go)和[metadata 用例](../packages/fuse/metadata_test.go)核对目录引用及 posix.permissions.v1；缺席显示默认值不写回，present malformed 不回退，已知 ChangeTime 优先于 Linux 的历史显示投影。

[NodeReference 相容性声明](../packages/metastore/sqlite/node_reference_claims_test.go)验证 ReadData/WriteData claim 的双向冲突、失败只释放自己的 claim，以及 claim 不授予字节/metadata/目录枚举方法。关闭、失效、detached 与原生引用拥有权保持；身份 namespace 操作不能凭父目录观察推导 ReadEntries。[HTTP 授权用例](../packages/transport/httprest/file_authorization_test.go)必须在 native 打开前把这些声明纳入 OpenAccess，拒绝不能产生引用。storage/native/HTTP 聚焦普通与 race 分别为 2/53/29 个通过 verdict；恢复旧 claim 拒绝与遗漏授权意图的两个隔离对照在指定断言失败，不把聚焦 profile 当全包覆盖。

[目录 fd 授权回归](../packages/fuse/directory_authorization_test.go)通过真实 HTTP 直接驱动 FUSE handle：拒绝写打开的策略仍允许 Opendir/读取，随后被拒绝的时间/权限修改没有效果；允许的修改在改名或 unlink 后仍作用于保留 NodeID，替代目录不受影响。它分别记录 file.set-node-attr/file.set-node-metadata 授权，不把读打开当写许可。[目录拥有权用例](../packages/fuse/directory_test.go)暂停修改并发 Releasedir，核对同一 mutex 覆盖引用 Scope、所需版本读取与身份修改；关闭、失效 Scope、到期和能力错误都在修改前失败。旧 WriteMetadata 打开代码的对照必须在只读策略处以 EACCES 失败；这些 callback/HTTP 回归不代替实际挂载验收。

[共享打开条件](../packages/storage/capability_validation_test.go)验证 ExpectedMetadata 的 SameNode 绑定、缺席/二进制 token、16 项与名字/token 边界，并与原 FileMutation 保持同一比较语义。[原子打开](../packages/metastore/sqlite/atomic_open_test.go)和[节点引用](../packages/metastore/sqlite/node_references_test.go)用例暂停客户端观察，在另一会话完成 metadata CAS 后继续打开，核对 stale/absent 条件在内容、metadata、身份、日志、quota、claim/intent 和 pin 生效前拒绝；当前条件则验证 Reset 保留身份、Replace 以旧目标比较且保留旧引用字节。该运行交错发生在客户端观察与打开之间；初步检查和最终事务复用同一比较由源码顺序保证，不宣称在持续持有的 native gate 内插入了并发修改。

[HTTP 条件回归](../packages/transport/httprest/file_capability_http_test.go)通过真实 SQLite 验证 OpenAt/OpenNodeRef/OpenChildRef 的零效果 EAGAIN、紧结果预算拒绝及原子成功。二进制/空 token、重复/null/过大输入和 SameNode 约束分别检查；相同 action 改变条件须返回原 EINVAL，不能按新输入重放。省略 DTO 条件的隔离负向对照必须让三种 stale 打开在预期断言失败，纯 wire 往返不能代替最终 native 拒绝。

[HTTP 能力用例](../packages/transport/httprest/file_capability_http_test.go)将 lost Open/ACK/retryable Close、零/部分结果、最大范围回执和原生输出预算放到真实 httptest 交换中。客户端和服务端上限不同时必须将较小值传到 producer，在 metadata 载入、引用保留或修改之前拒绝；只给真正返回目标收费，不给内部父观察错收费。每个范围的 Commands/Claims/Effects 上限及整个 envelope 在授予前判断。fixture 验证的交换和真实 SQLite-backed 的数据效果分别记录，不能互相冒充。

[metadata CAS 请求用例](../packages/transport/httprest/file_capabilities_test.go)分别验证 nil/空期望版本编码成缺席条件、空 data、字节所有权与规范 base64/非 null/预算拒绝，返回 OpaquePayload 仍拒绝空版本。[真实 HTTP 首次插入](../packages/transport/httprest/file_capability_http_test.go)核对新版本、同 session 的原引用和 sibling 继续可用、旧条件无效果冲突、当前条件成功及相同动作重放。旧结果 decoder 的隔离对照必须在这两种首次插入输入上重现 400 与 session 清理；这不改变一般 400 或结果未知时的恢复规则。

[能力 decoder](../packages/transport/httprest/file_capabilities_test.go)拒绝不完整观察、丢失 LinkTarget/DirectoryRevision 和错误 capability advertisement；五类中立错误经 wire/journal 仍可由 errors.Is 区分。[部分打开用例](../packages/transport/httprest/file_capability_client_test.go)确保后置 capability/barrier 失败不遗弃可关闭引用，不把发生过效果的取消改成安全重试。

### 目录 metadata 与引用名字观察

[共享值和预算用例](../packages/storage/name_observation_test.go)区分 Root/Linked/Detached、保留原始字节、拒绝矛盾字段和 NodeID 替换；无 I/O ReferenceIdentity 只读取已持有身份，不通过 Stat/Node 猜测。[目录结果](../packages/storage/directory_metadata_test.go)核对 IncludeName 与目标一致；[ListResult](../packages/storage/bounded_test.go)验证实际 prefix 不产生 entry，重复、迟调用、负 charge 和溢出使整体失败。非法 header 必须在 sizing callback 前拒绝，callback 不能少收最低驻留量。

[native 目录观察](../packages/metastore/sqlite/directory_metadata_test.go)使用真实 Scope/guard/目录修订，验证允许 metadata 披露仍不授予应用 ReadEntries 或 ReadMetadata；外来、关闭及 detached 目录目标均失败。IncludeName 的实际 prefix 先于子项收费，空目录/短名字可以通过紧预算，prefix 加子项越界使 token、Name 和列表全部失效；调用者低报 charge 不能绕过 native 硬上限。仅内部父/guard 的大 opaque metadata 不应被加载或占返回 payload 预算。

[native 引用观察](../packages/metastore/sqlite/name_observation_test.go)让另一客户端改名并复用旧名字，确认原引用仍观察原 NodeID 的新绑定，且不要求额外 ReadMetadata。Root、detached 文件/目录/链接、重复绑定、缺失节点、超长或非 BLOB 名字分别核对；损坏 header 在 payload 和预算 callback 前失败。持有 native gate 时 identity getter 仍完成；guards、关闭、取消和原 session 失效保持错误，不重新按路径打开。名字和 State/Stat 分别检查，不能把两次观察当作共同快照。

[HTTP 观察用例](../packages/transport/httprest/file_name_observation_test.go)核对两个固定 OpReplicationSnapshot 映射、context、严格字段组合与实际名字 JSON/base64 预算。server/client/ListResult 上限不同、空目录、caller prefix 拒绝都不得保留部分结果；本地 callback 只在远端有界解码后执行，不能当作已序列化的远端限额。旧式打开丢回复时沿原 action 恢复同一 node scalar，getter/捕获身份不符保留 pending-open 清理义务；纯观察不增加 history/ACK/barrier。[真实 native HTTP 用例](../packages/transport/httprest/file_name_observation_http_test.go)核对属性、目录枚举和内部披露的权限分离。

objectstore 的两个畸形/不支持观察矩阵各建一份真实 SQLite fixture，只共享不变的空文件或原始字节目录/子项；九项引用与七项目录子例仍各有新的 proxy、计数器、session、引用和 result。每例按引用/session 到 volume worker 的顺序检查关闭，父 fixture 最后关闭 authority；不在子例间共享活引用或可变结果。

[objectstore](../packages/storage/objectstore/directory_observation_test.go)、[limited](../packages/storage/limited/name_observation_test.go)、[locked](../packages/storage/locked/name_observation_test.go)与[replicated](../packages/storage/replicated/name_observation_test.go)分别验证整链能力拒绝、原 context/身份/错误、一次 prefix 与原数据准入。replicated 的观察回源，失效时不使用名字缓存；所有包各自取得 normal/race 和包内覆盖收据，不能把 wrapper 调用计入 native 的覆盖。Windows resolver 的祖先组合、最终 guard 与真实 QUERY_INFO/rename 行为仍需平台验收。

### 使用声明、精确范围与删除恢复

native 使用声明测试覆盖旧/新打开的双向 Uses/Deny、匿名路径操作与确切引用 Scope；public ReadDirNode 和 replicated.List 都不能绕过 ReadEntries。metadata Lookup 与显式目录枚举分别测试，不能把同 session 当自我豁免。FUSE 的[owner 用例](../packages/fuse/lock_owners_test.go)验证引用/显式 owner 生命周期、仅用于死锁图的 Group 和本地 PID 映射。

范围测试同时覆盖 Bytes 的 unsigned 末端/溢出和 Boundary 的相交关系、独立 ClaimID、DropBeforeAcquire 的已知释放 Effects、完整回执装不下时无效果拒绝。advisory 保留原参与者规则；DomainEnforced 按明确 DenySelf/DenyOthers 检查实际访问。Query/Cancel 及退役 owner 的保留历史不能被“已经没有 owner”替换为未发生。

[pending unlink 用例](../packages/metastore/sqlite/pending_unlink_test.go)验证指定引用关闭即激活、非空目录消费 armed intent、清除 pending 不抹掉其它 intent、activation 失败保留 claim/pin/charge，以及 named final close 的匿名 Strong gate。live cleanup 使用捕获 accounting，恢复使用当前维护 chain，旧/current incarnation 不重复计费。恢复遇到受保护节点仍推进其它节点，恢复入口在 Strong 尚未初始化时拒绝。

[本地 crash 用例](../packages/storage/localstore/capabilities_crash_linux_test.go)以 SIGKILL 和原生重新打开核对已确认删除义务、引用计数恢复、持久结果与清理所有权；schema 阶段不提前删除有名 pending。范围/名字/metadata 的结果预算失败还须证明没有发布、没有提前释放额度。临时故障、已知 Strong 冲突和提交未知分别断言，不能一律当作后台最终会成功。

[维护计量用例](../packages/storage/limited/maintenance_test.go)和[objectstore 绑定用例](../packages/storage/objectstore/maintenance_accounting_test.go)把实际 Usage 初始化与单条 maintenance chain 绑定放在同一 native gate，验证取消/并发与捕获清理拥有者；并行 wrapper 不靠重复 callback 取得两次退款。

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

[打开 ACK 用例](../packages/transport/httprest/file_client_test.go)在真实 Open 返回引用后取消 ACK，覆盖按路径只读、读写以及按节点读写的普通已有文件打开；要求没有返回 File、原取消原因和规范 EINTR 保留、ACK 发出零次，同一 Session／File 的清理确认一次，native open／close 各一次，属性与字节不变，unlink 后无残留引用用量。创建、截断、报告 DeadlineExceeded 的 context、已发送但丢失且核对失败的 ACK、会话关闭，以及清理 EIO／ESTALE 仍为无 File 的 EIO；创建和截断效果如实保留，未成功清理的引用仍占用实际用量。该 deadline 用例验证 ACK 前的错误分类，既有真实计时器超时用例继续覆盖时间到期。清理必须原始返回 nil，不能把 ESTALE 的后续抑制当作成功；既有丢失 ACK 后成功核对的用例保留。

[关闭清理用例](../packages/fuse/completion_test.go)覆盖请求值保存、关闭线程取消被隔离、默认 30 秒与显式 FlushTimeout、负值拒绝和较早请求 deadline 保留。预算在等 close mutex 之前起算，等待后只剩原 deadline 的余额；一次引用关闭只调用一次底层 Close，并发或重复关闭共享原结果，真实失败不重试，晚于预算返回的已确认成功不被改写成失败。Flush 清理 POSIX owner，Release 清理最后的 flock owner 并关闭引用；它们不发布文件内容。Fsync 调用 File.Sync 验证已发布内容的健康与持久屏障，继续响应请求取消，不能顺带退役引用。FlushTimeout 不构成内核 Unmount 或 Mount.Wait 的耗时上限。

[范围写与截断用例](../packages/fuse/space_cancellation_test.go)在 retained File 边界注入直接和包裹的取消、deadline、`EINTR` 与独立故障，核对原内容不变，下一次调用仍得到新的权威 `EDQUOT`。Space 被调用即让测试失败，确保普通写入直接依赖 native quota。真实配额已满时增长和扩展失败，缩短成功后能立即使用释放的空间；已确认成功之后才到达的取消保持成功。已有副作用或结果未知不能伪装成可安全重试的 `EINTR`，与取消合并的独立故障也不能被较轻的分类覆盖。Statfs 独立调用 Space 报告容量。

[读取信号探针](../packages/fuse/interruption_linux_test.go)在实际 HTTP 读取已进入服务端后，向执行系统调用的子进程线程发送 `SIGUSR1`，同时观察原 FUSE 与 HTTP 请求 context 被取消。原始 `Fstatat` 必须得到 `EINTR`，普通 `os.Stat` 依靠标准库处理中断后成功，两个调用方随后都须读到完整内容。[关闭与写入探针](../packages/fuse/interruption_write_linux_test.go)分别暂停 owner 清理与 retained WriteAt。Close 收到中断后清理 context 仍有效，普通关闭成功，Write 加 Close 总计只发出一次 WriteAt，随后重新读到已确认内容；不重试已经消耗的描述符。原始 Write 在进入下游前得到 `EINTR`，Go Write 重试后由实际 quota 以 `EDQUOT` 拒绝；两条路径分别核对调用次数和目标仍为空。

[打开信号探针](../packages/fuse/interruption_open_linux_test.go)暂停真实 HTTP Open 的完整成功响应，在 ACK 前向执行打开的子进程线程发送 SIGURG，并关联原始 FUSE OPEN／INTERRUPT 及 FUSE、HTTP context 的取消。原始 unix.Open 一次调用得到 EINTR；普通 os.OpenFile 依靠标准库重试后成功，没有应用层重试循环。两条路径都确认首个引用已经清理：raw 模式一次 Open、零次 ACK，Go 模式两次 Open、一次 ACK，全部引用最终关闭，原属性与内容不变。

信号探针独立于纯映射测试，也不把 SIGURG 当作所有历史失败已经证实的原因。四项历史 `cmd` 用例曾分别以 `-race` 固定采样 100 次，共 400 次；其失败记录和结果只对应当时实现，不能充当当前文件句柄路径的验收。当前验证同样不增加应用层 `EIO` 重试，不关闭异步抢占，不预热被测操作；每条阶段断言取得自己的执行证据，不以重跑整个无关命令套件代替它。

## 显式文件锁

[文件锁设计](design/server/file-locks.md)的验证分为共享 volume 契约、authority 状态机、native 最终发布、持久恢复和 HTTP 编码。`X` 证明本次修改的权限，内容版本比较另有自己的义务；普通打开或读取不会隐式取得租约，读取成功也不能充当租约仍有效的断言。

### 保护、身份与有界历史

[共享契约](../packages/storage/lockcontract/contract.go)在 SQLite/objectstore 与 HTTP volume 上使用同一套操作。用例覆盖 `S/S` 共存、`S/X` 与不同 Owner 的 `X/X` 冲突，匿名修改与只有 `S` 的持有者自身修改都被拒绝；带有效 `X` 的 Write、共同时间/metadata、Remove 与 Rename 才能修改受保护目标。普通读取在 `S`、`X` 下仍可完成，无活动保护冲突时匿名修改继续可用。每次拒绝后从 storage 重新读取内容，不能只检查错误码。

身份用例让祖先目录改名、内容原子替换、目标覆盖、删除与同名重建真正发生，验证源文件保持 ResourceID、被覆盖目标退役、旧 proof 不会指向新节点。覆盖式 Rename 必须同时提供受保护源与目标的 `X`；多余或不相关 proof 也必须失败。Scope 用例修改调用方原 proof slice，断言已创建的 scope 不变；显式匿名 scope 不继承外层权限。已解除或到期的 proof 即使没有竞争者，也不能退回匿名执行；空 SetAttr 与自身 Rename 同样验证它。共享契约与 SQLite 适配器另覆盖根、目录和缺失路径不能作为申请目标。

[authority 契约](../packages/locking/authority_contract_test.go)用可控制时钟分别断言不可变动作回执与当前 Grant 状态：冲突或 AlreadyHeld 的已记录拒绝在竞争结束后仍原样重放，成功 Acquire 的原回执在 Release、Expiry 或 TargetGone 后不变，重复 RequestID 携带不同意图必须拒绝。Renew 不缩短已有期限，重复 Renew 不延长第二次，持久水位准备期间到期的 Grant 不得复活。排队写者先于后来读者；排队超时、已授予后取消、尚未到达的 Acquire 被取消，以及取消后迟到的原请求均有确定性交错。控制请求 context 结束不会撤销已经受理的 Pending 意图。

[容量用例](../packages/locking/contract_capacity_test.go)分别耗尽 Session、已消费 enrollment ticket、Owner、Resource、Action、Grant、全局及各层队列、proof 数与请求字节预算，不能用一个较紧的上限代替另一个。活动 Session 上限独立于 ticket 到期；ticket 重放不会创建第二个 Session，原 Session 已关闭也不能借旧 ticket 创建另一个。活动 Owner 的动作回执不被逐条逐出，满历史仍保留 Release、已知 Cancel、RetireOwner 与 CloseSession 的清理路径；新的未见 Cancel 只有取得有界 tombstone 才能确认取消，否则为 OutcomeUnknown。未见动作查询同样不报告“未授予”。NotAdmitted 不产生虚构回执；Renew 在历史满时保留此前已确认期限。引用到期释放 Resource 容量但不缩短已有 Grant，Owner/Session 退役后旧能力不能重新执行动作。

[队列生命周期用例](../packages/locking/resources_internal_test.go)分别移除队首、中间和队尾，核对幸存项顺序、authority/Owner 计数及底层 slice 空闲位置均不再保留 action 指针；Owner 退役后整个 backing array 不能继续引用其历史。fence 使已有资源 worker 退出并保留 Pending，随后禁止重新启动 worker；维护循环仍按每个 Wait deadline 结束等待，包括先于队首到期的项，逐项清除队列引用而保留动作历史。到期清理不解除 fence，QueryAction 与 Publish 继续返回 Unavailable 并保留原始原因，不把无法核对的结果说成可用的回执。

[后台清理失败回归](../packages/locking/contract_regression_test.go)在手动时钟中确认目标到期 timer 已注册，并暂停它的返回；推进时钟后才放行，使后台路径实际观察到到期。仅看到曾经注册过的 timer，不能证明当前等待仍使用它。用例继续断言后台 `Forget` 失败使授权方停止发布，且 `Close` 不再次尝试结果不明的清理。

### 最终发布与观察次序

[objectstore 用例](../packages/storage/objectstore/locking_publication_test.go)在真实组合的 Objects.Put 边界暂停 staging，随后推进时钟或取得新的冲突 Grant；恢复上传后必须在最终发布拒绝旧 proof 或匿名修改，原内容保持完整。另一文件的修改必须在暂停期间完成，证明慢上传没有占用它的发布权。SQLite 另让 staging 后的路径指向不同节点，断言 [Commit 根据实际目标验证](../packages/metastore/sqlite/publication_test.go)，Reserve 成功不代表最终发布已经获准。

authority 的[发布交错用例](../packages/locking/contract_concurrency_test.go)暂停已经取得最终许可的 native 回调，要求同资源的 Release 等待，而另一资源的发布仍可前进。Grant 查询用例另推进时钟越过租约期限，查询必须等未完成发布结束后再报告 Expired。SQLite 保留自身 writer / health 串行化，其发布用例断言新 Stat 等待发布、已捕获快照仍读到旧版本，以及发布后新视图取得新大小和对象 key。objectstore 的已捕获 immutable object 读取在 Get 暂停期间允许后续授权和发布，恢复后仍返回旧的完整内容。这里验证授权与效果的次序，不要求已获准的 commit 或同步在到期时刻前物理返回。

失败用例区分准备拒绝、已知效果和结果未知。SQLite 准备失败保留 reservation 与 Grant，删除与目标覆盖退役真实受影响身份；[记账收尾失败](../packages/metastore/sqlite/publication_test.go)与[持久确认失败](../packages/metastore/sqlite/publication_test.go)使 volume 和 authority 停止发布，错误原因仍可核对。复合修改的部分效果由 FUSE 阶段用例验证，已有副作用时不返回暗示整个操作未执行的 `EINTR`。Close 排空在途发布与持久水位准备，关闭失败保留原因和必要所有权，不能对可能已经复用的描述符重试 Close。

[limited 构造与通用契约用例](../packages/storage/limited/limited_test.go)分别使用原生组合、委托原生计费但不暴露 retained 文件的 wrapper；两者都执行完整 storage 契约。缺少 native 计费能力在任何用量测量前以 `ENOSYS` 拒绝；[能力检查失败](../packages/storage/limited/publication_test.go)逐一保留原始错误，不因是否提供锁服务而改变。非 retained wrapper 继续验证有界初始化与 Recount：目录和 frontier 预算、取消、listing 失败均不发布部分计数，等待中的操作可在测量结束后推进。

[native quota hook 用例](../packages/storage/limited/publication_test.go)以真实 SQLite 与内存对象保留 3072／1024 字节的目录改名交错：外层额度为 4096，内层分别不设额度或设为 8192；暂停 Put 时无关文件的等长覆盖须完成，再依次把 d 改名为 old、e 改名为 d。恢复后，针对较小新目标的增长以 `EDQUOT` 拒绝，反向大小组合的缩短按最终目标结算；两份内容、元数据权威 Usage、即时 Used 与 Recount 必须一致。最终效果注入另外验证增长先预留、Applied 后才返还缩减或删除的字节、NotApplied 退回增长；已应用但回复失败仍按实际效果结算。[未知结果或 unwind／settlement 失败](../packages/storage/limited/publication_uncertainty_test.go)保守保留预留，并使后续修改、Space 与 Recount 失败，已在 staging 的调用也不能越过该状态。嵌套 quota 验证准备被拒绝后各层预留归还；scope、锁服务与 Close 继续配对透传。

[到期缩减用例](../packages/storage/limited/lease_quota_test.go)使用真实 SQLite、objectstore、内存 Objects 与外层 limited，metadata allowance 设为零以使外层独立承担记账。在 Objects.Put 暂停缩减写入后，立即核对旧字节仍占满配额且另一写入为 `EDQUOT`；推进时钟使 proof 到期，恢复后要求 `StaleGrant`、原内容不变，Space 和 Recount 都仍报告原用量。随后有效的匿名缩减才释放差额，另一文件必须能够恰好用完该差额，再由 Recount 核对总用量。

### 恢复与独占所有权

[SQLite 恢复用例](../packages/metastore/sqlite/lock_recovery_test.go)分别在 Prepared 提交、见证写入前后与 Accepted 完成处中断，重新打开后核对恢复出的最大时长与 Prepared 已清除。拒绝用例逐项构造缺失记录、缺失或回退见证、单边状态回退、错误身份、部分 Prepared、跳代、下降的时长与错误字段类型，断言 `EIO`。并发提高水位必须保持单调，取消或持久失败不能确认提高成功，注入的见证错误保留原因链。已有 v3 数据库的迁移先核对原 accepted witness，再改变 schema。

[native lease anchor](../packages/metastore/sqlite/internal/nativelease/anchor_test.go)覆盖显式初始化与重开、匹配的中断初始化 intent、数据库和状态身份绑定、缺失或损坏见证、复制或替换证据路径，以及 lifetime ownership。xattr、flock、rename、文件及目录 fsync 的能力探测在初始化和重开时验证错误与误报成功，known remote filesystem 在修改前拒绝，中断的探测与 stage 只清理可证明属于本次状态的残留。恢复期从实际取得独占所有权的单调时刻起算，较小的新配置不能缩短已记录时长；这些证据位于 SQLite-backed 存储自身的持久边界。

[SQLite 拥有者用例](../packages/metastore/sqlite/locking_store_test.go)与[跨进程所有权用例](../packages/metastore/sqlite/locking_store_test.go)验证 raw opener 的共享 flock 与授权方的排他 flock 在同进程、跨进程中双向排斥；数据库绑定后不能通过 raw constructor、路径别名或并发拥有者绕过保护。恢复配置和启用入口必须验证真实排他拥有者与 native anchor；重开另一个已有 volume 时仍须读取同一数据库级最大时长并等待完整恢复间隔，选择不同名字不创建新证据。local store 继续验证单个 volume 的根绑定，不能省略锁配置或丢弃见证来恢复为空的 authority。真实子进程在租约已确认后遭 `SIGKILL`，由新的进程或组合 store 重开：内容仍完整，恢复期间读取和状态查询可用，修改为 `EAGAIN`，较小配置不能缩短此前水位；旧 Owner、Grant 和动作不能在新 authority 下重放执行。故障注入与真实退出分别证明具体持久边界和进程生命周期，不把它们当作断电或设备缓存验证。

### HTTP v4 与客户端

[server 用例](../packages/transport/httprest/lock_server_test.go)验证 `/v4/` 协议标记和旧版本拒绝，以及匿名与 scoped 调用都抵达同一个执行保护的 volume。Scope header 的空值、重复值、错误 base64url、缺失或重复成员、未知字段、超长值和错误使用位置必须在修改前拒绝，随后 Stat 证实目标未创建；读取与控制操作不接受 mutation scope。能力值只在 body/header 中传递，URL 与错误诊断不能泄漏它们。[Scope 用例](../packages/transport/httprest/lock_scope_test.go)逐一验证所有 mutation、WithBarrier 与无效果 mutation 保留复制后的 proof，读取不发送它，匿名 handler 不继承被包装客户端的权限。

[副本转发用例](../packages/storage/replicated/lock_service_test.go)分别把基础 replica 和 scoped 视图交给真实 HTTP handler。代理查询须找到原授权方的 grant，匿名修改被拒绝，显式 proof 修改成功后本地 replica 立即可见；经代理 Release 后，再从原授权方确认 Released。随后匿名写与普通读仍可用，关闭代理 HTTP 服务后底层 replica 仍能写入，内容由原服务端重新读取核对。

[client 编解码用例](../packages/transport/httprest/lock_client_test.go)区分缺失字段与合法零值，覆盖枚举、整数毫秒、溢出、嵌套意图、截断和不一致的成功或错误结果。HTTP 422 的已记录拒绝必须同时返回原 ActionResult 与 typed error；`lockCode`、errno、recorded、意图、Grant 与回执 variant 不匹配时按协议错误处理，未受理的失败没有虚构 receipt。丢失 Acquire 回复后使用原 Owner、RequestID 与 ResourceRef 核对结果，迟到的原请求不能越过已确认 Cancel，也不能用新身份重新申请。错误归类继续保存原 context 或网络错误原因。

[历史窗口用例](../packages/locking/history_test.go)与 wire 用例推进时钟后核对 Resolve 的 `HistoryExpiresMillis` 已报告延长后的 Session 历史期限，同时区分资源引用本身的 expiry。响应缺少 historyExpiresMillis 必须解码失败，Acquire 的嵌套 ResourceRef 与 receipt 也保留该必需字段。其它成功控制与带回执的已记录拒绝均核对当前历史期限；Acquire / Renew 重放在等待查询前不能先延长期限，再因取消返回一份没有期限的错误。测试同时核对实际延长与返回的期限，不能只断言内部 Session 尚未到期。

Pending 用例先占满 authority 的申请队列，随后确认 HTTP control admission 已归还，Renew、QueryAction、Cancel 与 Release 仍可完成；Acquire 的 Wait 是受理后意图的有限寿命，不是 HTTP 长请求的占用时间。另一组用例同时耗尽 server body/response 与 client response admission，enrollment 和状态控制仍须完成。控制 admission 的配置用例分别检查默认值、非法上限和有效配置的传递；client 名额满时，拒绝必须发生在发送前，并保留 `recorded = false`，不能声称动作已有结果。

时间断言使用不同于墙钟的 authority ticks。[毫秒回归用例](../packages/locking/contract_regression_test.go)构造到期 1.1 ms、当前 0.9 ms 的边界，要求仍 Active 的 Grant 报告 `RemainingMillis = 0`，不能把分别取整后的两个时间相减得到 1 ms。客户端本地提示从请求发送时刻加 remaining 起算，零余额不获得新期限，Expired 不产生有效期限；原回执的 TTL、deadline 与 revision 不能重置当前保护。控制操作在 dispatch 前取消为 `EINTR`；已 dispatch 的状态修改即使因取消丢失回复也为结果未知的 `EIO`，只读 Query 的 POST 仍按读取语义分类。上述用例不以 HTTP method 判断操作是否已有副作用。

## 元数据副本的读写交接

[重建回放用例](../packages/storage/replicated/build_test.go)使用真实 SQLite 与 HTTP，分别控制 snapshot 的 done frame 和后续 change frame。在快照捕获后由独立客户端创建、覆盖、改名和删除；done 交付前不得请求新的 checkpoint，回放仍被阻塞时，副本的 Stat／List 必须返回 `EIO`。释放变更后核对完整树，并在 `MaxSubscriptions=1`、每主机两条 HTTP connection 的限制下检查 Subscribe、Snapshot 和 Checkpoint 次数，确保就绪交接使用原订阅。

固定目标用例在 checkpoint 捕获后再提交并阻塞另一条 change，要求构建达到原目标即可返回；随后取消构建调用方的 context，再释放该 change，持续跟随仍须应用它。空树用例把 snapshot HTTP body 保持到 client Close，验证释放连接后 checkpoint 才能完成，零位置无需等待事件。另覆盖合法的非连续 position、checkpoint 与订阅 incarnation 不同、checkpoint 落后于快照、权威端不可达、日志保留丢失和无法应用的 change。重建部分回放后断流时，公开读取继续失败，下一次 Resubscribe 必须从实际安装并应用到的位置恢复，最终树与权威端一致。

取消用例分别停在初次订阅的 start frame 和回放读取入口，要求构建返回 `EIO` 并保留 context 及自定义原因；订阅取消还须等待 handler 退出、不发送 Snapshot，并证明唯一订阅名额可复用。snapshot Close 故障保留原错误且不发送 Checkpoint；host context value 须到达 Snapshot 和 Checkpoint。[选项用例](../packages/storage/replicated/options_test.go)验证 `ReplayTimeout` 的十秒默认值及非正值拒绝；构建用例分别阻塞 Checkpoint 请求和回放读取，验证快照 EOF 后的期限以保留 `DeadlineExceeded` 的 `EIO` 结束构建，不能交付可用副本。

[checkpoint HTTP 用例](../packages/transport/httprest/checkpoint_test.go)验证 `GET /v4/checkpoint` 返回当前 `MutationBarrier`，重复读取不改变节点、目录、容量或日志位置。授权以可信 volume、host context 和 `OpReplicationCheckpoint` 先于 Log 读取执行；拒绝和策略故障只返回固定 `EACCES`／`EIO`，缺少 Log 在授权后返回 `ENOSYS`。非法权威 barrier、字段缺失或类型错误、额外字段、错误嵌套、尾随 JSON、协议不符、截断或超限响应都须失败且不暴露部分 checkpoint。直接调用 Checkpoint 的发送后取消保留 `EINTR` 和 context 原因；构建期间无法完成回放门则由上述构建用例要求 `EIO`。

五个副本确认、容量及 Close 用例使用[事件交付门](../packages/storage/replicated/harness_test.go)：真实 bootstrap 完成后才 arm，在转交响应字节前等待，用例先确认 entered，再执行对应取消或 Close，最后在原来的到达／完成位置释放。请求取消保留原 context 错误；两秒确认 grace、五秒／十秒容量 grace、二十毫秒 waiter 观察、一秒 Close 界限和原结果断言保持。慢 snapshot、replay 与全负载可见性继续使用各自原有条件。

[确认 bookkeeping 用例](../packages/storage/replicated/bookkeeping_test.go)确定性地使发送回调返回成功 barrier、确认前已发生 follower 失败，分别覆盖 barrier 尚未到达和已经到达；核对发送回调仅调用一次、PathError 的操作／路径、EIO 及未能确认已发生修改的诊断，并要求 active／waiter 归零。原有真实 HTTP 双故障用例保留，不能把竞争中哪一条错误先返回当作稳定覆盖条件。夹具边界与取舍见[测试工作量决定](../.agents/notes/implemented/testing/2026-09-09-scale-test-work-to-its-assertions.md)。

[副本读写门](../.agents/notes/implemented/bug-fix/2026-09-07-let-replica-writers-progress.md)分别验证类间次序和真实入口。门的确定性交错用例先证明写者已登记，再放开旧读者；读者批次必须在唤醒前保留名额，尚未获调度的读者也不能被下一写者越过。覆盖批次内读者的正常进入与取消、后来读者进入下一批、多写者中只撤销本次取消，以及取消最后一个等待写者后重新放行读取。

真实 `Replica` 用例覆盖 `Apply` 与 `Reseed` 等门取消、等待 commit gate 时释放外层名额，以及 `Stat`、`List`、`ListBounded` 在 reseed 后排队时的取消；失败的 `ListResult` 不能暴露已保留前缀。SQL 读取名额另验证与 reader pool 容量一致、名额耗尽时调用不进入阶段、交接后取消归还名额，以及 `Position` 与写者不消耗 SQL 读取名额。完整、回滚、无效 row 与提交失败的 reseed 都同时观察树和 `Position`，并验证重复 `Close`；读者批次还必须在多个真实 `Apply` 的积压之间得到执行机会。取消用例保留现有错误分类与 context 原因。

压力与可见性使用不同的时间判据。[SQLite 压力用例](../packages/metastore/sqlite/replica_test.go)先占满现有 reader pool，再启动 128 个公开 `List` 调用。用例在门的互斥保护下确认只有与 pool 容量相等的读者持有共享访问，其余调用仍在阶段外等待 SQL 名额；确认 `Apply` 已登记等待后才释放 reader pool。4096 文件的读取循环保持到写者结果返回，随后停止并取消本用例拥有的读者 context，join 全部读者并检查门空闲、读取 permit 为零。只有停止标志、私有取消原因、错误链中的 `context.Canceled` 和 `EINTR` 分类同时成立，才忽略预期退出；其它错误仍失败。30 秒写者截止时间、Applied、Stat 模式与 Position 断言保持，不能提前撤掉负载。结果后的清理不属于写者延迟，局部测量见[减少无用测试工作](../.agents/notes/implemented/testing/2026-09-09-reduce-test-work.md)；该死锁上限不代表复制延迟。

[真实 HTTP/SSE 全负载可见性用例](../packages/storage/replicated/replica_acceptance_test.go)使用 4096 文件与 128 个持续调用内部 metastore.Replica.List 的读者，以一秒为真实 HTTP 独立写者提交、SSE/Apply 后公共 Stat 观察到修改的上限。公共 List 的权威访问约束由单独测试验证，不把这项本地读写交接压力换成 128 个 HTTP 请求。用例先观察每个读者都成功完成列目录，再提交变更，并断言计时窗口内列目录继续推进；调用进入某个回调不构成负载成立的证据。

[point-read 准入用例](../packages/metastore/sqlite/replica_test.go)先占住默认 15 个 listing 名额，让更多扫描排队，核对总 SQL 名额仍是 16、排队扫描未进入共享阶段，而 Stat 能取得保留的入口。取消归还 listing quota；登记 Apply 后短查询不能越过写者，已捕获扫描先完成再交接。它与完整 4096/128/一秒验收各自证明调度边界和实际可见性，不能只凭单元测试或较短负载替代。

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

[元数据缓存用例](../cmd/replicated_test.go)的 `TestWalkingAMountedTreeUsesAuthoritativeNamesAndCachedMetadata` 按目录数核对 OpenNodeRef、ACK、Scope、ReadDirNode 与 Close，另要求 LookupAt/身份属性抵达 authority；目录改名计数 file.mutate-name，子树 inode 保持不变。直接路径 Stat 的副本查询单独断言不回源。计数起点等待已完成的引用关闭，避免此前异步 Release 混入。

[锁配置用例](../cmd/remote-fs-server/lock_configuration_test.go)验证容量和期限逐项传入 authority，`-initialize-lock-state` 是显式动作，非法配置在 listener 或状态初始化之前拒绝。`-dir`、`-lock-state-root`、目录预算与 CLI quota measurement 参数作为未知 flag 拒绝，目标 local store 保持空目录。[状态用例](../cmd/remote-fs-server/status_test.go)检查 ready、recovering、unavailable 和各项计数，不输出 Authority 能力材料；状态失败不拼接部分容量数字，不可用的 authority 不宣布就绪，缺失 status 能力的 volume 被关闭且关闭错误保留。这些包内断言验证参数与 lifecycle wiring，跨进程结论仍由二进制用例提供。

其余聚焦的 server 入口行为在 `cmd/remote-fs-server` 包内验证：配置解析与默认值、两种 storage mode 的打开路径、READY 与 signal ownership 的顺序、SIGHUP 的 metastore-backed status、SIGINT／SIGTERM 的 admission 停止与 handler 排空，以及 partial-open 或 shutdown failure 后的资源释放。pending、reader、integrity、sweep、snapshot-frame 与 subscription 参数到达各自组件，local waiting-operation 参数仅用于 local store，invalid bounds 在 listener/root mutation 前拒绝。sweep 用例拒绝非正 interval/batch 与超过 `MaxSweepBatch` 的 batch；write-bound 用例分别验证 local object 上限、Blob 5000 MiB 上限与 pending-byte threshold 的精确边界和超限拒绝。两种 status 都断言打印 effective reader/integrity-record/name-byte limits，local status 另打印 waiting/active operations。status 阻塞时，终止仍先停止 HTTP admission 并取消 status，handler 排空期间保持 native 所有权；不响应取消的 status 只能在 HTTP shutdown 后参与等待。这些用例直接调用命令内部的 opener 与 lifecycle helper；它们验证同一条命令代码路径，不构成已构建二进制的进程边界证据。

`cmd/remote-fs` 包内测试同样区分入口层次：mutation-confirmation 与 client frame flags 的默认、help、invalid-before-network 和 forwarding 直接驱动 `run`、dial/replica helper 及真实 HTTP stream；[副本构建失败用例](../cmd/remote-fs/main_test.go)要求 Snapshot 后 Checkpoint 返回 `ENOSYS` 时，以保留原原因的 `EIO` 结束并清理副本目录，不进入初次 Subscribe 不支持复制时的无副本模式。构建出的 mount 二进制端到端用例仍走默认配置。包内用例证明 command wiring 与复制路径，二进制用例证明交付程序的进程、信号与挂载边界，结论不能互换。

独立 HTTP server 的资源用例使用真实 TCP listener：占满 accepted-connection 名额后底层 `Accept` 不再前进，connection 的单次与重复 `Close` 只释放一份名额，关闭饱和的 listener 会唤醒正在等待的 `Accept`。两种 mode 都要求至少两条 connection，并在取得 listener 前拒绝非正 timeout。只发一部分 header 的连接在 `ReadHeaderTimeout` 内被关闭，keep-alive connection 超过 `IdleTimeout` 后被关闭；同一用例断言 request-wide `ReadTimeout` 与 `WriteTimeout` 保持为零。shutdown 用例覆盖已经存在和尚未被 tracker 观察到的 `StateNew` connection，确保 stopping state 会关闭 late notification，不把退出安全性押在 header timeout 上。

### SMB 协议基础组件

[wire 用例](../packages/smb/internal/wire)验证 bounded compound/header、请求/响应结构、UTF-16、lease/lock/notify 与 symlink reparse；畸形长度、偏移与上下文数被拒绝。[签名用例](../packages/smb/internal/signing/signing_test.go)验证 CMAC 向量、preauth/密钥派生、报文校验及 Destroy 与签名并发。现有 fuzz seeds 进入普通测试，不把这次运行称为持续 fuzz。

[认证 context](../packages/smb/auth_test.go)核对 Principal 的请求生存期与隔离；[Windows policy 用例](../packages/smb/windows/auth_test.go)核对 canonical SID、显示名不能授权、取消与 SECURITY_STATUS。非 Windows 分支明确拒绝 native authentication；[native SSPI 用例](../packages/smb/windows/auth_windows_test.go)必须在 Windows 实际执行，交叉编译不能代替。

这组代码以自己的测试 binary 验证：

```sh
.github/scripts/assert-every-test-ran.sh -count=1 -p=1 -timeout=3m \
  ./packages/smb ./packages/smb/internal/wire ./packages/smb/internal/signing ./packages/smb/windows
.github/scripts/assert-every-test-ran.sh -count=1 -p=1 -timeout=3m -race \
  ./packages/smb ./packages/smb/internal/wire ./packages/smb/internal/signing ./packages/smb/windows
go vet ./packages/smb ./packages/smb/internal/wire ./packages/smb/internal/signing ./packages/smb/windows
```

[Native protocol and SSPI 作业](../.github/workflows/native-smb-gate.yml)通过[原生认证脚本](../.github/scripts/native-smb-auth.ps1)直接执行当前 checkout 的这四个 package，不检出原型或施加 overlay。环境须为 Windows 11 24H2+ ARM64，记录 checkout、workflow 与 PR head SHA、OS/build，以及 SMB 源文件前后 hash。先列出全部测试，再无 -run 过滤地执行；每个 package 和已列出的 Test/Fuzz 根须有 pass，任何 fail/skip、缺失 verdict 或缺少两项 native SSPI 根均失败。

作业的十分钟上限与单次测试的三分钟上限分开；临时目录和 Go 缓存位于工作区 `.tmp/native-smb-auth`。Windows 原生 profile 只按各 package 自己的测试 binary 计覆盖，要求每包 70%、每函数 50%、合计 85%，无 profile/执行块同样失败。源码、环境、列表/执行 verdict、profile 和逐函数结果作为当前源码证据保存，结果只绑定实际 checkout。

普通与 race 收据各为 70 pass、无 fail/skip，Windows ARM64/AMD64 构建也通过。[当前源码原生运行 35095465239](https://github.com/codetreker/remote-fs/actions/runs/35095465239)另在 Windows 11 Enterprise 26200 ARM64 执行 42 个根、71 个 verdict，两项 native SSPI 根通过且无 fail/skip；其 source/profile 对应 checkout 328d5f64，不能用来替代随后改变的实现。它们只证明[协议基础组件](../.agents/notes/implemented/architecture/2026-09-16-smb-protocol-primitives.md)，不证明端点、映射、文件适配或 Windows 可用性；固定原型诊断另按下节的来源核对。

### SMB 本机会话端点

[端点配置/拥有权](../packages/smb/server_test.go)验证显式配置、loopback listener 接纳与拒绝的关闭归属、Publish 预留及同名 Export 身份、busy Unpublish 和失败后重试。Shutdown 用例区分当前尝试错误与历史诊断；旧失败不能覆盖后来成功，同次等待者也不能被下一次重试改写。[会话 registry](../packages/smb/session_registry_test.go)核对全局 ID/限额、previous-session 的新 principal 授权、重认证和跨连接生命周期，错误身份不得触发 raw Close。

[协议 TCP](../packages/smb/server_protocol_test.go)与[连接用例](../packages/smb/connection_test.go)验证 bootstrap/3.1.1、签名、实际 credit/payload 边界、related compound、资源拒绝、AsyncID CANCEL 与 LOGOFF 等待；不能以未签名或虚构成功响应绕过资源错误。[authority session](../packages/smb/authority_session_test.go)验证共享 tree 的单份 FileSession/续期、旧回复不延长期限、失效 fencing 及不带每次 I/O Status 的拥有权路径。

[handle 用例](../packages/smb/handle_test.go)直接提供 neutral fixture 引用，核对 File/NodeReference 别名只关闭一次、返回错误的非 nil 引用仍保留名额、退役后晚到安装拒绝、borrow 排空和 cleanup attempt 结果不被覆盖；[authority 清理用例](../packages/smb/authority_session_test.go)核对失败关闭继续占连接/session/tree/open/export 额度，确认重试后才归还。fixture 安装不证明 wire CREATE 或真实 SQLite 文件操作，尚未接入的命令须按协议明确不支持。

[认证到期用例](../packages/smb/authentication_expiry_test.go)在主 session 保持正常流量时遗弃第二次初始认证，并检查无需新帧的回收；重新认证到期保留原 signer/身份。它分别覆盖旧 generation、到期与 Step/Close 串行、Close 失败仍占容量、连接取消排空，以及真实 TCP 的未完成交换。provider 在截止后返回成功不能安装身份，迟到 timer 不得销毁后来交换。[交换到期 fixture](../packages/smb/commands_session_test.go)在 synctest 中推进真实 HandshakeTimeout，等待 watcher 完成退役后再发 continuation，要求 SESSION_DELETED；它不通过手改 deadline 或允许两种状态来绕过顺序，非法 SID 的拒绝独立保留。普通/race 各 14 个 verdict 通过；只恢复旧 DENIED 期望的对照在确切状态码处失败，生产到期行为不变。

[退役用例](../packages/smb/session_retirement_test.go)暂停 TREE_CONNECT 创建者，再执行 LOGOFF/最后 frame 收尾，验证最后 opener 返回后 global session 名额最终释放。非 nil session 加错误与 Close 失败保留 authority/额度，成功重试才归还；仍活动 session 的失败打开保持可用，晚到响应的 signer 必须保留到签名完成。它们验证现有拥有权收齐，不引入额外原生关闭或另一生命周期。

这一批 packages/smb 自身普通/race 各为 54 根、87 个通过 verdict，覆盖 86.2%、最低函数 50%，vet 与 ARM64/AMD64 交叉构建通过；previous-principal 负向对照在预期授权断言失败。这些是 Linux 端点协议/拥有权与构建证据。[Windows ARM64 包级运行 35118013777](https://github.com/codetreker/remote-fs/actions/runs/35118013777/job/104868296920)另通过四包的 94 根、156 个 verdict，覆盖新增端点根及真实 SSPI，源码绑定和覆盖见[实现决定](../.agents/notes/implemented/architecture/2026-09-16-smb-protocol-primitives.md)。旧原生 SSPI 的 71 verdict 仍只属于其原 checkout；新的包级结果也不代替系统重定向器、完整文件/映射/缓存验收。

### SMB 名字、metadata 与 CREATE

[名字用例](../packages/smb/names_test.go)核对字面路径、UTF-16/guard 边界、目录后缀、ADS 拒绝和整个目录的非法/歧义检测。[resolver 用例](../packages/smb/namespace_test.go)验证原始名字与 NodeID、前缀 guards、完整 metadata 披露授权、预算和最终/中间缺席区别；遗漏前缀 guards 的隔离对照必须失败。Linux 使用私有测试比较器，真实路径解析在那里拒绝；[Windows 比较器](../packages/smb/name_compare_windows_test.go)的原生执行另计。

[Windows metadata](../packages/smb/windows_metadata_test.go)和[信息编码](../packages/smb/file_information_test.go)核对格式/CAS token 分离、其它 namespace、缺席/畸形/未知格式、结构属性、FILETIME 极值与未知时间拒绝。大小、链接数、pending、identity 和 granted-access 必须来自显式输入，不靠缺值造事实；这些 helper 测试不能代替 QUERY_INFO 命令路径的验证。

[CREATE 计划和响应](../packages/smb/create_test.go)检查 access/share/disposition、版本条件、最终叶名状态、QFid/MxAc/忽略字段与虚拟大小/serial；[打开生命周期](../packages/smb/create_lifecycle_test.go)核对效果前收费、已知零效果冲突最多四轮、取消和未知结果不得重发、部分引用/清理失败仍归原拥有者。成功响应使用原子结果，不能由后置 Stat 修补。已有 SUPERSEDE、symlink 与尚未支持的 disposition/options 必须拒绝。

名字/helper 聚焦普通/race 分别通过 10 根/14 verdict 和 11 根/46 verdict；CREATE 为 18 根/54 verdict。包含这些代码的 SMB 普通包级验证通过 104 根/222 verdict，自身覆盖 88.6%，149 个函数均至少 50%。vet 和 ARM64/AMD64 构建证据来自最终平台无关状态修正之前的对应源码；最终 CREATE 普通/race/profile 已重新执行。交叉构建不等于 Windows 原生执行，旧会话/SSPI 收据也不能替新增 CREATE 证明系统客户端、共享模式、映射或缓存。

### SMB 保留引用的字节与信息命令

[字节用例](../packages/smb/file_io_test.go)核对一次 ReadAt 的 Attr/数据、短读与 MinimumCount/EOF、populated result 加错误拒绝，以及普通/append/零写只调用一次、全量成功 Count 和任何错误都不重发。append-only 使用 native EOF，-2 current-position 与未支持 flag/channel 在效果前拒绝。原引用在名字替换后保持身份，close/cancel 等待真实借用排空；FLUSH 必须调用 Sync，不允许目录或 metadata-only 成功空操作。

[真实 HTTP 组合](../packages/smb/file_io_native_test.go)使用当前 SQLite/objectstore 引用检验 append、quota、同步和共享保护。clear/absent ARCHIVE 不阻止写入且保持原样，不能把另一次 metadata mutation 隐藏为普通写成功。有效大 metadata 与完整 64 KiB 数据请求在分别足够的预算内成功；占满与 CREATE 共用的 result pool 必须在 HTTP/native 效果前拒绝，归还后恢复。原 context callback 仍可拒绝支持它的 producer，但不能把 callback 当跨 HTTP 预留协议；取消/关闭后 charge 只归还一次。

[查询用例](../packages/smb/query_info_test.go)逐 class 核对一次 Stat/State 或完全不观察 backend 的 identity/access/known-EA 路径，以及 Windows class-specific rights 和当前业务授权。Standard 的 Attr/pending/detached 不混捕获，未知时间与畸形事实明确失败。固定 buffer 边界、SMB 3.1.1 error context、资源预留、关闭交错和忽略输入字段分别断言；声明 InputLength 即使无语义也必须参加 credits。

[filesystem 用例](../packages/smb/filesystem_information_test.go)验证 512 字节虚拟单位的余数、Total/Avail 与 max(Total−Used,0) 的不同来源、超限 Used、Space 不可用/不一致及 Unicode 显示名前缀。已声明虚拟 remote disk、保留大小写的 Unicode 名字与空 EA 集合不能扩大为物理 allocation、ACL、EA/stream 或未知 volume 创建时间。字节/查询聚焦普通与 race 各通过 35 根、131 个 verdict；最终组合源码的 SMB 全包普通与 race 各通过 139 根、353 个 verdict，无 fail/skip，自身覆盖 89.7%，166 个函数均至少 50%。vet、格式检查和 Windows AMD64/ARM64 测试构建与可移植用例选择核对通过。隔离对照分别删除捕获身份校验、重发未知写入、增加 ARCHIVE 前置限制、跳过查询权限及漏计声明输入 credits，均在对应语义断言失败。真实 authority/HTTP 组合用例只在 Linux 执行；Windows 构建核对其余可移植命令用例，不把 Linux SQLite/nativelease 当作已移植。当前源码的系统重定向器、映射和缓存验收仍独立。

### 当前 authority 的独立运行环境

[当前 authority 工作流](../.github/workflows/native-current-authority.yml)分别在 Ubuntu 构建、Windows ARM64 消费同一源码绑定 artifact；[工具说明](../.github/scripts/native-current-authority/README.md)拥有固定 kernel/QEMU/Go 输入、精确命令、DACL/Job Object 与清理规则。Windows HTTP client 通过固定 native ARM64 QEMU/TCG 连接真正 Linux AMD64 guest 内的 localstore/SQLite/nativelease，通过唯一 loopback 转发读写私有 ext4；这里没有固定原型或内存 authority 镜像。平台文件语义仍属于客户端，测试准备不移植生产 backend。

readiness 经真实 HTTP lease/FileSession 检查原子打开、字节、metadata 条件、retained rename/unlink 与逻辑 quota，然后停止并普通重开同一磁盘，验证 sentinel 的 ID/字节/metadata。成功要求实际 authority/QEMU 退出、guest sync/unmount、整个 Job 清空、流排空、私有磁盘删除和基底不变；监听端口或单独的根进程退出不能代替它们。Windows 收尾在根退出后最多五秒等待实际 job-zero，失败/超时才强制结束并最多再用五秒确认，总预算十秒不变；forced 始终使结果失败。Job accounting 的 ABI/ReturnLength 和唯一 waiter 的进程 handle 关闭归属分别验证。该工具始终把 native_acceptance 标为未运行，不能充当 SMB 或一秒可见性门禁。

[模板测试入口](../.github/scripts/native-current-authority/check-tooling.py)显式列举隐藏 Go 模板的根和 verdict，普通 module 发现不覆盖它们。已有 Linux 模板普通验证为 36 根/176 verdict，guest/probe/controller 的普通与 race 分别通过 87/63/26 verdict；Python 装配、进程拥有权和 checker 共 26 根；结构化源码发现只从 stdout 解码，依赖下载等 stderr 诊断单独流出，失败退出仍传播，不能把合法诊断混入 JSON 或丢弃。一次本地 Linux TCG 真正启动、HTTP readiness、普通重启和完整关闭通过；来源明确为本地脏 artifact，不冒充未来 CI commit。[native ARM64 运行 35423839231](https://github.com/codetreker/remote-fs/actions/runs/35423839231)的 controller/probe 分别取得 53/63 个通过 verdict，路径 guard 十二项通过，并取得真实 Linux init、私有 ext4 与 boot-ready。随后 start 确认在三十秒内缺失，authority 执行不能由空日志判断，强制清理仍使生命周期失败；HTTP、普通重启和 graceful 关闭没有因此通过。旧 x64 0xC00000FF 的具体 unwind table/module 仍未知。native ARM64 controller 的普通/race 14 根/35 verdict、架构反转对照及构建/vet只拥有各自证据，不升级为 SMB/缓存验收。Job rundown 的三个 AST 提取根普通/race 各 11 个 verdict、因果负向对照、vet 与两个 Windows 架构构建分别保留来源。mock/交叉构建、已执行的 Windows helper 与 Linux VM 结果分别记录。[决定](../.agents/notes/implemented/testing/2026-09-19-current-authority-virtual-machine-fixture.md)说明与固定原型诊断的分工，普通重启不宣称断电可靠性。

[路径 guard 回归](../.github/scripts/native-current-authority/run_windows_test.ps1)从实际脚本抽取唯一 Assert-NoReparse 的 FunctionDefinitionAst，在 StrictMode 下直接执行。入口及逐级祖先使用真实 DirectoryInfo/FileInfo 类型，不能依赖只有 provider 初始对象才有的 PSIsContainer；路径不存在、非文件系统 provider、文件或祖先 reparse 均拒绝。工作流在 VM 前运行此检查。本地九项通过，原 guard 对照在 raw DirectoryInfo 父链失败；Windows 已执行的十二项包含三个 junction 场景，不扩大为整段 bootstrap 通过。

### Windows 原生 SMB 缓存诊断

[诊断工作流](../.github/workflows/native-smb-gate.yml)在原生 Windows 11 24H2+ ARM64 上，以固定的 [SMB 原型](https://github.com/codetreker/remote-fs/tree/1cb9ad7f49d998de4daa4d562d766b18cf06ce16/packages/smb/windows)为基底验证系统重定向器的名字、属性和已打开文件缓存行为。基底只检出到 `.tmp/native-cache-gate/fixture`；[运行脚本](../.github/scripts/native-smb-cache-gate.ps1)核对 commit，检查并应用[通知连续性补丁](../.github/scripts/native-smb-notify-continuity.patch)，再注入[聚焦探针](../.github/scripts/native-smb-cache-gate_test.go.txt)和[通知回归用例](../.github/scripts/native-smb-notify-continuity_test.go.txt)。实际执行对象由基底 SHA、补丁 SHA256 与探针 SHA 共同确定，不与纯基底混称，也不编译工作分支正在实现的 SMB/backend。已观察结果与取舍见[诊断决定](../.agents/notes/implemented/testing/2026-09-16-native-smb-cache-diagnostic.md)。

十八个独立作业先运行基底的 Notification 测试及两项通知连续性回归，再运行 `TestNativeNegativeNameCacheGate`。测试使用 `-count=1`、三分钟超时，作业上限十五分钟。两项新增回归分别验证 rescan 交付后、重挂前的事件保留，以及底层来源更换后旧监听不能继续信任原来的注册；missing_final_status 与 positive 两种模式还先运行两项专用回归，分别验证父目录已核对和仅最终 CREATE 分支可改变状态；positive 另须取得读取完成分类用例的通过 verdict。每项具名回归及原生测试必须取得精确 pass verdict，任何 fail、skip 或缺失预期 verdict 都不能算通过。

补丁只在健康且 generation 未变的底层 stream 上保留已建立的目录监听，在 rescan 响应交付之后继续积累有界事件，避免下一次请求以新 checkpoint 跳过间隔。来源更换、来源失败或关闭仍使旧注册失效。它是平台通知逻辑的诊断 overlay，不改变通用 File 生命周期或当前生产包；unit 回归通过也不能代替相同原生场景。

| variant | 被检验的条件 |
|---|---|
| baseline | 不显式保留父目录句柄 |
| held_parent | 保留父目录句柄，但不提交变化监听 |
| notify_parent | 保留父目录，并先确认 SMB CHANGE_NOTIFY 已被接纳，核对根下创建的匹配通知 |
| owned_nested | 独立于应用 Stat 的客户端递归 UNC 监听；检查多层目录、100 ms 重新监听间隔及八个名字的创建突发。前两个名字仍须有匹配 ADDED；只有突发阶段明确丢明细时才接受 rescan 结果，十个名字的可见性断言全部保留 |
| owned_lifecycle | 应用句柄使普通卸载以 busy 失败时，监听、匹配通知与一秒可见性仍成立；应用关闭后，仅内部 UNC 监听存在时普通卸载能够完成 |
| owned_outage | 监听显式报告 HTTP/SSE 故障；原生负缓存到期前，同一缺失名字的查询必须返回不可用错误，不能继续报告不存在 |
| owned_rescan | 仅将诊断 fixture 的 MaxNotifyEvents 设为 2，在暂停时积累八次创建；先核对与请求关联的真实 wire ENUM 响应，再在响应后、重新监听前建立新的负查找并远端创建，验证一秒/缓存到期前/新权威 CREATE，以及后续监听与另一轮创建 |
| directory_sharing | 以实际核对为 NTFS 且父目录可写的本地子目录，对比 SMB 子目录；分别记录 NTFS volume 根与导出 share 根。覆盖 LIST/READ_ATTRIBUTES-only 及双方打开顺序，先排除无监听者时已有的共享冲突，并要求普通文件的读共享拒绝对照成立 |
| find_notification | 在实际 NTFS volume 根与 SMB share 根先确认 LIST/share=6 的无冲突基线，再分别以双方打开顺序检查 FindFirstChangeNotification 与该打开能否共存；成功的 SMB 通知句柄须有新的 Pending 响应，不兼容则失败 |
| missing_final_status | 单独施加最终缺失状态补丁；无监听、无显式父目录句柄时核对 wire 0xc000000f 与 Win32 FILE_NOT_FOUND，验证新查询及一秒/到期前可见性；通过后再持有根 LIST/share=0 重复另一名字，全程任何 CHANGE_NOTIFY 都失败 |
| positive_unheld / positive_exclusive | 分别无根句柄或持有根 LIST/share=0；每种模式运行十五个隔离 cell，检查路径/BasicInfo/StandardInfo/overlapped ReadFile/同步 ReadFile 的首次增长与缩短观察，以及 rename、replace、recreate 的路径或新打开结果 |
| positive_postdeadline_unheld / positive_postdeadline_exclusive | 各一个新的路径增长 cell，首个 ACK 后 GetFileAttributesExW 计划在 1.1 秒开始；只收集超过一秒的单向违约证据，作业通过也不代表一秒可见性验收 |
| positive_noleasing_unheld / positive_noleasing_exclusive | 实验协商不支持 leasing，分别执行原十五项即时 cell；保留旧引用 A 与新打开 B 的身份/字节断言 |
| positive_postdeadline_noleasing_unheld / positive_postdeadline_noleasing_exclusive | 不支持 leasing 的两个全新路径增长延迟观察；保持晚新值 inconclusive，与原能力基线分开记录 |

创建可见性检查先取得 authority 的 CREATE/NAME_NOT_FOUND 响应，再由独立远端入口创建该名字。成功需要在写者确认后一秒内、且在最早可能的缓存到期之前观察到文件，并取得该名字的一次新权威 SMB CREATE；要求逐名字通知的情形还必须有匹配事件，不能用 rescan 替代 owned_nested 的初次创建和普通间隔创建通知。重复 Stat 是测量观察点，不是产品同步机制。环境中三个 SMB 缓存 lifetime 都须大于一秒；写者确认或观察越过可能到期时间时，不能据此证明非 TTL 可见性。故障情形单独检查缓存到期前的错误，不以创建成功代替断线诚实性。

directory_sharing 记录传给每次 CreateFile 的 access/share mask 及实际 SMB CREATE 轨迹；成功建立的 SMB watcher 还须有新的 Pending 响应。目录子项的对照必须完整，不能用两个根的行为替代；无监听者时已经有 sharing violation 属于不能据此判断的前置冲突，不能算目标行为或被跳过。仅查询属性与列目录分别观察，普通文件对照必须拒绝冲突读打开。此诊断不预设“目录忽略 ShareRead”，也不从某个 root 的结果修改通用访问规则。

find_notification 只检查替代通知入口的共存性；即使通过，也还需要独立证明可见性、重新监听和断线行为。它只读调用 RtlNtStatusToDosError 并记录 `0xc000000f`、`0xc0000034`、`0xc000003a` 的系统映射，明确记录 wire status 未变。Win32 映射不能证明 SMB 缓存行为，也不等于执行了缺失状态码替换实验。

[最终缺失状态补丁](../.github/scripts/native-smb-missing-status.patch)用于 missing_final_status 和 positive 两种模式，并与通知连续性 overlay 分别记录 SHA256。它在父目录检查已成功、确定最终叶名不存在的 CREATE 错误分支，把 0xc0000034 改为 0xc000000f；中间组件失败、权限拒绝、普通 ENOENT、清理失败及未知/合并错误保持原处理。[专用回归](../.github/scripts/native-smb-missing-status_test.go.txt)验证这个边界。该实验没有把未知改成缺失，也不修改 authority API；新状态在原生重定向器上的行为只由自己的运行判定，不能由只读 RTL 映射相同推导。

[positive cache 探针](../.github/scripts/native-smb-positive-cache_test.go.txt)的即时十五项各自新建 fixture，不共用先前 cell 的缓存。通过真实 HTTP 准备内容及过去的 mtime：A 为 2001 年，预先准备的替换 B 为 2002 年；实际 mutation 的 ACK/Attr 为期望。被测 API 在 ACK 后的第一次结果独立保存在 First[]，后面的 HTTP oracle、其它 API 或诊断等待不能覆盖它。namespace 的打开和读取对照使用同步句柄，overlapped ReadFile 由独立 cell 检验；recreate_open 的名字缺失与重建两阶段均以 CreateFile 为首次观察，并先暖对应的负查找。每次测量都在 ACK 后一秒内，且早于准备目标前最早一次实际 root/CREATE 所确定的缓存期限，时间比较保留单调时钟。增长尾部、缩短后的 EOF、旧/新身份分别核对；同步 EOF 要求成功且读取零字节，异步 EOF 仅接受纯完成结果，合并的等待、取消或清理错误不能变成成功。recreate 是 rename-away 后复用名字，不增加 Remove shim，也不冒充独立 unlink 验收。

[有界 wire 观察器](../.github/scripts/native-smb-positive-wire_test.go.txt)解析完整 compound 链，按连接和 MessageID 关联实际 CREATE/QUERY_INFO/READ 与固定 metadata、字节摘要；QUERY_INFO class 34 的 FILE_NETWORK_OPEN_INFORMATION 同时解出时间和长度，以覆盖 Basic/Standard 原生 API 实际选择的线格式；[合成回归](../.github/scripts/native-smb-positive-wire-checks_test.go.txt)检验分片、related CREATE、跨连接隔离、迟到响应、字段不完整与敏感内容排除。认证 token 和文件原字节不进入产物。positive cell 必须无 CHANGE_NOTIFY、无授予缓存权限，证据缺失/溢出及清理残留都失败；合成测试或 ARM64 构建通过不代表原生 positive cell 通过。

基线延迟观察的两个 cell 保留原型的协商能力 0x26 与 lease State NONE；独立 NoLeasing 变体使用下述无 leasing 策略，两者都不改变原有一秒 validator。 每个延迟变体先在独立 setup fixture 完成真实 CREATE/WRITE/无缓存授权证明，检查原生句柄关闭、移除映射、SMB/HTTP 停止及引用/连接/pending/cleanup 归零后，才新建测量 fixture/share。测量 share 没有 proof file、不执行原生数据写入，使用自己的空 trace、曝光起点与实际协商检查；即时矩阵保持原来的证明顺序。路径属性先暖旧值，真实 HTTP 增长确认后，在首个查询前不调用目标或相关目录的任何原生文件 API；开始写者前须无未完成的 warm-up 请求，ACK 到查询开始之间也不能有目标/目录 SMB 请求或响应。首个 GetFileAttributesExW 的实际开始必须严格晚于 ACK+1 秒，开始和完成都严格早于原始最早缓存到期；单调时间差和 UTC 时间戳分别记录。调度超窗、错误或轨迹不完整使样本无效，不重试同一缓存样本。独立 fixture 完全退役是拥有权边界，不是后台永远无请求的假设；测量安静间隔仍不豁免任何因名字不同而出现的额外流量。

首个查询返回后立即保存原始 tuple 和 matches_warm_tuple；无效时间、原生错误、pending/quiet 或证据失败记录为 invalidation，仍尝试 HTTP oracle 和普通清理。HTTP oracle 核对身份、revision、大小、mtime 和内容未再改变，不覆盖原生首值。旧 tuple 或任一旧分量只是待核实的违约候选，必须等本体、句柄及最终卸载/资源清理全部成功后才记录 violated；其它失败使结论 inconclusive，即使资源计数后来归零。当前 tuple 只证明这次较晚观察已新鲜，必须记 `within_one_second_proven:false`、`contract_result:inconclusive`；新 SMB 请求也不能证明一秒内已可见。工作流把这两个作业标为非 SLO 验收，绿色的证据收集不能关闭一秒门禁。即时十五项与延迟单项保持独立，不推断缩短、改名或其它 API 的延迟结果。

[NoLeasing 补丁](../.github/scripts/native-smb-no-leasing.patch)只用于名字中带 noleasing 的四个变体，依次在通知连续性和最终缺失补丁之后检查并应用，分别记录 SHA256。 这三份目标 Go 文件与将应用的补丁在基线 overlay 完成后规范为无 BOM 的 LF；原始 hash、规范输入/补丁 hash 和输出 hash分别记录，并逐一核对已审查常量。仅该次 git apply 使用 core.autocrlf=false，先 --check 再应用；规范化结果必须与原审查代码逐字节相等，未知输入或任何不符直接失败。它将正式/通配协商均设为 LARGE_MTU=0x4，最终 dialect 仍为 SMB 3.1.1，保留签名与 preauthentication；不分配 lease table/owner，不调用 lease-key preflight/commit。原 lease 身份检查源码保持原样。RqLs 内部字段在不支持 leasing 时忽略，CREATE/context 外层长度、offset 和 alignment 仍验证；成功 CREATE 的 oplock 为 NONE 且没有 RqLs response，未经请求的 ACK 明确拒绝。

[七项专用测试根](../.github/scripts/native-smb-no-leasing_test.go.txt)在原生场景前必须各有 pass；它们覆盖协商/签名、仅忽略内部 lease 字段、外层边界、替换前后引用身份、打开拒绝和已完成动作核对、授权/无请求 ACK 以及清理。QueryAction 恢复到 Completed 的成功不代表仍未知的结果已被清理。实际 native trace 还须核对每次协商的 capability=4、最终 3.1.1，以及全程没有缓存授权；即时场景的普通 proof-file CREATE 为 NONE/无 lease response，延迟场景由完全退役的 setup fixture 单独提供该证明，测量连接仍核对自己的协商与响应；旧变体的 0x26/0x6 与 State NONE 检查不放宽。即时和延迟使用独立 share/target，不把实验结果与基线合并。当前源码的原生 SSPI 作业仍不使用任何原型 overlay。

映射拥有权在 Map 前写入并同步临时记录，再原子改名。测试进程异常退出后，Verify 只清理与该记录精确匹配、且不在运行前基线中的 Local/Remote 映射；发生恢复仍将该次测试判为清理失败，不把管理员清扫变成成功。每个 positive cell 单独保留首结果、完整关联记录、authority metadata/digest 与最终资源状态。

产物记录 OS/build/架构、基底及探针 commit、实际通知/缺失状态 overlay SHA256、缓存策略、逐名字操作时间、SMB/authority 事件、映射和最终引用状态。轨迹最多保留 2048 项，溢出或编码失败使该次证据失败；递归监听由单独的测试拥有者驱动，不依靠应用调用推进。Windows overlapped 通知在取消完成前保留其缓冲和结构。零字节成功或 ERROR_NOTIFY_ENUM_DIR 被明确记录为丢失明细并重新监听，不代表空目录或没有变化；其余异步完成或事件关闭的异常清理错误同样失败。工作流保存诊断产物，并在环境准备成功后核对缓存策略未变且没有新增 SMB 映射残留。

owned_rescan 必须从实际捕获的 SMB Command 15 请求及同一 MessageID 的 `STATUS_NOTIFY_ENUM_DIR`（0x10c）响应证明前置状态，仅有本地零字节不足以开始验证。新名字的权威负查找与远端创建发生在该响应之后、重新监听之前；恢复后还须取得新的 Pending 响应，并在另一轮创建后仍保持健康监听。夹具不维护目录快照，因此这个用例验证的是丢明细后的重新监听和后续缓存观察，不宣称目录枚举已经恢复完整。

这些监听拥有者和受控间隔属于固定原型中的测试夹具，不定义生产 ManagedShare API。实际交付的 package、transport 和 backend 仍须独立验收目录、属性、改名、删除、已打开引用与故障；固定内存 authority 不证明 SQLite 持久性、全部 Windows 行为或历史时间投影。某个 variant 的成功不能扩大成其它 variant 或新实现通过，原生可行性门禁只按实际运行证据关闭。

### 祖先目录通知与属性失效实验

[独立工作流](../.github/workflows/native-smb-parent-invalidation.yml)通过[运行脚本](../.github/scripts/native-smb-parent-invalidation.ps1)执行 `TestNativeParentInvalidation`。它只由自身三个文件的 Pull Request opened/synchronize 变更触发，以单个十五分钟 Windows 11 24H2+ ARM64 作业运行；原十八个诊断作业保持独立。临时检出仍固定为上述原型，依次核对并应用通知连续性、最终缺失状态与 NoLeasing overlay，记录规范输入、补丁及输出 hash。这个执行对象不包含当前生产端点或新的文件适配。

[探针](../.github/scripts/native-smb-parent-invalidation_test.go.txt)使用真实 share 根及其真实子目录 `v`，在父目录保持递归监听，在 `v` 保持应用 LIST 句柄；这只模拟额外父目录的几何关系。七个独立 share/connection cell 保留四项增长（应用 ShareAccess=0/6 与 watcher-first/app-first）和一项 share0 noWatcher 增长对照，另以 share0/app-first 检查缩短与替换身份。实际 CREATE/CHANGE_NOTIFY 必须证明父、子、目标来自同一连接和 session，应用 mask 与当前 Pending 均须核对；任一打开顺序的共享拒绝不能当作通过。无监听对照不证明 ShareAccess=6 的因果关系。

增长先以真实 HTTP 准备 257 字节及旧 mtime，再两次暖 GetFileAttributesExW，由保留引用的 HTTP WriteAt 增长至 769 字节；缩短从 769 字节以 HTTP Truncate 缩至 73 字节。已完成收据提供期望身份、大小、时间和 ACK。native-call ledger 用连续、唯一的开始/结束序号及 mutation 序号判定操作先后，原时钟继续约束持续时间；相等时钟不代表准备访问跨越写入。写入后禁止 harness 额外访问目标或祖先及提前查询 oracle，只允许规定的通知等待与首次观察。增长/缩短须同时取得当前请求的 wire 与原生 `FILE_ACTION_MODIFIED v\target`。自动 redirector 刷新可以发生在 HTTP ACK 返回或首次 API 之前；新 tuple 的目标响应必须与通知、连接及请求精确关联，分别记录 `refreshed_before_first_api` 与 `refreshed_by_first_api`，来源不清则为 inconclusive。首次查询完成仍须在 ACK 后一秒内，包含通知等待，并早于最早实际曝光的原缓存到期。

替换场景只预先打开原生 A 句柄，B 通过 HTTP 创建和准备，不能先打开 B 向 redirector 提供它的身份。mutation 前快照与最终排空的完整轨迹都核对冷态：整个轨迹不得出现 B 的 CREATE/QUERY_INFO/READ，直到 ACK 不得出现目录枚举或无法关联的 file ID。最终核对只能维持或撤销先前资格，不能把已失败的证据升级；迟到的 B 暴露使样本和测试失败，保留原始首值及身份结果。HTTP 把 B 改名覆盖 target 后，首个目标观察是同步 CreateFile，随后核对同一 volume 中新旧原生 ID 不同、旧 ID 不变、旧句柄仍读 A 且新句柄读 B；原生 ID 不与 authority NodeID 作数值等同。首次打开及四项后续身份/读取检查全部须在 ACK+一秒及原缓存到期之前完成，后置 oracle 分别核对 A、B 和源名字移除。

替换通知明确区分 DETAIL 与 VERIFIED_RESCAN。DETAIL 必须匹配排空的 native 完成与同一 wire 请求/响应，先有 target 的 REMOVED，再有同批相邻的 replacement OLD_NAME / target NEW_NAME。只有当前请求的真实 STATUS_NOTIFY_ENUM_DIR（0x10c）与原生零字节或 ERROR_NOTIFY_ENUM_DIR 同时匹配，才能进入 VERIFIED_RESCAN；一旦进入便保留 rescan_required=true、precise_notification_proven=false，不把随后事件拼成完整精确通知。

两条路径都最多处理三次完成，沿同一 watcher 句柄、连接/session/filter/递归属性排空后重挂，最多三次重挂且每次核对新的实际 Pending，不能重开路径。rescan 后还核对 native event 尚未完成和 wire 观察顺序；最终排空轨迹若显示首个 API 之前已观察到 terminal，则撤销资格。序号解决相等时钟下的观测先后，readiness 检查只证明该检查点，不证明内核未来不会交付完成。所有等待/重挂共用 ACK+850 ms 的绝对通知截止，首个打开、身份与 A/B 字节仍遵守一秒/原 TTL。rescan 成功使用独立的 current_replacement_identity_after_verified_rescan_before_deadline 分类，只证明丢明细后该次身份/字节观察，不宣称精确通知或目录枚举恢复。

首值保持不可变，后置 HTTP oracle、完整轨迹、原策略及清理均为证据资格。合格的 watched 旧首值使候选测试失败，但一秒内的旧样本不是超过一秒的违约证明；noWatcher 的合格旧值或新值只作观察，均不证明一秒保证。取消 overlapped 通知必须等待完成再释放缓冲。三个解析/关联/资格测试根必须先于原生根通过，协议补丁的具名回归也不能缺失；所有测试使用 `-count=1`、三分钟上限并严格检查 fail/skip/verdict，产物保存十四天。

[五场景运行 35411939518](https://github.com/codetreker/remote-fs/actions/runs/35411939518)保留整体失败及相等时钟导致的 inconclusive。[七场景运行 35414601479](https://github.com/codetreker/remote-fs/actions/runs/35414601479)的四项增长和一项缩短取得合格当前值，noWatcher 仍只返回观察用旧值；替换在合法 0x10c/零字节通知处中止，尚无首次打开、身份或字节样本，整体仍失败。未赋值的 cold_source=false 不证明 B 被预热；历史结果不按新判据追改。完整来源和资格见[诊断决定](../.agents/notes/implemented/testing/2026-09-16-native-smb-cache-diagnostic.md)。

当前 typed-rescan 源码的精确 PowerShell 准备、actionlint 与完整 Windows ARM64 探针构建通过；三个可移植测试根普通/race 各 144 pass，使用限定 Windows 常量/Filetime shim。仅接受 detail 的对照在真实 ENUM 配对断言失败，移除 readiness 观察序号的对照在相等时钟下提前完成的断言失败。[typed-rescan 原生运行 35419230739](https://github.com/codetreker/remote-fs/actions/runs/35419230739)仍失败：四项增长和缩短合格，替换在有效 ENUM/重挂证据下取得同一 native FileIndex 的新旧句柄，虽然各自读取 B/A 正确字节。身份/字节检查在 ACK 后约 20.5 ms 完成，这是合格身份不一致，不能称为超过一秒的旧值证明。原 QFid 仅旧 CREATE 请求，新 CREATE 未请求，原观察器未记录其数值响应；因果仍需精确证据。生产父目录布局、映射、完整名字/身份与故障行为仍需各自证明。

### 精确历史通知与替换身份实验

[独立 precise 工作流](../.github/workflows/native-smb-precise-invalidation.yml)通过[运行脚本](../.github/scripts/native-smb-precise-invalidation.ps1)只执行一个全新 `TestNativePreciseReplacement/share0_app_first_replace_identity_precise`。它仍使用固定原型和既有三份 overlay，再核对并应用[历史通知补丁](../.github/scripts/native-smb-precise-history.patch)；这些字节只进入临时诊断检出。父探针 Go 模板及默认判据不变，公共 PowerShell 使用受限的产物目录选择与对应 precise binary 清理。补丁目标须先通过原始/规范 hash 核对，才允许将已知 CRLF 字节规范为 LF，随后再核对规范化/patch 输出；未知输入在重写前拒绝。Verify 成功明确退出 0，不能继承旧外部命令状态；真实策略/进程残留错误仍抛出。工作流保存本次输入/输出来源、首值及清理产物，不能把执行对象叫作当前生产适配。

[历史事实 producer](../.github/scripts/native-smb-precise-history_test.go.txt)先从实际 HTTP Snapshot 读到语义 EOF，核对 incarnation、完整树、位置与预算；最多 64 节点/256 KiB，证据序列总量最多 1 MiB。原 Rename 的同一次 authority 锁区间捕获 P/Q 和实际 Removed/Renamed 事件，绑定 session/reference/action、输入指纹与已保存的原 action receipt。实际 RenameWithBarrier 回复及其 HTTP barrier 必须与这些事实相符，真实 stream 交付的完整两事件区间也须一致，才能交付历史 full-name proof；不以测试预期拼造 mutation、位置或通知。

历史位置证明只回答修改时的名字事实；当前读取授权、存活引用、完整祖先及 sibling 的表示/歧义检查仍由独立披露路径执行。证明按 group 保留，验证时 pin，丢弃、关闭与失败均归还；Close 排空正在验证及延后 release，未知来源或 proof 失败不能降成成功。新单项必须取得完整 DETAIL，ENUM/VERIFIED_RESCAN 会结束本次验证，不能沿旧 rescan 成功条件继续。

[被动身份观察器](../.github/scripts/native-smb-precise-identity_test.go.txt)只解析已有流量的 QFid、QUERY_INFO 6/18/59 和 filesystem class 1；它不主动发查询。原始 SessionID 与可证明的同 compound effective SessionID 分别记录，按 connection/message/command/open 关联请求和响应；未请求、缺失、畸形与数值零分开。关联不清或轨迹超限使证据失效，不从 backend NodeID 猜原生 FileIndex。

HA-only、HTTP-only 冷 B、首次 CreateFile、新旧 native ID/volume 与 A/B 字节保持原判据，通知与所有观察仍受原 ACK+850 ms/一秒/最早 TTL 约束；默认缓存设置、后置 oracle 和清理不变。[producer/身份回归](../.github/scripts/native-smb-precise-history-controls_test.go.txt)与[通知拥有权回归](../.github/scripts/native-smb-precise-notify-controls_test.go.txt)合计 26 根，普通/race 各 285 个通过 verdict；四个独立对照核对 proof 失败、ENUM 误当 DETAIL、未绑定 QFid 与错误 compound session 处理。另有十项源提取 PowerShell 进程选择用例、错误源码 pin 拒绝、精确 fixture 准备、ARM64 构建和 actionlint 检查。CRLF 准备修复的验证从 559 份 CRLF Go 输入得到与当时组合相同的九份 Go 输出及 ARM64 binary；这份字节相等证据只归于该准备修复，不覆盖后续 QFid 策略变化。首次 precise 作业的准备失败仍不提供 native 结果。[已执行的单项 35423839239](https://github.com/codetreker/remote-fs/actions/runs/35423839239)随后取得完整 DETAIL，但成为合格的 replacement_identity_aliased：新句柄读 B 的 769 字节、旧句柄读 A 的 257 字节，二者仍报告相同 native FileIndex。全部身份/字节检查在 ACK 后 29.552 ms 完成；这不是超过一秒的陈旧证明，也不把 typed-rescan 的原失败重分类。原 QFid 响应的 DiskID=3 已被动捕获，新 CREATE 没有请求或返回 QFid，也没有身份 class 6/18/59 回复。来源、历史区间、冷 B、oracle 和清理均核对，仍不能推断 redirector 内部算法或已找到修复机制，更不代表当前 production/F guest/系统缓存验收。

### QFid 响应的独立互操作实验

[QFid 工作流](../.github/workflows/native-smb-qfid-invalidation.yml)只运行一个新 share/connection 的 `share0_app_first_replace_identity_always_qfid`，使用精确 DETAIL 实验的固定原型和历史通知证明。经校验的 IdentityContextPolicy 选择器默认仍为 requested-only；仅 always-truthful 应用[响应补丁](../.github/scripts/native-smb-qfid-context.patch)与对应控制。前述已完成的 requested-only 身份失败保留为对照，不改请求、挂载、缓存参数或原生观察来制造成功。

补丁在 CREATE 请求/context 校验后、打开准入前，实际读取一次 backend State 并要求已知 VolumeIdentity；真实零 serial 与未取得状态分开，错误不得制造身份。成功回复从同次 opened.Attr.ID 与该 volume serial 构造一个 32 字节 QFid，后 16 字节为零；已请求的 context 保留原次序，未请求时仅在私有响应列表追加，不修改借用的请求字节。重复/畸形请求仍拒绝，响应/frame 界限、失败打开和清理/fencing 保持。这个未请求响应的协议符合性未定，只用于原型互操作诊断，不是生产默认政策。

被动证据必须证明选定新 CREATE 未请求 QFid、成功响应恰有一个真实 B/volume tuple；如果系统实际请求了它，未请求响应这一因果条件不成立，不能算该实验成功。是否接纳响应、新 native ID 是否与 HA 区别、HA 是否稳定分别记录；新旧都变成 B 的身份仍失败。HA-only、HTTP-only 冷 B、首个同步 CreateFile/legacy identity API、A/B 字节、完整 DETAIL、原 ACK+850 ms/一秒/最早 TTL 和清理均保持，不补发身份查询或要求 native FileIndex 数值等于 NodeID。

[codec/准入控制](../.github/scripts/native-smb-qfid-context-controls_test.go.txt)及原有适用控制合计 46 根，普通/race 各 380 个通过 verdict；恢复 requested-only 回复和删除原生 alias 拒绝的两个隔离对照分别失败。七项脚本策略控制与 default/always 的实际 fixture 准备、ARM64 构建核对完成；default 的三份原型源码输入不变，改变响应策略的分支使用新的严格 no-leasing 控制，不借原零-context 断言充数。新单项尚未原生执行；本地结果不能证明协议符合性、redirector 内部身份机制或缓存修复。

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

观察结果预算的昂贵准备与被测读取分开。[目录硬字节用例](../packages/metastore/sqlite/directory_metadata_test.go)仍用 32 KiB metadata 准备原 8 MiB 边界可容纳数量加一项，在一个真实 s.mutate 中逐项调用原 namespace prepare/apply，随后核对实际行数、完整 payload 和 volume 完整性；被测零收费 callback、硬上限与失败后整份结果不可读的断言不变。[超长名字用例](../packages/metastore/sqlite/name_observation_test.go)使用 1 MiB BLOB，仍为 4096 字节上限的 256 倍，并保持分配少于 payload 四分之一、budget callback 未调用及纯 State 可用的判据。实际提前载入的负向对照必须触发分配断言，不能只靠减小输入使测试变快。

#### 迁移与完整性断言

迁移用例从独立手写的 v1/v2 数据库开始，不用当前 migration 反向构造历史。只含 referenced object row 且结构、计数、`sqlite_sequence` 与日志 tail 一致的旧库必须前滚到当前 schema；含任何 non-referenced object row 的 v1/v2 库必须以 `EIO` 拒绝。这条用例同时防止旧的零字节 pending 记录绕过当前 byte threshold，以及旧的 time-derived garbage 被新清扫器误当作 ownership-proven 对象删除。v2 retained changes 在迁移后必须为空、incarnation 必须改变、tail／trim 必须归零，node/change 高水位必须覆盖迁移前的全部 surviving reference 与 sequence；迁移后的第一份 node/change 严格使用更大的值。已完全 trim、`committed_position = trimmed_through > 0` 且没有 surviving row 的合法 v2 日志也必须可以迁移。

当前 schema 与历史迁移都要用 corruption fixtures 验证每个 volume 的可见节点恰是一棵 rooted tree：root 无 incoming entry，其余可见节点恰有一个同 volume parent，并且全部可达；cycle、未标记的孤儿与跨 volume entry 都失败。v6 按种类验证所有保留对象，detached 不进入可见树，普通文件内容和符号链接目标仍计入用量，空 detached 目录不能接受子项。另用整数、负数、溢出与 mismatch fixtures 验证`volumes.used`等于普通文件内容与符号链接目标 size 的 streaming sum。storage-class fixtures 把 entry/change name 改成 TEXT、把 schema/database/binding/log scalar 或 nullable change group 改成错误的 NULL/type，并构造非法 kind、position、metadata、size 与 nanoseconds；snapshot/log 不能漏行或接受 driver coercion 产生的可信零值。日志 fixtures 删除中间或尾部 retained change，并分别篡改 `previous_position`、`trimmed_through`、`committed_position`：`Open`、`Snapshot` 与 `ObjectStatus` 的完整链验证必须以 `EIO` 失败且不写入新 incarnation；`Since` 允许缺口之前的完整 page，跨到缺口的 page 必须整体失败且不暴露该页 prefix。空或较小日志在 caller 给出很大 limit 时仍按实际 anchor/row work 成功，page budget 耗尽且 tail 尚未返回时才以 `EFBIG` 拒绝。整链 record ceiling 由 `Open` 与 `ObjectStatus` 的精确边界／超限用例直接覆盖；snapshot row production 另有 caller-owned byte-budget 故障用例。`MaxIntegrityBytes` 用当前 schema 的精确边界与超大 corrupt entry name 验证 `EFBIG`；legacy migration 直接覆盖 record ceiling。ID fixtures 删除 node/change sequence、协同回退 internal high-water/sequence、把高水位压到其它 volume 的引用之下，并覆盖 node/change exhaustion；另抬高 committed tail，断言 append 在发布新 witness 前回滚。本地 durable fixture 再用外部 accepted state 拒绝协同回退。每一种拒绝都要断言版本、schema 与数据没有部分前进；volume 结构在 `Open` 与 `ObjectStatus` 两条入口覆盖，database-wide sequence/witness 拒绝由 durable open、page anchor 和分配路径覆盖。

v6 迁移与[metadata 完整性](../packages/metastore/sqlite/internal/schema/client_metadata_test.go)另验证旧 mode 转换前 preflight、历史 change 独立属性、Unknown 时间、directory revision、intent 关联及 metadata_used/triggers 的真实一致性。1–5 迁移内容保持不变；新的映射、SQL commit 与 witness Accept 的退出/未知结果只允许完整旧/新状态。可见 v6/G+1、accepted G 仅在必要 WAL 证据存在时可重开，迁移不重跑但普通 Prepare 继续推进 generation；schema version 与 witness State 分别判断。

### 本地持久对象存储

[shard 描述符用例](../packages/storage/objectstore/localdisk/objects_test.go)在真实私有对象目录中分别驱动 Get、GetBounded 与 Delete：占住唯一 active 名额，确认 shard 已打开且调用到达饱和的提升等待分支后取消，重复操作结束时按 device／inode 识别的 `/proc/self/fd` 目标 shard 描述符数恢复基线。单纯取消保持 EINTR，waiting／active、byte 预留与 key／shard 协调资源释放，归还名额后正常操作仍可完成；另在打开 shard 后注入健康失败，核对 EIO 与 FD 基线。[既有对象用例](../packages/storage/objectstore/localdisk/localdisk_test.go)继续核对缺席、打开及身份校验失败的错误语义；缺席判断仍位于 admission 与健康检查之后。

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
- 修改返回成功后构造 intact、缺失、空和仅含 header 的 WAL，结合 `A = C` 与 `A > C` 验证重开只接受可证明状态；已发布 final 的 checksum、store/volume/database identity mismatch 和 visible generation 前进／回退分别覆盖；未发布 stage 的内容与清理按下述独立矩阵验证。`Accept` 失败必须 poison 并阻止读取；snapshot pin 使 status 报告 pending checkpoint，后台重试在释放 pin 后推进 C，真实 checkpoint error 保留到成功。 这些测试的独立磁盘见证读取在新开 anchor、读取 final、严格文件校验、解码及关闭期间持有活动 witness 的发布锁，避免 checkpoint 原子替换使已打开的旧 inode 失去链接。[观察器回归](../packages/storage/localstore/witness_read_test.go)分别验证被替换旧 inode 仍因 nlink=0 拒绝，以及同步观察读取替换前记录、释放锁后读取新记录；生产 owner/type/device/mount/link 校验保持原样。open-time `Accept` failure 要保留可供下一次恢复的 WAL，成功 `Close` 则只在完整 checkpoint 和见证同步后释放 persistent WAL；清除调用失败保持可重试且不关闭 writer，不断言 flag 的最终值；
- data-plane 饱和时 status 仍通过 dedicated control slot 报出 waiting/active/in-flight-byte 数；组合 status 另报告 accepted/checkpointed generation、pending 与 checkpoint error。SIGHUP 成功输出 checkpoint generations/pending、SQLite reader、integrity-record/name-byte 与 effective sweep interval/batch，且有 deadline；checkpoint error 时只输出整次 status failure，不格式化 partial figures。关闭时先排空 handler、maintenance、waiting/active data-plane 与 control operation，并等待 checkpoint worker 退出，再建立 5 秒内部 context 约束 commit-gate admission 与 checkpoint；active reader 立即返回 `EBUSY`。reader pools 已关闭后的 checkpoint、见证或取消 failure 要断言 writer、所需 WAL 证据与磁盘锁保持，并发调用共享本轮错误且后续 `Close` 能从部分关闭状态重试；`PERSIST_WAL` 清除失败只断言 writer/ownership 保留且可重试，不断言 flag 或 WAL 的最终状态。pool close failure 要断言进入 terminal result、后续返回同一错误且所有权保留到进程退出。post-metastore `Open` failure 另验证 bounded close 失败后对未暴露 store 执行 `Abort`，无错误关闭前不会释放 object/root ownership；SQLite constructor pool cleanup failure 则验证 ownership-retained error 使内部 coordinator 与外部 root lock 都留到进程退出，普通构造失败仍释放。

[见证 stage 用例](../packages/storage/localstore/witness_test.go)以严格 final 为 A/C 来源：构造空、短写及完整长度撕裂内容，核对经过原 DB／WAL／高水位对账后清理并重开已确认状态；完整且内部有效的 foreign store、volume 或 database 记录仍失败。同身份 stage 的高低计数与无 stage 的正常重开对照，不能改变来自 final 的 A/C 下界。stage-only 继续校验完整内容与实际数据库身份，不被提升为 Accepted。另覆盖不安全 entry、长度越界及打开／读取／关闭失败，确保这些错误不被当作可丢弃的内容错误；DB／WAL 对账失败时不提前清理 stage，删除和目录同步失败保留原 cause 并阻止成功；live Checkpoint 的这两类故障还核对同一实例上的成功重试。

durability 用例在修改操作返回成功后关闭并重新打开组合 store，从另一条读取路径验证名字、属性、内容、用量、change log、database generation 与见证都存在。corruption 用例逐项修改磁盘文件，断言打开或受影响操作以 I/O 错误失败，不能只断言进程没有 panic。

仓库内的 crash 验证区分 barrier fault injection、人工构造的 recovery residue 与真实子进程终止。已有用例在 mutation 返回成功、WAL 仍被 snapshot pin 住时发送 `SIGKILL`，覆盖未执行正常 Close 的整体恢复；同一文件中的真实发布用例分别在 Accept／Checkpoint 的 stage 创建、部分写入、完整写入、文件 fsync、rename 和 root fsync 六个位置终止进程，观察 final／stage、原 A/C 与重开的数据。构造零字节 stage 的历史回归不冒充在运行中的 checkpoint 系统调用之间杀进程，未返回成功的 Accept 也不被称为已确认修改。这些用例均不宣称模拟电源中断、设备写缓存或文件系统掉电恢复；真实 filesystem 与硬件是否兑现 crash-time `fsync` 语义仍是部署前提。

## 挂载相关的测试是独立的一套

它们需要 `/dev/fuse`，且卸载不总是第一次就成功；CI 因此把这一层单独放进一个 job，并给 `go test` 一个比 job 更短的 `-timeout` —— 卡住的卸载要留下 goroutine 栈，而不是被超时静默杀掉。单元测试必须在任何地方都能跑 —— 那是它们真的会被跑的前提。

**每一个挂载都在 `t.Cleanup` 里拆掉，拆不掉就让这一轮失败。** 留在机器上的挂载点或 FUSE 连接会拖垮之后的每一次运行，而留下它的那一轮通常已经报告成功了。会挂载的两个包的 `TestMain` 因此都走 `fusetest.Run`：它在跑用例之前把 `TMPDIR` 指到一个本次运行专用的目录，跑完只报告那个目录下面还挂着的东西。

**一个检查只能对它归属得了的东西下结论。** 挂载表是全机器共享的，而 `go test` 一个包起一个二进制并行地跑，所以「表里有一个 `fuse.remote-fs` 挂载」这句话说不出它是谁挂的 —— 曾经据此判红的那个检查，抓到的是隔壁那个包的挂载点，也会抓到机器主人自己挂着在用的那一个。归属靠的是那个专用目录：`t.TempDir` 在它下面建目录、子进程继承 `TMPDIR`，于是用例能拿到挂载点的每一条路径都在它下面。

CI 在这一层跑完之后再查一遍机器：没有 `fuse.remote-fs` 挂载、没有多出来的 FUSE 连接、没有活着的 `fusermount` 进程。**那一遍扫全机器是成立的**，因为它跑在所有 `go test` 进程都退出之后、跑在一台跑完就扔的 runner 上；进程内那一遍两个条件都不具备，所以它不去扫全机器。进程内那一遍也看不到 panic 或 `-timeout` 之后的状态 —— `TestMain` 那时拿不回控制权 —— 那一段同样由 CI 这一遍兜着。

**在 CI 里，跳过就是失败。** 没有 `/dev/fuse` 时这一层会跳过自己，而 `go test` 照样退出 0 —— 那份绿色和「全部通过」逐字一致，而这两层是整个系统能工作的唯一证据。所以 CI 不看退出码就下结论：它检查有没有用例被跳过、有没有用例连结论都没给出。要在本地复现这一层，先确认这台机器真的能挂载，而不是让它替你把问题跳过去。

推送前只跑覆盖这次改动的检查，不跑全量；穷尽是 CI 那次全量运行的职责，它自己会启动，不需要谁记得去按。**没跑过的检查不许说通过。**
