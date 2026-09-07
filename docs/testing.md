# 测试策略

本仓库怎么测：分层，以及那些让「全绿」有意义的规则。命令在根 [AGENTS.md](../AGENTS.md)；理由在对应的 Agent Note 里。

## 分层

- **契约** —— `storage` 接口的义务写成一套可执行的用例，住在任何实现之外。普通目录、本地持久对象存储、HTTP 另一端与其它实现都跑同一批用例。**每一种实现通过同一套用例，是「这个接口是一层抽象、而不是对某一份实现的描述」的唯一证据。** 新的 storage 义务加进这套用例，而不是加在某个实现旁边 —— 只在一处验证过的义务，其它实现不会知道它存在。server backend 另跑 `storage.BoundedStorage` 契约：依赖在服务前可被校验，Read 在完整 payload 分配前拒绝超限，List 在保留越界 entry 前拒绝，且取消会停止产生结果。`objectstore.Objects` 与 `metastore.Store` 各有自己独立的契约套件；metastore 套件还验证 bounded `Incarnation` 在复制 identity 前拒绝超限、`Barrier` 原子返回同一 identity/committed position，并验证 `Since`/`Next` 在载入变长 payload 前预算、页满时不跳过下一项、单项超限与 production error 使整页不可读取。组合层再验证跨接口的顺序与错误保存。
- **单元** —— 核心逻辑：路径清洗与越界拒绝、errno 映射、打开文件的那份缓冲区、FUSE 单文件上限、HTTP request/response 的单体、operation、waiter 与 aggregate byte 上限、stream 的单帧预算、derived event/cursor products 与 snapshot-page admission、subscription 与 snapshot 上限、replica mutation confirmation 的 active 与 waiter 上限、quota measurement 的单目录与 frontier byte 上限、local-disk object active/waiting/byte 上限、SQLite reader-connection 与 integrity-record/name-byte 上限、持久 ID 高水位、日志 predecessor chain、WAL 见证与 checkpoint 状态、消息编解码与 URL 往返。**不需要挂载点、不需要 `/dev/fuse`、不需要特权** —— 一份本地目录 storage，或者直接构造的内部结构，就足以驱动它们。每项上限分别验证边界值、超限错误、并发 admission 与取消，不能用一项恰好更紧的上限代替另一项的测试；write 上限低于 protocol body 时，超限 write 必须失败，而落在 protocol body 内的 listing 仍须成功。request 与 response admission 分别占满后，用例断言有界等待、超额等待者的 `EAGAIN`、context cancellation 与释放后的恢复。response 用例同时覆盖 server 侧固定结果的 operation/waiter 占用、队列中的 `Write` 不读 request body、client 在解码或验证 mutation barrier 前不释放 reservation，以及 stream setup 的 error body 也必须经过同一 client admission。frame 用例分别覆盖 client/server 不同但相容的上限、超限 change 与 snapshot row、oversized pre-stream identity、tail 已前进却返回空 bounded page、subscription/frame 与 snapshots/frame 的 checked products、snapshot-page aggregate 与 waiter 饱和、取消释放，以及不可能成立的 option 组合；隔离用例占住 snapshot admission 后仍要观察 change frame 到达，证明 bulk transfer 的 gate 没有被放到 subscription 路径上。另一条集成用例让 snapshot producer 等待超过 client silence bound，断言 keepalive 保持 stream 可用，释放 admission 后 rows 继续到达。subscription 用例占满名额后断言新 stream 以 `EAGAIN` 拒绝，并在 client 关闭和 `Handler.Stop` 后恢复；另一条用例让 stream 从 opening tail 之后继续收到 live change，断言 `Position` 前进后 `CaughtUp` 仍为 true。mutation confirmation 用例断言 fixed-size admission 发生在 request 发出前，满 waiter queue 与取消都是 `EAGAIN` 且不会发送 mutation；另覆盖 event/response 两种先后顺序、并发 writer 造成的 later barrier、barrier incarnation/generation mismatch、grace、stream/request failure 与关闭时释放 active count。quota measurement 用例分别卡住一份目录的 `Entry` 加名字和 active/pending 完整路径组成的 frontier，验证精确边界、超限时不保留越界项、从不调用 ordinary `List`、取消，以及 recount 失败后保留原计数并释放 gate。SQLite reader 用例占满小型连接池，断言后续读取等待、取消后退出，并在释放一个 snapshot 后恢复 admission；integrity-record/name-byte 用例分别验证含 mandatory log row 的空 namespace 最小值、精确边界、跨 namespace label/parent/child 与 object/log/change 计费、超限 `EFBIG`、取消后 store 仍可用，以及 pre-open validation。偏重边界情况、错误路径、并发交错，以及回归的永久用例。**一个关于 errno 映射的测试若需要挂载点，说明有个边界划错了。**
- **对拍** —— 把 `fuse` 挂在本地目录实现上（不经网络），对挂载点与一个普通目录施加同一串操作，比较每一步观察到的结果、错误码，以及之后两棵树的路径、模式、大小与内容。不一致即缺陷 —— 不需要预先枚举「应该是什么样」，普通目录就是答案。适合随机化操作序列，因为它不需要预期值。刻意的偏差必须是 [`spec/requirements.md`](spec/requirements.md) 里明确列出的非目标，按名字跳过并注明是哪一条。
- **故障注入** —— 在被测那一层自己的下游接口上制造麻烦，下游有几个接口就注入几个。`storage` 接口上是一个专门制造麻烦的包装层：够不到的命名空间、提交失败、报不出类型的节点、以及在一次操作已经部分成功之后才失败。`objectstore.Objects` 的 `Get`、`Put`、`Delete`、`Available` 与 `Close` 都分别注入错误；能力拒绝只接受纯 `ENOSYS`，与其它错误 join 在一起时不能遮掉 measurement failure。组合层还要验证关闭顺序、两个 durable half 的错误同时保留。local-disk 实现在 `fsync`、`linkat`、`unlinkat` 与容量查询处注入错误，覆盖 shard identity/object publication 已发生但 durability 无法证明的状态；localstore 在 witness publication、WAL 缺失与 checkpoint 处注入，验证确认失败 poison、可重试 checkpoint 与锁保留。传输层覆盖拒绝不能产生有界结果的 backend，body 截断、request/response 单体与 admission 上限、等待队列溢出、bodyless 操作不保留意外 body、metastore 在 bounded page 中途失败或给出过大 payload、mutation 已提交后 `Log.Barrier` 失败、缺席/null/畸形 barrier、缺失答案、错误 framing，以及回答者根本不是本协议的情况。
- **端到端** —— 整条链路一起跑：一个服务端、两个各自挂载的客户端，验证一边写入另一边一秒内可见、以及服务端消失时每个操作都报错。两台机器在这里由两个挂载点代替，缺的只有主机之间的网络。交付出去的两个二进制也在这一层：旗标、诊断、退出码，以及 Ctrl-C 之后挂载点确实消失。

对拍必须跑在**交付出去的那套配置**上。为了让它好过而调松的任何一处 —— 内核超时、提交时机、单文件上限 —— 都会让它去验证一条生产中不存在的路径。

## 覆盖率是必要的，从来不是充分的

它只证明那些行跑过了，不证明特性按交付的样子工作。

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

构建出的 `remote-fs-server` 二进制覆盖普通目录的启动、挂载与停止，常见命令行拒绝，以及 local store 的跨进程重启持久性与独占锁竞争。需要证明锁跨进程生效时，第二个真实进程直接尝试打开同一根目录，不以首个进程的日志代替事实。

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

`packages/storage/objectstore/azblob` 对一个真的 Blob 端点跑，那个端点是 Azurite。它定义在 [`deployments/azurite.yml`](../deployments/azurite.yml)：本地 `make azurite` 起、`make azurite-down` 停；CI 的两个 job 各起同一份，第二个也要 —— 覆盖率闸门自己会把 `go test` 跑遍整个 module，而不是只跑那个 job 的那几个包。

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
