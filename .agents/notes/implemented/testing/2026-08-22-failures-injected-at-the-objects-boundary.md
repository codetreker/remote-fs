# Agent Note: 对象存储的失败在接口上注入，不在网络上制造

Status: implemented

## 问题

R-ERR-6 说，一份由多个能各自失败的部分拼成的 storage 实现，任何一部分不可达都按 R-ERR-1 失败。
[放进对象存储的卷](../architecture/2026-08-21-volume-in-an-object-store.md)正是这样一份实现：
一棵树加一个对象存储，两者各自会失败。这条保证要求的是**不管对象存储那一半报什么，卷都不得
把它答成一个关于名字的事实**。

「不管什么」是这条保证的全部内容，而它此前的证法送不进几种错。证法是把一个真实的 blob 客户端指向
一个没人监听的地址，再读一个文件——一个拒绝连接的地址产生的是 EIO，凭证被拒（EACCES）、请求超时
（不带任何 errno）、以及一份实现报出 errno 词汇表里根本没有的错误，这三类它一个都产生不了。

它证的大半也不是这一层的事。「真实客户端遇到 connection refused 不报 ENOENT」由 azblob 自己的用例
证，且证得更全——`Get`、`Put`、`Delete` 三个方法各一次。组合层再证一遍，净增量只有「中间那一层没有
改写它」这一句。

与此同时，组合层上 `Put` 失败与 `Delete` 失败这两条路径没有任何用例。它们与 `Get` 失败是同一条保证
的三个面：一次没落地的写不得留下名字或计费，一次删不掉的清扫不得把记录忘掉——记录一旦忘掉，那些字节
就再没有任何东西会把它们交出来回收，而它们一直在计费。

最后，那个证法要付出代价：SDK 默认重试三次、指数退避，一次打向死地址的调用睡 7 到 11 秒。两个包的
测试因此要跑 13.5 秒和 10.0 秒，其中绝大部分是纯睡眠。

## 决定

组合层的失败**注入在 `objectstore.Objects` 上**，不由网络制造。`packages/storage/objectstore/failure_test.go`
里的包装层让 `Get`、`Put`、`Delete` 中的一个按要求失败，其余的照常打到真的对象存储上：被读的对象是
一次真的 `Put` 写进去的，树是真的 SQLite，只有被点名的那一次调用换了答案。

送进去的是四种错，一种代表一类，任何一种都不得变成关于名字的答案：

| 注入 | 它代表 |
|---|---|
| `EIO` | 服务什么都没答——azblob 对一个不响应的端点给的就是它 |
| `EACCES` | 服务听懂了并且拒绝。它不是 EIO，所以一个写成「是不是那个失败 errno」而不是「是不是关于名字的答案」的判断会放它过去 |
| `context.DeadlineExceeded` | 请求活过了它的期限，不带任何 errno |
| 一个普通的 `errors.New` | 一份实现报出词汇表里没有的东西，这是一个按已知 errno 分支的 switch 会漏掉的情形 |

三个方法各自被断言的东西不同：

- **`Get` 失败** —— 读报出它遇到的失败本身（`errors.Is` 找得到注入的那个错），不是 ENOENT，也不是任何
  别的读起来像「这个名字怎么样了」的 errno；并且树这一半仍然回答，文件还在，长度还在，失败没有被写回树里。
- **`Put` 失败** —— 写失败，名字不存在，用量是 0；reservation 进入 unresolved，继续占 pending admission，
  且永远不授权清扫同 key 的对象。只有 `Put` 已成功、随后 `Commit` 失败时才 `Abandon`，因为这条路径已经
  证明对象由本次 reservation 创建。两种失败都保留原始对象错误与 metastore cleanup error。
- **`Delete` 失败** —— 清扫报出失败且报告删掉了 0 个；对象存储恢复之后，同一批积压还在原地等着被清掉。
  一次失败的删除**不忘记任何记录**。另有一例证明反向：一次成功的改动不会因为它触发的那次清扫删不掉
  旧对象而失败——名字指向该指的地方，唯一的代价是还没回收的字节，而记录仍在，下一次清扫拿得到它。

azblob 那一侧继续对着死地址跑真实客户端，那是它自己该证的事。变的只是那几个调用用
`policy.WithRetryOptions(ctx, policy.RetryOptions{MaxRetries: -1})` 把 SDK 的重试关掉：用例断言的是
这个失败被归成哪一类，而第四次尝试的错误和第一次归类完全一样。

规则本身写进了 [`docs/testing.md`](../../../../docs/testing.md)：「失败要注入，不要制造」与「不为一个用例
不断言的东西付出等待」。

## 哪一层证明什么

| 层 | 它证的 |
|---|---|
| `packages/storage/objectstore/azblob` | 一份真实的 blob 客户端怎么给失败分类：没人监听的地址、不存在的 container、被撤回的请求，`Get`、`Put`、`Delete` 各一次；`Available` 另探测正常、不可达、container 缺失与凭据拒绝；映射表本身只有 `BlobNotFound` 通向 ENOENT |
| `packages/storage/objectstore/objectstoretest` | 每份 `Objects` 实现共同履行 create-only、不可变、错误区分、容量、取消与并发契约，并能在用例 cleanup 时成功关闭；memory、azblob 与 localdisk 都运行同一套用例，关闭的并发、幂等与排空由各实现及组合层另测 |
| `packages/storage/objectstore` | `Get`、`Put`、`Delete` 的任意失败不被改写成名字事实；另以独立包装层覆盖物理容量测量、部分删除、`Forget` 与关闭失败 |
| `packages/storage/objectstore/localdisk` | 在真实文件系统操作 seam 注入 `fsync`、link、unlink、`statfs`、`statx` 与 device/mount mismatch，覆盖 FORMAT/lock/probe、objects/shard identity stage/link/barrier、recovery/staging/final object；并发用例分别锁住同一 shard 与另一 shard，证明[本地磁盘对象存储](../architecture/2026-09-04-local-disk-object-store.md)在持久性、store 归属或同一文件系统身份无法证明时停止作答，同时不把全部对象 I/O 串行化 |
| `packages/storage/localstore` | 在真实 SQLite 与本地文件系统上构造初始化中断、丢失卷、`METASTORE` stage/final 损坏、accepted/checkpointed generation 与 WAL 缺失组合、checkpoint pin/error，以及 active reader 下的关闭；只替换见证、单次 barrier 或 pool close reporting 时，证明确认失败 poison、checkpoint/取消可重试、pool close error 进入 terminal 状态、SQLite constructor cleanup 不确定时内部 coordinator 与外部 lifetime lock 都保留 |
| 契约套件 | 这两半装在一起，行为和别的卷一样。它跑在真的 Azurite 和真的 SQLite 上，不用替身——这一层声称的正是两者合起来对不对 |

## 备选方案

**契约套件也改用一份假的 `Objects`。** 会把这个包的测试从三秒变成毫秒级。输在它把契约套件声称的东西
换掉了：那套用例说的是「一个装在真 blob 容器加真 SQLite 里的卷，行为和别的卷一样」，
换成假的之后它说的变成「这个包的记账自洽」，那是一个更弱的命题，而且是别处已经覆盖的命题。

**在 `azblob.New*` 上暴露重试配置，用例传一个不重试的。** 部署方将来大概真的需要这个旋钮——一个挂载
点上每次读卡十秒是一个部署要能调的事。输在现在没有任何部署方要求它，而为了让测试快而给生产 API 加
参数是把测试的需要写进交付面。SDK 自己提供了按调用覆盖的 `policy.WithRetryOptions`，够用且不动生产
代码。真有部署需求时再加，那时它是一个由需求驱动的选项，不是一个由测试驱动的选项。

**保留原来那个死地址用例，只把它的重试关掉。** 改动最小，也确实把 8 秒变成 0。输在它只解决了速度：
那个用例仍然只送得进 EIO 一种错，`Put` 与 `Delete` 两条路径仍然没有任何用例，而慢只是这三件事里最不
重要的一件。

**用一个 `httptest` 服务器冒充 blob 服务，让它按用例的要求回状态码和错误码。** 比死地址能表达的多，
而且是真实的 HTTP 往返。输在它测的是 azblob 的错误码映射表，而那张表已经被 azblob 自己的用例直接对
着断言了；并且它照样产生不了「没有 errno 的错误」和「词汇表以外的错误」，因为那两类根本不是服务能
答出来的东西。

## 后果

买到的：

- **三个方法乘四种错，全部在组合层上被断言。** 当时按 `-coverpkg` 全模块口径量，`discard` 从 72.7% 到
  90.9%，`Sweep` 从 73.3% 到 80.0%，`Write` 从 87.5% 到 93.8%。这些是历史测量，不能作为现行[包内覆盖率](../../../../docs/testing.md#覆盖率是必要的从来不是充分的)的结果。
- **两条此前没有任何用例的保证第一次被写下来**：一次没落地的写不留名字也不计费；一次删不掉的清扫不
  忘记录，因而下一次还找得到。两条都是先把实现改坏、看着用例变红、再装回去验证过的。
- **这两个包的测试从 13.5 秒与 10.0 秒变成 3.0 秒与 0.06 秒**，都还对着同一个 Azurite 跑。

付出的：

- **组合层不再有任何用例走真实的网络失败。** 它现在依赖 azblob 那一侧证明真实失败被归成非 ENOENT，
  以及契约套件证明这一层拿到的确实是 `*azblob.Objects`。两侧都在，但它们是两个用例而不是一个。
- **注入用的包装层仍嵌入 `objectstore.Objects`。** 接口增加 `Available` 与 `Close` 时，前者被静默继承，
  后者必须显式改成 no-op，才能让借用夹具的卷只关闭自己的维护 worker。容量失败由
  `composition_test.go` 的专用包装层覆盖；以后再加方法，嵌入仍不会用编译失败提醒需要新的故障用例。
- **注入用的卷与夹具共享 metastore 和 objects。** 两个借用包装层的 `Close` 都是 no-op，测试
  cleanup 因而可以关闭卷、排空后台维护，却由原夹具继续拥有持久资源。这个 ownership 由类型约定
  与注释维持，编译器不证明底层只关闭一次。
- **`metastore.Store` 仍没有覆盖全部方法的任意错误注入。** 组合用例已经直接送入 `Space` failure 与
  `Forget` failure，SQLite 自己的用例和卷契约覆盖其余路径；还没有一个与 `failingObjects`
  对称、能让每个 metastore 方法分别返回任意错误的包装层。
- **部分删除与记录失败已经各自进入同一个用例。** 第二个 `Delete` 失败且 `Forget` 也失败时，`Sweep`
  同时返回两项失败、报告只删除一个对象，维护状态保留同一组事实。`forgetFails` 没有记录传入的 key，
  所以该用例仍不能区分 `Forget(ctx, gone)` 与错误的 `Forget(ctx, keys)`；成功删除的那一个是否是唯一被
  交给 `Forget` 的 key 仍缺直接断言。一次 backend 调用内部发生「删除已落地但返回结果未知」的外部服务
  语义也不在这些注入里。
