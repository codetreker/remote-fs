# Agent Note: 刚挂好的挂载点答 EIO

Status: proposed

## 问题

挂载层的用例会以大约每二十轮一次的比例变红，失败的形态是**一个刚挂好的挂载点上的第一批操作答 EIO**，而不是任何断言不成立。它与[挂载归属那次修复](../../implemented/testing/2026-08-22-a-mount-belongs-to-whoever-attached-it.md)无关：那一次修的是「把别人的挂载算成自己漏的」，这一次是挂载本身答错。两者在同一个 job 里，所以在这次之前它们混在一起看起来像同一个 flaky。

本机（Linux 6.8.0-137，16 核，go1.26.5）实测到的四次，路径不同、操作不同、断言位置不同：

| 用例 | 位置 | 报出来的 |
|---|---|---|
| `TestAnOverwriteOnOneMountpointIsSeenWhole` | `endtoend_test.go:201` | `reading through B after writing "first": read …/a.txt: input/output error` |
| `TestARenameOnOneMountpointIsSeenAsARename` | `endtoend_test.go:172` | `stat before.txt through B gave … input/output error, want ENOENT` |
| `TestADirectoryMadeOnOneMountpointIsADirectoryOnTheOther` | `endtoend_test.go:134` | `making d through A: mkdir …/d: input/output error` |
| `TestASymbolicLinkSurvivesTheWholeChain` | `endtoend_test.go:251` | `os.ReadDir: readdirent …: input/output error` |

共同点：**都发生在 `mountpointOn` 刚返回之后的头几个操作上**，都是 EIO，都不重现于同一条用例的单独重跑。

数出来的比例：`go test -count=1 ./cmd/...` 连跑 20 轮，1 轮红；同一条命令在这次修复之前的 commit 上连跑 20 轮，1 轮以这个形态红；`assert-every-test-ran.sh -race` 连跑 3 轮，1 轮红；更早一批 `-race` 下 30 轮，3 轮红。合计约 6 / 73。CI 的历史里 13 次运行中有 1 次是这个形态。**修复前后的比例分不出差别**，也就是说它既不是这次引入的，也不是这次修掉的。

EIO 在这个系统里是「够不到命名空间」的意思（R-ERR-1）。真正的坏处不是这几条用例红，而是**这条路径上的 EIO 说明有一段时间挂载点是挂着的、却答不了任何问题**。如果那是真的，它就不只是测试的问题。

### 现在诊断不下去的原因

`endtoend_test.go` 的挂载走 `mountpointOn`，它给 `fuse.Options{Logger: testLogger(t)}` 但 `Debug` 是关的，所以日志里没有内核问了什么、这一侧答了什么。服务端那一侧是同进程的 `http.Server`，它的失败只在 `t.Errorf` 里出现一次。

另一处更彻底：`cmd/quota_test.go` 里 `startMountBinary(...)` 的返回值被直接丢掉（例如 `:41`、`:113`、`:218`），而那个 `*process` 是唯一持有挂载二进制自己 stderr 的东西。这些用例红的时候，日志里没有任何一行来自那个进程。

## 提案

先让它可诊断，再谈修。

1. **留住挂载二进制的输出。** `quota_test.go` 里丢掉的 `*process` 收起来，失败时把 `p.output()` 一起打出来。这是纯增量，不改任何断言。
2. **失败时打开 FUSE trace。** 挂载层的用例在环境变量或 `-args` 开关下把 `fuse.Options.Debug` 打开，让复现跑能拿到一次完整的请求/应答序列。默认关着——常开会把日志淹掉。
3. **拿到一次带 trace 的复现之后再决定修哪里。** 眼下有三条互不排斥的猜测，都没有证据：`fuse.New` 返回时挂载尚未完全就绪，第一批请求打在半就绪的连接上；`httprest` 客户端与同进程服务端之间某个连接在挂载点刚建立时被复用/关闭；或者 EIO 是真的、来自服务端一侧某个短暂失败被 `errnoOf` 归并成了 EIO（`errnoOf` 对不带 errno 的错误一律答 EIO）。第三条最容易先排除，因为它只要求错误链里保留原因。

## 备选方案

**在这几条用例里重试。** 最省事，也最危险：R-CON-2 禁止让可见性取决于轮询，端到端判据里「B 上的读只做一次，永不重试」是被专门写下来的（见 [MVP 范围](../../implemented/process/2026-08-19-mvp-scope.md)）。加重试会把这个缺陷变成看不见的，而它可能是产品缺陷而不是测试缺陷。

**挂载后先做一次预热操作再开始计时/断言。** 同样把证据抹掉，而且 mvp-scope 已经写明预热会把第一次访问的成本挪出测量区间，而那个成本在真实使用里是有人付的。

**当作已知噪音，重跑 CI。** 6/73 的比例意味着挂载层的每一次 CI 都有约 8% 概率无故变红；照 AGENTS.md 的说法，把不稳定的检查当噪音就是在训练所有人忽略它。

**先修不先诊断。** 上面三条猜测里挑一条改掉，绿了就算完。输在无法分辨「修好了」和「概率被压到这批样本以下」——6/73 的现象需要几十轮才能证伪一次改动。

## 验收标准

- 一次带 FUSE trace 的复现被抓到，能说清 EIO 是从哪一层产生的。
- 原因写进 `docs/research/` 或本 note 的续写，然后才动代码。
- 改完之后 `go test -count=1 ./cmd/...` 连跑 100 轮零红；100 是从 6/73 这个比例推的——20 轮全绿说明不了什么。

## 风险

- **它可能不是测试的缺陷。** 如果挂载点确实有一段时间答不了问题，那么在真实使用里那一段时间里的每一个操作都会拿到 EIO，而 R-ERR-1 的意思是这句话不能是假的。真是这样的话，这份 note 要升级成一个产品缺陷。
- 打开 trace 会显著拖慢挂载层，且 trace 本身可能改变时序、让现象消失。这时要退回到「留住输出」那一半，并接受用更多轮次换一次复现。
- 上面还观察到一次形态不同的红（`TestAnAllowanceIsSpentAgainstWhatTheWorkspaceAlreadyHolds`：一次 36864 字节的 `write(2)` 全部写入并返回 `<nil>`，本该是 EDQUOT，配额在随后的 `close` 上才报出来），只见过一次，没有并入上表。它可能同源，也可能是配额记账自己的问题。
