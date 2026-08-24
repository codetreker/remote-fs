# Agent Note: 挂载属于挂它的那一次运行

Status: implemented

## 问题

一次 CI 运行里每一个用例都通过了，唯一的失败行是 `FAIL github.com/codetreker/remote-fs/cmd`，它前面跟着：

```
left behind: a mount: remote-fs /tmp/TestSpaceIsReportedInWholeBlocksa_reserve_that_only_the_superus1986454464/002 fuse.remote-fs …
left behind: a FUSE connection: /sys/fs/fuse/connections/48
```

那个挂载点的名字来自 `TestSpaceIsReportedInWholeBlocks/a reserve that only the superuser may spend`，它是 `packages/fuse` 的用例，不是 `cmd` 的。`go test` 一个包起一个测试二进制并行地跑，而 CI 那条命令一次给了它两个包：`cmd` 的用例 2.17 秒跑完、开始做泄漏检查的时候，`packages/fuse` 还有 1.07 秒要跑，手上正挂着一个挂载点。

检查读的是 `/proc/self/mounts`，凡是 `remote-fs ` 开头的行一律算作本次运行漏下的 —— **没有任何归属判断，连一次运行前后的相减都没有**。FUSE 连接那一半减掉了运行前的快照，但另一个进程在运行期间新开的连接相对那份快照照样是新增。

同一个文件里 `fusermountChildren` 已经把正确的原则写下来了：「Only our own children are considered: fusermount is a setuid helper that anything on the machine may be running, and a stranger's is not evidence about this run.」挂载是同一种共享的东西，前两个检查违反的正是它自己写的这句话。

这也不是只在 CI 上发作的事：谁在自己机器上真的挂着一个 remote-fs 在用，谁跑这套测试就必红。

**一个拿不出证据的红，和一个什么都没证明的绿，代价是一样的。** 下一个人看到「每个用例都过了、包却 FAIL」，学到的是把这一段划过去。

## 决定

新增 `packages/fuse/fusetest`。两个会挂载的包的 `TestMain` 都走它：

```go
func TestMain(m *testing.M) {
	os.Exit(fusetest.Run("remote-fs-cmd", func() int {
		code := m.Run()
		removeBuiltBinaries()
		return code
	}))
}
```

归属做成**结构性的，而不是登记式的**。`Run` 在跑用例之前把 `TMPDIR` 指向一个新建的、名字没有第二个进程知道得到的目录；`t.TempDir` 在它下面建目录，子进程继承它，于是**用例能拿到挂载点的每一条路径都在它下面**，而在它下面挂着的东西一定是这一次运行挂上去的。跑完只报告那个目录下面还挂着的东西，外加这个进程自己没收走的 `fusermount` 子进程。

不选「每个挂载点登记一次」，是因为登记式会静默失效。`cmd/binaries_test.go` 的拒绝用例表里今天就有五个 `-mountpoint` 参数是就地写出来的，不经过任何 helper；明天写的用例也不会记得登记。而漏登记的表现是「检查安静地不再覆盖这条用例」—— 没有人会发现。结构性的那条路不需要调用点配合，因此也没有可以忘记的地方。

### 读的是 mountinfo，不是 mounts

`/proc/self/mounts` 是 fstab 格式，里面没有设备号，所以它的一行没办法跟一个 FUSE 连接对上。`/proc/self/mountinfo` 的第三个字段是 `major:minor`；FUSE 挂在匿名块设备上，major 是 0，而那个 minor 就是 `/sys/fs/fuse/connections/` 下面那个目录的名字（`fc->dev = sb->s_dev`，`fuse_ctl_add_conn` 把它整个打印出来）。所以泄漏报告里能直接给出连接目录，而不需要再去和一份全机器的连接快照相减 —— 那份快照正是原来那半个假阳性的来源。

fuseblk 挂的是真的块设备，major 不是 0，那个号也就不是连接号；所以是**类型和 major 一起**决定要不要报连接，而不是只看类型。

### 读不懂的一行是错误，不是跳过

解析不了的一行不会被跳过。跳过它就是把「表里有一行我读不懂」变成「什么都没挂」，而这正是一个泄漏检查最不该编造的那个答案。同理，一张空的挂载表也是错误：机器上永远挂着东西，读到空的说明读的不是那张表。

### 路径按内核记录的形态比

根目录在建好之后先 `EvalSymlinks`，因为内核记的是解析过的路径。顺带修掉了同一类的第二处：`cmd/binaries_test.go` 的 `stillMounted` 拿未解析的路径去跟挂载表比，在 `TMPDIR` 是符号链接的机器上永远比不中 —— 而它是 R-ERR-5 唯一的一条直接断言，一条永远不可能失败的断言什么都不证明。它现在走 `fusetest.Mounted`，和泄漏检查共用同一个解析器。

### `packages/fuse` 这次一起接上

它挂载得比 `cmd` 多得多，而在这次之前**一个泄漏检查都没有**：本机上唯一能发现它漏挂载的东西，恰恰是 `cmd` 那个扫全机器的检查 —— 这次 CI 失败本身就是它在跨包起作用的证据。只修 `cmd`、把 `packages/fuse` 留到以后，等于在这两次改动之间把这个能力删掉。另外 `packages/fuse` 里有三处挂载（`fuse_test.go` 的 `TestNothingIsWrittenToTheProcessOutput`、`TestDiagnosticsGoOnlyWhereTheCallerAsked`、`TestMountingSomewhereItCannotBeDone`）根本没有注册 `t.Cleanup`，是这个仓库里 sweep 唯一能兜住的形态。

### 检查见过自己变红

`cmd` 与 `packages/fuse` 各自把一处 `t.Cleanup` 改成不卸载，跑一条挂载用例，看着 `TestMain` 报出 `left behind: a mount: …`、退 1，再改回去。`fusetest` 自己的用例里也有一条对应的：`TestRunFailsAndKeepsTheEvidenceWhenSomethingIsLeftBehind` 让 `Run` 面对一张说根目录下面还挂着东西的表，断言它退 1 并且**不删根目录** —— 删根目录会走进那个还挂着的挂载点，把服务的内容删掉。

## 备选方案

**给 mounts 那一半也补一次运行前后的相减。** 三行，与旁边的连接检查对称。输在它修不掉这次的失败：出事的那个挂载是在本次运行**期间**挂上的，相对任何「运行前」快照都是新的。实测过：一个在 `go test` 起来约 0.7 秒后挂上的 mount 复现出 CI 那两行。它买到的只是「机器上本来就挂着的那个不再被算进来」，代价是让这个检查看起来已经修好了。

**用 `-p 1` 把测试二进制串起来。** 实测有效：把 `-run` 收窄到一条 `packages/fuse` 用例、让 `cmd` 一个用例都不跑，默认 5 次全失败，加 `-p 1` 后 5 次全过。输在它把并发当成了缺陷，而缺陷是归属。两个 checkout 同时在跑、或者这台机器上真有一个 remote-fs 挂着的时候，`-p 1` 一句话也说不上；代价是这一层的测试不再重叠。

**删掉进程内的检查，只留 `ci.yml` 里那一步。** 那一步在所有 `go test` 进程退出之后、在一台跑完就扔的 runner 上扫全机器，它的全机器推理在那个位置是成立的 —— 这次那一步报的就是 success。输在它只回答了 CI 的那半个问题：要防的那个状态是「一个挂载点卡在那儿，要人手去拆」，那发生在开发者自己的机器上，而 `make test` 之后没有任何东西会去看一眼。删掉一个保证，将来要的是「重做」而不是「补上」。

**挂载的时候记下 FUSE 连接号，事后查它还在不在。** 这是 bazil/fuse 和 juicefs 用的形状，而且它能抓到「挂载点已经拆了、连接还开着」的懒卸载。输在两处：minor 是从一个全机器共享的 IDA 里回收再发的（实测两次连续运行都拿到 130），一个被回收给陌生人的号会报出一个不是我们的泄漏 —— 同一类假阳性，而且更难看出来；要把它变可靠就得配上挂载点路径去核对，那就回到路径这条路本身。此外它要求 `packages/fuse` 为了测试导出一个新的公开 API，而 `cmd` 的挂载发生在子进程里，这个 API 对那一半根本够不着。

## 后果

**买到的：**

- 这个检查从此只对本次运行的挂载说话。另一个包的测试二进制、另一个 checkout、或者机器主人自己挂的 remote-fs，都不再被算成本次运行的泄漏。
- `packages/fuse` 第一次有了泄漏检查，覆盖到它那三处没有 `t.Cleanup` 的挂载。
- 泄漏报告从「一整行 mount table」变成挂载点、文件系统类型、连接目录，加一条能直接粘的 `fusermount3 -u`。
- `assert-every-test-ran.sh` 用 `go test -list` 把每个 `TestMain` 再跑一遍，那一遍原来也是这个假阳性的载体（`m.Run` 在 list 模式下正常返回）。现在那一遍的根目录下面什么都没挂，它没有人可以指控。
- `stillMounted` 那条永远不可能失败的断言被换掉了。

**付出的：**

- **`TMPDIR` 被整个测试二进制改掉。** 这是一个进程级的副作用，也是这套归属成立的前提。`packages/fuse` 的 `TestNoProcessWideStateIsTouched` 禁的是**这个包**碰进程状态，它解析源码时排除 `_test.go`，所以 harness 这么做不与它冲突；但读的人会觉得逆着纹理。代价是这一轮的临时文件——包括 `go build` 的中间产物——都落在更深的一层路径下。
- **panic 和 `-timeout` 之后什么都不报。** 这两种情形下 `TestMain` 拿不回控制权，而它们恰恰是最容易留下挂载的。这一点在这次之前也一样，区别是现在它被写下来了；CI 那一遍全机器扫描是覆盖它的东西，开发者的机器上没有对应的。
- **真漏了的时候，证据在 `TestMain` 看到之前已经被削掉一部分。** `t.TempDir` 自己注册的清理会对挂载点做 `RemoveAll`：`rmdir` 失败之后它会打开目录、穿过那个还挂着的挂载点把里面的东西逐个删掉，也就是把被服务的命名空间删空，然后才报错。用例因此在 `TestMain` 之前就已经红了，但报告读起来是两件事而不是一件。跟进项，见下。
- **`packages/fuse` 那三处挂载仍然没有 `t.Cleanup`**，与 `docs/testing.md` 的「每一个挂载都在 `t.Cleanup` 里拆掉」不符。现在有 sweep 兜着，所以它们漏了会红；把它们改成走 `t.Cleanup` 是另一次改动。

**这次没有买到的：** 它一点也没有让挂载更不容易泄漏 —— 它只让「报出来的泄漏是真的」。这一层另一个红（刚挂好的挂载点上一次操作答 EIO，本机 `-race` 下约 10%，CI 历史 13 次里 1 次）与这次无关，也没有被这次碰到。

**跟进项：**

- 让挂载点不再由 `t.TempDir()` 交出，从而不再被 `RemoveAll` 穿过去；这是上面那条证据被削的根。
- `packages/fuse` 那三处挂载改成走 `t.Cleanup`。
- `Makefile` 的 `test` 目标没有 `-count=1`：测试缓存不认 `/dev/fuse` 的有无，一个在有 FUSE 的机器上记下的绿会在没有 FUSE 的机器上原样重放。
- 那个 EIO 的红：`cmd/quota_test.go` 丢掉了持有挂载二进制 stderr 的 `*process`，在把它留住之前这个失败无从诊断。
