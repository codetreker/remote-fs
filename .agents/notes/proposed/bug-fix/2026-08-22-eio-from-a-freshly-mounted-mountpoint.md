# Agent Note: 一次被中断的请求答成了 EIO

Status: proposed

## 问题

挂载层的用例会以大约每二十轮一次的比例变红，失败的形态是**挂载点上的一个操作答 EIO**，而不是任何断言不成立。本机（Linux 6.8.0-137，16 核，go1.26.5）实测到的四种，路径不同、操作不同、断言位置不同：

| 用例 | 位置 | 报出来的 |
|---|---|---|
| `TestAnOverwriteOnOneMountpointIsSeenWhole` | `endtoend_test.go:201` | `read …/a.txt: input/output error` |
| `TestARenameOnOneMountpointIsSeenAsARename` | `endtoend_test.go:172` | `stat before.txt gave … input/output error, want ENOENT` |
| `TestADirectoryMadeOnOneMountpointIsADirectoryOnTheOther` | `endtoend_test.go:134` | `mkdir …/d: input/output error` |
| `TestAnAllowanceIsSpentAgainstWhatTheWorkspaceAlreadyHolds` | `quota_test.go` | `open …/over.bin: input/output error` |

合计约 6 / 73；CI 历史 13 次运行里 1 次。

### 根因

内核在调用线程收到信号时发 `FUSE_INTERRUPT`；go-fuse 据此关掉那次请求的 cancel channel；这一侧的 storage 调用于是死在 `context canceled` 上；而 `errnoOf` 对任何不带 errno 的错误一律答 **EIO**。在 `errnoOf` 里临时打一行日志抓到了原文：

```
stat "over.bin": Get "http://…/<protocol>/stat?path=over.bin": context canceled: input/output error
```

这里把当时 URL 里的版本段省略为 `<protocol>`；根因在 FUSE cancellation 与 errno 分类，不依赖 wire protocol 版本。

决定性的对照是驱动方：同一批二进制，用 Python 在重载下驱动 **52/52 全清**；换成 `go test` 驱动约 **5%** 复现。差别是 Go 的**异步抢占** —— 运行时用 SIGURG 打断长时间不进入安全点的线程，而那个线程正阻塞在 `open` 里。这解释了为什么它只在这套测试里出现，以及为什么它换一个用例就换一个失败点：被打中的是哪个操作，纯粹看信号落在谁身上。

它**与元数据复制无关**：复现它的那个用例的服务端是 `-dir`，挂载时一份副本都没有。

### 为什么这不只是测试的毛病

EIO 的意思是「够不到命名空间」（R-ERR-1），而事实是「这次请求被撤回了」。一个会重试 `EINTR` 的程序本来能自己恢复，拿到 EIO 就只能失败 —— 而**任何信号密集的程序都会碰上它**，Go 写的程序尤其，因为异步抢占是它的常态而不是异常。R-FS-2 要的是「未经修改的、为本地目录编写的程序能在挂载点上正常工作」，这一条正落在它上面。

测试只是第一个碰到的用户。

## 提案

**先分开「被撤回」与「够不到」。** `errnoOf` 那个「不认识的错误一律 EIO」的兜底要先分出 `context.Canceled` 与 `context.DeadlineExceeded` 两支：前者是调用方撤回了请求，后者才是够不到。这一步无论下面怎么选都要做，而且它自己就能把这批红变成一个正确的 errno。

**再决定对一次被中断的请求回什么。** 两条路，都还没选：

- 回 `EINTR`（或 `ECANCELED`），让内核照常把它交给调用方；
- 干脆不回答 —— FUSE 允许对一个已被 `FUSE_INTERRUPT` 的请求不作答。

选哪条取决于 go-fuse 在 `FUSE_INTERRUPT` 之后还接不接受对原请求的回复。**这需要读它的源码确认，不是讨论能定的。**

## 备选方案

**在这几条用例里重试。** 最省事。输在它把一个产品缺陷伪装成测试抖动 —— 而且 R-CON-2 与端到端判据里「B 上的读只做一次，永不重试」是被专门写下来的（见 [MVP 范围](../../implemented/process/2026-08-19-mvp-scope.md)）。

**挂载后先做一次预热操作。** 同样抹掉证据，而且 mvp-scope 已经写明预热会把第一次访问的成本挪出测量区间，而那个成本在真实使用里是有人付的。

**当作已知噪音，重跑 CI。** 照 AGENTS.md 的说法，把不稳定的检查当噪音就是在训练所有人忽略它。而现在根因已知，这条连「暂时容忍」的理由都没有了。

**先修不先诊断。** 这一条已经**不再是备选** —— 诊断做完了。留在这里是因为当时它看起来很有道理：挑一条猜测改掉、绿了就算完。而 6/73 的现象需要几十轮才能证伪一次改动，那条路会以「大概修好了」收场。

## 验收标准

- `errnoOf` 对 `context.Canceled` 与 `context.DeadlineExceeded` 各有一条用例，且两者答不同的 errno。
- 一次被中断的请求在挂载点上表现为可重试的错误，而不是 EIO —— 用一个真的对自己发信号的调用方驱动，而不是靠注入。
- 改完之后 `go test -count=1 ./cmd/` 连跑 100 轮零红。100 是从 6/73 推的；20 轮全绿说明不了什么。

## 风险

- **不回答一个被中断的请求，若 go-fuse 不接受，会挂住那个调用方。** 这是两条路里更干净的一条，也是更容易做错的一条；确认它之前不要选它。
- **改了 `errnoOf` 的兜底，会影响每一条错误路径。** 那个函数今天是所有「说不出 errno 的错误」的汇合处，动它等于动整层的失败语义，因此每一支都要有自己的用例。
- **异步抢占是 Go 运行时的行为，将来只会更频繁。** 这个缺陷不会因为不管它而消退。
