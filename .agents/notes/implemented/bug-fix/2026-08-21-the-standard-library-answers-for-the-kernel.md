# Agent Note: 标准库替内核作答

Status: implemented

## 问题

这份记录中的 `localdir` 已由[移除宿主目录后端](../simplification/2026-09-08-remove-the-host-directory-backend.md)取消。下面保留当时的 syscall 选择与测量依据；仍适用的是 storage 的改名契约、操作数重合用例，以及不能让便利包装编造 errno 的规则。

当时 `localdir.Rename` 调的是 `os.Rename`，而这个包装在 Unix 上不是 `rename(2)` 的同义词。它先 `Lstat` 目标，发现那里是一个目录，就自己造一个 `EEXIST` 返回，syscall 根本没有发出去。**「两个操作数指向同一个已存在节点」正落在这个分支里**：`newname == oldname` 是它两个直接返回 `EEXIST` 的条件之一（[`os/file_unix.go`](https://github.com/golang/go/blob/c19862e5f8415b4f24b189d065ed739517c548ba/src/os/file_unix.go#L26-L52)，go1.26.5）。

POSIX 对这种改名的要求是成功并且什么都不做 —— 两个名字「resolve to either the same existing directory entry or different directory entries for the same existing file, rename() shall return successfully and perform no other action」（[rename](https://pubs.opengroup.org/onlinepubs/9799919799/functions/rename.html)）。于是这份实现**拒绝了一次内核会执行的移动**，而 R-FS-2 要的恰恰是为本地目录写的程序在挂载点上照常工作。

同一个分支还吞掉另外三个答案。四个都实测过（Linux 6.8、go1.26.5）：

| 情形 | `rename(2)` | `os.Rename` |
|---|---|---|
| 目录改名到它自己身上 | 成功，无副作用 | `EEXIST` |
| 目录改名到一个空目录上 | 成功 | `EEXIST` |
| 目标目录里还有东西 | `ENOTEMPTY` | `EEXIST` |
| 文件改名到一个目录上 | `EISDIR` | `EEXIST` |

四个答案被压成同一个 `EEXIST`：两次本该成功的移动被拒绝，两个各自指名了障碍的 errno 被换成一个只说得出「那里有东西」的 errno。

## 决定

当时的 `localdir.Rename` 改为直接调 `syscall.Rename`，失败时自己把 `*os.LinkError` 组回来。这一后端的实现与专用测试随包一起移除；下述取舍解释该修复为何成立，不要求现有 metastore 后端走 syscall。

组装由后端做还有第二个理由：`os.Rename` 组出来的那个 `*os.LinkError` 装的是宿主路径。那是服务端目录的布局，而这个错误要走到另一台机器上的调用方那里去。该修复把 `Old` 与 `New` 设为卷的路径。

### 这个包里的第二次同类改动

`Remove` 早就因为同一类理由不走 `os.Remove` 而走 `syscall.Unlink`：`os.Remove` 在 `unlink` 失败之后回退去试 `rmdir`，于是它会删掉一个契约要求以 `EISDIR` 拒绝的目录。

两处的共同点值得单独写下来：**标准库的便利包装有时不问内核就作答**。`os.Rename` 凭一次 `Lstat` 造一个 errno，`os.Remove` 自作主张发第二个 syscall —— 都是在把「内核会怎么答」换成「这个包装认为应该怎么答」。对一个把「不报告没有测量过的事」当作全部目的的系统来说（R-ERR-2），这样的包装是危险而不是便利：它给出的每个答案都得先确认是不是内核给的。凡是要把 errno 原样交出去的地方，这个包一律直接走 syscall。

### 缺陷是被一条操作数重合的契约用例照出来的

在这次之前，契约用例里有七条 rename 用例，**每一条的两个操作数都是互不重合的名字**。七条全绿，而上表里四个答案没有一条被碰到。缺陷一直在那儿，直到一条把节点改名到它自己身上的用例出现。

这是这次留下的持久教训：**一组操作数各不相同的用例，说不出实现在操作数重合时会做什么** —— 而重合恰好是这类包装替内核作答的地方。

契约现在有两条：`rename onto itself succeeds and changes nothing`，以及 `rename a missing node onto itself is ENOENT`。后者存在是因为前者可以被作弊 —— 一个从操作数就看得出「两个名字指向同一处」的实现，可以不碰文件系统直接报成功，而那与「那里根本没有节点」区分不开。

当时契约管不到的部分由 `localdir` 自己的用例守着。契约要在任意文件系统上成立，所以它对「目标目录里还有东西」接受 `ENOTEMPTY` 与 `EEXIST` 中的任意一个，对另外两个答案一个字都没说；已移除的 `TestRenameTakesAnEmptyDirectorysPlace` 与 `TestRenameReportsTheKernelsErrnoForADestinationItCannotReplace` 以 Linux 为宿主，直接钉住内核的那个答案。操作数重合的两条通用契约用例继续约束现有后端。

## 备选方案

**留着 `os.Rename`，在 `localdir` 里先判断两个操作数是否重合，重合就直接返回成功。** 改动只有几行，把报上来的那个缺陷挡在包装之前。输在它只修了四个答案里的一个：目标是一个空目录时，标准库照样在 syscall 之前造出 `EEXIST`，而那是 R-FS-2 明确要覆盖的形态 —— 一个构建工具正是用改名把暂存树换到它要替代的那棵树上。修掉一个，留下三个同源的兄弟。而且「重合就成功」这个判断本身就是又一次替内核作答：节点不在那里时 POSIX 要的是 `ENOENT`，直接返回成功就把它吞了 —— 这恰好是 `rename a missing node onto itself is ENOENT` 那条用例存在的理由。

**用 `golang.org/x/sys/unix` 的 `Renameat` 而不是 `syscall.Rename`。** 这是一个真选择：这个文件本来就依赖 `golang.org/x/sys/unix`（`UtimesNanoAt`、`Statfs`），引入成本是零，而顺着这条路还能够到 `renameat2` 的 `RENAME_NOREPLACE` 与 `RENAME_EXCHANGE`。取 `syscall.Rename`，是因为这次要的恰好就是 `rename(2)` 的原语义，`Renameat` 会多出一个在这里永远是 `AT_FDCWD` 的目录描述符参数，一个不表达任何东西的常量。这个包本来就按这条线分工：`syscall` 里有且语义正好的（`Unlink`、`Rmdir`、`Rename`）走 `syscall`，`syscall` 里没有的（`utimensat`、`statfs`）走 `unix`。契约将来若要一个「不覆盖已有目标」的改名，`unix.Renameat2` 是那时候的入口，与这次的选择不冲突。

以上是全部 —— 没有第三个被真正比较过的方案。

## 后果

**当时买到的：**

- 四个答案回到内核手里：同名改名成功且什么都不做，目录改名到空目录上成功，非空目标 `ENOTEMPTY`，文件改名到目录上 `EISDIR`。为本地目录写的程序在这四种情形下拿到的都是它在本地目录里会拿到的那个答案（R-FS-2）。
- 走到调用方那里的 `*os.LinkError` 装的是卷路径，服务端目录的布局不再随错误泄出去。

**当时付出的：**

- **目标目录里还有东西时，答的从标准库的 `EEXIST` 变成了内核的 `ENOTEMPTY`。** 两个都在契约的错误词汇表里，契约用例对这一条本来就接受两者中的任意一个（它要在任意文件系统上成立），所以这上面的每一层都不用动 —— 变的只是这份实现交出的是哪一个。
- **`syscall.Rename` 不做 `os.Rename` 那层 `EINTR` 重试。** 本地文件系统上 `rename(2)` 不会被信号打断，但被服务的目录本身落在一个 FUSE 或网络文件系统上时可以。`Remove` 走 `syscall.Unlink` 时已经付过同一笔，两处一致；真要还这笔账，得在这个包里统一做，而不是靠某几个操作恰好用了会重试的那个包装。
