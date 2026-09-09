# Agent Note: 让 CI 编译缓存随源码推进

Status: implemented

## 问题

依赖没有变化时，两个 CI 作业命中同一个不可变的 setup-go 缓存键，新增的编译产物无法写回。这个键没有源码或作业身份：[9 月 1 日的 mounted 作业](https://github.com/codetreker/remote-fs/actions/runs/33485884640)先保存了 Go 1.26.7 的快照，checks 作业未取得同键的保存名额；[25ce631 的运行](https://github.com/codetreker/remote-fs/actions/runs/34336894449)仍精确命中该快照并跳过保存。期间 Go 文件从 79 个变为 320 个，其中 247 个当前路径在旧源码中不存在。精确命中时不保存是[固定版本 action 的行为](https://github.com/actions/setup-go/blob/b7ad1dad31e06c5925ef5d2fc7ad053ef454303e/src/cache-save.ts#L75-L90)，不是一次上传失败。

编译缓存与测试结果缓存不同：`-count=1` 禁止复用测试结果，编译后的包仍可复用；普通 go test 不缓存已链接的测试可执行文件。旧快照中缺少新增源码的编译变体会带来重复准备，但 CI 日志不能把未观察到测试二进制的全部时间都归为编译。此前作业时长比较还混用了 Go 1.26.8 与 1.26.7，不能据此计算缓存的净收益。

## 决定

两个既有作业共同使用[仓库内的 Go setup action](../../../../.github/actions/setup-go/action.yml)，把执行工具链固定为 Go 1.26.8，关闭 setup-go 内建缓存。每个作业使用一次 actions/cache，把实际 `go env GOMODCACHE GOCACHE` 返回的两个目录按该顺序存入同一快照。

主键包含缓存格式前缀、操作系统、架构、runner 镜像、实际 Go 版本、作业、go.sum 摘要及源码 SHA。相同源码可以精确命中；新源码先查同作业、工具链与依赖的前缀，成功执行后保存自己的快照。不同作业保留各自实际使用的编译变体，测试仍以 `-count=1` 执行。

最后一个 restore key 精确构造旧 setup-go 的同平台、工具链与依赖键，让现有 Go 1.26.8 快照可以为迁移提供种子。两个目录的字符串、顺序、压缩方式与版本盐均按固定 action 核对一致；键中的架构沿用其小写命名。依据分别是 [setup-go 的键](https://github.com/actions/setup-go/blob/b7ad1dad31e06c5925ef5d2fc7ad053ef454303e/src/cache-restore.ts#L17-L40)、[目录顺序](https://github.com/actions/setup-go/blob/b7ad1dad31e06c5925ef5d2fc7ad053ef454303e/src/package-managers.ts#L10-L14)及[缓存版本算法](https://github.com/actions/cache/blob/55cc8345863c7cc4c66a329aec7e433d2d1c52a9/dist/restore/index.js#L43408-L43422)。升级任一 action 时须重查这个耦合；不相容时允许正常 miss，不能修改缓存内部格式或删除远端缓存来伪造命中。

## 备选方案

**分别缓存 module 与每作业的编译目录。** 两种数据的所有权更清楚，module 不随源码重复保存；但单目录 archive 的隐藏版本与既有双目录快照不相同，迁移要接受初次冷缓存。

**保留 setup-go 快照，再叠加每作业的编译缓存。** 可直接复用旧快照，但每次先恢复旧编译数据，再覆盖新编译数据，增加重复下载和解压。合并快照用一次恢复承接旧数据，代价是重复保存 module 内容。

## 后果

源码变化可以产生可更新的编译快照，两个作业不再争夺一个永久不变的主键。Go 自身仍校验源码、工具链与编译选项的兼容性；缓存命中不能代替测试成功。

每源码、每作业保存组合快照会增加压缩、上传和仓库缓存空间消耗。缓存检索还受 ref 范围影响，PR 范围内的旧精确项可能先于默认分支的新前缀被选中；这影响速度，不改变 Go 的有效性校验。需要同时观察恢复、测试和 post-save 的整作业耗时，不能只报告测试步骤减少了几秒。当前确认了旧快照无法刷新与迁移兼容性，净 CI 耗时收益仍由实际运行判定。

两个 CI 作业、包分配、权限、事件范围、测试命令、race、`-count=1`、严格 verdict 和[包内覆盖](../testing/2026-09-09-sqlite-package-local-coverage.md)门禁保持不变。[执行预算](2026-09-08-budget-ci-race-test-execution.md)也不因缓存调整而放宽；这项决定只拥有工具链选择与构建缓存，测试本身的精简由[测试工作决定](../testing/2026-09-09-reduce-test-work.md)拥有。
