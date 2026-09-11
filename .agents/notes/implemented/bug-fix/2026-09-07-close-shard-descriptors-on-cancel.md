# Agent Note: 对象操作取消后必须关闭已打开的 shard 描述符

Status: implemented

## 问题

严重级别：P1。[R-INT-3](../../../../docs/spec/requirements.md) 要求可累积资源具有可配置上限。[Get 先打开 shard，再等待 admission，最后才安装 Close](https://github.com/codetreker/remote-fs/blob/252259f1b89682a5b2acf54004f0fe8b8677528d/packages/storage/objectstore/localdisk/objects.go#L497-L511)；[Delete 有相同顺序](https://github.com/codetreker/remote-fs/blob/252259f1b89682a5b2acf54004f0fe8b8677528d/packages/storage/objectstore/localdisk/objects.go#L581-L597)。等待执行名额时取消，会返回错误但遗留 shard 目录文件描述符，后续请求可反复累积泄漏。

在该提交上，创建一个对象，将 `MaxInFlightOperations` 设为 1 并占住唯一执行名额。对现有对象启动 `Get`，确认 shard 描述符已打开后取消 context；重复三次，每次调用返回 `EINTR`，共遗留三个 shard 描述符。独立重复三次 `Delete` 同样遗留三个。计数通过 `/proc/self/fd` 中指向该 shard 的描述符完成。

## 决定

`Get` 与 `GetBounded` 共用的 `get` 路径，以及 `Delete`，在 `openShard` 成功返回有效描述符后立即登记一次关闭，覆盖之后的每条退出路径。shard 缺席或打开失败没有交给调用方的有效 FD，不登记关闭；已有 admission、健康检查与缺席判断的顺序保持。

这与 `Put` 已使用的所有权模式一致。waiting ticket、active reservation、Delete 的 key 锁继续由各自已有的释放路径管理；等待提升被取消或提升后的健康检查失败，也会执行 shard 的关闭。单纯取消仍为 EINTR，健康与存储故障保留各自分类，不为释放描述符吞掉原错误。

本修复补足[本地对象存储](../architecture/2026-09-04-local-disk-object-store.md)的资源生命周期；上限与打开协议见[存储设计](../../../../docs/design/server/local-disk-object-store.md)，断言分工见[测试策略](../../../../docs/testing.md)。

## 备选方案

**在每个错误出口显式关闭。** 可以修复已知的 admission 与健康失败分支，但让同一描述符的释放义务分散在多个 return；后续增加出口时还须逐处补齐。取得所有权时登记关闭，使新增退出路径自然受到同一约束。

**先提升 admission，再打开 shard。** 可以减少等待者持有的 FD，却改变现有 waiting／active 并发安排与错误顺序。同 shard 的协调工作会进入不同的 active 占用阶段，缺席、健康与取消的优先顺序也需要重新设计；这不是修复释放遗漏所需的改变。

## 后果

重复取消不再把一次请求的 shard FD 留到进程结束；资源仍由已有 waiting／active 上限约束，不增加新的 FD 配置或 API。缺席 shard 的 Get／GetBounded 仍报告 ENOENT，Delete 仍可成功，但只有先前的 admission 与健康检查允许执行到该分支时才如此回答。

提高进程 FD 上限只能延后旧泄漏耗尽资源，不能代替请求结束时释放所有权。本次调整不重排打开与 admission，也不改变目录 Close 的既有错误处理方式。
