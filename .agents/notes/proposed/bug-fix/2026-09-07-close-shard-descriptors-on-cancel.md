# Agent Note: 对象操作取消后必须关闭已打开的 shard 描述符

Status: proposed

## 问题

严重级别：P1。[R-INT-3](../../../../docs/spec/requirements.md) 要求可累积资源具有可配置上限。[Get 先打开 shard，再等待 admission，最后才安装 Close](https://github.com/codetreker/remote-fs/blob/252259f1b89682a5b2acf54004f0fe8b8677528d/packages/storage/objectstore/localdisk/objects.go#L497-L511)；[Delete 有相同顺序](https://github.com/codetreker/remote-fs/blob/252259f1b89682a5b2acf54004f0fe8b8677528d/packages/storage/objectstore/localdisk/objects.go#L581-L597)。等待执行名额时取消，会返回错误但遗留 shard 目录文件描述符，后续请求可反复累积泄漏。

在该提交上，创建一个对象，将 `MaxInFlightOperations` 设为 1 并占住唯一执行名额。对现有对象启动 `Get`，确认 shard 描述符已打开后取消 context；重复三次，每次调用返回 `EINTR`，共遗留三个 shard 描述符。独立重复三次 `Delete` 同样遗留三个。计数通过 `/proc/self/fd` 中指向该 shard 的描述符完成。

## 提案

让 shard 描述符的所有权从打开成功时起就覆盖全部退出路径，包括 admission 取消与后续健康检查失败。等待名额、执行名额、key 锁与描述符必须各自完成释放。

本项补足[本地对象存储](../../implemented/architecture/2026-09-04-local-disk-object-store.md)的资源生命周期；共享 `get` 路径的 `GetBounded` 也需纳入。

## 备选方案

未比较具体实现方案。提高进程 FD 上限只能推迟耗尽，不能改变一次取消遗留一个描述符的增长方式。

## 验收标准

- 对 `Get`、`GetBounded`、`Delete` 分别执行上述占满执行名额、打开 shard、取消的顺序；每次结束后该 shard 的描述符数恢复基线，重复取消不增长。
- 取消保留既有 `EINTR` 语义；释放执行名额后下一次正常操作成功，waiting、active 与 key 锁均无残留。
- 覆盖 shard 缺席、打开失败及打开后健康检查失败，确保已取得的描述符恰好关闭一次，未取得的描述符不会被误关。

## 风险

延后的是进程资源有界保证，客户端取消可使长期服务最终耗尽 FD 并影响无关请求。调整释放时机须区分 absent shard 与有效 FD，避免关闭无关描述符；不能用吞掉取消或 admission 错误来绕过泄漏路径。
