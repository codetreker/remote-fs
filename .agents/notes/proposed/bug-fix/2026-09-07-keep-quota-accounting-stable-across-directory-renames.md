# Agent Note: 目录改名不得使配额收费所依据的大小失效

Status: proposed

## 问题

严重级别：P1。[R-WS-5](../../../../docs/spec/requirements.md) 要求空间统计来自实际存储，且成功写入不能越过配额。[limited.Rename 只锁源和目标路径的 stripe](https://github.com/codetreker/remote-fs/blob/252259f1b89682a5b2acf54004f0fe8b8677528d/packages/storage/limited/limited.go#L576-L624)，目录的子路径写入可以在采样大小后失去原来的目标；收费仍按被移走节点的大小计算。

在该提交上，以 localdir 和可阻塞的 `Write` 构造配额 4096 字节的 storage，初始 `d/a` 为 3072 字节、`e/a` 为 1024 字节。经 limited 写入 3072 字节到 `d/a`，在完成大小采样并进入底层 Write 前阻塞；依次经 limited 执行 `Rename("d", "old")`、`Rename("e", "d")`，再放行写入。所有调用成功，`old/a` 与新的 `d/a` 各占 3072 字节，实际用量 6144，报告用量仍为 4096。

## 提案

[文件锁的原生发布集成](../../implemented/architecture/2026-09-07-file-locks.md)已提供 `CheckPublicationAccounting`：普通目录在实际发布处报告新旧大小，limited 的 Write、Remove 与 Rename 因而不再先按路径采样。该路径把收费与实际目标绑定，同时让暂存保持在最终转换之外。

本提案继续覆盖没有原生计费能力的有界第三方 backend。它们仍使用路径采样与直接路径的 stripe，祖先改名仍可使计量依据失效。修复须保证采样、收费与实际修改针对同一个对象，并覆盖改变路径解析结果的父目录改名；不能把可执行的普通 storage 包装误称为已经具备原生发布能力。

本项补足[容量上限](../../implemented/architecture/2026-08-21-space-limit.md)的通用路径协调，且与[缩短提交后释放配额](../../implemented/bug-fix/2026-09-07-release-shrunk-quota-after-commit.md)分别验收。原生路径的测试不能替代以下通用契约的验收。

## 备选方案

原生最终发布计费已经用于能确定实际目标和效果的 backend；它要求实现者提供这项能力，不能由包装层先 Stat 再 Write 模拟。未比较不透明第三方 storage 的具体协调方案。修复须保留 R-CC-2 的不同文件写入独立性和 R-INT-3 的资源上限，不能通过无界的逐路径锁表维持协调。

## 验收标准

- 将上述交错作为回归，等待与放行由测试闸控制：操作结束后实际用量不得超过 4096，`Space.Used` 必须等于实际用量；若写入最终面对 1024 字节的新目标，应在提交前正确计费并以 `EDQUOT` 拒绝越限。
- 覆盖多级祖先改名、目标覆盖、两个相反方向的改名，以及协调等待中的取消；不得死锁、漏收费或重复释放。
- 与改名无关的不同文件写入仍可独立推进；协调状态的数量有固定或可配置的上限。

## 风险

延后的是通用包装路径的配额计量保证，目录改名仍可使该账本永久少算并允许后续继续超额。这类 backend 不能同时发布非空锁授权方，避免用不完整计费支撑受保护的修改。协调过粗会扩大无关操作互相阻塞的范围，锁顺序不一致会死锁。此项约束配额层的计量对象，不决定[已打开文件的写入身份钉住](../architecture/2026-08-20-nothing-pins-an-open-file.md)方案。
