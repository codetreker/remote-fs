# Agent Note: 配额只在原生最终发布处计费

Status: implemented

## 问题

[R-WS-5](../../../../docs/spec/requirements.md) 要求空间统计来自实际存储，且成功写入不能越过配额。[limited.Rename 只锁源和目标路径的 stripe](https://github.com/codetreker/remote-fs/blob/252259f1b89682a5b2acf54004f0fe8b8677528d/packages/storage/limited/limited.go#L576-L624)，目录的子路径写入可以在采样大小后失去原来的目标；收费仍按被移走节点的大小计算。

在该提交上，以 localdir 和可阻塞的 `Write` 构造配额 4096 字节的 storage，初始 `d/a` 为 3072 字节、`e/a` 为 1024 字节。经 limited 写入 3072 字节到 `d/a`，在完成大小采样并进入底层 Write 前阻塞；依次经 limited 执行 `Rename("d", "old")`、`Rename("e", "d")`，再放行写入。所有调用成功，`old/a` 与新的 `d/a` 各占 3072 字节，实际用量 6144，报告用量仍为 4096。


这是旧路径采样实现的历史复现。名字解析在采样与发布之间变化后，包装层没有依据把某一次 Stat 的大小当作实际目标的大小；增加重数不能撤销已经成功的超额写入。

## 决定

`limited.New` 与 `NewWithLimits` 保持接收 `storage.Storage` 的签名，构造出的 wrapper 只持有同时提供 `storage.BoundedStorage` 与 `CheckPublicationAccounting` 的 private backend。配额和 measurement 配置先按原规则验证；缺少任一能力以 `ENOSYS` 拒绝，能力检查返回的错误原样传播，均发生在用量测量之前。`CheckPublicationAccounting` 必须确认底层能在最终发布处完成计费，不能只声明方法而绕过其义务。

所有路径修改把同一计费 hook 交给原生最终转换，由实际目标的新旧大小与效果推动计数。wrapper 不保留可选计费分支、路径大小采样或 hash stripe，也不为祖先目录增加另一套层级锁。scope、配对锁服务、retained File 与最后引用清理继续交给同一 backend；增长预留、Applied／NotApplied 结算和未知计费隔离遵守[缩短结算](2026-09-07-release-shrunk-quota-after-commit.md)，带外修改仍不属于受支持用法。

原生发布计费不等于要求所有 backend 提供权威用量查询。初始化与显式 Recount 优先采用 `Usage`；不支持 retained 文件时，仍可用有界目录遍历测量。支持 retained 文件的 backend 必须把 detached 字节纳入权威 `Usage`，不能以名字树代替。两个 measurement byte 上限、计费 revision 核对、最多八次重测与不确定账本隔离保留，见[容量上限](../architecture/2026-08-21-space-limit.md)。

本决定部分替代[容量上限](../architecture/2026-08-21-space-limit.md)接受不透明路径 backend 的选择，保留其额度、空间报告与显式重数的理由；也收束[文件锁](../architecture/2026-09-07-file-locks.md)和[移除宿主目录后端](../simplification/2026-09-08-remove-the-host-directory-backend.md)留下的通用路径计费缺口。

## 备选方案

**给路径采样增加祖先层级协调。** 可以让采样、目录改名与子路径修改互斥，但需要维护有界的层级锁状态、祖先与目标覆盖的获取顺序，以及取消后的释放；不同入口还必须共同遵守它。原生发布已经能识别最终目标，再实现一套路径协调会增加两套顺序的组合成本。

**用一个全局 gate 包住采样和整个修改。** 能消除这个交错，却让慢上传阻挡无关文件，与 R-CC-2 的独立推进要求冲突。现有 Recount gate 只负责测量与同步修改的交接，不扩展成这类上传互斥。

**同时要求所有 backend 提供权威 Usage。** 可省去初始化与 Recount 的目录遍历，却不决定一次修改的最终目标，也不修复采样后的改名。没有 retained 文件的 backend 可用现有有界测量获得完整初值；强制增加另一项能力会拒绝这些组合，超出修复发布计费所需的条件。

## 后果

不透明的有界 backend 在构造时明确失败，不能再借用 `limited` 的路径采样获得配额保证。第三方实现若要接入，须把计费准备、效果与收尾纳入自己的最终发布顺序；只在外层先 Stat 再调用写入不能满足这项能力。HTTP 与 replicated 不能把 Go context 内的计费 hook 传到远端最终发布处，配额必须在服务端的原生 backend 一侧配置。

配额判定仍可晚于对象暂存；超额以 `EDQUOT` 拒绝发布，不承诺避免此前的上传、分配或后续回收成本。pending／unresolved／garbage 的资源上限与清扫归属继续由对象存储管理。结果不明或计费收尾失败保持不可用，不能靠 Recount 或重试伪造可用额度。

目录改名不再改变受管修改的计量依据，且没有新增随路径增长的锁状态。初始化与 Recount 的有界遍历成本保留；retained 字节、原生权限和已接纳操作的生命周期结算也保持原规则。源码与断言入口见[服务端配额](../../../../docs/design/server/architecture.md#配额住在-storage-这一侧)和[最终发布测试](../../../../docs/testing.md#最终发布与观察次序)。
