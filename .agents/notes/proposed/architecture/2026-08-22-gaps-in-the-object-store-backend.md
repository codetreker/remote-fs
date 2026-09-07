# Agent Note: 对象存储后端已知没做的事

Status: proposed

## 问题

[对象存储命名空间](../../implemented/architecture/2026-08-21-namespace-in-an-object-store.md)与[本地磁盘对象存储](../../implemented/architecture/2026-09-04-local-disk-object-store.md)通过各自的契约与故障用例，仍有一批位于契约之外的已知缺口。它们不会让已验证的路径变成错误，但会在数据修复、Azure 行为和大规模垃圾积累时决定系统能否恢复或保持可运维。

这些缺口共享一个范围决定：尚无证据要求把它们放进当前交付，却必须保留可搜索的名字、可观察后果与完成条件。否则一个没有记录的缺口与一项已经存在的能力在代码旁边长得相同。

对象发布结果无法证明归属时的 fail-stop 规则由[未证实对象发布进入 unresolved](../../implemented/architecture/2026-09-04-unresolved-object-publication.md)拥有，不属于下面的待实现集合。

## 提案

以下事项分别处理；任何一项进入实现时都要拥有自己的决定与验收证据。

**namespace entry 与路径深度没有语义上限。** local-disk object key、payload 和 HTTP body 已有各自的上限，但使用者给出的 entry name 仍不受 Linux 常见的 255-byte component 限制，路径也可以任意深。metastore 的逐层解析会把深度直接变成一次调用内的查询数；`localdir` 由宿主内核拒绝的名字，object-store namespace 可能接受，两个后端因而会给出不同边界。

**用量计数损坏后没有带内修复。** metastore 在事务里维护精确的 referenced payload 用量，`Space` 发现数字不自洽时以 `EIO` 失败，不把它夹回一个看似合理的值。这保护了 R-ERR-2，代价是缺陷、部分恢复或人工修改一旦破坏账本，object-store namespace 没有与 `limited.Recount` 对应的修复操作，`Space` 会持续失败。

**Azure 的 `ResourceNotFound` 归类未定。** azblob 把它保守地归为 `EIO`，因为它既可能表示对象缺席，也可能表示账户或 container 缺席。若服务端实际上能把其中某一种证明为对象不存在，当前清扫器会对它持续重试；若贸然映射成 `ENOENT`，账户或 container 不可达又会被伪装成对象已经删除。

**没有对真实 Azure 的持续验证。** azblob 的集成用例运行在 Azurite。服务端摘要、错误码、条件请求与 API version 的差异来自 Azure 文档，不是同一套测试对真实服务的观测；Azurite 通过不能证明生产服务的全部行为相同。

**Azure SDK 的版本受 Azurite 上限约束。** Azurite 接受的最高 `x-ms-version` 与 SDK 默认发送的版本必须匹配。升级 SDK 需要先证明当前 emulator 能接收它，并重新运行真实错误分类与 digest 用例；单独覆盖一个请求头会让测试配置与生产默认分叉。

**SQLite 的跨进程写争用没有直接用例。** `busy_timeout` 已配置，但 metastore 并发用例使用同一进程里的连接池。local store 的 lifetime `flock` 阻止两个 owner 同时打开同一 root，不能证明 Azure Blob 模式下两个进程共享一份 SQLite metastore 时的等待、超时与错误分类。

**`Sweep` 的无错误部分完成分支不可达。** `discard` 只有在 `Delete` 失败时才会返回少于输入数量的 `gone`，而 `Sweep` 会先返回该错误，再走不到 `gone < len(keys)` 的无错误停止分支。部分删除与 `Forget` 同时失败已经有用例，未解决的是这个分支应删除，还是 `discard` 应明确允许不带错误的部分完成。

## 备选方案

**把每一项拆成独立 proposed note。** 每项的完成条件会更聚焦。输在它们目前都没有被排期，拆开会复制同一个范围理由，也会把「这是对象存储后端契约之外的已知集合」分散到多处。任何一项真正进入实现时再拆出拥有者。

**把已经解决的 broad claim 原样留在清单里。** 会保留原始列表的稳定性。输在 object key/payload/HTTP body 已经有界，schema migration 已经实际前滚，local-store lifetime ownership 也有实现和用例；继续保留旧概括会遮住仍然存在的 namespace component/depth 与 SQLite contention 缺口。

**不记录，等故障出现再定位。** 少一份长期维护的清单。输在表现会误导诊断：损坏的用量看起来像 SQLite 故障，Azure 账户缺失可能看起来像对象垃圾永远删不掉，跨进程争用则可能只表现为一次普通超时。

## 验收标准

每一项都必须满足以下两者之一：由独立决定实现，并以针对该缺口的用例或实测证据关闭；或者以新的证据明确判定不需要处理，并把理由写入 rejected note。最后一项离开本清单后，这份 note 删除，不保留空的 proposed 容器。

## 风险

这是一份已知集合，不是完整性证明。契约测不到的未知仍可能存在；新增缺口只有在能写出具体失败结果、受影响边界与完成条件时才进入这里，不能用「可能还有问题」扩大成不可验收的范围。
