# Agent Note: workspace 进入契约

Status: proposed

## 问题

workspace 必须长期存在、能冷挂载，而且存在的数量可以远大于同时挂载的数量（R-WS-1 至 R-WS-4、R-SCALE-2）。本提案提出时，设计没有说明 workspace 的归属；这会留下三个影响接口的问题：

- **storage 是一个 workspace 的命名空间，还是按 workspace 寻址的？** 这决定 R-INT-6 的第三方实现者到底要实现什么。
- **变更日志的全序是每服务端还是每 workspace？** 见[变更日志的身份](2026-08-19-change-log-identity.md)。
- **谁创建、列举、销毁 workspace？** R-WS-1 说它们长期存在于服务端、与是否挂载无关。而 CLI 按名字挂载，设计里没有任何东西解析这个名字。

鉴权将来也挂在这里 —— 没有身份就没有「这个 workspace 属于谁」。

当前[中心契约](../../../../docs/design/architecture.md)已经把一个 storage 实例定义为一份 workspace 命名空间，[元数据复制](../../implemented/architecture/2026-08-27-metadata-replication.md)按 workspace 隔离日志。[显式文件锁](../../implemented/architecture/2026-09-07-file-locks.md)进一步将授权方与同一份命名空间配对，保存归属证据并隔离另一写入授权方。这些实现解决实例内部的状态归属；一台 server 按不透明名字提供多个实例的注册、选择与生命周期边界仍是本提案。

## 提案

### 一个 storage 实例 = 一个 workspace 的命名空间

`storage` 的数据操作不逐次携带 workspace 参数；一个实例就是一份命名空间，包含根节点、层级与内容。这项归属已经成立。

本提案要求 server 持有**一组具名的 storage 实例**，名字到实例的映射由部署方提供。客户端挂载时给出名字，server 据此选中一个实例；当前单 namespace 的发布入口尚未提供这组注册与选择操作。

第三方实现者（R-INT-6）因此只需要实现「一份命名空间」，不必理解 workspace、不必处理隔离、不必在每个操作里携带一个他们用不上的标识。

发布实例还必须与约束该 namespace 的强锁授权方配对。Session、Owner、Grant、逻辑资源与动作历史都属于这个授权方；不能把一个实例上的 proof 用到另一个实例，也不能仅凭同名 workspace 重放旧动作。[实时文件句柄](../../implemented/architecture/2026-09-08-live-file-handles.md)另有 FileSession / File 与 advisory owner；每个挂载独立持有会话，同一 namespace 共享原生协调者。引用退役不重开旧路径，强占有的历史窗口与重启保护仍独立成立。

### 每个 workspace 一条变更日志

日志跟着 storage 实例走：独立的位置序列、独立的保留窗口、独立的化身。

一个客户端只订阅它所挂载的那个 workspace，因此不会收到别人的事件，也不会因为别人的流量被挤出自己的窗口。

文件锁的状态也不能跨实例混用。SQLite-backed 模式使用数据库与独立 Witness；外部 SQLite 的证据与唯一活跃拥有者覆盖整份数据库，后续启动选择另一个已有 namespace 仍须执行数据库级恢复等待；localstore 保持单 workspace 根绑定。[移除宿主目录后端](../../implemented/simplification/2026-09-08-remove-the-host-directory-backend.md)取消了单独的 StateRoot 配置。这些持久最大时长证据与恢复屏障不构成多个 workspace 的管理注册表，也不改变日志位置的含义。

### workspace 的创建与销毁不由本系统负责

本提案选择「按名字挂载一份已存在的命名空间」。创建、列举、销毁 workspace 的管理策略由部署方在自己的控制面完成，因为它们与计费、归属、保留策略绑在一起。随附 backend 的显式初始化与打开负责建立、验证一份存储，不代替这个管理控制面。

配额只有取值这一半留在那里：**报出配额与执行配额是本系统的事**，见[workspace 的容量上限](../../implemented/architecture/2026-08-21-space-limit.md)。workspace 的逻辑用量不等于 Blob container 或底层磁盘的剩余容量；持有命名空间的 storage 负责计量并拒绝越限修改。

R-WS-1（workspace 长期存在于服务端，与是否挂载无关）在这个提案中的边界是：挂载与卸载不销毁 workspace，命名空间的存续由部署方掌握。成功卸载结束该挂载的 FileSession、引用与 advisory 状态；EBUSY 不撤销仍在使用的会话。文件删除可以移除名字，仍被有效引用持有的 detached 文件继续占用配额，直到引用结束并完成清理；这些对象生命周期不构成 workspace 管理操作。

**这是一个范围决定，需要写进 `docs/spec/`**，因为它划的是系统边界，不是实现方式。

### 名字是不透明的

server 不解释 workspace 名字的结构。它是一个查找键，映射由部署方配置。

不规定命名规则，是为了不预设部署方的组织方式（按用户、按任务、按项目都可以）。

## 备选方案

**storage 按 workspace 寻址：每个操作携带 workspace 标识。** 一个实现即可服务全部 workspace，对「所有 workspace 都在同一个数据库里」的集成方最自然。输在三点：第三方实现者必须自己处理隔离，而隔离写错是静默的越权；日志作用域从「自然按实例分」变成「必须额外声明并强制执行」；以及每个操作都携带一个多数实现用不上的参数。

**不引入 workspace 概念：挂载时给一个地址，得到一份命名空间。** 最小改动，且 storage 契约完全不变。输在 R-WS-1 无人负责 —— 「长期存在、偶尔挂回来」需要有东西记得它存在，而「地址」这个说法把这件事推给了不存在的第三方。它实际上是把问题改名，不是解决。

**本系统负责 workspace 的创建与销毁。** 自成闭环，CLI 可以直接建一个 workspace 开始用。输在它把配额、归属、计费、保留策略拖进来 —— 而这些必然与部署方已有的系统冲突。且「销毁」意味着删除整份命名空间，与由部署方掌握其存续的边界相悖。

**名字带结构（例如 `用户/项目`）。** 便于按前缀做隔离与鉴权。输在它预设了部署方的组织方式，而我们没有依据这么预设。不透明的名字不阻止部署方在自己那侧使用结构化名字。

## 验收标准

- `docs/design/architecture.md` 出现 workspace，并写明「一个 storage 实例 = 一个 workspace 的命名空间」。
- `storage` 契约中不含任何 workspace 参数。
- server 侧写明它持有一组具名 storage 实例，名字到实例的映射由部署方提供。
- 变更日志按 workspace 分，与[变更日志的身份](2026-08-19-change-log-identity.md)一致。
- workspace 管理策略由部署方负责、挂载与卸载不销毁 workspace 的边界在 `docs/spec/` 中确定，不与 backend 初始化或文件删除混淆。
- 挂载流程写明：CLI 给出名字 → server 解析为实例。

## 风险

**workspace 生命周期与存储初始化不能混淆。** 解除挂载不销毁命名空间，显式初始化只建立一份存储，文件删除仍会删除其中的数据。多实例控制面必须分别表达这些动作；缺失的锁状态或 workspace 证据不能被管理入口当作新实例重新初始化。

**名字不透明意味着 server 无法做任何基于名字的检查。** 鉴权到来时，「谁能挂载哪个 workspace」的判断必须完全由外部提供的映射与凭据决定，server 不能从名字里推断任何东西。这是刻意的，但它限制了鉴权方案的形状 —— 相关取舍留给鉴权那份 note。

**多实例的全局资源限制仍未定。** 强锁授权方和文件服务分别限制会话、引用、owner、范围、动作与等待，并对会话再设内部上限；这不等于一台 server 的多 workspace 总量已有约束。日志保留窗口按实例隔离，新增注册表时仍须定义跨实例的入站并发、内存与状态总量，不能让每实例有界掩盖实例数无界。
