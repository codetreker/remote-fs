# Agent Note: volume 进入契约

Status: proposed

## 问题

volume 必须长期存在、能冷挂载，而且存在的数量可以远大于同时挂载的数量（R-WS-1 至 R-WS-4、R-SCALE-2）。本提案提出时，设计没有说明 volume 的归属；这会留下三个影响接口的问题：

- storage 是单个 volume，还是每个操作按 volume 寻址？这决定 R-INT-6 的第三方实现者要实现什么。
- 变更日志的全序是每 server 还是每 volume？见[变更日志的身份](2026-08-19-change-log-identity.md)。
- 谁把客户端选择的 volume 映射到一份存储，谁保存注册和生命周期责任？挂载地址不能自行承担 R-WS-1 的长期存续。

当前[中心契约](../../../../docs/design/architecture.md)已经把一个 storage 实例定义为一个 volume，[元数据复制](../../implemented/architecture/2026-08-27-metadata-replication.md)按 volume 隔离日志。[显式文件锁](../../implemented/architecture/2026-09-07-file-locks.md)进一步将授权方与同一个 volume 配对，保存归属证据并隔离另一授权方。这些实现解决实例内部的归属；多实例的注册、选择和总资源管理仍需要拥有者。

[独立多用户 volume 服务](../feature/2026-10-12-standalone-volume-service.md)拥有 R-SVC-1 至 R-SVC-8 的随附控制面，补齐该部署形态的账号、owner、注册、创建删除、跨实例预算和 Console/CLI。本 note 保留单 volume 库契约和嵌入方的归属，独立服务的管理机制只在服务提案中定义。

## 提案

### 一个 storage 实例就是一个 volume

`storage` 的数据操作不逐次携带 volume 参数；一个实例包含一个根节点、层级、内容与独立状态。这项实例归属已经成立，新增注册表不改变它。

发布方持有一组实例，把可信 volume 标识映射到正确实例，再执行该实例的文件操作。客户端对独立服务使用稳定、不复用的 volume ID；显示名称可改，不作为持久后端的根绑定。嵌入方仍可以使用自己的可信选择键和注册表，文件 packages 不要求采用服务的 SQLite 或用户模型。

第三方实现者只需要实现“一个 volume”，不必承担跨 volume 用户、注册与选择，也不在每次文件操作里携带无关标识。注册表不能把同一后端重复发布成独立 authority。

### 状态随实例隔离

每个 volume 的日志有独立位置序列、保留窗口和化身。客户端只订阅所选 volume；其它 volume 的流量不进入其日志，也不挤占其历史窗口。

storage 实例与强锁授权方配对。Session、Owner、Grant、逻辑资源和动作历史属于该 authority；不能把一个实例上的 proof 用到另一个实例，也不能仅凭同名 volume 重放旧动作。[实时文件句柄](../../implemented/architecture/2026-09-08-live-file-handles.md)的 FileSession、File 与 advisory owner 同样保持实例归属；每个挂载独立持有会话，同一 volume 共享原生协调者。

SQLite-backed 模式使用数据库与独立 Witness；外部 SQLite 的证据和唯一活跃拥有者覆盖整份数据库，后续启动选择另一个已有 volume 仍须执行数据库级恢复等待。localstore 保持单 volume 的永久根绑定。[移除宿主目录后端](../../implemented/simplification/2026-09-08-remove-the-host-directory-backend.md)取消了单独 StateRoot 配置。这些恢复证据和屏障不构成跨 volume 注册表，也不改变日志位置。

### 库与独立服务分别拥有管理

对可嵌入 packages，volume 的创建、列举、销毁、归属、计费和保留策略由集成方控制。文件 packages 不新增内置账号或控制面；[嵌入方操作授权](../../implemented/feature/2026-09-10-host-provided-authorization.md)仍由宿主提供身份 context 和逐 volume 操作策略。

独立服务由本系统交付管理控制面，其 SQLite 注册表、稳定 owner、创建删除责任与管理员摘要由[服务提案](../feature/2026-10-12-standalone-volume-service.md)拥有。部署方不需要另写账号或注册应用，但仍负责宿主私有盘、配置和运维。两种形态共用单 volume 文件契约，身份认证责任按 R-INT-7 与 R-SVC-7 分别归属。

[volume 的容量上限](../../implemented/architecture/2026-08-21-space-limit.md)区分 quota 配置和原生执行。无论管理策略来自嵌入方还是独立服务，持有 volume 的 storage 都负责计量、报出容量和拒绝越限修改；逻辑用量不是 Blob container 或底层盘剩余容量。

挂载与卸载不创建或销毁 volume。成功卸载结束该挂载的 FileSession、引用和 advisory 状态；EBUSY 不撤销仍在使用的会话。文件删除可以脱名，detached 对象直到引用退役与清理完成仍占用配额；这不是 volume 删除。backend 显式初始化只建立匹配意图的一份存储，不承担控制面的 owner 注册或整个 volume 删除恢复。

### 选择键不承载授权

volume 的可信选择键是不透明的；server 不从名字前缀推断 user、project 或权限。独立服务把稳定 ID 映射到 immutable 内部存储名和根，把可改显示名留在管理库。嵌入方可以在自己的业务层使用结构化名称，仍必须显式确认实际 volume 和当前身份。

## 备选方案

**storage 按 volume 寻址，每个操作携带标识。** 对全部 volumes 放同一数据库的集成方自然；但它让每个实现自行处理隔离，日志作用域要额外声明并执行，而且多数实现每次接收一个用不上的参数。选择单实例归属，让多实例映射集中在发布方。

**只按挂载地址取得目录树，不规定注册和管理归属。** storage 契约可以保持不变，但没有角色保存“长期存在、偶尔挂回来”的事实。选择键与持久管理责任必须有实际拥有者。

**把创建销毁和账号放进文件 packages。** 可以让一个调用入口自成闭环，却把用户、计费和保留策略强加给 R-INT-6 的集成方。独立服务提供这些功能，库继续只拥有一个 volume 的文件语义。

**全部管理都要求部署方另建控制面。** 这是本提案最初针对嵌入场景考虑的范围，仍适用于集成已有身份和计费系统。独立服务增加 Console 与 CLI 管理后，部署方另建应用不再满足 R-SVC-1；[服务提案](../feature/2026-10-12-standalone-volume-service.md)承担该产品形态，库边界保留。

**名字带结构，例如 `用户/项目`。** 可辅助查找，却预设组织方式并诱使实现按字符串推断权限。不透明标识不妨碍宿主自行管理名称。

## 验收标准

- storage 数据契约保持单 volume，不含逐操作 volume 参数；日志、锁、会话、proof 与引用不能跨实例混用。
- 发布方的注册/选择在实际操作前选定可信实例，不从不透明名称推断访问权。
- 嵌入方的身份、注册和生命周期继续由宿主控制，不被迫使用独立服务用户或数据库。
- 独立服务按[服务提案](../feature/2026-10-12-standalone-volume-service.md)验证稳定 ID、owner、创建删除恢复、惰性 runtime 和跨实例预算。
- 挂载、卸载、backend 初始化、文件删除和 volume 删除各有独立可观察结果，不能互相代替。

## 风险

**volume 生命周期与存储初始化不能混淆。** 缺失锁状态、WAL 或根绑定不能被管理入口当作新实例重新初始化；未知数据不能被自动发布成空 volume。

**不透明选择键仍要求可信映射。** 宿主或服务必须把请求目标映射到实际实例，并对当前身份执行授权；名称和 ID 本身不是凭据。

**每实例有界不等于多实例总量有界。** 嵌入方自行承担其注册表的总预算；独立服务在[服务提案](../feature/2026-10-12-standalone-volume-service.md)中定义请求、runtime、session、stream、spool 和 action 的全局 admission，不能无限打开已注册 volume。
