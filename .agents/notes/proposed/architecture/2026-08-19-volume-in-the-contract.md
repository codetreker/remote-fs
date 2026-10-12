# Agent Note: volume 进入契约

Status: proposed

## 问题

volume 必须长期存在、能冷挂载，存在的数量可以远大于同时挂载的数量（R-WS-1 至 R-WS-4、R-SCALE-2）。需要明确一个 storage 实例持有什么、日志和锁如何隔离，以及谁把可信选择键映射到存储并持有生命周期责任。挂载地址不能自行保存长期存续事实。

当前[中心契约](../../../../docs/design/architecture.md)已经定义一个 storage 实例就是一个 volume，[元数据复制](../../implemented/architecture/2026-08-27-metadata-replication.md)和[显式文件锁](../../implemented/architecture/2026-09-07-file-locks.md)分别落实日志与 authority 隔离。实例内部归属和多个实例的服务管理是两项责任。

## 提案

### 单实例归属

storage 数据操作不逐次携带 volume 参数；一个实例包含一份根、层级、内容和独立状态。第三方实现者只实现一个 volume，不必将跨 volume 用户或注册逻辑装进文件接口。发布方把可信选择键映射到正确实例，再执行其文件操作。

日志有独立位置序列、化身和保留窗口，客户端只订阅所选实例。锁 authority 与 storage 配对，Session、Owner、Grant、资源和动作历史属于该 authority；FileSession、File、NodeReference、Scope 和 advisory owner 同样不能跨实例混用。同 volume 的多个挂载共享原生协调者，各自持有会话。[变更日志身份](2026-08-19-change-log-identity.md)拥有日志选择。

外部 SQLite 的 Witness 与唯一活跃 owner 覆盖整份数据库，启动时换选其中另一 volume 仍须遵守数据库级恢复。localstore 保持单 volume 永久根绑定；这些 evidence 和 barrier 不构成多 volume 注册表。

### 管理归属

embedding host 继续持有身份、注册、创建、销毁、计费和保留策略；packages 不要求使用内置账户或管理 SQLite。逐 volume 操作授权由[host Authorizer](../../implemented/feature/2026-09-10-host-provided-authorization.md)提供。

[独立多用户服务](../feature/2026-10-12-standalone-volume-service.md)提供其部署形态的 SQLite 注册、稳定 owner、创建删除、管理员摘要和 Console/CLI。服务使用不复用 volume ID、immutable 内部存储名和根，显示名称可改；同一后端不能重复发布成两份独立 authority。部署方仍负责私有目录、配置和运维。

选择键不承载授权。服务和 embedding host 都必须显式确认实际实例与当前身份，不从名字前缀推断 user/project/权限。host 可以自定义组织名称，仍不能把名称当作凭据。

[容量上限](../../implemented/architecture/2026-08-21-space-limit.md)由 volume 自己计量、报告和拒绝超额增长；管理配置不能替代原生执行。embedding/reference fixed allowance 与服务的配额管理使用同一 Space 口径。

挂载和卸载不创建或销毁 volume。卸载结束该挂载的会话、引用和 advisory 状态，EBUSY 不撤销仍在使用的会话。文件删除可脱名，detached allocation 在最后引用和清理完成前继续收费；这不是 volume 删除。backend 初始化只建立匹配意图的一份存储，不承担 owner 注册或整个 volume 删除恢复。

## 备选方案

**每次 storage 操作携带 volume 标识。** 对多个 volumes 放同一数据库的实现自然，但让每个实现自行处理隔离和额外日志作用域，多数实现重复接收不用的参数。单实例归属将可信选择集中在发布方。

**只按挂载地址取得目录树，不规定管理归属。** 文件接口可以工作，却没有主体保存长期存续和冷挂载事实；可信选择和生命周期必须由 host 或 service 持有。

**把账号和创建销毁放进文件 packages。** 一个入口可闭环，却强迫 embedding 实现接受同一用户、计费和保留模型。独立 service 提供这些功能，文件 packages 保留中立语义。

**所有部署都由外部控制面管理。** 适合已有身份和计费系统的 embedding host，但不能交付 R-SVC 的独立 Console/CLI。服务承担该产品形态，host 保留自有控制面。

**名字使用 `用户/项目` 结构。** 可以辅助组织，却预设业务模型并容易被误用为授权。不透明选择键允许 host 自定义名称，权限仍显式检查。

## 验收标准

- 数据契约保持单 volume；日志、锁、会话、proof 和引用不能跨实例混用。
- 发布方在实际操作前选择可信实例并授权，不从名称推断权限。
- embedding host 不被迫使用独立服务用户或数据库；服务按[服务提案](../feature/2026-10-12-standalone-volume-service.md)验证 owner、管理和 Console/CLI。
- 挂载、卸载、backend 初始化、文件删除和 volume 删除分别报告真实结果。

## 风险

volume 管理不能绕过原生初始化与恢复。缺失 WAL、锁状态或根绑定不能被当作新空 volume，未知路径不能被自动发布或删除。ID 本身不是凭据，可信选择与当前身份授权始终属于发布方。
