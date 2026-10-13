# Agent Note: 独立 volume 管理服务

Status: implemented

## 问题

独立部署需要统一登录和多个 volume 的归属。使用者需要 Console/CLI 管理自己的 volumes 并查看文件元数据，管理员需要查看普通用户和容量摘要，又不能访问他们的逐文件信息。

## 决定

服务使用一份管理 SQLite，复用现有 localstore 的 List/Stat/Space、metadata snapshot 和创建时的 quota 配置。管理库保存用户、credential、immutable owner、volume 名称与存储配置。用户创建、改名和删除自己的 volumes，只读浏览自己的目录/文件元数据；管理员额外读取 ordinary-user 列表和 name/files/directories/Used/quota 摘要。普通用户登录等待身份系统接入，本地管理员使用账号密码。

单 Go module 中，`internal/service` 组合认证、管理 API 和现有 stores；`internal/management` 是具体 SQLite store；`internal/serviceapi` 保存 wire/client；`internal/console` 保存 TS UI 与 Go embed。文件 packages 和原协议不引入账号、服务 registry 或新原生能力。目录与运行机制见[服务架构](../../../../docs/design/server/service.md)，部署和命令见[README](../../../../README.md)。

文件元数据和摘要通过现有接口读取。服务遍历 Log.Snapshot/Next 计 regular+symlink 文件数与目录数，不跟随链接且不计 root；Used/quota 单独读取 Space。读取/关闭失败不回零，两次读取不承诺跨调用原子。所有接口在打开目标前校验当前身份和实际 owner；摘要不披露他人路径、属性、内容或文件入口。

quota 在 creating 时配置并初始化；active 在缓存或未缓存 store acquisition 前要求根中已有普通、非空、私有的 LOCALSTORE/FORMAT/metastore.sqlite evidence，缺失或不完整报错，不重新初始化。该 presence 检查不替代 native Open 的验证。删除关闭现有 store 后直接 RemoveAll 整份可信根，再于一个管理事务删除 volume 行及 credential_volumes 绑定；当前列表和注册名额只计算当前行，failed/deleting 保留。受限 credential 的最后一个 volume 被删后是零 volume 权限，签发新的显式选择仍拒绝空集合。close/remove/catalog 失败不报告成功。正常重启沿用管理行和原 native binding，不引入 quarantine、native receipt/proof、action namespace 或迁移计数器。

## 备选方案

**给单 volume reference 加静态配置页面。** 不能提供持久 owner 和每用户 volume 管理，也容易混用管理员与文件访问权限。

**另建管理应用控制多个文件进程。** 增加进程、端口、存储调度和管理凭据；同进程 service 可复用现有 store 并集中 owner 检查。

**PostgreSQL 管理库。** 单机管理 API 不需要外部数据库；更换数据库也不解决 localstore 的多节点接管。SQLite 保持部署独立。

## 后果

管理服务与文件系统库各自保留清楚的职责。文件页只读元数据，没有上传、下载、逐文件修改、内容读取或编辑；管理服务没有挂载或在线 quota 修改。既有 reference/embedding 文件能力继续由原入口提供。CLI logout 在撤销成功或确切 401/authentication_required 后清除本地 credential，后者仍返回远端撤销未确认的错误；其它远端失败保留本地文件。本地清除允许重新登录，不证明远端已撤销。

目录统计需要完整读取现有 snapshot，可能耗时或失败。服务不新增原生 snapshot/counter 保证；容量读取独立执行。普通用户登录延期仍要求稳定 UserID/owner，后续外部身份不能以可变邮箱或自行声明角色替代归属。
