# 独立 volume 管理服务

服务提供 Console/CLI volume 管理和只读文件元数据，复用现有 localstore、List、Stat 和 Space。文件 packages 的接口、SQLite schema、objectstore、localdisk、HTTP 文件协议和 locking 保持原有实现。

## 代码归属

| 位置 | 职责 |
|---|---|
| `cmd/remote-fs-server` | serve、init、离线 admin create/reset 与 reference 入口 |
| `cmd/remote-fs` | 登录、volume/user 管理及只读目录/元数据命令 |
| `internal/service` | 认证、owner 校验、volume 选择、管理 API 与 store 生命周期 |
| `internal/management` | 具体 SQLite store，保存用户、credential、owner 和 volume 配置 |
| `internal/serviceapi` | 管理 wire 类型与 HTTP client |
| `internal/console` | TypeScript UI 和 Go embed |

单 Go module。`cmd → service/serviceapi`，`service → management + console + packages`，packages 不导入 internal。Console 由 pnpm 构建后嵌入 Go binary；构建与部署见[仓库 README](../../../README.md)。

```text
cmd/{remote-fs-server,remote-fs}/
cmd/internal/password/
internal/
  service/          # auth、HTTP、store 选择与 volume CRUD
  management/       # SQLite store 和 migrations
  serviceapi/       # 管理/元数据 DTO 与 client
  console/          # Go embed、TS src、lockfile、dist
deployments/service/
packages/           # 既有文件系统库
```

私有 state root 保存管理 SQLite 和各 volume 的内部目录。server 与离线管理员命令竞争同一个独占锁。volume 的随机 ID、owner 和内部 storage name 不随显示名变化；目录路径由内部标识组成。native 根使用既有 localstore binding，管理库不复制文件树。

## 登录与授权

管理员通过离线命令创建/重设账号，密码保存 Argon2id hash。browser 使用 Secure/HttpOnly/SameSite cookie，mutation 检查同源 Origin 与 CSRF token；GET auth/session 恢复当前登录。CLI secret 保存在项目私有文件，服务保存 verifier。CLI logout 在远端撤销成功后删除本地 credential；收到确切 `401/authentication_required` 时也删除本地文件，允许重新登录，但返回远端撤销未确认的错误。其它远端失败保留本地文件。普通用户登录等待外部身份系统接入，没有生产临时登录入口。

credential 可以选择明确的 volume 集合。成功删除 volume 同时移除其选择绑定；已持久化 credential 的最后一个 volume 被删除后，空集合表示没有 volume 访问权限，不升级为 all-owned，也不使 credential 记录损坏。签发新的显式选择 credential 仍拒绝空集合。

每个管理或文件元数据请求先认证、读取实际 owner，再取得 store。管理员对自己的资源使用相同 owner 检查；额外只读权限是 ordinary-user 列表和 volume 名称/文件数/目录数/Used/配额摘要。目标 role 和当前 admin 权限在摘要扫描前检查。他人文件路径、属性、内容和文件 API 不进入摘要 DTO，隐藏页面入口不代替 API 拒绝。

身份响应 no-store，注销或切换身份清空 Console 私有页面状态。服务不挂载原 `/v5/` 文件 handler，也不签发远端文件或锁 capability。

## volume 和文件事实

creating 状态使用现有 localstore 配置初始化 volume 根，容量上限在此时指定。active 状态在每次取得缓存或未缓存 store 前，检查私有根和其中已有的普通、非空、私有 `LOCALSTORE`、`FORMAT`、`metastore.sqlite` 文件；缺失或不完整 evidence 返回 EIO，API 为 `503/storage_unavailable`，摘要为 `metadata_unavailable`，不走初始化路径。该 presence 检查不替代 native Open 的绑定与完整性验证。reopen 使用管理库中相同配置。显示名可以改，在线 quota 修改入口不存在。

文件浏览通过既有 List/Stat 取得路径、种类、大小和可用元数据，不读取内容。服务 DTO 用 decimal strings 保存整数，raw 名字与显示转义分开；显示字符串不替代实际请求路径。Console 文件页只有目录浏览和元数据。

摘要遍历现有 Log.Snapshot/Next 的元数据行计数，非目录（包括 symlink）各计一个文件，symlink target 不展开；root 不计目录。Used 与总量单独读取现有 Space，文件树不用于估算字节用量。snapshot 读取/关闭或 Space 失败返回明确安全错误，不返回零或旧结果。服务不新增原生计数器，也不承诺计数和 Space 的跨调用原子性。

## 创建、改名和删除

管理 SQLite 保存 owner、配置和 state。创建和改名完成后才返回成功；存储故障直接报告。删除先关闭服务持有的现有 store，再对可信 volume 根执行整目录 RemoveAll；两者成功后，在一个管理事务中删除 catalog row 和对应的 credential_volumes 绑定，完成才报告成功。列表和注册 volume 数量上限只计算当前行；failed/deleting 行仍保留并占用名额。close/remove/catalog 提交失败保留实际错误，不宣称成功。这是删除整份存储，不是逐文件 mutation，不引入 quarantine、native proof 或 action namespace 框架。

正常重启读取已有管理行和 localstore，缺绑定或残存数据不初始化为空 volume。Stop 停止新请求，Close 排空并关闭服务拥有的 store，失败保留实际错误。

管理 prefix 为 `/api/service/v1`。POST volumes `{name,quotaBytes}` 创建并返回 201 active Volume；PATCH volumes/{id} `{name}` 改名返回 200；DELETE volumes/{id} 没有 body，完成 close/RemoveAll 和管理行/credential 绑定删除后返回 204。GET volumes/{id}/files 的 path query 是 raw relative path 的 base64，返回只读目录 DTO：目录级 pathBase64 和 entries 的 nameBase64、name、nodeId、kind、sizeBytes、modifiedAt。CLI/Console 由目录 raw path 和条目 raw leaf 拼接子路径 token，wire 不逐项重复完整路径；显示 name 不作为请求路径。

Console 和 CLI 通过同一管理 API 执行 owner 校验，提供自己的 volume 列表/创建/改名/删除、文件元数据查询和管理员摘要。创建中或删除中的管理行在 Open 时通过同一同步流程继续处理，没有 operation journal 或后台 action worker。认证、输入错误、越权和存储不可用各自报告。

## 部署

Compose/systemd 使用 listen、external_origin、state_root、trusted_proxy_cidrs、shutdown_timeout。一个进程使用本机私有持久目录。Compose 包含一个 app 容器，容器内监听 `0.0.0.0:8080`，HTTP 入口发布到宿主 `127.0.0.1:8080`；systemd 直接运行时监听本机 loopback。部署方提供匹配 external_origin 的同源 HTTPS 代理，Console 与 API 共用该 origin，浏览器会话使用 Secure cookie。配置示例的 trusted_proxy_cidrs 为空；接收代理转发的客户端地址时，该字段配置应用所见的受信直接代理地址 CIDR。server 不需要 FUSE 权限。健康入口仅报告进程/服务状态；reference 模式仍提供既有单 volume 文件服务，权限由其部署方负责。
