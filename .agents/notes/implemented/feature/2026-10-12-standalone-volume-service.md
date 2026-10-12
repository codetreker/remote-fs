# Agent Note: 独立多用户 volume 服务

Status: implemented

## 问题

单 volume 文件核心可以发布持久文件树，但独立部署还需要用户登录、volume 归属和统一管理入口。使用者需要通过 Console 和 CLI 管理自己的多个 volumes，在 Console 中管理文件；管理员需要查看普通用户和容量摘要，又不能取得他们的文件访问权。

[R-SVC-1 至 R-SVC-8](../../../../docs/spec/requirements.md#九独立多用户服务)定义该产品形态。普通用户登录等待外部身份系统，owner 与服务端隔离不随登录入口延期。

## 决定

一个 Linux 服务组合 management SQLite、按需打开的 localstore volumes、同源 embedded Console 和 CLI。每个 volume 沿用自己的 SQLite metadata 与 immutable local objects；管理库保存账号、credential、immutable owner、volume 配置和操作责任，不复制文件树。管理员使用本地账号密码，普通用户没有临时注册或登录入口。Console 不提供在线编辑。

管理员管理自己的 volumes/文件，额外只能查看普通用户及 `name`、`fileCount`、`directoryCount`、`usedBytes`、`quotaBytes` 摘要或固定安全错误。管理员身份不授予他人文件名、路径、属性、内容、变更输出、挂载或修改权限；摘要 DTO 不暴露文件入口或 native revision。

[volume 进入契约](../../proposed/architecture/2026-08-19-volume-in-the-contract.md)保持单 storage 实例归属。服务负责可信 ID 到实例的映射、owner 和管理，文件 packages 不理解账号或角色。代码与 API 的现状由[服务架构](../../../../docs/design/server/service.md)定义，部署/命令由[仓库 README](../../../../README.md)定义。

### 代码归属

单 Go module，应用层为四个 package。service 内 auth/runtime/lifecycle/routes 使用文件分组，共享 manager；management 为一种具体 SQLite store，不为单一实现增加 repository 接口。

```text
cmd/
  remote-fs-server/            # serve、init/admin、reference
  remote-fs/                   # 管理、文件 CLI 与 foreground mount
  internal/password/          # 共用终端/私有文件密码输入
internal/
  service/                    # 认证、路由、runtime、lifecycle
  management/                 # SQLite store 和 migrations
  serviceapi/                 # wire 类型与 HTTP client
  console/                    # Go embed、TS src、pnpm lock、patches、dist
packages/
  storage/                    # 单 volume summary/quota/transfer
  metastore/sqlite/            # 原生计数、policy、receipt 与 publication
  storage/{objectstore,localstore}/
  transport/httprest/          # 文件协议与原变更通知
deployments/service/          # Compose、systemd、镜像与配置
```

依赖方向为 `cmd → service/serviceapi`、`service → management + console + packages`，packages 不导入 internal。Console 用 pnpm、TypeScript、React 和 Vite，UI build 先于 Go embed/build；浏览器只访问同源 API。

### 身份与原生事实

离线 init/admin 命令和 server 竞争同一个私有 state-root lock。管理员密码保存 Argon2id hash；browser 使用 Secure/HttpOnly/SameSite cookie、Origin 与 CSRF 检查。CLI secret 保存在项目私有文件，服务只保存 verifier。文件/mount/transfer 入口先认证并核对 owner，再打开 runtime；原 Authorizer 和 AccessBinding 继续约束已有 stateful capabilities 与持续输出。credential 轮换不改变 owner。

原生 Summary 在同一 healthy snapshot 返回文件/目录计数、allocation Used 与有效 quota。regular 和 symlink 各计一个文件，目录不计 root，脱名但仍被引用的 allocation 继续收费。quota 修改比较 native revision，rename/delete 比较 management revision；失败不回零或旧缓存。durable quota 与 receipt 由原 publication coordinator 提交，embedded/reference fixed allowance 保持独立。

Console 保留原始名字、NodeID 和目录条件，通过原 identity namespace 操作管理文件。管理数字用 decimal strings，v5 numeric wire 经 patched lossless codec 精确处理，不经过 JS Number。显示转义不成为操作 identity，也不施加 Windows-only 名字规则。

上传 private import 验证确切长度/digest 后，原子创建或 replace-name 为新 regular node，旧引用保留旧节点；symlink 本身可替换但不跟随，目录拒绝。下载同时 pin node 和 exact immutable key，完整验证一个版本后才输出成功。文件动作共用原锁/share、quota、log barrier 和通知，Console 不直写 SQLite 或补造通知。

### 管理责任

volume ID 不复用，owner/storage name 不变，显示名可以改。管理 operation 保留原 key、固定输入、结果或剩余责任；accepted 不表示 completed，响应丢失后查询同一动作，未知结果不换 key 重执行。

create 验证新根的 native binding 和持久结果后 active。delete 在 Stop 前封住 admission 并核对 live pins 与 native pending 责任，Busy 保持 mount 可用；确认 close 后以绑定根、proof 和 quarantine 继续回收。普通 Close 可移交 sealed import 供 reopen，不是永久删除证明。正常重启继续原 operation/native receipt；缺持久 evidence 不初始化为空 volume。

## 备选方案

**给单 volume reference server 加静态页面和配置列表。** 可以浏览预配置 volume，却没有动态 owner、create/delete 和管理员摘要隔离，不能满足所需管理能力。

**独立管理应用控制多个文件进程。** 文件故障域分开，但创建 volume 还需要进程/端口/存储调度和额外文件凭据。服务采用同进程 composition，保留 management 与 native volume 的职责分界。

**全部交给外部控制面。** 适合 embedding host 已有身份和注册体系的场景，不能提供所要求的独立 Console/CLI；packages 继续支持该用法。

**PostgreSQL 管理库。** 多节点共享管理行不能单独解决 localstore 与 authority 接管。当前是单机进程经 API 多用户访问，SQLite 不增加外部数据库部署；多节点成为需求时须同时设计存储接管。

## 后果

独立使用者得到 Console/CLI 管理入口，embedding host 保留自己的身份和控制面。管理事务与 native 事务不能合成一次跨库提交，operation 按原结果和剩余责任推进，代价是显式 lifecycle 和查询路径。管理员摘要与文件访问使用不同的权限路径，复用 DTO 或路由不能扩大披露。

部署使用私有本机持久目录和同源 TLS proxy，server 不需要 FUSE 权限。CLI mount 在前台运行，由 Ctrl-C/signal 卸载；既有 reference flags 保持可用。复用原 per-volume 文件/锁/传输上限，不承诺全局 physical/WAL/RAM 预算或全进程内存证明。

普通用户外部登录延期，未来受验证的 issuer/subject 关联稳定 UserID，邮箱或 role claim 不决定 owner/admin。备份/还原、反回滚体系、外部 NodeID 分配、运维 metrics 与专门日志系统不属于此决定。多节点 HA、跨 owner sharing/migration、文件历史、第三方管理 SDK 和 daemon 多挂载控制另有范围；没有在线编辑。
