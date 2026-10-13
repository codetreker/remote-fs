# Remote filesystem

Remote-fs 的文件系统 packages 提供 Linux FUSE、Windows 本地 SMB 和 Go 编程访问。独立管理服务包含 Console 和 CLI，管理用户自己的 volumes，并只读查看目录与文件元数据。

管理员使用账号密码登录，管理自己的 volumes，额外只读查看普通用户及其 volume 名称、文件数、目录数、已用空间和配额。管理员不能查看他人的逐文件信息。普通用户登录等待外部身份系统接入。管理服务不开放文件内容、上传、下载、修改、在线编辑或挂载。

## 构建

使用仓库固定的 Go toolchain 和 pnpm 版本：

```sh
make build
```

该命令先安装锁定前端依赖、构建 Console 并核对 assets，再将两个 binary 写入 `bin/`。`make console-check` 运行前端检查。

## 部署

[Compose](deployments/service/compose.yml) 提供一个 UID/GID 10001 的 app 容器，容器内使用 `listen: 0.0.0.0:8080` 和 `state_root: /var/lib/remote-fs`，HTTP 入口发布到宿主 loopback。`RFS_HTTP_PORT` 选择宿主端口，默认 8080。部署方提供同源 TLS proxy，将配置中的 HTTPS `external_origin` 转发到对应 loopback 端口；Console 与 API 使用同一个 origin，浏览器登录需要 HTTPS。

准备 UID/GID 10001 所有、0700 的私有 state 目录，复制[配置示例](deployments/service/config.example.yml)，设置本环境的 HTTPS `external_origin`。示例的 `trusted_proxy_cidrs` 为空。如果 proxy 转发客户端地址，通过此字段配置应用实际收到连接的受信直接代理地址 CIDR；Docker bridge 下该地址通常不是宿主 loopback 地址。server 不需要 FUSE 或 privileged container。

### GitHub Actions 发布

[服务发布 workflow](.github/workflows/release.yml) 手动触发，build 作业构建一次镜像并推送到私有 `ghcr.io/codetreker/remote-fs`。staging 先部署该镜像的 digest，Compose 就绪等待成功后，prod 自动部署同一 digest。任一步失败都会让发布失败；production 不重新构建镜像。

配置以下 GitHub Actions 变量与 secret：

| 位置 | 名称 | 内容 |
|---|---|---|
| 组织或仓库 variable | `OC_VM_HOST` | 部署机 SSH host。 |
| 组织或仓库 variable | `OC_VM_USER` | 可以运行 Docker Compose 的 SSH 用户。 |
| 组织或仓库 variable | `OC_VM_KNOWN_HOSTS` | 与 SSH host 匹配、已核验的标准 known_hosts 公钥记录。 |
| 组织或仓库 secret | `OC_VM_SSH_KEY` | 该用户的 SSH 私钥。 |
| `staging` Environment variable | `DEPLOY_PATH` | staging 部署目录的绝对路径。 |
| `prod` Environment variable | `DEPLOY_PATH` | production 部署目录的绝对路径。 |

部署机安装 Docker Engine 和 Docker Compose plugin。每个 `DEPLOY_PATH` 都由 SSH 用户持有，各自准备 `.env`、YAML 配置和已初始化的私有 state。`.env` 提供 `RFS_CONFIG`、`RFS_STATE_ROOT` 的宿主绝对路径和 `RFS_HTTP_PORT`。staging 与 production 使用不同的目录、配置、state 和 HTTPS origin；同机运行时还须使用不同的 HTTP 端口。

[部署脚本](.github/scripts/deploy-service.sh) 使用 `OC_VM_KNOWN_HOSTS` 校验 SSH host，通过 SSH/SCP 更新 `<DEPLOY_PATH>/compose.yml`，采用 `remote-fs-staging` 和 `remote-fs-prod` Compose project name，保留 `.env`、YAML 配置与 state。只有容器通过就绪等待后才写入 `release.env`，其中 `RFS_IMAGE` 保存此次完整 image digest。手动维护已发布环境时，同时传入 `--env-file .env --env-file release.env`。

构建作业使用自己的 `GITHUB_TOKEN` 发布 package，每个部署作业使用自己的 token 拉取私有镜像，无需另配 registry PAT。部署脚本将 registry 登录状态放在部署目录内的私有临时目录并在结束时清理。[GitHub 新 package 默认 private](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry)；保留该 visibility，并允许本仓库的 Actions 访问它。组织禁用 package 权限继承时，需在 package 的 Manage Actions access 中授予本仓库访问权。部署顺序与范围见[发布决定](.agents/notes/implemented/process/2026-10-13-stage-service-image-release.md)。

### 发布环境初始化

部署机需已有可运行的服务镜像，例如从包含仓库源码的构建目录生成并装载到部署机的 `remote-fs-service:local`。将 [Compose](deployments/service/compose.yml) 放到每个 `DEPLOY_PATH` 的 `compose.yml`，准备各自的 `.env`、配置与 state。在 staging 目录执行以下命令；production 使用自己的目录，并将 project name 改为 `remote-fs-prod`：

```sh
docker image inspect remote-fs-service:local
RFS_IMAGE=remote-fs-service:local docker compose --project-name remote-fs-staging --env-file .env -f compose.yml run --rm --pull never app init --config /etc/remote-fs/config.yml
RFS_IMAGE=remote-fs-service:local docker compose --project-name remote-fs-staging --env-file .env -f compose.yml run --rm --pull never app admin create --config /etc/remote-fs/config.yml --login admin --display-name Administrator
```

`RFS_IMAGE` 选择部署机已有的镜像；上述命令只初始化本环境的 state 和管理员。随后由发布 workflow 启动服务并写入已就绪镜像的 `release.env`。发布目录仅持有部署文件与本环境状态；源码构建使用下面的仓库目录命令。

### 从源码手动部署

在仓库目录设置 `RFS_STATE_ROOT` 为宿主 state 目录绝对路径，`RFS_CONFIG` 为配置文件绝对路径，然后执行：

```sh
docker compose -f deployments/service/compose.yml build app
docker compose -f deployments/service/compose.yml run --rm app init --config /etc/remote-fs/config.yml
docker compose -f deployments/service/compose.yml run --rm app admin create --config /etc/remote-fs/config.yml --login admin --display-name Administrator
docker compose -f deployments/service/compose.yml up --wait
```

管理员命令从 no-echo 终端读取密码，自动化可用 `--password-file` 指向仅 owner 可读的普通文件。初始化、离线管理员维护与服务运行竞争同一个独占锁；维护管理员时先停止服务。新部署没有普通用户注册入口。

### systemd 部署

[systemd unit](deployments/service/remote-fs-server.service) 和 [sysusers 配置](deployments/service/remote-fs.sysusers) 使用 UID/GID 10001 的 `remote-fs` 账号与同名组，以及私有 `/var/lib/remote-fs`。直接运行 binary 时将配置示例的 `listen` 改为 `127.0.0.1:8080`。安装 binary、配置同源 TLS proxy 后，以同一 service account 执行：

```sh
remote-fs-server init --config /etc/remote-fs/config.yml
remote-fs-server admin create --config /etc/remote-fs/config.yml --login admin --display-name Administrator
remote-fs-server serve --config /etc/remote-fs/config.yml
```

`admin reset --config ... --login ...` 重设密码并使该用户旧登录失效。配置包含监听地址、origin、state root、受信代理和 shutdown timeout。

## CLI 和 Console

CLI credential 默认保存在当前目录的私有 `.remote-fs/credentials.json`，可通过 `-credential-file` 选择显式路径。login 默认读取 no-echo 终端密码，自动化可用 `-password-stdin` 或私有 `-password-file`。`logout` 在远端撤销成功后清除本地文件；确切 `401/authentication_required` 也清除本地文件以便重新登录，同时以错误说明远端撤销未确认。其它远端失败保留本地 credential。

```sh
remote-fs login -server https://files.example.com -login admin
remote-fs status
remote-fs volume list -json
remote-fs volume create -name workspace -quota 20GiB
remote-fs volume show -volume <volume-id>
remote-fs volume rename -volume <volume-id> -name workspace-next
remote-fs volume delete -volume <volume-id>
remote-fs files ls -volume <volume-id>
remote-fs files ls -volume <volume-id> -path-base64 <directory-path-token>
remote-fs user list -json
remote-fs user volumes -json <user-id>
```

配额在创建时指定。volume 删除先关闭现有 store，再删除整份存储目录，最后原子移除管理行和 credential 的 volume 选择绑定。成功后不再出现在当前列表或占用注册名额，failed/deleting 行仍占名额；失败报告错误，不表示已经删除。已有 credential 的最后一个所选 volume 被删除后没有 volume 权限，签发新的显式选择 credential 仍拒绝空集合。Console 的 Files 页面只读展示自己的目录和现有元数据，管理员 Users 页面只展示普通用户与五项 volume 摘要。

`files ls` 的目录 token 由前一次列表的目录 pathBase64 和条目 nameBase64 原始字节组成，显示名不作为请求路径。active volume 的必要存储 evidence 缺失或不完整时，metadata API 返回 `503/storage_unavailable`，摘要显示 `metadata_unavailable`，不会初始化成空 volume。文件数与目录数从现有 metadata snapshot 统计，符号链接计一个文件且不展开；Used 与配额单独读取 Space。读取或关闭失败显示明确错误，不回零或旧数据；统计不承诺与容量查询跨调用原子。raw 名字/路径和 decimal-string 数字独立于显示文本保存。

## 既有文件系统入口

Go packages 与单 volume reference server 保持原有能力和接口。reference 模式和 direct flags 继续支持原本的 local/Azure 文件服务，其访问权限由 host 或部署方提供：

```sh
remote-fs-server reference -listen 127.0.0.1:8080 -local-store /srv/reference-volume -volume workspace -quota 20GiB -initialize-lock-state
```

lock-state 初始化仅用于新的匹配 store，reopen 验证已有 evidence。原 `remote-fs -server ... -mountpoint ...` 使用 reference 文件入口；管理服务没有相应挂载 endpoint。

[需求](docs/spec/requirements.md)、[管理服务架构](docs/design/server/service.md)与[文件系统设计](docs/design/architecture.md)分别定义产品范围和现有机制。[测试策略](docs/testing.md)保留既有检查要求，构建成功不能替代权限和实际管理行为验证。
