# Remote filesystem

Remote-fs 提供持久 volume，应用通过 Linux FUSE、Windows 本地 SMB endpoint 或 Go packages 访问。独立 Linux 服务包含 Web Console 和 CLI，用于管理 volumes 和文件。

每个 volume 只有一个 owner。管理员管理自己的 volumes 和文件，额外只能读取普通用户及其 volume 的名字、文件数、目录数、已使用 allocation 和配额；管理员身份不授予他人文件名、内容、挂载或修改权限。本地管理员使用账号密码登录，普通用户身份接入单独完成。Console 没有在线编辑器。

## 构建

使用仓库固定的 Go toolchain 和 pnpm 版本。前端依赖、严格 lossless JSON patch 与生成的 release assets 一起锁定。

```sh
make build
```

该命令先构建 Console，再将 `remote-fs` 和 `remote-fs-server` 写入 `bin/`。前端安装使用 frozen pnpm lock，生成 assets 后核对 source manifest，再由 Go embed 嵌入。`make console-check` 运行前端测试并核对 release assets 与源码一致。

## Compose 部署

提供的 Compose 部署运行一个应用容器和同源 TLS proxy。应用使用 UID/GID 10001、只读容器文件系统和私有持久 bind mount，无须 FUSE 或 privileged container。

1. 准备已有的私有 state 目录，owner 为 UID/GID 10001，模式为 0700。
2. 复制 [config.example.yml](deployments/service/config.example.yml)，将 `external_origin` 设为 HTTPS origin，容器内保留 `state_root: /var/lib/remote-fs`。
3. 将 `RFS_STATE_ROOT` 设为宿主 state 目录的绝对路径，`RFS_CONFIG` 设为配置文件绝对路径，`RFS_SITE` 设为匹配 `external_origin` 的公开 hostname。端口 80 和 443 必须能够到达 proxy。

```sh
docker compose -f deployments/service/compose.yml build app
docker compose -f deployments/service/compose.yml run --rm app init --config /etc/remote-fs/config.yml
docker compose -f deployments/service/compose.yml run --rm app admin create --config /etc/remote-fs/config.yml --login admin --display-name Administrator
docker compose -f deployments/service/compose.yml up -d
```

管理员命令从终端读取密码。自动化使用 `--password-file` 指向仅 owner 可读的普通文件，密码不进入命令行参数。建立或维护管理员时服务必须停止。初始化、管理员维护与服务运行竞争同一个 state-root 独占锁。

在配置的 HTTPS origin 打开 Console。新部署在身份系统接入前没有普通用户。

## systemd 部署

[remote-fs-server.service](deployments/service/remote-fs-server.service) 和 [remote-fs.sysusers](deployments/service/remote-fs.sysusers) 定义受限 `remote-fs` 账号与私有 `/var/lib/remote-fs` state 目录。将 server 安装到 `/usr/local/bin`，配置 `/etc/remote-fs/config.yml`，并提供同源 TLS proxy。

```sh
remote-fs-server init --config /etc/remote-fs/config.yml
remote-fs-server admin create --config /etc/remote-fs/config.yml --login admin --display-name Administrator
remote-fs-server serve --config /etc/remote-fs/config.yml
```

初始化和离线命令使用同一 service account。systemd unit 执行最后一条命令并处理 signals。`admin reset --config ... --login ...` 修改密码，使该用户现有 browser sessions 和 CLI credentials 失效。

## CLI

CLI 将 credential 保存在私有文件，默认是当前目录下 `.remote-fs/credentials.json`，`-credential-file` 可选择显式路径。父目录必须私有，文件仅 owner 可读写，符号链接被拒绝。login 默认从可取消的 no-echo 终端读取密码；`-password-stdin` 和 `-password-file` 提供明确的自动化输入。

```sh
remote-fs login -server https://files.example.com -login admin -password-stdin
remote-fs status
remote-fs user list -json
remote-fs user volumes -json <user-id>
remote-fs volume list -json
remote-fs volume create -name workspace -quota 20GiB
remote-fs volume show -volume <volume-id>
remote-fs volume rename -volume <volume-id> -name workspace-next
remote-fs volume quota -volume <volume-id> -quota 40GiB
remote-fs volume delete -volume <volume-id>
```

命令默认等待确认完成，`-no-wait` 只等待接受。durable work 在提交前保存 action journal；accepted 响应不表示已完成。响应丢失后查询或恢复原 action：

```sh
remote-fs operation show <operation-id>
remote-fs operation wait <operation-id>
remote-fs operation recover -action-file <journal-file>
```

quota change 比较 native quota revision，rename/delete 比较独立的 management revision。当前 quota 统计不可用时 CLI 报告错误，不编造 revision。

## 文件与挂载

```sh
remote-fs files ls -volume <volume-id>
remote-fs files mkdir -volume <volume-id> src
remote-fs files upload -volume <volume-id> ./local.txt src/remote.txt
remote-fs files upload -volume <volume-id> -replace ./replacement.txt src/remote.txt
remote-fs files download -volume <volume-id> src/remote.txt ./downloaded.txt
remote-fs files move -volume <volume-id> src/remote.txt remote.txt
remote-fs files rm -volume <volume-id> remote.txt
remote-fs mount -volume <volume-id> -mountpoint ./workspace -credential-file .remote-fs/credentials.json
```

mount 在前台运行，Ctrl-C 或终止 signal 解除挂载。原 `-server`/`-mountpoint` 挂载方式仍可用，可附带 credential transport。server 自身不挂载 volumes。删除先核对 live file、lock、stream、transfer 和尚未终结的 durable 原生责任；busy deletion 保持现有 mount 可用。

上传创建不存在的文件，或显式将被选名字替换为新 regular file。旧 open handles 保留旧对象；被选 symlink 可以被替换，但不会被跟随，目录不能被替换为文件。下载捕获并完整验证一个 immutable version，再输出成功内容，不覆盖已存在的本地目标。

durable upload journal 通过 `files query|resume|cancel -action-file FILE` 查询、续作或取消。普通 mkdir/rename/move/remove 使用现有有限 file-session action history；历史终结后仍未确定的结果继续报告 unknown，不以新 action 重试。普通失败退出 1，未确定的 operation result 退出 2。

文件列表保留原始名字与精确十进制 identity。`-path-base64` 接受 canonical padded standard base64，用于无法通过 UTF-8 输入的远程 path 参数；显示转义不成为操作目标。

`rotate` 修改 credential secret 并保留稳定 access identity，`logout` 或 `revoke` 使其失效。CLI 固定配置的 origin 并禁用 redirects；HTTP 仅在显式 development option 下允许。

## 存储、限制和健康状态

私有 state root 包含 lifetime lock、管理 SQLite 和各 volume 的持久目录。每个 volume 保留自己的 SQLite metadata 和 immutable local objects；ID 和 ownership 独立于显示名。

[示例配置](deployments/service/config.example.yml) 设置监听地址、外部 origin、私有 state root、受信代理及 idle/shutdown timeout。client 查询 service capabilities，不假设文件会话或传输上限。存储不可用、磁盘满和未确定结果作为错误报告。

`/health/live` 表示进程存活；`/health/ready` 表示服务可接受工作且 coordination workers 没有未解决失败。单 volume 状态和 native recovery protection 通过其操作报告；strong-lock recovery 期间健康读取和统计仍可用。logs 和 API errors 保留未确定结果。

## 嵌入与单 volume reference server

Go packages 可独立嵌入。host 提供认证和 `Authorizer`；可选 `AccessBinding` 将现有 stateful capabilities 绑定到业务身份，不把 service user model 引入库。

单 volume reference server 支持显式 `reference` subcommand 和原 direct flags：

```sh
remote-fs-server reference -listen 127.0.0.1:8080 -local-store /srv/reference-volume -volume workspace -quota 20GiB -initialize-lock-state
```

lock-state 初始化仅用于新的匹配 store，reopen 核对已有 evidence。reference mode 也保留 Azure Blob/SQLite 配置，其访问权限由 embedding 或 deployment host 提供。

## 设计与验证

[需求](docs/spec/requirements.md)定义系统保证；[服务架构](docs/design/server/service.md)描述 management 和 lifecycle；[文件系统架构](docs/design/architecture.md)描述库和 wire；[测试策略](docs/testing.md)规定真实存储、并发、崩溃和 mounted acceptance。

对变更组件运行相应检查。Blob tests 需要 emulator，mounted tests 需要 `/dev/fuse`，strict runner 拒绝 skipped tests。构建成功不能证明 owner 隔离、恢复、transfer publication 或 mount visibility。
