# 独立多用户服务

`internal/service` 将现有单 volume 文件能力组合为 Linux 服务、同源 Console 和 CLI 管理入口。服务持有用户、登录、immutable owner 和管理动作；文件 packages 保持嵌入方提供身份的单 volume 契约。对应 R-SVC-1 至 R-SVC-8。

## 组件和数据

| 位置 | 职责 |
|---|---|
| `cmd/remote-fs-server` | serve、init、离线 admin create/reset 与既有 reference 入口 |
| `cmd/remote-fs` | 登录、volume/user/operation/file 命令和 foreground mount |
| `internal/service` | 认证、HTTP 路由、runtime 选择、create/quota/delete 和停止 |
| `internal/management` | 具体 SQLite store，保存用户、credential、owner、volume 配置与操作状态 |
| `internal/serviceapi` | 管理 wire 类型和 HTTP client，不持有 store 或权限 |
| `internal/console` | TypeScript UI 与 Go embed，浏览器只访问同源 API |
| `packages/` | 原生文件、日志、锁、summary/quota 和 import/export，不读取服务角色 |

`cmd → service/serviceapi`，`service → management + console + packages`。service 的 auth/runtime/lifecycle/routes 是同一个 package 的文件组。Console 先由 pnpm 构建，再嵌入 Go binary；部署和命令见[仓库 README](../../../README.md)。

```text
state_root/
  service.lock
  data/
    root.json
    management.sqlite
    volumes/<volume-id>/
    quarantine/<volume-id>-<operation-id>/
    transfers/
```

server 与离线管理员命令竞争同一个私有 root 的 exclusive lock。root.json 和管理数据库身份匹配，native 根沿用 localstore 的持久绑定校验。volume ID 不复用，owner 和 storage name 不变，显示名可以改且不进入路径。管理库不复制文件树或有效 quota。

## 身份与权限

本地管理员由离线命令建立/重设，密码保存 Argon2id hash。browser cookie 为 Secure、HttpOnly、SameSite Strict；cookie mutation 检查配置的 Origin 和独立 CSRF token，GET auth/session 供 reload 恢复当前 session/token。CLI 使用自己的有期限 credential，secret 保存在项目私有文件，服务仅保存 verifier；轮换保留用户和 credential identity。

文件/mount/transfer 入口先认证并核对实际 immutable owner，再激活 runtime。管理员访问自己的文件同样经过 owner 检查，额外权限只有普通用户列表及五项 volume 摘要。summary 目标必须是 ordinary user，检查在目标 runtime 打开前完成；DTO 不含他人 volume ID、quota revision、文件 API 或文件事实。

Handler 的可选 AccessBinding 从可信 context 取得 opaque SubjectID/AccessID，附着到现有 file session/reference、locking session/enrollment/grant 与 volatile terminal history，不建立第二份 registry。同 owner 的不同 access 不能继承这些状态，credential 轮换保持 access。所有调用仍执行 Authorizer；持续输出检查当前身份，logout/revoke/到期停止其输出。replication cursor 只是历史 selector，新的有效同 owner 入口可以 resubscribe，输出继续按当前身份授权。

durable management/quota/import action 归稳定 subject 和固定 action key；同 subject 的新有效 credential 可以查询或续作原责任，不能继承旧 volatile capability。普通用户外部登录尚未接入，fresh 服务没有普通用户注册入口。

## 管理 API 和 Console

管理 prefix 为 `/api/service/v1`，身份响应 no-store。own-volume 入口提供 GET/POST volumes、GET/PATCH/DELETE volumes/{id} 和 PUT volumes/{id}/quota；auth 提供 login/logout/session 与自身 CLI credential 管理；action-namespaces/operations 提供原动作查询；admin/users 及 admin/users/{id}/volumes 仅提供只读 ordinary-user 信息。GET capabilities 返回实际文件 session/transfer 限制。

管理计数、字节和 revision 使用 decimal strings。管理员 volume 行只有 name/files/directories/used/quota 或固定安全 error，分页使用 actor/target 绑定的 opaque cursor。统计不可用不能返回零或旧缓存；quota CAS 使用 native quota revision，rename/delete 使用 management revision。

Console 页面为 Login、自己的 Volumes/Files 和管理员 Users。文件选择保留原始叶名字节和 NodeID，显示转义不进入请求 identity；raw leaf 使用 canonical padded standard base64。v5 numeric JSON 经 patched lossless codec 精确解析/序列化，大整数不经过 JS Number。注销或切换身份清空私有页面状态。文件页没有预览或在线编辑器。

## 原生统计、配额和文件传输

VolumeSummary 在同一 healthy snapshot 返回 named file/directory counters、allocation Used、effective quota 和 revision。regular 与 symlink 各计一个文件，目录不计 root；脱名节点不进入个数，仍被引用的 allocation 进入 Used。strong-lock recovery 允许健康统计和读取，冲突 grant/mutation 继续受保护。

服务显式选择 durable quota，首次初始化后 reopen 读取原 policy；构造器不能覆盖它。SetQuota 的策略、revision 和 exact receipt 在原 publication transaction 提交；quota 下调到 Used 以下允许，后续增长受 EDQUOT 约束。embedded/reference fixed allowance 保持既有模式。

import 接收固定 child condition、长度、digest 和 action。未发布内容不具名；sealed 后 Publish 原子创建或 replace-name 为新 regular node，提交 allocation/counters/log/receipt。被选 symlink 本身可替换，不跟随 target；目录拒绝，旧引用保留旧节点。下载同时 pin node 与 exact immutable key，完整验证 private spool 后才发送成功内容；旧历史 key 不重复计入 quota Used。

transfer 与原文件 handler 共用授权、scope/share/strong guard、commit/barrier/publisher。file-transfer 提供 begin/export，file-transfer-control 提供 query/resume/publish/cancel/detach/cancel-action/close-export；data 和 verified export 使用 bound token。sealed import 可以 Detach 后重启续作；unsealed work 的稳定 action cancellation 不继承旧 receive handle。未知 native result/cleanup 保留责任，不能误报成功。

## 生命周期与正常重启

管理 operation 保存固定输入与原 key，区分 pending/running/succeeded/failed。accepted 不表示 completed；丢响应查询同一个动作，未知结果不换新 key 重执行。原生 quota/import receipt 裁定实际结果，management 保存 intent 和已观察状态。

创建先保存 owner 和意图，初始化唯一根并验证 binding/父目录持久结果后 active。runtime 按稳定 ID 去重，cold summary 与已有 data runtime 共用同一 authority。永久删除在同一 admission gate 封住新工作，核对 live pins、Handler/locking retirement 和原生 CheckDeletionReady；Busy 在 Stop 前撤销未提交预留，mount 继续可用。pending durable import/action 阻止删除，terminal receipt 不长期 pin runtime；普通 Close 可以移交 sealed import 供 reopen，不是删除证明。

确认 close 后，delete 使用绑定根、operation proof 和 quarantine 回收；部分清理继续由原动作持有，未知路径禁止删除。正常停止/异常退出后按原管理状态和 native receipt 续作，缺失 SQLite/Witness/lease evidence 不初始化为空 volume。Service.Stop 关闭新 admission，Close 按原 owner 排空并保留错误；原有文件锁恢复保证继续成立。

配置字段为 listen、external_origin、state_root、trusted_proxy_cidrs、runtime_idle_timeout 和 shutdown_timeout。存储、文件 session、传输和管理状态保留各自既有有限上限；client 使用 capabilities。health/live 表示进程存活，health/ready 在停止或协调工作未解决失败时报告不可接受工作。配置错误、独占冲突和存储故障明确失败。
