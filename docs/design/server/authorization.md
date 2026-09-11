# 业务方提供的操作授权

本文描述 HTTP handler 的 volume 操作授权，对应 R-INT-7、R-INT-2、R-INT-3、R-SEC-4 至 R-SEC-6。跨角色的访问与错误约定见[顶层设计](../architecture.md)，取舍见[授权决定](../../../.agents/notes/implemented/feature/2026-09-10-host-provided-authorization.md)。

## 一、组件与配置

[`packages/authz`](../../../packages/authz/authz.go) 定义 `Authorizer`、`AuthorizerFunc`、`AccessRequest`、`Operation` 和 `ErrDenied`，并复用 [`storage.OpenAccess`](../../../packages/storage/files.go) 表达打开意图。它依赖基础 storage 类型，不依赖具体存储或 HTTP。HTTP handler 从严格解码的动作构造 AccessRequest；嵌入业务负责认证、身份 context、当前策略与审计。

```go
type Authorizer interface {
    Authorize(context.Context, AccessRequest) error
}

type AccessRequest struct {
    Volume    string
    Operation Operation
    Open      storage.OpenAccess
}
```

[`HandlerOptions`](../../../packages/transport/httprest/limits.go) 的 Authorizer 与 Volume 成对配置。两者都缺省时没有库内策略检查；非空 volume 配 nil Authorizer、非 nil Authorizer 配空 volume，以及 interface 内含 typed nil 或 nil AuthorizerFunc，都在构造时拒绝。volume 是不透明名字，不 trim、不从 URL、body 或 backend 推断。业务方将该可信名字与 backing、change log 和锁服务正确配对。

Authorizer 用业务自己的 context key 取得稳定访问身份，读取当前策略并同步返回。它必须支持并发、响应取消，不重入同一 handler。库不缓存策略、不重试 callback、不创建每次调用的后台 goroutine，也不记录原始策略错误。认证、用户、角色、token 签发／验证／刷新和 TLS 由嵌入方管理；独立二进制没有认证配置入口。

授权对象是整个 volume 的语义操作。路径 ACL、逐节点过滤和部分目录／快照视图不在这个接口内；Session、File、Owner 和 Grant 保持 bearer capability，使用它们的每个请求仍需授权。

## 二、操作映射

Operation 是 transport-neutral 字符串。[Operation 常量](../../../packages/authz/operations.go)在文件请求中直接作为 JSON `op` 的值，wire dispatch 与 AccessRequest.Operation 使用同一标识；普通 volume、复制与强锁的 URL 保留传输名称，在路由 opSpec 中登记对应语义。下表是这些入口的完整对应，file 行第二项是 body 的规范操作值。普通、bounded 与 confirmation barrier 变体使用同一语义，HTTP 方法本身不划分读写权限。业务策略对未知操作默认拒绝。

| Operation | HTTP wire 入口／动作 | 被授权的语义 |
|---|---|---|
| `volume.stat` | `/v3/stat` | 读取路径属性 |
| `volume.list` | `/v3/list` | 列出目录，含 bounded listing |
| `volume.read` | `/v3/read` | 读取完整文件，含 bounded read |
| `volume.space` | `/v3/space` | 查询容量 |
| `volume.set-attr` | `/v3/setattr` | 修改路径属性 |
| `volume.write` | `/v3/write` | 创建缺失的文件或替换已有路径内容 |
| `volume.create` | `/v3/create` | 创建文件 |
| `volume.mkdir` | `/v3/mkdir` | 创建目录 |
| `volume.remove` | `/v3/remove` | 删除文件名字 |
| `volume.remove-dir` | `/v3/removedir` | 删除目录 |
| `volume.rename` | `/v3/rename` | 在同一 volume 内改名或覆盖目标 |
| `replication.subscribe` | `/v3/subscribe` | 开始订阅及该订阅后续输出 |
| `replication.resubscribe` | `/v3/resubscribe` | 按游标续订及其后续输出 |
| `replication.snapshot` | `/v3/snapshot` | 捕获快照及发送整份快照 |
| `file.session-open` | `/v3/file`，`file.session-open` | 建立 FileSession |
| `file.status` | `/v3/file-control`，`file.status` | 查询 FileSession 状态与历史边界 |
| `file.renew` | `/v3/file-control`，`file.renew` | 续期 FileSession |
| `file.session-close` | `/v3/file-control`，`file.session-close` | 关闭 FileSession |
| `file.stat-node` | `/v3/file`，`file.stat-node` | 按节点身份读取属性 |
| `file.set-node-attr` | `/v3/file`，`file.set-node-attr` | 按节点身份修改属性 |
| `file.open` | `/v3/file`，`file.open` | 按路径打开，携带 OpenAccess |
| `file.open-node` | `/v3/file`，`file.open-node` | 按节点身份打开，携带 OpenAccess |
| `file.ack` | `/v3/file-control`，`file.ack` | 确认已交付的打开引用 |
| `file.stat` | `/v3/file`，`file.stat` | 查询保留引用的属性 |
| `file.read` | `/v3/file`，`file.read` | 按保留引用读取范围 |
| `file.write` | `/v3/file`，`file.write` | 按保留引用修改范围 |
| `file.truncate` | `/v3/file`，`file.truncate` | 改变保留文件长度 |
| `file.set-attr` | `/v3/file`，`file.set-attr` | 修改保留文件属性 |
| `file.sync` | `/v3/file`，`file.sync` | 同步检查已发布文件状态 |
| `file.get-lock` | `/v3/file-control`，`file.get-lock` | 查询 advisory 冲突 |
| `file.set-lock` | `/v3/file-control`，`file.set-lock` | 申请或转换共享／排他 advisory 锁 |
| `file.unlock` | `/v3/file-control`，`file.unlock` | 显式解除 advisory 锁或范围 |
| `file.query-lock` | `/v3/file-control`，`file.query-lock` | 核对 advisory 请求结果 |
| `file.cancel-lock` | `/v3/file-control`，`file.cancel-lock` | 取消／核对原 advisory 请求 |
| `file.drop-locks` | `/v3/file-control`，`file.drop-locks` | 清理指定 owner／family 的锁 |
| `file.close` | `/v3/file-control`，`file.close` | 关闭一个保留文件引用 |
| `lock.session-enrollment` | `/v3/session-enrollment` | 申请强占有会话 enrollment ticket |
| `lock.session-open` | `/v3/session-open` | 使用 ticket 建立强占有会话 |
| `lock.session-close` | `/v3/session-close` | 关闭强占有会话 |
| `lock.owner-create` | `/v3/owner-create` | 建立强占有 owner |
| `lock.owner-retire` | `/v3/owner-retire` | 退役强占有 owner |
| `lock.resolve` | `/v3/lock-resolve` | 取得逻辑资源引用 |
| `lock.acquire` | `/v3/lock-acquire` | 申请强 S／X 占有 |
| `lock.renew` | `/v3/lock-renew` | 续期强占有 |
| `lock.release` | `/v3/lock-release` | 释放强占有 |
| `lock.cancel` | `/v3/lock-cancel` | 取消／核对强占有动作 |
| `lock.query-action` | `/v3/lock-query-action` | 查询动作历史 |
| `lock.query-grant` | `/v3/lock-query-grant` | 查询 grant 当前状态 |
| `lock.status` | `/v3/lock-status` | 查询授权方状态 |


`FileOpenOptions` 嵌入共享的 `storage.OpenAccess`，其 Read、Write、Create、Truncate、Exclusive 与 AccessRequest.Open 是同一类型。Open 的合法性仍由 FileOpenOptions.Check／CheckNode 连同 mode、节点身份验证。AccessRequest.Open 仅在 file.open／file.open-node 携带这份已验证的值，其它操作为零值；open-node 不接受 Create／Exclusive。带 Create 的打开即使最终打开已有文件，也报告创建意图。一次入口 callback 同时决定全部打开意图，允许之后才创建、截断或分配文件引用。

`volume.write` 可以创建缺失文件，单独拒绝 volume.create 不能禁止创建。file.set-lock 表达共享／排他申请与转换；file.unlock 是独立的 wire 操作与策略操作，必须携带 Unlock 类型；file.set-lock 不能携带 Unlock。EX flock 可用于只读 fd，锁模式不代替内容写权限；后续内容修改仍检查 file.write 等操作。

## 三、请求、capability 与关闭

请求按以下顺序进入受控操作：

1. 业务 middleware 验证凭据，将稳定身份放入 request context。
2. handler 验证协议、方法、动作、大小与参数。通用的有界请求／响应 admission 可在策略前限制调用和错误响应占用，也可先返回传输层 EAGAIN；该层不查询受控状态。
3. handler 从已解码语义构造一个 AccessRequest，执行一次入口 Authorize。
4. 允许后才读取或触碰 capability、分配引用或动作、重放回执、读取 Log、取得订阅／snapshot／page 资源或访问 backend。存储权限、配额、锁与原有取消分类继续生效。

格式合法但不存在的 capability、无 Log（包括 publisher 为 nil）或错误续订游标，在拒绝时只返回授权错误；允许后才返回原有 ESTALE／ENOSYS 等结果。旧 RequestID、成功过的动作与已有 capability 均不能省略检查。ack、renew、status、history、cancel、unlock、release、retire 与 close 各自可被拒绝。

拒绝只说明本次尝试未获准，不能证明此前超时的动作未执行。client 无法获准核对时保留原来的未知结果，不合成 Cancelled、Released、NotApplied 或已记录的 rejection。请求 cleanup 被拒绝仍报告拒绝；服务器自主 lease 到期、退休、shutdown 与资源回收由原拥有者执行，不重新请求该访问身份的权限。

授权 context 派生自请求，保留业务值，并由 handler lifetime 取消。逐请求通过 `context.AfterFunc` 注册取消并在结束时解除注册；callback 前后检查原请求／handler 生命周期。普通 volume／file 请求在可证明尚未 dispatch 时接受调用方取消为 EINTR，deadline 和未知结果为 EIO。强锁 wire 协议没有 EINTR code，服务端原有生命周期或非 native 服务故障仍返回 native Unavailable／EIO、recorded=false 且无 action receipt；它与策略拒绝的普通 EACCES／EIO envelope 分开。这不改变 SDK 在本地发送前或只读阶段接受取消的 EINTR。handler 停止沿关闭路径；原请求仍有效时，策略自己的超时为授权 EIO。

Stop 非阻塞地发出取消／停止通知。嵌入方使用 `Stop → HTTP Shutdown／ServeHTTP drain → Handler.Close → backend teardown` 顺序：drain 等待已通知取消的 callback、stream、producer、snapshot、admission 和 watcher 清理；Stop 返回不表示排空完成。handler 的 registry 清理和 backend 所有权仍遵守[文件会话生命周期](file-handles.md#五http复制与资源)。

## 四、持续输出与撤权

启用 Authorizer 时，subscribe、resubscribe、snapshot 的入口 callback 与拒绝响应占用现有通用 response admission；允许后先释放该名额，再访问 Log、订阅或 snapshot 资源。未配置 hook 时不增加这项 stream 入口占用，既有容量参数不变。入口授权先于 Log 可用性判断、捕获和受控资源准入。每个 start／rebuild、change、snapshot open／rows／done 与 keepalive 在发送前再次调用相同 Operation 和 AccessRequest，使用同一业务身份 context 查询当前策略。游标只选择历史；快照授权覆盖整份捕获视图。

初始允许后可进行现有的有界、producer 拥有的 snapshot 预取；frame admission 与 Snap.Next 响应派生 context 的取消。每页出站前仍重新检查。拒绝或策略故障使排队页面、保活和成功 done 停止发送，只尝试一次固定 terminal fault，不为 fault 再调用策略。取消 producer、等待其退出、释放排队页面预留后，由原拥有者关闭 snapshot 和释放订阅资源。违反取消契约且仍使用 snapshot 的 backend 不能被并发强制关闭、遗弃或报告清理成功。

一个 write 通过本次发送检查后可以完成。仅更新策略不打断已准入且阻塞的写入；下一次检查才观察到变化。业务 context 取消或 handler Stop 使用同一 watcher 中断阻塞写，watcher 在初始 Flush 前建立；terminal fault 也先启动既有 100 毫秒 departureGrace，随后以 deadline 打断慢 peer。健康 stream 没有新增默认写超时、策略轮询或 token 生命周期 goroutine。

已经交付的字节、元数据与客户端／内核缓存不可撤回。下一次检查或 context 取消被观察前，已有副本仍可能回答同步过的元数据；外部策略更新与 storage commit、socket write 没有原子顺序。每份 mount／replica 固定属于同一访问身份：同一身份轮换凭据可保持连接，切换身份须建立新的 mount／replica 和 capability 状态。

## 五、错误与 wire

`errors.Is(err, authz.ErrDenied)` 是业务方明确拒绝，即使标记被包装或 join；无法决定时业务方返回不含该标记的错误。适配层保留本地原错误链，但网络只使用内部授权错误的可信字段。

| callback 结果 | errno | 固定消息 |
|---|---|---|
| nil | 允许 | 无 |
| ErrDenied | EACCES | `access denied` |
| 其它非 nil | EIO | `authorization failed` |

[内部授权错误](../../../packages/transport/httprest/authorization.go)的 Error() 返回固定文本，Classification() 拥有可信 errno，Unwrap() 保留原 cause。原错误不直接进入普通、file、lock 或 stream writer；writer 不沿策略错误链重新分类或发送其文本。原请求／handler 取消优先于 callback 分类；这一适配不改写普通存储错误或未知动作结果。

未开始 stream 时，授权错误使用 v3 协议标记、422 和普通 `{errno,message}`。强锁 decoder 遇到 lockCode 或 recorded 任一字段，都必须校验完整 native envelope；字段缺失、损坏不转入普通分支。只有精确的普通 errno/message 结构及 EACCES／EIO 才是 dispatch 前的授权失败，header、标记和大小限制仍须满足。授权错误不携带伪造的 native code、recorded 或动作回执。

stream fault 保留 message 并增加可选 errno，只接受 EACCES／EIO；缺省 errno 的 generic fault 仍为 EIO，null、空值、畸形或未知 errno，以及重复的 errno／message 字段，都是协议 EIO。该字段沿用 v3。直接 HTTP SDK 在所有初始 frame 与 Next 包装处保留有效 typed EACCES／EIO；replica 观察 follower 失败后仍统一以 EIO 使本地视图不可用。经失效副本回答的 FUSE 元数据错误因此可为 EIO，不能伪造空目录或不存在。

## 六、嵌入示例与观测

以下类型与函数均属于业务代码。authenticate 由宿主验证凭据并返回稳定身份；lookup 读取该身份对目标 volume 的当前策略。它们不是库提供的认证模块。

```go
package host

import (
    "context"
    "net/http"

    "github.com/codetreker/remote-fs/packages/authz"
    "github.com/codetreker/remote-fs/packages/metastore"
    "github.com/codetreker/remote-fs/packages/storage"
    "github.com/codetreker/remote-fs/packages/transport/httprest"
)

type identityKey struct{}

type permission struct {
    Allowed  map[authz.Operation]bool
    CanRead  bool
    CanWrite bool
}

func newAuthorizedHandler(
    backing storage.Storage,
    log metastore.Log,
    volume string,
    authenticate func(*http.Request) (string, error),
    lookup func(context.Context, string, string) (permission, error),
) (*httprest.Handler, http.Handler, error) {
    policy := authz.AuthorizerFunc(func(ctx context.Context, r authz.AccessRequest) error {
        identity, ok := ctx.Value(identityKey{}).(string)
        if !ok { return authz.ErrDenied }
        access, err := lookup(ctx, identity, r.Volume)
        if err != nil { return err }
        if !access.Allowed[r.Operation] { return authz.ErrDenied }
        if r.Operation == authz.FileOpen || r.Operation == authz.FileOpenNode {
            if r.Open.Read && !access.CanRead { return authz.ErrDenied }
            if (r.Open.Write || r.Open.Create || r.Open.Truncate) && !access.CanWrite {
                return authz.ErrDenied
            }
        }
        return nil
    })
    options := httprest.DefaultHandlerOptions()
    options.Volume, options.Authorizer = volume, policy
    handler, err := httprest.NewHandlerWithOptions(backing, log, options)
    if err != nil { return nil, nil, err }

    authenticated := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        identity, err := authenticate(r)
        if err != nil {
            http.Error(w, "authentication failed", http.StatusUnauthorized)
            return
        }
        ctx := context.WithValue(r.Context(), identityKey{}, identity)
        handler.ServeHTTP(w, r.WithContext(ctx))
    })
    mux := http.NewServeMux()
    mux.Handle("/storage/", http.StripPrefix("/storage", authenticated))
    return handler, mux, nil
}
```

业务策略分别列出允许的语义操作，并对 Open 检查 CanRead／CanWrite；只读角色可以显式允许它需要的查询、订阅、只读打开、锁与清理。返回的 handler 由宿主按第三节关闭，mux 由宿主 HTTP server 使用。不同 volume 使用各自配好的 handler，这个示例不提供实例注册表。

host 包装 Authorizer 记录策略版本、耗时、允许／拒绝／故障，使用请求 context 中自己的 request／trace ID 关联 `component=authz`、`phase=authorize`、volume、operation 和 decision。同一长连接的重查保留同一关联值；可通过宿主现有查询 `request_id=X component=authz` 核对请求与输出结果。身份脱敏、凭据、策略错误审计和资源预算属于业务方；库不增加日志目的地、exporter、查询栈或脱离请求的审计任务。

可执行的断言分层与故障用例见[测试策略](../../testing.md)。
