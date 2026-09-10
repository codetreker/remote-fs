# Agent Note: 由嵌入方提供 volume 操作授权

Status: proposed

## 问题

一个 volume 可以被同一业务中的不同访问身份使用，但业务方才知道用户、凭据、角色和当前权限。仅在 HTTP 方法或路径上拦截不足以表达文件打开的读写意图，也容易漏掉 SSE、保留文件引用、锁动作历史和清理入口。将用户体系放进文件系统则会重复业务方已有的认证与凭据生命周期。

[规格](../../../../docs/spec/requirements.md)的 R-INT-7、R-SEC-5、R-SEC-6 要求认证留在业务方，操作授权可替换，并明确持续输出与撤权的边界。本提案只设计这一入口；[多 volume 管理](../architecture/2026-08-19-volume-in-the-contract.md)继续拥有实例注册、选择与生命周期，保持其独立范围。

## 提案

### 范围和责任

嵌入方验证凭据，用自己的 context key 保存稳定访问身份，并提供读取当前策略的 Authorizer。库只提供 volume、语义操作和必要的打开意图，不定义 Principal、用户数据库、角色、凭据格式、token 签发或刷新协议。

授权范围是整个 volume，不包含路径 ACL、单个节点过滤或部分目录视图。一个 handler 对应业务方配置的可信 volume；不能从请求路径、客户端字段或 backend 名字推断它。已有 Session、File、Owner、Grant 仍是 bearer capability，不新增身份绑定；每次带 capability 的请求照样经过授权。

不增加独立二进制的认证配置或客户端凭据模块。原有外层认证 middleware、业务 mux 和自定义 HTTP client 继续可用；不同访问身份不得复用同一个 mount／replica 及其缓存与 capability。同一身份的凭据轮换可保持连接，切换身份需要新的客户端状态。

### 最小的中性 API

拟新增 `packages/authz`，只依赖标准库，公开以下值类型与函数类型：

```go
package authz

import ("context"; "errors")

type Operation string

type OpenAccess struct {
    Read      bool
    Write     bool
    Create    bool
    Truncate  bool
    Exclusive bool
}

type AccessRequest struct {
    Volume    string
    Operation Operation
    Open      OpenAccess
}

type Authorizer interface {
    Authorize(context.Context, AccessRequest) error
}

type AuthorizerFunc func(context.Context, AccessRequest) error

func (f AuthorizerFunc) Authorize(ctx context.Context, request AccessRequest) error {
    return f(ctx, request)
}

var ErrDenied = errors.New("access denied")
```

`Operation` 是下表中具体语义的稳定字符串，常量由 authz 包拥有，不导入 HTTP 或 storage 类型。角色归组由业务策略决定。未知操作不得从 HTTP 方法猜测；请求解码先拒绝未识别的动作，业务策略也应默认拒绝未列出的操作。

`Open` 只在 `file.open`、`file.open-node` 有效，其余操作为零值。它是合法打开参数的值拷贝，保留读、写、可能创建、截断与排他创建意图；不传原始 wire 对象。`open-node` 不接受创建意图。即使目标已经存在，带 Create 的打开仍报告 Create=true，策略决定是否允许该意图。

### handler 的配置

`httprest.HandlerOptions` 拟增加 `Authorizer authz.Authorizer` 与 `Volume string`。两者都未设置时，不添加库内授权检查；这保留现有嵌入方式和外层 middleware。启用时两者必须同时提供：nil callback 配非空 Volume，或非 nil callback 配空 Volume，构造都拒绝。typed-nil Authorizer 或 nil AuthorizerFunc 同样拒绝。Volume 是不透明名字，不自行 trim 或生成默认值；嵌入方负责使它与该 handler 的 backing 对应。

每次请求只有一次入口授权回调。复合打开的全部意图由同一个 AccessRequest 表达，在创建、截断、打开引用、动作记录或 backend 调用之前完成决定；库不拆成多次 read/write 检查，也不替业务方组合角色。

Authorizer 必须可并发调用、响应 context 取消，不重入同一 handler。库同步调用，不为超时遗留后台回调、不自动重试、不自动记录 callback 的错误文本。业务方负责策略读取、缓存、审计和自身服务的时延预算。

### 操作表

名称与传输方法分离；同一语义不因 GET／POST、普通／bounded 入口或 confirmation barrier 的有无而改变。以下字符串各有独立常量，不能合并成角色名。file 入口的第二项是请求 body 的动作字段；普通、bounded 和 barrier 变体共享该行映射。

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
| `file.session-open` | `/v3/file`，`new` | 建立 FileSession |
| `file.status` | `/v3/file-control`，`status` | 查询 FileSession 状态与历史边界 |
| `file.renew` | `/v3/file-control`，`renew` | 续期 FileSession |
| `file.session-close` | `/v3/file-control`，`session-close` | 关闭 FileSession |
| `file.stat-node` | `/v3/file`，`stat-node` | 按节点身份读取属性 |
| `file.set-node-attr` | `/v3/file`，`set-node-attr` | 按节点身份修改属性 |
| `file.open` | `/v3/file`，`open` | 按路径打开，携带 OpenAccess |
| `file.open-node` | `/v3/file`，`open-node` | 按节点身份打开，携带 OpenAccess |
| `file.ack` | `/v3/file-control`，`ack` | 确认已交付的打开引用 |
| `file.stat` | `/v3/file`，`stat` | 查询保留引用的属性 |
| `file.read` | `/v3/file`，`read` | 按保留引用读取范围 |
| `file.write` | `/v3/file`，`write` | 按保留引用修改范围 |
| `file.truncate` | `/v3/file`，`truncate` | 改变保留文件长度 |
| `file.set-attr` | `/v3/file`，`set-attr` | 修改保留文件属性 |
| `file.sync` | `/v3/file`，`sync` | 同步检查已发布文件状态 |
| `file.get-lock` | `/v3/file-control`，`get-lock` | 查询 advisory 冲突 |
| `file.set-lock` | `/v3/file-control`，`set-lock`（共享／排他） | 申请或转换共享／排他 advisory 锁 |
| `file.unlock` | `/v3/file-control`，`set-lock`（解锁） | 显式解除 advisory 锁或范围 |
| `file.query-lock` | `/v3/file-control`，`query-lock` | 核对 advisory 请求结果 |
| `file.cancel-lock` | `/v3/file-control`，`cancel-lock` | 取消／核对原 advisory 请求 |
| `file.drop-locks` | `/v3/file-control`，`drop-locks` | 清理指定 owner／family 的锁 |
| `file.close` | `/v3/file-control`，`close` | 关闭一个保留文件引用 |
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

`volume.write` 本身允许创建缺失文件，因此单独拒绝 `volume.create` 不能禁止创建；业务策略必须同时考虑所有带创建语义的操作。wire 的 set-lock 请求若明确解锁，映射为 `file.unlock`，不能归到申请权限。`FileSession`、advisory 和强占有是不同协议，不能因名称相近合并控制权限。EX 锁不等同于内容写权限：只读描述符可以合法取得排他 flock。后续写入仍检查 `file.write` 等操作；库不从锁模式推导数据权限。角色需要更细分的锁策略时另行设计，不把本范围伪装成逐路径或逐模式 ACL。

### 请求的执行次序

1. 业务 middleware 完成认证并建立身份 context，再调用已配置 volume 的 handler。
2. 保留原协议标记、请求大小、方法／动作和参数有效性检查，以及通用的有界请求／响应 admission。后者约束策略调用与错误响应占用，可在授权前返回传输层 EAGAIN；它不读取受控状态。非法请求不调用 backend，也不以任意字符串触发策略操作。
3. 从已解码语义构造一个 AccessRequest。`file.open` 的所有合法旗标同时进入 OpenAccess；`file.open-node` 的禁止旗标由原检查拒绝。
4. 在 capability 查询／触碰／分配、动作预留、Log 读取（包括 incarnation／barrier）、订阅／快照名额准入和快照捕获之前调用 Authorizer。拒绝不登记动作、不续期、不释放，也不读取受控内容。
5. 允许后沿原实现执行。格式合法但不存在的 capability、不可用的 Log 或无效续订游标，此时才返回原有 ESTALE 或其它协议／存储错误；被拒绝者只得到授权错误。存储权限、配额、锁、bounded admission、取消和错误判断仍成立，授权成功不覆盖这些条件。

已经在外部 middleware 认证的请求不能绕开库内已启用的授权。业务 mux 可以把不同地址交给不同 handler，但 Volume 必须来自该 handler 的可信配置。请求不能用路径或 body 中的另一名字改写它。

授权 context 派生自请求，保留业务身份与 trace 值，同时受 handler 生命周期取消控制。handler Stop 保持非阻塞，发出取消／停止通知后返回。嵌入方按 Stop → HTTP Shutdown／ServeHTTP drain → Handler.Close → backend teardown 的既有顺序关闭；drain 包括初始授权回调、stream 写入、producer、snapshot 与 admission 释放和 watcher 清理，Stop 返回本身不代表这些工作已结束。可用一个 handler lifetime context，逐请求通过 `context.AfterFunc` 注册 requestCancel，并在请求结束时解除注册；不为每次检查创建 goroutine。回调前与 dispatch 前都核对原请求／handler 生命周期：可证明的新请求取消沿用 EINTR，deadline 或结果不确定沿用 EIO，handler Stop 沿用关闭路径。请求生命周期仍有效时，策略服务自己的超时属于授权故障。

callback 的结果约束这次准入。允许后发生的策略撤销不回滚已经开始的 mutation，也不把存储提交与外部策略数据库绑进同一个事务。文件打开中的 create／truncate 必须在同一次准入决定之后；不得先执行其中一步再询问权限。

### 动作重放与清理

每个 ack、renew、status、history query、cancel、release、close 都单独检查。旧 capability、历史 RequestID 或已成功过的相同请求不能省略检查。被拒绝的重复请求不执行，也不查询并泄露已有回执。

授权拒绝只证明本次尝试没有通过入口，不能证明此前超时的 acquire、open 或 mutation 未生效。client 在被拒绝核对时保留原来的未知结果；不能合成 Cancelled、Released、NotApplied 或一份新的已记录 rejection。

调用方请求的 cleanup 被拒绝时仍返回拒绝，不设“所有 close 一律绕过授权”的特例。已有 lease 到期、服务器内部退休、shutdown 与资源回收按原拥有者继续执行，不重新代表该调用方请求权限；否则撤权会阻断必要回收。业务策略可明确允许持有者的清理操作，但这是业务选择。

### 持续输出与撤权

subscribe、resubscribe、snapshot 在读取初始 Log 或捕获资源前各自检查对应 Operation；`publisher == nil`／Log 不可用的 ENOSYS 分支也在授权成功后执行，拒绝者只得到授权错误。每次发送 start／rebuild、change、snapshot open／rows／done 和 keepalive 之前，再用同一个 AccessRequest 与原业务身份 context 查询当前策略。续订游标只选择历史，不能免除新检查；snapshot 的授权范围仍是整份捕获视图，不做逐行 ACL。

每个 stream 内顺序检查、顺序发送。入口检查不缓存一份永久 allow；reauthorization 使用原 Operation，不把后续 frame 伪装成新的客户端动作。正常请求只有一次入口回调，持续输出另有这些逐单元检查。

初始授权与 snapshot 准入成功后，允许现有 producer 拥有并受上限约束的预取；每个页面仍须在出站前检查。frame admission 与 Snap.Next 必须按依赖契约响应派生 context 的取消。权限拒绝或 callback 故障后抑制队列中的页面、保活和成功 done，只尝试一次固定内容的 terminal fault。取消 producer，等待其退出，再由原拥有者释放排队页面预留、关闭 snapshot、释放订阅名额，随后结束 stream。检查针对初始受控资源捕获和实际出站单元，不要求每条内部行生产前再次回调，也不允许无上限或无拥有者的预取。若 backend 违反取消契约仍在使用 snapshot，不能并发强制关闭、遗弃工作或报告清理成功。fault 不携带受控内容，不再次调用授权而形成递归。已经写出的字节无法收回；已通过本次发送检查的有界单元可以完成。外部策略变化与存储提交或 socket write 不具有原子顺序。

单纯更新策略不会打断已经准入且阻塞的 write；它在下一次授权检查时才被观察。host 取消身份 context 或 handler Stop 才能打断当前阻塞写。沿用现有写 deadline 机制，将触发源从 handler stopping 扩展为 stopping 或请求 context 取消，并在初始 openStream Flush 前安装。terminal fault 发送前启动同一结束机制，最多使用现有 `departureGrace`（100 毫秒）尝试通知，再以 deadline 打断阻塞写；不能先阻塞发送错误，再启动取消。正常健康 stream 不增加默认超时，也不增加轮询策略或 token 生命周期 goroutine。

下一次发送或 host context 取消才暴露撤权；此前客户端可能继续读取已经同步的元数据。撤权不是删除客户端、内核缓存或已交付文件内容的机制。host 必须把一个 mount／replica 固定给同一访问身份；同一身份换凭据不要求重建，换身份需要新的 mount／replica 与 capability 状态。

### 错误与协议

Authorizer 返回 nil 表示允许。`errors.Is(err, authz.ErrDenied)` 表示业务方已经作出拒绝决定；其余非 nil 表示无法完成授权，不能当作允许。ErrDenied 是业务方明确选择的分类，即使包装或 join 了其它错误仍表示拒绝；业务方若无法决定，必须返回不含该标记的故障。

| 来源 | 本地错误分类 | 对外固定内容 |
|---|---|---|
| 明确 ErrDenied | EACCES，包装并保留原 error 链 | `access denied` |
| 其它 callback 错误 | EIO，包装并保留原 error 链 | `authorization failed` |
| 原请求／handler 生命周期取消及其它非策略故障 | 保留原阶段分类 | 保留既有协议规则 |

先按请求／handler 生命周期判定取消，再分类仍有效请求中的 callback 结果。回调与调用方取消同时完成时，沿用原阶段的取消分类；可证明尚未 dispatch 的调用方取消继续为 EINTR，deadline、已 dispatch mutation 的不确定结果继续为 EIO。策略自身超时在请求仍有效时为授权 EIO。被拒绝的 reconcile 不能把此前未知结果改成未发生。

适配层构造内部授权错误，只持有可信的 EACCES／EIO 与固定消息；其 `Error()` 返回固定文本，`Unwrap()` 保留原 callback cause 供本地检查。原 callback error 不直接交给普通 HTTP、file、lock 或 stream writer。wire writer 读取该内部错误的可信字段，不重新沿其底层错误链分类，也不序列化底层 `.Error()`；库不自动记录原错误文本。

HTTP 未开始 stream 时，使用原协议响应标记、422 状态和普通 `{errno,message}` envelope；仅发送上表固定消息。强锁 client 先检查字段存在性：出现 `lockCode` 或 `recorded` 任意一个字段，就必须走完整 native envelope 校验，字段残缺或损坏不得 fallback。只有精确的普通 `{errno,message}` 结构且 errno 为 EACCES／EIO，才表示 dispatch 前的授权错误；原协议标记、header 和 body bounds 仍须通过。只有真实 locking authority 结果可携带 native lockCode、recorded 或动作回执，不能为授权错误捏造它们。

stream 已开始时，扩展现有 StreamFault：保留 message，增加可选 errno。授权 fault 只使用 `EACCES` 或 `EIO` 和固定消息；没有 errno 的旧 generic fault 保持 EIO，未知或畸形 errno 为协议 EIO。该可选字段沿用 v3，不引入认证消息或新 token 协议。

直接 HTTP SDK 在所有初始帧解析及 Next 包装处保留 typed EACCES／EIO，不能再次包成 generic unreachable 而丢掉分类。已有 replica 的 usable() 在观察到 follower 故障后统一返回 EIO，这一 taxonomy 不改变；因此直接 SDK 的授权拒绝可以是 EACCES，经失效元数据副本读取的 FUSE 操作仍可为 EIO。两者都不得用空目录或不存在替代失败。

### 业务接入示例

下面的身份 key、认证器和权限表属于业务代码；库不认识这些类型。业务 mux 为每个 handler 绑定可信 volume，外层 middleware 把同一身份放入 request context。

```go
type identityKey struct{}

policy := authz.AuthorizerFunc(func(ctx context.Context, r authz.AccessRequest) error {
    identity, ok := ctx.Value(identityKey{}).(string)
    if !ok { return authz.ErrDenied }
    permission, err := permissions.Lookup(ctx, identity, r.Volume)
    if err != nil { return err }
    if !permission.AllowsOperation(r.Operation) { return authz.ErrDenied }
    if r.Operation == authz.FileOpen || r.Operation == authz.FileOpenNode {
        if r.Open.Read && !permission.CanRead { return authz.ErrDenied }
        if (r.Open.Write || r.Open.Create || r.Open.Truncate) && !permission.CanWrite {
            return authz.ErrDenied
        }
    }
    return nil
})
options := httprest.DefaultHandlerOptions()
options.Volume = "project-data"
options.Authorizer = policy
handler, err := httprest.NewHandlerWithOptions(backing, log, options)
if err != nil { return err }
mux.Handle("/storage/", http.StripPrefix("/storage", authenticate(handler)))
```

操作常量名按表中语义导出，例如 FileOpen、FileOpenNode；示例中的权限查询是 host 自己的依赖。只读业务角色可显式允许 stat/list/read/space、订阅、只读 Open、状态和它需要的 lock／cleanup 操作；写角色再显式允许相应修改。不能把每个 POST 都当写权限，也不能因 file.open 字符串相同忽略其 Create／Truncate。Exclusive 仅描述排他创建条件，不单独成为内容写权限；合法参数的 Create 意图仍要被策略看到。

### 当前实现接点

这些现有入口承载拟议改动；授权功能仍由本提案定义。

| 当前源码 | 实现责任 |
|---|---|
| [server.go](../../../../packages/transport/httprest/server.go) | Handler、NewHandlerWithOptions 与普通请求 dispatch／错误输出 |
| [file_server.go](../../../../packages/transport/httprest/file_server.go) | file 数据／控制动作、capability registry 与清理 |
| [lock_client.go](../../../../packages/transport/httprest/lock_client.go) | 普通授权错误与 native lock envelope 的严格区分 |
| [publish.go](../../../../packages/transport/httprest/publish.go) | 初始 Log／snapshot 获取、逐单元输出与 producer 生命周期 |
| [stream.go](../../../../packages/transport/httprest/stream.go) | StreamFault、endWritesWhen 与既有 departureGrace |
| [subscribe.go](../../../../packages/transport/httprest/subscribe.go) | 初始 frame 和 Next 的错误分类保留 |

### 观测与资源责任

host 包装 Authorizer 记录允许／拒绝／故障、策略版本和时延，并决定身份如何脱敏。库不引入日志目的地、指标 exporter 或审计存储，不记录凭据、capability 或 callback 原错误。固定 wire 消息不包含用户名、规则文本或策略服务地址。

业务 middleware 将 request／trace ID 放入自身 context，Authorizer 包装器记录 `component=authz`、`phase=authorize`、request_id、volume、operation、decision；同一长连接的再次检查保留相同关联值。验收时用 host 已有审计查询 `request_id=X component=authz`，将允许、拒绝、故障与受控请求和长连接输出对应起来，核对字段完整、上下文连续且没有敏感错误文本。库不增加日志查询栈，不依赖回调次数推断内部发送阶段，也不脱离请求生命周期执行审计工作。

请求解析与网络 admission 仍受现有上限约束；授权不扩大单体、并发或 stream 名额。callback 是嵌入方代码，库不能安全终止一个忽略 context 的函数；host 必须落实它自身的截止时间与资源上限。服务关闭和取消 watcher 必须 join，不能让每次策略检查留下后台任务。

## 备选方案

**只使用外层认证 middleware。** 它仍是合法接入方式，也可以独立执行授权；但通用 HTTP method/path 检查不能直接表达 Open 意图、锁动作与流内发送边界。可选 handler hook 让业务方复用同一中性策略，外层 middleware 继续负责认证。

**在文件系统中提供用户、角色和 token 管理。** 可以统一部署入口，却会接管业务身份、签发与策略生命周期。本系统只承接授权决定，不建立这套控制面。

**只在建立 stream 时授权。** 成本较低，但允许旧连接在权限改变后继续收到新数据。逐有界发送单元检查当前策略，代价是回调次数与策略服务依赖；已经交付的数据仍不可撤回。

**把 capability 绑定到库定义的 Principal。** 可增加按用户区分的能力归属，却要求定义身份稳定性、切换、序列化与跨协议比较。本范围保留 bearer 语义和逐请求检查；业务方管理访问身份及客户端状态隔离。

## 验收标准

实现验证须使用真实 handler／client／副本和受控 policy，不增加 mock 存储捷径；逐包覆盖与现有错误、race、资源清理门禁保持。

- API：Authorizer interface 与函数适配器可用；两字段同时缺省保持原行为，单独配置、typed-nil 和 nil 函数拒绝；Volume 不从请求或 backend 推断。
- 操作映射：逐个覆盖表中操作，所有普通／bounded／barrier 入口映射一致；file session、强会话、owner、status、history、renew、ack 与 cleanup 没有遗漏。
- 打开：业务示例分别核对 CanRead 与 CanWrite；只读、只写、读写、create、truncate、exclusive 组合保留全部合法意图；一次 callback，拒绝时无 Create、Truncate、引用或动作记录副作用；open-node 禁止的旗标仍拒绝。
- 锁：只读 fd 的 EX flock 不被库错误归为写内容；申请／转换与显式 unlock 的 Operation 不同；drop-locks、release、retire、close 分别可被拒绝。
- 失败：direct／wrapped ErrDenied 为 EACCES；其它错误为 EIO；本地保留原链，所有 writer 只收到固定 Error() 和可信分类，wire 与自动日志不泄漏 callback 文本。ErrDenied 加入 join 仍是 host 明确拒绝，无法决定的 host 故障不能携带此标记。
- 原有取消：Stop 发出取消后立即返回，HTTP Shutdown／drain 等待普通请求与 stream 的回调、producer 和清理结束，之后才 Close handler 与 backend；取消与 callback 返回竞争仍沿用原阶段分类。dispatch 前可证明的取消保持 EINTR；deadline／未知 mutation 保持 EIO，单独策略超时为授权 EIO。拒绝重复或核对请求不修改此前未知结果，不合成 native lockCode／recorded 回执。
- 强锁 wire：带协议标记的 422 普通 EACCES／EIO 可解码；native envelope 仍严格校验；缺标记、坏字段、未知 errno 和错误结构返回协议 EIO，不进行 fallback 猜测。
- 准入：通用请求／响应容量耗尽可在授权前返回 EAGAIN；通过该层后，拒绝不触碰 capability、publisher／订阅／snapshot／page 的受控状态。格式合法的未知 capability、无 Log（含 publisher 为 nil）和错误续订游标在拒绝时不查询 backend；允许后保留原 ESTALE／ENOSYS 等错误。拒绝 volume.create 而允许 volume.write 时，后者仍可按原语义创建文件。
- stream：subscribe/resubscribe/snapshot 在初始 Log／资源访问前检查，start/rebuild、change、open/rows/done、keepalive 前逐次检查；拒绝后不发送排队数据、保活或成功 done。新游标与旧 capability 均无绕过路径。
- 时序：暂停在 allow 与发送之间再撤权，验证已准入单元可完成、下一单元被拒绝；不能把这个结果表述为原子撤权。已经交付的 metadata 不会被假装回收。
- 背压：peer 不读取时，host context 取消或 Stop 必须打断包括初始 Flush 在内的阻塞写；在下一次检查注入拒绝／policy 故障，terminal fault 须在既有结束预算下退出。仅更新策略不承诺打断已准入 write。验证已拥有的有界预取页面在拒绝后不出站；可取消的 frame admission／Snap.Next 退出后 producer 被等待，排队页面预留、连接、snapshot、订阅名额与 watcher 释放。违反取消契约的 backend 不能被强制关闭或被当作清理成功。
- SDK：初始 frame 和所有 Next 位置的授权 fault 保留 EACCES；旧 generic fault、畸形或未知 errno 仍 EIO。replica 观察故障后 unusable，FUSE 元数据错误仍 EIO，不能继续伪造可信结果。
- 生命周期：拒绝 session-close 等请求会保留真实拒绝结果；随后服务器自主 lease 到期和 shutdown 仍回收。库测试验证 context 与 stream 连续性；host 集成验证同一身份轮换凭据不打断既有 stream，切换身份建立新客户端状态。

## 风险

策略读取进入每次请求和发送路径，host 必须控制其耗时、并发与可用性。一个忽略 context 的 Authorizer 可以拖住调用；库不能靠遗留 goroutine 安全修复业务代码。外部策略更新与已准入操作没有事务关系。

持有 capability 不免授权，但它仍可被获准的另一身份使用；本提案不提供 Principal 绑定。没有 hook 时依赖已有外层控制，不能误称为默认强制授权。host 必须正确绑定 volume 和稳定身份，并隔离不同身份的 mount／replica。

撤权只能阻止后续服务端准入，不能擦除已交付内容或缓存。流内检查与安全 fault 会使合作客户端停止使用失效副本；它不对恶意客户端提供数据撤回。更细路径权限、即时全局撤权及独立二进制认证配置均不由本提案承担。中性 API 可以供其它传输使用；本提案只实现 HTTP 接点，后续传输仍须满足 R-SEC-5／R-SEC-6，不能成为绕过路径。
