# server 角色

volume、保留文件与显式占有的权威持有者。将原生 storage 与它的锁服务配对，经 HTTP 暴露给多个 client。

本文只写 server 内部。角色边界与跨角色契约的分工见 [`../architecture.md`](../architecture.md)。

## 一、内部构成

| 组件 | 职责 | 需求 |
|---|---|---|
| **请求处理**（`packages/transport/httprest`） | 一个 `http.Handler`。解析数据与锁控制请求，使用独立的有界 admission，验证 scope 与响应，把控制操作交给配对的授权方。它不缓存 volume 答案；文件 registry、订阅与快照保留各自有界的状态。 | R-INT-1、R-INT-3 |
| **业务授权**（`packages/authz` 与 handler adapter） | 把可信 volume、语义操作与完整 Open 意图交给嵌入方策略；请求入口和流出站分别检查，原生占有检查保持独立。 | R-INT-7、R-SEC-4 至 R-SEC-6 |
| **协议词汇**（`packages/transport/httprest`） | 请求 URL 的形状、响应体的形状；错误的名字取自 storage 契约的 errno 词汇。与 client 共用同一份。 | R-INT-9 |
| **变更日志** | volume 里每一次改动的有序记录，由 storage 底下的 metastore 提供。请求处理拿到它就开出复制那三个操作；拿不到（`nil`）时，在已启用的操作授权通过后以 `ENOSYS` 拒绝它们。 | R-CON-1、R-CON-2 |
| **storage** | 原生发布集成确定实际资源并执行最终转换。localstore 与 Azure 组合在 metastore 事务中记账；第三方实现须履行同一原生集成契约。 | R-INT-6、R-INT-13 |
| **保留文件与 advisory** | FileSession 拥有当前对象引用，原生节点保留无名内容；独立 advisory coordinator 管理 flock/POSIX owner 与范围。 | R-FS-6 至 R-FS-8、R-CC-12、R-CC-13、R-WS-7 |
| **文件占有**（`packages/locking`、`packages/storage/locked`） | 有限 S/X 授予、Session / Owner、动作核对与发布顺序；与同一 volume 绑定，重启通过持久证据恢复保护。 | R-CC-3、R-CC-6 至 R-CC-11 |

```
    HTTP 请求
        │
        ▼
 ┌─────────────┐
 │  请求处理    │  packages/transport/httprest
 └──────┬──────┘
        │ storage 接口
        ▼
 ┌─────────────┐
 │   storage   │  localstore / objectstore / 自有实现
 └─────────────┘
```

基础数据操作各自完成一次请求；FileSession 保留对象与标准 advisory 状态，显式 S/X 另有 Session、Owner、grant 与动作历史。handler 只接受 `locked.New` 验证过的配对 backend，其 `LockService()` 就是绑定原生发布检查的授权方，不能从另一份 storage 单独提供控制服务。控制状态、恢复与拒绝规则见[文件锁设计](file-locks.md)。可选 Authorizer 先按业务身份和语义决定准入，再访问 capability、Log 与 backend；通用有界请求／响应容量可先返回 EAGAIN。操作映射、策略错误与 context 生命周期由[业务授权](authorization.md)定义。

## 二、请求的形状

基础 volume 操作是 URL 路径的最后一段，操作数在 query string 里。保留文件与控制操作使用 JSON 请求体。

| 操作 | 方法与路径 | 操作数 | 请求体 |
|---|---|---|---|
| `Stat` | `GET /v3/stat` | `path` | — |
| `SetAttr` | `POST /v3/setattr` | `path` | 要改的属性，JSON |
| `List` | `GET /v3/list` | `path` | — |
| `Read` | `GET /v3/read` | `path` | — |
| `Write` | `POST /v3/write` | `path` | 文件内容 |
| `Create` | `POST /v3/create` | `path` | — |
| `Mkdir` | `POST /v3/mkdir` | `path` | — |
| `Remove` | `POST /v3/remove` | `path` | — |
| `RemoveDir` | `POST /v3/removedir` | `path` | — |
| `Rename` | `POST /v3/rename` | `path`（源）、`to` | — |
| `Space` | `GET /v3/space` | 无 | — |
| `Subscribe` | `GET /v3/subscribe` | 无 | — |
| `Resubscribe` | `GET /v3/resubscribe` | `incarnation`、`position` | — |
| `Snapshot` | `GET /v3/snapshot` | 无 | — |
| 保留文件数据 | `POST /v3/file` | 无 | 严格 JSON，op 使用规范的 file.* 操作值，携带 session/file 能力与参数 |
| 文件会话与 advisory 控制 | `POST /v3/file-control` | 无 | 严格 JSON，op 使用规范的 file.* 操作值，携带原动作身份及 owner |

volume 路径以 `url.Values` 的转义走 query string，任意字节序列都逐字往返。根是 `path=`：一个存在且为空的操作数。`Space` 描述整个 volume 而不是某个路径底下的东西，因此它一个操作数都不带；带了 `path=` 的 `Space` 请求与多带了任何操作数的请求一样，是请求错误。

请求处理**逐字转交**收到的路径，不做清洗、不做越界判定。路径规则由 storage 接口的 `CleanPath` 定义，只有一份。

解析是严格的：query 解析不了、操作数缺失、同一个操作数出现两次、出现了这个操作不要的操作数，都是请求错误。这几种情况在 `url.Values` 里读出来都是空字符串，而空字符串是根。

`Prefix` 为 `/v3/`，相对于 handler 被挂载的位置，挂到别处用 `http.StripPrefix`。v3 在既有 mutation barrier JSON 之外要求配对的锁授权方，并增加严格的控制消息与 mutation scope。server 不提供旧协议路由，client 同时验证路径版本与响应标记；旧服务端不能通过忽略 proof 接受受保护的修改。十三个 JSON POST 控制端点与 scope 编码见[文件锁协议](file-locks.md#http-v3-编码)，它们不把 capability 放入 query string。保留文件能力、Open 确认、动作历史、会话续期与 advisory 控制见[打开的文件](file-handles.md#五http复制与资源)。

## 三、响应的形状

每个响应带两个头：

- `Remote-Fs-Protocol: 3` —— 标记这是本协议的服务端给出的答案。
- `Cache-Control: no-store` —— 一个被缓存住的答案读起来与当前的答案没有区别。

| 情形 | 状态 | 体 |
|---|---|---|
| `Stat` 成功 | `200` | `{"attr":{…}}` |
| `List` 成功 | `200` | `{"entries":[…]}`，空目录是 `[]`，绝不是 `null` |
| `Read` 成功 | `200` | 文件内容，`Content-Type: application/octet-stream` |
| `Space` 成功 | `200` | `{"space":{"total":…,"used":…,"avail":…}}` |
| `SetAttr`、`Write`、`Create`、`Mkdir`、`Remove`、`RemoveDir`、`Rename` 成功 | `200` | metastore-backed volume 是 `{"barrier":{"incarnation":"…","position":…}}`；没有 change log 时是 `{}` |
| storage 报错，handler 在调用 storage 前以 `EFBIG`/`EAGAIN` 权威拒绝，或 mutation 成功后无法读取 replication barrier | `422` | `{"errno":"<名字>","message":"…"}`；最后一种固定为 `EIO` |
| 锁控制或最终 scope 检查拒绝 | `422` | 在 errno/message 之外有 `lockCode`、`recorded`；已记录的动作拒绝还返回 action receipt |
| `SetAttr` 请求体超过 `MaxBodyBytes` | `413` | `{"message":"…"}` |
| 操作不存在 | `404` | `{"message":"…"}` |
| 方法不对 | `405` | `{"message":"…"}` |
| 请求解析不了、请求体不完整 | `400` | `{"message":"…"}` |
| 复制那三个操作成功 | `200` | 一串 server-sent event，`Content-Type: text/event-stream`（见第五节） |

handler 用 `MaxBodyBytes` 限制基础 volume 的 non-write 请求与 non-streaming 响应，用 `MaxWriteBytes` 单独限制 `Write` 的文件内容；package options 中后者为零时继承前者，显式值必须为正且不大于前者。过大的 `Write` 在调用 storage 之前以 `EFBIG` 返回，过大的 `SetAttr` body 是 `413`。协议定义为 bodyless 的操作只探测是否出现第一个字节，任何非空 body 都是 `400`，不会按声明长度保留内容。过大的 `Read` 是 `EFBIG`；无法在 `MaxBodyBytes` 内编码的 `List` 是 `EIO`。错误响应也必须装进 `MaxBodyBytes`，过长的 detail 会换成固定诊断，协议 marker 与 errno 不丢失。锁控制单独使用固定 16 KiB body 与独立 operation / waiter admission，编码与预算见[锁协议](file-locks.md#控制准入与取消)。

`200` 是唯一的成功状态，`422` 是唯一承载 errno 的状态。它表示 server 已经得到一个可命名的操作结果：来自 storage、来自调用 storage 前就能权威判定的 `EFBIG`/`EAGAIN` 资源拒绝，或来自已经成功的 mutation 之后无法取得 barrier 的 `EIO`。最后一种不能退回成功：volume 已改变，但 server 无法给 replicated client 一条证明副本何时包含它的界线。errno 不编码进状态码 —— 状态码空间与 errno 空间不同构。

**每个 non-streaming 响应都显式声明 `Content-Length`，失败响应也不例外。** 这是顶层设计第四节「响应体的分帧必须能报告自己提前结束」那条义务在这一侧的落地：一个以连接关闭为终点的响应体，被截断与完整无从分辨，因此不被接受。SSE response 不声明整条 stream 的长度，它靠每个 frame 的明确边界与终止语义区分完整和截断。

**缺席与零值必须分辨得开。** `Stat` 响应里的 `attr`、以及每个目录条目里的 `attr`，都是可以在报文里缺席的字段，而缺席就是解码失败 —— 一个零值的属性读起来是「一个模式为 0、长度为 0、时间停在 1970 年的普通文件」，与一份合法的答案分辨不开。`entries` 同理：`null` 与 `[]` 相差两个字符，意思相反。mutation response 必须是一个只允许可选 `barrier` 字段的 JSON object：没有 change log 的 volume 返回 `{}`；有 log 时 barrier 必须存在，且带非空 incarnation 与非负 position，位置 0 也是合法的初始 barrier。incarnation 的 protocol ceiling 是 256 bytes；handler 再按 `MaxBodyBytes` 与 `MaxFrameBytes` 的 worst-case JSON escaping 计算同一份更紧预算，stream start 与 mutation barrier 都用它，保证合法 identity 在两条路径上一致且一定装得进各自边界。`null`、缺字段、未知字段或错误类型都不是成功答案；普通 remote storage 接受无 barrier 的 `{}`，replicated client 使用的 `*WithBarrier` 方法必须取得并验证它。

容量报告里的三个数各自也是可缺席的字段，而这里的理由更硬：它们是字节数，零对每一个都是合法答案 —— 一份什么都没装的 volume 已用为零，一份装满的可写入量为零 —— 所以一旦当成普通字段读进来，缺席与零就再也分不开，而一份掉了字段的报告读起来恰好是「没有剩余空间」，足以让每一次写入停下。解码还要判定这三个数能不能同时为真，判不成立同样是解码失败：它们最终要进内核回复的无符号字段，在那里一个负数是一个巨大的正数（R-ERR-2）。

目录条目的名字以原始字节编码（JSON 里是 base64）。文件名是任意字节序列，不是文本。

`Attr` 的 `id` 是节点身份（R-FS-5）：一个不透明的无符号整数，只可比较相等，随节点走过改名。**它是这里唯一一个零值不响的字段**，因此由解码拒绝：模式为 0 是合法答案、纪元时刻也是有人设得出来的值，所以别处的拒绝针对的是整个 `attr` 缺席；而身份为 0 在挂载点那边每次比较都相等，于是一个不发这个字段的对端不会被读成「什么都没说」，会被读成「所有节点都是同一个节点」。

`Attr` 的 `mode` 是 Go `io/fs.FileMode` 的位布局。两个时间 —— `access_time` 与 `mod_time` —— 各是一个对象，`unix_sec` 是自 Unix 纪元起的整秒数，`nanos` 是该秒之内的纳秒数。单独一个纳秒数装不下这两个字段要承载的范围 —— `time.Time.UnixNano` 只在 1678-09-21 到 2262-04-11 之间有定义，范围之外的时间（零值的 `time.Time` 也在其中）会变成另一个看上去完全合理的日期，且没有任何东西标出它是错的。秒与纳秒合成一个对象而不是并排两个字段，是因为 `SetAttr` 的请求里每个时间都可以整个缺席，而两个各自可空的字段能互相矛盾。

`SetAttr` 的请求体是 `{"change":{…}}`，`change` 里每个属性都是可选的：缺席就是「这一项不改」。`change` 本身缺席则是解码失败 —— 一个什么都不点名的改动是合法请求（它在问这个节点还在不在），因此靠字段本身分辨不出报文是不是掉了内容，外面这一层对象才分辨得出来。

保留文件的响应 envelope 按操作携带 FileSession 状态、引用能力、属性、字节、advisory 结果与可选 barrier，不能套用基础 mutation 的空 object 规则。Data 与 Path 使用 base64 字节字段；时间间隔以整数纳秒编码。Open 先返回有期限的待确认能力，client 完成确认才交给调用方；未确认引用与关闭的动作记录受 registry 上限约束。完整形状与核对边界见[文件协议](file-handles.md#五http复制与资源)。

## 四、错误如何离开 server

`packages/storage` 持有一张 errno 与符号名之间的双向表，它同时是一个 storage 实现允许报出的 errno 的全集。它归契约而不归某一种传输：一个实现可以报出哪些错误，是契约的性质。

storage 返回错误时，请求处理用 `storage.ErrnoNameOf` 取得 `422` 响应里的符号名。它与 FUSE 共用 `storage.ErrnoOf` 的错误树分类：已接受的纯取消为 `EINTR`，deadline 与未知错误为 `EIO`，词汇表内的已命名错误保留。当前节点的 `Classification() error` 对其子树具有权威性；独立故障分支不会被深层取消覆盖，join 顺序不改变这一点。普通存储错误的 `message` 保留原文本；策略错误使用内部可信 errno 与固定消息，Unwrap 仅供本地保留 cause，不能把 callback 文本交给 wire writer。没有失败对象可编码时 `ErrnoNameOf(nil)` 仍为 `EIO`。

锁控制的 typed code 与 `recorded` 额外区分冲突、未接纳、退役与结果未知，不能只用 errno 推断 Acquire 是否曾经成功。管理动作一旦 dispatch，取消或丢失响应不证明它未执行；Resolve、QueryAction、QueryGrant 与 Status 虽用 POST，仍按只读取消处理。身份能力不进入 message。授权失败以普通 422 EACCES／EIO 返回，不制造 native lockCode／recorded；decoder 只要看到任一 native 字段就要求完整 native envelope，详见[授权错误](authorization.md#五错误与-wire)。

**无法命名的失败一律是 `EIO`。** 挑一个最接近的名字，等于把一个不确定的失败说成一个确定的事实；而 `ENOENT` 一旦被这样说出去，上层会据以删除、重新生成或覆盖（R-ERR-1、R-ERR-2）。

`Space` 的 `ENOSYS` 走的也是这条路。它是关于那个 volume 的答案 —— 它没有自己的容量可报 —— 而不是本协议缺了一块，因此和其它 errno 一样以 `422` 带着自己的名字回去，不用 `501`。

## 五、复制那三个操作

一个 volume 的元数据能不能被复制，取决于集成方是否提供变更日志。随附的 localstore 与 Azure 形态都有日志；库调用方未提供时，三个操作在已启用的授权通过后返回 `ENOSYS` —— 那是关于那份 volume 的一句事实，与「够不到」是两回事，两者要求的动作正好相反：`ENOSYS` 说这里永远不会有副本，别再问了；`EIO` 说过一会儿再试。**绝不能答一条空的流或一份没有行的快照** —— 那读起来是「这个 volume 存在、是空的、永不改变」，而这正是一个副本会相信的答案。

| 操作 | 答什么 |
|---|---|
| `Subscribe` | 从日志当前的尾位置起，把此后每一条变更推给这个订阅者。要建副本的 client 先做这一步。 |
| `Resubscribe` | 带着（化身，位置）回来：化身对不上、或者那个位置已经掉出保留窗口，答「必须重建」并说明是从哪个维度掉出去的；否则从那个位置之后接着推。 |
| `Snapshot` | 一次一致性切割：所有行反映同一个瞬间，并带回那个瞬间的位置。分页送。 |

两点是这三个操作的形状所依赖的：

**先订阅、后取快照。** 反过来不收敛 —— 扫描耗时乘以变更速率超过保留窗口，快照的位置就已经掉出窗口，于是从头再来，而重建代价正比于树的大小。订阅在前之后，保留窗口不在首次同步的关键路径上，它只伺候断线重连。

**这三个流与请求／响应天生在不同的连接上。** 快照是系统里最大的一次批量传输；它若与事件挤在一条连接上，就会挤掉喂着副本的那条流，后果是重新拉一份快照 —— 一个自我放大的循环，而触发它只需要一次正常的冷挂载。走 SSE（`text/event-stream`）因此不需要额外机制：每个流是一次独立的 HTTP 请求。义务写成性质而不是拓扑 —— 从一次变更被记入日志，到它的事件抵达一个健康订阅者，其耗时与并发的批量传输无关 —— 将来的其它传输各自说明它用什么机制满足它。

**流的失败没有状态码可用。** 状态与响应头在第一帧之前就发走了，因此此后出的错以一个 `fault` 帧代替本该跟在后面的一切。generic fault 与断流都使副本失效；授权 fault 的可选 errno 还让直接 SDK 保留 EACCES／EIO。缺省 errno 仍为 EIO，null、畸形或未知值作为协议 EIO。每个数据、控制和 keepalive 出站前重新检查当前策略，拒绝后的固定 fault 不再次授权，不发送后续页面或成功 done，详见[持续输出](authorization.md#四持续输出与撤权)。

server 为这几个操作持有的资源都有上限：同时开着的订阅与快照数、一份快照最长可以送多久、单个 encoded frame、跨页保留的 snapshot cursor bytes、同时产生的 snapshot page 数与总 retained bytes、等待 snapshot-page admission 的 goroutine，以及一次读日志或快照最多处理多少行（R-INT-3）。change frame 的 aggregate 由 subscription 数与单帧预算共同给出，不与 snapshot bulk transfer 共用 gate。还有一个不是上限而是下限：**无话可说时多久也要说一句**——心跳的间隔。读的那一侧据此给「一个字节都没来」定上界，于是「流还活着」是被观测到的而不是被假定的；没有它，一条被切断的 TCP 与一个安静的 volume 是同一个观测结果。

`MaxFrameBytes` 不只在 JSON 已经生成后检查。`Log.Incarnation(ctx, maxBytes)` 在载入或复制 stream identity 前限制 UTF-8 bytes；`Log.Since` 把 payload lengths 交给 caller-owned `metastore.ChangeResult`，`Snap.Next` 使用 `metastore.RowResult`。producer 先以 fixed fields 与变长字段长度 `Reserve`，预算通过后才加载 name、source name 与 object key，并用 exact lengths `Commit`。一页已经有内容而下一项只是不够剩余空间时，该项留给下一页；单项本身装不进空 frame 时以 `EFBIG` 使整页失败。其它 production error 同样使 page 不可读取，snapshot 上的这类错误还终止该一致性切割。`EventPage` 与 `SnapshotPage` 继续限制一轮数据库工作量，不能代替 byte bound。`Log.Barrier(ctx, maxIncarnationBytes)` 则在 mutation 完成后原子给出同一份有界 identity 与 committed position。第三方 `metastore.Log` 与 `Snap` 也必须实现这些带预算的唯一入口；接口不保留会在内部建立 unbounded slice 的 count-only 变体。

每条 subscription 串行地产生 start/change frame，同一时刻至多保留一项，每项按 `3 * MaxFrameBytes` 覆盖 metastore result、wire conversion 与 encoded frame。它不等待共享 admission，因此一份 snapshot 的 bulk production 不能阻塞健康订阅者；默认最多 64 条 subscription 时，change/start 中间表示的 derived aggregate ceiling 是 `64 * 3 * 8 MiB = 1.5 GiB`。

snapshot page 另有共享 operation、aggregate byte 与 waiter admission，每份同样按 `3 * MaxFrameBytes` 预留；默认同时 16 页、aggregate 384 MiB、等待者 64 个。队列已满立即 `EAGAIN`；已经进入等待的 producer 服从 snapshot request context 与 `SnapshotDeadline`，取消或 deadline 会释放计数。等待期间 delivery loop 仍继续发送 keepalive，所以资源排队不会被 client 的 silence bound 误判为断流。每份打开的 SQLite snapshot 还在两页之间保留至多 `MaxFrameBytes` 的 name cursor；其 aggregate 从 `Snapshots * MaxFrameBytes` 推导，默认 `8 * 8 MiB = 64 MiB`，constructor 在乘法溢出时拒绝配置。这项 cursor bound 与 page admission 分开计量。

client 的 `DialOptions.MaxFrameBytes` 默认也是 8 MiB，逐 stream 限制 scanner 保留的一帧；client library 不拥有调用方打开多少条 stream，因此 stream cardinality 仍由调用方约束。

订阅默认最多 64 条。新订阅在保留唤醒 channel 与 stream state 前检查名额，已满时不排队，以 `EAGAIN` 拒绝；request cancellation、断连、失败或 `Handler.Stop` 结束 stream 时释放名额。快照默认最多 8 份；快照占着 storage 里的一个资源 —— 在 SQLite 上是一个读事务 —— 而它活多久由网络决定，所以已经开满时新的请求也以 `EAGAIN` 拒绝而不是排队：client 是先订阅再来要快照的，重试对它不花什么代价。

SQLite metastore 把普通 volume/log read 与长期 snapshot 放进两个 reader pool，避免慢 snapshot 占完普通操作与 event catch-up 能用的 connection。`MaxReaderConnections` 与 `MaxSnapshotReaderConnections` 默认各为 16；各自池满时读取等待 connection，并遵从对应 request/snapshot context 取消。snapshot 并发上限限制打开的读事务数，snapshot reader pool 限制数据库为它们持有的物理 connection 数，两者保持独立。普通读取只在启动 transaction 并以对 `database_state` 的常量查询钉住 SQLite snapshot 时持有 database health gate，page 扫描和 caller-owned result accounting 不继续占着 mutation commit/Accept 所需的 gate。

**订阅者是被唤醒的，不是被投喂的。** 每一次改动了 volume 的请求在答复之前唤醒所有订阅，被唤醒的订阅自己去读日志。于是「追上」与「跟上」是同一条代码路径，不可能对「一条变更是什么」有两种说法；也没有任何一处等待间隔（R-CON-2）。mutation 成功后，handler 再用 `Log.Barrier` 在 mutation response 的 incarnation budget 下原子读取 log incarnation 与 committed position；并发 mutation 可以让 position 更晚，但同一事务记录本次修改保证它不会更早。barrier 读取失败发生在 volume 已改变之后，以 `EIO` 返回且不伪装成未修改。两种随附 server 都通过数据库原生 EX 所有权维持单一活跃写入方；另一个绕过该所有权的 server 无法提供保留文件能力。detached 文件修改不产生路径日志，但文件操作的成功 response 仍可读取当前 barrier，确认不依赖虚构一个名字。

## 六、请求与响应的内存边界

`Write` 与 `SetAttr` 的请求体必须先完整到齐，才允许落到 storage。提前结束的文件内容不能被当作一次成功的前缀替换；只到一部分的属性改动也不能被当作完整请求。

handler 在读取 body 前取得 request operation 与 byte admission。`Write` 预留 `MaxWriteBytes`，`SetAttr` 预留 `MaxBodyBytes`；缺失或虚假的 `Content-Length` 不能换成较小 reservation。bodyless 操作已声明非空 body 时立即拒绝；长度未知时取得 operation slot，最多读一个探测字节。等待 admission 的请求服从 request context；等待者已满时新请求以 `EAGAIN` 失败。

HTTP handler 只接受实现 `storage.BoundedStorage` 的 volume。constructor 在服务请求前调用 `CheckBounded`；任一依赖无法在实际产生结果时接受预算，启动就直接失败。`ReadBounded` 在完整 payload 分配前知道 byte bound，超限以 `EFBIG` 失败。`ListBounded` 按顺序把 entry 加入调用方的 `ListResult`；`ListResult` 用 HTTP 表示的精确逐条 charge 计数，并在保留那条超限 entry 之前以 `EIO` 失败。backend 不得先建立完整的超限 `[]byte` 或 `[]Entry` 中间结果。

`Read` 与 `List` 的 wire format 仍是一份 non-streaming response，在发出前保留一份配置内的完整结果。每个非流式操作在调用 storage 前都取得 response operation 与 byte admission；`Read` 与 `List` 按 `4 * MaxBodyBytes` 预留，覆盖 storage entries、wire conversion、encoded body 与它们同时存在的保守峰值，其余固定结果操作按 `MaxBodyBytes` 预留，使得同时产生的最大错误 JSON 也在 aggregate 上限内。response 等待者已满时，`Stat`、`Write`、`Create` 等操作也会在到达 storage 前以 `EAGAIN` 失败；`Write` 与 `SetAttr` 还不会读取 request body。context cancellation 会退出等待并释放计数。

启用业务授权时，stream 入口的 callback 与拒绝响应也取得通用 response admission；允许后释放，再取得 Log、订阅与 snapshot 资源，长连接不持续占用该名额。未配置 hook 的 stream 不增加这项占用。授权和生命周期顺序见[业务授权](authorization.md)。

默认资源上限为：

| 资源 | 默认值 |
|---|---:|
| 单个 protocol body / file-content write | 1 GiB / 继承 protocol body |
| 同时保留的 request bodies / 等待者 | 64 / 64 |
| request-body aggregate bytes | 2 GiB |
| 同时保留的 responses / 等待者 | 64 / 64 |
| response aggregate bytes | 8 GiB |

`MaxBodyBytes` 至少为 1024 字节，且必须小到可以计算四倍 response reservation；`MaxWriteBytes` 必须为正且不大于 `MaxBodyBytes`。request aggregate 至少容纳一份 `MaxBodyBytes`，response aggregate 至少容纳一份四倍 reservation。effective operation 与 waiter 上限都为正；option 的零值选择有界默认值。client 对所有 non-streaming operation 持有独立的 response operation、waiter 与 aggregate byte admission，见 [`../client/architecture.md`](../client/architecture.md)。

内容替换的原子性由 storage 保证（R-CON-3），请求处理不参与。

## 七、storage 的位置

server 通过配对的 volume 与锁服务访问 volume。集成方注入具有原生发布能力的实现（R-INT-6），或选择随附形态：

| 形态 | 组成 | 变更日志 |
|---|---|---|
| 本地持久对象存储 | `packages/storage/localstore` 持有 `objectstore.Storage`、`localdisk.Objects`、绑定的 `sqlite.Store` 与外部提交见证 | 有 |
| Azure 对象存储 | `objectstore.Storage` + `azblob.Objects` + `sqlite.Store` | 有 |

Azure 形态依赖部署方分别提供和运维 Blob container、数据库及其相邻的 lease 证据，两类存储可以各自失败（R-INT-12、R-ERR-6）。`sqlite.OpenLocking` 对整份数据库取得 lifetime ownership，数据库及确定位置的证据保存数据库级最大 lease 时长。后续启动可以选择另一个已有 volume，但同一时刻只有一份活跃锁服务拥有该数据库，恢复等待仍覆盖整份数据库。`localstore` 则拥有一个私有本地目录下的对象、SQLite、WAL 外部见证、恢复状态与独占锁；它的完整设计见 [`local-disk-object-store.md`](local-disk-object-store.md)。

基础 storage 的十一个操作、路径规则与错误词汇见顶层设计第四节。`FileStorage` 的保留对象与 advisory 是独立能力，原生 EX 所有权、共享预算、retained-file schema 和最终释放见[文件句柄设计](file-handles.md)。

### 配额住在 storage 这一侧

配额属于 volume，计数与拒绝落在 storage 这一层。随附的 localstore 与 Azure 组合在 metastore 事务中记账；库另提供通用 `limited` 包装，供没有自身配额的第三方 storage 使用。

**包一层：`packages/storage/limited`** 包住任意一份 `storage.BoundedStorage`（R-WS-5、R-INT-3），这是把配额加到一份本来没有配额概念的实现上的通用办法：

- 支持保留文件的 backend 以权威 `Usage` 报告已命名与 detached 的合计用量；只有路径 API 的 backend 在打开时遍历计量。遍历前先调用 `CheckBounded`，每个目录经 `ListBounded` 逐项计量；当前目录的 `Entry` 与名字、尚待访问的完整目录路径分别受独立 byte bound 约束，默认各为 64 MiB。目录预算在保留越界 entry 前拒绝，frontier 预算包含当前正在访问的目录；任一上限不足都以 `EIO` 使整次计量失败，不提交部分结果。HTTP body 上限不参与 volume measurement。
- 原生 backend 的 `CheckPublicationAccounting` 能力把实际新旧长度与效果交给发布计费。增长在效果发生前预留，超限以 `EDQUOT` 拒绝；确定 Applied 后才释放缩短额度，即使随后返回确认错误也按实际效果结算。NotApplied 退回增长预留；volume 效果不明，或结算、撤销带 `IsPublicationAccountingUncertain` 时保留保守账本并使 Space、修改与 Recount 报错，直到重新打开。scope、FileStorage 与锁服务传给同一原生 backend，最后一个 detached 引用的释放同样结算实际效果。原有验收由[缩短提交后释放配额](../../../.agents/notes/implemented/bug-fix/2026-09-07-release-shrunk-quota-after-commit.md)拥有。
- 让 volume 变小的修改从不被拒绝，已经超出配额时也不拒绝 —— 否则一个超额的 volume 没有任何回到配额之内的路。
- 配额不得低于一个 4096 字节的块：再小的配额会被报成一个零块的文件系统，那读起来是一块没有剩余空间的盘，而不是一个空间很小的 volume。
- 实现原生计费能力时，Write、Remove 与 Rename 从实际发布取得大小，跳过包装层路径采样与 stripe。没有该能力的有界第三方 backend 仍用路径采样；祖先目录改名可能使其计量对象失效，且它不能向 server 提供非空锁授权方，见[目录改名中的配额记账](../../../.agents/notes/proposed/bug-fix/2026-09-07-keep-quota-accounting-stable-across-directory-renames.md)。库的 `Recount` 使用同一组 measurement limits，并与自主引用回收的记账 revision 核对；并发变化可有界重数，耗尽尝试为 `EAGAIN`。超限、取消或 listing 失败保留原计数，已经不确定的账本不能靠在线重数解除隔离。随附二进制没有 recount 入口。

`Space` 报出的「还能写入的量」取配额剩余与底层实现所报之中较小的那个。配额是「还允许写多少」而不是「这些字节一定放得下」：底下那块盘比配额更紧时若仍报配额，等于向先查空间再决定写不写的程序许诺机器给不出的余量。

**自己记账：`packages/storage/objectstore`** 不需要被包起来。它的树存在一个数据库里，于是「已用多少」由每一次改变文件大小的 transaction 同时更新和校验，不存在「判定有余量」与「占用余量」之间的窗口。没有配置配额时 metastore 以 `ENOSYS` 拒绝 `Space`。

SQLite metastore 还给 reserved、unresolved 与 garbage object records 的合计数量和 payload bytes 配置独立阈值。一个 payload 自身超过 byte threshold 时 `Reserve` 返回 `EFBIG`；请求本身能装下、但现有 backlog 使新记录越界时返回 `EAGAIN`。`Put` 失败时 reservation 转成 unresolved；这类结果没有 ownership proof，不会因为时间经过而被删除。已经存在的 volume 修改仍可产生 garbage 并把 backlog 推到阈值之上，此时新 reservation 保持拒绝，garbage 清扫与删除继续运行。package 默认值与 `-max-pending-objects`、`-max-pending-bytes` 用于 Azure 和本地形态；本地组合还通过 `Config.ObjectLimits` 暴露覆盖值，见 [`local-disk-object-store.md`](local-disk-object-store.md#七容量与资源上限)。两种 metastore-backed 形态同样使用有界 SQLite reader pool；package 默认为 16，独立 server 以 `-max-reader-connections` 配置。

SQLite 打开与 `ObjectStatus` 验证每个 volume 的 named 节点形成 rooted tree：root 没有 incoming entry，其余 named 节点恰有一个同 volume 的名字且从 root 可达。detached 只能是非 root 的普通文件，不得有名字或参与目录边；其它孤儿、cycle 与跨 volume entry 以 `EIO` 拒绝。`volumes.used` 必须是非负整数，并等于对所有 regular-file size 做 overflow-checked streaming sum 的结果。SQLite 的动态 storage class 也属于完整性：文件名必须是非空 BLOB，标量字段保持声明的整数/文本/可空类型，detached 为 0 或 1，内容 revision 为正；change kind 与 nullable node/from groups、mode/size/time 范围必须彼此一致。否则 cursor order、NULL coercion 或 fabricated reconciliation 可以把损坏记录变成一次成功但缺行/零值的复制结果，因此都在开放 volume 或 history 前拒绝。

SQLite 的 `database_state` 另持有数据库 identity、提交 generation，以及 node ID 与全局 change position 的持久高水位。ID 从高水位显式分配，`sqlite_sequence` 是同事务推进的冗余记录。durable-state validation 通过 expression indexes 的类型 discriminator 与最大 identity 边界读取全数据库 surviving references，打开、checkpoint 与每个 `Since` page 都要求 sequence 一致且任一 volume 的引用不超过高水位；每次分配也重新核对 sequence，change append 还要求当前 committed tail 严格小于新位置。每条 retained change 另保存同 volume 的 `previous_position`：第一条指向 `trimmed_through`，相邻记录逐条相连，最后一条等于 `committed_position`。位置是全数据库分配的，volume 内允许被其它 volume 留下空洞，完整性因此检查前驱链而不检查算术连续。`Open`、`Snapshot` 与 `ObjectStatus` 在暴露 volume 前验证受 `MaxIntegrityRecords` 限制的完整链；`Since` 用索引锚定 page 起点并执行 O(page) predecessor validation，缺口所在页整体失败，stream error 使 consumer 作废副本。同一 incarnation/position 的续订会在该缺口持续失败，直到持久日志被带外修复或出现合法 rebuild boundary。断链、tail 不一致或高水位回退都不生成新 incarnation 掩盖损坏。

本地持久形态还在 SQLite WAL 外保存 `METASTORE`：每次成功 open 或 mutation 的 SQLite commit 先推进 generation，再原子发布完整 accepted state；调用在发布完成后才成功。durable writer 为每条物理 connection 启用 SQLite `PERSIST_WAL`；accepted state 尚未完成 witnessed checkpoint 时，异常关闭、`Abort` 或未确认 `Accept` 会留下 WAL 供重开对账。checkpoint 只有在全部 WAL frame 已进入主数据库且状态仍等于 accepted state 时才推进见证中的 checkpoint generation。accepted 比 checkpoint 新时，下一次打开必须在 SQLite 打开前看见非空 WAL；缺失、空或仅有 header 的 WAL 表示确认状态可能回退，以 `EIO` 拒绝。accepted-state 见证发布失败会 poison 当前数据库，checkpoint 见证失败由单个后台 worker 定期重试；正常关闭在完整 checkpoint 和见证同步之后才尝试清除 `PERSIST_WAL` 并关闭 writer。清除调用失败时 flag 状态未知，但 `A = C` 已使 WAL 不再是恢复证据；writer 与 lifetime ownership 保留并允许重试。

recursive reachability 之前先以 scalar aggregate 计算这次验证会触及的 volume seed、node、relevant entry、object、log 与 change records 总数；entry 的 label、parent 或 child 任一接触目标 volume 都计入，避免 corrupt cross-volume edge 藏在预算之外。`MaxIntegrityRecords` 默认 1,000,000，`MinIntegrityRecords` 为 3，恰可容纳一个 volume seed、空 root 与 mandatory log row；零值选择默认，低于 3 或 `math.MaxInt64` 在数据库打开前以 `EINVAL` 拒绝。预检超过上限时返回 `EFBIG` 并要求提高配置，不运行 recursive CTE；count/query 本身失败是 `EIO`。

full integrity pass 对 entry 与 retained-change name 执行内容检查前，还以 SQLite `length` 分批累计 variable bytes，不先 materialize BLOB。`MaxIntegrityBytes` 默认 64 MiB，必须是小于 `math.MaxInt64` 的正数；超过上限以 `EFBIG` 拒绝。它与 record count 分别限制“一共有多少关系”和“这些行携带多少变长名字”，不能互相替代。`Since` page 的 variable payload 由 caller-owned frame budget 限制，不计入 full-pass name-byte budget。

v1/v2 数据库还面临 object lifecycle 证据缺失：旧实现把 reservation size 记为零，也可以依据时间把 reservation 改成 garbage。只有全部 object row 都是可验证的 referenced 状态且上述完整性成立时才允许前滚迁移；任一 non-referenced row 或结构、计数、序列错误都使打开以 `EIO` 失败，整个 migration transaction 不提交。legacy preflight 对整个数据库计量 integrity work，不只检查这次请求打开的 volume。迁移从现有引用与 `sqlite_sequence` 建立 node/change 高水位；v2 retained log 没有 predecessor，迁移会清空旧 retained rows、切换 incarnation 并保留全局 change 高水位，使 replica 明确重建且新 position 不复用旧值。这类旧对象状态需要运维先证明归属并修复数据，新代码不会猜测字节数或授权删除。

`objectstore.Storage.Space` 还会询问 `Objects.Available`。外部 object service 只返回 `ENOSYS` 时，报告继续使用 metastore 的逻辑配额；local-disk objects 返回底层 filesystem 扣除维护与 in-flight reserve 后的物理可用量，最终 `Avail` 是逻辑余量与物理余量中较小的那个。任何实际 measurement error，包括与 `ENOSYS` 一起出现的 error，都使整个调用失败。

事务内计数不会因正常 API 流量漂移，因此 metastore-backed 形态没有 `Recount`。本地对象文件仍可能被存储外的主体删除或修改；格式、身份、长度与 checksum 校验会把它暴露为 `EIO`，不会把这条旁路解释成一次 volume mutation。

## 八、server 不做什么

- **不缓存。** volume 的事实就在 storage 里，变更的事实就在日志里。
- **不保存订阅者的复制进度。** 位置由订阅者自己携带。锁 Session、Owner、有限 grant 与动作历史由服务端持有，TCP 断开不会提前解除保护。
- **不清洗路径。** 逐字交给 storage。
- **身份与策略归业务方。** handler 的可选 Authorizer 提供语义操作准入；认证、角色、凭据与 TLS 仍由嵌入方管理，锁 capability 不代替业务访问控制。接入方式见[业务授权](authorization.md)。
- **handler package 不打印，也不记日志。** 普通请求失败随响应返回；策略错误只发送固定安全消息，原 cause 留在本地错误链，审计由业务方包装 callback 完成。独立二进制只把 lifecycle、锁状态与 metastore-backed status 写到 stderr。

## 九、部署形态

作为库嵌入集成方既有的 server，或作为独立二进制运行（R-INT-1、R-INT-4）。

作为库时：不注册信号处理、不写 stdout/stderr、不调用进程退出、包初始化不产生副作用（R-INT-2）。`packages/transport/httprest` 暴露 `NewHandler`、`NewHandlerWithLimits` 与 `NewHandlerWithOptions`；`HandlerOptions.Check` 可在打开 storage 前验证 HTTP 上限及 Authorizer／Volume 配置，constructor 会再次验证，并要求带原生发布能力的 `locked.Backend`，内部构造 `locked.Storage`；`locked.New` 拒绝 `CheckBounded` 失败或缺少绑定锁服务的 backend。可信 volume 与 backing、Log 的配对由集成方负责；认证 middleware 的 context 为 Authorizer 提供身份。监听、TLS、超时、路由前缀与 lifecycle 都由集成方决定。调用方也可以直接组合 `localstore.Open`，并通过 `Status`、`MaintenanceStatus`、`Sweep` 与 `Close` 管理它。

独立二进制没有业务认证与授权配置，部署方继续保护访问边界。它接受两种互斥存储入口：

| storage mode | 命令行 | 配额 | 复制 |
|---|---|---|---|
| Azure Blob + SQLite | `-listen ADDR -blob-container NAME -metastore PATH -volume NAME [-blob-prefix PREFIX] [-quota SIZE] [METASTORE OPTIONS] [LOCK OPTIONS] [HTTP OPTIONS]` | 可选；SQLite transaction 内计数 | 有 |
| 本地持久对象存储 | `-listen ADDR -local-store DIR -volume NAME -quota SIZE [METASTORE OPTIONS] [LOCAL OPTIONS] [LOCK OPTIONS] [HTTP OPTIONS]` | 必填；SQLite transaction 内计数，并受 physical availability 限制 | 有 |

两种模式都在首次建立 lease 证据时显式使用 `-initialize-lock-state`，正常重开验证已有证据；该开关不能修复缺失或错配的 READY 状态。Azure 的证据位于数据库旁，本地对象存储的证据位于其私有根。初始化、恢复等待与数据库级所有权见[持久证据](file-locks.md#重启与持久证据)。

`locking.Options` 的全局容量通过 `-lock-max-sessions`、`-lock-max-tickets`、`-lock-max-owners`、`-lock-max-resources`、`-lock-max-actions`、`-lock-max-grants`、`-lock-max-queued` 暴露，默认依次为 1024、1024、4096、4096、262144、8192、4096。局部上限 `-lock-owners-per-session`、`-lock-owner-actions-per-session`、`-lock-actions-per-owner`、`-lock-grants-per-owner`、`-lock-queued-per-owner`、`-lock-queued-per-resource` 默认依次为 64、256、1024、64、64、128。`-lock-max-proofs` 默认 16，`-lock-max-request-bytes` 默认 128；HTTP 还有独立的协议上限。

`-lock-max-lease`、`-lock-max-wait`、`-lock-ticket-ttl`、`-lock-resource-ttl` 默认各一分钟，`-lock-session-idle` 默认十分钟。时长至少一毫秒，session idle 必须覆盖最大 lease 与等待；降低配置不缩短已记录的恢复保护。所有状态容量都必须为有限正数。

`SIZE` 是一个整数字节数，可带 B、K、M、G、T、P 或 KiB 到 PiB 的后缀，每一级都是 1024 的幂；`KB`、`MB` 这类按 1000 的幂拼写的后缀被拒绝。配额一旦给出就不得小于 4096 字节；不给 `-quota` 是 Azure volume 没有 configured allowance 的唯一方式。本地持久 mode 必须给出配额。

两种形态共用 `-max-pending-objects`、`-max-pending-bytes`、`-max-reader-connections`、`-max-snapshot-reader-connections`、`-max-integrity-records` 与 `-max-integrity-bytes`，默认分别为 4096、8 GiB、16、16、1,000,000 和 64 MiB。本地形态另以 `-local-max-waiting-operations` 限制尚在 key/shard coordination 或等待 active budget 的调用，默认 256，满额时以 `EAGAIN` 拒绝。

Azure 与 local 两种 objectstore-backed 形态还共用 `-sweep-interval` 与 `-sweep-batch`，默认 1 分钟和 64；前者必须为正，后者必须在 1 到 `objectstore.MaxSweepBatch`（1,048,576）之间。interval 决定 transient cleanup failure 无新 mutation 时的重试上界，batch 限制每轮 object/metastore 工作量。

non-streaming HTTP flags 控制单体 body/write，request body 的 operation、waiter 与 aggregate bytes，以及 response 的 operation、waiter 与 aggregate bytes：`-http-max-body-bytes`、`-http-max-write-bytes`、`-http-max-concurrent-bodies`、`-http-max-waiting-bodies`、`-http-max-in-flight-body-bytes`、`-http-max-concurrent-responses`、`-http-max-waiting-responses` 与 `-http-max-in-flight-response-bytes`。默认值见第六节。local-store 要求 effective write 上限不大于 `-local-max-object-bytes`；Blob mode 要求它不大于 `min(azblob.MaxObjectBytes, effective -max-pending-bytes)`，其中 Azure 单对象上限是 5000 MiB。这些关系都在打开 storage 前验证。

锁控制使用 `-http-max-concurrent-lock-controls` 与 `-http-max-waiting-lock-controls`，默认各 16，零值选择 package 默认；它们独立于普通 body/response 与复制资源，协议单体仍固定 16 KiB。

复制资源由 `-http-max-subscriptions`、`-http-max-frame-bytes`、`-http-max-concurrent-snapshot-frames`、`-http-max-in-flight-snapshot-frame-bytes` 与 `-http-max-waiting-snapshot-frames` 配置；默认分别为 64、8 MiB、16、384 MiB 与 64。subscription 已满时不排队并以 `EAGAIN` 拒绝；snapshot-frame aggregate 必须至少容纳 `3 * MaxFrameBytes`。change/start frames 的总预算由 subscription 与 frame 上限推导，不使用 snapshot admission。两种随附形态都提供 change log。local-only 资源 flags、默认值与约束由 [`local-disk-object-store.md`](local-disk-object-store.md#七容量与资源上限) 定义；用在其它 storage mode 时命令行直接拒绝。

独立二进制还在 handler 之外持有三项 HTTP server 资源配置：`-http-max-connections` 默认 256，`-http-read-header-timeout` 默认 2 秒，`-http-idle-timeout` 默认 1 分钟。listener 在 `Accept` 之前取得 connection 名额，已满时停止接受新 connection，已有 connection 关闭后释放名额。上限必须为有限正数；两种随附形态至少需要 2 条连接，因为冷挂载会在保持 Subscribe 的同时打开 Snapshot。connection、subscription 与 snapshot 是独立上限；更紧的 connection cap 可以先成为全局约束。两个 timeout 都必须为正：前者限制一份 request header 到齐所需的时间，后者限制 keep-alive connection 等待下一份 request 的空闲时间。shutdown 时 command-owned `ConnState` tracker 先进入永久 stopping 状态，关闭已经接受和尚未进入 handler 的 `StateNew` connection，也立即关闭此后到达的 late notification，再调用 `Shutdown`；安全性不依赖 header timeout 小于 shutdown grace。独立 server 不设全局 `WriteTimeout`：变更流没有自然终点，snapshot 由自己的 deadline 约束。嵌入形态的 listener、connection admission 与 HTTP timeouts 仍由调用方拥有。

启动先完成静态配置校验并占用 listener，再进行 storage open/recovery。这个顺序使无法取得服务地址的进程不会初始化一份新的持久 store。local store 查询一次组合状态，Azure 查询对象、维护与锁状态；启动行报告 ready、recovering 或 unavailable 及安全计数，不打印能力身份。服务已开始监听不表示恢复屏障已经结束，也不是 Azure 远端对象可达性的证明；实际故障由后续操作或状态查询返回。

存储就绪后安装终止与 SIGHUP lifecycle。`serving ... at http://...` 是 READY announcement：这行出现时 storage、listener 与 signal ownership 都已建立；HTTP accept loop 紧接着启动，`startedListener` 使 announcement 与 `Serve` 交接期间到达的终止信号关闭 listener 并等待 server goroutine 退出。READY 之前的失败会关闭已取得的 listener，并在能证明 storage handles 已关闭时释放 storage ownership；pool cleanup 不确定时本地持久形态保留 root lock 到进程退出。cleanup failure 并入命令结果。

收到 SIGINT／SIGTERM 后，外层 admission gate 先拒绝新请求，handler 的非阻塞 Stop 取消授权 context 并触发 stream 结束机制，外层给在途请求 5 秒完成。deadline 到期时关闭连接，但仍等待已经进入 application handler 的调用、授权 callback 与 stream producer／资源清理离开，随后调用 `Handler.Close` 退役并排空该 handler 的 FileSession registry。registry 清理失败保留 backend 所有权并返回错误；成功后才停止并等待后台 maintenance 与 checkpoint worker。local store 再建立独立的 5 秒 close context，用它等待 commit gate 与完整 WAL checkpoint；active reader 立即使本轮关闭返回 `EBUSY`，pool `Close` 本身不接受该 context。reader pools 已关闭后的 checkpoint busy/failure/cancellation 保留 writer/WAL 并可重试；任一 pool close error 是 terminal result，锁保留到进程退出。只有所有 pools 无错误关闭后才释放 object-store lifetime lock，关闭各层的错误合并为命令结果。

SIGHUP 不经过网络控制面，只执行带两秒 context deadline 的状态查询；一个 command-owned goroutine 同一时刻至多处理一项。SIGINT／SIGTERM 取消并等待它，已经进入的不可取消 syscall 仍须返回。

两种形态都分别报告 reserved、unresolved 与 garbage backlog、pending thresholds、SQLite ordinary/event-reader 与 snapshot-reader connection 上限、integrity record/name-byte work 上限、effective sweep interval/batch 与最近 maintenance outcome；local store 另外报逻辑/物理空间、store UUID、waiting/active resource、recovery records，以及 checkpoint 的 accepted/checkpointed generation 与 pending。SIGINT／SIGTERM 取消并等待它；已经进入的不可取消 syscall 仍须返回。checkpoint error 或任一 component 查询失败时只报告 status failure，不打印部分数字。
