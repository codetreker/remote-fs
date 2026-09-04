# server 角色

命名空间的权威持有者。持有一份 storage，经 HTTP 暴露给多个 client。

本文只写 server 内部。角色边界与两条契约的分工见 [`../architecture.md`](../architecture.md)。

## 一、内部构成

| 组件 | 职责 | 需求 |
|---|---|---|
| **请求处理**（`packages/transport/httprest`） | 一个 `http.Handler`。解析请求、执行有界的 request/response admission、把结果预算传给 storage，再写回协议响应。不持有命名空间状态，也不缓存。**复制那三个操作要它持有连接级状态**：每条开着的订阅一个唤醒通道，每份开着的快照一个名额，外加一个停止信号——它们的寿命恰好是一条连接的寿命，都有上限（见下）。 | R-INT-1、R-INT-3 |
| **协议词汇**（`packages/transport/httprest`） | 请求 URL 的形状、响应体的形状；错误的名字取自 storage 契约的 errno 词汇。与 client 共用同一份。 | R-INT-9 |
| **变更日志** | 命名空间里每一次改动的有序记录，由 storage 底下的 metastore 提供。请求处理拿到它就开出复制那三个操作；拿不到（`nil`）就以 `ENOSYS` 拒绝它们。 | R-CON-1、R-CON-2 |
| **storage** | 命名空间的实际存取。由集成方提供，或使用随附的普通目录、本地持久对象存储、Azure Blob + SQLite 组合；普通目录可套字节配额，两个 metastore-backed 组合自己记账（见第七节）。 | R-INT-6、R-INT-13 |

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
 │   storage   │  localdir / localstore / objectstore / 自有实现
 └─────────────┘
```

一次请求就是一次完整的操作。没有会话、没有句柄，两次请求之间不留任何东西。

## 二、请求的形状

操作是 URL 路径的最后一段，操作数在 query string 里。

| 操作 | 方法与路径 | 操作数 | 请求体 |
|---|---|---|---|
| `Stat` | `GET /v2/stat` | `path` | — |
| `SetAttr` | `POST /v2/setattr` | `path` | 要改的属性，JSON |
| `List` | `GET /v2/list` | `path` | — |
| `Read` | `GET /v2/read` | `path` | — |
| `Write` | `POST /v2/write` | `path` | 文件内容 |
| `Create` | `POST /v2/create` | `path` | — |
| `Mkdir` | `POST /v2/mkdir` | `path` | — |
| `Remove` | `POST /v2/remove` | `path` | — |
| `RemoveDir` | `POST /v2/removedir` | `path` | — |
| `Rename` | `POST /v2/rename` | `path`（源）、`to` | — |
| `Space` | `GET /v2/space` | 无 | — |
| `Subscribe` | `GET /v2/subscribe` | 无 | — |
| `Resubscribe` | `GET /v2/resubscribe` | `incarnation`、`position` | — |
| `Snapshot` | `GET /v2/snapshot` | 无 | — |

命名空间路径以 `url.Values` 的转义走 query string，任意字节序列都逐字往返。根是 `path=`：一个存在且为空的操作数。`Space` 描述整个命名空间而不是某个路径底下的东西，因此它一个操作数都不带；带了 `path=` 的 `Space` 请求与多带了任何操作数的请求一样，是请求错误。

请求处理**逐字转交**收到的路径，不做清洗、不做越界判定。路径规则由 storage 接口的 `CleanPath` 定义，只有一份。

解析是严格的：query 解析不了、操作数缺失、同一个操作数出现两次、出现了这个操作不要的操作数，都是请求错误。这几种情况在 `url.Values` 里读出来都是空字符串，而空字符串是根。

`Prefix` 为 `/v2/`，相对于 handler 被挂载的位置。挂到别处用 `http.StripPrefix`。v2 把 mutation success 从空 body 改成严格的 barrier JSON，是与 v1 不兼容的 wire version；server 不提供兼容路由，client 与 server 必须一起升级。两代互连会因 path/header version 不匹配响亮失败，不会把旧形状当成成功。

## 三、响应的形状

每个响应带两个头：

- `Remote-Fs-Protocol: 2` —— 标记这是本协议的服务端给出的答案。
- `Cache-Control: no-store` —— 一个被缓存住的答案读起来与当前的答案没有区别。

| 情形 | 状态 | 体 |
|---|---|---|
| `Stat` 成功 | `200` | `{"attr":{…}}` |
| `List` 成功 | `200` | `{"entries":[…]}`，空目录是 `[]`，绝不是 `null` |
| `Read` 成功 | `200` | 文件内容，`Content-Type: application/octet-stream` |
| `Space` 成功 | `200` | `{"space":{"total":…,"used":…,"avail":…}}` |
| `SetAttr`、`Write`、`Create`、`Mkdir`、`Remove`、`RemoveDir`、`Rename` 成功 | `200` | metastore-backed namespace 是 `{"barrier":{"incarnation":"…","position":…}}`；没有 change log 时是 `{}` |
| storage 报错，handler 在调用 storage 前以 `EFBIG`/`EAGAIN` 权威拒绝，或 mutation 成功后无法读取 replication barrier | `422` | `{"errno":"<名字>","message":"…"}`；最后一种固定为 `EIO` |
| `SetAttr` 请求体超过 `MaxBodyBytes` | `413` | `{"message":"…"}` |
| 操作不存在 | `404` | `{"message":"…"}` |
| 方法不对 | `405` | `{"message":"…"}` |
| 请求解析不了、请求体不完整 | `400` | `{"message":"…"}` |
| 复制那三个操作成功 | `200` | 一串 server-sent event，`Content-Type: text/event-stream`（见第五节） |

handler 用 `MaxBodyBytes` 限制 non-write 请求与所有 non-streaming 响应，用 `MaxWriteBytes` 单独限制 `Write` 的文件内容；后者为零时继承前者，显式值必须为正且不大于前者。过大的 `Write` 在调用 storage 之前以 `EFBIG` 返回，过大的 `SetAttr` body 是 `413`。协议定义为 bodyless 的操作只探测是否出现第一个字节，任何非空 body 都是 `400`，不会按声明长度保留内容。过大的 `Read` 是 `EFBIG`；无法在 `MaxBodyBytes` 内编码的 `List` 是 `EIO`。错误响应也必须装进 `MaxBodyBytes`，过长的 detail 会换成固定诊断，协议 marker 与 errno 不丢失。

`200` 是唯一的成功状态，`422` 是唯一承载 errno 的状态。它表示 server 已经得到一个可命名的操作结果：来自 storage、来自调用 storage 前就能权威判定的 `EFBIG`/`EAGAIN` 资源拒绝，或来自已经成功的 mutation 之后无法取得 barrier 的 `EIO`。最后一种不能退回成功：namespace 已改变，但 server 无法给 replicated client 一条证明副本何时包含它的界线。errno 不编码进状态码 —— 状态码空间与 errno 空间不同构。

**每个 non-streaming 响应都显式声明 `Content-Length`，失败响应也不例外。** 这是顶层设计第四节「响应体的分帧必须能报告自己提前结束」那条义务在这一侧的落地：一个以连接关闭为终点的响应体，被截断与完整无从分辨，因此不被接受。SSE response 不声明整条 stream 的长度，它靠每个 frame 的明确边界与终止语义区分完整和截断。

**缺席与零值必须分辨得开。** `Stat` 响应里的 `attr`、以及每个目录条目里的 `attr`，都是可以在报文里缺席的字段，而缺席就是解码失败 —— 一个零值的属性读起来是「一个模式为 0、长度为 0、时间停在 1970 年的普通文件」，与一份合法的答案分辨不开。`entries` 同理：`null` 与 `[]` 相差两个字符，意思相反。mutation response 必须是一个只允许可选 `barrier` 字段的 JSON object：没有 change log 的 namespace 返回 `{}`；有 log 时 barrier 必须存在，且带非空 incarnation 与非负 position，位置 0 也是合法的初始 barrier。incarnation 的 protocol ceiling 是 256 bytes；handler 再按 `MaxBodyBytes` 与 `MaxFrameBytes` 的 worst-case JSON escaping 计算同一份更紧预算，stream start 与 mutation barrier 都用它，保证合法 identity 在两条路径上一致且一定装得进各自边界。`null`、缺字段、未知字段或错误类型都不是成功答案；普通 remote storage 接受无 barrier 的 `{}`，replicated client 使用的 `*WithBarrier` 方法必须取得并验证它。

容量报告里的三个数各自也是可缺席的字段，而这里的理由更硬：它们是字节数，零对每一个都是合法答案 —— 一份什么都没装的命名空间已用为零，一份装满的可写入量为零 —— 所以一旦当成普通字段读进来，缺席与零就再也分不开，而一份掉了字段的报告读起来恰好是「没有剩余空间」，足以让每一次写入停下。解码还要判定这三个数能不能同时为真，判不成立同样是解码失败：它们最终要进内核回复的无符号字段，在那里一个负数是一个巨大的正数（R-ERR-2）。

目录条目的名字以原始字节编码（JSON 里是 base64）。文件名是任意字节序列，不是文本。

`Attr` 的 `id` 是节点身份（R-FS-5）：一个不透明的无符号整数，只可比较相等，随节点走过改名。**它是这里唯一一个零值不响的字段**，因此由解码拒绝：模式为 0 是合法答案、纪元时刻也是有人设得出来的值，所以别处的拒绝针对的是整个 `attr` 缺席；而身份为 0 在挂载点那边每次比较都相等，于是一个不发这个字段的对端不会被读成「什么都没说」，会被读成「所有节点都是同一个节点」。

`Attr` 的 `mode` 是 Go `io/fs.FileMode` 的位布局。两个时间 —— `access_time` 与 `mod_time` —— 各是一个对象，`unix_sec` 是自 Unix 纪元起的整秒数，`nanos` 是该秒之内的纳秒数。单独一个纳秒数装不下这两个字段要承载的范围 —— `time.Time.UnixNano` 只在 1678-09-21 到 2262-04-11 之间有定义，范围之外的时间（零值的 `time.Time` 也在其中）会变成另一个看上去完全合理的日期，且没有任何东西标出它是错的。秒与纳秒合成一个对象而不是并排两个字段，是因为 `SetAttr` 的请求里每个时间都可以整个缺席，而两个各自可空的字段能互相矛盾。

`SetAttr` 的请求体是 `{"change":{…}}`，`change` 里每个属性都是可选的：缺席就是「这一项不改」。`change` 本身缺席则是解码失败 —— 一个什么都不点名的改动是合法请求（它在问这个节点还在不在），因此靠字段本身分辨不出报文是不是掉了内容，外面这一层对象才分辨得出来。

## 四、错误如何离开 server

`packages/storage` 持有一张 errno 与符号名之间的双向表，它同时是一个 storage 实现允许报出的 errno 的全集。它归契约而不归某一种传输：一个实现可以报出哪些错误，是契约的性质。

storage 返回错误时，请求处理从错误链里取出 `syscall.Errno`：

- 取得到，且在表里 → 用它的名字，`422`。
- 取不到，或者不在表里 → `EIO`，`422`，原始错误的文本放进 `message`。

**无法命名的失败一律是 `EIO`。** 挑一个最接近的名字，等于把一个不确定的失败说成一个确定的事实；而 `ENOENT` 一旦被这样说出去，上层会据以删除、重新生成或覆盖（R-ERR-1、R-ERR-2）。

`Space` 的 `ENOSYS` 走的也是这条路。它是关于那个命名空间的答案 —— 它没有自己的容量可报 —— 而不是本协议缺了一块，因此和其它 errno 一样以 `422` 带着自己的名字回去，不用 `501`。

## 五、复制那三个操作

一份命名空间的元数据能不能被复制，取决于它底下有没有一条变更日志。有的（metastore 后端），这三个操作开着；没有的（`localdir`），三个一律 `ENOSYS` —— 那是关于那份命名空间的一句事实，与「够不到」是两回事，两者要求的动作正好相反：`ENOSYS` 说这里永远不会有副本，别再问了；`EIO` 说过一会儿再试。**绝不能答一条空的流或一份没有行的快照** —— 那读起来是「这个命名空间存在、是空的、永不改变」，而这正是一个副本会相信的答案。

| 操作 | 答什么 |
|---|---|
| `Subscribe` | 从日志当前的尾位置起，把此后每一条变更推给这个订阅者。要建副本的 client 先做这一步。 |
| `Resubscribe` | 带着（化身，位置）回来：化身对不上、或者那个位置已经掉出保留窗口，答「必须重建」并说明是从哪个维度掉出去的；否则从那个位置之后接着推。 |
| `Snapshot` | 一次一致性切割：所有行反映同一个瞬间，并带回那个瞬间的位置。分页送。 |

两点是这三个操作的形状所依赖的：

**先订阅、后取快照。** 反过来不收敛 —— 扫描耗时乘以变更速率超过保留窗口，快照的位置就已经掉出窗口，于是从头再来，而重建代价正比于树的大小。订阅在前之后，保留窗口不在首次同步的关键路径上，它只伺候断线重连。

**这三个流与请求／响应天生在不同的连接上。** 快照是系统里最大的一次批量传输；它若与事件挤在一条连接上，就会挤掉喂着副本的那条流，后果是重新拉一份快照 —— 一个自我放大的循环，而触发它只需要一次正常的冷挂载。走 SSE（`text/event-stream`）因此不需要额外机制：每个流是一次独立的 HTTP 请求。义务写成性质而不是拓扑 —— 从一次变更被记入日志，到它的事件抵达一个健康订阅者，其耗时与并发的批量传输无关 —— 将来的其它传输各自说明它用什么机制满足它。

**流的失败没有状态码可用。** 状态与响应头在第一帧之前就发走了，因此此后出的错以一个 `fault` 帧代替本该跟在后面的一切。收到它与流直接断掉在 client 那边是同一个判定（都算失败），这个帧只决定事后有没有人说得清出了什么事。

server 为这几个操作持有的资源都有上限：同时开着的订阅与快照数、一份快照最长可以送多久、单个 encoded frame、跨页保留的 snapshot cursor bytes、同时产生的 snapshot page 数与总 retained bytes、等待 snapshot-page admission 的 goroutine，以及一次读日志或快照最多处理多少行（R-INT-3）。change frame 的 aggregate 由 subscription 数与单帧预算共同给出，不与 snapshot bulk transfer 共用 gate。还有一个不是上限而是下限：**无话可说时多久也要说一句**——心跳的间隔。读的那一侧据此给「一个字节都没来」定上界，于是「流还活着」是被观测到的而不是被假定的；没有它，一条被切断的 TCP 与一个安静的命名空间是同一个观测结果。

`MaxFrameBytes` 不只在 JSON 已经生成后检查。`Log.Incarnation(ctx, maxBytes)` 在载入或复制 stream identity 前限制 UTF-8 bytes；`Log.Since` 把 payload lengths 交给 caller-owned `metastore.ChangeResult`，`Snap.Next` 使用 `metastore.RowResult`。producer 先以 fixed fields 与变长字段长度 `Reserve`，预算通过后才加载 name、source name 与 object key，并用 exact lengths `Commit`。一页已经有内容而下一项只是不够剩余空间时，该项留给下一页；单项本身装不进空 frame 时以 `EFBIG` 使整页失败。其它 production error 同样使 page 不可读取，snapshot 上的这类错误还终止该一致性切割。`EventPage` 与 `SnapshotPage` 继续限制一轮数据库工作量，不能代替 byte bound。`Log.Barrier(ctx, maxIncarnationBytes)` 则在 mutation 完成后原子给出同一份有界 identity 与 committed position。第三方 `metastore.Log` 与 `Snap` 也必须实现这些带预算的唯一入口；接口不保留会在内部建立 unbounded slice 的 count-only 变体。

每条 subscription 串行地产生 start/change frame，同一时刻至多保留一项，每项按 `3 * MaxFrameBytes` 覆盖 metastore result、wire conversion 与 encoded frame。它不等待共享 admission，因此一份 snapshot 的 bulk production 不能阻塞健康订阅者；默认最多 64 条 subscription 时，change/start 中间表示的 derived aggregate ceiling 是 `64 * 3 * 8 MiB = 1.5 GiB`。

snapshot page 另有共享 operation、aggregate byte 与 waiter admission，每份同样按 `3 * MaxFrameBytes` 预留；默认同时 16 页、aggregate 384 MiB、等待者 64 个。队列已满立即 `EAGAIN`；已经进入等待的 producer 服从 snapshot request context 与 `SnapshotDeadline`，取消或 deadline 会释放计数。等待期间 delivery loop 仍继续发送 keepalive，所以资源排队不会被 client 的 silence bound 误判为断流。每份打开的 SQLite snapshot 还在两页之间保留至多 `MaxFrameBytes` 的 name cursor；其 aggregate 从 `Snapshots * MaxFrameBytes` 推导，默认 `8 * 8 MiB = 64 MiB`，constructor 在乘法溢出时拒绝配置。这项 cursor bound 与 page admission 分开计量。

client 的 `DialOptions.MaxFrameBytes` 默认也是 8 MiB，逐 stream 限制 scanner 保留的一帧；client library 不拥有调用方打开多少条 stream，因此 stream cardinality 仍由调用方约束。

订阅默认最多 64 条。新订阅在保留唤醒 channel 与 stream state 前检查名额，已满时不排队，以 `EAGAIN` 拒绝；request cancellation、断连、失败或 `Handler.Stop` 结束 stream 时释放名额。快照默认最多 8 份；快照占着 storage 里的一个资源 —— 在 SQLite 上是一个读事务 —— 而它活多久由网络决定，所以已经开满时新的请求也以 `EAGAIN` 拒绝而不是排队：client 是先订阅再来要快照的，重试对它不花什么代价。

SQLite metastore 把普通 namespace/log read 与长期 snapshot 放进两个 reader pool，避免慢 snapshot 占完普通操作与 event catch-up 能用的 connection。`MaxReaderConnections` 与 `MaxSnapshotReaderConnections` 默认各为 16；各自池满时读取等待 connection，并遵从对应 request/snapshot context 取消。snapshot 并发上限限制打开的读事务数，snapshot reader pool 限制数据库为它们持有的物理 connection 数，两者保持独立。

**订阅者是被唤醒的，不是被投喂的。** 每一次改动了命名空间的请求在答复之前唤醒所有订阅，被唤醒的订阅自己去读日志。于是「追上」与「跟上」是同一条代码路径，不可能对「一条变更是什么」有两种说法；也没有任何一处等待间隔（R-CON-2）。mutation 成功后，handler 再用 `Log.Barrier` 在 mutation response 的 incarnation budget 下原子读取 log incarnation 与 committed position；并发 mutation 可以让 position 更晚，但同一事务记录本次修改保证它不会更早。barrier 读取失败发生在 namespace 已改变之后，以 `EIO` 返回且不伪装成未修改。它假定的是**这个进程是唯一在写这份命名空间的**：另一个 server 写到同一个数据库，它记下的变更到不了这里的订阅者。`-local-store` 用 lifetime lock 强制这项前提；Azure Blob + SQLite 形态由部署方保证同一 database/namespace 只有一个 active server。

## 六、请求与响应的内存边界

`Write` 与 `SetAttr` 的请求体必须先完整到齐，才允许落到 storage。提前结束的文件内容不能被当作一次成功的前缀替换；只到一部分的属性改动也不能被当作完整请求。

handler 在读取 body 前取得 request operation 与 byte admission。`Write` 预留 `MaxWriteBytes`，`SetAttr` 预留 `MaxBodyBytes`；缺失或虚假的 `Content-Length` 不能换成较小 reservation。bodyless 操作已声明非空 body 时立即拒绝；长度未知时取得 operation slot，最多读一个探测字节。等待 admission 的请求服从 request context；等待者已满时新请求以 `EAGAIN` 失败。

HTTP handler 只接受实现 `storage.BoundedStorage` 的 namespace。constructor 在服务请求前调用 `CheckBounded`；任一依赖无法在实际产生结果时接受预算，启动就直接失败。`ReadBounded` 在完整 payload 分配前知道 byte bound，超限以 `EFBIG` 失败。`ListBounded` 按顺序把 entry 加入调用方的 `ListResult`；`ListResult` 用 HTTP 表示的精确逐条 charge 计数，并在保留那条超限 entry 之前以 `EIO` 失败。backend 不得先建立完整的超限 `[]byte` 或 `[]Entry` 中间结果。

`Read` 与 `List` 的 wire format 仍是一份 non-streaming response，在发出前保留一份配置内的完整结果。每个非流式操作在调用 storage 前都取得 response operation 与 byte admission；`Read` 与 `List` 按 `4 * MaxBodyBytes` 预留，覆盖 storage entries、wire conversion、encoded body 与它们同时存在的保守峰值，其余固定结果操作按 `MaxBodyBytes` 预留，使得同时产生的最大错误 JSON 也在 aggregate 上限内。response 等待者已满时，`Stat`、`Write`、`Create` 等操作也会在到达 storage 前以 `EAGAIN` 失败；`Write` 与 `SetAttr` 还不会读取 request body。context cancellation 会退出等待并释放计数。

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

server 通过 storage 接口访问命名空间，不知道底下是什么。集成方注入自己的实现（R-INT-6），或选择随附形态：

| 形态 | 组成 | 变更日志 |
|---|---|---|
| 普通目录 | `packages/storage/localdir` | 无 |
| 本地持久对象存储 | `packages/storage/localstore` 持有 `objectstore.Storage`、`localdisk.Objects` 与绑定的 `sqlite.Store` | 有 |
| Azure 对象存储 | `objectstore.Storage` + `azblob.Objects` + `sqlite.Store` | 有 |

`localdir` 只依赖那个目录。Azure 形态依赖部署方分别提供和运维 Blob container 与数据库，两者可以各自失败（R-INT-12、R-ERR-6）。`localstore` 则拥有一个私有本地目录下的对象、SQLite、恢复状态与独占锁；它的完整设计见 [`local-disk-object-store.md`](local-disk-object-store.md)。

storage 必须履行的义务、十一个操作的形状、路径规则与错误词汇，由 storage 接口定义，见顶层设计第四节。

### 配额住在 storage 这一侧

配额属于 workspace，不属于它底下那块盘——一个普通目录根本没有配额这个概念——因此计数与拒绝都落在 storage 这一层，而不是在 server 里。随附实现用两种方式做到它。

**包一层：`packages/storage/limited`** 包住任意一份 `storage.BoundedStorage`（R-WS-5、R-INT-3），这是把配额加到一份本来没有配额概念的实现上的通用办法：

- 已用量在打开时走一遍命名空间量出来，此后由每一次经过这里的修改推动。遍历前先调用 `CheckBounded`，每个目录经 `ListBounded` 逐项计量；当前目录的 `Entry` 与名字、尚待访问的完整目录路径分别受独立 byte bound 约束，默认各为 64 MiB。目录预算在保留越界 entry 前拒绝，frontier 预算包含当前正在访问的目录；任一上限不足都以 `EIO` 使整次计量失败，不提交部分结果。HTTP body 上限不参与 namespace measurement。
- 写入超出配额以 `EDQUOT` 拒绝。收费发生在写之前，写失败则退回，因此两个写不同文件的调用方不会双双通过一次只够其中一个人的检查。
- 让命名空间变小的修改从不被拒绝，已经超出配额时也不拒绝 —— 否则一个超额的 workspace 没有任何回到配额之内的路。
- 配额不得低于一个 4096 字节的块：再小的配额会被报成一个零块的文件系统，那读起来是一块没有剩余空间的盘，而不是一个空间很小的 workspace。
- 绕过 server 直接改动底下的目录会让计数漂移，而任何经由这里的流量都修不回来（这是 `docs/spec/` 里写明的非目标）。计数在存的地方就被压在零以上，**被这条守住的是报出去的数字**：「还能写入的量」不会超过配额剩下的部分，也不会从内核回复的无符号字段里出来变成一个巨大的正数。它守不住账本本身 —— 一个从旁边被拷进来、再经由这里被删掉的文件，会让计数停在真相之下，此后的写入照单被接受，命名空间因此可以越过配额而没有任何人被拒绝。回到实测值的唯一途径是以同一组 measurement limits 重新量一遍：`Recount`，随附的二进制把它接在 SIGHUP 上（见第九节）；超限、取消或 listing 失败都保留原计数。

`Space` 报出的「还能写入的量」取配额剩余与底层实现所报之中较小的那个。配额是「还允许写多少」而不是「这些字节一定放得下」：底下那块盘比配额更紧时若仍报配额，等于向先查空间再决定写不写的程序许诺机器给不出的余量。

**自己记账：`packages/storage/objectstore`** 不需要被包起来。它的树存在一个数据库里，于是「已用多少」由每一次改变文件大小的 transaction 同时更新和校验，不存在「判定有余量」与「占用余量」之间的窗口。没有配置配额时 metastore 以 `ENOSYS` 拒绝 `Space`。

SQLite metastore 还给 reserved、unresolved 与 garbage object records 的合计数量和 payload bytes 配置独立阈值。一个 payload 自身超过 byte threshold 时 `Reserve` 返回 `EFBIG`；请求本身能装下、但现有 backlog 使新记录越界时返回 `EAGAIN`。`Put` 失败时 reservation 转成 unresolved；这类结果没有 ownership proof，不会因为时间经过而被删除。已经存在的 namespace 修改仍可产生 garbage 并把 backlog 推到阈值之上，此时新 reservation 保持拒绝，garbage 清扫与删除继续运行。package 默认值与 `-max-pending-objects`、`-max-pending-bytes` 用于 Azure 和本地形态；本地组合还通过 `Config.ObjectLimits` 暴露覆盖值，见 [`local-disk-object-store.md`](local-disk-object-store.md#七容量与资源上限)。两种 metastore-backed 形态同样使用有界 SQLite reader pool；package 默认为 16，独立 server 以 `-max-reader-connections` 配置。

SQLite 打开与 `ObjectStatus` 会验证每个 namespace 是一棵完整的 rooted tree：root 没有 incoming entry，每个非 root 节点恰有一个同 namespace 的名字，所有节点都从 root 可达，cycle 与孤儿都以 `EIO` 拒绝。`namespaces.used` 必须是非负整数，并等于对所有 regular-file size 做 overflow-checked streaming sum 的结果。SQLite 的动态 storage class 也属于完整性：文件名必须是非空 BLOB，标量字段保持声明的整数/文本/可空类型；log tail、change kind 与 nullable node/from groups、mode/size/time 范围必须彼此一致。`committed_position` 必须等于最新 surviving change position，或在没有 change 时为零；不一致直接使打开以 `EIO` 失败，不通过生成新 incarnation 把损坏解释成一次可重建历史。否则 cursor order、NULL coercion 或 fabricated reconciliation 可以把损坏记录变成一次成功但缺行/零值的复制结果，因此都在开放 namespace 或 history 前拒绝。

recursive reachability 之前先以 scalar aggregate 计算这次验证会触及的 namespace seed、node、relevant entry、object、log 与 change records 总数；entry 的 label、parent 或 child 任一接触目标 namespace 都计入，避免 corrupt cross-namespace edge 藏在预算之外。`MaxIntegrityRecords` 默认 1,000,000，`MinIntegrityRecords` 为 3，恰可容纳一个 namespace seed、空 root 与 mandatory log row；零值选择默认，低于 3 或 `math.MaxInt64` 在数据库打开前以 `EINVAL` 拒绝。预检超过上限时返回 `EFBIG` 并要求提高配置，不运行 recursive CTE；count/query 本身失败是 `EIO`。

v1/v2 数据库还面临 object lifecycle 证据缺失：旧实现把 reservation size 记为零，也可以依据时间把 reservation 改成 garbage。只有全部 object row 都是可验证的 referenced 状态且上述完整性成立时才允许前滚迁移；任一 non-referenced row 或结构、计数错误都使打开以 `EIO` 失败，整个迁移 transaction 不提交。legacy preflight 对整个数据库计量 integrity work，不只检查这次请求打开的 namespace。这类旧状态需要运维先证明对象归属并修复数据，新代码不会猜测字节数或授权删除。

`objectstore.Storage.Space` 还会询问 `Objects.Available`。外部 object service 只返回 `ENOSYS` 时，报告继续使用 metastore 的逻辑配额；local-disk objects 返回底层 filesystem 扣除维护与 in-flight reserve 后的物理可用量，最终 `Avail` 是逻辑余量与物理余量中较小的那个。任何实际 measurement error，包括与 `ENOSYS` 一起出现的 error，都使整个调用失败。

事务内计数不会因正常 API 流量漂移，因此 metastore-backed 形态没有 `Recount`。本地对象文件仍可能被存储外的主体删除或修改；格式、身份、长度与 checksum 校验会把它暴露为 `EIO`，不会把这条旁路解释成一次 namespace mutation。

## 八、server 不做什么

- **不缓存。** 命名空间的事实就在 storage 里，变更的事实就在日志里。
- **不替 client 记住任何东西。** 一个订阅者的位置在它自己手里，server 不保存它，也不知道谁复制过这份命名空间。
- **不持有 client 的状态。** client 消失即消失。
- **不清洗路径。** 逐字交给 storage。
- **不做鉴权。** 且不做半套 —— 不留形同虚设的校验分支、不留默认放行的开关、不打印会让人误以为受保护的日志。理由见[第一个可用版本的范围](../../../.agents/notes/implemented/process/2026-08-19-mvp-scope.md)。
- **handler package 不打印，也不记日志。** 请求失败的原因随该次响应返回；独立二进制只把 lifecycle、recount 与 metastore-backed status 写到 stderr。

## 九、部署形态

作为库嵌入集成方既有的 server，或作为独立二进制运行（R-INT-1、R-INT-4）。

作为库时：不注册信号处理、不写 stdout/stderr、不调用进程退出、包初始化不产生副作用（R-INT-2）。`packages/transport/httprest` 暴露 `NewHandler`、`NewHandlerWithLimits` 与 `NewHandlerWithOptions`；`HandlerOptions.Check` 可在打开 storage 前验证所有 HTTP 上限，constructor 会再次验证，并拒绝不实现 `storage.BoundedStorage` 或 `CheckBounded` 失败的 backend。监听、TLS、超时、路由前缀与 lifecycle 都由集成方决定。调用方也可以直接组合 `localstore.Open`，并通过 `Status`、`MaintenanceStatus`、`Sweep` 与 `Close` 管理它。

独立二进制接受三种互斥入口：

| storage mode | 命令行 | 配额 | 复制 |
|---|---|---|---|
| 普通目录 | `-listen ADDR -dir DIR [-quota SIZE] [QUOTA MEASUREMENT OPTIONS] [HTTP OPTIONS]` | 可选；给出后由 `limited.Storage` 有界计量 | 无 |
| Azure Blob + SQLite | `-listen ADDR -blob-container NAME -metastore PATH -workspace NAME [-blob-prefix PREFIX] [-quota SIZE] [METASTORE OPTIONS] [HTTP OPTIONS]` | 可选；SQLite transaction 内计数 | 有 |
| 本地持久对象存储 | `-listen ADDR -local-store DIR -workspace NAME -quota SIZE [METASTORE OPTIONS] [LOCAL OPTIONS] [HTTP OPTIONS]` | 必填；SQLite transaction 内计数，并受 physical availability 限制 | 有 |

`SIZE` 是一个整数字节数，可带 B、K、M、G、T、P 或 KiB 到 PiB 的后缀，每一级都是 1024 的幂；`KB`、`MB` 这类按 1000 的幂拼写的后缀被拒绝。配额一旦给出就不得小于 4096 字节；不给 `-quota` 是普通目录或 Azure namespace 没有 configured allowance 的唯一方式。本地持久 mode 必须给出配额。

quota-limited 普通目录以 `-quota-max-directory-bytes` 与 `-quota-max-frontier-bytes` 分别配置单目录结果和遍历 frontier，默认各为 64 MiB；只有同时给出 `-dir` 与 `-quota` 时才接受这两个 flags。metastore-backed 形态共用 `-max-pending-objects`、`-max-pending-bytes`、`-max-reader-connections`、`-max-snapshot-reader-connections` 与 `-max-integrity-records`，默认分别为 4096、8 GiB、16、16 和 1,000,000；`-dir` 携带它们会被拒绝。

Azure 与 local 两种 objectstore-backed 形态还共用 `-sweep-interval` 与 `-sweep-batch`，默认 1 分钟和 64；前者必须为正，后者必须在 1 到 `objectstore.MaxSweepBatch`（1,048,576）之间。interval 决定 transient cleanup failure 无新 mutation 时的重试上界，batch 限制每轮 object/metastore 工作量。普通目录没有 garbage ledger，显式携带它们会被拒绝。

non-streaming HTTP flags 控制单体 body/write，request body 的 operation、waiter 与 aggregate bytes，以及 response 的 operation、waiter 与 aggregate bytes：`-http-max-body-bytes`、`-http-max-write-bytes`、`-http-max-concurrent-bodies`、`-http-max-waiting-bodies`、`-http-max-in-flight-body-bytes`、`-http-max-concurrent-responses`、`-http-max-waiting-responses` 与 `-http-max-in-flight-response-bytes`。默认值见第六节。local-store 要求 effective write 上限不大于 `-local-max-object-bytes`；Blob mode 要求它不大于 `min(azblob.MaxObjectBytes, effective -max-pending-bytes)`，其中 Azure 单对象上限是 5000 MiB。两种关系都在打开 storage 前验证，避免 handler 先保留一份 backend 必然以 `EFBIG` 拒绝的 write body。

复制资源由 `-http-max-subscriptions`、`-http-max-frame-bytes`、`-http-max-concurrent-snapshot-frames`、`-http-max-in-flight-snapshot-frame-bytes` 与 `-http-max-waiting-snapshot-frames` 配置；默认分别为 64、8 MiB、16、384 MiB 与 64。subscription 已满时不排队并以 `EAGAIN` 拒绝；snapshot-frame aggregate 必须至少容纳 `3 * MaxFrameBytes`。change/start frames 的总预算由 subscription 与 frame 上限推导，不使用 snapshot admission。这些 flags 只对提供 change log 的 Azure/local 形态有效，`-dir` 显式携带任一项都会被拒绝。local-only 资源 flags、默认值与约束由 [`local-disk-object-store.md`](local-disk-object-store.md#七容量与资源上限) 定义；用在其它 storage mode 时命令行直接拒绝。

独立二进制还在 handler 之外持有三项 HTTP server 资源配置：`-http-max-connections` 默认 256，`-http-read-header-timeout` 默认 2 秒，`-http-idle-timeout` 默认 1 分钟。listener 在 `Accept` 之前取得 connection 名额，已满时停止接受新 connection，已有 connection 关闭后释放名额。上限必须为有限正数；metastore-backed 形态至少需要 2 条连接，因为冷挂载会在保持 Subscribe 的同时打开 Snapshot，普通目录允许 1。connection、subscription 与 snapshot 是独立上限；更紧的 connection cap 可以先成为全局约束。两个 timeout 都必须为正：前者限制一份 request header 到齐所需的时间，后者限制 keep-alive connection 等待下一份 request 的空闲时间。shutdown 时 command-owned `ConnState` tracker 先进入永久 stopping 状态，关闭已经接受和尚未进入 handler 的 `StateNew` connection，也立即关闭此后到达的 late notification，再调用 `Shutdown`；安全性不依赖 header timeout 小于 shutdown grace。独立 server 不设全局 `WriteTimeout`：变更流没有自然终点，snapshot 由自己的 deadline 约束。嵌入形态的 listener、connection admission 与 HTTP timeouts 仍由调用方拥有。

启动先完成静态配置校验并占用 listener，再进行 storage open/recovery。这个顺序使无法取得服务地址的进程不会初始化一份新的持久 store。quota-limited 普通目录此时才按配置的 directory/frontier bounds 遍历；超限或读取失败会关闭 listener，且不会打印 READY。它与 Azure namespace 还会查询一次容量，local store 会查询一次组合状态；无配额普通目录与无配额 Azure 形态不做这项启动查询。因此 Azure 的 READY 不是一份远端可达性证明；对象服务不可达会由随后的操作或 SIGHUP 状态如实报错。

存储就绪后安装终止与 SIGHUP lifecycle。`serving ... at http://...` 是 READY announcement：这行出现时 storage、listener 与 signal ownership 都已建立；HTTP accept loop 紧接着启动，`startedListener` 使 announcement 与 `Serve` 交接期间到达的终止信号关闭 listener 并等待 server goroutine 退出。READY 之前的失败会关闭已取得的 listener 与 storage，cleanup failure 并入命令结果。

收到 SIGINT／SIGTERM 后，外层 admission gate 先拒绝新请求，handler 向每条 change stream 发 server-stopping frame，并给在途请求 5 秒完成。deadline 到期时关闭连接，但仍等待已经进入 application handler 的调用离开，随后停止后台 maintenance、关闭 SQLite，最后释放 object-store lifetime lock。关闭各层的错误合并为命令结果。

SIGHUP 不经过网络控制面，它请求的工作在一个 command-owned goroutine 中运行，同一时刻至多一项。quota-limited 普通目录用启动时同一组 directory/frontier bounds 重新遍历 namespace；只有完整成功才替换可能漂移的计数，超限、取消或读取失败都会报告 recount failure 并保留原计数，服务继续运行。无配额目录说明没有可重数的 allowance。recount 自身没有 deadline，但不占住 signal loop；SIGINT／SIGTERM 先关闭 HTTP admission 并向 recount 发送取消，再关闭 listener、排空 handler，最后等待 recount 离开。已经进入的不可取消 filesystem syscall 仍可延迟进程退出，但不会让 HTTP 继续接受新请求。

两个 metastore-backed 形态执行带 2 秒 context deadline 的只读 status：它们都分别报 reserved、unresolved 与 garbage backlog、pending thresholds、SQLite ordinary/event-reader 与 snapshot-reader connection 上限、integrity record work 上限、effective sweep interval/batch 与最近 maintenance outcome；local store 另外报逻辑/物理空间、store UUID、in-flight resource 和 recovery records。SIGINT／SIGTERM 取消并等待它；已经进入的不可取消 syscall 仍须返回。任一 component 查询失败时只报告 status failure，不打印部分数字。
