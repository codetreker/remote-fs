# server 角色

命名空间的权威持有者。持有一份 storage，经 HTTP 暴露给多个 client。

本文只写 server 内部。角色边界与两条契约的分工见 [`../architecture.md`](../architecture.md)。

## 一、内部构成

| 组件 | 职责 | 需求 |
|---|---|---|
| **请求处理**（`packages/transport/httprest`） | 一个 `http.Handler`。解析请求、调用 storage、把结果写回。不持有命名空间状态，不缓存，不持有连接级状态。 | R-INT-1 |
| **协议词汇**（`packages/transport/httprest`） | 请求 URL 的形状、响应体的形状；错误的名字取自 storage 契约的 errno 词汇。与 client 共用同一份。 | R-INT-9 |
| **storage** | 命名空间的实际存取。由集成方提供，或使用随附的本地目录实现 `packages/storage/localdir`。 | R-INT-6 |

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
 │   storage   │  localdir / 集成方自有实现
 └─────────────┘
```

一次请求就是一次完整的操作。没有会话、没有句柄，两次请求之间不留任何东西。

## 二、请求的形状

操作是 URL 路径的最后一段，操作数在 query string 里。

| 操作 | 方法与路径 | 操作数 | 请求体 |
|---|---|---|---|
| `Stat` | `GET /v1/stat` | `path` | — |
| `SetAttr` | `POST /v1/setattr` | `path` | 要改的属性，JSON |
| `List` | `GET /v1/list` | `path` | — |
| `Read` | `GET /v1/read` | `path` | — |
| `Write` | `POST /v1/write` | `path` | 文件内容 |
| `Create` | `POST /v1/create` | `path` | — |
| `Mkdir` | `POST /v1/mkdir` | `path` | — |
| `Remove` | `POST /v1/remove` | `path` | — |
| `RemoveDir` | `POST /v1/removedir` | `path` | — |
| `Rename` | `POST /v1/rename` | `path`（源）、`to` | — |

命名空间路径以 `url.Values` 的转义走 query string，任意字节序列都逐字往返。根是 `path=`：一个存在且为空的操作数。

请求处理**逐字转交**收到的路径，不做清洗、不做越界判定。路径规则由 storage 接口的 `CleanPath` 定义，只有一份。

解析是严格的：query 解析不了、操作数缺失、同一个操作数出现两次、出现了这个操作不要的操作数，都是请求错误。这几种情况在 `url.Values` 里读出来都是空字符串，而空字符串是根。

`Prefix` 为 `/v1/`，相对于 handler 被挂载的位置。挂到别处用 `http.StripPrefix`。

## 三、响应的形状

每个响应带两个头：

- `Remote-Fs-Protocol: 1` —— 标记这是本协议的服务端给出的答案。
- `Cache-Control: no-store` —— 一个被缓存住的答案读起来与当前的答案没有区别。

| 情形 | 状态 | 体 |
|---|---|---|
| `Stat` 成功 | `200` | `{"attr":{…}}` |
| `List` 成功 | `200` | `{"entries":[…]}`，空目录是 `[]`，绝不是 `null` |
| `Read` 成功 | `200` | 文件内容，`Content-Type: application/octet-stream` |
| 其余操作成功 | `200` | 空 |
| storage 报告了错误 | `422` | `{"errno":"<名字>","message":"…"}` |
| 操作不存在 | `404` | `{"message":"…"}` |
| 方法不对 | `405` | `{"message":"…"}` |
| 请求解析不了、请求体不完整 | `400` | `{"message":"…"}` |

`200` 是唯一的成功状态，`422` 是唯一承载 errno 的状态。errno 不编码进状态码 —— 状态码空间与 errno 空间不同构。

**每个响应都显式声明 `Content-Length`，失败的响应与空的响应也不例外。** 这是顶层设计第四节「响应体的分帧必须能报告自己提前结束」那条义务在这一侧的落地：一个以连接关闭为终点的响应体，被截断与完整无从分辨，因此不被接受。

**缺席与零值必须分辨得开。** `Stat` 响应里的 `attr`、以及每个目录条目里的 `attr`，都是可以在报文里缺席的字段，而缺席就是解码失败 —— 一个零值的属性读起来是「一个模式为 0、长度为 0、时间停在 1970 年的普通文件」，与一份合法的答案分辨不开。`entries` 同理：`null` 与 `[]` 相差两个字符，意思相反。只报告结果的那些操作反过来 —— 空就是它们全部的证据，因此答案里出现任何内容都说明这不是这次操作的结果。

目录条目的名字以原始字节编码（JSON 里是 base64）。文件名是任意字节序列，不是文本。

`Attr` 的 `mode` 是 Go `io/fs.FileMode` 的位布局。两个时间 —— `access_time` 与 `mod_time` —— 各是一个对象，`unix_sec` 是自 Unix 纪元起的整秒数，`nanos` 是该秒之内的纳秒数。单独一个纳秒数装不下这两个字段要承载的范围 —— `time.Time.UnixNano` 只在 1678-09-21 到 2262-04-11 之间有定义，范围之外的时间（零值的 `time.Time` 也在其中）会变成另一个看上去完全合理的日期，且没有任何东西标出它是错的。秒与纳秒合成一个对象而不是并排两个字段，是因为 `SetAttr` 的请求里每个时间都可以整个缺席，而两个各自可空的字段能互相矛盾。

`SetAttr` 的请求体是 `{"change":{…}}`，`change` 里每个属性都是可选的：缺席就是「这一项不改」。`change` 本身缺席则是解码失败 —— 一个什么都不点名的改动是合法请求（它在问这个节点还在不在），因此靠字段本身分辨不出报文是不是掉了内容，外面这一层对象才分辨得出来。

## 四、错误如何离开 server

`packages/storage` 持有一张 errno 与符号名之间的双向表，它同时是一个 storage 实现允许报出的 errno 的全集。它归契约而不归某一种传输：一个实现可以报出哪些错误，是契约的性质。

storage 返回错误时，请求处理从错误链里取出 `syscall.Errno`：

- 取得到，且在表里 → 用它的名字，`422`。
- 取不到，或者不在表里 → `EIO`，`422`，原始错误的文本放进 `message`。

**无法命名的失败一律是 `EIO`。** 挑一个最接近的名字，等于把一个不确定的失败说成一个确定的事实；而 `ENOENT` 一旦被这样说出去，上层会据以删除、重新生成或覆盖（R-ERR-1、R-ERR-2）。

## 五、写入路径

```
POST /v1/write ──▶ 读满请求体
                    ├── 读取失败，或字节数与 Content-Length 不符 ──▶ 400，不调用 storage
                    └── 完整 ──▶ storage.Write ──▶ 200 / 422
```

请求体必须先完整到齐，才允许落到 storage。一个提前结束的请求体不会让读取报错，而把到货的那一部分写进去，就是用文件自身的前缀替换掉这个文件，并回报成功。

`SetAttr` 走同一条路：整份 JSON 到齐并解得开，才调用 storage。只到一半的改动会把没到的那些属性留在原样，却回报整个请求已经执行。

内容替换的原子性由 storage 保证（R-CON-3），请求处理不参与。

## 六、storage 的位置

server 通过 storage 接口访问命名空间，不知道底下是什么。集成方注入自己的实现（R-INT-6），或使用随附的本地目录实现。

storage 必须履行的义务、十个操作的形状、路径规则与错误词汇，由 storage 接口定义，见顶层设计第四节。

## 七、server 不做什么

- **不缓存。** 命名空间的事实就在 storage 里。
- **不持有 client 的状态。** client 消失即消失。
- **不清洗路径。** 逐字交给 storage。
- **不做鉴权。** 且不做半套 —— 不留形同虚设的校验分支、不留默认放行的开关、不打印会让人误以为受保护的日志。理由见[第一个可用版本的范围](../../../.agents/notes/implemented/process/2026-08-19-mvp-scope.md)。
- **不打印任何东西，也不记日志。** 失败的原因随该次请求的响应返回。

## 八、部署形态

作为库嵌入集成方既有的 server，或作为独立二进制运行（R-INT-1、R-INT-4）。

作为库时：不注册信号处理、不写 stdout/stderr、不调用进程退出、包初始化不产生副作用（R-INT-2）。`packages/transport/httprest` 暴露的就是一个 `http.Handler`，监听、TLS、超时、路由前缀全部由集成方的 server 决定。

作为二进制时，`cmd/remote-fs-server -listen ADDR -dir DIR` 把一份 `localdir` 与一个 handler 接到一个监听地址上。收到 SIGINT／SIGTERM 后它停止接受新连接，并给在途请求 5 秒走完 —— 一次被切断的写入会让调用方无从判断它究竟发生了没有。

请求体与响应体都整份驻留在内存里，没有大小上限。
