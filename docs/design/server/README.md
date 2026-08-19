# server 角色的设计

本目录承载 **server 角色**的内部设计。

server 是命名空间的权威持有者：持有一份 storage，经 RPC 暴露给多个 client，并记录与分发变更。角色边界与两条跨角色契约由 [`../architecture.md`](../architecture.md) 定义。

## 本目录的规则

- **只写 server 内部。** 不重讲系统全貌；那是顶层设计的事。
- **不假设 client 的内部构造。** 引用 client 只能通过顶层设计定义的契约——client 怎么缓存、怎么挂载，与本目录无关。一旦这里出现「因为 client 会……」这样的推断，就意味着契约没写清楚，应当去补契约而不是在这里迁就。
- **不写需求、不写调研、不写思考过程。** 依 [`../README.md`](../README.md)。

## 文档

| 文件 | 内容 |
|---|---|
| `architecture.md` | server 的内部构成、组件职责、内部数据流 |

server 内部若需进一步展开，在本目录增加文档，不影响 client 目录。
