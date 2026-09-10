# server 角色的设计

本目录承载 **server 角色**的内部设计。

server 是 volume、保留文件与显式占有的权威持有者：将 storage 与对应锁服务配对，经 HTTP 暴露给多个 client。角色边界与跨角色契约由 [`../architecture.md`](../architecture.md) 定义。

## 本目录的规则

- **只写 server 内部。** 不重讲系统全貌；那是顶层设计的事。
- **不假设 client 的内部构造。** 引用 client 只能通过顶层设计定义的契约——client 内部怎么组织、怎么呈现 volume，与本目录无关。一旦这里出现「因为 client 会……」这样的推断，就意味着契约没写清楚，应当去补契约而不是在这里迁就。
- **不写需求、不写调研、不写思考过程。** 依 [`../README.md`](../README.md)。

## 文档

| 文件 | 内容 |
|---|---|
| [`architecture.md`](architecture.md) | server 的内部构成、组件职责、内部数据流 |
| [`file-handles.md`](file-handles.md) | 保留对象、同步区间修改、标准 advisory owner 与文件会话协议 |
| [`file-locks.md`](file-locks.md) | 显式 S/X 占有、有限授权与动作历史、原生发布排序、重启恢复和 backend 绑定 |
| [`sqlite-modules.md`](sqlite-modules.md) | SQLite 公开入口、内部组件、事务所有权与测试资源归属 |
| [`local-disk-object-store.md`](local-disk-object-store.md) | 随附本地持久 storage 的磁盘格式、身份与 WAL 见证、打开与恢复、容量和维护 |

server 内部若需进一步展开，在本目录增加文档，不影响 client 目录。
