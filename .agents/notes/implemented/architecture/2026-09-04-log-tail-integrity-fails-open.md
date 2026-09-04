# Agent Note: 日志尾不一致时拒绝打开

Status: implemented

## 问题

metastore 在同一 transaction 里修改树、追加 change，并更新 `logs.committed_position`。打开时若 committed position 与该 namespace 最新 surviving change 不一致，数据库已经丢失或伪造了本实现认为不可分割的持久状态。

仅换一个 log incarnation 会让现有 replica 重建，却不能证明 database 本身仍可信。它还会把“日志 invariant 已损坏”改写成“这是一段合法的新历史”，随后继续 trim 和服务；运维失去最接近损坏现场的证据。tree snapshot 也无法重建丢失 change 的顺序、时间与中间 rename/remove 语义。

## 决定

SQLite metastore 在 Open transaction 中要求 `logs.committed_position` 与最新 surviving change position 完全一致；空日志对应 position 0。任一方向不一致都以 `EIO` 拒绝打开。检查发生在 log trim、其它 startup maintenance 与 transaction commit 之前，不修改 incarnation，也不留下部分 maintenance。

incarnation 只在创建一条新日志历史时生成。一个已存在数据库的持久日志若不再是自身的连续记录，不能靠换 identity 转成可服务状态；必须由能够证明 tree/log 来源的带外恢复处理。

这项决定取代[元数据复制](./2026-08-27-metadata-replication.md)中“对账失败时换 incarnation、令 replica 重建”的旧 startup recovery。incarnation mismatch 仍然要求远端 replica 重建；改变的是 server 不再把本地 log corruption 包装成一次正常 mismatch。

## 备选方案

**对账失败时 mint 新 incarnation。** 现有 replica 会响亮重建，重建出的 snapshot 可以反映当前 tree。输在 server 同时掩盖了自身 durable invariant 的破坏，并继续从一份缺少历史的 database 服务；这违反 fail-closed 原则，也让事故调查失去原状态。

**从当前 tree 合成缺失 changes。** 可以让 committed position 追上。输在一份最终 tree 不能证明中间发生过哪些 create/remove/rename，也不能恢复原 position 与时间；合成记录会把猜测写成历史事实。

**只拒绝 `committed_position` 超过 newest change。** 覆盖最直观的“tail change 被删”场景。输在 newest change 超过 committed position 同样不可能由合法 transaction 产生；放过任一方向都会把一个未知数据库形状当作可恢复差异。

## 后果

- 日志丢行、tail 被篡改或 committed position 被篡改都会阻止 namespace 启动；不会产生新的 incarnation，也不会先执行 trim。
- replica 只把来自健康 server 的 incarnation mismatch 当成重建信号。server 自己无法证明持久日志时不会开始提供 snapshot 或 change stream。
- 自动可用性降低：过去可以通过全量 snapshot 继续服务的损坏现在需要带外诊断。换来的是数据库 corruption 不被转换成一段看似合法的新历史。
