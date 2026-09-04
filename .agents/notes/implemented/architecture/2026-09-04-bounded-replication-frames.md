# Agent Note: 复制流在 payload 分配前受 frame budget 约束

Status: implemented

## 问题

快照与变更流原本只限制每页 row count。log incarnation、change 与 snapshot row 各自含有可变长度 identity、name、source name 或 object key；条数有限不能限制 initial frame、单行或一页的 byte allocation。handler 若在 metastore 已经返回完整 identity/page 后才检查 encoded frame，超限 payload、并发 frame production 与 admission waiter 仍可无界占用嵌入进程。

流的单次 frame、所有流同时持有的中间 representation、等待生产名额的 goroutine，以及长期占用 connection 的 subscription 都必须分别有界。client 也必须在 JSON decode 前限制收到的 frame，否则 server 的上限不是 end-to-end contract。

## 决定

handler 为 change-log incarnation 使用一个流与非流式 response 共用的上限：`min(256, (MaxBodyBytes-256)/6, (MaxFrameBytes-256)/6)`。`256` 是 protocol-wide identity ceiling，减去的 256 bytes 保留 JSON/frame envelope，六倍系数覆盖每个 UTF-8 byte 的最坏 JSON escaping。同一个实效值传给 `Log.Incarnation` 和 `Log.Barrier`，使 stream start 与 mutation success body 不会对同一日志身份给出不同可表示范围。

`metastore.Log.Incarnation(ctx, maxBytes)` 先读取 UTF-8 byte length，超过 handler-wide allowance 时在加载或复制 identity 前以 `EFBIG` 拒绝，stream 尚未开始。`Log.Barrier(ctx, maxBytes)` 对 mutation response 执行同一顺序。`Log.Since(ctx, after, limit, *ChangeResult)` 与 `Snap.Next(ctx, limit, *RowResult)` 分别填充 caller-owned bounded result：producer 先读取 scalar metadata 和 variable payload lengths，按调用方的 wire charge 取得 reservation，确定能装下后才加载 name、from-name 与 content key。reservation commit 使用 `bytes.Clone`/`strings.Clone` 取得字段所有权，pointer metadata 也复制到 result，并把 access/modification time 归一到 UTC 而不改变 instant。

一个 change/row 能装进空 frame、但装不进当前非空 page 时留给下一页；单项连空 frame 都装不下时以 `EFBIG` 失败。producer error、长度不一致或未完成 reservation 会使整个 result 失败，partial page 不可读取。这三项是 `Log`/`Snap` 唯一的 identity/page 读取接口，没有可绕过 caller byte budget 的 count-only overload；第三方实现同样承担“先 reservation、后 payload allocation”的义务。

handler 的默认 `MaxFrameBytes` 是 8 MiB，适用于 encoded change、snapshot-row 与 initial stream frame。start/change production 在每条 subscription 内串行：一条 stream 同时至多持有一个 bounded page/frame 的三份 representation，不取得全局 frame gate，也不因另一条慢 subscription 或 snapshot 等待共享名额。`httprest.Limits.MaxSubscriptions` 默认 64 且饱和时立即 `EAGAIN`，因此 event/start aggregate ceiling 由 `MaxSubscriptions * 3 * MaxFrameBytes` 推导，默认是 1.5 GiB；两项配置在 handler construction 前受 finite maximum 校验，长期 change stream 不建立 frame waiter。

snapshot page production 使用独立的 shared admission。`MaxConcurrentSnapshotFrames` 默认 16，`MaxInFlightSnapshotFrameBytes` 默认 384 MiB，每项按 `3 * MaxFrameBytes` 预留，`MaxWaitingSnapshotFrames` 默认 64。waiter queue 满时立即 `EAGAIN`；已经排队的 producer 由 snapshot request context 与 `SnapshotDeadline` 界定，等待期间 delivery loop 继续发送 keepalive，取消、deadline 与 shutdown 都释放 accounting。snapshot 原有的总并发、deadline 与 page-row limits 继续独立生效；row count 决定每次最多查多少记录，frame budget 决定这些记录最多占多少 bytes。固定 control frame 由 subscription/snapshot 数量界定，fault detail 在编码前截断。

每个 SQLite snapshot 还会在 page 之间保留 cursor name。该独立 aggregate ceiling 由 `Limits.Snapshots * MaxFrameBytes` 的 checked product 推导；默认 8 个 snapshots 与 8 MiB frame 得到 64 MiB，不能被 snapshot-page admission 的 384 MiB 替代。

subscription ceiling 限制 attached change streams，不等于整个 process 的 TCP connection 上限；独立 command 的 accepted connection 与 header/idle lifetime 由[HTTP connection 上限](./2026-09-04-standalone-http-connection-limits.md)单独管理。

client 的 `DialOptions.MaxFrameBytes` 独立可配，默认同为 8 MiB；frame reader 在保留和 decode 完整 JSON 前执行上限，server/client 配置不一致时响亮失败，`cmd/remote-fs -http-max-frame-bytes` 暴露该值。`cmd/remote-fs-server` 以 `-http-max-frame-bytes`、`-http-max-subscriptions`、`-http-max-concurrent-snapshot-frames`、`-http-max-in-flight-snapshot-frame-bytes` 与 `-http-max-waiting-snapshot-frames` 暴露 server limits，只允许 Blob/local-store 这两种 metastore-backed source 使用；普通 directory 没有复制流，显式给出这些 flags 在接触 storage 前被拒绝。

这项决定补全[元数据复制](./2026-08-27-metadata-replication.md)的 frame-production 资源边界；那份 note 继续拥有 subscribe-before-snapshot、stream liveness、mutation barrier 与 replica trust state。

## 备选方案

**只保留 SnapshotPage/EventPage 的 row count。** 查询与 wire 循环最直接。输在 variable field 没有长度上限，一个 row 就能越过整个进程预算；并发 16 条流时 row count 仍不能回答 aggregate bytes。

**metastore 先返回完整 page，handler 编码前再检查。** 不改变 `Log`/`Snap` interface。输在超限 name/content 已经被 SQLite 扫进 Go allocation，拒绝发生在资源上限之后；这与[有界地产生 Read 与 List 响应](./2026-09-04-bounded-read-and-list-responses.md)修正的缺陷相同。

**只在 client 限制 frame。** 能保护 mount 进程。输在 server 已经完成 metastore materialization、wire conversion 与 JSON encoding，恶意或失控订阅者仍能让嵌入进程无界分配。

**所有 stream 共用一个 frame gate。** 能从一个 operation/byte/waiter 三元组直接给出全局 aggregate。输在一个慢 snapshot 或饱和 waiter 会阻塞不相关的 change stream，破坏事件时延与并发 bulk transfer 无关的性质；change stream 按 subscription isolation 派生 ceiling，只有 snapshot pages 共用可等待的 gate。

## 后果

- row count 与 single-frame bytes 约束所有复制 payload；change/start aggregate 从 subscription ceiling 派生，snapshot cursor aggregate 从 snapshot count 派生，snapshot page 的 operations/aggregate/waiters 另有独立上限。调节 `MaxSubscriptions` 会同比改变 event/start aggregate，但不会占用 snapshot gate。
- 一个 payload 太大的 change/row 会使对应 stream 响亮失败；partial change page 或 snapshot page 不会被 replica 当成完整事实。
- server 与 client 默认同意 8 MiB，但不是协商协议；任一侧更小都会按自己的上限拒绝，运维必须让需要传输的最大单项装得下。
- snapshot page 按三倍 ceiling admission，会低估小 page 的可并发量，换取不依赖 allocation 时序的 384 MiB shared bound；page 间 cursor 另有默认 64 MiB derived bound。change/start 没有 shared waiter；其默认 1.5 GiB aggregate 是 64 条 subscription 各自最多保留三倍 8 MiB 的保守乘积。
- bounded `Incarnation`/`Since`/`Next` 增加第三方 metastore 的实现义务：identity/payload lengths 必须在读取 variable payload 前可得，result ownership 必须独立于数据库 driver 的复用 buffer。
