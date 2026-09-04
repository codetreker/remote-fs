# Agent Note: 独立 server 有界持有 HTTP connection

Status: implemented

## 问题

`httprest.Handler` 能限制 request/response body、stream frame、subscription 与 snapshot，却看不到 listener 已经接受但尚未进入 handler 的 connection。Go `http.Server` 默认不限制 accepted connection 数，header 可以无限慢地到达，keep-alive connection 也可以无限期等待下一份 request；每一条都会占用 file descriptor、goroutine 与 socket memory。

长生命周期 change stream 与 snapshot 又使全局 request/write timeout 不适用。一个为了赶走慢 header 而设置的 `WriteTimeout` 会按固定 wall clock 切断健康的 SSE；snapshot 已有自己的 deadline，普通 handler 也由 request context 与 storage contract 管理。独立二进制必须限制它自己拥有的 listener 与 HTTP lifecycle，同时保持 `httprest` 可嵌入且不替宿主决定网络策略。

## 决定

`cmd/remote-fs-server` 在 raw listener 外包一层 accepted-connection admission。默认最多 256 条 active connection；名额用尽时在调用底层 `Accept` 之前等待，已有 connection 的一次性 `Close` 归还名额。admission state 只与 configured ceiling 成正比，不为任意到达的 connection 建无界队列；shutdown 关闭 listener 并唤醒等待中的 `Accept`。`ConnState` tracker 记录尚未进入 handler 的 `StateNew` connection；shutdown 开始时它永久转入 stopping，关闭当前记录和随后抵达的 StateNew connection，再进入 `http.Server.Shutdown`，因此 partial header 不依赖自身 timeout 才能退出。

独立 server 默认设置：

- `ReadHeaderTimeout = 2s`，限制一份 request header 到齐所需时间；
- `IdleTimeout = 60s`，限制 keep-alive connection 等待下一份 request；
- `ReadTimeout = 0`、`WriteTimeout = 0`，不对整个 request/response 或无终点 change stream 设置 wall-clock deadline。

header timeout 与 idle timeout 必须为正，connection ceiling 必须有限且为正。metastore-backed source 至少需要两条 connection，因为 cold mount 会保持 change subscription，同时取得 snapshot；普通 directory source 可以把 ceiling 设为一。

CLI 以 `-http-max-connections`、`-http-read-header-timeout` 与 `-http-idle-timeout` 暴露三项设置，并在 listener/storage 打开前验证。connection、subscription/snapshot/[stream frame](./2026-09-04-bounded-replication-frames.md) 与 [non-stream response](./2026-09-04-bounded-read-and-list-responses.md) admission 互相独立；最小的 ceiling 先成为实际并发上限。

`packages/transport/httprest` 继续只提供 `http.Handler`。嵌入方拥有 listener、TLS、accepted-connection admission、`ReadHeaderTimeout`、`IdleTimeout` 与其它 `http.Server` lifecycle；使用默认 Go server 而不配置这些资源意味着宿主自己接受相应的无界风险。

## 备选方案

**只限制 handler 并发。** 可以约束已经解析完成的 request。输在 slow header、idle keep-alive 和等待进入 handler 的 accepted connection 已经消耗 fd/goroutine，handler semaphore 到得太晚。

**设置全局 `ReadTimeout` 与 `WriteTimeout`。** 一组 timeout 覆盖所有阶段。输在 change stream 没有自然终点，健康订阅会被 `WriteTimeout` 定期切断；大 snapshot 的生存期已经由 snapshot deadline 管理，再叠一项 request-wide deadline 会让两个不相干的策略竞争。

**达到 connection ceiling 后继续 Accept，再立即关闭。** 不阻塞 accept loop。输在仍先分配一条 process fd 与内核 connection state，突发流量可以在 close 之前越过 ceiling；在调用底层 `Accept` 前等待才使上限成立。

**把 listener 与 timeout 放进 `httprest.Handler`。** 能让每个调用方自动取得相同防护。输在 handler 本来就是可嵌入的 `http.Handler`，不拥有 TLS、routing、listener 或宿主的其它 endpoints；替嵌入方创建 server 会破坏这条库边界。

## 后果

- 独立二进制的 accepted connection、slow-header lifetime 与 idle keep-alive lifetime 有明确上限。默认 header timeout 是 2 秒；即使配置为长于 5 秒 shutdown grace，permanent-stopping StateNew tracker 也会在 Shutdown 前主动关闭已有和迟到的 partial-header connection。
- 256 是 deployment default，不是协议能力。低于实际 mount/stream/request 并发会让 accept loop 等待；metastore-backed 形态拒绝无法同时容纳 subscription 与 snapshot 的单 connection 配置。
- `WriteTimeout = 0` 保留长期 stream，但不等于 write 永远没人管：change stream 由 keepalive/silence 检测，snapshot 由 deadline，shutdown 会终止 stream 并排空 admitted handler。
- 嵌入方不会自动继承 command 的 listener wrapper 或 timeout。它获得可组合性，也承担显式配置这些资源的义务。
