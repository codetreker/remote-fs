# Agent Note: 首次复制订阅必须响应构建调用的取消

Status: proposed

## 问题

严重级别：P1。`replicated.New` 接受调用方 context，但 [build 使用独立的 s.lifetime 建立首次订阅](https://github.com/codetreker/remote-fs/blob/252259f1b89682a5b2acf54004f0fe8b8677528d/packages/storage/replicated/follow.go#L23-L37)。订阅返回前没有路径把调用方取消传给 HTTP 请求；[流式请求清除 HTTP 总超时](https://github.com/codetreker/remote-fs/blob/252259f1b89682a5b2acf54004f0fe8b8677528d/packages/transport/httprest/subscribe.go#L416-L425)，因此等待响应头可以超过构建调用的 deadline。

在该提交上，HTTP 服务收到订阅请求后持续不返回响应头；客户端的 HTTP timeout 与 silence limit 均为 100 毫秒，`New` 使用 50 毫秒 context deadline。deadline 到达后再等待 500 毫秒，`New` 仍未返回；只有服务端放行响应头后才退出。阻塞式冷挂载无法由调用方结束，妨碍 [R-INT-1 的 package 集成与 R-WS-4 的冷挂载可用性](../../../../docs/spec/requirements.md)。取消语义来自现有 context API，不新增独立的挂载时间上限。

## 提案

在构建成功前，让首次订阅的建连和握手响应调用方取消；构建成功后，持续订阅由 storage 生命周期持有。明确交接点，保证调用方在 `New` 成功后取消原 context 不会切断正常订阅。

本项补足[元数据复制](../../implemented/architecture/2026-08-27-metadata-replication.md)的初始化生命周期；[重建回放条件](2026-09-07-gate-rebuilt-replicas-on-replay.md)独立验收。

## 备选方案

未比较具体实现方案。直接把整条订阅永久绑到构建 context 会使构建后的取消中断运行中的副本；为长期 SSE 恢复有限的 HTTP 总超时也会终止健康长连接。这两项生命周期约束必须同时保留。

## 验收标准

- HTTP 服务收到请求后一直不返回响应头；50 毫秒构建 deadline 到达后，`New` 在额外 500 毫秒观察窗内明确失败并取消在途请求，无需服务端放行。
- 覆盖在请求发出前、等待响应头、首次订阅帧及快照构建期间取消；失败后不遗留 goroutine 或连接。
- `New` 成功后取消原调用 context，远端后续变更仍被副本应用；调用 `Close` 则停止订阅并释放其资源。

## 风险

延后的是可取消的初始化保证，冷挂载及嵌入方关闭流程可能无限等待。构建结束与取消同时发生时容易错误转移所有权；修复必须明确成功、取消与失败中谁负责关闭连接，避免重复关闭或遗留长期订阅。
