# Agent Note: 副本重建必须追上回放后才能恢复查询

Status: implemented

## 问题

严重级别：P1。[R-CON-1、R-ERR-2](../../../../docs/spec/requirements.md) 禁止超过可见期限仍提供旧元数据，或把尚未确定的存在性回答成不存在。[fill 在快照完成后立即调用 seeded](https://github.com/codetreker/remote-fs/blob/252259f1b89682a5b2acf54004f0fe8b8677528d/packages/storage/replicated/follow.go#L54-L83)，此时订阅中积累的快照之后的事件尚未回放。

在该提交上，先建立可用副本，再断开变更流并令续订被拒以触发全量重建。分别阻塞新快照的 `done` 帧与订阅的 `change` 帧；快照到达 `done` 后，通过另一客户端创建 `late.txt`，等待 1200 毫秒，只放行 `done`。回放仍受阻时，副本的 `Stat("late.txt")` 已从 `EIO` 转为 `ENOENT`，把已提交文件报告为不存在。


## 决定

首次构建与全量重建保持同一顺序：先建立订阅，再灌入快照；客户端观察到 snapshot EOF 并关闭这份 snapshot 的 HTTP 响应后，通过纯读取的 `Checkpoint` 取得一个新的已提交位置，再由唯一的 reader 沿原订阅应用到该固定目标，最后恢复查询。快照位置只说明捕获的树包含什么，不能单独宣布积压已回放。

`GET /v3/checkpoint` 复用 `Log.Barrier` 原子读取 incarnation 与 committed position，以现有 `httprest.MutationBarrier` 返回。它遵守普通请求／响应预算，先按 `storage.OpReplicationCheckpoint` 授权，再读取 Log，不产生 mutation、唤醒或新的订阅。snapshot position 不早于原订阅 opening tail，checkpoint 不早于 snapshot，且 checkpoint incarnation 与原订阅一致；snapshot 本身没有 incarnation 字段。

`ReplayTimeout` 是正数，默认十秒，从观察到 snapshot EOF 时开始，覆盖快照响应关闭、checkpoint 请求、本地 Complete 与固定目标回放。它不把整个快照传输算入这段预算，也不沿后续写入不断移动目标。Complete 与 Apply 确认成功时分别记录对应的已安装位置，即使对外读取仍保持 EIO；恢复查询以同一代树确实达到目标为条件，不能退回旧的可用状态。

构建期间，原订阅由 storage lifetime 的子 context 持有，并临时响应本次构建调用的取消。成功交接前解除并等待这个取消关联，随后确认可恢复查询；已完成构建后的调用方取消不能切断健康订阅。失败或关闭仍由原拥有者取消并关闭订阅，不增加并发 reader、客户端事件队列或第二条订阅。

本决定部分接续[元数据复制](../architecture/2026-08-27-metadata-replication.md)的快照后可用性条件；保留订阅先于快照、全量本地副本与套接字背压的取舍。具体接口与构建状态见[client 设计](../../../../docs/design/client/architecture.md)，验证分工见[测试策略](../../../../docs/testing.md)。

## 备选方案

**快照后再建立一条订阅，用它的 opening tail 作目标。** 可以得到较新的尾部位置，但需要第二份 subscription 与连接。配置只允许一条订阅时，合法的首次构建或重建就无法完成；checkpoint 不占用额外 stream 名额。

**关闭原订阅后重新续订。** 可以从快照位置读取保留历史，却放弃已经排入原连接的事件，并要求这些事件仍可从日志窗口再次取得。窗口变化可能触发另一次重建；保留正在提供这段历史的订阅避免这项额外依赖。

**从现有 Log 读取独立 checkpoint。** 增加一次有界 HTTP 读取，保留原订阅与其顺序，给 readiness 一个不会随未来写入移动的目标。它复用既有原子 Barrier，并在快照响应释放连接后发出请求，同时至多两条 HTTP 连接（其中一条为原订阅）仍可完成构建，不修改存储格式或引入新的日志机制。

## 后果

冷挂载与重建的不可用期包含 snapshot EOF 后的 checkpoint 交换和有限回放；超时、取消、流故障或无法证明同一代位置时明确失败。固定等待时间不能证明回放完成，不承担这项条件。

这封住快照传输期间已经提交、却在旧实现恢复查询时仍未应用的缺口。目标捕获之后的修改继续遵守原有事件流与确认规则；本决定不提供全局即时新鲜度证明，也不要求追逐不断前进的最新位置。

内部安装位置与公开可用性分开，使失败后的续订依据真实已提交的树继续推进。首次订阅的握手与取消交接保留[独立验收范围](../../proposed/bug-fix/2026-09-07-cancel-initial-replica-subscription.md)；回放阶段的取消与成功后持续订阅的断言，不代替尚未覆盖的订阅响应头等待场景。
