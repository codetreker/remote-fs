# Agent Note: 有界地产生 Read 与 List 响应

Status: implemented

## 问题

HTTP 的 wire body 上限若只在 `storage.Read` 返回完整 `[]byte`、`storage.List` 返回完整 `[]Entry` 之后检查，超限结果仍会先占用 backend 与 handler 内存。限制并发 operation 数也不能约束一次巨大 listing；先 `Stat` 再 `Read` 则无法排除两次调用之间的替换。R-INT-3 要求预算在产生结果的一侧生效，并同时约束并发 retention 与等待者。

这个边界跨过 storage contract、object backend、metastore enumeration、HTTP server 与 client。任何一层仍先构造无界中间结果，都会把 allocation 移到别处，不能形成 end-to-end 上限。

## 决定

`storage.Storage` 保留通用的完整结果 API，另提供 `storage.BoundedStorage` capability：

- `CheckBounded()` 在 server 接受请求前确认全部依赖都支持有界生产；`httprest.NewHandlerWithOptions` 拒绝没有这项 capability 或检查失败的 storage。
- `ReadBounded(ctx, path, maxBytes)` 要求正预算，在分配完整 payload 之前以 `EFBIG` 拒绝超限文件。
- `ListBounded(ctx, path, result)` 逐 entry 向 `storage.ListResult` 预留并提交。调用方给出基于 index、name length 与 attrs 的 complete-result byte charge；实现先计费，确定可容纳后才加载或保留 name。任一步失败都会使整个 result 进入 failed 状态，`Entries()` 不暴露空结果或部分前缀，因为省略的名字不能被解释成不存在。

object-store namespace 在 metastore 记录的 size 超限时先拒绝，再要求 backing `Objects` 实现 `GetBounded`。localdisk 先验证 envelope、store ID、key 与 declared length，在 payload admission 和 allocation 之前检查预算；Azure 先检查 service-declared length；memory 在 map lock 下检查 retained slice 长度。listing 通过 `metastore.BoundedLister` 按 bytewise name order 枚举；SQLite 先扫描 name length 与 fixed-size attrs，取得 `ListReservation` 后才把 name BLOB 复制进 Go memory，不建立完整 child slice。

localdir、limited、replicated、localstore 与 HTTP client 都传播相同 capability。wrapper 的 `CheckBounded` 必须验证下层；HTTP client 用自己的 wire 上限与调用方预算的较小值取得响应，随后把 decoded entries 逐项加入调用方的 `ListResult`。

server 与 client 各有独立的 response admission，限制 concurrent operations、aggregate retained bytes 和 bounded waiters。server 的 Read/List 在调用 storage 前按 `4 * MaxBodyBytes` 预留，覆盖 storage result、wire conversion 与 encoded body 可能同时存在的保守峰值；其它 fixed-result non-stream operation 按 `MaxBodyBytes` 预留。client 在发出任意 non-stream request 前统一按 `4 * MaxBodyBytes` 预留，使 raw body 与 decoded representation 可以同时留存。context cancellation 会释放等待；饱和且 waiter 已满时以 `EAGAIN` 响亮失败。每个 wire body 本身仍受 `MaxBodyBytes` 限制。

这些是 handler 已经接收 request 之后的 result bounds；独立 command 在 handler 之外另有[accepted connection 与 header/idle lifetime 上限](./2026-09-04-standalone-http-connection-limits.md)。

change/snapshot stream 不计入 non-stream response admission，也不由 `MaxBodyBytes` 约束；它们的 metastore page production 与 wire allocation 由[有界复制 frame](./2026-09-04-bounded-replication-frames.md)单独限制。

## 备选方案

**只在 handler 增加 response semaphore。** 能限制几个结果同时产生。输在一次 listing 仍可无界分配，且 waiter 本身没有数量边界；它减少概率，不建立单次结果上限。

**调用 `Stat` 预检文件长度，再使用现有 `Read`。** 对静止文件改动较小。输在 `Stat` 与 `Read` 之间存在替换竞争，backend 也可能返回与 metadata 不同的长度；directory listing 没有对应预检。

**只在完整结果返回后检查 wire size。** 保持 storage API 不变。输在超限 buffer 已经存在，多个并发 response 也已经占用 aggregate memory；wire refusal 来得太晚。

**把共同接口改成 streaming read 与 paginated list。** 能进一步降低单次 buffer，并为很大的文件和目录提供增量交付。输在中途失败需要新的完整性 framing，pagination 还必须定义并发修改下的一致观察、cursor 生命周期与恢复语义。当前调用方仍需要完整 POSIX-style 结果；caller-owned bounded builder 在不引入可恢复 cursor 的情况下先兑现确定内存上限。

**让 backend 自行选择是否有界。** localdisk 与 localdir 可以优化，第三方实现保持不变。输在 server 无法在启动时证明其依赖，第一次大请求才发现 capability 缺失；R-INT-6 的契约也无法验证共同保证。

## 后果

- 超限 Read 在完整 payload allocation 前失败；超限 List 在第一次放不下的 entry 处失败，并使 result 不可读取。调用方不能把空 slice 或已经积累的 prefix 当作成功 listing。
- HTTP handler 的构造阶段成为 capability gate；一个只有旧 `Storage` 方法的第三方实现仍可供其它本地调用方使用，但不能被嵌入 server。
- server 与 client 的 concurrent response、aggregate bytes 和 waiters 都有独立默认值与配置，不再借用 request-body admission 表达另一类资源。
- `ListResult` 的 charge 由调用方定义，因此 storage 不依赖 JSON 形状；代价是每个 transport 必须提供与自己 retained representation 一致的 name-length/attribute 计费函数，并用测试钉住边界。reservation 与 commit 必须成对，未提交 reservation 同样使结果失败。
- 成功结果仍完整驻留在内存里。默认 1 GiB body 上限、server Read/List 的四倍 reservation、server fixed-result operation 的一倍 reservation，以及 client non-stream call 的四倍 reservation 共同给出 ceiling，没有提供 streaming throughput、range read 或 paginated list；这些仍由[storage 操作词汇](../../proposed/architecture/2026-08-19-storage-operation-vocabulary.md)拥有。
- 四倍 reservation 会低估小 Read/List 与 client response 的可并发量，尤其是实际只保留一种 representation 时；server 的 fixed-result operation 不支付这项额外倍数。这个吞吐取舍换取无需依赖 allocation 时序的 aggregate bound。
