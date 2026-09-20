# Agent Note: 等待 SQLite transaction 归还连接

Status: implemented

## 问题

`database/sql` 会在 transaction context 结束后异步回滚。`Tx.Rollback` 此时可以先返回 `sql.ErrTxDone`，而 driver rollback 仍占用底层 connection。若操作把 `ErrTxDone` 当成收尾完成，读取可以在 connection 仍占用 reader pool 时返回，写事务和 Replica 也可以提前释放 commit、health 或 replica gate；构造失败还可能在迁移仍使用 connection 时释放数据库的原生排他所有权。

这会破坏已有的关闭与所有权保证：调用方已经观察到操作结束，后续 `Close` 却仍遇到 active connection；更严重时，另一 opener 可以在旧 driver rollback 尚未结束时取得同一数据库的原生所有权。直接的 `ErrTxDone` 只能证明 transaction 已被标记为结束，不能证明 connection 已经归还。

## 决定

本决定部分取代[请求中断](./2026-08-22-eio-from-a-freshly-mounted-mountpoint.md)中以直接 `sql.ErrTxDone` 证明 transaction cleanup 已完成的前提；该 note 继续拥有 `EINTR`／`EIO` 的阶段分类。

运行期 transaction 先从所属 pool 显式取得 `sql.Conn`，再在该 connection 上以原有 context 调用 `BeginTx`。私有 transaction owner 保留这份 connection，统一收尾先调用 `Tx.Rollback`，再调用一次 `Conn.Close`；`Conn.Close` 会等待自动回滚归还 connection。已成功 Commit 的 transaction 也经过同一收尾，以释放绑定 connection。`BeginTx` 失败仍关闭已经取得的 connection。

这项所有权覆盖普通读取、snapshot、volume mutation、Replica Apply/Seeding 与启动 schema Prepare。读取结果、commit/health/replica gate、Seeding 生命周期及 constructor 的数据库所有权，都在绑定 connection 归还之后才结束。清理继续使用 transaction 原有 context，不屏蔽取消，也不重新执行事务。

`Conn.Close` 只执行一次，其错误被保留。只有 connection 已无错误归还时，直接的 `sql.ErrTxDone` 才能按[请求中断](./2026-08-22-eio-from-a-freshly-mounted-mountpoint.md)的规则结合原 context 解释为自动回滚；独立 close failure 保持 cleanup failure，不能被取消原因覆盖。mutation、Replica 和 schema Prepare 沿用既有的未知 commit、durability failure、poison 与 fencing 规则。

这个决定落实[本地持久对象存储](../architecture/2026-09-04-local-disk-object-store.md)要求的 connection 与 lifetime ownership，并保持 [SQLite 内部模块](../architecture/2026-09-09-sqlite-internal-modules.md)对运行期 transaction 的归属。[副本写者进度](./2026-09-07-let-replica-writers-progress.md)继续拥有 replica gate 的公平与顺序；本决定只明确 gate 释放前 transaction connection 必须已经归还。

## 备选方案

**继续使用 `DB.BeginTx`，看到 `ErrTxDone` 后轮询 pool 状态。** pool 统计无法指出是哪一份 transaction 持有 connection，别的并发操作也会改变统计；轮询不能构成精确的资源归还证明。

**为 transaction cleanup 使用 `context.WithoutCancel`。** 这会改变请求取消和 deadline 的既有行为，也不能处理 Commit 后释放绑定 connection、`BeginTx` 失败或独立 close failure。原 context 保持不变，由显式 connection ownership 等待自动回滚完成。

**让 Store.Close 重试或容忍 active connection。** 操作已经返回之后再重试关闭，无法恢复提前释放的 gate 或原生所有权，也会把 lifecycle 缺陷转嫁给调用方。操作本身必须在宣告结束前归还它拥有的 connection。

## 后果

每个 transaction 多一次显式 `sql.Conn` acquisition，并由统一收尾执行一次 connection close。取消中的操作可能比 `Tx.Rollback` 返回更晚结束，因为它会等待 driver rollback 真正完成；这段等待正是保持 pool、gate 和原生所有权所需的边界。

真实 modernc rollback gate 用例通过普通读取、`Store.Create`、`Replica.Apply`、Replica Seeding 和 `PrepareConfigured` 的实际入口触发取消或 deadline，并核对 connection 归还前操作、gate 与 ownership 不结束。成功的 `Seeding.Complete` 另在 connection close 处暂停，验证完成结果不会越过资源归还。其它用例覆盖 snapshot 的自动回滚分类、`BeginTx` 失败、Commit 后单次清理，以及独立 `Conn.Close` failure 的错误保存。迁移 SQL、schema version、事务 context、提交顺序与外部错误分类保持不变。
