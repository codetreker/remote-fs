# Agent Note: 保留日志不连续时拒绝打开

Status: implemented

## 问题

metastore 在同一 transaction 里修改树、追加 change，并更新 `logs.committed_position`。replica 把 surviving changes 当作从 `trimmed_through` 到 committed tail 的完整历史；其中任意一条缺失，后续事件仍可把 replica 推到 tail，却永久漏掉被删除的 mutation。

只比较 `committed_position` 与最新 surviving position 能发现尾部丢失，不能发现中间缺行。position 又由整个数据库共享的序列分配，同一个 volume 的相邻 change 之间合法地夹着其它 volume 的位置，因此数值不连续不是损坏，不能用 `position + 1` 判定。

仅换一个 log incarnation 会让现有 replica 重建，却不能证明 database 本身仍可信。它把“日志持久状态无法证明”改写成“一段合法的新历史”，随后继续 trim 和服务；tree snapshot 也无法重建丢失 change 的顺序、时间与中间 rename/remove 语义。

## 决定

每条 `changes` row 保存 `previous_position`，指向同一 volume 的上一条 change position。追加 change 时，在同一个 SQLite transaction 中读取当前 `committed_position` 作为 predecessor，显式分配新的全局 position，插入 row，再把 committed tail 推进到新 position。

保留窗口形成一条以两个日志标量锚定的链：第一条 surviving change 的 `previous_position` 必须等于 `trimmed_through`，每条后续 change 必须指向按 position 排序的上一条，最后一条 position 必须等于 `committed_position`。没有 surviving change 时，committed tail 必须等于 `trimmed_through`。任何 row 位于 trim boundary 之下、predecessor 不匹配、tail 不匹配或字段形状不合法，都以 `EIO` 拒绝。

`Open`、`Snapshot` 与 `ObjectStatus` 在暴露 volume 前验证完整 retained chain，完整工作受 `MaxIntegrityRecords` 限制。`Since` 不为每个 subscriber 的每一页重复扫描整个窗口：它通过索引读取 database state、sequence、log header、oldest/newest 与按需的 page anchor，再把 page row 数收紧到 anchor 之后剩余的 integrity-record budget。请求 limit 很大而日志很小时只按实际存在的 work 计费；anchor 已耗尽预算且 tail 尚未返回时以 `EFBIG` 拒绝。第一页 row 必须指向该 anchor，后续 rows 逐条指向本页上一项，并在到达 tail 时核对终点。工作量与实际页大小成正比，变长 payload 仍在 caller-owned budget 取得 reservation 后才加载。

一个 page 完全位于后续缺口之前时，其中每条事件都是有效历史，可以成功返回；读取跨到缺失 predecessor 的那一页时，`ChangeResult` 整体失败，不暴露该页已经填充的 prefix。stream 报错使 consumer 立即作废副本；普通 `EIO` 续订仍携带原 incarnation/position，会在同一缺口持续失败，不自动取得 snapshot。只有带外修复，或健康 server 后续给出的合法 incarnation/window rebuild boundary，才让 client 进入重新 seed 路径。

schema v2 没有 predecessor，不能把旧 retained rows 回填成已经被证明连续的历史。v2 迁移先验证旧 row 形状、tail、trim 与 sequence，保留全局 node/change 高水位，然后删除 retained changes、为每个 volume 生成新 incarnation，并把 committed/trimmed position 归零。已有 replica 通过 incarnation mismatch 重建；迁移后的第一条 change position 严格高于旧高水位。

incarnation 只在新建日志或完成这次 schema history boundary 时生成。当前 schema 的持久日志若不再是自身的连续记录，不能靠换 identity 转成可服务状态；必须由能够证明 tree/log 来源的带外恢复处理。

这项决定收紧了[元数据复制](./2026-08-27-metadata-replication.md)中的 startup reconciliation，并与[SQLite 持久身份使用显式高水位](../bug-fix/2026-09-07-persistent-sqlite-identities-use-explicit-high-water-marks.md)共同保护 change position：前驱链证明保留段没有缺行，高水位证明已用位置不会回退或复用。

## 备选方案

**只比较 committed tail 与最新 surviving row。** 能发现尾部 row 被删或额外插入。输在删除中间 row 后 tail 仍然相等，replica 会跳过 mutation 并继续推进。

**要求同一 volume 的 position 数值连续。** 不增加 schema 字段。输在 position 是数据库级全序；其它 volume 的合法提交会在本 volume 相邻 row 之间留下空洞，算术连续会拒绝健康数据库。

**为每条 change 保存前一条完整内容的加密哈希。** 能同时证明顺序与 row 内容，比 position predecessor 更强。输在需要定义稳定的 canonical serialization、迁移与每次 append 的额外摘要成本；现有 row 已有独立字段、storage-class 和语义校验，要补上的缺口是删除检测，前驱 position 足以表达这条结构。

**对账失败时 mint 新 incarnation。** 现有 replica 会响亮重建，重建出的 snapshot 可以反映当前 tree。输在 server 同时掩盖自身 durable invariant 的破坏，并继续从一份缺少历史的 database 服务；这违反 fail-closed 原则，也让事故调查失去原状态。

**从当前 tree 合成缺失 changes。** 可以让 committed position 追上。输在一份最终 tree 不能证明中间发生过哪些 create/remove/rename，也不能恢复原 position 与时间；合成记录会把猜测写成历史事实。

## 后果

- 删除 retained log 的第一条、中间一条或尾部一条都会由完整 startup/snapshot/status validation 或跨到缺口的 change page 发现；不会产生新的 incarnation，也不会先执行 trim。
- 完整链验证与一个 volume 的 retained log 大小成正比，受 `MaxIntegrityRecords` 限制。合法但超过 serving ceiling 的 volume 在 open、snapshot 或 status 时以 `EFBIG` 停止，必须显式提高上限；`Since` 保持每页 O(page) 工作。
- v2 升级会让现有 replica 重建并失去旧 retained resume window，但保留全局 position 高水位，任何旧位置都不会重新指向新事件。
- replica 只把来自健康 server 的 incarnation/window mismatch 当成重建信号。full-integrity snapshot 入口在暴露 rows 前拒绝损坏；page-local change stream 可能先交付缺口之前的完整 page，跨到缺口后失败并使副本不可用。它不会把普通 corruption `EIO` 自动升级成 snapshot rebuild。
- 自动可用性降低：原本可能通过全量 snapshot 继续服务的损坏需要带外诊断。换来的是数据库 corruption 不被转换成一段看似合法的新历史。
