# Agent Note: 缩短文件在确定生效后释放配额

Status: implemented

## 问题

[R-WS-5](../../../../docs/spec/requirements.md) 要求拒绝会超出 volume 上限的写入。[limited.Write 在调用底层 Write 前就 reserve 负增量](https://github.com/codetreker/remote-fs/blob/252259f1b89682a5b2acf54004f0fe8b8677528d/packages/storage/limited/limited.go#L513-L541)，使尚未提交的缩短提前增加其它写者可用的额度。缩短失败后的回滚不能撤销已经成功的另一笔写入。

在该提交上，以 localdir 包装一个可阻塞并失败的 Write，配额为 4096 字节，初始 `a` 占满 4096 字节。将 `a` 缩短为零，在底层写入前阻塞；随后写入 4096 字节的 `b`，再放行 `a` 并使其返回 `EIO`。`b` 成功，`a` 保留原内容，实际用量达到 8192 字节。

## 决定

增长先预留，缩短后释放。尚未确定生效的缩短继续占用原额度；确定未生效的失败或取消保留原用量，成功只释放一次。增长确定未生效时只退回它自己的预留，不改变其它已经提交的内容所占额度。等长写入不预留或释放字节，超额 volume 仍可缩短文件。[活文件句柄](../architecture/2026-09-08-live-file-handles.md)使 `File.WriteAt` 与 `File.Truncate` 逐次同步返回发布结果，配额拒绝由相应调用报告，关闭负责引用清理。

具有 `CheckPublicationAccounting` 的原生 backend 在最终发布处提供实际新旧大小与效果。Applied 按实际效果结算，即使后续确认返回错误也保留已经发生的缩短；NotApplied 退回增长预留，缩短不释放。volume 效果不明，或结算、撤销被标记为 `IsPublicationAccountingUncertain` 时保留保守账本，并使 Space、修改与 Recount 失败，直到重新打开。已知文件没有改变也不能证明不确定的计费仍可使用，主错误与清理原因一并保留。

原生集成与[文件锁](../architecture/2026-09-07-file-locks.md)共用最终转换：上传和暂存不能提前释放额度，过期 proof 在最后权限判定处被拒绝，原字节与收费保持一致。保留句柄的最终发布还检查引用与会话是否有效，仍存在的物理 pin 不能延长已退役引用的写入资格。随附的 local-store 与 Azure Blob 组合都由 SQLite 提供最终发布计费；[移除宿主目录后端](../simplification/2026-09-08-remove-the-host-directory-backend.md)保留这项集成与 `limited` 包装。[目录改名中的配额记账](2026-09-07-keep-quota-accounting-stable-across-directory-renames.md)将原生发布计费收紧为构造条件，未提供能力的有界 backend 也明确拒绝。本决定继续拥有[容量上限](../architecture/2026-08-21-space-limit.md)的增长预留与缩短结算规则。

unlink 或覆盖名字后仍有引用的节点成为 detached，保留内容继续收费。终止引用先关闭后续发布资格，再排空已接纳操作，最后释放原生保留引用并结算已回收节点的额度。已知清理失败保留收费，未知效果仍按上述规则隔离账本；重复关闭不重复退款。`limited` 初始化与 Recount 使用包含 detached 字节的权威 `Usage`，目录树变空不能证明容量已释放。对象物理清扫与这笔逻辑结算的关系见[文件句柄设计](../../../../docs/design/server/file-handles.md)。

## 备选方案

**先释放，失败后加回。** 看起来使计数始终接近预期新长度，却把尚未发生的缩短当成可借出的容量。加回时可能已有其它写入成功，不能删除它们来补账。

**串行执行整个 volume 的写入。** 可以让另一写者等到缩短结果确定，但慢上传会阻挡无关文件，违背 R-CC-2。结算规则只约束额度与最终效果，不把整个写入包进全局互斥区。

**对所有错误都退回预留或恢复旧账。** 对已生效后确认失败的原生修改会制造错误计数，对未知结果也会伪造可用额度。明确的效果分类允许已知结果准确结算，无法判定的状态停止继续作答。

## 验证

[结算回归](../../../../packages/storage/limited/write_settlement_test.go)在真实 SQLite volume 与内存对象存储之上保留原来的 4096 字节交错：缩短暂停时 Used 仍为 4096、Avail 为零，竞争的 4096 字节写入返回 `EDQUOT`；缩短失败后原内容与实测、报告用量均为 4096。成功路径随后只释放一次。取消、等长写入、失败增长的预留退回，以及超额 volume 缩短均由 package 用例覆盖。

[路径写入的过期强占有回归](../../../../packages/storage/limited/lease_quota_test.go)使用 `sqlite.OpenLocking` 和可暂停 Put 的内存对象存储，经 `Storage.Write` 将 4096 字节文件暂存缩短为 6 字节。SQLite 不设置内层额度，全部收费由外层 `limited` 负责；暂停期间一字节的竞争增长也被拒绝。强占有到期后恢复上传得到 `StaleGrant`，原内容、Used 与 Recount 仍为 4096；随后一次有效缩短释放 4090 字节，恰好容纳相应增长，再多一字节仍为 `EDQUOT`。

上述用例属于 `limited` package 的普通与 race 测试。[句柄计费用例](../../../../packages/storage/limited/files_test.go)另外覆盖 unlink 后继续收费和增长、最后关闭只退款一次、到期清理使用独立生命周期计费、清理失败保留额度，以及初始化和重数包含 detached 字节。原生计费覆盖 Applied 后确认错误、NotApplied、未知效果、嵌套预留撤销与修改路径的结算失败，验证真实内容、保守计数、原错误及后续不可用状态，测试入口见[测试策略](../../../../docs/testing.md#最终发布与观察次序)。

## 后果

等待提交的缩短暂不提供可用额度，因为原字节仍由存储持有，即使节点已经没有名字。成功缩短或确定完成最后引用清理后，其它写者可以使用释放的额度；确定未生效的失败不要求事后重算或撤销另一写者的成功结果。

原生效果分类增加了计费与 backend 的集成义务，换取确认失败时仍按真实效果结算。保留句柄还要求独立的清理计费与权威用量，避免请求取消或目录树中缺少名字提前释放字节。结果未知时保留的额度可能偏多，代价是停止服务并重开核对，不能以推测的余量继续接受写入。不透明第三方 backend 由构造检查拒绝；带外修改造成的计数漂移仍不在受支持用法之内。
