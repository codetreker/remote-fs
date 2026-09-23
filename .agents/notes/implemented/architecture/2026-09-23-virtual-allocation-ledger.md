# Agent Note: 为内置 volume 建立虚拟分配账

Status: implemented

## 问题

[R-WS-5](../../../../docs/spec/requirements.md)要求容量报告与实际准入使用同一种权威口径。原有 SQLite 账本按普通文件的精确内容长度计费；该数字不能说明一个文件已分配多少空间。若 Windows 入口把内容长度向上取整后作为分配量报告，却继续按原长度收配额，两份各一字节的文件就能在 4096 字节的 volume 内声称共占 8192 字节。零字节也不能同时代表确实没有分配和后端无法回答。

已失去名字而仍由引用持有的文件、重启恢复与复制历史要求同一事实穿过所有节点视图。分配量若仅在 SMB 响应里计算，容量准入、FUSE 属性和其它入口会给出互相矛盾的答案。

## 决定

内置 SQLite authority 使用固定 4096 字节的虚拟 cluster。普通文件的分配量为内容长度向上取整后的 cluster 整数倍；零长度文件、目录和符号链接的分配量为零。这是 volume 的逻辑额度，不声称反映 Azure Blob 或宿主磁盘的物理块。`storage.Attr` 用 `AllocationSize` 与 `AllocationKnown` 携带可缺席的中立事实，已知零与未知分开。缺少该事实的第三方存储仍可供不依赖它的入口使用；依赖它的平台操作明确失败。

SQLite schema v10 为 authority 持久化每节点分配量和每个 volume 的 `allocated_used`，历史 change 保存可适用的同一事实。通用 Node 同时记录 `AllocationKnown`，replica 保留来源报告的已知值及其粒度，或保留未知。每次最终发布与引用回收在同一个权威事务中更新内容、节点分配量、精确 payload 账与分配总账。原有 `volumes.used` 和 `Usage()` 保持精确 payload 口径；内置配额准入以及 `Space.Used`、`Space.Avail` 使用分配总账，detached 文件继续收费。额外对象暂存和物理空间不足仍独立受资源预算与 `ENOSPC` 约束。

authority 的迁移在一个事务内为所有 volume 从存量节点和历史记录计算分配量，并重建每个 volume 的分配总账；旧 replica 行保持 NULL／未知，不从 Size 推断分配量。打开前的完整性检查拒绝负数、溢出、失配及不符合节点种类或 cluster 边界的值，包含 detached 节点。迁移后已经超过当前上限的 volume 保持可读，`Avail` 为零；不增加分配量的修改仍能使它回到额度之内。旧有内容不会因新账本而被删除或遮蔽。

HTTP v5 的每份 Attr 显式传输分配量与 known 标志，含直接查询、打开结果和目录条目；变更与快照的通用 Node 传输分配量及 known 标志；旧协议标记和路径不能把缺失字段解释成零。`AllocationReporting.CheckAllocationReporting()` 验证 FileSession 完整包装链保证已知分配量。SMB 在打开效果前检查能力，并对原子打开返回的属性再次核验；未知时明确失败。副本保留并验证来源的已知或未知事实；它不在本地提供 `Space`。FUSE 的块数投影与 SMB 的文件分配量均来自同一属性。强制完整的跨入口传递使任何一层都不能从 EOF 猜测权威分配状态。

[容量上限](2026-08-21-space-limit.md)继续拥有通用 `limited` 的精确 payload 计量与 `Space` 三值契约；`limited` 遮蔽所有返回属性中的底层分配事实，将其标为未知，且 `CheckAllocationReporting` 以 `EOPNOTSUPP` 拒绝。FUSE 与 SMB 在产生效果前拒绝这样的包装链。本决定细化内置 SQLite volume 的额度口径。[缩短文件](../bug-fix/2026-09-07-release-shrunk-quota-after-commit.md)和[原生发布计费](../bug-fix/2026-09-07-keep-quota-accounting-stable-across-directory-renames.md)的最终效果顺序同样用于分配量的增长与释放。

## 备选方案

**让内置 authority 按精确内容长度继续收费，只在平台响应时取整。** 这使 `AllocationSize` 与 `Space.Used` 所报告的两件事实互相矛盾，也容许已声称的分配量总和超过 volume 上限，因此未采用。通用 `limited` 仍按精确内容长度收费，但它不声称知道节点分配量。

**查询对象后端的物理分配块。** Azure Blob 没有与本地磁盘同义的文件 cluster，且相同逻辑文件可能经历压缩、包装与暂存。让平台身份依赖这类后端细节无法给出跨形态稳定的容量契约，因此未采用。

**让所有 storage 实现都必须提供分配量。** 第三方存储原有的内容、身份和容量义务不自动证明节点级分配事实；强加这一能力会使原本可用的接入整体失败。可缺席字段保留了能力边界，消费分配量的入口独立检查。

## 后果

同一个内置 volume 的节点属性、配额准入、`Space`、FUSE 与 Windows 响应具有一致的分配依据；分配粒度使跨 cluster 的增长一次消耗完整 cluster。内容长度仍由 `Size` 与 `Usage()` 精确表示。旧库迁移和在线完整性检查增加了逐节点及历史记录的验证成本；原有超额 volume 在释放足够额度之前不能再增加分配量。虚拟分配不是物理容量承诺，底层介质不足仍会使发布失败。
