# Agent Note: 新句柄不得读到旧句柄填入的页缓存

Status: implemented

## 问题

严重级别：P1。[R-CON-1、R-CON-3](../../../../docs/spec/requirements.md) 要求已提交内容在一秒内可见，读取不得混合不同提交的内容。基线 FUSE 的每个句柄保存自己的整文件缓冲区，但 [Open 返回的 flags 为 0](https://github.com/codetreker/remote-fs/blob/252259f1b89682a5b2acf54004f0fe8b8677528d/packages/fuse/node.go#L318-L339)，内核仍可在同一 inode 的句柄之间共享内容页。

在该提交上，以 SQLite metastore 与 memory object store 挂载文件系统：先写入 1 MiB 的 `A`，打开旧句柄；通过 storage 将同一节点覆写成等长的 `B`，等待 1200 毫秒，再打开新句柄。旧句柄先读取首个 4096 字节，新句柄随后读取同一范围，得到的仍是 `A`；当前已提交内容与新句柄取回的内容都是 `B`。旧句柄重新填入页缓存，因此单在新句柄打开时失效不能证明后续读取正确。

## 决定

普通文件的 Open 与 Create 返回 `FOPEN_DIRECT_IO`。挂载不保存各 handle 的全文件内容，读取通过保留的 File 对象取得当前状态，旧 handle 因而没有一份私有旧内容可以重新填入共享页缓存。节点 ID 直接用作 inode，同一节点的覆写仍保持身份。

[活跃文件句柄](../architecture/2026-09-08-live-file-handles.md)同时定义已有 fd 的 live 读取、同步修改及 rename/unlink 后的原对象保留。本项接续[名字不是身份](2026-09-01-a-name-is-not-an-identity.md)的编号保证；[读取内容与长度](2026-09-07-bind-buffered-reads-to-their-size.md)分别拥有属性和 EOF 的一致性。

## 备选方案

**保留旧 handle 快照，只在新 Open 时失效。** 已知交错表明旧 handle 可以在失效之后再次填回旧页，一次打开时的失效不能保护之后的读取。

**为内容缓存维护完整的版本与失效协议。** 这仍由[内核缓存与不可达](../../proposed/architecture/2026-08-19-kernel-cache-and-unreachable.md)保留；它需要共同约束内容来源、服务不可达和失效时序。普通 fd 采用逐次读取与 direct I/O，不把尚未交付的缓存保证当作成立。全文件缓冲的其它选择与成本由活跃文件句柄决定记录。

## 后果

普通文件读请求经过权威对象访问，不由共享旧页直接答复。[真实双 HTTP 挂载用例](../../../../packages/fuse/live_files_test.go)的 `TestOldDescriptorCannotRefillFreshDescriptorWithStalePages` 固定 1 MiB 的 `A` 到 `B`、等待 1200 毫秒、新开 fd、旧 fd 先读的顺序，随后按 64 KiB 交错读取旧、新 fd，断言均为 `B` 且大小与 EOF 正确。其它 live 用例还验证旧 fd 立即看到等长及变长的新内容，身份不因覆写改变。

代价是失去内核内容页缓存命中，每次读取都进入 FUSE 与 File 接口；backend 仍可能物化完整对象。direct I/O 不交付完整 mmap 语义，三个元数据超时仍为 0。[元数据复制](../architecture/2026-08-27-metadata-replication.md)的连续性与重建缺口、未来非零缓存、[显式内容依据与目录父身份](../../proposed/architecture/2026-08-20-nothing-pins-an-open-file.md)继续有独立边界，不能从内容缓存修复推断它们已经完成。
