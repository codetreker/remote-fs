# Agent Note: 读取内容与长度来自同一对象状态

Status: implemented

## 问题

严重级别：P1。[R-CON-3、R-CC-5](../../../../docs/spec/requirements.md) 要求并发覆写下的读取成功时得到完整的某次提交。基线 FUSE 句柄保存打开时的内容，而 [handle.describe 仅在节点身份变化时覆盖干净句柄的长度](https://github.com/codetreker/remote-fs/blob/252259f1b89682a5b2acf54004f0fe8b8677528d/packages/fuse/handle.go#L236-L249)。普通覆写可能保留身份，新的长度于是与旧缓冲区同时生效。

在该提交上，以 SQLite metastore 与 memory object store 挂载文件系统：写入 65536 字节的 `A` 后打开读句柄；通过 storage 覆写为 `BBB`，确认覆写前后的节点 ID 相同；对旧句柄执行 `Stat` 再读至 EOF。`Stat` 报告长度 3，读取成功返回 `AAA`，既非完整旧内容，也非完整新内容。

## 决定

FUSE 通过 FileSession 保留普通文件身份，每次 ReadAt 同时取得该对象一份状态的属性与区间字节。Getattr 使用 File.Stat 或 StatNode，不把新长度与旧 handle 缓冲区组合。已有 fd 的后续读取看见该对象已经完成的覆写、增长和缩短；同名替换后的旧 fd 继续访问原对象。

这项修复采用[活跃文件句柄](../architecture/2026-09-08-live-file-handles.md)的 direct I/O 与逐次权威读取，接续[名字不是身份](2026-09-01-a-name-is-not-an-identity.md)的对象识别。节点 ID 不因普通覆写改变，也不被当作内容版本；每次成功读取的字节、长度与 EOF 来自同一 revision。跨多个读取不冻结整份文件，持续覆写仍可以按 R-CC-5 明确失败。

## 备选方案

没有为长度问题另行选择一套缓冲修补机制。保留旧内容并只调整 Getattr 长度无法满足已有 fd 读取当前对象的语义；是否保留全文件缓冲及相应代价由活跃文件句柄决定统一比较。身份变化不能代替内容变化，修复不要求后端每次覆写都更换节点 ID。

## 后果

旧句柄不会把短的新长度用于旧字节，路径查询与新句柄也不被旧缓冲区强行覆盖。[真实双 HTTP 挂载用例](../../../../packages/fuse/live_files_test.go)的 `TestLiveDescriptorStatThenReadAfter65536ByteShrink` 固定 65536 字节 `A` 到 `BBB` 的顺序，旧 fd 先 Stat 再读，断言结果为 `BBB`。同文件中的 live 用例另核对等长覆写、增长、缩短至空文件后的 ReadAt、EOF、fstat 与 inode，并对 rename、unlink、同名替换分别验证旧对象保留。

每次读取都需要经过挂载与保留文件接口，失效引用和不可达不会由旧内容代答。完整对象物化与持续并发读取的成本仍在[文件句柄设计](../../../../docs/design/server/file-handles.md)中明确；[跨句柄页缓存](2026-09-07-prevent-cross-handle-page-cache-staleness.md)拥有另一项内核缓存触发顺序。[文件目标提案](../../proposed/architecture/2026-08-20-nothing-pins-an-open-file.md)继续保留显式内容依据与目录父身份约束。
