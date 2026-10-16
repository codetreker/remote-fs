# Agent Note: 减少多余的测试工作

Status: implemented

## 问题

部分 SQLite 用例花大量时间准备相同状态：两个当前 schema 的损坏矩阵在每个子用例重建健康数据库，随后才注入它要检查的损坏；索引准入用例则用 20,000 次 Go 到 SQLite 的调用写入固定记录。前者要证明损坏被拒绝，后者要证明大集合上的索引与准入行为，重复构造本身不是这些判据。公共 metastore 契约的每个子用例需要独立 volume 和连接生存期，却为此重复建立物理 schema；它的邻居 volume 只需制造真实日志间隙，持续创建新节点又让后续检查面对越来越大的无关树。读写交接用例在取得写者结果后，还会等待已经排队的完整读取结束；这些读取已不属于写者能否在负载下推进的观察窗口。公共 storage 原子替换用例的读循环还会在写入期间反复复制和校验相同的大块内容，持续争夺写者需要的 CPU。

[基线 CI](https://github.com/codetreker/remote-fs/actions/runs/34327807431)的 checks 作业耗时 457 秒，mounted 作业 246 秒。它们包含并发执行的包、测试与清理，不能把用例耗时直接相加，也不能单凭作业时长断言某类 CPU 或 I/O 成本。需要在同一配置下区分准备、被测操作和结果返回后的清理，同时保持原来的测试命题。

双 SMB sessions 的 share matrix 在每种 incumbent／share 条件下为三个 applicant 重复取得相同 incumbent；十四项 live scalar corruption 各自建立同一健康 Store；256-byte frontier 超限用例准备一百个目录。这三处的判据分别是共享准入、损坏读取拒绝、固定 byte bound 与 recount 原计数保持，重复的准备不构成额外行为证据。

objectstore 的 storage 契约为七十个子项重复形成相同 SQLite schema，两个只读 Get 错误矩阵重复写入同一健康内容。SQLite 内部组件的十四个校验父矩阵，也为 127 个原有命名子项反复准备同一健康数据库。它们必须验证独立 volume、调用方事务、实际拒绝原因与清理，重复的健康准备本身并不是这些判据。

[后续逐项调查](../../../../docs/research/2026-10-16-slow-ci-cases.md)在固定 head 的 CI 中找到 173 个超过五秒的 verdict，其中 18 个已包含在父项时间内。普通 schema、只读 native owner、历史 fixture 的健康提交以及不进入 wire 的大路径分配仍有重复成本；契约、崩溃与真实恢复的成本则包含必须保留的状态形成。目标需要同时观察各项准备和整份 CI 的调度，不能把局部时间相加当作 wall time 收益。

## 决定

### 契约共享物理数据库，子用例拥有独立 volume

根 [metastore 契约](../../../../packages/metastore/sqlite/contract_test.go)与 [objectstore 的公共 storage 契约](../../../../packages/storage/objectstore/storage_test.go)各由自己的顶层测试拥有一个真实文件数据库。metastore 工厂为每个子项分配唯一的 `volume-N`／`neighbour-N`，分别按原有 allowance 和默认选项打开 Store；objectstore 的原有七十个子项各取得唯一 `volume-N`，以及原有计数器分配的独立 Blob prefix。

子项拥有自己的 context、Store、连接和检查错误的 Close cleanup；objectstore 还为每项创建独立 Storage 与 maintenance worker。下一项不借用前项的活句柄。physical schema 可以复用，树、内容、配额、对象 key 空间、维护队列、volume 日志和 worker 都属于该项。objectstore 的 bounded 契约、关闭排空、容量、sweep 与失败 Put 指定矩阵也使用所属父测试的普通 physical schema，各例仍取得自己的 volume、原 allowance 和 Objects／Blob prefix。默认 `newParts` 为其它调用建立新数据库与 `workspace` volume。

顺序子用例回归先保存一份已有内容、配额、reserved／unresolved／garbage 对象和日志，再确认它的 Store 已关闭。下一对 volume 须从空目录、仅含根的 snapshot、零 Used／pending／garbage 与自己的 allowance 开始；新日志 identity 独立，共享数据库 identity 和高水位仍合法。第二例写入后重新读取第一例，核对其元数据、配额、对象状态和双方日志未被改动。每例仍制造稀疏 position，不假定新 volume 的全局序号从零开始。

objectstore 的隔离回归确认前一项 Store 已结束，新项没有继承名字、内容、root 属性、配额用量、对象维护队列或日志 incarnation／position；新 Blob prefix 不能读取前项的真实 object key。关闭第三个 sibling 后第二个 volume 仍可读，前项的 Blob 字节保持，物理 database identity 相同且 generation 推进。

### native storage 契约先形成独立 volume，再逐项完整打开

limited 的两种 storage capability 各用 [RunPrepared](../../../../packages/storage/storagetest/storagetest.go)把权威 case 表的实际数量交给父工厂，不维护第二份 case 计数。[memoryfixture.Prepared](../../../../packages/storage/lockcontract/memoryfixture/prepared.go)在第一次 native lease binding 前，通过普通 SQLite Open／Close 形成全部独立 volume。每个原子项仍执行完整 OpenLocking，拥有新的 authority、Objects、连接池和 worker，检查本项完整 Close 后才进入下一 volume；不复制数据库、native anchor 或对象内容。

父工厂按序分配且每个 volume 只用一次，mutex 拒绝仍 active、已耗尽或已 poison 的调用。检查 lease watermark 前还检查 owner live；前后 MaxLease 必须为零，任何打开、能力健康、watermark 或关闭失败都阻止后继。命名空间、root 属性、配额、history、对象队列、native capability、前项字节与健康后继由独立回归约束。strong grant 会留下 database-wide recovery watermark，这类锁契约继续使用独立 New，不进入该工厂。

预准备使一个父测试的普通 Open／Close 次数增加，却消除反复 migration 和首次 lease initialization；收益必须包含整份 upfront 工作再判断。`-run` 过滤仍准备完整 case 表对应的全部 volume，这是避免复制测试 matcher 与产生另一份清单的成本。原有两个能力分支、140 个 leaf、100 次原子替换、两个读者和 128／96 KiB 内容均保持；localstore 的 root 永久绑定不能使用该机制。

### 只读 Get 故障矩阵保留健康 owner

[objectstore 的两个 Get 故障矩阵](../../../../packages/storage/objectstore/failure_test.go)各自在父测试形成一份真实 SQLite／Blob 文件，原有四项和六项错误继续分别执行。每项创建自己的 failing wrapper、Storage worker 与错误／计数器，拒绝 Get 必须恰好发生一次；原错误原因、errno 与诊断断言保持。

每项开始前和 borrowed worker 成功 Close 后，以仍有效的父 context 核对完整 Attr（含 metadata 与时间）、根 List、Space 的值与错误文本、Usage、ObjectStatus，以及健康 owner 的原内容。cleanup 按 LIFO 顺序先关闭 borrower，再检查不变性；父 owner 只在所属测试结束时关闭。准备、mutation、Put、sweep、lease 与恢复用例继续取得自己的 fixture，不能借用这份只读 owner。

### 只读 native 拒绝保留能力与健康后继

objectstore 的 [node-open](../../../../packages/storage/objectstore/node_open_refusal_test.go)、[目录 metadata](../../../../packages/storage/objectstore/directory_observation_test.go)和[引用名字](../../../../packages/storage/objectstore/reference_name_test.go)三个矩阵各保留一份健康 native owner／tree，原有 13／7／9 项各自拥有 fault／capability wrapper、Objects、Storage worker、session、reference、counter 与结果字节。borrowed adapter 保留 native optional capabilities；child Close 只结束自己的资源。关闭 session、worker 后，以父 context 比较递归 Node／Child 属性、身份、metadata、revision、BackendIdentity、DurableState、零 MaxLease、barrier、Space 分类和值、Usage 与 ObjectStatus，并验证相冲突的健康后继打开及再次观察。清理失败使 owner 不可再借用。

limited 的三个拒绝矩阵将十七份 native 准备变为三份父 backing，保留七项 settlement、六项 capability 与四项 accounting error；locked 的四项 capability 变体同样各有新 session／probe／allowance／facade，父 backing 不被 child 关闭。拒绝必须保留原错误身份与精确注入次数，父树、history、Usage、ObjectStatus 及健康后继必须不受 child fence 影响。limited 的有状态目录 rename／publication 则仅共享普通 physical schema，四项分别拥有独立 volume、连接、Objects 和 worker。

replicated 的三项 checkpoint 拒绝借用一个健康 authority／seed，handler、server、gates、counter、replica、transport 仍逐项拥有，关闭且 active work 排空后再核对原状态。service 的八项 missing component 以 cached／reopened 各有一个 root，fresh reader 先结束，注入恢复后检查 root／DB identity、权限和 marker；末尾实际重开并验证 summary、Attr 和原内容。它们不跨顶层测试共享 owner，不把故障、journal 或未知关闭带到后继。

### 纯校验矩阵使用独立事务

[dbstate](../../../../packages/metastore/sqlite/internal/dbstate/fixture_test.go)、[changes](../../../../packages/metastore/sqlite/internal/changes/fixture_test.go)和 [schema](../../../../packages/metastore/sqlite/internal/schema/fixture_test.go)分别由所属父测试拥有健康文件数据库。复用限定为七个 dbstate、四个 changes、七个 schema 父矩阵，原有命名子项分别为 85、28、50 个。每项使用自己的 transaction，损坏、临时 trigger 和分配操作均在该事务中发生，原 variant 名称、errno、SQL 原因和结果断言保持。directory revision 的四项目录损坏保持仅含目录的健康前提，文件类型分支另有独立文件树，不为共享准备增加原来不存在的节点。

父 fixture 捕获完整 `sqlite_schema` 与所有表的 typed rows。纯校验子项结束时必须成功 Rollback；连接级 PRAGMA 另行恢复并读取确认，已提交 schema、数据与原基线比较，连接归还后 InUse 必须归零。dbstate 与 schema 恢复 `ignore_check_constraints`，changes 还恢复 `query_only`、`foreign_keys` 与 `recursive_triggers`。事务与清理使用不随子项取消的 owner context，被测操作仍使用原调用 context；不能让自动回滚与显式恢复争用一条连接。

`ReconcileStartup` 的命名 variants 保留每项的真实 Commit 与提交后的 Read，以证明数据库状态不变；最终缺失 state 的场景仍使用独立数据库。公开 Prepare、迁移、创建事实、uncertain commit、持久恢复与原生所有权保留各自真实状态形成和发布，不以内部纯校验事务替代。

### 健康 seed 批量提交，历史入口保持独立

dbstate 把初始 migration 与八条 seed DML 通过同一 Tx 完成并检查 Commit，返回仍是已提交的健康数据库。schema 的 populated historical v1～v5 与 integration 的 v1／v2、pre-retained v4／v5 builders 用检查提交、回滚与关闭的事务构造健康 DDL／DML。每份历史数据库仍有独立文件，原损坏、实际 Prepare／migration 和拒绝后的 unchanged-state 断言保持；批量提交只发生在被测操作之前的健康准备。

### 关闭的持久 root 仅供只读拒绝复用

localstore 的 malformed marker、foreign witness、unsafe stage 三组各由父测试持有已关闭健康 root，原有十四个变体逐项注入并恢复。整棵树的路径、inode、mode、owner、link count、文件长度和 SHA-256 都必须与基线相同；目录时间和读取可更新的 atime 不属于这份不变性。每项确认 native ownership 已释放，末尾实际重开验证已确认文件的 Attr、身份和内容。污染回归覆盖同长度字节、权限、替换 inode 和额外文件；恢复失败停止后继。oversized SQL、初始化、真正恢复、SIGKILL、WAL 与契约继续独立准备。

checkpoint-worker 用例用 held snapshot 和故障目录保持失败／恢复阶段，测试重试间隔为十毫秒，原 failure、Pending 和恢复状态均继续观察。serviceapi 的 25,000 项响应只为末条断言构造完整 child path，去除其它不进入 JSON 的路径分配；原 HTTP body、严格解析、大小边界和末条完整相等断言保持。

### 关闭错误分别验证显式重试与 maintenance

[确定 accounting refusal 用例](../../../../packages/storage/objectstore/files_test.go)保留两条独立 native lifecycle：volume close 验证原显式重试和 claim；maintenance 先等待失败 session.Close 完成，核对冲突打开仍被原 share claim 拒绝，恢复 accounting 后经公开空内容 Write 触发实际后台重试。`synctest.Wait` 确认 worker 完成，再在手动 volume Close 之前核对 native Usage 为零、旧 detached NodeID 返回零身份和 `ESTALE`、maintenance status 健康。阶段等待区分已完成失败和正在执行的 Close，避免把偶然进入的成功关闭分支当作错误重试覆盖；原回执容量、history 和关闭结果判据保持。

### 准备优化的范围

physical schema、健康只读内容和纯校验事务的复用服务于 [R-FS-1／R-FS-5、R-CON-3、R-ERR-2／R-ERR-6／R-ERR-7](../../../../docs/spec/requirements.md)的既有验证。新的 logical volume、独立错误注入与 worker owner、逐项事务结算及恢复后的不变性是复用条件；生产路径、public contract 的数量与负载、race、`-count=1`、watchdog 和 package-local coverage 判据保持。package 分组和启动顺序由[contract CI 平衡决定](../process/2026-10-16-balance-contract-ci.md)拥有。

延期的 native strong-lock 契约 pooling、HTTP／FUSE 只读矩阵和生产 SQL／JSON／publication 优化各自仍可局部实现与测量；保持跨顶层测试无可变共享状态、原生 inode binding、独立 authority／Store／worker 和工作负载，是使延期不变成架构重写的条件。localstore 的永久 root／volume 绑定、真实恢复、lease watermark 与原子读取并发窗口不由通用 fixture reset 代替。十分钟 CI 目标以完整 hosted step 实测裁定，局部候选上界不构成收益承诺。

### 邻居产生真实日志，目录保持固定

根 [metastore 契约夹具](../../../../packages/metastore/sqlite/contract_test.go)仍在同一文件数据库中打开被测 volume 与邻居。每次被测修改之前，邻居对自己的既有根执行 SetAttr；原子计数器生成唯一的 ModTime，确保每次调用都发布真实 Modified 事件并消耗全局 position。失败仍使测试失败，不能省略间隙。

邻居的根身份、模式和空目录保持不变。回归用例直接读取两边日志，验证被测 position 稀疏、八次邻居修改的时间值各异（含四个并发调用），并检查树没有增长。时间值唯一不要求并发调用按计数器次序提交。把旧的逐次 Create 夹具装回去会使该回归失败；未改变 metastore 契约原有的操作和结果断言。

### 固定大集合由一条 SQL 构造

[`TestPendingAdmissionSeeksPastALargeReferencedSet`](../../../../packages/metastore/sqlite/objects_test.go)在原有事务内用递归 CTE 写入 20,000 条 referenced object。记录按 ordinal 插入，key、volume、state、size、NULL digest、时间字段与原集合相同；`RowsAffected` 必须恰为 20,000。

用例仍对生产查询执行 EXPLAIN，并通过实际 Reserve 核对准入结果。它不缩小集合、不改成内存数据库，也不跳过真正被检查的路径；减少的是逐行跨越 Go/SQLite 调用边界的准备工作。

### 损坏矩阵复制完整、私有的健康种子

[`TestOpenRefusesInconsistentVolumeIntegrity` 与 `TestRetainedIntegrityRefusesInvalidDetachedState`](../../../../packages/metastore/sqlite/internal/integration/integrity_test.go)各自在顶层用例中建立一份种子。种子通过原有真实 Open 与公开 mutation 形成，两个 volume 的 Store 和查询句柄均检查关闭结果；确认 journal mode 为 WAL 后执行 TRUNCATE checkpoint，要求 busy、frames、checkpointed 全为零，再关闭最后句柄，读取完整主数据库字节和对应 fixture 元数据。

每个子用例把这份不可变镜像写入新路径的 `0600` 普通文件，拥有独立 inode、连接和可变状态。ID、高水位、日志和对象 key 保持一致，原来的 detach、损坏、Open 或先打开后 ObjectStatus 的顺序与错误断言保持原样。

种子同时接受健康与隔离检查：第一份副本通过公开 Create 写入并能 Stat；第二份副本能通过 Open/ObjectStatus，且看不到第一份副本的探针。顶层 cleanup 再核对种子字节未变。没有全局缓存、共享活句柄或共享可变数据库，也不能在 WAL 尚未完整归并时只复制主文件。

这项种子复制仅限两个当前 schema 的完整性损坏矩阵。迁移、WAL、恢复、lease、原生文件身份及跨进程用例继续使用各自原有的准备路径；这些用例的状态形成过程本身属于被验证的行为。

### 写者结果返回后结束读者负载

[`TestReplicaWriterProgressUnderContinuousListings`](../../../../packages/metastore/sqlite/replica_test.go)保持 128 个读者、4096 行、已占满的 reader pool、相同的等待与准入条件，以及 30 秒写者截止时间。正常路径在收到实际 Apply 结果后才停止循环并取消该用例拥有的读者 context；失败后的 cleanup 同样会停止它。写者等待期间不提前减少负载，Applied、Stat 模式和 Position 的断言保持不变。

停止后的错误只有同时满足四项条件才按预期取消处理：停止标志已设置、context 的取消原因是本用例的私有标记、错误保留 `context.Canceled`，且 `storage.ErrnoOf` 分类为 `EINTR`。独立故障、其它取消原因、deadline 和非 EINTR 结果仍使测试失败。所有读者必须 join，随后验证读写门空闲、读取 permit 为零。

这减少的是写者结果之后的排队清理工作，不是生产 Apply 的执行时间。每次成功 List 仍须返回完整 4096 行；结果返回后被取消的读取不再完成全表扫描。

### 原子替换的读者在每次观察后让出调度

这项调度决定以 [公共 storage 契约](../../../../packages/storage/storagetest/storagetest.go)的 200 次写入、两个读者、128 KiB 与 96 KiB 两种内容及原来的错误／完整值断言为前提。当前写入次数由后续[测试工作量决定](2026-09-09-scale-test-work-to-its-assertions.md)调整为 100；这里保留当时的理由与测量。读循环在成功读取并核对内容后调用 `runtime.Gosched`；合法的 `EAGAIN` 仍按原规则计数，继续前也让出调度。没有睡眠、固定采样间隔、读取次数上限或缩小内容，写者也不增加暂停。

这是测试负载的调度选择，不是 storage 的性能保证。正确后端的每次成功读取仍只能是两个完整值之一，汇总观察必须包含两种值；其它错误仍失败。选择时另外记录读写 API 调用区间，并用先发布空内容、再发布目标内容的真实后端包装器作负向对照，包装器不插入额外等待。API 区间重叠不等于命中了发布内部的短暂窗口，有限次负向对照也不能证明所有更短窗口都有相同检出率。

### SMB share matrix 按 incumbent 分组

[双 SMB sessions 与非 SMB share matrix](../../../../packages/smb/commands_file_integration_linux_test.go)保留 direct／HTTP、三类 incumbent access、八种 ShareAccess、三类 applicant access，共 144 个原向量。每个 incumbent／share 组合分别只建立一份 SMB incumbent 和一份非 SMB incumbent，在各自拥有的循环内测试三类 applicant；每个 applicant 仍独立打开和关闭。两个 SMB sessions、非 SMB applicant 和反向两个 SMB applicants 的冲突或成功判据保持，真实 Renew 继续执行。每类正向和反向 applicant 之后，以及最后一份 incumbent 关闭后，都核对原对象身份和四字节长度。每种 transport 的 incumbent 建立／释放从 144 组降为 48 组，总打开尝试从 432 次降为 336 次。

复用只跨同一 incumbent／share 组合内不修改对象的申请。incumbent 在另一方向开始前关闭，各成功 applicant 在下一项之前关闭；不能把新 grant、未结清 close 或残留共享限制带到下一组合。

### Live scalar corruption 每项恢复健康基线

[十四项 scalar corruption](../../../../packages/metastore/sqlite/reference_state_projection_test.go)在所属顶层测试中建立一份真实健康 Store 和 retained file reference。每个子项先确认完整 FileState、scalar projection、DurableState 和持久 used／allocated_used 与原基线完全相同，再注入原存储类型或非法值，调用真实 StateScalars 并要求 `EIO` 与全零 ReferenceState。

每项独立拥有注入用 writer connection 和清理；清理恢复节点的原 scalar 值、check constraints，并检查连接关闭。前置与清理后的检查还须确认 coordinator healthy、native file ownership 有效及 reader connection InUse 为零；拒绝返回后也立即核对 reader 已归还。清理后的读取使用顶层 context，因为子项的 context 在 cleanup 之前取消。共享活 fixture 仅限这一纯读取拒绝矩阵，不跨顶层测试，也不进入关闭、迁移或恢复用例；清理或恢复失败必须使测试失败并停止后续损坏。retained file 和 Store 按原 owner 清理，先关闭引用，再关闭 Store。

### Frontier 超限只准备足以越界的目录

[256-byte frontier recount 用例](../../../../packages/storage/limited/limited_test.go)以 active root 的 record charge 和每个 `directory-000` 格式路径的 record／name charge 计算最少目录数：`(MaxFrontierBytes - recordBytes) / childBytes + 1`。record size 沿用精确边界用例的同一结构，经 `unsafe.Sizeof` 适配平台字长；amd64 为七个目录，32-bit 为十个。最后一项加入后才超过原 256-byte 上限，单目录上限保持 1 MiB，不用它替代 frontier 拒绝。

真实 bounded backend、七字节原计数、后加的一百字节内容、`EIO` 与 traversal-frontier 原因、失败后保留计数及写入恢复 operation gate 的断言保持。这个用例检验越界和回滚；精确边界的独立用例继续覆盖 active／pending 路径计费。

## 备选方案

**每个契约子用例使用新数据库文件。** 物理隔离直接，但反复建立相同 schema。独立 volume 保留接口状态隔离，子用例保留 Open／Close；增加顺序隔离回归，是接受共享物理数据库的必要约束。专门验证数据库形成与恢复的其它用例不使用这个工厂。

**只复用 objectstore 契约的 physical schema。** 范围集中于一个工厂，可以去除七十项契约的重复 schema 准备；两个纯读错误矩阵和内部纯校验矩阵仍重复构造相同健康前提。它们的状态不变性或事务 rollback 已有明确判据，分别由自己的 owner 复用可以减少这类准备，并保留原拒绝路径。

**统一复用所有 native 契约的活 authority。** 其它 native 契约依赖独立 authority、root、lease 与发布历史，通用 pooling 需要重新证明这些隔离。预准备 volume 仍逐项完整打开／关闭 authority，只用于无 strong watermark 的 storage 契约；默认工厂仍提供新数据库，新的 logical volume 不能退化为路径前缀 facade，不能在不同父测试之间共享可变 native owner。更多扩展仍需在对应工厂增加局部证据。

**只改变 CI 调度。** 调度改变同时运行的工作，不消除每项重复 schema 准备，并可能增加 runner 上的竞争。当前分组由[独立决定](../process/2026-10-16-balance-contract-ci.md)拥有，局部准备优化仍分别测量，完整执行路径另行裁定。

**只优化超过五秒的 leaf。** 慢父项多由若干一秒左右的准备累积，且原二进制的大量时间仍分散于阈值以下。只改显眼的慢 leaf 会漏掉可复用的健康前提；逐个父矩阵保留全部子项，才能同时证明减少的是重复准备。

**SMB share matrix 按期限条件续期。** 该实验从真实 Status／Renew 保存 lease 和 history 的保守下界，只有不足以覆盖下一次 native operation 时才 Renew，保留 144 个向量及实际打开／关闭。匹配 race 调用的一次比较为 0.8385→0.8275 分钟，仅减少 0.0110 分钟（1.31%）；单对观察不能排除噪声，期限、取消、epoch 安装和错误处理的额外机制没有足够收益支撑。矩阵继续在每次新 SMB 打开之前真实 Renew；进一步改变需新的分段 profile 与重复测量。

**把 native、恢复和有状态矩阵放进通用共享池。** 它能省更多构造，却使 root 属性、lease watermark、reference、journal、worker 或恢复 residue 进入后继，native anchor 还绑定原 inode。共享限定于各父测试可证明不变的前提；独立契约和真实 lifecycle 的延后优化需自己的测量与隔离证据。

**每个损坏子用例重新构造完整健康数据库。** 它天然隔离，但为相同前置状态重复执行 schema 与公开 mutation。顶层只构造一次、每例复制独立数据库，保留真实来源和隔离，并通过健康对照检查复制本身。

**逐行执行已准备的 INSERT。** 它保留相同数据，却仍需 20,000 次调用。集合值和顺序固定，可以由一条 SQLite 语句构造；精确行数、EXPLAIN 和实际 Reserve 继续约束结果。

**邻居为每个间隙创建新节点。** 它确实产生真实日志，却同时增长被验证数据库中的另一棵树。间隙的目的在于消耗其它 volume 的 position；修改一个既有根可以满足这个目的，并由日志与隔离断言确认。

**收到写者结果后只设置停止标志，等所有已排队读取完成。** 它让下一轮循环停止，却仍把已进入等待的读取逐批跑完。这些完整扫描没有增加写者阶段的证据；显式取消自有负载并检查 join 和空闲状态，可以保留清理保证。

**读循环成功后立即抢占下一次读取。** 它取得更多大块内容样本，但在相同的 200 次写入期间反复消耗 CPU。每次完整观察后主动让出调度，保留无固定上限的读取循环；是否仍保留有效交错，由匹配测量、实际观察与负向对照约束，不能仅凭耗时更短选择。

**每个 share applicant 或 scalar corruption 都重建 incumbent／健康 fixture。** 自然隔离，却重复准备没有被修改的相同前置事实。按 incumbent 分组、逐项核对健康 scalar state 和恢复原值，保留每个申请的独立 owner 与错误判据；恢复失败不能被新的 fixture 隐藏。

**Frontier 超限用例保留一百个目录。** 它制造更大的真实树，但对固定 256-byte 上限，越界后的目录不会增加当前拒绝和原计数保持的证据。按实际 byte charge 跨过精确上限即可保留该命题，独立边界用例继续检验计费公式。

## 后果

以 ef64c5a 为基线的准备优化中，objectstore 的七十项契约只形成一份 physical schema，两个 Get 错误矩阵的健康 fixture 从十份减为两份；十四个内部父矩阵中，dbstate 的准备从 63 份减为六份（含独立的 missing-state 分支），changes 从 25 份减为三份，schema 从 45 份减为六份。历史测量小节保留该次匹配结果。

当前十八个内部父矩阵保留 163 个原有命名子项；native 只读复用、普通 volume 工厂、关闭的 root 恢复与批量健康 seed 各有所属 owner 和隔离检查。每个新的 volume 和事务仍独立；代价是父测试持有更多既有 volume，复用还依赖完整基线、连接设置恢复与检查 cleanup，任一失败都必须暴露。更少的 schema 准备不构成持久发布或 CI 运行时间的保证。

### d394f24 基线的匹配测量

以下比较固定 [d394f24](https://github.com/codetreker/remote-fs/tree/d394f2404c74c07245c3a80049518fdff8d31fb2) 的测试源码为基线，使用 Go 1.26.8、`GOMAXPROCS=2`、`go test -json -race -p=1 -count=1 -timeout 10m`，以 `-run` 选择对应父测试，objectstore 使用真实 pinned Azurite。每组前后各一次，编译预热独立记录。数字是每组共同父测试 inclusive Elapsed 的和，包含各自准备、子项及清理，不是进程或 CI wall time，也不是多轮中位数。

| 测试组 | 基线（分钟） | 优化后（分钟） | 减少 | 共同父项／原 verdict |
|---|---:|---:|---:|---:|
| SQLite 内部校验矩阵 | 0.3685 | 0.0812 | 77.97% | 4／40 |
| objectstore 指定矩阵 | 0.8408 | 0.3953 | 52.98% | 10／69 |
| service／storage 只读与准备矩阵 | 1.0412 | 0.4960 | 52.36% | 12／56 |
| limited 完整两能力契约 | 2.5722 | 1.8293 | 28.88% | 1／143 |

SQLite 的四项是 IdentityBounds、LegacySequences、Trim 与 DirectoryRevision，原 36 个命名 children 和四个父 verdict 全部保留。objectstore 保留原 69 个 verdict，新增两个隔离根及四个 children 后共 75 PASS；将 borrower Close 改为结束父 owner、将新 volume 改为复用旧 volume 的两个负向对照分别在 owner／namespace 断言失败，源码随后恢复。service／storage 保留原 56 个 verdict，新增十个判定后共 66 PASS；共同父项耗时包括它们新增的恢复 child，另外两个新增回归根合计 0.0405 分钟，未计入共同父项比较。各组均无 skip、fail 或丢失的原 verdict。

limited 完整契约的 143 个 verdict 和 140 个叶子完全相同；native publication 为 1.2808→0.9052 分钟，native accounting without retained files 为 1.2913→0.9242 分钟。父项测量包含全部 70＋70 份 upfront 普通 volume 准备；二进制为 2.5892→1.8464 分钟，不能把只测构造器的较大收益当作完整契约收益。Prepared helper 自身八个根项及四个 children 的 race 验证另有 12 PASS，无 skip／fail。

这些单轮匹配结果证明选定源码与环境下的局部收益及原判定保留，不能量化 runner 变化或整条 CI 的净收益。完整包 race、各包自己的覆盖块、vet 与最终 hosted gate 分别裁定；十分钟目标只有完整交付 head 的实际测试步骤可证明。

### ef64c5a 基线的历史测量

以 [ef64c5a](https://github.com/codetreker/remote-fs/tree/ef64c5a73992e388065673a81cf96739c1179f9f) 为基线，四组分别选择 objectstore 的三个、dbstate 的五个、changes 的三个、schema 的六个原有父测试，在相同 Go 1.26.8、`GOMAXPROCS=2` 与真实 pinned Azurite 下前后各测三轮。调用使用 `go test -json -race -p=1 -count=1 -timeout 10m`，用 `-run` 选择对应测试；编译预热独立记录。下表按每轮各组父测试的 Elapsed 求和，再取三轮中位数，包含准备、子项和清理，不是进程或 CI 的 wall time：

| 测试组 | 基线（分钟） | 复用准备后（分钟） | 减少 |
|---|---:|---:|---:|
| objectstore | 1.4105 | 0.5760 | 59.16% |
| dbstate | 0.6453 | 0.1120 | 82.64% |
| changes | 0.2515 | 0.0413 | 83.57% |
| schema | 0.5767 | 0.0977 | 83.06% |

三轮均保留原有 224 个 targeted verdict，只新增一个由原正向断言独立成项的 `valid_barriers`，没有 skip 或 fail。四个隔离回归另以 race 执行，13 个 verdict 全部通过。四包各自完整普通构建使用 `-json -p=1 -count=1 -timeout 10m` 并生成默认 package-local coverprofile，原有 842 个 verdict 全部保留，加上 14 个上述新增判定后共 856 个 PASS。生产覆盖块清单相同，没有丢失已覆盖块：objectstore、dbstate、changes 分别保持 1298、147、271 块；schema 为 555→556，增加的是既有 metadata 校验路径的一块。

四包的定向 vet 与十六个修改／新增 Go 文件的 `gofmt -l` 均退出零且无输出。这些结果只验证局部准备成本、原测试判据与覆盖块保留。完整 CI 与 module-wide coverage gate 各自执行并裁定，局部计时不推算 hosted runner 的收益。

下列准备与退出优化使用 Go 1.26.7，在同一环境以 `GOMAXPROCS=2`、`-race -count=1 -p=1` 串行运行，预热编译独立记录。每项优化前后各测三次，以下为顶层测试耗时中位数，包含各自准备与子用例，优化后的矩阵也包含新增健康／隔离对照：

| 用例 | 原执行方式 | 优化后 | 减少 |
|---|---:|---:|---:|
| 公共 metastore 契约 | 45.30 秒 | 40.30 秒 | 11.0% |
| 20,000 条 referenced object 准入 | 4.41 秒 | 2.48 秒 | 43.8% |
| 当前 volume 完整性拒绝矩阵 | 17.90 秒 | 2.97 秒 | 83.4% |
| detached 状态拒绝矩阵 | 16.97 秒 | 4.29 秒 | 74.7% |
| 128 读者下的写者推进与清理 | 19.82 秒 | 7.83 秒 | 60.5% |

每次比较保留原 verdict 名称集合：公共契约为 97 个，准入用例为 1 个，两个损坏矩阵合计为 80 个；全部通过，没有失败或跳过。新增邻居回归另以 race 构建验证，旧夹具的负向对照在预期断言失败。读者负载用例的写者阶段仍约 1.5 秒；结果后的 join 从约 12.2 秒降为 0.0006～0.0713 秒，节省来自观察窗口之后。编译、进程及包的耗时不混入这张表。这些是局部匹配结果，不能直接推算新的流水线耗时；整条 CI 的效果仍须由实际运行观察。当时的两个 CI 作业与测试门禁均未改变；当前调度由[并发 CI 决定](../process/2026-10-14-parallel-ci-execution.md)持有，工具链和构建缓存由[缓存决定](../process/2026-09-09-refresh-ci-go-build-caches.md)单独拥有。

共享物理数据库另在 Go 1.26.8 下作三组匹配测量：公共 metastore 契约由 39.64／39.63／39.66 秒变为 34.30／34.15／34.08 秒，中位数减少 5.49 秒（13.85%）。每次保留原 97 个 verdict，新增顺序隔离与既有邻居回归另通过 race。它不与上表 Go 1.26.7 的计时相减；全包与 CI 的结果各自验证。

原子替换读循环以 Go 1.26.7、相同 race 配置作三组配对测量。limited 的 native publication 分支中位数从 10.858 秒降至 8.238 秒（24.1%），unmanaged bounded 分支从 11.672 秒降至 9.384 秒（19.6%）；这张测量不混入后续 Go 1.26.8 的执行结果。总读取次数的中位数也分别从 4933 降为 3277、从 4980 降为 3359，因此不能只用耗时更短证明选择成立。每个读者的 API 区间都与全部 200 次写入重叠，且各自看到了两种值；按每个读者统计，至少含一次完整 Read 调用的 Write 调用区间数从 193～199 次变为 197～200 次。两种实现、调度前后各十次无额外等待的非原子负向对照，合计 40 次均检出损坏。正确后端样本没有遇到 EAGAIN，因此没有动态覆盖该让出分支；它仍沿用原错误接受规则。读取样本减少的代价由这些区间分布和负向对照约束，但它们不能证明更窄内部损坏窗口的检出率等价，也不能推出净 CI 收益。

SMB share matrix、live scalar corruption 和 frontier 超限用例的匹配测量比较 [0d49a6a](https://github.com/codetreker/remote-fs/tree/0d49a6a833bcb01f90d8ee5c56c2c93431ddb771) 与 [732a92b](https://github.com/codetreker/remote-fs/tree/732a92b007f710be6c987b4c71965b3db21d49fc) 的测试源码。两者使用 Go 1.26.8、`GOMAXPROCS=2`，前后各执行三次 `go test -json -race -p=1 -count=1 -timeout 10m`，每次用 `-run` 精确选择对应顶层测试。编译预热独立记录，以下为顶层测试 Elapsed 的中位数，包含准备、子项和清理，不包含编译与进程启动耗时：

| 用例 | 原执行方式（分钟） | 优化后（分钟） | 减少 |
|---|---:|---:|---:|
| 双会话 SMB share matrix | 1.0930 | 0.8862 | 18.92% |
| 十四项 live scalar corruption | 0.2580 | 0.0302 | 88.31% |
| 256-byte frontier 超限 | 0.1115 | 0.0252 | 77.43% |

每次的原 verdict 名称集合分别为 3、15 和 1 个，前后完全相同，全部通过且没有 skip 或 fail；144 个 SMB 逻辑向量仍在两个 transport 子项内逐项执行。这个比较只衡量三项用例的准备改动，不能推算并发调度后的完整 CI 耗时。

相同两份源码的 SQLite、SMB 与 limited 前后各一次完整普通构建使用带 `-json -p=1 -count=1 -timeout 10m` 的 `go test`，通过 `-coverprofile` 保存默认 package-local instrumentation 的执行结果。2181 项 Package／Test／Action verdict 完全相同，全部通过且没有 skip 或 fail。生产覆盖块清单相同，没有丢失任何已覆盖块；SQLite 为 2913→2913、limited 为 493→493，SMB 为 3299→3300，增加的是既有 cleanup 路径的一块运行观察。三包覆盖率分别为 81.5524%、88.2022%→88.2363% 和 91.2258%。这些是局部测试与 profile 比较，完整 module-wide coverage gate 仍由 CI 执行。三包的定向 `go vet -p=1` 与三个修改文件的 `gofmt -l` 均退出零且无输出。

共享契约数据库让前面子用例的 volume 留到顶层结束，增加后续 Open 面对的已存状态；工厂的唯一命名和跨例不变性检查不能省略。完整性矩阵另须保存一份种子字节，并为每个子用例复制文件、重新打开真实 SQLite。种子必须完整、不可变且只在所属顶层用例内存活；损坏或隔离检查失败就结束测试，不能回退成一份看似有效的新数据库。live scalar 矩阵则保留同一份活 owner，对前置状态和逐项恢复的验证不能省略；SMB 分组减少重复取得 incumbent，对每个 applicant 的独立关闭和最终清理检查保持。

拒绝矩阵复用、确认阶段事件门及写入重复次数的后续选择见[测试工作量决定](2026-09-09-scale-test-work-to-its-assertions.md)，这里的历史测量与原取舍保持。

这项决定保留了[包内覆盖](2026-09-09-sqlite-package-local-coverage.md)、race、`-count=1`、断言和当时的数据规模；[执行预算](../process/2026-09-08-budget-ci-race-test-execution.md)不因优化而放宽。此决定沿用[在所属边界注入失败](2026-08-22-failures-injected-at-the-objects-boundary.md)与[资源由创建者清理](2026-08-22-a-mount-belongs-to-whoever-attached-it.md)的约束，具体准备规则与读者退出判据分别见[测试准备](../../../../docs/testing.md#sqlite-测试准备与隔离)和[读写交接](../../../../docs/testing.md#元数据副本的读写交接)。
