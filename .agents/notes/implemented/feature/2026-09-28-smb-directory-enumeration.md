# Agent Note: SMB 完整目录捕获与句柄内分页

Status: implemented

## 问题

Windows 通过一个目录句柄多次领取有限大小的枚举结果。页间目录可以变化，Linux／SDK 也可以留下 Windows 无法表示或大小写等价的名字；逐页重新读取或先过滤坏名会造成遗漏、混合 revision 或虚假的空目录。已经完整捕获的目录仍不能在授权撤销、引用退休或 authority 不可达后被当作健康结果继续发送。

## 决定

QUERY_DIRECTORY 使用[有界目录引用](2026-09-28-smb-bounded-create-close.md)和[文件信息](2026-09-28-smb-file-information.md)的身份、StateScalars 与既有 FIFO。每次新 capture 在 `file.scope` 授权后调用原 NodeReference.Scope(ctx)，验证 Scope，再由 `DirectoryReader.ReadDirNodeBounded` 对 `DirectoryTarget{NodeID, Scope}` 完整捕获，Windows 名字和身份验证完成后才过滤、分页。该决定满足 R-FS-9、R-WIN-2／3、R-ERR-1／2／6、R-SEC-5 和 R-INT-3；结构与测试由[SMB 端点设计](../../../../docs/design/client/smb-endpoint.md)和[测试策略](../../../../docs/testing.md)拥有。

### 捕获、权限与健康

一次 capture 包含正确 ParentID、合法非空 opaque revision 和完整条目。错误、超限、非法 UTF-8、Windows 禁止名字、大小写等价冲突、错误 ID／kind 均使整个 capture 失败；pattern 不匹配坏项也不能隐藏它。条目按 ListResult 的 raw-name 字节顺序冻结，Windows 比较器只用于全量歧义检测与匹配。已有名字、对象和其它入口的数据不被改写。

每个 live 目录 FileId 至多保存一份与信息类独立的紧凑 snapshot、UTF-16 expression、下一项 ordinal 与独立的 settled-empty 状态。快照保留六类 codec 所需的共同标量、UTF-16 名字、稳定 ID／kind、matched bit 和固定 rich-field 有效性类别；完整 capture 与投影完成后丢弃 raw name、无关 metadata 和临时 map，不保存错误链或 issued-index 表。

每个请求，包括续页与 EOF，都要求 FILE_LIST_DIRECTORY、本次 `file.read-dir-node` 与 `file.state` 授权，以及真实 `State(StateScalars)` 观察。该调用执行 native SQL 与 objectstore IOHealth，核对原对象、目录 kind、scope 和生命期；编码后继续检查当前授权和本地退休状态。固定 Scope token 不替代健康调用，新的健康观察也不把新 revision 混入冻结快照。新 capture／restart 从原活引用取得实际 Scope；普通续页只执行 StateScalars 健康观察，不重取 Scope 或建立另一份缓存 scope。

HTTP 单独授权本次实际 capture 或 State 请求。已经成功交给 SMB 的 capture 不会因远端 list policy 后来撤销而被收回；宿主的当前 SMB Authorizer 决定每次新页面能否发送。没有重新捕获或仅授权的 RPC。

### 信息类与结果

| 类 | 固定头部与事实 |
|---|---|
| Names `0x0C` | 12 字节；UTF-16 child 名字，FileIndex=0 |
| Directory `0x01` | 64 字节；共同时间、EOF、allocation、属性与名字 |
| Full `0x02` | 68 字节；Directory 加 EaSize |
| Both `0x03` | 94 字节；Full 加空 short-name 字段 |
| IdFull `0x26` | 80 字节；Full 加稳定 64-bit NodeID |
| IdBoth `0x25` | 104 字节；Both 加稳定 64-bit NodeID |

Names 不需要 FILE_READ_ATTRIBUTES，也不依赖时间或 allocation 可表达性。每个 rich class 在 pattern 过滤前对快照的所有条目验证该类所需字段，不能用不匹配的 pattern 隐藏损坏；更换 class 沿原 cursor，不重新捕获。目录 Size 未指定，EOF 始终按目录 kind 报零；非负长度检查只用于普通文件。Full／Both 系列的 EaSize 对投影为 REPARSE_POINT 的 symlink 报 IO_REPARSE_TAG_SYMLINK `0xA000000C`，非 reparse 条目报零，不另加 tag 接口或伪造无 reparse。短名长度为零、数组清零，不制造 alias。

FileIndex 输出零；输入 FileIndex 和 INDEX_SPECIFIED 被忽略。所有名字来自真实 children，`.`／`..` 是合法 expression，但没有匹配的真实 child；不通过旧路径合成自身或父项。

### Expression、cursor 与 wire

Pattern 按现有 Windows 大小写等价匹配 UTF-16 units；`?` 消耗一个 unit，支持 `*`、`?`、DOS_STAR `<`、DOS_QM `>`、DOS_DOT `"`，`*`／`*.*` 匹配所有 child。expression 最多 255 units，迭代状态去重与固定 frontier 避免递归回溯；`MaxDirectoryMatchWork` 默认将完整过滤过程的累计工作限制为 16 Mi state visits，耗尽在任何条目可见前失败。

首次查询建立完整 snapshot。普通续页保持原 expression，忽略请求中新的 expression；RESTART_SCANS 与 REOPEN 都先丢弃旧条目，再完整重取，有非空新 expression 时采用它，缺席时保持原 expression。失败不恢复旧条目作为成功答案。class 切换保留 ordinal；没有跨连接 cursor、index registry 或第二把 cursor mutex。

首项固定头部装不下返回 INFO_LENGTH_MISMATCH，cursor 不前进。头部可容纳但名字不完整时，正常 QUERY_DIRECTORY body 返回 BUFFER_OVERFLOW，声明完整 FileNameLength、复制实际可容纳的原始字节 prefix，NextEntryOffset=0；保留该项，较大 continuation 重试同一完整项。后续条目必须完整容纳，条目起点八字节对齐、inter-entry padding 为零，最后 offset 为零且不要求末尾 padding。RETURN_SINGLE_ENTRY 最多一条；只有完整条目在整个 response 构造和最终授权通过后推进 cursor。

初次无匹配返回 NO_SUCH_FILE，独立 settled 状态使随后已耗尽的查询返回 NO_MORE_FILES，即使从未输出完整项也不会反复报告初始无匹配。partial warning 不结算 EOF。授权、健康、capture 或字段错误不能转成这两个状态。

有效的 BUFFER_OVERFLOW 正常 body 允许同一签名 related compound 的后继 CLOSE 执行；NO_SUCH_FILE／NO_MORE_FILES 仍走原前驱失败路径，受抑制的 CLOSE 不释放 ownership，需独立 CLOSE 或正常 teardown。输入 extent、严格 UTF-16、flags、class、FileId、credits 与完整响应 budget 在原 wire／connection 路径验证；未知 class 与已知未支持 class 分别为 INVALID_INFO_CLASS 与 NOT_SUPPORTED。

### 累计保留预算与清理

`MaxRetainedDirectoryBytes` 默认 64 MiB，是目录 cursor、snapshot、capture reservation 的单一进程累计上限；Server／Export 在既有 handleMu 下记录同一费用，Export 归属不建立另一配额。既有 `MaxDirectoryBytes` 仍限制单次 collector。一次 enumeration state 建立后保留 1024 字节 cursor／expression 费用，失败的 restart 也继续承担保留 expression。

捕获预算覆盖实际峰值表示与现有 HTTP 完整成功 body 的缓冲，不能假设 collector callback 先于 transport 分配。固定 allowance F 为 64 KiB；可用累计余额加已收费 cursor 部分为 A，collector limit `L=min(MaxDirectoryBytes,storage.MaxDirectoryBytes,floor((A−F)/5))`。L 不足固定 collector 费用时在 backing 前失败；capture 前另预留 `F−1024+4L`，保留原 cursor 费用。

名字 UTF-8 长度 n、规范 metadata 长度 m 的 entry 费用为 `C(n,m)=2×ObservedEntryBytes(n,m)+1536`，即 `2048+8n+2×MetadataRetentionBytes(m)`，同一 C 同时进入累计计费和 collector。该费用覆盖表示共存时的 slice／map、原名字及 UTF-16、metadata 解码与 clone。callback 的已取得费用独立记录；即使 ListResult 随后因自身余额拒绝最后一项，也释放这份 tally，不能用 UsedBytes 漏算它。并发 capture 争用余量时可明确失败，不保留 prefix。

完整捕获、验证和过滤完成后，最终 entry 费用 `R=sum(256+2×len(UTF16Name))` 同时覆盖每页临时 wire view；复用已投影名字，每项保存一个 matched bit。只有 backing call 与转换临时对象不再可达后，才释放 staging 和多余 C，最终保留 1024+R。预算是目录表示与成功 capture staging 的保守界限，不声称精确测量整个 Go heap；既有 transport、producer、错误 body 与 response admission 上限继续成立。

CLOSE 先 fence／drain 原 FIFO，再于 pending Basic reconcile 和 native close 之前清除本地 cursor 并释放剩余费用。drain 超时仍保存 state／charge；已经排空的只读 snapshot 不因 native close settlement 未知继续占用。restart 先归还旧 entries，失败释放全部本次 staging／entry tally，保留应继续拥有的 expression。tree、session、断线与停止复用相同幂等释放路径，饱和不能阻止状态或清理。

### 范围切分

| 延后能力 | 代价与保持的结构 | 请求结果 |
|---|---|---|
| [名字修改](../../proposed/feature/2026-09-28-smb-guarded-name-mutation.md) | 独立增加 ancestry guards；snapshot 的 raw order、ID 与 revision 不充当 mutation guard | rename／move／当前名字类由独立工作拥有 |
| [通知与缓存恢复](../../proposed/feature/2026-09-28-smb-change-notify-cache-coherence.md) | 增加通知、overflow 重取和原生缓存证明；冻结续页不冒充实时观察 | CHANGE_NOTIFY 与预热目录缓存资格仍独立交付 |
| 增量流式捕获 | 增加完整性证明；不能边读边输出或只验证匹配项 | 超过 capture 上限明确失败 |
| 持久或跨连接 cursor | 需要持久 handle／恢复状态；普通 FileId 与缓存不跨连接接管 | 断线后的旧 FileId 无效 |

## 备选方案

**每页回源或先过滤再验证。** 页间变化会混入 revision，未匹配的坏名字会被隐藏。完整捕获、全量验证和一次过滤保留枚举一致性；实际 StateScalars 只证明本次健康与生命期，不替换 snapshot。

**保留所有 Attr／namespace map，按请求信息类各建快照。** 同一目录会重复持有不需要的 payload，切换 class 也会换观察。紧凑共同字段和固定 rich-validity 类别保留同次事实，Names 不被不需要的时间／allocation 约束。

**把 FileIndex 做成可恢复的 byte-offset registry。** 需要 issued-index 映射、高水位、generation 与耗尽管理，而底层捕获没有可恢复流的承诺。零 FileIndex 与忽略 INDEX_SPECIFIED 不声称提供这种定位；真正跨连接续作需独立持久 handle 方案。

**为全局、volume、tree、FileId 分别建立配额及 owner。** 同一只读缓存会重复计费并引入另一套退休顺序。单一累计 bytes budget、Export 归属与原 FIFO／CLOSE 已覆盖保留责任；原 native／transport 限制仍独立成立。

**固定头部可容纳时仍拒绝首项部分名字，或按字符边界取整。** 会改变底层目录查询规定的 overflow 形状。完整长度与原始 byte prefix 保留协议结果，pending entry 不被消费，后续较大请求仍能取得完整项。

**按旧 CREATE 路径合成自身或父项。** rename、detachment 和 share root 会使该路径无法证明父身份。children-only 投影只输出真实 capture；`.`／`..` expression 不建立另一份 parent capture。

## 后果

一次成功枚举的页面共享完整捕获及 raw order；失败不输出被误认为完整的前缀，也不重写其它入口的名字。完整验证与一次过滤增加首项延迟和保留内存，capture cap 或 matcher work 耗尽会拒绝该枚举。紧凑快照和峰值 staging 预算增加计费与转换责任，但不新增底层 capability、mutation owner 或独立配额系统。

cursor 每页可能扫描并复制剩余 matched entries，连续 single-entry 页具有 O(N²) 的总工作量。collector 上限 8 MiB、每项 C 至少 2048 字节使一次 capture 少于 4096 项；保留费用中的固定 scalar 部分覆盖这份每页 wire view，response frame 另由原 admission 计费。

续页与 EOF 仍付出真实健康观察成本；冻结视图不提供客户端目录缓存失效。协议、direct／HTTP 与资源证明由[测试策略](../../../../docs/testing.md)记录；真实 Windows acceptance 的执行证据与 Windows 11 Home／Pro、WNet 及一秒缓存资格分别判定，模拟客户端不能提供原生通过结论。
