# Agent Note: SMB 文件内容访问

Status: implemented

## 问题

Windows 程序取得 SMB FileId 后，需要通过该句柄读取、范围写入、追加和刷新原对象。名字改名、删除或被替换不能使已有句柄转向替代物。写入必须在调用中确认服务端结果，内容与 Windows ARCHIVE 同时生效；断线、权限变化、关闭和资源饱和不能让未知写入被当作未发生，或让已经接受的内容失去持有者。

## 决定

SMB 3.1.1 的 regular-file READ、flags=0 WRITE 和 FLUSH 使用[有界 CREATE/CLOSE](2026-09-28-smb-bounded-create-close.md)保存的 typed FileId 与同一 authority。字节内容、固定派生 metadata、句柄排序、原 action 恢复、清理证明和有界失败状态共同构成一条内容访问路径。文件信息／EOF 和 volume 信息由[信息提案](../../proposed/feature/2026-09-28-smb-file-data-information.md)保留，范围锁、名字、通知／缓存与原生资格仍为独立能力。

该决定服务 R-FS-6 至 R-FS-8、R-CON-1、R-ERR-1 至 R-ERR-4、R-SEC-5、R-INT-8、R-WIN-2、R-WIN-5 和 R-WIN-10。实际结构见[SMB 端点设计](../../../../docs/design/client/smb-endpoint.md)与[文件契约](../../../../docs/design/server/file-handles.md)，错误与竞争验证见[测试策略](../../../../docs/testing.md)。这些命令的交付不宣布完整 Windows drive 或私有缓存资格完成。

### 平台解释与中立固定效果

OpenAtOptions.ContentMetadataEffects 将固定 namespace 转换密封在 exact writable File 上；OpenContentMetadata 检查整个链，ReferenceContentMetadata 只观察已 enrollment 的一个 namespace，返回 NodeID 与明确存在／缺席的 OpaquePayload。descriptor 包括 Namespace、PayloadBytes、PresentPrefix、AbsentPayload、ClearMask、SetMask；最多 16 项、单 payload 32 KiB、合计 64 KiB。prefix 对应 masks 为零；不存在值使用已批准模板，存在空值不冒充缺席。旧 FileOpenOptions 不接受该 enrollment。

SMB 密封 smb.windows 的八字节 SMW version-1 payload，固定清除 NORMAL、设置 ARCHIVE，保留 READONLY/HIDDEN/SYSTEM 与其它 namespace。Windows 解码和 READONLY 判定留在 SMB；authority 只验证通用格式、条件与固定转换，不执行平台 hook，不需要 host 注册平台策略。

FileMutation.ContentEffects 只用于非空 WriteAt／Append，Attr 与任意 Metadata 为空，每个所选 namespace 必须有显式 ExpectedMetadata entry；空 token 表示确认缺席，遗漏 entry 非法。native 最终 publication 同时核对 exact scope、数据 Use、metadata token、descriptor 格式和内容 revision，以 (old &^ ClearMask) | SetMask 计算结果，与内容、时间、日志一次提交。原 OpenAt descriptor、mutation target／command／enrolled vector 进入 action digest，每处 slice／map 深复制。

enrollment、限定观察与每次 mutation／typed replay 都经当前实际业务授权；AccessRequest 携带准确 descriptor，Metadata=nil 不能绕过 OpFileSetMetadata。OpenAt 批准不构成终身授权。公开 Stat、SetAttr、SetMetadata 和任意属性修改继续独立受 MetadataAccess 约束，byte-only 与 append-only 句柄不能借派生效果扩权。

### 协议与读取／写入

wire/file_io.go 对固定结构、当前 compound command extent、长度和 DataOffset 作无溢出验证。WRITE DataOffset≤256 且 DataOffset+Length 在当前 command 内；非空另要求 DataOffset≥112，空数据没有该下界但仍验证 extent。原签名字节不变，related FileId 只在 frame-local context 中继承。

READ 一次 ReadAt 返回同 revision 的 Data／Attr，核对 immutable NodeID、regular kind、非负 Size、allocation 和 captured EOF。合法零字节及低于 MinimumCount 为 END_OF_FILE；正 Length 且在 captured EOF 前非法零返回为 I/O 错误。正短读允许，不以第二次 ReadAt 拼满结果，MinimumCount>Length 不增加协议外 invalid 规则。

WRITE_DATA 支持普通范围与增长；APPEND_DATA-only 使用已有 MutateAppend，普通 offset 不能覆盖，-1 也选 authority 当前 EOF，其它高位 offset 拒绝。追加位置与内容／ARCHIVE 同动作排序。空 WRITE 用显式零 Data MutateWriteAt、空 ContentEffects 取得同对象事实，不改变内容、EOF、ARCHIVE 或时间，不调用公开 Stat。

READ Padding、Channel NONE 的 RemainingBytes 与 channel-info 被忽略；未协商 compression 的 READ compression 请求可普通返回。RDMA invalid；UNBUFFERED 与其 WRITE_THROUGH 配对 unsupported。当前 CREATE 排除 NO_INTERMEDIATE_BUFFERING，SMB 3.1.1 WRITE_THROUGH-only 必须在效果前 INVALID_PARAMETER，不能用 buffered mutation+Sync 模拟。依据为 [MS-SMB2 WRITE 处理](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/829f93f5-ed10-4f12-8347-42d235019459)。有效 unbuffered 配对需要另行切定 geometry 与 CREATE 能力范围。

### 逐句柄顺序与原动作

file_io_gate.go 保存 bounded FIFO、running pointer、sequence 与 fence。每次请求在入队前授权，取得 turn 后 backing dispatch 前再核对身份与当前授权。短锁按 authority installation、tree fileMu、session mu、handle ioMu 排序，全部释放后才访问 backing。queued 项保留原 frame 费用，取得 turn 前不复制 Data；不同 FileId 并行，同对象最终顺序由 authority 决定。

write_owner.go 在 dispatch 前预留原 immutable Data、action、token、effects、完整 response 与 failure-record。Completed receipt 只允许同 ID／reference／负载 typed replay，不能代替 Attr 与全链 barrier。bound NotExecuted 的 metadata 条件冲突最多重读两次新条件并 mint 两个新 action；Unknown、Retired、无 Operation 的 NotExecuted、失联或授权拒绝不 remint。Unknown 结束 active execution，但保留 owner 与 charge，不能阻塞无关 FileId、renew、状态与 cleanup。

FLUSH 用独立 flush_owner.go 的无 Data confirmation，通过同句柄 FIFO 后调用 exact File.Sync。HTTP Sync 不要求 Action、不进入 pending journal，也不先扫描全 session pending；仍保留 post-Sync barrier response。Sync 可重新确认同一引用，不重写 WRITE，不从旧路径重新打开。

CLOSE 在既有 requestCloseMu／closeLifetime 保护下 fence 新 I/O、移除 queued、排空 running，再有界 reconcile WRITE／FLUSH 并进入 exact close。Unknown 不必须先变成 Known，关闭本身是获得 no-future-publication／settlement 证明的途径。post-query Attr 在 drain 后捕获。全部共享 tree 退休时，既有固定 membership 和完整父证明可终结原引用；live sibling 阻止误关共享 authority。

### HTTP 恢复与责任终结

不超过 8 MiB 的 explicit WriteAt／Append 有独立 file capability＋nested action＋immutable request pending entry 与 recovery gate；其它 FileId、Renew、Status、Sync 与 cleanup 不扫描它。初次使用 data admission，原动作重投使用 POST /v5/file-recovery，业务授权仍是原 file.mutate／write／所选 metadata 效果。recovery envelope 为 min(maxBodyBytes,12 MiB)，每 session 一项、无排队，server 全局 MaxSessions；client 全局独立 MaxConcurrentLockControls。256 KiB control 和关闭保留容量不被大 data replay 占用。既有 native history、digest、quota 和 close receipts 不重设计。

exact reference 或完整父 session 的正面 Released 与 HTTP 下游链 settled 后，HTTP 在原 action gates 内撤下该 file／session pending entries、fence 迟到 replay 并释放本层复制 Data。等待 gate 不持短锁。HTTP 不确认其上方 replica 的 barrier；外层 SMB owner 保留原 Data 与额度，直到包含 replica 的完整链关闭／settlement 证明成立。只凭 ESTALE、receipt Retired、局部 Released 或待结算 barrier 不能释放相应责任；原 ExecutionUnknown 不变成未执行。

write_diagnostics.go 的 WriteFailure 不包含内容，保存不可复用 WriteOwnerID、export／FileId generation、NodeID、action、operation、byte count／digest、execution、FLUSH durability、response disposition、terminal／payload／reference／settlement 事实与净化 error category。ResponseSent 只代表本机发送，不证明应用消费。事实独立于已释放 handle，Status 返回 copied snapshot；diagnostic slot 在效果前预留，不自动逐出 Unknown。

Server.AcknowledgeWriteFailures 对整批 exact typed IDs 先验证后移除，只接受已 final response、terminal、payload released、chain settled 的失败记录；重复、缺失或 active／stale ID 整批失败。宿主先保存这些 immutable facts，并接管继续报告责任；ack 不更改原 Unknown、不确认执行、不重发内容。未完成资源或失败事实未转移时，Shutdown／Unpublish 返回错误且保留 stopping；真实责任完成及转移后后续停止可成功。

### 容量与状态

MaxHandleIORequests 默认 32 且不超过 MaxRequests；MaxWriteOwners 默认 128，MaxRetainedWriteBytes 默认 32 MiB，global 与每 export 同时计量。retained bytes 包括 Data、tokens 与 descriptor；WRITE／FLUSH owner 共享 MaxDiagnosticBytes 固定槽。READ/WRITE credits 按 max(1,ceil(requested Length/65536)) 计，不算 header／padding。READ 在 backing 前预留 requested Length 的完整响应，WRITE／FLUSH 预留固定完整响应；容量失败没有对象效果。

Status 包含 WriteOwners、UnknownWrites、PendingFlushes、RetainedWriteBytes 和 WriteFailures；HandleOwners 继续独立呈现引用 owner。恢复与 cleanup 使用有界独立 context 和原 operation 关联，不因请求取消证明未执行。日志不含 Data、metadata payload、raw names、credentials 或原始 backend error 文本。

## 备选方案

**直接 File.WriteAt，再补 metadata 设置 ARCHIVE。** 两个效果之间可发生观察、竞争、故障或响应丢失，内容成功但 ARCHIVE 未变违反 R-WIN-5，且一个 action 不能恢复两半的准确结果。选择在同一条件 publication 完成内容与派生属性。

**增加通用 metadata 权限。** 让 WRITE-only 句柄获得 Stat／任意 SetMetadata 可以接现有方法，却扩大 Windows DesiredAccess，允许数据写者清除 READONLY 或改变 HIDDEN/SYSTEM。密封效果只允许已批准的固定转换和限定观察，保留公开权限分离，因此选择后者。

**远端 host 注册平台策略／纯函数 hook。** 在 authority 内执行 Windows 专用规则可省观察往返，但需部署和协商 SMB 策略，改变中立存储的集成要求。采用由 OpenAt 业务授权批准的通用 descriptor；backend 验证固定数据转换，不执行 endpoint 代码或解释 Windows 常量。

**客户端提交有界 replacement payload。** authority 可以检查新旧位差范围，但与普通 `Metadata` 更新共用字段会增加权限旁路。选择 sealed descriptor + effect index，客户端不提交替代 payload，最终 authority 独立计算固定效果。

**同句柄并行 READ，另为 WRITE／FLUSH 建排序。** 可提高单句柄吞吐，却需要另一套 prior-write completion 与 CLOSE fence 证明。选择单句柄 FIFO，以明确 admission sequence 验证刷新和排空；不同 FileId 保持并行。若后续负载证明该限制有实际成本，可在保持顺序承诺下扩展调度。

**永久终态 payload escrow 或导出失败内容。** 可以让宿主保存原 bytes，但增加内容持久化／人工重放与敏感数据导出 API。选择在 no-future-publication 与全链 settlement 正面证明以前保留原内容，证明后保存不可自动抹去的 bounded failure facts；端点记录操作错误／未知及响应发送状态，不能假设应用已收到错误。

**将完整 7.4 放一个 PR。** 文件字节、信息类和 volume geometry 可以同时接入，但三个目的的权限、格式、证明与失败面不同，review 容易扩大。选择 7.4a 内容访问、7.4b 文件信息、7.4c volume 信息；内容确认、条件效果及关闭排空在 7.4a 一次完成，后段追加 codec 和事实投影。

## 后果

内容权限与公开 metadata 权限保持分离；固定 descriptor 让同一中立 publication 满足 Windows ARCHIVE，同时各入口继续以自己的规则解释 metadata。增加共享 descriptor／观察契约要求 native、HTTP 与 wrappers 全链验证，漏掉深复制或当前 effect 授权会扩大修改能力。

逐句柄 FIFO 限制该句柄吞吐，不同 FileId 保持并行。metadata 争用最多两次 fresh-condition retry 后明确失败，保证 READONLY 判定与原动作事实。Unknown payload 与诊断有界，饱和拒绝新数据但保留恢复／关闭／状态容量；宿主长期不转移终态事实会消耗有限 diagnostic slots。

package-local 正常、权限、CAS、byte-only、append、empty WRITE、wire／credits、未知恢复、并发 CLOSE 和真实 HTTP 链验证承担这些保证；完整测试策略与观察到的命令结果由当前验证记录提供。schema 未变，旧 FileOpenOptions 不因本能力改变；普通 File.WriteAt(empty) 的旧公开分支没有顺带重构。

QUERY_INFO、SET_INFO／EOF、volume presentation、目录枚举、directory FLUSH、range LOCK／CANCEL、名字／删除义务、通知／缓存及 WNet／原生支持资格仍由独立任务承担。真实 Windows redirector 的缓存可能遮蔽正确 backing 结果，本段 Go／wire 验证不等于完整 network-drive 资格。
