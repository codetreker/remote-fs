# 保留对象、访问声明与范围状态

本文描述 [`storage.FileStorage`](../../../packages/storage/files.go) 的对象身份、固定原子操作、会话与资源所有权。按路径的基础接口见[顶层设计](../architecture.md)，显式 S/X 见[文件占有](file-locks.md)，平台规则分别见 [FUSE](../client/architecture.md) 与 [Windows SMB](../client/windows-smb.md)。边界与取舍由[平台客户端隔离决定](../../../.agents/notes/implemented/architecture/2026-09-16-isolate-platform-filesystem-clients.md)记录。

## 身份、属性与会话

FileStorage 在 BoundedStorage 上提供 CheckFileStorage、FileState 与 NewFileSession。FileState 返回 VolumeIdentity、RootID 和 MaxEventBytes；NewFileSession 同时返回初始 status，使调用方在取得会话时便知道 action epoch。objectstore、localstore、locked、limited 与 HTTP 使用同一原生权威，不用路径重开模拟引用。

File 可保留普通文件、目录或符号链接。NodeID 绑定节点，EntryID 绑定目录项，FileReferenceID 属于会话内的一次保留；rename 不改变前两者，unlink 后旧引用不转向同名的新节点。Reference 只解析已有 ID，不新增 pin，不重开名字，也不复活已终止引用。根目录具有节点身份，没有可删除的普通 entry。

Attr 的 Kind、Size、时间与 revision 是通用事实；Metadata 是有序、版本化的 opaque envelope。metadata 至多 16 项、总编码 32 KiB、key 至多 64 字节；空 envelope 为 6 字节。客户端保留其它 key，通过 ExpectedRevision 更新完整 envelope，避免覆盖其它入口的字段。CreationTime／ChangeTime 缺失表示没有记录，不用 ModTime 或本机时钟补造。POSIX mode／UID／GID 和 DOS 属性的解释属于客户端。

FileSessionOptions 必须有效，可从 DefaultFileSessionOptions 开始。默认 lease 30 秒、history 1 分钟、单文件 1 GiB；每会话最多 4096 个引用、64 个活跃操作、256 个操作等待者、4096 个 range owner、65536 个 ranges、1024 个 pending actions 和 16384 个 actions。volume 的 FileServiceOptions 与 HTTP enrollment 上限另行约束总量。

Renew 确认并延长会话，Status 只观察。Epoch、Revision、Remaining、ActionEpoch、HistoryRemaining、Retired 与 Fenced 描述连续性；transport 扣除请求耗时，旧响应不重新启动租期或历史窗口。普通 I/O 成功与 TCP 存活不续期。会话失效后旧引用不能发布；尚有清理责任的 fenced 状态仍可被核对和清理。

## 条件操作与一致观察

| 调用 | 权威效果 |
|---|---|
| Retain | 按 NodeID 保留对象，可同时检查 metadata revision、claim、位置 witness 并安装 Prepared 意图 |
| RetainAt | 按精确父引用、名字、EntryID／NodeID 与 DirectoryRevision 保留已观察的对象 |
| CreateAndRetainAt | 在预期为空的 slot 创建节点，将初始 metadata／target、claim、引用和可选 Prepared 一并提交 |
| ResetAndRetainAt | 对同一预期节点原子重置内容、更新指定属性并取得引用；保留 NodeID |
| ReplaceAndRetainAt | 原子替换明确的目的 entry，创建新节点并保留；不能删掉另一个并发替代物 |
| Rename | 检查来源及目的 slot；NewName 独立指定输出字节拼写，只能占空 slot 或来源自身，只移除预期目的 |
| SetKind | 对唯一引用持有的空节点原子修改种类、target 和完整 metadata，同时检查 revision 与可选 witness |

EntryTarget 的零 ExpectedEntryID／ExpectedNodeID 表示明确要求缺席，非零表示精确身份。ExpectedMetadataRevision 与 DirectoryRevision 分别约束节点和目录观察。名字是精确字节；remote 不做大小写折叠、Windows 路径比较或 Unix 权限判定。

LookupAt 返回同一父目录版本下的精确名字结果；Found=false 是确认缺席，不附带虚构 Attr。ListAt 的 cursor 绑定 ParentID、Revision 和上次返回的名字，跨页不能混用目录版本。单页至多 1024 项、1 MiB，每项在加载名字与 metadata 前按 `256 + nameBytes + 4*metadataBytes` 计费。任何错误使结果失效，不能返回缺失尾部的成功列表。

IncludeLocation 与 IncludeLinkTarget 明确选择观察。EntryLocation 区分 Root、Linked 和 Detached，和 metadata／target 在同一观察中返回；祖先链至多 256 层、名字总量 64 KiB，单项名字至多 4096 字节。携带 witness 的名字操作在最终转换处验证每段身份及父 DirectoryRevision；CheckObservation 核对多次读取组成的观察。祖先移动或其同级目录出现新名字会使旧条件失效。纯身份 I/O 不要求位置，不用旧路径恢复 detached 名字。

## 访问声明与范围

AccessClaim 的 Uses／Excludes 使用 ReadContent、WriteContent、RemoveEntry。新旧声明在任一方向的 Uses 与 Excludes 相交时冲突。普通引用登记自身实际用途；路径和短暂操作也在同一权威中取得 admission，最终发布再次按 claims、范围与寿命排序。metadata 访问及父目录内创建名字不自动变成父节点的 WriteContent。

RangeOwnerID、RangeDomainID 与 RangeAcquisitionID 都是不透明身份。RangeScope 区分 advisory 冲突域与 Enforced 范围，remote 不解释 PID、flock 或 Windows batch。不同 acquisition 即使区间相同也保留独立身份；解除一个 shared acquisition 不解除另一个。普通闭区间可以覆盖未来 EOF；Boundary 标记表示切点 N，只与满足 `start < N <= end` 的区间相交，两个切点不相交。

RangeSnapshot 返回同一 Revision 的 Own、Other 与容量事实。ReplaceRanges 只替换调用方自有集合，并验证覆盖冲突、容量及生命周期的 revision。客户端从快照计算平台转换，不能修改其它 owner。强制范围参与相关 I/O；advisory 只约束同一域的参与者。平台错误与部分批次效果由 SMB／FUSE 保留，通用回执确认最终范围 revision 和效果。

WaitRanges 等待 guard 改变，不授予范围。新动作阻塞，已接纳 Pending 动作的重放可立即返回 Pending；调用方先核对原动作，不能忙轮询或重新规划未知结果。DetectDeadlock 使用通用 owner／resource 依赖进行有界环检测；登记、取消、授予与过期清理有序，容量或搜索不能完成时明确失败。R-CC-13 的有效会话阻塞等待不受任意快速重试次数截断。POSIX／flock 的转换、PID 诊断和描述符关闭由 FUSE 执行。

## 预备删除、drain 与回收

Prepared 是绑定引用和 EntryID 的持久退役意图；它不禁止相容的新引用，也不阻止目录增加子项。PrepareRemoval 可与创建／保留一起原子接纳。引用 Close 或有限 expiry 激活意图并退役；RemovalIfEmpty 在激活时发现目录非空，则消耗意图而不进入删除状态。

DrainEntry 将 entry 从 Active 转入 Draining，阻止新引用；目录还阻止经已有父引用的插入与 rename-in。最后引用退役后执行 detach，目录最终重检 IfEmpty。意图随 EntryID 改名，不删除占用旧名字的新 entry。CancelPrepared 操作所属 intent，CancelDrain 按当前 generation 取消单个 entry drain；它们不能互相替代，旧 generation 不能取消新 drain。

Prepared 是已经授权接纳的固定动作。新的 Prepare、Drain、Cancel 请求各自授权，退役执行已有意图的剩余效果；不保存凭据或授予任意新删除权限。最终效果仍检查 S/X、claims、完整性和用量。请求后来被拒绝，不证明先前意图不存在。未知清理保留责任和状态，不提前归还容量。

unlink 或替换去掉名字后，有引用的节点及内容仍保留。引用终止先 fencing，再排空已接纳操作，最后释放 pin 与物理内容；已知未生效的拒绝和未知清理分别处理。detached 内容仍计入实际用量，日志和目录只呈现仍有名字的节点。File 的数据库级恢复记录区分 Active 与已全库排空的 Quiescent：干净关闭后的重开跳过 File 等待，异常退出保留旧持久最大租期，下一次 session 先持久 Active。旧引用不恢复；与独立 Strong 的顺序见[本地持久存储](local-disk-object-store.md)。

## 内容操作与动作结果

ReadAt 返回同一捕获内容状态的 Attr 和区间字节。EOF 可返回空字节；EOF 前的正长度请求不能以空结果成功。引用钉住节点，不钉住内容 revision。objectstore 以不可变完整对象保存内容；读取在分配前预留完整对象和返回区间，旧对象被并发回收时仅在已知竞争路径重新捕获，当前对象缺失为 EIO。

WriteAt 只替换指定区间，Truncate 保留前缀并把增长部分置零。修改捕获当前完整内容、预留旧／新对象、构造不可变对象，再以原生 revision CAS 发布。上传不持有最终发布门；只有已知未提交且暂存清理成功的竞争才可重新尝试。ExpectedSize 同时检查捕获与最终发布大小，使客户端能表达依赖已观察 EOF 的写入；不匹配不改字节。

最终转换同时检查节点与内容 revision、引用／session 有效性、访问声明、强制范围、S/X proof 和记账。开始上传时有效不代表发布时仍有效。两个普通范围写可按实际顺序都成功；内部 CAS 拼接当前内容，不承诺打开时的内容依据仍新鲜。显式版本工作流见[独立决定](../../../.agents/notes/proposed/architecture/2026-08-19-ordering-and-versions.md)。Sync 验证已发布内容的健康与持久边界，Close 不提交本地 dirty 副本。

FileActionID 由服务端 action epoch 和随机 nonce 组成。相同 ID 保留原请求；不同参数不能重用同一动作。FileActionReceipt 只含值：Action、Operation、State、Effects、Reference、Observation、RangeRevision、Removal、Errno、Conflict 与 HistoryRemaining。metastore 不持有 storage.File；各层按自己拥有的 reference ID 解析引用。

Pending、Completed、NotApplied、Unknown 表达动作状态，Effects 在有错误时仍有意义。FileActionRetired 是当前引用／会话已终止的事实，不是所给动作已执行的历史回执：Action 为空，Effects、Errno 与 HistoryRemaining 为零，Operation／Reference 只标识清理范围。FileError.NotAdmitted 只证明本次调用在动作准入前被拒绝，不能证明同 ID 的较早调用未执行。

QueryAction／CancelAction 使用原 ID。Unknown 保持 EIO 和原错误链，不允许提交替代动作；是否以及何时核对由拥有原意图的调用方决定。有效 session 与声明的历史窗口内保留核对结果；旧 epoch 的未知动作不会重新执行，退役、重启或窗口结束不承诺永久历史。取消只在确认结果允许时报告中断，不能把 context cancellation 当成回滚证明。

## HTTP、复制与预算

HTTP 使用既有 file／file-control 动作入口、一份 session registry 与 storage.Operation 词汇。Session 能力是随机凭证，reference 与动作历史由 native session 持有。路径、名字、target 与内容用 byte 字段无损传输，平台错误和名字比较不进入 codec。请求先接受[业务授权](authorization.md)，再访问会话、引用或动作结果；已有 ID 不绕过策略。

transport 对丢失或无法验证的修改响应返回 Unknown／EIO，不自动 QueryAction，也不推断 NotApplied。SMB、FUSE 或 SDK 若持有原计划，可明确核对同一个动作；迟到答复和取消通过原 receipt 处理。HTTP 没有另一份 open ACK、引用恢复或历史状态机。HandlerOptions.Files 限制 registry 会话数与可申请的 FileSessionOptions，默认 64 个会话。Handler.Close 退役并排空自己创建的 registry，backend 仍由宿主持有。

replicated storage 转发同一个原生 session、引用、动作与 owner。按名 metadata 可来自副本；保留对象的属性与内容直接走权威。提供日志时，修改结果携带权威 barrier，replica 等待同 incarnation 的位置达到它；detached 修改没有虚构路径事件，仍可核对已有 volume 进度。没有日志的直接 HTTP 不制造 barrier，需要复制确认却缺少 barrier 时明确 EIO。

FileServiceOptions 限制每个 volume 的 session、动作、Prepared、drain、claims、owner、ranges、等待依赖及完整内容物化。默认单文件 1 GiB、同时物化 2 GiB、32 次 materialization、8 次状态竞争尝试、单次内容操作 30 秒。读预留完整对象与返回区间，写预留旧／新内容；不能只按 patch 长度计费。MaxRetainedFiles 默认 65536，退役引用在实际释放前仍占名额；包装同一 volume 不能建立独立预算。

CLI 暴露 max-retained-files、max-file-size、max-file-staging-bytes、file-operation-timeout、http-max-file-sessions，以及 file-session-lease、file-session-history、file-session-max-actions、file-session-max-pending-actions。动作和 pending 默认分别为 16384、1024；staging 至少容纳两份最大文件。其它共同上限通过库的 FileServiceOptions 配置。

## 显式 S/X 与平台规则

强 S/X 保护资源的内容、存在和身份；普通 Retain 不自动取得强占有或 advisory。只持 S 不能修改，有活动保护时最终发布核对 proof；没有保护冲突时普通匿名修改可成功。经授权 unlink／替换使旧的命名资源成为 TargetGone，原 grant 不转移到同名新对象。已保留节点仍保持自身身份。

Windows share/disposition、DOS 与精确 lock batch 在 SMB 解释，POSIX mode、flock／fcntl owner 和关闭转换在 FUSE 解释。它们把已解释的通用 claims、range sets、条件操作与删除意图交给同一权威。平台转换不改变显式 S/X 的有限期限，也不让任何平台入口绕过共同发布检查。
