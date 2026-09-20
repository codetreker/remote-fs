# Agent Note: 中立元数据与访问控制

Status: implemented

## 问题

同一个 volume 会同时被 Linux、Windows 与编程入口访问。节点种类、创建与变更时刻是共同事实，POSIX 权限与 Windows 属性则是各平台自己的解释。若通用属性继续直接携带 `fs.FileMode`，或者为 Windows 再增加固定字段，远端契约和持久格式会随每个平台扩张；若平台 metadata 只保存在客户端，它又无法跨入口持久、复制或参与原子修改。

标准 Linux advisory lock 与 Windows 的共享限制、强制字节范围保护也需要在同一活跃 authority 内共用顺序。只在某个客户端进程里保存限制，会让其它入口绕过；把 FUSE owner、SMB access mask 或平台解锁政策下传，则会让 storage 实现承担客户端协议。

已有 FileSession、保留 File、advisory coordinator、原生发布门和 HTTP 动作历史已经承担对象身份、有限生命周期、等待、结果核对与清理。新的中立能力应复用这些所有者，不建立第二套引用、租约或发布机制。

## 决定

### 节点事实与平台 metadata 分开

`storage.Attr` 使用非零 `ID`、`NodeKind`、`Size`、`AccessTime`、`ModTime`、可选 `BirthTime` / `ChangeTime` 和有界 opaque metadata。`NodeKind` 只区分普通文件、目录和符号链接；平台属性不进入这个枚举。

`BirthTime` 可由调用方显式设置，`ChangeTime` 只由 authority 维护，不进入 `AttrChange`。新节点在创建事务中记录两者；内容、共同属性、metadata 或名字关联的成功修改推进 `ChangeTime`。旧节点和旧日志缺失的时刻保持未知，副本保留 authority 给出的值，不从当前时间或 `ModTime` 推测历史。

平台拥有自己的 namespace。Linux 使用 `posix.permissions.v1` 保存四字节 little-endian `uint32`，只允许 `07777` 范围内的权限与 special bits。存在但畸形的 payload 是错误；缺席时 FUSE 只在展示层采用普通文件 `0644`、目录 `0755`、符号链接 `0777`，不会把默认值写回 authority。Windows 属性可以在后续平台 PR 中使用独立 namespace，不改变 Linux 或 SDK 的语义。

metadata 每节点最多 16 个 namespace，namespace 名最长 128 字节，单值最长 32 KiB，规范编码总长最多 64 KiB，版本 token 最长 64 字节。编码按 key 排序并带长度前缀；解码拒绝未知格式、重复或乱序 key、非法版本和尾随数据。结果及调用方输入都深拷贝可变字节。

`MetadataAccess` 按 NodeID 原子比较并替换一个 namespace；`ReferenceMetadataAccess` 在已有 File 身份上执行同一操作。空 expected version 表示该 namespace 必须不存在，空 data 表示一个存在的空值；authority 为成功结果分配非空版本，只比较这一 namespace，并保留其它 namespace。已知条件不符为 `ErrConditionConflict`，且没有修改；未知结果仍按原发布规则失败。返回属性和 metadata 在载入或保留变长数据前经过请求拥有的结果预算。

### 使用声明与范围控制保持中立

每个成功打开的 File 注册一份 `UseClaim{Uses, Deny}`。读写打开自动加入 `ReadData` / `WriteData`，调用方不能用空声明绕过已经存在的限制；公开目录读取派生 `ReadEntries`，`DeleteName` 为身份名字操作保留。新旧 claim 双向比较：任一方声明的 Deny 与另一方的 Uses 相交即冲突。实际读取、写入、目录枚举与发布在原生最终顺序重新检查对应 Uses；较早的客户端预检不能代替这一检查。

公开 replicated `List` / `ListBounded` 在确认本地副本仍可用后回源 authority，使目录枚举与当前 `ReadEntries` 限制共享 native 顺序。路径 Stat 与负查找继续使用副本，保留原来的本地名字查询收益；目录结果不能从副本绕过新建立的 deny。

File 可返回只代表该确切活引用的 `UseScope`。它在 FileSession 内验证所属 volume、NodeID 和存活状态，本身不授予方法、业务权限或 Strong proof。`UseOwners` 以这份 scope 注册 `OwnerReference` 或 `OwnerExplicit`；数值 owner 不可伪造权限，Group 只合并死锁参与者，不共享 claim、范围或自我豁免。

`RangeControl` 保留原动作 epoch、nonce、有限历史、等待、Query 与 Cancel。`DomainRecord` 和 `DomainWholeFile` 表达 advisory 冲突；`DomainEnforced` 通过显式 `DenySelf` / `DenyOthers` 约束真实 `ReadData` / `WriteData`。核心不从 shared/exclusive mode 推导某个平台的重复获取、转换或解除政策。

`Bytes` 表达正长度区间，`Boundary` 表达字节间边界；零长度不会被猜成 EOF。`Replace` / `Subtract` 保存 advisory 范围代数，`AddExact` / `RemoveExact` 保存不合并的强制 claim。一次请求的 Commands、Claims 和 Effects 各最多 64 项；完整结果无法保留时，在释放或授予任何状态前拒绝。Rejected 结果的 `FailedAt` 指向原 Commands 中失败项的零基下标；与某一命令无关的整批 admission 拒绝保持 nil。失败项之前已经成功的 release 保留在 Effects 中，本批新增 acquisition 则回滚。`DropBeforeAcquire` 已经完成的释放即使后续获取冲突，也不能被报告为整项未执行。取消本身不证明没有授予，结果未知时相关访问持续失败，直到核对或拥有者清理得到确定结果。

FUSE 负责把 kernel owner、`flock`、传统 POSIX record lock、关闭与转换规则映射为这些中立命令。`Flush` 对对应 owner 执行范围清理，最终 `Release` 关闭 File；直接 File API 不推断进程 owner。Linux errno、PID 诊断和未来 EOF 的解释留在 FUSE 层。

UseClaim、UseOwner、range、等待和动作回执是当前 authority 的有界内存状态，并受 FileSession 生命周期约束。authority 重启或 incarnation 改变后，旧 File、scope、owner、range 和请求历史都明确失效；它们不从 SQLite、变更日志或客户端 ledger 恢复。客户端必须建立新会话并重新取得所需状态，不能把旧持有者静默重绑到同名或同 NodeID 对象。Strong S/X 的持久恢复是独立机制，不由这些状态继承。

### 一个持久迁移和一个 wire 版本

SQLite migration 6 在既有 schema 准备事务中把合法 v5 mode 转为 `NodeKind` 与 `posix.permissions.v1`，并为 nodes 和 retained changes 增加可选 BirthTime / ChangeTime 与 metadata。它保留 NodeID、内容、原始名字、用量和日志事实；旧时间保持未知。migration 1 至 5 不变，旧布局先完成 preflight，schema 提交后继续经过既有 `Commit` 与 witness `Accept`。

每个 volume 的 `metadata_used` 同时计量当前节点、detached 节点和 retained change 中的规范 metadata。SQL triggers、启动完整性校验、复制写入、日志裁剪与物理删除维护同一计数；默认上限为 64 MiB，独立于内容 quota、单节点上限和 HTTP 驻留预算。缺失 trigger、错误 storage class、畸形 envelope、计数不一致或超限均拒绝打开或写入，不能从仍可读取的字段拼出成功结果。

HTTP v4 是不兼容的边界：基础属性从 POSIX mode 变为中立节点事实，文件锁 DTO 变为中立 owner/range，所有路由与 response marker 同时升级，v3 不作为兼容旁路。metadata CAS 请求使用独立 decoder，允许空 expected version，并继续要求 canonical base64；响应 `OpaquePayload` 必须携带非空 authority version。

能力协商在 session 和 File 结果里为原子打开、NodeReference、namespace、状态、删除、条件修改、目录 metadata 与名字观察保留布尔字段。当前 server 只宣告 `Metadata`、`Owners`、`Ranges` 和 File `Scope`；尚未实现某个 facet 的 v4 client 接受并忽略对应已知预留 bool 的 true 值，因为本地方法集仍不包含该能力。任意未知字段继续失败。Open/OpenNode 的可选 node 与 File.Close/FileSession.Close 的可选 barrier 使用同一兼容原则，为身份结果和可能产生持久修改的清理预留位置，不把它们解释成已交付行为。

每项可选能力的 Check 检查完整包装链；缺少底层保证时，Check 和调用都以 `EOPNOTSUPP` 失败。limited、locked、objectstore、localstore、replicated 与 HTTP 包装器保留预算、原始错误、barrier、引用和清理所有权，不能用路径重开、本地 mutex 或默认值模拟能力。

## 备选方案

**继续让 storage 使用 `fs.FileMode` 和 Linux lock 类型。** 修改量较小，但 Windows 或第三种客户端只能把自己的事实塞入 Linux 语义，所有远端实现也必须理解本不属于它的平台规则，因此不选。

**把 Windows metadata 与共享状态只放在本机入口。** 它不需要迁移持久格式，但其它 Windows、Linux 或 SDK 入口能够绕过保护，metadata 也无法由 authority 持久和复制，因此不选。Use/range 只承诺活跃 authority 内的共同执行，不因此取得跨重启恢复。

**为每个平台增加固定 Attr 字段和锁方法。** 直接但不可扩展：新增平台会再次改变公共类型、wire 和所有 wrapper。命名空间 payload 与中立 Uses/range 只固定共同约束，平台 codec 留在客户端。

**沿用 HTTP v3，并让缺失字段取默认值。** 旧 peer 会把新 Attr 或 range 误解成合法旧结果，形成静默错误。显式升级路径和 marker 能在解释状态码与 body 前拒绝不相容 peer。

**同时交付全部文件能力。** 这会把原子 OpenAt、NodeReference、名字观察和删除状态绑进同一次接口与持久格式变更，任何一部分的身份或恢复问题都会阻塞共同原语。它们可以在当前中立事实和协商位置上独立增加，因此不纳入本决定。

## 后果

节点事实与 opaque metadata 由 SQLite 持久并进入复制日志；同一活跃 authority 内的共享限制和范围保护约束所有入口。Linux FUSE 继续保持原有 advisory 可观察语义。代价是一次全协议 v4 升级、SQLite v6 数据转换、metadata 的独立持久额度，以及每个 authority/session 的有限内存控制状态。重启使普通 File/Use/range 持有者失效，不能以重建同值状态冒充连续性。

本决定接续[Windows 系统网络驱动器](../../proposed/feature/2026-09-16-windows-network-drive-support.md)中的平台中立约束，并扩展[活跃文件句柄](2026-09-08-live-file-handles.md)、[显式文件占有](2026-09-07-file-locks.md)和[业务方授权](../feature/2026-09-10-host-provided-authorization.md)的当前实现；它不取代这些决定。

原子 OpenAt、NodeReference、身份 namespace 操作、当前名字或完整目录 metadata 观察、pending deletion、条件文件修改和 SMB/Windows 适配仍未交付。Windows 属性的 codec、`ARCHIVE` 与内容的同事务更新、共享模式映射、原生系统客户端验证也不由这些中立原语自动成立。
