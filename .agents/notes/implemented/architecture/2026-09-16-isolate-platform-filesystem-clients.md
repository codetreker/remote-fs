# Agent Note: 将平台文件系统语义隔离到客户端

Status: implemented

## 问题

同一远端 volume 同时服务 Windows、Linux 与编程调用方。平台协议可以不同，对象身份、修改结果、用量和生效中的访问保护不能因入口不同而矛盾。把 Windows／POSIX 解释放进公共 storage、HTTP 和数据库，会要求每个后端理解自己不服务的平台；只在本机保存保护，又无法阻止另一入口绕过或在客户端退出后完成清理。

[本机 SMB 决定](../feature/2026-09-15-windows-local-smb-package.md)建立了可嵌入的协议引擎与本机映射，其 Windows 专用 authority 接口把平台语义带进远端。FileSession 的 flock／POSIX owner 也存在相同问题。单纯把类型改叫 native，或仅保留 FileSession 加强 S/X，都不能表达目录一致观察、精确父身份、双向使用相容性、有序范围部分效果和持久退役清理。

## 决定

### 平台解释与共同事实

Windows 客户端内的 SMB server 解释名字比较、路径限制、DesiredAccess／ShareAccess、DOS metadata、disposition、状态码、范围批次与 SSPI 身份。FUSE 解释 Unix mode／UID／GID、flock／POSIX owner、进程关闭与锁转换。远端处理精确字节、节点种类、内容、通用时间、版本和固定状态转换，不识别来访平台。

```text
Windows 程序 → 系统 SMB → Windows 客户端内的 SMB server
                                    | Windows 规则与本地计划
Linux 程序 ───── FUSE ───────────────+ Unix 规则与 owner 映射
编程调用方 ─────────────────────────+
                                    |
                      Storage / FileStorage / FileSession
                                    |
                       一份 file HTTP 协议与 session registry
                                    |
                       节点 / revisions / claims / ranges
                                    |
                         原生 storage 与持久 metastore
```

服务端仍用宿主 OS 的 fsync、flock、xattr 与平台文件实现持久性和所有权；这些不是客户端文件系统语义。强 S/X 继续是独立能力，普通打开既不自动取得强锁，也不自动取得 advisory。所有入口的最终发布共同检查已有保护。

### 固定原语与分层结果

FileStorage 提供能力检查、FileState 与返回初始 status 的 NewFileSession；File 可以保留文件、目录和 symlink。NodeID、EntryID 与会话 reference 分开：rename 跟随 entry，unlink 后旧引用继续绑定节点，同名重建不能恢复旧身份。

RetainAt、CreateAndRetainAt、ResetAndRetainAt、ReplaceAndRetainAt 与 Rename 接受确切 slot 和版本条件。需要的内容重置、初始 metadata、claim、引用与 Prepared 在一次固定提交内生效；不以打开后无条件 Truncate 拼接。Rename.NewName 把输出拼写与已观察目的 slot 分开，ExpectedSize 为依赖已捕获 EOF 的写入提供通用最终条件。

这些原语分别服务于目录相对访问、安全保存、条件编辑、协作使用与对象清理。server 不接受脚本、比较器、任意事务列表或关闭 callback。metastore receipt 只传递动作状态、Effects、观察和不透明 reference ID；storage／HTTP 各自解析所属层引用，metastore 不嵌入提供内容字节的 storage.File。

HTTP 只有共同 file／file-control 词汇和 codec；registry 管理 HTTP 会话 enrollment、生命周期和清理，native session 拥有引用、动作与历史。没有 Windows 平行 endpoint、open ACK 或另一份 HTTP 引用／动作账本。

### 名字、位置与平台 metadata

Publish 不安装持久 EnableWindows 或全 volume profile。其它入口可以写入 Windows 不可表示的名字；SMB 用同一 DirectoryRevision 的完整有界 ListAt，在本地检查 UTF-8、保留名、大小写唯一性和路径表示。受影响的按名访问、列举或通知明确失败，不隐藏、重命名或删除数据。无需名字观察的旧身份 I/O 可以继续。

EntryLocation 与 metadata／link target 一致捕获，包含每层父关系和 DirectoryRevision。SMB 检查完整祖先目录投影，最终操作验证 supplied witness；祖先移动或祖先旁的新冲突名字都使旧条件失效。CheckObservation 验证由多次调用组成的名字观察，LookupAt 提供精确名字存在／缺席事实。该位置证明有固定深度、名字与字节预算，不是整个 volume 图快照。

NodeKind、size、时间和 symlink target 是通用数据；Attr.Metadata 保存有界、按 key 排序的 opaque envelope，完整替换要求 ExpectedRevision。FUSE 拥有 posix v1，SMB 拥有 smb.windows v1，更新保留外来 key。可选 key 缺失采用明确配置的呈现缺省；已有值损坏或版本不支持时失败。缺少历史时间保持未知，权限呈现不授予业务访问。

symlink target 在 remote 中没有路径执行含义。SMB 在 Export 内解释和遍历目标，并用一致位置及最终条件防止越界；Unix 解释属于 FUSE。FUSE 可以报告链接类型，现有 Readlink／Symlinker 仍拒绝，不能由类型一致性推导完整链接支持。

通用 Notification 保存事件时 Kind、ChangeMask、前后 Attr／opaque metadata／EntryLocation 图像。删除及后续修改不影响历史分类，SMB 自己决定目录符号链接与 Windows filters。图像有界且随保留历史拥有；不存在 Windows 推导的服务端 Directory 位，也不反查后来节点补造过去。

### 使用声明、范围与等待

AccessClaim{Uses, Excludes} 双向判定相容性：任一方 Uses 与另一方 Excludes 相交即冲突。ReadContent、WriteContent 与 RemoveEntry 区分内容和删除使用；metadata 与父目录插入不自动占用父内容写入。普通引用及短暂路径操作登记实际使用，最终发布按新取得的声明重新排序。读且拒绝删除、允许写的引用不会阻止另一 HTTP 写入，但会阻止删除；这维持 R-CC-14 的跨入口保护。

RangeSnapshot 捕获 guard revision、全部相关自有／其它范围和容量事实。ReplaceRanges 只能改变调用方自有集合，每个 acquisition 保持独立 ID，相同 shared 区间不会被 remote 去重。Boundary 提供通用切点冲突，平台在本地计算实际范围。enforced 范围参与相关 I/O，advisory 只约束同域参与者；FUSE 的 PID 不进入共同 owner。

SMB 按条目顺序计算最终集合、部分效果与平台错误。lock 后项 UNLOCK 无效时保留成功前缀，真实 FAIL_IMMEDIATELY 冲突回滚本批次取得的前缀；unlock 后项带 SHARED／EXCLUSIVE 无效时保留前面的解锁。规则来自 [Processing Lock Requests](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/670c7eda-e683-4923-9477-414303959613) 与 [Processing Unlock Requests](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/79eb3c91-563b-4d48-a51c-0974f9d144f8)。确定的最终集合由通用 CAS 提交，平台错误不由服务器猜测。

所有依赖快照的结论，包括零效果冲突，都必须验证 guard revision。已知 NotApplied 竞争可重新捕获；Unknown 只核对同一 action，不用后来状态重算旧计划。快速重试有界，阻塞 advisory 转入 WaitRanges，健康且续期的 session 可以继续等待。通用 owner／resource 依赖与有界环检测保留跨 session／跨文件死锁判断，登记、取消、授予和过期清理有序；容量或判定失败明确报告。

### 持久退役意图

Prepared 与 entry 的 Active／Draining／Detached 分开。PrepareRemoval 是已经授权接纳、绑定 reference／EntryID／条件的固定动作；它可以与创建／保留一起提交，并允许相容后续 open 和目录子项。Close／expiry 激活意图并退役引用，目录非空则消耗意图而不激活。Draining 阻止新引用和经旧父引用的目录插入／rename-in，最后引用结束后再检查 IfEmpty 并 detach。

FILE_DELETE_ON_CLOSE 在 SMB 创建时准备、关闭时激活，不在关闭时重新检查 DOS readonly；SetDisposition(TRUE) 此刻检查平台资格并立即 drain，FALSE 只清当前 drain，不取消任何 Prepared。相关时点见 [CreateFileW](https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-createfilew)、[SetDisposition](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-fsa/386d9ec5-e0f6-4853-b175-c05be01419e0) 与 [Close](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-fsa/d142c93a-72bc-4b05-9d96-8e00371c3308)。remote 只执行固定通用转换，不解释这些 flags。

CancelPrepared 使用所属 intent，CancelDrain 校验当前 generation 与本次通用授权，可由另一个获授权引用执行；旧 generation 不撤销新 drain。准备动作随 entry 改名，不能删除旧名字上的新 entry。退役先 fencing、排空已接纳工作，再完成已有固定意图及内容回收，S/X、claims、完整性和 quota 仍在最终边界检查。请求后来被拒绝不表示旧意图不存在，未知清理保留所有权，不保存凭据或新建任意授权委托。

### 动作、恢复与边界资源

会话、action epoch／nonce、历史与原引用保持有限生命。receipt 区分 Pending、Completed、NotApplied、Unknown；Retired 只说明当前对象已终止，不证明给定动作曾执行。NotAdmitted 只证明当前调用未准入。transport 不自动 Query 或重发修改；拥有原计划的 SMB／FUSE／SDK 显式协调，无法确认时保留 EIO／fencing。

原动作核对限定在有效会话和声明的历史窗口；退役、重启或过期后可能为 retired／unknown，不承诺永久结果或自动恢复旧句柄。物理清理责任可以长于可查询动作历史。FileServiceOptions、FileSessionOptions、HTTP admission 与各适配器预算分别限制引用、actions、claims、ranges、等待依赖、Prepared、drain、metadata、事件和完整内容物化；未知效果不能通过遗忘状态制造空闲容量。

持久节点使用通用种类、时间、metadata、revision 与独立 entry 身份。历史迁移 0001–0005 与历史 v1–v3 fixture 保持其原始内容，新通用字段进入 0006；node_high_water 同时预留新的 EntryID，节点与 entry 的身份角色分开。升级先验证真实来源版本，再迁移并验证新格式。提交前可确认的失败回滚；Commit 未知或提交后见证失败沿用 durability fencing，不能声称来源库已恢复。

旧通知无法提供真实祖先图像，升级重置保留历史和 incarnation，同时保留节点 ID 与分配高水位；不为未合并 expanded-v5 建兼容分支。Strong 持久格式保持，File 使用独立 evidence 与 Quiescent。静止状态不降低 MaxLease；数据库级 admission 在所有 volume／Store 的引用、I/O、依赖和持久清理责任排空后发布 Quiescent，使干净重开无需 File quarantine。旧 Active 恢复期限也必须满足，下一次 session 则先持久 Active。未知清理不宣告静止，Strong Close 与保护独立。配置、证据与关闭顺序见[本地持久存储](../../../../docs/design/server/local-disk-object-store.md)。

宿主可通过经过净化的 SessionID／ActionID、阶段、revision 与容量计数关联 request → HTTP → publication，以及等待、续期、expiry 和 finalizer。验证丢失答复时，用原动作核对效果与清理责任；context 或归属任务 ID 贯穿异步交接。日志不输出凭据、opaque payload 或内容，不新增全局 stdout、无归属后台任务或遥测系统。

## 备选方案

**保留 Windows 专用远端栈。** 集中共享与删除规则能够约束其它入口，但公共接口、HTTP 和数据库持续承担 Windows 名字、flags 和状态语义，后端无法只实现共同文件事实。

**只改叫 native／common。** 重命名不会消除 Windows 比较、路径阈值、删除时点或 POSIX 进程规则；没有独立用途的同义接口仍然耦合平台。

**只使用原 FileSession 与 S/X。** 保留身份和强排他不能表达目录一致观察、atomic open disposition、双向共享、范围部分效果与关闭后的持久义务。强锁也不能用来替代普通共享语义。

**所有入口改走 SMB。** 可只保留一个平台解释器，但改变 SDK／HTTP 的部署和依赖，使 Unix 行为受 Windows 协议约束。

**可编程远端事务、比较器或关闭 callback。** 表达灵活，却引入远程执行、状态转换审计和资源上界问题；固定原语足以覆盖身份条件、集合转换与退役清理。

**只在客户端保存保护和 pending 删除。** 另一入口可以绕过，进程退出会丢失义务。保留本地平台计划的同时，共同 claims、范围和固定清理责任必须由权威持有。

## 后果

平台变化主要落在 SMB 或 FUSE，storage／metastore／HTTP 使用共同事实。代价是平台适配必须拥有有界目录投影、完整祖先条件、range planner 和未知结果协调；公共原语也需要独立用途、线性化点与清理预算，不能扩展为任意条件语言。

其它入口可创建 Windows 无法表示或有歧义的名字；受影响的 Windows 观察因此失败。它不会通过遗漏、改名或删除数据迎合平台，也不会把名字观察失败传播成对已保留身份内容的替换。缺少可选 metadata 使用明确缺省，损坏的已有值仍失败。

[测试策略](../../../../docs/testing.md)分别约束公共原语、平台映射、跨入口与历史迁移。需要保留的情形包括条件创建／重置／替换的全效果原子性、祖先竞争与跨页目录变化、opaque 缺席／损坏／外来 key、重复 shared acquisition 的精确解除、范围前缀效果、有界死锁图、Prepared／drain／旧父引用插入、取消／expiry／崩溃及未知提交后的责任。真实 v1–v5 升级、坏库回滚、v6 reopen、ID 高水位、日志 reset 和 Strong／File 域隔离各自验证，包内覆盖率不借其它测试二进制代计。

Linux mode、owner、flock／fcntl、direct I/O 与错误行为继续由真实挂载测试裁定；Windows native 调用、SSPI、映射及可见性另行裁定。实现平台隔离不构成这些执行结果的替代证明。

[原生 CI 34982701023](https://github.com/codetreker/remote-fs/actions/runs/34982701023) 中，Linux 两项检查成功，Windows 重启后认证／不同 SID 拒绝通过；TestNativeWindowsHTTPBridge 的一秒负查找可见性失败。live.bin 已取得有效 V2 StateNONE lease，记录中没有出现远端创建后的新 new.bin CREATE；记录只检查 compound 首成员，后续专门目录阶段未到达，详见[原生验收](../../../../docs/testing.md#windows-11-arm64-原生入口)。

该原生一秒门槛仍未通过。全局 TTL=0 未获批准或采用，主动缓存权利与 break 策略没有随平台隔离引入。R-CON-5 在应用层大 I/O 被拆分时的保证单位仍未决定，既有单次操作及断线错误保证继续适用。
