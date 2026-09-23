# Agent Note: 有界 SMB 文件引用

Status: implemented

## 问题

安全的 SMB session/tree 能连接到一份 volume，却不能为 CREATE 返回真正的打开引用，也不能把 CLOSE 与 tree 退役落实到同一对象。若 FileId 只是路径、NodeID 或临时 response 标识，改名和同名替换会把已打开句柄转向新对象；若创建效果早于本地句柄容量预留，响应丢失或后续清理失败会留下无人拥有的权威引用。

Windows 按大小写无关的名字查找共享目录，而 volume 允许其它入口保存 Windows 无法表示或彼此等价的名字。选中一项后再按原始路径打开，会在祖先改名、目录变化或目标替换时改变 CREATE 的对象。共享模式与创建、截断必须在最终 authority 次序里一起决定；客户端预检不能阻止其它入口越过它。

tree、session 和 connection 关闭会与已经接纳的 CREATE/CLOSE 交错。只取消已登记的 request 不能证明没有晚到的句柄，也不能证明一次失败的关闭已释放权威引用。返回错误但携带引用的打开和返回错误但已释放引用的关闭，要求所有权由权威结果而非错误有无来判定。

## 决定

`packages/smb` 在既有签名 session/tree 上处理有界 CREATE 与 CLOSE。CREATE 通过完整的目录 metadata observation 逐层应用 Windows 本机大小写无关 ordinal 比较，拒绝任一目录中的非法、不可表示或等价名字。解析保留 root、每层 directory revision 与已选择 edge；最终的 `ChildSelection`、SameNode／Absent 条件及既有目标 `smb.windows` metadata 版本或缺席在 `OpenAt` 或 `OpenChildRef` 的同一权威动作中核对。后续观察带上此前的 guards；已知 guard 冲突无部分效果，未知结果只核对或重投原 action。

普通文件数据引用由 `OpenAt` 保留；目录及 metadata-only 引用由 `OpenChildRef` 保留。打开声明的 access 与 share 映射为中立的 `UseClaim{Uses,Deny}`，随打开一起由 authority 与所有入口的 claim 双向比较。句柄另存获授 access，不能把 Use claim 当成 File 方法权限。新建普通文件的 Windows ARCHIVE 和初始属性保存在独立的 `smb.windows` opaque namespace，与创建同次提交；CREATE 响应只投影原动作的 `Attr`、`OpenOutcome`、真实时间、EndOfFile 与已知 AllocationSize，历史未知创建／变更时间为零。SMB 在打开前要求 `AllocationReporting.CheckAllocationReporting` 通过，并对原子打开结果再校验已知、非负且按端点 4096 字节 cluster 几何对齐的分配量；不相容的结果在效果前失败。中立的 `AllocationReporting` 不承诺这一粒度，零也不代表未知。

每棵 tree 有独立的 `MaxHandles` 槽位，默认 256；一次 CREATE 在权威效果前预留槽位和 SMB session 内单调唯一的 16 字节 FileId。在途打开、已返回的引用及清理未确认者都计入 `Status.Handles`。FileId 指一次打开，只在所属 tree 查找；稳定节点身份始终来自 authority NodeID，不能从 FileId、名字或连接推导。`MaxDirectoryBytes` 默认 8 MiB，约束每层完整观察和名字投影的驻留，超额结果整体失败。

显式 CLOSE 在当前请求授权下对原引用调用 `CloseWithResult`。`Released=true` 退役引用和槽位，即使同时带有语义错误；`Released=false` 保留原 tree 的清理责任，句柄停止其它文件工作，后续只重试清理。open 的错误若带非空引用，同样由预留的 tree 槽位持有并清理。response 丢失不重建路径、改用新 action 或把未知解释为未执行。related compound 只从紧前成功 CREATE 继承 FileId；完整响应预算在效果前核对。

tree 退役先关闭文件工作准入，取消 pending request，排空已接纳的文件工作，然后独立尝试所有句柄关闭，最后释放共享 authority ref。未确认释放的句柄使 tree、export 和 session 的 owner 留存，直到逐句柄重试或最后一棵 tree 的共享 FileSession 已确认释放全部引用；不因为一个失败就跳过其它句柄。LOGOFF、断线和 Shutdown 沿这条所有权顺序清理，不把连接终止当作引用已释放。

CREATE 的支持面为普通文件与目录的 OPEN、CREATE、OPEN_IF、OVERWRITE、OVERWRITE_IF，以及适用的 metadata-only 打开。SUPERSEDE、关闭时删除、带效果而未实现的 contexts 和标志在权威效果前失败。[SMB2 CREATE allocation context](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/3f343618-01b0-4aaf-b8a3-b270f9a6d334)允许不支持预分配的服务端忽略有效请求；畸形 context 仍被拒绝。普通可选 durable／persistent 与 lease 请求可以不授予；响应不能声称支持恢复，断线后 FileId 不可复用。READ、WRITE、FLUSH、文件信息命令、目录枚举和删除语义保持明确不支持。

各项延期的成本与保留条件如下；这些命令不借 CREATE 成功暗示已经授予。

| 延后项 | 延后成本 | 当前形状约束 |
|---|---|---|
| 数据读写、FLUSH 与文件信息命令 | 增加逐次 I/O、授权、响应预算与真实属性操作 | FileId 持有原对象和获授 access，后续调用不能按旧路径重开 |
| 目录枚举 | 增加目录句柄的有界完整读取、信息类编码与 continuation | 目录 CREATE 保留 NodeReference；名字观察不被当作可重用的陈旧枚举缓存 |
| 关闭时删除 | 增加接受时授权、持久义务和失败状态呈现 | 不接收 DELETE_ON_CLOSE；关闭结果与引用释放事实分开，不能丢失已接受责任 |
| SUPERSEDE、rename、disposition、通知和范围锁 | 增加独立的名字修改、事件恢复与范围顺序语义 | CREATE 不把路径、FileId、NodeID 合并，也不在 authority 存入 SMB 协议状态 |
| WNet 与真实 Windows 系统客户端验收 | 增加发布／移除和平台缓存、身份及故障矩阵 | 协议测试只证明端点适配，不声明系统网络驱动器已可用 |

## 备选方案

**只添加 CREATE/CLOSE wire codec，仍返回不支持。** 这能验证协议形状，却无法建立 Windows 打开引用、共享顺序或 tree 清理所有权；文件数据命令仍没有可信的 FileId 所属者，因此不选。

**把完整文件数据、目录枚举与关闭时删除一并加入。** 这些能力各有自己的数据预算、枚举观察和持久删除责任，会让 CREATE/CLOSE 的所有权问题与多个独立行为同时进入一个审查面。当前的 FileId 和清理形状保留后续能力所需的同一对象引用，因此这些命令可以独立接入。

**用一次大小写无关的客户端查找，随后按路径打开。** 这省去 guards 的传递，却允许查找到打开之间的祖先或目标替换改变最终对象；失败也无法证明没有创建或截断错误对象，因此不选。

**以 NodeID 直接作为 FileId，或让每棵 tree 独立循环分配 FileId。** NodeID 是对象身份，一对象可有多个打开；tree 局部序号又会在同一 SMB session 的另一 tree 中复用。两者都不能准确表示一次打开和它独立的共享声明、关闭责任，因此不选。

## 后果

每次成功 CREATE 都拥有一份由 tree 管理的权威引用；按名竞争不会把它转到替代对象，跨入口共享限制在最终事务排序。饱和或解析失败发生在创建效果前；清理不确定时仍能从 Status 看见占用并重试。客户端的 Windows 名字与属性解释没有进入 storage/HTTP schema。

CREATE/CLOSE 的通过只证明协议和 authority 适配，不能证明系统 Windows redirector 的可浏览、读写、缓存可见性或 WNet 映射。文件数据与信息命令、目录枚举、关闭时删除和其它 Windows mutation／并发命令仍各自需要实现及原生验收。一次完整目录观察可能比单槽位查找昂贵，受每层 byte 上限、路径深度和请求 timeout 共同限制；超出预算明确失败。

本决定接续[安全且有界的本机 SMB 端点](2026-09-21-secure-bounded-smb-endpoint.md)、[有界权威名字观察](2026-09-20-bounded-authoritative-name-observations.md)、[可恢复的关闭与删除义务归属](2026-09-23-recoverable-close-ownership.md)、[虚拟分配账本](2026-09-23-virtual-allocation-ledger.md)与[Windows 系统网络驱动器支持](../../proposed/feature/2026-09-16-windows-network-drive-support.md)。协议和引用契约见[本机 SMB 端点](../../../../docs/design/client/smb-endpoint.md)。
