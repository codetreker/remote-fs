# Agent Note: 当前 authority 的独立 Linux 测试环境

Status: implemented

## 问题

Windows 系统客户端的验收需要实际接入当前 HTTP 与持久 authority。现有 localstore、SQLite 和 nativelease 依赖 Linux；固定 SMB 原型的内存 authority 不能证明当前接口、持久身份、配额或重启行为。把这些差别留在验收替身中，会使原生客户端通过也无法说明交付组合正确。

测试环境还必须拥有全部进程、私有磁盘和凭据。启动日志或监听端口存在不足以证明连到了本次 volume，异常退出后的后台进程也不能成为后续样本的一部分。

## 决定

[独立工作流](../../../../.github/workflows/native-current-authority.yml)把当前源码的 Linux authority 和 Windows HTTP 客户端放在同一项可核对的测试中。Ubuntu 构建固定 kernel、最小 initramfs、当前 server/helper 与私有 ext4 基底；Windows ARM64 作业使用固定的 native ARM64 QEMU bundle，经 TCG 运行同一 Linux AMD64 guest。生产 backend 不移植、不替换；可选 cold 阶段组合当前 SMB/HTTP 代码与同一 authority，仍属于测试工具。

[工具说明](../../../../.github/scripts/native-current-authority/README.md)拥有依赖 pin、装配命令和进程规则。Weil 11.1.0 ARM64 bundle 按固定大小/SHA-512 和完整提取树核对，QEMU/controller/probe 必须具有 ARM64 PE；qemu-system-x86_64 的名字表示 guest target，不是宿主架构。已检查 85 项 bundled PE 的 ARM64 import closure，publisher 的未测试标签仍保留，字节 hash 不被当作签名或可复现构建。bundle 只提取，installer 不执行；kernel package 不执行安装脚本。结构化源码发现只解码 stdout，stderr 的下载/诊断保持独立输出，命令失败仍传播；合法诊断不改变 JSON 输入。artifact 绑定输入 hash、源码与工具链；Windows 作业核对同一源码的 artifact，CI 的脏来源、缺失 verdict 或来源变化直接失败。依赖、缓存、临时磁盘与证据都留在 checkout 的 `.tmp`。

每次运行复制私有可写 ext4 磁盘。guest 中 authority 以 UID/GID 1000 访问真实 localstore、SQLite、nativelease 与 local objects；宿主只开放一个 loopback HTTP 转发，无共享宿主目录。PID 1 在任何 mount/修改前核对执行身份、初始 root、配置和 boot token；nonce、顺序号、有界控制输出将实际 child 生命周期绑定到本次运行。

Windows 目录采用受保护的 owner/SYSTEM DACL，文件内容写入前核对拥有权与权限；reparse 或额外 ACE 不被默认接受。路径 guard 从 provider 入口沿实际 DirectoryInfo/FileInfo 祖先逐级检查，不把只附在入口对象上的 PowerShell 扩展属性当作 Parent 的成员；缺席、其它 provider 和任意 reparse 祖先均拒绝。QEMU、探针和提取进程先挂起启动、加入未命名 kill-on-close Job Object，再恢复。根进程退出与整个 Job 清空是不同事实：退出后最多用既有五秒预算等待实际 ActiveProcesses=0；根尚未退出、查询失败或等待到期才进入强制终止，并在最多另五秒内确认，整段清理不超过十秒。Job accounting 核对结构 ABI 与 ReturnLength，未知数据不能当作零。唯一 waiter 完成实际 wait/exit-code 查询后关闭进程 handle，Finish 不抢先关闭它。正常完成还要求 authority 实际退出、guest sync/unmount、QEMU/Job 结束和流排空，再删除私有副本并核对基底未变。强制终止即使清空也保留失败，不能借清扫取得成功。

Windows serial 控制使用 controller 在启动前独占绑定的 `tcp4/127.0.0.1:0` listener，QEMU 作为 client 连接且不重连。只接纳首个连接并关闭 listener；在读取任何控制字节或发送命令前，按反向四元组核对唯一 ESTABLISHED TCP owner row，要求属于原来保留且仍活跃的 QEMU process handle。查询前后都检查存活与期限，handle 关闭由同一 mutex 与唯一 waiter 串行，不能按 PID 重开进程或仅凭 loopback/nonce 信任对端。TCP table 固定 1 MiB、只查询一次，大小/计数/地址/端口或归属不明均失败；该同步 Windows API 不可取消，期限检查不能被描述为限制其内核执行时长。

绑定、启动、accept、peer proof 和 boot-ready 共用原 120 秒 deadline；JSON+LF 命令至多 1 KiB，沿原 nonce/sequence/source/phase 与 30 秒 deadline 写入 socket，不靠放慢或重发恢复。QEMU stdout/stderr 立即各自排空为诊断，serial 独立承载控制和 authority frame，每个流保留 16 MiB 上限。正常退出在同一 30 秒 shutdown deadline 内取得精确最终确认、自然 QEMU exit 0 与全部 reader 收齐；serial 可以是 EOF 或下述严格限定的 reset；失败路径从入口固定十秒总期限，关闭 socket、完成 Job 和 join readers 都使用剩余时间，原自然/强制各五秒上限同时受它约束。FinishBefore 只向既有拥有者传递绝对期限，不增加清理管理器；错误、未排空或强制结束保持失败。独立 Linux driver 继续使用其 POSIX stdio，guest/backend/image 和 HTTP 转发不变。

terminal serial reset 的合格条件独立于普通读错：必须已有带正确 run/source/nonce、phase/op/sequence 的 shutdown/4 成功 ACK 和 exit_code=0，原始串行 read 结果有独立的完整 LF 记录边界，并且 cause 是唯一的 10054 包裹链；join 的其它错误不能被掩盖。仅在等待这个最终 ACK 时，允许从已解析队列取回被 reset 先选中的 ACK；没有 queued ACK 不能以 reset、EOF、进程退出或 kernel 文本补造。随后在原剩余三十秒内等待自然零退出、全部 reader 完成且无其它错误，才将那个确切 serial 结果标为 qualified-reset 并保留 raw error，不改名为 EOF。缺失事实、尾部半帧、超时、非零退出、未知 Job 或强制清理仍失败，最后的磁盘/基底核对也不省略。

HTTP readiness 使用真实 lease/FileSession，验证引用、原子打开、字节、metadata 条件、改名/移除后的引用身份和逻辑配额；保留 sentinel，停止 authority，再以同一磁盘普通重开，核对 sentinel 的身份、字节和 metadata 后删除它。此工具保留 native_acceptance 未运行这个更广的门禁；可选 cold 结果单独记录，require-native 不能把它升级为完整 SMB/一秒验收。

当前源码的 cold 阶段由显式 -CurrentSMBCold 选择，工作流启用该选择，独立调用默认保持 HTTP-only。顺序是 create/1 留下并关闭 seed 引用，cold/2 以该不可变 seed 的 NodeID/epoch/nonce 通过当前 FileStorage 新建 SMB session，再执行 authority stop/2、start/3、reopen/2 与 shutdown/4。该 host 使用当前 SSPI/精确 SID 授权和独立 loopback SMB listener，不复用旧原型 dispatcher、Map 包或内存树。

[映射实现](../../../../.github/scripts/native-current-authority/smb_mapping_windows.go.txt)由现有 source-bound probe 自身的一次性 native helper 执行，复用原 Windows Job/进程/stdio 拥有者，不再通过 PowerShell 解释器做 Snapshot/Create/Remove。单份至多 4 KiB 的 JSON+LF 请求与唯一最终回复绑定 action、run/source/nonce/volume/sequence、state 文件身份、SID/AuthenticationId 及 drive/UNC/port；回复还核对 request digest 和实际 owner。helper 在普通 HTTP probe 参数处理前分流，非 Windows 明确拒绝。缺失、畸形、错配或超限 mutation 回复保持 unknown，exit 0 不代替完成事实。

CurrentSMBCold 的支持环境限定为新鲜隔离的 GitHub-hosted Windows ARM64 job，fixture 是该 logon 唯一修改映射的参与者。driver 核对 Actions/github-hosted 声明、run/attempt 与规范 artifact 路径；工作流在 cold step 传 runner context，手工/共享 logon、自托管调用在准入前拒绝。这是部署前提，不是对任意管理员/外部 actor 的认证；保护范围不跨任意 checkout root。canonical executable 推导固定 fixture root，state 必须位于该 root 下本次 run 的私有 evidence 路径。

Create 只解析 system MPR 的 WNetAddConnection4W，使用 DISK resource、选定盘符和 nonce loopback UNC、NULL auth/length0、CONNECT_REQUIRE_INTEGRITY；唯一 SDK transport option 为 24 字节 Wsk/TCP-port 数据，reserved、padding、QUIC/RDMA port 和 certificate-skip 为零。记录请求参数与返回 DWORD，不把配置当作实际网络身份、签名或端口证明。WNet4/provider 对该 options buffer 的接受性仍待原生验证；export 缺失、拒绝或其它失败不得换 API、退回 445、改策略或重试。现有 SSPI/当前 SID、owned-listener 与真实 signed traffic 验收继续必需。

Snapshot 完整枚举 WNet connected DISK，并核对 26 份 DOS/logical-drive 与 local-name lookup。NOT_CONNECTED 与 CONNECTION_UNAVAIL 分开，后者是占用/不可用；只有各观测均无占用且 lookup 明确未连接才可选盘符。固定 1 MiB 枚举 buffer、128 总 entry、129 次调用和 1025 UTF-16 lookup buffer 拒绝 MORE_DATA、坏指针/计数、无进展成功与不一致快照；只在 NO_ERROR 解析并复制有界数据，NO_MORE_ITEMS 才是终止，关闭枚举 handle 的错误保留。WNet 创建成功、connected membership、精确 lookup/DOS 绑定替代旧私有 CIM Status==OK 判据，不声称 health 等价或已可读文件。

固定 root 下以 SID/AuthenticationId 派生的受保护目录持有独占、同步的 immutable claim，与原 owner file lock 和 per-run ledger 配合。已有目录即消耗本 root/logon 的尝试，即使上次成功或 claim 半写也不接管/重置/删除；新 run 子目录不能绕过，只有精确同次 cleanup 可重入。claim 先于初次 inventory，helper 只接受匹配 claim 与 pending ledger；固定路径/祖先/ACL 和实际 primary-token/Job 均须核对。

create/remove 分别在唯一 dispatch 前持久化 pending。完整绑定回复可证明 not-invoked 或特定 API return，并在 owner lock 内保存 terminal knowledge；replacement 可能已发布 terminal 字节却返回错误，读回它不等于原调用方收到持久化成功确认。任何 mutable-ledger write/sync/close/replace 错误（包括 observational Save）都不可逆地撤销 live removal authority。unknown create/remove 保持隔离，后续空快照或先看到行再删除都不能清除它；不再发 mapping mutation，只收集 readonly 证据并收尾本地 server/process/disk，随后丢弃隔离 runner。错误/pending 返回不证明零效果，已知未调用也不授权 replay；缺失/损坏 ledger 没有积极 no-dispatch 证明时保持 unknown/error。

只有原始仍存活的 attempt 可持有不可序列化的一次 normal-removal 权限；须由 validated create API NO_ERROR、调用方已收到 create-result Save 成功确认、helper Job 静止与 streams 收齐共同授予。terminal ledger 或完整 success frame 单独不授予。纯 snapshot 读取失败可保留这份 live 权限，但再次修改前仍必须重新取得完整匹配 ledger、当前 owner/baseline/精确资源绑定。

权限在尝试 remove-pending 保存或 dispatch 前消耗，不能 retry、持久化或在退出/持久化错误后重建；之后才可执行 WNetCancelConnection2W(local drive,flags0,forceFALSE)。成功仍要求 API success、完整后置缺席和 baseline 不变。cleanup-only 只用于失败路径恢复，已核实的正常 cold 成功直接完成，不再调用它。重入对映射一律只读，即使已看见成功 terminal 字节或资源缺席，也不能补证原调用方 Save ACK、已结算 removal、恢复修改权限或重置 admission；未证明的恢复状态继续报错/隔离，本地 process/disk 清理事实另报。公开删除接口没有 generation compare，仍依赖 fresh/sole-actor 前提，不新增 acknowledgement journal、marker 或 manager。

每次 Snapshot/Create/Remove 的原四秒涵盖启动、全部 API/枚举页和完整回复处理，不在内部重置。probe 自身二十四秒、controller 三十秒 watchdog、恢复另有三十秒总期限与既有 Job 清理预算保持。native 阻塞发生在已拥有的 helper 内，确认终止只证明本地 process 清理，不证明 provider 取消/回滚。完整 API-success frame 也不覆盖独立 deadline、exit、stream 或 cleanup 错误；成功后的观测失败与 unknown create 分开处理。

[原生 cold 观察](../../../../.github/scripts/native-current-authority/smb_probe_windows.go.txt)只对一个已知文件首次普通打开，在同一 HANDLE 上核对 Basic/Standard、字节/EOF 和两次 FileIdInfo。原生 volume/128-bit ID 是 opaque tuple，只要求同 HANDLE 稳定，不要求非零或数值等同 NodeID。NodeID、SMB FileID、实际 wire 字段分别记录；被动观察不补发缺失查询，严格核对 session/tree/related compound 归属并拒绝未支持的 async。这个单对象证明不覆盖唯一性、跨改名/替换、原生写入、目录、Explorer、断线或暖缓存。

资源继续在现有 pool 中计量：cold 的 MaxOpens/MaxRequests 均为 16，8 MiB result pool 容纳十六份最坏 Standard-state 固定预留共 4,867,072 字节；64 KiB I/O、128 KiB frame 和 1 MiB HTTP body 分开约束。这样合法返回的 metadata 不被 frame 预算误拒绝，容量不足仍在效果前失败。cold 使用既有二十四秒 probe context 与三十秒 controller watchdog，额外映射恢复不增加成功操作的等待或改变一秒可见性要求。

## 备选方案

**继续用 PowerShell/CIM 做映射。** 已绑定的 cold 运行在已观察阶段或脚本 entry 前超时，后置成功又受负载和预热影响，尚未取得完整 cold 验收。一次性 native helper 去除这项解释器依赖，但不把原失败归因于 parser、binding 或 scope，也不假定 native API 一定及时返回。旧 tagged 六单元诊断与自动执行步骤退役，既有原始结果保持。

**为 WNet4 拒绝增加另一 API、端口或策略 fallback。** 这会改变一次受测调用的条件，未知 mutation 也不能安全重放。该配对必须自行取得原生证据，失败继续是失败。NetUse 的状态查询是另一真实选项，有不同 scope/分配成本；本实现采用有界 WNet connected/lookup/DOS 观测，不将未选方案说成无效，也不伪造 CIM health。

**允许共享 logon 或从缺席恢复尝试。** WNet 删除没有 generation compare，库存不含某行也不能证明先前 mutation 未发生。固定 root/logon 的 immutable claim 和 sticky unknown 保留失败，而 fresh runner 的独占参与者/最终丢弃是明确部署成本；不声称全局协调外部 actor。

**复用固定原型的内存 authority。** 它保留可复现的缓存诊断价值，但不运行当前持久 backend，无法代替当前引用、配额与普通重启的验证。[原型诊断决定](2026-09-16-native-smb-cache-diagnostic.md)继续拥有那组实验，二者不互相替代。

**为测试把 localstore/nativelease 移植到 Windows。** 这会同时改变生产持久性和平台依赖，验收对象本身随准备工作扩大。Linux guest 保持原 authority 的代码和运行环境。

**使用 WSL 或跨 runner tunnel。** 目标 ARM64 runner 没有可用的既有 WSL/隧道环境；本次 run 拥有的 guest 与 loopback 转发能把进程、来源及关闭证据约束在同一作业中。

**通过 Windows x64 翻译运行 QEMU。** 该 bundle 的版本/能力查询成功，但 guest 启动在 authority 之前以 0xC00000FF 退出，具体 unwind table/module 未确定。native ARM64 bundle 保留同一 QEMU 版本、guest target 和完整提取核对，移除宿主二进制翻译依赖；[native ARM64 的实测](https://github.com/codetreker/remote-fs/actions/runs/35423839231)已取得 guest boot-ready，但不能据此反推旧异常的根因，也不代表整个 authority 生命周期通过。

**继续用 Windows stdio 传控制命令。** 被检查的 callback 在 frontend 接纳能力不足时可能确认没有完整交付的输入，提取函数与实际 ARM64 binary 分析核对了该分支。[stdio 超时运行](https://github.com/codetreker/remote-fs/actions/runs/35423839231)没有逐字节接收记录，不能断言就是某次零容量造成；为保持有背压、可认证且可取消的控制通道，socket 不依赖这一 stdio 行为，原诊断输出仍被完整拥有。

**改用 Windows named pipe。** 已检查的 QEMU 后端只创建 server，未提供所需的明确 DACL、first-instance 或远端 client 拒绝配置，也不能连接 controller 先建的私有 pipe。创建后再改 ACL 留有窗口，TokenDefaultDacl 的覆盖及额外权限未获证明；不以这些假设替代持有进程 handle 的 socket peer 核对。

**MSYS2 QEMU、自编 kernel 或完整 cloud image。** MSYS2 需要固定完整 DLL/package 与签名闭包，自编 kernel 增加 compiler/config 维护，完整镜像增加无关启动和用户空间。固定 maintainer bundle、发行版 kernel 与小 initramfs 保留所需 Linux 行为，代价是显式验证提取树、固件和外部输入。

## 后果

本地 Linux TCG 已完成一次真实 guest/readiness/普通重启/关闭，私有磁盘删除、基底和输入保持不变。该本地 artifact 明确记录脏源码来源，不能充当随后 CI commit 的收据。[terminal 修正后的原生运行 35430629381](https://github.com/codetreker/remote-fs/actions/runs/35430629381)绑定源码 `31817a904fa42896b47ce5eb64e40cbe8be73e90`，核对 221 项源码输入、16 项 image 产物和 QEMU package；controller 46 根/191 verdict、probe 8 根/63 verdict 与十二项路径检查通过。实际 owned socket、真实 HTTP 创建和普通重开后同一 NodeID=3/新 epoch 已成立；精确 shutdown/4 ACK 后取得自然 QEMU exit 0、完整记录边界的 qualified-reset、非 forced 的空 Job，以及私有磁盘删除/基底不变，HTTP fixture 生命周期完整通过。原始 10054 仍在 serial_error 中，不伪装 EOF。[先前 b2 的强制结束](https://github.com/codetreker/remote-fs/actions/runs/35427363909)仍保持失败。这个通过只属于上述 HTTP fixture。[首次 current-SMB cold 运行 35434023125](https://github.com/codetreker/remote-fs/actions/runs/35434023125)在初始 Get-SmbMapping inventory 的四秒子命令期限处失败，没有创建 intent、本次 SMB 连接/流量或原生文件调用记录，实际 PowerShell 阶段未知。cleanup-only 确认没有本次映射，server/Serve 收齐，但 guest 强制清理仍使运行失败；不能把清理归零当作 cold 或缓存成功。

隐藏 Go 模板通过显式测试入口运行，不依赖普通模块发现。已有 Linux 模板普通验证为 36 根、176 个通过 verdict；guest/probe/controller 的普通与 race 各有 87/63/26 个通过 verdict，Python 装配/拥有权/checker 用例共 26 根。Job rundown 的三个 AST 提取测试根普通/race 各 11 个通过 verdict，恢复立即强制清理决策的对照在正常退出后计数收齐的断言失败；vet 和 Windows AMD64/ARM64 构建通过。这些局部检查与上述 native helper 收据分开，不能替代 guest 生命周期。各自验证错误、输出界限与清理，不把 mock、交叉构建或 Linux VM 成功称为 Windows 执行。[路径回归](../../../../.github/scripts/native-current-authority/run_windows_test.ps1)直接抽取实际 guard AST，在 VM 前核对 provider/raw-parent 转换及拒绝路径；本地九项通过，旧 guard 的因果对照在父链缺少 PSIsContainer 处失败。Windows 的十二项运行包含三个 junction 场景；这个已观测的路径检查不证明后续 bootstrap 可用。native ARM64 包选择的 controller 普通/race 各 14 根/35 verdict、宿主架构反转对照、相关构建/vet、七项 Python 与九项本地 PowerShell 检查通过；它们与后续 native boot 收据分别计证据。具体执行入口见[测试策略](../../../../docs/testing.md#当前-authority-的独立运行环境)。

current-SMB cold 的本地最终 helper 普通/race 各 83 根/432 verdict，可移植 mapping 控制 13 根/43 verdict；真实核心 pool 与错误 SID 的负向对照、Python/PowerShell source 检查及 Windows 构建/vet 均有独立收据。完整本地 artifact 的 265 项输入/16 项产物与冻结 README、源码一致，kernel/init/authority 字节保持原 F 组合；其 dirty=true 来源不当作未来 CI commit。这些验证不补足仍缺的映射、单对象原生观察或暖缓存证据。

当前 native mapping 的可注入普通/race 控制各 25 根/141 verdict、五项因果对照及 ARM64 程序/完整单元构建、vet/格式检查已通过；Windows 实际磁盘 publish-then-EIO 和 private claim/lock 用例只编译，未本地执行。支持层 Linux probe 各 35 根/215 verdict、本地 Unix guard 37 项与 checker 7 项分别计证据，不冒充 Windows WNet、DACL/锁或 provider-options 接受性。真实 mapping 与 sentinel 访问只属于原 cold 阶段；这些检查不能预热或替代它。

[最后一个解释器版本的首次运行 35459298859](https://github.com/codetreker/remote-fs/actions/runs/35459298859)绑定 `58d1663c19d675817d25154be8e35e6a35726c03`，原 inventory 4010 ms 失败且 stdout/stderr 均为零，没有 entry；实际 system ARM64 executable、resume1 与 219 字节写入已记录，但不能据此断言已到 import/object/native-file 阶段。后置对象 inventory 3793/947 ms 通过，discovery 4020 ms 失败、import/parse 480 ms 通过；负载/顺序变化不解释原 cold。当前 WNet4/provider-options 和完整 cold 仍待新源码原生执行，HTTP 生命周期与历史 helper unit 不能替代它。

以下映射结果和局部验证属于各自已记录的 PowerShell 实现；其[最后源码](https://github.com/codetreker/remote-fs/blob/58d1663c19d675817d25154be8e35e6a35726c03/.github/scripts/native-current-authority/smb_mapping_windows.go.txt)及[已退役诊断](https://github.com/codetreker/remote-fs/blob/58d1663c19d675817d25154be8e35e6a35726c03/.github/scripts/native-current-authority/mapping_startup_diagnostic_windows_test.go.txt)保留历史条件，不是当前操作说明。

[命令控制运行 35454012237](https://github.com/codetreker/remote-fs/actions/runs/35454012237)绑定 `7bef4859bc1cbde96bc2b97cf79d018f94fcf228`，原 inventory 仍在 4009 ms 失败，exact-name Core Get-Command 在 4018 ms、resolution 返回前超时；显式 System32 Utility import 与原 JSON pipeline 控制的执行器耗时 1437 ms，另有 19.0347 ms 的 manifest 准备，两者共享同一四秒 context，清理非 forced。它支持显式导入路径，但顺序预热、控制上下文与负载不同，不能据此断定原超时的唯一原因，更不修改原 cold 失败。[显式导入运行 35456649233](https://github.com/codetreker/remote-fs/actions/runs/35456649233)绑定 `00e2f4291c8033a7b2dc0c0773731d5a0b920523`，原 inventory 已到 after-utility/before-json，随后在 4012 ms 失败，没有 after-json 或 catch-entry；4.7423 ms 准备在同一四秒内。后置相同 inventory 的执行器为 2665/867 ms 通过，discovery 在 4020 ms 失败、import/parse 控制在 459 ms 通过。原 VM 负载、较晚 shutdown 与顺序预热不同，不能断定 try/assignment/PassThru 作用域缺陷、实际同名遮蔽或唯一原因。当时待执行的对象绑定路径后来仍未取得 cold 成功；后置通过不替代原 mapping/native file 和完整清理验收。

[原生脚本观察 35449538972](https://github.com/codetreker/remote-fs/actions/runs/35449538972)绑定 `a5ad2d9dfc8f6a9fbcde84fa30e7ee0fc1d56368`，原 cold 与 inventory 单元实际完成输入/环境观察：child ReadLine 为 218 个 UTF-16 单元、218 个 UTF-8 字节且 no-LF hash 与父端匹配，effective module path 的完整 93 ASCII 字节仅含 AllUsers/System32 Windows PowerShell 根，WinPSModulePath 为 null、cache 为 NUL。完整未截断的 761 字节输出止于 before-json，没有 catch/failure/after-json marker。原 inventory 4010 ms 失败，后置 entry 234/178 ms 通过、inventory 4020/4019 ms 失败；32165 字节 report 已保存且清理完成，仍没有 cold/nativefile/cache 通过。未解决的边界包括命令发现/autoload、binding、执行和赋值，不能仅据缺 marker 判定 parser 或 catch 原因；原 cold 失败保持；第五/六控制的后续结果见下述已绑定运行。

[捕获后的原生运行 35437123318](https://github.com/codetreker/remote-fs/actions/runs/35437123318)绑定 `5beb09c2fa5b514adaffa22a184ee7bf9f99bf97`，初始 inventory 在 4009 ms 超时；父进程写入 219/219 字节，三个 worker 收齐后 stdout/stderr 均为零、没有观察到 entry marker，清理前 child waiter 尚未报告 Done。没有本次映射/SMB/原生文件记录，恢复和磁盘清理完成。父端写完不证明子进程已消费输入，缺 marker 也不定位启动或模块阶段。[带启动事实的运行 35439665455](https://github.com/codetreker/remote-fs/actions/runs/35439665455)在 4010 ms 超时，219 字节已写入、实际 resume count=1，109 字节 stderr 记录 entry、before-readline、after-readline、before-json，没有观察到 after-json。输入 JSON 与 script hash 核对一致，但不能据最后 marker 认定 JSON 转换、命令发现或错误序列化的具体原因；原零输出准入按条件拒绝，所以四单元未执行。没有本次 mapping/SMB/原生文件效果。Linux PowerShell 7 的 stdin-open/EOF 均完成只约束那个环境；Windows PowerShell 5.1 原因仍未确定。规范 key 修复对应实际 Windows key 与 artifact key 不同的缺陷，原 timeout 本身未修复。[实际诊断运行 35442800512](https://github.com/codetreker/remote-fs/actions/runs/35442800512)绑定 `3eed8ec9eceb8bea5743e09d1b3c1fe7c26c093a`，原 cold inventory 仍在 4009 ms、before-json 后失败；后置四单元各执行一次且通过，entry/open、entry/EOF、inventory/open、inventory/EOF 分别为 247/191/3419/913 ms，均自然 exit 0、三个 worker 收齐、非 forced 清理。所观察的继承模块路径含排在 Windows PowerShell 根之前的 PowerShell 7 根，WinPSModulePath 缺席，cache 为 NUL。诊断根随后在私有 writer 的父目录 protected-DACL 校验失败，汇总文件为零字节；没有已保存的最终 native aggregate 或执行后 PE/hash 值。后置成功仍受 VM 清理、负载与顺序预热影响，不解释原 cold 原因，更不成为映射/cold/cache 通过。[私有目录和环境配置运行 35446688647](https://github.com/codetreker/remote-fs/actions/runs/35446688647)绑定 `26ba565b43b9c9569a7aad5e303a9290b7a9e61d`，真实 private writer 与 Go-child 环境用例通过，27917 字节 nested report 已保存，原 DACL 缺陷已关闭；原 inventory 仍在 4010 ms、before-json 后失败，后置 entry 两项为 248/186 ms 通过，inventory 两项均在 4019 ms 失败。配置的 System32 Modules/NUL 输入和最后单元的 EOF 完成都没有消除该边界，父继承与 child 输入证据分开保留。原 SMB 计数归零、恢复完成、磁盘删除且基底不变，但 QEMU 强制结束仍使整次运行失败。该次运行未记录 child 实际 ReadLine、effective runtime path 和 catch 分支，不能推断 JSON 或唯一原因；原四秒期限、flags、普通 stdin-open 和 mapping 行为保持。

该历史对象绑定版本的 mapping 普通/race 各 39 根/159 verdict、tagged 准备/诊断各三根通过；二十项实际本地 PowerShell 7.5.3 AST 场景验证真实 module/export 对象、同名 sentinel 下的对象调用、旧名称目标的因果失败，以及 guards/stdout/错误拥有权。wrong-kind CmdletInfo 只由源码变异覆盖，不虚构 runtime 实例。2040 字节/33 个 CRLF 是有界 formatter 投影，三项 ARM64 构建/vet/格式检查已核对。未变的控制按原 source/graph 复用，完整范围见[测试策略](../../../../docs/testing.md#当前源码-smb-的单对象-cold-检查)。局部绑定证明不解释真实 CI 是否有同名遮蔽，不替代当前 native helper 的 mapping/nativefile/完整清理结果；既有失败不追改。

维护成本包括 kernel/QEMU 固定输入、镜像生成、两作业 artifact 传递和宿主清理工具。boot 上限 120 秒，命令/probe 各 30 秒，输出与磁盘都有界；超限留下明确失败。磁盘使用正常 guest flush 的 writeback，普通重开证明已观察的同步持久路径，不宣称宿主断电或硬件缓存可靠性。完整平台接入仍由[Windows 提案](../../proposed/architecture/2026-09-16-platform-client-capabilities.md)承接。
