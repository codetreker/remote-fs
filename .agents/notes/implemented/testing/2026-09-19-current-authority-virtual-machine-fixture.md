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

[映射拥有者](../../../../.github/scripts/native-current-authority/smb_mapping_windows.go.txt)在 New-SmbMapping 前落下私有 intent，核对当前 logon/SID、空闲盘符、唯一 share、TCP port、完整 SMB/DOS-device 基线；要求完整性，不保存凭据、不建立 persistent/global mapping。它不接管或删除既有映射。正常收尾关闭原生 HANDLE、精确映射、server/Serve/export 与 HTTP 空闲连接，并验证远端和本地清理；child 未确认停止时禁止恢复映射。cleanup/2 只接受同一个不可变 seed 和已记录绑定，另有总计三十秒（含原 Job 收尾）的恢复预算，恢复成功也保留首次 cold 失败。映射命令在 Finish 和三个 I/O worker 收齐后才添加失败诊断：实际 action/PID、清理前是否已退出、elapsed、stdin 写入进度及两个至多 2 KiB 的 base64 输出前缀，附总字节数和截断标记，整体小于 16 KiB。io.Copy 必须经过同一 1 MiB 限制的 Write，不能借 bytes.Buffer 的 ReaderFrom 绕过限额与计数。固定 stderr 阶段标记不含请求或凭据，stdout 的成功 JSON 与原错误链保持；未采到某阶段不等于它没有开始。

[原生 cold 观察](../../../../.github/scripts/native-current-authority/smb_probe_windows.go.txt)只对一个已知文件首次普通打开，在同一 HANDLE 上核对 Basic/Standard、字节/EOF 和两次 FileIdInfo。原生 volume/128-bit ID 是 opaque tuple，只要求同 HANDLE 稳定，不要求非零或数值等同 NodeID。NodeID、SMB FileID、实际 wire 字段分别记录；被动观察不补发缺失查询，严格核对 session/tree/related compound 归属并拒绝未支持的 async。这个单对象证明不覆盖唯一性、跨改名/替换、原生写入、目录、Explorer、断线或暖缓存。

资源继续在现有 pool 中计量：cold 的 MaxOpens/MaxRequests 均为 16，8 MiB result pool 容纳十六份最坏 Standard-state 固定预留共 4,867,072 字节；64 KiB I/O、128 KiB frame 和 1 MiB HTTP body 分开约束。这样合法返回的 metadata 不被 frame 预算误拒绝，容量不足仍在效果前失败。cold 使用既有三十秒 probe watchdog，额外映射恢复不增加成功操作的等待或改变一秒可见性要求。

[启动诊断](../../../../.github/scripts/native-current-authority/mapping_startup_diagnostic_windows_test.go.txt)只在真实 cold 首次失败之后运行，先核对同 source/run/nonce 的初始 inventory command timeout、无 mapping/SMB/原生文件效果及清理已完成、worker 全部收齐；首行必须确为 context deadline exceeded，不能在子输出中搜索该词代替原因。带专用 tag 的测试不进入普通单元清单或生产程序；四个顺序单元各运行一次：最小立即 entry/result 脚本的 stdin-open/EOF，再运行原只读 inventory 脚本的 stdin-open/EOF。每个单元保留四秒命令和既有拥有者清理期限，完整写入一次后才可由同一 stdin 拥有者 close-once；short write、关闭错误和未知 Job 静止状态继续失败。后者禁止再启动下一单元，原 cold 失败始终保留。

准入不依赖输出为空：stdout/stderr 的非负 byte count、规范 base64、恰为 min(count,2048) 的前缀和准确截断标记必须一致，合法的空、部分或带阶段内容均保留。已知顺序 marker 可归类，未知尾部和半行不推断阶段；它们不改变无副作用只读诊断的资格。存在的原输入/script 事实必须匹配，缺席不补造。源码 manifest 统一生成严格 POSIX-relative key，并核对原 hash；Windows 分隔符不能作为另一套兼容 key。

mapping PowerShell 使用既有进程拥有者的显式 Unicode child environment：私有继承副本删除全部大小写等价的 PSModulePath/PSModuleAnalysisCachePath，再分别写入已验证 System32 WindowsPowerShell/v1.0/Modules 与 NUL，其余项（包括 drive-current-directory）保持。模块根取自同一系统 PowerShell home，目录不存在或有 reparse 祖先时在创建 child 前失败。原进程环境不修改，普通 Start 继续继承，QEMU 和其它 child 不变；原脚本、flags、stdin 与四秒期限保持。这纠正传给 Windows PowerShell 的兼容性输入，但 Windows PowerShell 可重新构造运行期搜索路径，因此不承诺 system-only，也不证明实际模块选择或原超时的唯一原因。

诊断在显式 delegated Start 分别记录父进程继承值与配置后的 child 输入，仅观察 PSModulePath、WinPSModulePath、PSModuleAnalysisCachePath 三项；presence、UTF-8 字节数、SHA256、至多 4096 字节 base64 前缀和截断分别保存，再调用原拥有者一次。不枚举其它变量，缺席值不补造，后续 child 配置不冒充原 cold 的历史环境。

固定 `native-evidence/mapping-startup.json` 位于 checker output 下一个本次独占创建的受保护子目录。既有 private helper 先以 owner/SYSTEM protected DACL 创建目录，再独占创建文件、seal 后写入/sync/close；它不更改 checker 外层 ACL，也不接管、截断或删除已存在的目录/文件。所有 write/sync/close 错误保留，失败产物继续作为证据。checker 只读这个固定嵌套路径，tagged receipt 保持含 LF 至多 256 KiB；命令捕获与执行器限额不变。

Windows 进程拥有者记录 CreateProcess/Job assignment/ResumeThread 的时长与错误，要求实际 previous suspend count=1，并从原保留 handle 读取实际 executable 与 process/native machine。同步 native 查询不承诺 context 可取消；唯一 waiter 仍拥有 handle 关闭。各单元收齐进程和三个 worker，记录实际创建次数、次序、输入/script hash、输出与清理。全部尝试之后才 hash/检查磁盘上的 PE，不把它当作历史加载字节证明。诊断不创建/删除映射或 intent，不发原生文件调用；其运行已在 VM/SMB 清理尝试后，负载和先前启动次数不同，后续成功不能独立定位原 cold 原因。120 秒测试和十五分钟作业期限保持。

## 备选方案

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

[捕获后的原生运行 35437123318](https://github.com/codetreker/remote-fs/actions/runs/35437123318)绑定 `5beb09c2fa5b514adaffa22a184ee7bf9f99bf97`，初始 inventory 在 4009 ms 超时；父进程写入 219/219 字节，三个 worker 收齐后 stdout/stderr 均为零、没有观察到 entry marker，清理前 child waiter 尚未报告 Done。没有本次映射/SMB/原生文件记录，恢复和磁盘清理完成。父端写完不证明子进程已消费输入，缺 marker 也不定位启动或模块阶段。[带启动事实的运行 35439665455](https://github.com/codetreker/remote-fs/actions/runs/35439665455)在 4010 ms 超时，219 字节已写入、实际 resume count=1，109 字节 stderr 记录 entry、before-readline、after-readline、before-json，没有观察到 after-json。输入 JSON 与 script hash 核对一致，但不能据最后 marker 认定 JSON 转换、命令发现或错误序列化的具体原因；原零输出准入按条件拒绝，所以四单元未执行。没有本次 mapping/SMB/原生文件效果。Linux PowerShell 7 的 stdin-open/EOF 均完成只约束那个环境；Windows PowerShell 5.1 原因仍未确定。规范 key 修复对应实际 Windows key 与 artifact key 不同的缺陷，原 timeout 本身未修复。[实际诊断运行 35442800512](https://github.com/codetreker/remote-fs/actions/runs/35442800512)绑定 `3eed8ec9eceb8bea5743e09d1b3c1fe7c26c093a`，原 cold inventory 仍在 4009 ms、before-json 后失败；后置四单元各执行一次且通过，entry/open、entry/EOF、inventory/open、inventory/EOF 分别为 247/191/3419/913 ms，均自然 exit 0、三个 worker 收齐、非 forced 清理。所观察的继承模块路径含排在 Windows PowerShell 根之前的 PowerShell 7 根，WinPSModulePath 缺席，cache 为 NUL。诊断根随后在私有 writer 的父目录 protected-DACL 校验失败，汇总文件为零字节；没有已保存的最终 native aggregate 或执行后 PE/hash 值。后置成功仍受 VM 清理、负载与顺序预热影响，不解释原 cold 原因，更不成为映射/cold/cache 通过。原四秒期限、flags、普通 stdin-open 和 mapping 行为保持；新的私有证据目录与 child 环境配置尚未原生验证。

当前进程拥有者、mapping、tagged 纯逻辑与私有 writer 的普通/race 控制均通过；保留旧继承模块策略、破坏 seal-before-write 次序与读取旧顶层 report 的隔离对照分别命中断言。二十七项 Python checker 用例、五项 ARM64 程序/默认与 tagged 测试构建及 vet/格式检查通过，完整范围见[测试策略](../../../../docs/testing.md#当前源码-smb-的单对象-cold-检查)。这些执行是可移植提取逻辑与受控 child 模型，新目录的真实 Windows DACL、Go-child 环境交付和 cold 效果仍待原生验证；先前四个叶用例通过、汇总写入失败和原 cold 失败分别保留。

维护成本包括 kernel/QEMU 固定输入、镜像生成、两作业 artifact 传递和宿主清理工具。boot 上限 120 秒，命令/probe 各 30 秒，输出与磁盘都有界；超限留下明确失败。磁盘使用正常 guest flush 的 writeback，普通重开证明已观察的同步持久路径，不宣称宿主断电或硬件缓存可靠性。完整平台接入仍由[Windows 提案](../../proposed/architecture/2026-09-16-platform-client-capabilities.md)承接。
