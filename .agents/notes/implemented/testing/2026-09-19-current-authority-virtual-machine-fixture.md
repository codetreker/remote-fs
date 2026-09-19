# Agent Note: 当前 authority 的独立 Linux 测试环境

Status: implemented

## 问题

Windows 系统客户端的验收需要实际接入当前 HTTP 与持久 authority。现有 localstore、SQLite 和 nativelease 依赖 Linux；固定 SMB 原型的内存 authority 不能证明当前接口、持久身份、配额或重启行为。把这些差别留在验收替身中，会使原生客户端通过也无法说明交付组合正确。

测试环境还必须拥有全部进程、私有磁盘和凭据。启动日志或监听端口存在不足以证明连到了本次 volume，异常退出后的后台进程也不能成为后续样本的一部分。

## 决定

[独立工作流](../../../../.github/workflows/native-current-authority.yml)把当前源码的 Linux authority 和 Windows HTTP 客户端放在同一项可核对的测试中。Ubuntu 构建固定 kernel、最小 initramfs、当前 server/helper 与私有 ext4 基底；Windows ARM64 作业使用固定的 native ARM64 QEMU bundle，经 TCG 运行同一 Linux AMD64 guest。生产 backend 不移植、不替换，当前 SMB 适配仍独立于这个准备工具。

[工具说明](../../../../.github/scripts/native-current-authority/README.md)拥有依赖 pin、装配命令和进程规则。Weil 11.1.0 ARM64 bundle 按固定大小/SHA-512 和完整提取树核对，QEMU/controller/probe 必须具有 ARM64 PE；qemu-system-x86_64 的名字表示 guest target，不是宿主架构。已检查 85 项 bundled PE 的 ARM64 import closure，publisher 的未测试标签仍保留，字节 hash 不被当作签名或可复现构建。bundle 只提取，installer 不执行；kernel package 不执行安装脚本。结构化源码发现只解码 stdout，stderr 的下载/诊断保持独立输出，命令失败仍传播；合法诊断不改变 JSON 输入。artifact 绑定输入 hash、源码与工具链；Windows 作业核对同一源码的 artifact，CI 的脏来源、缺失 verdict 或来源变化直接失败。依赖、缓存、临时磁盘与证据都留在 checkout 的 `.tmp`。

每次运行复制私有可写 ext4 磁盘。guest 中 authority 以 UID/GID 1000 访问真实 localstore、SQLite、nativelease 与 local objects；宿主只开放一个 loopback HTTP 转发，无共享宿主目录。PID 1 在任何 mount/修改前核对执行身份、初始 root、配置和 boot token；nonce、顺序号、有界控制输出将实际 child 生命周期绑定到本次运行。

Windows 目录采用受保护的 owner/SYSTEM DACL，文件内容写入前核对拥有权与权限；reparse 或额外 ACE 不被默认接受。路径 guard 从 provider 入口沿实际 DirectoryInfo/FileInfo 祖先逐级检查，不把只附在入口对象上的 PowerShell 扩展属性当作 Parent 的成员；缺席、其它 provider 和任意 reparse 祖先均拒绝。QEMU、探针和提取进程先挂起启动、加入未命名 kill-on-close Job Object，再恢复。根进程退出与整个 Job 清空是不同事实：退出后最多用既有五秒预算等待实际 ActiveProcesses=0；根尚未退出、查询失败或等待到期才进入强制终止，并在最多另五秒内确认，整段清理不超过十秒。Job accounting 核对结构 ABI 与 ReturnLength，未知数据不能当作零。唯一 waiter 完成实际 wait/exit-code 查询后关闭进程 handle，Finish 不抢先关闭它。正常完成还要求 authority 实际退出、guest sync/unmount、QEMU/Job 结束和流排空，再删除私有副本并核对基底未变。强制终止即使清空也保留失败，不能借清扫取得成功。

Windows serial 控制使用 controller 在启动前独占绑定的 `tcp4/127.0.0.1:0` listener，QEMU 作为 client 连接且不重连。只接纳首个连接并关闭 listener；在读取任何控制字节或发送命令前，按反向四元组核对唯一 ESTABLISHED TCP owner row，要求属于原来保留且仍活跃的 QEMU process handle。查询前后都检查存活与期限，handle 关闭由同一 mutex 与唯一 waiter 串行，不能按 PID 重开进程或仅凭 loopback/nonce 信任对端。TCP table 固定 1 MiB、只查询一次，大小/计数/地址/端口或归属不明均失败；该同步 Windows API 不可取消，期限检查不能被描述为限制其内核执行时长。

绑定、启动、accept、peer proof 和 boot-ready 共用原 120 秒 deadline；JSON+LF 命令至多 1 KiB，沿原 nonce/sequence/source/phase 与 30 秒 deadline 写入 socket，不靠放慢或重发恢复。QEMU stdout/stderr 立即各自排空为诊断，serial 独立承载控制和 authority frame，每个流保留 16 MiB 上限。正常退出在同一 30 秒 shutdown deadline 内取得确认、QEMU exit 与自然 EOF；失败路径从入口固定十秒总期限，关闭 socket、完成 Job 和 join readers 都使用剩余时间，原自然/强制各五秒上限同时受它约束。FinishBefore 只向既有拥有者传递绝对期限，不增加清理管理器；错误、未排空或强制结束保持失败。独立 Linux driver 继续使用其 POSIX stdio，guest/backend/image 和 HTTP 转发不变。

HTTP readiness 使用真实 lease/FileSession，验证引用、原子打开、字节、metadata 条件、改名/移除后的引用身份和逻辑配额；保留 sentinel，停止 authority，再以同一磁盘普通重开，核对 sentinel 的身份、字节和 metadata 后删除它。此工具明确报告 native_acceptance 未运行；请求它代替 SMB/一秒验收会失败。

## 备选方案

**复用固定原型的内存 authority。** 它保留可复现的缓存诊断价值，但不运行当前持久 backend，无法代替当前引用、配额与普通重启的验证。[原型诊断决定](2026-09-16-native-smb-cache-diagnostic.md)继续拥有那组实验，二者不互相替代。

**为测试把 localstore/nativelease 移植到 Windows。** 这会同时改变生产持久性和平台依赖，验收对象本身随准备工作扩大。Linux guest 保持原 authority 的代码和运行环境。

**使用 WSL 或跨 runner tunnel。** 目标 ARM64 runner 没有可用的既有 WSL/隧道环境；本次 run 拥有的 guest 与 loopback 转发能把进程、来源及关闭证据约束在同一作业中。

**通过 Windows x64 翻译运行 QEMU。** 该 bundle 的版本/能力查询成功，但 guest 启动在 authority 之前以 0xC00000FF 退出，具体 unwind table/module 未确定。native ARM64 bundle 保留同一 QEMU 版本、guest target 和完整提取核对，移除宿主二进制翻译依赖；[native ARM64 的实测](https://github.com/codetreker/remote-fs/actions/runs/35423839231)已取得 guest boot-ready，但不能据此反推旧异常的根因，也不代表整个 authority 生命周期通过。

**继续用 Windows stdio 传控制命令。** 被检查的 callback 在 frontend 接纳能力不足时可能确认没有完整交付的输入，提取函数与实际 ARM64 binary 分析核对了该分支。实际超时运行没有逐字节接收记录，不能断言就是某次零容量造成；为保持有背压、可认证且可取消的控制通道，socket 不依赖这一 stdio 行为，原诊断输出仍被完整拥有。

**改用 Windows named pipe。** 已检查的 QEMU 后端只创建 server，未提供所需的明确 DACL、first-instance 或远端 client 拒绝配置，也不能连接 controller 先建的私有 pipe。创建后再改 ACL 留有窗口，TokenDefaultDacl 的覆盖及额外权限未获证明；不以这些假设替代持有进程 handle 的 socket peer 核对。

**MSYS2 QEMU、自编 kernel 或完整 cloud image。** MSYS2 需要固定完整 DLL/package 与签名闭包，自编 kernel 增加 compiler/config 维护，完整镜像增加无关启动和用户空间。固定 maintainer bundle、发行版 kernel 与小 initramfs 保留所需 Linux 行为，代价是显式验证提取树、固件和外部输入。

## 后果

本地 Linux TCG 已完成一次真实 guest/readiness/普通重启/关闭，私有磁盘删除、基底和输入保持不变。该本地 artifact 明确记录脏源码来源，不能充当随后 CI commit 的收据。[native ARM64 运行 35423839231](https://github.com/codetreker/remote-fs/actions/runs/35423839231)绑定其镜像和源码，controller 21 根/53 verdict、probe 8 根/63 verdict 与路径 guard 十二项通过；实际 Linux init、私有 ext4 挂载及带身份的 boot-ready 已取得。后续 start 命令在三十秒内没有确认，authority 是否执行仍无法确认，空日志不能证明没有执行。强制 Job 清理使完整生命周期失败；私有磁盘已删除、基底不变也不能补成 graceful 成功。该次仍使用 stdio 控制。HTTP readiness、普通重启、完整 Windows 关闭及 SMB/缓存一秒验收尚无成功证据；新 socket 的 native peer proof 和整条生命周期同样尚未执行。

隐藏 Go 模板通过显式测试入口运行，不依赖普通模块发现。已有 Linux 模板普通验证为 36 根、176 个通过 verdict；guest/probe/controller 的普通与 race 各有 87/63/26 个通过 verdict，Python 装配/拥有权/checker 用例共 26 根。Job rundown 的三个 AST 提取测试根普通/race 各 11 个通过 verdict，恢复立即强制清理决策的对照在正常退出后计数收齐的断言失败；vet 和 Windows AMD64/ARM64 构建通过。这些局部检查与上述 native helper 收据分开，不能替代 guest 生命周期。各自验证错误、输出界限与清理，不把 mock、交叉构建或 Linux VM 成功称为 Windows 执行。[路径回归](../../../../.github/scripts/native-current-authority/run_windows_test.ps1)直接抽取实际 guard AST，在 VM 前核对 provider/raw-parent 转换及拒绝路径；本地九项通过，旧 guard 的因果对照在父链缺少 PSIsContainer 处失败。Windows 的十二项运行包含三个 junction 场景；这个已观测的路径检查不证明后续 bootstrap 可用。native ARM64 包选择的 controller 普通/race 各 14 根/35 verdict、宿主架构反转对照、相关构建/vet、七项 Python 与九项本地 PowerShell 检查通过；它们与后续 native boot 收据分别计证据。具体执行入口见[测试策略](../../../../docs/testing.md#当前-authority-的独立运行环境)。

维护成本包括 kernel/QEMU 固定输入、镜像生成、两作业 artifact 传递和宿主清理工具。boot 上限 120 秒，命令/probe 各 30 秒，输出与磁盘都有界；超限留下明确失败。磁盘使用正常 guest flush 的 writeback，普通重开证明已观察的同步持久路径，不宣称宿主断电或硬件缓存可靠性。完整平台接入仍由[Windows 提案](../../proposed/architecture/2026-09-16-platform-client-capabilities.md)承接。
