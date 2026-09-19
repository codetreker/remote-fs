# Agent Note: 当前 authority 的独立 Linux 测试环境

Status: implemented

## 问题

Windows 系统客户端的验收需要实际接入当前 HTTP 与持久 authority。现有 localstore、SQLite 和 nativelease 依赖 Linux；固定 SMB 原型的内存 authority 不能证明当前接口、持久身份、配额或重启行为。把这些差别留在验收替身中，会使原生客户端通过也无法说明交付组合正确。

测试环境还必须拥有全部进程、私有磁盘和凭据。启动日志或监听端口存在不足以证明连到了本次 volume，异常退出后的后台进程也不能成为后续样本的一部分。

## 决定

[独立工作流](../../../../.github/workflows/native-current-authority.yml)把当前源码的 Linux authority 和 Windows HTTP 客户端放在同一项可核对的测试中。Ubuntu 构建固定 kernel、最小 initramfs、当前 server/helper 与私有 ext4 基底；Windows ARM64 作业使用固定的 x64 QEMU bundle，经系统翻译和 TCG 运行 Linux guest。生产 backend 不移植、不替换，当前 SMB 适配仍独立于这个准备工具。

[工具说明](../../../../.github/scripts/native-current-authority/README.md)拥有依赖 pin、装配命令和进程规则。bundle 只提取，installer 不执行；kernel package 不执行安装脚本。结构化源码发现只解码 stdout，stderr 的下载/诊断保持独立输出，命令失败仍传播；合法诊断不改变 JSON 输入。artifact 绑定输入 hash、源码与工具链；Windows 作业核对同一源码的 artifact，CI 的脏来源、缺失 verdict 或来源变化直接失败。依赖、缓存、临时磁盘与证据都留在 checkout 的 `.tmp`。

每次运行复制私有可写 ext4 磁盘。guest 中 authority 以 UID/GID 1000 访问真实 localstore、SQLite、nativelease 与 local objects；宿主只开放一个 loopback HTTP 转发，无共享宿主目录。PID 1 在任何 mount/修改前核对执行身份、初始 root、配置和 boot token；nonce、顺序号、有界控制输出将实际 child 生命周期绑定到本次运行。

Windows 目录采用受保护的 owner/SYSTEM DACL，文件内容写入前核对拥有权与权限；reparse 或额外 ACE 不被默认接受。QEMU、探针和提取进程先挂起启动、加入未命名 kill-on-close Job Object，再恢复。正常完成要求 authority 实际退出、guest sync/unmount、QEMU 退出和流排空，再删除私有副本并核对基底未变。强制终止保留失败，不能借清扫取得成功。

HTTP readiness 使用真实 lease/FileSession，验证引用、原子打开、字节、metadata 条件、改名/移除后的引用身份和逻辑配额；保留 sentinel，停止 authority，再以同一磁盘普通重开，核对 sentinel 的身份、字节和 metadata 后删除它。此工具明确报告 native_acceptance 未运行；请求它代替 SMB/一秒验收会失败。

## 备选方案

**复用固定原型的内存 authority。** 它保留可复现的缓存诊断价值，但不运行当前持久 backend，无法代替当前引用、配额与普通重启的验证。[原型诊断决定](2026-09-16-native-smb-cache-diagnostic.md)继续拥有那组实验，二者不互相替代。

**为测试把 localstore/nativelease 移植到 Windows。** 这会同时改变生产持久性和平台依赖，验收对象本身随准备工作扩大。Linux guest 保持原 authority 的代码和运行环境。

**使用 WSL 或跨 runner tunnel。** 目标 ARM64 runner 没有可用的既有 WSL/隧道环境；本次 run 拥有的 guest 与 loopback 转发能把进程、来源及关闭证据约束在同一作业中。

**MSYS2 QEMU、自编 kernel 或完整 cloud image。** MSYS2 需要固定完整 DLL/package 与签名闭包，自编 kernel 增加 compiler/config 维护，完整镜像增加无关启动和用户空间。固定 maintainer bundle、发行版 kernel 与小 initramfs 保留所需 Linux 行为，代价是显式验证提取树、固件和外部输入。

## 后果

本地 Linux TCG 已完成一次真实 guest/readiness/普通重启/关闭，私有磁盘删除、基底和输入保持不变。该本地 artifact 明确记录脏源码来源，不能充当随后 CI commit 的收据。Windows 的 QEMU 翻译运行、native ACL/Job 实测及 HTTP 组合仍未执行；这些之外，SMB 映射、系统重定向器缓存和一秒可见性也尚未由本工具验证。

隐藏 Go 模板通过显式测试入口运行，不依赖普通模块发现。当前模板普通测试 36 根、176 个通过 verdict；guest/probe/controller 的普通与 race 各有 87/63/26 个通过 verdict，Python 装配/拥有权/checker 用例共 26 根。各自验证错误、输出界限与清理，不把 mock Windows API、交叉构建或 Linux VM 成功称为 Windows 执行。具体执行入口见[测试策略](../../../../docs/testing.md#当前-authority-的独立运行环境)。

维护成本包括 kernel/QEMU 固定输入、镜像生成、两作业 artifact 传递和宿主清理工具。boot 上限 120 秒，命令/probe 各 30 秒，输出与磁盘都有界；超限留下明确失败。磁盘使用正常 guest flush 的 writeback，普通重开证明已观察的同步持久路径，不宣称宿主断电或硬件缓存可靠性。完整平台接入仍由[Windows 提案](../../proposed/architecture/2026-09-16-platform-client-capabilities.md)承接。
