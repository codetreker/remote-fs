# Agent Note: 定位 FUSE 测试中的 EIO 与请求中断

Status: proposed

## 问题

[55a31e4 的 CI](https://github.com/codetreker/remote-fs/actions/runs/34333910544/job/102408693514)在普通覆盖率重执行对拍时失败：模式修改步骤在挂载点返回 EIO，普通目录成功。该[复合步骤](https://github.com/codetreker/remote-fs/blob/55a31e4d5322c924968f32e94d20790137ffd8e2/packages/fuse/fuse_test.go)先 WriteFile 再 Chmod，原诊断只保留 errno，实际失败调用与原因未确定。保留原错误并增加有界 backing 观察后的固定 20 次普通对拍全部通过，未复现该失败。

[20a279c 的 CI](https://github.com/codetreker/remote-fs/actions/runs/34350502211/job/102462316842)又在[真实 local-store 二进制用例](https://github.com/codetreker/remote-fs/blob/20a279c3ee8cd1cb6492155c8791566f184e6159/cmd/file_handles_linux_test.go#L47)打开替换路径时返回 EIO，随后旧描述符 Close 也报 EIO；没有记录能归属该次 open 的底层原因。mounted 步骤失败，覆盖率门禁未执行。

在 Go 1.26.8 的 race 构建中，对同一 local-store 二进制用例作固定 20 次带 strace 的自然运行，19 次通过，第 3 次在已有 writer 的 Sync 处失败。没有注入信号、屏蔽信号或开启 FUSE debug；20 次全部完成，没有超时或跳过，结束后无遗留挂载、连接或所属进程。

该局部样本记录了运行时线程通过 tgkill 向执行 fsync 的线程发送 SIGURG：第一次 fsync 返回 EINTR，重试返回 EIO，随后 Close 返回 EIO。挂载端分别记录原 FUSE 请求 context 已取消；重试中的 HTTP file 请求也因 context 取消以 EIO 结束，之后 FileSession 退役。这支持该样本中的“运行时 SIGURG → fsync 中断 → FUSE 请求取消”链路，但没有 FUSE Unique，不能用请求编号逐项闭合关联；ptrace 也会改变时序。它解释的是这次 Sync 类失败，不能倒推出历史 replacement-open 或 WriteFile／Chmod 失败的原因。

## 提案

继续按实际操作取证：保留原 PathError.Op、错误链和已完成阶段，在可复现的请求上关联 FUSE 请求身份、取消与底层调用结果。失败后的有界 backing 读取只是额外观察，不能替代原调用时刻的事实。先区分 Sync 中断与尚未归因的 open／复合步骤，再确定修复边界。

信号屏蔽、通用测试辅助函数及生产取消策略的修改均留待独立工作；本性能改动不据此扩大实现范围。[现有中断语义](../../implemented/bug-fix/2026-08-22-eio-from-a-freshly-mounted-mountpoint.md)、原断言和错误门禁保持，不重试 EIO 或忽略失败。

## 备选方案

尚未选择进一步诊断或修复方案。已捕获的自然信号样本不足以在信号屏蔽与生产行为调整之间作决定，也不能替历史失败作归因。

## 验收标准

- 对被解释的每类失败记录实际调用、原错误链、阶段及对应取消证据，明确仍未知的关联。
- 后续修复须通过同条件受控验证，并保留原对拍、race、覆盖率及中断断言；没有复现的历史失败继续标为未解释。
- 诊断使用固定次数或时间预算，并清理挂载、连接和进程；不能不断重跑直到通过。

## 风险

延后期间这些失败仍可能出现。19 次通过、1 次 Sync 失败的带追踪样本既不是无追踪故障率，也不证明其它 EIO 同源；观测对时序的影响必须随结论保留。
