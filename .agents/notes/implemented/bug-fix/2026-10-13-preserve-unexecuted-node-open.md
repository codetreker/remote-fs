# Agent Note: 保留节点打开在效果前拒绝的未执行结果

Status: implemented

## 问题

FUSE 打开目录需要取得带动作回执的节点引用。动作已经登记为 pending，并不表示它已经产生引用、Use claim 或 pin。SQLite 在等待提交门时接受取消、尚未构造引用的打开，如果被记录为 unknown，挂载层只能以 EIO 作答；能够处理 EINTR 的程序因此无法完成这次明确未执行的打开。

[确定性目录探针](../../../../packages/fuse/interruption_directory_linux_test.go)在动作登记后向调用线程发送 SIGURG，关联同一次 FUSE OPENDIR 与 INTERRUPT。[Linux 的请求等待](https://github.com/torvalds/linux/blob/e8f897f4afef0031fe618a8e94127a0934896aba/fs/fuse/dev.c#L364-L407)在已发送请求被信号中断时排入 FUSE_INTERRUPT，仍等待 daemon 完成原请求。SQLite OpenNodeRef 在取得提交门前返回原 context.Canceled，没有引用或打开效果；原动作回执仍为 unknown，原始 O_DIRECTORY 和 Go ReadDir 都得到 EIO。这条证据解释该节点打开阶段，不能替其它历史 EIO 作归因。[请求中断](2026-08-22-eio-from-a-freshly-mounted-mountpoint.md)继续拥有按操作阶段分类的规则，[未解释的 FUSE 调查](../../proposed/testing/2026-09-09-trace-unexplained-fuse-eio.md)保留其它未闭合的观测。

## 决定

native metastore 的 `NodeOpenResult.NotExecuted` 要求原错误非 nil，Reference、完整 State 与 Outcome 全为零，且没有被接受的引用、Use claim、名字或关闭删除效果以及待清理责任。SQLite 只在 `OpenNodeRef` 未取得提交门时提供该证明，并保留原错误。objectstore 使用同一私有 `nodeOpenActionError{cause, notExecuted}` 保存校验结果：有效证明以及本地 `beginOpen` 在 native 调用前的拒绝为 true；矛盾证明为 false，保留原 cause 并合并 EIO。

动作记录在普通已知拒绝之前，直接消费该私有错误的分类。OpenNodeRef／OpenChildRef、`notExecuted=true` 与无效果结果同时成立，才保存为既有的 `FileActionNotExecuted`。false 保持 unknown，不能因原 cause 携带已知拒绝而取得未执行状态；包装或 join 的错误树也不能借深层证明取得这一分类。原 action ID、输入摘要、错误与结果都保留，同 ID 重投返回同一次拒绝，不重新执行打开。不带证明的取消或未知错误继续按 unknown 处理；矛盾证明返回的已有引用仍交给调用方清理。errno 与执行状态分别判断：已知未执行的纯取消可保持 EINTR，deadline 仍为 EIO；挂载层对仍为 unknown 的节点打开继续返回 EIO。

## 范围

本决定落实 [R-FS-8、R-ERR-1、R-ERR-2](../../../../docs/spec/requirements.md) 已有的动作结果与错误真实性。证明放在能够确定效果尚未开始的 native 准入处，并在 objectstore 内消费；公开 storage 结果、wire 与磁盘格式保持原有形状。[原子文件操作](../architecture/2026-09-20-durable-identity-and-atomic-file-operations.md)继续拥有打开结果与部分引用的归属。

历史 Sync、replacement-open 与 WriteFile／Chmod 的未闭合失败继续由[原调查](../../proposed/testing/2026-09-09-trace-unexplained-fuse-eio.md)追踪。后续修复可以按各自的效果阶段加入证明；当前机制不得以 errno、空引用或取消原因代替效果尚未开始的证据。

## 备选方案

**只在 FUSE 中把带取消原因的 unknown 改为 EINTR。** unknown 本身仍不能证明未执行；这样会把已经产生效果但失去结果的操作交给调用方重试。

**把所有空结果的取消都记录为未执行。** 修改或引用构造可能已经产生效果，却没有返回结果。扩大通用 errno 规则会丢掉这些未知状态；效果前拒绝必须由实际掌握阶段的实现证明。

## 后果

目录打开在 native 准入前被取消时，其已有动作可以明确报告未执行。[Go 的目录打开](https://github.com/golang/go/blob/c293dd49cbe25e1fe8d97d94a5cb618e7b6d831e/src/os/file_unix.go#L284-L295)使用 [ignoringEINTR](https://github.com/golang/go/blob/c293dd49cbe25e1fe8d97d94a5cb618e7b6d831e/src/os/file_posix.go#L254-L268)，只在精确 EINTR 时重新打开；同 ID 仍返回原拒绝。挂载层仍将真正未知的打开报告为 EIO，非 nil 引用与错误同时返回时的清理责任保持原样。

native metastore 增加了一项必须与实际阶段一致的证明，objectstore 增加了对应校验和私有分类。它们不能让调用方推断后续执行阶段，也不提供库内 EIO 重试、信号屏蔽或跨动作恢复。[文件动作设计](../../../../docs/design/server/file-handles.md#文件动作与结果核对)与[测试策略](../../../../docs/testing.md#请求中断与修改结果)定义当前处理与验证。
