# Agent Note: 未发布见证的中断不得阻止已确认状态重开

Status: proposed

## 问题

严重级别：P1。[R-WS-6](../../../../docs/spec/requirements.md) 要求异常终止后仍能重新打开已确认的命名空间。[InspectMetastoreWitness 完整读取并校验 `.METASTORE.stage`](https://github.com/codetreker/remote-fs/blob/252259f1b89682a5b2acf54004f0fe8b8677528d/packages/storage/localstore/witness.go#L52-L78)，因此 stage 创建后尚未写入的零字节状态先以 `EIO` 失败，无法进入中断清理。已有有效 `METASTORE` 与 WAL 也无法使该存储重开。

在该提交上的验证先让子进程完成已确认修改并保留非空 WAL，再真实发送 SIGKILL。随后构造发布中 `Openat` 成功、首次写入尚未发生时的零字节 `.METASTORE.stage`。重开返回 `EIO`；只删除这个未发布 stage，再次重开就能找到已确认的 `acknowledged` 节点，已发布的 `METASTORE` 未被改变。**见证发布的中断点是状态构造，并非在运行中的 checkpoint 系统调用之间注入 SIGKILL。**

## 提案

明确未发布 stage 与已接受见证各自承担的证明责任，使合法发布中断能够恢复已有已确认状态。修复同时保留 R-ERR-7、R-ERR-8 的拒绝规则：必要持久结构缺失、身份不匹配或无法证明已接受状态时仍失败。

本项补足[本地持久存储](../../implemented/architecture/2026-09-04-local-disk-object-store.md)的见证发布恢复；完整协议及各崩溃点需在实现前核对。

## 备选方案

未比较具体实现方案。验证中删除空 stage 只是区分故障来源，不能据此决定无条件忽略或删除任何 stage；stage-only、完整待发布记录和已发布见证必须分别判断。

## 验收标准

- 将零字节 stage 的构造状态固化为回归：重开后已确认名字、属性与内容均保持，重复重开仍成功。
- 在真实见证发布路径中覆盖创建、部分写入、同步和发布之间的进程终止点，证明哪些 stage 属于可恢复状态；不能仅以手工构造替代这些崩溃验证。
- 有效 final 加合法中断 stage 可以恢复；必要见证缺失、final 损坏、异库身份、已确认 WAL 丢失等无法证明状态仍以 `EIO` 拒绝，且不得补造空命名空间。
- 清理或同步失败保留原错误，不能把恢复标为成功。

## 风险

延后的是已确认数据的重开保证，checkpoint 中断可能使完整 workspace 无法服务。放宽 stage 处理也可能误接纳损坏或另一份存储的证据；修复必须维护 accepted/checkpointed generation 与 WAL 的验证关系，不能以数据可能还在为理由绕开证明。
