# Agent Note: 未发布见证的中断不得阻止已确认状态重开

Status: implemented

## 问题

严重级别：P1。[R-WS-6](../../../../docs/spec/requirements.md) 要求异常终止后仍能重新打开已确认的 volume。[InspectMetastoreWitness 完整读取并校验 `.METASTORE.stage`](https://github.com/codetreker/remote-fs/blob/252259f1b89682a5b2acf54004f0fe8b8677528d/packages/storage/localstore/witness.go#L52-L78)，因此 stage 创建后尚未写入的零字节状态先以 `EIO` 失败，无法进入中断清理。已有有效 `METASTORE` 与 WAL 也无法使该存储重开。

在该提交上的验证先让子进程完成已确认修改并保留非空 WAL，再真实发送 SIGKILL。随后构造发布中 `Openat` 成功、首次写入尚未发生时的零字节 `.METASTORE.stage`。重开返回 `EIO`；只删除这个未发布 stage，再次重开就能找到已确认的 `acknowledged` 节点，已发布的 `METASTORE` 未被改变。**见证发布的中断点是状态构造，并非在运行中的 checkpoint 系统调用之间注入 SIGKILL。**

## 决定

已发布的 `METASTORE` 是 accepted state A 与 checkpointed generation C 的唯一见证。它继续完整验证格式、校验和及 store／volume／database 身份；数据库、WAL 与高水位的对账规则不变，不使用 `.METASTORE.stage` 补充或提升已接受状态。

有效 final 旁的 stage 先通过私有普通文件、owner、链接、文件系统与长度上限等结构验证，并完成有界读取。未发布内容可以是空、部分记录或完整长度的撕裂字节，不要求可解码前缀。若内容能独立解码为完整有效记录，却属于另一 store、volume 或数据库，则明确拒绝；不能把已证明的身份冲突归为写入中断。结构、打开、读取及关闭错误同样失败，不能混入可丢弃的内容解码错误。

stage 只在既有 SQLite／WAL／高水位对账完成之后，由启动 Accept 的 publish 路径清理；重试使用同一规则。stage-only 初始化继续完整解析并验证匹配身份，在实际数据库身份可核对时仍拒绝 foreign database；不会将 stage 当作已发布见证。缺少必要 final、final 损坏或持久状态无法证明时仍失败。

清理通过删除 stage 并同步所在目录完成，失败保留原始原因并阻止成功结果。它不修改见证格式、公共 API 或 SQLite 提交协议。完整机制见[本地持久存储设计](../../../../docs/design/server/local-disk-object-store.md)，原有 A/C/WAL 取舍由[本地对象存储](../architecture/2026-09-04-local-disk-object-store.md)拥有。

## 备选方案

**继续要求 stage 完整解码。** 保持原来的保守检查，却让创建后尚未写入的合法中断阻止已确认 volume 重开。未发布内容因此承担了本应只属于 final 的证明责任。

**只放宽为零字节或已知短前缀。** 可以覆盖空文件和部分短写，却仍拒绝完整长度的未同步撕裂内容。没有完成同步与发布的文件不能被假定只会保留一个合法前缀。

**无条件删除任何 stage。** 少了内容判断，却会删除不安全的文件结构、已被完整记录证明的异库状态，并掩盖必要 final 缺失。有 final 时按已发布证据、文件结构与身份冲突判断，stage-only 则须完整通过初始化校验。

**在 Open 初期清理。** 可以及早腾空 staging name，但会在数据库与 WAL 尚未对账时抹去现场。清理放在启动 Accept 的 publish 中，使现有持久证明先完成，再处理未发布记录。

## 后果

见证写入中断留下的安全 stage 不再阻止已确认 volume 重开；即使内容无法解码，A/C 仍只来自 final。完整且可验证的异库记录、损坏 final 与缺失必要 WAL 仍是错误，不能以“数据可能还在”替代确认关系。

恢复需要额外完成 stage 删除与目录同步；这些操作的失败仍可见，后续重试不能把未完成清理当作成功。真实进程终止用例验证软件持有的发布顺序，人工构造用例覆盖具体残留形状；两者均不证明设备缓存或文件系统的掉电语义。[测试策略](../../../../docs/testing.md)分别记录它们的断言。
