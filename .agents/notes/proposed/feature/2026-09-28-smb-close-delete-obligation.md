# Agent Note: SMB 关闭删除义务与跨重启恢复

Status: proposed

## 问题

Windows 的 delete-on-close 不是一次在 CLOSE 到达时才临时发起的按名删除。CREATE 已接受关闭删除时，删除授权、目标对象和名字关联必须固定；发起句柄后来显式关闭、连接丢失或宿主终止，都可能触发义务。其它已经允许删除共享的句柄仍可持有原对象，名字必须等最后相关句柄结束才移除。若响应丢失或宿主重启，单靠内存 FileId 既无法判断原义务是否已接受，也无法安全清理；对同名路径再执行删除可能伤害替代物。

本提案落实 [Windows 网络驱动器总方案](2026-09-16-windows-network-drive-support.md)的 7.6，并依赖 [有界 CREATE/CLOSE 提案](2026-09-28-smb-bounded-create-close.md)的 FileId、引用及 cleanup owner。要求来自 [R-FS-6、R-FS-8、R-WIN-7](../../../../docs/spec/requirements.md)。普通 SET_INFO disposition 和显式按名删除属于 [名字修改提案](2026-09-28-smb-guarded-name-mutation.md)。

## 提案

### 边界与数据形状

SMB CREATE 解码 `FILE_DELETE_ON_CLOSE` 后，先验证请求有 `DELETE` 访问、`Share.Delete` 兼容、目标是文件或目录、相关 create disposition 能由原子 `OpenAt`／`OpenChildRef` 承载。SMB 层只将 Windows 位翻译成中立 `CloseIntent{ID, Owner, Trigger: OnReferenceClose, Condition: UnlinkFile|UnlinkIfEmpty, ExpectedMetadata, Uses}`；远端契约不出现 Windows 标志、SID 或 NTSTATUS。`ExpectedMetadata` 指向 CREATE 观察到的 Windows 属性版本；SMB 层在准入时从 Windows metadata 解码并检查 `READONLY`，HTTP/直接 adapter 在同一已认证身份下完成本次删除授权。最终打开事务核对准确的 opaque metadata 版本、storage `DeleteName` 用途及共享准入、名字 guards、对象身份，再将创建／截断效果和 intent 接受一起提交；SQLite 不解释 Windows 属性，也不重新认证外层主体。打开失败且确定未执行时不得留下 intent；打开结果未知时保留原 action、intent ID 和 owner 责任，不用新动作重开路径。

宿主在每个可信 volume 的持久私有状态保存随机 `DeleteIntentOwner`。owner 是查找命名空间，不是授权凭据；认证仍由每次 authority 请求执行。配置中可信 `Share.Volume` 是稳定 volume key，跨宿主和 authority 进程重启不变；authority incarnation 只用于判定活跃文件引用失效，绝不参与 owner 文件的查找 key。`BackendIdentity` 预检必须提供并验证稳定的实际 backend volume ID，`LoadExisting` 必须核对它与持久 owner 记录及可信 `Share.Volume` 的绑定；同名 share 指向另一个 backend 时拒绝复用 owner。该身份不含 authority incarnation；重启生成的新 incarnation 不改变 owner 定位或这项绑定。

`Config.DeleteOwnerStore` 区分显式 `Provision(ctx, trustedVolumeID, actualBackendVolumeID)` 与运行时 `LoadExisting(ctx, trustedVolumeID, actualBackendVolumeID)`，两者都取得独占生命周期 lease。Provision 仅用于新 volume 首次发布之前的管理步骤：在已知该 volume 从未接受 delete intent、且尚无旧 owner 的条件下生成 owner，原子持久化并同步成功；不在 CREATE、启动恢复或正常 Publish 路径自动调用。运行时只执行 LoadExisting；文件缺失、损坏、身份不符或 lease 被占用时 fail closed，拒绝该 volume 的 delete-on-close 准入并报告恢复错误。丢失 owner 的既有 volume 必须从备份恢复原 owner，不能执行再次 Provision 来覆盖旧责任；若无法找回，继续保持不可恢复状态并显式报告，不声称 `ListDeleteIntents` 已遍历全部义务。

默认文件实现置于宿主显式配置的项目状态目录 `StateDir/delete-owners/<volume-key>/owner.json`，其中 key 只由稳定可信 volume ID 派生，不含 authority incarnation、进程或 session。文件含格式版本、可信 volume ID 摘要、实际 backend 稳定 volume ID 摘要、owner ID 和完整性校验；首次 Provision 使用同目录临时文件、文件同步、原子 rename、目录同步，且以不存在为创建前提。重启读取时校验完整记录；同一状态目录的竞争进程由独占 lock 阻止，lease 在全部本地 cleanup owner 完成前不释放。文件与其备份须一起保持持久；不能写入用户家目录的隐藏目录或依赖 Windows redirector 的句柄持久性。

每个 CREATE 在宿主 cleanup ledger 中先记录 `{volume, owner, intent ID, open action ID, file owner, phase}`，状态从 `prepared` 到 `accepted`／`unknown`，确认未执行后才可移除 `prepared`。同一个 action ID、intent ID、不可变请求负载用于重投；调用方不是 Windows 应用，而是 SMB endpoint。authority 的持久 `delete_intents` 表持有最终对象 ID、当时名字关联、触发引用和请求指纹。宿主 ledger 只跟踪自身责任及恢复游标，不作为删除事实的第二 authority。

### 状态机与所有权

| 状态 | 进入事件 | 允许动作与持有责任 |
|---|---|---|
| `prepared` | owner 与 intent ID 已同步，CREATE 尚无确定回执 | 查询／重投原 open action；保持资源预留。 |
| `armed` | authority 确认接受 CloseIntent，发起引用仍活着 | 引用可按权限使用；保持 FileId、共享 claim 与 intent owner。 |
| `pending` | 发起引用 CLOSE、连接／session 退出或宿主进程消失 | authority 立即阻止冲突新打开与名字效果；仍存在的兼容句柄可继续 I/O。 |
| `completed` | 最后相关引用离开后，原关联仍绑定原对象并成功移除 | 结算引用、锁和本地责任，然后 ACK。 |
| `not-executed` | 原关联已 unlink／被替换，或目录触发时非空等确定无删除效果 | 记录准确失败原因；结算本地责任后 ACK；不尝试后来复用的同名路径。 |
| `cleanup-failed`／`unknown` | 清理失败、网络丢失或无法判断 | 保留 owner 和额度；用原 intent ID 查询及安全推进，拒绝把它当完成。 |

发起引用关闭即触发 `pending`，不等待其它句柄；最终名字移除等待该对象的最后相关引用。CLOSE 响应中的释放结果和 delete barrier 分别处理：`Released=true` 可退休文件引用，但未终结的 intent owner 仍持有可见的 cleanup 责任；释放未知时 FileId 留在 cleanup-only 状态。重复 CLOSE、LOGOFF、断线、unpublish 与 stop 只能推进同一 intent，不分配新的删除动作。authority 的最终事务以对象 ID 和当前关联 ID 作条件；rename 更新该关联位置，unlink 或 replacement 分离关联。若触发时已分离，结果是明确未执行。目录的 `UnlinkIfEmpty` 在触发事务先检查为空；非空则立即以明确未执行终结，即使随后目录变空也不重试删除。触发时为空但等待其它相关句柄期间新增子项时，最终移除事务再次检查为空；非空则明确未执行，不能删目录中的子项。

### 恢复、授权与错误

启动时对每个已发布的稳定 volume 用 `LoadExisting` 装载原 owner，缺失即拒绝该 volume 的 delete-on-close；在允许新 delete-on-close 打开前，按 owner 分页调用 `ListDeleteIntents(owner, cursor, limit)`，逐个 `QueryDeleteIntent(owner, id)`，以原 id 推进清理；每页、单次恢复和并发数都有上限，进度持久化。列表的空页只有在 authority 确认同一 owner 的完整遍历后才表示没有待处理义务。宿主 crash 后 authority 凭持久 intent 与引用失活检测把 armed 义务触发为 pending；不依赖原 SMB session 再出现。stop/unpublish 时同一流程排空；失败进入可查询的 stopping 状态，不能报告 stopped。终态且本地引用／锁／ledger 已结算后，以稳定 `AcknowledgeDeleteIntentCommand{Action, Owner, Intent}` ACK；ACK 响应丢失则按同一动作核对，不能重复新建清理。ACK 仅回收已终结记录，不负责产生删除效果。

CREATE 的删除授权在本次 authority 请求准入时按已认证身份检查；在进入最终打开效果前，原子动作核对版本与用途并接受该义务。接受后的授权撤销不能使已接受效果被跳过。`QueryDeleteIntent`、`ListDeleteIntents`、补充清理及 ACK 是新请求，仍按当前身份和权限授权；撤销相应权限或远端不可达时保留 owner 和未完成责任，返回真实权限／I/O 错误。共享模式在打开最终事务双向核对；pending 后的新冲突 open、rename、unlink、replacement 在权威排序点失败。SMB 在准入时解码并拒绝不允许删除的 `READONLY`；最终事务以 `CloseIntent.ExpectedMetadata` 对 opaque 版本作 CAS，拒绝准入后变化；关闭时不以新观察重判已接受的效果。目录与文件的其它入口必须走同一 pending 检查；SMB 本地位不能假装全局授权。

普通 FileAction receipt 可能因 session 退休变成 `Unknown`／`Retired`，不能据此断言 CREATE 未接受。持久 intent owner 提供跨进程与 authority 重启的发现路径；同一 intent ID 的持久状态是结算关闭删除的依据。宿主对未解的普通 open action 保留有界诊断记录，直到 intent 查询给出可证明状态；若查询也不可用，向应用返回 I/O 错误并保持责任。遇到错误不得用当前名字推断原效果或删除替代物。

### 拟修改的目录与文件

| 位置 | 设计职责 |
|---|---|
| `packages/smb/delete_intent.go`（新） | 翻译 CREATE delete-on-close、预留 intent ID、驱动 armed/pending/terminal 状态和 CLOSE 结算；调用现有 FileStorage 能力。 |
| `packages/smb/delete_intent_recovery.go`（新） | 按 volume owner 分页发现、查询、重试、ACK；为 stop/status 暴露明确责任与错误。 |
| `packages/smb/config.go` | 在 `Config` 中声明必需的 `DeleteOwnerStore` 注入点；发布带 delete-on-close 能力的 share 前验证 store 与稳定 volume 身份。 |
| `packages/smb/delete_intent_owner.go`（新） | `DeleteOwnerStore` 的显式 Provision 与运行时 LoadExisting、owner 身份核对及生命周期 lease；加载先于打开准入。 |
| `packages/smb/ownerstate/`（新） | 可复用的文件持久实现：按稳定 volume ID 定位、显式首次 Provision、原子写入与 fsync、完整性校验、进程锁及备份恢复；由宿主显式选择项目状态目录。 |
| `packages/smb/create.go`、`close.go`、`handles.go`（随 7.3 建立） | CREATE 将 CloseIntent 带入同一个原子打开；CLOSE、断线和 tree/session 清理只交接既有 owner。 |
| `packages/smb/internal/wire/create.go`、`close.go`（随 7.3 建立） | CREATE 解码 delete-on-close 标志，CLOSE 编码释放与错误结果；wire 层不管理持久义务。 |
| `packages/storage/capabilities.go`、`packages/metastore/sqlite/pending_unlink.go`、`packages/transport/httprest/file_*`（按实现审查修改） | 保持 `CloseIntent`、持久查询／分页／ACK 与跨 adapter 结果一致；补足经测试证实的终态、绑定或恢复缺口。 |
| `docs/design/client/smb-endpoint.md`、相关 server/storage 设计节 | 在同一实现 PR 记录最终 owner、事务、状态与恢复合同。 |

## 备选方案

**只在 CLOSE 时按当前路径删除。** 此方案无法覆盖宿主终止，也会在 rename 或同名替换后删除错误对象，因此不选。

**只把 intent 存在 SMB 宿主内存。** 进程终止后原责任无法发现，与跨重启要求冲突，因此不选。

**让 authority 接受 intent 后直接删除名字。** 这会在其它已允许删除共享的句柄仍活着时提前移除名字，与 R-WIN-7 的关闭时序冲突，因此不选。

## 验收标准

- 对 CREATE 的打开成功、确定未执行、响应丢失、编码失败和取消分别注入故障；证实 owner 先持久化，未知结果只用原 action/intent 恢复，容量耗尽不触发对象效果。
- 在 `armed`、触发 `pending`、最后相关引用退出三个点分别注入 rename、unlink、同名替换、目录新增子项与 `READONLY` 改变；验证 rename 跟随绑定，分离后不删替代物，非空目录不丢子项，pending 阻止冲突新操作，旧兼容句柄继续工作。
- 在打开接受前后、触发前后、删除提交前后、ACK 前后分别终止 SMB 宿主和 authority；重启后用同一 per-volume owner 分页找到义务，重复恢复不重复效果，最终状态与 `Status` 资源计数一致。
- 区分首次显式 Provision 与正常启动 LoadExisting：删除或损坏已有 owner 文件、复制错误 volume 的文件、并发启动两个宿主、仅变更 authority incarnation；文件缺失、损坏或身份不符时必须拒绝新删除义务且不能生成新 owner；并发宿主只有持有 lease 者可继续；incarnation 变化后必须加载原 owner 并恢复旧 intent。由备份还原原 owner 后恢复才可继续。
- 分别撤销 CREATE 删除授权、后续查询授权、ACK 授权；验证新动作在准入时拒绝、已接纳删除仍推进，查询或 ACK 被拒绝时责任留存。并在 SMB 解码后、最终事务前改变 Windows metadata，验证 opaque 版本 CAS 拒绝旧请求。测试 HTTP adapter 与直接 FileStorage adapter，覆盖 file 与 directory。
- 对错误路径执行 package-local 覆盖、竞态测试及有界分页／额度测试；在真实 Windows 入口执行 WN-08 的关闭、断线和进程终止场景，不能只以模拟 SMB frame 代替。

## 风险

owner store 的持久性、显式首次 Provision、备份恢复和每个 volume 的稳定身份是新增部署责任；损坏或遗失时不能自动重新生成并声称清理完成。权限撤销可能让查询或 ACK 长期受阻，stop 也会长期停在 stopping；这比隐藏未完成删除更可诊断。7.6 不交付普通 disposition、显式 unlink 或 rename；它们由名字修改提案补足。当前结构必须让 intent 绑定关联可移动、可分离，且让其它入口的名字效果在同一权威排序点观察 pending，否则 8.1 将需要重写删除路径。
