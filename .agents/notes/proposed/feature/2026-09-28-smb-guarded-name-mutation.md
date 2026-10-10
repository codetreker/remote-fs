# Agent Note: SMB 受完整路径保护的名字修改

Status: proposed

## 问题

SMB 客户端按 Windows 名字发起 rename、move、replace、unlink、supersede 和删除 disposition。名字在请求观察与 authority 提交之间可能被其它入口改动；只比较末级 raw slot，不能证明父目录仍在原祖先下，也不能证明大小写折叠后唯一。对旧观察执行名字效果可能越过 share 根、修改另一对象，或让已打开句柄错误地跟随同名替代物。网络响应丢失后重新查当前名字同样不能判断原动作是否执行。

本提案落实 [Windows 网络驱动器总方案](2026-09-16-windows-network-drive-support.md)的 8.1，承接 [目录观察提案](2026-09-28-smb-directory-enumeration.md)提供的完整、有界名字事实及 [关闭删除义务提案](2026-09-28-smb-close-delete-obligation.md)的关联和 pending 状态。需求见 [R-FS-6、R-FS-8、R-FS-9、R-WIN-2、R-WIN-7](../../../../docs/spec/requirements.md)。这份工作只交付名字修改；范围锁、通知与缓存失效由后续独立提案承担。

## 提案

### 中立命令与 SMB 入口

SMB `SET_INFO` 的 rename、disposition 与当前名字信息从已认证的 Session/Tree/FileId 进入 `packages/smb` 文件命令路由。codec 验证信息类结构、长度、flag 和 Windows leaf，按 share 根生成精确 UTF-8/raw 名字映射；只支持无歧义的 Windows 可表示名字，歧义目录整体失败。文件句柄操作先确认 FileId 仍绑定原对象、具有所需 `DELETE` 访问、相关共享模式允许效果，以及当前远端授权；SMB 状态码只在边缘映射，中立 storage 接口只接收对象、名字、用途、版本和错误。

扩展 `storage.NameCommand` 为 `SourceGuards *NamespaceGuards`、`DestinationGuards *NamespaceGuards`（或等价的两个 `ChildSelection`），分别覆盖 share root 到源父目录、目标父目录的每一层目录 revision 和 exact raw edge。SMB 使用中立 `GuardedNameMutator.MutateNameGuarded` 能力提交含上述字段的 `NameCommand`；该入口要求非 nil 源 guards，rename/move/replace 还要求非 nil 目标 guards，普通无目标操作不携带目标 guards。既有非 SMB `MutateName` 可以继续接受其原有无 guard 命令，但不能作为 SMB 路由的降级路径。authority 从已认证、已绑定的 backend volume 取得可信 root ID，要求所有提供的 `RootID` 均等于该 root，且每组恰为从 root 到命令中实际 `Name.Parent`／`Destination.Parent` 的单一完整链：每条 edge 的 child 接续下一目录，终点 ID 等于实际命令父目录，不得缺段或附加无关 edge／directory。`NamespaceGuards.Check()` 只能验证内部结构，不能代替这项 root 和终点绑定。即使两个路径有共同祖先，也必须以同一观察版本表示共同节点；命令携带源 `ChildName`、`ChildCondition`，rename 目标携带 `RenameTarget{Parent, ObservedLeaf, Expected, OutputLeaf}`，并继续检查输出 slot 不被未观察的第三方占据。请求的字节和 guard 数量受现有 request budget 与 `MaxNamespaceGuards` 限制；超限在调用 authority 前失败。源或目标路径中发生替换、移动、revision 改变、名字歧义时，重新观察只允许发生在原动作被确证 `NotExecuted` 后；`Unknown` 时保留原 action 和原 payload。

SMB 在准入时解码 Windows metadata，拒绝 `READONLY` 禁止的动作；HTTP/直接 adapter 以本次已认证身份检查名字修改授权。`MutateName` 最终 authority 事务检查两侧完整 guards、精确对象 ID 和 leaf 绑定、原始与目标 opaque metadata 版本、storage `DeleteName`／共享 claim 及 pending 状态，然后原子提交 rename/move/replace/unlink。SQLite 不解释 Windows 位，也不在事务内重新认证 HTTP 主体；准入后属性变化由最终版本 CAS 拒绝。不能以 SMB 的客户端预检取代最终检查；也不能在 `MutateName` 外先 `Lookup` 后执行无 guard 操作。same-directory rename 记录旧/新关联一对；cross-directory move 仍是同一个中立动作，保留源／目标关联供后续 change feed 使用。替换让旧对象失去该名字关联，但旧引用继续指向旧对象；新对象和旧对象的 ID 永不合并。目录移动阻止形成祖先环，且拒绝把对象移出 share root。

Supersede 对已存在文件是**原对象内原子更新**：扩展中立 `OpenAtOptions.Existing` 为独立 `SupersedeFile` 效果，并给 `Initial.OnSupersede` 提供明确的初始属性／metadata 写集。guarded `ChildSelection` 与 `ChildCondition{SameNode, NodeID, ExpectedMetadata}` 在同一事务检查名字及属性版本、截断主内容、按 Windows 准入时计算的写集设置可设置属性，并在同一原子写集中将 `ARCHIVE` 置位、更新最后写入与变更时间；创建和最后访问时间保持原值，除非本次允许的显式非零时间设置要求改变，并完整保留非 Windows metadata namespaces。不得先截断后另发属性或时间动作。然后返回 `OpenResult{File, Attr, Outcome: Superseded}`；对象稳定 ID 与名字关联不变，已有兼容句柄仍指向该对象并观察其更新，新的 SMB CREATE 另得 per-open FileId。不存在目标时按 FILE_SUPERSEDE 的创建规则走同一原子打开。SMB 在准入时解码 `READONLY` 并取得所需 DELETE 访问，HTTP/直接 adapter 检查本次授权；最终事务核对 opaque metadata 版本和 storage 共享用途。FileId 容量预留、初始 metadata、allocation 和响应预算均在效果前落实。rename-based replacement 保持独立 `ReplaceNode`／`NameRename` 语义，它会分离旧关联并产生新对象身份，不能用于 FILE_SUPERSEDE。已有打开动作 receipt 用同一 action ID 和不可变 payload 恢复；引用即使伴错误返回也由预留 owner 清理。[MS-FSA §2.1.5.1.2](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-fsa/41f3734a-5bba-4c3b-9d04-7baafc9b7bfe)将已有文件的 FILE_SUPERSEDE 作为对现有 File/Stream 的修改，并报告 FILE_SUPERSEDED。

### 普通 disposition 与关闭删除义务

普通 `SET_INFO` disposition 在有效、具有删除访问的文件或目录引用上调用中立 `DeleteIntent.SetPendingUnlink(PendingUnlinkCommand)`。SMB 准入时检查 `READONLY`，HTTP/直接 adapter 检查当前删除授权。设置动作的最终事务以该引用的对象 ID、当前名字关联、`ExpectedMetadata` 的 opaque 版本、storage 删除用途、共享状态和目录为空为条件，全部通过才置 `pending_unlink`；失败无效果。每次成功的显式 SET 都独立递增持久 `pending_generation`，即使该位早已为真或另有 CloseIntent 使聚合 pending 为真。立即对冲突新打开和名字动作生效，旧兼容句柄继续按其权限使用原对象。最后相关引用离开时，以对象／关联绑定移除名字并结算对象回收；目录再次非空或关联被外部解除时报告准确终态，不动同名替代物。

清除 disposition 调用 `ClearPendingUnlink(ClearPendingUnlinkCommand{Action, Generation, Uses})`。任何仍绑定同一对象、拥有所需删除访问的有效句柄均可先读取当前 `ReferenceState.PendingGeneration` 再按该 generation CAS 清除，不能限定为原设置句柄。generation 改变则明确冲突，确证未执行后重新观察才可发新动作。成功 CLEAR 也推进显式 generation，使旧 token 永不重新生效；generation 与聚合 pending 状态分开维护。清除只影响普通 `pending_unlink` 位，不能删除 `delete_intents` 中其它句柄已经接受的 CloseIntent；若还有 pending close intent，`ReferenceState.PendingUnlink` 仍为真，新冲突请求仍受阻。FileId 关闭不意味着自动清除其它句柄可见的普通 disposition。

普通 disposition 与关闭删除共享最终权威排序点，但账本不同：普通状态是可清除的节点 generation；关闭删除是按 owner/intent ID 持久发现、触发后不可由普通 clear 撤销的义务。普通 disposition 的设置、清除、最终删除和 SMB 按名名字命令都使用各自稳定 action ID；响应丢失时查询／同 ID、同负载重投，`Unknown` 或过期时向应用给出 I/O 错误并保留 cleanup owner，不按当前路径猜测或创建新动作。只有明确 `NotExecuted` 才允许按新观察重试。

### 调用链与目录布局

| 位置 | 设计职责 |
|---|---|
| `packages/smb/commands_name.go`（新） | 执行 rename、move、replace、unlink 和普通 disposition；由 FileId 状态取得对象身份与权限。 |
| `packages/smb/name_selection.go`（新） | 用权威目录观察建立源／目标完整 ancestry guards，做 Windows 名字折叠唯一性与输出 leaf 核对；不得保留路径缓存作为最终证明。 |
| `packages/smb/internal/wire/set_info.go`（新）、`create.go`（承接 7.3） | 有界解码／编码 SET_INFO 名字与 disposition 信息类，并为 FILE_SUPERSEDE 保留独立 create disposition；wire 层不调用 storage。 |
| `packages/smb/create.go`、`handles.go`（承接 7.3） | FILE_SUPERSEDE 通过原子 `OpenAt(SupersedeFile)` 预留新 per-open FileId 与 cleanup owner；普通 disposition 由原 FileId 绑定旧对象。 |
| `packages/storage/capabilities.go`、`capability_validation.go` | 为 `NameCommand` 增加双侧 guards、metadata condition 与 `GuardedNameMutator`；验证必需 guards 是可信根到实际父目录的完整单链，保留 raw name、对象 ID 和用途的中立语义。 |
| `packages/transport/httprest/file_capability_json.go`、`file_name_observation.go`、`file_authorization.go`、`file_client.go`、`file_server.go` | wire 编码、request budget、逐操作授权及客户端 receipt 保留完整双侧 guards；服务器不得丢弃字段后调用无 guard 版本。 |
| `packages/storage/locked/capabilities.go`、`packages/storage/limited/capabilities.go`、`packages/storage/replicated/capabilities.go` | wrappers 明确实现并透传 `GuardedNameMutator`、完整 guards 与原 action；不凭局部观察宣布 namespace mutation 成功，也不降级到无 guard API。 |
| `packages/storage/objectstore/capabilities.go`、`file_actions.go` | 主直接 adapter 实现 guarded 名字动作与 `SupersedeFile`；canonical action digest/clone 纳入两侧 guards、metadata condition、supersede 写集，确保同 ID 改 payload 被拒绝且重投保留原请求。 |
| `packages/metastore/sqlite/namespace_mutations.go`、`namespace_guards.go`、`atomic_open.go`、`pending_unlink.go` | 在最终事务验证双侧完整 guards、对象/关联及 metadata CAS，执行 rename/unlink/replace/普通 disposition；`SupersedeFile` 在原对象内原子更新内容/属性并保留 ID，原子发布对应变化事件。 |
| `docs/design/client/smb-endpoint.md`、`docs/design/server/file-handles.md` 与相关 storage 设计节 | 同一实现 PR 记录正式调用链、事务和状态合同。 |

外层 adapter 必须把 `SourceGuards`、`DestinationGuards` 和条件字段字节级保留到最终 authority；它们不可被 JSON 零值或 wrapper 重建为当前新观察。`NameCommand.Check()` 验证结构与预算；`GuardedNameMutator` 的检查另外强制本操作所需 guards 非 nil、root 与命令父目录的链终点关系，不能声称观察仍有效或 root 已受信。authority 侧以实际 volume root 再核对两组 `RootID`、完整单链和终点，不接受调用方自选的另一个 root；最终 guard 版本验证与效果发生在同一个 SQLite write transaction，事务内先验证 root/ancestry、父目录 revision、源/目标 exact edge，再验证末级条件、opaque metadata 版本和 storage 共享用途，最后写名字及 event。对外只在 commit 后返回成功；事务 rollback 必须留下零名字效果。源和目标是同一目录时共享观察必须一致，不能拼接两个不同 revision 的证据。HTTP 服务器的授权检查与调用必须绑定同一已认证 authority session，并在接纳本次 mutation 前完成；拒绝时不得进入效果事务。直接 adapter 承担等价准入检查。

## 备选方案

**SMB 先查完整路径，随后发已有无 guards 的 `MutateName`。** 两步之间可发生目录移动或名字替换；末级条件无法证明仍在 share 根内，因此不选。

**只给源和目标末级父目录加 guard。** 上级目录可在不改变末级 raw slot 的情况下被移动或替换；这仍不能证明操作位于最初观察的路径，因此不选。

**用 `ReplaceNode` 实现 FILE_SUPERSEDE。** 这会更换底层对象稳定 ID，使已有兼容句柄滞留在被分离的旧对象；MS-FSA 要求修改现有 File/Stream，因此不选。

**先更新原对象，再单独打开新句柄。** 第二步失败或响应丢失会留下已修改内容却无法提供新句柄的状态，且动作回执无法原子描述两步，因此不选。

**让普通 disposition 与 CloseIntent 共用一个布尔位。** 普通 clear 会误撤销其它句柄已接受的删除义务，且无法跨重启按 owner 找到未完成责任，因此不选。

## 验收标准

- 对源、目标每层祖先分别执行并发 rename、替换、移动和 revision 变化，证明中立 `GuardedNameMutator` 的 `NameCommand` 经 HTTP、limited、replicated 到 SQLite 后在最终事务拒绝陈旧 guards，且未修改越界对象。两个路径共享祖先但观察 revision 不同时也必须拒绝。另将 guards 的 `RootID` 指向非可信 root、将完整链终点指向另一父目录、插入无关 edge／directory 或传 nil guards；即使每组 `NamespaceGuards.Check()` 自身通过，最终 authority 也必须拒绝且零效果。
- 制造大小写等价双名、无效 UTF-8、不可表示 Windows 名、目标 output slot 出现第三方占据、请求超 guard/byte 预算；逐项验证整次操作失败、无部分效果和准确错误。Unix/SDK 的 raw 名字语义不因 Windows codec 改变。
- 对 rename、跨目录 move、replace、unlink 和 supersede，在提交前、提交后响应前及客户端收到结果后注入失联；同 ID、同 payload 查询／重投只产生一个效果，过期 `Unknown` 不触碰后来占据原路径的新对象。篡改同 ID 请求的任一 source/destination guard、metadata condition 或 supersede 写集必须被 canonical digest 拒绝；直接 objectstore 与 HTTP/wrappers 得到相同回执。supersede 在成功时直接返回新 per-open 引用但对象稳定 ID 不变，旧兼容句柄仍指向同一对象并观察新内容；`ARCHIVE`、最后写入/变更时间与内容在同一提交点变化，创建/最后访问时间和其它 metadata namespaces 按 R-WIN-5 保持；rename replacement 则使旧句柄保持旧对象身份。
- 将 `READONLY`、共享 claim、授权撤销与名字操作交错；SMB/HTTP 准入拒绝新动作，准入后 opaque metadata 版本改变或共享冲突由最终事务拒绝。另在目标已 pending 时尝试打开／rename／replace，验证最终排序一致。目录 move 防祖先环、目录 unlink 防非空。
- 两个有效 DELETE 句柄先后设置、清除普通 disposition，验证连续 SET 每次推进显式 generation、成功 CLEAR 再推进、旧 token 无法清除新 SET、非设置者可清除、重复/丢响应幂等；再与另一句柄的 CloseIntent pending 交错，验证普通 clear 不撤销已接受关闭义务，最后引用离开才发生名字删除。
- 覆盖构造失败、传输错误、授权撤销、receipt 退休和提交后响应丢失；在真实 Windows 客户端跑 rename、replace、disposition 与旧句柄身份场景，并检查 Linux/SDK 经同一中立路径的行为。

## 风险

双侧完整 guards 增加请求大小，深路径可能超过预算；此时明确拒绝而不能削短祖先链。目录 revision 粗粒度可能造成并发下的假冲突；只有原动作被证实未执行时可重取观察并发起新动作。不同 adapter 的权限和事务实现必须达到同一最终核对；仅 SQLite 通过并不代表第三方 backend 可接受。若第三方 backend 不声明 guarded atomic mutation 能力，SMB 在提供这些名字操作前必须明确拒绝该 share 的相应能力。8.1 不实现范围 LOCK/CANCEL，也不承诺 redirector 缓存自动失效；这两项分别由 [范围控制提案](2026-09-28-smb-range-lock-cancel.md)与 [变更通知提案](2026-09-28-smb-change-notify-cache-coherence.md)交付。名字操作必须产出可关联的对象 ID 和旧/新关联，使后者无需从路径推测变更。
