# Agent Note: 原生 SMB 缓存诊断固定执行对象

Status: implemented

## 问题

Windows 原生重定向器可以在远端创建已经完成后继续复用此前的名字不存在结果。单个 SMB 请求正确、暖文件读取新内容或没有授予数据缓存权限，都不能证明后续按名访问满足一秒、非 TTL 的可见性。单次监听收到通知，还不足以证明重新监听的间隔、深层名字、卸载与故障期间的行为。

[平台客户端提案](../../proposed/architecture/2026-09-16-platform-client-capabilities.md)把这项可行性放在广泛实现之前。诊断需要一个能够实际运行、版本确定的 SMB 接入对象；负查找通过还不能说明已经缓存的属性、长度、内容或名字替换会及时更新。正在变化的通用接口不能成为每次重现时额外变化的因素。旧原型的成功也不能被当作新实现的验收。 与系统自带 SMB/NTFS 对照可以检验相同原生身份与字节行为，但没有完整 wire 证据时只能作为行为参考，不能据此解释缓存或租约机制。

## 决定

[诊断工作流](../../../../.github/workflows/native-smb-gate.yml)固定以 [1cb9ad7f49d998de4daa4d562d766b18cf06ce16 的 SMB 原型](https://github.com/codetreker/remote-fs/tree/1cb9ad7f49d998de4daa4d562d766b18cf06ce16/packages/smb/windows)为基底，在 `.tmp/native-cache-gate/fixture` 检出后，检查并应用[通知连续性补丁](../../../../.github/scripts/native-smb-notify-continuity.patch)，再按精确源码锚点注入聚焦测试及有界追踪。执行对象由基底、overlay 和探针共同确定，每次记录补丁 SHA256，不能仅凭 HEAD 仍指向 pin 就称树内容完全相同。补丁只进入临时诊断检出，不进入当前生产包或改变通用文件、身份及生命周期契约。

平台 overlay 在已建立目录监听的底层 stream 仍健康、generation 未变时，保留 rescan 交付后的注册与有界事件队列；重新请求不再用新 checkpoint 丢弃交付后的间隔。底层来源更换或失败仍使旧注册失效。两项[回归用例](../../../../.github/scripts/native-smb-notify-continuity_test.go.txt)分别检查间隔事件保留与来源替换隔离，和基底现有 Notification 测试一起先于原生场景执行。

工作流由其 workflow、运行脚本、原生探针、连续性/最终缺失状态补丁及对应回归、positive cache/wire 观察用例的 Pull Request 变更或手动触发，以十八个独立的 Windows 11 24H2+ ARM64 作业运行。`baseline`、`held_parent`、`notify_parent` 分别比较没有显式父目录句柄、保留父目录句柄及已有 CHANGE_NOTIFY 的情形。`owned_nested`、`owned_lifecycle`、`owned_outage` 使用测试夹具拥有的递归 UNC 监听，检查多层目录、100 ms 重新监听间隔与八名字突发、busy/正常卸载及 HTTP/SSE 故障。owned_rescan 仅把 fixture 的 MaxNotifyEvents 缩小为 2，在实际请求对应的 wire ENUM 响应后暂停，再在重新监听前建立一个新的负查找与远端创建。它继续要求一秒、缓存到期前的新权威查询和后续健康监听，不用本地零字节代替 wire 证据。directory_sharing 以实际 NTFS、可写父目录下的子目录对比 SMB 子目录，另行记录本地 volume 根与导出 share 根；它比较 LIST/READ_ATTRIBUTES-only、双方打开顺序和普通文件读共享拒绝对照，并先排除已有的共享冲突。root 与非 root 分别测量，目录是否受某种 share mask 约束须由真实结果回答。find_notification 进一步比较 FindFirstChangeNotification 与 LIST/share=6 根目录打开的双方顺序，并要求成功的 SMB 通知句柄有新 Pending；它只验证共存性，不包含可见性、重新监听或故障验收。该用例还只读记录三个 NTSTATUS 的系统 Win32 映射，不改线上状态码，不据此推断缓存策略。missing_final_status 是另一个独立对照：施加[最终缺失状态补丁](../../../../.github/scripts/native-smb-missing-status.patch)，先无监听/父目录句柄运行，再在第一阶段通过后持有根 LIST/share=0 重复创建可见性；任意 CHANGE_NOTIFY 都使该对照失败。这些测试拥有者不构成生产 ManagedShare API 的选择。

缺失状态补丁的标记仅在父目录检查成功后产生，最终 CREATE 且清理成功才选择 0xc000000f；中间组件、权限、普通 ENOENT 和未知错误没有因此改类。它有独立的 SHA256 和两项[回归](../../../../.github/scripts/native-smb-missing-status_test.go.txt)，不与连续性补丁或只读 RTL 观察混称。该诊断检验一个真实最终缺失结果的另一种平台表示，不给错误未知的情况增加默认值。

positive_unheld/positive_exclusive 同样施加最终缺失状态补丁，在无根句柄和根 LIST/share=0 两种条件下，各运行十五个隔离 cell。路径属性、BasicInfo、StandardInfo、异步/同步 ReadFile 分别以自己的第一次增长/缩短观察判定；rename、replace、rename-away 后重建分别比较路径、新打开及原引用。真实 HTTP 准备过去 mtime，ACK 后 First[] 的结果不可被随后诊断覆盖。namespace 对照使用同步句柄，异步读取独立检验；recreate_open 在缺失和重建阶段都先调用 CreateFile。这个拆分防止一个查询先刷新缓存，让另一个本来会失败的 API 看起来成功。

positive 的 compound/wire 观察与 authority metadata、digest 逐项关联，读取原字节和认证 token 不进入结果。即时 cell 要求一秒、到期前、没有 CHANGE_NOTIFY 与缓存权限；缓存期限从目标准备前最早的实际 root/CREATE 计起。异步 EOF 不容纳合并的等待、取消或清理错误；映射先记录拥有权，异常退出后的精确恢复仍判为清理失败。QUERY_INFO class 34 的 FILE_NETWORK_OPEN_INFORMATION 按实际线格式解析时间和长度，不能把观察器缺字段当作原生缓存旧值。

positive_postdeadline_unheld/positive_postdeadline_exclusive 各增加一个独立的路径增长观察。真实 CREATE/WRITE/协议无缓存授权证明先在独立 setup fixture 完成；句柄、映射、SMB/HTTP 和全部保留资源确认结束后，才新建没有 proof file 的测量 share。测量使用自己的 trace、协商检查与曝光起点。真实写入 ACK 后，直到计划 1.1 秒的第一次 GetFileAttributesExW，不触碰目标或相关目录；实际开始严格超过一秒且开始/结束都在原始缓存到期之前。测量间隔仍严格检查额外请求，fixture 退役不被当作 Windows 后台流量永远不会出现的保证。原协商能力 0x26/State NONE 和即时一秒 validator 保持原样。

延迟首值及其与暖 tuple 的相等性立即保存；时间、原生错误或安静间隔失败也继续尝试 post-query oracle 和普通清理。旧值只有在原生观察完整、严格安静、时间合法且 oracle 未变化时才是候选，经过全部本体、句柄和卸载清理后，没有其它错误才成为 violated；清理失败后计数归零也不修复证据资格。延迟当前值只说明在该次查询时已更新，记录 within_one_second_proven=false、contract_result=inconclusive。作业名明确非 SLO 验收，证据收集通过不把一秒期限改成 1.1 秒。独立 setup 退役及完整无效样本记录使原能力与 NoLeasing 的延迟结果都能按下述条件判定。观察器合成回归普通/race 各 31 pass，基线和 NoLeasing 两种 ARM64 组合构建通过；这些局部检查与原生结果分别归属。

NoLeasing 是另一个独立的平台策略对照。[补丁](../../../../.github/scripts/native-smb-no-leasing.patch)仅进入四个显式 noleasing 变体，在原通知/最终缺失 overlay 后逐层检查和记录；它声明 leasing 与 directory leasing 不受支持，只保留 LARGE_MTU=0x4，最终仍为 SMB 3.1.1。保留 signing/preauthentication 和 CREATE 外层结构验证，忽略不支持的 RqLs 内部字段，不建立 lease table/owner 或 lease-key 关联；所有成功 CREATE 都不授予 oplock/cache，未请求的 ACK 不伪装成功。原 lease 身份检查文件不变，也不改变 authority、NodeID 或引用生命周期。运行脚本在基线 overlay 完成后，只把 NoLeasing 的三份目标 Go 文件和补丁规范为无 BOM 的 LF，记录 raw/canonical input、patch 和 output hash，并核对已审查值。git apply 仅本次关闭 autocrlf，先检查再应用；LF 与 Windows CRLF 输入必须得到逐字节相同的已审查代码，任何偏差都失败，不用宽松 hunk 匹配改变执行对象。

两种根模式分别运行十五项即时 cell，再各用全新 fixture 运行一项独立延迟观察。原 0x26/0x6 基线及其 State NONE 保持原判据，实验 trace 必须实际显示 capability=4、最终 3.1.1；即时 fixture 或已完全退役的延迟 setup fixture 单独证明 proof-file CREATE 为 NONE 且无 lease response，测量连接仍检查自己的策略。旧 H 仍返回 A，新打开必须取得替换后的 B；一次重开成功不能代替属性或名字缓存验收，1.1 秒的新值也仍不证明一秒可见性。

七项[专用测试根](../../../../.github/scripts/native-smb-no-leasing_test.go.txt)在原生场景前取得逐根 verdict。局部协议/回归普通与 race 各 29 pass，ARM64 构建通过；只恢复原 capability 的负向对照在协商断言失败。这些不包含原生缓存效果，仍未知的动作也不由 QueryAction→Completed 回归宣称已经恢复。原生 NoLeasing 的结果与这份局部验证分开记录；它未被选作生产默认策略。

固定 Go 版本为 1.26.8，测试三分钟、作业十五分钟；编译缓存、模块缓存和临时状态放在工作区 `.tmp` 下。创建的成功判据同时约束 authority 结果、时间与新请求：初次负查找有匹配的 NAME_NOT_FOUND，远端创建确认后的一秒内出现新权威 CREATE 与可见文件，且观察早于最早可能的缓存到期。通知与故障情形各自核对匹配事件或不可用错误，零字节成功或 ERROR_NOTIFY_ENUM_DIR 明确表示丢失明细，测试拥有者记录后重新监听；它不维护目录快照，不能把重挂监听说成枚举恢复。owned_nested 的初次/普通间隔通知仍必需，只有突发明确丢明细时允许不具备每个名字的 ADDED，所有可见性检查保留。三个系统缓存 lifetime 必须大于一秒且诊断不得修改设置。完整可执行规则由[测试策略](../../../../docs/testing.md#windows-原生-smb-缓存诊断)拥有。

祖先通知实验由[独立工作流](../../../../.github/workflows/native-smb-parent-invalidation.yml)和[脚本](../../../../.github/scripts/native-smb-parent-invalidation.ps1)执行，在同一固定原型上施加三份已核对的 overlay。它只监听自身三个文件的 Pull Request opened/synchronize 变更，单作业执行七个新 share/connection，不改变原十八个诊断作业。真实 share 根上保留递归 watcher，应用在真实子目录 `v` 上以 LIST 打开；增长覆盖 ShareAccess=0/6 与双方打开顺序，另有 share0 无 watcher 对照，以及 share0/app-first 的缩短、替换身份。这回答不同目录上的监听能否共存并推进特定观察，尚不定义生产私有父目录或映射 API。

[祖先探针](../../../../.github/scripts/native-smb-parent-invalidation_test.go.txt)的增长和缩短分别以真实 HTTP 把暖属性的 257 字节改为 769 字节、769 字节改为 73 字节。首次 GetFileAttributesExW 必须在 ACK 后一秒内完成，且早于原始最早到期，计时包含通知等待。native-call ledger 以连续、唯一的操作开始/结束序号和 mutation 序号记录因果顺序，时钟仍用于期限；这避免时钟精度把已经完成的准备调用误判为插入写入后的访问。harness 不得在写入后额外访问目标/祖先或提前 oracle，合法的自动刷新不受此禁令限制。匹配通知之后、新 tuple 的目标响应可以早于 ACK 或首次 API，分别标记 `refreshed_before_first_api`、`refreshed_by_first_api`，关联不完整则不能证明机制。

替换只预先保留原生 A 句柄，B 完全通过 HTTP 准备；mutation 前及最终排空的轨迹均须满足冷态判据：整个轨迹排除 B 的 CREATE/QUERY_INFO/READ，直到 ACK 排除目录枚举和未知 file ID；最终核对只能撤销、不能升级先前资格，迟到污染保留原始数据并使样本 inconclusive、测试失败。不能用预先打开 B 来教会 redirector 新身份。覆盖后先同步打开 target，再核对同一 volume 上的新旧原生 ID 不同、旧 ID 未变及 A/B 各自字节，不能把原生 ID 数值直接等同于 authority NodeID。通知区分 DETAIL 与 VERIFIED_RESCAN：前者核对完整 wire/native 事件及 REMOVED 后相邻 OLD_NAME/NEW_NAME；后者只在当前请求的真实 0x10c 与排空的原生零字节或 ERROR_NOTIFY_ENUM_DIR 配对时成立。VERIFIED_RESCAN 一旦出现便保持 rescan_required=true、precise_notification_proven=false，不能把后续片段重组为已证明的完整事件序列。两者最多三次完成、三次重挂，保持同一 watcher 及精确新 Pending，共用 ACK+850 ms 截止。rescan 还须有 native event 的未完成检查点及首个 API 前的 wire 观察顺序，最终轨迹只能撤销资格；检查点不保证内核未来不会完成。首次打开与全部身份/读取检查仍须在 ACK+一秒及缓存到期之前完成，rescan 成功另标 current_replacement_identity_after_verified_rescan_before_deadline，不冒充精确通知或目录枚举恢复。原始首值、后置双对象 oracle、轨迹和最终清理共同决定资格；旧首值是候选失败，不单独成为超过一秒反例，无监听对照也不外推 share6 因果关系。

精确通知另由[独立单项工作流](../../../../.github/workflows/native-smb-precise-invalidation.yml)与[运行脚本](../../../../.github/scripts/native-smb-precise-invalidation.ps1)执行，只运行全新的 share0/app-first 替换身份场景。固定原型、既有三份 overlay 之外再加入[历史通知补丁](../../../../.github/scripts/native-smb-precise-history.patch)，父探针 Go 判据保持原样；共享脚本限定产物目录和 precise 测试进程清理。已知补丁输入经原始/规范 hash 核对后转换 CRLF→LF，再核对规范输出；不接受未知字节。成功 Verify 明确退出 0，策略变化或资源残留仍失败。实验对象仍是有明确来源的诊断树，不能成为当前生产通知 API 的承诺。

[历史 producer](../../../../.github/scripts/native-smb-precise-history_test.go.txt)从实际 HTTP Snapshot 的完整 EOF 建立有界前像，以原 Rename 同一锁内捕获的 P/Q、两项实际事件、action 输入指纹和已保存的原动作回执绑定事实，再核对 RenameWithBarrier 的回复/barrier 与 stream 真正交付的完整区间。上限为 64 节点、256 KiB 前像和 1 MiB 证据，不用预期结果填通知。历史 full-name proof 与当前披露权限分离：存活引用、授权、实际祖先和完整 sibling 集继续检查；group/验证 pin 的拥有权延续至 release，Close 排空验证和释放工作。这让历史叶名不必假装仍属于当前树，同时不会把旧权限当成当前授权。

新单项必须取得精确 DETAIL；任何 ENUM 都停止该证据路径。被动观察只记录已有 QFid/identity 查询及原始、有效 compound session 关联，不主动刷新目标。HA/Hnew、不预热的 B、首个打开、不同原生身份与 A/B 字节、一秒和原 TTL 的判据均不放宽。它检验有真实历史事实支持的精确通知能否改变这一个替换结果，不把旧 typed-rescan 的身份失败撤销。

[always-truthful QFid 单项](../../../../.github/workflows/native-smb-qfid-invalidation.yml)在同一固定精确通知 fixture 上只改变成功 CREATE 的身份 context：实际 State 在打开准入前取得 volume 身份，回复使用捕获对象 ID 和真实 serial；未请求时在私有响应列表添加一个 QFid，已请求时仍只有一个。请求校验、响应预算、错误及清理不放宽。[补丁](../../../../.github/scripts/native-smb-qfid-context.patch)限于诊断检出，默认 requested-only 路径保持原 server 输入。未请求 QFid 的符合性尚未确定，因此响应被客户端接受也不能自动成为生产方案。

这项刺激用于区分“没有新身份响应”和“收到真实新身份仍未分离”两种条件。选定新 CREATE 必须确实未请求 QFid；记录缺席、返回原始字段及捕获 B/volume 的关联，不能通过额外查询或预先打开 B 告诉重定向器答案。响应接纳、新句柄身份和旧 HA 稳定性分别判定，任一旧身份被替换或两个句柄仍同身份均失败；精确 DETAIL、原生 opaque 身份/A/B 字节、一秒与原 TTL 仍是同一判据。此前 requested-only 的合格失败保留，不因新实验可能成功而改写。

[POSIX 能力位实验](../../../../.github/workflows/native-smb-posix-capability.yml)单独回到 requested-only QFid 的精确 DETAIL 基线，只将原型 Fs5 的 0x6 改为 0x406。[SMB 查询规则](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/8608a6b8-1e4d-4b25-84e7-003d9faadc49)对该位使用 SHOULD clear，[Microsoft 产品说明 422](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/a64e55aa-1152-48e4-8206-edd96444e7f7#Appendix_A_422)另记录 Windows server 保留 FILE_SUPPORTS_POSIX_UNLINK_RENAME 的行为；真实 fixture 也验证了 A 被替换名字后仍保留引用与字节、B 可在新名字打开。这支持测试该底层能力声明，不预设 Windows 的身份算法，也不替任意 backend 声明能力；SET_INFO 64/65 等未实现命令仍拒绝。

[补丁与控制](../../../../.github/scripts/native-smb-posix-capability-controls_test.go.txt)保持名字、serial、协商、requested-only QFid、native 首调用和全部身份/字节期限不变。真正 Fs5 的 raw/decoded 响应须绑定 HA，先于与被动事件同域的全局 mutation marker；该 marker 位于原 native-call mutation boundary 后、Rename 前，不增加查询。否则能力未暴露或证据无效，即使其它观察看似正确也不能算该机制成功。always-QFid 的失败独立保留，不与这一个能力位同时改变。

[组合单项](../../../../.github/workflows/native-smb-posix-qfid-invalidation.yml)检验 POSIX 能力曝光是否改变随后真实新 QFid 的处理；这只是两个已定刺激的交互假设。显式 InteractionPolicy=posix-qfid 才允许同时启用 posix-unlink-rename/always-truthful，默认 standalone 保留原三个配置和意外组合拒绝。两份语义补丁/控制模板不变，先核对 QFid 的中间结果再施加 POSIX 位，未知或反序输入拒绝。

成功资格要求同一 HA 的实际 Fs5 早于全局 mutation，完整 DETAIL 后的首个新 CREATE 确实未请求但接收到真实 B/volume QFid；两份证据分别保留且都须成立。一个能力已暴露不能补足另一个缺席，新增回复也不能补造原生身份成功。原 first-call、HA 稳定/Hnew 不同、opaque 身份与 A/B 字节、一秒/TTL、冷 B 和清理不变；旧 alias 与 retained-identity 失败不会被重新解释。未请求 QFid 的符合性及客户端内部机制继续独立未决。

[pre-HA bootstrap 单项](../../../../.github/workflows/native-smb-early-capability-bootstrap.yml)只检验能力曝光时点：在现有 ancestor watcher 已 Pending 后、原 HA API 前，由同一测试 executable 的专用子进程查询 root Fs5。仍用 requested-only QFid 与固定 0x406，不改后来身份/字节和一秒/TTL 判据；已有目录 handle 保持，不能称为早于全部 FCB。公开依据没有确立 FCB latch 的具体时点，此实验也不把这一假设当事实。

子进程继承普通 token，必须证明 SID/logon/session、实际进程与 exact executable/nonce，查询不触及 A/B。同步 root handle 的一次 NtQuery 使用固定 pinned IOSB/4096 字节 buffer；非 PENDING 只接受返回 NTSTATUS 的 SUCCESS，正常 close 后实际 exit 0 才交付。PENDING 保留进程生命周期的 pins/root，明确失败退出；十秒执行加五秒 termination/drain 均不延长后来的一秒窗口，未证实 process completion 不能继续 HA 或声称取消完成。[被动检查](../../../../.github/scripts/native-smb-early-capability-wire_test.go.txt)另核对 root Fs5 < settled exit < HA API marker < HA CREATE 的全局次序、同一 connection/session/tree 与正确 share；raw/derived 归属分开，root open 可按真实已知关联复用，不由 token 相同推导连接或隐藏目标预热。

[early+QFid 单项](../../../../.github/workflows/native-smb-early-qfid-interaction.yml)把 pre-HA root 能力曝光与首个新 CREATE 的真实未请求 QFid 组合，检验两项刺激的时序交互。显式 before-ha-qfid 必须同时选择 posix-unlink-rename、always-truthful 和 posix-qfid；原 before-ha 仍为 requested-only 独立分支。复用原进程 helper、被动 collector 及 QFid→POSIX 补丁，进程十秒加五秒清理和原生调用序列保持。

[联合资格检查](../../../../.github/scripts/native-smb-early-qfid-interaction_test.go.txt)要求 root Fs5=0x406 < 成功退出 < HA API 的早期证据，与首个新 CREATE 未请求但实际返回真实 B/volume QFid 的证据共同成立。两者须绑定同一实际 HA/Hnew、connection/session/tree 和独立取得的 volume；源事实、进程失败和原生 oracle 分别保留。PENDING、forced、未确认退出或任一刺激缺席均不能通过。旧 HA 稳定、Hnew 不同、A/B 字节、精确 DETAIL、冷 B、一秒/TTL 与清理判据不变；未请求 QFid 的符合性仍未确定，这项组合不引入生产默认政策。

[inbox SMB/NTFS 参考](../../../../.github/workflows/native-smb-inbox-reference.yml)是独立的 direct445 单项。它使用实际计算机名、当前 logon 的 NULL username/password WNet 连接及本次拥有的 Temporary share，保持系统服务、安全和缓存设置。只读 preflight 要求 Windows 11 ARM64、权限、Server/Workstation、SMB2、445 与实际 NTFS 条件；不满足即拒绝，不启动服务、换凭据或改用代理。新私有目录只含 A257/B769，share 的路径/nonce/SID 指纹和本地目录身份都由外层拥有者保留。

[原生场景](../../../../.github/scripts/native-smb-inbox-reference_test.go.txt)保持 app-first/share0、祖先 watcher、HA 与原 legacy 身份/字节查询次序。变更由 server-local NTFS handle 执行一次 FileRenameInfoEx Flags3，成功返回即是本地 ACK，不冒充 HTTP/action barrier。成功必须保持旧 HA 身份及 A 字节、新打开取得不同身份及 B 字节，并在 ACK+一秒与从最早连接准入计算的原 TTL 前完成。原生 DETAIL/rescan 事实如实记录；rescan 只在原同 watcher 的有界 drain/rearm 下继续，不标成已证明的 wire ENUM，不插入目录枚举。

[目录 write-sharing 对照](../../../../.github/workflows/native-smb-inbox-share-write.yml)在独立新 share/connection 中只把应用目录的 ShareAccess 从 0 改为 FILE_SHARE_WRITE=2，DesiredAccess 仍为 LIST=1；绝对 local target、Flags3/RootDirectory NULL、watcher、HA、B 和原首值/字节/一秒/TTL 判据保持。它检验这一个目录共享参数对相同请求的影响，成功也不满足原 share0 场景，更不改变中立 core 的目录 share0 语义或生产策略。

原始和对照策略固定为 original/write-only，分别选择固定临时根、具名 entry 与结果名；ledger、环境、编译策略、初始请求、所有 IPC stage 和结果必须一致；策略四字段必须显式存在、非 null 且类型正确，缺失字段不默认 original 或 share0。生成器对冻结模板的副本加入双方相同的 policy/错误观察，再只改变对照的 entry/label 和唯一 app share 参数；逆向规范化须还原原 AST。提取的 native helper body 不变，但生成场景和默认 binary 已有观察字段变化，不能称为旧 binary 逐字节相同。原 NativeOracle 与 NativeOutcome 保留，对照结果单独使用 share_write_control_ 前缀。

两策略分别保存 source B open 与 rename 实际返回的错误对象、状态和可取得的 unsigned32 returned x/sys syscall.Errno。rename 观察在原成功 ACK 赋值之后、mutator.Close 与 error join 之前进行，不能借 close errno 推断 rename，也不另读 GetLastError。错误不可提取时明确 unavailable，未尝试与 nil 成功分开；x/sys 的返回 errno 不承诺是独立保存的原始 GetLastError。

[controller](../../../../.github/scripts/native-smb-inbox-reference.ps1)以绑定 nonce/PID/executable 的双阶段 IPC，在 mutation 前和原生观测完成后核对真实 server session/open、当前 SID 与 owned share/path；server bookkeeping ID 不冒充 native FileIndex。子进程拥有映射、watcher 和所有 handle，六十秒生命周期后仅另有五秒 termination/drain；未确认退出保留 ledger，forced 仍失败。只有原 child 静止、精确 mapping/share/目录指纹继续匹配时才清理；Temporary 的重启寿命不能代替清理完成。 Prepare 在两个固定根任一已有 ledger 时拒绝；Verify 只检查这两个已知位置，零份或两份候选不选择或删除。只有唯一 ledger 的 source/nonce/path 及原拥有权条件成立后，才按实际根清理；选择器或记录策略不一致继续是失败，既不授权删除未证明的 owner，也不放弃已证明 owner 的合法清理。

来源拒绝时，controller 只捕获现有选中 session/open 行的 ClientComputerName 原 CLR type、支持的 raw string、确切传入字符串，以及已有 preflight 本地地址的解析/原相等比较事实。记录 source/nonce/stage、opaque 行 ID 和已完成检查，不增加 CIM、接口或 DNS 查询。每阶段独占写入的 UTF-8 证据至多 64 KiB；未知 type 或超限明确 capture_incomplete，解析失败与不可用事实显式保留，不截断后猜出地址。捕获、写入或关闭故障另保留，最初的 origin error 继续是主失败；诊断不能授权后续 mutation。

管理 peer 的原精确 computer-name、loopback、mapped-address 与 IPAddress.Equals 成功路径保持。只有最后未匹配分支中的原始 string 不含显式 `%`、解析为 scope0 的 IPv6 link-local，且现有有界 preflight 输入恰有一行同 16 字节、非零 zone 的 link-local 地址时，才记录 `unique_unzoned_link_local_inventory_match`。同字节重复行（即使同 zone）、多个 zone、显式 `%0` 或不匹配 zone、foreign/port/非 link-local 等不能借此分支通过；畸形或超限比较输入失败，不跳过行；已经由原精确路径通过的情形不重分类。

对应成功 session/open 行的可选 peer_match 分别保留 raw peer、原 scope0、匹配 local input/nonzero scope 和 exact_equals=false，不把 peer 的 scope 改写为本地值。它只佐证这一 fixture 的管理字段表示，不能证明 wire zone、入站接口、独立客户端身份或作为 ACL；后续 SID、owned share/path、唯一 session、live opens、child/mapping/stage 全部仍须通过后才发 admit。来源 artifact/hash 绑定此方法与原始事实，原失败捕获和清理规则不变。

这一参考没有完整 packet/lease/break 轨迹，receipt 固定 mechanism_trace_complete=false。B 不经 fixture 的 SMB 预开或枚举只是调用纪律，不能证明自动流量中的 cold B；没有目标副作用的管理查询也不证明 connection topology。来源、变更、原生观察与清理均完整时，结果才具有 native_behavior_pass/failure 的资格；未知保留原始观察并按不完整处理，不归纳为内部 FCB、未请求 QFid 符合性或当前生产验收。

## 备选方案

**等整套新实现完成后才运行原生验收。** 最终仍要这样验证交付代码，但把可行性诊断也推迟到那时，会让一个已经知道可能失败的缓存行为在大量实现之后才决定能否交付。固定原型让这项依赖提前得到可复现的回答。

**通过修改全局缓存 lifetime 获得成功。** 这会把结果建立在机器级设置上，无法证明默认目标环境的非 TTL 可见性。诊断读取并比较设置，要求 lifetime 大于一秒，并验证观察发生在可能到期之前。

**每次 rescan 都重新取得 checkpoint。** 这会把已建立监听的后续请求当成新注册，跳过 rescan 响应交付后、下一次请求之前发生的事件。保留注册仅适用于同一健康 stream，来源更换仍须重新建立，不能无条件保留旧状态。

**继续支持 leasing 并修正协议文件名关联。** 这需要独立定义 lease 的协议名字与每个 open 的对象身份在外部改名/替换后的关系。NoLeasing 对照只检验不支持该协议能力时的行为，不通过放宽原身份检查来模拟修复；授予目录缓存权限则还需要另一份失效/撤销设计。

**用交叉编译或模拟 SMB 客户端代替原生执行。** 它们分别验证构建和协议处理，不执行 Windows 重定向器的名字缓存，不能回答这个问题。

## 后果

### 路径属性的一秒边界

两种独立策略都有合格的旧路径属性反例。运行环境均为 Windows 11 Enterprise build 26200 ARM64，FileNotFound/Directory/FileInfo cache lifetime 为默认 5/10/10 秒且未改变；每个样本都严格早于原始缓存到期。

| 被测策略与执行来源 | 首次 GetFileAttributesExW 相对 ACK 的时间 | 结果 |
|---|---|---|
| 原 capability 0x26、lease State NONE；[35111447579](https://github.com/codetreker/remote-fs/actions/runs/35111447579)，探针 `7f2168c32d4a3b89e7b395cbaa81f59838e20004` | exclusive 1.100140 秒；unheld 1.100189 秒 | 两项 stale_after_one_second / violated |
| NoLeasing capability 0x4、CREATE NONE/无 RqLs；[35114190139](https://github.com/codetreker/remote-fs/actions/runs/35114190139)，探针 `57ab51e17a9b25694e34940775fb70822f27201b` | exclusive 1.100531 秒；unheld 1.100566 秒 | 两项 stale_after_one_second / violated |

四个原生结果均仍为暖的 257 字节和 2001 年 mtime；各自 post-query HTTP oracle 确认同一 NodeID=2、769 字节、metadata revision=4 和预期内容。安静间隔无违规，轨迹完整，最终引用、连接、pending 与 cleanup failure 均为零。NoLeasing 的实际协商与无缓存授权断言已经执行，补丁及三个 canonical 输出 hash 与已审查代码一致；它没有因未安装补丁而退回基线。

这证明指定原型、补丁与设置下的这两种路径没有满足一秒要求，不是所有 SMB 实现或所有缓存失效方案都不可能满足的结论。当前端点与文件适配仍须有自己的验收，缓存处理的下一项决定保持未定，规格要求不因诊断失败而自动改变。

### 已打开读取与新打开身份

[原能力即时矩阵 35099917015](https://github.com/codetreker/remote-fs/actions/runs/35099917015)的八项同步/异步增长/缩短读取完整通过；BasicInfo/StandardInfo 的原生值已更新，但 class 34 观察器缺少解码，完整用例失败。即时路径旧值只是毫秒级首次观察，不单独证明持续超过一秒。replace_open 的原型 CREATE 返回 INVALID_PARAMETER，recreate_open 首次缺失返回 FILE_NOT_FOUND，但两种完整流程均未通过；不从这些局部结果推断缓存原因。

[NoLeasing 即时矩阵 35114190139](https://github.com/codetreker/remote-fs/actions/runs/35114190139)在两种根模式合计三十项中通过十六项：BasicInfo、StandardInfo、同步和异步读取各自的增长/缩短结果成立，完整矩阵仍失败。路径视图返回旧值；replace/recreate 的新打开进入原生身份检查后得到数值 3，authority 期望为替代对象的 4，原因尚未定位。

这份身份轨迹中的 QUERY_INFO class 7 是 EA 信息，不是 Position；新打开未出现可核对的 QFid，也没有 class 6/18 查询。因此不能据此归因于位置、lease 关联或 authority 把旧对象交给了新引用，更不能把数值不符改成通过。旧引用与新名字的对象区别仍须由完整请求及内容/身份证据验证。

### 负名字、监听与共享模式

[35074898091](https://github.com/codetreker/remote-fs/actions/runs/35074898091)的 baseline/held_parent 在一秒内分别查询 96/97 次，均无新权威 CREATE；notify_parent 两次查询取得一次新 CREATE 和匹配 ADDED，约 18 ms 可见。这只证明一次已接纳的根监听能推进该次创建。

[35088586249](https://github.com/codetreker/remote-fs/actions/runs/35088586249)的最终缺失状态对照将已核对的最终不存在返回为 0xc000000f：无父句柄和持有根 LIST/share=0 的两个名字分别约 7.55/7.49 ms 可见，各有新 CREATE，整个轨迹没有 CHANGE_NOTIFY。只读 RTL 的 0xf→2、0x34→2、0x3a→3 映射不能解释缓存行为；两个负名字成功也不能代替已缓存属性、内容或替换名字的验证。

[35080069968](https://github.com/codetreker/remote-fs/actions/runs/35080069968)中，未施加连续性 overlay 的 rescan 样本在真实 ENUM 后又进入 Pending，但一秒内 87 次查询没有新 CREATE。[35081515613](https://github.com/codetreker/remote-fs/actions/runs/35081515613)施加相同健康 generation 下保留注册的 overlay 后，notify_parent、nested、lifecycle、outage、rescan 五项通过；rescan 间隔约 133 ms 可见，随后创建约 35 ms，均有实际请求/通知并回到健康 Pending。监听恢复不能只以 Pending 判定，也不表示丢失的目录明细已经重建。

[35083279673](https://github.com/codetreker/remote-fs/actions/runs/35083279673)的十六项目录结果在已核对的 NTFS 与 SMB 子目录、root 上一致：LIST/share=6 与监听在两个顺序冲突，READ_ATTRIBUTES-only 成功；无监听基线清楚，普通文件读共享拒绝也成立。四个冲突的 SMB 第二次打开没有发出 CREATE，服务端不能处理尚未发送的请求。这个结论仅限被测 masks/顺序，不推出目录或 root 的通用豁免。

[FindFirstChangeNotification 对照 35087653528](https://github.com/codetreker/remote-fs/actions/runs/35087653528)中，NTFS root 两个顺序均冲突，SMB watcher-first 已有 Pending 而应用打开仍冲突；SMB application-first 的无监听基线失败，不能作为有效对照。替代通知入口尚未证明兼容，也没有由此取得可见性、重新监听或故障验收。

### 祖先通知的原生观察

[35411939518](https://github.com/codetreker/remote-fs/actions/runs/35411939518/job/105813131180)以探针 `45ba22aed7f338b298f0a7ae1543525b5f7abfec` 在 Windows 11 Enterprise 26200 ARM64 执行最初五项，整体失败。share0/watcher-first、share6/watcher-first、share6/app-first 分别在 ACK 后 25.185、26.501、23.653 ms 取得合格当前属性。share0/app-first 在 24.265 ms 返回新大小，但第二次 warm 完成与写入开始记录了相等时钟，原严格先后判据因此得到 Quiet=false；该项仍为 inconclusive，不能把新顺序判据追用于原结果并改称通过。share0 noWatcher 在 0.511 ms 返回旧的 257 字节，只作观察，不是超过一秒反例。

该次各项后置 oracle 与清理均完成，默认缓存 lifetime 仍为 5/10/10 秒，没有残留映射/进程。三项合格增长只证明各自配置，不等于全部顺序或生产方案通过。

[35414601479](https://github.com/codetreker/remote-fs/actions/runs/35414601479/job/105820628128)使用探针 `d7110afda15994310835e9a5a8354dec88f91df6` 的七项，四项增长在 ACK 后 21.842～23.660 ms 合格可见，share0/app-first 缩短在 22.086 ms 取得 73 字节；noWatcher 的 257 字节仍只作观察。替换收到合法 STATUS_NOTIFY_ENUM_DIR（0x10c）与零字节，detail-only helper 在首次打开前中止，因此没有新旧身份或字节首样本、没有后置双对象 oracle，整项及作业仍失败。cold_source 的 false 来自中止后未赋值的字段参与最终核对；已排空轨迹没有发现 B 名字暴露或目录枚举，不能据该 false 归因于 B 被预热。这只澄清缺失证据，不把原失败改为通过。默认 5/10/10 与清理检查保持，原始失败收据不变。

[typed-rescan 运行 35419230739](https://github.com/codetreker/remote-fs/actions/runs/35419230739/job/105833626982)以探针 `a6bdeeeba3bbe5bd7e49325d44051bdd6fe73fe7` 完成七项：四项增长和缩短通过，noWatcher 保留观察用旧值；替换是 EvidenceValid=true 的 replacement_identity_aliased，整项失败。真实 0x10c/原生零字节、同 watcher 新 Pending、首个 API 前的观察顺序、冷 B、HTTP oracle 和清理均合格。Hnew 有 769 字节并读 B，HA 保持 257 字节并读 A，但两者报告同一 native FileIndex=3、同一 volume；全部身份/字节检查在 ACK 后约 20.5 ms 完成。它是首个合格样本的身份不一致，不是超过一秒的 staleness 证明。

该轨迹中旧 CREATE 请求了 QFid，新 CREATE 只有 DH2Q，未出现 file QUERY_INFO 6/18/59；原观察器没有保存 QFid 数值响应。不能从请求存在或 authority NodeID 推导系统使用了哪个身份值。精确实验为此增加被动关联记录，既不补发查询，也不把原失败改成成功。

typed-rescan 的局部三个根普通/race 各 144 pass，detail-only 与 readiness 对照分别命中规定的失败。精确历史单项的 26 个本地根普通/race 各 285 个 verdict 通过，四个因果对照、十个源提取 PowerShell 清理选择用例、错误 pin 拒绝、精确准备、ARM64 构建和 actionlint 通过。CRLF 准备回归的九个 Go 输出与此前组合逐字节相同，ARM64 binary 也相同；沿用 Go 控制收据有明确字节依据。首次 precise 作业因准备失败未进入控制或原生场景，清理成功不能补出 native 结果。这些分别证明自己的 harness 来源、错误判据与拥有权，已有局部和增长/缩短结果不证明替换身份、当前生产适配或完整缓存方案。

[精确单项 35423839239](https://github.com/codetreker/remote-fs/actions/runs/35423839239)绑定探针 `6467000e8ab6c877f96762a9ff34557a288b6d40` 及实际 Go 1.26.8 Windows ARM64 binary，取得完整 REMOVED/OLD_NAME/NEW_NAME DETAIL；P=7/Q=9、前像、原 action 回执、HTTP barrier 与真实交付区间一致。冷 B、安静区间、原默认缓存和清理均合格，但结果为 replacement_identity_aliased：Hnew 读 B 的 769 字节，HA 保留 A 的 257 字节，两个 native FileIndex 均为 3、volume 均为 2206197897。所有身份/字节检查在 ACK 后 29.552 ms 完成，因此失败只约束这个首个合格样本，不证明超过一秒仍旧。

被动数据确认旧 CREATE 的 QFid DiskID=3，VolumeID=3872781749700257929；新 CREATE 未请求且未返回 QFid，实际 file 查询为 7/22/34，没有 6/18/59，也没有 QUERY_DIRECTORY。不能用原 QFid 或 backend NodeID 补造新打开的身份回复。完整 DETAIL 已成立仍出现相同 native identity，这缩小了被测条件，但 redirector 内部身份算法及纠正机制依然未知；固定原型结果不替当前生产接入或一秒缓存验收。

always-truthful QFid 的本地适用控制共 46 根，普通/race 各 380 个 verdict 通过；requested-only 响应回退与移除 alias 拒绝分别在因果断言失败，七项脚本策略控制、default/always 准备和精确 ARM64 构建也已核对。默认分支保持原 server 字节，策略改变处使用对应 no-leasing 控制。[原生运行 35426210106](https://github.com/codetreker/remote-fs/actions/runs/35426210106)绑定探针 `ad4d322bdf0eb5bb05a57786fea5a8b649fcf738`，实际控制用例 380 个 verdict 通过，单项仍为合格的 retained_identity_changed 失败。新 CREATE 未请求但接纳了真实 B QFid=4，Hnew 报 native FileIndex=4；HA 的 FileIndex 从初始 3 变成 4，HA 仍读 A 的 257 字节，Hnew 读 B 的 769 字节。完整 DETAIL、冷 B、原设置、HTTP oracle、来源与清理保持，全部身份/字节检查在 ACK 后约 29.63 ms 完成。

这项结果分别确立响应接纳、新身份观察和旧身份不稳定，不能将前两项当作整体成功，也不证明身份问题持续超过一秒。原 requested-only 与 typed-rescan 的失败均保持原样；这些原型观察没有证明共享 FCB 等内部实现，未请求 context 的符合性仍单独未决，当前生产接入也没有由此获得修复或缓存验收。

### 样本资格与执行来源

[35107398671](https://github.com/codetreker/remote-fs/actions/runs/35107398671)的两个原能力延迟样本虽在约 1.100 秒返回旧 tuple，仍是 inconclusive：安静间隔出现 lease-proof.bin 的 CREATE、QUERY_INFO class 48 与 CLOSE，且没有 post-query oracle。后台请求的具体发起者未确定；资源最终归零不能补回缺失证据。它不计入上述四个合格反例。

[35078136507](https://github.com/codetreker/remote-fs/actions/runs/35078136507)的 nested 用例也不能由子断言升级为整项通过：十个名字均及时可见，但 helper 在零字节丢明细后停止。该次 lifecycle 和 outage 通过，仍不消除 nested 的失败。成功判据覆盖监听拥有者的完整生命周期。

准备失败不提供协议效果。[35111447579](https://github.com/codetreker/remote-fs/actions/runs/35111447579)的 NoLeasing 作业因 CRLF/autocrlf 的补丁上下文不匹配而未进入测量；独立重现与 LF/CRLF 组合核对证明规范化后输出与已审查代码相同。[35114190139](https://github.com/codetreker/remote-fs/actions/runs/35114190139)实际完成了规范化准备并运行 NoLeasing，因而其失败是测量结果，不是沿用那个启动错误。

| overlay | 规范 LF SHA256 | 已核对的执行表示 |
|---|---|---|
| 通知连续性 | `f92edc59b55cc317fa0b5dcc1d1a55dd687b979e0e9a6bcb2112606ea4318e26` | Windows CRLF 为 `c18cc0d442de2a93cf8ce0a7a2e39817d033125c9af4b0d967d07099f428612d`，仅换行差异 |
| 最终缺失状态 | `a53e64d479138497e99f96ded82af01ec69be8434eac4cf0952f9c534dfc8d8a` | Windows CRLF 为 `263fd82b00308514e994de690351f5add58fe13bff3ddbf07c0853ea7c19ea1b`，仅换行差异 |
| NoLeasing | `62538e9c1eb5b2b12a4d6fed71937fdd675b378d253c8f1c6c9d875a84f06734` | 规范化后按该 hash 应用，三个目标源码的输入/输出另外逐一核对 |

每次结果绑定固定基底、overlay、探针 commit、variant、OS/build 和缓存设置；无 overlay 的样本保留各自来源。JSON 轨迹、Go verdict、映射前后状态和最终引用数作为产物保存十四天。轨迹溢出、编码失败、设置改变或映射残留使样本失败；取消 overlapped 通知须等真实完成后才释放结构和缓冲，意外清理错误不能当作正常取消。

POSIX 单项的实际准备证明非测试源码仅改变 Fs5 能力位，适用本地控制 41 根、普通/race 各 395 verdict，两个旧能力位负向对照、十二项脚本策略和三种 ARM64 组合构建通过。backend 控制证明自己的 retained A/B 行为，不是 Windows 结果。

[POSIX 原生单项 35428678823](https://github.com/codetreker/remote-fs/actions/runs/35428678823)绑定探针 `09d65eb77f74d4bda6dceab4f5a73e8f98496cf6`，实际 HA Fs5 raw/decoded flags=0x406，最大 component=255、名字不变，且响应全局序号 11 先于 mutation 12 和首次新打开 14。完整 DETAIL、冷 B、来源、oracle 与清理有效，395 个控制 verdict 通过，单项仍为合格 replacement_identity_aliased：初始 HA、Hnew、保留 HA 的 native FileIndex 均为 3，旧 A257 与新 B769 字节各自正确；requested-only 的新 CREATE 仍没有 QFid。全部身份/字节检查在 ACK 后 36.3051 ms 完成。能力曝光发生在 HA 创建之后、mutation 之前；结果仅约束这个已测顺序，不证明其它时点或内部 FCB 行为，也不是超过一秒的陈旧或所有无驱动 SMB 方案不可能的结论。

组合准备的本地控制 50 根、普通/race 各 452 verdict 通过，两个单刺激移除对照在原联合 exposure 断言失败；十九项策略控制、三项 source guard 和四种 ARM64 组合构建完成。三个旧分支的 254 份非测试 Go 输入及原生调用序列保持原样，新组合也不增加原生 API。[实际组合 35430629259](https://github.com/codetreker/remote-fs/actions/runs/35430629259)绑定探针 `31817a904fa42896b47ce5eb64e40cbe8be73e90` 和实际 binary，两项刺激均已真实送达同一 HA：Fs5=0x406 早于 mutation，首个新打开未请求却收到真实 B QFid=4。控制用例 473 个 verdict 通过，精确 DETAIL、冷 B、历史区间、oracle 和清理合格，但原生结果仍是 retained_identity_changed：Hnew 为 4，保留 HA 从初始 3 变成 4，而 A257/B769 字节保持正确。全部身份/字节检查在 ACK 后 44.3882 ms 完成。

这个合格首样本否定了该组合下的旧引用身份稳定性，不证明问题持续超过一秒，也不证明所有无驱动方案不可能。内部 FCB 算法与未请求 QFid 的符合性仍未决定，固定原型结果不代表当前生产能力或缓存验收；此前单刺激结果保持独立。

early-bootstrap 本地控制 64 根、普通/race 各 542 verdict，八项因果对照、二十六项策略控制和五种 ARM64 构建通过；四个旧分支字节保持。[原生单项 35437559379](https://github.com/codetreker/remote-fs/actions/runs/35437559379)绑定探针 `0a806421488815980d74c8d89e58fc9fef65a9bf`，真实 root Fs5=0x406 的全局序号 34 < helper 完成退出 36 < HA API 38，同一 connection/session/tree/token 与无目标预热均成立。542 个控制 verdict 通过，单项为合格 replacement_identity_aliased：初始 HA、新 Hnew、保留 HA 的 native FileIndex 均为 3，A257/B769 字节各自正确；requested-only 的新 CREATE 没有 QFid。精确 DETAIL、历史、oracle、原设置和清理有效，所有身份/字节检查在 ACK 后 37.1918 ms、最早 TTL 前完成。

该结果证明这次 Windows Nt 查询的终态成功、root 关闭和实际 exit 0，不覆盖 PENDING 或内核取消路径；身份失败也不证明持续超过一秒或内部 FCB 算法。既有失败保留，当前生产/缓存验收仍独立。

early+QFid 的本地适用控制 82 根、普通/race 各 635 verdict 通过；移除早期能力位拒绝和新 CREATE 未请求条件的两个隔离对照分别命中断言。三十三项脚本策略控制与六种 ARM64 配置构建通过，五个既有分支的准备结果及 254 份非测试 Go 输入保持不变。Linux 子进程控制和精确提取的 parser 已执行，Windows 组合胶合路径经源码核对与构建；新组合尚未原生运行，没有身份修复或生产接受结论。

inbox 参考的[生成器](../../../../.github/scripts/native-smb-inbox-reference-generate.go)按源码 hash 与符号提取七份输入中的 38 个既有 native helper 声明，原 legacy identity/data/time 判据不改写；独立 module 不引入 SMB authority、HTTP 或 metastore。original/write-only 两策略各九个可移植根，普通/race 各 170 verdict 通过，包含原 73 和新增 97 个 sharing verdict。六项默认 decoder 对照分别证明缺失/null 策略字段不能按合法零值接纳。十二项生成控制、双方逆向 AST 与两项 ARM64 构建已核对；share 参数、errno 归属和结果隔离的三个因果对照命中断言。七十五项 controller mock 及错误根 resolver 对照、十六项实际 Linux 模拟 child 与 guard 对照分别验证拥有权；它们和静态检查不证明 Windows 原生效果。[首次运行 35446155273](https://github.com/codetreker/remote-fs/actions/runs/35446155273)已建立本次 current-logon mapping 并到达 Ready1，随后在 administrative origin 核对处拒绝；没有进入 local Rename、Hnew 或新旧身份/字节判定。选中 session 与 open 两处复用同一判据，旧回执没有导出被拒绝的 raw peer 或具体调用点，因此不能从这次运行认定 IPv6、scope 或其它地址原因。child 经强制结束且已确认退出，精确 mapping/share/目录物理清理完成，原拒绝与 forced 失败仍保留；native behavior 尚未建立。

[捕获运行 35448201633](https://github.com/codetreker/remote-fs/actions/runs/35448201633)在 Ready1 的选中 session 行记录了无 zone 的原始 link-local string：peer scope0，现有本地输入唯一同字节行的 scope16，原精确 Equals=false。运行仍在 mutation/Hnew 前拒绝，没有身份 verdict；forced child 的实际退出与物理清理完成，原失败保留。这是该次管理字段表示导致的拒绝，不回填首次 35446155273 缺失的 peer，也不推断 wire zone。

唯一管理字段佐证的本地模拟控制共 107 项（zone 39、capture 22、origin 24、controller 22）通过；关闭新分支与丢弃 provenance 的两个对照命中断言，原 raw scope0/local scope16/Equals=false 不改写。该 zone 佐证变更保持 native helper、生成结果、binary 与进程拥有者，复用既有 Go 普通/race 和 ARM64 证据；新的 share2 对照另有生成观察变化，不沿用旧 binary 相等结论。[原 share0 运行 35449828229](https://github.com/codetreker/remote-fs/actions/runs/35449828229)已完成 Ready1 来源准入，source B 的 DELETE/share7 打开成功，实际 FileRenameInfoEx22/Flags3/RootDirectory NULL/绝对 target 返回 sharing-violation 文本；没有 ACK 或 Hnew，结果为 reference_mutation_refusal，没有 native identity verdict。该旧回执没有数值 rename errno 或阻塞 handle，不能补造它们；native/controller 清理完成不消除失败。新的 share2 对照尚未原生运行，所有旧来源拒绝、mutation 拒绝与身份失败保持独立。

代价是维护固定基底补丁、注入锚点、回归、十八个诊断作业及相互隔离的祖先/身份实验，输入不会自动跟随生产代码。诊断提供可行性或失败证据，不交付新的 SMB 文件实现、SQLite 持久性或历史时间显示策略。实际 package、transport 和 backend 仍须独立验收；缓存失效机制与平台接入仍由平台提案承接，既有一秒要求保持不变。
