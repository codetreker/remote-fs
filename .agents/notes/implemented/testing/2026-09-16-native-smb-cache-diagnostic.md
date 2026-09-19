# Agent Note: 原生 SMB 缓存诊断固定执行对象

Status: implemented

## 问题

Windows 原生重定向器可以在远端创建已经完成后继续复用此前的名字不存在结果。单个 SMB 请求正确、暖文件读取新内容或没有授予数据缓存权限，都不能证明后续按名访问满足一秒、非 TTL 的可见性。单次监听收到通知，还不足以证明重新监听的间隔、深层名字、卸载与故障期间的行为。

[平台客户端提案](../../proposed/architecture/2026-09-16-platform-client-capabilities.md)把这项可行性放在广泛实现之前。诊断需要一个能够实际运行、版本确定的 SMB 接入对象；负查找通过还不能说明已经缓存的属性、长度、内容或名字替换会及时更新。正在变化的通用接口不能成为每次重现时额外变化的因素。旧原型的成功也不能被当作新实现的验收。

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

祖先通知实验由[独立工作流](../../../../.github/workflows/native-smb-parent-invalidation.yml)和[脚本](../../../../.github/scripts/native-smb-parent-invalidation.ps1)执行，在同一固定原型上施加三份已核对的 overlay。它只监听自身三个文件的 Pull Request opened/synchronize 变更，单作业执行五个新 share/connection，不改变原十八个诊断作业。真实 share 根上保留递归 watcher，应用在真实子目录 `v` 上以 LIST 打开；四项覆盖 ShareAccess=0/6 与双方打开顺序，另有 share0 无 watcher 对照。父、子和目标的同一连接/session、mask、Pending 及匹配 MODIFY 都须有实际证据。这个几何实验回答不同目录上的监听能否共存并使属性及时更新，尚不定义生产私有父目录或映射 API。

[祖先探针](../../../../.github/scripts/native-smb-parent-invalidation_test.go.txt)把 257 字节旧属性暖两次，再由真实 HTTP 保留引用增长至 769 字节；首次 GetFileAttributesExW 必须在 ACK 后一秒内完成，且早于原始最早到期。计时包含等待通知。native-call ledger 禁止写入开始后 harness 的额外目标/祖先访问或提前 oracle；它不禁止 redirector 因通知自动刷新。匹配通知之后、新 tuple 的目标响应可以早于 ACK 或首次 API，分别标记 `refreshed_before_first_api`、`refreshed_by_first_api`，关联不完整则不能证明机制。原始首值、后置 oracle、轨迹完整性和最终清理共同决定资格。watched 旧首值使候选失败，但不被描述成超过一秒的反例；无监听的合格旧/新首值只作观察，也不外推 share6 的因果关系。

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

祖先实验目前完成 PowerShell 解析、精确源码准备、拒绝改动基底的负向对照及 actionlint；三个解析/关联/资格测试根经限定 Windows 常量和 Filetime shim 提取后，普通/race 各 39 pass，两个完整 Windows ARM64 测试 binary 构建通过。原生五项尚未执行。这些检查确认 harness 的来源和判据，不能证明重定向器失效效果，也不能代替完整名字、身份、故障或生产映射验收。

代价是维护固定基底补丁、注入锚点、回归、十八个诊断作业及独立祖先实验，输入不会自动跟随生产代码。诊断提供可行性或失败证据，不交付新的 SMB 文件实现、SQLite 持久性或历史时间显示策略。实际 package、transport 和 backend 仍须独立验收；缓存失效机制与平台接入仍由平台提案承接，既有一秒要求保持不变。
