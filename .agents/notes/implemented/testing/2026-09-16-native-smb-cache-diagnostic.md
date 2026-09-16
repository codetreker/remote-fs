# Agent Note: 原生 SMB 负缓存诊断固定执行对象

Status: implemented

## 问题

Windows 原生重定向器可以在远端创建已经完成后继续复用此前的名字不存在结果。单个 SMB 请求正确、暖文件读取新内容或没有授予数据缓存权限，都不能证明后续按名访问满足一秒、非 TTL 的可见性。单次监听收到通知，还不足以证明重新监听的间隔、深层名字、卸载与故障期间的行为。

[平台客户端提案](../../proposed/architecture/2026-09-16-platform-client-capabilities.md)把这项可行性放在广泛实现之前。诊断需要一个能够实际运行、版本确定的 SMB 接入对象；正在变化的通用接口不能成为每次重现时额外变化的因素。旧原型的成功也不能被当作新实现的验收。

## 决定

[诊断工作流](../../../../.github/workflows/native-smb-gate.yml)固定以 [1cb9ad7f49d998de4daa4d562d766b18cf06ce16 的 SMB 原型](https://github.com/codetreker/remote-fs/tree/1cb9ad7f49d998de4daa4d562d766b18cf06ce16/packages/smb/windows)为基底，在 `.tmp/native-cache-gate/fixture` 检出后，检查并应用[通知连续性补丁](../../../../.github/scripts/native-smb-notify-continuity.patch)，再按精确源码锚点注入聚焦测试及有界追踪。执行对象由基底、overlay 和探针共同确定，每次记录补丁 SHA256，不能仅凭 HEAD 仍指向 pin 就称树内容完全相同。补丁只进入临时诊断检出，不进入当前生产包或改变通用文件、身份及生命周期契约。

平台 overlay 在已建立目录监听的底层 stream 仍健康、generation 未变时，保留 rescan 交付后的注册与有界事件队列；重新请求不再用新 checkpoint 丢弃交付后的间隔。底层来源更换或失败仍使旧注册失效。两项[回归用例](../../../../.github/scripts/native-smb-notify-continuity_test.go.txt)分别检查间隔事件保留与来源替换隔离，和基底现有 Notification 测试一起先于原生场景执行。

工作流由其 workflow、运行脚本、原生探针、连续性/最终缺失状态补丁及对应回归用例的 Pull Request 变更或手动触发，以十个独立的 Windows 11 24H2+ ARM64 作业运行。`baseline`、`held_parent`、`notify_parent` 分别比较没有显式父目录句柄、保留父目录句柄及已有 CHANGE_NOTIFY 的情形。`owned_nested`、`owned_lifecycle`、`owned_outage` 使用测试夹具拥有的递归 UNC 监听，检查多层目录、100 ms 重新监听间隔与八名字突发、busy/正常卸载及 HTTP/SSE 故障。owned_rescan 仅把 fixture 的 MaxNotifyEvents 缩小为 2，在实际请求对应的 wire ENUM 响应后暂停，再在重新监听前建立一个新的负查找与远端创建。它继续要求一秒、缓存到期前的新权威查询和后续健康监听，不用本地零字节代替 wire 证据。directory_sharing 以实际 NTFS、可写父目录下的子目录对比 SMB 子目录，另行记录本地 volume 根与导出 share 根；它比较 LIST/READ_ATTRIBUTES-only、双方打开顺序和普通文件读共享拒绝对照，并先排除已有的共享冲突。root 与非 root 分别测量，目录是否受某种 share mask 约束须由真实结果回答。find_notification 进一步比较 FindFirstChangeNotification 与 LIST/share=6 根目录打开的双方顺序，并要求成功的 SMB 通知句柄有新 Pending；它只验证共存性，不包含可见性、重新监听或故障验收。该用例还只读记录三个 NTSTATUS 的系统 Win32 映射，不改线上状态码，不据此推断缓存策略。missing_final_status 是另一个独立对照：只在该 variant 施加[最终缺失状态补丁](../../../../.github/scripts/native-smb-missing-status.patch)，先无监听/父目录句柄运行，再在第一阶段通过后持有根 LIST/share=0 重复创建可见性；任意 CHANGE_NOTIFY 都使该对照失败。这些测试拥有者不构成生产 ManagedShare API 的选择。

缺失状态补丁的标记仅在父目录检查成功后产生，最终 CREATE 且清理成功才选择 0xc000000f；中间组件、权限、普通 ENOENT 和未知错误没有因此改类。它有独立的 SHA256 和两项[回归](../../../../.github/scripts/native-smb-missing-status_test.go.txt)，不与连续性补丁或只读 RTL 观察混称。该诊断检验一个真实最终缺失结果的另一种平台表示，不给错误未知的情况增加默认值。

固定 Go 版本为 1.26.8，测试三分钟、作业十五分钟；编译缓存、模块缓存和临时状态放在工作区 `.tmp` 下。创建的成功判据同时约束 authority 结果、时间与新请求：初次负查找有匹配的 NAME_NOT_FOUND，远端创建确认后的一秒内出现新权威 CREATE 与可见文件，且观察早于最早可能的缓存到期。通知与故障情形各自核对匹配事件或不可用错误，零字节成功或 ERROR_NOTIFY_ENUM_DIR 明确表示丢失明细，测试拥有者记录后重新监听；它不维护目录快照，不能把重挂监听说成枚举恢复。owned_nested 的初次/普通间隔通知仍必需，只有突发明确丢明细时允许不具备每个名字的 ADDED，所有可见性检查保留。三个系统缓存 lifetime 必须大于一秒且诊断不得修改设置。完整可执行规则由[测试策略](../../../../docs/testing.md#windows-原生-smb-负缓存诊断)拥有。

## 备选方案

**等整套新实现完成后才运行原生验收。** 最终仍要这样验证交付代码，但把可行性诊断也推迟到那时，会让一个已经知道可能失败的缓存行为在大量实现之后才决定能否交付。固定原型让这项依赖提前得到可复现的回答。

**通过修改全局缓存 lifetime 获得成功。** 这会把结果建立在机器级设置上，无法证明默认目标环境的非 TTL 可见性。诊断读取并比较设置，要求 lifetime 大于一秒，并验证观察发生在可能到期之前。

**每次 rescan 都重新取得 checkpoint。** 这会把已建立监听的后续请求当成新注册，跳过 rescan 响应交付后、下一次请求之前发生的事件。保留注册仅适用于同一健康 stream，来源更换仍须重新建立，不能无条件保留旧状态。

**用交叉编译或模拟 SMB 客户端代替原生执行。** 它们分别验证构建和协议处理，不执行 Windows 重定向器的名字缓存，不能回答这个问题。

## 后果

[原生运行 35074898091](https://github.com/codetreker/remote-fs/actions/runs/35074898091)在 Windows 11 Enterprise build 26200 ARM64 上执行前三个 variant。原型为上述固定 commit，runner 记录的探针 SHA 为 `48003488f0a88e66c9556fd795bea92e940a63be`；FileNotFound/Directory/FileInfo cache lifetime 分别为 5/10/10 秒，运行后保持不变。

| variant | 实际结果 |
|---|---|
| baseline | 一秒窗口内 96 次查询、零次新的权威 CREATE，未见创建，失败 |
| held_parent | 一秒窗口内 97 次查询、零次新的权威 CREATE，未见创建，失败 |
| notify_parent | 两次查询、一次新的权威 CREATE，收到匹配 ADDED；约 18 ms 后可见，早于五秒缓存期限，通过 |

三份产物的最终映射均为空，backend 引用、pending request 与 lease slot/bytes 均为零。这个结果只证明固定原型中一次已接纳的根目录监听能使该次创建及时可见。

[原生运行 35078136507](https://github.com/codetreker/remote-fs/actions/runs/35078136507)执行包含三个 owned variant 的诊断，runner 记录的探针 SHA 为 `aaf7e8d0a282fb9cf82e14f23315e7069c7def1d`，原型固定点不变：

| variant | 实际结果及限度 |
|---|---|
| owned_lifecycle | 通过。应用句柄使卸载返回 busy，随后创建约 20 ms 可见且有新权威查询；应用关闭后，仅内部 UNC 监听存在时普通卸载成功 |
| owned_outage | 通过。监听报告 I/O 故障，负缓存到期前对缺失名字返回 I/O 错误，未报告不存在 |
| owned_nested | 整项失败。十个名字的可见性均通过：首个约 34 ms、间隔中的名字约 132 ms、八个突发约 39–191 ms，且各有新权威查询；监听 helper 在零字节丢明细后停止，不能把这些通过的子断言写成整项通过 |

这次运行的缓存策略仍为 5/10/10 秒且未改变，最终 backend 引用、pending request 与 lease slot/bytes 归零。它暴露的丢明细处理是 owned_rescan 要验证的独立窗口：发生过 ENUM 之后，新负查找与创建能否跨越重新监听的间隔。[原生运行 35080069968](https://github.com/codetreker/remote-fs/actions/runs/35080069968)对这个新窗口给出了失败证据：runner 的探针 SHA 为 `2796afb08b536e85e2781592e8702bb754b2543a`，尚未施加通知连续性 overlay。owned_rescan 已关联实际 ENUM 响应 MID17，在其后完成新名字的权威负查找与远端创建，100 ms 暂停后又收到 Pending MID19；一秒内 87 次查询仍没有新的权威 CREATE，整项失败。重新进入 Pending 因而不能单独证明间隔没有丢失。

同次运行的 owned_nested 已完成十项可见性与丢明细后的继续监听，整项通过；owned_lifecycle、owned_outage 和 notify_parent 通过，baseline 与 held_parent 仍失败。七个作业的缓存策略不变、最终映射为空，backend 引用与 cleanup failure 均为零。这些是未施加连续性 overlay 的结果，不能从局部回归或构建成功推导加补丁后的行为。

[原生运行 35081515613](https://github.com/codetreker/remote-fs/actions/runs/35081515613)验证连续性 overlay：notify_parent、owned_nested、owned_lifecycle、owned_outage、owned_rescan 五项通过，baseline 与 held_parent 仍失败。rescan 间隔中的文件在 ACK 后约 133 ms 可见，带新权威 CREATE 和匹配 ADDED；后一轮创建约 35 ms 可见，监听回到健康 Pending。七个作业的两项新回归均通过、没有 skip；缓存策略仍为 5/10/10 秒且未改变，最终映射、backend 引用与 cleanup failure 均为空或零。

该次探针 SHA 为 `98073635bcb4bc5d4a83788ed77d2189751c429b`，Windows 检出补丁的 SHA256 为 `c18cc0d442de2a93cf8ce0a7a2e39817d033125c9af4b0d967d07099f428612d`。它与审查的 LF 补丁 `f92edc59b55cc317fa0b5dcc1d1a55dd687b979e0e9a6bcb2112606ea4318e26` 经 LF→CRLF 转换后的字节完全一致，差异不被误认为另一份逻辑修复。该证据只属于固定基底加指定 overlay；实际交付的 SMB/backend 仍须独立验收，directory_sharing 的共享兼容性仍须用自己的对照解释，通知通过不能代替这项证据。

[原生运行 35083279673](https://github.com/codetreker/remote-fs/actions/runs/35083279673)的探针 SHA 为 `8a85133bfc71a3e45aae73b2a1fa1a22bf77fa05`。directory_sharing 的 16 项目录结果与 NTFS 对照一致：所测 LIST/share=6 在两个打开顺序下都与监听句柄冲突，READ_ATTRIBUTES-only/share=6 在两个顺序下都成功；这次测得的两个 root 与各自子目录均如此。16 个无监听基线没有预先冲突，两个普通文件的读共享拒绝对照也成立。

四个冲突的 SMB 目录第二次打开均没有发出新的 CREATE，原生 API 已在重定向器侧返回共享冲突；只修改第二次打开的服务端检查，不能改变这组在发送前已被拒绝的请求。这个结论限于已测的访问/share masks 和打开顺序，不是关于所有目录或 root 的概括。八个作业的缓存策略保持不变、映射和 backend 引用清理完成，通知回归均通过且没有 skip。[原生运行 35087653528](https://github.com/codetreker/remote-fs/actions/runs/35087653528)对 FindFirstChangeNotification 的共存性返回失败，探针 SHA 为 `70beddac497daf1962df36a7827d99a0174d3d44`。NTFS root 两个顺序均冲突；SMB watcher-first 已有 Pending，后续应用打开仍冲突；SMB application-first 的无监听基线失败，因而该行不能作为其后打开顺序的有效对照。没有由此得到可见性、重新监听或故障验收。

该次只读 RTL 实测为 0xc000000f→2、0xc0000034→2、0xc000003a→3，wire status 未改变；返回同一 Win32 错误不证明缓存行为相同。missing_final_status 单独改变已核对的最终缺失状态，其结果不能从这次只读映射推导。

[原生运行 35088586249](https://github.com/codetreker/remote-fs/actions/runs/35088586249)随后验证了这个独立对照，探针 SHA 为 `6e7a0cf098b04c369754bf3ee81e8da20ceea097`。第一阶段没有监听或显式父目录句柄，wire 最终缺失为 0xc000000f、Win32 为 FILE_NOT_FOUND，远端创建在 ACK 后约 7.55 ms 可见；第二阶段持有根 LIST/share=0，另一个名字约 7.49 ms 可见。两次各有新权威 CREATE，并早于五秒缓存到期；整个轨迹没有 CHANGE_NOTIFY。

该次最终缺失 overlay 的 Windows SHA256 为 `263fd82b00308514e994de690351f5add58fe13bff3ddbf07c0853ea7c19ea1b`，与 LF 补丁 `a53e64d479138497e99f96ded82af01ec69be8434eac4cf0952f9c534dfc8d8a` 的 LF→CRLF 转换一致，连续性 overlay 仍单独记录。通知与最终缺失的专用回归均通过且无 skip；缓存策略未变，映射为空，backend 引用与 cleanup failure 为零。这个结果只证明指定基底/补丁下的两项负名字与独占根对照，不代替实际 Windows 集成、其它缓存类型、内部名字观察或当前绑定 API 的验收。

每次诊断的结果绑定基底 commit、各 overlay SHA256、探针 commit、variant、OS/build 和缓存设置；没有 overlay 的历史运行分别保留其原始执行来源。JSON 轨迹、Go verdict、映射前后状态及最终引用数作为产物保留十四天；轨迹溢出、编码失败、改变缓存策略或映射残留都会使结果失败。取消 overlapped 通知时保留其结构与缓冲直到完成，除明确的丢明细结果外，意外的异步完成/事件关闭错误仍报告失败，进程卡住由测试超时显式暴露。

代价是维护固定基底的补丁、注入锚点、回归用例与十个原生作业，诊断输入不自动跟随生产代码变化。此入口交付可行性或失败的证据，不证明新实现通过、SQLite 持久性、所有 Windows 操作或历史时间显示策略。实际交付的 package、transport 和 backend 仍须接受自己的原生验收，平台能力提案的状态不因诊断入口存在而改变。
