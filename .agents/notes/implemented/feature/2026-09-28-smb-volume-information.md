# Agent Note: SMB volume 信息

Status: implemented

## 问题

Windows 程序通过已有 FileId 查询 volume 身份、创建时间与容量。文件对象的名字可以改变，容量也会随写入、释放及配置变化；这些查询不能依赖旧路径、打开时的容量或可修改的根节点时间。标准 SMB 容量结构不能直接报告实测 Used，未知事实和超额用量必须在相应查询中明确失败。

## 决定

SMB 提供五种 `InfoType=0x02` QUERY_INFO，复用[文件信息](2026-09-28-smb-file-information.md)的原引用准入、StateScalars、身份与 allocation unit，以及已有 `Storage.Space`。[目录枚举](2026-09-28-smb-directory-enumeration.md)独立拥有目录捕获与分页。本决定服务 R-WIN-2、R-WS-5、R-ERR-1／2、R-SEC-5 和 R-INT-3；结构与验证分别由[SMB 端点设计](../../../../docs/design/client/smb-endpoint.md)、[文件契约](../../../../docs/design/server/file-handles.md)和[测试策略](../../../../docs/testing.md)拥有。

### 五类查询与事实来源

| 类 | 完整结果与事实来源 |
|---|---|
| `FileFsVolumeInformation` `0x01` | 18 字节 prefix 与 UTF-16 label；持久创建时间、可信 label 与原 volume serial 的低 32 位 |
| `FileFsSizeInformation` `0x03` | 24 字节；本次 Space 的 Total、caller Avail 与逻辑分配单位 |
| `FileFsDeviceInformation` `0x04` | 8 字节；实际 disk／remote endpoint 特征 |
| `FileFsAttributeInformation` `0x05` | 12 字节 prefix 与 UTF-16 filesystem name；仅报告已实现的名字能力 |
| `FileFsFullSizeInformation` `0x07` | 32 字节；本次 Total、caller Avail、actual free=`Total−Used` 与同一逻辑单位 |

五类均要求活 FileId 和当前业务授权，不要求公开 ReadMetadata 或字节读权。StateScalars 本次核对原 File／NodeReference 的对象、kind、scope 与生命期；固定身份或缓存事实不能代替这一步。查询使用既有逐引用 FIFO，CLOSE 仍按原 fence／drain 顺序；volume 查询不建立修改 action 或 owner。

### 静态事实与当前容量

`Share.VolumeLabel` 是可信宿主的可选 `*string`。nil 表示没有来源，已知空字符串表示无 label；Export 独立复制该值。UTF-8、NUL 与 32 个 UTF-16 code unit 的上限在发布准入检查。

SQLite schema v11 用 `volumes.created_sec`／`created_nsec` 保存真正的 volume 创建时点，新 volume 在原创建事务中同时写入；历史行保持两字段 NULL。两字段必须同时缺席，或为合法 INTEGER 秒与 `0<=nsec<1e9` 的纳秒；混合 presence、类型或范围错误明确失败。此事实随原提交和持久见证确认，重开不改变，根节点 BirthTime 的显式修改也不改变它。

`storage.VolumeCreatedAt(ctx, source)` 结构性发现可选 getter；缺席或历史未知为明确不支持，来源错误保留原错误链与分类；nil error 区分已知 year-1 值和未知事实。既有 BackendIdentity／FileSessionIdentity 的 descriptor 与方法集合保持不变。HTTP 在已经授权的 session enrollment 中封存创建时间或有界实际错误，复用现有 capability fact envelope；没有独立创建时间 RPC。包装链按自己的 admission、health 与 context 转送事实，缓存 getter 不证明当前引用健康。

32-bit serial 使用 `uint32(existing64serial)`。协议允许该字段碰撞；原 FileId 的 64-bit serial reservation 保留，不增加第二套唯一性 registry。Device 仅报告 FILE_DEVICE_DISK 与 REMOTE_DEVICE；filesystem name 为 `remote-fs`，Attribute 仅报告 CASE_PRESERVED_NAMES 与 UNICODE_ON_DISK，以及既有 255 code-unit component 上限。

容量查询每次在当前授权下调用原 authority Space。逻辑 `BytesPerSector=AllocationUnitBytes`、`SectorsPerAllocationUnit=1`，单位必须非零且可由 uint32 表示。这是同一 volume 的逻辑额度单位，不宣称 host 的物理 sector，也不引入 geometry 接口或新账本。

Space 先验证 Coherent，Total／Avail 必须可被逻辑单位整除，FullSize 还验证 `Total−Used`；Size 和 FullSize 均要求 `Used<=Total`。caller Avail 与 actual free 分别报告，不能把 `Total−Avail` 当作 Used。无单位、Space=ENOSYS、事实损坏、超额、溢出或不能精确表达时，相应容量类返回 I/O 错误；权限拒绝保持权限分类，底层错误仍可由程序查询。失败不撤销 tree／FileId，READ、EOF 缩短和 CLOSE 继续按原权限、共享与配额规则执行。

### 结果、预算与错误

Volume 的请求最小输出长度为 24，实际 serialized prefix 为 18；不插入六字节 padding。Attribute 的 prefix 与请求最小长度为 12。固定 Size／Device／FullSize 的完整长度分别为 24／8／32，短于最小值在 authority 之前失败。

变长 label/name 不足时，QUERY_INFO 使用正常结果 body 携带 BUFFER_OVERFLOW warning，长度字段仍声明完整 UTF-16 字节数，后部复制实际可容纳的原始字节 prefix，包括奇数字节；不另加 code point 或 surrogate 截断规则。该 warning 不使同一 related compound 的后继 CLOSE 被视作前驱失败。Volume 最大 payload 为 82 字节，完整对齐 frame 预算为 160；所有 backing 结果和响应容量在工作前预留，拒绝新查询不占清理专用额度。

五类使用明确 allowlist。已定义而未实现的 filesystem class 为 NOT_SUPPORTED，未知 class 为 INVALID_INFO_CLASS；畸形固定体和 extent 按协议失败。创建时间或 label 缺席只使 Volume 类失败，容量来源失败只影响 Size／FullSize；Device／Attribute 和原文件类按其自身能力工作。不用零时间、旧容量或默认字段拼出成功结果。

### 范围切分

| 延后能力 | 延后代价与保持的结构 | 请求结果 |
|---|---|---|
| 泛用物理 sector／geometry 能力 | 按真实消费者增加 capability；allocation 字节与 Space 计量不变，逻辑 tuple 不冒充物理事实 | 本决定只报告已验证的逻辑容量单位 |
| volume label setter、security／quota 和 FS control | 独立增加授权与修改语义；查询没有发布效果 | 相应请求明确未支持 |
| [关闭删除](../../implemented/feature/2026-09-28-smb-close-delete-obligation.md)及[名字修改](2026-09-28-smb-guarded-name-mutation.md) | 独立增加持久义务及 ancestry guards；原引用不转成旧路径 | 相应文件／目录命令按各自交付范围处理 |
| [范围锁](../../proposed/feature/2026-09-28-smb-range-lock-cancel.md)与[通知／缓存](../../proposed/feature/2026-09-28-smb-change-notify-cache-coherence.md) | 增加跨入口保护、失效协议及原生证明；本次查询不宣称私有缓存一致 | 相应能力独立交付 |

## 备选方案

**从根节点 BirthTime 得到 volume 创建时间，或为历史 volume 用当前时间补齐。** 根时间可由属性操作修改，历史行没有真实创建证据。独立 nullable volume 字段保留真实时点，未知只使依赖它的类失败。

**建立 VolumePresentation／geometry 接口、独立 RPC 和单位账本。** 现有身份、allocation unit 与 Space 已分别拥有所需事实；另一套接口会重复 checker、授权和转送。可选创建时间复用结构性 getter 与原 enrollment channel，容量每次走已有 Space。

**在 SMB 层统一写死 4096，或把容量舍入成可表示值。** 第三方分配单位和调用方 Avail 可以不同；舍入会伪造额度。可信单位构成明确的逻辑 tuple，无法精确编码的类失败。

**为 32-bit serial 再建立唯一性登记。** 该字段按协议不承担唯一性保证；对象区分继续由原 64-bit volume serial 与不可复用 NodeID 组合完成。

**只交付身份类，容量类全部延后。** 无法满足 Windows 程序的容量查询目的。五类独立验证各自来源，容量不可表达仍保留读取与释放能力。

## 后果

文件、容量和配额使用同一 authority 的事实；容量的局部失败不扩大为整个 tree 的失败。创建时间的持久字段与严格 enrollment envelope 增加了迁移、类型验证和错误预算成本；历史缺席保持未知，不能为了让 Volume 查询成功而生成新历史。

逻辑单位可能使第三方或物理余量收紧后的 Space 无法被标准容量类精确表达；相应查询失败是明确的代价。查询协议、授权、局部故障和真实 READ／缩短／关闭的证明由[测试策略](../../../../docs/testing.md)拥有，真实 Windows redirector／WNet 与缓存资格仍由各自独立验收确定。
