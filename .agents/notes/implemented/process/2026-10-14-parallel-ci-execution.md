# Agent Note: 并发执行 CI 的独立检查

Status: implemented

## 问题

完整 CI 的关键路径包含串行依赖和重复测试准备。[main 的运行](https://github.com/codetreker/remote-fs/actions/runs/38417266698)中，SQLite 作业耗时 9.75 分钟，随后 checks 耗时 17.85 分钟，整次运行约 27.73 分钟；checks 内的其它包 race suite 与 HTTP race 步骤又分别耗时 9.95 和 6.12 分钟。[另一份 PR 运行](https://github.com/codetreker/remote-fs/actions/runs/38404695932)中，SQLite 为 9.62 分钟，checks 为 27.98 分钟，整次约 37.73 分钟。两个根包必须完整执行，但其它检查并不依赖它们的结果才能开始。

这些作业时长包含 runner、缓存、编译、测试和清理，不能把不同环境的局部用例时长相减来解释全部成本，也不能把消除依赖后理论上的关键路径当作已经实现的 CI 收益。需要缩短执行等待和不属于测试判据的准备，同时保留 [R-ERR-1／R-ERR-2、R-INT-3 和 R-WIN-6](../../../../docs/spec/requirements.md)的完整验证。

## 决定

[CI workflow](../../../../.github/workflows/ci.yml)让 `checks-work`、`sqlite-race` 和 `http-race` 独立开始。`checks-work` 保留 build、Console、vet、格式、链接、串行全负载验收和其余非挂载 race；两个根包各自在独立 runner 完整运行。挂载 race、SMB／FUSE 联合验收、全模块 package-local coverage 和 Windows 测试继续由原作业执行。

最终 `checks` 只汇总三项结果，保留原必需状态的显示名称。它依赖全部三项，使用 `always()`，且只有三项都明确为 `success` 时才能成功；failure、cancelled 或 skipped 均失败。作业上限分别为 checks-work 三十五分钟、两个根包各二十分钟、门禁五分钟。每个 race 二进制的十分钟 watchdog、strict verdict、`-count=1`、包集合和 coverage 归属不变。

`checks-work` 保留 `checks` 缓存身份，避免改名丢失已有快照。[共享 setup action](../../../../.github/actions/setup-go/action.yml)允许指定缓存身份和额外恢复身份；HTTP 按自身前缀、相容的 checks 前缀、经校验的旧 archive 顺序恢复，并以自己的身份保存。缓存格式、目录顺序、平台／工具链／依赖相容约束由[缓存决定](2026-09-09-refresh-ci-go-build-caches.md)持有。

测试准备的改动限于 SMB 的双会话 share matrix、SQLite 的十四项 live scalar corruption 和 limited 的 256-byte frontier 超限用例，具体隔离、恢复及边界规则由[测试工作决定](../testing/2026-09-09-reduce-test-work.md)持有。它们不改变生产行为、测试策略、144 个共享向量或任何原有 leaf 判据。

本决定部分取代 [SQLite 调度决定](2026-09-29-isolate-sqlite-race-ci.md)的串行依赖和 [HTTP 调度决定](2026-10-12-isolate-http-race-ci.md)的同作业后续步骤。两者关于独立完整执行与 watchdog 的理由继续成立；原有测量和当时备选保留。

## 备选方案

**只调整 CI 依赖。** 可以消除 SQLite 在 checks 前面的等待，并让 HTTP 与其它包并发，但仍为相同 incumbent、健康 scalar 数据库和无关目录反复付出准备成本。三项用例的重复工作可以在保留各自判据的条件下去除，合并它们服务于同一个执行耗时目标。

**扩大到全套夹具与覆盖率合并。** 可以减少更多 schema 和重复集成执行，却会改变独立 case 的 ownership、隔离与 package-local coverage 证据，难以用这三处的匹配数据解释收益。其它 fixture 优化仍可按各自判据单独比较；实现不得引入跨顶层测试的可变共享状态，也不得把其它包的执行计入生产覆盖率。

**保留顺序执行、提高上限或缩小测试集合。** 前者继续承担已有的串行关键路径；后两者分别推迟真实挂起的终止和丢失原测试命题。已有的执行隔离与现行预算足以作为不改变判据的调度条件。

## 后果

SQLite、HTTP 与其它 checks 工作可以重叠执行，现有必需状态覆盖三者。代价是新增一份 HTTP runner 和独立编译快照，以及最终门禁的调度时间；并发缩短关键路径不会自动减少全部 runner 分钟数。完整 CI 净收益仍以同一提交的真实运行为准。

门禁脚本的 success／failure／cancelled／skipped 三项笛卡尔积共 64 组验证中，只有三项全 success 成功。局部用例的匹配计时、原 verdict 集合和整包覆盖率分别由[测试工作决定](../testing/2026-09-09-reduce-test-work.md)记录。它们提供测试准备改动的证据，不能替代新提交的完整 CI 结果。

其它夹具、coverage 合并、生产路径与测试门禁均不属于此改动。延后其它准备优化只需另作各自的匹配比较；取消跨测试可变状态和保持 package-local attribution 的约束，才会使这种延后变成隔离或覆盖率重构。
