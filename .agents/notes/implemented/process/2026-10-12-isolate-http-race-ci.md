# Agent Note: 在 checks 中单独执行 HTTP race suite

Status: implemented

## 问题

根 `packages/transport/httprest` 的整包 race 接近十分钟累计 watchdog。[已通过的源码运行](https://github.com/codetreker/remote-fs/actions/runs/38211857576)耗时 576.370 秒；相同 Git tree 合并到 main 后，[c08f521 的 checks](https://github.com/codetreker/remote-fs/actions/runs/38217401962/job/114707669908)在 600.040 秒触发累计超时，正在运行的子项刚开始不足一秒。[98a6361 的 checks](https://github.com/codetreker/remote-fs/actions/runs/38240848717/job/114786194058)同样在 600.039 秒中止 HTTP 二进制；`TestAStreamSaysHowFarTheLogHadGot` 当时才开始约 0.35 秒，栈仍在 SQLite fixture 初始化的 migration 路径推进。

两份失败记录没有显示单项测试持续挂起。452 项可无歧义对应的 HTTP 顶层测试，其报告时长之和从 main 的 500.95 秒增至 511.87 秒，增量分散在不同用例。HTTP 及其 storage／metastore 依赖源码未变；SMB 二进制在 HTTP 启动前已经结束，不能将这个 HTTP 超时归因于目录协议处理。

Go 1.26.8 下的一次独立 HTTP 调度实验使用原 strict script、`-race -count=1 -timeout 10m` 和完整包集合，耗时 506.253 秒并通过。489 个顶层组及 1480 个子测试共 1969 个 verdict 全部通过，没有 skip 或缺失 verdict。这个结果支持减少同作业 Go 包竞争；本地与 hosted runner 的耗时不能作为受控性能比较，也不保证每种 runner 负载下都能在上限内完成。

## 决定

[CI workflow](../../../../.github/workflows/ci.yml)在现有 `checks` 作业中，先运行其它非挂载包的并行 race suite，再用独立步骤执行完整根 HTTP 包。前一步的 `go list` 排除集合包括挂载包、根 `./packages/metastore/sqlite` 和根 `./packages/transport/httprest`；后一项只改变根包的调度，不排除其它 transport 包。HTTP 步骤使用：

```sh
.github/scripts/assert-every-test-ran.sh -race -count=1 -timeout 10m ./packages/transport/httprest
```

HTTP 步骤在前序 suite 成功后执行。它与其余 checks 步骤、[独立 SQLite 作业](2026-09-29-isolate-sqlite-race-ci.md)的成功结论共同决定现有 `checks` 状态；没有新的作业或 required status。挂载包、SQLite 根包、HTTP 根包与其余包的集合不重叠且覆盖整个模块，后续新增包继续由 `go list ./...` 进入相应集合。

strict script、race、`-count=1`、十分钟 per-binary watchdog、checks 三十五分钟作业预算与 module-wide package-local coverage 保持原契约。调度和预算分别由本决定、[原有 race 预算](2026-09-08-budget-ci-race-test-execution.md)与[全负载作业预算](2026-09-23-budget-ci-full-load-execution.md)持有。

## 备选方案

**让全部非挂载包串行运行。** 能消除同作业的 Go 包竞争，但让完成上限充足的其它包也失去并行执行。本决定只将已触及 watchdog 的 HTTP 根包移到后续步骤。

**新增独立 HTTP 作业。** 可同时执行其它 checks 步骤，但需要另一份 runner 和缓存，并须将新作业结果接入现有 required status。独立步骤已能移除同作业包竞争，复用原 runner 与编译缓存；若它使 checks 总预算不足，再根据真实关键路径比较独立作业。

**提高 HTTP watchdog。** 会推迟真正停滞时的测试栈，并将调度竞争与测试行为变化混入新的上限。完整独立执行已有通过证据，先改变调度、保留原完成期限。

## 后果

HTTP 获得没有同作业其它 Go suite 竞争的执行时段；其它非挂载包继续并行。代价是 HTTP 完整调用进入 checks 的顺序关键路径，必须由真实 CI 验证作业仍在三十五分钟预算内。这个步骤的失败或未执行不能形成成功的 `checks`，也不能用独立调度实验替代新提交的 CI 结果。

若单独调度后仍触及十分钟，应重新定位测试进展、fixture 成本或具体等待；本决定不授权提高 watchdog、缩小测试集合或用重跑替代原因修复。挂载作业的覆盖率门禁继续执行 HTTP 和 SQLite，不能把 race 调度当作覆盖率替代。执行判据见[测试策略](../../../../docs/testing.md#ci-执行预算)。
