# Agent Note: 单独调度 SQLite race 测试

Status: implemented

## 问题

[PR #41 的 checks 作业](https://github.com/codetreker/remote-fs/actions/runs/36560468737/job/109379968361)在根 `packages/metastore/sqlite` 包的 race 二进制累计运行十分钟后触发 `panic: test timed out after 10m0s`。到期时 `TestTheContract` 的 SQL 测试仍在推进；这份未完成的运行不能作为通过证据，也不能仅凭当时栈认定单项测试死锁。此前 main 上相同根包的运行约需 452–539 秒，本次新增 SQLite 用例的定向耗时约八秒。根包接近十分钟累计 watchdog，和其它非挂载包同时争用 runner 时，整包正常推进也可能越过截止时间。

[原有整包 race 预算决定](2026-09-08-budget-ci-race-test-execution.md)把每个包二进制的 watchdog 设为十分钟；当时单独调度 SQLite 尚缺持续竞争证据。本次有了接近上限的基线与竞争下的失败，须改变作业调度，同时继续让真实挂起在有界时间内留下测试栈，并保证所有包仍实际执行。

## 决定

[CI workflow](../../../../.github/workflows/ci.yml)使用三个 Linux 作业。`checks` 执行 build、vet、格式、链接、串行全负载验收和其它非挂载包的 race suite；它用 `go list` 从这份 suite 中精确排除挂载包及**根** `./packages/metastore/sqlite`。`sqlite-race` 在独立 Ubuntu runner 上只对这个根包执行 `assert-every-test-ran.sh -race -count=1 -timeout 10m ./packages/metastore/sqlite`。SQLite 的内部子包仍由 `checks` 运行，不能在两个过滤列表之间丢包。`mounted` 保持 FUSE／端到端 race、卸载检查及整个模块的 package-local 覆盖率门禁；覆盖率调用仍包含根 SQLite 包。Windows SSPI 作业独立。

SQLite race 作业上限为二十分钟，但 Go 的十分钟 watchdog 仍约束该包二进制的累计执行。strict script 继续拒绝 skip、无测试、缺失 verdict 与测试进程失败；`-count=1` 禁止缓存结果替代执行。独立 runner 去掉同作业其它 Go 包与 SQLite race 的并行资源竞争，不降低测试断言，也不放宽测试本身的完成期限。受保护分支的现有必需状态是 `checks`，不是新增 `sqlite-race`：workflow 令 `checks` 依赖它，并以 `always()` 使 checks 在依赖失败或跳过后仍运行；最后一步显式核对 `needs.sqlite-race.result == success`，其它结果使原必需检查失败。该作业没有 Blob 依赖；`checks` 与 `mounted` 继续各自启动 Azurite 供它们的 Blob 用例使用。

## 备选方案

**提高 SQLite 的 `go test -timeout`。** 可以让争用下的同一运行多等待一段时间，但会推迟真正卡住时的 goroutine 栈，并把 runner 竞争与测试行为变化混在新的更长上限里。已有 main 基线与当前步骤并发给出一个更窄的调度改变，因此保留原十分钟 watchdog。

**让 checks 的全部非挂载包串行运行。** 这也能减少 SQLite 的竞争，但让每个其它包失去并行执行，并扩大 CI 总耗时；目前只有根 SQLite race 二进制接近自身 watchdog。独立作业把调度改变限定在这一个包。

**从 race suite 移除 SQLite 或只重跑失败的 CI。** 前者取消关键并发验证，后者不能给当前提交一份必然可复现的完整结果；二者都无法说明一次接近 watchdog 的包究竟通过与否。

## 后果

根 SQLite race 与其它包在不同 runner 上执行，整包仍受十分钟累计 watchdog 约束；checks 作业的并行资源压力下降。代价是一份额外 Ubuntu runner、独立的 Go 编译缓存快照与串行依赖造成的关键路径延长。`sqlite-race` 的失败、跳过或超时都经 `checks` 的末步进入现有必需状态；工作流取消也不能形成成功的 `checks`。SQLite 内部子包和 module-wide coverage 继续运行，不把这个独立作业误当作覆盖率替代。

若独立 runner 上的 SQLite race 仍反复触及十分钟，或具体测试出现可重复的局部退化，应重新检查包内执行进展、锁等待和用例成本；本决定不授权再次提高 watchdog。全负载验收与整作业预算仍由[全负载执行预算决定](2026-09-23-budget-ci-full-load-execution.md)持有；测试层和 skip 判据见[测试策略](../../../../docs/testing.md#ci-执行预算)。
