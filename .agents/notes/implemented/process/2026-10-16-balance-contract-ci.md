# Agent Note: 平衡 contract CI 的并发工作

Status: implemented

## 问题

[固定 head 的 CI 调查](../../../../docs/research/2026-10-16-slow-ci-cases.md)中，Contract and unit tests 步骤耗时 14.6333 分钟。objectstore 到第 8.4984 分钟才开始，最后在第 14.4313 分钟结束；同机运行的 package binary 时间合计 45.7451 分钟。按四个运行槽和固定旧耗时计算，理论下界已有 11.4363 分钟，长包优先投影仍为 12.2573 分钟。相同 runner 的 package 排序不足以给十分钟目标留下余量，重复准备的局部优化也不能保证抵消全部尾部。

各 package 的真实 race、错误路径、能力、后端和负载验证需要保留。工作可以换 runner 和启动顺序，不能通过放宽 watchdog、丢弃子用例、覆盖率归属或失败结论来换取更短时间；[R-ERR-1／R-ERR-2、R-INT-3 与 R-WIN-6](../../../../docs/spec/requirements.md)的既有验证仍须完整成立。

## 决定

[CI workflow](../../../../.github/workflows/ci.yml)用同一 canonical `STORAGE_RACE_PACKAGES` 列表选择根 limited、localstore 和 objectstore。`storage-race` 在独立 Ubuntu 24.04 runner 上完整执行这三包；`checks-work` 从自动发现结果中排除同一列表，优先启动 SMB、SQLite integration、replicated，再运行 `./...` 发现的其余包。原有 34 包划分为 checks 的 31 包与 storage 的三包，不重叠、不遗漏，每包恰好一次。只列 root package，azblob、localdisk 等子包仍由 checks 发现。

`checks-work`、`storage-race`、`sqlite-race` 与 `http-race` 独立开始。storage 作业有二十分钟总预算，与其它 race 调用相同使用严格 wrapper、`-race -count=1 -timeout 10m`，完整运行、不带 `-run`。checks 的 build／Console／vet／格式／链接、串行全负载副本验收、SQLite／HTTP 独立整包、mounted race／联合验收／卸载检查、全模块 package-local coverage 与 Windows 作业保持原判据。

最终 `checks` 保留原必需状态显示名称，以 `always()` 依赖上述四个非挂载作业，只有四项都明确为 `success` 才成功。failure、cancelled 与 skipped 均失败；它不安装工具链或缓存。storage 的新结果必须经过这道 fail-closed gate。

storage 使用[共享 Go setup action](../../../../.github/actions/setup-go/action.yml)，工具链固定 Go 1.26.8，以自身 `storage-race` 身份保存 module／build 快照。恢复顺序是同身份前缀、相容的 `checks` 前缀、经校验的旧 archive。它在测试前启动现有 pinned Azurite compose 并等待 ready；checks 和 mounted 各自保留原 emulator，不能跨 runner 借用。缓存格式和兼容条件由[缓存决定](2026-09-09-refresh-ci-go-build-caches.md)持有。

本决定部分取代[并发 CI 决定](2026-10-14-parallel-ci-execution.md)的非挂载包分组和三项结果门禁；后者关于 SQLite／HTTP 与其它工作独立开始、原必需状态与失败关闭的理由继续成立。测试准备、native 身份和隔离条件由[测试工作决定](../testing/2026-09-09-reduce-test-work.md)拥有，不由 runner 分组改变。

## 备选方案

**只调整 package 顺序和 fixture。** 能消除 late start 与重复健康准备，不增加 runner；但按完整旧 binary 耗时固定计算的四槽下界仍超过十分钟，局部收益不能作为整份调用的预算保证。保留 case 优化，同时分担长包竞争，才能取得可实测的余量。

**每个长包各用一个 runner。** 可进一步减少同机竞争，却增加更多 runner 启动、module／build cache 下载和上传、Blob emulator 及维护项。三个 storage 根包组成一份工作，保留独立于 SMB／integration／replicated 的运行资源；若实际 hosted 数据仍显示分组内竞争限制目标，再用新测量判断更细分组。

**提高 watchdog 或减少测试工作量。** 提高 watchdog 只延后真实挂起的终止；减少能力、崩溃、原子读取或压力向量会改变被验证的命题。分组与健康准备优化不需要这些代价。

## 后果

新增一份 storage runner、一次 emulator 启动和一份独立编译快照。更多并发可以缩短关键路径，也可能增加 runner 分钟数、排队、缓存传输和仓库缓存占用；净收益须同时看测试步骤和完整作业。

固定旧 binary 耗时按新集合拆分后，checks 合计 25.0056 binary 分钟、storage 合计 20.7395 binary 分钟；四槽长包优先投影分别为 8.2799 与 7.5090 分钟。这些是资源分配估算，排除编译、strict 清单核对、emulator、缓存和其它步骤，且新竞争条件会改变实际 binary 耗时。它们不能证明十分钟已经达成；最终判断使用交付 head 的完整 hosted 测试步骤，局部匹配收益单独报告。

从实际 YAML 提取的选择命令在 Go 1.26.8 下核对原 34 包与 31＋3 的集合相同且不重叠、完整 root 列表和各包唯一性。实际 gate shell 的 success／failure／cancelled／skipped 四项笛卡尔积共 256 组中，只有四项全 success 成功。测试 verdict 和 package-local coverage 仍须由对应真实执行裁定，配置级检查不能替代它们。

延期的 HTTP／FUSE case 优化和更细 runner 分组各需局部证据，不增加当前分组的隐含依赖。自动 `./...` 发现、canonical storage 列表、严格错误门禁与 package-local coverage 保持，使后续新增包或再平衡不需要重建验收体系。
