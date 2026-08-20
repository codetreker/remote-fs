# AGENTS.md — 文档

`docs/` 下三个目录承载三种不同的东西，边界是强制的，不是建议。

| 目录 | 承载什么 | 完整规则 |
|---|---|---|
| `spec/` | 系统要做到什么、给谁、为什么。**不写怎么实现，也不写什么时候做。** | [`spec/README.md`](spec/README.md) |
| `design/` | 已经定下来的架构。**用现在时，不写权衡、不写计划。** | [`design/README.md`](design/README.md) |
| `research/` | 调研证据与引用链。**写完即冻结**，不随代码更新。 | — |

目录之外还有顶层文档，承载既不属于上述任何一类、也不属于某一个决定的横向内容：

| 文档 | 承载什么 |
|---|---|
| [`testing.md`](testing.md) | 怎么验证这个系统：分层、故障注入、什么必须有测试、什么刻意不测 |

三条编辑时必须记住的：

- **spec 与 design 的分界是「义务 vs 机制」**，不是「接口 vs 内部实现」。系统保证什么、第三方必须保证什么，是 spec；任何一方用什么机器去满足它，是 design。`spec/README.md` 给了两条判据和难判案例的对照表。
- **design 不写理由。** 一个决定为什么这么定、比过哪些方案、放弃了什么，属于 `.agents/notes/`。design 允许一句话的理由，仅当该决定很可能被不知情的人推翻；不允许一节，更不允许方案对比。
- **重写，不要追加更正。** 一段与它上文矛盾的话，比它试图修正的那个错误更糟。

`design/` 与代码在**同一次改动**里更新 —— 不提前一步，也不落后一步。

写作标准见 [`rfs-prose-standard`](../.agents/skills/rfs-prose-standard/SKILL.md)；清理推理过程泄漏见 [`rfs-trim-cot-leakage`](../.agents/skills/rfs-trim-cot-leakage/SKILL.md)。
