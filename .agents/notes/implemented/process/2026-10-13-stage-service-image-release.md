# Agent Note: 同一服务镜像经 staging 发布到 production

Status: implemented

## 问题

独立管理服务需要可重复的发布入口。production 使用的构建产物必须是 staging 已经启动并确认就绪的那一份；两套环境的用户、volumes、持久数据和配置不能在发布时互相覆盖。[R-SVC-9](../../../../docs/spec/requirements.md) 定义这些发布保证。

## 决定

[release workflow](../../../../.github/workflows/release.yml) 手动触发，build 作业构建一次服务镜像并推送到私有 GHCR package，以推送返回的 digest 标识此次产物。staging 和 prod 作业通过 [SSH 部署脚本](../../../../.github/scripts/deploy-service.sh) 在部署机上运行同一份 Compose，依次拉取并启动同一 digest；staging 的 Compose 就绪等待成功后才开始 prod。production 不重新构建镜像。

两个 GitHub Environments 分别提供自己的 `DEPLOY_PATH`。环境目录内的 `.env`、配置、state 和端口设置由部署方准备，workflow 更新 `compose.yml`，健康检查通过后将完整镜像 digest 写入 `release.env`。发布不初始化、复制或删除既有服务数据。两套部署采用独立的 Compose project name，容器仍使用 `/var/lib/remote-fs` 作为 state root。

GHCR 发布与拉取使用各作业自己的 `GITHUB_TOKEN`，SSH 使用已有部署机配置和经核验的 `OC_VM_KNOWN_HOSTS` 公钥记录。SSH 材料与 registry 登录状态在任务私有临时目录内使用并清理。包保持 private，部署方必须允许本仓库 workflow 访问该 package。服务的账号、owner、文件权限和存储行为仍由[独立管理服务](../feature/2026-10-12-standalone-volume-service.md)拥有；发布机制见[服务部署设计](../../../../docs/design/server/service.md)。

## 范围

| 内容 | 分类 | 处置 |
|---|---|---|
| R-SVC-9 的同一产物、staging 就绪顺序和错误结果 | 保证 | 构建输出 digest 贯穿两个部署，等待失败使发布失败。 |
| R-SVC-8、R-SVC-9 的数据与配置保留 | 保证 | 配置与 state 由环境目录持有，发布只更新运行产物。 |
| 额外发布账本、独立 promotion 服务和自动回滚 | 功能 | 不交付；以后可以围绕已有 digest 和环境配置增加，须另行定义其状态与数据兼容义务。 |
| 跨主机或更多环境 | 形状 | 不把环境配置和 state 打进镜像，不用同一宿主目录承载两套服务；以后增加目标仍能复用同一产物。 |

## 备选方案

**直接发布到 production。** 省去 staging 部署，但没有上线前启动与就绪确认，不能满足 R-SVC-9。

**两个环境分别构建相同源码。** 每个环境拥有独立构建结果，依赖解析或构建输入变化可能产生不同产物；staging 的就绪结果不能证明 production 所用镜像已经运行过。

**独立 promotion workflow、版本账本与自动回滚框架。** 需要新增持久发布状态、跨 workflow 核验和数据兼容规则。单次 workflow 的依赖关系和 digest 已满足本次发布顺序；这些额外能力保留为独立工作，不能在未定义数据恢复义务时将回滚解释为安全恢复。

## 后果

staging 与 production 使用相同代码和构建依赖，各自持有配置与数据。私有 GHCR 的拉取权限和 Docker Compose 是部署前提；缺少任一条件时发布失败。运维维护两套环境目录和既有 SSH 入口，发布流程不增加服务或数据库。

Compose 就绪等待把镜像拉取、启动与健康失败反映到作业结果。就绪仅说明服务可以接受请求，不证明管理员权限、存储完整性或全部业务行为；这些验证由[测试策略](../../../../docs/testing.md)定义。部署会替换单个 app 容器，因此不承诺零停机。失败不会自动切回旧镜像；已发生的服务启动与持久状态变化不能被 workflow 的失败结果撤销。
