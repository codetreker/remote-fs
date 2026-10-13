# Console

独立服务随附的 React 与 TypeScript 后台。页面和管理 API 使用同一 origin；服务端验证身份和 volume 所有权。

## 构建

构建 Go 二进制前生成前端资源：

```sh
pnpm --dir internal/console install --frozen-lockfile
pnpm --dir internal/console test
pnpm --dir internal/console build
pnpm --dir internal/console check:dist
make build
```

`build` 为前端源码和生成资源记录 SHA-256 manifest。`check:dist` 在源码或资源与 manifest 不符时失败。`dist` 随源码提交，Go 包测试直接使用它；CI 在发布前重新构建。缺少资源时 Go embed 编译失败。依赖和 pnpm 版本由 `package.json` 与 lockfile 固定；缓存位于仓库 `.tmp/`。

开发时在 loopback 启动 Vite：

```sh
pnpm --dir internal/console dev
```

连接真实服务时使用同源 TLS 开发代理。浏览器 session 需要配置的 HTTPS origin 和 Secure cookie。服务负责 `/api`，`console.Handler()` 提供页面及带内容指纹的静态资源。

## 身份与管理

管理员使用账号密码登录。普通用户登录等待身份系统接入。用户可以查看、创建、改名与删除自己的 volumes；创建时设置容量上限，现有 volume 的容量上限只读显示。管理请求在确认完成后提示成功；连接中断或无效响应显示结果未知，界面不会自动重复提交。

管理员还可以查看普通用户身份及各 volume 的名称、文件数、目录数、已用空间和容量上限。摘要不提供文件列表或内容入口。

CSRF token 只存在于前端内存；页面刷新通过当前 HttpOnly cookie session 恢复。登录和注销完成后，其他 tab 收到同源身份失效通知，清空私有页面并向服务重新确认身份。通知不提供角色、凭据或认证结果。身份切换取消原身份的浏览器请求并重建私有页面。

管理 API 的整数以十进制字符串传输，前端校验后转为 BigInt。统计失败显示明确错误，不以零值或部分总计表示成功。

## 只读文件浏览

自己的 volume 支持目录浏览、面包屑导航、目录内搜索及刷新。文件页通过 `GET /api/service/v1/volumes/{id}/files?path=...` 获取属性；响应只包含一次父路径与各项原始名字的 Base64 标识；客户端按原始字节拼接导航路径，显示名称不作为导航输入。符号链接只显示自身属性，不展开目标。

Console 不提供上传、下载、文件修改或在线编辑。管理及文件属性验证使用 `src/api/management.test.ts`；Go 包测试验证嵌入页面、静态资源与 API 命名空间隔离。
