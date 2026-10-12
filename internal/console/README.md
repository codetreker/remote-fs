# Console

独立服务随附的 React 与 TypeScript 后台。页面和 API 使用同一 origin；服务端验证身份、volume 所有权和每项操作的权限。

## 构建

构建 Go 二进制前生成前端资源：

```sh
pnpm --dir internal/console install --frozen-lockfile
pnpm --dir internal/console test
pnpm --dir internal/console build
pnpm --dir internal/console check:dist
make build
```

`build` 为前端源码和生成资源记录 SHA-256 manifest。`check:dist` 在源码或资源与 manifest 不符时失败。`dist` 随源码提交，Go 包测试直接使用它；CI 在发布前重新构建。缺少资源时 Go embed 编译失败。依赖、pnpm 版本和 lossless-json 的严格解析补丁由 `package.json` 与 lockfile 固定；项目缓存位于仓库 `.tmp/`。

开发时可在 loopback 启动 Vite：

```sh
pnpm --dir internal/console dev
```

连接真实服务时使用同源 TLS 开发代理。浏览器 session 需要配置的 HTTPS origin 和 Secure cookie。服务负责 `/api`，`console.Handler()` 提供公共页面及带内容指纹的静态资源。

## 身份与数据

管理员使用账号密码登录。普通用户登录由未来的身份系统提供。用户概览只包含普通用户身份和各 volume 的名称、文件数、目录数、已用空间、配额。

CSRF token 只存在于前端内存；页面刷新通过当前 HttpOnly cookie session 取回同一个 token。登录和注销完成后，其他 tab 收到同源身份失效通知，清空私有页面并向服务重新确认当前身份。通知不提供角色、凭据或认证结果。

管理操作的原动作 ID 和不可变输入，以及上传的原动作 ID，保存在 actor／volume 绑定的 sessionStorage 记录中。重试管理操作先查询原回执，必要时用相同 key 和输入继续；上传恢复只查询、继续已封存对象或清理原传输。原生终态和传输清理都确认后才删除上传记录。确认注销后删除该身份的记录；确认切换身份时删除其他身份的记录，并取消原身份的浏览器请求。

管理 API 的整数以十进制字符串传输，前端校验后转为 BigInt。v5 client 使用修补后的 lossless-json，保留 numeric JSON 的精确整数。文件选择保留原始字节名字及身份；显示编码可区分非法 UTF-8、字面反斜杠、控制字符和 Unicode 格式字符。

## 文件操作

文件浏览、目录操作和传输使用 owner 限定的 v5 session、条件对象身份及原生 publication 路径。上传使用协商的资源上限和有界摘要计算。浏览器下载将已验证的固定导出交给浏览器原生传输；交接不表示客户端已经完整收到文件。后台提供上传、下载、新建目录、改名、移动和删除。

健康元数据在 strong-lock 恢复期间仍提供真实统计和普通读取，会冲突的修改由服务拒绝。统计失败显示明确错误，不以零值或部分总计表示成功。

## 浏览器验证

交互与 wire 测试使用生产页面及拦截的 API fixtures；fixtures 位于测试目录，不进入发布 bundle。已有浏览器可运行此验证：

```sh
cd internal/console
CONSOLE_BROWSER_EXECUTABLE=/path/to/existing/chromium \
CONSOLE_BASE_URL=http://127.0.0.1:4317/ \
node tests/browser.mjs
```

浏览器 profile、截图和回执保存在本包忽略的 `.tmp/`。真实服务验收另外验证服务端认证、所有权隔离、持久操作以及文件的原生效果。
