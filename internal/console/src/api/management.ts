import type {
  FileDirectory,
  FileEntry,
  Page,
  Session,
  Summary,
  User,
  UserVolume,
  Volume,
} from "../types";
const PREFIX = "/api/service/v1";
const INT64_MAX = 9223372036854775807n;
const UINT64_MAX = 18446744073709551615n;
const object = (value: unknown, label: string): Record<string, unknown> => {
  if (typeof value !== "object" || value === null || Array.isArray(value))
    throw new Error(`${label} 返回格式无效。`);
  return value as Record<string, unknown>;
};
const string = (value: unknown, label: string): string => {
  if (typeof value !== "string") throw new Error(`${label} 必须是字符串。`);
  return value;
};
const decimal = (value: unknown, label: string, max = INT64_MAX): bigint => {
  const text = string(value, label);
  if (text.length > 20 || !/^(0|[1-9]\d*)$/.test(text))
    throw new Error(`${label} 不是确切的十进制整数。`);
  const number = BigInt(text);
  if (number > max) throw new Error(`${label} 超出范围。`);
  return number;
};
const requiredArray = (value: unknown, label: string) => {
  if (!Array.isArray(value)) throw new Error(`${label} 返回格式无效。`);
  return value;
};
const childPathBase64 = (parentBytes: string, nameBase64: string) =>
  btoa(parentBytes + (parentBytes === "" ? "" : "/") + atob(nameBase64));
export class APIError extends Error {
  constructor(
    public code: string,
    public status: number,
  ) {
    super(publicError(code));
  }
}
export class MutationOutcomeUnknown extends Error {
  constructor(cause: unknown) {
    super("操作结果未知。请刷新列表确认当前状态，确认前不要重新提交。", {
      cause,
    });
  }
}
export const publicError = (code: string) =>
  (
    ({
      authentication_required: "登录已失效，请重新登录。",
      access_denied: "你没有执行此操作的权限。",
      condition_conflict: "目标已发生变化，请刷新后重新确认操作。",
      volume_busy: "此 volume 正在使用，请稍后重试。",
      metadata_unavailable: "无法取得权威统计，数据暂不可用。",
      storage_unavailable: "存储暂不可用，请稍后重试。",
      not_found: "目标不存在。",
      authentication_capacity: "登录请求过多，请稍后再试。",
      quota_exceeded: "Volume 配额不足。",
      capacity_exhausted: "服务资源已达到上限，请稍后重试。",
      invalid_request: "请求无效，请检查输入。",
    }) as Record<string, string>
  )[code] ?? `服务拒绝了请求（${code}）。`;
const parseUser = (value: unknown): User => {
  const row = object(value, "用户");
  const role = string(row.role, "角色");
  if (role !== "admin" && role !== "user")
    throw new Error("服务返回了未知角色。");
  return {
    id: string(row.id, "用户 ID"),
    displayName: string(row.displayName, "显示名称"),
    role,
  };
};
const parseSession = (value: unknown): Session => {
  const row = object(value, "会话");
  return {
    user: parseUser(row.user),
    csrfToken: string(row.csrfToken, "CSRF token"),
    idleExpiresAt: string(row.idleExpiresAt, "会话期限"),
    absoluteExpiresAt: string(row.absoluteExpiresAt, "会话期限"),
  };
};
const parseSummary = (value: unknown): Summary => {
  const row = object(value, "统计");
  return {
    fileCount: decimal(row.fileCount, "文件数", UINT64_MAX),
    directoryCount: decimal(row.directoryCount, "目录数", UINT64_MAX),
    usedBytes: decimal(row.usedBytes, "已用字节"),
    quotaBytes: decimal(row.quotaBytes, "配额字节"),
  };
};
const parseError = (value: unknown) => ({
  code: string(object(value, "错误").code, "错误码"),
});
const statsBranches = (row: Record<string, unknown>) => {
  if ((row.summary === undefined) === (row.summaryError === undefined))
    throw new Error("统计必须包含确切结果或明确错误。");
};
const parseVolume = (value: unknown): Volume => {
  const row = object(value, "Volume");
  statsBranches(row);
  return {
    id: string(row.id, "Volume ID"),
    ownerUserId: string(row.ownerUserId, "所有者"),
    name: string(row.name, "名称"),
    state: string(row.state, "状态"),
    revision: decimal(row.revision, "管理版本"),
    createdAt: string(row.createdAt, "创建时间"),
    updatedAt: string(row.updatedAt, "更新时间"),
    ...(row.summary !== undefined
      ? { summary: parseSummary(row.summary) }
      : { summaryError: parseError(row.summaryError) }),
  };
};
const parseUserVolume = (value: unknown): UserVolume => {
  const row = object(value, "Volume 摘要");
  statsBranches(row);
  if (
    Object.keys(row).some(
      (key) => !["name", "summary", "summaryError"].includes(key),
    )
  )
    throw new Error("管理员摘要包含越界字段。");
  if (
    row.summary !== undefined &&
    Object.keys(object(row.summary, "统计")).some(
      (key) =>
        !["fileCount", "directoryCount", "usedBytes", "quotaBytes"].includes(
          key,
        ),
    )
  )
    throw new Error("管理员摘要包含越界字段。");
  return {
    name: string(row.name, "名称"),
    ...(row.summary !== undefined
      ? { summary: parseSummary(row.summary) }
      : { summaryError: parseError(row.summaryError) }),
  };
};
const readJSON = async (
  response: Response,
  maxBytes = 1024 * 1024,
): Promise<unknown> => {
  if (!response.body) throw new Error("服务返回了空响应。");
  const reader = response.body.getReader(),
    decoder = new TextDecoder("utf-8", { fatal: true });
  let bytes = 0,
    text = "";
  try {
    for (;;) {
      const chunk = await reader.read();
      if (chunk.done) break;
      bytes += chunk.value.byteLength;
      if (bytes > maxBytes) throw new Error("服务响应超过允许大小。");
      text += decoder.decode(chunk.value, { stream: true });
    }
    text += decoder.decode();
    return JSON.parse(text) as unknown;
  } catch (error) {
    try {
      await reader.cancel(error);
    } catch (cleanup) {
      throw new AggregateError([error, cleanup], "响应读取与流清理未完成。");
    }
    throw error;
  } finally {
    reader.releaseLock();
  }
};
const authResponse = async (response: Response): Promise<Session> => {
  const result = await readJSON(response);
  if (!response.ok)
    throw new APIError(
      string(object(object(result, "错误").error, "错误").code, "错误码"),
      response.status,
    );
  return parseSession(result);
};
export const recoverSession = async (
  fetchImpl: typeof fetch = fetch,
): Promise<Session> =>
  authResponse(
    await fetchImpl(`${PREFIX}/auth/session`, {
      credentials: "same-origin",
      cache: "no-store",
      headers: { Accept: "application/json" },
    }),
  );
export const login = async (
  loginName: string,
  password: string,
  fetchImpl: typeof fetch = fetch,
): Promise<Session> =>
  authResponse(
    await fetchImpl(`${PREFIX}/auth/login`, {
      method: "POST",
      credentials: "same-origin",
      cache: "no-store",
      headers: {
        "Content-Type": "application/json",
        Accept: "application/json",
        "X-RFS-Request": "1",
      },
      body: JSON.stringify({ loginName, password }),
    }),
  );
export class ManagementClient {
  private active = true;
  private abort = new AbortController();
  constructor(
    public session: Session,
    private onAuthenticationLost: () => void,
    private fetchImpl: typeof fetch = fetch,
  ) {
    this.fetchImpl = fetchImpl.bind(globalThis);
  }
  dispose() {
    this.active = false;
    this.abort.abort();
  }
  private async request(
    path: string,
    method = "GET",
    body?: unknown,
    signal?: AbortSignal,
    maxResponseBytes = 1024 * 1024,
  ): Promise<unknown> {
    if (!this.active) throw new Error("访问身份已更改。");
    const mutation = method !== "GET";
    const response = await this.fetchImpl(PREFIX + path, {
      method,
      credentials: "same-origin",
      cache: "no-store",
      signal: AbortSignal.any([this.abort.signal, ...(signal ? [signal] : [])]),
      headers: {
        Accept: "application/json",
        ...(mutation
          ? {
              "Content-Type": "application/json",
              "X-CSRF-Token": this.session.csrfToken,
            }
          : {}),
      },
      ...(body !== undefined ? { body: JSON.stringify(body) } : {}),
    });
    if (!this.active) throw new Error("访问身份已更改。");
    if (response.status === 204) return undefined;
    const result = await readJSON(response, maxResponseBytes);
    if (!this.active) throw new Error("访问身份已更改。");
    if (!response.ok) {
      const error = new APIError(
        string(object(object(result, "错误").error, "错误").code, "错误码"),
        response.status,
      );
      if (response.status === 401) this.onAuthenticationLost();
      throw error;
    }
    return result;
  }
  async logout() {
    const result = await this.request("/auth/logout", "POST", {});
    if (result !== undefined)
      throw new Error("注销响应无效，尚未确认会话撤销。");
    this.dispose();
  }
  private page<T>(
    value: unknown,
    key: string,
    parse: (value: unknown) => T,
  ): Page<T> {
    const row = object(value, "列表");
    return {
      items: requiredArray(row[key], "列表").map(parse),
      ...(row.nextAfter !== undefined
        ? { nextAfter: string(row.nextAfter, "分页游标") }
        : {}),
    };
  }
  private listPath(path: string, after?: string) {
    return (
      path +
      "?limit=50" +
      (after !== undefined ? `&after=${encodeURIComponent(after)}` : "")
    );
  }
  async volumes(after?: string, signal?: AbortSignal) {
    return this.page(
      await this.request(
        this.listPath("/volumes", after),
        "GET",
        undefined,
        signal,
      ),
      "volumes",
      parseVolume,
    );
  }
  async volume(id: string) {
    return parseVolume(
      await this.request(`/volumes/${encodeURIComponent(id)}`),
    );
  }
  async users(after?: string, signal?: AbortSignal) {
    return this.page(
      await this.request(
        this.listPath("/admin/users", after),
        "GET",
        undefined,
        signal,
      ),
      "users",
      parseUser,
    );
  }
  async userVolumes(userId: string, after?: string, signal?: AbortSignal) {
    return this.page(
      await this.request(
        this.listPath(
          `/admin/users/${encodeURIComponent(userId)}/volumes`,
          after,
        ),
        "GET",
        undefined,
        signal,
      ),
      "volumes",
      parseUserVolume,
    );
  }
  async files(
    volumeId: string,
    pathBase64: string,
    signal?: AbortSignal,
  ): Promise<FileDirectory> {
    const row = object(
      await this.request(
        `/volumes/${encodeURIComponent(volumeId)}/files?path=${encodeURIComponent(pathBase64)}`,
        "GET",
        undefined,
        signal,
        64 * 1024 * 1024,
      ),
      "目录",
    );
    const path = string(row.pathBase64, "当前路径");
    if (path !== pathBase64) throw new Error("目录返回了不同的目标路径。");
    const parentBytes = atob(path);
    const entries = requiredArray(row.entries, "目录项目").map(
      (value): FileEntry => {
        const entry = object(value, "目录项目");
        const kind = string(entry.kind, "类型");
        if (!["directory", "file", "symlink"].includes(kind))
          throw new Error("目录包含未知项目类型。");
        const nameBase64 = string(entry.nameBase64, "原始名称");
        return {
          name: string(entry.name, "显示名称"),
          nameBase64,
          pathBase64: childPathBase64(parentBytes, nameBase64),
          nodeId: decimal(entry.nodeId, "节点 ID", UINT64_MAX),
          kind: kind as FileEntry["kind"],
          sizeBytes: decimal(entry.sizeBytes, "文件大小"),
          modifiedAt: string(entry.modifiedAt, "修改时间"),
        };
      },
    );
    return { pathBase64: path, entries };
  }
  private async mutate(
    path: string,
    method: string,
    body: unknown,
  ): Promise<unknown> {
    try {
      return await this.request(path, method, body);
    } catch (error) {
      if (error instanceof APIError && error.status < 500) throw error;
      throw new MutationOutcomeUnknown(error);
    }
  }
  async createVolume(name: string, quotaBytes: bigint) {
    const bytes = validBytes(quotaBytes);
    try {
      return parseVolume(
        await this.mutate("/volumes", "POST", { name, quotaBytes: bytes }),
      );
    } catch (error) {
      if (error instanceof APIError || error instanceof MutationOutcomeUnknown)
        throw error;
      throw new MutationOutcomeUnknown(error);
    }
  }
  async renameVolume(volume: Volume, name: string) {
    try {
      return parseVolume(
        await this.mutate(
          `/volumes/${encodeURIComponent(volume.id)}`,
          "PATCH",
          { name },
        ),
      );
    } catch (error) {
      if (error instanceof APIError || error instanceof MutationOutcomeUnknown)
        throw error;
      throw new MutationOutcomeUnknown(error);
    }
  }
  async deleteVolume(volume: Volume) {
    const result = await this.mutate(
      `/volumes/${encodeURIComponent(volume.id)}`,
      "DELETE",
      undefined,
    );
    if (result !== undefined)
      throw new MutationOutcomeUnknown(new Error("删除响应无效。"));
  }
}
const validBytes = (value: bigint) => {
  if (value <= 0n || value > INT64_MAX)
    throw new Error("容量必须大于零且不超过 int64 字节上限。");
  return value.toString();
};
