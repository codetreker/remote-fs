import { clearActorJournals } from "./browser-state";
import type {
  ActionKey,
  DurableAction,
  Operation,
  Page,
  PublicSummary,
  Session,
  ServiceCapabilities,
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
export class APIError extends Error {
  constructor(
    public code: string,
    public status: number,
  ) {
    super(publicError(code));
  }
}
export const publicError = (code: string) =>
  (
    ({
      authentication_required: "登录已失效，请重新登录。",
      access_denied: "你没有执行此操作的权限。",
      condition_conflict: "目标已发生变化，请刷新后重新确认操作。",
      volume_busy: "此 volume 仍有活跃客户端或传输，请先卸载并关闭文件。",
      metadata_unavailable: "无法取得权威统计，数据暂不可用。",
      storage_unavailable: "存储暂不可用，请稍后重试。",
      not_found: "目标不存在。",
      authentication_capacity: "登录请求过多，请稍后再试。",
      acceptance_expired: "操作接受窗口已过期，请查询已有回执。",
      action_history_expired: "操作回执已退休，不能确认它未执行。",
      quota_exceeded: "Volume 配额不足，请释放空间或调整配额。",
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
    quotaRevision: decimal(row.quotaRevision, "配额版本"),
  };
};
const parsePublicSummary = (value: unknown): PublicSummary => {
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
    ...(row.lastOperationId !== undefined
      ? { lastOperationId: string(row.lastOperationId, "操作 ID") }
      : {}),
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
      ? { summary: parsePublicSummary(row.summary) }
      : { summaryError: parseError(row.summaryError) }),
  };
};
const parseOperation = (value: unknown): Operation => {
  const row = object(value, "操作");
  const state = string(row.state, "操作状态");
  if (!["pending", "running", "succeeded", "failed"].includes(state))
    throw new Error("服务返回了未知操作状态。");
  if (typeof row.settled !== "boolean") throw new Error("操作结算状态无效。");
  if (
    (row.settled && !["succeeded", "failed"].includes(state)) ||
    (state === "succeeded" && !row.settled)
  )
    throw new Error("操作状态与结算结果矛盾，不能确认最终结果。");
  return {
    id: string(row.id, "操作 ID"),
    volumeId: string(row.volumeId, "Volume ID"),
    kind: string(row.kind, "操作类型"),
    state,
    phase: string(row.phase, "操作阶段"),
    revision: decimal(row.revision, "操作版本"),
    settled: row.settled,
    createdAt: string(row.createdAt, "创建时间"),
    updatedAt: string(row.updatedAt, "更新时间"),
    ...(row.error !== undefined ? { error: parseError(row.error) } : {}),
  };
};
export class OutcomeUnknown extends Error {
  constructor(
    public action: ActionKey,
    public cause: unknown,
  ) {
    super("请求结果未知。请查询同一操作的持久回执，确认前不要重新提交。");
  }
}
const readJSON = async (response: Response): Promise<unknown> => {
  if (!response.body) throw new Error("服务返回了空响应。");
  const reader = response.body.getReader();
  const decoder = new TextDecoder("utf-8", { fatal: true });
  let bytes = 0,
    text = "";
  try {
    for (;;) {
      const chunk = await reader.read();
      if (chunk.done) break;
      bytes += chunk.value.byteLength;
      if (bytes > 1024 * 1024) throw new Error("服务响应超过允许大小。");
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
export const recoverSession = async (
  fetchImpl: typeof fetch = fetch,
): Promise<Session> => {
  const response = await fetchImpl(`${PREFIX}/auth/session`, {
    credentials: "same-origin",
    cache: "no-store",
    headers: { Accept: "application/json" },
  });
  const result = await readJSON(response);
  if (!response.ok)
    throw new APIError(
      string(object(object(result, "错误").error, "错误").code, "错误码"),
      response.status,
    );
  return parseSession(result);
};
export const login = async (
  loginName: string,
  password: string,
  fetchImpl: typeof fetch = fetch,
): Promise<Session> => {
  const response = await fetchImpl(`${PREFIX}/auth/login`, {
    method: "POST",
    credentials: "same-origin",
    cache: "no-store",
    headers: {
      "Content-Type": "application/json",
      Accept: "application/json",
      "X-RFS-Request": "1",
    },
    body: JSON.stringify({ loginName, password }),
  });
  const result = await readJSON(response);
  if (!response.ok)
    throw new APIError(
      string(object(object(result, "错误").error, "错误").code, "错误码"),
      response.status,
    );
  return parseSession(result);
};
export type PendingAction = {
  action: ActionKey;
  kind: string;
  operationId?: string;
  submittedAt: string;
  request: { path: string; method: string; payload: Record<string, unknown> };
};
const validateIntent = (
  kind: string,
  value: unknown,
): PendingAction["request"] => {
  const row = object(value, "原操作意图");
  const path = string(row.path, "原请求路径"),
    method = string(row.method, "原请求方法"),
    payload = object(row.payload, "原请求输入");
  const methods: Record<string, string> = {
    create: "POST",
    rename: "PATCH",
    quota: "PUT",
    delete: "DELETE",
  };
  if (
    methods[kind] !== method ||
    !(kind === "create"
      ? path === "/volumes"
      : kind === "quota"
        ? /^\/volumes\/[^/?#]+\/quota$/.test(path)
        : /^\/volumes\/[^/?#]+$/.test(path))
  )
    throw new Error("原操作意图无效。");
  const fields: Record<string, string[]> = {
    create: ["name", "quotaBytes"],
    rename: ["name", "expectedRevision"],
    quota: ["quotaBytes", "expectedRevision"],
    delete: ["expectedRevision"],
  };
  const allowed = fields[kind];
  if (
    !allowed ||
    Object.keys(payload).length !== allowed.length ||
    Object.keys(payload).some((key) => !allowed.includes(key))
  )
    throw new Error("原操作输入无效。");
  if ("name" in payload) string(payload.name, "原名称");
  if ("quotaBytes" in payload) decimal(payload.quotaBytes, "原配额");
  if ("expectedRevision" in payload)
    decimal(payload.expectedRevision, "原版本");
  return { path, method, payload };
};
export class LogoutCleanupError extends Error {
  constructor(cause: unknown) {
    super("注销会话已确认，但浏览器私有记录清理失败。请清除本站浏览器数据。", {
      cause,
    });
  }
}
const pendingKey = (userId: string) => `rfs.pending.${userId}`;
export class ManagementClient {
  private namespace: { value: bigint; acceptUntil: number } | undefined;
  private active = true;
  private abort = new AbortController();
  dispose() {
    this.active = false;
    this.abort.abort();
    this.namespace = undefined;
  }
  private allocation: Promise<void> | undefined;
  constructor(
    public session: Session,
    private onAuthenticationLost: () => void,
    private fetchImpl: typeof fetch = fetch,
  ) {
    this.fetchImpl = fetchImpl.bind(globalThis);
  }
  private async request(
    path: string,
    method = "GET",
    body?: unknown,
    signal?: AbortSignal,
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
    const result = await readJSON(response);
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
    try {
      clearActorJournals(this.session.user.id);
    } catch (error) {
      throw new LogoutCleanupError(error);
    }
    this.namespace = undefined;
  }
  async createAction(): Promise<DurableAction> {
    if (!this.namespace || this.namespace.acceptUntil <= Date.now() + 60_000) {
      if (!this.allocation) {
        this.allocation = (async () => {
          const row = object(
            await this.request("/action-namespaces", "POST", {}),
            "动作命名空间",
          );
          if (string(row.subjectId, "动作身份") !== this.session.user.id)
            throw new Error("动作命名空间属于其他身份。");
          const acceptUntil = Date.parse(string(row.acceptUntil, "动作期限"));
          if (!Number.isFinite(acceptUntil) || acceptUntil <= Date.now())
            throw new Error("动作命名空间已过期。");
          this.namespace = {
            value: decimal(row.namespace, "动作命名空间", UINT64_MAX),
            acceptUntil,
          };
        })();
      }
      try {
        await this.allocation;
      } finally {
        this.allocation = undefined;
      }
    }
    if (!this.namespace) throw new Error("无法取得动作命名空间。");
    const bytes = crypto.getRandomValues(new Uint8Array(16));
    const nonce = [...bytes]
      .map((byte) => byte.toString(16).padStart(2, "0"))
      .join("");
    return {
      subjectId: this.session.user.id,
      namespace: this.namespace.value,
      nonce,
    };
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
  async capabilities(): Promise<ServiceCapabilities> {
    const row = object(await this.request("/capabilities"), "服务能力");
    const policy = object(row.fileSession, "文件会话能力");
    const names = [
      "leaseNanoseconds",
      "historyNanoseconds",
      "maxFileSize",
      "maxFiles",
      "maxOperations",
      "maxWaiters",
      "maxLockOwners",
      "maxLockRanges",
      "maxPendingLocks",
      "maxLockActions",
      "maxCloseActions",
    ] as const;
    const fileSession = {} as ServiceCapabilities["fileSession"];
    for (const key of names) fileSession[key] = decimal(policy[key], key);
    return {
      fileSession,
      maxTransferBytes: decimal(row.maxTransferBytes, "传输上限"),
    };
  }
  async volumes(after?: string, signal?: AbortSignal): Promise<Page<Volume>> {
    return this.page(
      await this.request(
        "/volumes?limit=50" +
          (after !== undefined ? `&after=${encodeURIComponent(after)}` : ""),
        "GET",
        undefined,
        signal,
      ),
      "volumes",
      parseVolume,
    );
  }
  async volume(id: string): Promise<Volume> {
    return parseVolume(
      await this.request(`/volumes/${encodeURIComponent(id)}`),
    );
  }
  async users(after?: string, signal?: AbortSignal): Promise<Page<User>> {
    return this.page(
      await this.request(
        "/admin/users?limit=50" +
          (after !== undefined ? `&after=${encodeURIComponent(after)}` : ""),
        "GET",
        undefined,
        signal,
      ),
      "users",
      parseUser,
    );
  }
  async userVolumes(
    userId: string,
    after?: string,
    signal?: AbortSignal,
  ): Promise<Page<UserVolume>> {
    return this.page(
      await this.request(
        `/admin/users/${encodeURIComponent(userId)}/volumes?limit=50` +
          (after !== undefined ? `&after=${encodeURIComponent(after)}` : ""),
        "GET",
        undefined,
        signal,
      ),
      "volumes",
      parseUserVolume,
    );
  }
  async operation(id: string): Promise<Operation> {
    return parseOperation(
      await this.request(`/operations/${encodeURIComponent(id)}`),
    );
  }
  async queryAction(action: ActionKey): Promise<Operation> {
    return parseOperation(
      await this.request(
        `/operations?namespace=${encodeURIComponent(action.namespace)}&nonce=${encodeURIComponent(action.nonce)}`,
      ),
    );
  }
  pending(): PendingAction[] {
    const value = sessionStorage.getItem(pendingKey(this.session.user.id));
    if (value === null) return [];
    const parsed = JSON.parse(value) as unknown;
    return requiredArray(parsed, "待确认操作").map((item) => {
      const row = object(item, "待确认操作");
      const action = object(row.action, "动作");
      const namespace = decimal(action.namespace, "动作命名空间", UINT64_MAX);
      return {
        action: {
          namespace: namespace.toString(),
          nonce: string(action.nonce, "动作 nonce"),
        },
        kind: string(row.kind, "操作类型"),
        submittedAt: string(row.submittedAt, "提交时间"),
        request: validateIntent(string(row.kind, "操作类型"), row.request),
        ...(row.operationId !== undefined
          ? { operationId: string(row.operationId, "操作 ID") }
          : {}),
      };
    });
  }
  private savePending(pending: PendingAction[]) {
    if (!this.active) throw new Error("访问身份已更改。");
    sessionStorage.setItem(
      pendingKey(this.session.user.id),
      JSON.stringify(pending),
    );
  }
  forget(action: ActionKey) {
    this.savePending(
      this.pending().filter(
        (item) =>
          item.action.namespace !== action.namespace ||
          item.action.nonce !== action.nonce,
      ),
    );
  }
  private async mutation(
    path: string,
    method: string,
    kind: string,
    payload: Record<string, unknown>,
  ): Promise<Operation> {
    const allocated = await this.createAction();
    const action = {
      namespace: allocated.namespace.toString(),
      nonce: allocated.nonce,
    };
    const pending = {
      action,
      kind,
      submittedAt: new Date().toISOString(),
      request: validateIntent(kind, { path, method, payload }),
    };
    this.savePending([...this.pending(), pending]);
    try {
      const operation = parseOperation(
        await this.request(path, method, { ...payload, action }),
      );
      this.savePending(
        this.pending().map((item) =>
          item.action.namespace === action.namespace &&
          item.action.nonce === action.nonce
            ? { ...item, operationId: operation.id }
            : item,
        ),
      );
      return operation;
    } catch (error) {
      if (
        error instanceof APIError &&
        [400, 404, 409, 429].includes(error.status)
      ) {
        this.forget(action);
        throw error;
      }
      throw new OutcomeUnknown(action, error);
    }
  }
  async retryPending(pending: PendingAction): Promise<Operation> {
    let operation: Operation;
    try {
      operation = await this.queryAction(pending.action);
    } catch (error) {
      if (!(error instanceof APIError) || error.status !== 404) throw error;
      try {
        operation = parseOperation(
          await this.request(pending.request.path, pending.request.method, {
            ...pending.request.payload,
            action: pending.action,
          }),
        );
      } catch (cause) {
        throw new OutcomeUnknown(pending.action, cause);
      }
    }
    this.savePending(
      this.pending().map((item) =>
        item.action.namespace === pending.action.namespace &&
        item.action.nonce === pending.action.nonce
          ? { ...item, operationId: operation.id }
          : item,
      ),
    );
    return operation;
  }
  createVolume(name: string, quotaBytes: bigint) {
    return this.mutation("/volumes", "POST", "create", {
      name,
      quotaBytes: validBytes(quotaBytes),
    });
  }
  renameVolume(volume: Volume, name: string) {
    return this.mutation(
      `/volumes/${encodeURIComponent(volume.id)}`,
      "PATCH",
      "rename",
      { name, expectedRevision: volume.revision.toString() },
    );
  }
  setQuota(volume: Volume, quotaBytes: bigint) {
    if (!volume.summary)
      throw new Error("当前配额版本未知，请先取得权威统计。");
    return this.mutation(
      `/volumes/${encodeURIComponent(volume.id)}/quota`,
      "PUT",
      "quota",
      {
        quotaBytes: validBytes(quotaBytes),
        expectedRevision: volume.summary.quotaRevision.toString(),
      },
    );
  }
  deleteVolume(volume: Volume) {
    return this.mutation(
      `/volumes/${encodeURIComponent(volume.id)}`,
      "DELETE",
      "delete",
      { expectedRevision: volume.revision.toString() },
    );
  }
}
const validBytes = (value: bigint) => {
  if (value <= 0n || value > INT64_MAX)
    throw new Error("容量必须大于零且不超过 int64 字节上限。");
  return value.toString();
};
