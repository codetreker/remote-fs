import {
  parseV5,
  stringifyV5,
  uint64,
  int64,
  validatedNumber,
  v5Uint64,
  v5Int64,
  v5SmallNumber,
} from "./v5codec";
import { decodeRawLeaf, rawNameFromBase64 } from "./rawnames";
import { sha256 } from "@noble/hashes/sha2.js";
import type {
  DirectoryRef,
  DirectoryListing,
  NodeAttr,
  FileEntry,
  DurableAction,
  ImportReceipt,
  PreparedDownload,
  DownloadWriter,
  FileSessionPolicy,
} from "./file-types";
export type {
  DirectoryRef,
  DirectoryListing,
  NodeAttr,
  FileEntry,
  DurableAction,
  ImportReceipt,
  PreparedDownload,
  DownloadWriter,
  FileSessionPolicy,
} from "./file-types";

type Wire = Record<string, unknown>;
const MAX_JSON_BYTES = 16 * 1024 * 1024;
const HASH_CHUNK_BYTES = 1024 * 1024;
const CONTROL_OPS = new Set([
  "file.renew",
  "file.query-action",
  "file.session-close",
  "file.session-release-result",
  "file.close",
]);
const FILE_RESULTS: Record<string, string[]> = {
  "file.session-open": ["session", "status", "capabilities", "sessionIdentity"],
  "file.renew": ["status"],
  "file.read-dir-node": ["directory"],
  "file.query-action": ["actionReceipt"],
  "file.mutate-name": ["attr", "barrier"],
  "file.session-close": ["closeResult", "barrier"],
  "file.session-release-result": ["closeResult", "barrier"],
};
const ERRNOS = new Set([
  "EACCES",
  "EAGAIN",
  "EBADF",
  "EBUSY",
  "EDEADLK",
  "EDQUOT",
  "EEXIST",
  "EFBIG",
  "EINTR",
  "EINVAL",
  "EIO",
  "EISDIR",
  "ELOOP",
  "EMFILE",
  "EMLINK",
  "ENAMETOOLONG",
  "ENFILE",
  "ENODEV",
  "ENOENT",
  "ENOLCK",
  "ENOMEM",
  "ENOSPC",
  "ENOSYS",
  "ENOTDIR",
  "ENOTEMPTY",
  "ENXIO",
  "EOPNOTSUPP",
  "EOVERFLOW",
  "EPERM",
  "EROFS",
  "ESTALE",
  "ETXTBSY",
  "EXDEV",
]);
const CAPABILITY_ERRNOS: Record<string, string> = {
  "use-conflict": "EAGAIN",
  "range-conflict": "EAGAIN",
  "pending-delete": "EBUSY",
  "condition-conflict": "EAGAIN",
  "invalid-scope": "ESTALE",
};
const LOCK_ERRNOS: Record<string, string> = {
  invalid: "EINVAL",
  unsupportedTarget: "EOPNOTSUPP",
  conflict: "EBUSY",
  alreadyHeld: "EBUSY",
  requestMismatch: "EINVAL",
  capacity: "EAGAIN",
  retired: "ESTALE",
  outcomeUnknown: "EIO",
  staleResource: "ESTALE",
  staleGrant: "ESTALE",
  unrelatedProof: "ESTALE",
  recovering: "EAGAIN",
  unavailable: "EIO",
};

export class FileAPIError extends Error {
  constructor(
    message: string,
    readonly errno?: string,
    readonly unknownOutcome = false,
    readonly response?: Wire,
  ) {
    super(message);
    this.name = "FileAPIError";
  }
}

export class TransferOutcomeError extends FileAPIError {
  constructor(
    message: string,
    readonly action: DurableAction,
    readonly receipt?: ImportReceipt,
  ) {
    super(
      message,
      receipt?.failure,
      !receipt || !["completed", "not-executed"].includes(receipt.outcome),
    );
    this.name = "TransferOutcomeError";
  }
}

const object = (value: unknown, label: string): Wire => {
  if (value === null || typeof value !== "object" || Array.isArray(value))
    throw new FileAPIError(`Invalid ${label}`);
  return value as Wire;
};
const string = (value: unknown, label: string): string => {
  if (typeof value !== "string") throw new FileAPIError(`Missing ${label}`);
  return value;
};
const capability = (value: unknown): string => {
  const token = string(value, "capability");
  if (!/^[a-f0-9]{64}$/.test(token))
    throw new FileAPIError("Invalid capability");
  return token;
};
const positiveID = (value: unknown, label: string): bigint => {
  const id = uint64(value, label);
  if (id === 0n) throw new FileAPIError(`Invalid ${label}`);
  return id;
};
const bounded = (value: number) => v5SmallNumber(value, 0, 65536);
const target = (ref: DirectoryRef): Wire => ({
  NodeID: v5Uint64(ref.nodeId),
  ...(ref.scope === undefined ? {} : { Scope: { Token: ref.scope } }),
});
const condition = (state: number, nodeId = 0n): Wire => ({
  state: bounded(state),
  nodeId: v5Uint64(nodeId),
});
const actionWire = (action: DurableAction): Wire => ({
  SubjectID: action.subjectId,
  Namespace: v5Uint64(action.namespace),
  Nonce: action.nonce,
});
const sameAction = (left: DurableAction, right: DurableAction) =>
  left.subjectId === right.subjectId &&
  left.namespace === right.namespace &&
  left.nonce === right.nonce;
const actionKey = (action: DurableAction) =>
  `${action.subjectId}\0${action.namespace}\0${action.nonce}`;
const settled = (value: unknown): boolean => {
  if (value === undefined) return false;
  const result = object(value, "close result");
  return (
    result.released === true &&
    result.determined === true &&
    result.barrierPending !== true
  );
};
const rawLeaf = (value: string): string => {
  const bytes = rawNameFromBase64(value);
  if (
    bytes.length === 0 ||
    bytes.includes(0) ||
    bytes.includes(47) ||
    (bytes.length === 1 && bytes[0] === 46) ||
    (bytes.length === 2 && bytes[0] === 46 && bytes[1] === 46)
  ) {
    throw new FileAPIError("Invalid raw leaf");
  }
  return value;
};
const randomNonce = (): string =>
  Array.from(crypto.getRandomValues(new Uint8Array(16)), (byte) =>
    byte.toString(16).padStart(2, "0"),
  ).join("");
const decodeBytes = (value: unknown, label: string): Uint8Array => {
  const encoded = string(value, label);
  let binary: string;
  try {
    binary = atob(encoded);
  } catch {
    throw new FileAPIError(`Invalid ${label}`);
  }
  if (btoa(binary) !== encoded) throw new FileAPIError(`Noncanonical ${label}`);
  return Uint8Array.from(binary, (byte) => byte.charCodeAt(0));
};
const byteArrayWire = (bytes: Uint8Array) =>
  Array.from(bytes, (byte) => v5SmallNumber(byte, 0, 255));
const goJSON = (value: Wire) =>
  stringifyV5(value).replace(
    /[<>&\u2028\u2029]/g,
    (character) =>
      `\\u${character.charCodeAt(0).toString(16).padStart(4, "0")}`,
  );

const decodeAttr = (value: unknown): NodeAttr => {
  const attr = object(value, "node attributes");
  const kind = validatedNumber(attr.kind, 1, 3, "node kind");
  const size = int64(attr.size, "file size");
  const allocationSize = int64(attr.allocation_size, "allocation size");
  if (
    size < 0n ||
    allocationSize < 0n ||
    typeof attr.allocation_known !== "boolean"
  )
    throw new FileAPIError("Invalid node size");
  const time = object(attr.mod_time, "modification time");
  const seconds = int64(time.unix_sec, "modification seconds");
  const nanos = validatedNumber(
    time.nanos,
    0,
    999999999,
    "modification nanoseconds",
  );
  const millis = seconds * 1000n + BigInt(Math.floor(nanos / 1_000_000));
  const modifiedAt =
    millis < -8_640_000_000_000_000n || millis > 8_640_000_000_000_000n
      ? `${seconds}s + ${nanos}ns`
      : new Date(Number(millis)).toISOString();
  return {
    id: positiveID(attr.id, "node ID"),
    kind: (["file", "directory", "symlink"] as const)[kind - 1],
    size,
    allocationSize,
    allocationKnown: attr.allocation_known,
    modifiedAt,
  };
};

export class FileClient {
  private readonly base: string;
  private session?: string;
  private epoch = 0n;
  private nativeEpoch = 0n;
  private root?: DirectoryRef;
  private opening?: Promise<DirectoryRef>;
  private renewal?: ReturnType<typeof setInterval>;
  private renewalFailure?: unknown;
  private closeAction?: string;
  private closeGeneration = 1n;
  private readonly pending = new Map<string, Wire>();
  private readonly imports = new Map<
    string,
    {
      action: DurableAction;
      token: string;
      fingerprint?: string;
      bytes?: bigint;
    }
  >();
  private readonly exports = new Map<string, PreparedDownload>();

  constructor(
    private readonly volumeId: string,
    private readonly csrfToken: string,
    private readonly onAuthenticationLost: () => void,
    private readonly fetchImpl: typeof fetch,
    private readonly policy: FileSessionPolicy,
    private readonly onPermissionDenied?: () => void,
  ) {
    this.base = `/api/volumes/${encodeURIComponent(volumeId)}/v5/`;
    this.fetchImpl = fetchImpl.bind(globalThis);
  }

  private async response(response: Response): Promise<Wire> {
    if (response.status === 401) {
      this.stopRenewal();
      this.onAuthenticationLost();
      throw new FileAPIError(
        "Your session has expired. Sign in again.",
        "EACCES",
        true,
      );
    }
    if (response.headers.get("Remote-Fs-Protocol") !== "5")
      throw new FileAPIError(
        "The server returned an unverified file response.",
        undefined,
        true,
      );
    const reader = response.body?.getReader();
    if (!reader)
      throw new FileAPIError("The file response has no body.", undefined, true);
    const chunks: Uint8Array[] = [];
    let length = 0;
    try {
      for (;;) {
        const part = await reader.read();
        if (part.done) break;
        length += part.value.byteLength;
        if (length > MAX_JSON_BYTES)
          throw new FileAPIError(
            "The file response exceeds the client limit.",
            undefined,
            true,
          );
        chunks.push(part.value);
      }
    } catch (error) {
      try {
        await reader.cancel(error);
      } catch {
        /* Failed response reads remain unknown even when stream cancellation also fails. */
      }
      if (error instanceof FileAPIError) throw error;
      throw new FileAPIError(
        error instanceof Error
          ? error.message
          : "The file response was interrupted.",
        undefined,
        true,
      );
    }
    const bytes = new Uint8Array(length);
    let offset = 0;
    for (const chunk of chunks) {
      bytes.set(chunk, offset);
      offset += chunk.byteLength;
    }
    let body: Wire;
    try {
      body = object(
        parseV5(new TextDecoder("utf-8", { fatal: true }).decode(bytes)),
        "file response",
      );
    } catch (cause) {
      throw new FileAPIError(
        `Invalid file response: ${cause instanceof Error ? cause.message : "decode failed"}`,
        undefined,
        true,
      );
    }
    if (response.status === 422) {
      const errno =
        typeof body.errno === "string" && ERRNOS.has(body.errno)
          ? body.errno
          : undefined;
      const hasCapability = Object.hasOwn(body, "capabilityCode");
      const hasLock = Object.hasOwn(body, "lockCode");
      const hasLockRecorded = Object.hasOwn(body, "recorded");
      const capabilityValid =
        !hasCapability ||
        (typeof body.capabilityCode === "string" &&
          CAPABILITY_ERRNOS[body.capabilityCode] === errno);
      const lockValid =
        hasLock === hasLockRecorded &&
        (!hasLock ||
          (typeof body.lockCode === "string" &&
            LOCK_ERRNOS[body.lockCode] === errno &&
            typeof body.recorded === "boolean"));
      const determined =
        errno !== undefined &&
        body.fileRecorded === true &&
        capabilityValid &&
        lockValid &&
        !(hasCapability && hasLock) &&
        (body.message === undefined || typeof body.message === "string");
      const classified =
        errno !== undefined &&
        capabilityValid &&
        lockValid &&
        !(hasCapability && hasLock) &&
        (body.fileRecorded === undefined || body.fileRecorded === true) &&
        (body.message === undefined || typeof body.message === "string");
      if (classified && errno === "EACCES" && this.onPermissionDenied)
        queueMicrotask(this.onPermissionDenied);
      throw new FileAPIError(
        typeof body.message === "string"
          ? body.message
          : "File operation failed",
        errno,
        !determined,
        body,
      );
    }
    if (response.status !== 200)
      throw new FileAPIError(
        `File request failed (${response.status})`,
        undefined,
        true,
        body,
      );
    return body;
  }

  private async request(
    endpoint: string,
    body: Wire,
    signal?: AbortSignal,
  ): Promise<Wire> {
    let response: Response;
    try {
      response = await this.fetchImpl(this.base + endpoint, {
        method: "POST",
        credentials: "same-origin",
        headers: {
          "Content-Type": "application/json",
          "X-RFS-Request": "1",
          "X-CSRF-Token": this.csrfToken,
        },
        body: stringifyV5(body),
        signal,
        redirect: "error",
      });
    } catch (error) {
      throw new FileAPIError(
        error instanceof Error ? error.message : "File request failed",
        undefined,
        true,
      );
    }
    return this.response(response);
  }

  private async file(
    op: string,
    fields: Wire = {},
    signal?: AbortSignal,
  ): Promise<Wire> {
    if (op !== "file.session-open" && !this.session)
      throw new FileAPIError("The file session is not open.");
    const response = await this.request(
      CONTROL_OPS.has(op) ? "file-control" : "file",
      {
        op,
        path: "",
        data: "",
        ...(op === "file.session-open" ? {} : { session: this.session }),
        ...(op === "file.mutate-name"
          ? { resultBytes: v5Int64(BigInt(MAX_JSON_BYTES)) }
          : {}),
        ...fields,
      },
      signal,
    );
    try {
      positiveID(response.epoch, "file response action epoch");
      if (
        response.data !== undefined &&
        response.data !== null &&
        response.data !== ""
      )
        throw new FileAPIError("File response carries unrelated data.");
      if (response.retry !== undefined && typeof response.retry !== "boolean")
        throw new FileAPIError("Invalid file retry marker.");
      const retry = response.retry === true;
      if (retry && op !== "file.mutate-name" && op !== "file.session-close")
        throw new FileAPIError("Unexpected file action retry.");
      const allowed = new Set([
        "epoch",
        "data",
        "retry",
        ...(retry ? [] : FILE_RESULTS[op]),
      ]);
      if (Object.keys(response).some((key) => !allowed.has(key)))
        throw new FileAPIError("File response carries unrelated results.");
      return response;
    } catch (cause) {
      throw new FileAPIError(
        cause instanceof Error ? cause.message : "Invalid file response.",
        undefined,
        true,
      );
    }
  }

  private async transfer(body: Wire, signal?: AbortSignal): Promise<Wire> {
    return this.request(
      body.op === "begin" || body.op === "export"
        ? "file-transfer"
        : "file-transfer-control",
      body,
      signal,
    );
  }

  private stopRenewal(): void {
    if (this.renewal !== undefined) clearInterval(this.renewal);
    this.renewal = undefined;
  }
  private newAction(epoch = this.epoch): string {
    if (epoch === 0n) throw new FileAPIError("Missing file action epoch");
    return `${epoch}:${randomNonce()}`;
  }

  private async renew(): Promise<void> {
    const result = await this.file("file.renew");
    const renewed = object(result.status, "session status");
    if (
      renewed.Retired === true ||
      renewed.Fenced === true ||
      int64(renewed.Remaining, "session lease") <= 0n
    )
      throw new FileAPIError("File session continuity was lost.");
    this.epoch = positiveID(result.epoch, "transport action epoch");
    this.nativeEpoch = positiveID(renewed.ActionEpoch, "native action epoch");
  }

  async open(): Promise<DirectoryRef> {
    if (this.opening) return this.opening;
    if (this.root) return this.root;
    this.opening = this.openSession();
    try {
      return await this.opening;
    } finally {
      this.opening = undefined;
    }
  }

  private async openSession(): Promise<DirectoryRef> {
    const p = this.policy;
    const response = await this.file("file.session-open", {
      options: {
        Lease: v5Int64(p.leaseNanoseconds),
        History: v5Int64(p.historyNanoseconds),
        MaxFileSize: v5Int64(p.maxFileSize),
        MaxFiles: v5Int64(p.maxFiles),
        MaxOperations: v5Int64(p.maxOperations),
        MaxWaiters: v5Int64(p.maxWaiters),
        MaxLockOwners: v5Int64(p.maxLockOwners),
        MaxLockRanges: v5Int64(p.maxLockRanges),
        MaxPendingLocks: v5Int64(p.maxPendingLocks),
        MaxLockActions: v5Int64(p.maxLockActions),
        MaxCloseActions: v5Int64(p.maxCloseActions),
      },
    });
    this.session = capability(response.session);
    this.epoch = positiveID(response.epoch, "action epoch");
    const capabilities = object(response.capabilities, "file capabilities");
    if (
      capabilities.sessionIdentity !== true ||
      capabilities.namespace !== true ||
      capabilities.actions !== true
    )
      throw new FileAPIError(
        "The authority does not support identity-conditioned file management.",
        "EOPNOTSUPP",
      );
    const identity = object(response.sessionIdentity, "session identity");
    const backend = object(identity.backend, "backend identity");
    if (
      backend.volume !== this.volumeId ||
      string(backend.authority, "authority identity").length === 0
    )
      throw new FileAPIError(
        "The session identity does not belong to the requested volume.",
      );
    const root = { nodeId: positiveID(backend.rootNodeId, "root node ID") };
    const status = object(response.status, "session status");
    if (
      status.Retired !== false ||
      status.Fenced !== false ||
      int64(status.Remaining, "session lease") <= 0n
    )
      throw new FileAPIError("The authority did not provide a live session.");
    if (
      string(identity.sessionEpoch, "session epoch").length === 0 ||
      identity.sessionEpoch !== status.Epoch
    )
      throw new FileAPIError(
        "The session identity disagrees with its current status.",
      );
    this.nativeEpoch = positiveID(status.ActionEpoch, "native action epoch");
    this.root = root;
    this.renewal = setInterval(
      () => {
        void this.renew().catch((error) => {
          this.renewalFailure = error;
          this.stopRenewal();
        });
      },
      Math.max(1, Number(p.leaseNanoseconds / 3_000_000n)),
    );
    return this.root;
  }

  async list(parent: DirectoryRef): Promise<DirectoryListing> {
    if (this.renewalFailure) throw this.renewalFailure;
    const response = await this.file("file.read-dir-node", {
      directory: target(parent),
      resultBytes: v5Int64(BigInt(MAX_JSON_BYTES)),
    });
    const directory = object(response.directory, "directory observation");
    const observation = object(directory.observation, "directory revision");
    if (positiveID(observation.parentId, "observed parent") !== parent.nodeId)
      throw new FileAPIError("Directory response substituted another parent.");
    const revision = string(observation.revision, "directory revision");
    if (
      decodeBytes(revision, "directory revision").length === 0 ||
      !Array.isArray(directory.entries)
    )
      throw new FileAPIError("Directory response is incomplete.");
    const names = new Set<string>();
    const ids = new Set<bigint>();
    const entries = directory.entries.map((value) => {
      const entry = object(value, "directory entry");
      const raw = decodeRawLeaf(rawLeaf(string(entry.rawLeaf, "raw leaf")));
      const attr = decodeAttr(entry.attr);
      if (names.has(raw.rawLeaf) || ids.has(attr.id))
        throw new FileAPIError(
          "Directory response contains duplicate entries.",
        );
      names.add(raw.rawLeaf);
      ids.add(attr.id);
      return { ...raw, attr };
    });
    return { parent, revision, entries };
  }

  private async reconcileAction(action: string, request: Wire): Promise<void> {
    const response = await this.file("file.query-action", {
      fileAction: action,
    });
    const receipt = object(response.actionReceipt, "file action receipt");
    if (receipt.Action !== action || receipt.Operation !== request.op)
      throw new FileAPIError(
        "File action query returned an unrelated receipt.",
        undefined,
        true,
      );
    const outcome = validatedNumber(
      receipt.Outcome,
      1,
      5,
      "file action outcome",
    );
    if (outcome !== 2 && outcome !== 3)
      throw new FileAPIError(
        "A previous file operation has an undetermined outcome. Refresh its status before another change.",
        undefined,
        true,
      );
    try {
      const replay = await this.file(string(request.op, "operation"), request);
      if (replay.retry === true)
        throw new FileAPIError(
          "The retained file action cannot be replayed.",
          undefined,
          true,
        );
      this.checkNameResult(object(request.name, "name command"), replay);
      this.pending.delete(action);
    } catch (error) {
      if (error instanceof FileAPIError && !error.unknownOutcome)
        this.pending.delete(action);
      throw error;
    }
  }

  private checkNameResult(name: Wire, response: Wire): void {
    const kind = validatedNumber(name.kind, 1, 6, "name operation");
    if (kind !== 2 && kind !== 5) return;
    try {
      const attr = decodeAttr(response.attr);
      if (
        (kind === 2 && attr.kind !== "directory") ||
        (kind === 5 &&
          attr.id !== uint64(object(name.target, "name condition").nodeId))
      )
        throw new FileAPIError("Name result substituted another node.");
    } catch (cause) {
      throw new FileAPIError(
        cause instanceof Error
          ? cause.message
          : "The name result is incomplete.",
        undefined,
        true,
      );
    }
  }

  async reconcile(): Promise<void> {
    for (const [action, request] of this.pending) {
      try {
        await this.reconcileAction(action, request);
      } catch (error) {
        if (!(error instanceof FileAPIError) || error.unknownOutcome)
          throw error;
      }
    }
  }

  private async mutate(name: Wire): Promise<void> {
    if (this.renewalFailure) throw this.renewalFailure;
    await this.reconcile();
    await this.renew();
    let action = this.newAction(this.nativeEpoch);
    let request = { op: "file.mutate-name", action, name: { ...name, action } };
    try {
      let response = await this.file("file.mutate-name", request);
      if (response.retry === true) {
        this.epoch = positiveID(response.epoch, "action epoch");
        await this.renew();
        action = this.newAction(this.nativeEpoch);
        request = { ...request, action, name: { ...name, action } };
        response = await this.file("file.mutate-name", request);
        if (response.retry === true)
          throw new FileAPIError(
            "File action admission could not be obtained.",
            "EAGAIN",
          );
      }
      this.checkNameResult(name, response);
    } catch (error) {
      if (!(error instanceof FileAPIError) || !error.unknownOutcome)
        throw error;
      this.pending.set(action, request);
      await this.reconcileAction(action, request);
    }
  }

  async mkdir(parent: DirectoryListing, name: string): Promise<void> {
    await this.mutate({
      kind: bounded(2),
      name: { parent: target(parent.parent), rawLeaf: rawLeaf(name) },
      target: condition(2),
      initial: { attr: {} },
    });
  }

  async rename(
    parent: DirectoryListing,
    entry: FileEntry,
    name: string,
  ): Promise<void> {
    await this.move(parent, entry, parent, name);
  }

  async move(
    source: DirectoryListing,
    entry: FileEntry,
    destination: DirectoryListing,
    name: string,
  ): Promise<void> {
    this.selected(source, entry);
    const leaf = rawLeaf(name);
    await this.mutate({
      kind: bounded(5),
      name: { parent: target(source.parent), rawLeaf: entry.rawLeaf },
      target: condition(3, entry.attr.id),
      destination: {
        parent: target(destination.parent),
        observedLeaf: leaf,
        expected: condition(2),
        outputLeaf: leaf,
      },
      initial: { attr: {} },
    });
  }

  async remove(parent: DirectoryListing, entry: FileEntry): Promise<void> {
    this.selected(parent, entry);
    await this.mutate({
      kind: bounded(entry.attr.kind === "directory" ? 4 : 3),
      name: { parent: target(parent.parent), rawLeaf: entry.rawLeaf },
      target: condition(3, entry.attr.id),
      initial: { attr: {} },
    });
  }

  private selected(parent: DirectoryListing, entry: FileEntry): void {
    if (
      !parent.entries.some(
        (candidate) =>
          candidate.rawLeaf === entry.rawLeaf &&
          candidate.attr.id === entry.attr.id,
      )
    )
      throw new FileAPIError(
        "The selected item does not belong to this directory observation.",
      );
  }

  async limits(): Promise<{ maxBytes: bigint }> {
    const response = await this.transfer({ op: "check" });
    const caps = object(response.capabilities, "transfer capabilities");
    const maxBytes = int64(caps.max_bytes, "transfer byte limit");
    if (maxBytes <= 0n) throw new FileAPIError("Invalid transfer byte limit");
    return { maxBytes };
  }

  private receipt(
    value: unknown,
    action: DurableAction,
    close?: unknown,
  ): ImportReceipt {
    const receipt = object(value, "import receipt");
    const key = object(receipt.action, "import action");
    const returned = {
      subjectId: string(key.SubjectID, "subject ID"),
      namespace: positiveID(key.Namespace, "action namespace"),
      nonce: string(key.Nonce, "action nonce"),
    };
    if (!sameAction(returned, action))
      throw new FileAPIError(
        "Import returned an unrelated action.",
        undefined,
        true,
      );
    const outcome = validatedNumber(receipt.outcome, 1, 5, "import outcome");
    const outcomes = [
      "pending",
      "completed",
      "not-executed",
      "unknown",
      "history-expired",
    ] as const;
    const result: ImportReceipt = {
      action: returned,
      outcome: outcomes[outcome - 1],
      settled: settled(close),
    };
    if (outcome === 2) {
      result.attr = decodeAttr(receipt.attr);
      if (result.attr.kind !== "file" || receipt.failure !== undefined)
        throw new FileAPIError(
          "Invalid completed import receipt.",
          undefined,
          true,
        );
    } else if (receipt.attr !== undefined)
      throw new FileAPIError(
        "Uncompleted import returned attributes.",
        undefined,
        true,
      );
    if (receipt.failure !== undefined)
      result.failure = string(receipt.failure, "import failure");
    if (result.failure !== undefined && outcome !== 3)
      throw new FileAPIError(
        "Invalid import failure receipt.",
        undefined,
        true,
      );
    const fingerprint = receipt.fingerprint;
    if (!Array.isArray(fingerprint) || fingerprint.length !== 32)
      throw new FileAPIError("Invalid import fingerprint.", undefined, true);
    const fingerprintText = fingerprint
      .map((byte) =>
        validatedNumber(byte, 0, 255, "fingerprint byte")
          .toString(16)
          .padStart(2, "0"),
      )
      .join("");
    const held = this.imports.get(actionKey(action));
    if (held?.fingerprint !== undefined && held.fingerprint !== fingerprintText)
      throw new FileAPIError(
        "Import receipt changed the admitted command.",
        undefined,
        true,
      );
    if (
      held?.bytes !== undefined &&
      result.attr &&
      result.attr.size !== held.bytes
    )
      throw new FileAPIError(
        "Import receipt changed the admitted size.",
        undefined,
        true,
      );
    return result;
  }

  async queryUpload(action: DurableAction): Promise<ImportReceipt> {
    const response = await this.transfer({
      op: "query",
      action: actionWire(action),
    });
    return this.receipt(response.receipt, action, response.close);
  }

  private async publish(
    action: DurableAction,
    token: string,
  ): Promise<ImportReceipt> {
    let response: Wire;
    try {
      response = await this.transfer({ op: "publish", token });
    } catch (cause) {
      let receipt: ImportReceipt;
      try {
        receipt = await this.queryUpload(action);
      } catch {
        throw new TransferOutcomeError(
          "Upload publication could not be confirmed. Query the original action before retrying.",
          action,
        );
      }
      if (
        receipt.outcome === "completed" ||
        receipt.outcome === "not-executed"
      ) {
        await this.settleImport(action, token);
        receipt.settled = true;
        if (receipt.outcome === "completed") return receipt;
      }
      throw new TransferOutcomeError(
        cause instanceof Error ? cause.message : "Upload failed",
        action,
        receipt,
      );
    }
    const receipt = this.receipt(
      response.receipt,
      action,
      response.settlement === undefined ? response.close : undefined,
    );
    if (
      response.error !== undefined ||
      receipt.outcome !== "completed" ||
      !receipt.settled
    ) {
      if (
        receipt.outcome === "completed" ||
        receipt.outcome === "not-executed"
      ) {
        await this.settleImport(action, token);
        receipt.settled = true;
      }
      if (
        response.error === undefined &&
        receipt.outcome === "completed" &&
        receipt.settled
      )
        return receipt;
      throw new TransferOutcomeError(
        receipt.failure ?? "Upload publication is not confirmed.",
        action,
        receipt,
      );
    }
    this.imports.delete(actionKey(action));
    return receipt;
  }

  private async settleImport(
    action: DurableAction,
    token: string,
  ): Promise<void> {
    const response = await this.transfer({ op: "cancel", token });
    if (
      response.error !== undefined ||
      response.settlement !== undefined ||
      !settled(response.close)
    )
      throw new TransferOutcomeError(
        "Upload cleanup is not confirmed.",
        action,
      );
    this.imports.delete(actionKey(action));
  }

  async upload(
    file: File,
    parent: DirectoryListing,
    name: string,
    action: DurableAction,
    replace?: FileEntry,
    onProgress?: (loaded: number, total: number) => void,
    signal?: AbortSignal,
  ): Promise<ImportReceipt> {
    const { maxBytes } = await this.limits();
    if (BigInt(file.size) > maxBytes)
      throw new FileAPIError(
        `The file exceeds the service transfer limit (${maxBytes} bytes).`,
        "EFBIG",
      );
    const leaf = rawLeaf(name);
    if (replace) {
      this.selected(parent, replace);
      if (replace.rawLeaf !== leaf || replace.attr.kind === "directory")
        throw new FileAPIError(
          "Replacement requires the explicitly selected file or symbolic link.",
        );
    }
    const hasher = sha256.create();
    for (let offset = 0; offset < file.size; offset += HASH_CHUNK_BYTES) {
      signal?.throwIfAborted();
      hasher.update(
        new Uint8Array(
          await file
            .slice(offset, Math.min(file.size, offset + HASH_CHUNK_BYTES))
            .arrayBuffer(),
        ),
      );
    }
    signal?.throwIfAborted();
    const digest = hasher.digest();
    const command: Wire = {
      Action: actionWire(action),
      Fingerprint: byteArrayWire(new Uint8Array(32)),
      Target: {
        Name: { Parent: target(parent.parent), RawLeaf: leaf },
        Guards: {
          Directories: [
            {
              ParentID: v5Uint64(parent.parent.nodeId),
              Revision: parent.revision,
            },
          ],
          RootID: v5Uint64(0n),
        },
      },
      Expected: {
        State: bounded(replace ? 3 : 2),
        NodeID: v5Uint64(replace?.attr.id ?? 0n),
      },
      Mode: bounded(replace ? 2 : 1),
      DeclaredBytes: v5Int64(BigInt(file.size)),
      Digest: byteArrayWire(digest),
    };
    const fingerprintCommand = {
      ...command,
      Action: { SubjectID: "", Namespace: v5Uint64(0n), Nonce: "" },
    };
    const fingerprint = sha256(
      new TextEncoder().encode(
        `remote-fs/import/v1\0${goJSON(fingerprintCommand)}`,
      ),
    );
    command.Fingerprint = byteArrayWire(fingerprint);
    let begun: Wire;
    try {
      begun = await this.transfer({ op: "begin", command }, signal);
    } catch (cause) {
      if (!(cause instanceof FileAPIError) || !cause.unknownOutcome)
        throw cause;
      const receipt = await this.queryUpload(action).catch(() => undefined);
      if (receipt?.outcome !== "pending")
        throw new TransferOutcomeError(
          "Upload admission could not be confirmed. Query the original action before retrying.",
          action,
          receipt,
        );
      // A pending exact action is already admitted; the identical begin recovers its volatile token.
      begun = await this.transfer({ op: "begin", command });
    }
    const token = capability(begun.token);
    this.imports.set(actionKey(action), {
      action,
      token,
      fingerprint: Array.from(fingerprint, (byte) =>
        byte.toString(16).padStart(2, "0"),
      ).join(""),
      bytes: BigInt(file.size),
    });
    try {
      if (
        int64(begun.declared_bytes, "admitted upload length") !==
        BigInt(file.size)
      )
        throw new TransferOutcomeError(
          "The upload admission changed its declared length.",
          action,
        );
      signal?.throwIfAborted();
      onProgress?.(0, file.size);
      const received = await this.response(
        await this.fetchImpl(
          `${this.base}file-transfer-data?path=${encodeURIComponent(token)}`,
          {
            method: "POST",
            credentials: "same-origin",
            headers: {
              "Content-Type": "application/octet-stream",
              "X-RFS-Request": "1",
              "X-CSRF-Token": this.csrfToken,
            },
            body: file,
            signal,
            redirect: "error",
          },
        ),
      );
      const seal = object(received.seal, "upload seal");
      if (
        int64(seal.Bytes, "sealed bytes") !== BigInt(file.size) ||
        !Array.isArray(seal.Digest) ||
        seal.Digest.length !== 32 ||
        seal.Digest.some(
          (byte, index) =>
            validatedNumber(byte, 0, 255, "digest byte") !== digest[index],
        )
      ) {
        throw new FileAPIError(
          "The authority did not verify the complete upload.",
        );
      }
      onProgress?.(file.size, file.size);
    } catch (cause) {
      try {
        await this.settleImport(action, token);
      } catch {
        throw new TransferOutcomeError(
          "Upload reception failed and cleanup could not be confirmed. Cancel the original action.",
          action,
        );
      }
      throw cause;
    }
    if (signal?.aborted) {
      await this.settleImport(action, token);
      signal.throwIfAborted();
    }
    return this.publish(action, token);
  }

  async resumeUpload(action: DurableAction): Promise<ImportReceipt> {
    const receipt = await this.queryUpload(action);
    let held = this.imports.get(actionKey(action));
    if (receipt.outcome === "completed" || receipt.outcome === "not-executed") {
      if (!held) {
        const response = await this.transfer({
          op: "resume",
          action: actionWire(action),
        });
        held = { action, token: capability(response.token) };
        this.imports.set(actionKey(action), held);
      }
      await this.settleImport(action, held.token);
      receipt.settled = true;
      return receipt;
    }
    if (receipt.outcome !== "pending")
      throw new TransferOutcomeError(
        "This action cannot safely be resumed.",
        action,
        receipt,
      );
    if (!held) {
      const response = await this.transfer({
        op: "resume",
        action: actionWire(action),
      });
      held = { action, token: capability(response.token) };
      this.imports.set(actionKey(action), held);
    }
    return this.publish(action, held.token);
  }

  async cancelUpload(action: DurableAction): Promise<ImportReceipt> {
    const receipt = await this.queryUpload(action);
    let held = this.imports.get(actionKey(action));
    if (!held) {
      if (!["pending", "completed", "not-executed"].includes(receipt.outcome))
        throw new TransferOutcomeError(
          "Upload cancellation cannot be confirmed.",
          action,
          receipt,
        );
      const response = await this.transfer({
        op: "resume",
        action: actionWire(action),
      });
      held = { action, token: capability(response.token) };
      this.imports.set(actionKey(action), held);
    }
    await this.settleImport(action, held.token);
    const confirmed = await this.queryUpload(action);
    confirmed.settled = true;
    return confirmed;
  }

  async prepareDownload(entry: FileEntry): Promise<PreparedDownload> {
    if (entry.attr.kind !== "file")
      throw new FileAPIError(
        "Only regular files can be downloaded. Symbolic links are not followed.",
        "EOPNOTSUPP",
      );
    const response = await this.transfer({
      op: "export",
      export: { NodeID: v5Uint64(entry.attr.id) },
    });
    const token = capability(response.token);
    const prepared: PreparedDownload = {
      token,
      url: `${this.base}file-transfer-export?path=${encodeURIComponent(token)}`,
      filename: `file-${entry.attr.id}`,
      size: entry.attr.size,
      digest: new Uint8Array(32),
    };
    this.exports.set(token, prepared);
    try {
      const info = object(response.info, "verified export");
      const attr = decodeAttr(info.attr);
      const digest = decodeBytes(info.digest, "export digest");
      if (
        attr.id !== entry.attr.id ||
        attr.kind !== "file" ||
        digest.length !== 32
      )
        throw new FileAPIError(
          "The export did not capture the selected regular file.",
        );
      prepared.size = attr.size;
      prepared.digest = digest;
      return prepared;
    } catch (cause) {
      try {
        await this.closeDownload(prepared);
      } catch (cleanupError) {
        throw new AggregateError(
          [cause, cleanupError],
          "Invalid export response and cleanup could not be confirmed.",
        );
      }
      throw cause;
    }
  }

  async closeDownload(download: PreparedDownload): Promise<void> {
    if (!this.exports.has(download.token)) return;
    const response = await this.transfer({
      op: "close-export",
      token: download.token,
    });
    if (
      response.error !== undefined ||
      response.settlement !== undefined ||
      !settled(response.close)
    )
      throw new FileAPIError(
        "Download cleanup is not confirmed.",
        undefined,
        true,
      );
    this.exports.delete(download.token);
  }

  async download(
    prepared: PreparedDownload,
    writer?: DownloadWriter,
    signal?: AbortSignal,
  ): Promise<void> {
    if (this.exports.get(prepared.token) !== prepared)
      throw new FileAPIError(
        "The prepared download is not owned by this client.",
      );
    if (!writer) {
      const anchor = document.createElement("a");
      anchor.href = prepared.url;
      anchor.download = prepared.filename;
      document.body.append(anchor);
      anchor.click();
      anchor.remove();
      // This authenticated, single-use GET owns its bounded server cleanup; the browser owns completion reporting.
      this.exports.delete(prepared.token);
      return;
    }
    let reader: ReadableStreamDefaultReader<Uint8Array> | undefined;
    try {
      const response = await this.fetchImpl(prepared.url, {
        method: "GET",
        credentials: "same-origin",
        headers: { "X-RFS-Request": "1" },
        signal,
        redirect: "error",
      });
      if (
        response.status !== 200 ||
        response.headers.get("Remote-Fs-Protocol") !== "5"
      ) {
        await this.response(response);
        throw new FileAPIError("Unverified download response");
      }
      reader = response.body?.getReader();
      const length = response.headers.get("Content-Length");
      if (
        length === null ||
        !/^(0|[1-9][0-9]*)$/.test(length) ||
        BigInt(length) !== prepared.size
      )
        throw new FileAPIError(
          "Download length differs from its verified export.",
        );
      if (!reader) throw new FileAPIError("Download has no stream.");
      let received = 0n;
      const hasher = sha256.create();
      for (;;) {
        signal?.throwIfAborted();
        const part = await reader.read();
        if (part.done) break;
        received += BigInt(part.value.byteLength);
        if (received > prepared.size)
          throw new FileAPIError("Download exceeded its verified length.");
        hasher.update(part.value);
        await writer.write(part.value);
      }
      if (received !== prepared.size)
        throw new FileAPIError("Download was truncated.");
      if (
        hasher.digest().some((byte, index) => byte !== prepared.digest[index])
      )
        throw new FileAPIError(
          "Download content does not match its captured digest.",
        );
      signal?.throwIfAborted();
      await writer.close();
      await this.closeDownload(prepared);
    } catch (cause) {
      if (reader) await reader.cancel(cause).catch(() => undefined);
      try {
        await writer.abort(cause);
      } catch (abortError) {
        throw new AggregateError(
          [cause, abortError],
          "Download failed and local output cleanup failed.",
        );
      }
      try {
        await this.closeDownload(prepared);
      } catch (cleanupError) {
        throw new AggregateError(
          [cause, cleanupError],
          "Download failed and export cleanup could not be confirmed.",
        );
      }
      throw cause;
    }
  }

  async close(): Promise<void> {
    if (this.opening) {
      try {
        await this.opening;
      } catch {
        /* A captured capability, if any, is still closed below. */
      }
    }
    this.stopRenewal();
    await this.reconcile();
    for (const held of this.imports.values()) {
      const receipt = await this.cancelUpload(held.action);
      if (!["completed", "not-executed"].includes(receipt.outcome))
        throw new TransferOutcomeError(
          "Upload cleanup completed but its publication outcome remains unknown.",
          held.action,
          receipt,
        );
    }
    for (const download of this.exports.values())
      await this.closeDownload(download);
    if (!this.session) return;
    if (!this.closeAction && !this.renewalFailure) await this.renew();
    this.closeAction ??= this.newAction();
    let response: Wire;
    try {
      response = await this.file("file.session-close", {
        action: this.closeAction,
        closeGeneration: v5Uint64(this.closeGeneration),
      });
    } catch (cause) {
      try {
        response = await this.file("file.session-release-result");
      } catch {
        throw new FileAPIError(
          cause instanceof Error
            ? cause.message
            : "File session cleanup is not confirmed.",
          undefined,
          true,
        );
      }
    }
    if (response.retry === true) {
      this.epoch = positiveID(response.epoch, "cleanup epoch");
      this.closeAction = this.newAction();
      response = await this.file("file.session-close", {
        action: this.closeAction,
        closeGeneration: v5Uint64(this.closeGeneration),
      });
    }
    if (!settled(response.closeResult))
      throw new FileAPIError(
        "File session cleanup is not confirmed.",
        undefined,
        true,
      );
    this.session = undefined;
    this.root = undefined;
    this.renewalFailure = undefined;
  }
}
