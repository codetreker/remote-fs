export type DirectoryRef = { nodeId: bigint; scope?: string };

export type FileSessionPolicy = {
  leaseNanoseconds: bigint;
  historyNanoseconds: bigint;
  maxFileSize: bigint;
  maxFiles: bigint;
  maxOperations: bigint;
  maxWaiters: bigint;
  maxLockOwners: bigint;
  maxLockRanges: bigint;
  maxPendingLocks: bigint;
  maxLockActions: bigint;
  maxCloseActions: bigint;
};

export type NodeAttr = {
  id: bigint;
  kind: "file" | "directory" | "symlink";
  size: bigint;
  allocationSize: bigint;
  allocationKnown: boolean;
  modifiedAt: string;
};

export type FileEntry = {
  rawLeaf: string;
  rawBytes: Uint8Array;
  displayName: string;
  attr: NodeAttr;
};

export type DirectoryListing = {
  parent: DirectoryRef;
  revision: string;
  entries: FileEntry[];
};

export type DurableAction = {
  subjectId: string;
  namespace: bigint;
  nonce: string;
};
export type ImportReceipt = {
  action: DurableAction;
  outcome:
    "pending" | "completed" | "not-executed" | "unknown" | "history-expired";
  attr?: NodeAttr;
  failure?: string;
  settled: boolean;
};

export type PreparedDownload = {
  token: string;
  url: string;
  filename: string;
  size: bigint;
  digest: Uint8Array;
};

export type DownloadWriter = {
  write: (chunk: Uint8Array) => Promise<void>;
  close: () => Promise<void>;
  abort: (reason?: unknown) => Promise<void>;
};
