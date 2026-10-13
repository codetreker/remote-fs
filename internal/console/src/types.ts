export type User = { id: string; displayName: string; role: "admin" | "user" };
export type Session = {
  user: User;
  csrfToken: string;
  idleExpiresAt: string;
  absoluteExpiresAt: string;
};
export type Summary = {
  fileCount: bigint;
  directoryCount: bigint;
  usedBytes: bigint;
  quotaBytes: bigint;
};
export type PublicSummary = Summary;
export type ErrorDetail = { code: string };
export type Volume = {
  id: string;
  ownerUserId: string;
  name: string;
  state: string;
  revision: bigint;
  createdAt: string;
  updatedAt: string;
  summary?: Summary;
  summaryError?: ErrorDetail;
};
export type UserVolume = {
  name: string;
  summary?: PublicSummary;
  summaryError?: ErrorDetail;
};
export type Page<T> = { items: T[]; nextAfter?: string };
export type FileEntry = {
  name: string;
  nameBase64: string;
  pathBase64: string;
  nodeId: bigint;
  kind: "directory" | "file" | "symlink";
  sizeBytes: bigint;
  modifiedAt: string;
};
export type FileDirectory = { pathBase64: string; entries: FileEntry[] };
export const formatCount = (count: bigint) =>
  new Intl.NumberFormat("zh-CN").format(count);
export const formatBytes = (bytes: bigint) => {
  const units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
  let unit = 0;
  let scale = 1n;
  while (unit < units.length - 1 && bytes >= scale * 1024n) {
    unit++;
    scale *= 1024n;
  }
  if (unit === 0) return `${bytes} B`;
  const tenths = (bytes * 10n) / scale;
  return `${tenths / 10n}.${tenths % 10n} ${units[unit]}`;
};
export const percent = (used: bigint, quota: bigint) =>
  quota === 0n
    ? used > 0n
      ? 100
      : 0
    : Number(used >= quota ? 100n : (used * 100n) / quota);
export const exactBytes = (bytes: bigint) => `${formatCount(bytes)} 字节`;
export const errorMessage = (error: unknown) =>
  error instanceof Error ? error.message : "请求失败，无法确认服务状态。";
export const quotaInput = (value: string, unit: string) => {
  if (!/^(0|[1-9]\d*)(?:\.\d{1,6})?$/.test(value))
    throw new Error("请输入有效的非负容量，最多六位小数。");
  const factors: Record<string, bigint> = {
    B: 1n,
    MiB: 1024n ** 2n,
    GiB: 1024n ** 3n,
    TiB: 1024n ** 4n,
  };
  const factor = factors[unit];
  if (factor === undefined) throw new Error("容量单位无效。");
  const [integer, fraction = ""] = value.split(".");
  const denominator = 10n ** BigInt(fraction.length);
  const numerator =
    (BigInt(integer) * denominator + BigInt(fraction || "0")) * factor;
  if (numerator % denominator !== 0n)
    throw new Error("容量必须能精确表示为整数个字节。");
  const bytes = numerator / denominator;
  if (bytes <= 0n || bytes > 9223372036854775807n)
    throw new Error("容量必须大于零且不超过 int64 字节上限。");
  return bytes;
};
