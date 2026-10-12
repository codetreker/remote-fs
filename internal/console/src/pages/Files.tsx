import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  ArrowDownToLine,
  ArrowLeft,
  ChevronRight,
  CircleAlert,
  File,
  FileText,
  Folder,
  FolderPlus,
  HardDrive,
  Link2,
  Loader2,
  MoreHorizontal,
  RefreshCw,
  Search,
  Trash2,
  Upload,
} from "lucide-react";
import { FileAPIError, FileClient, TransferOutcomeError } from "../api/files";
import type {
  DirectoryListing,
  DirectoryRef,
  DurableAction,
  FileEntry,
  ImportReceipt,
} from "../api/file-types";
import { decodeRawLeaf, rawLeafFromInput } from "../api/rawnames";
import { parseV5 } from "../api/v5codec";
import {
  errorMessage,
  formatBytes,
  formatCount,
  percent,
  type ServiceCapabilities,
  type Volume,
} from "../types";
import {
  FileDialogs,
  type FileDialog,
  type FileDialogInput,
} from "../components/FileDialogs";

type Crumb = { label: string; ref: DirectoryRef };
type Selection = {
  dialog: FileDialog;
  listing: DirectoryListing;
  entry?: FileEntry;
};
type UploadResponsibility = {
  action: DurableAction;
  name: string;
  receipt?: ImportReceipt;
};
type Props = {
  userId: string;
  volume: Volume;
  capabilities: ServiceCapabilities;
  csrfToken: string;
  createAction: () => Promise<DurableAction>;
  onBack: () => void;
  onAuthLost: () => void;
  onPermissionDenied: () => void;
  onChanged: () => void;
  onNotice: (message: string) => void;
  registerLeaveGuard: (guard: (() => Promise<boolean>) | null) => void;
};
const PAGE_SIZE = 100;
const JOURNAL_MAX_CHARS = 32768;
const uploadJournalKey = (userId: string, volumeId: string) =>
  `rfs:upload:v1:${encodeURIComponent(userId)}:${encodeURIComponent(volumeId)}`;
const decodeUploadJournal = (
  text: string,
  userId: string,
): UploadResponsibility => {
  if (text.length > JOURNAL_MAX_CHARS)
    throw new Error("上传恢复记录超过允许的大小。");
  const value = parseV5(text);
  const object = (row: unknown, keys: string[]) => {
    if (
      row === null ||
      typeof row !== "object" ||
      Array.isArray(row) ||
      Object.keys(row).length !== keys.length ||
      !keys.every((key) => Object.hasOwn(row, key))
    )
      throw new Error("上传恢复记录结构无效。");
    return row as Record<string, unknown>;
  };
  const record = object(value, ["action", "name"]);
  const action = object(record.action, ["subjectId", "namespace", "nonce"]);
  if (
    action.subjectId !== userId ||
    typeof action.subjectId !== "string" ||
    new TextEncoder().encode(action.subjectId).length === 0 ||
    new TextEncoder().encode(action.subjectId).length > 128 ||
    typeof action.namespace !== "string" ||
    !/^[1-9][0-9]{0,19}$/.test(action.namespace) ||
    typeof action.nonce !== "string" ||
    !/^[a-f0-9]{32}$/.test(action.nonce) ||
    typeof record.name !== "string" ||
    record.name.length === 0 ||
    record.name.length > 24576 ||
    /[\p{Cc}\p{Cf}\p{Zl}\p{Zp}]/u.test(record.name)
  )
    throw new Error("上传恢复记录的身份或字段无效。");
  const namespace = BigInt(action.namespace);
  if (namespace > (1n << 64n) - 1n)
    throw new Error("上传恢复记录的动作编号超出范围。");
  for (const character of record.name) {
    const point = character.codePointAt(0)!;
    if (point >= 0xd800 && point <= 0xdfff)
      throw new Error("上传恢复记录的显示名称无效。");
  }
  return {
    action: { subjectId: action.subjectId, namespace, nonce: action.nonce },
    name: record.name,
  };
};
const restoreUpload = (userId: string, volumeId: string) => {
  try {
    const stored = sessionStorage.getItem(uploadJournalKey(userId, volumeId));
    return {
      pending:
        stored === null ? undefined : decodeUploadJournal(stored, userId),
      error: "",
    };
  } catch {
    return {
      pending: undefined,
      error:
        "保存的上传恢复记录无法读取或已损坏，不能核对原动作。新的文件修改暂停；原记录保留。",
    };
  }
};
const stateLabels: Record<string, string> = {
  active: "就绪",
  ready: "可用",
  recovering: "恢复中",
  unavailable: "不可用",
  overquota: "超出配额",
  busy: "使用中",
  creating: "创建中",
  deleting: "删除中",
  failed: "失败",
};
const fileError = (cause: unknown) => {
  if (!(cause instanceof FileAPIError)) return errorMessage(cause);
  const messages: Record<string, string> = {
    EACCES: "当前身份没有文件访问权限，或登录已失效。",
    ESTALE: "所选目录或项目的身份已变化，请重新读取目录后选择。",
    EAGAIN: "所选项目与当前权威状态冲突，请重新读取目录。",
    EEXIST: "目标目录已存在同名项目。",
    ENOENT: "所选项目已不存在，请重新读取目录。",
    ENOTEMPTY: "目录仍有内容，不能删除。",
    EDQUOT: "此次操作超过 volume 配额。",
    ENOSPC: "服务可用存储空间不足。",
    EFBIG: "文件超过服务配置的传输上限。",
    EBUSY: "项目仍被其他操作使用，请等待后重试。",
    EOPNOTSUPP: "当前存储不支持此次操作。",
    EINVAL: "服务拒绝了此次操作的输入。",
    EIO: "服务无法确认权威存储状态。",
  };
  return `${cause.errno && messages[cause.errno] ? messages[cause.errno] : "文件请求未得到可确认的结果。"}${cause.unknownOutcome ? "原动作结果未知，必须先核对原动作。" : ""}`;
};
const directoryOf = (entry: FileEntry, parent: DirectoryRef): DirectoryRef => ({
  nodeId: entry.attr.id,
  ...(parent.scope === undefined ? {} : { scope: parent.scope }),
});
const byteOrder = (left: FileEntry, right: FileEntry) => {
  if (left.attr.kind === "directory" && right.attr.kind !== "directory")
    return -1;
  if (left.attr.kind !== "directory" && right.attr.kind === "directory")
    return 1;
  for (
    let index = 0;
    index < Math.min(left.rawBytes.length, right.rawBytes.length);
    index += 1
  ) {
    if (left.rawBytes[index] !== right.rawBytes[index])
      return left.rawBytes[index] - right.rawBytes[index];
  }
  return left.rawBytes.length - right.rawBytes.length;
};
const EntryIcon = ({ entry }: { entry: FileEntry }) =>
  entry.attr.kind === "directory" ? (
    <Folder className="file-icon folder" />
  ) : entry.attr.kind === "symlink" ? (
    <Link2 className="file-icon symlink" />
  ) : (
    <FileText className="file-icon" />
  );

export const Files = (props: Props) => (
  <FileSession
    key={`${props.userId}:${props.volume.id}:${props.csrfToken}`}
    {...props}
  />
);

const FileSession = ({
  userId,
  volume,
  capabilities,
  csrfToken,
  createAction,
  onBack,
  onAuthLost,
  onPermissionDenied,
  onChanged,
  onNotice,
  registerLeaveGuard,
}: Props) => {
  const [client] = useState(
    () =>
      new FileClient(
        volume.id,
        csrfToken,
        onAuthLost,
        fetch,
        capabilities.fileSession,
        onPermissionDenied,
      ),
  );
  const [restored] = useState(() => restoreUpload(userId, volume.id));
  const [journalError, setJournalError] = useState(restored.error);
  const [listing, setListing] = useState<DirectoryListing>();
  const [crumbs, setCrumbs] = useState<Crumb[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [search, setSearch] = useState("");
  const [page, setPage] = useState(0);
  const [selection, setSelection] = useState<Selection>();
  const [dialogError, setDialogError] = useState("");
  const [busy, setBusy] = useState(false);
  const busyRef = useRef(false);
  const [progress, setProgress] = useState<{ loaded: number; total: number }>();
  const uploadController = useRef<AbortController | undefined>(undefined);
  const uploadRef = useRef<UploadResponsibility | undefined>(restored.pending);
  const [pendingUpload, setPendingUpload] = useState<
    UploadResponsibility | undefined
  >(restored.pending);
  const [menu, setMenu] = useState<string>();
  const [menuPosition, setMenuPosition] = useState({ left: 0, top: 0 });
  const [moveListing, setMoveListing] = useState<DirectoryListing>();
  const [moveCrumbs, setMoveCrumbs] = useState<Crumb[]>([]);
  const [moveLoading, setMoveLoading] = useState(false);
  const [moveError, setMoveError] = useState("");
  const [uploadLimit, setUploadLimit] = useState<bigint>();
  const alive = useRef(true);
  const loadSequence = useRef(0);
  const moveSequence = useRef(0);
  const setWorking = (value: boolean) => {
    busyRef.current = value;
    setBusy(value);
  };
  const keepUpload = (value: UploadResponsibility | undefined) => {
    uploadRef.current = value;
    setPendingUpload(value);
  };
  const persistUpload = (value: UploadResponsibility) => {
    const encoded = JSON.stringify({
      action: {
        subjectId: value.action.subjectId,
        namespace: value.action.namespace.toString(),
        nonce: value.action.nonce,
      },
      name: value.name,
    });
    decodeUploadJournal(encoded, userId);
    try {
      sessionStorage.setItem(uploadJournalKey(userId, volume.id), encoded);
    } catch {
      throw new Error("无法保存上传恢复记录，本次上传尚未发送。");
    }
    keepUpload(value);
  };
  const clearUpload = () => {
    try {
      sessionStorage.removeItem(uploadJournalKey(userId, volume.id));
    } catch {
      setJournalError(
        "上传结果已核对，但恢复记录未能移除。新的文件修改暂停；请重新登录后再次核对。",
      );
      throw new Error("上传恢复记录的清理尚未确认。");
    }
    keepUpload(undefined);
  };

  const load = useCallback(
    async (nextCrumbs?: Crumb[]) => {
      const sequence = ++loadSequence.current;
      setLoading(true);
      setError("");
      setListing(undefined);
      setMenu(undefined);
      try {
        const path = nextCrumbs ?? [
          { label: volume.name, ref: await client.open() },
        ];
        const observed = await client.list(path[path.length - 1].ref);
        if (!alive.current || sequence !== loadSequence.current) return;
        setCrumbs(path);
        setListing(observed);
        setSearch("");
        setPage(0);
        return true;
      } catch (cause) {
        if (alive.current && sequence === loadSequence.current)
          setError(fileError(cause));
        return false;
      } finally {
        if (alive.current && sequence === loadSequence.current)
          setLoading(false);
      }
    },
    [client, volume.name],
  );

  useEffect(() => {
    alive.current = true;
    void load();
    void client
      .limits()
      .then((limits) => {
        if (limits.maxBytes > capabilities.maxTransferBytes)
          throw new Error("文件入口返回的传输上限与服务配置不一致。");
        if (alive.current) setUploadLimit(limits.maxBytes);
      })
      .catch((cause: unknown) => {
        if (alive.current) setError(fileError(cause));
      });
    return () => {
      alive.current = false;
      ++loadSequence.current;
      void client
        .close()
        .catch((cause: unknown) =>
          onNotice(`文件会话关闭尚未确认：${fileError(cause)}`),
        );
    };
  }, [client]);

  const leave = useCallback(async () => {
    if (busyRef.current) {
      onNotice("文件操作正在确认，请先等待结果或取消上传。");
      return false;
    }
    ++loadSequence.current;
    ++moveSequence.current;
    try {
      if (uploadRef.current) {
        const receipt = await client.cancelUpload(uploadRef.current.action);
        if (
          !receipt.settled ||
          !["completed", "not-executed"].includes(receipt.outcome)
        )
          throw new Error("原上传的提交结果或传输清理尚未确认。");
        clearUpload();
      }
      await client.reconcile();
      await client.close();
      return true;
    } catch (cause) {
      setLoading(false);
      setMoveLoading(false);
      setError(`文件会话或传输清理尚未确认：${fileError(cause)}`);
      return false;
    }
  }, [client, onNotice]);
  useEffect(() => {
    registerLeaveGuard(leave);
    return () => registerLeaveGuard(null);
  }, [leave, registerLeaveGuard]);

  useEffect(() => {
    if (!menu) return;
    const close = (event: MouseEvent) => {
      if (!(event.target as HTMLElement).closest(".menu-anchor"))
        setMenu(undefined);
    };
    const key = (event: KeyboardEvent) => {
      if (event.key === "Escape") setMenu(undefined);
    };
    document.addEventListener("mousedown", close);
    document.addEventListener("keydown", key);
    return () => {
      document.removeEventListener("mousedown", close);
      document.removeEventListener("keydown", key);
    };
  }, [menu]);

  const ordered = useMemo(
    () => (listing ? [...listing.entries].sort(byteOrder) : []),
    [listing],
  );
  const filtered = ordered.filter((entry) =>
    entry.displayName.toLowerCase().includes(search.toLowerCase()),
  );
  const pageCount = Math.max(1, Math.ceil(filtered.length / PAGE_SIZE));
  const currentPage = Math.min(page, pageCount - 1);
  const rows = filtered.slice(
    currentPage * PAGE_SIZE,
    (currentPage + 1) * PAGE_SIZE,
  );
  const readOnly = !["active", "ready", "busy", "overquota"].includes(
    volume.state,
  );
  const overQuota =
    volume.summary !== undefined &&
    volume.summary.usedBytes > volume.summary.quotaBytes;
  const mutationDisabled =
    readOnly ||
    busy ||
    loading ||
    !listing ||
    !!pendingUpload ||
    !!journalError;
  const uploadDisabled =
    mutationDisabled || overQuota || uploadLimit === undefined;
  const refresh = async () => {
    try {
      await client.reconcile();
      if (await load(crumbs.length ? crumbs : undefined)) onChanged();
    } catch (cause) {
      setError(`原操作结果尚未确认：${fileError(cause)}`);
    }
  };

  const browseMove = async (path: Crumb[]) => {
    const sequence = ++moveSequence.current;
    setMoveLoading(true);
    setMoveError("");
    setMoveListing(undefined);
    try {
      const observed = await client.list(path[path.length - 1].ref);
      if (alive.current && sequence === moveSequence.current) {
        setMoveCrumbs(path);
        setMoveListing(observed);
      }
    } catch (cause) {
      if (alive.current && sequence === moveSequence.current)
        setMoveError(fileError(cause));
    } finally {
      if (alive.current && sequence === moveSequence.current)
        setMoveLoading(false);
    }
  };
  const openDialog = (kind: FileDialog["kind"], entry?: FileEntry) => {
    if (!listing) return;
    setMenu(undefined);
    setDialogError("");
    setProgress(undefined);
    setSelection({
      listing,
      entry,
      dialog: {
        kind,
        displayName: entry?.displayName,
        targetId: entry?.attr.id.toString(),
      },
    });
    if (kind === "move") void browseMove([crumbs[0]]);
  };
  const finishReceipt = async (receipt: ImportReceipt, name: string) => {
    if (receipt.outcome === "completed" && receipt.settled) {
      clearUpload();
      setSelection(undefined);
      setProgress(undefined);
      onNotice(`已确认发布 ${name}`);
      onChanged();
      await load(crumbs.length ? crumbs : undefined);
    } else if (receipt.outcome === "not-executed" && receipt.settled) {
      clearUpload();
      setSelection(undefined);
      setProgress(undefined);
      onNotice(
        `已确认本次上传未发布${receipt.failure ? `（${receipt.failure}）` : ""}`,
      );
    } else {
      const current = uploadRef.current;
      if (current) keepUpload({ ...current, receipt });
      setDialogError(
        receipt.outcome === "completed"
          ? "文件已发布，传输清理尚未确认。请核对原动作。"
          : receipt.outcome === "history-expired"
            ? "原动作历史已过期，不能判断发布结果，也不能重放。"
            : "原上传结果尚未确认。请查询同一动作；已封存的上传可继续提交。",
      );
    }
  };
  const submit = async ({ name, file }: FileDialogInput) => {
    if (!selection || busyRef.current || readOnly || journalError) return;
    if (pendingUpload) {
      setDialogError(
        "原上传仍需核对，请关闭对话框后查询、继续或清理同一动作。",
      );
      return;
    }
    setDialogError("");
    setWorking(true);
    try {
      const { listing: captured, entry, dialog } = selection;
      if (dialog.kind === "upload" || dialog.kind === "replace") {
        if (!file || uploadLimit === undefined)
          throw new Error("请选择文件并等待服务确认上传上限。");
        if (BigInt(file.size) > uploadLimit)
          throw new Error("文件超过服务配置的上传上限。");
        const rawLeaf =
          dialog.kind === "replace" && entry
            ? entry.rawLeaf
            : rawLeafFromInput(file.name);
        const action = await createAction();
        const displayName =
          dialog.kind === "replace" && entry
            ? entry.displayName
            : decodeRawLeaf(rawLeaf).displayName;
        persistUpload({ action, name: displayName });
        uploadController.current = new AbortController();
        setProgress({ loaded: 0, total: file.size });
        const receipt = await client.upload(
          file,
          captured,
          rawLeaf,
          action,
          dialog.kind === "replace" ? entry : undefined,
          (loaded, total) => setProgress({ loaded, total }),
          uploadController.current.signal,
        );
        await finishReceipt(receipt, displayName);
      } else {
        if (dialog.kind === "folder")
          await client.mkdir(captured, rawLeafFromInput(name));
        else if (!entry) throw new Error("所选项目身份缺失，请重新读取目录。");
        else if (dialog.kind === "rename")
          await client.rename(captured, entry, rawLeafFromInput(name));
        else if (dialog.kind === "remove") await client.remove(captured, entry);
        else if (dialog.kind === "move") {
          if (!moveListing) throw new Error("请选择可用的目标目录。");
          await client.move(captured, entry, moveListing, entry.rawLeaf);
        }
        setSelection(undefined);
        onNotice("文件操作已确认完成。");
        onChanged();
        await load(crumbs);
      }
    } catch (cause) {
      setDialogError(fileError(cause));
      if (uploadRef.current) {
        const receipt =
          cause instanceof TransferOutcomeError ? cause.receipt : undefined;
        if (receipt) keepUpload({ ...uploadRef.current, receipt });
      }
    } finally {
      uploadController.current = undefined;
      setWorking(false);
    }
  };
  const reconcileUpload = async (kind: "query" | "resume" | "cancel") => {
    const current = uploadRef.current;
    if (!current || busyRef.current) return;
    setWorking(true);
    setError("");
    try {
      if (kind === "cancel") {
        const receipt = await client.cancelUpload(current.action);
        if (
          !receipt.settled ||
          !["completed", "not-executed"].includes(receipt.outcome)
        )
          throw new Error("原上传的提交结果或传输清理尚未确认。");
        clearUpload();
        setSelection(undefined);
        setProgress(undefined);
        onNotice("原上传已核对，传输清理已确认。");
        onChanged();
        await load(crumbs);
      } else {
        const receipt =
          kind === "resume"
            ? await client.resumeUpload(current.action)
            : await client.queryUpload(current.action);
        await finishReceipt(receipt, current.name);
      }
    } catch (cause) {
      setError(`原上传仍需核对：${fileError(cause)}`);
    } finally {
      setWorking(false);
    }
  };
  const download = async (entry: FileEntry) => {
    if (busyRef.current || entry.attr.kind !== "file") return;
    setWorking(true);
    setError("");
    setMenu(undefined);
    try {
      const prepared = await client.prepareDownload(entry);
      await client.download(prepared);
      onNotice(
        `已交给浏览器下载 ${entry.displayName}。请在浏览器中确认下载完成。`,
      );
    } catch (cause) {
      setError(`下载尚未完成：${fileError(cause)}`);
    } finally {
      setWorking(false);
    }
  };
  const destination = (
    <div className="panel">
      <div className="file-toolbar">
        <nav className="breadcrumbs" aria-label="目标目录">
          {moveCrumbs.map((crumb, index) => (
            <span
              key={`${crumb.ref.nodeId}-${index}`}
              style={{ display: "flex", alignItems: "center", gap: 5 }}
            >
              {index > 0 && <ChevronRight size={13} />}
              <button
                type="button"
                disabled={busy || moveLoading}
                onClick={() => void browseMove(moveCrumbs.slice(0, index + 1))}
              >
                {crumb.label}
              </button>
            </span>
          ))}
        </nav>
      </div>
      {moveLoading ? (
        <div className="loading-state" role="status">
          <Loader2 className="spin" />
          <p>正在读取目标目录…</p>
        </div>
      ) : moveError ? (
        <div className="dialog-error" role="alert">
          <CircleAlert size={15} />
          {moveError}
        </div>
      ) : (
        <div style={{ maxHeight: 220, overflow: "auto", padding: 12 }}>
          {moveListing?.entries
            .filter(
              (entry) =>
                entry.attr.kind === "directory" &&
                entry.attr.id !== selection?.entry?.attr.id,
            )
            .map((entry) => (
              <button
                key={`${entry.attr.id}:${entry.rawLeaf}`}
                type="button"
                className="file-name"
                style={{ padding: "8px 0", width: "100%" }}
                disabled={busy}
                onClick={() =>
                  void browseMove([
                    ...moveCrumbs,
                    {
                      label: entry.displayName,
                      ref: directoryOf(entry, moveListing.parent),
                    },
                  ])
                }
              >
                <Folder className="file-icon folder" />
                {entry.displayName}
                <ChevronRight size={14} />
              </button>
            ))}
          {moveListing &&
            !moveListing.entries.some(
              (entry) =>
                entry.attr.kind === "directory" &&
                entry.attr.id !== selection?.entry?.attr.id,
            ) && <p className="muted">此目录没有可进入的子目录。</p>}
        </div>
      )}
    </div>
  );

  return (
    <>
      <button className="back-link" disabled={busy} onClick={onBack}>
        <ArrowLeft size={15} />
        我的 Volumes
      </button>
      <div className="page-heading file-heading">
        <div>
          <div className="volume-title">
            <span className="volume-icon hue-0">
              <HardDrive size={21} />
            </span>
            <h1>{volume.name}</h1>
            <span className={`badge ${volume.state}`}>
              <span />
              {stateLabels[volume.state] ?? volume.state}
            </span>
          </div>
          <p>
            你的 volume <span className="inline-separator">·</span>{" "}
            文件访问仅限所有者
          </p>
        </div>
        <div className="heading-actions">
          <button
            className="button secondary"
            disabled={mutationDisabled}
            onClick={() => openDialog("folder")}
          >
            <FolderPlus size={16} />
            新建目录
          </button>
          <button
            className="button primary"
            disabled={uploadDisabled}
            onClick={() => openDialog("upload")}
          >
            <Upload size={16} />
            上传文件
          </button>
        </div>
      </div>
      <div className="volume-summary">
        <span>
          <File size={16} />
          <strong>
            {volume.summary ? formatCount(volume.summary.fileCount) : "—"}
          </strong>{" "}
          文件
        </span>
        <span>
          <Folder size={16} />
          <strong>
            {volume.summary ? formatCount(volume.summary.directoryCount) : "—"}
          </strong>{" "}
          目录
        </span>
        <div className="summary-quota">
          {volume.summary ? (
            <>
              <span>
                <strong title={`${volume.summary.usedBytes} 字节`}>
                  {formatBytes(volume.summary.usedBytes)}
                </strong>
                {" / "}
                <span title={`${volume.summary.quotaBytes} 字节`}>
                  {formatBytes(volume.summary.quotaBytes)}
                </span>
              </span>
              <div className="usage-track">
                <i
                  style={{
                    width: `${percent(volume.summary.usedBytes, volume.summary.quotaBytes)}%`,
                  }}
                />
              </div>
            </>
          ) : (
            <span>
              统计无法确认
              {volume.summaryError ? `（${volume.summaryError.code}）` : ""}
            </span>
          )}
        </div>
      </div>
      {volume.state === "recovering" && (
        <div className="alert info">
          <Loader2 className="spin" size={19} />
          <div>
            <strong>Volume 正在恢复</strong>
            <p>健康元数据可继续浏览和下载。恢复完成前暂停新的文件修改。</p>
          </div>
        </div>
      )}
      {overQuota && (
        <div className="alert warning">
          <CircleAlert size={19} />
          <div>
            <strong>当前已使用空间超过配额</strong>
            <p>仍可读取和删除文件。增加空间占用的操作由服务拒绝。</p>
          </div>
        </div>
      )}
      {volume.state === "deleting" && (
        <div className="alert warning">
          <CircleAlert size={19} />
          <div>
            <strong>Volume 正在删除</strong>
            <p>新的文件操作已暂停，请查看 volume 的删除操作状态。</p>
          </div>
        </div>
      )}
      {journalError && (
        <div className="alert danger" role="alert">
          <CircleAlert size={19} />
          <div>
            <strong>上传恢复记录需要处理</strong>
            <p>{journalError}</p>
          </div>
        </div>
      )}
      {pendingUpload && (
        <div className="alert warning" role="status">
          <CircleAlert size={19} />
          <div>
            <strong>
              {pendingUpload.receipt?.outcome === "completed"
                ? "文件已发布，清理待确认"
                : "原上传需要核对"}
            </strong>
            <p>
              {pendingUpload.name} · 动作{" "}
              {pendingUpload.action.namespace.toString()} /{" "}
              {pendingUpload.action.nonce}
            </p>
            <p>
              状态：{pendingUpload.receipt?.outcome ?? "结果未知"}
              。查询与继续使用同一动作，不重新上传。
            </p>
            <div className="heading-actions" style={{ marginTop: 10 }}>
              <button
                className="button secondary"
                disabled={busy}
                onClick={() => void reconcileUpload("query")}
              >
                查询原动作
              </button>
              <button
                className="button secondary"
                disabled={
                  busy ||
                  readOnly ||
                  pendingUpload.receipt?.outcome === "completed" ||
                  pendingUpload.receipt?.outcome === "history-expired"
                }
                onClick={() => void reconcileUpload("resume")}
              >
                继续已封存上传
              </button>
              <button
                className="button secondary"
                disabled={busy}
                onClick={() => void reconcileUpload("cancel")}
              >
                核对并清理传输
              </button>
            </div>
          </div>
        </div>
      )}
      {error && !loading && (
        <div className="alert danger" role="alert">
          <CircleAlert size={19} />
          <div>
            <strong>当前操作或文件状态无法确认</strong>
            <p>{error}</p>
          </div>
          <button
            className="button secondary"
            disabled={busy}
            onClick={() => void refresh()}
          >
            <RefreshCw size={14} />
            核对并重新加载
          </button>
        </div>
      )}
      <div className="panel files-panel">
        <div className="file-toolbar">
          <nav className="breadcrumbs" aria-label="当前目录">
            {crumbs.map((crumb, index) => (
              <span
                key={`${crumb.ref.nodeId}:${index}`}
                style={{ display: "flex", alignItems: "center", gap: 8 }}
              >
                {index > 0 && <ChevronRight size={14} />}
                <button
                  disabled={busy || loading}
                  aria-current={
                    index === crumbs.length - 1 ? "page" : undefined
                  }
                  onClick={() => void load(crumbs.slice(0, index + 1))}
                >
                  {index === 0 && <Folder size={16} />} {crumb.label}
                </button>
              </span>
            ))}
          </nav>
          <div className="search-box">
            <Search size={16} />
            <input
              aria-label="搜索当前目录"
              placeholder="搜索当前目录…"
              disabled={loading || !listing}
              value={search}
              onChange={(event) => {
                setSearch(event.target.value);
                setPage(0);
              }}
            />
          </div>
          <button
            className="button secondary"
            disabled={busy || loading}
            onClick={() => void refresh()}
          >
            <RefreshCw size={14} />
            刷新目录
          </button>
        </div>
        {loading ? (
          <div className="loading-state" role="status">
            <Loader2 className="spin" />
            <p>正在读取目录…</p>
            <span>等待完整的权威文件状态</span>
          </div>
        ) : !listing ? (
          <div className="empty-state">
            <div className="empty-icon">
              <CircleAlert />
            </div>
            <h3>无法读取目录</h3>
            <p>服务未确认目录结果，请重新加载。</p>
            <button
              className="button secondary"
              disabled={busy}
              onClick={() => void refresh()}
            >
              <RefreshCw size={15} />
              重新加载
            </button>
          </div>
        ) : listing.entries.length === 0 ? (
          <div className="empty-state">
            <div className="empty-icon">
              <Folder />
            </div>
            <h3>这个目录还是空的</h3>
            <p>上传文件或新建目录，开始整理你的工作空间。</p>
          </div>
        ) : filtered.length === 0 ? (
          <div className="search-empty">当前目录没有匹配的项目。</div>
        ) : (
          <div className="table-scroll">
            <table className="data-table file-table">
              <thead>
                <tr>
                  <th>名称</th>
                  <th>类型</th>
                  <th>大小</th>
                  <th>修改时间</th>
                  <th aria-label="操作" />
                </tr>
              </thead>
              <tbody>
                {crumbs.length > 1 && (
                  <tr className="parent-row">
                    <td colSpan={5}>
                      <button
                        disabled={busy}
                        onClick={() => void load(crumbs.slice(0, -1))}
                      >
                        <ArrowLeft size={16} />
                        返回上级目录
                      </button>
                    </td>
                  </tr>
                )}
                {rows.map((entry) => {
                  const key = `${entry.attr.id}:${entry.rawLeaf}`;
                  return (
                    <tr key={key}>
                      <td>
                        {entry.attr.kind === "directory" ? (
                          <button
                            className="file-name"
                            disabled={busy}
                            onClick={() =>
                              void load([
                                ...crumbs,
                                {
                                  label: entry.displayName,
                                  ref: directoryOf(entry, listing.parent),
                                },
                              ])
                            }
                          >
                            <EntryIcon entry={entry} />
                            <span>{entry.displayName}</span>
                          </button>
                        ) : (
                          <span className="file-name passive">
                            <EntryIcon entry={entry} />
                            <span>{entry.displayName}</span>
                            {entry.attr.kind === "symlink" && (
                              <span className="link-label">链接</span>
                            )}
                          </span>
                        )}
                      </td>
                      <td className="muted">
                        {entry.attr.kind === "directory"
                          ? "目录"
                          : entry.attr.kind === "symlink"
                            ? "符号链接"
                            : "文件"}
                      </td>
                      <td
                        className="monospace muted"
                        title={
                          entry.attr.kind === "directory"
                            ? undefined
                            : `${entry.attr.size} 字节`
                        }
                      >
                        {entry.attr.kind === "directory"
                          ? "—"
                          : formatBytes(entry.attr.size)}
                      </td>
                      <td className="muted">
                        <time dateTime={entry.attr.modifiedAt}>
                          {Number.isNaN(Date.parse(entry.attr.modifiedAt))
                            ? entry.attr.modifiedAt
                            : new Date(entry.attr.modifiedAt).toLocaleString(
                                "zh-CN",
                              )}
                        </time>
                      </td>
                      <td>
                        <div className="row-actions">
                          {entry.attr.kind === "file" && (
                            <button
                              className="icon-button download-button"
                              aria-label={`下载 ${entry.displayName}`}
                              disabled={busy}
                              onClick={() => void download(entry)}
                            >
                              <ArrowDownToLine size={16} />
                            </button>
                          )}
                          <div className="menu-anchor">
                            <button
                              className="icon-button"
                              aria-label={`${entry.displayName} 更多操作`}
                              aria-expanded={menu === key}
                              disabled={busy}
                              onClick={(event) => {
                                const rect =
                                  event.currentTarget.getBoundingClientRect();
                                setMenuPosition({
                                  left: Math.min(
                                    window.innerWidth - 178,
                                    Math.max(10, rect.right - 168),
                                  ),
                                  top: Math.max(
                                    10,
                                    Math.min(
                                      rect.bottom + 5,
                                      window.innerHeight - 225,
                                    ),
                                  ),
                                });
                                setMenu(menu === key ? undefined : key);
                              }}
                            >
                              <MoreHorizontal size={18} />
                            </button>
                            {menu === key && (
                              <div className="dropdown" style={menuPosition}>
                                {entry.attr.kind === "file" && (
                                  <button onClick={() => void download(entry)}>
                                    <ArrowDownToLine size={15} />
                                    下载
                                  </button>
                                )}
                                {entry.attr.kind !== "directory" && (
                                  <button
                                    disabled={uploadDisabled}
                                    onClick={() => openDialog("replace", entry)}
                                  >
                                    <Upload size={15} />
                                    替换文件…
                                  </button>
                                )}
                                {entry.attr.kind === "symlink" && (
                                  <button
                                    disabled
                                    title="浏览器不会跟随符号链接"
                                  >
                                    <Link2 size={15} />
                                    符号链接不支持下载
                                  </button>
                                )}
                                <button
                                  disabled={mutationDisabled}
                                  onClick={() => openDialog("rename", entry)}
                                >
                                  <FileText size={15} />
                                  重命名
                                </button>
                                <button
                                  disabled={mutationDisabled}
                                  onClick={() => openDialog("move", entry)}
                                >
                                  <Folder size={15} />
                                  移动到…
                                </button>
                                <div className="dropdown-divider" />
                                <button
                                  className="danger"
                                  disabled={mutationDisabled}
                                  onClick={() => openDialog("remove", entry)}
                                >
                                  <Trash2 size={15} />
                                  删除
                                </button>
                              </div>
                            )}
                          </div>
                        </div>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
        <div className="table-footer">
          <span>
            {listing
              ? `当前目录 ${formatCount(BigInt(listing.entries.length))} 个项目${search ? `，匹配 ${filtered.length} 个` : ""}`
              : "目录状态尚未确认"}
          </span>
          {listing && pageCount > 1 ? (
            <div className="heading-actions">
              <button
                disabled={currentPage === 0 || busy}
                onClick={() => setPage(currentPage - 1)}
                aria-label="上一页"
              >
                上一页
              </button>
              <span>
                {currentPage + 1} / {pageCount}
              </span>
              <button
                disabled={currentPage === pageCount - 1 || busy}
                onClick={() => setPage(currentPage + 1)}
                aria-label="下一页"
              >
                下一页
              </button>
            </div>
          ) : (
            <span>所有文件操作使用当前账号权限</span>
          )}
        </div>
      </div>
      <div className="hint-row">
        <Link2 size={15} />
        <span>
          符号链接计为一个文件，浏览器不会跟随链接目标。目录浏览基于有界权威观察。
        </span>
      </div>
      {selection && (
        <FileDialogs
          key={`${selection.dialog.kind}:${selection.entry?.rawLeaf ?? ""}`}
          dialog={selection.dialog}
          busy={busy}
          error={dialogError}
          progress={progress}
          destination={destination}
          destinationReady={
            !!moveListing &&
            !moveLoading &&
            moveListing.parent.nodeId !== selection.listing.parent.nodeId
          }
          uploadLimit={uploadLimit}
          onSubmit={submit}
          onClose={() => {
            if (!busyRef.current) {
              ++moveSequence.current;
              setSelection(undefined);
            }
          }}
          onCancelUpload={() => uploadController.current?.abort()}
        />
      )}
    </>
  );
};
