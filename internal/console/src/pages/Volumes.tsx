import { useEffect, useRef, useState } from "react";
import {
  ArrowRight,
  CircleAlert,
  Database,
  FileText,
  Folder,
  HardDrive,
  LockKeyhole,
  MoreHorizontal,
  Plus,
  Search,
  Terminal,
  Trash2,
} from "lucide-react";
import {
  ManagementClient,
  MutationOutcomeUnknown,
  publicError,
} from "../api/management";
import { Empty, Failure, Loading } from "../components/States";
import { Modal } from "../components/Modal";
import {
  errorMessage,
  exactBytes,
  formatBytes,
  formatCount,
  percent,
  quotaInput,
  type Volume,
} from "../types";
export const volumeState = (volume: Volume) =>
  volume.state === "active"
    ? volume.summaryError
      ? "unavailable"
      : volume.summary && volume.summary.usedBytes > volume.summary.quotaBytes
        ? "overquota"
        : "ready"
    : volume.state;
const labels: Record<string, string> = {
  ready: "就绪",
  active: "就绪",
  creating: "创建中",
  deleting: "删除中",
  deleted: "已删除",
  failed: "失败",
  recovering: "恢复中",
  unavailable: "统计不可用",
  overquota: "超出配额",
};
export const VolumeBadge = ({ volume }: { volume: Volume }) => {
  const state = volumeState(volume);
  return (
    <span className={`badge ${state}`}>
      <span />
      {labels[state] ?? state}
    </span>
  );
};
type Dialog = {
  type: "create" | "rename" | "delete";
  volume?: Volume;
} | null;
export const Volumes = ({
  api,
  onOpen,
  onNotice,
  onChanged,
  revision,
}: {
  api: ManagementClient;
  onOpen: (volume: Volume) => void;
  onNotice: (message: string) => void;
  onChanged: () => void;
  revision: number;
}) => {
  const generation = useRef(0);
  const [volumes, setVolumes] = useState<Volume[]>([]);
  const [next, setNext] = useState<string | undefined>();
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [search, setSearch] = useState("");
  const [dialog, setDialog] = useState<Dialog>(null);
  const [more, setMore] = useState<string | null>(null);
  const [retry, setRetry] = useState(0);
  const [loadingMore, setLoadingMore] = useState(false);
  const [moreError, setMoreError] = useState("");
  const [menuPosition, setMenuPosition] = useState({ left: 0, top: 0 });
  useEffect(() => {
    ++generation.current;
    const abort = new AbortController();
    setLoading(true);
    setError("");
    setVolumes([]);
    setNext(undefined);
    api
      .volumes(undefined, abort.signal)
      .then((page) => {
        setVolumes(page.items);
        setNext(page.nextAfter);
      })
      .catch((error) => {
        if (!abort.signal.aborted) setError(errorMessage(error));
      })
      .finally(() => {
        if (!abort.signal.aborted) setLoading(false);
      });
    return () => abort.abort();
  }, [api, revision, retry]);
  useEffect(() => {
    if (!more) return;
    const close = (event: MouseEvent) => {
      if (!(event.target as HTMLElement).closest(".menu-anchor")) setMore(null);
    };
    const key = (event: KeyboardEvent) => {
      if (event.key === "Escape") setMore(null);
    };
    window.addEventListener("mousedown", close);
    window.addEventListener("keydown", key);
    return () => {
      window.removeEventListener("mousedown", close);
      window.removeEventListener("keydown", key);
    };
  }, [more]);
  const loadMore = async () => {
    if (next === undefined) return;
    const version = generation.current;
    setLoadingMore(true);
    setMoreError("");
    try {
      const page = await api.volumes(next);
      if (version !== generation.current) return;
      const existing = new Set(volumes.map((volume) => volume.id));
      if (page.items.some((volume) => existing.has(volume.id)))
        throw new Error("列表在分页期间发生变化，请重新加载。");
      setVolumes((previous) => [...previous, ...page.items]);
      if (page.nextAfter === next) throw new Error("服务返回了重复分页游标。");
      setNext(page.nextAfter);
    } catch (error) {
      setMoreError(errorMessage(error));
    } finally {
      setLoadingMore(false);
    }
  };
  const unknown = volumes.filter((volume) => volume.summaryError).length;
  const totals = volumes.reduce(
    (sum, volume) => {
      if (volume.summary) {
        sum.used += volume.summary.usedBytes;
        sum.quota += volume.summary.quotaBytes;
        sum.files += volume.summary.fileCount;
        sum.dirs += volume.summary.directoryCount;
      }
      return sum;
    },
    { used: 0n, quota: 0n, files: 0n, dirs: 0n },
  );
  const statsUnknown = loading || error !== "" || unknown > 0;
  const openDialog = (dialog: Dialog) => {
    setMore(null);
    setDialog(dialog);
  };
  return (
    <>
      <div className="page-heading">
        <div>
          <span className="eyebrow">YOUR WORKSPACE</span>
          <h1>我的 Volumes</h1>
          <p>独立的文件空间，连接你的应用、设备与 agents。</p>
        </div>
        <button
          className="button primary"
          onClick={() => openDialog({ type: "create" })}
        >
          <Plus size={17} />
          创建 volume
        </button>
      </div>
      <div className="stats-grid">
        <div className="stat-card">
          <div className="stat-label">
            当前列表的 volumes
            <HardDrive size={17} />
          </div>
          <div className="stat-value">
            {loading || error ? "—" : volumes.length}
            <span>个</span>
          </div>
          <div className="stat-caption">
            <span className="status-dot" />
            {next !== undefined
              ? "更多 volumes 可继续加载"
              : "全部属于你的账号"}
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-label">
            当前列表的空间使用
            <Database size={17} />
          </div>
          <div
            className="stat-value"
            title={
              statsUnknown
                ? "统计暂不可用"
                : `${exactBytes(totals.used)} / ${exactBytes(totals.quota)} 配额`
            }
          >
            {statsUnknown ? "—" : formatBytes(totals.used)}
            <small>
              / {statsUnknown ? "—" : formatBytes(totals.quota)} 配额
            </small>
          </div>
          <div className="usage-track">
            <i
              style={{
                width: `${statsUnknown ? 0 : percent(totals.used, totals.quota)}%`,
              }}
            />
          </div>
        </div>
        <div className="stat-card">
          <div className="stat-label">
            当前列表的文件与目录
            <Folder size={17} />
          </div>
          <div className="stat-value">
            {statsUnknown ? "—" : formatCount(totals.files)}
            <span>文件</span>
          </div>
          <div className="stat-caption">
            {statsUnknown
              ? unknown > 0
                ? `${unknown} 个 volume 统计暂不可用`
                : "正在确认统计"
              : `${formatCount(totals.dirs)} 个目录 · 符号链接计入文件数`}
          </div>
        </div>
      </div>
      <div className="section-toolbar">
        <div className="section-title">
          我的 volumes<span>{loading ? "…" : volumes.length}</span>
        </div>
        <div className="search-box">
          <Search size={16} />
          <input
            aria-label="搜索已加载 volumes"
            placeholder="搜索已加载的 volume…"
            value={search}
            onChange={(event) => setSearch(event.target.value)}
          />
        </div>
      </div>
      <div className="panel volume-panel">
        {loading ? (
          <Loading message="正在加载 volumes…" />
        ) : error ? (
          <Failure
            message={error}
            onRetry={() => setRetry((value) => value + 1)}
          />
        ) : volumes.length === 0 ? (
          <Empty
            title="创建你的第一个 volume"
            detail="为项目、资料或 agent 创建一个独立的文件空间。"
          >
            <button
              className="button primary"
              onClick={() => openDialog({ type: "create" })}
            >
              <Plus size={16} />
              创建 volume
            </button>
          </Empty>
        ) : (
          <div className="table-scroll">
            <table className="data-table volume-table">
              <thead>
                <tr>
                  <th>名称</th>
                  <th>状态</th>
                  <th>文件 / 目录</th>
                  <th>已用 / 配额</th>
                  <th>创建时间</th>
                  <th aria-label="操作" />
                </tr>
              </thead>
              <tbody>
                {volumes
                  .filter((volume) =>
                    volume.name
                      .toLocaleLowerCase()
                      .includes(search.toLocaleLowerCase()),
                  )
                  .map((volume, index) => (
                    <tr key={volume.id}>
                      <td>
                        <button
                          className="volume-name"
                          disabled={
                            !["active", "recovering"].includes(volume.state)
                          }
                          onClick={() => onOpen(volume)}
                        >
                          <span className={`volume-icon hue-${index % 3}`}>
                            <HardDrive size={19} />
                          </span>
                          <span>
                            <strong>{volume.name}</strong>
                            <small title={volume.id}>
                              Volume ID：{volume.id.slice(0, 12)}…
                            </small>
                          </span>
                        </button>
                      </td>
                      <td>
                        <VolumeBadge volume={volume} />
                      </td>
                      <td>
                        {volume.summary ? (
                          <div className="count-pair">
                            <strong>
                              {formatCount(volume.summary.fileCount)}
                              <span>文件</span>
                            </strong>
                            <small>
                              {formatCount(volume.summary.directoryCount)} 目录
                            </small>
                          </div>
                        ) : (
                          <span
                            className="summary-error"
                            title={
                              volume.summaryError
                                ? publicError(volume.summaryError.code)
                                : "统计未知"
                            }
                          >
                            统计暂不可用
                          </span>
                        )}
                      </td>
                      <td>
                        {volume.summary ? (
                          <div
                            className="quota-cell"
                            title={`${exactBytes(volume.summary.usedBytes)} / ${exactBytes(volume.summary.quotaBytes)}`}
                          >
                            <span>
                              <strong>
                                {formatBytes(volume.summary.usedBytes)}
                              </strong>{" "}
                              / {formatBytes(volume.summary.quotaBytes)}
                            </span>
                            <div className="usage-track">
                              <i
                                className={
                                  percent(
                                    volume.summary.usedBytes,
                                    volume.summary.quotaBytes,
                                  ) > 90
                                    ? "warning"
                                    : ""
                                }
                                style={{
                                  width: `${percent(volume.summary.usedBytes, volume.summary.quotaBytes)}%`,
                                }}
                              />
                            </div>
                          </div>
                        ) : (
                          <span className="muted">—</span>
                        )}
                      </td>
                      <td className="muted date-cell">
                        {new Date(volume.createdAt).toLocaleDateString("zh-CN")}
                      </td>
                      <td>
                        <div className="row-actions">
                          <button
                            className="browse-button"
                            disabled={
                              !["active", "recovering"].includes(volume.state)
                            }
                            onClick={() => onOpen(volume)}
                          >
                            打开
                            <ArrowRight size={14} />
                          </button>
                          <div className="menu-anchor">
                            <button
                              className="icon-button"
                              aria-label={`${volume.name} 更多操作`}
                              onClick={(event) => {
                                const rect =
                                  event.currentTarget.getBoundingClientRect();
                                setMenuPosition({
                                  left: Math.min(
                                    window.innerWidth - 178,
                                    Math.max(10, rect.right - 168),
                                  ),
                                  top:
                                    rect.bottom + 150 > window.innerHeight
                                      ? Math.max(10, rect.top - 150)
                                      : rect.bottom + 5,
                                });
                                setMore(more === volume.id ? null : volume.id);
                              }}
                            >
                              <MoreHorizontal size={18} />
                            </button>
                            {more === volume.id && (
                              <div className="dropdown" style={menuPosition}>
                                <button
                                  disabled={volume.state !== "active"}
                                  onClick={() =>
                                    openDialog({ type: "rename", volume })
                                  }
                                >
                                  <FileText size={15} />
                                  改名
                                </button>
                                <div className="dropdown-divider" />
                                <button
                                  className="danger"
                                  disabled={volume.state !== "active"}
                                  onClick={() =>
                                    openDialog({ type: "delete", volume })
                                  }
                                >
                                  <Trash2 size={15} />
                                  删除 volume
                                </button>
                              </div>
                            )}
                          </div>
                        </div>
                      </td>
                    </tr>
                  ))}
              </tbody>
            </table>
            {volumes.length > 0 &&
              !volumes.some((volume) =>
                volume.name
                  .toLocaleLowerCase()
                  .includes(search.toLocaleLowerCase()),
              ) && (
                <div className="search-empty">
                  已加载的列表中没有匹配的 volume。
                </div>
              )}
          </div>
        )}
      </div>
      {next !== undefined && (
        <div className="load-more">
          <button
            className="button secondary"
            disabled={loadingMore}
            onClick={() => void loadMore()}
          >
            {loadingMore ? "正在加载…" : "加载更多 volumes"}
          </button>
        </div>
      )}
      {moreError && (
        <div className="dialog-error" role="alert">
          <CircleAlert size={16} />
          {moreError}
        </div>
      )}
      <div className="hint-row">
        <LockKeyhole size={15} />
        <span>
          容量统计包含已删除但仍被打开的文件；各 volume 配额之和是逻辑配额。
        </span>
      </div>
      <div className="cli-callout">
        <div className="callout-icon">
          <Terminal size={21} />
        </div>
        <div>
          <strong>从终端连接你的文件空间</strong>
          <p>CLI 使用同一套访问权限，适合自动化工作流。</p>
        </div>
        <code>remote-fs volume list</code>
      </div>
      {dialog && (
        <VolumeDialog
          dialog={dialog}
          api={api}
          onClose={() => setDialog(null)}
          onNotice={onNotice}
          onChanged={onChanged}
        />
      )}
    </>
  );
};
const VolumeDialog = ({
  dialog,
  api,
  onClose,
  onNotice,
  onChanged,
}: {
  dialog: NonNullable<Dialog>;
  api: ManagementClient;
  onClose: () => void;
  onNotice: (message: string) => void;
  onChanged: () => void;
}) => {
  const current = dialog.volume;
  const [value, setValue] = useState("20");
  const [unit, setUnit] = useState("GiB");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [outcomeUnknown, setOutcomeUnknown] = useState(false);
  let parsedQuota: bigint | undefined;
  try {
    if (dialog.type === "create") parsedQuota = quotaInput(value, unit);
  } catch {}
  const close = () => {
    if (outcomeUnknown) onChanged();
    onClose();
  };
  const submit = async (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (busy || outcomeUnknown) return;
    setError("");
    const data = new FormData(event.currentTarget);
    setBusy(true);
    try {
      if (dialog.type === "create") {
        const name = String(data.get("name"));
        if (!name.trim()) throw new Error("请输入 volume 名称。");
        await api.createVolume(name, quotaInput(value, unit));
      } else if (dialog.type === "rename" && current) {
        const name = String(data.get("name"));
        if (!name.trim()) throw new Error("请输入 volume 名称。");
        await api.renameVolume(current, name);
      } else if (dialog.type === "delete" && current) {
        if (data.get("confirmation") !== current.name)
          throw new Error("输入的名称与目标 volume 不一致。");
        await api.deleteVolume(current);
      } else throw new Error("操作缺少目标。");
      onChanged();
      onClose();
      onNotice(
        dialog.type === "create"
          ? "Volume 已创建。"
          : dialog.type === "rename"
            ? "Volume 名称已修改。"
            : "Volume 已删除。",
      );
    } catch (error) {
      if (error instanceof MutationOutcomeUnknown) {
        setOutcomeUnknown(true);
        setError(
          "请求结果未知。请刷新 volumes 列表核对当前状态；刷新不能保证确认该请求是否已完成，请勿直接重复提交。",
        );
      } else setError(errorMessage(error));
    } finally {
      setBusy(false);
    }
  };
  const titles = {
    create: "创建 volume",
    rename: "修改 volume 名称",
    delete: "删除 volume",
  };
  return (
    <Modal title={titles[dialog.type]} onClose={close} busy={busy}>
      <form onSubmit={(event) => void submit(event)}>
        <div className="modal-body">
          {dialog.type === "create" && (
            <>
              <p className="dialog-description">
                创建属于你的独立文件空间。只有你可以访问其中的文件。
              </p>
              <label>
                名称
                <input
                  name="name"
                  placeholder="例如：agent-workspace"
                  required
                  autoComplete="off"
                />
              </label>
            </>
          )}
          {dialog.type === "rename" && current && (
            <>
              <p className="dialog-description">
                修改显示名称，已有 volume 标识和连接保持有效。
              </p>
              <label>
                名称
                <input
                  name="name"
                  defaultValue={current.name}
                  required
                  autoComplete="off"
                />
              </label>
            </>
          )}
          {dialog.type === "create" && (
            <>
              <label>
                容量配额
                <div className="quota-form-row">
                  <input
                    name="quota"
                    inputMode="decimal"
                    value={value}
                    onChange={(event) => setValue(event.target.value)}
                    required
                  />
                  <select
                    aria-label="容量单位"
                    value={unit}
                    onChange={(event) => setUnit(event.target.value)}
                  >
                    <option value="B">字节</option>
                    <option value="MiB">MiB</option>
                    <option value="GiB">GiB</option>
                    <option value="TiB">TiB</option>
                  </select>
                </div>
              </label>
              {parsedQuota !== undefined && (
                <div className="form-note">
                  确切容量：{exactBytes(parsedQuota)}
                </div>
              )}
            </>
          )}
          {dialog.type === "delete" && current && (
            <>
              <div className="delete-warning">
                <Trash2 size={23} />
                <div>
                  <strong>永久删除 {current.name} 及其中所有文件</strong>
                  <p>
                    此操作不可撤销。活跃客户端、文件句柄或传输会阻止删除受理，请先完成卸载和关闭。
                  </p>
                </div>
              </div>
              <label>
                输入 <strong>{current.name}</strong> 以确认
                <input
                  name="confirmation"
                  placeholder={current.name}
                  autoComplete="off"
                  required
                />
              </label>
            </>
          )}
          {error && (
            <div className="dialog-error" role="alert">
              <CircleAlert size={16} />
              {error}
            </div>
          )}
        </div>
        <div className="modal-footer">
          {!outcomeUnknown && (
            <button
              className="button secondary"
              type="button"
              disabled={busy}
              onClick={close}
            >
              取消
            </button>
          )}
          {outcomeUnknown ? (
            <button className="button primary" type="button" onClick={close}>
              刷新列表并关闭
            </button>
          ) : (
            <button
              className={`button ${dialog.type === "delete" ? "danger-button" : "primary"}`}
              disabled={busy}
              type="submit"
            >
              {busy
                ? "正在提交…"
                : dialog.type === "create"
                  ? "创建 volume"
                  : dialog.type === "delete"
                    ? "确认删除"
                    : "保存名称"}
            </button>
          )}
        </div>
      </form>
    </Modal>
  );
};
