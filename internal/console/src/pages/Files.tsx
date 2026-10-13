import { useEffect, useState } from "react";
import {
  ArrowLeft,
  ChevronRight,
  File,
  FileText,
  Folder,
  HardDrive,
  Link2,
  LockKeyhole,
  RefreshCw,
  Search,
} from "lucide-react";
import { ManagementClient } from "../api/management";
import { Empty, Failure, Loading } from "../components/States";
import { VolumeBadge } from "./Volumes";
import {
  errorMessage,
  formatBytes,
  formatCount,
  percent,
  type FileDirectory,
  type FileEntry,
  type Volume,
} from "../types";
type Crumb = { label: string; pathBase64: string };
const EntryIcon = ({ entry }: { entry: FileEntry }) =>
  entry.kind === "directory" ? (
    <Folder className="file-icon folder" size={20} />
  ) : entry.kind === "symlink" ? (
    <Link2 className="file-icon symlink" size={20} />
  ) : (
    <FileText className="file-icon" size={20} />
  );
export const Files = ({
  volume,
  api,
  onBack,
  revision,
}: {
  volume: Volume;
  api: ManagementClient;
  onBack: () => void;
  revision: number;
}) => {
  const [crumbs, setCrumbs] = useState<Crumb[]>([
    { label: volume.name, pathBase64: "" },
  ]);
  const [listing, setListing] = useState<FileDirectory | null>(null);
  const [loading, setLoading] = useState(true),
    [error, setError] = useState("");
  const [search, setSearch] = useState(""),
    [retry, setRetry] = useState(0),
    [page, setPage] = useState(0);
  const path = crumbs[crumbs.length - 1].pathBase64;
  useEffect(() => {
    const abort = new AbortController();
    setLoading(true);
    setListing(null);
    setError("");
    setPage(0);
    api
      .files(volume.id, path, abort.signal)
      .then((value) => {
        if (!abort.signal.aborted) setListing(value);
      })
      .catch((cause) => {
        if (!abort.signal.aborted) setError(errorMessage(cause));
      })
      .finally(() => {
        if (!abort.signal.aborted) setLoading(false);
      });
    return () => abort.abort();
  }, [api, volume.id, path, retry, revision]);
  const filtered =
    listing?.entries.filter((entry) =>
      entry.name.toLocaleLowerCase().includes(search.toLocaleLowerCase()),
    ) ?? [];
  const rows = filtered.slice(page * 100, (page + 1) * 100);
  const enter = (next: Crumb[]) => {
    setSearch("");
    setCrumbs(next);
  };
  return (
    <>
      <button className="back-link" onClick={onBack}>
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
            <VolumeBadge volume={volume} />
          </div>
          <p>
            你的 volume <span className="inline-separator">·</span>{" "}
            文件访问仅限所有者
          </p>
        </div>
        <span className="read-only-label">
          <LockKeyhole size={12} />
          只读
        </span>
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
                </span>{" "}
                配额
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
            <span className="muted">统计暂不可用</span>
          )}
        </div>
      </div>
      <div className="panel files-panel">
        <div className="file-toolbar">
          <nav className="breadcrumbs" aria-label="当前目录">
            {crumbs.map((crumb, index) => (
              <span
                key={`${crumb.pathBase64}:${index}`}
                style={{ display: "flex", alignItems: "center", gap: 8 }}
              >
                {index > 0 && <ChevronRight size={14} />}
                <button
                  disabled={loading}
                  aria-current={
                    index === crumbs.length - 1 ? "page" : undefined
                  }
                  onClick={() => enter(crumbs.slice(0, index + 1))}
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
            disabled={loading}
            onClick={() => setRetry((value) => value + 1)}
          >
            <RefreshCw size={14} />
            刷新目录
          </button>
        </div>
        {loading ? (
          <Loading message="正在读取目录…" />
        ) : error ? (
          <Failure
            message={error}
            onRetry={() => setRetry((value) => value + 1)}
          />
        ) : listing && listing.entries.length === 0 ? (
          <Empty title="这个目录还是空的" detail="当前目录没有文件或子目录。" />
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
                </tr>
              </thead>
              <tbody>
                {crumbs.length > 1 && (
                  <tr className="parent-row">
                    <td colSpan={4}>
                      <button onClick={() => enter(crumbs.slice(0, -1))}>
                        <ArrowLeft size={16} />
                        返回上级目录
                      </button>
                    </td>
                  </tr>
                )}
                {rows.map((entry) => (
                  <tr key={`${entry.nodeId}:${entry.nameBase64}`}>
                    <td>
                      {entry.kind === "directory" ? (
                        <button
                          className="file-name"
                          onClick={() =>
                            enter([
                              ...crumbs,
                              {
                                label: entry.name,
                                pathBase64: entry.pathBase64,
                              },
                            ])
                          }
                        >
                          <EntryIcon entry={entry} />
                          <span>{entry.name}</span>
                        </button>
                      ) : (
                        <span className="file-name passive">
                          <EntryIcon entry={entry} />
                          <span>{entry.name}</span>
                          {entry.kind === "symlink" && (
                            <span className="link-label">链接</span>
                          )}
                        </span>
                      )}
                    </td>
                    <td className="muted">
                      {entry.kind === "directory"
                        ? "目录"
                        : entry.kind === "symlink"
                          ? "符号链接"
                          : "文件"}
                    </td>
                    <td
                      className="monospace muted"
                      title={
                        entry.kind === "directory"
                          ? undefined
                          : `${entry.sizeBytes} 字节`
                      }
                    >
                      {entry.kind === "directory"
                        ? "—"
                        : formatBytes(entry.sizeBytes)}
                    </td>
                    <td className="muted date-cell">
                      {new Date(entry.modifiedAt).toLocaleString("zh-CN")}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
        {listing && filtered.length > 0 && (
          <div className="table-footer">
            <span>当前目录 {formatCount(BigInt(filtered.length))} 个项目</span>
            <div className="pagination">
              <button
                className="icon-button"
                aria-label="上一页"
                disabled={page === 0}
                onClick={() => setPage((value) => value - 1)}
              >
                <ArrowLeft size={15} />
              </button>
              <span>
                {page + 1} / {Math.ceil(filtered.length / 100)}
              </span>
              <button
                className="icon-button"
                aria-label="下一页"
                disabled={(page + 1) * 100 >= filtered.length}
                onClick={() => setPage((value) => value + 1)}
              >
                <ChevronRight size={15} />
              </button>
            </div>
          </div>
        )}
      </div>
      <div className="hint-row">
        <LockKeyhole size={15} />
        <span>仅显示目录与文件属性。符号链接不展开目标。</span>
      </div>
    </>
  );
};
