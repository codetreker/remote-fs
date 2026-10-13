import { useEffect, useRef, useState } from "react";
import {
  ChevronRight,
  File,
  HardDrive,
  LockKeyhole,
  Search,
  ShieldCheck,
} from "lucide-react";
import { ManagementClient, publicError } from "../api/management";
import { Empty, Failure, Loading } from "../components/States";
import {
  errorMessage,
  exactBytes,
  formatBytes,
  formatCount,
  percent,
  type User,
  type UserVolume,
} from "../types";
export const Users = ({ api }: { api: ManagementClient }) => {
  const selectedId = useRef<string | null>(null);
  const generation = useRef(0);
  const [users, setUsers] = useState<User[]>([]);
  const [nextUser, setNextUser] = useState<string | undefined>();
  const [selected, setSelected] = useState<User | null>(null);
  const [volumes, setVolumes] = useState<UserVolume[]>([]);
  const [nextVolume, setNextVolume] = useState<string | undefined>();
  const [search, setSearch] = useState("");
  const [loadingUsers, setLoadingUsers] = useState(true);
  const [loadingVolumes, setLoadingVolumes] = useState(false);
  const [usersError, setUsersError] = useState("");
  const [volumesError, setVolumesError] = useState("");
  const [retryUsers, setRetryUsers] = useState(0);
  const [retryVolumes, setRetryVolumes] = useState(0);
  const [moreLoading, setMoreLoading] = useState(false);
  useEffect(() => {
    ++generation.current;
    const abort = new AbortController();
    setLoadingUsers(true);
    setUsersError("");
    setUsers([]);
    setSelected(null);
    api
      .users(undefined, abort.signal)
      .then((page) => {
        if (page.items.some((user) => user.role !== "user"))
          throw new Error("普通用户列表返回了不符合权限边界的角色。");
        setUsers(page.items);
        setNextUser(page.nextAfter);
        if (page.items.length > 0) setSelected(page.items[0]);
      })
      .catch((error) => {
        if (!abort.signal.aborted) setUsersError(errorMessage(error));
      })
      .finally(() => {
        if (!abort.signal.aborted) setLoadingUsers(false);
      });
    return () => abort.abort();
  }, [api, retryUsers]);
  useEffect(() => {
    setVolumes([]);
    setNextVolume(undefined);
    setVolumesError("");
    if (!selected) return;
    const abort = new AbortController();
    setLoadingVolumes(true);
    api
      .userVolumes(selected.id, undefined, abort.signal)
      .then((page) => {
        setVolumes(page.items);
        setNextVolume(page.nextAfter);
      })
      .catch((error) => {
        if (!abort.signal.aborted) setVolumesError(errorMessage(error));
      })
      .finally(() => {
        if (!abort.signal.aborted) setLoadingVolumes(false);
      });
    return () => abort.abort();
  }, [api, selected, retryVolumes]);
  selectedId.current = selected?.id ?? null;
  const loadMoreUsers = async () => {
    if (nextUser === undefined) return;
    const version = generation.current;
    setMoreLoading(true);
    try {
      const page = await api.users(nextUser);
      if (version !== generation.current) return;
      if (page.items.some((user) => user.role !== "user"))
        throw new Error("列表返回了未知角色。");
      const ids = new Set(users.map((user) => user.id));
      if (
        page.items.some((user) => ids.has(user.id)) ||
        page.nextAfter === nextUser
      )
        throw new Error("列表在分页期间发生变化，请重新加载。");
      setUsers((previous) => [...previous, ...page.items]);
      setNextUser(page.nextAfter);
    } catch (error) {
      setUsersError(errorMessage(error));
    } finally {
      setMoreLoading(false);
    }
  };
  const loadMoreVolumes = async () => {
    if (!selected || nextVolume === undefined) return;
    const userId = selected.id;
    setMoreLoading(true);
    try {
      const page = await api.userVolumes(userId, nextVolume);
      if (selectedId.current !== userId) return;
      if (page.nextAfter === nextVolume)
        throw new Error("服务返回了重复分页游标。");
      setVolumes((previous) => [...previous, ...page.items]);
      setNextVolume(page.nextAfter);
    } catch (error) {
      setVolumesError(errorMessage(error));
    } finally {
      setMoreLoading(false);
    }
  };
  const unknown =
    loadingVolumes ||
    volumesError !== "" ||
    volumes.some((volume) => volume.summaryError);
  const totals = volumes.reduce(
    (total, volume) => {
      if (volume.summary) {
        total.used += volume.summary.usedBytes;
        total.quota += volume.summary.quotaBytes;
      }
      return total;
    },
    { used: 0n, quota: 0n },
  );
  const shortId = (id: string) =>
    id.length > 16 ? `${id.slice(0, 8)}…${id.slice(-4)}` : id;
  return (
    <>
      <div className="page-heading">
        <div>
          <span className="eyebrow">ADMINISTRATION</span>
          <h1>
            用户概览
            <span className="read-only-label">
              <LockKeyhole size={12} />
              只读
            </span>
          </h1>
          <p>查看普通用户及其 volume 容量摘要。</p>
        </div>
      </div>
      <div className="privacy-banner">
        <div className="privacy-icon">
          <ShieldCheck size={22} />
        </div>
        <div>
          <strong>文件内容始终属于 volume 所有者</strong>
          <p>
            管理员仅可查看用户和统计摘要，无权访问其他用户的文件列表或内容。
          </p>
        </div>
        <span className="privacy-pill">所有权隔离</span>
      </div>
      {loadingUsers ? (
        <div className="panel">
          <Loading message="正在加载用户…" />
        </div>
      ) : usersError ? (
        <div className="panel">
          <Failure
            message={usersError}
            onRetry={() => setRetryUsers((value) => value + 1)}
          />
        </div>
      ) : users.length === 0 ? (
        <div className="panel">
          <Empty
            title="还没有普通用户"
            detail="普通用户将在身份系统接入后出现。此处仅提供只读摘要。"
          />
        </div>
      ) : (
        <div className="users-layout">
          <div className="panel user-list-panel">
            <div className="user-list-heading">
              <strong>普通用户</strong>
              <span>{users.length}</span>
            </div>
            <div className="user-search">
              <Search size={16} />
              <input
                aria-label="搜索已加载用户"
                placeholder="搜索姓名或 ID…"
                value={search}
                onChange={(event) => setSearch(event.target.value)}
              />
            </div>
            {users
              .filter((user) =>
                (user.displayName + user.id)
                  .toLocaleLowerCase()
                  .includes(search.toLocaleLowerCase()),
              )
              .map((user, index) => (
                <button
                  className={`user-item ${selected?.id === user.id ? "selected" : ""}`}
                  key={user.id}
                  aria-label={`查看${user.displayName}的摘要`}
                  onClick={() => setSelected(user)}
                >
                  <div
                    className={`avatar ${["violet", "blue", "green", "orange"][index % 4]}`}
                    aria-hidden="true"
                  >
                    {[...user.displayName][0]}
                  </div>
                  <div>
                    <strong>{user.displayName}</strong>
                    <small title={user.id}>用户 ID：{shortId(user.id)}</small>
                  </div>
                  <ChevronRight size={15} />
                </button>
              ))}
            {nextUser !== undefined && (
              <button
                className="button secondary user-load-more"
                disabled={moreLoading}
                onClick={() => void loadMoreUsers()}
              >
                {moreLoading ? "加载中…" : "加载更多用户"}
              </button>
            )}
            <div className="user-list-foot">
              普通用户登录将在身份系统接入后开放。
            </div>
          </div>
          <div className="user-detail">
            {selected && (
              <>
                <div className="user-detail-heading">
                  <div className="avatar large violet" aria-hidden="true">
                    {[...selected.displayName][0]}
                  </div>
                  <div>
                    <h2>{selected.displayName}</h2>
                    <p title={selected.id}>用户 ID：{shortId(selected.id)}</p>
                  </div>
                  <span className="user-role">普通用户</span>
                </div>
                <div className="user-stats">
                  <div>
                    <span>当前列表的 volumes</span>
                    <strong>
                      {loadingVolumes || volumesError ? "—" : volumes.length}
                    </strong>
                  </div>
                  <div>
                    <span>当前列表的已用空间</span>
                    <strong
                      title={unknown ? "统计暂不可用" : exactBytes(totals.used)}
                    >
                      {unknown ? "—" : formatBytes(totals.used)}
                    </strong>
                  </div>
                  <div>
                    <span>当前列表的总配额</span>
                    <strong
                      title={
                        unknown ? "统计暂不可用" : exactBytes(totals.quota)
                      }
                    >
                      {unknown ? "—" : formatBytes(totals.quota)}
                    </strong>
                  </div>
                </div>
                <div className="detail-section-label">
                  <h3>Volume 摘要</h3>
                  <span>
                    <LockKeyhole size={13} />
                    仅统计信息
                  </span>
                </div>
                <div className="panel user-volume-panel">
                  {loadingVolumes ? (
                    <Loading message="正在读取权威统计…" />
                  ) : volumesError ? (
                    <Failure
                      message={volumesError}
                      onRetry={() => setRetryVolumes((value) => value + 1)}
                    />
                  ) : volumes.length === 0 ? (
                    <Empty
                      title="该用户还没有 volume"
                      detail="用户创建 volume 后会在这里显示统计摘要。"
                    />
                  ) : (
                    <div className="table-scroll">
                      <table className="data-table summary-table">
                        <thead>
                          <tr>
                            <th>名称</th>
                            <th>文件数</th>
                            <th>目录数</th>
                            <th>已用 / 配额</th>
                          </tr>
                        </thead>
                        <tbody>
                          {volumes.map((volume, index) => (
                            <tr key={`${index}:${volume.name}`}>
                              <td>
                                <div className="summary-volume-name">
                                  <HardDrive size={17} />
                                  <strong>{volume.name}</strong>
                                </div>
                                {volume.summaryError && (
                                  <small
                                    className="summary-error"
                                    title={publicError(
                                      volume.summaryError.code,
                                    )}
                                  >
                                    {publicError(volume.summaryError.code)}
                                  </small>
                                )}
                              </td>
                              <td>
                                {volume.summary
                                  ? formatCount(volume.summary.fileCount)
                                  : "—"}
                              </td>
                              <td>
                                {volume.summary
                                  ? formatCount(volume.summary.directoryCount)
                                  : "—"}
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
                                        style={{
                                          width: `${percent(volume.summary.usedBytes, volume.summary.quotaBytes)}%`,
                                        }}
                                      />
                                    </div>
                                  </div>
                                ) : (
                                  "—"
                                )}
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  )}
                </div>
                {nextVolume !== undefined && (
                  <div className="load-more">
                    <button
                      className="button secondary"
                      disabled={moreLoading}
                      onClick={() => void loadMoreVolumes()}
                    >
                      {moreLoading ? "加载中…" : "加载更多摘要"}
                    </button>
                  </div>
                )}
                <p className="summary-note">
                  <File size={14} />
                  文件数包含符号链接；目录数不包含 volume 根目录。
                </p>
              </>
            )}
          </div>
        </div>
      )}
    </>
  );
};
