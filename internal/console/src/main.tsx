import { clearOtherActorJournals } from "./api/browser-state";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { createRoot } from "react-dom/client";
import {
  Info,
  ChevronRight,
  HardDrive,
  LayoutGrid,
  LogOut,
  Menu,
  RefreshCw,
  ShieldCheck,
  Users as UsersIcon,
  X,
} from "lucide-react";
import {
  APIError,
  LogoutCleanupError,
  ManagementClient,
  recoverSession,
} from "./api/management";
import { Failure, Loading } from "./components/States";
import { Operations, OperationDetails } from "./components/Operations";
import { Login } from "./pages/Login";
import { Volumes } from "./pages/Volumes";
import { Users } from "./pages/Users";
import { Files } from "./pages/Files";
import {
  errorMessage,
  type ServiceCapabilities,
  type Session,
  type Volume,
} from "./types";
import "./styles.css";
const App = () => {
  const [session, setSession] = useState<Session | null>(null);
  const [identityGeneration, setIdentityGeneration] = useState(0);
  const [checking, setChecking] = useState(true);
  const [sessionError, setSessionError] = useState("");
  const [authMessage, setAuthMessage] = useState("");
  const [sessionRetry, setSessionRetry] = useState(0);
  const [view, setView] = useState<"volumes" | "files" | "users">("volumes");
  const [selectedVolume, setSelectedVolume] = useState<Volume | null>(null);
  const [capabilities, setCapabilities] = useState<ServiceCapabilities | null>(
    null,
  );
  const [capabilitiesError, setCapabilitiesError] = useState("");
  const [capabilitiesRetry, setCapabilitiesRetry] = useState(0);
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const [toast, setToast] = useState("");
  const [revision, setRevision] = useState(0);
  const [actionRevision, setActionRevision] = useState(0);
  const [navigationBusy, setNavigationBusy] = useState(false);
  const [inspectOperation, setInspectOperation] = useState<string | null>(null);
  const leaveGuard = useRef<(() => Promise<boolean>) | null>(null);
  const toastTimer = useRef<ReturnType<typeof setTimeout> | undefined>(
    undefined,
  );
  const sessionRef = useRef<Session | null>(session);
  sessionRef.current = session;
  const sessionChannel = useRef<BroadcastChannel | null>(null);
  const clearIdentity = useCallback(() => {
    sessionRef.current = null;
    setIdentityGeneration((value) => value + 1);
    setSession(null);
    setView("volumes");
    setSelectedVolume(null);
    setCapabilities(null);
    setCapabilitiesError("");
    setSidebarOpen(false);
    setToast("");
    setInspectOperation(null);
    leaveGuard.current = null;
    setRevision(0);
    setActionRevision(0);
  }, []);
  const onAuthLost = useCallback(() => {
    if (sessionRef.current !== session) return;
    clearIdentity();
    setAuthMessage("登录已失效，请重新登录。");
  }, [session, clearIdentity]);
  const api = useMemo(
    () => (session ? new ManagementClient(session, onAuthLost) : null),
    [session, onAuthLost],
  );
  const notify = useCallback(
    (message: string) => {
      if (sessionRef.current !== session) return;
      if (toastTimer.current) clearTimeout(toastTimer.current);
      setToast(message);
      toastTimer.current = setTimeout(() => setToast(""), 7000);
    },
    [session],
  );
  const onPermissionDenied = useCallback(() => {
    const current = sessionRef.current;
    if (!current || current !== session) return;
    void recoverSession()
      .then((value) => {
        if (sessionRef.current !== current) return;
        if (
          current.user.id !== value.user.id ||
          current.csrfToken !== value.csrfToken ||
          current.user.role !== value.user.role ||
          current.user.displayName !== value.user.displayName
        ) {
          clearIdentity();
          try {
            clearOtherActorJournals(value.user.id);
            setSession(value);
          } catch (error) {
            setSessionError(errorMessage(error));
          }
        }
      })
      .catch((error) => {
        if (sessionRef.current !== current) return;
        if (error instanceof APIError && error.status === 401) onAuthLost();
        else notify(`登录状态无法确认：${errorMessage(error)}`);
      });
  }, [session, clearIdentity, onAuthLost, notify]);
  const refresh = useCallback(() => setRevision((value) => value + 1), []);
  const onAction = useCallback(() => {
    setActionRevision((value) => value + 1);
    setRevision((value) => value + 1);
  }, []);
  const registerLeaveGuard = useCallback(
    (guard: (() => Promise<boolean>) | null) => {
      leaveGuard.current = guard;
    },
    [],
  );
  useEffect(() => {
    const channel = new BroadcastChannel("rfs-session");
    sessionChannel.current = channel;
    channel.onmessage = (event) => {
      if (event.data !== "identity-changed") return;
      clearIdentity();
      setChecking(true);
      setSessionRetry((value) => value + 1);
    };
    return () => {
      sessionChannel.current = null;
      channel.close();
    };
  }, [clearIdentity]);
  useEffect(() => {
    let active = true;
    setChecking(true);
    setSessionError("");
    recoverSession()
      .then((value) => {
        if (active) {
          clearOtherActorJournals(value.user.id);
          setSession(value);
        }
      })
      .catch((error) => {
        if (!active) return;
        if (error instanceof APIError && error.status === 401) {
          setSession(null);
        } else setSessionError(errorMessage(error));
      })
      .finally(() => {
        if (active) setChecking(false);
      });
    return () => {
      active = false;
    };
  }, [sessionRetry]);
  useEffect(
    () => () => {
      api?.dispose();
    },
    [api],
  );
  useEffect(() => {
    if (!api) return;
    let active = true;
    setCapabilities(null);
    setCapabilitiesError("");
    api
      .capabilities()
      .then((value) => {
        if (active) setCapabilities(value);
      })
      .catch((error) => {
        if (active) setCapabilitiesError(errorMessage(error));
      });
    return () => {
      active = false;
    };
  }, [api, capabilitiesRetry]);
  useEffect(() => {
    if (!api || view !== "files" || !selectedVolume) return;
    let active = true;
    const id = selectedVolume.id;
    api
      .volume(id)
      .then((volume) => {
        if (active)
          setSelectedVolume((current) =>
            current?.id === id ? volume : current,
          );
      })
      .catch((error) => {
        if (active) {
          setSelectedVolume((current) =>
            current?.id === id
              ? {
                  ...current,
                  summary: undefined,
                  summaryError: { code: "summary_request_failed" },
                }
              : current,
          );
          notify(`统计暂不可用：${errorMessage(error)}`);
        }
      });
    return () => {
      active = false;
    };
  }, [api, view, selectedVolume?.id, revision, notify]);
  useEffect(
    () => () => {
      if (toastTimer.current) clearTimeout(toastTimer.current);
    },
    [],
  );
  const navigate = async (
    next: "volumes" | "files" | "users",
    volume?: Volume,
  ) => {
    if (navigationBusy) return;
    setNavigationBusy(true);
    try {
      if (leaveGuard.current && !(await leaveGuard.current())) return;
      if (next === "users" && session?.user.role !== "admin")
        throw new Error("当前用户不能访问管理员摘要。");
      setSelectedVolume(next === "files" && volume ? volume : null);
      setView(next);
      setSidebarOpen(false);
    } catch (error) {
      notify(errorMessage(error));
    } finally {
      setNavigationBusy(false);
    }
  };
  const logout = async () => {
    if (!api || navigationBusy) return;
    setNavigationBusy(true);
    try {
      if (leaveGuard.current && !(await leaveGuard.current())) return;
      setView("volumes");
      setSelectedVolume(null);
      await api.logout();
      sessionChannel.current?.postMessage("identity-changed");
      clearIdentity();
      setAuthMessage("");
    } catch (error) {
      if (error instanceof LogoutCleanupError) {
        sessionChannel.current?.postMessage("identity-changed");
        clearIdentity();
        setAuthMessage(error.message);
      } else notify(errorMessage(error));
    } finally {
      setNavigationBusy(false);
    }
  };
  const onSession = (value: Session) => {
    clearIdentity();
    clearOtherActorJournals(value.user.id);
    setSession(value);
    sessionChannel.current?.postMessage("identity-changed");
    setAuthMessage("");
  };
  if (checking)
    return (
      <div className="startup-state">
        <Loading message="正在确认登录状态…" />
      </div>
    );
  if (sessionError)
    return (
      <div className="startup-state">
        <Failure
          message={sessionError}
          onRetry={() => setSessionRetry((value) => value + 1)}
        />
      </div>
    );
  if (!session || !api)
    return <Login onSession={onSession} message={authMessage} />;
  return (
    <div className="app-shell" key={identityGeneration}>
      <aside className={`sidebar ${sidebarOpen ? "open" : ""}`}>
        <button
          className="brand"
          onClick={() => void navigate("volumes")}
          disabled={navigationBusy}
        >
          <div className="brand-icon">
            <HardDrive size={21} />
          </div>
          <span>
            Remote FS<small>CONSOLE</small>
          </span>
        </button>
        <div className="instance">
          <span className="status-dot" />
          <div>
            <strong>当前服务</strong>
            <small>{window.location.host}</small>
          </div>
        </div>
        <div className="nav-label">工作空间</div>
        <nav>
          <button
            className={view !== "users" ? "active" : ""}
            onClick={() => void navigate("volumes")}
            disabled={navigationBusy}
          >
            <LayoutGrid size={18} />
            我的 Volumes
          </button>
        </nav>
        {session.user.role === "admin" && (
          <>
            <div className="nav-label administration">管理</div>
            <nav>
              <button
                className={view === "users" ? "active" : ""}
                onClick={() => void navigate("users")}
                disabled={navigationBusy}
              >
                <UsersIcon size={18} />
                用户概览
              </button>
            </nav>
          </>
        )}
        <div className="sidebar-bottom">
          <div className="privacy-note">
            <ShieldCheck size={17} />
            <p>
              文件访问仅限所有者<small>管理员可查看用户与容量摘要</small>
            </p>
          </div>
          <div className="profile">
            <div className="avatar admin-avatar" aria-hidden="true">
              {[...session.user.displayName][0]}
            </div>
            <div>
              <strong>{session.user.displayName}</strong>
              <small>
                {session.user.role === "admin" ? "管理员" : "普通用户"}
              </small>
            </div>
            <button
              className="icon-button"
              aria-label="退出登录"
              onClick={() => void logout()}
              disabled={navigationBusy}
            >
              <LogOut size={17} />
            </button>
          </div>
        </div>
      </aside>
      {sidebarOpen && (
        <button
          className="sidebar-scrim"
          aria-label="关闭导航"
          onClick={() => setSidebarOpen(false)}
        />
      )}
      <main>
        <header className="topbar">
          <div className="topbar-left">
            <button
              className="icon-button mobile-menu"
              aria-label="打开导航"
              onClick={() => setSidebarOpen(true)}
            >
              <Menu size={19} />
            </button>
            <span>{view === "users" ? "管理" : "工作空间"}</span>
            <ChevronRight size={14} />
            <strong>
              {view === "volumes"
                ? "我的 Volumes"
                : view === "users"
                  ? "用户概览"
                  : selectedVolume?.name}
            </strong>
          </div>
          <div className="topbar-right">
            <button
              className="icon-button"
              aria-label="刷新列表"
              disabled={navigationBusy || view === "files"}
              onClick={refresh}
            >
              <RefreshCw size={16} />
            </button>
          </div>
        </header>
        <div className="content">
          <Operations api={api} revision={actionRevision} onSettled={refresh} />
          {view === "volumes" && (
            <Volumes
              api={api}
              onOpen={(volume) => void navigate("files", volume)}
              onNotice={notify}
              onAction={onAction}
              onInspectOperation={setInspectOperation}
              revision={revision}
            />
          )}{" "}
          {view === "users" && session.user.role === "admin" && (
            <Users key={revision} api={api} />
          )}{" "}
          {view === "files" &&
            selectedVolume &&
            (capabilities ? (
              <Files
                key={selectedVolume.id}
                volume={selectedVolume}
                userId={session.user.id}
                csrfToken={session.csrfToken}
                capabilities={capabilities}
                createAction={() => api.createAction()}
                onBack={() => void navigate("volumes")}
                onAuthLost={onAuthLost}
                onPermissionDenied={onPermissionDenied}
                onChanged={refresh}
                onNotice={notify}
                registerLeaveGuard={registerLeaveGuard}
              />
            ) : capabilitiesError ? (
              <Failure
                message={capabilitiesError}
                onRetry={() => setCapabilitiesRetry((value) => value + 1)}
              />
            ) : (
              <Loading message="正在取得文件访问能力…" />
            ))}
        </div>
      </main>
      {inspectOperation && (
        <OperationDetails
          api={api}
          id={inspectOperation}
          onClose={() => setInspectOperation(null)}
        />
      )}
      {toast && (
        <div className="toast" role="status">
          <Info size={18} />
          {toast}
          <button aria-label="关闭提示" onClick={() => setToast("")}>
            <X size={14} />
          </button>
        </div>
      )}
    </div>
  );
};

createRoot(document.getElementById("root")!).render(<App />);
