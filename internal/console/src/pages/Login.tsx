import { useState } from "react";
import {
  ArrowRight,
  CircleAlert,
  HardDrive,
  Loader2,
  ShieldCheck,
} from "lucide-react";
import { login } from "../api/management";
import { errorMessage, type Session } from "../types";
export const Login = ({
  onSession,
  message,
}: {
  onSession: (session: Session) => void;
  message?: string;
}) => {
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const submit = async (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const data = new FormData(event.currentTarget);
    setBusy(true);
    setError("");
    try {
      const session = await login(
        String(data.get("loginName")),
        String(data.get("password")),
      );
      onSession(session);
    } catch (error) {
      setError(errorMessage(error));
    } finally {
      setBusy(false);
    }
  };
  return (
    <div className="login-page">
      <div className="login-brand">
        <div className="brand-icon">
          <HardDrive size={22} />
        </div>
        <span>Remote FS</span>
      </div>
      <div className="login-card">
        <span className="eyebrow">CONSOLE</span>
        <h1>欢迎回来</h1>
        <p>登录以管理你的 volumes 和文件。</p>
        <form onSubmit={(event) => void submit(event)}>
          <label>
            管理员账号
            <input
              name="loginName"
              autoComplete="username"
              required
              disabled={busy}
            />
          </label>
          <label>
            密码
            <input
              name="password"
              type="password"
              autoComplete="current-password"
              required
              disabled={busy}
            />
          </label>
          {(error || message) && (
            <div className="dialog-error" role="alert">
              <CircleAlert size={16} />
              {error || message}
            </div>
          )}
          <button className="button primary wide" type="submit" disabled={busy}>
            {busy ? (
              <>
                <Loader2 size={16} className="spin" />
                正在登录…
              </>
            ) : (
              <>
                登录
                <ArrowRight size={16} />
              </>
            )}
          </button>
        </form>
        <div className="login-notice">
          <ShieldCheck size={17} />
          <span>管理员仅可访问自己 volume 内的文件。</span>
        </div>
      </div>
      <div className="login-grid" />
    </div>
  );
};
