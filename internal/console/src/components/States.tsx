import {
  CircleAlert,
  Folder,
  HardDrive,
  Loader2,
  RefreshCw,
} from "lucide-react";
export const Loading = ({ message = "正在加载…" }: { message?: string }) => (
  <div className="loading-state" role="status">
    <Loader2 className="spin" />
    <p>{message}</p>
    <span>正在确认服务的当前状态</span>
  </div>
);
export const Failure = ({
  message,
  onRetry,
}: {
  message: string;
  onRetry: () => void;
}) => (
  <div className="empty-state">
    <div className="empty-icon">
      <CircleAlert />
    </div>
    <h3>请求未完成</h3>
    <p role="alert">{message}</p>
    <button className="button secondary" onClick={onRetry}>
      <RefreshCw size={15} />
      重新加载
    </button>
  </div>
);
export const Empty = ({
  title,
  detail,
  children,
  folder = false,
}: {
  title: string;
  detail: string;
  children?: React.ReactNode;
  folder?: boolean;
}) => (
  <div className="empty-state">
    <div className="empty-icon">{folder ? <Folder /> : <HardDrive />}</div>
    <h3>{title}</h3>
    <p>{detail}</p>
    {children}
  </div>
);
