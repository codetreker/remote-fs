import {
  useEffect,
  useId,
  useRef,
  useState,
  type FormEvent,
  type ReactNode,
} from "react";
import { CircleAlert, FileText, Loader2, Upload, X } from "lucide-react";
import { displayRawName } from "../api/rawnames";

export type FileDialogKind =
  "folder" | "rename" | "move" | "remove" | "upload" | "replace";
export type FileDialog = {
  kind: FileDialogKind;
  displayName?: string;
  targetId?: string;
};
export type FileDialogInput = { name: string; file?: globalThis.File };
type Props = {
  dialog: FileDialog;
  busy: boolean;
  error: string;
  progress?: { loaded: number; total: number };
  destination?: ReactNode;
  destinationReady?: boolean;
  uploadLimit?: bigint;
  onSubmit: (input: FileDialogInput) => Promise<void>;
  onClose: () => void;
  onCancelUpload: () => void;
};

const titles: Record<FileDialogKind, string> = {
  folder: "新建目录",
  rename: "重命名",
  move: "移动到…",
  remove: "删除项目",
  upload: "上传文件",
  replace: "替换文件",
};
const bytes = (value: number) => `${value.toLocaleString()} 字节`;

export const FileDialogs = ({
  dialog,
  busy,
  error,
  progress,
  destination,
  destinationReady,
  uploadLimit,
  onSubmit,
  onClose,
  onCancelUpload,
}: Props) => {
  const [name, setName] = useState("");
  const [file, setFile] = useState<globalThis.File>();
  const titleId = useId();
  const errorId = useId();
  const root = useRef<HTMLDivElement>(null);
  const originalFocus = useRef<HTMLElement | null>(null);
  useEffect(() => {
    originalFocus.current = document.activeElement as HTMLElement;
    root.current?.querySelector<HTMLElement>("input,button")?.focus();
    return () => originalFocus.current?.focus();
  }, []);
  useEffect(() => {
    const key = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        event.preventDefault();
        if (!busy) onClose();
      }
      if (event.key !== "Tab" || !root.current) return;
      const focusable = [
        ...root.current.querySelectorAll<HTMLElement>(
          'button:not([disabled]),input:not([disabled]),select:not([disabled]),[tabindex="0"]',
        ),
      ];
      const first = focusable[0];
      const last = focusable[focusable.length - 1];
      if (event.shiftKey && document.activeElement === first) {
        event.preventDefault();
        last?.focus();
      } else if (!event.shiftKey && document.activeElement === last) {
        event.preventDefault();
        first?.focus();
      }
    };
    document.addEventListener("keydown", key);
    return () => document.removeEventListener("keydown", key);
  }, [busy, onClose]);
  const submit = (event: FormEvent) => {
    event.preventDefault();
    void onSubmit({ name, file });
  };
  const transfer = dialog.kind === "upload" || dialog.kind === "replace";
  const fileTooLarge =
    file !== undefined &&
    uploadLimit !== undefined &&
    BigInt(file.size) > uploadLimit;
  return (
    <div
      className="modal-backdrop"
      onMouseDown={(event) => {
        if (event.target === event.currentTarget && !busy) onClose();
      }}
    >
      <div
        className="modal"
        role="dialog"
        aria-modal="true"
        aria-labelledby={titleId}
        aria-describedby={error ? errorId : undefined}
        ref={root}
      >
        <div className="modal-header">
          <h2 id={titleId}>{titles[dialog.kind]}</h2>
          <button
            className="icon-button"
            aria-label="关闭对话框"
            disabled={busy}
            onClick={onClose}
          >
            <X size={18} />
          </button>
        </div>
        <form onSubmit={submit}>
          <div className="modal-body">
            {dialog.displayName && (
              <p className="modal-intro">所选项目：{dialog.displayName}</p>
            )}
            {dialog.kind === "remove" && (
              <div className="delete-warning">
                <CircleAlert size={19} />
                <div>
                  <strong>确认删除 {dialog.displayName}？</strong>
                  <p>
                    删除立即移除当前名字，不提供回收站。非空目录会拒绝删除。符号链接仅删除链接本身。
                  </p>
                </div>
              </div>
            )}
            {dialog.kind === "replace" && (
              <div className="delete-warning">
                <CircleAlert size={19} />
                <div>
                  <strong>替换所选文件的名字绑定</strong>
                  <p>
                    新上传的文件将替换 {dialog.displayName}
                    。已打开的旧文件保持原对象；若所选项目发生变化，本次替换会报告冲突。
                  </p>
                </div>
              </div>
            )}
            {(dialog.kind === "folder" || dialog.kind === "rename") && (
              <label>
                {dialog.kind === "folder" ? "目录名称" : "新名称"}
                <input
                  name="name"
                  required
                  disabled={busy}
                  autoComplete="off"
                  value={name}
                  placeholder={
                    dialog.kind === "rename"
                      ? "输入新的 UTF-8 名称"
                      : "目录名称"
                  }
                  onChange={(event) => setName(event.target.value)}
                />
              </label>
            )}
            {dialog.kind === "move" && (
              <>
                <p className="muted">选择同一 volume 内的目标目录。</p>
                {destination}
              </>
            )}
            {transfer && (
              <>
                <label className="upload-zone">
                  <input
                    type="file"
                    aria-label="选择上传文件"
                    disabled={busy}
                    onChange={(event) => setFile(event.target.files?.[0])}
                  />
                  <div className="upload-icon">
                    <Upload size={22} />
                  </div>
                  <strong>选择要上传的文件</strong>
                  <span>
                    {uploadLimit === undefined
                      ? "正在确认服务的上传上限…"
                      : `单文件上限 ${uploadLimit.toLocaleString()} 字节`}
                  </span>
                </label>
                {file && (
                  <div className="upload-selection">
                    <div>
                      <FileText size={15} />
                      <span>
                        {displayRawName(new TextEncoder().encode(file.name))}
                      </span>
                      <small title={bytes(file.size)}>{bytes(file.size)}</small>
                    </div>
                  </div>
                )}
                {fileTooLarge && (
                  <div className="dialog-error" role="alert">
                    <CircleAlert size={15} />
                    文件超过配置的上传上限。
                  </div>
                )}
                {dialog.kind === "upload" && (
                  <div className="form-note">
                    <CircleAlert size={15} />
                    上传仅创建新名字。同名项目存在时会报告冲突；替换需要从已选文件的操作菜单明确发起。
                  </div>
                )}
              </>
            )}
            {dialog.targetId && (
              <div className="form-note">
                所选对象 ID：{dialog.targetId}。提交时会再次核对目录和对象身份。
              </div>
            )}
            {progress && (
              <div className="upload-progress" role="status">
                <div className="usage-track">
                  <i
                    style={{
                      width: `${progress.total === 0 ? 100 : Math.min(100, (progress.loaded / progress.total) * 100)}%`,
                    }}
                  />
                </div>
                <span>
                  {progress.loaded < progress.total
                    ? `已传输 ${bytes(progress.loaded)} / ${bytes(progress.total)}`
                    : "字节已发送，等待确认提交"}
                </span>
              </div>
            )}
            {error && (
              <div className="dialog-error" id={errorId} role="alert">
                <CircleAlert size={16} />
                {error}
              </div>
            )}
          </div>
          <div className="modal-footer">
            {busy && transfer ? (
              <button
                type="button"
                className="button secondary"
                onClick={onCancelUpload}
              >
                取消上传
              </button>
            ) : (
              <button
                type="button"
                className="button secondary"
                disabled={busy}
                onClick={onClose}
              >
                取消
              </button>
            )}
            <button
              type="submit"
              className={`button ${dialog.kind === "remove" ? "danger-button" : "primary"}`}
              disabled={
                busy ||
                (transfer &&
                  (!file || fileTooLarge || uploadLimit === undefined)) ||
                (dialog.kind === "move" && !destinationReady)
              }
            >
              {busy && <Loader2 className="spin" size={15} />}
              {busy
                ? "正在确认操作…"
                : dialog.kind === "remove"
                  ? "确认删除"
                  : dialog.kind === "replace"
                    ? "确认替换"
                    : dialog.kind === "move"
                      ? "移动到此目录"
                      : "确认"}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
};
