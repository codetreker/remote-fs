import { useEffect, useState } from "react";
import { Modal } from "./Modal";
import { CheckCircle2, CircleAlert, Loader2, RefreshCw } from "lucide-react";
import {
  APIError,
  ManagementClient,
  publicError,
  type PendingAction,
} from "../api/management";
import { errorMessage, type Operation } from "../types";
type Receipt = {
  pending: PendingAction;
  operation?: Operation;
  error?: string;
  retryable?: boolean;
};
const phases: Record<string, string> = {
  accepted: "已接收",
  initializing: "正在初始化",
  bound: "已绑定存储",
  applying: "正在应用",
  frozen: "正在停止新操作",
  "verified-close": "已确认关闭",
  quarantined: "已隔离数据",
  reclaiming: "正在回收数据",
  completed: "已完成",
};
const kinds: Record<string, string> = {
  create: "创建 volume",
  rename: "修改名称",
  quota: "调整配额",
  delete: "删除 volume",
};
export const Operations = ({
  api,
  revision,
  onSettled,
}: {
  api: ManagementClient;
  revision: number;
  onSettled: () => void;
}) => {
  const [receipts, setReceipts] = useState<Receipt[]>([]);
  const [error, setError] = useState("");
  const [retry, setRetry] = useState(0);
  useEffect(() => {
    let active = true;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const load = async () => {
      try {
        const pending = api.pending();
        const next: Receipt[] = [];
        let settled = false;
        let keepPolling = false;
        for (const item of pending) {
          try {
            const operation = item.operationId
              ? await api.operation(item.operationId)
              : await api.queryAction(item.action);
            next.push({ pending: item, operation });
            if (operation.settled) {
              api.forget(item.action);
              settled = true;
            } else keepPolling = true;
          } catch (error) {
            next.push({
              pending: item,
              error: errorMessage(error),
              retryable: error instanceof APIError && error.status === 404,
            });
          }
        }
        if (!active) return;
        setReceipts(next);
        setError("");
        if (settled) onSettled();
        if (keepPolling) timer = setTimeout(() => void load(), 2000);
      } catch (error) {
        if (active) setError(errorMessage(error));
      }
    };
    void load();
    return () => {
      active = false;
      if (timer) clearTimeout(timer);
    };
  }, [api, revision, retry]);
  if (error)
    return (
      <div className="alert danger">
        <CircleAlert size={18} />
        <div>
          <strong>无法读取待确认操作</strong>
          <p>{error}</p>
        </div>
        <button
          className="button secondary"
          onClick={() => setRetry((value) => value + 1)}
        >
          重试
        </button>
      </div>
    );
  if (receipts.length === 0) return null;
  return (
    <section className="operation-panel panel" aria-label="持久操作进度">
      <div className="operation-panel-heading">
        <strong>操作进度</strong>
        <button
          className="icon-button"
          aria-label="查询操作回执"
          onClick={() => setRetry((value) => value + 1)}
        >
          <RefreshCw size={15} />
        </button>
      </div>
      {receipts.map((receipt) => {
        const operation = receipt.operation;
        return (
          <div
            className="operation-row"
            key={`${receipt.pending.action.namespace}:${receipt.pending.action.nonce}`}
          >
            {operation?.settled && operation.state === "succeeded" ? (
              <CheckCircle2 className="operation-ok" size={18} />
            ) : receipt.error || operation?.error ? (
              <CircleAlert className="operation-fail" size={18} />
            ) : (
              <Loader2 className="spin" size={18} />
            )}
            <div>
              <strong>
                {kinds[receipt.pending.kind] ?? receipt.pending.kind}
              </strong>
              <p>
                {receipt.error
                  ? `当前结果尚未确认：${receipt.error}`
                  : operation?.error
                    ? publicError(operation.error.code)
                    : operation?.settled
                      ? operation.state === "succeeded"
                        ? "操作已完成并结算。"
                        : "操作失败并已结算。"
                      : operation
                        ? `${phases[operation.phase] ?? operation.phase}；等待最终结算。`
                        : "等待持久回执。"}
              </p>
              <small>
                动作 {receipt.pending.action.namespace}:
                {receipt.pending.action.nonce}
              </small>
              {operation && <small>操作 ID：{operation.id}</small>}
            </div>
            {receipt.error && (
              <div className="heading-actions">
                <button
                  className="button secondary"
                  onClick={() => setRetry((value) => value + 1)}
                >
                  查询回执
                </button>
                {receipt.retryable && (
                  <button
                    className="button secondary"
                    onClick={() =>
                      void api
                        .retryPending(receipt.pending)
                        .then(() => setRetry((value) => value + 1))
                        .catch((error) => setError(errorMessage(error)))
                    }
                  >
                    继续原操作
                  </button>
                )}
              </div>
            )}
          </div>
        );
      })}
    </section>
  );
};

export const OperationDetails = ({
  api,
  id,
  onClose,
}: {
  api: ManagementClient;
  id: string;
  onClose: () => void;
}) => {
  const [operation, setOperation] = useState<Operation>();
  const [error, setError] = useState("");
  const [retry, setRetry] = useState(0);
  useEffect(() => {
    let active = true;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const load = async () => {
      try {
        const receipt = await api.operation(id);
        if (!active) return;
        setOperation(receipt);
        setError("");
        if (!receipt.settled) timer = setTimeout(() => void load(), 2000);
      } catch (error) {
        if (active) {
          setError(errorMessage(error));
          setOperation(undefined);
        }
      }
    };
    void load();
    return () => {
      active = false;
      if (timer) clearTimeout(timer);
    };
  }, [api, id, retry]);
  return (
    <Modal title="持久操作进度" onClose={onClose}>
      <div className="modal-body">
        <div className="form-note">操作 ID：{id}</div>
        {error ? (
          <div className="dialog-error" role="alert">
            <CircleAlert size={16} />
            {error}
          </div>
        ) : operation ? (
          <div className="operation-detail">
            <strong>{kinds[operation.kind] ?? operation.kind}</strong>
            <p>阶段：{phases[operation.phase] ?? operation.phase}</p>
            <p>
              {operation.settled
                ? operation.state === "succeeded"
                  ? "操作已完成并结算。"
                  : "操作已失败并结算。"
                : "最终责任尚未结算，服务会继续恢复或清理。"}
            </p>
            {operation.error && (
              <div className="dialog-error" role="alert">
                <CircleAlert size={16} />
                {publicError(operation.error.code)}
              </div>
            )}
          </div>
        ) : (
          <div className="loading-state">
            <Loader2 className="spin" />
            <p>正在查询操作…</p>
          </div>
        )}
      </div>
      <div className="modal-footer">
        <button
          className="button secondary"
          onClick={() => setRetry((value) => value + 1)}
        >
          查询最新状态
        </button>
        <button className="button primary" onClick={onClose}>
          关闭
        </button>
      </div>
    </Modal>
  );
};
