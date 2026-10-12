import { beforeEach, describe, expect, it, vi } from "vitest";
import {
  APIError,
  login,
  ManagementClient,
  OutcomeUnknown,
  recoverSession,
} from "./management";
import { quotaInput, type Session, type Volume } from "../types";
const session: Session = {
  user: { id: "owner", displayName: "Administrator", role: "admin" },
  csrfToken: "csrf-same-session",
  idleExpiresAt: "2027-01-01T00:00:00Z",
  absoluteExpiresAt: "2027-01-02T00:00:00Z",
};
const operation = {
  id: "operation",
  volumeId: "volume",
  kind: "quota",
  state: "running",
  phase: "applying",
  revision: "1",
  settled: false,
  createdAt: "2026-10-12T00:00:00Z",
  updatedAt: "2026-10-12T00:00:00Z",
};
const volume: Volume = {
  id: "volume",
  ownerUserId: "owner",
  name: "workspace",
  state: "active",
  revision: 9n,
  createdAt: "2026-10-12T00:00:00Z",
  updatedAt: "2026-10-12T00:00:00Z",
  summary: {
    fileCount: 9007199254740993n,
    directoryCount: 3n,
    usedBytes: 9007199254740993n,
    quotaBytes: 9223372036854775807n,
    quotaRevision: 3n,
  },
};
const response = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
beforeEach(() => {
  const data = new Map<string, string>();
  vi.stubGlobal("sessionStorage", {
    getItem: (key: string) => data.get(key) ?? null,
    setItem: (key: string, value: string) => data.set(key, value),
    removeItem: (key: string) => data.delete(key),
  });
});
describe("management wire and authenticated state", () => {
  it("recovers the persistent CSRF token without credentials in browser storage", async () => {
    const fetcher = vi.fn().mockResolvedValue(response(session));
    expect(await recoverSession(fetcher)).toEqual(session);
    const init = fetcher.mock.calls[0][1];
    expect(init.credentials).toBe("same-origin");
    expect(init.cache).toBe("no-store");
    expect(sessionStorage.getItem("csrfToken")).toBeNull();
  });
  it("uses the explicit login marker and real administrator credentials", async () => {
    const fetcher = vi.fn().mockResolvedValue(response(session));
    await login("admin", "password", fetcher);
    const init = fetcher.mock.calls[0][1];
    expect(init.headers["X-RFS-Request"]).toBe("1");
    expect(JSON.parse(init.body)).toEqual({
      loginName: "admin",
      password: "password",
    });
    expect(init.headers.Authorization).toBeUndefined();
  });
  it("preserves string integer quantities beyond JS safe integer", async () => {
    const wire = {
      ...volume,
      revision: "9",
      summary: {
        fileCount: "9007199254740993",
        directoryCount: "3",
        usedBytes: "9007199254740993",
        quotaBytes: "9223372036854775807",
        quotaRevision: "3",
      },
    };
    const api = new ManagementClient(
      session,
      vi.fn(),
      vi.fn().mockResolvedValue(response({ volumes: [wire] })),
    );
    const parsed = (await api.volumes()).items[0];
    expect(parsed.summary?.usedBytes).toBe(9007199254740993n);
    expect(parsed.summary?.quotaBytes).toBe(9223372036854775807n);
  });
  it.each([
    9007199254740993,
    "01",
    "-1",
    "18446744073709551616",
    "9".repeat(10000),
  ])("rejects invalid numeric management fields %s", async (value) => {
    const wire = {
      ...volume,
      revision: "9",
      summary: {
        fileCount: "1",
        directoryCount: "3",
        usedBytes: value,
        quotaBytes: "100",
        quotaRevision: "3",
      },
    };
    const api = new ManagementClient(
      session,
      vi.fn(),
      vi.fn().mockResolvedValue(response({ volumes: [wire] })),
    );
    await expect(api.volumes()).rejects.toThrow();
  });
  it("sends native quota revision rather than renamed management revision", async () => {
    const fetcher = vi
      .fn()
      .mockResolvedValueOnce(
        response({
          subjectId: "owner",
          namespace: "9007199254740993",
          acceptUntil: "2099-01-01T00:00:00Z",
        }),
      )
      .mockResolvedValueOnce(response(operation));
    const api = new ManagementClient(session, vi.fn(), fetcher);
    await api.setQuota(volume, 9007199254740995n);
    const [path, init] = fetcher.mock.calls[1];
    expect(path).toBe("/api/service/v1/volumes/volume/quota");
    expect(init.method).toBe("PUT");
    expect(init.headers["X-CSRF-Token"]).toBe("csrf-same-session");
    expect(JSON.parse(init.body)).toMatchObject({
      expectedRevision: "3",
      quotaBytes: "9007199254740995",
      action: { namespace: "9007199254740993" },
    });
  });
  it("refuses resize when native quota revision is unknown", () => {
    const api = new ManagementClient(session, vi.fn(), vi.fn());
    expect(() =>
      api.setQuota(
        {
          ...volume,
          summary: undefined,
          summaryError: { code: "metadata_unavailable" },
        },
        4n,
      ),
    ).toThrow("配额版本未知");
  });
  it("retains an exact action when a mutation response is lost", async () => {
    const fetcher = vi
      .fn()
      .mockResolvedValueOnce(
        response({
          subjectId: "owner",
          namespace: "5",
          acceptUntil: "2099-01-01T00:00:00Z",
        }),
      )
      .mockRejectedValueOnce(new TypeError("network unavailable"))
      .mockResolvedValueOnce(response(operation));
    const api = new ManagementClient(session, vi.fn(), fetcher);
    await expect(api.createVolume("project", 1024n)).rejects.toBeInstanceOf(
      OutcomeUnknown,
    );
    const pending = api.pending();
    expect(pending).toHaveLength(1);
    const recovered = new ManagementClient(session, vi.fn(), fetcher);
    await recovered.queryAction(pending[0].action);
    expect(fetcher.mock.calls[2][0]).toContain(
      `namespace=5&nonce=${pending[0].action.nonce}`,
    );
    expect(
      fetcher.mock.calls.filter((call) => call[1]?.method === "POST"),
    ).toHaveLength(2);
  });
  it("does not erase pending actions on unsuccessful logout", async () => {
    sessionStorage.setItem("rfs.pending.owner", "[]");
    const api = new ManagementClient(
      session,
      vi.fn(),
      vi
        .fn()
        .mockResolvedValue(
          response({ error: { code: "authentication_unavailable" } }, 503),
        ),
    );
    await expect(api.logout()).rejects.toBeInstanceOf(APIError);
    expect(sessionStorage.getItem("rfs.pending.owner")).toBe("[]");
  });
  it("clears private pending state after confirmed logout", async () => {
    sessionStorage.setItem("rfs.pending.owner", "[]");
    const api = new ManagementClient(
      session,
      vi.fn(),
      vi.fn().mockResolvedValue(new Response(null, { status: 204 })),
    );
    await api.logout();
    expect(sessionStorage.getItem("rfs.pending.owner")).toBeNull();
  });
  it("never turns failed statistics into zero", async () => {
    const api = new ManagementClient(
      session,
      vi.fn(),
      vi.fn().mockResolvedValue(
        response({
          volumes: [
            {
              id: "v",
              ownerUserId: "owner",
              name: "v",
              state: "active",
              revision: "1",
              createdAt: "2026-10-12T00:00:00Z",
              updatedAt: "2026-10-12T00:00:00Z",
              summaryError: { code: "metadata_unavailable" },
            },
          ],
        }),
      ),
    );
    const result = (await api.volumes()).items[0];
    expect(result.summary).toBeUndefined();
    expect(result.summaryError?.code).toBe("metadata_unavailable");
  });
  it.each([
    {
      name: "private",
      id: "secret",
      summary: {
        fileCount: "1",
        directoryCount: "2",
        usedBytes: "3",
        quotaBytes: "4",
      },
    },
    {
      name: "private",
      summary: {
        fileCount: "1",
        directoryCount: "2",
        usedBytes: "3",
        quotaBytes: "4",
        quotaRevision: "1",
      },
    },
  ])("refuses administrative DTO privilege leakage", async (wire) => {
    const api = new ManagementClient(
      session,
      vi.fn(),
      vi.fn().mockResolvedValue(response({ volumes: [wire] })),
    );
    await expect(api.userVolumes("ordinary")).rejects.toThrow("越界字段");
  });
});
describe("quota input", () => {
  it("keeps exact integer bytes at the signed limit", () =>
    expect(quotaInput("9223372036854775807", "B")).toBe(9223372036854775807n));
  it("converts decimal units without JS Number rounding", () =>
    expect(quotaInput("1.5", "GiB")).toBe(1610612736n));
  it.each([
    ["1.000001", "GiB"],
    ["9223372036854775808", "B"],
    ["0", "B"],
    ["1e9", "B"],
  ])("rejects invalid or fractional bytes %s %s", (value, unit) =>
    expect(() => quotaInput(value, unit)).toThrow(),
  );
});

describe("durable management intent and identity lifetime", () => {
  it.each([
    { state: "pending", settled: true },
    { state: "running", settled: true },
    { state: "succeeded", settled: false },
  ])(
    "retains the original action for incoherent receipt %j",
    async (invalid) => {
      const fetcher = vi
        .fn()
        .mockResolvedValueOnce(
          response({
            subjectId: "owner",
            namespace: "5",
            acceptUntil: "2099-01-01T00:00:00Z",
          }),
        )
        .mockRejectedValueOnce(new TypeError("network unavailable"))
        .mockResolvedValueOnce(response({ ...operation, ...invalid }));
      const api = new ManagementClient(session, vi.fn(), fetcher);
      await expect(api.createVolume("original", 1024n)).rejects.toBeInstanceOf(
        OutcomeUnknown,
      );
      const original = api.pending();
      await expect(api.queryAction(original[0].action)).rejects.toThrow(
        "结算结果矛盾",
      );
      expect(api.pending()).toEqual(original);
      expect(fetcher.mock.calls).toHaveLength(3);
    },
  );
  it("queries then resumes the same lost action with immutable inputs", async () => {
    const fetcher = vi
      .fn()
      .mockResolvedValueOnce(
        response({
          subjectId: "owner",
          namespace: "5",
          acceptUntil: "2099-01-01T00:00:00Z",
        }),
      )
      .mockRejectedValueOnce(new TypeError("network unavailable"))
      .mockResolvedValueOnce(response({ error: { code: "not_found" } }, 404))
      .mockResolvedValueOnce(response({ ...operation, kind: "create" }));
    const api = new ManagementClient(session, vi.fn(), fetcher);
    await expect(
      api.createVolume("original-name", 9007199254740995n),
    ).rejects.toBeInstanceOf(OutcomeUnknown);
    const pending = api.pending()[0];
    expect(pending.request.payload).toEqual({
      name: "original-name",
      quotaBytes: "9007199254740995",
    });
    const reloaded = new ManagementClient(session, vi.fn(), fetcher);
    await reloaded.retryPending(pending);
    const original = JSON.parse(fetcher.mock.calls[1][1].body);
    const replay = JSON.parse(fetcher.mock.calls[3][1].body);
    expect(replay).toEqual(original);
    expect(fetcher.mock.calls[2][0]).toContain(
      "/operations?namespace=5&nonce=",
    );
    expect(reloaded.pending()[0].operationId).toBe("operation");
  });
  it("never resubmits a known durable receipt", async () => {
    const fetcher = vi
      .fn()
      .mockResolvedValueOnce(
        response({
          subjectId: "owner",
          namespace: "5",
          acceptUntil: "2099-01-01T00:00:00Z",
        }),
      )
      .mockRejectedValueOnce(new TypeError("network unavailable"))
      .mockResolvedValueOnce(response(operation));
    const api = new ManagementClient(session, vi.fn(), fetcher);
    await expect(api.setQuota(volume, 1024n)).rejects.toBeInstanceOf(
      OutcomeUnknown,
    );
    await api.retryPending(api.pending()[0]);
    expect(fetcher.mock.calls).toHaveLength(3);
    expect(fetcher.mock.calls[2][1].method).toBe("GET");
  });
  it("retains the action when authentication is lost after dispatch", async () => {
    const lost = vi.fn();
    const fetcher = vi
      .fn()
      .mockResolvedValueOnce(
        response({
          subjectId: "owner",
          namespace: "5",
          acceptUntil: "2099-01-01T00:00:00Z",
        }),
      )
      .mockResolvedValueOnce(
        response({ error: { code: "authentication_required" } }, 401),
      );
    const api = new ManagementClient(session, lost, fetcher);
    await expect(api.createVolume("private", 1024n)).rejects.toBeInstanceOf(
      OutcomeUnknown,
    );
    expect(lost).toHaveBeenCalledOnce();
    expect(api.pending()).toHaveLength(1);
  });
  it("binds fetch to its browser global receiver", async () => {
    const fetcher = vi.fn(function (this: unknown) {
      expect(this).toBe(globalThis);
      return Promise.resolve(response({ volumes: [] }));
    });
    const api = new ManagementClient(session, vi.fn(), fetcher as typeof fetch);
    await api.volumes();
    expect(fetcher).toHaveBeenCalledOnce();
  });
  it("does not revive old identity state after disposal", async () => {
    let resolve!: (value: Response) => void;
    const fetcher = vi.fn(
      () =>
        new Promise<Response>((done) => {
          resolve = done;
        }),
    );
    const lost = vi.fn();
    const api = new ManagementClient(session, lost, fetcher as typeof fetch);
    const request = api.volumes();
    api.dispose();
    resolve(response({ error: { code: "authentication_required" } }, 401));
    await expect(request).rejects.toThrow("身份已更改");
    expect(lost).not.toHaveBeenCalled();
  });
});
