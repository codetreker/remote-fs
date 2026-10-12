import { describe, expect, it, vi } from "vitest";
import {
  APIError,
  login,
  ManagementClient,
  MutationOutcomeUnknown,
  recoverSession,
} from "./management";
import { quotaInput, type Session, type Volume } from "../types";
const session: Session = {
  user: { id: "owner", displayName: "Administrator", role: "admin" },
  csrfToken: "csrf-same-session",
  idleExpiresAt: "2027-01-01T00:00:00Z",
  absoluteExpiresAt: "2027-01-02T00:00:00Z",
};
const wireVolume = {
  id: "volume",
  ownerUserId: "owner",
  name: "workspace",
  state: "active",
  revision: "9",
  createdAt: "2026-10-12T00:00:00Z",
  updatedAt: "2026-10-12T00:00:00Z",
  summary: {
    fileCount: "9007199254740993",
    directoryCount: "3",
    usedBytes: "9007199254740993",
    quotaBytes: "9223372036854775807",
  },
};
const volume: Volume = {
  ...wireVolume,
  revision: 9n,
  summary: {
    fileCount: 9007199254740993n,
    directoryCount: 3n,
    usedBytes: 9007199254740993n,
    quotaBytes: 9223372036854775807n,
  },
};
const response = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
describe("management wire and authenticated state", () => {
  it("recovers persistent CSRF without browser credential storage", async () => {
    const fetcher = vi.fn().mockResolvedValue(response(session));
    expect(await recoverSession(fetcher)).toEqual(session);
    expect(fetcher.mock.calls[0][1]).toMatchObject({
      credentials: "same-origin",
      cache: "no-store",
    });
  });
  it("sends the guarded login header", async () => {
    const fetcher = vi.fn().mockResolvedValue(response(session));
    await login("admin", "password", fetcher);
    expect(fetcher.mock.calls[0][1].headers["X-RFS-Request"]).toBe("1");
    expect(JSON.parse(fetcher.mock.calls[0][1].body)).toEqual({
      loginName: "admin",
      password: "password",
    });
  });
  it("keeps exact quantities above browser numeric precision", async () => {
    const api = new ManagementClient(
      session,
      vi.fn(),
      vi.fn().mockResolvedValue(response({ volumes: [wireVolume] })),
    );
    expect((await api.volumes()).items[0]).toEqual(volume);
  });
  it("retains unknown statistics as explicit error", async () => {
    const row = {
      ...wireVolume,
      summary: undefined,
      summaryError: { code: "metadata_unavailable" },
    };
    const api = new ManagementClient(
      session,
      vi.fn(),
      vi.fn().mockResolvedValue(response({ volumes: [row] })),
    );
    const parsed = (await api.volumes()).items[0];
    expect(parsed.summary).toBeUndefined();
    expect(parsed.summaryError?.code).toBe("metadata_unavailable");
  });
  it("refuses a missing summary branch", async () => {
    const api = new ManagementClient(
      session,
      vi.fn(),
      vi
        .fn()
        .mockResolvedValue(
          response({ volumes: [{ ...wireVolume, summary: undefined }] }),
        ),
    );
    await expect(api.volumes()).rejects.toThrow("确切结果");
  });
  it("uses direct confirmed CRUD with CSRF and immutable creation quota", async () => {
    const fetcher = vi
      .fn()
      .mockResolvedValueOnce(response(wireVolume, 201))
      .mockResolvedValueOnce(response({ ...wireVolume, name: "renamed" }))
      .mockResolvedValueOnce(new Response(null, { status: 204 }));
    const api = new ManagementClient(session, vi.fn(), fetcher);
    await api.createVolume("workspace", 9007199254740993n);
    await api.renameVolume(volume, "renamed");
    await api.deleteVolume(volume);
    expect(fetcher.mock.calls.map((call) => call[1].method)).toEqual([
      "POST",
      "PATCH",
      "DELETE",
    ]);
    expect(JSON.parse(fetcher.mock.calls[0][1].body)).toEqual({
      name: "workspace",
      quotaBytes: "9007199254740993",
    });
    expect(JSON.parse(fetcher.mock.calls[1][1].body)).toEqual({
      name: "renamed",
    });
    expect(fetcher.mock.calls[2][1].body).toBeUndefined();
    expect(
      fetcher.mock.calls.every(
        (call) => call[1].headers["X-CSRF-Token"] === session.csrfToken,
      ),
    ).toBe(true);
  });
  it("reports lost mutation responses as unknown without replay", async () => {
    const fetcher = vi
      .fn()
      .mockRejectedValue(new TypeError("network unavailable"));
    const api = new ManagementClient(session, vi.fn(), fetcher);
    await expect(api.createVolume("workspace", 1024n)).rejects.toBeInstanceOf(
      MutationOutcomeUnknown,
    );
    expect(fetcher).toHaveBeenCalledOnce();
  });
  it("keeps confirmed HTTP rejection distinct from unknown outcome", async () => {
    const api = new ManagementClient(
      session,
      vi.fn(),
      vi
        .fn()
        .mockResolvedValue(response({ error: { code: "access_denied" } }, 403)),
    );
    await expect(api.deleteVolume(volume)).rejects.toBeInstanceOf(APIError);
  });
  it("reports server failures after dispatch as unknown without replay", async () => {
    const fetcher = vi
      .fn()
      .mockResolvedValue(
        response({ error: { code: "storage_unavailable" } }, 503),
      );
    const api = new ManagementClient(session, vi.fn(), fetcher);
    await expect(api.createVolume("workspace", 1024n)).rejects.toBeInstanceOf(
      MutationOutcomeUnknown,
    );
    expect(fetcher).toHaveBeenCalledOnce();
  });
  it("clears access only after confirmed logout", async () => {
    const api = new ManagementClient(
      session,
      vi.fn(),
      vi.fn().mockResolvedValue(new Response(null, { status: 204 })),
    );
    await api.logout();
    await expect(api.volumes()).rejects.toThrow("身份已更改");
  });
  it("does not accept a malformed logout success", async () => {
    const api = new ManagementClient(
      session,
      vi.fn(),
      vi.fn().mockResolvedValue(response({})),
    );
    await expect(api.logout()).rejects.toThrow("注销响应无效");
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
describe("owner readonly metadata and administrator privacy", () => {
  it("keeps a deep-parent directory compact and derives exact binary child paths", async () => {
    const parentBytes = Array.from({ length: 16 }, () => "p".repeat(127)).join(
      "/",
    );
    const entries = Array.from({ length: 25000 }, (_, index) => ({
      name: `entry-${index}`,
      nameBase64: btoa(`entry-${index}`),
      nodeId: (9007199254740993n + BigInt(index)).toString(),
      kind: "file",
      sizeBytes: "9007199254740993",
      modifiedAt: "2026-10-12T00:00:00Z",
    }));
    entries[24999] = { ...entries[24999], name: "\\xFF", nameBase64: "/w==" };
    const body = { pathBase64: btoa(parentBytes), entries };
    const payload = JSON.stringify(body);
    const compactBytes = new TextEncoder().encode(payload).byteLength;
    expect(compactBytes).toBeLessThan(64 * 1024 * 1024);
    expect(
      compactBytes + entries.length * body.pathBase64.length,
    ).toBeGreaterThan(64 * 1024 * 1024);
    expect(payload.match(/"pathBase64"/g)).toHaveLength(1);
    const api = new ManagementClient(
      session,
      vi.fn(),
      vi.fn().mockResolvedValue(response(body)),
    );
    const listing = await api.files("volume", body.pathBase64);
    expect(listing.entries).toHaveLength(25000);
    expect(listing.entries[0].pathBase64).toBe(btoa(`${parentBytes}/entry-0`));
    expect(listing.entries[24999]).toMatchObject({
      nameBase64: "/w==",
      pathBase64: btoa(`${parentBytes}/\xff`),
      nodeId: 9007199254765992n,
      sizeBytes: 9007199254740993n,
    });
  });
  it("reads a 5000-entry directory above 1 MiB with exact identity and path fields", async () => {
    const entries = Array.from({ length: 5000 }, (_, index) => ({
      name: `${index}-${"metadata-name".repeat(8)}`,
      nameBase64: btoa(`${index}-${"metadata-name".repeat(8)}`),
      nodeId: (9007199254740993n + BigInt(index)).toString(),
      kind: "file",
      sizeBytes: "9007199254740993",
      modifiedAt: "2026-10-12T00:00:00Z",
    }));
    const body = { pathBase64: "Zm9sZGVy", entries };
    expect(
      new TextEncoder().encode(JSON.stringify(body)).byteLength,
    ).toBeGreaterThan(1024 * 1024);
    const api = new ManagementClient(
      session,
      vi.fn(),
      vi.fn().mockResolvedValue(response(body)),
    );
    const listing = await api.files("volume", body.pathBase64);
    expect(listing.entries).toHaveLength(5000);
    expect(listing.pathBase64).toBe(body.pathBase64);
    expect(listing.entries[4999]).toMatchObject({
      name: entries[4999].name,
      nameBase64: entries[4999].nameBase64,
      pathBase64: btoa(`folder/${entries[4999].name}`),
      nodeId: 9007199254745992n,
      sizeBytes: 9007199254740993n,
    });
  });
  it("preserves opaque raw path navigation and exact node IDs without file content requests", async () => {
    const token = "/w==";
    const fetcher = vi.fn().mockResolvedValue(
      response({
        pathBase64: token,
        entries: [
          {
            name: "\\xFF",
            nameBase64: "/w==",
            nodeId: "18446744073709551615",
            kind: "symlink",
            sizeBytes: "9007199254740993",
            modifiedAt: "2026-10-12T00:00:00Z",
          },
        ],
      }),
    );
    const api = new ManagementClient(session, vi.fn(), fetcher);
    const listing = await api.files("owned/id", token);
    expect(fetcher.mock.calls[0][0]).toBe(
      "/api/service/v1/volumes/owned%2Fid/files?path=%2Fw%3D%3D",
    );
    expect(fetcher.mock.calls[0][1].method).toBe("GET");
    expect(listing.entries[0]).toMatchObject({
      name: "\\xFF",
      pathBase64: "/y//",
      nodeId: 18446744073709551615n,
      sizeBytes: 9007199254740993n,
      kind: "symlink",
    });
  });
  it("refuses a directory response for another target", async () => {
    const api = new ManagementClient(
      session,
      vi.fn(),
      vi.fn().mockResolvedValue(response({ pathBase64: "other", entries: [] })),
    );
    await expect(api.files("volume", "")).rejects.toThrow("不同的目标路径");
  });
  it("uses owner scoped service routes and surfaces denied foreign metadata", async () => {
    const api = new ManagementClient(
      session,
      vi.fn(),
      vi
        .fn()
        .mockResolvedValue(response({ error: { code: "access_denied" } }, 403)),
    );
    await expect(api.files("foreign", "")).rejects.toBeInstanceOf(APIError);
  });
  it("accepts only five aggregate scalars for other users volumes", async () => {
    const fetcher = vi.fn().mockResolvedValue(
      response({
        volumes: [{ name: "ordinary", summary: wireVolume.summary }],
      }),
    );
    const api = new ManagementClient(session, vi.fn(), fetcher);
    expect((await api.userVolumes("ordinary/id")).items[0]).toEqual({
      name: "ordinary",
      summary: volume.summary,
    });
    expect(fetcher.mock.calls[0][0]).toBe(
      "/api/service/v1/admin/users/ordinary%2Fid/volumes?limit=50",
    );
  });
  it.each([{ id: "private-id" }, { files: [] }, { path: "private/path" }])(
    "rejects foreign metadata added to summaries %j",
    async (extra) => {
      const api = new ManagementClient(
        session,
        vi.fn(),
        vi.fn().mockResolvedValue(
          response({
            volumes: [
              { name: "ordinary", summary: wireVolume.summary, ...extra },
            ],
          }),
        ),
      );
      await expect(api.userVolumes("ordinary")).rejects.toThrow("越界字段");
    },
  );
  it("rejects per-file metadata inside public statistics", async () => {
    const api = new ManagementClient(
      session,
      vi.fn(),
      vi.fn().mockResolvedValue(
        response({
          volumes: [
            {
              name: "ordinary",
              summary: { ...wireVolume.summary, fileNames: ["private"] },
            },
          ],
        }),
      ),
    );
    await expect(api.userVolumes("ordinary")).rejects.toThrow("越界字段");
  });
});
describe("exact creation quota input", () => {
  it("keeps exact integral bytes", () => {
    expect(quotaInput("1.5", "GiB")).toBe(1610612736n);
    expect(quotaInput("9007199254740993", "B")).toBe(9007199254740993n);
  });
  it.each([
    ["0", "B"],
    ["0.1", "B"],
    ["9223372036854775808", "B"],
    ["1", "bad"],
  ])("rejects invalid quota %s %s", (value, unit) =>
    expect(() => quotaInput(value, unit)).toThrow(),
  );
});
