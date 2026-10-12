import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  FileClient,
  FileAPIError,
  TransferOutcomeError,
  type DirectoryListing,
  type DurableAction,
  type FileEntry,
  type FileSessionPolicy,
} from "./files";
import {
  parseV5,
  stringifyV5,
  uint64,
  validatedNumber,
  v5SmallNumber,
} from "./v5codec";
import { decodeRawLeaf } from "./rawnames";

type Wire = Record<string, unknown>;
const SESSION = "a".repeat(64);
const TOKEN = "b".repeat(64);
const ROOT = 9007199254740993n;
const NODE = 9007199254740995n;
const POLICY: FileSessionPolicy = {
  leaseNanoseconds: 30_000_000_000n,
  historyNanoseconds: 1_000_000_000n,
  maxFileSize: 1024n,
  maxFiles: 16n,
  maxOperations: 4n,
  maxWaiters: 64n,
  maxLockOwners: 16n,
  maxLockRanges: 64n,
  maxPendingLocks: 16n,
  maxLockActions: 32n,
  maxCloseActions: 33n,
};
const ACTION: DurableAction = {
  subjectId: "actor",
  namespace: 9007199254740997n,
  nonce: "nonce",
};
const attr = (id = NODE, kind = 1, size = 3n) =>
  `{"id":${id},"kind":${kind},"size":${size},"allocation_size":0,"allocation_known":true,"access_time":{"unix_sec":0,"nanos":0},"mod_time":{"unix_sec":0,"nanos":0}}`;
const reply = (body: string, status = 200, marker = true): Response =>
  new Response(body, {
    status,
    headers: {
      "Content-Type": "application/json",
      ...(marker ? { "Remote-Fs-Protocol": "5" } : {}),
    },
  });
const openReply = (volume = "volume") =>
  reply(
    `{"session":"${SESSION}","epoch":42,"status":{"Retired":false,"Fenced":false,"Remaining":30000000000,"Epoch":"session-epoch","ActionEpoch":42},"capabilities":{"sessionIdentity":true,"namespace":true,"actions":true},"sessionIdentity":{"backend":{"volume":"${volume}","authority":"authority","rootNodeId":${ROOT}},"sessionEpoch":"session-epoch"}}`,
  );
const renewReply = () =>
  reply(
    '{"epoch":42,"status":{"Retired":false,"Fenced":false,"Remaining":30000000000,"Epoch":"session-epoch","ActionEpoch":99}}',
  );
const closeReply = () =>
  reply('{"epoch":42,"closeResult":{"released":true,"determined":true}}');
const limitReply = (max = 268435456n) =>
  reply(`{"capabilities":{"max_bytes":${max},"replication":true}}`);
const parseRequest = (init?: RequestInit): Wire =>
  parseV5(String(init?.body)) as Wire;
const entry = (kind: "file" | "directory" | "symlink" = "file"): FileEntry => ({
  ...decodeRawLeaf("/24="),
  attr: {
    id: NODE,
    kind,
    size: 3n,
    allocationSize: 0n,
    allocationKnown: true,
    modifiedAt: "1970-01-01T00:00:00.000Z",
  },
});
const listing = (): DirectoryListing => ({
  parent: { nodeId: ROOT },
  revision: "AP8=",
  entries: [entry()],
});
const receipt = (outcome: number, fingerprint: unknown, completed = true) =>
  `{"action":${stringifyV5({ SubjectID: ACTION.subjectId, Namespace: parseV5(ACTION.namespace.toString()), Nonce: ACTION.nonce })},"fingerprint":${stringifyV5(fingerprint)},"outcome":${outcome}${completed ? `,"attr":${attr()}` : ""}}`;
const zeros = () => Array.from({ length: 32 }, () => v5SmallNumber(0, 0, 255));

const mockFile = (
  handle: (
    op: string,
    request: Wire,
    init?: RequestInit,
    url?: string,
  ) => Response | Promise<Response>,
) =>
  vi.fn(
    async (url: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
      const request = parseRequest(init);
      if (request.op === "file.renew") return renewReply();
      const op = request.op as string;
      if (op === "check") return limitReply();
      if (op === "file.session-open")
        return openReply(
          decodeURIComponent(
            String(url).split("/volumes/")[1].split("/v5/")[0],
          ),
        );
      if (op === "file.renew") return renewReply();
      if (op === "file.session-close") return closeReply();
      return handle(op, request, init, String(url));
    },
  ) as unknown as ReturnType<typeof vi.fn> & typeof fetch;

beforeEach(() => {
  vi.useFakeTimers();
});
afterEach(() => {
  vi.clearAllTimers();
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

describe("v5 browser file boundary", () => {
  it("invokes the native fetch implementation with its global receiver", async () => {
    const fetcher = vi.fn(function (
      this: unknown,
      _url: RequestInfo | URL,
      init?: RequestInit,
    ) {
      expect(this).toBe(globalThis);
      const request = parseRequest(init);
      if (request.op === "file.session-open")
        return Promise.resolve(openReply());
      if (request.op === "file.renew") return Promise.resolve(renewReply());
      return Promise.resolve(closeReply());
    }) as typeof fetch;
    const client = new FileClient("volume", "csrf", vi.fn(), fetcher, POLICY);
    await client.open();
    await client.close();
    expect(fetcher).toHaveBeenCalledTimes(3);
  });

  it("preserves exact uint64 identities and raw non-UTF8 names", async () => {
    const fetcher = mockFile((op, request, init, url) => {
      expect(op).toBe("file.read-dir-node");
      expect((request.directory as Wire).NodeID).toBeDefined();
      expect(uint64((request.directory as Wire).NodeID)).toBe(ROOT);
      expect(init?.credentials).toBe("same-origin");
      expect(init?.headers).toMatchObject({
        "X-CSRF-Token": "csrf",
        "X-RFS-Request": "1",
      });
      expect(url).toBe("/api/volumes/a%20b/v5/file");
      return reply(
        `{"epoch":42,"directory":{"observation":{"parentId":${ROOT},"revision":"AP8="},"entries":[{"rawLeaf":"/24=","attr":${attr()}}]}}`,
      );
    });
    const client = new FileClient("a b", "csrf", vi.fn(), fetcher, POLICY);
    const root = await client.open();
    expect(root.nodeId).toBe(ROOT);
    const result = await client.list(root);
    expect(result.entries[0].attr.id).toBe(NODE);
    expect(result.entries[0].rawBytes).toEqual(Uint8Array.of(255, 110));
    expect(result.entries[0].displayName).toBe("\\xFFn");
    expect(result.revision).toBe("AP8=");
    await client.close();
  });

  it("refuses missing, duplicate or substituted directory observations", async () => {
    let body =
      '{"epoch":42,"directory":{"observation":{"parentId":9007199254740993,"revision":"AQ=="}}}';
    const client = new FileClient(
      "volume",
      "csrf",
      vi.fn(),
      mockFile(() => reply(body)),
      POLICY,
    );
    const root = await client.open();
    await expect(client.list(root)).rejects.toThrow("incomplete");
    body = `{"epoch":42,"directory":{"observation":{"parentId":7,"revision":"AQ=="},"entries":[]}}`;
    await expect(client.list(root)).rejects.toThrow("substituted");
    body = `{"epoch":42,"directory":{"observation":{"parentId":${ROOT},"revision":"AQ=="},"entries":[{"rawLeaf":"/24=","attr":${attr()}},{"rawLeaf":"/24=","attr":${attr()}}]}}`;
    await expect(client.list(root)).rejects.toThrow("duplicate");
    await client.close();
  });

  it("never accepts an intermediary 200 response as a file result", async () => {
    const client = new FileClient(
      "volume",
      "csrf",
      vi.fn(),
      mockFile(() => reply('{"directory":{"entries":[]}}', 200, false)),
      POLICY,
    );
    const root = await client.open();
    await expect(client.list(root)).rejects.toMatchObject({
      unknownOutcome: true,
    });
    await client.close();
  });

  it("clears authenticated state on 401 without returning a directory", async () => {
    const lost = vi.fn();
    const client = new FileClient(
      "volume",
      "csrf",
      lost,
      mockFile(() => reply("{}", 401, false)),
      POLICY,
    );
    const root = await client.open();
    await expect(client.list(root)).rejects.toMatchObject({ errno: "EACCES" });
    expect(lost).toHaveBeenCalledOnce();
    await client.close();
  });

  it("probes a valid v5 permission denial without clearing authentication or waiting for the probe", async () => {
    const lost = vi.fn();
    const probe = vi.fn(() => new Promise<void>(() => undefined));
    const client = new FileClient(
      "volume",
      "csrf",
      lost,
      mockFile(() =>
        reply('{"errno":"EACCES","message":"Access denied"}', 422),
      ),
      POLICY,
      probe,
    );
    const root = await client.open();
    await expect(client.list(root)).rejects.toMatchObject({
      errno: "EACCES",
      unknownOutcome: true,
      message: "Access denied",
    });
    expect(probe).toHaveBeenCalledOnce();
    expect(lost).not.toHaveBeenCalled();
    await client.close();
  });

  it("does not probe authentication from a malformed permission denial", async () => {
    const lost = vi.fn();
    const probe = vi.fn();
    const client = new FileClient(
      "volume",
      "csrf",
      lost,
      mockFile(() => reply('{"errno":"EACCES","fileRecorded":false}', 422)),
      POLICY,
      probe,
    );
    const root = await client.open();
    await expect(client.list(root)).rejects.toMatchObject({
      unknownOutcome: true,
    });
    expect(probe).not.toHaveBeenCalled();
    expect(lost).not.toHaveBeenCalled();
    await client.close();
  });

  it("addresses conditional remove by selected identity and exact raw leaf", async () => {
    const fetcher = mockFile((op, request) => {
      expect(op).toBe("file.mutate-name");
      const name = request.name as Wire;
      expect((name.name as Wire).rawLeaf).toBe("/24=");
      expect(uint64((name.target as Wire).nodeId)).toBe(NODE);
      expect(validatedNumber((name.target as Wire).state, 1, 3)).toBe(3);
      expect(name.action).toBe(request.action);
      expect(request.action).toMatch(/^99:/);
      expect(request.path).toBe("");
      expect(request.data).toBe("");
      expect(uint64(request.resultBytes)).toBeGreaterThan(0n);
      return reply('{"epoch":42}');
    });
    const client = new FileClient("volume", "csrf", vi.fn(), fetcher, POLICY);
    await client.open();
    await client.remove(listing(), entry());
    await client.close();
  });

  it("queries a lost mutation before replaying the exact terminal action", async () => {
    let action: unknown;
    let attempts = 0;
    const seen: string[] = [];
    const fetcher = mockFile((op, request, _init, url) => {
      seen.push(op);
      if (op === "file.mutate-name") {
        attempts += 1;
        if (attempts === 1) {
          action = request.action;
          throw new TypeError("network disconnected");
        }
        expect(request.action).toBe(action);
        return reply('{"epoch":42}');
      }
      expect(op).toBe("file.query-action");
      expect(url).toContain("/file-control");
      return reply(
        `{"epoch":42,"actionReceipt":{"Action":"${action}","Operation":"file.mutate-name","Outcome":2}}`,
      );
    });
    const client = new FileClient("volume", "csrf", vi.fn(), fetcher, POLICY);
    await client.open();
    await client.remove(listing(), entry());
    expect(seen).toEqual([
      "file.mutate-name",
      "file.query-action",
      "file.mutate-name",
    ]);
    await client.close();
  });

  it("does not report a terminal not-executed mutation as success", async () => {
    let attempts = 0;
    let action: unknown;
    const client = new FileClient(
      "volume",
      "csrf",
      vi.fn(),
      mockFile((op, request) => {
        if (op === "file.query-action")
          return reply(
            `{"epoch":42,"actionReceipt":{"Action":"${action}","Operation":"file.mutate-name","Outcome":3}}`,
          );
        attempts += 1;
        action = request.action;
        if (attempts === 1) throw new TypeError("network disconnected");
        return reply(
          '{"errno":"ESTALE","message":"Selection changed","fileRecorded":true}',
          422,
        );
      }),
      POLICY,
    );
    await client.open();
    await expect(client.remove(listing(), entry())).rejects.toMatchObject({
      errno: "ESTALE",
      unknownOutcome: false,
    });
    await client.close();
  });

  it("preserves unknown close ownership and permits exact subsequent settlement", async () => {
    let closing = 0;
    const fetcher = vi.fn(async (_url, init) => {
      const request = parseRequest(init);
      if (request.op === "file.renew") return renewReply();
      if (request.op === "check") return limitReply();
      if (request.op === "file.session-open") return openReply();
      if (request.op === "file.session-close") {
        closing += 1;
        return closing === 1
          ? reply(
              '{"epoch":42,"closeResult":{"released":true,"determined":true,"barrierPending":true}}',
            )
          : closeReply();
      }
      return reply('{"errno":"EOPNOTSUPP"}', 422);
    }) as typeof fetch;
    const client = new FileClient("volume", "csrf", vi.fn(), fetcher, POLICY);
    await client.open();
    await expect(client.close()).rejects.toMatchObject({
      unknownOutcome: true,
    });
    await client.close();
    expect(closing).toBe(2);
  });

  it("deduplicates concurrent opens and closes a session opened during navigation", async () => {
    let resolveOpen!: (response: Response) => void;
    const pending = new Promise<Response>((resolve) => {
      resolveOpen = resolve;
    });
    const fetcher = vi.fn(async (_url, init) => {
      const request = parseRequest(init);
      if (request.op === "file.renew") return renewReply();
      if (request.op === "check") return limitReply(1024n);
      if (request.op === "file.session-open") {
        expect(uint64((request.options as Wire).MaxFileSize)).toBe(1024n);
        return pending;
      }
      return closeReply();
    }) as typeof fetch;
    const client = new FileClient("volume", "csrf", vi.fn(), fetcher, POLICY);
    const first = client.open();
    const second = client.open();
    const closed = client.close();
    await Promise.resolve();
    await Promise.resolve();
    resolveOpen(openReply());
    expect(await first).toEqual(await second);
    await closed;
    expect(fetcher).toHaveBeenCalledTimes(3);
  });

  it("retains a captured session for cleanup when its volume identity is wrong", async () => {
    const fetcher = vi.fn(async (_url, init) => {
      const request = parseRequest(init);
      if (request.op === "file.renew") return renewReply();
      if (request.op === "file.session-open")
        return openReply("another-volume");
      expect(request.op).toBe("file.session-close");
      expect(request.session).toBe(SESSION);
      return closeReply();
    }) as typeof fetch;
    const client = new FileClient("volume", "csrf", vi.fn(), fetcher, POLICY);
    await expect(client.open()).rejects.toThrow("requested volume");
    await client.close();
    expect(fetcher).toHaveBeenCalledTimes(3);
  });

  it("preserves native directory Scope casing and conditions a move on absence", async () => {
    const destination: DirectoryListing = {
      parent: { nodeId: ROOT + 4n, scope: "native-scope" },
      revision: "AP8=",
      entries: [],
    };
    const client = new FileClient(
      "volume",
      "csrf",
      vi.fn(),
      mockFile((op, request) => {
        expect(op).toBe("file.mutate-name");
        const name = request.name as Wire;
        const moved = name.destination as Wire;
        expect((moved.parent as Wire).Scope).toEqual({ Token: "native-scope" });
        expect(uint64((moved.parent as Wire).NodeID)).toBe(ROOT + 4n);
        expect(validatedNumber((moved.expected as Wire).state, 1, 3)).toBe(2);
        expect(moved.observedLeaf).toBe("/24=");
        expect(moved.outputLeaf).toBe("/24=");
        return reply(`{"epoch":42,"attr":${attr()}}`);
      }),
      POLICY,
    );
    await client.open();
    await client.move(listing(), entry(), destination, "/24=");
    await client.close();
  });

  it("fences new mutations while an earlier action remains unknown", async () => {
    let changes = 0;
    let action: unknown;
    const client = new FileClient(
      "volume",
      "csrf",
      vi.fn(),
      mockFile((op, request) => {
        if (op === "file.query-action")
          return reply(
            `{"epoch":42,"actionReceipt":{"Action":"${action}","Operation":"file.mutate-name","Outcome":4}}`,
          );
        changes += 1;
        action = request.action;
        throw new TypeError("lost reply");
      }),
      POLICY,
    );
    await client.open();
    await expect(client.remove(listing(), entry())).rejects.toMatchObject({
      unknownOutcome: true,
    });
    await expect(client.remove(listing(), entry())).rejects.toMatchObject({
      unknownOutcome: true,
    });
    expect(changes).toBe(1);
    await expect(client.close()).rejects.toMatchObject({
      unknownOutcome: true,
    });
  });

  it("treats a truncated mutation response stream as unknown and queries the original action", async () => {
    let action: unknown;
    let changes = 0;
    let queries = 0;
    const client = new FileClient(
      "volume",
      "csrf",
      vi.fn(),
      mockFile((op, request) => {
        if (op === "file.query-action") {
          queries += 1;
          return reply(
            `{"epoch":42,"actionReceipt":{"Action":"${action}","Operation":"file.mutate-name","Outcome":4}}`,
          );
        }
        changes += 1;
        action = request.action;
        const stream = new ReadableStream<Uint8Array>({
          start(controller) {
            controller.error(new TypeError("response disconnected"));
          },
        });
        return new Response(stream, { headers: { "Remote-Fs-Protocol": "5" } });
      }),
      POLICY,
    );
    await client.open();
    await expect(client.remove(listing(), entry())).rejects.toMatchObject({
      unknownOutcome: true,
    });
    expect(changes).toBe(1);
    expect(queries).toBe(1);
    await expect(client.remove(listing(), entry())).rejects.toMatchObject({
      unknownOutcome: true,
    });
    expect(changes).toBe(1);
    await expect(client.close()).rejects.toMatchObject({
      unknownOutcome: true,
    });
  });

  it.each([
    "{}",
    '{"epoch":42,"directory":{}}',
    '{"epoch":42,"retry":"false"}',
  ])(
    "rejects malformed success envelope %s and reconciles deletion",
    async (body) => {
      let action: unknown;
      let changes = 0;
      let queries = 0;
      const client = new FileClient(
        "volume",
        "csrf",
        vi.fn(),
        mockFile((op, request) => {
          if (op === "file.query-action") {
            queries += 1;
            return reply(
              `{"epoch":42,"actionReceipt":{"Action":"${action}","Operation":"file.mutate-name","Outcome":4}}`,
            );
          }
          changes += 1;
          action = request.action;
          return reply(body);
        }),
        POLICY,
      );
      await client.open();
      await expect(client.remove(listing(), entry())).rejects.toMatchObject({
        unknownOutcome: true,
      });
      expect(changes).toBe(1);
      expect(queries).toBe(1);
      await expect(client.close()).rejects.toMatchObject({
        unknownOutcome: true,
      });
    },
  );

  it.each([
    '{"errno":"EIO","fileRecorded":false}',
    '{"errno":"UNKNOWN","fileRecorded":true}',
    '{"fileRecorded":true}',
    '{"errno":"EIO","fileRecorded":true,"capabilityCode":"unknown"}',
    '{"errno":"EIO","fileRecorded":true,"capabilityCode":"condition-conflict"}',
  ])("rejects malformed error classification %s", async (body) => {
    let action: unknown;
    const client = new FileClient(
      "volume",
      "csrf",
      vi.fn(),
      mockFile((op, request) => {
        if (op === "file.query-action")
          return reply(
            `{"epoch":42,"actionReceipt":{"Action":"${action}","Operation":"file.mutate-name","Outcome":4}}`,
          );
        action = request.action;
        return reply(body, 422);
      }),
      POLICY,
    );
    await client.open();
    await expect(client.remove(listing(), entry())).rejects.toMatchObject({
      unknownOutcome: true,
    });
    await expect(client.close()).rejects.toMatchObject({
      unknownOutcome: true,
    });
  });
});

describe("native import and verified export", () => {
  it("matches the Go ImportCommand fingerprint and streams the original File", async () => {
    const file = new File(["abc"], "content");
    let command!: Wire;
    const fetcher = vi.fn(
      async (url: RequestInfo | URL, init?: RequestInit) => {
        if (String(url).includes("file-transfer-data")) {
          expect(init?.body).toBe(file);
          expect(init?.headers).toMatchObject({
            "Content-Type": "application/octet-stream",
            "X-CSRF-Token": "csrf",
          });
          return reply(
            `{"seal":{"Bytes":3,"Digest":${stringifyV5(command.Digest)}}}`,
          );
        }
        const request = parseRequest(init);
        if (request.op === "file.renew") return renewReply();
        if (request.op === "check") return limitReply();
        if (request.op === "begin") {
          command = request.command as Wire;
          expect(String(url)).toContain("/file-transfer");
          expect(uint64((command.Action as Wire).Namespace)).toBe(
            ACTION.namespace,
          );
          expect((command.Target as Wire).Name).toBeDefined();
          const fingerprint = (command.Fingerprint as unknown[])
            .map((byte) =>
              validatedNumber(byte, 0, 255).toString(16).padStart(2, "0"),
            )
            .join("");
          // Captured from storage.ImportCommand.EffectFingerprint using these exact native fields.
          expect(fingerprint).toBe(
            "cd1d6846c284191d7cb7c961a4e5816eeabe2fe8bcb78506ebb3375b31628306",
          );
          return reply(`{"token":"${TOKEN}","declared_bytes":3}`);
        }
        expect(request.op).toBe("publish");
        expect(String(url)).toContain("/file-transfer-control");
        return reply(
          `{"receipt":${receipt(2, command.Fingerprint)},"close":{"released":true,"determined":true}}`,
        );
      },
    ) as typeof fetch;
    const client = new FileClient("volume", "csrf", vi.fn(), fetcher, POLICY);
    const result = await client.upload(file, listing(), "/24=", ACTION);
    expect(result).toMatchObject({
      outcome: "completed",
      settled: true,
      attr: { id: NODE, size: 3n },
    });
    await client.close();
  });

  it("rejects over-limit files before admission or reading any bytes", async () => {
    const file = new File(["abc"], "content");
    const slice = vi.spyOn(file, "slice");
    const fetcher = vi.fn(async () => limitReply(2n)) as typeof fetch;
    const client = new FileClient("volume", "csrf", vi.fn(), fetcher, POLICY);
    await expect(
      client.upload(file, listing(), "/24=", ACTION),
    ).rejects.toMatchObject({ errno: "EFBIG" });
    expect(fetcher).toHaveBeenCalledOnce();
    expect(slice).not.toHaveBeenCalled();
  });

  it("hashes bounded file chunks without collecting the complete file in JS memory", async () => {
    const file = new File([new Uint8Array(2 * 1024 * 1024 + 7)], "large");
    const slice = vi.spyOn(file, "slice");
    let command!: Wire;
    const fetcher = vi.fn(
      async (url: RequestInfo | URL, init?: RequestInit) => {
        if (String(url).includes("file-transfer-data"))
          return reply(
            `{"seal":{"Bytes":${file.size},"Digest":${stringifyV5(command.Digest)}}}`,
          );
        const request = parseRequest(init);
        if (request.op === "file.renew") return renewReply();
        if (request.op === "check") return limitReply();
        if (request.op === "begin") {
          command = request.command as Wire;
          return reply(`{"token":"${TOKEN}","declared_bytes":${file.size}}`);
        }
        const captured = receipt(2, command.Fingerprint).replace(
          '"size":3',
          `"size":${file.size}`,
        );
        return reply(
          `{"receipt":${captured},"close":{"released":true,"determined":true}}`,
        );
      },
    ) as typeof fetch;
    const client = new FileClient("volume", "csrf", vi.fn(), fetcher, POLICY);
    await client.upload(file, listing(), "/24=", ACTION);
    expect(slice.mock.calls).toEqual([
      [0, 1048576],
      [1048576, 2097152],
      [2097152, 2097159],
    ]);
    await client.close();
  });

  it("cancels a mismatching receive seal without publishing", async () => {
    const seen: string[] = [];
    const fetcher = vi.fn(
      async (url: RequestInfo | URL, init?: RequestInit) => {
        if (String(url).includes("file-transfer-data"))
          return reply(`{"seal":{"Bytes":3,"Digest":${stringifyV5(zeros())}}}`);
        const request = parseRequest(init);
        seen.push(request.op as string);
        if (request.op === "check") return limitReply();
        if (request.op === "begin")
          return reply(`{"token":"${TOKEN}","declared_bytes":3}`);
        return reply('{"close":{"released":true,"determined":true}}');
      },
    ) as typeof fetch;
    const client = new FileClient("volume", "csrf", vi.fn(), fetcher, POLICY);
    await expect(
      client.upload(new File(["abc"], "content"), listing(), "/24=", ACTION),
    ).rejects.toThrow("complete upload");
    expect(seen).toEqual(["check", "begin", "cancel"]);
    await client.close();
  });

  it("uses a same-node replacement of a symbolic link without following its target", async () => {
    const link = entry("symlink");
    const parent = { ...listing(), entries: [link] };
    let command!: Wire;
    const fetcher = vi.fn(
      async (url: RequestInfo | URL, init?: RequestInit) => {
        if (String(url).includes("file-transfer-data"))
          return reply(
            `{"seal":{"Bytes":3,"Digest":${stringifyV5(command.Digest)}}}`,
          );
        const request = parseRequest(init);
        if (request.op === "file.renew") return renewReply();
        if (request.op === "check") return limitReply();
        if (request.op === "begin") {
          command = request.command as Wire;
          expect(validatedNumber(command.Mode, 1, 2)).toBe(2);
          expect(uint64((command.Expected as Wire).NodeID)).toBe(NODE);
          return reply(`{"token":"${TOKEN}","declared_bytes":3}`);
        }
        return reply(
          `{"receipt":${receipt(2, command.Fingerprint)},"close":{"released":true,"determined":true}}`,
        );
      },
    ) as typeof fetch;
    const client = new FileClient("volume", "csrf", vi.fn(), fetcher, POLICY);
    await client.upload(
      new File(["abc"], "content"),
      parent,
      "/24=",
      ACTION,
      link,
    );
    await expect(client.prepareDownload(link)).rejects.toMatchObject({
      errno: "EOPNOTSUPP",
    });
    await client.close();
  });

  it("queries an unknown publish result and preserves the original durable action", async () => {
    const seen: string[] = [];
    let command!: Wire;
    const fetcher = vi.fn(
      async (url: RequestInfo | URL, init?: RequestInit) => {
        if (String(url).includes("file-transfer-data"))
          return reply(
            `{"seal":{"Bytes":3,"Digest":${stringifyV5(command.Digest)}}}`,
          );
        const request = parseRequest(init);
        seen.push(request.op as string);
        if (request.op === "check") return limitReply();
        if (request.op === "begin") {
          command = request.command as Wire;
          return reply(`{"token":"${TOKEN}","declared_bytes":3}`);
        }
        if (request.op === "publish") throw new TypeError("lost publish reply");
        if (request.op === "query")
          return reply(
            `{"receipt":${receipt(4, command.Fingerprint, false)},"error":{"errno":"EIO"}}`,
          );
        return reply('{"close":{"released":true,"determined":true}}');
      },
    ) as typeof fetch;
    const client = new FileClient("volume", "csrf", vi.fn(), fetcher, POLICY);
    await expect(
      client.upload(new File(["abc"], "content"), listing(), "/24=", ACTION),
    ).rejects.toMatchObject({
      name: "TransferOutcomeError",
      action: ACTION,
      receipt: { outcome: "unknown" },
      unknownOutcome: true,
    });
    expect(seen).toEqual(["check", "begin", "publish", "query"]);
    await client.cancelUpload(ACTION);
    expect(seen.filter((op) => op === "publish")).toHaveLength(1);
  });

  it("does not equate physical release with settled upload cleanup", async () => {
    let command!: Wire;
    const fetcher = vi.fn(
      async (url: RequestInfo | URL, init?: RequestInit) => {
        if (String(url).includes("file-transfer-data"))
          return reply(
            `{"seal":{"Bytes":3,"Digest":${stringifyV5(command.Digest)}}}`,
          );
        const request = parseRequest(init);
        if (request.op === "file.renew") return renewReply();
        if (request.op === "check") return limitReply();
        if (request.op === "begin") {
          command = request.command as Wire;
          return reply(`{"token":"${TOKEN}","declared_bytes":3}`);
        }
        return reply(
          `{"receipt":${receipt(2, command.Fingerprint)},"close":{"released":true,"determined":true},"settlement":{"state":2,"cause":{"errno":"EIO"}},"error":{"errno":"EIO"}}`,
        );
      },
    ) as typeof fetch;
    const client = new FileClient("volume", "csrf", vi.fn(), fetcher, POLICY);
    await expect(
      client.upload(new File(["abc"], "content"), listing(), "/24=", ACTION),
    ).rejects.toBeInstanceOf(TransferOutcomeError);
  });

  it("streams a verified export and commits local output only after exact length", async () => {
    const order: string[] = [];
    const fetcher = vi.fn(
      async (_url: RequestInfo | URL, init?: RequestInit) => {
        if (init?.method === "GET")
          return new Response("abc", {
            headers: { "Remote-Fs-Protocol": "5", "Content-Length": "3" },
          });
        const request = parseRequest(init);
        if (request.op === "file.renew") return renewReply();
        if (request.op === "export")
          return reply(
            `{"token":"${TOKEN}","info":{"attr":${attr()},"digest":"ungWv48Bz+pBQUDeXa4iI7ADYaOWF3qctBD/YfIAFa0="}}`,
          );
        order.push("release");
        return reply('{"close":{"released":true,"determined":true}}');
      },
    ) as typeof fetch;
    const client = new FileClient("volume", "csrf", vi.fn(), fetcher, POLICY);
    const prepared = await client.prepareDownload(entry());
    const writer = {
      write: vi.fn(async (bytes: Uint8Array) => {
        order.push(new TextDecoder().decode(bytes));
      }),
      close: vi.fn(async () => {
        order.push("commit");
      }),
      abort: vi.fn(async () => undefined),
    };
    await client.download(prepared, writer);
    expect(order).toEqual(["abc", "commit", "release"]);
    expect(writer.abort).not.toHaveBeenCalled();
    await client.close();
  });

  it("aborts truncated output and releases export ownership", async () => {
    const fetcher = vi.fn(
      async (_url: RequestInfo | URL, init?: RequestInit) => {
        if (init?.method === "GET")
          return new Response("ab", {
            headers: { "Remote-Fs-Protocol": "5", "Content-Length": "3" },
          });
        const request = parseRequest(init);
        if (request.op === "file.renew") return renewReply();
        if (request.op === "export")
          return reply(
            `{"token":"${TOKEN}","info":{"attr":${attr()},"digest":"ungWv48Bz+pBQUDeXa4iI7ADYaOWF3qctBD/YfIAFa0="}}`,
          );
        return reply('{"close":{"released":true,"determined":true}}');
      },
    ) as typeof fetch;
    const client = new FileClient("volume", "csrf", vi.fn(), fetcher, POLICY);
    const prepared = await client.prepareDownload(entry());
    const writer = {
      write: vi.fn(async () => undefined),
      close: vi.fn(async () => undefined),
      abort: vi.fn(async () => undefined),
    };
    await expect(client.download(prepared, writer)).rejects.toThrow(
      "truncated",
    );
    expect(writer.close).not.toHaveBeenCalled();
    expect(writer.abort).toHaveBeenCalledOnce();
    await client.close();
  });

  it("aborts same-length corrupted content before committing local output", async () => {
    const fetcher = vi.fn(
      async (_url: RequestInfo | URL, init?: RequestInit) => {
        if (init?.method === "GET")
          return new Response("abd", {
            headers: { "Remote-Fs-Protocol": "5", "Content-Length": "3" },
          });
        const request = parseRequest(init);
        if (request.op === "file.renew") return renewReply();
        if (request.op === "export")
          return reply(
            `{"token":"${TOKEN}","info":{"attr":${attr()},"digest":"ungWv48Bz+pBQUDeXa4iI7ADYaOWF3qctBD/YfIAFa0="}}`,
          );
        return reply('{"close":{"released":true,"determined":true}}');
      },
    ) as typeof fetch;
    const client = new FileClient("volume", "csrf", vi.fn(), fetcher, POLICY);
    const prepared = await client.prepareDownload(entry());
    const writer = {
      write: vi.fn(async () => undefined),
      close: vi.fn(async () => undefined),
      abort: vi.fn(async () => undefined),
    };
    await expect(client.download(prepared, writer)).rejects.toThrow(
      "captured digest",
    );
    expect(writer.close).not.toHaveBeenCalled();
    expect(writer.abort).toHaveBeenCalledOnce();
    await client.close();
  });

  it("hands native download GET ownership to server cleanup without allocating a Blob", async () => {
    const anchor = { href: "", download: "", click: vi.fn(), remove: vi.fn() };
    vi.stubGlobal("document", {
      createElement: () => anchor,
      body: { append: vi.fn() },
    });
    const fetcher = vi.fn(async () =>
      reply(
        `{"token":"${TOKEN}","info":{"attr":${attr()},"digest":"ungWv48Bz+pBQUDeXa4iI7ADYaOWF3qctBD/YfIAFa0="}}`,
      ),
    ) as typeof fetch;
    const client = new FileClient("volume", "csrf", vi.fn(), fetcher, POLICY);
    const prepared = await client.prepareDownload(entry());
    await client.download(prepared);
    expect(anchor.href).toBe(
      `/api/volumes/volume/v5/file-transfer-export?path=${TOKEN}`,
    );
    expect(anchor.download).toBe(`file-${NODE}`);
    expect(anchor.click).toHaveBeenCalledOnce();
    await client.close();
    expect(fetcher).toHaveBeenCalledOnce();
  });

  it("settles an export token returned with malformed captured metadata", async () => {
    const seen: string[] = [];
    const fetcher = vi.fn(async (_url, init) => {
      const request = parseRequest(init);
      seen.push(request.op as string);
      if (request.op === "export")
        return reply(
          `{"token":"${TOKEN}","info":{"attr":${attr(NODE + 1n)},"digest":"ungWv48Bz+pBQUDeXa4iI7ADYaOWF3qctBD/YfIAFa0="}}`,
        );
      expect(request.token).toBe(TOKEN);
      return reply('{"close":{"released":true,"determined":true}}');
    }) as typeof fetch;
    const client = new FileClient("volume", "csrf", vi.fn(), fetcher, POLICY);
    await expect(client.prepareDownload(entry())).rejects.toThrow(
      "selected regular file",
    );
    expect(seen).toEqual(["export", "close-export"]);
    await client.close();
  });

  it("requires a resume and cleanup proof before settling a terminal action without its token", async () => {
    const seen: string[] = [];
    const fetcher = vi.fn(async (_url, init) => {
      const request = parseRequest(init);
      seen.push(request.op as string);
      if (request.op === "query")
        return reply(`{"receipt":${receipt(2, zeros())}}`);
      if (request.op === "resume") return reply(`{"token":"${TOKEN}"}`);
      return reply('{"close":{"released":true,"determined":true}}');
    }) as typeof fetch;
    const client = new FileClient("volume", "csrf", vi.fn(), fetcher, POLICY);
    const result = await client.resumeUpload(ACTION);
    expect(result).toMatchObject({ outcome: "completed", settled: true });
    expect(seen).toEqual(["query", "resume", "cancel"]);
    await client.close();
  });

  it("returns a terminal receipt with an explicit cleanup proof when cancelling an action-only journal", async () => {
    const seen: string[] = [];
    const fetcher = vi.fn(
      async (_url: RequestInfo | URL, init?: RequestInit) => {
        const request = parseRequest(init);
        seen.push(request.op as string);
        if (request.op === "query")
          return reply(`{"receipt":${receipt(2, zeros())}}`);
        if (request.op === "resume") return reply(`{"token":"${TOKEN}"}`);
        return reply('{"close":{"released":true,"determined":true}}');
      },
    ) as typeof fetch;
    const client = new FileClient("volume", "csrf", vi.fn(), fetcher, POLICY);
    const result = await client.cancelUpload(ACTION);
    expect(result).toMatchObject({ outcome: "completed", settled: true });
    expect(seen).toEqual(["query", "resume", "cancel", "query"]);
    const plainQuery = await client.queryUpload(ACTION);
    expect(plainQuery.settled).toBe(false);
    await client.close();
  });
});
