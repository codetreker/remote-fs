import { chromium } from "playwright";
import assert from "node:assert/strict";
import { mkdir, rm, writeFile } from "node:fs/promises";
const executablePath = process.env.CONSOLE_BROWSER_EXECUTABLE;
if (!executablePath)
  throw new Error("CONSOLE_BROWSER_EXECUTABLE must name an existing browser.");
const baseURL = process.env.CONSOLE_BASE_URL;
if (!baseURL)
  throw new Error("CONSOLE_BASE_URL must name the running Console dev server.");
await mkdir(".tmp/artifacts", { recursive: true });
await rm(".tmp/artifacts/failure.png", { force: true });
const profile = `.tmp/browser-profile-${process.pid}`;
const context = await chromium.launchPersistentContext(profile, {
  executablePath,
  headless: true,
  args: ["--no-sandbox"],
  viewport: { width: 1440, height: 1000 },
  acceptDownloads: true,
});
const page = await context.newPage();
const errors = [];
page.on("pageerror", (error) => errors.push(error.message));
context.on("page", (tab) =>
  tab.on("pageerror", (error) => errors.push(error.message)),
);
const owner = "1".repeat(32),
  volumeID = "2".repeat(32),
  other = "3".repeat(32);
const SESSION = "a".repeat(64),
  ROOT = "9007199254740993",
  NODE = "9007199254740995";
const session = {
  user: { id: owner, displayName: "Administrator", role: "admin" },
  csrfToken: "persistent-csrf",
  idleExpiresAt: "2099-01-01T00:00:00Z",
  absoluteExpiresAt: "2099-01-02T00:00:00Z",
};
let currentSession = session;
const caps = {
  fileSession: {
    leaseNanoseconds: "30000000000",
    historyNanoseconds: "1000000000",
    maxFileSize: "268435456",
    maxFiles: "16",
    maxOperations: "4",
    maxWaiters: "64",
    maxLockOwners: "16",
    maxLockRanges: "64",
    maxPendingLocks: "16",
    maxLockActions: "32",
    maxCloseActions: "33",
  },
  maxTransferBytes: "268435456",
};
let signedIn = false,
  summaryError = false,
  adminSummaryError = false,
  directoryDenied = false,
  logoutRejected = false,
  renewalDenied = false,
  delayTransferCheck = false,
  keepOperationPending = false,
  importOutcome = 4,
  importSettled = false;
const replacementOperationId = "5".repeat(32);
let releaseReplacementReceipt;
const replacementReceiptGate = new Promise((resolve) => {
  releaseReplacementReceipt = resolve;
});
let releaseOldAuthentication;
const oldAuthenticationGate = new Promise((resolve) => {
  releaseOldAuthentication = resolve;
});
let confirmOldAuthenticationDelivered;
const oldAuthenticationDelivered = new Promise((resolve) => {
  confirmOldAuthenticationDelivered = resolve;
});
const pendingOperation = (id, namespace) => ({
  action: { namespace, nonce: "d".repeat(32) },
  kind: "quota",
  operationId: id,
  submittedAt: "2026-10-12T00:00:00Z",
  request: {
    path: `/volumes/${volumeID}/quota`,
    method: "PUT",
    payload: { quotaBytes: "2048", expectedRevision: "3" },
  },
});
const journalKey = `rfs:upload:v1:${owner}:${volumeID}`;
const uploadAction = {
  subjectId: owner,
  namespace: "9007199254740999",
  nonce: "b".repeat(32),
};
const journal = JSON.stringify({ action: uploadAction, name: "recover.bin" });
const ownVolume = () => ({
  id: volumeID,
  ownerUserId: owner,
  name: "workspace",
  state: "active",
  revision: "9",
  createdAt: "2026-10-12T00:00:00Z",
  updatedAt: "2026-10-12T00:00:00Z",
  ...(summaryError
    ? { summaryError: { code: "metadata_unavailable" } }
    : {
        summary: {
          fileCount: "9007199254740993",
          directoryCount: "96",
          usedBytes: "9007199254740993",
          quotaBytes: "9223372036854775807",
          quotaRevision: "3",
        },
      }),
});
const requests = [];
const operation = {
  id: "4".repeat(32),
  volumeId: volumeID,
  kind: "quota",
  state: "succeeded",
  phase: "completed",
  revision: "2",
  settled: true,
  createdAt: "2026-10-12T00:00:00Z",
  updatedAt: "2026-10-12T00:00:00Z",
};
const attr = (id, kind) =>
  `{"id":${id},"kind":${kind},"size":3,"allocation_size":0,"allocation_known":true,"access_time":{"unix_sec":0,"nanos":0},"mod_time":{"unix_sec":0,"nanos":0}}`;
const native = (body) => ({
  status: 200,
  headers: { "Content-Type": "application/json", "Remote-Fs-Protocol": "5" },
  body,
});
const importReceipt = () =>
  `{"action":{"SubjectID":"${uploadAction.subjectId}","Namespace":${uploadAction.namespace},"Nonce":"${uploadAction.nonce}"},"outcome":${importOutcome},"fingerprint":[${Array(32).fill(0).join(",")}]${importOutcome === 2 ? `,"attr":${attr(NODE, 1)}` : ""}}`;
const importClose = () =>
  `{"released":${importSettled},"determined":${importSettled},"barrierPending":${!importSettled}}`;
try {
  await context.route(
    (url) => url.pathname.startsWith("/api/"),
    async (route) => {
      const request = route.request();
      const url = new URL(request.url());
      const body = request.postData();
      requests.push({
        path: url.pathname,
        method: request.method(),
        headers: request.headers(),
        body,
      });
      const json = (value, status = 200) =>
        route.fulfill({
          status,
          contentType: "application/json",
          body: JSON.stringify(value),
        });
      if (url.pathname.endsWith("/auth/session"))
        return signedIn
          ? json(currentSession)
          : json({ error: { code: "authentication_required" } }, 401);
      if (url.pathname.endsWith("/auth/login")) {
        assert.equal(request.headers()["x-rfs-request"], "1");
        assert.deepEqual(JSON.parse(body), {
          loginName: "admin",
          password: "password",
        });
        signedIn = true;
        return json(currentSession);
      }
      if (url.pathname.endsWith("/auth/logout")) {
        assert.equal(request.headers()["x-csrf-token"], "persistent-csrf");
        if (logoutRejected)
          return json({ error: { code: "revocation_unconfirmed" } }, 503);
        signedIn = false;
        return route.fulfill({ status: 204 });
      }
      if (url.pathname.endsWith("/capabilities")) return json(caps);
      if (
        url.pathname === "/api/service/v1/volumes" &&
        request.method() === "GET"
      )
        return json({
          volumes: currentSession.user.id === owner ? [ownVolume()] : [],
        });
      if (
        url.pathname === `/api/service/v1/volumes/${volumeID}` &&
        request.method() === "GET"
      )
        return json(ownVolume());
      if (url.pathname.endsWith("/action-namespaces"))
        return json({
          subjectId: owner,
          namespace: "9007199254740997",
          acceptUntil: "2099-01-01T00:00:00Z",
        });
      if (url.pathname.endsWith("/quota")) {
        assert.equal(request.headers()["x-csrf-token"], "persistent-csrf");
        assert.equal(JSON.parse(body).expectedRevision, "3");
        assert.equal(JSON.parse(body).quotaBytes, "9007199254740995");
        return json(operation);
      }
      if (url.pathname.includes("/operations/")) {
        if (url.pathname.endsWith(replacementOperationId)) {
          await replacementReceiptGate;
          return json({ ...operation, id: replacementOperationId });
        }
        return json(
          keepOperationPending
            ? {
                ...operation,
                state: "running",
                phase: "applying",
                settled: false,
              }
            : operation,
        );
      }
      if (url.pathname === "/api/service/v1/admin/users")
        return json({
          users: [{ id: other, displayName: "普通用户", role: "user" }],
        });
      if (url.pathname === `/api/service/v1/admin/users/${other}/volumes`)
        return json({
          volumes: [
            {
              name: "private-summary",
              ...(adminSummaryError
                ? { summaryError: { code: "metadata_unavailable" } }
                : {
                    summary: {
                      fileCount: "12",
                      directoryCount: "3",
                      usedBytes: "1024",
                      quotaBytes: "2048",
                    },
                  }),
            },
          ],
        });
      if (url.pathname.endsWith("/v5/file-transfer-control")) {
        const command = JSON.parse(body);
        assert.equal(request.headers()["x-csrf-token"], "persistent-csrf");
        if (command.op === "check" && delayTransferCheck) {
          await oldAuthenticationGate;
          await route.fulfill({
            status: 401,
            contentType: "application/json",
            body: '{"error":{"code":"authentication_required"}}',
          });
          confirmOldAuthenticationDelivered();
          return;
        }
        if (command.op === "check")
          return route.fulfill(
            native(
              '{"capabilities":{"max_bytes":268435456,"replication":true}}',
            ),
          );
        if (command.op === "query" || command.op === "resume") {
          assert.deepEqual(command.action, {
            SubjectID: uploadAction.subjectId,
            Namespace: Number(uploadAction.namespace),
            Nonce: uploadAction.nonce,
          });
          assert.ok(body.includes(`"Namespace":${uploadAction.namespace}`));
          return route.fulfill(
            native(
              command.op === "query"
                ? `{"receipt":${importReceipt()},"close":${importClose()}}`
                : `{"token":"${"c".repeat(64)}"}`,
            ),
          );
        }
        if (command.op === "cancel") {
          assert.equal(command.token, "c".repeat(64));
          return route.fulfill(native(`{"close":${importClose()}}`));
        }
        throw new Error(`Unexpected fixture transfer command ${command.op}`);
      }
      if (
        url.pathname.endsWith("/v5/file") ||
        url.pathname.endsWith("/v5/file-control")
      ) {
        assert.equal(request.headers()["x-csrf-token"], "persistent-csrf");
        const command = JSON.parse(body);
        if (command.op === "file.session-open")
          return route.fulfill(
            native(
              `{"session":"${SESSION}","epoch":42,"status":{"Retired":false,"Fenced":false,"Remaining":30000000000,"Epoch":"session-epoch","ActionEpoch":42},"capabilities":{"sessionIdentity":true,"namespace":true,"actions":true},"sessionIdentity":{"backend":{"volume":"${volumeID}","authority":"authority","rootNodeId":${ROOT}},"sessionEpoch":"session-epoch"}}`,
            ),
          );
        if (command.op === "file.renew" && renewalDenied)
          return route.fulfill({
            ...native('{"errno":"EACCES","fileRecorded":true}'),
            status: 422,
          });
        if (command.op === "file.renew")
          return route.fulfill(
            native(
              '{"epoch":42,"status":{"Retired":false,"Fenced":false,"Remaining":30000000000,"Epoch":"session-epoch","ActionEpoch":99}}',
            ),
          );
        if (command.op === "file.session-close")
          return route.fulfill(
            native(
              '{"epoch":42,"closeResult":{"released":true,"determined":true}}',
            ),
          );
        if (command.op === "file.read-dir-node") {
          assert.ok(body.includes(`"NodeID":${ROOT}`));
          if (directoryDenied)
            return route.fulfill({
              ...native('{"errno":"EACCES","fileRecorded":true}'),
              status: 422,
            });
          return route.fulfill(
            native(
              `{"epoch":42,"directory":{"observation":{"parentId":${ROOT},"revision":"AP8="},"entries":[{"rawLeaf":"/w==","attr":${attr(NODE, 1)}},{"rawLeaf":"XHhGRg==","attr":${attr("9007199254740996", 1)}},{"rawLeaf":"bGluaw==","attr":${attr("9007199254740997", 3)}}]}}`,
            ),
          );
        }
        throw new Error(`Unexpected fixture file command ${command.op}`);
      }
      throw new Error(
        `Unexpected fixture API ${request.method()} ${url.pathname}`,
      );
    },
  );
  await page.goto(baseURL);
  await page.getByRole("heading", { name: "欢迎回来", exact: true }).waitFor();
  await page.screenshot({ path: ".tmp/artifacts/01-login.png" });
  await page.locator("input[name=loginName]").fill("admin");
  await page.locator("input[name=password]").fill("password");
  await page.getByRole("button", { name: "登录", exact: true }).click();
  await page
    .getByRole("heading", { name: "我的 Volumes", exact: true })
    .waitFor();
  await page.locator(".volume-table").waitFor();
  assert.ok(
    (await page.locator(".stats-grid").innerText()).includes(
      "9,007,199,254,740,993",
    ),
  );
  assert.equal(
    await page.getByText("交互设计原型", { exact: false }).count(),
    0,
  );
  await page.screenshot({
    path: ".tmp/artifacts/02-own-volumes.png",
    fullPage: true,
  });
  await page.reload();
  await page
    .getByRole("heading", { name: "我的 Volumes", exact: true })
    .waitFor();
  assert.equal(
    requests.filter((item) => item.path.endsWith("/auth/login")).length,
    1,
  );
  await page
    .getByRole("button", { name: "workspace 更多操作", exact: true })
    .click();
  await page.getByRole("button", { name: "调整配额", exact: true }).click();
  await page
    .getByRole("dialog")
    .locator("input[name=quota]")
    .fill("9007199254740995");
  await page.getByRole("dialog").getByRole("checkbox").check();
  await page
    .getByRole("dialog")
    .getByRole("button", { name: "提交操作", exact: true })
    .click();
  await page.getByRole("dialog").waitFor({ state: "hidden" });
  console.log(
    "PASS production login/sessionreload and exact quota/native revision transport",
  );
  await page
    .getByRole("button", { name: "workspace 更多操作", exact: true })
    .waitFor();
  await page.getByRole("button", { name: "打开", exact: true }).click();
  await page.locator(".file-table").waitFor();
  assert.equal(await page.getByText("\\xFF", { exact: true }).count(), 1);
  assert.equal(await page.getByText("\\\\xFF", { exact: true }).count(), 1);
  assert.equal(
    await page.getByRole("button", { name: "下载 link", exact: true }).count(),
    0,
  );
  assert.equal(await page.getByText("在线编辑", { exact: false }).count(), 0);
  await page
    .getByRole("button", { name: "\\xFF 更多操作", exact: true })
    .click();
  await page.getByRole("button", { name: "重命名", exact: true }).click();
  assert.ok((await page.getByRole("dialog").innerText()).includes(NODE));
  await page.screenshot({
    path: ".tmp/artifacts/03-raw-name-selection.png",
    fullPage: true,
  });
  await page.keyboard.press("Escape");
  await page.getByRole("dialog").waitFor({ state: "hidden" });
  const namespacesBeforeJournal = requests.filter((item) =>
    item.path.endsWith("/action-namespaces"),
  ).length;
  await page.evaluate(({ key, value }) => sessionStorage.setItem(key, value), {
    key: journalKey,
    value: journal,
  });
  await page.reload();
  await page.locator(".volume-table").waitFor();
  await page.getByRole("button", { name: "打开", exact: true }).click();
  await page.locator(".file-table").waitFor();
  await page.getByText("原上传需要核对", { exact: true }).waitFor();
  assert.ok(
    (await page.getByRole("status").innerText()).includes(
      `${uploadAction.namespace} / ${uploadAction.nonce}`,
    ),
  );
  assert.equal(
    await page
      .getByRole("button", { name: "上传文件", exact: true })
      .isDisabled(),
    true,
  );
  await page.getByRole("button", { name: "查询原动作", exact: true }).click();
  await page
    .getByRole("button", { name: "查询原动作", exact: true })
    .waitFor({ state: "visible" });
  await page.waitForFunction(() =>
    [...document.querySelectorAll("button")].some(
      (button) => button.textContent === "查询原动作" && !button.disabled,
    ),
  );
  assert.equal(
    await page.evaluate((key) => sessionStorage.getItem(key), journalKey),
    journal,
  );
  await page
    .getByRole("button", { name: "继续已封存上传", exact: true })
    .click();
  await page.getByText("原上传仍需核对", { exact: false }).waitFor();
  await page
    .getByRole("button", { name: "核对并清理传输", exact: true })
    .click();
  await page.waitForFunction(() =>
    [...document.querySelectorAll("button")].some(
      (button) => button.textContent === "核对并清理传输" && !button.disabled,
    ),
  );
  assert.equal(
    await page.evaluate((key) => sessionStorage.getItem(key), journalKey),
    journal,
  );
  await page.screenshot({
    path: ".tmp/artifacts/08-restored-upload-unknown.png",
    fullPage: true,
  });
  importOutcome = 2;
  await page.getByRole("button", { name: "查询原动作", exact: true }).click();
  await page.getByText("文件已发布，清理待确认", { exact: true }).waitFor();
  assert.equal(
    await page.evaluate((key) => sessionStorage.getItem(key), journalKey),
    journal,
  );
  assert.equal(
    await page
      .getByRole("button", { name: "继续已封存上传", exact: true })
      .isDisabled(),
    true,
  );
  await page.screenshot({
    path: ".tmp/artifacts/09-upload-published-unsettled.png",
    fullPage: true,
  });
  await page
    .getByRole("button", { name: "核对并清理传输", exact: true })
    .click();
  await page.getByText("原上传仍需核对", { exact: false }).waitFor();
  assert.equal(
    await page.evaluate((key) => sessionStorage.getItem(key), journalKey),
    journal,
  );
  importSettled = true;
  await page
    .getByRole("button", { name: "核对并清理传输", exact: true })
    .click();
  await page
    .getByText("文件已发布，清理待确认", { exact: true })
    .waitFor({ state: "hidden" });
  await page.locator(".file-table").waitFor();
  assert.equal(
    await page.evaluate((key) => sessionStorage.getItem(key), journalKey),
    null,
  );
  assert.equal(
    requests.filter((item) => item.path.endsWith("/action-namespaces")).length,
    namespacesBeforeJournal,
  );
  assert.equal(
    requests.some(
      (item) =>
        item.path.endsWith("/file-transfer-data") ||
        item.path.endsWith("/file-transfer"),
    ),
    false,
  );
  console.log(
    "PASS durable upload reload, exact same action, unknown/terminal-unsettled retention and confirmed cleanup without new upload body or namespace",
  );
  await page.evaluate(
    (key) => sessionStorage.setItem(key, '{"action":{}}'),
    journalKey,
  );
  await page.reload();
  await page.locator(".volume-table").waitFor();
  await page.getByRole("button", { name: "打开", exact: true }).click();
  await page.getByText("上传恢复记录需要处理", { exact: true }).waitFor();
  await page.locator(".file-table").waitFor();
  assert.equal(
    await page
      .getByRole("button", { name: "上传文件", exact: true })
      .isDisabled(),
    true,
  );
  assert.equal(
    await page
      .getByRole("button", { name: "新建目录", exact: true })
      .isDisabled(),
    true,
  );
  assert.equal(
    await page.evaluate((key) => sessionStorage.getItem(key), journalKey),
    '{"action":{}}',
  );
  await page.screenshot({
    path: ".tmp/artifacts/10-invalid-upload-journal.png",
    fullPage: true,
  });
  await page.evaluate((key) => sessionStorage.removeItem(key), journalKey);
  await page.getByRole("button", { name: "用户概览", exact: true }).click();
  await page.getByRole("heading", { name: /用户概览/ }).waitFor();
  await page.locator(".summary-table").waitFor();
  assert.equal(await page.locator(".summary-table th").count(), 4);
  assert.equal(await page.locator(".summary-table tbody tr td").count(), 4);
  assert.equal(
    (await page.locator(".summary-table tbody").innerText())
      .replace(/\s+/g, " ")
      .trim(),
    "private-summary 12 3 1.0 KiB / 2.0 KiB",
  );
  assert.equal(await page.locator(".summary-table button").count(), 0);
  assert.equal(
    await page.getByRole("button", { name: "上传文件", exact: true }).count(),
    0,
  );
  assert.equal(
    requests.some((request) => request.path.includes(`/api/volumes/${other}`)),
    false,
  );
  await page.screenshot({
    path: ".tmp/artifacts/04-user-summary.png",
    fullPage: true,
  });
  console.log(
    "PASS production raw-byte filename identity and admin summary ownership boundary",
  );
  adminSummaryError = true;
  await page.getByRole("button", { name: "我的 Volumes", exact: true }).click();
  await page.locator(".volume-table").waitFor();
  await page.getByRole("button", { name: "用户概览", exact: true }).click();
  await page.locator(".summary-table").waitFor();
  assert.equal(
    await page
      .locator(".summary-table tbody td")
      .filter({ hasText: /^—$/ })
      .count(),
    3,
  );
  assert.ok((await page.locator(".user-stats").innerText()).includes("—"));
  await page.screenshot({
    path: ".tmp/artifacts/11-admin-statistics-unavailable.png",
    fullPage: true,
  });
  adminSummaryError = false;
  await page.getByRole("button", { name: "我的 Volumes", exact: true }).click();
  await page.locator(".volume-table").waitFor();
  directoryDenied = true;
  const probesBeforeDenied = requests.filter((item) =>
    item.path.endsWith("/auth/session"),
  ).length;
  await page.getByRole("button", { name: "打开", exact: true }).click();
  await page
    .getByRole("heading", { name: "无法读取目录", exact: true })
    .waitFor();
  await page
    .getByText("当前身份没有文件访问权限，或登录已失效。", { exact: true })
    .waitFor();
  await page.waitForFunction(() => !document.querySelector(".loading-state"));
  assert.equal(
    await page.getByRole("heading", { name: "欢迎回来", exact: true }).count(),
    0,
  );
  assert.ok(
    requests.filter((item) => item.path.endsWith("/auth/session")).length >
      probesBeforeDenied,
  );
  await page.screenshot({
    path: ".tmp/artifacts/12-file-permission-session-valid.png",
    fullPage: true,
  });
  signedIn = false;
  await page
    .getByRole("button", { name: "核对并重新加载", exact: true })
    .click();
  await page.getByRole("heading", { name: "欢迎回来", exact: true }).waitFor();
  assert.equal(await page.getByText("workspace", { exact: true }).count(), 0);
  await page.screenshot({
    path: ".tmp/artifacts/13-file-permission-session-expired.png",
    fullPage: true,
  });
  directoryDenied = false;
  await page.locator("input[name=loginName]").fill("admin");
  await page.locator("input[name=password]").fill("password");
  await page.getByRole("button", { name: "登录", exact: true }).click();
  await page.locator(".volume-table").waitFor();
  console.log(
    "PASS native EACCES probes session, preserves valid-login permission error, and clears private UI only on confirmed 401",
  );
  await page.getByRole("button", { name: "我的 Volumes", exact: true }).click();
  summaryError = true;
  await page.getByRole("button", { name: "刷新列表", exact: true }).click();
  await page.getByText("统计暂不可用", { exact: true }).first().waitFor();
  assert.ok((await page.locator(".stats-grid").innerText()).includes("—"));
  await page.screenshot({
    path: ".tmp/artifacts/05-statistics-unavailable.png",
    fullPage: true,
  });
  await page
    .getByRole("button", { name: "workspace 更多操作", exact: true })
    .click();
  assert.equal(
    await page
      .getByRole("button", { name: "调整配额", exact: true })
      .isDisabled(),
    true,
  );
  await page.keyboard.press("Escape");
  summaryError = false;
  await page.getByRole("button", { name: "刷新列表", exact: true }).click();
  await page.getByText("就绪", { exact: true }).waitFor();
  await page.setViewportSize({ width: 390, height: 844 });
  await page.waitForTimeout(250);
  await page.screenshot({
    path: ".tmp/artifacts/06-mobile-volumes.png",
    fullPage: true,
  });
  assert.equal(
    await page.evaluate(() => document.documentElement.scrollWidth),
    390,
  );
  const secondTab = await context.newPage();
  await secondTab.goto(baseURL);
  await secondTab.locator(".volume-table").waitFor();
  await secondTab.getByRole("button", { name: "打开", exact: true }).click();
  await secondTab.locator(".file-table").waitFor();
  await secondTab
    .getByRole("button", { name: "\\xFF 更多操作", exact: true })
    .click();
  await secondTab.getByRole("button", { name: "重命名", exact: true }).click();
  await secondTab.getByRole("dialog").waitFor();
  assert.ok((await secondTab.getByRole("dialog").innerText()).includes(NODE));
  await secondTab.screenshot({
    path: ".tmp/artifacts/14-second-tab-private-dialog.png",
    fullPage: true,
  });
  assert.equal(
    requests.filter((item) => item.path.endsWith("/auth/login")).length,
    2,
  );
  assert.ok(
    requests
      .filter(
        (item) => item.method === "POST" && item.path.endsWith("/v5/file"),
      )
      .every((item) => item.headers["x-csrf-token"] === session.csrfToken),
  );
  await page.getByRole("button", { name: "打开导航", exact: true }).click();
  await page.getByRole("button", { name: "用户概览", exact: true }).click();
  await page.locator(".summary-table").waitFor();
  await page.waitForTimeout(250);
  await page.screenshot({
    path: ".tmp/artifacts/07-mobile-user-summary.png",
    fullPage: true,
  });
  assert.equal(
    await page.evaluate(() => document.documentElement.scrollWidth),
    390,
  );
  await page.getByRole("button", { name: "打开导航", exact: true }).click();
  const unusedJournalKey = `rfs:upload:v1:${owner}:${"f".repeat(32)}`;
  await page.evaluate(
    ({ uploadKey, uploadValue, pendingKey }) => {
      sessionStorage.setItem(uploadKey, uploadValue);
      sessionStorage.setItem(pendingKey, "[]");
    },
    {
      uploadKey: unusedJournalKey,
      uploadValue: journal,
      pendingKey: `rfs.pending.${owner}`,
    },
  );
  logoutRejected = true;
  await page.getByRole("button", { name: "退出登录", exact: true }).click();
  await page
    .getByRole("heading", { name: "我的 Volumes", exact: true })
    .waitFor();
  await page
    .getByText("服务拒绝了请求（revocation_unconfirmed）。", { exact: true })
    .waitFor();
  assert.equal(
    await page.evaluate((key) => sessionStorage.getItem(key), unusedJournalKey),
    journal,
  );
  assert.equal(
    await page.evaluate(
      (key) => sessionStorage.getItem(key),
      `rfs.pending.${owner}`,
    ),
    "[]",
  );
  assert.equal(await secondTab.getByRole("dialog").count(), 1);
  logoutRejected = false;
  await page.getByRole("button", { name: "退出登录", exact: true }).click();
  await page.getByRole("heading", { name: "欢迎回来", exact: true }).waitFor();
  await secondTab
    .getByRole("heading", { name: "欢迎回来", exact: true })
    .waitFor();
  assert.equal(await secondTab.getByRole("dialog").count(), 0);
  assert.equal(
    await secondTab.getByText("workspace", { exact: true }).count(),
    0,
  );
  assert.equal(await secondTab.locator(".file-table").count(), 0);
  await secondTab.screenshot({
    path: ".tmp/artifacts/15-second-tab-confirmed-logout.png",
    fullPage: true,
  });
  await secondTab.close();
  assert.equal(
    await page.getByText("private-summary", { exact: true }).count(),
    0,
  );
  assert.equal(
    await page.evaluate((key) => sessionStorage.getItem(key), unusedJournalKey),
    null,
  );
  assert.equal(await page.getByText("workspace", { exact: true }).count(), 0);
  assert.equal(
    await page.evaluate(
      () =>
        Object.keys(sessionStorage).filter((key) =>
          key.startsWith("rfs.pending."),
        ).length,
    ),
    0,
  );
  console.log(
    "PASS two-tab stable CSRF and confirmed logout clears the other tab's private directory and dialog",
  );
  await page.setViewportSize({ width: 1440, height: 1000 });
  await page.locator("input[name=loginName]").fill("admin");
  await page.locator("input[name=password]").fill("password");
  await page.getByRole("button", { name: "登录", exact: true }).click();
  await page.locator(".volume-table").waitFor();
  keepOperationPending = true;
  await page.evaluate(
    ({ key, value, uploadKey, uploadValue }) => {
      sessionStorage.setItem(key, value);
      sessionStorage.setItem(uploadKey, uploadValue);
    },
    {
      key: `rfs.pending.${owner}`,
      value: JSON.stringify([pendingOperation(operation.id, "21")]),
      uploadKey: unusedJournalKey,
      uploadValue: journal,
    },
  );
  await page.reload();
  await page.locator(".volume-table").waitFor();
  await page
    .getByRole("region", { name: "持久操作进度", exact: true })
    .waitFor();
  assert.ok(
    (await page.locator(".operation-panel").innerText()).includes(operation.id),
  );
  delayTransferCheck = true;
  await page.getByRole("button", { name: "打开", exact: true }).click();
  await page.locator(".file-table").waitFor();
  await page
    .getByRole("button", { name: "\\xFF 更多操作", exact: true })
    .click();
  await page.getByRole("button", { name: "重命名", exact: true }).click();
  await page.getByRole("dialog").waitFor();
  await page.screenshot({
    path: ".tmp/artifacts/16-old-identity-private-state.png",
    fullPage: true,
  });
  const replacementUser = "6".repeat(32);
  const replacementJournalKey = `rfs:upload:v1:${replacementUser}:${"f".repeat(32)}`;
  const replacementJournal = JSON.stringify({
    action: { ...uploadAction, subjectId: replacementUser },
    name: "replacement.bin",
  });
  await page.evaluate(
    ({ key, value, uploadKey, uploadValue }) => {
      sessionStorage.setItem(key, value);
      sessionStorage.setItem(uploadKey, uploadValue);
    },
    {
      key: `rfs.pending.${replacementUser}`,
      value: JSON.stringify([pendingOperation(replacementOperationId, "22")]),
      uploadKey: replacementJournalKey,
      uploadValue: replacementJournal,
    },
  );
  currentSession = {
    ...session,
    user: {
      id: replacementUser,
      displayName: "Replacement administrator",
      role: "admin",
    },
    csrfToken: "replacement-csrf",
  };
  renewalDenied = true;
  await page.getByText("Replacement administrator", { exact: true }).waitFor();
  await page.waitForFunction(() => !document.querySelector(".loading-state"));
  assert.equal(await page.getByRole("dialog").count(), 0);
  assert.equal(await page.locator(".file-table").count(), 0);
  assert.equal(await page.getByText("workspace", { exact: true }).count(), 0);
  assert.equal(await page.locator(".operation-panel").count(), 0);
  assert.equal(
    await page.getByText(`操作 ID：${operation.id}`, { exact: true }).count(),
    0,
  );
  assert.equal(
    await page.evaluate(
      (key) => sessionStorage.getItem(key),
      `rfs.pending.${owner}`,
    ),
    null,
  );
  assert.equal(
    await page.evaluate((key) => sessionStorage.getItem(key), unusedJournalKey),
    null,
  );
  assert.equal(
    await page.evaluate(
      (key) => sessionStorage.getItem(key),
      replacementJournalKey,
    ),
    replacementJournal,
  );
  assert.ok(
    requests.some((item) => item.path.endsWith(replacementOperationId)),
  );
  await page.screenshot({
    path: ".tmp/artifacts/17-replacement-identity-isolation.png",
    fullPage: true,
  });
  console.log(
    "PASS same-batch authoritative identity replacement remounts private children while the new actor's receipt is still pending",
  );
  const identitySignalTab = await context.newPage();
  await identitySignalTab.goto(baseURL);
  await identitySignalTab
    .getByText("Replacement administrator", { exact: true })
    .waitFor();
  const probesBeforeBroadcast = requests.filter((item) =>
    item.path.endsWith("/auth/session"),
  ).length;
  await identitySignalTab.evaluate(() => {
    const channel = new BroadcastChannel("rfs-session");
    channel.postMessage("identity-changed");
    channel.close();
  });
  await page.getByText("Replacement administrator", { exact: true }).waitFor();
  await page.waitForTimeout(150);
  assert.ok(
    requests.filter((item) => item.path.endsWith("/auth/session")).length >
      probesBeforeBroadcast,
  );
  releaseOldAuthentication();
  await oldAuthenticationDelivered;
  await page.waitForTimeout(150);
  assert.equal(
    await page.getByRole("heading", { name: "欢迎回来", exact: true }).count(),
    0,
  );
  assert.equal(
    await page.getByText("Replacement administrator", { exact: true }).count(),
    1,
  );
  assert.equal(
    await page.getByText("登录已失效，请重新登录。", { exact: true }).count(),
    0,
  );
  await page.screenshot({
    path: ".tmp/artifacts/18-delayed-old-401-isolation.png",
    fullPage: true,
  });
  await identitySignalTab.close();
  releaseReplacementReceipt();
  await page.waitForTimeout(150);
  console.log(
    "PASS delayed old file 401 after BroadcastChannel recovery cannot clear the replacement identity",
  );
  assert.deepEqual(errors, []);
  console.log(
    "PASS unknown summaries, mobile layout, logout state clearing and zero page errors",
  );
  await writeFile(
    ".tmp/artifacts/browser-verification.json",
    JSON.stringify(
      {
        passed: true,
        mode: "production-ui-with-intercepted-wire-fixtures",
        screenshots: 18,
        pageErrors: errors,
        assertions: [
          "cookie-mode-login/sessionreload",
          "CSRF mutation",
          "quota-native-revision",
          "exact >2^53 integers",
          "injective raw names",
          "owner-selected identity",
          "durable upload reload and exact same action",
          "unknown and completed-but-unsettled upload retention",
          "adapter settlement before journal clearing",
          "no replay body or new namespace during reconciliation",
          "invalid upload journal blocks mutations",
          "native EACCES session probe and confirmed-401 identity clearing",
          "admin read-only summaries",
          "unknown statistics",
          "mobile width",
          "logout private-state clearing only after confirmed revocation",
          "two-tab stable CSRF and confirmed logout invalidation",
          "same-batch identity replacement clears old dialogs and pending receipts",
          "matching actor journal retention and previous actor journal removal",
          "delayed old file 401 cannot clear a replacement identity after broadcast recovery",
        ],
      },
      null,
      2,
    ),
  );
} catch (error) {
  console.log(await page.locator("body").innerText());
  console.log(errors);
  await page.screenshot({ path: ".tmp/artifacts/failure.png", fullPage: true });
  throw error;
} finally {
  releaseReplacementReceipt();
  releaseOldAuthentication();
  await context.close();
  await rm(profile, { recursive: true, force: true });
}
