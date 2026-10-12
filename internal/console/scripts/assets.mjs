import { createHash } from "node:crypto";
import { readdir, readFile, writeFile } from "node:fs/promises";
const sourceRoots = [
  ".npmrc",
  "package.json",
  "pnpm-lock.yaml",
  "index.html",
  "tsconfig.json",
  "vite.config.ts",
  "src",
  "patches",
  "scripts",
];
const collect = async (path) => {
  const entries = await readdir(path, { withFileTypes: true });
  const files = [];
  for (const entry of entries) {
    const full = `${path}/${entry.name}`;
    if (entry.isDirectory()) files.push(...(await collect(full)));
    else if (entry.isFile()) files.push(full);
    else throw new Error(`Unsupported build input: ${full}`);
  }
  return files;
};
const sourceFiles = [];
for (const path of sourceRoots) {
  if (["src", "patches", "scripts"].includes(path))
    sourceFiles.push(...(await collect(path)));
  else sourceFiles.push(path);
}
const digest = async (files) => {
  const hash = createHash("sha256");
  for (const file of [...files].sort()) {
    hash.update(file);
    hash.update("\0");
    hash.update(await readFile(file));
    hash.update("\0");
  }
  return hash.digest("hex");
};
const sourcesHash = await digest(sourceFiles);
const assets = await collect("dist");
const assetFiles = assets.filter((file) => file !== "dist/build-manifest.json");
if (
  !assetFiles.includes("dist/index.html") ||
  !assetFiles.some(
    (file) => file.startsWith("dist/assets/") && file.endsWith(".js"),
  )
)
  throw new Error(
    "Console distribution must contain the real built application.",
  );
const assetsHash = await digest(assetFiles);
const manifest = { version: 1, sourcesHash, assetsHash };
const command = process.argv[2];
if (command === "write") {
  await writeFile(
    "dist/build-manifest.json",
    JSON.stringify(manifest, null, 2) + "\n",
  );
  console.log(`Console assets built from ${sourcesHash}`);
} else if (command === "check") {
  const recorded = JSON.parse(
    await readFile("dist/build-manifest.json", "utf8"),
  );
  if (
    recorded.version !== 1 ||
    recorded.sourcesHash !== sourcesHash ||
    recorded.assetsHash !== assetsHash
  )
    throw new Error("Console assets are stale or modified. Run pnpm build.");
  console.log(`Console assets verified: ${sourcesHash}`);
} else throw new Error("Use assets.mjs write or check.");
