import { execSync, spawn } from "node:child_process";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { rmSync, existsSync } from "node:fs";

const REPO_ROOT = new URL("../../", import.meta.url).pathname;
const BIN_PATH = join(new URL(".", import.meta.url).pathname, "embyr-test-bin");
const MIGRATIONS_DIR = join(REPO_ROOT, "migrations");

let serverProcess = null;
let restPort = null;
let dbPath = null;

function freePort() {
  return new Promise((resolve, reject) => {
    const srv = createServer();
    srv.listen(0, "127.0.0.1", () => {
      const port = srv.address().port;
      srv.close(() => resolve(port));
    });
    srv.on("error", reject);
  });
}

async function waitReady(port, maxMs = 10_000) {
  const deadline = Date.now() + maxMs;
  while (Date.now() < deadline) {
    try {
      const res = await fetch(`http://127.0.0.1:${port}/healthz`);
      if (res.ok) return;
    } catch {
      // not ready yet
    }
    await new Promise((r) => setTimeout(r, 100));
  }
  throw new Error(
    `embyr did not become ready on port ${port} within ${maxMs}ms`,
  );
}

export async function setup() {
  // Build the binary from source.
  console.log("[setup] building embyr binary…");
  execSync(`go build -o ${BIN_PATH} ./cmd/embyr`, {
    cwd: REPO_ROOT,
    stdio: "inherit",
  });

  restPort = await freePort();
  const grpcPort = await freePort();
  dbPath = join(tmpdir(), `embyr-test-${restPort}.db`);

  console.log(`[setup] starting embyr on REST :${restPort} gRPC :${grpcPort}`);
  serverProcess = spawn(BIN_PATH, ["--migrations-dir", MIGRATIONS_DIR], {
    env: {
      ...process.env,
      EMBYR_SERVER_REST_PORT: String(restPort),
      EMBYR_SERVER_GRPC_PORT: String(grpcPort),
      EMBYR_BACKEND_TYPE: "sqlite",
      EMBYR_BACKEND_SQLITE_PATH: dbPath,
      EMBYR_AUTH_MODE: "none",
      EMBYR_LOG_FORMAT: "text",
    },
    stdio: ["ignore", "pipe", "pipe"],
  });

  serverProcess.stdout.on("data", (d) => process.stdout.write(`[embyr] ${d}`));
  serverProcess.stderr.on("data", (d) => process.stderr.write(`[embyr] ${d}`));
  serverProcess.on("error", (err) => {
    throw err;
  });

  await waitReady(restPort);
  console.log("[setup] embyr ready");

  // Expose ports for test files via process.env (globalThis is a different
  // context in vitest's globalSetup isolation).
  process.env.EMBYR_REST_PORT = String(restPort);
  process.env.EMBYR_GRPC_PORT = String(grpcPort);
}

export async function teardown() {
  if (serverProcess) {
    serverProcess.kill("SIGTERM");
    await new Promise((r) => serverProcess.on("exit", r));
    serverProcess = null;
  }
  if (dbPath && existsSync(dbPath)) {
    rmSync(dbPath, { force: true });
  }
  if (existsSync(BIN_PATH)) {
    rmSync(BIN_PATH, { force: true });
  }
  console.log("[teardown] embyr stopped");
}
