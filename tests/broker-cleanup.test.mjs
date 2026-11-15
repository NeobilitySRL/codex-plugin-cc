import fs from "node:fs";
import path from "node:path";
import test from "node:test";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";

import { CodexAppServerClient } from "../plugins/codex/scripts/lib/app-server.mjs";
import { createBrokerEndpoint } from "../plugins/codex/scripts/lib/broker-endpoint.mjs";
import {
  BROKER_IDLE_MS_ENV,
  clearBrokerSession,
  ensureBrokerSession,
  loadBrokerSession,
  saveBrokerSession,
  sendBrokerShutdown,
  teardownBrokerSession,
  waitForBrokerEndpoint
} from "../plugins/codex/scripts/lib/broker-lifecycle.mjs";
import { buildEnv, installFakeCodex } from "./fake-codex-fixture.mjs";
import { makeTempDir } from "./helpers.mjs";

const SCRIPT = fileURLToPath(new URL("../plugins/codex/scripts/app-server-broker.mjs", import.meta.url));
// Bound local subprocess startup and cleanup checks without relying on production unload timing.
const WAIT_TIMEOUT_MS = 10000;
const WAIT_INTERVAL_MS = 25;
// Short idle period so idle-exit tests finish quickly; several periods fit in IDLE_WAIT_MS.
const IDLE_MS = 500;
const IDLE_WAIT_MS = IDLE_MS * 3;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitFor(predicate, failureMessage = "Broker did not release its thread subscriptions.") {
  const deadline = Date.now() + WAIT_TIMEOUT_MS;
  while (Date.now() < deadline) {
    if (await predicate()) {
      return;
    }
    await sleep(WAIT_INTERVAL_MS);
  }
  assert.fail(failureMessage);
}

async function startBroker(t, { real = false, behavior = "review-ok", expectedStderr = null, idleMs = null } = {}) {
  const cwd = makeTempDir("cxc-cleanup-");
  const binDir = makeTempDir();
  const sessionDir = makeTempDir("cxc-");
  const endpoint = createBrokerEndpoint(sessionDir);
  const pidFile = path.join(sessionDir, "broker.pid");
  if (!real) {
    installFakeCodex(binDir, behavior);
  } else {
    // Use the supported unload setting to avoid the default 60-second grace period.
    fs.writeFileSync(path.join(binDir, "config.toml"), "thread_unload_delay_secs = 1\n");
  }
  const env = real ? { ...process.env, CODEX_HOME: binDir } : buildEnv(binDir);
  delete env.CODEX_COMPANION_APP_SERVER_ENDPOINT;
  if (idleMs !== null) {
    env[BROKER_IDLE_MS_ENV] = String(idleMs);
  }
  const proc = spawn(process.execPath, [SCRIPT, "serve", "--endpoint", endpoint, "--cwd", cwd, "--pid-file", pidFile], {
    cwd,
    env,
    stdio: ["ignore", "pipe", "pipe"]
  });
  let stderr = "";
  proc.stderr.on("data", (chunk) => { stderr += chunk; });
  const exited = new Promise((resolve) => proc.on("exit", (code) => resolve(code)));
  const clients = [];
  t.after(async () => {
    await Promise.all(clients.map((client) => client.close()));
    if (proc.exitCode === null) {
      await sendBrokerShutdown(endpoint);
    }
    assert.equal(await exited, 0, stderr);
    if (expectedStderr) {
      assert.match(stderr, expectedStderr);
    } else if (!real) {
      assert.equal(stderr, "");
    }
    clearBrokerSession(cwd);
    fs.rmSync(cwd, { recursive: true, force: true });
    fs.rmSync(binDir, { recursive: true, force: true });
    fs.rmSync(sessionDir, { recursive: true, force: true });
  });
  assert.equal(await waitForBrokerEndpoint(endpoint, WAIT_TIMEOUT_MS), true, stderr);
  return {
    cwd,
    endpoint,
    pidFile,
    sessionDir,
    exited,
    running() {
      return proc.exitCode === null;
    },
    async connect() {
      const client = await CodexAppServerClient.connect(cwd, { brokerEndpoint: endpoint });
      clients.push(client);
      return client;
    },
    state() {
      return JSON.parse(fs.readFileSync(path.join(binDir, "fake-codex-state.json"), "utf8"));
    },
    stderr() {
      return stderr;
    },
    finishStart() {
      fs.writeFileSync(path.join(binDir, "fake-codex-state.json.finish-start"), "");
    }
  };
}

test("broker unsubscribes completed threads when a client disconnects", async (t) => {
  const broker = await startBroker(t);
  const client = await broker.connect();
  const { thread } = await client.request("thread/start", { cwd: broker.cwd });
  await client.close();
  await waitFor(() => broker.state().unsubscribed?.includes(thread.id));
  assert.equal(broker.state().appServerStarts, 1);
});

test("broker retains a thread until both subscribing clients disconnect", async (t) => {
  const broker = await startBroker(t);
  const first = await broker.connect();
  const second = await broker.connect();
  const { thread } = await first.request("thread/start", { cwd: broker.cwd });
  await second.request("thread/resume", { threadId: thread.id });
  await first.close();
  await second.request("thread/list", {});
  assert.deepEqual(broker.state().unsubscribed ?? [], []);
  await second.close();
  await waitFor(() => broker.state().unsubscribed?.includes(thread.id));
});

test("broker releases threads after a later request fails", async (t) => {
  const broker = await startBroker(t);
  const client = await broker.connect();
  const { thread } = await client.request("thread/start", { cwd: broker.cwd });
  await assert.rejects(client.request("thread/resume", { threadId: "missing" }), /unknown thread/);
  await client.close();
  await waitFor(() => broker.state().unsubscribed?.includes(thread.id));
});

test("broker releases both source and detached review threads", async (t) => {
  const broker = await startBroker(t);
  const client = await broker.connect();
  const { thread } = await client.request("thread/start", { cwd: broker.cwd });
  const review = await client.request("review/start", {
    threadId: thread.id,
    delivery: "detached",
    target: { type: "uncommittedChanges" }
  });
  await client.close();
  await waitFor(() => broker.state().unsubscribed?.length === 2);
  assert.deepEqual(broker.state().unsubscribed, [thread.id, review.reviewThreadId]);
});

test("broker releases a thread whose start response arrives after disconnect", async (t) => {
  const broker = await startBroker(t, { behavior: "slow-thread-start" });
  const client = await broker.connect();
  const pending = assert.rejects(client.request("thread/start", { cwd: broker.cwd }), /connection closed/);
  await waitFor(() => broker.state().threads.length === 1);
  await client.close();
  await pending;
  broker.finishStart();
  await waitFor(() => broker.state().unsubscribed?.length === 1);
  assert.equal(broker.state().unsubscribed[0], broker.state().threads[0].id);
  const next = await broker.connect();
  await next.request("thread/list", {});
});

test("broker keeps a disconnected request's ownership until its response arrives", {
  timeout: WAIT_TIMEOUT_MS
}, async (t) => {
  const broker = await startBroker(t, { behavior: "slow-thread-start" });
  const first = await broker.connect();
  const pending = assert.rejects(first.request("thread/start", { cwd: broker.cwd }), /connection closed/);
  await waitFor(() => broker.state().threads.length === 1);
  await first.close();
  await pending;
  const second = await broker.connect();
  await assert.rejects(second.request("thread/list", {}), /Shared Codex broker is busy/);
  broker.finishStart();
  await waitFor(() => broker.state().unsubscribed?.length === 1);
});

test("broker releases subscriptions after an abrupt socket disconnect", async (t) => {
  const broker = await startBroker(t);
  const client = await broker.connect();
  const { thread } = await client.request("thread/start", { cwd: broker.cwd });
  client.socket.destroy();
  await waitFor(() => broker.state().unsubscribed?.includes(thread.id));
  assert.deepEqual(broker.state().unsubscribed, [thread.id]);
});

test("broker reports cleanup failures and rejects further work", async (t) => {
  const broker = await startBroker(t, {
    behavior: "unsubscribe-fails",
    expectedStderr: /Failed to release Codex thread .*thread cleanup failed.*Restart the shared Codex broker/
  });
  const client = await broker.connect();
  await client.request("thread/start", { cwd: broker.cwd });
  await client.close();
  await waitFor(() => broker.stderr().includes("thread cleanup failed"));
  const next = await broker.connect();
  await assert.rejects(next.request("thread/list", {}), /Failed to release Codex thread .*thread cleanup failed/);
});

test("real Codex unloads a thread after its broker client disconnects", {
  skip: process.env.CODEX_PLUGIN_E2E !== "1"
}, async (t) => {
  const broker = await startBroker(t, { real: true });
  const client = await broker.connect();
  const observer = await broker.connect();
  const { thread } = await client.request("thread/start", { cwd: broker.cwd, ephemeral: true });
  assert.ok((await observer.request("thread/loaded/list", {})).data.includes(thread.id));
  await client.close();
  await waitFor(async () => !(await observer.request("thread/loaded/list", {})).data.includes(thread.id));
});

test("broker shuts down after the idle period and removes its session files", {
  timeout: WAIT_TIMEOUT_MS
}, async (t) => {
  const broker = await startBroker(t, { idleMs: IDLE_MS });
  const logFile = path.join(broker.sessionDir, "broker.log");
  fs.writeFileSync(logFile, "");
  saveBrokerSession(broker.cwd, {
    endpoint: broker.endpoint,
    pidFile: broker.pidFile,
    logFile,
    sessionDir: broker.sessionDir,
    pid: null
  });
  assert.equal(await broker.exited, 0);
  assert.equal(loadBrokerSession(broker.cwd), null);
  assert.equal(fs.existsSync(broker.pidFile), false);
  assert.equal(fs.existsSync(logFile), false);
  assert.equal(fs.existsSync(broker.sessionDir), false);
});

test("broker idle exit keeps a broker.json that belongs to another broker", {
  timeout: WAIT_TIMEOUT_MS
}, async (t) => {
  const broker = await startBroker(t, { idleMs: IDLE_MS });
  const otherSession = {
    endpoint: createBrokerEndpoint(path.join(broker.cwd, "other-session")),
    pidFile: null,
    logFile: null,
    sessionDir: null,
    pid: null
  };
  saveBrokerSession(broker.cwd, otherSession);
  assert.equal(await broker.exited, 0);
  assert.deepEqual(loadBrokerSession(broker.cwd), otherSession);
  assert.equal(fs.existsSync(broker.pidFile), false);
});

test("broker stays up while a client is connected", {
  timeout: WAIT_TIMEOUT_MS
}, async (t) => {
  const broker = await startBroker(t, { idleMs: IDLE_MS });
  const client = await broker.connect();
  await sleep(IDLE_WAIT_MS);
  assert.equal(broker.running(), true);
  await client.request("thread/list", {});
  await client.close();
  assert.equal(await broker.exited, 0);
});

test("broker stays up while a turn is running", {
  timeout: WAIT_TIMEOUT_MS
}, async (t) => {
  const broker = await startBroker(t, { behavior: "interruptible-slow-task", idleMs: IDLE_MS });
  const client = await broker.connect();
  const { thread } = await client.request("thread/start", { cwd: broker.cwd });
  const { turn } = await client.request("turn/start", {
    threadId: thread.id,
    input: [{ type: "text", text: "long running task" }]
  });
  await sleep(IDLE_WAIT_MS);
  assert.equal(broker.running(), true);
  await client.request("turn/interrupt", { threadId: thread.id, turnId: turn.id });
  await client.close();
  assert.equal(await broker.exited, 0);
});

test("broker stays up while a disconnected client's request is still in flight", {
  timeout: WAIT_TIMEOUT_MS
}, async (t) => {
  const broker = await startBroker(t, { behavior: "slow-thread-start", idleMs: IDLE_MS });
  const client = await broker.connect();
  const pending = assert.rejects(client.request("thread/start", { cwd: broker.cwd }), /connection closed/);
  await waitFor(() => broker.state().threads.length === 1);
  await client.close();
  await pending;
  await sleep(IDLE_WAIT_MS);
  assert.equal(broker.running(), true);
  broker.finishStart();
  assert.equal(await broker.exited, 0);
});

test("broker never exits idle when the idle period is zero", {
  timeout: WAIT_TIMEOUT_MS
}, async (t) => {
  const broker = await startBroker(t, { idleMs: 0 });
  await sleep(IDLE_WAIT_MS);
  assert.equal(broker.running(), true);
});

async function startEnsuredBrokerWorkspace(t) {
  const cwd = makeTempDir("cxc-ensure-");
  const binDir = makeTempDir();
  installFakeCodex(binDir);
  const env = { ...buildEnv(binDir), [BROKER_IDLE_MS_ENV]: String(IDLE_MS) };
  delete env.CODEX_COMPANION_APP_SERVER_ENDPOINT;
  t.after(async () => {
    const session = loadBrokerSession(cwd);
    if (session) {
      await sendBrokerShutdown(session.endpoint);
      teardownBrokerSession(session);
      clearBrokerSession(cwd);
    }
    fs.rmSync(cwd, { recursive: true, force: true });
    fs.rmSync(binDir, { recursive: true, force: true });
  });
  return {
    cwd,
    ensure() {
      return ensureBrokerSession(cwd, { env, timeoutMs: WAIT_TIMEOUT_MS });
    },
    appServerStarts() {
      return JSON.parse(fs.readFileSync(path.join(binDir, "fake-codex-state.json"), "utf8")).appServerStarts;
    }
  };
}

test("ensureBrokerSession starts a new broker after the previous one exited idle", {
  timeout: WAIT_TIMEOUT_MS * 2
}, async (t) => {
  const workspace = await startEnsuredBrokerWorkspace(t);
  const first = await workspace.ensure();
  assert.ok(first);
  await waitFor(
    () => loadBrokerSession(workspace.cwd) === null && !fs.existsSync(first.pidFile),
    "Broker did not exit after its idle period."
  );

  const second = await workspace.ensure();
  assert.ok(second);
  assert.notEqual(second.endpoint, first.endpoint);
  const client = await CodexAppServerClient.connect(workspace.cwd, { brokerEndpoint: second.endpoint });
  await client.request("thread/list", {});
  await client.close();
  assert.equal(workspace.appServerStarts(), 2);
});

test("ensureBrokerSession replaces a broker.json whose endpoint does not answer", {
  timeout: WAIT_TIMEOUT_MS
}, async (t) => {
  const workspace = await startEnsuredBrokerWorkspace(t);
  const staleDir = makeTempDir("cxc-");
  t.after(() => fs.rmSync(staleDir, { recursive: true, force: true }));
  const staleSession = {
    endpoint: createBrokerEndpoint(staleDir),
    pidFile: path.join(staleDir, "broker.pid"),
    logFile: path.join(staleDir, "broker.log"),
    sessionDir: staleDir,
    pid: null
  };
  saveBrokerSession(workspace.cwd, staleSession);

  const session = await workspace.ensure();
  assert.ok(session);
  assert.notEqual(session.endpoint, staleSession.endpoint);
  assert.deepEqual(loadBrokerSession(workspace.cwd), session);
  assert.equal(fs.existsSync(staleDir), false);
});

test("ensureBrokerSession rejects a malformed idle period before starting a broker", async (t) => {
  const cwd = makeTempDir("cxc-ensure-");
  t.after(() => fs.rmSync(cwd, { recursive: true, force: true }));
  await assert.rejects(
    ensureBrokerSession(cwd, { env: { ...process.env, [BROKER_IDLE_MS_ENV]: "soon" } }),
    /CODEX_COMPANION_BROKER_IDLE_MS must be a number of milliseconds/
  );
  assert.equal(loadBrokerSession(cwd), null);
});
