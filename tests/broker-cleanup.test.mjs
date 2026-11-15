import fs from "node:fs";
import net from "node:net";
import path from "node:path";
import test from "node:test";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";

import { CodexAppServerClient } from "../plugins/codex/scripts/lib/app-server.mjs";
import { createBrokerEndpoint, parseBrokerEndpoint } from "../plugins/codex/scripts/lib/broker-endpoint.mjs";
import {
  BROKER_IDLE_MS_ENV,
  clearBrokerSession,
  ensureBrokerSession,
  loadBrokerSession,
  saveBrokerSession,
  sendBrokerShutdown,
  teardownBrokerSession
} from "../plugins/codex/scripts/lib/broker-lifecycle.mjs";
import { buildEnv, installFakeCodex } from "./fake-codex-fixture.mjs";
import { makeTempDir } from "./helpers.mjs";

const SCRIPT = fileURLToPath(new URL("../plugins/codex/scripts/app-server-broker.mjs", import.meta.url));
// Bound local subprocess startup and cleanup checks without relying on production unload timing.
const WAIT_TIMEOUT_MS = 10000;
const WAIT_INTERVAL_MS = 25;
// Retry interval for the connection that holds a new broker busy until the test releases it.
const HOLD_RETRY_MS = 5;
// Short idle period so idle-exit tests finish quickly; several periods fit in IDLE_WAIT_MS.
const IDLE_MS = 1000;
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

// Connects as soon as the broker accepts connections and keeps the connection open. While it is open the
// broker counts a client, so its idle period only starts once the test releases this connection.
async function holdBrokerConnection(endpoint) {
  const target = parseBrokerEndpoint(endpoint);
  const deadline = Date.now() + WAIT_TIMEOUT_MS;
  while (Date.now() < deadline) {
    const socket = await new Promise((resolve) => {
      const candidate = net.createConnection({ path: target.path });
      candidate.once("connect", () => resolve(candidate));
      candidate.once("error", () => resolve(null));
    });
    if (socket) {
      // The broker ends this connection when it shuts down; that is expected and needs no handling.
      socket.on("error", () => {});
      return socket;
    }
    await sleep(HOLD_RETRY_MS);
  }
  return null;
}

async function startBroker(t, { real = false, behavior = "review-ok", expectedStderr = null, idleMs = null } = {}) {
  const cwd = makeTempDir("cxc-cleanup-");
  const binDir = makeTempDir();
  const sessionDir = makeTempDir("cxc-");
  const endpoint = createBrokerEndpoint(sessionDir);
  const pidFile = path.join(sessionDir, "broker.pid");
  const logFile = path.join(sessionDir, "broker.log");
  fs.writeFileSync(logFile, "");
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
  const proc = spawn(process.execPath, [SCRIPT, "serve", "--endpoint", endpoint, "--cwd", cwd, "--pid-file", pidFile, "--log-file", logFile], {
    cwd,
    env,
    stdio: ["ignore", "pipe", "pipe"]
  });
  let stderr = "";
  proc.stderr.on("data", (chunk) => { stderr += chunk; });
  const exited = new Promise((resolve) => proc.on("exit", (code) => resolve(code)));
  const clients = [];
  let hold = null;
  t.after(async () => {
    hold?.destroy();
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
  hold = await holdBrokerConnection(endpoint);
  assert.ok(hold, `Broker did not accept connections.\n${stderr}`);
  return {
    cwd,
    endpoint,
    pidFile,
    logFile,
    sessionDir,
    exited,
    running() {
      return proc.exitCode === null;
    },
    // Starts the idle period: the broker then has only the clients the test opened.
    releaseHold() {
      hold.destroy();
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
  saveBrokerSession(broker.cwd, {
    endpoint: broker.endpoint,
    pidFile: broker.pidFile,
    logFile: broker.logFile,
    sessionDir: broker.sessionDir,
    pid: null
  });
  broker.releaseHold();
  assert.equal(await broker.exited, 0);
  assert.equal(loadBrokerSession(broker.cwd), null);
  assert.equal(fs.existsSync(broker.pidFile), false);
  assert.equal(fs.existsSync(broker.logFile), false);
  assert.equal(fs.existsSync(broker.sessionDir), false);
});

test("broker idle exit keeps another broker's broker.json and still removes its own files", {
  timeout: WAIT_TIMEOUT_MS
}, async (t) => {
  const broker = await startBroker(t, { idleMs: IDLE_MS });
  const otherSessionDir = makeTempDir("cxc-");
  t.after(() => fs.rmSync(otherSessionDir, { recursive: true, force: true }));
  const otherLogFile = path.join(otherSessionDir, "broker.log");
  fs.writeFileSync(otherLogFile, "");
  const otherSession = {
    endpoint: createBrokerEndpoint(otherSessionDir),
    pidFile: path.join(otherSessionDir, "broker.pid"),
    logFile: otherLogFile,
    sessionDir: otherSessionDir,
    pid: null
  };
  saveBrokerSession(broker.cwd, otherSession);
  broker.releaseHold();
  assert.equal(await broker.exited, 0);
  assert.deepEqual(loadBrokerSession(broker.cwd), otherSession);
  assert.equal(fs.existsSync(otherLogFile), true);
  assert.equal(fs.existsSync(broker.pidFile), false);
  assert.equal(fs.existsSync(broker.logFile), false);
  assert.equal(fs.existsSync(broker.sessionDir), false);
});

test("broker stays up while a client is connected", {
  timeout: WAIT_TIMEOUT_MS
}, async (t) => {
  const broker = await startBroker(t, { idleMs: IDLE_MS });
  const client = await broker.connect();
  broker.releaseHold();
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
  broker.releaseHold();
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
  broker.releaseHold();
  await sleep(IDLE_WAIT_MS);
  assert.equal(broker.running(), true);
  broker.finishStart();
  assert.equal(await broker.exited, 0);
});

test("broker never exits idle when the idle period is zero", {
  timeout: WAIT_TIMEOUT_MS
}, async (t) => {
  const broker = await startBroker(t, { idleMs: 0 });
  broker.releaseHold();
  await sleep(IDLE_WAIT_MS);
  assert.equal(broker.running(), true);
});

async function startEnsuredBrokerWorkspace(t) {
  const cwd = makeTempDir("cxc-ensure-");
  const binDir = makeTempDir();
  const fakeStatePath = path.join(binDir, "fake-codex-state.json");
  installFakeCodex(binDir);
  const env = { ...buildEnv(binDir), [BROKER_IDLE_MS_ENV]: String(IDLE_MS) };
  delete env.CODEX_COMPANION_APP_SERVER_ENDPOINT;
  const clients = [];
  t.after(async () => {
    await Promise.all(clients.map((client) => client.close()));
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
    // Connects the way companion commands do: discover or start the workspace broker, then hand-shake.
    async connect() {
      const client = await CodexAppServerClient.connect(cwd, { env });
      clients.push(client);
      return client;
    },
    appServerStarts() {
      return fs.existsSync(fakeStatePath) ? JSON.parse(fs.readFileSync(fakeStatePath, "utf8")).appServerStarts : 0;
    }
  };
}

// Serves a broker endpoint recorded in broker.json and passes every JSON-RPC message except broker/shutdown
// to onMessage, so a test controls exactly when the broker answers or goes away.
async function startScriptedBroker(t, cwd, onMessage) {
  const sessionDir = makeTempDir("cxc-");
  const endpoint = createBrokerEndpoint(sessionDir);
  const server = net.createServer((socket) => {
    socket.setEncoding("utf8");
    // Clients see the scripted disconnects; the server side needs no handling for them.
    socket.on("error", () => {});
    let buffer = "";
    socket.on("data", (chunk) => {
      buffer += chunk;
      let newlineIndex = buffer.indexOf("\n");
      while (newlineIndex !== -1) {
        const line = buffer.slice(0, newlineIndex);
        buffer = buffer.slice(newlineIndex + 1);
        newlineIndex = buffer.indexOf("\n");
        const message = JSON.parse(line);
        if (message.method === "broker/shutdown") {
          socket.end(`${JSON.stringify({ id: message.id, result: {} })}\n`);
          continue;
        }
        onMessage({ socket, server, message });
      }
    });
  });
  await new Promise((resolve) => server.listen(parseBrokerEndpoint(endpoint).path, resolve));
  saveBrokerSession(cwd, { endpoint, pidFile: null, logFile: null, sessionDir, pid: null });
  t.after(async () => {
    if (server.listening) {
      await new Promise((resolve) => server.close(resolve));
    }
    fs.rmSync(sessionDir, { recursive: true, force: true });
  });
  return { endpoint };
}

for (const { label, removesBrokerState } of [
  { label: "exits idle", removesBrokerState: true },
  { label: "dies without cleanup", removesBrokerState: false }
]) {
  test(`a command whose discovered broker ${label} during the handshake connects to a new broker`, {
    timeout: WAIT_TIMEOUT_MS
  }, async (t) => {
    const workspace = await startEnsuredBrokerWorkspace(t);
    let handshakes = 0;
    // Answers the readiness probe (a bare connection) but goes away as soon as a client sends initialize,
    // which is the window between broker discovery and the client handshake.
    const departed = await startScriptedBroker(t, workspace.cwd, ({ socket, server, message }) => {
      if (message.method !== "initialize") {
        return;
      }
      handshakes += 1;
      if (removesBrokerState) {
        clearBrokerSession(workspace.cwd);
      }
      server.close();
      socket.destroy();
    });

    const client = await workspace.connect();
    await client.request("thread/list", {});

    assert.equal(handshakes, 1);
    assert.equal(client.transport, "broker");
    const session = loadBrokerSession(workspace.cwd);
    assert.ok(session);
    assert.notEqual(session.endpoint, departed.endpoint);
    assert.equal(workspace.appServerStarts(), 1);
  });
}

test("a command retries a failed broker handshake only once", {
  timeout: WAIT_TIMEOUT_MS
}, async (t) => {
  const workspace = await startEnsuredBrokerWorkspace(t);
  let handshakes = 0;
  await startScriptedBroker(t, workspace.cwd, ({ socket, message }) => {
    if (message.method === "initialize") {
      handshakes += 1;
      socket.destroy();
    }
  });

  await assert.rejects(workspace.connect());
  assert.equal(handshakes, 2);
  assert.equal(workspace.appServerStarts(), 0);
});

test("a command does not retry once an application request was sent", {
  timeout: WAIT_TIMEOUT_MS
}, async (t) => {
  const workspace = await startEnsuredBrokerWorkspace(t);
  let handshakes = 0;
  let applicationRequests = 0;
  await startScriptedBroker(t, workspace.cwd, ({ socket, message }) => {
    if (message.method === "initialize") {
      handshakes += 1;
      socket.write(`${JSON.stringify({ id: message.id, result: {} })}\n`);
      return;
    }
    if (message.id !== undefined) {
      applicationRequests += 1;
      socket.destroy();
    }
  });

  const client = await workspace.connect();
  await assert.rejects(client.request("thread/list", {}));
  assert.equal(handshakes, 1);
  assert.equal(applicationRequests, 1);
  assert.equal(workspace.appServerStarts(), 0);
});

test("ensureBrokerSession starts a new broker after the previous one exited idle", {
  timeout: WAIT_TIMEOUT_MS * 2
}, async (t) => {
  const workspace = await startEnsuredBrokerWorkspace(t);
  const first = await workspace.ensure();
  assert.ok(first);
  await waitFor(() => !fs.existsSync(first.pidFile), "Broker did not exit after its idle period.");

  // The connected client keeps the new broker busy while the test reads its state.
  const client = await workspace.connect();
  await client.request("thread/list", {});
  assert.equal(client.transport, "broker");
  const second = loadBrokerSession(workspace.cwd);
  assert.ok(second);
  assert.notEqual(second.endpoint, first.endpoint);
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

  const client = await workspace.connect();
  await client.request("thread/list", {});
  const session = loadBrokerSession(workspace.cwd);
  assert.ok(session);
  assert.notEqual(session.endpoint, staleSession.endpoint);
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
