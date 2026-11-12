import fs from "node:fs";
import path from "node:path";
import test from "node:test";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";

import { CodexAppServerClient } from "../plugins/codex/scripts/lib/app-server.mjs";
import { createBrokerEndpoint } from "../plugins/codex/scripts/lib/broker-endpoint.mjs";
import { waitForBrokerEndpoint, sendBrokerShutdown } from "../plugins/codex/scripts/lib/broker-lifecycle.mjs";
import { buildEnv, installFakeCodex } from "./fake-codex-fixture.mjs";
import { makeTempDir } from "./helpers.mjs";

const SCRIPT = fileURLToPath(new URL("../plugins/codex/scripts/app-server-broker.mjs", import.meta.url));
// Bound local subprocess startup and cleanup checks without relying on production unload timing.
const WAIT_TIMEOUT_MS = 10000;
const WAIT_INTERVAL_MS = 25;

async function waitFor(predicate) {
  const deadline = Date.now() + WAIT_TIMEOUT_MS;
  while (Date.now() < deadline) {
    if (await predicate()) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, WAIT_INTERVAL_MS));
  }
  assert.fail("Broker did not release its thread subscriptions.");
}

async function startBroker(t, { real = false, behavior = "review-ok", expectedStderr = null } = {}) {
  const cwd = makeTempDir("cxc-cleanup-");
  const binDir = makeTempDir();
  const endpoint = createBrokerEndpoint(cwd);
  if (!real) {
    installFakeCodex(binDir, behavior);
  } else {
    // Use the supported unload setting to avoid the default 60-second grace period.
    fs.writeFileSync(path.join(binDir, "config.toml"), "thread_unload_delay_secs = 1\n");
  }
  const env = real ? { ...process.env, CODEX_HOME: binDir } : buildEnv(binDir);
  delete env.CODEX_COMPANION_APP_SERVER_ENDPOINT;
  const proc = spawn(process.execPath, [SCRIPT, "serve", "--endpoint", endpoint, "--cwd", cwd], {
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
    fs.rmSync(cwd, { recursive: true, force: true });
    fs.rmSync(binDir, { recursive: true, force: true });
  });
  assert.equal(await waitForBrokerEndpoint(endpoint, WAIT_TIMEOUT_MS), true, stderr);
  return {
    cwd,
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
