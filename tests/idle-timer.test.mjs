import test from "node:test";
import assert from "node:assert/strict";

import { IdleTimer } from "../plugins/codex/scripts/lib/idle-timer.mjs";
import {
  BROKER_IDLE_MS_ENV,
  DEFAULT_BROKER_IDLE_MS,
  resolveBrokerIdleMs
} from "../plugins/codex/scripts/lib/broker-lifecycle.mjs";

function createTimer(t, timeoutMs, { busy = false } = {}) {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  const state = { busy, idleCount: 0 };
  const timer = new IdleTimer(timeoutMs, {
    isBusy: () => state.busy,
    onIdle: () => {
      state.idleCount += 1;
    }
  });
  t.after(() => timer.stop());
  return { timer, state };
}

test("idle timer fires once the period passes without activity", (t) => {
  const { timer, state } = createTimer(t, 1000);
  timer.reset();
  t.mock.timers.tick(999);
  assert.equal(state.idleCount, 0);
  t.mock.timers.tick(1);
  assert.equal(state.idleCount, 1);
  t.mock.timers.tick(5000);
  assert.equal(state.idleCount, 1);
});

test("idle timer restarts the period on every reset", (t) => {
  const { timer, state } = createTimer(t, 1000);
  timer.reset();
  t.mock.timers.tick(800);
  timer.reset();
  t.mock.timers.tick(800);
  assert.equal(state.idleCount, 0);
  t.mock.timers.tick(200);
  assert.equal(state.idleCount, 1);
});

test("idle timer does not fire while busy and checks again every period", (t) => {
  const { timer, state } = createTimer(t, 1000, { busy: true });
  timer.reset();
  t.mock.timers.tick(5000);
  assert.equal(state.idleCount, 0);
  state.busy = false;
  t.mock.timers.tick(999);
  assert.equal(state.idleCount, 0);
  t.mock.timers.tick(1);
  assert.equal(state.idleCount, 1);
});

test("idle timer never fires when the period is zero or negative", (t) => {
  for (const timeoutMs of [0, -1]) {
    const { timer, state } = createTimer(t, timeoutMs);
    timer.reset();
    t.mock.timers.tick(DEFAULT_BROKER_IDLE_MS * 2);
    assert.equal(state.idleCount, 0);
    t.mock.timers.reset();
  }
});

test("idle timer stop cancels a pending period", (t) => {
  const { timer, state } = createTimer(t, 1000);
  timer.reset();
  timer.stop();
  t.mock.timers.tick(5000);
  assert.equal(state.idleCount, 0);
});

test("broker idle period defaults to 30 minutes", () => {
  assert.equal(DEFAULT_BROKER_IDLE_MS, 30 * 60 * 1000);
  assert.equal(resolveBrokerIdleMs({}), DEFAULT_BROKER_IDLE_MS);
  assert.equal(resolveBrokerIdleMs({ [BROKER_IDLE_MS_ENV]: "" }), DEFAULT_BROKER_IDLE_MS);
});

test("broker idle period honors the environment override", () => {
  assert.equal(BROKER_IDLE_MS_ENV, "CODEX_COMPANION_BROKER_IDLE_MS");
  assert.equal(resolveBrokerIdleMs({ [BROKER_IDLE_MS_ENV]: "1500" }), 1500);
  assert.equal(resolveBrokerIdleMs({ [BROKER_IDLE_MS_ENV]: "0" }), 0);
  assert.equal(resolveBrokerIdleMs({ [BROKER_IDLE_MS_ENV]: "-5" }), 0);
});

test("broker idle period rejects values that are not numbers", () => {
  assert.throws(
    () => resolveBrokerIdleMs({ [BROKER_IDLE_MS_ENV]: "thirty minutes" }),
    /CODEX_COMPANION_BROKER_IDLE_MS must be a number of milliseconds/
  );
});

test("broker idle period rejects values above the largest timer delay", () => {
  assert.equal(resolveBrokerIdleMs({ [BROKER_IDLE_MS_ENV]: String(2 ** 31 - 1) }), 2 ** 31 - 1);
  assert.throws(
    () => resolveBrokerIdleMs({ [BROKER_IDLE_MS_ENV]: String(2 ** 31) }),
    /CODEX_COMPANION_BROKER_IDLE_MS must be at most 2147483647 milliseconds/
  );
});
