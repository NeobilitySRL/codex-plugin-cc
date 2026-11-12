import test from "node:test";
import assert from "node:assert/strict";

import { ThreadSubscriptions } from "../plugins/codex/scripts/lib/thread-subscriptions.mjs";

for (const method of ["thread/start", "thread/resume", "thread/fork"]) {
  test(`subscriptions release the thread returned by ${method}`, () => {
    const subscriptions = new ThreadSubscriptions();
    const owner = {};
    subscriptions.record(owner, method, {}, { thread: { id: "thread-1" } });
    assert.deepEqual(subscriptions.release(owner), ["thread-1"]);
    assert.deepEqual(subscriptions.release(owner), []);
  });
}

test("subscriptions retain threads until their last owner disconnects", () => {
  const subscriptions = new ThreadSubscriptions();
  const first = {};
  const second = {};
  subscriptions.record(first, "thread/start", {}, { thread: { id: "shared" } });
  subscriptions.record(second, "thread/resume", {}, { thread: { id: "shared" } });
  assert.deepEqual(subscriptions.release(first), []);
  assert.deepEqual(subscriptions.release(second), ["shared"]);
});

test("subscriptions include detached review threads without duplicates", () => {
  const subscriptions = new ThreadSubscriptions();
  const owner = {};
  subscriptions.record(owner, "thread/start", {}, { thread: { id: "source" } });
  subscriptions.record(owner, "review/start", { threadId: "source" }, { reviewThreadId: "review" });
  assert.deepEqual(subscriptions.release(owner), ["source", "review"]);
});

test("read and interrupt requests do not acquire subscriptions", () => {
  const subscriptions = new ThreadSubscriptions();
  const owner = {};
  subscriptions.record(owner, "thread/read", {}, { thread: { id: "read" } });
  subscriptions.record(owner, "turn/interrupt", { threadId: "active" }, {});
  assert.deepEqual(subscriptions.release(owner), []);
});
