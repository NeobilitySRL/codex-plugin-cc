export class ThreadSubscriptions {
  constructor() {
    this.owners = new Map();
  }

  record(owner, method, params, result) {
    const threadIds = [];
    if (method === "thread/start" || method === "thread/resume" || method === "thread/fork") {
      threadIds.push(result.thread?.id);
    }
    if (method === "review/start") {
      threadIds.push(params.threadId, result.reviewThreadId);
    }
    for (const threadId of threadIds) {
      if (!threadId) {
        continue;
      }
      if (!this.owners.has(threadId)) {
        this.owners.set(threadId, new Set());
      }
      this.owners.get(threadId).add(owner);
    }
  }

  release(owner) {
    const threadIds = [];
    for (const [threadId, owners] of this.owners) {
      if (owners.delete(owner) && owners.size === 0) {
        this.owners.delete(threadId);
        threadIds.push(threadId);
      }
    }
    return threadIds;
  }
}
