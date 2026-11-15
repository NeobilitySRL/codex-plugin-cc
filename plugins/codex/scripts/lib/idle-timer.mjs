// Calls onIdle once a full period passes after the last reset while isBusy() is false.
// A period that ends while busy starts a new period. A period of 0 or less disables the timer.
export class IdleTimer {
  constructor(timeoutMs, { isBusy, onIdle }) {
    this.timeoutMs = timeoutMs;
    this.isBusy = isBusy;
    this.onIdle = onIdle;
    this.timer = null;
  }

  reset() {
    this.stop();
    if (!(this.timeoutMs > 0)) {
      return;
    }
    this.timer = setTimeout(() => {
      this.timer = null;
      if (this.isBusy()) {
        this.reset();
        return;
      }
      this.onIdle();
    }, this.timeoutMs);
  }

  stop() {
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
  }
}
