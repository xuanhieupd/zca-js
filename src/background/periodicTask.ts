export class PeriodicTask {
    private timer?: ReturnType<typeof setTimeout>;

    constructor(
        private readonly interval: () => number,
        private readonly run: () => Promise<unknown> | unknown,
        private readonly onError: (error: unknown) => void,
    ) {}

    start(runNow = false) {
        this.stop();
        this.schedule();
        if (runNow) this.tick();
    }

    stop() {
        clearTimeout(this.timer);
        this.timer = undefined;
    }

    private schedule() {
        this.timer = setTimeout(() => {
            this.schedule();
            this.tick();
        }, this.interval());
    }

    private tick() {
        try {
            Promise.resolve(this.run()).catch(this.onError);
        } catch (error) {
            this.onError(error);
        }
    }
}
