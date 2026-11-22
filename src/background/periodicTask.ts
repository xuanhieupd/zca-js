export class PeriodicTask {
    private timer?: ReturnType<typeof setInterval>;

    constructor(
        private readonly interval: number,
        private readonly run: () => Promise<unknown> | unknown,
        private readonly onError: (error: unknown) => void,
    ) {}

    start(runNow = false) {
        this.stop();
        this.timer = setInterval(() => this.tick(), this.interval);
        if (runNow) this.tick();
    }

    stop() {
        clearInterval(this.timer);
        this.timer = undefined;
    }

    private tick() {
        try {
            Promise.resolve(this.run()).catch(this.onError);
        } catch (error) {
            this.onError(error);
        }
    }
}
