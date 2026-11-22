export type SocketHeartbeatOptions = {
    interval: () => number;
    pongTimeout: number;
    maxSilence: () => number;
    pongRetryDelays: number[];
    send: (eventId: number) => void;
    onPingTimeout: () => void;
    onSilence: () => void;
};

export class SocketHeartbeat {
    private intervalTimer?: ReturnType<typeof setTimeout>;
    private pongTimer?: ReturnType<typeof setTimeout>;
    private retryTimer?: ReturnType<typeof setTimeout>;
    private lastPing = 0;
    private lastPong: number | null = null;
    private pongRetries = 0;

    constructor(private readonly options: SocketHeartbeatOptions) {}

    start() {
        this.stop();
        this.schedulePing();
        this.ping();
    }

    ping() {
        if (this.lastPong !== null && Date.now() - this.lastPong > this.options.maxSilence()) {
            this.stop();
            this.options.onSilence();
            return;
        }
        if (this.pongTimer) return;
        this.lastPing = Date.now();
        this.options.send(this.lastPing);
        this.pongTimer = setTimeout(() => this.missPong(), this.options.pongTimeout);
    }

    receivePong(eventId?: unknown) {
        if (eventId !== undefined && eventId !== null && Number(eventId) !== this.lastPing) return;
        clearTimeout(this.pongTimer);
        this.pongTimer = undefined;
        this.pongRetries = 0;
        this.lastPong = Date.now();
    }

    stop() {
        clearTimeout(this.intervalTimer);
        clearTimeout(this.pongTimer);
        clearTimeout(this.retryTimer);
        this.intervalTimer = undefined;
        this.pongTimer = undefined;
        this.retryTimer = undefined;
        this.lastPong = null;
        this.pongRetries = 0;
    }

    private schedulePing() {
        this.intervalTimer = setTimeout(() => {
            this.schedulePing();
            this.ping();
        }, this.options.interval());
    }

    private missPong() {
        this.pongTimer = undefined;
        const delay = this.options.pongRetryDelays[this.pongRetries];
        if (delay === undefined) {
            this.stop();
            this.options.onPingTimeout();
            return;
        }
        this.pongRetries++;
        this.retryTimer = setTimeout(() => this.ping(), delay);
    }
}
