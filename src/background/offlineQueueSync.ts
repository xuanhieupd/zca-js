import { OFFLINE_QUEUE_COMMANDS, queueKeyOf, type QueueCommand } from "./socketCommand.js";
import { isNewerActionId, type QueueCursorStore } from "./queueCursorStore.js";

export type QueueStatus = Record<string, { lastId?: unknown; evict?: number } | null | undefined>;

export type OfflineQueueData = {
    more?: number | boolean;
    lastActionId?: unknown;
    queueStatus?: QueueStatus;
};

export type OfflineQueueRequest = {
    cmd: number;
    subCmd: number;
    data: { first: boolean; reqId: string; lastId: string; preIds: string[] };
};

export type OfflineQueueSyncOptions = {
    store: QueueCursorStore;
    timeout: () => number;
    nextRequestId: () => string;
    send: (request: OfflineQueueRequest) => void;
    onDone: (queueKeys: string[]) => void;
    onTimeout: () => void;
    onError: (error: unknown) => void;
};

export class OfflineQueueSync {
    private readonly pending = new Set<string>();
    private pulled: string[] = [];
    private timer?: ReturnType<typeof setTimeout>;

    constructor(private readonly options: OfflineQueueSyncOptions) {}

    get pulling() {
        return this.pending.size > 0;
    }

    owns(cmd: number, subCmd: number) {
        return this.pending.has(queueKeyOf({ cmd, subCmd }));
    }

    async begin(queues: QueueCommand[]) {
        this.stop();
        const resumable: { queue: QueueCommand; lastId: string }[] = [];
        for (const queue of queues) {
            if (!OFFLINE_QUEUE_COMMANDS.has(queue.cmd)) continue;
            const lastId = await this.options.store.get(queueKeyOf(queue));
            if (lastId) resumable.push({ queue, lastId });
        }
        if (resumable.length === 0) return;
        this.pulled = resumable.map(({ queue }) => queueKeyOf(queue));
        this.pulled.forEach((key) => this.pending.add(key));
        this.timer = setTimeout(() => {
            this.stop();
            this.options.onTimeout();
        }, this.options.timeout());
        resumable.forEach(({ queue, lastId }) => this.request(queue, true, lastId));
    }

    async receive(cmd: number, subCmd: number, data: OfflineQueueData) {
        const queue = { cmd, subCmd };
        const key = queueKeyOf(queue);
        if (!this.pending.has(key)) return;
        await this.track(data.queueStatus);
        const lastActionId = String(data.lastActionId ?? "");
        if (data.more && /^\d+$/.test(lastActionId)) {
            this.request(queue, false, lastActionId);
            return;
        }
        this.pending.delete(key);
        if (this.pending.size > 0) return;
        clearTimeout(this.timer);
        this.timer = undefined;
        this.options.onDone(this.pulled);
    }

    async track(queueStatus?: QueueStatus) {
        if (!queueStatus || typeof queueStatus !== "object") return;
        try {
            for (const [queueKey, status] of Object.entries(queueStatus)) {
                const lastId = String(status?.lastId ?? "");
                if (isNewerActionId(lastId, await this.options.store.get(queueKey))) {
                    await this.options.store.set(queueKey, lastId);
                }
            }
        } catch (error) {
            this.options.onError(error);
        }
    }

    stop() {
        clearTimeout(this.timer);
        this.timer = undefined;
        this.pending.clear();
    }

    private request(queue: QueueCommand, first: boolean, lastId: string) {
        this.options.send({
            cmd: queue.cmd,
            subCmd: queue.subCmd,
            data: { first, reqId: this.options.nextRequestId(), lastId, preIds: [] },
        });
    }
}
