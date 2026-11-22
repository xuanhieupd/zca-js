import { isNewerActionId } from "./queueCursorStore.js";
import type { QueueStatus } from "./offlineQueueSync.js";
import { SocketCommand } from "./socketCommand.js";

export type VerifyMessageRequest = {
    cmd: number;
    subCmd: number;
    data: { nretry: number; queueName: string; ids: { actionId: string }[]; reqId: string };
};

export type VerifyMessageResult = { error?: number; reqId?: string };

export type MessageVerifierOptions = {
    interval: number;
    batchSize: number;
    retryMissDelay: number;
    nextRequestId: () => string;
    send: (request: VerifyMessageRequest) => void;
};

type Verifying = { queueName: string; batch: string[]; retry: number; retryTimer?: ReturnType<typeof setTimeout> };

export class MessageVerifier {
    private readonly queues = new Map<string, string[]>();
    private readonly timers = new Map<string, ReturnType<typeof setTimeout>>();
    private readonly verifying = new Map<string, Verifying>();
    private started = false;

    constructor(private readonly options: MessageVerifierOptions) {}

    init(queueNames: string[]) {
        this.stop();
        queueNames.forEach((queueName) => this.queues.set(queueName, []));
    }

    start() {
        this.started = true;
        this.queues.forEach((ids, queueName) => {
            if (ids.length >= this.options.batchSize) this.verify(queueName);
        });
    }

    observe(queueStatus?: QueueStatus) {
        if (!queueStatus || typeof queueStatus !== "object") return;
        for (const [queueName, status] of Object.entries(queueStatus)) {
            const ids = (status as { ids?: unknown[] } | null)?.ids;
            if (!this.queues.has(queueName) || !Array.isArray(ids)) continue;
            this.add(queueName, ids.map(String));
        }
    }

    receive(result: VerifyMessageResult) {
        const verifying = result.reqId ? this.verifying.get(result.reqId) : undefined;
        if (!verifying) return;
        if (result.error === 1) {
            clearTimeout(verifying.retryTimer);
            verifying.retryTimer = setTimeout(() => {
                this.verifying.delete(result.reqId!);
                this.verify(verifying.queueName, verifying.retry + 1);
            }, this.options.retryMissDelay);
            return;
        }
        this.finish(result.reqId!, verifying);
    }

    receiveMissed(reqId?: unknown) {
        const verifying = typeof reqId === "string" ? this.verifying.get(reqId) : undefined;
        if (verifying) this.finish(reqId as string, verifying);
    }

    stop() {
        this.started = false;
        this.timers.forEach((timer) => clearTimeout(timer));
        this.timers.clear();
        this.verifying.forEach((verifying) => clearTimeout(verifying.retryTimer));
        this.verifying.clear();
        this.queues.clear();
    }

    private add(queueName: string, ids: string[]) {
        const queue = this.queues.get(queueName)!;
        const anchor = queue[0] ?? null;
        for (const id of ids) {
            if (queue.includes(id)) continue;
            if (anchor !== null && !isNewerActionId(id, anchor)) continue;
            queue.push(id);
        }
        queue.sort((left, right) => (isNewerActionId(left, right) ? 1 : left === right ? 0 : -1));
        if (queue.length >= this.options.batchSize + 2) {
            this.verify(queueName);
            return;
        }
        if (this.timers.has(queueName) || this.isVerifying(queueName)) return;
        this.timers.set(
            queueName,
            setTimeout(() => {
                this.timers.delete(queueName);
                this.verify(queueName);
            }, this.options.interval),
        );
    }

    private verify(queueName: string, retry = 0) {
        if (!this.started || this.isVerifying(queueName)) return;
        clearTimeout(this.timers.get(queueName));
        this.timers.delete(queueName);
        const batch = (this.queues.get(queueName) ?? []).slice(0, this.options.batchSize);
        if (batch.length < 2) return;
        const reqId = this.options.nextRequestId();
        this.verifying.set(reqId, { queueName, batch, retry });
        this.options.send({
            cmd: SocketCommand.VerifyMessage,
            subCmd: 0,
            data: { nretry: retry, queueName, ids: batch.map((actionId) => ({ actionId })), reqId },
        });
    }

    private finish(reqId: string, verifying: Verifying) {
        clearTimeout(verifying.retryTimer);
        this.verifying.delete(reqId);
        const verified = verifying.batch.slice(0, -1);
        const queue = this.queues.get(verifying.queueName);
        if (!queue) return;
        this.queues.set(
            verifying.queueName,
            queue.filter((id) => !verified.includes(id)),
        );
        if (queue.length - verified.length >= this.options.batchSize) this.verify(verifying.queueName);
    }

    private isVerifying(queueName: string) {
        return [...this.verifying.values()].some((verifying) => verifying.queueName === queueName);
    }
}
