import { SocketCommand, queueKeyOf, type QueueCommand } from "./socketCommand.js";
import type { QueueCursorStore } from "./queueCursorStore.js";

export type MissingMessageId = { actionId: unknown; msgId: unknown };

export type HasMessageData = {
    queueId?: QueueCommand;
    ids?: MissingMessageId[];
};

export type GetMessageRequest = {
    cmd: number;
    subCmd: number;
    data: { queueId: QueueCommand; ids: MissingMessageId[]; lastId: string; reqId: string };
};

export type MissingMessageFetcherOptions = {
    store: QueueCursorStore;
    batchDelay: number;
    batchSize: number;
    retryDelay: number;
    maxRetries: number;
    nextRequestId: () => string;
    send: (request: GetMessageRequest) => void;
    onError: (error: unknown) => void;
};

type PendingMessage = { queueId: QueueCommand; id: MissingMessageId; retryTimer?: ReturnType<typeof setTimeout> };

export class MissingMessageFetcher {
    private queued: { queueId: QueueCommand; ids: MissingMessageId[] }[] = [];
    private batchTimer?: ReturnType<typeof setTimeout>;
    private readonly pending = new Map<string, PendingMessage>();

    constructor(private readonly options: MissingMessageFetcherOptions) {}

    announce(data: HasMessageData) {
        if (!data?.queueId || !Array.isArray(data.ids) || data.ids.length === 0) return;
        this.queued.push({ queueId: data.queueId, ids: data.ids });
        if (this.batchTimer) return;
        this.batchTimer = setTimeout(() => {
            this.batchTimer = undefined;
            this.flush().catch(this.options.onError);
        }, this.options.batchDelay);
    }

    received(msgIds: unknown[]) {
        for (const msgId of msgIds) {
            const pending = this.pending.get(String(msgId));
            if (!pending) continue;
            clearTimeout(pending.retryTimer);
            this.pending.delete(String(msgId));
        }
    }

    stop() {
        clearTimeout(this.batchTimer);
        this.batchTimer = undefined;
        this.queued = [];
        this.pending.forEach((pending) => clearTimeout(pending.retryTimer));
        this.pending.clear();
    }

    private async flush() {
        const grouped = new Map<string, { queueId: QueueCommand; ids: MissingMessageId[] }>();
        for (const { queueId, ids } of this.queued.splice(0)) {
            const group = grouped.get(queueKeyOf(queueId)) ?? { queueId, ids: [] };
            group.ids.push(...ids);
            grouped.set(queueKeyOf(queueId), group);
        }
        for (const { queueId, ids } of grouped.values()) {
            for (let index = 0; index < ids.length; index += this.options.batchSize) {
                await this.request(queueId, ids.slice(index, index + this.options.batchSize), 0);
            }
        }
    }

    private async request(queueId: QueueCommand, ids: MissingMessageId[], attempt: number) {
        const lastId = (await this.options.store.get(queueKeyOf(queueId))) ?? "1";
        this.options.send({
            cmd: SocketCommand.GetMessage,
            subCmd: 0,
            data: { queueId, ids, lastId, reqId: this.options.nextRequestId() },
        });
        ids.forEach((id) => this.awaitMessage(queueId, id, attempt));
    }

    private awaitMessage(queueId: QueueCommand, id: MissingMessageId, attempt: number) {
        const key = String(id.msgId);
        if (!id.msgId || (attempt === 0 && this.pending.has(key))) return;
        if (attempt >= this.options.maxRetries) return;
        const retryTimer = setTimeout(() => {
            this.pending.delete(key);
            this.request(queueId, [id], attempt + 1).catch(this.options.onError);
        }, this.options.retryDelay);
        this.pending.set(key, { queueId, id, retryTimer });
    }
}
