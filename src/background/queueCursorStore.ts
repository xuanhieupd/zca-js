export type QueueCursorStore = {
    get(queueKey: string): Promise<string | null> | string | null;
    set(queueKey: string, lastId: string): Promise<void> | void;
};

export class MemoryQueueCursorStore implements QueueCursorStore {
    private readonly cursors = new Map<string, string>();

    get(queueKey: string) {
        return this.cursors.get(queueKey) ?? null;
    }

    set(queueKey: string, lastId: string) {
        this.cursors.set(queueKey, lastId);
    }
}

export function isNewerActionId(candidate: string, current: string | null): boolean {
    if (!/^\d+$/.test(candidate)) return false;
    if (current === null || !/^\d+$/.test(current)) return true;
    return BigInt(candidate) > BigInt(current);
}
