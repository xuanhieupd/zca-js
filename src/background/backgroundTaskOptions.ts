import type { QueueCursorStore } from "./queueCursorStore.js";

export type BackgroundTaskOptions = {
    keepAlive: boolean;
    syncOffline: boolean;
    fetchMissingMessages: boolean;
    verifyMessages: boolean;
    refreshServerInfo: boolean;
    activeStatus: boolean;
    queueCursorStore?: QueueCursorStore;
};

export const DEFAULT_BACKGROUND_TASK_OPTIONS: BackgroundTaskOptions = {
    keepAlive: true,
    syncOffline: false,
    fetchMissingMessages: true,
    verifyMessages: true,
    refreshServerInfo: true,
    activeStatus: false,
};
