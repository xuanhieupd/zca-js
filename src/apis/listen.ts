import EventEmitter from "events";
import WebSocket from "ws";
import { type FriendEvent, initializeFriendEvent, type TFriendEvent } from "../models/FriendEvent.js";
import { type GroupEvent, initializeGroupEvent, type TGroupEvent } from "../models/GroupEvent.js";
import type { Message, TGroupMessage, TMessage, TReaction, TUndo, Typing } from "../models/index.js";
import { GroupMessage, UserMessage, Reaction, Undo, ThreadType, GroupTyping, UserTyping } from "../models/index.js";
import { decodeEventData, getFriendEventType, getGroupEventType, hasOwn, logger, makeURL } from "../utils.js";
import { ZaloApiError } from "../Errors/ZaloApiError.js";
import type { ContextSession } from "../context.js";
import { type SeenMessage, GroupSeenMessage, UserSeenMessage } from "../models/SeenMessage.js";
import { type DeliveredMessage, UserDeliveredMessage, GroupDeliveredMessage } from "../models/DeliveredMessage.js";
import { ActiveStatus } from "../background/activeStatus.js";
import { DEFAULT_BACKGROUND_TASK_OPTIONS, type BackgroundTaskOptions } from "../background/backgroundTaskOptions.js";
import { MessageVerifier, type VerifyMessageResult } from "../background/messageVerifier.js";
import { MissingMessageFetcher, type HasMessageData } from "../background/missingMessageFetcher.js";
import { getServerInfo } from "./login.js";
import { OfflineQueueSync, type OfflineQueueData } from "../background/offlineQueueSync.js";
import { PeriodicTask } from "../background/periodicTask.js";
import { MemoryQueueCursorStore } from "../background/queueCursorStore.js";
import {
    MESSAGE_QUEUE_COMMANDS,
    PingSubCommand,
    SocketCommand,
    queueKeyOf,
    type QueueCommand,
} from "../background/socketCommand.js";
import { SocketHeartbeat } from "../background/socketHeartbeat.js";

type UploadEventData = {
    fileUrl: string;
    fileId: string;
};

export type WsPayload<T = Record<string, unknown>> = {
    version: number;
    cmd: number;
    subCmd: number;
    data: T;
};

export type OnMessageCallback = (message: Message) => unknown;
export type OnClosedCallback = (code: CloseReason, reason: string) => unknown;
export type OnErrorCallback = (error: unknown) => unknown;

export enum CloseReason {
    ManualClosure = 1000,
    AbnormalClosure = 1006,
    DuplicateConnection = 3000,
    KickConnection = 3003,
    PingTimeout = 5011,
    EstablishTimeout = 5012,
    OfflineTimeout = 5014,
    AuthTimeout = 5015,
    AbnormalStopPing = 5016,
}

const INTERNAL_CLOSE_REASONS = new Set<number>([
    CloseReason.PingTimeout,
    CloseReason.EstablishTimeout,
    CloseReason.OfflineTimeout,
    CloseReason.AuthTimeout,
    CloseReason.AbnormalStopPing,
]);

const SOCKET_TIMING_DEFAULTS = {
    establishTimeout: 15_000,
    authTimeout: 15_000,
    pongTimeout: 5_000,
    maxSilence: 300_000,
    offlineTimeout: 480_000,
    activeInterval: 60_000,
    keepAliveSeconds: 120,
    getMessageDelay: 10,
    getMessageBatch: 20,
    getMessageRetryDelay: 3_000,
    getMessageMaxRetries: 4,
    verifyInterval: 30_000,
    verifyBatch: 20,
    verifyRetryMiss: 30_000,
    serverInfoInterval: 3_600_000,
};

type EventData = Awaited<ReturnType<typeof decodeEventData>>;

export type KeepAliveCall = () => Promise<{ config_vesion?: number } | null | undefined>;

interface ListenerEvents {
    connected: [];
    disconnected: [code: CloseReason, reason: string];
    closed: [code: CloseReason, reason: string];
    error: [error: unknown];
    typing: [typing: Typing];
    message: [message: Message];
    old_messages: [messages: Message[], type: ThreadType];
    seen_messages: [messages: SeenMessage[]];
    delivered_messages: [messages: DeliveredMessage[]];
    reaction: [reaction: Reaction];
    old_reactions: [reactions: Reaction[], isGroup: boolean];
    upload_attachment: [data: UploadEventData];
    undo: [data: Undo];
    friend_event: [data: FriendEvent];
    group_event: [data: GroupEvent];
    cipher_key: [key: string];
    offline_done: [queueKeys: string[]];
    config_version: [version: number];
    settings_updated: [settings: ContextSession["settings"]];
}

export class Listener extends EventEmitter<ListenerEvents> {
    private wsURL: string;
    private cookie: string;
    private userAgent: string;

    private ws: WebSocket | null;
    private retryCount: Record<
        string,
        {
            count: number;
            max: number;
            times: number[];
        }
    >;
    private rotateCount: number;

    private onConnectedCallback: () => unknown;
    private onClosedCallback: OnClosedCallback;
    private onErrorCallback: OnErrorCallback;
    private onMessageCallback: OnMessageCallback;
    private cipherKey?: string;

    private selfListen;
    private readonly tasks: BackgroundTaskOptions;
    private readonly heartbeat: SocketHeartbeat;
    private readonly offlineSync: OfflineQueueSync;
    private readonly missingMessages: MissingMessageFetcher;
    private readonly verifier: MessageVerifier;
    private readonly serverInfoTask: PeriodicTask;
    private readonly keepAliveTask: PeriodicTask;
    private readonly activeTask: PeriodicTask;
    private establishTimer?: ReturnType<typeof setTimeout>;
    private authTimer?: ReturnType<typeof setTimeout>;
    private internalCloseReason?: CloseReason;
    private configVersion = -1;

    private id = 0;

    constructor(
        private ctx: ContextSession,
        private urls: string[],
        private keepAliveCall?: KeepAliveCall,
    ) {
        super();
        if (!ctx.cookie) throw new ZaloApiError("Cookie is not available");
        if (!ctx.userAgent) throw new ZaloApiError("User agent is not available");

        this.wsURL = makeURL(this.ctx, this.urls[0], {
            t: Date.now(),
        });
        this.retryCount = {};
        this.rotateCount = 0;

        for (const retry in ctx.settings.features.socket.retries) {
            const { times, max } = ctx.settings.features.socket.retries[retry];
            this.retryCount[retry] = {
                count: 0,
                max: max || 0,
                times: typeof times === "number" ? [times] : times,
            };
        }

        this.cookie = ctx.cookie.getCookieStringSync("https://chat.zalo.me");
        this.userAgent = ctx.userAgent;

        this.selfListen = ctx.options.selfListen;

        this.ws = null;

        this.onConnectedCallback = () => {};
        this.onClosedCallback = () => {};
        this.onErrorCallback = () => {};
        this.onMessageCallback = () => {};

        this.tasks = { ...DEFAULT_BACKGROUND_TASK_OPTIONS, ...ctx.options.backgroundTasks };
        const store = this.tasks.queueCursorStore ?? new MemoryQueueCursorStore();
        const socketSettings = () => ctx.settings.features.socket;
        const onError = (error: unknown) => this.emit("error", error);

        this.heartbeat = new SocketHeartbeat({
            interval: () => socketSettings().ping_interval,
            pongTimeout: SOCKET_TIMING_DEFAULTS.pongTimeout,
            maxSilence: () => socketSettings().ping_max_timeout ?? SOCKET_TIMING_DEFAULTS.maxSilence,
            pongRetryDelays: this.retryDelaysOf("5013"),
            send: (eventId) =>
                this.sendWs(
                    { version: 1, cmd: SocketCommand.Ping, subCmd: PingSubCommand.KeepConnection, data: { eventId } },
                    false,
                ),
            onPingTimeout: () => this.closeInternally(CloseReason.PingTimeout),
            onSilence: () => this.closeInternally(CloseReason.AbnormalStopPing),
        });

        this.offlineSync = new OfflineQueueSync({
            store,
            timeout: () => socketSettings().offline_timeout ?? SOCKET_TIMING_DEFAULTS.offlineTimeout,
            nextRequestId: () => this.nextRequestId(),
            send: (request) => this.sendWs({ version: 1, ...request }, false),
            onDone: (queueKeys) => {
                this.verifier.start();
                this.emit("offline_done", queueKeys);
            },
            onTimeout: () => this.closeInternally(CloseReason.OfflineTimeout),
            onError,
        });

        this.missingMessages = new MissingMessageFetcher({
            store,
            batchDelay: () => socketSettings().delay_get_msg ?? SOCKET_TIMING_DEFAULTS.getMessageDelay,
            batchSize: () => socketSettings().max_msg_get ?? SOCKET_TIMING_DEFAULTS.getMessageBatch,
            retryDelay: SOCKET_TIMING_DEFAULTS.getMessageRetryDelay,
            maxRetries: SOCKET_TIMING_DEFAULTS.getMessageMaxRetries,
            nextRequestId: () => this.nextRequestId(),
            send: (request) => this.sendWs({ version: 1, ...request }, false),
            onError,
        });

        this.verifier = new MessageVerifier({
            interval: () => socketSettings().interval_verify || SOCKET_TIMING_DEFAULTS.verifyInterval,
            batchSize: () => socketSettings().max_msg_verify || SOCKET_TIMING_DEFAULTS.verifyBatch,
            retryMissDelay: () => socketSettings().timeout_retry_miss || SOCKET_TIMING_DEFAULTS.verifyRetryMiss,
            nextRequestId: () => this.nextRequestId(),
            send: (request) => this.sendWs({ version: 1, ...request }, false),
        });

        this.serverInfoTask = new PeriodicTask(
            () => SOCKET_TIMING_DEFAULTS.serverInfoInterval,
            () => this.refreshServerInfo(),
            onError,
        );

        this.keepAliveTask = new PeriodicTask(
            () =>
                (Number(ctx.settings.keepalive?.keepalive_duration) || SOCKET_TIMING_DEFAULTS.keepAliveSeconds) * 1000,
            () => this.keepAlive(),
            onError,
        );

        this.activeTask = new PeriodicTask(
            () => Number(ctx.settings.online_configs?.update_action_interval) || SOCKET_TIMING_DEFAULTS.activeInterval,
            () => this.sendActiveStatus(ActiveStatus.KeepActiveForeground),
            onError,
        );
    }

    /**
     * @deprecated Use `on` method instead
     */
    public onConnected(cb: () => unknown) {
        this.onConnectedCallback = cb;
    }

    /**
     * @deprecated Use `on` method instead
     */
    public onClosed(cb: OnClosedCallback) {
        this.onClosedCallback = cb;
    }

    /**
     * @deprecated Use `on` method instead
     */
    public onError(cb: OnErrorCallback) {
        this.onErrorCallback = cb;
    }

    /**
     * @deprecated Use `on` method instead
     */
    public onMessage(cb: OnMessageCallback) {
        this.onMessageCallback = cb;
    }

    private canRetry(code: CloseReason) {
        const retryable =
            this.ctx.settings.features.socket.close_and_retry_codes.includes(code) || INTERNAL_CLOSE_REASONS.has(code);
        if (!retryable || !this.retryCount[code.toString()]) return false;
        if (this.retryCount[code.toString()].count >= this.retryCount[code.toString()].max) return false;
        this.retryCount[code.toString()].count++;

        const { count, max, times } = this.retryCount[code.toString()];
        const retryTime = count - 1 < times.length ? times[count - 1] : times[times.length - 1];
        logger(this.ctx).verbose(`Retry for code ${code} in ${retryTime}ms (${count}/${max})`);

        return retryTime;
    }

    private shouldRotate(code: CloseReason) {
        if (!this.ctx.settings.features.socket.rotate_error_codes.includes(code)) return false;
        if (this.rotateCount >= this.urls.length - 1) return false;

        return true;
    }

    private rotateEndpoint() {
        this.rotateCount++;
        this.wsURL = makeURL(this.ctx, this.urls[this.rotateCount], {
            t: Date.now(),
        });
        logger(this.ctx).verbose(`Rotating endpoint to ${this.wsURL}`);
    }

    public start({ retryOnClose = false }: { retryOnClose?: boolean } = {}) {
        if (this.ws) throw new ZaloApiError("Already started");

        const ws = new WebSocket(this.wsURL, {
            headers: {
                "accept-encoding": "gzip, deflate, br, zstd",
                "accept-language": "en-US,en;q=0.9",
                "cache-control": "no-cache",
                connection: "Upgrade",
                host: new URL(this.wsURL).host,
                origin: "https://chat.zalo.me",
                prgama: "no-cache",
                "sec-websocket-extensions": "permessage-deflate; client_max_window_bits",
                "sec-websocket-version": "13",
                upgrade: "websocket",
                "user-agent": this.userAgent,
                cookie: this.cookie,
            },
            agent: this.ctx.options.agent,
        });
        this.ws = ws;
        const socketSettings = this.ctx.settings.features.socket;
        this.establishTimer = setTimeout(
            () => this.closeInternally(CloseReason.EstablishTimeout),
            socketSettings.establish_timeout || SOCKET_TIMING_DEFAULTS.establishTimeout,
        );

        ws.onopen = () => {
            clearTimeout(this.establishTimer);
            this.authTimer = setTimeout(
                () => this.closeInternally(CloseReason.AuthTimeout),
                socketSettings.timeout_authen || SOCKET_TIMING_DEFAULTS.authTimeout,
            );
            this.onConnectedCallback();
            this.emit("connected");
        };

        ws.onclose = (event) => {
            const code = (this.internalCloseReason ?? event.code) as CloseReason;
            const reason = this.internalCloseReason ? CloseReason[this.internalCloseReason] : event.reason;
            this.reset();
            this.emit("disconnected", code, reason);
            const retry = retryOnClose && this.canRetry(code);
            if (retry && retryOnClose) {
                const shouldRotate = this.shouldRotate(code);
                if (shouldRotate) {
                    this.rotateEndpoint();
                }
                setTimeout(() => {
                    this.start({ retryOnClose: true });
                }, retry);
            } else {
                this.onClosedCallback(code, reason);
                this.emit("closed", code, reason);
            }
        };

        ws.onerror = (event) => {
            this.onErrorCallback(event);
            this.emit("error", event);
        };

        ws.onmessage = async (event) => {
            const { data } = event;
            if (!(data instanceof Buffer)) return;

            const encodedHeader = data.subarray(0, 4);
            const [version, cmd, subCmd] = getHeader(encodedHeader);

            try {
                const dataToDecode = data.subarray(4);
                const decodedData = new TextDecoder("utf-8").decode(dataToDecode);
                if (decodedData.length == 0) return;

                const parsed = JSON.parse(decodedData);

                if (version == 1 && cmd == SocketCommand.Authenticate && subCmd == 1 && hasOwn(parsed, "key")) {
                    this.cipherKey = parsed.key;
                    this.emit("cipher_key", parsed.key);
                    await this.onAuthenticated(parsed);
                }

                if (cmd == SocketCommand.Ping) {
                    this.heartbeat.receivePong(parsed.eventId);
                    return;
                }

                if (this.offlineSync.owns(cmd, subCmd)) {
                    const parsedData = (await decodeEventData(parsed, this.cipherKey)).data as OfflineQueueData &
                        Record<string, unknown>;
                    this.emitQueueData(parsedData);
                    await this.offlineSync.receive(cmd, subCmd, parsedData);
                    return;
                }

                if (cmd == SocketCommand.VerifyMessage) {
                    const result = (
                        typeof parsed.data == "string" ? (await decodeEventData(parsed, this.cipherKey)).data : parsed
                    ) as VerifyMessageResult;
                    this.verifier.receive(result);
                    return;
                }

                if (cmd == SocketCommand.HasMessage) {
                    const parsedData = (await decodeEventData(parsed, this.cipherKey)).data as HasMessageData;
                    if (this.tasks.fetchMissingMessages) this.missingMessages.announce(parsedData);
                    return;
                }

                if (
                    (version == 1 && [501, 551, SocketCommand.PushGroupMessage].includes(cmd) && subCmd == 0) ||
                    cmd == SocketCommand.GetMessage ||
                    cmd == SocketCommand.PushMissedMessage
                ) {
                    const parsedData = (await decodeEventData(parsed, this.cipherKey)).data;
                    await this.offlineSync.track(parsedData.queueStatus);
                    if (cmd == SocketCommand.PushMissedMessage) this.verifier.receiveMissed(parsedData.reqId);
                    else this.verifier.observe(parsedData.queueStatus);
                    this.emitQueueData(parsedData);
                }

                if (version == 1 && cmd == 601 && subCmd == 0) {
                    const parsedData = (await decodeEventData(parsed, this.cipherKey)).data;
                    await this.offlineSync.track(parsedData.queueStatus);
                    this.emitControls(parsedData.controls);
                }

                if (cmd == 612) {
                    const parsedData = (await decodeEventData(parsed, this.cipherKey)).data;
                    await this.offlineSync.track(parsedData.queueStatus);
                    this.emitReactions(parsedData.reacts, parsedData.reactGroups);
                }

                if (cmd == 610 || cmd == 611) {
                    const parsedData = (await decodeEventData(parsed, this.cipherKey)).data;
                    const isGroup = cmd == 611;

                    const reacts = parsedData[isGroup ? "reactGroups" : "reacts"] as TReaction[];
                    const reactionObjects = reacts.map((react) => new Reaction(this.ctx.uid, react, isGroup));

                    this.emit("old_reactions", reactionObjects, isGroup);
                }

                if (cmd == 510 && subCmd == 1) {
                    const parsedData = (await decodeEventData(parsed, this.cipherKey)).data;
                    const msgs = parsedData.msgs as TMessage[];
                    const responseMsgs = msgs.map((msg) => new UserMessage(this.ctx.uid, msg));
                    this.emit("old_messages", responseMsgs, ThreadType.User);
                }

                if (cmd == 511 && subCmd == 1) {
                    const parsedData = (await decodeEventData(parsed, this.cipherKey)).data;
                    const groupMsgs = parsedData.groupMsgs as TGroupMessage[];
                    const responseMsgs = groupMsgs.map((msg) => new GroupMessage(this.ctx.uid, msg));
                    this.emit("old_messages", responseMsgs, ThreadType.Group);
                }

                if (cmd == 602 && subCmd == 0) {
                    const parsedData = (await decodeEventData(parsed, this.cipherKey)).data;
                    const { actions } = parsedData;

                    for (const action of actions) {
                        if (action.act_type == "typing") {
                            const data = JSON.parse(`{${action.data}}`);
                            if (action.act == "typing") {
                                const typingObject = new UserTyping(data);
                                this.emit("typing", typingObject);
                            } else if (action.act == "gtyping") {
                                // 26/02/2025
                                // For a group with only two people, Zalo doesn't send a typing event.

                                const typingObject = new GroupTyping(data);
                                this.emit("typing", typingObject);
                            }
                        }
                    }
                }

                if (cmd == 502 && subCmd == 0) {
                    const parsedData = (await decodeEventData(parsed, this.cipherKey)).data;
                    const { delivereds: deliveredMsgs, seens: seenMsgs } = parsedData;

                    if (Array.isArray(deliveredMsgs) && deliveredMsgs.length > 0) {
                        const deliveredObjects = deliveredMsgs.map((delivered) => new UserDeliveredMessage(delivered));
                        this.emit("delivered_messages", deliveredObjects);
                    }

                    if (Array.isArray(seenMsgs) && seenMsgs.length > 0) {
                        const seenObjects = seenMsgs.map((seen) => new UserSeenMessage(seen));
                        this.emit("seen_messages", seenObjects);
                    }
                }

                if (cmd == 522 && subCmd == 0) {
                    const parsedData = (await decodeEventData(parsed, this.cipherKey)).data;
                    const { delivereds: deliveredMsgs, groupSeens: groupSeenMsgs } = parsedData;

                    if (Array.isArray(deliveredMsgs) && deliveredMsgs.length > 0) {
                        let deliveredObjects = deliveredMsgs.map(
                            (delivered) => new GroupDeliveredMessage(this.ctx.uid, delivered),
                        );
                        if (!this.selfListen)
                            deliveredObjects = deliveredObjects.filter((delivered) => !delivered.isSelf);
                        this.emit("delivered_messages", deliveredObjects);
                    }

                    if (Array.isArray(groupSeenMsgs) && groupSeenMsgs.length > 0) {
                        let seenObjects = groupSeenMsgs.map((seen) => new GroupSeenMessage(this.ctx.uid, seen));
                        if (!this.selfListen) seenObjects = seenObjects.filter((seen) => !seen.isSelf);
                        this.emit("seen_messages", seenObjects);
                    }
                }

                if (version == 1 && cmd == 3000 && subCmd == 0) {
                    logger(this.ctx).error();
                    logger(this.ctx).error("Another connection is opened, closing this one");
                    logger(this.ctx).error();
                    if (ws.readyState !== WebSocket.CLOSED) ws.close(CloseReason.DuplicateConnection);
                }
            } catch (error) {
                this.onErrorCallback(error);
                this.emit("error", error);
            }
        };
    }

    private emitQueueData(data: Record<string, unknown> | null | undefined) {
        if (!data) return;
        const msgs = Array.isArray(data.msgs) ? (data.msgs as TMessage[]) : [];
        const groupMsgs = Array.isArray(data.groupMsgs) ? (data.groupMsgs as TGroupMessage[]) : [];
        if (this.tasks.fetchMissingMessages) {
            this.missingMessages.received([...msgs, ...groupMsgs].map((msg) => msg.msgId));
        }
        msgs.forEach((msg) => this.emitMessage(msg, false));
        groupMsgs.forEach((msg) => this.emitMessage(msg, true));
        if (Array.isArray(data.controls)) this.emitControls(data.controls);
        if (Array.isArray(data.reacts) || Array.isArray(data.reactGroups)) {
            this.emitReactions(data.reacts as TReaction[] | undefined, data.reactGroups as TReaction[] | undefined);
        }
    }

    private emitMessage(msg: TMessage | TGroupMessage, isGroup: boolean) {
        if (typeof msg.content == "object" && hasOwn(msg.content, "deleteMsg")) {
            const undoObject = new Undo(this.ctx.uid, msg as unknown as TUndo, isGroup);
            if (undoObject.isSelf && !this.selfListen) return;
            this.emit("undo", undoObject);
            return;
        }
        const messageObject = isGroup
            ? new GroupMessage(this.ctx.uid, msg as TGroupMessage)
            : new UserMessage(this.ctx.uid, msg as TMessage);
        if (messageObject.isSelf && !this.selfListen) return;
        this.onMessageCallback(messageObject);
        this.emit("message", messageObject);
    }

    private emitControls(controls: EventData[] | undefined) {
        (controls ?? []).forEach((control) => this.emitControl(control));
    }

    private emitControl(control: EventData) {
        if (control.content.act_type == "file_done") {
            const data = {
                fileUrl: control.content.data.url,
                fileId: control.content.fileId,
            };

            const uploadCallback = this.ctx.uploadCallbacks.get(String(control.content.fileId));
            if (uploadCallback) uploadCallback(data);
            this.ctx.uploadCallbacks.delete(String(control.content.fileId));

            this.emit("upload_attachment", data);
        } else if (control.content.act_type == "group") {
            // 31/08/2024
            // for some reason, Zalo send both join and join_reject event when admin approve join requests
            // Zalo itself doesn't seem to handle this properly either, so we gonna ignore the join_reject event

            if (control.content.act == "join_reject") return;

            const groupEventData: TGroupEvent =
                typeof control.content.data == "string" ? JSON.parse(control.content.data) : control.content.data;

            const groupEvent = initializeGroupEvent(
                this.ctx.uid,
                groupEventData,
                getGroupEventType(control.content.act),
                control.content.act,
            );
            if (groupEvent.isSelf && !this.selfListen) return;
            this.emit("group_event", groupEvent);
        } else if (control.content.act_type == "fr") {
            // 28/02/2025
            // Zalo send both req and req_v2 event when user send friend request
            // Zalo itself doesn't seem to handle this properly either, so we gonna ignore the req event

            if (control.content.act == "req") return;

            const friendEventData: TFriendEvent =
                typeof control.content.data == "string" ? JSON.parse(control.content.data) : control.content.data;

            // Handles the case when act is "pin_create" and params is a string
            if (
                typeof friendEventData == "object" &&
                "topic" in friendEventData &&
                typeof friendEventData.topic == "object" &&
                "params" in friendEventData.topic
            ) {
                friendEventData.topic.params = JSON.parse(`${friendEventData.topic.params}`);
            }

            const friendEvent = initializeFriendEvent(
                this.ctx.uid,
                typeof friendEventData == "number" ? control.content.data : friendEventData,
                getFriendEventType(control.content.act),
            );
            if (friendEvent.isSelf && !this.selfListen) return;
            this.emit("friend_event", friendEvent);
        }
    }

    private emitReactions(reacts: TReaction[] | undefined, reactGroups: TReaction[] | undefined) {
        for (const react of reacts ?? []) this.emitReaction(react, false);
        for (const reactGroup of reactGroups ?? []) this.emitReaction(reactGroup, true);
    }

    private emitReaction(react: TReaction, isGroup: boolean) {
        if (typeof react.content == "string") react.content = JSON.parse(react.content);
        const reactionObject = new Reaction(this.ctx.uid, react, isGroup);
        if (reactionObject.isSelf && !this.selfListen) return;
        this.emit("reaction", reactionObject);
    }

    private async onAuthenticated(parsed: Record<string, unknown>) {
        clearTimeout(this.authTimer);
        this.resetRetryCounts();
        this.heartbeat.start();
        if (this.tasks.keepAlive && this.keepAliveCall) this.keepAliveTask.start();
        if (this.tasks.refreshServerInfo) this.serverInfoTask.start();
        if (this.tasks.activeStatus) {
            this.activeTask.start();
            this.sendActiveStatus(ActiveStatus.FirstActiveForeground);
        }
        const authData = typeof parsed.data == "string" ? await decodeEventData(parsed, this.cipherKey) : null;
        const queuesOf = (value: unknown) => (Array.isArray(value) ? (value as QueueCommand[]) : []);
        const messageQueues = queuesOf(authData?.data?.qCmds);
        const controlQueues = queuesOf(authData?.data?.ctrl_qCmds);
        if (this.tasks.verifyMessages) {
            this.verifier.init(messageQueues.filter((queue) => MESSAGE_QUEUE_COMMANDS.has(queue.cmd)).map(queueKeyOf));
        }
        if (this.tasks.syncOffline) await this.offlineSync.begin([...messageQueues, ...controlQueues]);
        if (!this.offlineSync.pulling) this.verifier.start();
    }

    private async refreshServerInfo() {
        const serverInfo = await getServerInfo(this.ctx, true);
        const settings = serverInfo?.setttings || serverInfo?.settings;
        if (!settings) return;
        this.ctx.settings = settings;
        this.emit("settings_updated", settings);
    }

    private async keepAlive() {
        if (!this.keepAliveCall) return;
        const response = await this.keepAliveCall();
        const version = Number(response?.config_vesion);
        if (Number.isNaN(version)) return;
        if (this.configVersion !== -1 && version > this.configVersion) {
            this.emit("config_version", version);
            if (this.tasks.refreshServerInfo) await this.refreshServerInfo();
        }
        if (version > this.configVersion) this.configVersion = version;
    }

    public sendActiveStatus(status: ActiveStatus) {
        this.sendWs({ version: 1, cmd: SocketCommand.PingActive, subCmd: 0, data: { at: status } }, false);
    }

    private closeInternally(reason: CloseReason) {
        if (!this.ws) return;
        logger(this.ctx).verbose(`Closing socket internally: ${CloseReason[reason]}`);
        this.internalCloseReason = reason;
        this.ws.terminate();
    }

    private resetRetryCounts() {
        Object.values(this.retryCount).forEach((retry) => (retry.count = 0));
    }

    private retryDelaysOf(code: string) {
        const retry = this.ctx.settings.features.socket.retries[code];
        if (!retry) return [];
        const times = typeof retry.times === "number" ? [retry.times] : retry.times;
        const max = retry.max ?? times.length;
        return Array.from({ length: max }, (_, index) => times[Math.min(index, times.length - 1)]);
    }

    private nextRequestId() {
        return `req_${this.id++}`;
    }

    public stop() {
        if (this.ws) {
            this.ws.close(CloseReason.ManualClosure);
            this.reset();
        }
    }

    public sendWs(payload: WsPayload, requireId: boolean = true) {
        if (this.ws) {
            if (requireId) payload.data["req_id"] = `req_${this.id++}`;

            const encodedData = new TextEncoder().encode(JSON.stringify(payload.data));
            const dataLength = encodedData.length;

            const data = new DataView(Buffer.alloc(4 + dataLength).buffer);
            data.setUint8(0, payload.version);
            data.setInt32(1, payload.cmd, true);
            data.setInt8(3, payload.subCmd);

            encodedData.forEach((e, i) => {
                data.setUint8(4 + i, e);
            });

            this.ws.send(data);
        }
    }

    /**
     * Request old messages
     *
     * @param lastMsgId
     */
    public requestOldMessages(threadType: ThreadType, lastMsgId: string | null = null) {
        const payload = {
            version: 1,
            cmd: threadType === ThreadType.User ? 510 : 511,
            subCmd: 1,
            data: { first: true, lastId: lastMsgId, preIds: [] },
        };
        this.sendWs(payload);
    }

    /**
     * Request old messages
     *
     * @param lastMsgId
     */
    public requestOldReactions(threadType: ThreadType, lastMsgId: string | null = null) {
        const payload = {
            version: 1,
            cmd: threadType === ThreadType.User ? 610 : 611,
            subCmd: 1,
            data: { first: true, lastId: lastMsgId, preIds: [] },
        };
        this.sendWs(payload);
    }

    private reset() {
        this.ws = null;
        this.cipherKey = undefined;
        this.internalCloseReason = undefined;
        clearTimeout(this.establishTimer);
        clearTimeout(this.authTimer);
        this.heartbeat.stop();
        this.offlineSync.stop();
        this.missingMessages.stop();
        this.verifier.stop();
        this.serverInfoTask.stop();
        this.keepAliveTask.stop();
        this.activeTask.stop();
    }
}

function getHeader(buffer: Buffer) {
    if (buffer.byteLength < 4) {
        throw new ZaloApiError("Invalid header");
    }

    return [buffer[0], buffer.readUInt16LE(1), buffer[3]];
}
