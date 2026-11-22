import { afterEach, describe, expect, it, mock } from "bun:test";
import { once } from "node:events";
import toughCookie from "tough-cookie";
import { CloseReason, Listener } from "../src/apis/listen.js";
import { MemoryQueueCursorStore } from "../src/background/queueCursorStore.js";
import { createContext, type ContextSession } from "../src/context.js";
import type { Message } from "../src/models/index.js";

type Frame = { cmd: number; subCmd: number; data: Record<string, unknown> };

const wait = (milliseconds: number) => new Promise((resolve) => setTimeout(resolve, milliseconds));

function encodeFrame(cmd: number, subCmd: number, body: Record<string, unknown>) {
    const json = Buffer.from(JSON.stringify(body));
    const header = Buffer.alloc(4);
    header.writeUInt8(1, 0);
    header.writeUInt16LE(cmd, 1);
    header.writeUInt8(subCmd, 3);
    return Buffer.concat([header, json]);
}

function decodeFrame(raw: Buffer): Frame {
    return { cmd: raw.readUInt16LE(1), subCmd: raw.readUInt8(3), data: JSON.parse(raw.subarray(4).toString()) };
}

class TestSocket {
    static readonly CLOSED = 3;
    static current: TestSocket;
    readyState = 1;
    onopen?: () => void;
    onclose?: (event: { code: number; reason: string }) => void;
    onmessage?: (event: { data: Buffer }) => Promise<void>;
    onFrame?: (socket: TestSocket, frame: Frame) => void;

    constructor() {
        TestSocket.current = this;
        queueMicrotask(() => this.onopen?.());
    }

    send(data: DataView) {
        const frame = decodeFrame(Buffer.from(data.buffer, data.byteOffset, data.byteLength));
        queueMicrotask(() => this.onFrame?.(this, frame));
    }

    receive(data: Buffer) {
        queueMicrotask(() => void this.onmessage?.({ data }));
    }

    close(code: number) {
        this.readyState = TestSocket.CLOSED;
        queueMicrotask(() => this.onclose?.({ code, reason: "" }));
    }

    terminate() {
        this.close(1006);
    }
}

mock.module("ws", () => ({ default: TestSocket }));

const plain = (data: unknown) => ({ error_code: 0, encrypt: 0, data: JSON.stringify({ data }) });

function contextFor(store: MemoryQueueCursorStore, retries: Record<string, { times: number[]; max: number }> = {}) {
    const ctx = createContext() as ContextSession;
    ctx.uid = "999";
    ctx.secretKey = "secret";
    ctx.userAgent = "test-agent";
    ctx.cookie = new toughCookie.CookieJar();
    ctx.options.logging = false;
    ctx.options.backgroundTasks = { syncOffline: true, queueCursorStore: store };
    ctx.settings = {
        features: {
            sharefile: {} as ContextSession["settings"]["features"]["sharefile"],
            socket: {
                ping_interval: 60_000,
                retries,
                close_and_retry_codes: [],
                rotate_error_codes: [],
                timeout_authen: 1_000,
                establish_timeout: 1_000,
                delay_get_msg: 5,
            } as unknown as ContextSession["settings"]["features"]["socket"],
        },
        keepalive: { alway_keepalive: 1, keepalive_duration: 120, time_deactive: 300 },
    };
    return ctx;
}

const userMessage = (msgId: string) => ({
    actionId: msgId,
    msgId,
    cliMsgId: msgId,
    msgType: "webchat",
    uidFrom: "123",
    idTo: "0",
    dName: "Customer",
    ts: "1700000000000",
    status: 1,
    content: `hello ${msgId}`,
    notify: "1",
    ttl: 0,
    userId: "0",
    uin: "0",
    topOut: "0",
    topOutTimeOut: "0",
    topOutImprTimeOut: "0",
    propertyExt: { color: 0, size: 0, type: 0, subType: 0, ext: "{}" },
    paramsExt: { countUnread: 1, containType: 0, platformType: 1 },
    cmd: 501,
    st: 3,
    at: 5,
    realMsgId: "0",
});

describe("Listener background tasks", () => {
    let listener: Listener | null = null;

    afterEach(() => {
        listener?.stop();
        listener = null;
    });

    function connect(
        store: MemoryQueueCursorStore,
        onFrame: (socket: TestSocket, frame: Frame) => void,
        beforeStart?: (context: ContextSession) => void,
    ) {
        const context = contextFor(store);
        listener = new Listener(context, ["ws://localhost/?zpw_ver=1"]);
        beforeStart?.(context);
        listener.start();
        const socket = TestSocket.current;
        socket.onFrame = onFrame;
        return socket;
    }

    it("uses a ping interval changed after construction and a silence limit changed while connected", async () => {
        let context!: ContextSession;
        let pings = 0;
        const socket = connect(
            new MemoryQueueCursorStore(),
            (serverSocket, frame) => {
                if (frame.cmd !== 2) return;
                pings++;
                if (pings === 2) {
                    context.settings = {
                        ...context.settings,
                        features: {
                            ...context.settings.features,
                            socket: { ...context.settings.features.socket, ping_max_timeout: 0 },
                        },
                    };
                }
                serverSocket.receive(encodeFrame(2, 1, { error_code: 0, eventId: frame.data.eventId }));
            },
            (current) => {
                context = current;
                current.settings.features.socket.ping_interval = 60;
            },
        );
        const closed = once(listener!, "closed", { signal: AbortSignal.timeout(1_500) });
        socket.receive(encodeFrame(1, 1, { key: "irrelevant" }));

        expect(await closed).toEqual([CloseReason.AbnormalStopPing, "AbnormalStopPing"]);
        expect(pings).toBe(2);
    });

    it("resumes offline queues and emits their data through the live events", async () => {
        const store = new MemoryQueueCursorStore();
        store.set("510_1", "100");
        store.set("603_1", "300");
        store.set("610_0", "400");
        const received: Frame[] = [];
        const messages: Message[] = [];
        const offlineEvents: string[] = [];
        const socket = connect(store, (serverSocket, frame) => {
            received.push(frame);
            if (frame.cmd === 2)
                serverSocket.receive(encodeFrame(2, 1, { error_code: 0, eventId: frame.data.eventId }));
            if (frame.cmd === 603) {
                serverSocket.receive(
                    encodeFrame(
                        603,
                        1,
                        plain({
                            controls: [
                                { content: { act_type: "group", act: "update", data: { groupId: "234" } } },
                                { content: { act_type: "fr", act: "add", data: "345" } },
                            ],
                            more: 0,
                            lastActionId: "301",
                            queueStatus: { "603_1": { lastId: "301" } },
                        }),
                    ),
                );
            }
            if (frame.cmd === 610) {
                serverSocket.receive(
                    encodeFrame(
                        610,
                        0,
                        plain({
                            reacts: [{ ...userMessage("401"), content: { rIcon: "like", rType: 1 } }],
                            more: 0,
                            queueStatus: { "610_0": { lastId: "401" } },
                        }),
                    ),
                );
            }
            if (frame.cmd === 510) {
                const last = frame.data.first;
                serverSocket.receive(
                    encodeFrame(
                        510,
                        1,
                        plain(
                            last
                                ? {
                                      msgs: [userMessage("101")],
                                      more: 1,
                                      lastActionId: "101",
                                      queueStatus: { "510_1": { lastId: "101" } },
                                  }
                                : {
                                      msgs: [
                                          userMessage("102"),
                                          { ...userMessage("103"), content: { deleteMsg: true, globalMsgId: "102" } },
                                      ],
                                      more: 0,
                                      lastActionId: "102",
                                      queueStatus: { "510_1": { lastId: "102" } },
                                  },
                        ),
                    ),
                );
            }
        });
        listener!.on("message", (message) => messages.push(message));
        listener!.on("undo", (undo) => offlineEvents.push(`undo:${undo.data.msgId}`));
        listener!.on("reaction", (reaction) => offlineEvents.push(`reaction:${reaction.data.msgId}`));
        listener!.on("group_event", (event) => offlineEvents.push(`group_event:${event.threadId}`));
        listener!.on("friend_event", (event) => offlineEvents.push(`friend_event:${event.threadId}`));
        const done = once(listener!, "offline_done", { signal: AbortSignal.timeout(1_500) });

        socket.receive(
            encodeFrame(1, 1, {
                key: "irrelevant",
                ...plain({
                    qCmds: [
                        { cmd: 515, subCmd: 0, queueName: "515_0" },
                        { cmd: 510, subCmd: 1, queueName: "510_1" },
                    ],
                    ctrl_qCmds: [
                        { cmd: 603, subCmd: 1, queueName: "603_1" },
                        { cmd: 610, subCmd: 0, queueName: "610_0" },
                    ],
                    bk_cmds: { "510_1": "510_0" },
                }),
            }),
        );
        expect(await done).toEqual([["510_1", "603_1", "610_0"]]);

        expect(received.find((frame) => frame.cmd === 2)?.subCmd).toBe(1);
        const pulls = received.filter((frame) => frame.cmd === 510);
        expect(pulls.map((frame) => [frame.data.first, frame.data.lastId])).toEqual([
            [true, "100"],
            [false, "101"],
        ]);
        expect(received.some((frame) => frame.cmd === 515)).toBe(false);
        expect(messages.map((message) => message.data.msgId)).toEqual(["101", "102"]);
        expect(received.find((frame) => frame.cmd === 603)?.data).toMatchObject({ first: true, lastId: "300" });
        expect(offlineEvents.sort()).toEqual(["friend_event:345", "group_event:234", "reaction:401", "undo:103"]);
        expect(store.get("510_1")).toBe("102");
        expect(store.get("603_1")).toBe("301");
    });

    it("fetches announced missing messages with GET_MSG and tracks live queue status", async () => {
        const store = new MemoryQueueCursorStore();
        const received: Frame[] = [];
        const messages: Message[] = [];
        const socket = connect(store, (serverSocket, frame) => {
            received.push(frame);
            if (frame.cmd === 2)
                serverSocket.receive(encodeFrame(2, 1, { error_code: 0, eventId: frame.data.eventId }));
            if (frame.cmd === 570) serverSocket.receive(encodeFrame(570, 0, plain({ msgs: [userMessage("201")] })));
        });
        listener!.on("message", (message) => messages.push(message));

        socket.receive(encodeFrame(1, 1, { key: "irrelevant" }));
        socket.receive(
            encodeFrame(501, 0, plain({ msgs: [userMessage("200")], queueStatus: { "501_0": { lastId: "200" } } })),
        );
        socket.receive(
            encodeFrame(533, 0, plain({ queueId: { cmd: 501, subCmd: 0 }, ids: [{ actionId: "201", msgId: "201" }] })),
        );
        await wait(150);

        const getMessage = received.find((frame) => frame.cmd === 570);
        expect(getMessage?.data).toMatchObject({ queueId: { cmd: 501, subCmd: 0 }, lastId: "200" });
        expect(messages.map((message) => message.data.msgId)).toEqual(["200", "201"]);
        expect(store.get("501_0")).toBe("200");
    });

    it("closes with an internal reason when the server never authenticates", async () => {
        const closed: number[] = [];
        connect(new MemoryQueueCursorStore(), () => {});
        listener!.on("closed", (code) => closed.push(code));
        await wait(1_300);
        expect(closed).toEqual([CloseReason.AuthTimeout]);
    });
});
