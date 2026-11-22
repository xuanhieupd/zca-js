import { afterEach, describe, expect, it } from "bun:test";
import { WebSocketServer, type WebSocket as ServerSocket } from "ws";
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
    let server: WebSocketServer | null = null;
    let listener: Listener | null = null;

    afterEach(() => {
        listener?.stop();
        server?.close();
        server = null;
        listener = null;
    });

    async function connect(store: MemoryQueueCursorStore, onFrame: (socket: ServerSocket, frame: Frame) => void) {
        server = new WebSocketServer({ port: 0 });
        const port = (server.address() as { port: number }).port;
        const connected = new Promise<ServerSocket>((resolve) => {
            server!.on("connection", (socket) => {
                socket.on("message", (raw: Buffer) => onFrame(socket, decodeFrame(raw)));
                resolve(socket);
            });
        });
        listener = new Listener(contextFor(store), [`ws://127.0.0.1:${port}/?zpw_ver=1`]);
        listener.start();
        return connected;
    }

    it("answers pings, resumes offline queues from the cursor and emits the pulled messages", async () => {
        const store = new MemoryQueueCursorStore();
        store.set("510_1", "100");
        store.set("603_1", "300");
        const received: Frame[] = [];
        const messages: Message[] = [];
        const done: string[][] = [];
        const socket = await connect(store, (serverSocket, frame) => {
            received.push(frame);
            if (frame.cmd === 2) serverSocket.send(encodeFrame(2, 1, { error_code: 0, eventId: frame.data.eventId }));
            if (frame.cmd === 603) {
                serverSocket.send(
                    encodeFrame(
                        603,
                        1,
                        plain({
                            controls: [],
                            more: 0,
                            lastActionId: "301",
                            queueStatus: { "603_1": { lastId: "301" } },
                        }),
                    ),
                );
            }
            if (frame.cmd === 510) {
                const last = frame.data.first;
                serverSocket.send(
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
                                      msgs: [userMessage("102")],
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
        listener!.on("offline_done", (keys) => done.push(keys));

        socket.send(
            encodeFrame(1, 1, {
                key: "irrelevant",
                ...plain({
                    qCmds: [
                        { cmd: 515, subCmd: 0, queueName: "515_0" },
                        { cmd: 510, subCmd: 1, queueName: "510_1" },
                    ],
                    ctrl_qCmds: [{ cmd: 603, subCmd: 1, queueName: "603_1" }],
                    bk_cmds: { "510_1": "510_0" },
                }),
            }),
        );
        await wait(150);

        expect(received.find((frame) => frame.cmd === 2)?.subCmd).toBe(1);
        const pulls = received.filter((frame) => frame.cmd === 510);
        expect(pulls.map((frame) => [frame.data.first, frame.data.lastId])).toEqual([
            [true, "100"],
            [false, "101"],
        ]);
        expect(received.some((frame) => frame.cmd === 515)).toBe(false);
        expect(messages.map((message) => message.data.msgId)).toEqual(["101", "102"]);
        expect(received.find((frame) => frame.cmd === 603)?.data).toMatchObject({ first: true, lastId: "300" });
        expect(done).toEqual([["510_1", "603_1"]]);
        expect(store.get("510_1")).toBe("102");
        expect(store.get("603_1")).toBe("301");
    });

    it("fetches announced missing messages with GET_MSG and tracks live queue status", async () => {
        const store = new MemoryQueueCursorStore();
        const received: Frame[] = [];
        const messages: Message[] = [];
        const socket = await connect(store, (serverSocket, frame) => {
            received.push(frame);
            if (frame.cmd === 2) serverSocket.send(encodeFrame(2, 1, { error_code: 0, eventId: frame.data.eventId }));
            if (frame.cmd === 570) serverSocket.send(encodeFrame(570, 0, plain({ msgs: [userMessage("201")] })));
        });
        listener!.on("message", (message) => messages.push(message));

        socket.send(encodeFrame(1, 1, { key: "irrelevant" }));
        socket.send(
            encodeFrame(501, 0, plain({ msgs: [userMessage("200")], queueStatus: { "501_0": { lastId: "200" } } })),
        );
        socket.send(
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
        await connect(new MemoryQueueCursorStore(), () => {});
        listener!.on("closed", (code) => closed.push(code));
        await wait(1_300);
        expect(closed).toEqual([CloseReason.AuthTimeout]);
    });
});
