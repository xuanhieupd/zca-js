import { describe, expect, it } from "bun:test";
import { SocketHeartbeat } from "../src/background/socketHeartbeat.js";
import { OfflineQueueSync, type OfflineQueueRequest } from "../src/background/offlineQueueSync.js";
import { MissingMessageFetcher, type GetMessageRequest } from "../src/background/missingMessageFetcher.js";
import { MemoryQueueCursorStore, isNewerActionId } from "../src/background/queueCursorStore.js";
import { MessageVerifier, type VerifyMessageRequest } from "../src/background/messageVerifier.js";

const wait = (milliseconds: number) => new Promise((resolve) => setTimeout(resolve, milliseconds));

describe("SocketHeartbeat", () => {
    const heartbeatWith = (overrides: Partial<ConstructorParameters<typeof SocketHeartbeat>[0]> = {}) => {
        const sent: number[] = [];
        const outcomes: string[] = [];
        const heartbeat = new SocketHeartbeat({
            interval: () => 1000,
            pongTimeout: 20,
            maxSilence: () => 10_000,
            pongRetryDelays: [5],
            send: (eventId) => sent.push(eventId),
            onPingTimeout: () => outcomes.push("timeout"),
            onSilence: () => outcomes.push("silence"),
            ...overrides,
        });
        return { heartbeat, sent, outcomes };
    };

    it("pings right away and stays healthy when the pong echoes the event id", async () => {
        const { heartbeat, sent, outcomes } = heartbeatWith();
        heartbeat.start();
        heartbeat.receivePong(sent[0]);
        await wait(40);
        heartbeat.stop();
        expect(sent.length).toBe(1);
        expect(outcomes).toEqual([]);
    });

    it("accepts a pong without an event id", async () => {
        const { heartbeat, outcomes } = heartbeatWith();
        heartbeat.start();
        heartbeat.receivePong(undefined);
        await wait(40);
        heartbeat.stop();
        expect(outcomes).toEqual([]);
    });

    it("retries the ping, then reports a timeout when no pong arrives", async () => {
        const { heartbeat, sent, outcomes } = heartbeatWith();
        heartbeat.start();
        await wait(80);
        heartbeat.stop();
        expect(sent.length).toBe(2);
        expect(outcomes).toEqual(["timeout"]);
    });

    it("ignores a pong for another ping", async () => {
        const { heartbeat, outcomes } = heartbeatWith({ pongRetryDelays: [] });
        heartbeat.start();
        heartbeat.receivePong(1);
        await wait(40);
        expect(outcomes).toEqual(["timeout"]);
    });

    it("reports silence when the last pong is older than the limit", async () => {
        const { heartbeat, sent, outcomes } = heartbeatWith({ maxSilence: () => 10 });
        heartbeat.start();
        heartbeat.receivePong(sent[0]);
        await wait(20);
        heartbeat.ping();
        expect(outcomes).toEqual(["silence"]);
    });
});

describe("OfflineQueueSync", () => {
    const syncWith = (store = new MemoryQueueCursorStore()) => {
        const sent: OfflineQueueRequest[] = [];
        const done: string[][] = [];
        const timeouts: number[] = [];
        let requestId = 0;
        const sync = new OfflineQueueSync({
            store,
            timeout: () => 50,
            nextRequestId: () => `req_${requestId++}`,
            send: (request) => sent.push(request),
            onDone: (keys) => done.push(keys),
            onTimeout: () => timeouts.push(1),
            onError: (error) => {
                throw error;
            },
        });
        return { sync, sent, done, timeouts, store };
    };

    it("skips queues it has no cursor for and unsupported queues", async () => {
        const { sync, sent } = syncWith();
        await sync.begin([
            { cmd: 510, subCmd: 1 },
            { cmd: 515, subCmd: 0 },
        ]);
        expect(sent).toEqual([]);
        expect(sync.pulling).toBe(false);
    });

    it("pulls each resumable queue from its cursor and follows pagination", async () => {
        const store = new MemoryQueueCursorStore();
        store.set("510_1", "100");
        store.set("511_1", "200");
        const { sync, sent, done } = syncWith(store);
        await sync.begin([
            { cmd: 510, subCmd: 1 },
            { cmd: 511, subCmd: 1 },
        ]);
        expect(sent.map((request) => [request.cmd, request.data.lastId, request.data.first])).toEqual([
            [510, "100", true],
            [511, "200", true],
        ]);

        await sync.receive(510, 1, { more: 1, lastActionId: "150", queueStatus: { "510_1": { lastId: "150" } } });
        expect(sent[2].data).toMatchObject({ first: false, lastId: "150" });
        expect(store.get("510_1")).toBe("150");

        await sync.receive(510, 1, { more: 0, lastActionId: "160", queueStatus: { "510_1": { lastId: "160" } } });
        expect(done).toEqual([]);
        await sync.receive(511, 1, { more: 0, queueStatus: { "511_1": { lastId: "210" } } });
        expect(done).toEqual([["510_1", "511_1"]]);
        expect(sync.owns(510, 1)).toBe(false);
        expect(store.get("511_1")).toBe("210");
    });

    it("never moves a cursor backwards", async () => {
        const store = new MemoryQueueCursorStore();
        store.set("510_1", "9007199254740993");
        const { sync } = syncWith(store);
        await sync.track({ "510_1": { lastId: "9007199254740992" }, "510_0": { lastId: "7" } });
        expect(store.get("510_1")).toBe("9007199254740993");
        expect(store.get("510_0")).toBe("7");
    });

    it("times out when the pull does not finish", async () => {
        const store = new MemoryQueueCursorStore();
        store.set("510_1", "1");
        const { sync, timeouts } = syncWith(store);
        await sync.begin([{ cmd: 510, subCmd: 1 }]);
        await wait(80);
        expect(timeouts).toEqual([1]);
        expect(sync.pulling).toBe(false);
    });
});

describe("MissingMessageFetcher", () => {
    it("batches announced ids per queue and retries ids that never arrive", async () => {
        const store = new MemoryQueueCursorStore();
        store.set("501_0", "42");
        const sent: GetMessageRequest[] = [];
        let requestId = 0;
        const fetcher = new MissingMessageFetcher({
            store,
            batchDelay: () => 5,
            batchSize: () => 2,
            retryDelay: 20,
            maxRetries: 1,
            nextRequestId: () => `req_${requestId++}`,
            send: (request) => sent.push(request),
            onError: (error) => {
                throw error;
            },
        });
        const queueId = { cmd: 501, subCmd: 0 };
        fetcher.announce({ queueId, ids: [{ actionId: "1", msgId: "a" }] });
        fetcher.announce({
            queueId,
            ids: [
                { actionId: "2", msgId: "b" },
                { actionId: "3", msgId: "c" },
            ],
        });
        await wait(15);
        expect(sent.map((request) => request.data.ids.map((id) => id.msgId))).toEqual([["a", "b"], ["c"]]);
        expect(sent.every((request) => request.cmd === 570 && request.data.lastId === "42")).toBe(true);

        fetcher.received(["a", "b"]);
        await wait(40);
        expect(sent.slice(2).map((request) => request.data.ids.map((id) => id.msgId))).toEqual([["c"]]);
        await wait(40);
        expect(sent.length).toBe(3);
        fetcher.stop();
    });
});

describe("isNewerActionId", () => {
    it("compares ids as big integers and rejects non numeric ids", () => {
        expect(isNewerActionId("10", "9")).toBe(true);
        expect(isNewerActionId("9", "10")).toBe(false);
        expect(isNewerActionId("5", null)).toBe(true);
        expect(isNewerActionId("abc", null)).toBe(false);
    });
});

describe("MessageVerifier", () => {
    const verifierWith = () => {
        const sent: VerifyMessageRequest[] = [];
        let requestId = 0;
        const verifier = new MessageVerifier({
            interval: () => 20,
            batchSize: () => 3,
            retryMissDelay: () => 20,
            nextRequestId: () => `req_${requestId++}`,
            send: (request) => sent.push(request),
        });
        verifier.init(["510_1"]);
        return { verifier, sent };
    };

    it("waits for the interval, verifies a batch and keeps the last verified id as the anchor", async () => {
        const { verifier, sent } = verifierWith();
        verifier.start();
        verifier.observe({ "510_1": { ids: ["11", "10"] } as never, "999_0": { ids: ["1", "2"] } as never });
        expect(sent).toEqual([]);
        await wait(30);
        expect(sent.map((request) => [request.cmd, request.data.queueName, request.data.ids])).toEqual([
            [532, "510_1", [{ actionId: "10" }, { actionId: "11" }]],
        ]);
        verifier.receive({ error: 0, reqId: sent[0].data.reqId });
        verifier.observe({ "510_1": { ids: ["9", "12"] } as never });
        await wait(30);
        expect(sent[1].data.ids).toEqual([{ actionId: "11" }, { actionId: "12" }]);
        verifier.stop();
    });

    it("verifies at once when the queue fills up and retries a miss with a higher retry count", async () => {
        const { verifier, sent } = verifierWith();
        verifier.start();
        verifier.observe({ "510_1": { ids: ["1", "2", "3", "4", "5"] } as never });
        expect(sent.length).toBe(1);
        expect(sent[0].data.ids.length).toBe(3);
        verifier.receive({ error: 1, reqId: sent[0].data.reqId });
        await wait(30);
        expect(sent[1].data.nretry).toBe(1);
        verifier.receiveMissed(sent[1].data.reqId);
        expect(sent[2].data.ids).toEqual([{ actionId: "3" }, { actionId: "4" }, { actionId: "5" }]);
        verifier.stop();
    });

    it("does nothing before it is started", async () => {
        const { verifier, sent } = verifierWith();
        verifier.observe({ "510_1": { ids: ["1", "2"] } as never });
        await wait(30);
        expect(sent).toEqual([]);
        verifier.stop();
    });
});
