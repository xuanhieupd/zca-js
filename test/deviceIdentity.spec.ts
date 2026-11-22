import { describe, expect, it } from "bun:test";
import { acceptLanguageOf, getSignKey } from "../src/utils.js";

describe("acceptLanguageOf", () => {
    it("matches the browser header the Vietnamese web client sends", () => {
        expect(acceptLanguageOf("vi")).toBe("vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7");
        expect(acceptLanguageOf(undefined)).toBe("vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7");
    });

    it("keeps the English header and derives others from the language code", () => {
        expect(acceptLanguageOf("en")).toBe("en-US,en;q=0.9");
        expect(acceptLanguageOf("fr")).toBe("fr,en-US;q=0.8,en;q=0.7");
    });
});

describe("getServerInfo signkey", () => {
    const base = { imei: "device-1", type: 30, client_version: 695, computer_name: "Web" };

    it("signs over the same fields the web client sends, including sp_mtn and bkt", () => {
        const webClient = getSignKey("getserverinfo", { ...base, sp_mtn: -1, bkt: -1 });
        const withoutExtra = getSignKey("getserverinfo", base);
        expect(webClient).not.toBe(withoutExtra);
        expect(webClient).toBe(getSignKey("getserverinfo", { bkt: -1, sp_mtn: -1, ...base }));
    });
});
