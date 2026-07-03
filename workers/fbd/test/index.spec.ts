import { SELF } from "cloudflare:test";
import { describe, it, expect } from "vitest";

const AUTH = { "X-Auth-Token": "test-token" };

describe("fbd worker /delete-keys", () => {
	it("deletes an existing key and HEAD then returns 404", async () => {
		const key = "products/x/y/01.jpg";
		await SELF.fetch(`https://example.com/${key}`, {
			method: "PUT",
			headers: { ...AUTH, "Content-Type": "image/jpeg" },
			body: new Uint8Array([1, 2, 3]),
		});

		const before = await SELF.fetch(`https://example.com/${key}`, { method: "HEAD", headers: AUTH });
		expect(before.status).toBe(200);

		const res = await SELF.fetch("https://example.com/delete-keys", {
			method: "POST",
			headers: { ...AUTH, "Content-Type": "application/json" },
			body: JSON.stringify({ keys: [key] }),
		});
		expect(res.status).toBe(200);
		expect(await res.json()).toEqual({ deleted: 1 });

		const after = await SELF.fetch(`https://example.com/${key}`, { method: "HEAD", headers: AUTH });
		expect(after.status).toBe(404);
	});

	it("rejects an unauthorized request with 401", async () => {
		const res = await SELF.fetch("https://example.com/delete-keys", {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify({ keys: ["a"] }),
		});
		expect(res.status).toBe(401);
	});

	it("rejects an empty keys array with 400", async () => {
		const res = await SELF.fetch("https://example.com/delete-keys", {
			method: "POST",
			headers: { ...AUTH, "Content-Type": "application/json" },
			body: JSON.stringify({ keys: [] }),
		});
		expect(res.status).toBe(400);
	});
});

describe("fbd worker /list-keys", () => {
	it("lists every key, paging through the cursor", async () => {
		const keys = ["products/a/1.jpg", "products/b/2.jpg", "products/c/3.jpg"];
		for (const k of keys) {
			await SELF.fetch(`https://example.com/${k}`, {
				method: "PUT",
				headers: { ...AUTH, "Content-Type": "image/jpeg" },
				body: new Uint8Array([1]),
			});
		}

		const seen = new Set<string>();
		let cursor: string | null = null;
		for (let i = 0; i < 10; i++) {
			const q = cursor ? `?limit=1&cursor=${encodeURIComponent(cursor)}` : `?limit=1`;
			const res = await SELF.fetch(`https://example.com/list-keys${q}`, { headers: AUTH });
			expect(res.status).toBe(200);
			const body = (await res.json()) as { keys: string[]; cursor: string | null; truncated: boolean };
			body.keys.forEach((k) => seen.add(k));
			if (!body.truncated) break;
			cursor = body.cursor;
		}

		for (const k of keys) expect(seen.has(k)).toBe(true);
	});

	it("rejects an unauthorized request with 401", async () => {
		const res = await SELF.fetch("https://example.com/list-keys", {});
		expect(res.status).toBe(401);
	});
});
