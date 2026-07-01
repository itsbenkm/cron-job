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
