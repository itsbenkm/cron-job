/**
 * Welcome to Cloudflare Workers! This is your first worker.
 *
 * - Run `npm run dev` in your terminal to start a development server
 * - Open a browser tab at http://localhost:8787/ to see your worker in action
 * - Run `npm run deploy` to publish your worker
 *
 * Bind resources to your worker in `wrangler.jsonc`. After adding bindings, a type definition for the
 * `Env` object can be regenerated with `npm run cf-typegen`.
 *
 * Learn more at https://developers.cloudflare.com/workers/
 */
export interface Env {
	fbd: R2Bucket;
	// AUTH_TOKEN: string; // uncomment if you add a secret via: wrangler secret put AUTH_TOKEN
}

const CLEANUP_PREFIX = 'products/acne studios/';

export default {
	async fetch(request: Request, env: Env): Promise<Response> {
		// ── Optional auth (uncomment to enable) ──────────────────────
		// const token = request.headers.get("X-Auth-Token");
		// if (token !== env.AUTH_TOKEN) {
		//   return new Response("Unauthorized", { status: 401 });
		// }

		const url = new URL(request.url);
		const key = decodeURIComponent(url.pathname.slice(1));

		if (!key) {
			return new Response('Missing key', { status: 400 });
		}

		// ── CLEANUP — one-time delete of "acne studios/" (with space) ──
		// Hit: DELETE /cleanup-acne-studios
		// Once done, remove this block and redeploy
		if (request.method === 'DELETE' && key === 'cleanup-acne-studios') {
			const deleted: string[] = [];
			let cursor: string | undefined = undefined;

			// R2 list() is paginated — loop until all objects are listed
			do {
				const listed = await env.fbd.list({
					prefix: CLEANUP_PREFIX,
					cursor,
				});

				const keys = listed.objects.map((o) => o.key);

				if (keys.length > 0) {
					await env.fbd.delete(keys);
					deleted.push(...keys);
				}

				cursor = listed.truncated ? listed.cursor : undefined;
			} while (cursor);

			return new Response(JSON.stringify({ ok: true, deleted_count: deleted.length, deleted }), {
				status: 200,
				headers: { 'Content-Type': 'application/json' },
			});
		}

		// ── HEAD ──────────────────────────────────────────────────────
		if (request.method === 'HEAD') {
			const obj = await env.fbd.head(key);
			if (!obj) {
				return new Response(null, { status: 404 });
			}
			return new Response(null, {
				status: 200,
				headers: {
					'Content-Type': obj.httpMetadata?.contentType ?? 'application/octet-stream',
					'Content-Length': String(obj.size),
					'Last-Modified': obj.uploaded.toUTCString(),
					ETag: obj.etag ?? '',
				},
			});
		}

		// ── GET ───────────────────────────────────────────────────────
		if (request.method === 'GET') {
			const obj = await env.fbd.get(key);
			if (!obj) {
				return new Response('Not found', { status: 404 });
			}
			return new Response(obj.body, {
				status: 200,
				headers: {
					'Content-Type': obj.httpMetadata?.contentType ?? 'application/octet-stream',
					'Content-Length': String(obj.size),
					'Cache-Control': 'public, max-age=31536000, immutable',
				},
			});
		}

		// ── PUT ───────────────────────────────────────────────────────
		if (request.method === 'PUT') {
			const contentType = request.headers.get('Content-Type') ?? 'image/jpeg';
			const body = await request.arrayBuffer();

			if (!body || body.byteLength === 0) {
				return new Response('Empty body', { status: 400 });
			}

			await env.fbd.put(key, body, {
				httpMetadata: { contentType },
			});

			return new Response(JSON.stringify({ ok: true, key, size: body.byteLength }), {
				status: 201,
				headers: { 'Content-Type': 'application/json' },
			});
		}

		// DELETE on anything other than the cleanup endpoint is not allowed
		return new Response('Method not allowed', { status: 405 });
	},
} satisfies ExportedHandler<Env>;
