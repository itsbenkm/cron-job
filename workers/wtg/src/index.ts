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
	wtg: R2Bucket;
	AUTH_TOKEN: string;
}

export default {
	async fetch(request: Request, env: Env): Promise<Response> {
		// ── Auth ──────────────────────────────────────────────────────
		const token = request.headers.get('X-Auth-Token');
		if (token !== env.AUTH_TOKEN) {
			return new Response('Unauthorized', { status: 401 });
		}

		const url = new URL(request.url);
		// Everything after the leading slash is the R2 key
		// e.g. /products/nike/some-slug/images/01.jpg
		const r2Key = decodeURIComponent(url.pathname.slice(1));

		if (!r2Key) {
			return new Response('Missing R2 key in path', { status: 400 });
		}

		// ── HEAD — check if object exists and return its content-type ────────────
		if (request.method === 'HEAD') {
			const object = await env.wtg.head(r2Key);
			if (!object) {
				return new Response(null, { status: 404 });
			}
			return new Response(null, {
				status: 200,
				headers: {
					'Content-Type': object.httpMetadata?.contentType ?? 'application/octet-stream',
					'Content-Length': String(object.size),
				},
			});
		}

		// ── PUT — upload JPEG bytes to R2 ────────────────────────────────────────
		if (request.method === 'PUT') {
			const contentType = request.headers.get('Content-Type') ?? 'image/jpeg';
			const body = await request.arrayBuffer();

			if (!body || body.byteLength === 0) {
				return new Response('Empty body', { status: 400 });
			}

			await env.wtg.put(r2Key, body, {
				httpMetadata: { contentType },
			});

			return new Response(JSON.stringify({ ok: true, key: r2Key }), {
				status: 201,
				headers: { 'Content-Type': 'application/json' },
			});
		}

		// ── POST /delete-keys — batch delete R2 objects by exact key ──
		if (request.method === 'POST' && r2Key === 'delete-keys') {
			let parsed: { keys?: unknown };
			try {
				parsed = await request.json();
			} catch {
				return new Response('Invalid JSON body', { status: 400 });
			}
			const keys = parsed?.keys;
			if (!Array.isArray(keys) || keys.length === 0 || !keys.every((k) => typeof k === 'string')) {
				return new Response('Body must be { keys: string[] } with at least one key', { status: 400 });
			}

			let deleted = 0;
			for (let i = 0; i < keys.length; i += 1000) {
				const chunk = keys.slice(i, i + 1000) as string[];
				await env.wtg.delete(chunk);
				deleted += chunk.length;
			}

			return new Response(JSON.stringify({ deleted }), {
				status: 200,
				headers: { 'Content-Type': 'application/json' },
			});
		}

		return new Response('Method not allowed', { status: 405 });
	},
};
