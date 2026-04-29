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
	// AUTH_TOKEN: string; // uncomment if you add a secret via: wrangler secret put AUTH_TOKEN
}
export default {
	async fetch(request: Request, env: Env): Promise<Response> {
		const url = new URL(request.url);

		// Everything after the leading slash is the R2 key
		// e.g. /products/nike/some-slug/images/01.jpg
		const r2Key = url.pathname.slice(1);

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

		return new Response('Method not allowed', { status: 405 });
	},
};
