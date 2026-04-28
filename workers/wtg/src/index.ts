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
		try {
			let totalDeleted = 0;
			let cursor: string | undefined = undefined;

			while (true) {
				// List up to 1000 objects per page
				const listed = await env.wtg.list({
					limit: 1000,
					cursor: cursor,
				});

				const keys = listed.objects.map((obj: R2Object) => obj.key);

				if (keys.length === 0) break;

				// Batch delete all keys in this page
				await env.wtg.delete(keys);
				totalDeleted += keys.length;

				if (listed.truncated) {
					cursor = listed.cursor;
				} else {
					break;
				}
			}

			return Response.json({
				success: true,
				message: `Bucket wiped successfully`,
				total_deleted: totalDeleted,
			});
		} catch (error) {
			return Response.json(
				{
					success: false,
					error: error instanceof Error ? error.message : String(error),
				},
				{ status: 500 },
			);
		}
	},
};
