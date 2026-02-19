// Cloudflare Worker entry point.
// With `not_found_handling = "single-page-application"` in wrangler.toml,
// static assets are served automatically and unmatched routes fall back to
// index.html. This worker only needs to exist as the required `main` entry.

export default {
  async fetch(request: Request, env: any): Promise<Response> {
    // All static assets and SPA fallback are handled by the [assets] binding.
    // This fetch handler is a no-op passthrough — CF serves assets before
    // this is reached. If somehow invoked, return 404.
    return new Response('Not Found', { status: 404 })
  },
}
