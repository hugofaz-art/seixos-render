# SEIXOS Render Server

Renders the genuine on-chain Pebbles algorithm at high resolution (4K / 8K) on a server, off the iPad.

## Endpoints
- `POST /render`  body `{ "hash": "0x..", "genome": {..}, "h": 7680 }` -> `{ url, key, traits }`
- `GET  /img/:key` -> the JPEG
- `POST /email`   body `{ "to": "x@y.com", "hash", "genome", "h" }` -> sends via Resend (needs RESEND_API_KEY)
- `GET  /health`  -> ok

## Env vars
- `RENDER_H`  default render height. 3840 = 4K (fits ~512MB-2GB). 7680 = 8K (needs ~2GB, plan: standard).
- `PUBLIC_URL` the public base URL of this service (e.g. https://seixos-render.onrender.com) so image links are absolute.
- `RESEND_API_KEY` / `FROM_EMAIL`  for the /email endpoint (optional).

## Notes
- `pebbles_script.js` is the decoded on-chain generative script (NextGenCore retrieveGenerativeScript, token 10000000009), embedded so no RPC is needed at runtime.
- 8K (~5935x7680 ~= 45 MP) needs ~2GB RAM; 4K (2967x3840 ~= 11 MP) fits smaller instances.
