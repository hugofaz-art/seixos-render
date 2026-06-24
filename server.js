// SEIXOS Render Server — renders the genuine on-chain Pebbles algorithm at high resolution (up to 8K),
// off the iPad. The kiosk sends {hash, genome}; this returns a hosted high-res image URL (and can e-mail it).
const http = require('http');
const fs = require('fs');
const os = require('os');
const path = require('path');
const crypto = require('crypto');
const { render } = require('./engine');

const PORT      = process.env.PORT || 3000;
const DEFAULT_H = parseInt(process.env.RENDER_H || '3840', 10);   // 3840 = 4K. Set RENDER_H=7680 for 8K (needs a ~2GB instance).
const MAX_H     = parseInt(process.env.MAX_H || '8000', 10);
const QUALITY   = parseFloat(process.env.JPEG_QUALITY || '0.92');
const STORE     = process.env.STORE_DIR || path.join(os.tmpdir(), 'seixos');
const PUBLIC_URL= (process.env.PUBLIC_URL || '').replace(/\/$/, '');  // e.g. https://seixos-render.onrender.com
const RESEND_KEY= process.env.RESEND_API_KEY || '';
const FROM_EMAIL= process.env.FROM_EMAIL || 'Seixos <onboarding@resend.dev>';
const TTL_MS    = parseInt(process.env.TTL_HOURS || '48', 10) * 3600 * 1000;

fs.mkdirSync(STORE, { recursive: true });
// periodic cleanup of old files
setInterval(() => { try { const now = Date.now();
  for (const f of fs.readdirSync(STORE)) { const p = path.join(STORE, f);
    try { if (now - fs.statSync(p).mtimeMs > TTL_MS) fs.unlinkSync(p); } catch (e) {} } } catch (e) {}
}, 3600 * 1000).unref();

function cors(res){ res.setHeader('Access-Control-Allow-Origin','*'); res.setHeader('Access-Control-Allow-Methods','GET,POST,OPTIONS'); res.setHeader('Access-Control-Allow-Headers','content-type'); }
function json(res, code, obj){ cors(res); res.writeHead(code, {'content-type':'application/json'}); res.end(JSON.stringify(obj)); }
function readBody(req){ return new Promise((resolve)=>{ let b=''; req.on('data',c=>b+=c); req.on('end',()=>{ try{ resolve(JSON.parse(b||'{}')); }catch(e){ resolve({}); } }); }); }

async function renderToFile(hash, genome, h){
  const H = Math.max(200, Math.min(parseInt(h || DEFAULT_H, 10), MAX_H));
  const t0 = Date.now();
  const r = await render(hash, genome, H);
  if (!r.canvas) throw new Error('render produced no canvas');
  const buf = await r.canvas.encode('jpeg', Math.round(QUALITY*100));
  const key = crypto.randomBytes(8).toString('hex') + '.jpg';
  fs.writeFileSync(path.join(STORE, key), buf);
  return { key, traits: r.traits, w: r.w, h: r.hgt, ms: Date.now()-t0, bytes: buf.length, done: r.done };
}
function imgUrl(req, key){ const base = PUBLIC_URL || ('https://' + (req.headers.host||'localhost')); return base + '/img/' + key; }

const server = http.createServer(async (req, res) => {
  const u = new URL(req.url, 'http://x');
  if (req.method === 'OPTIONS') { cors(res); res.writeHead(204); return res.end(); }
  if (u.pathname === '/health' || u.pathname === '/') return json(res, 200, { ok:true, service:'seixos-render', defaultH:DEFAULT_H });

  if (u.pathname === '/img/' || u.pathname.startsWith('/img/')) {
    const key = u.pathname.slice('/img/'.length).replace(/[^a-f0-9.]/g,'');
    const p = path.join(STORE, key);
    if (!fs.existsSync(p)) { res.writeHead(404); return res.end('not found'); }
    cors(res); res.writeHead(200, {'content-type':'image/jpeg','cache-control':'public, max-age=604800'});
    return fs.createReadStream(p).pipe(res);
  }

  if (u.pathname === '/render' && req.method === 'POST') {
    try { const b = await readBody(req);
      if (!b.hash || !b.genome) return json(res, 400, { error:'hash and genome required' });
      process.stderr.write('[req] /render h='+(b.h||DEFAULT_H)+'\n');
      const out = await renderToFile(b.hash, b.genome, b.h);
      return json(res, 200, { url: imgUrl(req, out.key), key: out.key, traits: out.traits, w: out.w, h: out.h, ms: out.ms });
    } catch (e) { process.stderr.write('[req] /render ERROR '+String(e&&e.stack||e)+'\n'); return json(res, 500, { error: String(e && e.message || e) }); }
  }

  if (u.pathname === '/email' && req.method === 'POST') {
    try { const b = await readBody(req);
      if (!b.to || !b.hash || !b.genome) return json(res, 400, { error:'to, hash, genome required' });
      if (!RESEND_KEY) return json(res, 503, { error:'email not configured (set RESEND_API_KEY)' });
      const out = await renderToFile(b.hash, b.genome, b.h);
      const content = fs.readFileSync(path.join(STORE, out.key)).toString('base64');
      const r = await fetch('https://api.resend.com/emails', { method:'POST',
        headers:{ 'authorization':'Bearer '+RESEND_KEY, 'content-type':'application/json' },
        body: JSON.stringify({ from: FROM_EMAIL, to: b.to, subject: 'Seu Seixo — Casa NUA',
          html: '<p>Seu Seixo em alta resolução está em anexo. Obrigado por visitar a Casa NUA — Domínio Público.</p><p><img src="'+imgUrl(req,out.key)+'" width="300"/></p>',
          attachments: [{ filename: 'Seixo.jpg', content }] }) });
      const j = await r.json().catch(()=>({}));
      if (!r.ok) return json(res, 502, { error:'resend failed', detail:j });
      return json(res, 200, { ok:true, url: imgUrl(req, out.key) });
    } catch (e) { return json(res, 500, { error: String(e && e.message || e) }); }
  }
  json(res, 404, { error:'not found' });
});
server.listen(PORT, () => console.error('seixos-render listening on :'+PORT+' (default H='+DEFAULT_H+')'));
