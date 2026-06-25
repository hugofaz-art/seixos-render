// SEIXOS Render Server — renders the genuine on-chain Pebbles algorithm at high resolution
// off the iPad. Kiosk sends {hash, genome} -> /render returns {key, url, viewUrl}. /view/:key is the
// mobile page the QR opens (image + Save button). /email sends it (reusing the already-rendered image).
const http = require('http');
const fs = require('fs');
const os = require('os');
const path = require('path');
const crypto = require('crypto');
const { render } = require('./engine');

const PORT      = process.env.PORT || 3000;
const DEFAULT_H = parseInt(process.env.RENDER_H || '3840', 10);   // 3840 = 4K. 7680 = 8K (slower).
const MAX_H     = parseInt(process.env.MAX_H || '8000', 10);
const QUALITY   = parseFloat(process.env.JPEG_QUALITY || '0.92');
const STORE     = process.env.STORE_DIR || path.join(os.tmpdir(), 'seixos');   // set STORE_DIR to a Render persistent disk to keep the archive across deploys
const PUBLIC_URL= (process.env.PUBLIC_URL || '').replace(/\/$/, '');
const RESEND_KEY= process.env.RESEND_API_KEY || '';
const FROM_EMAIL= process.env.FROM_EMAIL || 'Seixos <onboarding@resend.dev>';
const TTL_HOURS = parseInt(process.env.TTL_HOURS || '0', 10);     // 0 = keep forever (archive). >0 = delete after N hours.

fs.mkdirSync(STORE, { recursive: true });
if (TTL_HOURS > 0) setInterval(() => { try { const now = Date.now(), ttl = TTL_HOURS*3600*1000;
  for (const f of fs.readdirSync(STORE)) { const p = path.join(STORE, f);
    try { if (now - fs.statSync(p).mtimeMs > ttl) fs.unlinkSync(p); } catch (e) {} } } catch (e) {}
}, 3600 * 1000).unref();

const sanitize = k => String(k||'').replace(/[^a-z0-9.]/g,'');
function cors(res){ res.setHeader('Access-Control-Allow-Origin','*'); res.setHeader('Access-Control-Allow-Methods','GET,POST,OPTIONS'); res.setHeader('Access-Control-Allow-Headers','content-type'); }
function json(res, code, obj){ cors(res); res.writeHead(code, {'content-type':'application/json'}); res.end(JSON.stringify(obj)); }
function html(res, code, body){ cors(res); res.writeHead(code, {'content-type':'text/html; charset=utf-8'}); res.end(body); }
function readBody(req){ return new Promise((resolve)=>{ let b=''; req.on('data',c=>b+=c); req.on('end',()=>{ try{ resolve(JSON.parse(b||'{}')); }catch(e){ resolve({}); } }); }); }
function baseUrl(req){ return PUBLIC_URL || ('https://' + (req.headers.host||'localhost')); }

async function renderToFile(hash, genome, h){
  const H = Math.max(200, Math.min(parseInt(h || DEFAULT_H, 10), MAX_H));
  const t0 = Date.now();
  const r = await render(hash, genome, H);
  if (!r.canvas) throw new Error('render produced no canvas');
  const buf = await r.canvas.encode('jpeg', Math.round(QUALITY*100));
  const key = crypto.randomBytes(8).toString('hex') + '.jpg';
  fs.writeFileSync(path.join(STORE, key), buf);
  // optional sidecar with traits for the /view page
  try { fs.writeFileSync(path.join(STORE, key+'.json'), JSON.stringify({ traits:r.traits, w:r.w, h:r.hgt })); } catch(e){}
  return { key, traits: r.traits, w: r.w, h: r.hgt, ms: Date.now()-t0, bytes: buf.length, done: r.done };
}

function viewPage(base, key, traits){
  const img = base + '/img/' + key;
  const size = (traits && traits.Size) ? traits.Size : '';
  return `<!doctype html><html lang="pt-BR"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<title>Seu Seixo — Casa NUA</title>
<style>:root{color-scheme:dark}*{box-sizing:border-box;margin:0;padding:0}
body{background:#0c0c0e;color:#ececec;font-family:-apple-system,BlinkMacSystemFont,Helvetica,Arial,sans-serif;min-height:100dvh;display:flex;flex-direction:column;align-items:center;padding:22px 16px 40px}
.logo{font-size:12px;letter-spacing:.2em;text-transform:uppercase;opacity:.65;margin-bottom:16px;text-align:center}
.frame{width:100%;max-width:520px;border-radius:14px;overflow:hidden;box-shadow:0 10px 44px rgba(0,0,0,.6);background:#000}
.frame img{width:100%;display:block}
.btn{margin-top:20px;width:100%;max-width:520px;padding:16px;border:none;border-radius:13px;background:#d8b25a;color:#1a1407;font-size:17px;font-weight:800;letter-spacing:.01em;cursor:pointer}
.btn:active{opacity:.85}
.hint{margin-top:11px;font-size:13px;opacity:.6;text-align:center;max-width:520px;line-height:1.5}
.foot{margin-top:26px;font-size:11.5px;opacity:.5;text-align:center;line-height:1.7}
a{color:#d8b25a;text-decoration:none}</style></head><body>
<div class="logo">Casa NUA · Domínio Público</div>
<div class="frame"><img id="peb" src="${img}" alt="Seu Seixo"></div>
<button class="btn" id="save">Salvar nas minhas Fotos</button>
<div class="hint" id="hint">Toque no botão para guardar. Você também pode tocar e segurar a imagem.</div>
<div class="foot">SEIXOS (Pebbles)${size?(' · '+size):''} · por Zeblocks · CC0<br>Arte digital descentralizada · <a href="https://6529.io">6529</a></div>
<script>
var IMG=${JSON.stringify(img)}, NAME="Seixo.jpg";
var btn=document.getElementById('save'), hint=document.getElementById('hint');
btn.addEventListener('click', async function(){
  hint.textContent='Preparando…';
  try{
    var resp=await fetch(IMG, {mode:'cors'}); var blob=await resp.blob();
    var file=new File([blob], NAME, {type:'image/jpeg'});
    if(navigator.canShare && navigator.canShare({files:[file]})){
      await navigator.share({files:[file], title:'Meu Seixo'});
      hint.textContent='Escolha "Salvar Imagem" para guardar nas Fotos.';
    } else {
      var a=document.createElement('a'); a.href=URL.createObjectURL(blob); a.download=NAME;
      document.body.appendChild(a); a.click(); a.remove();
      hint.textContent='Baixado. Toque e segure a imagem para guardar nas Fotos.';
    }
  }catch(e){ hint.textContent='Toque e segure a imagem acima para salvar nas suas Fotos.'; }
});
</script></body></html>`;
}

function emailHtml(base, key, traits){
  const img = base + '/img/' + key;
  const size = (traits && traits.Size) ? (' (tamanho '+traits.Size+')') : '';
  return `<div style="font-family:-apple-system,Helvetica,Arial,sans-serif;max-width:560px;margin:0 auto;color:#1d1d1f;line-height:1.55">
  <p style="font-size:12px;letter-spacing:.18em;text-transform:uppercase;color:#9a8038;margin:0 0 6px">Casa NUA · Domínio Público</p>
  <h1 style="font-size:23px;margin:0 0 14px">Seu Seixo chegou 🪨</h1>
  <p>Obrigado por visitar a <strong>Casa NUA</strong> na <strong>Galeria Domínio Público</strong>, no coração de São Paulo. Você acabou de gerar um Seixo único${size} — ele está em <strong>alta resolução</strong> em anexo neste e-mail, e logo abaixo:</p>
  <p style="text-align:center;margin:18px 0"><img src="${img}" alt="Seu Seixo" style="width:100%;max-width:420px;border-radius:10px"/></p>
  <h2 style="font-size:17px;margin:24px 0 8px">O que você acabou de criar</h2>
  <p><strong>Seixos (Pebbles)</strong> é uma obra generativa <strong>on-chain</strong> de <strong>Zeblocks</strong>, em <strong>domínio público (CC0)</strong> — livre para qualquer pessoa usar, remixar, imprimir e construir em cima, sem pedir permissão. O algoritmo que desenha cada Seixo vive na blockchain Ethereum, e cada combinação é única.</p>
  <h2 style="font-size:17px;margin:24px 0 8px">Sobre a Casa NUA</h2>
  <p>A Casa NUA é um museu de arte digital descentralizada em São Paulo. Na <strong>Galeria Domínio Público</strong> exibimos exclusivamente arte NFT em <strong>CC0</strong>, em parceria com a rede <strong>6529</strong> — um movimento por arte e propriedade verdadeiramente descentralizadas e de domínio público.</p>
  <p style="margin-top:18px">
    🔗 <a href="https://dominiopublico.nua.casa" style="color:#9a8038">dominiopublico.nua.casa</a> &nbsp;·&nbsp;
    <a href="https://6529.io" style="color:#9a8038">6529.io</a>
  </p>
  <p style="font-size:12px;color:#9b9b9b;margin-top:26px;border-top:1px solid #eee;padding-top:14px">Casa NUA · Domínio Público · São Paulo · Este Seixo é CC0 — é seu para guardar, imprimir e compartilhar.</p>
</div>`;
}

const server = http.createServer(async (req, res) => {
  const u = new URL(req.url, 'http://x');
  if (req.method === 'OPTIONS') { cors(res); res.writeHead(204); return res.end(); }
  if (u.pathname === '/health' || u.pathname === '/') return json(res, 200, { ok:true, service:'seixos-render', defaultH:DEFAULT_H });

  if (u.pathname.startsWith('/img/')) {
    const key = sanitize(u.pathname.slice('/img/'.length));
    const p = path.join(STORE, key);
    if (!key || !fs.existsSync(p)) { res.writeHead(404); return res.end('not found'); }
    cors(res); res.writeHead(200, {'content-type':'image/jpeg','cache-control':'public, max-age=604800'});
    return fs.createReadStream(p).pipe(res);
  }

  if (u.pathname.startsWith('/view/')) {
    const key = sanitize(u.pathname.slice('/view/'.length));
    const p = path.join(STORE, key);
    if (!key || !fs.existsSync(p)) return html(res, 404, '<body style="background:#0c0c0e;color:#ccc;font-family:sans-serif;text-align:center;padding:60px">Seixo não encontrado.</body>');
    let traits = null; try { traits = JSON.parse(fs.readFileSync(p+'.json','utf8')).traits; } catch(e){}
    return html(res, 200, viewPage(baseUrl(req), key, traits));
  }

  if (u.pathname === '/render' && req.method === 'POST') {
    try { const b = await readBody(req);
      if (!b.hash || !b.genome) return json(res, 400, { error:'hash and genome required' });
      process.stderr.write('[req] /render h='+(b.h||DEFAULT_H)+'\n');
      const out = await renderToFile(b.hash, b.genome, b.h);
      const base = baseUrl(req);
      return json(res, 200, { key: out.key, url: base+'/img/'+out.key, viewUrl: base+'/view/'+out.key, traits: out.traits, w: out.w, h: out.h, ms: out.ms });
    } catch (e) { process.stderr.write('[req] /render ERROR '+String(e&&e.stack||e)+'\n'); return json(res, 500, { error: String(e && e.message || e) }); }
  }

  if (u.pathname === '/email' && req.method === 'POST') {
    try { const b = await readBody(req);
      if (!b.to || !/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(b.to)) return json(res, 400, { error:'valid "to" required' });
      if (!RESEND_KEY) return json(res, 503, { error:'email not configured (set RESEND_API_KEY)' });
      let key = sanitize(b.key), traits = null;
      if (key && fs.existsSync(path.join(STORE, key))) {           // reuse the already-rendered image — no re-render
        try { traits = JSON.parse(fs.readFileSync(path.join(STORE, key+'.json'),'utf8')).traits; } catch(e){}
      } else if (b.hash && b.genome) {                             // fallback: render now
        const out = await renderToFile(b.hash, b.genome, b.h); key = out.key; traits = out.traits;
      } else return json(res, 400, { error:'key OR (hash+genome) required' });
      const base = baseUrl(req);
      const content = fs.readFileSync(path.join(STORE, key)).toString('base64');
      const r = await fetch('https://api.resend.com/emails', { method:'POST',
        headers:{ 'authorization':'Bearer '+RESEND_KEY, 'content-type':'application/json' },
        body: JSON.stringify({ from: FROM_EMAIL, to: b.to, subject: 'Seu Seixo — Casa NUA · Domínio Público',
          html: emailHtml(base, key, traits),
          attachments: [{ filename: 'Seixo.jpg', content }] }) });
      const j = await r.json().catch(()=>({}));
      if (!r.ok) { process.stderr.write('[req] /email resend fail '+JSON.stringify(j)+'\n'); return json(res, 502, { error:'resend failed', detail:j }); }
      return json(res, 200, { ok:true, key, viewUrl: base+'/view/'+key });
    } catch (e) { process.stderr.write('[req] /email ERROR '+String(e&&e.stack||e)+'\n'); return json(res, 500, { error: String(e && e.message || e) }); }
  }
  json(res, 404, { error:'not found' });
});
server.listen(PORT, () => console.error('seixos-render listening on :'+PORT+' (default H='+DEFAULT_H+', TTL_HOURS='+TTL_HOURS+')'));
