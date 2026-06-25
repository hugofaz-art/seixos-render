// SEIXOS Render Server — ASYNC. Kiosk POSTs /render {hash,genome} -> returns {key,viewUrl} INSTANTLY;
// the heavy render runs in a worker thread. /view/:key is the mobile page the QR opens (polls until ready,
// then shows the pebble + Save button). /email sends it. /status/:key reports readiness.
const http = require('http');
const fs = require('fs');
const os = require('os');
const path = require('path');
const crypto = require('crypto');
const { Worker } = require('worker_threads');

const PORT      = process.env.PORT || 3000;
const DEFAULT_H = parseInt(process.env.RENDER_H || '3840', 10);
const MAX_H     = parseInt(process.env.MAX_H || '8000', 10);
const QUALITY   = parseFloat(process.env.JPEG_QUALITY || '0.92');
const STORE     = process.env.STORE_DIR || path.join(os.tmpdir(), 'seixos');
const PUBLIC_URL= (process.env.PUBLIC_URL || '').replace(/\/$/, '');
const RESEND_KEY= process.env.RESEND_API_KEY || '';
const FROM_EMAIL= process.env.FROM_EMAIL || 'Seixos <onboarding@resend.dev>';
const TTL_HOURS = parseInt(process.env.TTL_HOURS || '0', 10);          // 0 = keep forever (archive)
const MAX_CONC  = parseInt(process.env.MAX_CONCURRENT || '1', 10);     // parallel renders (each ~6-9GB at 4K). 1 = safe on 16GB; bump via env if needed.
const HEAP_MB   = parseInt(process.env.WORKER_HEAP_MB || '7000', 10);  // V8 heap cap per render worker (4K needs a lot)

fs.mkdirSync(STORE, { recursive: true });
if (TTL_HOURS > 0) setInterval(() => { try { const now = Date.now(), ttl = TTL_HOURS*3600*1000;
  for (const f of fs.readdirSync(STORE)) { const p = path.join(STORE, f);
    try { if (now - fs.statSync(p).mtimeMs > ttl) fs.unlinkSync(p); } catch (e) {} } } catch (e) {}
}, 3600 * 1000).unref();

// ---- async render queue ----
const jobs = new Map();                 // key -> { status:'queued'|'rendering'|'done'|'error', error, traits, ts }
const queue = []; let active = 0;
function startJob(key, hash, genome, h){
  const seq = nextSeq();                                 // sequential number for this generated Seixo (used in the filename)
  jobs.set(key, { status:'queued', ts:Date.now(), seq });
  queue.push({ key, hash, genome, h, seq }); pump();
}
function pump(){
  while (active < MAX_CONC && queue.length){
    const job = queue.shift(); active++;
    const j = jobs.get(job.key) || {}; j.status='rendering'; jobs.set(job.key, j);
    process.stderr.write('[job] start '+job.key+' h='+job.h+' (active='+active+')\n');
    let w;
    try {
      w = new Worker(path.join(__dirname,'worker.js'), {
        workerData:{ hash:job.hash, genome:job.genome, h:job.h, key:job.key, storeDir:STORE, quality:QUALITY, seq:job.seq },
        resourceLimits:{ maxOldGenerationSizeMb: HEAP_MB }   // worker threads reject --expose-gc execArgv; engine's gc() is optional (guarded)
      });
    } catch(e){ const jj=jobs.get(job.key)||{}; jj.status='error'; jj.error=String(e); jobs.set(job.key,jj); active--; continue; }
    w.on('message', m => { const jj = jobs.get(job.key) || {};
      if (m.ok){ jj.status='done'; jj.traits=m.traits; process.stderr.write('[job] done '+job.key+'\n'); }
      else { jj.status='error'; jj.error=m.error; process.stderr.write('[job] error '+job.key+' '+m.error+'\n'); }
      jobs.set(job.key, jj); });
    w.on('error', e => { const jj=jobs.get(job.key)||{}; jj.status='error'; jj.error=String(e&&e.message||e); jobs.set(job.key,jj); process.stderr.write('[job] worker error '+job.key+' '+e+'\n'); });
    w.on('exit', () => { active--; pump(); });
  }
}
function isReady(key){ try { return fs.existsSync(path.join(STORE, key)); } catch(e){ return false; } }
function waitForKey(key, timeoutMs){ return new Promise((resolve,reject)=>{ const t0=Date.now();
  (function chk(){ if (isReady(key)) return resolve();
    const j = jobs.get(key); if (j && j.status==='error') return reject(new Error(j.error||'render failed'));
    if (Date.now()-t0 > timeoutMs) return reject(new Error('timeout'));
    setTimeout(chk, 1500); })(); }); }

const sanitize = k => String(k||'').replace(/[^a-z0-9.]/g,'');
function cors(res){ res.setHeader('Access-Control-Allow-Origin','*'); res.setHeader('Access-Control-Allow-Methods','GET,POST,OPTIONS'); res.setHeader('Access-Control-Allow-Headers','content-type'); }
function json(res, code, obj){ cors(res); res.writeHead(code, {'content-type':'application/json'}); res.end(JSON.stringify(obj)); }
function htmlRes(res, code, body){ cors(res); res.writeHead(code, {'content-type':'text/html; charset=utf-8'}); res.end(body); }
function readBody(req){ return new Promise((resolve)=>{ let b=''; req.on('data',c=>b+=c); req.on('end',()=>{ try{ resolve(JSON.parse(b||'{}')); }catch(e){ resolve({}); } }); }); }
function baseUrl(req){ return PUBLIC_URL || ('https://' + (req.headers.host||'localhost')); }
function traitsFor(key){ try { return JSON.parse(fs.readFileSync(path.join(STORE, key+'.json'),'utf8')).traits; } catch(e){ return null; } }

// ---- friendly filenames: Seixo_<NNNN>-<Palette-PT>.jpg ----
// Portuguese palette names. Others (Unigrids, Beatboxes, Sgt. Pepe, Blueprint, summer.jpg) fall back to the English name.
const PALETTE_PT = {
  "Shades of Hey!":"Tons de Finta","Terra Echoes":"Ecos da Terra","Coral Reef":"Recife de Coral","Dino Disco":"Disco Dino",
  "Forest Whisper":"Sussurro da Floresta","Serenity":"Serenidade","Dusk Riverbed":"Leito ao Anoitecer","Galactic Latte":"Latte Galatico",
  "Desert Mirage":"Miragem do Deserto","Citrus Slate":"Ardosia Citrica","Night Owl Doodles":"Rabiscos Noturnos",
  "Elephant Pajamas":"Pijama de Elefantinho","Salsa Blush":"Blush de Salsa","Magma Mambo":"Mambo de Magma","Lunar Chuckles":"Risadas Lunares"
};
function slugify(s){ return String(s||'').normalize('NFD').replace(/[̀-ͯ]/g,'').replace(/[^a-zA-Z0-9]+/g,'-').replace(/^-+|-+$/g,''); }
function fileNameFor(key){
  let traits=null, seq=0;
  try { const j=JSON.parse(fs.readFileSync(path.join(STORE, key+'.json'),'utf8')); traits=j.traits; seq=j.seq||0; } catch(e){}
  const pal = (traits && traits.Palette) ? (PALETTE_PT[traits.Palette] || traits.Palette) : '';
  const num = String(seq||0).padStart(4,'0');
  return 'Seixo_' + num + (pal ? '-'+slugify(pal) : '') + '.jpg';
}
// persistent sequence counter (STORE/counter.txt). NOTE: STORE defaults to ephemeral /tmp -> resets on redeploy.
const COUNTER_FILE = path.join(STORE, 'counter.txt');
let seqCounter = (()=>{ try { return parseInt(fs.readFileSync(COUNTER_FILE,'utf8'),10) || 0; } catch(e){ return 0; } })();
function nextSeq(){ seqCounter++; try { fs.writeFileSync(COUNTER_FILE, String(seqCounter)); } catch(e){} return seqCounter; }

function viewPage(base, key, traits){
  const img = base+'/img/'+key, status = base+'/status/'+key;
  const size = (traits && traits.Size) ? traits.Size : '';
  return `<!doctype html><html lang="pt-BR"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<title>Seu Seixo — Casa NUA</title>
<style>:root{color-scheme:dark}*{box-sizing:border-box;margin:0;padding:0}
body{background:#0c0c0e;color:#ececec;font-family:-apple-system,BlinkMacSystemFont,Helvetica,Arial,sans-serif;min-height:100dvh;display:flex;flex-direction:column;align-items:center;padding:22px 16px 40px}
.logo{font-size:12px;letter-spacing:.2em;text-transform:uppercase;opacity:.65;margin-bottom:16px;text-align:center}
.frame{width:100%;max-width:520px;aspect-ratio:1236/1600;border-radius:14px;overflow:hidden;box-shadow:0 10px 44px rgba(0,0,0,.6);background:#15151a;display:flex;align-items:center;justify-content:center;text-align:center}
.frame img{width:100%;height:100%;object-fit:cover;display:none}
.prep{padding:24px;opacity:.85;line-height:1.6;font-size:15px}
.spin{width:30px;height:30px;border:3px solid #333;border-top-color:#d8b25a;border-radius:50%;margin:0 auto 14px;animation:r 1s linear infinite}@keyframes r{to{transform:rotate(360deg)}}
.btn{margin-top:20px;width:100%;max-width:520px;padding:16px;border:none;border-radius:13px;background:#d8b25a;color:#1a1407;font-size:17px;font-weight:800;cursor:pointer;display:none}
.btn:active{opacity:.85}
.hint{margin-top:11px;font-size:13px;opacity:.6;text-align:center;max-width:520px;line-height:1.5}
.foot{margin-top:26px;font-size:11.5px;opacity:.5;text-align:center;line-height:1.7}
a{color:#d8b25a;text-decoration:none}</style></head><body>
<div class="logo">Casa NUA · Domínio Público</div>
<div class="frame"><div class="prep" id="prep"><div class="spin"></div>Preparando seu Seixo em alta resolução…<br><small style="opacity:.7">isso pode levar um instante</small></div><img id="peb" alt="Seu Seixo"></div>
<button class="btn" id="save">Salvar nas minhas Fotos</button>
<div class="hint" id="hint"></div>
<div class="foot">SEIXOS (Pebbles)${size?(' · '+size):''} · por Zeblocks · CC0<br>Arte digital descentralizada · <a href="https://6529.io">6529</a></div>
<script>
var IMG=${JSON.stringify(img)}, STATUS=${JSON.stringify(status)}, NAME=${JSON.stringify(fileNameFor(key))};
var prep=document.getElementById('prep'), peb=document.getElementById('peb'), btn=document.getElementById('save'), hint=document.getElementById('hint');
function reveal(){ peb.onload=function(){ prep.style.display='none'; peb.style.display='block'; btn.style.display='block'; hint.textContent='Toque no botão para guardar. Você também pode tocar e segurar a imagem.'; }; peb.src=IMG+'?t='+Date.now(); }
function poll(){ fetch(STATUS,{cache:'no-store'}).then(function(r){return r.json();}).then(function(j){
  if(j.ready){ if(j.filename) NAME=j.filename; reveal(); }
  else if(j.status==='error'){ prep.innerHTML='Não foi possível gerar este Seixo.<br>Tente gerar outro no totem.'; }
  else { setTimeout(poll, 2500); }
}).catch(function(){ setTimeout(poll, 3500); }); }
poll();
btn.addEventListener('click', async function(){ hint.textContent='Preparando…';
  try{ var resp=await fetch(IMG,{mode:'cors'}); var blob=await resp.blob(); var file=new File([blob],NAME,{type:'image/jpeg'});
    if(navigator.canShare && navigator.canShare({files:[file]})){ await navigator.share({files:[file],title:'Meu Seixo'}); hint.textContent='Escolha "Salvar Imagem" para guardar nas Fotos.'; }
    else { var a=document.createElement('a'); a.href=URL.createObjectURL(blob); a.download=NAME; document.body.appendChild(a); a.click(); a.remove(); hint.textContent='Baixado. Toque e segure a imagem para guardar nas Fotos.'; }
  }catch(e){ hint.textContent='Toque e segure a imagem acima para salvar nas suas Fotos.'; } });
</script></body></html>`;
}

function emailHtml(base, key, traits){
  const img = base+'/img/'+key;
  const size = (traits && traits.Size) ? (' (tamanho '+traits.Size+')') : '';
  return `<div style="font-family:-apple-system,Helvetica,Arial,sans-serif;max-width:560px;margin:0 auto;color:#1d1d1f;line-height:1.55">
  <p style="font-size:12px;letter-spacing:.18em;text-transform:uppercase;color:#9a8038;margin:0 0 6px">Casa NUA · Domínio Público</p>
  <h1 style="font-size:23px;margin:0 0 14px">Seu Seixo chegou 🪨</h1>
  <p>Obrigado por visitar a <strong>galeria Domínio Público</strong> da <strong>Casa NUA</strong> na Formosa, no coração de São Paulo. Você acabou de gerar um Seixo único${size} — ele está em <strong>alta resolução</strong> em anexo neste e-mail, e logo abaixo:</p>
  <p style="text-align:center;margin:18px 0"><img src="${img}" alt="Seu Seixo" style="width:100%;max-width:420px;border-radius:10px"/></p>
  <h2 style="font-size:17px;margin:24px 0 8px">O que você acabou de criar</h2>
  <p><strong>Seixos (Pebbles)</strong> é uma obra generativa <strong>on-chain</strong> de <strong>Zeblocks</strong>, em <strong>domínio público (CC0)</strong> — livre para qualquer pessoa usar, remixar, imprimir e construir em cima, sem pedir permissão. O algoritmo que desenha cada Seixo vive na blockchain Ethereum, e cada combinação é única. As <strong>1.000 Pebbles</strong> que você viu na exposição são os NFTs originais, mas o algoritmo permite a criação de <strong>infinitas</strong> novas obras como a que você acabou de criar, sempre únicas!</p>
  <h2 style="font-size:17px;margin:24px 0 8px">Sobre a Casa NUA</h2>
  <p>A Casa NUA é um museu de arte digital descentralizada em São Paulo. Na <strong>Galeria Domínio Público</strong> exibimos exclusivamente arte NFT em <strong>CC0</strong>, em parceria com a rede <strong>6529</strong> — um movimento por arte e propriedade verdadeiramente descentralizadas e de domínio público.</p>
  <h2 style="font-size:17px;margin:24px 0 8px">Siga a Casa NUA no Instagram</h2>
  <p>Acompanhe exposições, artistas e bastidores em <a href="https://instagram.com/nua.casa" style="color:#9a8038"><strong>@nua.casa</strong></a>.</p>
  <p style="text-align:center;margin:14px 0"><a href="https://instagram.com/nua.casa"><img src="${base}/ig-qr.png" alt="Instagram @nua.casa — Casa NUA" style="width:190px;height:190px;border-radius:14px"/></a></p>
  <p style="margin-top:18px">🔗 <a href="https://dominiopublico.nua.casa" style="color:#9a8038">dominiopublico.nua.casa</a> &nbsp;·&nbsp; <a href="https://6529.io" style="color:#9a8038">6529.io</a></p>
  <p style="font-size:12px;color:#9b9b9b;margin-top:26px;border-top:1px solid #eee;padding-top:14px">Casa NUA · Domínio Público · São Paulo · Este Seixo é CC0 — é seu para guardar, imprimir e compartilhar.</p>
</div>`;
}

const server = http.createServer(async (req, res) => {
  const u = new URL(req.url, 'http://x');
  if (req.method === 'OPTIONS') { cors(res); res.writeHead(204); return res.end(); }
  if (u.pathname === '/health' || u.pathname === '/') return json(res, 200, { ok:true, service:'seixos-render', defaultH:DEFAULT_H, active, queued:queue.length });

  if (u.pathname === '/ig-qr.png') {                                  // Instagram QR (Casa NUA) — used in the onboarding e-mail
    try { cors(res); res.writeHead(200, {'content-type':'image/png','cache-control':'public, max-age=86400'});
      return fs.createReadStream(path.join(__dirname,'ig-qr.png')).pipe(res); }
    catch(e){ res.writeHead(404); return res.end('not found'); }
  }

  if (u.pathname === '/list') {                                       // archive listing for the local pull script
    let files = []; try { files = fs.readdirSync(STORE).filter(f => /\.jpg$/.test(f)); } catch(e){}
    if (u.searchParams.get('format') === 'json') {
      const items = files.map(f => { let mtime=0, traits=null;
        try { mtime = fs.statSync(path.join(STORE,f)).mtimeMs; } catch(e){}
        try { traits = JSON.parse(fs.readFileSync(path.join(STORE,f+'.json'),'utf8')).traits; } catch(e){}
        return { key:f, filename: fileNameFor(f), mtime, traits }; }).sort((a,b)=>a.mtime-b.mtime);
      return json(res, 200, { count: items.length, items });
    }
    cors(res); res.writeHead(200, {'content-type':'text/plain; charset=utf-8'}); return res.end(files.join('\n'));
  }

  if (u.pathname.startsWith('/img/')) {
    const key = sanitize(u.pathname.slice('/img/'.length));
    const p = path.join(STORE, key);
    if (!key || !fs.existsSync(p)) { const j=jobs.get(key); res.writeHead(j&&(j.status==='rendering'||j.status==='queued')?202:404); return res.end(j&&j.status==='rendering'?'rendering':'not found'); }
    cors(res); res.writeHead(200, {'content-type':'image/jpeg','cache-control':'public, max-age=604800'});
    return fs.createReadStream(p).pipe(res);
  }

  if (u.pathname.startsWith('/status/')) {
    const key = sanitize(u.pathname.slice('/status/'.length));
    const ready = isReady(key); const j = jobs.get(key);
    return json(res, 200, { ready, status: ready ? 'done' : (j ? j.status : 'unknown'), filename: ready ? fileNameFor(key) : undefined, error: (j && j.status==='error') ? j.error : undefined });
  }

  if (u.pathname.startsWith('/view/')) {
    const key = sanitize(u.pathname.slice('/view/'.length));
    if (!key) return htmlRes(res, 404, '<body style="background:#0c0c0e;color:#ccc;font-family:sans-serif;text-align:center;padding:60px">Seixo não encontrado.</body>');
    return htmlRes(res, 200, viewPage(baseUrl(req), key, traitsFor(key)));   // page polls /status until ready
  }

  if (u.pathname === '/render' && req.method === 'POST') {
    try { const b = await readBody(req);
      if (!b.hash || !b.genome) return json(res, 400, { error:'hash and genome required' });
      const H = Math.max(200, Math.min(parseInt(b.h || DEFAULT_H, 10), MAX_H));
      const key = crypto.randomBytes(8).toString('hex') + '.jpg';
      startJob(key, b.hash, b.genome, H);                                    // returns immediately; render runs in a worker
      const base = baseUrl(req);
      return json(res, 202, { key, url: base+'/img/'+key, viewUrl: base+'/view/'+key, statusUrl: base+'/status/'+key, status:'rendering', h:H });
    } catch (e) { return json(res, 500, { error: String(e && e.message || e) }); }
  }

  if (u.pathname === '/email' && req.method === 'POST') {
    try { const b = await readBody(req);
      if (!b.to || !/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(b.to)) return json(res, 400, { error:'valid "to" required' });
      if (!RESEND_KEY) return json(res, 503, { error:'email not configured (set RESEND_API_KEY)' });
      let key = sanitize(b.key);
      if (!key || (!isReady(key) && !jobs.get(key))) {                        // no key (or unknown) -> render now
        if (!b.hash || !b.genome) return json(res, 400, { error:'key OR (hash+genome) required' });
        key = crypto.randomBytes(8).toString('hex') + '.jpg'; startJob(key, b.hash, b.genome, Math.max(200, Math.min(parseInt(b.h||DEFAULT_H,10), MAX_H)));
      }
      await waitForKey(key, 240000);                                         // wait up to 4 min for the render to finish
      const base = baseUrl(req);
      const content = fs.readFileSync(path.join(STORE, key)).toString('base64');
      const r = await fetch('https://api.resend.com/emails', { method:'POST',
        headers:{ 'authorization':'Bearer '+RESEND_KEY, 'content-type':'application/json' },
        body: JSON.stringify({ from: FROM_EMAIL, to: b.to, subject: 'Seu Seixo — Casa NUA · Domínio Público',
          html: emailHtml(base, key, traitsFor(key)), attachments: [{ filename: fileNameFor(key), content }] }) });
      const j = await r.json().catch(()=>({}));
      if (!r.ok) { process.stderr.write('[email] resend fail '+JSON.stringify(j)+'\n'); return json(res, 502, { error:'resend failed', detail:j }); }
      return json(res, 200, { ok:true, key, viewUrl: base+'/view/'+key });
    } catch (e) { process.stderr.write('[email] ERROR '+String(e&&e.stack||e)+'\n'); return json(res, 500, { error: String(e && e.message || e) }); }
  }
  json(res, 404, { error:'not found' });
});
server.listen(PORT, () => console.error('seixos-render (async) on :'+PORT+' H='+DEFAULT_H+' maxConc='+MAX_CONC+' heap='+HEAP_MB+'MB TTL='+TTL_HOURS));
