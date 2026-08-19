// SEIXOS Render Server — ASYNC. Kiosk POSTs /render {hash,genome} -> returns {key,viewUrl} INSTANTLY;
// the heavy render runs in a worker thread. /view/:key is the mobile page the QR opens (polls until ready,
// then shows the pebble + Save button). /email sends it. /status/:key reports readiness.
const http = require('http');
const fs = require('fs');
const os = require('os');
const path = require('path');
const crypto = require('crypto');
const { fork } = require('child_process');   // render each job in a short-lived CHILD PROCESS (was worker_threads) so the OS reclaims all memory per render — fixes memory-creep OOM

const PORT      = process.env.PORT || 3000;
const DEFAULT_H = parseInt(process.env.RENDER_H || '3840', 10);
const MAX_H     = parseInt(process.env.MAX_H || '8000', 10);
const QUALITY   = parseFloat(process.env.JPEG_QUALITY || '0.92');
const STORE     = process.env.STORE_DIR || path.join(os.tmpdir(), 'seixos');
const PUBLIC_URL= (process.env.PUBLIC_URL || '').replace(/\/$/, '');
const RESEND_KEY= process.env.RESEND_API_KEY || '';
const ADMIN_TOKEN= process.env.ADMIN_TOKEN || '';                      // protects /visitors export (visitor e-mails = personal data)
const FROM_EMAIL= process.env.FROM_EMAIL || 'Seixos <onboarding@resend.dev>';
const TTL_HOURS = parseInt(process.env.TTL_HOURS || '0', 10);          // 0 = keep forever (archive)
const MAX_CONC  = parseInt(process.env.MAX_CONCURRENT || '1', 10);     // parallel renders (each ~6-9GB at 4K). 1 = safe on 16GB; bump via env if needed.
const HEAP_MB   = parseInt(process.env.WORKER_HEAP_MB || '7000', 10);  // V8 heap cap per render worker (4K needs a lot)
const RENDER_TIMEOUT_MS = parseInt(process.env.RENDER_TIMEOUT_MS || '180000', 10);  // kill a render that runs longer than this — a HUNG render must never hold the single slot and freeze the whole queue (a normal render is 114-131s at h=2796). NOTE: this is only a ~1.4x margin over normal, so it may be shooting slow-but-healthy renders; unproven either way, left alone deliberately so the RENDER_H experiment isn't confounded.

fs.mkdirSync(STORE, { recursive: true });
if (TTL_HOURS > 0) setInterval(() => { try { const now = Date.now(), ttl = TTL_HOURS*3600*1000;
  for (const f of fs.readdirSync(STORE)) {
    if (f === 'counter.txt' || f[0] === '_') continue;          // NUNCA expira o contador nem os logs privados (_visitors.jsonl)
    const p = path.join(STORE, f);
    try { if (now - fs.statSync(p).mtimeMs > ttl) fs.unlinkSync(p); } catch (e) {} } } catch (e) {}
}, 3600 * 1000).unref();

// ---- async render queue ----
const jobs = new Map();                 // key -> { status:'queued'|'rendering'|'done'|'error', error, traits, ts }
const queue = []; let active = 0;
// PILE-UP GUARD (16 Aug). A visitor whose render is slow gets told it failed (/email gives up at 4 min,
// /view at 5) and taps again - but the first render is usually still going. With MAX_CONC=1 each retry
// lands BEHIND it, so the queue grows, more visitors cross the give-up line, and they retry too. That
// loop is why waits reached 429s against a 130s median. Identical work is now COLLAPSED: a repeat of a
// pebble already queued or rendering returns the SAME key, so the visitor re-attaches to the render
// already in flight instead of starting a second one.
const inflight = new Map();             // fingerprint -> key, only while queued/rendering
function fingerprint(hash, genome, h, mint){ return String(hash)+'|'+JSON.stringify(genome||null)+'|'+h+'|'+(mint||'MemeMaxis'); }
function inflightKey(hash, genome, h, mint){
  const f = fingerprint(hash, genome, h, mint), k = inflight.get(f);
  if (!k) return null;
  const j = jobs.get(k);
  if (j && (j.status === 'queued' || j.status === 'rendering')) return k;   // still going -> reuse it
  inflight.delete(f); return null;                                          // finished/failed -> let a fresh one run
}
function startJob(key, hash, genome, h, mint){
  const seq = nextSeq();                                 // sequential number for this generated Seixo (used in the filename)
  const createdAt = new Date().toISOString();            // generation timestamp (UTC, ISO 8601)
  jobs.set(key, { status:'queued', ts:Date.now(), seq, createdAt });
  inflight.set(fingerprint(hash, genome, h, mint), key);            // so a retry of the same pebble re-attaches instead of queueing again
  queue.push({ key, hash, genome, h, seq, createdAt, mint: mint || 'MemeMaxis' }); pump();
}
function pump(){
  while (active < MAX_CONC && queue.length){
    const job = queue.shift(); active++;
    const j = jobs.get(job.key) || {}; j.status='rendering'; jobs.set(job.key, j);
    process.stderr.write('[job] start '+job.key+' h='+job.h+' (active='+active+')\n');
    let w;
    try {
      w = fork(path.join(__dirname,'worker.js'), [], {
        execArgv:['--max-old-space-size='+HEAP_MB, '--expose-gc'],   // child_process (unlike worker_threads) accepts --expose-gc, so the engine's GC runs; the child EXITS after the render, returning all memory to the OS
        // MALLOC_ARENA_MAX (18 Aug) — set on the CHILD, which is the process that actually allocates.
        // On Linux, glibc hands each thread its own malloc arena and each arena keeps its own freed
        // memory instead of returning it, so RSS inflates well beyond live data. That is the exact shape
        // we measured: isolated 2-15GB spikes against a 16GB limit with NO leak and no creep, and a peak
        // that moves +/-1.8GB between identical runs. @napi-rs/canvas is native and multithreaded, so it
        // is a prime candidate. Capping the arenas trades a little allocator contention for a much
        // tighter RSS. Untestable on macOS (different allocator) - this is a production experiment, and
        // it is reversible by deleting these two lines.
        env: Object.assign({}, process.env, { MALLOC_ARENA_MAX: process.env.MALLOC_ARENA_MAX || '2' })
      });
      w.send({ hash:job.hash, genome:job.genome, h:job.h, key:job.key, storeDir:STORE, quality:QUALITY, seq:job.seq, createdAt:job.createdAt, mint:job.mint, progressive:!!job.progressive });
    } catch(e){ const jj=jobs.get(job.key)||{}; jj.status='error'; jj.error=String(e); jobs.set(job.key,jj); active--; continue; }
    const budget = job.progressive ? RENDER_TIMEOUT_MS*2 : RENDER_TIMEOUT_MS;   // nothing sets job.progressive any more (the rescue was removed 16 Aug), so this is always RENDER_TIMEOUT_MS. The branch is left in place because engine.js still supports the progressive path and we may yet want it.
    const killer = setTimeout(() => { process.stderr.write('[job] TIMEOUT '+job.key+' >'+budget+'ms — killing render to free the queue\n'); try { w.kill('SIGKILL'); } catch(e){} }, budget);   // watchdog: a stuck render is force-killed so it cannot hold the single slot. Its non-zero exit now marks the job failed instead of re-queueing it.
    w.on('message', m => { const jj = jobs.get(job.key) || {};
      if (m.ok){ jj.status='done'; jj.traits=m.traits; process.stderr.write('[job] done '+job.key+'\n'); }
      else { jj.status='error'; jj.error=m.error; process.stderr.write('[job] error '+job.key+' '+m.error+'\n'); }
      jobs.set(job.key, jj); });
    w.on('error', e => { const jj=jobs.get(job.key)||{}; jj.status='error'; jj.error=String(e&&e.message||e); jobs.set(job.key,jj); process.stderr.write('[job] worker error '+job.key+' '+e+'\n'); });
    w.on('exit', (code) => { clearTimeout(killer); const jj=jobs.get(job.key)||{};
      if (code!==0 && jj.status!=='done' && jj.status!=='error' && !isReady(job.key)){   // worker died mid-render. MEASURED (16 Aug) across 11 logged failures: 6 died of MEMORY before the watchdog, 5 hit it. Render's own event says "Ran out of memory (used over 16GB)"; the memory graph shows each render as an isolated 2-15GB spike against a 16GB limit, with NO creep between renders. So the ceiling is capacity, not a leak.
        // THE PROGRESSIVE RESCUE IS GONE (16 Aug). It was added 25 Jul to escape a supposed infinite loop
        // in the static path. Measured over the 7 days to 15 Aug it fired 17 times and produced ZERO
        // pebbles — every attempt died after 18-24s of its 360s budget (it allocates ~23MB per frame, a
        // full-size canvas per frame, and walks off the 16GB ceiling in about twenty seconds). Worse, on
        // 8-9 Aug it took the WHOLE INSTANCE down nine times ("Ran out of memory (used over 16GB)"), and an
        // instance death kills every OTHER visitor's in-flight render too, not just this one. So it never
        // saved a single visitor and it cost bystanders. Failing straight away is strictly better: the
        // visitor sees the honest error ~40s sooner and the queue keeps moving for everyone behind them.
        // The engine still SUPPORTS progressive (engine.js window.__PROGRESSIVE__) — it is simply no longer
        // used as a rescue. What actually fails renders is memory: Render's own event log says
        // "Ran out of memory (used over 16GB)", and the memory graph shows every render as an isolated
        // 2-15GB spike against a 16GB limit, with NO creep between renders. Whether that is fixable by
        // lowering RENDER_H is still open and is being measured separately.
        // ONE PLAIN RETRY, SAME RESOLUTION (18 Aug). Two swap-controlled runs of IDENTICAL work peaked at
        // 5.74 GB and 7.53 GB - the algorithm's own demand swings ~1.8 GB run to run. Against a 16 GB
        // ceiling where production spikes reach 15 GB, that variance alone decides whether a marginal
        // pebble lives or dies, so the same pebble that just failed has a real chance on a second roll.
        // NOT a lower resolution: measured, cutting 44% of the pixels does not reduce peak memory at all
        // (h=2796 -> 5.74/7.53 GB, h=2097 -> 7.07/7.22 GB), which is why the §16 ladder failed in July.
        // NOT progressive: 0 pebbles in 17 attempts. Just one more throw of the same dice. Safe from
        // pile-up because the de-dupe above collapses the visitor's own retries onto this job.
        const tries = (job.tries||0) + 1;
        if (tries <= 1){
          jj.status='rendering'; jobs.set(job.key, jj);
          queue.unshift({ key:job.key, hash:job.hash, genome:job.genome, h:job.h, seq:job.seq, createdAt:job.createdAt, mint:job.mint, tries });
          process.stderr.write('[job] retry '+job.key+' same res (try '+tries+') — first attempt died, memory demand varies ~1.8GB between runs\n');
          active--; pump(); return;                                   // keep the inflight mapping: the job is NOT over
        }
        jj.status='error'; jj.error='render failed'; jobs.set(job.key, jj);
        process.stderr.write('[job] FAILED '+job.key+' — died twice (exit code '+code+')\n');
      }
      inflight.delete(fingerprint(job.hash, job.genome, job.h, job.mint));   // job is over (done or failed) - stop collapsing onto it
      active--; pump(); });
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
  "Forest Whisper":"Sussurro da Floresta","Serenity":"Serenidade","Dusk Riverbed":"Leito ao Anoitecer","Galactic Latte":"Latte Galático",
  "Desert Mirage":"Miragem do Deserto","Citrus Slate":"Ardósia Cítrica","Night Owl Doodles":"Rabiscos Noturnos",
  "Elephant Pajamas":"Pijama de Elefantinho","Salsa Blush":"Blush de Salsa","Magma Mambo":"Mambo de Magma","Lunar Chuckles":"Risadas Lunares"
};
// Portuguese size names (for the e-mail text). Filename uses palette only.
const SIZE_PT = { "Crumb":"Migalha","Pebbit":"Seixinho","Stonelet":"Pedrinha","Rockling":"Rochinha","Boulderette":"Matacãozinho","Craglet":"Penhasquinho","Mountlet":"Montículo" };
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
// PRIVATE visitor log (e-mails). Starts with '_' so /img can't serve it (sanitize strips '_') and /list ignores it (.jpg only). NÃO exposto publicamente.
const VISITORS_FILE = path.join(STORE, '_visitors.jsonl');
// PRIVATE recovery log: every render request AND every e-mail request, persisted the INSTANT it arrives (before the render runs).
// So if a render fails, we still have {hash,genome,mint} to re-render the EXACT pebble, and (for e-mail requests) the address to resend to.
// '_' prefix -> not served by /img and skipped by the TTL cleanup + /list. Exported only via token.
const RENDERS_FILE = path.join(STORE, '_renders.jsonl');
function logRec(rec){ try { fs.appendFileSync(RENDERS_FILE, JSON.stringify(Object.assign({ ts:new Date().toISOString() }, rec))+'\n'); } catch(e){} }
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
a{color:#d8b25a;text-decoration:none}
.ig{display:inline-block;margin-top:20px;color:#d8b25a;font-size:14px;font-weight:700;border:1px solid #d8b25a;border-radius:12px;padding:12px 20px;text-decoration:none}
.ig:active{opacity:.85}</style></head><body>
<div class="logo">Casa NUA · Domínio Público</div>
<div class="frame"><div class="prep" id="prep"><div class="spin"></div></div><img id="peb" alt=""></div>
<button class="btn" id="save"></button>
<div class="hint" id="hint"></div>
<a class="ig" id="ig" href="https://instagram.com/nua.casa" target="_blank" rel="noopener"></a>
<div class="foot" id="foot"></div>
<script>
var IMG=${JSON.stringify(img)}, STATUS=${JSON.stringify(status)}, NAME=${JSON.stringify(fileNameFor(key))};
var EN = !/^pt/i.test(navigator.language||navigator.userLanguage||'pt');
var S = EN ? {
  prep:'Preparing your Pebble in high resolution…', prepSub:'this may take a moment',
  queue:function(n){ return n+(n===1?' person':' people')+' ahead of you in line.<br><small style="opacity:.7">your Pebble is already being prepared…</small>'; },
  save:'Save to my Photos', revealHint:'Tap the button to save. You can also tap and hold the image.',
  err:'Couldn’t generate this Pebble.<br>Try generating another at the kiosk.',
  preparing:'Preparing…', shareHint:'Choose “Save Image” to keep it in your Photos.', dlHint:'Downloaded. Tap and hold the image to save to Photos.', holdHint:'Tap and hold the image above to save to your Photos.',
  ig:'Follow Casa NUA &nbsp;·&nbsp; <strong>@nua.casa</strong> &nbsp;→', foot:'Pebbles · by Zeblocks · CC0<br>Decentralized digital art · <a href="https://6529.io">6529</a>', title:'Your Pebble'
} : {
  prep:'Preparando seu Seixo em alta resolução…', prepSub:'isso pode levar um instante',
  queue:function(n){ return n+(n===1?' pessoa':' pessoas')+' na sua frente na fila.<br><small style="opacity:.7">seu Seixo já está sendo preparado…</small>'; },
  save:'Salvar nas minhas Fotos', revealHint:'Toque no botão para guardar. Você também pode tocar e segurar a imagem.',
  err:'Não foi possível gerar este Seixo.<br>Tente gerar outro no totem.',
  preparing:'Preparando…', shareHint:'Escolha “Salvar Imagem” para guardar nas Fotos.', dlHint:'Baixado. Toque e segure a imagem para guardar nas Fotos.', holdHint:'Toque e segure a imagem acima para salvar nas suas Fotos.',
  ig:'Siga a Casa NUA &nbsp;·&nbsp; <strong>@nua.casa</strong> &nbsp;→', foot:'SEIXOS (Pebbles) · por Zeblocks · CC0<br>Arte digital descentralizada · <a href="https://6529.io">6529</a>', title:'Seu Seixo'
};
document.documentElement.lang = EN?'en':'pt-BR';
var prep=document.getElementById('prep'), peb=document.getElementById('peb'), btn=document.getElementById('save'), hint=document.getElementById('hint');
peb.alt=S.title; btn.textContent=S.save; document.getElementById('ig').innerHTML=S.ig; document.getElementById('foot').innerHTML=S.foot;
function setPrep(){ prep.innerHTML='<div class="spin"></div>'+S.prep+'<br><small style="opacity:.7">'+S.prepSub+'</small>'; }
setPrep();
function reveal(){ peb.onload=function(){ prep.style.display='none'; peb.style.display='block'; btn.style.display='block'; hint.textContent=S.revealHint; }; peb.src=IMG+'?t='+Date.now(); }
var pollStart=Date.now();
function poll(){ fetch(STATUS,{cache:'no-store'}).then(function(r){return r.json();}).then(function(j){
  if(j.ready){ if(j.filename) NAME=j.filename; reveal(); }
  else if(j.status==='error' || (j.status==='unknown'&&Date.now()-pollStart>15000) || Date.now()-pollStart>660000){ prep.innerHTML=S.err; }   // failed, lost (server restarted), or stuck >11min. Was 5min, which declared failure on renders that were still healthily working (observed tail 429s) - the status 'error' branch above is the REAL signal; this wall-clock is only a backstop for a lost job.
  else { prep.innerHTML = (j.ahead>0) ? ('<div class="spin"></div>'+S.queue(j.ahead)) : null; if(!(j.ahead>0)) setPrep(); setTimeout(poll, 2500); }
}).catch(function(){ setTimeout(poll, 3500); }); }
poll();
btn.addEventListener('click', async function(){ hint.textContent=S.preparing;
  try{ var resp=await fetch(IMG,{mode:'cors'}); var blob=await resp.blob(); var file=new File([blob],NAME,{type:'image/jpeg'});
    if(navigator.canShare && navigator.canShare({files:[file]})){ await navigator.share({files:[file],title:S.title}); hint.textContent=S.shareHint; }
    else { var a=document.createElement('a'); a.href=URL.createObjectURL(blob); a.download=NAME; document.body.appendChild(a); a.click(); a.remove(); hint.textContent=S.dlHint; }
  }catch(e){ hint.textContent=S.holdHint; } });
</script></body></html>`;
}

function emailHtml(base, key, traits, lang){
  const EN = lang==='en';
  const img = base+'/img/'+key;
  const pal = (traits && traits.Palette) ? (EN ? traits.Palette : (PALETTE_PT[traits.Palette]||traits.Palette)) : '';
  const sz  = (traits && traits.Size)    ? (EN ? traits.Size    : (SIZE_PT[traits.Size]||traits.Size))       : '';
  const detail = (pal||sz) ? (' (' + (EN
      ? [pal?('palette '+pal):'', sz?('size '+sz):''].filter(Boolean).join(', ')
      : [pal?('paleta '+pal):'', sz?('tamanho '+sz):''].filter(Boolean).join(', ')) + ')') : '';
  const t = EN ? {
    h1:'Your Pebble has arrived 🪨',
    p1:`Thank you for visiting the <strong>Domínio Público gallery</strong> at <strong>Casa NUA</strong> in Formosa, in the heart of São Paulo. You just generated a unique Pebble${detail} — it is attached to this e-mail in <strong>high resolution</strong>.`,
    h2a:'What you just created',
    p2:`<strong>Pebbles</strong> is a generative <strong>on-chain</strong> artwork by <strong>Zeblocks</strong>, in the <strong>public domain (CC0)</strong> — free for anyone to use, remix, print, and build upon, without asking permission. The algorithm that draws each Pebble lives on the Ethereum blockchain, and each combination is unique. The <strong>1,000 Pebbles</strong> you saw in the exhibition are the original NFTs, but the algorithm allows the creation of <strong>infinite</strong> new works like the one you just created, always unique!`,
    h2b:'About Casa NUA',
    p3:`Casa NUA is a decentralized digital art museum in São Paulo. At <strong>Galeria Domínio Público</strong> we exhibit exclusively <strong>CC0</strong> NFT art, in partnership with the <strong>6529</strong> network — a movement for truly decentralized, public-domain art and ownership.`,
    h2c:'Follow Casa NUA on Instagram',
    p4:`Follow exhibitions, artists, and behind-the-scenes at <a href="https://instagram.com/nua.casa" style="color:#9a8038"><strong>@nua.casa</strong></a>.`,
    foot:'Casa NUA · Domínio Público · São Paulo · This Pebble is CC0 — it is yours to keep, print, and share.',
    imgAlt:'Your Pebble'
  } : {
    h1:'Seu Seixo chegou 🪨',
    p1:`Obrigado por visitar a <strong>galeria Domínio Público</strong> da <strong>Casa NUA</strong> na Formosa, no coração de São Paulo. Você acabou de gerar um Seixo único${detail} — ele está em <strong>alta resolução</strong> em anexo neste e-mail.`,
    h2a:'O que você acabou de criar',
    p2:`<strong>Seixos (Pebbles)</strong> é uma obra generativa <strong>on-chain</strong> de <strong>Zeblocks</strong>, em <strong>domínio público (CC0)</strong> — livre para qualquer pessoa usar, remixar, imprimir e construir em cima, sem pedir permissão. O algoritmo que desenha cada Seixo vive na blockchain Ethereum, e cada combinação é única. As <strong>1.000 Pebbles</strong> que você viu na exposição são os NFTs originais, mas o algoritmo permite a criação de <strong>infinitas</strong> novas obras como a que você acabou de criar, sempre únicas!`,
    h2b:'Sobre a Casa NUA',
    p3:`A Casa NUA é um museu de arte digital descentralizada em São Paulo. Na <strong>Galeria Domínio Público</strong> exibimos exclusivamente arte NFT em <strong>CC0</strong>, em parceria com a rede <strong>6529</strong> — um movimento por arte e propriedade verdadeiramente descentralizadas e de domínio público.`,
    h2c:'Siga a Casa NUA no Instagram',
    p4:`Acompanhe exposições, artistas e bastidores em <a href="https://instagram.com/nua.casa" style="color:#9a8038"><strong>@nua.casa</strong></a>.`,
    foot:'Casa NUA · Domínio Público · São Paulo · Este Seixo é CC0 — é seu para guardar, imprimir e compartilhar.',
    imgAlt:'Seu Seixo'
  };
  return `<div style="font-family:-apple-system,Helvetica,Arial,sans-serif;max-width:560px;margin:0 auto;color:#1d1d1f;line-height:1.55">
  <p style="font-size:12px;letter-spacing:.18em;text-transform:uppercase;color:#9a8038;margin:0 0 6px">Casa NUA · Domínio Público</p>
  <h1 style="font-size:23px;margin:0 0 14px">${t.h1}</h1>
  <p>${t.p1}</p>
  <p style="text-align:center;margin:18px 0"><img src="${img}" alt="${t.imgAlt}" style="width:100%;max-width:420px;border-radius:10px"/></p>
  <h2 style="font-size:17px;margin:24px 0 8px">${t.h2a}</h2>
  <p>${t.p2}</p>
  <h2 style="font-size:17px;margin:24px 0 8px">${t.h2b}</h2>
  <p>${t.p3}</p>
  <h2 style="font-size:17px;margin:24px 0 8px">${t.h2c}</h2>
  <p>${t.p4}</p>
  <p style="text-align:center;margin:14px 0"><a href="https://instagram.com/nua.casa"><img src="${base}/ig-qr.png" alt="Instagram @nua.casa — Casa NUA" style="width:190px;height:190px;border-radius:14px"/></a></p>
  <p style="margin-top:18px">🔗 <a href="https://dominiopublico.nua.casa" style="color:#9a8038">dominiopublico.nua.casa</a> &nbsp;·&nbsp; <a href="https://6529.io" style="color:#9a8038">6529.io</a></p>
  <p style="font-size:12px;color:#9b9b9b;margin-top:26px;border-top:1px solid #eee;padding-top:14px">${t.foot}</p>
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

  if (u.pathname === '/visitors.csv' || u.pathname === '/visitors.jsonl') {   // PROTECTED export of visitor e-mails (token required) — for tabulation/invites
    if (!ADMIN_TOKEN || u.searchParams.get('token') !== ADMIN_TOKEN) { cors(res); res.writeHead(403); return res.end('forbidden'); }
    let lines=[]; try { lines = fs.readFileSync(VISITORS_FILE,'utf8').split('\n').filter(Boolean); } catch(e){}
    if (u.pathname === '/visitors.jsonl') { cors(res); res.writeHead(200,{'content-type':'application/x-ndjson; charset=utf-8'}); return res.end(lines.join('\n')); }
    const esc = s => '"'+String(s==null?'':s).replace(/"/g,'""')+'"';
    const rows = ['seq,data_hora_sao_paulo,email,paleta,tamanho,arquivo,key'];
    for (const ln of lines) { let r; try { r=JSON.parse(ln); } catch(e){ continue; }
      let dt=''; try { dt = new Date(r.createdAt||r.ts).toLocaleString('pt-BR',{timeZone:'America/Sao_Paulo'}); } catch(e){ dt=r.createdAt||r.ts||''; }
      rows.push([esc(r.seq), esc(dt), esc(r.email), esc(r.palette), esc(r.size), esc(r.filename), esc(r.key)].join(',')); }
    cors(res); res.writeHead(200, {'content-type':'text/csv; charset=utf-8'}); return res.end('\ufeff'+rows.join('\n'));
  }

  if (u.pathname === '/renders.jsonl') {                              // PROTECTED recovery export (token) \u2014 every render + e-mail request incl. the ones whose render FAILED
    if (!ADMIN_TOKEN || u.searchParams.get('token') !== ADMIN_TOKEN) { cors(res); res.writeHead(403); return res.end('forbidden'); }
    let lines=[]; try { lines = fs.readFileSync(RENDERS_FILE,'utf8').split('\n').filter(Boolean); } catch(e){}
    cors(res); res.writeHead(200,{'content-type':'application/x-ndjson; charset=utf-8'}); return res.end(lines.join('\n'));
  }

  if (u.pathname === '/list') {                                       // archive listing for the local pull script
    let files = []; try { files = fs.readdirSync(STORE).filter(f => /\.jpg$/.test(f)); } catch(e){}
    if (u.searchParams.get('format') === 'json') {
      const items = files.map(f => { let mtime=0, meta={};
        try { mtime = fs.statSync(path.join(STORE,f)).mtimeMs; } catch(e){}
        try { meta = JSON.parse(fs.readFileSync(path.join(STORE,f+'.json'),'utf8')); } catch(e){}
        return { key:f, filename: fileNameFor(f), seq: meta.seq, createdAt: meta.createdAt, mtime, traits: meta.traits }; }).sort((a,b)=>a.mtime-b.mtime);
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
    let ahead = 0;                                                   // how many renders are ahead of this one (queue position) — for the "wait in line" message
    if (!ready) { const qi = queue.findIndex(q=>q.key===key); ahead = qi>=0 ? (qi + active) : 0; }
    return json(res, 200, { ready, status: ready ? 'done' : (j ? j.status : 'unknown'), ahead, filename: ready ? fileNameFor(key) : undefined, error: (j && j.status==='error') ? j.error : undefined });
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
      const already = inflightKey(b.hash, b.genome, H, b.mint);      // same pebble already rendering? re-attach, don't pile up
      if (already) { const base0 = baseUrl(req);
        process.stderr.write('[job] collapse duplicate onto '+already+'\n');
        return json(res, 202, { key: already, url: base0+'/img/'+already, viewUrl: base0+'/view/'+already, statusUrl: base0+'/status/'+already, status:'rendering', h:H }); }
      const key = crypto.randomBytes(8).toString('hex') + '.jpg';
      startJob(key, b.hash, b.genome, H, b.mint);                            // returns immediately; render runs in a worker (mint gates special palettes)
      logRec({ type:'render', key, hash:b.hash, genome:b.genome, mint:b.mint||'MemeMaxis', h:H });   // recovery: persist the pebble's params NOW, before it renders
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
        const H2 = Math.max(200, Math.min(parseInt(b.h||DEFAULT_H,10), MAX_H));
        key = inflightKey(b.hash, b.genome, H2, b.mint) || (crypto.randomBytes(8).toString('hex') + '.jpg');
        if (!jobs.get(key)) startJob(key, b.hash, b.genome, H2, b.mint);
      }
      logRec({ type:'email', key, email:b.to, hash:b.hash||null, genome:b.genome||null, mint:b.mint||'MemeMaxis', lang:b.lang||null });   // recovery: capture the address + params BEFORE the render wait — so if the render fails we can still re-render and resend
      await waitForKey(key, 600000);                                         // MEASURED 16 Aug: median render+queue is 130s but the tail reaches 429s. The old 4-min cap was ABANDONING renders that then finished correctly and sat in the archive while the visitor had been told it failed (7 of 71 pebbles). 10 min covers the observed tail.
      const base = baseUrl(req);
      const content = fs.readFileSync(path.join(STORE, key)).toString('base64');
      const r = await fetch('https://api.resend.com/emails', { method:'POST',
        headers:{ 'authorization':'Bearer '+RESEND_KEY, 'content-type':'application/json' },
        body: JSON.stringify({ from: FROM_EMAIL, to: b.to, subject: (b.lang==='en' ? 'Your Pebble — Casa NUA · Domínio Público' : 'Seu Seixo — Casa NUA · Domínio Público'),
          html: emailHtml(base, key, traitsFor(key), b.lang), attachments: [{ filename: fileNameFor(key), content }] }) });
      const j = await r.json().catch(()=>({}));
      if (!r.ok) { process.stderr.write('[email] resend fail '+JSON.stringify(j)+'\n'); return json(res, 502, { error:'resend failed', detail:j }); }
      try {                                                            // PRIVATE visitor log (for tabulation / invites) — not exposed publicly
        let meta={}; try { meta = JSON.parse(fs.readFileSync(path.join(STORE, key+'.json'),'utf8')); } catch(e){}
        const rec = { ts:new Date().toISOString(), createdAt:meta.createdAt||null, seq:(meta.seq!=null?meta.seq:null), key,
          email:b.to, palette:(meta.traits&&meta.traits.Palette)||null, size:(meta.traits&&meta.traits.Size)||null, filename:fileNameFor(key) };
        fs.appendFileSync(VISITORS_FILE, JSON.stringify(rec)+'\n');
      } catch(e){ process.stderr.write('[visitors] '+String(e)+'\n'); }
      return json(res, 200, { ok:true, key, viewUrl: base+'/view/'+key });
    } catch (e) { process.stderr.write('[email] ERROR '+String(e&&e.stack||e)+'\n'); return json(res, 500, { error: String(e && e.message || e) }); }
  }
  json(res, 404, { error:'not found' });
});
server.listen(PORT, () => console.error('seixos-render (async) on :'+PORT+' H='+DEFAULT_H+' maxConc='+MAX_CONC+' heap='+HEAP_MB+'MB TTL='+TTL_HOURS));
