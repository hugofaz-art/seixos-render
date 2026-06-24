// Headless Pebbles renderer — runs the exact on-chain generative script via @napi-rs/canvas.
const napi = require('@napi-rs/canvas');
const fs = require('fs');

const RAW = fs.readFileSync(__dirname + '/pebbles_script.js', 'utf8');

function patch(s){
  s = s.replace(/^let hash='[^']*'/, "let hash=window.__SEED__");
  s = s.replace(",d=1.294,", ",d=window.__ASPECT__,");
  s = s.replace('t.get("render_static")', '"false"');                         // progressive (rAF) path — we drain it synchronously
  s = s.replace('l&&M.putImageData(W,0,0),console.log("done")', 'M.putImageData(W,0,0),console.log("done")');
  s = s.replace('t.get("height")', 'String(window.__H__||0)');
  s = s.replace("function o(){return i^=i<<13,i^=i>>17,i^=i<<5,(i<0?1+~i:i)%1e3/1e3}",
    "function o(){i^=i<<13,i^=i>>17,i^=i<<5;var _r=(i<0?1+~i:i)%1e3/1e3;window.__OC__=(window.__OC__|0)+1;var f=window.__FORCED__;return(f&&f[window.__OC__]!==undefined)?f[window.__OC__]:_r}");
  s = s.replace("let Q=[[function", "window.__TRAITS__=Object.assign({},traits);let Q=[[function");
  return s;
}
const PATCHED = patch(RAW);

function makeStubEl(){ return new Proxy({ style:{}, children:[] }, {
  get(t,k){ if(k in t) return t[k]; if(k==='innerText'||k==='innerHTML'||k==='textContent') return ''; return ()=>{}; },
  set(t,k,v){ t[k]=v; return true; }
}); }

// One-time global setup
global.Path2D   = napi.Path2D;
global.DOMMatrix= napi.DOMMatrix;
global.DOMPoint = napi.DOMPoint;
global.ImageData= napi.ImageData;
global.Image    = napi.Image;

let RAF=[]; let MAIN=null; const allCanvases=[];
global.requestAnimationFrame = (cb)=>{ RAF.push(cb); return RAF.length; };
global.cancelAnimationFrame = ()=>{};

global.document = {
  location: { search: '' },
  createElement(tag){
    if(String(tag).toLowerCase()==='canvas'){ const c=napi.createCanvas(300,150); allCanvases.push(c); return c; }
    return makeStubEl();
  },
  createElementNS(){ return makeStubEl(); },
  body: { appendChild(n){ if(n && typeof n.getContext==='function') MAIN=n; }, style:{}, },
  head: { appendChild(){} },
  querySelector(sel){ return MAIN || allCanvases[allCanvases.length-1] || null; },
  querySelectorAll(){ return []; },
  getElementById(){ return null; },
  addEventListener(){}, removeEventListener(){},
};
global.window = global;
global.location = global.document.location;
global.navigator = { userAgent:'node' };
global.devicePixelRatio = 1;

const FN = new Function(PATCHED);   // compiled once

// Render one pebble. genome = {1:..,2:..,...}; hash = '0x...'; h = target height px.
// async: yields to the event loop periodically so a long render never blocks the server (health checks keep passing)
// and forced GC can run, keeping peak RSS down. Logs rss to stderr for diagnosis.
async function render(hash, genome, h, aspect=1.294){
  RAF = []; MAIN = null; allCanvases.length = 0;
  window.__SEED__ = hash;
  window.__FORCED__ = genome;
  window.__ASPECT__ = aspect;
  window.__H__ = h;
  window.__OC__ = 0;
  window.__TRAITS__ = null;
  let done = false;
  const prevLog = console.log;
  const elog = (...a)=>{ try{ process.stderr.write('[engine] '+a.join(' ')+'\n'); }catch(e){} };
  global.console.log = (m)=>{ if(m==='done') done=true; };
  const t0=Date.now();
  elog('start h='+h+' rss(MB)='+Math.round(process.memoryUsage().rss/1048576));
  try { FN(); } catch(e){ /* progressive path schedules rAF; ignore sync throw */ }
  let guard=0;
  while(RAF.length && !done && guard++ < 5_000_000){
    const cb=RAF.shift();
    try{ cb(performance.now()); }catch(e){ global.console.log=prevLog; elog('THROW '+(e&&e.message)); throw e; }
    if((guard & 63)===0){
      if(global.gc) global.gc();
      await new Promise(r=>setImmediate(r));               // yield: keep event loop responsive + let GC reclaim
      if((guard & 16383)===0) elog('frame='+guard+' rss(MB)='+Math.round(process.memoryUsage().rss/1048576)+' done='+done+' ms='+(Date.now()-t0));
    }
  }
  if(global.gc) global.gc();
  global.console.log = prevLog;
  const cv = MAIN || allCanvases[allCanvases.length-1];
  elog('FINISH frames='+guard+' done='+done+' canvas='+(cv&&(cv.width+'x'+cv.height))+' rss(MB)='+Math.round(process.memoryUsage().rss/1048576)+' ms='+(Date.now()-t0));
  return { canvas: cv, traits: window.__TRAITS__, done, frames: guard, w: cv&&cv.width, hgt: cv&&cv.height };
}
module.exports = { render };
