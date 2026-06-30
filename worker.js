// Worker thread: runs ONE heavy Pebbles render off the main event loop, writes the JPEG, then exits.
// Keeps the HTTP server responsive (health/img/view/status keep answering) during the ~1-2 min 4K render.
const { parentPort, workerData } = require('worker_threads');
const fs = require('fs');
const path = require('path');
// Enable manual GC inside the worker. @napi-rs/canvas holds large NATIVE (off-heap) buffers per transient canvas;
// the engine makes many per frame at 4K. Without forced GC those natives pile up and OOM the instance.
// worker_threads reject --expose-gc in execArgv, but this runtime trick exposes global.gc anyway, so the
// engine's periodic global.gc() calls actually run and keep peak RSS down.
try { require('v8').setFlagsFromString('--expose-gc'); global.gc = require('vm').runInNewContext('gc'); } catch (e) {}
const { render } = require('./engine');

(async () => {
  const { hash, genome, h, key, storeDir, quality, seq, createdAt, mint } = workerData;
  try {
    const r = await render(hash, genome, h, 1.294, mint || 'MemeMaxis');
    if (!r || !r.canvas) throw new Error('render produced no canvas');
    const buf = await r.canvas.encode('jpeg', Math.round((quality || 0.92) * 100));
    const tmp = path.join(storeDir, key + '.tmp');
    fs.writeFileSync(tmp, buf);
    fs.renameSync(tmp, path.join(storeDir, key));            // atomic: file only appears complete
    try { fs.writeFileSync(path.join(storeDir, key + '.json'), JSON.stringify({ traits: r.traits, w: r.w, h: r.hgt, seq: seq, createdAt: createdAt })); } catch (e) {}
    parentPort.postMessage({ ok: true, traits: r.traits, w: r.w, h: r.hgt, bytes: buf.length });
  } catch (e) {
    parentPort.postMessage({ ok: false, error: String(e && e.message || e) });
  }
})();
