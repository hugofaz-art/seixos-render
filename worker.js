// Worker thread: runs ONE heavy Pebbles render off the main event loop, writes the JPEG, then exits.
// Keeps the HTTP server responsive (health/img/view/status keep answering) during the ~1-2 min 4K render.
const { parentPort, workerData } = require('worker_threads');
const fs = require('fs');
const path = require('path');
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
