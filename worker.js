// Render CHILD PROCESS: the server FORKS one of these per job (child_process.fork). It runs ONE heavy
// Pebbles render, writes the JPEG, delivers the result, then EXITS. Because each render runs in its own
// short-lived OS process, the operating system reclaims ALL of its memory on exit — including the large
// NATIVE (off-heap) buffers @napi-rs/canvas allocates at 4K. That makes memory-creep across renders
// impossible (the old worker_threads model kept those natives in the single shared server process, which
// crept upward over days until it OOM'd). Concurrency is still 1 (the server only forks one at a time).
const fs = require('fs');
const path = require('path');
// The parent passes --expose-gc in execArgv, so global.gc() exists here and the engine's periodic GC keeps
// the peak memory of a SINGLE render down (a heavy 4K render must still fit in RAM by itself).
const { render } = require('./engine');

process.on('message', async (job) => {
  const { hash, genome, h, key, storeDir, quality, seq, createdAt, mint, progressive } = job;
  let out;
  try {
    const r = await render(hash, genome, h, 1.294, mint || 'MemeMaxis', !!progressive);
    if (!r || !r.canvas) throw new Error('render produced no canvas');
    const buf = await r.canvas.encode('jpeg', Math.round((quality || 0.92) * 100));
    const tmp = path.join(storeDir, key + '.tmp');
    fs.writeFileSync(tmp, buf);
    fs.renameSync(tmp, path.join(storeDir, key));            // atomic: file only appears complete
    try { fs.writeFileSync(path.join(storeDir, key + '.json'), JSON.stringify({ traits: r.traits, w: r.w, h: r.hgt, seq: seq, createdAt: createdAt })); } catch (e) {}
    out = { ok: true, traits: r.traits, w: r.w, h: r.hgt, bytes: buf.length };
  } catch (e) {
    out = { ok: false, error: String(e && e.message || e) };
  }
  // send the result, THEN exit (in the flush callback) so the parent reliably gets the message before the
  // process dies and the OS frees every byte this render used.
  try { process.send(out, () => process.exit(0)); }
  catch (e) { process.exit(0); }
});
