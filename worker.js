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
    // NEVER DELIVER AN UNFINISHED RENDER (16 Aug). The on-chain script can throw part-way through;
    // engine.js used to swallow that, leaving a canvas holding only the BACKGROUND pass while this
    // file happily encoded and wrote it. The result was a full-size 2160x2796 JPEG of ~68 KB (a real
    // pebble is ~1569 KB) delivered to the visitor as a success: file written, counter advanced, no
    // gap in the sequence, e-mail sent. Measured over the 103 most recent pebbles, 21 of them (20%)
    // were these. They are invisible to every check we have because nothing looks at whether the
    // render actually FINISHED. r.done is that signal - the on-chain script logs "done" only when it
    // completes - so refuse anything without it. An honest error beats a picture of nothing.
    if (!r.done) throw new Error('render did not complete (frames=' + r.frames + ') - refusing to deliver a background-only image');
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
