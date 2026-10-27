// Measure cursor-only keyup in the production editor code with a mock DOM.
// Run with [path/to/app.js] to compare another version.
const { performance } = require('node:perf_hooks');
const { createHarness } = require('./editor-highlight-harness.cjs');

for (const lines of [10, 200, 5000]) {
    const h = createHarness(process.argv[2]);
    h.editor.value = Array.from({ length: lines }, (_, i) => `SELECT ${i} AS id, 'sample' AS name;`).join('\n');
    h.editor.selectionStart = Math.floor(h.editor.value.length / 2);
    h.setup();
    for (let i = 0; i < 1000; i++) h.emit('keyup');
    const samples = [];
    for (let run = 0; run < 9; run++) {
        const start = performance.now();
        for (let i = 0; i < 1000; i++) h.emit('keyup');
        samples.push((performance.now() - start) * 1000 / 1000);
    }
    samples.sort((a, b) => a - b);
    console.log(`${lines} lines: ${samples[4].toFixed(2)} µs per cursor keyup (median of 9)`);
    const base = h.editor.value;
    const changed = [];
    for (let run = 0; run < 9; run++) {
        const start = performance.now();
        for (let i = 0; i < 100; i++) {
            h.editor.value = `${base}${i}`;
            h.lineCount();
        }
        changed.push((performance.now() - start) * 1000 / 100);
    }
    changed.sort((a, b) => a - b);
    console.log(`${lines} lines: ${changed[4].toFixed(2)} µs per changed-text position update (median of 9)`);
}
