// Compare by running with a path to an earlier app.js and then without one.
// Measures one input followed by its keyup, using a lightweight mocked DOM.
const { performance } = require('node:perf_hooks');
const { createHarness } = require('./editor-highlight-harness.cjs');
const h = createHarness(process.argv[2]);
h.setup();
const initial = Array.from({ length: 200 }, (_, i) => `SELECT ${i} AS id, 'value ${i}' AS name FROM data WHERE id > ${i};`).join('\n');
const samples = [];
for (let i = 0; i < 150; i++) {
    h.editor.value = `${initial}\n-- edit ${i}`;
    h.editor.selectionStart = h.editor.value.length;
    const start = performance.now();
    h.emit('input');
    h.emit('keyup');
    const elapsed = performance.now() - start;
    if (i >= 25) samples.push(elapsed);
}
samples.sort((a, b) => a - b);
console.log(`input + keyup: ${samples[Math.floor(samples.length / 2)].toFixed(3)} ms median (125 runs), ${h.elements.sqlHighlight.writes} overlay writes`);
