// Measures result-cell HTML generation only, not DOM insertion or SQL.
// Compare with: node result-cells.bench.cjs /path/to/baseline/app.js
const { performance } = require('node:perf_hooks');
const { createHarness } = require('./result-cells.harness.cjs');
const h = createHarness(process.argv[2]);
for (const mode of ['plain', 'mixed', 'escaped']) {
    // One page of 100 rows and 10 columns, representative of the default.
    const values = Array.from({ length: 1000 }, (_, i) => {
        if (mode === 'escaped') return `Item <${i}> & "quoted"`;
        if (mode === 'plain') return `Product ${i} in Munich`;
        if (i % 10 === 0) return null;
        if (i % 10 <= 3) return i;
        if (i % 10 === 4) return `Item <${i}>`;
        return `Product ${i} in Munich`;
    });
    function run() { let len = 0; for (const value of values) len += h.format(value).length; return len; }
    for (let i = 0; i < 20; i++) run();
    const samples = [];
    for (let i = 0; i < 70; i++) {
        const start = performance.now();
        if (!run()) throw new Error('no rendered output');
        samples.push(performance.now() - start);
    }
    samples.sort((a, b) => a - b);
    console.log(`${mode}: ${samples[35].toFixed(3)} ms median (70 pages)`);
}
