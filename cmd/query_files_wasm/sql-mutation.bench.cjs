// Compare with: node sql-mutation.bench.cjs [path/to/app.js]
// Measures only change detection, not the query or snapshot itself.
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const source = fs.readFileSync(process.argv[2] || path.join(__dirname, 'app.js'), 'utf8');
const start = source.indexOf('function sqlMayMutate(');
const end = source.indexOf('// The legacy path', start);
if (start < 0 || end <= start) throw new Error('Cannot find sqlMayMutate');
const sandbox = {};
vm.runInNewContext(`${source.slice(start, end)}; globalThis.classify = sqlMayMutate`, sandbox);
for (const [name, sql] of [
    ['select', 'SELECT id, title, price FROM products WHERE price > 10 ORDER BY title LIMIT 100'],
    ['large_literal', `SELECT '${'x'.repeat(10000)}' AS text`],
    ['cte_select', 'WITH x AS (SELECT 1) SELECT * FROM x'],
]) {
    const iterations = 10000;
    for (let i = 0; i < 1000; i++) sandbox.classify(sql);
    const times = [];
    for (let run = 0; run < 5; run++) {
        const startTime = performance.now();
        for (let i = 0; i < iterations; i++) sandbox.classify(sql);
        times.push(performance.now() - startTime);
    }
    times.sort((a, b) => a - b);
    console.log(`${name}: ${(times[2] * 1e6 / iterations).toFixed(0)} ns/op (median of 5)`);
}
