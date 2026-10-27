// Run with `node result-view.bench.cjs [path/to/app.js]` to compare revisions.
// It measures the first client-side sort of 10,000 existing rows, not DOM work
// or reuse of the already cached view.
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const appPath = process.argv[2] || path.join(__dirname, 'app.js');
const source = fs.readFileSync(appPath, 'utf8');
const start = source.indexOf('let visibleResultsCache = null;');
const end = source.indexOf('let resultFilterTimer = null;', start);
if (start < 0 || end < 0) throw new Error('Could not find result view code');
const sandbox = {resultViewState: {filterText: '', sortColumn: 'value', sortDirection: 'asc'}, currentResults: null};
vm.runInNewContext(`${source.slice(start, end)}\nglobalThis.visible = getVisibleResults;`, sandbox);

for (const kind of ['number', 'number-string', 'text', 'mixed']) {
    const rows = Array.from({length: 10000}, (_, i) => {
        const number = (i * 7919) % 10000;
        const value = kind === 'number' ? number
            : kind === 'number-string' ? String(number)
                : kind === 'text' ? `Item ${number}`
                    : i % 9 === 0 ? null : i % 3 === 0 ? String(number) : i % 3 === 1 ? `Item ${number}` : number;
        return {value};
    });
    // A new source object represents a fresh result and bypasses the view cache.
    const run = () => sandbox.visible({columns: ['value'], rows});
    for (let i = 0; i < 4; i++) run();
    const times = [];
    for (let i = 0; i < 15; i++) {
        const started = performance.now();
        run();
        times.push(performance.now() - started);
    }
    times.sort((a, b) => a - b);
    console.log(`${kind}: ${times[7].toFixed(3)} ms median (15 runs)`);
}
