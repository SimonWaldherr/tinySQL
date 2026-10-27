// Isolates preparation of a workbook without XLSX serialization or DOM work.
// Usage: node export-xlsx.bench.cjs [path/to/app.js]
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const source = fs.readFileSync(process.argv[2] || path.join(__dirname, 'app.js'), 'utf8');
const start = source.indexOf('function exportXLSX(');
const end = source.indexOf('function exportMarkdown(', start);
if (start < 0 || end < 0) throw new Error('XLSX export not found');
const sandbox = { window: { XLSX: {
    utils: { aoa_to_sheet: () => ({}), book_new: () => ({}), book_append_sheet() {} },
    write: () => new Uint8Array(),
} }, downloadFile() {}, alert(message) { throw new Error(message); }, showToast(message) { throw new Error(message); } };
vm.runInNewContext(`${source.slice(start, end)}\nglobalThis.run = exportXLSX;`, sandbox);
const columns = Array.from({ length: 80 }, (_, i) => `Column${i}`);
for (const kind of ['short', 'wide']) {
    const rows = Array.from({ length: 3000 }, (_, index) => {
        const row = {};
        for (const column of columns) row[column] = kind === 'wide' ? 'A'.repeat(64) : `value${index % 20}`;
        return row;
    });
    const visible = { columns, rows };
    for (let i = 0; i < 3; i++) sandbox.run(visible);
    const times = [];
    for (let i = 0; i < 11; i++) {
        const start = performance.now();
        sandbox.run(visible);
        times.push(performance.now() - start);
    }
    times.sort((a, b) => a - b);
    console.log(`${kind}: ${times[5].toFixed(3)} ms median (11 runs)`);
}
