const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

function loadExport() {
    const source = fs.readFileSync(`${__dirname}/app.js`, 'utf8');
    const start = source.indexOf('function exportXLSX(');
    const end = source.indexOf('function exportMarkdown(', start);
    assert.ok(start >= 0 && end > start);
    let captured;
    const sandbox = {
        window: { XLSX: {
            utils: {
                aoa_to_sheet(rows) { captured = { rows }; return captured; },
                book_new() { return {}; }, book_append_sheet() {},
            },
            write() { return new Uint8Array(); },
        } },
        downloadFile() {}, alert(message) { throw new Error(message); },
        showToast(message) { throw new Error(message); },
    };
    vm.runInNewContext(`${source.slice(start, end)}\nglobalThis.exportXLSX = exportXLSX;`, sandbox);
    return { run: (visible) => { sandbox.exportXLSX(visible); return captured; } };
}

test('XLSX keeps formula protection, types, and sizing of the first 999 data rows', () => {
    const columns = ['name', 'formula', 'number', 'missing', 'long'];
    const rows = Array.from({ length: 1001 }, (_, i) => ({
        name: i === 998 ? 'a'.repeat(31) : i === 999 ? 'z'.repeat(45) : 'short',
        formula: i === 0 ? '=1+1' : '@x', number: i === 0 ? 17 : false,
        missing: i === 0 ? null : undefined, long: 'x'.repeat(80),
    }));
    const sheet = loadExport().run({ columns, rows });
    assert.equal(sheet.rows.length, 1002);
    assert.deepEqual(Array.from(sheet.rows[1]), ['short', "'=1+1", 17, '', 'x'.repeat(80)]);
    assert.deepEqual(Array.from(sheet.rows[2]), ['short', "'@x", false, '', 'x'.repeat(80)]);
    assert.deepEqual(Array.from(sheet['!cols'], ({ wch }) => wch), [33, 10, 10, 10, 48]);
});

test('XLSX sizes Unicode headers and distinguishes the 999th and 1000th rows', () => {
    const columns = ['Überschrift', 'b'];
    const rows = Array.from({ length: 1000 }, (_, i) => ({
        Überschrift: i === 998 ? 'included length' : 'x',
        b: i === 999 ? 'omitted width beyond the limit' : '',
    }));
    const sheet = loadExport().run({ columns, rows });
    assert.deepEqual(Array.from(sheet['!cols'], ({ wch }) => wch), [17, 10]);
});
