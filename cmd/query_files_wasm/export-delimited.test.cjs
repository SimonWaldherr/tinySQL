const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

const source = fs.readFileSync(`${__dirname}/app.js`, 'utf8');
const start = source.indexOf('function exportCSV(');
const end = source.indexOf('// Export an actual workbook', start);
assert.ok(start >= 0 && end > start);
let download;
const sandbox = {downloadFile: (data, name, mime) => { download = {data, name, mime}; }, alert() {}};
vm.runInNewContext(`${source.slice(start, end)}; globalThis.csv = exportCSV; globalThis.tsv = exportTSV`, sandbox);

test('CSV quotes headers and fields containing delimiters, quotes and line endings', () => {
    const columns = ['last,first', '"note"', 'line\rbreak', 'value'];
    const rows = [{ 'last,first': 'Doe,Jane', '"note"': 'say "hi"', 'line\rbreak': 'a\rb', value: 'x\ny' }];
    sandbox.csv({columns, rows});
    assert.equal(download.name, 'query_results.csv');
    assert.equal(download.data, '"last,first","""note""","line\rbreak",value\n"Doe,Jane","say ""hi""","a\rb","x\ny"\n');
});

test('TSV flattens tabs and all line endings in headers and fields', () => {
    const columns = ['a\tb', 'c\rd', 'e\r\nf'];
    const rows = [{'a\tb': 'one\rtwo', 'c\rd': 'three\nfour', 'e\r\nf': 'five\r\nsix'}];
    sandbox.tsv({columns, rows});
    assert.equal(download.name, 'query_results.tsv');
    assert.equal(download.data, 'a b\tc d\te f\none two\tthree four\tfive six');
});
