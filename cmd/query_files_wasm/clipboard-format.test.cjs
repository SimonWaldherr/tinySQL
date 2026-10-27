const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

const source = fs.readFileSync(`${__dirname}/app.js`, 'utf8');
const start = source.indexOf('function detectClipboardImportFormat(');
const end = source.indexOf('async function importClipboardData(', start);
assert.ok(start >= 0 && end > start);
const sandbox = {};
vm.runInNewContext(`${source.slice(start, end)}; globalThis.detect = detectClipboardImportFormat;`, sandbox);

test('detects JSON lines without confusing pretty-printed JSON', () => {
    for (const [input, want] of [
        ['{"name":"O\'Reilly"}\n{"name":"Ada"}', 'jsonl'],
        ['\ufeff{"id":1}\r\n\r\n{"id":2}', 'jsonl'],
        ['{"id":1}\r{"id":2}', 'jsonl'],
        ['{\n  "name": "Ada",\n  "nested": {"id": 1}\n}', 'json'],
        ['[{"id":1},{"id":2}]', 'json'],
        ['{"id":1}', 'json'],
        ['id\tname\n1\tAda', 'tsv'],
        ['id,name\n1,Ada', 'csv'],
    ]) assert.equal(sandbox.detect(input), want, input);
});
