const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

const source = fs.readFileSync(`${__dirname}/app.js`, 'utf8');
const start = source.indexOf('async function importExcelFile(');
const end = source.indexOf('// Sanitize table name', start);
assert.ok(start >= 0 && end > start);

function fixture(responses) {
    let saves = 0, renders = 0;
    const statuses = [];
    const sandbox = {
        activeWorkspaceId: 'workspace', workspaceEpoch: 0, currentTables: [],
        requireStableWorkspace: () => true,
        XLSX: { read: () => ({ SheetNames: ['first', 'second'], Sheets: {first: {}, second: {}} }),
            utils: { sheet_to_json: () => [{id: 1}] } },
        shp: async () => [{fileName: 'first', features: [{}]}, {fileName: 'second', features: [{}]}],
        readFile: async () => new ArrayBuffer(1),
        sanitizeTableName: name => name, quoteSqlIdentifier: name => `"${name}"`,
        wasmApi: { importFile: async () => { const response = responses.shift(); if (response instanceof Error) throw response; return response; } },
        updateStatus: text => statuses.push(text), renderTables: () => renders++,
        loadTables: async () => statuses.push('1 table(s), 0 virtual'),
        scheduleDatabaseSnapshotSave: () => saves++, setQuery() {}, showToast() {}, alert() {},
        document: { getElementById: () => null }, console: { error() {}, warn() {} },
    };
    vm.runInNewContext(`${source.slice(start, end)}; globalThis.excel = importExcelFile; globalThis.shape = importShapefileFile;`, sandbox);
    return {sandbox, statuses, counts: () => ({saves, renders})};
}

const ok = () => ({success: true, rowsImported: 1, columns: ['id']});
for (const [label, method, file] of [['Excel', 'excel', 'example.xlsx'], ['Shapefile', 'shape', 'example.zip']]) {
    test(`${label} preserves and reports partial imports after thrown or returned errors`, async () => {
        for (const failure of [new Error('failure'), {success: false, error: 'failure'}]) {
            const h = fixture([ok(), failure]);
            const result = await h.sandbox[method]({name: file});
            assert.equal(result, false);
            assert.equal(h.sandbox.currentTables.length, 1);
            assert.ok(h.counts().saves > 0, 'successful earlier import must be saved');
            assert.ok(h.counts().renders > 0, 'successful earlier import must be shown');
            assert.match(h.statuses.at(-1), /failed|incomplete/i);
        }
    });
    test(`${label} reports failure and conservatively saves after a failed first import`, async () => {
        const h = fixture([new Error('failure')]);
        const result = await h.sandbox[method]({name: file});
        assert.equal(result, false);
        assert.equal(h.sandbox.currentTables.length, 0);
        assert.equal(h.counts().saves, 1);
        assert.match(h.statuses.at(-1), /failed/i);
    });
    test(`${label} reports a returned error even when a later item imports`, async () => {
        const h = fixture([{success: false, error: 'failure'}, ok()]);
        const result = await h.sandbox[method]({name: file});
        assert.equal(result, false);
        assert.equal(h.sandbox.currentTables.length, 1);
        assert.ok(h.counts().saves > 0);
        assert.match(h.statuses.at(-1), /incomplete/i);
    });
}
