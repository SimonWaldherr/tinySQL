const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

const source = fs.readFileSync(`${__dirname}/app.js`, 'utf8');
const start = source.indexOf('function hasMultipleSQLStatements(');
const end = source.indexOf('function setQueryCancellationState(', start);
assert.ok(start >= 0 && end > start);
const sandbox = {};
vm.runInNewContext(`${source.slice(start, end)}; globalThis.hasMultiple = hasMultipleSQLStatements;`, sandbox);

test('CR and CRLF end SQL line comments before following statements', () => {
    for (const ending of ['\r', '\n', '\r\n']) {
        assert.equal(sandbox.hasMultiple(`SELECT 1; -- comment${ending}SELECT 2`), true, JSON.stringify(ending));
        assert.equal(sandbox.hasMultiple(`SELECT 1; -- comment${ending}`), false, JSON.stringify(ending));
    }
});
