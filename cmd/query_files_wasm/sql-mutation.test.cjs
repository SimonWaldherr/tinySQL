const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

const source = fs.readFileSync(`${__dirname}/app.js`, 'utf8');
const start = source.indexOf('function sqlMayMutate(');
const end = source.indexOf('// The legacy path', start);
assert.ok(start >= 0 && end > start);
const sandbox = {};
vm.runInNewContext(`${source.slice(start, end)}; globalThis.classify = sqlMayMutate;`, sandbox);

test('read-only queries and quoted text do not cause unnecessary snapshots', () => {
    for (const sql of [
        '', '-- comment only\r\n', 'SELECT 1', 'SELECT 1; SELECT 2;',
        `SELECT 'semi; INSERT', "DELETE", [UPDATE]`,
        `WITH data AS (SELECT 'INSERT; UPDATE' AS t) SELECT t FROM data`,
        `/* header */ WITH "UPDATE" AS (SELECT (1 + 2) AS n) SELECT n FROM "UPDATE"`,
        `WITH first AS (SELECT 1), second AS (SELECT 2) SELECT * FROM second`,
        `WITH RECURSIVE tree AS (SELECT 1) SELECT * FROM tree`,
        '/* INSERT */ SELECT 1 -- DELETE\n', 'EXPLAIN INSERT INTO t VALUES (1)',
        'EXPLAIN ANALYZE SELECT 1', 'EXPLAIN SELECT analyze FROM t',
        'EXPLAIN ANALYZE WITH src AS (SELECT 1) SELECT * FROM src',
    ]) assert.equal(sandbox.classify(sql), false, sql);
});

test('mutations and uncertain WITH forms schedule snapshots', () => {
    for (const sql of [
        'INSERT INTO t VALUES (1)', 'SELECT 1; DELETE FROM t',
        'WITH src AS (SELECT 1 AS n) INSERT INTO t SELECT n FROM src',
        `/* hi */ WITH "UPDATE" AS (SELECT ';)') INSERT INTO t SELECT * FROM "UPDATE"`,
        'WITH src AS (SELECT 1) UPDATE t SET n = 2',
        'WITH src AS (SELECT 1) DELETE FROM t',
        'WITH src AS (SELECT 1) MERGE INTO t USING src ON 1 = 1',
        'WITH src AS (SELECT 1)',
        '-- comment\rINSERT INTO t VALUES (1)',
        'SELECT ";SELECT"; CREATE TABLE t (x INT)',
        'EXPLAIN ANALYZE INSERT INTO t VALUES (1)',
        'EXPLAIN /* plan */ ANALYZE UPDATE t SET n = 2',
        'EXPLAIN ANALYZE DELETE FROM t',
        'EXPLAIN ANALYZE WITH src AS (SELECT 1) INSERT INTO t SELECT * FROM src',
        'EXPLAIN ANALYZE EXPLAIN ANALYZE DELETE FROM t',
    ]) assert.equal(sandbox.classify(sql), true, sql);
});
