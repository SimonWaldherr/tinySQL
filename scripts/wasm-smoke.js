#!/usr/bin/env node
// Run each module in its own process: the Go runtime and tinySQL API are global.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
globalThis.crypto ||= require('node:crypto').webcrypto;
require(path.resolve(process.argv[3]));

function success(result) {
  assert.equal(result.error, undefined, result.error);
  assert.notEqual(result.success, false);
  return result;
}

async function main() {
  const go = new Go();
  const { instance } = await WebAssembly.instantiate(fs.readFileSync(process.argv[2]), go.importObject);
  go.run(instance).catch(error => { console.error(error); process.exit(1); });
  for (let i = 0; i < 100 && !globalThis.tinySQL; i++) {
    await new Promise(resolve => setTimeout(resolve, 10));
  }
  const db = globalThis.tinySQL;
  assert.ok(db, 'WASM API must become ready');
  success(db.open('mem://?tenant=integration'));
  success(db.exec('CREATE TABLE items (id INT PRIMARY KEY, label TEXT, active BOOL)'));
  for (let id = 0; id < 200; id++) {
    success(db.exec(`INSERT INTO items VALUES (${id}, 'item_${id}', ${id % 2 === 0})`));
  }
  const expected = Array.from({ length: 200 }, (_, id) => [id, `item_${id}`, id % 2 === 0]);
  const result = success(db.query('SELECT id, label, active FROM items ORDER BY id'));
  assert.deepEqual(result.columns, ['id', 'label', 'active']);
  assert.deepEqual(result.rows, expected); // Exercises the bulk result bridge.
  assert.deepEqual(success(db.query('SELECT id FROM items WHERE id = -1')).rows, []);
  // Cross multiple result-transfer blocks, including the small/large boundary.
  success(db.exec('CREATE TABLE bulk_transfer (id INT, label TEXT, active BOOL)'));
  const bulkExpected = Array.from({ length: 5003 }, (_, id) => [id, `é漢🙂<&_${id}`, id % 2 === 0]);
  for (let offset = 0; offset < bulkExpected.length; offset += 500) {
    const values = bulkExpected.slice(offset, offset + 500)
      .map(([id, label, active]) => `(${id}, '${label}', ${active})`).join(',');
    success(db.exec(`INSERT INTO bulk_transfer VALUES ${values}`));
  }
  for (const limit of [31, 32, 1023, 1024, 1025, bulkExpected.length]) {
    const bulk = success(db.query(`SELECT id, label, active FROM bulk_transfer LIMIT ${limit}`));
    assert.deepEqual(bulk.columns, ['id', 'label', 'active']);
    assert.deepEqual(bulk.rows, bulkExpected.slice(0, limit));
    assert.equal(bulk.count, limit);
    assert.equal(typeof bulk.elapsed_ms, 'number');
  }
  // CTE ordering binds source qualifiers and output aliases before pagination.
  const orderedCTE = success(db.query('WITH c AS (SELECT id,label,active FROM bulk_transfer) SELECT q.id AS id,q.label AS caption FROM c q WHERE q.id>=4900 ORDER BY q.active DESC,id DESC LIMIT 5 OFFSET 2'));
  assert.deepEqual(orderedCTE.columns, ['id', 'caption']);
  assert.deepEqual(orderedCTE.rows, [4998, 4996, 4994, 4992, 4990].map(id => bulkExpected[id].slice(0, 2)));
  assert.deepEqual(success(db.query('WITH c AS (SELECT id,label FROM bulk_transfer) SELECT q.id AS caption,q.label AS caption FROM c q ORDER BY caption DESC LIMIT 3')).rows,
    [999, 998, 997].map(id => [bulkExpected[id][1]]));
  // Mutating an index run must visit every original candidate exactly once.
  success(db.exec('CREATE TABLE indexed_edits (id INT PRIMARY KEY, category TEXT, code INT, amount INT)'));
  success(db.exec("INSERT INTO indexed_edits VALUES (3,'a',10,7),(1,'b',10,8),(2,'a',10,9)"));
  success(db.exec('CREATE INDEX indexed_edits_key ON indexed_edits(category,code)'));
  assert.deepEqual(success(db.query("UPDATE indexed_edits SET category='moved' WHERE category='a' AND code=10 RETURNING id")).rows, [[3], [2]]);
  assert.deepEqual(success(db.query("UPDATE indexed_edits SET amount=amount WHERE category='moved' RETURNING id,amount")).rows, [[3, 7], [2, 9]]);
  assert.ok(db.exec("UPDATE indexed_edits SET id=999 WHERE category='moved'").error);
  assert.deepEqual(success(db.query("SELECT id,amount FROM indexed_edits WHERE category='moved'")).rows, [[3, 7], [2, 9]]);
  success(db.exec("CREATE TABLE batch_defaults (id INT, amount INT DEFAULT 7, label TEXT DEFAULT 'base')"));
  success(db.exec('INSERT INTO batch_defaults (id) VALUES '
    + Array.from({ length: 131 }, (_, id) => `(${id})`).join(',')));
  assert.deepEqual(success(db.query('SELECT COUNT(*),SUM(amount) FROM batch_defaults')).rows, [[131, 917]]);
  success(db.exec('CREATE TEMP TABLE selected_stage AS WITH c AS (SELECT id,label FROM bulk_transfer) SELECT q.id AS id,q.label AS label FROM c q WHERE q.id>=4999'));
  assert.deepEqual(success(db.query('SELECT id,label FROM selected_stage')).rows, bulkExpected.slice(4999).map(row => row.slice(0, 2)));
  assert.deepEqual(success(db.query('DELETE FROM selected_stage WHERE id=5001 RETURNING id,label')).rows, [bulkExpected[5001].slice(0, 2)]);
  assert.ok(db.query('DELETE FROM selected_stage WHERE id>=0 RETURNING missing').error);
  assert.deepEqual(success(db.query('SELECT id FROM selected_stage ORDER BY id')).rows, [[4999], [5000], [5002]]);
  success(db.exec('INSERT INTO selected_stage WITH c AS (SELECT id,label FROM bulk_transfer) SELECT q.id,q.label FROM c q WHERE q.id>=1 AND q.id<3'));
  assert.deepEqual(success(db.query('SELECT id FROM selected_stage ORDER BY id')).rows, [[1], [2], [4999], [5000], [5002]]);
  const contracts = JSON.parse(fs.readFileSync(path.join(__dirname,
    '../internal/engine/testdata/sql_semantics.json'), 'utf8'));
  for (const sql of contracts.setup) success(db.exec(sql));
  for (const contract of contracts.cases) {
    const actual = db.query(contract.sql);
    if (contract.error) {
      assert.ok(actual.error?.includes(contract.error),
        `${contract.name}: expected ${contract.error}, got ${JSON.stringify(actual)}`);
    } else {
      success(actual);
      assert.deepEqual(actual.columns, contract.columns, `${contract.name}: columns`);
      assert.deepEqual(actual.rows, contract.rows, `${contract.name}: rows`);
    }
  }
  // Revisit a warmed neighbor index across ingestion and structural edits.
  success(db.exec('CREATE TABLE context_edits (id INT PRIMARY KEY, doc TEXT, chunk INT)'));
  success(db.exec("INSERT INTO context_edits VALUES (1,'a',2),(2,'a',0),(3,'b',1)"));
  success(db.exec('CREATE TABLE context_edit_hits (doc TEXT, chunk INT)'));
  success(db.exec("INSERT INTO context_edit_hits VALUES ('a',0)"));
  const contextRows = () => success(db.query(`SELECT id FROM RAG_CONTEXT_FROM(
    'context_edits','doc','chunk','context_edit_hits','doc','chunk',0,1) ORDER BY id`)).rows;
  assert.deepEqual(contextRows(), [[2]]);
  success(db.exec("INSERT INTO context_edits VALUES (4,'a',1),(5,'a',1)"));
  assert.deepEqual(contextRows(), [[2],[4]]); // Duplicate positions keep the first source row.
  success(db.exec('UPDATE context_edits SET chunk=9 WHERE id=4'));
  assert.deepEqual(contextRows(), [[2],[5]]);
  success(db.exec('DELETE FROM context_edits WHERE id=2'));
  assert.deepEqual(contextRows(), [[5]]);
  const escaped = db.query("SELECT HTML_ESCAPE('<b>&</b>') AS escaped");
  if (process.argv[4] === 'tinygo') {
    assert.ok(escaped.error, 'TinyGo HTML escaping must report an error');
  } else {
    assert.deepEqual(success(escaped).rows, [['&lt;b&gt;&amp;&lt;/b&gt;']]);
  }
  const template = db.query(`SELECT HTML_TEMPLATE('<b>{{.name}}</b>', '{"name":"x"}') AS rendered`);
  if (process.argv[4] === 'minimal' || process.argv[4] === 'tinygo') {
    assert.ok(template.error, 'minimal templates must report an error');
    assert.ok(db.query("SELECT HTTP('https://example.com')").error, 'minimal HTTP must report an error');
  } else {
    assert.deepEqual(success(template).rows, [['<b>x</b>']]);
  }
  success(db.begin());
  success(db.exec("UPDATE items SET label = 'rolled back' WHERE id = 0"));
  success(db.rollback());
  assert.deepEqual(success(db.query('SELECT label FROM items WHERE id = 0')).rows, [['item_0']]);
  success(db.begin());
  success(db.exec("UPDATE items SET label = 'committed' WHERE id = 0"));
  success(db.commit());
  assert.deepEqual(success(db.query('SELECT label FROM items WHERE id = 0')).rows, [['committed']]);
  success(db.exec('CREATE TABLE mutable (id INT PRIMARY KEY, meta JSON)'));
  success(db.exec(`INSERT INTO mutable VALUES (1, '{"nested":{"value":"original"}}')`));
  success(db.begin());
  success(db.exec(`UPDATE mutable SET meta = JSON_SET(meta, 'nested.value', 'changed') WHERE id = 1`));
  assert.deepEqual(success(db.query("SELECT JSON_GET(meta, 'nested.value') FROM mutable")).rows, [['changed']]);
  success(db.exec('ALTER TABLE items ADD COLUMN extra INT'));
  success(db.exec("INSERT INTO items (id, label, active) VALUES (200, 'temporary', true)"));
  success(db.exec('DELETE FROM items WHERE id = 1'));
  success(db.rollback());
  assert.deepEqual(success(db.query("SELECT JSON_GET(meta, 'nested.value') FROM mutable")).rows, [['original']]);
  const rolledBack = success(db.query('SELECT * FROM items ORDER BY id'));
  assert.deepEqual(rolledBack.columns, ['id', 'label', 'active']);
  expected[0][1] = 'committed';
  assert.deepEqual(rolledBack.rows, expected);
  // Exercise the optimized numeric geometry decode and its escaped-key fallback.
  assert.deepEqual(success(db.query(`SELECT GEO_LON('{"type":"Point","coordinates":[13.405,52.52]}')`)).rows, [[13.405]]);
  assert.deepEqual(success(db.query(`SELECT GEO_LAT('{"ty\\u0070e":"Point","coordinates":[13.405,52.52]}')`)).rows, [[52.52]]);
  if (db.exportDB) {
    const snapshot = success(db.exportDB());
    success(db.close());
    assert.equal(db.importDB(snapshot.data, 'file://unsupported').success, false);
    assert.equal(db.status().connected, false, 'invalid import must leave the connection untouched');
    success(db.importDB(snapshot.data, 'mem://?tenant=integration'));
    assert.deepEqual(success(db.query('SELECT label FROM items WHERE id = 0')).rows, [['committed']]);
  }
  success(db.close());
  assert.ok(db.query('SELECT 1').error, 'closed database must reject queries');
  success(db.open());
  assert.ok(db.query('SELECT * FROM items').error, 'reopening must reset database and query cache');
  success(db.close());
  console.log(`WASM integration passed: ${path.basename(process.argv[2])}`);
  process.exit(0);
}
main().catch(error => { console.error(error); process.exit(1); });
