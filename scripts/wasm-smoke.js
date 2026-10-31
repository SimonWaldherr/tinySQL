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
