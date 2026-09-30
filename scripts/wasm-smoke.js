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
  assert.deepEqual(success(db.query("SELECT HTML_ESCAPE('<b>&</b>') AS escaped")).rows, [['&lt;b&gt;&amp;&lt;/b&gt;']]);
  const template = db.query(`SELECT HTML_TEMPLATE('<b>{{.name}}</b>', '{"name":"x"}') AS rendered`);
  if (process.argv[4] === 'minimal') {
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
