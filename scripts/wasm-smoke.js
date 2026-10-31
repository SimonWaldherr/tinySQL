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
  assert.deepEqual(success(db.query(`SELECT DATEDIFF(MONTH, '2024-01-31', '2024-02-29'),
    TIMESTAMPDIFF(MONTH, '2024-01-31', '2024-02-29'), ISODOW('2024-03-01')`)).rows, [[1, 0, 5]]);
  assert.deepEqual(success(db.query(`SELECT COUNT(*) FILTER (WHERE active),
    SUM(id) FILTER (WHERE active) FROM items`)).rows, [[100, 9900]]);
  assert.deepEqual(success(db.query(`SELECT id, COUNT(DISTINCT active) OVER () AS kinds,
    SUM(id) FILTER (WHERE active) OVER (ORDER BY id ROWS 1 PRECEDING) AS total
    FROM items WHERE id < 3 ORDER BY id`)).rows, [[0, 2, 0], [1, 2, 0], [2, 2, 2]]);
  assert.deepEqual(success(db.query(`SELECT id, SUM(id) OVER (ORDER BY id RANGE 1 PRECEDING)
    FROM items WHERE id < 3 ORDER BY id`)).rows, [[0, 0], [1, 1], [2, 3]]);
  assert.deepEqual(success(db.query(`SELECT value FROM (
    SELECT 1 AS value UNION ALL SELECT NULL AS value
    ) AS nullable ORDER BY value NULLS FIRST`)).rows, [[null], [1]]);
  assert.deepEqual(success(db.query(`SELECT value, ordinal FROM STRING_SPLIT('ä::🙂::', '::', 1)
    ORDER BY ordinal`)).rows, [['ä', 1], ['🙂', 2], ['', 3]]);
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
