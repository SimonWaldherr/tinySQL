#!/usr/bin/env node

const fs = require('fs');
const path = require('path');
const { performance } = require('perf_hooks');

require('./web/wasm_exec.js');

const wasmPath = path.resolve(process.argv[2] || './web/tinySQL.wasm');
const iterations = Number.parseInt(process.argv[3] || '1000', 10);

if (!Number.isInteger(iterations) || iterations < 1) {
  throw new Error('iterations must be a positive integer');
}

function parse(response) {
  // tinySQL's WASM API now returns native JS objects/arrays directly (via
  // syscall/js.ValueOf) instead of JSON strings, so no JSON.parse round trip
  // is needed here any more. Kept as a passthrough so this benchmark still
  // works unmodified against older tinySQL.wasm builds that returned strings.
  return typeof response === 'string' ? JSON.parse(response) : response;
}

function assertSuccess(response, operation) {
  const result = parse(response);
  if (result.error || result.success === false) {
    throw new Error(`${operation}: ${result.error || 'failed'}`);
  }
  return result;
}

async function loadTinySQL() {
  const go = new Go();
  const bytes = fs.readFileSync(wasmPath);
  const { instance } = await WebAssembly.instantiate(bytes, go.importObject);
  go.run(instance).catch((error) => {
    console.error('tinySQL WASM runtime failed:', error);
    process.exitCode = 1;
  });

  for (let attempt = 0; attempt < 100 && !global.tinySQL; attempt++) {
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  if (!global.tinySQL) {
    throw new Error('tinySQL API not available');
  }
  return global.tinySQL;
}

function benchmarkQuery(db, name, sql) {
  for (let i = 0; i < 20; i++) {
    assertSuccess(db.query(sql), `warmup ${name}`);
  }

  const started = performance.now();
  let rowCount = 0;
  for (let i = 0; i < iterations; i++) {
    rowCount += assertSuccess(db.query(sql), name).count;
  }
  const elapsedMs = performance.now() - started;

  return {
    name,
    iterations,
    rows: rowCount,
    elapsedMs: Number(elapsedMs.toFixed(2)),
    queriesPerSecond: Number((iterations * 1000 / elapsedMs).toFixed(2)),
    microsecondsPerQuery: Number((elapsedMs * 1000 / iterations).toFixed(2)),
  };
}

function benchmarkExec(db, name, sql) {
  for (let i = 0; i < 20; i++) {
    assertSuccess(db.exec(sql), `warmup ${name}`);
  }

  const started = performance.now();
  for (let i = 0; i < iterations; i++) {
    assertSuccess(db.exec(sql), name);
  }
  const elapsedMs = performance.now() - started;

  return {
    name,
    iterations,
    elapsedMs: Number(elapsedMs.toFixed(2)),
    operationsPerSecond: Number((iterations * 1000 / elapsedMs).toFixed(2)),
    microsecondsPerOperation: Number((elapsedMs * 1000 / iterations).toFixed(2)),
  };
}

async function main() {
  const db = await loadTinySQL();
  assertSuccess(db.open('mem://?tenant=benchmark'), 'open');
  assertSuccess(db.exec('CREATE TABLE users (id INT PRIMARY KEY, name TEXT, active BOOL)'), 'create');
  for (let id = 0; id < 200; id++) {
    assertSuccess(
      db.exec(`INSERT INTO users VALUES (${id}, 'user_${id}', ${id % 2 === 0})`),
      'insert',
    );
  }

  console.log(JSON.stringify({
    wasm: wasmPath,
    results: [
      benchmarkQuery(db, 'constant', 'SELECT 1 AS n, \'hello\' AS greeting'),
      benchmarkQuery(
        db,
        'filtered-scan',
        'SELECT id, name FROM users WHERE active = true ORDER BY id LIMIT 20',
      ),
      benchmarkExec(db, 'primary-key-update', 'UPDATE users SET name = \'updated\' WHERE id = 100'),
      benchmarkExec(db, 'primary-key-delete', 'DELETE FROM users WHERE id = 101'),
    ],
  }, null, 2));

  assertSuccess(db.close(), 'close');
  process.exit(0);
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
