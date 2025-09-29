#!/usr/bin/env node
/**
 * Node.js WASM Runner for tinySQL
 */

const fs = require('fs');
const path = require('path');
require('./wasm_exec.js');

async function main() {
  const wasmPath = path.join(__dirname, 'tinySQL.wasm');
  const go = new Go();
  const wasmBuffer = fs.readFileSync(wasmPath);
  const { instance } = await WebAssembly.instantiate(wasmBuffer, go.importObject);
  await go.run(instance);

  if (!global.tinySQL) {
    console.error('tinySQL API not available');
    process.exit(1);
  }

  const db = global.tinySQL;
  console.log('Opening DB...');
  console.log(db.open('mem://?tenant=default'));

  const args = process.argv.slice(2);
  const cmd = args[0] || 'status';
  const sql = args.slice(1).join(' ');

  const parse = (s) => typeof s === 'string' ? JSON.parse(s) : s;

  switch (cmd) {
    case 'exec':
      console.log(parse(db.exec(sql)));
      break;
    case 'query':
      const res = parse(db.query(sql));
      console.log(res);
      if (res && res.columns && res.rows) {
        console.table(res.rows.map(r => Object.fromEntries(res.columns.map((c,i)=>[c,r[i]]))));
      }
      break;
    case 'status':
    default:
      console.log(parse(db.status()));
  }

  console.log(parse(db.close()));
}

main().catch(err => { console.error(err); process.exit(1); });
