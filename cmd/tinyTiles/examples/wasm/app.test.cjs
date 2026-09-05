const { test } = require('node:test');
const assert = require('node:assert/strict');
const vm = require('node:vm');
const fs = require('node:fs');
const source = fs.readFileSync(`${__dirname}/app.js`, 'utf8');
const tick = () => new Promise(resolve => setImmediate(resolve));
async function setup(mime = 'application/wasm') {
  const defaults = { z:'8', xmin:'137', xmax:'137', ymin:'167', ymax:'167', concurrency:'4', dataset:'demo', 'cache-name':'demo', 'manifest-url':'http://localhost/manifest' };
  const elements = {};
  for (const id of ['status','open','close','sync','get',...Object.keys(defaults)]) elements[id] = {
    value: defaults[id] || '', textContent:'', disabled:false,
    addEventListener(event, fn) { this.click = fn; }
  };
  let fetches=0, syncs=0, streams=0, buffers=0;
  const window = {};
  const api = { version:'test', open:async()=>({opened:true}),close:async()=>({closed:true}),sync:async()=>{syncs++;return {};},get:async()=>({found:true,data:new Uint8Array(3)}) };
  const context = {
    window, performance, setTimeout,
    document:{getElementById:id=>elements[id],querySelectorAll:()=>Object.keys(defaults).map(id=>elements[id])},
    fetch:async()=>{fetches++; return {ok:true,headers:{get:()=>mime},arrayBuffer:async()=>new ArrayBuffer(0)};},
    WebAssembly:{instantiateStreaming:async()=>{streams++;return {instance:{}};},instantiate:async()=>{buffers++;return {instance:{}};}},
    Go:class { constructor(){this.importObject={};} run(){window.tinyTiles=api;return new Promise(()=>{});} }
  };
  vm.runInNewContext(source,context);
  assert.equal(elements.open.disabled,true);
  await tick();
  return {elements,api,counts:()=>({fetches,syncs,streams,buffers})};
}
test('one WASM download with either MIME path',async()=>{
  for(const mime of ['application/wasm','application/octet-stream']) {
    const s=await setup(mime);
    assert.equal(s.counts().fetches,1);
    assert.equal(s.counts().streams,mime==='application/wasm'?1:0);
    assert.equal(s.elements.open.disabled,false);
    assert.equal(s.elements.sync.disabled,true);
  }
});
test('serializes operations and validates integer/range bounds before sync',async()=>{
  const s=await setup();
  let release;
  s.api.open=()=>new Promise(resolve=>{release=resolve;});
  const pending=s.elements.open.click();
  assert.equal(s.elements.open.disabled,true);
  await s.elements.sync.click();
  assert.equal(s.counts().syncs,0);
  release({opened:true});await pending;
  assert.equal(s.elements.sync.disabled,false);
  for(const bad of ['2.5','','256']) {
    s.elements.xmax.value=bad;await s.elements.sync.click();
    assert.match(s.elements.status.textContent,/error/);
  }
  assert.equal(s.counts().syncs,0);
  s.elements.xmax.value='137';await s.elements.sync.click();
  assert.equal(s.counts().syncs,1);
  await s.elements.close.click();assert.equal(s.elements.sync.disabled,true);
});
