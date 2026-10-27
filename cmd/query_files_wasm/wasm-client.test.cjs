const { test } = require('node:test');
const assert = require('node:assert/strict');
const { TinySQLWasmClient } = require('./wasm-client.js');

class FakeWorker {
    static instances = [];
    constructor() { this.terminated = false; FakeWorker.instances.push(this); }
    postMessage() {}
    terminate() { this.terminated = true; }
    message(data) { this.onmessage?.({ data }); }
}

test('termination rejects pending initialization and ignores stale worker messages', async t => {
    const original = globalThis.Worker;
    globalThis.Worker = FakeWorker;
    FakeWorker.instances = [];
    t.after(() => { globalThis.Worker = original; });

    const client = new TinySQLWasmClient();
    const firstInit = client.init();
    const rejected = assert.rejects(firstInit, error => error.code === 'WASM_WORKER_TERMINATED');
    const oldWorker = FakeWorker.instances[0];
    client.terminate();
    await Promise.race([rejected, new Promise((_, reject) => setTimeout(() => reject(new Error('initialization remained pending')), 100))]);
    assert.equal(oldWorker.terminated, true);

    const secondInit = client.init();
    const newWorker = FakeWorker.instances[1];
    oldWorker.message({ type: 'ready', methods: ['executeQuery'] });
    assert.equal(client.ready, false);
    newWorker.message({ type: 'ready', methods: ['getResultPage'] });
    await secondInit;
    oldWorker.onerror?.({ message: 'late failure' });
    assert.equal(client.ready, true);
    assert.equal(client.supports('getResultPage'), true);
    assert.equal(client.supports('executeQuery'), false);
    client.terminate();
});

test('failed initialization can be retried and termination rejects queued work', async t => {
    const original = globalThis.Worker;
    globalThis.Worker = FakeWorker;
    FakeWorker.instances = [];
    t.after(() => { globalThis.Worker = original; });

    const client = new TinySQLWasmClient();
    const failed = client.init();
    FakeWorker.instances[0].message({ type: 'init-error', error: { message: 'load failed' } });
    await assert.rejects(failed, /load failed/);
    const ready = client.init();
    FakeWorker.instances[1].message({ type: 'ready', methods: ['executeQuery'] });
    await ready;
    const active = client.executeQuery('SELECT 1');
    const queued = client.executeQuery('SELECT 2');
    const rejects = [active, queued].map(promise => assert.rejects(promise, error => error.code === 'WASM_WORKER_TERMINATED'));
    client.terminate();
    await Promise.all(rejects);
});

test('retry started by a fatal-error listener remains the active initialization', async t => {
    const original = globalThis.Worker;
    globalThis.Worker = FakeWorker;
    FakeWorker.instances = [];
    t.after(() => { globalThis.Worker = original; });
    const client = new TinySQLWasmClient();
    let retry;
    client.on('error', () => { retry = client.init(); });
    const first = client.init();
    const failure = assert.rejects(first, /runtime failed/);
    FakeWorker.instances[0].message({ type: 'fatal', error: { message: 'runtime failed' } });
    assert.ok(retry);
    const concurrent = client.init();
    assert.equal(FakeWorker.instances.length, 2, 'retry must reuse the existing worker');
    FakeWorker.instances[1].message({ type: 'ready', methods: ['executeQuery'] });
    await Promise.all([failure, retry, concurrent]);
    assert.equal(client.supports('executeQuery'), true);
    client.terminate();
});
