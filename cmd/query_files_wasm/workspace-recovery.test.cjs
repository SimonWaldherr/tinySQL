const { test } = require('node:test');
const assert = require('node:assert/strict');
const { WorkspaceStorage, WorkspaceRecoveryError } = require('./workspace-storage.js');

// Model IndexedDB's cloning on a record read, and count records loaded. This
// exercises recovery ordering and snapshot validation without a browser.
function recoveryFixture(workspace, generations) {
    const store = new WorkspaceStorage();
    const reads = [];
    const restores = [];
    store.open = async () => ({});
    store.getWorkspace = async () => workspace;
    store._getGenerationNumbers = async () => generations.map(g => g.generation);
    store._getGenerationRecord = async (_db, _id, number) => {
        reads.push(number);
        const record = generations.find(g => g.generation === number);
        return record ? structuredClone(record) : null;
    };
    store.restoreGeneration = async (_id, number) => {
        restores.push(number);
        return { ...workspace, currentGeneration: number, lastKnownGoodGeneration: number };
    };
    return { store, reads, restores };
}

const record = (generation, snapshot) => ({
    workspaceId: 'demo', generation, snapshot, snapshotEncoding: 'raw', metadata: {},
});

test('generation discovery reads only IndexedDB keys', async () => {
    const store = new WorkspaceStorage();
    let query;
    const transaction = {};
    const request = { result: [['demo', 1], ['demo', 3]] };
    const db = {
        transaction(name, mode) {
            assert.equal(name, 'workspaceGenerations');
            assert.equal(mode, 'readonly');
            transaction.objectStore = () => ({ index: () => ({
                getAllKeys(id) {
                    query = id;
                    setImmediate(() => {
                        request.onsuccess();
                        setImmediate(() => transaction.oncomplete());
                    });
                    return request;
                },
                getAll() { throw new Error('snapshot values must not be fetched'); },
            }) });
            return transaction;
        },
    };
    assert.deepEqual(await store._getGenerationNumbers(db, 'demo'), [1, 3]);
    assert.equal(query, 'demo');
});

test('recovery reads only the current snapshot when validation succeeds', async () => {
    const workspace = { id: 'demo', currentGeneration: 3, lastKnownGoodGeneration: 2 };
    const f = recoveryFixture(workspace, [record(1, 'one'), record(2, 'two'), record(3, 'three')]);
    const result = await f.store.recoverWorkspace('demo', { validate: async snapshot => snapshot === 'three' });
    assert.equal(result.snapshot, 'three');
    assert.deepEqual(result.attemptedGenerations, [3]);
    assert.deepEqual(f.reads, [3]);
    assert.deepEqual(f.restores, [3]); // records the newly validated generation as known good
});

test('recovery attempts known-good then newest remaining snapshots', async () => {
    const workspace = { id: 'demo', currentGeneration: 4, lastKnownGoodGeneration: 2 };
    const f = recoveryFixture(workspace, [record(1, 'one'), record(2, 'two'), record(3, 'three'), record(4, 'four')]);
    const result = await f.store.recoverWorkspace('demo', {
        validate: async snapshot => {
            if (snapshot === 'four') throw new Error('bad snapshot');
            return snapshot === 'three';
        },
    });
    assert.equal(result.snapshot, 'three');
    assert.equal(result.recovered, true);
    assert.deepEqual(result.attemptedGenerations, [4, 2, 3]);
    assert.deepEqual(f.reads, [4, 2, 3]);
    assert.deepEqual(f.restores, [3]);
});

test('recovery skips generations removed by another tab', async () => {
    const workspace = { id: 'demo', currentGeneration: 2, lastKnownGoodGeneration: 1 };
    const f = recoveryFixture(workspace, [record(1, 'one')]);
    f.store._getGenerationNumbers = async () => [1, 2];
    const result = await f.store.recoverWorkspace('demo');
    assert.equal(result.snapshot, 'one');
    assert.deepEqual(f.reads, [2, 1]);
    assert.deepEqual(result.attemptedGenerations, [2, 1]);
});

test('recovery propagates IndexedDB read errors', async () => {
    const f = recoveryFixture({ id: 'demo', currentGeneration: 1, lastKnownGoodGeneration: 1 }, [record(1, 'one')]);
    f.store._getGenerationRecord = async () => { throw new Error('IndexedDB unavailable'); };
    await assert.rejects(f.store.recoverWorkspace('demo'), /IndexedDB unavailable/);
});

test('recovery reports empty and invalid histories', async () => {
    const workspace = { id: 'demo', currentGeneration: 2, lastKnownGoodGeneration: 1 };
    const empty = recoveryFixture(workspace, []);
    assert.equal((await empty.store.recoverWorkspace('demo')).snapshot, null);
    const f = recoveryFixture(workspace, [record(1, 'one'), record(2, 'two')]);
    await assert.rejects(f.store.recoverWorkspace('demo', { validate: () => false }), err => {
        assert.ok(err instanceof WorkspaceRecoveryError);
        assert.deepEqual(err.attemptedGenerations, [2, 1]);
        return true;
    });
});
