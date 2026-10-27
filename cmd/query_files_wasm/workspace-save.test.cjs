const {test} = require('node:test');
const assert = require('node:assert/strict');
const {WorkspaceStorage} = require('./workspace-storage.js');

// Simulate asynchronous IndexedDB requests in one transaction, including
// requests queued from another request's success handler.
function fakeSaveDatabase(existing) {
    const records = new Map(existing.map(record => [record.generation, record]));
    const reads = [], deleted = [];
    let pending = 0;
    const transaction = {};
    function request(action) {
        const result = {};
        pending++;
        setImmediate(() => {
            try {
                result.result = action();
                result.onsuccess?.();
            } catch (error) {
                result.error = error;
                result.onerror?.();
                transaction.onerror?.();
            } finally {
                pending--;
                if (!pending) setImmediate(() => { if (!pending) transaction.oncomplete?.(); });
            }
        });
        return result;
    }
    const current = {id: 'demo', nextGeneration: 1, currentGeneration: 1,
        lastKnownGoodGeneration: 1, format: 'raw', snapshotEncoding: 'raw', metadata: {}};
    const workspaces = {
        get() { return request(() => current); },
        put(value) { return request(() => value); },
    };
    const generations = {
        index() { return {
            getAll() { throw new Error('all snapshot records must not be loaded'); },
            getAllKeys() { return request(() => [...records.keys()].map(generation => ['demo', generation])); },
        }; },
        add(record) { return request(() => { records.set(record.generation, record); }); },
        get(key) { return request(() => { reads.push(key[1]); return records.get(key[1]); }); },
        delete(key) { return request(() => { deleted.push(key[1]); records.delete(key[1]); }); },
    };
    transaction.objectStore = name => name === 'workspaces' ? workspaces : generations;
    transaction.abort = () => transaction.onabort?.();
    return {db: {transaction: () => transaction}, reads, deleted, records};
}

const generation = n => ({workspaceId: 'demo', generation: n, snapshot: new Uint8Array([n]),
    snapshotBackend: 'opfs', opfsFileName: `snapshot-${n}`, format: 'raw'});

async function save(fixture, keep) {
    return new WorkspaceStorage()._saveWorkspaceAtomically(fixture.db, 'demo', new Uint8Array([9]),
        null, null, {}, keep);
}

test('saving without retention reads no old snapshot records', async () => {
    const f = fakeSaveDatabase([generation(1), generation(2)]);
    const result = await save(f, 5);
    assert.equal(result.workspace.currentGeneration, 3); // stale nextGeneration repaired
    assert.deepEqual(f.reads, []);
    assert.deepEqual(f.deleted, []);
    assert.deepEqual(result.obsoleteGenerations, []);
});

test('retention reads only deleted records for post-commit OPFS cleanup', async () => {
    const f = fakeSaveDatabase([1, 2, 3, 4, 5].map(generation));
    const result = await save(f, 3);
    assert.equal(result.workspace.currentGeneration, 6);
    assert.deepEqual(f.reads.sort(), [1, 2, 3]);
    assert.deepEqual(f.deleted.sort(), [1, 2, 3]);
    assert.deepEqual(result.obsoleteGenerations.map(g => g.opfsFileName).sort(),
        ['snapshot-1', 'snapshot-2', 'snapshot-3']);
    assert.deepEqual([...f.records.keys()].sort(), [4, 5, 6]);
});
