/*
 * tinySQL Workspace Storage
 * =========================
 *
 * An asynchronous, browser-local persistence layer for the Query Files WASM
 * application.  It deliberately has no UI or WASM dependency, so it can be
 * loaded before `app.js` and adopted incrementally.
 *
 * Browser API (classic-script global):
 *
 *   const store = new TinySQLWorkspaceStorage.WorkspaceStorage();
 *   await store.open();
 *
 *   // A snapshot can be the base64 string returned by exportDatabase(), a
 *   // Blob, an ArrayBuffer, or any TypedArray/DataView.  The default
 *   // `tinysql-gob-base64` format is decoded before it reaches IndexedDB and
 *   // re-encoded when it is loaded for importDatabase().
 *   const workspace = await store.createWorkspace({
 *     name: 'Customer analysis',
 *     snapshot: exportedSnapshot,
 *     format: 'tinysql-gob-base64',
 *     metadata: { query: 'SELECT ...' },
 *   });
 *
 *   await store.saveWorkspace(workspace.id, newerSnapshot, {
 *     metadata: { query: 'SELECT ...' }, // merged by default
 *     keepGenerations: 5,                // minimum is always one
 *     // snapshotEncoding: 'raw',         // opt out for ordinary strings
 *   });
 *   const workspaces = await store.listWorkspaces();
 *   const current = await store.loadWorkspace(workspace.id);
 *   const versions = await store.listGenerations(workspace.id);
 *   await store.restoreGeneration(workspace.id, versions[1].generation);
 *
 *   // Recovery tries the newest snapshots first.  Pass a validator that
 *   // imports a snapshot into an isolated engine and returns true only when
 *   // that import succeeds.  A successful older version becomes current.
 *   const recovered = await store.recoverWorkspace(workspace.id, {
 *     validate: async (snapshot) => importIntoScratchDatabase(snapshot),
 *   });
 *
 *   await store.renameWorkspace(workspace.id, 'Q3 customer analysis');
 *   await store.deleteWorkspace(workspace.id);
 *   await store.close();
 *
 * Methods return plain data rather than IndexedDB records.  `listWorkspaces`
 * and `listGenerations` never return snapshot bytes; `loadWorkspace` and
 * `recoverWorkspace` do.  Each save allocates its generation, writes the
 * generation and its current pointer, and applies retention in one IndexedDB
 * read/write transaction.  Concurrent browser tabs therefore cannot reuse or
 * overwrite a generation number.  Retention never removes the current or
 * last-known-good generation, which leaves a recovery path after a bad import
 * or interrupted browser session.
 *
 * Storage backend:
 *   tinySQL's base64 GOB snapshots are intentionally stored as binary
 *   IndexedDB values. Other strings stay raw unless callers opt into
 *   `snapshotEncoding: 'base64'`. When `preferOPFS` is true and a raw binary
 *   snapshot meets `opfsThresholdBytes` (8 MiB by default), its immutable
 *   bytes are written to a unique file in OPFS while IndexedDB continues to
 *   own all workspace metadata and generation pointers. IndexedDB remains the
 *   portable fallback for unavailable OPFS, small snapshots, strings, and any
 *   OPFS write failure. Old IndexedDB-only generations remain readable.
 *
 *   OPFS writes deliberately happen before their IndexedDB pointer
 *   transaction. If that transaction fails, only the newly-created file is
 *   cleaned up best-effort. Retention and workspace deletion remove OPFS files
 *   only after the corresponding IndexedDB transaction has committed. There is
 *   intentionally no broad orphan scan or directory GC: an interrupted write
 *   can leave an inaccessible file, but must never risk a retained snapshot.
 *
 * The file also exports `module.exports` when evaluated by a CommonJS test
 * harness.  No bundler, module loader, or third-party dependency is required.
 */
(function attachTinySQLWorkspaceStorage(globalObject) {
    'use strict';

    const DATABASE_NAME = 'tinysql-query-files-workspaces-v1';
    const DATABASE_VERSION = 1;
    const WORKSPACES_STORE = 'workspaces';
    const GENERATIONS_STORE = 'workspaceGenerations';
    const GENERATIONS_BY_WORKSPACE = 'byWorkspace';
    const DEFAULT_MAX_GENERATIONS = 5;
    const DEFAULT_FORMAT = 'tinysql-gob-base64';
    const DEFAULT_OPFS_THRESHOLD_BYTES = 8 * 1024 * 1024;
    const OPFS_SNAPSHOTS_DIRECTORY = 'tinysql-query-files-workspaces-v1';
    const OPFS_SNAPSHOT_FILE_PREFIX = 'snapshot-';

    class WorkspaceStorageError extends Error {
        constructor(message, code = 'WORKSPACE_STORAGE_ERROR', cause) {
            super(message);
            this.name = 'WorkspaceStorageError';
            this.code = code;
            if (cause !== undefined) this.cause = cause;
        }
    }

    class WorkspaceNotFoundError extends WorkspaceStorageError {
        constructor(workspaceId) {
            super(`Workspace not found: ${workspaceId}`, 'WORKSPACE_NOT_FOUND');
            this.name = 'WorkspaceNotFoundError';
            this.workspaceId = workspaceId;
        }
    }

    class WorkspaceRecoveryError extends WorkspaceStorageError {
        constructor(workspaceId, attemptedGenerations, cause) {
            super(
                `No recoverable snapshot found for workspace: ${workspaceId}`,
                'WORKSPACE_RECOVERY_FAILED',
                cause
            );
            this.name = 'WorkspaceRecoveryError';
            this.workspaceId = workspaceId;
            this.attemptedGenerations = attemptedGenerations;
        }
    }

    function hasOwn(object, key) {
        return Object.prototype.hasOwnProperty.call(object, key);
    }

    function nowISO() {
        return new Date().toISOString();
    }

    function normalizeWorkspaceID(value) {
        const id = String(value || '').trim();
        if (!id) throw new WorkspaceStorageError('A workspace id is required.', 'INVALID_WORKSPACE_ID');
        if (id.length > 256) {
            throw new WorkspaceStorageError('A workspace id may not exceed 256 characters.', 'INVALID_WORKSPACE_ID');
        }
        return id;
    }

    function normalizeWorkspaceName(value) {
        const name = String(value || '').trim();
        if (!name) throw new WorkspaceStorageError('A workspace name is required.', 'INVALID_WORKSPACE_NAME');
        if (name.length > 256) {
            throw new WorkspaceStorageError('A workspace name may not exceed 256 characters.', 'INVALID_WORKSPACE_NAME');
        }
        return name;
    }

    function createWorkspaceID() {
        if (globalObject.crypto && typeof globalObject.crypto.randomUUID === 'function') {
            return `workspace-${globalObject.crypto.randomUUID()}`;
        }
        const random = Math.random().toString(36).slice(2);
        return `workspace-${Date.now().toString(36)}-${random}`;
    }

    function normalizeGeneration(value) {
        const generation = Number(value);
        if (!Number.isSafeInteger(generation) || generation < 1) {
            throw new WorkspaceStorageError('A positive generation number is required.', 'INVALID_GENERATION');
        }
        return generation;
    }

    function normalizeKeepGenerations(value, fallback) {
        if (value === undefined || value === null) return fallback;
        const count = Number(value);
        if (!Number.isSafeInteger(count) || count < 1) {
            throw new WorkspaceStorageError('keepGenerations must be a positive integer.', 'INVALID_RETENTION');
        }
        return count;
    }

    function normalizeOPFSThreshold(value, fallback) {
        if (value === undefined || value === null) return fallback;
        const bytes = Number(value);
        if (!Number.isSafeInteger(bytes) || bytes < 0) {
            throw new WorkspaceStorageError(
                'opfsThresholdBytes must be a non-negative safe integer.',
                'INVALID_OPFS_THRESHOLD'
            );
        }
        return bytes;
    }

    function cloneMetadata(value) {
        if (value === undefined || value === null) return {};
        if (typeof value !== 'object' || Array.isArray(value)) {
            throw new WorkspaceStorageError('Workspace metadata must be an object.', 'INVALID_METADATA');
        }
        try {
            if (typeof globalObject.structuredClone === 'function') {
                return globalObject.structuredClone(value);
            }
            return JSON.parse(JSON.stringify(value));
        } catch (error) {
            throw new WorkspaceStorageError(
                'Workspace metadata must be structured-cloneable.',
                'INVALID_METADATA',
                error
            );
        }
    }

    function mergeMetadata(current, update, replace) {
        if (update === undefined) return cloneMetadata(current);
        const next = cloneMetadata(update);
        return replace ? next : { ...cloneMetadata(current), ...next };
    }

    function isArrayBufferView(value) {
        return typeof ArrayBuffer !== 'undefined'
            && typeof ArrayBuffer.isView === 'function'
            && ArrayBuffer.isView(value);
    }

    function isBlob(value) {
        return typeof globalObject.Blob !== 'undefined' && value instanceof globalObject.Blob;
    }

    function isBinarySnapshot(value) {
        return isBlob(value)
            || (typeof ArrayBuffer !== 'undefined' && value instanceof ArrayBuffer)
            || isArrayBufferView(value);
    }

    function isManagedOPFSSnapshotFileName(value) {
        return typeof value === 'string'
            && value.startsWith(OPFS_SNAPSHOT_FILE_PREFIX)
            && value.endsWith('.bin')
            && /^[A-Za-z0-9._-]+$/.test(value);
    }

    function snapshotBackendForRecord(record) {
        return record && record.snapshotBackend === 'opfs' ? 'opfs' : 'indexeddb';
    }

    function hasManagedOPFSSnapshotRecord(record) {
        return snapshotBackendForRecord(record) === 'opfs'
            && isManagedOPFSSnapshotFileName(record.opfsFileName);
    }

    function isNotFoundError(error) {
        return Boolean(error && (
            error.name === 'NotFoundError'
            || error.code === 'ENOENT'
        ));
    }

    function createOPFSSnapshotFileName() {
        let token = '';
        if (globalObject.crypto && typeof globalObject.crypto.randomUUID === 'function') {
            token = globalObject.crypto.randomUUID();
        } else if (globalObject.crypto && typeof globalObject.crypto.getRandomValues === 'function') {
            const values = new Uint32Array(4);
            globalObject.crypto.getRandomValues(values);
            token = Array.from(values, (value) => value.toString(16).padStart(8, '0')).join('-');
        } else {
            token = `${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`;
        }
        return `${OPFS_SNAPSHOT_FILE_PREFIX}${token}.bin`;
    }

    function copySnapshot(snapshot) {
        if (typeof snapshot === 'string') return snapshot;
        if (isBlob(snapshot)) return snapshot.slice(0, snapshot.size, snapshot.type);
        if (typeof ArrayBuffer !== 'undefined' && snapshot instanceof ArrayBuffer) {
            return snapshot.slice(0);
        }
        if (isArrayBufferView(snapshot)) {
            return snapshot.buffer.slice(snapshot.byteOffset, snapshot.byteOffset + snapshot.byteLength);
        }
        throw new WorkspaceStorageError(
            'A snapshot must be a string, Blob, ArrayBuffer, or TypedArray.',
            'INVALID_SNAPSHOT'
        );
    }

    function snapshotSizeBytes(snapshot) {
        if (typeof snapshot === 'string') {
            if (typeof globalObject.TextEncoder === 'function') {
                return new globalObject.TextEncoder().encode(snapshot).byteLength;
            }
            // Base64/GOB snapshots are ASCII.  The fallback deliberately errs
            // on the high side for arbitrary non-ASCII strings.
            return snapshot.length * 2;
        }
        if (isBlob(snapshot)) return snapshot.size;
        if (typeof ArrayBuffer !== 'undefined' && snapshot instanceof ArrayBuffer) return snapshot.byteLength;
        if (isArrayBufferView(snapshot)) return snapshot.byteLength;
        return 0;
    }

    function normalizeSnapshotEncoding(value, snapshot, format) {
        if (value === undefined || value === null) {
            return typeof snapshot === 'string' && format === DEFAULT_FORMAT ? 'base64' : 'raw';
        }
        if (value !== 'raw' && value !== 'base64') {
            throw new WorkspaceStorageError(
                "snapshotEncoding must be either 'raw' or 'base64'.",
                'INVALID_SNAPSHOT_ENCODING'
            );
        }
        if (value === 'base64' && typeof snapshot !== 'string') {
            throw new WorkspaceStorageError(
                "snapshotEncoding 'base64' requires a string snapshot.",
                'INVALID_SNAPSHOT_ENCODING'
            );
        }
        return value;
    }

    function base64ToArrayBuffer(value) {
        if (typeof globalObject.atob !== 'function') {
            throw new WorkspaceStorageError(
                'This browser cannot decode base64 snapshots.',
                'BASE64_UNAVAILABLE'
            );
        }
        let binary;
        try {
            binary = globalObject.atob(value.replace(/\s+/g, ''));
        } catch (error) {
            throw new WorkspaceStorageError('The snapshot is not valid base64.', 'INVALID_SNAPSHOT', error);
        }
        const bytes = new Uint8Array(binary.length);
        for (let index = 0; index < binary.length; index += 1) {
            bytes[index] = binary.charCodeAt(index);
        }
        return bytes.buffer;
    }

    function arrayBufferToBase64(value) {
        if (typeof globalObject.btoa !== 'function') {
            throw new WorkspaceStorageError(
                'This browser cannot encode base64 snapshots.',
                'BASE64_UNAVAILABLE'
            );
        }
        const bytes = new Uint8Array(value);
        const chunks = [];
        const chunkSize = 0x8000;
        for (let offset = 0; offset < bytes.length; offset += chunkSize) {
            let chunk = '';
            const end = Math.min(offset + chunkSize, bytes.length);
            for (let index = offset; index < end; index += 1) {
                chunk += String.fromCharCode(bytes[index]);
            }
            chunks.push(chunk);
        }
        return globalObject.btoa(chunks.join(''));
    }

    function prepareSnapshotForStorage(snapshotInput, format, requestedEncoding) {
        const snapshot = copySnapshot(snapshotInput);
        const snapshotEncoding = normalizeSnapshotEncoding(requestedEncoding, snapshot, format);
        if (snapshotEncoding === 'base64') {
            const binary = base64ToArrayBuffer(snapshot);
            return {
                snapshot: binary,
                snapshotEncoding,
                sizeBytes: binary.byteLength,
            };
        }
        return {
            snapshot,
            snapshotEncoding,
            sizeBytes: snapshotSizeBytes(snapshot),
        };
    }

    function nullableByteCount(value) {
        const number = Number(value);
        return Number.isFinite(number) && number >= 0 ? number : null;
    }

    function indexedDBSnapshotForRead(record) {
        if (record.snapshotEncoding === 'base64') {
            return arrayBufferToBase64(record.snapshot);
        }
        return copySnapshot(record.snapshot);
    }

    function snapshotStorageFields(stagedSnapshot) {
        if (stagedSnapshot && stagedSnapshot.backend === 'opfs') {
            return {
                snapshotBackend: 'opfs',
                opfsFileName: stagedSnapshot.opfsFileName,
            };
        }
        return {
            snapshotBackend: 'indexeddb',
            snapshot: stagedSnapshot && stagedSnapshot.preparedSnapshot
                ? stagedSnapshot.preparedSnapshot.snapshot
                : undefined,
        };
    }

    function requestAsPromise(request) {
        return new Promise((resolve, reject) => {
            request.onsuccess = () => resolve(request.result);
            request.onerror = () => reject(request.error || new WorkspaceStorageError('IndexedDB request failed.'));
        });
    }

    function transactionAsPromise(transaction) {
        return new Promise((resolve, reject) => {
            transaction.oncomplete = () => resolve();
            transaction.onerror = () => reject(transaction.error || new WorkspaceStorageError('IndexedDB transaction failed.'));
            transaction.onabort = () => reject(transaction.error || new WorkspaceStorageError('IndexedDB transaction was aborted.'));
        });
    }

    function sortNewestFirst(left, right) {
        return right.generation - left.generation;
    }

    function toPublicWorkspace(record) {
        return {
            id: record.id,
            name: record.name,
            createdAt: record.createdAt,
            updatedAt: record.updatedAt,
            sizeBytes: Number(record.sizeBytes) || 0,
            currentGeneration: record.currentGeneration || null,
            lastKnownGoodGeneration: record.lastKnownGoodGeneration || null,
            nextGeneration: Number(record.nextGeneration) || 1,
            format: record.format || DEFAULT_FORMAT,
            snapshotEncoding: record.snapshotEncoding || 'raw',
            metadata: cloneMetadata(record.metadata),
            backend: snapshotBackendForRecord(record),
        };
    }

    function toPublicGeneration(record) {
        return {
            workspaceId: record.workspaceId,
            generation: record.generation,
            createdAt: record.createdAt,
            sizeBytes: Number(record.sizeBytes) || 0,
            format: record.format || DEFAULT_FORMAT,
            snapshotEncoding: record.snapshotEncoding || 'raw',
            metadata: cloneMetadata(record.metadata),
            backend: snapshotBackendForRecord(record),
        };
    }

    /**
     * IndexedDB implementation of the workspace API described in this file's
     * header.  `indexedDB` and `navigatorObject` are injectable for browser
     * tests; production callers normally use the defaults.
     */
    class WorkspaceStorage {
        constructor(options = {}) {
            this.databaseName = options.databaseName || DATABASE_NAME;
            this.maxGenerations = normalizeKeepGenerations(
                options.maxGenerations,
                DEFAULT_MAX_GENERATIONS
            );
            this.preferOPFS = Boolean(options.preferOPFS);
            this.opfsThresholdBytes = normalizeOPFSThreshold(
                options.opfsThresholdBytes,
                DEFAULT_OPFS_THRESHOLD_BYTES
            );
            this._indexedDB = options.indexedDB || globalObject.indexedDB || null;
            this._navigator = options.navigatorObject || globalObject.navigator || null;
            this._db = null;
            this._opening = null;
            this._writeQueues = new Map();
            this._opfsRoot = null;
            this._opfsRootOpening = null;
            this._opfsLastWriteError = null;
        }

        /** Open (or reuse) the IndexedDB database. */
        async open() {
            if (this._db) return this._db;
            if (this._opening) return this._opening;
            if (!this._indexedDB || typeof this._indexedDB.open !== 'function') {
                throw new WorkspaceStorageError(
                    'IndexedDB is unavailable in this browser context.',
                    'INDEXEDDB_UNAVAILABLE'
                );
            }

            this._opening = new Promise((resolve, reject) => {
                let request;
                try {
                    request = this._indexedDB.open(this.databaseName, DATABASE_VERSION);
                } catch (error) {
                    reject(new WorkspaceStorageError('Could not open IndexedDB.', 'INDEXEDDB_OPEN_FAILED', error));
                    return;
                }

                request.onupgradeneeded = () => {
                    const db = request.result;
                    const transaction = request.transaction;
                    if (!db.objectStoreNames.contains(WORKSPACES_STORE)) {
                        db.createObjectStore(WORKSPACES_STORE, { keyPath: 'id' });
                    }
                    let generations;
                    if (!db.objectStoreNames.contains(GENERATIONS_STORE)) {
                        generations = db.createObjectStore(GENERATIONS_STORE, {
                            keyPath: ['workspaceId', 'generation'],
                        });
                    } else {
                        generations = transaction.objectStore(GENERATIONS_STORE);
                    }
                    if (!generations.indexNames.contains(GENERATIONS_BY_WORKSPACE)) {
                        generations.createIndex(GENERATIONS_BY_WORKSPACE, 'workspaceId', { unique: false });
                    }
                };
                request.onsuccess = () => {
                    const db = request.result;
                    db.onversionchange = () => {
                        db.close();
                        if (this._db === db) this._db = null;
                    };
                    this._db = db;
                    resolve(db);
                };
                request.onerror = () => {
                    reject(new WorkspaceStorageError(
                        'Could not open the workspace database.',
                        'INDEXEDDB_OPEN_FAILED',
                        request.error
                    ));
                };
            });

            try {
                return await this._opening;
            } finally {
                this._opening = null;
            }
        }

        /** Close the database connection.  Stored workspaces are unaffected. */
        async close() {
            if (this._opening) {
                try {
                    await this._opening;
                } catch (_) {
                    // There is no connection to close after a failed open.
                }
            }
            if (this._db) this._db.close();
            this._db = null;
        }

        /** Report the selected backend and browser storage capabilities. */
        async getCapabilities() {
            const storage = this._navigator && this._navigator.storage;
            const opfsSupported = Boolean(storage && typeof storage.getDirectory === 'function');
            let opfsAvailable = false;
            if (opfsSupported) {
                try {
                    await this._getOPFSRoot();
                    opfsAvailable = true;
                } catch (_) {
                    // OPFS is optional. A write will safely fall back to
                    // IndexedDB if the root cannot be opened later either.
                }
            }
            let estimate = null;
            if (storage && typeof storage.estimate === 'function') {
                try {
                    estimate = await storage.estimate();
                } catch (_) {
                    // Quota inspection is informative, never required.
                }
            }
            const opfsSelected = this.preferOPFS && opfsAvailable;
            return {
                // This is the policy selected for eligible snapshots. The
                // `backend` on workspace/generation records reports where a
                // particular retained snapshot actually lives.
                backend: opfsSelected ? 'opfs' : 'indexeddb',
                indexedDB: Boolean(this._indexedDB && typeof this._indexedDB.open === 'function'),
                opfs: {
                    supported: opfsSupported,
                    available: opfsAvailable,
                    selected: opfsSelected,
                    preferred: this.preferOPFS,
                    thresholdBytes: this.opfsThresholdBytes,
                    lastWriteFailed: Boolean(this._opfsLastWriteError),
                },
                usageBytes: nullableByteCount(estimate && estimate.usage),
                quotaBytes: nullableByteCount(estimate && estimate.quota),
            };
        }

        /** Create a named workspace, optionally with its first snapshot. */
        async createWorkspace(input = {}) {
            const id = hasOwn(input, 'id') ? normalizeWorkspaceID(input.id) : createWorkspaceID();
            const name = normalizeWorkspaceName(input.name);
            const createdAt = nowISO();
            const hasSnapshot = hasOwn(input, 'snapshot');
            const metadata = cloneMetadata(input.metadata);
            const format = String(input.format || DEFAULT_FORMAT);
            const preparedSnapshot = hasSnapshot
                ? prepareSnapshotForStorage(input.snapshot, format, input.snapshotEncoding)
                : null;
            const workspace = {
                id,
                name,
                createdAt,
                updatedAt: createdAt,
                sizeBytes: hasSnapshot ? preparedSnapshot.sizeBytes : 0,
                currentGeneration: hasSnapshot ? 1 : null,
                lastKnownGoodGeneration: hasSnapshot ? 1 : null,
                nextGeneration: hasSnapshot ? 2 : 1,
                format,
                snapshotEncoding: hasSnapshot ? preparedSnapshot.snapshotEncoding : 'raw',
                metadata,
                snapshotBackend: 'indexeddb',
            };

            await this._enqueueWrite(id, async () => {
                const db = await this.open();
                const stagedSnapshot = hasSnapshot
                    ? await this._stageSnapshotForStorage(preparedSnapshot)
                    : null;
                if (stagedSnapshot) workspace.snapshotBackend = stagedSnapshot.backend;
                const generation = hasSnapshot ? {
                    workspaceId: id,
                    generation: 1,
                    createdAt,
                    sizeBytes: workspace.sizeBytes,
                    format,
                    snapshotEncoding: preparedSnapshot.snapshotEncoding,
                    metadata: cloneMetadata(metadata),
                    ...snapshotStorageFields(stagedSnapshot),
                } : null;
                let committed = false;
                try {
                    // `add()` checks uniqueness as part of the transaction
                    // that also writes the initial generation. A separate read
                    // before put() would leave a cross-tab time-of-check/time-
                    // of-use gap.
                    await this._createWorkspaceAtomically(db, workspace, generation);
                    committed = true;
                } finally {
                    // A pre-written OPFS file has no pointer until the IndexedDB
                    // transaction commits. Clean up only that known new file;
                    // never scan or remove arbitrary OPFS entries.
                    if (!committed) await this._discardStagedOPFSSnapshot(stagedSnapshot);
                }
            });

            return toPublicWorkspace(workspace);
        }

        /**
         * Save a new immutable generation and make it current.
         * `metadata` merges with existing metadata unless `replaceMetadata` is
         * true.  A save is serialized per WorkspaceStorage instance so rapid
         * autosaves retain order.  Generation allocation also occurs inside
         * an IndexedDB read/write transaction, which makes independent
         * instances and browser tabs safe.
         */
        async saveWorkspace(workspaceId, snapshotInput, options = {}) {
            const id = normalizeWorkspaceID(workspaceId);
            const keepGenerations = normalizeKeepGenerations(options.keepGenerations, this.maxGenerations);
            let savedWorkspace;

            await this._enqueueWrite(id, async () => {
                const db = await this.open();
                // Snapshot bytes must be staged before opening IndexedDB's
                // short-lived read/write transaction. The preflight also keeps
                // the historical default-encoding behaviour intact; the
                // atomic method rechecks it in case another tab changed the
                // workspace in the meantime.
                const preflight = await this._getRecord(db, WORKSPACES_STORE, id);
                if (!preflight) throw new WorkspaceNotFoundError(id);
                const preflightFormat = String(options.format || preflight.format || DEFAULT_FORMAT);
                const preflightEncoding = options.snapshotEncoding === undefined
                    ? preflight.snapshotEncoding
                    : options.snapshotEncoding;
                const preparedSnapshot = prepareSnapshotForStorage(
                    snapshotInput,
                    preflightFormat,
                    preflightEncoding
                );
                const stagedSnapshot = await this._stageSnapshotForStorage(preparedSnapshot);
                let outcome = null;
                try {
                    outcome = await this._saveWorkspaceAtomically(
                        db,
                        id,
                        snapshotInput,
                        preparedSnapshot,
                        stagedSnapshot,
                        options,
                        keepGenerations
                    );
                } finally {
                    // If another tab changed the chosen encoding while this
                    // save waited for its transaction, the new OPFS candidate
                    // is intentionally not linked and can be safely discarded.
                    if (!outcome || !outcome.usedStagedOPFS) {
                        await this._discardStagedOPFSSnapshot(stagedSnapshot);
                    }
                }
                // IndexedDB no longer points at these generations. Deleting
                // their immutable OPFS files only now avoids data loss if the
                // transaction was aborted or failed.
                await this._deleteCommittedOPFSSnapshots(outcome.obsoleteGenerations);
                savedWorkspace = outcome.workspace;
            });

            return toPublicWorkspace(savedWorkspace);
        }

        /** Return workspace metadata sorted by most recently changed first. */
        async listWorkspaces() {
            const db = await this.open();
            const records = await this._getAllRecords(db, WORKSPACES_STORE);
            return records
                .map(toPublicWorkspace)
                .sort((left, right) => String(right.updatedAt).localeCompare(String(left.updatedAt)));
        }

        /** Return metadata for one workspace without reading a snapshot. */
        async getWorkspace(workspaceId) {
            const id = normalizeWorkspaceID(workspaceId);
            const db = await this.open();
            const record = await this._getRecord(db, WORKSPACES_STORE, id);
            if (!record) throw new WorkspaceNotFoundError(id);
            return toPublicWorkspace(record);
        }

        /**
         * Load the current snapshot, or one explicitly requested generation.
         * Workspaces created without a snapshot return `snapshot: null` and
         * `generation: null` instead of failing.
         */
        async loadWorkspace(workspaceId, options = {}) {
            const id = normalizeWorkspaceID(workspaceId);
            const db = await this.open();
            const workspace = await this._getRecord(db, WORKSPACES_STORE, id);
            if (!workspace) throw new WorkspaceNotFoundError(id);
            const requested = options.generation === undefined || options.generation === null
                ? workspace.currentGeneration
                : normalizeGeneration(options.generation);
            if (!requested) {
                return {
                    workspace: toPublicWorkspace(workspace),
                    generation: null,
                    snapshot: null,
                };
            }
            const generation = await this._getGenerationRecord(db, id, requested);
            if (!generation) {
                throw new WorkspaceStorageError(
                    `Workspace ${id} points to missing generation ${requested}.`,
                    'WORKSPACE_GENERATION_MISSING'
                );
            }
            return {
                workspace: toPublicWorkspace(workspace),
                generation: toPublicGeneration(generation),
                snapshot: await this._snapshotForRead(generation),
            };
        }

        /** Return generation metadata newest first, without snapshot bytes. */
        async listGenerations(workspaceId) {
            const id = normalizeWorkspaceID(workspaceId);
            const db = await this.open();
            const workspace = await this._getRecord(db, WORKSPACES_STORE, id);
            if (!workspace) throw new WorkspaceNotFoundError(id);
            const records = await this._getGenerations(db, id);
            return records.sort(sortNewestFirst).map(toPublicGeneration);
        }

        /** Rename a workspace without creating a new snapshot generation. */
        async renameWorkspace(workspaceId, nameInput) {
            const id = normalizeWorkspaceID(workspaceId);
            const name = normalizeWorkspaceName(nameInput);
            let renamed;
            await this._enqueueWrite(id, async () => {
                const db = await this.open();
                const current = await this._getRecord(db, WORKSPACES_STORE, id);
                if (!current) throw new WorkspaceNotFoundError(id);
                renamed = { ...current, name, updatedAt: nowISO() };
                await this._putRecord(db, WORKSPACES_STORE, renamed);
            });
            return toPublicWorkspace(renamed);
        }

        /** Make an existing generation current without copying its snapshot. */
        async restoreGeneration(workspaceId, generationInput) {
            const id = normalizeWorkspaceID(workspaceId);
            const generationNumber = normalizeGeneration(generationInput);
            let restored;
            await this._enqueueWrite(id, async () => {
                const db = await this.open();
                const current = await this._getRecord(db, WORKSPACES_STORE, id);
                if (!current) throw new WorkspaceNotFoundError(id);
                const generation = await this._getGenerationRecord(db, id, generationNumber);
                if (!generation) {
                    throw new WorkspaceStorageError(
                        `Generation ${generationNumber} does not exist for workspace ${id}.`,
                        'WORKSPACE_GENERATION_MISSING'
                    );
                }
                restored = {
                    ...current,
                    updatedAt: nowISO(),
                    sizeBytes: generation.sizeBytes,
                    currentGeneration: generationNumber,
                    lastKnownGoodGeneration: generationNumber,
                    format: generation.format || current.format || DEFAULT_FORMAT,
                    snapshotEncoding: generation.snapshotEncoding || current.snapshotEncoding || 'raw',
                    metadata: cloneMetadata(generation.metadata),
                    snapshotBackend: snapshotBackendForRecord(generation),
                };
                await this._putRecord(db, WORKSPACES_STORE, restored);
            });
            return toPublicWorkspace(restored);
        }

        /**
         * Try current and retained generations until `validate` accepts one.
         * The validator receives `(snapshot, generationMetadata,
         * workspaceMetadata)` and may return a boolean or Promise<boolean>.
         * Without a validator, the first available generation is returned;
         * callers that need corruption detection should always provide one.
         */
        async recoverWorkspace(workspaceId, options = {}) {
            const id = normalizeWorkspaceID(workspaceId);
            const workspace = await this.getWorkspace(id);
            const db = await this.open();
            const allGenerations = (await this._getGenerations(db, id)).sort(sortNewestFirst);
            if (allGenerations.length === 0) {
                return {
                    workspace,
                    generation: null,
                    snapshot: null,
                    recovered: false,
                    attemptedGenerations: [],
                };
            }

            const byGeneration = new Map(allGenerations.map((record) => [record.generation, record]));
            const ordered = [];
            for (const generation of [workspace.currentGeneration, workspace.lastKnownGoodGeneration]) {
                if (generation && byGeneration.has(generation)
                    && !ordered.some((candidate) => candidate.generation === generation)) {
                    ordered.push(byGeneration.get(generation));
                }
            }
            for (const generation of allGenerations) {
                if (!ordered.some((candidate) => candidate.generation === generation.generation)) {
                    ordered.push(generation);
                }
            }

            const attemptedGenerations = [];
            let lastError;
            for (const record of ordered) {
                attemptedGenerations.push(record.generation);
                let snapshot;
                try {
                    snapshot = await this._snapshotForRead(record);
                } catch (error) {
                    // A missing/corrupt OPFS file should not prevent recovery
                    // from trying an older immutable generation.
                    lastError = error;
                    continue;
                }
                let valid = true;
                if (typeof options.validate === 'function') {
                    try {
                        valid = Boolean(await options.validate(
                            snapshot,
                            toPublicGeneration(record),
                            workspace
                        ));
                    } catch (error) {
                        valid = false;
                        lastError = error;
                    }
                }
                if (!valid) continue;

                const recovered = record.generation !== workspace.currentGeneration;
                if (recovered || record.generation !== workspace.lastKnownGoodGeneration) {
                    await this.restoreGeneration(id, record.generation);
                }
                const loaded = await this.loadWorkspace(id, { generation: record.generation });
                return {
                    ...loaded,
                    recovered,
                    attemptedGenerations,
                };
            }

            throw new WorkspaceRecoveryError(id, attemptedGenerations, lastError);
        }

        /** Delete a workspace and every retained generation. */
        async deleteWorkspace(workspaceId) {
            const id = normalizeWorkspaceID(workspaceId);
            await this._enqueueWrite(id, async () => {
                const db = await this.open();
                const current = await this._getRecord(db, WORKSPACES_STORE, id);
                if (!current) throw new WorkspaceNotFoundError(id);
                const generations = await this._getGenerations(db, id);
                await this._deleteWorkspaceAndGenerations(db, id, generations);
                // The metadata transaction has committed, so none of these
                // files remains reachable. Best-effort cleanup must not make
                // a successful workspace deletion fail or touch other files.
                await this._deleteCommittedOPFSSnapshots(generations);
            });
        }

        /**
         * Convenience bridge for the existing localStorage snapshot.  It does
         * not read localStorage itself, so migration policy stays in the UI.
         */
        async importLegacySnapshot(snapshot, options = {}) {
            const input = {
                name: options.name || 'Recovered local database',
                snapshot,
                format: options.format || DEFAULT_FORMAT,
                metadata: {
                    ...cloneMetadata(options.metadata),
                    migratedFrom: options.migratedFrom || 'localStorage',
                },
            };
            if (options.id !== undefined && options.id !== null) input.id = options.id;
            return this.createWorkspace(input);
        }

        async _getOPFSRoot() {
            if (this._opfsRoot) return this._opfsRoot;
            if (this._opfsRootOpening) return this._opfsRootOpening;
            const storage = this._navigator && this._navigator.storage;
            if (!storage || typeof storage.getDirectory !== 'function') {
                throw new WorkspaceStorageError(
                    'Origin Private File System is unavailable in this browser context.',
                    'OPFS_UNAVAILABLE'
                );
            }

            this._opfsRootOpening = (async () => {
                try {
                    const root = await storage.getDirectory();
                    if (!root || typeof root.getDirectoryHandle !== 'function') {
                        throw new WorkspaceStorageError(
                            'The browser returned an invalid OPFS root directory.',
                            'OPFS_UNAVAILABLE'
                        );
                    }
                    this._opfsRoot = root;
                    return root;
                } catch (error) {
                    throw error instanceof WorkspaceStorageError
                        ? error
                        : new WorkspaceStorageError(
                            'Could not open Origin Private File System storage.',
                            'OPFS_UNAVAILABLE',
                            error
                        );
                }
            })();

            try {
                return await this._opfsRootOpening;
            } finally {
                this._opfsRootOpening = null;
            }
        }

        async _getOPFSSnapshotDirectory(create = false) {
            const root = await this._getOPFSRoot();
            try {
                return await root.getDirectoryHandle(OPFS_SNAPSHOTS_DIRECTORY, {
                    create: Boolean(create),
                });
            } catch (error) {
                throw new WorkspaceStorageError(
                    'Could not open the tinySQL OPFS snapshot directory.',
                    'OPFS_DIRECTORY_UNAVAILABLE',
                    error
                );
            }
        }

        _shouldUseOPFS(preparedSnapshot) {
            return this.preferOPFS
                && preparedSnapshot
                && preparedSnapshot.snapshotEncoding === 'raw'
                && isBinarySnapshot(preparedSnapshot.snapshot)
                && preparedSnapshot.sizeBytes >= this.opfsThresholdBytes;
        }

        /**
         * Return an IndexedDB-backed staged snapshot by default. OPFS is an
         * optional optimization; every failure leaves the caller with the
         * portable copy that was already prepared in memory.
         */
        async _stageSnapshotForStorage(preparedSnapshot) {
            const indexedDBStage = {
                backend: 'indexeddb',
                preparedSnapshot,
            };
            if (!this._shouldUseOPFS(preparedSnapshot)) return indexedDBStage;

            try {
                const descriptor = await this._writeOPFSSnapshot(preparedSnapshot.snapshot);
                this._opfsLastWriteError = null;
                return {
                    backend: 'opfs',
                    preparedSnapshot,
                    opfsFileName: descriptor.opfsFileName,
                };
            } catch (error) {
                // Do not make a workspace unavailable merely because OPFS is
                // full, disabled, or fails transiently. The IndexedDB record
                // will retain the same immutable bytes instead.
                this._opfsLastWriteError = error;
                return indexedDBStage;
            }
        }

        async _reserveOPFSSnapshotFileName(directory) {
            for (let attempt = 0; attempt < 16; attempt += 1) {
                const fileName = createOPFSSnapshotFileName();
                try {
                    // OPFS has no exclusive-create flag. A cryptographically
                    // unique name plus this probe means we never deliberately
                    // open an existing immutable snapshot for writing.
                    await directory.getFileHandle(fileName);
                } catch (error) {
                    if (isNotFoundError(error)) return fileName;
                    throw new WorkspaceStorageError(
                        'Could not reserve a unique OPFS snapshot file.',
                        'OPFS_WRITE_FAILED',
                        error
                    );
                }
            }
            throw new WorkspaceStorageError(
                'Could not allocate a unique OPFS snapshot filename.',
                'OPFS_WRITE_FAILED'
            );
        }

        async _writeOPFSSnapshot(snapshot) {
            let directory = null;
            let fileName = null;
            let created = false;
            let writable = null;
            try {
                directory = await this._getOPFSSnapshotDirectory(true);
                fileName = await this._reserveOPFSSnapshotFileName(directory);
                const file = await directory.getFileHandle(fileName, { create: true });
                created = true;
                writable = await file.createWritable();
                await writable.write(snapshot);
                await writable.close();
                writable = null;
                return { opfsFileName: fileName };
            } catch (error) {
                if (writable) {
                    try {
                        if (typeof writable.abort === 'function') {
                            await writable.abort();
                        } else if (typeof writable.close === 'function') {
                            await writable.close();
                        }
                    } catch (_) {
                        // The known new file is removed below when possible.
                    }
                }
                if (created && directory && fileName) {
                    try {
                        await directory.removeEntry(fileName);
                    } catch (_) {
                        // A crashed/failed pre-write can leave an unreachable
                        // orphan. It is safer to leave that file than to risk
                        // deleting anything outside this exact generated name.
                    }
                }
                throw error instanceof WorkspaceStorageError
                    ? error
                    : new WorkspaceStorageError(
                        'Could not write the workspace snapshot to OPFS.',
                        'OPFS_WRITE_FAILED',
                        error
                    );
            }
        }

        async _removeOPFSSnapshotFile(fileName, directory) {
            if (!isManagedOPFSSnapshotFileName(fileName)) {
                throw new WorkspaceStorageError(
                    'Refusing to remove an unmanaged OPFS snapshot filename.',
                    'OPFS_UNSAFE_FILENAME'
                );
            }
            const targetDirectory = directory || await this._getOPFSSnapshotDirectory(false);
            await targetDirectory.removeEntry(fileName);
        }

        async _discardStagedOPFSSnapshot(stagedSnapshot) {
            if (!stagedSnapshot || stagedSnapshot.backend !== 'opfs') return;
            try {
                await this._removeOPFSSnapshotFile(stagedSnapshot.opfsFileName);
            } catch (_) {
                // It was never reachable from IndexedDB, and an incomplete
                // cleanup must not mask the original workspace error.
            }
        }

        async _deleteCommittedOPFSSnapshots(generations) {
            const fileNames = Array.from(new Set((Array.isArray(generations) ? generations : [])
                .filter(hasManagedOPFSSnapshotRecord)
                .map((generation) => generation.opfsFileName)));
            if (!fileNames.length) return;

            let directory;
            try {
                directory = await this._getOPFSSnapshotDirectory(false);
            } catch (_) {
                // The IndexedDB mutation is already durable. Leaving an
                // unreachable file is preferable to reporting a failed save
                // or delete after its metadata has committed.
                return;
            }
            for (const fileName of fileNames) {
                try {
                    await this._removeOPFSSnapshotFile(fileName, directory);
                } catch (_) {
                    // No broad cleanup/retry scan here: retain conservative
                    // semantics and leave an orphan rather than risk data.
                }
            }
        }

        async _snapshotForRead(record) {
            if (snapshotBackendForRecord(record) !== 'opfs') {
                return indexedDBSnapshotForRead(record);
            }
            if (!isManagedOPFSSnapshotFileName(record.opfsFileName)) {
                throw new WorkspaceStorageError(
                    'The workspace generation has an invalid OPFS snapshot filename.',
                    'OPFS_SNAPSHOT_UNAVAILABLE'
                );
            }
            try {
                const directory = await this._getOPFSSnapshotDirectory(false);
                const fileHandle = await directory.getFileHandle(record.opfsFileName);
                const file = await fileHandle.getFile();
                if (!file || typeof file.arrayBuffer !== 'function') {
                    throw new WorkspaceStorageError(
                        'The OPFS snapshot file could not be read as binary data.',
                        'OPFS_SNAPSHOT_UNAVAILABLE'
                    );
                }
                // OPFS generations are currently written only for raw binary
                // values. Retain a defensive base64 branch so future records
                // remain compatible with the public read contract.
                if (record.snapshotEncoding === 'base64') {
                    return arrayBufferToBase64(await file.arrayBuffer());
                }
                return file;
            } catch (error) {
                throw error instanceof WorkspaceStorageError
                    ? error
                    : new WorkspaceStorageError(
                        'Could not load the OPFS workspace snapshot.',
                        'OPFS_SNAPSHOT_UNAVAILABLE',
                        error
                    );
            }
        }

        async _getRecord(db, storeName, key) {
            const transaction = db.transaction(storeName, 'readonly');
            const result = await requestAsPromise(transaction.objectStore(storeName).get(key));
            await transactionAsPromise(transaction);
            return result || null;
        }

        async _getAllRecords(db, storeName) {
            const transaction = db.transaction(storeName, 'readonly');
            const result = await requestAsPromise(transaction.objectStore(storeName).getAll());
            await transactionAsPromise(transaction);
            return Array.isArray(result) ? result : [];
        }

        async _getGenerationRecord(db, workspaceId, generation) {
            const transaction = db.transaction(GENERATIONS_STORE, 'readonly');
            const result = await requestAsPromise(
                transaction.objectStore(GENERATIONS_STORE).get([workspaceId, generation])
            );
            await transactionAsPromise(transaction);
            return result || null;
        }

        async _getGenerations(db, workspaceId) {
            const transaction = db.transaction(GENERATIONS_STORE, 'readonly');
            const index = transaction.objectStore(GENERATIONS_STORE).index(GENERATIONS_BY_WORKSPACE);
            const result = await requestAsPromise(index.getAll(workspaceId));
            await transactionAsPromise(transaction);
            return Array.isArray(result) ? result : [];
        }

        /**
         * Atomically create a workspace and (when supplied) its first
         * generation.  `add()` is intentional: it is the cross-tab-safe
         * existence check, unlike a preceding readonly `get()` plus `put()`.
         */
        async _createWorkspaceAtomically(db, workspace, generation) {
            return new Promise((resolve, reject) => {
                let transaction;
                let failure = null;
                let settled = false;

                const settleReject = (error) => {
                    if (settled) return;
                    settled = true;
                    reject(error);
                };
                const failAndAbort = (error) => {
                    failure = error;
                    try {
                        transaction.abort();
                    } catch (_) {
                        settleReject(error);
                    }
                };

                try {
                    transaction = db.transaction([WORKSPACES_STORE, GENERATIONS_STORE], 'readwrite');
                    transaction.oncomplete = () => {
                        if (settled) return;
                        settled = true;
                        resolve();
                    };
                    transaction.onerror = () => {
                        settleReject(failure || new WorkspaceStorageError(
                            'Could not create the workspace.',
                            'WORKSPACE_CREATE_FAILED',
                            transaction.error
                        ));
                    };
                    transaction.onabort = () => {
                        settleReject(failure || new WorkspaceStorageError(
                            'Could not create the workspace.',
                            'WORKSPACE_CREATE_FAILED',
                            transaction.error
                        ));
                    };

                    const workspaceRequest = transaction.objectStore(WORKSPACES_STORE).add(workspace);
                    workspaceRequest.onerror = () => {
                        const error = workspaceRequest.error;
                        failure = error && error.name === 'ConstraintError'
                            ? new WorkspaceStorageError(
                                `Workspace already exists: ${workspace.id}`,
                                'WORKSPACE_EXISTS',
                                error
                            )
                            : new WorkspaceStorageError(
                                'Could not create the workspace.',
                                'WORKSPACE_CREATE_FAILED',
                                error
                            );
                    };

                    if (generation) {
                        const generationRequest = transaction
                            .objectStore(GENERATIONS_STORE)
                            .add(generation);
                        generationRequest.onerror = () => {
                            if (!failure) {
                                failure = new WorkspaceStorageError(
                                    'Could not create the initial workspace generation.',
                                    'WORKSPACE_CREATE_FAILED',
                                    generationRequest.error
                                );
                            }
                        };
                    }
                } catch (error) {
                    const wrapped = error instanceof WorkspaceStorageError
                        ? error
                        : new WorkspaceStorageError(
                            'Could not create the workspace.',
                            'WORKSPACE_CREATE_FAILED',
                            error
                        );
                    if (transaction) {
                        failAndAbort(wrapped);
                    } else {
                        settleReject(wrapped);
                    }
                }
            });
        }

        /**
         * Allocate and save one immutable generation within a single
         * read/write transaction.  The workspace record is read from that
         * transaction, not beforehand, so IndexedDB's write-transaction
         * scheduler serializes allocation across tabs and connections.
         */
        async _saveWorkspaceAtomically(
            db,
            workspaceId,
            snapshotInput,
            prepreparedSnapshot,
            stagedSnapshot,
            options,
            keepGenerations
        ) {
            return new Promise((resolve, reject) => {
                let transaction;
                let savedWorkspace = null;
                let obsoleteGenerations = [];
                let usedStagedOPFS = false;
                let failure = null;
                let settled = false;

                const settleReject = (error) => {
                    if (settled) return;
                    settled = true;
                    reject(error);
                };
                const failAndAbort = (error) => {
                    failure = error;
                    try {
                        transaction.abort();
                    } catch (_) {
                        settleReject(error);
                    }
                };
                const requestFailure = (message, code, error) => new WorkspaceStorageError(
                    message,
                    code,
                    error
                );

                try {
                    transaction = db.transaction([WORKSPACES_STORE, GENERATIONS_STORE], 'readwrite');
                    const workspaces = transaction.objectStore(WORKSPACES_STORE);
                    const generations = transaction.objectStore(GENERATIONS_STORE);
                    const generationIndex = generations.index(GENERATIONS_BY_WORKSPACE);

                    transaction.oncomplete = () => {
                        if (settled) return;
                        settled = true;
                        resolve({
                            workspace: savedWorkspace,
                            obsoleteGenerations,
                            usedStagedOPFS,
                        });
                    };
                    transaction.onerror = () => {
                        settleReject(failure || requestFailure(
                            'Could not save the workspace.',
                            'WORKSPACE_SAVE_FAILED',
                            transaction.error
                        ));
                    };
                    transaction.onabort = () => {
                        settleReject(failure || requestFailure(
                            'Could not save the workspace.',
                            'WORKSPACE_SAVE_FAILED',
                            transaction.error
                        ));
                    };

                    const workspaceRequest = workspaces.get(workspaceId);
                    workspaceRequest.onerror = () => {
                        failure = requestFailure(
                            'Could not read the workspace before saving.',
                            'WORKSPACE_SAVE_FAILED',
                            workspaceRequest.error
                        );
                    };
                    workspaceRequest.onsuccess = () => {
                        const current = workspaceRequest.result;
                        if (!current) {
                            failAndAbort(new WorkspaceNotFoundError(workspaceId));
                            return;
                        }

                        // Queue this read while the transaction is active.
                        // It also repairs stale nextGeneration values left by
                        // an older client: allocation is always above every
                        // stored numeric generation.
                        const existingGenerationsRequest = generationIndex.getAll(workspaceId);
                        existingGenerationsRequest.onerror = () => {
                            failure = requestFailure(
                                'Could not read workspace generations before saving.',
                                'WORKSPACE_SAVE_FAILED',
                                existingGenerationsRequest.error
                            );
                        };
                        existingGenerationsRequest.onsuccess = () => {
                            try {
                                const existingGenerations = Array.isArray(existingGenerationsRequest.result)
                                    ? existingGenerationsRequest.result
                                    : [];
                                const generationNumber = this._allocateGenerationNumber(
                                    current,
                                    existingGenerations
                                );
                                const savedAt = nowISO();
                                const metadata = mergeMetadata(
                                    current.metadata,
                                    options.metadata,
                                    Boolean(options.replaceMetadata)
                                );
                                const format = String(options.format || current.format || DEFAULT_FORMAT);
                                const requestedSnapshotEncoding = options.snapshotEncoding === undefined
                                    ? current.snapshotEncoding
                                    : options.snapshotEncoding;
                                const normalizedSnapshotEncoding = normalizeSnapshotEncoding(
                                    requestedSnapshotEncoding,
                                    snapshotInput,
                                    format
                                );
                                // The pre-written OPFS file represents the
                                // copied snapshot captured before this
                                // transaction. Reuse it only when the current
                                // workspace still selects the same encoding;
                                // otherwise preserve historical behaviour by
                                // preparing the source input again below.
                                const preparedSnapshot = prepreparedSnapshot
                                    && prepreparedSnapshot.snapshotEncoding === normalizedSnapshotEncoding
                                    ? prepreparedSnapshot
                                    : prepareSnapshotForStorage(
                                        snapshotInput,
                                        format,
                                        requestedSnapshotEncoding
                                    );
                                const useStagedSnapshot = stagedSnapshot
                                    && stagedSnapshot.backend === 'opfs'
                                    && preparedSnapshot === prepreparedSnapshot;
                                const storedSnapshot = useStagedSnapshot
                                    ? stagedSnapshot
                                    : {
                                        backend: 'indexeddb',
                                        preparedSnapshot,
                                    };
                                const workspace = {
                                    ...current,
                                    updatedAt: savedAt,
                                    sizeBytes: preparedSnapshot.sizeBytes,
                                    currentGeneration: generationNumber,
                                    lastKnownGoodGeneration: generationNumber,
                                    nextGeneration: generationNumber + 1,
                                    format,
                                    snapshotEncoding: preparedSnapshot.snapshotEncoding,
                                    metadata,
                                    snapshotBackend: storedSnapshot.backend,
                                };
                                const generation = {
                                    workspaceId,
                                    generation: generationNumber,
                                    createdAt: savedAt,
                                    sizeBytes: preparedSnapshot.sizeBytes,
                                    format,
                                    snapshotEncoding: preparedSnapshot.snapshotEncoding,
                                    metadata: cloneMetadata(metadata),
                                    ...snapshotStorageFields(storedSnapshot),
                                };

                                const workspacePutRequest = workspaces.put(workspace);
                                workspacePutRequest.onerror = () => {
                                    if (!failure) {
                                        failure = requestFailure(
                                            'Could not update the workspace pointer.',
                                            'WORKSPACE_SAVE_FAILED',
                                            workspacePutRequest.error
                                        );
                                    }
                                };
                                const generationAddRequest = generations.add(generation);
                                generationAddRequest.onerror = () => {
                                    if (!failure) {
                                        failure = requestFailure(
                                            'Could not write the workspace generation.',
                                            'WORKSPACE_SAVE_FAILED',
                                            generationAddRequest.error
                                        );
                                    }
                                };

                                const allGenerations = existingGenerations.concat([generation]);
                                const obsolete = this._findObsoleteGenerations(
                                    allGenerations,
                                    workspace,
                                    keepGenerations
                                );
                                for (const obsoleteGeneration of obsolete) {
                                    const deleteRequest = generations.delete([
                                        workspaceId,
                                        obsoleteGeneration.generation,
                                    ]);
                                    deleteRequest.onerror = () => {
                                        if (!failure) {
                                            failure = requestFailure(
                                                'Could not trim old workspace generations.',
                                                'WORKSPACE_SAVE_FAILED',
                                                deleteRequest.error
                                            );
                                        }
                                    };
                                }
                                obsoleteGenerations = obsolete;
                                usedStagedOPFS = Boolean(useStagedSnapshot);
                                savedWorkspace = workspace;
                            } catch (error) {
                                failAndAbort(error instanceof WorkspaceStorageError
                                    ? error
                                    : requestFailure(
                                        'Could not prepare the workspace snapshot for saving.',
                                        'WORKSPACE_SAVE_FAILED',
                                        error
                                    ));
                            }
                        };
                    };
                } catch (error) {
                    const wrapped = error instanceof WorkspaceStorageError
                        ? error
                        : requestFailure('Could not save the workspace.', 'WORKSPACE_SAVE_FAILED', error);
                    if (transaction) {
                        failAndAbort(wrapped);
                    } else {
                        settleReject(wrapped);
                    }
                }
            });
        }

        _allocateGenerationNumber(workspace, generations) {
            const requestedNext = Number(workspace.nextGeneration);
            const minimumNext = Number.isSafeInteger(requestedNext) && requestedNext >= 1
                ? requestedNext
                : 1;
            const highestGeneration = generations.reduce((highest, record) => {
                const generation = Number(record && record.generation);
                return Number.isSafeInteger(generation) && generation > highest
                    ? generation
                    : highest;
            }, 0);
            const generation = Math.max(minimumNext, highestGeneration + 1);
            if (!Number.isSafeInteger(generation)) {
                throw new WorkspaceStorageError(
                    'The workspace has reached the maximum generation number.',
                    'GENERATION_LIMIT_REACHED'
                );
            }
            return generation;
        }

        _findObsoleteGenerations(generations, workspace, keepGenerations) {
            const newestFirst = generations.slice().sort(sortNewestFirst);
            const protectedGenerations = new Set([
                workspace.currentGeneration,
                workspace.lastKnownGoodGeneration,
            ].filter(Boolean));
            const kept = new Set(newestFirst
                .slice(0, keepGenerations)
                .map((record) => record.generation));
            for (const generation of protectedGenerations) kept.add(generation);
            return newestFirst.filter((record) => !kept.has(record.generation));
        }

        async _putRecord(db, storeName, record) {
            const transaction = db.transaction(storeName, 'readwrite');
            transaction.objectStore(storeName).put(record);
            await transactionAsPromise(transaction);
        }

        async _deleteWorkspaceAndGenerations(db, workspaceId, generations) {
            const transaction = db.transaction([WORKSPACES_STORE, GENERATIONS_STORE], 'readwrite');
            transaction.objectStore(WORKSPACES_STORE).delete(workspaceId);
            const generationStore = transaction.objectStore(GENERATIONS_STORE);
            for (const generation of generations) {
                generationStore.delete([workspaceId, generation.generation]);
            }
            await transactionAsPromise(transaction);
        }

        _enqueueWrite(workspaceId, operation) {
            const previous = this._writeQueues.get(workspaceId) || Promise.resolve();
            const next = previous.catch(() => undefined).then(operation);
            this._writeQueues.set(workspaceId, next);
            return next.finally(() => {
                if (this._writeQueues.get(workspaceId) === next) {
                    this._writeQueues.delete(workspaceId);
                }
            });
        }
    }

    const api = Object.freeze({
        WorkspaceStorage,
        WorkspaceStorageError,
        WorkspaceNotFoundError,
        WorkspaceRecoveryError,
        DATABASE_NAME,
        DATABASE_VERSION,
    });

    globalObject.TinySQLWorkspaceStorage = api;
    if (typeof module !== 'undefined' && module.exports) {
        module.exports = api;
    }
}(typeof globalThis !== 'undefined' ? globalThis : this));
