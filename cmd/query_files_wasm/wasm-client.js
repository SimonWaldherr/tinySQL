/* Promise-based client for wasm-worker.js. Loaded as a classic script. */
(function attachTinySQLWasmClient(globalObject) {
    'use strict';

    const METHODS = [
        'importFile', 'executeQuery', 'executeQueryStream', 'executeMulti', 'getResultPage',
        'clearDatabase', 'dropTable', 'listTables', 'exportResults',
        'getTableSchema', 'exportDatabase', 'exportDatabaseBytes',
        'importDatabase', 'importDatabaseBytes', 'validateDatabaseBytes',
        'getRuntimeStatus', 'setRuntimeIdentity',
    ];

    function abortError() {
        try {
            return new DOMException('WASM request was cancelled.', 'AbortError');
        } catch (_) {
            const error = new Error('WASM request was cancelled.');
            error.name = 'AbortError';
            return error;
        }
    }

    function workerError(payload, fallback) {
        const error = new Error(payload?.message || fallback || 'WASM worker request failed.');
        error.name = payload?.name || 'WasmWorkerError';
        if (payload?.code) error.code = payload.code;
        if (payload?.stack) error.stack = payload.stack;
        return error;
    }

    class TinySQLWasmClient extends EventTarget {
        constructor(options = {}) {
            super();
            this.options = {
                workerUrl: options.workerUrl || 'wasm-worker.js',
                wasmUrl: options.wasmUrl || 'query_files.wasm',
                wasmExecUrl: options.wasmExecUrl || 'wasm_exec.js',
                preferCompressed: options.preferCompressed !== false,
            };
            this.worker = null;
            this.ready = false;
            this._initializing = null;
            this._nextRequestId = 1;
            this._queued = [];
            this._active = null;
            this._handlers = new Map();
            this.availableMethods = new Set();
        }

        on(type, handler) {
            if (typeof handler !== 'function') return () => {};
            const handlers = this._handlers.get(type) || new Set();
            handlers.add(handler);
            this._handlers.set(type, handlers);
            return () => handlers.delete(handler);
        }

        _emit(type, detail) {
            this.dispatchEvent(new CustomEvent(type, { detail }));
            for (const handler of this._handlers.get(type) || []) {
                try { handler(detail); } catch (error) { console.error('WASM client event handler failed:', error); }
            }
        }

        async init() {
            if (this.ready) return this;
            if (this._initializing) return this._initializing;
            if (typeof Worker !== 'function') {
                throw new Error('This browser does not support Web Workers required by the local engine.');
            }
            const initPromise = new Promise((resolve, reject) => {
                let worker;
                try {
                    worker = new Worker(this.options.workerUrl);
                } catch (error) {
                    reject(workerError(error, 'Could not create the WASM worker.'));
                    return;
                }
                this.worker = worker;
                const cleanupInitError = (error) => {
                    if (this.ready) return;
                    this._initializing = null;
                    worker.terminate();
                    if (this.worker === worker) this.worker = null;
                    reject(error);
                };
                const handleWorkerFailure = (error) => {
                    if (!this.ready) {
                        cleanupInitError(error);
                        return;
                    }
                    this.ready = false;
                    if (this.worker === worker) this.worker = null;
                    worker.terminate();
                    this._emit('error', error);
                    this._failAll(error);
                };
                worker.onmessage = (event) => this._handleMessage(event.data, resolve, cleanupInitError);
                worker.onerror = (event) => handleWorkerFailure(workerError({ message: event.message, name: 'WorkerError' }));
                worker.onmessageerror = () => handleWorkerFailure(workerError({ message: 'WASM worker returned data that could not be cloned.' }));
                try {
                    worker.postMessage({ type: 'init', config: this.options });
                } catch (error) {
                    cleanupInitError(workerError(error, 'Could not start the WASM worker.'));
                }
            });
            this._initializing = initPromise;
            try {
                await initPromise;
                return this;
            } finally {
                // A failed initialisation must be retryable (for example after
                // a transient asset/network failure), rather than permanently
                // retaining its rejected promise.
                if (this._initializing === initPromise) this._initializing = null;
            }
        }

        _handleMessage(message, resolveInit, rejectInit) {
            if (!message || typeof message !== 'object') return;
            if (message.type === 'ready') {
                this.ready = true;
                this.availableMethods = new Set(Array.isArray(message.methods) ? message.methods : []);
                this._emit('ready', { methods: [...this.availableMethods] });
                resolveInit?.(this);
                return;
            }
            if (message.type === 'init-error') {
                rejectInit?.(workerError(message.error, 'Could not initialize the WASM worker.'));
                return;
            }
            if (message.type === 'fatal') {
                const error = workerError(message.error, 'The WASM worker stopped unexpectedly.');
                this.ready = false;
                this.worker?.terminate();
                this.worker = null;
                this._emit('error', error);
                this._failAll(error);
                rejectInit?.(error);
                return;
            }
            if (message.type === 'progress') {
                this._emit('progress', message);
                return;
            }
            if (message.type === 'status') {
                this._emit('status', message.status);
                return;
            }
            if (message.type === 'stream') {
                this._emit('stream', message);
                return;
            }
            if (message.type !== 'result' || !this._active || message.requestId !== this._active.requestId) return;

            const active = this._active;
            this._active = null;
            active.signal?.removeEventListener('abort', active.abortListener);
            if (!active.settled) {
                active.settled = true;
                if (message.cancelled) active.reject(abortError());
                else if (message.error) active.reject(workerError(message.error));
                else active.resolve(message.result);
            }
            this._pump();
        }

        call(method, args = [], options = {}) {
            if (!METHODS.includes(method)) {
                return Promise.reject(workerError({ message: `Unsupported WASM method: ${method}`, code: 'UNKNOWN_WASM_METHOD' }));
            }
            if (!this.ready || !this.worker) {
                return Promise.reject(workerError({
                    message: 'The WASM worker is not ready. Call init() before issuing requests.',
                    code: 'WASM_NOT_READY',
                }));
            }
            return new Promise((resolve, reject) => {
                const entry = {
                    requestId: this._nextRequestId++, method, args: Array.isArray(args) ? args : [],
                    resolve, reject, signal: options.signal, settled: false, dispatched: false,
                    cancelRequested: false, transfer: Array.isArray(options.transfer) ? options.transfer : [],
                };
                entry.abortListener = () => this._cancel(entry);
                if (entry.signal?.aborted) {
                    reject(abortError());
                    return;
                }
                entry.signal?.addEventListener('abort', entry.abortListener, { once: true });
                this._queued.push(entry);
                this._emit('progress', { type: 'progress', requestId: entry.requestId, method, phase: 'queued', elapsedMs: 0, estimated: false });
                this._pump();
            });
        }

        _cancel(entry) {
            if (entry.settled) return;
            if (entry.dispatched) {
                if (entry.cancelRequested) return;
                entry.cancelRequested = true;
                // Streaming calls resolve their Go export as a Promise. The
                // worker can therefore receive this message between batches
                // and cancel the associated Go context for real. Synchronous
                // legacy calls still acknowledge cancellation only once they
                // return, which remains safe and accurately represented.
                try {
                    this.worker?.postMessage({ type: 'cancel', requestId: entry.requestId });
                } catch (error) {
                    if (this._active === entry) this._active = null;
                    entry.signal?.removeEventListener('abort', entry.abortListener);
                    entry.settled = true;
                    entry.reject(workerError(error, 'Could not cancel the WASM request.'));
                    this._pump();
                    return;
                }
                this._emit('progress', {
                    type: 'progress', requestId: entry.requestId, method: entry.method,
                    phase: 'cancelling', elapsedMs: 0, estimated: false,
                });
            } else {
                entry.settled = true;
                entry.reject(abortError());
                this._queued = this._queued.filter((candidate) => candidate !== entry);
                entry.signal?.removeEventListener('abort', entry.abortListener);
                this._emit('progress', { type: 'progress', requestId: entry.requestId, method: entry.method, phase: 'cancelled', elapsedMs: 0, estimated: false });
            }
        }

        _pump() {
            if (this._active || !this.ready || !this.worker) return;
            const entry = this._queued.shift();
            if (!entry) return;
            if (entry.settled) {
                this._pump();
                return;
            }
            entry.dispatched = true;
            this._active = entry;
            try {
                const message = { type: 'call', requestId: entry.requestId, method: entry.method, args: entry.args };
                if (entry.transfer.length) this.worker.postMessage(message, entry.transfer);
                else this.worker.postMessage(message);
            } catch (error) {
                this._active = null;
                entry.signal?.removeEventListener('abort', entry.abortListener);
                if (!entry.settled) {
                    entry.settled = true;
                    entry.reject(workerError(error, 'Could not send the WASM request to its worker.'));
                }
                this._emit('progress', {
                    type: 'progress', requestId: entry.requestId, method: entry.method,
                    phase: 'failed', elapsedMs: 0, estimated: false, error: workerError(error),
                });
                this._pump();
            }
        }

        _failAll(error) {
            if (this._active) {
                this._active.signal?.removeEventListener('abort', this._active.abortListener);
                if (!this._active.settled) {
                    this._active.settled = true;
                    this._active.reject(error);
                }
            }
            this._active = null;
            for (const entry of this._queued.splice(0)) {
                entry.signal?.removeEventListener('abort', entry.abortListener);
                if (!entry.settled) {
                    entry.settled = true;
                    entry.reject(error);
                }
            }
        }

        terminate() {
            const error = workerError({ message: 'The WASM worker was terminated.', code: 'WASM_WORKER_TERMINATED' });
            this._failAll(error);
            this.worker?.terminate();
            this.worker = null;
            this.ready = false;
            this.availableMethods.clear();
            this._initializing = null;
        }

        supports(method) {
            return this.ready && this.availableMethods.has(method);
        }
    }

    for (const method of METHODS) {
        if (method === 'executeQueryStream' || method === 'importDatabaseBytes' || method === 'validateDatabaseBytes') continue;
        TinySQLWasmClient.prototype[method] = function wasmMethod(...args) {
            return this.call(method, args);
        };
    }

    TinySQLWasmClient.prototype.executeQueryStream = function executeQueryStream(sql, options = {}) {
        return this.call('executeQueryStream', [sql], options);
    };

    // Pass raw workspace snapshots through the worker as transferables. The
    // ArrayBuffer is an immutable copy returned by IndexedDB/OPFS, so detaching
    // the UI copy after hand-off avoids a second full structured-clone copy.
    TinySQLWasmClient.prototype.importDatabaseBytes = async function importDatabaseBytes(snapshot) {
        let value = snapshot;
        if (typeof Blob !== 'undefined' && value instanceof Blob) {
            value = await value.arrayBuffer();
        }
        if (typeof ArrayBuffer !== 'undefined' && value instanceof ArrayBuffer) {
            return this.call('importDatabaseBytes', [value], { transfer: [value] });
        }
        if (typeof ArrayBuffer !== 'undefined' && ArrayBuffer.isView(value)) {
            const bytes = value instanceof Uint8Array
                && value.byteOffset === 0 && value.byteLength === value.buffer.byteLength
                ? value
                : new Uint8Array(value.buffer.slice(value.byteOffset, value.byteOffset + value.byteLength));
            return this.call('importDatabaseBytes', [bytes], { transfer: [bytes.buffer] });
        }
        throw workerError({
            message: 'A binary workspace snapshot must be an ArrayBuffer, TypedArray, or Blob.',
            code: 'INVALID_SNAPSHOT',
        });
    };

    // Validation intentionally clones rather than transfers the bytes. The
    // caller still needs the same candidate snapshot for the following real
    // import once validation has selected a recovery generation.
    TinySQLWasmClient.prototype.validateDatabaseBytes = async function validateDatabaseBytes(snapshot) {
        let value = snapshot;
        if (typeof Blob !== 'undefined' && value instanceof Blob) {
            value = await value.arrayBuffer();
        }
        if (typeof ArrayBuffer !== 'undefined' && value instanceof ArrayBuffer) {
            return this.call('validateDatabaseBytes', [value]);
        }
        if (typeof ArrayBuffer !== 'undefined' && ArrayBuffer.isView(value)) {
            const bytes = value instanceof Uint8Array
                && value.byteOffset === 0 && value.byteLength === value.buffer.byteLength
                ? value
                : new Uint8Array(value.buffer.slice(value.byteOffset, value.byteOffset + value.byteLength));
            return this.call('validateDatabaseBytes', [bytes]);
        }
        throw workerError({
            message: 'A binary workspace snapshot must be an ArrayBuffer, TypedArray, or Blob.',
            code: 'INVALID_SNAPSHOT',
        });
    };

    globalObject.TinySQLWasmClient = TinySQLWasmClient;
    globalObject.createTinySQLWasmClient = (options) => new TinySQLWasmClient(options);
    if (typeof module !== 'undefined' && module.exports) {
        module.exports = { TinySQLWasmClient, METHODS };
    }
}(typeof globalThis !== 'undefined' ? globalThis : this));
