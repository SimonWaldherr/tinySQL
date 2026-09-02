/*
 * Owns the tinySQL Go/WASM runtime for the browser studio.
 *
 * The main thread never receives Go's globals. It talks to this worker with
 * a small message protocol instead, keeping imports and queries off the UI
 * thread. Calls are intentionally serialized: the current Go/WASM build owns
 * one in-memory database. Most exports are synchronous; the bounded
 * ResultStream path deliberately returns a Promise between result batches.
 */
(function tinySQLWasmWorker(scope) {
    'use strict';

    const exportedMethods = [
        'importFile', 'executeQuery', 'executeQueryStream', 'executeMulti', 'getResultPage',
        'clearDatabase', 'dropTable', 'listTables', 'exportResults',
        'getTableSchema', 'exportDatabase', 'exportDatabaseBytes',
        'importDatabase', 'importDatabaseBytes', 'validateDatabaseBytes',
        'getRuntimeStatus', 'setRuntimeIdentity',
    ];
    let initialized = false;
    let initialization = null;
    let callQueue = Promise.resolve();
    const cancelled = new Set();
    const activeCalls = new Map();

    function asError(error, fallback = 'WASM worker failed') {
        if (error instanceof Error) {
            return { message: error.message || fallback, name: error.name, stack: error.stack };
        }
        return { message: String(error || fallback), name: 'Error' };
    }

    function emit(message, transfer) {
        if (Array.isArray(transfer) && transfer.length) {
            scope.postMessage(message, transfer);
            return;
        }
        scope.postMessage(message);
    }

    // Binary snapshots are created in a worker-owned Uint8Array. Transfer the
    // backing ArrayBuffer to the UI rather than cloning it; the value is not
    // reused after a completed export, so ownership transfer is safe.
    function transferableResult(result) {
        const data = result && typeof result === 'object' ? result.data : null;
        if (typeof ArrayBuffer === 'undefined') return null;
        if (data instanceof ArrayBuffer) return [data];
        if (ArrayBuffer.isView(data)
            && data.buffer instanceof ArrayBuffer) {
            return [data.buffer];
        }
        return null;
    }

    function emitProgress(requestId, method, phase, startedAt, details = {}) {
        emit({
            type: 'progress',
            requestId,
            method,
            phase,
            elapsedMs: startedAt ? Math.max(0, performance.now() - startedAt) : 0,
            estimated: false,
            ...details,
        });
    }

    // Go's async ResultStream export calls this while it yields between
    // batches. Keeping the forwarding endpoint in the worker means the main
    // thread never needs access to a Go global or to syscall/js values.
    scope.__tinySQLReportQueryStream = (requestId, event = {}) => {
        const active = activeCalls.get(requestId);
        if (!active) return;
        emit({
            type: 'stream',
            requestId,
            method: active.method,
            elapsedMs: Math.max(0, performance.now() - active.startedAt),
            event,
        });
    };

    async function instantiateWasm(go, wasmURL, preferCompressed) {
        if (preferCompressed && typeof DecompressionStream !== 'undefined') {
            try {
                const compressed = await fetch(`${wasmURL}.gz`);
                if (!compressed.ok || !compressed.body) {
                    throw new Error(`compressed WASM unavailable (${compressed.status})`);
                }
                const stream = compressed.body.pipeThrough(new DecompressionStream('gzip'));
                return await WebAssembly.instantiateStreaming(
                    new Response(stream, { headers: { 'Content-Type': 'application/wasm' } }),
                    go.importObject
                );
            } catch (error) {
                console.info('Compressed WASM unavailable in worker, using standard loader:', error);
            }
        }
        if (WebAssembly.instantiateStreaming) {
            try {
                return await WebAssembly.instantiateStreaming(fetch(wasmURL), go.importObject);
            } catch (error) {
                console.warn('Worker instantiateStreaming failed, falling back to ArrayBuffer:', error);
            }
        }
        const response = await fetch(wasmURL);
        if (!response.ok) {
            throw new Error(`WASM request failed (${response.status})`);
        }
        return WebAssembly.instantiate(await response.arrayBuffer(), go.importObject);
    }

    async function waitForExports() {
        const deadline = performance.now() + 10000;
        while (typeof scope.executeQuery !== 'function') {
            if (performance.now() >= deadline) {
                throw new Error('Go/WASM started without publishing the tinySQL API');
            }
            await new Promise((resolve) => setTimeout(resolve, 10));
        }
    }

    function emitStatus() {
        if (typeof scope.getRuntimeStatus !== 'function') return;
        try {
            emit({ type: 'status', status: scope.getRuntimeStatus() });
        } catch (error) {
            console.warn('Could not collect worker runtime status:', error);
        }
    }

    async function initialize(config = {}) {
        if (initialized) return;
        if (initialization) return initialization;
        initialization = (async () => {
            const wasmExecURL = config.wasmExecUrl || 'wasm_exec.js';
            const wasmURL = config.wasmUrl || 'query_files.wasm';
            importScripts(wasmExecURL);
            if (typeof scope.Go !== 'function') {
                throw new Error('wasm_exec.js did not expose Go in the worker');
            }
            const go = new scope.Go();
            const result = await instantiateWasm(go, wasmURL, config.preferCompressed !== false);
            // `run` remains pending for the lifetime of the Go program. Start
            // it without awaiting, then wait until main() publishes its JS API.
            Promise.resolve(go.run(result.instance || result)).then(() => {
                if (initialized) emit({ type: 'fatal', error: { message: 'The WASM runtime exited unexpectedly.' } });
            }).catch((error) => emit({ type: 'fatal', error: asError(error) }));
            await waitForExports();
            initialized = true;
            emit({ type: 'ready', methods: exportedMethods.filter((method) => typeof scope[method] === 'function') });
            emitStatus();
        })();
        try {
            return await initialization;
        } finally {
            if (!initialized) initialization = null;
        }
    }

    async function runCall(message) {
        const { requestId, method, args = [] } = message;
        const startedAt = performance.now();
        if (cancelled.delete(requestId)) {
            emitProgress(requestId, method, 'cancelled', startedAt);
            emit({ type: 'result', requestId, cancelled: true });
            return;
        }
        if (!initialized) {
            emit({ type: 'result', requestId, error: { message: 'WASM worker is not ready.', code: 'WASM_NOT_READY' } });
            return;
        }
        const fn = scope[method];
        if (typeof fn !== 'function' || !exportedMethods.includes(method)) {
            emit({ type: 'result', requestId, error: { message: `Unsupported WASM method: ${method}`, code: 'UNKNOWN_WASM_METHOD' } });
            return;
        }

        activeCalls.set(requestId, { method, startedAt });
        emitProgress(requestId, method, 'started', startedAt);
        try {
            // The request id lets the Go stream job bind a cancellation context
            // without exposing worker protocol details to the page API.
            const callArgs = method === 'executeQueryStream'
                ? [requestId, ...args]
                : args;
            const result = await Promise.resolve(fn(...callArgs));
            if (cancelled.delete(requestId)) {
                emitProgress(requestId, method, 'cancelled', startedAt);
                emit({ type: 'result', requestId, cancelled: true });
                return;
            }
            emitProgress(requestId, method, 'completed', startedAt);
            emit({ type: 'result', requestId, result }, transferableResult(result));
            emitStatus();
        } catch (error) {
            emitProgress(requestId, method, 'failed', startedAt, { error: asError(error) });
            emit({ type: 'result', requestId, error: asError(error) });
            emitStatus();
        } finally {
            activeCalls.delete(requestId);
        }
    }

    scope.onmessage = (event) => {
        const message = event.data || {};
        if (message.type === 'init') {
            initialize(message.config).catch((error) => emit({ type: 'init-error', error: asError(error) }));
            return;
        }
        if (message.type === 'cancel') {
            cancelled.add(message.requestId);
            // This is deliberately outside callQueue. A streaming Go export
            // has already returned a Promise to the worker, so it can observe
            // the cancel while waiting for its next bounded result batch.
            if (typeof scope.cancelQueryStream === 'function') {
                try {
                    scope.cancelQueryStream(message.requestId);
                } catch (error) {
                    console.warn('Could not cancel streamed WASM query:', error);
                }
            }
            return;
        }
        if (message.type !== 'call') return;
        callQueue = callQueue.catch(() => undefined).then(() => runCall(message));
    };
}(self));
