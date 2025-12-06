// Minimal WASM Initialization for tinySQL Demo Pages
// This file loads the Go WASM binary and exposes executeQuery_wasm() for demo pages.
// It does NOT include UI code - just the core WASM loading and query execution.

(function(window){
    'use strict';

    // Track initialization state
    window.wasmReady = false;
    window.wasmApi = { importFile: null, executeQuery: null, clearDatabase: null };

    // Initialize WASM
    window.initWasm = async function() {
        if(window.wasmReady) {
            console.log('WASM already initialized');
            return;
        }

        // Ensure Go WASM runtime is loaded
        if(typeof Go === 'undefined'){
            throw new Error('Go WASM runtime (wasm_exec.js) not loaded. Load it before calling initWasm().');
        }

        const go = new Go();
        
        // Try local file first, then GitHub Pages fallback
        async function fetchWasmWithFallback(){
            const local = 'query_files.wasm';
            const fallback = 'https://simonwaldherr.github.io/tinySQL/query_files.wasm';
            
            try{
                const r = await fetch(local);
                if(r && r.ok) {
                    console.log('Loading WASM from local file:', local);
                    return r;
                }
            }catch(e){ /* try fallback */ }
            
            try{
                console.log('Local WASM not found, trying GitHub Pages:', fallback);
                const r2 = await fetch(fallback);
                if(r2 && r2.ok) return r2;
            }catch(e){ /* ignore */ }
            
            throw new Error('query_files.wasm not found locally or at GitHub Pages');
        }

        try {
            console.log('Initializing WASM...');
            const result = await WebAssembly.instantiateStreaming(
                fetchWasmWithFallback(),
                go.importObject
            );
            
            // Start the Go program (this registers global functions)
            go.run(result.instance);

            // Wait for exported functions to be registered by Go
            // (Go WASM exports are async and may take a moment after go.run())
            async function waitForExports(names, timeoutMs){
                const start = Date.now();
                while(Date.now() - start < timeoutMs){
                    const ok = names.every(n => typeof window[n] === 'function');
                    if(ok) return true;
                    await new Promise(r=>setTimeout(r, 50));
                }
                return false;
            }

            const ready = await waitForExports(['executeQuery'], 5000);
            
            // Capture function references
            window.wasmApi.importFile = (typeof window.importFile === 'function') ? window.importFile : null;
            window.wasmApi.executeQuery = (typeof window.executeQuery === 'function') ? window.executeQuery : null;
            window.wasmApi.clearDatabase = (typeof window.clearDatabase === 'function') ? window.clearDatabase : null;

            window.wasmReady = true;
            
            console.log('WASM initialized successfully', {
                executeQueryFound: ready,
                availableFunctions: {
                    importFile: typeof window.wasmApi.importFile,
                    executeQuery: typeof window.wasmApi.executeQuery,
                    clearDatabase: typeof window.wasmApi.clearDatabase
                }
            });

            if(!window.wasmApi.executeQuery){
                console.warn('WARNING: executeQuery not found. Check that query_files.wasm exports the expected functions.');
            }

        } catch (err) {
            console.error('Failed to initialize WASM:', err);
            throw err;
        }
    };

    // Wrapper function that normalizes executeQuery calls
    window.executeQuery_wasm = function(query) {
        if (!window.wasmReady) {
            return { success: false, error: 'WASM not initialized. Call initWasm() first.' };
        }
        
        if (!window.wasmApi.executeQuery) {
            return { success: false, error: 'executeQuery function not available from WASM binary' };
        }

        try {
            const result = window.wasmApi.executeQuery(query);
            
            // Ensure result has the expected shape
            if(result && typeof result === 'object'){
                return result;
            }
            
            return { success: false, error: 'Invalid result from executeQuery' };
        } catch(e) {
            return { success: false, error: String(e.message || e) };
        }
    };

})(window);
