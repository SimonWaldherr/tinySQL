// TinySQL Query Files - WASM Application
let wasmReady = false;
let currentTables = [];
let currentResults = null;
// Safe references to WASM-exported functions (set after init)
let wasmApi = {
    importFile: null,
    executeQuery: null,
    clearDatabase: null,
};

// Client-side pending tables (used when WASM not ready)
const pendingClientTables = {};

// Initialize WASM
async function initWasm() {
    const go = new Go();
    
    try {
        const result = await WebAssembly.instantiateStreaming(
            fetch("query_files.wasm"),
            go.importObject
        );
        
        go.run(result.instance);
        wasmReady = true;
        console.log("WASM initialized successfully");
        
        // Capture WASM API references safely
        wasmApi.importFile = window.importFile;
        wasmApi.executeQuery = window.executeQuery;
        wasmApi.clearDatabase = window.clearDatabase;

        // Check if functions are available
        console.log("Available WASM functions:", {
            importFile: typeof wasmApi.importFile,
            executeQuery: typeof wasmApi.executeQuery,
            clearDatabase: typeof wasmApi.clearDatabase
        });
        
        updateStatus("Ready");
        document.querySelector('.status-indicator').classList.add('ready');
        // If any tables were registered client-side before WASM was ready,
        // import them now into the WASM-backed database so queries will work.
        if (Object.keys(pendingClientTables).length > 0) {
            console.log('Importing pending client tables into WASM:', Object.keys(pendingClientTables));
            for (const [tableName, rows] of Object.entries(pendingClientTables)) {
                try {
                    const jsonContent = JSON.stringify(rows);
                    const result = wasmApi.importFile(`${tableName}.json`, jsonContent, tableName);
                    console.log(`Imported pending table ${tableName} to WASM:`, result);
                    // If successful, ensure table is present in currentTables
                    if (result && result.success) {
                        const tableInfo = {
                            name: tableName,
                            rowCount: result.rowsImported,
                            columns: Array.isArray(result.columns) ? result.columns.map(c => String(c)) : []
                        };
                        const existingIndex = currentTables.findIndex(t => t.name === tableName);
                        if (existingIndex >= 0) currentTables[existingIndex] = tableInfo;
                        else currentTables.push(tableInfo);
                    }
                } catch (err) {
                    console.error(`Failed to import pending table ${tableName}:`, err);
                }
            }
            renderTables();
            // Clear pending list now that we've attempted to import
            for (const k of Object.keys(pendingClientTables)) delete pendingClientTables[k];
        }
    } catch (err) {
        console.error("Failed to load WASM:", err);
        updateStatus("Failed to load WASM");
    }
}

// Demo data
const DEMO_TABLES = {
    sales: {
        name: 'sales',
        data: [
            { order_id: 1001, customer_name: 'Acme Corp', product: 'Widget A', quantity: 50, unit_price: 29.99, order_date: '2024-01-15', region: 'North', status: 'Delivered' },
            { order_id: 1002, customer_name: 'TechStart Inc', product: 'Widget B', quantity: 30, unit_price: 45.50, order_date: '2024-01-18', region: 'South', status: 'Delivered' },
            { order_id: 1003, customer_name: 'Global Solutions', product: 'Widget A', quantity: 100, unit_price: 29.99, order_date: '2024-01-20', region: 'East', status: 'Processing' },
            { order_id: 1004, customer_name: 'Innovate LLC', product: 'Widget C', quantity: 25, unit_price: 75.00, order_date: '2024-01-22', region: 'West', status: 'Shipped' },
            { order_id: 1005, customer_name: 'Acme Corp', product: 'Widget B', quantity: 60, unit_price: 45.50, order_date: '2024-01-25', region: 'North', status: 'Delivered' },
            { order_id: 1006, customer_name: 'DataTech Pro', product: 'Widget A', quantity: 40, unit_price: 29.99, order_date: '2024-02-01', region: 'South', status: 'Delivered' },
            { order_id: 1007, customer_name: 'SmartBiz Co', product: 'Widget C', quantity: 15, unit_price: 75.00, order_date: '2024-02-05', region: 'East', status: 'Processing' },
            { order_id: 1008, customer_name: 'Global Solutions', product: 'Widget B', quantity: 80, unit_price: 45.50, order_date: '2024-02-08', region: 'West', status: 'Shipped' },
            { order_id: 1009, customer_name: 'TechStart Inc', product: 'Widget A', quantity: 55, unit_price: 29.99, order_date: '2024-02-10', region: 'North', status: 'Delivered' },
            { order_id: 1010, customer_name: 'Innovate LLC', product: 'Widget C', quantity: 35, unit_price: 75.00, order_date: '2024-02-15', region: 'South', status: 'Delivered' }
        ]
    },
    logistics: {
        name: 'logistics',
        data: [
            { shipment_id: 'SHP-001', order_id: 1001, origin: 'New York', destination: 'Los Angeles', carrier: 'FastShip Express', weight_kg: 150, distance_km: 4500, shipping_cost: 450.00, dispatch_date: '2024-01-10', delivery_date: '2024-01-15', status: 'Delivered' },
            { shipment_id: 'SHP-002', order_id: 1002, origin: 'Chicago', destination: 'Miami', carrier: 'QuickMove Logistics', weight_kg: 200, distance_km: 2100, shipping_cost: 380.00, dispatch_date: '2024-01-12', delivery_date: '2024-01-16', status: 'Delivered' },
            { shipment_id: 'SHP-003', order_id: 1003, origin: 'Seattle', destination: 'Boston', carrier: 'FastShip Express', weight_kg: 120, distance_km: 4800, shipping_cost: 520.00, dispatch_date: '2024-01-15', delivery_date: '2024-01-20', status: 'In Transit' },
            { shipment_id: 'SHP-004', order_id: 1004, origin: 'Dallas', destination: 'Denver', carrier: 'RapidTransit Co', weight_kg: 180, distance_km: 1200, shipping_cost: 280.00, dispatch_date: '2024-01-18', delivery_date: '2024-01-21', status: 'Delivered' },
            { shipment_id: 'SHP-005', order_id: 1005, origin: 'Phoenix', destination: 'Portland', carrier: 'QuickMove Logistics', weight_kg: 95, distance_km: 2000, shipping_cost: 340.00, dispatch_date: '2024-01-20', delivery_date: '2024-01-24', status: 'In Transit' },
            { shipment_id: 'SHP-006', order_id: 1006, origin: 'Atlanta', destination: 'Houston', carrier: 'FastShip Express', weight_kg: 220, distance_km: 1100, shipping_cost: 310.00, dispatch_date: '2024-01-22', delivery_date: '2024-01-25', status: 'Delivered' },
            { shipment_id: 'SHP-007', order_id: 1007, origin: 'San Francisco', destination: 'Chicago', carrier: 'RapidTransit Co', weight_kg: 160, distance_km: 3400, shipping_cost: 420.00, dispatch_date: '2024-01-25', delivery_date: null, status: 'In Transit' },
            { shipment_id: 'SHP-008', order_id: 1008, origin: 'Boston', destination: 'Dallas', carrier: 'QuickMove Logistics', weight_kg: 140, distance_km: 2700, shipping_cost: 390.00, dispatch_date: '2024-02-01', delivery_date: null, status: 'Processing' },
            { shipment_id: 'SHP-009', order_id: 1009, origin: 'Los Angeles', destination: 'Seattle', carrier: 'FastShip Express', weight_kg: 175, distance_km: 1800, shipping_cost: 350.00, dispatch_date: '2024-02-03', delivery_date: null, status: 'Processing' },
            { shipment_id: 'SHP-010', order_id: 1010, origin: 'Miami', destination: 'Phoenix', carrier: 'RapidTransit Co', weight_kg: 210, distance_km: 3200, shipping_cost: 480.00, dispatch_date: '2024-02-05', delivery_date: null, status: 'In Transit' }
        ]
    }
};

// Load demo table
async function loadDemoTable(tableName) {
    if (!DEMO_TABLES[tableName]) {
        alert(`Demo table "${tableName}" not found`);
        return;
    }

    const demo = DEMO_TABLES[tableName];
    const jsonContent = JSON.stringify(demo.data, null, 2);
    
    updateStatus(`Loading demo table: ${tableName}...`);

        // If WASM is available, import via WASM; otherwise register client-side table
        if (wasmReady && typeof wasmApi.importFile === 'function') {
            updateStatus(`Loading demo table into WASM: ${tableName}...`);
            try {
                const result = wasmApi.importFile(`${tableName}.json`, jsonContent, tableName);
                if (result && result.success) {
                    const tableInfo = {
                        name: tableName,
                        rowCount: result.rowsImported,
                        columns: Array.isArray(result.columns) ? result.columns.map(c => String(c)) : []
                    };
                    const existingIndex = currentTables.findIndex(t => t.name === tableName);
                    if (existingIndex >= 0) {
                        currentTables[existingIndex] = tableInfo;
                    } else {
                        currentTables.push(tableInfo);
                    }
                    renderTables();
                    updateStatus(`Demo table "${tableName}" loaded: ${result.rowsImported} rows`);
                    // Reveal demo queries when demo tables are loaded
                    if (tableName === 'sales' || tableName === 'logistics') {
                        showDemoQueries();
                    }
                    // Set a relevant query
                    const editor = document.getElementById('queryEditor');
                    if (tableName === 'sales') {
                        editor.value = `SELECT customer_name, product, quantity * unit_price AS total_value\nFROM sales\nORDER BY total_value DESC\nLIMIT 10`;
                    } else if (tableName === 'logistics') {
                        editor.value = `SELECT carrier, COUNT(*) AS shipment_count, AVG(shipping_cost) AS avg_cost\nFROM logistics\nGROUP BY carrier\nORDER BY shipment_count DESC`;
                    }
                    document.getElementById('executeBtn').disabled = false;
                } else {
                    alert(`Failed to load demo table: ${result?.error || 'Unknown error'}`);
                    updateStatus('Demo load failed');
                }
            } catch (err) {
                alert(`Error loading demo table: ${err.message}`);
                updateStatus('Demo load failed');
            }
        } else {
            // WASM not ready: register client-side so user can see tables in UI
            registerClientTable(tableName, demo.data);
            // Prefill query editor for demo exploration
            const editor = document.getElementById('queryEditor');
            if (tableName === 'sales') {
                editor.value = `SELECT customer_name, product, quantity * unit_price AS total_value\nFROM sales\nORDER BY total_value DESC\nLIMIT 10`;
            } else if (tableName === 'logistics') {
                editor.value = `SELECT carrier, COUNT(*) AS shipment_count, AVG(shipping_cost) AS avg_cost\nFROM logistics\nGROUP BY carrier\nORDER BY shipment_count DESC`;
            }
            // Inform user that WASM is required for executing queries
            updateStatus(`Registered demo table "${tableName}" locally. WASM not ready yet; queries will run once WASM is initialized.`);
        }
    
}

// Load all demo tables
async function loadAllDemos() {
    await loadDemoTable('sales');
    await new Promise(resolve => setTimeout(resolve, 100));
    await loadDemoTable('logistics');
    
    // Set a complex demo query
    const editor = document.getElementById('queryEditor');
    editor.value = `-- Compare sales by region\nSELECT region, \n       COUNT(*) AS order_count,\n       SUM(quantity * unit_price) AS total_revenue\nFROM sales\nGROUP BY region\nORDER BY total_revenue DESC`;
    // Ensure demo queries are visible after loading all demos
    showDemoQueries();
}

// Load tables on startup
document.addEventListener('DOMContentLoaded', () => {
    initWasm();
    setupDragDrop();
    
    // Setup demo buttons
    const loadAllDemosBtn = document.getElementById('loadAllDemosBtn');
    if (loadAllDemosBtn) {
        loadAllDemosBtn.addEventListener('click', async () => {
            // hide button immediately
            loadAllDemosBtn.style.display = 'none';
            try {
                await loadAllDemos();
            } catch (e) {
                console.error('Error loading demos:', e);
            }
        });
    }
});

// Setup drag and drop
function setupDragDrop() {
    const uploadBtn = document.querySelector('.upload-btn');
    
    uploadBtn.addEventListener('dragover', (e) => {
        e.preventDefault();
        uploadBtn.classList.add('dragover');
    });
    
    uploadBtn.addEventListener('dragleave', () => {
        uploadBtn.classList.remove('dragover');
    });
    
    uploadBtn.addEventListener('drop', (e) => {
        e.preventDefault();
        uploadBtn.classList.remove('dragover');
        handleFiles(e.dataTransfer.files);
    });
}

// Handle file upload
async function handleFileUpload(event) {
    const files = event.target.files;
    if (!files || files.length === 0) return;
    
    await handleFiles(files);
    event.target.value = ''; // Reset file input
}

// Handle multiple files
async function handleFiles(files) {
    if (!wasmReady) {
        alert('Please wait for WASM to initialize...');
        return;
    }
    
    // Wait a bit more to ensure WASM functions are available
    let retries = 0;
    while (typeof wasmApi.importFile !== 'function' && retries < 10) {
        console.log('Waiting for WASM functions to be available...');
        await new Promise(resolve => setTimeout(resolve, 100));
        retries++;
    }
    
    if (typeof wasmApi.importFile !== 'function') {
        alert('WASM functions not available. Please refresh the page.');
        return;
    }
    
    for (const file of files) {
        await importSingleFile(file);
    }
}

// Import a single file
async function importSingleFile(file) {
    const fileName = file.name.toLowerCase();
    
    // Check if it's an Excel file
    if (fileName.endsWith('.xlsx') || fileName.endsWith('.xls')) {
        return await importExcelFile(file);
    }
    
    const reader = new FileReader();
    
    reader.onload = async (e) => {
        const content = e.target.result;
        const tableName = sanitizeTableName(file.name);
        
        updateStatus(`Importing ${file.name}...`);
        
        try {
            // Check if WASM functions are available
            if (typeof wasmApi.importFile !== 'function') {
                throw new Error('WASM importFile function not available. Make sure WASM is initialized.');
            }
            
            console.log('Calling WASM importFile with:', file.name, tableName);
            const result = wasmApi.importFile(file.name, content, tableName);
            console.log('WASM importFile result:', result);
            
            if (!result) {
                throw new Error('WASM importFile returned undefined/null');
            }
            
            if (typeof result !== 'object') {
                throw new Error(`WASM importFile returned invalid type: ${typeof result}`);
            }
            
            if (result.success) {
                // Add table to current tables
                const tableInfo = {
                    name: tableName,
                    rowCount: result.rowsImported,
                    columns: Array.isArray(result.columns) ? result.columns.map(c => String(c)) : []
                };
                
                // Update or add table
                const existingIndex = currentTables.findIndex(t => t.name === tableName);
                if (existingIndex >= 0) {
                    currentTables[existingIndex] = tableInfo;
                } else {
                    currentTables.push(tableInfo);
                }
                
                renderTables();
                
                let message = `Imported ${result.rowsImported} rows into "${tableName}"`;
                if (result.rowsSkipped > 0) {
                    message += ` (${result.rowsSkipped} skipped)`;
                }
                updateStatus(message);

                // Prefill query editor with a working example for this table
                const editor = document.getElementById('queryEditor');
                const defaultQuery = `SELECT * FROM ${tableName} LIMIT 10`;
                if (!editor.value || /SELECT \* FROM (mytable|table1|table2)/i.test(editor.value)) {
                    editor.value = defaultQuery;
                }

                // Ensure Execute is enabled
                const executeBtn = document.getElementById('executeBtn');
                if (executeBtn) executeBtn.disabled = false;
            } else {
                const errorMsg = result.error || 'Unknown import error';
                if (/Unsupported file format: \.xml/i.test(errorMsg)) {
                    alert('XML is not supported yet. Please convert to CSV/JSON for now. We can add XML support next.');
                } else {
                    alert(`Import failed: ${errorMsg}`);
                }
                updateStatus('Import failed');
                console.error('Import failed:', result);
            }
        } catch (err) {
            alert(`Import error: ${err.message}`);
            updateStatus('Import failed');
        }
    };
    
    reader.onerror = () => {
        alert(`Failed to read file: ${file.name}`);
    };
    
    reader.readAsText(file);
}

// Import Excel file using SheetJS
async function importExcelFile(file) {
    if (typeof XLSX === 'undefined') {
        alert('Excel support library not loaded. Please refresh the page.');
        return;
    }

    updateStatus(`Reading Excel file: ${file.name}...`);

    const reader = new FileReader();
    
    reader.onload = async (e) => {
        try {
            const data = new Uint8Array(e.target.result);
            const workbook = XLSX.read(data, { type: 'array' });
            
            // Import each sheet as a separate table
            for (const sheetName of workbook.SheetNames) {
                const worksheet = workbook.Sheets[sheetName];
                const jsonData = XLSX.utils.sheet_to_json(worksheet);
                
                if (jsonData.length === 0) {
                    console.log(`Sheet "${sheetName}" is empty, skipping`);
                    continue;
                }
                
                const tableName = sanitizeTableName(sheetName);
                const jsonContent = JSON.stringify(jsonData);
                
                updateStatus(`Importing sheet: ${sheetName}...`);
                
                const result = wasmApi.importFile(`${sheetName}.json`, jsonContent, tableName);
                
                if (result && result.success) {
                    const tableInfo = {
                        name: tableName,
                        rowCount: result.rowsImported,
                        columns: Array.isArray(result.columns) ? result.columns.map(c => String(c)) : []
                    };
                    
                    const existingIndex = currentTables.findIndex(t => t.name === tableName);
                    if (existingIndex >= 0) {
                        currentTables[existingIndex] = tableInfo;
                    } else {
                        currentTables.push(tableInfo);
                    }
                }
            }
            
            renderTables();
            updateStatus(`Excel file imported: ${workbook.SheetNames.length} sheet(s)`);
            
            // Enable execute button
            document.getElementById('executeBtn').disabled = false;
            
            // Set example query for first table
            if (currentTables.length > 0) {
                const firstTable = currentTables[0].name;
                document.getElementById('queryEditor').value = `SELECT * FROM ${firstTable} LIMIT 10`;
            }
        } catch (err) {
            alert(`Failed to parse Excel file: ${err.message}`);
            updateStatus('Excel import failed');
        }
    };
    
    reader.onerror = () => {
        alert(`Failed to read Excel file: ${file.name}`);
    };
    
    reader.readAsArrayBuffer(file);
}

// Sanitize table name
function sanitizeTableName(filename) {
    return filename
        .replace(/\.[^/.]+$/, '') // Remove extension
        .replace(/[^a-zA-Z0-9_]/g, '_') // Replace special chars
        .toLowerCase();
}

// Register a table client-side so it appears in the UI even when WASM is not ready.
function registerClientTable(tableName, rows) {
    const columns = rows.length ? Object.keys(rows[0]).map(c => String(c)) : [];
    const tableInfo = {
        name: tableName,
        rowCount: rows.length,
        columns
    };

    const existingIndex = currentTables.findIndex(t => t.name === tableName);
    if (existingIndex >= 0) {
        currentTables[existingIndex] = tableInfo;
    } else {
        currentTables.push(tableInfo);
    }

    // Save in pending list so it will be imported into WASM later
    pendingClientTables[tableName] = rows;

    renderTables();
    updateStatus(`Registered local table "${tableName}" (${rows.length} rows). Will import into WASM when ready.`);
    // Reveal demo queries when demo tables are registered
    if (tableName === 'sales' || tableName === 'logistics') {
        showDemoQueries();
    }
}

function showDemoQueries() {
    const dq = document.getElementById('demoQueries');
    if (dq) dq.classList.remove('hidden');
}

// Render tables in sidebar
function renderTables() {
    const tableList = document.getElementById('tableList');
    
    if (currentTables.length === 0) {
        tableList.innerHTML = `
            <div class="empty-state">
                <div class="empty-state-icon">📊</div>
                <div class="empty-state-title">No Tables Loaded</div>
                <div class="empty-state-text">Upload a file to get started</div>
            </div>
        `;
        return;
    }

    tableList.innerHTML = currentTables.map(table => {
        const isPending = Object.prototype.hasOwnProperty.call(pendingClientTables, table.name);
        const badgeHtml = isPending
            ? `<span class="table-badge pending">pending</span>`
            : `<span class="table-badge imported">imported</span>`;

        return `
        <div class="table-item" onclick="selectTable('${table.name}')">
            <div class="table-name">
                ${table.name} ${badgeHtml}
                <span class="table-remove" onclick="event.stopPropagation(); removeTable('${table.name}')" title="Remove table">✕</span>
            </div>
            <div class="table-meta">
                <span>📝 ${table.rowCount} rows</span>
                <span>📁 ${table.columns.length} cols</span>
            </div>
            ${table.columns.length > 0 ? `
                <div class="table-columns">
                    <div class="table-columns-label">Columns:</div>
                    ${table.columns.map(col => `<span class="column-tag">${col}</span>`).join('')}
                </div>
            ` : ''}
        </div>
    `;
    }).join('');
}

// Remove table
function removeTable(tableName) {
    currentTables = currentTables.filter(t => t.name !== tableName);
    renderTables();
    updateStatus(`Removed table "${tableName}"`);
}

// Select a table
function selectTable(tableName) {
    const query = buildSelectWithColumns(tableName, 10);
    setQuery(query);
}

// Get columns for a table from currentTables or pendingClientTables
function getTableColumns(tableName) {
    const t = currentTables.find(x => x.name === tableName);
    if (t && Array.isArray(t.columns) && t.columns.length) return t.columns.map(c => c);

    const pending = pendingClientTables[tableName];
    if (Array.isArray(pending) && pending.length) {
        return Object.keys(pending[0]);
    }

    return null;
}

// Build a SELECT statement that enumerates all columns instead of using *
function buildSelectWithColumns(tableName, limit) {
    const cols = getTableColumns(tableName);
    const colsPart = Array.isArray(cols) && cols.length
        ? cols.map(c => (/[\s\-\(\)\+\/\\]/.test(c) ? `\"${c}\"` : c)).join(', ')
        : '*';

    const lim = (typeof limit === 'number' && limit > 0) ? ` LIMIT ${limit}` : '';
    return `SELECT ${colsPart} FROM ${tableName}${lim}`;
}

// Set query in editor
function setQuery(query) {
    document.getElementById('queryEditor').value = query;
}

// Clear query
function clearQuery() {
    document.getElementById('queryEditor').value = '';
}

// Format query (basic)
function formatQuery() {
    const editor = document.getElementById('queryEditor');
    let query = editor.value.trim();
    
    // Basic SQL formatting
    query = query
        .replace(/\s+/g, ' ')
        .replace(/\b(SELECT|FROM|WHERE|JOIN|ON|ORDER BY|GROUP BY|HAVING|LIMIT)\b/gi, '\n$1')
        .replace(/,/g, ',\n  ')
        .trim();
    
    editor.value = query;
}

// Execute query (UI handler)
async function onExecuteClick() {
    const query = document.getElementById('queryEditor').value.trim();
    
    if (!query) {
        alert('Please enter a query');
        return;
    }

    if (!wasmReady) {
        alert('WASM not ready yet...');
        return;
    }

    const executeBtn = document.getElementById('executeBtn');
    const resultsContainer = document.getElementById('resultsContainer');
    
    executeBtn.disabled = true;
    executeBtn.innerHTML = '<span class="spinner"></span> Executing...';
    setOpenVanillaGridEnabled(false);
    
    updateStatus('Executing query...');

    try {
        if (typeof wasmApi.executeQuery !== 'function') {
            throw new Error('WASM executeQuery function not available');
        }

        console.log('Executing SQL:', query);
        const startTime = performance.now();
        const result = executeQuery_wasm(query);
        console.log('WASM executeQuery result:', result);
        const duration = ((performance.now() - startTime) / 1000).toFixed(3) + 's';

        if (result && typeof result === 'object' && result.success) {
            const cols = Array.isArray(result.columns) ? result.columns.map(c => String(c)) : [];
            const rows = Array.isArray(result.rows) ? result.rows : [];
            currentResults = {
                columns: cols,
                rows: rows,
                rowCount: rows.length,
                duration: duration
            };
            renderResults(currentResults);
            updateStatus(`Query completed: ${currentResults.rowCount} rows in ${duration}`);
        } else {
            const errMsg = result && result.error ? result.error : 'Unknown error';
            resultsContainer.innerHTML = `
                <div class="error-message">
                    <strong>Error:</strong> ${escapeHtml(errMsg)}
                </div>
            `;
            window.clearVanillaGrid?.();
            setOpenVanillaGridEnabled(false);
            updateStatus('Query failed');
        }
    } catch (error) {
        resultsContainer.innerHTML = `
            <div class="error-message">
                <strong>Error:</strong> ${escapeHtml(error.message)}
            </div>
        `;
        window.clearVanillaGrid?.();
        setOpenVanillaGridEnabled(false);
        updateStatus('Query failed');
    } finally {
        executeBtn.disabled = false;
        executeBtn.innerHTML = '▶ Execute';
    }
}

// Render query results
function renderResults(data) {
    const resultsContainer = document.getElementById('resultsContainer');

    if (data.rowCount === 0) {
        window.clearVanillaGrid?.();
        setOpenVanillaGridEnabled(false);
        resultsContainer.innerHTML = `
            <div class="empty-state">
                <div class="empty-state-icon">✓</div>
                <div class="empty-state-title">No Results</div>
                <div class="empty-state-text">
                    Query executed successfully but returned no rows
                    <br>Duration: ${data.duration}
                </div>
            </div>
        `;
        return;
    }

    window.clearVanillaGrid?.();
    const tableHtml = `
        <div class="results-header">
            <div class="results-info">
                <strong>${data.rowCount}</strong> rows • 
                <strong>${data.columns.length}</strong> columns • 
                ${data.duration}
            </div>
            <div class="results-actions">
                <button id="openVanillaGridBtn" onclick="openInVanillaGrid()" disabled>Open in VanillaGrid</button>
                <button onclick="exportCSV()">Export CSV</button>
                <button onclick="exportJSON()">Export JSON</button>
            </div>
        </div>
        <table class="result-table">
            <thead>
                <tr>
                    ${data.columns.map(col => `<th>${escapeHtml(col)}</th>`).join('')}
                </tr>
            </thead>
            <tbody>
                ${data.rows.map(row => `
                    <tr>
                        ${data.columns.map(col => {
                            const value = row[col];
                            return formatCell(value);
                        }).join('')}
                    </tr>
                `).join('')}
            </tbody>
        </table>
    `;

    resultsContainer.innerHTML = tableHtml;
    setOpenVanillaGridEnabled(true);
}

// Format table cell
function formatCell(value) {
    if (value === null || value === undefined) {
        return '<td class="null-value">NULL</td>';
    }
    
    if (typeof value === 'number') {
        return `<td class="number-value">${value}</td>`;
    }
    
    if (typeof value === 'boolean') {
        return `<td class="boolean-value">${value}</td>`;
    }
    
    return `<td>${escapeHtml(String(value))}</td>`;
}

// Show upload dialog
function showUploadDialog() {
    document.getElementById('fileInput').click();
}

// Load tables (for refresh button)
function loadTables() {
    renderTables();
}

// Update status
function updateStatus(text) {
    document.getElementById('statusText').textContent = text;
}

function setOpenVanillaGridEnabled(enabled) {
    const btn = document.getElementById('openVanillaGridBtn');
    if (btn) {
        btn.disabled = !enabled;
    }
}

function openInVanillaGrid() {
    if (!currentResults || !Array.isArray(currentResults.rows) || currentResults.rows.length === 0) {
        alert('No results to visualize yet. Execute a query with rows first.');
        return;
    }
    window.renderVanillaGrid?.(currentResults);
}

// Export to CSV
function exportCSV() {
    if (!currentResults || !currentResults.rows || currentResults.rows.length === 0) {
        alert('No results to export');
        return;
    }
    
    let csv = currentResults.columns.join(',') + '\n';
    
    currentResults.rows.forEach(row => {
        const values = currentResults.columns.map(col => {
            let value = row[col];
            if (value === null || value === undefined) {
                return '';
            }
            value = String(value);
            if (value.includes(',') || value.includes('"') || value.includes('\n')) {
                value = '"' + value.replace(/"/g, '""') + '"';
            }
            return value;
        });
        csv += values.join(',') + '\n';
    });
    
    downloadFile(csv, 'query_results.csv', 'text/csv');
}

// Export to JSON
function exportJSON() {
    if (!currentResults || !currentResults.rows || currentResults.rows.length === 0) {
        alert('No results to export');
        return;
    }
    
    const json = JSON.stringify(currentResults.rows, null, 2);
    downloadFile(json, 'query_results.json', 'application/json');
}

// Download file helper
function downloadFile(content, filename, mimeType) {
    const blob = new Blob([content], { type: mimeType });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    a.click();
    URL.revokeObjectURL(url);
}

// Escape HTML
function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

// Wrapper function for WASM executeQuery
function executeQuery_wasm(query) {
    return wasmApi.executeQuery(query);
}

// Keyboard shortcuts
document.addEventListener('DOMContentLoaded', () => {
    const editor = document.getElementById('queryEditor');
    if (editor) {
        editor.addEventListener('keydown', (e) => {
            // Ctrl/Cmd + Enter to execute
            if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
                e.preventDefault();
                onExecuteClick();
            }
            
            // Tab for indentation
            if (e.key === 'Tab') {
                e.preventDefault();
                const start = e.target.selectionStart;
                const end = e.target.selectionEnd;
                e.target.value = e.target.value.substring(0, start) + '  ' + e.target.value.substring(end);
                e.target.selectionStart = e.target.selectionEnd = start + 2;
            }
        });
    }
});
