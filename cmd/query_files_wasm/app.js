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
    } catch (err) {
        console.error("Failed to load WASM:", err);
        updateStatus("Failed to load WASM");
    }
}

// Load tables on startup
document.addEventListener('DOMContentLoaded', () => {
    initWasm();
    setupDragDrop();
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

// Sanitize table name
function sanitizeTableName(filename) {
    return filename
        .replace(/\.[^/.]+$/, '') // Remove extension
        .replace(/[^a-zA-Z0-9_]/g, '_') // Replace special chars
        .toLowerCase();
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

    tableList.innerHTML = currentTables.map(table => `
        <div class="table-item" onclick="selectTable('${table.name}')">
            <div class="table-name">
                ${table.name}
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
    `).join('');
}

// Remove table
function removeTable(tableName) {
    currentTables = currentTables.filter(t => t.name !== tableName);
    renderTables();
    updateStatus(`Removed table "${tableName}"`);
}

// Select a table
function selectTable(tableName) {
    setQuery(`SELECT * FROM ${tableName} LIMIT 10`);
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
            updateStatus('Query failed');
        }
    } catch (error) {
        resultsContainer.innerHTML = `
            <div class="error-message">
                <strong>Error:</strong> ${escapeHtml(error.message)}
            </div>
        `;
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

    const tableHtml = `
        <div class="results-header">
            <div class="results-info">
                <strong>${data.rowCount}</strong> rows • 
                <strong>${data.columns.length}</strong> columns • 
                ${data.duration}
            </div>
            <div class="results-actions">
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
