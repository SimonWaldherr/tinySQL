// TinySQL Query Files - WASM Application
let wasmReady = false;
let currentTables = [];
let currentResults = null;
const HISTORY_KEY = 'tinysql_query_history_v1';
// Safe references to WASM-exported functions (set after init)
let wasmApi = {
    importFile: null,
    executeQuery: null,
    executeMulti: null,
    clearDatabase: null,
    dropTable: null,
    listTables: null,
    exportResults: null,
    getTableSchema: null,
};

// Client-side pending tables (used when WASM not ready)
const pendingClientTables = {};

// Query history (newest first, max 50)
const MAX_HISTORY = 50;
let queryHistory = loadHistory();

function loadHistory() {
    const legacyKeys = ['tinySQL_history', 'tsql_history'];
    try {
        const current = localStorage.getItem(HISTORY_KEY);
        if (current) {
            const parsed = JSON.parse(current);
            return Array.isArray(parsed) ? parsed : [];
        }
        for (const key of legacyKeys) {
            const legacy = localStorage.getItem(key);
            if (!legacy) continue;
            const parsed = JSON.parse(legacy);
            if (Array.isArray(parsed)) {
                localStorage.setItem(HISTORY_KEY, JSON.stringify(parsed));
                localStorage.removeItem(key);
                return parsed;
            }
        }
    } catch (_) {
        // Keep empty history if storage is corrupted or blocked.
    }
    return [];
}

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
        wasmApi.executeMulti = window.executeMulti;
        wasmApi.clearDatabase = window.clearDatabase;
        wasmApi.dropTable = window.dropTable;
        wasmApi.listTables = window.listTables;
        wasmApi.exportResults = window.exportResults;
        wasmApi.getTableSchema = window.getTableSchema;

        console.log("Available WASM functions:", Object.fromEntries(
            Object.entries(wasmApi).map(([k,v]) => [k, typeof v])
        ));
        
        updateStatus("Ready");
        document.querySelector('.status-indicator').classList.add('ready');
        document.getElementById('executeBtn').disabled = false;
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
        loadTables();
    } catch (err) {
        console.error("Failed to load WASM:", err);
        updateStatus("Failed to load WASM");
        document.querySelector('.status-indicator').classList.add('failed');
    }
}

// Demo data
function createSeededRandom(seed) {
    let state = seed >>> 0;
    return function nextRandom() {
        state = (state * 1664525 + 1013904223) >>> 0;
        return state / 4294967296;
    };
}

function pickValue(values, random) {
    return values[Math.floor(random() * values.length)];
}

function randomInt(random, min, max) {
    return Math.floor(random() * (max - min + 1)) + min;
}

function randomNumber(random, min, max, decimals = 2) {
    return Number((min + random() * (max - min)).toFixed(decimals));
}

function formatIsoDate(date) {
    return date.toISOString().slice(0, 10);
}

function formatIsoDateTime(date) {
    return date.toISOString().slice(0, 19).replace('T', ' ');
}

function generateLargeSalesData(rowCount) {
    const random = createSeededRandom(1337);
    const catalog = [
        { product: 'Widget A', category: 'Widgets', minPrice: 24, maxPrice: 36 },
        { product: 'Widget B', category: 'Widgets', minPrice: 39, maxPrice: 58 },
        { product: 'Widget C', category: 'Widgets', minPrice: 68, maxPrice: 92 },
        { product: 'Sensor Hub', category: 'Electronics', minPrice: 120, maxPrice: 185 },
        { product: 'Analytics Suite', category: 'Software', minPrice: 210, maxPrice: 360 },
        { product: 'Edge Gateway', category: 'Infrastructure', minPrice: 440, maxPrice: 680 }
    ];
    const customerPrefixes = ['Acme', 'Northwind', 'BluePeak', 'Signal', 'Vertex', 'Bright', 'Nimbus', 'Evergreen', 'Cobalt', 'Atlas'];
    const customerSuffixes = ['Retail', 'Logistics', 'Systems', 'Works', 'Labs', 'Partners', 'Industries', 'Solutions', 'Stores', 'Networks'];
    const segments = ['SMB', 'Mid-Market', 'Enterprise', 'Public Sector'];
    const regions = ['North', 'South', 'East', 'West', 'Central'];
    const channels = ['Direct', 'Partner', 'Online', 'Inside Sales'];
    const statuses = ['Delivered', 'Delivered', 'Delivered', 'Shipped', 'Processing', 'Backorder'];
    const priorities = ['Low', 'Normal', 'High', 'Urgent'];
    const salesReps = ['A. Cole', 'B. Rivera', 'C. Shah', 'D. Fischer', 'E. Novak', 'F. Silva'];
    const baseDate = Date.UTC(2024, 0, 1);
    const rows = [];

    for (let index = 0; index < rowCount; index += 1) {
        const item = pickValue(catalog, random);
        const quantity = randomInt(random, 1, 120);
        const unitPrice = randomNumber(random, item.minPrice, item.maxPrice);
        const discountPct = randomInt(random, 0, 18);
        const grossTotal = Number((quantity * unitPrice).toFixed(2));
        const orderTotal = Number((grossTotal * (1 - discountPct / 100)).toFixed(2));
        const customerNumber = 1000 + randomInt(random, 0, 899);
        const orderDate = new Date(baseDate + randomInt(random, 0, 210) * 86400000);

        rows.push({
            order_id: 200000 + index,
            customer_id: `CUST-${customerNumber}`,
            customer_name: `${pickValue(customerPrefixes, random)} ${pickValue(customerSuffixes, random)}`,
            segment: pickValue(segments, random),
            region: pickValue(regions, random),
            channel: pickValue(channels, random),
            product: item.product,
            category: item.category,
            quantity,
            unit_price: unitPrice,
            discount_pct: discountPct,
            gross_total: grossTotal,
            order_total: orderTotal,
            status: pickValue(statuses, random),
            priority: pickValue(priorities, random),
            sales_rep: pickValue(salesReps, random),
            order_date: formatIsoDate(orderDate)
        });
    }

    return rows;
}

function generateLargeLogisticsData(salesRows) {
    const random = createSeededRandom(2024);
    const carriers = ['FastShip Express', 'QuickMove Logistics', 'RapidTransit Co', 'Northern Freight', 'CargoStream'];
    const warehouses = ['New York', 'Chicago', 'Dallas', 'Seattle', 'Rotterdam'];
    const serviceLevels = ['Standard', 'Priority', 'Two-Day', 'Economy'];
    const statuses = ['Delivered', 'Delivered', 'Delivered', 'In Transit', 'Processing', 'Delayed'];

    return salesRows.map((sale, index) => {
        const dispatchDelay = randomInt(random, 0, 3);
        const deliveryDays = randomInt(random, 1, 8);
        const dispatchDate = new Date(Date.parse(`${sale.order_date}T08:00:00Z`) + dispatchDelay * 86400000);
        const status = pickValue(statuses, random);
        const deliveryDate = status === 'Delivered'
            ? formatIsoDate(new Date(dispatchDate.getTime() + deliveryDays * 86400000))
            : null;

        return {
            shipment_id: `SHP-${sale.order_id}`,
            order_id: sale.order_id,
            customer_id: sale.customer_id,
            warehouse: pickValue(warehouses, random),
            carrier: pickValue(carriers, random),
            service_level: pickValue(serviceLevels, random),
            origin_region: pickValue(['North', 'South', 'East', 'West', 'Central'], random),
            destination_region: sale.region,
            weight_kg: randomNumber(random, 5, 380, 1),
            distance_km: randomInt(random, 120, 5200),
            shipping_cost: randomNumber(random, 35, 920),
            delivery_days: status === 'Delivered' ? deliveryDays : null,
            status,
            dispatch_date: formatIsoDate(dispatchDate),
            delivery_date: deliveryDate,
            batch_id: `BATCH-${100 + (index % 48)}`
        };
    });
}

function generateLargeWebEventsData(salesRows, rowCount) {
    const random = createSeededRandom(4242);
    const eventTypes = ['page_view', 'product_view', 'search', 'add_to_cart', 'checkout_start', 'purchase', 'support_chat'];
    const pages = ['/home', '/pricing', '/catalog', '/products/widget-a', '/products/widget-b', '/checkout', '/support'];
    const devices = ['desktop', 'mobile', 'tablet'];
    const countries = ['US', 'DE', 'FR', 'UK', 'NL', 'SE'];
    const acquisitionChannels = ['organic', 'paid', 'email', 'partner', 'direct'];
    const baseDate = Date.UTC(2024, 0, 1);
    const rows = [];

    for (let index = 0; index < rowCount; index += 1) {
        const sale = salesRows[randomInt(random, 0, salesRows.length - 1)];
        const eventType = pickValue(eventTypes, random);
        const eventDate = new Date(
            baseDate + randomInt(random, 0, 240) * 86400000 + randomInt(random, 0, 1439) * 60000
        );
        const revenueImpact = eventType === 'purchase'
            ? sale.order_total
            : Number((sale.order_total * random() * 0.08).toFixed(2));

        rows.push({
            event_id: `EVT-${500000 + index}`,
            session_id: `SES-${200000 + randomInt(random, 0, 90000)}`,
            customer_id: sale.customer_id,
            order_id: eventType === 'purchase' ? sale.order_id : null,
            event_type: eventType,
            page: pickValue(pages, random),
            device: pickValue(devices, random),
            region: sale.region,
            country: pickValue(countries, random),
            acquisition_channel: pickValue(acquisitionChannels, random),
            event_date: formatIsoDate(eventDate),
            event_timestamp: formatIsoDateTime(eventDate),
            duration_seconds: randomInt(random, 5, 1800),
            revenue_impact: revenueImpact,
            converted: eventType === 'purchase' ? 1 : (random() < 0.06 ? 1 : 0)
        });
    }

    return rows;
}

let generatedDemoTables = null;

function getGeneratedDemoTables() {
    if (generatedDemoTables) {
        return generatedDemoTables;
    }

    const salesLarge = generateLargeSalesData(5000);
    generatedDemoTables = {
        sales_large: salesLarge,
        logistics_large: generateLargeLogisticsData(salesLarge),
        web_events_large: generateLargeWebEventsData(salesLarge, 10000)
    };

    return generatedDemoTables;
}

function getDemoDefaultQuery(tableName) {
    const queries = {
        sales: `SELECT customer_name, product, quantity * unit_price AS total_value\nFROM sales\nORDER BY total_value DESC\nLIMIT 10`,
        logistics: `SELECT carrier, COUNT(*) AS shipment_count, AVG(shipping_cost) AS avg_cost\nFROM logistics\nGROUP BY carrier\nORDER BY shipment_count DESC`,
        sales_large: `SELECT region, channel, COUNT(*) AS orders, SUM(order_total) AS revenue\nFROM sales_large\nGROUP BY region, channel\nORDER BY revenue DESC`,
        logistics_large: `SELECT carrier, service_level, COUNT(*) AS shipments, AVG(delivery_days) AS avg_delivery_days\nFROM logistics_large\nGROUP BY carrier, service_level\nORDER BY shipments DESC`,
        web_events_large: `SELECT event_date, device, COUNT(*) AS events, SUM(revenue_impact) AS influenced_revenue\nFROM web_events_large\nGROUP BY event_date, device\nORDER BY event_date DESC`
    };

    return queries[tableName] || '';
}

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
    },
    sales_large: {
        name: 'sales_large',
        getData: () => getGeneratedDemoTables().sales_large
    },
    logistics_large: {
        name: 'logistics_large',
        getData: () => getGeneratedDemoTables().logistics_large
    },
    web_events_large: {
        name: 'web_events_large',
        getData: () => getGeneratedDemoTables().web_events_large
    }
};

// Load demo table
async function loadDemoTable(tableName) {
    if (!DEMO_TABLES[tableName]) {
        alert(`Demo table "${tableName}" not found`);
        return;
    }

    const demo = DEMO_TABLES[tableName];
    const rows = typeof demo.getData === 'function' ? demo.getData() : demo.data;
    const jsonContent = JSON.stringify(rows);
    const suggestedQuery = getDemoDefaultQuery(tableName);
    
    updateStatus(`Loading demo table: ${tableName}...`);

        if (wasmReady && typeof wasmApi.importFile === 'function') {
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
                    if (Object.prototype.hasOwnProperty.call(DEMO_TABLES, tableName)) {
                        showDemoQueries();
                    }
                    const editor = document.getElementById('queryEditor');
                    if (suggestedQuery) {
                        editor.value = suggestedQuery;
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
            registerClientTable(tableName, rows);
            const editor = document.getElementById('queryEditor');
            if (suggestedQuery) {
                editor.value = suggestedQuery;
            }
            updateStatus(`Registered demo table "${tableName}" locally. Queries will run once WASM is initialized.`);
        }
    
}

// Load all demo tables
async function loadAllDemos() {
    const tableNames = ['sales', 'logistics', 'sales_large', 'logistics_large', 'web_events_large'];

    for (const [index, tableName] of tableNames.entries()) {
        await loadDemoTable(tableName);
        if (index < tableNames.length - 1) {
            await new Promise(resolve => setTimeout(resolve, 25));
        }
    }
    
    // Set a complex demo query
    const editor = document.getElementById('queryEditor');
    editor.value = `-- Large demo: revenue and fulfillment by region and carrier\nSELECT s.region,\n       l.carrier,\n       COUNT(*) AS orders,\n       SUM(s.order_total) AS total_revenue,\n       AVG(l.shipping_cost) AS avg_shipping_cost\nFROM sales_large s\nJOIN logistics_large l ON s.order_id = l.order_id\nGROUP BY s.region, l.carrier\nORDER BY total_revenue DESC`;
    // Ensure demo queries are visible after loading all demos
    showDemoQueries();
    updateStatus('Loaded curated demos and generated large tables');
}

// Load tables on startup
document.addEventListener('DOMContentLoaded', () => {
    initWasm();
    setupDragDrop();
    renderHistory();
    
    // Setup demo buttons
    const loadAllDemosBtn = document.getElementById('loadAllDemosBtn');
    if (loadAllDemosBtn) {
        loadAllDemosBtn.addEventListener('click', async () => {
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
                alert(`Import failed: ${result.error || 'Unknown error'}`);
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
    if (Object.prototype.hasOwnProperty.call(DEMO_TABLES, tableName)) {
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
    const virtuals = window._virtualTables || [];
    const hasAny = currentTables.length > 0 || virtuals.length > 0;

    if (!hasAny) {
        tableList.innerHTML = `
            <div class="empty-state">
                <div class="empty-state-icon">📊</div>
                <div class="empty-state-title">No Tables Loaded</div>
                <div class="empty-state-text">Upload a file to get started</div>
            </div>
        `;
        return;
    }

    let html = '';

    // ── User tables ──────────────────────────────────────────────────────
    if (currentTables.length > 0) {
        html += `<div class="table-section-label">User Tables (${currentTables.length})</div>`;
        html += currentTables.map(table => {
            const isPending = Object.prototype.hasOwnProperty.call(pendingClientTables, table.name);
            const badgeHtml = isPending
                ? `<span class="table-badge pending">pending</span>`
                : `<span class="table-badge imported">imported</span>`;

            return `
            <div class="table-item" onclick="selectTable('${escapeHtml(table.name)}')">
                <div class="table-name">
                    ${escapeHtml(table.name)} ${badgeHtml}
                    <span class="table-remove" onclick="event.stopPropagation(); removeTable('${escapeHtml(table.name)}')" title="Remove table">✕</span>
                    <span class="table-info-btn" onclick="event.stopPropagation(); showTableInfo('${escapeHtml(table.name)}')" title="Show schema">ℹ</span>
                </div>
                <div class="table-meta">
                    <span>📝 ${table.rowCount} rows</span>
                    <span>📁 ${table.columns.length} cols</span>
                </div>
                ${table.columns.length > 0 ? `
                    <div class="table-columns">
                        <div class="table-columns-label">Columns:</div>
                        ${table.columns.map(col => `<span class="column-tag">${escapeHtml(col)}</span>`).join('')}
                    </div>
                ` : ''}
            </div>
        `;
        }).join('');
    }

    // ── Virtual tables ───────────────────────────────────────────────────
    if (virtuals.length > 0) {
        const collapsed = window._virtualCollapsed !== false;
        html += `
            <div class="table-section-label virtual-toggle" onclick="toggleVirtualTables()">
                <span>${collapsed ? '▶' : '▼'} Virtual Tables (${virtuals.length})</span>
            </div>`;
        if (!collapsed) {
            html += virtuals.map(vt => `
                <div class="table-item virtual-table-item" onclick="selectTable('${escapeHtml(vt.name)}')">
                    <div class="table-name">
                        ${escapeHtml(vt.name)}
                        <span class="table-badge virtual">virtual</span>
                        <span class="table-info-btn" onclick="event.stopPropagation(); showTableInfo('${escapeHtml(vt.name)}')" title="Show schema">ℹ</span>
                    </div>
                    <div class="table-meta"><span>computed at query time</span></div>
                </div>
            `).join('');
        }
    }

    tableList.innerHTML = html;
}

// Toggle virtual table section collapsed state
function toggleVirtualTables() {
    window._virtualCollapsed = !(window._virtualCollapsed !== false);
    renderTables();
}

// Show schema / info panel for a table (real or virtual)
function showTableInfo(tableName) {
    if (typeof wasmApi.getTableSchema !== 'function') {
        alert('Schema inspection requires WASM to be ready');
        return;
    }
    const info = wasmApi.getTableSchema(tableName);
    if (!info || !info.success) {
        alert(info?.error || 'Could not load schema');
        return;
    }

    const cols = Array.isArray(info.columns) ? info.columns : [];
    const isVirt = info.virtual === true;
    const rowInfo = isVirt ? 'dynamic' : String(info.rows);

    const panel = document.getElementById('schemaPanel');
    if (panel) {
        panel.innerHTML = `
            <div class="schema-panel-header">
                <strong>${escapeHtml(tableName)}</strong>
                ${isVirt ? '<span class="table-badge virtual">virtual</span>' : ''}
                <button onclick="document.getElementById('schemaPanel').classList.add('hidden')" class="schema-close">✕</button>
            </div>
            <div class="schema-meta">${rowInfo} rows · ${cols.length} columns</div>
            <table class="schema-table">
                <thead><tr><th>Column</th><th>Type</th></tr></thead>
                <tbody>
                    ${cols.map(c => `<tr><td>${escapeHtml(c.name)}</td><td class="schema-type">${escapeHtml(c.type)}</td></tr>`).join('')}
                </tbody>
            </table>
            <div class="schema-actions">
                <button onclick="setQuery('SELECT * FROM ${escapeHtml(tableName)} LIMIT 100'); document.getElementById('schemaPanel').classList.add('hidden');">SELECT *</button>
            </div>
        `;
        panel.classList.remove('hidden');
    }
}

// Remove table
function removeTable(tableName) {
    const isPending = Object.prototype.hasOwnProperty.call(pendingClientTables, tableName);

    if (!isPending && wasmReady && typeof wasmApi.dropTable === 'function') {
        const result = wasmApi.dropTable(tableName);
        if (!result || !result.success) {
            alert(`Failed to drop table "${tableName}": ${result?.error || 'Unknown error'}`);
            return;
        }
    }

    delete pendingClientTables[tableName];
    currentTables = currentTables.filter(t => t.name !== tableName);
    if (currentResults && currentResults.sourceTable === tableName) {
        currentResults = null;
    }
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

function clearAllTables() {
    if (!confirm('This will remove all imported tables. Continue?')) {
        return;
    }
    if (typeof wasmApi.clearDatabase === 'function') {
        const result = wasmApi.clearDatabase();
        if (!result || !result.success) {
            alert(`Failed to clear database: ${result?.error || 'Unknown error'}`);
            return;
        }
    }
    for (const key of Object.keys(pendingClientTables)) {
        delete pendingClientTables[key];
    }
    currentTables = [];
    currentResults = null;
    renderTables();
    const resultsContainer = document.getElementById('resultsContainer');
    if (resultsContainer) {
        resultsContainer.innerHTML = `
            <div class="empty-state">
                <div class="empty-state-icon">⚡</div>
                <div class="empty-state-title">Ready to Query</div>
                <div class="empty-state-text">
                    Upload a file and run a SQL query
                </div>
            </div>
        `;
    }
    window.clearVanillaGrid?.();
    updateStatus('Database cleared');
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
    executeBtn.innerHTML = '<span class="spinner"></span> Running…';
    setOpenVanillaGridEnabled(false);
    
    updateStatus('Executing query…');

    try {
        if (typeof wasmApi.executeQuery !== 'function') {
            throw new Error('WASM executeQuery function not available');
        }

        const startTime = performance.now();
        // Use executeMulti if available and query contains semicolons
        const hasMulti = query.includes(';') && typeof wasmApi.executeMulti === 'function';
        const result = hasMulti ? wasmApi.executeMulti(query) : wasmApi.executeQuery(query);
        const wallMs = performance.now() - startTime;
        const duration = result?.durationMs != null
            ? result.durationMs.toFixed(2) + ' ms'
            : wallMs.toFixed(1) + ' ms';

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
            updateStatus(`Query completed: ${currentResults.rowCount} rows in ${duration}${result.statementsRun > 1 ? ` (${result.statementsRun} statements)` : ''}`);
            pushHistory(query, duration, rows.length);
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
            pushHistory(query, '0 ms', 'err');
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
                <button onclick="doExport('csv')">Export CSV</button>
                <button onclick="doExport('json')">Export JSON</button>
                <button onclick="doExport('xml')">Export XML</button>
            </div>
        </div>
        <div class="result-table-wrap">
        <table class="result-table">
            <thead>
                <tr>
                    <th class="row-num-col">#</th>
                    ${data.columns.map(col => `<th>${escapeHtml(col)}</th>`).join('')}
                </tr>
            </thead>
            <tbody>
                ${data.rows.map((row, idx) => `
                    <tr>
                        <td class="row-num-col">${idx + 1}</td>
                        ${data.columns.map(col => {
                            const value = row[col];
                            return formatCell(value);
                        }).join('')}
                    </tr>
                `).join('')}
            </tbody>
        </table>
        </div>
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
    if (typeof wasmApi.listTables === 'function') {
        try {
            const info = wasmApi.listTables();
            if (info && Array.isArray(info.tables)) {
                // Separate user tables from virtual tables
                const userTbls = info.tables.filter(t => t.kind !== 'virtual');
                const virtTbls = info.tables.filter(t => t.kind === 'virtual');

                // Replace current user-table snapshot from backend state.
                currentTables = userTbls.map(t => ({
                    name: t.name,
                    rowCount: t.rows,
                    columns: Array.isArray(t.columns)
                        ? t.columns.map(c => typeof c === 'object' ? c.name : c)
                        : [],
                    columnTypes: Array.isArray(t.columns)
                        ? t.columns.filter(c => typeof c === 'object')
                        : [],
                    kind: 'table',
                }));

                // Keep pending local tables visible until they are imported.
                for (const [name, rows] of Object.entries(pendingClientTables)) {
                    if (!currentTables.some(t => t.name === name)) {
                        const cols = Array.isArray(rows) && rows.length > 0 ? Object.keys(rows[0]) : [];
                        currentTables.push({
                            name,
                            rowCount: Array.isArray(rows) ? rows.length : 0,
                            columns: cols,
                            kind: 'table',
                        });
                    }
                }

                // Store virtual tables separately
                window._virtualTables = virtTbls.map(t => ({
                    name: t.name,
                    kind: 'virtual',
                    rowCount: -1,
                    columns: [],
                }));

                renderTables();
                updateStatus(`${userTbls.length} table(s), ${virtTbls.length} virtual`);
                return;
            }
        } catch (e) { console.warn('listTables fallback:', e); }
    }
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

// Unified export dispatcher – tries WASM-side first, falls back to client-side
function doExport(format) {
    if (!currentResults || !currentResults.rows || currentResults.rows.length === 0) {
        alert('No results to export');
        return;
    }
    // Try WASM-side exporter
    if (typeof wasmApi.exportResults === 'function') {
        try {
            const res = wasmApi.exportResults(format);
            if (res && res.success && res.data) {
                const mimeType = (typeof res.mimeType === 'string' && res.mimeType) ||
                    (typeof res.mime === 'string' && res.mime) ||
                    'application/octet-stream';
                const ext = (typeof res.ext === 'string' && res.ext) ? res.ext : format;
                downloadFile(res.data, `query_results.${ext}`, mimeType);
                return;
            }
        } catch (_) { /* fall through */ }
    }
    // Client-side fallback
    if (format === 'csv') exportCSV();
    else if (format === 'json') exportJSON();
    else if (format === 'xml') exportXML();
    else alert('Unsupported export format: ' + format);
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

// Export to XML (client-side fallback)
function exportXML() {
    if (!currentResults || !currentResults.rows || currentResults.rows.length === 0) {
        alert('No results to export');
        return;
    }
    let xml = '<?xml version="1.0" encoding="UTF-8"?>\n<results>\n';
    currentResults.rows.forEach(row => {
        xml += '  <row>\n';
        currentResults.columns.forEach(col => {
            const val = row[col];
            const tag = toXmlTag(col);
            xml += `    <${tag}>${escapeXml(val == null ? '' : String(val))}</${tag}>\n`;
        });
        xml += '  </row>\n';
    });
    xml += '</results>\n';
    downloadFile(xml, 'query_results.xml', 'application/xml');
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

// Escape XML special chars
function escapeXml(s) {
    return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/'/g,'&apos;');
}

function toXmlTag(name) {
    const raw = String(name || '').trim();
    if (!raw) return 'col';
    let tag = raw.replace(/[^A-Za-z0-9_.-]/g, '_');
    if (!/^[A-Za-z_]/.test(tag)) {
        tag = `c_${tag}`;
    }
    return tag || 'col';
}

// ----- Query History -----
function pushHistory(sql, duration, rows) {
    queryHistory.unshift({ sql, duration, rows, ts: Date.now() });
    if (queryHistory.length > MAX_HISTORY) queryHistory.length = MAX_HISTORY;
    try { localStorage.setItem(HISTORY_KEY, JSON.stringify(queryHistory)); } catch (_) {}
    renderHistory();
}

function clearHistory() {
    queryHistory.length = 0;
    try {
        localStorage.removeItem(HISTORY_KEY);
        localStorage.removeItem('tinySQL_history');
        localStorage.removeItem('tsql_history');
    } catch (_) {}
    renderHistory();
}

function renderHistory() {
    const panel = document.getElementById('historyList');
    if (!panel) return;
    if (queryHistory.length === 0) {
        panel.innerHTML = '<div class="empty-state-text">No queries yet</div>';
        return;
    }
    panel.innerHTML = queryHistory.map((h, i) =>
        `<div class="history-item" onclick="recallHistory(${i})" title="${escapeHtml(h.sql)}">
            <div class="history-sql">${escapeHtml(h.sql.length > 80 ? h.sql.slice(0,77)+'…' : h.sql)}</div>
            <div class="history-meta">${h.rows} rows · ${h.duration} · ${timeAgo(h.ts)}</div>
        </div>`
    ).join('');
}

function recallHistory(idx) {
    const h = queryHistory[idx];
    if (h) {
        document.getElementById('queryEditor').value = h.sql;
    }
}

function timeAgo(ts) {
    const sec = Math.floor((Date.now() - ts) / 1000);
    if (sec < 60) return 'just now';
    if (sec < 3600) return Math.floor(sec/60) + 'm ago';
    if (sec < 86400) return Math.floor(sec/3600) + 'h ago';
    return Math.floor(sec/86400) + 'd ago';
}

function toggleHistory() {
    const panel = document.getElementById('historyPanel');
    if (panel) panel.classList.toggle('hidden');
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

            // ArrowUp in empty editor recalls last query
            if (e.key === 'ArrowUp' && editor.value.trim() === '' && queryHistory.length > 0) {
                e.preventDefault();
                editor.value = queryHistory[0].sql;
            }

            // Ctrl/Cmd + Shift + F to format SQL
            if ((e.ctrlKey || e.metaKey) && e.shiftKey && e.key === 'F') {
                e.preventDefault();
                if (typeof formatSQL === 'function') {
                    editor.value = formatSQL(editor.value);
                }
            }
        });
    }
});
