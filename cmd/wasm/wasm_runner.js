#!/usr/bin/env node
/**
 * Node.js WASM Runner for tinySQL
 * This allows running the WASM module outside the browser for testing
 */

const fs = require('fs');
const path = require('path');

// Load wasm_exec.js from Go runtime
require('./wasm_exec.js');

class WASMRunner {
    constructor() {
        this.db = null;
        this.ready = false;
    }

    async loadWASM() {
        console.log('🔄 Loading tinySQL WASM module...');
        
        try {
            // Read the WASM file
            const wasmPath = path.join(__dirname, 'tinySQL.wasm');
            const wasmBuffer = fs.readFileSync(wasmPath);
            
            // Create Go instance
            const go = new Go();
            
            // Instantiate WASM module
            const { instance } = await WebAssembly.instantiate(wasmBuffer, go.importObject);
            
            // Run the Go program
            go.run(instance);
            
            // Wait for tinySQL to be available
            let attempts = 0;
            while (!global.tinySQL && attempts < 50) {
                await new Promise(resolve => setTimeout(resolve, 100));
                attempts++;
            }
            
            if (global.tinySQL) {
                this.db = global.tinySQL;
                this.ready = true;
                console.log('✅ tinySQL WASM module loaded successfully');
                return true;
            } else {
                throw new Error('tinySQL API not available after 5 seconds');
            }
        } catch (error) {
            console.error('❌ Failed to load WASM module:', error.message);
            return false;
        }
    }

    parseResponse(response) {
        if (typeof response === 'string') {
            try {
                return JSON.parse(response);
            } catch {
                return { error: response };
            }
        }
        return response;
    }

    async connect(dsn = 'mem://?tenant=default') {
        if (!this.ready) {
            console.log('❌ WASM module not ready');
            return false;
        }

        console.log(`🔌 Connecting to database: ${dsn}`);
        const result = this.parseResponse(this.db.open(dsn));
        
        if (result.success) {
            console.log('✅ Connected to database');
            return true;
        } else {
            console.log('❌ Connection failed:', result.error);
            return false;
        }
    }

    async status() {
        if (!this.ready) return null;
        return this.parseResponse(this.db.status());
    }

    async exec(sql) {
        if (!this.ready) return null;
        console.log(`⚡ Executing: ${sql}`);
        const result = this.parseResponse(this.db.exec(sql));
        
        if (result.success) {
            console.log('✅', result.message);
        } else {
            console.log('❌', result.error);
        }
        
        return result;
    }

    async query(sql) {
        if (!this.ready) return null;
        console.log(`🔍 Querying: ${sql}`);
        const result = this.parseResponse(this.db.query(sql));
        
        if (result.error) {
            console.log('❌', result.error);
        } else {
            console.log(`✅ Query returned ${result.count} rows in ${Math.round(result.elapsed_ms / 1000000)}ms`);
            if (result.rows && result.rows.length > 0) {
                console.table(this.formatResultsForTable(result));
            }
        }
        
        return result;
    }

    formatResultsForTable(result) {
        if (!result.rows || result.rows.length === 0) return [];
        
        return result.rows.map(row => {
            const obj = {};
            result.columns.forEach((col, i) => {
                obj[col] = row[i];
            });
            return obj;
        });
    }

    async begin() {
        if (!this.ready) return null;
        console.log('🚀 Starting transaction...');
        const result = this.parseResponse(this.db.begin());
        
        if (result.success) {
            console.log('✅ Transaction started');
        } else {
            console.log('❌', result.error);
        }
        
        return result;
    }

    async commit() {
        if (!this.ready) return null;
        console.log('💾 Committing transaction...');
        const result = this.parseResponse(this.db.commit());
        
        if (result.success) {
            console.log('✅ Transaction committed');
        } else {
            console.log('❌', result.error);
        }
        
        return result;
    }

    async rollback() {
        if (!this.ready) return null;
        console.log('↩️  Rolling back transaction...');
        const result = this.parseResponse(this.db.rollback());
        
        if (result.success) {
            console.log('✅ Transaction rolled back');
        } else {
            console.log('❌', result.error);
        }
        
        return result;
    }

    async close() {
        if (!this.ready) return null;
        console.log('🔌 Closing database connection...');
        const result = this.parseResponse(this.db.close());
        
        if (result.success) {
            console.log('✅ Database closed');
        } else {
            console.log('❌', result.error);
        }
        
        return result;
    }
}

// Demo function
async function runDemo() {
    const runner = new WASMRunner();
    
    console.log('🎯 tinySQL WASM Demo (Node.js)');
    console.log('================================');
    
    // Load WASM
    if (!await runner.loadWASM()) {
        process.exit(1);
    }
    
    // Connect
    if (!await runner.connect()) {
        process.exit(1);
    }
    
    // Show status
    console.log('\n📊 Database Status:');
    const status = await runner.status();
    console.log(JSON.stringify(status, null, 2));
    
    // Create table
    console.log('\n🏗️  Creating demo tables...');
    await runner.exec('CREATE TABLE users (id INT PRIMARY KEY, name TEXT, email TEXT, active BOOL)');
    await runner.exec('CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount FLOAT, status TEXT, meta JSON)');
    
    // Insert data
    console.log('\n📝 Inserting demo data...');
    await runner.exec("INSERT INTO users VALUES (1, 'Alice', 'alice@example.com', TRUE)");
    await runner.exec("INSERT INTO users VALUES (2, 'Bob', NULL, TRUE)");
    await runner.exec("INSERT INTO users VALUES (3, 'Carol', 'carol@example.com', FALSE)");
    
    await runner.exec("INSERT INTO orders VALUES (101, 1, 100.50, 'PAID', '{\"device\":\"web\",\"items\":[{\"sku\":\"A\",\"qty\":1}]}')");
    await runner.exec("INSERT INTO orders VALUES (102, 1, 75.00, 'PAID', '{\"device\":\"app\",\"items\":[{\"sku\":\"B\",\"qty\":2}]}')");
    await runner.exec("INSERT INTO orders VALUES (103, 2, 200.00, 'PAID', '{\"device\":\"web\"}')");
    
    // Query data
    console.log('\n🔍 Querying data...');
    await runner.query('SELECT * FROM users');
    await runner.query('SELECT * FROM orders');
    
    // Join query
    console.log('\n🔗 Join query...');
    await runner.query(`
        SELECT u.name, u.email, o.amount, o.status 
        FROM users u 
        JOIN orders o ON u.id = o.user_id 
        WHERE o.status = 'PAID'
    `);
    
    // Transaction demo
    console.log('\n💳 Transaction demo...');
    await runner.begin();
    await runner.exec("UPDATE users SET active = FALSE WHERE id = 1");
    await runner.query("SELECT * FROM users WHERE id = 1");
    await runner.rollback();
    await runner.query("SELECT * FROM users WHERE id = 1");
    
    // Cleanup
    await runner.close();
    
    console.log('\n🎉 Demo completed successfully!');
}

// CLI interface
async function main() {
    const args = process.argv.slice(2);
    
    if (args.length === 0) {
        await runDemo();
        return;
    }
    
    const runner = new WASMRunner();
    
    if (!await runner.loadWASM()) {
        process.exit(1);
    }
    
    if (!await runner.connect()) {
        process.exit(1);
    }
    
    const command = args[0].toLowerCase();
    const sql = args.slice(1).join(' ');
    
    switch (command) {
        case 'exec':
            if (!sql) {
                console.log('Usage: node wasm_runner.js exec "SQL STATEMENT"');
                process.exit(1);
            }
            await runner.exec(sql);
            break;
            
        case 'query':
            if (!sql) {
                console.log('Usage: node wasm_runner.js query "SELECT STATEMENT"');
                process.exit(1);
            }
            await runner.query(sql);
            break;
            
        case 'status':
            const status = await runner.status();
            console.log(JSON.stringify(status, null, 2));
            break;
            
        default:
            console.log('Available commands:');
            console.log('  node wasm_runner.js                    # Run interactive demo');
            console.log('  node wasm_runner.js exec "SQL"         # Execute SQL statement');
            console.log('  node wasm_runner.js query "SQL"        # Execute SQL query');
            console.log('  node wasm_runner.js status             # Show database status');
    }
    
    await runner.close();
}

// Handle graceful shutdown
process.on('SIGINT', () => {
    console.log('\n👋 Shutting down...');
    process.exit(0);
});

// Run if called directly
if (require.main === module) {
    main().catch(error => {
        console.error('💥 Fatal error:', error);
        process.exit(1);
    });
}

module.exports = WASMRunner;