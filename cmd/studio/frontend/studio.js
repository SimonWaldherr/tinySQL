// tinySQL Studio - Main Application Logic

const studio = {
    activeTabId: 0,
    nextTabId: 1,
    tabs: new Map(),
    theme: 'dark',
    monacoEditors: new Map(),
    useMonaco: false,

    init() {
        // Initialize first tab
        this.tabs.set(0, {
            id: 0,
            title: 'Query 1',
            content: ''
        });

        // Refresh object explorer on load
        this.refreshObjectExplorer();

        // Load theme preference
        const savedTheme = localStorage.getItem('studio-theme') || 'dark';
        if (savedTheme === 'light') {
            document.body.classList.add('light-theme');
            this.theme = 'light';
        }

        // Keyboard shortcuts
        document.addEventListener('keydown', (e) => {
            if (e.key === 'F5') {
                e.preventDefault();
                this.executeQuery();
            } else if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'n') {
                e.preventDefault();
                this.newQuery();
            } else if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 's') {
                e.preventDefault();
                this.saveDatabase();
            } else if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'w') {
                e.preventDefault();
                this.closeTab(e, this.activeTabId);
            } else if (e.ctrlKey && e.key === 'Tab') {
                e.preventDefault();
                this.switchToNextTab();
            }
        });

        // Drag & Drop support
        this.initDragAndDrop();

        // If Monaco isn't loaded yet, poll briefly and upgrade textareas when ready
        if (window.monaco && window.monaco.editor) {
            this.upgradeTextareasToMonaco();
        } else {
            let retries = 0;
            const interval = setInterval(() => {
                if (window.monaco && window.monaco.editor) {
                    clearInterval(interval);
                    this.upgradeTextareasToMonaco();
                }
                retries++;
                if (retries > 50) clearInterval(interval);
            }, 100);
        }

        // Auto-save editor content
        setInterval(() => this.saveEditorContent(), 1000);

        // Create editor for initial tab
        this.createEditorForTab(0, '');

        // Ensure initial tab element has listeners and is active
        const initialTab = document.querySelector('.tab[data-tab-id="0"]');
        if (initialTab) {
            initialTab.classList.add('active');
            const closeBtn = initialTab.querySelector('.tab-close');
            if (closeBtn) closeBtn.onclick = (e) => this.closeTab(e, 0);
            initialTab.onclick = (e) => { if (!e.target.classList.contains('tab-close')) this.switchTab(0); };
        }
    },

    // Convert existing textarea editors to Monaco instances when Monaco becomes available
    upgradeTextareasToMonaco() {
        try {
            document.querySelectorAll('.editor-container').forEach(container => {
                const tabId = parseInt(container.dataset.tabId, 10);
                if (this.monacoEditors.has(tabId)) return; // already a monaco instance
                // if container already has a monaco model, skip
                const ta = container.querySelector('textarea');
                if (!ta) return;
                const content = ta.value || '';
                container.innerHTML = '';
                const monacoInstance = monaco.editor.create(container, {
                    value: content,
                    language: 'sql',
                    theme: document.body.classList.contains('light-theme') ? 'vs' : 'vs-dark',
                    automaticLayout: true,
                    minimap: { enabled: false },
                    fontSize: 13,
                });
                this.monacoEditors.set(tabId, monacoInstance);
                this.useMonaco = true;
                if (tabId === this.activeTabId) try { monacoInstance.focus(); } catch (e) {}
            });
        } catch (e) { console.warn('Failed to upgrade textareas to Monaco', e); }
    },

    // Editor helpers (Monaco or fallback textarea)
    createEditorForTab(tabId, content) {
        const editorArea = document.querySelector('.query-editor-area');
        const editorContainer = document.createElement('div');
        editorContainer.className = 'editor-container';
        editorContainer.dataset.tabId = tabId;
        editorContainer.id = `editorContainer${tabId}`;
        editorArea.appendChild(editorContainer);

        // If this is the active tab, make the editor container visible
        if (tabId === this.activeTabId) editorContainer.classList.add('active');

        // If Monaco is available, instantiate an editor
        if (window.monaco && window.monaco.editor) {
            try {
                const monacoInstance = monaco.editor.create(editorContainer, {
                    value: content || '',
                    language: 'sql',
                    theme: document.body.classList.contains('light-theme') ? 'vs' : 'vs-dark',
                    automaticLayout: true,
                    minimap: { enabled: false },
                    fontSize: 13,
                });
                this.monacoEditors.set(tabId, monacoInstance);
                this.useMonaco = true;
                // focus the editor so the user can type immediately
                try { monacoInstance.focus(); } catch (e) {}
                return;
            } catch (e) {
                console.warn('Failed to create Monaco editor, falling back to textarea', e);
            }
        }

        // Fallback: use a plaintext textarea
        editorContainer.innerHTML = `\n            <textarea class="query-editor" id="editor${tabId}" placeholder="-- Enter SQL query here..."></textarea>\n        `;
        const ta = editorContainer.querySelector('textarea');
        ta.value = content || '';
        try { ta.focus(); } catch (e) {}
    },

    getEditorValue(tabId) {
        const monacoInstance = this.monacoEditors.get(tabId);
        if (monacoInstance) return monacoInstance.getValue();
        const ta = document.getElementById(`editor${tabId}`);
        if (ta) return ta.value;
        return '';
    },

    // Drag & Drop functionality
    initDragAndDrop() {
        const container = document.getElementById('studioContainer');
        const overlay = document.getElementById('dropOverlay');
        if (!container || !overlay) return;
        let dragCounter = 0;

        container.addEventListener('dragenter', (e) => {
            e.preventDefault();
            dragCounter++;
            if (dragCounter === 1) overlay.classList.add('active');
        });

        container.addEventListener('dragleave', (e) => {
            dragCounter--;
            if (dragCounter === 0) overlay.classList.remove('active');
        });

        container.addEventListener('dragover', (e) => e.preventDefault());

        container.addEventListener('drop', async (e) => {
            e.preventDefault();
            dragCounter = 0;
            overlay.classList.remove('active');

            const files = e.dataTransfer.files;
            if (files.length > 0) this.handleDroppedFiles(files);
        });
    },

    setEditorValue(tabId, value) {
        const monacoInstance = this.monacoEditors.get(tabId);
        if (monacoInstance) {
            const model = monacoInstance.getModel();
            if (model) model.setValue(value);
            return;
        }
        const ta = document.getElementById(`editor${tabId}`);
        if (ta) ta.value = value;
    },

    // Tab management
    newQuery() {
        const tabId = this.nextTabId++;
        const title = `Query ${tabId + 1}`;

        this.tabs.set(tabId, { id: tabId, title: title, content: '' });

        // Add tab to UI
        const tabBar = document.getElementById('tabBar');
        if (tabBar) {
            const tabEl = document.createElement('div');
            tabEl.className = 'tab';
            tabEl.dataset.tabId = tabId;
            tabEl.innerHTML = `
                <span class="tab-title">${title}</span>
                <span class="tab-close">×</span>
            `;
            const closeBtn = tabEl.querySelector('.tab-close');
            if (closeBtn) closeBtn.onclick = (e) => this.closeTab(e, tabId);
            tabEl.onclick = (e) => { if (!e.target.classList.contains('tab-close')) this.switchTab(tabId); };
            tabBar.appendChild(tabEl);
        }

        // Create editor container and switch
        this.createEditorForTab(tabId, '');
        this.switchTab(tabId);
        return tabId;
    },

    switchTab(tabId) {
        // Save current content
        this.saveEditorContent();

        // Update active tab UI
        document.querySelectorAll('.tab').forEach(tab => tab.classList.toggle('active', parseInt(tab.dataset.tabId, 10) === tabId));
        document.querySelectorAll('.editor-container').forEach(ed => ed.classList.toggle('active', parseInt(ed.dataset.tabId, 10) === tabId));

        this.activeTabId = tabId;

        const tab = this.tabs.get(tabId);
        if (tab) {
            this.setEditorValue(tabId, tab.content || '');
            const mon = this.monacoEditors.get(tabId);
            if (mon && mon.layout) try { mon.layout(); mon.focus(); } catch (e) {}
        }
    },

    closeTab(event, tabId) {
        if (event && event.stopPropagation) event.stopPropagation();
        if (this.tabs.size <= 1) { this.addMessage('Cannot close the last tab', 'info'); return; }

        this.tabs.delete(tabId);

        const tabEl = document.querySelector(`.tab[data-tab-id="${tabId}"]`);
        if (tabEl) tabEl.remove();
        const editorEl = document.querySelector(`.editor-container[data-tab-id="${tabId}"]`);
        if (editorEl) editorEl.remove();

        // Dispose Monaco instance
        const mon = this.monacoEditors.get(tabId);
        if (mon) { try { mon.dispose(); } catch (e) {} this.monacoEditors.delete(tabId); }

        // Switch to first available tab
        const first = this.tabs.keys().next().value;
        if (first !== undefined) this.switchTab(first);
    },

    saveEditorContent() {
        const tab = this.tabs.get(this.activeTabId);
        if (tab) tab.content = this.getEditorValue(this.activeTabId) || tab.content;
    },

    insertTextAtCursor(text) {
        const mon = this.monacoEditors.get(this.activeTabId);
        if (mon) {
            try {
                const selection = mon.getSelection();
                const id = { major: 1, minor: 1 };
                const range = selection;
                mon.executeEdits('studio', [{ range, text, identifier: id }]);
                // move cursor to end of inserted text
                const endPos = mon.getPosition();
                try { mon.focus(); } catch (e) {}
                return;
            } catch (e) { console.warn('Insert into Monaco failed', e); }
        }

        const ta = document.getElementById(`editor${this.activeTabId}`);
        if (ta) {
            const start = ta.selectionStart || 0;
            const end = ta.selectionEnd || start;
            const before = ta.value.substring(0, start);
            const after = ta.value.substring(end);
            ta.value = before + text + after;
            const pos = start + text.length;
            try { ta.setSelectionRange(pos, pos); ta.focus(); } catch (e) {}
        }
    },

    // Quote identifiers using SQL standard double-quote and escape internal quotes
    quoteIdentifier(name) {
        if (typeof name !== 'string') return name;
        // If already a simple identifier (letters, digits, underscore) and not starting with digit, we can leave unquoted
        if (/^[A-Za-z_][A-Za-z0-9_]*$/.test(name)) return name;
        // Otherwise escape internal double quotes by doubling them and wrap in double quotes
        const escaped = name.replace(/"/g, '""');
        return `"${escaped}"`;
    },

    // Object Explorer: refresh list of tables
    async refreshObjectExplorer() {
        try {
            if (!window.go || !window.go.main || !window.go.main.App) return;

            const tables = await window.go.main.App.ListTables();
            const tablesTree = document.getElementById('tablesTree');
            if (!tablesTree) return;
            tablesTree.innerHTML = '';

            if (!tables || tables.length === 0) {
                tablesTree.innerHTML = '<div style="padding:8px;color:#6a737d;font-size:12px;">No tables</div>';
                return;
            }

            for (const tableName of tables) {
                const tableNode = document.createElement('div');
                tableNode.className = 'tree-node';
                tableNode.innerHTML = `
                    <div class="tree-node-header" onclick="studio.toggleTableNode(this, '${tableName.replace(/'/g, "\\'")}')">
                        <span class="tree-icon">▶</span>
                        <span class="tree-label">📋 ${escapeHtml(tableName)}</span>
                    </div>
                    <div class="tree-node-children" data-table="${escapeHtml(tableName)}"></div>
                `;
                tablesTree.appendChild(tableNode);
            }

            // Expand first node if present
            const firstHeader = document.querySelector('.tree-node-header');
            if (firstHeader && !firstHeader.classList.contains('expanded')) this.toggleTreeNode(firstHeader);
        } catch (err) {
            console.error('Failed to refresh object explorer:', err);
            this.addMessage('Failed to refresh object explorer: ' + err, 'error');
        }
    },

    toggleTreeNode(element) {
        element.classList.toggle('expanded');
        const children = element.nextElementSibling; if (children) children.classList.toggle('expanded');
    },

    async toggleTableNode(element, tableName) {
        this.toggleTreeNode(element);

        const children = element.nextElementSibling;
        if (children && children.classList.contains('expanded') && children.children.length === 0) {
            try {
                const tableInfo = await window.go.main.App.GetTableInfo(tableName);
                if (tableInfo && tableInfo.columns) {
                    children.innerHTML = `
                        <div class="tree-item" onclick="studio.previewTable('${tableName.replace(/'/g, "\\'")}')">👁️ Preview (Top 100)</div>
                        <div class="tree-item" onclick="studio.selectFromTable('${tableName.replace(/'/g, "\\'")}')">📝 SELECT * FROM</div>
                        <div class="tree-item" onclick="studio.exportTable('${tableName.replace(/'/g, "\\'")}')">💾 Export to CSV</div>
                        <div style="padding:4px 8px;color:#6a737d;font-size:11px;font-weight:600;">COLUMNS (${tableInfo.columns.length})</div>
                    `;
                    tableInfo.columns.forEach(col => {
                        const colItem = document.createElement('div');
                        colItem.className = 'tree-item'; colItem.style.fontSize = '12px';
                        colItem.innerHTML = `🔹 ${escapeHtml(col.name)} <span style="color:#6a737d;">${escapeHtml(col.type)}</span>`;
                        // Insert column name at cursor when clicked
                        colItem.addEventListener('click', (e) => { e.stopPropagation(); try { studio.insertTextAtCursor(col.name); } catch (err) { console.warn(err); } });
                        children.appendChild(colItem);
                    });
                }
            } catch (err) { console.error('Failed to load table info:', err); }
        }
    },
    
    async executeSelection() {
        const selectedText = (this.getEditorSelectionText(this.activeTabId) || '').trim();
        if (!selectedText) {
            this.addMessage('No text selected. Executing entire query.', 'info');
            await this.executeQuery();
            return;
        }

        // Execute selection directly
        const sql = selectedText;
        try {
            this.setStatus('Executing...'); this.clearMessages();
            const result = await window.go.main.App.ExecuteQuery(sql);
            this.displayResults(result);
            if (result.error) { this.addMessage('Error: ' + result.error, 'error'); this.setStatus('Error'); }
            else { this.addMessage(`Query completed successfully (${result.count} rows, ${result.elapsed_ms}ms)`, 'success'); this.setStatus('Success', result.count, result.elapsed_ms); }
        } catch (err) { this.addMessage('Exception: ' + err, 'error'); this.setStatus('Error'); }
    },

    async executeQuery() {
        const sql = (this.getEditorValue(this.activeTabId) || '').trim();
        if (!sql) { this.addMessage('No query to execute', 'info'); return; }

        this.setStatus('Executing...'); this.clearMessages();
        try {
            const result = await window.go.main.App.ExecuteQuery(sql);
            this.displayResults(result);
            if (result.error) {
                this.addMessage('Error: ' + result.error, 'error'); this.setStatus('Error');
            } else {
                this.addMessage(`Query completed successfully (${result.count} rows, ${result.elapsed_ms}ms)`, 'success');
                this.setStatus('Success', result.count, result.elapsed_ms);
                if (/^(CREATE|DROP|ALTER)\s+TABLE/i.test(sql)) this.refreshObjectExplorer();
            }
        } catch (err) {
            this.addMessage('Exception: ' + err, 'error'); this.setStatus('Error');
        }
    },
    
    displayResults(result) {
        const resultsView = document.getElementById('resultsView');
        
        if (result.error) {
            resultsView.innerHTML = `<div class="no-results" style="color:#f48771;">Error: ${escapeHtml(result.error)}</div>`;
            return;
        }
        
        if (result.message && !result.columns) {
            resultsView.innerHTML = `<div class="no-results" style="color:#4ec9b0;">${escapeHtml(result.message)}</div>`;
            return;
        }
        
        if (!result.columns || result.columns.length === 0) {
            resultsView.innerHTML = '<div class="no-results">No results returned.</div>';
            return;
        }
        
        let html = '<table class="results-table"><thead><tr>';
        result.columns.forEach(col => {
            html += `<th>${escapeHtml(col)}</th>`;
        });
        html += '</tr></thead><tbody>';
        
        if (result.rows && result.rows.length > 0) {
            result.rows.forEach(row => {
                html += '<tr>';
                row.forEach(cell => {
                    html += `<td>${formatValue(cell)}</td>`;
                });
                html += '</tr>';
            });
        }
        
        html += '</tbody></table>';
        resultsView.innerHTML = html;
        
        // Switch to results tab
        this.switchResultsTab('results');
    },
    
    // Results Panel
    switchResultsTab(tab) {
        document.querySelectorAll('.results-tab').forEach(t => {
            t.classList.toggle('active', t.textContent.toLowerCase() === tab);
        });
        
        document.querySelectorAll('.results-view').forEach(view => {
            view.classList.toggle('active', view.id === (tab + 'View'));
        });
    },
    
    addMessage(text, type = 'info') {
        const messagesContent = document.querySelector('.messages-content');
        const message = document.createElement('div');
        message.className = `message message-${type}`;
        message.innerHTML = `<span>[${new Date().toLocaleTimeString()}]</span><span>${escapeHtml(text)}</span>`;
        messagesContent.appendChild(message);
        messagesContent.scrollTop = messagesContent.scrollHeight;
    },
    
    clearMessages() {
        document.querySelector('.messages-content').innerHTML = '';
    },
    
    setStatus(text, rowCount = null, elapsedMs = null) {
        document.getElementById('statusText').textContent = text;
        document.getElementById('rowCount').textContent = rowCount !== null ? `${rowCount} rows` : '';
        document.getElementById('executionTime').textContent = elapsedMs !== null ? `${elapsedMs}ms` : '';
    },
    
    // File Operations
    async importFile() {
        try {
            const path = await window.go.main.App.OpenFileDialog();
            if (!path) return;
            
            this.setStatus('Importing...');
            const result = await window.go.main.App.ExecuteImportFromPath(path, '');
            
            if (result.error) {
                this.addMessage('Import failed: ' + result.error, 'error');
                this.setStatus('Import failed');
            } else {
                this.addMessage(`Imported ${result.rowsImported} rows into table "${result.tableName}"`, 'success');
                this.setStatus('Import completed');
                this.refreshObjectExplorer();
            }
        } catch (err) {
            this.addMessage('Import error: ' + err, 'error');
            this.setStatus('Error');
        }
    },
    
    async saveDatabase() {
        try {
            const path = await window.go.main.App.SaveDatabaseToFile();
            if (path) {
                this.addMessage(`Database saved to: ${path}`, 'success');
                this.setStatus('Database saved');
            }
        } catch (err) {
            this.addMessage('Save failed: ' + err, 'error');
            this.setStatus('Error');
        }
    },
    
    async loadDatabase() {
        try {
            const path = await window.go.main.App.LoadDatabaseFromFile();
            if (path) {
                this.addMessage(`Database loaded from: ${path}`, 'success');
                this.setStatus('Database loaded');
                this.refreshObjectExplorer();
            }
        } catch (err) {
            this.addMessage('Load failed: ' + err, 'error');
            this.setStatus('Error');
        }
    },
    
    // Helper Methods
    async previewTable(tableName) {
        let sql = `SELECT * FROM ${tableName} LIMIT 100;`;
        try {
            if (window.go && window.go.main && window.go.main.App) {
                const info = await window.go.main.App.GetTableInfo(tableName);
                if (info && info.columns && info.columns.length > 0) {
                    const cols = info.columns.map(c => this.quoteIdentifier(c.name)).join(', ');
                    sql = `SELECT ${cols} FROM ${this.quoteIdentifier(tableName)} LIMIT 100;`;
                }
            }
        } catch (err) {
            console.warn('Could not fetch table columns for preview, falling back to *', err);
        }

        this.setEditorValue(this.activeTabId, sql);
        const mon = this.monacoEditors.get(this.activeTabId);
        if (mon && mon.focus) try { mon.focus(); } catch (e) {}
        else { const ta = document.getElementById(`editor${this.activeTabId}`); if (ta) ta.focus(); }
        this.executeQuery();
    },
    
    selectFromTable(tableName) {
        const sql = `SELECT * FROM ${tableName};`;
        this.setEditorValue(this.activeTabId, sql);
        const mon = this.monacoEditors.get(this.activeTabId);
        if (mon && mon.focus) try { mon.focus(); } catch (e) {}
        else { const ta = document.getElementById(`editor${this.activeTabId}`); if (ta) ta.focus(); }
    },
    
    async exportTable(tableName) {
        try {
            this.setStatus('Exporting...');
            const result = await window.go.main.App.ExportTableToCSV(tableName);
            this.addMessage(result, 'success');
            this.setStatus('Export completed');
        } catch (err) {
            this.addMessage('Export failed: ' + err, 'error');
            this.setStatus('Error');
        }
    },
    
    // UI Enhancement Methods
    switchToNextTab() {
        const tabIds = Array.from(this.tabs.keys());
        const currentIndex = tabIds.indexOf(this.activeTabId);
        const nextIndex = (currentIndex + 1) % tabIds.length;
        this.switchTab(tabIds[nextIndex]);
    },
    
    async clearDatabase() {
        if (!confirm('Are you sure you want to clear all tables? This cannot be undone.')) {
            return;
        }
        
        try {
            const tables = await window.go.main.App.ListTables();
            for (const tableName of tables) {
                await window.go.main.App.ExecuteQuery(`DROP TABLE ${tableName}`);
            }
            this.addMessage('All tables cleared', 'success');
            this.refreshObjectExplorer();
            this.setStatus('Database cleared');
        } catch (err) {
            this.addMessage('Failed to clear database: ' + err, 'error');
        }
    },
    
    formatQuery() {
        const editor = document.getElementById(`editor${this.activeTabId}`);
        if (!editor) return;
        
        let sql = editor.value.trim();
        if (!sql) return;
        
        // Basic SQL formatting
        const keywords = [
            'SELECT', 'FROM', 'WHERE', 'JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'INNER JOIN',
            'GROUP BY', 'HAVING', 'ORDER BY', 'LIMIT', 'OFFSET',
            'INSERT INTO', 'VALUES', 'UPDATE', 'SET', 'DELETE FROM',
            'CREATE TABLE', 'DROP TABLE', 'ALTER TABLE',
            'AND', 'OR', 'NOT', 'IN', 'BETWEEN', 'LIKE', 'IS NULL', 'IS NOT NULL'
        ];
        
        // Add newlines before major keywords
        keywords.forEach(keyword => {
            const regex = new RegExp(`\\b${keyword}\\b`, 'gi');
            sql = sql.replace(regex, '\n' + keyword);
        });
        
        // Clean up multiple newlines and trim
        sql = sql.replace(/\n+/g, '\n').trim();
        
        // Indent lines after SELECT, WHERE, etc.
        const lines = sql.split('\n');
        let indentLevel = 0;
        const formatted = lines.map(line => {
            line = line.trim();
            if (line.match(/^(FROM|WHERE|JOIN|LEFT JOIN|RIGHT JOIN|INNER JOIN|GROUP BY|HAVING|ORDER BY)/i)) {
                return '  ' + line;
            }
            return line;
        }).join('\n');
        
        editor.value = formatted;
        this.addMessage('Query formatted', 'success');
    },
    
    toggleTheme() {
        this.theme = this.theme === 'dark' ? 'light' : 'dark';
        document.body.classList.toggle('light-theme');
        localStorage.setItem('studio-theme', this.theme);
        this.addMessage(`Switched to ${this.theme} theme`, 'info');
    },
    
    showHelp() {
        const modal = document.getElementById('helpModal');
        modal.classList.add('active');
    },
    
    closeModal(modalId) {
        const modal = document.getElementById(modalId);
        modal.classList.remove('active');
    }
};

// Utility functions
function escapeHtml(text) {
    if (!text) return '';
    return String(text).replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#039;");
}

function formatValue(val) {
    if (val === null || val === undefined) return '<span style="color:#6a737d;">NULL</span>';
    if (typeof val === 'boolean') return `<span style="color:#569cd6;">${val}</span>`;
    if (typeof val === 'number') return `<span style="color:#b5cea8;">${val}</span>`;
    if (typeof val === 'object') return escapeHtml(JSON.stringify(val));
    return escapeHtml(String(val));
}

// Initialize on load
document.addEventListener('DOMContentLoaded', () => {
    studio.init();
});

// Close modals on ESC key
document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') {
        document.querySelectorAll('.modal.active').forEach(modal => {
            modal.classList.remove('active');
        });
    }
});

// Close modals on background click
document.addEventListener('click', (e) => {
    if (e.target.classList.contains('modal')) {
        e.target.classList.remove('active');
    }
});

// Expose `studio` to the global `window` so inline `onclick="studio..."` works
try { window.studio = studio; } catch (e) { /* ignore non-browser envs */ }

// If there were queued stub calls (from the initial stub in index.html), flush them now
try {
    if (window.__studio_queue && Array.isArray(window.__studio_queue)) {
        const q = window.__studio_queue;
        for (const [name, args] of q) {
            try { if (typeof studio[name] === 'function') studio[name](...args); } catch (e) { console.warn('studio queued call failed', name, e); }
        }
        delete window.__studio_queue;
    }
} catch (e) {
    // ignore
}
