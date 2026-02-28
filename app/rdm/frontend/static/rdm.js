/* Reference Data Management Center — app logic */
'use strict';

// =========================================================================
// State
// =========================================================================

const state = {
    datasets: [],          // flat list from /api/datasets
    groups: {},            // domain → [dataset, …]
    current: null,         // currently selected dataset (full object with rows)
    csvFile: null,         // staged CSV file for import
    csvRows: [],           // parsed CSV rows for preview
};

// =========================================================================
// API helpers
// =========================================================================

const api = {
    async get(path) {
        const r = await fetch(path);
        if (!r.ok) { const e = await r.json().catch(() => ({})); throw new Error(e.detail || r.statusText); }
        return r.json();
    },
    async post(path, body) {
        const r = await fetch(path, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
        if (!r.ok) { const e = await r.json().catch(() => ({})); throw new Error(e.detail || r.statusText); }
        return r.json();
    },
    async put(path, body) {
        const r = await fetch(path, { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
        if (!r.ok) { const e = await r.json().catch(() => ({})); throw new Error(e.detail || r.statusText); }
        return r.json();
    },
    async del(path) {
        const r = await fetch(path, { method: 'DELETE' });
        if (!r.ok) { const e = await r.json().catch(() => ({})); throw new Error(e.detail || r.statusText); }
        return r.json();
    },
    async postForm(path, formData) {
        const r = await fetch(path, { method: 'POST', body: formData });
        if (!r.ok) { const e = await r.json().catch(() => ({})); throw new Error(e.detail || r.statusText); }
        return r.json();
    },
};

// =========================================================================
// Toast
// =========================================================================

let toastTimer = null;
function toast(msg, type = 'info') {
    const el = document.getElementById('toast');
    el.textContent = msg;
    el.className = `toast ${type}`;
    if (toastTimer) clearTimeout(toastTimer);
    toastTimer = setTimeout(() => { el.classList.add('hidden'); }, 3200);
}

// =========================================================================
// Nav
// =========================================================================

async function loadNav() {
    const groups = await api.get('/api/datasets/groups');
    state.groups = groups;
    renderNav(groups);
}

function renderNav(groups, filter = '') {
    const tree = document.getElementById('nav-tree');
    const filterLower = filter.toLowerCase();

    if (Object.keys(groups).length === 0) {
        tree.innerHTML = '<div class="nav-loading">No datasets yet — create one!</div>';
        return;
    }

    tree.innerHTML = '';
    for (const [domain, datasets] of Object.entries(groups)) {
        const filtered = filter
            ? datasets.filter(d => d.display_name.toLowerCase().includes(filterLower) || d.name.includes(filterLower))
            : datasets;
        if (filtered.length === 0) continue;

        const domainEl = document.createElement('div');
        domainEl.className = 'nav-domain';
        domainEl.innerHTML = `
            <div class="nav-domain-header" data-domain="${domain}">
                <span class="nav-domain-label">${domain}</span>
                <span class="nav-domain-toggle">▾</span>
            </div>
            <div class="nav-domain-items"></div>`;

        const itemsEl = domainEl.querySelector('.nav-domain-items');
        for (const ds of filtered) {
            const item = document.createElement('div');
            item.className = 'nav-item' + (state.current?.dataset?.id === ds.id ? ' active' : '');
            item.dataset.id = ds.id;
            item.innerHTML = `
                <span class="nav-item-icon">📋</span>
                <span class="nav-item-name">${ds.display_name}</span>
                <span class="nav-item-status ${ds.status}" title="${ds.status}"></span>`;
            item.addEventListener('click', () => selectDataset(ds.id));
            itemsEl.appendChild(item);
        }

        // Toggle collapse
        domainEl.querySelector('.nav-domain-header').addEventListener('click', (e) => {
            if (e.target.closest('.nav-item')) return;
            domainEl.classList.toggle('collapsed');
        });

        tree.appendChild(domainEl);
    }
}

// =========================================================================
// Dataset selection & rendering
// =========================================================================

async function selectDataset(datasetId) {
    try {
        const data = await api.get(`/api/datasets/${datasetId}`);
        state.current = data;
        renderDatasetView(data);
        // Update active nav item
        document.querySelectorAll('.nav-item').forEach(el => {
            el.classList.toggle('active', el.dataset.id === datasetId);
        });
        document.getElementById('empty-state').classList.add('hidden');
        document.getElementById('dataset-view').classList.remove('hidden');
        // Show data tab by default
        switchTab('data');
    } catch (err) {
        toast(`Error loading dataset: ${err.message}`, 'error');
    }
}

function renderDatasetView(data) {
    const { dataset, rows } = data;

    // Header
    document.getElementById('ds-display-name').textContent = dataset.display_name;
    const badge = document.getElementById('ds-status-badge');
    badge.textContent = dataset.status;
    badge.className = `status-badge ${dataset.status}`;

    document.getElementById('ds-domain').textContent = dataset.domain;
    document.getElementById('ds-version').textContent = `v${dataset.version}`;
    document.getElementById('ds-row-count').textContent = `${rows.length} rows`;
    document.getElementById('ds-description').textContent = dataset.description || '';

    const ownersEl = document.getElementById('ds-owners');
    ownersEl.textContent = dataset.owners.length ? dataset.owners[0] : '';
    ownersEl.style.display = dataset.owners.length ? '' : 'none';

    // Publish button state
    const publishBtn = document.getElementById('btn-publish');
    if (dataset.status === 'active' && dataset.atlan_synced_at) {
        publishBtn.textContent = '✓ Synced to Atlan';
        publishBtn.classList.add('btn-secondary');
        publishBtn.classList.remove('btn-primary');
    } else {
        publishBtn.textContent = 'Publish to Atlan';
        publishBtn.classList.add('btn-primary');
        publishBtn.classList.remove('btn-secondary');
    }

    renderGrid(dataset, rows);
    renderSchema(dataset);
    renderMdlh(dataset.id);
    renderGovernance(dataset);
}

// =========================================================================
// Grid (Data tab)
// =========================================================================

function renderGrid(dataset, rows) {
    const thead = document.getElementById('grid-head');
    const tbody = document.getElementById('grid-body');
    const cols = dataset.columns;

    // Header
    thead.innerHTML = '';
    const headerRow = document.createElement('tr');
    for (const col of cols) {
        const th = document.createElement('th');
        th.textContent = col.display_name;
        th.title = col.description || '';
        if (col.is_primary_key) th.classList.add('pk-col');
        headerRow.appendChild(th);
    }
    const thAct = document.createElement('th');
    thAct.style.width = '60px';
    headerRow.appendChild(thAct);
    thead.appendChild(headerRow);

    // Rows
    tbody.innerHTML = '';
    document.getElementById('grid-row-count').textContent = `${rows.length} row${rows.length !== 1 ? 's' : ''}`;

    for (const row of rows) {
        tbody.appendChild(buildDataRow(dataset, row));
    }
}

function buildDataRow(dataset, row) {
    const tr = document.createElement('tr');
    tr.dataset.rowId = row.id;

    for (const col of dataset.columns) {
        const td = document.createElement('td');
        const val = row.values[col.name] ?? '';
        td.textContent = val;
        if (col.is_primary_key) td.classList.add('pk-cell');
        if (['integer', 'decimal'].includes(col.column_type)) td.classList.add('mono-cell');
        td.dataset.col = col.name;

        // Double-click to edit
        td.addEventListener('dblclick', () => startCellEdit(tr, td, dataset, row));
        tr.appendChild(td);
    }

    // Actions
    const tdAct = document.createElement('td');
    tdAct.className = 'td-actions';
    tdAct.innerHTML = `<button class="btn-delete-row" title="Delete row">🗑</button>`;
    tdAct.querySelector('.btn-delete-row').addEventListener('click', () => deleteRow(dataset.id, row.id, tr));
    tr.appendChild(tdAct);

    return tr;
}

function startCellEdit(tr, td, dataset, row) {
    if (tr.classList.contains('editing')) return;
    tr.classList.add('editing');

    const originalValues = { ...row.values };

    // Convert all cells to inputs
    for (const col of dataset.columns) {
        const cell = tr.querySelector(`td[data-col="${col.name}"]`);
        const val = row.values[col.name] ?? '';
        const input = document.createElement('input');
        input.className = 'cell-input';
        input.value = val;
        input.dataset.col = col.name;
        cell.textContent = '';
        cell.appendChild(input);
    }

    // Update actions
    const tdAct = tr.querySelector('.td-actions');
    tdAct.innerHTML = `
        <button class="btn-save-row" title="Save">✓</button>
        <button class="btn-cancel-row" title="Cancel">✕</button>`;

    tdAct.querySelector('.btn-save-row').addEventListener('click', async () => {
        const newValues = {};
        tr.querySelectorAll('.cell-input').forEach(inp => { newValues[inp.dataset.col] = inp.value; });
        await saveRow(dataset, row, newValues, tr);
    });

    tdAct.querySelector('.btn-cancel-row').addEventListener('click', () => {
        row.values = originalValues;
        const newTr = buildDataRow(dataset, row);
        tr.replaceWith(newTr);
    });

    // Focus first input
    tr.querySelector('.cell-input')?.focus();
}

async function saveRow(dataset, row, newValues, tr) {
    try {
        const updated = await api.put(`/api/datasets/${dataset.id}/rows/${row.id}`, { values: newValues });
        row.values = updated.values;
        const newTr = buildDataRow(dataset, row);
        tr.replaceWith(newTr);
        // Update state
        if (state.current) {
            const idx = state.current.rows.findIndex(r => r.id === row.id);
            if (idx !== -1) state.current.rows[idx] = row;
        }
        toast('Row saved', 'success');
    } catch (err) {
        toast(`Save failed: ${err.message}`, 'error');
    }
}

async function deleteRow(datasetId, rowId, tr) {
    if (!confirm('Delete this row?')) return;
    try {
        await api.del(`/api/datasets/${datasetId}/rows/${rowId}`);
        tr.remove();
        // Update count
        const countEl = document.getElementById('grid-row-count');
        const n = document.getElementById('grid-body').querySelectorAll('tr').length;
        countEl.textContent = `${n} row${n !== 1 ? 's' : ''}`;
        document.getElementById('ds-row-count').textContent = `${n} rows`;
        toast('Row deleted', 'info');
    } catch (err) {
        toast(`Delete failed: ${err.message}`, 'error');
    }
}

function addNewRowToGrid(dataset) {
    const tbody = document.getElementById('grid-body');
    const cols = dataset.columns;

    const tr = document.createElement('tr');
    tr.classList.add('new-row');

    for (const col of cols) {
        const td = document.createElement('td');
        td.dataset.col = col.name;
        const input = document.createElement('input');
        input.className = 'cell-input';
        input.placeholder = col.display_name;
        input.dataset.col = col.name;
        td.appendChild(input);
        tr.appendChild(td);
    }

    const tdAct = document.createElement('td');
    tdAct.className = 'td-actions';
    tdAct.style.opacity = '1';
    tdAct.innerHTML = `
        <button class="btn-save-row" title="Save">✓</button>
        <button class="btn-cancel-row" title="Cancel">✕</button>`;

    tdAct.querySelector('.btn-save-row').addEventListener('click', async () => {
        const values = {};
        tr.querySelectorAll('.cell-input').forEach(inp => { if (inp.value) values[inp.dataset.col] = inp.value; });
        if (Object.keys(values).length === 0) { tr.remove(); return; }
        try {
            const row = await api.post(`/api/datasets/${dataset.id}/rows`, { values });
            state.current.rows.push(row);
            const newTr = buildDataRow(dataset, row);
            tr.replaceWith(newTr);
            const n = tbody.querySelectorAll('tr').length;
            document.getElementById('grid-row-count').textContent = `${n} row${n !== 1 ? 's' : ''}`;
            document.getElementById('ds-row-count').textContent = `${n} rows`;
            toast('Row added', 'success');
        } catch (err) {
            toast(`Add failed: ${err.message}`, 'error');
        }
    });

    tdAct.querySelector('.btn-cancel-row').addEventListener('click', () => tr.remove());
    tr.appendChild(tdAct);

    tbody.prepend(tr);
    tr.querySelector('.cell-input')?.focus();
}

// =========================================================================
// Schema tab
// =========================================================================

function renderSchema(dataset) {
    const tbody = document.getElementById('schema-body');
    tbody.innerHTML = '';
    for (const col of dataset.columns) {
        const tr = document.createElement('tr');
        tr.innerHTML = `
            <td><code style="font-family:var(--font-mono);font-size:12.5px">${col.name}</code></td>
            <td>${col.display_name}</td>
            <td><span class="type-badge">${col.column_type}</span></td>
            <td>${col.is_primary_key ? '<span class="pk-badge">PK</span>' : '—'}</td>
            <td>${col.is_nullable ? '✓' : '✕'}</td>
            <td style="color:var(--text-muted);font-size:12.5px">${col.description || '—'}</td>
            <td></td>`;
        tbody.appendChild(tr);
    }
    if (dataset.columns.length === 0) {
        tbody.innerHTML = '<tr><td colspan="7" style="color:var(--text-muted);padding:20px;text-align:center">No columns defined. Use the Schema tab to add columns.</td></tr>';
    }
}

// =========================================================================
// MDLH tab
// =========================================================================

async function renderMdlh(datasetId) {
    try {
        const s = await api.get(`/api/datasets/${datasetId}/mdlh-snippet`);
        document.getElementById('snippet-gold').textContent = s.snowflake_gold;
        document.getElementById('snippet-raw').textContent = s.snowflake_raw;
        document.getElementById('snippet-description').textContent = s.description;
    } catch (err) {
        document.getElementById('snippet-gold').textContent = '-- Error loading snippet';
    }
}

// =========================================================================
// Governance tab
// =========================================================================

function renderGovernance(dataset) {
    // Owners
    const ownersEl = document.getElementById('gov-owners');
    ownersEl.innerHTML = dataset.owners.length
        ? dataset.owners.map(o => `<span class="tag-pill owner">${o}</span>`).join('')
        : '<span style="color:var(--text-muted);font-size:12.5px">No owners assigned</span>';

    // Tags
    const tagsEl = document.getElementById('gov-tags');
    tagsEl.innerHTML = dataset.tags.length
        ? dataset.tags.map(t => `<span class="tag-pill">${t}</span>`).join('')
        : '<span style="color:var(--text-muted);font-size:12.5px">No tags</span>';

    // Details
    document.getElementById('gov-name').textContent    = dataset.name;
    document.getElementById('gov-domain').textContent  = dataset.domain;
    document.getElementById('gov-version').textContent = dataset.version;
    document.getElementById('gov-status').textContent  = dataset.status;
    document.getElementById('gov-created').textContent = fmtDate(dataset.created_at);
    document.getElementById('gov-updated').textContent = fmtDate(dataset.updated_at);
    document.getElementById('gov-synced').textContent  = dataset.atlan_synced_at
        ? fmtDate(dataset.atlan_synced_at)
        : 'Not synced';
}

function fmtDate(iso) {
    if (!iso) return '—';
    return new Date(iso).toLocaleString('en-US', { dateStyle: 'medium', timeStyle: 'short' });
}

// =========================================================================
// Tabs
// =========================================================================

function switchTab(tabName) {
    document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.classList.toggle('active', btn.dataset.tab === tabName);
    });
    document.querySelectorAll('.tab-panel').forEach(panel => {
        panel.classList.toggle('hidden', panel.id !== `tab-${tabName}`);
    });
}

// =========================================================================
// Create Dataset modal
// =========================================================================

function openNewDatasetModal() {
    document.getElementById('new-ds-display-name').value = '';
    document.getElementById('new-ds-name').value = '';
    document.getElementById('new-ds-description').value = '';
    document.getElementById('modal-new-dataset').classList.remove('hidden');
    document.getElementById('new-ds-display-name').focus();
}

// Auto-slug display name → name
document.getElementById('new-ds-display-name').addEventListener('input', (e) => {
    const nameField = document.getElementById('new-ds-name');
    if (!nameField.dataset.manual) {
        nameField.value = e.target.value.toLowerCase().replace(/\s+/g, '_').replace(/[^a-z0-9_]/g, '');
    }
});
document.getElementById('new-ds-name').addEventListener('input', (e) => {
    e.target.dataset.manual = 'true';
});

document.getElementById('btn-create-dataset').addEventListener('click', async () => {
    const displayName = document.getElementById('new-ds-display-name').value.trim();
    const name        = document.getElementById('new-ds-name').value.trim();
    const domain      = document.getElementById('new-ds-domain').value;
    const description = document.getElementById('new-ds-description').value.trim();

    if (!displayName || !name) { toast('Display name and slug are required', 'error'); return; }

    try {
        const ds = await api.post('/api/datasets', { display_name: displayName, name, domain, description, columns: [], owners: [], tags: [] });
        closeAllModals();
        toast(`Dataset "${ds.display_name}" created`, 'success');
        await loadNav();
        await selectDataset(ds.id);
        switchTab('schema');  // Jump to schema so they can add columns
    } catch (err) {
        toast(`Error: ${err.message}`, 'error');
    }
});

// =========================================================================
// Add Column modal
// =========================================================================

function openAddColumnModal() {
    document.getElementById('col-name').value = '';
    document.getElementById('col-display-name').value = '';
    document.getElementById('col-type').value = 'string';
    document.getElementById('col-pk').checked = false;
    document.getElementById('col-nullable').checked = true;
    document.getElementById('col-description').value = '';
    document.getElementById('modal-add-column').classList.remove('hidden');
    document.getElementById('col-name').focus();
}

// Auto-slug col name → display name
document.getElementById('col-name').addEventListener('input', (e) => {
    const dispField = document.getElementById('col-display-name');
    if (!dispField.dataset.manual) {
        dispField.value = e.target.value.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
    }
});
document.getElementById('col-display-name').addEventListener('input', (e) => {
    e.target.dataset.manual = 'true';
});

document.getElementById('btn-confirm-add-column').addEventListener('click', async () => {
    if (!state.current) return;
    const colName = document.getElementById('col-name').value.trim();
    const dispName = document.getElementById('col-display-name').value.trim();
    if (!colName || !dispName) { toast('Column name and display name required', 'error'); return; }

    const dataset = state.current.dataset;
    const newCol = {
        name: colName,
        display_name: dispName,
        column_type: document.getElementById('col-type').value,
        is_primary_key: document.getElementById('col-pk').checked,
        is_nullable: document.getElementById('col-nullable').checked,
        description: document.getElementById('col-description').value.trim() || null,
    };

    const updatedCols = [...dataset.columns, newCol];
    try {
        const updated = await api.put(`/api/datasets/${dataset.id}`, { columns: updatedCols });
        state.current.dataset = updated;
        renderSchema(updated);
        renderGrid(updated, state.current.rows);
        renderMdlh(dataset.id);
        closeAllModals();
        toast(`Column "${newCol.display_name}" added`, 'success');
    } catch (err) {
        toast(`Error: ${err.message}`, 'error');
    }
});

// =========================================================================
// CSV Import
// =========================================================================

function openImportModal() {
    state.csvFile = null;
    state.csvRows = [];
    document.getElementById('csv-filename').textContent = '';
    document.getElementById('csv-preview').classList.add('hidden');
    document.getElementById('csv-preview').innerHTML = '';
    document.getElementById('csv-replace-all').checked = false;
    document.getElementById('btn-confirm-import').disabled = true;
    document.getElementById('modal-import-csv').classList.remove('hidden');
}

document.getElementById('csv-file-input').addEventListener('change', (e) => {
    handleCsvFile(e.target.files[0]);
});

const dropZone = document.getElementById('csv-drop-zone');
dropZone.addEventListener('dragover', (e) => { e.preventDefault(); dropZone.classList.add('dragover'); });
dropZone.addEventListener('dragleave', () => dropZone.classList.remove('dragover'));
dropZone.addEventListener('drop', (e) => {
    e.preventDefault();
    dropZone.classList.remove('dragover');
    handleCsvFile(e.dataTransfer.files[0]);
});

function handleCsvFile(file) {
    if (!file || !file.name.endsWith('.csv')) { toast('Please select a .csv file', 'error'); return; }
    state.csvFile = file;
    document.getElementById('csv-filename').textContent = file.name;

    const reader = new FileReader();
    reader.onload = (e) => {
        const text = e.target.result;
        state.csvRows = parseCsv(text);
        renderCsvPreview(state.csvRows);
        document.getElementById('btn-confirm-import').disabled = state.csvRows.length === 0;
    };
    reader.readAsText(file);
}

function parseCsv(text) {
    const lines = text.trim().split('\n');
    if (lines.length < 2) return [];
    const headers = lines[0].split(',').map(h => h.trim().replace(/^"|"$/g, ''));
    return lines.slice(1).map(line => {
        const vals = line.split(',').map(v => v.trim().replace(/^"|"$/g, ''));
        const obj = {};
        headers.forEach((h, i) => { obj[h] = vals[i] ?? ''; });
        return obj;
    });
}

function renderCsvPreview(rows) {
    if (!rows.length) return;
    const preview = document.getElementById('csv-preview');
    const headers = Object.keys(rows[0]);
    const previewRows = rows.slice(0, 5);

    let html = '<table><thead><tr>' + headers.map(h => `<th>${h}</th>`).join('') + '</tr></thead><tbody>';
    for (const row of previewRows) {
        html += '<tr>' + headers.map(h => `<td>${row[h] ?? ''}</td>`).join('') + '</tr>';
    }
    if (rows.length > 5) html += `<tr><td colspan="${headers.length}" style="text-align:center;color:var(--text-muted)">…and ${rows.length - 5} more rows</td></tr>`;
    html += '</tbody></table>';
    preview.innerHTML = html;
    preview.classList.remove('hidden');
}

document.getElementById('btn-confirm-import').addEventListener('click', async () => {
    if (!state.current || !state.csvRows.length) return;
    const replaceAll = document.getElementById('csv-replace-all').checked;
    try {
        const result = await api.post(`/api/datasets/${state.current.dataset.id}/rows/import`, {
            rows: state.csvRows,
            replace_all: replaceAll,
        });
        closeAllModals();
        toast(`Imported ${result.imported} rows`, 'success');
        // Refresh
        const data = await api.get(`/api/datasets/${state.current.dataset.id}`);
        state.current = data;
        renderGrid(data.dataset, data.rows);
        document.getElementById('ds-row-count').textContent = `${data.rows.length} rows`;
    } catch (err) {
        toast(`Import failed: ${err.message}`, 'error');
    }
});

// =========================================================================
// Publish
// =========================================================================

document.getElementById('btn-publish').addEventListener('click', async () => {
    if (!state.current) return;
    const btn = document.getElementById('btn-publish');
    btn.disabled = true;
    btn.textContent = 'Publishing…';
    try {
        const result = await api.post(`/api/datasets/${state.current.dataset.id}/publish`);
        if (result.atlan) {
            toast(`✓ Published to Atlan — ${result.synced_rows} rows synced`, 'success');
        } else {
            toast(`Published locally${result.warning ? ' (Atlan: ' + result.warning + ')' : ''}`, 'info');
        }
        // Refresh dataset state
        const data = await api.get(`/api/datasets/${state.current.dataset.id}`);
        state.current = data;
        renderDatasetView(data);
        await loadNav();
    } catch (err) {
        toast(`Publish failed: ${err.message}`, 'error');
    } finally {
        btn.disabled = false;
    }
});

// =========================================================================
// Copy buttons
// =========================================================================

document.querySelectorAll('.btn-copy').forEach(btn => {
    btn.addEventListener('click', () => {
        const targetId = btn.dataset.target;
        const text = document.getElementById(targetId)?.textContent || '';
        navigator.clipboard.writeText(text).then(() => {
            btn.textContent = 'Copied!';
            btn.classList.add('copied');
            setTimeout(() => { btn.textContent = 'Copy'; btn.classList.remove('copied'); }, 2000);
        });
    });
});

// =========================================================================
// Nav search
// =========================================================================

document.getElementById('nav-search').addEventListener('input', (e) => {
    renderNav(state.groups, e.target.value);
});

// =========================================================================
// Modal wiring
// =========================================================================

function closeAllModals() {
    document.querySelectorAll('.modal-overlay').forEach(m => m.classList.add('hidden'));
}

document.querySelectorAll('.modal-close, [data-modal]').forEach(el => {
    el.addEventListener('click', () => {
        const modalId = el.dataset.modal || el.closest('.modal-overlay')?.id;
        if (modalId) document.getElementById(modalId)?.classList.add('hidden');
    });
});

document.querySelectorAll('.modal-overlay').forEach(overlay => {
    overlay.addEventListener('click', (e) => {
        if (e.target === overlay) overlay.classList.add('hidden');
    });
});

// New dataset
document.getElementById('btn-new-dataset').addEventListener('click', openNewDatasetModal);
document.getElementById('btn-new-dataset-empty').addEventListener('click', openNewDatasetModal);

// Add column
document.getElementById('btn-add-column').addEventListener('click', openAddColumnModal);

// Add row
document.getElementById('btn-add-row').addEventListener('click', () => {
    if (state.current) addNewRowToGrid(state.current.dataset);
});

// Import CSV
document.getElementById('btn-import-csv').addEventListener('click', openImportModal);

// Tab switching
document.querySelectorAll('.tab-btn').forEach(btn => {
    btn.addEventListener('click', () => switchTab(btn.dataset.tab));
});

// Reseed
document.getElementById('btn-reseed').addEventListener('click', async () => {
    if (!confirm('Reset all data to demo state?')) return;
    try {
        await api.post('/api/demo/reseed', {});
        state.current = null;
        document.getElementById('empty-state').classList.remove('hidden');
        document.getElementById('dataset-view').classList.add('hidden');
        await loadNav();
        toast('Demo data reset', 'info');
    } catch (err) {
        toast('Reset failed: ' + err.message, 'error');
    }
});

// =========================================================================
// Boot
// =========================================================================

async function init() {
    await loadNav();
    // Auto-select first dataset if any
    const allDatasets = await api.get('/api/datasets');
    if (allDatasets.length > 0) {
        await selectDataset(allDatasets[0].id);
    }
}

init().catch(err => console.error('RDM init error:', err));
