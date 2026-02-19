/**
 * ContractApp — Contract detail page: stage stepper, schema editor,
 * comments, quality, SLAs, team, review, and approval.
 */
const ContractApp = {
    sessionId: null,
    session: null,
    atlanConfigured: false,
    atlanSearchResults: [],
    atlanCart: [],
    atlanBrowseOpen: false,
    _globalSearchTimer: null,
    _expandedQualityId: null,
    _expandedSLAId: null,
    _expandedServerId: null,
    _expandedRoleId: null,
    _approverSearchTimer: null,
    _pendingApprovers: {},

    STAGES: ['request', 'discovery', 'specification', 'review', 'approval', 'active'],
    STAGE_LABELS: {
        request: 'Request',
        discovery: 'Discovery',
        specification: 'Specification',
        review: 'Review',
        approval: 'Approval',
        active: 'Active',
    },
    ADVANCE_LABELS: {
        request: 'Begin Discovery',
        discovery: 'Start Specification',
        specification: 'Submit for Review',
        review: 'Move to Approval',
        approval: 'Approve Contract',
    },
    LOGICAL_TYPES: ['string', 'integer', 'number', 'boolean', 'date', 'timestamp', 'time', 'array', 'object'],
    CLASSIFICATIONS: ['', 'public', 'internal', 'confidential', 'pii', 'sensitive'],

    // -----------------------------------------------------------------------
    // Init
    // -----------------------------------------------------------------------
    async init() {
        const parts = window.location.pathname.split('/');
        this.sessionId = parts[parts.length - 1];
        // Check Atlan connectivity
        try {
            const status = await DDLC.api.fetchJSON('/api/atlan/status');
            this.atlanConfigured = status.configured;
        } catch { this.atlanConfigured = false; }
        await this.load();
    },

    async load() {
        try {
            this.session = await DDLC.api.fetchJSON(`/api/sessions/${this.sessionId}`);
            document.getElementById('headerTitle').textContent = this.session.request.title;
            this.renderStepper();
            this.renderSidebar();
            this.renderMain();
            // Re-render cart drawer if items exist (DOM was rebuilt by renderMain)
            if (this.atlanCart.length > 0) {
                this.renderCartDrawer();
            }
            // Re-open browse panel if it was open
            if (this.atlanBrowseOpen) {
                this.atlanBrowseOpen = false; // reset so toggleAtlanBrowse opens it
                this.toggleAtlanBrowse();
            }
            // Show YAML toggle from specification onward
            const idx = this.STAGES.indexOf(this.session.current_stage);
            document.getElementById('yamlToggle').style.display = idx >= 2 ? '' : 'none';
            document.getElementById('dbtToggle').style.display = idx >= 2 ? '' : 'none';
        } catch (err) {
            document.getElementById('mainContent').innerHTML = `
                <div class="empty-state"><h3>Session not found</h3><a href="/" class="btn">Back to Dashboard</a></div>
            `;
        }
    },

    // -----------------------------------------------------------------------
    // Stage stepper
    // -----------------------------------------------------------------------
    renderStepper() {
        const el = document.getElementById('stepper');
        const currentIdx = this.STAGES.indexOf(this.session.current_stage);
        const isRejected = this.session.current_stage === 'rejected';

        el.innerHTML = this.STAGES.map((stage, i) => {
            let cls = '';
            if (isRejected) cls = '';
            else if (i < currentIdx) cls = 'completed';
            else if (i === currentIdx) cls = 'current';

            const connector = i < this.STAGES.length - 1
                ? `<div class="stage-connector ${i < currentIdx ? 'completed' : ''}"></div>`
                : '';

            return `
                <div class="stage-step ${cls}">
                    <div class="step-circle">${i < currentIdx ? '&#10003;' : i + 1}</div>
                    <span class="step-label">${this.STAGE_LABELS[stage]}</span>
                </div>
                ${connector}
            `;
        }).join('');
    },

    // -----------------------------------------------------------------------
    // Sidebar
    // -----------------------------------------------------------------------
    renderSidebar() {
        const s = this.session;
        const el = document.getElementById('sidebar');

        const historyHtml = s.history.length > 0
            ? s.history.slice().reverse().map(h => `
                <div class="timeline-item">
                    <div class="timeline-dot"></div>
                    <div class="timeline-text">
                        <strong>${h.from_stage}</strong> &rarr; <strong>${h.to_stage}</strong>
                        <br>${this.timeAgo(h.timestamp)}
                    </div>
                </div>
            `).join('')
            : '<div style="font-size:0.78rem; color:var(--text-dim);">No transitions yet</div>';

        el.innerHTML = `
            <div class="sidebar-section">
                <h4>Contract Info</h4>
                <dl class="sidebar-meta">
                    <dt>Status</dt>
                    <dd><span class="stage-badge ${s.current_stage}">${s.current_stage}</span></dd>
                    <dt>Domain</dt>
                    <dd>${this.esc(s.contract.domain || 'Not set')}</dd>
                    <dt>Data Product</dt>
                    <dd>${this.esc(s.contract.data_product || 'Not set')}</dd>
                    <dt>Version</dt>
                    <dd>${this.esc(s.contract.version)}</dd>
                    <dt>Urgency</dt>
                    <dd><span class="urgency-badge ${s.request.urgency}">${s.request.urgency}</span></dd>
                </dl>
            </div>
            <div class="sidebar-section">
                <h4>Participants</h4>
                ${s.participants.map(p => `
                    <div style="font-size:0.82rem; padding:4px 0;">
                        ${this.esc(p.name)}<br>
                        <span style="color:var(--text-dim); font-size:0.75rem;">${this.esc(p.email)}</span>
                    </div>
                `).join('')}
            </div>
            <div class="sidebar-section">
                <h4>Stage History</h4>
                ${historyHtml}
            </div>
            ${this.renderStageActions()}
        `;
    },

    renderStageActions() {
        const stage = this.session.current_stage;
        if (stage === 'active' || stage === 'rejected') return '';

        const label = this.ADVANCE_LABELS[stage];
        if (!label) return '';

        const nextIdx = this.STAGES.indexOf(stage) + 1;
        const nextStage = this.STAGES[nextIdx];

        return `
            <div class="sidebar-section" style="text-align:center;">
                <button class="btn btn-primary" style="width:100%;" onclick="ContractApp.advanceStage('${nextStage}')">
                    ${label} &rarr;
                </button>
                ${stage !== 'request' ? `
                    <button class="btn btn-danger btn-sm" style="width:100%; margin-top:8px;" onclick="ContractApp.advanceStage('rejected')">
                        Reject
                    </button>
                ` : ''}
            </div>
        `;
    },

    // -----------------------------------------------------------------------
    // Main content (stage-specific)
    // -----------------------------------------------------------------------
    renderMain() {
        const el = document.getElementById('mainContent');
        const stage = this.session.current_stage;

        switch (stage) {
            case 'request':
                el.innerHTML = this.renderRequestStage();
                break;
            case 'discovery':
                el.innerHTML = this.renderDiscoveryStage();
                break;
            case 'specification':
                el.innerHTML = this.renderSpecificationStage();
                break;
            case 'review':
                el.innerHTML = this.renderReviewStage();
                break;
            case 'approval':
                el.innerHTML = this.renderApprovalStage();
                break;
            case 'active':
                el.innerHTML = this.renderActiveStage();
                break;
            case 'rejected':
                el.innerHTML = this.renderRejectedStage();
                break;
        }
    },

    // --- Request stage ---
    renderRequestStage() {
        const r = this.session.request;
        return `
            <div class="section-panel">
                <div class="section-header"><h3>Data Asset Request</h3></div>
                <div class="section-body">
                    <div class="request-detail">
                        <div class="request-field">
                            <div class="field-label">Description</div>
                            <div class="field-value">${this.esc(r.description)}</div>
                        </div>
                        ${r.business_context ? `
                            <div class="request-field">
                                <div class="field-label">Business Context</div>
                                <div class="field-value">${this.esc(r.business_context)}</div>
                            </div>
                        ` : ''}
                        ${r.target_use_case ? `
                            <div class="request-field">
                                <div class="field-label">Target Use Case</div>
                                <div class="field-value">${this.esc(r.target_use_case)}</div>
                            </div>
                        ` : ''}
                        ${r.desired_fields && r.desired_fields.length ? `
                            <div class="request-field">
                                <div class="field-label">Desired Fields</div>
                                <div class="field-value">${r.desired_fields.map(f => `<code style="background:var(--bg-input); padding:2px 6px; border-radius:4px; margin-right:4px; font-size:0.82rem;">${this.esc(f)}</code>`).join(' ')}</div>
                            </div>
                        ` : ''}
                        <div class="request-field">
                            <div class="field-label">Requested By</div>
                            <div class="field-value">${this.esc(r.requester.name)} (${this.esc(r.requester.email)})</div>
                        </div>
                    </div>
                </div>
            </div>
            ${this.renderComments()}
        `;
    },

    // --- Discovery stage ---
    renderDiscoveryStage() {
        return `
            <div class="section-panel">
                <div class="section-header"><h3>Discovery</h3></div>
                <div class="section-body">
                    <p style="color:var(--text-muted); font-size:0.85rem; margin-bottom:16px;">
                        Discuss the request with the data producer. Ask clarifying questions,
                        share context about existing assets, and align on scope.
                    </p>
                    ${this.renderRequestSummary()}
                </div>
            </div>
            ${this.renderComments()}
        `;
    },

    renderRequestSummary() {
        const r = this.session.request;
        return `
            <div style="padding:12px 16px; background:var(--bg-input); border-radius:var(--radius); margin-bottom:12px;">
                <div style="font-size:0.72rem; color:var(--text-dim); text-transform:uppercase; margin-bottom:4px;">Original Request</div>
                <div style="font-size:0.85rem; color:var(--text);">${this.esc(r.description)}</div>
                ${r.desired_fields && r.desired_fields.length ? `
                    <div style="margin-top:8px; font-size:0.78rem; color:var(--text-muted);">
                        Desired fields: ${r.desired_fields.map(f => `<code style="background:var(--bg); padding:1px 4px; border-radius:3px;">${this.esc(f)}</code>`).join(', ')}
                    </div>
                ` : ''}
            </div>
        `;
    },

    // --- Specification stage ---
    renderSpecificationStage() {
        return `
            ${this.renderMetadataSection()}
            ${this.renderSchemaSection()}
            ${this.renderQualitySection()}
            ${this.renderSLASection()}
            ${this.renderServersSection()}
            ${this.renderRolesSection()}
            ${this.renderCustomPropertiesSection()}
            ${this.renderTeamSection()}
            ${this.renderComments()}
        `;
    },

    renderMetadataSection() {
        const c = this.session.contract;
        return `
            <div class="section-panel">
                <div class="section-header">
                    <h3>Contract Metadata</h3>
                    <button class="btn btn-sm" onclick="ContractApp.toggleMetadataEdit()">Edit</button>
                </div>
                <div class="section-body" id="metadataBody">
                    <div style="display:grid; grid-template-columns:1fr 1fr; gap:12px;">
                        <div class="request-field"><div class="field-label">Name</div><div class="field-value">${this.esc(c.name || 'Not set')}</div></div>
                        <div class="request-field"><div class="field-label">Version</div><div class="field-value">${this.esc(c.version)}</div></div>
                        <div class="request-field"><div class="field-label">Domain</div><div class="field-value">${this.esc(c.domain || 'Not set')}</div></div>
                        <div class="request-field"><div class="field-label">Data Product</div><div class="field-value">${this.esc(c.data_product || 'Not set')}</div></div>
                    </div>
                    ${c.description_purpose ? `<div class="request-field" style="margin-top:12px;"><div class="field-label">Purpose</div><div class="field-value">${this.esc(c.description_purpose)}</div></div>` : ''}
                    ${c.description_limitations ? `<div class="request-field" style="margin-top:12px;"><div class="field-label">Limitations</div><div class="field-value">${this.esc(c.description_limitations)}</div></div>` : ''}
                    ${c.description_usage ? `<div class="request-field" style="margin-top:12px;"><div class="field-label">Usage</div><div class="field-value">${this.esc(c.description_usage)}</div></div>` : ''}
                    ${c.tags.length ? `<div class="request-field" style="margin-top:12px;"><div class="field-label">Tags</div><div class="field-value">${c.tags.map(t => `<span class="col-badge" style="background:var(--bg-input);">${this.esc(t)}</span>`).join(' ')}</div></div>` : ''}
                </div>
            </div>
        `;
    },

    toggleMetadataEdit() {
        const body = document.getElementById('metadataBody');
        const c = this.session.contract;

        // Check if already in edit mode
        if (body.querySelector('.inline-form')) {
            this.renderMain();
            return;
        }

        body.innerHTML = `
            <div class="inline-form">
                <div class="inline-form-row">
                    <div><label>Name</label><input id="meta_name" value="${this.esc(c.name || '')}"></div>
                    <div><label>Version</label><input id="meta_version" value="${this.esc(c.version)}"></div>
                </div>
                <div class="inline-form-row">
                    <div><label>Domain</label><input id="meta_domain" value="${this.esc(c.domain || '')}"></div>
                    <div><label>Data Product</label><input id="meta_data_product" value="${this.esc(c.data_product || '')}"></div>
                </div>
                <div class="inline-form-row">
                    <div><label>Tenant</label><input id="meta_tenant" value="${this.esc(c.tenant || '')}"></div>
                    <div><label>Tags (comma-separated)</label><input id="meta_tags" value="${(c.tags || []).join(', ')}"></div>
                </div>
                <div><label>Purpose</label><textarea id="meta_purpose" rows="2">${this.esc(c.description_purpose || '')}</textarea></div>
                <div><label>Limitations</label><textarea id="meta_limitations" rows="2">${this.esc(c.description_limitations || '')}</textarea></div>
                <div><label>Usage</label><textarea id="meta_usage" rows="2">${this.esc(c.description_usage || '')}</textarea></div>
                <div class="form-actions">
                    <button class="btn btn-sm" onclick="ContractApp.renderMain()">Cancel</button>
                    <button class="btn btn-primary btn-sm" onclick="ContractApp.saveMetadata()">Save</button>
                </div>
            </div>
        `;
    },

    async saveMetadata() {
        try {
            const tags = document.getElementById('meta_tags').value
                .split(',').map(t => t.trim()).filter(Boolean);

            await DDLC.api.put(`/api/sessions/${this.sessionId}/contract/metadata`, {
                name: document.getElementById('meta_name').value,
                version: document.getElementById('meta_version').value,
                domain: document.getElementById('meta_domain').value,
                data_product: document.getElementById('meta_data_product').value,
                tenant: document.getElementById('meta_tenant').value,
                tags: tags,
                description_purpose: document.getElementById('meta_purpose').value,
                description_limitations: document.getElementById('meta_limitations').value,
                description_usage: document.getElementById('meta_usage').value,
            });
            DDLC.toast.show('Metadata saved');
            await this.load();
        } catch (err) {
            DDLC.toast.show(err.message, 'error');
        }
    },

    // --- Schema editor ---
    // Track which target table is selected and which source tables are expanded
    _selectedTable: null,
    _expandedSources: {},
    _selectedSourceCols: {},  // { "tableName": Set of column names }

    renderSchemaSection() {
        const objects = this.session.contract.schema_objects || [];
        const cartCount = this.atlanCart.length;
        const selectedObj = objects.length > 0
            ? objects.find(o => o.name === this._selectedTable) || objects[0]
            : null;

        return `
            <div class="section-panel">
                <div class="section-header">
                    <h3>Source-to-Target Mapping</h3>
                    <div style="display:flex; gap:8px;">
                        <button class="btn btn-sm btn-primary" onclick="ContractApp.toggleAtlanBrowse()">Browse Atlan${cartCount > 0 ? `<span class="cart-badge">${cartCount}</span>` : ''}</button>
                        <button class="btn btn-sm" onclick="ContractApp.showAddTable()">+ Add Target Table</button>
                    </div>
                </div>
                <div class="section-body" id="schemaBody">
                    <div id="atlanBrowsePanel"></div>
                    <div id="atlanCartDrawer"></div>
                    <div id="addTableForm"></div>
                    ${objects.length > 0 ? this.renderTableTabs(objects, selectedObj ? selectedObj.name : null) : ''}
                    ${selectedObj ? this.renderMappingLayout(selectedObj) : this.renderEmptySchema()}
                </div>
            </div>
        `;
    },

    renderEmptySchema() {
        return `
            <div class="mapping-empty">
                <div class="mapping-empty-icon">&#128203;</div>
                <div style="font-weight:500; margin-bottom:4px;">No target tables defined yet</div>
                <div style="font-size:0.82rem; color:var(--text-dim); max-width:400px; margin:0 auto;">
                    Click <strong>+ Add Target Table</strong> to define your new data asset, then add source tables from Atlan to map columns.
                </div>
            </div>
        `;
    },

    renderTableTabs(objects, selectedName) {
        return `
            <div class="mapping-tabs">
                ${objects.map(obj => `
                    <button class="mapping-tab ${obj.name === selectedName ? 'active' : ''}"
                        onclick="ContractApp.selectTargetTable('${this.esc(obj.name)}')">
                        ${this.esc(obj.name)}
                        <span style="font-size:0.7rem; color:var(--text-dim); margin-left:4px;">(${(obj.properties || []).length} cols)</span>
                    </button>
                `).join('')}
                <button class="mapping-tab-add" onclick="ContractApp.showAddTable()" title="Add another target table">+</button>
            </div>
        `;
    },

    selectTargetTable(name) {
        this._selectedTable = name;
        this.renderMain();
    },

    renderMappingLayout(obj) {
        return `
            <div style="display:flex; align-items:center; justify-content:space-between; padding:4px 0 8px; margin-bottom:4px;">
                <div style="display:flex; align-items:center; gap:8px;">
                    <span style="font-size:0.82rem; color:var(--text-dim);">Target:</span>
                    <strong style="font-family:var(--font-mono); color:var(--accent);">${this.esc(obj.name)}</strong>
                    ${obj.physical_name ? `<span style="font-size:0.75rem; color:var(--text-dim);">(${this.esc(obj.physical_name)})</span>` : ''}
                </div>
                <button class="btn-icon" onclick="ContractApp.deleteTable('${this.esc(obj.name)}')" title="Delete target table" style="color:var(--danger); font-size:0.75rem;">&#128465; Delete</button>
            </div>
            ${obj.description ? `<div style="font-size:0.8rem; color:var(--text-muted); margin-bottom:8px;">${this.esc(obj.description)}</div>` : ''}
            <div class="mapping-layout">
                <div class="mapping-sources">
                    ${this.renderSourcesPanel(obj)}
                </div>
                <div class="mapping-target">
                    ${this.renderTargetPanel(obj)}
                </div>
            </div>
        `;
    },

    renderSourcesPanel(obj) {
        const sources = obj.source_tables || [];
        const totalSelected = Object.values(this._selectedSourceCols).reduce((sum, s) => sum + (s ? s.size : 0), 0);

        return `
            <div class="mapping-panel-header">
                <span class="panel-label">&#128230; Sources (${sources.length})</span>
                <button class="btn btn-sm" style="font-size:0.72rem; padding:2px 8px;" onclick="ContractApp.showAtlanSearch('${this.esc(obj.name)}')">+ Add Source</button>
            </div>
            <div class="mapping-panel-body">
                <div id="atlanSearch-${obj.name}"></div>
                ${sources.length === 0 ? `
                    <div style="padding:20px; text-align:center; color:var(--text-dim); font-size:0.82rem;">
                        No source tables yet.<br>
                        <span style="font-size:0.78rem;">Use <strong>Browse Atlan</strong> or <strong>+ Add Source</strong> to add source tables.</span>
                    </div>
                ` : sources.map((s, i) => this.renderSourceTableCard(obj.name, s, i)).join('')}
            </div>
            ${totalSelected > 0 ? `
                <div class="mapping-action-bar">
                    <span class="selected-count">${totalSelected} column${totalSelected !== 1 ? 's' : ''} selected</span>
                    <button class="btn btn-sm btn-primary" style="font-size:0.78rem;" onclick="ContractApp.mapSelectedColumns('${this.esc(obj.name)}')">Map to Target &#8594;</button>
                </div>
            ` : ''}
        `;
    },

    renderSourceTableCard(objName, source, idx) {
        const expanded = this._expandedSources[source.name] !== false; // default open
        const cols = source.columns || [];
        const selectedSet = this._selectedSourceCols[source.name] || new Set();

        return `
            <div class="source-table-card">
                <div class="source-table-header" onclick="ContractApp.toggleSourceExpand('${this.esc(source.name)}')">
                    <div style="display:flex; align-items:center; gap:6px;">
                        <span class="source-table-toggle ${expanded ? 'open' : ''}">&#9654;</span>
                        <span class="source-table-name">${this.esc(source.name)}</span>
                        <span class="source-table-meta">${this.esc(source.database_name || '')}.${this.esc(source.schema_name || '')}</span>
                        ${source.connector_name ? `<span class="col-badge" style="background:rgba(99,102,241,0.1); color:var(--accent); font-size:0.6rem;">${this.esc(source.connector_name)}</span>` : ''}
                    </div>
                    <div style="display:flex; align-items:center; gap:4px;">
                        <span style="font-size:0.7rem; color:var(--text-dim);">${cols.length} cols</span>
                        <button class="btn-icon" onclick="event.stopPropagation(); ContractApp.importColumnsFromSource('${this.esc(objName)}', '${this.esc(source.qualified_name || '')}', '${this.esc(source.name)}')" title="Import all columns to target" style="font-size:0.68rem; color:var(--info);">Import All</button>
                        <button class="btn-icon" onclick="event.stopPropagation(); ContractApp.deleteSourceTable('${this.esc(objName)}', ${idx})" title="Remove source">&#128465;</button>
                    </div>
                </div>
                ${expanded && cols.length > 0 ? `
                    <div class="source-column-list">
                        <div style="display:flex; align-items:center; justify-content:space-between; margin-bottom:4px; padding:0 8px;">
                            <label style="font-size:0.7rem; color:var(--text-dim); cursor:pointer;">
                                <input type="checkbox" style="accent-color:var(--accent);" onchange="ContractApp.toggleAllSourceCols('${this.esc(source.name)}', this.checked)"
                                    ${selectedSet.size === cols.length && cols.length > 0 ? 'checked' : ''}> Select all
                            </label>
                        </div>
                        ${cols.map(c => `
                            <div class="source-column-item">
                                <input type="checkbox"
                                    ${selectedSet.has(c.name) ? 'checked' : ''}
                                    onchange="ContractApp.toggleSourceCol('${this.esc(source.name)}', '${this.esc(c.name)}', this.checked)">
                                <span class="source-column-name">${this.esc(c.name)}</span>
                                <span class="source-column-type">${this.esc(c.data_type || c.logical_type || '')}</span>
                                ${c.is_primary ? '<span class="source-column-pk">PK</span>' : ''}
                            </div>
                        `).join('')}
                    </div>
                ` : (expanded && cols.length === 0 ? `
                    <div style="padding:8px 16px; font-size:0.78rem; color:var(--text-dim);">
                        No column data available.
                        <button class="btn-icon" style="color:var(--info); font-size:0.75rem;" onclick="ContractApp.fetchSourceColumns('${this.esc(objName)}', '${this.esc(source.name)}')">Fetch columns</button>
                    </div>
                ` : '')}
            </div>
        `;
    },

    toggleSourceExpand(sourceName) {
        this._expandedSources[sourceName] = this._expandedSources[sourceName] === false;
        this.renderMain();
    },

    toggleSourceCol(sourceName, colName, checked) {
        if (!this._selectedSourceCols[sourceName]) {
            this._selectedSourceCols[sourceName] = new Set();
        }
        if (checked) {
            this._selectedSourceCols[sourceName].add(colName);
        } else {
            this._selectedSourceCols[sourceName].delete(colName);
        }
        this.renderMain();
    },

    toggleAllSourceCols(sourceName, checked) {
        const obj = (this.session.contract.schema_objects || []).find(o => o.name === (this._selectedTable || (this.session.contract.schema_objects[0] || {}).name));
        if (!obj) return;
        const source = (obj.source_tables || []).find(s => s.name === sourceName);
        if (!source || !source.columns) return;

        if (checked) {
            this._selectedSourceCols[sourceName] = new Set(source.columns.map(c => c.name));
        } else {
            this._selectedSourceCols[sourceName] = new Set();
        }
        this.renderMain();
    },

    async fetchSourceColumns(objName, sourceName) {
        try {
            const result = await DDLC.api.fetchJSON(`/api/sessions/${this.sessionId}/contract/objects/${objName}/source-columns`);
            // Update the session data with cached columns
            const obj = (this.session.contract.schema_objects || []).find(o => o.name === objName);
            if (obj) {
                for (const src of obj.source_tables) {
                    if (result[src.name]) {
                        src.columns = result[src.name];
                    }
                }
            }
            this.renderMain();
        } catch (err) {
            DDLC.toast.show(`Error fetching columns: ${err.message}`, 'error');
        }
    },

    async mapSelectedColumns(objName) {
        const mappings = [];
        const obj = (this.session.contract.schema_objects || []).find(o => o.name === objName);
        if (!obj) return;

        for (const source of obj.source_tables || []) {
            const selected = this._selectedSourceCols[source.name];
            if (!selected || selected.size === 0) continue;

            const cols = source.columns || [];
            for (const col of cols) {
                if (selected.has(col.name)) {
                    mappings.push({
                        source_table: source.name,
                        source_column: col.name,
                        source_table_qualified_name: source.qualified_name || null,
                        target_column_name: col.name,
                        logical_type: col.logical_type || 'string',
                        is_primary: col.is_primary || false,
                        description: col.description || null,
                        transform_logic: null,
                    });
                }
            }
        }

        if (mappings.length === 0) return DDLC.toast.show('No columns selected', 'error');

        try {
            const result = await DDLC.api.post(
                `/api/sessions/${this.sessionId}/contract/objects/${objName}/map-columns`,
                { mappings }
            );
            let msg = `Mapped ${result.added} column${result.added !== 1 ? 's' : ''} to target`;
            if (result.skipped > 0) msg += ` (${result.skipped} already existed)`;
            DDLC.toast.show(msg);
            // Clear selections
            this._selectedSourceCols = {};
            await this.load();
        } catch (err) {
            DDLC.toast.show(err.message, 'error');
        }
    },

    renderTargetPanel(obj) {
        const props = obj.properties || [];
        const total = props.length;

        return `
            <div class="mapping-panel-header">
                <span class="panel-label">&#127919; Target (${total} columns)</span>
            </div>
            <div class="mapping-panel-body">
                ${total > 0 ? `
                    <table class="target-columns-table">
                        <thead><tr>
                            <th style="width:30px;"></th><th>Column</th><th>Type</th><th>Source</th><th style="width:36px;"></th>
                        </tr></thead>
                        <tbody>
                            ${props.map((p, i) => this.renderTargetPropertyRow(obj.name, p, i, total)).join('')}
                        </tbody>
                    </table>
                ` : `
                    <div style="padding:20px; text-align:center; color:var(--text-dim); font-size:0.82rem;">
                        No target columns yet.<br>
                        <span style="font-size:0.78rem;">Select source columns and click <strong>Map to Target</strong>, or add columns manually below.</span>
                    </div>
                `}
                <div style="padding:8px 12px; display:flex; gap:8px;" id="addCol-${obj.name}">
                    <button class="btn btn-sm" onclick="ContractApp.showAddColumn('${this.esc(obj.name)}')">+ Add Column Manually</button>
                </div>
            </div>
        `;
    },

    renderTargetPropertyRow(objName, prop, index, totalCount) {
        const badges = [];
        if (prop.primary_key) badges.push('<span class="col-badge pk">PK</span>');
        if (prop.required) badges.push('<span class="col-badge req">REQ</span>');
        if (prop.unique) badges.push('<span class="col-badge uniq">UNQ</span>');

        const sources = prop.sources || [];
        let sourceHtml;
        if (sources.length > 0) {
            sourceHtml = sources.map(s => {
                let html = `<span class="target-col-source" title="${this.esc(s.transform_logic || '')}">${this.esc(s.source_table)}.${this.esc(s.source_column)}</span>`;
                if (s.transform_logic) {
                    html += `<br><span class="target-col-transform" title="${this.esc(s.transform_logic)}">${this.esc(s.transform_logic.substring(0, 40))}${s.transform_logic.length > 40 ? '...' : ''}</span>`;
                }
                return html;
            }).join(' ');
        } else {
            sourceHtml = `<span style="font-size:0.7rem; color:var(--text-dim);">+ map source</span>`;
        }

        const escName = this.esc(prop.name);
        const escObj = this.esc(objName);

        return `
            <tr onclick="ContractApp.openColumnEditor('${escObj}', '${escName}')" title="Click to edit column">
                <td onclick="event.stopPropagation();" style="padding:4px 2px; text-align:center;">
                    <div class="reorder-arrows">
                        <button onclick="ContractApp.moveColumn('${escObj}', '${escName}', 'up')"
                                ${index === 0 ? 'disabled' : ''} title="Move up">&#9650;</button>
                        <button onclick="ContractApp.moveColumn('${escObj}', '${escName}', 'down')"
                                ${index === totalCount - 1 ? 'disabled' : ''} title="Move down">&#9660;</button>
                    </div>
                </td>
                <td>
                    <span class="target-col-name">${escName}</span>
                    ${badges.join('')}
                    ${prop.classification ? `<span class="col-badge pii">${this.esc(prop.classification)}</span>` : ''}
                </td>
                <td class="col-type">${prop.logical_type}</td>
                <td style="max-width:200px; overflow:hidden;">${sourceHtml}</td>
                <td onclick="event.stopPropagation();">
                    <button class="btn-icon" onclick="ContractApp.deleteProperty('${escObj}', '${escName}')" title="Delete">&#128465;</button>
                </td>
            </tr>
        `;
    },

    // Keep old renderTableCard for backward compatibility (used by other stages if needed)
    renderTableCard(obj) {
        const props = obj.properties || [];
        const sources = obj.source_tables || [];
        return `
            <div class="table-card">
                <div class="table-card-header" onclick="ContractApp.toggleTable('${obj.name}')">
                    <h4>
                        <span style="color:var(--accent);">&#9638;</span>
                        ${this.esc(obj.name)}
                        ${obj.physical_name ? `<span style="color:var(--text-dim); font-weight:400; font-size:0.75rem;">(${this.esc(obj.physical_name)})</span>` : ''}
                    </h4>
                    <div style="display:flex; align-items:center; gap:8px;">
                        <span class="table-meta">${props.length} column${props.length !== 1 ? 's' : ''}</span>
                        ${sources.length > 0 ? `<span class="table-meta" style="color:var(--info);">${sources.length} source${sources.length !== 1 ? 's' : ''}</span>` : ''}
                        <button class="btn-icon" onclick="event.stopPropagation(); ContractApp.deleteTable('${obj.name}')" title="Delete table">&#128465;</button>
                    </div>
                </div>
                <div class="table-card-body" id="table-${obj.name}" style="display:block;">
                    ${obj.description ? `<div style="padding:8px 16px; font-size:0.8rem; color:var(--text-muted); border-bottom:1px solid var(--border);">${this.esc(obj.description)}</div>` : ''}
                    ${this.renderSourceTables(obj)}
                    ${props.length > 0 ? `
                        <table class="columns-table">
                            <thead><tr>
                                <th>Column</th><th>Type</th><th>Constraints</th><th>Classification</th><th>Source</th><th></th>
                            </tr></thead>
                            <tbody>
                                ${props.map(p => this.renderPropertyRow(obj.name, p)).join('')}
                            </tbody>
                        </table>
                    ` : ''}
                    <div class="add-row" id="addCol-${obj.name}">
                        <button class="btn btn-sm" onclick="ContractApp.showAddColumn('${obj.name}')">+ Add Column</button>
                        <button class="btn btn-sm" style="margin-left:8px; border-color:var(--info); color:var(--info);" onclick="ContractApp.showAtlanSearch('${obj.name}')">Link Source Table</button>
                    </div>
                </div>
            </div>
        `;
    },

    renderPropertyRow(objName, prop) {
        const badges = [];
        if (prop.primary_key) badges.push('<span class="col-badge pk">PK</span>');
        if (prop.required) badges.push('<span class="col-badge req">REQ</span>');
        if (prop.unique) badges.push('<span class="col-badge uniq">UNQ</span>');
        const classBadge = prop.classification
            ? `<span class="col-badge pii">${this.esc(prop.classification)}</span>`
            : '<span style="color:var(--text-dim); font-size:0.75rem;">—</span>';

        const sources = prop.sources || [];
        let sourceHtml;
        if (sources.length > 0) {
            sourceHtml = sources.map(s =>
                `<span style="font-size:0.72rem; background:rgba(59,130,246,0.1); padding:1px 6px; border-radius:3px; color:var(--info);" title="${this.esc(s.transform_logic || '')}">${this.esc(s.source_table)}.${this.esc(s.source_column)}</span>`
            ).join(' ');
        } else {
            sourceHtml = `<button class="btn-icon" style="font-size:0.7rem; color:var(--text-dim);" onclick="ContractApp.showAddLineage('${objName}', '${prop.name}')" title="Add lineage">+ lineage</button>`;
        }

        return `
            <tr>
                <td class="col-name" title="${this.esc(prop.description || '')}">${this.esc(prop.name)}</td>
                <td class="col-type">${prop.logical_type}</td>
                <td>${badges.join('') || '<span style="color:var(--text-dim);">—</span>'}</td>
                <td>${classBadge}</td>
                <td style="font-size:0.78rem; max-width:250px; overflow:hidden;">${sourceHtml}</td>
                <td><button class="btn-icon" onclick="ContractApp.deleteProperty('${objName}', '${prop.name}')" title="Delete">&#128465;</button></td>
            </tr>
        `;
    },

    toggleTable(name) {
        const el = document.getElementById(`table-${name}`);
        if (el) el.style.display = el.style.display === 'none' ? 'block' : 'none';
    },

    showAddTable() {
        const el = document.getElementById('addTableForm');
        el.innerHTML = `
            <div class="inline-form">
                <div style="font-size:0.82rem; font-weight:600; margin-bottom:6px;">Define Target Table</div>
                <div class="inline-form-row">
                    <div><label>Table Name *</label><input id="newTableName" placeholder="e.g., customer_360"></div>
                    <div><label>Physical Name</label><input id="newTablePhysical" placeholder="e.g., analytics.curated.customer_360"></div>
                </div>
                <div><label>Description</label><input id="newTableDesc" placeholder="What is this new data asset?"></div>
                <div class="form-actions">
                    <button class="btn btn-sm" onclick="ContractApp.renderMain()">Cancel</button>
                    <button class="btn btn-primary btn-sm" onclick="ContractApp.addTable()">Create Target Table</button>
                </div>
            </div>
        `;
        document.getElementById('newTableName').focus();
    },

    async addTable() {
        const name = document.getElementById('newTableName').value.trim();
        if (!name) return DDLC.toast.show('Table name is required', 'error');

        try {
            await DDLC.api.post(`/api/sessions/${this.sessionId}/contract/objects`, {
                name,
                physical_name: document.getElementById('newTablePhysical').value.trim(),
                description: document.getElementById('newTableDesc').value.trim(),
            });
            DDLC.toast.show(`Target table "${name}" created`);
            this._selectedTable = name;
            await this.load();
        } catch (err) {
            DDLC.toast.show(err.message, 'error');
        }
    },

    async deleteTable(name) {
        if (!confirm(`Delete target table "${name}" and all its columns?`)) return;
        try {
            await DDLC.api.del(`/api/sessions/${this.sessionId}/contract/objects/${name}`);
            DDLC.toast.show(`Table "${name}" deleted`);
            // Reset selection if we deleted the selected table
            if (this._selectedTable === name) {
                this._selectedTable = null;
            }
            await this.load();
        } catch (err) {
            DDLC.toast.show(err.message, 'error');
        }
    },

    showAddColumn(objName) {
        const el = document.getElementById(`addCol-${objName}`);
        el.innerHTML = `
            <div class="inline-form" style="text-align:left;">
                <div class="inline-form-row">
                    <div><label>Column Name *</label><input id="newColName-${objName}" placeholder="e.g., customer_id"></div>
                    <div><label>Type</label>
                        <select id="newColType-${objName}">
                            ${this.LOGICAL_TYPES.map(t => `<option value="${t}">${t}</option>`).join('')}
                        </select>
                    </div>
                </div>
                <div class="inline-form-row">
                    <div><label>Description</label><input id="newColDesc-${objName}" placeholder="What does this column represent?"></div>
                    <div><label>Classification</label>
                        <select id="newColClass-${objName}">
                            ${this.CLASSIFICATIONS.map(c => `<option value="${c}">${c || 'None'}</option>`).join('')}
                        </select>
                    </div>
                </div>
                <div class="inline-form-row">
                    <div><label>Examples (comma-separated)</label><input id="newColExamples-${objName}" placeholder="e.g., 12345, 67890"></div>
                    <div></div>
                </div>
                <div class="toggle-group">
                    <label class="toggle"><input type="checkbox" id="newColReq-${objName}"> Required</label>
                    <label class="toggle"><input type="checkbox" id="newColPK-${objName}"> Primary Key</label>
                    <label class="toggle"><input type="checkbox" id="newColUniq-${objName}"> Unique</label>
                    <label class="toggle"><input type="checkbox" id="newColCDE-${objName}"> Critical Data Element</label>
                </div>
                <div class="form-actions">
                    <button class="btn btn-sm" onclick="ContractApp.renderMain()">Cancel</button>
                    <button class="btn btn-primary btn-sm" onclick="ContractApp.addColumn('${objName}')">Add Column</button>
                </div>
            </div>
        `;
        document.getElementById(`newColName-${objName}`).focus();
    },

    async addColumn(objName) {
        const name = document.getElementById(`newColName-${objName}`).value.trim();
        if (!name) return DDLC.toast.show('Column name is required', 'error');

        const examplesRaw = document.getElementById(`newColExamples-${objName}`).value.trim();
        const examples = examplesRaw ? examplesRaw.split(',').map(e => e.trim()).filter(Boolean) : null;

        try {
            await DDLC.api.post(`/api/sessions/${this.sessionId}/contract/objects/${objName}/properties`, {
                name,
                logical_type: document.getElementById(`newColType-${objName}`).value,
                description: document.getElementById(`newColDesc-${objName}`).value.trim(),
                classification: document.getElementById(`newColClass-${objName}`).value || null,
                required: document.getElementById(`newColReq-${objName}`).checked,
                primary_key: document.getElementById(`newColPK-${objName}`).checked,
                unique: document.getElementById(`newColUniq-${objName}`).checked,
                critical_data_element: document.getElementById(`newColCDE-${objName}`).checked,
                examples: examples,
            });
            DDLC.toast.show(`Column "${name}" added`);
            await this.load();
        } catch (err) {
            DDLC.toast.show(err.message, 'error');
        }
    },

    async deleteProperty(objName, propName) {
        try {
            await DDLC.api.del(`/api/sessions/${this.sessionId}/contract/objects/${objName}/properties/${propName}`);
            DDLC.toast.show(`Column "${propName}" deleted`);
            await this.load();
        } catch (err) {
            DDLC.toast.show(err.message, 'error');
        }
    },

    // --- Source tables (Atlan integration) ---
    renderSourceTables(obj) {
        const sources = obj.source_tables || [];
        return `
            <div style="padding:8px 16px; border-bottom:1px solid var(--border);">
                <div style="display:flex; align-items:center; justify-content:space-between; margin-bottom:6px;">
                    <span style="font-size:0.72rem; color:var(--text-dim); text-transform:uppercase; font-weight:600; letter-spacing:0.5px;">Source Tables</span>
                    <button class="btn-icon" style="font-size:0.75rem; color:var(--info);" onclick="event.stopPropagation(); ContractApp.showAtlanSearch('${obj.name}')">+ Search Atlan</button>
                </div>
                <div id="atlanSearch-${obj.name}"></div>
                ${sources.length > 0 ? sources.map((s, i) => `
                    <div style="display:flex; align-items:center; justify-content:space-between; padding:4px 8px; background:rgba(59,130,246,0.05); border:1px solid rgba(59,130,246,0.15); border-radius:var(--radius); margin-bottom:4px;">
                        <div>
                            <span style="font-size:0.82rem; font-weight:500; color:var(--info); font-family:var(--font-mono);">${this.esc(s.name)}</span>
                            ${s.schema_name ? `<span style="font-size:0.7rem; color:var(--text-dim); margin-left:6px;">${this.esc(s.database_name || '')}.${this.esc(s.schema_name)}</span>` : ''}
                            ${s.connector_name ? `<span class="col-badge" style="background:rgba(99,102,241,0.1); color:var(--accent); margin-left:6px;">${this.esc(s.connector_name)}</span>` : ''}
                        </div>
                        <div style="display:flex; gap:4px;">
                            <button class="btn-icon" style="font-size:0.7rem; color:var(--info);" onclick="ContractApp.importColumnsFromSource('${obj.name}', '${this.esc(s.qualified_name || '')}', '${this.esc(s.name)}')" title="Import columns from this source">Import Cols</button>
                            <button class="btn-icon" onclick="ContractApp.deleteSourceTable('${obj.name}', ${i})" title="Remove source">&#128465;</button>
                        </div>
                    </div>
                `).join('') : '<div style="font-size:0.78rem; color:var(--text-dim); padding:4px 0;">No source tables added. Search Atlan to add sources.</div>'}
            </div>
        `;
    },

    showAtlanSearch(objName) {
        const el = document.getElementById(`atlanSearch-${objName}`);
        if (!this.atlanConfigured) {
            el.innerHTML = `
                <div style="margin:8px 0; padding:12px; background:rgba(245,158,11,0.08); border:1px solid rgba(245,158,11,0.2); border-radius:var(--radius); font-size:0.82rem; color:var(--text-muted); display:flex; align-items:center; justify-content:space-between;">
                    <span>&#128268; Atlan not connected. Use <strong>Browse Atlan</strong> above for demo data, or configure credentials.</span>
                    <button class="btn-icon" onclick="document.getElementById('atlanSearch-${objName}').innerHTML = '';" style="font-size:0.75rem;">Close</button>
                </div>
            `;
            return;
        }
        el.innerHTML = `
            <div class="inline-form" style="margin:8px 0;">
                <div style="display:flex; gap:8px;">
                    <input id="atlanQuery-${objName}" placeholder="Search for tables in Atlan..." style="flex:1;" oninput="ContractApp.debounceAtlanSearch('${objName}')">
                    <select id="atlanType-${objName}" style="width:100px;" onchange="ContractApp.debounceAtlanSearch('${objName}')">
                        <option value="Table">Tables</option>
                        <option value="View">Views</option>
                    </select>
                    <button class="btn btn-sm" onclick="document.getElementById('atlanSearch-${objName}').innerHTML = '';">Close</button>
                </div>
                <div id="atlanResults-${objName}" style="max-height:250px; overflow-y:auto;"></div>
            </div>
        `;
        document.getElementById(`atlanQuery-${objName}`).focus();
    },

    _searchTimers: {},
    debounceAtlanSearch(objName) {
        clearTimeout(this._searchTimers[objName]);
        this._searchTimers[objName] = setTimeout(() => this.executeAtlanSearch(objName), 300);
    },

    async executeAtlanSearch(objName) {
        const query = document.getElementById(`atlanQuery-${objName}`).value.trim();
        const assetType = document.getElementById(`atlanType-${objName}`).value;
        const resultsEl = document.getElementById(`atlanResults-${objName}`);

        if (!query) {
            resultsEl.innerHTML = '<div style="font-size:0.78rem; color:var(--text-dim); padding:8px;">Type to search...</div>';
            return;
        }

        resultsEl.innerHTML = '<div style="font-size:0.78rem; color:var(--text-dim); padding:8px;">Searching...</div>';

        try {
            const results = await DDLC.api.fetchJSON(`/api/atlan/search-tables?q=${encodeURIComponent(query)}&asset_type=${assetType}&limit=15`);
            if (results.length === 0) {
                resultsEl.innerHTML = '<div style="font-size:0.78rem; color:var(--text-dim); padding:8px;">No results found.</div>';
                return;
            }
            resultsEl.innerHTML = results.map(r => `
                <div style="display:flex; align-items:center; justify-content:space-between; padding:6px 8px; border-bottom:1px solid var(--border); cursor:pointer;" onmouseover="this.style.background='var(--bg-hover)'" onmouseout="this.style.background=''">
                    <div>
                        <div style="font-size:0.82rem; font-weight:500; font-family:var(--font-mono);">${this.esc(r.name)}</div>
                        <div style="font-size:0.7rem; color:var(--text-dim);">${this.esc(r.database_name)}.${this.esc(r.schema_name)} ${r.connector_name ? '(' + this.esc(r.connector_name) + ')' : ''}</div>
                        ${r.description ? `<div style="font-size:0.7rem; color:var(--text-dim); margin-top:2px;">${this.esc(r.description).substring(0, 80)}</div>` : ''}
                    </div>
                    <button class="btn btn-sm" style="border-color:var(--info); color:var(--info);" onclick="ContractApp.addSourceFromSearch('${objName}', ${JSON.stringify(r).replace(/"/g, '&quot;')}); event.stopPropagation();">Add</button>
                </div>
            `).join('');
        } catch (err) {
            resultsEl.innerHTML = `<div style="font-size:0.78rem; color:var(--danger); padding:8px;">Error: ${this.esc(err.message)}</div>`;
        }
    },

    async addSourceFromSearch(objName, result) {
        try {
            await DDLC.api.post(`/api/sessions/${this.sessionId}/contract/objects/${objName}/sources`, {
                name: result.name,
                qualified_name: result.qualified_name,
                database_name: result.database_name,
                schema_name: result.schema_name,
                connector_name: result.connector_name,
                description: result.description,
            });
            DDLC.toast.show(`Source "${result.name}" added`);
            await this.load();
        } catch (err) {
            DDLC.toast.show(err.message, 'error');
        }
    },

    async deleteSourceTable(objName, idx) {
        try {
            await DDLC.api.del(`/api/sessions/${this.sessionId}/contract/objects/${objName}/sources/${idx}`);
            DDLC.toast.show('Source table removed');
            await this.load();
        } catch (err) {
            DDLC.toast.show(err.message, 'error');
        }
    },

    async importColumnsFromSource(objName, qualifiedName, sourceName) {
        if (!qualifiedName) return DDLC.toast.show('No qualified name for this source', 'error');
        try {
            const result = await DDLC.api.post(`/api/sessions/${this.sessionId}/contract/objects/${objName}/import-from-atlan`, {
                qualified_name: qualifiedName,
                source_name: sourceName,
            });
            DDLC.toast.show(`Imported ${result.imported} columns from "${sourceName}"`);
            await this.load();
        } catch (err) {
            DDLC.toast.show(err.message, 'error');
        }
    },

    // --- Atlan Browse & Cart (top-level schema import) ---

    toggleAtlanBrowse() {
        this.atlanBrowseOpen = !this.atlanBrowseOpen;
        const panel = document.getElementById('atlanBrowsePanel');
        if (!panel) return;

        if (this.atlanBrowseOpen) {
            if (!this.atlanConfigured) {
                // Atlan not connected — show informational panel with demo option
                panel.innerHTML = `
                    <div class="atlan-browse-panel">
                        <div class="atlan-browse-header" style="justify-content:space-between;">
                            <span style="font-weight:600; font-size:0.85rem;">Browse Atlan</span>
                            <button class="btn btn-sm" onclick="ContractApp.toggleAtlanBrowse()">Close</button>
                        </div>
                        <div style="padding:24px; text-align:center;">
                            <div style="font-size:1.5rem; margin-bottom:8px;">&#128268;</div>
                            <div style="font-size:0.88rem; font-weight:500; margin-bottom:6px; color:var(--text);">Atlan Not Connected</div>
                            <div style="font-size:0.82rem; color:var(--text-dim); margin-bottom:16px; max-width:400px; margin-left:auto; margin-right:auto;">
                                Set <code style="background:var(--bg-input); padding:1px 4px; border-radius:3px;">ATLAN_BASE_URL</code> and
                                <code style="background:var(--bg-input); padding:1px 4px; border-radius:3px;">ATLAN_API_KEY</code>
                                environment variables to browse your catalog.
                            </div>
                            <button class="btn btn-sm btn-primary" onclick="ContractApp.loadMockSourceTables()">
                                Use Demo Data
                            </button>
                        </div>
                    </div>
                `;
            } else {
                panel.innerHTML = `
                    <div class="atlan-browse-panel">
                        <div class="atlan-browse-header">
                            <input id="globalAtlanQuery" placeholder="Search Atlan catalog for tables..." oninput="ContractApp.debounceGlobalSearch()">
                            <select id="globalAtlanType" style="width:100px;" onchange="ContractApp.debounceGlobalSearch()">
                                <option value="Table">Tables</option>
                                <option value="View">Views</option>
                            </select>
                            <button class="btn btn-sm" onclick="ContractApp.toggleAtlanBrowse()">Close</button>
                        </div>
                        <div class="atlan-results" id="globalAtlanResults">
                            <div style="font-size:0.82rem; color:var(--text-dim); padding:16px; text-align:center;">Search for tables to add to your schema</div>
                        </div>
                    </div>
                `;
                document.getElementById('globalAtlanQuery').focus();
            }
        } else {
            panel.innerHTML = '';
        }
        this.renderCartDrawer();
    },

    loadMockSourceTables() {
        this.atlanCart = [
            {
                name: 'orders',
                qualified_name: 'default/snowflake/analytics/raw/ecommerce/orders',
                database_name: 'analytics',
                schema_name: 'raw_ecommerce',
                connector_name: 'snowflake',
                description: 'Raw e-commerce order transactions',
                columns: [
                    { name: 'order_id', data_type: 'NUMBER', logical_type: 'integer', is_primary: true, is_nullable: false },
                    { name: 'customer_id', data_type: 'NUMBER', logical_type: 'integer', is_primary: false, is_nullable: false },
                    { name: 'order_date', data_type: 'TIMESTAMP_NTZ', logical_type: 'timestamp', is_primary: false, is_nullable: false },
                    { name: 'total_amount', data_type: 'NUMBER(12,2)', logical_type: 'number', is_primary: false, is_nullable: true },
                    { name: 'status', data_type: 'VARCHAR', logical_type: 'string', is_primary: false, is_nullable: true },
                    { name: 'shipping_address', data_type: 'VARCHAR', logical_type: 'string', is_primary: false, is_nullable: true },
                ],
            },
            {
                name: 'customers',
                qualified_name: 'default/snowflake/analytics/raw/ecommerce/customers',
                database_name: 'analytics',
                schema_name: 'raw_ecommerce',
                connector_name: 'snowflake',
                description: 'Customer master data',
                columns: [
                    { name: 'customer_id', data_type: 'NUMBER', logical_type: 'integer', is_primary: true, is_nullable: false },
                    { name: 'first_name', data_type: 'VARCHAR', logical_type: 'string', is_primary: false, is_nullable: false },
                    { name: 'last_name', data_type: 'VARCHAR', logical_type: 'string', is_primary: false, is_nullable: false },
                    { name: 'email', data_type: 'VARCHAR', logical_type: 'string', is_primary: false, is_nullable: false },
                    { name: 'created_at', data_type: 'TIMESTAMP_NTZ', logical_type: 'timestamp', is_primary: false, is_nullable: true },
                    { name: 'loyalty_tier', data_type: 'VARCHAR', logical_type: 'string', is_primary: false, is_nullable: true },
                ],
            },
            {
                name: 'products',
                qualified_name: 'default/snowflake/analytics/raw/ecommerce/products',
                database_name: 'analytics',
                schema_name: 'raw_ecommerce',
                connector_name: 'snowflake',
                description: 'Product catalog with pricing',
                columns: [
                    { name: 'product_id', data_type: 'NUMBER', logical_type: 'integer', is_primary: true, is_nullable: false },
                    { name: 'product_name', data_type: 'VARCHAR', logical_type: 'string', is_primary: false, is_nullable: false },
                    { name: 'category', data_type: 'VARCHAR', logical_type: 'string', is_primary: false, is_nullable: true },
                    { name: 'price', data_type: 'NUMBER(10,2)', logical_type: 'number', is_primary: false, is_nullable: true },
                    { name: 'sku', data_type: 'VARCHAR', logical_type: 'string', is_primary: false, is_nullable: false },
                ],
            },
        ];
        // Close the browse panel and show the cart
        this.atlanBrowseOpen = false;
        document.getElementById('atlanBrowsePanel').innerHTML = '';
        this.renderCartDrawer();
        this.renderMain();
    },

    debounceGlobalSearch() {
        clearTimeout(this._globalSearchTimer);
        this._globalSearchTimer = setTimeout(() => this.executeGlobalAtlanSearch(), 300);
    },

    async executeGlobalAtlanSearch() {
        const input = document.getElementById('globalAtlanQuery');
        const typeSelect = document.getElementById('globalAtlanType');
        const resultsEl = document.getElementById('globalAtlanResults');
        if (!input || !resultsEl) return;

        const query = input.value.trim();
        const assetType = typeSelect ? typeSelect.value : 'Table';

        if (!query) {
            resultsEl.innerHTML = '<div style="font-size:0.82rem; color:var(--text-dim); padding:16px; text-align:center;">Search for tables to add to your schema</div>';
            return;
        }

        resultsEl.innerHTML = '<div style="font-size:0.82rem; color:var(--text-dim); padding:16px; text-align:center;">Searching...</div>';

        try {
            const results = await DDLC.api.fetchJSON(`/api/atlan/search-tables?q=${encodeURIComponent(query)}&asset_type=${assetType}&limit=20`);
            if (results.length === 0) {
                resultsEl.innerHTML = '<div style="font-size:0.82rem; color:var(--text-dim); padding:16px; text-align:center;">No results found</div>';
                return;
            }

            // Store results for button refresh
            this.atlanSearchResults = results;

            resultsEl.innerHTML = results.map(r => {
                const inCart = this.atlanCart.some(c => c.qualified_name === r.qualified_name);
                const existsInSchema = (this.session.contract.schema_objects || []).some(
                    o => o.name.toUpperCase() === r.name.toUpperCase()
                );
                return `
                    <div class="atlan-result-row" data-qn="${this.esc(r.qualified_name)}">
                        <div class="atlan-result-info">
                            <div class="atlan-result-name">${this.esc(r.name)}</div>
                            <div class="atlan-result-path">
                                ${this.esc(r.database_name || '')}.${this.esc(r.schema_name || '')}
                                ${r.connector_name ? `<span class="col-badge" style="background:rgba(99,102,241,0.1); color:var(--accent); margin-left:4px; font-size:0.65rem;">${this.esc(r.connector_name)}</span>` : ''}
                            </div>
                            ${r.description ? `<div class="atlan-result-desc">${this.esc(r.description)}</div>` : ''}
                        </div>
                        ${existsInSchema
                            ? `<span style="font-size:0.72rem; color:var(--text-dim); white-space:nowrap;">Already in schema</span>`
                            : `<button class="btn-cart-add ${inCart ? 'in-cart' : ''}"
                                onclick="ContractApp.toggleCartItem(${JSON.stringify(r).replace(/"/g, '&quot;')})"
                                title="${inCart ? 'In cart' : 'Add to cart'}">${inCart ? '&#10003;' : '+'}</button>`
                        }
                    </div>
                `;
            }).join('');
        } catch (err) {
            resultsEl.innerHTML = `<div style="font-size:0.82rem; color:var(--danger); padding:16px; text-align:center;">Error: ${this.esc(err.message)}</div>`;
        }
    },

    toggleCartItem(result) {
        const idx = this.atlanCart.findIndex(c => c.qualified_name === result.qualified_name);
        if (idx >= 0) {
            this.atlanCart.splice(idx, 1);
        } else {
            this.atlanCart.push(result);
        }
        this.renderCartDrawer();
        this.refreshSearchResultButtons();
        // Update the Browse Atlan button badge
        this.updateBrowseButtonBadge();
    },

    addToCart(result) {
        if (this.atlanCart.some(c => c.qualified_name === result.qualified_name)) return;
        this.atlanCart.push(result);
        this.renderCartDrawer();
        this.refreshSearchResultButtons();
        this.updateBrowseButtonBadge();
    },

    removeFromCart(qualifiedName) {
        this.atlanCart = this.atlanCart.filter(c => c.qualified_name !== qualifiedName);
        this.renderCartDrawer();
        this.refreshSearchResultButtons();
        this.updateBrowseButtonBadge();
    },

    clearCart() {
        this.atlanCart = [];
        this.renderCartDrawer();
        this.refreshSearchResultButtons();
        this.updateBrowseButtonBadge();
    },

    updateBrowseButtonBadge() {
        // Find the Browse Atlan button and update its badge
        const btns = document.querySelectorAll('.section-header .btn-primary');
        for (const btn of btns) {
            if (btn.textContent.includes('Browse Atlan')) {
                const count = this.atlanCart.length;
                btn.innerHTML = `Browse Atlan${count > 0 ? `<span class="cart-badge">${count}</span>` : ''}`;
                break;
            }
        }
    },

    renderCartDrawer() {
        const drawer = document.getElementById('atlanCartDrawer');
        if (!drawer) return;

        if (this.atlanCart.length === 0) {
            drawer.innerHTML = '';
            return;
        }

        drawer.innerHTML = `
            <div class="atlan-cart">
                <div class="atlan-cart-header">
                    <h4>Selected Tables (${this.atlanCart.length})</h4>
                    <button class="btn-icon" style="font-size:0.75rem; color:var(--text-dim);" onclick="ContractApp.clearCart()">Clear All</button>
                </div>
                <div>
                    ${this.atlanCart.map(item => `
                        <div class="atlan-cart-item">
                            <div>
                                <span class="atlan-cart-item-name">${this.esc(item.name)}</span>
                                <span class="atlan-cart-item-path">${this.esc(item.database_name || '')}.${this.esc(item.schema_name || '')}${item.connector_name ? ' (' + this.esc(item.connector_name) + ')' : ''}</span>
                            </div>
                            <button class="btn-icon" onclick="ContractApp.removeFromCart('${this.esc(item.qualified_name)}')" title="Remove">&#x2715;</button>
                        </div>
                    `).join('')}
                </div>
                <div class="atlan-cart-actions">
                    <button class="btn btn-primary btn-sm" id="bulkImportBtn" onclick="ContractApp.bulkImportFromCart()">
                        Add ${this.atlanCart.length} Table${this.atlanCart.length !== 1 ? 's' : ''} as Sources
                    </button>
                </div>
            </div>
        `;
    },

    refreshSearchResultButtons() {
        const resultsEl = document.getElementById('globalAtlanResults');
        if (!resultsEl) return;

        const rows = resultsEl.querySelectorAll('.atlan-result-row');
        for (const row of rows) {
            const qn = row.dataset.qn;
            const btn = row.querySelector('.btn-cart-add');
            if (!btn) continue;
            const inCart = this.atlanCart.some(c => c.qualified_name === qn);
            btn.className = `btn-cart-add ${inCart ? 'in-cart' : ''}`;
            btn.innerHTML = inCart ? '&#10003;' : '+';
            btn.title = inCart ? 'In cart' : 'Add to cart';
        }
    },

    async bulkImportFromCart() {
        if (this.atlanCart.length === 0) return;

        const objects = this.session.contract.schema_objects || [];
        const selectedObj = objects.length > 0
            ? objects.find(o => o.name === this._selectedTable) || objects[0]
            : null;

        if (!selectedObj) {
            DDLC.toast.show('Create a target table first, then add sources to it.', 'error');
            return;
        }

        const btn = document.getElementById('bulkImportBtn');
        if (btn) {
            btn.disabled = true;
            btn.textContent = 'Adding sources...';
        }

        try {
            let added = 0;
            let skipped = 0;
            for (const item of this.atlanCart) {
                try {
                    await DDLC.api.post(
                        `/api/sessions/${this.sessionId}/contract/objects/${selectedObj.name}/sources`,
                        {
                            name: item.name,
                            qualified_name: item.qualified_name,
                            database_name: item.database_name,
                            schema_name: item.schema_name,
                            connector_name: item.connector_name,
                            description: item.description,
                            columns: item.columns || null,
                        }
                    );
                    added++;
                } catch (err) {
                    if (err.message && err.message.includes('already added')) {
                        skipped++;
                    } else {
                        throw err;
                    }
                }
            }

            let msg = `Added ${added} source table${added !== 1 ? 's' : ''} to "${selectedObj.name}"`;
            if (skipped > 0) msg += ` (${skipped} already existed)`;
            DDLC.toast.show(msg);

            this.atlanCart = [];
            this.atlanBrowseOpen = false;
            await this.load();
        } catch (err) {
            DDLC.toast.show(err.message, 'error');
            if (btn) {
                btn.disabled = false;
                btn.textContent = `Add ${this.atlanCart.length} Table${this.atlanCart.length !== 1 ? 's' : ''} as Sources`;
            }
        }
    },

    showAddLineage(objName, propName) {
        // Get source tables for this object to offer as options
        const obj = (this.session.contract.schema_objects || []).find(o => o.name === objName);
        const sources = (obj && obj.source_tables) || [];

        const modal = document.createElement('div');
        modal.id = `lineageForm-${objName}-${propName}`;
        modal.innerHTML = `
            <div class="inline-form" style="margin:8px 0;">
                <div style="font-size:0.78rem; font-weight:600; margin-bottom:4px;">Add lineage for "${propName}"</div>
                <div class="inline-form-row">
                    <div><label>Source Table</label>
                        ${sources.length > 0
                            ? `<select id="lineageSrcTable-${objName}-${propName}">
                                ${sources.map(s => `<option value="${this.esc(s.name)}" data-qn="${this.esc(s.qualified_name || '')}">${this.esc(s.name)}</option>`).join('')}
                                <option value="__custom__">Other (type manually)</option>
                               </select>`
                            : `<input id="lineageSrcTable-${objName}-${propName}" placeholder="source_table_name">`
                        }
                    </div>
                    <div><label>Source Column</label><input id="lineageSrcCol-${objName}-${propName}" placeholder="source_column_name"></div>
                </div>
                <div class="inline-form-row">
                    <div><label>Transform Logic (SQL)</label><input id="lineageLogic-${objName}-${propName}" placeholder="e.g., COALESCE(a.col, b.col)"></div>
                    <div><label>Description</label><input id="lineageDesc-${objName}-${propName}" placeholder="e.g., Combines from two sources"></div>
                </div>
                <div class="form-actions">
                    <button class="btn btn-sm" onclick="this.closest('.inline-form').remove()">Cancel</button>
                    <button class="btn btn-primary btn-sm" onclick="ContractApp.saveLineage('${objName}', '${propName}')">Add Source</button>
                </div>
            </div>
        `;

        // Insert the form after the row — try new target-columns-table first, fall back to old layout
        const tbody = document.querySelector(`.target-columns-table tbody`) || document.querySelector(`#table-${objName} tbody`);
        if (tbody) {
            const tr = document.createElement('tr');
            tr.innerHTML = `<td colspan="4">${modal.innerHTML}</td>`;
            // Find the row for this property
            const rows = tbody.querySelectorAll('tr');
            for (const row of rows) {
                const nameEl = row.querySelector('.target-col-name') || row.querySelector('.col-name');
                if (nameEl && nameEl.textContent === propName) {
                    row.after(tr);
                    break;
                }
            }
        }
    },

    async saveLineage(objName, propName) {
        const srcTableEl = document.getElementById(`lineageSrcTable-${objName}-${propName}`);
        const srcTable = srcTableEl ? srcTableEl.value : '';
        const srcCol = document.getElementById(`lineageSrcCol-${objName}-${propName}`).value.trim();

        if (!srcTable || !srcCol) return DDLC.toast.show('Source table and column are required', 'error');

        // Get qualified name from select data attribute if available
        let srcQN = null;
        if (srcTableEl && srcTableEl.selectedOptions && srcTableEl.selectedOptions[0]) {
            srcQN = srcTableEl.selectedOptions[0].dataset.qn || null;
        }

        try {
            await DDLC.api.post(`/api/sessions/${this.sessionId}/contract/objects/${objName}/properties/${propName}/sources`, {
                source_table: srcTable,
                source_column: srcCol,
                source_table_qualified_name: srcQN,
                transform_logic: document.getElementById(`lineageLogic-${objName}-${propName}`).value.trim() || null,
                transform_description: document.getElementById(`lineageDesc-${objName}-${propName}`).value.trim() || null,
            });
            DDLC.toast.show('Lineage source added');
            await this.load();
        } catch (err) {
            DDLC.toast.show(err.message, 'error');
        }
    },

    // --- Quality section (Phase 1.2: enriched for Monte Carlo) ---

    _QUALITY_METHODS: [
        { value: '', label: 'None' },
        { value: 'freshness', label: 'Freshness' },
        { value: 'volume', label: 'Volume' },
        { value: 'schema', label: 'Schema Change' },
        { value: 'field_health', label: 'Field Health' },
        { value: 'dimension_tracking', label: 'Dim Tracking' },
        { value: 'sql_rule', label: 'SQL Rule' },
        { value: 'referential_integrity', label: 'Ref Integrity' },
    ],
    _QUALITY_ENGINES: ['', 'monte-carlo', 'great-expectations', 'soda', 'dbt'],
    _QUALITY_SEVERITIES: ['', 'critical', 'high', 'medium', 'low'],
    _QUALITY_TYPES: ['text', 'library', 'sql', 'custom'],
    _SEVERITY_COLORS: { critical: '#ef4444', high: '#f59e0b', medium: '#3b82f6', low: '#6b7280' },

    _getAllColumns() {
        const cols = [];
        (this.session.contract.schema_objects || []).forEach(obj => {
            (obj.properties || []).forEach(prop => {
                cols.push(`${obj.name}.${prop.name}`);
            });
        });
        return cols;
    },

    renderQualitySection() {
        const checks = this.session.contract.quality_checks || [];
        const expandedId = this._expandedQualityId || null;
        return `
            <div class="section-panel">
                <div class="section-header">
                    <h3>Quality Rules (${checks.length})</h3>
                    <button class="btn btn-sm btn-primary" onclick="ContractApp.showAddQuality()">+ Add Rule</button>
                </div>
                <div class="section-body" id="qualityBody">
                    <div id="addQualityForm"></div>
                    ${checks.length === 0 ? '<div style="color:var(--text-dim); font-size:0.85rem; text-align:center; padding:12px;">No quality rules defined.</div>' : ''}
                    ${checks.map(q => this.renderQualityCard(q, expandedId === q.id)).join('')}
                </div>
            </div>
        `;
    },

    renderQualityCard(q, isExpanded) {
        const sevColor = this._SEVERITY_COLORS[q.severity] || 'var(--text-dim)';
        const methodLabel = (this._QUALITY_METHODS.find(m => m.value === q.method) || {}).label || q.method;
        return `
            <div class="quality-card ${isExpanded ? 'expanded' : ''}" data-quality-id="${q.id}">
                <div class="quality-card-header" onclick="ContractApp.toggleQualityCard('${q.id}')">
                    <div class="quality-card-badges">
                        ${q.method ? `<span class="col-badge" style="background:rgba(16,185,129,0.15); color:#10b981;">${this.esc(methodLabel)}</span>` : ''}
                        <span class="col-badge" style="background:rgba(99,102,241,0.15); color:var(--accent);">${this.esc(q.type)}</span>
                        ${q.dimension ? `<span class="col-badge" style="background:rgba(59,130,246,0.15); color:var(--info);">${this.esc(q.dimension)}</span>` : ''}
                        ${q.severity ? `<span style="display:inline-block; width:8px; height:8px; border-radius:50%; background:${sevColor}; margin-left:4px;" title="${this.esc(q.severity)}"></span>` : ''}
                    </div>
                    <div class="quality-card-desc">${this.esc(q.description)}</div>
                    <div class="quality-card-meta">
                        ${q.column ? `<span class="quality-meta-tag">&#128204; ${this.esc(q.column)}</span>` : ''}
                        ${q.schedule ? `<span class="quality-meta-tag">&#128339; ${this.esc(q.schedule)}</span>` : ''}
                        ${q.engine ? `<span class="quality-meta-tag">&#9881; ${this.esc(q.engine)}</span>` : ''}
                    </div>
                    <div class="quality-card-actions">
                        <button class="btn-icon" onclick="event.stopPropagation(); ContractApp.deleteQuality('${q.id}')">&#128465;</button>
                    </div>
                </div>
                ${isExpanded ? this.renderQualityEditForm(q) : ''}
            </div>
        `;
    },

    toggleQualityCard(checkId) {
        this._expandedQualityId = (this._expandedQualityId === checkId) ? null : checkId;
        this.renderMain();
    },

    renderQualityEditForm(q) {
        const allColumns = this._getAllColumns();
        const showQuery = (q.method === 'sql_rule' || q.type === 'sql');

        return `
            <div class="quality-edit-body">
                <div class="inline-form-row three-col">
                    <div><label>Type</label>
                        <select id="editQualType-${q.id}">
                            ${this._QUALITY_TYPES.map(t => `<option value="${t}" ${q.type === t ? 'selected' : ''}>${t}</option>`).join('')}
                        </select>
                    </div>
                    <div><label>Method (Monitor Type)</label>
                        <select id="editQualMethod-${q.id}" onchange="ContractApp.onQualMethodChange('${q.id}')">
                            ${this._QUALITY_METHODS.map(m => `<option value="${m.value}" ${(q.method || '') === m.value ? 'selected' : ''}>${m.label}</option>`).join('')}
                        </select>
                    </div>
                    <div><label>Severity</label>
                        <select id="editQualSeverity-${q.id}">
                            ${this._QUALITY_SEVERITIES.map(s => `<option value="${s}" ${(q.severity || '') === s ? 'selected' : ''}>${s || 'None'}</option>`).join('')}
                        </select>
                    </div>
                </div>

                <div class="inline-form-row three-col">
                    <div><label>Dimension</label>
                        <input id="editQualDim-${q.id}" value="${this.esc(q.dimension || '')}" placeholder="e.g., completeness, accuracy">
                    </div>
                    <div><label>Target Column</label>
                        <select id="editQualColumn-${q.id}">
                            <option value="">Table-level (no column)</option>
                            ${allColumns.map(c => `<option value="${this.esc(c)}" ${(q.column || '') === c ? 'selected' : ''}>${this.esc(c)}</option>`).join('')}
                        </select>
                    </div>
                    <div><label>Engine</label>
                        <select id="editQualEngine-${q.id}">
                            ${this._QUALITY_ENGINES.map(e => `<option value="${e}" ${(q.engine || '') === e ? 'selected' : ''}>${e || 'None'}</option>`).join('')}
                        </select>
                    </div>
                </div>

                <div><label>Description *</label>
                    <textarea id="editQualDesc-${q.id}" rows="2">${this.esc(q.description || '')}</textarea>
                </div>

                <div class="inline-form-row">
                    <div><label>Schedule (cron)</label>
                        <input id="editQualSchedule-${q.id}" value="${this.esc(q.schedule || '')}" placeholder="e.g., 0 6 * * *">
                    </div>
                    <div><label>Scheduler</label>
                        <input id="editQualScheduler-${q.id}" value="${this.esc(q.scheduler || '')}" placeholder="e.g., cron, airflow, monte-carlo">
                    </div>
                </div>

                <div id="qualQueryRow-${q.id}" style="${showQuery ? '' : 'display:none;'}">
                    <label>SQL Query</label>
                    <textarea id="editQualQuery-${q.id}" rows="3" style="font-family:var(--font-mono,monospace); font-size:0.82rem;">${this.esc(q.query || '')}</textarea>
                </div>

                <div class="inline-form-row three-col">
                    <div><label>Must Be</label>
                        <input id="editQualMustBe-${q.id}" value="${this.esc(q.must_be || '')}" placeholder="e.g., unique, not null">
                    </div>
                    <div><label>Must Be Greater Than</label>
                        <input type="number" id="editQualGT-${q.id}" value="${q.must_be_greater_than != null ? q.must_be_greater_than : ''}" step="any">
                    </div>
                    <div><label>Must Be Less Than</label>
                        <input type="number" id="editQualLT-${q.id}" value="${q.must_be_less_than != null ? q.must_be_less_than : ''}" step="any">
                    </div>
                </div>

                <div><label>Business Impact</label>
                    <textarea id="editQualImpact-${q.id}" rows="2" placeholder="What happens if this rule fails?">${this.esc(q.business_impact || '')}</textarea>
                </div>

                <div class="form-actions">
                    <button class="btn btn-sm" onclick="ContractApp.toggleQualityCard('${q.id}')">Cancel</button>
                    <button class="btn btn-primary btn-sm" onclick="ContractApp.saveQualityEdits('${q.id}')">Save Changes</button>
                </div>
            </div>
        `;
    },

    onQualMethodChange(checkId) {
        const method = document.getElementById(`editQualMethod-${checkId}`)?.value;
        const type = document.getElementById(`editQualType-${checkId}`)?.value;
        const queryRow = document.getElementById(`qualQueryRow-${checkId}`);
        if (queryRow) {
            queryRow.style.display = (method === 'sql_rule' || type === 'sql') ? '' : 'none';
        }
    },

    async saveQualityEdits(checkId) {
        const desc = document.getElementById(`editQualDesc-${checkId}`)?.value.trim();
        if (!desc) return DDLC.toast.show('Description is required', 'error');

        const gtRaw = document.getElementById(`editQualGT-${checkId}`)?.value;
        const ltRaw = document.getElementById(`editQualLT-${checkId}`)?.value;

        try {
            await DDLC.api.put(`/api/sessions/${this.sessionId}/contract/quality/${checkId}`, {
                type: document.getElementById(`editQualType-${checkId}`)?.value || 'text',
                description: desc,
                dimension: document.getElementById(`editQualDim-${checkId}`)?.value.trim() || null,
                severity: document.getElementById(`editQualSeverity-${checkId}`)?.value || null,
                method: document.getElementById(`editQualMethod-${checkId}`)?.value || null,
                column: document.getElementById(`editQualColumn-${checkId}`)?.value || null,
                engine: document.getElementById(`editQualEngine-${checkId}`)?.value || null,
                schedule: document.getElementById(`editQualSchedule-${checkId}`)?.value.trim() || null,
                scheduler: document.getElementById(`editQualScheduler-${checkId}`)?.value.trim() || null,
                query: document.getElementById(`editQualQuery-${checkId}`)?.value.trim() || null,
                must_be: document.getElementById(`editQualMustBe-${checkId}`)?.value.trim() || null,
                must_be_greater_than: gtRaw !== '' && gtRaw != null ? parseFloat(gtRaw) : null,
                must_be_less_than: ltRaw !== '' && ltRaw != null ? parseFloat(ltRaw) : null,
                business_impact: document.getElementById(`editQualImpact-${checkId}`)?.value.trim() || null,
            });
            DDLC.toast.show('Quality rule updated');
            this._expandedQualityId = null;
            await this.load();
        } catch (err) {
            DDLC.toast.show(err.message, 'error');
        }
    },

    showAddQuality() {
        const allColumns = this._getAllColumns();
        document.getElementById('addQualityForm').innerHTML = `
            <div class="quality-edit-body" style="border:1px solid var(--accent); border-radius:var(--radius); margin-bottom:12px;">
                <div style="font-weight:600; font-size:0.85rem; margin-bottom:4px;">New Quality Rule</div>

                <div class="inline-form-row three-col">
                    <div><label>Type</label>
                        <select id="newQualType">
                            ${this._QUALITY_TYPES.map(t => `<option value="${t}">${t}</option>`).join('')}
                        </select>
                    </div>
                    <div><label>Method (Monitor Type)</label>
                        <select id="newQualMethod" onchange="ContractApp.onNewQualMethodChange()">
                            ${this._QUALITY_METHODS.map(m => `<option value="${m.value}">${m.label}</option>`).join('')}
                        </select>
                    </div>
                    <div><label>Severity</label>
                        <select id="newQualSeverity">
                            ${this._QUALITY_SEVERITIES.map(s => `<option value="${s}">${s || 'None'}</option>`).join('')}
                        </select>
                    </div>
                </div>

                <div class="inline-form-row three-col">
                    <div><label>Dimension</label>
                        <input id="newQualDimension" placeholder="e.g., completeness, accuracy">
                    </div>
                    <div><label>Target Column</label>
                        <select id="newQualColumn">
                            <option value="">Table-level (no column)</option>
                            ${allColumns.map(c => `<option value="${this.esc(c)}">${this.esc(c)}</option>`).join('')}
                        </select>
                    </div>
                    <div><label>Engine</label>
                        <select id="newQualEngine">
                            ${this._QUALITY_ENGINES.map(e => `<option value="${e}">${e || 'None'}</option>`).join('')}
                        </select>
                    </div>
                </div>

                <div><label>Description *</label>
                    <textarea id="newQualDesc" rows="2" placeholder="e.g., Customer ID must be unique across all rows."></textarea>
                </div>

                <div class="inline-form-row">
                    <div><label>Schedule (cron)</label>
                        <input id="newQualSchedule" placeholder="e.g., 0 6 * * *">
                    </div>
                    <div><label>Scheduler</label>
                        <input id="newQualScheduler" placeholder="e.g., cron, airflow, monte-carlo">
                    </div>
                </div>

                <div id="newQualQueryRow" style="display:none;">
                    <label>SQL Query</label>
                    <textarea id="newQualQuery" rows="3" style="font-family:var(--font-mono,monospace); font-size:0.82rem;" placeholder="SELECT COUNT(*) FROM ..."></textarea>
                </div>

                <div class="inline-form-row three-col">
                    <div><label>Must Be</label>
                        <input id="newQualMustBe" placeholder="e.g., unique, not null">
                    </div>
                    <div><label>Must Be Greater Than</label>
                        <input type="number" id="newQualGT" step="any">
                    </div>
                    <div><label>Must Be Less Than</label>
                        <input type="number" id="newQualLT" step="any">
                    </div>
                </div>

                <div><label>Business Impact</label>
                    <textarea id="newQualImpact" rows="2" placeholder="What happens if this rule fails?"></textarea>
                </div>

                <div class="form-actions">
                    <button class="btn btn-sm" onclick="ContractApp.renderMain()">Cancel</button>
                    <button class="btn btn-primary btn-sm" onclick="ContractApp.addQuality()">Add Rule</button>
                </div>
            </div>
        `;
    },

    onNewQualMethodChange() {
        const method = document.getElementById('newQualMethod')?.value;
        const type = document.getElementById('newQualType')?.value;
        const queryRow = document.getElementById('newQualQueryRow');
        if (queryRow) {
            queryRow.style.display = (method === 'sql_rule' || type === 'sql') ? '' : 'none';
        }
    },

    async addQuality() {
        const desc = document.getElementById('newQualDesc').value.trim();
        if (!desc) return DDLC.toast.show('Description is required', 'error');

        const gtRaw = document.getElementById('newQualGT')?.value;
        const ltRaw = document.getElementById('newQualLT')?.value;

        try {
            await DDLC.api.post(`/api/sessions/${this.sessionId}/contract/quality`, {
                type: document.getElementById('newQualType').value,
                description: desc,
                dimension: document.getElementById('newQualDimension').value.trim() || null,
                severity: document.getElementById('newQualSeverity')?.value || null,
                method: document.getElementById('newQualMethod')?.value || null,
                column: document.getElementById('newQualColumn')?.value || null,
                engine: document.getElementById('newQualEngine')?.value || null,
                schedule: document.getElementById('newQualSchedule')?.value.trim() || null,
                scheduler: document.getElementById('newQualScheduler')?.value.trim() || null,
                query: document.getElementById('newQualQuery')?.value.trim() || null,
                must_be: document.getElementById('newQualMustBe')?.value.trim() || null,
                must_be_greater_than: gtRaw ? parseFloat(gtRaw) : null,
                must_be_less_than: ltRaw ? parseFloat(ltRaw) : null,
                business_impact: document.getElementById('newQualImpact')?.value.trim() || null,
            });
            DDLC.toast.show('Quality rule added');
            await this.load();
        } catch (err) {
            DDLC.toast.show(err.message, 'error');
        }
    },

    async deleteQuality(checkId) {
        try {
            await DDLC.api.del(`/api/sessions/${this.sessionId}/contract/quality/${checkId}`);
            DDLC.toast.show('Quality rule deleted');
            await this.load();
        } catch (err) {
            DDLC.toast.show(err.message, 'error');
        }
    },

    // --- SLA section (Phase 1.3: enriched for Airflow) ---

    _SLA_PROPERTIES: [
        { value: 'freshness', label: 'Freshness' },
        { value: 'availability', label: 'Availability' },
        { value: 'latency', label: 'Latency' },
        { value: 'retention', label: 'Retention' },
        { value: 'frequency', label: 'Frequency' },
        { value: 'throughput', label: 'Throughput' },
        { value: 'completeness', label: 'Completeness' },
    ],
    _SLA_SCHEDULERS: ['', 'airflow', 'cron', 'prefect', 'dagster'],
    _SLA_DRIVERS: ['', 'regulatory', 'analytics', 'operational', 'compliance'],
    _SERVER_TYPES: [
        { value: 'snowflake',  label: 'Snowflake' },
        { value: 'bigquery',   label: 'BigQuery' },
        { value: 'databricks', label: 'Databricks' },
        { value: 'redshift',   label: 'Redshift' },
        { value: 'postgres',   label: 'PostgreSQL' },
        { value: 'other',      label: 'Other' },
    ],
    _SERVER_ENVS: ['prod', 'staging', 'dev', 'test'],
    _SERVER_ENV_COLORS: { prod: '#ef4444', staging: '#f59e0b', dev: '#3b82f6', test: '#6b7280' },
    _SERVER_ACCT_LABELS: { snowflake: 'Account', bigquery: 'Project ID', databricks: 'Workspace URL', redshift: 'Cluster Endpoint', postgres: 'Host', other: 'Account / Host' },
    _ACCESS_LEVELS: [
        { value: 'read',  label: 'Read',  color: '#3b82f6' },
        { value: 'write', label: 'Write', color: '#f59e0b' },
        { value: 'admin', label: 'Admin', color: '#ef4444' },
    ],

    _getSchemaObjectNames() {
        return (this.session.contract.schema_objects || []).map(o => o.name);
    },

    _cronPreviewForFreshness(value, unit) {
        if (!value || !unit) return null;
        const v = parseInt(value, 10);
        if (isNaN(v) || v <= 0) return null;
        const u = (unit || '').toLowerCase();
        if (u === 'hours' || u === 'hour' || u === 'h') {
            if (v === 1) return { cron: '0 * * * *', desc: 'Every hour' };
            if (v === 6) return { cron: '0 */6 * * *', desc: 'Every 6 hours' };
            if (v === 12) return { cron: '0 */12 * * *', desc: 'Every 12 hours' };
            if (v === 24) return { cron: '0 0 * * *', desc: 'Daily at midnight' };
            return { cron: `0 */${v} * * *`, desc: `Every ${v} hours` };
        }
        if (u === 'minutes' || u === 'minute' || u === 'min') {
            if (v === 30) return { cron: '*/30 * * * *', desc: 'Every 30 minutes' };
            if (v === 15) return { cron: '*/15 * * * *', desc: 'Every 15 minutes' };
            return { cron: `*/${v} * * * *`, desc: `Every ${v} minutes` };
        }
        if (u === 'days' || u === 'day' || u === 'd') {
            if (v === 1) return { cron: '0 0 * * *', desc: 'Daily at midnight' };
            if (v === 7) return { cron: '0 0 * * 0', desc: 'Weekly (Sunday midnight)' };
            return { cron: `0 0 */${v} * *`, desc: `Every ${v} days` };
        }
        return null;
    },

    renderSLASection() {
        const slas = this.session.contract.sla_properties || [];
        const expandedId = this._expandedSLAId || null;
        return `
            <div class="section-panel">
                <div class="section-header">
                    <h3>Service Level Agreements (${slas.length})</h3>
                    <button class="btn btn-sm btn-primary" onclick="ContractApp.showAddSLA()">+ Add SLA</button>
                </div>
                <div class="section-body" id="slaBody">
                    <div id="addSLAForm"></div>
                    ${slas.length === 0 ? '<div style="color:var(--text-dim); font-size:0.85rem; text-align:center; padding:12px;">No SLAs defined.</div>' : ''}
                    ${slas.map(s => this.renderSLACard(s, expandedId === s.id)).join('')}
                </div>
            </div>
        `;
    },

    renderSLACard(s, isExpanded) {
        const propLabel = (this._SLA_PROPERTIES.find(p => p.value === s.property) || {}).label || s.property;
        const driverLabel = s.driver ? s.driver.charAt(0).toUpperCase() + s.driver.slice(1) : '';
        return `
            <div class="sla-card ${isExpanded ? 'expanded' : ''}">
                <div class="sla-card-header" onclick="ContractApp.toggleSLACard('${s.id}')">
                    <div class="sla-card-prop">${this.esc(propLabel)}</div>
                    <div class="sla-card-value">
                        ${this.esc(s.value)}${s.unit ? `<span class="sla-unit">${this.esc(s.unit)}</span>` : ''}
                    </div>
                    <div class="sla-card-desc">${s.description ? this.esc(s.description) : ''}</div>
                    <div class="sla-card-meta">
                        ${s.schedule ? `<span class="sla-meta-tag">🕓 ${this.esc(s.schedule)}</span>` : ''}
                        ${s.driver ? `<span class="sla-meta-tag">📋 ${this.esc(driverLabel)}</span>` : ''}
                        ${s.element ? `<span class="sla-meta-tag">📌 ${this.esc(s.element)}</span>` : ''}
                    </div>
                    <div class="sla-card-actions">
                        <button class="btn-icon" onclick="event.stopPropagation(); ContractApp.deleteSLA('${s.id}')">&#128465;</button>
                    </div>
                </div>
                ${isExpanded ? `<div class="sla-edit-body">${this.renderSLAEditForm(s)}</div>` : ''}
            </div>
        `;
    },

    toggleSLACard(slaId) {
        this._expandedSLAId = (this._expandedSLAId === slaId) ? null : slaId;
        this.renderMain();
    },

    renderSLAEditForm(s) {
        const schemaObjects = this._getSchemaObjectNames();
        const cronPreview = (s.property === 'freshness') ? this._cronPreviewForFreshness(s.value, s.unit) : null;
        return `
            <div class="inline-form-row three-col">
                <div>
                    <label>Property</label>
                    <select id="editSLA_prop_${s.id}">
                        ${this._SLA_PROPERTIES.map(p => `<option value="${p.value}" ${s.property === p.value ? 'selected' : ''}>${p.label}</option>`).join('')}
                    </select>
                </div>
                <div>
                    <label>Value *</label>
                    <input id="editSLA_value_${s.id}" value="${this.esc(s.value)}" placeholder="e.g., 24, 99.9, daily">
                </div>
                <div>
                    <label>Unit</label>
                    <input id="editSLA_unit_${s.id}" value="${this.esc(s.unit || '')}" placeholder="e.g., hours, percent, ms">
                </div>
            </div>
            <div class="inline-form-row three-col">
                <div>
                    <label>Schedule (cron)</label>
                    <input id="editSLA_schedule_${s.id}" value="${this.esc(s.schedule || '')}" placeholder="e.g., 0 6 * * *" style="font-family:var(--font-mono);">
                </div>
                <div>
                    <label>Scheduler</label>
                    <select id="editSLA_scheduler_${s.id}">
                        ${this._SLA_SCHEDULERS.map(sc => `<option value="${sc}" ${(s.scheduler || '') === sc ? 'selected' : ''}>${sc || 'None'}</option>`).join('')}
                    </select>
                </div>
                <div>
                    <label>Driver</label>
                    <select id="editSLA_driver_${s.id}">
                        ${this._SLA_DRIVERS.map(d => `<option value="${d}" ${(s.driver || '') === d ? 'selected' : ''}>${d ? d.charAt(0).toUpperCase() + d.slice(1) : 'None'}</option>`).join('')}
                    </select>
                </div>
            </div>
            <div class="inline-form-row">
                <div>
                    <label>Element (Schema Object)</label>
                    <select id="editSLA_element_${s.id}">
                        <option value="">All (no specific object)</option>
                        ${schemaObjects.map(name => `<option value="${name}" ${(s.element || '') === name ? 'selected' : ''}>${name}</option>`).join('')}
                    </select>
                </div>
                <div></div>
            </div>
            <div>
                <label>Description</label>
                <textarea id="editSLA_desc_${s.id}" rows="2" placeholder="Additional context for this SLA">${this.esc(s.description || '')}</textarea>
            </div>
            ${cronPreview ? `
                <div class="cron-preview">
                    <div class="cron-label">Suggested cron for freshness</div>
                    <code>${cronPreview.cron}</code> — ${cronPreview.desc}
                </div>
            ` : ''}
            <div class="form-actions">
                <button class="btn btn-sm" onclick="ContractApp.toggleSLACard('${s.id}')">Cancel</button>
                <button class="btn btn-primary btn-sm" onclick="ContractApp.saveSLAEdits('${s.id}')">Save Changes</button>
            </div>
        `;
    },

    async saveSLAEdits(slaId) {
        const get = (field) => {
            const el = document.getElementById(`editSLA_${field}_${slaId}`);
            return el ? el.value.trim() : '';
        };
        try {
            await DDLC.api.put(`/api/sessions/${this.sessionId}/contract/sla/${slaId}`, {
                property: get('prop'),
                value: get('value'),
                unit: get('unit') || null,
                description: get('desc') || null,
                schedule: get('schedule') || null,
                scheduler: get('scheduler') || null,
                driver: get('driver') || null,
                element: get('element') || null,
            });
            this._expandedSLAId = null;
            DDLC.toast.show('SLA updated');
            await this.load();
        } catch (err) {
            DDLC.toast.show(err.message, 'error');
        }
    },

    showAddSLA() {
        const schemaObjects = this._getSchemaObjectNames();
        document.getElementById('addSLAForm').innerHTML = `
            <div class="inline-form">
                <div class="inline-form-row three-col" style="grid-template-columns:1fr 1fr 1fr;">
                    <div><label>Property</label>
                        <select id="newSLAProp" onchange="ContractApp.onNewSLAPropChange()">
                            ${this._SLA_PROPERTIES.map(p => `<option value="${p.value}">${p.label}</option>`).join('')}
                        </select>
                    </div>
                    <div><label>Value *</label><input id="newSLAValue" placeholder="e.g., 24, 99.9, daily"></div>
                    <div><label>Unit</label><input id="newSLAUnit" placeholder="e.g., hours, percent, ms"></div>
                </div>
                <div class="inline-form-row three-col" style="grid-template-columns:1fr 1fr 1fr;">
                    <div><label>Schedule (cron)</label><input id="newSLASchedule" placeholder="e.g., 0 6 * * *" style="font-family:var(--font-mono);"></div>
                    <div><label>Scheduler</label>
                        <select id="newSLAScheduler">
                            ${this._SLA_SCHEDULERS.map(sc => `<option value="${sc}">${sc || 'None'}</option>`).join('')}
                        </select>
                    </div>
                    <div><label>Driver</label>
                        <select id="newSLADriver">
                            ${this._SLA_DRIVERS.map(d => `<option value="${d}">${d ? d.charAt(0).toUpperCase() + d.slice(1) : 'None'}</option>`).join('')}
                        </select>
                    </div>
                </div>
                <div class="inline-form-row">
                    <div><label>Element (Schema Object)</label>
                        <select id="newSLAElement">
                            <option value="">All (no specific object)</option>
                            ${schemaObjects.map(name => `<option value="${name}">${name}</option>`).join('')}
                        </select>
                    </div>
                    <div><label>Description</label><input id="newSLADesc" placeholder="Additional context"></div>
                </div>
                <div id="newSLACronPreview"></div>
                <div class="form-actions">
                    <button class="btn btn-sm" onclick="ContractApp.renderMain()">Cancel</button>
                    <button class="btn btn-primary btn-sm" onclick="ContractApp.addSLA()">Add SLA</button>
                </div>
            </div>
        `;
    },

    onNewSLAPropChange() {
        const prop = document.getElementById('newSLAProp').value;
        const value = (document.getElementById('newSLAValue') || {}).value || '';
        const unit = (document.getElementById('newSLAUnit') || {}).value || '';
        const previewEl = document.getElementById('newSLACronPreview');
        if (!previewEl) return;
        if (prop === 'freshness' && value && unit) {
            const preview = this._cronPreviewForFreshness(value, unit);
            if (preview) {
                previewEl.innerHTML = `<div class="cron-preview"><div class="cron-label">Suggested cron for freshness</div><code>${preview.cron}</code> — ${preview.desc}</div>`;
                return;
            }
        }
        previewEl.innerHTML = '';
    },

    async addSLA() {
        const value = document.getElementById('newSLAValue').value.trim();
        if (!value) return DDLC.toast.show('Value is required', 'error');
        try {
            await DDLC.api.post(`/api/sessions/${this.sessionId}/contract/sla`, {
                property: document.getElementById('newSLAProp').value,
                value,
                unit: document.getElementById('newSLAUnit').value.trim() || null,
                description: document.getElementById('newSLADesc').value.trim() || null,
                schedule: document.getElementById('newSLASchedule').value.trim() || null,
                scheduler: document.getElementById('newSLAScheduler').value || null,
                driver: document.getElementById('newSLADriver').value || null,
                element: document.getElementById('newSLAElement').value || null,
            });
            DDLC.toast.show('SLA added');
            await this.load();
        } catch (err) {
            DDLC.toast.show(err.message, 'error');
        }
    },

    async deleteSLA(slaId) {
        try {
            await DDLC.api.del(`/api/sessions/${this.sessionId}/contract/sla/by-id/${slaId}`);
            DDLC.toast.show('SLA deleted');
            await this.load();
        } catch (err) {
            DDLC.toast.show(err.message, 'error');
        }
    },

    // --- Servers / Infrastructure section ---
    renderServersSection() {
        const servers = this.session.contract.servers || [];
        const expandedId = this._expandedServerId;
        return `
            <div class="section-panel">
                <div class="section-header">
                    <h3>Infrastructure / Servers (${servers.length})</h3>
                    <button class="btn btn-sm btn-primary" onclick="ContractApp.showAddServer()">+ Add Server</button>
                </div>
                <div class="section-body" id="serverBody">
                    <div id="addServerForm"></div>
                    ${servers.length === 0 ? '<div style="color:var(--text-dim); font-size:0.85rem; text-align:center; padding:12px;">No servers defined. Add at least one to specify where this asset will be materialized.</div>' : ''}
                    ${servers.map(s => this.renderServerCard(s, expandedId === s.id)).join('')}
                </div>
            </div>
        `;
    },

    renderServerCard(s, isExpanded) {
        const typeLabel = (this._SERVER_TYPES.find(t => t.value === s.type) || {}).label || s.type;
        const envColor = this._SERVER_ENV_COLORS[s.environment] || 'var(--text-dim)';
        const connSummary = [s.database, s.schema_name].filter(Boolean).join('.');
        return `
            <div class="server-card ${isExpanded ? 'expanded' : ''}">
                <div class="server-card-header" onclick="ContractApp.toggleServerCard('${s.id}')">
                    <div class="server-card-type">${this.esc(typeLabel)}</div>
                    <span class="col-badge server-card-env" style="background:${envColor}22; color:${envColor};">${this.esc(s.environment)}</span>
                    ${connSummary ? `<span style="font-size:0.82rem; font-family:var(--font-mono); color:var(--text-muted);">${this.esc(connSummary)}</span>` : ''}
                    <div class="server-card-desc">${s.description ? this.esc(s.description) : ''}</div>
                    <div class="server-card-meta">
                        ${s.account ? `<span class="server-meta-tag">&#127968; ${this.esc(s.account)}</span>` : ''}
                        ${s.host ? `<span class="server-meta-tag">&#128279; ${this.esc(s.host)}</span>` : ''}
                    </div>
                    <div class="server-card-actions">
                        <button class="btn-icon" onclick="event.stopPropagation(); ContractApp.deleteServer('${s.id}')">&#128465;</button>
                    </div>
                </div>
                ${isExpanded ? `<div class="server-edit-body">${this.renderServerEditForm(s)}</div>` : ''}
            </div>
        `;
    },

    renderServerEditForm(s) {
        const acctLabel = this._SERVER_ACCT_LABELS[s.type] || 'Account';
        const typeOpts = this._SERVER_TYPES.map(t =>
            `<option value="${t.value}" ${s.type === t.value ? 'selected' : ''}>${t.label}</option>`
        ).join('');
        const envOpts = this._SERVER_ENVS.map(e =>
            `<option value="${e}" ${s.environment === e ? 'selected' : ''}>${e}</option>`
        ).join('');
        const connQn = s.connection_qualified_name || '';
        const connDisplay = connQn ? connQn.split('/').slice(0, 3).join('/') : '';
        return `
            <div class="inline-form-row">
                <div>
                    <label>Type</label>
                    <select id="editSrv_type_${s.id}" onchange="ContractApp.onServerTypeChange('${s.id}')">${typeOpts}</select>
                </div>
                <div>
                    <label>Environment</label>
                    <select id="editSrv_env_${s.id}">${envOpts}</select>
                </div>
            </div>
            ${this.atlanConfigured ? `
            <div style="position:relative; margin-bottom:8px;">
                <label>Atlan Connection</label>
                <input id="editSrv_conn_search_${s.id}" autocomplete="off"
                       value="${this.esc(connDisplay)}"
                       placeholder="Search Atlan connections…"
                       oninput="ContractApp.onConnectionSearch('${s.id}', 'edit')"
                       onblur="setTimeout(()=>ContractApp.hideConnectionDropdown('${s.id}', 'edit'), 200)"
                       style="width:100%;">
                <div id="connDropdown_edit_${s.id}"
                     style="display:none; position:absolute; top:100%; left:0; right:0; z-index:1500;
                            background:var(--bg-card); border:1px solid var(--border-focus);
                            border-radius:var(--radius); max-height:180px; overflow-y:auto;
                            box-shadow:0 4px 12px rgba(0,0,0,0.3);"></div>
                <input type="hidden" id="editSrv_conn_qn_${s.id}" value="${this.esc(connQn)}">
            </div>` : ''}
            <div class="inline-form-row">
                <div>
                    <label id="editSrv_acctLabel_${s.id}">${acctLabel}</label>
                    <input id="editSrv_account_${s.id}" value="${this.esc(s.account || '')}" placeholder="e.g. myorg.us-east-1">
                </div>
                <div>
                    <label>Host / URL</label>
                    <input id="editSrv_host_${s.id}" value="${this.esc(s.host || '')}" placeholder="e.g. abc123.snowflakecomputing.com">
                </div>
            </div>
            <div class="inline-form-row">
                <div>
                    <label>Database</label>
                    <input id="editSrv_database_${s.id}" value="${this.esc(s.database || '')}" placeholder="e.g. ANALYTICS_DB">
                </div>
                <div>
                    <label>Schema</label>
                    <input id="editSrv_schema_${s.id}" value="${this.esc(s.schema_name || '')}" placeholder="e.g. MARTS">
                </div>
            </div>
            <div>
                <label>Description</label>
                <textarea id="editSrv_desc_${s.id}" rows="2" placeholder="Notes about this connection...">${this.esc(s.description || '')}</textarea>
            </div>
            <div class="form-actions">
                <button class="btn btn-sm" onclick="ContractApp.toggleServerCard('${s.id}')">Cancel</button>
                <button class="btn btn-sm btn-primary" onclick="ContractApp.saveServerEdits('${s.id}')">Save Changes</button>
            </div>
        `;
    },

    onServerTypeChange(serverId) {
        const type = document.getElementById(`editSrv_type_${serverId}`)?.value;
        const labelEl = document.getElementById(`editSrv_acctLabel_${serverId}`);
        if (labelEl) labelEl.textContent = this._SERVER_ACCT_LABELS[type] || 'Account';
    },

    toggleServerCard(serverId) {
        this._expandedServerId = (this._expandedServerId === serverId) ? null : serverId;
        this.renderMain();
    },

    async saveServerEdits(serverId) {
        const get = (field) => document.getElementById(`editSrv_${field}_${serverId}`)?.value.trim() || '';
        try {
            await DDLC.api.put(`/api/sessions/${this.sessionId}/contract/servers/${serverId}`, {
                type: get('type') || 'snowflake',
                environment: get('env') || 'prod',
                account: get('account') || null,
                host: get('host') || null,
                database: get('database') || null,
                schema_name: get('schema') || null,
                description: get('desc') || null,
                connection_qualified_name: get('conn_qn') || null,
            });
            DDLC.toast.show('Server updated');
            this._expandedServerId = null;
            await this.load();
        } catch (err) {
            DDLC.toast.show(err.message, 'error');
        }
    },

    showAddServer() {
        const el = document.getElementById('addServerForm');
        if (!el) return;
        const typeOpts = this._SERVER_TYPES.map(t => `<option value="${t.value}">${t.label}</option>`).join('');
        const envOpts = this._SERVER_ENVS.map(e => `<option value="${e}">${e}</option>`).join('');
        const connPickerHtml = this.atlanConfigured ? `
            <div style="position:relative; margin-bottom:8px;">
                <label>Atlan Connection</label>
                <input id="addSrvConnSearch" autocomplete="off"
                       placeholder="Search Atlan connections…"
                       oninput="ContractApp.onConnectionSearch('add', 'add')"
                       onblur="setTimeout(()=>ContractApp.hideConnectionDropdown('add', 'add'), 200)"
                       style="width:100%;">
                <div id="connDropdown_add_add"
                     style="display:none; position:absolute; top:100%; left:0; right:0; z-index:1500;
                            background:var(--bg-card); border:1px solid var(--border-focus);
                            border-radius:var(--radius); max-height:180px; overflow-y:auto;
                            box-shadow:0 4px 12px rgba(0,0,0,0.3);"></div>
                <input type="hidden" id="addSrvConnQn" value="">
            </div>` : '';
        el.innerHTML = `
            <div class="section-inline-form" style="margin-bottom:12px;">
                <div class="inline-form-row">
                    <div>
                        <label>Type</label>
                        <select id="addSrvType" onchange="ContractApp.onAddServerTypeChange()">${typeOpts}</select>
                    </div>
                    <div>
                        <label>Environment</label>
                        <select id="addSrvEnv">${envOpts}</select>
                    </div>
                </div>
                ${connPickerHtml}
                <div class="inline-form-row">
                    <div>
                        <label id="addSrvAcctLabel">Account</label>
                        <input id="addSrvAccount" placeholder="e.g. myorg.us-east-1">
                    </div>
                    <div>
                        <label>Host / URL</label>
                        <input id="addSrvHost" placeholder="e.g. abc123.snowflakecomputing.com">
                    </div>
                </div>
                <div class="inline-form-row">
                    <div>
                        <label>Database</label>
                        <input id="addSrvDatabase" placeholder="e.g. ANALYTICS_DB">
                    </div>
                    <div>
                        <label>Schema</label>
                        <input id="addSrvSchema" placeholder="e.g. MARTS">
                    </div>
                </div>
                <div>
                    <label>Description</label>
                    <textarea id="addSrvDesc" rows="2" placeholder="Notes about this connection..."></textarea>
                </div>
                <div class="form-actions">
                    <button class="btn btn-sm" onclick="document.getElementById('addServerForm').innerHTML=''">Cancel</button>
                    <button class="btn btn-sm btn-primary" onclick="ContractApp.addServer()">Add Server</button>
                </div>
            </div>
        `;
        el.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
    },

    onAddServerTypeChange() {
        const type = document.getElementById('addSrvType')?.value;
        const labelEl = document.getElementById('addSrvAcctLabel');
        if (labelEl) labelEl.textContent = this._SERVER_ACCT_LABELS[type] || 'Account';
    },

    async addServer() {
        const type = document.getElementById('addSrvType')?.value || 'snowflake';
        const environment = document.getElementById('addSrvEnv')?.value || 'prod';
        try {
            await DDLC.api.post(`/api/sessions/${this.sessionId}/contract/servers`, {
                type,
                environment,
                account: document.getElementById('addSrvAccount')?.value.trim() || null,
                host: document.getElementById('addSrvHost')?.value.trim() || null,
                database: document.getElementById('addSrvDatabase')?.value.trim() || null,
                schema_name: document.getElementById('addSrvSchema')?.value.trim() || null,
                description: document.getElementById('addSrvDesc')?.value.trim() || null,
                connection_qualified_name: document.getElementById('addSrvConnQn')?.value.trim() || null,
            });
            DDLC.toast.show('Server added');
            await this.load();
        } catch (err) {
            DDLC.toast.show(err.message, 'error');
        }
    },

    async deleteServer(serverId) {
        try {
            await DDLC.api.del(`/api/sessions/${this.sessionId}/contract/servers/${serverId}`);
            DDLC.toast.show('Server deleted');
            await this.load();
        } catch (err) {
            DDLC.toast.show(err.message, 'error');
        }
    },

    // --- Connection picker helpers ---
    _connSearchTimer: null,

    onConnectionSearch(serverId, mode) {
        clearTimeout(this._connSearchTimer);
        this._connSearchTimer = setTimeout(async () => {
            const inputId = mode === 'add' ? 'addSrvConnSearch' : `editSrv_conn_search_${serverId}`;
            const q = (document.getElementById(inputId)?.value || '').trim();
            try {
                const url = `/api/atlan/search-connections?q=${encodeURIComponent(q)}&limit=15`;
                const data = await DDLC.api.fetchJSON(url);
                this.renderConnectionDropdown(serverId, mode, data.connections || []);
            } catch (err) {
                console.warn('Connection search failed:', err.message);
            }
        }, 300);
    },

    renderConnectionDropdown(serverId, mode, connections) {
        const ddId = `connDropdown_${mode}_${serverId}`;
        const dd = document.getElementById(ddId);
        if (!dd) return;
        if (!connections.length) { dd.style.display = 'none'; return; }
        dd.innerHTML = connections.map(c => {
            const payload = JSON.stringify(c).replace(/\\/g, '\\\\').replace(/'/g, "\\'");
            return `<div style="padding:8px 12px; cursor:pointer; font-size:0.82rem;
                               border-bottom:1px solid var(--border); transition:background 0.15s;"
                         onmousedown="ContractApp.selectConnection('${serverId}', '${mode}', JSON.parse('${payload.replace(/"/g, '&quot;')}'))"
                         onmouseover="this.style.background='var(--bg-input)'"
                         onmouseout="this.style.background=''">
                <div style="font-weight:600; color:var(--text);">${this.esc(c.name)}</div>
                <div style="font-size:0.72rem; color:var(--text-muted); margin-top:2px;">${this.esc(c.connector_name || '')}</div>
                <div style="font-size:0.7rem; color:var(--text-dim); font-family:var(--font-mono); margin-top:2px;">${this.esc(c.qualified_name || '')}</div>
            </div>`;
        }).join('');
        dd.style.display = 'block';
    },

    selectConnection(serverId, mode, conn) {
        if (mode === 'add') {
            const si = document.getElementById('addSrvConnSearch');
            const hi = document.getElementById('addSrvConnQn');
            if (si) si.value = conn.name || '';
            if (hi) hi.value = conn.qualified_name || '';
        } else {
            const si = document.getElementById(`editSrv_conn_search_${serverId}`);
            const hi = document.getElementById(`editSrv_conn_qn_${serverId}`);
            if (si) si.value = conn.name || '';
            if (hi) hi.value = conn.qualified_name || '';
        }
        this.hideConnectionDropdown(serverId, mode);
    },

    hideConnectionDropdown(serverId, mode) {
        const dd = document.getElementById(`connDropdown_${mode}_${serverId}`);
        if (dd) dd.style.display = 'none';
    },

    // --- Roles & Access Control section ---

    renderRolesSection() {
        const roles = this.session.contract.roles || [];
        const expandedId = this._expandedRoleId;
        return `
            <div class="section-panel">
                <div class="section-header">
                    <h3>Roles & Access Control (${roles.length})</h3>
                    <button class="btn btn-sm btn-primary" onclick="ContractApp.showAddRole()">+ Add Role</button>
                </div>
                <div class="section-body" id="roleBody">
                    <div id="addRoleForm"></div>
                    ${roles.length === 0
                        ? '<div style="color:var(--text-dim); font-size:0.85rem; text-align:center; padding:12px;">No roles defined. Add roles to specify who can access this asset.</div>'
                        : ''}
                    ${roles.map(r => this.renderRoleCard(r, expandedId === r.id)).join('')}
                </div>
            </div>
        `;
    },

    renderRoleCard(r, isExpanded) {
        const accessInfo = this._ACCESS_LEVELS.find(a => a.value === r.access) || { label: r.access, color: 'var(--text-dim)' };
        const approvers = r.approvers || [];
        return `
            <div class="role-card ${isExpanded ? 'expanded' : ''}">
                <div class="role-card-header" onclick="ContractApp.toggleRoleCard('${r.id}')">
                    <div class="role-card-name">${this.esc(r.role)}</div>
                    <span class="col-badge role-card-access"
                        style="background:${accessInfo.color}22; color:${accessInfo.color};">
                        ${accessInfo.label}
                    </span>
                    <div class="role-card-desc">${r.description ? this.esc(r.description) : ''}</div>
                    <div class="role-card-meta">
                        ${approvers.slice(0, 3).map(a =>
                            `<span class="role-meta-tag">👤 ${this.esc(a.display_name || a.email)}</span>`
                        ).join('')}
                        ${approvers.length > 3 ? `<span class="role-meta-tag">+${approvers.length - 3} more</span>` : ''}
                    </div>
                    <div class="role-card-actions">
                        <button class="btn-icon" onclick="event.stopPropagation(); ContractApp.deleteRole('${r.id}')">&#128465;</button>
                    </div>
                </div>
                ${isExpanded ? `<div class="role-edit-body">${this.renderRoleEditForm(r)}</div>` : ''}
            </div>
        `;
    },

    renderRoleEditForm(r) {
        const accessOpts = this._ACCESS_LEVELS.map(a =>
            `<option value="${a.value}" ${r.access === a.value ? 'selected' : ''}>${a.label}</option>`
        ).join('');
        if (!this._pendingApprovers[r.id]) {
            this._pendingApprovers[r.id] = [...(r.approvers || [])];
        }
        const pending = this._pendingApprovers[r.id];
        const approverTagsHtml = pending.map(a => `
            <span class="approver-tag" data-guid="${this.esc(a.guid || '')}" data-email="${this.esc(a.email || '')}">
                ${this.esc(a.display_name || a.email)}
                <button onclick="ContractApp.removeApprover('${r.id}', '${this.esc(a.guid || a.email)}')" type="button">×</button>
            </span>
        `).join('');
        return `
            <div class="inline-form-row">
                <div><label>Role Name</label>
                    <input id="editRole_name_${r.id}" value="${this.esc(r.role)}" placeholder="e.g. Data Consumer"></div>
                <div><label>Access Level</label>
                    <select id="editRole_access_${r.id}">${accessOpts}</select></div>
            </div>
            <div>
                <label>Approvers (Atlan users)</label>
                <div class="approver-picker">
                    <input class="approver-picker-input" id="approverSearch_${r.id}"
                        placeholder="${this.atlanConfigured ? 'Search Atlan users by name or email…' : 'Enter email address and press Enter'}"
                        oninput="ContractApp.onApproverSearch('${r.id}')"
                        onkeydown="ContractApp.onApproverKeydown(event, '${r.id}')"
                        onblur="setTimeout(()=>{const d=document.getElementById('approverDropdown_${r.id}');if(d)d.style.display='none';},200)">
                    <div class="approver-dropdown" id="approverDropdown_${r.id}" style="display:none;"></div>
                </div>
                <div class="approver-tags" id="approverTags_${r.id}">${approverTagsHtml}</div>
            </div>
            <div><label>Description</label>
                <textarea id="editRole_desc_${r.id}" rows="2" placeholder="Who uses this role and for what purpose...">${this.esc(r.description || '')}</textarea></div>
            <div class="form-actions">
                <button class="btn btn-sm" onclick="ContractApp.toggleRoleCard('${r.id}')">Cancel</button>
                <button class="btn btn-sm btn-primary" onclick="ContractApp.saveRoleEdits('${r.id}')">Save Changes</button>
            </div>
        `;
    },

    onApproverSearch(roleId) {
        clearTimeout(this._approverSearchTimer);
        const q = document.getElementById(`approverSearch_${roleId}`)?.value.trim();
        if (!q || !this.atlanConfigured) return;
        this._approverSearchTimer = setTimeout(async () => {
            try {
                const data = await DDLC.api.fetchJSON(`/api/atlan/search-users?q=${encodeURIComponent(q)}&limit=10`);
                this.renderApproverDropdown(roleId, data.users || []);
            } catch (err) { console.warn('Approver search failed:', err.message); }
        }, 300);
    },

    onApproverKeydown(event, roleId) {
        if (event.key === 'Enter' && !this.atlanConfigured) {
            event.preventDefault();
            const email = event.target.value.trim();
            if (email) {
                this.addApproverToForm(roleId, { username: email, email, guid: '', display_name: email });
                event.target.value = '';
            }
        }
        if (event.key === 'Escape') {
            const dd = document.getElementById(`approverDropdown_${roleId}`);
            if (dd) dd.style.display = 'none';
        }
    },

    renderApproverDropdown(roleId, users) {
        const dropdown = document.getElementById(`approverDropdown_${roleId}`);
        if (!dropdown) return;
        if (!users.length) { dropdown.style.display = 'none'; return; }
        dropdown.innerHTML = users.map(u => {
            const userJson = JSON.stringify(u).replace(/'/g, '&#39;');
            return `<div class="approver-option" onclick="ContractApp.selectApprover('${roleId}', '${userJson.replace(/"/g, '&quot;')}')">
                <span class="approver-option-name">${this.esc(u.display_name || u.username)}</span>
                <span class="approver-option-email">${this.esc(u.email)}</span>
            </div>`;
        }).join('');
        dropdown.style.display = 'block';
    },

    selectApprover(roleId, userJson) {
        try {
            const user = JSON.parse(userJson.replace(/&quot;/g, '"').replace(/&#39;/g, "'"));
            this.addApproverToForm(roleId, user);
        } catch (e) { /* ignore parse errors */ }
        const input = document.getElementById(`approverSearch_${roleId}`);
        if (input) input.value = '';
        const dd = document.getElementById(`approverDropdown_${roleId}`);
        if (dd) dd.style.display = 'none';
    },

    addApproverToForm(roleId, user) {
        if (!this._pendingApprovers[roleId]) this._pendingApprovers[roleId] = [];
        const existing = this._pendingApprovers[roleId];
        if (existing.some(a => a.email === user.email)) return;
        existing.push(user);
        const container = document.getElementById(`approverTags_${roleId}`);
        if (container) {
            const tag = document.createElement('span');
            tag.className = 'approver-tag';
            tag.dataset.guid = user.guid || '';
            tag.dataset.email = user.email || '';
            const identifier = user.guid || user.email;
            tag.innerHTML = `${this.esc(user.display_name || user.email)}<button onclick="ContractApp.removeApprover('${roleId}', '${identifier.replace(/'/g, "\\'")}')">×</button>`;
            container.appendChild(tag);
        }
    },

    removeApprover(roleId, identifier) {
        if (!this._pendingApprovers[roleId]) return;
        this._pendingApprovers[roleId] = this._pendingApprovers[roleId]
            .filter(a => a.guid !== identifier && a.email !== identifier);
        const container = document.getElementById(`approverTags_${roleId}`);
        if (container) {
            container.querySelectorAll('.approver-tag').forEach(tag => {
                if (tag.dataset.guid === identifier || tag.dataset.email === identifier) {
                    tag.remove();
                }
            });
        }
    },

    toggleRoleCard(roleId) {
        if (this._expandedRoleId === roleId) {
            this._expandedRoleId = null;
            delete this._pendingApprovers[roleId];
        } else {
            this._expandedRoleId = roleId;
        }
        this.renderMain();
    },

    async saveRoleEdits(roleId) {
        const name = document.getElementById(`editRole_name_${roleId}`)?.value.trim();
        if (!name) { DDLC.toast.show('Role name is required', 'error'); return; }
        const approvers = this._pendingApprovers[roleId] || [];
        try {
            await DDLC.api.put(`/api/sessions/${this.sessionId}/contract/roles/${roleId}`, {
                role: name,
                access: document.getElementById(`editRole_access_${roleId}`)?.value || 'read',
                approvers,
                description: document.getElementById(`editRole_desc_${roleId}`)?.value.trim() || null,
            });
            delete this._pendingApprovers[roleId];
            DDLC.toast.show('Role updated');
            await this.load();
        } catch (err) {
            DDLC.toast.show(err.message, 'error');
        }
    },

    showAddRole() {
        const accessOpts = this._ACCESS_LEVELS.map(a =>
            `<option value="${a.value}">${a.label}</option>`
        ).join('');
        if (!this._pendingApprovers['_new']) this._pendingApprovers['_new'] = [];
        const form = document.getElementById('addRoleForm');
        if (!form) return;
        form.innerHTML = `
            <div class="role-edit-body" style="margin-bottom:12px; border:1px solid var(--border); border-radius:var(--radius);">
                <div class="inline-form-row">
                    <div><label>Role Name</label>
                        <input id="newRole_name" placeholder="e.g. Data Consumer"></div>
                    <div><label>Access Level</label>
                        <select id="newRole_access">${accessOpts}</select></div>
                </div>
                <div>
                    <label>Approvers (Atlan users)</label>
                    <div class="approver-picker">
                        <input class="approver-picker-input" id="approverSearch__new"
                            placeholder="${this.atlanConfigured ? 'Search Atlan users…' : 'Enter email and press Enter'}"
                            oninput="ContractApp.onApproverSearch('_new')"
                            onkeydown="ContractApp.onApproverKeydown(event, '_new')"
                            onblur="setTimeout(()=>{const d=document.getElementById('approverDropdown__new');if(d)d.style.display='none';},200)">
                        <div class="approver-dropdown" id="approverDropdown__new" style="display:none;"></div>
                    </div>
                    <div class="approver-tags" id="approverTags__new"></div>
                </div>
                <div><label>Description (optional)</label>
                    <textarea id="newRole_desc" rows="2" placeholder="Who uses this role..."></textarea></div>
                <div class="form-actions">
                    <button class="btn btn-sm" onclick="document.getElementById('addRoleForm').innerHTML=''; delete ContractApp._pendingApprovers['_new'];">Cancel</button>
                    <button class="btn btn-sm btn-primary" onclick="ContractApp.addRole()">Add Role</button>
                </div>
            </div>
        `;
    },

    async addRole() {
        const name = document.getElementById('newRole_name')?.value.trim();
        if (!name) { DDLC.toast.show('Role name is required', 'error'); return; }
        const approvers = this._pendingApprovers['_new'] || [];
        try {
            await DDLC.api.post(`/api/sessions/${this.sessionId}/contract/roles`, {
                role: name,
                access: document.getElementById('newRole_access')?.value || 'read',
                approvers,
                description: document.getElementById('newRole_desc')?.value.trim() || null,
            });
            delete this._pendingApprovers['_new'];
            DDLC.toast.show('Role added');
            await this.load();
        } catch (err) {
            DDLC.toast.show(err.message, 'error');
        }
    },

    async deleteRole(roleId) {
        try {
            await DDLC.api.del(`/api/sessions/${this.sessionId}/contract/roles/${roleId}`);
            DDLC.toast.show('Role removed');
            await this.load();
        } catch (err) {
            DDLC.toast.show(err.message, 'error');
        }
    },

    // --- Custom Properties section ---

    renderCustomPropertiesSection() {
        const props = this.session.contract.custom_properties || [];
        return `
            <div class="section-panel">
                <div class="section-header">
                    <h3>Custom Properties (${props.length})</h3>
                </div>
                <div class="section-body">
                    ${props.length === 0
                        ? '<div style="color:var(--text-dim); font-size:0.85rem; padding:4px 0 8px;">No custom properties yet.</div>'
                        : ''}
                    ${props.map(p => `
                        <div class="customprop-row">
                            <span class="customprop-key">${this.esc(p.key)}</span>
                            <span class="customprop-sep">:</span>
                            <span class="customprop-value">${this.esc(p.value)}</span>
                            <button class="btn-icon" style="margin-left:auto;" onclick="ContractApp.deleteCustomProperty('${p.id}')">&#128465;</button>
                        </div>
                    `).join('')}
                    <div style="display:flex; gap:8px; margin-top:8px; align-items:flex-end;">
                        <div style="flex:1;">
                            <div style="font-size:0.72rem; color:var(--text-muted); margin-bottom:4px;">Key</div>
                            <input id="newCustomProp_key" style="width:100%; padding:7px 10px; background:var(--bg-input); border:1px solid var(--border); border-radius:var(--radius); color:var(--text); font-family:var(--font-mono); font-size:0.82rem;" placeholder="cost_center">
                        </div>
                        <div style="flex:2;">
                            <div style="font-size:0.72rem; color:var(--text-muted); margin-bottom:4px;">Value</div>
                            <input id="newCustomProp_value" style="width:100%; padding:7px 10px; background:var(--bg-input); border:1px solid var(--border); border-radius:var(--radius); color:var(--text); font-family:var(--font-mono); font-size:0.82rem;" placeholder="eng-platform">
                        </div>
                        <button class="btn btn-sm btn-primary" onclick="ContractApp.addCustomProperty()" style="flex-shrink:0;">+ Add</button>
                    </div>
                </div>
            </div>
        `;
    },

    async addCustomProperty() {
        const key = document.getElementById('newCustomProp_key')?.value.trim();
        const value = document.getElementById('newCustomProp_value')?.value.trim();
        if (!key || !value) { DDLC.toast.show('Key and value are required', 'error'); return; }
        try {
            await DDLC.api.post(`/api/sessions/${this.sessionId}/contract/custom-properties`, { key, value });
            DDLC.toast.show('Custom property added');
            await this.load();
        } catch (err) {
            DDLC.toast.show(err.message, 'error');
        }
    },

    async deleteCustomProperty(propId) {
        try {
            await DDLC.api.del(`/api/sessions/${this.sessionId}/contract/custom-properties/${propId}`);
            DDLC.toast.show('Property removed');
            await this.load();
        } catch (err) {
            DDLC.toast.show(err.message, 'error');
        }
    },

    // --- Team section ---
    renderTeamSection() {
        const team = this.session.contract.team || [];
        return `
            <div class="section-panel">
                <div class="section-header">
                    <h3>Team (${team.length})</h3>
                    <button class="btn btn-sm btn-primary" onclick="ContractApp.showAddTeam()">+ Add Member</button>
                </div>
                <div class="section-body" id="teamBody">
                    <div id="addTeamForm"></div>
                    ${team.length === 0 ? '<div style="color:var(--text-dim); font-size:0.85rem; text-align:center; padding:12px;">No team members defined.</div>' : ''}
                    ${team.map((t, i) => `
                        <div style="display:flex; align-items:center; justify-content:space-between; padding:8px 12px; background:var(--bg-input); border-radius:var(--radius); margin-bottom:6px;">
                            <div>
                                <strong style="font-size:0.82rem;">${this.esc(t.name)}</strong>
                                <span style="font-size:0.78rem; color:var(--text-dim); margin-left:8px;">${this.esc(t.email)}</span>
                                <span class="col-badge" style="background:rgba(99,102,241,0.15); color:var(--accent); margin-left:8px;">${this.esc(t.role)}</span>
                            </div>
                            <button class="btn-icon" onclick="ContractApp.deleteTeamMember(${i})">&#128465;</button>
                        </div>
                    `).join('')}
                </div>
            </div>
        `;
    },

    showAddTeam() {
        document.getElementById('addTeamForm').innerHTML = `
            <div class="inline-form">
                <div class="inline-form-row" style="grid-template-columns:1fr 1fr 1fr;">
                    <div><label>Name *</label><input id="newTeamName" placeholder="Jane Smith"></div>
                    <div><label>Email *</label><input id="newTeamEmail" placeholder="jane@company.com"></div>
                    <div><label>Role *</label>
                        <select id="newTeamRole">
                            <option value="Data Owner">Data Owner</option>
                            <option value="Data Steward">Data Steward</option>
                            <option value="Data Engineer">Data Engineer</option>
                            <option value="Data Consumer">Data Consumer</option>
                            <option value="Reviewer">Reviewer</option>
                        </select>
                    </div>
                </div>
                <div class="form-actions">
                    <button class="btn btn-sm" onclick="ContractApp.renderMain()">Cancel</button>
                    <button class="btn btn-primary btn-sm" onclick="ContractApp.addTeamMember()">Add Member</button>
                </div>
            </div>
        `;
    },

    async addTeamMember() {
        const name = document.getElementById('newTeamName').value.trim();
        const email = document.getElementById('newTeamEmail').value.trim();
        if (!name || !email) return DDLC.toast.show('Name and email are required', 'error');
        try {
            await DDLC.api.post(`/api/sessions/${this.sessionId}/contract/team`, {
                name,
                email,
                role: document.getElementById('newTeamRole').value,
            });
            DDLC.toast.show('Team member added');
            await this.load();
        } catch (err) {
            DDLC.toast.show(err.message, 'error');
        }
    },

    async deleteTeamMember(idx) {
        try {
            await DDLC.api.del(`/api/sessions/${this.sessionId}/contract/team/${idx}`);
            DDLC.toast.show('Team member removed');
            await this.load();
        } catch (err) {
            DDLC.toast.show(err.message, 'error');
        }
    },

    // --- Review stage ---
    renderReviewStage() {
        return `
            ${this.renderReadOnlyContract()}
            ${this.renderComments()}
        `;
    },

    renderReadOnlyContract() {
        const c = this.session.contract;
        const objects = c.schema_objects || [];
        const quality = c.quality_checks || [];
        const slas = c.sla_properties || [];
        const servers = c.servers || [];
        const roles = c.roles || [];
        const customProps = c.custom_properties || [];

        let schemaHtml = objects.map(obj => {
            const props = obj.properties || [];
            return `
                <div style="margin-bottom:16px;">
                    <h4 style="font-size:0.85rem; color:var(--accent); margin-bottom:8px;">${this.esc(obj.name)}</h4>
                    ${obj.description ? `<p style="font-size:0.78rem; color:var(--text-muted); margin-bottom:8px;">${this.esc(obj.description)}</p>` : ''}
                    ${props.length > 0 ? `
                        <table class="columns-table">
                            <thead><tr><th>Column</th><th>Type</th><th>Constraints</th><th>Classification</th><th>Source</th></tr></thead>
                            <tbody>${props.map(p => {
                                const badges = [];
                                if (p.primary_key) badges.push('<span class="col-badge pk">PK</span>');
                                if (p.required) badges.push('<span class="col-badge req">REQ</span>');
                                if (p.unique) badges.push('<span class="col-badge uniq">UNQ</span>');
                                const srcs = (p.sources || []).map(s =>
                                    `<span style="font-size:0.7rem; color:var(--info);">${this.esc(s.source_table)}.${this.esc(s.source_column)}</span>`
                                ).join(', ') || '—';
                                return `<tr>
                                    <td class="col-name">${this.esc(p.name)}</td>
                                    <td class="col-type">${p.logical_type}</td>
                                    <td>${badges.join('') || '—'}</td>
                                    <td>${p.classification ? `<span class="col-badge pii">${this.esc(p.classification)}</span>` : '—'}</td>
                                    <td>${srcs}</td>
                                </tr>`;
                            }).join('')}</tbody>
                        </table>
                    ` : '<div style="color:var(--text-dim); font-size:0.82rem;">No columns</div>'}
                </div>
            `;
        }).join('');

        return `
            <div class="section-panel">
                <div class="section-header"><h3>Contract Review</h3></div>
                <div class="section-body">
                    <div style="display:grid; grid-template-columns:1fr 1fr; gap:12px; margin-bottom:16px;">
                        <div class="request-field"><div class="field-label">Name</div><div class="field-value">${this.esc(c.name || 'Not set')}</div></div>
                        <div class="request-field"><div class="field-label">Domain</div><div class="field-value">${this.esc(c.domain || 'Not set')}</div></div>
                    </div>
                    ${c.description_purpose ? `<div class="request-field" style="margin-bottom:16px;"><div class="field-label">Purpose</div><div class="field-value">${this.esc(c.description_purpose)}</div></div>` : ''}
                    <h4 style="font-size:0.82rem; color:var(--text-muted); text-transform:uppercase; margin-bottom:12px;">Schema</h4>
                    ${schemaHtml || '<div style="color:var(--text-dim);">No schema defined</div>'}
                    ${quality.length > 0 ? `
                        <h4 style="font-size:0.82rem; color:var(--text-muted); text-transform:uppercase; margin:16px 0 8px;">Quality Rules</h4>
                        ${quality.map(q => {
                            const sevColors = { critical: '#ef4444', high: '#f59e0b', medium: '#3b82f6', low: '#6b7280' };
                            const methodLbl = (this._QUALITY_METHODS.find(m => m.value === q.method) || {}).label || q.method;
                            return `<div style="padding:8px 0; font-size:0.82rem; border-bottom:1px solid var(--border);">
                                <div>
                                    ${q.method ? `<span class="col-badge" style="background:rgba(16,185,129,0.15); color:#10b981;">${this.esc(methodLbl)}</span>` : ''}
                                    <span class="col-badge" style="background:rgba(99,102,241,0.15); color:var(--accent);">${q.type}</span>
                                    ${q.dimension ? `<span class="col-badge" style="background:rgba(59,130,246,0.15); color:var(--info);">${this.esc(q.dimension)}</span>` : ''}
                                    ${q.severity ? `<span class="col-badge" style="background:${sevColors[q.severity] || 'gray'}22; color:${sevColors[q.severity] || 'gray'};">${this.esc(q.severity)}</span>` : ''}
                                    ${this.esc(q.description)}
                                </div>
                                ${(q.column || q.schedule || q.business_impact) ? `<div style="font-size:0.75rem; color:var(--text-dim); margin-top:4px;">
                                    ${q.column ? `Column: <code>${this.esc(q.column)}</code>` : ''}
                                    ${q.schedule ? ` &middot; Schedule: ${this.esc(q.schedule)}` : ''}
                                    ${q.business_impact ? `<div style="margin-top:2px; color:var(--warning);">Impact: ${this.esc(q.business_impact)}</div>` : ''}
                                </div>` : ''}
                            </div>`;
                        }).join('')}
                    ` : ''}
                    ${slas.length > 0 ? `
                        <h4 style="font-size:0.82rem; color:var(--text-muted); text-transform:uppercase; margin:16px 0 8px;">SLAs</h4>
                        ${slas.map(s => {
                            const propLbl = (this._SLA_PROPERTIES.find(p => p.value === s.property) || {}).label || s.property;
                            return `<div style="padding:6px 0; font-size:0.82rem;">
                                <strong>${this.esc(propLbl)}</strong>: ${this.esc(s.value)} ${s.unit ? this.esc(s.unit) : ''}
                                ${s.schedule ? `<span style="margin-left:8px; font-size:0.72rem; color:var(--text-dim);">🕓 ${this.esc(s.schedule)}</span>` : ''}
                                ${s.driver ? `<span style="margin-left:8px; font-size:0.72rem; color:var(--text-dim);">📋 ${this.esc(s.driver)}</span>` : ''}
                                ${s.element ? `<span style="margin-left:8px; font-size:0.72rem; color:var(--text-dim);">📌 ${this.esc(s.element)}</span>` : ''}
                                ${s.scheduler ? `<span style="margin-left:8px; font-size:0.72rem; color:var(--text-dim);">⚙ ${this.esc(s.scheduler)}</span>` : ''}
                                ${s.description ? `<div style="margin-top:2px; font-size:0.75rem; color:var(--text-muted);">${this.esc(s.description)}</div>` : ''}
                            </div>`;
                        }).join('')}
                    ` : ''}
                    ${servers.length > 0 ? `
                        <h4 style="font-size:0.82rem; color:var(--text-muted); text-transform:uppercase; margin:16px 0 8px;">Infrastructure</h4>
                        ${servers.map(s => {
                            const typeLabel = (this._SERVER_TYPES.find(t => t.value === s.type) || {}).label || s.type;
                            const envColor = this._SERVER_ENV_COLORS[s.environment] || 'var(--text-dim)';
                            const connSummary = [s.database, s.schema_name].filter(Boolean).join('.');
                            return `<div style="padding:6px 0; font-size:0.82rem; border-bottom:1px solid var(--border);">
                                <span class="col-badge" style="background:${envColor}22; color:${envColor};">${this.esc(s.environment)}</span>
                                <strong>${this.esc(typeLabel)}</strong>
                                ${connSummary ? `<span style="font-family:var(--font-mono); margin-left:6px; color:var(--text-muted);">${this.esc(connSummary)}</span>` : ''}
                                ${s.account ? `<span style="margin-left:8px; font-size:0.72rem; color:var(--text-dim);">&#127968; ${this.esc(s.account)}</span>` : ''}
                                ${s.host ? `<span style="margin-left:8px; font-size:0.72rem; color:var(--text-dim);">&#128279; ${this.esc(s.host)}</span>` : ''}
                                ${s.description ? `<div style="margin-top:2px; font-size:0.75rem; color:var(--text-muted);">${this.esc(s.description)}</div>` : ''}
                            </div>`;
                        }).join('')}
                    ` : ''}
                    ${roles.length > 0 ? `
                        <h4 style="font-size:0.82rem; color:var(--text-muted); text-transform:uppercase; margin:16px 0 8px;">Roles & Access</h4>
                        ${roles.map(r => {
                            const accessInfo = this._ACCESS_LEVELS.find(a => a.value === r.access) || { label: r.access, color: 'var(--text-dim)' };
                            const approvers = r.approvers || [];
                            return `<div style="padding:6px 0; font-size:0.82rem; border-bottom:1px solid var(--border); display:flex; align-items:center; gap:8px; flex-wrap:wrap;">
                                <span class="col-badge" style="background:${accessInfo.color}22; color:${accessInfo.color};">${accessInfo.label}</span>
                                <strong>${this.esc(r.role)}</strong>
                                ${approvers.length > 0 ? `<span style="font-size:0.72rem; color:var(--text-dim);">${approvers.map(a => this.esc(a.display_name || a.email)).join(', ')}</span>` : ''}
                                ${r.description ? `<span style="font-size:0.75rem; color:var(--text-muted); flex-basis:100%; padding-left:4px;">${this.esc(r.description)}</span>` : ''}
                            </div>`;
                        }).join('')}
                    ` : ''}
                    ${customProps.length > 0 ? `
                        <h4 style="font-size:0.82rem; color:var(--text-muted); text-transform:uppercase; margin:16px 0 8px;">Custom Properties</h4>
                        ${customProps.map(p => `
                            <div style="display:flex; gap:8px; padding:4px 0; font-size:0.82rem; font-family:var(--font-mono);">
                                <span style="color:var(--accent); font-weight:600; min-width:120px; flex-shrink:0;">${this.esc(p.key)}</span>
                                <span style="color:var(--text-dim);">:</span>
                                <span style="color:var(--text);">${this.esc(p.value)}</span>
                            </div>
                        `).join('')}
                    ` : ''}
                </div>
            </div>
        `;
    },

    // --- Approval stage ---
    renderApprovalStage() {
        return `
            ${this.renderReadOnlyContract()}
            <div class="section-panel">
                <div class="section-body">
                    <div class="approval-panel">
                        <h3>Ready for Approval</h3>
                        <p>Review the contract above. Once approved, the contract status will change to <strong>Active</strong> and the ODCS YAML will be finalized.</p>
                        <div class="approval-actions">
                            <button class="btn btn-danger" onclick="ContractApp.advanceStage('rejected')">Reject</button>
                            <button class="btn btn-success" style="padding:10px 32px; font-size:0.9rem;" onclick="ContractApp.advanceStage('active')">Approve Contract</button>
                        </div>
                    </div>
                </div>
            </div>
            ${this.renderComments()}
        `;
    },

    // --- Active stage ---
    renderActiveStage() {
        const contract = this.session?.contract || {};
        const atlanUrl = contract.atlan_table_url || null;
        const atlanQn = contract.atlan_table_qualified_name || null;
        const atlanBlock = atlanUrl ? `
            <div style="margin-top:16px; padding:12px 16px; background:var(--bg-input);
                        border:1px solid var(--border); border-radius:var(--radius);">
                <div style="font-size:0.75rem; color:var(--text-muted); margin-bottom:6px; text-transform:uppercase; letter-spacing:.05em;">
                    Registered Atlan Asset
                </div>
                ${atlanQn ? `<div style="font-size:0.78rem; font-family:var(--font-mono); color:var(--text-dim); margin-bottom:8px;">${this.esc(atlanQn)}</div>` : ''}
                <a href="${this.esc(atlanUrl)}" target="_blank" rel="noopener"
                   class="btn btn-primary btn-sm" style="text-decoration:none;">
                    View in Atlan &rarr;
                </a>
            </div>` : '';
        return `
            <div class="section-panel" style="border-color: var(--success);">
                <div class="section-body">
                    <div class="approval-panel">
                        <div style="font-size:2rem;">&#9989;</div>
                        <h3 style="color:var(--success);">Contract Active</h3>
                        <p>This data contract has been approved and is now active.</p>
                        <div class="approval-actions">
                            <button class="btn btn-primary" onclick="ContractApp.showYamlPanel()">View YAML</button>
                            <button class="btn btn-success" onclick="ContractApp.downloadYaml()">Download .odcs.yaml</button>
                        </div>
                        ${atlanBlock}
                    </div>
                </div>
            </div>
            ${this.renderReadOnlyContract()}
        `;
    },

    // --- Rejected stage ---
    renderRejectedStage() {
        return `
            <div class="section-panel" style="border-color: var(--danger);">
                <div class="section-body">
                    <div class="approval-panel">
                        <div style="font-size:2rem;">&#10060;</div>
                        <h3 style="color:var(--danger);">Contract Rejected</h3>
                        <p>This data contract has been rejected.</p>
                    </div>
                </div>
            </div>
            ${this.renderComments()}
        `;
    },

    // -----------------------------------------------------------------------
    // Comments
    // -----------------------------------------------------------------------
    renderComments() {
        const comments = this.session.comments || [];
        return `
            <div class="section-panel">
                <div class="section-header">
                    <h3>Discussion (${comments.length})</h3>
                </div>
                <div class="section-body">
                    <div class="comment-thread">
                        ${comments.length === 0 ? '<div style="color:var(--text-dim); font-size:0.85rem; text-align:center; padding:12px;">No comments yet. Start the conversation.</div>' : ''}
                        ${comments.map(c => `
                            <div class="comment-item">
                                <div class="comment-header">
                                    <span class="comment-author">${this.esc(c.author.name)}<span class="stage-badge ${c.stage} comment-stage-badge">${c.stage}</span></span>
                                    <span class="comment-time">${this.timeAgo(c.created_at)}</span>
                                </div>
                                <div class="comment-body">${this.esc(c.content)}</div>
                            </div>
                        `).join('')}
                    </div>
                    <div class="comment-form">
                        <textarea id="commentInput" placeholder="Add a comment..." rows="2"></textarea>
                        <div style="display:flex; flex-direction:column; gap:4px;">
                            <input id="commentAuthor" placeholder="Your name" style="padding:6px 10px; background:var(--bg-input); border:1px solid var(--border); border-radius:var(--radius); color:var(--text); font-size:0.8rem;">
                            <button class="btn btn-primary btn-sm" onclick="ContractApp.postComment()">Send</button>
                        </div>
                    </div>
                </div>
            </div>
        `;
    },

    async postComment() {
        const content = document.getElementById('commentInput').value.trim();
        const author = document.getElementById('commentAuthor').value.trim();
        if (!content) return DDLC.toast.show('Comment cannot be empty', 'error');
        if (!author) return DDLC.toast.show('Please enter your name', 'error');

        try {
            await DDLC.api.post(`/api/sessions/${this.sessionId}/comments`, {
                content,
                author_name: author,
                author_email: '',
            });
            DDLC.toast.show('Comment added');
            await this.load();
        } catch (err) {
            DDLC.toast.show(err.message, 'error');
        }
    },

    // -----------------------------------------------------------------------
    // Column reorder & edit
    // -----------------------------------------------------------------------
    async moveColumn(objName, propName, direction) {
        try {
            await DDLC.api.post(`/api/sessions/${this.sessionId}/contract/objects/${objName}/properties/reorder`, {
                property_name: propName,
                direction: direction,
            });
            await this.load();
        } catch (err) {
            DDLC.toast.show(err.message, 'error');
        }
    },

    // -----------------------------------------------------------------------
    // Column edit side panel
    // -----------------------------------------------------------------------
    openColumnEditor(objName, propName) {
        const objects = this.session.contract.schema_objects || [];
        const obj = objects.find(o => o.name === objName);
        if (!obj) return;
        const prop = (obj.properties || []).find(p => p.name === propName);
        if (!prop) return;

        // Remove existing panel if any
        this.closeColumnEditor(true);

        const sourceTables = obj.source_tables || [];
        const panelHtml = this.renderColumnEditPanel(objName, prop, sourceTables);

        // Add scrim + panel to body
        const scrim = document.createElement('div');
        scrim.className = 'column-edit-scrim';
        scrim.id = 'colEditScrim';
        scrim.onclick = () => this.closeColumnEditor();
        document.body.appendChild(scrim);

        const panel = document.createElement('div');
        panel.className = 'column-edit-panel';
        panel.id = 'colEditPanel';
        panel.innerHTML = panelHtml;
        document.body.appendChild(panel);

        // Trigger animation
        requestAnimationFrame(() => {
            scrim.classList.add('open');
            panel.classList.add('open');
        });
    },

    renderColumnEditPanel(objName, prop, sourceTables) {
        const escObj = this.esc(objName);
        const escProp = this.esc(prop.name);
        const sources = prop.sources || [];

        // Build source entries
        const sourceEntries = sources.map((s, i) => `
            <div class="source-entry">
                <div class="source-entry-header">
                    <span class="source-ref">${this.esc(s.source_table)}.${this.esc(s.source_column)}</span>
                    <div class="source-actions">
                        <button class="btn-icon" onclick="ContractApp.deleteSourceInEditor('${escObj}', '${escProp}', ${i})" title="Remove source">&#10005;</button>
                    </div>
                </div>
                <div class="source-entry-body">
                    <div class="field-group" style="margin-bottom:6px;">
                        <label>Transform Logic (SQL)</label>
                        <textarea id="srcTransform-${i}" rows="2">${this.esc(s.transform_logic || '')}</textarea>
                    </div>
                    <div class="field-group">
                        <label>Transform Description</label>
                        <input id="srcDesc-${i}" value="${this.esc(s.transform_description || '')}" placeholder="Human-readable explanation">
                    </div>
                </div>
            </div>
        `).join('');

        // Build source table dropdown for "add source" form
        const sourceOptions = sourceTables.length > 0
            ? sourceTables.map(s => `<option value="${this.esc(s.name)}" data-qn="${this.esc(s.qualified_name || '')}">${this.esc(s.name)}</option>`).join('') + '<option value="__custom__">Other (type manually)</option>'
            : '';

        return `
            <div class="column-edit-header">
                <h3>&#9998; Edit Column: <span style="color:var(--accent);">${escProp}</span></h3>
                <button class="close-btn" onclick="ContractApp.closeColumnEditor()">&#10005;</button>
            </div>
            <div class="column-edit-body">
                <!-- Section 1: Identity -->
                <div class="column-edit-section">
                    <h4>Identity</h4>
                    <div class="field-group">
                        <label>Column Name</label>
                        <input id="editColName" value="${escProp}">
                    </div>
                    <div class="field-row">
                        <div class="field-group">
                            <label>Logical Type</label>
                            <select id="editColType">
                                ${this.LOGICAL_TYPES.map(t => `<option value="${t}" ${prop.logical_type === t ? 'selected' : ''}>${t}</option>`).join('')}
                            </select>
                        </div>
                        <div class="field-group">
                            <label>Classification</label>
                            <select id="editColClassification">
                                ${this.CLASSIFICATIONS.map(c => `<option value="${c}" ${(prop.classification || '') === c ? 'selected' : ''}>${c || 'None'}</option>`).join('')}
                            </select>
                        </div>
                    </div>
                    <div class="field-group">
                        <label>Description</label>
                        <textarea id="editColDesc" rows="2">${this.esc(prop.description || '')}</textarea>
                    </div>
                </div>

                <!-- Section 2: Constraints -->
                <div class="column-edit-section">
                    <h4>Constraints</h4>
                    <div class="toggle-group" style="margin-bottom:12px;">
                        <label class="toggle"><input type="checkbox" id="editColRequired" ${prop.required ? 'checked' : ''}> Required</label>
                        <label class="toggle"><input type="checkbox" id="editColPK" ${prop.primary_key ? 'checked' : ''}> Primary Key</label>
                        <label class="toggle"><input type="checkbox" id="editColUnique" ${prop.unique ? 'checked' : ''}> Unique</label>
                    </div>
                    <div class="field-row">
                        <div class="field-group">
                            <label>PK Position</label>
                            <input type="number" id="editColPKPos" value="${prop.primary_key_position != null ? prop.primary_key_position : ''}" placeholder="e.g., 1" min="1">
                        </div>
                        <div></div>
                    </div>
                </div>

                <!-- Section 3: Enrichment -->
                <div class="column-edit-section">
                    <h4>Enrichment</h4>
                    <div class="toggle-group" style="margin-bottom:12px;">
                        <label class="toggle"><input type="checkbox" id="editColCDE" ${prop.critical_data_element ? 'checked' : ''}> Critical Data Element</label>
                    </div>
                    <div class="field-group">
                        <label>Examples (comma-separated)</label>
                        <input id="editColExamples" value="${this.esc((prop.examples || []).join(', '))}" placeholder="e.g., 12345, hello, 2024-01-01">
                    </div>
                </div>

                <!-- Section 4: Lineage / Sources -->
                <div class="column-edit-section">
                    <h4>Lineage / Sources (${sources.length})</h4>
                    <div id="sourceEntriesContainer">
                        ${sourceEntries || '<div style="font-size:0.8rem; color:var(--text-dim); padding:8px 0;">No sources mapped yet.</div>'}
                    </div>
                    <div style="margin-top:8px;">
                        ${sourceTables.length > 0 ? `
                            <div id="addSourceForm" style="display:none; margin-top:8px;">
                                <div class="inline-form" style="margin:0; padding:12px;">
                                    <div class="inline-form-row">
                                        <div><label>Source Table</label>
                                            <select id="newSrcTable" onchange="if(this.value==='__custom__'){document.getElementById('newSrcTableCustom').style.display='block';}else{document.getElementById('newSrcTableCustom').style.display='none';}">
                                                ${sourceOptions}
                                            </select>
                                            <input id="newSrcTableCustom" style="display:none; margin-top:4px;" placeholder="Custom table name">
                                        </div>
                                        <div><label>Source Column</label><input id="newSrcCol" placeholder="column_name"></div>
                                    </div>
                                    <div class="field-group">
                                        <label>Transform Logic (SQL)</label>
                                        <textarea id="newSrcTransform" rows="2" placeholder="e.g., COALESCE(a.col, b.col)"></textarea>
                                    </div>
                                    <div class="form-actions">
                                        <button class="btn btn-sm" onclick="document.getElementById('addSourceForm').style.display='none';">Cancel</button>
                                        <button class="btn btn-primary btn-sm" onclick="ContractApp.addSourceInEditor('${escObj}', '${escProp}')">Add Source</button>
                                    </div>
                                </div>
                            </div>
                            <button class="btn btn-sm" onclick="document.getElementById('addSourceForm').style.display='block'; this.style.display='none';">+ Add Source</button>
                        ` : `
                            <div id="addSourceForm" style="display:none; margin-top:8px;">
                                <div class="inline-form" style="margin:0; padding:12px;">
                                    <div class="inline-form-row">
                                        <div><label>Source Table</label><input id="newSrcTable" placeholder="source_table_name"></div>
                                        <div><label>Source Column</label><input id="newSrcCol" placeholder="column_name"></div>
                                    </div>
                                    <div class="field-group">
                                        <label>Transform Logic (SQL)</label>
                                        <textarea id="newSrcTransform" rows="2" placeholder="e.g., COALESCE(a.col, b.col)"></textarea>
                                    </div>
                                    <div class="form-actions">
                                        <button class="btn btn-sm" onclick="document.getElementById('addSourceForm').style.display='none';">Cancel</button>
                                        <button class="btn btn-primary btn-sm" onclick="ContractApp.addSourceInEditor('${escObj}', '${escProp}')">Add Source</button>
                                    </div>
                                </div>
                            </div>
                            <button class="btn btn-sm" onclick="document.getElementById('addSourceForm').style.display='block'; this.style.display='none';">+ Add Source</button>
                        `}
                    </div>
                </div>
            </div>
            <div class="column-edit-footer">
                <button class="btn btn-sm" onclick="ContractApp.closeColumnEditor()">Cancel</button>
                <button class="btn btn-primary btn-sm" onclick="ContractApp.saveColumnEdits('${escObj}', '${escProp}')">Save Changes</button>
            </div>
        `;
    },

    async saveColumnEdits(objName, originalPropName) {
        const nameEl = document.getElementById('editColName');
        const newName = nameEl ? nameEl.value.trim() : originalPropName;
        if (!newName) return DDLC.toast.show('Column name is required', 'error');

        const examplesRaw = document.getElementById('editColExamples')?.value.trim() || '';
        const examples = examplesRaw ? examplesRaw.split(',').map(e => e.trim()).filter(Boolean) : null;

        const pkPosRaw = document.getElementById('editColPKPos')?.value;
        const pkPosition = pkPosRaw ? parseInt(pkPosRaw, 10) : null;

        // 1. Save column metadata
        try {
            await DDLC.api.put(`/api/sessions/${this.sessionId}/contract/objects/${objName}/properties/${originalPropName}`, {
                name: newName,
                logical_type: document.getElementById('editColType')?.value || 'string',
                description: document.getElementById('editColDesc')?.value.trim() || null,
                classification: document.getElementById('editColClassification')?.value || null,
                required: document.getElementById('editColRequired')?.checked || false,
                primary_key: document.getElementById('editColPK')?.checked || false,
                unique: document.getElementById('editColUnique')?.checked || false,
                critical_data_element: document.getElementById('editColCDE')?.checked || false,
                examples: examples,
                primary_key_position: pkPosition,
            });
        } catch (err) {
            DDLC.toast.show(err.message, 'error');
            return;
        }

        // 2. Update existing source transform logic/descriptions via PUT
        // Use the (potentially renamed) name for subsequent calls
        const propNameForSrc = newName;
        const sourceEntries = document.querySelectorAll('#sourceEntriesContainer .source-entry');
        for (let i = 0; i < sourceEntries.length; i++) {
            const transformEl = document.getElementById(`srcTransform-${i}`);
            const descEl = document.getElementById(`srcDesc-${i}`);
            if (transformEl || descEl) {
                try {
                    await DDLC.api.put(`/api/sessions/${this.sessionId}/contract/objects/${objName}/properties/${propNameForSrc}/sources/${i}`, {
                        transform_logic: transformEl?.value.trim() || null,
                        transform_description: descEl?.value.trim() || null,
                    });
                } catch (err) {
                    // Non-critical — continue saving others
                    console.warn(`Failed to update source ${i}:`, err);
                }
            }
        }

        DDLC.toast.show(`Column "${newName}" updated`);
        this.closeColumnEditor(true);
        await this.load();
    },

    closeColumnEditor(immediate) {
        const scrim = document.getElementById('colEditScrim');
        const panel = document.getElementById('colEditPanel');
        if (!scrim && !panel) return;

        if (immediate) {
            scrim?.remove();
            panel?.remove();
            return;
        }

        scrim?.classList.remove('open');
        panel?.classList.remove('open');
        setTimeout(() => {
            scrim?.remove();
            panel?.remove();
        }, 260);
    },

    async addSourceInEditor(objName, propName) {
        const tableSelect = document.getElementById('newSrcTable');
        let srcTable = '';
        let srcTableQN = null;

        if (tableSelect?.tagName === 'SELECT') {
            if (tableSelect.value === '__custom__') {
                srcTable = document.getElementById('newSrcTableCustom')?.value.trim() || '';
            } else {
                srcTable = tableSelect.value;
                srcTableQN = tableSelect.selectedOptions[0]?.dataset.qn || null;
            }
        } else {
            srcTable = tableSelect?.value.trim() || '';
        }

        const srcCol = document.getElementById('newSrcCol')?.value.trim() || '';
        const transform = document.getElementById('newSrcTransform')?.value.trim() || null;

        if (!srcTable || !srcCol) {
            return DDLC.toast.show('Source table and column are required', 'error');
        }

        try {
            await DDLC.api.post(`/api/sessions/${this.sessionId}/contract/objects/${objName}/properties/${propName}/sources`, {
                source_table: srcTable,
                source_column: srcCol,
                source_table_qualified_name: srcTableQN,
                transform_logic: transform,
            });
            DDLC.toast.show('Source added');
            // Re-open the panel to refresh
            await this.load();
            this.openColumnEditor(objName, propName);
        } catch (err) {
            DDLC.toast.show(err.message, 'error');
        }
    },

    async deleteSourceInEditor(objName, propName, sourceIndex) {
        try {
            await DDLC.api.del(`/api/sessions/${this.sessionId}/contract/objects/${objName}/properties/${propName}/sources/${sourceIndex}`);
            DDLC.toast.show('Source removed');
            // Re-open the panel to refresh
            await this.load();
            this.openColumnEditor(objName, propName);
        } catch (err) {
            DDLC.toast.show(err.message, 'error');
        }
    },

    // -----------------------------------------------------------------------
    // Stage transitions
    // -----------------------------------------------------------------------
    async advanceStage(targetStage) {
        try {
            const result = await DDLC.api.put(`/api/sessions/${this.sessionId}/stage`, {
                target_stage: targetStage,
            });
            await this.load();
            if (targetStage === 'active' && result?.atlan_url) {
                DDLC.toast.show('Contract approved — asset registered in Atlan!', 'success');
            } else if (targetStage === 'active' && result?.atlan_warning) {
                DDLC.toast.show(result.atlan_warning, 'error');
            } else {
                DDLC.toast.show(`Moved to ${targetStage}`);
            }
        } catch (err) {
            DDLC.toast.show(err.message, 'error');
        }
    },

    // -----------------------------------------------------------------------
    // YAML panel
    // -----------------------------------------------------------------------
    showYamlPanel() {
        document.getElementById('yamlOverlay').style.display = 'block';
        YAMLPreview.load(this.sessionId);
    },

    hideYamlPanel() {
        document.getElementById('yamlOverlay').style.display = 'none';
    },

    async downloadYaml() {
        window.open(`/api/sessions/${this.sessionId}/contract/download`, '_blank');
    },

    // -----------------------------------------------------------------------
    // dbt Project panel
    // -----------------------------------------------------------------------

    /** Cache of {relative_path: content} from the last preview load. */
    _dbtFiles: {},

    async showDbtPanel() {
        const overlay = document.getElementById('dbtOverlay');
        overlay.style.display = 'flex';

        // Check dbt Cloud configuration and show/hide the trigger button
        try {
            const status = await DDLC.api.fetchJSON('/api/dbt/status');
            document.getElementById('dbtCloudBtn').style.display = status.configured ? '' : 'none';
        } catch (_) {
            document.getElementById('dbtCloudBtn').style.display = 'none';
        }

        await this._loadDbtPreview();
    },

    hideDbtPanel() {
        document.getElementById('dbtOverlay').style.display = 'none';
    },

    async _loadDbtPreview() {
        const tree = document.getElementById('dbtFileTree');
        const content = document.getElementById('dbtFileContent');
        tree.innerHTML = '<div class="dbt-folder-label">Loading…</div>';
        content.textContent = '';

        try {
            const data = await DDLC.api.fetchJSON(`/api/sessions/${this.sessionId}/contract/dbt/preview`);
            this._dbtFiles = data.files || {};
            this._renderDbtFileTree();
            // Auto-select the first file
            const paths = Object.keys(this._dbtFiles);
            if (paths.length > 0) {
                this._showDbtFile(paths[0]);
            }
        } catch (err) {
            tree.innerHTML = '<div class="dbt-folder-label" style="color:var(--danger)">Error loading preview</div>';
            content.textContent = String(err);
        }
    },

    _renderDbtFileTree() {
        const tree = document.getElementById('dbtFileTree');
        const paths = Object.keys(this._dbtFiles).sort();
        const html = [];

        // Separate root-level files from models/ subfolder
        const rootFiles = paths.filter(p => !p.startsWith('models/'));
        const modelFiles = paths.filter(p => p.startsWith('models/'));

        for (const p of rootFiles) {
            const name = p.split('/').pop();
            html.push(`<div class="dbt-file-item" data-path="${this.esc(p)}" onclick="ContractApp._showDbtFile('${this.esc(p)}')">${this.esc(name)}</div>`);
        }

        if (modelFiles.length > 0) {
            html.push('<div class="dbt-folder-label">models/</div>');
            for (const p of modelFiles) {
                const name = p.split('/').pop();
                html.push(`<div class="dbt-file-item" style="padding-left:20px;" data-path="${this.esc(p)}" onclick="ContractApp._showDbtFile('${this.esc(p)}')">${this.esc(name)}</div>`);
            }
        }

        tree.innerHTML = html.join('');
    },

    _showDbtFile(path) {
        const content = document.getElementById('dbtFileContent');
        content.textContent = this._dbtFiles[path] || '';

        // Toggle active class on file tree items
        document.querySelectorAll('#dbtFileTree .dbt-file-item').forEach(el => {
            el.classList.toggle('active', el.dataset.path === path);
        });
    },

    async downloadDbt() {
        window.open(`/api/sessions/${this.sessionId}/contract/dbt/download`, '_blank');
    },

    async triggerDbtCloud() {
        const btn = document.getElementById('dbtCloudBtn');
        const orig = btn.textContent;
        btn.textContent = 'Triggering…';
        btn.disabled = true;
        try {
            const result = await DDLC.api.fetchJSON(
                `/api/sessions/${this.sessionId}/contract/dbt/trigger`,
                { method: 'POST' }
            );
            DDLC.ui.toast(`dbt Cloud run triggered! Run ID: ${result.run?.data?.id || '—'}`, 'success');
        } catch (err) {
            DDLC.ui.toast(`Failed to trigger dbt Cloud run: ${err.message || err}`, 'error');
        } finally {
            btn.textContent = orig;
            btn.disabled = false;
        }
    },

    // -----------------------------------------------------------------------
    // Utilities
    // -----------------------------------------------------------------------
    esc(str) {
        if (!str) return '';
        const d = document.createElement('div');
        d.textContent = str;
        return d.innerHTML;
    },

    timeAgo(isoStr) {
        if (!isoStr) return '';
        const d = new Date(isoStr);
        const now = new Date();
        const diff = Math.floor((now - d) / 1000);
        if (diff < 60) return 'just now';
        if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
        if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
        return `${Math.floor(diff / 86400)}d ago`;
    },
};
