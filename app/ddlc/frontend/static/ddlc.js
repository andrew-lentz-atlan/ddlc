/**
 * Atlan iframe handshake — handles the postMessage protocol used by
 * SecureExternalApp.vue when DDLC is embedded as an external nav tab.
 *
 * Protocol:
 *   1. Atlan sends  → { type: 'ATLAN_HANDSHAKE', payload: { appId } }
 *   2. DDLC replies → { type: 'IFRAME_READY' }
 *   3. Atlan sends  → { type: 'ATLAN_AUTH_CONTEXT', payload: { token, user, … } }
 *   4. DDLC stores the token for future API calls
 *   5. Atlan may resend ATLAN_AUTH_CONTEXT periodically to refresh the token
 *   6. On logout Atlan sends → { type: 'ATLAN_LOGOUT' }
 */
;(function AtlanHandshake() {
    let _atlanToken = null;
    let _atlanUser  = null;

    window.__atlan = {
        getToken: () => _atlanToken,
        getUser:  () => _atlanUser,
    };

    window.addEventListener('message', function(event) {
        // Only handle messages from the parent window (Atlan frontend)
        if (event.source !== window.parent) return;

        const data = event.data;
        if (!data || typeof data.type !== 'string') return;

        switch (data.type) {
            case 'ATLAN_HANDSHAKE':
                // Respond to let Atlan know the iframe is alive
                event.source.postMessage({ type: 'IFRAME_READY' }, event.origin);
                break;

            case 'ATLAN_AUTH_CONTEXT':
                // Store auth context for use in API calls
                if (data.payload && data.payload.token) {
                    _atlanToken = data.payload.token;
                    _atlanUser  = data.payload.user || null;
                }
                break;

            case 'ATLAN_LOGOUT':
                _atlanToken = null;
                _atlanUser  = null;
                break;
        }
    });

    // Let Atlan know immediately if it already sent a handshake before our
    // listener was registered (race condition on slow page loads)
    if (window.parent !== window) {
        window.parent.postMessage({ type: 'IFRAME_READY' }, '*');
    }
})();

/**
 * DDLC — Dashboard and Request Form logic.
 */
const DDLC = {
    api: {
        async fetchJSON(url, opts = {}) {
            const resp = await fetch(url, {
                headers: { 'Content-Type': 'application/json' },
                ...opts,
            });
            if (!resp.ok) {
                const err = await resp.json().catch(() => ({ detail: resp.statusText }));
                throw new Error(err.detail || resp.statusText);
            }
            return resp.json();
        },

        async post(url, body) {
            return this.fetchJSON(url, { method: 'POST', body: JSON.stringify(body) });
        },

        async put(url, body) {
            return this.fetchJSON(url, { method: 'PUT', body: JSON.stringify(body) });
        },

        async del(url) {
            return this.fetchJSON(url, { method: 'DELETE' });
        },
    },

    toast: {
        show(message, type = 'success') {
            const el = document.getElementById('toast');
            if (!el) return;
            el.textContent = message;
            el.className = `toast ${type} show`;
            setTimeout(() => el.classList.remove('show'), 3000);
        },
    },

    // -----------------------------------------------------------------------
    // Dashboard
    // -----------------------------------------------------------------------
    dashboard: {
        currentFilter: null,
        allSessions: [],

        async init() {
            await this.load();
        },

        async load() {
            try {
                this.allSessions = await DDLC.api.fetchJSON('/api/sessions');
                this.renderStats();
                this.renderFilters();
                this.renderSessions();
            } catch (err) {
                console.error('Failed to load sessions:', err);
            }
        },

        renderStats() {
            const stats = document.getElementById('stats');
            if (!stats) return;

            const stages = ['request', 'discovery', 'specification', 'review', 'approval', 'active'];
            const counts = {};
            stages.forEach(s => counts[s] = 0);
            this.allSessions.forEach(s => {
                if (counts[s.current_stage] !== undefined) counts[s.current_stage]++;
            });

            stats.innerHTML = `
                <div class="stat-card">
                    <div class="stat-value">${this.allSessions.length}</div>
                    <div class="stat-label">Total</div>
                </div>
                ${stages.map(s => `
                    <div class="stat-card">
                        <div class="stat-value" style="color: var(--stage-${s})">${counts[s]}</div>
                        <div class="stat-label">${s}</div>
                    </div>
                `).join('')}
            `;
        },

        renderFilters() {
            const el = document.getElementById('filters');
            if (!el) return;

            const stages = [null, 'request', 'discovery', 'specification', 'review', 'approval', 'active'];
            const labels = ['All', 'Request', 'Discovery', 'Specification', 'Review', 'Approval', 'Active'];

            el.innerHTML = stages.map((s, i) => `
                <button class="filter-tab ${s === this.currentFilter ? 'active' : ''}"
                        onclick="DDLC.dashboard.filter('${s}')">
                    ${labels[i]}
                </button>
            `).join('');
        },

        filter(stage) {
            this.currentFilter = stage === 'null' ? null : stage;
            this.renderFilters();
            this.renderSessions();
        },

        renderSessions() {
            const el = document.getElementById('sessions');
            if (!el) return;

            let sessions = this.allSessions;
            if (this.currentFilter) {
                sessions = sessions.filter(s => s.current_stage === this.currentFilter);
            }

            if (sessions.length === 0) {
                el.innerHTML = `
                    <div class="empty-state">
                        <h3>No contracts yet</h3>
                        <p>Submit a request to start the Data Contract Development Lifecycle.</p>
                        <a href="/request" class="btn btn-primary">+ New Request</a>
                    </div>
                `;
                return;
            }

            el.innerHTML = sessions.map(s => `
                <a href="/contract/${s.id}" class="session-row">
                    <div>
                        <div class="session-title">${this.esc(s.title)}</div>
                        <div class="session-domain">${s.domain || 'No domain'} ${s.data_product ? '/ ' + this.esc(s.data_product) : ''}</div>
                    </div>
                    <span class="stage-badge ${s.current_stage}">${s.current_stage}</span>
                    <span class="urgency-badge ${s.urgency}">${s.urgency}</span>
                    <span class="session-meta">${this.esc(s.requester_name)}</span>
                    <span class="session-meta">${this.timeAgo(s.created_at)}</span>
                </a>
            `).join('');
        },

        esc(str) {
            if (!str) return '';
            const d = document.createElement('div');
            d.textContent = str;
            return d.innerHTML;
        },

        timeAgo(isoStr) {
            const d = new Date(isoStr);
            const now = new Date();
            const diff = Math.floor((now - d) / 1000);
            if (diff < 60) return 'just now';
            if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
            if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
            return `${Math.floor(diff / 86400)}d ago`;
        },
    },

    // -----------------------------------------------------------------------
    // Request form
    // -----------------------------------------------------------------------
    request: {
        _dpTimer: null,
        _domainTimer: null,
        _atlanConfigured: null,  // null = unknown, true/false once checked

        async init() {
            // Check whether Atlan is configured so we know how to behave
            try {
                const status = await DDLC.api.fetchJSON('/api/atlan/status');
                this._atlanConfigured = status.configured === true;
            } catch (_) {
                this._atlanConfigured = false;
            }

            // Data Product fallback: freeform text when Atlan not configured
            const dpSearchInput = document.getElementById('data_product_search');
            if (dpSearchInput && !this._atlanConfigured) {
                dpSearchInput.placeholder = 'e.g., Customer Analytics';
                dpSearchInput.addEventListener('input', () => {
                    const hiddenDp = document.getElementById('data_product');
                    if (hiddenDp) hiddenDp.value = dpSearchInput.value;
                });
            }

            // Domain fallback: freeform text when Atlan not configured
            const domainSearchInput = document.getElementById('domain_search');
            if (domainSearchInput && !this._atlanConfigured) {
                domainSearchInput.placeholder = 'e.g., Finance';
                domainSearchInput.addEventListener('input', () => {
                    const hiddenDomain = document.getElementById('domain');
                    if (hiddenDomain) hiddenDomain.value = domainSearchInput.value;
                });
            }
        },

        onDataProductSearch() {
            clearTimeout(this._dpTimer);
            this._dpTimer = setTimeout(async () => {
                const searchInput = document.getElementById('data_product_search');
                const q = (searchInput?.value || '').trim();

                if (!this._atlanConfigured) return;

                try {
                    const url = `/api/atlan/search-products?q=${encodeURIComponent(q)}`;
                    const data = await DDLC.api.fetchJSON(url);
                    // Endpoint returns a list directly
                    this.renderDataProductDropdown(Array.isArray(data) ? data : (data.products || []));
                } catch (err) {
                    console.warn('Data product search failed:', err.message);
                }
            }, 300);
        },

        renderDataProductDropdown(products) {
            const dd = document.getElementById('dataProductDropdown');
            if (!dd) return;
            if (!products.length) {
                dd.style.display = 'none';
                return;
            }
            dd.innerHTML = products.map(p => {
                const safeP = JSON.stringify(p).replace(/'/g, "\\'").replace(/"/g, '&quot;');
                return `<div style="padding:8px 12px; cursor:pointer; font-size:0.82rem;
                                   border-bottom:1px solid var(--border); transition:background 0.15s;"
                             onmousedown="DDLC.request.selectDataProduct(${safeP.replace(/&quot;/g, "'")})"
                             onmouseover="this.style.background='var(--bg-input)'"
                             onmouseout="this.style.background=''">
                    <div style="font-weight:600; color:var(--text);">${DDLC.request._esc(p.name)}</div>
                    ${p.description ? `<div style="font-size:0.72rem; color:var(--text-muted); margin-top:2px;">${DDLC.request._esc(p.description)}</div>` : ''}
                    <div style="font-size:0.7rem; color:var(--text-dim); margin-top:2px; font-family:monospace;">${DDLC.request._esc(p.qualified_name || '')}</div>
                </div>`;
            }).join('');
            dd.style.display = 'block';
        },

        selectDataProduct(p) {
            const searchInput = document.getElementById('data_product_search');
            const hiddenDp = document.getElementById('data_product');
            const hiddenQn = document.getElementById('data_product_qualified_name');
            if (searchInput) searchInput.value = p.name || '';
            if (hiddenDp) hiddenDp.value = p.name || '';
            if (hiddenQn) hiddenQn.value = p.qualified_name || '';
            this.hideDataProductDropdown();
        },

        hideDataProductDropdown() {
            const dd = document.getElementById('dataProductDropdown');
            if (dd) dd.style.display = 'none';
        },

        onDomainSearch() {
            clearTimeout(this._domainTimer);
            this._domainTimer = setTimeout(async () => {
                const searchInput = document.getElementById('domain_search');
                const q = (searchInput?.value || '').trim();

                if (!this._atlanConfigured) return;

                try {
                    const url = `/api/atlan/search-domains?q=${encodeURIComponent(q)}`;
                    const data = await DDLC.api.fetchJSON(url);
                    // Endpoint returns a list directly
                    this.renderDomainDropdown(Array.isArray(data) ? data : (data.domains || []));
                } catch (err) {
                    console.warn('Domain search failed:', err.message);
                }
            }, 300);
        },

        renderDomainDropdown(domains) {
            const dd = document.getElementById('domainDropdown');
            if (!dd) return;
            if (!domains.length) {
                dd.style.display = 'none';
                return;
            }
            dd.innerHTML = domains.map(d => {
                const safeD = JSON.stringify(d).replace(/'/g, "\\'").replace(/"/g, '&quot;');
                return `<div style="padding:8px 12px; cursor:pointer; font-size:0.82rem;
                                   border-bottom:1px solid var(--border); transition:background 0.15s;"
                             onmousedown="DDLC.request.selectDomain(${safeD.replace(/&quot;/g, "'")})"
                             onmouseover="this.style.background='var(--bg-input)'"
                             onmouseout="this.style.background=''">
                    <div style="font-weight:600; color:var(--text);">${DDLC.request._esc(d.name)}</div>
                    ${d.description ? `<div style="font-size:0.72rem; color:var(--text-muted); margin-top:2px;">${DDLC.request._esc(d.description)}</div>` : ''}
                    <div style="font-size:0.7rem; color:var(--text-dim); margin-top:2px; font-family:monospace;">${DDLC.request._esc(d.qualified_name || '')}</div>
                </div>`;
            }).join('');
            dd.style.display = 'block';
        },

        selectDomain(d) {
            const searchInput = document.getElementById('domain_search');
            const hiddenDomain = document.getElementById('domain');
            const hiddenQn = document.getElementById('domain_qualified_name');
            if (searchInput) searchInput.value = d.name || '';
            if (hiddenDomain) hiddenDomain.value = d.name || '';
            if (hiddenQn) hiddenQn.value = d.qualified_name || '';
            this.hideDomainDropdown();
        },

        hideDomainDropdown() {
            const dd = document.getElementById('domainDropdown');
            if (dd) dd.style.display = 'none';
        },

        _esc(str) {
            if (!str) return '';
            const d = document.createElement('div');
            d.textContent = str;
            return d.innerHTML;
        },

        async submit(event) {
            event.preventDefault();
            const form = document.getElementById('requestForm');
            const btn = document.getElementById('submitBtn');
            btn.disabled = true;
            btn.textContent = 'Submitting...';

            try {
                const dpSearch = document.getElementById('data_product_search');
                const dpHidden = document.getElementById('data_product');
                const dpQnHidden = document.getElementById('data_product_qualified_name');

                // If Atlan not configured, data_product comes from the search input directly
                const dataProductName = (dpHidden?.value || dpSearch?.value || '').trim();
                const dataProductQn = (dpQnHidden?.value || '').trim() || null;

                const domainSearch = document.getElementById('domain_search');
                const domainHidden = document.getElementById('domain');
                const domainQnHidden = document.getElementById('domain_qualified_name');
                // If Atlan not configured, domain comes from the search input directly
                const domainName = (domainHidden?.value || domainSearch?.value || '').trim();
                const domainQn = (domainQnHidden?.value || '').trim() || null;

                const data = {
                    title: form.title.value,
                    description: form.description.value,
                    business_context: form.business_context.value,
                    target_use_case: form.target_use_case.value,
                    domain: domainName || null,
                    domain_qualified_name: domainQn,
                    data_product: dataProductName || null,
                    data_product_qualified_name: dataProductQn,
                    urgency: form.urgency.value,
                    desired_fields: form.desired_fields.value,
                    requester_name: form.requester_name.value,
                    requester_email: form.requester_email.value,
                };

                const result = await DDLC.api.post('/api/sessions', data);
                window.location.href = `/contract/${result.id}`;
            } catch (err) {
                DDLC.toast.show(err.message, 'error');
                btn.disabled = false;
                btn.textContent = 'Submit Request';
            }
            return false;
        },
    },
};
