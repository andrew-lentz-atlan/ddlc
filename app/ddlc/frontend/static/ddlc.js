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
        async submit(event) {
            event.preventDefault();
            const form = document.getElementById('requestForm');
            const btn = document.getElementById('submitBtn');
            btn.disabled = true;
            btn.textContent = 'Submitting...';

            try {
                const data = {
                    title: form.title.value,
                    description: form.description.value,
                    business_context: form.business_context.value,
                    target_use_case: form.target_use_case.value,
                    domain: form.domain.value,
                    data_product: form.data_product.value,
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
