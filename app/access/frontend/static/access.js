/**
 * Access App — DIGIC ServiceNow form rendering with cascading dropdowns.
 *
 * Flow:
 * 1. Read ?qn= from URL
 * 2. Fetch form schema + DIGIC lookup data in parallel
 * 3. Render form fields dynamically
 * 4. Wire cascading: data_product → filter access_role choices + update description
 * 5. Pre-populate from Atlan auth context
 * 6. Validate + submit via POST /api/submit-request
 */

const POLICY_TEXT = `The Digital Information Core (DIGIC) is a cross platform solution where we house our RAW Source System Data, Data Products, as well Curated Data that is published for end-user consumption. It provides a trusted source as well easy to consume data that the organization can leverage with confidence while maximizing the business value of data. DIGIC data is intended to be used in-place, meaning users connect to the platform via visualization tools or direct database access depending on the role selected. Across the Source System Data and Curated Data Domains are assigned Data Owners that are responsible for the data access roles. They help define the intended use, data security requirements, approval access, and necessary governance in form of masking and row-level security.

A DIGIC user must:
- Not opportunistically use or mine data that is entrusted for purposes that is not outlined by the Data Owner
- Obtain written permission from the Data Owner (e.g. ServiceNow approval) before engaging a subcontractor
- Assume full liability for failures of subcontractors to meet the intended use
- Upon request, delete or return all personal data to the Data Owner at the end of the service contract
- Enable and contribute to compliance audits conducted by the Data Owner or a representative of the Data Owner
- Take reasonable steps to secure data, such as:
  - Row-level security
  - Encryption and pseudonymization
  - Stability and uptime
  - Backup and disaster recovery
  - Regular security testing
- Notify Data Owners without undue delay upon learning of data breaches
- Restrict data transfer to a third country only if legal safeguards are obtained`;

const AccessApp = {
    /** @type {string} */
    qn: '',
    /** @type {string|null} */
    assetGuid: null,
    /** @type {object|null} */
    schema: null,
    /** @type {object|null} DIGIC data product → role lookup */
    digicData: null,
    /** @type {'loading'|'form'|'submitting'|'success'|'error'} */
    state: 'loading',
    /** @type {object|null} */
    submitResult: null,
    /** @type {string} */
    errorMessage: '',

    // ------------------------------------------------------------------
    // Init
    // ------------------------------------------------------------------

    async init() {
        this.qn = new URLSearchParams(window.location.search).get('qn') || '';
        this.assetGuid = window.__atlanAssetGuid || null;
        this.render();
        await this.loadData();
    },

    // ------------------------------------------------------------------
    // Data fetching
    // ------------------------------------------------------------------

    async loadData() {
        this.state = 'loading';
        this.render();
        try {
            const [schemaResp, digicResp] = await Promise.all([
                fetch(`/api/form-schema?qn=${encodeURIComponent(this.qn)}`),
                fetch('/api/digic-data'),
            ]);
            if (!schemaResp.ok) throw new Error(`Form schema: HTTP ${schemaResp.status}`);
            this.schema = await schemaResp.json();
            this.digicData = digicResp.ok ? await digicResp.json() : null;
            this.state = 'form';
        } catch (err) {
            this.errorMessage = `Failed to load form: ${err.message}`;
            this.state = 'error';
        }
        this.render();
        if (this.state === 'form') {
            this._wireEvents();
            this.prefillFromAuth();
        }
    },

    // ------------------------------------------------------------------
    // Pre-populate from Atlan auth context
    // ------------------------------------------------------------------

    prefillFromAuth() {
        const auth = window.__atlanAuth;
        if (!auth || !auth.user) return;
        if (this.state !== 'form') return;

        if (!this.assetGuid && auth.page) {
            this.assetGuid = auth.page.guid || (auth.page.params && auth.page.params.id) || null;
        }

        const mappings = {
            'requested_for': auth.user.name || auth.user.username || '',
            'email': auth.user.email || '',
            'requester_name': auth.user.name || '',
            'requester_email': auth.user.email || '',
        };

        for (const [name, value] of Object.entries(mappings)) {
            const el = document.getElementById(`field-${name}`);
            if (el && !el.value && value) {
                el.value = value;
            }
        }
    },

    // ------------------------------------------------------------------
    // Event wiring (cascading dropdowns)
    // ------------------------------------------------------------------

    _wireEvents() {
        // When data_product changes → filter access_role choices + clear description
        const dpSelect = document.getElementById('field-data_product');
        if (dpSelect) {
            dpSelect.addEventListener('change', () => this._onDataProductChange());
        }

        // When access_role changes → update description
        const roleSelect = document.getElementById('field-access_role');
        if (roleSelect) {
            roleSelect.addEventListener('change', () => this._onAccessRoleChange());
        }
    },

    _onDataProductChange() {
        const dpVal = document.getElementById('field-data_product')?.value || '';
        const roleSelect = document.getElementById('field-access_role');
        const descEl = document.getElementById('field-access_description');

        if (!roleSelect || !this.digicData) return;

        // Get the roles for the selected data product
        const dpInfo = this.digicData[dpVal];
        const availableRoles = dpInfo ? Object.keys(dpInfo.roles) : [];

        // Filter access_role options
        const options = roleSelect.querySelectorAll('option');
        let firstVisible = null;
        options.forEach(opt => {
            if (!opt.value) return; // keep "— Select —"
            if (availableRoles.length === 0) {
                // No filter, show all
                opt.style.display = '';
                opt.disabled = false;
            } else if (availableRoles.includes(opt.value)) {
                opt.style.display = '';
                opt.disabled = false;
                if (!firstVisible) firstVisible = opt.value;
            } else {
                opt.style.display = 'none';
                opt.disabled = true;
            }
        });

        // Reset selection if current value is now hidden
        if (roleSelect.value && !availableRoles.includes(roleSelect.value) && availableRoles.length > 0) {
            roleSelect.value = '';
        }

        // Clear description
        if (descEl) descEl.value = dpInfo ? dpInfo.description : '';

        this._onAccessRoleChange();
    },

    _onAccessRoleChange() {
        const dpVal = document.getElementById('field-data_product')?.value || '';
        const roleVal = document.getElementById('field-access_role')?.value || '';
        const descEl = document.getElementById('field-access_description');

        if (!descEl || !this.digicData) return;

        const dpInfo = this.digicData[dpVal];
        if (!dpInfo || !roleVal) {
            descEl.value = dpInfo ? dpInfo.description : '';
            return;
        }

        const roleInfo = dpInfo.roles[roleVal];
        descEl.value = roleInfo ? roleInfo.description : dpInfo.description;
    },

    // ------------------------------------------------------------------
    // Rendering
    // ------------------------------------------------------------------

    render() {
        const container = document.getElementById('app');
        if (!container) return;

        switch (this.state) {
            case 'loading':
                container.innerHTML = this._renderLoading();
                break;
            case 'form':
                container.innerHTML = this._renderForm();
                break;
            case 'submitting':
                container.innerHTML = this._renderSubmitting();
                break;
            case 'success':
                container.innerHTML = this._renderSuccess();
                break;
            case 'error':
                container.innerHTML = this._renderError();
                break;
        }
    },

    _renderLoading() {
        return `
            <div class="access-container">
                <div class="state-loading">
                    <div class="spinner"></div>
                    <p>Loading access request form...</p>
                </div>
            </div>`;
    },

    _renderSubmitting() {
        return `
            <div class="access-container">
                <div class="state-loading">
                    <div class="spinner"></div>
                    <p>Submitting your request...</p>
                </div>
            </div>`;
    },

    _renderForm() {
        if (!this.schema) return '';
        const s = this.schema;
        const fields = (s.variables || []).map(v => this._renderField(v)).join('');

        return `
            <div class="access-container">
                <div class="access-header">
                    <h1>${this._esc(s.catalog_item_name)}</h1>
                    ${s.short_description ? `<p class="subtitle">${this._esc(s.short_description)}</p>` : ''}
                    ${this.qn ? `
                        <div class="access-meta">
                            <span>Data Product: <strong>${this._esc(this._displayQn(this.qn))}</strong></span>
                        </div>` : ''}
                    <div class="digic-description">
                        <p>If requesting access to Global Sales Reporting, please request via the "Global Sales Reporting Access Request" form.</p>
                    </div>
                </div>
                <div class="required-notice"><span class="required">*</span> Indicates required</div>
                <div class="access-form">
                    ${fields}
                    <div class="form-actions">
                        <button class="btn btn-primary" onclick="AccessApp.handleSubmit()">Submit Request</button>
                    </div>
                </div>
            </div>`;
    },

    _renderField(v) {
        const id = `field-${v.name}`;
        const req = v.mandatory ? '<span class="required">*</span>' : '';
        const help = v.help_text ? `<div class="help-text">${this._esc(v.help_text)}</div>` : '';
        const errMsg = '<div class="error-message">This field is required</div>';

        // ── Label / info type — render as instruction text, no input
        if (v.field_type === 'label') {
            return `<div class="form-group form-group-label" data-name="${v.name}">
                <div class="label-text">${req}${this._esc(v.label)}</div>
            </div>`;
        }

        // ── Policy header — render policy text block before the next field
        if (v.name === 'policy_header') {
            return `<div class="form-group form-group-policy" data-name="${v.name}">
                <div class="policy-label">${req}${this._esc(v.label)} <span class="info-icon" title="Read the full policy">&#9432;</span></div>
                <div class="policy-box">
                    <button class="policy-close" onclick="this.parentElement.style.display='none'" title="Close">&times;</button>
                    <div class="policy-text">${this._renderPolicyText()}</div>
                </div>
            </div>`;
        }

        let input = '';
        switch (v.field_type) {
            case 'text':
            case 'email':
            case 'url':
                input = `<input type="${v.field_type === 'text' ? 'text' : v.field_type}" id="${id}" name="${v.name}"
                    value="${this._esc(v.default_value)}"
                    ${v.read_only ? 'readonly class="readonly"' : ''}
                    ${v.mandatory ? 'required' : ''}>`;
                break;

            case 'reference':
                // Render as a text input with search icon styling
                input = `<div class="reference-field">
                    <span class="reference-icon" title="Search">&#9432;</span>
                    <input type="text" id="${id}" name="${v.name}"
                        value="${this._esc(v.default_value)}"
                        placeholder="Search..."
                        ${v.mandatory ? 'required' : ''}>
                    <button class="reference-clear" onclick="document.getElementById('${id}').value=''" title="Clear">&times;</button>
                </div>`;
                break;

            case 'number':
                input = `<input type="number" id="${id}" name="${v.name}"
                    value="${this._esc(v.default_value)}"
                    ${v.read_only ? 'readonly' : ''}
                    ${v.mandatory ? 'required' : ''}>`;
                break;

            case 'textarea':
                input = `<textarea id="${id}" name="${v.name}"
                    ${v.read_only ? 'readonly' : ''}
                    ${v.mandatory ? 'required' : ''}>${this._esc(v.default_value)}</textarea>`;
                break;

            case 'select':
                const options = (v.choices || []).map(c =>
                    `<option value="${this._esc(c.value)}" ${c.value === v.default_value ? 'selected' : ''}>${this._esc(c.label)}</option>`
                ).join('');
                input = `<select id="${id}" name="${v.name}" ${v.mandatory ? 'required' : ''}>
                    <option value="">-- Select --</option>
                    ${options}
                </select>`;
                break;

            case 'radio':
                const radios = (v.choices || []).map(c =>
                    `<div class="radio-option">
                        <input type="radio" id="${id}-${c.value}" name="${v.name}" value="${this._esc(c.value)}"
                            ${c.value === v.default_value ? 'checked' : ''}>
                        <label for="${id}-${c.value}">${this._esc(c.label)}</label>
                    </div>`
                ).join('');
                input = `<div class="radio-group">${radios}</div>`;
                break;

            case 'multi_select':
                const checks = (v.choices || []).map(c =>
                    `<div class="checkbox-wrapper">
                        <input type="checkbox" id="${id}-${c.value}" name="${v.name}" value="${this._esc(c.value)}">
                        <label for="${id}-${c.value}">${this._esc(c.label)}</label>
                    </div>`
                ).join('');
                input = `<div class="multi-select-group">${checks}</div>`;
                break;

            case 'checkbox':
                return `<div class="form-group form-group-checkbox" data-name="${v.name}">
                    <div class="checkbox-wrapper">
                        <input type="checkbox" id="${id}" name="${v.name}" ${v.default_value === 'true' ? 'checked' : ''}>
                        <label for="${id}">${this._esc(v.label)}${req}</label>
                    </div>
                    ${help}${errMsg}
                </div>`;

            case 'date':
                input = `<input type="date" id="${id}" name="${v.name}"
                    value="${this._esc(v.default_value)}"
                    ${v.mandatory ? 'required' : ''}>`;
                break;

            case 'datetime':
                input = `<input type="datetime-local" id="${id}" name="${v.name}"
                    value="${this._esc(v.default_value)}"
                    ${v.mandatory ? 'required' : ''}>`;
                break;

            default:
                input = `<input type="text" id="${id}" name="${v.name}"
                    value="${this._esc(v.default_value)}"
                    ${v.mandatory ? 'required' : ''}>`;
        }

        return `<div class="form-group" data-name="${v.name}">
            <label for="${id}">${req}${this._esc(v.label)}</label>
            ${input}${help}${errMsg}
        </div>`;
    },

    _renderPolicyText() {
        // Convert the policy text to HTML with bullet points
        return POLICY_TEXT.split('\n').map(line => {
            const trimmed = line.trim();
            if (trimmed.startsWith('- ')) {
                return `<li>${this._esc(trimmed.slice(2))}</li>`;
            }
            if (trimmed.startsWith('  - ')) {
                return `<li class="sub-item">${this._esc(trimmed.slice(4))}</li>`;
            }
            if (trimmed === '') return '';
            return `<p>${this._esc(trimmed)}</p>`;
        }).join('');
    },

    _renderSuccess() {
        const r = this.submitResult || {};
        return `
            <div class="access-container">
                <div class="state-success">
                    <div class="success-icon">&#10003;</div>
                    <h2>Request Submitted Successfully</h2>
                    <p>Your access request has been submitted to ServiceNow.</p>
                    <div class="request-numbers">
                        ${r.request_number ? `<div class="req-badge"><span class="req-label">Request</span><strong>${this._esc(r.request_number)}</strong></div>` : ''}
                        ${r.request_item_number ? `<div class="req-badge"><span class="req-label">Item</span><strong>${this._esc(r.request_item_number)}</strong></div>` : ''}
                    </div>
                    <button class="btn btn-secondary" onclick="AccessApp.loadData()">Submit Another Request</button>
                </div>
            </div>`;
    },

    _renderError() {
        return `
            <div class="access-container">
                <div class="state-error">
                    <h2>Something went wrong</h2>
                    <p>${this._esc(this.errorMessage)}</p>
                    <button class="btn btn-primary" onclick="AccessApp.loadData()">Try Again</button>
                </div>
            </div>`;
    },

    // ------------------------------------------------------------------
    // Validation + submission
    // ------------------------------------------------------------------

    validate() {
        if (!this.schema) return false;
        let valid = true;
        let firstError = null;

        for (const v of this.schema.variables) {
            // Skip label / policy header fields
            if (v.field_type === 'label' || v.name === 'policy_header') continue;

            const group = document.querySelector(`.form-group[data-name="${v.name}"]`);
            if (!group) continue;
            group.classList.remove('has-error');

            if (!v.mandatory) continue;

            const value = this._getFieldValue(v);
            if (!value || (typeof value === 'string' && !value.trim())) {
                group.classList.add('has-error');
                valid = false;
                if (!firstError) firstError = group;
            }
        }

        if (firstError) {
            firstError.scrollIntoView({ behavior: 'smooth', block: 'center' });
        }
        return valid;
    },

    _getFieldValue(v) {
        if (v.field_type === 'checkbox') {
            const el = document.getElementById(`field-${v.name}`);
            return el ? el.checked : false;
        }
        if (v.field_type === 'radio') {
            const checked = document.querySelector(`input[name="${v.name}"]:checked`);
            return checked ? checked.value : '';
        }
        if (v.field_type === 'multi_select') {
            const checked = document.querySelectorAll(`input[name="${v.name}"]:checked`);
            return Array.from(checked).map(c => c.value).join(',');
        }
        const el = document.getElementById(`field-${v.name}`);
        return el ? el.value : '';
    },

    collectFormValues() {
        const values = {};
        if (!this.schema) return values;
        for (const v of this.schema.variables) {
            if (v.field_type === 'label' || v.name === 'policy_header') continue;
            values[v.name] = this._getFieldValue(v);
        }
        return values;
    },

    async handleSubmit() {
        if (!this.validate()) return;

        // Collect values BEFORE re-rendering (render destroys the DOM form elements)
        const formVars = this.collectFormValues();

        this.state = 'submitting';
        this.render();
        if (this.qn) formVars['__data_product_qn'] = this.qn;
        if (this.assetGuid) formVars['__data_product_guid'] = this.assetGuid;

        const payload = {
            catalog_item_sys_id: this.schema.catalog_item_sys_id,
            variables: formVars,
            data_product_qn: this.qn,
            requester_email: (window.__atlanAuth && window.__atlanAuth.user)
                ? window.__atlanAuth.user.email || ''
                : '',
        };

        try {
            const resp = await fetch('/api/submit-request', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload),
            });
            if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
            const result = await resp.json();
            if (result.success) {
                this.submitResult = result;
                this.state = 'success';
            } else {
                this.errorMessage = result.error || 'Submission failed';
                this.state = 'error';
            }
        } catch (err) {
            this.errorMessage = `Submission failed: ${err.message}`;
            this.state = 'error';
        }
        this.render();
    },

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    _esc(str) {
        if (!str) return '';
        const d = document.createElement('div');
        d.textContent = String(str);
        return d.innerHTML;
    },

    _displayQn(qn) {
        const parts = qn.split('/');
        return parts.length > 2 ? parts.slice(-2).join(' / ') : qn;
    },
};

// Boot
document.addEventListener('DOMContentLoaded', () => AccessApp.init());
