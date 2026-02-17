/**
 * mock-api.js — GitHub Pages demo layer for DDLC.
 *
 * Intercepts all fetch() calls to /api/* and routes them to an in-memory
 * store seeded with 5 demo sessions. No backend required.
 *
 * Loaded AFTER ddlc.js / contract-builder.js / yaml-preview.js but
 * BEFORE DOMContentLoaded fires, so patches are applied in time.
 */
(function () {
    'use strict';

    // =====================================================================
    // Helpers
    // =====================================================================

    const _now = new Date();
    function _ts(daysAgo, hoursAgo) {
        const d = new Date(_now);
        d.setDate(d.getDate() - (daysAgo || 0));
        d.setHours(d.getHours() - (hoursAgo || 0));
        return d.toISOString();
    }
    function _uuid() { return crypto.randomUUID(); }

    const STAGE_ORDER = ['request', 'discovery', 'specification', 'review', 'approval', 'active'];
    const STAGE_TO_STATUS = {
        request: 'proposed', discovery: 'proposed',
        specification: 'draft', review: 'draft',
        approval: 'draft', active: 'active',
    };

    // =====================================================================
    // In-memory store
    // =====================================================================

    const _store = new Map();

    function _getSession(id) { return _store.get(id) || null; }

    function _saveSession(s) {
        s.updated_at = new Date().toISOString();
        _store.set(s.id, s);
    }

    function _sessionSummary(s) {
        return {
            id: s.id,
            title: s.request.title,
            domain: s.request.domain,
            data_product: s.request.data_product,
            current_stage: s.current_stage,
            urgency: s.request.urgency,
            requester_name: s.request.requester.name,
            created_at: s.created_at,
            updated_at: s.updated_at,
            num_objects: s.contract.schema_objects.length,
            num_comments: s.comments.length,
        };
    }

    function _findObject(session, objName) {
        return session.contract.schema_objects.find(o => o.name === objName) || null;
    }

    function _findProperty(obj, propName) {
        return obj.properties.find(p => p.name === propName) || null;
    }

    // =====================================================================
    // Seed Data — 5 demo sessions
    // =====================================================================

    function seedDemoData() {
        const sessions = [
            _buildCustomer360(),
            _buildOrderEvents(),
            _buildProductCatalog(),
            _buildMarketingAttribution(),
            _buildInventorySnapshot(),
        ];
        sessions.forEach(s => _store.set(s.id, s));
    }

    // --- Shared participants ---
    const ALICE = { name: 'Alice Chen', email: 'alice.chen@acme.com' };
    const BOB   = { name: 'Bob Martinez', email: 'bob.martinez@acme.com' };
    const CAROL = { name: 'Carol Wang', email: 'carol.wang@acme.com' };
    const DAVE  = { name: 'Dave Okonkwo', email: 'dave.okonkwo@acme.com' };
    const EVE   = { name: 'Eve Thompson', email: 'eve.thompson@acme.com' };

    // Session 1: SPECIFICATION — Customer 360 (richest demo)
    function _buildCustomer360() {
        const id = _uuid(), cid = _uuid();
        return {
            id, current_stage: 'specification',
            request: {
                title: 'Customer 360 Unified Profile',
                description: 'We need a unified customer profile table that combines CRM data, order history, and web engagement metrics into a single analytics-ready dataset.',
                business_context: 'The marketing team is running a major personalization initiative in Q2. They need a single source of truth for customer attributes to power segmentation models and campaign targeting.',
                target_use_case: 'Customer segmentation, churn prediction, lifetime value analysis, personalized marketing campaigns.',
                urgency: 'high',
                requester: { ...ALICE },
                domain: 'Customer Analytics',
                data_product: 'Customer Intelligence Platform',
                desired_fields: ['customer_id','full_name','email','total_orders','total_revenue','last_order_date','web_sessions_30d'],
                created_at: _ts(5),
            },
            contract: {
                id: cid, api_version: 'v3.1.0', kind: 'DataContract',
                name: 'Customer 360', version: '0.1.0', status: 'draft',
                domain: 'Customer Analytics', tenant: 'Acme Corp',
                data_product: 'Customer Intelligence Platform',
                description_purpose: 'Provide a unified, deduplicated view of customer data for marketing, support, and product analytics teams.',
                description_limitations: 'Does not include real-time streaming data. Updated on a daily batch cadence. Phone numbers may be incomplete for legacy accounts.',
                description_usage: 'Use for customer segmentation, churn analysis, lifetime value calculations, and personalization. Do not use for regulatory reporting without cross-referencing with the compliance dataset.',
                tags: ['customer','analytics','pii','gold-tier'],
                schema_objects: [{
                    name: 'customer_360',
                    physical_name: 'ANALYTICS.MART.CUSTOMER_360',
                    description: 'Unified customer profile combining CRM data, order history, and web engagement metrics.',
                    source_tables: [
                        { name: 'raw_customers', qualified_name: 'default/snowflake/PROD_DB/RAW/CUSTOMERS', database_name: 'PROD_DB', schema_name: 'RAW', connector_name: 'snowflake', description: 'Raw customer data from the CRM system (daily extract).', columns: [
                            {name:'customer_id',logical_type:'integer',is_primary:true,is_nullable:false},
                            {name:'first_name',logical_type:'string',is_nullable:false},
                            {name:'last_name',logical_type:'string',is_nullable:false},
                            {name:'email',logical_type:'string',is_nullable:false},
                            {name:'phone',logical_type:'string',is_nullable:true},
                            {name:'created_at',logical_type:'timestamp',is_nullable:false},
                            {name:'updated_at',logical_type:'timestamp',is_nullable:true},
                            {name:'status',logical_type:'string',is_nullable:false},
                        ]},
                        { name: 'raw_orders', qualified_name: 'default/snowflake/PROD_DB/RAW/ORDERS', database_name: 'PROD_DB', schema_name: 'RAW', connector_name: 'snowflake', description: 'Order transaction data from the e-commerce platform.', columns: [
                            {name:'order_id',logical_type:'integer',is_primary:true,is_nullable:false},
                            {name:'customer_id',logical_type:'integer',is_nullable:false},
                            {name:'order_date',logical_type:'date',is_nullable:false},
                            {name:'total_amount',logical_type:'number',is_nullable:false},
                            {name:'status',logical_type:'string',is_nullable:false},
                        ]},
                        { name: 'raw_web_events', qualified_name: 'default/snowflake/PROD_DB/RAW/WEB_EVENTS', database_name: 'PROD_DB', schema_name: 'RAW', connector_name: 'snowflake', description: 'Clickstream web events from the analytics platform.', columns: [
                            {name:'event_id',logical_type:'string',is_primary:true,is_nullable:false},
                            {name:'customer_id',logical_type:'integer',is_nullable:true},
                            {name:'event_type',logical_type:'string',is_nullable:false},
                            {name:'page_url',logical_type:'string',is_nullable:false},
                            {name:'event_timestamp',logical_type:'timestamp',is_nullable:false},
                        ]},
                    ],
                    properties: [
                        { name:'customer_id', logical_type:'integer', description:'Unique customer identifier from CRM.', required:true, primary_key:true, primary_key_position:1, unique:true, classification:'internal', critical_data_element:true, examples:[], sources:[{source_table:'raw_customers',source_column:'customer_id',source_table_qualified_name:'default/snowflake/PROD_DB/RAW/CUSTOMERS',transform_logic:'CAST(customer_id AS INT)',transform_description:'Direct mapping from CRM customer ID.'}] },
                        { name:'full_name', logical_type:'string', description:'Customer full name (first + last).', required:true, classification:'pii', critical_data_element:true, examples:[], sources:[{source_table:'raw_customers',source_column:'first_name',source_table_qualified_name:'default/snowflake/PROD_DB/RAW/CUSTOMERS',transform_logic:"CONCAT(first_name, ' ', last_name)",transform_description:'Concatenation of first and last name.'},{source_table:'raw_customers',source_column:'last_name',source_table_qualified_name:'default/snowflake/PROD_DB/RAW/CUSTOMERS',transform_logic:null,transform_description:null}] },
                        { name:'email', logical_type:'string', description:'Primary email address.', required:true, unique:true, classification:'pii', critical_data_element:true, examples:['alice@example.com','bob@test.org'], sources:[{source_table:'raw_customers',source_column:'email',source_table_qualified_name:'default/snowflake/PROD_DB/RAW/CUSTOMERS',transform_logic:'LOWER(TRIM(email))',transform_description:'Normalized to lowercase, trimmed.'}] },
                        { name:'phone', logical_type:'string', description:'Phone number (optional).', required:false, classification:'pii', examples:[], sources:[{source_table:'raw_customers',source_column:'phone',source_table_qualified_name:'default/snowflake/PROD_DB/RAW/CUSTOMERS',transform_logic:"REGEXP_REPLACE(phone, '[^0-9+]', '')",transform_description:'Strip non-numeric characters except +.'}] },
                        { name:'customer_status', logical_type:'string', description:'Current account status (active, churned, suspended).', required:true, examples:['active','churned','suspended'], sources:[{source_table:'raw_customers',source_column:'status',source_table_qualified_name:'default/snowflake/PROD_DB/RAW/CUSTOMERS',transform_logic:'UPPER(status)',transform_description:'Uppercased status from CRM.'}] },
                        { name:'total_orders', logical_type:'integer', description:'Lifetime count of orders placed.', required:true, critical_data_element:true, examples:[], sources:[{source_table:'raw_orders',source_column:'order_id',source_table_qualified_name:'default/snowflake/PROD_DB/RAW/ORDERS',transform_logic:'COUNT(DISTINCT order_id)',transform_description:'Count of distinct orders per customer.'}] },
                        { name:'total_revenue', logical_type:'number', description:'Lifetime revenue from all orders.', required:true, critical_data_element:true, examples:[], sources:[{source_table:'raw_orders',source_column:'total_amount',source_table_qualified_name:'default/snowflake/PROD_DB/RAW/ORDERS',transform_logic:'SUM(total_amount)',transform_description:'Sum of order amounts per customer.'}] },
                        { name:'last_order_date', logical_type:'date', description:'Date of most recent order.', required:false, examples:[], sources:[{source_table:'raw_orders',source_column:'order_date',source_table_qualified_name:'default/snowflake/PROD_DB/RAW/ORDERS',transform_logic:'MAX(order_date)',transform_description:'Most recent order date per customer.'}] },
                        { name:'web_sessions_30d', logical_type:'integer', description:'Number of web sessions in the last 30 days.', required:false, examples:[], sources:[{source_table:'raw_web_events',source_column:'event_id',source_table_qualified_name:'default/snowflake/PROD_DB/RAW/WEB_EVENTS',transform_logic:"COUNT(DISTINCT DATE_TRUNC('day', event_timestamp)) WHERE event_timestamp >= CURRENT_DATE - 30",transform_description:'Count of distinct days with activity in last 30 days.'}] },
                        { name:'first_seen_at', logical_type:'timestamp', description:'Timestamp of account creation.', required:true, examples:[], sources:[{source_table:'raw_customers',source_column:'created_at',source_table_qualified_name:'default/snowflake/PROD_DB/RAW/CUSTOMERS',transform_logic:'created_at',transform_description:'Direct mapping.'}] },
                        { name:'last_updated_at', logical_type:'timestamp', description:'Timestamp of last profile update.', required:false, examples:[], sources:[{source_table:'raw_customers',source_column:'updated_at',source_table_qualified_name:'default/snowflake/PROD_DB/RAW/CUSTOMERS',transform_logic:'COALESCE(updated_at, created_at)',transform_description:'Falls back to created_at if never updated.'}] },
                    ],
                }],
                quality_checks: [
                    { id:_uuid(), type:'sql', description:'Customer ID must be unique across all rows.', dimension:'uniqueness', severity:'critical', must_be:'unique', must_be_greater_than:null, must_be_less_than:null, metric:null, method:'field_health', column:'customer_360.customer_id', schedule:'0 6 * * *', scheduler:'monte-carlo', engine:'monte-carlo', query:null, business_impact:'Duplicate customers cause double-counting in revenue attribution and corrupt marketing segmentation models.' },
                    { id:_uuid(), type:'sql', description:'Email must not be null for active customers.', dimension:'completeness', severity:'critical', must_be:"not null where customer_status = 'ACTIVE'", must_be_greater_than:null, must_be_less_than:null, metric:null, method:'sql_rule', column:'customer_360.email', schedule:'0 6 * * *', scheduler:'monte-carlo', engine:'monte-carlo', query:"SELECT COUNT(*) FROM customer_360 WHERE email IS NULL AND customer_status = 'ACTIVE'", business_impact:'Null emails prevent campaign delivery and break personalization workflows.' },
                    { id:_uuid(), type:'sql', description:'Total revenue must be non-negative.', dimension:'validity', severity:'high', must_be:null, must_be_greater_than:0.0, must_be_less_than:null, metric:null, method:'field_health', column:'customer_360.total_revenue', schedule:'0 7 * * *', scheduler:null, engine:'monte-carlo', query:null, business_impact:'Negative revenue values corrupt LTV calculations and finance reconciliation.' },
                    { id:_uuid(), type:'text', description:'Row count should not drop more than 10% day-over-day.', dimension:'volume', severity:'medium', must_be:null, must_be_greater_than:null, must_be_less_than:null, metric:null, method:'volume', column:null, schedule:'0 6 * * *', scheduler:null, engine:'monte-carlo', query:null, business_impact:'Sudden volume drops indicate upstream pipeline failures or data loss.' },
                ],
                sla_properties: [
                    { id:_uuid(), property:'freshness', value:'24', unit:'hours', description:'Data refreshed daily by 6am UTC.', schedule:'0 6 * * *', scheduler:'airflow', driver:'analytics', element:'customer_360' },
                    { id:_uuid(), property:'availability', value:'99.5', unit:'percent', description:'Target uptime for the downstream BI dashboards.', schedule:null, scheduler:null, driver:'operational', element:'customer_360' },
                    { id:_uuid(), property:'latency', value:'30', unit:'minutes', description:'Maximum pipeline runtime from source extract to mart load.', schedule:'0 6 * * *', scheduler:'airflow', driver:'analytics', element:'customer_360' },
                    { id:_uuid(), property:'retention', value:'7', unit:'years', description:'Retain historical snapshots for 7 years per compliance policy.', schedule:null, scheduler:null, driver:'regulatory', element:null },
                ],
                team: [
                    { name:'Alice Chen', email:'alice.chen@acme.com', role:'Data Owner' },
                    { name:'Bob Martinez', email:'bob.martinez@acme.com', role:'Data Steward' },
                    { name:'Carol Wang', email:'carol.wang@acme.com', role:'Data Engineer' },
                    { name:'Dave Okonkwo', email:'dave.okonkwo@acme.com', role:'Analytics Engineer' },
                ],
            },
            participants: [{ ...ALICE },{ ...BOB },{ ...CAROL },{ ...DAVE }],
            comments: [
                { id:_uuid(), author:{...ALICE}, content:'We need this customer 360 table to power our Q2 personalization initiative. The marketing team is blocked without a unified customer view.', stage:'request', parent_id:null, created_at:_ts(5) },
                { id:_uuid(), author:{...BOB}, content:"I've reviewed the source systems. We have three primary sources: raw_customers (CRM), raw_orders (e-commerce), and raw_web_events (clickstream). All are available in the RAW schema on Snowflake.", stage:'discovery', parent_id:null, created_at:_ts(4) },
                { id:_uuid(), author:{...CAROL}, content:"The raw_customers table has ~2.5M rows with daily incremental loads. raw_orders is ~15M rows. Web events are ~100M rows but we'll aggregate to customer-level metrics. I'll set up the source-to-target mapping.", stage:'discovery', parent_id:null, created_at:_ts(4,2) },
                { id:_uuid(), author:{...DAVE}, content:"For the analytics models, we'll need total_orders, total_revenue, and web_sessions_30d as pre-computed aggregates. The dbt model should handle the joins and aggregations.", stage:'discovery', parent_id:null, created_at:_ts(3) },
            ],
            history: [
                { from_stage:'request', to_stage:'discovery', transitioned_by:{...BOB}, reason:'Request is clear and well-scoped. Moving to discovery.', timestamp:_ts(4,6) },
                { from_stage:'discovery', to_stage:'specification', transitioned_by:{...CAROL}, reason:'Source systems identified. Starting specification.', timestamp:_ts(3) },
            ],
            created_at: _ts(5),
            updated_at: _ts(0,2),
        };
    }

    // Session 2: REVIEW — Order Events Fact Table
    function _buildOrderEvents() {
        const id = _uuid();
        return {
            id, current_stage: 'review',
            request: {
                title: 'Order Events Fact Table',
                description: 'Need a fact table joining orders with payment status for the revenue analytics team.',
                business_context: 'Finance needs reliable daily revenue figures. Current ad-hoc queries are slow and inconsistent.',
                target_use_case: 'Revenue dashboards, finance reconciliation, cohort revenue analysis.',
                urgency: 'critical',
                requester: { ...EVE },
                domain: 'Revenue Analytics',
                data_product: 'Revenue Intelligence',
                desired_fields: [],
                created_at: _ts(8),
            },
            contract: {
                id: _uuid(), api_version: 'v3.1.0', kind: 'DataContract',
                name: 'Order Events Fact', version: '0.2.0', status: 'draft',
                domain: 'Revenue Analytics', tenant: null,
                data_product: 'Revenue Intelligence',
                description_purpose: 'Provide a denormalized order events fact table for revenue reporting and forecasting.',
                description_limitations: 'Does not include subscription or recurring revenue events. Payment status may lag by up to 4 hours.',
                description_usage: 'Use for daily revenue dashboards, cohort analysis, and finance reconciliation.',
                tags: ['orders','revenue','finance','silver-tier'],
                schema_objects: [{
                    name: 'fact_order_events',
                    physical_name: 'ANALYTICS.MART.FACT_ORDER_EVENTS',
                    description: 'Fact table capturing each order event with payment status for revenue analytics.',
                    source_tables: [
                        { name:'raw_orders', qualified_name:'default/snowflake/PROD_DB/RAW/ORDERS', database_name:'PROD_DB', schema_name:'RAW', connector_name:'snowflake', description:'Order transaction data from the e-commerce platform.', columns:[] },
                        { name:'raw_payments', qualified_name:'default/snowflake/PROD_DB/RAW/PAYMENTS', database_name:'PROD_DB', schema_name:'RAW', connector_name:'snowflake', description:'Payment records linked to orders.', columns:[] },
                    ],
                    properties: [
                        { name:'order_event_id', logical_type:'string', description:'Surrogate key for the order event.', required:true, primary_key:true, primary_key_position:1, unique:true, examples:[], sources:[{source_table:'raw_orders',source_column:'order_id',source_table_qualified_name:'default/snowflake/PROD_DB/RAW/ORDERS',transform_logic:"MD5(CONCAT(order_id, '_', order_date))",transform_description:'Surrogate key from order_id + date.'}] },
                        { name:'order_id', logical_type:'integer', description:'Natural order ID from source system.', required:true, examples:[], sources:[{source_table:'raw_orders',source_column:'order_id',source_table_qualified_name:'default/snowflake/PROD_DB/RAW/ORDERS',transform_logic:null,transform_description:null}] },
                        { name:'customer_id', logical_type:'integer', description:'FK to customer dimension.', required:true, critical_data_element:true, examples:[], sources:[{source_table:'raw_orders',source_column:'customer_id',source_table_qualified_name:'default/snowflake/PROD_DB/RAW/ORDERS',transform_logic:null,transform_description:null}] },
                        { name:'order_date', logical_type:'date', description:'Date the order was placed.', required:true, examples:[], sources:[{source_table:'raw_orders',source_column:'order_date',source_table_qualified_name:'default/snowflake/PROD_DB/RAW/ORDERS',transform_logic:null,transform_description:null}] },
                        { name:'order_amount', logical_type:'number', description:'Total order amount in USD.', required:true, critical_data_element:true, examples:[], sources:[{source_table:'raw_orders',source_column:'total_amount',source_table_qualified_name:'default/snowflake/PROD_DB/RAW/ORDERS',transform_logic:null,transform_description:null}] },
                        { name:'payment_status', logical_type:'string', description:'Payment status (paid, pending, refunded).', required:true, examples:['paid','pending','refunded'], sources:[{source_table:'raw_payments',source_column:'status',source_table_qualified_name:'default/snowflake/PROD_DB/RAW/PAYMENTS',transform_logic:"COALESCE(p.status, 'unknown')",transform_description:"Joined from payments table; defaults to 'unknown' if no payment record."}] },
                    ],
                }],
                quality_checks: [
                    { id:_uuid(), type:'sql', description:'order_event_id must be unique.', dimension:'uniqueness', severity:'critical', must_be:'unique', must_be_greater_than:null, must_be_less_than:null, metric:null, method:'field_health', column:'fact_order_events.order_event_id', schedule:'*/6 * * * *', scheduler:null, engine:'monte-carlo', query:null, business_impact:'Duplicate order events inflate revenue metrics and break finance reconciliation.' },
                    { id:_uuid(), type:'sql', description:'order_amount must be >= 0.', dimension:'validity', severity:'critical', must_be:null, must_be_greater_than:0.0, must_be_less_than:null, metric:null, method:'field_health', column:'fact_order_events.order_amount', schedule:'*/6 * * * *', scheduler:null, engine:'monte-carlo', query:null, business_impact:'Negative order amounts distort revenue dashboards and quarterly reporting.' },
                    { id:_uuid(), type:'sql', description:'No more than 5% null payment_status values.', dimension:'completeness', severity:'high', must_be:null, must_be_greater_than:null, must_be_less_than:null, metric:null, method:'sql_rule', column:'fact_order_events.payment_status', schedule:'0 8 * * *', scheduler:null, engine:'monte-carlo', query:"SELECT ROUND(100.0 * SUM(CASE WHEN payment_status IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) FROM fact_order_events", business_impact:'Missing payment status causes incorrect revenue recognition and cash flow projections.' },
                ],
                sla_properties: [
                    { id:_uuid(), property:'freshness', value:'6', unit:'hours', description:'Updated every 6 hours to support intra-day revenue reporting.', schedule:'0 */6 * * *', scheduler:'airflow', driver:'operational', element:'fact_order_events' },
                    { id:_uuid(), property:'availability', value:'99.9', unit:'percent', description:'Critical for finance reporting and month-end close.', schedule:null, scheduler:null, driver:'compliance', element:'fact_order_events' },
                ],
                team: [
                    { name:'Eve Thompson', email:'eve.thompson@acme.com', role:'Data Owner' },
                    { name:'Carol Wang', email:'carol.wang@acme.com', role:'Data Engineer' },
                ],
            },
            participants: [{ ...EVE },{ ...CAROL },{ ...BOB }],
            comments: [
                { id:_uuid(), author:{...EVE}, content:'Finance team needs this urgently for month-end close.', stage:'request', parent_id:null, created_at:_ts(8) },
                { id:_uuid(), author:{...CAROL}, content:'Sources identified: raw_orders + raw_payments. Both in Snowflake RAW schema.', stage:'discovery', parent_id:null, created_at:_ts(7) },
                { id:_uuid(), author:{...BOB}, content:'Schema looks good. Payment join logic is clean. Moving to review.', stage:'specification', parent_id:null, created_at:_ts(5) },
                { id:_uuid(), author:{...EVE}, content:'Reviewing the quality rules and SLAs. The 6-hour freshness should work for daily reporting but let me confirm with the CFO.', stage:'review', parent_id:null, created_at:_ts(2) },
            ],
            history: [
                { from_stage:'request', to_stage:'discovery', transitioned_by:{...CAROL}, reason:null, timestamp:_ts(7) },
                { from_stage:'discovery', to_stage:'specification', transitioned_by:{...CAROL}, reason:null, timestamp:_ts(6) },
                { from_stage:'specification', to_stage:'review', transitioned_by:{...BOB}, reason:'Spec complete, ready for stakeholder review.', timestamp:_ts(3) },
            ],
            created_at: _ts(8),
            updated_at: _ts(1),
        };
    }

    // Session 3: ACTIVE — Product Catalog Dimension
    function _buildProductCatalog() {
        const id = _uuid();
        return {
            id, current_stage: 'active',
            request: {
                title: 'Product Catalog Dimension',
                description: 'Need a product dimension table for the analytics mart.',
                business_context: 'Product analytics team needs a reliable product dimension to join with order facts.',
                target_use_case: 'Product performance dashboards, category analytics.',
                urgency: 'medium',
                requester: { ...BOB },
                domain: 'Product',
                data_product: 'Product Analytics',
                desired_fields: [],
                created_at: _ts(14),
            },
            contract: {
                id: _uuid(), api_version: 'v3.1.0', kind: 'DataContract',
                name: 'Product Catalog Dimension', version: '1.0.0', status: 'active',
                domain: 'Product', tenant: null,
                data_product: 'Product Analytics',
                description_purpose: 'Provide a clean, enriched product dimension for joining with fact tables.',
                description_limitations: null, description_usage: null,
                tags: ['product','dimension','gold-tier'],
                schema_objects: [{
                    name: 'dim_products',
                    physical_name: 'ANALYTICS.MART.DIM_PRODUCTS',
                    description: 'Product dimension with enriched category hierarchy and pricing.',
                    source_tables: [
                        { name:'raw_products', qualified_name:'default/snowflake/PROD_DB/RAW/PRODUCTS', database_name:'PROD_DB', schema_name:'RAW', connector_name:'snowflake', description:'Product master data from the PIM system.', columns:[] },
                    ],
                    properties: [
                        { name:'product_id', logical_type:'integer', description:'Product PK.', required:true, primary_key:true, primary_key_position:1, unique:true, examples:[], sources:[{source_table:'raw_products',source_column:'product_id',source_table_qualified_name:'default/snowflake/PROD_DB/RAW/PRODUCTS',transform_logic:null,transform_description:null}] },
                        { name:'product_name', logical_type:'string', description:'Display name.', required:true, critical_data_element:true, examples:[], sources:[{source_table:'raw_products',source_column:'name',source_table_qualified_name:'default/snowflake/PROD_DB/RAW/PRODUCTS',transform_logic:null,transform_description:null}] },
                        { name:'category', logical_type:'string', description:'Product category (L1).', required:true, examples:[], sources:[{source_table:'raw_products',source_column:'category',source_table_qualified_name:'default/snowflake/PROD_DB/RAW/PRODUCTS',transform_logic:null,transform_description:null}] },
                        { name:'subcategory', logical_type:'string', description:'Product subcategory (L2).', required:false, examples:[], sources:[{source_table:'raw_products',source_column:'subcategory',source_table_qualified_name:'default/snowflake/PROD_DB/RAW/PRODUCTS',transform_logic:null,transform_description:null}] },
                        { name:'price', logical_type:'number', description:'Current list price in USD.', required:true, examples:[], sources:[{source_table:'raw_products',source_column:'list_price',source_table_qualified_name:'default/snowflake/PROD_DB/RAW/PRODUCTS',transform_logic:'ROUND(list_price, 2)',transform_description:null}] },
                        { name:'is_active', logical_type:'boolean', description:'Whether the product is currently available.', required:true, examples:[], sources:[{source_table:'raw_products',source_column:'status',source_table_qualified_name:'default/snowflake/PROD_DB/RAW/PRODUCTS',transform_logic:"CASE WHEN status = 'active' THEN TRUE ELSE FALSE END",transform_description:null}] },
                    ],
                }],
                quality_checks: [
                    { id:_uuid(), type:'sql', description:'product_id must be unique.', dimension:'uniqueness', severity:'critical', must_be:'unique', must_be_greater_than:null, must_be_less_than:null, metric:null, method:'field_health', column:'dim_products.product_id', schedule:'0 6 * * *', scheduler:null, engine:'monte-carlo', query:null, business_impact:'Duplicate product IDs cause incorrect joins in fact tables and inflate product counts.' },
                    { id:_uuid(), type:'sql', description:'price must be > 0.', dimension:'validity', severity:'high', must_be:null, must_be_greater_than:0.0, must_be_less_than:null, metric:null, method:'field_health', column:'dim_products.price', schedule:'0 6 * * *', scheduler:null, engine:'monte-carlo', query:null, business_impact:'Zero or negative prices corrupt revenue calculations when joined with order facts.' },
                ],
                sla_properties: [
                    { id:_uuid(), property:'freshness', value:'24', unit:'hours', description:'Daily refresh aligned with upstream PIM sync.', schedule:'0 4 * * *', scheduler:'airflow', driver:'analytics', element:'dim_products' },
                    { id:_uuid(), property:'availability', value:'99.5', unit:'percent', description:'Standard availability for dimension tables.', schedule:null, scheduler:null, driver:'operational', element:'dim_products' },
                ],
                team: [
                    { name:'Bob Martinez', email:'bob.martinez@acme.com', role:'Data Owner' },
                    { name:'Carol Wang', email:'carol.wang@acme.com', role:'Data Engineer' },
                ],
            },
            participants: [{ ...BOB },{ ...CAROL }],
            comments: [
                { id:_uuid(), author:{...BOB}, content:'Straightforward product dimension from the PIM source.', stage:'request', parent_id:null, created_at:_ts(14) },
                { id:_uuid(), author:{...CAROL}, content:'Source table identified. Simple 1:1 mapping with a few transforms.', stage:'discovery', parent_id:null, created_at:_ts(13) },
                { id:_uuid(), author:{...BOB}, content:'Spec looks clean. Approving.', stage:'review', parent_id:null, created_at:_ts(10) },
                { id:_uuid(), author:{...BOB}, content:'Approved and active. dbt model deployed.', stage:'approval', parent_id:null, created_at:_ts(9) },
            ],
            history: [
                { from_stage:'request', to_stage:'discovery', transitioned_by:{...CAROL}, reason:null, timestamp:_ts(13) },
                { from_stage:'discovery', to_stage:'specification', transitioned_by:{...CAROL}, reason:null, timestamp:_ts(12) },
                { from_stage:'specification', to_stage:'review', transitioned_by:{...CAROL}, reason:null, timestamp:_ts(11) },
                { from_stage:'review', to_stage:'approval', transitioned_by:{...BOB}, reason:'All checks pass.', timestamp:_ts(10) },
                { from_stage:'approval', to_stage:'active', transitioned_by:{...BOB}, reason:'Deployed to production.', timestamp:_ts(9) },
            ],
            created_at: _ts(14),
            updated_at: _ts(9),
        };
    }

    // Session 4: REQUEST — Marketing Attribution Model (fresh)
    function _buildMarketingAttribution() {
        const id = _uuid();
        return {
            id, current_stage: 'request',
            request: {
                title: 'Marketing Attribution Model',
                description: 'We need a multi-touch attribution table that credits marketing channels for conversions. This will power the marketing ROI dashboard.',
                business_context: 'The CMO has asked for a reliable attribution model to allocate the $5M quarterly ad budget more effectively. Current last-touch attribution is misleading.',
                target_use_case: 'Multi-touch attribution reporting, marketing channel ROI analysis, budget allocation optimization.',
                urgency: 'medium',
                requester: { ...DAVE },
                domain: 'Marketing Analytics',
                data_product: 'Marketing Intelligence',
                desired_fields: ['attribution_id','customer_id','conversion_date','channel','touchpoint_count','attributed_revenue','attribution_model'],
                created_at: _ts(1),
            },
            contract: {
                id: _uuid(), api_version: 'v3.1.0', kind: 'DataContract',
                name: 'Marketing Attribution', version: '0.1.0', status: 'proposed',
                domain: 'Marketing Analytics', tenant: null, data_product: null,
                description_purpose: null, description_limitations: null, description_usage: null,
                tags: [],
                schema_objects: [],
                quality_checks: [],
                sla_properties: [],
                team: [],
            },
            participants: [{ ...DAVE }],
            comments: [
                { id:_uuid(), author:{...DAVE}, content:'Submitting this request on behalf of the marketing analytics team. We need multi-touch attribution to replace the current last-touch model. Happy to discuss requirements in discovery.', stage:'request', parent_id:null, created_at:_ts(1) },
            ],
            history: [],
            created_at: _ts(1),
            updated_at: _ts(1),
        };
    }

    // Session 5: DISCOVERY — Daily Inventory Snapshot
    function _buildInventorySnapshot() {
        const id = _uuid();
        return {
            id, current_stage: 'discovery',
            request: {
                title: 'Daily Inventory Snapshot',
                description: 'Daily snapshot of inventory levels across all warehouses for supply chain optimization.',
                business_context: 'Supply chain team needs daily inventory visibility to optimize reorder points and prevent stockouts during peak season.',
                target_use_case: 'Inventory dashboards, stockout prediction, reorder point optimization.',
                urgency: 'high',
                requester: { ...EVE },
                domain: 'Supply Chain',
                data_product: 'Supply Chain Analytics',
                desired_fields: ['snapshot_date','warehouse_id','product_id','quantity_on_hand','quantity_reserved','reorder_point'],
                created_at: _ts(3),
            },
            contract: {
                id: _uuid(), api_version: 'v3.1.0', kind: 'DataContract',
                name: 'Daily Inventory Snapshot', version: '0.1.0', status: 'proposed',
                domain: 'Supply Chain', tenant: null, data_product: null,
                description_purpose: null, description_limitations: null, description_usage: null,
                tags: [],
                schema_objects: [],
                quality_checks: [],
                sla_properties: [],
                team: [],
            },
            participants: [{ ...EVE },{ ...CAROL }],
            comments: [
                { id:_uuid(), author:{...EVE}, content:'Peak season is in 6 weeks. We need inventory visibility ASAP.', stage:'request', parent_id:null, created_at:_ts(3) },
                { id:_uuid(), author:{...CAROL}, content:"I've found two potential source tables in the ERP system: raw_inventory_levels (real-time) and raw_warehouse_master (reference data). Let me check data quality.", stage:'discovery', parent_id:null, created_at:_ts(2) },
                { id:_uuid(), author:{...EVE}, content:'We also need the reorder_point from the supply planning system. That\'s in a separate Postgres database \u2014 can we pull that in too?', stage:'discovery', parent_id:null, created_at:_ts(1,12) },
            ],
            history: [
                { from_stage:'request', to_stage:'discovery', transitioned_by:{...CAROL}, reason:'Starting source discovery.', timestamp:_ts(2,6) },
            ],
            created_at: _ts(3),
            updated_at: _ts(1),
        };
    }

    // =====================================================================
    // Stage Validation
    // =====================================================================

    function _validateStageTransition(session, target) {
        const current = session.current_stage;
        if (current === 'active' || current === 'rejected')
            return "Cannot transition from terminal stage '" + current + "'";
        if (target === 'rejected') return null;
        const ci = STAGE_ORDER.indexOf(current);
        const ti = STAGE_ORDER.indexOf(target);
        if (ci < 0 || ti < 0) return 'Invalid transition: ' + current + ' -> ' + target;
        if (ti !== ci + 1) return 'Can only advance one stage at a time. Current: ' + current + ', requested: ' + target;
        if (target === 'specification') {
            if (!session.comments.some(c => c.stage === 'discovery'))
                return 'At least one discovery comment is required before moving to specification';
        } else if (target === 'review') {
            if (!session.contract.schema_objects.some(o => o.properties && o.properties.length > 0))
                return 'At least one table with one or more columns is required before review';
        } else if (target === 'approval') {
            if (!session.comments.some(c => c.stage === 'review'))
                return 'At least one review comment is required before approval';
        }
        return null;
    }

    // =====================================================================
    // ODCS YAML Serializer (port of odcs.py)
    // =====================================================================

    function _serializeProperty(prop) {
        const out = { name: prop.name, logicalType: prop.logical_type };
        if (prop.description) out.description = prop.description;
        if (prop.required) out.required = true;
        if (prop.primary_key) out.primaryKey = true;
        if (prop.primary_key_position != null) out.primaryKeyPosition = prop.primary_key_position;
        if (prop.unique) out.unique = true;
        if (prop.classification) out.classification = prop.classification;
        if (prop.critical_data_element) out.criticalDataElement = true;
        if (prop.examples && prop.examples.length) out.examples = prop.examples;
        if (prop.sources && prop.sources.length) {
            out.transformSourceObjects = prop.sources.map(s => s.source_table);
            const logic = prop.sources.map(s => s.transform_logic).filter(Boolean);
            if (logic.length) out.transformLogic = logic.join('; ');
            const desc = prop.sources.map(s => s.transform_description).filter(Boolean);
            if (desc.length) out.transformDescription = desc.join('; ');
        }
        return out;
    }

    function _serializeSchemaObject(obj) {
        const out = { name: obj.name };
        if (obj.physical_name) out.physicalName = obj.physical_name;
        if (obj.description) out.description = obj.description;
        if (obj.properties && obj.properties.length) out.properties = obj.properties.map(_serializeProperty);
        return out;
    }

    function _serializeQualityCheck(q) {
        const out = { type: q.type };
        if (q.description) out.description = q.description;
        if (q.dimension) out.dimension = q.dimension;
        if (q.metric) out.metric = q.metric;
        if (q.severity) out.severity = q.severity;
        if (q.must_be != null) out.mustBe = q.must_be;
        if (q.must_be_greater_than != null) out.mustBeGreaterThan = q.must_be_greater_than;
        if (q.must_be_less_than != null) out.mustBeLessThan = q.must_be_less_than;
        if (q.schedule) out.schedule = q.schedule;
        if (q.scheduler) out.scheduler = q.scheduler;
        if (q.business_impact) out.businessImpact = q.business_impact;
        if (q.method) out.method = q.method;
        if (q.column) out.column = q.column;
        if (q.query) out.query = q.query;
        if (q.engine) out.engine = q.engine;
        return out;
    }

    function _serializeSLA(s) {
        const out = { property: s.property, value: s.value };
        if (s.unit) out.unit = s.unit;
        if (s.description) out.description = s.description;
        if (s.schedule) out.schedule = s.schedule;
        if (s.scheduler) out.scheduler = s.scheduler;
        if (s.driver) out.driver = s.driver;
        if (s.element) out.element = s.element;
        return out;
    }

    function _contractToOdcsDict(contract) {
        const odcs = {
            apiVersion: contract.api_version || 'v3.1.0',
            kind: contract.kind || 'DataContract',
            id: contract.id,
            version: contract.version || '0.1.0',
            status: contract.status || 'draft',
        };
        if (contract.name) odcs.name = contract.name;
        if (contract.domain) odcs.domain = contract.domain;
        if (contract.tenant) odcs.tenant = contract.tenant;
        if (contract.data_product) odcs.dataProduct = contract.data_product;
        if (contract.tags && contract.tags.length) odcs.tags = contract.tags;
        const desc = {};
        if (contract.description_purpose) desc.purpose = contract.description_purpose;
        if (contract.description_limitations) desc.limitations = contract.description_limitations;
        if (contract.description_usage) desc.usage = contract.description_usage;
        if (Object.keys(desc).length) odcs.description = desc;
        if (contract.schema_objects && contract.schema_objects.length)
            odcs.schema = contract.schema_objects.map(_serializeSchemaObject);
        if (contract.quality_checks && contract.quality_checks.length)
            odcs.quality = contract.quality_checks.map(_serializeQualityCheck);
        if (contract.sla_properties && contract.sla_properties.length)
            odcs.slaProperties = contract.sla_properties.map(_serializeSLA);
        if (contract.team && contract.team.length)
            odcs.team = contract.team.map(t => ({ name: t.name, email: t.email, role: t.role }));
        return odcs;
    }

    function _contractToYaml(contract) {
        if (typeof jsyaml !== 'undefined') {
            return jsyaml.dump(_contractToOdcsDict(contract), { sortKeys: false, lineWidth: -1 });
        }
        return JSON.stringify(_contractToOdcsDict(contract), null, 2);
    }

    // =====================================================================
    // Route Handlers
    // =====================================================================

    function _json(body, status) {
        return new Response(JSON.stringify(body), {
            status: status || 200,
            headers: { 'Content-Type': 'application/json' },
        });
    }

    function _text(body, status) {
        return new Response(body, {
            status: status || 200,
            headers: { 'Content-Type': 'text/plain; charset=utf-8' },
        });
    }

    function _err(detail, status) {
        return _json({ detail: detail }, status || 400);
    }

    function handleApiRequest(method, url, body) {
        // Parse URL and query params
        const [path, qs] = url.split('?');
        const parts = path.replace(/^\/api\//, '').split('/').filter(Boolean);
        const params = new URLSearchParams(qs || '');

        // --- Atlan ---
        if (parts[0] === 'atlan') {
            if (parts[1] === 'status') return _json({ configured: false });
            return _err('Atlan not configured in demo mode', 503);
        }

        // --- Demo seed ---
        if (parts[0] === 'demo' && parts[1] === 'seed' && method === 'POST') {
            _store.clear();
            seedDemoData();
            return _json({ ok: true, count: _store.size });
        }

        // --- Sessions list ---
        if (parts[0] === 'sessions' && parts.length === 1) {
            if (method === 'GET') {
                let sessions = Array.from(_store.values());
                const stageFilter = params.get('stage');
                if (stageFilter) sessions = sessions.filter(s => s.current_stage === stageFilter);
                sessions.sort((a, b) => new Date(b.updated_at) - new Date(a.updated_at));
                return _json(sessions.map(_sessionSummary));
            }
            if (method === 'POST') {
                const id = _uuid();
                const now = new Date().toISOString();
                const session = {
                    id, current_stage: 'request',
                    request: {
                        title: body.title || 'Untitled',
                        description: body.description || '',
                        business_context: body.business_context || '',
                        target_use_case: body.target_use_case || '',
                        urgency: body.urgency || 'medium',
                        requester: { name: body.requester_name || 'Anonymous', email: body.requester_email || '' },
                        domain: body.domain || '',
                        data_product: body.data_product || '',
                        desired_fields: body.desired_fields ? body.desired_fields.split(',').map(f => f.trim()).filter(Boolean) : [],
                        created_at: now,
                    },
                    contract: {
                        id: _uuid(), api_version: 'v3.1.0', kind: 'DataContract',
                        name: body.title || 'Untitled', version: '0.1.0', status: 'proposed',
                        domain: body.domain || '', tenant: null, data_product: body.data_product || '',
                        description_purpose: null, description_limitations: null, description_usage: null,
                        tags: [], schema_objects: [], quality_checks: [], sla_properties: [], team: [],
                    },
                    participants: [{ name: body.requester_name || 'Anonymous', email: body.requester_email || '' }],
                    comments: [],
                    history: [],
                    created_at: now,
                    updated_at: now,
                };
                _store.set(id, session);
                return _json({ id: id });
            }
        }

        // --- Single session ---
        if (parts[0] === 'sessions' && parts.length >= 2) {
            const sessionId = parts[1];
            const session = _getSession(sessionId);
            if (!session) return _err('Session not found', 404);

            // GET /api/sessions/{id}
            if (parts.length === 2 && method === 'GET') return _json(session);

            // DELETE /api/sessions/{id}
            if (parts.length === 2 && method === 'DELETE') {
                _store.delete(sessionId);
                return _json({ ok: true });
            }

            // PUT /api/sessions/{id}/stage
            if (parts[2] === 'stage' && method === 'PUT') {
                const target = body.stage;
                const error = _validateStageTransition(session, target);
                if (error) return _err(error, 400);
                const prev = session.current_stage;
                session.current_stage = target;
                if (STAGE_TO_STATUS[target]) session.contract.status = STAGE_TO_STATUS[target];
                session.history.push({
                    from_stage: prev, to_stage: target,
                    transitioned_by: body.transitioned_by || { name: 'Demo User', email: 'demo@acme.com' },
                    reason: body.reason || null,
                    timestamp: new Date().toISOString(),
                });
                _saveSession(session);
                return _json({ stage: target });
            }

            // --- Comments ---
            if (parts[2] === 'comments') {
                if (method === 'GET') {
                    let comments = session.comments;
                    const sf = params.get('stage');
                    if (sf) comments = comments.filter(c => c.stage === sf);
                    return _json(comments);
                }
                if (method === 'POST') {
                    const comment = {
                        id: _uuid(),
                        author: body.author || { name: 'Demo User', email: 'demo@acme.com' },
                        content: body.content,
                        stage: body.stage || session.current_stage,
                        parent_id: body.parent_id || null,
                        created_at: new Date().toISOString(),
                    };
                    session.comments.push(comment);
                    _saveSession(session);
                    return _json({ ok: true, comment: comment });
                }
            }

            // --- Contract routes ---
            if (parts[2] === 'contract') {
                const contract = session.contract;

                // PUT metadata
                if (parts[3] === 'metadata' && method === 'PUT') {
                    const fields = ['name','version','domain','data_product','tenant',
                        'description_purpose','description_limitations','description_usage','tags'];
                    fields.forEach(f => { if (body[f] !== undefined) contract[f] = body[f]; });
                    _saveSession(session);
                    return _json({ ok: true });
                }

                // YAML
                if (parts[3] === 'yaml' && method === 'GET') {
                    return _text(_contractToYaml(contract));
                }
                if (parts[3] === 'download' && method === 'GET') {
                    return _text(_contractToYaml(contract));
                }

                // --- Schema objects ---
                if (parts[3] === 'objects') {
                    // POST new object
                    if (parts.length === 4 && method === 'POST') {
                        const obj = {
                            name: body.name,
                            physical_name: body.physical_name || '',
                            description: body.description || '',
                            source_tables: [],
                            properties: [],
                        };
                        contract.schema_objects.push(obj);
                        _saveSession(session);
                        return _json({ ok: true });
                    }

                    if (parts.length >= 5) {
                        const objName = decodeURIComponent(parts[4]);
                        const obj = _findObject(session, objName);

                        // PUT update object
                        if (parts.length === 5 && method === 'PUT') {
                            if (obj) {
                                if (body.name !== undefined) obj.name = body.name;
                                if (body.physical_name !== undefined) obj.physical_name = body.physical_name;
                                if (body.description !== undefined) obj.description = body.description;
                                _saveSession(session);
                            }
                            return _json({ ok: true });
                        }

                        // DELETE object
                        if (parts.length === 5 && method === 'DELETE') {
                            contract.schema_objects = contract.schema_objects.filter(o => o.name !== objName);
                            _saveSession(session);
                            return _json({ ok: true });
                        }

                        if (!obj) return _err('Object not found: ' + objName, 404);

                        // --- Sources ---
                        if (parts[5] === 'sources') {
                            if (method === 'POST' && parts.length === 6) {
                                obj.source_tables = obj.source_tables || [];
                                obj.source_tables.push(body);
                                _saveSession(session);
                                return _json({ ok: true });
                            }
                            if (method === 'DELETE' && parts.length === 7) {
                                const idx = parseInt(parts[6]);
                                if (obj.source_tables && idx >= 0 && idx < obj.source_tables.length) {
                                    obj.source_tables.splice(idx, 1);
                                    _saveSession(session);
                                }
                                return _json({ ok: true });
                            }
                        }

                        // GET source-columns
                        if (parts[5] === 'source-columns' && method === 'GET') {
                            const result = {};
                            (obj.source_tables || []).forEach(st => {
                                result[st.name] = st.columns || [];
                            });
                            return _json(result);
                        }

                        // POST map-columns
                        if (parts[5] === 'map-columns' && method === 'POST') {
                            const mappings = body.mappings || body;
                            if (Array.isArray(mappings)) {
                                mappings.forEach(m => {
                                    if (!obj.properties.find(p => p.name === m.name)) {
                                        obj.properties.push({
                                            name: m.name,
                                            logical_type: m.logical_type || 'string',
                                            description: m.description || '',
                                            required: false,
                                            examples: [],
                                            sources: m.sources || [],
                                        });
                                    }
                                });
                            }
                            _saveSession(session);
                            return _json({ ok: true });
                        }

                        // POST import-from-atlan
                        if (parts[5] === 'import-from-atlan' && method === 'POST') {
                            return _err('Atlan not configured in demo mode', 503);
                        }

                        // --- Properties ---
                        if (parts[5] === 'properties') {
                            // POST new property
                            if (parts.length === 6 && method === 'POST') {
                                // Check for reorder request
                                if (body.action === 'reorder' || body.reorder) {
                                    const idx = obj.properties.findIndex(p => p.name === body.name);
                                    if (idx >= 0) {
                                        const dir = body.direction || body.reorder;
                                        if (dir === 'up' && idx > 0) {
                                            [obj.properties[idx-1], obj.properties[idx]] = [obj.properties[idx], obj.properties[idx-1]];
                                        } else if (dir === 'down' && idx < obj.properties.length - 1) {
                                            [obj.properties[idx], obj.properties[idx+1]] = [obj.properties[idx+1], obj.properties[idx]];
                                        }
                                    }
                                    _saveSession(session);
                                    return _json({ ok: true });
                                }
                                const prop = {
                                    name: body.name,
                                    logical_type: body.logical_type || 'string',
                                    description: body.description || '',
                                    required: !!body.required,
                                    primary_key: !!body.primary_key,
                                    primary_key_position: body.primary_key_position || null,
                                    unique: !!body.unique,
                                    classification: body.classification || '',
                                    critical_data_element: !!body.critical_data_element,
                                    examples: body.examples || [],
                                    sources: body.sources || [],
                                };
                                obj.properties.push(prop);
                                _saveSession(session);
                                return _json({ ok: true });
                            }

                            // POST reorder
                            if (parts.length === 7 && parts[6] === 'reorder' && method === 'POST') {
                                const idx = obj.properties.findIndex(p => p.name === body.name);
                                if (idx >= 0) {
                                    if (body.direction === 'up' && idx > 0) {
                                        [obj.properties[idx-1], obj.properties[idx]] = [obj.properties[idx], obj.properties[idx-1]];
                                    } else if (body.direction === 'down' && idx < obj.properties.length - 1) {
                                        [obj.properties[idx], obj.properties[idx+1]] = [obj.properties[idx+1], obj.properties[idx]];
                                    }
                                }
                                _saveSession(session);
                                return _json({ ok: true });
                            }

                            if (parts.length >= 7) {
                                const propName = decodeURIComponent(parts[6]);
                                const prop = _findProperty(obj, propName);

                                // PUT update property
                                if (parts.length === 7 && method === 'PUT') {
                                    if (prop) {
                                        const fields = ['name','logical_type','description','required','primary_key',
                                            'primary_key_position','unique','classification','critical_data_element','examples'];
                                        fields.forEach(f => { if (body[f] !== undefined) prop[f] = body[f]; });
                                    }
                                    _saveSession(session);
                                    return _json({ ok: true });
                                }

                                // DELETE property
                                if (parts.length === 7 && method === 'DELETE') {
                                    obj.properties = obj.properties.filter(p => p.name !== propName);
                                    _saveSession(session);
                                    return _json({ ok: true });
                                }

                                // --- Column sources (lineage) ---
                                if (parts[7] === 'sources' && prop) {
                                    prop.sources = prop.sources || [];
                                    if (method === 'POST' && parts.length === 8) {
                                        prop.sources.push(body);
                                        _saveSession(session);
                                        return _json({ ok: true });
                                    }
                                    if (parts.length === 9) {
                                        const idx = parseInt(parts[8]);
                                        if (method === 'PUT') {
                                            if (idx >= 0 && idx < prop.sources.length)
                                                Object.assign(prop.sources[idx], body);
                                            _saveSession(session);
                                            return _json({ ok: true });
                                        }
                                        if (method === 'DELETE') {
                                            if (idx >= 0 && idx < prop.sources.length)
                                                prop.sources.splice(idx, 1);
                                            _saveSession(session);
                                            return _json({ ok: true });
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                // --- Quality checks ---
                if (parts[3] === 'quality') {
                    if (parts.length === 4 && method === 'POST') {
                        const q = { id: _uuid(), ...body };
                        contract.quality_checks.push(q);
                        _saveSession(session);
                        return _json({ ok: true });
                    }
                    if (parts.length === 5) {
                        const qid = parts[4];
                        if (method === 'PUT') {
                            const q = contract.quality_checks.find(c => c.id === qid);
                            if (q) Object.assign(q, body);
                            _saveSession(session);
                            return _json({ ok: true });
                        }
                        if (method === 'DELETE') {
                            contract.quality_checks = contract.quality_checks.filter(c => c.id !== qid);
                            _saveSession(session);
                            return _json({ ok: true });
                        }
                    }
                }

                // --- SLA properties ---
                if (parts[3] === 'sla') {
                    if (parts.length === 4 && method === 'POST') {
                        const s = { id: _uuid(), ...body };
                        contract.sla_properties.push(s);
                        _saveSession(session);
                        return _json({ ok: true });
                    }
                    // DELETE by-id
                    if (parts.length === 6 && parts[4] === 'by-id' && method === 'DELETE') {
                        const sid = parts[5];
                        contract.sla_properties = contract.sla_properties.filter(s => s.id !== sid);
                        _saveSession(session);
                        return _json({ ok: true });
                    }
                    if (parts.length === 5) {
                        const slaIdOrIdx = parts[4];
                        if (method === 'PUT') {
                            const s = contract.sla_properties.find(s => s.id === slaIdOrIdx);
                            if (s) Object.assign(s, body);
                            _saveSession(session);
                            return _json({ ok: true });
                        }
                        if (method === 'DELETE') {
                            // Try by ID first, then by index
                            const byId = contract.sla_properties.findIndex(s => s.id === slaIdOrIdx);
                            if (byId >= 0) {
                                contract.sla_properties.splice(byId, 1);
                            } else {
                                const idx = parseInt(slaIdOrIdx);
                                if (!isNaN(idx) && idx >= 0 && idx < contract.sla_properties.length)
                                    contract.sla_properties.splice(idx, 1);
                            }
                            _saveSession(session);
                            return _json({ ok: true });
                        }
                    }
                }

                // --- Team ---
                if (parts[3] === 'team') {
                    if (parts.length === 4 && method === 'POST') {
                        contract.team.push(body);
                        _saveSession(session);
                        return _json({ ok: true });
                    }
                    if (parts.length === 5 && method === 'DELETE') {
                        const idx = parseInt(parts[4]);
                        if (!isNaN(idx) && idx >= 0 && idx < contract.team.length)
                            contract.team.splice(idx, 1);
                        _saveSession(session);
                        return _json({ ok: true });
                    }
                }
            }
        }

        // --- Bulk import ---
        if (parts[0] === 'sessions' && parts[2] === 'contract' && parts[3] === 'objects' && parts[4] === 'bulk-import-from-atlan') {
            return _err('Atlan not configured in demo mode', 503);
        }

        // Unmatched route
        return _err('Not found: ' + method + ' ' + url, 404);
    }

    // =====================================================================
    // Fetch Interceptor
    // =====================================================================

    const _origFetch = window.fetch;
    window.fetch = async function (input, init) {
        init = init || {};
        const url = typeof input === 'string' ? input : (input && input.url ? input.url : String(input));
        const method = (init.method || 'GET').toUpperCase();

        if (url.startsWith('/api/')) {
            let body = null;
            if (init.body) {
                try { body = JSON.parse(init.body); } catch (e) { body = init.body; }
            }
            const response = handleApiRequest(method, url, body);
            if (response) return response;
        }

        return _origFetch.call(window, input, init);
    };

    // =====================================================================
    // URL Routing Patches for GitHub Pages
    // =====================================================================

    // Patch dashboard links: /contract/{id} → contract.html?id={id}
    if (typeof DDLC !== 'undefined') {
        const origRender = DDLC.dashboard.renderSessions;
        DDLC.dashboard.renderSessions = function () {
            origRender.call(this);
            document.querySelectorAll('a[href^="/contract/"]').forEach(a => {
                const m = a.getAttribute('href').match(/\/contract\/(.+)/);
                if (m) a.setAttribute('href', 'contract.html?id=' + m[1]);
            });
            // Fix empty-state "New Request" link
            document.querySelectorAll('a[href="/request"]').forEach(a => {
                a.setAttribute('href', 'request.html');
            });
        };

        // Patch request form redirect
        const origSubmit = DDLC.request.submit;
        DDLC.request.submit = async function (event) {
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
                window.location.href = 'contract.html?id=' + result.id;
            } catch (err) {
                DDLC.toast.show(err.message, 'error');
                btn.disabled = false;
                btn.textContent = 'Submit Request';
            }
            return false;
        };
    }

    // Patch ContractApp to read session ID from query param
    if (typeof ContractApp !== 'undefined') {
        const origInit = ContractApp.init;
        ContractApp.init = async function () {
            const params = new URLSearchParams(window.location.search);
            if (params.has('id')) {
                this.sessionId = params.get('id');
            } else {
                // Fallback to pathname parsing
                const pathParts = window.location.pathname.split('/');
                this.sessionId = pathParts[pathParts.length - 1];
            }
            try {
                const status = await DDLC.api.fetchJSON('/api/atlan/status');
                this.atlanConfigured = status.configured;
            } catch (e) { this.atlanConfigured = false; }
            await this.load();
        };

        // Patch downloadYaml to use Blob download
        const origDownload = ContractApp.downloadYaml;
        ContractApp.downloadYaml = function () {
            const session = _getSession(this.sessionId);
            if (!session) return;
            const yaml = _contractToYaml(session.contract);
            const blob = new Blob([yaml], { type: 'application/x-yaml' });
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = (session.contract.name || 'contract').replace(/\s+/g, '_').toLowerCase() + '.odcs.yaml';
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);
            URL.revokeObjectURL(url);
        };
    }

    // =====================================================================
    // Seed data on load
    // =====================================================================

    seedDemoData();

    // =====================================================================
    // Demo mode banner
    // =====================================================================

    document.addEventListener('DOMContentLoaded', () => {
        const banner = document.createElement('div');
        banner.style.cssText = 'background: linear-gradient(90deg, #6366f1 0%, #8b5cf6 100%); color: white; text-align: center; padding: 8px 16px; font-size: 13px; font-weight: 500; position: relative; z-index: 1000;';
        banner.innerHTML = '\u{1F680} <strong>Demo Mode</strong> \u2014 Explore the full DDLC experience. Changes are stored in-memory and reset on page refresh. <a href="https://github.com/andrew-lentz-atlan/ddlc" style="color: #e0e7ff; text-decoration: underline; margin-left: 8px;">View on GitHub \u2192</a>';
        document.body.prepend(banner);
    });

})();
