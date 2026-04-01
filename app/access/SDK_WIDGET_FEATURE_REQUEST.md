# Feature Request: SDK Widget Forms on Asset Profile Tabs

## Problem

The only SDK-native UI surface (config-map-driven setup forms) is not available on asset profile tabs. Asset tabs today require an externally hosted iframe app, whereas setup forms are rendered natively from config maps without any customer-managed frontend hosting.

This gap blocks a class of apps that need to present **contextual, dynamic forms on asset pages** — such as access request workflows, data quality rule editors, or approval flows — from shipping as fully SDK-native apps.

## Concrete Use Case: ServiceNow Access Request

We're building an app that adds an "Access" tab to Data Product pages. When a user views a Data Product in Atlan, the tab presents a ServiceNow access request form so they can request access to that product's underlying AD group.

### What the form needs

| Field | Widget Type | Notes |
|-------|-------------|-------|
| Requested for | TextInput | Pre-populated from logged-in user context |
| What do you want to do? | DropDown | Add / Remove / Modify access |
| Select Environment | DropDown | Production & Non-Production / Production Only / Non-Production Only |
| Select Role Type | DropDown | Data Product Role / Source System Role / Curated Domain Role |
| Select Data Product | DropDown | Choices filtered by asset context; cascades to filter "Select Access" |
| Select Access | DropDown | Choices filtered by selected Data Product |
| Access description | TextInput (read-only) | Auto-populated based on Data Product + Access selection |
| Policy acknowledgment | BooleanInput | Mandatory checkbox with associated policy text |
| Business Justification | TextInput (multi-line) | Free-text, mandatory |
| Additional comments | TextInput (multi-line) | Optional |

### What happens on submit

A Temporal activity calls the ServiceNow Service Catalog API (`POST /api/sn_sc/servicecatalog/items/{sys_id}/order_now`) with the form values + ServiceNow OAuth credentials stored in the app's config. No frontend-to-external-service communication needed.

## What We're Asking For

### 1. Widget-based form rendering in asset profile tab slots

Allow an app to declare that an `asset-profile-tab` slot should render a config-map-style widget form instead of an iframe.

Today an app registers a tab like:

```yaml
render_at:
  - slot: asset-profile-tab
    when:
      assetTypes: [DataProduct]
```

This renders an iframe. We'd need something like:

```yaml
render_at:
  - slot: asset-profile-tab
    when:
      assetTypes: [DataProduct]
    form: access-request-form  # references a config map definition
```

Where `access-request-form` is defined using the existing config map JSON schema (DropDown, TextInput, BooleanInput, etc.) and rendered natively by the Atlan frontend — same as setup forms are today.

### 2. Asset context passed to the form

The form needs to know which asset the user is viewing. The tab renderer should inject context into the form (or make it available to dynamic field resolution):

- **Asset qualified name** (e.g. `default/snowflake/production/my_data_product`)
- **Asset GUID**
- **Asset type** (e.g. `DataProduct`)
- **Logged-in user** (name, email — for pre-populating "Requested for")

This context would allow:
- Resolving which ServiceNow catalog item to use (different forms per data product type)
- Pre-populating fields with asset and user information
- Filtering dropdown choices based on the asset

### 3. Submission triggers a workflow/activity

When the user clicks submit, the SDK should invoke a Temporal workflow or activity with the collected form values + asset context. The app's backend handles the rest (calling ServiceNow, etc.).

This mirrors how setup forms work today — the config is collected by the frontend and passed to the backend — but scoped to an asset-tab interaction rather than app configuration.

### 4. Dynamic/cascading field support (nice-to-have)

The customer's real ServiceNow form has cascading dropdowns: selecting a Data Product filters the available Access Roles. Ideally the widget system would support:

- **Dependent fields**: Field B's choices are filtered based on Field A's value
- **Computed fields**: Field C's value is auto-set based on Field A + B (e.g. access description)
- **Conditional visibility**: Show/hide fields based on other field values

If full cascading support is complex, even a basic "on-change callback to the backend that returns updated field definitions" would work — the backend already has the mapping logic.

## Current Workaround

We've built the form as a standalone FastAPI app serving vanilla HTML/JS, rendered in the asset tab iframe. This works for local demos but can't ship to customers because:

- It requires hosting a frontend server outside Atlan
- Customer infrastructure teams would need to provision and maintain it
- It bypasses the SDK's deployment, scaling, and security model

The backend logic (ServiceNow API integration, OAuth, catalog item resolution) is already implemented as standard Python that could run as Temporal activities. The only gap is the UI surface.

## Impact

This capability would unblock not just the ServiceNow access request app, but any app that needs to present a form on an asset page:

- **Access request workflows** (ServiceNow, Jira Service Management, custom)
- **Data quality rule editors** (inline rule creation on table/column pages)
- **Approval flows** (request/approve metadata changes)
- **Feedback forms** (data consumer feedback on asset pages)
- **Custom metadata editors** (structured input for custom metadata beyond what the native UI provides)

All of these follow the same pattern: contextual form on an asset tab, submission triggers a backend workflow.
