# DDLC - Data Contract Development Lifecycle

**A collaborative platform for building, reviewing, and managing data contracts using the [ODCS v3.1.0](https://bitol.io/spec/open-data-contract-standard/) standard — powered by [Atlan](https://atlan.com).**

> Built during the Atlan Hackathon 2025 as an [Atlan Application SDK](https://developer.atlan.com/getting-started/) app.

---

## The Problem

Data contracts are the missing handshake between data producers and consumers. Today, teams negotiate data agreements in Slack threads, Confluence pages, and email chains — leading to:

- **Tribal knowledge** about what data means, where it comes from, and who owns it
- **No standard format** for expressing expectations around schema, quality, and SLAs
- **No lifecycle** — contracts are static docs that rot, not living artifacts that evolve
- **Disconnected from the catalog** — contracts don't link back to the actual assets in your data platform

## The Solution

**DDLC** (Data Contract Development Lifecycle) brings structure to the data contract process with a guided, stage-based workflow that produces machine-readable [ODCS v3.1.0](https://bitol.io/spec/open-data-contract-standard/) contracts — directly connected to your Atlan metadata catalog.

```
REQUEST  -->  DISCOVERY  -->  SPECIFICATION  -->  REVIEW  -->  APPROVAL  -->  ACTIVE
   |              |                |               |              |             |
Consumer      Stakeholder     Schema + Quality   Peer Review   Sign-off    Published
 intake       discussion       + SLA + Lineage   + Comments    + Gates      Contract
```

---

## What's Been Built (Phase 1)

### Core Lifecycle Engine
- **7-stage workflow**: REQUEST > DISCOVERY > SPECIFICATION > REVIEW > APPROVAL > ACTIVE (or REJECTED from any stage)
- **Validation gates**: Each stage transition enforces business rules (e.g., can't advance to REVIEW without at least one table with columns)
- **Audit trail**: Full history of stage transitions with who/when/why
- **Threaded comments**: Per-stage discussion threads for collaborative review

### ODCS v3.1.0 Contract Builder
- **Schema designer**: Add tables/topics with typed columns (STRING, INTEGER, BOOLEAN, TIMESTAMP, etc.)
- **Column-level lineage**: Track where each field comes from — source table, source column, and transform logic
- **Quality checks**: Define rules with dimensions (uniqueness, completeness, freshness, etc.) and Monte Carlo-compatible fields
- **SLA properties**: Set expectations for freshness, availability, latency, retention — with Airflow-compatible scheduling fields
- **Team management**: Assign data product owners, domain experts, and stewards
- **Live YAML preview**: See the ODCS v3.1.0 output in real-time as you build, and download the `.odcs.yaml` file

### Atlan Integration
- **Search & discover**: Find tables, views, and materialized views from your Atlan catalog
- **One-click import**: Pull columns from Atlan assets directly into your contract schema — with types auto-mapped
- **Bulk import**: Select multiple source tables and create schema objects in batch
- **Source lineage**: Link contract fields back to their Atlan-cataloged source assets
- **Data product & domain lookup**: Search Atlan for existing products and domains to associate with contracts

### Frontend
- **Dashboard**: See all contracts at a glance with stats by stage, urgency, and domain
- **Contract builder**: Full-featured editor that adapts to the current lifecycle stage
- **Stage stepper**: Visual progress indicator showing where you are in the DDLC
- **Dark theme**: Clean, modern UI built with vanilla HTML/CSS/JS

### Demo Data
- **5 pre-seeded sessions** across different stages so you can explore the full lifecycle immediately:
  - Customer 360 (Specification) — richly detailed with lineage, quality, and SLAs
  - Order Events Fact (Review) — finance-focused with revenue metrics
  - Product Catalog (Active) — fully approved and deployed
  - Marketing Attribution (Request) — fresh intake
  - Daily Inventory Snapshot (Discovery) — in-progress discussion

---

## Quick Start

### Prerequisites
- Python 3.11+
- [uv](https://docs.astral.sh/uv/) package manager

### Run it

```bash
cd hello_world
uv sync                              # Install dependencies
uv run python -m app.ddlc.server     # Start the DDLC server
```

Open **http://localhost:8002** and explore the 5 demo sessions.

### Optional: Atlan Integration

To connect to your Atlan tenant for live metadata search and import, set these environment variables:

```bash
export ATLAN_BASE_URL=https://your-tenant.atlan.com
export ATLAN_API_KEY=your-api-key
```

Without these, the app runs fully standalone — Atlan features are gracefully disabled.

---

## Architecture

```
hello_world/
├── app/
│   └── ddlc/
│       ├── server.py           # FastAPI server (60+ REST endpoints)
│       ├── models.py           # Pydantic data models (DDLCSession aggregate root)
│       ├── store.py            # Async in-memory state store (swappable to Dapr)
│       ├── odcs.py             # ODCS v3.1.0 YAML serializer
│       ├── atlan_assets.py     # Atlan pyatlan integration (search, import)
│       ├── demo_seed.py        # Pre-populated demo data
│       └── frontend/
│           ├── index.html      # Dashboard
│           ├── request.html    # New request form
│           ├── contract.html   # Contract builder + detail view
│           └── static/
│               ├── ddlc.css    # Dark theme styles
│               ├── ddlc.js     # Dashboard + form logic
│               ├── contract-builder.js  # Contract editor UI
│               └── yaml-preview.js      # YAML preview panel
├── pyproject.toml              # Dependencies + poe tasks
└── uv.lock                     # Locked dependency versions
```

### Design Decisions

| Decision | Choice | Why |
|---|---|---|
| **Aggregate root** | `DDLCSession` holds everything | One key = one contract lifecycle. Simple to reason about, serialize, and sync |
| **State store** | In-memory dict with async interface | Fast for hackathon; interface is 1:1 swappable to Dapr statestore for production |
| **ODCS format** | v3.1.0 with camelCase keys | Latest standard. Machine-readable, vendor-neutral, growing ecosystem adoption |
| **Frontend** | Vanilla HTML/CSS/JS | Zero build step. Fast iteration. No framework lock-in |
| **Atlan integration** | pyatlan FluentSearch | Official SDK. Handles auth, pagination, type mapping cleanly |

---

## Roadmap

### Phase 2 — Intelligence & Automation
- [ ] **AI-assisted specification**: Auto-suggest schema, quality rules, and SLAs from source metadata + business context
- [ ] **Smart lineage**: Infer column mappings from naming patterns and data profiling
- [ ] **Contract templates**: Start from industry or domain-specific templates
- [ ] **Diff & versioning**: Track changes between contract versions

### Phase 3 — Governance Integration
- [ ] **Publish to Atlan**: Push approved contracts back as Atlan assets with lineage
- [ ] **MDLH queries**: Read from Metadata Lakehouse for bulk metadata analysis
- [ ] **Notification workflows**: Slack/email alerts for stage transitions and review requests
- [ ] **RBAC**: Role-based access (requester, steward, approver)

### Phase 4 — Enforcement & Monitoring
- [ ] **Contract-as-code**: Generate dbt tests, Great Expectations suites, or Monte Carlo monitors from quality checks
- [ ] **Runtime validation**: Compare live data against contract SLAs
- [ ] **Drift detection**: Alert when source schemas change vs. contracted expectations
- [ ] **Airflow DAG generation**: Auto-generate scheduling from SLA properties

---

## ODCS v3.1.0 Compliance

The contracts produced by DDLC follow the [Open Data Contract Standard v3.1.0](https://bitol.io/spec/open-data-contract-standard/):

- `apiVersion: v3.1.0`
- camelCase field naming (`dataProduct`, `logicalType`, `primaryKey`, `slaProperties`)
- Nested `description` block with `purpose`, `limitations`, `usage`
- Schema objects with typed properties and classification
- Quality checks with dimensions and scheduling
- SLA properties with driver/element structure
- Team members with roles

---

## Tech Stack

| Component | Technology |
|---|---|
| Backend | Python 3.11+ / FastAPI |
| Data Models | Pydantic v2 |
| YAML Generation | PyYAML |
| Atlan Integration | pyatlan (FluentSearch) |
| State Management | In-memory (Dapr-ready interface) |
| Frontend | Vanilla HTML / CSS / JavaScript |
| Package Manager | uv |
| Application Framework | Atlan Application SDK |

---

## API Reference

The DDLC server exposes **60+ REST endpoints** organized by resource:

| Resource | Endpoints | Description |
|---|---|---|
| Sessions | `POST/GET/DELETE /api/sessions` | CRUD for DDLC sessions |
| Stage | `PUT /api/sessions/{id}/stage` | Advance lifecycle stage |
| Contract Metadata | `PUT /api/sessions/{id}/contract/metadata` | Update name, domain, version, descriptions |
| Schema Objects | `POST/PUT/DELETE /api/sessions/{id}/contract/objects` | Manage tables/topics |
| Properties | `POST/PUT/DELETE .../objects/{name}/properties` | Manage columns/fields |
| Quality Checks | `POST/PUT/DELETE .../contract/quality` | Define quality rules |
| SLA Properties | `POST/PUT/DELETE .../contract/sla` | Set SLA expectations |
| Team | `POST/DELETE .../contract/team` | Manage team members |
| Comments | `POST/GET .../comments` | Stage-threaded discussion |
| YAML Export | `GET .../contract/yaml` | Live ODCS preview + download |
| Lineage | `POST .../sources`, `POST .../map-columns` | Source table + column lineage |
| Atlan | `GET /api/atlan/*` | Search tables, columns, products, domains |
| Demo | `POST /api/demo/seed` | Re-seed demo data |

---

## Contributing

This project is in active development during the Atlan Hackathon. We welcome feedback, ideas, and contributions!

1. Fork the repo
2. Create a feature branch (`git checkout -b feature/amazing-thing`)
3. Commit your changes
4. Push and open a PR

---

## License

Built with the [Atlan Application SDK](https://developer.atlan.com/getting-started/). See [pyproject.toml](./pyproject.toml) for dependencies.
