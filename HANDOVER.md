# DDLC "Data Studio" — Handover & Finish-the-Job Guide

> **Audience:** the engineer taking this over.
> **Goal of the project:** get DDLC deployed as a **native Atlan app** on a real AtlanHQ tenant — fully contained inside Atlan (left-nav entry → iframe served from the tenant itself), not a standalone localhost app.
> **Where we are:** the app is feature-complete for a POV and the native-deploy plumbing is written and locally validated. What's left is platform confirmation + CI + the org move. See the checklist at the bottom.

---

## 1. What this app is

DDLC (Data Development Lifecycle), branded **"Data Studio"** in the Atlan nav, is a FastAPI + Temporal + Dapr app built on the **Atlan Application SDK** (`atlan-application-sdk==2.8.4`). It manages the lifecycle of **ODCS v3.1.0 data contracts**: `REQUEST → DISCOVERY → SPECIFICATION → REVIEW → APPROVAL → ACTIVE`. On activation it publishes an Atlan asset and can push DQ rules to Data Quality Studio.

It's one of three "AI Ready Data" modules: **Data Studio (this app) → Data Quality Studio (existing Atlan) → Context Studio (existing Atlan)**.

## 2. The one architectural fact to internalize

**Every custom Atlan app embeds via an iframe. There is no non-iframe DOM injection for custom apps.** "Native / contained in Atlan" means the app *container* runs inside the tenant, serves its own UI, and the platform embeds that UI at a tenant-relative path (`https://{tenant}/apps/ddlc/`) — same-origin, so it feels native. True first-class UI woven into Atlan's Vue frontend would require an `atlan-frontend` PR by Atlan product-eng, which is not available to us. This is expected and fine; plan around the iframe.

## 3. Repo layout & key files

```
hello_world/                     # git repo — remote `origin` = andrew-lentz-atlan/ddlc, branch `atlan-app`
├── main.py                      # SDK entrypoint: DDLCApplication + DDLCServer, startup seeding, CORS/CSP
├── atlan.yaml                   # ← NATIVE DEPLOY MANIFEST (the platform reads this)
├── Dockerfile                   # ← app-runtime-base:3, builds the app image
├── pyproject.toml / uv.lock     # deps (uv). Build uses `uv sync --locked`
├── app/ddlc/
│   ├── server.py                # ~1300 lines, all /api/* routes (contracts, stages, nuggets, DQS push)
│   ├── models.py                # Pydantic: DDLCSession (aggregate root), ContextNugget, etc.
│   ├── store.py                 # ← durable persistence (ObjectStore + in-memory fallback)
│   ├── odcs.py                  # ODCS v3.1.0 YAML serializer
│   ├── atlan_assets.py          # publishes contract → Atlan asset (pyatlan)
│   ├── nugget_extractor.py / nugget_export.py  # Context Nuggets feature
│   ├── demo_seed.py             # ⚠️ GITIGNORED (PII) — not in the repo; see §6
│   └── frontend/                # vanilla HTML/CSS/JS, served at "/" (iframe handshake already wired)
└── components/                  # Dapr component YAMLs (local dev)
```

**Not part of this app / not in the repo** (gitignored, local only): `atlan-frontend/` (the ~350 MB Atlan product frontend, cloned by `demo-start.sh` for local nav testing), `temporal.db`, `governance_pending.csv`, `app/access/`, `app/dps/` (separate apps).

## 4. What the native migration already did (done + locally validated)

| Area | Change |
|------|--------|
| `atlan.yaml` | **New** v3 manifest. `type: utility`, `deploy.execution_mode: native`, `ingress.ui.path: /apps/ddlc` (+`stripPath`), `splitDeploymentEnabled`, `temporalWorkerDeployment.enabled`, Dapr objectstore/secretstore/statestore, KEDA. |
| `Dockerfile` | Rebased onto `registry.atlan.com/public/app-runtime-base:3` (was `application-sdk:main-2.3.1`). Explicit `ENTRYPOINT python main.py` preserves DDLC's custom startup. |
| `store.py` | In-memory dicts → **ObjectStore-backed** durable CRUD (keys `ddlc/sessions/{id}.json`, `ddlc/nuggets/{id}.json`, pinned to `DEPLOYMENT_OBJECT_STORE_NAME`), with **automatic in-memory fallback** when Dapr is absent so local dev still works. `clear_all` is now `async`. |
| `main.py` | Startup seeding is **idempotent** (seed only when store is empty) and gated by env `DDLC_SEED_DEMO` (see §6). Added `ATLAN_APP_ALLOWED_ORIGINS` env for extra CSP origins. |

Already correct before the migration (no action needed): SDK `register_workflow()` wired, graceful no-Temporal degradation, iframe `postMessage` handshake in all HTML pages, all `/api/*` fetches use relative paths, CORS + `frame-ancestors` CSP middleware.

## 5. How to run it locally

```bash
cd hello_world
uv sync
poe start-deps                 # Dapr (3000/3500) + Temporal (7233; UI 8233)
uv run python main.py          # serves http://localhost:8000
# Temporal UI: http://localhost:8233
```
Without `poe start-deps`, the app still runs: Temporal degrades gracefully and `store.py` falls back to in-memory (you'll see a one-time "Object store unavailable … falling back" log — expected).

For nav testing inside Atlan, `./demo-start.sh` (from `/Desktop/Hackathon`) also starts the Atlan frontend on `:3333`; local nav registration is in `atlan-frontend/src/components/bootstrap/LaunchDarklyBootstrap.vue` (`DDLC_DEV_FLAG.ddlc` → `localhost:8000`).

## 6. Seed data (important nuance)

`demo_seed.py` is **gitignored** (it contains PII — names/emails) and is **not in a fresh clone**. That's intentional:
- **Production** has no seed data. `atlan.yaml` sets `DDLC_SEED_DEMO: 'false'`; the store starts empty and users create real contracts. `main.py` imports `demo_seed` lazily and tolerates its absence.
- **POV/demo tenants**: set `DDLC_SEED_DEMO: 'true'` **and** supply a `demo_seed.py` (`seed_demo_data() -> list[str]`). Ask Andrew for the file, or write a fresh one — it just builds `DDLCSession` objects and calls `store.save_session()`.

## 7. Dev↔Prod: how the app gets into the nav

| | Local dev | Production native |
|---|---|---|
| Registered in | `LaunchDarklyBootstrap.vue` → `DDLC_DEV_FLAG` | `atlan.yaml` → `deploy.ingress.ui.path` |
| iframe target | `http://localhost:8000` | `https://{tenant}/apps/ddlc/` |
| Who wires it | you (edit the Vue file) | the platform (reads the manifest) |

**You do not edit the Vue file for production.** The old `atlan-app-registry.json` in the repo is a superseded registration format — `atlan.yaml` is the source of truth now.

---

## 8. ✅ Finish-the-job checklist (what's left)

Ordered roughly by dependency. Items 1–2 are in our control today; 3–5 need platform/org input; 6 is the deploy.

- [ ] **1. Delete the stale test.** `tests/unit/test_hello_world.py` imports `HelloWorldActivities` (never existed here — it's `DDLCActivities`). It breaks `pytest` collection, which **fails the marketplace CI unit-test gate**. Delete it, or replace with a real DDLC smoke test. `tests/unit/test_blueprint_generator.py` passes (11/11) and can be the seed of a real suite.
- [ ] **2. De-CDN the fonts (optional, recommended).** All frontend HTML pulls Inter from `fonts.googleapis.com`. Strict-CSP / air-gapped tenants block it. Fix: drop the `<link>` and use a system stack `font-family: Inter, -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;` (zero external requests), or self-host the woff2 in `static/`.
- [ ] **3. Confirm two things with the Apps platform team** (#pod-builder-experience — draft message below):
      - correct `atlan.yaml` `type:` — `utility` vs `connector` for a UI+workflow app with no crawling.
      - whether this tenant requires a `contract/app.pkl` alongside `atlan.yaml` (Faizan owns the contract toolkit).
- [ ] **4. Set the real `DEPLOYMENT_OBJECT_STORE_NAME`.** `atlan.yaml` has a `{release-name}-objectstore` placeholder. The Helm chart provisions a **release-prefixed** binding; the SDK default `"objectstore"` won't match, and a mismatch makes `store.py` silently fall back to in-memory (no durability). Set it to the actual binding name for the deploy's release.
- [ ] **5. Validate the scaling config.** The `splitDeploymentEnabled` + KEDA + graceful-shutdown settings in `atlan.yaml` were written to match the wdcustom sample. Run the project-scoped **`app-dynamic-scaling-migration`** skill (in `.claude/skills`) to confirm the manifest + single-entrypoint Dockerfile meet current split-deployment requirements (worker/API pods, graceful shutdown, SDK version).
- [ ] **6. AtlanHQ deployment.** Prereqs: move/mirror this repo into the **`atlanhq` org** (org-owned, not just member-owned — org secrets like `ORG_PAT_GITHUB` only resolve for org-owned repos). Add the CI caller `.github/workflows/build-and-publish.yaml` (`uses: atlanhq/application-sdk/.github/workflows/build-and-publish-app.yaml@main`, `secrets: inherit`) + a credential ConfigMap if credentials are used. Then the platform schedules from `atlan.yaml`. Full playbook: the global `/atlan-app` skill ("Native Marketplace App (V3)").

### Draft message for #pod-builder-experience

> **Native app deploy — two config confirmations before first push**
> Migrating our DDLC app ("Data Studio") to a v3 native marketplace app. Two confirmations before deploying to an AtlanHQ tenant:
> 1. **`type` value** — for an app that's purely UI + REST + a Temporal workflow (no source crawling / metadata extraction), should `atlan.yaml` `type:` be `utility` or `connector`? The wdcustom sample uses `utility` with a "confirm this" note.
> 2. **`contract/app.pkl`** — is a Pkl contract file mandatory alongside `atlan.yaml` for current-gen tenants, or is `atlan.yaml` sufficient? If required, is there a generator in the contract toolkit? cc Faizan.
> Config: `execution_mode: native`, `splitDeploymentEnabled: true`, `temporalWorkerDeployment.enabled: true`, Dapr objectstore/secretstore/statestore, KEDA, `ingress.ui.path: /apps/ddlc`. Happy to share the full manifest.

## 9. Who to ask (App Framework team)

- **Abhishek R** — app framework architecture, native model, GMA setup wizard
- **Onkar Ravgan** — native orchestration, Heracles, migration docs
- **Faizan Shaik** — contract toolkit, `app.pkl`, SDK CI
- **Simone** — Helm chart, marketplace release flow
- **Naveen / Anurag** — platform debugging, cluster access, Heracles log lookups
- `#pod-builder-experience` — native app build issues · `#collab-app-platform` — platform/cluster
