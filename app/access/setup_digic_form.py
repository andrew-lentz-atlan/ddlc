#!/usr/bin/env python3
"""
Set up the DIGIC (Digital Information Core) access request form in ServiceNow dev instance.

Recreates the customer's real form with:
- Reference lookups (Requested for, Environment, Data Product, Access Role)
- Cascading/dependent selects
- Policy acknowledgment checkbox
- Business justification textarea

Usage:
    cd hello_world && uv run python -m app.access.setup_digic_form
"""
from __future__ import annotations

import json
import sys
import httpx

INSTANCE = "https://dev290876.service-now.com"
USER = "admin"
PASS = "GkN6D2Wumj%-"

# ── Helpers ──────────────────────────────────────────────────────────────

def api(method: str, path: str, **kwargs) -> dict:
    """Make a ServiceNow REST API call with basic auth."""
    url = f"{INSTANCE}{path}"
    resp = httpx.request(
        method, url,
        auth=(USER, PASS),
        headers={"Accept": "application/json", "Content-Type": "application/json"},
        timeout=30,
        **kwargs,
    )
    if resp.status_code >= 400:
        print(f"  ERROR {resp.status_code}: {resp.text[:500]}")
    resp.raise_for_status()
    return resp.json().get("result", resp.json())


def create_record(table: str, data: dict) -> dict:
    """Create a record in a ServiceNow table."""
    return api("POST", f"/api/now/table/{table}", json=data)


def query_records(table: str, query: str, fields: str = "", limit: int = 100) -> list[dict]:
    """Query records from a ServiceNow table."""
    params = {"sysparm_query": query, "sysparm_limit": limit}
    if fields:
        params["sysparm_fields"] = fields
    return api("GET", f"/api/now/table/{table}", params=params)


def find_or_create(table: str, query: str, data: dict) -> dict:
    """Find existing record or create new one."""
    existing = query_records(table, query, limit=1)
    if existing:
        print(f"  Found existing: {existing[0].get('name', existing[0].get('sys_id'))}")
        return existing[0]
    print(f"  Creating in {table}...")
    return create_record(table, data)


# ── Step 1: Create the Catalog Item ─────────────────────────────────────

def create_catalog_item() -> str:
    """Create the DIGIC access request catalog item."""
    print("\n═══ Step 1: Creating DIGIC Catalog Item ═══")

    # Find the default Service Catalog
    catalogs = query_records("sc_catalog", "title=Service Catalog", "sys_id,title")
    catalog_id = catalogs[0]["sys_id"] if catalogs else ""

    # Find or create a category
    cat_query = "title=Data Access Requests"
    categories = query_records("sc_category", cat_query, "sys_id,title")
    if categories:
        category_id = categories[0]["sys_id"]
    else:
        cat = create_record("sc_category", {
            "title": "Data Access Requests",
            "sc_catalog": catalog_id,
            "active": "true",
        })
        category_id = cat["sys_id"]

    # Create the catalog item
    item_query = "name=Digital Information Core Access Request"
    existing = query_records("sc_cat_item", item_query, "sys_id,name", limit=1)
    if existing:
        item_id = existing[0]["sys_id"]
        print(f"  Catalog item already exists: {item_id}")
        return item_id

    item = create_record("sc_cat_item", {
        "name": "Digital Information Core Access Request",
        "short_description": "Provides access to data within the Digital Information Core",
        "description": (
            "The Digital Information Core (DIGIC) is a cross platform solution where we house "
            "our RAW Source System Data, Data Products, as well Curated Data that is published "
            "for end-user consumption. It provides a trusted source as well easy to consume data "
            "that the organization can leverage with confidence while maximizing the business value "
            "of data. DIGIC data is intended to be used in-place, meaning users connect to the "
            "platform via visualization tools or direct database access depending on the role selected."
        ),
        "sc_catalogs": catalog_id,
        "category": category_id,
        "active": "true",
        "use_sc_layout": "true",
    })
    item_id = item["sys_id"]
    print(f"  Created catalog item: {item_id}")
    return item_id


# ── Step 2: Create Reference Data ───────────────────────────────────────

# Data Products and their available access roles
DATA_PRODUCTS = {
    "Corporate Administration": {
        "description": "Corporate administration data domain",
        "roles": {
            "Value Stream Operation": "Provides data access to business application, business capability, Planisware projects, & EVA (an AI Assistant).",
            "Value Stream Analyst": "Provides analytical access to corporate administration data with read-only permissions.",
            "Value Stream Admin": "Full administrative access to corporate administration data domain.",
        },
    },
    "Finance": {
        "description": "Finance and accounting data domain",
        "roles": {
            "Finance Analyst": "Read-only access to financial reports and dashboards.",
            "Finance Power User": "Access to financial data with ability to create custom reports.",
            "Finance Admin": "Full access to all finance data including sensitive compensation data.",
        },
    },
    "Supply Chain": {
        "description": "Supply chain and logistics data domain",
        "roles": {
            "Supply Chain Viewer": "Read-only access to supply chain metrics and dashboards.",
            "Supply Chain Analyst": "Analytical access to supply chain data with export capabilities.",
            "Supply Chain Admin": "Full administrative access to supply chain data domain.",
        },
    },
    "Quality & Regulatory": {
        "description": "Quality management and regulatory compliance data",
        "roles": {
            "Quality Viewer": "Read-only access to quality metrics and compliance dashboards.",
            "Quality Analyst": "Analytical access to quality and regulatory data.",
            "Quality Admin": "Full administrative access including audit trail data.",
        },
    },
    "Human Resources": {
        "description": "HR and people analytics data domain",
        "roles": {
            "HR Viewer": "Access to anonymized workforce analytics.",
            "HR Analyst": "Access to HR data with PII for authorized reporting.",
            "HR Admin": "Full access to all HR data including sensitive personnel records.",
        },
    },
}

ENVIRONMENTS = [
    ("prod_and_nonprod", "Production & Non-Production"),
    ("production", "Production Only"),
    ("non_production", "Non-Production Only"),
]

ACTIONS = [
    ("add_access", "Add access"),
    ("remove_access", "Remove access"),
    ("modify_access", "Modify access"),
]

ROLE_TYPES = [
    ("data_product_role", "Data Product Role"),
    ("source_system_role", "Source System Role"),
    ("curated_domain_role", "Curated Domain Role"),
]


# ── Step 3: Create Variables ────────────────────────────────────────────

def create_variable(cat_item: str, data: dict) -> dict:
    """Create a catalog item variable."""
    # Check if variable already exists
    existing = query_records(
        "item_option_new",
        f"cat_item={cat_item}^name={data.get('name', '')}",
        "sys_id,name",
        limit=1,
    )
    if existing:
        print(f"    Variable '{data.get('name')}' already exists")
        return existing[0]

    data["cat_item"] = cat_item
    result = create_record("item_option_new", data)
    print(f"    Created variable: {data.get('name')} (type {data.get('type')})")
    return result


def create_choice(variable_sys_id: str, value: str, text: str, order: int) -> dict:
    """Create a question choice for a variable."""
    existing = query_records(
        "question_choice",
        f"question={variable_sys_id}^value={value}",
        "sys_id",
        limit=1,
    )
    if existing:
        return existing[0]

    return create_record("question_choice", {
        "question": variable_sys_id,
        "value": value,
        "text": text,
        "order": str(order),
        "active": "true",
    })


def setup_variables(cat_item_id: str) -> dict[str, str]:
    """Create all form variables and return a map of name → sys_id."""
    print("\n═══ Step 3: Creating Form Variables ═══")
    var_ids: dict[str, str] = {}

    # ── 1. Requested for (Reference to sys_user) ──
    v = create_variable(cat_item_id, {
        "name": "requested_for",
        "question_text": "Requested for",
        "type": "8",  # Reference
        "reference": "sys_user",
        "mandatory": "true",
        "order": "100",
    })
    var_ids["requested_for"] = v["sys_id"]

    # ── 2. What do you want to do? (Select Box) ──
    v = create_variable(cat_item_id, {
        "name": "action_type",
        "question_text": "What do you want to do?",
        "type": "5",  # Select Box
        "mandatory": "true",
        "order": "200",
        "default_value": "add_access",
    })
    var_ids["action_type"] = v["sys_id"]
    for i, (val, text) in enumerate(ACTIONS):
        create_choice(v["sys_id"], val, text, (i + 1) * 100)

    # ── 3. Select Environment (Select Box) ──
    v = create_variable(cat_item_id, {
        "name": "environment",
        "question_text": "Select Environment",
        "type": "5",  # Select Box
        "mandatory": "true",
        "order": "300",
        "default_value": "prod_and_nonprod",
    })
    var_ids["environment"] = v["sys_id"]
    for i, (val, text) in enumerate(ENVIRONMENTS):
        create_choice(v["sys_id"], val, text, (i + 1) * 100)

    # ── 4. Select Role Type (Select Box) ──
    v = create_variable(cat_item_id, {
        "name": "role_type",
        "question_text": "Select Role Type",
        "type": "5",  # Select Box
        "mandatory": "true",
        "order": "400",
        "default_value": "data_product_role",
    })
    var_ids["role_type"] = v["sys_id"]
    for i, (val, text) in enumerate(ROLE_TYPES):
        create_choice(v["sys_id"], val, text, (i + 1) * 100)

    # ── 5. Select Data Product (Select Box — simulating reference lookup) ──
    v = create_variable(cat_item_id, {
        "name": "data_product",
        "question_text": "Select Data Product",
        "type": "5",  # Select Box
        "mandatory": "true",
        "order": "500",
    })
    var_ids["data_product"] = v["sys_id"]
    for i, dp_name in enumerate(DATA_PRODUCTS.keys()):
        create_choice(v["sys_id"], dp_name.lower().replace(" ", "_").replace("&", "and"), dp_name, (i + 1) * 100)

    # ── 6. Select Access / Role (Select Box — choices depend on data product) ──
    # In a real setup these would be cascading. For the POC we add ALL roles as choices.
    v = create_variable(cat_item_id, {
        "name": "access_role",
        "question_text": "Select Access",
        "type": "5",  # Select Box
        "mandatory": "true",
        "order": "600",
    })
    var_ids["access_role"] = v["sys_id"]
    order = 100
    for dp_name, dp_info in DATA_PRODUCTS.items():
        for role_name in dp_info["roles"]:
            create_choice(v["sys_id"], role_name.lower().replace(" ", "_"), role_name, order)
            order += 100

    # ── 7. Access description (Read-only text populated by selection) ──
    v = create_variable(cat_item_id, {
        "name": "access_description",
        "question_text": "Access description",
        "type": "6",  # Single Line Text
        "mandatory": "false",
        "read_only": "true",
        "order": "700",
        "default_value": "Select a data product and access role to see the description.",
    })
    var_ids["access_description"] = v["sys_id"]

    # ── 8. Policy acknowledgment info text ──
    v = create_variable(cat_item_id, {
        "name": "policy_header",
        "question_text": "Please read the Digital Information Core Intended Use Policy and confirm your understanding by checking the box below",
        "type": "24",  # Label (rendered as info/instruction text)
        "mandatory": "false",
        "order": "800",
    })
    var_ids["policy_header"] = v["sys_id"]

    # ── 9. Policy acknowledgment checkbox ──
    v = create_variable(cat_item_id, {
        "name": "policy_acknowledged",
        "question_text": "I acknowledge that I have read and hereby accept the terms and conditions of obtaining access to Digital Information Core.",
        "type": "7",  # Check Box
        "mandatory": "true",
        "order": "900",
    })
    var_ids["policy_acknowledged"] = v["sys_id"]

    # ── 10. Business Justification (Multi-line text) ──
    v = create_variable(cat_item_id, {
        "name": "business_justification",
        "question_text": "For the business justification, please provide enough detail for the approver to understand how this role will be used.",
        "type": "2",  # Multi Line Text
        "mandatory": "true",
        "order": "1000",
    })
    var_ids["business_justification"] = v["sys_id"]

    # ── 11. Additional comments (Multi-line text) ──
    v = create_variable(cat_item_id, {
        "name": "additional_comments",
        "question_text": "Additional comments",
        "type": "2",  # Multi Line Text
        "mandatory": "false",
        "order": "1100",
    })
    var_ids["additional_comments"] = v["sys_id"]

    return var_ids


# ── Step 4: Store role descriptions for dynamic lookup ──────────────────

def store_role_descriptions(cat_item_id: str):
    """
    Store data product → role → description mappings.
    We'll use a custom table or property for the app to look up descriptions.
    For the POC, we embed this in the catalog item's extended description.
    """
    print("\n═══ Step 4: Storing role descriptions ═══")

    # Build a JSON lookup the frontend can use
    lookup = {}
    for dp_name, dp_info in DATA_PRODUCTS.items():
        dp_key = dp_name.lower().replace(" ", "_").replace("&", "and")
        lookup[dp_key] = {
            "name": dp_name,
            "description": dp_info["description"],
            "roles": {},
        }
        for role_name, role_desc in dp_info["roles"].items():
            role_key = role_name.lower().replace(" ", "_")
            lookup[dp_key]["roles"][role_key] = {
                "name": role_name,
                "description": role_desc,
            }

    # Save to a file the backend can serve
    lookup_path = "/Users/andrew.lentz/Desktop/Hackathon/hello_world/app/access/digic_data.json"
    with open(lookup_path, "w") as f:
        json.dump(lookup, f, indent=2)
    print(f"  Saved role descriptions to {lookup_path}")
    return lookup


# ── Step 5: Update access_config.json ───────────────────────────────────

def update_config(cat_item_id: str):
    """Update access_config.json with the new catalog item."""
    print("\n═══ Step 5: Updating access_config.json ═══")

    config_path = "/Users/andrew.lentz/Desktop/Hackathon/hello_world/app/access/access_config.json"
    config = {
        "servicenow": {
            "instance_url": INSTANCE,
            "client_id": "",
            "client_secret": "",
            "username": USER,
            "password": PASS,
        },
        "default_catalog_item_sys_id": cat_item_id,
        "mappings": [
            {
                "qn_pattern": "*",
                "catalog_item_sys_id": cat_item_id,
                "display_name": "Digital Information Core Access Request",
            }
        ],
    }
    with open(config_path, "w") as f:
        json.dump(config, f, indent=4)
    print(f"  Updated {config_path}")


# ── Main ────────────────────────────────────────────────────────────────

def main():
    print("╔══════════════════════════════════════════════════════╗")
    print("║  DIGIC Access Request Form — ServiceNow Setup       ║")
    print("║  Instance: dev290876.service-now.com                 ║")
    print("╚══════════════════════════════════════════════════════╝")

    # Step 1: Create catalog item
    cat_item_id = create_catalog_item()

    # Step 2: (Reference data embedded in choices — no separate tables needed for POC)

    # Step 3: Create variables with choices
    var_ids = setup_variables(cat_item_id)

    # Step 4: Store role description lookup data
    lookup = store_role_descriptions(cat_item_id)

    # Step 5: Update config
    update_config(cat_item_id)

    print("\n╔══════════════════════════════════════════════════════╗")
    print("║  Setup Complete!                                     ║")
    print(f"║  Catalog Item: {cat_item_id}  ║")
    print("║                                                      ║")
    print("║  Variables created:                                   ║")
    for name, sid in var_ids.items():
        print(f"║    {name:30s} {sid}  ║")
    print("║                                                      ║")
    print("║  Next: ./access-start.sh to test                     ║")
    print("╚══════════════════════════════════════════════════════╝")


if __name__ == "__main__":
    main()
