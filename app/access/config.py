"""Configuration loader — maps data product QNs to ServiceNow catalog items."""
from __future__ import annotations

import json
import logging
from fnmatch import fnmatch
from pathlib import Path

from app.access.models import AccessConfig

log = logging.getLogger(__name__)

_HERE = Path(__file__).parent
_CONFIG_FILE = _HERE / "access_config.json"

_config: AccessConfig | None = None


def load_config() -> AccessConfig:
    """Load configuration from access_config.json."""
    global _config
    if _config is not None:
        return _config
    if _CONFIG_FILE.exists():
        raw = json.loads(_CONFIG_FILE.read_text())
        _config = AccessConfig(**raw)
    else:
        log.warning(f"Config file not found: {_CONFIG_FILE} — using defaults")
        _config = AccessConfig()
    return _config


def resolve_catalog_item(qn: str) -> str:
    """Match a data product QN against config mappings; return the catalog item sys_id."""
    cfg = load_config()
    for mapping in cfg.mappings:
        if fnmatch(qn, mapping.qn_pattern):
            return mapping.catalog_item_sys_id
    return cfg.default_catalog_item_sys_id
