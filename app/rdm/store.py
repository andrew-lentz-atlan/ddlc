"""In-memory state store for Reference Data Management.

Same pattern as app/ddlc/store.py — swappable to Dapr later.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional

from app.rdm.models import (
    DatasetStatus,
    ReferenceDataset,
    ReferenceRow,
    RowStatus,
)


class ReferenceDataStore:
    """Thread-safe (asyncio) in-memory store for datasets and rows."""

    def __init__(self) -> None:
        self._datasets: Dict[str, ReferenceDataset] = {}
        # dataset_id → {row_id → ReferenceRow}
        self._rows: Dict[str, Dict[str, ReferenceRow]] = {}

    # ------------------------------------------------------------------
    # Datasets
    # ------------------------------------------------------------------

    async def list_datasets(self) -> List[ReferenceDataset]:
        return sorted(
            self._datasets.values(),
            key=lambda d: (d.domain, d.display_name),
        )

    async def get_dataset(self, dataset_id: str) -> Optional[ReferenceDataset]:
        return self._datasets.get(dataset_id)

    async def get_dataset_by_name(self, name: str) -> Optional[ReferenceDataset]:
        for ds in self._datasets.values():
            if ds.name == name:
                return ds
        return None

    async def save_dataset(self, dataset: ReferenceDataset) -> ReferenceDataset:
        dataset.updated_at = datetime.now(timezone.utc)
        # Sync row_count
        dataset.row_count = len(self._rows.get(dataset.id, {}))
        self._datasets[dataset.id] = dataset
        return dataset

    async def delete_dataset(self, dataset_id: str) -> bool:
        if dataset_id not in self._datasets:
            return False
        del self._datasets[dataset_id]
        self._rows.pop(dataset_id, None)
        return True

    # ------------------------------------------------------------------
    # Rows
    # ------------------------------------------------------------------

    async def list_rows(
        self,
        dataset_id: str,
        include_deprecated: bool = True,
    ) -> List[ReferenceRow]:
        bucket = self._rows.get(dataset_id, {})
        rows = list(bucket.values())
        if not include_deprecated:
            rows = [r for r in rows if r.status == RowStatus.ACTIVE]
        # Sort by primary key value for stable display
        return sorted(rows, key=lambda r: list(r.values.values())[0] if r.values else r.id)

    async def get_row(self, dataset_id: str, row_id: str) -> Optional[ReferenceRow]:
        return self._rows.get(dataset_id, {}).get(row_id)

    async def save_row(self, row: ReferenceRow) -> ReferenceRow:
        row.updated_at = datetime.now(timezone.utc)
        if row.dataset_id not in self._rows:
            self._rows[row.dataset_id] = {}
        self._rows[row.dataset_id][row.id] = row
        # Keep row_count in sync
        if row.dataset_id in self._datasets:
            self._datasets[row.dataset_id].row_count = len(self._rows[row.dataset_id])
        return row

    async def delete_row(self, dataset_id: str, row_id: str) -> bool:
        bucket = self._rows.get(dataset_id, {})
        if row_id not in bucket:
            return False
        del bucket[row_id]
        if dataset_id in self._datasets:
            self._datasets[dataset_id].row_count = len(bucket)
        return True

    async def bulk_upsert_rows(
        self,
        dataset_id: str,
        rows_data: List[Dict],
        replace_all: bool = False,
    ) -> List[ReferenceRow]:
        """Upsert a list of {col_name: value} dicts as rows."""
        if replace_all:
            self._rows[dataset_id] = {}

        if dataset_id not in self._rows:
            self._rows[dataset_id] = {}

        saved: List[ReferenceRow] = []
        for values in rows_data:
            row = ReferenceRow(
                id=str(uuid.uuid4()),
                dataset_id=dataset_id,
                values={k: str(v) for k, v in values.items() if v is not None},
            )
            self._rows[dataset_id][row.id] = row
            saved.append(row)

        if dataset_id in self._datasets:
            self._datasets[dataset_id].row_count = len(self._rows[dataset_id])

        return saved

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    async def domain_groups(self) -> Dict[str, List[ReferenceDataset]]:
        """Return datasets grouped by domain, sorted alphabetically."""
        groups: Dict[str, List[ReferenceDataset]] = {}
        for ds in sorted(self._datasets.values(), key=lambda d: d.display_name):
            groups.setdefault(ds.domain, []).append(ds)
        return dict(sorted(groups.items()))


# Singleton store instance
store = ReferenceDataStore()
