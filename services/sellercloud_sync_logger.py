"""
SellerCloud sync operation logger.

Tracks multi-step alias sync operations (swaps, transfers, UI add/delete)
in the sellercloud_sync_operations and sellercloud_alias_sync_history tables.

Follows the same module-level function pattern as sellercloud_sync_queue.py.
All SQL uses parameterized queries. Tracking failures never block the caller.
"""

import json
import logging
from contextlib import asynccontextmanager
from collections.abc import AsyncGenerator
from typing import Any

from tortoise import connections

logger = logging.getLogger(__name__)


def _conn():
    return connections.get("product_db")


# ---------------------------------------------------------------------------
# Core functions
# ---------------------------------------------------------------------------

async def create_operation(
    operation: str,
    target_sku: str,
    value: str,
    source: str = "ui",
    source_sku: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> int:
    """Create a sync operation record. Returns the operation id."""
    result = await _conn().execute_query_dict(
        """
        INSERT INTO sellercloud_sync_operations
            (operation, target_sku, value, source, source_sku, total_steps, metadata)
        VALUES ($1, $2, $3, $4, $5, 0, $6::jsonb)
        RETURNING id
        """,
        [
            operation, target_sku, value, source, source_sku,
            json.dumps(metadata) if metadata else None,
        ],
    )
    return result[0]["id"]


async def complete_operation(
    operation_id: int,
    status: str,
    completed_steps: int,
    total_steps: int,
    error_message: str | None = None,
) -> None:
    """Finalize an operation with its outcome."""
    await _conn().execute_query(
        """
        UPDATE sellercloud_sync_operations
        SET status = $2, completed_steps = $3, total_steps = $4,
            error_message = $5, completed_at = CURRENT_TIMESTAMP
        WHERE id = $1
        """,
        [operation_id, status, completed_steps, total_steps, error_message],
    )


async def log_step(
    operation_id: int | None,
    sku: str,
    value: str,
    sync_type: str,
    status: str,
    source: str = "ui",
    step_seq: int = 1,
    detail: str | None = None,
    error_message: str | None = None,
) -> int:
    """
    Log a single sync step to sellercloud_alias_sync_history.

    Rows are inserted with terminal status (synced/failed/skipped), never 'pending'.
    The poller ignores them because it only claims status='pending'.
    """
    result = await _conn().execute_query_dict(
        """
        INSERT INTO sellercloud_alias_sync_history
            (sku, value, type, status, source, operation_id, step_seq, detail, error_message)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        RETURNING id
        """,
        [
            sku, value, sync_type, status, source,
            operation_id, step_seq, detail, error_message,
        ],
    )
    return result[0]["id"]


# ---------------------------------------------------------------------------
# OperationTracker — yielded by the tracked_operation context manager
# ---------------------------------------------------------------------------

class OperationTracker:
    """Tracks steps within a multi-step sync operation."""

    def __init__(self, operation_id: int, source: str):
        self.operation_id = operation_id
        self.source = source
        self._step_count = 0
        self._completed = 0
        self._failed = 0
        self._skipped = 0
        self._last_error: str | None = None

    async def record_step(
        self,
        sku: str,
        value: str,
        sync_type: str,
        detail: str = "",
    ) -> None:
        """Record a successful sync step."""
        self._step_count += 1
        self._completed += 1
        try:
            await log_step(
                self.operation_id, sku, value, sync_type,
                status="synced", source=self.source,
                step_seq=self._step_count, detail=detail,
            )
        except Exception as e:
            logger.warning(f"Failed to log step {self._step_count} for op {self.operation_id}: {e}")

    async def record_failure(
        self,
        sku: str,
        value: str,
        sync_type: str,
        error: str,
        detail: str = "",
    ) -> None:
        """Record a failed sync step. This is a real failure that affects operation status."""
        self._step_count += 1
        self._failed += 1
        self._last_error = error
        try:
            await log_step(
                self.operation_id, sku, value, sync_type,
                status="failed", source=self.source,
                step_seq=self._step_count, detail=detail,
                error_message=error,
            )
        except Exception as e:
            logger.warning(f"Failed to log failure step {self._step_count} for op {self.operation_id}: {e}")

    async def record_skip(
        self,
        sku: str,
        value: str,
        sync_type: str,
        detail: str = "",
    ) -> None:
        """Record a step that was checked and not needed. Does not affect operation status."""
        self._step_count += 1
        self._skipped += 1
        try:
            await log_step(
                self.operation_id, sku, value, sync_type,
                status="skipped", source=self.source,
                step_seq=self._step_count, detail=detail,
            )
        except Exception as e:
            logger.warning(f"Failed to log skip step {self._step_count} for op {self.operation_id}: {e}")

    @property
    def status(self) -> str:
        if self._failed > 0:
            return "failed"
        return "completed"


# ---------------------------------------------------------------------------
# tracked_operation — async context manager
# ---------------------------------------------------------------------------

@asynccontextmanager
async def tracked_operation(
    operation: str,
    target_sku: str,
    value: str,
    source: str = "ui",
    source_sku: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> AsyncGenerator[OperationTracker, None]:
    """
    Async context manager for multi-step sync operations.

    Creates an operation record on entry, yields an OperationTracker for
    logging individual steps, and finalizes the operation status on exit.
    total_steps is computed from actual steps recorded, not estimated upfront.

    The finalizer NEVER raises — it wraps everything in try/except so the
    caller's original exception propagates cleanly.

    Usage:
        async with tracked_operation("swap_primary", "SKU-B", "012345", ...) as tracker:
            await sc_call_1(...)
            await tracker.record_step("SKU-A", "012345", "delete_alias", "Removed from source")
            await sc_call_2(...)
            await tracker.record_step("SKU-B", "012345", "add_alias", "Added to target")
    """
    op_id = await create_operation(
        operation, target_sku, value, source,
        source_sku=source_sku, metadata=metadata,
    )
    tracker = OperationTracker(op_id, source)

    try:
        yield tracker
    except Exception as exc:
        # Record the exception as a failure if no step already captured it
        if tracker._failed == 0:
            tracker._failed += 1
            tracker._last_error = f"{type(exc).__name__}: {exc}"
        raise  # always re-raise — never suppress the caller's exception
    finally:
        # Finalize operation status. MUST NOT raise.
        try:
            await complete_operation(
                op_id,
                tracker.status,
                tracker._completed,
                tracker._step_count,  # actual total from steps recorded
                tracker._last_error,
            )
        except Exception as finalize_err:
            logger.error(f"Failed to finalize operation {op_id}: {finalize_err}")
