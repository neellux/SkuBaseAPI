from datetime import datetime
from typing import Any, Dict, List


class SellerCloudSubmitError(Exception):
    """A SellerCloud submission failed, with the stage and per-SKU outcomes.

    submit_listing_to_sellercloud fans out one PUT per child SKU. A single child
    failing fails the whole submission, but the caller still needs to know which
    child broke and how far the submission got, so the dashboard can say more
    than "Failed to submit".

    `stage` names the phase that failed (map_fields, gender, weight, description,
    resolve_parent, fetch_children, update_children). `failures` is
    [{"sku", "error"}, ...] and `succeeded` lists the child SKUs that were
    written before the failure, which matters because those writes are not rolled
    back on the SellerCloud side.
    """

    def __init__(
        self,
        message: str,
        stage: str,
        failures: List[Dict[str, str]] | None = None,
        succeeded: List[str] | None = None,
    ):
        super().__init__(message)
        self.stage = stage
        self.failures = failures or []
        self.succeeded = succeeded or []
        self.timestamp = datetime.now()

    @property
    def sku_errors(self) -> Dict[str, str]:
        """{sku: error}, the shape SPO and Grailed already store in platform_meta."""
        return {f["sku"]: f.get("error", "Unknown error") for f in self.failures if f.get("sku")}

    def display(self, limit: int = 500) -> str:
        """Short user-facing summary; error_display is shown verbatim in the UI."""
        if not self.failures:
            return f"Failed at {self.stage}"
        parts = [f"{f['sku']} ({f.get('error', 'Unknown error')})" for f in self.failures]
        return f"Failed SKUs: {', '.join(parts)}"[:limit]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "error": str(self),
            "stage": self.stage,
            "failed_count": len(self.failures),
            "succeeded_count": len(self.succeeded),
            "failures": self.failures,
            "succeeded": self.succeeded,
            "timestamp": self.timestamp.isoformat(),
        }
