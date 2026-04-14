from typing import List, Dict, Any
from datetime import datetime


class BatchCreationError(Exception):
    def __init__(self, message: str, product_failures: List[Dict[str, Any]]):
        super().__init__(message)
        self.product_failures = product_failures
        self.timestamp = datetime.now()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "error": str(self),
            "total_products": len(self.product_failures),
            "failed_count": len(self.product_failures),
            "failed_products": self.product_failures,
            "timestamp": self.timestamp.isoformat(),
        }
