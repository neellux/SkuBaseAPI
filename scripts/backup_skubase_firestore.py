"""
Backup lux_skubase Firestore data (config, endpoints, users with skubase roles)
to a local JSON file before running migrations.

Usage: python3.11 API/scripts/backup_skubase_firestore.py
"""

import json
import os
from datetime import datetime

import firebase_admin
from firebase_admin import credentials, firestore

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
API_DIR = os.path.dirname(SCRIPT_DIR)
SERVICE_ACCOUNT_PATH = os.path.join(API_DIR, "service-account-temp.json")
BACKUP_DIR = os.path.join(API_DIR, "backups")


def main():
    os.makedirs(BACKUP_DIR, exist_ok=True)

    cred = credentials.Certificate(SERVICE_ACCOUNT_PATH)
    app = firebase_admin.initialize_app(cred, {"projectId": "lux-internal"})
    db = firestore.client(database_id="lux-internal")

    print("Backing up lux_skubase Firestore data...")

    # Config
    config_doc = None
    for doc in db.collection("configs").where("short_name", "==", "lux_skubase").stream():
        config_doc = {"doc_id": doc.id, "data": doc.to_dict()}
        break

    if not config_doc:
        raise RuntimeError("lux_skubase config not found")

    print(f"  Config: {config_doc['doc_id']}")

    # Endpoints
    endpoints = []
    for doc in db.collection("endpoints").where("app", "==", "lux_skubase").stream():
        endpoints.append({"doc_id": doc.id, "data": doc.to_dict()})

    print(f"  Endpoints: {len(endpoints)}")

    # Users with any lux_skubase_* role
    users = []
    for doc in db.collection("users").stream():
        d = doc.to_dict()
        if any(r.startswith("lux_skubase") for r in d.get("roles", [])):
            users.append({"doc_id": doc.id, "data": d})

    print(f"  Users: {len(users)}")

    backup = {
        "timestamp": datetime.now().isoformat(),
        "config": config_doc,
        "endpoints": endpoints,
        "users": users,
    }

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = os.path.join(BACKUP_DIR, f"skubase_backup_{timestamp}.json")

    with open(backup_path, "w") as f:
        json.dump(backup, f, indent=2, default=str)

    size = os.path.getsize(backup_path)
    print(f"\nBackup written to: {backup_path}")
    print(f"Size: {size:,} bytes")

    firebase_admin.delete_app(app)


if __name__ == "__main__":
    main()
