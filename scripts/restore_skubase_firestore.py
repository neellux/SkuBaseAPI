"""
Restore lux_skubase Firestore data from a backup JSON file.
Overwrites config, endpoints, and user roles to match the backup exactly.

Usage: python3.11 API/scripts/restore_skubase_firestore.py <backup_file.json>
"""

import json
import os
import sys

import firebase_admin
from firebase_admin import credentials, firestore

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
API_DIR = os.path.dirname(SCRIPT_DIR)
SERVICE_ACCOUNT_PATH = os.path.join(API_DIR, "service-account-temp.json")


def main():
    if len(sys.argv) != 2:
        print("Usage: python3.11 restore_skubase_firestore.py <backup_file.json>")
        sys.exit(1)

    backup_path = sys.argv[1]
    if not os.path.exists(backup_path):
        print(f"Backup file not found: {backup_path}")
        sys.exit(1)

    with open(backup_path) as f:
        backup = json.load(f)

    print(f"Restoring from: {backup_path}")
    print(f"Backup timestamp: {backup['timestamp']}")
    print(f"  Config: 1 doc")
    print(f"  Endpoints: {len(backup['endpoints'])} docs")
    print(f"  Users: {len(backup['users'])} docs")

    confirm = input("\nThis will OVERWRITE current Firestore data. Type 'yes' to continue: ")
    if confirm != "yes":
        print("Aborted.")
        return

    cred = credentials.Certificate(SERVICE_ACCOUNT_PATH)
    app = firebase_admin.initialize_app(cred, {"projectId": "lux-internal"})
    db = firestore.client(database_id="lux-internal")

    # Restore config
    config = backup["config"]
    db.collection("configs").document(config["doc_id"]).set(config["data"])
    print(f"\n  Restored config {config['doc_id']}")

    # Delete all current lux_skubase endpoints, then restore from backup
    current_eps = list(db.collection("endpoints").where("app", "==", "lux_skubase").stream())
    print(f"  Deleting {len(current_eps)} current endpoints...")
    batch = db.batch()
    count = 0
    for doc in current_eps:
        batch.delete(doc.reference)
        count += 1
        if count % 400 == 0:
            batch.commit()
            batch = db.batch()
    batch.commit()

    # Restore endpoints
    print(f"  Restoring {len(backup['endpoints'])} endpoints...")
    batch = db.batch()
    count = 0
    for ep in backup["endpoints"]:
        ref = db.collection("endpoints").document(ep["doc_id"])
        batch.set(ref, ep["data"])
        count += 1
        if count % 400 == 0:
            batch.commit()
            batch = db.batch()
    batch.commit()

    # Restore users (only roles field to avoid wiping other user data)
    for user in backup["users"]:
        db.collection("users").document(user["doc_id"]).update({"roles": user["data"]["roles"]})
        print(f"  Restored roles for user {user['doc_id']}")

    print("\nRestore complete.")
    firebase_admin.delete_app(app)


if __name__ == "__main__":
    main()
