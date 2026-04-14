"""
Restructure lux_skubase roles and permissions for granular access control.

- Replaces valid_roles, valid_permissions, role_permissions in the config
- Remaps permissions on product-related endpoints to use new granular perms
- Run --dry-run first to preview changes

Usage:
  python3.11 API/scripts/restructure_skubase_roles.py --dry-run
  python3.11 API/scripts/restructure_skubase_roles.py
"""

import os
import sys

import firebase_admin
from firebase_admin import credentials, firestore

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
API_DIR = os.path.dirname(SCRIPT_DIR)
SERVICE_ACCOUNT_PATH = os.path.join(API_DIR, "service-account-temp.json")

NEW_ROLES = [
    {"id": "lux_skubase_catalog_viewer", "name": "Catalog Viewer"},
    {"id": "lux_skubase_listing_viewer", "name": "Listing Viewer"},
    {"id": "lux_skubase_product_editor", "name": "Product Editor"},
    {"id": "lux_skubase_image_manager", "name": "Image Manager"},
    {"id": "lux_skubase_upc_manager", "name": "UPC Manager"},
    {"id": "lux_skubase_listing_manager", "name": "Listing Manager"},
    {"id": "lux_skubase_dev", "name": "Dev"},
    {"id": "lux_skubase_admin", "name": "Admin"},
]

NEW_PERMISSIONS = [
    {"id": "global", "name": "Global", "priority": 1, "is_page": False, "parent": ""},
    {"id": "view_catalog", "name": "View Catalog", "priority": 2, "is_page": False, "parent": ""},
    {"id": "view_batches", "name": "View Batches", "priority": 3, "is_page": False, "parent": ""},
    {"id": "manage_batches", "name": "Manage Batches", "priority": 4, "is_page": False, "parent": ""},
    {"id": "edit_products", "name": "Edit Products", "priority": 5, "is_page": False, "parent": ""},
    {"id": "edit_images", "name": "Edit Images", "priority": 6, "is_page": False, "parent": ""},
    {"id": "edit_upcs", "name": "Edit UPCs", "priority": 7, "is_page": False, "parent": ""},
    {"id": "create_products", "name": "Create Products", "priority": 8, "is_page": False, "parent": ""},
    {"id": "manage_templates", "name": "Manage Templates", "priority": 9, "is_page": False, "parent": ""},
    {"id": "manage_settings", "name": "Manage Settings", "priority": 10, "is_page": False, "parent": ""},
    {"id": "manage_platforms", "name": "Manage Platforms", "priority": 11, "is_page": False, "parent": ""},
    {"id": "manage_tables", "name": "Manage Tables", "priority": 12, "is_page": False, "parent": ""},
    {"id": "upsert_records", "name": "Upsert Records", "priority": 13, "is_page": False, "parent": ""},
    {"id": "manage_classes", "name": "Manage Classes", "priority": 14, "is_page": False, "parent": ""},
    {"id": "edit_record_names", "name": "Edit Record Names", "priority": 15, "is_page": False, "parent": ""},
    {"id": "create_sizing_schemes", "name": "Create Sizing Schemes", "priority": 16, "is_page": False, "parent": ""},
    {"id": "edit_sizing_schemes", "name": "Edit Sizing Schemes", "priority": 17, "is_page": False, "parent": ""},
]

ALL_PERMS = [p["id"] for p in NEW_PERMISSIONS]

ROLE_PERMISSIONS = {
    "lux_skubase_catalog_viewer": ["global", "view_catalog"],
    "lux_skubase_listing_viewer": ["global", "view_catalog", "view_batches"],
    "lux_skubase_product_editor": [
        "global", "view_catalog", "edit_products", "edit_images", "edit_upcs",
        "upsert_records",
    ],
    "lux_skubase_image_manager": ["global", "view_catalog", "edit_images"],
    "lux_skubase_upc_manager": ["global", "view_catalog", "edit_upcs"],
    "lux_skubase_listing_manager": [
        "global", "view_catalog", "view_batches", "manage_batches",
        "edit_products", "edit_images", "edit_upcs",
        "create_products", "manage_templates",
        "upsert_records", "manage_classes", "edit_record_names",
        "create_sizing_schemes", "edit_sizing_schemes",
    ],
    "lux_skubase_dev": ALL_PERMS,
    "lux_skubase_admin": ALL_PERMS,
}

# Endpoint permission remapping: (method, path) -> new permissions list
ENDPOINT_REMAP = {
    # Catalog reads -> view_catalog
    ("GET", "/products/search"): ["view_catalog"],
    ("GET", "/products/details"): ["view_catalog"],
    ("GET", "/products/brands"): ["view_catalog"],
    ("GET", "/products/colors"): ["view_catalog"],
    ("GET", "/products/product_types"): ["view_catalog"],
    ("GET", "/products/images"): ["view_catalog"],
    ("GET", "/products/images/product_types"): ["view_catalog"],
    ("GET", "/products/images/shot_types"): ["view_catalog"],
    ("GET", "/products/images/required_shots"): ["view_catalog"],
    ("GET", "/products/export"): ["view_catalog"],

    # Product detail edits -> edit_products
    ("POST", "/products"): ["edit_products"],
    ("POST", "/products/add_size"): ["edit_products"],
    ("PUT", "/products/update_product_info"): ["edit_products"],
    ("PUT", "/products/reassign"): ["edit_products"],
    ("GET", "/products/reassign/preview"): ["edit_products"],
    ("POST", "/products/reassign/bulk"): ["edit_products"],
    ("GET", "/products/reassign/bulk/preview"): ["edit_products"],
    ("POST", "/products/reassign/bulk/process"): ["edit_products"],
    ("GET", "/products/reassign/bulk/status"): ["edit_products"],
    ("POST", "/products/bulk_import"): ["edit_products"],
    ("POST", "/products/bulk_import/validate"): ["edit_products"],

    # Image edits -> edit_images
    ("POST", "/products/images/save"): ["edit_images"],

    # UPC edits -> edit_upcs
    ("POST", "/products/upc"): ["edit_upcs"],
    ("DELETE", "/products/upc"): ["edit_upcs"],
    ("PUT", "/products/primary_upc"): ["edit_upcs"],
    ("POST", "/products/keyword"): ["edit_upcs"],
    ("DELETE", "/products/keyword"): ["edit_upcs"],
}


def main():
    dry_run = "--dry-run" in sys.argv

    cred = credentials.Certificate(SERVICE_ACCOUNT_PATH)
    app = firebase_admin.initialize_app(cred, {"projectId": "lux-internal"})
    db = firestore.client(database_id="lux-internal")

    print(f"{'DRY RUN' if dry_run else 'LIVE RUN'} — restructuring lux_skubase roles\n")

    # Load current config
    config_doc = None
    for doc in db.collection("configs").where("short_name", "==", "lux_skubase").stream():
        config_doc = doc
        break

    if not config_doc:
        print("ERROR: lux_skubase config not found")
        sys.exit(1)

    current = config_doc.to_dict()
    old_roles = {r["id"] for r in current.get("valid_roles", [])}
    new_roles = {r["id"] for r in NEW_ROLES}

    print("=== CONFIG CHANGES ===")
    print(f"  Removed roles: {sorted(old_roles - new_roles)}")
    print(f"  Added roles:   {sorted(new_roles - old_roles)}")
    print(f"  Kept roles:    {sorted(old_roles & new_roles)}")

    old_perms = {p["id"] for p in current.get("valid_permissions", [])}
    new_perms = {p["id"] for p in NEW_PERMISSIONS}
    print(f"\n  Added permissions: {sorted(new_perms - old_perms)}")
    print(f"  Removed permissions: {sorted(old_perms - new_perms)}")

    # Load all endpoints
    endpoints = list(db.collection("endpoints").where("app", "==", "lux_skubase").stream())

    # Compute endpoint changes
    endpoint_updates = []
    for doc in endpoints:
        d = doc.to_dict()
        for method in d["methods"]:
            key = (method, d["endpoint"])
            if key in ENDPOINT_REMAP:
                new_perms_for_ep = ENDPOINT_REMAP[key]
                if sorted(d.get("permissions", [])) != sorted(new_perms_for_ep):
                    endpoint_updates.append({
                        "doc_id": doc.id,
                        "method": method,
                        "endpoint": d["endpoint"],
                        "old": d.get("permissions", []),
                        "new": new_perms_for_ep,
                    })

    print(f"\n=== ENDPOINT CHANGES ({len(endpoint_updates)}) ===")
    for u in endpoint_updates:
        print(f"  {u['method']:6s} {u['endpoint']:45s} {u['old']} -> {u['new']}")

    if dry_run:
        print("\n(dry run — no changes committed)")
        firebase_admin.delete_app(app)
        return

    print("\n=== COMMITTING ===")

    # Update config
    config_doc.reference.update({
        "valid_roles": NEW_ROLES,
        "valid_permissions": NEW_PERMISSIONS,
        "role_permissions": ROLE_PERMISSIONS,
    })
    print("  Config updated")

    # Update endpoints
    batch = db.batch()
    for u in endpoint_updates:
        ref = db.collection("endpoints").document(u["doc_id"])
        batch.update(ref, {"permissions": u["new"]})
    batch.commit()
    print(f"  {len(endpoint_updates)} endpoints updated")

    print("\nDONE")
    firebase_admin.delete_app(app)


if __name__ == "__main__":
    main()
