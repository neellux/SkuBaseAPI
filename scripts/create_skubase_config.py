"""
Create lux_skubase config and endpoints in Firestore.
Combines lux_listing + lux_listingoptions into a single app config.

Usage: python3.11 API/scripts/create_skubase_config.py
"""

import firebase_admin
from firebase_admin import credentials, firestore
import os

SERVICE_ACCOUNT_PATH = os.path.join(os.path.dirname(__file__), "..", "service-account-temp.json")

def get_db():
    cred = credentials.Certificate(SERVICE_ACCOUNT_PATH)
    app = firebase_admin.initialize_app(cred, {"projectId": "lux-internal"})
    return firestore.client(database_id="lux-internal"), app


def create_config(db):
    """Create the lux_skubase config document in the configs collection."""
    config = {
        "disabled": False,
        "name": "SkuBase",
        "short_name": "lux_skubase",
        "url": "https://skubase.luxinternal.com",
        "constants": {},
        "icon": '<path d="M17 19.22H5V7h7V5H5c-1.1 0-2 .9-2 2v12c0 1.1.9 2 2 2h12c1.1 0 2-.9 2-2v-7h-2z"></path><path d="M19 2h-2v3h-3c.01.01 0 2 0 2h3v2.99c.01.01 2 0 2 0V7h3V5h-3zM7 9h8v2H7zm0 3v2h8v-2h-3zm0 3h8v2H7z"></path>',
        "logo": '<svg class="MuiSvgIcon-root MuiSvgIcon-fontSizeMedium css-i4bv87-MuiSvgIcon-root" focusable="false" aria-hidden="true" viewBox="0 0 24 24" data-testid="PostAddIcon"><path d="M17 19.22H5V7h7V5H5c-1.1 0-2 .9-2 2v12c0 1.1.9 2 2 2h12c1.1 0 2-.9 2-2v-7h-2z"></path><path d="M19 2h-2v3h-3c.01.01 0 2 0 2h3v2.99c.01.01 2 0 2 0V7h3V5h-3zM7 9h8v2H7zm0 3v2h8v-2h-3zm0 3h8v2H7z"></path></svg>',
        "valid_roles": [
            {"id": "lux_skubase_admin", "name": "Admin"},
            {"id": "lux_skubase_dev", "name": "Dev"},
            {"id": "lux_skubase_manager", "name": "Manager"},
            {"id": "lux_skubase_staff", "name": "Staff"},
            {"id": "lux_skubase_editor", "name": "Editor"},
            {"id": "lux_skubase_viewer", "name": "Viewer"},
            {"id": "lux_skubase_products", "name": "Products"},
        ],
        "valid_permissions": [
            {"id": "global", "name": "Global", "priority": 1, "is_page": False, "parent": ""},
            {"id": "manage_settings", "name": "Manage Settings", "priority": 2, "is_page": False, "parent": ""},
            {"id": "view_batches", "name": "View Batches", "priority": 3, "is_page": False, "parent": ""},
            {"id": "manage_templates", "name": "Manage Templates", "priority": 4, "is_page": False, "parent": ""},
            {"id": "manage_batches", "name": "Manage Batches", "priority": 5, "is_page": False, "parent": ""},
            {"id": "create_products", "name": "Create Products", "priority": 6, "is_page": False, "parent": ""},
            {"id": "manage_platforms", "name": "Manage Platforms", "priority": 7, "is_page": False, "parent": ""},
            {"id": "manage_tables", "name": "Manage Tables", "priority": 8, "is_page": False, "parent": ""},
            {"id": "upsert_records", "name": "Upsert Records", "priority": 9, "is_page": False, "parent": ""},
            {"id": "manage_classes", "name": "Manage Classes", "priority": 10, "is_page": False, "parent": ""},
            {"id": "edit_record_names", "name": "Edit Record Names", "priority": 11, "is_page": False, "parent": ""},
            {"id": "create_sizing_schemes", "name": "Create Sizing Schemes", "priority": 12, "is_page": False, "parent": ""},
            {"id": "edit_sizing_schemes", "name": "Edit Sizing Schemes", "priority": 13, "is_page": False, "parent": ""},
        ],
        "role_permissions": {
            "lux_skubase_admin": [
                "global", "manage_settings", "view_batches", "manage_templates",
                "manage_batches", "create_products", "manage_platforms", "manage_tables",
                "upsert_records", "manage_classes", "edit_record_names",
                "create_sizing_schemes", "edit_sizing_schemes",
            ],
            "lux_skubase_dev": [
                "global", "manage_settings", "view_batches", "manage_templates",
                "manage_batches", "create_products", "manage_platforms", "manage_tables",
                "upsert_records", "manage_classes", "edit_record_names",
                "create_sizing_schemes", "edit_sizing_schemes",
            ],
            "lux_skubase_manager": [
                "global", "manage_settings", "view_batches", "manage_templates",
                "manage_batches", "create_products", "upsert_records", "manage_classes",
                "edit_record_names", "edit_sizing_schemes",
            ],
            "lux_skubase_staff": [
                "global", "view_batches", "upsert_records",
            ],
            "lux_skubase_editor": [
                "global", "upsert_records",
            ],
            "lux_skubase_viewer": [
                "global",
            ],
            "lux_skubase_products": [
                "global",
            ],
        },
    }

    ref = db.collection("configs").document()
    ref.set(config)
    print(f"Created config document: {ref.id}")
    print(f"  name: {config['name']}")
    print(f"  short_name: {config['short_name']}")
    print(f"  roles: {len(config['valid_roles'])}")
    print(f"  permissions: {len(config['valid_permissions'])}")
    return ref.id


def create_endpoints(db):
    """Create all endpoint documents in the endpoints collection."""
    endpoints = [
        # Root endpoints
        {"endpoint": "/", "methods": ["GET"], "name": "Root API Info", "permissions": ["global"], "page": ""},
        {"endpoint": "/app_settings", "methods": ["GET"], "name": "Get App Settings", "permissions": ["global"], "page": ""},
        {"endpoint": "/app_users", "methods": ["GET"], "name": "Get App Users", "permissions": ["global"], "page": ""},

        # Product routes
        {"endpoint": "/products", "methods": ["POST"], "name": "Add Product", "permissions": ["manage_settings"], "page": "products"},
        {"endpoint": "/products/add_size", "methods": ["POST"], "name": "Add Product Size", "permissions": ["manage_settings"], "page": "products"},
        {"endpoint": "/products/reassign_add_size", "methods": ["POST"], "name": "Add size for reassigning products", "permissions": ["create_products"], "page": ""},
        {"endpoint": "/products/update_product_info", "methods": ["PUT"], "name": "Update Product Info", "permissions": ["manage_settings"], "page": "products"},
        {"endpoint": "/products/reassign/preview", "methods": ["GET"], "name": "Preview Reassignment", "permissions": ["manage_settings"], "page": "products"},
        {"endpoint": "/products/reassign", "methods": ["PUT"], "name": "Reassign Child Parent", "permissions": ["manage_settings"], "page": "products"},
        {"endpoint": "/products/product_types", "methods": ["GET"], "name": "Get Product Types", "permissions": ["global"], "page": ""},
        {"endpoint": "/products/colors", "methods": ["GET"], "name": "Get Color options", "permissions": ["global"], "page": ""},
        {"endpoint": "/products/brands", "methods": ["GET"], "name": "Get Brands List", "permissions": ["global"], "page": ""},
        {"endpoint": "/products/search", "methods": ["GET"], "name": "Search Products", "permissions": ["global"], "page": "products"},
        {"endpoint": "/products/details", "methods": ["GET"], "name": "Get Product Details", "permissions": ["global"], "page": "products"},
        {"endpoint": "/products/reassign/bulk/preview", "methods": ["GET"], "name": "Bulk Reassign Preview", "permissions": ["manage_settings"], "page": "products"},
        {"endpoint": "/products/reassign/bulk", "methods": ["POST"], "name": "Create Bulk Reassignment", "permissions": ["manage_settings"], "page": "products"},
        {"endpoint": "/products/reassign/bulk/status", "methods": ["GET"], "name": "Get Bulk Reassignment Status", "permissions": ["manage_settings"], "page": "products"},
        {"endpoint": "/products/reassign/bulk/process", "methods": ["POST"], "name": "Process Bulk Reassignment", "permissions": ["manage_settings"], "page": "products"},
        {"endpoint": "/products/images", "methods": ["GET"], "name": "Get Product Images", "permissions": ["global"], "page": ""},
        {"endpoint": "/products/images/save", "methods": ["POST"], "name": "Save product images", "permissions": ["manage_settings"], "page": ""},

        # Listing routes
        {"endpoint": "/listings/product/confirm", "methods": ["GET"], "name": "Get Product Confirmation Data", "permissions": ["global"], "page": "listings"},
        {"endpoint": "/listings", "methods": ["POST"], "name": "Create Listing", "permissions": ["global"], "page": "listings"},
        {"endpoint": "/listings/images", "methods": ["GET"], "name": "Get Listing Images", "permissions": ["global"], "page": "listings"},
        {"endpoint": "/listings/children", "methods": ["GET"], "name": "Get Listing Children", "permissions": ["global"], "page": "listings"},
        {"endpoint": "/listings/sizing_schemes", "methods": ["GET"], "name": "Get Sizing Schemes", "permissions": ["global"], "page": "listings"},
        {"endpoint": "/listings/platform_size_records", "methods": ["GET"], "name": "Get platform sizes", "permissions": ["global"], "page": ""},
        {"endpoint": "/listings/save_size_mapping", "methods": ["POST"], "name": "Update Size Mappings", "permissions": ["global"], "page": ""},
        {"endpoint": "/listings/product_type_info", "methods": ["GET"], "name": "Get Product Type Info", "permissions": ["global"], "page": ""},
        {"endpoint": "/listings/detail", "methods": ["GET"], "name": "Get Listing Detail", "permissions": ["global"], "page": "listings"},
        {"endpoint": "/listings", "methods": ["PUT"], "name": "Update Listing", "permissions": ["global"], "page": "listings"},
        {"endpoint": "/listings", "methods": ["DELETE"], "name": "Delete Listing", "permissions": ["global"], "page": "listings"},
        {"endpoint": "/listings/submission_status", "methods": ["GET"], "name": "Get Submission Status", "permissions": ["global"], "page": "listings"},
        {"endpoint": "/listings/submit", "methods": ["POST"], "name": "Submit Listing", "permissions": ["global"], "page": "listings"},
        {"endpoint": "/listings/disable_product", "methods": ["POST"], "name": "Disable Product Variant", "permissions": ["global"], "page": ""},
        {"endpoint": "/listings", "methods": ["GET"], "name": "Get Listings", "permissions": ["global"], "page": "listings"},
        {"endpoint": "/listings/schema", "methods": ["GET"], "name": "Get Listing Schema", "permissions": ["global"], "page": "listings"},
        {"endpoint": "/listings/batch/confirm", "methods": ["POST"], "name": "Batch Product Confirmation", "permissions": ["manage_batches"], "page": "batch"},
        {"endpoint": "/listings/batch", "methods": ["POST"], "name": "Create Batch", "permissions": ["manage_batches"], "page": "batch"},
        {"endpoint": "/listings/batch/detail", "methods": ["GET"], "name": "Get Batch Detail", "permissions": ["view_batches"], "page": "batch"},
        {"endpoint": "/listings/batches/filter_options", "methods": ["GET"], "name": "Batch filter options", "permissions": ["global"], "page": ""},
        {"endpoint": "/listings/batches", "methods": ["GET"], "name": "Get Batches", "permissions": ["view_batches"], "page": "batch"},
        {"endpoint": "/listings/batch", "methods": ["PUT"], "name": "Update Batch Info", "permissions": ["manage_batches"], "page": ""},
        {"endpoint": "/listings/batch", "methods": ["DELETE"], "name": "Delete Batch", "permissions": ["manage_batches"], "page": "batch"},

        # Template routes
        {"endpoint": "/templates/list", "methods": ["GET"], "name": "List Templates", "permissions": ["global"], "page": "templates"},
        {"endpoint": "/templates/product_fields", "methods": ["GET"], "name": "Get Product Fields", "permissions": ["manage_templates"], "page": "templates"},
        {"endpoint": "/templates/search_product_fields", "methods": ["GET"], "name": "Search Product Fields", "permissions": ["manage_templates"], "page": "templates"},
        {"endpoint": "/templates/template_fields", "methods": ["GET"], "name": "Get Template Fields", "permissions": ["global"], "page": "templates"},
        {"endpoint": "/templates/listingoptions_meta", "methods": ["GET"], "name": "Get Listing Options Meta", "permissions": ["global", "manage_templates"], "page": "templates"},
        {"endpoint": "/templates/detail", "methods": ["GET"], "name": "Get Template Detail", "permissions": ["global"], "page": "templates"},
        {"endpoint": "/templates/create", "methods": ["POST"], "name": "Create Template", "permissions": ["manage_templates"], "page": "templates"},
        {"endpoint": "/templates/", "methods": ["PUT"], "name": "Update Template", "permissions": ["manage_templates"], "page": "templates"},
        {"endpoint": "/templates/with_fields", "methods": ["PUT"], "name": "Update Template with Fields", "permissions": ["manage_templates"], "page": "templates"},
        {"endpoint": "/templates/add_field", "methods": ["POST"], "name": "Add Field to Template", "permissions": ["manage_templates"], "page": "templates"},
        {"endpoint": "/templates/update_field", "methods": ["PUT"], "name": "Update Template Field", "permissions": ["manage_templates"], "page": "templates"},
        {"endpoint": "/templates/field", "methods": ["DELETE"], "name": "Remove Template Field", "permissions": ["manage_templates"], "page": "templates"},
        {"endpoint": "/templates/reorder_fields", "methods": ["POST"], "name": "Reorder Template Fields", "permissions": ["manage_templates"], "page": "templates"},
        {"endpoint": "/templates/", "methods": ["DELETE"], "name": "Delete Template", "permissions": ["manage_templates"], "page": "templates"},

        # Settings routes
        {"endpoint": "/settings/field_templates", "methods": ["GET"], "name": "List Field Templates", "permissions": ["global"], "page": ""},
        {"endpoint": "/settings/field_templates", "methods": ["PUT"], "name": "Manage Field Templates", "permissions": ["manage_settings"], "page": ""},
        {"endpoint": "/settings/variables", "methods": ["GET"], "name": "Get App Variables", "permissions": ["manage_settings"], "page": ""},
        {"endpoint": "/settings/variables", "methods": ["PUT"], "name": "Update App Variables", "permissions": ["manage_settings"], "page": ""},
        {"endpoint": "/settings/platform_settings", "methods": ["GET"], "name": "Get Platform Settings", "permissions": ["global"], "page": "settings"},
        {"endpoint": "/settings/platform_settings", "methods": ["PUT"], "name": "Update Platform Settings", "permissions": ["manage_settings"], "page": "settings"},
        {"endpoint": "/settings/platform_meta", "methods": ["GET"], "name": "Get Platform Metadata", "permissions": ["global"], "page": "settings"},
        {"endpoint": "/settings/platforms", "methods": ["GET"], "name": "Get Enabled Platforms", "permissions": ["global"], "page": "settings"},
        {"endpoint": "/settings/platforms", "methods": ["PUT"], "name": "Update Enabled Platforms", "permissions": ["manage_settings"], "page": "settings"},

        # ListingOptions - Table routes
        {"endpoint": "/listingoptions/tables/create", "methods": ["POST"], "name": "Create Table", "permissions": ["manage_tables"], "page": ""},
        {"endpoint": "/listingoptions/tables/add_column", "methods": ["POST"], "name": "Add Column to Table", "permissions": ["manage_tables"], "page": ""},
        {"endpoint": "/listingoptions/tables/update_column", "methods": ["PUT"], "name": "Update column schema", "permissions": ["manage_tables"], "page": ""},
        {"endpoint": "/listingoptions/tables/reorder_columns", "methods": ["POST"], "name": "Change column order", "permissions": ["manage_tables"], "page": ""},
        {"endpoint": "/listingoptions/tables/list", "methods": ["GET"], "name": "List Tables", "permissions": ["global"], "page": ""},
        {"endpoint": "/listingoptions/tables/schema", "methods": ["GET"], "name": "Get Table Schema", "permissions": ["global"], "page": ""},
        {"endpoint": "/listingoptions/tables/list_names", "methods": ["GET"], "name": "Get Table Names", "permissions": ["global"], "page": ""},
        {"endpoint": "/listingoptions/tables/records", "methods": ["GET"], "name": "Get Table Records", "permissions": ["global"], "page": ""},
        {"endpoint": "/listingoptions/tables/search_records", "methods": ["GET"], "name": "Search Records", "permissions": ["global"], "page": ""},
        {"endpoint": "/listingoptions/tables/upsert_records", "methods": ["POST"], "name": "Upsert Records", "permissions": ["upsert_records"], "page": ""},
        {"endpoint": "/listingoptions/tables/bulk_import", "methods": ["POST"], "name": "Bulk Import", "permissions": ["upsert_records"], "page": ""},
        {"endpoint": "/listingoptions/tables/export", "methods": ["GET"], "name": "Export Table", "permissions": ["global"], "page": ""},
        {"endpoint": "/listingoptions/tables/delete_column", "methods": ["DELETE"], "name": "Delete Column", "permissions": ["manage_tables"], "page": ""},

        # ListingOptions - List routes
        {"endpoint": "/listingoptions/lists/records", "methods": ["GET"], "name": "Get List Records", "permissions": ["global"], "page": ""},
        {"endpoint": "/listingoptions/lists/default", "methods": ["POST"], "name": "Add Default List Entry", "permissions": ["upsert_records"], "page": ""},
        {"endpoint": "/listingoptions/lists/sizing", "methods": ["POST"], "name": "Add Sizing List Entry", "permissions": ["upsert_records"], "page": ""},
        {"endpoint": "/listingoptions/lists/default", "methods": ["PUT"], "name": "Update Default List Entry", "permissions": ["upsert_records"], "page": ""},
        {"endpoint": "/listingoptions/lists/default/internal_values", "methods": ["PUT"], "name": "Update Internal Values for lists", "permissions": ["upsert_records"], "page": ""},
        {"endpoint": "/listingoptions/lists/sizing", "methods": ["PUT"], "name": "Update Sizing List Entry", "permissions": ["upsert_records"], "page": ""},
        {"endpoint": "/listingoptions/lists/entry", "methods": ["DELETE"], "name": "Delete List Entry", "permissions": ["upsert_records"], "page": ""},
        {"endpoint": "/listingoptions/lists/platforms", "methods": ["GET"], "name": "Get Platforms for Table", "permissions": ["global"], "page": ""},
        {"endpoint": "/listingoptions/lists/entry", "methods": ["GET"], "name": "Get List Entry", "permissions": ["global"], "page": ""},
        {"endpoint": "/listingoptions/lists/bulk_import", "methods": ["POST"], "name": "Bulk Import List Entries", "permissions": ["upsert_records"], "page": ""},
        {"endpoint": "/listingoptions/lists/create_mapping_table", "methods": ["POST"], "name": "Create Mapping Table", "permissions": ["manage_tables"], "page": ""},
        {"endpoint": "/listingoptions/lists/export", "methods": ["GET"], "name": "Export List Entries", "permissions": ["global"], "page": ""},

        # ListingOptions - Platform routes
        {"endpoint": "/listingoptions/platforms/list", "methods": ["GET"], "name": "List Platforms", "permissions": ["global"], "page": ""},
        {"endpoint": "/listingoptions/platforms/get", "methods": ["GET"], "name": "Get Platform", "permissions": ["global"], "page": ""},
        {"endpoint": "/listingoptions/platforms/create", "methods": ["POST"], "name": "Create Platform", "permissions": ["manage_platforms"], "page": ""},
        {"endpoint": "/listingoptions/platforms/update", "methods": ["PUT"], "name": "Update Platform", "permissions": ["manage_platforms"], "page": ""},
        {"endpoint": "/listingoptions/platforms/delete", "methods": ["DELETE"], "name": "Delete Platform", "permissions": ["manage_platforms"], "page": ""},
        {"endpoint": "/listingoptions/platforms/exists", "methods": ["GET"], "name": "Check Platform Exists", "permissions": ["global"], "page": ""},

        # ListingOptions - Sizing Scheme routes
        {"endpoint": "/listingoptions/sizing_schemes/sizes", "methods": ["GET"], "name": "Get all sizes", "permissions": ["global"], "page": ""},
        {"endpoint": "/listingoptions/sizing_schemes", "methods": ["POST"], "name": "Create Sizing Scheme", "permissions": ["edit_sizing_schemes"], "page": ""},
        {"endpoint": "/listingoptions/sizing_schemes", "methods": ["GET"], "name": "List Sizing Schemes", "permissions": ["global"], "page": ""},
        {"endpoint": "/listingoptions/sizing_schemes/detail", "methods": ["GET"], "name": "Get Sizing Scheme Details", "permissions": ["global"], "page": ""},
        {"endpoint": "/listingoptions/sizing_schemes", "methods": ["PUT"], "name": "Update Sizing Scheme", "permissions": ["edit_sizing_schemes"], "page": ""},
        {"endpoint": "/listingoptions/sizing_schemes", "methods": ["DELETE"], "name": "Delete Sizing Scheme", "permissions": ["edit_sizing_schemes"], "page": ""},
        {"endpoint": "/listingoptions/sizing_schemes/sizes", "methods": ["POST"], "name": "Add Size to Sizing Scheme", "permissions": ["edit_sizing_schemes"], "page": ""},
        {"endpoint": "/listingoptions/sizing_schemes/sizes/detail", "methods": ["GET"], "name": "Get Size from Scheme", "permissions": ["global"], "page": ""},
        {"endpoint": "/listingoptions/sizing_schemes/sizes", "methods": ["PUT"], "name": "Update Size in Scheme", "permissions": ["edit_sizing_schemes"], "page": ""},
        {"endpoint": "/listingoptions/sizing_schemes/sizes", "methods": ["DELETE"], "name": "Delete Size from Scheme", "permissions": ["edit_sizing_schemes"], "page": ""},
        {"endpoint": "/listingoptions/sizing_schemes/entries_by_name", "methods": ["GET"], "name": "Get Scheme Entries by Name", "permissions": ["global"], "page": ""},
        {"endpoint": "/listingoptions/sizing_schemes/sizing_type_options", "methods": ["GET"], "name": "Sizing Type Options", "permissions": ["global"], "page": ""},
        {"endpoint": "/listingoptions/sizing_schemes/platform_default_sizes", "methods": ["GET"], "name": "Get Platform default sizes", "permissions": ["global"], "page": ""},
        {"endpoint": "/listingoptions/sizing_schemes/export", "methods": ["GET"], "name": "Export Sizes", "permissions": ["global"], "page": ""},

        # ListingOptions - Sizing Lists routes
        {"endpoint": "/listingoptions/sizing_lists", "methods": ["POST"], "name": "Create Sizing List Entry", "permissions": ["upsert_records"], "page": ""},
        {"endpoint": "/listingoptions/sizing_lists", "methods": ["GET"], "name": "List Sizing List Entries", "permissions": ["global"], "page": ""},
        {"endpoint": "/listingoptions/sizing_lists/detail", "methods": ["GET"], "name": "Get Sizing List Entry", "permissions": ["global"], "page": ""},
        {"endpoint": "/listingoptions/sizing_lists/update", "methods": ["PUT"], "name": "Update Sizing List Entry", "permissions": ["upsert_records"], "page": ""},
        {"endpoint": "/listingoptions/sizing_lists/delete", "methods": ["DELETE"], "name": "Delete Sizing List Entry", "permissions": ["upsert_records"], "page": ""},

        # ListingOptions - Parent Type routes
        {"endpoint": "/listingoptions/parent_types", "methods": ["GET"], "name": "Get Parent Types", "permissions": ["global"], "page": ""},
        {"endpoint": "/listingoptions/parent_types/divisions", "methods": ["GET"], "name": "Get Divisions", "permissions": ["global"], "page": ""},
        {"endpoint": "/listingoptions/parent_types/genders", "methods": ["GET"], "name": "Get Gender List", "permissions": ["global"], "page": ""},
        {"endpoint": "/listingoptions/parent_types/reporting_categories", "methods": ["GET"], "name": "Get Reporting Categories Options", "permissions": ["global"], "page": ""},
        {"endpoint": "/listingoptions/parent_types/departments", "methods": ["GET"], "name": "Get Departments", "permissions": ["global"], "page": ""},
        {"endpoint": "/listingoptions/parent_types/classes", "methods": ["GET"], "name": "Get Classes", "permissions": ["global"], "page": ""},

        # ListingOptions - Misc routes
        {"endpoint": "/listingoptions/records", "methods": ["GET"], "name": "Read Table Records", "permissions": ["global"], "page": ""},
        {"endpoint": "/listingoptions/brand_by_name", "methods": ["GET"], "name": "Get Brand by Name", "permissions": ["global"], "page": ""},
        {"endpoint": "/listingoptions/color_by_name", "methods": ["GET"], "name": "Get Color by Name", "permissions": ["global"], "page": ""},

        # Public API (no auth)
        {"endpoint": "/api/create_batch", "methods": ["POST"], "name": "Create Batch (Public)", "permissions": ["global"], "page": ""},
    ]

    batch = db.batch()
    count = 0

    for ep in endpoints:
        ref = db.collection("endpoints").document()
        doc = {
            "app": "lux_skubase",
            "endpoint": ep["endpoint"],
            "methods": ep["methods"],
            "name": ep["name"],
            "permissions": ep["permissions"],
            "page": ep["page"],
            "id": ref.id,
        }
        batch.set(ref, doc)
        count += 1

        # Firestore batches max 500 writes
        if count % 400 == 0:
            batch.commit()
            batch = db.batch()
            print(f"  Committed {count} endpoints so far...")

    batch.commit()
    print(f"Created {count} endpoint documents")
    return count


def main():
    print("Connecting to Firestore (lux-internal)...")
    db, app = get_db()

    print("\n--- Creating lux_skubase config ---")
    config_id = create_config(db)

    print("\n--- Creating endpoint documents ---")
    endpoint_count = create_endpoints(db)

    print(f"\n=== DONE ===")
    print(f"Config doc ID: {config_id}")
    print(f"Endpoints created: {endpoint_count}")

    firebase_admin.delete_app(app)


if __name__ == "__main__":
    main()
