"""Static schema definitions and table metadata for the Microsoft Purview connector.

Schemas are derived from two Purview data-plane surfaces:

* the **Unified Catalog Data Governance** REST API
  (``api-version=2026-03-20-preview``) for ``business_domains``,
  ``data_products`` and ``terms``; and
* the **Data Map (Atlas) + Scanning** REST APIs (``api-version=2023-09-01``,
  host ``https://{account}.purview.azure.com``) for ``classifications``,
  ``classification_rules``, ``asset_classifications`` and ``asset_tags``.

See ``microsoft_purview_api_doc.md`` and the Microsoft Learn reference:
https://learn.microsoft.com/en-us/rest/api/purview/

Design notes:
    * UUID identifiers are ``StringType`` \u2014 never cast UUIDs to binary/long.
    * Unified Catalog ``systemData.*At`` fields are ISO-8601 UTC strings; Data
      Map ``createTime`` / ``updateTime`` are epoch-ms and kept as ``LongType``.
    * Nested objects are kept as ``StructType`` \u2014 never ``MapType``. Free-form
      maps whose keys are not known up front (classification ``options``) are
      JSON-serialized into a ``*_json`` string column.
    * Repeated objects are ``ArrayType(StructType(...))``.
    * Integer-valued fields use ``LongType`` (never ``IntegerType``).
    * Every row carries a non-null ``purview_tenant_id`` column for
      multi-tenant disambiguation; Data Map rows also carry ``purview_account``.
"""

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# =============================================================================
# Reusable nested struct definitions
# =============================================================================

# CatalogModelSystemDataWithExpired \u2014 audit metadata common to every catalog
# entity. ``*At`` fields are ISO-8601 UTC strings; ``*By`` fields are AAD oids.
SYSTEM_DATA_STRUCT = StructType(
    [
        StructField("createdAt", StringType(), True),
        StructField("createdBy", StringType(), True),
        StructField("lastModifiedAt", StringType(), True),
        StructField("lastModifiedBy", StringType(), True),
        StructField("expiredAt", StringType(), True),
        StructField("expiredBy", StringType(), True),
        StructField("provisioningState", StringType(), True),
    ]
)

# CatalogModelContactsValueInner \u2014 one contact assignment.
CONTACT_STRUCT = StructType(
    [
        StructField("id", StringType(), True),
        StructField("description", StringType(), True),
    ]
)

# ContactsMap \u2014 contacts keyed by role. Kept as an explicit struct (not a map)
# so each role is a first-class, typed array.
CONTACTS_STRUCT = StructType(
    [
        StructField("owner", ArrayType(CONTACT_STRUCT), True),
        StructField("expert", ArrayType(CONTACT_STRUCT), True),
        StructField("databaseAdmin", ArrayType(CONTACT_STRUCT), True),
    ]
)

# CatalogModelExternalLink \u2014 used by data product termsOfUse / documentation.
EXTERNAL_LINK_STRUCT = StructType(
    [
        StructField("url", StringType(), True),
        StructField("name", StringType(), True),
        StructField("dataAssetId", StringType(), True),
    ]
)

# CatalogModelManagedAttribute \u2014 governance custom attribute on any entity.
MANAGED_ATTRIBUTE_STRUCT = StructType(
    [
        StructField("name", StringType(), True),
        StructField("value", StringType(), True),
        StructField("isRequired", BooleanType(), True),
    ]
)

# CatalogModelTermResource \u2014 external resource link on a term.
TERM_RESOURCE_STRUCT = StructType(
    [
        StructField("name", StringType(), True),
        StructField("url", StringType(), True),
    ]
)

# CatalogModelDataProductAllOfAdditionalProperties.
ADDITIONAL_PROPERTIES_STRUCT = StructType(
    [
        StructField("assetCount", LongType(), True),
    ]
)

# CatalogModelDomainAllOfThumbnail.
THUMBNAIL_STRUCT = StructType(
    [
        StructField("color", StringType(), True),
    ]
)

# CatalogModelRelatedCollectionParentCollection.
PARENT_COLLECTION_STRUCT = StructType(
    [
        StructField("refName", StringType(), True),
        StructField("type", StringType(), True),
    ]
)

# CatalogModelRelatedCollection.
RELATED_COLLECTION_STRUCT = StructType(
    [
        StructField("name", StringType(), True),
        StructField("friendlyName", StringType(), True),
        StructField("parentCollection", PARENT_COLLECTION_STRUCT, True),
    ]
)

# CatalogModelPlatformDomain \u2014 a physical-platform mapping under a domain.
PLATFORM_DOMAIN_STRUCT = StructType(
    [
        StructField("name", StringType(), True),
        StructField("friendlyName", StringType(), True),
        StructField("relatedCollections", ArrayType(RELATED_COLLECTION_STRUCT), True),
    ]
)


# =============================================================================
# Table schemas \u2014 Unified Catalog
# =============================================================================

BUSINESS_DOMAINS_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("parentId", StringType(), True),
        StructField("status", StringType(), True),
        StructField("type", StringType(), True),
        StructField("isRestricted", BooleanType(), True),
        StructField("thumbnail", THUMBNAIL_STRUCT, True),
        StructField("domains", ArrayType(PLATFORM_DOMAIN_STRUCT), True),
        StructField("managedAttributes", ArrayType(MANAGED_ATTRIBUTE_STRUCT), True),
        StructField("systemData", SYSTEM_DATA_STRUCT, True),
        StructField("purview_tenant_id", StringType(), False),
    ]
)

DATA_PRODUCTS_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("domain", StringType(), True),
        StructField("type", StringType(), True),
        StructField("description", StringType(), True),
        StructField("businessUse", StringType(), True),
        StructField("status", StringType(), True),
        StructField("endorsed", BooleanType(), True),
        StructField("updateFrequency", StringType(), True),
        StructField("sensitivityLabel", StringType(), True),
        StructField("audience", ArrayType(StringType()), True),
        StructField("dataQualityScore", DoubleType(), True),
        StructField("activeSubscriberCount", LongType(), True),
        StructField("contacts", CONTACTS_STRUCT, True),
        StructField("termsOfUse", ArrayType(EXTERNAL_LINK_STRUCT), True),
        StructField("documentation", ArrayType(EXTERNAL_LINK_STRUCT), True),
        StructField("managedAttributes", ArrayType(MANAGED_ATTRIBUTE_STRUCT), True),
        StructField("additionalProperties", ADDITIONAL_PROPERTIES_STRUCT, True),
        StructField("systemData", SYSTEM_DATA_STRUCT, True),
        # Top-level copy of systemData.lastModifiedAt, promoted so it can serve
        # as the cdc sequence_by column (SDP APPLY CHANGES cannot resolve a
        # nested/dotted sequence_by path \u2014 see CURSOR_FIELD note below).
        StructField("last_modified_at", StringType(), True),
        StructField("purview_tenant_id", StringType(), False),
    ]
)

TERMS_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("domain", StringType(), True),
        StructField("status", StringType(), True),
        StructField("parentId", StringType(), True),
        StructField("isLeaf", BooleanType(), True),
        StructField("acronyms", ArrayType(StringType()), True),
        StructField("contacts", CONTACTS_STRUCT, True),
        StructField("resources", ArrayType(TERM_RESOURCE_STRUCT), True),
        StructField("managedAttributes", ArrayType(MANAGED_ATTRIBUTE_STRUCT), True),
        StructField("systemData", SYSTEM_DATA_STRUCT, True),
        # Top-level copy of systemData.lastModifiedAt (see DATA_PRODUCTS_SCHEMA).
        StructField("last_modified_at", StringType(), True),
        StructField("purview_tenant_id", StringType(), False),
    ]
)


# =============================================================================
# Table schemas \u2014 Data Map (Atlas) + Scanning: classifications, tags, rules
# =============================================================================

# AtlasAttributeDef (subset) on a classification type definition.
CLASSIFICATION_ATTRIBUTE_DEF_STRUCT = StructType(
    [
        StructField("name", StringType(), True),
        StructField("typeName", StringType(), True),
        StructField("description", StringType(), True),
        StructField("isOptional", BooleanType(), True),
        StructField("cardinality", StringType(), True),
        StructField("isUnique", BooleanType(), True),
        StructField("isIndexable", BooleanType(), True),
        StructField("defaultValue", StringType(), True),
    ]
)

# AtlasClassificationDef \u2014 GET /datamap/api/atlas/v2/types/typedefs?type=classification
CLASSIFICATIONS_SCHEMA = StructType(
    [
        StructField("guid", StringType(), False),
        StructField("name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("category", StringType(), True),
        StructField("serviceType", StringType(), True),
        StructField("typeVersion", StringType(), True),
        StructField("version", LongType(), True),
        StructField("createdBy", StringType(), True),
        StructField("updatedBy", StringType(), True),
        StructField("createTime", LongType(), True),  # epoch ms
        StructField("updateTime", LongType(), True),  # epoch ms
        StructField("lastModifiedTS", StringType(), True),  # ETag counter, not a time
        StructField("superTypes", ArrayType(StringType()), True),
        StructField("subTypes", ArrayType(StringType()), True),
        StructField("entityTypes", ArrayType(StringType()), True),
        StructField("attributeDefs", ArrayType(CLASSIFICATION_ATTRIBUTE_DEF_STRUCT), True),
        StructField("options_json", StringType(), True),  # free-form map as JSON
        StructField("is_system", BooleanType(), True),  # MICROSOFT.* namespace
        StructField("purview_account", StringType(), True),
        StructField("purview_tenant_id", StringType(), False),
    ]
)

# Regex pattern on a classification rule (dataPatterns / columnPatterns).
RULE_PATTERN_STRUCT = StructType(
    [
        StructField("kind", StringType(), True),  # e.g. "Regex"
        StructField("pattern", StringType(), True),
    ]
)

# ClassificationRule (System | Custom) \u2014 GET /scan/classificationrules.
# ``properties`` is flattened to top-level columns.
CLASSIFICATION_RULES_SCHEMA = StructType(
    [
        StructField("name", StringType(), False),
        StructField("id", StringType(), True),
        StructField("kind", StringType(), True),  # System | Custom
        StructField("description", StringType(), True),
        StructField("version", LongType(), True),
        StructField("classificationName", StringType(), True),  # -> classifications.name
        StructField("ruleStatus", StringType(), True),  # Enabled | Disabled
        StructField("classificationAction", StringType(), True),  # Keep | Delete
        StructField("minimumPercentageMatch", DoubleType(), True),
        StructField("minimumDistinctMatchCount", LongType(), True),
        StructField("dataPatterns", ArrayType(RULE_PATTERN_STRUCT), True),
        StructField("columnPatterns", ArrayType(RULE_PATTERN_STRUCT), True),
        StructField("createdAt", StringType(), True),
        StructField("lastModifiedAt", StringType(), True),
        StructField("purview_account", StringType(), True),
        StructField("purview_tenant_id", StringType(), False),
    ]
)

# Asset identity columns shared by the search-derived tables
# (POST /datamap/api/search/query result value[]).
_SEARCH_ASSET_FIELDS = [
    StructField("entity_guid", StringType(), False),
    StructField("qualified_name", StringType(), True),
    StructField("entity_name", StringType(), True),
    StructField("entity_type", StringType(), True),
    StructField("object_type", StringType(), True),
    StructField("asset_type", ArrayType(StringType()), True),
    StructField("collection_id", StringType(), True),
    StructField("create_time", LongType(), True),  # epoch ms
    StructField("update_time", LongType(), True),  # epoch ms
]

# One row per (asset, applied classification).
ASSET_CLASSIFICATIONS_SCHEMA = StructType(
    _SEARCH_ASSET_FIELDS
    + [
        StructField("classification_name", StringType(), False),
        StructField("purview_account", StringType(), True),
        StructField("purview_tenant_id", StringType(), False),
    ]
)

# One row per (asset, tag). Purview exposes free-form asset tags as ``label``.
ASSET_TAGS_SCHEMA = StructType(
    _SEARCH_ASSET_FIELDS
    + [
        StructField("tag", StringType(), False),
        StructField("purview_account", StringType(), True),
        StructField("purview_tenant_id", StringType(), False),
    ]
)


TABLE_SCHEMAS: dict[str, StructType] = {
    "business_domains": BUSINESS_DOMAINS_SCHEMA,
    "data_products": DATA_PRODUCTS_SCHEMA,
    "terms": TERMS_SCHEMA,
    "classifications": CLASSIFICATIONS_SCHEMA,
    "classification_rules": CLASSIFICATION_RULES_SCHEMA,
    "asset_classifications": ASSET_CLASSIFICATIONS_SCHEMA,
    "asset_tags": ASSET_TAGS_SCHEMA,
}


# =============================================================================
# Table metadata
# =============================================================================

# The source cursor lives at a nested path (``systemData.lastModifiedAt``), but
# SDP's managed CDC (APPLY CHANGES) maps ``cursor_field`` to ``sequence_by`` and
# cannot resolve a nested/dotted sequence_by path (SEQUENCE_BY_COLUMN_NOT_FOUND).
# So the cdc readers promote that value to a top-level ``last_modified_at``
# column (see the cdc schemas) and the cursor_field points at that. The
# connector's own client-side incremental read still reads the nested value
# directly (see ``_record_cursor``); this constant is only the declared
# sequence_by column.
CURSOR_FIELD = "last_modified_at"

TABLE_METADATA: dict[str, dict] = {
    # Governance domains are a low-volume taxonomy that changes infrequently \u2014
    # snapshotted in full each run (mirrors Collibra's ``domains``).
    "business_domains": {
        "primary_keys": ["id"],
        "ingestion_type": "snapshot",
    },
    "data_products": {
        "primary_keys": ["id"],
        "cursor_field": CURSOR_FIELD,
        "ingestion_type": "cdc",
    },
    "terms": {
        "primary_keys": ["id"],
        "cursor_field": CURSOR_FIELD,
        "ingestion_type": "cdc",
    },
    # Data Map / Scanning tables: low-volume definitions or assignment sets
    # with no reliable server-side change feed \u2014 full snapshot each run.
    "classifications": {
        "primary_keys": ["guid"],
        "ingestion_type": "snapshot",
    },
    "classification_rules": {
        "primary_keys": ["name"],
        "ingestion_type": "snapshot",
    },
    "asset_classifications": {
        "primary_keys": ["entity_guid", "classification_name"],
        "ingestion_type": "snapshot",
    },
    "asset_tags": {
        "primary_keys": ["entity_guid", "tag"],
        "ingestion_type": "snapshot",
    },
}


SUPPORTED_TABLES: list[str] = list(TABLE_SCHEMAS.keys())

# Tables served from the Data Map / Scanning host
# (https://{account}.purview.azure.com) rather than the shared Unified Catalog
# endpoint. Reading any of these requires ``purview_account_name`` or
# ``datamap_endpoint`` on the connection.
DATAMAP_TABLES: frozenset[str] = frozenset(
    {
        "classifications",
        "classification_rules",
        "asset_classifications",
        "asset_tags",
    }
)


# =============================================================================
# API + HTTP tuning
# =============================================================================

# Unified Catalog Data Governance data-plane API version (public preview).
API_VERSION = "2026-03-20-preview"

# Shared data-plane endpoint for the Unified Catalog (not per-account).
DEFAULT_ENDPOINT = "https://api.purview-service.microsoft.com"

# Data Map (Atlas) + Scanning data-plane API versions (GA).
DATAMAP_API_VERSION = "2023-09-01"
SCAN_API_VERSION = "2023-09-01"

# Discovery /search/query maximum page size.
SEARCH_PAGE_SIZE = 1000

# ``top`` page size for skip/top-paginated endpoints (dataProducts / terms).
# The API does not document a hard max; 100 is a conservative default that
# keeps well under the per-operation rate windows (100 req / 20 s).
DEFAULT_PAGE_SIZE = 100
MAX_PAGE_SIZE = 1000

RETRIABLE_STATUS_CODES = {429, 500, 502, 503, 504}
MAX_RETRIES = 5
INITIAL_BACKOFF = 1.0  # seconds; doubled after each retry
