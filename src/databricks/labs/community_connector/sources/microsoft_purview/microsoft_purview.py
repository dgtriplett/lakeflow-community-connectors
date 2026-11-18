"""Microsoft Purview connector (read-only).

Ingests governed metadata from Microsoft Purview into Unity Catalog via the
standard ``LakeflowConnect`` interface (single-driver reads), from two
data-plane surfaces:

1. **Unified Catalog Data Governance** REST API (``api-version=2026-03-20-preview``,
   public preview, shared host ``https://api.purview-service.microsoft.com``):
   ``business_domains``, ``data_products``, ``terms``.
2. **Data Map (Atlas) + Scanning** REST APIs (``api-version=2023-09-01``, GA,
   per-account host ``https://{account}.purview.azure.com``):
   ``classifications`` (classification type definitions),
   ``classification_rules`` (system + custom scan rules),
   ``asset_classifications`` (classifications applied to assets) and
   ``asset_tags`` (free-form asset tags / labels). These are only read when the
   connection supplies ``purview_account_name`` or ``datamap_endpoint``.

Why not ``SupportsPartitionedStream``?
    The partitioned-stream pattern exists to parallelise **server-side
    time-range** queries across executors. The Unified Catalog list endpoints
    accept only ``skip`` / ``top`` / ``orderBy`` / ``$skipToken`` -- there is
    no server-side ``lastModifiedAt`` range filter -- so time-window
    partitioning would multiply work for no benefit. The Data Map tables are
    low-volume definition sets or snapshot assignment sets and are read
    sequentially too.

Auth (m2m OAuth, offloaded to Unity Catalog):
    Purview data-plane APIs (Unified Catalog, Data Map and Scanning alike) use
    Azure Entra ID OAuth 2.0 client-credentials with scope
    ``https://purview.azure.net/.default``. The UC COMMUNITY connection runs
    the token exchange and refresh server-side and injects a fresh bearer
    token as ``access_token`` at query time; the connector simply sends
    ``Authorization: Bearer {access_token}``. A ``token`` fallback is accepted
    for ad-hoc / personal-token use.

Incremental model:
    ``data_products`` and ``terms`` are ``cdc`` on the
    ``systemData.lastModifiedAt`` cursor (ISO-8601 UTC string), applied
    CLIENT-SIDE with an init-time upper bound (``self._init_ts``) so a
    Trigger.AvailableNow microbatch terminates. ``business_domains`` and all
    four Data Map tables are ``snapshot``.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Any, Iterator
from urllib.parse import urlparse

import requests
from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.microsoft_purview.microsoft_purview_schemas import (  # pylint: disable=line-too-long
    API_VERSION,
    DATAMAP_API_VERSION,
    DATAMAP_TABLES,
    DEFAULT_ENDPOINT,
    DEFAULT_PAGE_SIZE,
    MAX_PAGE_SIZE,
    SCAN_API_VERSION,
    SEARCH_PAGE_SIZE,
    SUPPORTED_TABLES,
    TABLE_METADATA,
    TABLE_SCHEMAS,
)
from databricks.labs.community_connector.sources.microsoft_purview.microsoft_purview_utils import (  # pylint: disable=line-too-long
    api_get,
    continuation_paginate,
    empty_structs_to_none,
    next_link_paginate,
    normalize_contacts,
    skip_top_paginate,
    to_long,
)

_LOG = logging.getLogger(__name__)

# The connector version, kept in the source (not only pyproject.toml) so it
# survives being copied into a workspace or merged into a single file, where
# package metadata isn't available. Must equal `version` in pyproject.toml; a
# unit test enforces that. Answers "which version is my pipeline running?"
CONNECTOR_VERSION = "1.1.0"

DEFAULT_MAX_RECORDS_PER_BATCH = 5000

# Catalog path prefixes (relative to the data-plane endpoint).
_CATALOG_BASE = "/datagovernance/catalog"

# Search-filter table options -> Discovery filter keys. Each option accepts a
# comma-separated list (OR'd); options are AND'd together.
_SEARCH_FILTER_OPTIONS = {
    "asset_type": "assetType",  # e.g. "Azure Databricks Unity Catalog"
    "object_type": "objectType",  # e.g. "Tables"
    "entity_type": "entityType",  # e.g. "databricks_table"
    "collection_id": "collectionId",
    "classification": "classification",  # e.g. "MICROSOFT.PERSONAL.EMAIL"
}


class MicrosoftPurviewLakeflowConnect(LakeflowConnect):
    """LakeflowConnect implementation for the Purview data-plane APIs."""

    # The connector is constructed many times per run (per table and batch),
    # so the version is logged once per process rather than on every init.
    _version_logged = False

    def __init__(self, options: dict[str, str]) -> None:
        """Initialize the Microsoft Purview connector.

        Expected options:
            - tenant_id: Azure Entra (AAD) tenant ID/GUID. Required.
            - endpoint: Unified Catalog data-plane base URL. Defaults to the
              shared ``https://api.purview-service.microsoft.com``.
            - purview_account_name: Purview account name; builds the Data Map /
              Scanning host ``https://{name}.purview.azure.com``. Required only
              for the Data Map tables.
            - datamap_endpoint: full Data Map base URL override (takes
              precedence over ``purview_account_name``).
            - access_token: OAuth 2.0 bearer token, injected by the UC
              COMMUNITY connection (m2m / client-credentials).
            - token: personal/API token fallback (``Authorization: Bearer``).
            - page_size: ``top`` page size for skip/top-paginated endpoints.
        """
        super().__init__(options)
        if not MicrosoftPurviewLakeflowConnect._version_logged:
            _LOG.info("Microsoft Purview connector version %s", CONNECTOR_VERSION)
            MicrosoftPurviewLakeflowConnect._version_logged = True

        self.tenant_id = options.get("tenant_id") or options.get("account")
        if not self.tenant_id:
            raise ValueError(
                "Microsoft Purview connector requires 'tenant_id' (Azure "
                "Entra tenant ID)"
            )

        endpoint = options.get("endpoint") or DEFAULT_ENDPOINT
        self.endpoint = endpoint.rstrip("/")

        # Data Map (Atlas) + Scanning host for classifications / tags / rules.
        # Optional: only required when one of DATAMAP_TABLES is read, so
        # existing Unified-Catalog-only pipelines are unaffected.
        account_name = options.get("purview_account_name")
        datamap_endpoint = options.get("datamap_endpoint") or (
            f"https://{account_name}.purview.azure.com" if account_name else None
        )
        self.datamap_endpoint = (
            datamap_endpoint.rstrip("/") if datamap_endpoint else None
        )
        host = (
            urlparse(self.datamap_endpoint).hostname if self.datamap_endpoint else None
        )
        self.purview_account = account_name or (host.split(".")[0] if host else None)

        try:
            self._page_size = max(
                1,
                min(
                    MAX_PAGE_SIZE,
                    int(options.get("page_size") or DEFAULT_PAGE_SIZE),
                ),
            )
        except (TypeError, ValueError):
            self._page_size = DEFAULT_PAGE_SIZE

        self._session = requests.Session()
        self._session.headers.update({"Accept": "application/json"})
        self._configure_auth(options)

        # Cap incremental cursors at init time so a single Trigger.AvailableNow
        # run only drains data that existed when the connector started; data
        # modified after this point is picked up by the next trigger with a
        # fresh _init_ts. ISO-8601 UTC string to match the
        # systemData.lastModifiedAt cursor unit.
        self._init_ts = datetime.now(timezone.utc).isoformat()

    def _configure_auth(self, options: dict[str, str]) -> None:
        """Set the Bearer auth header from the injected token.

        ``access_token`` (OAuth m2m, UC-injected) takes precedence; ``token``
        is an equivalent personal-token fallback. The connector never runs the
        OAuth client-credentials exchange itself.
        """
        access_token = options.get("access_token") or options.get("token")
        if not access_token:
            raise ValueError(
                "Microsoft Purview connector requires 'access_token' (OAuth "
                "m2m bearer token, injected by the UC connection) or 'token'"
            )
        self._session.headers["Authorization"] = f"Bearer {access_token}"

    # ------------------------------------------------------------------ #
    # Interface methods
    # ------------------------------------------------------------------ #

    def list_tables(self) -> list[str]:
        return list(SUPPORTED_TABLES)

    def get_table_schema(
        self, table_name: str, table_options: dict[str, str]
    ) -> StructType:
        self._validate_table(table_name)
        return TABLE_SCHEMAS[table_name]

    def read_table_metadata(
        self, table_name: str, table_options: dict[str, str]
    ) -> dict:
        self._validate_table(table_name)
        return dict(TABLE_METADATA[table_name])

    def read_table(
        self,
        table_name: str,
        start_offset: dict,
        table_options: dict[str, str],
    ) -> tuple[Iterator[dict], dict]:
        self._validate_table(table_name)
        if table_name == "business_domains":
            return self._read_business_domains(table_options)
        if table_name == "data_products":
            return self._read_data_products(start_offset, table_options)
        if table_name == "terms":
            return self._read_terms(start_offset, table_options)
        if table_name in DATAMAP_TABLES:
            self._require_datamap(table_name)
        if table_name == "classifications":
            return self._read_classifications(table_options)
        if table_name == "classification_rules":
            return self._read_classification_rules(table_options)
        if table_name == "asset_classifications":
            return self._read_asset_classifications(table_options)
        if table_name == "asset_tags":
            return self._read_asset_tags(table_options)
        raise ValueError(f"Unsupported table: {table_name!r}")

    # ------------------------------------------------------------------ #
    # Unified Catalog table readers
    # ------------------------------------------------------------------ #

    def _read_business_domains(
        self, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read business (governance) domains as a snapshot.

        The businessdomains endpoint paginates via a ``$skipToken`` embedded in
        ``nextLink``; ``next_link_paginate`` follows it transparently.
        """
        params = {"api-version": API_VERSION}
        if table_options.get("write_only"):
            params["writeOnly"] = table_options["write_only"]

        url = f"{self.endpoint}{_CATALOG_BASE}/businessdomains"
        records: list[dict[str, Any]] = []
        for raw in next_link_paginate(self._session, url, params, "business_domains"):
            records.append(self._shape_business_domain(raw))
        return iter(records), {}

    def _read_data_products(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read data products incrementally on ``systemData.lastModifiedAt``.

        Paginated by advancing ``skip`` (``top`` = page size). Optional
        ``domain_id`` scopes to a single governance domain.
        """
        params = {"api-version": API_VERSION, "top": str(self._page_size)}
        if table_options.get("domain_id"):
            params["domainId"] = table_options["domain_id"]
        if table_options.get("order_by"):
            params["orderBy"] = table_options["order_by"]

        url = f"{self.endpoint}{_CATALOG_BASE}/dataProducts"
        record_iter = skip_top_paginate(self._session, url, params, "data_products")
        return self._incremental_from_iter(
            record_iter, start_offset, transform=self._shape_data_product
        )

    def _read_terms(
        self, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Read glossary terms incrementally on ``systemData.lastModifiedAt``.

        Paginated by advancing ``skip`` (``top`` = page size). Optional
        ``domain_id`` / ``parent_id`` / ``keyword`` narrow the extract.
        """
        params = {"api-version": API_VERSION, "top": str(self._page_size)}
        if table_options.get("domain_id"):
            params["domainId"] = table_options["domain_id"]
        if table_options.get("parent_id"):
            params["parentId"] = table_options["parent_id"]
        if table_options.get("keyword"):
            params["keyword"] = table_options["keyword"]
        if table_options.get("order_by"):
            params["orderBy"] = table_options["order_by"]

        url = f"{self.endpoint}{_CATALOG_BASE}/terms"
        record_iter = skip_top_paginate(self._session, url, params, "terms")
        return self._incremental_from_iter(
            record_iter, start_offset, transform=self._shape_term
        )

    # ------------------------------------------------------------------ #
    # Data Map / Scanning readers (classifications, rules, asset tags)
    # ------------------------------------------------------------------ #

    def _require_datamap(self, table_name: str) -> None:
        if not self.datamap_endpoint:
            raise ValueError(
                f"Table {table_name!r} reads the Purview Data Map API and "
                "requires 'purview_account_name' (or 'datamap_endpoint', e.g. "
                "https://<account>.purview.azure.com) on the connection."
            )

    def _stamp(self, rec: dict[str, Any]) -> dict[str, Any]:
        rec["purview_account"] = self.purview_account
        rec["purview_tenant_id"] = self.tenant_id
        return rec

    def _read_classifications(
        self, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Classification type definitions (system MICROSOFT.* + custom).

        Option ``include_system=false`` drops the built-in MICROSOFT.* set.
        """
        url = f"{self.datamap_endpoint}/datamap/api/atlas/v2/types/typedefs"
        params = {"api-version": DATAMAP_API_VERSION, "type": "classification"}
        body = api_get(self._session, url, params, "classifications")
        include_system = (
            str(table_options.get("include_system", "true")).lower() != "false"
        )
        records: list[dict[str, Any]] = []
        for raw in body.get("classificationDefs") or []:
            if not isinstance(raw, dict) or not raw.get("guid"):
                continue
            rec = self._shape_classification(raw)
            if not include_system and rec["is_system"]:
                continue
            records.append(rec)
        return iter(records), {}

    def _shape_classification(self, raw: dict[str, Any]) -> dict[str, Any]:
        rec: dict[str, Any] = {
            k: raw.get(k)
            for k in (
                "guid",
                "name",
                "description",
                "category",
                "serviceType",
                "typeVersion",
                "createdBy",
                "updatedBy",
                "lastModifiedTS",
                "superTypes",
                "subTypes",
                "entityTypes",
            )
        }
        rec["version"] = to_long(raw.get("version"))
        rec["createTime"] = to_long(raw.get("createTime"))
        rec["updateTime"] = to_long(raw.get("updateTime"))
        attribute_defs = [
            {
                "name": a.get("name"),
                "typeName": a.get("typeName"),
                "description": a.get("description"),
                "isOptional": a.get("isOptional"),
                "cardinality": a.get("cardinality"),
                "isUnique": a.get("isUnique"),
                "isIndexable": a.get("isIndexable"),
                "defaultValue": (
                    None if a.get("defaultValue") is None else str(a["defaultValue"])
                ),
            }
            for a in (raw.get("attributeDefs") or [])
            if isinstance(a, dict)
        ]
        rec["attributeDefs"] = attribute_defs or None
        rec["options_json"] = json.dumps(raw["options"]) if raw.get("options") else None
        rec["is_system"] = (raw.get("name") or "").upper().startswith("MICROSOFT.")
        return self._stamp(rec)

    def _read_classification_rules(
        self, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """System + custom classification (scan) rules from the Scanning API.

        Option ``rule_kind`` = ``System`` | ``Custom`` filters client-side.
        """
        url = f"{self.datamap_endpoint}/scan/classificationrules"
        params = {"api-version": SCAN_API_VERSION}
        kind_filter = (table_options.get("rule_kind") or "").strip().lower()
        records: list[dict[str, Any]] = []
        for raw in next_link_paginate(
            self._session, url, params, "classification_rules"
        ):
            if not isinstance(raw, dict) or not raw.get("name"):
                continue
            if kind_filter and (raw.get("kind") or "").lower() != kind_filter:
                continue
            records.append(self._shape_classification_rule(raw))
        return iter(records), {}

    def _shape_classification_rule(self, raw: dict[str, Any]) -> dict[str, Any]:
        props = raw.get("properties") or {}

        def _patterns(value: Any) -> list[dict[str, Any]] | None:
            out = [
                {"kind": p.get("kind"), "pattern": p.get("pattern")}
                for p in (value or [])
                if isinstance(p, dict)
            ]
            return out or None

        pct = props.get("minimumPercentageMatch")
        try:
            pct_value = float(pct) if pct is not None else None
        except (TypeError, ValueError):
            pct_value = None
        rec = {
            "name": raw.get("name"),
            "id": raw.get("id"),
            "kind": raw.get("kind"),
            "description": props.get("description"),
            "version": to_long(props.get("version")),
            "classificationName": props.get("classificationName"),
            "ruleStatus": props.get("ruleStatus"),
            "classificationAction": props.get("classificationAction"),
            "minimumPercentageMatch": pct_value,
            "minimumDistinctMatchCount": to_long(props.get("minimumDistinctMatchCount")),
            "dataPatterns": _patterns(props.get("dataPatterns")),
            "columnPatterns": _patterns(props.get("columnPatterns")),
            "createdAt": props.get("createdAt"),
            "lastModifiedAt": props.get("lastModifiedAt"),
        }
        return self._stamp(rec)

    @staticmethod
    def _build_search_filter(table_options: dict[str, str]) -> dict | None:
        """Translate table options into a Discovery ``filter`` expression."""
        clauses: list[dict[str, Any]] = []
        for opt, key in _SEARCH_FILTER_OPTIONS.items():
            raw = table_options.get(opt)
            if not raw:
                continue
            values = [v.strip() for v in str(raw).split(",") if v.strip()]
            if len(values) == 1:
                clauses.append({key: values[0]})
            elif values:
                clauses.append({"or": [{key: v} for v in values]})
        if not clauses:
            return None
        return clauses[0] if len(clauses) == 1 else {"and": clauses}

    def _search_assets(
        self, table_options: dict[str, str], label: str
    ) -> Iterator[dict[str, Any]]:
        url = f"{self.datamap_endpoint}/datamap/api/search/query"
        params = {"api-version": DATAMAP_API_VERSION}
        body: dict[str, Any] = {
            "keywords": table_options.get("keywords") or "*",
            "limit": SEARCH_PAGE_SIZE,
        }
        search_filter = self._build_search_filter(table_options)
        if search_filter:
            body["filter"] = search_filter
        return continuation_paginate(self._session, url, params, body, label)

    @staticmethod
    def _shape_search_asset(raw: dict[str, Any]) -> dict[str, Any]:
        asset_type = raw.get("assetType")
        if isinstance(asset_type, str):
            asset_type = [asset_type]
        return {
            "entity_guid": raw.get("id"),
            "qualified_name": raw.get("qualifiedName"),
            "entity_name": raw.get("name"),
            "entity_type": raw.get("entityType"),
            "object_type": raw.get("objectType"),
            "asset_type": asset_type or None,
            "collection_id": raw.get("collectionId"),
            "create_time": to_long(raw.get("createTime")),
            "update_time": to_long(raw.get("updateTime")),
        }

    def _explode_search(
        self,
        table_options: dict[str, str],
        label: str,
        source_key: str,
        out_col: str,
    ) -> tuple[Iterator[dict], dict]:
        """One row per (asset, value), deduped on the composite primary key."""
        rows: dict[tuple[str, str], dict[str, Any]] = {}
        for raw in self._search_assets(table_options, label):
            if not isinstance(raw, dict) or not raw.get("id"):
                continue
            values = raw.get(source_key) or []
            if isinstance(values, str):
                values = [values]
            if not values:
                continue
            base = self._shape_search_asset(raw)
            for value in values:
                if not isinstance(value, str) or not value:
                    continue
                rows[(base["entity_guid"], value)] = self._stamp(
                    {**base, out_col: value}
                )
        return iter(rows.values()), {}

    def _read_asset_classifications(
        self, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        return self._explode_search(
            table_options,
            "asset_classifications",
            "classification",
            "classification_name",
        )

    def _read_asset_tags(
        self, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        return self._explode_search(table_options, "asset_tags", "label", "tag")

    # ------------------------------------------------------------------ #
    # Incremental engine
    # ------------------------------------------------------------------ #

    def _incremental_from_iter(
        self,
        record_iter: Iterator[dict[str, Any]],
        start_offset: dict,
        transform,
    ) -> tuple[Iterator[dict], dict]:
        """Apply the client-side incremental cursor over a record iterator.

        Applies, per record:
          * strict ``> since`` filtering (skip records at or below the
            watermark) so a resumed run never re-emits the boundary record --
            required by the ``SimpleDataSourceStreamReader`` contract that a
            non-empty batch must advance the end offset past the start.
            ``systemData.lastModifiedAt`` has sub-second (100-ns) precision, so
            distinct records effectively never share a cursor value; and
          * an init-time upper cap (skip records modified after
            ``self._init_ts``) so Trigger.AvailableNow terminates.

        Records are treated as **not** cursor-sorted (``orderBy`` on the nested
        cursor path is unverified live), so the full collection is drained per
        run rather than truncated by count. CDC upsert on the primary key
        tolerates the resulting re-reads.

        Returns ``(records, end_offset)``. When nothing new is emitted, the
        offset is returned unchanged so the trigger converges.
        """
        start_offset = start_offset or {}
        since = start_offset.get("cursor")

        # Already caught up to init time -- nothing new can be emitted.
        if since is not None and since >= self._init_ts:
            return iter([]), start_offset

        records: list[dict[str, Any]] = []
        max_seen = since
        for raw in record_iter:
            cursor = self._record_cursor(raw)
            # Strict `>` boundary (see docstring).
            if since is not None and cursor is not None and cursor <= since:
                continue
            # Init-time cap: skip records modified after the connector started.
            if cursor is not None and cursor > self._init_ts:
                continue

            records.append(transform(raw))
            if cursor is not None and (max_seen is None or cursor > max_seen):
                max_seen = cursor

        if not records or max_seen is None or max_seen == since:
            # No forward progress -- return the offset unchanged so the
            # framework sees end_offset == start_offset and terminates.
            return iter(records), start_offset

        return iter(records), {"cursor": max_seen}

    @staticmethod
    def _record_cursor(raw: dict[str, Any]) -> str | None:
        """Extract the ``systemData.lastModifiedAt`` cursor value from a record.

        Returns the ISO-8601 string, or ``None`` when absent so it is treated
        as uncomparable and never breaks ordering.
        """
        system_data = raw.get("systemData")
        if isinstance(system_data, dict):
            value = system_data.get("lastModifiedAt")
            if isinstance(value, str) and value:
                return value
        return None

    # ------------------------------------------------------------------ #
    # Record shaping (Unified Catalog)
    # ------------------------------------------------------------------ #

    def _shape_business_domain(self, raw: dict[str, Any]) -> dict[str, Any]:
        rec = dict(raw)
        rec["purview_tenant_id"] = self.tenant_id
        return empty_structs_to_none(rec, TABLE_SCHEMAS["business_domains"])

    def _shape_data_product(self, raw: dict[str, Any]) -> dict[str, Any]:
        rec = dict(raw)
        rec["contacts"] = normalize_contacts(raw.get("contacts"))
        # Promote the nested cursor to a top-level column for cdc sequence_by.
        rec["last_modified_at"] = self._record_cursor(raw)
        rec["purview_tenant_id"] = self.tenant_id
        return empty_structs_to_none(rec, TABLE_SCHEMAS["data_products"])

    def _shape_term(self, raw: dict[str, Any]) -> dict[str, Any]:
        rec = dict(raw)
        rec["contacts"] = normalize_contacts(raw.get("contacts"))
        # Promote the nested cursor to a top-level column for cdc sequence_by.
        rec["last_modified_at"] = self._record_cursor(raw)
        rec["purview_tenant_id"] = self.tenant_id
        return empty_structs_to_none(rec, TABLE_SCHEMAS["terms"])

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #

    def _validate_table(self, table_name: str) -> None:
        if table_name not in SUPPORTED_TABLES:
            raise ValueError(
                f"Unsupported table {table_name!r}; "
                f"supported: {SUPPORTED_TABLES}"
            )
