"""Unit tests for the Data Map / Scanning tables (classifications, tags, rules).

Mocked HTTP only -- no network, no simulator.
"""

import pytest

from databricks.labs.community_connector.sources.microsoft_purview.microsoft_purview import (  # noqa: E501
    MicrosoftPurviewLakeflowConnect,
)
from databricks.labs.community_connector.sources.microsoft_purview.microsoft_purview_schemas import (  # noqa: E501
    DATAMAP_TABLES,
    TABLE_METADATA,
    TABLE_SCHEMAS,
)
from databricks.labs.community_connector.sources.microsoft_purview.microsoft_purview_utils import (  # noqa: E501
    continuation_paginate,
    to_long,
)


class _Resp:
    def __init__(self, status=200, json_body=None, headers=None, text=""):
        self.status_code = status
        self._json = {} if json_body is None else json_body
        self.headers = headers or {}
        self.text = text

    def json(self):
        return self._json


class _Session:
    """Pops queued responses on .get/.post and records each call."""

    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = []  # (method, url, params, json)
        self.headers = {}

    def get(self, url, params=None, timeout=None):
        self.calls.append(("GET", url, params, None))
        return self._responses.pop(0)

    def post(self, url, params=None, json=None, timeout=None):
        self.calls.append(("POST", url, params, json))
        return self._responses.pop(0)


def _conn(**over):
    opts = {
        "tenant_id": "tid",
        "access_token": "t",
        "purview_account_name": "contoso-pv",
    }
    opts.update(over)
    return MicrosoftPurviewLakeflowConnect(opts)


# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #


def test_datamap_endpoint_built_from_account_name():
    c = _conn()
    assert c.datamap_endpoint == "https://contoso-pv.purview.azure.com"
    assert c.purview_account == "contoso-pv"


def test_datamap_endpoint_override_wins_and_derives_account():
    c = MicrosoftPurviewLakeflowConnect(
        {
            "tenant_id": "t",
            "access_token": "x",
            "datamap_endpoint": "https://acct2.purview.azure.us/",
        }
    )
    assert c.datamap_endpoint == "https://acct2.purview.azure.us"
    assert c.purview_account == "acct2"


@pytest.mark.parametrize("table", sorted(DATAMAP_TABLES))
def test_datamap_tables_require_account(table):
    c = MicrosoftPurviewLakeflowConnect({"tenant_id": "t", "access_token": "x"})
    with pytest.raises(ValueError, match="purview_account_name"):
        c.read_table(table, {}, {})


def test_new_tables_registered_as_snapshot_with_pk_in_schema():
    for table in DATAMAP_TABLES:
        meta = TABLE_METADATA[table]
        assert meta["ingestion_type"] == "snapshot"
        names = TABLE_SCHEMAS[table].fieldNames()
        for pk in meta["primary_keys"]:
            assert pk in names


# --------------------------------------------------------------------------- #
# classifications
# --------------------------------------------------------------------------- #


def test_classifications_shape_and_system_filter():
    c = _conn()
    c._session = _Session([_Resp(json_body={"classificationDefs": [
        {"guid": "g1", "name": "MICROSOFT.PERSONAL.EMAIL", "updateTime": 1700000000000},
        {"guid": "g2", "name": "CONTOSO.EMPLOYEE_ID", "options": {"x": "y"},
         "version": "3",
         "attributeDefs": [{"name": "a", "typeName": "string", "defaultValue": 5}]},
    ]})])
    rows = list(c.read_table("classifications", {}, {"include_system": "false"})[0])
    assert [r["guid"] for r in rows] == ["g2"]
    r = rows[0]
    assert r["is_system"] is False
    assert r["version"] == 3
    assert r["options_json"] == '{"x": "y"}'
    assert r["attributeDefs"][0]["defaultValue"] == "5"
    assert r["purview_account"] == "contoso-pv"
    assert r["purview_tenant_id"] == "tid"
    method, url, params, _ = c._session.calls[0]
    assert method == "GET"
    assert url == "https://contoso-pv.purview.azure.com/datamap/api/atlas/v2/types/typedefs"
    assert params["type"] == "classification"


def test_classifications_include_system_by_default():
    c = _conn()
    c._session = _Session([_Resp(json_body={"classificationDefs": [
        {"guid": "g1", "name": "MICROSOFT.PERSONAL.EMAIL"},
        {"name": "no-guid-dropped"},
    ]})])
    rows = list(c.read_table("classifications", {}, {})[0])
    assert [r["guid"] for r in rows] == ["g1"]
    assert rows[0]["is_system"] is True


# --------------------------------------------------------------------------- #
# classification_rules
# --------------------------------------------------------------------------- #


def test_classification_rules_flatten_and_kind_filter():
    c = _conn()
    c._session = _Session([_Resp(json_body={"value": [
        {"name": "r1", "kind": "Custom", "properties": {
            "classificationName": "CONTOSO.EMPLOYEE_ID", "ruleStatus": "Enabled",
            "minimumPercentageMatch": 60, "version": 2,
            "dataPatterns": [{"kind": "Regex", "pattern": "^E[0-9]{6}$"}],
            "columnPatterns": [{"kind": "Regex", "pattern": "emp.*id"}]}},
        {"name": "MICROSOFT.PERSONAL.EMAIL", "kind": "System", "properties": {}},
    ]})])
    rows = list(c.read_table("classification_rules", {}, {"rule_kind": "Custom"})[0])
    assert len(rows) == 1
    r = rows[0]
    assert r["classificationName"] == "CONTOSO.EMPLOYEE_ID"
    assert r["minimumPercentageMatch"] == 60.0
    assert r["version"] == 2
    assert r["dataPatterns"] == [{"kind": "Regex", "pattern": "^E[0-9]{6}$"}]
    assert c._session.calls[0][1].endswith("/scan/classificationrules")


def test_classification_rules_follow_next_link():
    c = _conn()
    c._session = _Session([
        _Resp(json_body={"value": [{"name": "a", "kind": "System"}],
                         "nextLink": "https://contoso-pv.purview.azure.com/scan/x?p=2"}),
        _Resp(json_body={"value": [{"name": "b", "kind": "Custom"}]}),
    ])
    rows = list(c.read_table("classification_rules", {}, {})[0])
    assert [r["name"] for r in rows] == ["a", "b"]
    assert rows[0]["dataPatterns"] is None


# --------------------------------------------------------------------------- #
# search-derived tables
# --------------------------------------------------------------------------- #


def test_search_filter_and_or():
    f = MicrosoftPurviewLakeflowConnect._build_search_filter(
        {"asset_type": "Azure Databricks Unity Catalog", "object_type": "Tables,Files"}
    )
    assert f == {"and": [
        {"assetType": "Azure Databricks Unity Catalog"},
        {"or": [{"objectType": "Tables"}, {"objectType": "Files"}]},
    ]}
    assert MicrosoftPurviewLakeflowConnect._build_search_filter({}) is None


def test_asset_classifications_explode_dedupe_and_paginate():
    c = _conn()
    c._session = _Session([
        _Resp(json_body={"value": [
            {"id": "e1", "qualifiedName": "databricks://w/c/s/t1",
             "classification": ["MICROSOFT.PERSONAL.EMAIL", "MICROSOFT.PERSONAL.NAME"],
             "assetType": ["Azure Databricks Unity Catalog"], "updateTime": 1700000000000},
            {"id": "e2", "classification": []},
        ], "continuationToken": "tok"}),
        _Resp(json_body={"value": [
            {"id": "e1", "classification": ["MICROSOFT.PERSONAL.EMAIL"]},  # dup
        ]}),
    ])
    rows = list(c.read_table(
        "asset_classifications", {}, {"asset_type": "Azure Databricks Unity Catalog"}
    )[0])
    keys = sorted((r["entity_guid"], r["classification_name"]) for r in rows)
    assert keys == [("e1", "MICROSOFT.PERSONAL.EMAIL"), ("e1", "MICROSOFT.PERSONAL.NAME")]
    first_body = c._session.calls[0][3]
    assert c._session.calls[0][0] == "POST"
    assert first_body["filter"] == {"assetType": "Azure Databricks Unity Catalog"}
    assert "continuationToken" not in first_body
    assert c._session.calls[1][3]["continuationToken"] == "tok"


def test_asset_tags_from_labels():
    c = _conn()
    c._session = _Session([_Resp(json_body={"value": [
        {"id": "e1", "label": ["finance", "gdpr-relevant"], "assetType": "Azure SQL Database"},
        {"id": "e2"},
    ]})])
    rows = list(c.read_table("asset_tags", {}, {})[0])
    assert sorted(r["tag"] for r in rows) == ["finance", "gdpr-relevant"]
    assert rows[0]["asset_type"] == ["Azure SQL Database"]
    assert c._session.calls[0][3]["keywords"] == "*"


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #


def test_continuation_stops_on_repeated_token():
    s = _Session([
        _Resp(json_body={"value": [{"id": 1}], "continuationToken": "a"}),
        _Resp(json_body={"value": [{"id": 2}], "continuationToken": "a"}),
    ])
    out = list(continuation_paginate(s, "https://x", {}, {}, "t"))
    assert [r["id"] for r in out] == [1, 2]
    assert len(s.calls) == 2


def test_to_long():
    assert to_long("12") == 12
    assert to_long(3.0) == 3
    assert to_long(None) is None
    assert to_long(True) is None
    assert to_long("x") is None
