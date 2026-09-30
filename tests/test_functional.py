"""Functional (VCR cassette) tests for keboola.ex-retain-cloud.

Three things here go beyond the stock `component-defaults` runner, all deliberate:

1. `_LogNormalizingTestDataDir` restores `keboola.vcr`'s DEFAULT_NORMALIZERS. `VCRTestDataDir`
   passes its own one-element `log_normalizers` list, and `compare_logs` *replaces* the defaults
   whenever a list is supplied rather than extending them. Four of the seven cassettes here are
   deliberate-failure cases whose captured logs contain a full traceback, and a traceback carries
   absolute `File "..."` paths that differ between the machine that recorded (`/Users/...`) and
   the container that replays (`/code/...`). Without the default `File "..."` normalizer those
   tests pass locally and fail in CI.

2. The same class appends `_TENANT_PATH_NORMALIZER` — see its comment. Two *different* redaction
   mechanisms spell the tenant differently on the two sides of the log comparison, and only a
   normalizer can reconcile them.

3. `test_full_load_output_contract` re-derives `extractor.py`'s typing and primary-key rules from
   the recorded `structure/richfieldstructure` response plus the committed output rows, and checks
   the manifest against that. Asserting the *rule* rather than a hardcoded column list keeps the
   test honest if the reference table is re-recorded or swapped for one of its fallbacks.
"""

import csv
import json
from pathlib import Path

import pytest
from keboola.datadirtest.vcr import VCRDataDirTester, get_test_cases
from keboola.datadirtest.vcr.tester import VCRTestDataDir
from keboola.vcr.log_capture import DEFAULT_NORMALIZERS, normalize_message

TESTS_DIR = Path(__file__).parent
FUNCTIONAL_DIR = str(TESTS_DIR / "functional")
COMPONENT_SCRIPT = str(TESTS_DIR.parent / "src" / "component.py")

FULL_LOAD_TEST = "05_run_fullLoad_success"

# Collapse whatever stands in the tenant slot of a `/DataAccessAPI/<tenant>/` URL to one fixed
# token, so the recorded and the replayed spelling of that URL compare equal.
#
# The tenant reaches the captured log text at all because `client.py`'s base URL embeds it and
# `requests`' own `HTTPError` message quotes the failing URL in full; that message then lands in
# the traceback `component.py`'s `logger.exception(...)` writes. Two independent redactions then
# disagree about how to spell it:
#
#   * RECORDING side — `keboola.vcr.log_capture.LogSanitizer` does an exact-value substring
#     replacement of every value it finds in `secrets.json` (`#`-prefixed or not) with `***`, so
#     the persisted `logs.json` carries `/DataAccessAPI/***/`. This is log-capture machinery and
#     is entirely separate from `component.py`'s `VCR_SANITIZERS`, which only ever touch HTTP
#     cassette content.
#   * REPLAY side — no secrets file is merged in replay mode
#     (`VCRTestDataDir._run_component_with_vcr` only calls `_merge_secrets_into_config()` when
#     recording), so the component genuinely runs against `config.json`'s placeholder and
#     naturally logs `/DataAccessAPI/tenant/` — which is also what `component.py`'s
#     `UrlPatternSanitizer` wrote into the cassette URIs.
#
# Same real value, two placeholder spellings, one spurious diff. `compare_logs` applies its
# `normalizers` to BOTH sides symmetrically (`log_capture.compare_logs` → `_normalize_messages`
# over `recorded.logs` and `replayed.logs` alike, and over both stderr strings) — it is NOT a
# one-sided rewrite of the expected text. So the pattern is deliberately written to canonicalize
# *any* tenant spelling rather than to map one spelling onto the other: it matches `***`,
# `tenant`, and a real tenant identifier alike, and stays correct if the placeholder in
# `tests/setup/*.json` is ever renamed.
_TENANT_PATH_NORMALIZER = (r"(/DataAccessAPI/)[^/\s]+(/)", r"\g<1><TENANT>\g<2>")


class _LogNormalizingTestDataDir(VCRTestDataDir):
    """`VCRTestDataDir` with DEFAULT_NORMALIZERS put back, plus the tenant-path normalizer."""

    def _setup_vcr(self):
        super()._setup_vcr()
        if self.vcr_recorder is not None:
            own = self.vcr_recorder.log_normalizers or []
            self.vcr_recorder.log_normalizers = [*DEFAULT_NORMALIZERS, *own, _TENANT_PATH_NORMALIZER]


@pytest.mark.parametrize("test_name", get_test_cases(FUNCTIONAL_DIR))
def test_functional(test_name):
    """Run a single VCR functional test case (replays its cassette, compares outputs and logs)."""
    tester = VCRDataDirTester(
        data_dir=FUNCTIONAL_DIR,
        component_script=COMPONENT_SCRIPT,
        test_data_dir_class=_LogNormalizingTestDataDir,
        selected_tests=[test_name],
    )
    tester.run()


def test_tenant_path_normalizer_reconciles_both_redaction_spellings():
    """`***` (LogSanitizer, recording side) and `tenant` (placeholder, replay side) must converge.

    Guards the normalizer itself: without it, `05`'s recorded-vs-replayed log comparison fails on
    nothing but the two spellings of the same tenant. Exercised through `normalize_message` with
    the full normalizer list this module installs, so ordering against DEFAULT_NORMALIZERS counts.
    """
    normalizers = [*DEFAULT_NORMALIZERS, _TENANT_PATH_NORMALIZER]
    url = "https://us.retaincloud.com/DataAccessAPI/{}/api/tableaccess/billingtype/paging/paged"

    recorded = normalize_message(f"400 Client Error: Bad Request for url: {url.format('***')}", normalizers)
    replayed = normalize_message(f"400 Client Error: Bad Request for url: {url.format('tenant')}", normalizers)
    assert recorded == replayed
    # Canonicalises *any* spelling, so a renamed placeholder or an unredacted real value converges too.
    assert normalize_message(f"x {url.format('some-real-tenant-id')} y", normalizers) == (
        f"x {url.format('<TENANT>')} y"
    )
    # Nothing outside the tenant slot is touched.
    assert "tableaccess/billingtype/paging/paged" in recorded


# ---------------------------------------------------------------------------------------------
# Output contract for 05_run_fullLoad_success
# ---------------------------------------------------------------------------------------------

_DATETIME_DECLARED = "DateTime"
_VERIFIED_DECLARED = {"Bool", "Int", "Float"}
_NATIVE_FOR_DECLARED = {"Bool": "BOOLEAN", "Int": "INTEGER", "Float": "FLOAT"}
# Independent re-implementation of extractor.py's `_BOOL_TOKENS`, seen through `_stringify`
# (which renders Python True/False into the CSV as "True"/"False").
_BOOL_CSV_TOKENS = {"true", "false", "True", "False", "1", "0"}


def _read_cassette_richfieldstructure(cassette_path: Path) -> list[dict]:
    """Return the `structure/richfieldstructure` response body from a recorded cassette."""
    cassette = json.loads(cassette_path.read_text())
    for interaction in cassette.get("interactions", []):
        if "richfieldstructure" in interaction["request"]["uri"]:
            return json.loads(interaction["response"]["body"]["string"])
    raise AssertionError(f"No richfieldstructure interaction in {cassette_path}")


def _read_manifest(manifest_path: Path) -> tuple[list[str], dict[str, str], list[str]]:
    """Return (column order, column -> base type, primary key columns) for either manifest format.

    The new format carries `schema: [{name, data_type: {base: {type}}, primary_key}]`; the legacy
    one carries `columns` + `column_metadata` + a flat `primary_key` list.
    """
    manifest = json.loads(manifest_path.read_text())

    if "schema" in manifest:
        columns = [c["name"] for c in manifest["schema"]]
        base_types = {c["name"]: c.get("data_type", {}).get("base", {}).get("type") for c in manifest["schema"]}
        primary_keys = [c["name"] for c in manifest["schema"] if c.get("primary_key")]
        return columns, base_types, primary_keys

    columns = manifest["columns"]
    base_types = {}
    for name, entries in manifest.get("column_metadata", {}).items():
        for entry in entries:
            if entry.get("key") == "KBC.datatype.basetype":
                base_types[name] = entry.get("value")
    return columns, base_types, manifest.get("primary_key", [])


def _coerces_as_csv_text(declared: str, value: str) -> bool:
    """Does this CSV cell honour its declared type? Empty cell == NULL, which always honours it."""
    if value == "":
        return True
    if declared == "Int":
        try:
            int(value)
        except ValueError:
            return False
        return True
    if declared == "Float":
        try:
            float(value)
        except ValueError:
            return False
        return True
    if declared == "Bool":
        return value in _BOOL_CSV_TOKENS
    return True


@pytest.mark.skipif(
    not (Path(FUNCTIONAL_DIR) / FULL_LOAD_TEST / "expected" / "data" / "out" / "tables").is_dir(),
    reason=f"{FULL_LOAD_TEST} has not been recorded yet",
)
def test_full_load_output_contract():
    """The full-load cassette must produce a populated table whose manifest obeys extractor.py."""
    test_dir = Path(FUNCTIONAL_DIR) / FULL_LOAD_TEST
    tables_dir = test_dir / "expected" / "data" / "out" / "tables"
    cassette = test_dir / "source" / "data" / "cassettes" / "requests.json"

    manifests = sorted(tables_dir.glob("*.manifest"))
    assert len(manifests) == 1, f"expected exactly one output table, found {[m.name for m in manifests]}"
    manifest_path = manifests[0]
    data_path = manifest_path.with_suffix("")
    table_name = data_path.stem

    columns, base_types, primary_keys = _read_manifest(manifest_path)

    # Column order and declared types come straight from the recorded richfieldstructure call.
    rich_fields = _read_cassette_richfieldstructure(cassette)
    declared_by_name = {f["name"]: f.get("dataType", "Unknown") for f in rich_fields}
    assert columns == [f["name"] for f in rich_fields], "manifest column order must follow richfieldstructure"

    # The data file is written without a header — the manifest supplies the column names.
    assert data_path.is_file(), f"missing output data file {data_path}"
    with open(data_path, encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f))
    assert rows, "full-load extraction produced an empty output table"
    assert all(len(row) == len(columns) for row in rows), "every row must have one cell per manifest column"

    by_column = {name: [row[i] for row in rows] for i, name in enumerate(columns)}

    # Native types: DateTime is trusted outright, Bool/Int/Float only survive if every real value
    # coerces, everything else ships as STRING.
    for name in columns:
        declared = declared_by_name[name]
        if declared == _DATETIME_DECLARED:
            expected = "TIMESTAMP"
        elif declared in _VERIFIED_DECLARED and all(_coerces_as_csv_text(declared, v) for v in by_column[name]):
            expected = _NATIVE_FOR_DECLARED[declared]
        else:
            expected = "STRING"
        assert base_types[name] == expected, (
            f"column {name!r} declared {declared!r} should map to {expected}, manifest says {base_types[name]}"
        )

    # Primary key: declared only when <table>_guid exists AND verified unique this run.
    pk_candidate = f"{table_name}_guid"
    values = by_column.get(pk_candidate)
    if values is not None and len(set(values)) == len(values):
        assert primary_keys == [pk_candidate], f"unique {pk_candidate} must be declared as the primary key"
    else:
        assert not primary_keys, "no primary key may be declared when <table>_guid is absent or not unique"
