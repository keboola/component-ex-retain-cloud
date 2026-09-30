"""Static hygiene checks on the COMMITTED cassettes themselves.

`test_vcr_sanitizers.py` proves the sanitizer chain behaves; this module proves the artifacts on
disk were actually produced by that chain. The two are not the same claim, and the gap between
them is exactly how the finding this module guards got shipped: `RequestContentLengthSanitizer`
was added to `VCR_SANITIZERS`, its unit tests went green, and the seven already-recorded cassettes
were left carrying their pre-redaction `Content-Length` — a code-only fix that never reached the
committed bytes.

Why the declared length matters on `IntegrationApi/token` specifically: the body sanitizer
rewrites `useremail`, `userpassword` and `tenant` to a fixed `REDACTED`, so every recording of
that request holds an identical body. A stale `Content-Length` re-introduces the variance the
redaction removed — `declared - actual` is the combined character count of the three real values,
and the deliberately-wrong-credential cases (02/04/07) pin username and password to dummies that
are public in `tests/setup/`, leaving the real tenant's length recoverable by subtraction. The
only recording that leaks nothing is one whose declared length describes the redacted body.

This test is deliberately artifact-level and re-record-sensitive: it must go red whenever a
cassette is committed that the current sanitizer chain would not have produced.
"""

import json
from pathlib import Path

import pytest

TESTS_DIR = Path(__file__).parent
CASSETTE_GLOB = "functional/*/source/data/cassettes/requests.json"

# The one endpoint whose request body carries credentials, and so the one whose declared length is
# a side channel. Matched as a substring of the URI, as the host/tenant portion is itself redacted.
TOKEN_URI_FRAGMENT = "IntegrationApi/token"


def _cassette_paths() -> list[Path]:
    return sorted(TESTS_DIR.glob(CASSETTE_GLOB))


def _case_id(path: Path) -> str:
    """`.../functional/05_run_fullLoad_success/source/...` -> `05_run_fullLoad_success`."""
    return path.parents[3].name


def _body_bytes(body) -> bytes:
    """Byte length of a cassette-stored request body, across the shapes VCR persists.

    A body is either absent, a bare string, or the `{"string": ...}` wrapper the rest of this
    codebase already reads (`test_functional.py::_read_cassette_richfieldstructure`); the payload
    inside is `str` or `bytes` depending on the serializer. `str.encode("utf-8")` is what makes
    this a *byte* count rather than a character count — the same measure `Content-Length` states.
    """
    if body is None:
        return b""
    if isinstance(body, dict):
        body = body.get("string", "")
        if body is None:
            return b""
    if isinstance(body, bytes):
        return body
    return str(body).encode("utf-8")


def _declared_content_length(headers) -> str | None:
    """The `Content-Length` header value, case-insensitively, as a plain string.

    Cassette JSON stores header values either as a bare string or as a one-element list (VCR's
    multi-valued form). A genuinely multi-valued `Content-Length` is malformed, so it is surfaced
    verbatim and left to fail the comparison rather than silently taking the first element.
    """
    for name, value in (headers or {}).items():
        if name.lower() != "content-length":
            continue
        if isinstance(value, list):
            return value[0] if len(value) == 1 else f"<multi-valued: {value!r}>"
        return str(value)
    return None


def test_cassettes_are_present():
    """Guards the glob: a typo'd path would make every check below vacuously pass."""
    paths = _cassette_paths()
    assert paths, f"no cassettes matched {CASSETTE_GLOB!r} under {TESTS_DIR}"


@pytest.mark.parametrize("cassette_path", _cassette_paths(), ids=_case_id)
def test_token_request_content_length_matches_recorded_body(cassette_path: Path):
    """Every recorded `IntegrationApi/token` request must declare the length it actually stores.

    A mismatch means the cassette predates (or bypassed) `RequestContentLengthSanitizer` and still
    announces the size of the unredacted credentials. Re-record the case to fix it; do not hand-edit
    the header, or the cassette stops being a faithful record of the sanitizer chain.
    """
    cassette = json.loads(cassette_path.read_text())
    interactions = cassette.get("interactions", [])

    checked = 0
    failures = []
    for index, interaction in enumerate(interactions):
        request = interaction.get("request", {})
        if TOKEN_URI_FRAGMENT not in request.get("uri", ""):
            continue

        checked += 1
        actual = len(_body_bytes(request.get("body")))
        declared = _declared_content_length(request.get("headers"))

        if declared is None:
            failures.append(f"  interaction[{index}]: body is {actual} bytes but no Content-Length is declared")
        elif not declared.isdigit():
            failures.append(f"  interaction[{index}]: Content-Length is {declared}, recorded body is {actual} bytes")
        elif int(declared) != actual:
            failures.append(
                f"  interaction[{index}]: Content-Length declares {declared} "
                f"but the recorded body is {actual} bytes "
                f"(leaks {int(declared) - actual} characters of real credentials)"
            )

    assert checked, f"{cassette_path}: no {TOKEN_URI_FRAGMENT!r} interaction found — did the cassette change shape?"
    assert not failures, (
        f"{cassette_path}\nstale Content-Length on {len(failures)} of {checked} "
        f"{TOKEN_URI_FRAGMENT!r} request(s) — re-record this case:\n" + "\n".join(failures)
    )


# ---------------------------------------------------------------------------------------------
# Shape handling of the two readers above
#
# Today's cassettes happen to use exactly one shape each (list-valued header, bare-string body),
# so the remaining branches would ship untested — and a reader that silently returns 0 bytes or
# None for an unfamiliar shape turns this whole module into a no-op that reports success. These
# pin the shapes VCR is known to persist.
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("body", "expected"),
    [
        ("abc", 3),
        (b"abc", 3),
        ({"string": "abc"}, 3),
        ({"string": b"abc"}, 3),
        (None, 0),
        ({"string": None}, 0),
        ({}, 0),
        # Multi-byte: Content-Length counts bytes, not characters.
        ("é", 2),
        ({"string": "é"}, 2),
    ],
)
def test_body_bytes_handles_every_persisted_shape(body, expected):
    assert len(_body_bytes(body)) == expected


@pytest.mark.parametrize(
    ("headers", "expected"),
    [
        ({"Content-Length": "96"}, "96"),
        ({"Content-Length": ["96"]}, "96"),
        ({"content-length": ["96"]}, "96"),
        ({"CONTENT-LENGTH": "96"}, "96"),
        ({"Accept": "*/*"}, None),
        ({}, None),
        (None, None),
    ],
)
def test_declared_content_length_handles_every_persisted_shape(headers, expected):
    assert _declared_content_length(headers) == expected


def test_multi_valued_content_length_is_reported_not_silently_accepted():
    """Two lengths is malformed; it must surface as a failure, not resolve to the first one."""
    declared = _declared_content_length({"Content-Length": ["96", "126"]})
    assert declared is not None
    assert not declared.isdigit(), "a multi-valued length must not pass as a plain number"
    assert "126" in declared, "the offending values belong in the failure message"
