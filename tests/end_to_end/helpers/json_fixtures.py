"""Large JSON-looking text fixtures used by source-to-target regressions."""

import base64
import gzip
import hashlib
import json

from functools import lru_cache


TICKET_20155_JSON_METADATA_LENGTH = 50_416
TICKET_20155_JSON_METADATA_SHA256 = (
    '87ae5de0cdd7624271d82d56f705985b094ec5d0bb3d366b21fde7c36d3ebe5b'
)

# Exact ``json_metadata`` value extracted from query-result-ticket-20155.txt.
# It is compressed so this test-only module does not contain 50 KiB of opaque
# dashboard metadata while still preserving every byte of the regression case.
_TICKET_20155_JSON_METADATA_GZIP_BASE64 = """
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
vcWofePwz/39//5IHhSds3d7e3/AHwtnv7wxAAA
"""


@lru_cache(maxsize=1)
def ticket_20155_json_metadata():
    """Return and validate the exact large JSON text from ticket 20155."""
    payload = gzip.decompress(base64.b64decode(
        _TICKET_20155_JSON_METADATA_GZIP_BASE64
    ))
    digest = hashlib.sha256(payload).hexdigest()
    if len(payload) != TICKET_20155_JSON_METADATA_LENGTH:
        raise AssertionError(
            f'Ticket 20155 fixture has {len(payload)} bytes; expected '
            f'{TICKET_20155_JSON_METADATA_LENGTH}'
        )
    if digest != TICKET_20155_JSON_METADATA_SHA256:
        raise AssertionError(
            f'Ticket 20155 fixture has SHA-256 {digest}; expected '
            f'{TICKET_20155_JSON_METADATA_SHA256}'
        )

    decoded = payload.decode('utf-8')
    if not isinstance(json.loads(decoded), dict):
        raise AssertionError('Ticket 20155 fixture must contain a JSON object')
    return decoded


def assert_ticket_20155_json_metadata(rows, route):
    """Require one query result containing the unmodified ticket payload."""
    if len(rows) != 1 or len(rows[0]) != 1:
        raise AssertionError(
            f'{route} returned a {len(rows)}-row result; expected one text value'
        )

    actual = rows[0][0]
    expected = ticket_20155_json_metadata()
    if actual == expected:
        return

    actual_bytes = (
        actual.encode('utf-8') if isinstance(actual, str) else repr(actual).encode('utf-8')
    )
    raise AssertionError(
        f'{route} changed ticket 20155 JSON metadata: expected '
        f'{len(expected.encode("utf-8"))} bytes with SHA-256 '
        f'{TICKET_20155_JSON_METADATA_SHA256}, got {type(actual).__name__} with '
        f'{len(actual_bytes)} bytes and SHA-256 '
        f'{hashlib.sha256(actual_bytes).hexdigest()}'
    )
