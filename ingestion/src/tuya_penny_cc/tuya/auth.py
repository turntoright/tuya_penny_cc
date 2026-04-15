"""Tuya OpenAPI v2.0 request signing.

The signature is HMAC-SHA256(secret, signStr) uppercased, where:

    signStr = client_id + access_token + t + nonce + stringToSign

    stringToSign = HTTPMethod + "\n"
                 + SHA256(body) + "\n"
                 + signedHeaders + "\n"
                 + URL

For endpoints called before a token is acquired (e.g. GET /v1.0/token),
access_token is the empty string.
"""

import hashlib
import hmac
from urllib.parse import quote


def build_string_to_sign(
    *,
    method: str,
    path: str,
    query: dict[str, str] | None,
    body: bytes,
    signed_headers: str = "",
) -> str:
    """Build the canonical string-to-sign for a Tuya OpenAPI request."""
    body_sha = hashlib.sha256(body).hexdigest()
    if query:
        sorted_pairs = sorted(query.items())
        # Commas must not be percent-encoded; Tuya's server verifies signatures
        # against the decoded URL, so comma-separated values (e.g. codes=a,b)
        # must appear as literal commas in the canonical string.
        query_str = "&".join(f"{k}={quote(str(v), safe=',')}" for k, v in sorted_pairs)
        url = f"{path}?{query_str}"
    else:
        url = path
    return f"{method.upper()}\n{body_sha}\n{signed_headers}\n{url}"


def compute_signature(
    *,
    access_id: str,
    access_secret: str,
    timestamp_ms: str,
    nonce: str,
    access_token: str,
    string_to_sign: str,
) -> str:
    """Compute the Tuya v2 signature, returned uppercase hex."""
    sign_str = f"{access_id}{access_token}{timestamp_ms}{nonce}{string_to_sign}"
    digest = hmac.new(
        access_secret.encode("utf-8"),
        sign_str.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return digest.upper()
