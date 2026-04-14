# Ingestion MVP Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the minimum viable ingestion pipeline that fetches the Tuya
device list and writes it to a BigQuery raw table, runnable end-to-end from a
local machine.

**Architecture:** Python package under `ingestion/` that exposes a typer CLI.
A thin `tuya/` HTTP client handles auth, signing, and retries. A `bq/` writer
batch-loads JSON rows to BigQuery. A `jobs/device_sync.py` orchestrates one
ingestion task end-to-end. Configuration comes from environment variables for
local dev (Secret Manager is added later in Plan D).

**Tech Stack:** Python 3.11+, uv (package manager), pydantic + pydantic-settings,
httpx, tenacity, typer, google-cloud-bigquery, pytest, respx (HTTP mocking).

**Out of scope (later plans):** Energy-data jobs, backfill, dbt project,
Cloud Run deployment, Cloud Scheduler, Secret Manager, CI/CD, `ops_runs` table.
This plan stops once `device_sync` writes a row to BigQuery from a developer
laptop.

---

## Pre-requisites (engineer must have before starting)

- Python 3.11 or newer.
- `uv` installed (`pip install uv` or per https://docs.astral.sh/uv/).
- `gcloud` CLI installed and authenticated; Application Default Credentials
  configured: `gcloud auth application-default login`.
- A target GCP project with BigQuery enabled.
- A BigQuery dataset named `tuya_raw` in that project, in a region of choice
  (e.g. `US` or `asia-east1`). Create with:
  `bq --location=US mk --dataset <PROJECT_ID>:tuya_raw`.
- A Tuya IoT Cloud project with `access_id` and `access_secret`.
- The user's Tuya account linked to that cloud project, with at least one
  device assigned. (Tuya: Cloud → Project → Devices → Link Tuya App Account.)

---

## File Structure

This plan creates the following files. Each file has one responsibility.

```
ingestion/
├── pyproject.toml                              # deps, build config, ruff/pytest config
├── README.md                                   # how to run locally
├── .python-version                             # 3.11
├── .env.example                                # template for required env vars
├── src/
│   └── tuya_penny_cc/
│       ├── __init__.py                         # version
│       ├── main.py                             # typer CLI dispatch on --task
│       ├── config.py                           # Settings (pydantic-settings)
│       ├── tuya/
│       │   ├── __init__.py
│       │   ├── auth.py                         # signing + token cache (pure functions where possible)
│       │   ├── client.py                       # httpx-based HTTP client with retries
│       │   └── models.py                       # pydantic response models
│       ├── bq/
│       │   ├── __init__.py
│       │   ├── schemas.py                      # raw_devices BigQuery schema
│       │   └── writer.py                       # batch loader (load_table_from_json)
│       └── jobs/
│           ├── __init__.py
│           └── device_sync.py                  # orchestrates client + writer + lineage
└── tests/
    ├── __init__.py
    ├── conftest.py                             # shared fixtures
    └── unit/
        ├── __init__.py
        ├── test_config.py
        ├── tuya/
        │   ├── __init__.py
        │   ├── test_auth.py
        │   └── test_client.py
        ├── bq/
        │   ├── __init__.py
        │   └── test_writer.py
        └── jobs/
            ├── __init__.py
            └── test_device_sync.py
```

Branch convention: continue work on `init_claude` (current branch). Each task
ends with a commit. Commits use Conventional Commits.

---

## Task 1: Bootstrap the `ingestion/` Python package

**Files:**
- Create: `ingestion/pyproject.toml`
- Create: `ingestion/.python-version`
- Create: `ingestion/.env.example`
- Create: `ingestion/README.md`
- Create: `ingestion/src/tuya_penny_cc/__init__.py`
- Create: `ingestion/src/tuya_penny_cc/main.py` (placeholder)
- Create: `ingestion/tests/__init__.py`
- Create: `ingestion/tests/unit/__init__.py`

- [ ] **Step 1: Create `ingestion/pyproject.toml`**

```toml
[project]
name = "tuya-penny-cc-ingestion"
version = "0.1.0"
description = "Tuya IoT ingestion to BigQuery"
requires-python = ">=3.11"
dependencies = [
    "google-cloud-bigquery>=3.25.0",
    "httpx>=0.27.0",
    "pydantic>=2.7.0",
    "pydantic-settings>=2.3.0",
    "tenacity>=8.5.0",
    "typer>=0.12.0",
]

[dependency-groups]
dev = [
    "pytest>=8.2.0",
    "pytest-cov>=5.0.0",
    "respx>=0.21.0",
    "ruff>=0.5.0",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/tuya_penny_cc"]

[tool.pytest.ini_options]
testpaths = ["tests"]
addopts = "-ra --strict-markers"

[tool.ruff]
line-length = 100
target-version = "py311"

[tool.ruff.lint]
select = ["E", "F", "I", "B", "UP", "SIM"]
```

- [ ] **Step 2: Create `ingestion/.python-version`**

```
3.11
```

- [ ] **Step 3: Create `ingestion/.env.example`**

```bash
# Tuya IoT Cloud credentials (Cloud → Project → Authorization Key)
TUYA_ACCESS_ID=
TUYA_ACCESS_SECRET=
# Pick the regional endpoint matching your account.
# Common values: https://openapi.tuyacn.com (China), https://openapi.tuyaus.com (US),
#                https://openapi.tuyaeu.com (EU), https://openapi.tuyain.com (India)
TUYA_BASE_URL=https://openapi.tuyacn.com
# Tuya project's data center user UID (Cloud → Project → Linked Accounts → UID)
TUYA_USER_UID=

# GCP / BigQuery
GCP_PROJECT_ID=
BQ_DATASET_RAW=tuya_raw
# Optional: location for the dataset; only needed if not US.
BQ_LOCATION=US
```

- [ ] **Step 4: Create `ingestion/README.md`**

```markdown
# tuya_penny_cc ingestion

Python ingestion pipeline that reads from Tuya OpenAPI and writes to BigQuery.

## Local setup

1. Install [uv](https://docs.astral.sh/uv/).
2. From this directory:
   ```bash
   uv sync
   ```
3. Authenticate with GCP:
   ```bash
   gcloud auth application-default login
   ```
4. Copy the env template and fill in values:
   ```bash
   cp .env.example .env
   # edit .env
   ```

## Run a job

```bash
uv run python -m tuya_penny_cc --task=device_sync
```

## Tests

```bash
uv run pytest
```
```

- [ ] **Step 5: Create empty package files**

`ingestion/src/tuya_penny_cc/__init__.py`:
```python
__version__ = "0.1.0"
```

`ingestion/src/tuya_penny_cc/main.py`:
```python
def app() -> None:
    raise SystemExit("CLI not implemented yet")


if __name__ == "__main__":
    app()
```

`ingestion/tests/__init__.py` and `ingestion/tests/unit/__init__.py`: empty files.

- [ ] **Step 6: Initialize uv lockfile and verify install**

Run from `ingestion/`:
```bash
uv sync
```
Expected: creates `.venv/`, installs all deps, generates `uv.lock`.

- [ ] **Step 7: Verify ruff and pytest run**

Run from `ingestion/`:
```bash
uv run ruff check .
uv run pytest
```
Expected: ruff passes (no files to check or all clean), pytest reports
`no tests ran` (exit 5 is OK for empty test set; treat as pass for now).

- [ ] **Step 8: Commit**

```bash
git add ingestion/
git commit -m "feat(ingestion): bootstrap Python package skeleton"
```

---

## Task 2: Settings module loaded from environment variables

**Files:**
- Create: `ingestion/src/tuya_penny_cc/config.py`
- Create: `ingestion/tests/unit/test_config.py`

- [ ] **Step 1: Write the failing test**

`ingestion/tests/unit/test_config.py`:
```python
from tuya_penny_cc.config import Settings


def test_settings_loads_from_env(monkeypatch):
    monkeypatch.setenv("TUYA_ACCESS_ID", "id123")
    monkeypatch.setenv("TUYA_ACCESS_SECRET", "secret456")
    monkeypatch.setenv("TUYA_BASE_URL", "https://openapi.tuyacn.com")
    monkeypatch.setenv("TUYA_USER_UID", "uid789")
    monkeypatch.setenv("GCP_PROJECT_ID", "my-proj")
    monkeypatch.setenv("BQ_DATASET_RAW", "tuya_raw")

    s = Settings()

    assert s.tuya_access_id == "id123"
    assert s.tuya_access_secret.get_secret_value() == "secret456"
    assert s.tuya_base_url == "https://openapi.tuyacn.com"
    assert s.tuya_user_uid == "uid789"
    assert s.gcp_project_id == "my-proj"
    assert s.bq_dataset_raw == "tuya_raw"
    assert s.bq_location == "US"  # default


def test_settings_missing_required_raises(monkeypatch):
    monkeypatch.delenv("TUYA_ACCESS_ID", raising=False)
    monkeypatch.delenv("TUYA_ACCESS_SECRET", raising=False)
    monkeypatch.delenv("GCP_PROJECT_ID", raising=False)
    import pytest
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        Settings(_env_file=None)
```

- [ ] **Step 2: Run test to verify it fails**

Run from `ingestion/`:
```bash
uv run pytest tests/unit/test_config.py -v
```
Expected: ImportError / ModuleNotFoundError on `tuya_penny_cc.config`.

- [ ] **Step 3: Implement `config.py`**

`ingestion/src/tuya_penny_cc/config.py`:
```python
from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables (or .env)."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Tuya
    tuya_access_id: str
    tuya_access_secret: SecretStr
    tuya_base_url: str
    tuya_user_uid: str

    # BigQuery
    gcp_project_id: str
    bq_dataset_raw: str = "tuya_raw"
    bq_location: str = "US"
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/unit/test_config.py -v
```
Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add ingestion/src/tuya_penny_cc/config.py ingestion/tests/unit/test_config.py
git commit -m "feat(ingestion): add Settings module loaded from env"
```

---

## Task 3: Tuya request signing

The Tuya OpenAPI v2.0 signature scheme HMAC-SHA256s a string built from the
client id, the (optional) access token, a millisecond timestamp, a nonce, and
a canonical "string to sign" derived from the HTTP request. We isolate this in
a pure function for testability.

Reference: Tuya docs "Sign requests" — engineer should verify the algorithm
against current docs at `https://developer.tuya.com/en/docs/iot/new-singnature`
during this task and adjust if Tuya has changed it.

**Files:**
- Create: `ingestion/src/tuya_penny_cc/tuya/__init__.py` (empty)
- Create: `ingestion/src/tuya_penny_cc/tuya/auth.py`
- Create: `ingestion/tests/unit/tuya/__init__.py` (empty)
- Create: `ingestion/tests/unit/tuya/test_auth.py`

- [ ] **Step 1: Write the failing tests**

`ingestion/tests/unit/tuya/test_auth.py`:
```python
from tuya_penny_cc.tuya.auth import build_string_to_sign, compute_signature


def test_string_to_sign_for_get_no_body():
    s = build_string_to_sign(method="GET", path="/v1.0/token", query={"grant_type": "1"}, body=b"")
    # Format: METHOD\nSHA256(body)\nheaders(empty)\nURL
    # SHA256("") hex = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
    expected = (
        "GET\n"
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\n"
        "\n"
        "/v1.0/token?grant_type=1"
    )
    assert s == expected


def test_string_to_sign_sorts_query_alphabetically():
    s = build_string_to_sign(
        method="GET", path="/v1.0/x", query={"b": "2", "a": "1"}, body=b""
    )
    assert s.endswith("/v1.0/x?a=1&b=2")


def test_signature_is_uppercase_hex_64_chars():
    sig = compute_signature(
        access_id="id",
        access_secret="secret",
        timestamp_ms="1700000000000",
        nonce="n1",
        access_token="",
        string_to_sign="GET\ne3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\n\n/v1.0/token?grant_type=1",
    )
    assert len(sig) == 64
    assert sig.isupper()
    assert all(c in "0123456789ABCDEF" for c in sig)


def test_signature_changes_when_secret_changes():
    args = dict(
        access_id="id",
        timestamp_ms="1700000000000",
        nonce="n1",
        access_token="",
        string_to_sign="GET\n\n\n/x",
    )
    a = compute_signature(access_secret="secret_a", **args)
    b = compute_signature(access_secret="secret_b", **args)
    assert a != b


def test_signature_changes_when_access_token_present():
    args = dict(
        access_id="id",
        access_secret="secret",
        timestamp_ms="1700000000000",
        nonce="n1",
        string_to_sign="GET\n\n\n/x",
    )
    no_token = compute_signature(access_token="", **args)
    with_token = compute_signature(access_token="tok123", **args)
    assert no_token != with_token


def test_signature_is_deterministic():
    args = dict(
        access_id="id",
        access_secret="secret",
        timestamp_ms="1700000000000",
        nonce="n1",
        access_token="tok",
        string_to_sign="GET\n\n\n/x",
    )
    assert compute_signature(**args) == compute_signature(**args)
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/unit/tuya/test_auth.py -v
```
Expected: ImportError on `tuya_penny_cc.tuya.auth`.

- [ ] **Step 3: Implement `auth.py`**

`ingestion/src/tuya_penny_cc/tuya/__init__.py`: empty file.

`ingestion/src/tuya_penny_cc/tuya/auth.py`:
```python
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
        query_str = "&".join(f"{k}={quote(str(v), safe='')}" for k, v in sorted_pairs)
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
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/unit/tuya/test_auth.py -v
```
Expected: 6 passed.

- [ ] **Step 5: Commit**

```bash
git add ingestion/src/tuya_penny_cc/tuya/__init__.py \
        ingestion/src/tuya_penny_cc/tuya/auth.py \
        ingestion/tests/unit/tuya/__init__.py \
        ingestion/tests/unit/tuya/test_auth.py
git commit -m "feat(tuya): add OpenAPI v2 request signing"
```

---

## Task 4: Tuya HTTP client with token caching and retries

**Files:**
- Create: `ingestion/src/tuya_penny_cc/tuya/client.py`
- Create: `ingestion/src/tuya_penny_cc/tuya/models.py`
- Create: `ingestion/tests/unit/tuya/test_client.py`
- Create: `ingestion/tests/conftest.py`

- [ ] **Step 1: Add a shared `respx` fixture in `conftest.py`**

`ingestion/tests/conftest.py`:
```python
import pytest
import respx


@pytest.fixture
def mock_router():
    with respx.mock(base_url="https://openapi.test", assert_all_called=False) as router:
        yield router
```

- [ ] **Step 2: Write the failing tests for token fetch**

`ingestion/tests/unit/tuya/test_client.py`:
```python
import httpx
import pytest

from tuya_penny_cc.tuya.client import TuyaClient


def make_client():
    return TuyaClient(
        base_url="https://openapi.test",
        access_id="id",
        access_secret="secret",
        user_uid="uid",
        clock=lambda: 1700000000.0,  # deterministic timestamps in tests
        nonce_factory=lambda: "fixed-nonce",
    )


def test_fetch_token_success(mock_router):
    mock_router.get("/v1.0/token", params={"grant_type": "1"}).mock(
        return_value=httpx.Response(
            200,
            json={
                "result": {
                    "access_token": "tok-abc",
                    "refresh_token": "rt-xyz",
                    "expire_time": 7200,
                    "uid": "uid",
                },
                "success": True,
                "t": 1700000000000,
            },
        )
    )
    c = make_client()
    token = c._fetch_token()
    assert token.access_token == "tok-abc"
    assert token.expires_at_epoch == pytest.approx(1700000000.0 + 7200)


def test_fetch_token_retries_on_5xx(mock_router):
    route = mock_router.get("/v1.0/token", params={"grant_type": "1"})
    route.side_effect = [
        httpx.Response(500, json={"msg": "boom"}),
        httpx.Response(500, json={"msg": "boom"}),
        httpx.Response(
            200,
            json={
                "result": {
                    "access_token": "tok",
                    "refresh_token": "rt",
                    "expire_time": 7200,
                    "uid": "uid",
                },
                "success": True,
                "t": 1700000000000,
            },
        ),
    ]
    c = make_client()
    token = c._fetch_token()
    assert token.access_token == "tok"
    assert route.call_count == 3


def test_fetch_token_gives_up_after_max_retries(mock_router):
    route = mock_router.get("/v1.0/token", params={"grant_type": "1"})
    route.return_value = httpx.Response(500, json={"msg": "down"})
    c = make_client()
    with pytest.raises(httpx.HTTPStatusError):
        c._fetch_token()
    assert route.call_count == 3  # 1 attempt + 2 retries by tenacity config


def test_token_is_cached_until_expiry(mock_router):
    route = mock_router.get("/v1.0/token", params={"grant_type": "1"}).mock(
        return_value=httpx.Response(
            200,
            json={
                "result": {
                    "access_token": "tok",
                    "refresh_token": "rt",
                    "expire_time": 7200,
                    "uid": "uid",
                },
                "success": True,
                "t": 1700000000000,
            },
        )
    )
    c = make_client()
    a = c._get_access_token()
    b = c._get_access_token()
    assert a == b == "tok"
    assert route.call_count == 1
```

- [ ] **Step 3: Run tests to verify they fail**

```bash
uv run pytest tests/unit/tuya/test_client.py -v
```
Expected: ImportError on `tuya_penny_cc.tuya.client`.

- [ ] **Step 4: Implement `models.py`**

`ingestion/src/tuya_penny_cc/tuya/models.py`:
```python
from dataclasses import dataclass


@dataclass(frozen=True)
class TuyaToken:
    access_token: str
    refresh_token: str
    expires_at_epoch: float
    uid: str
```

- [ ] **Step 5: Implement `client.py` (token-only first; device list comes in Task 5)**

`ingestion/src/tuya_penny_cc/tuya/client.py`:
```python
"""HTTP client for Tuya OpenAPI v2.0.

Responsibilities:
- Sign every request (including the bare token request) per Tuya v2 rules.
- Cache the access token in-memory until ~5 min before expiry.
- Retry transient failures (HTTP 5xx, 429) with exponential backoff.

Not responsible for:
- Persisting the token across process restarts.
- Refresh-token flow (we just re-fetch a new access token; cheap enough
  for our scale).
"""

from __future__ import annotations

import time
import uuid
from collections.abc import Callable
from typing import Any

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from tuya_penny_cc.tuya.auth import build_string_to_sign, compute_signature
from tuya_penny_cc.tuya.models import TuyaToken

_TOKEN_PATH = "/v1.0/token"
_TOKEN_QUERY = {"grant_type": "1"}
_TOKEN_REFRESH_LEEWAY_SECONDS = 300


class TuyaClient:
    def __init__(
        self,
        *,
        base_url: str,
        access_id: str,
        access_secret: str,
        user_uid: str,
        clock: Callable[[], float] = time.time,
        nonce_factory: Callable[[], str] = lambda: uuid.uuid4().hex,
        http_client: httpx.Client | None = None,
        timeout: float = 30.0,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._access_id = access_id
        self._access_secret = access_secret
        self._user_uid = user_uid
        self._clock = clock
        self._nonce_factory = nonce_factory
        self._http = http_client or httpx.Client(base_url=self._base_url, timeout=timeout)
        self._token: TuyaToken | None = None

    # ---- token management ------------------------------------------------

    def _get_access_token(self) -> str:
        now = self._clock()
        if self._token is None or self._token.expires_at_epoch - now < _TOKEN_REFRESH_LEEWAY_SECONDS:
            self._token = self._fetch_token()
        return self._token.access_token

    @retry(
        retry=retry_if_exception_type(httpx.HTTPStatusError),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=0.5, min=0.5, max=4),
        reraise=True,
    )
    def _fetch_token(self) -> TuyaToken:
        payload = self._signed_request(
            method="GET",
            path=_TOKEN_PATH,
            query=_TOKEN_QUERY,
            access_token="",
        )
        result = payload["result"]
        return TuyaToken(
            access_token=result["access_token"],
            refresh_token=result["refresh_token"],
            expires_at_epoch=self._clock() + float(result["expire_time"]),
            uid=result["uid"],
        )

    # ---- signed request helper ------------------------------------------

    def _signed_request(
        self,
        *,
        method: str,
        path: str,
        query: dict[str, str] | None,
        access_token: str,
        body: bytes = b"",
    ) -> dict[str, Any]:
        timestamp_ms = str(int(self._clock() * 1000))
        nonce = self._nonce_factory()
        sts = build_string_to_sign(method=method, path=path, query=query, body=body)
        sig = compute_signature(
            access_id=self._access_id,
            access_secret=self._access_secret,
            timestamp_ms=timestamp_ms,
            nonce=nonce,
            access_token=access_token,
            string_to_sign=sts,
        )
        headers = {
            "client_id": self._access_id,
            "sign": sig,
            "sign_method": "HMAC-SHA256",
            "t": timestamp_ms,
            "nonce": nonce,
            "Content-Type": "application/json",
        }
        if access_token:
            headers["access_token"] = access_token
        resp = self._http.request(method, path, params=query, content=body, headers=headers)
        resp.raise_for_status()
        body_json = resp.json()
        if not body_json.get("success", False):
            raise httpx.HTTPStatusError(
                f"Tuya API error: code={body_json.get('code')} msg={body_json.get('msg')}",
                request=resp.request,
                response=resp,
            )
        return body_json

    # ---- lifecycle -------------------------------------------------------

    def close(self) -> None:
        self._http.close()
```

- [ ] **Step 6: Run tests to verify they pass**

```bash
uv run pytest tests/unit/tuya/test_client.py -v
```
Expected: 4 passed.

- [ ] **Step 7: Commit**

```bash
git add ingestion/src/tuya_penny_cc/tuya/client.py \
        ingestion/src/tuya_penny_cc/tuya/models.py \
        ingestion/tests/conftest.py \
        ingestion/tests/unit/tuya/test_client.py
git commit -m "feat(tuya): add HTTP client with token caching and retry"
```

---

## Task 5: Device list endpoint

Tuya's device-list endpoint for cloud projects is paginated. We use the
`/v1.3/iot-03/devices` endpoint with `last_row_key`-based pagination. The
engineer should confirm against current Tuya docs, since Tuya occasionally
introduces newer endpoint versions.

**Files:**
- Modify: `ingestion/src/tuya_penny_cc/tuya/client.py` (add `list_devices`)
- Modify: `ingestion/tests/unit/tuya/test_client.py` (add tests)

- [ ] **Step 1: Add failing tests**

Append to `ingestion/tests/unit/tuya/test_client.py`:
```python
def _token_response():
    return httpx.Response(
        200,
        json={
            "result": {
                "access_token": "tok",
                "refresh_token": "rt",
                "expire_time": 7200,
                "uid": "uid",
            },
            "success": True,
            "t": 1700000000000,
        },
    )


def test_list_devices_single_page(mock_router):
    mock_router.get("/v1.0/token", params={"grant_type": "1"}).mock(return_value=_token_response())
    mock_router.get("/v1.3/iot-03/devices").mock(
        return_value=httpx.Response(
            200,
            json={
                "result": {
                    "list": [
                        {"id": "d1", "name": "Switch A"},
                        {"id": "d2", "name": "Switch B"},
                    ],
                    "has_more": False,
                    "last_row_key": "",
                },
                "success": True,
                "t": 1700000000000,
            },
        )
    )
    c = make_client()
    devices = list(c.list_devices(page_size=20))
    assert [d["id"] for d in devices] == ["d1", "d2"]


def test_list_devices_paginates(mock_router):
    mock_router.get("/v1.0/token", params={"grant_type": "1"}).mock(return_value=_token_response())
    page1 = httpx.Response(
        200,
        json={
            "result": {
                "list": [{"id": "d1"}],
                "has_more": True,
                "last_row_key": "rk1",
            },
            "success": True,
            "t": 1700000000000,
        },
    )
    page2 = httpx.Response(
        200,
        json={
            "result": {
                "list": [{"id": "d2"}, {"id": "d3"}],
                "has_more": False,
                "last_row_key": "",
            },
            "success": True,
            "t": 1700000000000,
        },
    )
    route = mock_router.get("/v1.3/iot-03/devices")
    route.side_effect = [page1, page2]
    c = make_client()
    devices = list(c.list_devices(page_size=20))
    assert [d["id"] for d in devices] == ["d1", "d2", "d3"]
    assert route.call_count == 2
```

- [ ] **Step 2: Run to verify failure**

```bash
uv run pytest tests/unit/tuya/test_client.py::test_list_devices_single_page -v
```
Expected: AttributeError on `list_devices`.

- [ ] **Step 3: Implement `list_devices`**

Append to `ingestion/src/tuya_penny_cc/tuya/client.py` (inside the `TuyaClient`
class, after `_signed_request`):

```python
    # ---- devices ---------------------------------------------------------

    DEVICES_PATH = "/v1.3/iot-03/devices"

    def list_devices(self, *, page_size: int = 100):
        """Yield device dicts across all pages."""
        access_token = self._get_access_token()
        last_row_key = ""
        while True:
            query: dict[str, str] = {
                "page_size": str(page_size),
            }
            if last_row_key:
                query["last_row_key"] = last_row_key
            payload = self._signed_request(
                method="GET",
                path=self.DEVICES_PATH,
                query=query,
                access_token=access_token,
            )
            result = payload["result"]
            for device in result.get("list", []):
                yield device
            if not result.get("has_more"):
                break
            last_row_key = result.get("last_row_key", "")
            if not last_row_key:
                break
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/unit/tuya/test_client.py -v
```
Expected: 6 passed.

- [ ] **Step 5: Commit**

```bash
git add ingestion/src/tuya_penny_cc/tuya/client.py \
        ingestion/tests/unit/tuya/test_client.py
git commit -m "feat(tuya): add paginated device list endpoint"
```

---

## Task 6: BigQuery schema and writer

**Files:**
- Create: `ingestion/src/tuya_penny_cc/bq/__init__.py` (empty)
- Create: `ingestion/src/tuya_penny_cc/bq/schemas.py`
- Create: `ingestion/src/tuya_penny_cc/bq/writer.py`
- Create: `ingestion/tests/unit/bq/__init__.py` (empty)
- Create: `ingestion/tests/unit/bq/test_writer.py`

- [ ] **Step 1: Write the failing tests**

`ingestion/tests/unit/bq/test_writer.py`:
```python
from unittest.mock import MagicMock

import pytest
from google.cloud import bigquery

from tuya_penny_cc.bq.schemas import RAW_DEVICES_SCHEMA
from tuya_penny_cc.bq.writer import BigQueryWriter


def test_raw_devices_schema_has_lineage_columns():
    names = {f.name for f in RAW_DEVICES_SCHEMA}
    required = {
        "ingest_ts",
        "ingest_run_id",
        "source_endpoint",
        "payload",
        "device_id",
        "ingest_date",
    }
    assert required.issubset(names)


def test_raw_devices_payload_is_json_type():
    payload_field = next(f for f in RAW_DEVICES_SCHEMA if f.name == "payload")
    assert payload_field.field_type == "JSON"
    assert payload_field.mode == "REQUIRED"


def test_writer_load_calls_bigquery_load_table_from_json():
    mock_client = MagicMock(spec=bigquery.Client)
    mock_load_job = MagicMock()
    mock_load_job.output_rows = 2
    mock_client.load_table_from_json.return_value = mock_load_job

    writer = BigQueryWriter(client=mock_client, project="proj", dataset="tuya_raw")
    rows = [{"device_id": "d1"}, {"device_id": "d2"}]
    written = writer.load("raw_devices", rows, schema=RAW_DEVICES_SCHEMA)

    assert written == 2
    assert mock_client.load_table_from_json.called
    kwargs = mock_client.load_table_from_json.call_args.kwargs
    assert mock_client.load_table_from_json.call_args.args[0] == rows
    assert mock_client.load_table_from_json.call_args.args[1] == "proj.tuya_raw.raw_devices"
    job_config = kwargs["job_config"]
    assert job_config.schema == RAW_DEVICES_SCHEMA
    assert job_config.write_disposition == "WRITE_APPEND"
    mock_load_job.result.assert_called_once()


def test_writer_load_empty_rows_is_noop():
    mock_client = MagicMock(spec=bigquery.Client)
    writer = BigQueryWriter(client=mock_client, project="proj", dataset="tuya_raw")
    written = writer.load("raw_devices", [], schema=RAW_DEVICES_SCHEMA)
    assert written == 0
    mock_client.load_table_from_json.assert_not_called()


def test_writer_load_propagates_load_job_failure():
    mock_client = MagicMock(spec=bigquery.Client)
    mock_load_job = MagicMock()
    mock_load_job.result.side_effect = RuntimeError("BQ load failed")
    mock_client.load_table_from_json.return_value = mock_load_job

    writer = BigQueryWriter(client=mock_client, project="proj", dataset="tuya_raw")
    with pytest.raises(RuntimeError, match="BQ load failed"):
        writer.load("raw_devices", [{"x": 1}], schema=RAW_DEVICES_SCHEMA)
```

- [ ] **Step 2: Run to verify failure**

```bash
uv run pytest tests/unit/bq/test_writer.py -v
```
Expected: ImportError.

- [ ] **Step 3: Implement `schemas.py`**

`ingestion/src/tuya_penny_cc/bq/__init__.py`: empty.

`ingestion/src/tuya_penny_cc/bq/schemas.py`:
```python
"""BigQuery schemas for raw landing tables.

Conventions (per design spec §7.2):
- Every raw_* table includes lineage columns: ingest_ts, ingest_run_id,
  source_endpoint, payload, plus a partition column.
- Business keys (device_id, stat_date) are promoted from payload to top
  level for partition pruning and join performance.
"""

from google.cloud import bigquery

RAW_DEVICES_SCHEMA: list[bigquery.SchemaField] = [
    bigquery.SchemaField("ingest_ts", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("ingest_run_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source_endpoint", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("payload", "JSON", mode="REQUIRED"),
    bigquery.SchemaField("device_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("ingest_date", "DATE", mode="REQUIRED"),
]
```

- [ ] **Step 4: Implement `writer.py`**

`ingestion/src/tuya_penny_cc/bq/writer.py`:
```python
"""BigQuery batch writer.

Wraps `Client.load_table_from_json` because at our scale (≤100 devices,
a few runs per day) batch loads are free and avoid streaming-buffer
side effects on partitioned tables.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from google.cloud import bigquery


class BigQueryWriter:
    def __init__(self, *, client: bigquery.Client, project: str, dataset: str) -> None:
        self._client = client
        self._project = project
        self._dataset = dataset

    def load(
        self,
        table: str,
        rows: Sequence[dict[str, Any]],
        *,
        schema: list[bigquery.SchemaField],
    ) -> int:
        """Append `rows` to `<project>.<dataset>.<table>` and return rows written."""
        if not rows:
            return 0
        table_id = f"{self._project}.{self._dataset}.{table}"
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition="WRITE_APPEND",
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )
        job = self._client.load_table_from_json(rows, table_id, job_config=job_config)
        job.result()  # raises on failure
        return int(job.output_rows or len(rows))
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
uv run pytest tests/unit/bq/test_writer.py -v
```
Expected: 5 passed.

- [ ] **Step 6: Commit**

```bash
git add ingestion/src/tuya_penny_cc/bq/__init__.py \
        ingestion/src/tuya_penny_cc/bq/schemas.py \
        ingestion/src/tuya_penny_cc/bq/writer.py \
        ingestion/tests/unit/bq/__init__.py \
        ingestion/tests/unit/bq/test_writer.py
git commit -m "feat(bq): add raw_devices schema and BigQuery batch writer"
```

---

## Task 7: `device_sync` job — orchestration with lineage

**Files:**
- Create: `ingestion/src/tuya_penny_cc/jobs/__init__.py` (empty)
- Create: `ingestion/src/tuya_penny_cc/jobs/device_sync.py`
- Create: `ingestion/tests/unit/jobs/__init__.py` (empty)
- Create: `ingestion/tests/unit/jobs/test_device_sync.py`

- [ ] **Step 1: Write the failing tests**

`ingestion/tests/unit/jobs/test_device_sync.py`:
```python
import json
from datetime import datetime, timezone
from unittest.mock import MagicMock

from tuya_penny_cc.bq.schemas import RAW_DEVICES_SCHEMA
from tuya_penny_cc.jobs.device_sync import run


def test_run_writes_one_row_per_device_with_lineage():
    fake_client = MagicMock()
    fake_client.list_devices.return_value = iter(
        [
            {"id": "d1", "name": "Switch A", "category": "kg"},
            {"id": "d2", "name": "Switch B", "category": "kg"},
        ]
    )
    fake_writer = MagicMock()
    fake_writer.load.return_value = 2

    fixed_now = datetime(2026, 4, 14, 12, 0, 0, tzinfo=timezone.utc)
    rows_written = run(
        tuya=fake_client,
        writer=fake_writer,
        run_id="run-uuid-1",
        now=lambda: fixed_now,
    )

    assert rows_written == 2
    fake_writer.load.assert_called_once()
    table_arg, rows_arg = fake_writer.load.call_args.args
    assert table_arg == "raw_devices"
    assert fake_writer.load.call_args.kwargs["schema"] == RAW_DEVICES_SCHEMA
    assert len(rows_arg) == 2

    first = rows_arg[0]
    assert first["device_id"] == "d1"
    assert first["ingest_run_id"] == "run-uuid-1"
    assert first["ingest_ts"] == "2026-04-14T12:00:00+00:00"
    assert first["ingest_date"] == "2026-04-14"
    assert first["source_endpoint"] == "/v1.3/iot-03/devices"
    assert json.loads(first["payload"])["name"] == "Switch A"


def test_run_with_no_devices_writes_nothing_but_returns_zero():
    fake_client = MagicMock()
    fake_client.list_devices.return_value = iter([])
    fake_writer = MagicMock()
    fake_writer.load.return_value = 0

    n = run(
        tuya=fake_client,
        writer=fake_writer,
        run_id="r",
        now=lambda: datetime(2026, 4, 14, tzinfo=timezone.utc),
    )
    assert n == 0
    # Writer is still called with [] so behavior is consistent; writer no-ops.
    fake_writer.load.assert_called_once()
```

- [ ] **Step 2: Run to verify failure**

```bash
uv run pytest tests/unit/jobs/test_device_sync.py -v
```
Expected: ImportError.

- [ ] **Step 3: Implement the job**

`ingestion/src/tuya_penny_cc/jobs/__init__.py`: empty.

`ingestion/src/tuya_penny_cc/jobs/device_sync.py`:
```python
"""Device-list sync job.

Reads the full device list from Tuya, wraps each device dict into a raw
row with lineage columns, and batch-loads to `raw_devices`.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any, Protocol

from tuya_penny_cc.bq.schemas import RAW_DEVICES_SCHEMA

SOURCE_ENDPOINT = "/v1.3/iot-03/devices"
TABLE = "raw_devices"


class _TuyaLike(Protocol):
    def list_devices(self, *, page_size: int = 100) -> Any: ...


class _WriterLike(Protocol):
    def load(self, table: str, rows: list[dict[str, Any]], *, schema: list) -> int: ...


def run(
    *,
    tuya: _TuyaLike,
    writer: _WriterLike,
    run_id: str,
    now: Callable[[], datetime] = lambda: datetime.now(tz=timezone.utc),
    page_size: int = 100,
) -> int:
    """Run the device sync. Returns number of rows written."""
    ts = now()
    ts_iso = ts.isoformat()
    date_iso = ts.date().isoformat()

    rows: list[dict[str, Any]] = []
    for device in tuya.list_devices(page_size=page_size):
        rows.append(
            {
                "ingest_ts": ts_iso,
                "ingest_run_id": run_id,
                "source_endpoint": SOURCE_ENDPOINT,
                "payload": json.dumps(device, ensure_ascii=False),
                "device_id": device["id"],
                "ingest_date": date_iso,
            }
        )
    return writer.load(TABLE, rows, schema=RAW_DEVICES_SCHEMA)
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/unit/jobs/test_device_sync.py -v
```
Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add ingestion/src/tuya_penny_cc/jobs/__init__.py \
        ingestion/src/tuya_penny_cc/jobs/device_sync.py \
        ingestion/tests/unit/jobs/__init__.py \
        ingestion/tests/unit/jobs/test_device_sync.py
git commit -m "feat(jobs): add device_sync orchestration with lineage columns"
```

---

## Task 8: CLI entry point (`python -m tuya_penny_cc`)

**Files:**
- Modify: `ingestion/src/tuya_penny_cc/main.py` (replace stub with typer app)
- Create: `ingestion/src/tuya_penny_cc/__main__.py`

- [ ] **Step 1: Add `__main__.py` so `python -m tuya_penny_cc` works**

`ingestion/src/tuya_penny_cc/__main__.py`:
```python
from tuya_penny_cc.main import app

if __name__ == "__main__":
    app()
```

- [ ] **Step 2: Replace `main.py` with the typer CLI**

`ingestion/src/tuya_penny_cc/main.py`:
```python
"""CLI entry point. Dispatches to per-task modules based on --task."""

from __future__ import annotations

import logging
import uuid
from enum import Enum

import typer
from google.cloud import bigquery

from tuya_penny_cc.bq.writer import BigQueryWriter
from tuya_penny_cc.config import Settings
from tuya_penny_cc.jobs import device_sync
from tuya_penny_cc.tuya.client import TuyaClient

logger = logging.getLogger("tuya_penny_cc")


class Task(str, Enum):
    device_sync = "device_sync"


def app() -> None:
    typer.run(_main)


def _main(
    task: Task = typer.Option(..., "--task", help="Which ingestion task to run."),
    log_level: str = typer.Option("INFO", "--log-level"),
) -> None:
    logging.basicConfig(level=log_level.upper(), format="%(asctime)s %(levelname)s %(name)s %(message)s")
    settings = Settings()
    run_id = uuid.uuid4().hex
    logger.info("starting task=%s run_id=%s", task.value, run_id)

    bq_client = bigquery.Client(project=settings.gcp_project_id, location=settings.bq_location)
    writer = BigQueryWriter(
        client=bq_client,
        project=settings.gcp_project_id,
        dataset=settings.bq_dataset_raw,
    )
    tuya = TuyaClient(
        base_url=settings.tuya_base_url,
        access_id=settings.tuya_access_id,
        access_secret=settings.tuya_access_secret.get_secret_value(),
        user_uid=settings.tuya_user_uid,
    )
    try:
        if task is Task.device_sync:
            written = device_sync.run(tuya=tuya, writer=writer, run_id=run_id)
        else:
            raise typer.BadParameter(f"Unknown task: {task}")
        logger.info("task=%s wrote %d rows", task.value, written)
    finally:
        tuya.close()


if __name__ == "__main__":
    app()
```

- [ ] **Step 3: Sanity-check the CLI loads (no real call yet)**

Run from `ingestion/`:
```bash
uv run python -m tuya_penny_cc --help
```
Expected: typer prints usage including `--task` option with `device_sync` choice.

- [ ] **Step 4: Run the full test suite**

```bash
uv run pytest -v
uv run ruff check .
```
Expected: all tests pass; ruff clean.

- [ ] **Step 5: Commit**

```bash
git add ingestion/src/tuya_penny_cc/main.py \
        ingestion/src/tuya_penny_cc/__main__.py
git commit -m "feat(cli): add typer entry point dispatching on --task"
```

---

## Task 9: End-to-end smoke run (manual, against real Tuya + BigQuery)

This task is **manual** — it talks to real systems. Do it once, document the
result, and use it as the acceptance gate for Plan A.

**Pre-checks:**
- `.env` file in `ingestion/` is populated (do NOT commit it; `.env` is in
  `.gitignore` already).
- The user's Tuya account is linked to the cloud project and has at least
  one device.
- The BigQuery dataset `<GCP_PROJECT_ID>.tuya_raw` exists and the user
  running the command has `bigquery.dataEditor` on it.
- Application Default Credentials are active:
  `gcloud auth application-default print-access-token` succeeds.

- [ ] **Step 1: Run the job**

From `ingestion/`:
```bash
uv run python -m tuya_penny_cc --task=device_sync
```
Expected log lines (approximate):
```
... INFO tuya_penny_cc starting task=device_sync run_id=<32-hex>
... INFO tuya_penny_cc task=device_sync wrote N rows
```
where N matches the number of devices in your Tuya account.

- [ ] **Step 2: Verify rows in BigQuery**

```bash
bq query --use_legacy_sql=false \
  "SELECT COUNT(*) AS n, MIN(ingest_ts) AS first_ts, MAX(ingest_ts) AS last_ts
   FROM \`${GCP_PROJECT_ID}.tuya_raw.raw_devices\`
   WHERE ingest_date = CURRENT_DATE()"
```
Expected: `n` equals the `wrote N rows` from Step 1.

- [ ] **Step 3: Spot-check a row**

```bash
bq query --use_legacy_sql=false --max_rows=1 \
  "SELECT device_id, JSON_VALUE(payload, '\$.name') AS name, ingest_run_id
   FROM \`${GCP_PROJECT_ID}.tuya_raw.raw_devices\`
   WHERE ingest_date = CURRENT_DATE()
   LIMIT 1"
```
Expected: a real device id and human-readable name.

- [ ] **Step 4: Document the smoke-run result**

Add a short note at the bottom of `ingestion/README.md` under a "Verified
end-to-end on YYYY-MM-DD" line, e.g.:

```markdown
## Verified end-to-end

- 2026-04-14: `device_sync` wrote 17 rows to `proj.tuya_raw.raw_devices`.
```

- [ ] **Step 5: Commit the verification note**

```bash
git add ingestion/README.md
git commit -m "docs(ingestion): record initial end-to-end smoke run"
```

---

## Acceptance criteria for Plan A

- All unit tests pass: `uv run pytest -v` (≥ 17 tests).
- ruff is clean: `uv run ruff check .`.
- `python -m tuya_penny_cc --task=device_sync` against the real Tuya + BQ
  writes one row per device into `<project>.tuya_raw.raw_devices`.
- Each row has populated lineage columns (`ingest_ts`, `ingest_run_id`,
  `source_endpoint`, `payload`, `device_id`, `ingest_date`).
- README documents how to set up and run locally.

---

## What Plan B will pick up

- Three more job modules under `jobs/`: `energy_realtime`, `energy_hourly`,
  `energy_daily`.
- Three more raw schemas + tables.
- A `backfill` job that loops the aggregate jobs across a date range.
- Additional Tuya client methods for the energy / statistics endpoints.

Plan B reuses everything built here (config, signing, client, writer,
lineage pattern). The architecture in this plan is the template.
