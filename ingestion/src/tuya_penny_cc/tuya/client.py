"""HTTP client for Tuya OpenAPI v2.0.

Responsibilities:
- Sign every request (including the bare token request) per Tuya v2 rules.
- Cache the access token in-memory until ~5 min before expiry.
- Retry transient failures (HTTP 5xx, 429) with exponential backoff on token fetch.

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
from urllib.parse import quote

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
        if self._token is not None:
            time_until_expiry = self._token.expires_at_epoch - now
            should_refresh = time_until_expiry < _TOKEN_REFRESH_LEEWAY_SECONDS
        else:
            should_refresh = True
        if should_refresh:
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

    # ---- devices ---------------------------------------------------------

    DEVICES_PATH = "/v2.0/cloud/thing/device"
    # v2 endpoint caps page_size at 20; larger values return an error.
    _DEVICES_MAX_PAGE_SIZE = 20

    def list_devices(self, *, page_size: int = 20):
        """Yield device dicts across all pages.

        Uses /v2.0/cloud/thing/device which returns a flat list under ``result``.
        Pagination is inferred: when the page contains fewer items than
        ``page_size`` there are no more pages.
        """
        page_size = min(page_size, self._DEVICES_MAX_PAGE_SIZE)
        access_token = self._get_access_token()
        page_no = 1
        while True:
            payload = self._signed_request(
                method="GET",
                path=self.DEVICES_PATH,
                query={"page_size": str(page_size), "page_no": str(page_no)},
                access_token=access_token,
            )
            devices: list = payload.get("result") or []
            yield from devices
            if len(devices) < page_size:
                break
            page_no += 1

    # ---- device DPs -------------------------------------------------------------

    DPS_PATH = "/v1.0/iot-03/devices/{device_id}/status"

    def get_device_dps(self, device_id: str) -> list[dict]:
        """Return current DP values for a single device.

        Calls /v1.0/iot-03/devices/{device_id}/status and returns the
        ``result`` list of {code, value} dicts. Returns an empty list if
        the endpoint returns no result.
        """
        access_token = self._get_access_token()
        path = self.DPS_PATH.format(device_id=quote(device_id, safe=""))
        payload = self._signed_request(
            method="GET",
            path=path,
            query=None,
            access_token=access_token,
        )
        return payload.get("result") or []

    # ---- energy stats -----------------------------------------------------------

    ENERGY_STATS_PATH = "/v1.0/iot-03/devices/{device_id}/statistics-month"
    _VALID_GRANULARITIES: frozenset[str] = frozenset({"hour", "day"})

    def get_energy_stats(
        self,
        device_id: str,
        granularity: str,
        start_ts_ms: int,
        end_ts_ms: int,
    ) -> list[dict]:
        """Return aggregated energy stats for a single device.

        NOTE: Endpoint path and query param names are best-guess starting points
        based on Tuya OpenAPI conventions. Verify against actual API response
        during smoke testing and adjust path/params as needed.

        Args:
            device_id: Tuya device ID.
            granularity: ``"hour"`` or ``"day"``.
            start_ts_ms: Window start as Unix milliseconds.
            end_ts_ms: Window end as Unix milliseconds.
        """
        if granularity not in self._VALID_GRANULARITIES:
            valid = sorted(self._VALID_GRANULARITIES)
            raise ValueError(
                f"granularity must be one of {valid!r}, got {granularity!r}"
            )
        access_token = self._get_access_token()
        path = self.ENERGY_STATS_PATH.format(device_id=quote(device_id, safe=""))
        payload = self._signed_request(
            method="GET",
            path=path,
            query={
                "type": granularity,
                "start_time": str(start_ts_ms),
                "end_time": str(end_ts_ms),
            },
            access_token=access_token,
        )
        return payload.get("result") or []

    # ---- device DP log ----------------------------------------------------------

    DP_LOG_PATH = "/v2.0/cloud/thing/{device_id}/report-logs"
    # Confirmed working during smoke test (2026-04-15).

    # Tuya rate-limit error code for the DP log endpoint.
    _DP_LOG_RATE_LIMIT_CODE = 40000309
    # Per-page retry budget and base wait in seconds (doubles each attempt).
    _DP_LOG_RATE_LIMIT_RETRIES = 5
    _DP_LOG_RATE_LIMIT_BASE_WAIT_S = 10

    def get_dp_log(
        self,
        device_id: str,
        codes: list[str],
        start_ts_ms: int,
        end_ts_ms: int,
    ) -> list[dict]:
        """Return historical DP change records for a single device.

        Paginates using last_row_key cursor until all records are fetched.
        Returns a flat list of {code, value, event_time} dicts.

        Automatically retries individual pages on Tuya's rate-limit error
        (code 40000309) with exponential backoff (10 s, 20 s, … up to 5 times).
        """
        if not codes:
            raise ValueError("codes must be a non-empty list")
        path = self.DP_LOG_PATH.format(device_id=quote(device_id, safe=""))
        events: list[dict] = []
        last_row_key: str | None = None
        while True:
            query: dict[str, str] = {
                "codes": ",".join(codes),
                "start_time": str(start_ts_ms),
                "end_time": str(end_ts_ms),
                "size": "50",
            }
            if last_row_key:
                query["last_row_key"] = last_row_key
            payload = self._fetch_dp_log_page(path, query)
            result = payload.get("result") or {}
            page_events: list[dict] = result.get("logs") or []
            events.extend(page_events)
            last_row_key = result.get("last_row_key")  # None or "" both mean no more pages
            if not last_row_key:
                break
        return events

    def _fetch_dp_log_page(
        self,
        path: str,
        query: dict[str, str],
    ) -> dict:
        """Fetch a single DP log page, retrying on rate-limit (40000309).

        Calls _get_access_token() on every attempt so the token is always
        fresh even during long paginations that exceed its ~110-min lifetime.
        """
        for attempt in range(self._DP_LOG_RATE_LIMIT_RETRIES + 1):
            try:
                return self._signed_request(
                    method="GET",
                    path=path,
                    query=query,
                    access_token=self._get_access_token(),
                )
            except httpx.HTTPStatusError as exc:
                if attempt < self._DP_LOG_RATE_LIMIT_RETRIES:
                    try:
                        code = exc.response.json().get("code")
                    except Exception:
                        raise exc
                    if code == self._DP_LOG_RATE_LIMIT_CODE:
                        wait = min(
                            self._DP_LOG_RATE_LIMIT_BASE_WAIT_S * (2**attempt),
                            60,
                        )
                        time.sleep(wait)
                        continue
                raise
        raise RuntimeError("unreachable")

    # ---- lifecycle -------------------------------------------------------

    def close(self) -> None:
        self._http.close()
