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
