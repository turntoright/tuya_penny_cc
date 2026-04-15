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


def test_string_to_sign_does_not_encode_commas_in_query_values():
    # Tuya verifies signatures against the decoded URL, so comma-separated values
    # (e.g. codes=add_ele,cur_power) must appear as literal commas, not %2C.
    s = build_string_to_sign(
        method="GET",
        path="/v2.0/cloud/thing/abc/report-logs",
        query={"codes": "add_ele,cur_power", "size": "10"},
        body=b"",
    )
    assert "codes=add_ele,cur_power" in s
    assert "%2C" not in s


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
