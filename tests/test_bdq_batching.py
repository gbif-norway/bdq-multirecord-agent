import requests
import pytest

from app.services.bdq_api_service import BDQAPIService


def _make_resp(items):
    class R:
        status_code = 200
        text = ""

        def __init__(self, items):
            self._items = items

        def json(self):
            return self._items

        def raise_for_status(self):
            pass

    return R(items)


def test_backoff_succeeds_on_two_chunks(monkeypatch):
    svc = BDQAPIService()
    payload = [{"id": "T", "params": {"i": i}} for i in range(6)]

    call_no = {"n": 0}

    def fake_post(url, json, timeout):
        call_no["n"] += 1
        if call_no["n"] == 1:
            # First attempt (1-chunk) times out
            raise requests.Timeout("simulate 1-chunk timeout")
        # 2-chunk stage succeeds; echo input order via params
        results = [
            {"status": "RUN_HAS_RESULT", "result": item["params"]["i"]}
            for item in json
        ]
        return _make_resp(results)

    monkeypatch.setattr(
        "app.services.bdq_api_service.http_post_with_retry", fake_post
    )

    ok, results = svc._post_batch_with_backoff(payload, timeout=1)
    assert ok
    assert len(results) == len(payload)
    assert [r["result"] for r in results] == list(range(6))


def test_backoff_succeeds_on_four_chunks(monkeypatch):
    svc = BDQAPIService()
    n = 9
    payload = [{"id": "T", "params": {"i": i}} for i in range(n)]

    # Simulate: 1-chunk timeout; 2-chunk second sub-batch timeout; 4-chunk all succeed
    stage = {"value": "one"}
    sub_call = {"index": 0}

    def fake_post(url, json, timeout):
        # Stage progression is inferred by call ordering and payload sizes
        if stage["value"] == "one":
            stage["value"] = "two"
            raise requests.Timeout("simulate 1-chunk timeout")
        elif stage["value"] == "two":
            # We expect two calls here: first half ok, second half timeout
            sub_call["index"] += 1
            if sub_call["index"] == 1:
                # first sub-batch OK
                return _make_resp(
                    [
                        {"status": "RUN_HAS_RESULT", "result": item["params"]["i"]}
                        for item in json
                    ]
                )
            else:
                # second sub-batch timeout -> move to four
                stage["value"] = "four"
                raise requests.Timeout("simulate 2nd sub-batch timeout")
        else:
            # four-chunk stage: always succeed
            return _make_resp(
                [
                    {"status": "RUN_HAS_RESULT", "result": item["params"]["i"]}
                    for item in json
                ]
            )

    monkeypatch.setattr(
        "app.services.bdq_api_service.http_post_with_retry", fake_post
    )

    ok, results = svc._post_batch_with_backoff(payload, timeout=1)
    assert ok
    assert len(results) == len(payload)
    assert [r["result"] for r in results] == list(range(n))


def test_backoff_succeeds_on_eight_chunks(monkeypatch):
    svc = BDQAPIService()
    n = 17
    payload = [{"id": "T", "params": {"i": i}} for i in range(n)]

    # Simulate: 1-chunk timeout; 2-chunk both timeout; 4-chunk one sub-batch timeout; 8-chunk all succeed
    four_calls = {"n": 0}

    def fake_post(url, json, timeout):
        # Decide behavior based on chunk size rather than absolute call number
        size = len(json)
        if size == 17:
            # 1-chunk stage
            raise requests.Timeout("simulate 1-chunk timeout")
        if size in (9, 8):
            # 2-chunk stage
            raise requests.Timeout("simulate 2-chunk timeout")
        if size in (5, 4):
            # 4-chunk stage: three succeed then one timeout
            four_calls["n"] += 1
            if four_calls["n"] < 4:
                return _make_resp([
                    {"status": "RUN_HAS_RESULT", "result": item["params"]["i"]}
                    for item in json
                ])
            raise requests.Timeout("simulate 4-chunk timeout")
        # 8-chunk stage (sizes 3/2 for n=17): all succeed
        return _make_resp([
            {"status": "RUN_HAS_RESULT", "result": item["params"]["i"]}
            for item in json
        ])

    monkeypatch.setattr(
        "app.services.bdq_api_service.http_post_with_retry", fake_post
    )

    ok, results = svc._post_batch_with_backoff(payload, timeout=1)
    assert ok
    assert len(results) == len(payload)
    assert [r["result"] for r in results] == list(range(n))


def test_backoff_fails_after_eight_chunks(monkeypatch):
    svc = BDQAPIService()
    n = 16
    payload = [{"id": "T", "params": {"i": i}} for i in range(n)]

    # Simulate: 1-chunk timeout; 2-chunk both timeout; 4-chunk one times out; 8-chunk one times out -> final failure
    four_seen = {"n": 0}
    eight_seen = {"n": 0}

    def fake_post(url, json, timeout):
        size = len(json)
        if size == 16:
            # 1-chunk stage
            raise requests.Timeout("simulate 1-chunk timeout")
        if size == 8:
            # 2-chunk stage
            raise requests.Timeout("simulate 2-chunk timeout")
        if size == 4:
            # 4-chunk stage: one of them times out
            four_seen["n"] += 1
            if four_seen["n"] == 2:
                raise requests.Timeout("simulate 4-chunk sub-batch timeout")
            return _make_resp([
                {"status": "RUN_HAS_RESULT", "result": item["params"]["i"]}
                for item in json
            ])
        if size == 2:
            # 8-chunk stage: third sub-batch times out
            eight_seen["n"] += 1
            if eight_seen["n"] == 3:
                raise requests.Timeout("simulate 8-chunk sub-batch timeout")
            return _make_resp([
                {"status": "RUN_HAS_RESULT", "result": item["params"]["i"]}
                for item in json
            ])
        # Fallback success
        return _make_resp([
            {"status": "RUN_HAS_RESULT", "result": item["params"]["i"]}
            for item in json
        ])

    monkeypatch.setattr(
        "app.services.bdq_api_service.http_post_with_retry", fake_post
    )

    ok, results = svc._post_batch_with_backoff(payload, timeout=1)
    assert not ok
    assert results is None
