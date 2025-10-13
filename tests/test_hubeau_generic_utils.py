import pytest

pytest.importorskip("dlt", reason="dlt must be installed to import hubeau_generic")

from dlt_pipeline.hubeau_generic import _evaluate_until_expr, _should_stop_pagination


def test_evaluate_until_expr_len_lt():
    payload = {"data": [1, 2, 3]}
    assert _evaluate_until_expr("len($.data) < 4", payload)
    assert not _evaluate_until_expr("len($.data) < 3", payload)


def test_evaluate_until_expr_invalid():
    with pytest.raises(ValueError):
        _evaluate_until_expr("invalid expr", {})


def test_should_stop_pagination_with_until_expr():
    payload = {"data": [1, 2]}
    pagination = {"page_size": 2, "until_expr": "len($.data) < 2"}
    assert not _should_stop_pagination(payload, payload["data"], pagination)
    pagination = {"page_size": 2, "until_expr": "len($.data) <= 2"}
    assert _should_stop_pagination(payload, payload["data"], pagination)
