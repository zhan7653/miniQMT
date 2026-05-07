import pytest

from fundlab.data.processors import normalize_symbol


def test_normalize_symbol():
    assert normalize_symbol("510300") == "510300.SH"
    assert normalize_symbol("159915") == "159915.SZ"
    assert normalize_symbol("510300.SH") == "510300.SH"


def test_normalize_symbol_rejects_invalid_code():
    with pytest.raises(ValueError):
        normalize_symbol("abc")

