from scripts.create_fake_data import create_fake_v2_portal


def test_legacy_pit_datasets_are_absent_from_normal_v2_portal(tmp_path):
    portal = create_fake_v2_portal(tmp_path / "v2")
    for method in ("get_index_valuation", "get_nav", "get_premium_discount", "get_dividends"):
        assert not hasattr(portal, method)
