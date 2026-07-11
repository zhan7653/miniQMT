from scripts.create_fake_data import create_fake_v2_portal


def test_future_feature_does_not_affect_signal_date(tmp_path):
    portal = create_fake_v2_portal(tmp_path / "v2")
    signal = portal.get_features(["510300.SH"], "2026-01-09")
    future = portal.get_features(["510300.SH"], "2026-01-12")
    assert signal.iloc[0]["ret_20d"] != future.iloc[0]["ret_20d"]
    assert signal.index.tolist() == ["510300.SH"]
