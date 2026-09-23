from __future__ import annotations

import pandas as pd

from lake.export_training_set import data_fingerprint, export_training_set, point_in_time_join


def _feature_events() -> pd.DataFrame:
    # mmsi 1 has two known fixes; mmsi 2 has one.
    return pd.DataFrame(
        {
            "mmsi": [1, 1, 2],
            "t": pd.to_datetime(
                ["2024-06-01T00:00:00Z", "2024-06-01T00:10:00Z", "2024-06-01T00:05:00Z"]
            ),
            "lat": [40.0, 40.5, 41.0],
            "lon": [-74.0, -74.5, -75.0],
        }
    )


def test_request_between_two_fixes_gets_the_earlier_one_not_the_later():
    requests = pd.DataFrame(
        {
            "mmsi": [1],
            "event_timestamp": pd.to_datetime(["2024-06-01T00:05:00Z"]),  # between t0 and t1
        }
    )
    out = point_in_time_join(requests, _feature_events())
    assert out.iloc[0]["lat"] == 40.0  # the 00:00 fix, never the 00:10 fix


def test_request_before_any_history_gets_nulls_not_a_future_value():
    requests = pd.DataFrame(
        {
            "mmsi": [1],
            "event_timestamp": pd.to_datetime(["2024-05-31T23:00:00Z"]),  # before any fix
        }
    )
    out = point_in_time_join(requests, _feature_events())
    assert pd.isna(out.iloc[0]["lat"])


def test_request_exactly_at_a_fix_timestamp_may_use_that_fix():
    # merge_asof(direction="backward") includes an exact-timestamp match: the
    # feature is "as of" that instant, the conventional feature-store semantic.
    requests = pd.DataFrame(
        {
            "mmsi": [1],
            "event_timestamp": pd.to_datetime(["2024-06-01T00:10:00Z"]),
        }
    )
    out = point_in_time_join(requests, _feature_events())
    assert out.iloc[0]["lat"] == 40.5


def test_request_after_the_latest_fix_gets_the_latest_not_a_future_row():
    requests = pd.DataFrame(
        {
            "mmsi": [1],
            "event_timestamp": pd.to_datetime(["2024-06-01T01:00:00Z"]),  # well after both fixes
        }
    )
    out = point_in_time_join(requests, _feature_events())
    assert out.iloc[0]["lat"] == 40.5  # the latest known fix, not NaN and not a future one


def test_join_never_leaks_a_later_entitys_row_across_entities():
    requests = pd.DataFrame(
        {
            "mmsi": [2],
            "event_timestamp": pd.to_datetime(["2024-06-01T00:06:00Z"]),
        }
    )
    out = point_in_time_join(requests, _feature_events())
    assert out.iloc[0]["lat"] == 41.0  # mmsi 2's own fix, not mmsi 1's


def test_data_fingerprint_is_deterministic():
    df = _feature_events()
    assert data_fingerprint(df, timestamp_col="t") == data_fingerprint(df, timestamp_col="t")


def test_data_fingerprint_changes_with_the_data():
    df = _feature_events()
    changed = df.copy()
    changed.loc[0, "lat"] = 99.0
    assert data_fingerprint(df, timestamp_col="t") != data_fingerprint(changed, timestamp_col="t")


def test_data_fingerprint_changes_with_row_count():
    df = _feature_events()
    fewer = df.iloc[:-1]
    assert data_fingerprint(df, timestamp_col="t") != data_fingerprint(fewer, timestamp_col="t")


def test_export_training_set_returns_join_and_fingerprint_together():
    requests = pd.DataFrame(
        {
            "mmsi": [1, 2],
            "event_timestamp": pd.to_datetime(["2024-06-01T00:05:00Z", "2024-06-01T00:06:00Z"]),
        }
    )
    exported, fingerprint = export_training_set(requests, _feature_events())
    assert len(exported) == 2
    assert fingerprint == data_fingerprint(exported, timestamp_col="event_timestamp")
