"""Unit tests for the live AISStream path (gate G9). No network: streams injected."""

from __future__ import annotations

import json
from unittest.mock import Mock

import pytest
from botocore.exceptions import ClientError

from ingestor.ingest import PutRecordsExhaustedError, _kinesis_putter, record_to_entry
from ingestor.live import DEFAULT_BBOX, parse_aisstream_message, run_live, subscribe_message


def _position(mmsi: int, heading=50) -> str:
    return json.dumps(
        {
            "MessageType": "PositionReport",
            "MetaData": {
                "MMSI": mmsi,
                "latitude": 40.4,
                "longitude": -74.0,
                "time_utc": "2024-06-01 05:00:00.000000000 +0000 UTC",
            },
            "Message": {"PositionReport": {"Sog": 9.7, "Cog": 49.6, "TrueHeading": heading}},
        }
    )


def test_subscribe_message_shape():
    m = subscribe_message("KEY")
    assert m["APIKey"] == "KEY"
    assert m["BoundingBoxes"] == DEFAULT_BBOX
    assert m["FilterMessageTypes"] == ["PositionReport"]


def test_parse_position_report():
    rec = parse_aisstream_message(_position(200000001))
    assert rec is not None
    assert rec.mmsi == 200000001
    assert (rec.lat, rec.lon, rec.sog, rec.heading) == (40.4, -74.0, 9.7, 50.0)
    assert rec.t.year == 2024 and rec.t.hour == 5


def test_parse_ignores_non_position_and_na_heading():
    assert parse_aisstream_message(json.dumps({"MessageType": "ShipStaticData"})) is None
    rec = parse_aisstream_message(_position(1, heading=511))  # 511 = not available
    assert rec is not None and rec.heading is None


def test_run_live_streams_and_returns_count():
    handled = []
    n = run_live(
        open_stream=lambda: [_position(1), '{"MessageType":"x"}', _position(2)],
        handle=handled.append,
        sleep=lambda _s: None,
    )
    assert n == 2
    assert [r.mmsi for r in handled] == [1, 2]


def test_run_live_reconnects_with_backoff_then_succeeds():
    calls = {"n": 0}
    slept: list[float] = []

    def flaky_stream():
        calls["n"] += 1
        if calls["n"] == 1:
            raise ConnectionError("dropped")
        return [_position(7)]

    n = run_live(flaky_stream, handle=lambda _r: None, sleep=slept.append, max_reconnects=3)
    assert n == 1
    assert len(slept) == 1  # one backoff before the successful reconnect
    assert slept[0] > 0


def test_run_live_reconnects_after_midstream_source_failure():
    calls = 0
    handled = []
    slept: list[float] = []

    def open_stream():
        nonlocal calls
        calls += 1
        if calls == 1:

            def interrupted_stream():
                yield _position(1)
                raise ConnectionError("dropped midstream")

            return interrupted_stream()
        return iter([_position(2)])

    n = run_live(open_stream, handle=handled.append, sleep=slept.append, max_reconnects=2)

    assert n == 2
    assert [record.mmsi for record in handled] == [1, 2]
    assert calls == 2
    assert len(slept) == 1


def test_run_live_raises_after_exhausting_retries():
    def always_fails():
        raise ConnectionError("down")

    with pytest.raises(ConnectionError):
        run_live(always_fails, handle=lambda _r: None, sleep=lambda _s: None, max_reconnects=2)


def test_run_live_surfaces_sink_exhaustion_without_reconnecting():
    opened = 0
    slept: list[float] = []

    def open_stream():
        nonlocal opened
        opened += 1
        return [_position(1)]

    def fail_sink(_record):
        raise PutRecordsExhaustedError(
            failed_count=1,
            attempts=2,
            error_codes={"ProvisionedThroughputExceededException": 1},
        )

    with pytest.raises(PutRecordsExhaustedError):
        run_live(open_stream, handle=fail_sink, sleep=slept.append, max_reconnects=3)

    assert opened == 1
    assert slept == []


def test_run_live_surfaces_raw_client_error_without_reconnecting():
    opened = 0
    slept: list[float] = []
    client = Mock()
    error = ClientError(
        {"Error": {"Code": "AccessDeniedException", "Message": "denied"}},
        "PutRecords",
    )
    client.put_records.side_effect = error
    put = _kinesis_putter(client, "ais-raw")

    def open_stream():
        nonlocal opened
        opened += 1
        return [_position(1)]

    with pytest.raises(ClientError) as raised:
        run_live(
            open_stream,
            handle=lambda record: put([record_to_entry(record)]),
            sleep=slept.append,
            max_reconnects=3,
        )

    assert raised.value is error
    assert opened == 1
    assert slept == []
