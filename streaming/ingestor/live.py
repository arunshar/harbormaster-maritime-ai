"""Live AISStream.io ingest path (Phase 1.9, gate G9).

Off by default (AIS_LIVE=true enables it; REPLAY stays the CI/test path). Connects
to the AISStream.io websocket, subscribes to a bounding box, maps PositionReport
messages to AisRecord, and hands them to a sink (the same Kinesis putter the replay
path uses). Reconnects with the ingestor's capped exponential backoff. The message
parser, the subscribe frame, and the reconnect loop are unit-tested with injected
streams; the real websocket client is imported lazily in main().
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterable
from datetime import datetime

from ingestor.ingest import backoff_schedule
from replay.loader import AisRecord

AISSTREAM_URL = "wss://stream.aisstream.io/v0/stream"
# Demo bounding box roughly covering the US East approaches the fixture uses.
DEFAULT_BBOX = [[[-76.0, 38.0], [-72.0, 41.5]]]
_HEADING_NA = 511  # AIS "not available" heading sentinel


def subscribe_message(api_key: str, bbox: list | None = None) -> dict:
    """The first frame AISStream expects: key + bounding box + message-type filter."""
    return {
        "APIKey": api_key,
        "BoundingBoxes": bbox or DEFAULT_BBOX,
        "FilterMessageTypes": ["PositionReport"],
    }


def _parse_ais_time(s: str) -> datetime:
    # AISStream MetaData.time_utc: "2024-06-01 05:00:00.000000000 +0000 UTC".
    s = s.strip().removesuffix(" UTC").strip()
    if "." in s:
        head, rest = s.split(".", 1)
        frac, _, tz = rest.partition(" ")
        return datetime.strptime(f"{head}.{frac[:6]} {tz}", "%Y-%m-%d %H:%M:%S.%f %z")
    return datetime.strptime(s, "%Y-%m-%d %H:%M:%S %z")


def parse_aisstream_message(raw: str | bytes) -> AisRecord | None:
    """Map an AISStream PositionReport to an AisRecord; None for any other message."""
    d = json.loads(raw)
    if d.get("MessageType") != "PositionReport":
        return None
    meta = d.get("MetaData", {})
    pr = d.get("Message", {}).get("PositionReport", {})
    try:
        hdg = pr.get("TrueHeading")
        return AisRecord(
            mmsi=int(meta["MMSI"]),
            lat=float(meta["latitude"]),
            lon=float(meta["longitude"]),
            t=_parse_ais_time(str(meta["time_utc"])),
            sog=None if pr.get("Sog") is None else float(pr["Sog"]),
            cog=None if pr.get("Cog") is None else float(pr["Cog"]),
            heading=None if hdg in (None, _HEADING_NA) else float(hdg),
        )
    except (KeyError, ValueError, TypeError):
        return None  # skip malformed / partial reports rather than crash the stream


def run_live(
    open_stream: Callable[[], Iterable[str]],
    handle: Callable[[AisRecord], None],
    sleep: Callable[[float], None],
    max_reconnects: int = 5,
) -> int:
    """Consume PositionReports from open_stream() into `handle`.

    Source and parse failures reconnect with capped backoff. Sink failures from
    `handle` propagate immediately because reconnecting cannot replay a record that
    was already consumed. A clean stream end returns; exhausting source retries
    re-raises. Dependencies are injected so the loop tests with no network.
    """
    handled = 0
    for attempt in range(max_reconnects + 1):
        try:
            stream = iter(open_stream())
        except Exception:
            if attempt >= max_reconnects:
                raise
            sleep(backoff_schedule(attempt + 1))
            continue

        while True:
            try:
                msg = next(stream)
                rec = parse_aisstream_message(msg)
            except StopIteration:
                return handled
            except Exception:
                if attempt >= max_reconnects:
                    raise
                sleep(backoff_schedule(attempt + 1))
                break

            if rec is not None:
                # Sink failures propagate immediately. Reconnecting the source
                # cannot replay a record that has already been consumed.
                handle(rec)
                handled += 1
    return handled


def real_open_stream(api_key: str, url: str = AISSTREAM_URL) -> Iterable[str]:
    """Connect + subscribe, then yield raw websocket messages. Lazy websocket dep."""
    import websocket  # websocket-client

    ws = websocket.create_connection(url)
    ws.send(json.dumps(subscribe_message(api_key)))
    try:
        while True:
            yield ws.recv()
    finally:
        ws.close()
