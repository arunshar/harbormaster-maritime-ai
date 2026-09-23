"""HITL reviewer-console client + pure view helpers (Phase 1.6, gate G6).

The Streamlit console (console.py) reads the pending review queue from the serving
API (GET /v1/hitl/pending) and submits verdicts (POST /v1/feedback). The pure
helpers here (row formatting, feedback payload, position extraction) unit-test
without a server or Streamlit; HitlApi is the thin urllib client used at runtime.
"""

from __future__ import annotations

import json
import urllib.request
from typing import Any

LABELS = ("correct", "incorrect", "ambiguous")


def feedback_payload(trace_id: str, label: str, reviewer: str, notes: str | None = None) -> dict:
    """Build a POST /v1/feedback body; rejects an out-of-set label early."""
    if label not in LABELS:
        raise ValueError(f"label must be one of {LABELS}, got {label!r}")
    body: dict[str, Any] = {"trace_id": trace_id, "label": label, "reviewer": reviewer}
    if notes:
        body["notes"] = notes
    return body


def reason_codes(row: dict) -> list[str]:
    """The reason-code strings on one queue row (reasons is a JSONB list of dicts)."""
    return [r.get("code", "") for r in row.get("reasons", []) if isinstance(r, dict)]


def format_row(row: dict) -> dict:
    """Flatten a pending queue row into the console's table shape."""
    return {
        "trace_id": row.get("trace_id"),
        "mmsi": row.get("mmsi"),
        "ts": str(row.get("ts", "")),
        "score": round(float(row.get("score", 0.0)), 3),
        "confidence": round(float(row.get("confidence", 0.0)), 3),
        "reasons": ", ".join(reason_codes(row)) or "-",
    }


def positions_from_rows(rows: list[dict]) -> list[dict]:
    """Best-effort map points: pull lat/lon from the first reason evidence that has them."""
    out: list[dict] = []
    for row in rows:
        for r in row.get("reasons", []):
            ev = r.get("evidence", {}) if isinstance(r, dict) else {}
            if "lat" in ev and "lon" in ev:
                out.append(
                    {"mmsi": row.get("mmsi"), "lat": float(ev["lat"]), "lon": float(ev["lon"])}
                )
                break
    return out


class HitlApi:
    """Thin urllib client for the serving HITL endpoints."""

    def __init__(self, base_url: str, timeout: float = 5.0) -> None:
        self.base = base_url.rstrip("/")
        self.timeout = timeout

    def pending(self) -> list[dict]:
        with urllib.request.urlopen(f"{self.base}/v1/hitl/pending", timeout=self.timeout) as r:  # nosec B310  # fixed http(s) base_url to internal serving API, no user-supplied scheme
            return json.loads(r.read())

    def feedback(self, payload: dict) -> dict:
        req = urllib.request.Request(
            f"{self.base}/v1/feedback",
            data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=self.timeout) as r:  # nosec B310  # fixed http(s) base_url to internal serving API, no user-supplied scheme
            return json.loads(r.read())
