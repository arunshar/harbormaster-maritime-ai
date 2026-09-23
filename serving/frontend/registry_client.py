"""Registry-tab client + pure view helpers (Phase 2, gate C1).

The Streamlit console's Registry tab edits vessels / watchlist / sanctions_flags
through the serving API, which writes Postgres, the system of record. The CDC
pipeline (Debezium -> cdc/consumer) is what propagates those edits to the online
store the scorer reads; this client never touches DynamoDB or Redis.

Pure helpers (payload builders, row formatting) unit-test without a server or
Streamlit; RegistryApi is the thin urllib client used at runtime, mirroring
hitl_client.HitlApi.
"""

from __future__ import annotations

import json
import urllib.request
from typing import Any


def watchlist_payload(reason: str, severity: float = 0.9, added_by: str = "") -> dict:
    """Build a PUT /v1/registry/watchlist/{mmsi} body; reject bad input early."""
    if not reason.strip():
        raise ValueError("reason must be non-empty")
    if not 0.0 <= severity <= 1.0:
        raise ValueError(f"severity must be in [0, 1], got {severity}")
    return {"reason": reason.strip(), "severity": severity, "added_by": added_by}


def sanction_payload(regime: str, reference: str = "") -> dict:
    """Build a PUT /v1/registry/sanctions/{mmsi} body."""
    if not regime.strip():
        raise ValueError("regime must be non-empty")
    return {"regime": regime.strip(), "reference": reference}


def vessel_payload(name: str = "", flag_state: str = "", vessel_type: str = "") -> dict:
    return {"name": name, "flag_state": flag_state, "vessel_type": vessel_type}


def format_watchlist_row(row: dict) -> dict:
    """Flatten a watchlist row into the console's table shape."""
    return {
        "mmsi": row.get("mmsi"),
        "reason": row.get("reason", ""),
        "severity": round(float(row.get("severity", 0.0)), 2),
        "added_by": row.get("added_by", ""),
        "updated_at": str(row.get("updated_at", "")),
    }


class RegistryApi:
    """Thin urllib client for the serving registry endpoints."""

    def __init__(self, base_url: str, timeout: float = 5.0) -> None:
        self.base = base_url.rstrip("/")
        self.timeout = timeout

    def _send(self, method: str, path: str, body: dict | None = None) -> Any:
        req = urllib.request.Request(
            f"{self.base}{path}",
            data=None if body is None else json.dumps(body).encode(),
            headers={"Content-Type": "application/json"},
            method=method,
        )
        with urllib.request.urlopen(req, timeout=self.timeout) as r:  # nosec B310  # fixed http(s) base_url to internal registry API, no user-supplied scheme
            return json.loads(r.read())

    def list_watchlist(self) -> list[dict]:
        return self._send("GET", "/v1/registry/watchlist")

    def put_watchlist(self, mmsi: int, payload: dict) -> dict:
        return self._send("PUT", f"/v1/registry/watchlist/{mmsi}", payload)

    def delete_watchlist(self, mmsi: int) -> dict:
        return self._send("DELETE", f"/v1/registry/watchlist/{mmsi}")

    def get_vessel(self, mmsi: int) -> dict:
        return self._send("GET", f"/v1/registry/vessels/{mmsi}")

    def put_vessel(self, mmsi: int, payload: dict) -> dict:
        return self._send("PUT", f"/v1/registry/vessels/{mmsi}", payload)

    def put_sanction(self, mmsi: int, payload: dict) -> dict:
        return self._send("PUT", f"/v1/registry/sanctions/{mmsi}", payload)

    def delete_sanctions(self, mmsi: int) -> dict:
        return self._send("DELETE", f"/v1/registry/sanctions/{mmsi}")
