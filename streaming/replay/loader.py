"""Fixture loader + schema validation (Phase 1.1, gate G1).

Parses streaming/fixtures/ais_recorded.jsonl, schema-validates every record, and
verifies the recorded SHA256. The 1.4 Fargate ingestor reuses `load_fixture` to
read records before PutRecords to Kinesis.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, ValidationError

REPO = Path(__file__).resolve().parents[2]
FIX_DIR = REPO / "streaming" / "fixtures"
JSONL = FIX_DIR / "ais_recorded.jsonl"
SHA = FIX_DIR / "ais_recorded.sha256"
EXPECT = FIX_DIR / "expectations.json"


class AisRecord(BaseModel):
    """Schema every fixture line must satisfy."""

    model_config = ConfigDict(extra="forbid")

    mmsi: int = Field(..., ge=0, le=999_999_999)
    lat: float = Field(..., ge=-90, le=90)
    lon: float = Field(..., ge=-180, le=180)
    t: datetime
    sog: float | None = Field(None, ge=0)
    cog: float | None = Field(None, ge=0, le=360)
    heading: float | None = Field(None, ge=0, le=360)


def load_fixture(path: Path | str = JSONL) -> list[AisRecord]:
    """Parse and schema-validate every record. Raises ValueError with the line
    number on the first malformed or schema-invalid record."""

    path = Path(path)
    out: list[AisRecord] = []
    for n, line in enumerate(path.read_text().splitlines(), start=1):
        line = line.strip()
        if not line:
            continue
        try:
            out.append(AisRecord(**json.loads(line)))
        except (json.JSONDecodeError, ValidationError) as exc:
            raise ValueError(f"{path.name} line {n}: {exc}") from exc
    return out


def sha256_of(path: Path | str = JSONL) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def recorded_sha256(path: Path | str = SHA) -> str:
    return Path(path).read_text().strip()


def load_expectations(path: Path | str = EXPECT) -> dict:
    return json.loads(Path(path).read_text())


def verify_fixture() -> bool:
    """True iff the fixture parses and its SHA256 matches the recorded value."""

    load_fixture()
    return sha256_of() == recorded_sha256()
