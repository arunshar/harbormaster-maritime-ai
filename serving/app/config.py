"""12-factor settings for the Harbormaster serving plane. Read once, immutable.

Env prefix is HM_ (Harbormaster). The kinematic constant mirrors GeoTrace's
GT_VESSEL_V_MAX_KTS=25 and the HITL threshold mirrors GT_HITL_CONFIDENCE_THRESHOLD=0.7
(the reuse anchors in docs/phases/PHASE_1.md); only the env prefix differs.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Literal
from uuid import UUID

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# Phase 5 single-tenant sentinel: the zero UUID every row and session carries
# when HM_TENANT_ID is unset. Mirrors cdc/schema/tenancy.py's copy; a unit test
# asserts the two never drift.
DEFAULT_TENANT_ID = "00000000-0000-0000-0000-000000000000"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_prefix="HM_", extra="ignore")

    env: Literal["dev", "staging", "prod"] = "dev"
    version: str = "0.1.0"

    # storage (HITL queue + registry). Empty/unreachable -> in-memory backends.
    # Either a full DSN, or the parts below (the ECS task definition injects
    # HM_PG_USER / HM_PG_PASSWORD from the RDS-managed Secrets Manager secret
    # and HM_PG_HOST from Terraform; the secret is JSON, never a DSN).
    pg_dsn: str = ""
    pg_host: str = ""
    pg_port: int = 5432
    pg_db: str = "harbormaster"
    pg_user: str = ""
    pg_password: str = ""

    # Phase 2: the CDC-fed online watchlist read path. online_table empty keeps
    # the lookup disabled (Phase 1 behavior, golden outputs unchanged).
    # ddb_endpoint_url points boto3 at DynamoDB Local on the kind stack.
    online_table: str = ""
    ddb_endpoint_url: str = ""
    redis_url: str = ""
    watchlist_severity: float = Field(0.9, ge=0, le=1)
    sanctions_severity: float = Field(0.95, ge=0, le=1)
    # TTL is a staleness backstop only; CDC invalidation is the freshness path.
    watchlist_cache_ttl_s: int = Field(300, gt=0)

    # kinematics (hard physical bounds). vessel_v_max_kts * 0.514444 = 12.861 m/s.
    vessel_v_max_kts: float = Field(25.0, gt=0)
    vehicle_v_max_kmh: float = Field(130.0, gt=0)

    # scoring / HITL routing
    hitl_confidence_threshold: float = Field(0.7, ge=0, le=1)
    anomaly_hitl_threshold: float = Field(0.6, ge=0, le=1)

    # gap detection
    coverage_threshold_s: float = Field(600.0, gt=0)  # > 10 min between fixes is a gap
    gap_saturation_s: float = Field(7200.0, gt=0)  # >= 2 h silence saturates severity

    # speed scoring vs corrupt-data rejection. A vessel doing several x v_max is a
    # scored anomaly (spoofing / GPS error); a teleport beyond corrupt_reject_factor
    # x v_max is non-physical sensor corruption and is rejected (422), not scored.
    speed_saturation_mult: float = Field(3.0, gt=0)  # v_req = 4x v_max -> severity 1
    corrupt_reject_factor: float = Field(12.0, gt=1)

    # corridor (static sea-lane graph)
    corridor_artifact_path: str = "app/artifacts/corridors.json"
    off_corridor_threshold_m: float = Field(2_000.0, gt=0)
    corridor_saturation_m: float = Field(6_000.0, gt=0)  # deviation that saturates severity
    unexpected_node_heading_deg: float = Field(45.0, gt=0)
    # a course change within this distance of a node is expected, not anomalous
    waypoint_radius_m: float = Field(5_000.0, gt=0)

    # Phase 3: the SageMaker async Pi-DPM endpoint. Empty keeps the existing
    # analytic P_data estimator (Phase 1/2 goldens unchanged), matching the
    # online_table-empty-disables-lookup convention from Phase 2.
    pidpm_endpoint: str = ""
    pidpm_input_bucket: str = ""

    # Phase 5 gate 5.6: the Bedrock explanation layer. Empty keeps it disabled
    # (no client is ever constructed, explain() returns None), matching the
    # pidpm_endpoint / online_table empty-disables convention. Explanation
    # only, never a scoring path (docs/ARCHITECTURE.md:71).
    bedrock_model_id: str = ""

    # Phase 5 gate 5.7: the PPO route-optimization STRETCH service. A labeled
    # stretch, INDEPENDENT of the other Phase 5 toggles: default false, and even
    # when true it only gates the standalone mlops/route_optimizer FastAPI
    # service (never the scoring path, never the promotion pipeline). CPU-only,
    # never on AWS. Mirrors the empty/false-disables convention of the toggles
    # above; kept a plain bool because it flips a whole separate service on, not
    # a client endpoint.
    enable_phase5_ppo_stretch: bool = False

    # Phase 5 gate 5.4: this deployment's tenant. Empty means single-tenant
    # back-compat (every Postgres session pins the zero-UUID sentinel, so the
    # RLS policies pass and Phase 1-4 behavior is unchanged), matching the
    # HM_PIDPM_ENDPOINT / online_table empty-disables convention. Non-empty
    # must be a UUID: it becomes the app.tenant_id GUC on every DB session.
    tenant_id: str = ""

    @field_validator("tenant_id")
    @classmethod
    def _tenant_id_is_a_uuid_or_empty(cls, v: str) -> str:
        # Validate at construction, not at query time: a malformed tenant id
        # would otherwise surface as a ::uuid cast error inside every RLS
        # policy evaluation, the worst possible place to learn about a typo.
        if v:
            UUID(v)
        return v

    @property
    def vessel_v_max_mps(self) -> float:
        return self.vessel_v_max_kts * 0.514_444

    def resolved_tenant_id(self) -> str:
        """The tenant every Postgres session pins as app.tenant_id: the
        configured tenant when set, else the single-tenant zero-UUID sentinel
        (back-compat; rows and sessions agree, so nothing filters out)."""
        return self.tenant_id or DEFAULT_TENANT_ID

    def resolved_pg_dsn(self) -> str:
        """The DSN to connect with: pg_dsn verbatim, else built from the parts
        (password URL-quoted; RDS-managed passwords can contain reserved
        characters). Empty when neither is configured -> memory backends."""
        if self.pg_dsn:
            return self.pg_dsn
        if self.pg_host and self.pg_user and self.pg_password:
            from urllib.parse import quote

            return (
                f"postgresql://{quote(self.pg_user, safe='')}:"
                f"{quote(self.pg_password, safe='')}@"
                f"{self.pg_host}:{self.pg_port}/{self.pg_db}"
            )
        return ""


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
