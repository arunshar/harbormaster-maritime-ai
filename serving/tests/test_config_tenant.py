"""Settings.tenant_id tests (Phase 5, gate 5.4): the empty-disables back-compat
convention, the UUID guard, and the session-tenant resolver."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from app.config import DEFAULT_TENANT_ID, Settings

TENANT = "11111111-1111-1111-1111-111111111111"


def test_default_is_single_tenant_backcompat():
    s = Settings()
    assert s.tenant_id == ""
    assert s.resolved_tenant_id() == DEFAULT_TENANT_ID


def test_a_configured_tenant_resolves_to_itself():
    s = Settings(tenant_id=TENANT)
    assert s.resolved_tenant_id() == TENANT


def test_a_malformed_tenant_id_is_rejected_at_construction():
    # not at query time, where it would surface as a ::uuid cast error inside
    # every RLS policy evaluation
    with pytest.raises(ValidationError):
        Settings(tenant_id="not-a-uuid")


def test_the_sentinel_is_the_zero_uuid():
    assert DEFAULT_TENANT_ID == "00000000-0000-0000-0000-000000000000"
