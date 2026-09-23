"""Stable error codes. Never lose context. Never swallow.

Vendored shape from GeoTrace-Agent (app/errors.py); KinematicViolation keeps its
name and 422 status because the vendored ValidatorAgent raises it.
"""

from __future__ import annotations

from typing import Any


class HarbormasterError(Exception):
    code: str = "harbormaster.unknown"
    http_status: int = 500
    message: str = "internal error"

    def __init__(self, message: str | None = None, **context: Any) -> None:
        super().__init__(message or self.message)
        if message:
            self.message = message
        self.context = context


class KinematicViolation(HarbormasterError):
    """A region or observed segment violates the hard kinematic invariant."""

    code = "harbormaster.kinematic_violation"
    http_status = 422


class CorruptInput(HarbormasterError):
    """AIS input is non-physical (teleport beyond corrupt_reject_factor x v_max)."""

    code = "harbormaster.corrupt_input"
    http_status = 422


class GuardrailTripped(HarbormasterError):
    code = "harbormaster.guardrail"
    http_status = 400


class HitlTraceNotFound(HarbormasterError):
    """Feedback references a trace_id that is not in the HITL queue."""

    code = "harbormaster.hitl_trace_not_found"
    http_status = 404


class RegistryEntryNotFound(HarbormasterError):
    """A registry lookup (vessel / watchlist / sanctions) matched nothing."""

    code = "harbormaster.registry_not_found"
    http_status = 404
