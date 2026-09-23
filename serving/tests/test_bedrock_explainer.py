"""BedrockExplainer tests (Phase 5, gate 5.6): prompt-construction content
assertions (no raw trajectory leakage), disabled-by-default, the happy path
against an injected fake client, and failure fail-open. No AWS anywhere; the
smoke criterion (no AWS profile or Bedrock access configured -> explain()
returns None without raising) is the from_settings default-Settings test."""

from __future__ import annotations

import re
from typing import Any

import pytest

from app.bedrock_explainer import BedrockExplainer, build_prompt
from app.config import Settings
from app.models import ReasonCode

ALL_REASON_CODES = [c.value for c in ReasonCode]
SCORE = 0.874


class FakeBedrock:
    def __init__(self, *, text: str = "A calm narrative paragraph.", err: Exception | None = None):
        self.text = text
        self.err = err
        self.calls: list[dict[str, Any]] = []

    def converse(self, **kwargs: Any) -> dict:
        self.calls.append(kwargs)
        if self.err:
            raise self.err
        return {"output": {"message": {"content": [{"text": self.text}]}}}


def _explainer(client: Any | None, model_id: str = "anthropic.claude-3-haiku") -> BedrockExplainer:
    return BedrockExplainer(bedrock_client=client, model_id=model_id)


# ---- prompt construction: the no-leak guarantee -----------------------------


def test_prompt_contains_only_the_reason_codes_and_the_numeric_score():
    prompt = build_prompt(ALL_REASON_CODES, SCORE)
    for code in ALL_REASON_CODES:
        assert code in prompt
    assert f"{SCORE:.3f}" in prompt
    # the ONLY numbers in the prompt are the score and its 0-to-1 scale: no
    # coordinate, timestamp, or trajectory value can be present
    assert sorted(re.findall(r"\d+(?:\.\d+)?", prompt)) == sorted(["0.874", "0", "1"])


def test_prompt_never_contains_a_raw_lat_lon_or_fix_prism_field():
    prompt = build_prompt(ALL_REASON_CODES, SCORE).lower()
    for forbidden in ("lat", "lon", "fix", "prism", "trajectory", "coordinate"):
        assert forbidden not in prompt
    # no coordinate-pair shape anywhere (e.g. "43.0721, -70.7626")
    assert not re.search(r"-?\d{1,3}\.\d{3,}", prompt.replace(f"{SCORE:.3f}", ""))


@pytest.mark.parametrize(
    "leaky_reason",
    [
        "43.0721,-70.7626",  # a raw coordinate pair
        "Fix(lat=43.07, lon=-70.76)",  # a serialized Fix field
        "prism_reachable_area",  # Prism vocabulary in a code-shaped string
        "fix_sequence",  # Fix vocabulary in a code-shaped string
        "off corridor",  # free text, not a code
        "OFF_CORRIDOR",  # wrong case: codes are lowercase snake
        "{'lat': 43.07}",  # a serialized dict
        "",  # empty string
    ],
)
def test_prompt_construction_rejects_anything_that_is_not_a_reason_code(leaky_reason):
    with pytest.raises(ValueError):
        build_prompt([leaky_reason], SCORE)


@pytest.mark.parametrize(
    "code_shaped_leak",
    [
        "latitude_spoof",
        "longitude_spoof",
        "fix_sequence",
        "prism_reachable_area",
        "trajectory_gap",
        "coordinate_hint",
    ],
)
def test_every_forbidden_vocabulary_token_is_rejected(code_shaped_leak):
    with pytest.raises(ValueError, match="forbidden vocabulary"):
        build_prompt([code_shaped_leak], SCORE)


def test_prompt_construction_rejects_an_empty_reason_list_and_bad_scores():
    with pytest.raises(ValueError):
        build_prompt([], SCORE)
    for bad_score in (-0.1, 1.1, float("nan")):
        with pytest.raises(ValueError):
            build_prompt(["off_corridor"], bad_score)


def test_every_real_reason_code_is_accepted():
    prompt = build_prompt(ALL_REASON_CODES, 0.0)
    assert "implausible_speed, abnormal_gap, off_corridor" in prompt


@pytest.mark.parametrize("boundary_score", [0.0, 1.0])
def test_score_interval_is_inclusive_at_both_boundaries(boundary_score):
    prompt = build_prompt(["off_corridor"], boundary_score)
    assert f"{boundary_score:.3f}" in prompt


# ---- disabled by default ----------------------------------------------------


def test_disabled_when_no_client_injected():
    assert _explainer(None).enabled is False
    assert _explainer(None).explain(["off_corridor"], SCORE) is None


def test_disabled_when_model_id_empty_even_with_a_client():
    explainer = _explainer(FakeBedrock(), model_id="")
    assert explainer.enabled is False
    assert explainer.explain(["off_corridor"], SCORE) is None


def test_from_settings_default_is_disabled_and_explain_returns_none_without_raising():
    # the gate 5.6 smoke criterion: no AWS profile, no Bedrock access, no
    # model id configured -> disabled, explain() is None, nothing raises
    explainer = BedrockExplainer.from_settings(Settings())
    assert explainer.enabled is False
    assert explainer.explain(["off_corridor", "watchlist_hit"], SCORE) is None


def test_from_settings_degrades_to_disabled_on_construction_failure(monkeypatch):
    import boto3

    def boom(*args: Any, **kwargs: Any):
        raise RuntimeError("no credentials, no region, nothing")

    monkeypatch.setattr(boto3, "client", boom)
    explainer = BedrockExplainer.from_settings(Settings(bedrock_model_id="anthropic.claude-3"))
    assert explainer.enabled is False
    assert explainer.explain(["off_corridor"], SCORE) is None


# ---- happy path against the injected fake -----------------------------------


def test_happy_path_returns_the_narrative_and_sends_only_the_prompt():
    fake = FakeBedrock(text="  These codes together indicate a dark rendezvous pattern.  ")
    explainer = _explainer(fake)
    narrative = explainer.explain(["abnormal_gap", "watchlist_hit"], SCORE)
    assert narrative == "These codes together indicate a dark rendezvous pattern."
    (call,) = fake.calls
    assert call["modelId"] == "anthropic.claude-3-haiku"
    sent = call["messages"][0]["content"][0]["text"]
    assert sent == build_prompt(["abnormal_gap", "watchlist_hit"], SCORE)


async def test_aexplain_matches_explain_and_is_zero_cost_when_disabled():
    fake = FakeBedrock(text="One paragraph.")
    assert await _explainer(fake).aexplain(["off_corridor"], SCORE) == "One paragraph."
    assert await _explainer(None).aexplain(["off_corridor"], SCORE) is None


# ---- fail-open on every failure shape ----------------------------------------


def test_a_bedrock_outage_fails_open_to_none():
    fake = FakeBedrock(err=ConnectionError("bedrock is down"))
    assert _explainer(fake).explain(["off_corridor"], SCORE) is None


def test_a_malformed_response_fails_open_to_none():
    class WeirdBedrock:
        def converse(self, **kwargs: Any) -> dict:
            return {"output": {}}

    assert _explainer(WeirdBedrock()).explain(["off_corridor"], SCORE) is None


def test_an_empty_narrative_fails_open_to_none():
    assert _explainer(FakeBedrock(text="   ")).explain(["off_corridor"], SCORE) is None


def test_a_leaky_caller_gets_none_not_a_leaked_prompt():
    # bad input is treated like any other failure: fail-open, and the client
    # is NEVER invoked, so nothing non-code-shaped can reach Bedrock
    fake = FakeBedrock()
    assert _explainer(fake).explain(["Fix(lat=43.07)"], SCORE) is None
    assert fake.calls == []
