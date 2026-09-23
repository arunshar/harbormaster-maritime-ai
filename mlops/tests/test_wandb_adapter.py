from __future__ import annotations

import pytest

from mlops.wandb_adapter import WandbAdapter


@pytest.fixture(autouse=True)
def _no_wandb_api_key(monkeypatch):
    # Force the local/structlog fallback path in every test here: no network,
    # no API key needed, matching the adapter test convention of an earlier
    # reinforcement-learning repository of mine.
    monkeypatch.delenv("WANDB_API_KEY", raising=False)


def test_adapter_falls_back_to_local_logging_without_an_api_key():
    adapter = WandbAdapter(project="harbormaster-pidpm")
    assert adapter._run is None


def test_log_does_not_raise_without_a_live_run():
    adapter = WandbAdapter(project="harbormaster-pidpm")
    adapter.log({"reward/total": 1.5}, step=3)  # must not raise, no network


def test_log_lineage_does_not_raise_without_a_live_run():
    adapter = WandbAdapter(project="harbormaster-pidpm")
    adapter.log_lineage(
        run_id="run-1",
        git_sha="abc1234",
        config_hash="cfg-hash",
        data_fingerprint="fp-hash",
        checkpoint_manifest_path="runs/run-1/step_0000005/deadbeefcafefeed.bin",
    )  # must not raise, no network


def test_log_lineage_logs_every_required_field(monkeypatch):
    # structlog's default renderer does not route through stdlib logging, so
    # caplog cannot see it; patch the module's logger directly instead.
    calls = []
    monkeypatch.setattr(
        "mlops.wandb_adapter.log",
        type(
            "_FakeLog", (), {"info": staticmethod(lambda event, **kw: calls.append((event, kw)))}
        )(),
    )
    adapter = WandbAdapter(project="harbormaster-pidpm")
    adapter.log_lineage(
        run_id="run-1",
        git_sha="abc1234",
        config_hash="cfg-hash",
        data_fingerprint="fp-hash",
        checkpoint_manifest_path="runs/run-1/step_0000005/deadbeefcafefeed.bin",
    )
    assert len(calls) == 1
    event, fields = calls[0]
    assert event == "lineage"
    assert fields == {
        "run_id": "run-1",
        "git_sha": "abc1234",
        "config_hash": "cfg-hash",
        "data_fingerprint": "fp-hash",
        "checkpoint_manifest_path": "runs/run-1/step_0000005/deadbeefcafefeed.bin",
    }
