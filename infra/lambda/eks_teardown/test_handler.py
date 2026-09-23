"""Tests for the EKS teardown guard Lambda (Phase 5, gate 5.0).

Run WITHOUT AWS credentials and WITHOUT real boto3 calls by monkeypatching
handler.boto3 with a fake factory, following infra/lambda/teardown's
test_handler.py convention. Coverage focus, per the gate spec:
  - the pure decision function should_teardown against boundary timestamps,
  - keep-alive tag parsing failing TOWARD teardown on bad input,
  - DRY_RUN making no mutating call,
  - node-groups-first ordering (no DeleteCluster while node groups exist),
  - defensive behavior (describe failure, missing config) never raising.

Run with either:
    python -m pytest infra/lambda/eks_teardown/test_handler.py
    python infra/lambda/eks_teardown/test_handler.py   (built-in runner fallback)
"""

import datetime
import os
import sys

# Ensure the handler module is importable when tests run from any cwd.
HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

import handler  # noqa: E402

UTC = datetime.UTC
CREATED = datetime.datetime(2026, 7, 11, 12, 0, 0, tzinfo=UTC)
FOUR_H = datetime.timedelta(hours=4)
SEC = datetime.timedelta(seconds=1)


# --------------------------------------------------------------------------- #
# should_teardown: boundary timestamps
# --------------------------------------------------------------------------- #
def test_alive_strictly_inside_the_age_window():
    assert handler.should_teardown(CREATED, None, CREATED + FOUR_H - SEC, 4) is False


def test_teardown_exactly_at_the_age_boundary():
    assert handler.should_teardown(CREATED, None, CREATED + FOUR_H, 4) is True


def test_teardown_after_the_age_boundary():
    assert handler.should_teardown(CREATED, None, CREATED + FOUR_H + SEC, 4) is True


def test_keep_alive_in_the_future_holds_the_guard_off():
    keep = CREATED + datetime.timedelta(hours=8)
    assert handler.should_teardown(CREATED, keep, CREATED + FOUR_H + SEC, 4) is False


def test_teardown_exactly_at_the_keep_alive_boundary():
    keep = CREATED + datetime.timedelta(hours=8)
    assert handler.should_teardown(CREATED, keep, keep, 4) is True


def test_keep_alive_already_past_grants_nothing():
    keep = CREATED + datetime.timedelta(hours=2)
    assert handler.should_teardown(CREATED, keep, CREATED + FOUR_H, 4) is True


def test_keep_alive_is_irrelevant_while_age_window_is_open():
    keep = CREATED + datetime.timedelta(hours=1)  # already past at eval time
    assert handler.should_teardown(CREATED, keep, CREATED + datetime.timedelta(hours=2), 4) is False


def test_zero_max_age_tears_down_immediately():
    assert handler.should_teardown(CREATED, None, CREATED, 0) is True


def test_negative_max_age_raises():
    try:
        handler.should_teardown(CREATED, None, CREATED, -1)
    except ValueError:
        pass
    else:
        raise AssertionError("negative max_age_hours must raise ValueError")


def test_naive_datetimes_are_treated_as_utc():
    naive_created = CREATED.replace(tzinfo=None)
    naive_now = (CREATED + FOUR_H).replace(tzinfo=None)
    assert handler.should_teardown(naive_created, None, naive_now, 4) is True


def test_fractional_hours_window():
    just_inside = CREATED + datetime.timedelta(minutes=29)
    at_boundary = CREATED + datetime.timedelta(minutes=30)
    assert handler.should_teardown(CREATED, None, just_inside, 0.5) is False
    assert handler.should_teardown(CREATED, None, at_boundary, 0.5) is True


# --------------------------------------------------------------------------- #
# parse_keep_alive: fails toward teardown
# --------------------------------------------------------------------------- #
def test_parse_keep_alive_z_suffix():
    parsed = handler.parse_keep_alive("2026-07-12T02:00:00Z")
    assert parsed == datetime.datetime(2026, 7, 12, 2, 0, 0, tzinfo=UTC)


def test_parse_keep_alive_explicit_offset_normalized_to_utc():
    parsed = handler.parse_keep_alive("2026-07-11T21:00:00-05:00")
    assert parsed == datetime.datetime(2026, 7, 12, 2, 0, 0, tzinfo=UTC)


def test_parse_keep_alive_naive_assumed_utc():
    parsed = handler.parse_keep_alive("2026-07-12T02:00:00")
    assert parsed == datetime.datetime(2026, 7, 12, 2, 0, 0, tzinfo=UTC)


def test_parse_keep_alive_garbage_gives_no_extension():
    assert handler.parse_keep_alive("next tuesday") is None


def test_parse_keep_alive_empty_and_none_give_no_extension():
    assert handler.parse_keep_alive("") is None
    assert handler.parse_keep_alive("   ") is None
    assert handler.parse_keep_alive(None) is None
    assert handler.parse_keep_alive(1234) is None


# --------------------------------------------------------------------------- #
# Fake EKS client
# --------------------------------------------------------------------------- #
class _Recorder:
    def __init__(self):
        self.calls = []

    def note(self, name, **kwargs):
        self.calls.append((name, kwargs))


class FakeEksClient:
    def __init__(self, recorder, created_at, tags=None, nodegroups=None, describe_error=None):
        self._rec = recorder
        self._created_at = created_at
        self._tags = {"Project": "harbormaster"} if tags is None else tags
        self._nodegroups = list(nodegroups or [])
        self._describe_error = describe_error

    def describe_cluster(self, name):
        if self._describe_error:
            raise self._describe_error
        return {
            "cluster": {
                "name": name,
                "status": "ACTIVE",
                "createdAt": self._created_at,
                "tags": self._tags,
            }
        }

    def list_nodegroups(self, clusterName, **kwargs):
        return {"nodegroups": list(self._nodegroups)}

    def delete_nodegroup(self, clusterName, nodegroupName):
        self._rec.note("eks.delete_nodegroup", cluster=clusterName, nodegroup=nodegroupName)

    def delete_cluster(self, name):
        self._rec.note("eks.delete_cluster", cluster=name)


class FakeSnsClient:
    def __init__(self, recorder):
        self._rec = recorder

    def publish(self, **kwargs):
        self._rec.note("sns.publish", **kwargs)


class FakeBoto3:
    def __init__(self, clients):
        self._clients = clients

    def client(self, service, **kwargs):
        return self._clients[service]


def _run_handler(monkeypatch, eks_client, recorder, env):
    monkeypatch.setattr(
        handler, "boto3", FakeBoto3({"eks": eks_client, "sns": FakeSnsClient(recorder)})
    )
    env_keys = ("DRY_RUN", "CLUSTER_NAME", "MAX_AGE_HOURS", "ALERT_TOPIC_ARN", "KEEP_ALIVE_TAG_KEY")
    for key in env_keys:
        monkeypatch.delenv(key, raising=False)
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    return handler.lambda_handler({"source": "test"}, None)


def _old_enough():
    return datetime.datetime.now(UTC) - datetime.timedelta(hours=5)


def _fresh():
    return datetime.datetime.now(UTC) - datetime.timedelta(minutes=10)


# --------------------------------------------------------------------------- #
# Handler behavior
# --------------------------------------------------------------------------- #
def test_dry_run_decides_teardown_but_mutates_nothing(monkeypatch):
    rec = _Recorder()
    eks = FakeEksClient(rec, _old_enough(), nodegroups=["spot-a"])
    result = _run_handler(
        monkeypatch, eks, rec, {"CLUSTER_NAME": "harbormaster-base-eks", "DRY_RUN": "true"}
    )
    assert result["decision"] == "teardown"
    assert rec.calls == []
    assert result["results"]["eks"]["nodegroups_deleted"] == ["spot-a"]
    assert result["results"]["eks"]["cluster_deleted"] is True  # would-delete, recorded


def test_armed_run_deletes_nodegroups_but_defers_cluster(monkeypatch):
    rec = _Recorder()
    eks = FakeEksClient(rec, _old_enough(), nodegroups=["spot-a", "spot-b"])
    result = _run_handler(
        monkeypatch, eks, rec, {"CLUSTER_NAME": "harbormaster-base-eks", "DRY_RUN": "false"}
    )
    assert result["decision"] == "teardown"
    called = [name for name, _ in rec.calls]
    assert called.count("eks.delete_nodegroup") == 2
    assert "eks.delete_cluster" not in called  # node groups still draining
    assert result["results"]["eks"]["cluster_deleted"] is False


def test_armed_run_deletes_cluster_once_no_nodegroups_remain(monkeypatch):
    rec = _Recorder()
    eks = FakeEksClient(rec, _old_enough(), nodegroups=[])
    result = _run_handler(
        monkeypatch, eks, rec, {"CLUSTER_NAME": "harbormaster-base-eks", "DRY_RUN": "false"}
    )
    assert result["decision"] == "teardown"
    assert [name for name, _ in rec.calls if name.startswith("eks.")] == ["eks.delete_cluster"]
    assert result["results"]["eks"]["cluster_deleted"] is True


def test_fresh_cluster_is_kept(monkeypatch):
    rec = _Recorder()
    eks = FakeEksClient(rec, _fresh(), nodegroups=["spot-a"])
    result = _run_handler(
        monkeypatch, eks, rec, {"CLUSTER_NAME": "harbormaster-base-eks", "DRY_RUN": "false"}
    )
    assert result["decision"] == "keep"
    assert rec.calls == []


def test_keep_alive_tag_holds_off_an_old_cluster(monkeypatch):
    rec = _Recorder()
    keep = (datetime.datetime.now(UTC) + datetime.timedelta(hours=2)).isoformat()
    eks = FakeEksClient(
        rec, _old_enough(), tags={"Project": "harbormaster", "KeepAliveUntil": keep}
    )
    result = _run_handler(
        monkeypatch, eks, rec, {"CLUSTER_NAME": "harbormaster-base-eks", "DRY_RUN": "false"}
    )
    assert result["decision"] == "keep"
    assert rec.calls == []


def test_unparseable_keep_alive_fails_toward_teardown(monkeypatch):
    rec = _Recorder()
    eks = FakeEksClient(
        rec,
        _old_enough(),
        tags={"Project": "harbormaster", "KeepAliveUntil": "not-a-timestamp"},
    )
    result = _run_handler(
        monkeypatch, eks, rec, {"CLUSTER_NAME": "harbormaster-base-eks", "DRY_RUN": "false"}
    )
    assert result["decision"] == "teardown"


def test_untagged_cluster_is_never_touched(monkeypatch):
    rec = _Recorder()
    eks = FakeEksClient(rec, _old_enough(), tags={"Project": "something-else"})
    result = _run_handler(monkeypatch, eks, rec, {"CLUSTER_NAME": "other-eks", "DRY_RUN": "false"})
    assert result["decision"] == "skip_untagged"
    assert rec.calls == []


def test_absent_cluster_is_a_clean_noop(monkeypatch):
    rec = _Recorder()
    eks = FakeEksClient(
        rec, _old_enough(), describe_error=RuntimeError("ResourceNotFoundException")
    )
    result = _run_handler(
        monkeypatch, eks, rec, {"CLUSTER_NAME": "harbormaster-base-eks", "DRY_RUN": "false"}
    )
    assert result["decision"] == "absent"
    assert rec.calls == []


def test_missing_cluster_name_is_misconfigured_not_a_crash(monkeypatch):
    rec = _Recorder()
    eks = FakeEksClient(rec, _old_enough())
    result = _run_handler(monkeypatch, eks, rec, {"DRY_RUN": "false"})
    assert result["decision"] == "misconfigured"
    assert rec.calls == []


def test_unparseable_max_age_falls_back_to_default_window(monkeypatch):
    rec = _Recorder()
    eks = FakeEksClient(rec, _old_enough(), nodegroups=[])
    result = _run_handler(
        monkeypatch,
        eks,
        rec,
        {"CLUSTER_NAME": "harbormaster-base-eks", "DRY_RUN": "true", "MAX_AGE_HOURS": "soon"},
    )
    # 5 hours old > default 4h window: the typo must not disarm the guard.
    assert result["decision"] == "teardown"


def test_summary_published_to_sns_when_armed(monkeypatch):
    rec = _Recorder()
    eks = FakeEksClient(rec, _old_enough(), nodegroups=[])
    result = _run_handler(
        monkeypatch,
        eks,
        rec,
        {
            "CLUSTER_NAME": "harbormaster-base-eks",
            "DRY_RUN": "false",
            "ALERT_TOPIC_ARN": "arn:aws:sns:us-east-1:123456789012:alerts",
        },
    )
    published = [kwargs for name, kwargs in rec.calls if name == "sns.publish"]
    assert len(published) == 1
    assert "teardown" in published[0]["Message"]
    assert result["results"]["sns"]["published"] is True


def test_sns_publish_failure_is_fail_open(monkeypatch):
    class ExplodingSns(FakeSnsClient):
        def publish(self, **kwargs):
            raise RuntimeError("sns down")

    rec = _Recorder()
    eks = FakeEksClient(rec, _old_enough(), nodegroups=[])
    monkeypatch.setattr(handler, "boto3", FakeBoto3({"eks": eks, "sns": ExplodingSns(rec)}))
    monkeypatch.setenv("CLUSTER_NAME", "harbormaster-base-eks")
    monkeypatch.setenv("DRY_RUN", "false")
    monkeypatch.setenv("ALERT_TOPIC_ARN", "arn:aws:sns:us-east-1:123456789012:alerts")
    result = handler.lambda_handler({"source": "test"}, None)
    # The teardown still happened and the run still returned cleanly.
    assert result["results"]["eks"]["cluster_deleted"] is True
    assert result["results"]["sns"] == {"published": False, "error": "sns down"}


def test_sns_dry_run_would_publish_only(monkeypatch):
    rec = _Recorder()
    eks = FakeEksClient(rec, _old_enough(), nodegroups=[])
    result = _run_handler(
        monkeypatch,
        eks,
        rec,
        {
            "CLUSTER_NAME": "harbormaster-base-eks",
            "DRY_RUN": "true",
            "ALERT_TOPIC_ARN": "arn:aws:sns:us-east-1:123456789012:alerts",
        },
    )
    assert [name for name, _ in rec.calls] == []
    assert result["results"]["sns"] == {"published": False, "error": None}


def test_teardown_block_failure_is_accumulated_not_raised(monkeypatch):
    class ExplodingEks(FakeEksClient):
        def list_nodegroups(self, clusterName, **kwargs):
            raise RuntimeError("throttled")

    rec = _Recorder()
    eks = ExplodingEks(rec, _old_enough())
    result = _run_handler(
        monkeypatch, eks, rec, {"CLUSTER_NAME": "harbormaster-base-eks", "DRY_RUN": "false"}
    )
    assert result["decision"] == "teardown"
    assert result["results"]["eks"]["error"] == "throttled"
    assert result["results"]["eks"]["cluster_deleted"] is False


if __name__ == "__main__":
    # Built-in runner fallback so the file passes where pytest is absent.
    import inspect

    class _MonkeyPatch:
        def __init__(self):
            self._undo = []

        def setattr(self, obj, name, value):
            self._undo.append(("attr", obj, name, getattr(obj, name)))
            setattr(obj, name, value)

        def setenv(self, key, value):
            self._undo.append(("env", key, os.environ.get(key)))
            os.environ[key] = value

        def delenv(self, key, raising=True):
            self._undo.append(("env", key, os.environ.get(key)))
            os.environ.pop(key, None)

        def undo(self):
            for entry in reversed(self._undo):
                if entry[0] == "attr":
                    _, obj, name, old = entry
                    setattr(obj, name, old)
                else:
                    _, key, old = entry
                    if old is None:
                        os.environ.pop(key, None)
                    else:
                        os.environ[key] = old

    failures = 0
    for _name, fn in sorted(globals().items()):
        if not (_name.startswith("test_") and callable(fn)):
            continue
        mp = _MonkeyPatch()
        try:
            fn(mp) if "monkeypatch" in inspect.signature(fn).parameters else fn()
            print(f"PASS {_name}")
        except Exception as exc:  # noqa: BLE001
            failures += 1
            print(f"FAIL {_name}: {exc}")
        finally:
            mp.undo()
    sys.exit(1 if failures else 0)
