"""Dependency-light tests for the Harbormaster teardown Lambda.

These tests run WITHOUT AWS credentials and WITHOUT the real boto3 SDK calls by
monkeypatching boto3.client with a fake factory that returns canned responses.
They exercise the DRY_RUN path end to end and assert that:
  - no mutating API call is made while DRY_RUN is true,
  - only resources tagged Project=harbormaster are selected,
  - a partial failure in one service does not abort the whole run,
  - the handler returns a JSON-serializable result.

Run with either:
    python -m pytest infra/lambda/teardown/test_handler.py
    python infra/lambda/teardown/test_handler.py     (built-in runner fallback)

The built-in fallback at the bottom means the file passes even where pytest is
not installed.
"""

import importlib
import json
import os
import sys

# Ensure the handler module is importable when tests run from any cwd.
HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)


# --------------------------------------------------------------------------- #
# Fake boto3 clients
# --------------------------------------------------------------------------- #
TAGGED = [
    {"Key": "Project", "Value": "harbormaster"},
    {"Key": "Environment", "Value": "base"},
]
UNTAGGED = [{"Key": "Project", "Value": "something-else"}]
FRONTDOOR_TAGGED = TAGGED + [{"Key": "Module", "Value": "eks_frontdoor"}]
NETWORK_TAGGED = TAGGED + [{"Key": "Module", "Value": "network"}]
WRONG_ENV_FRONTDOOR_TAGGED = [
    {"Key": "Project", "Value": "harbormaster"},
    {"Key": "Environment", "Value": "demo"},
    {"Key": "Module", "Value": "eks_frontdoor"},
]
WRONG_MODULE_TAGGED = TAGGED + [{"Key": "Module", "Value": "not-the-owner"}]


class _Recorder:
    """Records mutating calls so tests can assert they did NOT happen in
    DRY_RUN mode."""

    def __init__(self):
        self.calls = []

    def note(self, name, **kwargs):
        self.calls.append((name, kwargs))


class FakeFlinkClient:
    def __init__(self, recorder):
        self._rec = recorder

    def list_applications(self, **kwargs):
        return {
            "ApplicationSummaries": [
                {
                    "ApplicationName": "harbormaster-detector",
                    "ApplicationStatus": "RUNNING",
                    "ApplicationARN": "arn:aws:kinesisanalytics:::app/hb",
                },
                {
                    "ApplicationName": "other-app",
                    "ApplicationStatus": "RUNNING",
                    "ApplicationARN": "arn:aws:kinesisanalytics:::app/other",
                },
            ]
        }

    def list_tags_for_resource(self, ResourceARN):
        if ResourceARN.endswith("/hb"):
            return {"Tags": TAGGED}
        return {"Tags": UNTAGGED}

    def stop_application(self, **kwargs):
        self._rec.note("flink.stop_application", **kwargs)


class FakeEmrClient:
    def __init__(self, recorder):
        self._rec = recorder

    def list_clusters(self, **kwargs):
        return {
            "Clusters": [
                {"Id": "j-HB", "Name": "harbormaster-emr"},
                {"Id": "j-OTHER", "Name": "other-emr"},
            ]
        }

    def describe_cluster(self, ClusterId):
        if ClusterId == "j-HB":
            return {"Cluster": {"Tags": TAGGED}}
        return {"Cluster": {"Tags": UNTAGGED}}

    def terminate_job_flows(self, **kwargs):
        self._rec.note("emr.terminate_job_flows", **kwargs)


class FakeMskClient:
    def __init__(self, recorder):
        self._rec = recorder

    def list_clusters_v2(self, **kwargs):
        return {
            "ClusterInfoList": [
                {
                    "ClusterArn": "arn:aws:kafka:::cluster/hb",
                    "ClusterName": "harbormaster-msk",
                    "ClusterType": "SERVERLESS",
                    "Tags": {"Project": "harbormaster"},
                },
                {
                    "ClusterArn": "arn:aws:kafka:::cluster/other",
                    "ClusterName": "other-msk",
                    "ClusterType": "SERVERLESS",
                    "Tags": {"Project": "nope"},
                },
            ]
        }

    def delete_cluster(self, **kwargs):
        self._rec.note("msk.delete_cluster", **kwargs)


class _Paginator:
    def __init__(self, pages):
        self._pages = pages

    def paginate(self, **kwargs):
        yield from self._pages


class FakeElbv2Client:
    def __init__(self, recorder):
        self._rec = recorder

    def get_paginator(self, name):
        assert name == "describe_load_balancers"
        return _Paginator(
            [
                {
                    "LoadBalancers": [
                        {
                            "LoadBalancerArn": "arn:aws:elasticloadbalancing:::loadbalancer/net/hb",
                            "LoadBalancerName": "harbormaster-eks",
                            "Type": "network",
                        },
                        {
                            "LoadBalancerArn": (
                                "arn:aws:elasticloadbalancing:::loadbalancer/net/other"
                            ),
                            "LoadBalancerName": "other-nlb",
                            "Type": "network",
                        },
                        {
                            "LoadBalancerArn": "arn:aws:elasticloadbalancing:::loadbalancer/app/hb",
                            "LoadBalancerName": "harbormaster-alb",
                            "Type": "application",
                        },
                    ]
                },
                {
                    "LoadBalancers": [
                        {
                            "LoadBalancerArn": (
                                "arn:aws:elasticloadbalancing:::loadbalancer/net/wrong-module"
                            ),
                            "LoadBalancerName": "harbormaster-wrong-module",
                            "Type": "network",
                        },
                        {
                            "LoadBalancerArn": (
                                "arn:aws:elasticloadbalancing:::loadbalancer/net/wrong-env"
                            ),
                            "LoadBalancerName": "harbormaster-wrong-env",
                            "Type": "network",
                        },
                    ]
                },
            ]
        )

    def describe_tags(self, ResourceArns):
        arn = ResourceArns[0]
        if arn.endswith("/hb"):
            tags = FRONTDOOR_TAGGED
        elif arn.endswith("/wrong-module"):
            tags = WRONG_MODULE_TAGGED
        elif arn.endswith("/wrong-env"):
            tags = WRONG_ENV_FRONTDOOR_TAGGED
        else:
            tags = UNTAGGED
        return {"TagDescriptions": [{"ResourceArn": arn, "Tags": tags}]}

    def delete_load_balancer(self, **kwargs):
        self._rec.note("elbv2.delete_load_balancer", **kwargs)


class FakeEc2Client:
    def __init__(self, recorder):
        self._rec = recorder

    def describe_nat_gateways(self, **kwargs):
        assert kwargs["Filter"] == [{"Name": "tag:Project", "Values": ["harbormaster"]}]
        if kwargs.get("NextToken") == "nat-page-2":
            return {
                "NatGateways": [
                    {
                        "NatGatewayId": "nat-wrong-module",
                        "State": "available",
                        "Tags": WRONG_MODULE_TAGGED,
                    }
                ]
            }
        assert "NextToken" not in kwargs
        return {
            "NatGateways": [
                {
                    "NatGatewayId": "nat-hb",
                    "State": "available",
                    "Tags": NETWORK_TAGGED,
                },
                {
                    "NatGatewayId": "nat-wrong-env",
                    "State": "available",
                    "Tags": [
                        {"Key": "Project", "Value": "harbormaster"},
                        {"Key": "Environment", "Value": "demo"},
                        {"Key": "Module", "Value": "network"},
                    ],
                },
            ],
            "NextToken": "nat-page-2",
        }

    def delete_nat_gateway(self, **kwargs):
        self._rec.note("ec2.delete_nat_gateway", **kwargs)

    def describe_addresses(self, **kwargs):
        assert kwargs == {"Filters": [{"Name": "tag:Project", "Values": ["harbormaster"]}]}
        return {
            "Addresses": [
                {"AllocationId": "eipalloc-hb", "Tags": NETWORK_TAGGED},
                {
                    "AllocationId": "eipalloc-association",
                    "AssociationId": "eipassoc-hb",
                    "Tags": NETWORK_TAGGED,
                },
                {
                    "AllocationId": "eipalloc-eni",
                    "NetworkInterfaceId": "eni-hb",
                    "Tags": NETWORK_TAGGED,
                },
                {
                    "AllocationId": "eipalloc-instance",
                    "InstanceId": "i-hb",
                    "Tags": NETWORK_TAGGED,
                },
                {
                    "AllocationId": "eipalloc-private-ip",
                    "PrivateIpAddress": "10.0.0.2",
                    "Tags": NETWORK_TAGGED,
                },
                {
                    "AllocationId": "eipalloc-wrong-env",
                    "Tags": [
                        {"Key": "Project", "Value": "harbormaster"},
                        {"Key": "Environment", "Value": "demo"},
                        {"Key": "Module", "Value": "network"},
                    ],
                },
                {
                    "AllocationId": "eipalloc-wrong-module",
                    "Tags": WRONG_MODULE_TAGGED,
                },
            ]
        }

    def release_address(self, **kwargs):
        self._rec.note("ec2.release_address", **kwargs)


class FakeClock:
    def __init__(self):
        self.now = 0.0
        self.sleeps = []

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


class FakeNatDetachEc2Client:
    """Expose one exact-scoped NAT EIP through a controlled detach sequence."""

    ALLOCATION_ID = "eipalloc-nat"

    def __init__(
        self,
        recorder,
        detach_after_direct_checks=3,
        scope_mismatch_after_direct_check=False,
    ):
        self._rec = recorder
        self.detach_after_direct_checks = detach_after_direct_checks
        self.scope_mismatch_after_direct_check = scope_mismatch_after_direct_check
        self.direct_checks = 0

    def describe_nat_gateways(self, **kwargs):
        assert kwargs == {"Filter": [{"Name": "tag:Project", "Values": ["harbormaster"]}]}
        return {
            "NatGateways": [
                {
                    "NatGatewayId": "nat-hb",
                    "State": "available",
                    "Tags": NETWORK_TAGGED,
                    "NatGatewayAddresses": [{"AllocationId": self.ALLOCATION_ID}],
                }
            ]
        }

    def delete_nat_gateway(self, **kwargs):
        self._rec.note("ec2.delete_nat_gateway", **kwargs)

    def describe_addresses(self, **kwargs):
        assert kwargs == {"Filters": [{"Name": "tag:Project", "Values": ["harbormaster"]}]}
        self.direct_checks += 1
        if self.scope_mismatch_after_direct_check and self.direct_checks > 1:
            return {"Addresses": [self._address(attached=True, tags=WRONG_MODULE_TAGGED)]}
        attached = (
            self.detach_after_direct_checks is None
            or self.direct_checks < self.detach_after_direct_checks
        )
        return {"Addresses": [self._address(attached=attached)]}

    def release_address(self, **kwargs):
        self._rec.note("ec2.release_address", **kwargs)

    def _address(self, attached, tags=NETWORK_TAGGED):
        address = {"AllocationId": self.ALLOCATION_ID, "Tags": tags}
        if attached:
            address.update(
                {
                    "AssociationId": "eipassoc-nat",
                    "NetworkInterfaceId": "eni-nat",
                    "PrivateIpAddress": "10.0.0.9",
                }
            )
        return address


class FakeAsgClient:
    def __init__(self, recorder):
        self._rec = recorder

    def get_paginator(self, name):
        return _Paginator(
            [
                {
                    "AutoScalingGroups": [
                        {
                            "AutoScalingGroupName": "harbormaster-asg",
                            "Tags": TAGGED,
                            "DesiredCapacity": 3,
                            "MinSize": 1,
                        },
                        {
                            "AutoScalingGroupName": "other-asg",
                            "Tags": UNTAGGED,
                            "DesiredCapacity": 5,
                            "MinSize": 2,
                        },
                    ]
                }
            ]
        )

    def update_auto_scaling_group(self, **kwargs):
        self._rec.note("asg.update_auto_scaling_group", **kwargs)


class FakeCeClient:
    def __init__(self, recorder):
        self._rec = recorder

    def get_cost_and_usage(self, **kwargs):
        return {"ResultsByTime": [{"Total": {"UnblendedCost": {"Amount": "12.34", "Unit": "USD"}}}]}


class FakeSnsClient:
    def __init__(self, recorder):
        self._rec = recorder

    def publish(self, **kwargs):
        self._rec.note("sns.publish", **kwargs)


def make_fake_boto3_client(recorder, failing_service=None):
    """Return a function that mimics boto3.client(service_name) and dispatches
    to the right fake. If failing_service is set, that service raises to test
    the defensive per-service error handling."""

    def _factory(service_name, **kwargs):
        if service_name == failing_service:
            raise RuntimeError(f"simulated {service_name} outage")
        if service_name == "kinesisanalyticsv2":
            return FakeFlinkClient(recorder)
        if service_name == "emr":
            return FakeEmrClient(recorder)
        if service_name == "kafka":
            return FakeMskClient(recorder)
        if service_name == "autoscaling":
            return FakeAsgClient(recorder)
        if service_name == "elbv2":
            return FakeElbv2Client(recorder)
        if service_name == "ec2":
            return FakeEc2Client(recorder)
        if service_name == "ce":
            return FakeCeClient(recorder)
        if service_name == "sns":
            return FakeSnsClient(recorder)
        raise AssertionError(f"unexpected service: {service_name}")

    return _factory


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
class _Boto3Stub:
    """Stand-in for the boto3 module so tests run even where boto3 is not
    installed. Tests overwrite .client with a fake factory."""

    client = None


def _load_handler(monkeyenv):
    """Import (or reimport) the handler module with the given environment so the
    module-level PROJECT_TAG_VALUE picks up overrides. Substitute a private
    stub object whose .client the test replaces, leaving global boto3 intact."""
    for k, v in monkeyenv.items():
        os.environ[k] = v
    if "handler" in sys.modules:
        handler = importlib.reload(sys.modules["handler"])
    else:
        handler = importlib.import_module("handler")
    # Always isolate the handler from the process-wide boto3 module. Replacing
    # boto3.client directly would leak the fake into unrelated tests.
    handler.boto3 = _Boto3Stub()
    return handler


# --------------------------------------------------------------------------- #
# Tests
# --------------------------------------------------------------------------- #
def test_dry_run_makes_no_mutating_calls():
    os.environ["DRY_RUN"] = "true"
    os.environ.pop("ALERT_TOPIC_ARN", None)
    handler = _load_handler({"PROJECT_TAG": "harbormaster", "ENVIRONMENT": "base"})
    recorder = _Recorder()
    handler.boto3.client = make_fake_boto3_client(recorder)

    result = handler.lambda_handler({"source": "test"}, None)

    # DRY_RUN must not perform any mutation or publish.
    assert recorder.calls == [], recorder.calls
    assert result["dry_run"] is True
    # The result must be JSON-serializable.
    json.dumps(result, default=str)


def test_only_tagged_resources_are_selected():
    os.environ["DRY_RUN"] = "true"
    handler = _load_handler({"PROJECT_TAG": "harbormaster", "ENVIRONMENT": "base"})
    recorder = _Recorder()
    handler.boto3.client = make_fake_boto3_client(recorder)

    result = handler.lambda_handler({}, None)
    res = result["results"]

    assert res["managed_flink"]["stopped"] == ["harbormaster-detector"]
    assert res["emr"]["terminated"] == ["j-HB"]
    assert res["msk_serverless"]["deleted"] == ["harbormaster-msk"]
    assert res["auto_scaling"]["zeroed"] == ["harbormaster-asg"]
    assert res["network_load_balancers"]["would_delete"] == ["harbormaster-eks"]
    assert res["nat_gateways"]["would_delete"] == ["nat-hb"]
    assert res["elastic_ips"]["would_release"] == ["eipalloc-hb"]
    assert res["network_load_balancers"]["delete_requested"] == []
    assert res["nat_gateways"]["delete_requested"] == []
    assert res["elastic_ips"]["release_requested"] == []
    assert res["cost_explorer"]["summary"]["amount"] == 12.34


def test_per_service_failure_does_not_abort_run():
    os.environ["DRY_RUN"] = "true"
    handler = _load_handler({"PROJECT_TAG": "harbormaster", "ENVIRONMENT": "base"})
    recorder = _Recorder()
    # EMR blows up; everything else must still complete.
    handler.boto3.client = make_fake_boto3_client(recorder, failing_service="emr")

    result = handler.lambda_handler({}, None)
    res = result["results"]

    assert res["emr"]["error"] is not None
    assert "simulated emr outage" in res["emr"]["error"]
    # Other services unaffected.
    assert res["managed_flink"]["stopped"] == ["harbormaster-detector"]
    assert res["auto_scaling"]["zeroed"] == ["harbormaster-asg"]
    assert res["cost_explorer"]["summary"]["amount"] == 12.34


def test_network_failure_does_not_abort_later_cleanup_blocks():
    os.environ["DRY_RUN"] = "true"
    handler = _load_handler({"PROJECT_TAG": "harbormaster", "ENVIRONMENT": "base"})
    recorder = _Recorder()
    handler.boto3.client = make_fake_boto3_client(recorder, failing_service="elbv2")

    result = handler.lambda_handler({}, None)
    res = result["results"]

    assert "simulated elbv2 outage" in res["network_load_balancers"]["error"]
    assert res["nat_gateways"]["would_delete"] == ["nat-hb"]
    assert res["elastic_ips"]["would_release"] == ["eipalloc-hb"]
    assert res["cost_explorer"]["summary"]["amount"] == 12.34


def test_wet_run_performs_actions_and_publishes():
    os.environ["DRY_RUN"] = "false"
    os.environ["ALERT_TOPIC_ARN"] = "arn:aws:sns:us-east-1:000000000000:hb-alerts"
    handler = _load_handler({"PROJECT_TAG": "harbormaster", "ENVIRONMENT": "base"})
    recorder = _Recorder()
    handler.boto3.client = make_fake_boto3_client(recorder)

    result = handler.lambda_handler({}, None)

    names = [c[0] for c in recorder.calls]
    assert "flink.stop_application" in names
    assert "emr.terminate_job_flows" in names
    assert "msk.delete_cluster" in names
    assert "asg.update_auto_scaling_group" in names
    assert "elbv2.delete_load_balancer" in names
    assert "ec2.delete_nat_gateway" in names
    assert "ec2.release_address" in names
    assert "sns.publish" in names
    assert result["dry_run"] is False
    assert result["results"]["network_load_balancers"]["delete_requested"] == ["harbormaster-eks"]
    assert result["results"]["nat_gateways"]["delete_requested"] == ["nat-hb"]
    assert result["results"]["elastic_ips"]["release_requested"] == ["eipalloc-hb"]
    # Reset for any subsequent runners.
    os.environ["DRY_RUN"] = "true"
    os.environ.pop("ALERT_TOPIC_ARN", None)


def _load_handler_with_nat_detach_client(
    detach_after_direct_checks=3,
    scope_mismatch_after_direct_check=False,
):
    handler = _load_handler({"PROJECT_TAG": "harbormaster", "ENVIRONMENT": "base"})
    recorder = _Recorder()
    client = FakeNatDetachEc2Client(
        recorder,
        detach_after_direct_checks=detach_after_direct_checks,
        scope_mismatch_after_direct_check=scope_mismatch_after_direct_check,
    )
    handler.boto3.client = lambda service_name, **kwargs: client
    return handler, recorder, client


def test_nat_eip_released_after_later_bounded_detach_pass():
    handler, recorder, client = _load_handler_with_nat_detach_client()
    clock = FakeClock()
    results = {}

    handler.delete_nat_gateways(False, results)
    candidates = results["nat_gateways"]["delete_requested_allocation_ids"]
    handler.release_unattached_elastic_ips(
        False,
        results,
        convergence_allocation_ids=candidates,
        convergence_timeout_seconds=5,
        convergence_poll_seconds=1,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    )

    assert candidates == [client.ALLOCATION_ID]
    assert [name for name, _ in recorder.calls] == [
        "ec2.delete_nat_gateway",
        "ec2.release_address",
    ]
    assert clock.sleeps == [1, 1]
    assert client.direct_checks == 3
    assert results["elastic_ips"]["converged_after_wait"] == [client.ALLOCATION_ID]
    assert results["elastic_ips"]["convergence_timed_out"] == []
    assert results["elastic_ips"]["error"] is None


def test_nat_eip_timeout_never_releases_still_attached_address():
    handler, recorder, client = _load_handler_with_nat_detach_client(
        detach_after_direct_checks=None
    )
    clock = FakeClock()
    results = {}
    events = []
    handler._log = lambda event, **fields: events.append((event, fields))

    handler.delete_nat_gateways(False, results)
    handler.release_unattached_elastic_ips(
        False,
        results,
        convergence_allocation_ids=results["nat_gateways"]["delete_requested_allocation_ids"],
        convergence_timeout_seconds=3,
        convergence_poll_seconds=1,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    )

    assert [name for name, _ in recorder.calls] == ["ec2.delete_nat_gateway"]
    assert clock.sleeps == [1, 1, 1]
    assert results["elastic_ips"]["convergence_timed_out"] == [client.ALLOCATION_ID]
    assert "timed out waiting" in results["elastic_ips"]["error"]
    failure_events = [(event, fields) for event, fields in events if event.endswith("_failed")]
    assert failure_events == [
        (
            "elastic_ip_convergence_failed",
            {
                "error": ("timed out waiting for NAT Elastic IP detach: " + client.ALLOCATION_ID),
                "not_visible": [],
                "scope_mismatch": [],
                "timed_out": [client.ALLOCATION_ID],
                "timeout_seconds": 3.0,
            },
        )
    ]


def test_nat_eip_scope_change_fails_closed_without_release():
    handler, recorder, client = _load_handler_with_nat_detach_client(
        scope_mismatch_after_direct_check=True
    )
    clock = FakeClock()
    results = {}

    handler.delete_nat_gateways(False, results)
    handler.release_unattached_elastic_ips(
        False,
        results,
        convergence_allocation_ids=results["nat_gateways"]["delete_requested_allocation_ids"],
        convergence_timeout_seconds=3,
        convergence_poll_seconds=1,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    )

    assert [name for name, _ in recorder.calls] == ["ec2.delete_nat_gateway"]
    assert clock.sleeps == [1]
    assert results["elastic_ips"]["convergence_scope_mismatch"] == [client.ALLOCATION_ID]
    assert "scope mismatch" in results["elastic_ips"]["error"]


def test_nat_eip_dry_run_never_polls_or_mutates():
    handler, recorder, client = _load_handler_with_nat_detach_client()
    clock = FakeClock()
    results = {}

    handler.delete_nat_gateways(True, results)
    candidates = results["nat_gateways"]["would_detach_allocation_ids"]
    handler.release_unattached_elastic_ips(
        True,
        results,
        convergence_allocation_ids=candidates,
        convergence_timeout_seconds=5,
        convergence_poll_seconds=1,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    )

    assert candidates == [client.ALLOCATION_ID]
    assert recorder.calls == []
    assert clock.sleeps == []
    assert client.direct_checks == 1
    assert results["elastic_ips"]["release_requested"] == []


def test_lambda_handler_wires_dry_run_nat_eip_candidates_end_to_end():
    os.environ["DRY_RUN"] = "true"
    os.environ.pop("ALERT_TOPIC_ARN", None)
    handler = _load_handler({"PROJECT_TAG": "harbormaster", "ENVIRONMENT": "base"})
    recorder = _Recorder()
    ec2 = FakeNatDetachEc2Client(recorder)
    fallback = make_fake_boto3_client(recorder)

    def _factory(service_name, **kwargs):
        if service_name == "ec2":
            return ec2
        return fallback(service_name, **kwargs)

    handler.boto3.client = _factory
    result = handler.lambda_handler({"source": "dry-run-regression"}, None)

    assert recorder.calls == []
    assert result["results"]["nat_gateways"]["would_detach_allocation_ids"] == [ec2.ALLOCATION_ID]
    assert result["results"]["elastic_ips"]["convergence_targets"] == [ec2.ALLOCATION_ID]
    assert ec2.direct_checks == 1


def test_initial_eip_inventory_latency_consumes_convergence_budget():
    handler, recorder, client = _load_handler_with_nat_detach_client(
        detach_after_direct_checks=None
    )
    clock = FakeClock()
    results = {}
    original_describe_addresses = client.describe_addresses

    def _slow_initial_inventory(**kwargs):
        if client.direct_checks == 0:
            clock.now += 4
        return original_describe_addresses(**kwargs)

    client.describe_addresses = _slow_initial_inventory
    handler.delete_nat_gateways(False, results)
    handler.release_unattached_elastic_ips(
        False,
        results,
        convergence_allocation_ids=results["nat_gateways"]["delete_requested_allocation_ids"],
        convergence_timeout_seconds=3,
        convergence_poll_seconds=1,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    )

    assert [name for name, _ in recorder.calls] == ["ec2.delete_nat_gateway"]
    assert client.direct_checks == 1
    assert clock.sleeps == []
    assert results["elastic_ips"]["convergence_timed_out"] == [client.ALLOCATION_ID]


def test_nat_eip_convergence_budget_preserves_lambda_completion_reserve():
    handler = _load_handler({"PROJECT_TAG": "harbormaster", "ENVIRONMENT": "base"})

    class _Context:
        def __init__(self, remaining_millis):
            self.remaining_millis = remaining_millis

        def get_remaining_time_in_millis(self):
            return self.remaining_millis

    assert handler._eip_convergence_timeout(_Context(120_000)) == 90
    assert handler._eip_convergence_timeout(_Context(20_000)) == 5
    assert handler._eip_convergence_timeout(_Context(10_000)) == 0
    assert handler._eip_convergence_timeout(object()) == 0
    assert handler._eip_convergence_timeout(_Context("invalid")) == 0
    os.environ.pop("HM_TEST_UNSET_BOOL", None)
    assert handler._env_bool("HM_TEST_UNSET_BOOL", False) is False
    assert handler._tag_matches([]) is False
    assert handler._tag_matches(["malformed", {"key": "Project", "value": "harbormaster"}]) is True


def test_nat_cleanup_surfaces_api_failure_and_skips_non_active_state():
    handler = _load_handler({"PROJECT_TAG": "harbormaster", "ENVIRONMENT": "base"})

    class _DeletingNatClient:
        def describe_nat_gateways(self, **kwargs):
            return {
                "NatGateways": [
                    {
                        "NatGatewayId": "nat-deleting",
                        "State": "deleting",
                        "Tags": NETWORK_TAGGED,
                    }
                ]
            }

    handler.boto3.client = lambda service_name, **kwargs: _DeletingNatClient()
    results = {}
    handler.delete_nat_gateways(False, results)
    assert results["nat_gateways"]["delete_requested"] == []
    assert results["nat_gateways"]["error"] is None

    class _FailedNatClient:
        def describe_nat_gateways(self, **kwargs):
            raise RuntimeError("describe NAT failed")

    handler.boto3.client = lambda service_name, **kwargs: _FailedNatClient()
    results = {}
    handler.delete_nat_gateways(False, results)
    assert results["nat_gateways"]["delete_requested_allocation_ids"] == []
    assert results["nat_gateways"]["error"] == "describe NAT failed"


def test_nat_eip_candidates_cover_absence_scope_and_immediate_detach_paths():
    handler = _load_handler({"PROJECT_TAG": "harbormaster", "ENVIRONMENT": "base"})
    recorder = _Recorder()
    clock = FakeClock()

    class _MixedEipClient:
        def __init__(self):
            self.calls = 0

        def describe_addresses(self, **kwargs):
            self.calls += 1
            if self.calls == 1:
                return {
                    "Addresses": [
                        {"Tags": NETWORK_TAGGED},
                        {"AllocationId": "eipalloc-wrong-filter", "Tags": WRONG_MODULE_TAGGED},
                        {"AllocationId": "eipalloc-detached-filter", "Tags": NETWORK_TAGGED},
                        {
                            "AllocationId": "eipalloc-attached-then-missing",
                            "AssociationId": "eipassoc-pending",
                            "Tags": NETWORK_TAGGED,
                        },
                    ]
                }
            return {"Addresses": []}

        def release_address(self, **kwargs):
            recorder.note("ec2.release_address", **kwargs)

    client = _MixedEipClient()
    handler.boto3.client = lambda service_name, **kwargs: client
    targets = [
        "eipalloc-attached-then-missing",
        "eipalloc-detached-filter",
        "eipalloc-missing",
        "eipalloc-wrong-filter",
    ]
    results = {}

    handler.release_unattached_elastic_ips(
        False,
        results,
        convergence_allocation_ids=targets,
        convergence_timeout_seconds=2,
        convergence_poll_seconds=1,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    )

    released = [kwargs["AllocationId"] for _, kwargs in recorder.calls]
    assert released == ["eipalloc-detached-filter"]
    assert results["elastic_ips"]["convergence_not_visible"] == [
        "eipalloc-attached-then-missing",
        "eipalloc-missing",
    ]
    assert results["elastic_ips"]["convergence_scope_mismatch"] == [
        "eipalloc-wrong-filter",
    ]
    assert results["elastic_ips"]["convergence_timed_out"] == []
    assert clock.sleeps == [1]


def test_elastic_ip_api_failure_is_accumulated_without_raise():
    handler = _load_handler({"PROJECT_TAG": "harbormaster", "ENVIRONMENT": "base"})

    class _FailedEipClient:
        def describe_addresses(self, **kwargs):
            raise RuntimeError("describe EIP failed")

    handler.boto3.client = lambda service_name, **kwargs: _FailedEipClient()
    results = {}
    handler.release_unattached_elastic_ips(False, results)

    assert results["elastic_ips"]["release_requested"] == []
    assert results["elastic_ips"]["convergence_timed_out"] == []
    assert results["elastic_ips"]["error"] == "describe EIP failed"


# --------------------------------------------------------------------------- #
# Built-in runner fallback (works without pytest installed).
# --------------------------------------------------------------------------- #
def _run_all():
    tests = [
        test_dry_run_makes_no_mutating_calls,
        test_only_tagged_resources_are_selected,
        test_per_service_failure_does_not_abort_run,
        test_network_failure_does_not_abort_later_cleanup_blocks,
        test_wet_run_performs_actions_and_publishes,
        test_nat_eip_released_after_later_bounded_detach_pass,
        test_nat_eip_timeout_never_releases_still_attached_address,
        test_nat_eip_scope_change_fails_closed_without_release,
        test_nat_eip_dry_run_never_polls_or_mutates,
        test_lambda_handler_wires_dry_run_nat_eip_candidates_end_to_end,
        test_initial_eip_inventory_latency_consumes_convergence_budget,
        test_nat_eip_convergence_budget_preserves_lambda_completion_reserve,
        test_nat_cleanup_surfaces_api_failure_and_skips_non_active_state,
        test_nat_eip_candidates_cover_absence_scope_and_immediate_detach_paths,
        test_elastic_ip_api_failure_is_accumulated_without_raise,
    ]
    failures = 0
    for t in tests:
        try:
            t()
            print(f"PASS {t.__name__}")
        except AssertionError as e:
            failures += 1
            print(f"FAIL {t.__name__}: {e}")
        except Exception as e:  # noqa: BLE001
            failures += 1
            print(f"ERROR {t.__name__}: {e}")
    if failures:
        print(f"{failures} test(s) failed")
        return 1
    print("all tests passed")
    return 0


if __name__ == "__main__":
    sys.exit(_run_all())
