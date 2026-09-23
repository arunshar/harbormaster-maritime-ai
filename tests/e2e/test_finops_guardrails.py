"""Structural tests for the $75 spend freeze and nightly network cleanup."""

from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
FINOPS = REPO / "infra" / "terraform" / "modules" / "finops" / "main.tf"
FINOPS_VARIABLES = FINOPS.with_name("variables.tf")
ROOT_VARIABLES = REPO / "infra" / "terraform" / "envs" / "base" / "variables.tf"


def _spend_freeze_block() -> str:
    text = FINOPS.read_text()
    return text.split('data "aws_iam_policy_document" "spend_freeze"', 1)[1].split(
        'resource "aws_iam_policy" "spend_freeze"', 1
    )[0]


def _statement_for_sid(sid: str) -> str:
    text = FINOPS.read_text()
    marker = f'"{sid}"'
    sid_offset = text.index(marker)
    start = text.rfind("  statement {", 0, sid_offset)
    end = text.find("\n  statement {", sid_offset)
    if end == -1:
        end = len(text)
    return text[start:end]


def test_spend_freeze_blocks_w4_network_and_compute_creation():
    block = _spend_freeze_block()
    required = {
        "ec2:AllocateAddress",
        "ec2:AllocateHosts",
        "ec2:CreateCapacityReservation",
        "ec2:CreateCapacityReservationFleet",
        "ec2:CreateFleet",
        "ec2:CreateNatGateway",
        "ec2:PurchaseHostReservation",
        "ec2:PurchaseReservedInstancesOffering",
        "ec2:RequestSpotFleet",
        "ec2:RequestSpotInstances",
        "eks:CreateCluster",
        "eks:CreateNodegroup",
        "eks:UpdateNodegroupConfig",
        "elasticloadbalancing:CreateLoadBalancer",
        "autoscaling:SetDesiredCapacity",
        "autoscaling:UpdateAutoScalingGroup",
        "kinesisanalytics:StartApplication",
    }
    missing = sorted(action for action in required if f'"{action}"' not in block)
    assert not missing, f"spend freeze is missing W4 cost controls: {missing}"


def test_spend_freeze_preserves_read_and_teardown_paths():
    block = _spend_freeze_block()
    assert '"eks:*"' not in block
    assert '"emr-serverless:*"' not in block
    assert ":Delete" not in block
    assert ":Terminate" not in block
    assert ":Stop" not in block
    assert ":Describe" not in block


def test_nightly_teardown_role_can_remove_w4_network_costs():
    text = FINOPS.read_text()
    for action in (
        "elasticloadbalancing:DescribeLoadBalancers",
        "elasticloadbalancing:DescribeTags",
        "elasticloadbalancing:DeleteLoadBalancer",
        "ec2:DescribeNatGateways",
        "ec2:DeleteNatGateway",
        "ec2:DescribeAddresses",
        "ec2:ReleaseAddress",
    ):
        assert f'"{action}"' in text


def test_nightly_teardown_role_can_read_tags_before_mutating():
    text = FINOPS.read_text()
    for action in (
        "kinesisanalytics:ListTagsForResource",
        "kafka:ListTagsForResource",
        "elasticloadbalancing:DescribeTags",
    ):
        assert f'"{action}"' in text


def test_budget_action_trust_is_bound_to_this_account_and_exact_budget():
    text = FINOPS.read_text()
    block = text.split('data "aws_iam_policy_document" "budget_action_assume"', 1)[1].split(
        'resource "aws_iam_role" "budget_action"', 1
    )[0]
    assert 'variable = "aws:SourceAccount"' in block
    assert "values   = [data.aws_caller_identity.current.account_id]" in block
    assert 'variable = "aws:SourceArn"' in block
    assert "budget/${local.name_prefix}-hard-75" in block
    assert "budget/*" not in block


def test_destructive_network_iam_is_resource_and_tag_scoped():
    cases = {
        "DeleteTaggedEksFrontdoorNetworkLoadBalancer": (
            "elasticloadbalancing:DeleteLoadBalancer",
            "loadbalancer/net/${local.name_prefix}-eks/*",
            "aws:ResourceTag/",
            "eks_frontdoor",
        ),
        "DeleteTaggedNetworkNatGateway": (
            "ec2:DeleteNatGateway",
            "natgateway/*",
            "ec2:ResourceTag/",
            "network",
        ),
        "ReleaseTaggedNetworkElasticIp": (
            "ec2:ReleaseAddress",
            "elastic-ip/*",
            "ec2:ResourceTag/",
            "network",
        ),
    }
    for sid, (action, resource, condition_prefix, module) in cases.items():
        block = _statement_for_sid(sid)
        assert f'"{action}"' in block
        assert resource in block
        assert 'resources = ["*"]' not in block
        for key in ("Project", "Environment", "Module"):
            assert f'variable = "{condition_prefix}{key}"' in block
        assert f'values   = ["{module}"]' in block


def test_finops_boundary_coupling_is_validated_at_both_module_layers():
    for path in (FINOPS_VARIABLES, ROOT_VARIABLES):
        text = path.read_text()
        assert 'var.project == "harbormaster"' in text
        assert 'var.platform_role_name == "harbormaster-platform"' in text
