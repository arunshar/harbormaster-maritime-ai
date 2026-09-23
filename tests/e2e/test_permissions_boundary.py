"""The customer-managed permissions boundary must not silently disable the
teardown safety-net (Wave 3 pressure-test finding; war story P38).

The boundary is a ceiling: a role's effective permissions are its inline policy
INTERSECT this boundary. If a service the teardown Lambdas call is missing from
the Allow ceiling, every such call is AccessDenied at runtime, the Lambda
catches it and reports "nothing to tear down," and the resource bills
indefinitely past the $75 cap. That is exactly the procedural-failure mode the
structural teardown guard (gate 5.0) and the nightly finops sweeper were built
to prevent, so the boundary must cover every service they delete.
"""

from __future__ import annotations

import json
from fnmatch import fnmatchcase
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
BOUNDARY = REPO / "infra" / "aws" / "harbormaster-permissions-boundary.json"
PLATFORM_POLICY = REPO / "infra" / "aws" / "harbormaster-platform-permissions.json"

# Services the teardown paths invoke, each with a delete/scale-to-zero call that
# fails closed (leaving cost running) if the boundary omits it:
#   eks         - the EKS teardown guard force-destroys the control plane + node groups
#   kafka       - the finops nightly sweep deletes MSK Serverless (~$18/day, the biggest threat)
#   autoscaling - the finops sweep drains EC2 Auto Scaling groups to zero
#   ec2 / elasticloadbalancing - the finops sweep removes tagged NAT/EIP/NLB costs
#   sagemaker / elasticmapreduce / emr-serverless / kinesisanalytics - compute the sweep stops
#   ce          - month-to-date spend reporting keeps a partial teardown visible
#   sns / cloudwatch / logs - how the guard reports and observes
_TEARDOWN_CRITICAL = {
    "eks",
    "kafka",
    "autoscaling",
    "sagemaker",
    "elasticmapreduce",
    "emr-serverless",
    "ce",
    "kinesisanalytics",
    "sns",
    "cloudwatch",
    "logs",
    "ec2",
    "elasticloadbalancing",
    "xray",
}

_ACCOUNT_ID = "123456789012"
_PLATFORM_PRINCIPAL = f"arn:aws:iam::{_ACCOUNT_ID}:role/harbormaster-platform"
_SERVICE_PRINCIPAL = f"arn:aws:iam::{_ACCOUNT_ID}:role/harbormaster-base-eks-cluster"
_BOUNDARY_ARN = f"arn:aws:iam::{_ACCOUNT_ID}:policy/harbormaster-permissions-boundary"


def _as_list(value: object) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value]
    return [str(value)]


def _condition_matches(statement: dict[str, object], context: dict[str, str]) -> bool:
    conditions = statement.get("Condition", {})
    assert isinstance(conditions, dict)
    for operator, clauses in conditions.items():
        assert isinstance(clauses, dict)
        for key, expected in clauses.items():
            actual = context.get(key)
            expected_values = _as_list(expected)
            if operator in {"ArnLike", "StringLike"}:
                matches = actual is not None and any(
                    fnmatchcase(actual, pattern) for pattern in expected_values
                )
            elif operator in {"ArnEquals", "StringEquals"}:
                matches = actual is not None and actual in expected_values
            elif operator in {"ArnNotLike", "StringNotLike"}:
                matches = actual is None or not any(
                    fnmatchcase(actual, pattern) for pattern in expected_values
                )
            elif operator in {"ArnNotEquals", "StringNotEquals"}:
                matches = actual is None or actual not in expected_values
            else:
                raise AssertionError(
                    f"test evaluator does not support condition operator {operator}"
                )
            if not matches:
                return False
    return True


def _statement_matches(
    statement: dict[str, object],
    *,
    action: str,
    resource: str,
    context: dict[str, str],
) -> bool:
    actions = _as_list(statement["Action"])
    resources = _as_list(statement.get("Resource", "*"))
    return (
        any(fnmatchcase(action.lower(), pattern.lower()) for pattern in actions)
        and any(fnmatchcase(resource, pattern) for pattern in resources)
        and _condition_matches(statement, context)
    )


def _policy_decision(
    policy: dict[str, object],
    *,
    action: str,
    resource: str,
    context: dict[str, str],
) -> str:
    statements = policy["Statement"]
    assert isinstance(statements, list)
    matching = [
        statement
        for statement in statements
        if _statement_matches(statement, action=action, resource=resource, context=context)
    ]
    if any(statement["Effect"] == "Deny" for statement in matching):
        return "explicitDeny"
    if any(statement["Effect"] == "Allow" for statement in matching):
        return "allowed"
    return "implicitDeny"


def _platform_request(statement_sid: str, action: str) -> tuple[str, dict[str, str]]:
    context = {"aws:PrincipalArn": _PLATFORM_PRINCIPAL}
    role = f"arn:aws:iam::{_ACCOUNT_ID}:role/harbormaster-base-eks-cluster"

    if statement_sid == "CreateHarbormasterRolesOnlyWithBoundary":
        context["iam:PermissionsBoundary"] = _BOUNDARY_ARN
        return role, context
    if statement_sid == "AttachAndPutPoliciesOnlyOnBoundedHarbormasterRoles":
        context["iam:PermissionsBoundary"] = _BOUNDARY_ARN
        return role, context
    if statement_sid == "ManageHarbormasterRoleLifecycle":
        return role, context
    if statement_sid == "PassOnlyHarbormasterRolesToServices":
        context["iam:PassedToService"] = "eks.amazonaws.com"
        return role, context
    if statement_sid == "ManageHarbormasterEksOidcProvider":
        return (
            "arn:aws:iam::123456789012:oidc-provider/oidc.eks.us-east-1.amazonaws.com/id/EXAMPLE",
            context,
        )
    if statement_sid == "ListHarbormasterEksOidcProviders":
        return "*", context
    if statement_sid == "ManageHarbormasterCustomerManagedPolicies":
        return f"arn:aws:iam::{_ACCOUNT_ID}:policy/harbormaster-base-eks", context
    if statement_sid == "ManageHarbormasterInstanceProfiles":
        if action == "iam:ListInstanceProfilesForRole":
            return role, context
        return (
            f"arn:aws:iam::{_ACCOUNT_ID}:instance-profile/harbormaster-base-eks",
            context,
        )
    if statement_sid == "CreateHarbormasterServiceLinkedRoles":
        context["iam:AWSServiceName"] = "elasticloadbalancing.amazonaws.com"
        return (
            f"arn:aws:iam::{_ACCOUNT_ID}:role/aws-service-role/"
            "elasticloadbalancing.amazonaws.com/AWSServiceRoleForElasticLoadBalancing",
            context,
        )
    raise AssertionError(f"missing representative request for {statement_sid}")


def _platform_allow_requests(
    policy: dict[str, object],
) -> list[tuple[str, str, dict[str, str]]]:
    requests: list[tuple[str, str, dict[str, str]]] = []
    statements = policy["Statement"]
    assert isinstance(statements, list)
    for statement in statements:
        if statement["Effect"] != "Allow":
            continue
        sid = str(statement["Sid"])
        for action in _as_list(statement["Action"]):
            resource, context = _platform_request(sid, action)
            requests.append((action, resource, context))
    return requests


def _ceiling_services() -> set[str]:
    doc = json.loads(BOUNDARY.read_text())
    ceiling = next(s for s in doc["Statement"] if s["Sid"] == "AllowHarbormasterServiceCeiling")
    actions = ceiling["Action"]
    assert ceiling["Effect"] == "Allow"
    return {a.split(":", 1)[0] for a in actions}


def test_boundary_ceiling_covers_every_teardown_critical_service():
    services = _ceiling_services()
    missing = sorted(_TEARDOWN_CRITICAL - services)
    assert not missing, (
        f"the permissions boundary omits {missing}; the teardown Lambdas' delete/scale "
        "calls for those services would be AccessDenied and the cost would bill indefinitely"
    )


def test_boundary_still_denies_iam_and_boundary_escalation():
    # the ceiling widening must not have weakened the escalation guards
    doc = json.loads(BOUNDARY.read_text())
    deny_sids = {s["Sid"] for s in doc["Statement"] if s["Effect"] == "Deny"}
    assert "DenyAllIamWriteEscalation" in deny_sids
    assert "DenyPlatformSelfMutation" in deny_sids
    assert "DenyBoundaryPolicyMutation" in deny_sids
    assert "DenyBoundaryAlterationAndOrgEscape" in deny_sids


def test_platform_iam_actions_survive_identity_boundary_intersection():
    identity = json.loads(PLATFORM_POLICY.read_text())
    boundary = json.loads(BOUNDARY.read_text())
    identity_allowed: set[str] = set()
    boundary_allowed: set[str] = set()

    for action, resource, context in _platform_allow_requests(identity):
        if (
            _policy_decision(
                identity,
                action=action,
                resource=resource,
                context=context,
            )
            == "allowed"
        ):
            identity_allowed.add(action)
        if (
            _policy_decision(
                boundary,
                action=action,
                resource=resource,
                context=context,
            )
            == "allowed"
        ):
            boundary_allowed.add(action)

    required = {
        action
        for statement in identity["Statement"]
        if statement["Effect"] == "Allow"
        for action in _as_list(statement["Action"])
    }
    effective = identity_allowed & boundary_allowed
    assert effective == required, (
        "platform IAM actions missing from the effective identity-policy and permissions-boundary "
        f"intersection: {sorted(required - effective)}"
    )


def test_non_platform_role_cannot_use_platform_iam_write_policy_through_boundary():
    identity = json.loads(PLATFORM_POLICY.read_text())
    boundary = json.loads(BOUNDARY.read_text())
    effective_writes: set[str] = set()

    for action, resource, platform_context in _platform_allow_requests(identity):
        operation = action.split(":", 1)[1]
        if operation.startswith(("Get", "List")):
            continue
        service_context = {**platform_context, "aws:PrincipalArn": _SERVICE_PRINCIPAL}
        if (
            _policy_decision(
                identity,
                action=action,
                resource=resource,
                context=service_context,
            )
            == "allowed"
            and _policy_decision(
                boundary,
                action=action,
                resource=resource,
                context=service_context,
            )
            == "allowed"
        ):
            effective_writes.add(action)

    assert not effective_writes, (
        "a non-platform service role could perform IAM writes if it accidentally received the "
        f"platform identity policy: {sorted(effective_writes)}"
    )


def test_platform_create_role_still_requires_the_exact_boundary_context():
    identity = json.loads(PLATFORM_POLICY.read_text())
    boundary = json.loads(BOUNDARY.read_text())
    role = f"arn:aws:iam::{_ACCOUNT_ID}:role/harbormaster-base-eks-cluster"
    context = {
        "aws:PrincipalArn": _PLATFORM_PRINCIPAL,
        "iam:PermissionsBoundary": f"arn:aws:iam::{_ACCOUNT_ID}:policy/different-boundary",
    }

    assert (
        _policy_decision(
            identity,
            action="iam:CreateRole",
            resource=role,
            context=context,
        )
        == "implicitDeny"
    )
    assert (
        _policy_decision(
            boundary,
            action="iam:CreateRole",
            resource=role,
            context=context,
        )
        == "implicitDeny"
    )


def test_platform_iam_ceiling_is_resource_scoped_and_within_aws_policy_limit():
    boundary = json.loads(BOUNDARY.read_text())
    minified = json.dumps(boundary, separators=(",", ":"))
    platform_statements = [
        statement for statement in boundary["Statement"] if statement["Sid"].startswith("Platform")
    ]

    assert len(minified) <= 6_144
    assert platform_statements
    assert all("iam:*" not in _as_list(statement["Action"]) for statement in platform_statements)
    wildcard_resources = {
        statement["Sid"] for statement in platform_statements if statement["Resource"] == "*"
    }
    assert wildcard_resources == {"PlatformListEksOidc"}


def test_platform_iam_ceiling_rejects_out_of_scope_resources_and_services():
    boundary = json.loads(BOUNDARY.read_text())
    common = {"aws:PrincipalArn": _PLATFORM_PRINCIPAL}
    denied_requests = [
        (
            "iam:CreateRole",
            f"arn:aws:iam::{_ACCOUNT_ID}:role/unrelated-admin",
            {**common, "iam:PermissionsBoundary": _BOUNDARY_ARN},
        ),
        (
            "iam:CreatePolicy",
            f"arn:aws:iam::{_ACCOUNT_ID}:policy/unrelated-admin",
            common,
        ),
        (
            "iam:PassRole",
            f"arn:aws:iam::{_ACCOUNT_ID}:role/harbormaster-base-eks-cluster",
            {**common, "iam:PassedToService": "iam.amazonaws.com"},
        ),
        (
            "iam:CreateServiceLinkedRole",
            f"arn:aws:iam::{_ACCOUNT_ID}:role/aws-service-role/"
            "unknown.amazonaws.com/AWSServiceRoleForUnknown",
            {**common, "iam:AWSServiceName": "unknown.amazonaws.com"},
        ),
    ]

    decisions = {
        action: _policy_decision(
            boundary,
            action=action,
            resource=resource,
            context=context,
        )
        for action, resource, context in denied_requests
    }
    assert decisions == {
        "iam:CreateRole": "implicitDeny",
        "iam:CreatePolicy": "implicitDeny",
        "iam:PassRole": "implicitDeny",
        "iam:CreateServiceLinkedRole": "implicitDeny",
    }


def test_platform_cannot_mutate_its_own_role_but_budget_freeze_can_attach():
    boundary = json.loads(BOUNDARY.read_text())
    platform_role = f"arn:aws:iam::{_ACCOUNT_ID}:role/harbormaster-platform"
    platform_context = {
        "aws:PrincipalArn": _PLATFORM_PRINCIPAL,
        "iam:PermissionsBoundary": _BOUNDARY_ARN,
    }
    self_mutations = {
        "iam:PutRolePolicy",
        "iam:DeleteRolePolicy",
        "iam:UpdateAssumeRolePolicy",
        "iam:DeleteRole",
        "iam:UpdateRole",
        "iam:AttachRolePolicy",
        "iam:DetachRolePolicy",
    }

    assert {
        action: _policy_decision(
            boundary,
            action=action,
            resource=platform_role,
            context=platform_context,
        )
        for action in self_mutations
    } == {action: "explicitDeny" for action in self_mutations}

    budget_context = {
        "aws:PrincipalArn": (f"arn:aws:iam::{_ACCOUNT_ID}:role/harbormaster-base-budget-action"),
        "iam:PolicyARN": (f"arn:aws:iam::{_ACCOUNT_ID}:policy/harbormaster-base-spend-freeze"),
    }
    assert (
        _policy_decision(
            boundary,
            action="iam:AttachRolePolicy",
            resource=platform_role,
            context=budget_context,
        )
        == "allowed"
    )


def test_platform_identity_cannot_put_policy_on_its_unbounded_self_role():
    identity = json.loads(PLATFORM_POLICY.read_text())
    platform_role = f"arn:aws:iam::{_ACCOUNT_ID}:role/harbormaster-platform"

    assert (
        _policy_decision(
            identity,
            action="iam:PutRolePolicy",
            resource=platform_role,
            context={"aws:PrincipalArn": _PLATFORM_PRINCIPAL},
        )
        == "implicitDeny"
    )


def test_boundary_policy_version_and_deletion_actions_are_explicitly_denied():
    boundary = json.loads(BOUNDARY.read_text())
    context = {"aws:PrincipalArn": _PLATFORM_PRINCIPAL}
    mutations = {
        "iam:CreatePolicyVersion",
        "iam:DeletePolicyVersion",
        "iam:SetDefaultPolicyVersion",
        "iam:DeletePolicy",
    }

    assert {
        action: _policy_decision(
            boundary,
            action=action,
            resource=_BOUNDARY_ARN,
            context=context,
        )
        for action in mutations
    } == {action: "explicitDeny" for action in mutations}
    assert (
        _policy_decision(
            boundary,
            action="iam:CreatePolicyVersion",
            resource=f"arn:aws:iam::{_ACCOUNT_ID}:policy/harbormaster-base-eks",
            context=context,
        )
        == "allowed"
    )


def test_boundary_allows_only_the_budget_role_to_apply_the_exact_freeze_policy():
    doc = json.loads(BOUNDARY.read_text())
    allow = next(s for s in doc["Statement"] if s["Sid"] == "AllowBudgetActionSpendFreeze")
    deny = next(s for s in doc["Statement"] if s["Sid"] == "DenyAllIamWriteEscalation")

    assert set(allow["Action"]) == {"iam:AttachRolePolicy", "iam:DetachRolePolicy"}
    assert allow["Resource"] == "arn:aws:iam::*:role/harbormaster-platform"
    conditions = allow["Condition"]["ArnLike"]
    principals = conditions["aws:PrincipalArn"]
    policies = conditions["iam:PolicyARN"]
    assert fnmatchcase(
        "arn:aws:iam::123456789012:role/harbormaster-base-budget-action",
        principals[0],
    )
    assert not any(
        fnmatchcase("arn:aws:iam::123456789012:role/harbormaster-platform", pattern)
        for pattern in principals
    )
    assert fnmatchcase(
        "arn:aws:iam::123456789012:policy/harbormaster-base-spend-freeze",
        policies[0],
    )
    assert not any(
        fnmatchcase("arn:aws:iam::123456789012:policy/AdministratorAccess", pattern)
        for pattern in policies
    )
    assert "iam:AttachRolePolicy" not in deny["Action"]
    assert "iam:DetachRolePolicy" not in deny["Action"]


def test_platform_policy_can_pass_roles_to_eks_and_ec2():
    doc = json.loads(PLATFORM_POLICY.read_text())
    statement = next(
        item for item in doc["Statement"] if item["Sid"] == "PassOnlyHarbormasterRolesToServices"
    )
    services = statement["Condition"]["StringEquals"]["iam:PassedToService"]
    assert {"eks.amazonaws.com", "ec2.amazonaws.com"} <= set(services)


def test_platform_policy_boundary_condition_matches_a_concrete_account_arn():
    doc = json.loads(PLATFORM_POLICY.read_text())
    concrete = "arn:aws:iam::123456789012:policy/harbormaster-permissions-boundary"
    for sid in (
        "CreateHarbormasterRolesOnlyWithBoundary",
        "AttachAndPutPoliciesOnlyOnBoundedHarbormasterRoles",
    ):
        statement = next(item for item in doc["Statement"] if item["Sid"] == sid)
        pattern = statement["Condition"]["ArnLike"]["iam:PermissionsBoundary"]
        assert fnmatchcase(concrete, pattern)
        assert not fnmatchcase(
            "arn:aws:iam::123456789012:policy/different-boundary",
            pattern,
        )


def test_platform_policy_boundary_replacement_deny_uses_arn_wildcard_matching():
    doc = json.loads(PLATFORM_POLICY.read_text())
    statement = next(
        item
        for item in doc["Statement"]
        if item["Sid"] == "DenyReplacingBoundaryWithADifferentPolicy"
    )
    condition = statement["Condition"]
    assert "StringNotEquals" not in condition
    pattern = condition["ArnNotLike"]["iam:PermissionsBoundary"]
    assert fnmatchcase(
        "arn:aws:iam::123456789012:policy/harbormaster-permissions-boundary",
        pattern,
    )
    assert not fnmatchcase(
        "arn:aws:iam::123456789012:policy/different-boundary",
        pattern,
    )


def test_platform_policy_allows_only_required_eks_service_linked_roles():
    doc = json.loads(PLATFORM_POLICY.read_text())
    statement = next(
        item for item in doc["Statement"] if item["Sid"] == "CreateHarbormasterServiceLinkedRoles"
    )
    services = set(statement["Condition"]["StringEquals"]["iam:AWSServiceName"])
    assert {
        "eks.amazonaws.com",
        "eks-nodegroup.amazonaws.com",
        "elasticloadbalancing.amazonaws.com",
        "autoscaling.amazonaws.com",
        "spot.amazonaws.com",
    } <= services


def test_platform_policy_scopes_eks_oidc_provider_lifecycle():
    doc = json.loads(PLATFORM_POLICY.read_text())
    statement = next(
        item for item in doc["Statement"] if item["Sid"] == "ManageHarbormasterEksOidcProvider"
    )
    assert statement["Resource"] == (
        "arn:aws:iam::*:oidc-provider/oidc.eks.us-east-1.amazonaws.com/id/*"
    )
    assert {
        "iam:CreateOpenIDConnectProvider",
        "iam:DeleteOpenIDConnectProvider",
        "iam:GetOpenIDConnectProvider",
        "iam:UpdateOpenIDConnectProviderThumbprint",
        "iam:TagOpenIDConnectProvider",
        "iam:UntagOpenIDConnectProvider",
    } <= set(statement["Action"])
