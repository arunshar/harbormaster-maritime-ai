"""SageMaker Model Registry (Phase 3, gate 3.7).

Registers a candidate that passed the holdout gate as a new Model Package
version with ModelApprovalStatus=PendingManualApproval and the gate result
attached as model-package metadata; approval (flipping to Approved) is
scripted here, and an operator can also do it by hand in the console. Every
AWS mutation in this repo is operator-run. W&B (gate 3.5)
owns lineage; this module owns the package approval record. That approval is
not conflated with deployment authorization because the current Terraform
endpoint consumes a separately reviewed artifact directly. The two owners are
deliberately not merged. The registry decision keeps W&B plus the SageMaker
Model Registry and excludes managed MLflow.

The SageMaker client is injected throughout (mirrors this repo's pattern
everywhere else): unit tests use a fake, never real boto3/AWS.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Protocol

from mlops.holdout_gate import HoldoutGateResult

PENDING_MANUAL_APPROVAL = "PendingManualApproval"
APPROVED = "Approved"
REJECTED = "Rejected"


class SageMakerModelRegistryClient(Protocol):
    def create_model_package(self, **kwargs: Any) -> dict: ...
    def update_model_package(self, **kwargs: Any) -> dict: ...


@dataclass(frozen=True)
class RegisteredCandidate:
    model_package_arn: str
    approval_status: str
    holdout_gate: HoldoutGateResult


def register_candidate(
    *,
    sagemaker_client: SageMakerModelRegistryClient,
    model_package_group_name: str,
    model_data_url: str,
    container_image: str,
    holdout_result: HoldoutGateResult,
) -> RegisteredCandidate:
    """A candidate that failed the holdout gate is never registered at all:
    invariant 1 (docs/phases/PHASE_3.md) is enforced here by refusing to
    call SageMaker, not by registering-then-rejecting."""
    if not holdout_result.passed:
        raise ValueError(
            f"refusing to register a candidate that failed the holdout gate: "
            f"{holdout_result.failures}"
        )

    metadata = {
        "auc": holdout_result.auc,
        "crps": holdout_result.crps,
        "calibration_ratio": holdout_result.calibration_ratio,
    }
    resp = sagemaker_client.create_model_package(
        ModelPackageGroupName=model_package_group_name,
        ModelApprovalStatus=PENDING_MANUAL_APPROVAL,
        InferenceSpecification={
            "Containers": [{"Image": container_image, "ModelDataUrl": model_data_url}],
            "SupportedContentTypes": ["application/json"],
            "SupportedResponseMIMETypes": ["application/json"],
        },
        CustomerMetadataProperties={"holdout_gate": json.dumps(metadata, sort_keys=True)},
    )
    return RegisteredCandidate(
        model_package_arn=resp["ModelPackageArn"],
        approval_status=PENDING_MANUAL_APPROVAL,
        holdout_gate=holdout_result,
    )


def approve(sagemaker_client: SageMakerModelRegistryClient, model_package_arn: str) -> None:
    sagemaker_client.update_model_package(
        ModelPackageArn=model_package_arn, ModelApprovalStatus=APPROVED
    )


def reject(sagemaker_client: SageMakerModelRegistryClient, model_package_arn: str) -> None:
    sagemaker_client.update_model_package(
        ModelPackageArn=model_package_arn, ModelApprovalStatus=REJECTED
    )
