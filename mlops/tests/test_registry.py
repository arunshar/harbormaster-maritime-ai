from __future__ import annotations

import json
from typing import Any

import pytest

from mlops.holdout_gate import HoldoutGateResult
from mlops.registry import (
    APPROVED,
    PENDING_MANUAL_APPROVAL,
    REJECTED,
    approve,
    register_candidate,
    reject,
)

PASSING = HoldoutGateResult(auc=0.95, crps=0.2, calibration_ratio=1.0, passed=True, failures=[])
FAILING = HoldoutGateResult(
    auc=0.5, crps=0.2, calibration_ratio=1.0, passed=False, failures=["auc 0.5 < auc_min 0.85"]
)


class FakeSageMakerRegistry:
    def __init__(self) -> None:
        self.create_calls: list[dict[str, Any]] = []
        self.update_calls: list[dict[str, Any]] = []

    def create_model_package(self, **kwargs: Any) -> dict:
        self.create_calls.append(kwargs)
        return {"ModelPackageArn": "arn:aws:sagemaker:us-east-1:123:model-package/hm-pidpm/1"}

    def update_model_package(self, **kwargs: Any) -> dict:
        self.update_calls.append(kwargs)
        return {}


def test_register_candidate_calls_create_model_package_with_pending_approval():
    client = FakeSageMakerRegistry()
    registered = register_candidate(
        sagemaker_client=client,
        model_package_group_name="hm-pidpm",
        model_data_url="s3://hm-models-bucket/pidpm/us-east-1/run-1/model.tar.gz",
        container_image="123456789012.dkr.ecr.us-east-1.amazonaws.com/hm-pidpm:latest",
        holdout_result=PASSING,
    )
    assert registered.approval_status == PENDING_MANUAL_APPROVAL
    assert len(client.create_calls) == 1
    assert client.create_calls[0]["ModelApprovalStatus"] == PENDING_MANUAL_APPROVAL


def test_register_candidate_attaches_the_holdout_gate_result_as_metadata():
    client = FakeSageMakerRegistry()
    register_candidate(
        sagemaker_client=client,
        model_package_group_name="hm-pidpm",
        model_data_url="s3://bucket/model.tar.gz",
        container_image="image:latest",
        holdout_result=PASSING,
    )
    metadata = json.loads(client.create_calls[0]["CustomerMetadataProperties"]["holdout_gate"])
    assert metadata == {"auc": 0.95, "crps": 0.2, "calibration_ratio": 1.0}


def test_register_candidate_refuses_a_failing_candidate_without_calling_sagemaker():
    client = FakeSageMakerRegistry()
    with pytest.raises(ValueError):
        register_candidate(
            sagemaker_client=client,
            model_package_group_name="hm-pidpm",
            model_data_url="s3://bucket/model.tar.gz",
            container_image="image:latest",
            holdout_result=FAILING,
        )
    assert client.create_calls == []  # invariant 1: never reaches the registry at all


def test_approve_updates_the_model_package_status():
    client = FakeSageMakerRegistry()
    approve(client, "arn:aws:sagemaker:us-east-1:123:model-package/hm-pidpm/1")
    assert client.update_calls[0]["ModelApprovalStatus"] == APPROVED
    assert (
        client.update_calls[0]["ModelPackageArn"]
        == "arn:aws:sagemaker:us-east-1:123:model-package/hm-pidpm/1"
    )


def test_reject_updates_the_model_package_status():
    client = FakeSageMakerRegistry()
    reject(client, "arn:aws:sagemaker:us-east-1:123:model-package/hm-pidpm/1")
    assert client.update_calls[0]["ModelApprovalStatus"] == REJECTED
