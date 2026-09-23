"""Hermetic regression tests for the Phase 5 digest-only renderer."""

from __future__ import annotations

import pytest

from scripts.render_phase5_serving import (
    IMAGE_SENTINEL,
    render_manifest,
    validate_ecr_digest,
    validate_ecr_repository,
    write_atomic,
)

REPOSITORY = "123456789012.dkr.ecr.us-east-1.amazonaws.com/harbormaster-base-serving"
IMAGE = REPOSITORY + "@sha256:" + "a" * 64


def test_validate_ecr_digest_accepts_only_an_immutable_ecr_reference():
    assert validate_ecr_repository(f"  {REPOSITORY}  ") == REPOSITORY
    assert validate_ecr_digest(f"  {IMAGE}  ", REPOSITORY) == IMAGE


@pytest.mark.parametrize(
    "image",
    [
        "harbormaster-serving:latest",
        "123456789012.dkr.ecr.us-east-1.amazonaws.com/repo:commit",
        "docker.io/example/repo@sha256:" + "a" * 64,
        "123456789012.dkr.ecr.us-east-1.amazonaws.com/repo@sha256:short",
        "123456789012.dkr.ecr.us-east-1.amazonaws.com/Repo@sha256:" + "a" * 64,
    ],
)
def test_validate_ecr_digest_rejects_tags_non_ecr_and_malformed_values(image):
    with pytest.raises(ValueError, match="immutable ECR reference"):
        validate_ecr_digest(image, REPOSITORY)


@pytest.mark.parametrize(
    "repository",
    [
        "111122223333.dkr.ecr.us-east-1.amazonaws.com/harbormaster-base-serving",
        "123456789012.dkr.ecr.us-west-2.amazonaws.com/harbormaster-base-serving",
        "123456789012.dkr.ecr.us-east-1.amazonaws.com/other-serving",
    ],
)
def test_validate_ecr_digest_rejects_a_different_expected_repository(repository):
    with pytest.raises(ValueError, match="does not match"):
        validate_ecr_digest(IMAGE, repository)


def test_render_replaces_exactly_one_sentinel():
    source = f"before\nimage: {IMAGE_SENTINEL}\nafter\n"
    rendered = render_manifest(source, IMAGE, REPOSITORY)
    assert IMAGE_SENTINEL not in rendered
    assert rendered.count(IMAGE) == 1


@pytest.mark.parametrize("source", ["no image here", f"{IMAGE_SENTINEL}\n{IMAGE_SENTINEL}\n"])
def test_render_rejects_missing_or_duplicate_sentinel(source):
    with pytest.raises(ValueError, match="exactly one image sentinel"):
        render_manifest(source, IMAGE, REPOSITORY)


def test_atomic_write_replaces_an_existing_artifact(tmp_path):
    output = tmp_path / "w4" / "serving.yaml"
    output.parent.mkdir()
    output.write_text("partial")
    write_atomic(output, "complete\n")
    assert output.read_text() == "complete\n"
    assert list(output.parent.glob(".serving.yaml.*.tmp")) == []
