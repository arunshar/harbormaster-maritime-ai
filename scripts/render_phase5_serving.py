"""Render the Phase 5 serving manifests with one immutable ECR image."""

from __future__ import annotations

import argparse
import re
import shutil
import subprocess
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
BASE_DIR = REPO_ROOT / "deploy/k8s/serving/base"
IMAGE_SENTINEL = "harbormaster.invalid/serving@sha256:" + "0" * 64
ECR_REPOSITORY_PATTERN = (
    r"(?P<repository>[0-9]{12}\.dkr\.ecr\.[a-z0-9-]+\.amazonaws\.com(?:\.cn)?/"
    r"[a-z0-9]+(?:[._/-][a-z0-9]+)*)"
)
ECR_REPOSITORY_RE = re.compile(rf"^{ECR_REPOSITORY_PATTERN}$")
ECR_DIGEST_RE = re.compile(rf"^{ECR_REPOSITORY_PATTERN}@sha256:[0-9a-f]{{64}}$")


def validate_ecr_repository(repository: str) -> str:
    """Return one normalized ECR repository URL or reject it."""
    repository = repository.strip()
    if not ECR_REPOSITORY_RE.fullmatch(repository):
        raise ValueError(
            "repository must be an ECR repository URL: "
            "<12-digit-account>.dkr.ecr.<region>.amazonaws.com/<repo>"
        )
    return repository


def validate_ecr_digest(image: str, expected_repository: str) -> str:
    """Require an immutable digest from the exact expected ECR repository."""
    image = image.strip()
    match = ECR_DIGEST_RE.fullmatch(image)
    if not match:
        raise ValueError(
            "image must be an immutable ECR reference: "
            "<12-digit-account>.dkr.ecr.<region>.amazonaws.com/<repo>@sha256:<64 hex>"
        )
    expected_repository = validate_ecr_repository(expected_repository)
    if match.group("repository") != expected_repository:
        raise ValueError("image repository does not match the expected ECR repository")
    return image


def render_manifest(build_output: str, image: str, expected_repository: str) -> str:
    """Replace exactly one non-runnable image sentinel in Kustomize output."""
    image = validate_ecr_digest(image, expected_repository)
    count = build_output.count(IMAGE_SENTINEL)
    if count != 1:
        raise ValueError(f"expected exactly one image sentinel in Kustomize output, found {count}")
    return build_output.replace(IMAGE_SENTINEL, image)


def build_base() -> str:
    """Build the tracked base with a local kustomize implementation."""
    if shutil.which("kustomize"):
        command = ["kustomize", "build", str(BASE_DIR)]
    elif shutil.which("kubectl"):
        command = ["kubectl", "kustomize", str(BASE_DIR)]
    else:
        raise RuntimeError("kustomize or kubectl is required to render the Phase 5 manifests")
    result = subprocess.run(command, check=True, capture_output=True, text=True)
    return result.stdout


def write_atomic(path: Path, content: str) -> None:
    """Write a complete artifact without leaving a partial manifest."""
    path = path.expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        with open(descriptor, "w") as handle:
            handle.write(content)
        temporary.replace(path)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", required=True, help="Immutable ECR repository@sha256 reference")
    parser.add_argument(
        "--repository",
        required=True,
        help="Expected Terraform-derived ECR repository URL",
    )
    parser.add_argument("--output", required=True, type=Path, help="Rendered YAML artifact path")
    args = parser.parse_args()

    try:
        image = validate_ecr_digest(args.image, args.repository)
        rendered = render_manifest(build_base(), image, args.repository)
        write_atomic(args.output, rendered)
    except (OSError, RuntimeError, ValueError, subprocess.CalledProcessError) as error:
        parser.error(str(error))
    print(f"rendered {args.output} with {image}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
