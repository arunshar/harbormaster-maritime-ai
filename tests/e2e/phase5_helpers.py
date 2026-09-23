"""Pure helpers for the Phase 5 scaffold acceptance (gates 5.0+).

Phase 5's infrastructure is authored-not-applied, so what a test CAN honestly
pin down is structural, mirroring gate 4.7's phase4_helpers.py and the CMK
pass's kms_helpers.py: the Phase 5 modules are whole-module count-gated on
enable_phase5, enable_phase5 carries the enable_phase1 cross-validation (the
enable_phase2/3 convention), and the gate 5.0 teardown guard is armed by
default (guard_dry_run defaults false) with the 4-hour window the spec names.
Unit-tested unguarded in test_phase5_helpers.py.
"""

from __future__ import annotations

import re
from pathlib import Path

INFRA_TERRAFORM_PATH = Path(__file__).parent.parent.parent / "infra" / "terraform"
ENV_MAIN_TF_PATH = INFRA_TERRAFORM_PATH / "envs" / "base" / "main.tf"
ENV_VARIABLES_TF_PATH = INFRA_TERRAFORM_PATH / "envs" / "base" / "variables.tf"
GUARD_MODULE_PATH = INFRA_TERRAFORM_PATH / "modules" / "eks_teardown_guard"
EKS_CLUSTER_MODULE_PATH = INFRA_TERRAFORM_PATH / "modules" / "eks_cluster"
EKS_NODE_GROUP_MODULE_PATH = INFRA_TERRAFORM_PATH / "modules" / "eks_node_group"

# Matches a `module "<name>" { count = var.enable_phase5 ? 1 : 0 ... }` block
# specifically, scoped to content between its own braces (gate 3.9's lesson:
# an unscoped substring match can be fooled by a mention in a comment).
_PHASE5_GATED_MODULE_RE_TEMPLATE = (
    r'module\s+"{name}"\s*\{{[^{{}}]*count\s*=\s*var\.enable_phase5\s*\?\s*1\s*:\s*0\b'
)

# The enable_phase5 -> enable_phase1 cross-variable validation, inside the
# enable_phase5 variable block's validation stanza.
_PHASE5_REQUIRES_PHASE1_RE = re.compile(
    r'variable\s+"enable_phase5"\s*\{.*?'
    r"condition\s*=\s*!var\.enable_phase5\s*\|\|\s*var\.enable_phase1",
    re.DOTALL,
)

_VARIABLE_BLOCK_RE_TEMPLATE = r'variable\s+"{name}"\s*\{{(.*?)\n\}}'
_DEFAULT_RE = re.compile(r"default\s*=\s*(\S+)")


def module_is_gated_on_enable_phase5(env_main_tf_text: str, module_name: str) -> bool:
    """True if the named module call's whole-module count is structurally tied
    to enable_phase5 (not hardcoded, not gated on a different var)."""
    pattern = re.compile(_PHASE5_GATED_MODULE_RE_TEMPLATE.format(name=re.escape(module_name)))
    return pattern.search(env_main_tf_text) is not None


def enable_phase5_requires_enable_phase1(env_variables_tf_text: str) -> bool:
    """True if enable_phase5 carries the exact cross-variable validation the
    enable_phase2/enable_phase3 convention prescribes."""
    return _PHASE5_REQUIRES_PHASE1_RE.search(env_variables_tf_text) is not None


def variable_default(tf_text: str, variable_name: str) -> str | None:
    """The literal default expression of a variable block, or None when the
    variable is absent or declares no default. Matches the block lazily up to
    the first newline-anchored closing brace, so nested validation blocks
    inside the variable stay in scope."""
    pattern = re.compile(
        _VARIABLE_BLOCK_RE_TEMPLATE.format(name=re.escape(variable_name)), re.DOTALL
    )
    block = pattern.search(tf_text)
    if not block:
        return None
    default = _DEFAULT_RE.search(block.group(1))
    return default.group(1) if default else None


def guard_is_armed_by_default(guard_variables_tf_text: str) -> bool:
    """True if the teardown guard's dry-run default is false (armed): the
    structural-not-procedural property gate 5.0 exists to provide."""
    return variable_default(guard_variables_tf_text, "guard_dry_run") == "false"


def guard_window_default_hours(guard_variables_tf_text: str) -> str | None:
    """The guard's default force-destroy window (the spec pins 4)."""
    return variable_default(guard_variables_tf_text, "max_age_hours")


def _strip_hcl_comments(tf_text: str) -> str:
    """Drop full-line # comments so prose mentions of a block (the module
    header narrating the GKE pattern, say) can never satisfy a structural
    check, the gate 3.9 lesson applied to block matches."""
    return "\n".join(line for line in tf_text.splitlines() if not line.lstrip().startswith("#"))


def node_group_scaling_config(node_group_main_tf_text: str) -> dict[str, str] | None:
    """The min/max/desired literals inside the aws_eks_node_group's
    scaling_config block, or None when the block is absent. Gate 5.1 pins the
    scale-to-zero shape: min 0, max 3, desired 0 (via variable defaults, so
    this reads the variables file's defaults instead when given it)."""
    code_only = _strip_hcl_comments(node_group_main_tf_text)
    block = re.search(r"scaling_config\s*\{(.*?)\}", code_only, re.DOTALL)
    if not block:
        return None
    out: dict[str, str] = {}
    for key in ("min_size", "max_size", "desired_size"):
        m = re.search(rf"{key}\s*=\s*(\S+)", block.group(1))
        if m:
            out[key] = m.group(1)
    return out


def keda_release_is_gated_on_install_keda(eks_cluster_main_tf_text: str) -> bool:
    """True if the keda helm_release's count is structurally tied to
    install_keda (the documented two-step provider story), never
    unconditional."""
    pattern = re.compile(
        r'resource\s+"helm_release"\s+"keda"\s*\{[^{}]*'
        r"count\s*=\s*var\.install_keda\s*\?\s*1\s*:\s*0\b"
    )
    return pattern.search(eks_cluster_main_tf_text) is not None


def helm_data_sources_gated_on_access_flag(env_main_tf_text: str) -> bool:
    """True if BOTH cluster-credential data sources feeding the helm provider
    are count-gated on enable_phase5_kubernetes_access. Keeping provider access
    separate from the release flag permits a clean Helm uninstall before the
    teardown guard removes the cluster."""
    for data_type in ("aws_eks_cluster", "aws_eks_cluster_auth"):
        pattern = re.compile(
            rf'data\s+"{data_type}"\s+"phase5"\s*\{{[^{{}}]*'
            r"count\s*=\s*var\.enable_phase5_kubernetes_access\s*\?\s*1\s*:\s*0\b"
        )
        if not pattern.search(env_main_tf_text):
            return False
    return True


def enable_phase5_references(env_main_tf_text: str) -> set[str]:
    """Names of the module calls in envs/base/main.tf whose count references
    enable_phase5. Supports the zero-diff argument: every Phase 5 surface must
    sit behind the toggle, so the false plan collapses them all."""
    module_headers = [
        (m.start(), m.group(1)) for m in re.finditer(r'module\s+"([^"]+)"\s*\{', env_main_tf_text)
    ]
    gated: set[str] = set()
    for ref in re.finditer(r"count\s*=\s*var\.enable_phase5\s*\?\s*1\s*:\s*0", env_main_tf_text):
        preceding = [name for start, name in module_headers if start < ref.start()]
        if preceding:
            gated.add(preceding[-1])
    return gated
