"""Pure helpers for the CMK (modules/kms) wiring acceptance.

The CMK path is authored-not-applied, so what a test CAN honestly pin down is
structural: the kms module is count-gated on enable_cmk, the root local
collapses to the empty string while the flag is off, and every consumer
declares kms_key_arn with an empty default. Those three properties are the
zero-diff invariant (enable_cmk = false plans byte-identical to pre-CMK).
Unit-tested unguarded in test_kms_helpers.py, mirroring gate 4.7's
phase4_helpers.py convention.
"""

from __future__ import annotations

import re
from pathlib import Path

INFRA_TERRAFORM_PATH = Path(__file__).parent.parent.parent / "infra" / "terraform"
ENV_MAIN_TF_PATH = INFRA_TERRAFORM_PATH / "envs" / "base" / "main.tf"
MODULES_PATH = INFRA_TERRAFORM_PATH / "modules"

# Matches a `module "kms" { count = var.enable_cmk ? 1 : 0 ... }` block
# specifically, scoped to content between its own braces (gate 3.9's lesson:
# an unscoped substring match can be fooled by a mention in a comment).
_KMS_MODULE_RE = re.compile(
    r'module\s+"kms"\s*\{[^{}]*count\s*=\s*var\.enable_cmk\s*\?\s*1\s*:\s*0\b'
)

# The root local every consumer receives. It must collapse to "" while
# enable_cmk is false; consumers treat empty as "keep the pre-CMK encryption".
_KMS_LOCAL_RE = re.compile(
    r'kms_key_arn\s*=\s*var\.enable_cmk\s*\?\s*module\.kms\[0\]\.key_arn\s*:\s*""'
)

_MODULE_HEADER_RE = re.compile(r'module\s+"([^"]+)"\s*\{')
_WIRING_RE = re.compile(r"kms_key_arn\s*=\s*local\.kms_key_arn\b")
_KMS_VARIABLE_RE = re.compile(r'variable\s+"kms_key_arn"\s*\{([^}]*)\}')
_DEFAULT_RE = re.compile(r'default\s*=\s*"([^"]*)"')


def kms_module_is_gated_on_enable_cmk(env_main_tf_text: str) -> bool:
    """True if the kms module's whole-module count is structurally tied to
    enable_cmk (not hardcoded, not gated on a different var)."""
    return _KMS_MODULE_RE.search(env_main_tf_text) is not None


def kms_key_arn_local_collapses_to_empty(env_main_tf_text: str) -> bool:
    """True if the root kms_key_arn local is the exact enable_cmk ternary that
    collapses to the empty string when the flag is off."""
    return _KMS_LOCAL_RE.search(env_main_tf_text) is not None


def modules_wired_with_kms_key_arn(env_main_tf_text: str) -> set[str]:
    """Names of the module blocks in envs/base/main.tf that receive
    kms_key_arn = local.kms_key_arn. Each wiring line is attributed to the
    nearest preceding module header, which is its enclosing call."""
    wired: set[str] = set()
    headers = [(m.start(), m.group(1)) for m in _MODULE_HEADER_RE.finditer(env_main_tf_text)]
    for wire in _WIRING_RE.finditer(env_main_tf_text):
        preceding = [name for start, name in headers if start < wire.start()]
        if preceding:
            wired.add(preceding[-1])
    return wired


def consumer_kms_key_arn_defaults(modules_dir: Path) -> dict[str, str | None]:
    """Map of module name -> declared default for its kms_key_arn variable,
    across every module under modules_dir that declares one. None means the
    variable exists but has no string default (a zero-diff violation)."""
    defaults: dict[str, str | None] = {}
    for tf_file in sorted(modules_dir.glob("*/*.tf")):
        var_match = _KMS_VARIABLE_RE.search(tf_file.read_text())
        if var_match:
            default_match = _DEFAULT_RE.search(var_match.group(1))
            defaults[tf_file.parent.name] = default_match.group(1) if default_match else None
    return defaults
