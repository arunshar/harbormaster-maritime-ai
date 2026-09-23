"""Pure helpers for the Phase 4 drift/flywheel e2e acceptance (gate 4.7).
Unit-tested unguarded in test_phase4_helpers.py, mirroring gate 3.9's
lake_helpers.py convention."""

from __future__ import annotations

import re
from pathlib import Path

ENV_MAIN_TF_PATH = (
    Path(__file__).parent.parent.parent / "infra" / "terraform" / "envs" / "base" / "main.tf"
)

# Matches a `module "drift_watch" { count = var.enable_phase4 ? 1 : 0 ... }`
# block specifically, scoped to content between its own braces (gate 3.9's
# lesson: an unscoped substring match can be fooled by a mention of the term
# in a docstring/comment elsewhere in the file).
_DRIFT_WATCH_MODULE_RE = re.compile(
    r'module\s+"drift_watch"\s*\{[^{}]*count\s*=\s*var\.enable_phase4\s*\?\s*1\s*:\s*0\b'
)


def drift_watch_module_is_gated_on_enable_phase4(env_main_tf_text: str) -> bool:
    """True if the drift_watch module's whole-module count is structurally
    tied to enable_phase4 (not hardcoded, not gated on a different var), the
    same plain text/regex check gate 3.9 established: the property is
    hardcoded in the call site, not behind anything that could silently
    diverge, so its literal presence is exactly the guarantee this gate's
    checksum promised."""
    return _DRIFT_WATCH_MODULE_RE.search(env_main_tf_text) is not None
