"""Pure helpers for the Phase 3 lake/promotion e2e acceptance (gate 3.9).
Unit-tested unguarded in test_lake_helpers.py; test_phase3.py uses them
against the local plane (fakes/pure functions) by default, or the AWS
showcase when pointed at a real demo apply (operator-run)."""

from __future__ import annotations

import re
from pathlib import Path

_MODULES_DIR = Path(__file__).parent.parent.parent / "infra" / "terraform" / "modules"
EMR_MODULE_PATH = _MODULES_DIR / "emr_backfill" / "main.tf"

# Matches an `auto_stop_configuration { ... enabled = true ... }` BLOCK
# specifically (not just the term appearing anywhere, e.g. in a comment):
# scoped to content between its own braces, no nested `{`/`}` inside.
_AUTO_STOP_BLOCK_RE = re.compile(
    r"auto_stop_configuration\s*\{[^{}]*\benabled\s*=\s*true\b[^{}]*\}"
)


def emr_module_has_auto_terminate(module_text: str) -> bool:
    """True if the EMR Serverless application resource structurally carries
    an enabled auto_stop_configuration block. A plain text/regex check
    rather than a full HCL parse: the property here is hardcoded in the
    resource body (not behind a variable that could be false), so its
    literal presence is exactly the guarantee gate 3.2's checksum promised.
    Scoped to the actual brace block, so a mention of the term in a comment
    (as this file's own module docstring has) cannot produce a false
    positive."""
    return _AUTO_STOP_BLOCK_RE.search(module_text) is not None
