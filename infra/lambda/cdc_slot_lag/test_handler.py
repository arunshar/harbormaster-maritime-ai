"""Gate: infra/lambda/cdc_slot_lag/handler.py.

Kept out of pyproject.toml's testpaths, matching the sibling drift_watch and
teardown Lambda tests (Lambda-runtime code, run explicitly rather than as part
of the default `pytest -q`):
`python -m pytest infra/lambda/cdc_slot_lag/test_handler.py`.

Covers the TLS context builder only: it must verify certificates with secure
defaults and, when RDS_CA_BUNDLE is set, pin that CA bundle. No network.
"""

from __future__ import annotations

import os
import ssl
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
for p in (HERE, REPO_ROOT):
    if p not in sys.path:
        sys.path.insert(0, p)

import handler  # noqa: E402


def test_ssl_context_verifies_certificates_by_default(monkeypatch):
    monkeypatch.delenv("RDS_CA_BUNDLE", raising=False)

    ctx = handler._build_ssl_context()

    assert ctx.verify_mode == ssl.CERT_REQUIRED
    assert ctx.check_hostname is True


def test_ssl_context_loads_pinned_bundle_when_env_set(monkeypatch, tmp_path):
    # A self-signed PEM is enough to prove the bundle path is loaded; no CA
    # material from the ambient trust store is needed for this assertion.
    calls: list[str] = []
    real_load = ssl.SSLContext.load_verify_locations

    def _spy(self, *, cafile=None, capath=None, cadata=None):
        calls.append(cafile)
        # Do not actually parse a file: assert on the call, keep the test hermetic.

    monkeypatch.setattr(ssl.SSLContext, "load_verify_locations", _spy)

    bundle = tmp_path / "rds-ca.pem"
    bundle.write_text("placeholder")
    monkeypatch.setenv("RDS_CA_BUNDLE", str(bundle))

    ctx = handler._build_ssl_context()

    assert calls == [str(bundle)]
    assert ctx.verify_mode == ssl.CERT_REQUIRED
    assert ctx.check_hostname is True
    # Silence the unused reference; the real loader stays swappable for callers.
    assert real_load is not None
