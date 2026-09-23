"""Minimal SageMaker container entrypoint for the Phase 3 demo stand-in.

SageMaker passes ``serve`` to a real-time inference image. This entrypoint
deliberately ignores that argument and replaces itself with the fixed Gunicorn
server command, so the image also works when run locally without arguments.
"""

from __future__ import annotations

import os


def main() -> None:
    # The argv list is fixed. It uses no shell and takes no user-controlled path or argument.
    os.execv(  # nosec B606
        "/usr/local/bin/gunicorn",
        [
            "/usr/local/bin/gunicorn",
            "--bind",
            "0.0.0.0:8080",
            "--workers",
            "1",
            "server:app",
        ],
    )


if __name__ == "__main__":
    main()
