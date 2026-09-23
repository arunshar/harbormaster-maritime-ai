"""Build the local application zip consumed by Managed Flink."""

from __future__ import annotations

import os
import shutil
import tempfile
import zipfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
OUTPUT_MARKER = ".harbormaster-flink-package-output"
OUTPUT_MARKER_CONTENT = "managed by harbormaster flink.package_app\n"
PACKAGE_MEMBERS = (
    "main.py",
    "requirements.txt",
    "flink/__init__.py",
    "flink/window_logic.py",
    "lib/pyflink-dependencies.jar",
)


def build_flink_package(
    *,
    repo_root: Path = REPO_ROOT,
    jar_path: Path | None = None,
    output_dir: Path | None = None,
) -> Path:
    """Stage and zip every file needed by the Flink driver and UDF worker."""
    repo_root = repo_root.resolve()
    source_dir = (repo_root / "streaming/flink").resolve()
    jar_path = (jar_path or source_dir / "target/pyflink-dependencies.jar").resolve()
    default_output_dir = (repo_root / "dist").resolve()
    output_dir = (output_dir or default_output_dir).resolve()
    staging_dir = output_dir / "flink-app"
    archive_path = output_dir / "flink-app.zip"
    sources = {
        "main.py": source_dir / "job.py",
        "requirements.txt": source_dir / "requirements.txt",
        "flink/__init__.py": source_dir / "__init__.py",
        "flink/window_logic.py": source_dir / "window_logic.py",
        "lib/pyflink-dependencies.jar": jar_path,
    }
    if output_dir == repo_root or output_dir in repo_root.parents:
        raise ValueError("Flink output directory cannot be the repository or its ancestor")
    if repo_root in output_dir.parents and output_dir != default_output_dir:
        raise ValueError("custom Flink output directory cannot be inside the repository")
    if any(staging_dir == source or staging_dir in source.parents for source in sources.values()):
        raise ValueError("Flink staging directory cannot contain a package source")
    if archive_path in sources.values():
        raise ValueError("Flink archive path cannot replace a package source")
    if archive_path.exists() and not archive_path.is_file():
        raise ValueError("Flink archive path must be a file")

    marker = output_dir / OUTPUT_MARKER
    marker_owned = marker.is_file() and marker.read_text() == OUTPUT_MARKER_CONTENT
    if (
        output_dir != default_output_dir
        and output_dir.exists()
        and any(output_dir.iterdir())
        and not marker_owned
    ):
        raise ValueError("custom Flink output directory is not empty and is not tool-owned")
    output_dir.mkdir(parents=True, exist_ok=True)
    marker.write_text(OUTPUT_MARKER_CONTENT)

    shutil.rmtree(staging_dir, ignore_errors=True)
    archive_path.unlink(missing_ok=True)
    missing = [str(path) for path in sources.values() if not path.is_file()]
    if missing:
        raise FileNotFoundError(f"Flink package sources are missing: {', '.join(missing)}")

    temporary_archive: Path | None = None
    try:
        staging_dir.mkdir(parents=True)
        for member in PACKAGE_MEMBERS:
            destination = staging_dir / member
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(sources[member], destination)

        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{archive_path.name}.",
            suffix=".tmp",
            dir=archive_path.parent,
        )
        os.close(descriptor)
        temporary_archive = Path(temporary_name)
        with zipfile.ZipFile(
            temporary_archive,
            mode="w",
            compression=zipfile.ZIP_DEFLATED,
            compresslevel=6,
        ) as archive:
            for member in PACKAGE_MEMBERS:
                archive.write(staging_dir / member, arcname=member)
        temporary_archive.replace(archive_path)
    except Exception:
        if temporary_archive is not None:
            temporary_archive.unlink(missing_ok=True)
        shutil.rmtree(staging_dir, ignore_errors=True)
        raise

    return archive_path


def main() -> None:
    archive = build_flink_package()
    print(
        f"packaged {archive.relative_to(REPO_ROOT)} "
        "(main.py + flink/window_logic.py + connector JAR)"
    )


if __name__ == "__main__":
    main()
