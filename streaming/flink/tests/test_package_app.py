"""Hermetic checks for the Managed Flink application zip."""

from __future__ import annotations

import os
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest

from flink.package_app import PACKAGE_MEMBERS, build_flink_package

ROOT = Path(__file__).parents[3]


def test_package_contains_driver_module_and_is_importable(tmp_path):
    fake_jar = tmp_path / "pyflink-dependencies.jar"
    fake_jar.write_bytes(b"fake connector jar")
    output_dir = tmp_path / "output"
    staging_dir = output_dir / "flink-app"
    archive_path = output_dir / "flink-app.zip"

    result = build_flink_package(
        repo_root=ROOT,
        jar_path=fake_jar,
        output_dir=output_dir,
    )

    assert result == archive_path
    (staging_dir / "stale.txt").write_text("must be removed")
    build_flink_package(repo_root=ROOT, jar_path=fake_jar, output_dir=output_dir)
    assert not (staging_dir / "stale.txt").exists()
    expected_members = (
        "main.py",
        "requirements.txt",
        "flink/__init__.py",
        "flink/window_logic.py",
        "lib/pyflink-dependencies.jar",
    )
    assert PACKAGE_MEMBERS == expected_members
    with zipfile.ZipFile(archive_path) as archive:
        assert tuple(archive.namelist()) == expected_members
        assert archive.read("main.py") == (ROOT / "streaming/flink/job.py").read_bytes()
        assert (
            archive.read("flink/window_logic.py")
            == (ROOT / "streaming/flink/window_logic.py").read_bytes()
        )

    env = {**os.environ, "PYTHONPATH": str(archive_path)}
    imported = subprocess.run(
        [
            sys.executable,
            "-S",
            "-c",
            "import flink.window_logic as module; print(module.__file__)",
        ],
        check=True,
        capture_output=True,
        text=True,
        env=env,
        cwd=tmp_path,
    )
    assert imported.stdout.strip() == f"{archive_path}/flink/window_logic.py"


def test_missing_source_invalidates_preexisting_archive(tmp_path):
    fake_jar = tmp_path / "pyflink-dependencies.jar"
    fake_jar.write_bytes(b"fake connector jar")
    output_dir = tmp_path / "output"
    archive_path = build_flink_package(repo_root=ROOT, jar_path=fake_jar, output_dir=output_dir)
    fake_jar.unlink()

    with pytest.raises(FileNotFoundError, match="package sources are missing"):
        build_flink_package(
            repo_root=ROOT,
            jar_path=fake_jar,
            output_dir=output_dir,
        )

    assert not archive_path.exists()


def test_zip_failure_leaves_no_uploadable_or_partial_artifact(tmp_path, monkeypatch):
    fake_jar = tmp_path / "pyflink-dependencies.jar"
    fake_jar.write_bytes(b"fake connector jar")
    output_dir = tmp_path / "output"
    staging_dir = output_dir / "flink-app"
    archive_path = build_flink_package(repo_root=ROOT, jar_path=fake_jar, output_dir=output_dir)
    real_write = zipfile.ZipFile.write

    def fail_on_window_logic(self, filename, arcname=None, *args, **kwargs):
        if arcname == "flink/window_logic.py":
            raise OSError("injected zip failure")
        return real_write(self, filename, arcname, *args, **kwargs)

    monkeypatch.setattr(zipfile.ZipFile, "write", fail_on_window_logic)

    with pytest.raises(OSError, match="injected zip failure"):
        build_flink_package(
            repo_root=ROOT,
            jar_path=fake_jar,
            output_dir=output_dir,
        )

    assert not archive_path.exists()
    assert not staging_dir.exists()
    assert list(output_dir.glob(".flink-app.zip.*.tmp")) == []


def test_package_refuses_repository_as_output(tmp_path):
    fake_jar = tmp_path / "pyflink-dependencies.jar"
    fake_jar.write_bytes(b"fake connector jar")
    job_before = (ROOT / "streaming/flink/job.py").read_bytes()

    with pytest.raises(ValueError, match="repository"):
        build_flink_package(repo_root=ROOT, jar_path=fake_jar, output_dir=ROOT)

    assert (ROOT / "streaming/flink/job.py").read_bytes() == job_before


def test_package_refuses_unowned_nonempty_output(tmp_path):
    fake_jar = tmp_path / "pyflink-dependencies.jar"
    fake_jar.write_bytes(b"fake connector jar")
    unrelated = tmp_path / "unrelated"
    unrelated.mkdir()
    keep = unrelated / "keep.txt"
    keep.write_text("do not delete")

    with pytest.raises(ValueError, match="not empty and is not tool-owned"):
        build_flink_package(repo_root=ROOT, jar_path=fake_jar, output_dir=unrelated)

    assert keep.read_text() == "do not delete"


def test_make_target_calls_tested_packager():
    makefile = (ROOT / "Makefile").read_text()
    target = makefile.split("flink-package:", 1)[1].split("\ne2e:", 1)[0]

    clean = "rm -rf dist/flink-app dist/flink-app.zip"
    jar = "$(MAKE) flink-jar"
    packager = "PYTHONPATH=streaming $(PY) -m flink.package_app"
    assert target.index(clean) < target.index(jar) < target.index(packager)
