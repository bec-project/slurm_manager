"""Check Gitea package publishing without contacting external registries."""

import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest


def write_checksums(artifacts):
    entries = []
    for path in sorted(artifacts.iterdir()):
        if path.name == "checksums.txt":
            continue
        entries.append(f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}\n")
    (artifacts / "checksums.txt").write_text("".join(entries))


PUBLISH_SCRIPT = Path(__file__).parents[1] / "worker/scripts/upload-packages.sh"


@pytest.fixture
def publishing_environment(tmp_path):
    artifacts = tmp_path / "artifacts"
    artifacts.mkdir()
    names = [
        "bec-slurm-worker_0.2.0_linux_amd64.tar.gz",
        "bec-slurm-worker_0.2.0_linux_arm64.tar.gz",
        "bec-slurm-worker-0.2.0-1.x86_64.rpm",
        "bec-slurm-worker-0.2.0-1.aarch64.rpm",
    ]
    for name in names:
        (artifacts / name).write_text(f"test artifact {name}")
    write_checksums(artifacts)
    calls = tmp_path / "calls.jsonl"
    curl = tmp_path / "curl"
    stub = Path(__file__).with_name("fixtures") / "curl_stub.py"
    curl.write_text(f"#!{sys.executable}\n" + stub.read_text())
    curl.chmod(0o700)
    env = {
        **os.environ,
        "PATH": f"{tmp_path}{os.pathsep}{os.defpath}",
        "GITEA_REGISTRY_TOKEN": "example:test-token",
        "MOCK_CURL_CALLS": str(calls),
        "MOCK_CURL_STATE": str(tmp_path / "registry.json"),
    }
    return artifacts, calls, env


def publish(artifacts, env, version="0.2.0"):
    return subprocess.run(
        ["bash", str(PUBLISH_SCRIPT), version, str(artifacts)],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def test_uploads_complete_bundle_and_indexes_rpms(publishing_environment):
    artifacts, calls, env = publishing_environment
    result = publish(artifacts, env)
    assert result.returncode == 0, result.stderr
    requests = [json.loads(line) for line in calls.read_text().splitlines()]
    assert len(requests) == 7
    generic = "https://gitea.psi.ch/api/packages/bec/generic/bec-slurm-worker/0.2.0/"
    assert {request[-1] for request in requests[:5]} == {
        generic + path.name for path in artifacts.iterdir()
    }
    assert all(
        request[-1] == "https://gitea.psi.ch/api/packages/bec/rpm/upload"
        for request in requests[5:]
    )
    for request in requests:
        assert request[request.index("--user") + 1] == "example:test-token"
        assert "--write-out" in request
        assert "--max-time" in request
    assert "test-token" not in result.stdout + result.stderr


@pytest.mark.parametrize(
    "missing", ["*.rpm", "*.tar.gz", "checksums.txt", "*.aarch64.rpm", "*linux_arm64.tar.gz"]
)
def test_incomplete_packages_are_rejected_before_upload(publishing_environment, missing):
    artifacts, calls, env = publishing_environment
    for path in artifacts.glob(missing):
        path.unlink()
    result = publish(artifacts, env)
    assert result.returncode != 0
    assert "refusing an incomplete upload" in result.stderr
    assert not calls.exists()


def test_upload_failure_stops_publication(publishing_environment):
    artifacts, calls, env = publishing_environment
    result = publish(artifacts, {**env, "MOCK_CURL_EXIT": "22"})
    assert result.returncode == 22
    assert len(calls.read_text().splitlines()) == 1


@pytest.mark.parametrize("version", ["../other-package", "v0.2.0", "0.2.0/extra"])
def test_invalid_version_cannot_change_upload_path(publishing_environment, version):
    artifacts, calls, env = publishing_environment
    result = publish(artifacts, env, version)
    assert result.returncode != 0
    assert "semantic version" in result.stderr
    assert not calls.exists()


def test_missing_credentials_are_rejected_before_upload(publishing_environment):
    artifacts, calls, env = publishing_environment
    env.pop("GITEA_REGISTRY_TOKEN")
    result = publish(artifacts, env)
    assert result.returncode != 0
    assert "GITEA_REGISTRY_TOKEN" in result.stderr
    assert not calls.exists()


@pytest.mark.parametrize("fail_at", [2, 6, 7])
def test_retry_resumes_partial_generic_and_rpm_uploads(publishing_environment, fail_at):
    artifacts, calls, env = publishing_environment
    env["MOCK_CURL_FAIL_AT"] = str(fail_at)
    first = publish(artifacts, env)
    assert first.returncode != 0
    assert "HTTP 503" in first.stderr

    second = publish(artifacts, env)
    assert second.returncode == 0, second.stderr
    assert "Identical package already published" in second.stdout
    state = json.loads(Path(env["MOCK_CURL_STATE"]).read_text())
    assert len(state["files"]) == 7
    assert any("--upload-file" not in json.loads(line) for line in calls.read_text().splitlines())


def test_successful_release_can_be_retried_without_overwriting(publishing_environment):
    artifacts, _, env = publishing_environment
    assert publish(artifacts, env).returncode == 0
    before = json.loads(Path(env["MOCK_CURL_STATE"]).read_text())["files"]
    result = publish(artifacts, env)
    assert result.returncode == 0, result.stderr
    assert json.loads(Path(env["MOCK_CURL_STATE"]).read_text())["files"] == before


@pytest.mark.parametrize("registry", ["generic", "rpm"])
def test_conflicting_remote_bytes_are_rejected(publishing_environment, registry):
    artifacts, _, env = publishing_environment
    assert publish(artifacts, env).returncode == 0
    state_path = Path(env["MOCK_CURL_STATE"])
    state = json.loads(state_path.read_text())
    url = next(url for url in state["files"] if f"/{registry}/" in url)
    state["files"][url] = b"different remote contents".hex()
    state_path.write_text(json.dumps(state))

    result = publish(artifacts, env)
    assert result.returncode != 0
    assert "Conflicting remote package" in result.stderr
    assert json.loads(state_path.read_text())["files"][url] == b"different remote contents".hex()


@pytest.mark.parametrize(
    "damage", ["missing_entry", "duplicate_entry", "unknown_entry", "changed_bytes"]
)
def test_invalid_manifest_or_local_checksum_stops_all_uploads(publishing_environment, damage):
    artifacts, calls, env = publishing_environment
    manifest = artifacts / "checksums.txt"
    lines = manifest.read_text().splitlines(keepends=True)
    if damage == "missing_entry":
        manifest.write_text("".join(lines[1:]))
    elif damage == "duplicate_entry":
        manifest.write_text("".join(lines + [lines[0]]))
    elif damage == "unknown_entry":
        manifest.write_text("".join(lines) + f"{'0' * 64}  unexpected.rpm\n")
    else:
        next(artifacts.glob("*.rpm")).write_text("corrupt package")

    result = publish(artifacts, env)
    assert result.returncode != 0
    assert not calls.exists()


def test_prerelease_rpm_retry_uses_rpm_metadata_version(publishing_environment):
    artifacts, _, env = publishing_environment
    for path in list(artifacts.iterdir()):
        if path.name == "checksums.txt":
            continue
        version = "0.2.0_dev.1" if path.suffix == ".rpm" else "0.2.0-dev.1"
        path.rename(path.with_name(path.name.replace("0.2.0", version)))
    write_checksums(artifacts)

    for _ in range(2):
        result = publish(artifacts, env, version="0.2.0-dev.1")
        assert result.returncode == 0, result.stderr
    files = json.loads(Path(env["MOCK_CURL_STATE"]).read_text())["files"]
    assert (
        "https://gitea.psi.ch/api/packages/bec/rpm/package/bec-slurm-worker/0.2.0~dev.1-1/x86_64"
        in files
    )
