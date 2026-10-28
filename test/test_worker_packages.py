"""Check Gitea package publishing without contacting external registries."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

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
        "checksums.txt",
    ]
    for name in names:
        (artifacts / name).write_text("test artifact")
    calls = tmp_path / "calls.jsonl"
    curl = tmp_path / "curl"
    curl.write_text(
        f"#!{sys.executable}\n"
        "import json, os, sys\n"
        "from pathlib import Path\n"
        "path = Path(os.environ['MOCK_CURL_CALLS'])\n"
        "with path.open('a') as output:\n"
        "    output.write(json.dumps(sys.argv[1:]) + '\\n')\n"
        "sys.exit(int(os.environ.get('MOCK_CURL_EXIT', '0')))\n"
    )
    curl.chmod(0o700)
    env = {
        **os.environ,
        "PATH": f"{tmp_path}{os.pathsep}{os.defpath}",
        "GITEA_REGISTRY_TOKEN": "example:test-token",
        "MOCK_CURL_CALLS": str(calls),
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
        assert "--fail-with-body" in request
        assert "--max-time" in request
    assert "test-token" not in result.stdout + result.stderr


@pytest.mark.parametrize("missing", ["*.rpm", "*.tar.gz", "checksums.txt"])
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
