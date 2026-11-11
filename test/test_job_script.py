"""Verify local scripts and configuration reach the worker without shell expansion."""

import json
import subprocess
import sys

import pytest

from slurm_manager.slurm_manager import SlurmManager


def test_local_scripts_are_passed_to_worker_as_json(tmp_path):
    workload = tmp_path / "work load.sh"
    activation = tmp_path / "activate.sh"
    workload.write_text('#!/bin/bash\nprintf "$HOME and $value"\n')
    activation.write_text("export TEST_VALUE='literal $value and spaces'\n")
    worker = tmp_path / "worker $(touch must-not-exist)"
    worker.write_text(
        f"#!{sys.executable}\nimport json,sys\n"
        "print(json.dumps({'args':sys.argv[1:], 'payload':json.load(sys.stdin)}))\n"
    )
    worker.chmod(0o700)
    script = SlurmManager._build_job_script(
        script_path=workload,
        env_path=activation,
        working_directory="/remote/work",
        job_id="job-id",
        worker_path=str(worker),
        redis_address="redis.example:6380",
    )
    result = subprocess.run(
        ["bash"], input=script, capture_output=True, text=True, check=True, cwd=tmp_path
    )
    received = json.loads(result.stdout)
    assert received["payload"] == {
        "job_id": "job-id",
        "script": workload.read_text(),
        "activation": activation.read_text(),
    }
    assert received["args"] == ["--redis-address", "redis.example:6380", "--redis-tls=true"]
    assert "job-id" not in received["args"]
    assert "job-id" not in script.splitlines()[1]
    assert str(workload) not in script
    assert not (tmp_path / "must-not-exist").exists()


def test_relative_remote_script_path_stays_bound_to_the_job_directory(tmp_path):
    remote_path = "-script $(touch unintended).sh"
    batch_script = SlurmManager._build_job_script(
        remote_script_path=remote_path, working_directory=str(tmp_path), job_id="job-id"
    )
    assert "--command" in batch_script.splitlines()[1]
    payload = json.loads(batch_script.splitlines()[2])

    workload = tmp_path / remote_path
    workload.write_text('printf "%s" "$0"\n')
    activation_directory = tmp_path / "other"
    activation_directory.mkdir()
    (activation_directory / remote_path).write_text("echo wrong-script\n")
    result = subprocess.run(
        ["/bin/bash", "-c", payload["script"]],
        cwd=activation_directory,
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.stdout == str(workload)
    assert not (activation_directory / "unintended").exists()


def test_missing_local_file_fails_before_packaging(tmp_path):
    with pytest.raises(FileNotFoundError):
        SlurmManager._build_job_script(
            script_path=tmp_path / "missing.sh", working_directory="/remote/work", job_id="job-id"
        )


def test_worker_path_must_be_absolute(tmp_path):
    with pytest.raises(ValueError, match="absolute"):
        SlurmManager._build_job_script(
            script_path=tmp_path / "unused.sh",
            env_path=tmp_path / "unused-env.sh",
            working_directory="/remote/work",
            job_id="job-id",
            worker_path="worker",
        )
