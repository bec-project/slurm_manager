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
        workload, activation, "job-id", worker_path=str(worker), redis_address="redis.example:6380"
    )
    result = subprocess.run(
        ["bash"], input=script, capture_output=True, text=True, check=True, cwd=tmp_path
    )
    received = json.loads(result.stdout)
    assert received["payload"] == {
        "script": workload.read_text(),
        "activation": activation.read_text(),
    }
    assert received["args"] == ["--job-id", "job-id", "--redis-address", "redis.example:6380"]
    assert str(workload) not in script
    assert not (tmp_path / "must-not-exist").exists()


def test_missing_local_file_fails_before_packaging(tmp_path):
    with pytest.raises(FileNotFoundError):
        SlurmManager._build_job_script(tmp_path / "missing.sh", tmp_path / "activate.sh", "job-id")


def test_worker_path_must_be_absolute(tmp_path):
    with pytest.raises(ValueError, match="absolute"):
        SlurmManager._build_job_script(
            tmp_path / "unused.sh", tmp_path / "unused-env.sh", "job-id", worker_path="worker"
        )
