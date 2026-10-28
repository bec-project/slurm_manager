"""Reject invalid resource requests before sending them to Slurm."""

import pytest
from pydantic import ValidationError

from slurm_manager.job_options import JobOptions


@pytest.mark.parametrize(
    "field,value",
    [
        ("tasks", 0),
        ("tasks", -1),
        ("tasks", True),
        ("tasks", 1.5),
        ("time_limit_minutes", 0),
        ("time_limit_minutes", -1),
        ("time_limit_minutes", "60"),
        ("partition", "   "),
        ("hold", False),
        ("time_limit", {"set": True, "number": 1}),
    ],
)
def test_invalid_or_internal_options_are_rejected(field, value):
    with pytest.raises(ValidationError):
        JobOptions(**{field: value})


def test_unset_options_preserve_slurm_defaults():
    assert JobOptions().to_rest() == {"tasks": 1}


def test_runtime_uses_minutes_and_slurm_optional_number_shape():
    options = JobOptions(partition="hour", tasks=2, time_limit_minutes=60)
    assert options.to_rest() == {
        "partition": "hour",
        "tasks": 2,
        "time_limit": {"set": True, "number": 60},
    }
    assert options.time_limit_minutes == 60


@pytest.mark.parametrize("partition", ["hour", "day", "week", "gpu"])
def test_supported_ra_partitions(partition):
    assert JobOptions(partition=partition).to_rest()["partition"] == partition


@pytest.mark.parametrize("partition", ["admin", "jupyter", "gpu-large", "unknown"])
def test_unsupported_partitions_are_rejected(partition):
    with pytest.raises(ValidationError):
        JobOptions(partition=partition)
