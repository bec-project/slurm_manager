"""Reject invalid resource requests before sending them to Slurm."""

import pytest
from pydantic import ValidationError

from slurm_manager.clusters.job_options import JobOptions
from slurm_manager.clusters.ra import RAJobOptions


@pytest.mark.parametrize(
    "field,value",
    [
        ("tasks", 0),
        ("tasks", -1),
        ("tasks", True),
        ("tasks", 1.5),
        ("time_limit", 0),
        ("time_limit", -1),
        ("time_limit", "60"),
        ("partition", "   "),
        ("reservation", "   "),
        ("constraints", "   "),
        ("dependency", "   "),
        ("nodes", "   "),
        ("nodes", 2),
        ("required_nodes", "node01"),
        ("required_nodes", []),
        ("required_nodes", [" "]),
        ("tasks_per_node", 0),
        ("tasks_per_node", True),
        ("tasks_per_node", 65534),
        ("tasks_per_core", 0),
        ("tasks_per_core", True),
        ("tasks_per_core", 65536),
        ("tres_per_node", "   "),
        ("tres_per_task", "   "),
        ("cpus_per_task", 0),
        ("cpus_per_task", True),
        ("cpus_per_task", 65534),
        ("memory_per_cpu", 0),
        ("memory_per_node", -1),
        ("memory_per_node", "4096"),
        ("exclusive", 1),
        ("shared", "none"),
        ("shared", []),
        ("shared", ["none", "oversubscribe"]),
        ("shared", ["exclusive"]),
        ("hold", False),
        ("time_limit", {"set": True, "number": 1}),
    ],
)
def test_invalid_or_internal_options_are_rejected(field, value):
    with pytest.raises(ValidationError):
        JobOptions(**{field: value})


def test_unset_options_preserve_slurm_defaults():
    assert JobOptions().to_rest() == {"tasks": 1}


def test_required_nodes_use_the_rest_array_field():
    assert RAJobOptions(nodes="3", required_nodes=["node[01-02,04]"]).to_rest() == {
        "tasks": 1,
        "nodes": "3",
        "required_nodes": ["node[01-02,04]"],
    }


def test_per_node_layout_omits_an_implicit_total_task_count():
    assert JobOptions(nodes="2", tasks_per_node=4).to_rest() == {"nodes": "2", "tasks_per_node": 4}


@pytest.mark.parametrize("tasks_per_core", [1, 2, 65535])
def test_tasks_per_core_is_sent_as_a_rest_integer(tasks_per_core):
    assert RAJobOptions(tasks_per_core=tasks_per_core).to_rest() == {
        "tasks": 1,
        "tasks_per_core": tasks_per_core,
    }


def test_runtime_uses_minutes_and_slurm_optional_number_shape():
    options = JobOptions(partition="hour", tasks=2, time_limit=60)
    assert options.to_rest() == {
        "partition": "hour",
        "tasks": 2,
        "time_limit": {"set": True, "number": 60},
    }
    assert options.time_limit == 60


def test_cluster_job_options_serialize_reservation_cpus_memory_and_exclusive():
    options = RAJobOptions(
        reservation=" beamtime ", cpus_per_task=4, memory_per_cpu=2048, shared=["none"]
    )
    assert options.to_rest() == {
        "tasks": 1,
        "reservation": "beamtime",
        "cpus_per_task": 4,
        "memory_per_cpu": {"set": True, "number": 2048},
        "shared": ["none"],
    }


def test_exclusive_uses_slurm_sharing_mode():
    assert RAJobOptions(exclusive=True).to_rest() == {"tasks": 1, "shared": ["none"]}
    assert RAJobOptions(exclusive=False).to_rest() == {"tasks": 1}


def test_exclusive_cannot_be_combined_with_explicit_sharing():
    with pytest.raises(ValidationError, match="either exclusive=True or shared"):
        JobOptions(exclusive=True, shared=["oversubscribe"])


def test_job_options_forward_node_layout_resources_and_dependencies():
    options = RAJobOptions(
        nodes="2-4",
        tasks=4,
        tasks_per_node=2,
        constraints="a100",
        dependency="afterok:12345",
        tres_per_node="gres/gpu=2",
        tres_per_task="gres/gpu=1",
    )
    assert options.to_rest() == {
        "nodes": "2-4",
        "tasks": 4,
        "tasks_per_node": 2,
        "constraints": "a100",
        "dependency": "afterok:12345",
        "tres_per_node": "gres/gpu=2",
        "tres_per_task": "gres/gpu=1",
    }


@pytest.mark.parametrize("memory_mib", [0, 4096])
def test_memory_per_node_serializes_zero_and_explicit_amounts(memory_mib):
    assert JobOptions(memory_per_node=memory_mib).to_rest() == {
        "tasks": 1,
        "memory_per_node": {"set": True, "number": memory_mib},
    }


def test_memory_options_are_mutually_exclusive():
    with pytest.raises(ValidationError, match="either memory_per_cpu or memory_per_node"):
        JobOptions(memory_per_cpu=1024, memory_per_node=0)


@pytest.mark.parametrize("partition", ["hour", "day", "week", "gpu"])
def test_supported_ra_partitions(partition):
    assert RAJobOptions(partition=partition).to_rest()["partition"] == partition


@pytest.mark.parametrize("partition", ["admin", "jupyter", "gpu-large", "unknown"])
def test_unsupported_ra_partitions_are_rejected(partition):
    with pytest.raises(ValidationError):
        RAJobOptions.model_validate({"partition": partition})


def test_generic_job_options_accept_a_custom_partition():
    assert JobOptions(partition="batch").to_rest()["partition"] == "batch"
