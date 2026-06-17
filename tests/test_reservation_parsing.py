#!/usr/bin/env python3
"""
Unit tests for Reservation partition-aware parsing using node name extraction.

This test verifies that the Reservation class correctly extracts partition from
node names following the sdf{PARTITION}{###} convention.
"""

import pendulum

from maint_operator import Reservation, get_partition_from_node_name


def test_get_partition_from_node_name_standard():
    """Test partition extraction from standard sdf node names."""
    test_cases = [
        ("sdfgpu001", "gpu"),
        ("sdfgpu042", "gpu"),
        ("sdfcompute001", "compute"),
        ("sdfcompute123", "compute"),
        ("sdfmilano001", "milano"),
        ("sdftur042", "tur"),
    ]

    for node_name, expected_partition in test_cases:
        partition = get_partition_from_node_name(node_name)
        assert partition == expected_partition, \
            f"For '{node_name}': expected '{expected_partition}', got '{partition}'"


def test_get_partition_from_node_name_invalid():
    """Test handling of non-sdf node names."""
    test_cases = [
        "node01",
        "compute-node",
        "",
        "gpu001",  # Missing sdf prefix
        "xdfgpu001",  # Wrong prefix
    ]

    for node_name in test_cases:
        partition = get_partition_from_node_name(node_name)
        assert partition is None, \
            f"For '{node_name}': expected None, got '{partition}'"

def test_get_partition_from_node_name_edge_cases():
    """Test edge cases."""
    # No partition name (just sdf + numbers)
    assert get_partition_from_node_name("sdf001") is None

    # Just prefix
    assert get_partition_from_node_name("sdf") is None

    # No numbers (valid - partition name only)
    assert get_partition_from_node_name("sdfgpu") == "gpu"

    # Very long partition name
    assert get_partition_from_node_name("sdfverylongpartitionname123") == "verylongpartitionname"


def test_reservation_get_partition_from_node():
    """Test Reservation.get_partition() extracts from node name."""
    # Create reservation with sdf node name
    res = Reservation(
        name="maint:sdfgpu001",
        nodes=["sdfgpu001"],
        start_time=pendulum.now(),
        duration=pendulum.duration(hours=1)
    )

    # Verify partition extraction
    partition = res.get_partition()
    assert partition == "gpu", f"Expected 'gpu', got '{partition}'"


def test_reservation_get_partition_different_partitions():
    """Test partition extraction for various partition names."""
    test_cases = [
        ("maint:sdfcompute042", "sdfcompute042", "compute"),
        ("maint:sdfmilano123", "sdfmilano123", "milano"),
        ("maint:sdftur001", "sdftur001", "tur"),
    ]

    for res_name, node_name, expected_partition in test_cases:
        res = Reservation(
            name=res_name,
            nodes=[node_name],
            start_time=pendulum.now(),
            duration=pendulum.duration(hours=1)
        )

        partition = res.get_partition()
        assert partition == expected_partition, \
            f"For {node_name}: expected '{expected_partition}', got '{partition}'"


def test_reservation_get_partition_non_maintenance():
    """Test get_partition() with non-maintenance reservations."""

    # Non-maintenance reservation
    res = Reservation(
        name="user-reservation-123",
        nodes=["sdfgpu001"],
        start_time=pendulum.now(),
        duration=pendulum.duration(hours=1)
    )

    # Should still extract partition from node
    partition = res.get_partition()
    assert partition == "gpu", f"Expected 'gpu', got '{partition}'"


def test_reservation_get_partition_non_sdf_node():
    """Test get_partition() with non-sdf node names."""
    # Reservation with non-sdf node
    res = Reservation(
        name="maint:node01",
        nodes=["node01"],
        start_time=pendulum.now(),
        duration=pendulum.duration(hours=1)
    )

    # Should return None for non-sdf nodes
    partition = res.get_partition()
    assert partition is None, f"Expected None, got '{partition}'"
