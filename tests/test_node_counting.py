#!/usr/bin/env python3
"""
Integration test for unavailable vs down node counting.

This test verifies that the MaintenanceManager correctly counts unavailable
nodes (maintenance-related) and down nodes (Slurm state) separately, and
combines them appropriately when checking limits.
"""

import sys
from unittest.mock import Mock

# Import the classes we need to test
from maint_operator import (
    MaintenanceManager,
    SlurmController,
    ReservationManager,
    NodeState,
    PartitionInfo,
    Reservation,
    RebootState,
    NodeRebootStatus,
)


def create_mock_reservation(name, nodes, is_active=True, is_starting_soon=False):
    """Create a mock reservation for testing."""
    res = Mock(spec=Reservation)
    res.name = name
    res.nodes = nodes
    res.is_active.return_value = is_active
    res.is_starting_soon.return_value = is_starting_soon
    res.is_active_or_starting_soon.return_value = is_active or is_starting_soon

    # Parse node name based on format
    if name.startswith("maint:"):
        parts = name.split(":", 1)
        if len(parts) == 2:
            # Format: maint:{node}
            node_name = parts[1]
            res.get_node_name.return_value = node_name
            # Extract partition from node name (sdf{PARTITION}{###})
            from maint_operator import get_partition_from_node_name
            res.get_partition.return_value = get_partition_from_node_name(node_name)
    else:
        res.get_node_name.return_value = nodes[0] if nodes else None
        res.get_partition.return_value = None

    return res


def test_count_unavailable_nodes():
    """Test counting unavailable nodes (maintenance-related)."""
    # Create mock objects
    controller = Mock(spec=SlurmController)
    reservation_manager = Mock(spec=ReservationManager)

    # Setup partition with sdf node names
    partition_info = PartitionInfo(
        name="compute",
        nodes=[f"sdfcompute{i:02d}" for i in range(1, 11)],
        total_node_count=10
    )
    controller.get_partition_info.return_value = [partition_info]

    # Create manager
    manager = MaintenanceManager(
        controller=controller,
        reservation_manager=reservation_manager,
        max_down_percentage=20.0,
        reboot_timeout=600,
        reservation_lead_time=60,
    )

    # Setup: 2 nodes with active reservations
    reservations = [
        create_mock_reservation("maint:sdfcompute01", ["sdfcompute01"], is_active=True),
        create_mock_reservation("maint:sdfcompute02", ["sdfcompute02"], is_active=True),
    ]
    reservation_manager.get_maintenance.return_value = reservations

    # Setup: 1 node in REBOOTING state, 1 node in PENDING state
    manager.node_reboot_status = {
        "sdfcompute03": NodeRebootStatus(
            node_name="sdfcompute03",
            partition="compute",
            state=RebootState.REBOOTING,
        ),
        "sdfcompute04": NodeRebootStatus(
            node_name="sdfcompute04",
            partition="compute",
            state=RebootState.PENDING,
        ),
    }

    # Count unavailable nodes
    unavailable_count, total_count, unavailable_nodes = manager.count_unavailable_nodes("compute")

    # Verify results
    assert total_count == 10, f"Expected total 10, got {total_count}"
    assert unavailable_count == 4, f"Expected 4 unavailable, got {unavailable_count}"
    assert unavailable_nodes == {"sdfcompute01", "sdfcompute02", "sdfcompute03", "sdfcompute04"}, \
        f"Unexpected unavailable nodes: {unavailable_nodes}"



def test_count_down_nodes_slurm():
    """Test counting down nodes according to Slurm."""
    # Create mock objects
    controller = Mock(spec=SlurmController)
    reservation_manager = Mock(spec=ReservationManager)

    # Setup partition with sdf node names
    partition_info = PartitionInfo(
        name="compute",
        nodes=[f"sdfcompute{i:02d}" for i in range(1, 11)],
        total_node_count=10
    )
    controller.get_partition_info.return_value = [partition_info]

    # Setup node states: 3 down nodes
    node_states = {
        "sdfcompute01": NodeState(name="sdfcompute01", state="IDLE"),
        "sdfcompute02": NodeState(name="sdfcompute02", state="ALLOCATED"),
        "sdfcompute03": NodeState(name="sdfcompute03", state="DOWN"),
        "sdfcompute04": NodeState(name="sdfcompute04", state="DRAIN"),
        "sdfcompute05": NodeState(name="sdfcompute05", state="IDLE+DRAIN"),
        "sdfcompute06": NodeState(name="sdfcompute06", state="IDLE"),
        "sdfcompute07": NodeState(name="sdfcompute07", state="MIXED"),
        "sdfcompute08": NodeState(name="sdfcompute08", state="IDLE"),
        "sdfcompute09": NodeState(name="sdfcompute09", state="IDLE"),
        "sdfcompute10": NodeState(name="sdfcompute10", state="IDLE"),
    }
    controller.get_node_states.return_value = node_states

    # Create manager
    manager = MaintenanceManager(
        controller=controller,
        reservation_manager=reservation_manager,
        max_down_percentage=20.0,
        reboot_timeout=600,
        reservation_lead_time=60,
    )

    # Count down nodes
    down_count, total_count, down_nodes = manager.count_down_nodes_slurm("compute")

    # Verify results
    assert total_count == 10, f"Expected total 10, got {total_count}"
    assert down_count == 3, f"Expected 3 down, got {down_count}"
    assert down_nodes == {"sdfcompute03", "sdfcompute04", "sdfcompute05"}, \
        f"Unexpected down nodes: {down_nodes}"



def test_combined_counting_no_overlap():
    """Test combined counting when there's no overlap between unavailable and down."""
    # Create mock objects
    controller = Mock(spec=SlurmController)
    reservation_manager = Mock(spec=ReservationManager)

    # Setup partition with 100 sdf nodes
    partition_info = PartitionInfo(
        name="compute",
        nodes=[f"sdfcompute{i:03d}" for i in range(1, 101)],
        total_node_count=100
    )
    controller.get_partition_info.return_value = [partition_info]

    # Setup: 5 unavailable nodes (maintenance)
    reservations = [
        create_mock_reservation(f"maint:sdfcompute{i:03d}", [f"sdfcompute{i:03d}"], is_active=True)
        for i in range(1, 6)
    ]
    reservation_manager.get_maintenance.return_value = reservations

    # Setup: 8 down nodes (not in maintenance)
    node_states = {}
    for i in range(1, 101):
        if 10 <= i < 18:  # nodes 010-017 are down
            node_states[f"sdfcompute{i:03d}"] = NodeState(name=f"sdfcompute{i:03d}", state="DOWN")
        else:
            node_states[f"sdfcompute{i:03d}"] = NodeState(name=f"sdfcompute{i:03d}", state="IDLE")
    controller.get_node_states.return_value = node_states

    # Create manager with 10% limit
    manager = MaintenanceManager(
        controller=controller,
        reservation_manager=reservation_manager,
        max_down_percentage=10.0,
        reboot_timeout=600,
        reservation_lead_time=60,
    )

    # Count both types
    unavailable_count, _, unavailable_nodes = manager.count_unavailable_nodes("compute")
    down_count, _, down_nodes = manager.count_down_nodes_slurm("compute")

    # Calculate combined
    combined_nodes = unavailable_nodes.union(down_nodes)
    combined_count = len(combined_nodes)
    overlap_count = len(unavailable_nodes.intersection(down_nodes))
    combined_percentage = (combined_count / 100) * 100

    # Verify
    assert unavailable_count == 5, f"Expected 5 unavailable, got {unavailable_count}"
    assert down_count == 8, f"Expected 8 down, got {down_count}"
    assert overlap_count == 0, f"Expected no overlap, got {overlap_count}"
    assert combined_count == 13, f"Expected 13 combined (5+8), got {combined_count}"
    assert combined_percentage == 13.0, f"Expected 13%, got {combined_percentage}%"

    # Check can_add_reservation (simulating adding a new node)
    can_add = manager.can_add_reservation("compute", "sdfcompute099")
    assert not can_add, "Should not be able to add reservation (13% >= 10% limit)"


def test_combined_counting_with_overlap():
    """Test combined counting when some nodes are both unavailable and down."""
    # Create mock objects
    controller = Mock(spec=SlurmController)
    reservation_manager = Mock(spec=ReservationManager)

    # Setup partition with 100 sdf nodes
    partition_info = PartitionInfo(
        name="compute",
        nodes=[f"sdfcompute{i:03d}" for i in range(1, 101)],
        total_node_count=100
    )
    controller.get_partition_info.return_value = [partition_info]

    # Setup: 5 unavailable nodes (maintenance)
    reservations = [
        create_mock_reservation(f"maint:sdfcompute{i:03d}", [f"sdfcompute{i:03d}"], is_active=True)
        for i in range(1, 6)
    ]
    reservation_manager.get_maintenance.return_value = reservations

    # Setup: 8 down nodes, 2 of which overlap with maintenance
    node_states = {}
    for i in range(1, 101):
        if i in [3, 4] or (10 <= i < 16):  # nodes 003, 004, 010-015 are down
            node_states[f"sdfcompute{i:03d}"] = NodeState(name=f"sdfcompute{i:03d}", state="DOWN")
        else:
            node_states[f"sdfcompute{i:03d}"] = NodeState(name=f"sdfcompute{i:03d}", state="IDLE")
    controller.get_node_states.return_value = node_states

    # Create manager with 15% limit
    manager = MaintenanceManager(
        controller=controller,
        reservation_manager=reservation_manager,
        max_down_percentage=15.0,
        reboot_timeout=600,
        reservation_lead_time=60,
    )

    # Count both types
    unavailable_count, _, unavailable_nodes = manager.count_unavailable_nodes("compute")
    down_count, _, down_nodes = manager.count_down_nodes_slurm("compute")

    # Calculate combined
    combined_nodes = unavailable_nodes.union(down_nodes)
    combined_count = len(combined_nodes)
    overlap_count = len(unavailable_nodes.intersection(down_nodes))
    overlap_nodes = unavailable_nodes.intersection(down_nodes)
    combined_percentage = (combined_count / 100) * 100

    # Verify
    assert unavailable_count == 5, f"Expected 5 unavailable, got {unavailable_count}"
    assert down_count == 8, f"Expected 8 down, got {down_count}"
    assert overlap_count == 2, f"Expected 2 overlap, got {overlap_count}"
    assert overlap_nodes == {"sdfcompute003", "sdfcompute004"}, f"Unexpected overlap: {overlap_nodes}"
    assert combined_count == 11, f"Expected 11 combined (5+8-2), got {combined_count}"
    assert combined_percentage == 11.0, f"Expected 11%, got {combined_percentage}%"

    # Check can_add_reservation (simulating adding a new node)
    can_add = manager.can_add_reservation("compute", "sdfcompute099")
    assert can_add, "Should be able to add reservation (11% < 15% limit)"


def test_all_down_states():
    """Test that all expected down states are recognized."""
    down_states_to_test = [
        "DOWN", "DOWN*", "DRAIN", "DRAINED", "DRAINING", "DRAINING*",
        "FAIL", "FAILING", "NOT_RESPONDING", "NO_RESPOND",
        "POWER_DOWN", "POWERING_DOWN", "IDLE+DRAIN", "MIXED+DRAIN"
    ]
    
    for state in down_states_to_test:
        node = NodeState(name="test-node", state=state)
        assert node.is_down(), f"State '{state}' should be recognized as down"


def test_count_unavailable_nodes_partition_filtering():
    """Test that count_unavailable_nodes filters reservations by partition."""
    # Create mock objects
    controller = Mock(spec=SlurmController)
    reservation_manager = Mock(spec=ReservationManager)

    # Setup partition with realistic sdf node names
    partition_info = PartitionInfo(
        name="compute",
        nodes=[f"sdfcompute{i:03d}" for i in range(1, 11)],
        total_node_count=10
    )
    controller.get_partition_info.return_value = [partition_info]

    # Create manager
    manager = MaintenanceManager(
        controller=controller,
        reservation_manager=reservation_manager,
        max_down_percentage=20.0,
        reboot_timeout=600,
        reservation_lead_time=60,
    )

    # Setup: Mix of compute and gpu partition reservations (sdf node names)
    reservations = [
        create_mock_reservation("maint:sdfcompute001", ["sdfcompute001"], is_active=True),
        create_mock_reservation("maint:sdfcompute002", ["sdfcompute002"], is_active=True),
        create_mock_reservation("maint:sdfgpu003", ["sdfgpu003"], is_active=True),  # Different partition!
        create_mock_reservation("maint:sdfgpu004", ["sdfgpu004"], is_active=True),  # Different partition!
    ]
    reservation_manager.get_maintenance.return_value = reservations

    # Count unavailable nodes for compute partition
    unavailable_count, total_count, unavailable_nodes = manager.count_unavailable_nodes("compute")

    # Verify: should only count compute partition reservations
    assert total_count == 10, f"Expected total 10, got {total_count}"
    assert unavailable_count == 2, f"Expected 2 unavailable (only compute partition), got {unavailable_count}"
    assert unavailable_nodes == {"sdfcompute001", "sdfcompute002"}, \
        f"Expected only compute nodes, got {unavailable_nodes}"



def test_count_unavailable_nodes_cross_partition_isolation():
    """Test that operators for different partitions don't interfere with each other."""
    # Create mock objects
    controller = Mock(spec=SlurmController)
    reservation_manager = Mock(spec=ReservationManager)

    # Setup GPU partition with sdf node names
    gpu_partition_info = PartitionInfo(
        name="gpu",
        nodes=["sdfgpu001", "sdfgpu002", "sdfgpu003"],
        total_node_count=3
    )

    # Setup compute partition with different sdf nodes (no overlap in this test)
    compute_partition_info = PartitionInfo(
        name="compute",
        nodes=["sdfcompute001", "sdfcompute002", "sdfcompute003", "sdfcompute004"],
        total_node_count=4
    )

    # Create managers for both partitions
    gpu_manager = MaintenanceManager(
        controller=controller,
        reservation_manager=reservation_manager,
        max_down_percentage=30.0,
        reboot_timeout=600,
        reservation_lead_time=60,
    )

    compute_manager = MaintenanceManager(
        controller=controller,
        reservation_manager=reservation_manager,
        max_down_percentage=30.0,
        reboot_timeout=600,
        reservation_lead_time=60,
    )

    # Setup: GPU operator created reservations for gpu nodes
    reservations = [
        create_mock_reservation("maint:sdfgpu001", ["sdfgpu001"], is_active=True),
        create_mock_reservation("maint:sdfgpu002", ["sdfgpu002"], is_active=True),
    ]
    reservation_manager.get_maintenance.return_value = reservations

    # Test GPU partition count
    controller.get_partition_info.return_value = [gpu_partition_info]
    gpu_unavailable_count, _, gpu_unavailable_nodes = gpu_manager.count_unavailable_nodes("gpu")

    # Test compute partition count
    controller.get_partition_info.return_value = [compute_partition_info]
    compute_unavailable_count, _, compute_unavailable_nodes = compute_manager.count_unavailable_nodes("compute")

    # Verify GPU partition sees its reservations
    assert gpu_unavailable_count == 2, f"GPU: Expected 2 unavailable, got {gpu_unavailable_count}"
    assert gpu_unavailable_nodes == {"sdfgpu001", "sdfgpu002"}, \
        f"GPU: Expected sdfgpu001, sdfgpu002, got {gpu_unavailable_nodes}"

    # Verify compute partition does NOT see GPU partition's reservations
    assert compute_unavailable_count == 0, \
        f"Compute: Expected 0 unavailable (gpu reservations shouldn't count), got {compute_unavailable_count}"
    assert compute_unavailable_nodes == set(), \
        f"Compute: Expected empty set, got {compute_unavailable_nodes}"


def test_count_unavailable_nodes_backward_compatibility():
    """Test backward compatibility with non-sdf node names."""
    # Create mock objects
    controller = Mock(spec=SlurmController)
    reservation_manager = Mock(spec=ReservationManager)

    # Setup partition with non-sdf node names (old style)
    partition_info = PartitionInfo(
        name="compute",
        nodes=[f"node{i:02d}" for i in range(1, 11)],
        total_node_count=10
    )
    controller.get_partition_info.return_value = [partition_info]

    # Create manager
    manager = MaintenanceManager(
        controller=controller,
        reservation_manager=reservation_manager,
        max_down_percentage=20.0,
        reboot_timeout=600,
        reservation_lead_time=60,
    )

    # Setup: Non-sdf format reservations (won't extract partition)
    reservations = [
        create_mock_reservation("maint:node01", ["node01"], is_active=True),  # No partition in node name
        create_mock_reservation("maint:node02", ["node02"], is_active=True),  # No partition in node name
    ]
    reservation_manager.get_maintenance.return_value = reservations

    # Count unavailable nodes
    unavailable_count, total_count, unavailable_nodes = manager.count_unavailable_nodes("compute")

    # Verify: Since get_partition_from_node_name returns None, filtering will exclude these
    # (res_partition != partition where None != "compute")
    assert total_count == 10, f"Expected total 10, got {total_count}"
    assert unavailable_count == 0, f"Expected 0 unavailable (non-sdf nodes filtered out), got {unavailable_count}"
    assert unavailable_nodes == set(), \
        f"Expected empty set, got {unavailable_nodes}"

def test_pending_reservations_capacity_tracking():
    """Test that pending reservations (approved but not yet in Slurm) are counted in capacity checks."""
    # Create mock objects
    controller = Mock(spec=SlurmController)
    reservation_manager = Mock(spec=ReservationManager)

    # Setup partition with 100 nodes
    partition_info = PartitionInfo(
        name="compute",
        nodes=[f"sdfcompute{i:03d}" for i in range(1, 101)],
        total_node_count=100
    )
    controller.get_partition_info.return_value = [partition_info]

    # Setup: No existing reservations in Slurm (simulating first iteration)
    reservation_manager.get_maintenance.return_value = []

    # Setup: No down nodes
    node_states = {}
    controller.get_node_states.return_value = node_states

    # Create manager with 10% limit (allows 10 nodes)
    manager = MaintenanceManager(
        controller=controller,
        reservation_manager=reservation_manager,
        max_down_percentage=10.0,
        reboot_timeout=600,
        reservation_lead_time=60,
        state_file=None,
        target_nodes=None
    )

    # Reset pending reservations (simulating iteration start)
    manager.reset_pending_reservations()

    # Simulate sequential approval of reservations
    approved_nodes = []
    for i in range(1, 15):  # Try to approve 14 nodes (should only approve 10)
        node_name = f"sdfcompute{i:03d}"
        can_add = manager.can_add_reservation("compute", node_name)

        if can_add:
            # Mark as pending immediately after approval
            manager.mark_reservation_pending(node_name)
            approved_nodes.append(node_name)
        else:
            # Should be rejected once we hit the limit
            break

    # Verify: Should have approved exactly 10 nodes (10% of 100)
    assert len(approved_nodes) == 10, \
        f"Expected to approve 10 nodes (10% limit), but approved {len(approved_nodes)}"

    # Verify: 11th node should be rejected
    node_11 = "sdfcompute011"
    can_add_11 = manager.can_add_reservation("compute", node_11)
    assert not can_add_11, \
        f"Should reject 11th node (would be 11%), but was approved"

    # Verify: After reset, same node should be approved again
    manager.reset_pending_reservations()
    can_add_after_reset = manager.can_add_reservation("compute", "sdfcompute001")
    assert can_add_after_reset, \
        "Should be able to approve node after reset (pending cleared)"
    