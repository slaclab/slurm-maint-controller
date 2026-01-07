#!/usr/bin/env python3
"""
Integration test for unavailable vs down node counting.

This test verifies that the MaintenanceManager correctly counts unavailable
nodes (maintenance-related) and down nodes (Slurm state) separately, and
combines them appropriately when checking limits.
"""

import sys
from unittest.mock import Mock, MagicMock, patch
from datetime import datetime

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
import pendulum


def create_mock_reservation(name, nodes, is_active=True, is_starting_soon=False):
    """Create a mock reservation for testing."""
    res = Mock(spec=Reservation)
    res.name = name
    res.nodes = nodes
    res.is_active.return_value = is_active
    res.is_starting_soon.return_value = is_starting_soon
    res.is_active_or_starting_soon.return_value = is_active or is_starting_soon
    res.get_node_name.return_value = nodes[0] if nodes else None
    return res


def test_count_unavailable_nodes():
    """Test counting unavailable nodes (maintenance-related)."""
    print("Testing count_unavailable_nodes()...")
    
    # Create mock objects
    controller = Mock(spec=SlurmController)
    reservation_manager = Mock(spec=ReservationManager)
    
    # Setup partition with 10 nodes
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
    
    # Setup: 2 nodes with active reservations
    reservations = [
        create_mock_reservation("maint:node01", ["node01"], is_active=True),
        create_mock_reservation("maint:node02", ["node02"], is_active=True),
    ]
    reservation_manager.get_maintenance.return_value = reservations
    
    # Setup: 1 node in REBOOTING state, 1 node in PENDING state
    manager.node_reboot_status = {
        "node03": NodeRebootStatus(
            node_name="node03",
            partition="compute",
            state=RebootState.REBOOTING,
        ),
        "node04": NodeRebootStatus(
            node_name="node04",
            partition="compute",
            state=RebootState.PENDING,
        ),
    }
    
    # Count unavailable nodes
    unavailable_count, total_count, unavailable_nodes = manager.count_unavailable_nodes("compute")
    
    # Verify results
    assert total_count == 10, f"Expected total 10, got {total_count}"
    assert unavailable_count == 4, f"Expected 4 unavailable, got {unavailable_count}"
    assert unavailable_nodes == {"node01", "node02", "node03", "node04"}, \
        f"Unexpected unavailable nodes: {unavailable_nodes}"
    
    print("  ✓ Correctly counted 4 unavailable nodes (2 reservations + 2 reboot states)")
    print()


def test_count_down_nodes_slurm():
    """Test counting down nodes according to Slurm."""
    print("Testing count_down_nodes_slurm()...")
    
    # Create mock objects
    controller = Mock(spec=SlurmController)
    reservation_manager = Mock(spec=ReservationManager)
    
    # Setup partition with 10 nodes
    partition_info = PartitionInfo(
        name="compute",
        nodes=[f"node{i:02d}" for i in range(1, 11)],
        total_node_count=10
    )
    controller.get_partition_info.return_value = [partition_info]
    
    # Setup node states: 3 down nodes
    node_states = {
        "node01": NodeState(name="node01", state="IDLE"),
        "node02": NodeState(name="node02", state="ALLOCATED"),
        "node03": NodeState(name="node03", state="DOWN"),
        "node04": NodeState(name="node04", state="DRAIN"),
        "node05": NodeState(name="node05", state="IDLE+DRAIN"),
        "node06": NodeState(name="node06", state="IDLE"),
        "node07": NodeState(name="node07", state="MIXED"),
        "node08": NodeState(name="node08", state="IDLE"),
        "node09": NodeState(name="node09", state="IDLE"),
        "node10": NodeState(name="node10", state="IDLE"),
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
    assert down_nodes == {"node03", "node04", "node05"}, \
        f"Unexpected down nodes: {down_nodes}"
    
    print("  ✓ Correctly counted 3 down nodes (DOWN, DRAIN, IDLE+DRAIN)")
    print()


def test_combined_counting_no_overlap():
    """Test combined counting when there's no overlap between unavailable and down."""
    print("Testing combined counting (no overlap)...")
    
    # Create mock objects
    controller = Mock(spec=SlurmController)
    reservation_manager = Mock(spec=ReservationManager)
    
    # Setup partition with 100 nodes
    partition_info = PartitionInfo(
        name="compute",
        nodes=[f"node{i:03d}" for i in range(1, 101)],
        total_node_count=100
    )
    controller.get_partition_info.return_value = [partition_info]
    
    # Setup: 5 unavailable nodes (maintenance)
    reservations = [
        create_mock_reservation(f"maint:node{i:03d}", [f"node{i:03d}"], is_active=True)
        for i in range(1, 6)
    ]
    reservation_manager.get_maintenance.return_value = reservations
    
    # Setup: 8 down nodes (not in maintenance)
    node_states = {}
    for i in range(1, 101):
        if 10 <= i < 18:  # nodes 010-017 are down
            node_states[f"node{i:03d}"] = NodeState(name=f"node{i:03d}", state="DOWN")
        else:
            node_states[f"node{i:03d}"] = NodeState(name=f"node{i:03d}", state="IDLE")
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
    
    # Check can_add_reservation
    can_add = manager.can_add_reservation("compute")
    assert not can_add, "Should not be able to add reservation (13% > 10%)"
    
    print("  ✓ Unavailable: 5/100 (5%)")
    print("  ✓ Down: 8/100 (8%)")
    print("  ✓ Combined: 13/100 (13%) - no overlap")
    print("  ✓ Correctly blocked adding reservation (13% > 10% limit)")
    print()


def test_combined_counting_with_overlap():
    """Test combined counting when some nodes are both unavailable and down."""
    print("Testing combined counting (with overlap)...")
    
    # Create mock objects
    controller = Mock(spec=SlurmController)
    reservation_manager = Mock(spec=ReservationManager)
    
    # Setup partition with 100 nodes
    partition_info = PartitionInfo(
        name="compute",
        nodes=[f"node{i:03d}" for i in range(1, 101)],
        total_node_count=100
    )
    controller.get_partition_info.return_value = [partition_info]
    
    # Setup: 5 unavailable nodes (maintenance)
    reservations = [
        create_mock_reservation(f"maint:node{i:03d}", [f"node{i:03d}"], is_active=True)
        for i in range(1, 6)
    ]
    reservation_manager.get_maintenance.return_value = reservations
    
    # Setup: 8 down nodes, 2 of which overlap with maintenance
    node_states = {}
    for i in range(1, 101):
        if i in [3, 4] or (10 <= i < 16):  # nodes 003, 004, 010-015 are down
            node_states[f"node{i:03d}"] = NodeState(name=f"node{i:03d}", state="DOWN")
        else:
            node_states[f"node{i:03d}"] = NodeState(name=f"node{i:03d}", state="IDLE")
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
    assert overlap_nodes == {"node003", "node004"}, f"Unexpected overlap: {overlap_nodes}"
    assert combined_count == 11, f"Expected 11 combined (5+8-2), got {combined_count}"
    assert combined_percentage == 11.0, f"Expected 11%, got {combined_percentage}%"
    
    # Check can_add_reservation
    can_add = manager.can_add_reservation("compute")
    assert can_add, "Should be able to add reservation (11% < 15%)"
    
    print("  ✓ Unavailable: 5/100 (5%)")
    print("  ✓ Down: 8/100 (8%)")
    print("  ✓ Overlap: 2 nodes (node003, node004)")
    print("  ✓ Combined: 11/100 (11%) = 5 + 8 - 2")
    print("  ✓ Correctly allowed adding reservation (11% < 15% limit)")
    print()


def test_all_down_states():
    """Test that all expected down states are recognized."""
    print("Testing all down states recognition...")
    
    down_states_to_test = [
        "DOWN", "DOWN*", "DRAIN", "DRAINED", "DRAINING", "DRAINING*",
        "FAIL", "FAILING", "NOT_RESPONDING", "NO_RESPOND",
        "POWER_DOWN", "POWERING_DOWN", "IDLE+DRAIN", "MIXED+DRAIN"
    ]
    
    for state in down_states_to_test:
        node = NodeState(name="test-node", state=state)
        assert node.is_down(), f"State '{state}' should be recognized as down"
    
    print(f"  ✓ All {len(down_states_to_test)} down states correctly recognized")
    print()


def run_all_tests():
    """Run all integration tests."""
    print("=" * 70)
    print("Running Node Counting Integration Tests")
    print("=" * 70)
    print()
    
    try:
        test_count_unavailable_nodes()
        test_count_down_nodes_slurm()
        test_combined_counting_no_overlap()
        test_combined_counting_with_overlap()
        test_all_down_states()
        
        print("=" * 70)
        print("🎉 All integration tests passed!")
        print("=" * 70)
        return 0
        
    except AssertionError as e:
        print()
        print("=" * 70)
        print(f"❌ Test failed: {e}")
        print("=" * 70)
        return 1
    except Exception as e:
        print()
        print("=" * 70)
        print(f"❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        print("=" * 70)
        return 1


if __name__ == "__main__":
    sys.exit(run_all_tests())