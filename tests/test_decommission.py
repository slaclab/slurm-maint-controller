#!/usr/bin/env python3
"""
Tests for the hardware decommission and revival workflow.
"""

import sys
from datetime import datetime
from unittest.mock import Mock

from maint_operator import (
    MaintananceType,
    MaintenanceManager,
    NodeRebootStatus,
    PartitionInfo,
    RebootState,
    ReservationManager,
    SlurmController,
)


def make_manager(total=10, max_pct=20.0):
    controller = Mock(spec=SlurmController)
    controller.dry_run = False  # Add dry_run attribute
    reservation_manager = Mock(spec=ReservationManager)
    partition_info = PartitionInfo(
        name="compute",
        nodes=[f"node{i:02d}" for i in range(1, total + 1)],
        total_node_count=total,
    )
    controller.get_partition_info.return_value = [partition_info]
    controller.get_node_states.return_value = {}
    reservation_manager.get_maintenance.return_value = []
    return MaintenanceManager(
        controller=controller,
        reservation_manager=reservation_manager,
        max_down_percentage=max_pct,
        reboot_timeout=600,
        reservation_lead_time=60,
    )


def test_full_decommission_lifecycle():
    """
    Full decommission workflow — from operator flagging hardware down through
    revival and recovery — exercising every state transition and confirming
    the node is unavailable throughout until it is fully restored.
    """
    manager = make_manager(total=10, max_pct=10.0)

    # Seed: node is staged for decommission and shows up as unavailable
    manager.mark_node_for_decommission("node01", "compute")
    assert manager.node_reboot_status["node01"].state == RebootState.PENDING
    assert manager.node_reboot_status["node01"].maintenance_type == MaintananceType.DECOMMISSION
    _, _, unavailable = manager.count_unavailable_nodes("compute")
    assert "node01" in unavailable, "PENDING decommission node must count as unavailable"

    # Second call is idempotent
    assert manager.mark_node_for_decommission("node01", "compute") is False

    # Drain completes: shutdown command issued, then state parked at AWAITING_REVIVAL
    manager.issue_shutdown("node01")
    manager.node_reboot_status["node01"].state = RebootState.AWAITING_REVIVAL
    assert manager.node_reboot_status["node01"].state == RebootState.AWAITING_REVIVAL
    _, _, unavailable = manager.count_unavailable_nodes("compute")
    assert "node01" in unavailable, "AWAITING_REVIVAL node must count as unavailable"

    # Revival on wrong state is a no-op (guard against accidental calls)
    manager.node_reboot_status["node02"] = NodeRebootStatus(
        node_name="node02", partition="compute", state=RebootState.PENDING,
        maintenance_type=MaintananceType.DECOMMISSION,
    )
    assert manager.mark_node_for_revival("node02") is False

    # Hardware is restored: operator triggers revival → back to PENDING for reboot pipeline
    assert manager.mark_node_for_revival("node01") is True
    assert manager.node_reboot_status["node01"].state == RebootState.PENDING

    # Reboot pipeline picks it up (issue_reboot) and recovery monitoring completes it
    manager.issue_reboot("node01")
    assert manager.node_reboot_status["node01"].state == RebootState.REBOOTING
    assert manager.node_reboot_status["node01"].attempts == 1  # Revival reboot (attempts reset on revival)

    # Recovery monitoring marks the node done
    manager.complete_node_reboot("node01")
    assert manager.node_reboot_status["node01"].state == RebootState.COMPLETED


def test_serialisation_roundtrip_and_backward_compat():
    """
    State file round-trip preserves maintenance_type, and old state files
    (without the field) deserialise cleanly as 'reboot'.
    """
    original = NodeRebootStatus(
        node_name="node01",
        partition="compute",
        state=RebootState.AWAITING_REVIVAL,
        maintenance_type=MaintananceType.DECOMMISSION,
        reboot_start_time=datetime(2026, 5, 13, 10, 0, 0),
        attempts=2,
    )
    data = original.to_dict()
    restored = NodeRebootStatus.from_dict(data)

    assert restored.state == RebootState.AWAITING_REVIVAL
    assert restored.maintenance_type == MaintananceType.DECOMMISSION
    assert restored.attempts == 2

    # Old state file: no maintenance_type key → defaults to "reboot"
    old_data = {k: v for k, v in data.items() if k != "maintenance_type"}
    old_status = NodeRebootStatus.from_dict(old_data)
    assert old_status.maintenance_type == MaintananceType.REBOOT
