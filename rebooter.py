#!/usr/bin/env python3.9
"""
Find Slurm Nodes in Maintenance Reservations

This script periodically queries all Slurm reservations and identifies nodes
that are in maintenance reservations (format: maint:<nodename>).
"""

import json
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Set

import click
from loguru import logger


@dataclass
class PartitionInfo:
    """Information about a Slurm partition and its nodes."""

    name: str
    nodes: List[str] = field(default_factory=list)
    total_node_count: int = 0


class RebootState(Enum):
    """States for node reboot tracking."""

    PENDING = "pending"  # Node identified for reboot but not yet started
    REBOOTING = "rebooting"  # Reboot command issued, waiting for node to return
    COMPLETED = "completed"  # Node has successfully rebooted
    FAILED = "failed"  # Reboot failed or timed out


@dataclass
class NodeRebootStatus:
    """Tracks the reboot status of a node."""

    node_name: str
    partition: str
    state: RebootState
    reboot_start_time: Optional[datetime] = None
    reboot_complete_time: Optional[datetime] = None
    attempts: int = 0
    max_attempts: int = 3

    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "node_name": self.node_name,
            "partition": self.partition,
            "state": self.state.value,
            "reboot_start_time": self.reboot_start_time.isoformat()
            if self.reboot_start_time
            else None,
            "reboot_complete_time": self.reboot_complete_time.isoformat()
            if self.reboot_complete_time
            else None,
            "attempts": self.attempts,
            "max_attempts": self.max_attempts,
        }

    @staticmethod
    def from_dict(data: Dict) -> "NodeRebootStatus":
        """Create from dictionary loaded from JSON."""
        return NodeRebootStatus(
            node_name=data["node_name"],
            partition=data["partition"],
            state=RebootState(data["state"]),
            reboot_start_time=datetime.fromisoformat(data["reboot_start_time"])
            if data["reboot_start_time"]
            else None,
            reboot_complete_time=datetime.fromisoformat(data["reboot_complete_time"])
            if data["reboot_complete_time"]
            else None,
            attempts=data["attempts"],
            max_attempts=data["max_attempts"],
        )


@dataclass
class MaintenanceReservation:
    """Represents a maintenance reservation for a node."""

    name: str
    node: str
    start_time: str
    end_time: str
    state: str

    def __str__(self) -> str:
        return f"Reservation: {self.name}, Node: {self.node}, Start: {self.start_time}, End: {self.end_time}, State: {self.state}"


class RebootManager:
    """Manages node reboots with partition awareness and rate limiting."""

    def __init__(
        self,
        max_reboot_percentage: float = 10.0,
        reboot_timeout: int = 600,
        state_file: Optional[str] = None,
    ):
        """
        Initialize the reboot manager.

        Args:
            max_reboot_percentage: Maximum percentage of nodes that can be rebooting at once
            reboot_timeout: Timeout in seconds to wait for a node to come back up
            state_file: Path to JSON file for persisting reboot state
        """
        self.max_reboot_percentage = max_reboot_percentage
        self.reboot_timeout = reboot_timeout
        self.state_file = state_file
        self.node_reboot_status: Dict[str, NodeRebootStatus] = {}

        # Load state from file if it exists
        if self.state_file:
            self.load_state()

    def expand_slurm_nodelist(self, nodelist: str) -> Set[str]:
        """
        Expand a Slurm nodelist into individual node names.

        Args:
            nodelist: Slurm nodelist string (e.g., "node[1-3,5]" or "node1,node2")

        Returns:
            Set of individual node names.
        """
        try:
            result = subprocess.run(
                ["scontrol", "show", "hostnames", nodelist],
                capture_output=True,
                text=True,
                check=True,
            )
            nodes = {
                node.strip()
                for node in result.stdout.strip().split("\n")
                if node.strip()
            }
            logger.debug(f"Expanded Slurm nodelist '{nodelist}' to {len(nodes)} nodes")
            return nodes
        except subprocess.CalledProcessError as e:
            logger.warning(f"Failed to expand Slurm nodelist '{nodelist}': {e.stderr}")
            # Fall back to treating it as a single node name
            return {nodelist}

    def get_partition_info(
        self, partition_name: Optional[str] = None
    ) -> List[PartitionInfo]:
        """
        Get information about Slurm partitions and their nodes using sinfo.

        Args:
            partition_name: Specific partition to query, or None for all partitions

        Returns:
            List of PartitionInfo objects.
        """
        try:
            cmd = ["sinfo", "-h", "-o", "%P|%N"]
            if partition_name:
                cmd.extend(["-p", partition_name])

            logger.debug(f"Running command: {' '.join(cmd)}")

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True,
            )

            partition_nodes: Dict[str, Set[str]] = {}

            for line in result.stdout.strip().split("\n"):
                if not line.strip():
                    continue

                parts = line.split("|")
                if len(parts) != 2:
                    continue

                # Remove asterisk from default partition
                partition = parts[0].rstrip("*")
                nodes_str = parts[1]

                # Expand node list (e.g., "node[1-3]" -> ["node1", "node2", "node3"])
                nodes = self._expand_node_list(nodes_str)

                if partition not in partition_nodes:
                    partition_nodes[partition] = set()
                partition_nodes[partition].update(nodes)

            # Convert to PartitionInfo objects
            partition_info_list = []
            for partition, nodes in partition_nodes.items():
                node_list = sorted(list(nodes))
                partition_info_list.append(
                    PartitionInfo(
                        name=partition, nodes=node_list, total_node_count=len(node_list)
                    )
                )

            logger.info(f"Found {len(partition_info_list)} partition(s)")
            for pinfo in partition_info_list:
                logger.debug(
                    f"  Partition '{pinfo.name}': {pinfo.total_node_count} nodes"
                )

            return partition_info_list

        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to get partition info: {e.stderr}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error getting partition info: {e}")
            return []

    def _expand_node_list(self, nodes_str: str) -> List[str]:
        """
        Expand a Slurm node list into individual node names.

        Args:
            nodes_str: Node list string (e.g., "node[1-3,5]" or "node1,node2")

        Returns:
            List of individual node names.
        """
        # Use scontrol show hostnames to expand the node list
        try:
            result = subprocess.run(
                ["scontrol", "show", "hostnames", nodes_str],
                capture_output=True,
                text=True,
                check=True,
            )
            return [
                node.strip()
                for node in result.stdout.strip().split("\n")
                if node.strip()
            ]
        except subprocess.CalledProcessError:
            # If scontrol fails, return the original string as a single node
            logger.warning(f"Failed to expand node list: {nodes_str}")
            return [nodes_str]

    def can_reboot_node(self, node_name: str, partition: str) -> bool:
        """
        Check if a node can be rebooted without exceeding the maximum reboot percentage.

        Args:
            node_name: Name of the node to check
            partition: Partition the node belongs to

        Returns:
            True if the node can be rebooted, False otherwise.
        """
        # Get partition info
        partition_info_list = self.get_partition_info(partition)
        if not partition_info_list:
            logger.error(f"Could not get info for partition '{partition}'")
            return False

        partition_info = partition_info_list[0]
        total_nodes = partition_info.total_node_count

        if total_nodes == 0:
            logger.error(f"Partition '{partition}' has no nodes")
            return False

        # Count nodes currently rebooting in this partition
        rebooting_count = sum(
            1
            for status in self.node_reboot_status.values()
            if status.partition == partition and status.state == RebootState.REBOOTING
        )

        # Calculate current percentage
        current_percentage = (rebooting_count / total_nodes) * 100

        logger.debug(
            f"Partition '{partition}': {rebooting_count}/{total_nodes} nodes rebooting "
            f"({current_percentage:.1f}% of max {self.max_reboot_percentage}%)"
        )

        # Check if we can add another node
        if current_percentage >= self.max_reboot_percentage:
            logger.debug(
                f"Cannot reboot '{node_name}': would exceed max reboot percentage"
            )
            return False

        return True

    def mark_node_for_reboot(self, node_name: str, partition: str) -> bool:
        """
        Mark a node as pending reboot.

        Args:
            node_name: Name of the node to mark
            partition: Partition the node belongs to

        Returns:
            True if the node was successfully marked, False otherwise.
        """
        if node_name in self.node_reboot_status:
            status = self.node_reboot_status[node_name]
            if status.state in [RebootState.REBOOTING, RebootState.PENDING]:
                logger.debug(
                    f"Node '{node_name}' already marked for reboot (state: {status.state.value})"
                )
                return False

        self.node_reboot_status[node_name] = NodeRebootStatus(
            node_name=node_name, partition=partition, state=RebootState.PENDING
        )
        logger.info(f"Marked node '{node_name}' (partition: {partition}) for reboot")
        return True

    def issue_reboot(self, node_name: str) -> bool:
        """
        Issue a reboot command to a node (STUB - does not actually reboot yet).

        Args:
            node_name: Name of the node to reboot

        Returns:
            True if the reboot command was successfully issued, False otherwise.
        """
        if node_name not in self.node_reboot_status:
            logger.error(f"Node '{node_name}' not found in reboot status tracking")
            return False

        status = self.node_reboot_status[node_name]

        if status.state != RebootState.PENDING:
            logger.warning(
                f"Node '{node_name}' is not in PENDING state (current: {status.state.value})"
            )
            return False

        # STUB: This is where you would actually issue the reboot command
        # For example: subprocess.run(["ssh", node_name, "sudo", "reboot"])
        logger.info(f"[STUB] Issuing reboot command to node '{node_name}'")

        # Update status
        status.state = RebootState.REBOOTING
        status.reboot_start_time = datetime.now()
        status.attempts += 1

        logger.info(
            f"Node '{node_name}' reboot initiated (attempt {status.attempts}/{status.max_attempts})"
        )
        return True

    def monitor_node_recovery(self, node_name: str) -> bool:
        """
        Check if a node has recovered from reboot (STUB - does not actually check yet).

        Args:
            node_name: Name of the node to monitor

        Returns:
            True if the node is back up, False otherwise.
        """
        if node_name not in self.node_reboot_status:
            logger.error(f"Node '{node_name}' not found in reboot status tracking")
            return False

        status = self.node_reboot_status[node_name]

        if status.state != RebootState.REBOOTING:
            logger.debug(
                f"Node '{node_name}' is not in REBOOTING state (current: {status.state.value})"
            )
            return status.state == RebootState.COMPLETED

        # STUB: This is where you would actually check if the node is up
        # For example: Check if node responds to ping or SSH
        # For example: Check sinfo to see if node state is idle/allocated
        logger.debug(f"[STUB] Checking if node '{node_name}' has recovered from reboot")

        # Check for timeout
        if status.reboot_start_time:
            elapsed = (datetime.now() - status.reboot_start_time).total_seconds()
            if elapsed > self.reboot_timeout:
                logger.warning(
                    f"Node '{node_name}' reboot timed out after {elapsed:.0f}s "
                    f"(timeout: {self.reboot_timeout}s)"
                )

                if status.attempts >= status.max_attempts:
                    status.state = RebootState.FAILED
                    logger.error(
                        f"Node '{node_name}' failed after {status.attempts} attempts"
                    )
                else:
                    # Reset to pending for retry
                    status.state = RebootState.PENDING
                    logger.info(f"Node '{node_name}' will be retried")

                return False

        # STUB: For now, we return False (node not yet recovered)
        # In a real implementation, this would check actual node status
        return False

    def complete_node_reboot(self, node_name: str) -> None:
        """
        Mark a node's reboot as completed.

        Args:
            node_name: Name of the node that completed reboot
        """
        if node_name not in self.node_reboot_status:
            logger.error(f"Node '{node_name}' not found in reboot status tracking")
            return

        status = self.node_reboot_status[node_name]
        status.state = RebootState.COMPLETED
        status.reboot_complete_time = datetime.now()

        if status.reboot_start_time:
            duration = (
                status.reboot_complete_time - status.reboot_start_time
            ).total_seconds()
            logger.info(f"Node '{node_name}' reboot completed in {duration:.0f}s")
        else:
            logger.info(f"Node '{node_name}' reboot completed")

    def get_nodes_by_state(
        self, state: RebootState, partition: Optional[str] = None
    ) -> List[str]:
        """
        Get all nodes in a specific state.

        Args:
            state: RebootState to filter by
            partition: Optional partition to filter by

        Returns:
            List of node names.
        """
        nodes = []
        for node_name, status in self.node_reboot_status.items():
            if status.state == state:
                if partition is None or status.partition == partition:
                    nodes.append(node_name)
        return nodes

    def get_reboot_summary(self, partition: Optional[str] = None) -> Dict[str, int]:
        """
        Get a summary of reboot states.

        Args:
            partition: Optional partition to filter by

        Returns:
            Dictionary mapping state names to counts.
        """
        summary = {state.value: 0 for state in RebootState}

        for status in self.node_reboot_status.values():
            if partition is None or status.partition == partition:
                summary[status.state.value] += 1

        return summary

    def save_state(self) -> None:
        """Save current reboot state to JSON file."""
        if not self.state_file:
            return

        try:
            state_data = {
                "timestamp": datetime.now().isoformat(),
                "max_reboot_percentage": self.max_reboot_percentage,
                "reboot_timeout": self.reboot_timeout,
                "nodes": {
                    node_name: status.to_dict()
                    for node_name, status in self.node_reboot_status.items()
                },
            }

            # Write to temporary file first, then rename (atomic operation)
            temp_file = f"{self.state_file}.tmp"
            with open(temp_file, "w") as f:
                json.dump(state_data, f, indent=2)

            # Atomic rename
            os.replace(temp_file, self.state_file)

            logger.debug(
                f"Saved state to {self.state_file} ({len(self.node_reboot_status)} nodes)"
            )

        except Exception as e:
            logger.error(f"Failed to save state to {self.state_file}: {e}")

    def load_state(self) -> None:
        """Load reboot state from JSON file."""
        if not self.state_file or not os.path.exists(self.state_file):
            logger.debug(f"No state file found at {self.state_file}, starting fresh")
            return

        try:
            with open(self.state_file, "r") as f:
                state_data = json.load(f)

            # Validate state file structure
            if "nodes" not in state_data:
                logger.warning(
                    f"Invalid state file format in {self.state_file}, ignoring"
                )
                return

            # Load node status
            loaded_count = 0
            for node_name, node_data in state_data["nodes"].items():
                try:
                    self.node_reboot_status[node_name] = NodeRebootStatus.from_dict(
                        node_data
                    )
                    loaded_count += 1
                except Exception as e:
                    logger.warning(f"Failed to load state for node {node_name}: {e}")

            saved_timestamp = state_data.get("timestamp", "unknown")
            logger.info(
                f"Loaded state from {self.state_file} (saved: {saved_timestamp}, {loaded_count} nodes)"
            )

            # Log summary of loaded state
            summary = self.get_reboot_summary()
            if any(count > 0 for count in summary.values()):
                logger.info("Restored reboot state:")
                for state, count in summary.items():
                    if count > 0:
                        logger.info(f"  {state}: {count}")

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse state file {self.state_file}: {e}")
        except Exception as e:
            logger.error(f"Failed to load state from {self.state_file}: {e}")

    def clear_completed_nodes(self, max_age_hours: int = 24) -> int:
        """
        Remove completed/failed nodes from state to prevent file from growing indefinitely.

        Args:
            max_age_hours: Remove nodes completed/failed more than this many hours ago

        Returns:
            Number of nodes removed
        """
        cutoff_time = datetime.now().timestamp() - (max_age_hours * 3600)
        nodes_to_remove = []

        for node_name, status in self.node_reboot_status.items():
            if status.state in [RebootState.COMPLETED, RebootState.FAILED]:
                # Check if old enough to remove
                completion_time = status.reboot_complete_time
                if not completion_time:
                    # No completion time, use start time if available
                    completion_time = status.reboot_start_time

                if completion_time and completion_time.timestamp() < cutoff_time:
                    nodes_to_remove.append(node_name)

        # Remove old nodes
        for node_name in nodes_to_remove:
            del self.node_reboot_status[node_name]

        if nodes_to_remove:
            logger.info(
                f"Cleaned up {len(nodes_to_remove)} old completed/failed nodes from state"
            )

        return len(nodes_to_remove)


class MaintenanceNodeFinder:
    """Finds and tracks nodes in maintenance reservations."""

    def __init__(self, reboot_manager: Optional[RebootManager] = None):
        """
        Initialize the maintenance node finder.

        Args:
            reboot_manager: Optional RebootManager for handling reboots
        """
        self.last_seen_reservations: Set[str] = set()
        self.reboot_manager = reboot_manager

    def get_all_reservations(self) -> List[Dict[str, str]]:
        """
        Query Slurm for all active reservations.

        Returns:
            List of dictionaries containing reservation information.
        """
        try:
            cmd = ["scontrol", "show", "reservation", "-o"]
            logger.debug(f"Running command: {' '.join(cmd)}")

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True,
            )

            reservations = []
            for line in result.stdout.strip().split("\n"):
                if not line.strip():
                    continue

                # Parse the output line into a dictionary
                reservation = self._parse_reservation_line(line)
                if reservation:
                    reservations.append(reservation)

            logger.debug(f"Found {len(reservations)} total reservations")
            return reservations

        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to get reservations: {e.stderr}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error getting reservations: {e}")
            return []

    def _parse_reservation_line(self, line: str) -> Optional[Dict[str, str]]:
        """
        Parse a single reservation line from scontrol output.

        Args:
            line: A line of output from scontrol show reservation

        Returns:
            Dictionary with reservation fields or None if parsing fails.
        """
        try:
            reservation = {}

            # Parse key=value pairs, handling values with spaces
            pattern = r"(\w+)=([^\s]+(?:\s+[^\s=]+)*?)(?=\s+\w+=|$)"
            matches = re.finditer(pattern, line)

            for match in matches:
                key = match.group(1)
                value = match.group(2).strip()
                reservation[key] = value

            return reservation

        except Exception as e:
            logger.warning(f"Failed to parse reservation line: {e}")
            return None

    def filter_maintenance_reservations(
        self, reservations: List[Dict[str, str]]
    ) -> List[MaintenanceReservation]:
        """
        Filter reservations to find maintenance reservations (format: maint:<nodename>).

        Args:
            reservations: List of all reservations

        Returns:
            List of MaintenanceReservation objects.
        """
        maint_reservations = []

        for res in reservations:
            name = res.get("ReservationName", "")

            # Check if reservation name matches maint:<nodename> format
            if name.startswith("maint:"):
                node_name = name.split(":", 1)[1] if ":" in name else ""
                nodes = res.get("Nodes", "")

                # Validate that the reservation contains a single node
                if nodes and node_name:
                    maint_res = MaintenanceReservation(
                        name=name,
                        node=nodes,
                        start_time=res.get("StartTime", "N/A"),
                        end_time=res.get("EndTime", "N/A"),
                        state=res.get("State", "N/A"),
                    )
                    maint_reservations.append(maint_res)

                    # Validate node name matches reservation name
                    if node_name not in nodes:
                        logger.warning(
                            f"Maintenance reservation '{name}' contains nodes '{nodes}' "
                            f"that don't match expected node '{node_name}'"
                        )

        return maint_reservations

    def find_nodes_in_maintenance(
        self, partition: Optional[str] = None, nodelist: Optional[str] = None
    ) -> List[MaintenanceReservation]:
        """
        Find all nodes currently in maintenance reservations.

        Args:
            partition: Optional partition to filter nodes by
            nodelist: Optional Slurm nodelist to filter nodes
                     Examples: node[1-10,15], node001,node002, sdfmilan[001-200,207]

        Returns:
            List of MaintenanceReservation objects.
        """
        reservations = self.get_all_reservations()
        maint_reservations = self.filter_maintenance_reservations(reservations)

        # Apply partition filter if specified
        if partition:
            partition_info = (
                self.reboot_manager.get_partition_info(partition)
                if self.reboot_manager
                else []
            )
            if partition_info:
                partition_nodes = set(partition_info[0].nodes)
                maint_reservations = [
                    res
                    for res in maint_reservations
                    if res.node in partition_nodes
                    or any(node in partition_nodes for node in res.node.split(","))
                ]
                logger.debug(
                    f"Filtered to {len(maint_reservations)} reservations in partition '{partition}'"
                )

        # Apply nodelist filter if specified
        if nodelist:
            # Slurm nodelist format - expand and match exactly
            logger.debug(f"Using Slurm nodelist: '{nodelist}'")
            if self.reboot_manager:
                target_nodes = self.reboot_manager.expand_slurm_nodelist(nodelist)
                maint_reservations = [
                    res for res in maint_reservations if res.node in target_nodes
                ]
                logger.debug(
                    f"Filtered to {len(maint_reservations)} reservations matching nodelist '{nodelist}'"
                )

        logger.info(f"Found {len(maint_reservations)} maintenance reservations")

        return maint_reservations

    def track_changes(self, current_reservations: List[MaintenanceReservation]) -> None:
        """
        Track and log changes in maintenance reservations.

        Args:
            current_reservations: Current list of maintenance reservations
        """
        current_names = {res.name for res in current_reservations}

        # Find new reservations
        new_reservations = current_names - self.last_seen_reservations
        if new_reservations:
            logger.info(
                f"New maintenance reservations detected: {', '.join(sorted(new_reservations))}"
            )
            for res in current_reservations:
                if res.name in new_reservations:
                    logger.info(f"  {res}")

        # Find removed reservations
        removed_reservations = self.last_seen_reservations - current_names
        if removed_reservations:
            logger.info(
                f"Maintenance reservations removed: {', '.join(sorted(removed_reservations))}"
            )

        # Update tracking set
        self.last_seen_reservations = current_names


def run_periodic_check(
    interval: int,
    max_iterations: Optional[int] = None,
    partition: Optional[str] = None,
    nodelist: Optional[str] = None,
    max_reboot_percentage: float = 10.0,
    reboot_timeout: int = 600,
    enable_reboots: bool = False,
    state_file: Optional[str] = None,
) -> None:
    """
    Periodically check for nodes in maintenance reservations and manage reboots.

    Args:
        interval: Time in seconds between checks
        max_iterations: Maximum number of iterations (None for infinite)
        partition: Optional partition to filter nodes by
        nodelist: Optional Slurm nodelist to filter nodes (e.g., node[1-10,15])
        max_reboot_percentage: Maximum percentage of nodes that can be rebooting at once
        reboot_timeout: Timeout in seconds to wait for a node to come back up
        enable_reboots: If True, actually process reboots (otherwise just track)
        state_file: Path to JSON file for persisting reboot state
    """
    reboot_manager = RebootManager(
        max_reboot_percentage=max_reboot_percentage,
        reboot_timeout=reboot_timeout,
        state_file=state_file,
    )
    finder = MaintenanceNodeFinder(reboot_manager=reboot_manager)
    iteration = 0

    logger.info(
        f"Starting periodic maintenance node finder and reboot manager (interval: {interval}s)"
    )
    logger.info(f"  Partition filter: {partition if partition else 'ALL'}")
    logger.info(f"  Nodelist filter: {nodelist if nodelist else 'ALL'}")
    logger.info(f"  Max reboot percentage: {max_reboot_percentage}%")
    logger.info(f"  Reboot timeout: {reboot_timeout}s")
    logger.info(f"  Reboots enabled: {enable_reboots}")
    if state_file:
        logger.info(f"  State file: {state_file}")

    try:
        while True:
            iteration += 1
            if max_iterations and iteration > max_iterations:
                logger.info(f"Reached maximum iterations ({max_iterations}), exiting")
                break

            logger.info(f"--- Check iteration {iteration} ---")

            # Find nodes in maintenance
            maint_reservations = finder.find_nodes_in_maintenance(partition, nodelist)

            # Track changes
            finder.track_changes(maint_reservations)

            # Display current state
            if maint_reservations:
                logger.info("Current maintenance reservations:")
                for res in maint_reservations:
                    logger.info(f"  {res}")
            else:
                logger.info("No maintenance reservations found")

            if enable_reboots:
                # Process nodes in maintenance reservations
                for res in maint_reservations:
                    node_name = res.node

                    # Determine partition for this node if not specified by user
                    node_partition = partition
                    if not node_partition:
                        # Get all partitions and find which one contains this node
                        all_partitions = reboot_manager.get_partition_info()
                        for pinfo in all_partitions:
                            if node_name in pinfo.nodes:
                                node_partition = pinfo.name
                                break

                    if not node_partition:
                        logger.warning(
                            f"Could not determine partition for node '{node_name}'"
                        )
                        continue

                    # Mark node for reboot if not already tracked
                    if node_name not in reboot_manager.node_reboot_status:
                        reboot_manager.mark_node_for_reboot(node_name, node_partition)

                # Process pending reboots
                pending_nodes = reboot_manager.get_nodes_by_state(
                    RebootState.PENDING, partition
                )
                for node_name in pending_nodes:
                    status = reboot_manager.node_reboot_status[node_name]

                    # Check if we can reboot this node without exceeding percentage
                    if reboot_manager.can_reboot_node(node_name, status.partition):
                        reboot_manager.issue_reboot(node_name)
                    else:
                        logger.debug(
                            f"Deferring reboot of '{node_name}' due to rate limit"
                        )

                # Monitor rebooting nodes
                rebooting_nodes = reboot_manager.get_nodes_by_state(
                    RebootState.REBOOTING, partition
                )
                for node_name in rebooting_nodes:
                    if reboot_manager.monitor_node_recovery(node_name):
                        reboot_manager.complete_node_reboot(node_name)

                # Display reboot summary
                summary = reboot_manager.get_reboot_summary(partition)
                if any(count > 0 for count in summary.values()):
                    logger.info("Reboot status summary:")
                    for state, count in summary.items():
                        if count > 0:
                            logger.info(f"  {state}: {count}")

                # Save state after processing
                if state_file:
                    reboot_manager.save_state()

                # Periodically clean up old completed/failed nodes
                if iteration % 10 == 0:  # Every 10 iterations
                    reboot_manager.clear_completed_nodes(max_age_hours=24)
            else:
                logger.debug(
                    "Reboot processing disabled (use --enable-reboots to enable)"
                )

            # Wait before next check
            if max_iterations is None or iteration < max_iterations:
                logger.debug(f"Sleeping for {interval} seconds...")
                time.sleep(interval)

    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down gracefully...")

        # Save final state
        if enable_reboots and state_file:
            logger.info("Saving final state before exit...")
            reboot_manager.save_state()

        # Display final summary
        if enable_reboots:
            logger.info("Final reboot summary:")
            summary = reboot_manager.get_reboot_summary(partition)
            for state, count in summary.items():
                if count > 0:
                    logger.info(f"  {state}: {count}")
    except Exception as e:
        logger.error(f"Unexpected error in periodic check: {e}")

        # Save state on unexpected error
        if enable_reboots and state_file:
            try:
                reboot_manager.save_state()
                logger.info(f"State saved to {state_file} before exit")
            except Exception:
                pass

        raise


@click.command()
@click.option(
    "--interval",
    "-i",
    type=int,
    default=60,
    help="Time in seconds between checks (default: 60)",
    show_default=True,
)
@click.option(
    "--max-iterations",
    "-n",
    type=int,
    default=None,
    help="Maximum number of iterations (default: infinite)",
)
@click.option(
    "--verbose",
    "-v",
    count=True,
    help="Increase verbosity (can be repeated: -v for INFO, -vv for DEBUG)",
)
@click.option(
    "--once", is_flag=True, help="Run once and exit (equivalent to --max-iterations=1)"
)
@click.option(
    "--partition",
    "-p",
    type=str,
    default=None,
    help="Slurm partition to filter nodes (default: all partitions)",
)
@click.option(
    "--nodelist",
    type=str,
    default=None,
    help="Filter nodes by Slurm nodelist format (e.g., node[1-10,15], sdfmilan[001-200,207]). Default: all nodes",
)
@click.option(
    "--max-reboot-percentage",
    type=float,
    default=10.0,
    help="Maximum percentage of partition nodes that can be rebooting at once (default: 10.0)",
    show_default=True,
)
@click.option(
    "--reboot-timeout",
    type=int,
    default=600,
    help="Timeout in seconds to wait for a node to come back up (default: 600)",
    show_default=True,
)
@click.option(
    "--enable-reboots",
    is_flag=True,
    help="Enable actual reboot processing (default: disabled, monitoring only)",
)
@click.option(
    "--state-file",
    type=click.Path(),
    default="/tmp/slurm-rebooter-state.json",
    help="Path to JSON file for persisting reboot state (default: /tmp/slurm-rebooter-state.json)",
    show_default=True,
)
def main(
    interval: int,
    max_iterations: Optional[int],
    verbose: int,
    once: bool,
    partition: Optional[str],
    nodelist: Optional[str],
    max_reboot_percentage: float,
    reboot_timeout: int,
    enable_reboots: bool,
    state_file: Optional[str],
) -> None:
    """
    Find Slurm nodes in maintenance reservations and manage rolling reboots.

    This script periodically queries Slurm for all reservations and identifies
    nodes that are in maintenance reservations with the format maint:<nodename>.

    When --enable-reboots is specified, it will orchestrate rolling reboots of
    nodes in maintenance reservations, ensuring that no more than the specified
    percentage of nodes in a partition are rebooting at any given time.

    Examples:

        # Monitor maintenance reservations without rebooting (default)
        python rebooter.py

        # Monitor and reboot nodes in the 'compute' partition
        python rebooter.py --partition compute --enable-reboots

        # Reboot specific nodes with 20% max concurrent reboots
        python rebooter.py --nodelist "node[1-50]" --max-reboot-percentage 20 --enable-reboots

        # Run once and exit
        python rebooter.py --once --partition compute
    """
    # Configure logger
    logger.remove()
    # Map verbose count to log level: 0=WARNING, 1=INFO, 2+=DEBUG
    if verbose == 0:
        log_level = "WARNING"
    elif verbose == 1:
        log_level = "INFO"
    else:
        log_level = "DEBUG"

    logger.add(
        sys.stderr,
        level=log_level,
        format="<cyan>{time:YYYY-MM-DDTHH:mm:ss}</cyan> | <level>{level: <8}</level> | <level>{message}</level>",
    )

    # Override max_iterations if --once flag is set
    if once:
        max_iterations = 1

    # Validate parameters
    if interval < 1:
        logger.error("Interval must be at least 1 second")
        sys.exit(1)

    if max_iterations is not None and max_iterations < 1:
        logger.error("Max iterations must be at least 1")
        sys.exit(1)

    if max_reboot_percentage <= 0 or max_reboot_percentage > 100:
        logger.error("Max reboot percentage must be between 0 and 100")
        sys.exit(1)

    if reboot_timeout < 1:
        logger.error("Reboot timeout must be at least 1 second")
        sys.exit(1)

    # Validate nodelist if provided
    if nodelist:
        # Test Slurm nodelist expansion
        temp_mgr = RebootManager()
        try:
            nodes = temp_mgr.expand_slurm_nodelist(nodelist)
            logger.debug(f"Nodelist is valid Slurm format ({len(nodes)} nodes)")
        except Exception as e:
            logger.error(f"Invalid Slurm nodelist format: {e}")
            sys.exit(1)

    # Run the periodic check
    run_periodic_check(
        interval=interval,
        max_iterations=max_iterations,
        partition=partition,
        nodelist=nodelist,
        max_reboot_percentage=max_reboot_percentage,
        reboot_timeout=reboot_timeout,
        enable_reboots=enable_reboots,
        state_file=state_file,
    )


if __name__ == "__main__":
    main()
