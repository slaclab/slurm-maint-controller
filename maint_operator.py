#!/usr/bin/env python3.9
"""
Slurm Maintenance Operator

This script combines the functionality of controller.py and rebooter.py to:
1. Create maintenance reservations for nodes based on running jobs
2. Monitor maintenance reservations and orchestrate rolling reboots
3. Ensure that no more than a specified percentage of nodes are unavailable at any time

Terminology:
- UNAVAILABLE: Nodes not actively being used by Slurm for jobs, including:
  * Nodes with active maintenance reservations
  * Nodes with reservations starting soon (within reservation_lead_time)
  * Nodes currently being rebooted
  * Nodes pending reboot
- DOWN: Nodes that Slurm considers offline/not accessible (Slurm's native DOWN state)

The script considers both unavailable and down nodes when enforcing the max percentage
limit to ensure cluster availability.
"""

import getpass
import json
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, List, Optional, Set, Tuple

import click
import pendulum
from loguru import logger

# ==================== Data Classes ====================


@dataclass
class PartitionInfo:
    """Information about a Slurm partition and its nodes."""

    name: str
    nodes: List[str] = field(default_factory=list)
    total_node_count: int = 0


@dataclass
class NodeState:
    """Information about a node's state in Slurm."""

    name: str
    state: str  # Raw Slurm state (e.g., "IDLE", "ALLOCATED", "DOWN", "DRAIN", etc.)

    def is_down(self) -> bool:
        """Check if node is in a DOWN state according to Slurm."""
        # Slurm states that indicate the node is not accessible
        down_states = [
            "DOWN",
            "DRAINED",
            "DRAINING",
            "DRAIN",
            "FAIL",
            "FAILING",
            "NOT_RESPONDING",
            "NO_RESPOND",
            "POWER_DOWN",
            "POWERING_DOWN",
        ]
        # Check if state contains any down state (handles DOWN*, DRAIN*, IDLE+DRAIN, etc.)
        state_upper = self.state.upper()
        # Split on common delimiters to handle composite states like "IDLE+DRAIN"
        state_parts = state_upper.replace("+", " ").replace("*", " ").split()
        for part in state_parts:
            if any(part.startswith(ds) for ds in down_states):
                return True
        return False


@dataclass
class JobInfo:
    """Information about a Slurm job."""

    job_id: str
    node: str
    start_time: pendulum.DateTime
    time_limit: pendulum.Duration
    user: str
    partition: str
    qos: str
    state: str
    account: str = ""

    @property
    def end_time(self) -> pendulum.DateTime:
        """Calculate the expected end time of the job."""
        return self.start_time.add(
            days=self.time_limit.days,
            hours=self.time_limit.hours,
            minutes=self.time_limit.minutes,
            seconds=self.time_limit.seconds,
        )

    @property
    def remaining_time(self) -> pendulum.Duration:
        """Calculate remaining time for the job."""
        now = pendulum.now()
        if self.end_time > now:
            return self.end_time - now
        return pendulum.duration(seconds=0)

    @property
    def normalized(self) -> str:
        return f"{self.account}".split("@")[0] + "/" + f"{self.user}"

    def __str__(self) -> str:
        return (
            f"Job {self.job_id} on {self.node}: "
            f"{self.normalized} @ {self.partition}^{self.qos}, "
            f"started {self.start_time.diff_for_humans()}, "
            f"ends at {self.end_time.to_datetime_string()}"
        )


class RebootState(Enum):
    """States for node reboot tracking."""

    PENDING = "pending"
    REBOOTING = "rebooting"
    COMPLETED = "completed"
    FAILED = "failed"


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


# ==================== Reservation Class ====================


@dataclass
class Reservation:
    """
    Data class representing a Slurm reservation.

    This is a pure data representation. All CRUD operations with Slurm
    are handled by ReservationManager.
    """

    name: str
    nodes: List[str]
    start_time: pendulum.DateTime
    duration: pendulum.Duration
    user: Optional[str] = None
    account: Optional[str] = None
    flags: Optional[str] = None
    state: Optional[str] = None  # For existing reservations: ACTIVE, INACTIVE, etc.
    end_time: Optional[pendulum.DateTime] = None  # Calculated or parsed

    def __post_init__(self):
        """Calculate end_time if not provided."""
        if self.end_time is None and self.start_time and self.duration:
            self.end_time = self.start_time.add(
                days=self.duration.days,
                hours=self.duration.hours,
                minutes=self.duration.minutes,
                seconds=self.duration.seconds,
            )

    @classmethod
    def from_slurm_dict(cls, res_dict: Dict[str, str]) -> Optional["Reservation"]:
        """
        Parse a reservation from scontrol output dictionary.

        Args:
            res_dict: Dictionary of reservation fields from scontrol

        Returns:
            Reservation object or None if parsing fails
        """
        try:
            name = res_dict.get("ReservationName", "")
            nodes_str = res_dict.get("Nodes", "")
            start_time_str = res_dict.get("StartTime", "")
            end_time_str = res_dict.get("EndTime", "")
            state = res_dict.get("State", "")
            users = res_dict.get("Users", None)
            accounts = res_dict.get("Accounts", None)
            flags = res_dict.get("Flags", None)

            if not name or not nodes_str:
                return None

            # Parse nodes (could be comma-separated or range)
            nodes = nodes_str.split(",")

            # Parse start time
            try:
                start_time = pendulum.parse(start_time_str)
            except Exception:
                logger.warning(
                    f"Could not parse start time '{start_time_str}' for reservation {name}"
                )
                start_time = pendulum.now()

            # Parse end time and calculate duration
            try:
                end_time = pendulum.parse(end_time_str)
                duration = end_time - start_time
            except Exception:
                logger.warning(
                    f"Could not parse end time '{end_time_str}' for reservation {name}"
                )
                end_time = None
                duration = pendulum.duration(hours=6)  # Default

            return cls(
                name=name,
                nodes=nodes,
                start_time=start_time,
                duration=duration,
                user=users,
                account=accounts,
                flags=flags,
                state=state,
                end_time=end_time,
            )

        except Exception as e:
            logger.warning(f"Failed to parse reservation from dict: {e}")
            return None

    @classmethod
    def from_slurm_output_line(cls, line: str) -> Optional["Reservation"]:
        """
        Parse a reservation from a single line of scontrol output.

        Args:
            line: Output line from scontrol show reservation

        Returns:
            Reservation object or None if parsing fails
        """
        try:
            res_dict = {}
            pattern = r"(\w+)=([^\s]+(?:\s+[^\s=]+)*?)(?=\s+\w+=|$)"
            matches = re.finditer(pattern, line)

            for match in matches:
                key = match.group(1)
                value = match.group(2).strip()
                res_dict[key] = value

            return cls.from_slurm_dict(res_dict)

        except Exception as e:
            logger.warning(f"Failed to parse reservation line: {e}")
            return None

    def is_maintenance_reservation(self) -> bool:
        """Check if this is a maintenance reservation."""
        return self.name.startswith("maint:")

    def is_active(self) -> bool:
        """Check if reservation is currently active."""
        if self.state:
            return self.state == "ACTIVE"

        # If state not available, check based on time
        now = pendulum.now()
        return self.start_time <= now < self.end_time

    def is_starting_soon(self, lead_time_minutes: int = 60) -> bool:
        """
        Check if reservation is starting within lead_time_minutes.

        Args:
            lead_time_minutes: Minutes threshold for "starting soon"

        Returns:
            True if reservation starts within the threshold
        """
        try:
            now = pendulum.now()
            time_until_start = (self.start_time - now).total_minutes()

            return 0 <= time_until_start <= lead_time_minutes
        except Exception:
            return False

    def is_active_or_starting_soon(self, lead_time_minutes: int = 60) -> bool:
        """
        Check if reservation is active or starting soon.

        Args:
            lead_time_minutes: Minutes threshold for "starting soon"

        Returns:
            True if reservation is active or starting within threshold
        """
        return self.is_active() or self.is_starting_soon(lead_time_minutes)

    def get_node_name(self) -> Optional[str]:
        """
        Get the node name from a maintenance reservation.

        For maintenance reservations with format 'maint:nodename',
        returns the node name. Otherwise returns the first node.
        """
        if self.is_maintenance_reservation():
            parts = self.name.split(":", 1)
            if len(parts) > 1:
                return parts[1]

        return self.nodes[0] if self.nodes else None

    def format_duration(self) -> str:
        """Format duration in Slurm format (days-hours:minutes:seconds)."""
        total_seconds = int(self.duration.total_seconds())
        days = total_seconds // 86400
        hours = (total_seconds % 86400) // 3600
        minutes = (total_seconds % 3600) // 60
        seconds = total_seconds % 60

        if days > 0:
            return f"{days}-{hours:02d}:{minutes:02d}:{seconds:02d}"
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

    def build_command_args(self) -> List[str]:
        """Build common command arguments for create/update."""
        args = [
            f"ReservationName={self.name}",
            f"StartTime={self.start_time.format('YYYY-MM-DDTHH:mm:ss')}",
            f"Duration={self.format_duration()}",
            f"Nodes={','.join(self.nodes)}",
        ]

        if self.user:
            args.append(f"Users={self.user}")

        if self.account:
            args.append(f"Accounts={self.account}")

        if self.flags:
            args.append(f"Flags={self.flags}")

        return args

    def __str__(self) -> str:
        state_str = f", state={self.state}" if self.state else ""
        return (
            f"Reservation '{self.name}' for {self.nodes}: "
            f"starts {self.start_time.to_datetime_string()}, "
            f"duration {self.format_duration()}{state_str}"
        )


# ==================== Reservation Manager ====================


class ReservationManager:
    """
    Manages all interactions with Slurm reservations.

    Handles CRUD operations for reservations:
    - Query (get_all, get_maintenance, exists)
    - Create
    - Update
    - Delete
    """

    def __init__(self, dry_run: bool = False):
        """
        Initialize the reservation manager.

        Args:
            dry_run: If True, log commands but don't execute them
        """
        self.dry_run = dry_run

    def get_all(self) -> List[Reservation]:
        """
        Query Slurm for all reservations.

        Returns:
            List of Reservation objects
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

                reservation = Reservation.from_slurm_output_line(line)
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

    def get_maintenance(self) -> List[Reservation]:
        """
        Get all maintenance reservations (name starts with 'maint:').

        Returns:
            List of Reservation objects for maintenance reservations
        """
        all_reservations = self.get_all()
        return [res for res in all_reservations if res.is_maintenance_reservation()]

    def exists(self, name: str) -> bool:
        """
        Check if a reservation exists in Slurm.

        Args:
            name: Reservation name

        Returns:
            True if reservation exists, False otherwise
        """
        try:
            cmd = ["scontrol", "show", "reservation", name]
            logger.debug(f"Checking reservation existence: {' '.join(cmd)}")
            result = subprocess.run(cmd, capture_output=True, text=True, check=False)

            if result.returncode == 0 and result.stdout.strip():
                return True

            if (
                "not found" in result.stdout.lower()
                or "invalid reservation" in result.stdout.lower()
            ):
                return False
            if (
                "not found" in result.stderr.lower()
                or "invalid reservation" in result.stderr.lower()
            ):
                return False

            if result.returncode != 0:
                logger.error(f"Failed to check reservation '{name}': {result.stderr}")
                return False

            return False
        except Exception as e:
            logger.error(f"Unexpected error checking reservation '{name}': {e}")
            return False

    def create(self, reservation: Reservation) -> bool:
        """
        Create a reservation in Slurm.

        Args:
            reservation: Reservation object to create

        Returns:
            True if successful, False otherwise
        """
        cmd = ["scontrol", "create", "reservation"] + reservation.build_command_args()

        logger.info(f"Creating reservation: {reservation}")
        logger.debug(f"Command: {' '.join(cmd)}")

        if self.dry_run:
            logger.warning("DRY RUN: Would execute: " + " ".join(cmd))
            return True

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            logger.success(f"Reservation {reservation.name} created successfully")
            return True
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to create reservation: {e.stderr}")
            return False

    def update(self, reservation: Reservation) -> bool:
        """
        Update an existing reservation in Slurm.

        Args:
            reservation: Reservation object with updated values

        Returns:
            True if successful, False otherwise
        """
        cmd = ["scontrol", "update", "reservation"] + reservation.build_command_args()

        logger.info(f"Updating reservation: {reservation}")
        logger.debug(f"Command: {' '.join(cmd)}")

        if self.dry_run:
            logger.warning("DRY RUN: Would execute: " + " ".join(cmd))
            return True

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            logger.success(f"Reservation updated successfully")
            return True
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to update reservation: {e.stderr}")
            return False

    def create_or_update(self, reservation: Reservation) -> bool:
        """
        Create or update a reservation (checks if it exists first).

        Args:
            reservation: Reservation object to create or update

        Returns:
            True if successful, False otherwise
        """
        if self.exists(reservation.name):
            return self.update(reservation)
        else:
            return self.create(reservation)

    def delete(self, reservation: Reservation) -> bool:
        """
        Delete a reservation from Slurm.

        Args:
            reservation: Reservation object to delete

        Returns:
            True if successful, False otherwise
        """
        cmd = ["scontrol", "delete", "reservation", reservation.name]

        logger.info(f"Deleting reservation: {reservation.name}")
        logger.debug(f"Command: {' '.join(cmd)}")

        if self.dry_run:
            logger.warning("DRY RUN: Would execute: " + " ".join(cmd))
            return True

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            logger.success(f"Reservation deleted successfully")
            return True
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to delete reservation: {e.stderr}")
            return False


# ==================== Job List Management ====================


class JobList:
    """Container for a list of JobInfo instances with filtering methods."""

    def __init__(self, jobs: Optional[List[JobInfo]] = None):
        self.jobs = jobs if jobs is not None else []

    def filter(self, qos: Optional[str] = None) -> List[JobInfo]:
        """Filter jobs by QoS. Supports negation with '!' prefix."""
        if qos is None:
            return self.jobs

        if qos.startswith("!"):
            # Negation: return jobs NOT matching this QoS
            target_qos = qos[1:]
            return [job for job in self.jobs if job.qos != target_qos]
        else:
            # Normal filter: return jobs matching this QoS
            return [job for job in self.jobs if job.qos == qos]

    def get_longest_job(self) -> Optional[JobInfo]:
        """Get the job with the longest remaining time."""
        if not self.jobs:
            return None

        return max(self.jobs, key=lambda job: job.end_time)

    def __len__(self) -> int:
        return len(self.jobs)

    def __iter__(self):
        return iter(self.jobs)


# ==================== Slurm Utilities ====================


def expand_slurm_nodelist(nodelist: str) -> Set[str]:
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
            node.strip() for node in result.stdout.strip().split("\n") if node.strip()
        }
        # logger.debug(f"Expanded Slurm nodelist '{nodelist}' to {len(nodes)} nodes")
        return nodes
    except subprocess.CalledProcessError as e:
        logger.warning(f"Failed to expand Slurm nodelist '{nodelist}': {e.stderr}")
        return {nodelist}


def parse_duration_string(duration_str: str) -> pendulum.Duration:
    """Parse duration string in HH:MM:SS format."""
    parts = duration_str.split(":")
    if len(parts) == 3:
        return pendulum.duration(
            hours=int(parts[0]), minutes=int(parts[1]), seconds=int(parts[2])
        )
    elif len(parts) == 2:
        return pendulum.duration(minutes=int(parts[0]), seconds=int(parts[1]))
    else:
        raise ValueError(f"Invalid duration format: {duration_str}")


# ==================== Slurm Controller ====================


class SlurmController:
    """Interface to Slurm commands."""

    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run

    def get_all_nodes(self) -> List[str]:
        """Get all node names in the cluster."""
        try:
            result = subprocess.run(
                ["scontrol", "show", "nodes", "-o"],
                capture_output=True,
                text=True,
                check=True,
            )
            nodes = []
            for line in result.stdout.strip().split("\n"):
                match = re.search(r"NodeName=(\S+)", line)
                if match:
                    nodes.append(match.group(1))
            logger.info(f"Found {len(nodes)} total nodes in cluster")
            return nodes
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to get nodes: {e.stderr}")
            return []

    def get_node_states(
        self, partition_name: Optional[str] = None
    ) -> Dict[str, NodeState]:
        """Get the state of all nodes, optionally filtered by partition."""
        try:
            cmd = ["sinfo", "-h", "-o", "%n|%T"]
            if partition_name:
                cmd.extend(["-p", partition_name])

            logger.debug(
                f"Querying node states from Slurm for partition '{partition_name}'"
            )
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)

            node_states = {}
            for line in result.stdout.strip().split("\n"):
                if not line.strip():
                    continue

                parts = line.split("|")
                if len(parts) != 2:
                    continue

                node_name = parts[0]
                state = parts[1]

                # Handle node ranges - expand them
                if "[" in node_name:
                    expanded_nodes = expand_slurm_nodelist(node_name)
                    for expanded_node in expanded_nodes:
                        node_states[expanded_node] = NodeState(
                            name=expanded_node, state=state
                        )
                else:
                    node_states[node_name] = NodeState(name=node_name, state=state)

            logger.debug(f"Retrieved states for {len(node_states)} nodes from Slurm")
            return node_states

        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to get node states: {e.stderr}")
            return {}

    def get_partition_info(
        self, partition_name: Optional[str] = None
    ) -> List[PartitionInfo]:
        """Get information about Slurm partitions and their nodes."""
        try:
            cmd = ["sinfo", "-h", "-o", "%P|%N"]
            if partition_name:
                cmd.extend(["-p", partition_name])

            result = subprocess.run(cmd, capture_output=True, text=True, check=True)

            partition_nodes: Dict[str, Set[str]] = {}

            for line in result.stdout.strip().split("\n"):
                if not line.strip():
                    continue

                parts = line.split("|")
                if len(parts) != 2:
                    continue

                partition = parts[0].rstrip("*")
                nodes_str = parts[1]
                nodes = expand_slurm_nodelist(nodes_str)

                if partition not in partition_nodes:
                    partition_nodes[partition] = set()
                partition_nodes[partition].update(nodes)

            partition_info_list = []
            for partition, nodes in partition_nodes.items():
                node_list = sorted(list(nodes))
                partition_info_list.append(
                    PartitionInfo(
                        name=partition, nodes=node_list, total_node_count=len(node_list)
                    )
                )

            return partition_info_list

        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to get partition info: {e.stderr}")
            return []

    def get_jobs_on_node(self, node_name: str) -> JobList:
        """Get all jobs running on a specific node."""
        try:
            cmd = [
                "squeue",
                "-w",
                node_name,
                "-h",
                "-o",
                "%i|%N|%S|%l|%u|%P|%q|%T|%a",
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)

            jobs = []
            for line in result.stdout.strip().split("\n"):
                if not line.strip():
                    continue

                parts = line.split("|")
                if len(parts) < 8:
                    continue

                try:
                    start_time = pendulum.parse(parts[2])
                    time_limit = self._parse_time_limit(parts[3])

                    job = JobInfo(
                        job_id=parts[0],
                        node=parts[1],
                        start_time=start_time,
                        time_limit=time_limit,
                        user=parts[4],
                        partition=parts[5],
                        qos=parts[6],
                        state=parts[7],
                        account=parts[8] if len(parts) > 8 else "",
                    )
                    jobs.append(job)
                except Exception as e:
                    logger.warning(f"Failed to parse job line '{line}': {e}")
                    continue

            return JobList(jobs)

        except subprocess.CalledProcessError as e:
            logger.debug(f"No jobs found on node {node_name}: {e.stderr}")
            return JobList([])

    def _parse_time_limit(self, time_str: str) -> pendulum.Duration:
        """Parse Slurm time limit format."""
        if time_str == "UNLIMITED" or time_str == "NOT_SET":
            return pendulum.duration(days=365)

        # Format: [days-]hours:minutes:seconds
        days = 0
        if "-" in time_str:
            day_part, time_part = time_str.split("-", 1)
            days = int(day_part)
            time_str = time_part

        parts = time_str.split(":")
        if len(parts) == 3:
            hours, minutes, seconds = map(int, parts)
        elif len(parts) == 2:
            hours, minutes = map(int, parts)
            seconds = 0
        else:
            hours = int(parts[0])
            minutes = seconds = 0

        return pendulum.duration(
            days=days, hours=hours, minutes=minutes, seconds=seconds
        )

    def terminate_job(self, job: JobInfo) -> bool:
        """Terminate a job."""
        if self.dry_run:
            logger.info(f"[DRY RUN] Would terminate job {job.job_id}")
            return True

        try:
            cmd = ["scancel", job.job_id]
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            logger.info(f"Terminated job {job.job_id}")
            return True
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to terminate job {job.job_id}: {e.stderr}")
            return False


# ==================== Maintenance Manager ====================


class MaintenanceManager:
    """Manages maintenance reservations and node reboots with percentage-based rate limiting."""

    def __init__(
        self,
        controller: SlurmController,
        reservation_manager: ReservationManager,
        max_down_percentage: float = 10.0,
        reboot_timeout: int = 600,
        reservation_lead_time: int = 60,
        state_file: Optional[str] = None,
    ):
        self.controller = controller
        self.reservation_manager = reservation_manager
        self.max_down_percentage = max_down_percentage
        self.reboot_timeout = reboot_timeout
        self.reservation_lead_time = reservation_lead_time  # minutes
        self.state_file = state_file
        self.node_reboot_status: Dict[str, NodeRebootStatus] = {}

        if self.state_file:
            self.load_state()

    def get_maintenance_reservations(self) -> List[Reservation]:
        """Get all maintenance reservations from Slurm."""
        return self.reservation_manager.get_maintenance()

    def count_unavailable_nodes(self, partition: str) -> Tuple[int, int, Set[str]]:
        """
        Count nodes that are unavailable (in reservations, rebooting, or pending).

        Returns:
            Tuple of (unavailable_count, total_count, unavailable_node_set)
        """
        # Get partition info
        partition_info_list = self.controller.get_partition_info(partition)
        if not partition_info_list:
            logger.error(f"Could not get info for partition '{partition}'")
            return (0, 0, set())

        partition_info = partition_info_list[0]
        total_nodes = partition_info.total_node_count
        partition_nodes = set(partition_info.nodes)

        if total_nodes == 0:
            return (0, 0, set())

        unavailable_nodes = set()

        # Count nodes in maintenance reservations (active or starting soon)
        reservations = self.get_maintenance_reservations()
        for res in reservations:
            node_name = res.get_node_name()
            if node_name and node_name in partition_nodes:
                if res.is_active_or_starting_soon(self.reservation_lead_time):
                    unavailable_nodes.add(node_name)

        # Count nodes being rebooted or pending reboot
        for node_name, status in self.node_reboot_status.items():
            if node_name in partition_nodes:
                if status.partition == partition and status.state in [
                    RebootState.REBOOTING,
                    RebootState.PENDING,
                ]:
                    unavailable_nodes.add(node_name)

        unavailable_count = len(unavailable_nodes)
        return (unavailable_count, total_nodes, unavailable_nodes)

    def count_down_nodes_slurm(self, partition: str) -> Tuple[int, int, Set[str]]:
        """
        Count nodes that Slurm considers DOWN.

        Returns:
            Tuple of (down_count, total_count, down_node_set)
        """
        # Get partition info
        partition_info_list = self.controller.get_partition_info(partition)
        if not partition_info_list:
            logger.error(f"Could not get info for partition '{partition}'")
            return (0, 0, set())

        partition_info = partition_info_list[0]
        total_nodes = partition_info.total_node_count
        partition_nodes = set(partition_info.nodes)

        if total_nodes == 0:
            return (0, 0, set())

        # Get node states from Slurm (refreshed on every call)
        logger.debug(f"Refreshing node states from Slurm for partition '{partition}'")
        node_states = self.controller.get_node_states(partition)

        down_nodes = set()
        for node_name in partition_nodes:
            if node_name in node_states and node_states[node_name].is_down():
                down_nodes.add(node_name)

        down_count = len(down_nodes)
        logger.debug(
            f"Found {down_count} down nodes in partition '{partition}': {','.join(sorted(down_nodes)) if down_nodes else '(none)'}"
        )
        return (down_count, total_nodes, down_nodes)

    def can_add_reservation(self, partition: str) -> bool:
        """
        Check if we can add another reservation without exceeding max percentage.

        This considers both unavailable nodes (maintenance-related) and down nodes
        (Slurm's native DOWN state) to ensure we don't exceed the limit.
        """
        unavailable_count, total_nodes, unavailable_nodes = (
            self.count_unavailable_nodes(partition)
        )
        down_count, _, down_nodes = self.count_down_nodes_slurm(partition)

        if total_nodes == 0:
            return False

        # Count unique nodes that are either unavailable or down
        combined_nodes = unavailable_nodes.union(down_nodes)
        combined_count = len(combined_nodes)

        current_percentage = (combined_count / total_nodes) * 100

        # Separate counts for logging
        unavailable_percentage = (unavailable_count / total_nodes) * 100
        down_percentage = (down_count / total_nodes) * 100
        overlap_count = len(unavailable_nodes.intersection(down_nodes))

        logger.debug(
            f"Partition '{partition}': {combined_count}/{total_nodes} nodes unavailable or down "
            f"({current_percentage:.1f}% of max {self.max_down_percentage}%) - "
            f"unavailable: {unavailable_count} ({unavailable_percentage:.1f}%), "
            f"down: {down_count} ({down_percentage:.1f}%), "
            f"overlap: {overlap_count}"
        )

        if current_percentage >= self.max_down_percentage:
            logger.debug(
                f"Cannot add reservation: would exceed max unavailable/down percentage"
            )
            return False

        return True

    def mark_node_for_reboot(self, node_name: str, partition: str) -> bool:
        """Mark a node as pending reboot."""
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
        """Issue a reboot command to a node."""
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
        logger.info(f"[STUB] Issuing reboot command to node '{node_name}'")

        status.state = RebootState.REBOOTING
        status.reboot_start_time = datetime.now()
        status.attempts += 1

        logger.info(
            f"Node '{node_name}' reboot initiated (attempt {status.attempts}/{status.max_attempts})"
        )
        return True

    def monitor_node_recovery(self, node_name: str) -> bool:
        """Check if a node has recovered from reboot."""
        if node_name not in self.node_reboot_status:
            return False

        status = self.node_reboot_status[node_name]

        if status.state != RebootState.REBOOTING:
            return status.state == RebootState.COMPLETED

        # STUB: This is where you would actually check if the node is up
        logger.debug(f"[STUB] Checking if node '{node_name}' has recovered from reboot")

        # Check for timeout
        if status.reboot_start_time:
            elapsed = (datetime.now() - status.reboot_start_time).total_seconds()
            if elapsed > self.reboot_timeout:
                logger.warning(
                    f"Node '{node_name}' reboot timed out after {elapsed:.0f}s"
                )

                if status.attempts >= status.max_attempts:
                    status.state = RebootState.FAILED
                    logger.error(
                        f"Node '{node_name}' failed after {status.attempts} attempts"
                    )
                else:
                    status.state = RebootState.PENDING
                    logger.info(f"Node '{node_name}' will be retried")

                return False

        return False

    def complete_node_reboot(self, node_name: str) -> bool:
        """Mark a node reboot as completed."""
        if node_name not in self.node_reboot_status:
            return False

        status = self.node_reboot_status[node_name]
        status.state = RebootState.COMPLETED
        status.reboot_complete_time = datetime.now()

        logger.success(f"Node '{node_name}' reboot completed successfully")
        return True

    def get_nodes_by_state(
        self, state: RebootState, partition: Optional[str] = None
    ) -> List[str]:
        """Get list of nodes in a specific reboot state."""
        nodes = []
        for node_name, status in self.node_reboot_status.items():
            if status.state == state:
                if partition is None or status.partition == partition:
                    nodes.append(node_name)
        return nodes

    def get_reboot_summary(self, partition: Optional[str] = None) -> Dict[str, int]:
        """Get summary of reboot states."""
        summary = {state.value: 0 for state in RebootState}

        for status in self.node_reboot_status.values():
            if partition is None or status.partition == partition:
                summary[status.state.value] += 1

        return summary

    def save_state(self) -> None:
        """Save reboot state to file."""
        if not self.state_file:
            logger.debug("No state file configured, skipping save")
            return

        try:
            state_data = {
                "node_reboot_status": {
                    node_name: status.to_dict()
                    for node_name, status in self.node_reboot_status.items()
                }
            }

            with open(self.state_file, "w") as f:
                json.dump(state_data, f, indent=2)

            logger.debug(f"Saved state to {self.state_file}")
        except Exception as e:
            logger.error(f"Failed to save state: {e}")

    def load_state(self) -> None:
        """Load reboot state from file."""
        if not self.state_file or not os.path.exists(self.state_file):
            return

        try:
            with open(self.state_file, "r") as f:
                state_data = json.load(f)

            self.node_reboot_status = {
                node_name: NodeRebootStatus.from_dict(status_dict)
                for node_name, status_dict in state_data.get(
                    "node_reboot_status", {}
                ).items()
            }

            logger.info(
                f"Loaded state from {self.state_file} ({len(self.node_reboot_status)} nodes)"
            )
        except Exception as e:
            logger.error(f"Failed to load state: {e}")

    def clear_completed_nodes(self, max_age_hours: int = 24) -> None:
        """Remove old completed/failed nodes from tracking."""
        cutoff_time = datetime.now() - timedelta(hours=max_age_hours)
        nodes_to_remove = []

        for node_name, status in self.node_reboot_status.items():
            if status.state in [RebootState.COMPLETED, RebootState.FAILED]:
                if (
                    status.reboot_complete_time
                    and status.reboot_complete_time < cutoff_time
                ):
                    nodes_to_remove.append(node_name)

        for node_name in nodes_to_remove:
            del self.node_reboot_status[node_name]
            logger.debug(f"Removed old reboot status for node '{node_name}'")


# ==================== Main Operator Logic ====================


def run_operator(
    interval: int,
    max_iterations: Optional[int],
    nodelist: Optional[str],
    partition: str,
    max_down_percentage: float,
    reservation_duration: str,
    reservation_lead_time: int,
    reboot_timeout: int,
    enable_reservations: bool,
    enable_reboots: bool,
    terminate_preemptable_jobs: bool,
    dry_run: bool,
    state_file: Optional[str],
    user: Optional[str],
    account: Optional[str],
    flags: Optional[str],
) -> None:
    """
    Main operator loop that creates reservations and manages reboots.
    """
    controller = SlurmController(dry_run=dry_run)
    reservation_manager = ReservationManager(dry_run=dry_run)
    manager = MaintenanceManager(
        controller=controller,
        reservation_manager=reservation_manager,
        max_down_percentage=max_down_percentage,
        reboot_timeout=reboot_timeout,
        reservation_lead_time=reservation_lead_time,
        state_file=state_file,
    )

    logger.info(f"Starting Slurm Maintenance Operator (interval: {interval}s)")
    logger.info(f"  Partition: {partition}")
    logger.info(f"  Nodelist filter: {nodelist if nodelist else 'ALL'}")
    logger.info(f"  Max unavailable/down percentage: {max_down_percentage}%")
    logger.info(f"  Reservation duration: {reservation_duration}")
    logger.info(f"  Reservation lead time: {reservation_lead_time} minutes")
    logger.info(f"  Reboot timeout: {reboot_timeout}s")
    logger.info(f"  Reservations enabled: {enable_reservations}")
    logger.info(f"  Reboots enabled: {enable_reboots}")
    logger.info(f"  Terminate preemptable: {terminate_preemptable_jobs}")
    logger.info(f"  Dry run: {dry_run}")

    # Get target nodes
    if nodelist:
        target_nodes = list(expand_slurm_nodelist(nodelist))
    else:
        target_nodes = controller.get_all_nodes()

    logger.info(f"Managing {len(target_nodes)} nodes")

    iteration = 0

    try:
        while True:
            iteration += 1
            if max_iterations and iteration > max_iterations:
                logger.info(f"Reached maximum iterations ({max_iterations}), exiting")
                break

            logger.info(f"--- Iteration {iteration} ---")

            # Get current maintenance reservations
            existing_reservations = manager.get_maintenance_reservations()
            existing_reservation_nodes = {
                res.get_node_name() for res in existing_reservations
            }

            logger.info(
                f"Found {len(existing_reservations)} existing maintenance reservations"
            )

            # Phase 0: Check and update existing reservations if needed
            if enable_reservations:
                logger.debug(
                    f"Phase 0: Checking {len(existing_reservations)} existing reservations for updates"
                )
                for res in existing_reservations:
                    node_name = res.get_node_name()
                    if not node_name:
                        continue

                    # Skip if reservation is already active (too late to update)
                    if res.is_active():
                        logger.debug(
                            f"Skipping reservation for {node_name}: already active"
                        )
                        continue

                    logger.debug(
                        f"Checking reservation for {node_name} (current start: {res.start_time.to_datetime_string()})"
                    )

                    # Get current jobs on the node
                    job_list = controller.get_jobs_on_node(node_name)
                    preemptable_jobs = JobList(job_list.filter(qos="preemptable"))
                    non_preemptable_jobs = JobList(job_list.filter(qos="!preemptable"))

                    logger.debug(
                        f"  Jobs on {node_name}: {len(job_list)} total, "
                        f"{len(preemptable_jobs)} preemptable, "
                        f"{len(non_preemptable_jobs)} non-preemptable"
                    )

                    longest_preemptable = preemptable_jobs.get_longest_job()
                    longest_non_preemptable = non_preemptable_jobs.get_longest_job()

                    if longest_preemptable:
                        logger.debug(
                            f"  Longest preemptable job on {node_name}: {longest_preemptable.job_id}, "
                            f"ends at {longest_preemptable.end_time.to_datetime_string()}"
                        )
                    if longest_non_preemptable:
                        logger.debug(
                            f"  Longest non-preemptable job on {node_name}: {longest_non_preemptable.job_id}, "
                            f"ends at {longest_non_preemptable.end_time.to_datetime_string()}"
                        )

                    # Determine what the reservation start time should be
                    new_reservation_start_time = pendulum.now()

                    if len(non_preemptable_jobs) == 0:
                        # Only preemptable jobs
                        if terminate_preemptable_jobs:
                            # Jobs will be terminated, start soon
                            logger.debug(
                                f"  {node_name}: Only preemptable jobs, will terminate -> start soon"
                            )
                            new_reservation_start_time = (
                                pendulum.now() + pendulum.duration(seconds=1)
                            )
                        else:
                            if longest_preemptable:
                                logger.debug(
                                    f"  {node_name}: Only preemptable jobs, not terminating -> wait for longest"
                                )
                                new_reservation_start_time = (
                                    longest_preemptable.end_time
                                )
                            else:
                                logger.debug(f"  {node_name}: No jobs -> start soon")
                                new_reservation_start_time = (
                                    pendulum.now() + pendulum.duration(seconds=1)
                                )

                    elif longest_non_preemptable and preemptable_jobs:
                        # Both types of jobs - use longest non-preemptable
                        logger.debug(
                            f"  {node_name}: Both job types -> wait for longest non-preemptable"
                        )
                        new_reservation_start_time = longest_non_preemptable.end_time

                    else:
                        # Only non-preemptable jobs
                        if longest_non_preemptable:
                            logger.debug(
                                f"  {node_name}: Only non-preemptable jobs -> wait for longest"
                            )
                            new_reservation_start_time = (
                                longest_non_preemptable.end_time
                            )
                        else:
                            logger.debug(f"  {node_name}: No jobs -> start soon")
                            new_reservation_start_time = (
                                pendulum.now() + pendulum.duration(seconds=1)
                            )

                    # Check if the reservation start time needs to be updated
                    # Allow a small tolerance (e.g., 60 seconds) to avoid unnecessary updates
                    time_diff = abs(
                        (new_reservation_start_time - res.start_time).total_seconds()
                    )

                    logger.debug(
                        f"  {node_name}: Calculated new start time: {new_reservation_start_time.to_datetime_string()}, "
                        f"time diff: {time_diff:.0f}s"
                    )

                    if time_diff > 60:
                        logger.info(
                            f"Updating reservation for {node_name}: "
                            f"old start={res.start_time.to_datetime_string()}, "
                            f"new start={new_reservation_start_time.to_datetime_string()} "
                            f"(diff: {time_diff:.0f}s)"
                        )

                        # Terminate preemptable jobs if needed
                        if (
                            terminate_preemptable_jobs
                            and len(non_preemptable_jobs) == 0
                        ):
                            logger.info(f"Terminating preemptable jobs on {node_name}")
                            for job in preemptable_jobs:
                                controller.terminate_job(job)
                        elif (
                            terminate_preemptable_jobs
                            and longest_non_preemptable
                            and preemptable_jobs
                        ):
                            for job in preemptable_jobs:
                                if job.end_time > longest_non_preemptable.end_time:
                                    logger.info(
                                        f"Terminating preemptable job {job.job_id} on {node_name}"
                                    )
                                    controller.terminate_job(job)

                        # Create updated reservation object
                        duration = parse_duration_string(reservation_duration)
                        updated_reservation = Reservation(
                            name=f"maint:{node_name}",
                            nodes=[node_name],
                            start_time=new_reservation_start_time,
                            duration=duration,
                            user=user,
                            account=account,
                            flags=flags,
                        )

                        reservation_manager.update(updated_reservation)
                    else:
                        logger.debug(
                            f"  {node_name}: No update needed (time diff {time_diff:.0f}s <= 60s tolerance)"
                        )

            # Phase 1: Create new reservations for nodes that don't have them
            if enable_reservations:
                nodes_needing_reservations = [
                    node
                    for node in target_nodes
                    if node not in existing_reservation_nodes
                ]

                logger.info(
                    f"{len(nodes_needing_reservations)} nodes need reservations"
                )

                for node_name in nodes_needing_reservations:
                    # Use the partition argument (now required)
                    node_partition = partition

                    # Check if we can add a reservation
                    if not manager.can_add_reservation(node_partition):
                        logger.debug(
                            f"Deferring reservation for '{node_name}' due to max down percentage"
                        )
                        continue

                    # Get jobs on node
                    job_list = controller.get_jobs_on_node(node_name)
                    preemptable_jobs = JobList(job_list.filter(qos="preemptable"))
                    non_preemptable_jobs = JobList(job_list.filter(qos="!preemptable"))

                    longest_preemptable = preemptable_jobs.get_longest_job()
                    longest_non_preemptable = non_preemptable_jobs.get_longest_job()

                    # Determine reservation start time
                    reservation_start_time = pendulum.now()

                    if len(non_preemptable_jobs) == 0:
                        # Only preemptable jobs
                        if terminate_preemptable_jobs:
                            logger.info(f"Terminating preemptable jobs on {node_name}")
                            for job in preemptable_jobs:
                                controller.terminate_job(job)
                            reservation_start_time = pendulum.now() + pendulum.duration(
                                seconds=1
                            )
                        else:
                            if longest_preemptable:
                                reservation_start_time = longest_preemptable.end_time
                            else:
                                reservation_start_time = (
                                    pendulum.now() + pendulum.duration(seconds=1)
                                )

                    elif longest_non_preemptable and preemptable_jobs:
                        # Both types of jobs
                        if terminate_preemptable_jobs:
                            for job in preemptable_jobs:
                                if job.end_time > longest_non_preemptable.end_time:
                                    logger.info(
                                        f"Terminating preemptable job {job.job_id} on {node_name}"
                                    )
                                    controller.terminate_job(job)

                        reservation_start_time = longest_non_preemptable.end_time

                    else:
                        # Only non-preemptable jobs
                        if longest_non_preemptable:
                            reservation_start_time = longest_non_preemptable.end_time
                        else:
                            reservation_start_time = pendulum.now() + pendulum.duration(
                                seconds=1
                            )

                    # Create reservation object and create it in Slurm
                    duration = parse_duration_string(reservation_duration)
                    reservation = Reservation(
                        name=f"maint:{node_name}",
                        nodes=[node_name],
                        start_time=reservation_start_time,
                        duration=duration,
                        user=user,
                        account=account,
                        flags=flags,
                    )

                    logger.info(
                        f"Creating reservation for {node_name} starting at {reservation_start_time.to_datetime_string()}"
                    )
                    reservation_manager.create_or_update(reservation)

            # Phase 2: Manage reboots for nodes in active reservations
            if enable_reboots:
                existing_reservations = manager.get_maintenance_reservations()
                active_reservations = [
                    res for res in existing_reservations if res.is_active()
                ]

                logger.info(
                    f"{len(active_reservations)} reservations are currently active"
                )

                for res in active_reservations:
                    node_name = res.get_node_name()
                    if not node_name:
                        continue

                    # Use the partition argument (now required)
                    node_partition = partition

                    # Mark node for reboot if not already tracked
                    if node_name not in manager.node_reboot_status:
                        manager.mark_node_for_reboot(node_name, node_partition)

                # Process pending reboots
                pending_nodes = manager.get_nodes_by_state(
                    RebootState.PENDING, partition
                )
                for node_name in pending_nodes:
                    status = manager.node_reboot_status[node_name]

                    # Check if we can reboot (based on unavailable/down percentage)
                    unavailable_count, total_count, unavailable_nodes = (
                        manager.count_unavailable_nodes(status.partition)
                    )
                    down_count, _, down_nodes = manager.count_down_nodes_slurm(
                        status.partition
                    )
                    combined_count = len(unavailable_nodes.union(down_nodes))

                    if total_count > 0:
                        current_percentage = (combined_count / total_count) * 100
                        if current_percentage < max_down_percentage:
                            manager.issue_reboot(node_name)
                        else:
                            logger.debug(
                                f"Deferring reboot of '{node_name}' due to rate limit"
                            )

                # Monitor rebooting nodes
                rebooting_nodes = manager.get_nodes_by_state(
                    RebootState.REBOOTING, partition
                )
                for node_name in rebooting_nodes:
                    if manager.monitor_node_recovery(node_name):
                        manager.complete_node_reboot(node_name)

                # Display reboot summary
                summary = manager.get_reboot_summary(partition)
                if any(count > 0 for count in summary.values()):
                    logger.info("Reboot status summary:")
                    for state, count in summary.items():
                        if count > 0:
                            logger.info(f"  {state}: {count}")

            # Save state after each iteration (always, not just when reboots enabled)
            manager.save_state()

            # Periodically clean up old nodes
            if enable_reboots:
                if iteration % 10 == 0:
                    manager.clear_completed_nodes(max_age_hours=24)

            # Display unavailable and down node statistics
            unavailable_count, total_count, unavailable_nodes = (
                manager.count_unavailable_nodes(partition)
            )
            down_count, _, down_nodes = manager.count_down_nodes_slurm(partition)
            combined_nodes = unavailable_nodes.union(down_nodes)
            combined_count = len(combined_nodes)
            overlap_count = len(unavailable_nodes.intersection(down_nodes))

            if total_count > 0:
                unavailable_pct = unavailable_count / total_count * 100
                down_pct = down_count / total_count * 100
                combined_pct = combined_count / total_count * 100

                logger.info(
                    f"Partition '{partition}' status: "
                    f"unavailable: {unavailable_count}/{total_count} ({unavailable_pct:.1f}%), "
                    f"down: {down_count}/{total_count} ({down_pct:.1f}%), "
                    f"combined: {combined_count}/{total_count} ({combined_pct:.1f}%), "
                    f"overlap: {overlap_count}"
                )

            # Wait before next iteration
            if max_iterations is None or iteration < max_iterations:
                logger.debug(f"Sleeping for {interval} seconds...")
                time.sleep(interval)

    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down gracefully...")

        logger.info("Saving final state before exit...")
        manager.save_state()

        if enable_reboots:
            logger.info("Final reboot summary:")
            summary = manager.get_reboot_summary(partition)
            for state, count in summary.items():
                if count > 0:
                    logger.info(f"  {state}: {count}")

    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        try:
            manager.save_state()
            logger.info(f"State saved to {state_file} before exit")
        except Exception:
            pass
        raise


# ==================== CLI ====================


@click.command()
@click.option(
    "--interval",
    "-i",
    type=int,
    default=60,
    help="Time in seconds between checks",
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
    "--once",
    is_flag=True,
    help="Run once and exit (equivalent to --max-iterations=1)",
)
@click.option(
    "--nodelist",
    type=str,
    default=None,
    help="Filter nodes by Slurm nodelist format (e.g., node[1-10,15]). Default: all nodes",
)
@click.option(
    "--partition",
    "-p",
    type=str,
    required=True,
    help="Slurm partition to manage (required)",
)
@click.option(
    "--max-down-percentage",
    type=float,
    default=10.0,
    help="Maximum percentage of nodes that can be unavailable (maintenance) or down (Slurm state) at once",
    show_default=True,
)
@click.option(
    "--reservation-duration",
    "-d",
    default="6:00:00",
    help="Duration for maintenance reservations (HH:MM:SS format)",
    show_default=True,
)
@click.option(
    "--reservation-lead-time",
    type=int,
    default=60,
    help="Minutes before reservation start to count node as 'down'",
    show_default=True,
)
@click.option(
    "--reboot-timeout",
    type=int,
    default=600,
    help="Timeout in seconds to wait for a node to come back up",
    show_default=True,
)
@click.option(
    "--enable-reservations",
    is_flag=True,
    help="Enable creation of maintenance reservations",
)
@click.option(
    "--enable-reboots",
    is_flag=True,
    help="Enable actual reboot processing",
)
@click.option(
    "--terminate-preemptable-jobs",
    is_flag=True,
    help="Allow termination of preemptable jobs",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Don't actually create reservations or terminate jobs",
)
@click.option(
    "--state-file",
    type=click.Path(),
    default=None,
    help="Path to JSON file for persisting state (default: /var/tmp/slurm-maint-{partition}.json)",
)
@click.option(
    "--user",
    "-u",
    default=getpass.getuser(),
    help="Username(s) for reservations",
)
@click.option(
    "--account",
    "-A",
    help="Account(s) for reservations",
)
@click.option(
    "--flags",
    "-f",
    help="Reservation flags (e.g., MAINT,IGNORE_JOBS)",
)
@click.option(
    "--verbose",
    "-v",
    count=True,
    help="Increase verbosity (can be repeated: -v for INFO, -vv for DEBUG)",
)
def main(
    interval: int,
    max_iterations: Optional[int],
    once: bool,
    nodelist: Optional[str],
    partition: str,
    max_down_percentage: float,
    reservation_duration: str,
    reservation_lead_time: int,
    reboot_timeout: int,
    enable_reservations: bool,
    enable_reboots: bool,
    terminate_preemptable_jobs: bool,
    dry_run: bool,
    state_file: Optional[str],
    user: Optional[str],
    account: Optional[str],
    flags: Optional[str],
    verbose: int,
) -> None:
    """
    Slurm Maintenance Operator - Integrated reservation and reboot management.

    This operator creates maintenance reservations for nodes based on running jobs,
    then orchestrates rolling reboots while respecting a maximum percentage of nodes
    that can be "down" at any time.

    A node is considered "down" if it:
    - Has an active maintenance reservation
    - Has a reservation starting within --reservation-lead-time minutes
    - Is currently being rebooted
    - Is pending reboot

    Examples:

        # Monitor only (no reservations or reboots)
        python maint_operator.py

        # Create reservations and reboot nodes
        python maint_operator.py --enable-reservations --enable-reboots

        # Manage specific partition with 20% max down
        python maint_operator.py -p compute --max-down-percentage 20 --enable-reservations --enable-reboots

        # Dry run to see what would happen
        python maint_operator.py --dry-run --enable-reservations --enable-reboots --once

        # Full automation with preemptable job termination
        python maint_operator.py --enable-reservations --enable-reboots --terminate-preemptable-jobs
    """
    # Auto-generate state file name if not provided
    if state_file is None:
        state_file = f"/var/tmp/slurm-maint-{partition}.json"

    # Configure logger
    logger.remove()
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

    logger.info(f"State file: {state_file}")

    # Override max_iterations if --once flag is set
    if once:
        max_iterations = 1

    # Validate parameters
    if interval < 1:
        logger.error("Interval must be at least 1 second")
        sys.exit(1)

    if max_down_percentage <= 0 or max_down_percentage > 100:
        logger.error("Max down percentage must be between 0 and 100")
        sys.exit(1)

    if reservation_lead_time < 0:
        logger.error("Reservation lead time must be non-negative")
        sys.exit(1)

    # Validate nodelist if provided
    if nodelist:
        try:
            nodes = expand_slurm_nodelist(nodelist)
            logger.debug(f"Nodelist is valid Slurm format ({len(nodes)} nodes)")
        except Exception as e:
            logger.error(f"Invalid Slurm nodelist format: {e}")
            sys.exit(1)

    # Run the operator
    run_operator(
        interval=interval,
        max_iterations=max_iterations,
        nodelist=nodelist,
        partition=partition,
        max_down_percentage=max_down_percentage,
        reservation_duration=reservation_duration,
        reservation_lead_time=reservation_lead_time,
        reboot_timeout=reboot_timeout,
        enable_reservations=enable_reservations,
        enable_reboots=enable_reboots,
        terminate_preemptable_jobs=terminate_preemptable_jobs,
        dry_run=dry_run,
        state_file=state_file,
        user=user,
        account=account,
        flags=flags,
    )


if __name__ == "__main__":
    main()
