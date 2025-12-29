#!/usr/bin/env python3.9
"""
Slurm Reservation Manager

This script queries all Slurm nodes for the longest running job and creates
an advanced reservation starting when that job finishes.
"""

import re
import subprocess
import sys
import getpass

from dataclasses import dataclass
from pathlib import Path

from sre_parse import parse
from types import LambdaType
from typing import Dict, List, Optional, Tuple

import click
import pendulum
from loguru import logger


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
            f"ends at {self.end_time.to_datetime_string()}, "
        )


class JobList:
    """Container for a list of JobInfo instances with filtering methods using bitmasks."""

    def __init__(self, jobs: Optional[List[JobInfo]] = None):
        """
        Initialize the JobList.

        Args:
            jobs: List of JobInfo instances
        """
        self.jobs = jobs if jobs is not None else []
        self._rebuild_indexes()

    def _rebuild_indexes(self) -> None:
        """Rebuild the bitmask indexes for all filterable fields."""
        # Dictionary mapping field values to bitmasks
        self._qos_index: Dict[str, int] = {}
        self._state_index: Dict[str, int] = {}
        self._partition_index: Dict[str, int] = {}
        self._user_index: Dict[str, int] = {}
        self._account_index: Dict[str, int] = {}

        # Build bitmasks for each job
        for i, job in enumerate(self.jobs):
            bit = 1 << i

            if job.qos not in self._qos_index:
                self._qos_index[job.qos] = 0
            self._qos_index[job.qos] |= bit

            if job.state not in self._state_index:
                self._state_index[job.state] = 0
            self._state_index[job.state] |= bit

            if job.partition not in self._partition_index:
                self._partition_index[job.partition] = 0
            self._partition_index[job.partition] |= bit

            if job.user not in self._user_index:
                self._user_index[job.user] = 0
            self._user_index[job.user] |= bit

            if job.account not in self._account_index:
                self._account_index[job.account] = 0
            self._account_index[job.account] |= bit

    def filter(
        self,
        qos: Optional[str] = None,
        state: Optional[str] = None,
        partition: Optional[str] = None,
        user: Optional[str] = None,
        account: Optional[str] = None,
    ) -> List[JobInfo]:
        """
        Filter jobs by multiple criteria using bitmasks.

        Args:
            qos: Quality of Service to filter by (prefix with ! to negate)
            state: Job state to filter by (prefix with ! to negate)
            partition: Partition name to filter by (prefix with ! to negate)
            user: Username to filter by (prefix with ! to negate)
            account: Account to filter by (prefix with ! to negate)

        Returns:
            List of JobInfo instances matching all specified criteria
        """
        if not self.jobs:
            return []

        # Start with all jobs matching (all bits set for valid positions)
        result_mask = (1 << len(self.jobs)) - 1

        # Apply each filter by ANDing the masks
        if qos is not None:
            if qos.startswith("!"):
                # Negate: match all jobs that don't have this qos
                actual_qos = qos[1:]
                qos_mask = self._qos_index.get(actual_qos, 0)
                result_mask &= ~qos_mask & ((1 << len(self.jobs)) - 1)
            else:
                result_mask &= self._qos_index.get(qos, 0)

        if state is not None:
            if state.startswith("!"):
                # Negate: match all jobs that don't have this state
                actual_state = state[1:]
                state_mask = self._state_index.get(actual_state, 0)
                result_mask &= ~state_mask & ((1 << len(self.jobs)) - 1)
            else:
                result_mask &= self._state_index.get(state, 0)

        if partition is not None:
            if partition.startswith("!"):
                # Negate: match all jobs that don't have this partition
                actual_partition = partition[1:]
                partition_mask = self._partition_index.get(actual_partition, 0)
                result_mask &= ~partition_mask & ((1 << len(self.jobs)) - 1)
            else:
                result_mask &= self._partition_index.get(partition, 0)

        if user is not None:
            if user.startswith("!"):
                # Negate: match all jobs that don't have this user
                actual_user = user[1:]
                user_mask = self._user_index.get(actual_user, 0)
                result_mask &= ~user_mask & ((1 << len(self.jobs)) - 1)
            else:
                result_mask &= self._user_index.get(user, 0)

        if account is not None:
            if account.startswith("!"):
                # Negate: match all jobs that don't have this account
                actual_account = account[1:]
                account_mask = self._account_index.get(actual_account, 0)
                result_mask &= ~account_mask & ((1 << len(self.jobs)) - 1)
            else:
                result_mask &= self._account_index.get(account, 0)

        # Extract jobs from the result mask
        result = []
        for i, job in enumerate(self.jobs):
            if result_mask & (1 << i):
                result.append(job)

        return result

    def add_job(self, job: JobInfo) -> None:
        """
        Add a job to the list.

        Args:
            job: JobInfo instance to add
        """
        self.jobs.append(job)
        self._rebuild_indexes()

    def get_longest_job(
        self,
        qos: Optional[str] = None,
        state: Optional[str] = None,
        partition: Optional[str] = None,
        user: Optional[str] = None,
        account: Optional[str] = None,
    ) -> Optional[JobInfo]:
        """
        Get the job with the longest remaining time (latest end time).

        Args:
            qos: Quality of Service to filter by (prefix with ! to negate)
            state: Job state to filter by (prefix with ! to negate)
            partition: Partition name to filter by (prefix with ! to negate)
            user: Username to filter by (prefix with ! to negate)
            account: Account to filter by (prefix with ! to negate)

        Returns:
            JobInfo object or None if no jobs match the filter criteria
        """
        filtered_jobs = self.filter(
            qos=qos,
            state=state,
            partition=partition,
            user=user,
            account=account,
        )

        if not filtered_jobs:
            return None
        return max(filtered_jobs, key=lambda j: j.end_time)

    def __len__(self) -> int:
        """Return the number of jobs in the list."""
        return len(self.jobs)

    def __iter__(self):
        """Make JobList iterable."""
        return iter(self.jobs)

    def __getitem__(self, index: int) -> JobInfo:
        """Allow indexing into the job list."""
        return self.jobs[index]


@dataclass
class Reservation:
    """Information about a Slurm reservation."""

    name: str
    nodes: List[str]
    start_time: pendulum.DateTime
    duration: pendulum.Duration
    user: Optional[str] = None
    account: Optional[str] = None
    flags: Optional[str] = None

    def to_command(self) -> List[str]:
        """Generate scontrol command to create the reservation."""
        cmd = [
            "scontrol",
            "create",
            "reservation",
            f"ReservationName={self.name}",
            f"StartTime={self.start_time.format('YYYY-MM-DDTHH:mm:ss')}",
            f"Duration={self._format_duration()}",
            f"Nodes={','.join(self.nodes)}",
        ]

        if self.user:
            cmd.append(f"Users={self.user}")

        if self.account:
            cmd.append(f"Accounts={self.account}")

        if self.flags:
            cmd.append(f"Flags={self.flags}")

        return cmd

    def to_update_command(self) -> List[str]:
        """Generate scontrol command to update an existing reservation."""
        cmd = [
            "scontrol",
            "update",
            "reservation",
            f"ReservationName={self.name}",
            f"StartTime={self.start_time.format('YYYY-MM-DDTHH:mm:ss')}",
            f"Duration={self._format_duration()}",
            f"Nodes={','.join(self.nodes)}",
        ]

        if self.user:
            cmd.append(f"Users={self.user}")

        if self.account:
            cmd.append(f"Accounts={self.account}")

        if self.flags:
            cmd.append(f"Flags={self.flags}")

        return cmd

    def _format_duration(self) -> str:
        """Format duration in Slurm format (days-hours:minutes:seconds)."""
        total_seconds = int(self.duration.total_seconds())
        days = total_seconds // 86400
        hours = (total_seconds % 86400) // 3600
        minutes = (total_seconds % 3600) // 60
        seconds = total_seconds % 60

        if days > 0:
            return f"{days}-{hours:02d}:{minutes:02d}:{seconds:02d}"
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

    def __str__(self) -> str:
        return (
            f"Reservation '{self.name}' for {self.nodes}: "
            f"starts {self.start_time.to_datetime_string()}, "
            f"duration {self._format_duration()}"
        )

def expand_node_spec(node_spec: str) -> Tuple[str, ...]:
    """
    Expand a Slurm-style node expression into a tuple of node names.

    Args:
        node_spec: Expression such as 'sdfmilan[104,107-108,113,116]'.

    Returns:
        Tuple of expanded node names, e.g.
        ('sdfmilan104', 'sdfmilan107', 'sdfmilan108', 'sdfmilan113', 'sdfmilan116').

    Raises:
        ValueError: If the node specification contains malformed ranges.
    """
    if node_spec is None:
        return tuple()

    node_spec = node_spec.strip()
    if not node_spec:
        return tuple()

    match = re.fullmatch(r"([^\[\]]+)\[(.+)\]", node_spec)
    if not match:
        return (node_spec,)

    prefix, range_part = match.groups()
    nodes: List[str] = []

    for chunk in range_part.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue

        if "-" in chunk:
            start_str, end_str = chunk.split("-", 1)
            if not start_str or not end_str:
                raise ValueError(f"Invalid range segment '{chunk}' in '{node_spec}'")

            width = max(len(start_str), len(end_str))
            try:
                start = int(start_str)
                end = int(end_str)
            except ValueError as exc:
                raise ValueError(f"Non-numeric range bounds in '{chunk}'") from exc

            step = 1 if end >= start else -1
            for value in range(start, end + step, step):
                nodes.append(f"{prefix}{str(value).zfill(width)}")
        else:
            try:
                value = int(chunk)
                nodes.append(f"{prefix}{str(value).zfill(len(chunk))}")
            except ValueError:
                nodes.append(f"{prefix}{chunk}")

    return tuple(nodes)


class SlurmController:
    """Controller for interacting with Slurm cluster."""

    def __init__(self, dry_run: bool = False):
        """
        Initialize the Slurm controller.

        Args:
            dry_run: If True, don't actually create reservations
        """
        self.dry_run = dry_run
        logger.info(f"Initialized SlurmController (dry_run={dry_run})")

    def get_all_nodes(self) -> List[str]:
        """
        Get list of all nodes in the cluster.

        Returns:
            List of node names
        """
        logger.info("Querying all Slurm nodes")

        try:
            result = subprocess.run(
                ["sinfo", "-N", "-h", "-o", "%N"],
                capture_output=True,
                text=True,
                check=True,
            )

            nodes = [
                line.strip() for line in result.stdout.splitlines() if line.strip()
            ]
            logger.info(f"Found {len(nodes)} nodes")
            return nodes

        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to get nodes: {e.stderr}")
            raise

    def get_jobs_on_node(self, node: str) -> JobList:
        """
        Get all running jobs on a specific node.

        Args:
            node: Node name

        Returns:
            JobList object containing jobs
        """
        logger.debug(f"Querying jobs on node {node}")

        job_list = JobList()

        try:
            result = subprocess.run(
                [
                    "squeue",
                    "-w",
                    node,
                    "-h",
                    "-o",
                    "%i|%N|%S|%l|%u|%P|%T|%q|%a",
                    "-t",
                    "RUNNING",
                ],
                capture_output=True,
                text=True,
                check=True,
            )

            for line in result.stdout.splitlines():
                if not line.strip():
                    continue

                parts = line.strip().split("|")
                if len(parts) != 9:
                    logger.warning(f"Skipping malformed line: {line}")
                    continue

                (
                    job_id,
                    job_node,
                    start_time_str,
                    time_limit_str,
                    user,
                    partition,
                    state,
                    qos,
                    account,
                ) = parts

                # Parse start time
                try:
                    start_time = pendulum.parse(start_time_str)
                except Exception as e:
                    logger.warning(
                        f"Could not parse start time '{start_time_str}': {e}"
                    )
                    continue

                # Parse time limit
                try:
                    time_limit = self._parse_time_limit(time_limit_str)
                except Exception as e:
                    logger.warning(
                        f"Could not parse time limit '{time_limit_str}': {e}"
                    )
                    continue

                job = JobInfo(
                    job_id=job_id,
                    node=job_node,
                    start_time=start_time,
                    time_limit=time_limit,
                    user=user,
                    partition=partition,
                    qos=qos,
                    state=state,
                    account=account,
                )
                job_list.add_job(job)
                logger.debug(f"found: {job}")

            logger.debug(f"Found {len(job_list)} running jobs on {node}")
            return job_list

        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to get jobs for node {node}: {e.stderr}")
            return job_list

    def _parse_time_limit(self, time_str: str) -> pendulum.Duration:
        """
        Parse Slurm time limit string.

        Formats:
        - minutes
        - minutes:seconds
        - hours:minutes:seconds
        - days-hours:minutes:seconds

        Args:
            time_str: Time limit string from Slurm

        Returns:
            pendulum.Duration object
        """
        days = 0
        hours = 0
        minutes = 0
        seconds = 0

        # Check for days
        if "-" in time_str:
            day_part, time_part = time_str.split("-", 1)
            days = int(day_part)
            time_str = time_part

        # Split time components
        parts = time_str.split(":")

        if len(parts) == 1:
            minutes = int(parts[0])
        elif len(parts) == 2:
            minutes = int(parts[0])
            seconds = int(parts[1])
        elif len(parts) == 3:
            hours = int(parts[0])
            minutes = int(parts[1])
            seconds = int(parts[2])
        else:
            raise ValueError(f"Invalid time format: {time_str}")

        return pendulum.duration(
            days=days, hours=hours, minutes=minutes, seconds=seconds
        )

    def find_longest_job(
        self,
        node: str = None,
        qos: Optional[str] = None,
        state: Optional[str] = None,
        partition: Optional[str] = None,
        user: Optional[str] = None,
        account: Optional[str] = None,
    ) -> Optional[JobInfo]:
        """
        Find the job with the longest remaining time on a specific node.

        Args:
            node: Node name to search for jobs
            qos: Quality of Service to filter by
            state: Job state to filter by
            partition: Partition name to filter by
            user: Username to filter by
            account: Account to filter by

        Returns:
            JobInfo object or None if no jobs found
        """
        job_list = self.get_jobs_on_node(node)

        if len(job_list) == 0:
            logger.warning(f"No running jobs found on node {node}")
            return None

        # Find job with latest end time using indexed fields
        longest_job = job_list.get_longest_job(
            qos=qos,
            state=state,
            partition=partition,
            user=user,
            account=account,
        )

        return longest_job

    def terminate_job(self, job: JobInfo) -> bool:
        """
        Terminate a Slurm job.

        Args:
            job: JobInfo object representing the job to terminate

        Returns:
            True if successful, False otherwise
        """

        cmd = ["scancel", job.job_id]

        if self.dry_run:
            logger.warning(
                f"DRY RUN: Terminating job {job.job_id} on node {job.node}: {' '.join(cmd)}"
            )
            return True

        try:
            # result = subprocess.run(
            #     cmd,
            #     capture_output=True,
            #     text=True,
            #     check=True,
            # )
            logger.warning(f"TO BE IMPLEMENTED: Job {job.job_id} terminated successfully")
            return True

        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to terminate job {job.job_id}: {e.stderr}")
            return False

    def _reservation_exists(self, name: str) -> bool:
        """
        Determine whether a reservation already exists.

        Args:
            name: Reservation name

        Returns:
            True if the reservation exists, False otherwise.
        """
        try:
            result = subprocess.run(
                ["scontrol", "show", "reservation", name],
                capture_output=True,
                text=True,
                check=True,
            )
            return bool(result.stdout.strip())
        except subprocess.CalledProcessError as e:
            if "Invalid reservation" in (e.stderr or ""):
                return False
            logger.error(f"Failed to check reservation '{name}': {e.stderr}")
            raise

    def create_reservation(self, reservation: Reservation) -> bool:
        """
        Create or update a Slurm reservation.

        Args:
            reservation: Reservation object

        Returns:
            True if successful, False otherwise
        """
        try:
            exists = self._reservation_exists(reservation.name)
        except subprocess.CalledProcessError:
            return False

        action = "Updating" if exists else "Creating"
        cmd = reservation.to_update_command() if exists else reservation.to_command()

        logger.info(f"{action} reservation: {reservation}")
        logger.debug(f"Command: {' '.join(cmd)}")

        if self.dry_run:
            logger.warning("DRY RUN: Would execute: " + " ".join(cmd))
            return True

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True
            )
            logger.success(
                f"Reservation {action.lower()} succeeded: {result.stdout}"
            )
            return True

        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to {action.lower()} reservation: {e.stderr}")
            return False


def parse_duration_string(time_str: str) -> pendulum.Duration:
    """Convert HH:MM:SS string to Pendulum Duration."""
    hours, minutes, seconds = map(int, time_str.split(":"))
    return pendulum.duration(hours=hours, minutes=minutes, seconds=seconds)


def run_reservation_manager(
    nodes: Tuple[str, ...],
    duration: str,
    user: Optional[str],
    account: Optional[str],
    flags: Optional[str],
    dry_run: bool,
    log_level: str,
    terminate_preemptable_jobs: bool,
) -> None:
    """
    Main logic for managing Slurm reservations based on running jobs.

    This function performs the following steps for each node:
    1. Queries all running jobs on the node
    2. Separates jobs into preemptable and non-preemptable categories
    3. Determines reservation start time based on job types:
       - If only preemptable jobs exist: terminates all and starts immediately
       - If both types exist: terminates preemptable jobs that would run past
         non-preemptable jobs, and schedules reservation after longest non-preemptable job
       - If only non-preemptable jobs exist: schedules reservation after longest job
    4. Creates or updates a maintenance reservation for the specified duration

    Args:
        nodes: Tuple of node specifications (e.g., 'node[1-10]'). If empty, uses all cluster nodes.
        duration: Duration string in HH:MM:SS format for the reservation length.
        user: Username(s) to associate with the reservation.
        account: Account(s) to associate with the reservation.
        flags: Slurm reservation flags (e.g., 'MAINT', 'IGNORE_JOBS').
        dry_run: If True, only logs actions without executing them.
        log_level: Logging verbosity level (not currently used in this function).
        terminate_preemptable: If True, allows termination of preemptable jobs.

    Returns:
        None
    """

    # Initialize controller
    controller = SlurmController(dry_run=dry_run)
    node_names = []

    if nodes:
        expanded_nodes: List[str] = []
        for node_spec in nodes:
            try:
                expanded = expand_node_spec(node_spec)
            except ValueError as exc:
                logger.error(
                    f"Failed to expand node specification '{node_spec}': {exc}"
                )
                continue
            if not expanded:
                logger.warning(f"No nodes matched specification '{node_spec}'")
            expanded_nodes.extend(expanded)
        node_names = list(dict.fromkeys(expanded_nodes))
        if not nodes:
            logger.warning("No valid nodes derived from user input; exiting.")
            return
        logger.info(f"Using user-specified nodes: {', '.join(node_names)}")
    else:
        # Get all nodes in the cluster
        node_names = controller.get_all_nodes()

    for node_name in node_names:

        reservation_start_time = pendulum.now()

        # Find longest job across all nodes
        job_list = controller.get_jobs_on_node(node_name)
        preemptable_jobs = JobList(job_list.filter(qos="preemptable"))
        longest_preemptable_job = preemptable_jobs.get_longest_job()
        non_preemptable_jobs = JobList(job_list.filter(qos="!preemptable"))
        longest_non_preemptable_job = non_preemptable_jobs.get_longest_job()
        logger.info(
            f"Longest preemptable job on {node_name}: {longest_preemptable_job}"
        )
        logger.info(
            f"Longest non-preemptable job on {node_name}: {longest_non_preemptable_job}"
        )

        # if only preemptable jobs, then terminate all jobs on node
        if len(non_preemptable_jobs) == 0:

            if terminate_preemptable_jobs:
                logger.info(f"No non-preemptable jobs on {node_name}, terminating all jobs")
                for preemptable_job in preemptable_jobs:
                    controller.terminate_job(preemptable_job)

                reservation_start_time = pendulum.now() + pendulum.duration(seconds=1)
            else:

                # TODO: problem is that we can't set a reservation if there are jobs already running the time frame
                logger.info(f"No non-preemptable jobs on {node_name}, but termination disabled")
                if longest_preemptable_job:
                    reservation_start_time = longest_preemptable_job.end_time
                else:
                    reservation_start_time = pendulum.now() + pendulum.duration(seconds=1)

        # If there's a non-preemptable job and preemptable jobs exist
        elif longest_non_preemptable_job and preemptable_jobs:

            if terminate_preemptable_jobs:
                # Check if any preemptable job has an end_time after the non-preemptable job
                for preemptable_job in preemptable_jobs:
                    if preemptable_job.end_time > longest_non_preemptable_job.end_time:
                        logger.info(
                            f"Preemptable job {preemptable_job.job_id} ends after non-preemptable job, terminating all preemptable job"
                        )
                        # Terminate all preemptable jobs on this node
                        controller.terminate_job(preemptable_job)

            reservation_start_time = longest_non_preemptable_job.end_time

        else:
            logger.warning(f"Only non-preemptable jobs on {node_name}")

            reservation_start_time = longest_non_preemptable_job.end_time

        # TODO: do i need to wait for the jobs to finish first?

        # set the advanced reservation to the end_time of the longest non-preemptable job
        pendulum_duration = parse_duration_string(duration)
        reservation = Reservation(
            name=f"maint:{node_name}",
            nodes=[node_name],
            start_time=reservation_start_time,
            duration=pendulum_duration,
            user=user,
            account=account,
            flags=flags,
        )

        logger.warning(f"Will create reservation {reservation}")
        controller.create_reservation(reservation)



@click.command()
@click.option(
    "--nodes",
    "-n",
    multiple=True,
    help="Specific node(s) to consider (can be provided multiple times)",
)
@click.option(
    "--duration",
    "-d",
    default="6:00:00",
    help="Duration for the reservation (HH:MM:SS format)",
)
@click.option(
    "--user",
    "-u",
    default=getpass.getuser(),
    help="Username(s) for the reservation",
)
@click.option(
    "--account",
    "-A",
    help="Account(s) for the reservation",
)
@click.option("--flags", "-f", help="Reservation flags")
@click.option("--dry-run", is_flag=True, help="Don't actually create the reservation")
@click.option(
    "--terminate-preemptable-jobs",
    is_flag=True,
    default=False,
    help="Allow termination of preemptable jobs",
)
@click.option(
    "--log-level",
    type=click.Choice(
        ["TRACE", "DEBUG", "INFO", "SUCCESS", "WARNING", "ERROR", "CRITICAL"]
    ),
    default="INFO",
    help="Logging level",
)

# main
def main(
    nodes: Tuple[str, ...],
    duration: str,
    user: Optional[str],
    account: Optional[str],
    flags: Optional[str],
    dry_run: bool,
    terminate_preemptable_jobs: bool,
    log_level: str,
) -> None:
    """
    CLI entry point for Slurm Reservation Manager.
    """

    # Configure logging
    logger.remove()
    logger.add(
        # lambda msg: click.echo(msg, err=True),
        sys.stdout,
        level=log_level,
        colorize=True,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
    )
    logger.info("Starting Slurm Reservation Manager")

    run_reservation_manager(
        nodes=nodes,
        duration=duration,
        user=user,
        account=account,
        flags=flags,
        dry_run=dry_run,
        log_level=log_level,
        terminate_preemptable_jobs=terminate_preemptable_jobs,
    )


if __name__ == "__main__":
    main()
