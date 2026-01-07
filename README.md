# Slurm Maintenance Operator

`maint_operator.py` is an integrated tool that automates the creation of maintenance reservations and orchestrates rolling reboots of Slurm compute nodes while maintaining cluster availability.

## What It Does

The operator combines two key functions:

1. **Creates maintenance reservations** for nodes based on running jobs, automatically scheduling reservations to start after jobs complete
2. **Orchestrates rolling reboots** of nodes while ensuring that no more than a specified percentage of nodes are unavailable at any time

### Node Availability Management

The operator carefully manages node availability by tracking nodes in various states:

- **Nodes with active maintenance reservations**
- **Nodes with reservations starting soon** (within the lead time window)
- **Nodes currently being rebooted**
- **Nodes in Slurm's DOWN state**

It ensures the total percentage of unavailable nodes never exceeds the configured threshold, maintaining cluster capacity for jobs.

## How It Works

The operator runs in a continuous loop (or once with `--once`) and performs these steps each iteration:

1. **Updates existing reservations** - Adjusts reservation start times based on current job schedules
2. **Creates new reservations** - For nodes without reservations, creates them to start after jobs complete
3. **Initiates reboots** - When reservations become active and capacity permits, marks nodes for reboot
4. **Monitors recovery** - Tracks nodes during reboot and verifies they return to service
5. **Manages state** - Persists reboot state to disk for resilience across restarts

### Reservation Creation Logic

For each node, the operator determines the optimal reservation start time:

- **No jobs running**: Start reservation immediately
- **Only preemptable jobs**: 
  - If `--terminate-preemptable-jobs` is set: Start immediately (jobs will be terminated)
  - Otherwise: Start after the longest preemptable job completes
- **Non-preemptable jobs present**: Wait for the longest non-preemptable job to complete

### Reboot Orchestration

The operator maintains a state machine for each node being rebooted:

```
Node Discovery
    ↓
PENDING_REBOOT ──capacity available──> REBOOTING ──node down──> RECOVERING ──node up──> COMPLETED
                                           │                          │
                                           └──timeout──> FAILED       └──timeout──> FAILED
```

**State Descriptions:**

- **PENDING_REBOOT**: Node identified for reboot, queued and waiting for capacity to become available
- **REBOOTING**: Reboot command issued, waiting for node to go down (become unreachable)
- **RECOVERING**: Node is down, waiting for it to come back up and return to service
- **COMPLETED**: Node has successfully rebooted and is back in service, verified via Slurm state
- **FAILED**: Reboot timed out or failed after maximum retry attempts (default: 3 attempts)

**State Transitions:**

1. New nodes are discovered in maintenance reservations and marked as **PENDING_REBOOT**
2. When capacity is available (under `max-down-percentage`), the operator transitions the node to **REBOOTING** and issues the reboot command
3. The operator monitors for the node to go down (become unreachable), then transitions to **RECOVERING**
4. Once the node comes back online and Slurm reports it as healthy, it transitions to **COMPLETED**
5. If any step times out (default: 600 seconds), the node may retry or transition to **FAILED**

**Retry Logic:**

- Each node gets up to 3 reboot attempts
- Failed attempts return the node to **PENDING_REBOOT** for retry
- After 3 failures, the node is marked as **FAILED** and requires manual intervention

## Understanding Node Availability

### UNAVAILABLE vs DOWN Terminology

The operator distinguishes between two types of node unavailability:

#### UNAVAILABLE Nodes

Nodes that are **not actively being used by Slurm for jobs** due to maintenance activities:

- **Nodes with active maintenance reservations** - Currently in a maintenance reservation
- **Nodes with reservations starting soon** - Within the `--reservation-lead-time` window (default: 60 minutes)
- **Nodes being rebooted** - In `PENDING_REBOOT`, `REBOOTING`, or `RECOVERING` states
- **Nodes managed by this operator** - Tracked in the state file

#### DOWN Nodes

Nodes that **Slurm considers offline or not accessible** according to Slurm's native state:

- `DOWN` - Node is not responding
- `DRAIN`, `DRAINED`, `DRAINING` - Node is being drained or has been drained
- `FAIL`, `FAILING` - Node has failed
- `NOT_RESPONDING`, `NO_RESPOND` - Node is not responding to Slurm
- `POWER_DOWN`, `POWERING_DOWN` - Node is powered down or powering down

The operator also correctly handles composite states like `IDLE+DRAIN` or `DOWN*`.

### How the Operator Enforces Capacity Limits

The `--max-down-percentage` limit applies to the **combined** count of unavailable and down nodes:

1. **Count unavailable nodes** - Nodes in maintenance reservations or being rebooted
2. **Count down nodes** - Nodes in Slurm's DOWN states (queried via `sinfo`)
3. **Calculate unique combined count** - Count nodes that are either unavailable OR down (avoiding double-counting)
4. **Compare to limit** - Only proceed if `(combined_count / total_nodes * 100) < max_down_percentage`

**Example Scenario:**

Partition with 100 nodes, `max-down-percentage=10%`:

```
Unavailable nodes: 5 (in maintenance reservations)
Down nodes: 8 (hardware failures)
Overlap: 2 (nodes that are both in maintenance AND down)
Combined unique: 5 + 8 - 2 = 11 nodes

Combined percentage: 11/100 = 11%
```

Result: The operator **would NOT** create new reservations because 11% > 10% limit.

This approach ensures:
- **Cluster awareness** - The operator detects non-maintenance issues affecting the cluster
- **Conservative scheduling** - Won't add maintenance load when cluster is already degraded
- **Better visibility** - Operators see both maintenance and non-maintenance unavailability
- **Overlap detection** - Identifies nodes experiencing problems during maintenance
- **Graceful degradation** - Automatically backs off during cluster issues

### Monitoring Output

Each iteration logs detailed partition statistics:

```
Partition 'compute' status: unavailable: 5/100 (5.0%), down: 8/100 (8.0%), combined: 11/100 (11.0%), overlap: 2
```

This shows:
- Capacity unavailable due to maintenance
- Capacity down due to other issues (hardware, network, etc.)
- Total impact on cluster capacity
- Number of nodes experiencing both issues

## Partition-Aware Rate Limiting

The operator enforces limits **per-partition**, not cluster-wide:

- Each partition is tracked independently
- One partition's issues don't prevent maintenance in other partitions
- Percentage calculations use the total nodes in each specific partition
- Example: With 100 compute nodes at 10% limit, max 10 compute nodes can be unavailable

**Algorithm:**

```
Current Unavailable/Down = Count of nodes unavailable OR down in partition
Max Allowed = (Total Partition Nodes × max_down_percentage) / 100

Can Proceed? = (Current Unavailable/Down < Max Allowed)
```

## Usage

### Basic Command Structure

```bash
python maint_operator.py [OPTIONS]
```

### Common Usage Patterns

#### Monitor Mode (No Actions)
Monitor nodes and reservations without making any changes:
```bash
python maint_operator.py -p compute
```

#### Dry Run
See what would happen without actually performing actions:
```bash
python maint_operator.py -p compute --enable-reservations --enable-reboots --dry-run --once
```

#### Create Reservations Only
Create and manage maintenance reservations without performing reboots:
```bash
python maint_operator.py -p compute --enable-reservations
```

#### Full Automation
Create reservations and perform rolling reboots:
```bash
python maint_operator.py -p compute --enable-reservations --enable-reboots
```

#### Full Automation with Preemptable Job Termination
Terminate preemptable jobs to speed up reservation starts:
```bash
python maint_operator.py -p compute --enable-reservations --enable-reboots --terminate-preemptable-jobs
```

#### Manage Specific Nodes
Target a specific set of nodes:
```bash
python maint_operator.py -p compute --nodelist "node[001-100]" --enable-reservations --enable-reboots
```

#### Increase Parallelism
Allow up to 20% of nodes to be unavailable simultaneously:
```bash
python maint_operator.py -p compute --max-down-percentage 20 --enable-reservations --enable-reboots
```

#### Run Once and Exit
Useful for testing or manual execution:
```bash
python maint_operator.py -p compute --enable-reservations --enable-reboots --once
```

### Command-Line Options

#### Required Options
- `-p, --partition TEXT` - Slurm partition to manage (required)

#### Operational Control
- `--enable-reservations` - Enable creation of maintenance reservations (default: disabled)
- `--enable-reboots` - Enable actual reboot processing (default: disabled)
- `--terminate-preemptable-jobs` - Allow termination of preemptable jobs (default: disabled)
- `--dry-run` - Don't actually create reservations or terminate jobs (default: disabled)

#### Scheduling Options
- `-i, --interval INTEGER` - Time in seconds between checks (default: 60)
- `-n, --max-iterations INTEGER` - Maximum number of iterations (default: infinite)
- `--once` - Run once and exit (equivalent to `--max-iterations=1`)

#### Capacity Management
- `--max-down-percentage FLOAT` - Maximum percentage of nodes that can be unavailable at once (default: 10.0)
- `--reservation-lead-time INTEGER` - Minutes before reservation start to count node as unavailable (default: 60)

#### Reservation Configuration
- `-d, --reservation-duration TEXT` - Duration for maintenance reservations in HH:MM:SS format (default: 6:00:00)
- `-u, --user TEXT` - Username(s) for reservations
- `-A, --account TEXT` - Account(s) for reservations
- `-f, --flags TEXT` - Reservation flags (e.g., MAINT,IGNORE_JOBS)

#### Node Selection
- `--nodelist TEXT` - Filter nodes by Slurm nodelist format (e.g., node[1-10,15]). Default: all nodes

#### Reboot Configuration
- `--reboot-timeout INTEGER` - Timeout in seconds to wait for a node to come back up (default: 600)

#### State Management
- `--state-file PATH` - Path to JSON file for persisting state (default: /var/tmp/slurm-maint-{partition}.json)

#### Logging
- `-v, --verbose` - Increase verbosity (can be repeated: `-v` for INFO, `-vv` for DEBUG)

## State Persistence

The operator automatically saves and restores state to ensure resilience and allow resumption after interruptions.

### What's Stored

The state file (default: `/var/tmp/slurm-maint-{partition}.json`) contains:

```json
{
  "nodes": [
    {
      "node_name": "node001",
      "state": "REBOOTING",
      "partition": "compute",
      "marked_time": "2024-01-15T10:30:00Z",
      "reboot_start_time": "2024-01-15T10:32:00Z",
      "reboot_complete_time": null,
      "attempts": 1,
      "max_attempts": 3
    }
  ]
}
```

**Tracked Information:**
- Node name and partition
- Current reboot state (PENDING_REBOOT, REBOOTING, RECOVERING, COMPLETED, FAILED)
- Timestamps for state transitions
- Number of reboot attempts and maximum allowed
- When the node was marked for reboot
- When reboot was initiated and completed

### Automatic State Management

- **Automatic Save**: State is saved after each iteration and on graceful exit
- **Automatic Load**: State is restored on startup if the file exists
- **Atomic Writes**: Uses temporary files to prevent corruption during writes
- **Auto-Cleanup**: Removes old completed/failed nodes (>24 hours by default)
- **Resumable**: The operator can be stopped and restarted without losing progress

### State File Operations

```bash
# View current state
cat /var/tmp/slurm-maint-compute.json | jq .

# Count nodes by state
cat /var/tmp/slurm-maint-compute.json | jq '.nodes | group_by(.state) | map({state: .[0].state, count: length})'

# List nodes in REBOOTING state
cat /var/tmp/slurm-maint-compute.json | jq '.nodes | map(select(.state == "REBOOTING")) | .[].node_name'

# List failed nodes
cat /var/tmp/slurm-maint-compute.json | jq '.nodes | map(select(.state == "FAILED")) | .[].node_name'

# Reset state (delete file)
rm /var/tmp/slurm-maint-compute.json
```

### Interruption Handling

The operator handles interruptions gracefully:

1. **Normal Exit (Ctrl+C)**: Saves final state and displays summary
2. **Crash Recovery**: On restart, loads previous state and resumes from where it left off
3. **State Conflicts**: If a node in the state file no longer has a reservation, it's cleaned up
4. **Timeout Recovery**: Nodes that timed out are automatically retried or marked as failed

### Production Recommendations

For production deployments, use a persistent location:

```bash
python maint_operator.py -p compute \
  --enable-reservations \
  --enable-reboots \
  --state-file /var/lib/slurm/maint-operator-compute.json
```

This ensures state survives system reboots and is backed up with system data.

## Architecture and Implementation

### Core Components

The operator consists of several key classes working together:

1. **SlurmController** - Interface to Slurm commands (`scontrol`, `squeue`, `sinfo`)
   - Queries node states and partition information
   - Gets running jobs on nodes
   - Terminates preemptable jobs when configured

2. **ReservationManager** - Manages Slurm maintenance reservations
   - Creates, updates, and deletes reservations
   - Queries existing reservations
   - Validates reservation parameters

3. **MaintenanceManager** - Core orchestration logic
   - Tracks unavailable and down nodes
   - Enforces capacity limits
   - Manages reboot state machine
   - Persists state to JSON file

4. **NodeRebootStatus** - Tracks individual node reboot state
   - State transitions through reboot lifecycle
   - Retry counting and timeout tracking
   - Serialization for state persistence

### Workflow Loop

Each iteration performs these steps:

1. **Load State** - Restore previous reboot state from JSON file
2. **Query Slurm** - Get partition info, node states, reservations, and jobs
3. **Update Reservations** - Adjust existing reservations based on current job schedules
4. **Create Reservations** - Add new reservations for nodes without them
5. **Check Capacity** - Calculate unavailable/down percentage
6. **Initiate Reboots** - Start reboots for nodes with active reservations if capacity allows
7. **Monitor Recovery** - Check if rebooting nodes have come back online
8. **Update State** - Transition nodes through state machine
9. **Save State** - Persist current state to JSON file
10. **Wait** - Sleep for `--interval` seconds before next iteration

### Reboot Implementation

The operator uses two key methods that may need site-specific implementation:

#### `issue_reboot(node_name)`

Issues the actual reboot command. Current implementations to consider:

```python
# Option 1: SSH-based reboot
subprocess.run(["ssh", node_name, "sudo", "reboot"])

# Option 2: IPMI/BMC reboot
subprocess.run(["ipmitool", "-H", f"{node_name}-ipmi", "power", "cycle"])

# Option 3: Custom orchestration tool
subprocess.run(["your-reboot-tool", node_name])
```

#### `monitor_node_recovery(node_name)`

Verifies that a node has successfully rebooted and is ready for work:

```python
# Option 1: Check Slurm node state
result = subprocess.run(["sinfo", "-h", "-n", node_name, "-o", "%T"])
state = result.stdout.strip().lower()
return state in ["idle", "allocated", "mixed"]

# Option 2: Ping check
result = subprocess.run(["ping", "-c", "1", "-W", "2", node_name])
return result.returncode == 0

# Option 3: SSH connectivity
result = subprocess.run(["ssh", "-o", "ConnectTimeout=5", node_name, "uptime"])
return result.returncode == 0
```

## Safety Features

The operator includes multiple safety mechanisms:

- **Safe by default**: Requires explicit `--enable-reservations` and `--enable-reboots` flags
- **Capacity enforcement**: Never exceeds the configured percentage of unavailable/down nodes
- **Partition isolation**: One partition's issues don't affect other partitions
- **Timeout handling**: Marks nodes as FAILED if they don't recover within the timeout
- **Retry logic**: Failed reboots get up to 3 attempts before permanent failure
- **State persistence**: Maintains state across restarts, crashes, and interruptions
- **Dry-run mode**: Test configuration without making changes (`--dry-run`)
- **Granular control**: Separate flags for reservations and reboots allow staged rollout
- **Overlap detection**: Identifies nodes that are both in maintenance and experiencing other issues
- **Graceful shutdown**: Ctrl+C saves state and displays summary before exiting
- **Atomic state writes**: Uses temporary files to prevent corruption
- **Lead time buffer**: Counts nodes as unavailable before reservations start (configurable)

## Best Practices

1. **Start with monitoring**: Run without `--enable-reservations` or `--enable-reboots` to observe behavior
2. **Use dry-run mode**: Test with `--dry-run --once` before enabling actions
3. **Stage the rollout**: Enable reservations first, then add reboots once comfortable
4. **Set appropriate capacity limits**: Start with a low `--max-down-percentage` (e.g., 5-10%)
5. **Monitor the state file**: Watch the state file to track reboot progress
6. **Use appropriate timeouts**: Adjust `--reboot-timeout` based on your nodes' typical boot time
7. **Handle preemptable jobs carefully**: Only use `--terminate-preemptable-jobs` if your workload supports it
8. **Set realistic reservation durations**: Ensure `--reservation-duration` is long enough for reboots to complete

## Example Workflows

### Initial Testing
```bash
# 1. Monitor only
python maint_operator.py -p compute -vv --once

# 2. Dry run with reservations
python maint_operator.py -p compute --enable-reservations --dry-run --once -v

# 3. Dry run with reservations and reboots
python maint_operator.py -p compute --enable-reservations --enable-reboots --dry-run --once -v
```

### Production Rolling Reboot
```bash
# Conservative: 10% max unavailable, 6-hour reservations
python maint_operator.py \
  -p compute \
  --enable-reservations \
  --enable-reboots \
  --max-down-percentage 10 \
  --reservation-duration 6:00:00 \
  --reservation-lead-time 60 \
  --reboot-timeout 600 \
  -v
```

### Aggressive Rolling Reboot
```bash
# Faster: 25% max unavailable, terminate preemptable jobs
python maint_operator.py \
  -p compute \
  --enable-reservations \
  --enable-reboots \
  --terminate-preemptable-jobs \
  --max-down-percentage 25 \
  --reservation-duration 4:00:00 \
  -v
```

## Troubleshooting

### No reservations are being created
- Ensure `--enable-reservations` is set
- Check that nodes don't already have reservations
- Verify the partition name is correct
- Check logs for errors

### No reboots are happening
- Ensure both `--enable-reservations` and `--enable-reboots` are set
- Check that reservations have become active
- Verify you haven't reached the `--max-down-percentage` limit
- Check the state file for nodes in FAILED state

### Reboots are too slow
- Increase `--max-down-percentage` to allow more concurrent reboots
- Use `--terminate-preemptable-jobs` to start reservations sooner
- Reduce `--reservation-lead-time` to count nodes as unavailable closer to reservation start

### Nodes stuck in REBOOTING or RECOVERING state
- Check if nodes are actually coming back online
- Verify network connectivity to nodes
- Increase `--reboot-timeout` if nodes take longer to boot
- Manually investigate failed nodes and clear them from the state file if needed

### State file corruption
- The state file is human-readable JSON
- You can manually edit or delete it to reset state
- The operator will recreate it on next run

## Advanced Configuration

### Tuning for Different Scenarios

#### Conservative (Production, High Availability)
```bash
python maint_operator.py -p compute \
  --enable-reservations \
  --enable-reboots \
  --max-down-percentage 5 \
  --reservation-duration 8:00:00 \
  --reservation-lead-time 120 \
  --reboot-timeout 900 \
  --interval 60 \
  -v
```

Best for: Production clusters where uptime is critical

#### Balanced (Standard Maintenance)
```bash
python maint_operator.py -p compute \
  --enable-reservations \
  --enable-reboots \
  --max-down-percentage 10 \
  --reservation-duration 6:00:00 \
  --reservation-lead-time 60 \
  --reboot-timeout 600 \
  --interval 60 \
  -v
```

Best for: Regular maintenance windows, moderate urgency

#### Aggressive (Scheduled Downtime)
```bash
python maint_operator.py -p compute \
  --enable-reservations \
  --enable-reboots \
  --terminate-preemptable-jobs \
  --max-down-percentage 25 \
  --reservation-duration 4:00:00 \
  --reservation-lead-time 30 \
  --reboot-timeout 300 \
  --interval 30 \
  -v
```

Best for: Scheduled maintenance windows, fast completion needed

### Monitoring and Alerting

#### Watch Progress
```bash
# Monitor logs
tail -f /var/log/maint-operator.log

# Check state file
watch -n 5 'cat /var/tmp/slurm-maint-compute.json | jq ".nodes | group_by(.state) | map({state: .[0].state, count: length})"'

# Check Slurm reservations
watch -n 10 'scontrol show reservation | grep maint:'

# Monitor partition status
watch -n 10 'sinfo -p compute'
```

#### Key Metrics to Track
- Number of nodes in each state (PENDING, REBOOTING, RECOVERING, COMPLETED, FAILED)
- Current unavailable/down percentage
- Number of active maintenance reservations
- Time to complete full rolling reboot
- Failure rate and common failure reasons

### Continuous Operation

For 24/7 operation, create a systemd service:

```ini
# /etc/systemd/system/slurm-maint-operator@.service
[Unit]
Description=Slurm Maintenance Operator for %i partition
After=network.target slurmd.service
Wants=network.target

[Service]
Type=simple
User=slurm
WorkingDirectory=/opt/slurm-maint-controller
ExecStart=/usr/bin/python3 /opt/slurm-maint-controller/maint_operator.py \
  --partition %i \
  --enable-reservations \
  --enable-reboots \
  --max-down-percentage 10 \
  --state-file /var/lib/slurm/maint-operator-%i.json \
  -v
Restart=always
RestartSec=30
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

Enable and start:
```bash
sudo systemctl daemon-reload
sudo systemctl enable slurm-maint-operator@compute
sudo systemctl start slurm-maint-operator@compute
sudo systemctl status slurm-maint-operator@compute

# View logs
sudo journalctl -u slurm-maint-operator@compute -f
```

### Scheduled Maintenance Windows

Use cron or systemd timers for time-restricted maintenance:

```bash
# /etc/cron.d/slurm-maintenance
# Run rolling reboots during off-peak hours (2-6 AM, Mon-Fri)
0 2 * * 1-5 slurm /opt/slurm-maint-controller/maint_operator.py -p compute --enable-reservations --enable-reboots --max-iterations 48 --interval 300 -v >> /var/log/maint-operator.log 2>&1
```

This runs for 4 hours (48 iterations × 5 minutes), limiting maintenance to the specified window.

## Requirements

- Python 3.9+
- Access to Slurm commands (`scontrol`, `squeue`, `sinfo`, `scancel`)
- Appropriate permissions to create reservations and reboot nodes
- SSH access to nodes (for reboot commands) or alternative reboot mechanism
- Python packages: `click`, `pendulum`, `loguru`

## Installation

```bash
# Install dependencies
pip install -r requirements.txt

# Or create virtual environment
python3 -m venv venv
source venv/bin/activate
pip install click pendulum loguru

# Make executable
chmod +x maint_operator.py

# Test installation
python maint_operator.py --help
```
