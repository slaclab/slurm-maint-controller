#!/usr/bin/env python3
"""
weka_slurm_validate.py

Validates WEKA node configuration prior to adding a node to slurm.conf.

Steps implemented:
  1. Validate NUMA domain (numactl --hardware must show > 1 NUMA node)
  1b. Discover WEKA container -> core-id mapping (weka local resources)
  1c. Cross-check WEKA cores against /etc/fstab and running process affinity
  2. Compute the proper CPUSpecList (the full NUMA node(s) that own the WEKA
     cores, including hyperthread siblings)

Run this ON the target node (e.g. via `clush -w <node> weka_slurm_validate.py`),
or locally. Requires: numactl, weka, taskset, ps.

Exit codes:
  0 => all validations passed
  1 => hard failure (NUMA invalid or CPUSpecList not computable)
  2 => only one NUMA domain (escalate to ISS team)
  3 => warnings in fstab/config/process cross-check
"""

import argparse
import os
import re
import subprocess
import sys
from collections import defaultdict


# ----------------------------------------------------------------------------
# helpers
# ----------------------------------------------------------------------------

class Colors:
    OK = "\033[92m"
    WARN = "\033[93m"
    FAIL = "\033[91m"
    INFO = "\033[96m"
    BOLD = "\033[1m"
    END = "\033[0m"


def c(text, color):
    if sys.stdout.isatty():
        return f"{color}{text}{Colors.END}"
    return text


def run(cmd, check=False):
    """Run a command, return (rc, stdout, stderr)."""
    try:
        p = subprocess.run(
            cmd, shell=isinstance(cmd, str),
            capture_output=True, text=True, check=check,
        )
        return p.returncode, p.stdout, p.stderr
    except FileNotFoundError as e:
        return 127, "", str(e)


def expand_cpu_ranges(spec):
    """'24-35,72-83' or '25,26' -> sorted list of ints."""
    cores = set()
    for part in spec.replace(" ", "").split(","):
        if not part:
            continue
        if "-" in part:
            a, b = part.split("-")
            cores.update(range(int(a), int(b) + 1))
        else:
            cores.add(int(part))
    return sorted(cores)


def compress_to_ranges(cores):
    """[24,25,26,72,73] -> '24-26,72-73'."""
    cores = sorted(set(cores))
    if not cores:
        return ""
    ranges = []
    start = prev = cores[0]
    for x in cores[1:]:
        if x == prev + 1:
            prev = x
        else:
            ranges.append((start, prev))
            start = prev = x
    ranges.append((start, prev))
    return ",".join(f"{a}" if a == b else f"{a}-{b}" for a, b in ranges)


# ----------------------------------------------------------------------------
# Step 1: NUMA topology
# ----------------------------------------------------------------------------

def parse_numactl_hardware(text):
    """
    Parse `numactl --hardware` output.
    Returns dict: {numa_node_id: [cpu ids...]}
    """
    numa = {}
    for line in text.splitlines():
        m = re.match(r"\s*node\s+(\d+)\s+cpus:\s*(.*)", line)
        if m:
            node_id = int(m.group(1))
            cpus = [int(x) for x in m.group(2).split()]
            numa[node_id] = cpus
    return numa


def get_numa_topology():
    rc, out, err = run(["numactl", "--hardware"])
    if rc != 0:
        return None, err or "numactl not available"
    return parse_numactl_hardware(out), out


def cpu_to_numa_map(numa):
    """{cpu_id: numa_node_id}"""
    m = {}
    for node, cpus in numa.items():
        for cpu in cpus:
            m[cpu] = node
    return m


def total_cpu_count(numa):
    """Total number of logical CPUs across all NUMA nodes."""
    return len({cpu for cpus in (numa or {}).values() for cpu in cpus})


# ----------------------------------------------------------------------------
# Step 1b: WEKA container -> core-id mapping
# ----------------------------------------------------------------------------

def get_weka_containers():
    rc, out, err = run(["weka", "local", "ps", "--no-header", "-o", "name"])
    if rc != 0:
        return None, err or "weka not available"
    containers = [ln.strip() for ln in out.splitlines() if ln.strip()]
    return containers, out

def get_weka_container_cores(container, max_valid_core=None):
    """
    Parse `weka local resources -C <container>` and return the set of
    explicitly pinned CORE IDs.

    Robustness rules:
      - only parse rows *after* a 'ROLES ... CORE ID' header
      - read the CORE ID strictly from the header's column offset
      - accept only bare integers that are plausible core ids
        (0 <= core < max_valid_core); reject '<auto>' and junk like 14000
    Returns (pinned_cores:set, raw_output:str)
    """
    rc, out, err = run(["weka", "local", "resources", "-C", container])
    if rc != 0:
        return set(), (err or f"could not read resources for {container}")

    cores = set()
    core_col = None
    core_hdr_end = None
    header_seen = False

    for line in out.splitlines():
        if re.search(r"\bROLES\b", line) and re.search(r"CORE\s*ID", line):
            header_seen = True
            m = re.search(r"CORE\s*ID", line)
            core_col = m.start()
            core_hdr_end = m.end()
            continue

        if not header_seen or not line.strip():
            continue

        # Strict: take only the token sitting under the CORE ID column.
        if core_col is not None and len(line) > core_col:
            segment = line[core_col:]
            tok = segment.split()[0] if segment.split() else None
        else:
            tok = None

        if tok is None or not tok.isdigit():
            continue  # '<auto>', blanks, or non-numeric -> skip (floating)

        val = int(tok)
        # Sanity bound: reject anything that can't be a real logical CPU id.
        if max_valid_core is not None and val >= max_valid_core:
            continue
        cores.add(val)

    return cores, out


# ----------------------------------------------------------------------------
# Step 1c: fstab parsing + process affinity cross-check
# ----------------------------------------------------------------------------

def parse_fstab_weka_cores(fstab_path="/etc/fstab"):
    """
    Returns dict: {container_name: set(core ids)}
    For wekafs entries, the first path component of the device is the container
    name, and 'core=N' mount options give the pinned cores.
    """
    result = defaultdict(set)
    if not os.path.exists(fstab_path):
        return result
    with open(fstab_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "wekafs" not in line:
                continue
            fields = line.split()
            if len(fields) < 4:
                continue
            device = fields[0]        # e.g. sdfscratch/some/path
            options = fields[3]       # mount options containing core=...
            container = device.split("/")[0]
            for m in re.finditer(r"core=(\d+)", options):
                result[container].add(int(m.group(1)))
    return result


def get_taskset_affinity(pid):
    rc, out, err = run(["taskset", "-p", "-c", str(pid)])
    if rc != 0:
        return None
    # "pid 8474's current affinity list: 31"
    m = re.search(r":\s*(.*)$", out.strip())
    return m.group(1).strip() if m else None


def is_floating(affinity, total_cpus):
    """
    Decide whether a process affinity represents a *floating* (e.g. suspended
    MANAGEMENT) process rather than a genuinely pinned FRONTEND/COMPUTE core.

    A pinned WEKA data-plane process is bound to 1-2 cores. A floating process
    is allowed to roam across most of the machine (often "all cores minus the
    handful pinned elsewhere"). We therefore classify by *count*: if the
    process can run on more than half of all logical CPUs, it is floating.

    This is robust to arbitrary CPU counts (96, 128, ...) and to affinity
    strings like '0-32,40-96,104-127' that are the whole box minus pinned cores.
    """
    if total_cpus <= 0:
        # No topology info: fall back to a conservative literal check.
        return affinity in ("0-127", "0-95", "0-63")
    cores = expand_cpu_ranges(affinity)
    return len(cores) > (total_cpus // 2)


def get_running_weka_affinity(numa):
    """
    Inspect running /weka/wekanode processes and return:
      {container: set(cores actually pinned)}   (excluding floating processes)
    plus a list of human-readable detail tuples.
    """
    used = defaultdict(set)
    details = []

    total_cpus = total_cpu_count(numa)

    rc, out, err = run("ps aux | grep '/weka/wekanode' | grep -v grep")
    if rc != 0 and not out:
        return used, details

    for line in out.splitlines():
        toks = line.split()
        if len(toks) < 3:
            continue
        pid = toks[1]
        state = toks[7] if len(toks) > 7 else "?"
        container = toks[-1]
        if not pid.isdigit():
            continue

        affinity = get_taskset_affinity(pid)
        if not affinity:
            continue

        floating = is_floating(affinity, total_cpus)
        details.append((pid, state, affinity, container, floating))
        if not floating:
            used[container].update(expand_cpu_ranges(affinity))

    return used, details


# ----------------------------------------------------------------------------
# Validation orchestration
# ----------------------------------------------------------------------------

def step1_validate_numa(numa, raw):
    print(c("\n=== STEP 1: Validate NUMA domain ===", Colors.BOLD))
    if numa is None:
        print(c(f"  FAIL: could not read numactl --hardware ({raw})", Colors.FAIL))
        return False
    n = len(numa)
    print(f"  numactl reports {n} NUMA node(s): {sorted(numa)}")
    if n <= 1:
        print(c("  FAIL: only 1 NUMA domain visible. BIOS needs NPS/NUMA "
                "configuration (ISS team).", Colors.FAIL))
        return False
    print(c(f"  OK: {n} NUMA domains present.", Colors.OK))
    return True


def step1b_weka_cores(containers, numa):
    print(c("\n=== STEP 1b: WEKA container -> pinned core IDs ===", Colors.BOLD))
    container_cores = {}
    if containers is None:
        print(c("  WARNING: could not list weka containers.", Colors.WARN))
        return {}
    max_valid = total_cpu_count(numa) or None   # e.g. 128 on this node
    for ctr in containers:
        cores, _ = get_weka_container_cores(ctr, max_valid_core=max_valid)
        container_cores[ctr] = cores
        print(f"  {ctr:<15} pinned cores: {sorted(cores) if cores else '(none/auto)'}")
    return container_cores

def step1c_crosscheck(numa, weka_cfg_cores):
    """
    Cross-check three sources per container:
      - weka local resources (config)          -> weka_cfg_cores
      - /etc/fstab core= options                -> fstab_cores
      - running process affinity (taskset)      -> running_cores
    """
    print(c("\n=== STEP 1c: Cross-check fstab / config / running processes ===",
            Colors.BOLD))

    fstab_cores = parse_fstab_weka_cores()
    running_cores, details = get_running_weka_affinity(numa)

    print(c("\n  Running WEKA process affinities:", Colors.INFO))
    for pid, state, affinity, container, floating in details:
        tag = "(floating/MGMT)" if floating else "(pinned)"
        print(f"    pid {pid:<7} state={state:<7} container={container:<12} "
              f"affinity={affinity:<24} {tag}")

    all_ok = True
    all_containers = set(weka_cfg_cores) | set(fstab_cores) | set(running_cores)

    print(c("\n  Per-container reconciliation:", Colors.INFO))
    for ctr in sorted(all_containers):
        cfg = set(weka_cfg_cores.get(ctr, set()))
        fst = set(fstab_cores.get(ctr, set()))
        run_ = set(running_cores.get(ctr, set()))

        print(f"\n  container '{ctr}':")
        print(f"      weka config cores : {sorted(cfg) if cfg else '(none)'}")
        print(f"      fstab core=       : {sorted(fst) if fst else '(none)'}")
        print(f"      running pinned    : {sorted(run_) if run_ else '(none)'}")

        # fstab is the source of truth for what SHOULD be pinned.
        if fst:
            missing_running = fst - run_
            # Only compare against config when config data is actually available.
            missing_cfg = (fst - cfg) if cfg else set()

            if missing_running:
                print(c(f"      WARNING: fstab cores not seen running: "
                        f"{sorted(missing_running)}", Colors.WARN))
                all_ok = False
            if cfg and missing_cfg:
                print(c(f"      WARNING: fstab cores not in weka config: "
                        f"{sorted(missing_cfg)}", Colors.WARN))
                all_ok = False
            if not cfg:
                print(c("      note: weka config cores unavailable; skipping "
                        "config comparison (using fstab + running).", Colors.INFO))
            if not missing_running and not missing_cfg:
                print(c("      OK: fstab and running processes agree.", Colors.OK))
        else:
            print(c("      note: no fstab core= entries for this container.",
                    Colors.INFO))

    return all_ok, (weka_cfg_cores, fstab_cores, running_cores)


def step2_cpuspeclist(numa, weka_cfg_cores, fstab_cores, running_cores):
    """
    Determine which NUMA node(s) own the WEKA cores and emit the CPUSpecList
    as the full CPU list (physical + HT siblings) of those NUMA node(s).

    WEKA cores are taken from the trusted sources in priority order:
      1. fstab core= entries (declarative source of truth)
      2. running pinned affinities (actual state)
      3. weka config cores (if available)
    """
    print(c("\n=== STEP 2: Pick the proper CPUSpecList ===", Colors.BOLD))

    weka_cores = set()
    for d in (fstab_cores, running_cores, weka_cfg_cores):
        for cores in d.values():
            weka_cores.update(cores)

    if not weka_cores:
        print(c("  FAIL: no WEKA cores discovered; cannot compute CPUSpecList.",
                Colors.FAIL))
        return None, False

    print(f"  WEKA cores discovered on this node: {sorted(weka_cores)}")

    c2n = cpu_to_numa_map(numa)
    owning_nodes = sorted({c2n[core] for core in weka_cores if core in c2n})
    unknown = [core for core in weka_cores if core not in c2n]
    if unknown:
        print(c(f"  WARNING: cores not found in NUMA topology: {unknown}",
                Colors.WARN))

    print(f"  WEKA cores belong to NUMA node(s): {owning_nodes}")

    # Reserve every logical CPU (incl. HT siblings) of the owning NUMA node(s).
    reserved = set()
    for node in owning_nodes:
        reserved.update(numa[node])
        print(f"    NUMA node {node} cpus: {compress_to_ranges(numa[node])}")

    cpuspeclist = compress_to_ranges(reserved)
    print(c(f"\n  >>> CPUSpecList={cpuspeclist}", Colors.BOLD + Colors.OK))
    return cpuspeclist, True


# ----------------------------------------------------------------------------
# main
# ----------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(
        description="Validate a WEKA node and compute slurm CPUSpecList.")
    ap.add_argument("--node", default=os.uname().nodename,
                    help="node label for reporting output")
    args = ap.parse_args()

    print(c(f"{'='*15}\n{args.node}\n{'='*15}", Colors.BOLD))

    results = {}

    # Step 1 - NUMA
    numa, raw = get_numa_topology()
    results["numa"] = step1_validate_numa(numa, raw)
    if not results["numa"]:
        print(c("\nStopping: NUMA domain invalid. Escalate to ISS team.",
                Colors.FAIL))
        sys.exit(2)

    # Step 1b - weka container cores
    containers, _ = get_weka_containers()
    weka_cfg_cores = step1b_weka_cores(containers, numa)

    # Step 1c - cross-check
    xcheck_ok, (cfg, fst, runc) = step1c_crosscheck(numa, weka_cfg_cores)
    results["xcheck"] = xcheck_ok

    # Step 2 - CPUSpecList
    cpuspeclist, cpu_ok = step2_cpuspeclist(numa, cfg, fst, runc)
    results["cpuspeclist"] = cpu_ok

    # summary
    print(c("\n=== SUMMARY ===", Colors.BOLD))
    for k, v in results.items():
        status = c("PASS", Colors.OK) if v else c("FAIL/WARN", Colors.WARN)
        print(f"  {k:<12}: {status}")

    if cpuspeclist:
        print(c(f"\nSuggested slurm.conf fragment for {args.node}:", Colors.INFO))
        print(f"  ...  State=UNKNOWN CPUSpecList={cpuspeclist}")

    # exit codes
    if not (results["numa"] and results["cpuspeclist"]):
        sys.exit(1)
    if not results["xcheck"]:
        sys.exit(3)
    sys.exit(0)


if __name__ == "__main__":
    main()

