#!/usr/bin/env python3
"""Report milan nodes booted before or after a specified date/time."""

import argparse
import re
import subprocess
import sys
from datetime import datetime, timezone, timedelta

PST = timezone(timedelta(hours=-8))


def parse_args():
    parser = argparse.ArgumentParser(
        description="Report milan nodes booted before or after a specified date/time (PST).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s "2026-05-28 08:00"              # nodes booted before this time
  %(prog)s "2026-05-28 08:00" --after      # nodes booted after this time
  %(prog)s "2026-05-28"                    # defaults to 00:00:00
        """
    )
    parser.add_argument(
        "date",
        help="Date/time in PST (format: YYYY-MM-DD [HH:MM[:SS]])"
    )
    parser.add_argument(
        "--after",
        action="store_true",
        help="Find nodes booted after the specified date (default is before)"
    )
    return parser.parse_args()


def parse_datetime(date_str):
    """Parse date string with flexible format support."""
    # Try different datetime formats
    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.replace(tzinfo=PST)
        except ValueError:
            continue

    raise ValueError(f"Could not parse date '{date_str}'. Expected format: YYYY-MM-DD [HH:MM[:SS]]")


def main():
    args = parse_args()

    try:
        cutoff = parse_datetime(args.date)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    result = subprocess.run(
        ["scontrol", "show", "node", "-o"],
        capture_output=True, text=True, check=True,
    )

    matching_boots = []
    for line in result.stdout.splitlines():
        name_match = re.search(r"NodeName=(\S+)", line)
        if not name_match or "milan" not in name_match.group(1):
            continue
        node = name_match.group(1)

        boot_match = re.search(r"BootTime=(\S+)", line)
        if not boot_match or boot_match.group(1) in ("None", "Unknown"):
            continue

        boot_time = datetime.fromisoformat(boot_match.group(1)).replace(tzinfo=PST)

        # Apply the before/after filter
        if args.after and boot_time > cutoff:
            matching_boots.append((node, boot_time))
        elif not args.after and boot_time <= cutoff:
            matching_boots.append((node, boot_time))

    mode = "after" if args.after else "before"
    cutoff_str = cutoff.strftime('%Y-%m-%d %H:%M:%S')

    if not matching_boots:
        print(f"No milan nodes booted {mode} {cutoff_str} PST.")
        sys.exit(0)

    matching_boots.sort(key=lambda x: int(x[0][-3:]))
    print(f"{'Node':<20} {'Boot Time (PST)'}")
    print("-" * 45)
    for node, boot_time in matching_boots:
        print(f"{node:<20} {boot_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")

    print(f"\n{len(matching_boots)} node(s) booted {mode} {cutoff_str} PST.")


if __name__ == "__main__":
    main()
