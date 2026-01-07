#!/usr/bin/env python3
"""
Test script for node state detection logic.

This script tests the NodeState class and its is_down() method to ensure
it correctly identifies nodes in DOWN states according to Slurm.
"""

from maint_operator import NodeState


def test_node_state_detection():
    """Test that various Slurm node states are correctly identified as down or not."""
    
    test_cases = [
        # (state, should_be_down, description)
        ("IDLE", False, "Idle nodes should not be considered down"),
        ("ALLOCATED", False, "Allocated nodes should not be considered down"),
        ("MIXED", False, "Mixed nodes should not be considered down"),
        ("DOWN", True, "DOWN nodes should be considered down"),
        ("DOWN*", True, "DOWN* nodes should be considered down"),
        ("DRAIN", True, "DRAIN nodes should be considered down"),
        ("DRAINED", True, "DRAINED nodes should be considered down"),
        ("DRAINING", True, "DRAINING nodes should be considered down"),
        ("DRAINING*", True, "DRAINING* nodes should be considered down"),
        ("FAIL", True, "FAIL nodes should be considered down"),
        ("FAILING", True, "FAILING nodes should be considered down"),
        ("NOT_RESPONDING", True, "NOT_RESPONDING nodes should be considered down"),
        ("NO_RESPOND", True, "NO_RESPOND nodes should be considered down"),
        ("POWER_DOWN", True, "POWER_DOWN nodes should be considered down"),
        ("POWERING_DOWN", True, "POWERING_DOWN nodes should be considered down"),
        ("COMPLETING", False, "COMPLETING nodes should not be considered down"),
        ("IDLE+DRAIN", True, "IDLE+DRAIN should be considered down (contains DRAIN)"),
        ("down", True, "lowercase down should be considered down"),
        ("Reserved", False, "Reserved nodes should not be considered down"),
    ]
    
    print("Testing node state detection:\n")
    
    all_passed = True
    for state, expected_down, description in test_cases:
        node = NodeState(name="test-node", state=state)
        is_down = node.is_down()
        
        status = "✓ PASS" if is_down == expected_down else "✗ FAIL"
        if is_down != expected_down:
            all_passed = False
        
        print(f"{status}: {description}")
        print(f"       State: '{state}' -> is_down()={is_down} (expected={expected_down})")
        print()
    
    if all_passed:
        print("\n🎉 All tests passed!")
        return 0
    else:
        print("\n❌ Some tests failed!")
        return 1


if __name__ == "__main__":
    import sys
    sys.exit(test_node_state_detection())