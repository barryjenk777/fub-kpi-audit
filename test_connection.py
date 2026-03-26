#!/usr/bin/env python3
"""
Quick test to verify your FUB API connection works.
Run this first before running the full audit.

Usage:
    export FUB_API_KEY="your_key_here"
    python test_connection.py
"""

import sys
from fub_client import FUBClient


def main():
    print("Testing FUB API connection...\n")

    try:
        client = FUBClient()
    except ValueError as e:
        print(f"❌ {e}")
        print("\nSet your API key:")
        print('  export FUB_API_KEY="your_key_here"')
        sys.exit(1)

    # Test 1: Get current user (identity)
    print("1. Checking API key validity...")
    try:
        me = client._request("GET", "me")
        print(f"   ✅ Connected as: {me.get('firstName', '')} {me.get('lastName', '')}")
        print(f"   Account: {me.get('teamName', 'unknown')}")
    except Exception as e:
        print(f"   ❌ Authentication failed: {e}")
        print("   Check that your API key is correct.")
        sys.exit(1)

    # Test 2: List users
    print("\n2. Fetching team roster...")
    try:
        users = client.get_users()
        print(f"   ✅ Found {len(users)} users:")
        for u in users:
            name = f"{u.get('firstName', '')} {u.get('lastName', '')}".strip()
            status = u.get("status", "unknown")
            print(f"      • {name} (ID: {u['id']}, status: {status})")
    except Exception as e:
        print(f"   ❌ Failed to fetch users: {e}")

    # Test 3: List groups
    print("\n3. Fetching lead routing groups...")
    try:
        groups = client.get_groups()
        if groups:
            print(f"   ✅ Found {len(groups)} groups:")
            for g in groups:
                print(f"      • {g.get('name', 'unnamed')} (ID: {g['id']})")
        else:
            print("   ℹ️  No groups found. You'll need to create a Priority Agents group.")
    except Exception as e:
        print(f"   ❌ Failed to fetch groups: {e}")

    # Test 4: Quick call count
    print("\n4. Testing calls endpoint...")
    try:
        from datetime import datetime, timedelta
        since = datetime.now() - timedelta(days=7)
        calls = client.get_calls(since=since)
        print(f"   ✅ Found {len(calls)} calls in the last 7 days")
    except Exception as e:
        print(f"   ❌ Failed to fetch calls: {e}")

    print(f"\n{'='*50}")
    print("Connection test complete!")
    print(f"Total API requests: {client.request_count}")
    print(f"\nNext steps:")
    print("  1. Verify the agent names above match config.py")
    print("  2. Create a 'Priority Agents' group in FUB if needed")
    print("  3. Run: python kpi_audit.py")


if __name__ == "__main__":
    main()
