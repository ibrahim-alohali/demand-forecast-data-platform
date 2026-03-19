"""Run all data quality contracts and print a summary.

Exits 0 if all contracts pass, 1 if any fail.

Usage:
    python -m src.quality.run_contracts
"""

from __future__ import annotations

import sys

from src.db import get_connection
from src.quality.contracts import ALL_CONTRACTS


def run_all() -> int:
    """Run all contracts and print results.

    Returns:
        0 if all passed, 1 if any failed.
    """
    conn = get_connection()
    try:
        results = [contract(conn) for contract in ALL_CONTRACTS]
    finally:
        conn.close()

    # Print header
    print()
    print(f"{'Table':<38} {'Check':<26} {'Result':<6} {'Message'}")
    print("-" * 100)

    failed = 0
    for r in results:
        status = "PASS" if r.passed else "FAIL"
        print(f"{r.table:<38} {r.check_name:<26} {status:<6} {r.message}")
        if not r.passed:
            failed += 1

    print("-" * 100)
    total = len(results)
    passed = total - failed
    print(f"{passed}/{total} contracts passed.")

    if failed > 0:
        print(f"{failed} contract(s) FAILED.")
        return 1
    return 0


def main() -> None:
    sys.exit(run_all())


if __name__ == "__main__":
    main()
