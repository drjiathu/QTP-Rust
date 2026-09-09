"""Allocation/lookup P1: cumulative stage ABBA and full regression vs 0b4c37a.

Each ABBA stage freezes the currently built example, compares it to the same
baseline, and uses a fresh directory. Stages are not independent effect estimates.
"""

import argparse
from pathlib import Path

import run_callback_validation as callback

ROOT = callback.campaign.ROOT
callback.BASELINE = ROOT / "reports/20260909-p1-callback-full-regression"
callback.FULL = ROOT / "reports/20260909-p1-allocation-full-regression"
COUNTERS = (
    "observation_calls",
    "scalar_rejected_candidates",
    "depth_materializations",
    "candidate_cache_hits",
)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["stage1", "stage2", "stage3", "full"])
    parser.add_argument(
        "--binary", type=Path, help="frozen cumulative-stage binary (ABBA only)"
    )
    args = parser.parse_args()
    if args.mode == "full":
        if args.binary is not None:
            parser.error("--binary is only supported for ABBA")
        raise SystemExit(
            callback.full(
                label="P1 分配与查表（基线 0b4c37a）",
                stage="p1_allocations",
                counter_fields=COUNTERS,
            )
        )
    callback.AB = ROOT / f"reports/20260909-p1-allocation-{args.mode}-abba"
    callback.sample_abba(args.binary, COUNTERS)
