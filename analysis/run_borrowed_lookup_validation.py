"""Full 15-day borrowed-key regression against the last allocation benchmark.

Use Clara Python after building the release profiling example. Existing campaign
directories are never overwritten. Input identity checks use metadata, not full
Parquet content hashes.
"""

import json

import run_callback_validation as callback

ROOT = callback.campaign.ROOT
BASELINE = ROOT / "reports/20260909-p1-allocation-full-regression"
OUTPUT = ROOT / "reports/20260909-p1-borrowed-lookup-full-regression"
COUNTERS = (
    "observation_calls",
    "scalar_rejected_candidates",
    "depth_materializations",
    "candidate_cache_hits",
)
METRICS = (
    "input_spool_seconds",
    "replay_excluding_validation_seconds",
    "restore_total_seconds",
    "validation_total_seconds",
    "profiled_total_seconds",
)


def main():
    baseline = json.loads((BASELINE / "manifest.json").read_text())
    if baseline["status"] != "completed" or not baseline["baseline_gate_passed"]:
        raise RuntimeError("baseline has not passed full acceptance")
    if (
        callback.campaign.shared.inventory(baseline["baseline_days"])
        != baseline["sources"]
    ):
        raise RuntimeError("input metadata differs from the previous campaign")
    callback.BASELINE = BASELINE
    callback.FULL = OUTPUT
    result = callback.full(
        label="P1 恢复路径借用键（基线上一轮 allocation）",
        stage="p1_borrowed_lookup",
        counter_fields=COUNTERS,
    )
    # Preserve detailed input/replay timings, not only callback-centric totals.
    rows = []
    for path in sorted(OUTPUT.glob("*-full-run.json")):
        receipt = json.loads(path.read_text())
        if not receipt.get("acceptance_passed"):
            continue
        rows.append(
            {
                "date": receipt["date"],
                "market": receipt["market"],
                "old": receipt["previous_timing_summary"],
                "new": receipt["timing_summary"],
            }
        )
    aggregates = {}
    for market in ("ALL", "SH", "SZ"):
        selected = [r for r in rows if market == "ALL" or r["market"] == market]
        metrics = {}
        for field in METRICS:
            old = sum(r["old"][field] for r in selected)
            new = sum(r["new"][field] for r in selected)
            metrics[field] = {
                "old_seconds": old,
                "new_seconds": new,
                "reduction_percent": (old - new) / old * 100 if old else None,
                "faster_jobs": sum(r["new"][field] < r["old"][field] for r in selected),
            }
        aggregates[market] = {"accepted_jobs": len(selected), "metrics": metrics}
    callback.campaign.shared.save(
        OUTPUT / "restore-comparison.json",
        {
            "baseline": str(BASELINE),
            "campaign_passed": result == 0,
            "aggregates": aggregates,
            "rows": rows,
            "caveat": "Historical six-worker wall-time comparison, not an isolated causal benchmark.",
        },
    )
    return result


if __name__ == "__main__":
    raise SystemExit(main())
