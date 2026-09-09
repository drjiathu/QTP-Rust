"""Callback optimization: serial sample ABBA and a frozen full-market regression.

Use Clara Python. Each mode refuses to overwrite its own campaign directory.
"""

import argparse
import json
import shutil
import subprocess
from pathlib import Path

import run_p0_optimized_regression as campaign

BASELINE = campaign.ROOT / "reports/20260909-p1-optimized-full-regression"
FULL = campaign.ROOT / "reports/20260909-p1-callback-full-regression"
AB = campaign.ROOT / "reports/20260909-p1-callback-abba"


def sample_abba():
    AB.mkdir(parents=True, exist_ok=False)
    old_manifest = json.loads((BASELINE / "manifest.json").read_text())
    old = Path(old_manifest["frozen_binary"])
    new = AB / "validation_benchmark"
    shutil.copy2(campaign.ROOT / "target/release/examples/validation_benchmark", new)
    assert campaign.shared.digest(old) == old_manifest["binary_sha256"]
    manifest = {
        "status": "running",
        "started_at": campaign.shared.utc(),
        "old_binary": str(old),
        "old_sha256": campaign.shared.digest(old),
        "new_binary": str(new),
        "new_sha256": campaign.shared.digest(new),
        "sequence": ["old", "new", "new", "old"],
        "runs": [],
    }
    campaign.shared.save(AB / "manifest.json", manifest)
    try:
        for market, symbols in [("SH", "600519,510300"), ("SZ", "000001,159915")]:
            reference = None
            for i, variant in enumerate(manifest["sequence"]):
                name = f"{market.lower()}-{i}-{variant}"
                report = AB / f"{name}.json"
                timings = AB / f"{name}-timings.json"
                cmd = [
                    str(old if variant == "old" else new),
                    "--date",
                    "20260828",
                    "--market",
                    market,
                    "--symbols",
                    symbols,
                    "--report",
                    str(report),
                    "--timings",
                    str(timings),
                    "--temp-root",
                    str(campaign.ROOT / "target/callback-abba-spool"),
                    "--max-detail-records",
                    "0",
                ]
                with (AB / f"{name}.log").open("x") as log:
                    result = subprocess.run(
                        cmd,
                        cwd=campaign.ROOT,
                        stdout=log,
                        stderr=subprocess.STDOUT,
                        check=False,
                    )
                if result.returncode != 0:
                    raise RuntimeError(
                        f"{name} exited {result.returncode}; inspect its log"
                    )
                current = json.loads(report.read_text())
                if reference is None:
                    reference = current
                if current != reference:
                    raise RuntimeError(f"{name} report differs from the first old run")
                row = {
                    "market": market,
                    "symbols": symbols,
                    "variant": variant,
                    "report_equal": True,
                    "command": cmd,
                    "stages": json.loads(timings.read_text())["stages"],
                }
                manifest["runs"].append(row)
                campaign.shared.save(AB / "manifest.json", manifest)
                print(
                    "ABBA",
                    name,
                    row["stages"]["validation_callbacks_seconds"],
                    flush=True,
                )
        manifest["status"] = "completed"
    except Exception as error:
        manifest.update(status="failed", error=str(error))
        raise
    finally:
        manifest["finished_at"] = campaign.shared.utc()
        campaign.shared.save(AB / "manifest.json", manifest)


def full():
    campaign.OUT = FULL
    campaign.OLD_PRIMARY = BASELINE
    campaign.OLD_ADDITIONAL = BASELINE
    campaign.DAYS = json.loads((BASELINE / "manifest.json").read_text())[
        "baseline_days"
    ]
    campaign.CAMPAIGN_LABEL = "P1 validation 回调（基线 72b7d43）"
    campaign.STAGE = "p1_callbacks"
    campaign.shared.OUT = FULL
    compared_job = campaign.shared.run_job

    def checked_job(*args):
        receipt = compared_job(*args)
        if receipt.get("status") == "completed":
            old = receipt["previous_timing_summary"]
            new = receipt["timing_summary"]
            equal = all(
                old[key] == new[key]
                for key in ("observation_calls", "scalar_rejected_candidates")
            )
            receipt["observation_and_pruning_counts_equal"] = equal
            receipt["acceptance_passed"] = receipt["acceptance_passed"] and equal
            name = f"{receipt['date']}-{receipt['market'].lower()}-full-run.json"
            campaign.shared.save(FULL / name, receipt)
        return receipt

    campaign.shared.run_job = checked_job
    return campaign.main()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["abba", "full"])
    args = parser.parse_args()
    if args.mode == "abba":
        sample_abba()
    else:
        raise SystemExit(full())
