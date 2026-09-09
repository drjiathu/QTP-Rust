"""Revalidate every previously accepted full-market day after the P0 optimizations."""

import json
import os
import shutil
import subprocess
import traceback
from pathlib import Path

import run_current_full_regression as shared

ROOT = shared.ROOT
OUT = ROOT / "reports/20260909-p0-optimized-full-regression"
OLD_PRIMARY = ROOT / "reports/20260907-current-full-regression"
OLD_ADDITIONAL = ROOT / "reports/20260908-additional-random-full"
DAYS = sorted(
    {
        *json.loads((OLD_PRIMARY / "manifest.json").read_text())["baseline_days"],
        *json.loads((OLD_PRIMARY / "manifest.json").read_text())["random_days"],
        *json.loads((OLD_ADDITIONAL / "manifest.json").read_text())["random_days"],
    }
)

shared.OUT = OUT
shared.WORKERS = 6
original_run_job = shared.run_job


def old_directory(day):
    primary = json.loads((OLD_PRIMARY / "manifest.json").read_text())
    if day in primary["baseline_days"] + primary["random_days"]:
        return OLD_PRIMARY
    return OLD_ADDITIONAL


def run_compared_job(binary, day, market, stage, sources):
    receipt = original_run_job(binary, day, market, stage, sources)
    if receipt["status"] != "completed":
        return receipt
    name = f"{day}-{market.lower()}-full"
    new_report = json.loads((OUT / f"{name}.json").read_text())
    previous = old_directory(day)
    old_report = json.loads((previous / f"{name}.json").read_text())
    semantic_equal = new_report == old_report
    old_run = json.loads((previous / f"{name}-run.json").read_text())
    receipt["comparison_baseline"] = str(previous.relative_to(ROOT))
    receipt["semantic_report_equal"] = semantic_equal
    receipt["previous_timing_summary"] = old_run["timing_summary"]
    receipt["previous_peak_rss_kib"] = old_run["peak_rss_kib"]
    receipt["acceptance_passed"] = receipt["acceptance_passed"] and semantic_equal
    shared.save(OUT / f"{name}-run.json", receipt)
    print("COMPARED", name, semantic_equal, flush=True)
    return receipt


shared.run_job = run_compared_job


def metric_row(receipt, field):
    old = receipt["previous_timing_summary"][field]
    new = receipt["timing_summary"][field]
    return {
        "old_seconds": old,
        "new_seconds": new,
        "speedup": old / new if new else None,
        "reduction_percent": (old - new) / old * 100 if old else None,
    }


def write_comparison():
    rows = []
    for day in DAYS:
        for market in ("SH", "SZ"):
            name = f"{day}-{market.lower()}-full"
            path = OUT / f"{name}-run.json"
            if not path.exists():
                continue
            receipt = json.loads(path.read_text())
            if receipt.get("status") != "completed" or "previous_timing_summary" not in receipt:
                continue
            old_rss = receipt["previous_peak_rss_kib"]
            new_rss = receipt["peak_rss_kib"]
            rows.append(
                {
                    "date": day,
                    "market": market,
                    "semantic_report_equal": receipt["semantic_report_equal"],
                    "validation_total": metric_row(receipt, "validation_total_seconds"),
                    "validation_callbacks": metric_row(
                        receipt, "validation_callbacks_seconds"
                    ),
                    "reference_load": metric_row(receipt, "reference_load_seconds"),
                    "report_finalize": metric_row(receipt, "report_finalize_seconds"),
                    "restore_total": metric_row(receipt, "restore_total_seconds"),
                    "peak_rss": {
                        "old_gib": old_rss / 1048576,
                        "new_gib": new_rss / 1048576,
                        "reduction_percent": (old_rss - new_rss) / old_rss * 100,
                    },
                }
            )

    aggregates = {}
    for market in ("ALL", "SH", "SZ"):
        selected = [row for row in rows if market == "ALL" or row["market"] == market]
        if not selected:
            continue
        aggregates[market] = {
            "jobs": len(selected),
            "semantic_reports_equal": all(row["semantic_report_equal"] for row in selected),
        }
        for metric in (
            "validation_total",
            "validation_callbacks",
            "reference_load",
            "report_finalize",
            "restore_total",
        ):
            old = sum(row[metric]["old_seconds"] for row in selected)
            new = sum(row[metric]["new_seconds"] for row in selected)
            aggregates[market][metric] = {
                "old_seconds": old,
                "new_seconds": new,
                "speedup": old / new,
                "reduction_percent": (old - new) / old * 100,
            }
        old = sum(row["peak_rss"]["old_gib"] for row in selected) / len(selected)
        new = sum(row["peak_rss"]["new_gib"] for row in selected) / len(selected)
        aggregates[market]["mean_peak_rss"] = {
            "old_gib": old,
            "new_gib": new,
            "reduction_percent": (old - new) / old * 100,
        }

    comparison = {"updated_at": shared.utc(), "aggregates": aggregates, "rows": rows}
    shared.save(OUT / "comparison.json", comparison)
    lines = [
        "# P0 优化前后全市场 validation 对比",
        "",
        "旧、新批次均为六进程并发 wall time；同一日市场任务逐份比较完整 JSON 语义报告。",
        "",
        "| 范围 | 任务 | 报告一致 | validation 旧/新秒 | 加速 | callbacks 旧/新秒 | 加速 | 平均峰值 GiB 旧/新 |",
        "|---|---:|---|---:|---:|---:|---:|---:|",
    ]
    for market, values in aggregates.items():
        validation = values["validation_total"]
        callbacks = values["validation_callbacks"]
        rss = values["mean_peak_rss"]
        lines.append(
            f"| {market} | {values['jobs']} | {values['semantic_reports_equal']} | "
            f"{validation['old_seconds']:.2f}/{validation['new_seconds']:.2f} | "
            f"{validation['speedup']:.2f}x | "
            f"{callbacks['old_seconds']:.2f}/{callbacks['new_seconds']:.2f} | "
            f"{callbacks['speedup']:.2f}x | {rss['old_gib']:.2f}/{rss['new_gib']:.2f} |"
        )
    (OUT / "comparison.md").write_text("\n".join(lines) + "\n")


def main():
    OUT.mkdir(parents=True, exist_ok=False)
    manifest = None
    try:
        frozen = ROOT / "target/validation-binaries" / OUT.name / "validation_benchmark"
        frozen.parent.mkdir(parents=True, exist_ok=False)
        shutil.copy2(ROOT / "target/release/examples/validation_benchmark", frozen)
        hashes = {}
        sources_to_freeze = [
            *ROOT.glob("src/**/*.rs"),
            ROOT / "Cargo.toml",
            ROOT / "Cargo.lock",
            ROOT / "examples/validation_benchmark.rs",
            Path(__file__),
        ]
        for path in sorted(sources_to_freeze):
            relative = path.relative_to(ROOT)
            destination = OUT / "source-snapshot" / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, destination)
            hashes[str(relative)] = shared.digest(path)
        shared.save(OUT / "source-sha256.json", hashes)
        (OUT / "working-tree.patch").write_bytes(
            subprocess.check_output(["git", "diff", "--binary", "HEAD"], cwd=ROOT)
        )
        manifest = {
            "status": "running",
            "started_at": shared.utc(),
            "heartbeat_at": shared.utc(),
            "driver_pid": os.getpid(),
            "baseline_days": DAYS,
            "random_days": [],
            "random_stage": "not_applicable",
            "baseline_gate_passed": False,
            "max_workers": shared.WORKERS,
            "features": ["profiling"],
            "scope": "all_stocks_and_etfs",
            "frozen_binary": str(frozen),
            "binary_sha256": shared.digest(frozen),
            "sources": shared.inventory(DAYS),
            "git_head": subprocess.check_output(
                ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
            ).strip(),
            "working_tree_dirty": True,
            "timing_mode": "exclusive instrumented wall time",
            "comparison_campaigns": [
                str(OLD_PRIMARY.relative_to(ROOT)),
                str(OLD_ADDITIONAL.relative_to(ROOT)),
            ],
            "selection_note": "重跑此前已验收的 15 个交易日、沪深两市全部股票和 ETF；完整语义报告必须逐任务相等。",
        }
        shared.save(OUT / "manifest.json", manifest)
        shared.summarize(manifest)
        passed = shared.stage(manifest, frozen, DAYS, "p0_optimized")
        manifest.update(
            status="completed" if passed else "failed",
            baseline_gate_passed=passed,
            finished_at=shared.utc(),
        )
        shared.save(OUT / "manifest.json", manifest)
        shared.summarize(manifest)
        write_comparison()
        return 0 if passed else 1
    except Exception:
        if manifest is None:
            manifest = {
                "baseline_days": DAYS,
                "random_days": [],
                "random_stage": "not_applicable",
            }
        manifest.update(status="driver_error", error=traceback.format_exc(), finished_at=shared.utc())
        shared.save(OUT / "manifest.json", manifest)
        shared.summarize(manifest)
        write_comparison()
        raise


if __name__ == "__main__":
    raise SystemExit(main())
