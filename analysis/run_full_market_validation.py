"""Resumable, resource-gated full-market validation against a frozen inventory.

Run with the Clara Python environment (PyArrow required). No market rule changes,
automatic mismatch retries, source writes, or historical-manifest dependencies.
"""

import argparse
import fcntl
import hashlib
import json
import os
import re
import shutil
import signal
import subprocess
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq

GIB = 1024**3
FEEDS = {
    "SH": ("mdl_4_24_0", "MarketData"),
    "SZ": ("mdl_6_33_0", "mdl_6_36_0", "mdl_6_28_0"),
}
COUNT_FIELDS = (
    "total_anchors",
    "comparable_anchors",
    "matched",
    "mismatched",
    "not_comparable",
    "excluded_by_status",
    "data_errors",
    "missing_source",
    "omitted_matched_records",
    "omitted_mismatched_records",
    "omitted_not_comparable_records",
)
V2_COUNT_FIELDS = (
    "selected_references",
    "comparable_references",
    "matched",
    "mismatched",
    "data_errors",
    "missing_source",
    "omitted_matched_records",
    "omitted_failure_records",
)
TERMINAL = {
    "passed",
    "no_eligible_references",
    "mismatched",
    "input_error",
    "runtime_error",
}
FAILURES = {"mismatched", "input_error", "runtime_error"}


def report_version(report):
    version = report.get("report_schema_version", 1)
    require(
        type(version) is int and version in (1, 2),
        f"unsupported report version: {version}",
    )
    return version


def count_fields(version):
    return COUNT_FIELDS if version == 1 else V2_COUNT_FIELDS


def validate_v2_counts(report):
    selected = report["selected_references"]
    comparable = report["comparable_references"]
    require(
        selected
        == sum(
            report[k]
            for k in ("matched", "mismatched", "data_errors", "missing_source")
        ),
        "selected reference count mismatch",
    )
    require(
        comparable == report["matched"] + report["mismatched"],
        "comparable count mismatch",
    )
    for key in ("matched", "mismatched", "data_errors", "missing_source"):
        require(
            sum(v[key] for v in report["breakdown"].values()) == report[key],
            f"breakdown mismatch: {key}",
        )
    require(
        sum(v["total"] for v in report["breakdown"].values()) == selected,
        "breakdown total mismatch",
    )
    audits = report["selection_audit"]
    require(
        len({a["symbol"] for a in audits}) == len(audits), "duplicate selection audit"
    )
    audit_selected = 0
    for audit in audits:
        n = sum(audit["selected_counts"].values())
        require(
            audit["reference_records"] == n + sum(audit["skipped_counts"].values()),
            "selection audit count mismatch",
        )
        require(
            sum(audit["status_counts"].values()) == audit["reference_records"],
            "status count mismatch",
        )
        expected = (
            "selected"
            if n
            else (
                "no_eligible_references"
                if audit["reference_records"]
                else "no_reference_records"
            )
        )
        require(
            audit["coverage"] == expected, "selection coverage classification mismatch"
        )
        require(len(audit["first_samples"]) <= 3, "unbounded selection samples")
        audit_selected += n
    require(audit_selected == selected, "audit/selected count mismatch")
    coverage = report["coverage"]
    require(coverage["symbols"] == len(audits), "coverage symbol count mismatch")
    for field, status in (
        ("symbols_with_selected_references", "selected"),
        ("symbols_without_reference_records", "no_reference_records"),
        ("symbols_without_eligible_references", "no_eligible_references"),
    ):
        require(
            coverage[field] == sum(a["coverage"] == status for a in audits),
            f"coverage mismatch: {field}",
        )
    require(
        sum(v["selected_references"] for v in coverage["by_stage"].values())
        == selected,
        "stage coverage count mismatch",
    )
    require(
        report["match_rate"]
        == (report["matched"] / comparable if comparable else None),
        "match rate mismatch",
    )
    for record in report["records"]:
        require(
            record["outcome"]
            in ("matched", "mismatched", "data_error", "missing_source"),
            "non-comparison outcome",
        )
        require(
            type(record["reference_time_ms"]) is int
            and record["reference_source_row_no"] > 0,
            "missing reference identity",
        )
    require(
        sum(r["outcome"] == "matched" for r in report["records"])
        + report["omitted_matched_records"]
        == report["matched"],
        "matched detail count mismatch",
    )
    require(
        sum(r["outcome"] != "matched" for r in report["records"])
        + report["omitted_failure_records"]
        == selected - report["matched"],
        "failure detail count mismatch",
    )
    failed = bool(
        report["mismatched"]
        or report["data_errors"]
        or report["missing_source"]
        or report["replay"].get("sz_after_close_events", 0)
    )
    expected = (
        "failed" if failed else ("passed" if selected else "no_eligible_references")
    )
    require(report["run_outcome"] == expected, "run outcome mismatch")


def utc():
    return datetime.now(timezone.utc).isoformat()


def save(path, value):
    path = Path(path)
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w") as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)


def digest(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def require(condition, message):
    if not condition:
        raise ValueError(message)


def memory_available():
    match = re.search(
        r"^MemAvailable:\s+(\d+)", Path("/proc/meminfo").read_text(), re.MULTILINE
    )
    require(match is not None, "MemAvailable is unavailable")
    return int(match[1]) * 1024


def can_admit(active_count, memory_bytes, disk_bytes, output_bytes, reservations):
    return (
        memory_bytes >= (64 + 32 * (active_count + 1)) * GIB
        and disk_bytes >= 200 * GIB + sum(reservations)
        and output_bytes >= 20 * GIB
    )


def inspect_input(source):
    path = Path(source["path"])
    stat = path.stat()
    require(stat.st_size == source["bytes"], f"size changed: {path}")
    require(stat.st_mtime_ns == int(source["mtime_ns"]), f"mtime changed: {path}")
    require(len(list(path.parent.glob("*.parquet"))) == 1, f"unexpected parts: {path}")
    with pq.ParquetFile(path) as parquet:
        require(
            parquet.metadata.num_rows == source["rows"] > 0, f"rows changed: {path}"
        )
        schema = str(parquet.schema_arrow.remove_metadata())
        require(
            hashlib.sha256(schema.encode()).hexdigest() == source["schema_sha256"],
            f"field schema changed: {path}",
        )
        metadata = {
            k.decode(): v.decode()
            for k, v in parquet.metadata.metadata.items()
            if k.startswith(b"clara.raw.")
        }
    market = "SH" if source["feed"] in FEEDS["SH"] else "SZ"
    expected = {
        "clara.raw.market": market,
        "clara.raw.feed": source["feed"],
        "clara.raw.date": source["date"],
        "clara.raw.document_version": "4.1",
        "clara.raw.format_version": "2",
        "clara.raw.schema_id": f"tonglian.level2.v4_1.{market}.{source['feed']}",
    }
    for key, value in expected.items():
        require(metadata.get(key) == value, f"{key} differs: {path}")
    require(bool(metadata.get("clara.raw.schema_hash")), f"schema_hash absent: {path}")
    after = path.stat()
    require(
        (after.st_size, after.st_mtime_ns) == (stat.st_size, stat.st_mtime_ns),
        f"input changed during footer read: {path}",
    )
    return metadata


def validate_report(report, job):
    require(not report["diagnostic_window_override"], "diagnostic window override")
    version = report_version(report)
    if version == 2:
        validate_v2_counts(report)
    else:
        require(
            report["total_anchors"]
            == report["matched"] + report["mismatched"] + report["not_comparable"],
            "total count mismatch",
        )
        require(
            report["comparable_anchors"] == report["matched"] + report["mismatched"],
            "comparable count mismatch",
        )
        require(
            report["not_comparable"]
            == report["excluded_by_status"]
            + report["data_errors"]
            + report["missing_source"],
            "exclusion count mismatch",
        )
        for key in COUNT_FIELDS[2:8]:
            require(
                sum(v[key] for v in report["breakdown"].values()) == report[key],
                f"breakdown mismatch: {key}",
            )
        require(
            sum(v["total"] for v in report["breakdown"].values())
            == report["total_anchors"],
            "breakdown total mismatch",
        )
    ticks = [s for s in job["sources"] if s["feed"] not in ("MarketData", "mdl_6_28_0")]
    require(
        report["replay"]["input_rows"] == sum(s["rows"] for s in ticks),
        "not all source rows scanned",
    )
    require(report["replay"]["scheduled_snapshots"] == 0, "unexpected scheduled output")
    require(
        report["continuous_lookback_ms"] == (1000 if job["market"] == "SH" else 0),
        "lookback mismatch",
    )
    for symbol, horizon in report["continuous_lookahead_ms_by_symbol"].items():
        expected = 1000
        if job["market"] == "SZ":
            if symbol.startswith("159"):
                expected = 1100
            elif symbol.startswith(("300", "301", "302")):
                expected = 3000
        require(horizon == expected, f"lookahead mismatch: {symbol}")
    if job["market"] == "SZ":
        require(
            report["replay"]["sz_market_order_policy"] == "rest_at_last_trade_price",
            "SZ policy mismatch",
        )
    for record in report["records"]:
        require(record["market"] == job["market"], "record market mismatch")
    if (
        report["data_errors"]
        or report["missing_source"]
        or report["replay"].get("sz_after_close_events", 0)
    ):
        return "input_error"
    if version == 2 and report["run_outcome"] == "no_eligible_references":
        return "no_eligible_references"
    comparable = (
        report["comparable_references"]
        if version == 2
        else report["comparable_anchors"]
    )
    if comparable <= 0:
        return "input_error"
    return "mismatched" if report["mismatched"] else "passed"


def summarize(manifest, out):
    summary = {
        "updated_at": utc(),
        "campaign_status": manifest["status"],
        "expected_jobs": len(manifest["jobs"]),
        "job_statuses": dict(Counter(j["status"] for j in manifest["jobs"])),
        "markets": {},
        "failed_jobs": [],
        "per_job": [],
        "interpretation": "Default practical SZ replay; schema versions are aggregated separately. No eligible references is not acceptance.",
    }
    for job in manifest["jobs"]:
        if job["status"] not in TERMINAL or not job.get("receipt"):
            continue
        receipt = json.loads((out / job["receipt"]).read_text())
        item = {
            k: receipt.get(k)
            for k in (
                "date",
                "market",
                "status",
                "exit_code",
                "elapsed_seconds",
                "peak_rss_kib",
                "counts",
                "error",
                "report",
                "timings",
                "report_schema_version",
                "coverage",
            )
        }
        summary["per_job"].append(item)
        if job["status"] not in ("passed", "no_eligible_references"):
            summary["failed_jobs"].append(item)
        if "counts" not in receipt:
            # No report means no known schema; do not invent a v1 aggregate for a failed v2 run.
            continue
        version = report_version(receipt)
        versions = summary["markets"].setdefault(job["market"], {"versions": {}})[
            "versions"
        ]
        aggregate = versions.setdefault(
            str(version),
            {
                "counts": dict.fromkeys(count_fields(version), 0),
                "breakdown": {},
                "mismatch_fields": {},
                "failure_reasons": {},
                "coverage": {},
                "elapsed_seconds_sum": 0.0,
                "peak_rss_kib_max": 0,
            },
        )
        aggregate["elapsed_seconds_sum"] += receipt.get("elapsed_seconds", 0)
        aggregate["peak_rss_kib_max"] = max(
            aggregate["peak_rss_kib_max"], receipt.get("peak_rss_kib") or 0
        )
        for key in count_fields(version):
            aggregate["counts"][key] += receipt["counts"][key]
        for key, value in receipt["breakdown"].items():
            bucket = aggregate["breakdown"].setdefault(key, {})
            for field, count in value.items():
                bucket[field] = bucket.get(field, 0) + count
        for field, source in (
            ("mismatch_fields", "mismatch_fields"),
            (
                "failure_reasons",
                "not_comparable_reasons" if version == 1 else "failure_reasons",
            ),
        ):
            for key, count in receipt[source].items():
                aggregate[field][key] = aggregate[field].get(key, 0) + count
        if version == 2:
            for key, value in receipt["coverage"].items():
                if key == "by_stage":
                    stages = aggregate["coverage"].setdefault(key, {})
                    for stage, counts in value.items():
                        bucket = stages.setdefault(stage, {})
                        for field, count in counts.items():
                            bucket[field] = bucket.get(field, 0) + count
                else:
                    aggregate["coverage"][key] = (
                        aggregate["coverage"].get(key, 0) + value
                    )
    for market in summary["markets"].values():
        for version, aggregate in market["versions"].items():
            comparable = aggregate["counts"][
                "comparable_anchors" if version == "1" else "comparable_references"
            ]
            aggregate["match_rate"] = (
                aggregate["counts"]["matched"] / comparable if comparable else None
            )
    summary["acceptance_passed"] = bool(manifest["jobs"]) and all(
        j["status"] == "passed" for j in manifest["jobs"]
    )
    summary["execution_completed_without_failure"] = all(
        j["status"] in ("passed", "no_eligible_references") for j in manifest["jobs"]
    )
    save(out / "summary.json", summary)
    return summary


def make_jobs(inventory, pilot_dates):
    sources = {(s["date"], s["feed"]): s for s in inventory["inputs"]}
    days = sorted({s["date"] for s in inventory["inputs"]})
    jobs = []
    for day in days:
        for market, feeds in FEEDS.items():
            selected = [sources[(day, feed)] for feed in feeds]
            require(
                all(s["status"] == "ok" for s in selected), f"incomplete {day} {market}"
            )
            tick_rows = sum(s["rows"] for s in selected[:-1])
            if market == "SH":
                spool = selected[0]["rows"] * 77
            else:
                spool = selected[0]["rows"] * 60 + selected[1]["rows"] * 75
            jobs.append(
                {
                    "id": f"{day}-{market}",
                    "date": day,
                    "market": market,
                    "pilot": day in pilot_dates,
                    "sources": selected,
                    "status": "pending",
                    "spool_reservation": spool * 4,
                    "work_weight": tick_rows,
                    "attempts": [],
                }
            )
    return sorted(jobs, key=lambda j: (not j["pilot"], -j["work_weight"], j["id"]))


def launch(job, manifest, out, spool_root):
    attempt = len(job["attempts"]) + 1
    folder = out / "jobs" / job["id"] / f"attempt-{attempt:02}"
    folder.mkdir(parents=True, exist_ok=False)
    spool = spool_root / job["id"] / f"attempt-{attempt:02}"
    spool.mkdir(parents=True, exist_ok=False)
    receipt = {
        "date": job["date"],
        "market": job["market"],
        "status": "running",
        "started_at": utc(),
        "target_universe": "all_stocks_and_etfs",
        "spool_path": str(spool),
        "binary_sha256": manifest["binary_sha256"],
    }
    receipt_path = folder / "receipt.json"
    job["receipt"] = str(receipt_path.relative_to(out))
    job["attempts"].append(job["receipt"])
    try:
        for source in job["sources"]:
            metadata = inspect_input(source)
            require(
                metadata == manifest["provenance"][source["path"]],
                "footer provenance changed",
            )
        cmd = [
            str(out / "bin/validation_benchmark"),
            "--date",
            job["date"],
            "--market",
            job["market"],
            "--raw-root",
            manifest["raw_root"],
            "--temp-root",
            str(spool),
            "--batch-size",
            str(manifest["batch_size"]),
            "--max-detail-records",
            str(manifest["max_detail_records"]),
            "--report",
            str(folder / "validation.json"),
            "--timings",
            str(folder / "timings.json"),
        ]
        receipt["command"] = cmd
        save(receipt_path, receipt)
        with (folder / "stdout.log").open("x") as log:
            proc = subprocess.Popen(
                ["/usr/bin/time", "-v", "-o", str(folder / "resource.txt"), *cmd],
                stdout=log,
                stderr=subprocess.STDOUT,
                env={**os.environ, "LC_ALL": "C"},
                start_new_session=True,
            )
        receipt["pid"] = proc.pid
        job["pid"] = proc.pid
        job["status"] = "running"
        save(receipt_path, receipt)
        print(f"{utc()} START {job['id']} pid={proc.pid}", flush=True)
        return proc, time.monotonic(), folder, receipt
    except (OSError, ValueError, KeyError, TypeError, OverflowError) as error:
        job["status"] = "input_error"
        receipt.update(status="input_error", error=str(error), finished_at=utc())
        save(receipt_path, receipt)
        print(f"{utc()} PRECHECK_FAILED {job['id']} {error}", flush=True)
        return None


def finish(job, running, manifest, out):
    proc, start, folder, receipt = running
    receipt.update(
        exit_code=proc.returncode,
        elapsed_seconds=time.monotonic() - start,
        finished_at=utc(),
        status="runtime_error",
    )
    resource = folder / "resource.txt"
    if resource.exists():
        match = re.search(
            r"Maximum resident set size \(kbytes\):\s*(\d+)", resource.read_text()
        )
        receipt["peak_rss_kib"] = int(match[1]) if match else None
    report_path = folder / "validation.json"
    try:
        for source in job["sources"]:
            require(
                inspect_input(source) == manifest["provenance"][source["path"]],
                f"input provenance changed after run: {source['path']}",
            )
        receipt["input_identity_unchanged"] = True
        if not report_path.exists():
            with (folder / "stdout.log").open("rb") as stream:
                stream.seek(max(0, stream.seek(0, 2) - 16000))
                receipt["error"] = stream.read().decode(errors="replace")
            receipt["status"] = (
                "input_error"
                if any(
                    token in receipt["error"]
                    for token in (
                        "Schema",
                        "Validation",
                        "Normalize",
                        "AmbiguousSequence",
                        "OrderBook",
                        "Invalid",
                    )
                )
                else "runtime_error"
            )
        else:
            report = json.loads(report_path.read_text())
            status = validate_report(report, job)
            receipt.update(
                report_schema_version=report_version(report),
                counts={k: report[k] for k in count_fields(report_version(report))},
                breakdown=report["breakdown"],
                mismatch_fields=report["mismatch_fields"],
                **(
                    {"not_comparable_reasons": report["not_comparable_reasons"]}
                    if report_version(report) == 1
                    else {
                        "failure_reasons": report["failure_reasons"],
                        "coverage": report["coverage"],
                    }
                ),
                report=str(report_path.relative_to(out)),
                report_sha256=digest(report_path),
                audit_symbols=len(
                    report["phase_audit"]
                    if report_version(report) == 1
                    else report["selection_audit"]
                ),
                replay_symbols=report["replay"]["symbols"],
                mismatched_symbols=report["mismatched_symbols"],
            )
            timing_path = folder / "timings.json"
            timing = json.loads(timing_path.read_text())
            stages = timing["stages"]
            require(
                abs(
                    stages["profiled_total_seconds"]
                    - stages["restore_total_seconds"]
                    - stages["validation_total_seconds"]
                    - stages["unattributed_seconds"]
                )
                < 1e-4,
                "timing partition mismatch",
            )
            receipt.update(
                timings=str(timing_path.relative_to(out)),
                timing_summary=stages,
                timings_sha256=digest(timing_path),
                status=status,
            )
            if proc.returncode != 0 and status in ("passed", "no_eligible_references"):
                receipt.update(
                    status="runtime_error", error="success report with nonzero exit"
                )
    except (OSError, ValueError, KeyError, TypeError, OverflowError) as error:
        receipt.update(status="input_error", error=str(error))
    job["status"] = receipt["status"]
    job.pop("pid", None)
    save(folder / "receipt.json", receipt)
    print(
        f"{utc()} END {job['id']} {job['status']} elapsed={receipt['elapsed_seconds']:.1f}s",
        flush=True,
    )


def stop_on_failure(manifest, active, out):
    """Stop only process groups launched by this controller; retain their evidence."""
    failures = [j["id"] for j in manifest["jobs"] if j["status"] in FAILURES]
    if not manifest.get("stop_on_failure") or not failures:
        return False
    manifest.update(status="stopping_on_failure", stopped_by=failures)
    save(out / "manifest.json", manifest)
    for job_id, running in list(active.items()):
        proc, start, folder, receipt = running
        job = next(j for j in manifest["jobs"] if j["id"] == job_id)
        if proc.poll() is not None:
            finish(job, running, manifest, out)
        else:
            # launch() creates a new session, so pid is this task's own PGID.
            try:
                os.killpg(proc.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(proc.pid, signal.SIGKILL)
                proc.wait(timeout=5)
            job.update(status="cancelled_after_failure")
            job.pop("pid", None)
            receipt.update(
                status=job["status"],
                exit_code=proc.returncode,
                elapsed_seconds=time.monotonic() - start,
                finished_at=utc(),
                stopped_by=failures,
                error="Cancelled by this campaign's fail-fast policy; no validation conclusion.",
            )
            save(folder / "receipt.json", receipt)
            print(f"{utc()} CANCEL {job_id} pid={proc.pid}; spool retained", flush=True)
        del active[job_id]
    manifest.update(status="stopped_on_failure", finished_at=utc())
    save(out / "manifest.json", manifest)
    summarize(manifest, out)
    print(f"{utc()} STOP_ON_FAILURE {failures}", flush=True)
    return True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--spool-root", type=Path, required=True)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument("--batch-size", type=int, default=262144)
    parser.add_argument("--max-detail-records", type=int, default=5000)
    parser.add_argument("--pilot-dates", default="20260114,20260717,20260828")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument(
        "--stop-on-failure",
        action="store_true",
        help="Stop dispatch and cancel this campaign's active jobs after the first failed market-day report.",
    )
    args = parser.parse_args()
    require(
        args.workers > 0 and args.batch_size > 0 and args.max_detail_records >= 0,
        "invalid parameters",
    )
    out, spool = args.output.resolve(), args.spool_root.resolve()
    require(
        out != spool and out not in spool.parents and spool not in out.parents,
        "output and spool overlap",
    )
    require(
        out != Path(out.anchor) and spool != Path(spool.anchor),
        "root path is not a campaign",
    )
    out.mkdir(parents=True, exist_ok=True)
    spool.mkdir(parents=True, exist_ok=True)
    lock = (out / "campaign.lock").open("a+")
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    manifest_path = out / "manifest.json"
    options = {
        "inventory_sha256": digest(args.inventory),
        "source_commit": args.source_commit,
        "workers": args.workers,
        "batch_size": args.batch_size,
        "max_detail_records": args.max_detail_records,
        "spool_root": str(spool),
        "runner_sha256": digest(__file__),
        "pilot_dates": args.pilot_dates.split(","),
        "stop_on_failure": args.stop_on_failure,
    }
    if manifest_path.exists():
        require(args.resume, "campaign exists; --resume required")
        manifest = json.loads(manifest_path.read_text())
        for key, value in options.items():
            require(manifest[key] == value, f"resume configuration changed: {key}")
        require(
            digest(out / "bin/validation_benchmark") == manifest["binary_sha256"],
            "frozen binary changed",
        )
        for job in manifest["jobs"]:
            if job["status"] == "running":
                try:
                    os.killpg(job["pid"], 0)
                except ProcessLookupError:
                    old = json.loads((out / job["receipt"]).read_text())
                    old.update(status="interrupted", finished_at=utc())
                    save(out / job["receipt"], old)
                    job.update(status="pending")
                else:
                    raise RuntimeError(
                        f"prior task still alive; do not duplicate: {job['id']}"
                    )
            if job["status"] in ("passed", "no_eligible_references"):
                receipt = json.loads((out / job["receipt"]).read_text())
                require(
                    digest(out / receipt["report"]) == receipt["report_sha256"],
                    "saved report changed",
                )
                require(
                    digest(out / receipt["timings"]) == receipt["timings_sha256"],
                    "saved timings changed",
                )
                for source in job["sources"]:
                    require(
                        inspect_input(source) == manifest["provenance"][source["path"]],
                        "resume input changed",
                    )
    else:
        require(not args.resume, "no manifest to resume")
        inventory = json.loads(args.inventory.read_text())
        manifest = {
            **options,
            "created_at": utc(),
            "status": "preflight",
            "raw_root": inventory["raw_root"],
            "binary_sha256": digest(args.binary),
            "provenance": {},
            "jobs": make_jobs(inventory, options["pilot_dates"]),
        }
        (out / "bin").mkdir(exist_ok=False)
        shutil.copy2(args.binary, out / "bin/validation_benchmark")
        shutil.copy2(args.inventory, out / "inventory.json")
        save(manifest_path, manifest)
        for index, job in enumerate(manifest["jobs"]):
            try:
                for source in job["sources"]:
                    manifest["provenance"][source["path"]] = inspect_input(source)
            except (OSError, ValueError, KeyError, TypeError, OverflowError) as error:
                folder = out / "jobs" / job["id"] / "preflight"
                folder.mkdir(parents=True, exist_ok=False)
                job.update(
                    status="input_error",
                    receipt=str((folder / "receipt.json").relative_to(out)),
                )
                save(
                    folder / "receipt.json",
                    {
                        "date": job["date"],
                        "market": job["market"],
                        "status": "input_error",
                        "error": str(error),
                        "finished_at": utc(),
                    },
                )
            if index % 20 == 0:
                save(manifest_path, manifest)
                print(
                    f"{utc()} PREFLIGHT {index + 1}/{len(manifest['jobs'])}", flush=True
                )
    manifest.update(status="running", controller_pid=os.getpid())
    manifest.setdefault("started_at", utc())
    active = {}
    heartbeat = 0
    while True:
        for job_id, running in list(active.items()):
            if running[0].poll() is not None:
                job = next(j for j in manifest["jobs"] if j["id"] == job_id)
                finish(job, running, manifest, out)
                del active[job_id]
                save(manifest_path, manifest)
                summarize(manifest, out)
        if stop_on_failure(manifest, active, out):
            return 1
        pending = [j for j in manifest["jobs"] if j["status"] == "pending"]
        if not pending and not active:
            break
        pilot_incomplete = any(
            j["pilot"] and j["status"] in ("pending", "running")
            for j in manifest["jobs"]
        )
        for job in pending:
            if len(active) >= args.workers:
                break
            if pilot_incomplete and not job["pilot"]:
                continue
            # Reserve future peak memory/repair space for all active tasks,
            # deliberately double-counting already allocated bytes for safety.
            reservations = [
                job["spool_reservation"],
                *(
                    j["spool_reservation"]
                    for j in manifest["jobs"]
                    if j["id"] in active
                ),
            ]
            if not can_admit(
                len(active),
                memory_available(),
                shutil.disk_usage(spool).free,
                shutil.disk_usage(out).free,
                reservations,
            ):
                continue
            running = launch(job, manifest, out, spool)
            if running:
                active[job["id"]] = running
            save(manifest_path, manifest)
            if job["status"] in FAILURES and args.stop_on_failure:
                break
        if time.monotonic() - heartbeat >= 60:
            heartbeat = time.monotonic()
            manifest["heartbeat_at"] = utc()
            manifest["resource_snapshot"] = {
                "mem_available_gib": memory_available() / GIB,
                "spool_free_gib": shutil.disk_usage(spool).free / GIB,
                "output_free_gib": shutil.disk_usage(out).free / GIB,
            }
            manifest["pilot_complete"] = not any(
                j["pilot"] and j["status"] in ("pending", "running")
                for j in manifest["jobs"]
            )
            save(manifest_path, manifest)
            summary = summarize(manifest, out)
            print(
                f"{utc()} HEARTBEAT {summary['job_statuses']} resources={manifest['resource_snapshot']}",
                flush=True,
            )
        time.sleep(5)
    manifest.update(status="completed", finished_at=utc())
    save(manifest_path, manifest)
    summary = summarize(manifest, out)
    print(
        f"{utc()} COMPLETE passed={summary['acceptance_passed']} {summary['job_statuses']}",
        flush=True,
    )
    return 0 if summary["execution_completed_without_failure"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
