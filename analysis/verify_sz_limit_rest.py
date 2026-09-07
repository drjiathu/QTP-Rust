"""Targeted V0 regression against unmodified raw snapshots; no window overrides."""
from collections import Counter
import hashlib
import json
from pathlib import Path
import subprocess
import time

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "reports/20260907-sz-limit-rest-verification"
BASE = ROOT / "reports/20260907-sz-cross-date-diagnosis"


def run():
    OUT.mkdir(exist_ok=True)
    binary = ROOT / "target/release/qtp-replay"
    receipts = []
    for label, raw_root in [
        ("original-extract", BASE / "fixtures/stock-original"),
        ("original-raw", Path("/hdd/data/stock/raw_level2_parquet")),
    ]:
        report = OUT / f"{label}.json"
        assert not report.exists(), f"Preserve existing evidence: {report}"
        command = [str(binary), "validate", "--date", "20260320", "--market", "SZ",
                   "--symbols", "300391", "--raw-root", str(raw_root),
                   "--temp-root", str(ROOT / "target/sz-limit-rest-spool"),
                   "--report", str(report), "--retain-matched-records"]
        started = time.perf_counter()
        with (OUT / f"{label}.log").open("x") as log:
            proc = subprocess.run(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
        receipt = dict(command=command, exit_code=proc.returncode,
                       elapsed_seconds=time.perf_counter() - started,
                       binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest())
        receipts.append(receipt)
        (OUT / "runs.json").write_text(json.dumps(receipts, indent=2) + "\n")
        if not report.exists():
            raise RuntimeError((OUT / f"{label}.log").read_text())
        value = json.loads(report.read_text())
        print(label, {k: value[k] for k in ["matched", "mismatched", "data_errors", "missing_source"]},
              receipt["elapsed_seconds"], flush=True)
    return audit()


def audit():
    baseline = json.loads((BASE / "stock-v0-current-audit.json").read_text())
    results = {name: json.loads((OUT / f"{name}.json").read_text())
               for name in ["original-extract", "original-raw"]}
    key = lambda r: (r["symbol"], r["anchor"], r["reference_time_ms"])
    old = {key(r): r for r in baseline["records"]}
    summary = {}
    for name, report in results.items():
        new = {key(r): r for r in report["records"]}
        assert len(new) == len(report["records"]) and new.keys() == old.keys()
        fixed = [k for k, r in old.items() if r["anchor"] == "continuous_trading" and r["outcome"] == "mismatched"]
        assert len(fixed) == 101
        assert all(new[k]["outcome"] == "matched" for k in fixed)
        assert all(new[k]["outcome"] == "matched" for k, r in old.items() if r["outcome"] == "matched")
        assert report["data_errors"] == report["missing_source"] == 0
        assert report["mismatched"] == 1
        remaining = [r for r in new.values() if r["outcome"] == "mismatched"]
        assert remaining[0]["anchor"] == "market_close"
        assert remaining[0]["differences"] == old[key(remaining[0])]["differences"]
        assert report["replay"]["applied_events"] == baseline["replay"]["applied_events"] == 34101
        assert report["replay"]["sz_direct_rest_limit_orders"] == 17745
        assert report["replay"]["sz_phase_rows"] == 0
        assert not report["diagnostic_window_override"]
        summary[name] = dict(matched=report["matched"], mismatched=report["mismatched"],
                             fixed_v0_frames=len(fixed), new_regressions=0,
                             outcome_by_anchor={a: dict(Counter(r["outcome"] for r in new.values() if r["anchor"] == a))
                                                for a in sorted({r["anchor"] for r in new.values()})},
                             first_fixed_candidate=new[fixed[0]], remaining=remaining)
    assert results["original-extract"]["records"] == results["original-raw"]["records"]
    summary["original_raw_and_extract_identical_validation_records"] = True
    (OUT / "audit.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n")
    print("PASS: all 101 original T0 failures fixed; no new regressions; unchanged E0 difference", flush=True)
    return summary


if __name__ == "__main__":
    run()
