"""Compare unchanged tick replay before/after validation-only E0 projection."""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import hashlib
import json
import subprocess
import time
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT / "reports/20260907-sz-cross-date-diagnosis"
OUT = ROOT / "reports/20260907-sz-e0-projection-verification"
RAW = Path("/hdd/data/stock/raw_level2_parquet")
CASES = [("20260320", "300391"), ("20260401", "001257,301683"),
         ("20260706", "001248"), ("20260806", "001232")]


def invoke(binary, date, symbols, raw, label):
    path = OUT / f"{date}-{label}.json"
    assert not path.exists(), f"Preserve evidence: {path}"
    command = [str(binary), "validate", "--date", date, "--market", "SZ", "--symbols", symbols,
               "--raw-root", str(raw), "--temp-root", str(ROOT / "target/e0-projection-spool"),
               "--report", str(path), "--retain-matched-records"]
    started = time.perf_counter()
    with (OUT / f"{date}-{label}.log").open("x") as log:
        proc = subprocess.run(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
    receipt = dict(command=command, exit_code=proc.returncode, seconds=time.perf_counter()-started,
                   binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest())
    (OUT / f"{date}-{label}-run.json").write_text(json.dumps(receipt, indent=2)+"\n")
    report = json.loads(path.read_text())
    print(date, label, {k: report[k] for k in ["matched", "mismatched", "data_errors", "missing_source"]},
          round(receipt["seconds"], 3), flush=True)
    return report


def case(args):
    date, symbols = args
    fixture = OUT / "fixtures"
    for feed in ["mdl_6_28_0", "mdl_6_33_0", "mdl_6_36_0"]:
        target = fixture / f"date={date}" / feed / "part-0.parquet"
        target.parent.mkdir(parents=True, exist_ok=True)
        assert not target.exists()
        table = pq.ParquetFile(BASE / date / f"{feed}.parquet").read()
        meta = {k: v for k, v in pq.ParquetFile(RAW / f"date={date}" / feed / "part-0.parquet").metadata.metadata.items()
                if k != b"ARROW:schema"}
        meta[b"qtp.diagnostic.derivative"] = b"true"
        meta[b"qtp.diagnostic.description"] = b"original-value symbol extract; no price/time/status edits"
        pq.write_table(table.replace_schema_metadata(meta), target)
    invoke(Path("/tmp/qtp-e0-before-34fe3e8"), date, symbols, fixture, "before")
    invoke(ROOT / "target/release/qtp-replay", date, symbols, fixture, "after-extract")
    invoke(ROOT / "target/release/qtp-replay", date, symbols, RAW, "after-raw")


def audit():
    results = []
    key = lambda r: (r["symbol"], r["anchor"], r["reference_time_ms"])
    for date, symbols in CASES:
        old, new, raw = [json.loads((OUT / f"{date}-{label}.json").read_text())
                         for label in ["before", "after-extract", "after-raw"]]
        assert old["replay"] == new["replay"], "replay report must be unchanged"
        a, b, c = [{key(r): r for r in report["records"]} for report in [old, new, raw]]
        assert a.keys() == b.keys() == c.keys()
        for k in b:
            # Only the provenance path differs between extract and original.
            bv, cv = json.loads(json.dumps(b[k])), json.loads(json.dumps(c[k]))
            for record in [bv, cv]:
                if record.get("close_price_band"):
                    record["close_price_band"].pop("price_limit_metadata_source")
            assert bv == cv
            if k[1] != "market_close":
                assert a[k] == b[k], "non-close validation changed"
            else:
                assert a[k]["outcome"] == "mismatched"
                assert b[k]["outcome"] == "matched" and b[k]["close_price_band"]
        assert raw["mismatched"] == raw["data_errors"] == raw["missing_source"] == 0
        assert not raw["diagnostic_window_override"]
        results.append(dict(date=date, symbols=symbols, matched=raw["matched"],
                            original_mismatched=old["mismatched"], mismatched=0,
                            unchanged_replay_report=True, unchanged_non_close_records=True,
                            close_records=[r for r in raw["records"] if r["anchor"] == "market_close"]))
    (OUT / "audit.json").write_text(json.dumps(results, ensure_ascii=False, indent=2)+"\n")
    print("PASS: 5 E0 matched, replay reports and every non-close record unchanged", flush=True)
    return results


def audit_output(prefix=""):
    paths = []
    commands = []
    for label, binary in [("before", Path("/tmp/qtp-e0-before-34fe3e8")), ("after", ROOT / "target/release/qtp-replay")]:
        destination = OUT / f"{prefix}replay-output-{label}"
        assert not destination.exists()
        command = [str(binary), "replay", "--date", "20260320", "--market", "SZ", "--symbols", "300391",
                   "--raw-root", str(OUT / "fixtures"), "--output-root", str(destination),
                   "--temp-root", str(ROOT / "target/e0-projection-output-spool"), "--snapshot-interval", "30s"]
        proc = subprocess.run(command, cwd=ROOT, capture_output=True, text=True)
        assert proc.returncode == 0, proc.stderr
        commands.append(dict(command=command, stdout=proc.stdout))
        paths.append(destination / "date=20260320/market=SZ/channel=2014/part-0.parquet")
    tables = [pq.ParquetFile(path).read() for path in paths]
    assert tables[0].equals(tables[1], check_metadata=True)
    digest = [hashlib.sha256(path.read_bytes()).hexdigest() for path in paths]
    assert digest[0] == digest[1]
    result = dict(commands=commands, rows=tables[0].num_rows, identical_parquet_bytes=True, sha256=digest[0])
    (OUT / f"{prefix}production-output-audit.json").write_text(json.dumps(result, indent=2)+"\n")
    print("PASS: 30s production snapshots and final full-book close are byte-identical", flush=True)
    return result


def audit_final_binary():
    def check(args):
        date, symbols = args
        result = invoke(ROOT / "target/release/qtp-replay", date, symbols, OUT / "fixtures", "after-final-extract")
        previous = json.loads((OUT / f"{date}-after-extract.json").read_text())
        assert result == previous, "final rebuilt binary changed validation output"
    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(check, CASES))
    audit_output("final-")
    (OUT / "final-binary-audit.json").write_text(json.dumps(dict(
        binary_sha256=hashlib.sha256((ROOT / "target/release/qtp-replay").read_bytes()).hexdigest(),
        all_four_date_reports_identical=True, production_output_byte_identical=True), indent=2)+"\n")


if __name__ == "__main__":
    OUT.mkdir(exist_ok=True)
    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(case, CASES))
    audit()
    audit_output()
