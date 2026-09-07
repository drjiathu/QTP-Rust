"""Replay original 20260616 inputs; preserve executable and a reproducible receipt."""
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import time
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "reports/20260907-sz-sequence-repair"


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    binary = ROOT / "target/validation-binaries/20260907-sz-sequence-repair/validation_benchmark"
    binary.parent.mkdir(parents=True, exist_ok=True)
    if binary.exists():
        raise RuntimeError("frozen binary already exists; use a new run directory")
    shutil.copy2(ROOT / "target/release/examples/validation_benchmark", binary)
    command = [str(binary), "--date", "20260616", "--market", "SZ",
               "--symbols", "000555,000937,300179",
               "--report", str(OUT / "20260616-sz-three-symbols.json"),
               "--timings", str(OUT / "20260616-sz-three-symbols-timings.json"),
               "--temp-root", str(ROOT / "target/sz-sequence-repair-spool")]
    receipt = {
        "status": "running", "scope": "three_symbols_full_day_original_raw",
        "symbols": ["000555", "000937", "300179"], "command": command,
        "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
        "git_head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "working_tree_dirty": True,
        "started_at": datetime.now(timezone.utc).isoformat(),
    }
    inputs = []
    for feed in ["mdl_6_33_0", "mdl_6_36_0", "mdl_6_28_0"]:
        path = Path("/hdd/data/stock/raw_level2_parquet/date=20260616") / feed / "part-0.parquet"
        stat = path.stat()
        inputs.append(dict(path=str(path), size=stat.st_size, mtime_ns=stat.st_mtime_ns))
    receipt["inputs"] = inputs
    manifest = OUT / "run.json"
    manifest.write_text(json.dumps(receipt, indent=2) + "\n")
    started = time.monotonic()
    with (OUT / "validation.log").open("x") as log:
        proc = subprocess.Popen(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
        receipt["pid"] = proc.pid
        manifest.write_text(json.dumps(receipt, indent=2) + "\n")
        while proc.poll() is None:
            time.sleep(10)
            print("validation running", round(time.monotonic() - started, 1), "seconds", flush=True)
    receipt.update(status="completed", exit_code=proc.returncode,
                   elapsed_seconds=time.monotonic() - started,
                   finished_at=datetime.now(timezone.utc).isoformat())
    receipt["inputs_unchanged"] = all(
        (Path(r["path"]).stat().st_size, Path(r["path"]).stat().st_mtime_ns) ==
        (r["size"], r["mtime_ns"]) for r in inputs)
    manifest.write_text(json.dumps(receipt, indent=2) + "\n")
    print((OUT / "validation.log").read_text(), flush=True)
    print("elapsed_seconds", receipt["elapsed_seconds"], "exit_code", proc.returncode, flush=True)


if __name__ == "__main__":
    main()
