"""Random additional dates, frozen benchmark, durable two-process scheduler."""
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from contextlib import redirect_stderr, redirect_stdout
import hashlib
import json
from pathlib import Path
import random
import shutil
import subprocess
import time
import traceback

import pyarrow.parquet as pq

from run_practical_full_validation import ROOT, RAW, FEEDS, utc
from run_latest_cross_date_validation import save
from summarize_validation_campaign import OUT, write_summary


def run_job(out, binary, day, market):
    name = f'{day}-{market.lower()}-full'
    report = out / f'{name}.json'
    timing = out / f'{name}-timings.json'
    command = [str(binary), '--date', day, '--market', market, '--report', str(report),
        '--timings', str(timing), '--temp-root', str(ROOT/'target/profile-spool')]
    receipt = dict(date=day, market=market, scope='full', status='running', report=None,
        command=command, started_at=utc(), target_universe='all_stocks_and_etfs')
    path = out/f'{name}-run.json'
    save(path, receipt)
    started = time.monotonic()
    print('START', name, flush=True)
    try:
        with (out/f'{name}.log').open('x') as log:
            process = subprocess.run(['/usr/bin/time', '-v', '-o', str(out/f'{name}.time.txt')] + command,
                cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, check=False)
        receipt.update(exit_code=process.returncode, elapsed_seconds=time.monotonic()-started, finished_at=utc())
        if report.exists():
            receipt.update(status='completed', report=report.name, timings=timing.name if timing.exists() else None)
        else:
            receipt.update(status='replay_aborted', error=(out/f'{name}.log').read_text()[-16000:])
    except Exception:
        receipt.update(status='driver_error', exit_code=1, error=traceback.format_exc(),
            elapsed_seconds=time.monotonic()-started, finished_at=utc())
    save(path, receipt)
    print('END', name, receipt['status'], receipt['exit_code'], flush=True)
    return receipt


def main():
    OUT.mkdir(parents=True, exist_ok=False)
    with (OUT/'driver.log').open('x', buffering=1) as log, redirect_stdout(log), redirect_stderr(log):
        manifest = None
        try:
            excluded = {'20260828','20260601','20260706','20260806'}
            pool = sorted(p.name[5:] for p in RAW.glob('date=*') if p.name[5:] not in excluded
                and all((p/feed/'part-0.parquet').is_file() for feed in FEEDS))
            days = sorted(random.Random(20260907).sample(pool, 3))
            assert days == ['20260320','20260401','20260616'], 'date pool changed; review before running'
            binary = ROOT/'target/validation-binaries'/OUT.name/'validation_benchmark'
            binary.parent.mkdir(parents=True, exist_ok=False)
            shutil.copy2(ROOT/'target/release/examples/validation_benchmark', binary)
            sources = []
            for day in days:
                for feed in FEEDS:
                    path = RAW/f'date={day}'/feed/'part-0.parquet'
                    parquet = pq.ParquetFile(path)
                    sources.append(dict(date=day, feed=feed, path=str(path), rows=parquet.metadata.num_rows,
                        size=path.stat().st_size, mtime_ns=path.stat().st_mtime_ns, schema=str(parquet.schema_arrow)))
            files = sorted([*ROOT.glob('src/**/*.rs'),ROOT/'Cargo.toml',ROOT/'Cargo.lock',ROOT/'examples/validation_benchmark.rs'])
            hashes = {}
            for file in files:
                name = file.relative_to(ROOT)
                destination = OUT/'source-snapshot'/name
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(file, destination)
                hashes[str(name)] = hashlib.sha256(file.read_bytes()).hexdigest()
            save(OUT/'source-sha256.json', hashes)
            manifest = dict(status='running', started_at=utc(), heartbeat_at=utc(), days=days, seed=20260907,
                eligible_dates=pool, excluded_dates=sorted(excluded), sources=sources, runs=[], max_workers=2,
                frozen_binary=str(binary), binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
                features=['profiling'], git_head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),
                working_tree_dirty=True, timing_note='instrumented wall-time attribution; no matching rule changes')
            save(OUT/'manifest.json', manifest)
            jobs = [(d,m) for d in days for m in ('SH','SZ')]
            with ThreadPoolExecutor(max_workers=2) as executor:
                pending = {executor.submit(run_job, OUT, binary, *job):job for job in jobs}
                while pending:
                    done,_ = wait(pending, timeout=30, return_when=FIRST_COMPLETED)
                    for future in done:
                        pending.pop(future)
                        manifest['runs'].append(future.result())
                    manifest.update(heartbeat_at=utc(), pending_jobs=list(pending.values()))
                    save(OUT/'manifest.json',manifest)
                    write_summary(OUT)
                    print('HEARTBEAT',manifest['heartbeat_at'],'pending',len(pending),flush=True)
            manifest.update(status='finished',finished_at=utc())
            save(OUT/'manifest.json',manifest)
            write_summary(OUT)
            return int(any(r['exit_code'] for r in manifest['runs']))
        except Exception:
            print(traceback.format_exc(),flush=True)
            if manifest is not None:
                manifest.update(status='driver_error',finished_at=utc())
                save(OUT/'manifest.json',manifest)
            return 1


if __name__ == '__main__':
    raise SystemExit(main())
