"""Frozen-binary smoke + six full-market jobs; never changes validation rules.

Run with Clara's Python (pyarrow). A fresh output directory is required.
Failures remain failures and do not prevent other independent full jobs.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import time

import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
RAW = Path('/hdd/data/stock/raw_level2_parquet')
DAYS = ['20260828', '20260601', '20260806']
FEEDS = ('mdl_4_24_0', 'MarketData', 'mdl_6_33_0', 'mdl_6_36_0', 'mdl_6_28_0')
SYMBOLS = {'SH': '600000,601318,688981,510300,588000,513100',
           'SZ': '000001,002475,300001,300750,159915,159501'}


def utc():
    return datetime.now(timezone.utc).isoformat()


def save(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2)+'\n')


def run(out, binary, day, market, scope):
    name = f'{day}-{market.lower()}-{scope}'
    report = out / f'{name}.json'
    log = out / f'{name}.log'
    performance = out / f'{name}.time.txt'
    command = [str(binary), 'validate', '--date', day, '--market', market,
               '--batch-size', '262144', '--temp-root', str(ROOT/'target/practical-full-spool'),
               '--max-detail-records', '5000', '--report', str(report)]
    if market == 'SZ':
        command += ['--sz-market-order-policy', 'rest-at-last-trade-price']
    command += ['--include-etfs'] if scope == 'full' else ['--symbols', SYMBOLS[market]]
    start = time.monotonic()
    entry = dict(date=day, market=market, scope=scope, command=command, started_at=utc(),
                 status='running', report=None)
    save(out/f'{name}-run.json', entry)
    print('START', name, flush=True)
    with log.open('w') as stream:
        result = subprocess.run(['/usr/bin/time', '-v', '-o', str(performance)] + command,
                                cwd=ROOT, stdout=stream, stderr=subprocess.STDOUT, check=False)
    entry.update(exit_code=result.returncode, finished_at=utc(),
                 elapsed_seconds=round(time.monotonic()-start, 3), log=log.name,
                 performance=performance.name)
    if report.is_file():
        data = json.loads(report.read_text())
        entry.update(status='completed', report=report.name,
                     counts={key: data[key] for key in ('total_anchors', 'matched', 'mismatched',
                             'excluded_by_status', 'data_errors', 'missing_source', 'breakdown')},
                     replay=data['replay'])
    else:
        entry.update(status='replay_aborted', error=log.read_text()[-16000:])
    save(out/f'{name}-run.json', entry)
    print('END', name, entry['status'], result.returncode, log.read_text()[-2000:], flush=True)
    return entry


def stage(out, binary, jobs, manifest):
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = {pool.submit(run, out, binary, *job): job for job in jobs}
        while futures:
            done, _ = wait(futures, timeout=30, return_when=FIRST_COMPLETED)
            if not done:
                print('ACTIVE', utc(), list(futures.values()), flush=True)
            for future in done:
                futures.pop(future)
                manifest['runs'].append(future.result())
                save(out/'manifest.json', manifest)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    out = args.output.resolve()
    out.mkdir(parents=True, exist_ok=False)
    build = ROOT/'target/release/qtp-replay'
    frozen_dir = ROOT/'target/validation-binaries'/out.name
    frozen_dir.mkdir(parents=True, exist_ok=False)
    binary = frozen_dir/'qtp-replay'
    shutil.copy2(build, binary)
    sources = []
    for day in DAYS:
        for feed in FEEDS:
            path = RAW/f'date={day}'/feed/'part-0.parquet'
            parquet = pq.ParquetFile(path)
            sources.append(dict(date=day, feed=feed, path=str(path), size=path.stat().st_size,
                                mtime_ns=path.stat().st_mtime_ns, rows=parquet.metadata.num_rows,
                                schema=str(parquet.schema_arrow)))
    manifest = dict(started_at=utc(), status='running', days=DAYS,
                    algorithm='SZ rest_at_last_trade_price; U keeps strict reconciliation',
                    frozen_binary=str(binary), binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
                    git_head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),
                    working_tree_dirty=True, date_selection='20260828 + prior random 20260601 + seed 20260906 in complete dates [20260706,20260828): 20260806',
                    sources=sources, runs=[])
    save(out/'manifest.json', manifest)
    stage(out, binary, [('20260828', m, 'smoke') for m in SYMBOLS], manifest)
    # Do not escalate a binary/engine abort from the smoke test to full runs.
    if any(r['status'] != 'completed' for r in manifest['runs']):
        manifest['status'] = 'blocked_by_smoke_replay'
        save(out/'manifest.json', manifest)
        return 1
    stage(out, binary, [('20260828', m, 'full') for m in SYMBOLS], manifest)
    stage(out, binary, [(d, m, 'full') for d in DAYS[1:] for m in SYMBOLS], manifest)
    manifest.update(status='finished', finished_at=utc())
    save(out/'manifest.json', manifest)
    return int(any(r['exit_code'] for r in manifest['runs']))


if __name__ == '__main__':
    raise SystemExit(main())
