"""Reproducible current-binary cross-date samples; no reconstruction rule changes."""
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import hashlib
import json
import random
import subprocess
import time

ROOT = Path(__file__).resolve().parents[1]
RAW = Path('/hdd/data/stock/raw_level2_parquet')
OUT = ROOT / 'reports/20260906-cross-date-pending'
FEEDS = ('mdl_4_24_0', 'MarketData', 'mdl_6_33_0', 'mdl_6_36_0', 'mdl_6_28_0')
SYMBOLS = {
    'SH': ['600000', '601318', '688981', '510300', '588000', '513100'],
    'SZ': ['000001', '002475', '300001', '300750', '159915', '159501'],
}

def run(day, market):
    report = OUT / f'{day}-{market.lower()}-strict.json'
    command = [str(ROOT / 'target/release/qtp-replay'), 'validate', '--date', day,
               '--market', market, '--symbols', ','.join(SYMBOLS[market]),
               '--batch-size', '262144', '--temp-root', str(ROOT / 'target/cross-date-spool'),
               '--max-detail-records', '1000', '--report', str(report)]
    start = time.monotonic()
    print('START', day, market, flush=True)
    result = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, check=False)
    entry = dict(date=day, market=market, symbols=SYMBOLS[market], command=command,
                 exit_code=result.returncode, elapsed_seconds=round(time.monotonic()-start, 3),
                 stdout=result.stdout, stderr=result.stderr,
                 report=str(report.relative_to(ROOT)) if report.exists() else None)
    (OUT / f'{day}-{market.lower()}-run.json').write_text(json.dumps(entry, ensure_ascii=False, indent=2)+'\n')
    print('END', day, market, result.returncode, result.stdout.strip(), result.stderr.strip(), flush=True)
    return entry

if __name__ == '__main__':
    OUT.mkdir(parents=True, exist_ok=False)
    days = sorted(p.name[5:] for p in RAW.glob('date=*') if p.name != 'date=20260828'
                  and all((p / f / 'part-0.parquet').is_file() for f in FEEDS))
    chosen = random.Random(20260906).choice(days)
    binary = ROOT / 'target/release/qtp-replay'
    manifest = dict(seed=20260906, eligible_dates=days, selected_dates=['20260828', chosen],
                    scope='stratified symbols, full trading day; NOT all-market acceptance',
                    binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
                    source_head=subprocess.check_output(['git','rev-parse','HEAD'], cwd=ROOT, text=True).strip(),
                    source_dirty=True, runs=[])
    with ThreadPoolExecutor(max_workers=2) as pool:
        jobs = [pool.submit(run, day, market) for day in manifest['selected_dates'] for market in SYMBOLS]
        for future in as_completed(jobs):
            manifest['runs'].append(future.result())
            (OUT / 'runs.json').write_text(json.dumps(manifest, ensure_ascii=False, indent=2)+'\n')
