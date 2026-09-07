"""Early cross-date samples using the same frozen binary as the full batch."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import redirect_stderr, redirect_stdout
import hashlib
import json
from pathlib import Path
import traceback

from run_practical_full_validation import ROOT, run, utc
from run_latest_cross_date_validation import save


def main():
    parent = ROOT / 'reports/20260906-latest-cross-date-full'
    source = json.loads((parent / 'manifest.json').read_text())
    binary = Path(source['frozen_binary'])
    assert hashlib.sha256(binary.read_bytes()).hexdigest() == source['binary_sha256']
    out = parent / 'smoke'
    out.mkdir(exist_ok=False)
    with (out / 'driver.log').open('x', buffering=1) as log, redirect_stdout(log), redirect_stderr(log):
        manifest = dict(status='running', started_at=utc(), days=source['days'],
                        binary_sha256=source['binary_sha256'], runs=[])
        save(out / 'manifest.json', manifest)
        try:
            jobs = [(d, m, 'smoke') for d in source['days'] for m in ('SH', 'SZ')]
            with ThreadPoolExecutor(max_workers=2) as pool:
                for receipt in pool.map(lambda job: run(out, binary, *job), jobs):
                    manifest['runs'].append(receipt)
                    save(out / 'manifest.json', manifest)
            manifest.update(status='finished', finished_at=utc())
        except Exception:
            manifest.update(status='driver_error', error=traceback.format_exc())
            print(manifest['error'], flush=True)
        save(out / 'manifest.json', manifest)
        return int(manifest['status'] != 'finished' or any(r['exit_code'] for r in manifest['runs']))


if __name__ == '__main__':
    raise SystemExit(main())
