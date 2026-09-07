"""Isolate SZ ETF samples so a stock abort cannot mask ETF coverage."""
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import subprocess
import time

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'reports/20260906-cross-date-pending'


def run(day):
    source = json.loads((OUT / f'{day}-sz-run.json').read_text())
    report = OUT / f'{day}-sz-etfs-strict.json'
    command = source['command'][:-1] + [str(report)]
    command[command.index('--symbols') + 1] = '159915,159501'
    start = time.monotonic()
    completed = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, check=False)
    entry = dict(date=day, market='SZ', symbols=['159915', '159501'], command=command,
                 exit_code=completed.returncode, elapsed_seconds=time.monotonic()-start,
                 stdout=completed.stdout, stderr=completed.stderr,
                 report=str(report.relative_to(ROOT)) if report.exists() else None)
    (OUT / f'{day}-sz-etfs-run.json').write_text(json.dumps(entry, ensure_ascii=False, indent=2)+'\n')
    print(json.dumps(entry, ensure_ascii=False), flush=True)


if __name__ == '__main__':
    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(run, ['20260828', '20260601']))
