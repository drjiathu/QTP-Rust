"""Five additional untested dates using the proven frozen six-process profiler."""
from pathlib import Path
import argparse
import json
import os
import random
import re
import shutil
import subprocess
import sys
import traceback

import run_current_full_regression as shared

ROOT = shared.ROOT
OUT = ROOT / 'reports/20260908-additional-random-full'
PREVIOUS = ROOT / 'reports/20260907-current-full-regression'
SEED = 20260909
COUNT = 5
shared.OUT = OUT


def main():
    previous = json.loads((PREVIOUS/'manifest.json').read_text())
    result = json.loads((PREVIOUS/'summary.json').read_text())
    assert previous['status']=='completed' and all(r['acceptance_passed'] for r in result['rows'])
    OUT.mkdir(parents=True, exist_ok=False)
    manifest = None
    try:
        excluded = shared.previously_tested_dates()
        eligible = sorted(p.name[5:] for p in shared.RAW.glob('date=*')
                          if re.fullmatch(r'date=\d{8}',p.name) and p.name[5:] not in excluded
                          and all((p/f/'part-0.parquet').is_file() for f in shared.FEEDS))
        assert len(eligible)>=COUNT
        days = sorted(random.Random(SEED).sample(eligible, COUNT))
        binary = ROOT/'target/validation-binaries'/OUT.name/'validation_benchmark'
        binary.parent.mkdir(parents=True, exist_ok=False)
        shutil.copy2(ROOT/'target/release/examples/validation_benchmark', binary)
        assert shared.digest(binary)==previous['binary_sha256'], 'Expected unchanged validated binary'
        hashes = {}
        for path in sorted([*ROOT.glob('src/**/*.rs'), ROOT/'Cargo.toml', ROOT/'Cargo.lock',
                            ROOT/'examples/validation_benchmark.rs', Path(shared.__file__), Path(__file__)]):
            relative = path.relative_to(ROOT)
            dest = OUT/'source-snapshot'/relative
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, dest)
            hashes[str(relative)] = shared.digest(path)
        previous_hashes = json.loads((PREVIOUS/'source-sha256.json').read_text())
        for name, value in hashes.items():
            if name.startswith('src/') or name in ('Cargo.toml','Cargo.lock','examples/validation_benchmark.rs'):
                assert value == previous_hashes[name], name
        shared.save(OUT/'source-sha256.json', hashes)
        manifest = dict(status='random_running', random_stage='running', driver_pid=os.getpid(),
                        started_at=shared.utc(), heartbeat_at=shared.utc(),
                        baseline_days=[], baseline_gate_passed=True, previous_campaign=str(PREVIOUS.relative_to(ROOT)),
                        random_days=days, random_count=COUNT, seed=SEED, eligible_random_dates=eligible,
                        previously_tested_dates=sorted(excluded), max_workers=shared.WORKERS,
                        scope='all_stocks_and_etfs', features=['profiling'], frozen_binary=str(binary),
                        binary_sha256=shared.digest(binary), sources=shared.inventory(days),
                        git_head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),
                        core_sources_unchanged_from_previous_campaign=True,
                        timing_mode='exclusive instrumented wall time',
                        selection_note='此前十日全量已通过；本轮从其他完整日期中固定种子随机抽五日，独立运行两市全量。不匹配不自动扩窗，所有失败明细保留。')
        shared.save(OUT/'manifest.json', manifest)
        shared.summarize(manifest)
        print('SELECTED', days, 'ELIGIBLE',len(eligible),flush=True)
        passed = shared.stage(manifest, binary, days, 'random')
        manifest.update(status='completed' if passed else 'random_failed', random_stage='completed', finished_at=shared.utc())
        shared.save(OUT/'manifest.json',manifest)
        shared.summarize(manifest)
        return 0 if passed else 1
    except Exception:
        if manifest is None:
            manifest = dict(baseline_days=[],random_days=[],random_stage='not_started')
        manifest.update(status='driver_error', error=traceback.format_exc(), finished_at=shared.utc())
        shared.save(OUT/'manifest.json',manifest)
        shared.summarize(manifest)
        raise


if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--launch', action='store_true')
    parser.add_argument('--summarize', action='store_true')
    args = parser.parse_args()
    if args.launch:
        assert not OUT.exists(), 'Never overwrite or duplicate a campaign'
        log_path = ROOT/'target/additional-random-validation-driver.log'
        with log_path.open('x',buffering=1) as log:
            child = subprocess.Popen([sys.executable,str(Path(__file__).resolve())],cwd=ROOT,
                                     stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,
                                     start_new_session=True)
        print(json.dumps(dict(driver_pid=child.pid,log=str(log_path),output=str(OUT))))
    elif args.summarize:
        shared.summarize(json.loads((OUT/'manifest.json').read_text()))
    else:
        raise SystemExit(main())
