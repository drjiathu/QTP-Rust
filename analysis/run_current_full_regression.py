"""Frozen six-process regression, conditional random extension and durable profiling.

No replay/validation policy overrides, no source writes, no automatic fixes.
Run with Clara's pyarrow Python; every campaign needs a fresh directory.
"""
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime, timezone
from pathlib import Path
import argparse
import hashlib
import json
import os
import random
import re
import shutil
import subprocess
import sys
import time
import traceback

import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
RAW = Path('/hdd/data/stock/raw_level2_parquet')
OUT = ROOT / 'reports/20260907-current-full-regression'
DAYS = ['20260320', '20260401', '20260601', '20260616', '20260706', '20260806', '20260828']
FEEDS = ('mdl_4_24_0', 'MarketData', 'mdl_6_33_0', 'mdl_6_36_0', 'mdl_6_28_0')
WORKERS = 6
SEED = 20260908


def utc():
    return datetime.now(timezone.utc).isoformat()


def save(path, value):
    tmp = path.with_suffix(path.suffix + '.tmp')
    tmp.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n')
    tmp.replace(path)


def digest(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def previously_tested_dates():
    """Preserve selection history even after superseded report files are pruned."""
    dates = set(DAYS)
    for path in (ROOT / 'reports').glob('*/manifest.json'):
        manifest = json.loads(path.read_text())
        for key in ('baseline_days', 'random_days', 'days', 'previously_tested_dates', 'excluded_dates'):
            dates.update(str(day) for day in manifest.get(key, []) if re.fullmatch(r'20\d{6}', str(day)))
    for path in (ROOT / 'reports').rglob('*-run.json'):
        match = re.match(r'(20\d{6})-', path.name)
        if match:
            dates.add(match[1])
    return dates


def inventory(days):
    rows = []
    for day in days:
        for feed in FEEDS:
            path = RAW / f'date={day}' / feed / 'part-0.parquet'
            f = pq.ParquetFile(path)
            rows.append(dict(date=day, feed=feed, path=str(path), rows=f.metadata.num_rows,
                             size=path.stat().st_size, mtime_ns=path.stat().st_mtime_ns,
                             schema=str(f.schema_arrow)))
    return rows


def validate_report(report, day, market, sources):
    assert not report['diagnostic_window_override']
    assert report['omitted_mismatched_records'] == 0
    assert report['total_anchors'] == report['matched'] + report['mismatched'] + report['not_comparable']
    assert report['comparable_anchors'] == report['matched'] + report['mismatched']
    for field in ('matched', 'mismatched', 'not_comparable', 'excluded_by_status', 'data_errors', 'missing_source'):
        assert sum(v[field] for v in report['breakdown'].values()) == report[field]
    feeds = ('mdl_4_24_0',) if market == 'SH' else ('mdl_6_33_0', 'mdl_6_36_0')
    assert report['replay']['input_rows'] == sum(s['rows'] for s in sources if s['date']==day and s['feed'] in feeds)
    assert report['continuous_lookback_ms'] == (1000 if market == 'SH' else 0)
    for symbol, horizon in report['continuous_lookahead_ms_by_symbol'].items():
        expected = 1000
        if market == 'SZ':
            if symbol.startswith('159'):
                expected = 1100
            elif symbol.startswith(('300', '301', '302')):
                expected = 3000
        assert horizon == expected, (symbol, horizon, expected)
    if market == 'SZ':
        assert report['replay']['sz_market_order_policy'] == 'rest_at_last_trade_price'
    return all(report[k] == 0 for k in ('mismatched', 'data_errors', 'missing_source')) and report['comparable_anchors'] > 0


def run_job(binary, day, market, stage, sources):
    name = f'{day}-{market.lower()}-full'
    path = OUT / f'{name}-run.json'
    report_path = OUT / f'{name}.json'
    timings_path = OUT / f'{name}-timings.json'
    assert not path.exists() and not report_path.exists()
    cmd = [str(binary), '--date', day, '--market', market, '--raw-root', str(RAW),
           '--report', str(report_path), '--timings', str(timings_path),
           '--temp-root', str(ROOT / 'target/current-regression-spool'), '--max-detail-records', '0']
    receipt = dict(date=day, market=market, stage=stage, status='running', started_at=utc(),
                   command=cmd, target_universe='all_stocks_and_etfs', acceptance_passed=False)
    save(path, receipt)
    start = time.perf_counter()
    try:
        with (OUT / f'{name}.log').open('x') as log:
            proc = subprocess.run(['/usr/bin/time', '-v', '-o', str(OUT / f'{name}.time.txt')] + cmd,
                                  cwd=ROOT, stdout=log, stderr=subprocess.STDOUT,
                                  env={**os.environ, 'LC_ALL': 'C'})
        receipt.update(exit_code=proc.returncode, elapsed_seconds=time.perf_counter()-start, finished_at=utc())
        perf = (OUT / f'{name}.time.txt').read_text()
        rss = re.search(r'Maximum resident set size \(kbytes\):\s*(\d+)', perf)
        receipt['peak_rss_kib'] = int(rss[1]) if rss else None
        if not report_path.exists():
            receipt.update(status='replay_aborted', error=(OUT / f'{name}.log').read_text()[-16000:])
        else:
            report = json.loads(report_path.read_text())
            acceptance = validate_report(report, day, market, sources)
            # Detect input replacement during the job, without claiming a full content hash.
            for source in sources:
                if source['date'] == day:
                    stat = Path(source['path']).stat()
                    assert (stat.st_size, stat.st_mtime_ns) == (source['size'], source['mtime_ns'])
            timing = json.loads(timings_path.read_text())
            t = timing['stages']
            assert abs(t['profiled_total_seconds'] - t['restore_total_seconds'] - t['validation_total_seconds'] - t['unattributed_seconds']) < 1e-5
            receipt.update(status='completed', report=report_path.name, timings=timings_path.name,
                           acceptance_passed=acceptance and proc.returncode == 0,
                           timing_summary=t, input_identity_unchanged=True)
            receipt['counts'] = {k: report[k] for k in (
                'total_anchors', 'comparable_anchors', 'matched', 'mismatched', 'not_comparable',
                'excluded_by_status', 'data_errors', 'missing_source', 'match_rate', 'not_comparable_rate',
                'breakdown', 'mismatch_fields', 'mismatch_reasons', 'not_comparable_reasons')}
            receipt['replay'] = report['replay']
            receipt['first_anomalies'] = [r for r in report['records'] if r['outcome'] not in ('matched','excluded_by_status')][:20]
            receipt['throughput_applied_events_per_restore_second'] = report['replay']['applied_events'] / t['restore_total_seconds']
    except Exception:
        receipt.update(status='driver_error', error=traceback.format_exc(), acceptance_passed=False,
                       elapsed_seconds=time.perf_counter()-start, finished_at=utc())
    save(path, receipt)
    print('FINISHED', name, receipt['status'], receipt['acceptance_passed'], flush=True)
    return receipt


def summarize(manifest):
    rows = []
    for day in manifest['baseline_days'] + manifest.get('random_days', []):
        for market in ('SH', 'SZ'):
            path = OUT / f'{day}-{market.lower()}-full-run.json'
            rows.append(json.loads(path.read_text()) if path.exists() else dict(date=day, market=market, status='queued'))
    summary = dict(updated_at=utc(), campaign_status=manifest['status'], random_stage=manifest['random_stage'],
                   baseline_gate_passed=manifest.get('baseline_gate_passed', False),
                   all_jobs_finished=all(r['status'] in ('completed','replay_aborted','driver_error') for r in rows),
                   all_scheduled_jobs_passed=bool(rows) and all(r.get('acceptance_passed',False) for r in rows), rows=rows)
    save(OUT / 'summary.json', summary)
    lines = ['# 当前代码沪深全市场回归与 profiling', '',
             f"状态：{manifest['status']}；随机阶段：{manifest['random_stage']}；更新时间：{summary['updated_at']}。", '',
             '| 日期 | 市场 | 状态 | 匹配 | 不匹配 | 状态排除 | 数据错/缺源 | 恢复秒 | 对比秒 | 总秒 | 峰值 GiB |',
             '|---|---|---|---:|---:|---:|---|---:|---:|---:|---:|']
    for row in rows:
        c, t = row.get('counts', {}), row.get('timing_summary', {})
        fmt = lambda value: f'{value:.2f}' if value is not None else '—'
        values = [row['date'], row['market'], row['status'], c.get('matched','—'), c.get('mismatched','—'),
                  c.get('excluded_by_status','—'), f"{c.get('data_errors','—')}/{c.get('missing_source','—')}",
                  fmt(t.get('restore_total_seconds')), fmt(t.get('validation_total_seconds')),
                  fmt(row.get('elapsed_seconds')), fmt(row['peak_rss_kib']/1048576 if row.get('peak_rss_kib') else None)]
        lines.append('| ' + ' | '.join(map(str, values)) + ' |')
    lines += ['', '恢复 = 输入读取/分片 + 排除验证回调的回放 + 分片清理；对比 = 参考读取/分类 + 比较回调 + 报告整理。',
              '总时间另含启动/序列化/写报告。均为六进程并发下的 instrumented wall time，不是 CPU 时间或独立纯回放基准。',
              '可比率、状态排除、字段差异和来源缺失分别计数。规则固定，禁止失败后扩窗。',
              manifest.get('selection_note', '首阶段 14 个日市场任务全部通过且审计通过后，才随机抽取三个未测试日期。失败时保留逐帧明细与临时分片位置。')]
    (OUT / 'summary.md').write_text('\n'.join(lines)+'\n')
    return summary


def stage(manifest, binary, days, name):
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        pending = {pool.submit(run_job, binary, day, market, name, manifest['sources']):(day,market)
                   for day in days for market in ('SH','SZ')}
        results = []
        while pending:
            done, _ = wait(pending, timeout=30, return_when=FIRST_COMPLETED)
            for future in done:
                pending.pop(future)
                results.append(future.result())
            manifest.update(heartbeat_at=utc(), pending_jobs=list(pending.values()))
            save(OUT / 'manifest.json', manifest)
            summarize(manifest)
        return all(r['acceptance_passed'] for r in results)


def main():
    OUT.mkdir(parents=True, exist_ok=False)
    manifest = None
    try:
        frozen = ROOT / 'target/validation-binaries' / OUT.name / 'validation_benchmark'
        frozen.parent.mkdir(parents=True, exist_ok=False)
        shutil.copy2(ROOT / 'target/release/examples/validation_benchmark', frozen)
        hashes = {}
        for path in sorted([*ROOT.glob('src/**/*.rs'), ROOT/'Cargo.toml', ROOT/'Cargo.lock',
                            ROOT/'examples/validation_benchmark.rs', Path(__file__)]):
            relative = path.relative_to(ROOT)
            dest = OUT / 'source-snapshot' / relative
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, dest)
            hashes[str(relative)] = digest(path)
        save(OUT/'source-sha256.json', hashes)
        (OUT/'working-tree.patch').write_bytes(subprocess.check_output(['git','diff','--binary','HEAD'], cwd=ROOT))
        historical = previously_tested_dates()
        eligible = sorted(p.name[5:] for p in RAW.glob('date=*') if re.fullmatch(r'date=\d{8}',p.name)
                          and p.name[5:] not in historical and all((p/f/'part-0.parquet').is_file() for f in FEEDS))
        manifest = dict(status='baseline_running', started_at=utc(), heartbeat_at=utc(), driver_pid=os.getpid(),
                        baseline_days=DAYS, random_days=[], random_stage='waiting_for_baseline', seed=SEED,
                        random_count=3, eligible_random_dates=eligible, previously_tested_dates=sorted(historical),
                        max_workers=WORKERS, features=['profiling'], scope='all_stocks_and_etfs',
                        frozen_binary=str(frozen), binary_sha256=digest(frozen), sources=inventory(DAYS),
                        git_head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),
                        working_tree_dirty=True, timing_mode='exclusive instrumented wall time',
                        resource_policy='6 workers; observed historical peak about 40 GiB per process; retain failure spools')
        save(OUT/'manifest.json',manifest)
        summarize(manifest)
        passed = stage(manifest, frozen, DAYS, 'baseline')
        manifest['baseline_gate_passed'] = passed
        if not passed:
            manifest.update(status='baseline_failed', random_stage='not_started_baseline_failed', finished_at=utc())
        elif len(eligible) < 3:
            manifest.update(status='random_selection_blocked', random_stage='insufficient_untested_dates', finished_at=utc())
        else:
            days = sorted(random.Random(SEED).sample(eligible, 3))
            manifest.update(random_days=days, random_stage='running', status='random_running')
            manifest['sources'].extend(inventory(days))
            save(OUT/'manifest.json',manifest)
            passed = stage(manifest, frozen, days, 'random')
            manifest.update(status='completed' if passed else 'random_failed', random_stage='completed', finished_at=utc())
        save(OUT/'manifest.json',manifest)
        summarize(manifest)
        return 0 if manifest['status']=='completed' else 1
    except Exception:
        if manifest is None:
            manifest = dict(baseline_days=DAYS, random_days=[], random_stage='not_started')
        manifest.update(status='driver_error', error=traceback.format_exc(), finished_at=utc())
        save(OUT/'manifest.json',manifest)
        summarize(manifest)
        raise


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--summarize', action='store_true')
    parser.add_argument('--launch', action='store_true')
    args = parser.parse_args()
    if args.launch:
        assert not OUT.exists(), 'Do not overwrite or duplicate an existing campaign'
        log_path = ROOT / 'target/current-full-regression-driver.log'
        with log_path.open('x', buffering=1) as log:
            child = subprocess.Popen([sys.executable, str(Path(__file__).resolve())], cwd=ROOT,
                                     stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT,
                                     start_new_session=True)
        print(json.dumps(dict(driver_pid=child.pid, log=str(log_path), output=str(OUT))))
    elif args.summarize:
        summarize(json.loads((OUT/'manifest.json').read_text()))
    else:
        raise SystemExit(main())
