"""Durable full-market validation driver; run with Clara's pyarrow Python.

Logs never depend on an interactive tool pipe. Each completed job has its own
receipt; the manifest heartbeat and generated summary survive a lost UI session.
Never reuses a report directory or changes replay/validation semantics.
"""
import argparse
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from contextlib import redirect_stderr, redirect_stdout
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import traceback

import pyarrow.parquet as pq

from run_practical_full_validation import FEEDS, RAW, ROOT, run, utc


def save(path, value):
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n')
    temporary.replace(path)


def summarize(out, manifest):
    rows = []
    for day in manifest['days']:
        for market in ('SH', 'SZ'):
            path = out / f'{day}-{market.lower()}-full-run.json'
            receipt = json.loads(path.read_text()) if path.exists() else {}
            row = dict(date=day, market=market, status=receipt.get('status', 'queued'))
            if 'exit_code' in receipt:
                row.update(exit_code=receipt['exit_code'], elapsed_seconds=receipt['elapsed_seconds'])
            if receipt.get('report'):
                report = json.loads((out / receipt['report']).read_text())
                for key in ('total_anchors', 'matched', 'mismatched', 'excluded_by_status',
                            'data_errors', 'missing_source', 'breakdown', 'mismatch_fields',
                            'mismatch_reasons', 'not_comparable_reasons', 'match_tags',
                            'omitted_mismatched_records'):
                    row[key] = report[key]
                row['replayed_symbols'] = report['replay']['symbols']
                row['applied_events'] = report['replay']['applied_events']
                row['comparable_match_rate'] = report['match_rate']
                row['not_comparable_rate'] = report['not_comparable_rate']
                row['after_close_events'] = report['replay']['sz_after_close_events']
                row['sample_anomalies'] = [r for r in report['records']
                    if r['outcome'] not in ('matched', 'excluded_by_status')][:10]
                assert not report['diagnostic_window_override']
                assert sum(c['total'] for c in report['breakdown'].values()) == report['total_anchors']
                for key in ('matched', 'mismatched', 'excluded_by_status', 'data_errors', 'missing_source'):
                    assert sum(c[key] for c in report['breakdown'].values()) == report[key]
                feeds = ('mdl_4_24_0',) if market == 'SH' else ('mdl_6_33_0', 'mdl_6_36_0')
                assert report['replay']['input_rows'] == sum(s['rows'] for s in manifest['sources']
                    if s['date'] == day and s['feed'] in feeds)
                assert '--include-etfs' in receipt['command'] and '--symbols' not in receipt['command']
                if market == 'SZ':
                    assert report['replay']['sz_market_order_policy'] == 'rest_at_last_trade_price'
                row['report'] = receipt['report']
            elif receipt.get('error'):
                row['error'] = receipt['error']
            rows.append(row)
    finished = all(r['status'] in ('completed', 'replay_aborted', 'driver_error') for r in rows)
    passed = finished and all(r['status'] == 'completed' and r.get('exit_code') == 0 for r in rows)
    result = dict(updated_at=utc(), binary_sha256=manifest['binary_sha256'],
                  all_jobs_finished=finished, all_snapshot_validations_passed=passed, rows=rows)
    save(out / 'summary.json', result)
    lines = ['# 最新代码跨日期全市场验证（自动汇总）', '',
             f"更新时间：{result['updated_at']}；全部完成：{finished}；全部快照通过：{passed}。", '',
             '股票与 ETF；raw snapshot；标准比较窗口不变；SZ 市价采用 RestAtLastTradePrice。',
             '快照匹配不证明全部逐事件中间态或 FIFO 正确。回放中止不具有全日匹配率。', '',
             '| 日期 | 市场 | 状态 | 匹配 | 不匹配 | 状态排除 | 数据错误 | 缺少来源 |',
             '|---|---|---|---:|---:|---:|---:|---:|']
    for row in rows:
        columns = [row.get(k, '—') for k in ('date', 'market', 'status', 'matched',
                   'mismatched', 'excluded_by_status', 'data_errors', 'missing_source')]
        lines.append('| ' + ' | '.join(map(str, columns)) + ' |')
    lines += ['', '详细分类、异常字段、首批异常上下文见 summary.json 和各 full.json。',
              '来源、二进制及源码指纹见 manifest.json；调度日志见 driver.log。']
    (out / 'summary.md').write_text('\n'.join(lines) + '\n')
    return result


def execute(out, manifest):
    jobs = [(day, market, 'full') for day in manifest['days'] for market in ('SH', 'SZ')]
    binary = Path(manifest['frozen_binary'])
    with ThreadPoolExecutor(max_workers=manifest['max_workers']) as pool:
        pending = {pool.submit(run, out, binary, *job): job for job in jobs}
        while pending:
            done, _ = wait(pending, timeout=30, return_when=FIRST_COMPLETED)
            for future in done:
                job = pending.pop(future)
                try:
                    manifest['runs'].append(future.result())
                except Exception:
                    error = traceback.format_exc()
                    print(error, flush=True)
                    manifest['scheduler_errors'].append(dict(job=job, error=error))
            manifest['heartbeat_at'] = utc()
            manifest['pending_jobs'] = list(pending.values())
            save(out / 'manifest.json', manifest)
            summarize(out, manifest)
            print('HEARTBEAT', manifest['heartbeat_at'], 'unfinished', len(pending), flush=True)
    summary = summarize(out, manifest)
    manifest.update(status='finished' if not manifest['scheduler_errors'] else 'driver_error',
                    finished_at=utc(), all_snapshot_validations_passed=summary['all_snapshot_validations_passed'])
    save(out / 'manifest.json', manifest)
    return int(not summary['all_snapshot_validations_passed'])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    out = args.output.resolve()
    out.mkdir(parents=True, exist_ok=False)
    # Redirect in-process before launching workers. Broken UI pipes cannot stop
    # scheduling subsequent dates, and tracebacks remain inspectable on disk.
    with (out / 'driver.log').open('x', buffering=1) as log, redirect_stdout(log), redirect_stderr(log):
        manifest = None
        try:
            days = ['20260601', '20260706', '20260806']
            sources = []
            for day in days:
                for feed in FEEDS:
                    path = RAW / f'date={day}' / feed / 'part-0.parquet'
                    parquet = pq.ParquetFile(path)
                    sources.append(dict(date=day, feed=feed, path=str(path),
                        rows=parquet.metadata.num_rows, size=path.stat().st_size,
                        mtime_ns=path.stat().st_mtime_ns, schema=str(parquet.schema_arrow)))
            frozen = ROOT / 'target/validation-binaries' / out.name / 'qtp-replay'
            frozen.parent.mkdir(parents=True, exist_ok=False)
            shutil.copy2(ROOT / 'target/release/qtp-replay', frozen)
            source_paths = sorted([*ROOT.glob('src/**/*.rs'), ROOT / 'Cargo.toml', ROOT / 'Cargo.lock'])
            source_hashes = {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in source_paths}
            save(out / 'source-sha256.json', source_hashes)
            (out / 'source-working-tree.patch').write_bytes(subprocess.check_output(['git', 'diff', '--', 'src', 'Cargo.toml', 'Cargo.lock'], cwd=ROOT))
            # Include untracked Rust modules, which git diff does not capture.
            for path in source_paths:
                destination = out / 'source-snapshot' / path.relative_to(ROOT)
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(path, destination)
            manifest = dict(status='running', started_at=utc(), heartbeat_at=utc(), days=days,
                date_selection='20260601 requested; 20260706 SH ETF rule boundary; 20260806 previously selected with seed 20260906 from 39 complete dates [20260706,20260828)',
                frozen_binary=str(frozen), binary_sha256=hashlib.sha256(frozen.read_bytes()).hexdigest(),
                git_head=subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
                working_tree_dirty=bool(subprocess.check_output(['git', 'status', '--porcelain'], cwd=ROOT)),
                sources=sources, source_hashes_file='source-sha256.json',
                source_snapshot='source-snapshot', max_workers=2, runs=[], scheduler_errors=[])
            save(out / 'manifest.json', manifest)
            return execute(out, manifest)
        except Exception:
            print(traceback.format_exc(), flush=True)
            if manifest is not None:
                manifest.update(status='driver_error', finished_at=utc())
                save(out / 'manifest.json', manifest)
            return 1


if __name__ == '__main__':
    raise SystemExit(main())
