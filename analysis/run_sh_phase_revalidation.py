"""Six independent SH processes; immutable binary, receipts and phase timings."""
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from contextlib import redirect_stderr, redirect_stdout
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import traceback

import pyarrow.parquet as pq

from run_latest_cross_date_validation import save
from run_practical_full_validation import ROOT, RAW, utc
from run_random_profiled_validation import run_job

OUT = ROOT / 'reports/20260907-sh-phase-revalidation'
DAYS = ['20260320', '20260401', '20260601', '20260616', '20260706', '20260806']


def duration(seconds):
    if seconds is None:
        return '—'
    n = round(seconds)
    return f'{n // 3600:02d}:{n // 60 % 60:02d}:{n % 60:02d}'


def collect(out=OUT):
    manifest = json.loads((out / 'manifest.json').read_text())
    rows = []
    for day in manifest['days']:
        receipt_path = out / f'{day}-sh-full-run.json'
        receipt = json.loads(receipt_path.read_text()) if receipt_path.exists() else {}
        item = dict(date=day, status=receipt.get('status', 'queued'))
        for key in ('elapsed_seconds', 'exit_code', 'error'):
            if key in receipt:
                item[key] = receipt[key]
        if receipt.get('report'):
            report = json.loads((out / receipt['report']).read_text())
            baseline = json.loads((ROOT / manifest['baselines'][day]).read_text())
            assert receipt['target_universe'] == 'all_stocks_and_etfs'
            assert '--symbols' not in receipt['command']
            assert not report['diagnostic_window_override']
            assert report['replay']['input_rows'] == next(s['rows'] for s in manifest['sources']
                if s['date'] == day and s['feed'] == 'mdl_4_24_0')
            assert report['total_anchors'] == report['matched'] + report['mismatched'] + report['not_comparable']
            for key in ('matched', 'mismatched', 'excluded_by_status', 'data_errors', 'missing_source'):
                assert sum(b[key] for b in report['breakdown'].values()) == report[key]
                item[key] = report[key]
            item.update(match_rate=report['match_rate'], not_comparable_rate=report['not_comparable_rate'],
                        breakdown=report['breakdown'], replay_counters_unchanged=report['replay'] == baseline['replay'],
                        matched_count_unchanged=report['matched'] == baseline['matched'],
                        total_anchors_unchanged=report['total_anchors'] == baseline['total_anchors'],
                        report=receipt['report'])
            current = {(r['symbol'], r['anchor']): r for r in report['records']}
            item['previous_anomalies'] = [dict(symbol=r['symbol'], anchor=r['anchor'], previous=r['outcome'],
                current=current.get((r['symbol'], r['anchor']), {}).get('outcome', 'detail_not_retained'),
                reason=current.get((r['symbol'], r['anchor']), {}).get('reason'))
                for r in baseline['records'] if r['outcome'] in ('data_error', 'missing_source')]
            item['all_previous_anomalies_excluded'] = all(r['current'] == 'excluded_by_status' for r in item['previous_anomalies'])
            item['anomalies'] = [r for r in report['records'] if r['outcome'] not in ('matched', 'excluded_by_status')]
            measurement = json.loads((out / receipt['timings']).read_text())
            item['timings'] = measurement
            t = measurement['stages']
            assert abs(t['profiled_total_seconds'] - t['restore_total_seconds'] - t['validation_total_seconds'] - t['unattributed_seconds']) < 1e-6
        rows.append(item)
    finished = all(r['status'] in ('completed', 'replay_aborted', 'driver_error') for r in rows)
    passed = finished and all(r.get('exit_code') == 0 and r['status'] == 'completed' for r in rows)
    regression_ok = finished and all(all(r.get(k, False) for k in (
        'replay_counters_unchanged', 'matched_count_unchanged', 'total_anchors_unchanged', 'all_previous_anomalies_excluded')) for r in rows)
    return dict(updated_at=utc(), all_jobs_finished=finished, all_snapshot_validations_passed=passed,
                classification_regression_passed=regression_ok, rows=rows)


def summarize(out=OUT):
    result = collect(out)
    save(out / 'summary.json', result)
    lines = ['# 沪市停牌参考帧分类修正：六日全量重跑', '',
             f"更新 {result['updated_at']}；全部结束 {result['all_jobs_finished']}；全部验证通过 {result['all_snapshot_validations_passed']}；分类回归通过 {result['classification_regression_passed']}。", '',
             '每日期独立进程，沪市全量股票和 ETF；raw snapshot；正常比较窗口及订单簿恢复不变。',
             '只有本轮实际生成的 full.json 才计为完成，旧批次仅作分类回归基线。', '',
             '| 日期 | 状态 | 匹配 | 不匹配 | 排除 | 数据错/缺源 | 恢复 | 对比 | 总耗时 |',
             '|---|---|---:|---:|---:|---|---|---|---|']
    for r in result['rows']:
        t = r.get('timings', {}).get('stages', {})
        values = [r['date'], r['status'], r.get('matched', '—'), r.get('mismatched', '—'), r.get('excluded_by_status', '—'),
            f"{r.get('data_errors', '—')}/{r.get('missing_source', '—')}", duration(t.get('restore_total_seconds')),
            duration(t.get('validation_total_seconds')), duration(r.get('elapsed_seconds'))]
        lines.append('| ' + ' | '.join(map(str, values)) + ' |')
    lines += ['', '恢复 = 逐笔读取/分片 + 回放扣除验证回调 + 临时分片清理。',
              '对比 = 参考帧读取/分类 + 验证回调 + 报告整理。进程总时间另含启动和报告输出。',
              '全部为本机六进程并发下的 wall time，包含共享 I/O 和调度影响，不等于纯 CPU 时间。',
              '状态排除不是匹配；快照通过不证明全部逐事件中间态或 FIFO。',
              'summary.json 同时核对原 33 项缺源和 1 项状态错误的分类、匹配计数与回放统计是否保持预期。']
    (out / 'summary.md').write_text('\n'.join(lines) + '\n')
    return result


def main():
    OUT.mkdir(parents=True, exist_ok=False)
    with (OUT / 'driver.log').open('x', buffering=1) as log, redirect_stdout(log), redirect_stderr(log):
        manifest = None
        try:
            binary = ROOT / 'target/validation-binaries' / OUT.name / 'validation_benchmark'
            binary.parent.mkdir(parents=True, exist_ok=False)
            shutil.copy2(ROOT / 'target/release/examples/validation_benchmark', binary)
            sources = []
            for day in DAYS:
                for feed in ('mdl_4_24_0', 'MarketData'):
                    p = RAW / f'date={day}' / feed / 'part-0.parquet'
                    f = pq.ParquetFile(p)
                    sources.append(dict(date=day, feed=feed, path=str(p), rows=f.metadata.num_rows,
                        size=p.stat().st_size, mtime_ns=p.stat().st_mtime_ns, schema=str(f.schema_arrow)))
            old_campaign = json.loads((ROOT / 'reports/20260906-random-profiled-full/campaign.json').read_text())
            baselines = {}
            for r in old_campaign['rows']:
                if r['market'] == 'SH' and r['date'] in DAYS:
                    p = ROOT / r['receipt']
                    baselines[r['date']] = str((p.parent / json.loads(p.read_text())['report']).relative_to(ROOT))
            hashes = {}
            for p in sorted([*ROOT.glob('src/**/*.rs'), ROOT / 'Cargo.toml', ROOT / 'Cargo.lock', ROOT / 'examples/validation_benchmark.rs', Path(__file__)]):
                rel = p.relative_to(ROOT)
                dest = OUT / 'source-snapshot' / rel
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(p, dest)
                hashes[str(rel)] = hashlib.sha256(p.read_bytes()).hexdigest()
            save(OUT / 'source-sha256.json', hashes)
            manifest = dict(status='running', started_at=utc(), heartbeat_at=utc(), days=DAYS, market='SH',
                max_workers=6, sources=sources, baselines=baselines, runs=[], features=['profiling'],
                frozen_binary=str(binary), binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
                git_head=subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
                working_tree_dirty=True, scope='all_stocks_and_etfs',
                timing_note='six concurrent SH processes; phase classification fix only')
            save(OUT / 'manifest.json', manifest)
            with ThreadPoolExecutor(max_workers=6) as pool:
                pending = {pool.submit(run_job, OUT, binary, day, 'SH'): day for day in DAYS}
                while pending:
                    done, _ = wait(pending, timeout=30, return_when=FIRST_COMPLETED)
                    for f in done:
                        pending.pop(f)
                        manifest['runs'].append(f.result())
                    manifest.update(heartbeat_at=utc(), pending_jobs=sorted(pending.values()))
                    save(OUT / 'manifest.json', manifest)
                    summarize()
                    print('HEARTBEAT', manifest['heartbeat_at'], 'pending', len(pending), flush=True)
            result = summarize()
            manifest.update(status='finished', finished_at=utc(), all_snapshot_validations_passed=result['all_snapshot_validations_passed'],
                            classification_regression_passed=result['classification_regression_passed'])
            save(OUT / 'manifest.json', manifest)
            return int(not result['all_snapshot_validations_passed'] or not result['classification_regression_passed'])
        except Exception:
            print(traceback.format_exc(), flush=True)
            if manifest is not None:
                manifest.update(status='driver_error', finished_at=utc())
                save(OUT / 'manifest.json', manifest)
            return 1


if __name__ == '__main__':
    raise SystemExit(main())
