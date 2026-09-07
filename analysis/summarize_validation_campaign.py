"""Read-only campaign aggregation; --write refreshes derived summary artifacts."""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import tempfile

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'reports/20260906-random-profiled-full'


def collect(out=OUT):
    manifest_path = out / 'manifest.json'
    new = json.loads(manifest_path.read_text()) if manifest_path.exists() else {}
    batches = [
        (ROOT / 'reports/20260906-rest-at-last-trade-full', ['20260828']),
        (ROOT / 'reports/20260906-latest-cross-date-full', ['20260601', '20260706', '20260806']),
        (out, new.get('days', ['20260320', '20260401', '20260616'])),
    ]
    rows = []
    for folder, dates in batches:
        provenance = json.loads((folder / 'manifest.json').read_text()) if (folder / 'manifest.json').exists() else {}
        for date in dates:
            for market in ('SH', 'SZ'):
                path = folder / f'{date}-{market.lower()}-full-run.json'
                receipt = json.loads(path.read_text()) if path.exists() else {}
                row = dict(date=date, market=market, status=receipt.get('status', 'queued'),
                    receipt=str(path.relative_to(ROOT)), binary_sha256=provenance.get('binary_sha256'),
                    timing_mode='instrumented' if folder == out else 'unprofiled')
                for key in ('exit_code', 'elapsed_seconds', 'started_at', 'finished_at', 'error'):
                    if key in receipt:
                        row[key] = receipt[key]
                if receipt.get('report'):
                    report = json.loads((folder / receipt['report']).read_text())
                    assert '--symbols' not in receipt['command']
                    if folder == out:
                        assert receipt['target_universe'] == 'all_stocks_and_etfs'
                    else:
                        assert '--include-etfs' in receipt['command']
                    feeds = ('mdl_4_24_0',) if market == 'SH' else ('mdl_6_33_0','mdl_6_36_0')
                    assert report['replay']['input_rows'] == sum(s['rows'] for s in provenance['sources']
                        if s['date'] == date and s['feed'] in feeds)
                    assert report['total_anchors'] == report['matched'] + report['mismatched'] + report['not_comparable']
                    for key in ('matched', 'mismatched', 'excluded_by_status', 'data_errors', 'missing_source',
                                'match_rate', 'not_comparable_rate', 'breakdown', 'mismatch_fields', 'not_comparable_reasons'):
                        row[key] = report[key]
                    assert not report['diagnostic_window_override']
                    for key in ('matched', 'mismatched', 'excluded_by_status', 'data_errors', 'missing_source'):
                        assert sum(c[key] for c in report['breakdown'].values()) == report[key]
                    row['symbols'] = report['replay']['symbols']
                    row['sample_anomalies'] = [r for r in report['records'] if r['outcome'] not in ('matched', 'excluded_by_status')][:5]
                    row['snapshot_passed'] = receipt['exit_code'] == 0
                if receipt.get('timings'):
                    measurement = json.loads((folder / receipt['timings']).read_text())
                    stages = measurement['stages']
                    assert abs(stages['profiled_total_seconds'] - stages['restore_total_seconds']
                        - stages['validation_total_seconds'] - stages['unattributed_seconds']) < 1e-6
                    row['timings'] = measurement
                rows.append(row)
    rows.sort(key=lambda r: (r['date'], r['market']))
    daily = []
    for date in sorted({r['date'] for r in rows}):
        markets = [r for r in rows if r['date'] == date]
        complete = all(r.get('finished_at') for r in markets)
        item = dict(date=date, complete=complete, matched=sum(r.get('matched', 0) for r in markets),
                    mismatched=sum(r.get('mismatched', 0) for r in markets))
        if complete:
            item['day_wall_span_seconds'] = (max(datetime.fromisoformat(r['finished_at']) for r in markets)
                - min(datetime.fromisoformat(r['started_at']) for r in markets)).total_seconds()
            item['market_elapsed_sum_seconds'] = sum(r['elapsed_seconds'] for r in markets)
        if all('timings' in r for r in markets):
            item['restore_market_sum_seconds'] = sum(r['timings']['stages']['restore_total_seconds'] for r in markets)
            item['validation_market_sum_seconds'] = sum(r['timings']['stages']['validation_total_seconds'] for r in markets)
        daily.append(item)
    finished = all(r['status'] in ('completed', 'replay_aborted', 'driver_error') for r in rows)
    return dict(updated_at=datetime.now(timezone.utc).isoformat(), all_jobs_finished=finished,
        all_snapshot_validations_passed=finished and all(r.get('snapshot_passed', False) for r in rows),
        completed_market_jobs=sum(r['status'] == 'completed' for r in rows), expected_market_jobs=len(rows),
        rows=rows, daily=daily)


def write_summary(out=OUT):
    result = collect(out)
    def duration(value):
        if value is None:
            return '未记录'
        seconds = round(value)
        return f'{seconds//3600:02d}:{seconds//60%60:02d}:{seconds%60:02d}'
    lines = ['# 跨日期全量验证与耗时汇总', '',
        f"更新：{result['updated_at']}；完成报告 {result['completed_market_jobs']}/{result['expected_market_jobs']}；全部结束 {result['all_jobs_finished']}；全部通过 {result['all_snapshot_validations_passed']}。", '',
        '不含任何小样本。所有时间为 elapsed wall time，HH:MM:SS；未埋点的旧任务不能追溯拆分。', '',
        '| 日期 | 市场 | 状态 | 匹配 | 不匹配 | 排除 | 数据错/缺源 | 恢复 | 对比 | 进程总耗时 |',
        '|---|---|---|---:|---:|---:|---|---|---|---|']
    for row in result['rows']:
        stages = row.get('timings', {}).get('stages', {})
        values = [row['date'], row['market'], row['status'], row.get('matched', '—'), row.get('mismatched', '—'),
            row.get('excluded_by_status', '—'), f"{row.get('data_errors', '—')}/{row.get('missing_source', '—')}",
            duration(stages.get('restore_total_seconds')), duration(stages.get('validation_total_seconds')),
            duration(row.get('elapsed_seconds'))]
        lines.append('| ' + ' | '.join(map(str, values)) + ' |')
    lines += ['', '## 每日沪深合计', '',
        '日期跨度是首个市场启动到最后一个市场结束，可能含错峰启动等待；不是两个并发进程耗时相加。',
        '恢复/对比累计是两个市场各自分项时间之和，不能当作并行后的日历耗时。', '',
        '| 日期 | 两市任务跨度 | 市场耗时累计 | 恢复累计 | 对比累计 |', '|---|---|---|---|---|']
    for row in result['daily']:
        lines.append('| ' + ' | '.join([row['date']] + [duration(row.get(k)) for k in
            ('day_wall_span_seconds', 'market_elapsed_sum_seconds', 'restore_market_sum_seconds', 'validation_market_sum_seconds')]) + ' |')
    lines += ['', '恢复 = 输入/分片 + 回放循环扣除验证回调 + 分片清理；对比 = 参考数据加载/阶段检查 + 验证回调 + 报告整理。',
        '回放部分包含迭代、解码、归一化和簿更新，不是纯 apply 耗时。验证回调包含候选盘口提取与比较。',
        '进程总耗时另含启动、序列化、写报告、退出；计时器开销未校正，多进程共享资源，不作线性性能外推。',
        '不匹配、错误或中止须单独调查；快照匹配不证明所有逐事件中间态和 FIFO 正确。详见 campaign.json。']
    for name, text in [('campaign.json', json.dumps(result, ensure_ascii=False, indent=2)+'\n'),
                       ('campaign.md', '\n'.join(lines)+'\n')]:
        with tempfile.NamedTemporaryFile(mode='w', dir=out, prefix=name+'.', suffix='.tmp', delete=False) as handle:
            handle.write(text)
            temporary = handle.name
        os.replace(temporary, out / name)
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--write', action='store_true')
    parser.add_argument('--require-complete', action='store_true')
    args = parser.parse_args()
    result = write_summary() if args.write else collect()
    if args.require_complete:
        assert result['all_jobs_finished'], 'Campaign still incomplete'
    print(json.dumps(result, ensure_ascii=False, indent=2))
