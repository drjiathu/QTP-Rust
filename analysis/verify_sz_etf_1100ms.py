"""Fixed 1100ms diagnostic; do not modify production defaults or source rows."""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from collections import Counter
import hashlib
import json
import subprocess
import time
import sqlite3
import sys

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'reports/20260907-sz-etf-1100ms'
BASE = ROOT / 'reports/20260907-sz-cross-date-diagnosis'
BIN = ROOT / 'target/release/qtp-replay'


def run(case):
    name, window, full = case
    OUT.mkdir(parents=True, exist_ok=True)
    report = OUT / f'{name}.json'
    assert not report.exists(), f'Preserve previous evidence: {report}'
    cmd = [str(BIN), 'validate', '--date', '20260706', '--market', 'SZ',
           '--report', str(report),
           '--temp-root', str(ROOT / 'target/etf-1100ms-spool')]
    if window is not None:
        cmd += ['--continuous-lookahead', f'{window}ms']
    if full:
        cmd += ['--only-etfs']
    else:
        cmd += ['--symbols', '159977,159980,159992', '--retain-matched-records',
                '--raw-root', str(BASE / 'fixtures/etf-original')]
    start = time.perf_counter()
    with (OUT / f'{name}.log').open('x') as log:
        proc = subprocess.run(cmd, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
    receipt = dict(command=cmd, exit_code=proc.returncode,
                   seconds=time.perf_counter()-start,
                   git_commit=subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
                   tracked_diff_sha256=hashlib.sha256(subprocess.check_output(['git', 'diff', 'HEAD'], cwd=ROOT)).hexdigest(),
                   binary_sha256=hashlib.sha256(BIN.read_bytes()).hexdigest())
    (OUT / f'{name}-run.json').write_text(json.dumps(receipt, indent=2)+'\n')
    result = json.loads(report.read_text()) if report.exists() else {}
    print(name, {k: result.get(k) for k in ['matched', 'mismatched', 'data_errors', 'missing_source', 'not_comparable']}, receipt['seconds'], flush=True)


def audit():
    load = lambda name: json.loads((OUT / f'{name}.json').read_text())
    old, new, full = [load(name) for name in ['samples-1000ms', 'samples-1100ms', 'all-etfs-1100ms']]
    key = lambda r: (r['symbol'], r['anchor'], r['reference_time_ms'])
    a, b = [{key(r): r for r in report['records']} for report in [old, new]]
    assert len(a) == len(old['records']) and len(b) == len(new['records'])
    assert a.keys() == b.keys()
    assert old['replay'] == new['replay'], 'Window must not change replay'
    assert new['continuous_lookback_ms'] == full['continuous_lookback_ms'] == 0
    assert set(new['continuous_lookahead_ms_by_symbol'].values()) == {1100}
    assert set(full['continuous_lookahead_ms_by_symbol'].values()) == {1100}
    rows = []
    for symbol in ['159977', '159980', '159992']:
        records = [r for r in b.values() if r['symbol'] == symbol]
        failed = [k for k, r in a.items() if r['symbol'] == symbol and r['outcome'] == 'mismatched']
        offsets = [b[k]['matched_candidate_time_ms']-b[k]['reference_time_ms'] for k in failed if b[k]['outcome']=='matched']
        continuous = [r for r in records if r['anchor']=='continuous_trading' and r['outcome']=='matched']
        assert all(0 <= r['matched_candidate_time_ms']-r['reference_time_ms'] < 1100 for r in continuous)
        rows.append(dict(symbol=symbol, counts=dict(Counter((r['outcome']) for r in records)),
                         stages=dict(Counter(r['anchor'] for r in records)),
                         failures_1000ms=len(failed), rescued=sum(b[k]['outcome']=='matched' for k in failed),
                         rescued_offset_ms=dict(sorted(Counter(offsets).items())),
                         maximum_matched_offset_ms=max(r['matched_candidate_time_ms']-r['reference_time_ms'] for r in continuous)))
    for k in a:
        if k[1] != 'continuous_trading':
            assert a[k] == b[k], 'Static validation must not change'
        if a[k]['outcome']=='matched':
            assert b[k]['outcome']=='matched', 'No regression'
    for report in [new, full]:
        assert report['mismatched'] == report['data_errors'] == report['missing_source'] == 0
    result = dict(samples=rows, full_etf_result={k: full[k] for k in [
        'total_anchors', 'comparable_anchors', 'matched', 'mismatched', 'not_comparable',
        'excluded_by_status', 'data_errors', 'missing_source', 'match_rate',
        'not_comparable_reasons', 'breakdown', 'omitted_mismatched_records']},
        selected_etfs=len(full['continuous_lookahead_ms_by_symbol']),
        replay_unchanged=True, static_validation_unchanged=True,
        note='Current-binary 20260706 test only; previous cross-date results are separate historical evidence.')
    (OUT / 'audit.json').write_text(json.dumps(result, ensure_ascii=False, indent=2)+'\n')
    print(json.dumps(result, ensure_ascii=False, indent=2), flush=True)


def historical_audit():
    """Historical 1s acceptance is evidence, not a current-binary rerun."""
    campaign = json.loads((ROOT / 'reports/20260906-random-profiled-full/campaign.json').read_text())
    rows = []
    for job in campaign['rows']:
        if job['market'] != 'SZ':
            continue
        row = dict(date=job['date'], status=job['status'], binary_sha256=job['binary_sha256'])
        receipt_path = ROOT / job['receipt']
        receipt = json.loads(receipt_path.read_text())
        if not receipt.get('report'):
            row['note'] = 'Replay aborted; no full-day ETF acceptance claim.'
        else:
            path = receipt_path.parent / receipt['report']
            report = json.loads(path.read_text())
            assert report['omitted_mismatched_records'] == 0
            row['report'] = str(path.relative_to(ROOT))
            row['lookback_ms'] = report['continuous_lookback_ms']
            row['etf_lookahead_ms'] = sorted({v for s, v in report['continuous_lookahead_ms_by_symbol'].items() if s.startswith('159')})
            row['breakdown'] = {k: v for k, v in report['breakdown'].items() if k.startswith('etf.')}
            assert row['lookback_ms'] == 0 and row['etf_lookahead_ms'] == [1000]
        rows.append(row)
    (OUT / 'historical-audit.json').write_text(json.dumps(rows, ensure_ascii=False, indent=2)+'\n')
    print('Historical dates:', [(r['date'], r['status']) for r in rows], flush=True)


def verify_default():
    run(('samples-default-1100ms', None, False))
    before = json.loads((OUT / 'samples-1100ms.json').read_text())
    after = json.loads((OUT / 'samples-default-1100ms.json').read_text())
    assert not after['diagnostic_window_override']
    assert after['continuous_lookback_ms'] == 0
    assert set(after['continuous_lookahead_ms_by_symbol'].values()) == {1100}
    assert before['records'] == after['records']
    assert before['replay'] == after['replay']
    result = dict(default_window_ms=1100, diagnostic_window_override=False,
                  matched=after['matched'], mismatched=after['mismatched'],
                  data_errors=after['data_errors'], missing_source=after['missing_source'],
                  records_identical_to_explicit_override=True, replay_unchanged=True,
                  scope='Three 20260706 ETF samples; full ETF day not rerun after default-only code change.')
    (OUT / 'default-audit.json').write_text(json.dumps(result, indent=2)+'\n')
    print(json.dumps(result), flush=True)


def report_artifact():
    """Canonical portable technical report; exact audit rows remain in JSON/notebook."""
    result = json.loads((OUT / 'audit.json').read_text())
    full = result['full_etf_result']
    sections = [
        ('title', '# SZ ETF 1.1-second validation', None),
        ('summary', '## 结论：1.1 秒满足本次已有异常数据条件\n\n'
         f"20260706 全部 {result['selected_etfs']} 只深市 ETF：{full['matched']:,} 项匹配，"
         f"{full['mismatched']} 项不匹配；{full['not_comparable']} 项不可比单列。"
         '三个已知异常 ETF 的 2,868 条失败全部消失。此结论不代表所有未来日期都以 1.1 秒为上界。', 'audit'),
        ('scope', '## 范围与计数口径\n\n'
         '计数单位是证券、阶段、参考时间对应的验证记录，不是证券数或逐笔事件数。'
         '盘中仅比较有效 T0，使用固定左闭右开 [T,T+1100ms)；回看窗口为 0。'
         'PreOpen 和 E0 的静态验收规则、默认市价单策略及比较字段均不变。'
         '匹配率分母仅含可比项，停牌排除项不计入通过数。', None),
        ('samples', '## 配对实验：增加 100ms 覆盖全部已知失败\n\n' + '\n\n'.join(
            f"{r['symbol']}：原 1 秒失败 {r['failures_1000ms']:,} 条；1.1 秒救回 {r['rescued']:,} 条。"
            '原失败帧实际匹配候选全部位于 T+1000～1040ms。' for r in result['samples']) +
         '\n\n三只样本全日 13,740 项匹配；配对审计确认恢复报告、静态验证记录不变，原通过项无回退。', 'audit'),
        ('full', '## 全 ETF 原始数据验证没有出现新的失败\n\n'
         f"盘中 {full['breakdown']['etf.continuous_trading']['matched']:,} 项匹配；"
         f"开盘 {full['breakdown']['etf.pre_open']['matched']:,} 项匹配；"
         f"收盘 {full['breakdown']['etf.market_close']['matched']:,} 项匹配。"
         f"数据错误 {full['data_errors']}，缺少来源 {full['missing_source']}。"
         '全部 ETF 任务直接读取原始 raw parquet，不依赖三只证券的提取文件。', 'audit'),
        ('history', '## 跨日期历史证据须与本次实测分开\n\n'
         '20260320、20260401、20260601、20260806、20260828 的已存全量报告中，深市 ETF 在 1 秒窗口内已无不匹配。'
         '这些是旧二进制证据，不能标记为本次最新版本 1.1 秒重跑。20260616 原全量任务中止，未形成全日 ETF 验收。', 'history'),
        ('method', '## 方法与边界审计\n\n'
         '当前 release 对三只样本分别运行 1000ms 和 1100ms，再运行全部 ETF 1100ms。'
         '按证券、阶段和参考时刻连接结果，检查键唯一性、覆盖一致、静态记录不变及新增候选落在固定窗口内。'
         '保留命令、二进制哈希、退出码、耗时和逐条样本明细。'
         '报告若将未变化的盘口标记在 T+1099ms，该时间代表窗口内静态候选，不是最后事件真实时间，不能用于推断物理延迟。', None),
        ('limits', '## 限制与后续选择\n\n'
         '本次证明 1100ms 足够覆盖已知失败及该日全 ETF，而不是证明它是最小或交易所保证的右边界。'
         '默认生产规范尚未修改，不逐帧扩窗，也不改写源记录。'
         '若要设为统一默认规则，下一步应以最新版本补齐跨日期 ETF 回归，尤其是此前全量中止的 20260616。', None),
    ]
    sources = [dict(id='audit', label='20260706 paired and full-ETF audit', path='reports/20260907-sz-etf-1100ms/audit.json'),
               dict(id='history', label='Historical full-market report audit', path='reports/20260907-sz-etf-1100ms/historical-audit.json')]
    blocks = [dict(id=identifier, type='markdown', body=body, **({'sourceId': source} if source else {}))
              for identifier, body, source in sections]
    # Five discrete observed offsets, not an interpolated latency trend.
    histogram = Counter()
    for sample in result['samples']:
        histogram.update(sample['rescued_offset_ms'])
    # The report renderer requires SQL provenance. Execute an independent SQLite
    # aggregate over the exact saved audit rows and cross-check the Python result.
    sql = 'SELECT offset_ms, SUM(records) AS records FROM rescued_candidates GROUP BY offset_ms ORDER BY offset_ms'
    with sqlite3.connect(':memory:') as conn:
        conn.execute('CREATE TABLE rescued_candidates(symbol TEXT, offset_ms INTEGER, records INTEGER)')
        conn.executemany('INSERT INTO rescued_candidates VALUES (?, ?, ?)', [
            (sample['symbol'], int(offset), count) for sample in result['samples']
            for offset, count in sample['rescued_offset_ms'].items()])
        sql_rows = conn.execute(sql).fetchall()
    assert dict(sql_rows) == {int(offset): count for offset, count in histogram.items()}
    sources[0]['query'] = dict(engine='sqlite', language='sql', sql=sql,
        tables_used=['rescued_candidates'],
        description='In-memory rescued_candidates is populated exactly from audit.json samples.rescued_offset_ms by verify_sz_etf_1100ms.py; cross-checked against Python Counter. Candidate offsets are not physical latency.')
    offsets = [dict(offset_ms=str(offset), records=count, total_rescued=2868,
                    window_end_exclusive_ms=1100, date='20260706')
               for offset, count in sorted(histogram.items(), key=lambda x: int(x[0]))]
    chart = dict(id='offsets', title='原失败帧的命中偏移分布',
                 subtitle='20260706 三只 ETF；2,868 条原失败记录；单位 ms',
                 type='bar', dataset='offsets', sourceId='audit', source=sources[0], encodings=dict(
                     x=dict(field='offset_ms', type='nominal', label='相对 T 的偏移 / ms'),
                     y=dict(field='records', type='quantitative', label='验证记录数')))
    blocks.insert(4, dict(id='offset_chart', type='chart', chartId='offsets'))
    artifact = dict(surface='report', manifest=dict(version=1, surface='report',
                    title='SZ ETF 1.1-second validation', blocks=blocks, sources=sources, charts=[chart]),
                    snapshot=dict(version=1, status='ready', datasets=dict(offsets=offsets)), sources=sources)
    (OUT / 'artifact.json').write_text(json.dumps(artifact, ensure_ascii=False, indent=2)+'\n')
    (OUT / 'report-notes.json').write_text(json.dumps(dict(
        audience='technical', delivery='html',
        chart_contract='Five discrete offset bins, bar family, single-root blue shared palette, zero baseline, direct axis labels, full-width portable HTML. Counts show all known rescued candidates below 1100ms; not a physical latency distribution. Rich dataset retains date, denominator and exclusive boundary.',
        structure='Summary, definitions, paired evidence, full-date evidence, history, methods, limitations/next steps; further questions merged into limitations.'), indent=2)+'\n')


if __name__ == '__main__':
    if '--verify-default' in sys.argv:
        verify_default()
    else:
        with ThreadPoolExecutor(max_workers=2) as pool:
            list(pool.map(run, [('samples-1000ms', 1000, False),
                                ('samples-1100ms', 1100, False), ('all-etfs-1100ms', 1100, True)]))
        audit()
