"""Read-only raw snapshot audit of SH campaign missing-close records."""
from collections import Counter
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
RAW = Path('/hdd/data/stock/raw_level2_parquet')


def audit():
    campaign = json.loads((ROOT / 'reports/20260906-random-profiled-full/campaign.json').read_text())
    results = []
    for job in campaign['rows']:
        if job['market'] != 'SH':
            continue
        receipt_path = ROOT / job['receipt']
        receipt = json.loads(receipt_path.read_text())
        report = json.loads((receipt_path.parent / receipt['report']).read_text())
        targets = [r for r in report['records'] if r['outcome'] in ('missing_source', 'data_error')]
        assert len(targets) == report['missing_source'] + report['data_errors']
        if not targets:
            continue
        states = {r['symbol']: dict(record=r, rows=0, statuses=Counter(), transitions=[], tail=[],
                                    close_rows=[], regressions=0) for r in targets}
        path = RAW / f"date={job['date']}" / 'MarketData/part-0.parquet'
        file = pq.ParquetFile(path)
        columns = ['SecurityID', 'UpdateTime', 'InstruStatus', 'source_row_no',
                   'TradNumber', 'TradVolume', 'LastPrice', 'ClosePrice']
        scanned = 0
        for batch in file.iter_batches(batch_size=262144, columns=columns):
            scanned += batch.num_rows
            selected = batch.filter(pc.is_in(batch.column('SecurityID'), value_set=pa.array(list(states))))
            for row in selected.to_pylist():
                s = states[row['SecurityID']]
                status = (row['InstruStatus'] or '').strip()
                s['rows'] += 1
                s['statuses'][status] += 1
                if s['tail'] and (row['UpdateTime'] < s['tail'][-1]['UpdateTime'] or
                                  row['source_row_no'] <= s['tail'][-1]['source_row_no']):
                    s['regressions'] += 1
                if not s['transitions'] or s['transitions'][-1]['InstruStatus'].strip() != status:
                    s['transitions'].append(row)
                s['tail'] = (s['tail'] + [row])[-3:]
                if status == 'CLOSE':
                    s['close_rows'].append(row)
        assert scanned == file.metadata.num_rows
        item = dict(date=job['date'], path=str(path), source_rows=scanned,
                    source_size=path.stat().st_size, source_mtime_ns=path.stat().st_mtime_ns,
                    symbols=states)
        results.append(item)
        print(json.dumps(item, ensure_ascii=False, default=str), flush=True)
    assert sum(s['record']['outcome'] == 'missing_source' for d in results for s in d['symbols'].values()) == 33
    return results


if __name__ == '__main__':
    results = audit()
    out = ROOT / 'reports/20260907-sh-missing-close'
    out.mkdir(exist_ok=True)
    (out / 'raw-snapshot-audit.json').write_text(json.dumps(results, ensure_ascii=False, indent=2, default=str) + '\n')
